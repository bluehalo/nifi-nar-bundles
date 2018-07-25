package com.asymmetrik.nifi.processors.mongo;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.UUID;

import com.asymmetrik.nifi.services.mongo.MongoClientService;
import com.asymmetrik.nifi.services.mongo.StandardMongoClientService;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.apache.commons.io.FileUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Requires a Mongo instance running at localhost:28017
 */
public class UpdateMongoIT {
    private static final String MONGO_CONTROLLER_SERVICE = "mongo_controller_service";
    private static final String MONGO_DATABASE_NAME = "testUpdateMongo" + UUID.randomUUID();
    private static final Configuration JSON_PROVIDER_CONFIGURATION = Configuration.builder().jsonProvider(new GsonJsonProvider()).build();
    private static final JsonProvider JSON_PROVIDER = JSON_PROVIDER_CONFIGURATION.jsonProvider();

    MongoClientService mongo;

    @Before
    public void setUp() throws Exception {
        mongo = new StandardMongoClientService();
        insertRefinedPayload();
    }

    @After
    public void tearDown() throws Exception {
        if (null != mongo) {
            MongoClient client = mongo.getMongoClient();
            MongoDatabase db = client.getDatabase(MONGO_DATABASE_NAME);
            if (db != null) {
                db.drop();
            }
            client.close();
        }
    }

    protected void addMongoService(TestRunner runner) throws InitializationException {
        HashMap<String, String> props = new HashMap<>();

        // Add mongo controller service
        runner.addControllerService(MONGO_CONTROLLER_SERVICE, mongo, props);
        runner.setProperty(MongoProps.MONGO_SERVICE, MONGO_CONTROLLER_SERVICE);
        runner.enableControllerService(mongo);
    }

    protected void insertRefinedPayload() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new StoreInMongo());

        addMongoService(runner);
        runner.setProperty(MongoProps.DATABASE, MONGO_DATABASE_NAME);
        runner.setProperty(MongoProps.COLLECTION, "insert_test");

        String contents = FileUtils.readFileToString(Paths.get("src/test/resources/mongo/payload.json").toFile());

        runner.enqueue(contents.getBytes());
        runner.run();
    }

    protected Document buildQuery(String queryKey, final JsonObject jsonPayload) throws Exception {
        Document queryDocument = null;

        // Extract the element corresponding to the query key
        JsonElement element = JsonUtil.extractElement(jsonPayload, queryKey);
        if (element != null) {
            // Try to extract oid element
            JsonElement oid = JsonUtil.extractElement(element, "$oid");
            if (oid != null) {
                // Query key is an ObjectId; Create new ObjectId

                // Trim quotation marks, if any
                String oidString = oid.toString().replaceAll("\"", "");
                queryDocument = new Document(queryKey, new ObjectId(oidString));
            } else {
                // Query key is not an ObjectId, use as-is
                queryDocument = new Document(queryKey, element.toString());
            }
        }

        return queryDocument;
    }

    @Test
    public void testPushUpdate() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new UpdateMongo());
        addMongoService(runner);
        runner.setProperty(MongoProps.DATABASE, MONGO_DATABASE_NAME);
        runner.setProperty(MongoProps.COLLECTION, "insert_test");
        runner.setProperty(MongoProps.UPDATE_QUERY_KEYS, "d.id");
        runner.setProperty(MongoProps.UPDATE_KEYS, "d.media");
        runner.setProperty(MongoProps.UPDATE_OPERATOR, "$addToSet");

        String contents = FileUtils.readFileToString(Paths.get("src/test/resources/mongo/update_payload.json").toFile());

        runner.enqueue(contents.getBytes());
        runner.run();

        runner.assertTransferCount(AbstractMongoProcessor.REL_FAILURE, 0);
        runner.assertTransferCount(AbstractMongoProcessor.REL_SUCCESS, 1);

        // Verify whether the update was made
        MongoClient client = mongo.getMongoClient();
        MongoDatabase db = client.getDatabase(MONGO_DATABASE_NAME);
        if (db != null) {
            MongoCollection<Document> collection = db.getCollection("insert_test");
            Document query = buildQuery("d.id", (JsonObject) JSON_PROVIDER.parse(contents));
            assertEquals(1, collection.count(query.append("d.media", new Document("$exists", true))));
        }
    }

    @Test
    public void testMalformedJsonUpdate() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new UpdateMongo());
        addMongoService(runner);
        runner.setProperty(MongoProps.DATABASE, MONGO_DATABASE_NAME);
        runner.setProperty(MongoProps.COLLECTION, "insert_test");
        runner.setProperty(MongoProps.UPDATE_QUERY_KEYS, "d.id");
        runner.setProperty(MongoProps.UPDATE_KEYS, "d.media");
        runner.setProperty(MongoProps.UPDATE_OPERATOR, "$push");

        runner.enqueue("bad payload".getBytes());
        runner.run();

        runner.assertTransferCount(AbstractMongoProcessor.REL_FAILURE, 1);
        runner.assertTransferCount(AbstractMongoProcessor.REL_SUCCESS, 0);
    }

    @Test
    public void testSetUpdate() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new UpdateMongo());
        addMongoService(runner);
        runner.setProperty(MongoProps.DATABASE, MONGO_DATABASE_NAME);
        runner.setProperty(MongoProps.COLLECTION, "insert_test");
        runner.setProperty(MongoProps.UPDATE_QUERY_KEYS, "d.id");
        runner.setProperty(MongoProps.UPDATE_KEYS, "d.s");
        runner.setProperty(MongoProps.UPDATE_OPERATOR, "$set");

        String contents = FileUtils.readFileToString(Paths.get("src/test/resources/mongo/update_payload.json").toFile());

        runner.enqueue(contents.getBytes());
        runner.run();

        runner.assertTransferCount(AbstractMongoProcessor.REL_FAILURE, 0);
        runner.assertTransferCount(AbstractMongoProcessor.REL_SUCCESS, 1);

        // Verify whether the update was made
        MongoClient client = mongo.getMongoClient();
        MongoDatabase db = client.getDatabase(MONGO_DATABASE_NAME);
        if (db != null) {
            MongoCollection<Document> collection = db.getCollection("insert_test");
            Document query = buildQuery("d.id", (JsonObject) JSON_PROVIDER.parse(contents));
            assertEquals(collection.count(query.append("d.s", new Document("$eq", "xyz"))), 1);
        }
    }

    @Test
    public void testAbsentUpdateKey() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new UpdateMongo());
        addMongoService(runner);
        runner.setProperty(MongoProps.DATABASE, MONGO_DATABASE_NAME);
        runner.setProperty(MongoProps.COLLECTION, "insert_test");
        runner.setProperty(MongoProps.UPDATE_QUERY_KEYS, "_id");
        runner.setProperty(MongoProps.UPDATE_KEYS, "xxx");
        runner.setProperty(MongoProps.UPDATE_OPERATOR, "$set");

        String contents = FileUtils.readFileToString(Paths.get("src/test/resources/mongo/update_payload.json").toFile());

        runner.enqueue(contents.getBytes());
        runner.run();

        runner.assertTransferCount(AbstractMongoProcessor.REL_FAILURE, 1);
        runner.assertTransferCount(AbstractMongoProcessor.REL_SUCCESS, 0);

        // Verify whether the update was made
        MongoClient client = mongo.getMongoClient();
        MongoDatabase db = client.getDatabase(MONGO_DATABASE_NAME);
        if (db != null) {
            MongoCollection<Document> collection = db.getCollection("insert_test");
            Document query = buildQuery("d.id", (JsonObject) JSON_PROVIDER.parse(contents));
            assertEquals(collection.count(query.append("d.s", new Document("$eq", null))), 1);
        }
    }

    @Test
    public void testUnsetUpdate() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new UpdateMongo());
        addMongoService(runner);
        runner.setProperty(MongoProps.DATABASE, MONGO_DATABASE_NAME);
        runner.setProperty(MongoProps.COLLECTION, "insert_test");
        runner.setProperty(MongoProps.UPDATE_QUERY_KEYS, "d.id");
        runner.setProperty(MongoProps.UPDATE_KEYS, "d.g");
        runner.setProperty(MongoProps.UPDATE_OPERATOR, "$unset");

        String contents = FileUtils.readFileToString(Paths.get("src/test/resources/mongo/update_payload.json").toFile());

        runner.enqueue(contents.getBytes());
        runner.run();

        runner.assertTransferCount(AbstractMongoProcessor.REL_FAILURE, 0);
        runner.assertTransferCount(AbstractMongoProcessor.REL_SUCCESS, 1);

        // Verify whether the update was made
        MongoClient client = mongo.getMongoClient();
        MongoDatabase db = client.getDatabase(MONGO_DATABASE_NAME);
        if (db != null) {
            MongoCollection<Document> collection = db.getCollection("insert_test");
            Document query = buildQuery("d.id", (JsonObject) JSON_PROVIDER.parse(contents));
            assertEquals(collection.count(query.append("d.g", new Document("$exists", true))), 0);
        }
    }

    @Test
    public void testUpsertTrue() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new UpdateMongo());
        addMongoService(runner);
        runner.setProperty(MongoProps.DATABASE, MONGO_DATABASE_NAME);
        runner.setProperty(MongoProps.COLLECTION, "upsert_test");
        runner.setProperty(MongoProps.UPDATE_QUERY_KEYS, "d.id");
        runner.setProperty(MongoProps.UPDATE_KEYS, "d.g");
        runner.setProperty(MongoProps.UPDATE_OPERATOR, "$unset");
        runner.setProperty(MongoProps.UPSERT, "true");

        String contents = FileUtils.readFileToString(Paths.get("src/test/resources/mongo/update_payload.json").toFile());

        runner.enqueue(contents.getBytes());
        runner.run();

        runner.assertTransferCount(AbstractMongoProcessor.REL_FAILURE, 0);
        runner.assertTransferCount(AbstractMongoProcessor.REL_SUCCESS, 1);

        // Verify that document wasn't inserted
        MongoClient client = mongo.getMongoClient();
        MongoDatabase db = client.getDatabase(MONGO_DATABASE_NAME);
        if (db != null) {
            MongoCollection<Document> collection = db.getCollection("upsert_test");
            assertEquals(1, collection.count());
        }
    }

    @Test
    public void testUpsertFalse() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new UpdateMongo());
        addMongoService(runner);
        runner.setProperty(MongoProps.DATABASE, MONGO_DATABASE_NAME);
        runner.setProperty(MongoProps.COLLECTION, "upsert_test");
        runner.setProperty(MongoProps.UPDATE_QUERY_KEYS, "d.id");
        runner.setProperty(MongoProps.UPDATE_KEYS, "d.g");
        runner.setProperty(MongoProps.UPDATE_OPERATOR, "$unset");
        runner.setProperty(MongoProps.UPSERT, "false");

        String contents = FileUtils.readFileToString(Paths.get("src/test/resources/mongo/update_payload.json").toFile());

        runner.enqueue(contents.getBytes());
        runner.run();

        runner.assertTransferCount(AbstractMongoProcessor.REL_FAILURE, 0);
        runner.assertTransferCount(AbstractMongoProcessor.REL_SUCCESS, 1);

        // Verify that document wasn't inserted
        MongoClient client = mongo.getMongoClient();
        MongoDatabase db = client.getDatabase(MONGO_DATABASE_NAME);
        if (db != null) {
            MongoCollection<Document> collection = db.getCollection("upsert_test");
            assertEquals(0, collection.count());
        }
    }
}
