package com.asymmetrik.nifi.processors.mongo;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;

import com.asymmetrik.nifi.services.mongo.MongoClientService;
import com.asymmetrik.nifi.services.mongo.StandardMongoClientService;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;

import org.apache.commons.io.FileUtils;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Requires a Mongo instance running at localhost:28017
 */
public class DistinctMongoIT {
    private static final String MONGO_CONTROLLER_SERVICE = "mongo_controller_service";
    private static final String MONGO_DATABASE_NAME = "testDistinctMongo" + UUID.randomUUID();
    private static final String MONGO_COLLECTION = "distinct_test";

    MongoClientService mongo;

    @Before
    public void setUp() throws Exception {
        mongo = new StandardMongoClientService();
        if (null != mongo) {
            MongoClient client = new MongoClient();
            MongoDatabase db = client.getDatabase(MONGO_DATABASE_NAME);
            if (db != null) {
                db.drop();
            }
            client.close();
        }
        insertRefinedPayload();
    }

    @After
    public void tearDown() throws Exception {
        if (null != mongo) {
            MongoClient client = new MongoClient();
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
        //insert documents into db
        MongoClient client = new MongoClient();
        MongoDatabase db = client.getDatabase(MONGO_DATABASE_NAME);
        if (db != null) {
            db.createCollection(MONGO_COLLECTION);
            MongoCollection<Document> collection = db.getCollection(MONGO_COLLECTION);
            String contents = FileUtils.readFileToString(Paths.get("src/test/resources/mongo/distinct_payload.json").toFile());
            BasicDBList parse = (BasicDBList) JSON.parse(contents);
            for(Object dbObj : parse) {
                collection.insertOne(new Document(((BasicDBObject)dbObj).toMap()));
            }
        }
        client.close();
    }

    @Test
    public void testDistinct() throws Exception {

        final TestRunner runner = TestRunners.newTestRunner(new DistinctMongo());
        addMongoService(runner);
        runner.setProperty(MongoProps.DATABASE, MONGO_DATABASE_NAME);
        runner.setProperty(MongoProps.COLLECTION, MONGO_COLLECTION);
        runner.setProperty(DistinctMongo.DISTINCT_FIELD, "${test_attribute}");

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        ff = session.putAttribute(ff, "test_attribute", "a");

        runner.enqueue(ff);
        runner.run();

        runner.assertTransferCount(AbstractMongoProcessor.REL_FAILURE, 0);
        runner.assertTransferCount(DistinctMongo.REL_NO_RESULT_SUCCESS, 0);
        runner.assertTransferCount(AbstractMongoProcessor.REL_SUCCESS, 4);

        String[] vals = { "one", "1.0", "{ \"a\" : 1 }", "two" };

        for(MockFlowFile mockFlowFile : runner.getFlowFilesForRelationship(AbstractMongoProcessor.REL_SUCCESS)) {
            String content = new String(mockFlowFile.toByteArray(), StandardCharsets.UTF_8);
            assertTrue("FlowFile contains distinct value "+content, Arrays.asList(vals).contains(content));
        }
    }

}