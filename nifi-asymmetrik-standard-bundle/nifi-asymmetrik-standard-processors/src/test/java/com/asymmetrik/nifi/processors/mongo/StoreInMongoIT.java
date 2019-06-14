/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asymmetrik.nifi.processors.mongo;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.asymmetrik.nifi.services.mongo.MongoClientService;
import com.asymmetrik.nifi.services.mongo.StandardMongoClientService;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Requires a Mongo instance running at localhost:28017
 */
public class StoreInMongoIT {
    private static final String MONGO_CONTROLLER_SERVICE = "mongo_controller_service";
    private static final String MONGO_DATABASE_NAME = "testStoreInMongo" + UUID.randomUUID();

    MongoClientService mongo;

    @Before
    public void setUp() throws Exception {
        mongo = new StandardMongoClientService();
    }

    @After
    public void tearDown() throws Exception {
        if (null != mongo) {
            MongoClient client = mongo.getMongoClient();
            if (client != null) {
                MongoDatabase db = client.getDatabase(MONGO_DATABASE_NAME);
                if (db != null) {
                    db.drop();
                }
                client.close();
            }
        }
    }

    protected void addMongoService(TestRunner runner) throws InitializationException {
        HashMap<String, String> props = new HashMap<>();

        // Add mongo controller service
        runner.addControllerService(MONGO_CONTROLLER_SERVICE, mongo, props);
        runner.setProperty(MongoProps.MONGO_SERVICE, MONGO_CONTROLLER_SERVICE);
        runner.enableControllerService(mongo);
    }

    @Test
    public void insert_refined_payload_test() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new StoreInMongo());
        addMongoService(runner);
        runner.setProperty(MongoProps.DATABASE, MONGO_DATABASE_NAME);
        runner.setProperty(MongoProps.COLLECTION, "insert_test");

        String contents = FileUtils.readFileToString(Paths.get("src/test/resources/mongo/payload.json").toFile());

        runner.enqueue(contents.getBytes());
        runner.run();

        runner.assertTransferCount(AbstractMongoProcessor.REL_FAILURE, 0);
        runner.assertTransferCount(AbstractMongoProcessor.REL_SUCCESS, 1);

        // Verify Wrapped Payload
        MockFlowFile out = runner.getFlowFilesForRelationship(AbstractMongoProcessor.REL_SUCCESS).get(0);
        BasicDBObject actual = (BasicDBObject) JSON.parse(new String(out.toByteArray(), StandardCharsets.UTF_8));
        assertNotNull(actual.getString("d"));
    }

    @Test
    public void testMixOfSuccessAndFailure() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new StoreInMongo());
        addMongoService(runner);
        runner.setProperty(MongoProps.DATABASE, MONGO_DATABASE_NAME);
        runner.setProperty(MongoProps.COLLECTION, "insert_test");
        runner.setProperty(MongoProps.BATCH_SIZE, "50");

        String successOne = FileUtils.readFileToString(Paths.get("src/test/resources/mongo/payload.json").toFile());
        runner.enqueue(successOne.getBytes());
        runner.enqueue("bad payload".getBytes());
        runner.enqueue("{\"a\":\"a\"}".getBytes());

        runner.run();

        runner.assertTransferCount(AbstractMongoProcessor.REL_FAILURE, 1);
        runner.assertTransferCount(AbstractMongoProcessor.REL_SUCCESS, 2);

        FlowFile failure = runner.getFlowFilesForRelationship(AbstractMongoProcessor.REL_FAILURE).get(0);
        String parseException = failure.getAttribute("mongo.exception");
        assertTrue(StringUtils.isNotBlank(parseException));

    }

    @Test
    public void testDuplicateKeyFailureOrderedFalse() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new StoreInMongo());
        addMongoService(runner);
        runner.setProperty(MongoProps.DATABASE, MONGO_DATABASE_NAME);
        runner.setProperty(MongoProps.COLLECTION, "insert_test");
        runner.setProperty(MongoProps.BATCH_SIZE, "10");
        runner.setProperty(MongoProps.ORDERED, "false");

        ObjectId objectId = ObjectId.get();

        String success = FileUtils.readFileToString(Paths.get("src/test/resources/mongo/payload.json").toFile());
        String successTwo = "{\"a\":\"a\"}";
        String payloadOne = "{\"_id\":\"" + objectId.toString() + "\", \"text\": \"first value\"}";
        String payloadTwo = "{\"_id\":\"" + objectId.toString() + "\", \"text\": \"second value\"}";

        runner.enqueue(payloadOne.getBytes());
        runner.enqueue(success.getBytes());
        runner.enqueue(payloadTwo.getBytes());
        runner.enqueue(successTwo.getBytes()); // add another successful message

        runner.run();

        runner.assertTransferCount(AbstractMongoProcessor.REL_FAILURE, 1);
        runner.assertTransferCount(AbstractMongoProcessor.REL_SUCCESS, 3);

        FlowFile failure = runner.getFlowFilesForRelationship(AbstractMongoProcessor.REL_FAILURE).get(0);
        String errorCode = failure.getAttribute("mongo.errorcode");
        assertEquals("11000", errorCode); // duplicate key error code
        String errorMessage = failure.getAttribute("mongo.errormessage");
        assertTrue(StringUtils.isNotBlank(errorMessage));

        // Test contents of mongo
        MongoCollection<Document> collection = mongo.getMongoClient().getDatabase(MONGO_DATABASE_NAME).getCollection("insert_test");
        assertEquals(3L, collection.count());
    }

    @Test
    public void testDuplicateKeyFailureOrderedTrue() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new StoreInMongo());
        addMongoService(runner);
        runner.setProperty(MongoProps.DATABASE, MONGO_DATABASE_NAME);
        runner.setProperty(MongoProps.COLLECTION, "insert_test");
        runner.setProperty(MongoProps.BATCH_SIZE, "10");
        runner.setProperty(MongoProps.ORDERED, "true");

        ObjectId objectId = ObjectId.get();

        String success = FileUtils.readFileToString(Paths.get("src/test/resources/mongo/payload.json").toFile());
        String successTwo = "{\"a\":\"a\"}";
        String payloadOne = "{\"_id\":\"" + objectId.toString() + "\", \"text\": \"first value\"}";
        String payloadTwo = "{\"_id\":\"" + objectId.toString() + "\", \"text\": \"second value\"}";

        runner.enqueue(payloadOne.getBytes());
        runner.enqueue(success.getBytes());
        runner.enqueue(payloadTwo.getBytes());
        runner.enqueue(successTwo.getBytes()); // add another successful message

        runner.run(4);

        runner.assertTransferCount(AbstractMongoProcessor.REL_FAILURE, 2);
        runner.assertTransferCount(AbstractMongoProcessor.REL_SUCCESS, 2);

        List<MockFlowFile> failures = runner.getFlowFilesForRelationship(AbstractMongoProcessor.REL_FAILURE);
        FlowFile failure1 = failures.get(0);
        String errorCode1 = failure1.getAttribute("mongo.errorcode");
        assertEquals("11000", errorCode1); // duplicate key error code
        String errorMessage1 = failure1.getAttribute("mongo.errormessage");
        assertTrue(StringUtils.isNotBlank(errorMessage1));

        // FlowFile failure2 = failures.get(1);
        // String errorMessage2 = failure2.getAttribute("storeinmongo.error");
        // assertTrue(StringUtils.isNotBlank(errorMessage2));
        // String mongoErrorCode2 = failure2.getAttribute("mongo.errorcode");
        // assertTrue(StringUtils.isBlank(mongoErrorCode2));

        // Test contents of mongo
        MongoCollection<Document> collection = mongo.getMongoClient().getDatabase(MONGO_DATABASE_NAME).getCollection("insert_test");
        assertEquals(2L, collection.count());
    }

    @Test
    public void testDuplicateKeyFailureWithoutBatching() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new StoreInMongo());
        addMongoService(runner);
        runner.setProperty(MongoProps.DATABASE, MONGO_DATABASE_NAME);
        runner.setProperty(MongoProps.COLLECTION, "insert_test");
        runner.setProperty(MongoProps.BATCH_SIZE, "1");
        runner.setProperty(MongoProps.ORDERED, "true");

        ObjectId objectId = ObjectId.get();

        String success = FileUtils.readFileToString(Paths.get("src/test/resources/mongo/payload.json").toFile());
        String successTwo = "{\"a\":\"a\"}";
        String payloadOne = "{\"_id\":\"" + objectId.toString() + "\", \"text\": \"first value\"}";
        String payloadTwo = "{\"_id\":\"" + objectId.toString() + "\", \"text\": \"second value\"}";

        runner.enqueue(payloadOne.getBytes());
        runner.enqueue(success.getBytes());
        runner.enqueue(payloadTwo.getBytes());
        runner.enqueue(successTwo.getBytes()); // add another successful message

        runner.run(runner.getQueueSize().getObjectCount()); // run through the entire queue

        runner.assertTransferCount(AbstractMongoProcessor.REL_FAILURE, 1);
        runner.assertTransferCount(AbstractMongoProcessor.REL_SUCCESS, 3);

        FlowFile failure = runner.getFlowFilesForRelationship(AbstractMongoProcessor.REL_FAILURE).get(0);
        String errorCode = failure.getAttribute("mongo.errorcode");
        assertEquals("11000", errorCode); // duplicate key error code
        String errorMessage = failure.getAttribute("mongo.errormessage");
        assertTrue(StringUtils.isNotBlank(errorMessage));

    }

    @Test
    public void badPayload() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new StoreInMongo());
        addMongoService(runner);
        runner.setProperty(MongoProps.DATABASE, MONGO_DATABASE_NAME);
        runner.setProperty(MongoProps.COLLECTION, "insert_test");

        String contents = "Some random, non-BSON text";

        runner.enqueue(contents.getBytes());
        runner.run(1);

        runner.assertTransferCount(AbstractMongoProcessor.REL_FAILURE, 1);
        runner.assertTransferCount(AbstractMongoProcessor.REL_SUCCESS, 0);

    }

    @Test
    public void insert_test() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new StoreInMongo());
        addMongoService(runner);
        runner.setProperty(MongoProps.DATABASE, MONGO_DATABASE_NAME);
        runner.setProperty(MongoProps.COLLECTION, "insert_test");

        runner.enqueue("{\"a\":\"a\"}".getBytes());
        runner.run();

        runner.assertTransferCount(AbstractMongoProcessor.REL_FAILURE, 0);
        runner.assertTransferCount(AbstractMongoProcessor.REL_SUCCESS, 1);

        // Verify Wrapped Payload
        MockFlowFile out = runner.getFlowFilesForRelationship(AbstractMongoProcessor.REL_SUCCESS).get(0);
        BasicDBObject actual = (BasicDBObject) JSON.parse(new String(out.toByteArray(), StandardCharsets.UTF_8));
        assertEquals("a", actual.getString("a"));
    }

    @Test
    public void testNormalizeIndexName() {
        String index = "[ {\"t\":1, 's': 1} ]";
        assertTrue((AbstractMongoProcessor.INDEX_NAMESPACE_PREFIX + "t_1_s_1").equals(AbstractMongoProcessor.normalizeIndexName(index)));
        assertTrue(("namespace_t_1_s_1").equals(AbstractMongoProcessor.normalizeIndexName("namespace_", index)));
        assertTrue(("t_1_s_1").equals(AbstractMongoProcessor.normalizeIndexName("", index)));
    }

    @Test
    public void createIndexTest() throws Exception {
        StoreInMongo processor = new StoreInMongo();
        final TestRunner runner = TestRunners.newTestRunner(processor);
        addMongoService(runner);
        runner.setProperty(MongoProps.DATABASE, MONGO_DATABASE_NAME);
        runner.setProperty(MongoProps.COLLECTION, "index_test");
        String simpleIndex = "[ {\"a\": 1} ]";
        runner.setProperty(MongoProps.INDEX, simpleIndex);

        PropertyDescriptor i1 = new PropertyDescriptor.Builder().name("Compound Index").description("asdf").dynamic(true).build();
        String compoundIndex = "[ {\"t\":1, \"s\": 1} ]";
        runner.setProperty(i1, compoundIndex);

        PropertyDescriptor i2 = new PropertyDescriptor.Builder().name("TTL Index").description("asdf").dynamic(true).build();
        String ttlIndex = "[ {\"t\":1}, {\"expireAfterSeconds\": 3900} ]";
        runner.setProperty(i2, ttlIndex);

        runner.enqueue("{\"a\":\"a\"}".getBytes());
        runner.run();

        // Connect to mongo to inspect indexes
        MongoClient client = mongo.getMongoClient();
        ListIndexesIterable<Document> indexes = client.getDatabase(MONGO_DATABASE_NAME).getCollection("index_test").listIndexes();

        List<Document> indexesArray = new ArrayList<>();
        indexesArray = indexes.into(indexesArray);

        // Check for compound index
        boolean hasCompound = false;
        for (Document doc : indexesArray) {
            if (doc.get("name").toString().equals(AbstractMongoProcessor.normalizeIndexName(compoundIndex))) {
                hasCompound = true;
            }
        }
        assertTrue("Should have compound index", hasCompound);

        // Check for TTL index
        boolean hasTTL = false;
        for (Document doc : indexesArray) {
            if (doc.get("name").toString().equals(AbstractMongoProcessor.normalizeIndexName(ttlIndex))) {
                hasTTL = true;
            }
        }
        assertTrue("Should have compound index", hasTTL);

        // Check for simple index
        boolean hasSimple = false;
        for (Document doc : indexesArray) {
            if (doc.get("name").toString().equals(AbstractMongoProcessor.normalizeIndexName(simpleIndex))) {
                hasSimple = true;
            }
        }
        assertTrue("Should have compound index", hasSimple);
    }

    @Test
    public void createSimpleIndexTest() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new StoreInMongo());
        addMongoService(runner);
        runner.setProperty(MongoProps.DATABASE, MONGO_DATABASE_NAME);
        runner.setProperty(MongoProps.COLLECTION, "index_test");
        runner.setProperty(MongoProps.INDEX, "[{\"a\": 1}]");

        runner.assertValid();
        runner.run();
    }

    @Test
    public void createSimpleIndexBadTest1() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new StoreInMongo());
        addMongoService(runner);
        runner.setProperty(MongoProps.DATABASE, MONGO_DATABASE_NAME);
        runner.setProperty(MongoProps.COLLECTION, "index_test");
        runner.setProperty(MongoProps.INDEX, "[{\"a\": 1}");

        runner.assertNotValid();
    }

    @Test
    public void createSimpleIndexBadTest2() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new StoreInMongo());
        addMongoService(runner);
        runner.setProperty(MongoProps.DATABASE, MONGO_DATABASE_NAME);
        runner.setProperty(MongoProps.COLLECTION, "index_test");
        runner.setProperty(MongoProps.INDEX, "{\"a\": 1}");

        runner.assertNotValid();
    }

    @Test
    public void emptySecondaryIndexTest() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new StoreInMongo());

        addMongoService(runner);
        runner.setProperty(MongoProps.DATABASE, MONGO_DATABASE_NAME);
        runner.setProperty(MongoProps.COLLECTION, "index_test");
        // runner.setProperty(MongoProps.INDEX, "[{\"a\": 1}]");

        runner.run();

        // Connect to mongo to inspect indexes
        MongoClient client = mongo.getMongoClient();
        ListIndexesIterable<Document> indexes = client.getDatabase(MONGO_DATABASE_NAME).getCollection("index_test").listIndexes();

        List<Document> indexesArray = new ArrayList<>();
        indexesArray = indexes.into(indexesArray);

        // Check for compound index
        boolean hasCompound = false;
        for (Document doc : indexesArray) {
            if (doc.get("name").toString().equals("t_1_s_1")) {
                hasCompound = true;
            }
        }
        assertFalse("Should not have compound index", hasCompound);
    }

    @Test
    public void testKeepIndexOutsideNamespace() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(new StoreInMongo());
        addMongoService(runner);
        MongoCollection<Document> collection = mongo.getMongoClient().getDatabase(MONGO_DATABASE_NAME).getCollection("index_test");
        Document index = new Document().append("_id", "hashed");
        collection.createIndex(index);
        runner.setProperty(MongoProps.DATABASE, MONGO_DATABASE_NAME);
        runner.setProperty(MongoProps.COLLECTION, "index_test");
        runner.assertValid();
        runner.run();
        Set<String> indexNames = AbstractMongoProcessor.getIndexNames(collection);
        String normalizedIndexName = AbstractMongoProcessor.normalizeIndexName("", index.toJson());
        assertTrue(indexNames.contains(normalizedIndexName));
    }

    @Test
    public void testDropIndexInNamespace() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(new StoreInMongo());
        addMongoService(runner);
        MongoCollection<Document> collection = mongo.getMongoClient().getDatabase(MONGO_DATABASE_NAME).getCollection("index_test");
        runner.setProperty(MongoProps.DATABASE, MONGO_DATABASE_NAME);
        runner.setProperty(MongoProps.COLLECTION, "index_test");
        String index1Json = "[ {\"a\": 1} ]";
        runner.setProperty(MongoProps.INDEX, index1Json);
        runner.assertValid();
        runner.run();
        Set<String> indexNames = AbstractMongoProcessor.getIndexNames(collection);
        String normalizedIndex1Name = AbstractMongoProcessor.normalizeIndexName(index1Json);
        assertTrue(indexNames.contains(normalizedIndex1Name));

        String index2Json = "[ {\"a\": -1} ]";
        runner.setProperty(MongoProps.INDEX, index2Json);
        runner.assertValid();
        runner.run();
        indexNames = AbstractMongoProcessor.getIndexNames(collection);
        String normalizedIndex2Name = AbstractMongoProcessor.normalizeIndexName(index2Json);
        assertTrue(indexNames.contains(normalizedIndex2Name));
        assertTrue(!indexNames.contains(normalizedIndex1Name));
    }
}
