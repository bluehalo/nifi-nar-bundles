package com.asymmetrik.nifi.processors;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AttributesToJsonContentTest {

    @Test
    public void testJsonAttributes() {
        TestRunner runner = TestRunners.newTestRunner(new AttributesToJsonContent());
        runner.setProperty(AttributesToJsonContent.ATTRIBUTES_LIST, "Vision, Array  ,  ,, , String  , Integer  ,Double  ");
        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        // Create a JSON attribute
        JsonObject root = new JsonObject();
        root.addProperty("foo", "bar");
        root.addProperty("score", 0.8453);
        ff = session.putAttribute(ff, "Vision", root.toString());
        ff = session.putAttribute(ff, "Array", "[ \"a\", \"b\"]");
        ff = session.putAttribute(ff, "String", "hello, I'm a string");
        ff = session.putAttribute(ff, "Integer", "1");
        ff = session.putAttribute(ff, "Double", "3.14");

        runner.enqueue(ff);
        runner.run();

        runner.assertTransferCount(AttributesToJsonContent.REL_SUCCESS, 1);
        runner.assertTransferCount(AttributesToJsonContent.REL_ORIGINAL, 1);
        MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(AttributesToJsonContent.REL_SUCCESS).get(0);

        // Try to parse content as JSON
        JsonParser parser = new JsonParser();
        JsonObject content = parser.parse(new String(mockFlowFile.toByteArray())).getAsJsonObject();

        assertTrue(content.get("Array").isJsonArray());
        assertEquals(content.get("Integer").getAsInt(), 1);
        assertEquals(content.get("String").getAsString(), "hello, I'm a string");
        assertEquals(content.get("Double").getAsDouble(), 3.14, 0.0001);

        assertTrue(content.get("Vision").isJsonObject());
        JsonObject vision = content.getAsJsonObject("Vision");
        assertEquals(vision.get("foo").getAsString(), "bar");
        assertEquals(vision.get("score").getAsDouble(), 0.8453, 0.00001);
    }

    @Test
    public void testInvalidJsonAttributes() {
        TestRunner runner = TestRunners.newTestRunner(new AttributesToJsonContent());
        runner.setProperty(AttributesToJsonContent.ATTRIBUTES_LIST, "DNE, invalid_json, empty");
        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        // Create a JSON attribute
        ff = session.putAttribute(ff, "invalid_json", "{\"foo\":\"-}");
        ff = session.putAttribute(ff, "empty", "");

        runner.enqueue(ff);
        runner.run();
        runner.assertTransferCount(AttributesToJsonContent.REL_FAILURE, 1);
    }

    @Test
    public void testEmptyStringAttribute() {
        TestRunner runner = TestRunners.newTestRunner(new AttributesToJsonContent());
        runner.setProperty(AttributesToJsonContent.ATTRIBUTES_LIST, "DNE, invalid_json, empty");
        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        // Create a JSON attribute
        ff = session.putAttribute(ff, "empty", "");

        runner.enqueue(ff);
        runner.run();
        runner.assertTransferCount(AttributesToJsonContent.REL_SUCCESS, 1);
        runner.assertTransferCount(AttributesToJsonContent.REL_ORIGINAL, 1);
    }

    @Test
    public void testNoAttributesFound() {
        TestRunner runner = TestRunners.newTestRunner(new AttributesToJsonContent());
        runner.setProperty(AttributesToJsonContent.ATTRIBUTES_LIST, "DNE");
        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        // Create a JSON attribute
        ff = session.putAttribute(ff, "foo", "bar");

        runner.enqueue(ff);
        runner.run();
        runner.assertTransferCount(AttributesToJsonContent.REL_SUCCESS, 1);
        runner.assertTransferCount(AttributesToJsonContent.REL_ORIGINAL, 1);
    }
}