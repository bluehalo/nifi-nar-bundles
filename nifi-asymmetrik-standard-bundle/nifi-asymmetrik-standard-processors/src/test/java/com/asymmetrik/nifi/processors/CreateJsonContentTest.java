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
package com.asymmetrik.nifi.processors;

import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CreateJsonContentTest {

    @Test
    public void payloadWithSubstitutions() {

        String payload = "{\"a\": \"{{from}}\", \"pi\": \"{{pi:float}}\", \"boolean\": \"{{boolean:bool}}\", \"long\": \"{{long:int}}\"}";
        final TestRunner runner = TestRunners.newTestRunner(new CreateJsonContent());
        runner.setProperty(CreateJsonContent.CONTENT_FIELD, payload);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        Map<String, String> props = new HashMap<>();
        props.put("from", "test@example.com");
        props.put("pi", "3.14");
        props.put("long", "12345678901234");
        props.put("boolean", "true");
        ff = session.putAllAttributes(ff, props);
        runner.enqueue(ff);
        runner.run();

        runner.assertTransferCount(CreateJsonContent.REL_FAILURE, 0);
        runner.assertTransferCount(CreateJsonContent.REL_SUCCESS, 1);
        runner.assertTransferCount(CreateJsonContent.REL_ORIGINAL, 1);

        String expected = "{\"a\":\"test@example.com\",\"pi\":3.14,\"boolean\":true,\"long\":12345678901234}";
        final MockFlowFile out = runner.getFlowFilesForRelationship(CreateJsonContent.REL_SUCCESS).get(0);
        assertEquals(expected, new String(out.toByteArray()));
    }

    @Test
    public void payloadWithMissingAttributes() {

        String payload = "{\"a\": \"{{from}}\", \"pi\": \"{{pi:float}}\", \"boolean\": \"{{boolean:bool}}\"}";
        final TestRunner runner = TestRunners.newTestRunner(new CreateJsonContent());
        runner.setProperty(CreateJsonContent.CONTENT_FIELD, payload);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        Map<String, String> props = new HashMap<>();
        props.put("from", "test@example.com");
        props.put("pi", "3.14");
        ff = session.putAllAttributes(ff, props);
        runner.enqueue(ff);
        runner.run();

        runner.assertTransferCount(CreateJsonContent.REL_FAILURE, 0);
        runner.assertTransferCount(CreateJsonContent.REL_SUCCESS, 1);
        runner.assertTransferCount(CreateJsonContent.REL_ORIGINAL, 1);

        String expected = "{\"a\":\"test@example.com\",\"pi\":3.14,\"boolean\":null}";
        MockFlowFile out = runner.getFlowFilesForRelationship(CreateJsonContent.REL_SUCCESS).get(0);
        String actual = new String(out.toByteArray());
        assertEquals(expected, actual);
    }

    @Test
    public void payloadWithTokenAttributes() {

        String payload = "{\"a\": \"{{from}}\", \"pi\": \"{{pi:float}}\", \"text\": \"{{text}}\"}";
        final TestRunner runner = TestRunners.newTestRunner(new CreateJsonContent());
        runner.setProperty(CreateJsonContent.CONTENT_FIELD, payload);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        Map<String, String> props = new HashMap<>();
        props.put("from", "test@example.com");
        props.put("pi", "3.14");
        props.put("text", "\"here is some {{text}}\"");
        ff = session.putAllAttributes(ff, props);
        runner.enqueue(ff);
        runner.run();

        runner.assertTransferCount(CreateJsonContent.REL_FAILURE, 0);
        runner.assertTransferCount(CreateJsonContent.REL_SUCCESS, 1);
        runner.assertTransferCount(CreateJsonContent.REL_ORIGINAL, 1);

        String expected = "{\"a\":\"test@example.com\",\"pi\":3.14,\"text\":\"\\\"here is some {{text}}\\\"\"}";
        MockFlowFile out = runner.getFlowFilesForRelationship(CreateJsonContent.REL_SUCCESS).get(0);
        String actual = new String(out.toByteArray());
        assertEquals(expected, actual);
    }

    @Test
    public void payloadWithSpecialCharacters() {

        String payload = "{\"a\": \"{{from}}\", \"pi\": \"{{pi:float}}\", \"text\": \"{{text}}\"}";
        final TestRunner runner = TestRunners.newTestRunner(new CreateJsonContent());
        runner.setProperty(CreateJsonContent.CONTENT_FIELD, payload);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        Map<String, String> props = new HashMap<>();
        props.put("from", "test@example.com");
        props.put("pi", "3.14");
        props.put("text", "\"here is some &$^()}{{}+_!@#\"");
        ff = session.putAllAttributes(ff, props);
        runner.enqueue(ff);
        runner.run();

        runner.assertTransferCount(CreateJsonContent.REL_FAILURE, 0);
        runner.assertTransferCount(CreateJsonContent.REL_SUCCESS, 1);
        runner.assertTransferCount(CreateJsonContent.REL_ORIGINAL, 1);

        String expected = "{\"a\":\"test@example.com\",\"pi\":3.14,\"text\":\"\\\"here is some &$^()}{{}+_!@#\\\"\"}";
        MockFlowFile out = runner.getFlowFilesForRelationship(CreateJsonContent.REL_SUCCESS).get(0);
        String actual = new String(out.toByteArray());
        assertEquals(expected, actual);
    }

    @Test
    public void payloadWithDoubleQuotes() {

        String payload = "{\"a\": \"{{from}}\", \"pi\": \"{{pi:float}}\", \"text\": \"{{text}}\"}";
        final TestRunner runner = TestRunners.newTestRunner(new CreateJsonContent());
        runner.setProperty(CreateJsonContent.CONTENT_FIELD, payload);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        Map<String, String> props = new HashMap<>();
        props.put("from", "test@example.com");
        props.put("pi", "3.14");
        props.put("text", "here is some \"text\"");
        ff = session.putAllAttributes(ff, props);
        runner.enqueue(ff);
        runner.run();

        runner.assertTransferCount(CreateJsonContent.REL_FAILURE, 0);
        runner.assertTransferCount(CreateJsonContent.REL_SUCCESS, 1);
        runner.assertTransferCount(CreateJsonContent.REL_ORIGINAL, 1);

        String expected = "{\"a\":\"test@example.com\",\"pi\":3.14,\"text\":\"here is some \\\"text\\\"\"}";
        MockFlowFile out = runner.getFlowFilesForRelationship(CreateJsonContent.REL_SUCCESS).get(0);
        String actual = new String(out.toByteArray());
        assertEquals(expected, actual);
    }

    @Test
    public void payloadWithSpacesWithinDelimiters() {

        String payload = "{\"a\": \"{{ from}}\", \"pi\": \"{{pi }}\", \"text\": \" {{ text }} \"}";
        final TestRunner runner = TestRunners.newTestRunner(new CreateJsonContent());
        runner.setProperty(CreateJsonContent.CONTENT_FIELD, payload);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        Map<String, String> props = new HashMap<>();
        props.put("from", "test@example.com");
        props.put("pi", "3.14");
        props.put("text", "here is some text");
        ff = session.putAllAttributes(ff, props);
        runner.enqueue(ff);
        runner.run();

        runner.assertTransferCount(CreateJsonContent.REL_FAILURE, 0);
        runner.assertTransferCount(CreateJsonContent.REL_SUCCESS, 1);
        runner.assertTransferCount(CreateJsonContent.REL_ORIGINAL, 1);

        String expected = "{\"a\":\"test@example.com\",\"pi\":\"3.14\",\"text\":\"here is some text\"}";
        MockFlowFile out = runner.getFlowFilesForRelationship(CreateJsonContent.REL_SUCCESS).get(0);
        String actual = new String(out.toByteArray());
        assertEquals(expected, actual);
    }

    @Test
    public void payloadWithSpanishCharacters() {

        String payload = "{\"a\": \"{{from}}\", \"pi\": \"{{pi}}\", \"text\": \"{{text}}\"}";
        final TestRunner runner = TestRunners.newTestRunner(new CreateJsonContent());
        runner.setProperty(CreateJsonContent.CONTENT_FIELD, payload);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        Map<String, String> props = new HashMap<>();
        props.put("from", "test@example.com");
        props.put("pi", "3.14");  // This should be a string representation of the number 3.14
        props.put("text", "Mañana se me va un amigo capas el mejor que tuve y que voy a tener, te re quiero turrasdf Pasa que cada cosa que pienso/hago quiero contarle a mi él.  Quiero gelatina y la bastarda no se hace mas -.-");
        ff = session.putAllAttributes(ff, props);
        runner.enqueue(ff);
        runner.run();

        runner.assertTransferCount(CreateJsonContent.REL_FAILURE, 0);
        runner.assertTransferCount(CreateJsonContent.REL_SUCCESS, 1);
        runner.assertTransferCount(CreateJsonContent.REL_ORIGINAL, 1);

        String expected = "{\"a\":\"test@example.com\",\"pi\":\"3.14\",\"text\":\"Mañana se me va un amigo capas el mejor que tuve y que voy a tener, te re quiero turrasdf Pasa que cada cosa que pienso/hago quiero contarle a mi él.  Quiero gelatina y la bastarda no se hace mas -.-\"}";
        MockFlowFile out = runner.getFlowFilesForRelationship(CreateJsonContent.REL_SUCCESS).get(0);
        String actual = new String(out.toByteArray());
        assertEquals(expected, actual);
    }

    @Test
    public void payloadWithUnicode() {
        StringBuffer sb = new StringBuffer();
        sb.append(Character.toChars(127467));
        sb.append(Character.toChars(127479));

        String payload = "{\"a\": \"{{from}}\", \"pi\": \"{{pi:float}}\", \"text\": \"{{text}}\"}";
        final TestRunner runner = TestRunners.newTestRunner(new CreateJsonContent());
        runner.setProperty(CreateJsonContent.CONTENT_FIELD, payload);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        Map<String, String> props = new HashMap<>();
        props.put("from", "test@example.com");
        props.put("pi", "3.14");
        props.put("text", sb.toString());
        ff = session.putAllAttributes(ff, props);
        runner.enqueue(ff);
        runner.run();

        runner.assertTransferCount(CreateJsonContent.REL_FAILURE, 0);
        runner.assertTransferCount(CreateJsonContent.REL_SUCCESS, 1);
        runner.assertTransferCount(CreateJsonContent.REL_ORIGINAL, 1);

        String expected = "{\"a\":\"test@example.com\",\"pi\":3.14,\"text\":\"" + sb.toString() + "\"}";
        MockFlowFile out = runner.getFlowFilesForRelationship(CreateJsonContent.REL_SUCCESS).get(0);
        String actual = new String(out.toByteArray());
        assertEquals(expected, actual);
    }

    @Test
    public void payloadWithBadFloatSubstitution() {

        String payload = "{\"pi\": \"{{pi:float}}\"}";
        final TestRunner runner = TestRunners.newTestRunner(new CreateJsonContent());
        runner.setProperty(CreateJsonContent.CONTENT_FIELD, payload);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        Map<String, String> props = new HashMap<>();
        props.put("pi", "This is not a float!");
        ff = session.putAllAttributes(ff, props);
        runner.enqueue(ff);
        runner.run();

        runner.assertTransferCount(CreateJsonContent.REL_FAILURE, 1);
        runner.assertTransferCount(CreateJsonContent.REL_SUCCESS, 0);
        runner.assertTransferCount(CreateJsonContent.REL_ORIGINAL, 1);

        FlowFile actualFlowFile = runner.getFlowFilesForRelationship(CreateJsonContent.REL_FAILURE).get(0);
        assertEquals("$.pi", actualFlowFile.getAttribute("json.error.key"));
        assertEquals("This is not a float!", actualFlowFile.getAttribute("json.error.value"));
    }

    @Test
    public void payloadWithBadIntSubstitution() {

        String payload = "{\"time\": \"{{time:int}}\"}";
        final TestRunner runner = TestRunners.newTestRunner(new CreateJsonContent());
        runner.setProperty(CreateJsonContent.CONTENT_FIELD, payload);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        Map<String, String> props = new HashMap<>();
        props.put("time", "2015-06-22T13:45:23.000Z");
        ff = session.putAllAttributes(ff, props);
        runner.enqueue(ff);
        runner.run();

        runner.assertTransferCount(CreateJsonContent.REL_FAILURE, 1);
        runner.assertTransferCount(CreateJsonContent.REL_SUCCESS, 0);
        runner.assertTransferCount(CreateJsonContent.REL_ORIGINAL, 1);

        FlowFile actualFlowFile = runner.getFlowFilesForRelationship(CreateJsonContent.REL_FAILURE).get(0);
        assertEquals("$.time", actualFlowFile.getAttribute("json.error.key"));
        assertEquals("2015-06-22T13:45:23.000Z", actualFlowFile.getAttribute("json.error.value"));
    }
}
