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

public class CreateContentTest {

    @Test
    public void payloadWithoutSubstitutions() {

        String payload = "{\"a\": \"a\"}";
        final TestRunner runner = TestRunners.newTestRunner(new CreateContent());
        runner.setProperty(CreateContent.CONTENT_FIELD, payload);

        runner.enqueue(payload.getBytes());
        runner.run();

        runner.assertTransferCount(CreateContent.REL_FAILURE, 0);
        runner.assertTransferCount(CreateContent.REL_SUCCESS, 1);
        runner.assertTransferCount(CreateContent.REL_ORIGINAL, 1);

        final MockFlowFile out = runner.getFlowFilesForRelationship(CreateContent.REL_SUCCESS).get(0);
        assertEquals(payload, new String(out.toByteArray()));
    }

    @Test
    public void payloadWithSubstitutions() {

        String payload = "{\"a\": {{from}}, \"pi\": {{pi}}, \"boolean\": {{boolean}}}";
        final TestRunner runner = TestRunners.newTestRunner(new CreateContent());
        runner.setProperty(CreateContent.CONTENT_FIELD, payload);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        Map<String, String> props = new HashMap<>();
        props.put("from", "\"test@example.com\"");
        props.put("pi", "3.14");
        props.put("boolean", "true");
        ff = session.putAllAttributes(ff, props);
        runner.enqueue(ff);
        runner.run();

        runner.assertTransferCount(CreateContent.REL_FAILURE, 0);
        runner.assertTransferCount(CreateContent.REL_SUCCESS, 1);
        runner.assertTransferCount(CreateContent.REL_ORIGINAL, 1);

        String expected = "{\"a\": \"test@example.com\", \"pi\": 3.14, \"boolean\": true}";
        final MockFlowFile out = runner.getFlowFilesForRelationship(CreateContent.REL_SUCCESS).get(0);
        assertEquals(expected, new String(out.toByteArray()));
    }

    @Test
    public void payloadWithMissingAttributes() {

        String payload = "{\"a\": {{from}}, \"pi\": {{pi}}, \"boolean\": {{boolean}}}";
        final TestRunner runner = TestRunners.newTestRunner(new CreateContent());
        runner.setProperty(CreateContent.CONTENT_FIELD, payload);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        Map<String, String> props = new HashMap<>();
        props.put("from", "\"test@example.com\"");
        props.put("pi", "3.14");
        ff = session.putAllAttributes(ff, props);
        runner.enqueue(ff);
        runner.run();

        runner.assertTransferCount(CreateContent.REL_FAILURE, 0);
        runner.assertTransferCount(CreateContent.REL_SUCCESS, 1);
        runner.assertTransferCount(CreateContent.REL_ORIGINAL, 1);

        String expected = "{\"a\": \"test@example.com\", \"pi\": 3.14, \"boolean\": null}";
        MockFlowFile out = runner.getFlowFilesForRelationship(CreateContent.REL_SUCCESS).get(0);
        String actual = new String(out.toByteArray());
        assertEquals(expected, actual);
    }

    @Test
    public void payloadWithTokenAttributes() {

        String payload = "{\"a\": {{from}}, \"pi\": {{pi}}, \"text\": {{text}}}";
        final TestRunner runner = TestRunners.newTestRunner(new CreateContent());
        runner.setProperty(CreateContent.CONTENT_FIELD, payload);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        Map<String, String> props = new HashMap<>();
        props.put("from", "\"test@example.com\"");
        props.put("pi", "3.14");
        props.put("text", "\"here is some {{text}}\"");
        ff = session.putAllAttributes(ff, props);
        runner.enqueue(ff);
        runner.run();

        runner.assertTransferCount(CreateContent.REL_FAILURE, 0);
        runner.assertTransferCount(CreateContent.REL_SUCCESS, 1);
        runner.assertTransferCount(CreateContent.REL_ORIGINAL, 1);

        String expected = "{\"a\": \"test@example.com\", \"pi\": 3.14, \"text\": \"here is some {{text}}\"}";
        MockFlowFile out = runner.getFlowFilesForRelationship(CreateContent.REL_SUCCESS).get(0);
        String actual = new String(out.toByteArray());
        assertEquals(expected, actual);
    }

    @Test
    public void payloadWithSpecialCharacters() {

        String payload = "{\"a\": {{from}}, \"pi\": {{pi}}, \"text\": {{text}}}";
        final TestRunner runner = TestRunners.newTestRunner(new CreateContent());
        runner.setProperty(CreateContent.CONTENT_FIELD, payload);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        Map<String, String> props = new HashMap<>();
        props.put("from", "\"test@example.com\"");
        props.put("pi", "3.14");
        props.put("text", "\"here is some &$^()}{{}+_!@#\"");
        ff = session.putAllAttributes(ff, props);
        runner.enqueue(ff);
        runner.run();

        runner.assertTransferCount(CreateContent.REL_FAILURE, 0);
        runner.assertTransferCount(CreateContent.REL_SUCCESS, 1);
        runner.assertTransferCount(CreateContent.REL_ORIGINAL, 1);

        String expected = "{\"a\": \"test@example.com\", \"pi\": 3.14, \"text\": \"here is some &$^()}{{}+_!@#\"}";
        MockFlowFile out = runner.getFlowFilesForRelationship(CreateContent.REL_SUCCESS).get(0);
        String actual = new String(out.toByteArray());
        assertEquals(expected, actual);
    }

    @Test
    public void payloadWithDoubleQuotes() {

        String payload = "{\"a\": {{from}}, \"pi\": {{pi}}, \"text\": {{text}}}";
        final TestRunner runner = TestRunners.newTestRunner(new CreateContent());
        runner.setProperty(CreateContent.CONTENT_FIELD, payload);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        Map<String, String> props = new HashMap<>();
        props.put("from", "\"test@example.com\"");
        props.put("pi", "3.14");
        props.put("text", "\"here is some \"text\"\"");
        ff = session.putAllAttributes(ff, props);
        runner.enqueue(ff);
        runner.run();

        runner.assertTransferCount(CreateContent.REL_FAILURE, 0);
        runner.assertTransferCount(CreateContent.REL_SUCCESS, 1);
        runner.assertTransferCount(CreateContent.REL_ORIGINAL, 1);

        String expected = "{\"a\": \"test@example.com\", \"pi\": 3.14, \"text\": \"here is some \"text\"\"}";
        MockFlowFile out = runner.getFlowFilesForRelationship(CreateContent.REL_SUCCESS).get(0);
        String actual = new String(out.toByteArray());
        assertEquals(expected, actual);
    }

    @Test
    public void payloadWithSpacesWithinDelimiters() {

        String payload = "{\"a\": {{ from}}, \"pi\": {{pi }}, \"text\": {{ text }}}";
        final TestRunner runner = TestRunners.newTestRunner(new CreateContent());
        runner.setProperty(CreateContent.CONTENT_FIELD, payload);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        Map<String, String> props = new HashMap<>();
        props.put("from", "\"test@example.com\"");
        props.put("pi", "3.14");
        props.put("text", "\"here is some text\"");
        ff = session.putAllAttributes(ff, props);
        runner.enqueue(ff);
        runner.run();

        runner.assertTransferCount(CreateContent.REL_FAILURE, 0);
        runner.assertTransferCount(CreateContent.REL_SUCCESS, 1);
        runner.assertTransferCount(CreateContent.REL_ORIGINAL, 1);

        String expected = "{\"a\": \"test@example.com\", \"pi\": 3.14, \"text\": \"here is some text\"}";
        MockFlowFile out = runner.getFlowFilesForRelationship(CreateContent.REL_SUCCESS).get(0);
        String actual = new String(out.toByteArray());
        assertEquals(expected, actual);
    }

    @Test
    public void payloadWithSpanishCharacters() {

        String payload = "{\"a\": {{from}}, \"pi\": {{pi}}, \"text\": {{text}}}";
        final TestRunner runner = TestRunners.newTestRunner(new CreateContent());
        runner.setProperty(CreateContent.CONTENT_FIELD, payload);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        Map<String, String> props = new HashMap<>();
        props.put("from", "\"test@example.com\"");
        props.put("pi", "3.14");
        props.put("text", "\"Mañana se me va un amigo capas el mejor que tuve y que voy a tener, te re quiero turrasdf Pasa que cada cosa que pienso/hago quiero contarle a mi él.  Quiero gelatina y la bastarda no se hace mas -.-\"");
        ff = session.putAllAttributes(ff, props);
        runner.enqueue(ff);
        runner.run();

        runner.assertTransferCount(CreateContent.REL_FAILURE, 0);
        runner.assertTransferCount(CreateContent.REL_SUCCESS, 1);
        runner.assertTransferCount(CreateContent.REL_ORIGINAL, 1);

        String expected = "{\"a\": \"test@example.com\", \"pi\": 3.14, \"text\": \"Mañana se me va un amigo capas el mejor que tuve y que voy a tener, te re quiero turrasdf Pasa que cada cosa que pienso/hago quiero contarle a mi él.  Quiero gelatina y la bastarda no se hace mas -.-\"}";
        MockFlowFile out = runner.getFlowFilesForRelationship(CreateContent.REL_SUCCESS).get(0);
        String actual = new String(out.toByteArray());
        assertEquals(expected, actual);
    }

    @Test
    public void payloadWithUnicode() {
        StringBuffer sb = new StringBuffer();
        sb.append("\"");
        sb.append(Character.toChars(127467));
        sb.append(Character.toChars(127479));
        sb.append("\"");

        String payload = "{\"a\": {{from}}, \"pi\": {{pi}}, \"text\": {{text}}}";
        final TestRunner runner = TestRunners.newTestRunner(new CreateContent());
        runner.setProperty(CreateContent.CONTENT_FIELD, payload);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        Map<String, String> props = new HashMap<>();
        props.put("from", "\"test@example.com\"");
        props.put("pi", "3.14");
        props.put("text", sb.toString());
        ff = session.putAllAttributes(ff, props);
        runner.enqueue(ff);
        runner.run();

        runner.assertTransferCount(CreateContent.REL_FAILURE, 0);
        runner.assertTransferCount(CreateContent.REL_SUCCESS, 1);
        runner.assertTransferCount(CreateContent.REL_ORIGINAL, 1);

        String expected = "{\"a\": \"test@example.com\", \"pi\": 3.14, \"text\": " + sb.toString() + "}";
        MockFlowFile out = runner.getFlowFilesForRelationship(CreateContent.REL_SUCCESS).get(0);
        String actual = new String(out.toByteArray());
        assertEquals(expected, actual);
    }
}
