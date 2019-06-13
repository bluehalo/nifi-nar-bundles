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
package com.asymmetrik.nifi.processors.stats;

import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class CalculateAttributeMomentsTest {

    private static final String FLOWID = "flowId";

    private TestRunner runner;

    @Before
    public void setup() {
        runner = TestRunners.newTestRunner(CalculateAttributeMoments.class);
        runner.setProperty(CalculateAttributeMoments.PROP_ATTR_NAME, "x");
        runner.setProperty(CalculateAttributeMoments.CORRELATION_ATTR, FLOWID);
        runner.setProperty(CalculateAttributeMoments.REPORTING_INTERVAL, "1 s");
        runner.setProperty(CalculateAttributeMoments.BATCH_SIZE, "20");
        runner.assertValid();
    }

    @Test
    public void testSingleFlowId() {
        String data = "a";
        int n = 20;
        for (int i = 0; i < n; i++) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(FLOWID, "foobar");
            attributes.put("x", String.valueOf(i));
            runner.enqueue(data, attributes);
        }
        runner.run(20);

        // all 20 originals emitted, 1 stats file
        runner.assertTransferCount(AbstractStatsProcessor.REL_STATS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AbstractStatsProcessor.REL_STATS).get(0);

        assertEquals(n, Integer.parseInt(flowFile.getAttribute("CalculateAttributeMoments.count")));
        assertEquals(190.0, Double.parseDouble(flowFile.getAttribute("CalculateAttributeMoments.sum")), 1e-6);
        assertEquals(0.0, Double.parseDouble(flowFile.getAttribute("CalculateAttributeMoments.min")), 1e-6);
        assertEquals(19.0, Double.parseDouble(flowFile.getAttribute("CalculateAttributeMoments.max")), 1e-6);
        assertEquals(9.5, Double.parseDouble(flowFile.getAttribute("CalculateAttributeMoments.avg")), 1e-6);
    }

    @Test
    public void testMissingAttrValue() {
        String data = "a";
        int n = 20;
        for (int i = 0; i < n; i++) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(FLOWID, "foobar");
            runner.enqueue(data, attributes);
        }
        runner.run(20);

        // all 20 originals emitted, but no statistics were generated
        runner.assertTransferCount(AbstractStatsProcessor.REL_STATS, 0);
    }

    @Test
    public void testNonnumericAttrValue() {
        String data = "a";
        int n = 20;
        for (int i = 0; i < n; i++) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(FLOWID, "foobar");
            attributes.put("x", "nan");
            runner.enqueue(data, attributes);
        }
        runner.run(20);

        // all 20 originals emitted, but no statistics were generated
        runner.assertTransferCount(AbstractStatsProcessor.REL_STATS, 0);
    }

    @Test
    public void testMultipleFlowId() {
        runner.setProperty(CalculateAttributeMoments.BATCH_SIZE, "40");
        String data = "a";
        int n = 20;
        for (int i = 0; i < n; i++) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(FLOWID, "foo");
            attributes.put("x", String.valueOf(i));
            runner.enqueue(data, attributes);

            attributes = new HashMap<>();
            attributes.put(FLOWID, "bar");
            attributes.put("x", String.valueOf(i));
            runner.enqueue(data, attributes);
        }
        runner.run(2*n);

        // all 20 originals emitted, 1 stats file
        runner.assertTransferCount(AbstractStatsProcessor.REL_STATS, 2);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AbstractStatsProcessor.REL_STATS).get(0);
        assertEquals(n, Integer.parseInt(flowFile.getAttribute("CalculateAttributeMoments.count")));
        assertEquals(190.0, Double.parseDouble(flowFile.getAttribute("CalculateAttributeMoments.sum")), 1e-6);
        assertEquals(0.0, Double.parseDouble(flowFile.getAttribute("CalculateAttributeMoments.min")), 1e-6);
        assertEquals(19.0, Double.parseDouble(flowFile.getAttribute("CalculateAttributeMoments.max")), 1e-6);
        assertEquals(9.5, Double.parseDouble(flowFile.getAttribute("CalculateAttributeMoments.avg")), 1e-6);

        flowFile = runner.getFlowFilesForRelationship(AbstractStatsProcessor.REL_STATS).get(1);
        assertEquals(n, Integer.parseInt(flowFile.getAttribute("CalculateAttributeMoments.count")));
        assertEquals(190.0, Double.parseDouble(flowFile.getAttribute("CalculateAttributeMoments.sum")), 1e-6);
        assertEquals(0.0, Double.parseDouble(flowFile.getAttribute("CalculateAttributeMoments.min")), 1e-6);
        assertEquals(19.0, Double.parseDouble(flowFile.getAttribute("CalculateAttributeMoments.max")), 1e-6);
        assertEquals(9.5, Double.parseDouble(flowFile.getAttribute("CalculateAttributeMoments.avg")), 1e-6);
    }
}