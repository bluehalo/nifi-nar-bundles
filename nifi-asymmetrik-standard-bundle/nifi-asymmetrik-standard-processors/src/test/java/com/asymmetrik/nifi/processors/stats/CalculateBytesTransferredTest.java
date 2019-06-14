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

import static org.junit.Assert.assertEquals;

public class CalculateBytesTransferredTest {

    private static final String FLOWID = "flowId";

    private TestRunner runner;

    @Before
    public void setup() {
        runner = TestRunners.newTestRunner(CalculateBytesTransferred.class);
        runner.setProperty(CalculateBytesTransferred.CORRELATION_ATTR, FLOWID);
        runner.setProperty(CalculateBytesTransferred.REPORTING_INTERVAL, "1 s");
        runner.setProperty(CalculateBytesTransferred.BATCH_SIZE, "20");
        runner.assertValid();
    }

    @Test
    public void testSingleFlowId() {
        String data = "a";
        int n = 20;
        for (int i = 0; i < n; i++) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(FLOWID, "foobar");
            runner.enqueue(data, attributes);
        }
        runner.run();

        // all 20 originals emitted, 1 stats file
        runner.assertTransferCount(AbstractStatsProcessor.REL_STATS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AbstractStatsProcessor.REL_STATS).get(0);

        int count = n;
        assertEquals(count, Integer.parseInt(flowFile.getAttribute("CalculateBytesTransferred.count")));
        assertEquals(count, Double.parseDouble(flowFile.getAttribute("CalculateBytesTransferred.sum")), 1e-6);
        assertEquals(1, Double.parseDouble(flowFile.getAttribute("CalculateBytesTransferred.min")), 1e-6);
        assertEquals(1, Double.parseDouble(flowFile.getAttribute("CalculateBytesTransferred.max")), 1e-6);
        assertEquals(1.0, Double.parseDouble(flowFile.getAttribute("CalculateBytesTransferred.avg")), 1e-6);
    }

    @Test
    public void testMultipleFlowId() {
        String data = "a";
        int n = 20;
        for (int i = 0; i < n; i++) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(FLOWID, "foo");
            runner.enqueue(data, attributes);
            attributes.put(FLOWID, "bar");
            runner.enqueue(data, attributes);
        }
        runner.run();

        // all 20 originals emitted, 1 stats file
        runner.assertTransferCount(AbstractStatsProcessor.REL_STATS, 2);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AbstractStatsProcessor.REL_STATS).get(0);

        int count = n/2;
        assertEquals(count, Integer.parseInt(flowFile.getAttribute("CalculateBytesTransferred.count")));
        assertEquals(count, Double.parseDouble(flowFile.getAttribute("CalculateBytesTransferred.sum")), 1e-6);
        assertEquals(1, Double.parseDouble(flowFile.getAttribute("CalculateBytesTransferred.min")), 1e-6);
        assertEquals(1, Double.parseDouble(flowFile.getAttribute("CalculateBytesTransferred.max")), 1e-6);
        assertEquals(1.0, Double.parseDouble(flowFile.getAttribute("CalculateBytesTransferred.avg")), 1e-6);
    }
}