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

import com.google.common.collect.ImmutableMap;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static com.asymmetrik.nifi.processors.stats.AbstractStatsProcessor.DEFAULT_MOMENT_AGGREGATOR_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class CalculateLatencyStatisticsTest {

    private final byte[] data = new byte[]{'x'};
    private final String attribute = "time";
    private final Map<String, String> attributes = ImmutableMap.of(attribute, Long.toString(System.currentTimeMillis()));

    private TestRunner runner;

    @Before
    public void setup() {
        runner = TestRunners.newTestRunner(CalculateLatencyStatistics.class);
        runner.setProperty(CalculateLatencyStatistics.ATTR_NAME, attribute);
        runner.setProperty(AbstractStatsProcessor.REPORTING_INTERVAL, "1 s");
        runner.setProperty(AbstractStatsProcessor.BATCH_SIZE, "100");
        runner.assertValid();
    }

    @Test
    public void testMissingAttributeName() {
        TestRunner runner = TestRunners.newTestRunner(CalculateLatencyStatistics.class);
        runner.assertNotValid();
    }

    @Test
    public void testValidInput() throws Exception {
        // run once to begin the reporting interval timer
        runner.run();

        // sleep for the remainder of the reporting interval
        Thread.sleep(1000);

        // send 20 files through
        for (int i = 0; i < 20; i++) {
            runner.enqueue(data, attributes);
        }
        runner.run();

        // all 20 originals emitted
        runner.assertTransferCount(AbstractStatsProcessor.REL_ORIGINAL, 20);
        for (MockFlowFile f : runner.getFlowFilesForRelationship(AbstractStatsProcessor.REL_ORIGINAL)) {
            f.assertContentEquals(data);
        }

        // 1 stats report emitted
        runner.assertTransferCount(AbstractStatsProcessor.REL_STATS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AbstractStatsProcessor.REL_STATS).get(0);
        assertEquals(0, flowFile.getSize());
        assertStatAttributesPresent(flowFile);
    }

    @Test
    public void testNoInput() throws Exception {
        // run once to begin the reporting interval timer
        runner.run();

        // sleep for the remainder of the reporting interval
        Thread.sleep(1000);

        // run again after the reporting interval
        runner.run();

        // no input, so nothing emitted to ORIGINAL
        runner.assertTransferCount(AbstractStatsProcessor.REL_ORIGINAL, 0);

        // processor does not emit stats if there is no data
        runner.assertTransferCount(AbstractStatsProcessor.REL_STATS, 0);
    }

    @Test
    public void testInputFollowedByNone() throws Exception {
        // run once to begin the reporting interval timer
        runner.run();

        // sleep for the remainder of the reporting interval
        Thread.sleep(1000);

        // send 20 files through
        for (int i = 0; i < 20; i++) {
            runner.enqueue(data, attributes);
            runner.run();
        }

        // all 20 originals emitted, 1 stats file
        runner.assertTransferCount(AbstractStatsProcessor.REL_ORIGINAL, 20);
        runner.assertTransferCount(AbstractStatsProcessor.REL_STATS, 1);

        // reset state
        runner.clearTransferState();

        // sleep for another reporting interval
        Thread.sleep(1000);

        // run again after the reporting interval
        runner.run();

        // no new input, so nothing emitted to ORIGINAL
        runner.assertTransferCount(AbstractStatsProcessor.REL_ORIGINAL, 0);

        // processor does not emit stats if there is no data
        runner.assertTransferCount(AbstractStatsProcessor.REL_STATS, 0);
    }

    @Test
    public void testInputMissingAttributeName() throws Exception {
        runner.setProperty(CalculateLatencyStatistics.ATTR_NAME, "DOES_NOT_EXIST");
        runner.assertValid();

        for (int i = 0; i < 20; i++) {
            runner.enqueue(data, attributes);
        }
        runner.run();

        // nothing was aggregated, so no stats emitted
        runner.assertTransferCount(AbstractStatsProcessor.REL_STATS, 0);

        // nothing was aggregated, so originals are emitted without additional stat attributes
        runner.assertTransferCount(AbstractStatsProcessor.REL_ORIGINAL, 20);
        for (MockFlowFile f : runner.getFlowFilesForRelationship(AbstractStatsProcessor.REL_ORIGINAL)) {
            f.assertContentEquals(data);
            assertStatAttributesNotPresent(f);
        }
    }

    @Test
    public void testSingleCorrelationAttribute() {
        runner.setProperty(CalculateLatencyStatistics.CORRELATION_ATTR, "flowId");
        runner.assertValid();
        Map<String, String> attrs = new HashMap<>(attributes);
        for (int i = 0; i < 20; i++) {
            attrs.put("flowId", "conway");
            runner.enqueue(data, attrs);
        }
        runner.run();
        runner.assertTransferCount(AbstractStatsProcessor.REL_STATS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AbstractStatsProcessor.REL_STATS).get(0);
        assertStatAttributesPresent(flowFile);
        assertEquals("conway", flowFile.getAttribute("AbstractStatsProcessor.correlationKey"));
    }

    @Test
    public void testTwoCorrelationAttributeValues() {
        runner.setProperty(CalculateLatencyStatistics.CORRELATION_ATTR, "flowId");
        runner.assertValid();

        Map<String, String> attrs = new HashMap<>(attributes);
        for (int i = 0; i < 10; i++) {
            attrs.put("flowId", "conway");
            runner.enqueue(data, attrs);
        }

        for (int i = 0; i < 5; i++) {
            attrs.put("flowId", "victor");
            runner.enqueue(data, attrs);
        }
        runner.run();
        runner.assertTransferCount(AbstractStatsProcessor.REL_STATS, 2);

        MockFlowFile conwayFlowFile = runner.getFlowFilesForRelationship(AbstractStatsProcessor.REL_STATS).get(0);
        assertStatAttributesPresent(conwayFlowFile);
        assertEquals(10, Integer.parseInt(conwayFlowFile.getAttribute("CalculateLatencyStatistics.count")));
        assertEquals("conway", conwayFlowFile.getAttribute("AbstractStatsProcessor.correlationKey"));

        MockFlowFile foobarFlowFile = runner.getFlowFilesForRelationship(AbstractStatsProcessor.REL_STATS).get(1);
        assertStatAttributesPresent(foobarFlowFile);
        assertEquals(5, Integer.parseInt(foobarFlowFile.getAttribute("CalculateLatencyStatistics.count")));
        assertEquals("victor", foobarFlowFile.getAttribute("AbstractStatsProcessor.correlationKey"));
    }

    @Test
    public void testSomeFlowfilesMissingCorrelationAttributeValues() {
        runner.setProperty(CalculateLatencyStatistics.CORRELATION_ATTR, "flowId");
        runner.assertValid();

        Map<String, String> attrs = new HashMap<>(attributes);
        for (int i = 0; i < 10; i++) {
            attrs.put("flowId", "foobar");
            runner.enqueue(data, attrs);
        }
        for (int i = 0; i < 10; i++) {
            runner.enqueue(data, attributes);
        }
        runner.run();
        runner.assertTransferCount(AbstractStatsProcessor.REL_STATS, 2);

        MockFlowFile foobarFlowFile = runner.getFlowFilesForRelationship(AbstractStatsProcessor.REL_STATS).get(0);
        assertStatAttributesPresent(foobarFlowFile);
        assertEquals(10, Integer.parseInt(foobarFlowFile.getAttribute("CalculateLatencyStatistics.count")));
        assertEquals(DEFAULT_MOMENT_AGGREGATOR_KEY, foobarFlowFile.getAttribute("AbstractStatsProcessor.correlationKey"));

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AbstractStatsProcessor.REL_STATS).get(1);
        assertStatAttributesPresent(flowFile);
        assertEquals(10, Integer.parseInt(flowFile.getAttribute("CalculateLatencyStatistics.count")));
        assertEquals("foobar", flowFile.getAttribute("AbstractStatsProcessor.correlationKey"));
    }

    private void assertStatAttributesPresent(MockFlowFile f) {
        assertNotNull(f.getAttribute("CalculateLatencyStatistics.count"));
        assertNotNull(f.getAttribute("CalculateLatencyStatistics.sum"));
        assertNotNull(f.getAttribute("CalculateLatencyStatistics.min"));
        assertNotNull(f.getAttribute("CalculateLatencyStatistics.max"));
        assertNotNull(f.getAttribute("CalculateLatencyStatistics.avg"));
        assertNotNull(f.getAttribute("CalculateLatencyStatistics.stdev"));
        assertNotNull(f.getAttribute("CalculateLatencyStatistics.timestamp"));
        assertEquals("Seconds", f.getAttribute("CalculateLatencyStatistics.units"));
    }

    private void assertStatAttributesNotPresent(MockFlowFile f) {
        assertNull(f.getAttribute("CalculateLatencyStatistics.count"));
        assertNull(f.getAttribute("CalculateLatencyStatistics.sum"));
        assertNull(f.getAttribute("CalculateLatencyStatistics.min"));
        assertNull(f.getAttribute("CalculateLatencyStatistics.max"));
        assertNull(f.getAttribute("CalculateLatencyStatistics.avg"));
        assertNull(f.getAttribute("CalculateLatencyStatistics.stdev"));
        assertNull(f.getAttribute("CalculateLatencyStatistics.timestamp"));
        assertNull(f.getAttribute("CalculateLatencyStatistics.units"));
    }
}