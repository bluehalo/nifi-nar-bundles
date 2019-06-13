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

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CalculateVolumeStatisticsTest {

    private final byte[] data = new byte[]{'x'};

    private TestRunner runner;

    @Before
    public void setup() {
        runner = TestRunners.newTestRunner(CalculateVolumeStatistics.class);
        runner.setProperty(AbstractStatsProcessor.REPORTING_INTERVAL, "1 s");
        runner.setProperty(AbstractStatsProcessor.BATCH_SIZE, "100");
        runner.assertValid();
    }

    @Test
    public void testValidInput() throws Exception {
        // run once to begin the reporting interval timer
        runner.run();

        // sleep for the remainder of the reporting interval
        Thread.sleep(1000);

        // send 20 files through
        for (int i = 0; i < 20; i++) {
            runner.enqueue(data);
            runner.run();
        }

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
        assertEquals(1, Integer.parseInt(flowFile.getAttribute("CalculateVolumeStatistics.count")));
        assertEquals(1, Integer.parseInt(flowFile.getAttribute("CalculateVolumeStatistics.sum")));
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
            runner.enqueue(data);
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

    private void assertStatAttributesPresent(MockFlowFile f) {
        assertNotNull(Integer.parseInt(f.getAttribute("CalculateVolumeStatistics.count")));
        assertNotNull(Integer.parseInt(f.getAttribute("CalculateVolumeStatistics.sum")));
        assertNotNull(Long.parseLong(f.getAttribute("CalculateVolumeStatistics.timestamp")));
    }
}