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
        runner.setProperty(CalculateVolumeStatistics.BUCKET_INTERVAL, "1 s");
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
        assertEquals(1, Integer.parseInt(flowFile.getAttribute("CalculateVolumeStatistics.min")));
        assertEquals(1, Integer.parseInt(flowFile.getAttribute("CalculateVolumeStatistics.max")));
        assertEquals(1, Integer.parseInt(flowFile.getAttribute("CalculateVolumeStatistics.avg")));
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
        assertNotNull(Integer.parseInt(f.getAttribute("CalculateVolumeStatistics.min")));
        assertNotNull(Integer.parseInt(f.getAttribute("CalculateVolumeStatistics.max")));
        assertNotNull(Integer.parseInt(f.getAttribute("CalculateVolumeStatistics.avg")));
        assertNotNull(Double.parseDouble(f.getAttribute("CalculateVolumeStatistics.stdev")));
        assertNotNull(Long.parseLong(f.getAttribute("CalculateVolumeStatistics.timestamp")));
        assertEquals("Count/Second", f.getAttribute("CalculateVolumeStatistics.units"));
    }
}