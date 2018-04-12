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

        int count = n;
        assertEquals(count, Integer.parseInt(flowFile.getAttribute("CalculateAttributeMoments.count")));
        assertEquals(190.0, Double.parseDouble(flowFile.getAttribute("CalculateAttributeMoments.sum")), 1e-6);
        assertEquals(0.0, Double.parseDouble(flowFile.getAttribute("CalculateAttributeMoments.min")), 1e-6);
        assertEquals(19.0, Double.parseDouble(flowFile.getAttribute("CalculateAttributeMoments.max")), 1e-6);
        assertEquals(9.5, Double.parseDouble(flowFile.getAttribute("CalculateAttributeMoments.avg")), 1e-6);
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