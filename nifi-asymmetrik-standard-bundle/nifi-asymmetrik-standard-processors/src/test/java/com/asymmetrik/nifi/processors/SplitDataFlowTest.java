package com.asymmetrik.nifi.processors;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

@SuppressWarnings("Duplicates")
public class SplitDataFlowTest {

    @Test
    public void enqueueTooFew() {

        final TestRunner runner = TestRunners.newTestRunner(new SplitDataFlow());
        runner.setProperty(SplitDataFlow.PERCENTAGE, "50");

        int total = 10;
        String payload = "{\"a\": \"a\"}";
        for (int i = 0; i < total; i++) {
            runner.enqueue(payload.getBytes());
            runner.run(1);
        }

        runner.assertTransferCount(SplitDataFlow.REL_A, 10);
        runner.assertTransferCount(SplitDataFlow.REL_B, 0);
    }

    @Test
    public void enqueueLessThan100() {

        final TestRunner runner = TestRunners.newTestRunner(new SplitDataFlow());
        runner.setProperty(SplitDataFlow.PERCENTAGE, "30");

        int total = 70;
        String payload = "{\"a\": \"a\"}";
        for (int i = 0; i < total; i++) {
            runner.enqueue(payload.getBytes());
            runner.run(1);
        }

        runner.assertTransferCount(SplitDataFlow.REL_A, 30);
        runner.assertTransferCount(SplitDataFlow.REL_B, 40);
    }

    @Test
    public void enqueueMoreThan100() {

        final TestRunner runner = TestRunners.newTestRunner(new SplitDataFlow());
        runner.setProperty(SplitDataFlow.PERCENTAGE, "25");

        int total = 110;
        String payload = "{\"a\": \"a\"}";
        for (int i = 0; i < total; i++) {
            runner.enqueue(payload.getBytes());
            runner.run(1);
        }

        runner.assertTransferCount(SplitDataFlow.REL_A, 35);
        runner.assertTransferCount(SplitDataFlow.REL_B, 75);
    }

    @Test
    public void zeroPercentage() {

        final TestRunner runner = TestRunners.newTestRunner(new SplitDataFlow());
        runner.setProperty(SplitDataFlow.PERCENTAGE, "0");

        int total = 170;
        String payload = "{\"a\": \"a\"}";
        for (int i = 0; i < total; i++) {
            runner.enqueue(payload.getBytes());
            runner.run(1);
        }

        runner.assertTransferCount(SplitDataFlow.REL_A, 0);
        runner.assertTransferCount(SplitDataFlow.REL_B, 170);
    }

    @Test
    public void oneHundredPercentage() {

        final TestRunner runner = TestRunners.newTestRunner(new SplitDataFlow());
        runner.setProperty(SplitDataFlow.PERCENTAGE, "100");

        int total = 170;
        String payload = "{\"a\": \"a\"}";
        for (int i = 0; i < total; i++) {
            runner.enqueue(payload.getBytes());
            runner.run(1);
        }

        runner.assertTransferCount(SplitDataFlow.REL_A, 170);
        runner.assertTransferCount(SplitDataFlow.REL_B, 0);
    }

    @Test
    public void percentageLessThan0() {

        final TestRunner runner = TestRunners.newTestRunner(new SplitDataFlow());
        runner.setProperty(SplitDataFlow.PERCENTAGE, "-10");

        int total = 170;
        String payload = "{\"a\": \"a\"}";
        for (int i = 0; i < total; i++) {
            runner.enqueue(payload.getBytes());
            runner.run(1);
        }

        runner.assertTransferCount(SplitDataFlow.REL_A, 0);
        runner.assertTransferCount(SplitDataFlow.REL_B, 170);
    }
}
