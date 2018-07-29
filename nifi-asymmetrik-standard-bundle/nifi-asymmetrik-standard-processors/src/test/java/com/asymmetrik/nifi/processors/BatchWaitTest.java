package com.asymmetrik.nifi.processors;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class BatchWaitTest {

    @Test
    public void testBulkSingle() {
        final TestRunner runner = TestRunners.newTestRunner(new BatchWait());
        runner.setProperty(BatchWait.BATCH_SIZE, "1");
        runner.enqueue("sample1");
        runner.enqueue("sample2");
        runner.enqueue("sample3");
        runner.run();
        runner.assertQueueNotEmpty();
        runner.run();
        runner.assertQueueNotEmpty();
        runner.run();
        runner.assertQueueEmpty();
    }

    @Test
    public void testBulk() {
        final TestRunner runner = TestRunners.newTestRunner(new BatchWait());
        runner.setProperty(BatchWait.BATCH_SIZE, "10");
        runner.enqueue("sample1");
        runner.enqueue("sample2");
        runner.enqueue("sample3");
        runner.run();
        runner.assertQueueEmpty();
    }

    @Test
    public void testAllSplitsInSingleBatch() {
        final TestRunner runner = TestRunners.newTestRunner(new BatchWait());
        runner.setProperty(BatchWait.BATCH_SIZE, "5");

        String firstId = UUID.randomUUID().toString();

        enqueueFlowFileFragment(runner, firstId, "3");
        enqueueFlowFileFragment(runner, firstId, "3");
        enqueueFlowFileFragment(runner, firstId, "3");

        runner.run();
        runner.assertQueueEmpty();

        runner.assertTransferCount(BatchWait.COMPLETED, 1);
        runner.assertTransferCount(BatchWait.ORIGINAL, 3);
        runner.assertTransferCount(BatchWait.FAILURE, 0);

    }

    @Test
    public void testMixSplitsInSingleBatch() {
        final TestRunner runner = TestRunners.newTestRunner(new BatchWait());
        runner.setProperty(BatchWait.BATCH_SIZE, "5");

        String firstId = UUID.randomUUID().toString();
        String secondId = UUID.randomUUID().toString();

        enqueueFlowFileFragment(runner, firstId, "3");
        enqueueFlowFileFragment(runner, firstId, "3");
        enqueueFlowFileFragment(runner, firstId, "3");
        enqueueFlowFileFragment(runner, secondId, "5");
        enqueueFlowFileFragment(runner, secondId, "5");

        runner.run();
        runner.assertQueueEmpty();

        // Only "abc" should be completed. "def" should not be completed yet
        runner.assertTransferCount(BatchWait.COMPLETED, 1);
        runner.assertTransferCount(BatchWait.ORIGINAL, 5);
        runner.assertTransferCount(BatchWait.FAILURE, 0);

    }

    @Test
    public void testMixSplitsAcrossMultipleRuns() {
        final TestRunner runner = TestRunners.newTestRunner(new BatchWait());
        runner.setProperty(BatchWait.BATCH_SIZE, "5");

        String firstId = UUID.randomUUID().toString();
        String secondId = UUID.randomUUID().toString();

        enqueueFlowFileFragment(runner, firstId, "3");
        enqueueFlowFileFragment(runner, firstId, "3");
        runner.run();
        runner.assertQueueEmpty();
        runner.assertTransferCount(BatchWait.COMPLETED, 0);
        runner.assertTransferCount(BatchWait.ORIGINAL, 2);
        runner.assertTransferCount(BatchWait.FAILURE, 0);

        enqueueFlowFileFragment(runner, firstId, "3");
        enqueueFlowFileFragment(runner, secondId, "5");
        enqueueFlowFileFragment(runner, secondId, "5");

        runner.run();
        runner.assertQueueEmpty();

        // Only "abc" should be completed. "def" should not be completed yet
        runner.assertTransferCount(BatchWait.COMPLETED, 1);
        runner.assertTransferCount(BatchWait.ORIGINAL, 5);
        runner.assertTransferCount(BatchWait.FAILURE, 0);

    }

    @Test
    public void testSplitsWithDifferentAttributesAndFailureConditions() {
        final TestRunner runner = TestRunners.newTestRunner(new BatchWait());
        runner.setProperty(BatchWait.BATCH_SIZE, "10");
        runner.setProperty(BatchWait.FRAGMENT_COUNT, "top.fragment.count");
        runner.setProperty(BatchWait.FRAGMENT_IDENTIFIER, "top.fragment.identifier");

        String firstId = UUID.randomUUID().toString();
        String secondId = UUID.randomUUID().toString();
        String thirdId = UUID.randomUUID().toString();

        enqueueFlowFileFragment(runner, firstId, "3", "top.fragment.identifier", "top.fragment.count");
        enqueueFlowFileFragment(runner, firstId, "3", "top.fragment.identifier", "top.fragment.count");
        runner.run();
        runner.assertQueueEmpty();
        runner.assertTransferCount(BatchWait.COMPLETED, 0);
        runner.assertTransferCount(BatchWait.ORIGINAL, 2);
        runner.assertTransferCount(BatchWait.FAILURE, 0);

        enqueueFlowFileFragment(runner, firstId, "3", "top.fragment.identifier", "top.fragment.count");
        enqueueFlowFileFragment(runner, secondId, "5", "top.fragment.identifier", "top.fragment.count");
        enqueueFlowFileFragment(runner, secondId, "5", "top.fragment.identifier", "top.fragment.count");

        // Should route to Failure because it doesn't have the required attributes
        enqueueFlowFileFragment(runner, thirdId, "5", "other.attribute", "does.not.exist");

        // Should route to Failure because it doesn't have a number in the count attribute
        enqueueFlowFileFragment(runner, thirdId, "NaN", "top.fragment.identifier", "top.fragment.count");

        runner.run();
        runner.assertQueueEmpty();

        // Only "abc" should be completed. "def" should not be completed yet
        runner.assertTransferCount(BatchWait.COMPLETED, 1);
        runner.assertTransferCount(BatchWait.ORIGINAL, 7);
        runner.assertTransferCount(BatchWait.FAILURE, 2);
    }

    public void enqueueFlowFileFragment(TestRunner runner, String identifier, String count) {
        enqueueFlowFileFragment(runner, identifier, count, "fragment.identifier", "fragment.count");
    }

    public void enqueueFlowFileFragment(TestRunner runner, String identifier, String count, String identifierAttribute, String countAttribute) {
        Map<String, String> firstAttributes = new HashMap<>();
        firstAttributes.put(identifierAttribute, identifier);
        firstAttributes.put(countAttribute, count);
        runner.enqueue("data", firstAttributes);
    }

}
