package com.asymmetrik.nifi.processors;

import com.google.common.collect.ImmutableMap;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DelayProcessorTest {

    @Test
    public void testValid() {
        TestRunner runner = TestRunners.newTestRunner(DelayProcessor.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(DelayProcessor.TIME_PERIOD, "10 sec");

        runner.assertValid();
    }

    @Test
    public void testInvalid() {
        TestRunner runner = TestRunners.newTestRunner(DelayProcessor.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(DelayProcessor.TIME_PERIOD, "10 kb");

        runner.assertNotValid();
    }

    @Test
    public void testSuccess() throws InterruptedException {
        TestRunner runner = TestRunners.newTestRunner(DelayProcessor.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(DelayProcessor.TIME_PERIOD, "1 sec");

        runner.enqueue(new byte[0], ImmutableMap.of(CoreAttributes.FILENAME.key(), "file1"));

        runner.run();
        // nothing transferred, 1 file still on queue and cached
        runner.assertTransferCount(DelayProcessor.REL_SUCCESS, 0);
        assertEquals(1, runner.getQueueSize().getObjectCount());
        assertEquals(1, ((DelayProcessor) runner.getProcessor()).delayMap.size());

        // wait until delay has passed
        Thread.sleep(1010);

        runner.run();
        // file transferred, nothing on queue, cache empty
        runner.assertAllFlowFilesTransferred(DelayProcessor.REL_SUCCESS, 1);
        assertEquals(0, ((DelayProcessor) runner.getProcessor()).delayMap.size());
    }

    @Test
    public void testExpireMultipleFilesSimultaneously() throws InterruptedException {
        TestRunner runner = TestRunners.newTestRunner(DelayProcessor.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(DelayProcessor.TIME_PERIOD, "1 sec");

        runner.enqueue(new byte[0], ImmutableMap.of(CoreAttributes.FILENAME.key(), "file1"));
        runner.enqueue(new byte[0], ImmutableMap.of(CoreAttributes.FILENAME.key(), "file2"));

        runner.run();
        // nothing transferred, 2 files still on queue and cached
        runner.assertTransferCount(DelayProcessor.REL_SUCCESS, 0);
        assertEquals(2, runner.getQueueSize().getObjectCount());
        assertEquals(2, ((DelayProcessor) runner.getProcessor()).delayMap.size());

        runner.enqueue(new byte[0], ImmutableMap.of(CoreAttributes.FILENAME.key(), "file3"));

        // wait until delay has passed
        Thread.sleep(1010);

        runner.run();
        // 1st 2 files transferred, 3rd still on queue and cached
        runner.assertAllFlowFilesTransferred(DelayProcessor.REL_SUCCESS, 2);
        assertEquals(1, ((DelayProcessor) runner.getProcessor()).delayMap.size());

        runner.clearTransferState();

        // wait until delay has passed
        Thread.sleep(1010);
        runner.run();

        // 3rd file now transferred, cache empty
        runner.assertAllFlowFilesTransferred(DelayProcessor.REL_SUCCESS, 1);
        assertEquals(0, ((DelayProcessor) runner.getProcessor()).delayMap.size());
    }
}
