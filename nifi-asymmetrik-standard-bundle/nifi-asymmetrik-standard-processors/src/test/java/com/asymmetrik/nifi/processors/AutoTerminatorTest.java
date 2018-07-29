package com.asymmetrik.nifi.processors;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class AutoTerminatorTest {

    @Test
    public void testBulkSingle() {
        final TestRunner runner = TestRunners.newTestRunner(new AutoTerminator());
        runner.setProperty(AutoTerminator.BULK, "1");
        runner.enqueue("sample1".getBytes());
        runner.enqueue("sample2".getBytes());
        runner.enqueue("sample3".getBytes());
        runner.run();
        runner.assertQueueNotEmpty();
        runner.run();
        runner.assertQueueNotEmpty();
        runner.run();
        runner.assertQueueEmpty();
    }

    @Test
    public void testBulk() {
        final TestRunner runner = TestRunners.newTestRunner(new AutoTerminator());
        runner.setProperty(AutoTerminator.BULK, "10");
        runner.enqueue("sample1".getBytes());
        runner.enqueue("sample2".getBytes());
        runner.enqueue("sample3".getBytes());
        runner.run();
        runner.assertQueueEmpty();
    }
}
