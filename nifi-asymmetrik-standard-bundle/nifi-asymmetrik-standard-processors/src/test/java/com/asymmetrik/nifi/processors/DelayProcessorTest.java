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
