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

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RetryCounterTest {

    private TestRunner runner;

    @Before
    public void setUp() {
        runner = TestRunners.newTestRunner(RetryCounter.class);
    }

    @Test
    public void testBasic() {
        runner.setProperty(RetryCounter.CONDITIONAL_EXPRESSION, "${retry.counter:lt(3)}");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(RetryCounter.RETRY, 1);
        runner.assertPenalizeCount(1);
        assertEquals("1", runner.getPenalizedFlowFiles().get(0).getAttribute("retry.counter"));
        runner.assertTransferCount(RetryCounter.RETRY, 1);
        assertEquals("1", runner.getFlowFilesForRelationship(RetryCounter.RETRY).get(0).getAttribute("retry.counter"));
        runner.clearTransferState();

        runner.enqueue(new byte[0], ImmutableMap.of("retry.counter", "1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(RetryCounter.RETRY, 1);
        runner.assertPenalizeCount(2);
        assertEquals("2", runner.getPenalizedFlowFiles().get(1).getAttribute("retry.counter"));
        runner.assertTransferCount(RetryCounter.RETRY, 1);
        assertEquals("2", runner.getFlowFilesForRelationship(RetryCounter.RETRY).get(0).getAttribute("retry.counter"));
        runner.clearTransferState();

        runner.enqueue(new byte[0], ImmutableMap.of("retry.counter", "2"));
        runner.run();

        runner.assertAllFlowFilesTransferred(RetryCounter.RETRY, 1);
        runner.assertPenalizeCount(3);
        assertEquals("3", runner.getPenalizedFlowFiles().get(2).getAttribute("retry.counter"));
        runner.assertTransferCount(RetryCounter.RETRY, 1);
        assertEquals("3", runner.getFlowFilesForRelationship(RetryCounter.RETRY).get(0).getAttribute("retry.counter"));
        runner.clearTransferState();

        runner.enqueue(new byte[0], ImmutableMap.of("retry.counter", "3"));
        runner.run();

        runner.assertAllFlowFilesTransferred(RetryCounter.EXCEEDED, 1);
        runner.assertPenalizeCount(3);
        assertEquals(3, runner.getPenalizedFlowFiles().size());
        assertEquals(1, runner.getFlowFilesForRelationship(RetryCounter.EXCEEDED).size());
        assertEquals("3", runner.getFlowFilesForRelationship(RetryCounter.EXCEEDED).get(0).getAttribute("retry.counter"));
   }

    @Test
    public void testZeroIteration() {
        runner.setProperty(RetryCounter.CONDITIONAL_EXPRESSION, "${retry.counter:lt(0)}");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(RetryCounter.EXCEEDED, 1);
        runner.assertPenalizeCount(0);
        assertEquals("0", runner.getFlowFilesForRelationship(RetryCounter.EXCEEDED).get(0).getAttribute("retry.counter"));
    }

    @Test
    public void testSingleIteration() {
        runner.setProperty(RetryCounter.CONDITIONAL_EXPRESSION, "${retry.counter:lt(1)}");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(RetryCounter.RETRY, 1);
        runner.assertPenalizeCount(1);
        assertEquals("1", runner.getPenalizedFlowFiles().get(0).getAttribute("retry.counter"));
        runner.assertTransferCount(RetryCounter.RETRY, 1);
        assertEquals("1", runner.getFlowFilesForRelationship(RetryCounter.RETRY).get(0).getAttribute("retry.counter"));
        runner.clearTransferState();

        runner.enqueue(new byte[0], ImmutableMap.of("retry.counter", "1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(RetryCounter.EXCEEDED, 1);
        runner.assertPenalizeCount(1);
        assertEquals("1", runner.getFlowFilesForRelationship(RetryCounter.EXCEEDED).get(0).getAttribute("retry.counter"));
    }

    @Test
    public void testAttributes() {
        runner.setProperty(RetryCounter.CONDITIONAL_EXPRESSION, "${retry.counter:lt(1)}");

        runner.setProperty("foo", "bar");
        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(RetryCounter.RETRY, 1);
        assertNull(runner.getFlowFilesForRelationship(RetryCounter.RETRY).get(0).getAttribute("foo"));

        runner.clearTransferState();

        runner.setProperty("foo", "bar");
        runner.enqueue(new byte[0], ImmutableMap.of("retry.counter", "1"));
        runner.run();

        runner.assertAllFlowFilesTransferred(RetryCounter.EXCEEDED, 1);
        assertEquals("bar", runner.getFlowFilesForRelationship(RetryCounter.EXCEEDED).get(0).getAttribute("foo"));
    }
}