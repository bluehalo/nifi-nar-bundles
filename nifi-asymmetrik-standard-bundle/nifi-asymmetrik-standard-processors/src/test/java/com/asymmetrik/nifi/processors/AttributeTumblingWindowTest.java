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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static com.asymmetrik.nifi.processors.AttributeTumblingWindow.*;
import static org.junit.Assert.assertEquals;

public class AttributeTumblingWindowTest {

    private TestRunner runner;

    @Before
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(AttributeTumblingWindow.class);
        runner.setProperty(VALUE_TO_TRACK, "${foobar}");
        runner.setProperty(TIME_WINDOW, "2 s");
    }

    @Test
    public void nonElapsed() {
        Map<String, String> attributes = ImmutableMap.of("foobar", "1");

        runner.enqueue(new byte[]{}, attributes);
        runner.enqueue(new byte[]{}, attributes);
        runner.enqueue(new byte[]{}, attributes);
        runner.enqueue(new byte[]{}, attributes);
        runner.enqueue(new byte[]{}, attributes);

        runner.run(5);

        runner.assertTransferCount(ELAPSED, 0);
        runner.assertTransferCount(SUCCESS, 5);
        runner.assertTransferCount(FAILURE, 0);
    }

    @Test
    public void oneElapsed() throws InterruptedException, IOException {
        Map<String, String> attributes = ImmutableMap.of("foobar", "2");

        runner.enqueue(new byte[]{}, attributes);
        runner.enqueue(new byte[]{}, attributes);
        runner.enqueue(new byte[]{}, attributes);
        runner.enqueue(new byte[]{}, attributes);
        runner.enqueue(new byte[]{}, attributes);
        runner.enqueue(new byte[]{}, attributes);

        runner.run(5, false);

        StateMap beforeElapse = runner.getStateManager().getState(Scope.LOCAL);

        Thread.sleep(2000);
        runner.run(1, false, false);

        StateMap afterElapse = runner.getStateManager().getState(Scope.LOCAL);

        List<MockFlowFile> elapsed = runner.getFlowFilesForRelationship(ELAPSED);
        assertEquals(1, elapsed.size());
        runner.assertTransferCount(SUCCESS, 6);
        runner.assertTransferCount(FAILURE, 0);

        assertEquals("10.0", elapsed.get(0).getAttribute(TUMBLING_WINDOW_SUM_KEY));
        assertEquals("5", elapsed.get(0).getAttribute(TUMBLING_WINDOW_COUNT_KEY));

        assertEquals("10.0", beforeElapse.get(TUMBLING_WINDOW_SUM_KEY));
        assertEquals("5", beforeElapse.get(TUMBLING_WINDOW_COUNT_KEY));
        assertEquals("2.0", afterElapse.get(TUMBLING_WINDOW_SUM_KEY));
        assertEquals("1", afterElapse.get(TUMBLING_WINDOW_COUNT_KEY));
    }

    @Test
    public void multipleElapsed() throws InterruptedException {
        Map<String, String> attributes = ImmutableMap.of("foobar", "2");

        runner.enqueue(new byte[]{}, attributes);
        runner.enqueue(new byte[]{}, attributes);
        runner.enqueue(new byte[]{}, attributes);
        runner.enqueue(new byte[]{}, attributes);
        runner.enqueue(new byte[]{}, attributes);
        runner.enqueue(new byte[]{}, attributes);

        runner.run(1, false);
        Thread.sleep(2000);
        // since processor is running serially, after the first flowfile, timer gets reset
        runner.run(2, false, false);
        Thread.sleep(2000);
        runner.run(3, false, false);

        List<MockFlowFile> elapsed = runner.getFlowFilesForRelationship(ELAPSED);
        assertEquals(2, elapsed.size());
        runner.assertTransferCount(SUCCESS, 6);
        runner.assertTransferCount(FAILURE, 0);

        assertEquals("2.0", elapsed.get(0).getAttribute(TUMBLING_WINDOW_SUM_KEY));
        assertEquals("1", elapsed.get(0).getAttribute(TUMBLING_WINDOW_COUNT_KEY));
        assertEquals("4.0", elapsed.get(1).getAttribute(TUMBLING_WINDOW_SUM_KEY));
        assertEquals("2", elapsed.get(1).getAttribute(TUMBLING_WINDOW_COUNT_KEY));
    }

    @Test
    public void failure() throws InterruptedException {
        Map<String, String> attributes = ImmutableMap.of("foobar", "2");
        Map<String, String> badAttributes = ImmutableMap.of("foobar", "foo");

        runner.enqueue(new byte[]{}, attributes);
        // this causes a failure but the timer shouldn't reset
        runner.enqueue(new byte[]{}, badAttributes);
        // this should should still trigger timer elapsed so we'll get a ELAPSED flowfile
        runner.enqueue(new byte[]{}, attributes);

        runner.run(1, false);           // SUCCESS
        Thread.sleep(2000);
        runner.run(1, false, false);    // FAILURE
        runner.run(1, false, false);    // ELAPSED

        runner.assertTransferCount(ELAPSED, 1);
        runner.assertTransferCount(SUCCESS, 2);
        runner.assertTransferCount(FAILURE, 1);
    }
}