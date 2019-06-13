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
