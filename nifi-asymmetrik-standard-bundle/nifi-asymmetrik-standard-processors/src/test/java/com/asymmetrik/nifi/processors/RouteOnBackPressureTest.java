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

public class RouteOnBackPressureTest {

    @Test
    public void testBackPressured() {
        final TestRunner runner = TestRunners.newTestRunner(RouteOnBackPressure.class);
        runner.assertValid();

        for (int i = 0; i < 100; ++i) {
            runner.enqueue(new byte[0]);
        }

        runner.run(100);

        runner.assertTransferCount(RouteOnBackPressure.PASS_THROUGH, 100);
        runner.assertTransferCount(RouteOnBackPressure.BACK_PRESSURED, 0);

        runner.setRelationshipUnavailable(RouteOnBackPressure.PASS_THROUGH);

        for (int i = 0; i < 100; ++i) {
            runner.enqueue(new byte[0]);
        }

        runner.run(100);

        runner.assertTransferCount(RouteOnBackPressure.PASS_THROUGH, 100);
        runner.assertTransferCount(RouteOnBackPressure.BACK_PRESSURED, 100);
    }
}
