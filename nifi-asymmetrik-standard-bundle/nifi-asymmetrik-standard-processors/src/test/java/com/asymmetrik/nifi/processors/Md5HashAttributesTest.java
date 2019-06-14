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

import java.util.List;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class Md5HashAttributesTest {

    @Test
    public void singleAttributeTest() {
        TestRunner runner = TestRunners.newTestRunner(Md5HashAttributes.class);
        runner.setProperty("hash", "${attribute1}");

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        ff = session.putAttribute(ff, "attribute1", "pbs.twimg.com/media/ClvFsHiUgAE4fT6.jpg");
        runner.enqueue(ff);
        runner.run();


        // All results were processed with out failure
        runner.assertQueueEmpty();

        runner.assertTransferCount(Md5HashAttributes.REL_SUCCESS, 1);

        // If you need to read or do additional tests on results you can access the content
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(Md5HashAttributes.REL_SUCCESS);
        assertTrue("1 match", results.size() == 1);
        MockFlowFile result = results.get(0);

        result.assertAttributeEquals("hash", "e2c26a2479f497562a615a42ecb1ef7e");
    }


    @Test
    public void multipleAttributeTest() {
        TestRunner runner = TestRunners.newTestRunner(Md5HashAttributes.class);
        runner.setProperty("hash", "${attribute1}");
        runner.setProperty("hash2", "${attribute2}");

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        ff = session.putAttribute(ff, "attribute1", "pbs.twimg.com/media/ClvFsHiUgAE4fT6.jpg");
        ff = session.putAttribute(ff, "attribute2", "");

        runner.enqueue(ff);
        runner.run();


        // All results were processed with out failure
        runner.assertQueueEmpty();

        runner.assertTransferCount(Md5HashAttributes.REL_SUCCESS, 1);

        // If you need to read or do additional tests on results you can access the content
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(Md5HashAttributes.REL_SUCCESS);
        assertTrue("1 match", results.size() == 1);
        MockFlowFile result = results.get(0);

        result.assertAttributeEquals("hash", "e2c26a2479f497562a615a42ecb1ef7e");
        result.assertAttributeEquals("hash2", "d41d8cd98f00b204e9800998ecf8427e");

    }
}