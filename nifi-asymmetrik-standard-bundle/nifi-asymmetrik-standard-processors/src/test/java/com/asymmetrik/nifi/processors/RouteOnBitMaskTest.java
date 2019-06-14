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

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

import com.google.common.collect.ImmutableMap;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static com.asymmetrik.nifi.processors.RouteOnBitMask.FLIP_BIT;
import static org.junit.Assert.assertEquals;

public class RouteOnBitMaskTest {
    private TestRunner runner;

    @Before
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(RouteOnBitMask.class);
        runner.setProperty(RouteOnBitMask.ATTRIBUTE_NAME, "foo");
    }

    @Test
    public void testBasicSingleBitMatch() {
        String attr = binary("101100");
        String mask = binary("000100");

        runner.setProperty("matched", mask);
        runner.enqueue("x", ImmutableMap.of("foo", attr));
        runner.run();

        runner.assertAllFlowFilesTransferred("matched", 1);

        MockFlowFile out = runner.getFlowFilesForRelationship("matched").get(0);
        validate(out, "x", attr);
    }

    @Test
    public void testBasicSingleBitUnmatch() {
        String attr = binary("101100");
        String mask = binary("010000");

        runner.setProperty("no_match", mask);
        runner.enqueue("x", ImmutableMap.of("foo", attr));
        runner.run();

        runner.assertAllFlowFilesTransferred(RouteOnBitMask.UNMATCHED, 1);

        MockFlowFile out = runner.getFlowFilesForRelationship(RouteOnBitMask.UNMATCHED).get(0);
        validate(out, "x", attr);
    }

    @Test
    public void testBasicMultiBitMatch() {
        String attr = binary("101100");
        String mask = binary("001100");

        runner.setProperty("matched", mask);
        runner.enqueue("x", ImmutableMap.of("foo", attr));
        runner.run();

        runner.assertAllFlowFilesTransferred("matched", 1);

        MockFlowFile out = runner.getFlowFilesForRelationship("matched").get(0);
        validate(out, "x", attr);
    }

    @Test
    public void testBasicMultiBitUnmatch() {
        String attr = binary("101100");
        String mask = binary("001010");

        runner.setProperty("no_match", mask);
        runner.enqueue("x", ImmutableMap.of("foo", attr));
        runner.run();

        runner.assertAllFlowFilesTransferred(RouteOnBitMask.UNMATCHED, 1);

        MockFlowFile out = runner.getFlowFilesForRelationship(RouteOnBitMask.UNMATCHED).get(0);
        validate(out, "x", attr);
    }

    @Test
    public void testFlipSingleBit() {
        String attr = binary("101100");
        String mask = binary("000100");

        runner.setProperty(FLIP_BIT, "true");
        runner.setProperty("matched", mask);
        runner.enqueue("x", ImmutableMap.of("foo", attr));
        runner.run();

        runner.assertAllFlowFilesTransferred("matched", 1);
        MockFlowFile out = runner.getFlowFilesForRelationship("matched").get(0);
        validate(out, "x", binary("101000"));
    }

    @Test
    public void testFlipBitMultiBitMatch() {
        String attr = binary("101100");
        String mask = binary("100100");

        runner.setProperty(FLIP_BIT, "true");
        runner.setProperty("matched", mask);
        runner.enqueue("x", ImmutableMap.of("foo", attr));
        runner.run();

        runner.assertAllFlowFilesTransferred("matched", 1);
        MockFlowFile out = runner.getFlowFilesForRelationship("matched").get(0);
        validate(out, "x", binary("001000"));
    }

    @Test
    public void testInvalidAttributeName() {
        String attr = binary("101100");
        String mask = binary("000100");

        runner.setProperty("matched", mask);
        runner.enqueue("x", ImmutableMap.of("bar", attr));
        runner.run();

        runner.assertAllFlowFilesTransferred(RouteOnBitMask.FAILURE, 1);
        MockFlowFile out = runner.getFlowFilesForRelationship(RouteOnBitMask.FAILURE).get(0);
        validate(out, "x", null);
    }

    @Test
    public void testNonLongAttribute() {
        String mask = binary("000100");

        runner.setProperty("matched", mask);
        runner.enqueue("x", ImmutableMap.of("foo", "hello"));
        runner.run();

        runner.assertAllFlowFilesTransferred(RouteOnBitMask.FAILURE, 1);
        MockFlowFile out = runner.getFlowFilesForRelationship(RouteOnBitMask.FAILURE).get(0);
        validate(out, "x", "hello");
    }


    @Test
    public void testEmptyAttributeBitValue() {
        String mask = binary("000100");

        runner.setProperty("matched", mask);
        runner.enqueue("x", ImmutableMap.of("foo", ""));
        runner.run();

        runner.assertAllFlowFilesTransferred(RouteOnBitMask.FAILURE, 1);
        MockFlowFile out = runner.getFlowFilesForRelationship(RouteOnBitMask.FAILURE).get(0);
        validate(out, "x", "");
    }

    @Test
    public void testNullAttributeBitValue() {
        String mask = binary("000100");

        runner.setProperty("matched", mask);
        runner.enqueue("x");
        runner.run();

        runner.assertAllFlowFilesTransferred(RouteOnBitMask.FAILURE, 1);
        MockFlowFile out = runner.getFlowFilesForRelationship(RouteOnBitMask.FAILURE).get(0);
        validate(out, "x", null);
    }

    @Test
    public void testMultiRelationshipSomeMatch() {
        String attr =    binary("101100");
        String match1 =  binary("000100");
        String noMatch = binary("010000");
        String match2 =  binary("100000");

        runner.setProperty("match1", match1);
        runner.setProperty("no_match", noMatch);
        runner.setProperty("match2", match2);
        runner.enqueue("x", ImmutableMap.of("foo", attr));
        runner.run();

        runner.assertTransferCount("match1", 1);
        runner.assertTransferCount("no_match", 0);
        runner.assertTransferCount("match2", 1);
        runner.assertTransferCount(RouteOnBitMask.UNMATCHED, 0);
        runner.assertTransferCount(RouteOnBitMask.FAILURE, 0);

        MockFlowFile out = runner.getFlowFilesForRelationship("match1").get(0);
        validate(out, "x", attr);
        out = runner.getFlowFilesForRelationship("match2").get(0);
        validate(out, "x", attr);
    }

    @Test
    public void testMultiRelationshipSomeMatchFlipBit() {
        String attr =    binary("101100");
        String match1 =  binary("000100");
        String noMatch = binary("010000");
        String match2 =  binary("100000");

        runner.setProperty(FLIP_BIT, "true");
        runner.setProperty("match1", match1);
        runner.setProperty("no_match", noMatch);
        runner.setProperty("match2", match2);
        runner.enqueue("x", ImmutableMap.of("foo", attr));
        runner.run();

        runner.assertTransferCount("match1", 1);
        runner.assertTransferCount("no_match", 0);
        runner.assertTransferCount("match2", 1);
        runner.assertTransferCount(RouteOnBitMask.UNMATCHED, 0);
        runner.assertTransferCount(RouteOnBitMask.FAILURE, 0);

        MockFlowFile out = runner.getFlowFilesForRelationship("match1").get(0);
        validate(out, "x", binary("001000"));
        out = runner.getFlowFilesForRelationship("match2").get(0);
        validate(out, "x", binary("001000"));
    }

    @Test
    public void testMultiRelationshipAllMatch() {
        String attr =   binary("101100");
        String match1 = binary("000100");
        String match2 = binary("100100");
        String match3 = binary("001000");

        runner.setProperty("match1", match1);
        runner.setProperty("match2", match2);
        runner.setProperty("match3", match3);
        runner.enqueue("x", ImmutableMap.of("foo", attr));
        runner.run();

        runner.assertTransferCount("match1", 1);
        runner.assertTransferCount("match2", 1);
        runner.assertTransferCount("match3", 1);
        runner.assertTransferCount(RouteOnBitMask.UNMATCHED, 0);
        runner.assertTransferCount(RouteOnBitMask.FAILURE, 0);

        MockFlowFile out = runner.getFlowFilesForRelationship("match1").get(0);
        validate(out, "x", attr);
        out = runner.getFlowFilesForRelationship("match2").get(0);
        validate(out, "x", attr);
        out = runner.getFlowFilesForRelationship("match3").get(0);
        validate(out, "x", attr);
    }

    @Test
    public void testMultiRelationshipNoneMatch() {
        String attr =     binary("101100");
        String noMatch1 = binary("010011");
        String noMatch2 = binary("000001");
        String noMatch3 = binary("010000");

        runner.setProperty("noMatch1", noMatch1);
        runner.setProperty("noMatch2", noMatch2);
        runner.setProperty("noMatch3", noMatch3);
        runner.enqueue("x", ImmutableMap.of("foo", attr));
        runner.run();

        runner.assertTransferCount("noMatch1", 0);
        runner.assertTransferCount("noMatch2", 0);
        runner.assertTransferCount("noMatch3", 0);
        runner.assertTransferCount(RouteOnBitMask.UNMATCHED, 1);
        runner.assertTransferCount(RouteOnBitMask.FAILURE, 0);

        MockFlowFile out = runner.getFlowFilesForRelationship(RouteOnBitMask.UNMATCHED).get(0);
        validate(out, "x", attr);
    }

    @Test
    public void testMultiRelationshipFailureAttr() {
        String attr =   "hello";
        String match1 = binary("00010");
        String match2 = binary("100100");
        String match3 = binary("001000");

        runner.setProperty("match1", match1);
        runner.setProperty("match2", match2);
        runner.setProperty("match3", match3);
        runner.enqueue("x", ImmutableMap.of("foo", attr));
        runner.run();

        runner.assertTransferCount("match1", 0);
        runner.assertTransferCount("match2", 0);
        runner.assertTransferCount("match3", 0);
        runner.assertTransferCount(RouteOnBitMask.UNMATCHED, 0);
        runner.assertTransferCount(RouteOnBitMask.FAILURE, 1);

        MockFlowFile out = runner.getFlowFilesForRelationship(RouteOnBitMask.FAILURE).get(0);
        validate(out, "x", attr);
    }

    @Test
    public void testMultiRelationshipFlipBit() {
        String attr =   binary("01111");
        String match1 = binary("00001");
        String match2 = binary("00100");
        String match3 = binary("00101");

        runner.setProperty(FLIP_BIT, "true");
        runner.setProperty("match1", match1);
        runner.setProperty("match2", match2);
        runner.setProperty("match3", match3);
        runner.enqueue("x", ImmutableMap.of("foo", attr));
        runner.run();

        runner.assertTransferCount("match1", 1);
        runner.assertTransferCount("match2", 1);
        runner.assertTransferCount("match3", 1);
        runner.assertTransferCount(RouteOnBitMask.UNMATCHED, 0);
        runner.assertTransferCount(RouteOnBitMask.FAILURE, 0);

        MockFlowFile out = runner.getFlowFilesForRelationship("match1").get(0);
        validate(out, "x", binary("01010"));
        out = runner.getFlowFilesForRelationship("match2").get(0);
        validate(out, "x", binary("01010"));
        out = runner.getFlowFilesForRelationship("match3").get(0);
        validate(out, "x", binary("01010"));
    }

    private void validate(MockFlowFile flowFile, String expectedContent, String expectedFooValue) {
        String content = new String(flowFile.toByteArray(), StandardCharsets.UTF_8);
        assertEquals(expectedContent, content);

        String result = flowFile.getAttribute("foo");
        assertEquals(expectedFooValue, result);
    }

    private String binary(String binary) {
        return String.valueOf(new BigInteger(binary, 2).longValue());
    }
}