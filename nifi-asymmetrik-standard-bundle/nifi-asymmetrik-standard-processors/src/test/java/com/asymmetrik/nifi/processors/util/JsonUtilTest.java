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
package com.asymmetrik.nifi.processors.util;

import java.util.Map;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class JsonUtilTest {
    @Test
    public void simple_shallow() {
        String template = "{ \"a\" : \"{{value}}\", \"b\": \"b\"}";

        JsonObject document = (JsonObject) new JsonParser().parse(template);
        Map<String, String> paths = JsonUtil.getJsonPathsForTemplating(document);

        assertEquals(1, paths.size());

        Object value = paths.get("$.a");
        assertNotNull(value);
        assertEquals("{{value}}", value.toString());
    }

    @Test
    public void double_shallow() {
        String template = "{ \"a\" : \"{{value}}\", \"b\": \"{{value}}\"}";

        JsonObject document = (JsonObject) new JsonParser().parse(template);
        Map<String, String> paths = JsonUtil.getJsonPathsForTemplating(document);

        assertEquals(2, paths.size());

        Object a = paths.get("$.a");
        assertNotNull(a);
        assertEquals("{{value}}", a.toString());

        Object b = paths.get("$.b");
        assertNotNull(b);
        assertEquals("{{value}}", a.toString());
    }

    @Test
    public void simple_deep() {
        String template = "{ \"a\" : { \"b\": \"{{value}}\"}}";

        JsonObject document = (JsonObject) new JsonParser().parse(template);
        Map<String, String> paths = JsonUtil.getJsonPathsForTemplating(document);

        assertEquals(1, paths.size());

        Object value = paths.get("$.a.b");
        assertNotNull(value);
        assertEquals("{{value}}", value.toString());
    }

    @Test
    public void double_deep() {
        String template = "{ \"a\" : { \"b\": \"{{value}}\"}, \"c\": { \"b\": \"{{value}}\"}}";

        JsonObject document = (JsonObject) new JsonParser().parse(template);
        Map<String, String> paths = JsonUtil.getJsonPathsForTemplating(document);

        assertEquals(2, paths.size());

        Object a = paths.get("$.a.b");
        assertNotNull(a);
        assertEquals("{{value}}", a.toString());

        Object b = paths.get("$.c.b");
        assertNotNull(b);
        assertEquals("{{value}}", a.toString());
    }

    @Test
    public void double_nested_deep() {
        String template = "{ \"a\" : { \"b\": \"{{value}}\", \"c\": { \"b\": \"{{value}}\"}}, \"c\": { \"b\": \"{{value}}\"}}";

        JsonObject document = (JsonObject) new JsonParser().parse(template);
        Map<String, String> paths = JsonUtil.getJsonPathsForTemplating(document);

        assertEquals(3, paths.size());

        Object a = paths.get("$.a.b");
        assertNotNull(a);
        assertEquals("{{value}}", a.toString());

        Object b = paths.get("$.a.c.b");
        assertNotNull(b);
        assertEquals("{{value}}", b.toString());

        Object c = paths.get("$.c.b");
        assertNotNull(c);
        assertEquals("{{value}}", c.toString());
    }

    @Test
    public void mixed_data_types() {
        String template = "{ \"a\" : { \"b\": \"{{value}}\", \"c\": { \"b\": \"{{value}}\"}}, \"c\": { \"b\": \"{{value}}\"}, \"num\": 343, \"bool\": true}";

        JsonObject document = (JsonObject) new JsonParser().parse(template);
        Map<String, String> paths = JsonUtil.getJsonPathsForTemplating(document);

        assertEquals(3, paths.size());

        Object a = paths.get("$.a.b");
        assertNotNull(a);
        assertEquals("{{value}}", a.toString());

        Object b = paths.get("$.a.c.b");
        assertNotNull(b);
        assertEquals("{{value}}", b.toString());

        Object c = paths.get("$.c.b");
        assertNotNull(c);
        assertEquals("{{value}}", c.toString());
    }

}
