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
package com.asymmetrik.nifi.processors.mongo;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

@SuppressWarnings("Duplicates")
public final class JsonUtil {
    public static JsonElement extractElement(JsonElement json, String field) {
        if (json == null) {
            return JsonNull.INSTANCE;
        }

        // We reached the field, return the leaf's value as a string
        if (!field.contains(".")) {
            if (json.isJsonObject()) {
                JsonElement element = ((JsonObject) json).get(field);
                return element == null ? JsonNull.INSTANCE : element;
            } else {
                return JsonNull.INSTANCE;
            }
        }

        int i = field.indexOf('.');
        String key = field.substring(i + 1, field.length());
        JsonElement value = ((JsonObject) json).get(field.substring(0, i));
        return extractElement(value, key);
    }

    public static boolean isNull(JsonElement json) {
        return json == null || json instanceof JsonNull;
    }
}
