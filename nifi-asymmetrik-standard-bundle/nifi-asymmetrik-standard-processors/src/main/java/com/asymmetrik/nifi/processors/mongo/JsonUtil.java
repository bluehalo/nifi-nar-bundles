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
