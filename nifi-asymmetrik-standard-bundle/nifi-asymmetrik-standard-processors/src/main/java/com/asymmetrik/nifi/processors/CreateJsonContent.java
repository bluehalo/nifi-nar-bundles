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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.asymmetrik.nifi.processors.util.JsonUtil;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;

import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

@SupportsBatching
@SideEffectFree
@Tags({"asymmetrik", "json", "content", "create"})
@CapabilityDescription("Creates new flowfile content based on the content property")
public class CreateJsonContent extends AbstractProcessor {
    /**
     * Relationship
     */
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("the original content")
            .build();
    /**
     * Relationship Descriptors
     */
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("the embedded payload")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files that could not be created for some reason are transferred to this relationship")
            .build();
    /**
     * Property Descriptors
     */
    public static final PropertyDescriptor CONTENT_FIELD = new PropertyDescriptor.Builder()
            .name("Content Field")
            .description("The template for the JSON payload to be created")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    private static final Configuration JSON_PROVIDER_CONFIGURATION = Configuration.builder().jsonProvider(new GsonJsonProvider()).build();
    private static final JsonProvider JSON_PROVIDER = JSON_PROVIDER_CONFIGURATION.jsonProvider();
    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private Map<String, String> pathsForTemplating;

    @Override
    protected void init(ProcessorInitializationContext context) {
        final List<PropertyDescriptor> props = Arrays.asList(CONTENT_FIELD);
        properties = Collections.unmodifiableList(props);

        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_ORIGINAL);
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(rels);
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        String content = context.getProperty(CONTENT_FIELD).getValue().trim();

        JsonObject json = (JsonObject) JSON_PROVIDER.parse(content);
        pathsForTemplating = JsonUtil.getJsonPathsForTemplating(json);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) {

        FlowFile original = session.get();

        FlowFile flowFile;
        if (original == null) {
            return;
        }
        flowFile = session.clone(original);
        Map<String, String> flowFileAttributes = flowFile.getAttributes();
        session.transfer(original, REL_ORIGINAL);

        String content = context.getProperty(CONTENT_FIELD).getValue().trim();
        if (content.isEmpty()) {
            return;
        }

        final JsonObject json = (JsonObject) JSON_PROVIDER.parse(content);
        String[] attrSplit;
        String value = null;
        JsonElement replacement;
        for (Map.Entry<String, String> entry : pathsForTemplating.entrySet()) {

            String attrKey = entry.getValue().trim();
            String entryKey = entry.getKey();

            try {
                attrKey = attrKey.substring(2, attrKey.length() - 2).trim();

                String type = null;
                if (attrKey.contains(":")) {
                    attrSplit = attrKey.split(":");
                    attrKey = attrSplit[0];
                    type = attrSplit[1];
                    value = flowFileAttributes.get(attrKey);
                } else {
                    value = flowFile.getAttribute(attrKey);
                }

                replacement = processJson(type, value);
                JsonUtil.updateField(json, entryKey.substring(2), replacement);
            } catch (Exception e) {
                getLogger().error("Unable to update {} with {} for attributes {}", new Object[]{entryKey, attrKey, flowFileAttributes}, e);
                Map<String, String> failureAttributes = new HashMap<>();
                failureAttributes.put("json.error.message", e.getMessage());
                failureAttributes.put("json.error.key", entryKey);
                failureAttributes.put("json.error.value", value);
                flowFile = session.putAllAttributes(flowFile, failureAttributes);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
        }
        flowFile = session.write(flowFile, outputStream -> outputStream.write(json.toString().getBytes(StandardCharsets.UTF_8)));
        session.getProvenanceReporter().modifyContent(flowFile);
        session.transfer(flowFile, REL_SUCCESS);
    }

    private JsonElement processJson(String type, String value) {
        if (type == null) {
            return value == null || value.isEmpty() ? JsonNull.INSTANCE : new JsonPrimitive(value);
        }

        if (value == null || value.isEmpty()) {
            return JsonNull.INSTANCE;
        }

        JsonElement replacement = null;
        if (type.startsWith("bool")) {
            replacement = new JsonPrimitive(Boolean.valueOf(value));
        } else if (type.startsWith("int")) {
            replacement = new JsonPrimitive(Long.valueOf(value));
        } else if (type.startsWith("float")) {
            replacement = new JsonPrimitive(Double.valueOf(value));
        } else if (type.startsWith("json")) {
            replacement = (JsonElement) JSON_PROVIDER.parse(value);
        }
        return replacement;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

}
