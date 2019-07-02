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
package com.asymmetrik.nifi.processors.influxdb;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

@SuppressWarnings("Duplicates")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SupportsBatching
@Tags({"put", "influxdb", "influx", "db", "write", "database", "measurement", "timeseries"})
@CapabilityDescription("Converts the results of Asymmetrik's ExecuteInfluxDBQuery to the line protocol. This is intended to be used with the Apache PutInfluxDB processor" +
        "to insert data into influxDB. For more information of the line protocol, see https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_tutorial/")
public class ConvertInfluxOutputToLineProtocol extends AbstractInfluxProcessor {

    static final PropertyDescriptor PROP_TAG_NAMES = new PropertyDescriptor.Builder()
            .name("names.tag")
            .displayName("Tag Names")
            .description("A CSV of tag names. This value is used to determine the column indices that correspond to influx tag values.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    static final PropertyDescriptor PROP_FIELD_NAMES = new PropertyDescriptor.Builder()
            .name("names.field")
            .displayName("Field Names")
            .description("A CSV of field names. This value is used to determine the column indices that correspond to influx field values.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    private static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("the original, unmodified flowfile is sent to this relationship")
            .build();

    private JsonParser parser = new JsonParser();
    private Map<String, Integer> tags;
    private Map<String, Integer> fields;
    private String tagCSV;
    private String fieldCSV;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        tagCSV = context.getProperty(PROP_TAG_NAMES).getValue();
        fieldCSV = context.getProperty(PROP_FIELD_NAMES).getValue();
        parser = new JsonParser();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) {
        FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final byte[] buffer = new byte[(int) original.getSize()];
        session.read(original, in -> StreamUtils.fillBuffer(in, buffer));
        String payload = new String(buffer, StandardCharsets.UTF_8);

        try {
            List<String> lines = convertPayload(payload, tagCSV, fieldCSV);
            for (String line : lines) {
                byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
                FlowFile flowFile = session.write(session.clone(original), out -> out.write(bytes));
                session.transfer(flowFile, REL_SUCCESS);
            }
            session.transfer(original, REL_ORIGINAL);
        } catch (Exception e) {
            getLogger().error("Error creating line protocol: " + e.getMessage(), e);
            session.transfer(original, REL_FAILURE);
            context.yield();
        }
    }

    /**
     * @return an optional of the data points
     */
    List<String> convertPayload(String json, String tagCSV, String fieldCSV) {
        JsonElement root = parser.parse(json);

        // StringBuilder line = new StringBuilder();
        JsonObject r = root.getAsJsonObject();
        JsonArray columns = r.getAsJsonArray("columns");
        indexTags(columns, tagCSV, fieldCSV);

        String measurement = r.get("measurement").getAsString();
        TimeUnit precision = TimeUnit.valueOf(r.get("precision").getAsString());

        List<String> lines = new ArrayList<>();
        JsonArray values = r.getAsJsonArray("values");
        for (JsonElement value : values) {
            JsonArray valueArray = value.getAsJsonArray();
            lines.add(processRow(measurement, valueArray, precision));
        }
        return lines;
    }

    private String processRow(String measurement, JsonArray row, TimeUnit precision) {
        // Build tags
        List<String> tagValues = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : tags.entrySet()) {
            JsonElement tagValue = row.get(entry.getValue());
            if (!tagValue.isJsonNull()) {
                tagValues.add(entry.getKey() + "=" + tagValue.getAsString());
            }
        }

        // Build fields
        List<String> fieldValues = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : fields.entrySet()) {
            JsonElement fieldValue = row.get(entry.getValue());
            if (!fieldValue.isJsonNull()) {
                fieldValues.add(entry.getKey() + "=" + fieldValue.getAsDouble());
            }
        }

        // create line protocol entry
        StringBuilder sb = new StringBuilder(measurement);

        // append tag key=value, if present
        if (!tagValues.isEmpty()) {
            sb.append(",").append(String.join(",", tagValues));
        }

        // append field ky=value, if present
        if (!fieldValues.isEmpty()) {
            sb.append(" ").append(String.join(",", fieldValues));
        }

        // append timestamp
        long timestamp = precision.convert(row.get(0).getAsBigDecimal().longValue(), TimeUnit.NANOSECONDS);
        sb.append(" ").append(timestamp);

        return sb.toString();
    }

    void indexTags(JsonArray columns, String tagCsv, String fieldCsv) {
        if (tags != null) {
            return;
        }

        Splitter splitter = Splitter.onPattern(",").trimResults().omitEmptyStrings();
        Map<String, Integer> columnIndices = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            columnIndices.put(columns.get(i).getAsString(), i);
        }

        tags = new HashMap<>();
        if (tagCsv != null) {
            for (String t : splitter.split(tagCsv)) {
                if (columnIndices.get(t) != null) {
                    tags.put(t, columnIndices.get(t));
                }
            }
        }

        fields = new HashMap<>();
        for(String t : splitter.split(fieldCsv)) {
            if (columnIndices.get(t) != null) {
                fields.put(t, columnIndices.get(t));
            }
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(
                PROP_TAG_NAMES,
                PROP_FIELD_NAMES
        );
    }

    @Override
    public Set<Relationship> getRelationships() {
        return ImmutableSet.of(REL_ORIGINAL, REL_SUCCESS, REL_FAILURE);
    }
}
