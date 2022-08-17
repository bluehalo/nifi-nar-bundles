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

import com.asymmetrik.nifi.services.influxdb2.InfluxClientApi;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.opencsv.CSVParserBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.*;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@SuppressWarnings("Duplicates")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SideEffectFree
@SupportsBatching
@Tags({"put", "influxdb", "influx", "db", "write", "database", "measurement", "timeseries"})
@DynamicProperty(
        name = "Field Key (string)",
        value = "Field Value (double)",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "Specifies field key and their corresponding values. Field values are always assumed to be double precision floating-point numbers.")
@CapabilityDescription("Writes to data to InfluxDB (https://docs.influxdata.com/influxdb/v2.3/). This processor parses " +
        "dynamic properties as field key/values. It should be noted, that field values are assumed " +
        "to be double precision floating-point values, or can be converted to double precision floating-point values.")
public class PutInfluxDBv2 extends AbstractProcessor {
    static final PropertyDescriptor PROP_INFLUX_DB_SERVICE = new PropertyDescriptor.Builder()
            .name("InfluxDBv2 Service")
            .displayName("InfluxDBv2 Service")
            .description("The Controller Service that is used to communicate with InfluxDBv2")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .identifiesControllerService(InfluxClientApi.class)
            .build();

    static final PropertyDescriptor PROP_ORG = new PropertyDescriptor.Builder()
            .name("org")
            .displayName("Organization")
            .description("Organization")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();

    static final PropertyDescriptor PROP_BUCKET = new PropertyDescriptor.Builder()
            .name("bucket")
            .displayName("Bucket")
            .description("Bucket")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();

    static final PropertyDescriptor PROP_MEASUREMENT = new PropertyDescriptor.Builder()
            .name("measurement")
            .displayName("Measurement")
            .description("The measurement to write")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    static final PropertyDescriptor PROP_TAGS = new PropertyDescriptor.Builder()
            .name("tags")
            .displayName("Tag Key/Value CSV")
            .description("Key-value tags containing metadata. Conceptually tags are indexed columns in a table. Format expected: <tag-key>=<tag-value>,..." +
                    "Warning: Due to the way CSVParser is implemented, if a key-value pair contains any inner quotes, it's not guaranteed that inner quote's " +
                    "corresponding outer quote will not get stripped.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(new KeyValueStringValidator())
            .build();

    static final PropertyDescriptor PROP_PRECISION = new PropertyDescriptor.Builder()
            .name("precision")
            .displayName("Precision")
            .description("The temporal precision for metrics.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("ns", "ms", "s")
            .defaultValue("ms")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor PROP_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("batch")
            .displayName("Batch Size")
            .description("The maximum number of flowfiles to take from the incoming work queue. Defaults to 1")
            .defaultValue("1")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .required(false)
            .build();

    static final PropertyDescriptor PROP_TIMESTAMP = new PropertyDescriptor.Builder()
            .name("timestamp")
            .displayName("Event Timestamp")
            .description("A long integer value representing the timestamp of a measurement point. Please note that " +
                    "the precision property directly affects how this value is interpreted. For example, setting this " +
                    "property to 'System.currentTimeMillis()' requires the 'precision' property to be set to 'Milliseconds' " +
                    "in order to properly interpret the value.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are successfully written to InfluxDB are routed to this relationship")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that cannot be written to InfluxDB are routed to this relationship")
            .build();

    private InfluxDBClient influxClient;
    private WritePrecision precision;
    private Map<String, PropertyValue> dynamicFieldValues;

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        Map<String, PropertyValue> result = context.getProperties().keySet().stream()
                .filter(PropertyDescriptor::isDynamic)
                .collect(Collectors.toMap(PropertyDescriptor::getName, context::getProperty));

        if (result.isEmpty()) {
            throw new IOException("At least one field key/value pair must be defined using dynamic properties");
        }

        dynamicFieldValues = result;

        influxClient = context.getProperty(PROP_INFLUX_DB_SERVICE)
                .asControllerService(InfluxClientApi.class)
                .getInfluxDb();

        precision = WritePrecision.fromValue(context.getProperty(PROP_PRECISION).getValue());
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) {
        List<FlowFile> flowFiles = session.get(context.getProperty(PROP_BATCH_SIZE).asInteger());
        if (flowFiles.isEmpty()) {
            return;
        }

        List<Point> points = collectPoints(context, flowFiles);
        if (CollectionUtils.isEmpty(points)) {
            session.transfer(flowFiles, REL_FAILURE);
            context.yield();
            return;
        }

        try {
            String bucket = context.getProperty(PROP_BUCKET).evaluateAttributeExpressions().getValue();
            String org = context.getProperty(PROP_ORG).evaluateAttributeExpressions().getValue();
            WriteApiBlocking writeApi = influxClient.getWriteApiBlocking();
            writeApi.writePoints(bucket, org, points);
            session.transfer(flowFiles, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("Error while writing to InfluxDB: " + e.getMessage(), e);
            session.transfer(flowFiles, REL_FAILURE);
            context.yield();
        }
    }

    /**
     * @return an optional of the data points
     */
    List<Point> collectPoints(ProcessContext context, List<FlowFile> flowFiles) {
        PropertyValue measurementProp = context.getProperty(PROP_MEASUREMENT);

        List<Point> points = new ArrayList<>();
        PropertyValue timestampProp = context.getProperty(PROP_TIMESTAMP);

        long now = Instant.now().toEpochMilli();
        for (FlowFile flowfile : flowFiles) {
            Map<String, Object> fields = getFields(flowfile);
            if (fields.isEmpty()) {
                continue;
            }

            // create point
            Point point = Point
                    .measurement(measurementProp.evaluateAttributeExpressions(flowfile).getValue())
                    .addFields(fields);

            // add tags, if applicable
            String tags = context.getProperty(PROP_TAGS).evaluateAttributeExpressions(flowfile).getValue();
            Map<String, String> tagsMap = KeyValueStringValidator.parse(tags);
            if (MapUtils.isNotEmpty(tagsMap)) {
                point = point.addTags(tagsMap);
            }

            // set timestamp
            now = timestampProp.isSet() ? timestampProp.evaluateAttributeExpressions(flowfile).asLong() : now;
            point.time(now, precision);

            points.add(point);
        }

        return points;
    }

    /**
     * @param flowfile       the nifi flowfile
     * @return a map of the valid dynamic properties after evaluating expression language.
     */
    private Map<String, Object> getFields(FlowFile flowfile) {

        // get field name and field values from dynamic attributes
        Map<String, Object> fields = new HashMap<>();
        for (Map.Entry<String, PropertyValue> entry : dynamicFieldValues.entrySet()) {
            PropertyValue val = entry.getValue().evaluateAttributeExpressions(flowfile);
            try {
                fields.put(entry.getKey(), val.asDouble());
            } catch (NumberFormatException nfe) {
                getLogger().warn("Invalid number provided: {}", val.getValue());
            }
        }
        return fields;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(PROP_INFLUX_DB_SERVICE, PROP_BUCKET, PROP_ORG, PROP_MEASUREMENT, PROP_TAGS, PROP_BATCH_SIZE, PROP_PRECISION, PROP_TIMESTAMP);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return ImmutableSet.of(REL_SUCCESS, REL_FAILURE);
    }

    /**
     * Key Value CSV Validator/Parser
     */
    static class KeyValueStringValidator implements Validator {
        private static final CSVParserBuilder parserBuilder = new CSVParserBuilder().withIgnoreLeadingWhiteSpace(true);

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return (new ValidationResult.Builder()).subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
            }

            List<String> invalids;
            try {
                invalids = Arrays.stream(parserBuilder.build()
                        .parseLine(input))
                        .filter(s -> KeyValueStringValidator.split(s) == null)
                        .collect(Collectors.toList());

                if (invalids.isEmpty()) {
                    return new ValidationResult.Builder()
                            .subject(subject)
                            .input(input)
                            .valid(true)
                            .build();
                }
            } catch (Exception e) {
                // ignore, handled below
            }

            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .valid(false)
                    .explanation("Unable to parse string '" + input + "'. Expected format is key1=value1, key2=value2, ...")
                    .build();
        }

        /**
         * Parses our tag and field comma delimited key value pair strings
         *
         * @param str The string to parse
         * @return Map of key value pairs
         */
        public static Map<String, String> parse(String str) {
            if (str == null) {
                return new HashMap<>();
            }

            try {
                return Arrays.stream(parserBuilder.build()
                        .parseLine(str))
                        .map(KeyValueStringValidator::split)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toMap(a -> a[0], a -> a[1]));

            } catch (IOException e) {
                return new HashMap<>();
            }
        }

        /**
         * Splits s into an array of size 2 where index position 0 is left of equal sign and position 1 is right of equal sign.
         * Both parts (the key and value) of the split must be non blank otherwise returns null
         * @param s The string to split
         * @return array of size 2 or null for invalid input
         */
        private static String[] split(String s) {
            final int found = s.indexOf('=');
            if (found == -1) {
                return null;
            }

            final String left = s.substring(0, found).trim();
            final String right = s.substring(found + 1).trim();

            return StringUtils.isAnyEmpty(left, right)
                    ? null
                    : new String[]{ left, right };
        }
    }
}
