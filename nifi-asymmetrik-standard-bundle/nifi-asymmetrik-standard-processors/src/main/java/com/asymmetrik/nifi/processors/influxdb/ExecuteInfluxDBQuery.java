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
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import com.asymmetrik.nifi.services.influxdb.InfluxDatabaseService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@SideEffectFree
@DefaultSchedule(period = "10 sec")
@Tags({"query", "get", "database", "influxdb", "influx", "db"})
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type to application/json."),
})
@CapabilityDescription("Queries InfluxDB, generating a flowFile for each result found. It should " +
        "be noted that SELECT queries must include one of LIMIT, WHERE or GROUP BY clause.")
public class ExecuteInfluxDBQuery extends AbstractInfluxProcessor {
    static final PropertyDescriptor PROP_QUERY_STRING = new PropertyDescriptor.Builder()
            .name("influxql.query")
            .displayName("InfluxQL Query String")
            .description("The InfluxDB query to execute.")
            .addValidator(new QueryStringValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .build();

    static final PropertyDescriptor PROP_SPLIT_RESULTS = new PropertyDescriptor.Builder()
            .name("results.split")
            .displayName("Split results?")
            .description("Set this to 'true' to split the results into separate JSON objects.")
            .required(true)
            .allowableValues("true", "false")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .defaultValue("false")
            .build();

    private AtomicReference<InfluxDB> influxRef = new AtomicReference<>();
    private Gson gson = new Gson();
    private Query query;
    private TimeUnit precision;
    private Boolean splitResults;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(
                INFLUX_DB_SERVICE,
                DATABASE_NAME,
                PRECISION,
                PROP_QUERY_STRING,
                PROP_SPLIT_RESULTS);
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        influxRef.set(context.getProperty(INFLUX_DB_SERVICE)
                .asControllerService(InfluxDatabaseService.class)
                .getInfluxDb());

        if (influxRef.get() == null) {
            getLogger().error("Unable to retrieve InfluxDB connection", new IllegalArgumentException("Unable to retrieve InfluxDB connection"));
        }

        String database = context.getProperty(DATABASE_NAME).evaluateAttributeExpressions().getValue();
        query = new Query(context.getProperty(PROP_QUERY_STRING).getValue(), database);
        precision = TimeUnit.valueOf(context.getProperty(PRECISION).getValue().toUpperCase());
        splitResults = context.getProperty(PROP_SPLIT_RESULTS).asBoolean();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) {
        QueryResult queryResult = influxRef.get().query(query, precision);

        List<FlowFile> failures = new ArrayList<>();
        for (Result result : queryResult.getResults()) {
            if (result.hasError()) {
                final String json = gson.toJson(queryResult);
                getLogger().error(result.getError());
                failures.add(session.write(session.create(), outputStream -> outputStream.write(json.getBytes())));
            }
        }
        if (!failures.isEmpty()) {
            session.transfer(failures);
        }

        List<Result> results = queryResult.getResults();
        List<FlowFile> flowFiles = new ArrayList<>();
        if (splitResults) {
            delegateSplitResults(session, flowFiles, results);
        } else {
            final byte[] bytes = gson.toJson(results).getBytes(StandardCharsets.UTF_8);
            FlowFile flowFile = session.create();
            flowFile = session.putAttribute(flowFile, "mime.type", "application/json");
            flowFiles.add(session.write(flowFile, outputStream -> outputStream.write(bytes)));
        }

        session.transfer(flowFiles, REL_SUCCESS);
    }

    private void delegateSplitResults(ProcessSession session, List<FlowFile> flowFiles, List<Result> results) {
        for (Result result : results) {
            if (CollectionUtils.isEmpty(result.getSeries())) {
                continue;
            }

            for (QueryResult.Series series : result.getSeries()) {
                final byte[] bytes = gson.toJson(series).getBytes(StandardCharsets.UTF_8);
                FlowFile flowFile = session.create();
                flowFile = session.putAttribute(flowFile, "mime.type", "application/json");
                flowFiles.add(session.write(flowFile, outputStream -> outputStream.write(bytes)));
            }
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return ImmutableSet.of(REL_SUCCESS, REL_FAILURE);
    }

    /**
     *
     */
    private static class QueryStringValidator implements Validator {

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {

            Pattern queryTypePattern = Pattern.compile("^(select|show|create|alter|drop|delete)\\s+.*", Pattern.CASE_INSENSITIVE);
            Pattern selectPattern = Pattern.compile("^select\\s.*(from|into).*((\\bs?limit\\s\\d+\\b)|(\\bwhere\\b)|(group by)).*", Pattern.CASE_INSENSITIVE);
            Pattern showPattern = Pattern.compile("^show\\s+(databases|measurements\\s+on|series\\s+on|field\\s+keys\\s+on|retention\\s+policies\\s+on\\s+|tag\\s+keys\\s+on\\s+|tag\\s+values\\s+on\\s+).*", Pattern.CASE_INSENSITIVE);
            Pattern createPattern = Pattern.compile("^create\\s+(database|retention\\s+policy)\\s+[a-zA-z_()\'\"].*", Pattern.CASE_INSENSITIVE);

            String in = input.trim().toLowerCase();
            String reason = "";
            if (StringUtils.isEmpty(in) || !queryTypePattern.matcher(in).matches()) {
                reason = "Query string must begin with one of: SELECT, SHOW, CREATE, ALTER, DROP, or DELETE";
                return validationResult(subject, input, reason, StringUtils.isEmpty(reason));
            }

            if (in.startsWith("select") && !selectPattern.matcher(in).matches()) {
                reason = "SELECT queries must contain start with select and contain either a limit, slimit or a where clause";
            } else if (in.startsWith("show") && !showPattern.matcher(in).matches()) {
                reason = "SHOW queries must be followed by databases, measurements on [db], series on [db], field keys on [db], retention policies on [db], tag keys on [db], tag values on [db]";
            } else if (in.startsWith("create") && !createPattern.matcher(in).matches()) {
                reason = "CREATE queries must be followed by database or retention policy";
            }

            return validationResult(subject, input, reason, StringUtils.isEmpty(reason));
        }

        private ValidationResult validationResult(String subject, String input, String reason, boolean valid) {
            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .explanation(reason)
                    .valid(valid)
                    .build();
        }
    }
}
