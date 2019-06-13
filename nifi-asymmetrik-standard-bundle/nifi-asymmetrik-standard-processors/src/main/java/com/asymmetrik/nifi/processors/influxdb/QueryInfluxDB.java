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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import com.asymmetrik.nifi.services.influxdb.InfluxDatabaseService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
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
@CapabilityDescription("Queries InfluxDB, generating a flowFile for each result found. It should " +
        "be noted that SELECT queries must include one of LIMIT, WHERE or GROUP BY clause.")
public class QueryInfluxDB extends AbstractInfluxProcessor {
    static final PropertyDescriptor QUERY_STRING = new PropertyDescriptor.Builder()
            .name("Query String")
            .displayName("Query String")
            .description("The query string")
            .addValidator(new QueryStringValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .build();

    static final PropertyDescriptor TIME_UNIT = new PropertyDescriptor.Builder()
            .name("Time Units")
            .displayName("Precision")
            .description("Precision of the results.")
            .required(true)
            .allowableValues("Nanoseconds", "Microseconds", "Milliseconds", "Seconds", "Minutes", "Hours", "Days")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .defaultValue("Milliseconds")
            .build();

    private AtomicReference<InfluxDB> influxRef = new AtomicReference<>();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(INFLUX_DB_SERVICE, DATABASE_NAME, QUERY_STRING, TIME_UNIT);
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        influxRef.set(context.getProperty(INFLUX_DB_SERVICE)
                .asControllerService(InfluxDatabaseService.class)
                .getInfluxDb());

        if (influxRef.get() == null) {
            getLogger().error("Unable to retrieve InfluxDB connection", new IllegalArgumentException("Unable to retrieve InfluxDB connection"));
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) {
        String database = context.getProperty(DATABASE_NAME).evaluateAttributeExpressions().getValue();
        Query query = new Query(context.getProperty(QUERY_STRING).getValue(), database);

        TimeUnit precision = TimeUnit.valueOf(context.getProperty(TIME_UNIT).getValue().toUpperCase());
        QueryResult queryResult = influxRef.get().query(query, precision);

        if (queryResult.hasError()) {
            String message = queryResult.getError();
            getLogger().error(message);
            FlowFile flowFile = session.write(session.create(), outputStream -> outputStream.write(message.getBytes()));
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        serializeResults(session, queryResult.getResults());
    }

    private void serializeResults(ProcessSession session, List<Result> results) {
        List<FlowFile> flowFiles = new ArrayList<>();
        for (Result result : results) {
            FlowFile flowFile = session.write(session.create(), outputStream -> outputStream.write(result.toString().getBytes()));
            flowFiles.add(flowFile);
        }
        session.transfer(flowFiles, REL_SUCCESS);
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
