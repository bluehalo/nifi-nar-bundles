package com.asymmetrik.nifi.processors.influxdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import com.asymmetrik.nifi.services.influxdb.InfluxDatabaseService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@SideEffectFree
@DefaultSchedule(period = "10 sec")
@Tags({"query", "get", "database", "influxdb", "influx", "db"})
@CapabilityDescription("Queries InfluxDB, generating a flowFile for each result found. It should " +
        "be noted that SELECT queries must include one of LIMIT, WHERE or GROUP BY clause.  In fact, " +
        "the query string property will fail if one of the aforementioned clauses is not specified.")
public class QueryInfluxDB extends AbstractProcessor {
    static final ObjectMapper OBJ_MAPPER = new ObjectMapper();

    static final PropertyDescriptor INFLUX_DB_SERVICE = new PropertyDescriptor.Builder()
            .name("InfluxDb Service")
            .displayName("InfluxDb Service")
            .description("The Controller Service that is used to communicate with InfluxDB")
            .required(true)
            .identifiesControllerService(InfluxDatabaseService.class)
            .build();

    static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
            .name("Database Name")
            .displayName("Database Name")
            .description("The database name to reference")
            .expressionLanguageSupported(true)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor QUERY_STRING = new PropertyDescriptor.Builder()
            .name("Query String")
            .displayName("Query String")
            .description("The query string")
            .addValidator(new QueryStringValidator())
            .expressionLanguageSupported(false)
            .required(true)
            .build();

    static final PropertyDescriptor TIME_UNIT = new PropertyDescriptor.Builder()
            .name("Time Units")
            .displayName("Time Units")
            .description("Time Units for results.")
            .required(true)
            .allowableValues("Nanoseconds", "Microseconds", "Milliseconds", "Seconds", "Minutes", "Hours", "Days")
            .defaultValue("Milliseconds")
            .build();

    static final PropertyDescriptor CONSISTENCY_LEVEL = new PropertyDescriptor.Builder()
            .name("consistency")
            .displayName("Consistency Level")
            .description("The consistency level.")
            .required(true)
            .allowableValues("All", "Any", "One", "Quorum")
            .defaultValue("One")
            .build();

    static final PropertyDescriptor RETENTION_POLICY = new PropertyDescriptor.Builder()
            .name("retention")
            .displayName("Retention Policy")
            .description("The retention policy.")
            .required(false)
            .build();

    static final PropertyDescriptor LOG_LEVEL = new PropertyDescriptor.Builder()
            .name("log_level")
            .displayName("Log Level")
            .description("The Log Level")
            .required(true)
            .allowableValues("None", "Basic", "Headers", "Full")
            .defaultValue("None")
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are successfully written to InfluxDB are routed to this relationship")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that cannot be written to InfluxDB are routed to this relationship")
            .build();

    private volatile InfluxDB influxDb;
    private Query query;
    private TimeUnit timeUnit;

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        InfluxDB db = context.getProperty(INFLUX_DB_SERVICE)
                .asControllerService(InfluxDatabaseService.class)
                .getInfluxDb();
        this.influxDb = db;

        if (db == null) {
            getLogger().error("Unable to retrieve InfluxDB connection", new IllegalArgumentException("Unable to retrieve InfluxDB connection"));
        }

        String database = context.getProperty(DATABASE_NAME).evaluateAttributeExpressions().getValue();
        if (!db.databaseExists(database)) {
            String message = "InfluxDB database does not exist. The specified database must" +
                    "exist prior to initializing this processor.";
            getLogger().error(message, new IllegalArgumentException(message));
        }
        this.influxDb.setDatabase(database);

        InfluxDB.ConsistencyLevel consistency = InfluxDB.ConsistencyLevel.valueOf(context.getProperty(CONSISTENCY_LEVEL).getValue().toLowerCase());
        this.influxDb.setConsistency(consistency);

        PropertyValue retentionProperty = context.getProperty(RETENTION_POLICY);
        if (retentionProperty.isSet()) {
            this.influxDb.setRetentionPolicy(retentionProperty.getValue());
        }

        PropertyValue logLevelPropery = context.getProperty(LOG_LEVEL);
        if (logLevelPropery.isSet()) {
            this.influxDb.setLogLevel(InfluxDB.LogLevel.valueOf(logLevelPropery.getValue().toUpperCase()));
        }

        this.timeUnit = TimeUnit.valueOf(context.getProperty(TIME_UNIT).getValue().toUpperCase());
        this.query = new Query(context.getProperty(QUERY_STRING).getValue(), database);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) {
        QueryResult queryResult = influxDb.query(query, timeUnit);
        if (queryResult.hasError()) {
            String message = queryResult.getError();
            getLogger().error(message);
            FlowFile flowFile = session.write(session.create(), out -> out.write(message.getBytes()));
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        FlowFile flowFile = session.write(session.create(), out -> out.write(toJson(queryResult).asText().getBytes()));
        session.transfer(flowFile, REL_SUCCESS);
    }

    static ObjectNode toJson(QueryResult queryResult) {
        ArrayNode results = OBJ_MAPPER.createArrayNode();
        for (QueryResult.Result result : queryResult.getResults()) {
            results.add(buildSeries(result));
        }
        ObjectNode root = OBJ_MAPPER.createObjectNode();
        root.replace("results", results);
        return root;
    }

    static ArrayNode buildSeries(QueryResult.Result result) {
        ArrayNode resultsJson = OBJ_MAPPER.createArrayNode();
        for (QueryResult.Series series : result.getSeries()) {
            ObjectNode seriesJson = OBJ_MAPPER.createObjectNode();
            seriesJson.put("name", series.getName());

            Optional<ObjectNode> tagsOptional = buildTags(series);
            tagsOptional.ifPresent(tags -> seriesJson.replace("tags", tags));

            Optional<ArrayNode> rowsOptional = buildRows(series);
            rowsOptional.ifPresent(rows -> seriesJson.replace("rows", rows));

            resultsJson.add(seriesJson);
        }
        return resultsJson;
    }

    static Optional<ObjectNode> buildTags(QueryResult.Series series) {
        Map<String, String> tags = series.getTags();
        if (MapUtils.isEmpty(tags)) {
            return Optional.empty();
        }

        ObjectNode tag = OBJ_MAPPER.createObjectNode();
        for (Map.Entry<String, String> t : series.getTags().entrySet()) {
            tag.put(t.getKey(), t.getValue());
        }
        return Optional.of(tag);
    }

    static Optional<ArrayNode> buildRows(QueryResult.Series series) {
        List<String> columns = series.getColumns();
        if (CollectionUtils.isEmpty(columns)) {
            return Optional.empty();
        }

        ArrayNode rows = OBJ_MAPPER.createArrayNode();
        for (List<Object> row : series.getValues()) {
            rows.add(buildRow(columns, row));
        }
        return Optional.of(rows);
    }

    static ObjectNode buildRow(List<String> columns, List<Object> values) {
        ObjectNode row = OBJ_MAPPER.createObjectNode();
        int i = 0;
        for (String column : columns) {
            Object value = values.get(i++);
            if (value instanceof Double) {
                row.put(column, ((Number) value).doubleValue());
            } else if (value instanceof Long) {
                row.put(column, ((Number) value).longValue());
            } else if (value instanceof Integer) {
                row.put(column, ((Number) value).intValue());
            } else if (value instanceof Boolean) {
                row.put(column, (Boolean) value);
            } else {
                row.put(column, (String) value);
            }
        }
        return row;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return ImmutableSet.of(REL_SUCCESS, REL_FAILURE);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(INFLUX_DB_SERVICE, DATABASE_NAME, QUERY_STRING, TIME_UNIT);
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
