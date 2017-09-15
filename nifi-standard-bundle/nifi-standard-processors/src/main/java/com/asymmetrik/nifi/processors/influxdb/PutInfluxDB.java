package com.asymmetrik.nifi.processors.influxdb;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.asymmetrik.nifi.services.influxdb.InfluxDatabaseService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
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
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SideEffectFree
@SupportsBatching
@Tags({"put", "influxdb", "influx", "db", "write", "database", "measurement", "timeseries"})
@DynamicProperty(
        name = "Field Key (string)",
        value = "Field Value (double)",
        supportsExpressionLanguage = true,
        description = "Specifies field key and their corresponding values. Field values are always assumed to be double precision floating-point numbers.")
@CapabilityDescription("Writes to data to InfluxDB (https://docs.influxdata.com/influxdb/v1.3). This processor parses " +
        "dynamic properties as field key/values. It should be noted, that field values are assumed " +
        "to be double precision floating-point values, or can be converted to double precision floating-point values.")
public class PutInfluxDB extends AbstractProcessor {

    static final PropertyDescriptor INFLUX_DB_SERVICE = new PropertyDescriptor.Builder()
            .name("InfluxDb Service")
            .description("The Controller Service that is used to communicate with InfluxDB")
            .required(true)
            .identifiesControllerService(InfluxDatabaseService.class)
            .build();

    static final PropertyDescriptor MEASUREMENT = new PropertyDescriptor.Builder()
            .name("Measurement")
            .description("The measurement to write")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .required(true)
            .build();

    static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
            .name("Database Name")
            .description("The database name to reference")
            .expressionLanguageSupported(true)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor TAGS = new PropertyDescriptor.Builder()
            .name("Tag Key/Value CSV")
            .description("Key-value tags containing metadata. Conceptually tags are indexed columns in a table. Format expected: <tag-key>=<tag-value>,...")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(new KeyValueStringValidator())
            .build();

    private static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of flowfiles to take from the incoming work queue. Defaults to 1")
            .defaultValue("1")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .required(false)
            .build();

    static final PropertyDescriptor CONSISTENCY_LEVEL = new PropertyDescriptor.Builder()
            .name("consistency")
            .displayName("Consistency Level")
            .description("The consistency level used to store events.")
            .required(true)
            .allowableValues("All", "Any", "One", "Quorum")
            .defaultValue("One")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor RETENTION_POLICY = new PropertyDescriptor.Builder()
            .name("retention")
            .displayName("Retention Policy")
            .description("The retention policy used to store events (https://docs.influxdata.com/influxdb/v1.3/concepts/key_concepts/#retention-policy).")
            .required(true)
            .defaultValue("autogen")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor LOG_LEVEL = new PropertyDescriptor.Builder()
            .name("log_level")
            .displayName("Log Level")
            .description("The Log Level")
            .required(true)
            .allowableValues("None", "Basic", "Headers", "Full")
            .defaultValue("None")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are successfully written to InfluxDB are routed to this relationship")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that cannot be written to InfluxDB are routed to this relationship")
            .build();

    Map<String, PropertyValue> dynamicFieldValues;
    private volatile InfluxDB influxDb;
    private volatile String databaseName;

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        InfluxDB db = context.getProperty(INFLUX_DB_SERVICE)
                .asControllerService(InfluxDatabaseService.class)
                .getInfluxDb();
        db.disableBatch();

        String database = context.getProperty(DATABASE_NAME).evaluateAttributeExpressions().getValue();
        if (!db.databaseExists(database)) {
            db.createDatabase(database);
        }

        final Map<String, PropertyValue> dynamicProperties = new ConcurrentHashMap<>();
        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (!descriptor.isDynamic()) {
                continue;
            }
            dynamicProperties.put(descriptor.getName(), context.getProperty(descriptor));
        }

        this.influxDb = db;
        this.databaseName = database;
        this.dynamicFieldValues = dynamicProperties;

        InfluxDB.ConsistencyLevel consistency = InfluxDB.ConsistencyLevel.valueOf(context.getProperty(CONSISTENCY_LEVEL).getValue().toUpperCase());
        this.influxDb.setConsistency(consistency);

        PropertyValue retentionProperty = context.getProperty(RETENTION_POLICY);
        if (retentionProperty.isSet()) {
            this.influxDb.setRetentionPolicy(retentionProperty.getValue());
        }

        PropertyValue logLevelProperty = context.getProperty(LOG_LEVEL);
        if (logLevelProperty.isSet()) {
            this.influxDb.setLogLevel(InfluxDB.LogLevel.valueOf(logLevelProperty.getValue().toUpperCase()));
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) {
        List<FlowFile> flowFiles = session.get(context.getProperty(BATCH_SIZE).asInteger());
        if (flowFiles.isEmpty()) {
            return;
        }

        if (dynamicFieldValues.isEmpty()) {
            getLogger().error("At least on field key/value pair must be defined using dynamic properties");
            session.transfer(flowFiles, REL_FAILURE);
            return;
        }

        Optional<BatchPoints> batchPoints = collectPoints(context, flowFiles, databaseName);
        if (!batchPoints.isPresent()) {
            session.transfer(flowFiles, REL_FAILURE);
            return;
        }

        try {
            influxDb.write(batchPoints.get());
            session.transfer(flowFiles, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("Error while writing to InfluxDB: " + e.getMessage(), e);
            session.transfer(flowFiles, REL_FAILURE);
        }
    }

    /**
     *
     * @return
     */
    Optional<BatchPoints> collectPoints(ProcessContext context, List<FlowFile> flowFiles, String database) {
        BatchPoints batchPoints = BatchPoints.database(database).build();
        for (FlowFile flowfile : flowFiles) {
            String tags = context.getProperty(TAGS).evaluateAttributeExpressions(flowfile).getValue();
            Map<String, String> tagsMap = KeyValueStringValidator.parse(tags);
            if (null == tagsMap) {
                getLogger().error("Error while parsing tags property");
                continue;
            }

            Map<String, Object> fields = getFields(flowfile, dynamicFieldValues);
            if (fields.isEmpty()) {
                continue;
            }

            batchPoints.point(Point
                    .measurement(context.getProperty(MEASUREMENT).evaluateAttributeExpressions(flowfile).getValue())
                    .tag(tagsMap)
                    .fields(getFields(flowfile, dynamicFieldValues))
                    .build()
            );
        }
        return batchPoints.getPoints().isEmpty() ? Optional.empty() : Optional.of(batchPoints);
    }

    /**
     *
     * @param flowfile
     * @param fieldKeyValues
     * @return
     */
    Map<String, Object> getFields(FlowFile flowfile, Map<String, PropertyValue> fieldKeyValues) {

        // get field name and field values from dynamic attributes
        Map<String, Object> fields = new HashMap<>();
        for (Map.Entry<String, PropertyValue> entry : fieldKeyValues.entrySet()) {
            try {
                fields.put(entry.getKey(), entry.getValue().evaluateAttributeExpressions(flowfile).asDouble());
            } catch (NumberFormatException nfe) {
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
                .expressionLanguageSupported(true)
                .dynamic(true)
                .build();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(INFLUX_DB_SERVICE, MEASUREMENT, DATABASE_NAME, TAGS, BATCH_SIZE,
                RETENTION_POLICY, CONSISTENCY_LEVEL, LOG_LEVEL);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return ImmutableSet.of(REL_SUCCESS, REL_FAILURE);
    }

    /**
     *
     */
    static class KeyValueStringValidator implements Validator {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            return parse(input) == null ?
                    new ValidationResult.Builder()
                            .subject(subject)
                            .input(input)
                            .valid(false)
                            .explanation("Unable to parse string '" + input + "'. Expected format is key1=value1, key2=value2, ...")
                            .build() :
                    new ValidationResult.Builder()
                            .subject(subject)
                            .input(input)
                            .valid(true)
                            .build();
        }

        /**
         * Parses our tag and field comma delimited key value pair strings
         *
         * @param str The string to parse
         * @return null or error otherwise Map of key value pairs
         */
        public static Map<String, String> parse(String str) {
            Map<String, String> result = new HashMap<>();

            if (str != null) {
                String keyValuePair[] = str.split(",");
                for (String pair : keyValuePair) {
                    String[] keyValue = pair.split("=");
                    if (keyValue.length != 2) {
                        return null;
                    }
                    result.put(keyValue[0].trim(), keyValue[1].trim());
                }
            }

            return result;
        }
    }
}
