package com.asymmetrik.nifi.processors.influxdb;

import com.asymmetrik.nifi.services.influxdb.InfluxDatabaseService;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

abstract class AbstractInfluxProcessor extends AbstractProcessor {
    static final String PRECISION_SECONDS = "Seconds";
    static final String PRECISION_MILLISECONDS = "Milliseconds";
    static final String PRECISION_MICROSECONDS = "Microseconds";
    static final String PRECISION_NANOSECONDS = "Nanoseconds";
    static final String CONSISTENCY_LEVEL_ONE = "One";
    static final String CONSISTENCY_LEVEL_ALL = "All";
    static final String CONSISTENCY_LEVEL_ANY = "Any";
    static final String CONSISTENCY_LEVEL_QUORUM = "Quorum";

    static final PropertyDescriptor INFLUX_DB_SERVICE = new PropertyDescriptor.Builder()
            .name("InfluxDb Service")
            .description("The Controller Service that is used to communicate with InfluxDB")
            .required(true)
            .identifiesControllerService(InfluxDatabaseService.class)
            .build();

    static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
            .name("Database Name")
            .description("The database name to reference")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor CONSISTENCY_LEVEL = new PropertyDescriptor.Builder()
            .name("consistency")
            .displayName("Consistency Level")
            .description("The consistency level used to store events.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .allowableValues(CONSISTENCY_LEVEL_ONE, CONSISTENCY_LEVEL_ANY, CONSISTENCY_LEVEL_QUORUM, CONSISTENCY_LEVEL_ALL)
            .defaultValue(CONSISTENCY_LEVEL_ONE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor RETENTION_POLICY = new PropertyDescriptor.Builder()
            .name("retention")
            .displayName("Retention Policy")
            .description("The retention policy used to store events (https://docs.influxdata.com/influxdb/v1.7/concepts/key_concepts/#retention-policy).")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor PRECISION = new PropertyDescriptor.Builder()
            .name("precision")
            .displayName("Precision")
            .description("The temporal precision for metrics.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(PRECISION_SECONDS, PRECISION_MILLISECONDS, PRECISION_MICROSECONDS, PRECISION_NANOSECONDS)
            .defaultValue(PRECISION_MILLISECONDS)
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
}