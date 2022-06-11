package com.asymmetrik.nifi.reporting;

import java.util.List;

import com.asymmetrik.nifi.models.SystemMetricsSnapshot;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.ReportingContext;


@Tags({"disk", "system", "monitoring", "metrics", "reporting"})
@CapabilityDescription("Calculates the amount of storage space available for the Content Repositories and Flowfile Repository, " +
        "calculates the total count and size of the queue, and emits these metrics to InfluxDB")

public class LoggerNiFiClusterMetricsReporter extends AbstractNiFiClusterMetricsReporter {

    static final PropertyDescriptor LOG_LEVEL = new PropertyDescriptor.Builder()
            .name("Log Level")
            .displayName("Log Level")
            .description("Log level.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("info", "debug", "warn", "error")
            .defaultValue("error")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(
                LOG_LEVEL,
                VOLUMES,
                PROCESS_GROUPS,
                REMOTE_PROCESS_GROUPS,
                PROCESSORS,
                CONNECTIONS,
                INPUT_PORTS,
                OUTPUT_PORTS,
                INCLUDE_FILE_DESCRIPTOR_METRICS);
    }

    @Override
    void publish(ReportingContext reportingContext, SystemMetricsSnapshot systemMetricsSnapshot) {
        Gson gson = new Gson();
        String json = gson.toJson(systemMetricsSnapshot);
        switch (reportingContext.getProperty(LOG_LEVEL).getValue()) {
            case "info":
                getLogger().info(json);
                break;
            case "debug":
                getLogger().debug(json);
                break;
            case "warn":
                getLogger().warn(json);
                break;
            case "error":
                getLogger().error(json);
                break;
        }
    }
}
