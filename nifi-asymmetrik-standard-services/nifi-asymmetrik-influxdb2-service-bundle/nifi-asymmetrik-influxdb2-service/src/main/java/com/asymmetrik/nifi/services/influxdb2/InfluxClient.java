package com.asymmetrik.nifi.services.influxdb2;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableList;

import com.influxdb.LogLevel;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

@SuppressWarnings("Duplicates")
@Tags({"InfluxDB", "metrics", "time-series"})
@CapabilityDescription("Manages connections to an InfluxDB V2 time-series database.  Please note that" +
        "this controller service is compatible with version 2.x and is incompatible with 1.x")
public class InfluxClient extends AbstractControllerService implements InfluxClientApi {
    private static final String LOG_LEVEL_NONE = "None";
    private static final String LOG_LEVEL_BASIC = "Basic";
    private static final String LOG_LEVEL_HEADERS = "Headers";
    private static final String LOG_LEVEL_BODY = "Body";

    private static final PropertyDescriptor PROP_URL = new PropertyDescriptor.Builder()
            .name("url")
            .displayName("InfluxDB Host")
            .description("Hostname of the InfluxDB database server")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("http://127.0.0.1:8086")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    private static final PropertyDescriptor PROP_TOKEN = new PropertyDescriptor.Builder()
            .name("token")
            .displayName("API Token")
            .description("API Token")
            .sensitive(true)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor PROP_LOG_LEVEL = new PropertyDescriptor.Builder()
            .name("log_level")
            .displayName("Log Level")
            .description("The Log Level")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .allowableValues(LOG_LEVEL_NONE, LOG_LEVEL_BASIC, LOG_LEVEL_HEADERS, LOG_LEVEL_BODY)
            .defaultValue(LOG_LEVEL_NONE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private final AtomicReference<InfluxDBClient> influxRef = new AtomicReference<>();

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        PropertyValue urlProp = context.getProperty(PROP_URL);
        PropertyValue token = context.getProperty(PROP_TOKEN);

        String url = urlProp.evaluateAttributeExpressions().getValue();
        influxRef.set(InfluxDBClientFactory.create(url, token.getValue().toCharArray()));
        InfluxDBClient influxDB = influxRef.get();

        PropertyValue logLevelProperty = context.getProperty(PROP_LOG_LEVEL);
        if (logLevelProperty.isSet()) {
            influxDB.setLogLevel(LogLevel.valueOf(logLevelProperty.getValue().toUpperCase()));
        }
    }

    @OnDisabled
    public void onDisabled() {
        InfluxDBClient influxDB = influxRef.get();
        if (influxDB != null) {
            influxRef.get().close();
            influxRef.set(null);
        }
    }

    @Override
    public InfluxDBClient getInfluxDb() {
        return influxRef.get();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(PROP_URL, PROP_TOKEN, PROP_LOG_LEVEL);
    }
}