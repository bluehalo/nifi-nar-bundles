package com.asymmetrik.nifi.services.influxdb2;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableList;

import com.influxdb.LogLevel;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import okhttp3.OkHttpClient;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
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

    private static final PropertyDescriptor PROP_CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connection Timeout")
            .description("Max wait time for connection to remote service.")
            .required(true)
            .defaultValue("5 secs")
            .addValidator(Validator.VALID)
            .build();

    private static final PropertyDescriptor PROP_READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Read Timeout")
            .description("Max wait time for response from remote service.")
            .required(true)
            .defaultValue("10 secs")
            .addValidator(Validator.VALID)
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
        InfluxDBClientOptions.Builder options = InfluxDBClientOptions.builder()
                .url(context.getProperty(PROP_URL).evaluateAttributeExpressions().getValue())
                .authenticateToken(context.getProperty(PROP_TOKEN).evaluateAttributeExpressions().getValue().toCharArray())
                .okHttpClient(new OkHttpClient.Builder()
                        .connectTimeout(context.getProperty(PROP_CONNECT_TIMEOUT).asTimePeriod(TimeUnit.SECONDS), TimeUnit.SECONDS)
                        .readTimeout(context.getProperty(PROP_READ_TIMEOUT).asTimePeriod(TimeUnit.SECONDS), TimeUnit.SECONDS)
                        .followRedirects(true)
                );

        if (context.getProperty(PROP_LOG_LEVEL).isSet()) {
            options = options.logLevel(LogLevel.valueOf(context.getProperty(PROP_LOG_LEVEL).getValue().toUpperCase()));
        }

        influxRef.set(InfluxDBClientFactory.create(options.build()));
    }

    @OnDisabled
    public void onDisabled() {
        InfluxDBClient influxDB = influxRef.getAndSet(null);
        if (influxDB != null) {
            influxDB.close();
        }
    }

    @Override
    public InfluxDBClient getInfluxDb() {
        return influxRef.get();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(PROP_URL, PROP_TOKEN, PROP_CONNECT_TIMEOUT, PROP_READ_TIMEOUT, PROP_LOG_LEVEL);
    }
}