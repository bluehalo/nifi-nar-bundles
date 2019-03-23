package com.asymmetrik.nifi.services.influxdb;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableList;

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
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

import okhttp3.OkHttpClient;

@SuppressWarnings("Duplicates")
@Tags({"InfluxDB", "metrics", "time-series"})
@CapabilityDescription("Manages connections to an InfluxDB time-series database")
public class InfluxDbService extends AbstractControllerService implements InfluxDatabaseService {
    private static final String LOG_LEVEL_NONE = "None";
    private static final String LOG_LEVEL_BASIC = "Basic";
    private static final String LOG_LEVEL_HEADERS = "Headers";
    private static final String LOG_LEVEL_FULL = "Full";

    private static final PropertyDescriptor PROP_URL = new PropertyDescriptor.Builder()
            .name("InfluxDB Host")
            .description("Hostname of the InfluxDB database server")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("http://127.0.0.1:8086")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    private static final PropertyDescriptor PROP_TIMEOUT = new PropertyDescriptor.Builder()
            .name("InfluxDB Max Connection Time Out (seconds)")
            .description("The maximum time for establishing connection to the InfluxDB")
            .defaultValue("0 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .sensitive(false)
            .build();

    private static final PropertyDescriptor PROP_USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Username")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor PROP_PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("Password")
            .sensitive(true)
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor PROP_LOG_LEVEL = new PropertyDescriptor.Builder()
            .name("log_level")
            .displayName("Log Level")
            .description("The Log Level")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .allowableValues(LOG_LEVEL_NONE, LOG_LEVEL_BASIC, LOG_LEVEL_HEADERS, LOG_LEVEL_FULL)
            .defaultValue(LOG_LEVEL_NONE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private AtomicReference<InfluxDB> influxRef = new AtomicReference<>();

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        PropertyValue urlProp = context.getProperty(PROP_URL);
        PropertyValue timeoutProp = context.getProperty(PROP_TIMEOUT);
        PropertyValue usernameProp = context.getProperty(PROP_USERNAME);
        PropertyValue passwordProp = context.getProperty(PROP_PASSWORD);

        String url = urlProp.evaluateAttributeExpressions().getValue();
        long connectionTimeout = timeoutProp.asTimePeriod(TimeUnit.SECONDS);
        OkHttpClient.Builder builder = new OkHttpClient.Builder().connectTimeout(connectionTimeout, TimeUnit.SECONDS);
        if (usernameProp.isSet() && passwordProp.isSet()) {
            String username = usernameProp.evaluateAttributeExpressions().getValue();
            String password = passwordProp.evaluateAttributeExpressions().getValue();
            InfluxDB influx = InfluxDBFactory.connect(url, username, password, builder);
            influxRef.set(influx);
        } else {
            InfluxDB influx = InfluxDBFactory.connect(url, builder);
            influxRef.set(influx);
        }
        InfluxDB influxDB = influxRef.get();
        influxDB.disableBatch();

        PropertyValue logLevelProperty = context.getProperty(PROP_LOG_LEVEL);
        if (logLevelProperty.isSet()) {
            influxDB.setLogLevel(InfluxDB.LogLevel.valueOf(logLevelProperty.getValue().toUpperCase()));
        }
    }

    @OnDisabled
    public void onDisabled() {
        InfluxDB influxDB = influxRef.get();
        if (influxDB != null) {
            influxRef.get().close();
            influxRef.set(null);
        }
    }

    @Override
    public InfluxDB getInfluxDb() {
        return influxRef.get();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(PROP_URL, PROP_TIMEOUT, PROP_USERNAME, PROP_PASSWORD, PROP_LOG_LEVEL);
    }
}