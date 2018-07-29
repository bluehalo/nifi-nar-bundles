package com.asymmetrik.nifi.services.influxdb;

import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Tags({"InfluxDB", "metrics", "time-series"})
@CapabilityDescription("Manages connections to an InfluxDB time-series database")
public class InfluxDbService extends AbstractControllerService implements InfluxDatabaseService {

    private static final Logger logger = LoggerFactory.getLogger(InfluxDbService.class);

    static final PropertyDescriptor URL = new PropertyDescriptor.Builder()
            .name("InfluxDB Host")
            .description("Hostname of the InfluxDB database server")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("http://127.0.0.1:8086")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Username")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("Password")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private volatile InfluxDB influxDb;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(URL, USERNAME, PASSWORD);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        final String url = context.getProperty(URL).evaluateAttributeExpressions().getValue();

        if (context.getProperty(USERNAME).isSet() && context.getProperty(PASSWORD).isSet()) {
            String username = context.getProperty(USERNAME).getValue();
            String password = context.getProperty(PASSWORD).getValue();
            this.influxDb = InfluxDBFactory.connect(url, username, password);
        } else {
            this.influxDb = InfluxDBFactory.connect(url);
        }
    }

    @OnDisabled
    public void onDisabled() {
        influxDb.close();
    }

    @Override
    public InfluxDB getInfluxDb() {
        return influxDb;
    }
}