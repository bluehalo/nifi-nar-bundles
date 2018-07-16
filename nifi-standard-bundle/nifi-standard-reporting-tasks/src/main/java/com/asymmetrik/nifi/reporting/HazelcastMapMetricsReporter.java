package com.asymmetrik.nifi.reporting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.asymmetrik.nifi.services.influxdb.InfluxDatabaseService;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

public class HazelcastMapMetricsReporter extends AbstractReportingTask {

    private static final PropertyDescriptor INFLUXDB_SERVICE = new PropertyDescriptor.Builder()
            .name("InfluxDB Service")
            .displayName("InfluxDB Service")
            .description("The service holding the connection pool to the InfluxDB.")
            .required(true)
            .identifiesControllerService(InfluxDatabaseService.class)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor DATABASE = new PropertyDescriptor.Builder()
            .name("database")
            .displayName("InfluxDB Database Name")
            .description("The InfluxDB database into which the metrics will be stored.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor MEASUREMENT = new PropertyDescriptor.Builder()
            .name("influx.measurement")
            .displayName("InfluxDB Measurement Name")
            .description("The InfluxDB measurement name into which the metrics will be stored.")
            .required(true)
            .defaultValue("hazelcast-map-stats")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor CSV_HOSTS = new PropertyDescriptor.Builder()
            .name("host.ports")
            .displayName("Hazelcast Members")
            .description("Specifies a CSV of host:port of hazelcast members supporting jmx agents. ie '127.0.0.1:9999, 127.0.0.2:9998'")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    private static final PropertyDescriptor CLUSTER_NAME = new PropertyDescriptor.Builder()
            .name("cluster.name")
            .displayName("Hazelcast Cluster Group Name")
            .description("Specifies the name of the cluster group name to connect to")
            .defaultValue("dev")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    private static final PropertyDescriptor MAP_NAMES = new PropertyDescriptor.Builder()
            .name("map.names")
            .displayName("Hazelcast Map Names")
            .description("Specifies CSV of map names to retrieve stats of. If nothing is provided, all maps will be retrieved")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    // https://github.com/hazelcast/hazelcast/blob/master/hazelcast/src/main/java/com/hazelcast/internal/jmx/MapMBean.java
    private static final PropertyDescriptor BEAN_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("attribute.names")
            .displayName("JMX Bean Attributes for Map")
            .description("Specifies CSV of attributes to retrieve from the specified bean. If nothing is provided, all attribute values will be retrieved")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    private static List<String> defaultAttributes = Collections.unmodifiableList(Arrays.asList(
            "localOwnedEntryCount", "localBackupEntryCount", "localBackupCount", "localOwnedEntryMemoryCost",
            "localBackupEntryMemoryCost", "localHits", "localLockedEntryCount", "localDirtyEntryCount",
            "localPutOperationCount", "localGetOperationCount", "localRemoveOperationCount", "localTotalPutLatency",
            "localTotalGetLatency", "localTotalRemoveLatency", "localMaxPutLatency", "localMaxGetLatency",
            "localMaxRemoveLatency", "localEventOperationCount", "localOtherOperationCount", "localTotal",
            "localHeapCost", "size"
    ));

    // list of hazelcast member urls to connect to retrieve jmx stats
    private List<JMXServiceURL> jmxServiceUrls;
    // the cluster name (usually dev)
    private String clusterName;
    // array of map names to retrieve stats from
    private Set<String> mapNames;
    // array of map stat names to retrieve
    private String[] jmxBeanAttributes;

    private InfluxDB influxDB;
    private String database;
    private String measurement;

    @OnScheduled
    public void startup(ConfigurationContext context) throws IOException {
        Splitter splitter = Splitter.on(',').trimResults().omitEmptyStrings();
        Joiner joiner = Joiner.on("");

        this.jmxServiceUrls = new ArrayList<>();
        for (String hostPort : splitter.split(context.getProperty(CSV_HOSTS).evaluateAttributeExpressions().getValue())) {
            jmxServiceUrls.add(new JMXServiceURL(
                    joiner.join("service:jmx:rmi://", hostPort, "/jndi/rmi://", hostPort, "/jmxrmi")));
        }

        this.clusterName = context.getProperty(CLUSTER_NAME).evaluateAttributeExpressions().getValue().trim();

        this.mapNames = context.getProperty(MAP_NAMES).isSet()
                ? Sets.newHashSet(splitter.split(context.getProperty(MAP_NAMES).evaluateAttributeExpressions().getValue()))
                : new HashSet<>();

        this.jmxBeanAttributes = context.getProperty(BEAN_ATTRIBUTES).isSet()
                ? Iterables.toArray(splitter.split(context.getProperty(BEAN_ATTRIBUTES).getValue()), String.class)
                : defaultAttributes.toArray(new String[defaultAttributes.size()]);

        this.influxDB = context.getProperty(INFLUXDB_SERVICE)
                .asControllerService(InfluxDatabaseService.class)
                .getInfluxDb();
        this.database = context.getProperty(DATABASE).evaluateAttributeExpressions().getValue();
        this.measurement = context.getProperty(MEASUREMENT).getValue();

        this.influxDB.disableBatch();
    }

    @Override
    public void onTrigger(ReportingContext context) {
        for (JMXServiceURL url : jmxServiceUrls) {
            try (JMXConnector jmxConnector = JMXConnectorFactory.connect(url, null)) {
                MBeanServerConnection connection = jmxConnector.getMBeanServerConnection();

                for (ObjectName mapBeanName : findBeanNames(connection, mapNames, clusterName)) {
                    AttributeList stats = connection.getAttributes(mapBeanName, jmxBeanAttributes);
                    publish(stats, url, mapBeanName.getKeyProperty("name"));
                }
            } catch (Exception e) {
                getLogger().error(e.getMessage(), e);
            }
        }
    }

    private List<ObjectName> findBeanNames(MBeanServerConnection connection, Set<String> mapNames, String clusterName) throws IOException {
        List<ObjectName> output = new ArrayList<>();

        Set<ObjectName> names = connection.queryNames(null, null);
        for (ObjectName name : names) {
            if (isValid(name, mapNames, clusterName)) {
                output.add(name);
            }
        }

        return output;
    }

    boolean isValid(ObjectName name, Set<String> mapNames, String clusterName) {
        if (!"com.hazelcast".equals(name.getDomain())) {
            return false;
        }

        String propertyName = name.getKeyProperty("name");
        if (!mapNames.isEmpty() && !mapNames.contains(propertyName)) {
            return false;
        }

        String typeProperty = name.getKeyProperty("type");
        if (StringUtils.isBlank(typeProperty) || !"IMap".equals(name.getKeyProperty("type"))) {
            return false;
        }

        // we do a contains check here because the instance name appear to be decorated with
        // additional prefix where the numerical portion may or may not be random: ie
        // _hzInstance_1_<clusterName>
        String instanceProperty = name.getKeyProperty("instance");
        if (StringUtils.isBlank(instanceProperty) || !instanceProperty.contains(clusterName)) {
            return false;
        }

        return true;
    }

    private void publish(AttributeList stats, JMXServiceURL url, String mapName) {
        BatchPoints.Builder builder = BatchPoints
                .database(database)
                .tag("host", url.getHost())
                .tag("port", String.valueOf(url.getPort()))
                .tag("cluster", clusterName)
                .tag("mapName", StringUtils.isBlank(mapName) ? "< ---- UNNAMED MAP ---- >" : mapName);

        Map<String, Object> fields = new HashMap<>();
        for (Object stat : stats) {
            Attribute attribute = (Attribute) stat;
            fields.put(attribute.getName(), attribute.getValue());
        }

        builder.point(Point.measurement(measurement)
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .fields(fields)
                .build()
        );

        influxDB.write(builder.build());
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(INFLUXDB_SERVICE, DATABASE, MEASUREMENT, CSV_HOSTS, CLUSTER_NAME, MAP_NAMES,
                BEAN_ATTRIBUTES);
    }
}
