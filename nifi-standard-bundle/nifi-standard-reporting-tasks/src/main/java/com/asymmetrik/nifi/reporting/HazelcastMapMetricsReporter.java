package com.asymmetrik.nifi.reporting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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
            .description("A connection pool to the InfluxDB.")
            .required(true)
            .identifiesControllerService(InfluxDatabaseService.class)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor DATABASE = new PropertyDescriptor.Builder()
            .name("database")
            .displayName("Database")
            .description("The database into which the metrics will be stored.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor CSV_HOSTS = new PropertyDescriptor.Builder()
            .name("host.ports")
            .displayName("Hazelcast Members")
            .description("Specifies a CSV of host:port of hazelcast members supporting jmx agents. ie '127.0.0.1:9999, 127.0.0.2:9998'")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor CLUSTER_NAME = new PropertyDescriptor.Builder()
            .name("cluster.name")
            .displayName("Cluster Group Name")
            .description("Specifies the name of the cluster group name to connect to")
            .defaultValue("dev")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor MAP_NAMES = new PropertyDescriptor.Builder()
            .name("map.names")
            .displayName("Map Names")
            .description("Specifies CSV of map names to retrieve stats of")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    // https://github.com/hazelcast/hazelcast/blob/master/hazelcast/src/main/java/com/hazelcast/internal/jmx/MapMBean.java
    static final PropertyDescriptor BEAN_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("attribute.names")
            .displayName("Bean Attributes for Map")
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
    private String[] mapNames;
    // array of map stat names to retrieve
    private String[] jmxBeanAttributes;

    private String database;
    private InfluxDB influxDB;

    @OnScheduled
    public void startup(ConfigurationContext context) throws IOException {
        Splitter splitter = Splitter.on(',').trimResults().omitEmptyStrings();
        Joiner joiner = Joiner.on("");

        this.jmxServiceUrls = new ArrayList<>();
        for (String hostPort : splitter.split(context.getProperty(CSV_HOSTS).getValue())) {
            jmxServiceUrls.add(new JMXServiceURL(
                    joiner.join("service:jmx:rmi://", hostPort, "/jndi/rmi://", hostPort, "/jmxrmi")));
        }

        this.clusterName = context.getProperty(CLUSTER_NAME).getValue().trim();

        this.mapNames = Iterables.toArray(splitter.split(
                context.getProperty(MAP_NAMES).getValue()), String.class);

        this.jmxBeanAttributes = context.getProperty(BEAN_ATTRIBUTES).isSet()
                ? Iterables.toArray(splitter.split(context.getProperty(BEAN_ATTRIBUTES).getValue()), String.class)
                : defaultAttributes.toArray(new String[defaultAttributes.size()]);

        this.database = context.getProperty(DATABASE).getValue();
        this.influxDB = context.getProperty(INFLUXDB_SERVICE)
                .asControllerService(InfluxDatabaseService.class)
                .getInfluxDb();

        this.influxDB.disableBatch();
    }

    @Override
    public void onTrigger(ReportingContext context) {
        for (JMXServiceURL url : jmxServiceUrls) {
            for (String mapName : mapNames) {
                try {
                    JMXConnector jmxConnector = JMXConnectorFactory.connect(url, null);
                    MBeanServerConnection connection = jmxConnector.getMBeanServerConnection();

                    ObjectName mapBeanName = findBeanName(connection, mapName, clusterName);
                    if (mapBeanName != null) {
                        AttributeList stats = connection.getAttributes(mapBeanName, jmxBeanAttributes);
                        publish(stats, url, mapName);
                    }
                } catch (Exception e) {
                    getLogger().error(e.getMessage(), e);
                }
            }
        }
    }

    private ObjectName findBeanName(MBeanServerConnection connection, String mapName, String clusterName) throws IOException {
        Set<ObjectName> names = connection.queryNames(null, null);
        for (ObjectName name : names) {
            if (isValid(name, mapName, clusterName)) {
                return name;
            }
        }

        return null;
    }

    private boolean isValid(ObjectName name, String mapName, String clusterName) {
        if (!"com.hazelcast".equals(name.getDomain())) {
            return false;
        }
        if (!mapName.equals(name.getKeyProperty("name"))) {
            return false;
        }
        if (!"IMap".equals(name.getKeyProperty("type"))) {
            return false;
        }

        // we do a contains check here because the instance name appear to be decorated with
        // additional prefix where the numerical portion may or may not be random: ie
        // _hzInstance_1_<clusterName>
        if (!name.getKeyProperty("instance").contains(clusterName)) {
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
                .tag("mapName", mapName);

        Map<String, Object> fields = new HashMap<>();
        for (Object stat : stats) {
            Attribute attribute = (Attribute) stat;
            fields.put(attribute.getName(), attribute.getValue());
        }

        builder.point(Point.measurement("hazelcast-map-stats")
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .fields(fields)
                .build()
        );

        influxDB.write(builder.build());
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(INFLUXDB_SERVICE, DATABASE, CSV_HOSTS, CLUSTER_NAME, MAP_NAMES,
                BEAN_ATTRIBUTES);
    }
}
