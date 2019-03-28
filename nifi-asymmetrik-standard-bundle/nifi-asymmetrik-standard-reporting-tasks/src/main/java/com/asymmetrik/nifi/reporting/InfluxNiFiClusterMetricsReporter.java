package com.asymmetrik.nifi.reporting;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.asymmetrik.nifi.models.ConnectionStatusMetric;
import com.asymmetrik.nifi.models.influxdb.MetricTags;
import com.asymmetrik.nifi.models.PortStatusMetric;
import com.asymmetrik.nifi.models.ProcessGroupStatusMetric;
import com.asymmetrik.nifi.models.ProcessorStatusMetric;
import com.asymmetrik.nifi.models.RemoteProcessGroupStatusMetric;
import com.asymmetrik.nifi.models.SystemMetricsSnapshot;
import com.asymmetrik.nifi.models.influxdb.MetricMeasurements;
import com.asymmetrik.nifi.services.influxdb.InfluxDatabaseService;
import com.google.common.collect.ImmutableList;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.ReportingContext;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

@Tags({"disk", "system", "monitoring", "metrics", "reporting"})
@DynamicProperty(name = "tag name", value = "tag value", description = "dynamic properties will be converted to tags and will be applied to all records")
@CapabilityDescription("Calculates the amount of storage space available for the Content Repositories and Flowfile Repository, " +
        "calculates the total count and size of the queue, and emits these metrics to InfluxDB.")

public class InfluxNiFiClusterMetricsReporter extends AbstractNiFiClusterMetricsReporter {
    private static final String PRECISION_SECONDS = "Seconds";
    private static final String PRECISION_MILLISECONDS = "Milliseconds";
    private static final String PRECISION_MICROSECONDS = "Microseconds";
    private static final String PRECISION_NANOSECONDS = "Nanoseconds";
    private static final String CONSISTENCY_LEVEL_ONE = "One";
    private static final String CONSISTENCY_LEVEL_ALL = "All";
    private static final String CONSISTENCY_LEVEL_ANY = "Any";
    private static final String CONSISTENCY_LEVEL_QUORUM = "Quorum";

    private static final PropertyDescriptor INFLUXDB_SERVICE = new PropertyDescriptor.Builder()
            .name("InfluxDB Service")
            .displayName("InfluxDB Service")
            .description("A connection pool to the InfluxDB.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .identifiesControllerService(InfluxDatabaseService.class)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor DATABASE = new PropertyDescriptor.Builder()
            .name("database")
            .displayName("Database")
            .description("The database into which the metrics will be stored.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor CONSISTENCY_LEVEL = new PropertyDescriptor.Builder()
            .name("consistency")
            .displayName("Consistency Level")
            .description("The consistency level used to store events.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .allowableValues(CONSISTENCY_LEVEL_ONE, CONSISTENCY_LEVEL_ANY, CONSISTENCY_LEVEL_QUORUM, CONSISTENCY_LEVEL_ALL)
            .defaultValue(CONSISTENCY_LEVEL_ONE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor RETENTION_POLICY = new PropertyDescriptor.Builder()
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

    private ConcurrentHashMap<String, String> globalTags;
    private AtomicReference<InfluxDB> influxRef = new AtomicReference<>();

    @OnScheduled
    public void startup(ConfigurationContext context) {
        InfluxDB influxDb = context.getProperty(INFLUXDB_SERVICE)
                .asControllerService(InfluxDatabaseService.class)
                .getInfluxDb();
        influxDb.disableBatch();
        influxRef.set(influxDb);

        globalTags = new ConcurrentHashMap<>();
        for (Map.Entry<PropertyDescriptor, String> prop : context.getProperties().entrySet()) {
            if (prop.getKey().isDynamic()) {
                globalTags.put(prop.getKey().getDisplayName(), prop.getValue());
            }
        }
    }

    @Override
    void publish(ReportingContext reportingContext, SystemMetricsSnapshot systemMetricsSnapshot) {
        PropertyValue database = reportingContext.getProperty(DATABASE);
        PropertyValue consistency = reportingContext.getProperty(CONSISTENCY_LEVEL);
        PropertyValue precision = reportingContext.getProperty(PRECISION);

        BatchPoints.Builder builder = BatchPoints
                .database(database.evaluateAttributeExpressions().getValue())
                .tag(MetricTags.CLUSTER_NODE_ID, systemMetricsSnapshot.getClusterNodeIdentifier())
                .tag(MetricTags.IP_ADDRESS, systemMetricsSnapshot.getIpAddress())
                .consistency(InfluxDB.ConsistencyLevel.valueOf(consistency.evaluateAttributeExpressions().getValue().toUpperCase()))
                .precision(TimeUnit.valueOf(precision.getValue().toUpperCase()));

        PropertyValue retention = reportingContext.getProperty(RETENTION_POLICY);
        if (retention.isSet()) {
            builder.retentionPolicy(retention.evaluateAttributeExpressions().getValue());
        }

        // Add tags pull from dynamic properties
        applyTags(builder);

        // Add data
        long now = System.currentTimeMillis();
        BatchPoints points = builder.build();
        collectMeasurements(now, systemMetricsSnapshot, points);
        influxRef.get().write(points);
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(
                INFLUXDB_SERVICE,
                DATABASE,
                RETENTION_POLICY,
                CONSISTENCY_LEVEL,
                RETENTION_POLICY,
                PRECISION,
                VOLUMES,
                PROCESS_GROUPS,
                REMOTE_PROCESS_GROUPS,
                PROCESSORS,
                CONNECTIONS,
                INPUT_PORTS,
                OUTPUT_PORTS
        );
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .displayName(propertyDescriptorName)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                .dynamic(true)
                .build();
    }

    private void applyTags(BatchPoints.Builder points) {
        for (Map.Entry<String, String> tag : globalTags.entrySet()) {
            points.tag(tag.getKey(), tag.getValue());
        }
    }

    /**
     * @param now      the timestamp to use at the metric time
     * @param snapshot the system metric snapshot
     * @param points   a set of points to which measurements will be added
     */
    void collectMeasurements(long now, SystemMetricsSnapshot snapshot, BatchPoints points) {
        collectMemoryMetrics(now, snapshot, points);
        collectJvmMetrics(now, snapshot, points);
        collectDiskUsageMetrics(now, snapshot, points);
        collectProcessGroupMetrics(now, Collections.singletonList(snapshot.getRootProcessGroupSnapshot()), points);
        collectProcessGroupMetrics(now, snapshot.getProcessGroupSnapshots(), points);
        collectRemoteProcessGroupMetrics(now, snapshot.getRemoteProcessGroupSnapshots(), points);
        collectProcessorMetrics(now, snapshot.getProcessorSnapshots(), points);
        collectConnectionMetrics(now, snapshot.getConnectionSnapshots(), points);
        collectInputPortMetrics(now, snapshot.getInputPortSnapshots(), points);
        collectOutputPortMetrics(now, snapshot.getOutputPortSnapshots(), points);
    }

    private void collectMemoryMetrics(long now, SystemMetricsSnapshot metrics, BatchPoints points) {
        points.point(Point.measurement(MetricMeasurements.MEMORY)
                .time(now, TimeUnit.MILLISECONDS)
                .fields(metrics.getMachineMemory())
                .build());
    }

    private void collectJvmMetrics(long now, SystemMetricsSnapshot snapshot, BatchPoints points) {
        points.point(Point.measurement(MetricMeasurements.JVM)
                .time(now, TimeUnit.MILLISECONDS)
                .fields(snapshot.getJvmMetrics())
                .build());
    }

    private void collectDiskUsageMetrics(long now, SystemMetricsSnapshot snapshot, BatchPoints points) {
        for (Map.Entry<File, Map<String, Object>> entry : snapshot.getDiskMetrics().entrySet()) {
            points.point(Point.measurement(MetricMeasurements.DISK)
                    .time(now, TimeUnit.MILLISECONDS)
                    .fields(entry.getValue())
                    .tag(MetricTags.PATH, entry.getKey().getAbsolutePath())
                    .build());
        }
    }

    private void collectProcessGroupMetrics(long now, List<ProcessGroupStatusMetric> metrics, BatchPoints points) {
        for (ProcessGroupStatusMetric processGroupStatusMetric : metrics) {
            points.point(Point.measurement(MetricMeasurements.PROCESS_GROUP)
                    .time(now, TimeUnit.MILLISECONDS)
                    .fields(processGroupStatusMetric.valuesAsMap())
                    .tag(processGroupStatusMetric.tags())
                    .build());
        }
    }

    private void collectRemoteProcessGroupMetrics(long now, List<RemoteProcessGroupStatusMetric> metrics, BatchPoints points) {
        for (RemoteProcessGroupStatusMetric remoteProcessGroupStatusMetric : metrics) {
            points.point(Point.measurement(MetricMeasurements.REMOTE_PROCESS_GROUP)
                    .time(now, TimeUnit.MILLISECONDS)
                    .fields(remoteProcessGroupStatusMetric.valuesAsMap())
                    .tag(remoteProcessGroupStatusMetric.tags())
                    .build());
        }
    }

    private void collectConnectionMetrics(long now, List<ConnectionStatusMetric> metrics, BatchPoints points) {
        for (ConnectionStatusMetric connectionStatusMetric : metrics) {
            points.point(Point.measurement(MetricMeasurements.CONNECTION)
                    .time(now, TimeUnit.MILLISECONDS)
                    .fields(connectionStatusMetric.valuesAsMap())
                    .tag(connectionStatusMetric.tags())
                    .build());
        }
    }

    private void collectProcessorMetrics(long now, List<ProcessorStatusMetric> metrics, BatchPoints points) {
        for (ProcessorStatusMetric processorStatusMetric : metrics) {
            points.point(Point.measurement(MetricMeasurements.PROCESSOR)
                    .time(now, TimeUnit.MILLISECONDS)
                    .fields(processorStatusMetric.valuesAsMap())
                    .tag(processorStatusMetric.tags())
                    .build());
        }
    }

    private void collectInputPortMetrics(long now, List<PortStatusMetric> metrics, BatchPoints points) {
        for (PortStatusMetric inputPortStatusMetric : metrics) {
            points.point(Point.measurement(MetricMeasurements.INPUT_PORT)
                    .time(now, TimeUnit.MILLISECONDS)
                    .fields(inputPortStatusMetric.valuesAsMap())
                    .tag(inputPortStatusMetric.tags())
                    .build());
        }
    }

    private void collectOutputPortMetrics(long now, List<PortStatusMetric> metrics, BatchPoints points) {
        for (PortStatusMetric outputPortStatusMetric : metrics) {
            points.point(Point.measurement(MetricMeasurements.OUTPUT_PORT)
                    .time(now, TimeUnit.MILLISECONDS)
                    .fields(outputPortStatusMetric.valuesAsMap())
                    .tag(outputPortStatusMetric.tags())
                    .build());
        }
    }
}
