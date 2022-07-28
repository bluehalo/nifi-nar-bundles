package com.asymmetrik.nifi.reporting;

import com.asymmetrik.nifi.models.*;
import com.asymmetrik.nifi.models.influxdb.MetricMeasurements;
import com.asymmetrik.nifi.models.influxdb.MetricTags;
import com.asymmetrik.nifi.services.influxdb2.InfluxClientApi;
import com.google.common.collect.ImmutableList;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.ReportingContext;

import java.io.File;
import java.time.Instant;
import java.util.*;

@Tags({"disk", "system", "monitoring", "metrics", "reporting"})
@DynamicProperty(name = "tag name", value = "tag value", description = "dynamic properties will be converted to tags and will be applied to all records")
@CapabilityDescription("Calculates the amount of storage space available for the Content Repositories and Flowfile Repository, " +
        "calculates the total count and size of the queue, and emits these metrics to InfluxDB V2.")
public class InfluxV2NiFiClusterMetricsReporter extends AbstractNiFiClusterMetricsReporter {
    private static final PropertyDescriptor PROP_INFLUXDB_SERVICE = new PropertyDescriptor.Builder()
            .name("InfluxDBv2 Service")
            .displayName("InfluxDBv2 Service")
            .description("The Controller Service that is used to communicate with InfluxDBv2")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .identifiesControllerService(InfluxClientApi.class)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor PROP_ORG = new PropertyDescriptor.Builder()
            .name("org")
            .displayName("Organization")
            .description("Organization")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    static final PropertyDescriptor PROP_BUCKET = new PropertyDescriptor.Builder()
            .name("bucket")
            .displayName("Bucket")
            .description("Bucket")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    static final PropertyDescriptor PROP_PRECISION = new PropertyDescriptor.Builder()
            .name("precision")
            .displayName("Precision")
            .description("The temporal precision for metrics.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("ns", "ms", "s")
            .defaultValue("ms")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private InfluxDBClient influxClient;
    private Map<String, String> globalTags;

    @OnScheduled
    public void startup(ConfigurationContext context) {
        influxClient = context.getProperty(PROP_INFLUXDB_SERVICE)
                .asControllerService(InfluxClientApi.class)
                .getInfluxDb();

        globalTags = new HashMap<>();
        for (Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            PropertyDescriptor prop = entry.getKey();
            if (prop.isDynamic()) {
                String value = context.getProperty(prop).evaluateAttributeExpressions().getValue();
                globalTags.put(prop.getDisplayName(), value);
            }
        }
    }

    @Override
    void publish(ReportingContext reportingContext, SystemMetricsSnapshot systemMetricsSnapshot) {

        // add common tags
        Map<String, String> tags = new HashMap<>();
        tags.put(MetricTags.CLUSTER_NODE_ID, systemMetricsSnapshot.getClusterNodeIdentifier());
        tags.put(MetricTags.IP_ADDRESS, systemMetricsSnapshot.getIpAddress());

        // add dynamic property tags
        tags.putAll(globalTags);

        // Collect data points
        WritePrecision precision = WritePrecision.fromValue(reportingContext.getProperty(PROP_PRECISION).getValue());
        Instant now = Instant.now();
        List<Point> points = collectMeasurements(now, tags, precision, systemMetricsSnapshot);

        String bucket = reportingContext.getProperty(PROP_BUCKET).evaluateAttributeExpressions().getValue();
        String org = reportingContext.getProperty(PROP_ORG).evaluateAttributeExpressions().getValue();
        WriteApiBlocking writeApi = influxClient.getWriteApiBlocking();
        writeApi.writePoints(bucket, org, points);
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(
                PROP_INFLUXDB_SERVICE,
                PROP_ORG,
                PROP_BUCKET,
                PROP_PRECISION,
                VOLUMES,
                PROCESS_GROUPS,
                REMOTE_PROCESS_GROUPS,
                PROCESSORS,
                CONNECTIONS,
                INPUT_PORTS,
                OUTPUT_PORTS,
                INCLUDE_FILE_DESCRIPTOR_METRICS
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

    /**
     * @param now      the timestamp to use at the metric time
     * @param snapshot the system metric snapshot
     * @return points   a set of points to which measurements were added
     */
    List<Point> collectMeasurements(Instant now, Map<String, String> tags, WritePrecision precision, SystemMetricsSnapshot snapshot) {
        List<Point> points = new ArrayList<>();
        collectMemoryMetrics(now, tags, precision, snapshot, points);
        collectJvmMetrics(now, tags, precision, snapshot, points);
        collectDiskUsageMetrics(now, tags, precision, snapshot, points);
        collectProcessGroupMetrics(now, tags, precision, Collections.singletonList(snapshot.getRootProcessGroupSnapshot()), points);
        collectProcessGroupMetrics(now, tags, precision, snapshot.getProcessGroupSnapshots(), points);
        collectRemoteProcessGroupMetrics(now, tags, precision, snapshot.getRemoteProcessGroupSnapshots(), points);
        collectProcessorMetrics(now, tags, precision, snapshot.getProcessorSnapshots(), points);
        collectConnectionMetrics(now, tags, precision, snapshot.getConnectionSnapshots(), points);
        collectInputPortMetrics(now, tags, precision, snapshot.getInputPortSnapshots(), points);
        collectOutputPortMetrics(now, tags, precision, snapshot.getOutputPortSnapshots(), points);
        return points;
    }

    private void collectMemoryMetrics(Instant now, Map<String, String> tags, WritePrecision precision, SystemMetricsSnapshot metrics, List<Point> points) {
        points.add(Point.measurement(MetricMeasurements.MEMORY)
                .time(now, precision)
                .addFields(metrics.getMachineMemory())
                .addTags(tags)
        );
    }

    private void collectJvmMetrics(Instant now, Map<String, String> tags, WritePrecision precision, SystemMetricsSnapshot snapshot, List<Point> points) {
        points.add(Point.measurement(MetricMeasurements.JVM)
                .time(now, precision)
                .addFields(snapshot.getJvmMetrics())
                .addTags(tags)
        );
    }

    private void collectDiskUsageMetrics(Instant now, Map<String, String> tags, WritePrecision precision, SystemMetricsSnapshot snapshot, List<Point> points) {
        for (Map.Entry<File, Map<String, Object>> entry : snapshot.getDiskMetrics().entrySet()) {
            points.add(Point.measurement(MetricMeasurements.DISK)
                    .time(now, precision)
                    .addFields(entry.getValue())
                    .addTag(MetricTags.PATH, entry.getKey().getAbsolutePath())
                    .addTags(tags)
            );
        }
    }

    private void collectProcessGroupMetrics(Instant now, Map<String, String> tags, WritePrecision precision, List<ProcessGroupStatusMetric> metrics, List<Point> points) {
        for (ProcessGroupStatusMetric processGroupStatusMetric : metrics) {
            points.add(Point.measurement(MetricMeasurements.PROCESS_GROUP)
                    .time(now, precision)
                    .addFields(processGroupStatusMetric.valuesAsMap())
                    .addTags(processGroupStatusMetric.tags())
                    .addTags(tags)
            );
        }
    }

    private void collectRemoteProcessGroupMetrics(Instant now, Map<String, String> tags, WritePrecision precision, List<RemoteProcessGroupStatusMetric> metrics, List<Point> points) {
        for (RemoteProcessGroupStatusMetric remoteProcessGroupStatusMetric : metrics) {
            points.add(Point.measurement(MetricMeasurements.REMOTE_PROCESS_GROUP)
                    .time(now, precision)
                    .addFields(remoteProcessGroupStatusMetric.valuesAsMap())
                    .addTags(remoteProcessGroupStatusMetric.tags())
                    .addTags(tags)
            );
        }
    }

    private void collectConnectionMetrics(Instant now, Map<String, String> tags, WritePrecision precision, List<ConnectionStatusMetric> metrics, List<Point> points) {
        for (ConnectionStatusMetric connectionStatusMetric : metrics) {
            points.add(Point.measurement(MetricMeasurements.CONNECTION)
                    .time(now, precision)
                    .addFields(connectionStatusMetric.valuesAsMap())
                    .addTags(connectionStatusMetric.tags())
                    .addTags(tags)
            );
        }
    }

    private void collectProcessorMetrics(Instant now, Map<String, String> tags, WritePrecision precision, List<ProcessorStatusMetric> metrics, List<Point> points) {
        for (ProcessorStatusMetric processorStatusMetric : metrics) {
            points.add(Point.measurement(MetricMeasurements.PROCESSOR)
                    .time(now, precision)
                    .addFields(processorStatusMetric.valuesAsMap())
                    .addTags(processorStatusMetric.tags())
                    .addTags(tags)
            );
        }
    }

    private void collectInputPortMetrics(Instant now, Map<String, String> tags, WritePrecision precision, List<PortStatusMetric> metrics, List<Point> points) {
        for (PortStatusMetric inputPortStatusMetric : metrics) {
            points.add(Point.measurement(MetricMeasurements.INPUT_PORT)
                    .time(now, precision)
                    .addFields(inputPortStatusMetric.valuesAsMap())
                    .addTags(inputPortStatusMetric.tags())
                    .addTags(tags)
            );
        }
    }

    private void collectOutputPortMetrics(Instant now, Map<String, String> tags, WritePrecision precision, List<PortStatusMetric> metrics, List<Point> points) {
        for (PortStatusMetric outputPortStatusMetric : metrics) {
            points.add(Point.measurement(MetricMeasurements.OUTPUT_PORT)
                    .time(now, precision)
                    .addFields(outputPortStatusMetric.valuesAsMap())
                    .addTags(outputPortStatusMetric.tags())
                    .addTags(tags)
            );
        }
    }
}
