package com.asymmetrik.nifi.reporting;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.asymmetrik.nifi.models.SystemMetricsSnapshot;
import com.google.common.collect.ImmutableList;

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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;

@Tags({"disk", "system", "monitoring", "metrics", "reporting", "aws", "cloudwatch"})
@DynamicProperty(name = "tag name", value = "tag value", description = "dynamic properties will be converted to dimensions and will be applied to all metrics")
@CapabilityDescription("Calculates the amount of storage space available for Content and Flowfile Repositories, " +
        "calculates the total count and size of NiFi entity queues, and emits these metrics to CloudWatch.")

public class CloudwatchNiFiClusterMetricsReporter extends AbstractNiFiClusterMetricsReporter {

    private static final PropertyDescriptor NAMESPACE = new PropertyDescriptor.Builder()
            .name("namespace")
            .displayName("Namespace")
            .description("The namespace to store the NiFi metrics.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor MEMORY = new PropertyDescriptor.Builder()
            .name("memory")
            .displayName("Capture System Memory")
            .description("Boolean value used to choose whether to log each NiFi nodes' system memory.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    private static final PropertyDescriptor JVM = new PropertyDescriptor.Builder()
            .name("jvm")
            .displayName("Capture JVM Metrics")
            .description("Boolean value used to choose whether to log each NiFi nodes' JVM properties.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    private AmazonCloudWatch cloudWatch;
    private String namespace;
    private boolean collectsMemory;
    private boolean collectsJVMMetrics;
    private List<Dimension> dynamicDimensions;

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(NAMESPACE, MEMORY, JVM, PROCESS_GROUPS, REMOTE_PROCESS_GROUPS, PROCESSORS, CONNECTIONS, INPUT_PORTS, OUTPUT_PORTS, VOLUMES);
    }

    @OnScheduled
    public void startup(ConfigurationContext context) {
        namespace = context.getProperty(NAMESPACE).getValue();
        collectsMemory = context.getProperty(MEMORY).asBoolean();
        collectsJVMMetrics = context.getProperty(JVM).asBoolean();

        AWSCredentialsProvider provider = new DefaultAWSCredentialsProviderChain();

        cloudWatch = AmazonCloudWatchClientBuilder.standard()
                .withCredentials(provider)
                .build();

        dynamicDimensions = new ArrayList<>();
        context.getProperties().forEach((property, value) -> {
            // For each dynamic property, create a new dimension with that key/value pair
            if (property.isDynamic() && value != null && !value.isEmpty()) {
                dynamicDimensions.add(new Dimension()
                        .withName(property.getDisplayName())
                        .withValue(value)
                );
            }
        });  
    }

    @Override
    void publish(ReportingContext reportingContext, SystemMetricsSnapshot snapshot) {

        Date now = new Date(System.currentTimeMillis());
        List<Dimension> dimensions = new ArrayList<>();

        // IP Address is always included in the dimensions
        dimensions.add(new Dimension()
                .withName("Ip Address")
                .withValue(snapshot.getIpAddress())
        );
        // Extra dimensions are created in startup and added here
        dimensions.addAll(dynamicDimensions);

        List<MetricDatum> metrics = collectMeasurements(now, snapshot, dimensions);

        sendToCloudWatch(metrics);
    }

    public List<MetricDatum> collectMeasurements(Date now, SystemMetricsSnapshot snapshot, List<Dimension> dimensions) {

        List<MetricDatum> toCloudwatch = new ArrayList<>();
        // System Memory Logging
        if (collectsMemory) {
            getMetrics("System Memory", snapshot.getMachineMemory(), now, dimensions, toCloudwatch);
        }
        // System JVM Logging
        if (collectsJVMMetrics) {
            getMetrics("System JVM", snapshot.getJvmMetrics(), now, dimensions, toCloudwatch);
        }
        // Selected Process Group Logging
        snapshot.getProcessGroupSnapshots().forEach((groupData) -> 
            // Send metrics for each processor group in the CSV list
            getMetrics(groupData.getProcessGroupName(), groupData.valuesAsMap(), now, dimensions, toCloudwatch)
        );
        // Selected Remote Process Group Logging
        snapshot.getRemoteProcessGroupSnapshots().forEach((groupData) -> 
            // Send metrics for each remote process group in the CSV list
            getMetrics(groupData.getRemoteProcessGroupName(), groupData.valuesAsMap(), now, dimensions, toCloudwatch)
        );
        // Selected Processor Logging
        snapshot.getProcessorSnapshots().forEach((processorData) -> 
            // Send metrics for each processor in the CSV list
            getMetrics(processorData.getProcessorName(), processorData.valuesAsMap(), now, dimensions, toCloudwatch)
        );
        // Selected Connection logging
        snapshot.getConnectionSnapshots().forEach((connectionData) -> 
            // Send metrics for each connection in the CSV list
            getMetrics(connectionData.getConnectionName(), connectionData.valuesAsMap(), now, dimensions, toCloudwatch)
        );
        // Selected Input Port logging
        snapshot.getInputPortSnapshots().forEach((portData) -> 
            // Send metrics for each input port in the CSV list
            getMetrics(portData.getInputPortName(), portData.valuesAsMap(), now, dimensions, toCloudwatch)
        );
        // Selected Output Port logging
        snapshot.getOutputPortSnapshots().forEach((portData) -> 
            // Send metrics for each output port in the CSV list
            getMetrics(portData.getInputPortName(), portData.valuesAsMap(), now, dimensions, toCloudwatch)
        );
        // Selected Volume Logging
        snapshot.getDiskMetrics().entrySet().forEach((fileData) ->
            // Send metrics for each volume location in the CSV list
            getMetrics(fileData.getKey().getAbsolutePath(), fileData.getValue(), now, dimensions, toCloudwatch)
        );

        return toCloudwatch;
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
     * Converts the metrics in the given map into metrics digestable to Amazon CloudWatch
     * @param metricName   the name to use in the "Metric Location" dimension for the list of metrics
     * @param metrics      the system metric to use, should be a map<String, double>
     * @param now          the date to use at the metric time
     * @param dimensions   the list of dimensions used in addition to the "Metric Location" dimension
     * @param toCloudwatch the list to add all the metrics to, which should get sent to cloudwatch
     */
    private void getMetrics(String metricName, Map<String, Object> metrics, Date now, List<Dimension> dimensions, List<MetricDatum> toCloudwatch) {
        // Don't try to send metrics with missing dimension keys to CloudWatch, it doesn't like that
        // If the thing doesn't have a name, it's not probably not important enough to log anyways
        // The main case for this is unnamed connections, so name them if you want their logs
        if(metricName.isEmpty()) {
            return;
        }

        List<Dimension> metricDimensions = new ArrayList<>(dimensions);

        metricDimensions.add(new Dimension()
                .withName("Metric Location")
                .withValue(metricName)
        );

        metrics.forEach((name, value) -> {
            // Make sure the key is defined, and that the metric value is a double
            if(name != null && !name.isEmpty() && value != null && value instanceof Double) {
                toCloudwatch.add(new MetricDatum()
                        .withMetricName(name)
                        .withValue((double) value)
                        .withDimensions(metricDimensions)
                        .withTimestamp(now)
                );
            }
        });
            
    }

    /**
     * Sends the given list to CloudWatch, under the namespace given in the property values
     */
    private void sendToCloudWatch(List<MetricDatum> toCloudwatch) {
         // CloudWatch has a hard limit of 20 allowed metrics for one PutMetricDataRequest.
        final int CLOUDWATCH_LIMIT = 20;
        
        // Loop over the list of metrics, sending over CLOUDWATCH_LIMIT number of metrics at a time
        for (int startIndex = 0; startIndex < toCloudwatch.size(); startIndex += CLOUDWATCH_LIMIT) {

            // If the list deosn't have CLOUDWATCH_LIMIT elements left, only send the remaining ones
            int endIndex = toCloudwatch.size() - startIndex < CLOUDWATCH_LIMIT 
                    ? toCloudwatch.size() 
                    : startIndex + CLOUDWATCH_LIMIT;
            
            PutMetricDataRequest request = new PutMetricDataRequest()
                    .withNamespace(namespace)
                    .withMetricData(toCloudwatch.subList(startIndex, endIndex));
            
            cloudWatch.putMetricData(request);
        }
    }

    void setCollectsMemory (boolean collectsMemory) {
        this.collectsMemory = collectsMemory;
    }

    void setCollectsJVMMetrics (boolean collectsJVMMetrics) {
        this.collectsJVMMetrics = collectsJVMMetrics;
    }
}
