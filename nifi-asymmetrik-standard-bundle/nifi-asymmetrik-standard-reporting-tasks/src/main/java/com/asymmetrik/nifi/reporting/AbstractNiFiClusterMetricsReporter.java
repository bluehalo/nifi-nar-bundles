package com.asymmetrik.nifi.reporting;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.amazonaws.util.EC2MetadataUtils;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.Tag;

import com.asymmetrik.nifi.models.ConnectionStatusMetric;
import com.asymmetrik.nifi.models.PortStatusMetric;
import com.asymmetrik.nifi.models.ProcessGroupStatusMetric;
import com.asymmetrik.nifi.models.ProcessorStatusMetric;
import com.asymmetrik.nifi.models.RemoteProcessGroupStatusMetric;
import com.asymmetrik.nifi.models.SystemMetricsSnapshot;
import com.asymmetrik.nifi.models.influxdb.MetricFields;
import com.yammer.metrics.core.VirtualMachineMetrics;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.util.NiFiProperties;

@SuppressWarnings("Duplicates")
abstract class AbstractNiFiClusterMetricsReporter extends AbstractReportingTask {
    static final PropertyDescriptor VOLUMES = new PropertyDescriptor.Builder()
            .name("Disk Usage")
            .description("CSV list of directories for which % disk space used will be generated.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor PROCESS_GROUPS = new PropertyDescriptor.Builder()
            .name("Process Groups")
            .displayName("Process Groups")
            .description("CSV list of process group names or UUIDs for which the aggregated statistics will be generated. " +
                    "If no value is set, statistics from all process groups will be reported. Use the \"Set empty string\" " +
                    "checkbox to exclude all process groups from being reported.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(Validator.VALID)
            .build();
    static final PropertyDescriptor REMOTE_PROCESS_GROUPS = new PropertyDescriptor.Builder()
            .name("process_group_uuids")
            .displayName("Remote Process Groups")
            .description("CSV list of remote process group UUIDs for which the aggregated statistics will be generated." +
                    "If no value is set, statistics from all remote process groups will be reported. Use the \"Set empty string\" " +
                    "checkbox to exclude all remote process groups from being reported.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(Validator.VALID)
            .build();
    static final PropertyDescriptor PROCESSORS = new PropertyDescriptor.Builder()
            .name("Processors")
            .displayName("Processors")
            .description("CSV list of processor names or UUIDs for which the aggregated statistics will be generated." +
                    "If no value is set, statistics from all processors will be reported. Use the \"Set empty string\" " +
                    "checkbox to exclude all processors from being reported.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(Validator.VALID)
            .build();
    static final PropertyDescriptor CONNECTIONS = new PropertyDescriptor.Builder()
            .name("connections_uuids")
            .displayName("Connections")
            .description("CSV list of connection UUIDs for which the aggregated statistics will be generated." +
                    "If no value is set, statistics from all connections will be reported. Use the \"Set empty string\" " +
                    "checkbox to exclude all connections from being reported.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(Validator.VALID)
            .build();
    static final PropertyDescriptor INPUT_PORTS = new PropertyDescriptor.Builder()
            .name("Input Ports")
            .displayName("Input Ports")
            .description("CSV list of input port UUIDs for which the aggregated statistics will be generated." +
                    "If no value is set, statistics from all input ports will be reported. Use the \"Set empty string\" " +
                    "checkbox to exclude all input ports from being reported.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(Validator.VALID)
            .build();
    static final PropertyDescriptor OUTPUT_PORTS = new PropertyDescriptor.Builder()
            .name("Output Ports")
            .displayName("Output Ports")
            .description("CSV list of output port UUIDs for which the aggregated statistics will be generated." +
                    "If no value is set, statistics from all output ports will be reported. Use the \"Set empty string\" " +
                    "checkbox to exclude all output ports from being reported.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(Validator.VALID)
            .build();

    private AmazonEC2 ec2;
    private List<String> volumes;
    private List<String> processGroups;
    private List<String> remoteProcessGroups;
    private List<String> processors;
    private List<String> connections;
    private List<String> inputPorts;
    private List<String> outputPorts;
    private boolean collectAllProcessGroups;
    private boolean collectAllRemoteProcessGroups;
    private boolean collectAllProcessors;
    private boolean collectAllConnections;
    private boolean collectAllInputPorts;
    private boolean collectAllOutputPorts;

    abstract void publish(ReportingContext reportingContext, SystemMetricsSnapshot systemMetricsSnapshot);

    @OnScheduled
    public void onScheduled(final ConfigurationContext context) {
        volumes = populateInputsFromCsv(context, VOLUMES);
        processGroups = populateInputsFromCsv(context, PROCESS_GROUPS);
        remoteProcessGroups = populateInputsFromCsv(context, REMOTE_PROCESS_GROUPS);
        processors = populateInputsFromCsv(context, PROCESSORS);
        connections = populateInputsFromCsv(context, CONNECTIONS);
        inputPorts = populateInputsFromCsv(context, INPUT_PORTS);
        outputPorts = populateInputsFromCsv(context, OUTPUT_PORTS);

        collectAllProcessGroups = !context.getProperty(PROCESS_GROUPS).isSet();
        collectAllRemoteProcessGroups = !context.getProperty(REMOTE_PROCESS_GROUPS).isSet();
        collectAllProcessors = !context.getProperty(PROCESSORS).isSet();
        collectAllConnections = !context.getProperty(CONNECTIONS).isSet();
        collectAllInputPorts = !context.getProperty(INPUT_PORTS).isSet();
        collectAllOutputPorts = !context.getProperty(OUTPUT_PORTS).isSet();
    }

    @Override
    public void onTrigger(final ReportingContext context) {
        NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(null, null);

        try {
            EventAccess eventAccess = context.getEventAccess();

            SystemMetricsSnapshot systemMetricsSnapshot = new SystemMetricsSnapshot()
                    .setClusterNodeIdentifier(context.getClusterNodeIdentifier())
                    .setHostname(getHostname())
                    .setRootProcessGroupSnapshot(new ProcessGroupStatusMetric(eventAccess.getControllerStatus()))
                    .setMachineMemory(computeMachineMemory())
                    .setJvmMetrics(populateJVMMetrics());

            // Add Content Repositories usages to response
            Map<File, Map<String, Object>> diskMetrics = systemMetricsSnapshot.getDiskMetrics();
            for (final Map.Entry<String, Path> entry : nifiProperties.getContentRepositoryPaths().entrySet()) {
                Path path = entry.getValue();
                diskMetrics.put(path.toFile(), computeRepositoryMetrics(path));
            }

            // Add Flowfile Repository usage to response
            Path flowFileRepository = nifiProperties.getFlowFileRepositoryPath();
            diskMetrics.put(flowFileRepository.toFile(), computeRepositoryMetrics(flowFileRepository));

            // Added additional disk usages to response
            for (String v : volumes) {
                diskMetrics.put(new File(v), computeRepositoryMetrics(new File(v)));
            }

            // Add optional process group snapshots
            if (collectAllProcessGroups || CollectionUtils.isNotEmpty(processGroups)) {
                populateAdditionalProcessGroupSnapshots(eventAccess.getControllerStatus(), systemMetricsSnapshot);
            }

            // Add optional remote process group snapshots
            if (collectAllRemoteProcessGroups || CollectionUtils.isNotEmpty(remoteProcessGroups)) {
                populateRemoteProcessGroupSnapshots(eventAccess.getControllerStatus(), systemMetricsSnapshot);
            }

            // Add optional processor metrics
            if (collectAllProcessors || CollectionUtils.isNotEmpty(processors)) {
                populateProcessorSnapshots(eventAccess.getControllerStatus(), systemMetricsSnapshot);
            }

            // Add optional connection metrics
            if (collectAllConnections || CollectionUtils.isNotEmpty(connections)) {
                populateConnectionSnapshots(eventAccess.getControllerStatus(), systemMetricsSnapshot);
            }

            // Add optional input port metrics
            if (collectAllInputPorts || CollectionUtils.isNotEmpty(inputPorts)) {
                populateInputPortSnapshots(eventAccess.getControllerStatus(), systemMetricsSnapshot);
            }

            // Add optional output port metrics
            if (collectAllOutputPorts || CollectionUtils.isNotEmpty(outputPorts)) {
                populateOutputPortSnapshots(eventAccess.getControllerStatus(), systemMetricsSnapshot);
            }

            publish(context, systemMetricsSnapshot);

        } catch (Exception e) {
            getLogger().warn("Unable to send metrics due to {}", new Object[]{e.getMessage()}, e);
        }
    }

    private void populateAdditionalProcessGroupSnapshots(ProcessGroupStatus status, SystemMetricsSnapshot systemMetricsSnapshot) {
        if (collectAllProcessGroups || processGroups.contains(status.getName()) || processGroups.contains(status.getId())) {
            systemMetricsSnapshot.getProcessGroupSnapshots().add(new ProcessGroupStatusMetric(status));
        }
        for (ProcessGroupStatus processGroupStatus : new ArrayList<>(status.getProcessGroupStatus())) {
            populateAdditionalProcessGroupSnapshots(processGroupStatus, systemMetricsSnapshot);
        }
    }

    private void populateProcessorSnapshots(ProcessGroupStatus status, SystemMetricsSnapshot systemMetricsSnapshot) {
        for (ProcessorStatus processorStatus : status.getProcessorStatus()) {
            if (collectAllProcessors || processors.contains(processorStatus.getName()) || processors.contains(processorStatus.getId())) {
                systemMetricsSnapshot.getProcessorSnapshots().add(new ProcessorStatusMetric(processorStatus));
            }
        }

        for (ProcessGroupStatus processorStatus : new ArrayList<>(status.getProcessGroupStatus())) {
            populateProcessorSnapshots(processorStatus, systemMetricsSnapshot);
        }
    }

    private void populateRemoteProcessGroupSnapshots(ProcessGroupStatus status, SystemMetricsSnapshot systemMetricsSnapshot) {
        for (RemoteProcessGroupStatus remoteProcessGroupStatus : status.getRemoteProcessGroupStatus()) {
            if (collectAllRemoteProcessGroups || remoteProcessGroups.contains(remoteProcessGroupStatus.getId())) {
                systemMetricsSnapshot.getRemoteProcessGroupSnapshots().add(new RemoteProcessGroupStatusMetric(remoteProcessGroupStatus));
            }
        }

        for (ProcessGroupStatus processorStatus : new ArrayList<>(status.getProcessGroupStatus())) {
            populateRemoteProcessGroupSnapshots(processorStatus, systemMetricsSnapshot);
        }
    }

    private void populateConnectionSnapshots(ProcessGroupStatus status, SystemMetricsSnapshot systemMetricsSnapshot) {
        for (ConnectionStatus connectionStatus : status.getConnectionStatus()) {
            if (collectAllConnections || connections.contains(connectionStatus.getId())) {
                systemMetricsSnapshot.getConnectionSnapshots().add(new ConnectionStatusMetric(connectionStatus));
            }
        }

        for (ProcessGroupStatus processorStatus : new ArrayList<>(status.getProcessGroupStatus())) {
            populateConnectionSnapshots(processorStatus, systemMetricsSnapshot);
        }
    }

    private void populateInputPortSnapshots(ProcessGroupStatus status, SystemMetricsSnapshot systemMetricsSnapshot) {
        for (PortStatus portStatus : status.getInputPortStatus()) {
            if (collectAllInputPorts || inputPorts.contains(portStatus.getId())) {
                systemMetricsSnapshot.getInputPortSnapshots().add(new PortStatusMetric(portStatus));
            }
        }

        for (ProcessGroupStatus processorStatus : new ArrayList<>(status.getProcessGroupStatus())) {
            populateInputPortSnapshots(processorStatus, systemMetricsSnapshot);
        }
    }

    private void populateOutputPortSnapshots(ProcessGroupStatus status, SystemMetricsSnapshot systemMetricsSnapshot) {
        for (PortStatus portStatus : status.getOutputPortStatus()) {
            if (collectAllOutputPorts || outputPorts.contains(portStatus.getId())) {
                systemMetricsSnapshot.getOutputPortSnapshots().add(new PortStatusMetric(portStatus));
            }
        }

        for (ProcessGroupStatus processorStatus : new ArrayList<>(status.getProcessGroupStatus())) {
            populateOutputPortSnapshots(processorStatus, systemMetricsSnapshot);
        }
    }

    private Map<String, Object> computeMachineMemory() {
        Map<String, Object> memory = new HashMap<>();
        Runtime runtime = Runtime.getRuntime();

        Double free = (double) runtime.freeMemory();
        Double total = (double) runtime.totalMemory();

        memory.put(MetricFields.FREE, free);
        memory.put(MetricFields.TOTAL, total);
        memory.put(MetricFields.USED_PERCENT, 100.0 * (1.0 - (free / total)));
        return memory;
    }

    private Map<String, Object> computeRepositoryMetrics(File file) {
        Map<String, Object> disk = new HashMap<>();

        Double totalBytes = (double) file.getTotalSpace();
        Double freeBytes = (double) file.getFreeSpace();

        disk.put(MetricFields.FREE, freeBytes);
        disk.put(MetricFields.TOTAL, totalBytes);
        disk.put(MetricFields.USED_PERCENT, 100.0 * (1.0 - freeBytes / totalBytes));
        return disk;
    }

    private Map<String, Object> computeRepositoryMetrics(final Path path) {
        return computeRepositoryMetrics(path.toFile());
    }

    private List<String> populateInputsFromCsv(ConfigurationContext context, PropertyDescriptor propertyDescriptor) {
        if (!context.getProperty(propertyDescriptor).isSet()) {
            return new ArrayList<>();
        }

        return parseInputField(context.getProperty(propertyDescriptor).evaluateAttributeExpressions().getValue());
    }

    List<String> parseInputField(String input) {
        Set<String> list = new HashSet<>();
        if (StringUtils.isEmpty(input)) {
            return new ArrayList<>();
        }

        for (String s : input.split(",")) {
            s = s.trim();
            if (StringUtils.isNotEmpty(s)) {
                list.add(s);
            }
        }
        return new ArrayList<>(list);
    }

    //virtual machine metrics
    private Map<String, Object> populateJVMMetrics() {
        VirtualMachineMetrics virtualMachineMetrics = VirtualMachineMetrics.getInstance();
        final Map<String, Object> metrics = new HashMap<>();
        metrics.put(MetricFields.JVM_UPTIME, (double) virtualMachineMetrics.uptime());
        metrics.put(MetricFields.JVM_HEAP_USED, virtualMachineMetrics.heapUsed());
        metrics.put(MetricFields.JVM_HEAP_USAGE, virtualMachineMetrics.heapUsage());
        metrics.put(MetricFields.JVM_NON_HEAP_USAGE, virtualMachineMetrics.nonHeapUsage());
        metrics.put(MetricFields.JVM_THREAD_COUNT, (double) virtualMachineMetrics.threadCount());
        metrics.put(MetricFields.JVM_DAEMON_THREAD_COUNT, (double) virtualMachineMetrics.daemonThreadCount());
        metrics.put(MetricFields.JVM_FILE_DESCRIPTOR_USAGE, virtualMachineMetrics.fileDescriptorUsage());

        for (Map.Entry<Thread.State, Double> entry : virtualMachineMetrics.threadStatePercentages().entrySet()) {
            final int normalizedValue = (int) (100 * (entry.getValue() == null ? 0 : entry.getValue()));
            switch (entry.getKey()) {
                case BLOCKED:
                    metrics.put(MetricFields.JVM_THREAD_STATES_BLOCKED, (double) normalizedValue);
                    break;
                case RUNNABLE:
                    metrics.put(MetricFields.JVM_THREAD_STATES_RUNNABLE, (double) normalizedValue);
                    break;
                case TERMINATED:
                    metrics.put(MetricFields.JVM_THREAD_STATES_TERMINATED, (double) normalizedValue);
                    break;
                case TIMED_WAITING:
                    metrics.put(MetricFields.JVM_THREAD_STATES_TIMED_WAITING, (double) normalizedValue);
                    break;
                default:
                    break;
            }
        }

        for (Map.Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : virtualMachineMetrics.garbageCollectors().entrySet()) {
            final String gcName = entry.getKey().replace(" ", "");
            final long runs = entry.getValue().getRuns();
            final long timeMS = entry.getValue().getTime(TimeUnit.MILLISECONDS);
            metrics.put(MetricFields.JVM_GC_RUNS + "." + gcName, (double) runs);
            metrics.put(MetricFields.JVM_GC_TIME + "." + gcName, (double) timeMS);
        }

        return metrics;
    }

    public String getHostname() {
        ec2 = AmazonEC2ClientBuilder.standard().build();
        //Get the Instance information using the Instance ID
        DescribeInstancesRequest request =  new DescribeInstancesRequest()
                .withInstanceIds(EC2MetadataUtils.getInstanceId());

        for(Tag tag : ec2.describeInstances(request)
                .getReservations().get(0)
                .getInstances().get(0).getTags())   // Get all of the Tags for the Instance
        {
            //Return the Tag Name which is the EC2 Host Name
            if(tag.getKey().equals("Name")){
                return tag.getValue();
            }
        }
        return "";
    }
}
