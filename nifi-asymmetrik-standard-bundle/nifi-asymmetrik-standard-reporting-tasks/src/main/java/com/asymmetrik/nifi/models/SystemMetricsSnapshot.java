package com.asymmetrik.nifi.models;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SystemMetricsSnapshot {
    private String clusterNodeIdentifier;
    private String hostname;
    private String ipAddress;
    private ProcessGroupStatusMetric rootProcessGroupSnapshot;
    private List<ConnectionStatusMetric> connectionSnapshots = new ArrayList<>();
    private List<ProcessGroupStatusMetric> processGroupSnapshots = new ArrayList<>();
    private List<RemoteProcessGroupStatusMetric> remoteProcessGroupSnapshots = new ArrayList<>();
    private List<ProcessorStatusMetric> processorSnapshots = new ArrayList<>();
    private List<PortStatusMetric> inputPortSnapshots = new ArrayList<>();
    private List<PortStatusMetric> outputPortSnapshots = new ArrayList<>();
    private Map<File, Map<String, Object>> diskMetrics = new HashMap<>();
    private Map<String, Object> jvmMetrics = new HashMap<>();
    private Map<String, Object> machineMemory;

    public ProcessGroupStatusMetric getRootProcessGroupSnapshot() {
        return rootProcessGroupSnapshot;
    }

    public SystemMetricsSnapshot setRootProcessGroupSnapshot(ProcessGroupStatusMetric rootProcessGroupSnapshot) {
        this.rootProcessGroupSnapshot = rootProcessGroupSnapshot;
        return this;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public String getHostname() { return hostname; }

    public SystemMetricsSnapshot setHostname(String hostname) {
        this.hostname= hostname;
        return this;
    }

    public SystemMetricsSnapshot setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
        return this;
    }

    public Map<File, Map<String, Object>> getDiskMetrics() {
        return diskMetrics;
    }

    public SystemMetricsSnapshot setDiskMetrics(Map<File, Map<String, Object>> contentRepoUsage) {
        this.diskMetrics = contentRepoUsage;
        return this;
    }

    public Map<String, Object> getMachineMemory() {
        return machineMemory;
    }

    public SystemMetricsSnapshot setMachineMemory(Map<String, Object> machineMemory) {
        this.machineMemory = machineMemory;
        return this;
    }

    public String getClusterNodeIdentifier() {
        return clusterNodeIdentifier == null ? "standalone" : clusterNodeIdentifier;
    }

    public SystemMetricsSnapshot setClusterNodeIdentifier(String clusterNodeIdentifier) {
        this.clusterNodeIdentifier = clusterNodeIdentifier;
        return this;
    }

    public List<ProcessGroupStatusMetric> getProcessGroupSnapshots() {
        return processGroupSnapshots;
    }

    public SystemMetricsSnapshot setProcessGroupSnapshots(List<ProcessGroupStatusMetric> processGroupSnapshots) {
        this.processGroupSnapshots = processGroupSnapshots;
        return this;
    }

    public List<RemoteProcessGroupStatusMetric> getRemoteProcessGroupSnapshots() {
        return remoteProcessGroupSnapshots;
    }

    public SystemMetricsSnapshot setRemoteProcessGroupSnapshots(List<RemoteProcessGroupStatusMetric> remoteProcessGroupSnapshots) {
        this.remoteProcessGroupSnapshots = remoteProcessGroupSnapshots;
        return this;
    }

    public List<ConnectionStatusMetric> getConnectionSnapshots() {
        return connectionSnapshots;
    }

    public SystemMetricsSnapshot setConnectionSnapshots(List<ConnectionStatusMetric> connectionSnapshots) {
        this.connectionSnapshots = connectionSnapshots;
        return this;
    }

    public List<ProcessorStatusMetric> getProcessorSnapshots() {
        return processorSnapshots;
    }

    public SystemMetricsSnapshot setProcessorSnapshots(List<ProcessorStatusMetric> processorSnapshots) {
        this.processorSnapshots = processorSnapshots;
        return this;
    }

    public List<PortStatusMetric> getInputPortSnapshots() {
        return inputPortSnapshots;
    }

    public SystemMetricsSnapshot setInputPortSnapshots(List<PortStatusMetric> inputPortSnapshots) {
        this.inputPortSnapshots = inputPortSnapshots;
        return this;
    }

    public List<PortStatusMetric> getOutputPortSnapshots() {
        return outputPortSnapshots;
    }

    public SystemMetricsSnapshot setOutputPortSnapshots(List<PortStatusMetric> outputPortSnapshots) {
        this.outputPortSnapshots = outputPortSnapshots;
        return this;
    }

    public Map<String, Object> getJvmMetrics() {
        return jvmMetrics;
    }

    public SystemMetricsSnapshot setJvmMetrics(Map<String, Object> jvmMetrics) {
        this.jvmMetrics = jvmMetrics;
        return this;
    }
}
