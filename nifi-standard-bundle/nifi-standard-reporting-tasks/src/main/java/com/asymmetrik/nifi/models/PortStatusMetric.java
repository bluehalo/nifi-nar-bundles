package com.asymmetrik.nifi.models;

import java.util.HashMap;
import java.util.Map;

import com.asymmetrik.nifi.models.influxdb.MetricFields;
import com.asymmetrik.nifi.models.influxdb.MetricTags;

import org.apache.nifi.controller.status.PortStatus;

public class PortStatusMetric {
    private String id;
    private String groupId;
    private String inputPortName;
    private Double activeThreadCount;
    private Double bytesReceived;
    private Double bytesSent;
    private Double flowFilesReceived;
    private Double flowFilesSent;
    private Double inputBytes;
    private Double inputCount;
    private Double outputBytes;
    private Double outputCount;
    private Double runStatus;

    public PortStatusMetric() {
    }

    public PortStatusMetric(PortStatus portStatus) {
        setId(portStatus.getId());
        setGroupId(portStatus.getGroupId());
        setInputPortName(portStatus.getName());
        setActiveThreadCount(Double.valueOf(portStatus.getActiveThreadCount()));
        setBytesReceived((double) portStatus.getBytesReceived());
        setBytesSent((double) portStatus.getBytesSent());
        setFlowFilesReceived((double) portStatus.getFlowFilesReceived());
        setFlowFilesSent((double) portStatus.getFlowFilesSent());
        setInputBytes((double) portStatus.getInputBytes());
        setInputCount((double) portStatus.getInputCount());
        setOutputBytes((double) portStatus.getOutputBytes());
        setOutputCount((double) portStatus.getOutputCount());
        setRunStatus((double) portStatus.getRunStatus().ordinal());
    }

    public String getId() {
        return id;
    }

    public PortStatusMetric setId(String id) {
        this.id = id;
        return this;
    }

    public String getGroupId() {
        return groupId;
    }

    public PortStatusMetric setGroupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public String getInputPortName() {
        return inputPortName;
    }

    public PortStatusMetric setInputPortName(String inputPortName) {
        this.inputPortName = inputPortName;
        return this;
    }

    public Double getActiveThreadCount() {
        return activeThreadCount;
    }

    public PortStatusMetric setActiveThreadCount(Double activeThreadCount) {
        this.activeThreadCount = activeThreadCount;
        return this;
    }

    public Double getBytesReceived() {
        return bytesReceived;
    }

    public PortStatusMetric setBytesReceived(Double bytesReceived) {
        this.bytesReceived = bytesReceived;
        return this;
    }

    public Double getBytesSent() {
        return bytesSent;
    }

    public PortStatusMetric setBytesSent(Double bytesSent) {
        this.bytesSent = bytesSent;
        return this;
    }

    public Double getFlowFilesReceived() {
        return flowFilesReceived;
    }

    public PortStatusMetric setFlowFilesReceived(Double flowFilesReceived) {
        this.flowFilesReceived = flowFilesReceived;
        return this;
    }

    public Double getFlowFilesSent() {
        return flowFilesSent;
    }

    public PortStatusMetric setFlowFilesSent(Double flowFilesSent) {
        this.flowFilesSent = flowFilesSent;
        return this;
    }

    public Double getInputBytes() {
        return inputBytes;
    }

    public PortStatusMetric setInputBytes(Double inputBytes) {
        this.inputBytes = inputBytes;
        return this;
    }

    public Double getInputCount() {
        return inputCount;
    }

    public PortStatusMetric setInputCount(Double inputCount) {
        this.inputCount = inputCount;
        return this;
    }

    public Double getOutputBytes() {
        return outputBytes;
    }

    public PortStatusMetric setOutputBytes(Double outputBytes) {
        this.outputBytes = outputBytes;
        return this;
    }

    public Double getOutputCount() {
        return outputCount;
    }

    public PortStatusMetric setOutputCount(Double outputCount) {
        this.outputCount = outputCount;
        return this;
    }

    public Double getRunStatus() {
        return runStatus;
    }

    public PortStatusMetric setRunStatus(Double runStatus) {
        this.runStatus = runStatus;
        return this;
    }

    public Map<String, Object> valuesAsMap() {
        Map<String, Object> values = new HashMap<>();
        values.put(MetricFields.ACTIVE_THREAD_COUNT, getActiveThreadCount());
        values.put(MetricFields.BYTES_RECEIVED, getBytesReceived());
        values.put(MetricFields.BYTES_SENT, getBytesSent());
        values.put(MetricFields.FLOWFILES_RECEIVED, getFlowFilesReceived());
        values.put(MetricFields.FLOWFILES_SENT, getFlowFilesSent());
        values.put(MetricFields.INPUT_BYTES, getInputBytes());
        values.put(MetricFields.INPUT_COUNT, getInputCount());
        values.put(MetricFields.OUTPUT_BYTES, getOutputBytes());
        values.put(MetricFields.OUTPUT_COUNT, getOutputCount());
        values.put(MetricFields.RUN_STATUS, getRunStatus());
        return values;
    }

    public Map<String, String> tags() {
        Map<String, String> tags = new HashMap<>();
        tags.put(MetricTags.GROUP_ID, getGroupId());
        tags.put(MetricTags.PORT_NAME, getInputPortName());
        tags.put(MetricTags.ID, getId());
        return tags;
    }
}
