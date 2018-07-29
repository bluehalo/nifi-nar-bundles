package com.asymmetrik.nifi.models;

import java.util.HashMap;
import java.util.Map;

import com.asymmetrik.nifi.models.influxdb.MetricFields;
import com.asymmetrik.nifi.models.influxdb.MetricTags;

import org.apache.nifi.controller.status.ConnectionStatus;

public class ConnectionStatusMetric {
    private String id;
    private String groupId;
    private String connectionName;
    private Double backPressureBytesThreshold;
    private Double backPressureObjectThreshold;
    private Double inputBytes;
    private Double inputCount;
    private Double maxQueuedBytes;
    private Double maxQueuedCount;
    private Double outputBytes;
    private Double outputCount;
    private Double queuedBytes;
    private Double queued;

    public ConnectionStatusMetric() {
    }

    public ConnectionStatusMetric(ConnectionStatus connectionStatusStatus) {
        setId(connectionStatusStatus.getId());
        setGroupId(connectionStatusStatus.getGroupId());
        setConnectionName(connectionStatusStatus.getName());
        setBackPressureBytesThreshold((double) connectionStatusStatus.getBackPressureBytesThreshold());
        setBackPressureObjectThreshold((double) connectionStatusStatus.getBackPressureObjectThreshold());
        setInputBytes((double) connectionStatusStatus.getInputBytes());
        setInputCount((double) connectionStatusStatus.getInputCount());
        setMaxQueuedBytes((double) connectionStatusStatus.getMaxQueuedBytes());
        setMaxQueuedCount((double) connectionStatusStatus.getMaxQueuedCount());
        setOutputBytes((double) connectionStatusStatus.getOutputBytes());
        setOutputCount((double) connectionStatusStatus.getOutputCount());
        setQueuedBytes((double) connectionStatusStatus.getQueuedBytes());
        setQueued((double) connectionStatusStatus.getQueuedCount());
    }

    public String getId() {
        return id;
    }

    public ConnectionStatusMetric setId(String id) {
        this.id = id;
        return this;
    }

    public String getGroupId() {
        return groupId;
    }

    public ConnectionStatusMetric setGroupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public String getConnectionName() {
        return connectionName;
    }

    public ConnectionStatusMetric setConnectionName(String connectionName) {
        this.connectionName = connectionName;
        return this;
    }

    public Double getBackPressureBytesThreshold() {
        return backPressureBytesThreshold;
    }

    public void setBackPressureBytesThreshold(Double backPressureBytesThreshold) {
        this.backPressureBytesThreshold = backPressureBytesThreshold;
    }

    public Double getBackPressureObjectThreshold() {
        return backPressureObjectThreshold;
    }

    public void setBackPressureObjectThreshold(Double backPressureObjectThreshold) {
        this.backPressureObjectThreshold = backPressureObjectThreshold;
    }

    public Double getInputBytes() {
        return inputBytes;
    }

    public void setInputBytes(Double inputBytes) {
        this.inputBytes = inputBytes;
    }

    public Double getInputCount() {
        return inputCount;
    }

    public void setInputCount(Double inputCount) {
        this.inputCount = inputCount;
    }

    public Double getMaxQueuedBytes() {
        return maxQueuedBytes;
    }

    public void setMaxQueuedBytes(Double maxQueuedBytes) {
        this.maxQueuedBytes = maxQueuedBytes;
    }

    public Double getMaxQueuedCount() {
        return maxQueuedCount;
    }

    public void setMaxQueuedCount(Double maxQueuedCount) {
        this.maxQueuedCount = maxQueuedCount;
    }

    public Double getOutputBytes() {
        return outputBytes;
    }

    public void setOutputBytes(Double outputBytes) {
        this.outputBytes = outputBytes;
    }

    public Double getOutputCount() {
        return outputCount;
    }

    public void setOutputCount(Double outputCount) {
        this.outputCount = outputCount;
    }

    public Double getQueuedBytes() {
        return queuedBytes;
    }

    public void setQueuedBytes(Double queuedBytes) {
        this.queuedBytes = queuedBytes;
    }

    public Double getQueued() {
        return queued;
    }

    public void setQueued(Double queued) {
        this.queued = queued;
    }

    public Map<String, Object> valuesAsMap() {
        Map<String, Object> values = new HashMap<>();
        values.put(MetricFields.BACKPRESSURE_BYTES_THRESHOLD, getBackPressureBytesThreshold());
        values.put(MetricFields.BACKPRESSURE_OBJECT_THRESHOLD, getBackPressureObjectThreshold());
        values.put(MetricFields.INPUT_BYTES, getInputBytes());
        values.put(MetricFields.INPUT_COUNT, getInputCount());
        values.put(MetricFields.MAX_QUEUED_BYTES, getMaxQueuedBytes());
        values.put(MetricFields.MAX_QUEUED_COUNT, getMaxQueuedCount());
        values.put(MetricFields.OUTPUT_BYTES, getOutputBytes());
        values.put(MetricFields.OUTPUT_COUNT, getOutputCount());
        values.put(MetricFields.QUEUED_BYTES, getQueuedBytes());
        values.put(MetricFields.QUEUED, getQueued());
        return values;
    }

    public Map<String, String> tags() {
        Map<String, String> tags = new HashMap<>();
        tags.put(MetricTags.GROUP_ID, getGroupId());
        tags.put(MetricTags.CONNECTION_NAME, getConnectionName());
        tags.put(MetricTags.ID, getId());
        return tags;
    }
}
