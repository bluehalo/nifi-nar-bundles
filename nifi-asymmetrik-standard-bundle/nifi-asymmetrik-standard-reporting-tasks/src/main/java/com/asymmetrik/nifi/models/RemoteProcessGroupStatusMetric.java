package com.asymmetrik.nifi.models;

import java.util.HashMap;
import java.util.Map;

import com.asymmetrik.nifi.models.influxdb.MetricFields;
import com.asymmetrik.nifi.models.influxdb.MetricTags;

import org.apache.nifi.controller.status.RemoteProcessGroupStatus;

public class RemoteProcessGroupStatusMetric {

    private String id;
    private String remoteProcessGroupName;
    private Double activeThreadCount;
    private Double activeRemotePortCount;
    private Double averageLinageDuration;
    private Double inactiveRemotePortCount;
    private Double receivedContentSize;
    private Double receivedCount;
    private Double sentContentSize;
    private Double sentCount;
    private Double transmissionStatus;

    public RemoteProcessGroupStatusMetric() {
    }

    public RemoteProcessGroupStatusMetric(RemoteProcessGroupStatus remoteProcessGroupStatus) {
        setId(remoteProcessGroupStatus.getId());
        setRemoteProcessGroupName(remoteProcessGroupStatus.getName());
        setActiveThreadCount(Double.valueOf(remoteProcessGroupStatus.getActiveThreadCount()));
        setActiveRemotePortCount(Double.valueOf(remoteProcessGroupStatus.getActiveRemotePortCount()));
        setAverageLinageDuration((double) remoteProcessGroupStatus.getAverageLineageDuration());
        setInactiveRemotePortCount(Double.valueOf(remoteProcessGroupStatus.getInactiveRemotePortCount()));
        setReceivedContentSize(Double.valueOf(remoteProcessGroupStatus.getReceivedContentSize()));
        setReceivedCount(Double.valueOf(remoteProcessGroupStatus.getReceivedCount()));
        setSentContentSize(Double.valueOf(remoteProcessGroupStatus.getSentContentSize()));
        setSentCount(Double.valueOf(remoteProcessGroupStatus.getSentCount()));
        setTransmissionStatus((double) remoteProcessGroupStatus.getTransmissionStatus().ordinal());
    }

    public String getId() {
        return id;
    }

    public RemoteProcessGroupStatusMetric setId(String id) {
        this.id = id;
        return this;
    }

    public String getRemoteProcessGroupName() {
        return remoteProcessGroupName;
    }

    public RemoteProcessGroupStatusMetric setRemoteProcessGroupName(String name) {
        this.remoteProcessGroupName = name;
        return this;
    }

    public Double getActiveThreadCount() {
        return activeThreadCount;
    }

    public RemoteProcessGroupStatusMetric setActiveThreadCount(Double activeThreadCount) {
        this.activeThreadCount = activeThreadCount;
        return this;
    }

    public Double getActiveRemotePortCount() {
        return activeRemotePortCount;
    }

    public RemoteProcessGroupStatusMetric setActiveRemotePortCount(Double activeRemotePortCount) {
        this.activeRemotePortCount = activeRemotePortCount;
        return this;
    }

    public Double getAverageLinageDuration() {
        return averageLinageDuration;
    }

    public RemoteProcessGroupStatusMetric setAverageLinageDuration(Double averageLinageDuration) {
        this.averageLinageDuration = averageLinageDuration;
        return this;
    }

    public Double getInactiveRemotePortCount() {
        return inactiveRemotePortCount;
    }

    public RemoteProcessGroupStatusMetric setInactiveRemotePortCount(Double inactiveRemotePortCount) {
        this.inactiveRemotePortCount = inactiveRemotePortCount;
        return this;
    }

    public Double getReceivedContentSize() {
        return receivedContentSize;
    }

    public RemoteProcessGroupStatusMetric setReceivedContentSize(Double receivedContentSize) {
        this.receivedContentSize = receivedContentSize;
        return this;
    }

    public Double getReceivedCount() {
        return receivedCount;
    }

    public RemoteProcessGroupStatusMetric setReceivedCount(Double receivedCount) {
        this.receivedCount = receivedCount;
        return this;
    }

    public Double getSentContentSize() {
        return sentContentSize;
    }

    public RemoteProcessGroupStatusMetric setSentContentSize(Double sentContentSize) {
        this.sentContentSize = sentContentSize;
        return this;
    }

    public Double getSentCount() {
        return sentCount;
    }

    public RemoteProcessGroupStatusMetric setSentCount(Double sentCount) {
        this.sentCount = sentCount;
        return this;
    }

    public Double getTransmissionStatus() {
        return transmissionStatus;
    }

    public RemoteProcessGroupStatusMetric setTransmissionStatus(Double transmissionStatus) {
        this.transmissionStatus = transmissionStatus;
        return this;
    }

    public Map<String, Object> valuesAsMap() {
        Map<String, Object> values = new HashMap<>();
        values.put(MetricFields.ACTIVE_THREAD_COUNT, getActiveThreadCount());
        values.put(MetricFields.ACTIVE_REMOTE_PORT_COUNT, getActiveRemotePortCount());
        values.put(MetricFields.AVERAGE_LINEAGE_DURATION, getAverageLinageDuration());
        values.put(MetricFields.INACTIVE_REMOTE_PORT_COUNT, getInactiveRemotePortCount());
        values.put(MetricFields.RECEIVED_CONTENT_SIZE, getReceivedContentSize());
        values.put(MetricFields.RECEIVED_COUNT, getReceivedCount());
        values.put(MetricFields.SENT_CONTENT_SIZE, getSentContentSize());
        values.put(MetricFields.SENT_COUNT, getSentCount());
        values.put(MetricFields.TRANSMISSION_STATUS, getTransmissionStatus());
        return values;
    }

    public Map<String, String> tags() {
        Map<String, String> tags = new HashMap<>();
        tags.put(MetricTags.REMOTE_PROCESS_GROUP_NAME, getRemoteProcessGroupName());
        tags.put(MetricTags.ID, getId());
        return tags;
    }

}
