package com.asymmetrik.nifi.models;

import java.util.HashMap;
import java.util.Map;

import com.asymmetrik.nifi.models.influxdb.MetricFields;
import com.asymmetrik.nifi.models.influxdb.MetricTags;

import org.apache.nifi.controller.status.ProcessorStatus;

public class ProcessorStatusMetric {

    private String id;
    private String processorName;
    private String groupId;
    private Double activeThreadCount;
    private Double averageLineageDuration;
    private Double bytesRead;
    private Double bytesReceived;
    private Double bytesSent;
    private Double bytesWritten;
    private Double bytesTransferred;
    private Double inputBytes;
    private Double inputCount;
    private Double flowFilesReceived;
    private Double flowFilesRemoved;
    private Double flowFilesSent;
    private Double outputBytes;
    private Double outputCount;
    private Double runStatus;
    private String type;

    public ProcessorStatusMetric() {
    }

    public ProcessorStatusMetric(ProcessorStatus processorStatus) {
        setId(processorStatus.getId());
        setProcessorName(processorStatus.getName());
        setGroupId(processorStatus.getGroupId());
        setActiveThreadCount((double) processorStatus.getActiveThreadCount());
        setAverageLineageDuration((double) processorStatus.getAverageLineageDuration());
        setBytesRead((double) processorStatus.getBytesRead());
        setBytesReceived((double) processorStatus.getBytesReceived());
        setBytesSent((double) processorStatus.getBytesSent());
        setBytesWritten((double) processorStatus.getBytesWritten());
        setFlowFilesReceived((double) processorStatus.getFlowFilesReceived());
        setFlowFilesRemoved((double) processorStatus.getFlowFilesRemoved());
        setFlowFilesSent((double) processorStatus.getFlowFilesSent());
        setInputBytes((double) processorStatus.getInputBytes());
        setInputCount((double) processorStatus.getInputCount());
        setOutputBytes((double) processorStatus.getOutputBytes());
        setOutputCount((double) processorStatus.getOutputCount());
        setRunStatus((double) processorStatus.getRunStatus().ordinal());
        setType(processorStatus.getType());
    }

    public String getId() {
        return id;
    }

    public ProcessorStatusMetric setId(String id) {
        this.id = id;
        return this;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getType() {
        return type;
    }

    public ProcessorStatusMetric setType(String type) {
        this.type = type;
        return this;
    }

    public ProcessorStatusMetric setGroupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public String getProcessorName() {
        return processorName;
    }

    public ProcessorStatusMetric setProcessorName(String processorName) {
        this.processorName = processorName;
        return this;
    }

    public Double getBytesRead() {
        return bytesRead;
    }

    public ProcessorStatusMetric setBytesRead(Double bytesRead) {
        this.bytesRead = bytesRead;
        return this;
    }

    public Double getBytesWritten() {
        return bytesWritten;
    }

    public ProcessorStatusMetric setBytesWritten(Double bytesWritten) {
        this.bytesWritten = bytesWritten;
        return this;
    }

    public Double getBytesTransferred() {
        return bytesTransferred;
    }

    public ProcessorStatusMetric setBytesTransferred(Double bytesTransferred) {
        this.bytesTransferred = bytesTransferred;
        return this;
    }

    public Double getAverageLineageDuration() {
        return averageLineageDuration;
    }

    public ProcessorStatusMetric setAverageLineageDuration(Double averageLineageDuration) {
        this.averageLineageDuration = averageLineageDuration;
        return this;
    }

    public Double getBytesReceived() {
        return bytesReceived;
    }

    public ProcessorStatusMetric setBytesReceived(Double bytesReceived) {
        this.bytesReceived = bytesReceived;
        return this;
    }

    public Double getFlowFilesReceived() {
        return flowFilesReceived;
    }

    public ProcessorStatusMetric setFlowFilesReceived(Double flowFilesReceived) {
        this.flowFilesReceived = flowFilesReceived;
        return this;
    }

    public Double getBytesSent() {
        return bytesSent;
    }

    public ProcessorStatusMetric setBytesSent(Double bytesSent) {
        this.bytesSent = bytesSent;
        return this;
    }

    public Double getFlowFilesSent() {
        return flowFilesSent;
    }

    public ProcessorStatusMetric setFlowFilesSent(Double flowFilesSent) {
        this.flowFilesSent = flowFilesSent;
        return this;
    }

    public Double getFlowFilesRemoved() {
        return flowFilesRemoved;
    }

    public ProcessorStatusMetric setFlowFilesRemoved(Double flowFilesRemoved) {
        this.flowFilesRemoved = flowFilesRemoved;
        return this;
    }

    public Double getActiveThreadCount() {
        return activeThreadCount;
    }

    public ProcessorStatusMetric setActiveThreadCount(Double activeThreadCount) {
        this.activeThreadCount = activeThreadCount;
        return this;
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

    public Double getRunStatus() {
        return runStatus;
    }

    public void setRunStatus(Double runStatus) {
        this.runStatus = runStatus;
    }

    public Map<String, Object> valuesAsMap() {
        Map<String, Object> values = new HashMap<>();
        values.put(MetricFields.ACTIVE_THREAD_COUNT, getActiveThreadCount());
        values.put(MetricFields.BYTES_READ, getBytesRead());
        values.put(MetricFields.BYTES_RECEIVED, getBytesReceived());
        values.put(MetricFields.BYTES_SENT, getBytesSent());
        values.put(MetricFields.BYTES_TRANSFERRED, getBytesTransferred());
        values.put(MetricFields.BYTES_WRITTEN, getBytesWritten());
        values.put(MetricFields.FLOWFILES_RECEIVED, getFlowFilesReceived());
        values.put(MetricFields.FLOWFILES_REMOVED, getFlowFilesRemoved());
        values.put(MetricFields.FLOWFILES_SENT, getFlowFilesSent());
        values.put(MetricFields.LINEAGE_DURATION, getAverageLineageDuration());
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
        tags.put(MetricTags.TYPE, getType());
        tags.put(MetricTags.PROCESSOR_NAME, getProcessorName());
        tags.put(MetricTags.ID, getId());
        return tags;
    }
}
