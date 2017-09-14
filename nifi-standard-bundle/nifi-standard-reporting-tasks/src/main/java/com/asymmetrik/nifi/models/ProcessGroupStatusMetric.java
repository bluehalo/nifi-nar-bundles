package com.asymmetrik.nifi.models;

import java.util.HashMap;
import java.util.Map;

import com.asymmetrik.nifi.models.influxdb.MetricFields;
import com.asymmetrik.nifi.models.influxdb.MetricTags;

import org.apache.nifi.controller.status.ProcessGroupStatus;

public class ProcessGroupStatusMetric {

    private String id;
    private String processGroupName;
    private Double activeThreadCount;
    private Double bytesRead;
    private Double bytesReceived;
    private Double bytesSent;
    private Double bytesTransferred;
    private Double bytesWritten;
    private Double flowFilesReceived;
    private Double flowFilesSent;
    private Double flowFilesTransferred;
    private Double inputCount;
    private Double inputContentSize;
    private Double outputCount;
    private Double outputContentSize;
    private Double queued;
    private Double queuedContentSize;

    public ProcessGroupStatusMetric() {
    }

    public ProcessGroupStatusMetric(ProcessGroupStatus processGroupStatus) {
        setId(processGroupStatus.getId());
        setProcessGroupName(processGroupStatus.getName());
        setActiveThreadCount(Double.valueOf(processGroupStatus.getActiveThreadCount()));
        setBytesRead(Double.valueOf(processGroupStatus.getBytesRead()));
        setBytesReceived((double) processGroupStatus.getBytesReceived());
        setBytesSent((double) processGroupStatus.getBytesSent());
        setBytesTransferred((double) processGroupStatus.getBytesTransferred());
        setBytesWritten(Double.valueOf(processGroupStatus.getBytesWritten()));
        setFlowFilesReceived((double) processGroupStatus.getFlowFilesReceived());
        setFlowFilesSent((double) processGroupStatus.getFlowFilesSent());
        setFlowFilesTransferred((double) processGroupStatus.getFlowFilesTransferred());
        setOutputContentSize((double)processGroupStatus.getOutputContentSize());
        setInputCount((double)processGroupStatus.getInputCount());
        setInputContentSize((double)processGroupStatus.getInputContentSize());
        setOutputCount((double)processGroupStatus.getOutputCount());
        setQueued(Double.valueOf(processGroupStatus.getQueuedCount()));
        setQueuedContentSize(Double.valueOf(processGroupStatus.getQueuedContentSize()));
    }

    public String getId() {
        return id;
    }

    public ProcessGroupStatusMetric setId(String id) {
        this.id = id;
        return this;
    }

    public String getProcessGroupName() {
        return processGroupName;
    }

    public ProcessGroupStatusMetric setProcessGroupName(String name) {
        this.processGroupName = name;
        return this;
    }

    public Double getBytesRead() {
        return bytesRead;
    }

    public ProcessGroupStatusMetric setBytesRead(Double bytesRead) {
        this.bytesRead = bytesRead;
        return this;
    }

    public Double getBytesWritten() {
        return bytesWritten;
    }

    public ProcessGroupStatusMetric setBytesWritten(Double bytesWritten) {
        this.bytesWritten = bytesWritten;
        return this;
    }

    public Double getFlowFilesTransferred() {
        return flowFilesTransferred;
    }

    public ProcessGroupStatusMetric setFlowFilesTransferred(Double flowFilesTransferred) {
        this.flowFilesTransferred = flowFilesTransferred;
        return this;
    }

    public Double getBytesTransferred() {
        return bytesTransferred;
    }

    public ProcessGroupStatusMetric setBytesTransferred(Double bytesTransferred) {
        this.bytesTransferred = bytesTransferred;
        return this;
    }

    public Double getBytesReceived() {
        return bytesReceived;
    }

    public ProcessGroupStatusMetric setBytesReceived(Double bytesReceived) {
        this.bytesReceived = bytesReceived;
        return this;
    }

    public Double getFlowFilesReceived() {
        return flowFilesReceived;
    }

    public ProcessGroupStatusMetric setFlowFilesReceived(Double flowFilesReceived) {
        this.flowFilesReceived = flowFilesReceived;
        return this;
    }

    public Double getBytesSent() {
        return bytesSent;
    }

    public ProcessGroupStatusMetric setBytesSent(Double bytesSent) {
        this.bytesSent = bytesSent;
        return this;
    }

    public Double getFlowFilesSent() {
        return flowFilesSent;
    }

    public ProcessGroupStatusMetric setFlowFilesSent(Double flowFilesSent) {
        this.flowFilesSent = flowFilesSent;
        return this;
    }

    public Double getActiveThreadCount() {
        return activeThreadCount;
    }

    public ProcessGroupStatusMetric setActiveThreadCount(Double activeThreadCount) {
        this.activeThreadCount = activeThreadCount;
        return this;
    }

    public Double getQueued() {
        return queued;
    }

    public ProcessGroupStatusMetric setQueued(Double queued) {
        this.queued = queued;
        return this;
    }

    public Double getQueuedContentSize() {
        return queuedContentSize;
    }

    public ProcessGroupStatusMetric setQueuedContentSize(Double queuedContentSize) {
        this.queuedContentSize = queuedContentSize;
        return this;
    }

    public Double getInputCount() {
        return inputCount;
    }

    public void setInputCount(Double inputCount) {
        this.inputCount = inputCount;
    }

    public Double getInputContentSize() {
        return inputContentSize;
    }

    public void setInputContentSize(Double inputContentSize) {
        this.inputContentSize = inputContentSize;
    }

    public Double getOutputCount() {
        return outputCount;
    }

    public void setOutputCount(Double outputCount) {
        this.outputCount = outputCount;
    }

    public Double getOutputContentSize() {
        return outputContentSize;
    }

    public void setOutputContentSize(Double outputContentSize) {
        this.outputContentSize = outputContentSize;
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
        values.put(MetricFields.FLOWFILES_SENT, getFlowFilesSent());
        values.put(MetricFields.FLOWFILES_TRANSFERRED, getFlowFilesTransferred());
        values.put(MetricFields.INPUT_COUNT, getInputCount());
        values.put(MetricFields.INPUT_CONTENT_SIZE, getInputContentSize());
        values.put(MetricFields.OUTPUT_COUNT, getOutputCount());
        values.put(MetricFields.OUTPUT_CONTENT_SIZE, getOutputContentSize());
        values.put(MetricFields.QUEUED, getQueued());
        values.put(MetricFields.QUEUED_CONTENT_SIZE, getQueuedContentSize());
        return values;
    }

    public Map<String, String> tags() {
        Map<String, String> tags = new HashMap<>();
        tags.put(MetricTags.PROCESS_GROUP_NAME, getProcessGroupName());
        tags.put(MetricTags.ID, getId());
        return tags;
    }

}
