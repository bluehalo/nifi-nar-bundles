package com.asymmetrik.nifi.models.influxdb;

public class MetricFields {
    public static final String ACTIVE_THREAD_COUNT = "activeThreadCount";
    public static final String BYTES_OUT = "bytesOut";
    public static final String BYTES_READ = "bytesRead";
    public static final String BYTES_RECEIVED = "bytesReceived";
    public static final String BYTES_SENT = "bytesSent";
    public static final String BYTES_TRANSFERRED = "bytesTransferred";
    public static final String BYTES_WRITTEN = "bytesWritten";
    public static final String FLOWFILES_RECEIVED = "flowFilesReceived";
    public static final String FLOWFILES_REMOVED = "flowFilesRemoved";
    public static final String FLOWFILES_SENT = "flowFilesSent";
    public static final String FLOWFILES_TRANSFERRED = "flowFilesTransferred";

    public static final String INPUT_BYTES = "inputBytes";
    public static final String INPUT_COUNT = "inputCount";
    public static final String INPUT_CONTENT_SIZE = "inputContentSize";
    public static final String OUTPUT_BYTES = "outputBytes";
    public static final String OUTPUT_COUNT = "outputCount";
    public static final String OUTPUT_CONTENT_SIZE = "outputContentSize";

    public static final String BACKPRESSURE_BYTES_THRESHOLD = "backPressureBytesThreshold";
    public static final String BACKPRESSURE_OBJECT_THRESHOLD = "backPressureObjectThreshold";

    public static final String RUN_STATUS = "runStatus";

    public static final String LINEAGE_DURATION = "lineageDuration";
    public static final String AVERAGE_LINEAGE_DURATION = "averageLineageDuration";

    public static final String QUEUED = "queued";
    public static final String QUEUED_BYTES = "queuedBytes";
    public static final String MAX_QUEUED_BYTES = "maxQueuedBytes";
    public static final String MAX_QUEUED_COUNT = "maxQueuedCount";
    public static final String QUEUED_CONTENT_SIZE = "queuedContentSize";

    public static final String ACTIVE_REMOTE_PORT_COUNT = "activeRemotePortCount";
    public static final String INACTIVE_REMOTE_PORT_COUNT = "inactiveRemotePortCount";
    public static final String RECEIVED_CONTENT_SIZE = "receivedContentSize";
    public static final String RECEIVED_COUNT = "receivedCount";
    public static final String SENT_CONTENT_SIZE = "sentContentSize";
    public static final String SENT_COUNT = "sentCount";
    public static final String TRANSMISSION_STATUS = "transmissionStatus";

    public static final String USED_PERCENT = "usedPercent";
    public static final String USED = "used";
    public static final String FREE = "free";
    public static final String TOTAL = "total";

    // JVM Metrics
    public static final String JVM_UPTIME = "jvm.uptime";
    public static final String JVM_HEAP_USED = "jvm.heap_used";
    public static final String JVM_HEAP_USAGE = "jvm.heap_usage";
    public static final String JVM_NON_HEAP_USAGE = "jvm.non_heap_usage";
    public static final String JVM_THREAD_STATES_RUNNABLE = "jvm.thread_states.runnable";
    public static final String JVM_THREAD_STATES_BLOCKED = "jvm.thread_states.blocked";
    public static final String JVM_THREAD_STATES_TIMED_WAITING = "jvm.thread_states.timed_waiting";
    public static final String JVM_THREAD_STATES_TERMINATED = "jvm.thread_states.terminated";
    public static final String JVM_THREAD_COUNT = "jvm.thread_count";
    public static final String JVM_DAEMON_THREAD_COUNT = "jvm.daemon_thread_count";
    public static final String JVM_FILE_DESCRIPTOR_USAGE = "jvm.file_descriptor_usage";
    public static final String JVM_GC_RUNS = "jvm.gc.runs";
    public static final String JVM_GC_TIME = "jvm.gc.time";

    public MetricFields() {}
}
