package com.asymmetrik.nifi.reporting;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.asymmetrik.nifi.models.ProcessGroupStatusMetric;
import com.asymmetrik.nifi.models.SystemMetricsSnapshot;

import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestInfluxNiFiClusterMetricsReporter {
    private ReportingInitializationContext initContext;
    private ConfigurationContext configurationContext;
    private ReportingContext reportingContext;
    private EventAccess eventAccess;
    private BatchPoints batchPoints;

    @Before
    public void before() {

        initContext = mock(ReportingInitializationContext.class);
        configurationContext = mock(ConfigurationContext.class);
        reportingContext = mock(ReportingContext.class);
        eventAccess = Mockito.mock(EventAccess.class);
        batchPoints = Mockito.mock(BatchPoints.class);

        when(reportingContext.getEventAccess()).thenReturn(eventAccess);
        when(reportingContext.getClusterNodeIdentifier()).thenReturn("clusterNodeId");
        when(batchPoints.getDatabase()).thenReturn("mock_database");

        ProcessGroupStatus status = new ProcessGroupStatus();
        status.setId("abc-abc-abc");
        status.setName("root");
        status.setActiveThreadCount(15);
        status.setBytesRead(1L);
        status.setBytesReceived(2L);
        status.setBytesSent(3L);
        status.setBytesTransferred(5L);
        status.setBytesWritten(10L);
        status.setFlowFilesSent(10);
        status.setFlowFilesReceived(34);
        status.setFlowFilesTransferred(30);
        status.setQueuedCount(100);
        status.setQueuedContentSize(1024L);

        when(eventAccess.getControllerStatus()).thenReturn(status);
    }

    @Test
    public void testCollectMeasurements() throws InitializationException {
        InfluxNiFiClusterMetricsReporter metricsInfluxDbReporter = new InfluxNiFiClusterMetricsReporter();
        ProcessGroupStatusMetric processGroupStatusMetric = new ProcessGroupStatusMetric()
                .setId("id")
                .setProcessGroupName("root")
                .setActiveThreadCount(15.0)
                .setBytesRead(1.4)
                .setBytesReceived(4.)
                .setBytesSent(45.)
                .setBytesWritten(2387.)
                .setFlowFilesReceived(3425.)
                .setFlowFilesSent(3425.)
                .setFlowFilesTransferred(4243.)
                .setQueued(34521.)
                .setQueuedContentSize(487652.);

        Map<String, Object> memory = new HashMap<>();
        memory.put("mem", 1.2);

        Map<String, Object> jvm = new HashMap<>();
        jvm.put("free", 1.2);
        SystemMetricsSnapshot metrics = new SystemMetricsSnapshot()
                .setClusterNodeIdentifier("clusterId")
                .setMachineMemory(memory)
                .setJvmMetrics(jvm)
                .setRootProcessGroupSnapshot(processGroupStatusMetric)
                .setProcessGroupSnapshots(Arrays.asList(processGroupStatusMetric));

        BatchPoints batchPoints = BatchPoints
                .database("mock_database")
                .build();
        metricsInfluxDbReporter.collectMeasurements(System.currentTimeMillis(), metrics, batchPoints);

        List<Point> points = batchPoints.getPoints();
        assertEquals(4, points.size());
    }
}