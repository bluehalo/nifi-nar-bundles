package com.asymmetrik.nifi.reporting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.asymmetrik.nifi.models.ProcessGroupStatusMetric;
import com.asymmetrik.nifi.models.SystemMetricsSnapshot;

import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCloudwatchNiFiClusterMetricsReporter {
    private ReportingInitializationContext initContext;
    private ConfigurationContext configurationContext;
    private ReportingContext reportingContext;
    private EventAccess eventAccess;

    @Before
    public void before() {

        initContext = mock(ReportingInitializationContext.class);
        configurationContext = mock(ConfigurationContext.class);
        reportingContext = mock(ReportingContext.class);
        eventAccess = Mockito.mock(EventAccess.class);

        when(reportingContext.getEventAccess()).thenReturn(eventAccess);
        when(reportingContext.getClusterNodeIdentifier()).thenReturn("clusterNodeId");

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
        CloudwatchNiFiClusterMetricsReporter metricsCloudwatchReporter = new CloudwatchNiFiClusterMetricsReporter();
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

        List<Dimension> dimensions = new ArrayList<>();
        List<MetricDatum> cloudwatchMetrics = 
                metricsCloudwatchReporter.collectMeasurements(new Date(System.currentTimeMillis()), metrics, dimensions);

        assertEquals(10, cloudwatchMetrics.size());

        metricsCloudwatchReporter.setCollectsMemory(true);
        metricsCloudwatchReporter.setCollectsJVMMetrics(true);

        cloudwatchMetrics = metricsCloudwatchReporter.collectMeasurements(new Date(System.currentTimeMillis()), metrics, dimensions);

        assertEquals(12, cloudwatchMetrics.size());
    }

    @Test
    public void testCsvParsing() {
        CloudwatchNiFiClusterMetricsReporter metricsInfluxDbReporter = new CloudwatchNiFiClusterMetricsReporter();
        List<String> fields = metricsInfluxDbReporter.parseInputField("a, b, , , b ,   ,   cat, ");
        assertEquals(3, fields.size());
    }
}