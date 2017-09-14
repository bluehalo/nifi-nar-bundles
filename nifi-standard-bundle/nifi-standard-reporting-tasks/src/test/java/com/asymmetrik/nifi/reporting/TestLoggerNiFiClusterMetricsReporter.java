package com.asymmetrik.nifi.reporting;

import java.net.URISyntaxException;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.util.MockPropertyValue;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static com.asymmetrik.nifi.reporting.AbstractNiFiClusterMetricsReporter.PROCESSORS;
import static com.asymmetrik.nifi.reporting.AbstractNiFiClusterMetricsReporter.PROCESS_GROUPS;
import static com.asymmetrik.nifi.reporting.AbstractNiFiClusterMetricsReporter.VOLUMES;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestLoggerNiFiClusterMetricsReporter {
    private ReportingInitializationContext initContext;
    private ConfigurationContext configurationContext;
    private ReportingContext reportingContext;

    private EventAccess eventAccess;

    @Before
    public void setup() throws URISyntaxException {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH,
                LoggerNiFiClusterMetricsReporter.class.getResource("/reporting/nifi.properties").toURI().getPath());

        initContext = mock(ReportingInitializationContext.class);
        configurationContext = mock(ConfigurationContext.class);
        reportingContext = mock(ReportingContext.class);

        eventAccess = Mockito.mock(EventAccess.class);
        when(reportingContext.getEventAccess()).thenReturn(eventAccess);
        when(reportingContext.getClusterNodeIdentifier()).thenReturn("clusterNodeId");

        when(configurationContext.getProperty(VOLUMES))
                .thenReturn(new MockPropertyValue("/var/log"));

        // mock the rest of the properties to empty values
        final PropertyValue nullProperty = new MockPropertyValue(null);
        final PropertyDescriptor[] unSetDescriptors = {PROCESS_GROUPS, PROCESSORS};
        for (PropertyDescriptor d : unSetDescriptors) {
            when(configurationContext.getProperty(d))
                    .thenReturn(nullProperty);
        }

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
    public void testReport() throws InitializationException {
        // MetricsHttpReporter reportingTask = new MetricsHttpReporter();
        // reportingTask.initialize(initContext);
        // reportingTask.onScheduled(configurationContext);
        // reportingTask.onTrigger(reportingContext);
        //
        // assertEquals(reportingTask.publish());
        // Map<String, Number> metrics = reportingTask.getMetrics();
        // Map<String, StandardUnit> units = reportingTask.getUnits();
        //
        // assertEquals(6, metrics.size());
        // assertEquals(1024L, metrics.get(KEY_QUEUE_SIZE));
        // assertEquals(100, metrics.get(KEY_QUEUE_COUNT));
        // assertTrue(metrics.containsKey(PREFIX_CONTENT_REPOS + ' ' + KEY_DISK_USED));
        // assertTrue(metrics.containsKey(PREFIX_FLOWFILE_REPO + ' ' + KEY_DISK_USED));
        // assertTrue(metrics.containsKey("/ " + KEY_DISK_USED));
        // assertTrue(metrics.containsKey("/tmp " + KEY_DISK_USED));
        //
        // assertEquals(6, units.size());
        // assertEquals(StandardUnit.Bytes, units.get(KEY_QUEUE_SIZE));
        // assertEquals(StandardUnit.Count, units.get(KEY_QUEUE_COUNT));
        // assertEquals(StandardUnit.Percent, units.get(PREFIX_CONTENT_REPOS + ' ' + KEY_DISK_USED));
        // assertEquals(StandardUnit.Percent, units.get(PREFIX_FLOWFILE_REPO + ' ' + KEY_DISK_USED));
        // assertEquals(StandardUnit.Percent, units.get("/ " + KEY_DISK_USED));
        // assertEquals(StandardUnit.Percent, units.get("/tmp " + KEY_DISK_USED));

    }
}