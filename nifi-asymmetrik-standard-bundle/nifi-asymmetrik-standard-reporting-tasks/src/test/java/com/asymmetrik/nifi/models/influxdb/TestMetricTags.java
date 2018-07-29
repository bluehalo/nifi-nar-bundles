package com.asymmetrik.nifi.models.influxdb;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestMetricTags {

    @Test
    public void test() {
        MetricTags metricTags = new MetricTags();
        assertEquals(MetricTags.ID, MetricTags.ID);
    }
}