package com.asymmetrik.nifi.models.influxdb;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestMetricMeasurements {

    @Test
    public void test() {
        MetricMeasurements metricMeasurements = new MetricMeasurements();
        assertEquals(MetricMeasurements.CONNECTION, MetricMeasurements.CONNECTION);
    }

}