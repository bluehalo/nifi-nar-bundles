package com.asymmetrik.nifi.models.influxdb;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestMetricFields {

    @Test
    public void test() {
        MetricFields metricFields = new MetricFields();
        assertEquals(MetricFields.BYTES_OUT, MetricFields.BYTES_OUT);
    }

}