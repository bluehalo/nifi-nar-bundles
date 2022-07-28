package com.asymmetrik.nifi.services.influxdb2;

import com.influxdb.client.InfluxDBClient;
import org.apache.nifi.controller.ControllerService;

public interface InfluxClientApi extends ControllerService {
    InfluxDBClient getInfluxDb();
}