package com.asymmetrik.nifi.services.influxdb;

import org.apache.nifi.controller.ControllerService;
import org.influxdb.InfluxDB;

public interface InfluxDatabaseService extends ControllerService {
    InfluxDB getInfluxDb();
}