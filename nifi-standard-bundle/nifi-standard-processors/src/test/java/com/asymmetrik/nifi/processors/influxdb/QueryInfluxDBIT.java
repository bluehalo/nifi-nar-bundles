package com.asymmetrik.nifi.processors.influxdb;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import com.asymmetrik.nifi.services.influxdb.InfluxDbService;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.Test;

public class QueryInfluxDBIT {
    public static final ObjectMapper MAPPER = QueryInfluxDB.OBJ_MAPPER;

    @Test
    public void validQueryTests() throws InitializationException {
        HashMap<String, String> props = new HashMap<>();
        InfluxDbService influxDbService = new InfluxDbService();

        TestRunner runner = TestRunners.newTestRunner(QueryInfluxDB.class);
        runner.addControllerService("influxService", influxDbService, props);
        runner.setProperty(QueryInfluxDB.INFLUX_DB_SERVICE, "influxService");
        runner.setProperty(QueryInfluxDB.DATABASE_NAME, "db");
        runner.enableControllerService(influxDbService);

        runner.setProperty(QueryInfluxDB.QUERY_STRING, "select * from db limit 1");
        runner.assertValid();

        runner.setProperty(QueryInfluxDB.QUERY_STRING, "SELECT * FROM ingress LIMIT 10");
        runner.assertValid();

        runner.setProperty(QueryInfluxDB.QUERY_STRING, "SELECT MAX(\"water_level\") FROM \"h2o_feet\" WHERE \"location\"='coyote_creek' AND time >= '2015-09-18T16:00:00Z' AND time <= '2015-09-18T16:42:00Z' GROUP BY time(12m)");
        runner.assertValid();

        runner.setProperty(QueryInfluxDB.QUERY_STRING, "SELECT * INTO \"copy_NOAA_water_database\".\"autogen\".:MEASUREMENT FROM \"NOAA_water_database\".\"autogen\"./.*/ GROUP BY *");
        runner.assertValid();

        runner.setProperty(QueryInfluxDB.QUERY_STRING, "SELECT \"water_level\" INTO \"where_else\".\"autogen\".\"h2o_feet_copy_2\" FROM \"h2o_feet\" WHERE \"location\" = 'coyote_creek'");
        runner.assertValid();

        runner.setProperty(QueryInfluxDB.QUERY_STRING, "show databases");
        runner.assertValid();

        runner.setProperty(QueryInfluxDB.QUERY_STRING, "show measurements on \"db\"");
        runner.assertValid();

        runner.setProperty(QueryInfluxDB.QUERY_STRING, "SHOW RETENTION POLICIES ON \"my_database\"");
        runner.assertValid();

        runner.setProperty(QueryInfluxDB.QUERY_STRING, "SHOW SERIES ON \"my_database\"");
        runner.assertValid();

        runner.setProperty(QueryInfluxDB.QUERY_STRING, "SHOW MEASUREMENTS ON \"my_database\"");
        runner.assertValid();

        runner.setProperty(QueryInfluxDB.QUERY_STRING, "create database \"hello\"");
        runner.assertValid();

        runner.setProperty(QueryInfluxDB.QUERY_STRING, "DROP DATABASE \"water_database\"");
        runner.assertValid();

        runner.setProperty(QueryInfluxDB.QUERY_STRING, "CREATE DATABASE \"some_database\" WITH DURATION 3d REPLICATION 1 SHARD DURATION 1h NAME \"liquid\"");
        runner.assertValid();

        runner.setProperty(QueryInfluxDB.QUERY_STRING, "DROP SERIES FROM \"h2o_feet\" WHERE \"location\" = 'santa_monica'");
        runner.assertValid();

        runner.setProperty(QueryInfluxDB.QUERY_STRING, "ALTER RETENTION POLICY \"what_is_time\" ON \"NOAA_water_database\" DURATION 3w SHARD DURATION 30m DEFAULT");
        runner.assertValid();
    }

    @Test
    public void invalidQueryTests() throws InitializationException {
        HashMap<String, String> props = new HashMap<>();
        InfluxDbService influxDbService = new InfluxDbService();

        TestRunner runner = TestRunners.newTestRunner(QueryInfluxDB.class);
        runner.addControllerService("influxService", influxDbService, props);
        runner.setProperty(QueryInfluxDB.INFLUX_DB_SERVICE, "influxService");
        runner.enableControllerService(influxDbService);

        runner.setProperty(QueryInfluxDB.TIME_UNIT, "Milliseconds");
        runner.setProperty(QueryInfluxDB.DATABASE_NAME, "testDatabase");
        runner.assertNotValid();

        runner.setProperty(QueryInfluxDB.QUERY_STRING, "selected * from db limit 1");
        runner.assertNotValid();

        // No limit specified
        runner.setProperty(QueryInfluxDB.QUERY_STRING, "SELECT * FROM ingress");
        runner.assertNotValid();

        runner.setProperty(QueryInfluxDB.QUERY_STRING, "show database");
        runner.assertNotValid();
    }

    @Test
    public void testResponse() {
        InfluxDB influxDb = InfluxDBFactory.connect("http://127.0.0.1:8086");

        // 1. clear testdb
        // 2. add elements to testdb
        // 3. query elements from testdb
        Query query = new Query("select * from process_group limit 10", "datastork");
        QueryResult queryResult = influxDb.query(query, TimeUnit.MILLISECONDS);
        System.out.println(QueryInfluxDB.toJson(queryResult));
    }
}