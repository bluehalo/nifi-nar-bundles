/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asymmetrik.nifi.processors.influxdb;

import com.asymmetrik.nifi.services.influxdb.InfluxDbService;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ExecuteInfluxDBQueryIT {

    private TestRunner runner;
    private InfluxDbService influxDbService;
    private Query create = new Query("CREATE DATABASE ExecuteInfluxDBQueryIT", "ExecuteInfluxDBQueryIT");
    private Query drop = new Query("DROP DATABASE ExecuteInfluxDBQueryIT", "ExecuteInfluxDBQueryIT");


    @Before
    public void beforeEach() throws InitializationException {
        HashMap<String, String> props = new HashMap<>();
        influxDbService = new InfluxDbService();
        runner = TestRunners.newTestRunner(ExecuteInfluxDBQuery.class);
        runner.addControllerService("influxService", influxDbService, props);
        runner.setProperty(ExecuteInfluxDBQuery.INFLUX_DB_SERVICE, "influxService");
        runner.setProperty(ExecuteInfluxDBQuery.DATABASE_NAME, "db");
        runner.enableControllerService(influxDbService);
    }

    @Test
    public void validQueryTests() {
        runner.setProperty(ExecuteInfluxDBQuery.PROP_QUERY_STRING, "select * from db limit 1");
        runner.assertValid();

        runner.setProperty(ExecuteInfluxDBQuery.PROP_QUERY_STRING, "SELECT * FROM ingress LIMIT 10");
        runner.assertValid();

        runner.setProperty(ExecuteInfluxDBQuery.PROP_QUERY_STRING, "SELECT MAX(\"water_level\") FROM \"h2o_feet\" WHERE \"location\"='coyote_creek' AND time >= '2015-09-18T16:00:00Z' AND time <= '2015-09-18T16:42:00Z' GROUP BY time(12m)");
        runner.assertValid();

        runner.setProperty(ExecuteInfluxDBQuery.PROP_QUERY_STRING, "SELECT * INTO \"copy_NOAA_water_database\".\"autogen\".:MEASUREMENT FROM \"NOAA_water_database\".\"autogen\"./.*/ GROUP BY *");
        runner.assertValid();

        runner.setProperty(ExecuteInfluxDBQuery.PROP_QUERY_STRING, "SELECT \"water_level\" INTO \"where_else\".\"autogen\".\"h2o_feet_copy_2\" FROM \"h2o_feet\" WHERE \"location\" = 'coyote_creek'");
        runner.assertValid();

        runner.setProperty(ExecuteInfluxDBQuery.PROP_QUERY_STRING, "show databases");
        runner.assertValid();

        runner.setProperty(ExecuteInfluxDBQuery.PROP_QUERY_STRING, "show measurements on \"db\"");
        runner.assertValid();

        runner.setProperty(ExecuteInfluxDBQuery.PROP_QUERY_STRING, "SHOW RETENTION POLICIES ON \"my_database\"");
        runner.assertValid();

        runner.setProperty(ExecuteInfluxDBQuery.PROP_QUERY_STRING, "SHOW SERIES ON \"my_database\"");
        runner.assertValid();

        runner.setProperty(ExecuteInfluxDBQuery.PROP_QUERY_STRING, "SHOW MEASUREMENTS ON \"my_database\"");
        runner.assertValid();

        runner.setProperty(ExecuteInfluxDBQuery.PROP_QUERY_STRING, "create database \"hello\"");
        runner.assertValid();

        runner.setProperty(ExecuteInfluxDBQuery.PROP_QUERY_STRING, "DROP DATABASE \"water_database\"");
        runner.assertValid();

        runner.setProperty(ExecuteInfluxDBQuery.PROP_QUERY_STRING, "CREATE DATABASE \"some_database\" WITH DURATION 3d REPLICATION 1 SHARD DURATION 1h NAME \"liquid\"");
        runner.assertValid();

        runner.setProperty(ExecuteInfluxDBQuery.PROP_QUERY_STRING, "DROP SERIES FROM \"h2o_feet\" WHERE \"location\" = 'santa_monica'");
        runner.assertValid();

        runner.setProperty(ExecuteInfluxDBQuery.PROP_QUERY_STRING, "ALTER RETENTION POLICY \"what_is_time\" ON \"NOAA_water_database\" DURATION 3w SHARD DURATION 30m DEFAULT");
        runner.assertValid();
    }

    @Test
    public void invalidQueryTests() throws InitializationException {
        HashMap<String, String> props = new HashMap<>();
        InfluxDbService influxDbService = new InfluxDbService();

        TestRunner runner = TestRunners.newTestRunner(ExecuteInfluxDBQuery.class);
        runner.addControllerService("influxService", influxDbService, props);
        runner.setProperty(ExecuteInfluxDBQuery.INFLUX_DB_SERVICE, "influxService");
        runner.enableControllerService(influxDbService);

        runner.setProperty(ExecuteInfluxDBQuery.PRECISION, "Milliseconds");
        runner.setProperty(ExecuteInfluxDBQuery.DATABASE_NAME, "testDatabase");
        runner.assertNotValid();

        runner.setProperty(ExecuteInfluxDBQuery.PROP_QUERY_STRING, "selected * from db limit 1");
        runner.assertNotValid();

        // No limit specified
        runner.setProperty(ExecuteInfluxDBQuery.PROP_QUERY_STRING, "SELECT * FROM ingress");
        runner.assertNotValid();

        runner.setProperty(ExecuteInfluxDBQuery.PROP_QUERY_STRING, "show database");
        runner.assertNotValid();
    }

    @Test
    public void testResponse() {
        InfluxDB influxDb = InfluxDBFactory.connect("http://127.0.0.1:8086");
        influxDb.createDatabase("testdb");

        Query query = new Query("select * from processor limit 1", "testdb");
        QueryResult queryResult = influxDb.query(query, TimeUnit.MILLISECONDS);
        assertNotNull(queryResult);
    }

    @Test
    public void testDatabaseNotFound() {
        runner.setProperty(ExecuteInfluxDBQuery.PROP_QUERY_STRING, "SELECT * FROM ingress LIMIT 10");
        runner.run();
        runner.assertTransferCount(ExecuteInfluxDBQuery.REL_FAILURE, 1);

        final MockFlowFile out = runner.getFlowFilesForRelationship(ExecuteInfluxDBQuery.REL_FAILURE).get(0);
        assertEquals("{\"results\":[{\"error\":\"database not found: db\"}]}", new String(out.toByteArray()));
    }

    @Test
    public void testMeasurementNotFound() {
        InfluxDB influxDb = influxDbService.getInfluxDb();
        influxDb.query(create);

        influxDb.query(new Query("drop measurement ingress", "ExecuteInfluxDBQueryIT"));
        influxDb.query(new Query("drop database ExecuteInfluxDBQueryIT", "ExecuteInfluxDBQueryIT"));
        influxDb.query(new Query("create database ExecuteInfluxDBQueryIT", "ExecuteInfluxDBQueryIT"));

        runner.setProperty(ExecuteInfluxDBQuery.DATABASE_NAME, "ExecuteInfluxDBQueryIT");
        runner.setProperty(ExecuteInfluxDBQuery.PROP_QUERY_STRING, "SELECT * FROM ingress LIMIT 10");
        runner.run();
        runner.assertTransferCount(ExecuteInfluxDBQuery.REL_SUCCESS, 0);
        runner.assertTransferCount(ExecuteInfluxDBQuery.REL_FAILURE, 0);
    }

    @Test
    @Ignore
    public void testValidListOfResults() {
        InfluxDB influxDb = influxDbService.getInfluxDb();
        influxDb.query(create);

        influxDb.query(new Query("drop measurement ingress", "ExecuteInfluxDBQueryIT"));
        influxDb.query(new Query("drop database ExecuteInfluxDBQueryIT", "ExecuteInfluxDBQueryIT"));
        influxDb.query(new Query("create database ExecuteInfluxDBQueryIT", "ExecuteInfluxDBQueryIT"));


        String measurement = "ingress";
        BatchPoints points = BatchPoints.database("ExecuteInfluxDBQueryIT")
                .point(Point.measurement(measurement).tag("tag1", "tag1").addField("v", 1.23).build())
                .point(Point.measurement(measurement).tag("tag1", "tag1.1").addField("v", 3.14).build())
                .build();
        influxDb.write(points);

        runner.setProperty(ExecuteInfluxDBQuery.DATABASE_NAME, "ExecuteInfluxDBQueryIT");
        runner.setProperty(ExecuteInfluxDBQuery.PROP_QUERY_STRING, "SELECT * FROM " + measurement + " LIMIT 10");
        runner.run();
        runner.assertTransferCount(ExecuteInfluxDBQuery.REL_SUCCESS, 1);

        // Timestamp will differ from expected.
        final MockFlowFile out = runner.getFlowFilesForRelationship(ExecuteInfluxDBQuery.REL_SUCCESS).get(0);
        assertEquals("{\"measurement\":\"ingress\",\"columns\":[\"time\",\"tag1\",\"v\"],\"values\":[[1.561998264547E12,\"tag1\",1.23]]}", new String(out.toByteArray()));
    }
}