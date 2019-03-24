package com.asymmetrik.nifi.processors.influxdb;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import com.asymmetrik.nifi.services.influxdb.InfluxDatabaseService;
import com.asymmetrik.nifi.services.influxdb.InfluxDbService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockPropertyValue;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PutInfluxDBIT {

    private static final String INFLUX_CONTROLLER_SERVICE = "influx_controller_service";

    private InfluxDatabaseService influxDbService;
    private PropertyDescriptor dynamicProp;

    private static final String DB = "TestPutInfluxDB";
    private static final String MEASUREMENT = "m";

    @Before
    public void beforeEach() {
        influxDbService = new InfluxDbService();
        dynamicProp = new PropertyDescriptor.Builder()
                .name("fieldKey")
                .description("Key-value tags containing metadata. Conceptually tags are indexed columns in a table. Format expected: <tag-key>=<tag-value>,...")
                .required(false)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .dynamic(true)
                .build();

    }

    @Test
    public void simplePropertyTests() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new PutInfluxDB());
        addInfluxService(runner);
        runner.setProperty(PutInfluxDB.MEASUREMENT, MEASUREMENT);
        runner.setProperty(PutInfluxDB.DATABASE_NAME, DB);
        runner.setProperty(PutInfluxDB.CONSISTENCY_LEVEL, PutInfluxDB.CONSISTENCY_LEVEL_QUORUM);
        runner.setProperty(PutInfluxDB.RETENTION_POLICY, "ten_days");
        runner.setProperty(PutInfluxDB.TAGS, "invalidTags");
        runner.assertNotValid();

        runner.setProperty(PutInfluxDB.TAGS, "tag1=t1, tag2=t2");
        runner.assertValid();

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        runner.enqueue(ff);
        runner.run();
        runner.assertTransferCount(PutInfluxDB.REL_FAILURE, 1);
    }

    @Test
    public void testOptionalProperties() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new PutInfluxDB());
        addInfluxService(runner);
        runner.setProperty(PutInfluxDB.MEASUREMENT, MEASUREMENT);
        runner.setProperty(PutInfluxDB.DATABASE_NAME, DB);
        runner.setProperty(PutInfluxDB.CONSISTENCY_LEVEL, PutInfluxDB.CONSISTENCY_LEVEL_ALL);
        runner.assertValid();

        runner.setProperty(PutInfluxDB.RETENTION_POLICY, "ten_days");
        runner.assertValid();
    }

    @Test
    public void testOptionalTimestampProperties() throws InitializationException {
        PutInfluxDB putInfluxDB = new PutInfluxDB();
        TestRunner runner = TestRunners.newTestRunner(putInfluxDB);
        addInfluxService(runner);
        runner.setProperty(PutInfluxDB.MEASUREMENT, MEASUREMENT);
        runner.setProperty(PutInfluxDB.DATABASE_NAME, DB);
        runner.setProperty(PutInfluxDB.TIMESTAMP, "123456789012");
        PropertyDescriptor i1 = new PropertyDescriptor.Builder().name("field").description("asdf").dynamic(true).build();
        runner.setProperty(i1, "1.1");

        runner.assertValid();

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        runner.enqueue(ff);
        runner.run();
        runner.assertTransferCount(PutInfluxDB.REL_FAILURE, 0);
        runner.assertTransferCount(PutInfluxDB.REL_SUCCESS, 1);
    }

    @Test
    public void simpleTest() throws InitializationException {
        Map<String, PropertyValue> dynamicProps = new ConcurrentHashMap<>();
        dynamicProps.put("field", new MockPropertyValue("1.1"));

        PutInfluxDB putInfluxDB = new PutInfluxDB();
        putInfluxDB.dynamicFieldValues = dynamicProps;

        TestRunner runner = TestRunners.newTestRunner(putInfluxDB);
        addInfluxService(runner);
        runner.setProperty(PutInfluxDB.MEASUREMENT, MEASUREMENT);
        runner.setProperty(PutInfluxDB.DATABASE_NAME, DB);
        runner.setProperty(PutInfluxDB.CONSISTENCY_LEVEL, PutInfluxDB.CONSISTENCY_LEVEL_ANY);
        runner.setProperty(PutInfluxDB.TAGS, "tag1=t1, tag2=t2");
        runner.setProperty(dynamicProp, "1.2");

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        runner.enqueue(ff);
        runner.run();
        runner.assertTransferCount(PutInfluxDB.REL_FAILURE, 0);
        runner.assertTransferCount(PutInfluxDB.REL_SUCCESS, 1);
    }

    @Test
    public void testNonExistentRetentionPolicy() throws InitializationException {
        Map<String, PropertyValue> dynamicProps = new ConcurrentHashMap<>();
        dynamicProps.put("field", new MockPropertyValue("1.1"));

        PutInfluxDB putInfluxDB = new PutInfluxDB();
        putInfluxDB.dynamicFieldValues = dynamicProps;

        TestRunner runner = TestRunners.newTestRunner(putInfluxDB);
        addInfluxService(runner);
        runner.setProperty(PutInfluxDB.MEASUREMENT, MEASUREMENT);
        runner.setProperty(PutInfluxDB.DATABASE_NAME, DB);
        runner.setProperty(PutInfluxDB.CONSISTENCY_LEVEL, PutInfluxDB.CONSISTENCY_LEVEL_ANY);
        runner.setProperty(PutInfluxDB.RETENTION_POLICY, "does_not_exists");
        runner.setProperty(PutInfluxDB.TAGS, "tag1=t1, tag2=t2");
        runner.setProperty(dynamicProp, "1.2");

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        runner.enqueue(ff);
        runner.run();
        runner.assertTransferCount(PutInfluxDB.REL_FAILURE, 1);
        runner.assertTransferCount(PutInfluxDB.REL_SUCCESS, 0);
    }

    @Test
    public void simpleInvalidFieldValueTest() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new PutInfluxDB());
        addInfluxService(runner);
        runner.setProperty(PutInfluxDB.MEASUREMENT, MEASUREMENT);
        runner.setProperty(PutInfluxDB.DATABASE_NAME, DB);
        runner.setProperty(PutInfluxDB.CONSISTENCY_LEVEL, PutInfluxDB.CONSISTENCY_LEVEL_ONE);
        runner.setProperty(PutInfluxDB.RETENTION_POLICY, "ten_days");
        runner.setProperty(PutInfluxDB.TAGS, "tag1=t1, tag2=t2");
        runner.setProperty(dynamicProp, "invalid");

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        runner.enqueue(ff);
        runner.run();
        runner.assertTransferCount(PutInfluxDB.REL_FAILURE, 1);
        runner.assertTransferCount(PutInfluxDB.REL_SUCCESS, 0);
    }

    private void addInfluxService(TestRunner runner) throws InitializationException {
        runner.addControllerService(INFLUX_CONTROLLER_SERVICE, influxDbService, new HashMap<>());
        runner.setProperty(PutInfluxDB.INFLUX_DB_SERVICE, INFLUX_CONTROLLER_SERVICE);
        runner.enableControllerService(influxDbService);
    }

    @Test
    public void testCollectPoints() {
        String tagString = "foobar=helloworld,yes=no";
        String measurement = "testing123";
        String database = "testdb";

        final MockProcessContext context = new MockProcessContext(new PutInfluxDB());
        context.setProperty(PutInfluxDB.DATABASE_NAME, database);
        context.setProperty(PutInfluxDB.MEASUREMENT, measurement);
        context.setProperty(PutInfluxDB.TAGS, tagString);

        Map<String, PropertyValue> dynamicProps = new ConcurrentHashMap<>();
        dynamicProps.put("field", new MockPropertyValue("1.1"));

        PutInfluxDB putInfluxDB = new PutInfluxDB();
        putInfluxDB.dynamicFieldValues = dynamicProps;

        MockFlowFile ff = new MockFlowFile(1);
        Optional<BatchPoints> result = putInfluxDB.collectPoints(context, ImmutableList.of(ff));
        assertTrue(result.isPresent());

        BatchPoints batchPoints = result.get();
        assertEquals(database, batchPoints.getDatabase());
        List<Point> points = batchPoints.getPoints();
        assertEquals(1, points.size());
    }

    @Test
    public void testGetFields() {
        String fieldName = "field_name";
        String fieldValue = "1.1";

        final MockProcessContext context = new MockProcessContext(new PutInfluxDB());
        context.setProperty(fieldName, fieldValue);

        MockFlowFile ff = new MockFlowFile(1);
        Map<String, Object> result = new PutInfluxDB().getFields(ff, ImmutableMap.of(
                "field_name2", new MockPropertyValue("1.2"),
                "field_name3", new MockPropertyValue("1.3")
        ));

        assertEquals(2, result.size());
        assertEquals(1.2, (double) result.remove("field_name2"), Double.MIN_VALUE);
        assertEquals(1.3, (double) result.remove("field_name3"), Double.MIN_VALUE);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetFieldsNoOptionalFields() {
        Map<String, PropertyValue> dynamicProps = new ConcurrentHashMap<>();
        dynamicProps.put("field", new MockPropertyValue("1.1"));

        MockFlowFile ff = new MockFlowFile(1);
        PutInfluxDB putInfluxDB = new PutInfluxDB();
        Map<String, Object> result = putInfluxDB.getFields(ff, dynamicProps);

        assertEquals(1, result.size());
        assertEquals(1.1, (double) result.remove("field"), Double.MIN_VALUE);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetFieldsExpressions() {
        String fieldName = "field_name";
        String fieldValue = "${field_value}";

        final MockProcessContext context = new MockProcessContext(new PutInfluxDB());
        context.setProperty(fieldName, fieldValue);

        MockFlowFile ff = new MockFlowFile(1);
        ff.putAttributes(ImmutableMap.of("field_value", "2.1"));

        Map<String, Object> result = new PutInfluxDB().getFields(ff, ImmutableMap.of(
                "field_name2", new MockPropertyValue("${field_value}"),
                "field_name3", new MockPropertyValue("${field_value}")
        ));

        assertEquals(2, result.size());
        assertEquals(2.1, (double) result.remove("field_name2"), Double.MIN_VALUE);
        assertEquals(2.1, (double) result.remove("field_name3"), Double.MIN_VALUE);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testKeyValueValidatorParse() {
        Map<String, String> result = PutInfluxDB.KeyValueStringValidator.parse("foo=bar");
        assert result != null;

        assertEquals(1, result.size());
        assertEquals("bar", result.get("foo"));

        result = PutInfluxDB.KeyValueStringValidator.parse("foo=bar,hello = world ");
        assertEquals(2, result.size());
        assertEquals("world", result.get("hello"));

        assertNull(PutInfluxDB.KeyValueStringValidator.parse("abc"));
        assertNull(PutInfluxDB.KeyValueStringValidator.parse(""));
        assertTrue(PutInfluxDB.KeyValueStringValidator.parse(null).isEmpty());
    }

    @Test
    public void testKeyValueValidatorValidate() {
        PutInfluxDB.KeyValueStringValidator validator = new PutInfluxDB.KeyValueStringValidator();

        ValidationResult validationResult = validator.validate("sdf", "asdf", null);
        assertFalse(validationResult.isValid());

        validationResult = validator.validate("adsf", "foo=bar,hello = world ", null);
        assertTrue(validationResult.isValid());
    }

   // @Before
   // public void setUp() throws Exception {
   //     influxService = new InfluxDbService();
   //
   //     TestRunner runner = TestRunners.newTestRunner(PutInfluxDB.class);
   //     runner.setProperty(PutInfluxDB.DATABASE_NAME, "UnitTesting");
   //     runner.setProperty(PutInfluxDB.INFLUX_DB_SERVICE, "influxdb");
   //
   //     runner.addControllerService("influx", influxService, new HashMap<>());
   //     runner.setProperty(PutInfluxDB.INFLUX_DB_SERVICE, "influx");
   //     runner.enableControllerService(influxService);
   // }

//    @Test
//    public void tagNoDynamicField() {
//        runner.setProperty(PutInfluxDB.MEASUREMENT, "appleseed");
//        runner.setProperty(PutInfluxDB.TAGS, "chang=lee, good=bad, walking = dead");
//
//        runner.enqueue(new byte[0]);
//        runner.run();
//    }
//
//
//    @Test
//    public void tagDynamicField() {
//        runner.setProperty(PutInfluxDB.MEASUREMENT, "def");
//        runner.setProperty(PutInfluxDB.TAGS, "chang=lee, good=bad, walking = dead");
//        runner.setProperty(PutInfluxDB.FIELD_VALUE, "35.13");
//
//        runner.setProperty("dynamic1", "3939.03");
//        runner.setProperty("dynamic2", "3939.05");
//
//        runner.enqueue(new byte[0]);
//        runner.run();
//    }
//
//    @Test
//    public void emptyTagNoDynamicField() {
//        runner.setProperty(PutInfluxDB.MEASUREMENT, "ghi");
//        runner.setProperty(PutInfluxDB.TAGS, "");
//
//        runner.enqueue(new byte[0]);
//        runner.run();
//    }
//
//    @Test
//    public void emptyTagDynamicField() {
//        runner.setProperty(PutInfluxDB.MEASUREMENT, "jkl");
//        runner.setProperty(PutInfluxDB.TAGS, "");
//
//        runner.setProperty("dynamic1", "3939.03");
//        runner.setProperty("dynamic2", "3939.05");
//
//        runner.enqueue(new byte[0]);
//        runner.run();
//    }
//
//    @Test
//    public void noTagNoDynamicField() {
//        runner.setProperty(PutInfluxDB.MEASUREMENT, "mno");
//
//        runner.enqueue(new byte[0]);
//        runner.run();
//    }
//
//    @Test
//    public void noTagDynamicField() {
//        runner.setProperty(PutInfluxDB.MEASUREMENT, "pqr");
//
//        runner.setProperty("dynamic1", "3939.03");
//        runner.setProperty("dynamic2", "3939.05");
//
//        runner.enqueue(new byte[0]);
//        runner.run();
//    }
}