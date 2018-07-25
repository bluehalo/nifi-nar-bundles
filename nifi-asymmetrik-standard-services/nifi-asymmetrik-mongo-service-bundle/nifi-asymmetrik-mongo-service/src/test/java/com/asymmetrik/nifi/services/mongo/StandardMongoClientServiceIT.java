package com.asymmetrik.nifi.services.mongo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Requires a Mongo instance running at localhost:28017
 */
public class StandardMongoClientServiceIT {

    private static final String MONGO_DATABASE_NAME = "testUpdateMongo" + UUID.randomUUID();
    final static String DB_LOCATION = "target/db";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setup() {
        System.setProperty("derby.stream.error.file", "target/derby.log");
    }

    @Test
    public void test_mongoservice_not_enabled() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final StandardMongoClientService service = new StandardMongoClientService();

        runner.addControllerService("test-mongo-closed", service);
        runner.enableControllerService(service);

        // Close MongoClient
        MongoClient client = service.getMongoClient();

        // NOTE: This test requires mongo to be running
        assertEquals("localhost:27017", client.getConnectPoint());

        // Close the mongo connection
        client.close();

        // Now, this should throw an illegal state exception
        thrown.expect(IllegalStateException.class);
        client.getConnectPoint();
    }

    /**
     * Unknown database system.
     */
    @SuppressWarnings("unused")
    @Test
    public void testUnknownDatabaseSystem() throws InitializationException {
        new StandardMongoClientService();
    }

    @Test
    public void single_host_without_credentials() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final StandardMongoClientService service = new StandardMongoClientService();
        runner.addControllerService("test-good1", service);

        runner.setProperty(service, StandardMongoClientService.HOSTS, "localhost:27017");
        runner.enableControllerService(service);

        runner.assertValid(service);
        StandardMongoClientService mongoService = (StandardMongoClientService) runner.getProcessContext().getControllerServiceLookup().getControllerService("test-good1");
        assertNotNull(mongoService);
        MongoClient mongoClient = mongoService.getMongoClient();
        assertNotNull(mongoClient);

        assertEquals(0, mongoClient.getDatabase(MONGO_DATABASE_NAME).getCollection("sample").count());
        mongoClient.dropDatabase(MONGO_DATABASE_NAME);
    }

    @Test
    public void validating_valid_inputs_1() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final StandardMongoClientService service = new StandardMongoClientService();

        Map<String, String> props = new HashMap<>();
        props.put(StandardMongoClientService.HOSTS.getName(), "localhost:27017");
        runner.addControllerService("test_hosts", service, props);
        runner.assertValid(service);
    }

    @Test
    public void validating_incomplete_auth_inputs_1() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final StandardMongoClientService service = new StandardMongoClientService();

        Map<String, String> props = new HashMap<>();
        props.put(StandardMongoClientService.HOSTS.getName(), "localhost:27017");
        props.put(StandardMongoClientService.USERNAME.getName(), "mongouser");
        runner.addControllerService("test_hosts", service, props);
        runner.assertNotValid(service);
    }

    @Test
    public void validating_incomplete_auth_inputs_2() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final StandardMongoClientService service = new StandardMongoClientService();

        Map<String, String> props = new HashMap<>();
        props.put(StandardMongoClientService.HOSTS.getName(), "localhost:27017");
        props.put(StandardMongoClientService.PASSWORD.getName(), "mongouser");
        runner.addControllerService("test_hosts", service, props);
        runner.assertNotValid(service);
    }

    @Test
    public void validating_incomplete_auth_inputs_3() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final StandardMongoClientService service = new StandardMongoClientService();

        Map<String, String> props = new HashMap<>();
        props.put(StandardMongoClientService.HOSTS.getName(), "localhost:27017");
        props.put(StandardMongoClientService.AUTH_DATABASE.getName(), "mongouser");
        runner.addControllerService("test_hosts", service, props);
        runner.assertNotValid(service);
    }

    @Test
    public void validating_incomplete_auth_inputs_4() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final StandardMongoClientService service = new StandardMongoClientService();

        Map<String, String> props = new HashMap<>();
        props.put(StandardMongoClientService.HOSTS.getName(), "localhost:27017");
        props.put(StandardMongoClientService.USERNAME.getName(), "mongouser");
        props.put(StandardMongoClientService.PASSWORD.getName(), "mongouser");
        runner.addControllerService("test_hosts", service, props);
        runner.assertNotValid(service);
    }

    @Test
    public void validating_incomplete_auth_inputs_5() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final StandardMongoClientService service = new StandardMongoClientService();

        Map<String, String> props = new HashMap<>();
        props.put(StandardMongoClientService.HOSTS.getName(), "localhost:27017");
        props.put(StandardMongoClientService.USERNAME.getName(), "mongouser");
        props.put(StandardMongoClientService.AUTH_DATABASE.getName(), "mongouser");
        runner.addControllerService("test_hosts", service, props);
        runner.assertNotValid(service);
    }

    @Test
    public void validating_incomplete_auth_inputs_6() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final StandardMongoClientService service = new StandardMongoClientService();

        Map<String, String> props = new HashMap<>();
        props.put(StandardMongoClientService.HOSTS.getName(), "localhost:27017");
        props.put(StandardMongoClientService.PASSWORD.getName(), "mongouser");
        props.put(StandardMongoClientService.AUTH_DATABASE.getName(), "mongouser");
        runner.addControllerService("test_hosts", service, props);
        runner.assertNotValid(service);
    }

    @Test
    public void validating_complete_auth_inputs() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final StandardMongoClientService service = new StandardMongoClientService();

        Map<String, String> props = new HashMap<>();
        props.put(StandardMongoClientService.HOSTS.getName(), "localhost:27017");
        props.put(StandardMongoClientService.USERNAME.getName(), "mongouser");
        props.put(StandardMongoClientService.PASSWORD.getName(), "mongouser");
        props.put(StandardMongoClientService.AUTH_DATABASE.getName(), "mongouser");
        runner.addControllerService("test_hosts", service, props);
        runner.assertValid(service);
    }

    @Test
    public void test_various_host_inputs() {
        StandardMongoClientService service = new StandardMongoClientService();

        // Single host without specifying port
        List<ServerAddress> addresses = service.parseServerAddresses("e01sv05");
        assertEquals("Expecting one host", 1, addresses.size());

        ServerAddress address = addresses.get(0);
        assertEquals("should be host: e01sv05", "e01sv05", address.getHost());
        assertEquals("should be default port: 27017", 27017, address.getPort());

        // Multiple hosts without specifying port
        addresses = service.parseServerAddresses("e01sv05, e01sv06");
        assertEquals("Expecting two hosts", 2, addresses.size());

        address = addresses.get(0);
        assertEquals("should be host: e01sv05", "e01sv05", address.getHost());
        assertEquals("should be default port: 27017", 27017, address.getPort());

        address = addresses.get(1);
        assertEquals("should be host: e01sv06", "e01sv06", address.getHost());
        assertEquals("should be default port: 27017", 27017, address.getPort());

        // Multiple hosts, specifying port
        addresses = service.parseServerAddresses("e01sv05:2700, e01sv05:2701");
        assertEquals("Expecting two hosts", 2, addresses.size());

        address = addresses.get(0);
        assertEquals("should be host: e01sv05", "e01sv05", address.getHost());
        assertEquals("should be port: 2700", 2700, address.getPort());

        address = addresses.get(1);
        assertEquals("should be host: e01sv05", "e01sv05", address.getHost());
        assertEquals("should be port: 2701", 2701, address.getPort());

        // Multiple, repeating hosts, specifying port
        addresses = service.parseServerAddresses("e01sv05, e01sv05");
        assertEquals("Expecting two hosts", 2, addresses.size());

        address = addresses.get(0);
        assertEquals("should be host: e01sv05", "e01sv05", address.getHost());
        assertEquals("should be port: 27017", 27017, address.getPort());

        address = addresses.get(1);
        assertEquals("should be host: e01sv05", "e01sv05", address.getHost());
        assertEquals("should be port: 27017", 27017, address.getPort());

        // Try space delimiter
        addresses = service.parseServerAddresses("e01sv05:27017 e01sv05:27018");
        assertEquals("Expecting one host", 2, addresses.size());

        address = addresses.get(0);
        assertEquals("should be host: e01sv05", "e01sv05", address.getHost());
        assertEquals("should be port: 27017", 27017, address.getPort());

        address = addresses.get(1);
        assertEquals("should be host: e01sv05", "e01sv05", address.getHost());
        assertEquals("should be port: 27018", 27018, address.getPort());

        // Try repeated space delimiter
        addresses = service.parseServerAddresses("e01sv05:27017      e01sv05:27018");
        assertEquals("Expecting one host", 2, addresses.size());

        address = addresses.get(0);
        assertEquals("should be host: e01sv05", "e01sv05", address.getHost());
        assertEquals("should be port: 27017", 27017, address.getPort());

        address = addresses.get(1);
        assertEquals("should be host: e01sv05", "e01sv05", address.getHost());
        assertEquals("should be port: 27018", 27018, address.getPort());

        // Try semi-colon delimiter
        addresses = service.parseServerAddresses("e01sv05:27017;e01sv05:27018");
        assertEquals("Expecting one host", 2, addresses.size());

        address = addresses.get(0);
        assertEquals("should be host: e01sv05", "e01sv05", address.getHost());
        assertEquals("should be port: 27017", 27017, address.getPort());

        address = addresses.get(1);
        assertEquals("should be host: e01sv05", "e01sv05", address.getHost());
        assertEquals("should be port: 27018", 27018, address.getPort());

        // Try repeated semi-colon delimiter
        addresses = service.parseServerAddresses("e01sv05:27017;;;;e01sv05:27018");
        assertEquals("Expecting one host", 2, addresses.size());

        address = addresses.get(0);
        assertEquals("should be host: e01sv05", "e01sv05", address.getHost());
        assertEquals("should be port: 27017", 27017, address.getPort());

        address = addresses.get(1);
        assertEquals("should be host: e01sv05", "e01sv05", address.getHost());
        assertEquals("should be port: 27018", 27018, address.getPort());
    }
}
