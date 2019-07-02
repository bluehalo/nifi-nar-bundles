package com.asymmetrik.nifi.processors.influxdb;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;

import com.google.common.io.Resources;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class ConvertInfluxOutputToLineProtocolTest {
    private TestRunner runner;


    @Before
    public void beforeEach() {
        HashMap<String, String> props = new HashMap<>();
        runner = TestRunners.newTestRunner(ConvertInfluxOutputToLineProtocol.class);
    }

    @Test
    public void simple() {
        String payload = "{\n" +
                "  \"measurement\": \"ingress\",\n" +
                "  \"columns\": [\"time\",\"tag1\",\"v\"],\n" +
                "  \"values\": [\n" +
                "    [1562078292789611010,\"tag1\",1.23],\n" +
                "    [1562078292723411123,\"tag1.1\",3.14]\n" +
                "  ],\n" +
                "  \"precision\": \"NANOSECONDS\"\n" +
                "}";
        String tags = "tag1";
        String fields = "v";

        JsonElement root = new JsonParser().parse(payload);

        ConvertInfluxOutputToLineProtocol converter = new ConvertInfluxOutputToLineProtocol();

        System.out.println(converter.convertPayload(payload, tags, fields));
    }

    @Test
    public void testValidListOfResults() {
        String payload = "{\"measurement\":\"ingress\",\"columns\":[\"time\",\"tag1\",\"v\"],\"values\":[[1562078292789611010,\"tag1\",1.23],[1562078292723411123,\"tag1.1\",3.14]],\"precision\":\"NANOSECONDS\"}";
        runner.setProperty(ConvertInfluxOutputToLineProtocol.PROP_FIELD_NAMES, "v");
        runner.enqueue(payload);
        runner.run();
        runner.assertTransferCount(ConvertInfluxOutputToLineProtocol.REL_SUCCESS, 2);
    }

    @Test
    public void testSimple() throws IOException {
        URL url = Resources.getResource("ConvertInfluxOutputToLineProtocol/sample_01.txt");
        final byte[] bytes = Resources.toByteArray(url);
        runner.setProperty(ConvertInfluxOutputToLineProtocol.PROP_FIELD_NAMES, "sum");
        runner.enqueue(bytes);
        runner.run();
        runner.assertTransferCount(ConvertInfluxOutputToLineProtocol.REL_SUCCESS, 361);
    }
}