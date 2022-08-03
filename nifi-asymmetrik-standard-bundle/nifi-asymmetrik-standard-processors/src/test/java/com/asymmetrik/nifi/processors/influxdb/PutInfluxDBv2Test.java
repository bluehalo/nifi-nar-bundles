package com.asymmetrik.nifi.processors.influxdb;

import org.apache.nifi.components.ValidationResult;
import org.junit.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class PutInfluxDBv2Test {

    @Test
    public void testBasicKeyValueStringValidator() {
        final String kvp = "foo=world, bar=hello";

        ValidationResult vResult = new PutInfluxDBv2.KeyValueStringValidator().validate("subject", kvp, null);
        assertTrue(vResult.isValid());

        Map<String, String> result =  PutInfluxDBv2.KeyValueStringValidator.parse(kvp);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("world", result.remove("foo"));
        assertEquals("hello", result.remove("bar"));
    }

    @Test
    public void testPartialKeyValueStringValidator() {
        final String kvp = "foo=world, bar=";

        ValidationResult vResult = new PutInfluxDBv2.KeyValueStringValidator().validate("subject", kvp, null);
        assertFalse(vResult.isValid());
    }

    @Test
    public void testQuotedKeyValueStringValidator() {
        final String kvp = "\"foo=hello, world\", bar=hello";

        ValidationResult vResult = new PutInfluxDBv2.KeyValueStringValidator().validate("subject", kvp, null);
        assertTrue(vResult.isValid());

        Map<String, String> result =  PutInfluxDBv2.KeyValueStringValidator.parse(kvp);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("hello, world", result.remove("foo"));
        assertEquals("hello", result.remove("bar"));
    }

    @Test
    public void testInnerQuotesKeyValueStringValidator() {
        final String kvp = "foo=he said \"hello, world\", bar=hello";

        ValidationResult vResult = new PutInfluxDBv2.KeyValueStringValidator().validate("subject", kvp, null);
        assertTrue(vResult.isValid());

        Map<String, String> result =  PutInfluxDBv2.KeyValueStringValidator.parse(kvp);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("he said \"hello, world", result.remove("foo"));
        assertEquals("hello", result.remove("bar"));
    }

    @Test
    public void testInnerOuterQuotesKeyValueStringValidator() {
        final String kvp = "foo=he said \"hello, world\", bar=hello";

        ValidationResult vResult = new PutInfluxDBv2.KeyValueStringValidator().validate("subject", kvp, null);
        assertTrue(vResult.isValid());

        Map<String, String> result =  PutInfluxDBv2.KeyValueStringValidator.parse(kvp);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("he said \"hello, world", result.remove("foo"));
        assertEquals("hello", result.remove("bar"));
    }

    @Test
    public void testEqualsKeyValueStringValidator() {
        final String kvp = "  =";

        ValidationResult vResult = new PutInfluxDBv2.KeyValueStringValidator().validate("subject", kvp, null);
        assertFalse(vResult.isValid());

        Map<String, String> result =  PutInfluxDBv2.KeyValueStringValidator.parse(kvp);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testEmptyKeyValueStringValidator() {
        final String kvp = "";

        ValidationResult vResult = new PutInfluxDBv2.KeyValueStringValidator().validate("subject", kvp, null);
        assertFalse(vResult.isValid());

        Map<String, String> result =  PutInfluxDBv2.KeyValueStringValidator.parse(kvp);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testNullKeyValueStringValidator() {
        final String kvp = null;

        ValidationResult vResult = new PutInfluxDBv2.KeyValueStringValidator().validate("subject", kvp, null);
        assertFalse(vResult.isValid());

        Map<String, String> result =  PutInfluxDBv2.KeyValueStringValidator.parse(kvp);
        assertTrue(result.isEmpty());
    }
}