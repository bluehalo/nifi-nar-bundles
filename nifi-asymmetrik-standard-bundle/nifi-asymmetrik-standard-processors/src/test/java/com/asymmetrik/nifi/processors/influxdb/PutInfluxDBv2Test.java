package com.asymmetrik.nifi.processors.influxdb;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.util.MockValidationContext;
import org.junit.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PutInfluxDBv2Test {

    @Test
    public void testBasicKeyValueStringValidator() {
        final String kvp = "foo=world, bar=hello";

        assertTrue(validate(kvp, false, false));

        Map<String, String> result =  PutInfluxDBv2.KeyValueStringValidator.parse(kvp);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("world", result.remove("foo"));
        assertEquals("hello", result.remove("bar"));
    }

    @Test
    public void testPartialKeyValueStringValidator() {
        final String kvp = "foo=world, bar=";
        assertFalse(validate(kvp, false, false));
    }

    @Test
    public void testQuotedKeyValueStringValidator() {
        final String kvp = "\"foo=hello, world\", bar=hello";

        assertTrue(validate(kvp, false, false));

        Map<String, String> result =  PutInfluxDBv2.KeyValueStringValidator.parse(kvp);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("hello, world", result.remove("foo"));
        assertEquals("hello", result.remove("bar"));
    }

    @Test
    public void testInnerQuotesKeyValueStringValidator() {
        final String kvp = "foo=he said \"hello, world\", bar=hello";

        assertTrue(validate(kvp, false, false));

        Map<String, String> result =  PutInfluxDBv2.KeyValueStringValidator.parse(kvp);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("he said \"hello, world", result.remove("foo"));
        assertEquals("hello", result.remove("bar"));
    }

    @Test
    public void testInnerOuterQuotesKeyValueStringValidator() {
        final String kvp = "foo=he said \"hello, world\", bar=hello";

        assertTrue(validate(kvp, false, false));

        Map<String, String> result =  PutInfluxDBv2.KeyValueStringValidator.parse(kvp);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("he said \"hello, world", result.remove("foo"));
        assertEquals("hello", result.remove("bar"));
    }

    @Test
    public void testEqualsKeyValueStringValidator() {
        final String kvp = "  =";

        assertFalse(validate(kvp, false, false));

        Map<String, String> result =  PutInfluxDBv2.KeyValueStringValidator.parse(kvp);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testEmptyKeyValueStringValidator() {
        final String kvp = "";

        assertFalse(validate(kvp, false, false));

        Map<String, String> result =  PutInfluxDBv2.KeyValueStringValidator.parse(kvp);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testNullKeyValueStringValidator() {
        final String kvp = null;

        assertFalse(validate(kvp, false, false));

        Map<String, String> result =  PutInfluxDBv2.KeyValueStringValidator.parse(kvp);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testExpressionLanguageKeyValueStringValidator() {
        final String kvp = "${foo}";
        assertTrue(validate(kvp, true, true));
    }

    private boolean validate(String value, boolean expressionLanguageSupported, boolean isExpressionLanguagePresent) {
        final String subject = "subject";
        MockValidationContext mockContext = mock(MockValidationContext.class);
        when(mockContext.isExpressionLanguageSupported(subject)).thenReturn(expressionLanguageSupported);
        when(mockContext.isExpressionLanguagePresent(value)).thenReturn(isExpressionLanguagePresent);

        ValidationResult vResult = new PutInfluxDBv2.KeyValueStringValidator().validate(subject, value, mockContext);
        return vResult.isValid();
    }
}