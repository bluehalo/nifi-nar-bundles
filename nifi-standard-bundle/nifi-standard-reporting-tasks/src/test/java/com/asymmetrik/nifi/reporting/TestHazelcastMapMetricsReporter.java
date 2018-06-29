package com.asymmetrik.nifi.reporting;

import java.util.Hashtable;
import java.util.Set;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestHazelcastMapMetricsReporter {

    @Test
    public void testIsValid() throws MalformedObjectNameException {
        ObjectName o = new ObjectName("domain", "key", "value");
        Set<String> mapNames = ImmutableSet.of("foo");
        HazelcastMapMetricsReporter hazelcastMapMetricsReporter = new HazelcastMapMetricsReporter();
        boolean isValid = hazelcastMapMetricsReporter.isValid(o, mapNames, "cluster");
        assertFalse(isValid);

        String c = "foo";
        o = new ObjectName("com.hazelcast", "key", "value");
        isValid = hazelcastMapMetricsReporter.isValid(o, mapNames, c);
        assertFalse(isValid);

        o = new ObjectName("com.hazelcast", "name", "foo");
        isValid = hazelcastMapMetricsReporter.isValid(o, mapNames, c);
        assertFalse(isValid);

        Hashtable<String, String> keys = new Hashtable<>();
        keys.put("name", "foo");
        keys.put("type", "IMap");
        o = new ObjectName("com.hazelcast", keys);
        isValid = hazelcastMapMetricsReporter.isValid(o, mapNames, c);
        assertFalse(isValid);

        keys = new Hashtable<>();
        keys.put("name", "foo");
        keys.put("type", "IMap");
        keys.put("instance", "foo");
        o = new ObjectName("com.hazelcast", keys);
        isValid = hazelcastMapMetricsReporter.isValid(o, mapNames, c);
        assertTrue(isValid);
    }

}