package com.asymmetrik.nifi.processors;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StringFormatterTest {

    @Test
    public void singleFieldTest() {

        Map<String, String> props = new HashMap<>();
        props.put("person", "Rick");
        String sample = "Hello {{person}}.  Your name is {{person}}.";

        String updated = CreateContent.StringFormatter.format(sample, props);
        assertEquals("Hello Rick.  Your name is Rick.", updated);
    }

    @Test
    public void multipleFieldsTest() {

        Map<String, String> props = new HashMap<>();
        props.put("person", "Rick");
        props.put("sport", "Jiu-Jitsu");
        String sample = "Hello {{person}}.  Your sport is {{sport}}.";

        String updated = CreateContent.StringFormatter.format(sample, props);
        assertEquals("Hello Rick.  Your sport is Jiu-Jitsu.", updated);
    }

}
