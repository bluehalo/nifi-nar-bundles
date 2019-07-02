package com.asymmetrik.nifi.processors;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"json", "attributes", "flowfile"})
@CapabilityDescription("Generates a JSON representation of the input FlowFile Attributes. The resulting JSON " +
        "is written to the FlowFile as content.")
public class AttributesToJsonContent extends AbstractProcessor {

    static final PropertyDescriptor ATTRIBUTES_LIST = new PropertyDescriptor.Builder()
            .name("Attributes List")
            .description("Comma separated list of attributes to be included in the JSON content.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The generated JSON content is sent to this relationship")
            .build();
    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original flowfile is sent to this relationship")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to convert attributes to JSON")
            .build();

    private volatile Set<String> attributes;
    private JsonParser parser = new JsonParser();

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        Splitter splitter = Splitter.on(',').trimResults().omitEmptyStrings();
        attributes = new HashSet<>(splitter.splitToList(context.getProperty(ATTRIBUTES_LIST).getValue()));
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        try {
            final JsonObject json = buildJsonContent(original.getAttributes(), this.attributes);

            FlowFile flowFile = session.create(original);
            flowFile = session.putAttribute(flowFile, "mime.type", "application/json");
            flowFile = session.write(flowFile, out ->
                    out.write(json.toString().getBytes(StandardCharsets.UTF_8)));
            session.transfer(flowFile, REL_SUCCESS);
            session.transfer(original, REL_ORIGINAL);
        } catch (Exception e) {
            getLogger().error(e.getMessage());
            session.transfer(original, REL_FAILURE);
        }
    }

    private JsonObject buildJsonContent(Map<String, String> attrs, Set<String> attributes) {
        JsonObject root = new JsonObject();
        if (attributes == null || attributes.isEmpty()) {
            return root;
        }

        for (String attribute : attributes) {
            String value = attrs.get(attribute);
            if (StringUtils.isNotBlank(value)) {
                root.add(attribute, coerceValue(value.trim()));
            }
        }
        return root;
    }

    private JsonElement coerceValue(String value) {

        // Try to coerce JSON value
        if (value.startsWith("{") && value.endsWith("}")) {
            return parser.parse(value);
        } else if (value.startsWith("[") && value.endsWith("]")) {
            return parser.parse(value);
        } else {
            try {
                return new JsonPrimitive(Integer.parseInt(value));
            } catch (NumberFormatException nfe) {
                try {
                    return new JsonPrimitive(Double.parseDouble(value));
                } catch (NumberFormatException nde) {
                    return new JsonPrimitive(value);
                }
            }
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(ATTRIBUTES_LIST);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return ImmutableSet.of(REL_ORIGINAL, REL_SUCCESS, REL_FAILURE);
    }
}