package com.asymmetrik.nifi.processors;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

@SideEffectFree
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@SupportsBatching
@Tags({"asymmetrik", "content", "create"})
@CapabilityDescription("Creates new flowfile content based on the content property")
public class CreateContent extends AbstractProcessor {

    /**
     * Relationships
     */
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("the original content")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("the embedded payload")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files that could not be written to the output directory for some reason are transferred to this relationship")
            .build();

    public static final PropertyDescriptor CONTENT_FIELD = new PropertyDescriptor.Builder()
            .name("Content Field")
            .description("The template for the content to be created")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;


    @Override
    protected void init(ProcessorInitializationContext context) {
        final List<PropertyDescriptor> props = Arrays.asList(CONTENT_FIELD);
        properties = Collections.unmodifiableList(props);

        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_ORIGINAL);
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(rels);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) {
        FlowFile original = session.get();

        FlowFile flowFile;
        if (original == null) {
            flowFile = session.create();
            session.getProvenanceReporter().create(flowFile);
        } else {
            flowFile = session.clone(original);
            session.transfer(original, REL_ORIGINAL);
        }

        final String updatedContent = StringFormatter.format(context.getProperty(CONTENT_FIELD).getValue(), flowFile.getAttributes());
        this.getLogger().debug("Created content: {}", new Object[]{updatedContent});

        flowFile = session.write(flowFile, outputStream -> outputStream.write(updatedContent.getBytes(StandardCharsets.UTF_8)));
        session.getProvenanceReporter().modifyContent(flowFile);
        session.transfer(flowFile, REL_SUCCESS);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    public static class StringFormatter {

        private static final String FIELD_START = "\\{\\{";
        private static final String FIELD_END = "\\}\\}";

        private static final String REGEX = FIELD_START + "([^}]+)" + FIELD_END;
        private static final Pattern PATTERN = Pattern.compile(REGEX);

        private StringFormatter() {
        }

        public static String format(String content, Map<String, String> props) {
            Matcher m = PATTERN.matcher(Pattern.quote(content));
            String result = content;
            while (m.find()) {
                String found = m.group(1);
                String newVal = props.getOrDefault(found.trim(), "null");

                String escaped = StringEscapeUtils.escapeHtml4(newVal);
                result = result.replaceFirst(REGEX, Matcher.quoteReplacement(escaped));
            }
            return StringEscapeUtils.unescapeHtml4(result);
        }
    }
}
