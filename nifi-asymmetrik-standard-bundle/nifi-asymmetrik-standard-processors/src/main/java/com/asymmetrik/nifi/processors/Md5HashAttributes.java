package com.asymmetrik.nifi.processors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@EventDriven
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"hash", "content", "MD5"})
@CapabilityDescription("Calculates a hash value for each dynamic property value and puts that hash value on the FlowFile as an attribute whose name "
        + "is determined by the key the dynamic property")
@DynamicProperty(name = "A FlowFile attribute to hash", value = "The value that will be hashed", supportsExpressionLanguage = true,
        description = "Updates a FlowFile attribute specified by the Dynamic Property's key with the value's hash  specified by the Dynamic Property's value")
@WritesAttribute(attribute = "<Dynamic Property Key>", description = "This Processor adds an attribute whose value is the result of Hashing the "
        + "dynamic property value. The name of this attribute is specified by the <Dynamic Property Key> property")
public class Md5HashAttributes extends AbstractProcessor {


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are process successfully will be sent to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that cannot be processed successfully will be sent to this relationship without any attribute being added")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;


    @Override
    protected void init(ProcessorInitializationContext context) {
        final List<PropertyDescriptor> props = new ArrayList<>();
        properties = Collections.unmodifiableList(props);

        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(rels);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .required(false)
                .dynamic(true)
                .build();
    }


    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }


        final ComponentLog logger = getLogger();


        try {


            for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
                if (!entry.getKey().isDynamic()) {
                    continue;
                }
                PropertyDescriptor attributeDescriptor = entry.getKey();
                String value = context.getProperty(attributeDescriptor).evaluateAttributeExpressions(flowFile).getValue();
                String hash = Hashing.md5().hashString(value, Charsets.UTF_8).toString();

                flowFile = session.putAttribute(flowFile, attributeDescriptor.getName(), hash);
                logger.debug("Successfully added attribute '{}' to {} with a value of {}; routing to success", new Object[]{attributeDescriptor.getName(), flowFile, hash});
                session.getProvenanceReporter().modifyAttributes(flowFile);

            }

            session.getProvenanceReporter().modifyAttributes(flowFile);
            session.transfer(flowFile, REL_SUCCESS);

        } catch (final ProcessException e) {
            logger.error("Failed to process {} due to {}; routing to failure", new Object[]{flowFile, e});
            session.transfer(flowFile, REL_FAILURE);
        } catch (final Exception e) {
            logger.error("Failed to process {} due to {}; routing to failure", new Object[]{flowFile, e});
            session.transfer(flowFile, REL_FAILURE);
        }
    }

}