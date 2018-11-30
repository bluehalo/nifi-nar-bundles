package com.asymmetrik.nifi.processors.mongo;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.bson.types.ObjectId;

@SideEffectFree
@Tags({"asymmetrik", "json", "mongo", "bson"})
@CapabilityDescription("Adds a new Mongo ID in the form of an ObjectId to attributes.")
public class CreateObjectId extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("the embedded payload")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files that could not be written to the output directory for some reason are transferred to this relationship")
            .build();

    public static final PropertyDescriptor ATTR_NAME = new PropertyDescriptor.Builder()
            .name("name")
            .description("The name of the of the attribute in which to store the newly created ObjectId")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SEED = new PropertyDescriptor.Builder()
            .name("seed")
            .description("The number of milliseconds since epoch to use as the ObjectId seed")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private String attrName;

    @Override
    protected void init(ProcessorInitializationContext context) {
        final List<PropertyDescriptor> props = Arrays.asList(ATTR_NAME, SEED);
        properties = Collections.unmodifiableList(props);

        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(rels);
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        attrName = context.getProperty(ATTR_NAME).evaluateAttributeExpressions().getValue().trim();
        ComponentLog logger = this.getLogger();
        logger.info("Generating object Id using attribute_name: {}", new Object[]{attrName});
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        ObjectId oid;
        PropertyValue seedProperty = context.getProperty(SEED);
        if (seedProperty.isSet()) {
            String seed = seedProperty.evaluateAttributeExpressions(flowFile).getValue();

            // Try converting to int
            try {
                oid = new ObjectId(new Date(Long.parseLong(seed)));
            } catch (NumberFormatException nfe) {
                oid = ObjectId.isValid(seed) ? new ObjectId(seed) : new ObjectId();
            }
        } else {
            oid = ObjectId.get();
        }

        flowFile = session.putAttribute(flowFile, attrName, oid.toString());
        session.getProvenanceReporter().modifyAttributes(flowFile);
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

}
