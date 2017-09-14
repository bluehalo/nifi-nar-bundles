package com.asymmetrik.nifi.processors;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@SupportsBatching
@SideEffectFree
@Tags({"asymmetrik", "terminate", "sink"})
@CapabilityDescription("Commits every flowfile, essentially terminating the flow.")
public class AutoTerminator extends AbstractProcessor {

    /**
     * Relationships
     */
    public static final Relationship REL_NOWHERE = new Relationship.Builder()
            .name("nowhere")
            .description("Nothing is sent to this relationship. It is defined to address a bug in NiFi 1.x. In particular," +
                    " with NiFi 1.0.0, templates cannot reimport processors with no relationships")
            .autoTerminateDefault(true)
            .build();

    /**
     * Property Descriptors
     */
    static final PropertyDescriptor BULK = new PropertyDescriptor.Builder()
            .name("Bulk Size")
            .description("The number of flowfiles to terminate on each trigger. This value can be increased to " +
                    "improve performance when flowfile velocity is high.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .build();

    private Integer batchSize;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        batchSize = context.getProperty(BULK).asInteger();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) {
        session.remove(session.get(batchSize));
        session.commit();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.unmodifiableList(Collections.singletonList(BULK));
    }

    @Override
    public Set<Relationship> getRelationships() {
        Set<Relationship> relationships = new HashSet<>(1);
        relationships.add(REL_NOWHERE);
        return Collections.unmodifiableSet(relationships);
    }
}
