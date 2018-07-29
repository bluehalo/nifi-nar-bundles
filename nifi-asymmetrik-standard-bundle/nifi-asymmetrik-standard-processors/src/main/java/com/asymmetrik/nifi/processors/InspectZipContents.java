package com.asymmetrik.nifi.processors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.stream.io.StreamUtils;

@SuppressWarnings("Duplicates")
@EventDriven
@SupportsBatching
@SideEffectFree
@Tags({"asymmetrik", "zip", "content", "route"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@WritesAttribute(attribute = "inspect_zip_entry.first", description = "The name of the first entry in the zip file.")
@CapabilityDescription("Creates an attribute value from the name of the first entry in the zip file.")
public class InspectZipContents extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successful inspections are transferred to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files that could not be processed are transferred to this relationship")
            .build();
    static final String ATTR = "inspect_zip_entry.first";
    private static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("The Character Set in which the data is encoded")
            .required(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(CHARACTER_SET);
        return Collections.unmodifiableList(descriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        return Collections.unmodifiableSet(rels);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final byte[] buffer = new byte[(int) flowFile.getSize()];
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(buffer)) {
            session.read(flowFile, in -> StreamUtils.fillBuffer(in, buffer));

            try (ZipInputStream zipInputStream = new ZipInputStream(byteArrayInputStream)) {
                // Check the first entry.
                ZipEntry zipEntry = zipInputStream.getNextEntry();

                String name = "";
                if (zipEntry != null) {
                    name = zipEntry.getName();
                }
                flowFile = session.putAttribute(flowFile, ATTR, name);
                session.getProvenanceReporter().modifyAttributes(flowFile);
                session.transfer(flowFile, REL_SUCCESS);
            }

        } catch (Exception e) {
            getLogger().error("Unable to update flowFile {} due to {}", new Object[]{flowFile, e.getMessage()}, e);
            session.transfer(flowFile, REL_FAILURE);
        }

    }
}
