/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asymmetrik.nifi.processors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SupportsBatching
@Tags({"asymmetrik", "batch", "wait", "fragment", "split"})
@CapabilityDescription("Counts the number of instances of a particular fragment of FlowFiles and transfers the last FlowFile when the batch is complete.")
@EventDriven
public class BatchWait extends AbstractProcessor {

    // TODO Consider moving this to non-volatile storage
    public static final Map<String, Long> FRAGMENT_COUNTERS = new ConcurrentHashMap<>();

    public static final Relationship COMPLETED = new Relationship.Builder()
            .name("batch-completed")
            .description("Passes the last FlowFile in a batch to this relationship.")
            .build();

    public static final Relationship ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Receives all processed FlowFiles")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFiles that do not have the Identifier or Count attributes will be routed to failure.")
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of FlowFiles to take from the incoming work queue")
            .defaultValue("50")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor FRAGMENT_IDENTIFIER = new PropertyDescriptor.Builder()
            .name("Identifier Attribute")
            .description("The FlowFile attribute used to identify the batch of the FlowFiles. " +
                    "Defaults to the attribute used by most Split* processors.")
            .defaultValue("fragment.identifier")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor FRAGMENT_COUNT = new PropertyDescriptor.Builder()
            .name("Count Attribute")
            .description("The FlowFile attribute used to identify the total count of FlowFiles " +
                    "in the batch. Defaults to the attribute used by most Split* processors.")
            .defaultValue("fragment.count")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    private int batchSize;
    private String fragmentIdentifierAttribute;
    private String fragmentCountAttribute;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        this.batchSize = context.getProperty(BATCH_SIZE).isSet() ? context.getProperty(BATCH_SIZE).asInteger() : 1;
        this.fragmentCountAttribute = context.getProperty(FRAGMENT_COUNT).getValue();
        this.fragmentIdentifierAttribute = context.getProperty(FRAGMENT_IDENTIFIER).getValue();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        List<FlowFile> incoming = session.get(batchSize);
        if (incoming.isEmpty()) {
            return;
        }

        // For memory efficiency, only create this object if we have completed FlowFiles
        List<FlowFile> completedFlowFiles = null;

        for(FlowFile ff : incoming) {
            // Update the in-memory map of counts for each FlowFile
            String fragmentIdentifier = ff.getAttribute(fragmentIdentifierAttribute);
            String fragmentCountString = ff.getAttribute(fragmentCountAttribute);

            if( fragmentIdentifier == null || fragmentCountString == null ) {
                getLogger().error("Invalid attributes for Identifier: [{}] or Count: [{}]",
                        new Object[]{fragmentIdentifier, fragmentCountString});
                session.transfer(session.clone(ff), FAILURE);
                continue;
            }

            Long fragmentCount;
            try {
                fragmentCount = Long.valueOf( fragmentCountString );
            } catch( NumberFormatException ex ) {
                getLogger().error("Count attribute not a number: [{}]",
                        new Object[]{fragmentCountString});
                session.transfer(session.clone(ff), FAILURE);
                continue;
            }

            FRAGMENT_COUNTERS.putIfAbsent(fragmentIdentifier, 0l);
            FRAGMENT_COUNTERS.compute(fragmentIdentifier, (key, value) -> value + 1l);

            /*
             * If we've reached the total count, send the FlowFile to the completed
             * relationship and remove the map entry
             */
            if( FRAGMENT_COUNTERS.get(fragmentIdentifier).equals(fragmentCount) ) {

                // Do we need to create the completed array list yet?
                if( completedFlowFiles == null ) {
                    completedFlowFiles = new ArrayList<>();
                }

                // Clone so that the original can be routed to ORIGINAL
                FlowFile lastFlowFile = session.clone(ff);

                completedFlowFiles.add(lastFlowFile);
                FRAGMENT_COUNTERS.remove(fragmentIdentifier);
            }
        }

        // If we've completed any FlowFiles, transfer them to the completed relationship
        if( completedFlowFiles != null ) {
            session.transfer(completedFlowFiles, COMPLETED);
        }

        session.transfer(incoming, ORIGINAL);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return ImmutableSet.of(COMPLETED, ORIGINAL, FAILURE);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(BATCH_SIZE, FRAGMENT_COUNT, FRAGMENT_IDENTIFIER);
    }
}
