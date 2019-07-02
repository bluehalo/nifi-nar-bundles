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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import com.asymmetrik.nifi.services.cache.SetCacheService;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@EventDriven
@SupportsBatching
@Tags({"asymmetrik", "duplicate", "dedupe"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Caches a value, computed from FlowFile attributes, for each incoming FlowFile and determines if the cached value has already been seen. "
        + "If so, routes the FlowFile to 'duplicate'. otherwise routes the FlowFile to 'non-duplicate'")
@SeeAlso(classNames = {
        "com.asymmetrik.nifi.standard.services.cache.SetCacheService",
        "com.asymmetrik.nifi.standard.services.cache.local.LocalCache",
        "com.asymmetrik.nifi.standard.services.cache.distributed.DistributedCacheClient"
})
public class IdentifyDuplicate extends AbstractProcessor {

    public static final PropertyDescriptor CACHE_SERVICE = new PropertyDescriptor.Builder()
            .name("Cache Service")
            .description("The Controller Service that is used to cache unique identifiers, used to determine duplicates")
            .required(true)
            .identifiesControllerService(SetCacheService.class)
            .build();
    public static final PropertyDescriptor CACHE_ENTRY_IDENTIFIER = new PropertyDescriptor.Builder()
            .name("Cache Entry Identifier")
            .description(
                    "A FlowFile attribute, or the results of an Attribute Expression Language statement, which will be evaluated "
                            + "against a FlowFile in order to determine the value used to identify duplicates; it is this value that is cached")
            .required(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(ResultType.STRING, true))
            .defaultValue("${id}")
            .expressionLanguageSupported(true)
            .build();
    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of flowfiles to take from the incoming work queue.")
            .defaultValue("1")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .required(true)
            .build();

    public static final Relationship REL_DUPLICATE = new Relationship.Builder()
            .name("duplicate")
            .description("If a FlowFile has been detected to be a duplicate, it will be routed to this relationship")
            .build();
    public static final Relationship REL_NON_DUPLICATE = new Relationship.Builder()
            .name("non-duplicate")
            .description("If a FlowFile's Cache Entry Identifier was not found in the cache, it will be routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If unable to communicate with the cache, the FlowFile will be penalized and routed to this relationship")
            .build();

    private final Set<Relationship> relationships;

    private SetCacheService<String> cacheService;
    private int batchSize;

    public IdentifyDuplicate() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_DUPLICATE);
        rels.add(REL_NON_DUPLICATE);
        rels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(rels);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(CACHE_SERVICE);
        descriptors.add(CACHE_ENTRY_IDENTIFIER);
        descriptors.add(BATCH_SIZE);
        return descriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void initialize(final ProcessContext context) {
        this.cacheService = context.getProperty(CACHE_SERVICE).asControllerService(SetCacheService.class);
        this.batchSize = context.getProperty(BATCH_SIZE).asInteger();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles.isEmpty()) {
            return;
        }

        List<FlowFile> nonDuplicates = new ArrayList<>();
        List<FlowFile> duplicates = new ArrayList<>();
        List<FlowFile> failures = new ArrayList<>();

        // create List of keys for each flowFile, removing those without keys
        ListIterator<FlowFile> flowFilesLi = flowFiles.listIterator();
        List<String> keys = new ArrayList<>();
        while (flowFilesLi.hasNext()) {
            FlowFile flowFile = flowFilesLi.next();
            String cacheKey = context.getProperty(CACHE_ENTRY_IDENTIFIER).evaluateAttributeExpressions(flowFile).getValue();
            if (StringUtils.isNotBlank(cacheKey)) {
                keys.add(cacheKey);
            } else {
                // remove flowfiles without cache keys, and add them to failures list
                failures.add(flowFile);
                flowFilesLi.remove();
            }
        }

        // call the cache service
        List<Boolean> addedList = cacheService.addIfAbsent(keys);

        // process results of cache service: true indicates element was added to the cache (i.e. non-duplicate)
        Iterator<FlowFile> flowFilesIt = flowFiles.iterator();
        Iterator<Boolean> addedIt = addedList.iterator();
        while (flowFilesIt.hasNext() && addedIt.hasNext()) {
            if (addedIt.next().booleanValue()) {
                nonDuplicates.add(flowFilesIt.next());
            } else {
                duplicates.add(flowFilesIt.next());
            }
        }

        routeFiles(session, nonDuplicates, REL_NON_DUPLICATE);
        routeFiles(session, duplicates, REL_DUPLICATE);
        routeFiles(session, failures, REL_FAILURE);
    }

    private void routeFiles(final ProcessSession session, final List<FlowFile> files, final Relationship relationship) {
        if (!files.isEmpty()) {
            session.transfer(files, relationship);
            getLogger().debug("Routing {} files to {}", new Object[]{files.size(), relationship.getName()});
        }
    }
}
