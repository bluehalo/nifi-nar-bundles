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
package com.asymmetrik.nifi.processors.mongo;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Sets;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.bson.Document;

import static com.asymmetrik.nifi.processors.mongo.MongoProps.*;

@SupportsBatching
@Tags({"asymmetrik", "mongo", "query"})
@CapabilityDescription("Performs mongo queries.")
public class QueryMongo extends AbstractMongoProcessor {

    protected static final Relationship REL_NO_RESULT_SUCCESS = new Relationship.Builder().name("noresult")
            .description("Query files that generate no results are transferred to this relationship").build();

    Integer limit;
    private List<PropertyDescriptor> props = Arrays.asList(MONGO_SERVICE, DATABASE, COLLECTION, QUERY, PROJECTION, SORT,
            LIMIT, WRITE_CONCERN);
    private PropertyValue queryProperty;
    private PropertyValue projectionProperty;
    private PropertyValue sortProperty;

    @Override
    protected void init(ProcessorInitializationContext context) {
        properties = Collections.unmodifiableList(props);
        relationships = Collections.unmodifiableSet(Sets.newHashSet(REL_SUCCESS, REL_FAILURE, REL_NO_RESULT_SUCCESS));
        clientId = getIdentifier();
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        queryProperty = context.getProperty(QUERY);
        projectionProperty = context.getProperty(PROJECTION);
        sortProperty = context.getProperty(SORT);

        limit = context.getProperty(LIMIT).isSet() ? context.getProperty(LIMIT).asInteger() : null;

        createMongoConnection(context);
        ensureIndexes(context, collection);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        ComponentLog logger = this.getLogger();

        // Evaluate expression language and create BSON Documents
        Document query = (queryProperty.isSet())
                ? Document.parse(queryProperty.evaluateAttributeExpressions(flowFile).getValue()) : null;
        Document projection = (projectionProperty.isSet())
                ? Document.parse(projectionProperty.evaluateAttributeExpressions(flowFile).getValue()) : null;
        Document sort = (sortProperty.isSet())
                ? Document.parse(sortProperty.evaluateAttributeExpressions(flowFile).getValue()) : null;

        try {
            FindIterable<Document> it = (query != null) ? collection.find(query) : collection.find();

            // Apply projection if needed
            if (projection != null) {
                it.projection(projection);
            }

            // Apply sort if needed
            if (sort != null) {
                it.sort(sort);
            }

            // Apply limit if set
            if (limit != null) {
                it.limit(limit.intValue());
            }

            // Iterate and create flowfile for each result
            final MongoCursor<Document> cursor = it.iterator();

            try {
                if (!cursor.hasNext()) {
                    FlowFile ff = session.clone(flowFile);
                    session.transfer(ff, REL_NO_RESULT_SUCCESS);
                } else {
                    while (cursor.hasNext()) {
                        // Create new flowfile with all parent attributes
                        FlowFile ff = session.clone(flowFile);
                        ff = session.write(ff, new OutputStreamCallback() {
                            @Override
                            public void process(OutputStream outputStream) throws IOException {
                                IOUtils.write(cursor.next().toJson(), outputStream);
                            }
                        });

                        session.transfer(ff, REL_SUCCESS);
                    }
                }
            } finally {
                cursor.close();
                session.remove(flowFile);
            }

        } catch (Exception e) {
            logger.error("Failed to execute query {} due to {}.", new Object[] { query, e }, e);
            flowFile = session.putAttribute(flowFile, "mongo.exception", e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
