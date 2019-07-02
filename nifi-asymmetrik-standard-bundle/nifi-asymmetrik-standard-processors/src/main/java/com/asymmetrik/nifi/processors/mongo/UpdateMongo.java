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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.util.JSON;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.nifi.processor.io.InputStreamCallback;
import org.bson.Document;
import org.bson.types.ObjectId;

@SupportsBatching
@Tags({"asymmetrik", "egress", "mongo", "store", "update"})
@CapabilityDescription("Performs mongo updates of JSON/BSON.")
public class UpdateMongo extends AbstractMongoProcessor {
    private static final Configuration JSON_PROVIDER_CONFIGURATION = Configuration.builder().jsonProvider(new GsonJsonProvider()).build();
    private static final JsonProvider JSON_PROVIDER = JSON_PROVIDER_CONFIGURATION.jsonProvider();

    private List<PropertyDescriptor> props = Arrays.asList(MongoProps.MONGO_SERVICE, MongoProps.DATABASE, MongoProps.COLLECTION, MongoProps.UPDATE_OPERATOR, MongoProps.UPDATE_QUERY_KEYS, MongoProps.UPDATE_KEYS, MongoProps.UPSERT, MongoProps.WRITE_CONCERN, MongoProps.BATCH_SIZE);

    private String updateOperator;
    private Set<String> updateQueryKeys;
    private UpdateOptions updateOptions;

    private PropertyValue updateKeysProp;

    @Override
    protected void init(ProcessorInitializationContext context) {
        properties = Collections.unmodifiableList(props);
        relationships = Collections.unmodifiableSet(Sets.newHashSet(REL_SUCCESS, REL_FAILURE));
        clientId = getIdentifier();
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        batchSize = context.getProperty(MongoProps.BATCH_SIZE).asInteger();
        updateOperator = context.getProperty(MongoProps.UPDATE_OPERATOR).getValue();
        updateQueryKeys = getSetFromCsvList(context.getProperty(MongoProps.UPDATE_QUERY_KEYS).getValue());

        updateKeysProp = context.getProperty(MongoProps.UPDATE_KEYS);

        boolean upsert = context.getProperty(MongoProps.UPSERT).asBoolean();
        updateOptions = new UpdateOptions().upsert(upsert);

        createMongoConnection(context);
        ensureIndexes(context, collection);
    }

    private Set<String> getSetFromCsvList(String csv) {
        Set<String> set = Sets.newHashSet();
        for (String value : StringUtils.split(csv, ",")) {
            set.add(value.trim());
        }
        return set;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) {

        final List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles == null) {
            return;
        }

        ComponentLog logger = this.getLogger();

        List<UpdateManyModel<Document>> documentsToUpdate = new ArrayList<>(flowFiles.size());

        /*
         * Collect FlowFiles that are marked for bulk update. Matches same
         * index as documentsUpdate
         */
        List<FlowFile> flowFilesAttemptedUpdate = new ArrayList<>();

        logger.debug("Attempting to batch update {} FlowFiles", new Object[]{flowFiles.size()});
        for (FlowFile flowFile : flowFiles) {
            Set<String> updateKeys = getSetFromCsvList(updateKeysProp.evaluateAttributeExpressions(flowFile).getValue());

            StringStreamCallback stringStreamCallback = new StringStreamCallback();
            session.read(flowFile, stringStreamCallback);

            try {
                String payload = stringStreamCallback.getText();
                logger.debug("Found payload {}", new Object[]{payload});

                // Parse the payload in order to extract the query key and the key-value pairs to update
                final JsonObject json = (JsonObject) JSON_PROVIDER.parse(payload);

                Document queryDocument = buildQuery(json);

                BasicDBObject keysToUpdate = null;
                for (String key : updateKeys) {
                    // Extract the value for this key
                    JsonElement element = JsonUtil.extractElement(json, key);

                    if (!JsonUtil.isNull(element)) {
                        // Initialize the BasicDBObject if this the first valid key
                        if (keysToUpdate == null) {
                            keysToUpdate = new BasicDBObject();
                        }

                        keysToUpdate.append(key, JSON.parse(element.toString()));
                    }
                }

                if (queryDocument != null && keysToUpdate != null) {
                    Document documentToUpdate = new Document(this.updateOperator, keysToUpdate);
                    logger.debug("Creating UpdateManyModel with Query {} and Document {}", new Object[]{queryDocument, documentToUpdate});

                    UpdateManyModel<Document> iom = new UpdateManyModel<>(queryDocument, documentToUpdate, updateOptions);
                    documentsToUpdate.add(iom);

                    // add to the ordered list so we can determine which fail on bulk
                    // write
                    flowFilesAttemptedUpdate.add(flowFile);
                } else {
                    logger.error("Insufficient information found in payload, update aborted");
                    session.transfer(flowFile, REL_FAILURE);
                }

            } catch (Exception e) {
                /*
                 * If any FlowFiles error on translation to a Mongo Object, they
                 * were not added to the documentsToUpdate, so route to failure
                 * immediately
                 */
                logger.error("Encountered exception while processing FlowFile for Mongo Storage. Routing to failure and continuing.", e);
                FlowFile failureFlowFile = session.putAttribute(flowFile, "mongo.exception", e.getMessage());
                session.transfer(failureFlowFile, REL_FAILURE);
                continue;
            }

        }

        /*
         * Perform the bulk updates if any documents are there to update
         */
        if (!documentsToUpdate.isEmpty()) {
            logger.debug("Attempting to bulk update {} documents", new Object[]{documentsToUpdate.size()});
            Map<Integer, BulkWriteError> writeErrors = executeBulkUpdate(documentsToUpdate);

            /*
             * Route FlowFiles to the proper relationship based on the returned
             * errors
             */
            logger.debug("Evaluating FlowFile routing against {} Write Errors for {} FlowFiles", new Object[]{writeErrors.size(), flowFilesAttemptedUpdate.size()});
            transferFlowFiles(session, flowFilesAttemptedUpdate, writeErrors);
        }

    }

    /*
     * Builds the query document based on the specified query key. If the
     * query key is an oid, construct an ObjectId, otherwise use the
     * value of the query key as-is.
     */
    private Document buildQuery(final JsonObject jsonPayload) {
        Document queryDocument = null;

        for (String key : updateQueryKeys) {
            JsonElement element = JsonUtil.extractElement(jsonPayload, key);
            if (!JsonUtil.isNull(element)) {
                if (queryDocument == null) {
                    queryDocument = new Document();
                }
                // Try to create typed object
                Object obj = getJsonElementAsObject(element);
                if (obj != null) {
                    queryDocument.append(key, obj);
                } else {
                    // Query key is of unknown type, use as-is
                    queryDocument.append(key, element.toString().replace("\"", ""));
                }
            }
        }
        return queryDocument;
    }

    private Object getJsonElementAsObject(JsonElement json) {
        JsonElement oid = JsonUtil.extractElement(json, "$oid");
        if (!JsonUtil.isNull(oid)) {
            return new ObjectId(oid.toString().replaceAll("\"", ""));
        }
        JsonElement date = JsonUtil.extractElement(json, "$date");
        if (!JsonUtil.isNull(date)) {
            return new Date(Long.parseLong(date.toString().replaceAll("\"", "")));
        }
        return null;
    }

    protected Map<Integer, BulkWriteError> executeBulkUpdate(List<UpdateManyModel<Document>> documentsToUpdate) {
        // mapping of array indices for flow file errors
        Map<Integer, BulkWriteError> writeErrors = new HashMap<>();
        try {
            collection.bulkWrite(documentsToUpdate);
        } catch (MongoBulkWriteException e) {
            List<BulkWriteError> errors = e.getWriteErrors();
            for (BulkWriteError docError : errors) {
                writeErrors.put(docError.getIndex(), docError);
            }
            getLogger().warn("Error occurred during bulk write", e);
        }
        return writeErrors;
    }

    protected void transferFlowFiles(ProcessSession session, List<FlowFile> flowFilesAttemptedUpdate, Map<Integer, BulkWriteError> writeErrors) {

        ComponentLog logger = this.getLogger();

        if (!writeErrors.isEmpty()) {
            logger.debug("Encountered errors on write");
            /*
             * For each Bulk Updated Document, see if it encountered an error.
             * If it had an error (based on index in the list), add the Mongo
             * Error to the FlowFile attribute and route to Failure. Otherwise,
             * route to Success
             */
            for (int i = 0; i < flowFilesAttemptedUpdate.size(); i++) {
                FlowFile ff = flowFilesAttemptedUpdate.get(i);
                if (writeErrors.containsKey(i)) {

                    logger.debug("Found error for FlowFile index {}", new Object[]{i});

                    // Add the error information to the FlowFileAttributes, and
                    // route to failure
                    BulkWriteError bwe = writeErrors.get(i);

                    logger.debug("FlowFile ID {} had Error Code {} and Message {}", new Object[]{ff.getId(), bwe.getCode(), bwe.getMessage()});

                    Map<String, String> failureAttributes = getAttributesForWriteFailure(bwe);
                    ff = session.putAllAttributes(ff, failureAttributes);

                    session.transfer(ff, REL_FAILURE);
                } else {
                    logger.debug("Routing FlowFile ID {} with Index {} to Success", new Object[]{ff.getId(), i});
                    // Flow File did not have error, so route to success
                    session.transfer(ff, REL_SUCCESS);
                }
            }
        } else {
            logger.debug("No errors encountered on bulk write, so routing all to success");
            // All succeeded, so write all to success
            session.transfer(flowFilesAttemptedUpdate, REL_SUCCESS);
        }
    }

    protected Map<String, String> getAttributesForWriteFailure(BulkWriteError bwe) {
        Map<String, String> failureAttributes = new HashMap<>();
        failureAttributes.put("mongo.errorcode", String.valueOf(bwe.getCode()));
        failureAttributes.put("mongo.errormessage", bwe.getMessage());
        return failureAttributes;
    }


    private static class StringStreamCallback implements InputStreamCallback {

        private String text;

        @Override
        public void process(InputStream inputStream) throws IOException {
            text = IOUtils.toString(inputStream);
        }

        public String getText() {
            return text;
        }
    }
}
