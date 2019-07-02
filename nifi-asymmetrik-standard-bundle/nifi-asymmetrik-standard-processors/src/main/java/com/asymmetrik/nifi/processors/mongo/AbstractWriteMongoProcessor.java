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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.util.JSON;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.bson.Document;

import java.util.*;

/**
 * Performs a bulk write to Mongo based on the input FlowFiles
 */
public abstract class AbstractWriteMongoProcessor extends AbstractMongoProcessor {

    protected BulkWriteOptions writeOptions;

    protected abstract WriteModel<Document> getModelForDocument(Document document);

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        final List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles == null) {
            return;
        }

        ComponentLog logger = this.getLogger();

        final String source = context.getProperty(INSERT_COMMAND_SOURCE).getValue();

        List<WriteModel<Document>> documentsToWrite = new ArrayList<>(flowFiles.size());

        /*
         * Collect FlowFiles that are marked for bulk write. Matches same
         * index as documentsToWrite
         */
        List<FlowFile> flowFilesAttemptedWrite = new ArrayList<>();

        logger.debug("Attempting to batch write {} FlowFiles", new Object[]{flowFiles.size()});
        for (FlowFile flowFile : flowFiles) {

            final String payload;

            try {
                switch (source) {
                    case "content":
                        final String[] result = new String[1];
                        session.read(flowFile, (in) -> result[0] = IOUtils.toString(in));
                        payload = result[0];
                        break;
                    case "attribute":
                        String command = context.getProperty(INSERT_COMMAND_ATTRIBUTE).evaluateAttributeExpressions(flowFile).getValue();
                        payload = flowFile.getAttribute(command);
                        break;
                    default:
                        throw new Exception("Invalid source choice: " + source);
                }

                BasicDBObject parse = (BasicDBObject) JSON.parse(payload);
                Document documentToWrite = new Document(parse.toMap());
                logger.debug("Creating WriteModel with Document {}", new Object[]{documentToWrite});

                WriteModel<Document> writeDoc = getModelForDocument(documentToWrite);
                documentsToWrite.add(writeDoc);

            } catch (Exception e) {
                /*
                 * If any FlowFiles error on translation to a Mongo Object, they were not added to
                 * the documentsToWrite, so route to failure immediately
                 */
                logger.error("Encountered exception while processing FlowFile for Mongo Storage. Routing to failure and continuing.", e);
                FlowFile failureFlowFile = session.putAttribute(flowFile, "mongo.exception", e.getMessage());
                session.transfer(failureFlowFile, REL_FAILURE);
                continue;
            }

            // add to the ordered list so we can determine which fail on bulk write
            flowFilesAttemptedWrite.add(flowFile);
        }

        /*
         * Perform the bulk write if any documents are there to write
         */
        if (!documentsToWrite.isEmpty()) {
            logger.debug("Attempting to bulk write {} documents", new Object[]{documentsToWrite.size()});
            Map<Integer, BulkWriteError> writeErrors = executeBulkWrite(documentsToWrite);

            /*
             * Route FlowFiles to the proper relationship based on the returned
             * errors
             */
            logger.debug("Evaluating FlowFile routing against {} Write Errors for {} FlowFiles", new Object[]{writeErrors.size(), flowFilesAttemptedWrite.size()});
            transferFlowFiles(session, flowFilesAttemptedWrite, writeErrors);
        }
    }

    private static final List<AllowableValue> insertCommandChoices = ImmutableList.of(
            new AllowableValue("content", "flowfile-content", "retrieves insert command from flowfile content"),
            new AllowableValue("attribute", "flowfile-attribute", "retrieves insert command from specified flowfile attribute")
    );

    public static final PropertyDescriptor INSERT_COMMAND_SOURCE = new PropertyDescriptor.Builder()
            .name("Insert command source")
            .description("Indicates whether the source of the insert command is retrieved from flowfile attribute or content")
            .allowableValues(insertCommandChoices.toArray(new AllowableValue[insertCommandChoices.size()]))
            .defaultValue(insertCommandChoices.get(0).getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor INSERT_COMMAND_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("Insert command attribute")
            .description("The attribute evaluated to retrieve the json insert command. Only applicable if 'Insert command source' is 'flowfile-attribute'")
            .required(false)
            .addValidator((subject, value, context) -> (new ValidationResult.Builder()).subject(subject).input(value).explanation("always valid").valid(true).build())
            .expressionLanguageSupported(true)
            .build();

    private List<PropertyDescriptor> props = Arrays.asList(INSERT_COMMAND_SOURCE, INSERT_COMMAND_ATTRIBUTE,
            MongoProps.MONGO_SERVICE, MongoProps.DATABASE, MongoProps.COLLECTION, MongoProps.WRITE_CONCERN,
            MongoProps.INDEX, MongoProps.BATCH_SIZE, MongoProps.ORDERED);

    @Override
    protected void init(ProcessorInitializationContext context) {
        properties = Collections.unmodifiableList(props);
        relationships = Collections.unmodifiableSet(Sets.newHashSet(REL_SUCCESS, REL_FAILURE));
        clientId = getIdentifier();
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        batchSize = context.getProperty(MongoProps.BATCH_SIZE).asInteger();
        boolean ordered = context.getProperty(MongoProps.ORDERED).asBoolean();
        writeOptions = new BulkWriteOptions().ordered(ordered);

        createMongoConnection(context);
        ensureIndexes(context, collection);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        List<ValidationResult> results = new ArrayList<>();

        if (validationContext.getProperty(INSERT_COMMAND_SOURCE).getValue().equals("attribute")) {
            results.add(StandardValidators.NON_BLANK_VALIDATOR.validate(
                    INSERT_COMMAND_ATTRIBUTE.getName(),
                    validationContext.getProperty(INSERT_COMMAND_ATTRIBUTE).getValue(),
                    validationContext));
        }

        return results;
    }

    protected Map<Integer, BulkWriteError> executeBulkWrite(List<? extends WriteModel<Document>> documentsToWrite) {
        // mapping of array indices for flow file errors
        Map<Integer, BulkWriteError> writeErrors = new HashMap<>();
        try {
            this.collection.bulkWrite(documentsToWrite, this.writeOptions);
        } catch (MongoBulkWriteException e) {
            List<BulkWriteError> errors = e.getWriteErrors();
            for (BulkWriteError docError : errors) {
                writeErrors.put(docError.getIndex(), docError);
            }
            getLogger().warn("Error occurred during bulk write", e);
        }
        return writeErrors;
    }

    protected void transferFlowFiles(ProcessSession session, List<FlowFile> flowFilesAttempted, Map<Integer, BulkWriteError> writeErrors) {

        ComponentLog logger = this.getLogger();

        if (!writeErrors.isEmpty()) {
            logger.debug("Encountered errors on write");
            /*
             * For each Bulk Deleted Document, see if it encountered an error.
             * If it had an error (based on index in the list), add the Mongo
             * Error to the FlowFile attribute and route to Failure. Otherwise,
             * route to Success
             */
            int numFlowfiles = flowFilesAttempted.size();
            for (int i = 0; i < numFlowfiles; i++) {
                FlowFile ff = flowFilesAttempted.get(i);
                if (writeErrors.containsKey(i)) {

                    logger.debug("Found error for FlowFile index {}", new Object[]{i});

                    // Add the error information to the FlowFileAttributes, and
                    // route to failure
                    BulkWriteError bwe = writeErrors.get(i);

                    logger.debug("FlowFile ID {} had Error Code {} and Message {}", new Object[]{ff.getId(), bwe.getCode(), bwe.getMessage()});

                    Map<String, String> failureAttributes = getAttributesForWriteFailure(bwe);
                    ff = session.putAllAttributes(ff, failureAttributes);

                    session.transfer(ff, REL_FAILURE);

                    // If ordered=true, mongo will stop processing delete attempts after the first failure in a batch
                    if (writeOptions.isOrdered()) {
                        logger.debug("Routing all flowfiles after FlowFile ID {} with Index {} to Failure because an error occurred and ordered=true",
                                new Object[]{ff.getId(), i});
                        for (int j = i + 1; j < numFlowfiles; j++) {
                            ff = flowFilesAttempted.get(j);
                            ff = session.putAttribute(ff, "writemongo.error", "Write not attempted because there was a failure earlier in batch and ordered=true");
                            session.transfer(ff, REL_FAILURE);
                        }
                        break;
                    }
                } else {
                    logger.debug("Routing FlowFile ID {} with Index {} to Success", new Object[]{ff.getId(), i});
                    // Flow File did not have error, so route to success
                    session.transfer(ff, REL_SUCCESS);
                }
            }
        } else {
            logger.debug("No errors encountered on bulk write, so routing all to success");
            // All succeeded, so write all to success
            session.transfer(flowFilesAttempted, REL_SUCCESS);
        }
    }

    protected Map<String, String> getAttributesForWriteFailure(BulkWriteError bwe) {
        Map<String, String> failureAttributes = new HashMap<>();
        failureAttributes.put("mongo.errorcode", String.valueOf(bwe.getCode()));
        failureAttributes.put("mongo.errormessage", bwe.getMessage());
        return failureAttributes;
    }

}
