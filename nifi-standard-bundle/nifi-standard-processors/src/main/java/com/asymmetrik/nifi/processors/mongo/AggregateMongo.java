package com.asymmetrik.nifi.processors.mongo;

import com.google.common.collect.Sets;
import com.mongodb.Block;
import com.mongodb.DBObject;
import com.mongodb.client.AggregateIterable;
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
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@SupportsBatching
@Tags({"asymmetrik", "mongo", "query", "aggregate"})
@CapabilityDescription("Executes Mongo aggregation pipelines")
public class AggregateMongo extends AbstractMongoProcessor {

    public static final PropertyDescriptor AGGREGATION_PIPELINE = new PropertyDescriptor.Builder()
            .name("aggregate-pipeline")
            .displayName("Aggregation Pipeline")
            .description("The aggregation pipeline to execute")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue("[]")
            .required(true)
            .build();

    public static final PropertyDescriptor OUTPUT_COLLECTION = new PropertyDescriptor.Builder()
            .name("output-collection")
            .displayName("Output Collection")
            .description("The collection to receive the output results")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private List<PropertyDescriptor> props = Arrays.asList(MongoProps.MONGO_SERVICE, MongoProps.DATABASE,
            MongoProps.COLLECTION, MongoProps.WRITE_CONCERN,
            AGGREGATION_PIPELINE, OUTPUT_COLLECTION);

    protected static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Aggregation pipelines that succeed will be routed to this relationship.")
            .build();

    private PropertyValue pipelineProperty;
    private PropertyValue outputCollectionProperty;

    @Override
    protected void init(ProcessorInitializationContext context) {
        properties = Collections.unmodifiableList(props);
        relationships = Collections.unmodifiableSet(Sets.newHashSet(REL_SUCCESS, REL_FAILURE, REL_ORIGINAL));
        clientId = getIdentifier();
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        pipelineProperty = context.getProperty(AGGREGATION_PIPELINE);
        outputCollectionProperty = context.getProperty(OUTPUT_COLLECTION);

        createMongoConnection(context);
        ensureIndexes(context, collection);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if(flowFile == null) {
            session.remove(flowFile);
        }

        try {
            String pipelineString = pipelineProperty.evaluateAttributeExpressions(flowFile).getValue();

            List<DBObject> pipeline = (List<DBObject>) JSON.parse(pipelineString);
            List<Document> stages = new ArrayList<>(pipeline.size());
            for(DBObject p : pipeline) {
                stages.add(new Document(p.toMap()));
            }

            String outputCollection = outputCollectionProperty.evaluateAttributeExpressions(flowFile).getValue();
            boolean hasOutputCollection = StringUtils.isNotBlank(outputCollection);
            if(hasOutputCollection) {
                stages.add(new Document("$out", outputCollection));
            }

            Block<Document> transferBlock = document -> {
                FlowFile ff = session.clone(flowFile);
                ff = session.write(ff, outputStream -> IOUtils.write(document.toJson(), outputStream));
                session.transfer(ff, REL_SUCCESS);
            };

            AggregateIterable<Document> agg = collection.aggregate(stages);
            if(hasOutputCollection) {
                agg.toCollection();
            }
            else {
                agg.forEach(transferBlock);
            }

            session.transfer(flowFile, REL_ORIGINAL);

        } catch (Exception ex) {
            getLogger().error("Failed to run aggregation pipeline", ex);
            session.transfer(flowFile, REL_FAILURE);
        }

    }
}
