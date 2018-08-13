package com.asymmetrik.nifi.processors.mongo;

import com.google.common.collect.Sets;
import com.mongodb.client.MongoCursor;
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
import org.apache.nifi.processor.util.StandardValidators;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.Document;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.asymmetrik.nifi.processors.mongo.MongoProps.*;

@SupportsBatching
@Tags({"asymmetrik", "mongo", "distincy"})
@CapabilityDescription("Performs distinct mongo query")
public class DistinctMongo extends AbstractMongoProcessor {

    protected static final Relationship REL_NO_RESULT_SUCCESS = new Relationship.Builder().name("noresult")
            .description("Query files that generate no results are transferred to this relationship").build();

    private List<PropertyDescriptor> props = Arrays.asList(MONGO_SERVICE, DATABASE, COLLECTION, DISTINCT_FIELD, QUERY, WRITE_CONCERN);


    static final PropertyDescriptor DISTINCT_FIELD = new PropertyDescriptor.Builder()
            .displayName("Field")
            .name("Field")
            .description("The field for which to return distinct values.")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .required(true)
            .build();

    private PropertyValue queryProperty;
    private PropertyValue fieldProperty;

    private byte[] parseBsonValue(BsonValue val) {
        if(val.isDocument()) {
            return val.asDocument().toJson().getBytes(StandardCharsets.UTF_8);
        } else if(val.isString()) {
            return val.asString().getValue().getBytes(StandardCharsets.UTF_8);
        } else if(val.isNumber()) {
            return (Double.toString(val.asNumber().doubleValue())).getBytes(StandardCharsets.UTF_8);
        }  else if(val.isBoolean()) {
            return (Boolean.toString(val.asBoolean().getValue())).getBytes(StandardCharsets.UTF_8);
        }
        return null;
    }

    @Override
    protected void init(ProcessorInitializationContext context) {
        properties = Collections.unmodifiableList(props);
        relationships = Collections.unmodifiableSet(Sets.newHashSet(REL_SUCCESS, REL_FAILURE, REL_NO_RESULT_SUCCESS));
        clientId = getIdentifier();
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        queryProperty = context.getProperty(QUERY);
        fieldProperty = context.getProperty(DISTINCT_FIELD);


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

        String field = fieldProperty.evaluateAttributeExpressions(flowFile).getValue();

        try {
            final MongoCursor<BsonValue> cursor;

            if (queryProperty.isSet()) {
                Document query = Document.parse(queryProperty.evaluateAttributeExpressions(flowFile).getValue());
                cursor = this.collection.distinct(field, query, BsonValue.class).iterator();
            } else {
                cursor = this.collection.distinct(field, BsonValue.class).iterator();
            }

            try {
                if (!cursor.hasNext()) {
                    FlowFile ff = session.clone(flowFile);
                    session.transfer(ff, REL_NO_RESULT_SUCCESS);
                } else {
                    while (cursor.hasNext()) {
                        // Create new flowfile with all parent attribute
                        FlowFile ff = session.clone(flowFile);
                        BsonValue res = cursor.next();
                        ff = session.write(ff, out -> out.write(parseBsonValue(res)));
                        session.transfer(ff, REL_SUCCESS);
                    }
                }
            } finally {
                cursor.close();
                session.remove(flowFile);
            }

        } catch (Exception e) {
            logger.error("Failed to execute distinct on field {} due to {}.", new Object[] { field, e }, e);
            flowFile = session.putAttribute(flowFile, "mongo.exception", e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
