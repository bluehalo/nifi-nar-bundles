package com.asymmetrik.nifi.processors.mongo;

import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.bson.Document;

@SupportsBatching
@Tags({"asymmetrik", "egress", "mongo", "store", "insert"})
@CapabilityDescription("Performs a mongo inserts of JSON/BSON.")
public class StoreInMongo extends AbstractWriteMongoProcessor {

    @Override
    protected WriteModel<Document> getModelForDocument(Document document) {
        return new InsertOneModel<>(document);
    }

}
