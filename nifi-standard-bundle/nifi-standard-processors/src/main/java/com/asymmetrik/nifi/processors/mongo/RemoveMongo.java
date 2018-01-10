package com.asymmetrik.nifi.processors.mongo;

import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.WriteModel;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.bson.Document;

@SupportsBatching
@Tags({"asymmetrik", "mongo", "remove", "delete"})
@CapabilityDescription("Removes many documents from Mongo based on input JSON query criteria.")
public class RemoveMongo extends AbstractWriteMongoProcessor {

    @Override
    protected WriteModel<Document> getModelForDocument(Document document) {
        return new DeleteManyModel<>(document);
    }

}
