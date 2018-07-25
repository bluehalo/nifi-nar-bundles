package com.asymmetrik.nifi.services.mongo;

import com.mongodb.MongoClient;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;

@Tags({"asymmetrik", "mongo", "database", "connection"})
@CapabilityDescription("Provides Mongo Database Client Service.")
public interface MongoClientService extends ControllerService {
    MongoClient getMongoClient();
}
