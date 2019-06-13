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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.asymmetrik.nifi.services.mongo.MongoClientService;
import com.google.common.collect.Maps;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.util.JSON;

import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.bson.Document;
import org.bson.conversions.Bson;

import static com.asymmetrik.nifi.processors.mongo.MongoProps.*;

public abstract class AbstractMongoProcessor extends AbstractProcessor {
    /**
     * Prefix to use for indexes created according to this processor's configuration.
     */
    public static final String INDEX_NAMESPACE_PREFIX = "nifi_";
    /**
     * Relationship Definitions
     */
    protected static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Files that could not be stored in MongoDB are transferred to this relationship").build();
    protected static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Files successfully written to MongoDB are transferred to this relationship").build();
    protected Set<Relationship> relationships;
    protected List<PropertyDescriptor> properties;

    protected String collectionName;
    protected String databaseName;
    protected String clientId;

    protected MongoClientService clientService;
    protected MongoClient client;
    protected MongoDatabase database;
    protected MongoCollection<Document> collection;

    protected Integer batchSize = 0;
    Map<String, BasicDBList> indexes = Maps.newHashMap();

    public static Set<String> getIndexNames(MongoCollection<Document> collection) {
        ListIndexesIterable<Document> currentIndexes = collection.listIndexes();
        MongoCursor<Document> cursor = currentIndexes.iterator();
        Set<String> indexNames = new HashSet<>();
        while (cursor.hasNext()) {
            Object next = cursor.next();
            String name = ((Document) next).get("name").toString();
            indexNames.add(name);
        }
        return indexNames;
    }

    /**
     * Generate normalized index name within NiFi's namespace
     */
    public static String normalizeIndexName(String indexJson) {
        return normalizeIndexName(INDEX_NAMESPACE_PREFIX, indexJson);
    }

    /**
     * Generate normalized index name with a prefix (optionally empty string) for namespacing.
     */
    public static String normalizeIndexName(String prefix, String indexJson) {
        return prefix + indexJson.replaceAll("[ {}\"']", "").replaceAll("[,:]", "_").replace("[", "").replace("]", "");
    }

    protected void createMongoConnection(ProcessContext context) {
        // Get processor properties
        databaseName = context.getProperty(DATABASE).evaluateAttributeExpressions().getValue();
        collectionName = context.getProperty(COLLECTION).evaluateAttributeExpressions().getValue();

        clientService = context.getProperty(MONGO_SERVICE).asControllerService(MongoClientService.class);
        client = clientService.getMongoClient();
        database = client.getDatabase(databaseName);
        collection = database.getCollection(collectionName).withWriteConcern(determineWriteConcern(context.getProperty(WRITE_CONCERN)));
    }

    protected boolean isIndexProperty(String propertyName) {
        return propertyName != null && propertyName.toLowerCase().contains("index");
    }

    protected void ensureIndexes(ProcessContext context, MongoCollection<Document> collection) {

        if (context.getProperty(INDEX).isSet()) {
            String value = context.getProperty(INDEX).getValue();
            String key = normalizeIndexName(value);
            indexes.put(key, (BasicDBList) JSON.parse(value));
        }

        for (Map.Entry<PropertyDescriptor, String> prop : context.getProperties().entrySet()) {

            PropertyDescriptor property = prop.getKey();
            if (!property.isDynamic())
                continue;

            // Add dynamic indexes
            PropertyValue propertyValue = context.getProperty(property);
            if (isIndexProperty(property.getName()) && propertyValue.isSet()) {
                String value = propertyValue.getValue();
                String key = normalizeIndexName(value);
                indexes.put(key, (BasicDBList) JSON.parse(value));
            }
        }

        Set<String> indexNames = getIndexNames(collection);

        // Check for stale indexes within nifi's namespace
        for (String name : indexNames) {
            if (name.startsWith(INDEX_NAMESPACE_PREFIX) && !indexes.containsKey(name)) {
                collection.dropIndex(name);
            }
        }

        // Add new indexes
        for (Map.Entry<String, BasicDBList> index : indexes.entrySet()) {
            String name = index.getKey();
            if (!indexNames.contains(name)) {
                createIndex(name, index.getValue(), collection);
            }
        }
    }

    protected void createIndex(String name, BasicDBList dbList, MongoCollection<Document> collection) {
        if (dbList.isEmpty() || name == null) {
            return;
        }

        BasicDBObject index = (BasicDBObject) dbList.get(0);
        IndexOptions options = new IndexOptions().name(name);
        if (dbList.size() > 1) {
            BasicDBObject opts = (BasicDBObject) dbList.get(1);
            if (opts.get("unique") != null && opts.get("unique") instanceof Boolean) {
                options = options.unique(opts.getBoolean("unique"));
            }
            if (opts.get("expireAfterSeconds") != null && opts.get("expireAfterSeconds") instanceof Integer) {
                options = options.expireAfter(Long.valueOf(opts.getInt("expireAfterSeconds")), TimeUnit.SECONDS);
            }
            if (opts.get("background") != null && opts.get("background") instanceof Boolean) {
                options = options.background(opts.getBoolean("background"));
            }
            if (opts.get("bits") != null && opts.get("bits") instanceof Integer) {
                options = options.bits(opts.getInt("bits"));
            }
            if (opts.get("bucketSize") != null && opts.get("bucketSize") instanceof Double) {
                options = options.bucketSize(opts.getDouble("bucketSize"));
            }
            if (opts.get("defaultLanguage") != null && opts.get("defaultLanguage") instanceof String) {
                options = options.defaultLanguage(opts.getString("defaultLanguage"));
            }
            if (opts.get("languageOverride") != null && opts.get("languageOverride") instanceof String) {
                options = options.languageOverride(opts.getString("languageOverride"));
            }
            if (opts.get("min") != null && opts.get("min") instanceof Double) {
                options = options.min(opts.getDouble("min"));
            }
            if (opts.get("max") != null && opts.get("max") instanceof Double) {
                options = options.max(opts.getDouble("max"));
            }
            if (opts.get("sparse") != null && opts.get("sparse") instanceof Boolean) {
                options = options.sparse(opts.getBoolean("sparse"));
            }
            if (opts.get("textVersion") != null && opts.get("textVersion") instanceof Integer) {
                options = options.textVersion(opts.getInt("textVersion"));
            }
            if (opts.get("version") != null && opts.get("version") instanceof Integer) {
                options = options.version(opts.getInt("version"));
            }
            if (opts.get("weights") != null && opts.get("weights") instanceof Bson) {
                options = options.weights((Bson) opts.get("weights"));
            }
        }
        collection.createIndex(new Document(index), options);
    }

    /**
     * Whenever processor is stopped, remove cached indexes
     */
    @OnStopped
    public void cleanUp() {
        indexes.clear();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        // Index properties must be JSON lists
        if (isIndexProperty(propertyDescriptorName)) {
            return MongoProps.DYNAMIC_INDEX
                    .name(propertyDescriptorName)
                    .build();
        }
        // We don't support any other types of dynamic properties right now.
        return null;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    protected WriteConcern determineWriteConcern(PropertyValue property) {
        return determineWriteConcern(property.getValue().toLowerCase());
    }

    private WriteConcern determineWriteConcern(String label) {
        switch (label.toLowerCase()) {
            case "acknowledged":
                return WriteConcern.ACKNOWLEDGED;
            case "unacknowledged":
                return WriteConcern.UNACKNOWLEDGED;
            case "journaled":
                return WriteConcern.JOURNALED;
            case "majority":
                return WriteConcern.MAJORITY;
            default:
                return WriteConcern.ACKNOWLEDGED;
        }
    }
}