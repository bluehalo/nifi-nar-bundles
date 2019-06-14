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
