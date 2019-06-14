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
package com.asymmetrik.nifi.processors;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.TriggerWhenAnyDestinationAvailable;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SideEffectFree
@Tags({"asymmetrik", "route", "queue", "input", "back", "pressure"})
@CapabilityDescription("Routes all flowfiles through pass-through relationship as long as all of its" +
        " connections are not back-pressured at which point all flowfiles will route through" +
        " back-pressured relationship")
@EventDriven
@TriggerWhenAnyDestinationAvailable
public class RouteOnBackPressure extends AbstractProcessor {

    public static final Relationship BACK_PRESSURED = new Relationship.Builder()
            .name("back-pressured")
            .description("This relationship is used when back pressure is detected in ALL pass-through connections")
            .build();

    public static final Relationship PASS_THROUGH = new Relationship.Builder()
            .name("pass-through")
            .description("This relationship is used to pass through all flowfiles to until back-pressure is detected")
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of flowfiles to take from the incoming work queue")
            .defaultValue("1")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .required(false)
            .build();

    private int batchSize;

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        this.batchSize = context.getProperty(BATCH_SIZE).isSet() ? context.getProperty(BATCH_SIZE).asInteger() : 1;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        List<FlowFile> incoming = session.get(batchSize);
        if (incoming.isEmpty()) {
            return;
        }

        /*
            Each relationship can have multiple connections.
            context.getAvailableRelationships().contains(PASS_THROUGH) will return true if all
            PASS_THROUGH connections are available (not back pressured). So if a PASS_THROUGH
            relationship has 2 connections with back pressure of 10 and 20 and only the 10's
            connection is back pressured, contains(PASS_THROUGH) will return false
         */
        final boolean isPassthroughClear = context.getAvailableRelationships().contains(PASS_THROUGH);
        session.transfer(incoming, isPassthroughClear ? PASS_THROUGH : BACK_PRESSURED);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return ImmutableSet.of(PASS_THROUGH, BACK_PRESSURED);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(BATCH_SIZE);
    }
}
