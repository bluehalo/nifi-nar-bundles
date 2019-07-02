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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@EventDriven
@SupportsBatching
@Tags({"retry", "counter"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Determines whether incoming flowfiles qualifies for retrying based on specified conditional")
@DynamicProperty(name = "A FlowFile attribute to update",
        value = "The value to set it to",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "Updates a FlowFile attribute specified by the Dynamic Property's key with the value specified by the Dynamic Property's value for all finished relationships")
@DefaultSettings(penaltyDuration = "2 seconds")
public class RetryCounter extends AbstractProcessor {

    static final PropertyDescriptor COUNTER_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("counter.variable")
            .displayName("Counter Attribute")
            .description("The attribute name holding our counter value. If counter variable doesn't exist, one will be created and initialized to 0")
            .required(true)
            .defaultValue("retry.counter")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor CONDITIONAL_EXPRESSION = new PropertyDescriptor.Builder()
            .name("while.conditional")
            .displayName("While Retry Expression")
            .description("Expression that must evaluate to true in order to continue retrying")
            .required(true)
            .defaultValue("${retry.counter:lt(10)}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    static final PropertyDescriptor ITERATION_EXPRESSION = new PropertyDescriptor.Builder()
            .name("iteration.expression")
            .displayName("Iteration Expression")
            .description("Expression that's executed on every iteration and who's result is assigned to the counter variable")
            .required(true)
            .defaultValue("${retry.counter:plus(1)}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor PENALIZE_ITERATION = new PropertyDescriptor.Builder()
            .name("iteration.penalize")
            .displayName("Penalize per Iteration")
            .description("Indicates whether or not to penalize the flowfile per iteration")
            .allowableValues(Boolean.TRUE.toString(), Boolean.FALSE.toString())
            .defaultValue(Boolean.TRUE.toString())
            .build();

    static final Relationship RETRY = new Relationship.Builder()
            .name("retry")
            .description("Files that are still in retry state are transferred to this relationship")
            .build();

    static final Relationship EXCEEDED = new Relationship.Builder()
            .name("conditional-violated")
            .description("Files who's conditional expression evaluated to false are sent to this relationship")
            .build();

    private Map<String, PropertyDescriptor> dynamicAttributes;

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        this.dynamicAttributes = new HashMap<>();
        for (Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.isDynamic()) {
                dynamicAttributes.put(descriptor.getName(), descriptor);
            }
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowfile = session.get();
        if (flowfile == null) {
            return;
        }

        String counterAttribute = context.getProperty(COUNTER_ATTRIBUTE).getValue();
        String counterValue = flowfile.getAttribute(counterAttribute);
        if (counterValue == null) {
            flowfile = session.putAttribute(flowfile, counterAttribute, "0");
        }

        if (context.getProperty(CONDITIONAL_EXPRESSION).evaluateAttributeExpressions(flowfile).asBoolean()) {
            String updatedValue = context.getProperty(ITERATION_EXPRESSION).evaluateAttributeExpressions(flowfile).getValue();
            flowfile = session.putAttribute(flowfile, counterAttribute, updatedValue);

            if (context.getProperty(PENALIZE_ITERATION).asBoolean()) {
                flowfile = session.penalize(flowfile);
            }
            session.transfer(flowfile, RETRY);
        } else {
            // finished relationship attributes
            Map<String, String> attributes = new HashMap<>();
            for (Map.Entry<String, PropertyDescriptor> entry : dynamicAttributes.entrySet()) {
                attributes.put(entry.getKey(), context.getProperty(entry.getValue()).evaluateAttributeExpressions(flowfile).getValue());
            }

            flowfile = session.putAllAttributes(flowfile, attributes);
            session.transfer(flowfile, EXCEEDED);
        }
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(COUNTER_ATTRIBUTE, CONDITIONAL_EXPRESSION, ITERATION_EXPRESSION,
                PENALIZE_ITERATION);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return ImmutableSet.of(RETRY, EXCEEDED);
    }
}
