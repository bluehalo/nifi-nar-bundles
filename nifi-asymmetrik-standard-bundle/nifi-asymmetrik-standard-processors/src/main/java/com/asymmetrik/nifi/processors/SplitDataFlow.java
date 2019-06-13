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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SupportsBatching
@SideEffectFree
@Tags({"split", "throttle"})
@CapabilityDescription("Splits the dataflow into two; \"a\" and \"b\". The route a will route the first " +
        "n flowfiles to the relationship a and the remaining flowfiles are routed to the \"b\" relationship. For example, " +
        "if the split percentage is set to 13, the first 13 flowfiles will be sent to \"a\" and the remaining 87 flowfiles " +
        "will be routed to \"b\"")
public class SplitDataFlow extends AbstractProcessor {

    /**
     * Property Descriptors
     */
    static final PropertyDescriptor PERCENTAGE = new PropertyDescriptor.Builder()
            .name("Split Percentage")
            .displayName("Split Percentage")
            .description("The percentage of the flow to send to relationship \"a\".")
            .required(true)
            .addValidator(new PercentageValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("50")
            .build();

    static final Relationship REL_A = new Relationship.Builder()
            .name("a")
            .description("The first n flowfiles will be sent to this relationship")
            .build();

    static final Relationship REL_B = new Relationship.Builder()
            .name("b")
            .description("The flowfiles after the first n will be sent to this relationship")
            .build();

    private int percentage = 0;
    private AtomicInteger counter = new AtomicInteger(0);

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_A);
        rels.add(REL_B);
        return Collections.unmodifiableSet(rels);
    }

    @OnScheduled
    public void initialize(final ProcessContext context) {
        percentage = context.getProperty(PERCENTAGE).evaluateAttributeExpressions().asInteger();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        Relationship rel;
        if (percentage >= 100) {
            rel = REL_A;
            counter.getAndSet(0);
        } else if (percentage <= 0) {
            rel = REL_B;
        } else {
            rel = counter.incrementAndGet() <= percentage ? REL_A : REL_B;
        }

        session.transfer(flowFile, rel);
        if (counter.get() >= 100) {
            counter.set(0);
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.unmodifiableList(Collections.singletonList(PERCENTAGE));
    }

    public static class PercentageValidator implements Validator {

        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return (new ValidationResult.Builder()).subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
            } else {
                try {
                    int i = Integer.parseInt(input);
                    return new ValidationResult.Builder().subject(subject).valid(true).input(input).explanation(i + " is valid").build();
                } catch (final Exception e) {
                    return new ValidationResult.Builder().subject(subject).valid(false).input(input).explanation("Invalid Percentage: " + e).build();
                }
            }
        }
    }
}
