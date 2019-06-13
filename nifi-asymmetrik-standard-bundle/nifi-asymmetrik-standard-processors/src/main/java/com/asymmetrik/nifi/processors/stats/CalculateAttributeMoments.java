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
package com.asymmetrik.nifi.processors.stats;

import java.util.Map;
import java.util.Optional;

import com.asymmetrik.nifi.processors.util.MomentAggregator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.util.StandardValidators;

@TriggerWhenEmpty
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@SupportsBatching
@Tags({"aggregation", "monitoring", "statistics"})
@CapabilityDescription("This processor inspects the flowfile attributes for a specified key.  For each flowfile containing\n" +
        "    this attribute, and for all flowfile where this attribute resolves to a number, the first two statistical moments are computed.\n" +
        "    That is, the min, max, sum, count, mean, and standard deviation are computed.  Computing these moments efficiently is of paramount\n" +
        "    importance.")
@WritesAttributes({
        @WritesAttribute(attribute = "AbstractStatsProcessor.correlationKey"),
        @WritesAttribute(attribute = "CalculateAttributeMoments.count"),
        @WritesAttribute(attribute = "CalculateAttributeMoments.sum"),
        @WritesAttribute(attribute = "CalculateAttributeMoments.min"),
        @WritesAttribute(attribute = "CalculateAttributeMoments.max"),
        @WritesAttribute(attribute = "CalculateAttributeMoments.avg"),
        @WritesAttribute(attribute = "CalculateAttributeMoments.stdev"),
        @WritesAttribute(attribute = "CalculateAttributeMoments.timestamp")
})
public class CalculateAttributeMoments extends AbstractStatsProcessor {

    static final PropertyDescriptor PROP_ATTR_NAME = new PropertyDescriptor.Builder()
            .name("attr.key")
            .displayName("Attribute Name")
            .description("The name of the attribute holding the value to aggregate. The resultant value must resolve to a number.")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private String attrKey;

    @Override
    protected void init(ProcessorInitializationContext context) {
        properties = ImmutableList.of(PROP_ATTR_NAME, CORRELATION_ATTR, REPORTING_INTERVAL, BATCH_SIZE);
    }

    @OnScheduled
    @Override
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);
        attrKey = context.getProperty(PROP_ATTR_NAME).getValue();
    }

    @Override
    protected void updateStats(FlowFile flowFile, MomentAggregator aggregator, long currentTimestamp) {
        String value = flowFile.getAttribute(attrKey);
        if (StringUtils.isBlank(value)) {
            getLogger().warn("No value found for attribute key: ", new Object[] {attrKey});
            return;
        }

        try {
            aggregator.addValue(Double.parseDouble(value));
        } catch (Exception e) {
            getLogger().warn("Unable to parse {} to number", new Object[] {value}, e);
        }
    }

    @Override
    protected Optional<Map<String, String>> buildStatAttributes(long currentTimestamp, MomentAggregator aggregator) {
        // emit stats only if there is data
        if (aggregator.getN() > 0) {
            Map<String, String> attributes = new ImmutableMap.Builder<String, String>()
                    .put("CalculateAttributeMoments.count", Integer.toString(aggregator.getN()))
                    .put("CalculateAttributeMoments.sum", Double.toString(aggregator.getSum()))
                    .put("CalculateAttributeMoments.min", Double.toString(aggregator.getMin()))
                    .put("CalculateAttributeMoments.max", Double.toString(aggregator.getMax()))
                    .put("CalculateAttributeMoments.avg", Double.toString(aggregator.getMean()))
                    .put("CalculateAttributeMoments.stdev", Double.toString(aggregator.getStandardDeviation()))
                    .put("CalculateAttributeMoments.timestamp", Long.toString(currentTimestamp))
                    .build();
            return Optional.of(attributes);

        } else {
            return Optional.empty();
        }
    }
}
