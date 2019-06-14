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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;

@TriggerWhenEmpty
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@SupportsBatching
@Tags({"asymmetrik", "bytes", "monitoring", "statistics"})
@CapabilityDescription("Calculates latency statistics for a flow.")
@WritesAttributes({
        @WritesAttribute(attribute = "AbstractStatsProcessor.correlationKey"),
        @WritesAttribute(attribute = "CalculateBytesTransferred.count"),
        @WritesAttribute(attribute = "CalculateBytesTransferred.sum"),
        @WritesAttribute(attribute = "CalculateBytesTransferred.min"),
        @WritesAttribute(attribute = "CalculateBytesTransferred.max"),
        @WritesAttribute(attribute = "CalculateBytesTransferred.avg"),
        @WritesAttribute(attribute = "CalculateBytesTransferred.stdev"),
        @WritesAttribute(attribute = "CalculateBytesTransferred.timestamp")
})
public class CalculateBytesTransferred extends AbstractStatsProcessor {

    static final String FILE_SIZE = "fileSize";

    @Override
    protected void init(ProcessorInitializationContext context) {
        properties = ImmutableList.of(CORRELATION_ATTR, REPORTING_INTERVAL, BATCH_SIZE);
    }

    @OnScheduled
    @Override
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);
    }

    @Override
    protected void updateStats(FlowFile flowFile, MomentAggregator aggregator, long currentTimestamp) {
        aggregator.addValue(flowFile.getSize());
    }

    @Override
    protected Optional<Map<String, String>> buildStatAttributes(long currentTimestamp, MomentAggregator aggregator) {

        // emit stats only if there is data
        int n = aggregator.getN();
        if (n > 0) {
            Map<String, String> attributes = new ImmutableMap.Builder<String, String>()
                    .put("CalculateBytesTransferred.count", Integer.toString(aggregator.getN()))
                    .put("CalculateBytesTransferred.sum", Double.toString(aggregator.getSum()))
                    .put("CalculateBytesTransferred.min", Double.toString(aggregator.getMin()))
                    .put("CalculateBytesTransferred.max", Double.toString(aggregator.getMax()))
                    .put("CalculateBytesTransferred.avg", Double.toString(aggregator.getMean()))
                    .put("CalculateBytesTransferred.stdev", Double.toString(aggregator.getStandardDeviation()))
                    .put("CalculateBytesTransferred.timestamp", Long.toString(currentTimestamp))
                    .put("CalculateBytesTransferred.units", BYTES)
                    .build();
            return Optional.of(attributes);
        } else {
            return Optional.empty();
        }
    }
}
