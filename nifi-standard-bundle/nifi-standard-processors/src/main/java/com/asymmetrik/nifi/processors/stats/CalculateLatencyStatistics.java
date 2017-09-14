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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.util.StandardValidators;

@TriggerWhenEmpty
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@SupportsBatching
@Tags({"asymmetrik", "latency", "monitoring", "statistics"})
@CapabilityDescription("Calculates latency statistics for a flow.")
@WritesAttributes({
        @WritesAttribute(attribute = "AbstractStatsProcessor.correlationKey"),
        @WritesAttribute(attribute = "CalculateLatencyStatistics.count"),
        @WritesAttribute(attribute = "CalculateLatencyStatistics.sum"),
        @WritesAttribute(attribute = "CalculateLatencyStatistics.min"),
        @WritesAttribute(attribute = "CalculateLatencyStatistics.max"),
        @WritesAttribute(attribute = "CalculateLatencyStatistics.avg"),
        @WritesAttribute(attribute = "CalculateLatencyStatistics.stdev"),
        @WritesAttribute(attribute = "CalculateLatencyStatistics.timestamp")
})
public class CalculateLatencyStatistics extends AbstractStatsProcessor {

    /**
     * Property Descriptors
     */
    static final PropertyDescriptor ATTR_NAME = new PropertyDescriptor.Builder()
            .name("timestamp")
            .displayName("Timestamp Attribute")
            .description("The attribute key holding the timestamp value. This attribute is assumed to be a Java long that represents the milliseconds after epoch.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private volatile String keyName;


    @Override
    protected void init(ProcessorInitializationContext context) {
        properties = ImmutableList.of(ATTR_NAME, CORRELATION_ATTR, REPORTING_INTERVAL, BATCH_SIZE);
    }

    @OnScheduled
    @Override
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);
        keyName = context.getProperty(ATTR_NAME).getValue();
    }

    @Override
    protected void updateStats(FlowFile flowFile, MomentAggregator aggregator, long currentTimestamp) {

        String eventTimeAttribute = flowFile.getAttribute(keyName);
        try {
            // Extract timestamp from original flowfile
            long eventTime = Long.parseLong(eventTimeAttribute);

            // calculate latency and add to aggregator
            Long latency = currentTimestamp - eventTime;
            aggregator.addValue(latency.doubleValue() / 1000.0);
        } catch (NumberFormatException nfe) {
            getLogger().warn("Unable to convert {} to a long", new Object[]{eventTimeAttribute}, nfe);
        }
    }

    @Override
    protected Optional<Map<String, String>> buildStatAttributes(long currentTimestamp, MomentAggregator aggregator) {
        // emit stats only if there is data
        if (aggregator.getN() > 0) {
            int n = aggregator.getN();
            double sum = aggregator.getSum();
            double min = aggregator.getMin();
            double max = aggregator.getMax();
            double mean = aggregator.getMean();
            double stdev = aggregator.getStandardDeviation();

            Map<String, String> attributes = new ImmutableMap.Builder<String, String>()
                    .put("CalculateLatencyStatistics.count", Integer.toString(n))
                    .put("CalculateLatencyStatistics.sum", Double.toString(sum))
                    .put("CalculateLatencyStatistics.min", Double.toString(min))
                    .put("CalculateLatencyStatistics.max", Double.toString(max))
                    .put("CalculateLatencyStatistics.avg", Double.toString(mean))
                    .put("CalculateLatencyStatistics.stdev", Double.toString(stdev))
                    .put("CalculateLatencyStatistics.timestamp", Long.toString(currentTimestamp))
                    .put("CalculateLatencyStatistics.units", SECONDS)
                    .build();
            return Optional.of(attributes);

        } else {
            return Optional.empty();
        }
    }
}
