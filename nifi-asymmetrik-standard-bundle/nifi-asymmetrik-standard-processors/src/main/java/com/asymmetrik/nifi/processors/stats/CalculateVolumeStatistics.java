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
@Tags({"asymmetrik", "volume", "monitoring", "statistics"})
@CapabilityDescription("Calculates volume statistics for a flow.")
@WritesAttributes({
        @WritesAttribute(attribute = "AbstractStatsProcessor.correlationKey"),
        @WritesAttribute(attribute = "CalculateVolumeStatistics.count"),
        @WritesAttribute(attribute = "CalculateVolumeStatistics.sum"),
        @WritesAttribute(attribute = "CalculateVolumeStatistics.timestamp")
})
public class CalculateVolumeStatistics extends AbstractStatsProcessor {

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
        aggregator.addValue(1.0);
    }

    @Override
    protected Optional<Map<String, String>> buildStatAttributes(long currentTimestamp, MomentAggregator aggregator) {
        // emit stats only if there is data
        if (aggregator.getN() > 0) {
            Map<String, String> attributes = new ImmutableMap.Builder<String, String>()
                    .put("CalculateVolumeStatistics.count", Long.toString(aggregator.getN()))
                    .put("CalculateVolumeStatistics.sum", Long.toString((long) aggregator.getSum()))
                    .put("CalculateVolumeStatistics.timestamp", Long.toString(currentTimestamp))
                    .build();
            return Optional.of(attributes);

        } else {
            return Optional.empty();
        }
    }
}
