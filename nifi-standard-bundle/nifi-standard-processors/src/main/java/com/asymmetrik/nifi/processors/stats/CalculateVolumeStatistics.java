package com.asymmetrik.nifi.processors.stats;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
@Tags({"asymmetrik", "volume", "monitoring", "statistics"})
@CapabilityDescription("Calculates volume statistics for a flow.")
@WritesAttributes({
        @WritesAttribute(attribute = "AbstractStatsProcessor.correlationKey"),
        @WritesAttribute(attribute = "CalculateVolumeStatistics.count"),
        @WritesAttribute(attribute = "CalculateVolumeStatistics.sum"),
        @WritesAttribute(attribute = "CalculateVolumeStatistics.min"),
        @WritesAttribute(attribute = "CalculateVolumeStatistics.max"),
        @WritesAttribute(attribute = "CalculateVolumeStatistics.avg"),
        @WritesAttribute(attribute = "CalculateVolumeStatistics.stdev"),
        @WritesAttribute(attribute = "CalculateVolumeStatistics.units"),
        @WritesAttribute(attribute = "CalculateVolumeStatistics.timestamp")
})
public class CalculateVolumeStatistics extends AbstractStatsProcessor {

    /**
     * Property Descriptors
     */
    static final PropertyDescriptor BUCKET_INTERVAL = new PropertyDescriptor.Builder()
            .name("Bucket Interval")
            .description("Indicates how long to aggregate event counts before sending to the statistics calculator.")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("1 s")
            .build();

    private volatile long bucketIntervalMillis;
    private AtomicLong lastBucketFullTime = new AtomicLong();
    private AtomicLong count = new AtomicLong();

    @Override
    protected void init(ProcessorInitializationContext context) {
        properties = ImmutableList.of(BUCKET_INTERVAL, CORRELATION_ATTR, REPORTING_INTERVAL, BATCH_SIZE);
    }

    @OnScheduled
    @Override
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);
        bucketIntervalMillis = context.getProperty(BUCKET_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);
    }

    @Override
    protected void updateStats(FlowFile flowFile, MomentAggregator aggregator, long currentTimestamp) {
        count.incrementAndGet();

        // Add the number of flowfiles seen if the window time is exceeded, then reset the counter
        if (currentTimestamp >= lastBucketFullTime.get() + bucketIntervalMillis) {
            lastBucketFullTime.set(currentTimestamp);
            aggregator.addValue(count.getAndSet(0L));
        }
    }

    @Override
    protected Optional<Map<String, String>> buildStatAttributes(long currentTimestamp, MomentAggregator aggregator) {
        // emit stats only if there is data
        if (aggregator.getN() > 0) {
            Double bucket = (double) bucketIntervalMillis;
            Double min = 1000.0 * aggregator.getMin() / bucket;
            Double max = 1000.0 * aggregator.getMax() / bucket;
            Double avg = 1000.0 * aggregator.getMean() / bucket;
            Double stdev = 1000.0 * aggregator.getStandardDeviation() / bucket;

            Map<String, String> attributes = new ImmutableMap.Builder<String, String>()
                    .put("CalculateVolumeStatistics.count", Integer.toString(aggregator.getN()))
                    .put("CalculateVolumeStatistics.sum", Integer.toString((int) aggregator.getSum()))
                    .put("CalculateVolumeStatistics.min", Integer.toString(min.intValue()))
                    .put("CalculateVolumeStatistics.max", Integer.toString(max.intValue()))
                    .put("CalculateVolumeStatistics.avg", Integer.toString(avg.intValue()))
                    .put("CalculateVolumeStatistics.stdev", stdev.toString())
                    .put("CalculateVolumeStatistics.timestamp", Long.toString(currentTimestamp))
                    .put("CalculateVolumeStatistics.units", COUNT_PER_SECOND)
                    .build();

            return Optional.of(attributes);

        } else {
            return Optional.empty();
        }
    }
}
