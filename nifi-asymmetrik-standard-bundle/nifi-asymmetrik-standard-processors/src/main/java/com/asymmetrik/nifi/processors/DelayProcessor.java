package com.asymmetrik.nifi.processors;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

@SupportsBatching
@SideEffectFree
@TriggerSerially
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"asymmetrik", "delay", "rate"})
@CapabilityDescription("Delays every FlowFile by a specified duration")
public class DelayProcessor extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles are routed to success... eventually")
            .build();

    public static final PropertyDescriptor TIME_PERIOD = new PropertyDescriptor.Builder()
            .name("Time Duration")
            .description("The amount of time to delay every FlowFile.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.createTimePeriodValidator(1, TimeUnit.SECONDS, Integer.MAX_VALUE, TimeUnit.SECONDS))
            .defaultValue("1 min")
            .build();
    final ConcurrentMap<String, Delay> delayMap = new ConcurrentHashMap<>();
    private final List<PropertyDescriptor> properties = ImmutableList.of(TIME_PERIOD);
    private final Set<Relationship> relationships = ImmutableSet.of(REL_SUCCESS);
    private final DelayFilter delayFilter = new DelayFilter();

    private ComponentLog logger;
    private volatile PropertyValue delayPeriodPropertyValue;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        super.onPropertyModified(descriptor, oldValue, newValue);

        if (descriptor.equals(TIME_PERIOD)) {
            // if delay criteria is changed, we must clear our map
            getLogger().debug("Time Period changed, clearing state");
            delayMap.clear();
        }
    }

    @OnScheduled
    public void initialize(final ProcessContext context) {
        logger = getLogger();
        delayPeriodPropertyValue = context.getProperty(TIME_PERIOD);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        List<FlowFile> flowFiles = session.get(delayFilter);

        if (flowFiles.isEmpty()) {
            logger.debug("no flowFiles ready to be processed, yielding");
            context.yield();
        } else {
            for (FlowFile flowFile : flowFiles) {
                delayMap.remove(flowFile.getAttribute(CoreAttributes.UUID.key()));
            }
            logger.debug("transferring {} files to 'success': {}", new Object[]{flowFiles.size(), flowFiles});
            session.transfer(flowFiles, REL_SUCCESS);
        }
    }

    private static class Delay {
        private final long delayMillis;
        private final long startTimeMillis;

        public Delay(final long delayMillis) {
            this.delayMillis = delayMillis;
            this.startTimeMillis = System.currentTimeMillis();
        }

        public boolean evaluate() {
            return ((System.currentTimeMillis() - startTimeMillis) >= delayMillis);
        }
    }

    private class DelayFilter implements FlowFileFilter {

        @Override
        public FlowFileFilterResult filter(FlowFile flowFile) {
            final String uuid = flowFile.getAttribute(CoreAttributes.UUID.key());
            Delay delay = delayMap.get(uuid);
            if (delay == null) {
                delay = new Delay(delayPeriodPropertyValue.evaluateAttributeExpressions(flowFile).asTimePeriod(TimeUnit.MILLISECONDS));
                delayMap.put(uuid, delay);
            }

            return delay.evaluate() ? FlowFileFilterResult.ACCEPT_AND_CONTINUE : FlowFileFilterResult.REJECT_AND_CONTINUE;
        }
    }
}
