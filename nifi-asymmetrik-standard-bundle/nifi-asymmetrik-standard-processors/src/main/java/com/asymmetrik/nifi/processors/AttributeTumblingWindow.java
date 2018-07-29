package com.asymmetrik.nifi.processors;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"Attribute Expression Language", "state", "data science", "rolling", "window"})
@CapabilityDescription("Track a Tumbling Window based on evaluating an Expression Language expression on each FlowFile and add that value to the processor's state. Each FlowFile will be emitted " +
    "with the count of FlowFiles and total aggregate value of values processed in the current time window.")
@WritesAttributes({
    @WritesAttribute(attribute = AttributeTumblingWindow.TUMBLING_WINDOW_SUM_KEY, description = "The rolling window value (sum of all the values stored)."),
    @WritesAttribute(attribute = AttributeTumblingWindow.TUMBLING_WINDOW_COUNT_KEY, description = "The count of the number of FlowFiles seen in the rolling window.")
})
@Stateful(scopes = {Scope.LOCAL}, description = "Store the values backing the rolling window. This includes storing the individual values and their time-stamps or the batches of values and their " +
    "counts.")
public class AttributeTumblingWindow extends AbstractProcessor {
    // write attributes
    static final String TUMBLING_WINDOW_SUM_KEY = "AttributeTumblingWindow.sum";
    static final String TUMBLING_WINDOW_COUNT_KEY = "AttributeTumblingWindow.count";

    static final PropertyDescriptor VALUE_TO_TRACK = new PropertyDescriptor.Builder()
        .displayName("Value to track")
        .name("Value to track")
        .description("The expression on which to evaluate each FlowFile. The result of the expression will be added to the rolling window value as a double.")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
        .required(true)
        .build();
    static final PropertyDescriptor TIME_WINDOW = new PropertyDescriptor.Builder()
        .displayName("Time window")
        .name("Time window")
        .description("The time window on which to calculate the tumbling window.")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .required(true)
        .build();

    static final Relationship ELAPSED = new Relationship.Builder()
        .description("Flowfiles arriving after specified time window will get routed here")
        .name("elapsed")
        .build();
    static final Relationship SUCCESS = new Relationship.Builder()
        .description("ALL successfully processed flowFiles arrive here which includes all ELAPSED flowfiles")
        .name("success")
        .build();
    static final Relationship FAILURE = new Relationship.Builder()
        .name("failure")
        .description("When a flowFile fails for a reason other than failing to set state it is routed here.")
        .build();

    private CheckedStopWatch stopWatch;

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        context.getStateManager().clear(Scope.LOCAL);

        this.stopWatch = new CheckedStopWatch(context.getProperty(TIME_WINDOW).asTimePeriod(TimeUnit.MILLISECONDS));
        this.stopWatch.start();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            double value = context.getProperty(VALUE_TO_TRACK).evaluateAttributeExpressions(flowFile).asDouble();

            if (stopWatch.elapsed()) {
                Map<String, String> aggregate = incrementAndSet(context.getStateManager(), value, true);
                flowFile = session.putAllAttributes(flowFile, aggregate);

                session.transfer(session.clone(flowFile), SUCCESS);
                session.transfer(flowFile, ELAPSED);
                stopWatch.restart();
            } else {
                Map<String, String> aggregate = incrementAndSet(context.getStateManager(), value, false);
                flowFile = session.putAllAttributes(flowFile, aggregate);
                session.transfer(flowFile, SUCCESS);
            }
        } catch (Exception e) {
            getLogger().error(e.getMessage(), e);
            session.transfer(flowFile, FAILURE);
        }
    }

    private Map<String, String> incrementAndSet(StateManager stateManager, double value, boolean resetFirst) throws IOException {
        StateMap stateMap = stateManager.getState(Scope.LOCAL);

        String oldSumString = stateMap.get(TUMBLING_WINDOW_SUM_KEY);
        String oldCountString = stateMap.get(TUMBLING_WINDOW_COUNT_KEY);

        if (oldCountString == null || oldSumString == null) {
            oldSumString = "0.0";
            oldCountString = "0";
        }

        String newSumString = String.valueOf(value);
        String newCountString = "1";

        if (!resetFirst) {
            newSumString = String.valueOf(Double.valueOf(oldSumString) + value);
            newCountString = String.valueOf(Long.valueOf(oldCountString) + 1);
        }

        Map<String, String> newState = ImmutableMap.of(TUMBLING_WINDOW_SUM_KEY, newSumString, TUMBLING_WINDOW_COUNT_KEY, newCountString);
        stateManager.setState(newState, Scope.LOCAL);

        return resetFirst ?
            ImmutableMap.of(TUMBLING_WINDOW_SUM_KEY, oldSumString, TUMBLING_WINDOW_COUNT_KEY, oldCountString) :
            newState;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(VALUE_TO_TRACK, TIME_WINDOW);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return ImmutableSet.of(ELAPSED, SUCCESS, FAILURE);
    }

    private static class CheckedStopWatch {
        private final StopWatch stopWatch;
        private final long timeFrameMilliseconds;

        CheckedStopWatch(long timeframeMilliseconds) {
            this.stopWatch = new StopWatch(false);
            this.timeFrameMilliseconds = timeframeMilliseconds;
        }

        boolean elapsed() {
            return stopWatch.getElapsed(TimeUnit.MILLISECONDS) >= timeFrameMilliseconds;
        }

        void restart() {
            stopWatch.stop();
            stopWatch.start();
        }

        void start() {
            stopWatch.start();
        }
    }
}
