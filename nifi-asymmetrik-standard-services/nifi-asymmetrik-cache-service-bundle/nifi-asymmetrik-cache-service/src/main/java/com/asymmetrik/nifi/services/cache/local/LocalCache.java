package com.asymmetrik.nifi.services.cache.local;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.asymmetrik.nifi.services.cache.SetCacheService;
import com.asymmetrik.nifi.services.cache.impl.SetCacheImpl;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;

@Tags({"asymmetrik", "duplicate", "dedupe", "local", "cache"})
@CapabilityDescription("Used to identify duplicate flowfiles within a single NiFi node")
@SeeAlso(classNames = {
        "com.asymmetrik.nifi.standard.services.cache.distributed.DistributedCacheClient",
        "com.asymmetrik.nifi.standard.processors.IdentifyDuplicate"
})
public class LocalCache extends AbstractControllerService implements SetCacheService<String> {

    private SetCacheImpl<String> cache;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(MAX_SIZE);
        descriptors.add(AGE_OFF_DURATION);
        descriptors.add(CONCURRENCY_LEVEL);
        return descriptors;
    }

    @OnEnabled
    public void initialize(final ConfigurationContext context) {
        if (cache == null) {
            final int maxSize = context.getProperty(MAX_SIZE).asInteger();
            final Long expireDuration = context.getProperty(AGE_OFF_DURATION).asTimePeriod(TimeUnit.MILLISECONDS);
            final int concurrencyLevel = context.getProperty(CONCURRENCY_LEVEL).isSet() ?
                    context.getProperty(CONCURRENCY_LEVEL).asInteger() : -1;
            cache = new SetCacheImpl<>(maxSize, expireDuration, TimeUnit.MILLISECONDS, concurrencyLevel);
        }
    }

    @OnDisabled
    public void shutdown() {
        if (cache != null) {
            cache.removeAll();
        }
        cache = null;
    }

    @Override
    public boolean addIfAbsent(String key) {
        return cache.addIfAbsent(key);
    }

    @Override
    public List<Boolean> addIfAbsent(List<String> keys) {
        return cache.addIfAbsent(keys);
    }

    @Override
    public boolean contains(String key) {
        return cache.contains(key);
    }

    @Override
    public void remove(String key) {
        cache.remove(key);
    }

    @Override
    public void removeAll() {
        cache.removeAll();
    }
}
