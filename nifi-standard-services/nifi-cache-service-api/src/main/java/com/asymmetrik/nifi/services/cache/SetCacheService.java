package com.asymmetrik.nifi.services.cache;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * {@link ControllerService} encapsulating {@link SetCache} functionalty.
 */
public interface SetCacheService<K> extends SetCache<K>, ControllerService {
    PropertyDescriptor MAX_SIZE = new PropertyDescriptor.Builder()
            .name("Maximum Entries")
            .description("The maximum number of entries that the cache can hold")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10000")
            .build();
    PropertyDescriptor AGE_OFF_DURATION = new PropertyDescriptor.Builder()
            .name("Age Off Duration")
            .description("Time interval to expire cache entries")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();
    PropertyDescriptor CONCURRENCY_LEVEL = new PropertyDescriptor.Builder()
            .name("Concurrency Level")
            .description("Guides the allowed concurrency among update operations. Used as a hint for internal sizing. Refer to Guava CacheBuilder for specific details.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
}
