package com.asymmetrik.nifi.services.cache.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.asymmetrik.nifi.services.cache.SetCache;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * {@link SetCache} implementation using an underlying Guava Cache.
 */
public class SetCacheImpl<K> implements SetCache<K> {
    private final Cache<K, byte[]> cache;

    private static final byte[] EMPTY_VALUE = new byte[0];

    /**
     * @param maxSize        the maximum number of entries that the cache can hold
     * @param expireDuration the length of time after an entry is created that it should be
     *                       automatically removed
     * @param expireUnit     the unit that {@code duration} is expressed in
     */
    public SetCacheImpl(int maxSize, long expireDuration, TimeUnit expireUnit) {
        this(maxSize, expireDuration, expireUnit, -1);
    }

    /**
     * @param maxSize          the maximum number of entries that the cache can hold
     * @param expireDuration   the length of time after an entry is created that it should be
     *                         automatically removed
     * @param expireUnit       the unit that {@code duration} is expressed in
     * @param concurrencyLevel guides the allowed concurrency among update operations. Used as a
     *                         hint for internal sizing. Refer to Guava CacheBuilder for specific
     *                         details.
     */
    public SetCacheImpl(int maxSize, long expireDuration, TimeUnit expireUnit, int concurrencyLevel) {
        CacheBuilder builder = CacheBuilder.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(expireDuration, expireUnit);
        if (concurrencyLevel > 0) {
            builder = builder.concurrencyLevel(concurrencyLevel);
        }

        this.cache = builder.build();
    }

    @Override
    public boolean addIfAbsent(K key) {
        if (cache.getIfPresent(key) == null) {
            cache.put(key, EMPTY_VALUE);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public List<Boolean> addIfAbsent(List<K> keys) {
        List<Boolean> returnValues = new ArrayList<>();
        for (K key : keys) {
            returnValues.add(addIfAbsent(key));
        }
        return returnValues;
    }

    @Override
    public boolean contains(K key) {
        return cache.getIfPresent(key) != null;
    }

    @Override
    public void remove(K key) {
        cache.invalidate(key);
    }

    @Override
    public void removeAll() {
        cache.invalidateAll();
    }
}
