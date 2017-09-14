package com.asymmetrik.nifi.services.cache;

import java.util.List;

/**
 * API for a cache to handle duplicate elements.
 */
public interface SetCache<K> {
    /**
     * Adds element to the cache if not already present.
     *
     * @param key element to add to cache
     * @return true if element was added, false if not added (already present)
     */
    boolean addIfAbsent(K key);

    /**
     * Adds elements to the cache if not already present.
     *
     * @param keys elements to add to cache
     * @return list of booleans for each element in <code>keys</code>, true if element was added,
     * false if not added (already present)
     */
    List<Boolean> addIfAbsent(List<K> keys);

    /**
     * Returns true if cache contains the specified element.
     *
     * @param key element to check
     * @return true if cache contains specified element
     */
    boolean contains(K key);

    /**
     * Discards any cached value for the specified element.
     *
     * @param key element to remove from cache
     */
    void remove(K key);

    /**
     * Discards all entries in the cache.
     */
    void removeAll();
}
