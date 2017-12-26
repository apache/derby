/*

   Derby - Class org.apache.derby.impl.services.cache.ConcurrentCache

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.services.cache;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.Module;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.cache.CacheManager;
import org.apache.derby.iapi.services.cache.Cacheable;
import org.apache.derby.iapi.services.cache.CacheableFactory;
import org.apache.derby.iapi.services.daemon.DaemonService;
import org.apache.derby.iapi.services.jmx.ManagementService;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.util.Matchable;
import org.apache.derby.mbeans.CacheManagerMBean;

/**
 * A cache manager based on the utilities found in the
 * <code>java.util.concurrent</code> package. It allows multiple threads to
 * access the cache concurrently without blocking each other, given that they
 * request different objects and the requested objects are present in the
 * cache.
 *
 * <p>
 *
 * All methods of this class should be thread safe. When exclusive access to an
 * entry is required, it is achieved by calling the <code>lock()</code> method
 * on the <code>CacheEntry</code> object. To ensure that the entry is always
 * unlocked, all calls to <code>CacheEntry.lock()</code> should be followed by
 * a <code>try</code> block with a <code>finally</code> clause that unlocks the
 * entry.
 */
final class ConcurrentCache implements CacheManager {

    /** Map with all the cached objects. */
    private final ConcurrentHashMap<Object, CacheEntry> cache;
    /** Factory which creates <code>Cacheable</code>s. */
    private final CacheableFactory holderFactory;
    /** Name of this cache. */
    private final String name;
    /** The maximum size (number of elements) for this cache. */
    private final int maxSize;
    /** Replacement policy to be used for this cache. */
    private final ReplacementPolicy replacementPolicy;

    // Fields used by the MBean that monitors this instance.

    /** The identifier of the MBean that allows monitoring of this instance. */
    private Object mbean;
    /** Flag that tells if hit/miss/eviction counts should be collected. */
    private volatile boolean collectAccessCounts;
    /** The number of cache hits. */
    private final AtomicLong hits = new AtomicLong();
    /** The number of cache misses. */
    private final AtomicLong misses = new AtomicLong();
    /** The number of evictions from the cache. */
    private final AtomicLong evictions = new AtomicLong();

    /**
     * Flag that indicates whether this cache instance has been shut down. When
     * it has been stopped, <code>find()</code>, <code>findCached()</code> and
     * <code>create()</code> will return <code>null</code>. The flag is
     * declared <code>volatile</code> so that no synchronization is needed when
     * it is accessed by concurrent threads.
     */
    private volatile boolean stopped;

    /**
     * Background cleaner which can be used to clean cached objects in a
     * separate thread to avoid blocking the user threads.
     */
    private BackgroundCleaner cleaner;

    /**
     * Creates a new cache manager.
     *
     * @param holderFactory factory which creates <code>Cacheable</code>s
     * @param name the name of the cache
     * @param initialSize the initial capacity of the cache
     * @param maxSize maximum number of elements in the cache
     */
    ConcurrentCache(CacheableFactory holderFactory, String name,
                    int initialSize, int maxSize) {
        cache = new ConcurrentHashMap<Object, CacheEntry>(initialSize);
        replacementPolicy = new ClockPolicy(this, initialSize, maxSize);
        this.holderFactory = holderFactory;
        this.name = name;
        this.maxSize = maxSize;
    }

    /**
     * Return the <code>ReplacementPolicy</code> instance for this cache.
     *
     * @return replacement policy
     */
    ReplacementPolicy getReplacementPolicy() {
        return replacementPolicy;
    }

    /**
     * Get the entry associated with the specified key from the cache. If the
     * entry does not exist, insert an empty one and return it. The returned
     * entry is always locked for exclusive access by the current thread, but
     * not kept. If another thread is currently setting the identity of this
     * entry, this method will block until the identity has been set.
     *
     * @param key the identity of the cached object
     * @return an entry for the specified key, always locked
     */
    private CacheEntry getEntry(Object key) {
        CacheEntry entry = cache.get(key);
        while (true) {
            if (entry != null) {
                // Found an entry in the cache, lock it.
                entry.lock();
                // If someone else is setting the identity of the Cacheable
                // in this entry, we'll need to wait for them to complete.
                entry.waitUntilIdentityIsSet();
                if (entry.isValid()) {
                    // Entry is still valid. Return it.
                    return entry;
                } else {
                    // This entry has been removed from the cache while we were
                    // waiting for the lock. Unlock it and try again.
                    entry.unlock();
                    entry = cache.get(key);
                }
            } else {
                CacheEntry freshEntry = new CacheEntry();
                // Lock the entry before it's inserted to avoid window for
                // others to remove it.
                freshEntry.lock();
                CacheEntry oldEntry = cache.putIfAbsent(key, freshEntry);
                if (oldEntry != null) {
                    // Someone inserted the entry while we created a new
                    // one. Retry with the entry currently in the cache. Don't
                    // bother unlocking freshEntry since no one else could have
                    // seen it.
                    entry = oldEntry;
                } else {
                    // We successfully inserted a new entry.
                    return freshEntry;
                }
            }
        }
    }

    /**
     * Remove an entry from the cache. Its <code>Cacheable</code> is cleared
     * and made available for other entries. This method should only be called
     * if the entry is present in the cache and locked by the current thread.
     *
     * @param key the identity of the entry to remove
     */
    private void removeEntry(Object key) {
        CacheEntry entry = cache.remove(key);
        Cacheable c = entry.getCacheable();
        if (c != null && c.getIdentity() != null) {
            // The cacheable should not have an identity when it has been
            // removed.
            c.clearIdentity();
        }
        entry.free();
    }

    /**
     * Evict an entry to make room for a new entry that is being inserted into
     * the cache. Clear the identity of its {@code Cacheable} and set it to
     * {@code null}. When this method is called, the caller has already chosen
     * the {@code Cacheable} for reuse. Therefore, this method won't call
     * {@code CacheEntry.free()} as that would make the {@code Cacheable} free
     * for reuse by other entries as well.
     *
     * <p>
     *
     * The caller must have locked the entry that is about to be evicted.
     *
     * @param key identity of the entry to remove
     */
    void evictEntry(Object key) {
        CacheEntry entry = cache.remove(key);
        entry.getCacheable().clearIdentity();
        entry.setCacheable(null);
        countEviction();
    }

    /**
     * Insert a {@code CacheEntry} into a free slot in the {@code
     * ReplacementPolicy}'s internal data structure, and return a {@code
     * Cacheable} that the caller can reuse. The entry must have been locked
     * before this method is called.
     *
     * @param key the identity of the object being inserted
     * @param entry the entry that is being inserted
     * @return a {@code Cacheable} object that the caller can reuse
     * @throws StandardException if an error occurs while inserting the entry
     * or while allocating a new {@code Cacheable}
     */
    private Cacheable insertIntoFreeSlot(Object key, CacheEntry entry)
            throws StandardException {

        try {
            replacementPolicy.insertEntry(entry);
        } catch (StandardException se) {
            // Failed to insert the entry into the replacement policy. Make
            // sure that it's also removed from the hash table.
            removeEntry(key);
            throw se;
        }

        Cacheable free = entry.getCacheable();

        if (free == null) {
            // We didn't get a reusable cacheable. Create a new one.
            free = holderFactory.newCacheable(this);
        }

        entry.keep(true);

        return free;
    }

    /**
     * Complete the setting of the identity. This includes notifying the
     * threads that are waiting for the setting of the identity to complete,
     * so that they can wake up and continue. If setting the identity failed,
     * the entry will be removed from the cache.
     *
     * @param key the identity of the object being inserted
     * @param entry the entry which is going to hold the cached object
     * @param item a {@code Cacheable} object with the identity set (if
     * the identity was successfully set), or {@code null} if setting the
     * identity failed
     */
    private void settingIdentityComplete(
            Object key, CacheEntry entry, Cacheable item) {
        entry.lock();
        try {
            entry.settingIdentityComplete();
            if (item != null) {
                entry.setCacheable(item);
            } else {
                removeEntry(key);
            }
        } finally {
            entry.unlock();
        }
    }

    // Implementation of the CacheManager interface

    /**
     * Find an object in the cache. If it is not present, add it to the
     * cache. The returned object is kept until <code>release()</code> is
     * called.
     *
     * @param key identity of the object to find
     * @return the cached object, or <code>null</code> if it cannot be found
     */
    public Cacheable find(Object key) throws StandardException {

        if (stopped) {
            return null;
        }

        CacheEntry entry = getEntry(key);

        Cacheable item;
        try {
            item = entry.getCacheable();
            if (item != null) {
                // The object is already cached. Increase the use count and
                // return it.
                entry.keep(true);
                countHit();
                return item;
            } else {
                // The object is not cached. Insert the entry into a free
                // slot and retrieve a reusable Cacheable.
                item = insertIntoFreeSlot(key, entry);
                countMiss();
            }
        } finally {
            entry.unlock();
        }

        // Set the identity without holding the lock on the entry. If we
        // hold the lock, we may run into a deadlock if the user code in
        // setIdentity() re-enters the buffer manager.
        Cacheable itemWithIdentity = null;
        try {
            itemWithIdentity = item.setIdentity(key);
        } finally {
            // Always invoke settingIdentityComplete(), also on error,
            // otherwise other threads may wait forever. If setIdentity()
            // fails, itemWithIdentity is going to be null.
            settingIdentityComplete(key, entry, itemWithIdentity);
        }

        return itemWithIdentity;
    }

    /**
     * Find an object in the cache. If it is not present, return
     * <code>null</code>. The returned object is kept until
     * <code>release()</code> is called.
     *
     * @param key identity of the object to find
     * @return the cached object, or <code>null</code> if it's not in the cache
     */
    public Cacheable findCached(Object key) throws StandardException {

        if (stopped) {
            return null;
        }

        // Use get() instead of getEntry() so that we don't insert an empty
        // entry if the requested object isn't there.
        CacheEntry entry = cache.get(key);
        if (entry == null) {
            // No such object was found in the cache.
            countMiss();
            return null;
        }

        // Lock the entry, but wait until its identity has been set.
        entry.lock();
        try {
            // If the identity of the cacheable is being set, we need to wait
            // for it to complete so that we don't return a cacheable that
            // isn't fully initialized.
            entry.waitUntilIdentityIsSet();

            // Return the cacheable. If the entry was removed right before we
            // locked it, getCacheable() returns null and so should we do.
            Cacheable item = entry.getCacheable();
            if (item != null) {
                countHit();
                entry.keep(true);
            } else {
                countMiss();
            }
            return item;
        } finally {
            entry.unlock();
        }
    }

    /**
     * Create an object in the cache. The object is kept until
     * <code>release()</code> is called.
     *
     * @param key identity of the object to create
     * @param createParameter parameters passed to
     * <code>Cacheable.createIdentity()</code>
     * @return a reference to the cached object, or <code>null</code> if the
     * object cannot be created
     * @exception StandardException if the object is already in the cache, or
     * if some other error occurs
     * @see Cacheable#createIdentity(Object,Object)
     */
    public Cacheable create(Object key, Object createParameter)
            throws StandardException {

        if (stopped) {
            return null;
        }

        CacheEntry entry = new CacheEntry();
        entry.lock();

        if (cache.putIfAbsent(key, entry) != null) {
            // We can't create the object if it's already in the cache.
            throw StandardException.newException(
                    SQLState.OBJECT_EXISTS_IN_CACHE, name, key);
        }

        Cacheable item;
        try {
            item = insertIntoFreeSlot(key, entry);
        } finally {
            entry.unlock();
        }

        // Create the identity without holding the lock on the entry.
        // Otherwise, we may run into a deadlock if the user code in
        // createIdentity() re-enters the buffer manager.
        Cacheable itemWithIdentity = null;
        try {
            itemWithIdentity = item.createIdentity(key, createParameter);
        } finally {
            // Always invoke settingIdentityComplete(), also on error,
            // otherwise other threads may wait forever. If createIdentity()
            // fails, itemWithIdentity is going to be null.
            settingIdentityComplete(key, entry, itemWithIdentity);
        }

        return itemWithIdentity;
    }

    /**
     * Release an object that has been fetched from the cache with
     * <code>find()</code>, <code>findCached()</code> or <code>create()</code>.
     *
     * @param item a <code>Cacheable</code> value
     */
    public void release(Cacheable item) {
        // The entry must be present and kept when this method is called, so we
        // don't need the complexity of getEntry() to ensure that the entry is
        // not added to or removed from the cache before we have locked
        // it. Just call get() which is cheaper.
        CacheEntry entry = cache.get(item.getIdentity());
        entry.lock();
        try {
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(item == entry.getCacheable());
            }
            entry.unkeep();
        } finally {
            entry.unlock();
        }
    }

    /**
     * Remove an object from the cache. The object must previously have been
     * fetched from the cache with <code>find()</code>,
     * <code>findCached()</code> or <code>create()</code>. The user of the
     * cache must make sure that only one caller executes this method on a
     * cached object. This method will wait until the object has been removed
     * (its keep count must drop to zero before it can be removed).
     *
     * @param item the object to remove from the cache
     */
    public void remove(Cacheable item) throws StandardException {
        Object key = item.getIdentity();

        // The entry must be present and kept when this method is called, so we
        // don't need the complexity of getEntry() to ensure that the entry is
        // not added to or removed from the cache before we have locked
        // it. Just call get() which is cheaper.
        CacheEntry entry = cache.get(key);

        entry.lock();
        try {
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(item == entry.getCacheable());
            }
            entry.unkeepForRemove();
            item.clean(true);
            removeEntry(key);
        } finally {
            entry.unlock();
        }
    }

    /**
     * Clean all dirty objects in the cache. All objects that existed in the
     * cache at the time of the call will be cleaned. Objects added later may
     * or may not be cleaned.
     */
    public void cleanAll() throws StandardException {
        cleanCache(null);
    }

    /**
     * Clean all dirty objects matching a partial key.
     *
     * @param partialKey the partial (or exact) key to match
     */
    public void clean(Matchable partialKey) throws StandardException {
        cleanCache(partialKey);
    }

    /**
     * Clean all dirty objects matching a partial key. If no key is specified,
     * clean all dirty objects in the cache.
     *
     * @param partialKey the partial (or exact) key to match, or
     * <code>null</code> to match all keys
     */
    private void cleanCache(Matchable partialKey) throws StandardException {
        for (CacheEntry entry : cache.values()) {
            final Cacheable dirtyObject;
            entry.lock();
            try {
                if (!entry.isValid()) {
                    // no need to clean an invalid entry
                    continue;
                }
                Cacheable c = entry.getCacheable();
                if (partialKey != null && !partialKey.match(c.getIdentity())) {
                    // don't clean objects that don't match the partial key
                    continue;
                }
                if (!c.isDirty()) {
                    // already clean
                    continue;
                }

                // Increment the keep count for this entry to prevent others
                // from removing it. Then release the lock on the entry to
                // avoid blocking others when the object is cleaned.
                entry.keep(false);
                dirtyObject = c;

            } finally {
                entry.unlock();
            }

            // Clean the object and decrement the keep count.
            cleanAndUnkeepEntry(entry, dirtyObject);
        }
    }

    /**
     * Clean an entry in the cache.
     *
     * @param entry the entry to clean
     * @exception StandardException if an error occurs while cleaning
     */
    void cleanEntry(CacheEntry entry) throws StandardException {
        // Fetch the cacheable while having exclusive access to the entry.
        // Release the lock before cleaning to avoid blocking others.
        Cacheable item;
        entry.lock();
        try {
            item = entry.getCacheable();
            if (item == null) {
                // nothing to do
                return;
            }
            entry.keep(false);
        } finally {
            entry.unlock();
        }
        cleanAndUnkeepEntry(entry, item);
    }

    /**
     * Clean an entry in the cache and decrement its keep count. The entry must
     * be kept before this method is called, and it must contain the specified
     * <code>Cacheable</code>.
     *
     * @param entry the entry to clean
     * @param item the cached object contained in the entry
     * @exception StandardException if an error occurs while cleaning
     */
    void cleanAndUnkeepEntry(CacheEntry entry, Cacheable item)
            throws StandardException {
        try {
            // Clean the cacheable while we're not holding
            // the lock on its entry.
            item.clean(false);
        } finally {
            // Re-obtain the lock on the entry, and reduce the keep count
            // since the entry should not be kept by the cleaner any longer.
            entry.lock();
            try {
                if (SanityManager.DEBUG) {
                    // Since the entry is kept, the Cacheable shouldn't
                    // have changed.
                    SanityManager.ASSERT(entry.getCacheable() == item,
                            "CacheEntry didn't contain the expected Cacheable");
                }
                entry.unkeep();
            } finally {
                entry.unlock();
            }
        }
    }

    /**
     * Remove all objects that are not kept and not dirty.
     */
    public void ageOut() {
        for (CacheEntry entry : cache.values()) {
            entry.lock();
            try {
                // never remove kept entries
                if (!entry.isKept()) {
                    Cacheable c = entry.getCacheable();
                    // If c is null, it's not in the cache and there's no need
                    // to remove it. If c is dirty, we can't remove it yet.
                    if (c != null && !c.isDirty()) {
                        removeEntry(c.getIdentity());
                    }
                }
            } finally {
                entry.unlock();
            }
        }
    }

    /**
     * Shut down the cache.
     */
    public void shutdown() throws StandardException {
        stopped = true;
        cleanAll();
        ageOut();
        if (cleaner != null) {
            cleaner.unsubscribe();
        }
        deregisterMBean();
    }

    /**
     * Specify a daemon service that can be used to perform operations in
     * the background. Callers must provide enough synchronization so that
     * they have exclusive access to the cache when this method is called.
     *
     * @param daemon the daemon service to use
     */
    public void useDaemonService(DaemonService daemon) {
        if (cleaner != null) {
            cleaner.unsubscribe();
        }
        // Create a background cleaner that can queue up 1/10 of the elements
        // in the cache.
        cleaner = new BackgroundCleaner(this, daemon, Math.max(maxSize/10, 1));
    }

    BackgroundCleaner getBackgroundCleaner() {
        return cleaner;
    }

    /**
     * Discard all unused objects that match a partial key. Dirty objects will
     * not be cleaned before their removal.
     *
     * @param partialKey the partial (or exact) key, or <code>null</code> to
     * match all keys
     * @return <code>true</code> if all matching objects were removed,
     * <code>false</code> otherwise
     */
    public boolean discard(Matchable partialKey) {
        boolean allRemoved = true;
        for (CacheEntry entry : cache.values()) {
            entry.lock();
            try {
                Cacheable c = entry.getCacheable();
                if (c == null) {
                    // not in the cache - no need to remove it
                    continue;
                }
                if (partialKey != null && !partialKey.match(c.getIdentity())) {
                    // not a match, don't remove it
                    continue;
                }
                if (entry.isKept()) {
                    // still in use, don't remove it
                    allRemoved = false;
                    continue;
                }
                removeEntry(c.getIdentity());
            } finally {
                entry.unlock();
            }
        }
        return allRemoved;
    }

    /**
     * Return a collection view of all the <code>Cacheable</code>s in the
     * cache. There is no guarantee that the objects in the collection can be
     * accessed in a thread-safe manner once this method has returned, so it
     * should only be used for diagnostic purposes. (Currently, it is only used
     * by the <code>StatementCache</code> VTI.)
     *
     * @return a collection view of the objects in the cache
     */
    public Collection<Cacheable> values() {
        ArrayList<Cacheable> values = new ArrayList<Cacheable>();
        for (CacheEntry entry : cache.values()) {
            entry.lock();
            try {
                Cacheable c = entry.getCacheable();
                if (c != null) {
                    values.add(c);
                }
            } finally {
                entry.unlock();
            }
        }
        return values;
    }

    @Override
    public void registerMBean(String dbName) throws StandardException {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(mbean == null, "registerMBean() called twice");
        }

        ManagementService managementService =
                (ManagementService) getSystemModule(Module.JMX);

        if (managementService != null) {
            mbean = managementService.registerMBean(
                    new ConcurrentCacheMBeanImpl(this),
                    CacheManagerMBean.class,
                    "type=CacheManager,name=" + name +
                    ",db=" + managementService.quotePropertyValue(dbName));
        }
    }

    @Override
    public void deregisterMBean() {
        if (mbean != null) {
            ManagementService managementService =
                (ManagementService) getSystemModule(Module.JMX);
            if (managementService != null) {
                managementService.unregisterMBean(mbean);
            }
            mbean = null;
        }
    }

    /** Count a cache hit. */
    private void countHit() {
        if (collectAccessCounts) {
            hits.getAndIncrement();
        }
    }

    /** Count a cache miss. */
    private void countMiss() {
        if (collectAccessCounts) {
            misses.getAndIncrement();
        }
    }

    /** Count an eviction from the cache. */
    private void countEviction() {
        if (collectAccessCounts) {
            evictions.getAndIncrement();
        }
    }

    /** Enable or disable collection of hit/miss/eviction counts. */
    void setCollectAccessCounts(boolean collect) {
        collectAccessCounts = collect;
    }

    /** Check if collection of hit/miss/eviction counts is enabled. */
    boolean getCollectAccessCounts() {
        return collectAccessCounts;
    }

    /** Get the number of cache hits. */
    long getHitCount() {
        return hits.get();
    }

    /** Get the number of cache misses. */
    long getMissCount() {
        return misses.get();
    }

    /** Get the number of evictions from the cache. */
    long getEvictionCount() {
        return evictions.get();
    }

    /** Get the maximum number of entries in the cache. */
    long getMaxEntries() {
        return maxSize;
    }

    /** Get the number of allocated entries in the cache. */
    long getAllocatedEntries() {
        return replacementPolicy.size();
    }

    /** Get the number of cached objects. */
    long getUsedEntries() {
        return cache.size();
    }
    
    /**
     * Privileged module lookup. Must be private so that user code
     * can't call this entry point.
     */
    private static  Object getSystemModule( final String factoryInterface )
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<Object>()
             {
                 public Object run()
                 {
                     return Monitor.getSystemModule( factoryInterface );
                 }
             }
             );
    }

}
