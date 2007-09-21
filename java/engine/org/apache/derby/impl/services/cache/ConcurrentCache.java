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

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.cache.CacheManager;
import org.apache.derby.iapi.services.cache.Cacheable;
import org.apache.derby.iapi.services.cache.CacheableFactory;
import org.apache.derby.iapi.services.daemon.DaemonService;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.util.Matchable;

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

    /**
     * Flag that indicates whether this cache instance has been shut down. When
     * it has been stopped, <code>find()</code>, <code>findCached()</code> and
     * <code>create()</code> will return <code>null</code>. The flag is
     * declared <code>volatile</code> so that no synchronization is needed when
     * it is accessed by concurrent threads.
     */
    private volatile boolean stopped;

    /**
     * Creates a new cache manager.
     *
     * @param holderFactory factory which creates <code>Cacheable</code>s
     * @param name the name of the cache
     */
    ConcurrentCache(CacheableFactory holderFactory, String name) {
        cache = new ConcurrentHashMap<Object, CacheEntry>();
        this.holderFactory = holderFactory;
        this.name = name;
    }

    /**
     * Get the entry associated with the specified key from the cache. If the
     * entry does not exist, insert an empty one and return it. The returned
     * entry is always locked for exclusive access by the current thread, but
     * not kept.
     *
     * @param key the identity of the cached object
     * @return an entry for the specified key, always locked
     */
    private CacheEntry getEntry(Object key) {
        CacheEntry entry = cache.get(key);
        while (true) {
            if (entry != null) {
                // Found an entry in the cache. Lock it and validate that it's
                // still there.
                entry.lock();
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
     * Find a free cacheable and give the specified entry a pointer to it. If
     * a free cacheable cannot be found, allocate a new one. The entry must be
     * locked by the current thread.
     *
     * @param entry the entry for which a <code>Cacheable</code> is needed
     */
    private void findFreeCacheable(CacheEntry entry) {
        // TODO - When the replacement algorithm has been implemented, we
        // should reuse a cacheable if possible.
        entry.setCacheable(holderFactory.newCacheable(this));
    }

    /**
     * Remove an entry from the cache. Its <code>Cacheable</code> is cleared
     * and made available for other entries. This method should only be called
     * if the entry is locked by the current thread.
     *
     * @param key the identity of the entry to remove
     */
    private void removeEntry(Object key) {
        CacheEntry entry = cache.remove(key);
        Cacheable c = entry.getCacheable();
        if (c != null && c.getIdentity() != null) {
            // The cacheable should not have an identity when it has been
            // removed.
            entry.getCacheable().clearIdentity();
        }
        entry.setCacheable(null);
        // TODO - When replacement policy is implemented, return the
        // cacheable to the free list
    }

    /**
     * Initialize an entry by finding a free <code>Cacheable</code> and setting
     * its identity. If the identity is successfully set, the entry is kept and
     * the <code>Cacheable</code> is inserted into the entry and returned.
     * Otherwise, the entry is removed from the cache and <code>null</code>
     * is returned.
     *
     * @param entry the entry to initialize
     * @param key the identity to set
     * @param createParameter parameter to <code>createIdentity()</code>
     * (ignored if <code>create</code> is <code>false</code>)
     * @param create if <code>true</code>, create new identity with
     * <code>Cacheable.createIdentity()</code>; otherwise, set identity with
     * <code>Cacheable.setIdentity()</code>
     * @return a <code>Cacheable</code> if the identity could be set,
     * <code>null</code> otherwise
     * @exception StandardException if an error occured while searching for a
     * free <code>Cacheable</code> or while setting the identity
     * @see Cacheable#setIdentity(Object)
     * @see Cacheable#createIdentity(Object,Object)
     */
    private Cacheable initIdentity(CacheEntry entry,
            Object key, Object createParameter, boolean create)
            throws StandardException {
        Cacheable c = null;
        try {
            findFreeCacheable(entry);
            if (create) {
                c = entry.getCacheable().createIdentity(key, createParameter);
            } else {
                c = entry.getCacheable().setIdentity(key);
            }
        } finally {
            if (c == null) {
                // Either an exception was thrown, or setIdentity() or
                // createIdentity() returned null. In either case, the entry is
                // invalid and must be removed.
                removeEntry(key);
            }
        }

        // If we successfully set the identity, insert the cacheable and mark
        // the entry as kept.
        if (c != null) {
            entry.setCacheable(c);
            entry.keep();
        }
        return c;
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
        try {
            Cacheable item = entry.getCacheable();
            if (item != null) {
                entry.keep();
                return item;
            }
            // not currently in the cache
            return initIdentity(entry, key, null, false);
        } finally {
            entry.unlock();
        }
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

        // We don't want to insert it if it's not there, so there's no need to
        // use getEntry().
        CacheEntry entry = cache.get(key);
        if (entry == null) {
            // No such object was found in the cache.
            return null;
        }
        entry.lock();
        try {
            // Return the cacheable. If the entry was removed right before we
            // locked it, getCacheable() returns null and so should we do.
            Cacheable item = entry.getCacheable();
            if (item != null) {
                entry.keep();
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

        CacheEntry entry = getEntry(key);
        try {
            if (entry.isValid()) {
                throw StandardException.newException(
                    SQLState.OBJECT_EXISTS_IN_CACHE, name, key);
            }
            return initIdentity(entry, key, createParameter, true);
        } finally {
            entry.unlock();
        }
    }

    /**
     * Release an object that has been fetched from the cache with
     * <code>find()</code>, <code>findCached()</code> or <code>create()</code>.
     *
     * @param item a <code>Cacheable</code> value
     */
    public void release(Cacheable item) {
        // The entry must be present, so we don't need to call getEntry().
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
        // The entry must be present, so we don't need to call getEntry().
        Object key = item.getIdentity();
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
            entry.lock();
            try {
                Cacheable c = entry.getCacheable();
                if (c != null && c.isDirty() &&
                        (partialKey == null ||
                             partialKey.match(c.getIdentity()))) {
                    c.clean(false);
                }
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
        // TODO - unsubscribe background writer
        stopped = true;
        cleanAll();
        ageOut();
    }

    public void useDaemonService(DaemonService daemon) {
        // TODO
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
}
