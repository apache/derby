/*

   Derby - Class org.apache.derby.impl.services.cache.ClockPolicy

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
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.cache.Cacheable;
import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * Implementation of a replacement policy which uses the clock algorithm. All
 * the cache entries are stored in a circular buffer, called the clock. There
 * is also a clock hand which points to one of the entries in the clock. Each
 * time an entry is accessed, it is marked as recently used. If a new entry is
 * inserted into the cache and the cache is full, the clock hand is moved until
 * it is over a not recently used entry, and that entry is evicted to make
 * space for the new entry. Each time the clock hand sweeps over a recently
 * used entry, it is marked as not recently used, and it will be a candidate
 * for removal the next time the clock hand sweeps over it, unless it has been
 * marked as recently used in the meantime.
 *
 * <p>
 *
 * To allow concurrent access from multiple threads, the methods in this class
 * need to synchronize on a number of different objects:
 *
 * <ul>
 *
 * <li><code>CacheEntry</code> objects must be locked before they can be
 * used</li>
 *
 * <li>accesses to the clock structure (circular buffer + clock hand) should be
 * synchronized on the <code>ArrayList</code> representing the circular
 * buffer</li>
 *
 * <li>accesses to individual <code>Holder</code> objects in the clock
 * structure should be protected by synchronizing on the holder</li>
 *
 * </ul>
 *
 * To avoid deadlocks, we need to ensure that all threads obtain
 * synchronization locks in the same order. <code>CacheEntry</code>'s class
 * javadoc dictates the order when locking <code>CacheEntry</code>
 * objects. Additionally, we require that no thread should obtain any other
 * synchronization locks while it is holding a synchronization lock on the
 * clock structure or on a <code>Holder</code> object. The threads are however
 * allowed to obtain synchronization locks on the clock structure or on a
 * holder while they are locking one or more <code>CacheEntry</code> objects.
 */
final class ClockPolicy implements ReplacementPolicy {

    /** The cache manager for which this replacement policy is used. */
    private final ConcurrentCache cacheManager;

    /**
     * The maximum size of the cache. When this size is exceeded, entries must
     * be evicted before new ones are inserted.
     */
    private final int maxSize;

    /**
     * The circular clock buffer which holds all the entries in the
     * cache. Accesses to <code>clock</code> and <code>hand</code> must be
     * synchronized on <code>clock</code>.
     */
    private final ArrayList<Holder> clock = new ArrayList<Holder>();

    /** The current position of the clock hand. */
    private int hand;

    /**
     * Create a new <code>ClockPolicy</code> instance.
     *
     * @param cacheManager the cache manager that requests this policy
     * @param maxSize the maximum size of the cache
     */
    ClockPolicy(ConcurrentCache cacheManager, int maxSize) {
        this.cacheManager = cacheManager;
        this.maxSize = maxSize;
    }

    /**
     * Insert an entry into the cache. If the maximum size is exceeded, evict a
     * <em>not recently used</em> object from the cache. If there are no
     * entries available for reuse, increase the size of the cache.
     *
     * @param entry the entry to insert (must be locked)
     * @return callback object used by the cache manager
     * @exception StandardException if an error occurs when inserting the entry
     */
    public Callback insertEntry(CacheEntry entry) throws StandardException {
        synchronized (clock) {
            if (clock.size() < maxSize) {
                // TODO - check whether there are free entries that could be
                // used instead of growing
                return grow(entry);
            }
        }

        // rotate clock hand (look at up to 20% of the cache)
        Holder h = rotateClock(entry, (float) 0.2);
        if (h != null) {
            return h;
        }

        // didn't find a victim, so we need to grow
        synchronized (clock) {
            return grow(entry);
        }
    }

    /**
     * Holder class which represents an entry in the cache. It maintains a
     * <code>recentlyUsed</code> required by the clock algorithm. The class
     * also implements the <code>Callback</code> interface, so that
     * <code>ConcurrentCache</code> can notify the clock policy about events
     * relevant to the clock algorithm.
     */
    private static class Holder implements Callback {
        /**
         * Flag indicating whether or not this entry has been accessed
         * recently. Should only be accessed/modified when the current thread
         * has locked the <code>CacheEntry</code> object stored in the
         * <code>entry</code> field.
         */
        boolean recentlyUsed;

        /**
         * Reference to the <code>CacheEntry</code> object held by this
         * object. The reference should only be accessed when the thread owns
         * the monitor on this holder. A thread is only allowed to change the
         * reference if it also has locked the entry that the reference points
         * to (if the reference is non-null). This ensures that no other thread
         * can disassociate a holder from its entry while the entry is locked,
         * even though the monitor on the holder has been released.
         */
        private CacheEntry entry;

        /**
         * Cacheable object from a removed object. If this object is non-null,
         * <code>entry</code> must be <code>null</code> (which means that the
         * holder is not associated with any object in the cache).
         */
        private Cacheable freedCacheable;

        Holder(CacheEntry e) {
            entry = e;
            e.setCallback(this);
        }

        /**
         * Mark this entry as recently used. Caller must have locked
         * <code>entry</code>.
         */
        public void access() {
            recentlyUsed = true;
        }

        /**
         * Mark this object as free and reusable. Caller must have locked
         * <code>entry</code>.
         */
        public synchronized void free() {
            freedCacheable = entry.getCacheable();
            entry = null;
            recentlyUsed = false;
        }

        /**
         * Associate this holder with the specified entry if the holder is free
         * (that is, not associated with any other entry).
         *
         * @param e the entry to associate the holder with (it must be locked
         * by the current thread)
         * @return <code>true</code> if the holder has been associated with the
         * specified entry, <code>false</code> if someone else has taken it
         */
        synchronized boolean takeIfFree(CacheEntry e) {
            if (entry == null) {
                // the holder is free - take it!
                e.setCacheable(freedCacheable);
                e.setCallback(this);
                entry = e;
                freedCacheable = null;
                return true;
            }
            // someone else has taken it
            return false;
        }

        /**
         * Returns the entry that is currently associated with this holder.
         *
         * @return the associated entry
         */
        synchronized CacheEntry getEntry() {
            return entry;
        }

        /**
         * Switch which entry the holder is associated with. Will be called
         * when we evict an entry to make room for a new one. When this method
         * is called, the current thread must have locked both the entry that
         * is evicted and the entry that is inserted.
         *
         * @param e the entry to associate this holder with
         */
        synchronized void switchEntry(CacheEntry e) {
            e.setCallback(this);
            e.setCacheable(entry.getCacheable());
            entry = e;
        }
    }

    /**
     * Get the holder under the clock hand, and move the hand to the next
     * holder.
     *
     * @return the holder under the clock hand
     */
    private Holder moveHand() {
        synchronized (clock) {
            if (hand >= clock.size()) {
                hand = 0;
            }
            return clock.get(hand++);
        }
    }

    /**
     * Increase the size of the clock by one and return a new holder. The
     * caller must be synchronized on <code>clock</code>.
     *
     * @param entry the entry to insert into the clock
     * @return a new holder which wraps the entry
     */
    private Holder grow(CacheEntry entry) {
        Holder h = new Holder(entry);
        clock.add(h);
        return h;
    }

    /**
     * Rotate the clock in order to find a free space for a new entry, or a
     * <em>not recently used</em> entry that we can evict to make free
     * space. Entries that we move past are marked as recently used.
     *
     * @param entry the entry to insert
     * @param partOfClock how large part of the clock to look at before we give
     * up
     * @return a holder that we can reuse, or <code>null</code> if we didn't
     * find one
     */
    private Holder rotateClock(CacheEntry entry, float partOfClock)
            throws StandardException {

        // calculate how many items to check
        int itemsToCheck;
        synchronized (clock) {
            itemsToCheck = clock.size();
            if (itemsToCheck < 20) {
                // if we have a very small cache, allow two rounds before
                // giving up
                itemsToCheck *= 2;
            } else {
                // otherwise, just check a fraction of the clock
                itemsToCheck *= partOfClock;
            }
        }

        while (itemsToCheck-- > 0) {

            final Holder h = moveHand();
            final CacheEntry e = h.getEntry();

            if (e == null) {
                if (h.takeIfFree(entry)) {
                    return h;
                }
                // Someone else grabbed this entry between the calls to
                // getEntry() and takeIfFree(). Just move on to the next entry.
                continue;
            }

            e.lock();
            try {
                if (h.getEntry() != e) {
                    // Someone else evicted this entry before we obtained the
                    // lock. Move on to the next entry.
                    continue;
                }

                if (SanityManager.DEBUG) {
                    // At this point the entry must be valid. Otherwise, it
                    // would have been removed from the Holder.
                    SanityManager.ASSERT(e.isValid());
                }

                if (e.isKept()) {
                    // The entry is in use. Move on to the next entry.
                    continue;
                }

                if (h.recentlyUsed) {
                    // The object has been used recently. Clear the
                    // recentlyUsed flag and move on to the next entry.
                    h.recentlyUsed = false;
                    continue;
                }

                // OK, we can use this Holder
                Cacheable c = e.getCacheable();
                if (c.isDirty()) {
                    c.clean(false);
                }

                h.switchEntry(entry);

                cacheManager.evictEntry(c.getIdentity());

                return h;

            } finally {
                e.unlock();
            }
        }

        return null;
    }
}
