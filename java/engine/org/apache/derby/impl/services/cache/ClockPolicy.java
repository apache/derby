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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.cache.Cacheable;
import org.apache.derby.shared.common.sanity.SanityManager;

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

    /**
     * The minimum number of items to check before we decide to give up
     * looking for evictable entries when rotating the clock.
     */
    private static final int MIN_ITEMS_TO_CHECK = 20;

    /**
     * How large part of the clock to look at before giving up in
     * {@code rotateClock()}.
     */
    private static final float MAX_ROTATION = 0.2f;

    /**
     * How large part of the clock to look at before giving up finding
     * an evictable entry in {@code shrinkMe()}.
     */
    private static final float PART_OF_CLOCK_FOR_SHRINK = 0.1f;

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
    private final ArrayList<Holder> clock;

    /** The current position of the clock hand. */
    private int hand;

    /**
     * The number of free entries. This is the number of objects that have been
     * removed from the cache and whose entries are free to be reused without
     * eviction.
     */
    private final AtomicInteger freeEntries = new AtomicInteger();

    /**
     * Tells whether there currently is a thread in the {@code doShrink()}
     * method. If this variable is {@code true} a call to {@code doShrink()}
     * will be a no-op.
     */
    private final AtomicBoolean isShrinking = new AtomicBoolean();

    /**
     * Create a new <code>ClockPolicy</code> instance.
     *
     * @param cacheManager the cache manager that requests this policy
     * @param initialSize the initial capacity of the cache
     * @param maxSize the maximum size of the cache
     */
    ClockPolicy(ConcurrentCache cacheManager, int initialSize, int maxSize) {
        this.cacheManager = cacheManager;
        this.maxSize = maxSize;
        clock = new ArrayList<Holder>(initialSize);
    }

    @Override
    public int size() {
        synchronized (clock) {
            return clock.size();
        }
    }

    /**
     * Insert an entry into the cache. If the maximum size is exceeded, evict a
     * <em>not recently used</em> object from the cache. If there are no
     * entries available for reuse, increase the size of the cache.
     *
     * @param entry the entry to insert (must be locked)
     * @exception StandardException if an error occurs when inserting the entry
     */
    public void insertEntry(CacheEntry entry) throws StandardException {

        final int size;
        synchronized (clock) {
            size = clock.size();
            if (size < maxSize) {
                if (freeEntries.get() == 0) {
                    // We have not reached the maximum size yet, and there's no
                    // free entry to reuse. Make room by growing.
                    clock.add(new Holder(entry));
                    return;
                }
            }
        }

        if (size > maxSize) {
            // Maximum size is exceeded. Shrink the clock in the background
            // cleaner, if we have one; otherwise, shrink it in the current
            // thread.
            BackgroundCleaner cleaner = cacheManager.getBackgroundCleaner();
            if (cleaner != null) {
                cleaner.scheduleShrink();
            } else {
                doShrink();
            }
        }

        // Rotate the clock hand (look at up to 20% of the cache) and try to
        // find free space for the entry. Only allow evictions if the cache
        // has reached its maximum size. Otherwise, we only look for invalid
        // entries and rather grow the cache than evict valid entries.
        Holder h = rotateClock(entry, size >= maxSize);

        if (h == null) {
            // didn't find a victim, so we need to grow
            synchronized (clock) {
                clock.add(new Holder(entry));
            }
        }
    }

    /**
     * Holder class which represents an entry in the cache. It maintains a
     * <code>recentlyUsed</code> required by the clock algorithm. The class
     * also implements the <code>Callback</code> interface, so that
     * <code>ConcurrentCache</code> can notify the clock policy about events
     * relevant to the clock algorithm.
     */
    private class Holder implements Callback {
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

        /**
         * Flag which tells whether this holder has been evicted from the
         * clock. If it has been evicted, it can't be reused when a new entry
         * is inserted. Only the owner of this holder's monitor is allowed to
         * access this variable.
         */
        private boolean evicted;

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
            // let others know that a free entry is available
            int free = freeEntries.incrementAndGet();
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(
                    free > 0,
                    "freeEntries should be greater than 0, but is " + free);
            }
        }

        /**
         * Associate this holder with the specified entry if the holder is free
         * (that is, not associated with any other entry).
         *
         * @param e the entry to associate the holder with (it must be locked
         * by the current thread)
         * @return <code>true</code> if the holder has been associated with the
         * specified entry, <code>false</code> if someone else has taken it or
         * the holder has been evicted from the clock
         */
        synchronized boolean takeIfFree(CacheEntry e) {
            if (entry == null && !evicted) {
                // the holder is free - take it!
                int free = freeEntries.decrementAndGet();
                if (SanityManager.DEBUG) {
                    SanityManager.ASSERT(
                        free >= 0, "freeEntries is negative: " + free);
                }
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

        /**
         * Evict this holder from the clock if it is not associated with an
         * entry.
         *
         * @return <code>true</code> if the holder was successfully evicted,
         * <code>false</code> otherwise
         */
        synchronized boolean evictIfFree() {
            if (entry == null && !evicted) {
                int free = freeEntries.decrementAndGet();
                if (SanityManager.DEBUG) {
                    SanityManager.ASSERT(
                        free >= 0, "freeEntries is negative: " + free);
                }
                evicted = true;
                return true;
            }
            return false;
        }

        /**
         * Mark this holder as evicted from the clock, effectively preventing
         * reuse of the holder. Calling thread must have locked the holder's
         * entry.
         */
        synchronized void setEvicted() {
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(!evicted, "Already evicted");
            }
            evicted = true;
            entry = null;
        }

        /**
         * Check whether this holder has been evicted from the clock.
         *
         * @return <code>true</code> if it has been evicted, <code>false</code>
         * otherwise
         */
        synchronized boolean isEvicted() {
            return evicted;
        }
    }

    /**
     * Get the holder under the clock hand, and move the hand to the next
     * holder.
     *
     * @return the holder under the clock hand, or {@code null} if the clock is
     * empty
     */
    private Holder moveHand() {
        synchronized (clock) {
            if (clock.isEmpty()) {
                return null;
            }
            if (hand >= clock.size()) {
                hand = 0;
            }
            return clock.get(hand++);
        }
    }

    /**
     * Rotate the clock in order to find a free space for a new entry. If
     * <code>allowEvictions</code> is <code>true</code>, an not recently used
     * object might be evicted to make room for the new entry. Otherwise, only
     * unused entries are searched for. When evictions are allowed, entries are
     * marked as not recently used when the clock hand sweeps over them. The
     * search stops when a reusable entry is found, or when more than a certain
     * percentage of the entries have been visited. If there are
     * free (unused) entries, the search will continue until a reusable entry
     * is found, regardless of how many entries that need to be checked.
     *
     * @param entry the entry to insert
     * @param allowEvictions tells whether evictions are allowed (normally
     * <code>true</code> if the cache is full and <code>false</code> otherwise)
     * @return a holder that we can reuse, or <code>null</code> if we didn't
     * find one
     */
    private Holder rotateClock(CacheEntry entry, boolean allowEvictions)
            throws StandardException {

        // Calculate how many items we need to check before we give up
        // finding an evictable one. If we don't allow evictions, none should
        // be checked (however, we may search for unused entries in the loop
        // below).
        int itemsToCheck = 0;
        if (allowEvictions) {
            synchronized (clock) {
                itemsToCheck = Math.max(MIN_ITEMS_TO_CHECK,
                                        (int) (clock.size() * MAX_ROTATION));
            }
        }

        // Check up to itemsToCheck entries before giving up, but don't give up
        // if we know there are unused entries.
        while (itemsToCheck-- > 0 || freeEntries.get() > 0) {

            final Holder h = moveHand();

            if (h == null) {
                // There are no elements in the clock, hence there is no
                // reusable entry.
                return null;
            }

            final CacheEntry e = h.getEntry();

            if (e == null) {
                if (h.takeIfFree(entry)) {
                    return h;
                }
                // Someone else grabbed this entry between the calls to
                // getEntry() and takeIfFree(). Just move on to the next entry.
                continue;
            }

            if (!allowEvictions) {
                // Evictions are not allowed, so we can't reuse this entry.
                continue;
            }

            // This variable will hold a dirty cacheable that should be cleaned
            // after the try/finally block.
            final Cacheable dirty;

            e.lock();
            try {
                if (!isEvictable(e, h, true)) {
                    continue;
                }

                // The entry is not in use, and has not been used for at least
                // one round on the clock. See if it needs to be cleaned.
                Cacheable c = e.getCacheable();
                if (!c.isDirty()) {
                    // Not in use and not dirty. Take over the holder.
                    h.switchEntry(entry);
                    cacheManager.evictEntry(c.getIdentity());
                    return h;
                }

                // Ask the background cleaner to clean the entry.
                BackgroundCleaner cleaner = cacheManager.getBackgroundCleaner();
                if (cleaner != null && cleaner.scheduleClean(e)) {
                    // Successfully scheduled the clean operation. We can't
                    // evict it until the clean operation has finished. Since
                    // we'd like to be as responsive as possible, move on to
                    // the next entry instead of waiting for the clean
                    // operation to finish.
                    continue;
                }

                // There is no background cleaner, or the background cleaner
                // has no free capacity. Let's clean the object ourselves.
                // First, mark the entry as kept to prevent eviction until
                // we have cleaned it, but don't mark it as accessed (recently
                // used).
                e.keep(false);
                dirty = c;

            } finally {
                e.unlock();
            }

            // Clean the entry and unkeep it.
            cacheManager.cleanAndUnkeepEntry(e, dirty);

            // If no one has touched the entry while we were cleaning it, we
            // could reuse it at this point. The old buffer manager (Clock)
            // would however under high load normally move on to the next
            // entry in the clock instead of reusing the one it recently
            // cleaned. Some of the performance tests performed as part of
            // DERBY-2911 indicated that not reusing the entry that was just
            // cleaned made the replacement algorithm more efficient. For now
            // we try to stay as close to the old buffer manager as possible
            // and don't reuse the entry immediately.
        }

        return null;
    }

    /**
     * Check if an entry can be evicted. Only entries that still are present in
     * the cache, are not kept and not recently used, can be evicted. This
     * method does not check whether the {@code Cacheable} contained in the
     * entry is dirty, so it may be necessary to clean it before an eviction
     * can take place even if the method returns {@code true}. The caller must
     * hold the lock on the entry before calling this method.
     *
     * @param e the entry to check
     * @param h the holder which holds the entry
     * @param clearRecentlyUsedFlag tells whether or not the recently used flag
     * should be cleared on the entry ({@code true} only when called as part of
     * a normal clock rotation)
     * @return whether or not this entry can be evicted (provided that its
     * {@code Cacheable} is cleaned first)
     */
    private boolean isEvictable(CacheEntry e, Holder h,
                                boolean clearRecentlyUsedFlag) {

        if (h.getEntry() != e) {
            // Someone else evicted this entry before we obtained the
            // lock, so we can't evict it.
            return false;
        }

        if (e.isKept()) {
            // The entry is in use and cannot be evicted.
            return false;
        }

        if (SanityManager.DEBUG) {
            // At this point the entry must be valid. If it's not, it's either
            // removed (in which case getEntry() != e and we shouldn't get
            // here), or it is setting its identity (in which case it is kept
            // and we shouldn't get here).
            SanityManager.ASSERT(e.isValid(), "Holder contains invalid entry");
            SanityManager.ASSERT(!h.isEvicted(), "Holder is evicted");
        }

        if (h.recentlyUsed) {
            // The object has been used recently, so it cannot be evicted.
            if (clearRecentlyUsedFlag) {
                h.recentlyUsed = false;
            }
            return false;
        }

        return true;
    }

    /**
     * Remove the holder at the given clock position.
     *
     * @param pos position of the holder
     * @param h the holder to remove
     */
    private void removeHolder(int pos, Holder h) {
        synchronized (clock) {
            Holder removed = clock.remove(pos);
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(removed == h, "Wrong Holder removed");
            }
        }
    }

    /**
     * Try to shrink the clock if it's larger than its maximum size.
     */
    public void doShrink() {
        // If we're already performing a shrink, ignore this request. We'll get
        // a new call later by someone else if the current shrink operation is
        // not enough. If we manage to change isShrinking atomically from false
        // to true, no one else is currently inside shrinkMe(), and others will
        // be blocked from entering it until we reset isShrinking to false.
        if (isShrinking.compareAndSet(false, true)) {
            try {
                shrinkMe();
            } finally {
                // allow others to call shrinkMe()
                isShrinking.set(false);
            }
        }
    }

    /**
     * Perform the shrinking of the clock. This method should only be called
     * by a single thread at a time.
     */
    private void shrinkMe() {

        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(isShrinking.get(),
                    "Called shrinkMe() without ensuring exclusive access");
        }

        // Max number of candidates to look at (always at least 1).
        int maxLooks = Math.max(1, (int) (maxSize * PART_OF_CLOCK_FOR_SHRINK));

        // Since we don't scan the entire cache, start at the clock hand so
        // that we don't always scan the first 10% of the cache.
        int pos;
        synchronized (clock) {
            pos = hand;
        }

        while (maxLooks-- > 0) {

            final Holder h;
            final int size;

            // Fetch the next holder from the clock.
            synchronized (clock) {
                size = clock.size();
                if (pos >= size) {
                    pos = 0;
                }
                h = clock.get(pos);
            }

            // The index of the holder we're looking at. Since no one else than
            // us can remove elements from the clock while we're in this
            // method, and new elements will be added at the end of the list,
            // the index for a holder does not change until we remove it.
            final int index = pos;

            // Let pos point at the index of the holder we'll look at in the
            // next iteration.
            pos++;

            // No need to shrink if the size isn't greater than maxSize.
            if (size <= maxSize) {
                break;
            }

            final CacheEntry e = h.getEntry();

            if (e == null) {
                // The holder does not hold an entry. Try to remove it.
                if (h.evictIfFree()) {
                    removeHolder(index, h);
                    // move position back because of the removal so that we
                    // don't skip one clock element
                    pos = index;
                }
                // Either the holder was evicted, or someone else took it
                // before we could evict it. In either case, we should move on
                // to the next holder.
                continue;
            }

            e.lock();
            try {
                if (!isEvictable(e, h, false)) {
                    continue;
                }

                final Cacheable c = e.getCacheable();
                if (c.isDirty()) {
                    // Don't evict dirty entries.
                    continue;
                }

                // mark as evicted to prevent reuse
                h.setEvicted();

                // remove from cache manager
                cacheManager.evictEntry(c.getIdentity());

                // remove from clock
                removeHolder(index, h);

                // move position back because of the removal so that we don't
                // skip one clock element
                pos = index;

            } finally {
                e.unlock();
            }
        }
    }
}
