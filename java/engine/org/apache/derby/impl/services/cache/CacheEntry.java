/*

   Derby - Class org.apache.derby.impl.services.cache.CacheEntry

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

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.derby.iapi.services.cache.Cacheable;
import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * Class representing an entry in the cache. It is used by
 * <code>ConcurrentCache</code>. When a thread invokes any of the methods in
 * this class, except <code>lock()</code>, it must have called
 * <code>lock()</code> to ensure exclusive access to the entry. No thread
 * should ever lock more than one entry in order to prevent deadlocks.
 */
final class CacheEntry {
    /** Mutex which guards the internal state of the entry. */
    private final ReentrantLock mutex = new ReentrantLock();
    /**
     * The cached object. If it is null, it means that the entry is invalid
     * (either uninitialized or removed).
     */
    private Cacheable cacheable;
    /** How many threads are currently keeping this entry. */
    private int keepCount;
    /**
     * Condition variable used to notify a thread that it is allowed to remove
     * the entry from the cache. If it is null, there is no thread waiting for
     * the entry to be unkept.
     */
    private Condition forRemove;

    /**
     * Block until the current thread is granted exclusive access to the entry.
     */
    void lock() {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(!mutex.isHeldByCurrentThread());
        }
        mutex.lock();
    }

    /**
     * Give up exclusive access.
     */
    void unlock() {
        mutex.unlock();
    }

    /**
     * Increase the keep count for this entry. An entry which is kept cannot be
     * removed from the cache.
     */
    void keep() {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(mutex.isHeldByCurrentThread());
        }
        keepCount++;
    }

    /**
     * Decrement the keep count for this entry. An entry cannot be removed from
     * the cache until its keep count is zero.
     */
    void unkeep() {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(isKept());
        }
        keepCount--;
        if (forRemove != null && keepCount == 1) {
            // This entry is only kept by the thread waiting in
            // unkeepForRemove(). Signal that the entry can be removed.
            forRemove.signal();
        }
    }

    /**
     * Check whether or not this entry is kept.
     *
     * @return <code>true</code> if the object is kept
     */
    boolean isKept() {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(mutex.isHeldByCurrentThread());
            SanityManager.ASSERT(keepCount >= 0);
        }
        return keepCount > 0;
    }

    /**
     * Unkeep the entry and wait until no other thread is keeping it. This
     * method is used when a thread requests the removal of the entry. As
     * defined by the contract of <code>CacheManager.remove()</code>, it is the
     * responsibility of the caller to ensure that only a single thread
     * executes this method on an object.
     *
     * @see org.apache.derby.iapi.services.cache.CacheManager#remove
     */
    void unkeepForRemove() {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(isKept());
            SanityManager.ASSERT(forRemove == null);
        }
        if (keepCount > 1) {
            forRemove = mutex.newCondition();
            while (keepCount > 1) {
                forRemove.awaitUninterruptibly();
            }
            forRemove = null;
        }
        keepCount--;
    }

    /**
     * Set the cached object held by this entry.
     *
     * @param c a cacheable, or <code>null</code> if the entry is about to be
     * removed
     */
    void setCacheable(Cacheable c) {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(mutex.isHeldByCurrentThread());
        }
        cacheable = c;
    }

    /**
     * Return the cached object held by this entry.
     *
     * @return the cached object in this entry
     */
    Cacheable getCacheable() {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(mutex.isHeldByCurrentThread());
        }
        return cacheable;
    }

    /**
     * Check whether this entry holds a valid object.
     *
     * @return <code>true</code> if the entry holds a valid object
     */
    boolean isValid() {
        return getCacheable() != null;
    }
}
