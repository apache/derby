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
 * this class, except <code>lock()</code>, it must first have called
 * <code>lock()</code> to ensure exclusive access to the entry.
 *
 * <p>
 *
 * When no thread holds the lock on the entry, it must be in one of the
 * following states:
 *
 * <dl>
 *
 * <dt>Uninitialized</dt> <dd>The entry object has just been constructed, but
 * has not yet been initialized. In this state, <code>isValid()</code> returns
 * <code>false</code>, whereas <code>isKept()</code> returns <code>true</code>
 * in order to prevent removal of the entry until it has been initialized.
 * When the entry is in this state, calls to
 * <code>lockWhenIdentityIsSet()</code> will block until
 * <code>settingIdentityComplete()</code> has been called.</dd>
 *
 * <dt>Unkept</dt> <dd>In this state, the entry object contains a reference to
 * a <code>Cacheable</code> and the keep count is zero. <code>isValid()</code>
 * returns <code>true</code> and <code>isKept()</code> returns
 * <code>false</code> in this state. <code>getCacheable()</code> returns a
 * non-null value.<dd>
 *
 * <dt>Kept</dt> <dd>Same as the unkept state, except that the keep count is
 * positive and <code>isKept()</code> returns <code>true</code>.</dd>
 *
 * <dt>Removed</dt> <dd>The entry has been removed from the cache. In this
 * state, <code>isValid()</code> and <code>isKept()</code> return
 * <code>false</code>, and <code>getCacheable()</code> returns
 * <code>null</code>. When an entry has entered the removed state, it cannot be
 * transitioned back to any of the other states.</dd>
 *
 * </dl>
 *
 * <p>
 *
 * To prevent deadlocks, each thread should normally lock only one entry at a
 * time. In some cases it is legitimate to hold the lock on two entries, for
 * instance if an entry must be evicted to make room for a new entry. If this
 * is the case, exactly one of the two entries must be in the uninitialized
 * state, and the uninitialized entry must be locked before the lock on the
 * other entry can be requested.
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
     * Condition variable used to notify a thread that the setting of this
     * entry's identity is complete. This variable is non-null when the object
     * is created, and will be set to null when the identity has been set.
     * @see #settingIdentityComplete()
     */
    private Condition settingIdentity = mutex.newCondition();

    /**
     * Callback object used to notify the replacement algorithm about events on
     * the cached objects (like accesses and requests for removal).
     */
    private ReplacementPolicy.Callback callback;

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
     * Block until this entry's cacheable has been initialized (that is, until
     * {@code settingIdentityComplete()} has been called on this object). If
     * the cacheable has been initialized before this method is called, it will
     * return immediately. The entry must have been locked for exclusive access
     * before this method is called. If the method needs to wait, it will
     * release the lock and reobtain it when it wakes up again.
     */
    void waitUntilIdentityIsSet() {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(mutex.isHeldByCurrentThread());
        }
        while (settingIdentity != null) {
            settingIdentity.awaitUninterruptibly();
        }
    }

    /**
     * Give up exclusive access.
     */
    void unlock() {
        mutex.unlock();
    }

    /**
     * Notify this entry that the initialization of its cacheable has been
     * completed. This method should be called after
     * <code>Cacheable.setIdentity()</code> or
     * <code>Cacheable.createIdentity()</code> has been called.
     */
    void settingIdentityComplete() {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(mutex.isHeldByCurrentThread());
        }
        settingIdentity.signalAll();
        settingIdentity = null;
    }

    /**
     * Increase the keep count for this entry. An entry which is kept cannot be
     * removed from the cache.
     *
     * @param accessed if <code>true</code>, notify the entry's callback object
     * that it has been accessed (normally because of calls to create, find or
     * findCached); otherwise, don't notify the callback object
     */
    void keep(boolean accessed) {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(mutex.isHeldByCurrentThread());
        }
        keepCount++;
        if (accessed) {
            callback.access();
        }
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
     * Check whether this entry holds a valid object. That is, it must hold
     * a non-null <code>Cacheable</code> and have completed setting its
     * identity.
     *
     * @return <code>true</code> if the entry holds a valid object
     */
    boolean isValid() {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(mutex.isHeldByCurrentThread());
        }
        return (settingIdentity == null) && (cacheable != null);
    }

    /**
     * Set the callback object used to notify the replacement algorithm about
     * actions performed on the cached object.
     *
     * @param cb the callback object
     */
    void setCallback(ReplacementPolicy.Callback cb) {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(mutex.isHeldByCurrentThread());
        }
        callback = cb;
    }

    /**
     * Clear this entry and notify the replacement algorithm that the
     * <code>Cacheable</code> can be reused.
     */
    void free() {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(mutex.isHeldByCurrentThread());
        }
        if (callback != null) {
            // The entry was inserted into the ReplacementPolicy before
            // removal. Now we need to mark it as free.
            callback.free();
        }
        cacheable = null;
    }
}
