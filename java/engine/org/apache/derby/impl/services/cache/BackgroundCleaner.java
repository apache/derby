/*

   Derby - Class org.apache.derby.impl.services.cache.BackgroundCleaner

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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.daemon.DaemonService;
import org.apache.derby.iapi.services.daemon.Serviceable;

/**
 * A background cleaner that {@code ConcurrentCache} can use to clean {@code
 * Cacheable}s asynchronously in a background instead of synchronously in the
 * user threads. It is normally used by the replacement algorithm in order to
 * make dirty {@code Cacheable}s clean and evictable in the future. When the
 * background cleaner is asked to clean an item, it puts the item in a queue
 * and requests to be serviced by a <code>DaemonService</code> running in a
 * separate thread.
 */
final class BackgroundCleaner implements Serviceable {

    /** The service thread which performs the clean operations. */
    private final DaemonService daemonService;

    /** Subscription number for this <code>Serviceable</code>. */
    private final int clientNumber;

    /**
     * Flag which tells whether the cleaner has a still unprocessed job
     * scheduled with the daemon service. If this flag is <code>true</code>,
     * calls to <code>serviceNow()</code> won't result in the cleaner being
     * serviced.
     */
    private final AtomicBoolean scheduled = new AtomicBoolean();

    /** A queue of cache entries that need to be cleaned. */
    private final ArrayBlockingQueue<CacheEntry> queue;

    /**
     * Flag which tells whether the cleaner should try to shrink the cache
     * the next time it wakes up.
     */
    private volatile boolean shrink;

    /** The cache manager owning this cleaner. */
    private final ConcurrentCache cacheManager;

    /**
     * Create a background cleaner instance and subscribe it to a daemon
     * service.
     *
     * @param cache the cache manager that owns the cleaner
     * @param daemon the daemon service which perfoms the work
     * @param queueSize the maximum number of entries to keep in the queue
     * (must be greater than 0)
     */
    BackgroundCleaner(
            ConcurrentCache cache, DaemonService daemon, int queueSize) {
        queue = new ArrayBlockingQueue<CacheEntry>(queueSize);
        daemonService = daemon;
        cacheManager = cache;
        // subscribe with the onDemandOnly flag
        clientNumber = daemon.subscribe(this, true);
    }

    /**
     * Try to schedule a clean operation in the background cleaner.
     *
     * @param entry the entry that needs to be cleaned
     * @return <code>true</code> if the entry has been scheduled for clean,
     * <code>false</code> if the background cleaner can't clean the entry (its
     * queue is full)
     */
    boolean scheduleClean(CacheEntry entry) {
        final boolean queued = queue.offer(entry);
        if (queued) {
            requestService();
        }
        return queued;
    }

    /**
     * Request that the cleaner tries to shrink the cache the next time it
     * wakes up.
     */
    void scheduleShrink() {
        shrink = true;
        requestService();
    }

    /**
     * Notify the daemon service that the cleaner needs to be serviced.
     */
    private void requestService() {
        // Calling serviceNow() doesn't have any effect if we have already
        // called it and the request hasn't been processed yet. Therefore, we
        // only call serviceNow() if we can atomically change scheduled from
        // false to true. If the cleaner is waiting for service (schedule is
        // true), we don't need to call serviceNow() since the cleaner will
        // re-request service when it finishes its current operation and
        // detects that there is more work in the queue.
        if (scheduled.compareAndSet(false, true)) {
            daemonService.serviceNow(clientNumber);
        }
    }

    /**
     * Stop subscribing to the daemon service.
     */
    void unsubscribe() {
        daemonService.unsubscribe(clientNumber);
    }

    /**
     * Clean the first entry in the queue. If there is more work, re-request
     * service from the daemon service.
     *
     * @param context ignored
     * @return status for the performed work (normally
     * <code>Serviceable.DONE</code>)
     * @throws StandardException if <code>Cacheable.clean()</code> fails
     */
    public int performWork(ContextManager context) throws StandardException {
        // allow others to schedule more work
        scheduled.set(false);

        // First, try to shrink the cache if requested.
        if (shrink) {
            shrink = false;
            cacheManager.getReplacementPolicy().doShrink();
        }

        // See if there are objects waiting to be cleaned.
        CacheEntry e = queue.poll();
        if (e != null) {
            try {
                cacheManager.cleanEntry(e);
            } finally {
                if (!queue.isEmpty() || shrink) {
                    // We have more work in the queue. Request service again.
                    requestService();
                }
            }
        }
        return Serviceable.DONE;
    }

    /**
     * Indicate that we want to be serviced ASAP.
     * @return <code>true</code>
     */
    public boolean serviceASAP() {
        return true;
    }

    /**
     * Indicate that we don't want the work to happen immediately in the
     * user thread.
     * @return <code>false</code>
     */
    public boolean serviceImmediately() {
        // This method isn't actually used by BasicDaemon, but we still need to
        // implement it in order to satisfy the Serviceable interface.
        return false;
    }
}
