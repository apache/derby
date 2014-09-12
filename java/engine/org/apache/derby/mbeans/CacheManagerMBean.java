/*

   Derby - Class org.apache.derby.mbeans.CacheManagerMBean

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

package org.apache.derby.mbeans;

/**
 * This is an MBean that provides information about one of Derby's cache
 * managers.
 */
public interface CacheManagerMBean {
    /**
     * Enable or disable collection of cache access counts. That is, whether
     * or not each hit, miss and eviction should be counted. Enabling it might
     * impose a small overhead on cache accesses, and might reduce the system
     * performance. Access counts are disabled by default.
     *
     * @param collect {@code true} if access counts should be collected, or
     *                {@code false} otherwise
     * @see #getCollectAccessCounts()
     * @see #getHitCount()
     * @see #getMissCount()
     * @see #getEvictionCount()
     */
    void setCollectAccessCounts(boolean collect);

    /**
     * Check if collection of cache access counts is enabled.
     *
     * @return {@code true} if access counts are enabled,
     *         {@code false} otherwise
     * @see #setCollectAccessCounts(boolean)
     */
    boolean getCollectAccessCounts();

    /**
     * Get the number of cache accesses where the requested object was
     * already in the cache.
     *
     * @return the number of cache hits
     */
    long getHitCount();

    /**
     * Get the number of cache accesses where the requested object was
     * not already in the cache.
     *
     * @return the number of cache misses
     */
    long getMissCount();

    /**
     * Get the number of cached objects that have been evicted from the
     * cache in order to make room for other objects.
     *
     * @return the number of evicted objects
     */
    long getEvictionCount();

    /**
     * Get the maximum number of entries that could be held by this cache.
     *
     * @return the maximum number of entries in the cache
     */
    long getMaxEntries();

    /**
     * Get the number of entries currently allocated in the cache. This
     * number includes entries for objects that have been removed from the
     * cache, whose entries have not yet been reused for other objects.
     *
     * @return the number of entries in the cache
     */
    long getAllocatedEntries();

    /**
     * Get the number of objects that are currently in the cache.
     *
     * @return the number of objects in the cache
     */
    long getUsedEntries();
}
