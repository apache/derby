/*

   Derby - Class org.apache.derby.impl.services.cache.ReplacementPolicy

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

import org.apache.derby.iapi.error.StandardException;

/**
 * Interface that must be implemented by classes that provide a replacement
 * algorithm for <code>ConcurrentCache</code>.
 */
interface ReplacementPolicy {
    /**
     * Insert an entry into the <code>ReplacementPolicy</code>'s data
     * structure, possibly evicting another entry. The entry should be
     * uninitialized when the method is called (that is, its
     * <code>Cacheable</code> should be <code>null</code>), and it should be
     * locked. When the method returns, the entry may have been initialized
     * with a <code>Cacheable</code> which is ready to be reused. It is also
     * possible that the <code>Cacheable</code> is still <code>null</code> when
     * the method returns, in which case the caller must allocate one itself.
     * The entry will be associated with a {@code Callback} object that it can
     * use to communicate back to the replacement policy events (for instance,
     * that it has been accessed or become invalid).
     *
     * @param entry the entry to insert
     * @exception StandardException if an error occurs while inserting the
     * entry
     *
     * @see CacheEntry#setCallback(ReplacementPolicy.Callback)
     */
    void insertEntry(CacheEntry entry) throws StandardException;

    /**
     * Try to shrink the cache if it has exceeded its maximum size. It is not
     * guaranteed that the cache will actually shrink.
     */
    void doShrink();

    /**
     * The interface for the callback objects that <code>ConcurrentCache</code>
     * uses to notify the replacement algorithm about events such as look-ups
     * and removals. Each <code>Callback</code> object is associated with a
     * single entry in the cache.
     */
    interface Callback {
        /**
         * Notify the replacement algorithm that the cache entry has been
         * accessed. The replacement algorithm can use this information to
         * collect statistics about access frequency which can be used to
         * determine the order of evictions.
         *
         * <p>
         *
         * The entry associated with the callback object must be locked by the
         * current thread.
         */
        void access();

        /**
         * Notify the replacement algorithm that the entry associated with this
         * callback object has been removed, and the callback object and the
         * <code>Cacheable</code> can be reused.
         *
         * <p>
         *
         * The entry associated with the callback object must be locked by the
         * current thread.
         */
        void free();
    }
}
