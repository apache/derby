/*

   Derby - Class org.apache.derby.iapi.services.cache.SizedCacheable

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.iapi.services.cache;

/**
 * This interface extends the Cacheable interface (@see Cacheable) with a method that
 * estimates the size of the Cacheable object, in bytes. CacheManagers constructed with the SizedCacheFactory
 * interface regulate the total estimated cache size, in bytes.
 * CacheManagers constructed with the CacheFactory use regulate the total number of cache entries.
 *
 * @see Cacheable
 * @see SizedCacheableFactory
 * @see CacheFactory
 *
 * @author Jack Klebanoff
 */

public interface SizedCacheable extends Cacheable
{

    /**
     * Get the estimated size of the cacheable object.
     *
     * @return the estimated size, in bytes
     */
    public int getSize();
}
