/*

   Derby - Class org.apache.derby.iapi.services.cache.CacheFactory

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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
	A factory for handing out caches.
*/
public interface CacheFactory {
	
	/**
		Create a cache that uses the class represented by holderClass as
		the holder class. This holderClass must implement Cacheable.

		@param holderFactory The factory for the objects that are to be cached.
		@param name			The name of the cache
		@param initParam	The object passed to each holder object's initParameter() method.
		@param initialSize	The number of initial holder objects that will be created
		@param maximumSize  The maximum number of objects the cache will hold
 
	*/
	
	public CacheManager newCacheManager(CacheableFactory holderFactory, String name,
										int initialSize, int maximumSize);
	
	/**
		Create a cache that uses the class represented by holderClass as
		the holder class. This holderClass must implement Cacheable.

		@param holderClass	The Class object representing the holder class.
		@param name			The name of the cache
		@param initParam	The object passed to each holder object's initParameter() method.
		@param initialSize	The number of initial holder objects that will be created
		@param maximumSize  The maximum total size of the objects that the cache will hold
 
	*/
	
	public CacheManager newSizedCacheManager(CacheableFactory holderFactory, String name,
										int initialSize, long maximumSize);
}

