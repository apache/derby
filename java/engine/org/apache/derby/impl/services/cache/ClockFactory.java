/*

   Derby - Class org.apache.derby.impl.services.cache.ClockFactory

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

package org.apache.derby.impl.services.cache;

import org.apache.derby.iapi.services.cache.CacheFactory;
import org.apache.derby.iapi.services.cache.CacheManager;
import org.apache.derby.iapi.services.cache.Cacheable;
import org.apache.derby.iapi.services.cache.CacheableFactory;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.util.Properties;

/**
  Multithreading considerations: no need to be MT-safe, caller (module control)
  provides synchronization. Besides, this class is stateless.
*/

public class ClockFactory implements CacheFactory {

	/**
		Trace flag to display cache statistics
	*/
	public static final String CacheTrace = SanityManager.DEBUG ? "CacheTrace" : null;

	public ClockFactory() {
	}



	/*
	** Methods of CacheFactory
	*/

	public CacheManager newCacheManager(CacheableFactory holderFactory, String name, int initialSize, int maximumSize)
	{

		if (initialSize <= 0)
			initialSize = 1;

		return new Clock(holderFactory, name, initialSize, maximumSize, false);
	}
	
	public CacheManager newSizedCacheManager(CacheableFactory holderFactory, String name,
										int initialSize, long maximumSize)
	{

		if (initialSize <= 0)
			initialSize = 1;

		return new Clock(holderFactory, name, initialSize, maximumSize, true);
	}
}
