/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.cache
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.cache;

/**
	A factory for handing out caches.
*/
public interface CacheFactory {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	
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

