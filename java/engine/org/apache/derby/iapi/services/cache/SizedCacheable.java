/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.cache
   (C) Copyright IBM Corp. 2002, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
		IBM Copyright &copy notice.
	*/

    public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2002_2004;

    /**
     * Get the estimated size of the cacheable object.
     *
     * @return the estimated size, in bytes
     */
    public int getSize();
}
