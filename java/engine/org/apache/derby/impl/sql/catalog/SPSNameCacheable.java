/*

   Derby - Class org.apache.derby.impl.sql.catalog.SPSNameCacheable

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.sql.catalog;

import org.apache.derby.iapi.services.cache.Cacheable;
import org.apache.derby.iapi.services.cache.CacheManager;

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;

import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.SPSDescriptor;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.util.Hashtable;

/**
 * This class implements a Cacheable for a DataDictionary cache of
 * sps descriptors, with the lookup key being the name/schema of the sps.
 * Assumes client passes in a string that includes the schema name.
 * <p>
 * The cache ensures that the class of the target sps is loaded
 * if the sps is found in cache.  This is ensured by calling
 * loadGeneratedClass() on the sps when it is added to the cache.
 * Each subsequent user of the sps cache will do its own load/unload
 * on the class.  Because the class manager/loader maintains reference
 * counts on the classes it is handling, the user load/unload will
 * just increment/decrement the use count.  Only when the sps is
 * uncached will it be unloaded.
 */
class SPSNameCacheable implements Cacheable
{
	private TableKey 			identity;
	private SPSDescriptor		spsd;
	private final DataDictionaryImpl	dd;


	SPSNameCacheable(DataDictionaryImpl dd) {
		this.dd = dd;
	}

	/* Cacheable interface */

	/** @see Cacheable#clearIdentity */
	public void clearIdentity()
	{
		if (spsd != null)
		{
			dd.spsCacheEntryRemoved(spsd);

			if (SanityManager.DEBUG)
			{
				if (SanityManager.DEBUG_ON("SPSNameCacheTrace"))
				{
					System.out.println("SPSCACHE: clearIdentity() on "+spsd.getName());
				}
			}
			spsd = null;
			identity = null;
		}
	}

	/** @see Cacheable#getIdentity */
	public Object getIdentity()
	{
		return identity;
	}

	/** @see Cacheable#createIdentity */
	public Cacheable createIdentity(Object key, Object createParameter)
	{
		if (SanityManager.DEBUG)
		{
			if (!(key instanceof TableKey))
			{
				SanityManager.THROWASSERT("Key for a SPSNameCacheElement is a " +
						key.getClass().getName() +
						" instead of a TableKey");
			}
			if (!(createParameter instanceof SPSDescriptor))
			{
				SanityManager.THROWASSERT("Create parameter for a SPSNameCacheElement is a " +
					createParameter.getClass().getName() +
					"instead of a SPSDescriptorImpl");
			}			
		}

		identity = (TableKey)key;
		spsd = (SPSDescriptor) createParameter;
		
		if (spsd != null)
		{
			if (SanityManager.DEBUG)
			{
				if (SanityManager.DEBUG_ON("SPSNameCacheTrace"))
				{
					System.out.println("SPSCACHE: createIdentity() on "+spsd.getName());
				}
			}

			dd.spsCacheEntryAdded(spsd);
			try
			{
				spsd.loadGeneratedClass();
			} catch (StandardException e)
			{
				/*		
				** We cannot throw an exception here, and although
				** we don't expect a problem, we'll put some debugging
				** under sanity just in case.  Note that even if we
				** do get an exception here, everything else will work
				** ok -- subsequent attempts to access the generated
				** class for this sps will do a load themselves, and
				** they will throw their exception back to the user.
				*/	
				if (SanityManager.DEBUG)
				{
					System.out.println("Error loading class for "+spsd.getName());
					System.out.println(e);
					e.printStackTrace();
				}
			}
			return this;
		}
		else
		{
			return null;
		}
	}

	/**
	 * @see Cacheable#setIdentity
	 *
	 * @exception StandardException		Thrown on error
	 */
	public Cacheable setIdentity(Object key) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			if (!(key instanceof TableKey))
			{
				SanityManager.THROWASSERT("Key for a SPSNameCacheable Element is a " +
					key.getClass().getName() +
					" instead of a TableKey");
			}
		}

		
		identity = (TableKey)key ;
		spsd = dd.getUncachedSPSDescriptor(identity);
		if (spsd != null)
		{
			if (SanityManager.DEBUG)
			{
				if (SanityManager.DEBUG_ON("SPSNameCacheTrace"))
				{
					System.out.println("SPSCACHE: setIdentity() on "+spsd.getName());
				}
			}

			dd.spsCacheEntryAdded(spsd);
			try
			{
				spsd.loadGeneratedClass();
			} catch (StandardException e)
			{
				/*		
				** We cannot throw an exception here, and although
				** we don't expect a problem, we'll put some debugging
				** under sanity just in case.  Note that even if we
				** do get an exception here, everything else will work
				** ok -- subsequent attempts to access the generated
				** class for this sps will do a load themselves, and
				** they will throw their exception back to the user.
				*/	
				if (SanityManager.DEBUG)
				{
					System.out.println("Error loading class for "+spsd.getName());
					System.out.println(e);
					e.printStackTrace();
				}
			}
			return this;
		}
		else
		{
			return null;
		}
	}

	/* Cacheable interface */

	/** @see Cacheable#clean */
	public void clean(boolean forRemove)
	{
		return;
	}

	/** @see Cacheable#isDirty */
	public boolean isDirty()
	{
		return false;
	}

	/**
	 * Get the sps descriptor that is associated with this Cacheable
	 */
	public SPSDescriptor getSPSDescriptor()
	{
		return spsd;
	}

	/**
	 * Check the consistency of the table descriptor held by this TDCacheable
	 * versus an uncached table descriptor.
	 *
	 * @param uncachedSpsd	The uncached descriptor to compare to
	 * @param identity		The identity of the table descriptor
	 * @param reportInconsistent	A HeaderPrintWriter to send complaints to
	 *
	 * @return	true if the descriptors are the same, false if they're different
	 *
	 * @exception StandardException		Thrown on error
	 */
	private boolean checkConsistency(SPSDescriptor uncachedSpsd,
										Object identity,
										HeaderPrintWriter reportInconsistent)
			throws StandardException
	{
		boolean	retval = true;

		if (SanityManager.DEBUG)
		{
			if (uncachedSpsd == null)
			{
				reportInconsistent.println(
					"Inconsistent SPSNameCacheable: identity = " + identity +
					", uncached table descriptor not found.");
				retval = false;
			}
			else
			{
				if (
					(!uncachedSpsd.getText().equals(spsd.getText())) ||
					(!uncachedSpsd.getUsingText().equals(spsd.getUsingText())) ||
					(!uncachedSpsd.getQualifiedName().equals(spsd.getQualifiedName()))
			   	)
				{
					reportInconsistent.println(
						"Inconsistent SPSNameCacheable: identity = " + identity +
						", cached  SPS = " +
						spsd +
						", uncached SPS = " +
						uncachedSpsd);

					retval = false;
				}
			}
		}

		return retval;
	}
}
