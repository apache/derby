/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.catalog
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.catalog;

import org.apache.derby.iapi.services.cache.Cacheable;
import org.apache.derby.iapi.services.cache.CacheManager;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

/**
 * This class implements a Cacheable for a DataDictionary cache of
 * table descriptors.  It is an abstract class - there is more than
 * one cache of table descriptors per data dictionary, and this class
 * provides the implementation that's common to all of them.  The lookup
 * key for the cache (the "identity" of the cache item) is provided by
 * the subclass.
 *
 * Another design alternative was to make the table descriptors themselves
 * the cacheable objects.  This was rejected because: we would have only
 * one way of caching table descriptors, and we need at least two (by UUID
 * and by name); the contents of a table descriptor would have to be
 * split out into a separate class, so it could be used as the createParameter
 * to the createIdentity() method; the releasing of the Cacheable would
 * have to be done when at the end of compilation by traversing the tree -
 * by creating a separate Cacheable object, we can release the object within
 * the getTableDescriptor() method after getting the table descriptor out
 * of it.
 */
abstract class TDCacheable implements Cacheable
{
	protected TableDescriptor	td;
	protected final DataDictionaryImpl	dd;

	TDCacheable(DataDictionaryImpl dd) {
		this.dd = dd;
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
	 * Get the table descriptor that is associated with this Cacheable
	 */
	public TableDescriptor getTableDescriptor()
	{
		return td;
	}

	/**
	 * Check the consistency of the table descriptor held by this TDCacheable
	 * versus an uncached table descriptor.
	 *
	 * @param uncachedTD	The uncached descriptor to compare to
	 * @param identity		The identity of the table descriptor
	 * @param reportInconsistent	A HeaderPrintWriter to send complaints to
	 *
	 * @return	true if the descriptors are the same, false if they're different
	 *
	 * @exception StandardException		Thrown on error
	 */
	protected boolean checkConsistency(TableDescriptor uncachedTD,
										Object identity,
										HeaderPrintWriter reportInconsistent)
			throws StandardException
	{
		boolean	retval = true;

		if (SanityManager.DEBUG)
		{
			if (uncachedTD == null)
			{
				reportInconsistent.println(
					"Inconsistent NameTDCacheable: identity = " + identity +
					", uncached table descriptor not found.");
				retval = false;
			}
			else
			{
				if (
					(uncachedTD.getHeapConglomerateId() !=
										td.getHeapConglomerateId()) ||
					( ! uncachedTD.getUUID().equals(td.getUUID())) ||
					( ! uncachedTD.getSchemaName().equals(td.getSchemaName()))||
					( ! uncachedTD.getName().equals(td.getName())) ||
					( uncachedTD.getTableType() != td.getTableType())
			   	)
				{
					reportInconsistent.println(
						"Inconsistent NameTDCacheable: identity = " + identity +
						", cached TD = " +
						td +
						", uncached TD = " +
						uncachedTD);

					retval = false;
				}
			}
		}

		return retval;
	}
}
