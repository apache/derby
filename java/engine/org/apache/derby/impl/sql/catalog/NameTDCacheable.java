/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.catalog
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.catalog;

import org.apache.derby.iapi.services.cache.Cacheable;

import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * This class implements a Cacheable for a DataDictionary cache of
 * table descriptors, with the lookup key being the name of the table.
 *
 * Assumes client passes in a string that includes the schema name.
 */
class NameTDCacheable extends TDCacheable
{
	private TableKey identity;

	NameTDCacheable(DataDictionaryImpl dd) {
		super(dd);
	}


	/* Cacheable interface */

	/** @see Cacheable#clearIdentity */
	public void clearIdentity()
	{
		identity = null;
		td = null;
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
				SanityManager.THROWASSERT("Key for a NameTDCacheElement is a " +
					key.getClass().getName() +
					" instead of a TableKey");
			}

			if (!(createParameter instanceof TableDescriptor))
			{
				SanityManager.THROWASSERT("Create parameter for a NameTDCacheElement is a " +
					createParameter.getClass().getName() +
					"instead of a TableDescriptorImpl");
			}			
		}

		identity = (TableKey)key;
		td = (TableDescriptor) createParameter;

		if (td != null)
			return this;
		else
			return null;
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
				SanityManager.THROWASSERT("Key for a NameTDCacheElement is a " +
					key.getClass().getName() +
					" instead of a TableKey");
			}
		}

		;
		td = dd.getUncachedTableDescriptor(identity = (TableKey)key);

		if (td != null)
		{
			// add table descriptor to the oidTdcache in the Datadictionary.
			// no fear of deadlocks because this is called outside the 
			// synchronize block in the cache code.
			dd.addTableDescriptorToOtherCache(td, this);
			return this;
		}
		else
			return null;
	}

	/**
	  @exception StandardException		Thrown on error
	  */
	// If this code is required it should be moved into a D_ class. - djd

/*
	public boolean isConsistent(HeaderPrintWriter reportInconsistent)
		throws StandardException
	{
		boolean retval = true;

		if (SanityManager.DEBUG)
		{
			TableDescriptor uncachedTD;

			try
			{
				uncachedTD = dd.getUncachedTableDescriptor(identity);
			}
			catch (StandardException se)
			{
				reportInconsistent.println("Unexpected exception " + se +
				  " while getting cached table descriptor in NameTDCacheable.");
				uncachedTD = null;
			}

			retval = checkConsistency(uncachedTD, identity, reportInconsistent);
		}

		return retval;
	}
	*/
}
