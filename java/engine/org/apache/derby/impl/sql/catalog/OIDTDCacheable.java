/*

   Derby - Class org.apache.derby.impl.sql.catalog.OIDTDCacheable

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

package org.apache.derby.impl.sql.catalog;

import org.apache.derby.iapi.services.cache.Cacheable;

import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.catalog.UUID;

/**
 * This class implements a Cacheable for a DataDictionary cache of
 * table descriptors, with the lookup key being the UUID of the table.
 */
class OIDTDCacheable extends TDCacheable
{
	private UUID	identity;

	OIDTDCacheable(DataDictionaryImpl dd) {
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
			if (!(key instanceof UUID))
			{
				SanityManager.THROWASSERT("Key for an OIDTDCacheElement is a " +
					key.getClass().getName() +
					" instead of an UUID");
			}
			if (!(createParameter instanceof TableDescriptor))
			{
				SanityManager.THROWASSERT("Create parameter for an OIDTDCacheElement is a " +
					createParameter.getClass().getName() +
					"instead of a TableDescriptorImpl");
			}
		}

		identity = ((UUID) key).cloneMe();
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
			if (!(key instanceof UUID))
			{
				SanityManager.THROWASSERT("Key for an OIDTDCacheElement is a " +
					key.getClass().getName() +
					" instead of an UUID");
			}
		}

		identity = ((UUID) key).cloneMe();
		td = dd.getUncachedTableDescriptor(identity);

		if (td != null)
		{
			// Add cache entry to the nameTdCache in the DataDictionary.
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
				  " while getting cached table descriptor in OIDTDCacheable.");
				uncachedTD = null;
			}

			retval = checkConsistency(uncachedTD, identity, reportInconsistent);
		}

		return retval;
	}
	*/
}
