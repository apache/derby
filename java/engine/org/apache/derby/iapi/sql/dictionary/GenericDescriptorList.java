/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.dictionary
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.UniqueTupleDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.catalog.UUID;

import java.util.ArrayList;
import java.util.Iterator;

public class GenericDescriptorList extends ArrayList
{
	private boolean scanned;

	/**
	 * Mark whether or not the underlying system table has
	 * been scanned.  (If a table does not have any
	 * constraints then the size of its CDL will always
	 * be 0.  We used these get/set methods to determine
	 * when we need to scan the table.
	 *
	 * @param scanned	Whether or not the underlying system table has been scanned.
	 *
	 * @return Nothing.
	 */
	public void setScanned(boolean scanned)
	{
		this.scanned = scanned;
	}

	/**
	 * Return whether or not the underlying system table has been scanned.
	 *
	 * @return		Where or not the underlying system table has been scanned.
	 */
	public boolean getScanned()
	{
		return scanned;
	}

	/**
	 * Get the UniqueTupleDescriptor that matches the 
	 * input uuid.
	 *
	 * @param uuid		The UUID for the object
	 *
	 * @return The matching UniqueTupleDescriptor.
	 */
	public UniqueTupleDescriptor getUniqueTupleDescriptor(UUID uuid)
	{
		for (Iterator iterator = iterator(); iterator.hasNext(); )
		{
			UniqueTupleDescriptor ud = (UniqueTupleDescriptor) iterator.next();
			if (ud.getUUID().equals(uuid))
			{
				return ud;
			}
		}
		return null;
	}

	public java.util.Enumeration elements() {
		return java.util.Collections.enumeration(this);
	}
}
