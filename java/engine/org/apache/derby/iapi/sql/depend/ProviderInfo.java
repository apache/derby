/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.depend
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql.depend;

import org.apache.derby.iapi.services.io.Formatable;

import org.apache.derby.catalog.DependableFinder;
import org.apache.derby.catalog.UUID;

/**
 * A ProviderInfo associates a DependableFinder with a UUID that stands
 * for a database object.  For example, the tables used by a view have
 * DependableFinders associated with them, and a ProviderInfo associates
 * the tables' UUIDs with their DependableFinders.
 */
public interface ProviderInfo extends Formatable
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
	/**
	 * Get the DependableFinder.
	 */
	DependableFinder getDependableFinder();

	/**
	 * Get the object id
	 */
	UUID getObjectId();

	/**
	 * Get the provider's name.
	 */
	String getProviderName();
}
