/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.depend
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql.depend;

import org.apache.derby.iapi.error.StandardException;

import java.util.Enumeration;
import java.util.Hashtable;

/**
 * ProviderList is a list of Providers that is being
 * tracked for some object other than the current dependent.
 */

public class ProviderList extends Hashtable
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
	/**
	 * Add a Provider to the list.
	 *
	 * @param prov	The Provider to add to the list.
	 *
	 * @return Nothing.
	 */
	public void addProvider(Provider prov)
	{
		put (prov.getObjectID(), prov);
	}
}
