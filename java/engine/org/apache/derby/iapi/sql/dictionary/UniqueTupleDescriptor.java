/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.dictionary
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.catalog.UUID;

/**
 * Simple interface for Tuple Descriptors that have
 * UUIDS.
 *
 * @author jamie
 */
public interface UniqueTupleDescriptor
{ 
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
	/**
	 * Return the UUID for this Descriptor
	 *
	 * @return the uuid
	 */
	public UUID getUUID();
}
