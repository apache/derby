/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.dictionary
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.catalog.UUID;

/**
 * This is a descriptor for something that is a 
 * SQL object that has the following properties:
 * <UL>
 *	<LI> resides in a schema </LI>
 *	<LI> has a name (that is unique when combined with schema) </LI>
 *	<LI> has a unique identifier (UUID) </LI>
 * </UL>
 *
 * UUIDS.
 *
 * @author jamie
 */
public interface UniqueSQLObjectDescriptor extends UniqueTupleDescriptor
{ 
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
	/**
	 * Get the name of this object.  E.g. for a table descriptor,
	 * this will be the table name.
	 * 
	 * @return the name
	 */
	public String getName();

	/**
	 * Get the objects schema descriptor
	 *
	 * @return the schema descriptor
	 *
	 * @exception StandardException on error
	 */
	public SchemaDescriptor getSchemaDescriptor()
		throws StandardException;
}
