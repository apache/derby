/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.execute
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql.execute;

import org.apache.derby.iapi.error.StandardException;

/**
 * This is a table name reference that can be retrieved from
 * an active cursor.  
 *
 * @author jamie
 */
public interface ExecCursorTableReference
{
	/**
	 * Return the base name of the table
 	 *
	 * @return the base name
	 */
	String getBaseName();

	/**
	 * Return the exposed name of the table.  Exposed
	 * name is another term for correlation name.  If
	 * there is no correlation, this will return the base
	 * name.
 	 *
	 * @return the base name
	 */
	String getExposedName();


	/**
	 * Return the schema for the table.  
	 *
	 * @return the schema name
	 */
	String getSchemaName();
}
