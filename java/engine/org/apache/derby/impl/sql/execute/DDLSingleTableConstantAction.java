/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute;

import org.apache.derby.catalog.UUID;

/**
 * Abstract class that has actions that are across
 * all DDL actions that are tied to a table.  An example
 * of DDL that affects a table is CREATE INDEX or
 * DROP VIEW.  An example of DDL that does not affect
 * a table is CREATE STATEMENT or DROP SCHEMA.
 *
 * @author jamie
 */
abstract class DDLSingleTableConstantAction extends DDLConstantAction 
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
	protected UUID					tableId;

	
	/**
	 * constructor
	 *
	 * @param tableId the target table
	 */
	protected DDLSingleTableConstantAction(UUID tableId)
	{
		super();
		this.tableId = tableId;
	}

	/**
	 * Does this constant action modify the passed in table
	 * uuid?  By modify we mean add or drop things tied to
	 * this table (e.g. index, trigger, constraint).  Things
	 * like views or spses that reference this table don't
	 * count.
	 *
	 * @param tableId the table id
	 */
	public boolean modifiesTableId(UUID tableId)
	{
		return (this.tableId == null) ?
			false :
			this.tableId.equals(tableId);
	}
}
