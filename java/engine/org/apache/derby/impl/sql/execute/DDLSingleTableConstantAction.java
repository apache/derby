/*

   Derby - Class org.apache.derby.impl.sql.execute.DDLSingleTableConstantAction

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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
