/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.RowList

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.iapi.sql.execute.ExecRow;

import java.util.Vector;

/**
 * This interface wraps a list of Rows.
 *
 * @version 0.1
 * @author Rick Hillegas
 */

public class RowList extends Vector
{
	///////////////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	///////////////////////////////////////////////////////////////////////

	private		String		tableName;

	private	transient	TabInfo		tableInfo;


	///////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	///////////////////////////////////////////////////////////////////////

	/**
	 * Public niladic constructor. Needed for Formatable interface to work.
	 *
	 */
    public	RowList() {}

	/**
	 * Constructor. Creates a list of rows for a table.
	 *
	 * @param tableInfo	Table information
	 *
	 */
    public RowList( TabInfo tableInfo )
	{
		this.tableInfo = tableInfo;

		tableName = tableInfo.getTableName();
	}


	///////////////////////////////////////////////////////////////////////
	//
	//	ROW LIST INTERFACE
	//
	///////////////////////////////////////////////////////////////////////

	/**
	 * Add a row to the list for this table.
	 *
	 * @param row   Row to chain onto list.
	 *
	 * @return	Nothing
	 *
	 */

	public void add(ExecRow row)
	{
		super.addElement(row);
	}
}








