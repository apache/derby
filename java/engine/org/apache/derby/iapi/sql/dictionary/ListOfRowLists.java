/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.ListOfRowLists

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

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.error.StandardException;

/**
 * This interface wraps a list of RowLists.
 *
 * @version 0.1
 * @author Rick Hillegas
 */

public interface ListOfRowLists
{

	/**
	 * Add another system table's RowList to this list of lists.
	 *
	 * @param row   RowList to add
	 *
	 * @return	Nothing
	 *
	 */
	public void add(RowList rowList);

	/**
	 * Return number of items currently on this list.
	 *
	 *
	 * @return	Item count.
	 *
	 */
	public int size();

	/**
	 * Get the RowList for a system table given the table's ID.
	 *
	 *
	 * @param tableID   ID of system table whose RowList we should return
	 *
	 * @return	RowList corresponding to this system table.
	 *
	 * @exception StandardException		Thrown on error
	 */
    public RowList getRowList(long tableID) throws StandardException;

	/**
	 * Execution-time routine to delete all the tuples on these row lists
	 * from their respective catalogs.
	 *
	 *	@param	lcc	language state variable
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	deleteFromCatalogs( LanguageConnectionContext lcc )
					throws StandardException;

	/**
	 * Execution-time routine to write all the row lists to their respective
	 * system tables.
	 *
	 *	@param	lcc	language state variable
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	stuffAllCatalogs( LanguageConnectionContext lcc )
					throws StandardException;

}
