/*

   Derby - Class org.apache.derby.iapi.sql.execute.RowChanger

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

package org.apache.derby.iapi.sql.execute;

import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;

import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.store.access.TransactionController;

/**
  Perform row at a time DML operations of tables and maintain indexes.
  */
public interface RowChanger
{
	/**
	  Open this RowChanger.

	  <P>Note to avoid the cost of fixing indexes that do not
	  change during update operations use openForUpdate(). 
	  @param lockMode	The lock mode to use
							(row or table, see TransactionController)

	  @exception StandardException thrown on failure to convert
	  */
	public void open(int lockMode)
		 throws StandardException;

	/**
	 * Set the row holder for this changer to use.
	 * If the row holder is set, it wont bother 
	 * saving copies of rows needed for deferred
	 * processing.  Also, it will never close the
	 * passed in rowHolder.
	 *
	 * @param rowHolder	the row holder
	 */
	public void setRowHolder(TemporaryRowHolder rowHolder);

	/**
	 * Sets the index names of the tables indices. Used for error reporting.
	 * 
	 * @param indexNames		Names of all the indices on this table.
	 */
	public void setIndexNames(String[] indexNames);

	/**
	  Open this RowChanger to avoid fixing indexes that do not change
	  during update operations. 

	  @param fixOnUpdate fixOnUpdat[ix] == true ==> fix index 'ix' on
	  an update operation.
	  @param lockMode	The lock mode to use
							(row or table, see TransactionController)
	  @param wait		If true, then the caller wants to wait for locks. False will be
							when we using a nested user xaction - we want to timeout right away
							if the parent holds the lock.  (bug 4821)

	  @exception StandardException thrown on failure to convert
	  */
	public void openForUpdate( boolean[] fixOnUpdate, int lockMode, boolean wait )
		 throws StandardException;

	/**
	  Insert a row into the table and perform associated index maintenance.

	  @param baseRow the row.
	  @param baseRowLocation the row's base conglomerate
	     location
	  @exception StandardException		Thrown on error
	  */
	public void insertRow(ExecRow baseRow)
		 throws StandardException;
		
	/**
	  Delete a row from the table and perform associated index maintenance.

	  @param baseRow the row.
	  @param baseRowLocation the row's base conglomerate
	     location
	  @exception StandardException		Thrown on error
	  */
	public void deleteRow(ExecRow baseRow, RowLocation baseRowLocation)
		 throws StandardException;

	/**
	  Update a row in the table and perform associated index maintenance.

	  @param oldBaseRow the old image of the row.
	  @param newBaseRow the new image of the row.
	  @param baseRowLocation the row's base conglomerate
	     location
	  @exception StandardException		Thrown on error
	  */
	public void updateRow(ExecRow oldBaseRow,
						  ExecRow newBaseRow,
						  RowLocation baseRowLocation)
		 throws StandardException;

	/**
	  Finish processing the changes.  This means applying the deferred
	  inserts for updates to unique indexes.

	  @exception StandardException		Thrown on error
	 */
	public void finish()
		throws StandardException;

	/**
	  Close this RowChanger.

	  @exception StandardException		Thrown on error
	  */
	public void close()
		throws StandardException;

	/** 
	 * Return the ConglomerateController from this RowChanger.
	 * This is useful when copying properties from heap to 
	 * temp conglomerate on insert/update/delete.
	 *
	 * @return The ConglomerateController from this RowChanger.
	 */
	public ConglomerateController getHeapConglomerateController();

}
