/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.TabInfo

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.TupleFilter;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.catalog.UUID;
import java.util.Properties;

/**
 * This interface is for communicating between the DataDictionary and
 * the various CatalogRowFactories.  It tries to hide as much about
 * each catalog as it can behind this interface.
 */

public interface TabInfo
{
	/**
	 * ROWNOTDUPLICATE is out of range for a row
	 * number.  If a return code does not equal 
	 * this value, then it refers to the row
	 * that is a duplicate.
	 */
	public	static	final	int	ROWNOTDUPLICATE = -1;

	/**
	 * Get the conglomerate for the heap.
	 *
	 * @return long		The conglomerate for the heap.
	 */
	public long getHeapConglomerate();

	/**
	 * Set the heap conglomerate for the TabInfo.
	 *
	 * @param heapConglomerate	The new heap conglomerate.
	 *
	 * @return Nothing.
	 */
	public void setHeapConglomerate(long heapConglomerate);

	/**
	 * Get the conglomerate for the specified index.
	 *
	 * @return long		The conglomerate for the specified index.
	 */
	public long getIndexConglomerate(int indexID);

	/**
	 * Set the index conglomerate for the table.
	 *
	 * @param index				Index number for index for table
	 * @param indexConglomerate	The conglomerate for that index
	 *
	 * @return Nothing
	 */
	public void setIndexConglomerate(int index, long indexConglomerate);

	/**
	 * Set the index conglomerate for the table.
	 *
	 * @param cd	The ConglomerateDescriptor for one of the index
	 *				for this table.
	 *
	 * @return Nothing
	 */
	public void setIndexConglomerate(ConglomerateDescriptor cd);

	/**
	 * Get the table name.
	 *
	 * @return String	The table name.
	 */
	public String getTableName();

	/**
	 * Get the index name.
	 *
	 * @param indexID	Index number for index for table
	 *
	 * @return String	The index name.
	 */
	public String getIndexName(int indexID);

	/**
	 * Set the index name for the specified indexID
	 *
	 * @param indexID	Index number for index for table
	 * @param indexName	The name for that index ID
	 */
	public void setIndexName(int indexID, String indexName);

	/** 
	 * Get the CatalogRowFactory for this TabInfo.
	 *
	 * @return CatalogRowFactory	The CatalogRowFactory for this TabInfo.
	 */
	public CatalogRowFactory getCatalogRowFactory();

	/**
	 * Is the TabInfo fully initialized.  
	 * (i.e., is all conglomerate info initialized)
	 *
	 * @return boolean	Whether or not the TabInfo is fully initialized.
	 */
	public boolean isComplete();

	/**
	 * Get the column count for the specified index number.
	 *
	 * @param indexNumber	The index number.
	 *
	 * @return int			The column count for the specified index.
	 */
	public int getIndexColumnCount(int indexNumber);

	/**
	 * Get the IndexRowGenerator for the specified index number.
	 *
	 * @param indexNumber	The index number.
	 *
	 * @return IndexRowGenerator	The IRG for the specified index number.
	 */
	public IndexRowGenerator getIndexRowGenerator(int indexNumber);

	/**
	 * Set the IndexRowGenerator for the specified index number.
	 *
	 * @param indexNumber	The index number.
	 * @param irg			The IndexRowGenerator for the specified index number.
	 *
	 * @return Nothing.
	 */
	public void setIndexRowGenerator(int indexNumber, IndexRowGenerator irg);

	/** 
	 * Get the number of indexes on this catalog.
	 *
	 * @return int	The number of indexes on this catalog.
	 */
	public int getNumberOfIndexes();

	/**
	 * Get the base column position for a column within a catalog
	 * given the (0-based) index number for this catalog and the
	 * (0-based) column number for the column within the index.
	 *
	 * @param indexNumber	The index number
	 * @param colNumber		The column number within the index
	 *
	 * @return int		The base column position for the column.
	 */
	public int getBaseColumnPosition(int indexNumber, int colNumber);

	/**
	 * Set the base column position for a column within a catalog
	 * given the (0-based) index number for this catalog and the
	 * (0-based) column number for the column within the index.
	 *
	 * @param indexNumber	The index number
	 * @param colNumber		The column number within the index
	 * @param baseColPos	The base column position for the column.
	 *
	 * @return Nothing.
	 */
	public void setBaseColumnPosition(int indexNumber, int colNumber,
									  int baseColumnPosition);

	/**
	 * Return whether or not this index is declared unique
	 *
	 * @param indexNumber	The index number
	 *
	 * @return boolean		Whether or not this index is declared unique
	 */
	public boolean isIndexUnique(int indexNumber);


	/**
	 * Inserts a base row into a catalog and inserts all the corresponding
	 * index rows.
	 *
	 *	@param	row			row to insert
	 *	@param	tc			transaction
	 *	@param	wait		to wait for lock or not
	 *	@return	row number (>= 0) if duplicate row inserted into an index
	 *			ROWNOTDUPLICATE otherwise
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public int insertRow( ExecRow row, TransactionController tc, boolean wait )
		throws StandardException;

	/**
	 * Inserts a base row into a catalog and inserts all the corresponding
	 * index rows.
	 *
	 *	@param	row			row to insert
	 *	@param	lcc			language state variable
	 *	@return	row number (>= 0) if duplicate row inserted into an index
	 *			ROWNOTDUPLICATE otherwise
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public int insertRow( ExecRow row, LanguageConnectionContext lcc )
		throws StandardException;


	/**
	 * Inserts a base row into a catalog and inserts all the corresponding
	 * index rows.
	 *
	 * @param	row			row to insert
	 * @param	tc			transaction controller
	 * @return	The row location for the inserted row.
	 * @exception StandardException		Thrown on failure
	 */
    public RowLocation insertRowAndFetchRowLocation(ExecRow r, TransactionController tc)
		 throws StandardException;

	/**
	 * Deletes a list of keyed rows from a catalog and all the corresponding
	 * index rows.  
	 *
	 *	@param	rowList		List of keyed rows to delete
	 *	@param	lcc			language state variable
	 *
	 * @return the number of rows deleted.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public int deleteRowList( RowList rowList, LanguageConnectionContext lcc )
		throws StandardException;

	/**
	 * Inserts a list of base rows into a catalog and inserts all the corresponding
	 * index rows.
	 *
	 *	@param	rowList		List of rows to insert
	 *	@param	tc			transaction controller
	 *
	 *
	 *	@return	row number (>= 0) if duplicate row inserted into an index
	 *			ROWNOTDUPLICATE otherwise
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public int insertRowList( RowList rowList, TransactionController tc )
		throws StandardException;

	/**
	 * Inserts a list of base rows into a catalog and inserts all the corresponding
	 * index rows.
	 *
	 *	@param	rowList		List of rows to insert
	 *	@param	lcc			language state variable
	 *
	 *
	 *	@return	row number (>= 0) if duplicate row inserted into an index
	 *			ROWNOTDUPLICATE otherwise
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public int insertRowList( RowList rowList, LanguageConnectionContext lcc )
		throws StandardException;

	/**
	 * Delete all the rows from the table.
	 * <p>
	 * LOCKING: exclusive TABLE locking
	 * 
	 * @param  tc			transaction controller
	 * @return the number of rows deleted.
	 * @exception StandardException		Thrown on failure
	 */
	public int truncate( TransactionController tc )
		 throws StandardException;

	/**
	  *	Delete the set of rows defined by a scan on an index
	  * from the table. Most of the parameters are simply passed
	  * to TransactionController.openScan. Please refer to the
	  * TransactionController documentation for details.
	  * <p>
	  * LOCKING: row locking if there is a start and a stop
	  * key; otherwise, table locking
	  *
	  *	@param	tc			transaction controller
	  *	@param	startKey	key to start the scan.
	  * @param  startOp     operation to start the scan.
	  *	@param	stopKey	    key to start the scan.
	  * @param  qualifier   a qualifier for the scan.
	  * @param  filter		filter on base rows
	  * @param  stopOp      operation to start the scan.
	  *	@param	indexNumber	Key is appropriate for this index.
	  * @return the number of rows deleted.
	  * @exception StandardException		Thrown on failure
	  * @see TransactionController#openScan
	  */
	public int deleteRows(TransactionController tc,
						  ExecIndexRow startKey,
						  int startOp,
						  Qualifier[][] qualifier,
						  TupleFilter filter,
						  ExecIndexRow stopKey,
						  int stopOp,
						  int indexNumber)
		 throws StandardException;
	/**
	  *	Given a key row, delete all matching heap rows and their index
	  *	rows.
	  * <p>
	  * LOCKING: row locking if there is a key; otherwise, 
	  * table locking.
	  *
	  *	@param	tc			transaction controller
	  *	@param	key			key to delete by.
	  *	@param	indexNumber	Key is appropriate for this index.
	  * @return the number of rows deleted. If key is not unique,
	  *         this may be more than one.
	  * @exception StandardException		Thrown on failure
	  */
	public int deleteRow( TransactionController tc,
						  ExecIndexRow key,
						  int indexNumber )
		throws StandardException;

	/**
	  *	Given a key row, return the first matching heap row.
	  * <p>
	  * LOCKING: shared row locking.
	  *
	  *	@param	tc			transaction controller
	  *	@param	key			key to read by.
	  *	@param	indexNumber	Key is appropriate for this index.
	  * @exception StandardException		Thrown on failure
	  */
	public ExecRow getRow( TransactionController tc,
						   ExecIndexRow key,
						   int indexNumber )
		 throws StandardException;
		 
	/**
	  *	Given a key row, return the first matching heap row.
	  * <p>
	  * LOCKING: shared row locking.
	  *
	  *	@param	tc			transaction controller
	  *	@param	heap		heap to look in
	  *	@param	key			key to read by.
	  *	@param	indexNumber	Key is appropriate for this index.
	  * @exception StandardException		Thrown on failure
	  */
	public ExecRow getRow( TransactionController tc,
						   ConglomerateController heap,
						   ExecIndexRow key,
						   int indexNumber )
		 throws StandardException;
		 
	/**
	 * Given an index row and index number return the RowLocation
	 * in the heap of the first matching row.
	 * Used by the autoincrement code to get the RowLocation in
	 * syscolumns given a <tablename, columname> pair.
	 * 
	 * @see org.apache.derby.impl.sql.catalog.DataDictionaryImpl#computeRowLocation(TransactionController, TableDescriptor, String)
	 *
	 * @param tc		  Transaction Controller to use.
	 * @param key		  Index Row to search in the index.
	 * @param indexNumber Identifies the index to use.
	 *
	 * @exception		  StandardException thrown on failure.
	 */
	public RowLocation getRowLocation(TransactionController tc,
									  ExecIndexRow key,
									  int indexNumber)
		throws StandardException;

	/**
	 * Updates a set of base rows in a catalog with same index key on an index
	 * and updates all the corresponding index rows.
	 * <p>
	 * LOCKING: exclusive row locking
	 *
	 *
	 *	@param	key				key row
	 *	@param	newRows			new version of the set of rows
	 *	@param	indexNumber		index that key operates
	 *	@param	indicesToUpdate	array of booleans, one for each index on the catalog.
	 *							if a boolean is true, that means we must update the
	 *							corresponding index because changes in the newRow
	 *							affect it.
	 *	@param	colsToUpdate	int array of columns to be updated (1 based).  If
	 *							null, all cols are updated
	 *	@param	tc			transaction controller
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void updateRow( ExecIndexRow				key, 
						   ExecRow[]				newRows, 
						   int						indexNumber,
						   boolean[]				indicesToUpdate,
						   int[]					colsToUpdate,
						   TransactionController	tc )
		throws StandardException;

	/**
	 * Updates a base row in a catalog and updates all the corresponding
	 * index rows.
	 * <p>
	 * LOCKING: exclusive row locking
	 *
	 *
	 *	@param	key				key row
	 *	@param	newRow			new version of the row
	 *	@param	indexNumber		index that key operates
	 *	@param	indicesToUpdate	array of booleans, one for each index on the catalog.
	 *							if a boolean is true, that means we must update the
	 *							corresponding index because changes in the newRow
	 *							affect it.
	 *	@param	colsToUpdate	int array of columns to be updated (1 based).  If
	 *							null, all cols are updated
	 *	@param	tc			transaction controller
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void updateRow( ExecIndexRow				key, 
						   ExecRow 					newRow, 
						   int						indexNumber,
						   boolean[]				indicesToUpdate,
						   int[]					colsToUpdate,
						   TransactionController	tc )
		throws StandardException;

	/**
	 * Updates a base row in a catalog and updates all the corresponding
	 * index rows.
	 * <p>
	 * LOCKING: exclusive row locking
	 *
	 *
	 *	@param	key				key row
	 *	@param	newRow			new version of the row
	 *	@param	indexNumber		index that key operates
	 *	@param	indicesToUpdate	array of booleans, one for each index on the catalog.
	 *							if a boolean is true, that means we must update the
	 *							corresponding index because changes in the newRow
	 *							affect it.
	 *	@param	colsToUpdate	int array of columns to be updated (1 based).  If
	 *							null, all cols are updated
	 *	@param	tc			transaction controller
	 *	@param wait		If true, then the caller wants to wait for locks. False will be
	 *							when we using a nested user xaction - we want to timeout right away
	 *							if the parent holds the lock.  (bug 4821)
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void updateRow( ExecIndexRow				key, 
						   ExecRow 					newRow, 
						   int						indexNumber,
						   boolean[]				indicesToUpdate,
						   int[]					colsToUpdate,
						   TransactionController	tc,
						   boolean	wait )
		throws StandardException;
	/**
	 * Get the Properties associated with creating the heap.
	 *
	 * @return The Properties associated with creating the heap.
	 */
	public Properties getCreateHeapProperties();

	/**
	 * Get the Properties associated with creating the specified index.
	 *
	 * @param indexNumber	The specified index number.
	 *
	 * @return The Properties associated with creating the specified index.
	 */
	public Properties getCreateIndexProperties(int indexNumber);
}
