/*

   Derby - Class org.apache.derby.impl.sql.catalog.TabInfoImpl

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

package org.apache.derby.impl.sql.catalog;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.StreamStorable;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.sql.dictionary.RowList;
import org.apache.derby.iapi.sql.dictionary.TabInfo;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.RowChanger;
import org.apache.derby.iapi.sql.execute.TupleFilter;
import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;

import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.catalog.UUID;
import java.util.Enumeration;
import java.util.Properties;

/**
* A poor mans structure used in DataDictionaryImpl.java.
* Used to save heapId, name pairs for non core tables.
*
* @author jamie
*/
public class TabInfoImpl implements TabInfo
{
	private IndexInfoImpl[]				indexes;
	private String						name;
	private long						heapConglomerate;
	private int							numIndexesSet;
	private boolean						heapSet;
	private UUID						uuid;
	private CatalogRowFactory			crf;

	private	ExecutionFactory			executionFactory;

	/**
	 * Constructor
	 *
	 * @param crf				the associated CatalogRowFactory
	 * @param executionFactory	execution factory of the database
	 */
	public TabInfoImpl(CatalogRowFactory crf)
	{
		this.name = crf.getCatalogName();
		this.heapConglomerate = -1;
		this.crf = crf;
		this.executionFactory = crf.getExecutionFactory();

		int numIndexes = crf.getNumIndexes();

		if (numIndexes > 0)
		{
			indexes = new IndexInfoImpl[numIndexes];

			/* Init indexes */
			for (int indexCtr = 0; indexCtr < numIndexes; indexCtr++)
			{
				indexes[indexCtr] = new IndexInfoImpl(
											-1, 
											crf.getIndexName(indexCtr),
											crf.getIndexColumnCount(indexCtr),
											crf.isIndexUnique(indexCtr),
											indexCtr,
											crf);
			}
		}
	}

	/**
	 * @see TabInfo#getHeapConglomerate
	 */
	public long getHeapConglomerate()
	{
		return heapConglomerate;
	}

	/**
	 * @see TabInfo#setHeapConglomerate
	 */
	public void setHeapConglomerate(long heapConglomerate)
	{
		this.heapConglomerate = heapConglomerate;
		heapSet = true;
	}

	/**
	 * @see TabInfo#getIndexConglomerate
	 */
	public long getIndexConglomerate(int indexID)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(indexes != null,
				"indexes is expected to be non-null");
            if (indexID >= indexes.length)
            {
                SanityManager.THROWASSERT(
                    "indexID (" + indexID + ") is out of range(0-" +
                    indexes.length + ")");
            }
		}

		return indexes[indexID].getConglomerateNumber();
	}

	/**
	 * @see TabInfo#setIndexConglomerate
	 */
	public void setIndexConglomerate(int index, long indexConglomerate)
	{
		/* Index names must be set before conglomerates.
		 * Also verify that we are not setting the same conglomerate
		 * twice.
		 */
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(indexes[index] != null,
				"indexes[index] expected to be non-null");
			SanityManager.ASSERT(indexes[index].getConglomerateNumber() == -1,
				"indexes[index] expected to be -1");
		}
		indexes[index].setConglomerateNumber(indexConglomerate);

		/* We are completely initialized when all indexes have 
		 * their conglomerates initialized 
		 */
		numIndexesSet++;
	}

	public void setIndexConglomerate(ConglomerateDescriptor cd)
	{
		int		index;
		String	indexName = cd.getConglomerateName();

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(indexes != null,
				"indexes is expected to be non-null");
		}

		for (index = 0; index < indexes.length; index++)
		{
			/* All index names expected to be set before
			 * any conglomerate is set.
			 */
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(indexes[index] != null,
					"indexes[index] expected to be non-null");
				SanityManager.ASSERT(indexes[index].getIndexName() != null,
					"indexes[index].getIndexName() expected to be non-null");
			}

			/* Do we have a match? */
			if (indexes[index].getIndexName().equals(indexName))
			{
				indexes[index].setConglomerateNumber(cd.getConglomerateNumber());
				break;
			}
		}

		if (SanityManager.DEBUG)
		{
			if (index == indexes.length)
			{
				SanityManager.THROWASSERT("match not found for " + indexName);
			}
		}

		/* We are completely initialized when all indexIds are initialized */
		numIndexesSet++;
	}

	/**
	 * @see TabInfo#getTableName
	 */
	public String getTableName()
	{
		return name;
	}

	/**
	 * @see TabInfo#getIndexName
	 */
	public String getIndexName(int indexId)
	{
		return indexes[indexId].getIndexName();
	}

	/**
	 * @see TabInfo#setIndexName
	 */
	public void setIndexName(int indexID, String indexName)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(indexes != null,
				"indexes is expected to be non-null");

			if (indexID >= indexes.length)
				SanityManager.THROWASSERT(
				"indexID (" + indexID + ") is out of range(0-" +
				indexes.length + ")");
		}
		indexes[indexID].setIndexName(indexName);
	}

	/**
	 * @see TabInfo#getCatalogRowFactory
	 */
	public CatalogRowFactory getCatalogRowFactory()
	{
		return crf;
	}

	/**
	 * @see TabInfo#isComplete
	 */
	public boolean isComplete()
	{
		/* We are complete when heap conglomerate and all
		 * index conglomerates are set.
		 */
		if (! heapSet)
		{
			return false;
		}
		return (indexes == null ||	indexes.length == numIndexesSet);
	}

	/**
	 * @see TabInfo#getIndexColumnCount
	 */
	public int getIndexColumnCount(int indexNumber)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(indexes != null,
				"indexes is expected to be non-null");

			if (!(indexNumber < indexes.length))
			{
				SanityManager.THROWASSERT("indexNumber (" + indexNumber + ") is out of range(0-" +
				indexes.length + ")");
			}
		}

		return indexes[indexNumber].getColumnCount();
	}

	/**
	 * @see TabInfo#getIndexRowGenerator
	 */
	public IndexRowGenerator getIndexRowGenerator(int indexNumber)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(indexes != null,
				"indexes is expected to be non-null");
            if (indexNumber >= indexes.length)
            {
                SanityManager.THROWASSERT(
                    "indexNumber (" + indexNumber + ") is out of range(0-" +
                    indexes.length + ")");
            }
		}
		return indexes[indexNumber].getIndexRowGenerator();
	}

	/**
	 * @see TabInfo#setIndexRowGenerator
	 */
	public void setIndexRowGenerator(int indexNumber, IndexRowGenerator irg)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(indexes != null,
				"indexes is expected to be non-null");
            if (indexNumber >= indexes.length)
            {
                SanityManager.THROWASSERT(
                    "indexNumber (" + indexNumber + ") is out of range(0-" +
                    indexes.length + ")");
            }
		}

		indexes[indexNumber].setIndexRowGenerator(irg);
	}

	/**
	 * @see TabInfo#getNumberOfIndexes
	 */
	public int getNumberOfIndexes()
	{
		if (indexes == null)
		{
			return 0;
		}
		else
		{
			return indexes.length;
		}
	}

	/**
	 * @see TabInfo#getBaseColumnPosition
	 */
	public int getBaseColumnPosition(int indexNumber, int colNumber)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(indexes != null,
				"indexes is expected to be non-null");
			if (indexNumber >= indexes.length)
			{
				SanityManager.THROWASSERT("indexNumber (" + indexNumber + ") is out of range(0-" +
					indexes.length + ")");
			}
		}

		return indexes[indexNumber].getBaseColumnPosition(colNumber);
	}

	/**
	 * @see TabInfo#setBaseColumnPosition
	 */
	public void setBaseColumnPosition(int indexNumber, int colNumber,
									 int baseColumnPosition)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(indexes != null,
				"indexes is expected to be non-null");
			if (indexNumber >= indexes.length)
			{
				SanityManager.THROWASSERT("indexNumber (" + indexNumber + ") is out of range(0-" +
					indexes.length + ")");
			}
		}

		indexes[indexNumber].setBaseColumnPosition(colNumber, baseColumnPosition);
	}

	/**
	 * @see TabInfo#isIndexUnique
	 */
	public boolean isIndexUnique(int indexNumber)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(indexes != null,
				"indexes is expected to be non-null");

			if (indexNumber >= indexes.length)
			{
				SanityManager.THROWASSERT("indexNumber (" + indexNumber + ") is out of range(0-" +
					indexes.length + ")");
			}
		}

		return indexes[indexNumber].isIndexUnique();
	}

	/**
	 * Inserts a base row into a catalog and inserts all the corresponding
	 * index rows.
	 *
	 *	@param	row			row to insert
	 *	@param	tc			transaction
	 *	@param	wait		to wait on lock or quickly TIMEOUT
	 *	@return	row number (>= 0) if duplicate row inserted into an index
	 *			ROWNOTDUPLICATE otherwise
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public int insertRow( ExecRow row, TransactionController tc, boolean wait)
		throws StandardException
	{
		RowList					rowList = new RowList( this );

		rowList.add(row);

		RowLocation[] 			notUsed = new RowLocation[1]; 

		return insertRowListImpl(rowList,tc,notUsed, wait);
	}


	/**
	 * Inserts a base row into a catalog and inserts all the corresponding
	 * index rows.
	 *
	 *	@param	row			row to insert
	 *	@param	lcc			language state variable
	 *	@return	row number (>= 0) if duplicate row inserted into an index
	 *			ROWNOTDUPLICATE otherwise
	 *
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public int insertRow( ExecRow row, LanguageConnectionContext lcc )
		throws StandardException
	{
		RowList					rowList = new RowList( this );

		rowList.add(row);

		return	insertRowList( rowList, lcc );
	}

	/**
	 @see TabInfo#insertRowAndFetchRowLocation
	 @exception StandardException Thrown on failure
	 */
	public RowLocation insertRowAndFetchRowLocation(ExecRow row, TransactionController tc)
		throws StandardException
	{
		RowList	rowList = new RowList( this );
		rowList.add(row);
		RowLocation[] rowLocationOut = new RowLocation[1]; 
		insertRowListImpl(rowList,tc,rowLocationOut, true);
		return rowLocationOut[0];
	}

	/**
	 * Deletes a list of keyed rows from a catalog and all the corresponding
	 * index rows. Deletes through the first index--all tuples are assumed to
	 * be keys into the first index.
	 *
	 *	@param	rowList		List of keyed rows to delete
	 *	@param	lcc			language state variable
	 *
	 * @return the number of rows deleted.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public int deleteRowList( RowList rowList, LanguageConnectionContext lcc )
		throws StandardException
	{
		int						totalRows = 0;
		int						indexNumber;
		ExecIndexRow			key;
		Enumeration	       		iterator;
		TransactionController	tc = lcc.getTransactionExecute();

		// loop through rows on this list, deleting them through the first index.
		for (iterator =  rowList.elements(); iterator.hasMoreElements(); )
		{
			indexNumber = crf.getPrimaryKeyIndexNumber();

			key = (ExecIndexRow) iterator.nextElement();
			totalRows += deleteRow( tc, key, indexNumber );
		}

		return	totalRows;
	}

	/**
	 * Inserts a list of base rows into a catalog and inserts all the corresponding
	 * index rows.
	 *
	 *	@param	rowList		List of rows to insert
	 *	@param	tc			transaction controller
	 *
	 *
	 *	@return	row  number (>= 0) if duplicate row inserted into an index
	 *			ROWNOTDUPLICATE otherwise
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public int insertRowList( RowList rowList, TransactionController tc )
		throws StandardException
	{
		RowLocation[] 			notUsed = new RowLocation[1]; 

		return insertRowListImpl(rowList,tc,notUsed, true);
	}

	/**
	 * Inserts a list of base rows into a catalog and inserts all the corresponding
	 * index rows.
	 *
	 *	@param	rowList		List of rows to insert
	 *	@param	lcc			language state variable
	 *
	 *
	 *	@return	row  number (>= 0) if duplicate row inserted into an index
	 *			ROWNOTDUPLICATE otherwise
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public int insertRowList( RowList rowList, LanguageConnectionContext lcc )
		throws StandardException
	{
		TransactionController	tc = lcc.getTransactionExecute();

		return insertRowList(rowList,tc);
	}

	/**
	  Insert logic to insert a list of rows into a table. This logic has two
	  odd features to support the TabInfo interface.

	  <OL>
	  <LI>Returns an indication if any returned row was a duplicate.
	  <LI>Returns the RowLocation of the last row inserted.
	  </OL>
	  @param rowList the list of rows to insert
	  @param tc	transaction controller
	  @param rowLocationOut on output rowLocationOut[0] is set to the
	         last RowLocation inserted.
	  @param wait   to wait on lock or quickly TIMEOUT
	  @return row number (>= 0) if duplicate row inserted into an index
	  			ROWNOTDUPLICATE otherwise
	 */
	private int insertRowListImpl( RowList rowList, TransactionController tc, RowLocation[] rowLocationOut,
								   boolean wait)
		throws StandardException
	{
		ConglomerateController		heapController;
		RowLocation					heapLocation;
		ExecIndexRow				indexableRow;
		int							insertRetCode;
		int							retCode = ROWNOTDUPLICATE;
		int							indexCount = crf.getNumIndexes();
		ConglomerateController[]	indexControllers = new ConglomerateController[ indexCount ];
		Enumeration	       			iterator;
		ExecRow						row;

		// Open the conglomerates
		heapController = 
            tc.openConglomerate(
                getHeapConglomerate(), 
                false,
				(TransactionController.OPENMODE_FORUPDATE |
                    ((wait) ? 0 : TransactionController.OPENMODE_LOCK_NOWAIT)),
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_REPEATABLE_READ);
		
		/* NOTE: Due to the lovely problem of trying to add
		 * a new column to syscolumns and an index on that
		 * column during upgrade, we have to deal with the
		 * issue of the index not existing yet.  So, it's okay
		 * if the index doesn't exist yet.  (It will magically
		 * get created at a later point during upgrade.)
		 */

		for ( int ictr = 0; ictr < indexCount; ictr++ )
		{
			long conglomNumber = getIndexConglomerate(ictr);
			if (conglomNumber > -1)
			{
				indexControllers[ ictr ] = 
		            tc.openConglomerate( 
			            conglomNumber, 
                        false,
						(TransactionController.OPENMODE_FORUPDATE |
                    		((wait) ? 0 : TransactionController.OPENMODE_LOCK_NOWAIT)),
					    TransactionController.MODE_RECORD,
						TransactionController.ISOLATION_REPEATABLE_READ);
			}
		}

		heapLocation = heapController.newRowLocationTemplate();
		rowLocationOut[0]=heapLocation;

		// loop through rows on this list, inserting them into system table
		int rowNumber = 0;
		for (iterator =  rowList.elements(); iterator.hasMoreElements(); rowNumber++)
		{
			row = (ExecRow) iterator.nextElement();
			// insert the base row and get its new location 
			heapController.insertAndFetchLocation(row.getRowArray(), heapLocation);
			
			for ( int ictr = 0; ictr < indexCount; ictr++ )
		    {
				if (indexControllers[ ictr ] == null)
				{
					continue;
				}

				// Get an index row based on the base row
				indexableRow = getIndexRowFromHeapRow( getIndexRowGenerator(ictr),
													   heapLocation,
													   row );

				insertRetCode = 
                    indexControllers[ ictr ].insert(indexableRow.getRowArray());

				if ( insertRetCode == ConglomerateController.ROWISDUPLICATE )
				{
					retCode = rowNumber;
				}
			}

		}	// end loop through rows on list

		// Close the open conglomerates
		for ( int ictr = 0; ictr < indexCount; ictr++ )
		{
			if (indexControllers[ ictr ] == null)
			{
				continue;
			}

			indexControllers[ ictr ].close();
		}
		heapController.close();

		return	retCode;
	}

	/**
	  * @exception StandardException		Thrown on failure
	  * @see TabInfo#truncate
	  */
	public int truncate( TransactionController tc )
		 throws StandardException
	{
		ConglomerateController		heapCC;
		ScanController				drivingScan;
		RowLocation					baseRowLocation;
		RowChanger 					rc;
		ExecRow						baseRow = crf.makeEmptyRow();

		rc = getRowChanger( tc, (int[])null,baseRow );
		// Table level locking
		rc.open(TransactionController.MODE_TABLE);
		int rowsDeleted = 0;
		
		drivingScan = tc.openScan(
			getHeapConglomerate(),  // conglomerate to open
			false,        // don't hold open across commit
            TransactionController.OPENMODE_FORUPDATE, // for update
            TransactionController.MODE_TABLE,
            TransactionController.ISOLATION_REPEATABLE_READ,
			(FormatableBitSet) null, // all fields as objects
			null,         // start position - first row
            ScanController.NA,
			null,         //scanQualifier
			null,         // stop position - through last row
            ScanController.NA
                           // startSearchOperation
			);     

		/* Open the heap conglomerate */
		heapCC = tc.openConglomerate(
                    getHeapConglomerate(),
                    false,
                    TransactionController.OPENMODE_FORUPDATE,
                    TransactionController.MODE_TABLE,
                    TransactionController.ISOLATION_REPEATABLE_READ);

		baseRowLocation = heapCC.newRowLocationTemplate();
		while (drivingScan.next())
		{
			rowsDeleted++;
			drivingScan.fetchLocation(baseRowLocation);
			boolean base_row_exists = 
                heapCC.fetch(
                    baseRowLocation, baseRow.getRowArray(), (FormatableBitSet) null);

            if (SanityManager.DEBUG)
            {
                // it can not be possible for heap row to disappear while 
                // holding scan cursor on index at ISOLATION_REPEATABLE_READ.
                SanityManager.ASSERT(base_row_exists, "base row not found");
            }
			rc.deleteRow( baseRow, baseRowLocation );
		}

		heapCC.close();
		drivingScan.close();
		rc.close();
		return rowsDeleted;
	}

	/**
	 * LOCKING: row locking if there there is a key
	 *
	 * @exception StandardException		Thrown on failure
	 * @see TabInfo#deleteRow
	 */
	public int deleteRow( TransactionController tc, ExecIndexRow key, int indexNumber )
		throws StandardException
	{
		// Always row locking
		return  deleteRows(tc,
						   key,
						   ScanController.GE,
						   null,
						   null,
						   key,
						   ScanController.GT,
						   indexNumber);
	}

	/**
	 * LOCKING: row locking if there is both a start and
	 * stop key; otherwise table locking
	 *
	 * @exception StandardException		Thrown on failure
	 * @see TabInfo#deleteRows
	 */
	public int deleteRows(TransactionController tc,
						  ExecIndexRow startKey,
						  int startOp,
						  Qualifier[][] qualifier,
						  TupleFilter filter,
						  ExecIndexRow stopKey,
						  int stopOp,
						  int indexNumber)
		 throws StandardException
	{
		ConglomerateController		heapCC;
		ScanController				drivingScan;
		ExecIndexRow	 			drivingIndexRow;
		RowLocation					baseRowLocation;
		RowChanger 					rc;
		ExecRow						baseRow = crf.makeEmptyRow();
		int                         rowsDeleted = 0;
		boolean						passedFilter = true;

		rc = getRowChanger( tc, (int[])null,baseRow );

		/*
		** If we have a start and a stop key, then we are going to 
		** get row locks, otherwise, we are getting table locks.
		** This may be excessive locking for the case where there
		** is a start key and no stop key or vice versa.
		*/
		int lockMode = ((startKey != null) && (stopKey != null)) ? 
				tc.MODE_RECORD : 
				tc.MODE_TABLE;

		/*
		** Don't use level 3 if we have the same start/stop key.
		*/
		int isolation = 
            ((startKey != null) && (stopKey != null) && (startKey == stopKey)) ?
				TransactionController.ISOLATION_REPEATABLE_READ :
				TransactionController.ISOLATION_SERIALIZABLE;

		// Row level locking
		rc.open(lockMode);

		DataValueDescriptor[] startKeyRow = 
            startKey == null ? null : startKey.getRowArray();

		DataValueDescriptor[] stopKeyRow = 
            stopKey == null  ? null : stopKey.getRowArray();

		/* Open the heap conglomerate */
		heapCC = tc.openConglomerate(
                    getHeapConglomerate(),
                    false,
                    TransactionController.OPENMODE_FORUPDATE,
                    lockMode,
                    TransactionController.ISOLATION_REPEATABLE_READ);

		drivingScan = tc.openScan(
			getIndexConglomerate(indexNumber),  // conglomerate to open
			false, // don't hold open across commit
            TransactionController.OPENMODE_FORUPDATE, // for update
            lockMode,
			isolation,
			(FormatableBitSet) null, // all fields as objects
			startKeyRow,   // start position - first row
            startOp,      // startSearchOperation
			qualifier, //scanQualifier
			stopKeyRow,   // stop position - through last row
            stopOp);     // stopSearchOperation

		// Get an index row based on the base row
		drivingIndexRow = getIndexRowFromHeapRow(
			getIndexRowGenerator( indexNumber ),
			heapCC.newRowLocationTemplate(),
			crf.makeEmptyRow());

		while (drivingScan.next())
		{
			drivingScan.fetch(drivingIndexRow.getRowArray());
			baseRowLocation = (RowLocation)
						drivingIndexRow.getColumn(drivingIndexRow.nColumns());

			boolean base_row_exists = 
                heapCC.fetch(
                    baseRowLocation, baseRow.getRowArray(), (FormatableBitSet) null);

            if (SanityManager.DEBUG)
            {
                // it can not be possible for heap row to disappear while 
                // holding scan cursor on index at ISOLATION_REPEATABLE_READ.
                SanityManager.ASSERT(base_row_exists, "base row not found");
            }

			// only delete rows which pass the base-row filter
			if ( filter != null ) { passedFilter = filter.execute( baseRow ).equals( true ); }
			if ( passedFilter )
			{
				rc.deleteRow( baseRow, baseRowLocation );
				rowsDeleted++;
			}
		}

		heapCC.close();
		drivingScan.close();
		rc.close();
		return rowsDeleted;
	}

	/**
	  * @exception StandardException		Thrown on failure
	  * @see TabInfo#getRow
	  */
	public ExecRow getRow( TransactionController tc,
						ExecIndexRow key,
						int indexNumber )
		throws StandardException
	{
		ConglomerateController		heapCC;

		/* Open the heap conglomerate */
		heapCC = tc.openConglomerate(
                    getHeapConglomerate(),
                    false,
                    0, 						// for read only
                    TransactionController.MODE_RECORD,
                    TransactionController.ISOLATION_REPEATABLE_READ);

		try { return getRow( tc, heapCC, key, indexNumber ); }
		finally { heapCC.close(); }
	}

	/**
	 * Given an index row and index number return the RowLocation
	 * in the heap of the first matching row.
	 * Used by the autoincrement code to get the RowLocation in
	 * syscolumns given a <tablename, columname> pair.
	 * 
	 * @see DataDictionaryImpl#computeRowLocation(TransactionController, TableDescriptor, String)
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
			  throws StandardException
	{
		ConglomerateController		heapCC;
		heapCC = tc.openConglomerate(
                    getHeapConglomerate(),
                    false,
                    0, 						// for read only
                    TransactionController.MODE_RECORD,
                    TransactionController.ISOLATION_REPEATABLE_READ);

		try 
		{
			RowLocation rl[] = new RowLocation[1];
			ExecRow notUsed = getRowInternal(tc, heapCC, key, indexNumber, rl);
			return rl[0];
		}
		finally
		{
			heapCC.close();
		}
	}
	/**
	  * @exception StandardException		Thrown on failure
	  * @see TabInfo#getRow
	  */
	public ExecRow getRow( TransactionController tc,
						   ConglomerateController heapCC,
						   ExecIndexRow key,
						   int indexNumber)
							
		 throws StandardException
	{
		RowLocation rl[] = new RowLocation[1];
		return getRowInternal(tc, heapCC, key, indexNumber, rl);
	}

	/**
	  * @exception StandardException		Thrown on failure
	  * @see TabInfo#getRow
	  */
	private ExecRow getRowInternal( TransactionController tc,
									ConglomerateController heapCC,
									ExecIndexRow key,
									int indexNumber,
									RowLocation rl[])

		 throws StandardException
	{
		ScanController				drivingScan;
		ExecIndexRow	 			drivingIndexRow;
		RowLocation					baseRowLocation;
		ExecRow						baseRow = crf.makeEmptyRow();

		drivingScan = tc.openScan(
			getIndexConglomerate(indexNumber),
			                     // conglomerate to open
			false,               // don't hold open across commit
			0,                   // open for read
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_REPEATABLE_READ,
			(FormatableBitSet) null,      // all fields as objects
			key.getRowArray(),   // start position - first row
            ScanController.GE,   // startSearchOperation
			null,                //scanQualifier
			key.getRowArray(),   // stop position - through last row
            ScanController.GT);  // stopSearchOperation

		// Get an index row based on the base row
		drivingIndexRow = getIndexRowFromHeapRow(
			getIndexRowGenerator( indexNumber ),
			heapCC.newRowLocationTemplate(),
			crf.makeEmptyRow());

		try	{
			if (drivingScan.next())
			{
				drivingScan.fetch(drivingIndexRow.getRowArray());
				rl[0] = baseRowLocation = (RowLocation)
					drivingIndexRow.getColumn(drivingIndexRow.nColumns());
				boolean base_row_exists = 
                    heapCC.fetch(
                        baseRowLocation, baseRow.getRowArray(), (FormatableBitSet) null);

                if (SanityManager.DEBUG)
                {
                    // it can not be possible for heap row to disappear while 
                    // holding scan cursor on index at ISOLATION_REPEATABLE_READ.
                    SanityManager.ASSERT(base_row_exists, "base row not found");
                }

				return baseRow;
			}
			else
			{
				return null;
			}
		}

		finally {
			drivingScan.close();
		}
	}

	/**
	 * Updates a base row in a catalog and updates all the corresponding
	 * index rows.
	 *
	 *	@param	key			key row
	 *	@param	newRow		new version of the row
	 *	@param	indexNumber	index that key operates
	 *	@param	indicesToUpdate	array of booleans, one for each index on the catalog.
	 *							if a boolean is true, that means we must update the
	 *							corresponding index because changes in the newRow
	 *							affect it.
	 *	@param  colsToUpdate	array of ints indicating which columns (1 based)
	 *							to update.  If null, do all.
	 *	@param	tc			transaction controller
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void updateRow( ExecIndexRow				key, 
						   ExecRow					newRow, 
						   int						indexNumber,
						   boolean[]				indicesToUpdate,
						   int[]					colsToUpdate,
						   TransactionController	tc )
		throws StandardException
	{
		updateRow(key, newRow, indexNumber, indicesToUpdate, colsToUpdate, tc, true);
	}

	/**
	 * Updates a base row in a catalog and updates all the corresponding
	 * index rows.
	 *
	 *	@param	key			key row
	 *	@param	newRow		new version of the row
	 *	@param	indexNumber	index that key operates
	 *	@param	indicesToUpdate	array of booleans, one for each index on the catalog.
	 *							if a boolean is true, that means we must update the
	 *							corresponding index because changes in the newRow
	 *							affect it.
	 *	@param  colsToUpdate	array of ints indicating which columns (1 based)
	 *							to update.  If null, do all.
	 *	@param	tc			transaction controller
	 *	@param wait		If true, then the caller wants to wait for locks. False will be
	 *	when we using a nested user xaction - we want to timeout right away if the parent
	 *	holds the lock.  (bug 4821)
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void updateRow( ExecIndexRow				key, 
						   ExecRow					newRow, 
						   int						indexNumber,
						   boolean[]				indicesToUpdate,
						   int[]					colsToUpdate,
						   TransactionController	tc,
						   boolean	wait )
		throws StandardException
	{
		ExecRow[] newRows = new ExecRow[1];
		newRows[0] = newRow;
		updateRow(key, newRows, indexNumber, indicesToUpdate, colsToUpdate, tc, wait);
	}

	/**
	 * Updates a set of base rows in a catalog with the same key on an index
	 * and updates all the corresponding index rows. 
	 *
	 *	@param	key			key row
	 *	@param	newRows		new version of the array of rows
	 *	@param	indexNumber	index that key operates
	 *	@param	indicesToUpdate	array of booleans, one for each index on the catalog.
	 *							if a boolean is true, that means we must update the
	 *							corresponding index because changes in the newRow
	 *							affect it.
	 *	@param  colsToUpdate	array of ints indicating which columns (1 based)
	 *							to update.  If null, do all.
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
		throws StandardException
	{
		updateRow(key, newRows, indexNumber, indicesToUpdate, colsToUpdate, tc, true);
	}

	/**
	 * Updates a set of base rows in a catalog with the same key on an index
	 * and updates all the corresponding index rows. If parameter wait is true,
	 * then the caller wants to wait for locks. When using a nested user xaction
	 * we want to timeout right away if the parent holds the lock.
	 *
	 *	@param	key			key row
	 *	@param	newRows		new version of the array of rows
	 *	@param	indexNumber	index that key operates
	 *	@param	indicesToUpdate	array of booleans, one for each index on the catalog.
	 *							if a boolean is true, that means we must update the
	 *							corresponding index because changes in the newRow
	 *							affect it.
	 *	@param  colsToUpdate	array of ints indicating which columns (1 based)
	 *							to update.  If null, do all.
	 *	@param	tc			transaction controller
	 *	@param wait		If true, then the caller wants to wait for locks. When
	 *							using a nested user xaction we want to timeout right away
	 *							if the parent holds the lock. (bug 4821)
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void updateRow( ExecIndexRow				key,
						   ExecRow[]				newRows,
						   int						indexNumber,
						   boolean[]				indicesToUpdate,
						   int[]					colsToUpdate,
						   TransactionController	tc,
						   boolean wait)
		throws StandardException
	{
		ConglomerateController		heapCC;
		ScanController				drivingScan;
		ExecIndexRow	 			drivingIndexRow;
		RowLocation					baseRowLocation;
		ExecIndexRow				templateRow;
		ExecRow						baseRow = crf.makeEmptyRow();

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT( indicesToUpdate.length == crf.getNumIndexes(),
								 "Wrong number of indices." );
		}

		RowChanger 					rc  = getRowChanger( tc, colsToUpdate,baseRow );

		// Row level locking
		rc.openForUpdate(indicesToUpdate, TransactionController.MODE_RECORD, wait); 

		/* Open the heap conglomerate */
		heapCC = tc.openConglomerate(
                    getHeapConglomerate(),
                    false,
                    (TransactionController.OPENMODE_FORUPDATE |
                    ((wait) ? 0 : TransactionController.OPENMODE_LOCK_NOWAIT)),
                    TransactionController.MODE_RECORD,
                    TransactionController.ISOLATION_REPEATABLE_READ);

		drivingScan = tc.openScan(
			getIndexConglomerate(indexNumber),  // conglomerate to open
			false, // don't hold open across commit
			(TransactionController.OPENMODE_FORUPDATE |
            ((wait) ? 0 : TransactionController.OPENMODE_LOCK_NOWAIT)), 
            TransactionController.MODE_RECORD,
            TransactionController.ISOLATION_REPEATABLE_READ,
			(FormatableBitSet) null,     // all fields as objects
			key.getRowArray(),   // start position - first row
            ScanController.GE,      // startSearchOperation
			null, //scanQualifier
			key.getRowArray(),   // stop position - through last row
            ScanController.GT);     // stopSearchOperation

		// Get an index row based on the base row
		drivingIndexRow = getIndexRowFromHeapRow(
			getIndexRowGenerator( indexNumber ),
			heapCC.newRowLocationTemplate(),
			crf.makeEmptyRow());

		int rowNum = 0;
		while (drivingScan.next())
		{
			drivingScan.fetch(drivingIndexRow.getRowArray());

			baseRowLocation = (RowLocation)
						drivingIndexRow.getColumn(drivingIndexRow.nColumns());
			boolean base_row_exists = 
                heapCC.fetch(
                    baseRowLocation, baseRow.getRowArray(), (FormatableBitSet) null);

            if (SanityManager.DEBUG)
            {
                // it can not be possible for heap row to disappear while 
                // holding scan cursor on index at ISOLATION_REPEATABLE_READ.
                SanityManager.ASSERT(base_row_exists, "base row not found");
            }
			
			rc.updateRow(baseRow, (rowNum == newRows.length - 1) ?
						newRows[rowNum] : newRows[rowNum++], baseRowLocation );
		}
		rc.finish();
		heapCC.close();
		drivingScan.close();
		rc.close();
	}

	/**
	 * Get the Properties associated with creating the heap.
	 *
	 * @return The Properties associated with creating the heap.
	 */
	public Properties getCreateHeapProperties()
	{
		return crf.getCreateHeapProperties();
	}

	/**
	 * Get the Properties associated with creating the specified index.
	 *
	 * @param indexNumber	The specified index number.
	 *
	 * @return The Properties associated with creating the specified index.
	 */
	public Properties getCreateIndexProperties(int indexNumber)
	{
		return crf.getCreateIndexProperties(indexNumber);
	}

	/**
	  *	Gets a row changer for this catalog.
	  *
	  *	@param	tc	transaction controller
	  *	@param	changedCols	the columns to change (1 based), may be null
	  * @param  baseRow used to detemine column types at creation time
	  *         only. The row changer does ***Not*** keep a referance to
	  *         this row or change it in any way.
	  *
	  *	@return	a row changer for this catalog.
	  * @exception StandardException		Thrown on failure
	  */
	private	RowChanger	getRowChanger( TransactionController tc,
									   int[] changedCols,
									   ExecRow baseRow)
		throws StandardException
	{
		RowChanger 					rc;
		int							indexCount = crf.getNumIndexes();
		IndexRowGenerator[]			irgs = new IndexRowGenerator[ indexCount ];
		long[]						cids = new long[ indexCount ];

		if (SanityManager.DEBUG)
		{
			if (changedCols != null)
			{
				for (int i = changedCols.length - 1; i >= 0; i--)
				{
					SanityManager.ASSERT(changedCols[i] != 0, 
						"Column id is 0, but should be 1 based");
				}
			}
		}

		for ( int ictr = 0; ictr < indexCount; ictr++ )
		{
			irgs[ictr] = getIndexRowGenerator(ictr);
			cids[ictr] = getIndexConglomerate(ictr);
		}

		rc = executionFactory.getRowChanger(getHeapConglomerate(),
											(StaticCompiledOpenConglomInfo) null,
											(DynamicCompiledOpenConglomInfo) null,
											irgs,
											cids,
											(StaticCompiledOpenConglomInfo[]) null,
											(DynamicCompiledOpenConglomInfo[]) null,
											crf.getHeapColumnCount(),
											tc,
											changedCols,
											getStreamStorableHeapColIds(baseRow),
											(Activation) null);
		return	rc;
	}

	private boolean computedStreamStorableHeapColIds = false;
	private int[] streamStorableHeapColIds;
	private int[] getStreamStorableHeapColIds(ExecRow baseRow) throws StandardException
	{
		if (!computedStreamStorableHeapColIds)
		{
			int sshcidLen = 0;
			//
			//Compute the length of streamStorableHeapColIds
			//One entry for each column id.
			DataValueDescriptor[] ra = baseRow.getRowArray();
			for(int ix=0;ix<ra.length;ix++)
				if (ra[ix] instanceof StreamStorable) sshcidLen++;

			//
			//If we have some streamStorableHeapColIds we
			//allocate an array to remember them and fill in
			//the array with the 0 based column ids. If we
			//have none leave streamStorableHeapColIds Null.
			if (sshcidLen > 0)
			{
				streamStorableHeapColIds = new int[sshcidLen];
				int sshcidOffset=0;
				for(int ix=0;ix<ra.length;ix++)
					if (ra[ix] instanceof StreamStorable)
 						streamStorableHeapColIds[sshcidOffset++] = ix;
			}
			computedStreamStorableHeapColIds = true;
		}
		return streamStorableHeapColIds;
	}

	/**
	 * Get an index row based on a row from the heap.
	 *
	 * @param irg		IndexRowGenerator to use
	 * @param rl		RowLocation for heap
	 * @param heapRow	Row from the heap
	 *
	 * @return ExecIndexRow	Index row.
	 *
	 * @exception StandardException		Thrown on error
	 */
	private ExecIndexRow getIndexRowFromHeapRow(IndexRowGenerator irg,
								   RowLocation rl,
								   ExecRow heapRow)
		throws StandardException
	{
		ExecIndexRow		indexRow;

		indexRow = irg.getIndexRowTemplate();
		// Get an index row based on the base row
		irg.getIndexRow(heapRow, rl, indexRow, (FormatableBitSet) null);

		return indexRow;
	}

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return "name: " + name + 
				"\n\theapCongolomerate: "+heapConglomerate +
				"\n\tnumIndexes: " + ((indexes != null) ? indexes.length : 0) +
				"\n\tnumIndexesSet: " + numIndexesSet +
				"\n\theapSet: " + heapSet +
				"\n\tuuid: " + uuid; 
		}
		else
		{
			return "";
		}
	}
}
