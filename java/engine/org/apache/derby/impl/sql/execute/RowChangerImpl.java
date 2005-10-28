/*

   Derby - Class org.apache.derby.impl.sql.execute.RowChangerImpl

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.io.FormatableBitSet;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.RowChanger;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.TemporaryRowHolder;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.RowLocation;

import java.util.Vector;

/**
  Perform row at a time DML operations of tables and maintain indexes.
  */
public class RowChangerImpl	implements	RowChanger
{
	boolean isOpen = false;

	//
	//Stuff provided to the constructor
	boolean[] fixOnUpdate = null;
	long heapConglom;
	DynamicCompiledOpenConglomInfo heapDCOCI;
	StaticCompiledOpenConglomInfo heapSCOCI;
	long[] indexCIDS = null;
	DynamicCompiledOpenConglomInfo[] indexDCOCIs;
	StaticCompiledOpenConglomInfo[] indexSCOCIs;
	IndexRowGenerator[] irgs = null;
	Activation		activation;
	TransactionController	tc;
	FormatableBitSet 	changedColumnBitSet;	
	FormatableBitSet 	baseRowReadList;	
	protected int[]		baseRowReadMap;	//index=heap column, value=input row column.
	int[]		changedColumnIds;
	TemporaryRowHolderImpl	rowHolder;
	
	// for error reporting.
	String[]	indexNames;

	//
	//Stuff filled in by open
	protected ConglomerateController baseCC = null;
	protected RowLocation	baseRowLocation = null;
	IndexSetChanger isc;

	// a row array with all non-updated columns compacted out
	private DataValueDescriptor[] sparseRowArray;
	private	int[] partialChangedColumnIds;
	
	/**
	  Create a new RowChanger for performing update and delete operations
	  based on partial before and after rows.

	  @param heapConglom Conglomerate # for the heap
	  @param heapSCOCI	SCOCI for heap.
	  @param heapDCOCI	DCOCI for heap
	  @param irgs the IndexRowGenerators for the table's indexes. We use
	    positions in this array as local id's for indexes. To support updates,
	    only indexes that change need be included.
	  @param indexCIDS the conglomerateids for the table's idexes. 
	  	indexCIDS[ix] corresponds to the same index as irgs[ix].
	  @param indexSCOCIs the SCOCIs for the table's idexes. 
	  	indexSCOCIs[ix] corresponds to the same index as irgs[ix].
	  @param indexDCOCIs the DCOCIs for the table's idexes. 
	  	indexDCOCIs[ix] corresponds to the same index as irgs[ix].
	  @param numberOfColumns	Number of columns in partial write row.
	  @param changedColumnIdsInput array of 1 based ints indicating the columns
		to be updated.  Only used for updates
	  @param tc the transaction controller
	  @param baseRowReadList bit set of columns read from base row. 1 based.
	  @param baseRowReadMap BaseRowReadMap[heapColId]->ReadRowColumnId. (0 based)
	  @exception StandardException		Thrown on error
	  */
	public RowChangerImpl(
			   long heapConglom,
			   StaticCompiledOpenConglomInfo heapSCOCI,
			   DynamicCompiledOpenConglomInfo heapDCOCI,
			   IndexRowGenerator[] irgs,
			   long[] indexCIDS,
			   StaticCompiledOpenConglomInfo[] indexSCOCIs,
			   DynamicCompiledOpenConglomInfo[] indexDCOCIs,
			   int numberOfColumns,
			   int[] changedColumnIdsInput,
			   TransactionController tc,
			   FormatableBitSet	baseRowReadList,
			   int[] baseRowReadMap,
			   Activation activation)
		 throws StandardException
	{
		this.heapConglom = heapConglom;
		this.heapSCOCI = heapSCOCI;
		this.heapDCOCI = heapDCOCI;
		this.irgs = irgs;
		this.indexCIDS = indexCIDS;
		this.indexSCOCIs = indexSCOCIs;
		this.indexDCOCIs = indexDCOCIs;
		this.tc = tc;
		this.baseRowReadList = baseRowReadList;
		this.baseRowReadMap = baseRowReadMap;
		this.activation = activation;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(indexCIDS != null, "indexCIDS is null");
		}

		/*
		** Construct the update column FormatableBitSet.
		** It is 0 based as opposed to the 1 based
		** changed column ids.
		*/
		if (changedColumnIdsInput != null)
		{
			/*
			** Sometimes replication does not have columns
			** in sorted order, and basically needs to
			** have the changed columns in non-sorted order.
			** So sort them first if needed.
			*/
			changedColumnIds = RowUtil.inAscendingOrder(changedColumnIdsInput) ?
								changedColumnIdsInput : sortArray(changedColumnIdsInput);

			/*
			** Allocate the row array we are going to use during
			** update here, to avoid extra work.  setup
			** the FormatableBitSet of columns being updated.  See updateRow
			** for the use.
			**
			** changedColumnIds is guaranteed to be in order, so just take
			** the last column number in the array to be the highest
			** column number.
			*/
			sparseRowArray =
				new DataValueDescriptor[changedColumnIds[changedColumnIds.length - 1] + 1];
			changedColumnBitSet = new FormatableBitSet(numberOfColumns);
			for (int i = 0; i < changedColumnIds.length; i++)
			{
				// make sure changedColumnBitSet can accomodate bit 
				// changedColumnIds[i] - 1 
				changedColumnBitSet.grow(changedColumnIds[i]);
				changedColumnBitSet.set(changedColumnIds[i] - 1);
			}

			/*
			** If we have a read map and a write map, we
			** need to have a way to map the changed column
			** ids to be relative to the read map.
			*/
			if (baseRowReadList != null)
			{
				partialChangedColumnIds = new int[changedColumnIds.length];
				int partialColumnNumber = 1;
				int currentColumn = 0;
				for (int i = 0; i < changedColumnIds.length; i++)
				{
					for (; currentColumn < changedColumnIds[i]; currentColumn++)
					{
						if (baseRowReadList.get(currentColumn))
						{
							partialColumnNumber++;
						}
					}
					partialChangedColumnIds[i] = partialColumnNumber;
				}
			}	
		}

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(indexCIDS != null, "indexCIDS is null");
		}
		
	}

	/**
	 * Set the row holder for this changer to use.
	 * If the row holder is set, it wont bother 
	 * saving copies of rows needed for deferred
	 * processing.  Also, it will never close the
	 * passed in rowHolder.
	 *
	 * @param rowHolder	the TemporaryRowHolder
	 */
	public void setRowHolder(TemporaryRowHolder rowHolder)
	{
		this.rowHolder = (TemporaryRowHolderImpl)rowHolder;
	}

	/**
	 * @see RowChanger#setIndexNames
	 */
	public void setIndexNames(String[] indexNames)
	{
		this.indexNames = indexNames;
	}

	/**
	  Open this RowChanger.

	  <P>Note to avoid the cost of fixing indexes that do not
	  change during update operations use openForUpdate().
	  @param lockMode	The lock mode to use
							(row or table, see TransactionController)

	  @exception StandardException thrown on failure to convert
	  */
	public void open(int lockMode)
		 throws StandardException
	{
		//
		//We open for update but say to fix every index on
		//updates.
		if (fixOnUpdate == null)
		{
			fixOnUpdate = new boolean[irgs.length];
			for (int ix = 0; ix < irgs.length; ix++)
				fixOnUpdate[ix] = true;
		}
		openForUpdate(fixOnUpdate, lockMode, true);
	}
	
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
	public void openForUpdate(
				  boolean[] fixOnUpdate, int lockMode, boolean wait
			  )
		 throws StandardException
	{
		LanguageConnectionContext lcc = null;

		if (SanityManager.DEBUG)
		    SanityManager.ASSERT( ! isOpen, "RowChanger already open");
		
		if (activation != null)
		{
			lcc = activation.getLanguageConnectionContext();
		}

		/* Isolation level - translate from language to store */
		int isolationLevel;
		if (lcc == null)
		{
			isolationLevel = ExecutionContext.READ_COMMITTED_ISOLATION_LEVEL;
		}
		else
		{
			isolationLevel = lcc.getCurrentIsolationLevel();
		}


		switch (isolationLevel)
		{
			// Even though we preserve the isolation level at READ UNCOMMITTED,
			// Cloudscape Store will overwrite it to READ COMMITTED for update.
			case ExecutionContext.READ_UNCOMMITTED_ISOLATION_LEVEL:
				isolationLevel = 
                    TransactionController.ISOLATION_READ_UNCOMMITTED;
				break;

			case ExecutionContext.READ_COMMITTED_ISOLATION_LEVEL:
				isolationLevel = 
                    TransactionController.ISOLATION_READ_COMMITTED;
				break;

			case ExecutionContext.REPEATABLE_READ_ISOLATION_LEVEL:
				isolationLevel = 
                    TransactionController.ISOLATION_REPEATABLE_READ;
				break;

			case ExecutionContext.SERIALIZABLE_ISOLATION_LEVEL:
				isolationLevel = 
                    TransactionController.ISOLATION_SERIALIZABLE;
				break;

			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT(
						"Invalid isolation level - " + isolationLevel);
				}
		}

		try {

		/* We can get called by either an activation or 
		 * the DataDictionary.  The DD cannot use the
		 * CompiledInfo while the activation can.
		 */
		if (heapSCOCI != null)
		{
	        baseCC =
				tc.openCompiledConglomerate(
					false,
                    (TransactionController.OPENMODE_FORUPDATE |
                    ((wait) ? 0 : TransactionController.OPENMODE_LOCK_NOWAIT)),
					lockMode,
					isolationLevel,
					heapSCOCI,
					heapDCOCI);
		}
		else
		{
	        baseCC =
				tc.openConglomerate(
					heapConglom,
					false,
                    (TransactionController.OPENMODE_FORUPDATE |
                    ((wait) ? 0 : TransactionController.OPENMODE_LOCK_NOWAIT)),
					lockMode,
					isolationLevel);
		}

		} catch (StandardException se) {
			if (activation != null)
				activation.checkStatementValidity();
			throw se;
		}

		/* Save the ConglomerateController off in the activation
		 * to eliminate the need to open it a 2nd time if we are doing
		 * and index to base row for the search as part of an update or
		 * delete below us.
		 * NOTE: activation can be null.  (We don't have it in
		 * the DataDictionary.)
		 */
		if (activation != null)
		{
			activation.checkStatementValidity();
			activation.setHeapConglomerateController(baseCC);
		}

		/* Only worry about indexes if there are indexes to worry about */
		if (indexCIDS.length != 0)
		{
			/* IndexSetChanger re-used across executions. */
			if (isc == null)
			{
				isc = new IndexSetChanger(irgs,
										  indexCIDS,
										  indexSCOCIs,
										  indexDCOCIs,
										  indexNames,
										  baseCC,
										  tc,
										  lockMode,
										  baseRowReadList,
										  isolationLevel,
										  activation
										  );
				isc.setRowHolder(rowHolder);
			}
			else
			{

				/* Propagate the heap's ConglomerateController to
				 * all of the underlying index changers.
				 */
				isc.setBaseCC(baseCC);
			}

			isc.open(fixOnUpdate);

			if (baseRowLocation == null)
				baseRowLocation = baseCC.newRowLocationTemplate();
		}

		isOpen = true;
	}
	   
	/**
	  Insert a row into the table and perform associated index maintenance.

	  @param baseRow the row.
	  @param baseRowLocation the row's base conglomerate
	     location
	  @exception StandardException		Thrown on error
	  */
	public void insertRow(ExecRow baseRow)
		 throws StandardException
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(! baseCC.isKeyed(),
								 "Keyed inserts not yet supported");

		if (baseCC.isKeyed())
		{
			//kcc.insert(row.key(), row());
		}
		else
		{
			if (isc != null)
			{
				baseCC.insertAndFetchLocation(baseRow.getRowArray(), baseRowLocation);
				isc.insert(baseRow, baseRowLocation);
			}
			else
			{
				baseCC.insert(baseRow.getRowArray());
			}
		}
	}

		
	/**
	  Delete a row from the table and perform associated index maintenance.

	  @param baseRow the row.
	  @param baseRowLocation the row's base conglomerate
	     location
	  @exception StandardException		Thrown on error
	  */
	public void deleteRow(ExecRow baseRow, RowLocation baseRowLocation)
		 throws StandardException
	{
		if (isc != null)
		{
			isc.delete(baseRow, baseRowLocation);
		}
		baseCC.delete(baseRowLocation);
	}

	/**
	  Update a row in the table and perform associated index maintenance.

	  @param ef	ExecutionFactory to use for cloning
	  @param oldBaseRow the old image of the row.
	  @param newBaseRow the new image of the row.
	  @param baseRowLocation the row's base conglomerate
	     location
	  @exception StandardException		Thrown on error
	  */
	public void updateRow(ExecRow oldBaseRow,
						  ExecRow newBaseRow,
						  RowLocation baseRowLocation)
		 throws StandardException
	{
		if (isc != null)
		{
			isc.update(oldBaseRow, newBaseRow, baseRowLocation);
		}

		if (changedColumnBitSet != null)
		{
			DataValueDescriptor[] baseRowArray = newBaseRow.getRowArray();
			int[] changedColumnArray = (partialChangedColumnIds == null) ?
					changedColumnIds : partialChangedColumnIds;
			int nextColumnToUpdate = -1;
			for (int i = 0; i < changedColumnArray.length; i++)
			{
				int copyFrom = changedColumnArray[i] - 1;
				nextColumnToUpdate =
							changedColumnBitSet.anySetBit(nextColumnToUpdate);
				if (SanityManager.DEBUG)
				{
					SanityManager.ASSERT(nextColumnToUpdate >= 0,
						"More columns in changedColumnArray than in changedColumnBitSet");
				}
				sparseRowArray[nextColumnToUpdate] = baseRowArray[copyFrom];
			}
		}
		else
		{
			sparseRowArray = newBaseRow.getRowArray();
		}
		baseCC.replace(baseRowLocation, 
					sparseRowArray, 
					changedColumnBitSet);
	}

	/**
	  Finish processing the changes.  This means applying the deferred
	  inserts for updates to unique indexes.

	  @exception StandardException		Thrown on error
	 */
	public void finish()
		throws StandardException
	{
		if (isc != null)
		{
			isc.finish();
		}
	}

	/**
	  Close this RowChanger.

	  @exception StandardException		Thrown on error
	  */
	public void close()
		throws StandardException
	{
		//
		//NOTE: isc uses baseCC. Since we close baseCC we free isc for now.
		//We could consider making isc open its own baseCC or even leaving
		//baseCC open to promote re-use. We must keep in mind that baseCC
		//is associated with the opener's TransactionController.
		if (isc != null)
		{
			isc.close(); 
		}

		if (baseCC != null)
		{
			if (activation == null || activation.getForUpdateIndexScan() == null)
				baseCC.close();		//beetle 3865, don't close if borrowed to cursor
			baseCC = null;
		}
		
		isOpen = false;

		// rowHolder is reused across executions and closed by caller
		// since caller creates it

		if (activation != null)
		{
			activation.clearHeapConglomerateController();
		}
	}

	/** @see RowChanger#getHeapConglomerateController */
	public ConglomerateController getHeapConglomerateController()
	{
		return baseCC;
	}

	private int[] sortArray(int[] input)
	{
		/*
		** Sotring.sort() will change the underlying array, so we
		** 'clone' it first
		*/
		int[] output = new int[input.length];
		System.arraycopy(input, 0, output, 0, input.length);
		java.util.Arrays.sort(output);
		return output;
	}
}
