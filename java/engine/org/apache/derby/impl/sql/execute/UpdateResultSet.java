/*

   Derby - Class org.apache.derby.impl.sql.execute.UpdateResultSet

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.services.stream.InfoStreams;
import org.apache.derby.iapi.services.io.StreamStorable;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.types.BooleanDataValue;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.RowChanger;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.db.TriggerExecutionContext;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import java.util.Properties;
import java.util.Hashtable;

/**
 * Update the rows from the specified
 * base table. This will cause constraints to be checked
 * and triggers to be executed based on the c's and t's
 * compiled into the update plan.
 *
 * @author ames
 */
public class UpdateResultSet extends DMLWriteResultSet
{
	public TransactionController 	tc;
	public ExecRow					newBaseRow;
	public ExecRow 					row;
	public ExecRow 					deferredSparseRow;
	public UpdateConstantAction		constants;
	
    private ResultDescription 		resultDescription;
	private NoPutResultSet			source;
	public	NoPutResultSet			savedSource;
	private RowChanger				rowChanger;

	protected ConglomerateController	deferredBaseCC;

	protected long[]				deferredUniqueCIDs;
	protected boolean[]				deferredUniqueCreated;
	protected ConglomerateController	deferredUniqueCC[];
	protected ScanController[]		deferredUniqueScans;
	public	LanguageConnectionContext lcc;

	private	TemporaryRowHolderImpl	deletedRowHolder;
	private	TemporaryRowHolderImpl	insertedRowHolder;

	// cached 
	private RISetChecker			riChecker;
	private	TriggerInfo				triggerInfo;
	private TriggerEventActivator	triggerActivator;
	private boolean					updatingReferencedKey;
	private boolean					updatingForeignKey;
	private	int						numOpens;
	private long					heapConglom; 
	private FKInfo[]				fkInfoArray;
	private FormatableBitSet 				baseRowReadList;
	private GeneratedMethod			checkGM;
	private int						resultWidth;
	private int						numberOfBaseColumns;
	private ExecRow					deferredTempRow;
	private ExecRow					deferredBaseRow;
	private ExecRow					oldDeletedRow;
	private ResultDescription		triggerResultDescription;

	int lockMode;
	boolean deferred;
	boolean beforeUpdateCopyRequired = false;

	/**
     * Returns the description of the updated rows.
     * REVISIT: Do we want this to return NULL instead?
	 */
	public ResultDescription getResultDescription()
	{
	    return resultDescription;
	}

    /*
     * class interface
     *
     */
    /**
	 * @param source update rows come from source
	 * @param checkGM	Generated method for enforcing check constraints
	 * @param compiledConstants constantAction for the update
	 * @exception StandardException thrown on error
     */
    public UpdateResultSet(NoPutResultSet source,
						   GeneratedMethod checkGM,
						   Activation activation)
      throws StandardException
    {
		this(source, checkGM , activation, activation.getConstantAction(),null);
	}

    /*
     * class interface
     *
     */
    /**
	 * @param source update rows come from source
	 * @param checkGM	Generated method for enforcing check constraints
	 * @param activation Activation
	 * @param constantActionItem  id of the update constant action saved objec
	 * @param rsdItem  id of the Result Description saved object
	 * @exception StandardException thrown on error
     */
    public UpdateResultSet(NoPutResultSet source,
						   GeneratedMethod checkGM,
						   Activation activation, 
						   int constantActionItem,
						   int rsdItem)
      throws StandardException
    {
		this(source, checkGM , activation,
			  ((ConstantAction)activation.getPreparedStatement().getSavedObject(constantActionItem)),
			 (ResultDescription) activation.getPreparedStatement().getSavedObject(rsdItem));
	
		// In case of referential action update, we do a deferred updates
		deferred = true;
	}


    /*
     * class interface
     *
     */
    /**
	 * @param source update rows come from source
	 * @param checkGM	Generated method for enforcing check constraints
	 * @param compiledConstants constantAction for the update
	 * @exception StandardException thrown on error
     */
    public UpdateResultSet(NoPutResultSet source,
						   GeneratedMethod checkGM,
						   Activation activation,
						   ConstantAction passedInConstantAction,
						   ResultDescription passedInRsd)
      throws StandardException
    {
		super(activation, passedInConstantAction);
		// find the language context.
		lcc = activation.getLanguageConnectionContext();
        // Get the current transaction controller
        tc = activation.getTransactionController();
		this.source = source;
		this.checkGM = checkGM;

		constants = (UpdateConstantAction) constantAction;
		fkInfoArray = constants.getFKInfo( lcc.getExecutionContext() );
		triggerInfo = constants.getTriggerInfo(lcc.getExecutionContext());

		heapConglom = constants.conglomId;

		baseRowReadList = constants.getBaseRowReadList();
		if(passedInRsd ==null)
			resultDescription = source.getResultDescription();
		else
			resultDescription = passedInRsd;
		/*
		** We NEED a result description when we are going to
		** to have to kick off a trigger.  In a replicated environment
		** we don't get a result description when we are replaying
		** source xacts on the target, which should never be the
		** case for an UpdateResultSet.
		*/
		if (SanityManager.DEBUG)
		{
			if (resultDescription == null)
			{
				SanityManager.ASSERT(triggerInfo == null, "triggers need a result description to pass to result sets given to users");
			}
		}

		if (fkInfoArray != null)
		{
			for (int i = 0; i < fkInfoArray.length; i++)
			{
				if (fkInfoArray[i].type == FKInfo.REFERENCED_KEY)
				{
					updatingReferencedKey = true;
					if (SanityManager.DEBUG)
					{
						SanityManager.ASSERT(constants.deferred, "updating referenced key but update not deferred, wuzzup?");
					}
				}
				else
				{	
					updatingForeignKey = true;
				}
			}
		}

		/* Get the # of columns in the ResultSet */
		resultWidth = resultDescription.getColumnCount();
		
		/*
		** Calculate the # of columns in the base table.  The result set
		** contains the before columns, the after columns, and the RowLocation,
		** so the number of base columns is half of the number of result set
		** columns, after subtracting one for the row location column.
		*/
		numberOfBaseColumns = (resultWidth - 1) / 2;
		
		/* Get the new base row */
		newBaseRow = RowUtil.getEmptyValueRow(numberOfBaseColumns, lcc);

		/* decode lock mode */
		lockMode = decodeLockMode(lcc, constants.lockMode);
		deferred = constants.deferred;
		
		//update can be marked for deferred mode because the scan is being done
		//using index. But it is not necesary  to keep the before copy
		//of the row in the temporary row holder (deletedRowHolder) unless
		//there are RI constraint or Triggers.(beetle:5301)
		if(triggerInfo != null || fkInfoArray !=null){
			beforeUpdateCopyRequired = true;
		}
		
	}
	/**
		@exception StandardException Standard Cloudscape error policy
	*/
	public void open() throws StandardException
	{

		setup();
		collectAffectedRows();

		/*
		** If this is a deferred update, read the new rows and RowLocations
		** from the temporary conglomerate and update the base table using
		** the RowChanger.
		*/
		if (deferred)
		{

			runChecker(true); //check for only RESTRICT referential action rule violations
			fireBeforeTriggers();
			updateDeferredRows();
			/* Apply deferred inserts to unique indexes */
			rowChanger.finish();
			runChecker(false); //check for all  violations
			fireAfterTriggers();

		}
		else{
		/* Apply deferred inserts to unique indexes */
		rowChanger.finish();
		}

		cleanUp();
    }


	/**
		@exception StandardException Standard Cloudscape error policy
	*/
	void setup() throws StandardException
	{
		boolean firstOpen = (rowChanger == null);

		rowCount = 0;
		
		/* Cache query plan text for source, before it gets blown away */
		if (lcc.getRunTimeStatisticsMode())
		{
			/* savedSource nulled after run time statistics generation */
			savedSource = source;
		}

		/* Get or re-use the row changer.
		 * NOTE: We need to set ourself as the top result set
		 * if this is not the 1st execution.  (Done in constructor
		 * for 1st execution.)
		 */
		if (firstOpen)
		{
			rowChanger = lcc.getLanguageConnectionFactory().getExecutionFactory()
				             .getRowChanger( heapConglom, 
										 constants.heapSCOCI, 
										 heapDCOCI,
										 constants.irgs,
										 constants.indexCIDS,
										 constants.indexSCOCIs,
										 indexDCOCIs,
										 constants.numColumns,
										 tc,
										 constants.changedColumnIds,
										 constants.getBaseRowReadList(),
										 constants.getBaseRowReadMap(),
										 constants.getStreamStorableHeapColIds(),
										 activation);
			rowChanger.setIndexNames(constants.indexNames);
		}
		else
		{
			lcc.getStatementContext().setTopResultSet(this, subqueryTrackingArray);
		}


		/* Open the RowChanger before the source ResultSet so that
		 * the store will see the RowChanger's lock as a covering lock
		 * if it is a table lock.
		 */
		rowChanger.open(lockMode);

		if (numOpens++ == 0)
		{
			source.openCore();
		}
		else
		{
			source.reopenCore();
		}

		/* The source does not know whether or not we are doing a
		 * deferred mode update.  If we are, then we must clear the
		 * index scan info from the activation so that the row changer
		 * does not re-use that information (which won't be valid for
		 * a deferred mode update).
		 */
		if (deferred)
		{
			activation.clearIndexScanInfo();
		}

		if (fkInfoArray != null)
		{
			if (riChecker == null)
			{
				riChecker = new RISetChecker(tc, fkInfoArray);
			}
			else
			{
				riChecker.reopen();
			}
		}

		if (deferred)
		{
			/* Allocate the temporary rows and get result description
			 * if this is the 1st time that we are executing.
			 */
			if (firstOpen)
			{
				deferredTempRow = RowUtil.getEmptyValueRow(numberOfBaseColumns+1, lcc);
				oldDeletedRow = RowUtil.getEmptyValueRow(numberOfBaseColumns, lcc);
				triggerResultDescription = (resultDescription != null) ?
									resultDescription.truncateColumns(numberOfBaseColumns+1) :
									null;
			}

			Properties properties = new Properties();

			// Get the properties on the heap
			rowChanger.getHeapConglomerateController().getInternalTablePropertySet(properties);
			if(beforeUpdateCopyRequired){
				deletedRowHolder = new TemporaryRowHolderImpl(tc, properties, triggerResultDescription);
			}
			insertedRowHolder = new TemporaryRowHolderImpl(tc, properties, triggerResultDescription);

			rowChanger.setRowHolder(insertedRowHolder);
		}

	}	

	/* Following 2 methods are for checking and make sure we don't have one un-objectified stream
	 * to be inserted into 2 temp table rows for deferred update.  Otherwise it would cause problem
	 * when writing to disk using the stream a second time.  In other cases we don't want to
	 * unnecessarily objectify the stream. beetle 4896.
	 */
	private FormatableBitSet checkStreamCols()
	{
		DataValueDescriptor[] cols = row.getRowArray();
		FormatableBitSet streamCols = null;
		for (int i = 0; i < numberOfBaseColumns; i++)
		{
			if (cols[i+numberOfBaseColumns] instanceof StreamStorable)  //check new values
			{
				if (streamCols == null) streamCols = new FormatableBitSet(numberOfBaseColumns);
				streamCols.set(i);
			}
		}
		return streamCols;
	}

	private void objectifyStream(ExecRow tempRow, FormatableBitSet streamCols) throws StandardException
	{
		DataValueDescriptor[] cols = tempRow.getRowArray();
		for (int i = 0; i < numberOfBaseColumns; i++)
		{
			if (cols[i] != null && streamCols.get(i))
				((StreamStorable)cols[i]).loadStream();
		}
	}

	public boolean collectAffectedRows() throws StandardException
	{

		boolean rowsFound = false;
		row = getNextRowCore(source);
		if (row!=null)
			rowsFound = true;
		else
		{
			activation.addWarning(
						StandardException.newWarning(
							SQLState.LANG_NO_ROW_FOUND));
		}

		//beetle 3865, update cursor use index.
		TableScanResultSet tableScan = (TableScanResultSet) activation.getForUpdateIndexScan();
		boolean notifyCursor = ((tableScan != null) && ! tableScan.sourceDrained);
		boolean checkStream = (deferred && rowsFound && ! constants.singleRowSource);
		FormatableBitSet streamCols = (checkStream ? checkStreamCols() : null);
		checkStream = (streamCols != null);

        while ( row != null )
        {

			/* By convention, the last column in the result set for an
			 * update contains a SQLRef containing the RowLocation of
			 * the row to be updated.
			 */

			/*
			** If we're doing deferred update, write the new row and row
			** location to the temporary conglomerate.  If we're not doing
			** deferred update, update the permanent conglomerates now
			** using the RowChanger.
			*/
			if (deferred)
			{
				/*
				** If we have a before trigger, we must evaluate the 
				** check constraint after we have executed the trigger.
				** Note that we have compiled checkGM accordingly (to
				** handle the different row shape if we are evaluating
				** against the input result set or a temporary row holder
				** result set).
				*/
				if (triggerInfo == null)
				{
					evaluateCheckConstraints( checkGM, activation );
				}

				/*
				** We are going to only save off the updated
				** columns and the RID.  For a trigger, all columns
				** were marked as needed so we'll copy them all.
				*/
				RowUtil.copyRefColumns(deferredTempRow,
											row,
											numberOfBaseColumns,
											numberOfBaseColumns + 1);
				if (checkStream)
					objectifyStream(deferredTempRow, streamCols);

				insertedRowHolder.insert(deferredTempRow); 

				/*
				** Grab a copy of the row to delete.  We are
				** going to use this for deferred RI checks.
				*/
				if(beforeUpdateCopyRequired)
				{
					RowUtil.copyRefColumns(oldDeletedRow,
										   row,
										   numberOfBaseColumns);

					deletedRowHolder.insert(oldDeletedRow);
				}

				/*
				** If we haven't already, lets get a template to
				** use as a template for our rescan of the base table.
				** Do this now while we have a real row to use
				** as a copy.
				**
				** There is one less column in the base row than
				** there is in source row, because the base row
				** doesn't contain the row location.
				*/
				if (deferredBaseRow == null)
				{
					deferredBaseRow = RowUtil.getEmptyValueRow(numberOfBaseColumns, lcc);
			
					RowUtil.copyCloneColumns(deferredBaseRow, row, 
											numberOfBaseColumns);

					/*
					** While we're here, let's also create a sparse row for
					** fetching from the store.
					*/
					deferredSparseRow = makeDeferredSparseRow(deferredBaseRow,
																baseRowReadList,
																lcc);
				}
			}
			else
			{
				evaluateCheckConstraints( checkGM, activation );

				/* Get the RowLocation to update 
			 	* NOTE - Column #s in the Row are 1 based.
			 	*/
				RowLocation baseRowLocation = (RowLocation)
					(row.getColumn(resultWidth)).getObject();

				RowUtil.copyRefColumns(newBaseRow,
										row,
										numberOfBaseColumns,
										numberOfBaseColumns);

				if (riChecker != null)
				{
					/*
					** Make sure all foreign keys in the new row
					** are maintained.  Note that we don't bother 
					** checking primary/unique keys that are referenced
					** here.  The reason is that if we are updating
					** a referenced key, we'll be updating in deferred
					** mode, so we wont get here.
					*/
					riChecker.doFKCheck(newBaseRow);
				}

				rowChanger.updateRow(row,newBaseRow,baseRowLocation);

				//beetle 3865, update cursor use index.
				if (notifyCursor)
					notifyForUpdateCursor(row.getRowArray(),newBaseRow.getRowArray(),baseRowLocation,
											tableScan);
			}

			rowCount++;

			// No need to do a next on a single row source
			if (constants.singleRowSource)
			{
				row = null;
			}
			else
			{
				row = getNextRowCore(source);
			}
		}

		return rowsFound;
	}

	/* beetle 3865, updateable cursor use index. If the row we are updating has new value that
	 * falls into the direction of the index scan of the cursor, we save this rid into a hash table
	 * (for fast search), so that when the cursor hits it again, it knows to skip it.  When we get
	 * to a point that the hash table is full, we scan forward the cursor until one of two things
	 * happen: (1) we hit a record whose rid is in the hash table (we went through it already, so
	 * skip it), we remove it from hash table, so that we can continue to use hash table. OR, (2) the scan
	 * forward hit the end.  If (2) happens, we can de-reference the hash table to make it available
	 * for garbage collection.  We save the future row id's in a virtual mem heap.  In any case,
	 * next read will use a row id that we saved.
	 */
	private void notifyForUpdateCursor(DataValueDescriptor[] row, DataValueDescriptor[] newBaseRow,
										RowLocation rl, TableScanResultSet tableScan)
		throws StandardException
	{
		int[] indexCols = tableScan.indexCols;
		int[] changedCols = constants.changedColumnIds;
		boolean placedForward = false, ascending, decided = false, overlap = false;
		int basePos, k;
		/* first of all, we see if there's overlap between changed column ids and index key
		 * columns.  If so, we see if the new update value falls into the future range of the
		 * index scan, if so, we need to save it in hash table.
		 */
		for (int i = 0; i < indexCols.length; i++)
		{
			basePos = indexCols[i];
			if (basePos > 0)
				ascending = true;
			else
			{
				ascending = false;
				basePos = -basePos;
			}
			for (int j = 0; j < changedCols.length; j++)
			{
				if (basePos == changedCols[j])
				{
					decided = true;		//we pretty much decided if new row falls in front
										//of the cursor or behind
					/* the row and newBaseRow we get are compact base row that only have
				 	 * referenced columns.  Our "basePos" is index in sparse heap row, so
					 * we need the BaseRowReadMap to map into the compact row.
					 */
					int[] map = constants.getBaseRowReadMap();
					if (map == null)
						k = basePos - 1;
					else
						k =  map[basePos - 1];

					DataValueDescriptor key;
					/* We need to compare with saved most-forward cursor scan key if we
					 * are reading records from the saved RowLocation temp table (instead
					 * of the old column value) because we only care if new update value
					 * jumps forward the most-forward scan key.
					 */
					if (tableScan.compareToLastKey)
						key = tableScan.lastCursorKey.getColumn(i + 1);
					else
						key = row[k];

					/* Starting from the first index key column forward, we see if the direction
					 * of the update change is consistent with the direction of index scan.
					 * If so, we save it in hash table.
					 */
					if ((ascending && key.greaterThan(newBaseRow[k], key).equals(true)) ||
						(!ascending && key.lessThan(newBaseRow[k], key).equals(true)))
						placedForward = true;
					else if (key.equals(newBaseRow[k], key).equals(true))
					{
						decided = false;
						overlap = true;
					}
					break;
				}
			}
			if (decided)  // already decided if new row falls in front or behind
				break;
		}
		/* If index row gets updated but key value didn't actually change, we still
		 * put it in hash table because it can either fall in front or behind.  This
		 * can happen if the update explicitly sets a value, but same as old.
		 */
		if (overlap && !decided)
			placedForward = true;

		if (placedForward)		// add it to hash table
		{
			/* determining initial capacity of hash table from a few factors:
			 * (1) user specified MAX_MEMORY_PER_TABLE property, (2) min value 100
			 * (3) optimizer estimated row count.  We want to avoid re-hashing if
			 * possible, for performance reason, yet don't waste space.  If initial
			 * capacity is greater than max size divided by load factor, no rehash
			 * is ever needed.
			 */
			int maxCapacity = lcc.getOptimizerFactory().getMaxMemoryPerTable() / 16;
			if (maxCapacity < 100)
				maxCapacity = 100;

			if (tableScan.past2FutureTbl == null)
			{
				double rowCount = tableScan.getEstimatedRowCount();
				int initCapacity = 32 * 1024;
				if (rowCount > 0.0)
				{
					rowCount = rowCount / 0.75 + 1.0;	// load factor
					if (rowCount < initCapacity)
						initCapacity = (int) rowCount;
				}
				if (maxCapacity < initCapacity)
					initCapacity = maxCapacity;

				tableScan.past2FutureTbl = new Hashtable(initCapacity);
			}

			Hashtable past2FutureTbl = tableScan.past2FutureTbl;
			/* If hash table is not full, we add it in.  The key of the hash entry
			 * is the string value of the RowLocation.  If the hash table is full,
			 * as the comments above this function say, we scan forward.
			 *
			 * Need to save a clone because when we get cached currentRow, "rl" shares the
			 * same reference, so is changed at the same time.
			 */
			RowLocation updatedRL = (RowLocation) rl.getClone();

			if (past2FutureTbl.size() < maxCapacity)
				past2FutureTbl.put(updatedRL, updatedRL);
			else
			{
				tableScan.skipFutureRowHolder = true;
				ExecRow rlRow = new ValueRow(1);

				for (;;)
				{
					ExecRow aRow = tableScan.getNextRowCore();
					if (aRow == null)
					{
						tableScan.sourceDrained = true;
						tableScan.past2FutureTbl = null;	// de-reference for garbage coll.
						break;
					}
					RowLocation rowLoc = (RowLocation) aRow.getColumn(aRow.nColumns());

					if (updatedRL.equals(rowLoc))  //this row we are updating jumped forward
					{
						saveLastCusorKey(tableScan, aRow);
						break;	// don't need to worry about adding this row to hash any more
					}

					if (tableScan.futureForUpdateRows == null)
					{
						// virtual memory heap. In-memory part size 100. With the co-operation
						// of hash table and in-memory part of heap (hash table shrinks while
						// in-memory heap grows), hopefully we never spill temp table to disk.

						tableScan.futureForUpdateRows = new TemporaryRowHolderImpl
							(tc, null, null, 100, false, true);
					}

					rlRow.setColumn(1, rowLoc);
					tableScan.futureForUpdateRows.insert(rlRow);
					if (past2FutureTbl.size() < maxCapacity) //we got space in the hash table now, stop!
					{
						past2FutureTbl.put(updatedRL, updatedRL);
						saveLastCusorKey(tableScan, aRow);
						break;
					}
				}
				tableScan.skipFutureRowHolder = false;
			}
		}
	}

	private void saveLastCusorKey(TableScanResultSet tableScan, ExecRow aRow) throws StandardException
	{
		/* We save the most-forward cursor scan key where we are stopping, so
		 * that next time when we decide if we need to put an updated row id into
		 * hash table, we can compare with this key.  This is an optimization on
		 * memory usage of the hash table, otherwise it may be "leaking".
		 */
		if (tableScan.lastCursorKey == null)
			tableScan.lastCursorKey = new ValueRow(aRow.nColumns() - 1);
		for (int i = 1; i <= tableScan.lastCursorKey.nColumns(); i++)
		{
			DataValueDescriptor aCol = aRow.getColumn(i);
			if (aCol != null)
				tableScan.lastCursorKey.setColumn(i, aCol.getClone());
		}
	}

	void fireBeforeTriggers() throws StandardException
	{
		if (deferred)
		{
			if (triggerInfo != null)
			{
				if (triggerActivator == null)
				{
				triggerActivator = new TriggerEventActivator(lcc, 
											tc, 
											constants.targetUUID,
											triggerInfo,
											TriggerExecutionContext.UPDATE_EVENT,
											activation, null);
				}
				else
				{
					triggerActivator.reopen();
				}

				// fire BEFORE trigger, do this before checking constraints
				triggerActivator.notifyEvent(TriggerEvents.BEFORE_UPDATE, 
												deletedRowHolder.getResultSet(),
												insertedRowHolder.getResultSet());

			}
		}
	}

    void fireAfterTriggers() throws StandardException
	{
		if (deferred)
		{
			if (triggerActivator != null)
			{
				triggerActivator.notifyEvent(TriggerEvents.AFTER_UPDATE, 
										deletedRowHolder.getResultSet(),
										insertedRowHolder.getResultSet());
			}
		}
	}



	void updateDeferredRows() throws StandardException
	{
		if (deferred)
		{
			// we already have everything locked 
			deferredBaseCC = 
                tc.openCompiledConglomerate(
                    false,
                    tc.OPENMODE_FORUPDATE|tc.OPENMODE_SECONDARY_LOCKED,
                    lockMode,
                    TransactionController.ISOLATION_SERIALIZABLE,
                    constants.heapSCOCI,
                    heapDCOCI);
			
			CursorResultSet rs = insertedRowHolder.getResultSet();
			try
			{
				/*
				** We need to do a fetch doing a partial row
				** read.  We need to shift our 1-based bit
				** set to a zero based bit set like the store
				** expects.
				*/
				FormatableBitSet readBitSet = RowUtil.shift(baseRowReadList, 1);
				ExecRow deferredTempRow2;

				rs.open();
				while ((deferredTempRow2 = rs.getNextRow()) != null)
				{
					/*
					** Check the constraint now if we have triggers.
					** Otherwise we evaluated them as we read the
					** rows in from the source.
					*/
					if (triggerInfo != null)
					{
						source.setCurrentRow(deferredTempRow);
						evaluateCheckConstraints(checkGM, activation);
					}

					/* 
					** The last column is a Ref, which contains a 
					** RowLocation.
					*/
					DataValueDescriptor rlColumn = deferredTempRow2.getColumn(numberOfBaseColumns + 1);
					RowLocation baseRowLocation = 
							(RowLocation) (rlColumn).getObject();
	
					/* Get the base row at the given RowLocation */
					boolean row_exists = 
						deferredBaseCC.fetch(
							baseRowLocation, deferredSparseRow.getRowArray(), 
							readBitSet);

					if (SanityManager.DEBUG)
					{
						SanityManager.ASSERT(row_exists, "did not find base row in deferred update");
					}
	
					/*
					** Copy the columns from the temp row to the base row.
					** The base row has fewer columns than the temp row,
					** because it doesn't contain the row location.
					*/
					RowUtil.copyRefColumns(newBaseRow,
											deferredTempRow2,
											numberOfBaseColumns);

					rowChanger.updateRow(deferredBaseRow,
										newBaseRow,
										baseRowLocation);
				}
			} finally
			{
				source.clearCurrentRow();
				rs.close();
			}
		}
	}


	
	void runChecker(boolean restrictCheckOnly) throws StandardException
	{

		/*
		** For a deferred update, make sure that there
		** aren't any primary keys that were removed which
		** are referenced.  
		*/
		if (deferred && updatingReferencedKey)
		{
			ExecRow	deletedRow;
			CursorResultSet deletedRows; 

			/*
			** For each referenced key that was modified
			*/
			for (int i = 0; i < fkInfoArray.length; i++)
			{
				if (fkInfoArray[i].type == FKInfo.FOREIGN_KEY)
				{
					continue;
				}

				deletedRows = deletedRowHolder.getResultSet();
				try
				{
					/*
					** For each delete row
					*/	
					deletedRows.open();
					while ((deletedRow = deletedRows.getNextRow()) != null)
					{
						if (!foundRow(deletedRow, 
										fkInfoArray[i].colArray, 
										insertedRowHolder))
						{
							riChecker.doRICheck(i, deletedRow, restrictCheckOnly);
						}
					}	
				}
				finally
				{
					deletedRows.close();
				}
			}
		}

		/*
		** For a deferred update, make sure that there
		** aren't any foreign keys that were added that
 		** aren't referenced.  
		*/
		if (deferred && updatingForeignKey)
		{
			ExecRow	insertedRow;
			CursorResultSet insertedRows; 

			/*
			** For each foreign key that was modified
			*/
			for (int i = 0; i < fkInfoArray.length; i++)
			{
				if (fkInfoArray[i].type == FKInfo.REFERENCED_KEY)
				{
					continue;
				}

				insertedRows = insertedRowHolder.getResultSet();
				try
				{
					/*
					** For each inserted row
					*/	
					insertedRows.open();
					while ((insertedRow = insertedRows.getNextRow()) != null)
					{
						if (!foundRow(insertedRow, 
										fkInfoArray[i].colArray, 
										deletedRowHolder))
						{
							riChecker.doRICheck(i, insertedRow, restrictCheckOnly);
						}
					}	
				}
				finally
				{
					insertedRows.close();
				}
			}
		}

	}

	public static boolean foundRow
	(
		ExecRow					checkRow, 
		int[]					colsToCheck,
		TemporaryRowHolderImpl	rowHolder
	)
		throws StandardException
	{
		ExecRow				scanRow;
		boolean				foundMatch = false;
		Object[] 			checkRowArray = checkRow.getRowArray();
		DataValueDescriptor	checkCol;
		DataValueDescriptor	scanCol;

		CursorResultSet rs = rowHolder.getResultSet();
		try
		{	
			/*
			** For each inserted row
			*/	
			rs.open();
			while ((scanRow = rs.getNextRow()) != null)
			{
				Object[] scanRowArray = scanRow.getRowArray();
				int i;
				for (i = 0; i < colsToCheck.length; i++)
				{
					checkCol = (DataValueDescriptor)checkRowArray[colsToCheck[i]-1];
					scanCol = (DataValueDescriptor)scanRowArray[colsToCheck[i]-1];

					BooleanDataValue result = checkCol.equals(
											scanCol,
											checkCol); // result
					if (!result.getBoolean())
					{
						break;
					}
				}
				if (i == colsToCheck.length)
				{
					foundMatch = true;
					break;
				}	
			}
		}
		finally
		{
			rs.close();
		}
		return foundMatch;
	}


	/**
	 * @see ResultSet#cleanUp
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	cleanUp() throws StandardException
	{ 
		numOpens = 0;

		/* Close down the source ResultSet tree */
		if (source != null)
		{
			source.close();
			// cache source across open()s
		}

		if (triggerActivator != null)
		{
			triggerActivator.cleanup();
			// cache triggerActivator across open()s
		}

		if (rowChanger != null)
			rowChanger.close();

		if (deferredBaseCC != null)
			deferredBaseCC.close();
		deferredBaseCC = null;

		if (insertedRowHolder != null)
		{
			insertedRowHolder.close();
		}
	
		if (deletedRowHolder != null)
		{
			deletedRowHolder.close();
		}

		if (riChecker != null)
		{
			riChecker.close();
			// cache riChecker across open()s
		}

		super.close();

		endTime = getCurrentTimeMillis();
	}

    /**
     * Decode the update lock mode.
     * <p>
     * The value for update lock mode is in the 2nd 2 bytes for 
     * ExecutionContext.SERIALIZABLE_ISOLATION_LEVEL isolation level.  Otherwise
     * (REPEATABLE READ, READ COMMITTED, and READ UNCOMMITTED) the lock mode is
     * located in the first 2 bytes.
     * <p>
     * This is done to override the optimizer choice to provide maximum 
     * concurrency of record level locking except in SERIALIZABLE where table
     * level locking is required in heap scans for correctness.
     * <p>
     * See Compilation!QueryTree!FromBaseTable for encoding of the lockmode.
     * <p>
     *
	 * @return The lock mode (record or table) to use to open the result set.
     *
     * @param lcc       The context to look for current isolation level.
     * @param lockMode  The compiled encoded lock mode for this query.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	protected static int decodeLockMode(
    LanguageConnectionContext   lcc, 
    int                         lockMode)
	{
		if ((lockMode >>> 16) != 0)
		{
            // Note that isolation level encoding from 
            // getCurrentIsolationLevel() returns 
            // ExecutionContext.*ISOLATION_LEVEL constants, not 
            // TransactionController.ISOLATION* constants.

			int isolationLevel = lcc.getCurrentIsolationLevel();

            if (isolationLevel != ExecutionContext.SERIALIZABLE_ISOLATION_LEVEL)
            {
				lockMode = lockMode & 0xff;
            }
            else
            {
				lockMode = lockMode >>> 16;
            }
		}
		return lockMode;
	}

	
	void rowChangerFinish() throws StandardException
	{
		rowChanger.finish();
	}

}
