/*

   Derby - Class org.apache.derby.impl.sql.execute.UpdateResultSet

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.sql.execute;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.db.TriggerExecutionContext;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.StreamStorable;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.sql.execute.RowChanger;
import org.apache.derby.iapi.store.access.BackingStoreHashtable;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.BooleanDataValue;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.SQLBoolean;
import org.apache.derby.iapi.types.SQLRef;
import org.apache.derby.impl.sql.execute.DeferredConstraintsMemory.CheckInfo;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptorList;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
/**
 * Update the rows from the specified
 * base table. This will cause constraints to be checked
 * and triggers to be executed based on the c's and t's
 * compiled into the update plan.
 *
 */
class UpdateResultSet extends DMLWriteGeneratedColumnsResultSet
{
	private TransactionController 	tc;
	private ExecRow					newBaseRow;
	private ExecRow 					row;
	private ExecRow 					deferredSparseRow;
	UpdateConstantAction		constants;
	
	NoPutResultSet			savedSource;
	private RowChanger				rowChanger;

	protected ConglomerateController	deferredBaseCC;

	protected long[]				deferredUniqueCIDs;
	protected boolean[]				deferredUniqueCreated;
	protected ConglomerateController	deferredUniqueCC[];
	protected ScanController[]		deferredUniqueScans;

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
    private GeneratedMethod         generationClauses;
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

    private List<UUID>              violatingCheckConstraints;
    private BackingStoreHashtable   deferredChecks; // cached ref.
    /*
     * class interface
     *
     */
    /**
	 * @param source update rows come from source
	 * @param generationClauses	Generated method for computed generation clauses
	 * @param checkGM	Generated method for enforcing check constraints
     * @param activation The activation
	 * @exception StandardException thrown on error
     */
    UpdateResultSet(NoPutResultSet source,
						   GeneratedMethod generationClauses,
						   GeneratedMethod checkGM,
						   Activation activation)
      throws StandardException
    {
		this(source, generationClauses, checkGM , activation, activation.getConstantAction(),null);
	}

    /*
     * class interface
     *
     */
    /**
	 * @param source update rows come from source
	 * @param generationClauses	Generated method for computed generation clauses
	 * @param checkGM	Generated method for enforcing check constraints
	 * @param activation Activation
     * @param constantActionItem  id of the update constant action saved object
	 * @param rsdItem  id of the Result Description saved object
	 * @exception StandardException thrown on error
     */
    UpdateResultSet(NoPutResultSet source,
						   GeneratedMethod generationClauses,
						   GeneratedMethod checkGM,
						   Activation activation, 
						   int constantActionItem,
						   int rsdItem)
      throws StandardException
    {
		this(source, generationClauses, checkGM , activation,
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
	 * @param generationClauses	Generated method for computed generation clauses
	 * @param checkGM	Generated method for enforcing check constraints
     * @param activation
     * @param passedInConstantAction
     * @param passedInRsd
	 * @exception StandardException thrown on error
     */
    UpdateResultSet(NoPutResultSet source,
						   GeneratedMethod generationClauses,
						   GeneratedMethod checkGM,
						   Activation activation,
						   ConstantAction passedInConstantAction,
						   ResultDescription passedInRsd)
      throws StandardException
    {
		super(activation, passedInConstantAction);

		// Get the current transaction controller
        tc = activation.getTransactionController();
        this.sourceResultSet = source;
        this.generationClauses = generationClauses;
		this.checkGM = checkGM;

		constants = (UpdateConstantAction) constantAction;
		fkInfoArray = constants.getFKInfo();
		triggerInfo = constants.getTriggerInfo();

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
            for (FKInfo fkInfo : fkInfoArray) {
                if (fkInfo.type == FKInfo.REFERENCED_KEY) {
                    updatingReferencedKey = true;
                    if (SanityManager.DEBUG)
                    {
                        SanityManager.ASSERT(
                            constants.deferred,
                            "updating referenced key but update not " +
                                "deferred, wuzzup?");
                    }
                } else {
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

		deferred = constants.deferred;
		
		//update can be marked for deferred mode because the scan is being done
		//using index. But it is not necesary  to keep the before copy
		//of the row in the temporary row holder (deletedRowHolder) unless
		//there are RI constraint or Triggers.(beetle:5301)
		if(triggerInfo != null || fkInfoArray !=null){
			beforeUpdateCopyRequired = true;
		}
		
        identitySequenceUUIDString = constants.identitySequenceUUIDString;
        initializeAIcache(constants.getAutoincRowLocation());
	}
	/**
		@exception StandardException Standard Derby error policy
	*/
    @Override
	public void open() throws StandardException
	{

		setup();
		autoincrementGenerated = false;
		collectAffectedRows();

		/*
		** If this is a deferred update, read the new rows and RowLocations
		** from the temporary conglomerate and update the base table using
		** the RowChanger.
		*/
		if (deferred)
		{

            runChecker(true); // check for only RESTRICT referential
                                    // action rule violations
			fireBeforeTriggers();
            updateDeferredRows();
			/* Apply deferred inserts to unique indexes */
			rowChanger.finish();
            runChecker(false); // check for all  violations
			fireAfterTriggers();

		}
		else{
		/* Apply deferred inserts to unique indexes */
		rowChanger.finish();
		}

		saveAIcacheInformation(constants.getSchemaName(), 
			constants.getTableName(), constants.getColumnNames());
		cleanUp();
    }


	/**
		@exception StandardException Standard Derby error policy
	*/
    @Override
	void setup() throws StandardException
	{
		super.setup();

		/* decode lock mode */
		lockMode = decodeLockMode(constants.lockMode);

		boolean firstOpen = (rowChanger == null);

		rowCount = 0L;
		
		/* Cache query plan text for source, before it gets blown away */
		if (lcc.getRunTimeStatisticsMode())
		{
			/* savedSource nulled after run time statistics generation */
			savedSource = sourceResultSet;
		}

		/* Get or re-use the row changer.
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

		verifyAutoGeneratedRScolumnsList(constants.targetUUID);

		/* Open the RowChanger before the source ResultSet so that
		 * the store will see the RowChanger's lock as a covering lock
		 * if it is a table lock.
		 */
		rowChanger.open(lockMode);

		if (numOpens++ == 0)
		{
			sourceResultSet.openCore();
		}
		else
		{
			sourceResultSet.reopenCore();
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
                riChecker = new RISetChecker(lcc, tc, fkInfoArray);
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
				deletedRowHolder =
					new TemporaryRowHolderImpl(activation, properties,
											   triggerResultDescription);
			}
			insertedRowHolder =
				new TemporaryRowHolderImpl(activation, properties,
										   triggerResultDescription);

			rowChanger.setRowHolder(insertedRowHolder);
		}

		firstExecuteSpecialHandlingAutoGen(firstOpen, rowChanger, constants.targetUUID);
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


    /**
     * Run check constraints against the current row. Raise an error if
     * a check constraint is violated, unless all the offending checks are
     * deferred, in which case a false value will be returned. A NULL value
     * will be interpreted as success (not violation).
     *
     * @exception StandardException thrown on error
     */
    private boolean evaluateCheckConstraints() throws StandardException     {
        boolean result = true;

        if (checkGM != null) {
            // Evaluate the check constraints. If all check constraint modes are
            // immediate, a check error will throw rather than return a false
            // value.
            SQLBoolean allOk =
                    (SQLBoolean)checkGM.invoke(activation);
            result = allOk.isNull() || allOk.getBoolean();
        }

        return result;
    }


	public boolean collectAffectedRows() throws StandardException
	{

		boolean rowsFound = false;
		row = getNextRowCore(sourceResultSet);

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
		boolean notifyCursor = (tableScan != null);
		boolean checkStream = (deferred && rowsFound && ! constants.singleRowSource);
		FormatableBitSet streamCols = (checkStream ? checkStreamCols() : null);
		checkStream = (streamCols != null);

        while ( row != null )
        {
            evaluateGenerationClauses( generationClauses, activation, sourceResultSet, row, true );

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
                    boolean allOk = evaluateCheckConstraints();
                    if (!allOk) {
                        DataValueDescriptor[] rw = row.getRowArray();
                        SQLRef r = (SQLRef)rw[rw.length - 1];
                        RowLocation baseRowLocation =
                            (RowLocation)r.getObject();

                        deferredChecks =
                            DeferredConstraintsMemory.rememberCheckViolations(
                                lcc,
                                constants.targetUUID,
                                constants.getSchemaName(),
                                constants.getTableName(),
                                deferredChecks,
                                violatingCheckConstraints,
                                baseRowLocation,
                                new CheckInfo[1]);
                    }
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
                boolean allOk = evaluateCheckConstraints();

                /* Get the RowLocation to update
			 	* NOTE - Column #s in the Row are 1 based.
			 	*/
				RowLocation baseRowLocation = (RowLocation)
					(row.getColumn(resultWidth)).getObject();

                if (!allOk) {
                    deferredChecks =
                        DeferredConstraintsMemory.rememberCheckViolations(
                            lcc,
                            constants.targetUUID,
                            constants.getSchemaName(),
                            constants.getTableName(),
                            deferredChecks,
                            violatingCheckConstraints,
                            baseRowLocation,
                            new CheckInfo[1]);
                }

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
                    riChecker.doFKCheck(activation, newBaseRow);
				}

				sourceResultSet.updateRow(newBaseRow, rowChanger);
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
				row = getNextRowCore(sourceResultSet);
			}
		}
        
        if(rowCount==1 && constants.hasAutoincrement()) 
			lcc.setIdentityValue(identityVal);

		return rowsFound;
	}

    @Override
	protected ExecRow getNextRowCore( NoPutResultSet source )
		throws StandardException
	{
        ExecRow nextRow = super.getNextRowCore( source );

        if ( (nextRow != null) && constants.underMerge() ) {
            nextRow = processMergeRow( source, nextRow );
        }

        return nextRow;
	}

    /**
     * <p>
     * Special handling if this is an UPDATE action of a MERGE statement.
     * </p>
     */
	private ExecRow processMergeRow( NoPutResultSet sourceRS, ExecRow row )
		throws StandardException
	{
        //
        // After we fix derby-6414, we will need to handle the DEFAULT keyword
        // for identity columns, just as we do in InsertResultSet.processMergeRow().
        // For the moment, we just allow the bad behavior described by derby-6414.
        //
        return normalizeRow( sourceRS, row );
	}

	/* beetle 3865, updateable cursor use index. If the row we are updating has new value that
	 * falls into the direction of the index scan of the cursor, we save this rid into a hash table
	 * (for fast search), so that when the cursor hits it again, it knows to skip it.
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

					DataValueDescriptor key = row[k];

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
                double rowCnt = tableScan.getEstimatedRowCount();
				int initCapacity = 32 * 1024;
                if (rowCnt > 0.0)
				{
                    rowCnt = rowCnt / 0.75 + 1.0;   // load factor
                    if (rowCnt < initCapacity)
                        initCapacity = (int) rowCnt;
				}
				if (maxCapacity < initCapacity)
					initCapacity = maxCapacity;

                tableScan.past2FutureTbl = new BackingStoreHashtable(
                        tc, null, new int[]{0}, false, -1,
                        maxCapacity, initCapacity, -1, false,
                        tableScan.getActivation().getResultSetHoldability());
			}

            /* Add the row location to the hash table.
             *
             * Need to save a clone because when we get cached currentRow, "rl"
             * shares the same reference, so is changed at the same time.
             */
            tableScan.past2FutureTbl.putRow
                (
                 false,
                 new DataValueDescriptor[] { rl.cloneValue(false) },
                 null
                 );
		}
	}

	void fireBeforeTriggers() throws StandardException
	{
		if (deferred)
		{
			if (triggerInfo != null)
			{
				Vector<AutoincrementCounter> v = null;
				if (aiCache != null)
				{
					v = new Vector<AutoincrementCounter>();
					for (int i = 0; i < aiCache.length; i++)
					{
						String s, t, c;
						if (aiCache[i] == null)
							continue;
					
						Long initialValue = 
							lcc.lastAutoincrementValue(
								(s = constants.getSchemaName()),
								(t = constants.getTableName()),
								(c = constants.getColumnName(i)));

						AutoincrementCounter aic = 
							new AutoincrementCounter(
								 initialValue,
								 constants.getAutoincIncrement(i),
								 aiCache[i].getLong(),
								 s, t, c, i + 1);
						v.addElement(aic);
					}
				}
				if (triggerActivator == null)
				{
				triggerActivator = new TriggerEventActivator(lcc, 
											constants.targetUUID,
											triggerInfo,
											TriggerExecutionContext.UPDATE_EVENT,
											activation, v);
				}
				else
				{
					triggerActivator.reopen();
				}

				// fire BEFORE trigger, do this before checking constraints
				triggerActivator.notifyEvent(TriggerEvents.BEFORE_UPDATE, 
												deletedRowHolder.getResultSet(),
												insertedRowHolder.getResultSet(),
												constants.getBaseRowReadMap());

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
										insertedRowHolder.getResultSet(),
										constants.getBaseRowReadMap());
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
                    (TransactionController.OPENMODE_SECONDARY_LOCKED |
                     TransactionController.OPENMODE_FORUPDATE),
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
                    boolean allOk = true;

					if (triggerInfo != null)
					{
						sourceResultSet.setCurrentRow(deferredTempRow);
                        allOk = evaluateCheckConstraints();
					}

					/* 
					** The last column is a Ref, which contains a 
					** RowLocation.
					*/
					DataValueDescriptor rlColumn = deferredTempRow2.getColumn(numberOfBaseColumns + 1);
					RowLocation baseRowLocation = 
							(RowLocation) (rlColumn).getObject();

                    if (!allOk) {
                        deferredChecks =
                            DeferredConstraintsMemory.rememberCheckViolations(
                                lcc,
                                constants.targetUUID,
                                constants.getSchemaName(),
                                constants.getTableName(),
                                deferredChecks,
                                violatingCheckConstraints,
                                baseRowLocation,
                                new CheckInfo[1]);
                    }

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
				sourceResultSet.clearCurrentRow();
				rs.close();
			}
		}
	}


	
    void runChecker(boolean restrictCheckOnly)
            throws StandardException
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
                            // Argument "1" below: If a PK referenced by an FK
                            // is deferred, require at least one to be present
                            // in the primary table since we have modified the
                            // row's PK, unless postCheck == true, in which the
                            // call to postChecks does the actual checking, and
                            // we need at least one row intact to fulfill the
                            // constraint.
                           riChecker.doRICheck(
                                    activation,
                                    i,
                                    deletedRow,
                                    restrictCheckOnly,
                                    1);
						}
					}	

                    if (restrictCheckOnly) {
                        riChecker.postCheck(i);
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
                            riChecker.doRICheck(
                                activation,
                                i,
                                insertedRow,
                                restrictCheckOnly,
                                0);        // N/A, not referenced key
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
    @Override
	public void	cleanUp() throws StandardException
	{ 
		numOpens = 0;

		/* Close down the source ResultSet tree */
		if (sourceResultSet != null)
		{
			sourceResultSet.close();
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

		close();

		endTime = getCurrentTimeMillis();
	}

    @Override
    public void close() throws StandardException
    {
        super.close( constants.underMerge() );
    }
                               
	void rowChangerFinish() throws StandardException
	{
		rowChanger.finish();
	}

    @Override
    public void rememberConstraint(UUID cid) throws StandardException {
        if (violatingCheckConstraints == null) {
            violatingCheckConstraints = new ArrayList<UUID>();
        }

        violatingCheckConstraints.add(cid);
    }

    /**
     * getSetAutoincrementValue will get the autoincrement value of the 
     * columnPosition specified for the target table. If increment is 
     * non-zero we will also update the autoincrement value. 
     *
     * @param columnPosition	position of the column in the table (1-based)
     * @param increment			amount of increment. 
     *
     * @exception StandardException if anything goes wrong.
     */
    public NumberDataValue
    	getSetAutoincrementValue(int columnPosition, long increment)
    	throws StandardException
    {
        autoincrementGenerated = true;
        int index = columnPosition - 1;	// all our indices are 0 based.
        NumberDataValue newValue;
        newValue = activation.getCurrentValueAndAdvance
                ( identitySequenceUUIDString, aiCache[ index ].getTypeFormatId() );
        aiCache[index] = newValue;
        //Save the generated auto increment value for use by JDBC api and
        // IDENTITY_LOCAL_VAL function
		identityVal = newValue.getLong();
        return (NumberDataValue) aiCache[index];
    }
	
    /*
     * The implementation of this method is slightly different than the one
     *  in InsertResultSet. This code was originally written for insert but
     *  with DERBY-6414, we have started supporting update of auto generated
     *  column with keyword DEFAULT. The reason of different implementation is
     *  that the array used in InsertResultSet's implementation of this method,  
     *  ColumnDescriptors in resultDescription hold different entries for
     *  insert and update case. For insert case, the array holds the column
     *  descriptors of all the columns in the table. This is because all the
     *  columns in the table are going to get some value into them whether
     *  or not they were included directly in the actual INSERT statement.
     *  The 2nd array, rla has a spot for each of the columns in the table, 
     *  with non null value for auto generated column. But in case of Update,
     *  resultDescription does not include all the columns in the table. It
     *  only has the columns being touched by the Update statement(the rest of
     *  the columns in the table will retain their original values), and for 
     *  each of those touched columns, it has a duplicate entry in 
     *  resultDescription in order to have before and after values for the 
     *  changed column values. Lastly, it has a row location information for 
     *  the row being updated. This difference in array content of 
     *  resultDescription requires us to have separate implementation of this
     *  method for insert and update.
     */
	protected void  initializeAIcache(RowLocation[] rla) 
			throws StandardException{
        if (rla != null)
        {
        	aiCache = new DataValueDescriptor[ rla.length ];
        	ColumnDescriptorList columns = lcc.getDataDictionary().getTableDescriptor(constants.targetUUID).getColumnDescriptorList();
       		for (int i = 0; i < columns.size(); i++)
        	{
        		if (rla[i] == null)
        			continue;        		
        		aiCache[i] = columns.elementAt(i).getType().getNull();
    		}
        }
	}
}
