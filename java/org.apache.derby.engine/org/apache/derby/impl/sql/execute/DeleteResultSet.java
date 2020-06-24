/*

   Derby - Class org.apache.derby.impl.sql.execute.DeleteResultSet

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

import java.util.Properties;
import org.apache.derby.iapi.db.TriggerExecutionContext;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.sql.execute.RowChanger;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * Delete the rows from the specified
 * base table. This will cause constraints to be checked
 * and triggers to be executed based on the c's and t's
 * compiled into the insert plan.
 */
//IC see: https://issues.apache.org/jira/browse/DERBY-467
class DeleteResultSet extends DMLWriteResultSet
{
	private TransactionController   	tc;
	DeleteConstantAction		constants;
	protected  NoPutResultSet			source;
	NoPutResultSet			savedSource;
	int 							numIndexes;
	protected RowChanger 			rc;
	private ExecRow					row;

	protected ConglomerateController	deferredBaseCC;

	protected TemporaryRowHolderImpl	rowHolder;

	private int						numOpens; // number of opens w/o a close
	private boolean 				firstExecute;

	// cached across opens()s
	private FormatableBitSet 				baseRowReadList; 
	private int						rlColumnNumber;
	protected FKInfo[]				fkInfoArray;
	private TriggerInfo 			triggerInfo;
	private	RISetChecker			fkChecker;
	private TriggerEventActivator	triggerActivator;
	private boolean					noTriggersOrFks;

	ExecRow		deferredSparseRow; 
	ExecRow		deferredBaseRow;
	int lockMode; 
	protected  boolean cascadeDelete;
	ExecRow		deferredRLRow = null;
	int	numberOfBaseColumns = 0;

    /*
     * class interface
     *
     */
    DeleteResultSet
	(
		NoPutResultSet		source,
		Activation			activation
	)
		throws StandardException
    {
		this(source, activation.getConstantAction(), activation);
	}
    /**
     * REMIND: At present this takes just the conglomerate id
     * of the table. We can expect this to expand to include
     * passing information about triggers, constraints, and
     * any additional conglomerates on the underlying table
     * for access methods.
     *
	 * @exception StandardException		Thrown on error
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-467
//IC see: https://issues.apache.org/jira/browse/DERBY-467
    DeleteResultSet
	(
		NoPutResultSet		source,
		ConstantAction		passedInConstantAction,
		Activation			activation
	)
		throws StandardException
    {
		super(activation, passedInConstantAction);
		this.source = source;

		tc = activation.getTransactionController();
		constants = (DeleteConstantAction) constantAction;
//IC see: https://issues.apache.org/jira/browse/DERBY-2661
		fkInfoArray = constants.getFKInfo();
		triggerInfo = constants.getTriggerInfo();
		noTriggersOrFks = ((fkInfoArray == null) && (triggerInfo == null));
		baseRowReadList = constants.getBaseRowReadList();
		if(source != null)
//IC see: https://issues.apache.org/jira/browse/DERBY-4610
//IC see: https://issues.apache.org/jira/browse/DERBY-3049
			resultDescription = source.getResultDescription();
		else
			resultDescription = constants.resultDescription;

	}

	/**
		@exception StandardException Standard Derby error policy
	*/
    @Override
	public void open() throws StandardException
	{

		setup();
		boolean rowsFound = collectAffectedRows(); //this call also deletes rows , if not deferred
		if (! rowsFound)
		{
			activation.addWarning(
						StandardException.newWarning(
							SQLState.LANG_NO_ROW_FOUND));
		}

		/*
		** If the delete is deferred, scan the temporary conglomerate to
		** get the RowLocations of the rows to be deleted.  Re-fetch the
		** rows and delete them using the RowChanger.
		*/
		if (constants.deferred)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-6576
            runFkChecker(true); // check for only RESTRICT referential
                                      // action rule violations
			fireBeforeTriggers();
            deleteDeferredRows();
            runFkChecker(false); //check for all constraint violations
			// apply 
			rc.finish();
			fireAfterTriggers();
		}

	
		/* Cache query plan text for source, before it gets blown away */
		if (lcc.getRunTimeStatisticsMode())
		{
			/* savedSource nulled after run time statistics generation */
			savedSource = source;
		}

		cleanUp();
		endTime = getCurrentTimeMillis();

    }
	

	//this routine open the source and find the dependent rows 
    @Override
	void  setup() throws StandardException
	{
		super.setup();
//IC see: https://issues.apache.org/jira/browse/DERBY-2597

		// Remember if this is the 1st execution
		firstExecute = (rc == null);

		try {

			//open the source for the parent tables
			if (numOpens++ == 0)
			{
				source.openCore();
			}
			else
			{
        		source.reopenCore();
			}
		} catch (StandardException se) {
			activation.checkStatementValidity();
			throw se;

		}

		activation.checkStatementValidity();

		/* Get or re-use the row changer.
		 */
		if (firstExecute)
		{
			rc = lcc.getLanguageConnectionFactory().getExecutionFactory().
					     getRowChanger( 
								constants.conglomId,
								constants.heapSCOCI, 
								heapDCOCI,
								constants.irgs,
								constants.indexCIDS,
								constants.indexSCOCIs,
							    indexDCOCIs,
								constants.numColumns,
								tc,
								(int[])null,
								baseRowReadList,
								constants.getBaseRowReadMap(),
								constants.getStreamStorableHeapColIds(),
								activation);
		}

		/* decode the lock mode for the execution isolation level */
		lockMode = decodeLockMode(constants.lockMode);
//IC see: https://issues.apache.org/jira/browse/DERBY-2597

		/* Open the RowChanger before the source ResultSet so that
		 * the store will see the RowChanger's lock as a covering lock
		 * if it is a table lock.
		 */
		rc.open(lockMode); 

		/* The source does not know whether or not we are doing a
		 * deferred mode delete.  If we are, then we must clear the
		 * index scan info from the activation so that the row changer
		 * does not re-use that information (which won't be valid for
		 * a deferred mode delete).
		 */
		if (constants.deferred || cascadeDelete)
		{
			activation.clearIndexScanInfo();
		}

//IC see: https://issues.apache.org/jira/browse/DERBY-6000
        rowCount = 0L;
        if(!cascadeDelete)
			row = getNextRowCore(source);

		/*
		** We need the number of columns even if there are
		** no rows. Note that source.ressultDescription() may 
		** be null on a rep target doing a refresh.
		*/
		if (resultDescription == null)
		{
			if (SanityManager.DEBUG)
			{
				/*
				** We NEED a result description when we are going to
				** to have to kick off a trigger.  In a replicated environment
				** we don't get a result description when we are replaying
				** source xacts on the target, but we shouldn't be firing
				** a trigger in that case anyway.
				*/
				SanityManager.ASSERT(triggerInfo == null, "result description is needed to supply to trigger result sets");
			}
			numberOfBaseColumns = (row == null) ? 0 : row.nColumns();
		}
		else
		{
			numberOfBaseColumns = resultDescription.getColumnCount();
		}

		numIndexes = constants.irgs.length;

		if (constants.deferred || cascadeDelete)
		{
			Properties properties = new Properties();

			// Get the properties on the old heap
			rc.getHeapConglomerateController().getInternalTablePropertySet(properties);

			/*
			** If deferred and fk or trigger, we are going to grab
			** the entire row.  
			**
			** If we are deferred w/o a fk, then we only
			** save the row location.
			*/
			deferredRLRow = RowUtil.getEmptyValueRow(1, lcc);
			rlColumnNumber = noTriggersOrFks ? 1: numberOfBaseColumns;
			if(cascadeDelete)
			{
//IC see: https://issues.apache.org/jira/browse/DERBY-1112
				rowHolder = new TemporaryRowHolderImpl(activation, properties, 
//IC see: https://issues.apache.org/jira/browse/DERBY-4610
//IC see: https://issues.apache.org/jira/browse/DERBY-3049
						(resultDescription != null) ?
							resultDescription.truncateColumns(rlColumnNumber) :
							null, false);


			}else
			{

//IC see: https://issues.apache.org/jira/browse/DERBY-1112
				rowHolder = new TemporaryRowHolderImpl(activation, properties, 
						(resultDescription != null) ?
							resultDescription.truncateColumns(rlColumnNumber) :
							null);

			}

			rc.setRowHolder(rowHolder);
		}

		if (fkInfoArray != null)
		{
			if (fkChecker == null)
			{
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                fkChecker = new RISetChecker(lcc, tc, fkInfoArray);
			}
			else
			{
				fkChecker.reopen();
			}
		}
	}


	boolean  collectAffectedRows() throws StandardException
	{	

		DataValueDescriptor		rlColumn;
		RowLocation	baseRowLocation;
		boolean rowsFound = false;

		if(cascadeDelete)
			row = getNextRowCore(source);

		while ( row != null )
		{
			/* By convention, the last column for a delete contains a data value
			 * wrapping the RowLocation of the row to be deleted.  If we're
			 * doing a deferred delete, store the RowLocations in the
			 * temporary conglomerate.  If we're not doing a deferred delete,
			 * just delete the rows immediately.
			 */

			rowsFound = true;

			rlColumn = row.getColumn( row.nColumns() );
		
			if (constants.deferred || cascadeDelete)
			{

				/*
				** If we are deferred because of a trigger or foreign
				** key, we need to save off the entire row.  Otherwise,
				** we just save the RID.
				*/
				if (noTriggersOrFks)
				{
					deferredRLRow.setColumn(1, rlColumn);
					rowHolder.insert(deferredRLRow);
				}
				else
				{
					rowHolder.insert(row);
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
					deferredBaseRow = RowUtil.getEmptyValueRow(numberOfBaseColumns - 1, lcc);
			
					RowUtil.copyCloneColumns(deferredBaseRow, row, 
											numberOfBaseColumns - 1);
					deferredSparseRow = makeDeferredSparseRow(deferredBaseRow,
																baseRowReadList,
																lcc);
				}
			}
			else
			{
				if (fkChecker != null)
				{
                    // Argument "2" below: If a PK referenced by an FK is
                    // deferred, require at least two rows to be present in the
                    // primary table since we are deleting one of them below,
                    // and we need at least one to fulfill the constraint.
//IC see: https://issues.apache.org/jira/browse/DERBY-6576
                    fkChecker.doPKCheck(activation, row, false, 2);
				}

				baseRowLocation = 
					(RowLocation) (rlColumn).getObject();

                if (SanityManager.DEBUG)
				{
					SanityManager.ASSERT(baseRowLocation != null,
							"baseRowLocation is null");
				}

				rc.deleteRow(row,baseRowLocation);
//IC see: https://issues.apache.org/jira/browse/DERBY-690
				source.markRowAsDeleted();
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


	// execute the before triggers set on the table
    void fireBeforeTriggers() throws StandardException
	{

		if (triggerInfo != null)
		{
			if (triggerActivator == null)
			{
				triggerActivator = new TriggerEventActivator(lcc, 
															 constants.targetUUID,
															 triggerInfo,
															 TriggerExecutionContext.DELETE_EVENT,
															 activation, null
															 );
			}
			else
			{
				triggerActivator.reopen();
			}

			// fire BEFORE trigger
			triggerActivator.notifyEvent(TriggerEvents.BEFORE_DELETE, 
										 rowHolder.getResultSet(), 
										 (CursorResultSet)null,
										 constants.getBaseRowReadMap());
			triggerActivator.cleanup();

		}

	}

	//execute the after triggers set on the table.
	void fireAfterTriggers() throws StandardException
	{

		// fire AFTER trigger
		if (triggerActivator != null)
		{
			triggerActivator.reopen();
			triggerActivator.notifyEvent(TriggerEvents.AFTER_DELETE, 
										 rowHolder.getResultSet(),
										 (CursorResultSet)null,
										 constants.getBaseRowReadMap());
			triggerActivator.cleanup();
		}
		
	}


	//delete the rows that in case deferred case and
	//during cascade delete (All deletes are deferred during cascade action)
	void deleteDeferredRows() throws StandardException
	{
		
		DataValueDescriptor		rlColumn;
 		RowLocation	baseRowLocation;
        ExecRow     defRLRow;
//IC see: https://issues.apache.org/jira/browse/DERBY-673

        deferredBaseCC = tc.openCompiledConglomerate(
                false,
                (TransactionController.OPENMODE_FORUPDATE|
                 TransactionController.OPENMODE_SECONDARY_LOCKED),
                lockMode,
                TransactionController.ISOLATION_SERIALIZABLE,
                constants.heapSCOCI,
                heapDCOCI);
			
		CursorResultSet rs = rowHolder.getResultSet();
		try
		{
			/*
			** We need to do a fetch doing a partial row
			** read.  We need to shift our 1-based bit
			** set to a zero based bit set like the store
			** expects.
			*/
			FormatableBitSet readBitSet = RowUtil.shift(baseRowReadList, 1);

			rs.open();
//IC see: https://issues.apache.org/jira/browse/DERBY-673
            while ((defRLRow = rs.getNextRow()) != null)
			{
                rlColumn = defRLRow.getColumn(rlColumnNumber);
				baseRowLocation = 
					(RowLocation) (rlColumn).getObject();
	
				/* Get the base row at the given RowLocation */
				boolean row_exists = 
					deferredBaseCC.fetch(
										 baseRowLocation, deferredSparseRow.getRowArray(), 
										 readBitSet);

				// In case of cascade delete , things like before triggers can delete 
				// the rows before the dependent result get a chance to delete
				if(cascadeDelete && !row_exists)
					continue;

				if (SanityManager.DEBUG)
				{
					if (!row_exists)
					{
                        	SanityManager.THROWASSERT("could not find row "+baseRowLocation);
					}
				}
	
				rc.deleteRow(deferredBaseRow, baseRowLocation);
//IC see: https://issues.apache.org/jira/browse/DERBY-690
				source.markRowAsDeleted();
			}
		} finally
		{
				rs.close();
		}
	}


    /**
     * Make sure foreign key constraints are not violated
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-6576
    void runFkChecker(boolean restrictCheckOnly)
            throws StandardException
	{

		if (fkChecker != null)
		{
			/*
			** Second scan to make sure all the foreign key
			** constraints are ok.  We have to do this after
			** we have completed the deletes in case of self referencing
			** constraints.
			*/
			CursorResultSet rs = rowHolder.getResultSet();
			try
			{
				rs.open();

                ExecRow defRLRow;

                while ((defRLRow = rs.getNextRow()) != null)
				{
                    // Argument "1" below: If a PK referenced by an FK is
                    // deferred, require at least one to be present in the
                    // primary table since we have deleted the row unless
                    // postCheck == true, in which the call to postChecks does
                    // the actual checking, and we need at least one to fulfill
                    // the constraint.
                    fkChecker.doPKCheck(
//IC see: https://issues.apache.org/jira/browse/DERBY-6576
                            activation,
                            defRLRow,
                            restrictCheckOnly,
                            1);
				}

//IC see: https://issues.apache.org/jira/browse/DERBY-6576
                if (restrictCheckOnly) {
                    fkChecker.postCheck();
                }

			} finally
			{
				rs.close();
			}
		}
	}

	/**
	  *	create a source for the dependent table
	  *
	  * <P>Delete Cascade ResultSet class will override this method.
	  *
	  * @exception StandardException		Thrown on error
	  */
	NoPutResultSet createDependentSource(RowChanger rc)
		throws StandardException
	{
		return null;
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
		if (source != null)
		{
			source.close();
			// source is reused across executions
		}
		if (rc != null)
		{
			rc.close();
			// rc is reused across executions
		}

		if (rowHolder != null)
		{
			rowHolder.close();
			// rowHolder is reused across executions
		}

		if (fkChecker != null)
		{
			fkChecker.close();
			// fkcheckers is reused across executions
		}

		if (deferredBaseCC != null)
			deferredBaseCC.close();
		deferredBaseCC = null;

		if (rc != null) {
			rc.close();
		}
		close();
	}

    @Override
    public void close() throws StandardException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6576
        super.close( constants.underMerge() );
    }
                               
    @Override
	public void finish() throws StandardException {
		if (source != null)
			source.finish();
		super.finish();
	}

}












