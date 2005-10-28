/*

   Derby - Class org.apache.derby.impl.sql.execute.DeleteResultSet

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

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.services.stream.InfoStreams;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.RowChanger;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.db.TriggerExecutionContext;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import java.util.Properties;

import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;

/**
 * Delete the rows from the specified
 * base table. This will cause constraints to be checked
 * and triggers to be executed based on the c's and t's
 * compiled into the insert plan.
 */
public class DeleteResultSet extends DMLWriteResultSet
{
	public TransactionController   	tc;
	public DeleteConstantAction		constants;
    protected ResultDescription 				resultDescription;
	protected  NoPutResultSet			source;
	public  NoPutResultSet			savedSource;
	int 							numIndexes;
	protected RowChanger 			rc;
	public ExecRow					row;

	protected ConglomerateController	deferredBaseCC;
	public		LanguageConnectionContext lcc;

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

	/**
     * Returns the description of the deleted rows.
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
    public DeleteResultSet
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
    protected DeleteResultSet
	(
		NoPutResultSet		source,
		ConstantAction		passedInConstantAction,
		Activation			activation
	)
		throws StandardException
    {
		super(activation, passedInConstantAction);
		this.source = source;

		lcc = activation.getLanguageConnectionContext();
		tc = activation.getTransactionController();
		constants = (DeleteConstantAction) constantAction;
		fkInfoArray = constants.getFKInfo( lcc.getExecutionContext() );
		triggerInfo = constants.getTriggerInfo(lcc.getExecutionContext());
		noTriggersOrFks = ((fkInfoArray == null) && (triggerInfo == null));
		baseRowReadList = constants.getBaseRowReadList();
		if(source != null)
			resultDescription = source.getResultDescription();
		else
			resultDescription = constants.resultDescription;

	}

	/**
		@exception StandardException Standard Cloudscape error policy
	*/
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
			runFkChecker(true); //check for only RESTRICT referential action rule violations
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
	void  setup() throws StandardException
	{

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
		 * NOTE: We need to set ourself as the top result set
		 * if this is not the 1st execution.  (Done in constructor
		 * for 1st execution.)
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
		else
		{
			lcc.getStatementContext().setTopResultSet(this, subqueryTrackingArray);
		}
		/* decode the lock mode for the execution isolation level */
		lockMode = UpdateResultSet.decodeLockMode(lcc, constants.lockMode);

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

        rowCount = 0;
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
				rowHolder = new TemporaryRowHolderImpl(tc, properties, 
						(resultDescription != null) ?
							resultDescription.truncateColumns(rlColumnNumber) :
							null, false);


			}else
			{

				rowHolder = new TemporaryRowHolderImpl(tc, properties, 
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
				fkChecker = new RISetChecker(tc, fkInfoArray);
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
			/* By convention, the last column for a delete contains a SQLRef
			 * containing the RowLocation of the row to be deleted.  If we're
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
					fkChecker.doPKCheck(row, false);
				}

				baseRowLocation = 
					(RowLocation) (rlColumn).getObject();

				if (SanityManager.DEBUG)
				{
					SanityManager.ASSERT(baseRowLocation != null,
							"baseRowLocation is null");
				}

				rc.deleteRow(row,baseRowLocation);
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
															 tc, 
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
										 (CursorResultSet)null);

		}

	}

	//execute the after triggers set on the table.
	void fireAfterTriggers() throws StandardException
	{

		// fire AFTER trigger
		if (triggerActivator != null)
		{
			triggerActivator.notifyEvent(TriggerEvents.AFTER_DELETE, 
										 rowHolder.getResultSet(),
										 (CursorResultSet)null);
		}
		
	}


	//delete the rows that in case deferred case and
	//during cascade delete (All deletes are deferred during cascade action)
	void deleteDeferredRows() throws StandardException
	{
		
		DataValueDescriptor		rlColumn;
 		RowLocation	baseRowLocation;
		ExecRow		deferredRLRow = null;

		deferredBaseCC = tc.openCompiledConglomerate(false,
													 tc.OPENMODE_FORUPDATE|tc.OPENMODE_SECONDARY_LOCKED,
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
			while ((deferredRLRow = rs.getNextRow()) != null)
			{
				rlColumn = deferredRLRow.getColumn(rlColumnNumber);
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
			}
		} finally
		{
				rs.close();
		}
	}


	// make sure foreign key constraints are not violated
    void runFkChecker(boolean restrictCheckOnly) throws StandardException
	{

		ExecRow		deferredRLRow = null;
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
				while ((deferredRLRow = rs.getNextRow()) != null)
				{
					fkChecker.doPKCheck(deferredRLRow, restrictCheckOnly);
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
	public void	cleanUp() throws StandardException
	{ 
		numOpens = 0;

		if (triggerActivator != null)
		{
			triggerActivator.cleanup();
			// trigger activator is reused
		}

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

		super.close();
	}

	public void finish() throws StandardException {
		if (source != null)
			source.finish();
		super.finish();
	}

}












