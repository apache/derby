/*

   Derby - Class org.apache.derby.impl.sql.execute.InsertResultSet

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
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.types.StatisticsImpl;
import org.apache.derby.iapi.db.TriggerExecutionContext;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.StreamStorable;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.LanguageProperties;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.StatementUtil;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.dictionary.BulkInsertCounter;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.sql.dictionary.StatisticsDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecRowBuilder;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.sql.execute.RowChanger;
import org.apache.derby.iapi.sql.execute.TargetResultSet;
import org.apache.derby.iapi.store.access.AccessFactoryGlobals;
import org.apache.derby.iapi.store.access.BackingStoreHashtable;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.GroupFetchScanController;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowLocationRetRowSource;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.SortController;
import org.apache.derby.iapi.store.access.SortObserver;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.SQLBoolean;
import org.apache.derby.iapi.util.StringUtil;
import org.apache.derby.impl.sql.execute.DeferredConstraintsMemory.CheckInfo;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * Insert the rows from the source into the specified
 * base table. This will cause constraints to be checked
 * and triggers to be executed based on the c's and t's
 * compiled into the insert plan.
 */
//IC see: https://issues.apache.org/jira/browse/DERBY-6742
//IC see: https://issues.apache.org/jira/browse/DERBY-6743
//IC see: https://issues.apache.org/jira/browse/DERBY-6414
class InsertResultSet extends DMLWriteGeneratedColumnsResultSet implements TargetResultSet
{
	// RESOLVE. Embarrassingly large public state. If we could move the 
	// Replication code into the same package, then these variables 
	// could be protected. 
	// passed in at construction time
                                                 
	NoPutResultSet			savedSource;
	InsertConstantAction	constants;
    private GeneratedMethod         generationClauses;
	private GeneratedMethod			checkGM;
	private long					heapConglom;

	// divined at run time

	private RowChanger 				rowChanger;

	private	TransactionController 	tc;
	private	ExecRow					row;
	
	boolean					userSpecifiedBulkInsert;
	boolean					bulkInsertPerformed;

	// bulkInsert
	protected boolean				bulkInsert;
	private boolean					bulkInsertReplace;
	private boolean					firstRow = true;
	private	boolean[]				needToDropSort;

	/*
	** This hashtable is used to convert an index conglomerate
	** from it's old conglom number to the new one.  It is
	** bulk insert specific.
	*/
	private Hashtable<Long,Long>				indexConversionTable;

	// indexedCols is 1-based
	private FormatableBitSet					indexedCols;
	private ConglomerateController	bulkHeapCC;

	protected DataDictionary			dd;
	protected TableDescriptor			td;
		
	private ExecIndexRow[]			indexRows;
    private final int               fullTemplateId;
    private final String            schemaName;
    private final String            tableName;
	private	long[]					sortIds;
	private RowLocationRetRowSource[]
                                    rowSources;
	private	ScanController			bulkHeapSC;
	private ColumnOrdering[][]		ordering;
	private int[][]		            collation;
	private SortController[]		sorters;
	private	TemporaryRowHolderImpl	rowHolder;
	private RowLocation				rl;

	private	boolean					hasBeforeRowTrigger;
	private	BulkTableScanResultSet	tableScan;

	private int						numOpens;
	private boolean					firstExecute;

	// cached across open()s
	private	FKInfo[]				fkInfoArray;
	private	TriggerInfo				triggerInfo;
	private RISetChecker 			fkChecker;
	private TriggerEventActivator	triggerActivator;
    private BulkInsertCounter[]                 bulkInsertCounters;
    private BackingStoreHashtable   deferredChecks; // cached ref.
    private List<UUID>              violatingCheckConstraints;
	
	// TargetResultSet interface

	/**
	 * @see TargetResultSet#changedRow
	 *
	 * @exception StandardException thrown if cursor finish ed.
	 */
	public void changedRow(ExecRow execRow, RowLocation rowLocation)
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(bulkInsert,
				"bulkInsert exected to be true");
		}

		/* Set up sorters, etc. if 1st row and there are indexes */
		if (constants.irgs.length > 0)
		{
           RowLocation rlClone = (RowLocation) rowLocation.cloneValue(false);
//IC see: https://issues.apache.org/jira/browse/DERBY-4520

			// Objectify any the streaming columns that are indexed.
			for (int i = 0; i < execRow.getRowArray().length; i++)
			{
				if (! constants.indexedCols[i])
				{
					continue;
				}

				if (execRow.getRowArray()[i] instanceof StreamStorable)
					((DataValueDescriptor)execRow.getRowArray()[i]).getObject();
			}

			// Every index row will share the same row location, etc.
			if (firstRow)
			{
				firstRow = false;
				indexRows = new ExecIndexRow[constants.irgs.length];
				setUpAllSorts(execRow.getNewNullRow(), rlClone);
			}

			// Put the row into the indexes
			for (int index = 0; index < constants.irgs.length; index++)
			{
				// Get a new object Array for the index
				indexRows[index].getNewObjectArray();
				// Associate the index row with the source row
				constants.irgs[index].getIndexRow(execRow, 
											   rlClone, 
											   indexRows[index],
											   (FormatableBitSet) null);

				// Insert the index row into the matching sorter
				sorters[index].insert(indexRows[index].getRowArray());
			}
		}
	}

	/**
	 * Preprocess the source row.  Apply any check constraints here.
	 * Do an inplace cloning of all key columns.  For triggers, if
	 * we have a before row trigger, we fire it here if we can.
	 * This is useful for bulk insert where the store stands between
	 * the source and us.
	 *
	 * @param execRow	The source row.
	 *
	 * @return The preprocessed source row.
	 * @exception StandardException thrown on error
	 */
	public ExecRow preprocessSourceRow(ExecRow execRow)
		throws StandardException
	{
        if (triggerInfo != null) {
            // We do not use bulk insert if we have triggers
            if (SanityManager.DEBUG) {
                SanityManager.NOTREACHED();
            }
		}

        if ( generationClauses != null )
        {
            evaluateGenerationClauses( generationClauses, activation, sourceResultSet, execRow, false );
        }

        if (checkGM != null) {
            boolean allOk = evaluateCheckConstraints();
//IC see: https://issues.apache.org/jira/browse/DERBY-532
            if (!allOk) {
                if (SanityManager.DEBUG) {
                    SanityManager.ASSERT(
                            violatingCheckConstraints != null &&
                            violatingCheckConstraints.size() > 0) ;
                }

                // We will do a callback to remember this by
                // offendingRowLocation called from HeapController#load
            }
		}
		// RESOLVE - optimize the cloning
		if (constants.irgs.length > 0)
		{
			/* Do in-place cloning of all of the key columns */
			return execRow.getClone(indexedCols);
		}
		else
		{
			return execRow;
		}
	}


    public void offendingRowLocation(RowLocation rl, long constainerId)
//IC see: https://issues.apache.org/jira/browse/DERBY-532
            throws StandardException {
        if (violatingCheckConstraints != null) {
            deferredChecks =
                    DeferredConstraintsMemory.rememberCheckViolations(
                            lcc,
                            constants.targetUUID,
                            schemaName,
                            tableName,
                            deferredChecks,
                            violatingCheckConstraints,
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                            rl,
                            new CheckInfo[1] /* dummy */);
            violatingCheckConstraints.clear();
        }
    }

    /**
     * Run the check constraints against the current row. Raise an error if
     * a check constraint is violated, unless all the offending checks are
     * deferred, in which case a false value will be returned. A NULL value
     * will be interpreted as success (not violation).
     *
     * @exception StandardException thrown on error
     */
    private boolean evaluateCheckConstraints() throws StandardException {
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

    /*
     * class interface
     *
     */
    /**
	 *
	 * @exception StandardException		Thrown on error
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-467
    InsertResultSet(NoPutResultSet source, 
						   GeneratedMethod generationClauses,
						   GeneratedMethod checkGM,
                           int fullTemplate,
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                           String schemaName,
                           String tableName,
						   Activation activation)
		throws StandardException
    {
		super(activation);
		sourceResultSet = source;
		constants = (InsertConstantAction) constantAction;
        this.generationClauses = generationClauses;
		this.checkGM = checkGM;
//IC see: https://issues.apache.org/jira/browse/DERBY-6003
        this.fullTemplateId = fullTemplate;
//IC see: https://issues.apache.org/jira/browse/DERBY-532
        this.schemaName = schemaName;
        this.tableName = tableName;
		heapConglom = constants.conglomId;
        identitySequenceUUIDString = constants.identitySequenceUUIDString;

        tc = activation.getTransactionController();
//IC see: https://issues.apache.org/jira/browse/DERBY-2661
		fkInfoArray = constants.getFKInfo();
		triggerInfo = constants.getTriggerInfo();
		
		hasBeforeRowTrigger = (triggerInfo != null) ?
				triggerInfo.hasTrigger(true, true) :
				false;

//IC see: https://issues.apache.org/jira/browse/DERBY-4610
//IC see: https://issues.apache.org/jira/browse/DERBY-3049
        resultDescription = sourceResultSet.getResultDescription();
        
		// Is this a bulkInsert or regular insert?
		String insertMode = constants.getProperty("insertMode");

		initializeAIcache(constants.getAutoincRowLocation());
//IC see: https://issues.apache.org/jira/browse/DERBY-6742
//IC see: https://issues.apache.org/jira/browse/DERBY-6743
//IC see: https://issues.apache.org/jira/browse/DERBY-6414

		if (insertMode != null)
		{
			if (StringUtil.SQLEqualsIgnoreCase(insertMode,"BULKINSERT"))
			{
				userSpecifiedBulkInsert = true;
			}
			else if (StringUtil.SQLEqualsIgnoreCase(insertMode,"REPLACE"))
			{
				userSpecifiedBulkInsert = true;
				bulkInsertReplace = true;
				bulkInsert = true;

				/*
				** For now, we don't allow bulk insert replace when 
				** there is a trigger. 
				*/
				if (triggerInfo != null)
				{
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                    TriggerDescriptor trD = triggerInfo.getTriggerArray()[0];
                    throw StandardException.newException(
                        SQLState.LANG_NO_BULK_INSERT_REPLACE_WITH_TRIGGER_DURING_EXECUTION,
                        constants.getTableName(),
                        trD.getName());
				}
			}
		}
	}
	
	/**
		@exception StandardException Standard Derby error policy
	*/
	public void open() throws StandardException
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-2597
		setup();
		// Remember if this is the 1st execution
		firstExecute = (rowChanger == null);

		autoincrementGenerated = false;

		dd = lcc.getDataDictionary();

		verifyAutoGeneratedRScolumnsList(constants.targetUUID);
//IC see: https://issues.apache.org/jira/browse/DERBY-6742
//IC see: https://issues.apache.org/jira/browse/DERBY-6753

		rowCount = 0L;
//IC see: https://issues.apache.org/jira/browse/DERBY-6000

		if (numOpens++ == 0)
		{
			sourceResultSet.openCore();
		}
		else
		{
			sourceResultSet.reopenCore();
		}

		/* If the user specified bulkInsert (or replace) then we need 
		 * to get an exclusive table lock on the table.  If it is a
		 * regular bulk insert then we need to check to see if the
		 * table is empty.  (If not empty, then we end up doing a row
		 * at a time insert.)
		 */
		if (userSpecifiedBulkInsert)
		{
			if (! bulkInsertReplace)
			{
				bulkInsert = verifyBulkInsert();
			}
			else
			{
				getExclusiveTableLock();
			}
		}

		if (bulkInsert)
		{
			// Notify the source that we are the target
			sourceResultSet.setTargetResultSet(this);

//IC see: https://issues.apache.org/jira/browse/DERBY-6003
            ExecRow fullTemplate =
                ((ExecRowBuilder) activation.getPreparedStatement().
                    getSavedObject(fullTemplateId)).build(
                        activation.getExecutionFactory());

            bulkInsertCore(lcc, fullTemplate, heapConglom);

            if (triggerInfo != null) {
                if (SanityManager.DEBUG) {
                    // If we have triggers, we do not use bulkInsert
                    SanityManager.NOTREACHED();
                }
			}
			
//IC see: https://issues.apache.org/jira/browse/DERBY-6003
            bulkValidateForeignKeys(tc, lcc.getContextManager(), fullTemplate);
	
			bulkInsertPerformed = true;
		}
		else
		{
	        row = getNextRowCore(sourceResultSet);
			normalInsertCore(lcc, firstExecute);
		}

		/* Cache query plan text for source, before it gets blown away */
		if (lcc.getRunTimeStatisticsMode())
		{
			/* savedSource nulled after run time statistics generation */
			savedSource = sourceResultSet;
		}

		cleanUp();

//IC see: https://issues.apache.org/jira/browse/DERBY-6742
//IC see: https://issues.apache.org/jira/browse/DERBY-6743
//IC see: https://issues.apache.org/jira/browse/DERBY-6414
		saveAIcacheInformation(constants.getSchemaName(), 
			constants.getTableName(), constants.getColumnNames());

		endTime = getCurrentTimeMillis();
	}

	/**
	 * Clean up resources and call close on data members.
	 */
	public void close() throws StandardException {
		close( constants.underMerge() );
		if (autoGeneratedKeysRowsHolder != null) {
			autoGeneratedKeysRowsHolder.close();
		}
	}

	/**
	 * If user didn't provide columns list for auto-generated columns, then only include
	 * columns with auto-generated values in the resultset. Those columns would be ones
	 * with default value defined.
	 */
	private int[] generatedColumnPositionsArray()
		throws StandardException
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-532
        TableDescriptor tabDesb = dd.getTableDescriptor(constants.targetUUID);
		ColumnDescriptor cd;
        int size = tabDesb.getMaxColumnID();

		int[] generatedColumnPositionsArray = new int[size];
        Arrays.fill(generatedColumnPositionsArray, -1);
		int generatedColumnNumbers = 0;

		for (int i=0; i<size; i++) {
            cd = tabDesb.getColumnDescriptor(i+1);
			if (cd.isAutoincrement()) { //if the column has auto-increment value
				generatedColumnNumbers++;
				generatedColumnPositionsArray[i] = i+1;
			} else if (cd.getDefaultValue() != null || cd.getDefaultInfo() != null) {//default value
				generatedColumnNumbers++;
				generatedColumnPositionsArray[i] = i+1;
			}
		}
		int[] returnGeneratedColumnPositionsArray = new int[generatedColumnNumbers];

		for (int i=0, j=0; i<size; i++) {
			if (generatedColumnPositionsArray[i] != -1)
				returnGeneratedColumnPositionsArray[j++] = generatedColumnPositionsArray[i];
		}

		return returnGeneratedColumnPositionsArray;
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
		int index = columnPosition - 1;	// all our indices are 0 based.

		/* As in DB2, only for single row insert: insert into t1(c1) values (..) do
		 * we return the correct most recently generated identity column value.  For
		 * multiple row insert, or insert with sub-select, the return value is non-
		 * deterministic, and is the previous return value of the IDENTITY_VAL_LOCAL
		 * function, before the insert statement.  Also, DB2 can have at most 1 identity
		 * column per table.  The return value won't be affected either if Derby
		 * table has more than one identity columns.
		 */
		setIdentity = (! autoincrementGenerated) && isSourceRowResultSet();
		autoincrementGenerated = true;

  		if (bulkInsert)
  		{
            if ( identitySequenceUUIDString == null )
            {
                getOldStyleBulkInsertValue( index, increment );
            }
            else
            {
                if ( bulkInsertCounters[ index ] == null )
                {
                    bulkInsertCounters[ index ] = dd.getBulkInsertCounter( identitySequenceUUIDString, bulkInsertReplace );
                }
                bulkInsertCounters[ index ].getCurrentValueAndAdvance( (NumberDataValue) aiCache[ index ] );
            }
		}	
		else
		{
			NumberDataValue newValue;

            //
            // If there is a sequence generator uuid, then the database is at level
            // 10.11 or higher and we use the sequence generator to get the next
            // identity value. Otherwise, we use old-style logic.
            //
            if ( identitySequenceUUIDString == null )
            {
                newValue = getOldStyleIdentityValue( index );
            }
            else
            {
                newValue = activation.getCurrentValueAndAdvance
                    ( identitySequenceUUIDString, aiCache[ index ].getTypeFormatId() );
            }
            
			aiCache[index] = newValue;
			if (setIdentity)
            {
				identityVal = newValue.getLong();
            }
		}

		return (NumberDataValue) aiCache[index];
	}

     /**
      * Identity generation logic for bulk-insert used in pre-10.11 databases.
      *
      * @param index   0-based index into aiCache
      */
     private void    getOldStyleBulkInsertValue( int index, long increment )
         throws StandardException
     {
         NumberDataValue dvd;
         int columnPosition = index + 1;
         ColumnDescriptor cd = td.getColumnDescriptor(columnPosition);
         long ret;
 
         // for bulk insert we have the table descriptor
         //			System.out.println("in bulk insert");
         if (aiCache[index].isNull())
         {
//IC see: https://issues.apache.org/jira/browse/DERBY-532
             long startValue;
 
             if (bulkInsertReplace)
             {
                 startValue = cd.getAutoincStart();
             }
             else
             {
                 dvd = dd.getSetAutoincrementValue(
                                                   constants.autoincRowLocation[index],
                                                   tc, false, (NumberDataValue) aiCache[index], true);
                 startValue = dvd.getLong();
             }
             lcc.autoincrementCreateCounter(td.getSchemaName(),
                                            td.getName(),
                                            cd.getColumnName(),
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                                            Long.valueOf(startValue),
                                            increment,
                                            columnPosition);
 			
         }  		
         ret = lcc.nextAutoincrementValue(td.getSchemaName(),
                                          td.getName(),
                                          cd.getColumnName());
         aiCache[columnPosition - 1].setValue(ret);
     }
 
     /**
      * Identity generation logic used in pre-10.11 databases.
      *
      * @param index   0-based index into aiCache
      */
     private NumberDataValue getOldStyleIdentityValue( int index )
         throws StandardException
     {
//IC see: https://issues.apache.org/jira/browse/DERBY-5687
//IC see: https://issues.apache.org/jira/browse/DERBY-4437
         NumberDataValue newValue;
//IC see: https://issues.apache.org/jira/browse/DERBY-532
         TransactionController nestedTC = null;
         TransactionController tcToUse;
 
         try
         {
             // DERBY-5780, defaulting log syncing to false, which improves
             // performance of identity value generation.  If system 
             // crashes may reuse an identity value because commit did not
             // sync, but only if no subsequent user transaction has 
             // committed or aborted and thus no row can exist that used
             // the previous value.  Without this identity values pay
             // a synchronous I/O to the log file for each new value no
             // matter how many are inserted in a single transaction.
             nestedTC = tc.startNestedUserTransaction(false, false);
             tcToUse = nestedTC;
         }
         catch (StandardException se)
         {
             // If I cannot start a Nested User Transaction use the parent
             // transaction to do all the work.
             tcToUse = tc;
         }
 
         try 
         {
             /* If tcToUse == tc, then we are using parent xaction-- this
                can happen if for some reason we couldn't start a nested
                transaction
             */
             newValue = dd.getSetAutoincrementValue(
                                                    constants.autoincRowLocation[index],
                                                    tcToUse, true, (NumberDataValue) aiCache[index], (tcToUse == tc));
         }
         catch (StandardException se)
         {
             if (tcToUse == tc)
             {
                 /* we've using the parent xaction and we've timed out; just
                    throw an error and exit.
                 */
                 throw se;
             }
 
//IC see: https://issues.apache.org/jira/browse/DERBY-6692
             if ( se.getMessageId().equals(SQLState.LOCK_TIMEOUT) || se.isSelfDeadlock() )
             {
                 // if we couldn't do this with a nested xaction, retry with
                 // parent-- we need to wait this time!
                 newValue = dd.getSetAutoincrementValue(
                                                        constants.autoincRowLocation[index],
                                                        tc, true, (NumberDataValue) aiCache[index], true);
             }
             else if (se.getMessageId().equals(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE))
             {
                 // if we got an overflow error, throw a more meaningful
                 // error message
                 throw StandardException.newException(
                                                      SQLState.LANG_AI_OVERFLOW,
                                                      se,
                                                      constants.getTableName(),
                                                      constants.getColumnName(index));
             }
             else throw se;
         }
         finally 
         {
             // no matter what, commit the nested transaction; if something
             // bad happened in the child xaction lets not abort the parent
             // here.
                 
             if (nestedTC != null)
             {
                 // DERBY-5493 - prior to fix all nested user update 
                 // transactions did a nosync commit when commit() was 
                 // called, this default has been changed to do synced 
                 // commit.  Changed this commit to be commitNoSync to
                 // not introduce performce degredation for autoincrement
                 // keys.  As before, if server crashes the changes 
                 // made in the nested transaction may be lost.  If any
                 // subsequent user transaction is commited, including any
                 // inserts that would depend on the autoincrement value
                 // change then the nested tranaction is guaranteed on
                 // system crash.
                 nestedTC.commitNoSync(TransactionController.RELEASE_LOCKS);
                 nestedTC.destroy();
             }
         }
 
         return newValue;
     }
     
	// Is sourceResultSet a RowResultSet (values clause)?
	private boolean isSourceRowResultSet ()
	{
		boolean isRow = false;
		if (sourceResultSet instanceof NormalizeResultSet)
			isRow = (((NormalizeResultSet) sourceResultSet).source instanceof RowResultSet);
		return isRow;
	}

    // checks if source result set is a RowResultSet type.
    private boolean isSingleRowResultSet()
    {
        boolean isRow = false;
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1554
        if (sourceResultSet instanceof RowResultSet)
        	isRow = true;
        else if (sourceResultSet instanceof NormalizeResultSet)
            isRow = (((NormalizeResultSet) sourceResultSet).source instanceof RowResultSet);
        
        return isRow;
    }
	
	// Do the work for a "normal" insert
	private void normalInsertCore(LanguageConnectionContext lcc, boolean firstExecute)
		throws StandardException
	{
		boolean setUserIdentity = constants.hasAutoincrement() && isSingleRowResultSet();
//IC see: https://issues.apache.org/jira/browse/DERBY-532
        ExecRow deferredRowBuffer;
//IC see: https://issues.apache.org/jira/browse/DERBY-353
        long user_autoinc=0;
                        
		/* Get or re-use the row changer.
		 */
		if (firstExecute)
		{
			rowChanger = lcc.getLanguageConnectionFactory().getExecutionFactory()
						     .getRowChanger(
									 heapConglom,
									 constants.heapSCOCI,
									 heapDCOCI,
									 constants.irgs,
									 constants.indexCIDS,
									 constants.indexSCOCIs,
									 indexDCOCIs,
									 0, // number of columns in partial row meaningless for insert
									 tc,
									 null, //Changed column ids
									 constants.getStreamStorableHeapColIds(),
									 activation
							       );
			rowChanger.setIndexNames(constants.indexNames);
		}

		/* decode lock mode for the execution isolation level */
		int lockMode = decodeLockMode(constants.lockMode);

		rowChanger.open(lockMode);

		/* The source does not know whether or not we are doing a
		 * deferred mode insert.  If we are, then we must clear the
		 * index scan info from the activation so that the row changer
		 * does not re-use that information (which won't be valid for
		 * a deferred mode insert).
		 */
		if (constants.deferred)
		{
			activation.clearIndexScanInfo();
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

		if (firstExecute && constants.deferred)
		{
			Properties properties = new Properties();

			// Get the properties on the old heap
			rowChanger.getHeapConglomerateController().getInternalTablePropertySet(properties);

			/*
			** If deferred we save a copy of the entire row.
			*/
//IC see: https://issues.apache.org/jira/browse/DERBY-1112
			rowHolder = new TemporaryRowHolderImpl(activation, properties,
												   resultDescription);
			rowChanger.setRowHolder(rowHolder);
		}

		firstExecuteSpecialHandlingAutoGen(firstExecute, rowChanger, constants.targetUUID);
//IC see: https://issues.apache.org/jira/browse/DERBY-6742
//IC see: https://issues.apache.org/jira/browse/DERBY-6753

		while ( row != null )
		{
            // Collect auto-generated keys if requested.
            // DERBY-5823: No need to collect them if there are no
            // auto-generated key columns.
            if (activation.getAutoGeneratedKeysResultsetMode() &&
                    autoGeneratedKeysColumnIndexes.length > 0) {
                autoGeneratedKeysRowsHolder.insert(
                        getCompactRow(row, autoGeneratedKeysColumnIndexes));
            }

            // fill in columns that are computed from expressions on other columns
            evaluateGenerationClauses( generationClauses, activation, sourceResultSet, row, false );
                    
			/*
			** If we're doing a deferred insert, insert into the temporary
			** conglomerate.  Otherwise, insert directly into the permanent
			** conglomerates using the rowChanger.
			*/
			if (constants.deferred)
			{
					rowHolder.insert(row);
			}
			else
			{
                // Immediate mode violations will throw, so we only ever
                // see false here with deferred constraint mode for one or more
                // of the constraints being checked.
                boolean allOk = evaluateCheckConstraints();

				if (fkChecker != null)
				{
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                    fkChecker.doFKCheck(activation, row);
				}

				// Objectify any streaming columns that are indexed.
				if (constants.irgs.length > 0)
				{
					DataValueDescriptor[] rowArray = row.getRowArray();
					for (int i = 0; i < rowArray.length; i++)
					{
						//System.out.println("checking " + i);
						if (! constants.indexedCols[i])
						{
							continue;
						}


						if (rowArray[i] instanceof StreamStorable)
							rowArray[i].getObject();
					}
				}

//IC see: https://issues.apache.org/jira/browse/DERBY-532
                if (allOk) {
                    rowChanger.insertRow(row, false);
                } else {
                    RowLocation offendingRow = rowChanger.insertRow(row, true);
                    deferredChecks =
                        DeferredConstraintsMemory.rememberCheckViolations(
                            lcc,
                            constants.targetUUID,
                            schemaName,
                            tableName,
                            deferredChecks,
                            violatingCheckConstraints,
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                            offendingRow,
                            new CheckInfo[1] /* dummy */);
                }
			}

            rowCount++;
            
//IC see: https://issues.apache.org/jira/browse/DERBY-1554
            if(setUserIdentity )
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-353
                        dd = lcc.getDataDictionary();
                        td = dd.getTableDescriptor(constants.targetUUID);
                       
                        int maxColumns = td.getMaxColumnID();
                        int col;
                        
                        for(col=1;col<=maxColumns;col++)
                        {
                            ColumnDescriptor cd = td.getColumnDescriptor(col);
                            if(cd.isAutoincrement())
                            {
                                break;
                            }
                        }
                        
                        if(col <= maxColumns)
                        {
                            DataValueDescriptor dvd = row.cloneColumn(col);
                            user_autoinc = dvd.getLong();
                        }
             } 

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

		/*
		** If it's a deferred insert, scan the temporary conglomerate and
		** insert the rows into the permanent conglomerates using rowChanger.
		*/
		if (constants.deferred)
		{
			if (triggerInfo != null)
			{
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
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
										TriggerExecutionContext.INSERT_EVENT,
										activation, 
										v);
				}
				else
				{
					triggerActivator.reopen();
				}

				// fire BEFORE trigger, do this before checking constraints
				triggerActivator.notifyEvent(TriggerEvents.BEFORE_INSERT, 
												(CursorResultSet)null,
												rowHolder.getResultSet(), 
												(int[])null);
			}

			CursorResultSet rs = rowHolder.getResultSet();
			try
			{
				rs.open();
				while ((deferredRowBuffer = rs.getNextRow()) != null)
				{
					// we have to set the source row so the check constraint
					// sees the correct row.
					sourceResultSet.setCurrentRow(deferredRowBuffer);
                    boolean allOk = evaluateCheckConstraints();

//IC see: https://issues.apache.org/jira/browse/DERBY-532
                    if (allOk) {
                        rowChanger.insertRow(deferredRowBuffer, false);
                    } else {
                        RowLocation offendingRow =
                            rowChanger.insertRow(deferredRowBuffer, true);
                        deferredChecks =
                            DeferredConstraintsMemory.rememberCheckViolations(
                                lcc,
//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
                                constants.targetUUID,
                                schemaName,
                                tableName,
                                deferredChecks,
                                violatingCheckConstraints,
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                                offendingRow,
                                new CheckInfo[1]);
                    }
				}
			} finally
			{
				sourceResultSet.clearCurrentRow();
				rs.close();
			}
			
			if (fkChecker != null)
			{
				/*
				** Second scan to make sure all the foreign key
				** constraints are ok.  We have to do this after
				** we have completed the inserts in case of self
				** referencing constraints.
				*/
				rs = rowHolder.getResultSet();
				try
				{
					rs.open();
					while ((deferredRowBuffer = rs.getNextRow()) != null)
					{
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                        fkChecker.doFKCheck(activation, deferredRowBuffer);
					}
				} finally
				{
					rs.close();
				}
			}

			// fire AFTER trigger
			if (triggerActivator != null)
			{
				triggerActivator.notifyEvent(TriggerEvents.AFTER_INSERT, 
										(CursorResultSet)null,
										rowHolder.getResultSet(), 
										(int[])null);
			}
		}

		if (rowHolder != null)
		{
			rowHolder.close();
			// rowHolder kept across opens
		}
		if (fkChecker != null)
		{
			fkChecker.close();
			fkChecker = null;
		}
		if (setIdentity)
			lcc.setIdentityValue(identityVal);
                /*
                 * find the value of the identity column from the user inserted value
                 * and do a lcc.setIdentityValue(<user_value>);
                 */
//IC see: https://issues.apache.org/jira/browse/DERBY-1554
                else if(setUserIdentity )
                {
//IC see: https://issues.apache.org/jira/browse/DERBY-353
                        lcc.setIdentityValue(user_autoinc);
                } 
 }

    @Override
	protected ExecRow getNextRowCore( NoPutResultSet source )
		throws StandardException
	{
        ExecRow nextRow = super.getNextRowCore( source );
//IC see: https://issues.apache.org/jira/browse/DERBY-532

        if ( (nextRow != null) && constants.underMerge() ) {
            nextRow = processMergeRow( source, nextRow );
        }

        return nextRow;
	}

    /**
     * <p>
     * Special handling if this is an INSERT action of a MERGE statement.
     * </p>
     */
	private ExecRow processMergeRow( NoPutResultSet sourceRS, ExecRow row )
		throws StandardException
	{
        //
        // The normal processing of autogenerated columns happens in
        // the evaluation of the driving ResultSet. That processing assumes
        // that the distinguished ResultSet in the Activation is an InsertResultSet.
        // This is not true if we are running under a MERGE statement. In that
        // case, the distinguished ResultSet in the Activation is a MergeResultSet.
        // We have to wait until we have an InsertResultSet before we can generate
        // new identity values.
        //
        //
        if ( constants.hasAutoincrement() )
        {
            int     columnPosition = constants.getAutoGenColumn();
            long    increment = constants.getAutoincIncrement( columnPosition );
            DataValueDescriptor dvd = row.getColumn( columnPosition + 1 );
            
            //
            // If the identity column was declared BY DEFAULT, then it could be
            // overridden by the WHEN NOT MATCHED clause. In that case, the
            // row will contain a non-null value which we do not want to clobber.
            //
            //boolean needToGenerateValue = ( dvd == null ) || ( dvd.isNullOp().getBoolean() );
            boolean needToGenerateValue = ( dvd == null );

            if ( needToGenerateValue )
            {
                NumberDataValue newIdentityValue = getSetAutoincrementValue( columnPosition + 1, increment );

                row.setColumn( columnPosition + 1, newIdentityValue );
            }
        }

        return normalizeRow( sourceRS, row );
	}

	// Do the work for a bulk insert
    private void bulkInsertCore(LanguageConnectionContext lcc,
//IC see: https://issues.apache.org/jira/browse/DERBY-6003
                                ExecRow fullTemplate,
								long oldHeapConglom)
		throws StandardException
	{
		bulkHeapCC = tc.openCompiledConglomerate(
                                false,
                                TransactionController.OPENMODE_FORUPDATE,
                                TransactionController.MODE_TABLE,
                                TransactionController.ISOLATION_SERIALIZABLE,
								constants.heapSCOCI,
								heapDCOCI);

		long newHeapConglom;

		Properties properties = new Properties();

		// Get the properties on the old heap
		bulkHeapCC.getInternalTablePropertySet(properties);

        if (triggerInfo != null) {
            // no triggers in bulk insert mode
            if (SanityManager.DEBUG) {
                SanityManager.NOTREACHED();
            }
		}

		/*
		** If we have a before row trigger, then we
		** are going to use a row holder pass to our
		** trigger.
		*/
		if (hasBeforeRowTrigger && rowHolder != null)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-1112
			rowHolder =
//IC see: https://issues.apache.org/jira/browse/DERBY-4610
//IC see: https://issues.apache.org/jira/browse/DERBY-3049
				new TemporaryRowHolderImpl(activation, properties,
										   resultDescription);
		}

		// Add any new properties or change the values of any existing properties
		Properties targetProperties = constants.getTargetProperties();
		Enumeration key = targetProperties.keys();
		while (key.hasMoreElements())
		{
			String keyValue = (String) key.nextElement();
			properties.put(keyValue, targetProperties.getProperty(keyValue));
		}

		// Are there indexes to be updated?
		if (constants.irgs.length > 0)
		{
			// Tell source whether or not we need the RIDs back
			sourceResultSet.setNeedsRowLocation(true);
		}

//IC see: https://issues.apache.org/jira/browse/DERBY-532
        if (constants.hasDeferrableChecks) {
            sourceResultSet.setHasDeferrableChecks();
        }


		dd = lcc.getDataDictionary();
		td = dd.getTableDescriptor(constants.targetUUID);

		/* Do the bulk insert - only okay to reuse the
		 * same conglomerate if bulkInsert.
		 */
		long[] loadedRowCount = new long[1];
		if (bulkInsertReplace)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
			newHeapConglom = 
                tc.createAndLoadConglomerate(
                    "heap",
                    fullTemplate.getRowArray(),
                    null, //column sort order - not required for heap
                    td.getColumnCollationIds(),
                    properties,
                    TransactionController.IS_DEFAULT,
                    sourceResultSet,
                    loadedRowCount);
		}
		else
		{
			newHeapConglom = 
                tc.recreateAndLoadConglomerate(
                    "heap",
                    false,
                    fullTemplate.getRowArray(),
                    null, //column sort order - not required for heap
                    td.getColumnCollationIds(),
                    properties,
                    TransactionController.IS_DEFAULT,
                    oldHeapConglom,
                    sourceResultSet,
                    loadedRowCount);
		}

		/* Nothing else to do if we get back the same conglomerate number.
		 * (In 2.0 this means that 0 rows were inserted.)
		 */
		if (newHeapConglom == oldHeapConglom)
		{
            return;
		}

		// Find out how many rows were inserted
		rowCount = loadedRowCount[0];
//IC see: https://issues.apache.org/jira/browse/DERBY-6000

		// Set the "estimated" row count
		setEstimatedRowCount(newHeapConglom);

		/*
		** Inform the data dictionary that we are about to write to it.
		** There are several calls to data dictionary "get" methods here
		** that might be done in "read" mode in the data dictionary, but
		** it seemed safer to do this whole operation in "write" mode.
		**
		** We tell the data dictionary we're done writing at the end of
		** the transaction.
		*/
		dd.startWriting(lcc);

        //
        // If we were doing bulkInsert, then we need to flush the last
        // identity value to disk.
        //
        if ( identitySequenceUUIDString == null )
        {
            lcc.autoincrementFlushCache(constants.targetUUID);
        }
        else
        {
            for ( BulkInsertCounter bic : bulkInsertCounters )
            {
                if ( bic != null )
                {
                    dd.flushBulkInsertCounter( identitySequenceUUIDString, bic );
                }
            }
        }

		// invalidate any prepared statements that
		// depended on this table (including this one)
		DependencyManager dm = dd.getDependencyManager();

		dm.invalidateFor(td, DependencyManager.BULK_INSERT, lcc);

		
		// Update all indexes
		if (constants.irgs.length > 0)
		{
//            MEN VI HAR MANGE SORTS, EN PR INDEX: alle blir droppet, hvordan
//                    assossiere alle med nye indekser som tildeles inni her???
//                            FIXME!!
			updateAllIndexes(newHeapConglom, constants, td, dd, fullTemplate);
		}

		// Drop the old conglomerate
		bulkHeapCC.close();
		bulkHeapCC = null;

		/* Update the DataDictionary
		 * RESOLVE - this will change in 1.4 because we will get
		 * back the same conglomerate number
		 */
		// Get the ConglomerateDescriptor for the heap
		ConglomerateDescriptor cd = td.getConglomerateDescriptor(oldHeapConglom);

		// Update sys.sysconglomerates with new conglomerate #
		dd.updateConglomerateDescriptor(cd, newHeapConglom, tc);
		tc.dropConglomerate(oldHeapConglom);
		// END RESOLVE
	}

	/**
	** Bulk Referential Integrity Checker
	*/
    private void bulkValidateForeignKeys(
//IC see: https://issues.apache.org/jira/browse/DERBY-6003
            TransactionController tc, ContextManager cm, ExecRow fullTemplate)
		throws StandardException
	{
		/*
		** If there are no foreign keys, then nothing to worry 
		** about.
		** With bulk insert replace, we still need to verify 
		** all non-self referencing foreign keys when
		** there are no rows inserted into the table.
		*/
		if ((indexRows == null && !bulkInsertReplace) || 
			fkInfoArray == null)
		{
			return;
		}

//IC see: https://issues.apache.org/jira/browse/DERBY-532
        for (FKInfo fkInfo : fkInfoArray)
		{

			/* With regular bulk insert, we only need to check the
			 * foreign keys in the table we inserted into.  We need
			 * to get the new conglomerate #s for the foreign keys.
			 *
			 * With bulk insert replace, we need to check both the
			 * foreign keys in the table as well as any foreign keys
			 * on other tables referencing the table we inserted into.
			 * If the foreign key is self-referencing then we need to
			 * get the new conglomerate #, otherwise the conglomerate
			 * # is the same as the compile time conglomerate #.
			 * If the foreign key is self-referencing then we need to
			 * get the new conglomerate # for the primary key as it
			 * has changed.  However, if the foreign key is not self-referencing
			 * then we only need to get the new conglomerate # for
			 * the primary key if the primary key is on the table being 
			 * inserted into.
			 */
			if (bulkInsertReplace)
			{
				for (int index = 0; index < fkInfo.fkConglomNumbers.length; index++)
				{
					/* No need to check foreign key if it is self referencing
					 * and there were no rows inserted on the replace, as both
					 * indexes will be empty.
					 */
					if (fkInfo.fkIsSelfReferencing[index] && indexRows == null)
					{
						continue;
					}

					long pkConglom;
					long fkConglom;

					if (fkInfo.fkIsSelfReferencing[index])
					{
						/* Self-referencing foreign key.  Both conglomerate
						 * #s have changed.
						 */
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
						pkConglom = (indexConversionTable.get(
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                            Long.valueOf(fkInfo.refConglomNumber))).longValue();
						fkConglom = (indexConversionTable.get(
                            Long.valueOf(fkInfo.fkConglomNumbers[index]))).
                            longValue();
					}
					else
					{
						/* Non-self referencing foreign key.  At this point we
						 * don't know if the primary key or the foreign key is
						 * on this table.  So, for each one, we look to see
						 * if the old conglomerate # is in the conversion table.
						 * If so, then we get the new conglomerate #, otherwise
						 * we use the compile time conglomerate #.  This
						 * is very simple, though not very elegant.
						 */
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
						Long pkConglomLong = indexConversionTable.get(
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                            Long.valueOf(fkInfo.refConglomNumber));
						Long fkConglomLong = indexConversionTable.get(
                            Long.valueOf(fkInfo.fkConglomNumbers[index]));

						if (pkConglomLong == null)
						{
							pkConglom = fkInfo.refConglomNumber;
						}
						else
						{
							pkConglom = pkConglomLong.longValue();
						}
						if (fkConglomLong == null)
						{
							fkConglom = fkInfo.fkConglomNumbers[index];
						}
						else
						{
							fkConglom = fkConglomLong.longValue();
						}
					}
					bulkValidateForeignKeysCore(
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                            tc, cm, fkInfo, fkConglom, pkConglom,
//IC see: https://issues.apache.org/jira/browse/DERBY-6003
							fkInfo.fkConstraintNames[index], fullTemplate);
				}
			}
			else
			{
				/*
				** We have a FKInfo for each foreign key we are
				** checking.  Note that there are no primary key
				** checks on insert, so we can always reference
				** element[0] in the current FKInfo structure.
				*/ 
				if (SanityManager.DEBUG)
				{
					SanityManager.ASSERT(fkInfo.type == FKInfo.FOREIGN_KEY, 
						"error, expected to only check foreign keys on insert");
				}
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
				Long fkConglom = indexConversionTable.get(fkInfo.fkConglomNumbers[0]);
				bulkValidateForeignKeysCore(
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                        tc, cm, fkInfo, fkConglom.longValue(),
//IC see: https://issues.apache.org/jira/browse/DERBY-6003
						fkInfo.refConglomNumber, fkInfo.fkConstraintNames[0],
                        fullTemplate);
			}
		}
	}

	private void bulkValidateForeignKeysCore(
						TransactionController tc, ContextManager cm, 
						FKInfo fkInfo, long fkConglom, long pkConglom,
//IC see: https://issues.apache.org/jira/browse/DERBY-6003
                        String fkConstraintName, ExecRow fullTemplate)
		throws StandardException
	{
		ExecRow 		            template;
		GroupFetchScanController 	refScan = null;
		GroupFetchScanController 	fkScan  = null;

		try
		{

				template = makeIndexTemplate(fkInfo, fullTemplate, cm);

				/*
				** The indexes have been dropped and recreated, so
				** we need to get the new index conglomerate number.
				*/
				fkScan = 
                    tc.openGroupFetchScan(
                        fkConglom,
                        false,                       // hold 
                        0, 							 // read only
                        // doesn't matter, already locked
//IC see: https://issues.apache.org/jira/browse/DERBY-6063
                        TransactionController.MODE_TABLE,
                        // doesn't matter, already locked
                        TransactionController.ISOLATION_READ_COMMITTED,
                        (FormatableBitSet)null, 				 // retrieve all fields
                        (DataValueDescriptor[])null, // startKeyValue
                        ScanController.GE,           // startSearchOp
                        null,                        // qualifier
                        (DataValueDescriptor[])null, // stopKeyValue
                        ScanController.GT            // stopSearchOp 
                        );

				if (SanityManager.DEBUG)
				{	
					/*
					** Bulk insert replace calls this method regardless
					** of whether or not any rows were inserted because
					** it has to check any referencing foreign keys
					** after the replace.  Otherwise, we
					** make sure that we actually have a row in the fk.
					** If not, we have an error because we thought that
					** since indexRows != null, we must have gotten some
					** rows.
					*/ 
					if (! bulkInsertReplace)
					{
						SanityManager.ASSERT(fkScan.next(),
							"No rows in fk index, even though indexRows != null");
			
						/*
						** Crank up the scan again.
						*/	
						fkScan.reopenScan(
							(DataValueDescriptor[])null,    // startKeyValue
							ScanController.GE,              // startSearchOp
							null,                           // qualifier
							(DataValueDescriptor[])null,    // stopKeyValue
							ScanController.GT               // stopSearchOp 
		                      );
					}
				}

				/*
				** Open the referenced key scan.  Use row locking on
				** the referenced table unless it is self-referencing
	 			** (in which case we don't need locks)
				*/	
				refScan = 
                    tc.openGroupFetchScan(
						pkConglom,
						false,                       	// hold 
						0, 								// read only
                        (fkConglom == pkConglom) ?
//IC see: https://issues.apache.org/jira/browse/DERBY-6063
                                TransactionController.MODE_TABLE :
                                TransactionController.MODE_RECORD,
                        // read committed is good enough
                        TransactionController.ISOLATION_READ_COMMITTED,
						(FormatableBitSet)null, 					// retrieve all fields
						(DataValueDescriptor[])null,    // startKeyValue
						ScanController.GE,            	// startSearchOp
						null,                         	// qualifier
						(DataValueDescriptor[])null,    // stopKeyValue
						ScanController.GT             	// stopSearchOp 
						);

				/*
				** Give the scans to the bulk checker to do its
				** magic.  It will do a merge on the two indexes. 
				*/	
				ExecRow firstFailedRow = template.getClone();
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                RIBulkChecker riChecker = new RIBulkChecker(activation,
                                            refScan,
											fkScan, 
											template, 	
											true, 				// fail on 1st failure
											(ConglomerateController)null,
                                            firstFailedRow,
                                            fkInfo.schemaName,
                                            fkInfo.tableName,
                                            fkInfo.fkIds[0],
                                            fkInfo.deferrable[0],
                                            fkConglom,
                                            pkConglom);
	
				int numFailures = riChecker.doCheck();
				if (numFailures > 0)
				{
					StandardException se = StandardException.newException(SQLState.LANG_FK_VIOLATION, fkConstraintName,
									fkInfo.tableName,
									StatementUtil.typeName(fkInfo.stmtType),
									RowUtil.toString(firstFailedRow, 0, fkInfo.colArray.length - 1));
					throw se;
				}
		}
		finally
		{
			if (fkScan != null)
			{
				fkScan.close();
			}
			if (refScan != null)
			{
				refScan.close();
			}
		}
	}

	/**
	 * Make a template row with the correct columns.
	 */
	private ExecRow makeIndexTemplate(FKInfo fkInfo, ExecRow fullTemplate, ContextManager cm)
		throws StandardException
	{
		ExecRow newRow = RowUtil.getEmptyIndexRow(fkInfo.colArray.length+1, lcc);
//IC see: https://issues.apache.org/jira/browse/DERBY-2661

		DataValueDescriptor[] templateColArray = fullTemplate.getRowArray();
		DataValueDescriptor[] newRowColArray   = newRow.getRowArray();

		int i;
		for (i = 0; i < fkInfo.colArray.length; i++)
		{
			newRowColArray[i] = 
//IC see: https://issues.apache.org/jira/browse/DERBY-4520
                templateColArray[fkInfo.colArray[i] - 1].cloneValue(false);
		}

       newRowColArray[i] = fkInfo.rowLocation.cloneValue(false);

		return newRow;
	}

	/**
	 * Set up to update all of the indexes on a table when doing a bulk insert
	 * on an empty table.
	 *
	 * @exception StandardException					thrown on error
	 */
	private void setUpAllSorts(ExecRow sourceRow,
							   RowLocation rl)
		throws StandardException
    {
		int					numIndexes = constants.irgs.length;
		int					numColumns = td.getNumberOfColumns();

//IC see: https://issues.apache.org/jira/browse/DERBY-2537
		ordering        = new ColumnOrdering[numIndexes][];
        collation       = new int[numIndexes][];
		needToDropSort  = new boolean[numIndexes];
		sortIds         = new long[numIndexes];
		rowSources      = new RowLocationRetRowSource[numIndexes];
		// indexedCols is 1-based
		indexedCols     = new FormatableBitSet(numColumns + 1);


		/* For each index, build a single index row, collation templage, 
         * and a sorter. 
         */
		for (int index = 0; index < numIndexes; index++)
		{
			// Update the bit map of indexed columns
			int[] keyColumns = constants.irgs[index].baseColumnPositions();
			for (int i2 = 0; i2 < keyColumns.length; i2++)
			{
				// indexedCols is 1-based
				indexedCols.set(keyColumns[i2]);
			}

			// create a single index row template for each index
			indexRows[index] = constants.irgs[index].getIndexRowTemplate();

			// Get an index row based on the base row
			// (This call is only necessary here because we need to 
            // pass a template to the sorter.)
			constants.irgs[index].getIndexRow(sourceRow, 
											  rl, 
											  indexRows[index],
											  (FormatableBitSet) null);

			/* For non-unique indexes, we order by all columns + the RID.
			 * For unique indexes, we just order by the columns.
			 * We create a unique index observer for unique indexes
			 * so that we can catch duplicate key
			 */

			// Get the ConglomerateDescriptor for the index
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
			ConglomerateDescriptor cd = 
                td.getConglomerateDescriptor(constants.indexCIDS[index]);

			int[] baseColumnPositions = 
                constants.irgs[index].baseColumnPositions();
			boolean[] isAscending     = constants.irgs[index].isAscending();
           
			int numColumnOrderings;
            SortObserver sortObserver;
//IC see: https://issues.apache.org/jira/browse/DERBY-532

			/* We can only reuse the wrappers when doing an
			 * external sort if there is only 1 index.  Otherwise,
			 * we could get in a situation where 1 sort reuses a
			 * wrapper that is still in use in another sort.
			 */
			boolean reuseWrappers = (numIndexes == 1);
//IC see: https://issues.apache.org/jira/browse/DERBY-532
//IC see: https://issues.apache.org/jira/browse/DERBY-3330
//IC see: https://issues.apache.org/jira/browse/DERBY-6419
            final IndexRowGenerator indDes = cd.getIndexDescriptor();
            Properties sortProperties = null;
            String indexOrConstraintName = cd.getConglomerateName();
            boolean deferred = false;
            boolean deferrable = false;

            UUID uniqueDeferrableConstraintId = null;
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
            if (cd.isConstraint())
            {
                // so, the index is backing up a constraint

                ConstraintDescriptor conDesc =
                        dd.getConstraintDescriptor(td, cd.getUUID());

                indexOrConstraintName = conDesc.getConstraintName();
                deferred = lcc.isEffectivelyDeferred(
                        lcc.getCurrentSQLSessionContext(activation),
//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
                        conDesc.getUUID());
                deferrable = conDesc.deferrable();
                uniqueDeferrableConstraintId = conDesc.getUUID();
            }

            if (indDes.isUnique() || indDes.isUniqueDeferrable())
            {
                numColumnOrderings =
                        indDes.isUnique() ? baseColumnPositions.length :
                        baseColumnPositions.length + 1;

				sortObserver = 
                    new UniqueIndexSortObserver(
                            lcc,
//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
                            uniqueDeferrableConstraintId,
                            false, // don't clone rows
                            deferrable,
                            deferred,
                            indexOrConstraintName,
                            indexRows[index],
                            reuseWrappers,
                            td.getName());
            } else if (indDes.isUniqueWithDuplicateNulls())
            {
                numColumnOrderings = baseColumnPositions.length + 1;

                // tell transaction controller to use the unique with
                // duplicate nulls sorter, when making createSort() call.
                sortProperties = new Properties();
                sortProperties.put(
                   AccessFactoryGlobals.IMPL_TYPE,
                   AccessFactoryGlobals.SORT_UNIQUEWITHDUPLICATENULLS_EXTERNAL);
                //use sort operator which treats nulls unequal
                sortObserver =
                        new UniqueWithDuplicateNullsIndexSortObserver(
                        lcc,
//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
                        uniqueDeferrableConstraintId,
                        true,
                        deferrable,
                        deferred,
                        indexOrConstraintName,
                        indexRows[index],
                        true,
                        td.getName());

            }
			else
			{
				numColumnOrderings = baseColumnPositions.length + 1;
				sortObserver       = new BasicSortObserver(false, false, 
													 indexRows[index],
													 reuseWrappers);
			}
			ordering[index] = new ColumnOrdering[numColumnOrderings];
			for (int ii =0; ii < isAscending.length; ii++) 
			{
				ordering[index][ii] = new IndexColumnOrder(ii, isAscending[ii]);
			}
			if (numColumnOrderings > isAscending.length)
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
				ordering[index][isAscending.length] = 
                    new IndexColumnOrder(isAscending.length);
            }

            // set collation templates for later index creation 
            // call (createAndLoadConglomerate())
            collation[index] = 
                constants.irgs[index].getColumnCollationIds(
                    td.getColumnDescriptorList());

			// create the sorters
			sortIds[index] = 
                tc.createSort(
//IC see: https://issues.apache.org/jira/browse/DERBY-532
//IC see: https://issues.apache.org/jira/browse/DERBY-3330
//IC see: https://issues.apache.org/jira/browse/DERBY-6419
                    sortProperties,
                    indexRows[index].getRowArrayClone(),
                    ordering[index],
                    sortObserver,
                    false,			                             // not in order
                    (int) sourceResultSet.getEstimatedRowCount(), // est rows	
                    -1				// est row size, -1 means no idea	
                    );

			needToDropSort[index] = true;
		}

		sorters = new SortController[numIndexes];

		// Open the sorts
		for (int index = 0; index < numIndexes; index++)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
			sorters[index]        = tc.openSort(sortIds[index]);
			needToDropSort[index] = true;
		}
	}

	/**
	 * Update all of the indexes on a table when doing a bulk insert
	 * on an empty table.
	 *
	 * @exception StandardException					thrown on error
	 */
	private void updateAllIndexes(long newHeapConglom, 
								  InsertConstantAction constants, 
								  TableDescriptor td,
								  DataDictionary dd,
								  ExecRow fullTemplate)
		throws StandardException
    {
		int	numIndexes = constants.irgs.length;

		/*
		** If we didn't actually read in any rows, then
		** we don't need to do anything, unless we were
		** doing a replace.
		*/
		if (indexRows == null)
		{
			if (bulkInsertReplace)
			{
				emptyIndexes(newHeapConglom, constants, td, dd, fullTemplate);
			}
			return;
		}

		dd.dropStatisticsDescriptors(td.getUUID(), null, tc);
		long[] newIndexCongloms = new long[numIndexes];

//IC see: https://issues.apache.org/jira/browse/DERBY-6213
		indexConversionTable = new Hashtable<Long,Long>(numIndexes);
		// Populate each index
		for (int index = 0; index < numIndexes; index++)
		{
			ConglomerateController indexCC;
			Properties properties = new Properties();
			ConglomerateDescriptor cd;
			// Get the ConglomerateDescriptor for the index
			cd = td.getConglomerateDescriptor(constants.indexCIDS[index]);

			
			// Build the properties list for the new conglomerate
			indexCC = tc.openCompiledConglomerate(
                                false,
                                TransactionController.OPENMODE_FORUPDATE,
                                TransactionController.MODE_TABLE,
                                TransactionController.ISOLATION_SERIALIZABLE,
								constants.indexSCOCIs[index],
								indexDCOCIs[index]);

			// Get the properties on the old index
			indexCC.getInternalTablePropertySet(properties);

			/* Create the properties that language supplies when creating the
			 * the index.  (The store doesn't preserve these.)
			 */
			int indexRowLength = indexRows[index].nColumns();
			properties.put("baseConglomerateId", Long.toString(newHeapConglom));
			if (cd.getIndexDescriptor().isUnique())
			{
				properties.put("nUniqueColumns", 
							   Integer.toString(indexRowLength - 1));
			}
			else
			{
				properties.put("nUniqueColumns", 
							   Integer.toString(indexRowLength));
			}

//IC see: https://issues.apache.org/jira/browse/DERBY-532
            if ( cd.getIndexDescriptor().isUniqueWithDuplicateNulls() &&
                !cd.getIndexDescriptor().hasDeferrableChecking() )
			{
				properties.put(
                    "uniqueWithDuplicateNulls", Boolean.toString(true));
			}

			properties.put("rowLocationColumn", 
							Integer.toString(indexRowLength - 1));
			properties.put("nKeyFields", Integer.toString(indexRowLength));

			indexCC.close();

			// We can finally drain the sorter and rebuild the index
			// RESOLVE - all indexes are btrees right now
			// Populate the index.
			sorters[index].completedInserts();
			sorters[index] = null;
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
			rowSources[index] = 
                new CardinalityCounter(tc.openSortRowSource(sortIds[index]));

//IC see: https://issues.apache.org/jira/browse/DERBY-2537
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
			newIndexCongloms[index] = 
                tc.createAndLoadConglomerate(
                    "BTREE",
                    indexRows[index].getRowArray(),
                    ordering[index],
                    collation[index],
                    properties,
                    TransactionController.IS_DEFAULT,
                    rowSources[index],
                    (long[]) null);

			CardinalityCounter cCount = (CardinalityCounter)rowSources[index];
			long numRows;
			if ((numRows = cCount.getRowCount()) > 0)
			{
				long[] c = cCount.getCardinality();

				for (int i= 0; i < c.length; i++)
				{
					StatisticsDescriptor statDesc = 
						new StatisticsDescriptor(dd, dd.getUUIDFactory().createUUID(),
													cd.getUUID(), td.getUUID(),
													"I", new
														StatisticsImpl(numRows,
																	   c[i]),
													i + 1);
					dd.addDescriptor(statDesc, null, 
									 DataDictionary.SYSSTATISTICS_CATALOG_NUM,
									 true, tc);					
				}	
				
			}

			/* Update the DataDictionary
			 * RESOLVE - this will change in 1.4 because we will get
			 * back the same conglomerate number
			 *
			 * Update sys.sysconglomerates with new conglomerate #, if the
			 * conglomerate is shared by duplicate indexes, all the descriptors
			 * for those indexes need to be updated with the new number.
			 */
			dd.updateConglomerateDescriptor(
						td.getConglomerateDescriptors(constants.indexCIDS[index]),
						newIndexCongloms[index], tc);

			// Drop the old conglomerate
			tc.dropConglomerate(constants.indexCIDS[index]);

//IC see: https://issues.apache.org/jira/browse/DERBY-6856
			indexConversionTable.put(constants.indexCIDS[index], newIndexCongloms[index]);
		}
	}

	/**
	 * @see ResultSet#cleanUp
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	cleanUp() throws StandardException
	{

		if (tableScan != null)
		{
			tableScan.close();
			tableScan = null;
		}

		if (triggerActivator != null)
		{
			triggerActivator.cleanup();
			// triggerActivator is reused across executions
		}

		/* Close down the source ResultSet tree */
		if (sourceResultSet != null)
		{
			sourceResultSet.close();
			// sourceResultSet is reused across executions
		}
		numOpens = 0;

		if (rowChanger != null)
		{
			rowChanger.close();
		}

		if (rowHolder != null)
		{
			rowHolder.close();
		}

		if (fkChecker != null)
		{
			fkChecker.close();
			// fkChecker is reused across executions
		}

		if (bulkHeapCC != null)
		{
			bulkHeapCC.close();
			bulkHeapCC = null;
		}

		if (bulkHeapSC != null)
		{
			bulkHeapSC.close();
			bulkHeapSC = null;
		}

		// Close each sorter
		if (sorters != null)
		{
			for (int index = 0; index < constants.irgs.length; index++)
			{
				if (sorters[index] != null)
				{
//IC see: https://issues.apache.org/jira/browse/DERBY-2486
//IC see: https://issues.apache.org/jira/browse/DERBY-2486
//IC see: https://issues.apache.org/jira/browse/DERBY-2486
					sorters[index].completedInserts();
				}
				sorters[index] = null;
			}
		}

		if (needToDropSort != null)
		{
			for (int index = 0; index < needToDropSort.length; index++)
			{
				if (needToDropSort[index])
				{
				 	tc.dropSort(sortIds[index]);
					needToDropSort[index] = false;
				}
			}
		}

		if (rowSources != null)
		{
			for (int index = 0; index < rowSources.length; index++)
			{
				if (rowSources[index] != null)
				{
					rowSources[index].closeRowSource();
					rowSources[index] = null;
				}
			}
		}
		close( constants.underMerge() );
	}

	// Class implementation
	
	/**
	 * Verify that bulkInsert is allowed on this table.
	 * The execution time check to see if bulkInsert is allowed
	 * simply consists of checking to see if this is not a deferred
	 * mode insert and that the table is empty if this is not replace.
	 *
	 * A side effect of calling this method is to get an exclusive
	 * table lock on the table.
	 *
	 * @return Whether or not bulkInsert is allowed on this table.
	 *
	 * @exception StandardException		Thrown on error
	 */
	protected boolean verifyBulkInsert()
		throws StandardException
	{
		// bulk insert is disabled for deferred mode inserts
		if (constants.deferred)
		{
			/* bulk insert replace should be disallowed for
			 * deferred mode inserts.
			 */
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(! bulkInsertReplace,
					"bulkInsertReplace expected to be false for deferred mode inserts");
			}
			return false;
		}

		return getExclusiveTableLock();
	}

	/**
	 * Get an exclusive table lock on the target table
	 * (and check to see if the table is populated if
	 * this is not a bulk insert replace).
	 *
	 * @return Whether or not bulkInsert is allowed on this table.
	 *
	 * @exception StandardException		Thrown on error
	 */
	private boolean getExclusiveTableLock()
		throws StandardException
	{
		boolean rowFound = false;

		bulkHeapSC = tc.openCompiledScan(
							false,
							TransactionController.OPENMODE_FORUPDATE,
							TransactionController.MODE_TABLE,
                            TransactionController.ISOLATION_SERIALIZABLE,
							(FormatableBitSet) null,
							(DataValueDescriptor[]) null,
							0,
							(Qualifier[][]) null,
							(DataValueDescriptor[]) null,
							0,
							constants.heapSCOCI,
							heapDCOCI);

		/* No need to do next if bulk insert replace
		 * but we do need to get a row location for the
		 * case where the replace leaves an empty table.
		 */
		if (! bulkInsertReplace)
		{
			rowFound = bulkHeapSC.next();
		}
		else
		{
			rl = bulkHeapSC.newRowLocationTemplate();
		}

		bulkHeapSC.close();
		bulkHeapSC = null;

		return ! rowFound;
	}

	/**
	 * Set the estimated row count for this table.
	 *
	 * @param heapConglom	Conglomerate number for the heap
	 *
	 * @exception StandardException		Thrown on failure
	 */
	private void setEstimatedRowCount(long heapConglom)
		throws StandardException
	{
		bulkHeapSC = tc.openCompiledScan(
							false,
							TransactionController.OPENMODE_FORUPDATE,
							TransactionController.MODE_TABLE,
                            TransactionController.ISOLATION_SERIALIZABLE,
							(FormatableBitSet) null,
							(DataValueDescriptor[]) null,
							0,
							(Qualifier[][]) null,
							(DataValueDescriptor[]) null,
							0,
							constants.heapSCOCI,
							heapDCOCI);
		
		bulkHeapSC.setEstimatedRowCount(rowCount);

		bulkHeapSC.close();
		bulkHeapSC = null;
	}

	/**
	 * Empty the indexes after doing a bulk insert replace
	 * where the table has 0 rows after the replace.
	 * RESOLVE: This method is ugly!  Prior to 2.0, we simply
	 * scanned back across the table to build the indexes.  We
	 * changed this in 2.0 to populate the sorters via a call back
	 * as we populated the table.  Doing a 0 row replace into a
	 * table with indexes is a degenerate case, hence we allow
	 * ugly and unoptimized code.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	private void emptyIndexes(long newHeapConglom, 
							  InsertConstantAction constants, 
							  TableDescriptor td,
							  DataDictionary dd,
							  ExecRow fullTemplate)
		throws StandardException
	{
		int					numIndexes = constants.irgs.length;
//IC see: https://issues.apache.org/jira/browse/DERBY-532
        ExecIndexRow[]      idxRows = new ExecIndexRow[numIndexes];
        ExecRow             baseRows;
        ColumnOrdering[][]  order = new ColumnOrdering[numIndexes][];
		int					numColumns = td.getNumberOfColumns();
        collation       = new int[numIndexes][];

		// Create the BitSet for mapping the partial row to the full row
		FormatableBitSet bitSet = new FormatableBitSet(numColumns + 1);
		// Need to check each index for referenced columns
		int numReferencedColumns = 0;
		for (int index = 0; index < numIndexes; index++)
		{
			int[] baseColumnPositions = constants.irgs[index].baseColumnPositions();
			for (int bcp = 0; bcp < baseColumnPositions.length; bcp++)
			{
				if (! bitSet.get(baseColumnPositions[bcp]))
				{
					bitSet.set(baseColumnPositions[bcp] );
					numReferencedColumns++;
				}
			}
		}

		// We can finally create the partial base row
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
		baseRows = 
            activation.getExecutionFactory().getValueRow(numReferencedColumns);

		// Fill in each base row with nulls of the correct data type
		int colNumber = 0;
		for (int index = 0; index < numColumns; index++)
		{
			if (bitSet.get(index + 1))
			{
				colNumber++;
				// NOTE: 1-based column numbers
				baseRows.setColumn(
						colNumber,
//IC see: https://issues.apache.org/jira/browse/DERBY-4520
						fullTemplate.getColumn(index + 1).cloneValue(false));
			}
		}

		needToDropSort = new boolean[numIndexes];
		sortIds = new long[numIndexes];

		/* Do the initial set up before scanning the heap.
		 * For each index, build a single index row and a sorter.
		 */
		for (int index = 0; index < numIndexes; index++)
		{
			// create a single index row template for each index
            idxRows[index] = constants.irgs[index].getIndexRowTemplate();
//IC see: https://issues.apache.org/jira/browse/DERBY-532

			// Get an index row based on the base row
			// (This call is only necessary here because we need to pass a 
            // template to the sorter.)
			constants.irgs[index].getIndexRow(baseRows, 
											  rl, 
                                              idxRows[index],
											  bitSet);

			/* For non-unique indexes, we order by all columns + the RID.
			 * For unique indexes, we just order by the columns.
			 * We create a unique index observer for unique indexes
			 * so that we can catch duplicate key
			 */
			ConglomerateDescriptor cd;
			// Get the ConglomerateDescriptor for the index
			cd = td.getConglomerateDescriptor(constants.indexCIDS[index]);
			int[] baseColumnPositions = constants.irgs[index].baseColumnPositions();
			boolean[] isAscending = constants.irgs[index].isAscending();
			int numColumnOrderings;
//IC see: https://issues.apache.org/jira/browse/DERBY-532
            SortObserver sortObserver;
            final IndexRowGenerator indDes = cd.getIndexDescriptor();

            if (indDes.isUnique() || indDes.isUniqueDeferrable())
			{
                numColumnOrderings =
                        indDes.isUnique() ? baseColumnPositions.length :
                        baseColumnPositions.length + 1;

				String indexOrConstraintName = cd.getConglomerateName();
                boolean deferred = false;
                boolean uniqueDeferrable = false;

//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
                UUID uniqueDeferrableConstraintId = null;
				if (cd.isConstraint()) 
				{
                    // so, the index is backing up a constraint
					ConstraintDescriptor conDesc = 
                        dd.getConstraintDescriptor(td, cd.getUUID());
					indexOrConstraintName = conDesc.getConstraintName();
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                    deferred = lcc.isEffectivelyDeferred(
                            lcc.getCurrentSQLSessionContext(activation),
//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
                            conDesc.getUUID());
                    uniqueDeferrable = conDesc.deferrable();
                    uniqueDeferrableConstraintId = conDesc.getUUID();
				}
				sortObserver = 
                    new UniqueIndexSortObserver(
                            lcc,
                            uniqueDeferrableConstraintId,
                            false, // don't clone rows
                            uniqueDeferrable,
                            deferred,
                            indexOrConstraintName,
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                            idxRows[index],
                            true,
                            td.getName());
			}
			else
			{
				numColumnOrderings = baseColumnPositions.length + 1;
				sortObserver       = new BasicSortObserver(false, false, 
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                                                     idxRows[index],
													 true);
			}
            order[index] = new ColumnOrdering[numColumnOrderings];
			for (int ii =0; ii < isAscending.length; ii++) 
			{
                order[index][ii] = new IndexColumnOrder(ii, isAscending[ii]);
			}
			if (numColumnOrderings > isAscending.length)
            {
                order[index][isAscending.length] =
                    new IndexColumnOrder(isAscending.length);
            }

			// create the sorters
			sortIds[index] = 
                tc.createSort(
                    (Properties)null, 
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                    idxRows[index].getRowArrayClone(),
                    order[index],
                    sortObserver,
                    false,			// not in order
                    rowCount,		// est rows	
                    -1				// est row size, -1 means no idea	
                    );

			needToDropSort[index] = true;
		}

		// Populate sorters and get the output of each sorter into a row
		// source.  The sorters have the indexed columns only and the columns
		// are in the correct order. 
		rowSources = new RowLocationRetRowSource[numIndexes];
		// Fill in the RowSources
//IC see: https://issues.apache.org/jira/browse/DERBY-532
        SortController[]    sorter = new SortController[numIndexes];
		for (int index = 0; index < numIndexes; index++)
		{
            sorter[index] = tc.openSort(sortIds[index]);
            sorter[index].completedInserts();
			rowSources[index] = tc.openSortRowSource(sortIds[index]);
		}

		long[] newIndexCongloms = new long[numIndexes];

		// Populate each index
		for (int index = 0; index < numIndexes; index++)
		{
			ConglomerateController indexCC;
			Properties properties = new Properties();
			ConglomerateDescriptor cd;
			// Get the ConglomerateDescriptor for the index
			cd = td.getConglomerateDescriptor(constants.indexCIDS[index]);

			
			// Build the properties list for the new conglomerate
			indexCC = tc.openCompiledConglomerate(
                                false,
                                TransactionController.OPENMODE_FORUPDATE,
                                TransactionController.MODE_TABLE,
                                TransactionController.ISOLATION_SERIALIZABLE,
								constants.indexSCOCIs[index],
								indexDCOCIs[index]);

			// Get the properties on the old index
			indexCC.getInternalTablePropertySet(properties);

			/* Create the properties that language supplies when creating the
			 * the index.  (The store doesn't preserve these.)
			 */
            int indexRowLength = idxRows[index].nColumns();
			properties.put("baseConglomerateId", Long.toString(newHeapConglom));
			if (cd.getIndexDescriptor().isUnique())
			{
				properties.put("nUniqueColumns", 
							   Integer.toString(indexRowLength - 1));
			}
			else
			{
				properties.put("nUniqueColumns", 
							   Integer.toString(indexRowLength));
			}
//IC see: https://issues.apache.org/jira/browse/DERBY-532
            if( cd.getIndexDescriptor().isUniqueWithDuplicateNulls() &&
               !cd.getIndexDescriptor().hasDeferrableChecking() )
			{
				properties.put(
                    "uniqueWithDuplicateNulls", Boolean.toString(true));
			}

            properties.put("rowLocationColumn",
							Integer.toString(indexRowLength - 1));
			properties.put("nKeyFields", Integer.toString(indexRowLength));

			indexCC.close();
			
            collation[index] = 
                constants.irgs[index].getColumnCollationIds(
                    td.getColumnDescriptorList());

			// We can finally drain the sorter and rebuild the index
			// Populate the index.
			newIndexCongloms[index] = 
                tc.createAndLoadConglomerate(
                    "BTREE",
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                    idxRows[index].getRowArray(),
                    null, //default column sort order 
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
                    collation[index],
                    properties,
                    TransactionController.IS_DEFAULT,
                    rowSources[index],
                    (long[]) null);

			/* Update the DataDictionary
			 *
			 * Update sys.sysconglomerates with new conglomerate #, if the
			 * conglomerate is shared by duplicate indexes, all the descriptors
			 * for those indexes need to be updated with the new number.
			 */
			dd.updateConglomerateDescriptor(
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
                td.getConglomerateDescriptors(constants.indexCIDS[index]),
                newIndexCongloms[index], tc);

			// Drop the old conglomerate
			tc.dropConglomerate(constants.indexCIDS[index]);
		}
	}

	/**
	 * Get me a table scan result set, preferably a bulk
	 * table scan, thank you.  If we already have one, reopen it.
	 */
	private BulkTableScanResultSet getTableScanResultSet
	(
		long	conglomId
	) throws StandardException
	{
		if (tableScan == null)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
			tableScan = 
                new BulkTableScanResultSet(
                    conglomId,
                    tc.getStaticCompiledConglomInfo(conglomId),
                    activation,
//IC see: https://issues.apache.org/jira/browse/DERBY-6003
                    fullTemplateId,
                    0,						// result set number
                    (GeneratedMethod)null, 	// start key getter
                    0, 						// start search operator
                    (GeneratedMethod)null,	// stop key getter
                    0, 						// start search operator
                    false,
                    (Qualifier[][])null,	// qualifiers
                    "tableName",
//IC see: https://issues.apache.org/jira/browse/DERBY-573
                    (String)null,
                    (String)null,			// index name
                    false,					// is constraint
                    false,					// for update
                    -1,						// saved object for referenced bitImpl
                    -1,
//IC see: https://issues.apache.org/jira/browse/DERBY-6063
                    TransactionController.MODE_TABLE,
                    true,					// table locked
                    TransactionController.ISOLATION_READ_COMMITTED,
                    LanguageProperties.BULK_FETCH_DEFAULT_INT,	// rows per read
                    false,                  // never disable bulk fetch
                    false,					// not a 1 row per scan
                    0d,						// estimated rows
//IC see: https://issues.apache.org/jira/browse/DERBY-1700
                    0d 					// estimated cost
                    );
			tableScan.openCore();
		}
		else
		{	
			tableScan.reopenCore();
		}
		return tableScan;
	}

	private String[] getColumnNames(int[] baseColumnPositions)
	{
		int length = baseColumnPositions.length;
		String[] columnNames = new String[length];
		for(int i = 0; i < length; i++)
		{
			columnNames[i] = constants.getColumnName(i);
		}
		return columnNames;
	}

    @Override
	public void finish() throws StandardException {
		sourceResultSet.finish();
		super.finish();
	}

    @Override
    public void rememberConstraint(UUID cid) throws StandardException {
//IC see: https://issues.apache.org/jira/browse/DERBY-532
        if (violatingCheckConstraints == null) {
            violatingCheckConstraints = new ArrayList<UUID>();
        }

        if (!violatingCheckConstraints.contains(cid)) {
            violatingCheckConstraints.add(cid);
        }
    }
    
    /*
     * The implementation of this method is slightly different than the one
     *  in UpdateResultSet. This code was originally written for insert but
     *  with DERBY-6414, we have started supporting update of auto generated
     *  column with keyword DEFAULT. The reason of different implementation is
     *  that the array used in the following method, namely, 
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
    	if ((rla = constants.getAutoincRowLocation()) != null)
    	{
    		aiCache = new DataValueDescriptor[ rla.length ];
    		bulkInsertCounters = new BulkInsertCounter[ rla.length ];
    		for (int i = 0; i < resultDescription.getColumnCount(); i++)
    		{
    			if (rla[i] == null)
    				continue;
    			ResultColumnDescriptor rcd = 
    				resultDescription.getColumnDescriptor(i + 1);
    			aiCache[i] = rcd.getType().getNull();
    		}
    	}
    }
}
