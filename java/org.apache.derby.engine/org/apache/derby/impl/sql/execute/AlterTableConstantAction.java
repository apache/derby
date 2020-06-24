/*

   Derby - Class org.apache.derby.impl.sql.execute.AlterTableConstantAction

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
import java.util.List;
import java.util.Properties;
import org.apache.derby.catalog.Dependable;
import org.apache.derby.catalog.DependableFinder;
import org.apache.derby.catalog.IndexDescriptor;
import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.types.ReferencedColumnsDescriptorImpl;
import org.apache.derby.catalog.types.StatisticsImpl;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.StreamStorable;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.PreparedStatement;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.StatementType;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.Parser;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.dictionary.CheckConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptorList;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptorList;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DefaultDescriptor;
import org.apache.derby.iapi.sql.dictionary.DependencyDescriptor;
import org.apache.derby.iapi.sql.dictionary.ForeignKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.IndexLister;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.sql.dictionary.ReferencedKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.SPSDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.SequenceDescriptor;
import org.apache.derby.iapi.sql.dictionary.StatisticsDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.GroupFetchScanController;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowLocationRetRowSource;
import org.apache.derby.iapi.store.access.RowSource;
import org.apache.derby.iapi.store.access.RowUtil;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.SortController;
import org.apache.derby.iapi.store.access.SortObserver;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.impl.sql.compile.ColumnDefinitionNode;
import org.apache.derby.impl.sql.compile.StatementNode;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 *	This class  describes actions that are ALWAYS performed for an
 *	ALTER TABLE Statement at Execution time.
 *
 */

class AlterTableConstantAction extends DDLSingleTableConstantAction
 implements RowLocationRetRowSource
{
    private static final int RANGE_TOP = 0;
    private static final int RANGE_BOTTOM = 1;

    // copied from constructor args and stored locally.
    private	    SchemaDescriptor			sd;
    private	    String						tableName;
    private	    UUID						schemaId;
    private	    int							tableType;
    private	    ColumnInfo[]				columnInfo;
    private	    ConstraintConstantAction[]	constraintActions;
    private	    char						lockGranularity;
    private	    long						tableConglomerateId;
    private	    boolean					    compressTable;
    private     int						    behavior;
    private	    boolean					    sequential;
    private     boolean                     truncateTable;
	//The following three (purge, defragment and truncateEndOfTable) apply for 
	//inplace compress
    private	    boolean					    purge;
    private	    boolean					    defragment;
    private	    boolean					    truncateEndOfTable;

	/**
	 * updateStatistics will indicate that we are here for updating the
	 * statistics. It could be statistics of just one index or all the
	 * indexes on a given table. 
	 */
    private	    boolean					    updateStatistics;
	/**
	 * The flag updateStatisticsAll will tell if we are going to update the 
	 * statistics of all indexes or just one index on a table. 
	 */
    private	    boolean					    updateStatisticsAll;
	/**
	 * dropStatistics will indicate that we are here for dropping the
	 * statistics. It could be statistics of just one index or all the
	 * indexes on a given table. 
	 */
    private	    boolean					    dropStatistics;
	/**
	 * The flag dropStatisticsAll will tell if we are going to drop the 
	 * statistics of all indexes or just one index on a table. 
	 */
    private	    boolean					    dropStatisticsAll;
	/**
	 * If statistic is getting updated/dropped for just one index, then 
	 * indexNameForStatistics will tell the name of the specific index 
	 * whose statistics need to be updated/dropped.
	 */
    private	    String						indexNameForStatistics;

    
    // Alter table compress and Drop column
    private     boolean					    doneScan;
    private     boolean[]				    needToDropSort;
    private     boolean[]				    validRow;
    private	    int						    bulkFetchSize = 16;
    private	    int						    currentCompressRow;
    private     int						    numIndexes;
    private     int						    rowCount;
    private     long					    estimatedRowCount;
    private     long[]					    indexConglomerateNumbers;
    private	    long[]					    sortIds;
    private     FormatableBitSet			indexedCols;
    private     ConglomerateController	    compressHeapCC;
    private     ExecIndexRow[]			    indexRows;
    private     ExecRow[]				    baseRow;
    private     ExecRow					    currentRow;
    private	    GroupFetchScanController    compressHeapGSC;
    private     IndexRowGenerator[]		    compressIRGs;
    private	    DataValueDescriptor[][]		baseRowArray;
    private     RowLocation[]			    compressRL;
    private     SortController[]		    sorters;
    private     int						    droppedColumnPosition;
    private     ColumnOrdering[][]		    ordering;
    private     int[][]		                collation;

    private	TableDescriptor 		        td;



    // CONSTRUCTORS
    private LanguageConnectionContext lcc;
    private DataDictionary dd;
    private DependencyManager dm;
    private TransactionController tc;
    private Activation activation;

	/**
	 *	Make the AlterAction for an ALTER TABLE statement.
	 *
	 *  @param sd			        descriptor for the table's schema.
	 *  @param tableName	        Name of table.
	 *	@param tableId		        UUID of table
	 *	@param tableConglomerateId	heap conglomerate number of table
	 *  @param tableType	        Type of table (e.g., BASE).
	 *  @param columnInfo	        Information on all the columns in the table.
	 *  @param constraintActions	ConstraintConstantAction[] for constraints
	 *  @param lockGranularity	    The lock granularity.
	 *	@param compressTable	    Whether or not this is a compress table
	 *	@param behavior		        drop behavior for dropping column
	 *	@param sequential	        If compress table/drop column, 
     *	                            whether or not sequential
	 *  @param truncateTable	    Whether or not this is a truncate table
	 *  @param purge				PURGE during INPLACE COMPRESS?
	 *  @param defragment			DEFRAGMENT during INPLACE COMPRESS?
	 *  @param truncateEndOfTable	TRUNCATE END during INPLACE COMPRESS?
	 *  @param updateStatistics		TRUE means we are here to update statistics
	 *  @param updateStatisticsAll	TRUE means we are here to update statistics
	 *  	of all the indexes. False means we are here to update statistics of
	 *  	only one index.
	 *  @param dropStatistics		TRUE means we are here to drop statistics
	 *  @param dropStatisticsAll	TRUE means we are here to drop statistics
	 *  	of all the indexes. False means we are here to drop statistics of
	 *  	only one index.
	 *  @param indexNameForStatistics	Will name the index whose statistics
	 *  	will be updated/dropped. This param is looked at only if 
	 *  	updateStatisticsAll/dropStatisticsAll is set to false and
	 *  	updateStatistics/dropStatistics is set to true.
	 */
	AlterTableConstantAction(
    SchemaDescriptor            sd,
    String			            tableName,
    UUID			            tableId,
    long			            tableConglomerateId,
    int				            tableType,
    ColumnInfo[]	            columnInfo,
    ConstraintConstantAction[]  constraintActions,
    char			            lockGranularity,
    boolean			            compressTable,
    int				            behavior,
    boolean			            sequential,
    boolean                     truncateTable,
    boolean                     purge,
    boolean                     defragment,
    boolean                     truncateEndOfTable,
    boolean                     updateStatistics,
    boolean                     updateStatisticsAll,
    boolean                     dropStatistics,
    boolean                     dropStatisticsAll,
    String                      indexNameForStatistics)
	{
		super(tableId);
		this.sd                     = sd;
		this.tableName              = tableName;
		this.tableConglomerateId    = tableConglomerateId;
		this.tableType              = tableType;
		this.columnInfo             = columnInfo;
		this.constraintActions      = constraintActions;
		this.lockGranularity        = lockGranularity;
		this.compressTable          = compressTable;
		this.behavior               = behavior;
		this.sequential             = sequential;
		this.truncateTable          = truncateTable;
		this.purge          		= purge;
		this.defragment          	= defragment;
		this.truncateEndOfTable     = truncateEndOfTable;
		this.updateStatistics     	= updateStatistics;
		this.updateStatisticsAll    = updateStatisticsAll;
		this.dropStatistics     	= dropStatistics;
		this.dropStatisticsAll    = dropStatisticsAll;
		this.indexNameForStatistics = indexNameForStatistics;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(sd != null, "schema descriptor is null");
		}
	}

	// OBJECT METHODS

	public	String	toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.

		// we don't bother trying to print out the
		// schema because we don't have it until execution
		if(truncateTable)
			return "TRUNCATE TABLE " + tableName;
		else
			return "ALTER TABLE " + tableName;
	}

	// INTERFACE METHODS

    /**
     * Run this constant action.
     *
     * @param activation the activation in which to run the action
     * @throws StandardException if an error happens during execution
     * of the action
     */
    public void executeConstantAction(Activation activation)
            throws StandardException {
        try {
            executeConstantActionBody(activation);
        } finally {
            clearState();
        }
    }

	/**
	 *	This is the guts of the Execution-time logic for ALTER TABLE.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
    private void executeConstantActionBody(Activation activation)
//IC see: https://issues.apache.org/jira/browse/DERBY-4899
            throws StandardException {
        // Save references to the main structures we need.
        this.activation = activation;
        lcc = activation.getLanguageConnectionContext();
        dd = lcc.getDataDictionary();
        dm = dd.getDependencyManager();
        tc = lcc.getTransactionExecute();

		int							numRows = 0;
        boolean						tableScanned = false;

        if (compressTable || truncateTable) {
//IC see: https://issues.apache.org/jira/browse/DERBY-532
            DeferredConstraintsMemory.compressOrTruncate(
                    lcc, tableId, tableName);
        }

        //Following if is for inplace compress. Compress using temporary
        //tables to do the compression is done later in this method.
		if (compressTable)
		{
			if (purge || defragment || truncateEndOfTable)
			{
				td = dd.getTableDescriptor(tableId);
				if (td == null)
				{
					throw StandardException.newException(
						SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, tableName);
				}
	            // Each of the following may give up locks allowing ddl on the
	            // table, so each phase needs to do the data dictionary lookup.
	            // The order is important as it makes sense to first purge
	            // deleted rows, then defragment existing non-deleted rows, and
	            // finally to truncate the end of the file which may have been
	            // made larger by the previous purge/defragment pass.
	            if (purge)
	                purgeRows(tc);

	            if (defragment)
                    defragmentRows(tc);
//IC see: https://issues.apache.org/jira/browse/DERBY-4899

	            if (truncateEndOfTable)
	                truncateEnd(tc);            
	            return;				
			}
		}

//IC see: https://issues.apache.org/jira/browse/DERBY-4899
        if (updateStatistics) {
            updateStatistics();
            return;
		}

        if (dropStatistics) {
            dropStatistics();
            return;
		}
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

		// now do the real work

		// get an exclusive lock of the heap, to avoid deadlock on rows of
		// SYSCOLUMNS etc datadictionary tables and phantom table
		// descriptor, in which case table shape could be changed by a
		// concurrent thread doing add/drop column.

		// older version (or at target) has to get td first, potential deadlock
		if (tableConglomerateId == 0)
		{
			td = dd.getTableDescriptor(tableId);
			if (td == null)
			{
				throw StandardException.newException(
					SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, tableName);
			}
			tableConglomerateId = td.getHeapConglomerateId();
		}

		lockTableForDDL(tc, tableConglomerateId, true);

		td = dd.getTableDescriptor(tableId);
		if (td == null)
		{
			throw StandardException.newException(
				SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, tableName);
		}

//IC see: https://issues.apache.org/jira/browse/DERBY-2537
		if (truncateTable)
			dm.invalidateFor(td, DependencyManager.TRUNCATE_TABLE, lcc);
		else
			dm.invalidateFor(td, DependencyManager.ALTER_TABLE, lcc);

		// Save the TableDescriptor off in the Activation
		activation.setDDLTableDescriptor(td);

		/*
		** If the schema descriptor is null, then we must have just read 
        ** ourselves in.  So we will get the corresponding schema descriptor 
        ** from the data dictionary.
		*/
		if (sd == null)
		{
			sd = getAndCheckSchemaDescriptor(dd, schemaId, "ALTER TABLE");
		}
		
		/* Prepare all dependents to invalidate.  (This is their chance
		 * to say that they can't be invalidated.  For example, an open
		 * cursor referencing a table/view that the user is attempting to
		 * alter.) If no one objects, then invalidate any dependent objects.
		 */
		if(truncateTable)
			dm.invalidateFor(td, DependencyManager.TRUNCATE_TABLE, lcc);
		else
			dm.invalidateFor(td, DependencyManager.ALTER_TABLE, lcc);


		// Are we working on columns?
		if (columnInfo != null)
		{
            boolean tableNeedsScanning = false;

			/* NOTE: We only allow a single column to be added within
			 * each ALTER TABLE command at the language level.  However,
			 * this may change some day, so we will try to plan for it.
			 */
			/* for each new column, see if the user is adding a non-nullable
			 * column.  This is only allowed on an empty table.
			 */
			for (int ix = 0; ix < columnInfo.length; ix++)
			{

				/* Is this new column non-nullable?  
				 * If so, it can only be added to an
				 * empty table if it does not have a default value.	
				 * We need to scan the table to find out how many rows 
				 * there are.
				 */
				if ((columnInfo[ix].action == ColumnInfo.CREATE) &&
					!(columnInfo[ix].dataType.isNullable()) &&
					(columnInfo[ix].defaultInfo == null) &&
					(columnInfo[ix].autoincInc == 0)
					)
				{
					tableNeedsScanning = true;
				}
			}

			// Scan the table if necessary
			if (tableNeedsScanning)
			{
				numRows = getSemiRowCount(tc);
				// Don't allow add of non-nullable column to non-empty table
				if (numRows > 0)
				{
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
					throw StandardException.newException(
                        SQLState.LANG_ADDING_NON_NULL_COLUMN_TO_NON_EMPTY_TABLE,
                        td.getQualifiedName());
				}
				tableScanned = true;
			}

			// for each related column, stuff system.column
			for (int ix = 0; ix < columnInfo.length; ix++)
			{
				/* If there is a default value, use it, otherwise use null */
				
				// Are we adding a new column or modifying a default?
				
				if (columnInfo[ix].action == ColumnInfo.CREATE)
				{
//IC see: https://issues.apache.org/jira/browse/DERBY-4899
					addNewColumnToTable(ix);
				}
				else if (columnInfo[ix].action == 
						 ColumnInfo.MODIFY_COLUMN_DEFAULT_RESTART ||
//IC see: https://issues.apache.org/jira/browse/DERBY-2371
						 columnInfo[ix].action == 
						 ColumnInfo.MODIFY_COLUMN_DEFAULT_INCREMENT ||
//IC see: https://issues.apache.org/jira/browse/DERBY-6903
//IC see: https://issues.apache.org/jira/browse/DERBY-6904
//IC see: https://issues.apache.org/jira/browse/DERBY-6905
//IC see: https://issues.apache.org/jira/browse/DERBY-6906
//IC see: https://issues.apache.org/jira/browse/DERBY-534
						 columnInfo[ix].action ==
						 ColumnInfo.MODIFY_COLUMN_DEFAULT_CYCLE ||
						 columnInfo[ix].action == 
						 ColumnInfo.MODIFY_COLUMN_DEFAULT_VALUE)
				{
//IC see: https://issues.apache.org/jira/browse/DERBY-4899
                    modifyColumnDefault(ix);
				}
				else if (columnInfo[ix].action == 
						 ColumnInfo.MODIFY_COLUMN_TYPE)
				{
                    modifyColumnType(ix);
				}
				else if (columnInfo[ix].action == 
						 ColumnInfo.MODIFY_COLUMN_CONSTRAINT)
				{
                    modifyColumnConstraint(columnInfo[ix].name, true);
				}
				else if (columnInfo[ix].action == 
						 ColumnInfo.MODIFY_COLUMN_CONSTRAINT_NOT_NULL)
				{
					if (!tableScanned)
					{
						tableScanned = true;
						numRows = getSemiRowCount(tc);
					}

					// check that the data in the column is not null
					String colNames[]  = new String[1];
					colNames[0]        = columnInfo[ix].name;
					boolean nullCols[] = new boolean[1];

					/* note validateNotNullConstraint returns true if the
					 * column is nullable
					 */
					if (validateNotNullConstraint(
                            colNames, nullCols, numRows, lcc, 
                            SQLState.LANG_NULL_DATA_IN_NON_NULL_COLUMN))
					{
						/* nullable column - modify it to be not null
						 * This is O.K. at this point since we would have
						 * thrown an exception if any data was null
						 */
//IC see: https://issues.apache.org/jira/browse/DERBY-4899
                        modifyColumnConstraint(columnInfo[ix].name, false);
					}
				}
				else if (columnInfo[ix].action == ColumnInfo.DROP)
				{
                    dropColumnFromTable(columnInfo[ix].name);
				}
				else if (
                         (columnInfo[ix].action == ColumnInfo.MODIFY_COLUMN_GENERATED_ALWAYS) ||
                         (columnInfo[ix].action == ColumnInfo.MODIFY_COLUMN_GENERATED_BY_DEFAULT)
                         )
				{
                    modifyIdentityState(ix);
				}
				else if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT(
							  "Unexpected action in AlterTableConstantAction");
				}
			}
		}

        // adjust dependencies on user defined types
        adjustUDTDependencies( lcc, dd, td, columnInfo, false );

        /* Create/Drop/alter any constraints */
		if (constraintActions != null)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
			for (int conIndex = 0; 
                 conIndex < constraintActions.length; 
                 conIndex++)
			{
				ConstraintConstantAction cca = constraintActions[conIndex];
                boolean isCheckInitiallyDeferred = false;

				if (cca instanceof CreateConstraintConstantAction)
				{
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                    final CreateConstraintConstantAction ccca =
                            (CreateConstraintConstantAction)cca;

                    int constraintType = ccca.getConstraintType();

                    isCheckInitiallyDeferred =
                        (constraintType == DataDictionary.CHECK_CONSTRAINT) &&
                        ccca.isInitiallyDeferred();

					/* Some constraint types require special checking:
					 *   Check		 - table must be empty, for now
					 *   Primary Key - table cannot already have a primary key
					 */
					switch (constraintType)
					{
						case DataDictionary.PRIMARYKEY_CONSTRAINT:

							// Check to see if a constraint of the same type 
                            // already exists
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
							ConstraintDescriptorList cdl = 
                                dd.getConstraintDescriptors(td);

							if (cdl.getPrimaryKey() != null)
							{
								throw StandardException.newException(
                                    SQLState.LANG_ADD_PRIMARY_KEY_FAILED1, 
                                    td.getQualifiedName());
							}

							if (!tableScanned)
							{
								tableScanned = true;
								numRows = getSemiRowCount(tc);
							}

							break;

						case DataDictionary.CHECK_CONSTRAINT:

							if (!tableScanned)
							{
								tableScanned = true;
								numRows = getSemiRowCount(tc);
							}

//IC see: https://issues.apache.org/jira/browse/DERBY-532
                            if (isCheckInitiallyDeferred) {
                                // Need to do this early to get UUID
                                // assigned
                                constraintActions[conIndex].
                                        executeConstantAction(activation);
                            }

							if (numRows > 0)
							{
								/*
								** We are assuming that there will only be one 
								** check constraint that we are adding, so it
								** is ok to do the check now rather than try
								** to lump together several checks.	
								*/

								ConstraintConstantAction.validateConstraint(
                                    cca.getConstraintName(),
                                    ((CreateConstraintConstantAction)cca).getConstraintText(),
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                                    cca.getConstraintId(),
                                    td,
                                    lcc,
                                    true,
                                    isCheckInitiallyDeferred);
							}
							break;
					}
				}
				else
				{
					if (SanityManager.DEBUG)
					{
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                        if (!(cca instanceof DropConstraintConstantAction ||
                              cca instanceof AlterConstraintConstantAction))
						{
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
							SanityManager.THROWASSERT(
                                "constraintActions[" + conIndex + 
                                "] expected to be instanceof " + 
                                "DropConstraintConstantAction not " +
                                cca.getClass().getName());
						}
					}
				}

//IC see: https://issues.apache.org/jira/browse/DERBY-532
                if (!isCheckInitiallyDeferred) {
                    constraintActions[conIndex].
                        executeConstantAction(activation);
                } // else it is done early, see above.
			}
		}

		// Are we changing the lock granularity?
		if (lockGranularity != '\0')
		{
			if (SanityManager.DEBUG)
			{
				if (lockGranularity != 'T' &&
					lockGranularity != 'R')
				{
					SanityManager.THROWASSERT(
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
						"lockGranularity expected to be 'T'or 'R', not " + 
                        lockGranularity);
				}
			}

			// update the TableDescriptor
			td.setLockGranularity(lockGranularity);
			// update the DataDictionary
			dd.updateLockGranularity(td, sd, lockGranularity, tc);
		}

		// Are we doing a compress table?
		if (compressTable)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-4899
            compressTable();
		}

		// Are we doing a truncate table?
		if (truncateTable)
		{
            truncateTable();
		}
	}

    /**
     * Clear the state of this constant action.
     */
    private void clearState() {
        // DERBY-3009: executeConstantAction() stores some of its state in
        // instance variables instead of local variables for convenience.
        // These variables should be cleared after the execution of the
        // constant action has completed, so that the objects they reference
        // can be garbage collected.
        td = null;
        lcc = null;
        dd = null;
        dm = null;
        tc = null;
        activation = null;
    }

	/**
	 * Drop statistics of either all the indexes on the table or only one
	 * specific index depending on what user has requested.
	 * 
	 * @throws StandardException
	 */
    private void dropStatistics()
            throws StandardException {
        td = dd.getTableDescriptor(tableId);

        dd.startWriting(lcc);
        dm.invalidateFor(td, DependencyManager.UPDATE_STATISTICS, lcc);

        if (dropStatisticsAll) {
            dd.dropStatisticsDescriptors(td.getUUID(), null, tc);
        } else {
            ConglomerateDescriptor cd = 
                dd.getConglomerateDescriptor(
                    indexNameForStatistics, sd, false);
            dd.dropStatisticsDescriptors(td.getUUID(), cd.getUUID(), tc);
        }
    }

	/**
	 * Update statistics of either all the indexes on the table or only one
	 * specific index depending on what user has requested.
	 * 
	 * @throws StandardException
	 */
    private void updateStatistics()
//IC see: https://issues.apache.org/jira/browse/DERBY-4899
            throws StandardException {
//IC see: https://issues.apache.org/jira/browse/DERBY-4937
        ConglomerateDescriptor[] cds;
        td = dd.getTableDescriptor(tableId);

        if (updateStatisticsAll) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5680
            cds = null;
        } else {
            cds = new ConglomerateDescriptor[1];
            cds[0] = dd.getConglomerateDescriptor(
                    indexNameForStatistics, sd, false);
        }
        dd.getIndexStatsRefresher(false).runExplicitly(
                                                lcc, td, cds, "ALTER TABLE");
    }

    /**
     * Truncate end of conglomerate.
     * <p>
     * Returns the contiguous free space at the end of the table back to
     * the operating system.  Takes care of space allocation bit maps, and
     * OS call to return the actual space.
     * <p>
     *
     * @param tc                transaction controller to use to do updates.
     *
     **/
	private void truncateEnd(
    TransactionController   tc)
        throws StandardException
	{
        switch (td.getTableType())
        {
        /* Skip views and vti tables */
        case TableDescriptor.VIEW_TYPE:
        case TableDescriptor.VTI_TYPE:
        	break;
        // other types give various errors here
        // DERBY-719,DERBY-720
        default:
          {
          ConglomerateDescriptor[] conglom_descriptors = 
                td.getConglomerateDescriptors();

            for (int cd_idx = 0; cd_idx < conglom_descriptors.length; cd_idx++)
            {
                ConglomerateDescriptor cd = conglom_descriptors[cd_idx];

                tc.compressConglomerate(cd.getConglomerateNumber());
            }
          }
        }
    }

    /**
     * Defragment rows in the given table.
     * <p>
     * Scans the rows at the end of a table and moves them to free spots
     * towards the beginning of the table.  In the same transaction all
     * associated indexes are updated to reflect the new location of the
     * base table row.
     * <p>
     * After a defragment pass, if was possible, there will be a set of
     * empty pages at the end of the table which can be returned to the
     * operating system by calling truncateEnd().  The allocation bit
     * maps will be set so that new inserts will tend to go to empty and
     * half filled pages starting from the front of the conglomerate.
     *
     * @param tc                transaction controller to use to do updates.
     *
     **/
    private void defragmentRows(TransactionController tc)
//IC see: https://issues.apache.org/jira/browse/DERBY-4899
            throws StandardException {
        GroupFetchScanController base_group_fetch_cc = null;
        int                      num_indexes         = 0;

        int[][]                  index_col_map       =  null;
        ScanController[]         index_scan          =  null;
        ConglomerateController[] index_cc            =  null;
        DataValueDescriptor[][]  index_row           =  null;

		TransactionController     nested_tc = null;

		try {

            nested_tc = 
                tc.startNestedUserTransaction(false, true);

            switch (td.getTableType())
            {
            /* Skip views and vti tables */
            case TableDescriptor.VIEW_TYPE:
            case TableDescriptor.VTI_TYPE:
            	return;
            // other types give various errors here
            // DERBY-719,DERBY-720
            default:
            	break;
            }

			/* Get a row template for the base table */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
            ExecRow br =
                lcc.getLanguageConnectionFactory().getExecutionFactory().getValueRow(
                    td.getNumberOfColumns());

			/* Fill the row with nulls of the correct type */
            for (ColumnDescriptor cd : td.getColumnDescriptorList())
			{
                br.setColumn(cd.getPosition(), cd.getType().getNull());
			}

            DataValueDescriptor[][] row_array = new DataValueDescriptor[100][];
            row_array[0] = br.getRowArray();
            RowLocation[] old_row_location_array = new RowLocation[100];
            RowLocation[] new_row_location_array = new RowLocation[100];

            // Create the following 3 arrays which will be used to update
            // each index as the scan moves rows about the heap as part of
            // the compress:
            //     index_col_map - map location of index cols in the base row, 
            //                     ie. index_col_map[0] is column offset of 1st
            //                     key column in base row.  All offsets are 0 
            //                     based.
            //     index_scan - open ScanController used to delete old index row
            //     index_cc   - open ConglomerateController used to insert new 
            //                  row

            ConglomerateDescriptor[] conglom_descriptors = 
                td.getConglomerateDescriptors();

            // conglom_descriptors has an entry for the conglomerate and each 
            // one of it's indexes.
            num_indexes = conglom_descriptors.length - 1;

            // if indexes exist, set up data structures to update them
            if (num_indexes > 0)
            {
                // allocate arrays
                index_col_map   = new int[num_indexes][];
                index_scan      = new ScanController[num_indexes];
                index_cc        = new ConglomerateController[num_indexes];
                index_row       = new DataValueDescriptor[num_indexes][];

                setup_indexes(
                    nested_tc,
                    td,
                    index_col_map,
                    index_scan,
                    index_cc,
                    index_row);

            }

			/* Open the heap for reading */
			base_group_fetch_cc = 
                nested_tc.defragmentConglomerate(
                    td.getHeapConglomerateId(), 
                    false,
                    true, 
                    TransactionController.OPENMODE_FORUPDATE, 
				    TransactionController.MODE_TABLE,
					TransactionController.ISOLATION_SERIALIZABLE);

            int num_rows_fetched;

            while ((num_rows_fetched = 
                        base_group_fetch_cc.fetchNextGroup(
                            row_array, 
                            old_row_location_array, 
                            new_row_location_array)) != 0)
            {
                if (num_indexes > 0)
                {
                    for (int row = 0; row < num_rows_fetched; row++)
                    {
                        for (int index = 0; index < num_indexes; index++)
                        {
                            fixIndex(
                                row_array[row],
                                index_row[index],
                                old_row_location_array[row],
                                new_row_location_array[row],
                                index_cc[index],
                                index_scan[index],
                                index_col_map[index]);
                        }
                    }
                }
            }

            // TODO - It would be better if commits happened more frequently
            // in the nested transaction, but to do that there has to be more
            // logic to catch a ddl that might jump in the middle of the 
            // above loop and invalidate the various table control structures
            // which are needed to properly update the indexes.  For example
            // the above loop would corrupt an index added midway through
            // the loop if not properly handled.  See DERBY-1188.  
            nested_tc.commit();
			
		}
		finally
		{
                /* Clean up before we leave */
                if (base_group_fetch_cc != null)
                {
                    base_group_fetch_cc.close();
                    base_group_fetch_cc = null;
                }

                if (num_indexes > 0)
                {
                    for (int i = 0; i < num_indexes; i++)
                    {
                        if (index_scan != null && index_scan[i] != null)
                        {
                            index_scan[i].close();
                            index_scan[i] = null;
                        }
                        if (index_cc != null && index_cc[i] != null)
                        {
                            index_cc[i].close();
                            index_cc[i] = null;
                        }
                    }
                }

                if (nested_tc != null)
                {
                    nested_tc.destroy();
                }

		}
	}

    private static void setup_indexes(
    TransactionController       tc,
    TableDescriptor             td,
    int[][]                     index_col_map,
    ScanController[]            index_scan,
    ConglomerateController[]    index_cc,
    DataValueDescriptor[][]     index_row)
		throws StandardException
    {

        // Initialize the following 3 arrays which will be used to update
        // each index as the scan moves rows about the heap as part of
        // the compress:
        //     index_col_map - map location of index cols in the base row, ie.
        //                     index_col_map[0] is column offset of 1st key
        //                     column in base row.  All offsets are 0 based.
        //     index_scan - open ScanController used to delete old index row
        //     index_cc   - open ConglomerateController used to insert new row

        ConglomerateDescriptor[] conglom_descriptors =
                td.getConglomerateDescriptors();


        int index_idx = 0;
        for (int cd_idx = 0; cd_idx < conglom_descriptors.length; cd_idx++)
        {
            ConglomerateDescriptor index_cd = conglom_descriptors[cd_idx];

            if (!index_cd.isIndex())
            {
                // skip the heap descriptor entry
                continue;
            }

            // ScanControllers are used to delete old index row
            index_scan[index_idx] = 
                tc.openScan(
                    index_cd.getConglomerateNumber(),
                    true,	// hold
                    TransactionController.OPENMODE_FORUPDATE,
                    TransactionController.MODE_TABLE,
                    TransactionController.ISOLATION_SERIALIZABLE,
                    null,   // full row is retrieved, 
                            // so that full row can be used for start/stop keys
                    null,	// startKeyValue - will be reset with reopenScan()
                    0,		// 
                    null,	// qualifier
                    null,	// stopKeyValue  - will be reset with reopenScan()
                    0);		// 

            // ConglomerateControllers are used to insert new index row
            index_cc[index_idx] = 
                tc.openConglomerate(
                    index_cd.getConglomerateNumber(),
                    true,  // hold
                    TransactionController.OPENMODE_FORUPDATE,
                    TransactionController.MODE_TABLE,
                    TransactionController.ISOLATION_SERIALIZABLE);

            // build column map to allow index row to be built from base row
            int[] baseColumnPositions   = 
                index_cd.getIndexDescriptor().baseColumnPositions();
            int[] zero_based_map        = 
                new int[baseColumnPositions.length];

            for (int i = 0; i < baseColumnPositions.length; i++)
            {
                zero_based_map[i] = baseColumnPositions[i] - 1; 
            }

            index_col_map[index_idx] = zero_based_map;

            // build row array to delete from index and insert into index
            //     length is length of column map + 1 for RowLocation.
            index_row[index_idx] = 
                new DataValueDescriptor[baseColumnPositions.length + 1];

            index_idx++;
        }
    }


    /**
     * Delete old index row and insert new index row in input index.
     * <p>
     *
     * @param base_row      all columns of base row
     * @param index_row     an index row template, filled in by this routine
     * @param old_row_loc   old location of base row, used to delete index
     * @param new_row_loc   new location of base row, used to update index
     * @param index_cc      index conglomerate to insert new row
     * @param index_scan    index scan to delete old entry
     * @param index_col_map description of mapping of index row to base row,
     *                      
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    private static void fixIndex(
    DataValueDescriptor[]   base_row,
    DataValueDescriptor[]   index_row,
    RowLocation             old_row_loc,
    RowLocation             new_row_loc,
    ConglomerateController  index_cc,
    ScanController          index_scan,
	int[]					index_col_map)
        throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            // baseColumnPositions should describe all columns in index row
            // except for the final column, which is the RowLocation.
            SanityManager.ASSERT(index_col_map != null);
            SanityManager.ASSERT(index_row != null);
            SanityManager.ASSERT(
                (index_col_map.length == (index_row.length - 1)));
        }

        // create the index row to delete from from the base row, using map
        for (int index = 0; index < index_col_map.length; index++)
        {
            index_row[index] = base_row[index_col_map[index]];
        }
        // last column in index in the RowLocation
        index_row[index_row.length - 1] = old_row_loc;

        // position the scan for the delete, the scan should already be open.
        // This is done by setting start scan to full key, GE and stop scan
        // to full key, GT.
        index_scan.reopenScan(
            index_row,
            ScanController.GE,
            (Qualifier[][]) null,
            index_row,
            ScanController.GT);

        // position the scan, serious problem if scan does not find the row.
        if (index_scan.next())
        {
            index_scan.delete();
        }
        else
        {
            // Didn't find the row we wanted to delete.
            if (SanityManager.DEBUG)
            {
                SanityManager.THROWASSERT(
                    "Did not find row to delete." +
                    "base_row = " + RowUtil.toString(base_row) +
                    "index_row = " + RowUtil.toString(index_row));
            }
        }

        // insert the new index row into the conglomerate
        index_row[index_row.length - 1] = new_row_loc;

        index_cc.insert(index_row);
    }

    /**
     * Purge committed deleted rows from conglomerate.
     * <p>
     * Scans the table and purges any committed deleted rows from the 
     * table.  If all rows on a page are purged then page is also 
     * reclaimed.
     * <p>
     *
     * @param tc                transaction controller to use to do updates.
     *
     **/
	private void purgeRows(TransactionController   tc)
        throws StandardException
	{
        switch (td.getTableType())
        {
        /* Skip views and vti tables */
        case TableDescriptor.VIEW_TYPE:
        case TableDescriptor.VTI_TYPE:
        	break;
        // other types give various errors here
        // DERBY-719,DERBY-720
        default:
          {

            ConglomerateDescriptor[] conglom_descriptors = 
                td.getConglomerateDescriptors();

            for (int cd_idx = 0; cd_idx < conglom_descriptors.length; cd_idx++)
            {
                ConglomerateDescriptor cd = conglom_descriptors[cd_idx];

                tc.purgeConglomerate(cd.getConglomerateNumber());
            }
          }
        }
    }

	/**
	 * Workhorse for adding a new column to a table.
	 *
	 * @param   ix 			the index of the column specfication in the ALTER 
	 *						statement-- currently we allow only one.
	 * @exception StandardException 	thrown on failure.
	 */
    private void addNewColumnToTable(int ix)
	        throws StandardException
	{
		ColumnDescriptor columnDescriptor   = 
			td.getColumnDescriptor(columnInfo[ix].name);
		DataValueDescriptor storableDV;
		int                     colNumber   = td.getMaxColumnID() + ix;

		/* We need to verify that the table does not have an existing
		 * column with the same name before we try to add the new
		 * one as addColumnDescriptor() is a void method.
		 */
		if (columnDescriptor != null)
		{
			throw 
				StandardException.newException(
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
                   SQLState.LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT,
                   columnDescriptor.getDescriptorType(),
                   columnInfo[ix].name,
                   td.getDescriptorType(),
                   td.getQualifiedName());
		}

		if (columnInfo[ix].defaultValue != null)
			storableDV = columnInfo[ix].defaultValue;
		else
			storableDV = columnInfo[ix].dataType.getNull();

		// Add the column to the conglomerate.(Column ids in store are 0-based)
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
		tc.addColumnToConglomerate(
            td.getHeapConglomerateId(), 
            colNumber, 
            storableDV, 
            columnInfo[ix].dataType.getCollationType());

		UUID defaultUUID = columnInfo[ix].newDefaultUUID;

		/* Generate a UUID for the default, if one exists
		 * and there is no default id yet.
		 */
		if (columnInfo[ix].defaultInfo != null &&
			defaultUUID == null)
		{
			defaultUUID = dd.getUUIDFactory().createUUID();
		}

		// Add the column to syscolumns. 
		// Column ids in system tables are 1-based
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
		columnDescriptor = 
            new ColumnDescriptor(
                   columnInfo[ix].name,
                   colNumber + 1,
                   columnInfo[ix].dataType,
                   columnInfo[ix].defaultValue,
                   columnInfo[ix].defaultInfo,
                   td,
                   defaultUUID,
                   columnInfo[ix].autoincStart,
                   columnInfo[ix].autoincInc,
                   columnInfo[ix].autoinc_create_or_modify_Start_Increment,
                   columnInfo[ix].autoincCycle
                   );

		dd.addDescriptor(columnDescriptor, td,
						 DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);

		// now add the column to the tables column descriptor list.
		td.getColumnDescriptorList().add(columnDescriptor);

        if (columnDescriptor.isAutoincrement())
		{
            //
            // Create a sequence generator for the auto-increment column.
            // See DERBY-6542.
            //
            CreateSequenceConstantAction csca =
                    CreateTableConstantAction.makeCSCA(
                            columnInfo[ ix],
                            TableDescriptor.makeSequenceName(td.getUUID()));
            csca.executeConstantAction(activation);
		}

		// Update the new column to its default, if it has a non-null default
        if (columnDescriptor.isAutoincrement() ||
                columnDescriptor.hasNonNullDefault())
		{
            updateNewColumnToDefault(columnDescriptor);
		}	

        //
        // Add dependencies. These can arise if a generated column depends
        // on a user created function.
        //
        addColumnDependencies( lcc, dd, td, columnInfo[ix] );

		// Update SYSCOLPERMS table which tracks the permissions granted
		// at columns level. The sytem table has a bit map of all the columns
		// in the user table to help determine which columns have the 
		// permission granted on them. Since we are adding a new column,
		// that bit map needs to be expanded and initialize the bit for it
		// to 0 since at the time of ADD COLUMN, no permissions have been
		// granted on that new column.
		//
		dd.updateSYSCOLPERMSforAddColumnToUserTable(td.getUUID(), tc);
	}

	/**
	 * Workhorse for dropping a column from a table.
	 *
	 * This routine drops a column from a table, taking care
	 * to properly handle the various related schema objects.
	 * 
	 * The syntax which gets you here is:
	 * 
	 *   ALTER TABLE tbl DROP [COLUMN] col [CASCADE|RESTRICT]
	 * 
	 * The keyword COLUMN is optional, and if you don't
	 * specify CASCADE or RESTRICT, the default is CASCADE
	 * (the default is chosen in the parser, not here).
	 * 
	 * If you specify RESTRICT, then the column drop should be
	 * rejected if it would cause a dependent schema object
	 * to become invalid.
	 * 
	 * If you specify CASCADE, then the column drop should
	 * additionally drop other schema objects which have
	 * become invalid.
	 * 
	 * You may not drop the last (only) column in a table.
	 * 
	 * Schema objects of interest include:
	 *  - views
	 *  - triggers
	 *  - constraints
	 *    - check constraints
	 *    - primary key constraints
	 *    - foreign key constraints
	 *    - unique key constraints
	 *    - not null constraints
	 *  - privileges
	 *  - indexes
	 *  - default values
	 * 
	 * Dropping a column may also change the column position
	 * numbers of other columns in the table, which may require
	 * fixup of schema objects (such as triggers and column
	 * privileges) which refer to columns by column position number.
	 * 
	 * Indexes are a bit interesting. The official SQL spec
	 * doesn't talk about indexes; they are considered to be
	 * an imlementation-specific performance optimization.
	 * The current Derby behavior is that:
	 *  - CASCADE/RESTRICT doesn't matter for indexes
	 *  - when a column is dropped, it is removed from any indexes
	 *    which contain it.
	 *  - if that column was the only column in the index, the
	 *    entire index is dropped. 
	 *
	 * @param   columnName the name of the column specfication in the ALTER 
	 *						statement-- currently we allow only one.
	 * @exception StandardException 	thrown on failure.
	 */
    private void dropColumnFromTable(String columnName )
	        throws StandardException
	{
		boolean cascade = (behavior == StatementType.DROP_CASCADE);

        // drop any generated columns which reference this column
        ColumnDescriptorList    generatedColumnList = td.getGeneratedColumns();
        int                                 generatedColumnCount = generatedColumnList.size();
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        ArrayList<String>           cascadedDroppedColumns = new ArrayList<String> ();
        for ( int i = 0; i < generatedColumnCount; i++ )
        {
            ColumnDescriptor    generatedColumn = generatedColumnList.elementAt( i );
            String[]                       referencedColumnNames = generatedColumn.getDefaultInfo().getReferencedColumnNames();
            int                         referencedColumnCount = referencedColumnNames.length;
            for ( int j = 0; j < referencedColumnCount; j++ )
            {
                if ( columnName.equals( referencedColumnNames[ j ] ) )
                {
                    String      generatedColumnName = generatedColumn.getColumnName();
                    
                    // ok, the current generated column references the column
                    // we're trying to drop
                    if (! cascade)
                    {
                        // Reject the DROP COLUMN, because there exists a
                        // generated column which references this column.
                        //
                        throw StandardException.newException
                            (
                             SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT,
                             dm.getActionString(DependencyManager.DROP_COLUMN),
                             columnName, "GENERATED COLUMN",
                             generatedColumnName
                             );
                    }
                    else
                    {
                        cascadedDroppedColumns.add( generatedColumnName );
                    }
                }
            }
        }

		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();
        int                             cascadedDrops = cascadedDroppedColumns.size();
		int sizeAfterCascadedDrops = td.getColumnDescriptorList().size() - cascadedDrops;

		// can NOT drop a column if it is the only one in the table
		if (sizeAfterCascadedDrops == 1)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
			throw StandardException.newException(
                    SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT,
                    dm.getActionString(DependencyManager.DROP_COLUMN),
                    "THE *LAST* COLUMN " + columnName,
                    "TABLE",
                    td.getQualifiedName() );
		}

        // now drop dependent generated columns
        for ( int i = 0; i < cascadedDrops; i++ )
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
            String      generatedColumnName = cascadedDroppedColumns.get( i );
            
            activation.addWarning
                ( StandardException.newWarning( SQLState.LANG_GEN_COL_DROPPED, generatedColumnName, td.getName() ) );

            //
            // We can only recurse 2 levels since a generation clause cannot
            // refer to other generated columns.
            //
//IC see: https://issues.apache.org/jira/browse/DERBY-4899
            dropColumnFromTable(generatedColumnName);
        }

        /*
         * Cascaded drops of dependent generated columns may require us to
         * rebuild the table descriptor.
         */
		td = dd.getTableDescriptor(tableId);

		ColumnDescriptor columnDescriptor = td.getColumnDescriptor( columnName );

		// We already verified this in bind, but do it again
		if (columnDescriptor == null)
		{
			throw 
				StandardException.newException(
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
                    SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE, 
                    columnName,
                    td.getQualifiedName());
		}

		int size = td.getColumnDescriptorList().size();
		droppedColumnPosition = columnDescriptor.getPosition();

		FormatableBitSet toDrop = new FormatableBitSet(size + 1);
		toDrop.set(droppedColumnPosition);
		td.setReferencedColumnMap(toDrop);

//IC see: https://issues.apache.org/jira/browse/DERBY-1489
//IC see: https://issues.apache.org/jira/browse/DERBY-1909
//IC see: https://issues.apache.org/jira/browse/DERBY-1926
		dm.invalidateFor(td, 
                        (cascade ? DependencyManager.DROP_COLUMN
                                 : DependencyManager.DROP_COLUMN_RESTRICT),
                        lcc);
					
		// If column has a default we drop the default and any dependencies
		if (columnDescriptor.getDefaultInfo() != null)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
			dm.clearDependencies(
                lcc, columnDescriptor.getDefaultDescriptor(dd));
		}

        // If the column is an identity column (and the dictionary is at least version 10.11),
        // then we need to drop the system-generated sequence backing it.
        if (
            columnDescriptor.isAutoincrement() &&
            dd.checkVersion( DataDictionary.DD_VERSION_DERBY_10_11, null )
            )
        {
            DropTableConstantAction.dropIdentitySequence( dd, td, activation );
        }

		//Now go through each trigger on this table and see if the column 
		//being dropped is part of it's trigger columns or trigger action 
		//columns which are used through REFERENCING clause
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        for (TriggerDescriptor trd : dd.getTriggerDescriptors(td)) {
			//If we find that the trigger is dependent on the column being 
			//dropped because column is part of trigger columns list, then
			//we will give a warning or drop the trigger based on whether
			//ALTER TABLE DROP COLUMN is RESTRICT or CASCADE. In such a
			//case, no need to check if the trigger action columns referenced
			//through REFERENCING clause also used the column being dropped.
			boolean triggerDroppedAlready = false;

			int[] referencedCols = trd.getReferencedCols();
//IC see: https://issues.apache.org/jira/browse/DERBY-4988
			if (referencedCols != null) {
				int refColLen = referencedCols.length, j;
				boolean changed = false;
				for (j = 0; j < refColLen; j++)
				{
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
					if (referencedCols[j] > droppedColumnPosition)
	                {
						//Trigger is not defined on the column being dropped
						//but the column position of trigger column is changing
						//because the position of the column being dropped is
						//before the the trigger column
						changed = true;
	                }
					else if (referencedCols[j] == droppedColumnPosition)
					{
						//the trigger is defined on the column being dropped
						if (cascade)
						{
	                        trd.drop(lcc);
	                        triggerDroppedAlready = true;
							activation.addWarning(
								StandardException.newWarning(
	                                SQLState.LANG_TRIGGER_DROPPED, 
	                                trd.getName(), td.getName()));
						}
						else
						{	// we'd better give an error if don't drop it,
							// otherwsie there would be unexpected behaviors
							throw StandardException.newException(
	                            SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT,
	                            dm.getActionString(DependencyManager.DROP_COLUMN),
	                            columnName, "TRIGGER",
	                            trd.getName() );
						}
						break;
					}
				}

				// The following if condition will be true if the column
				// getting dropped is not a trigger column, but one or more
				// of the trigge column's position has changed because of
				// drop column.
				if (j == refColLen && changed)
				{
					dd.dropTriggerDescriptor(trd, tc);
					for (j = 0; j < refColLen; j++)
					{
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
						if (referencedCols[j] > droppedColumnPosition)
							referencedCols[j]--;
					}
//IC see: https://issues.apache.org/jira/browse/DERBY-6202
                    trd.setReferencedCols( referencedCols );
					dd.addDescriptor(trd, sd,
									 DataDictionary.SYSTRIGGERS_CATALOG_NUM,
									 false, tc);
				}
			}

			// If the trigger under consideration got dropped through the 
			// loop above, then move to next trigger
			if (triggerDroppedAlready) continue;
			
			// Column being dropped is not one of trigger columns. Check if 
			// that column is getting used inside the trigger action through 
			// REFERENCING clause. This can be tracked only for triggers 
			// created in 10.7 and higher releases. Derby releases prior to
			// that did not keep track of trigger action columns used 
			// through the REFERENCING clause.
			int[] referencedColsInTriggerAction = trd.getReferencedColsInTriggerAction();
			if (referencedColsInTriggerAction != null) {
				int refColInTriggerActionLen = referencedColsInTriggerAction.length, j;
				boolean changedColPositionInTriggerAction = false;
				for (j = 0; j < refColInTriggerActionLen; j++)
				{
					if (referencedColsInTriggerAction[j] > droppedColumnPosition)
					{
						changedColPositionInTriggerAction = true;
					}
					else if (referencedColsInTriggerAction[j] == droppedColumnPosition)
					{
						if (cascade)
						{
	                        trd.drop(lcc);
	                        triggerDroppedAlready = true;
							activation.addWarning(
								StandardException.newWarning(
	                                SQLState.LANG_TRIGGER_DROPPED, 
	                                trd.getName(), td.getName()));
						}
						else
						{	// we'd better give an error if don't drop it,
							throw StandardException.newException(
	                            SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT,
	                            dm.getActionString(DependencyManager.DROP_COLUMN),
	                            columnName, "TRIGGER",
	                            trd.getName() );
						}
						break;
					}
				}

				// change trigger to refer to columns in new positions
				// The following if condition will be true if the column
				// getting dropped is not getting used in the trigger action
				// sql through the REFERENCING clause but one or more of those
				// column's position has changed because of drop column.
				// This applies only to triggers created with 10.7 and higher.
				// Prior to that, Derby did not keep track of the trigger 
				// action column used through the REFERENCING clause. Such
				// triggers will be caught later on in this method after the
				// column has been actually dropped from the table descriptor.
				if (j == refColInTriggerActionLen && changedColPositionInTriggerAction)
				{
					dd.dropTriggerDescriptor(trd, tc);
					for (j = 0; j < refColInTriggerActionLen; j++)
					{
						if (referencedColsInTriggerAction[j] > droppedColumnPosition)
							referencedColsInTriggerAction[j]--;
					}
//IC see: https://issues.apache.org/jira/browse/DERBY-6202
                    trd.setReferencedColsInTriggerAction( referencedColsInTriggerAction );
					dd.addDescriptor(trd, sd,
							 DataDictionary.SYSTRIGGERS_CATALOG_NUM,
							 false, tc);
				}
			}
		}

		ConstraintDescriptorList csdl = dd.getConstraintDescriptors(td);
		int csdl_size = csdl.size();

		ArrayList<ConstantAction> newCongloms = new ArrayList<ConstantAction>();
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

		// we want to remove referenced primary/unique keys in the second
		// round.  This will ensure that self-referential constraints will
		// work OK.
		int tbr_size = 0;
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
		ConstraintDescriptor[] toBeRemoved = 
            new ConstraintDescriptor[csdl_size];

		// let's go downwards, don't want to get messed up while removing
		for (int i = csdl_size - 1; i >= 0; i--)
		{
			ConstraintDescriptor cd = csdl.elementAt(i);
			int[] referencedColumns = cd.getReferencedColumns();
			int numRefCols = referencedColumns.length, j;
			boolean changed = false;
			for (j = 0; j < numRefCols; j++)
			{
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
				if (referencedColumns[j] > droppedColumnPosition)
					changed = true;
				if (referencedColumns[j] == droppedColumnPosition)
					break;
			}
			if (j == numRefCols)			// column not referenced
			{
				if ((cd instanceof CheckConstraintDescriptor) && changed)
				{
					dd.dropConstraintDescriptor(cd, tc);
					for (j = 0; j < numRefCols; j++)
					{
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
						if (referencedColumns[j] > droppedColumnPosition)
							referencedColumns[j]--;
					}
					((CheckConstraintDescriptor) cd).setReferencedColumnsDescriptor(new ReferencedColumnsDescriptorImpl(referencedColumns));
					dd.addConstraintDescriptor(cd, tc);
				}
				continue;
			}

			if (! cascade)
			{
				// Reject the DROP COLUMN, because there exists a constraint
				// which references this column.
				//
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
				throw StandardException.newException(
                        SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT,
                        dm.getActionString(DependencyManager.DROP_COLUMN),
                        columnName, "CONSTRAINT",
                        cd.getConstraintName() );
			}

			if (cd instanceof ReferencedKeyConstraintDescriptor)
			{
				// restrict will raise an error in invalidate if referenced
				toBeRemoved[tbr_size++] = cd;
				continue;
			}

			// drop now in all other cases
			dm.invalidateFor(cd, DependencyManager.DROP_CONSTRAINT,
									lcc);

			dropConstraint(cd, td, newCongloms, activation, lcc, true);
			activation.addWarning(
                StandardException.newWarning(SQLState.LANG_CONSTRAINT_DROPPED,
				cd.getConstraintName(), td.getName()));
		}

		for (int i = tbr_size - 1; i >= 0; i--)
		{
			ConstraintDescriptor cd = toBeRemoved[i];
			dropConstraint(cd, td, newCongloms, activation, lcc, false);

			activation.addWarning(
                StandardException.newWarning(SQLState.LANG_CONSTRAINT_DROPPED,
                cd.getConstraintName(), td.getName()));

			if (cascade)
			{
				ConstraintDescriptorList fkcdl = dd.getForeignKeys(cd.getUUID());

//IC see: https://issues.apache.org/jira/browse/DERBY-673
                for (ConstraintDescriptor fkcd : fkcdl)
                {
					dm.invalidateFor(fkcd,
									DependencyManager.DROP_CONSTRAINT,
									lcc);

					dropConstraint(fkcd, td,
						newCongloms, activation, lcc, true);

					activation.addWarning(
                        StandardException.newWarning(
                            SQLState.LANG_CONSTRAINT_DROPPED,
						    fkcd.getConstraintName(), 
                            fkcd.getTableDescriptor().getName()));
				}
			}

			dm.invalidateFor(cd, DependencyManager.DROP_CONSTRAINT, lcc);
			dm.clearDependencies(lcc, cd);
		}

		/* If there are new backing conglomerates which must be
		 * created to replace a dropped shared conglomerate
		 * (where the shared conglomerate was dropped as part
		 * of a "drop constraint" call above), then create them
		 * now.  We do this *after* dropping all dependent
		 * constraints because we don't want to waste time
		 * creating a new conglomerate if it's just going to be
		 * dropped again as part of another "drop constraint".
		 */
		createNewBackingCongloms(newCongloms, (long[])null);
//IC see: https://issues.apache.org/jira/browse/DERBY-4899

        /*
         * The work we've done above, specifically the possible
         * dropping of primary key, foreign key, and unique constraints
         * and their underlying indexes, may have affected the table
         * descriptor. By re-reading the table descriptor here, we
         * ensure that the compressTable code is working with an
         * accurate table descriptor. Without this line, we may get
         * conglomerate-not-found errors and the like due to our
         * stale table descriptor.
         */
		td = dd.getTableDescriptor(tableId);
//IC see: https://issues.apache.org/jira/browse/DERBY-1489
//IC see: https://issues.apache.org/jira/browse/DERBY-1909
//IC see: https://issues.apache.org/jira/browse/DERBY-1926

        compressTable();
//IC see: https://issues.apache.org/jira/browse/DERBY-4899

		ColumnDescriptorList tab_cdl = td.getColumnDescriptorList();

		// drop the column from syscolumns 
		dd.dropColumnDescriptor(td.getUUID(), columnName, tc);		
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
		ColumnDescriptor[] cdlArray = 
            new ColumnDescriptor[size - columnDescriptor.getPosition()];

		// For each column in this table with a higher column position,
		// drop the entry from SYSCOLUMNS, but hold on to the column
		// descriptor and reset its position to adjust for the dropped
		// column. Then, re-add all those adjusted column descriptors
		// back to SYSCOLUMNS
		//
		for (int i = columnDescriptor.getPosition(), j = 0; i < size; i++, j++)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-673
            ColumnDescriptor cd = tab_cdl.elementAt(i);
			dd.dropColumnDescriptor(td.getUUID(), cd.getColumnName(), tc);
			cd.setPosition(i);
//IC see: https://issues.apache.org/jira/browse/DERBY-3175
			if (cd.isAutoincrement())
			{
				cd.setAutoinc_create_or_modify_Start_Increment(
						ColumnDefinitionNode.CREATE_AUTOINCREMENT);
			}

			cdlArray[j] = cd;
		}
		dd.addDescriptorArray(cdlArray, td,
							  DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);

		// By this time, the column has been removed from the table descriptor.
		// Now, go through all the triggers and regenerate their trigger action
		// SPS and rebind the generated trigger action sql. If the trigger  
		// action is using the dropped column, it will get detected here. If 
		// not, then we will have generated the internal trigger action sql
		// which matches the trigger action sql provided by the user.
		//
		// eg of positive test case
		// create table atdc_16_tab1 (a1 integer, b1 integer, c1 integer);
		// create table atdc_16_tab2 (a2 integer, b2 integer, c2 integer);
		// create trigger atdc_16_trigger_1 
		//    after update of b1 on atdc_16_tab1
		//    REFERENCING NEW AS newt
		//    for each row 
		//    update atdc_16_tab2 set c2 = newt.c1
		// The internal representation for the trigger action before the column
		// is dropped is as follows
		// 	 update atdc_16_tab2 set c2 = 
		//   org.apache.derby.iapi.db.Factory::getTriggerExecutionContext().
		//   getONewRow().getInt(3)
		// After the drop column shown as below
		//   alter table DERBY4998_SOFT_UPGRADE_RESTRICT drop column c11
		// The above internal representation of tigger action sql is not 
		// correct anymore because column position of c1 in atdc_16_tab1 has 
		// now changed from 3 to 2. Following while loop will regenerate it and
		// change it to as follows
		// 	 update atdc_16_tab2 set c2 = 
		//   org.apache.derby.iapi.db.Factory::getTriggerExecutionContext().
		//   getONewRow().getInt(2)
		//
		// We could not do this before the actual column drop, because the 
		// rebind would have still found the column being dropped in the
		// table descriptor and hence use of such a column in the trigger
		// action rebind would not have been caught.

		//For the table on which ALTER TABLE is getting performed, find out
		// all the SPSDescriptors that use that table as a provider. We are
		// looking for SPSDescriptors that have been created internally for
		// trigger action SPSes. Through those SPSDescriptors, we will be
		// able to get to the triggers dependent on the table being altered
		//Following will get all the dependent objects that are using
		// ALTER TABLE table as provider
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        List<DependencyDescriptor> depsOnAlterTableList =
                dd.getProvidersDescriptorList(td.getObjectID().toString());

        for (DependencyDescriptor depOnAT : depsOnAlterTableList)
        {
            // Go through all the dependent objects on the table being altered
            DependableFinder dependent = depOnAT.getDependentFinder();

            // For the given dependent, we are only interested in it if it is a
			// stored prepared statement.
			if (dependent.getSQLObjectType().equals(Dependable.STORED_PREPARED_STATEMENT))
			{
                // Look for all the dependent objects that are using this
				// stored prepared statement as provider. We are only 
				// interested in dependents that are triggers.
                List<DependencyDescriptor> depsTrigger =
                    dd.getProvidersDescriptorList(depOnAT.getUUID().toString());

                for (DependencyDescriptor depsTriggerDesc : depsTrigger)
				{
					DependableFinder providerIsTrigger = depsTriggerDesc.getDependentFinder();
					//For the given dependent, we are only interested in it if
					// it is a trigger
					if (providerIsTrigger.getSQLObjectType().equals(Dependable.TRIGGER)) {
						//Drop and recreate the trigger after regenerating 
						// it's trigger action plan. If the trigger action
						// depends on the column being dropped, it will be
						// caught here.
						TriggerDescriptor trdToBeDropped  = dd.getTriggerDescriptor(depsTriggerDesc.getUUID());

                        // First check for dependencies in the trigger's WHEN
                        // clause, if there is one.
                        UUID whenClauseId = trdToBeDropped.getWhenClauseId();
                        boolean gotDropped = false;
                        if (whenClauseId != null) {
                            gotDropped = columnDroppedAndTriggerDependencies(
                                    trdToBeDropped, whenClauseId, true,
                                    cascade, columnName);
                        }

                        // If no dependencies were found in the WHEN clause,
                        // we have to check if the triggered SQL statement
                        // depends on the column being dropped. But if there
                        // were dependencies and the trigger has already been
                        // dropped, there is no point in looking for more
                        // dependencies.
                        if (!gotDropped) {
                            columnDroppedAndTriggerDependencies(trdToBeDropped,
                                    trdToBeDropped.getActionId(), false,
                                    cascade, columnName);
                        }
					}
				}
			}
		}
		// Adjust the column permissions rows in SYSCOLPERMS to reflect the
		// changed column positions due to the dropped column:
		dd.updateSYSCOLPERMSforDropColumn(td.getUUID(), tc, columnDescriptor);

        // remove column descriptor from table descriptor. this fixes up the
        // list in case we were called recursively in order to cascade-drop a
        // dependent generated column.
        tab_cdl.remove( td.getColumnDescriptor( columnName ) );
	}
    
	// For the trigger, get the trigger action sql provided by the user
	// in the create trigger sql. This sql is saved in the system
	// table. Since a column has been dropped from the trigger table,
	// the trigger action sql may not be valid anymore. To establish
	// that, we need to regenerate the internal representation of that 
	// sql and bind it again.
    //
    // This method is called both on the WHEN clause (if one exists) and the
    // triggered SQL statement of the trigger action.
    //
    // Return true if the trigger was dropped by this method (if cascade is
    // true and it turns out the trigger depends on the column being dropped),
    // or false otherwise.
    private boolean columnDroppedAndTriggerDependencies(TriggerDescriptor trd,
            UUID spsUUID, boolean isWhenClause,
			boolean cascade, String columnName)
	throws StandardException {
		dd.dropTriggerDescriptor(trd, tc);

		// Here we get the trigger action sql and use the parser to build
		// the parse tree for it.
        SchemaDescriptor compSchema = dd.getSchemaDescriptor(
                dd.getSPSDescriptor(spsUUID).getCompSchemaId(),
                null);
		CompilerContext newCC = lcc.pushCompilerContext(compSchema);
		Parser	pa = newCC.getParser();
        String originalSQL = isWhenClause ? trd.getWhenClauseText()
                                          : trd.getTriggerDefinition();
        Visitable node = isWhenClause ? pa.parseSearchCondition(originalSQL)
                                      : pa.parseStatement(originalSQL);
		lcc.popCompilerContext(newCC);
		// Do not delete following. We use this in finally clause to 
		// determine if the CompilerContext needs to be popped.
		newCC = null;
		
		try {
			// Regenerate the internal representation for the trigger action 
			// sql using the ColumnReference classes in the parse tree. It
			// will catch dropped column getting used in trigger action sql
			// through the REFERENCING clause(this can happen only for the
			// the triggers created prior to 10.7. Trigger created with
			// 10.7 and higher keep track of trigger action column used
			// through the REFERENCING clause in system table and hence
			// use of dropped column will be detected earlier in this 
			// method for such triggers).
			//
			// We might catch errors like following during this step.
			// Say that following pre-10.7 trigger exists in the system and
			// user is dropping column c11. During the regeneration of the
			// internal trigger action sql format, we will catch that 
			// column oldt.c11 does not exist anymore
			// CREATE TRIGGER DERBY4998_SOFT_UPGRADE_RESTRICT_tr1 
			//    AFTER UPDATE OF c12 
			//    ON DERBY4998_SOFT_UPGRADE_RESTRICT REFERENCING OLD AS oldt
			//    FOR EACH ROW 
			//    SELECT oldt.c11 from DERBY4998_SOFT_UPGRADE_RESTRICT

            SPSDescriptor sps = isWhenClause ? trd.getWhenClauseSPS(lcc)
                                             : trd.getActionSPS(lcc);
			int[] referencedColsInTriggerAction = new int[td.getNumberOfColumns()];
			java.util.Arrays.fill(referencedColsInTriggerAction, -1);
            String newText = dd.getTriggerActionString(node,
				trd.getOldReferencingName(),
				trd.getNewReferencingName(),
                originalSQL,
				trd.getReferencedCols(),
				referencedColsInTriggerAction,
				0,
				trd.getTableDescriptor(),
				trd.getTriggerEventMask(),
                true,
//IC see: https://issues.apache.org/jira/browse/DERBY-6783
                null,
                null);

            if (isWhenClause) {
                // The WHEN clause is not a full SQL statement, just a search
                // condition, so we need to turn it into a statement in order
                // to create an SPS.
                newText = "VALUES " + newText;
            }

            sps.setText(newText);
			
			// Now that we have the internal format of the trigger action sql, 
			// bind that sql to make sure that we are not using colunm being
			// dropped in the trigger action sql directly (ie not through
			// REFERENCING clause.
			// eg
			// create table atdc_12 (a integer, b integer);
			// create trigger atdc_12_trigger_1 after update of a 
			//     on atdc_12 for each row select a,b from atdc_12
			// Drop one of the columns used in the trigger action
			//   alter table atdc_12 drop column b
			// Following rebinding of the trigger action sql will catch the use
			// of column b in trigger atdc_12_trigger_1
			newCC = lcc.pushCompilerContext(compSchema);
		    newCC.setReliability(CompilerContext.INTERNAL_SQL_LEGAL);
			pa = newCC.getParser();
            StatementNode stmtnode = (StatementNode) pa.parseStatement(newText);
			// need a current dependent for bind
            newCC.setCurrentDependent(sps.getPreparedStatement());
			stmtnode.bindStatement();
		} catch (StandardException se)
		{
			//Need to catch for few different kinds of sql states depending
			// on what kind of trigger action sql is using the column being 
			// dropped. Following are examples for different sql states
			//
			//SQLState.LANG_COLUMN_NOT_FOUND is thrown for following usage in
			// trigger action sql of column being dropped atdc_12.b
			//        create trigger atdc_12_trigger_1 after update 
			//           of a on atdc_12 
			//           for each row 
			//           select a,b from atdc_12
			//
			//SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE is thrown for following
			// usage in trigger action sql of column being dropped  
			// atdc_14_tab2a2 with restrict clause
			//        create trigger atdc_14_trigger_1 after update 
			//           on atdc_14_tab1 REFERENCING NEW AS newt 
			//           for each row 
			//           update atdc_14_tab2 set a2 = newt.a1
			//
			// SQLState.LANG_DB2_INVALID_COLS_SPECIFIED is thrown for following
			//  usage in trigger action sql of column being dropped  
			//  ATDC_13_TAB1_BACKUP.c11 with restrict clause
			//         create trigger ATDC_13_TAB1_trigger_1 after update
			//           on ATDC_13_TAB1 for each row
			//           INSERT INTO ATDC_13_TAB1_BACKUP
			//           SELECT C31, C32 from ATDC_13_TAB3
			//
			//SQLState.LANG_TABLE_NOT_FOUND is thrown for following scenario
			//   create view ATDC_13_VIEW2 as select c12 from ATDC_13_TAB3 where c12>0
			//Has following trigger defined
			//         create trigger ATDC_13_TAB1_trigger_3 after update
			//           on ATDC_13_TAB1 for each row
			//           SELECT * from ATDC_13_VIEW2
			// Ane drop column ATDC_13_TAB3.c12 is issued
			if (se.getMessageId().equals(SQLState.LANG_COLUMN_NOT_FOUND)||
					(se.getMessageId().equals(SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE) ||
					(se.getMessageId().equals(SQLState.LANG_DB2_INVALID_COLS_SPECIFIED) ||
					(se.getMessageId().equals(SQLState.LANG_TABLE_NOT_FOUND)))))
			{
				if (cascade)
				{
                    trd.drop(lcc);
					activation.addWarning(
						StandardException.newWarning(
                            SQLState.LANG_TRIGGER_DROPPED, 
                            trd.getName(), td.getName()));
                    return true;
				}
				else
				{	// we'd better give an error if don't drop it,
					throw StandardException.newException(
                        SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT,
                        dm.getActionString(DependencyManager.DROP_COLUMN),
                        columnName, "TRIGGER",
                        trd.getName() );
				}
			} else
				throw se;
		}
		finally
		{
			if (newCC != null)
				lcc.popCompilerContext(newCC);
		}
		
		// If we are here, then it means that the column being dropped
		// is not getting used in the trigger action. 
		//
		// We have recreated the trigger action SPS and recollected the 
		// column positions for trigger columns and trigger action columns
		// getting accessed through REFERENCING clause because
		// drop column can affect the column positioning of existing
		// columns in the table. We will save that in the system table.
		dd.addDescriptor(trd, sd,
				 DataDictionary.SYSTRIGGERS_CATALOG_NUM,
				 false, tc);

        return false;
    }

    private void modifyColumnType(int ix)
            throws StandardException {
		ColumnDescriptor columnDescriptor = 
            td.getColumnDescriptor(columnInfo[ix].name);
//IC see: https://issues.apache.org/jira/browse/DERBY-673

        ColumnDescriptor newColumnDescriptor =
			new ColumnDescriptor(columnInfo[ix].name,
									columnDescriptor.getPosition(),
									columnInfo[ix].dataType,
									columnDescriptor.getDefaultValue(),
									columnDescriptor.getDefaultInfo(),
									td,
									columnDescriptor.getDefaultUUID(),
								    columnInfo[ix].autoincStart,
								    columnInfo[ix].autoincInc,
								    columnInfo[ix].autoincCycle
									);
		


		// Update the ColumnDescriptor with new default info
		dd.dropColumnDescriptor(td.getUUID(), columnInfo[ix].name, tc);
		dd.addDescriptor(newColumnDescriptor, td,
						 DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);
	}	

	/**
	 * Workhorse for modifying column level constraints. 
	 * Right now it is restricted to modifying a null constraint to a not null
	 * constraint.
	 */
    private void modifyColumnConstraint(String colName, boolean nullability)
//IC see: https://issues.apache.org/jira/browse/DERBY-4899
//IC see: https://issues.apache.org/jira/browse/DERBY-4899
            throws StandardException {
		ColumnDescriptor columnDescriptor = 
//IC see: https://issues.apache.org/jira/browse/DERBY-673
            td.getColumnDescriptor(colName);
        
        // Get the type and change the nullability
		DataTypeDescriptor dataType =
            columnDescriptor.getType().getNullabilityType(nullability);

        //check if there are any unique constraints to update
        ConstraintDescriptorList cdl = dd.getConstraintDescriptors(td);
        int columnPostion = columnDescriptor.getPosition();
//IC see: https://issues.apache.org/jira/browse/DERBY-3456
        for (int i = 0; i < cdl.size(); i++) 
        {
            ConstraintDescriptor cd = cdl.elementAt(i);
            if (cd.getConstraintType() == DataDictionary.UNIQUE_CONSTRAINT) 
            {
                ColumnDescriptorList columns = cd.getColumnDescriptors();
                for (int count = 0; count < columns.size(); count++) 
                {
                    if (columns.elementAt(count).getPosition() != columnPostion)
                        break;

                    //get backing index
                    ConglomerateDescriptor desc = 
                        td.getConglomerateDescriptor(cd.getConglomerateId());

                    //check if the backing index was created when the column
                    //not null ie is backed by unique index
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                    if (! (desc.getIndexDescriptor().isUnique() ||
                           desc.getIndexDescriptor().hasDeferrableChecking())) {
                        break;
                    }

                    // replace backing index with a unique when not null index.
                    recreateUniqueConstraintBackingIndexAsUniqueWhenNotNull(
                        desc, td, activation, lcc);
                }
            }
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-673
        ColumnDescriptor newColumnDescriptor =
			 new ColumnDescriptor(colName,
									columnDescriptor.getPosition(),
									dataType,
									columnDescriptor.getDefaultValue(),
									columnDescriptor.getDefaultInfo(),
									td,
									columnDescriptor.getDefaultUUID(),
									columnDescriptor.getAutoincStart(),
//IC see: https://issues.apache.org/jira/browse/DERBY-6903
//IC see: https://issues.apache.org/jira/browse/DERBY-6904
//IC see: https://issues.apache.org/jira/browse/DERBY-6905
//IC see: https://issues.apache.org/jira/browse/DERBY-6906
//IC see: https://issues.apache.org/jira/browse/DERBY-534
									columnDescriptor.getAutoincInc(),
									columnDescriptor.getAutoincCycle());
        
		// Update the ColumnDescriptor with new default info
		dd.dropColumnDescriptor(td.getUUID(), colName, tc);
		dd.addDescriptor(newColumnDescriptor, td,
//IC see: https://issues.apache.org/jira/browse/DERBY-3456
						 DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);		
	}
	/**
	 * Workhorse for modifying the default value of a column.
	 * 
	 * @param       ix 		the index of the column specfication in the ALTER 
	 *						statement-- currently we allow only one.
	 * @exception	StandardException, thrown on error.
	 */
    private void modifyColumnDefault(int ix)
			throws StandardException						 
	{
		ColumnDescriptor columnDescriptor = 
			td.getColumnDescriptor(columnInfo[ix].name);
		int columnPosition = columnDescriptor.getPosition();

		// Clean up after the old default, if non-null
		if (columnDescriptor.hasNonNullDefault())
		{
			// Invalidate off of the old default
			DefaultDescriptor defaultDescriptor = new DefaultDescriptor(dd, columnInfo[ix].oldDefaultUUID, 
										 td.getUUID(), columnPosition);

		
			dm.invalidateFor(defaultDescriptor, DependencyManager.MODIFY_COLUMN_DEFAULT, lcc);
		
			// Drop any dependencies
			dm.clearDependencies(lcc, defaultDescriptor);
		}

		UUID defaultUUID = columnInfo[ix].newDefaultUUID;

		/* Generate a UUID for the default, if one exists
		 * and there is no default id yet.
		 */
		if (columnInfo[ix].defaultInfo != null &&
			defaultUUID == null)
		{	
			defaultUUID = dd.getUUIDFactory().createUUID();
		}

		/* Get a ColumnDescriptor reflecting the new default */
		columnDescriptor = new ColumnDescriptor(
												   columnInfo[ix].name,
												   columnPosition,
												   columnInfo[ix].dataType,
												   columnInfo[ix].defaultValue,
												   columnInfo[ix].defaultInfo,
												   td,
												   defaultUUID,
												   columnInfo[ix].autoincStart,
//IC see: https://issues.apache.org/jira/browse/DERBY-6903
//IC see: https://issues.apache.org/jira/browse/DERBY-6904
//IC see: https://issues.apache.org/jira/browse/DERBY-6905
//IC see: https://issues.apache.org/jira/browse/DERBY-6906
//IC see: https://issues.apache.org/jira/browse/DERBY-534
												   columnInfo[ix].autoincInc,
//IC see: https://issues.apache.org/jira/browse/DERBY-6903
//IC see: https://issues.apache.org/jira/browse/DERBY-6904
//IC see: https://issues.apache.org/jira/browse/DERBY-6905
//IC see: https://issues.apache.org/jira/browse/DERBY-6906
//IC see: https://issues.apache.org/jira/browse/DERBY-534
//IC see: https://issues.apache.org/jira/browse/DERBY-6903
//IC see: https://issues.apache.org/jira/browse/DERBY-6904
//IC see: https://issues.apache.org/jira/browse/DERBY-6905
//IC see: https://issues.apache.org/jira/browse/DERBY-6906
//IC see: https://issues.apache.org/jira/browse/DERBY-534
												   columnInfo[ix].autoinc_create_or_modify_Start_Increment,
												   columnInfo[ix].autoincCycle
												   );

		// Update the ColumnDescriptor with new default info
		dd.dropColumnDescriptor(td.getUUID(), columnInfo[ix].name, tc);
		dd.addDescriptor(columnDescriptor, td,
						 DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);
	
//IC see: https://issues.apache.org/jira/browse/DERBY-783
		if (columnInfo[ix].action == ColumnInfo.MODIFY_COLUMN_DEFAULT_INCREMENT)
		{
			// adding an autoincrement default-- calculate the maximum value 
			// of the autoincrement column.
//IC see: https://issues.apache.org/jira/browse/DERBY-4899
            long maxValue = getColumnMax(td, columnInfo[ix].name,
                                         columnInfo[ix].autoincInc);
			dd.setAutoincrementValue(tc, td.getUUID(), columnInfo[ix].name,
									 maxValue, true);
//IC see: https://issues.apache.org/jira/browse/DERBY-783
		} else if (columnInfo[ix].action == ColumnInfo.MODIFY_COLUMN_DEFAULT_RESTART)
		{
			dd.setAutoincrementValue(tc, td.getUUID(), columnInfo[ix].name,
					 columnInfo[ix].autoincStart, false);
		} 
		// else we are simply changing the default value

		if (
            (columnInfo[ix].action == ColumnInfo.MODIFY_COLUMN_DEFAULT_INCREMENT) ||
//IC see: https://issues.apache.org/jira/browse/DERBY-6903
//IC see: https://issues.apache.org/jira/browse/DERBY-6904
//IC see: https://issues.apache.org/jira/browse/DERBY-6905
//IC see: https://issues.apache.org/jira/browse/DERBY-6906
//IC see: https://issues.apache.org/jira/browse/DERBY-534
            (columnInfo[ix].action == ColumnInfo.MODIFY_COLUMN_DEFAULT_RESTART) ||
             (columnInfo[ix].action == ColumnInfo.MODIFY_COLUMN_DEFAULT_CYCLE) 
            )
        {
            //
            // If we're at level 10.11 or higher, we re-create the sequence generator
            // for the auto-increment column. See derby-6542.
            //
            if ( dd.checkVersion( DataDictionary.DD_VERSION_DERBY_10_11, null ) )
            {
                Long    currentValue = null;
                
                // don't clobber the current value of the sequence generator if we
                // are just changing the increment. see DERBY-6579.
//IC see: https://issues.apache.org/jira/browse/DERBY-6903
//IC see: https://issues.apache.org/jira/browse/DERBY-6904
//IC see: https://issues.apache.org/jira/browse/DERBY-6905
//IC see: https://issues.apache.org/jira/browse/DERBY-6906
//IC see: https://issues.apache.org/jira/browse/DERBY-534
                if ( (columnInfo[ix].action == ColumnInfo.MODIFY_COLUMN_DEFAULT_INCREMENT) || (columnInfo[ix].action == ColumnInfo.MODIFY_COLUMN_DEFAULT_CYCLE ))
                {
                    currentValue = dd.peekAtIdentity( td.getSchemaName(), td.getName() );
                }
                
//IC see: https://issues.apache.org/jira/browse/DERBY-6961
                if (columnInfo[ix].action == ColumnInfo.MODIFY_COLUMN_DEFAULT_CYCLE)
                {
                    if (columnInfo[ix].autoincCycle)
                    {
                        // ALTER TABLE ALTER COLUMN $columnName SET CYCLE
                        if (currentValue == null)
                        {
                            //
                            // If the current value is NULL, then the sequence generator
                            // is exhausted and it must have been a NO CYCLE generator,
                            // which we are changing to CYCLE.
                            // According to the 2016 SQL Standard, section 4.27.2
                            // (Operations involving sequence generators),
                            // the next value of the sequence generator should be the minimum value
                            // (for an ascending sequence generator) or the maximum value
                            // (for a descending sequence generator). See DERBY-6961.
                            // This logic will have to change in the future if we
                            // let users configure the maximum and minimum values of identity columns.
                            //
                            int topOrBottom = (columnInfo[ix].autoincInc > 0) ? RANGE_BOTTOM : RANGE_TOP;
                            currentValue = getRangeBound(columnInfo[ix].dataType, topOrBottom);
                        }
                    }
                    else
                    {
                        // ALTER TABLE ALTER COLUMN $columnName SET NO CYCLE
                        //
                        // If we are just about to issue the rollover value,
                        // set it to NULL in order to prevent cycling.
                        int topOrBottom = (columnInfo[ix].autoincInc > 0) ? RANGE_BOTTOM : RANGE_TOP;
                        Long rolloverValue = getRangeBound(columnInfo[ix].dataType, topOrBottom);

                        if ((currentValue != null) && (currentValue.equals(rolloverValue)))
                        { currentValue = null; }
                    }
                }

                DropTableConstantAction.dropIdentitySequence( dd, td, activation );

                // recreate the sequence
                String      sequenceName = TableDescriptor.makeSequenceName( td.getUUID() );
                CreateSequenceConstantAction   csca = CreateTableConstantAction.makeCSCA
                    ( columnInfo[ix], sequenceName );
                csca.executeConstantAction( activation );

                // reset the current value of the sequence generator as necessary
//IC see: https://issues.apache.org/jira/browse/DERBY-6903
//IC see: https://issues.apache.org/jira/browse/DERBY-6904
//IC see: https://issues.apache.org/jira/browse/DERBY-6905
//IC see: https://issues.apache.org/jira/browse/DERBY-6906
//IC see: https://issues.apache.org/jira/browse/DERBY-534
                if ( (columnInfo[ix].action == ColumnInfo.MODIFY_COLUMN_DEFAULT_INCREMENT) || (columnInfo[ix].action == ColumnInfo.MODIFY_COLUMN_DEFAULT_CYCLE ) )
                {
                    SequenceDescriptor  sequence = dd.getSequenceDescriptor
                        ( dd.getSystemSchemaDescriptor(), sequenceName );
                    RowLocation[] rowLocation = new RowLocation[ 1 ];
                    SequenceDescriptor[] sequenceDescriptor = new SequenceDescriptor[ 1 ];
            
                    dd.computeSequenceRowLocation
                        ( tc, sequence.getUUID().toString(), rowLocation, sequenceDescriptor );
                    dd.updateCurrentSequenceValue
                        ( tc, rowLocation[ 0 ], true, null, currentValue );
                }
                
            }
        }
	}

    /**
     * Get the ran max or min range bound for an autoincrement column.
     *
     * @param dtd The type of the autoincrement column.
     * @param topOrBottom RANGE_TOP or RANGE_BOTTOM
     *
     * @return the top or bottom of the range
     */
    private long getRangeBound(DataTypeDescriptor dtd, int topOrBottom)
//IC see: https://issues.apache.org/jira/browse/DERBY-6961
        throws StandardException
    {
        TypeId typeId = dtd.getTypeId();
        boolean bottom = (topOrBottom == RANGE_BOTTOM);
        if (typeId == TypeId.SMALLINT_ID)
        {
            return (bottom ? Long.valueOf(Short.MIN_VALUE) : Long.valueOf(Short.MAX_VALUE));
        }
        else if (typeId == TypeId.INTEGER_ID)
        {
            return (bottom ? Long.valueOf(Integer.MIN_VALUE) : Long.valueOf(Integer.MAX_VALUE));
        }
        else // must be BIGINT
        {
            // but verify this is BIGINT in case someone adds
            // a new numeric type in the future
            if (typeId != TypeId.BIGINT_ID)
            {
                throw StandardException.newException( SQLState.NOT_IMPLEMENTED );
            }

            return (bottom ? Long.MIN_VALUE : Long.MAX_VALUE);
        }
    }
  
	/**
	 * Change an identity from ALWAYS to BY DEFAULT (or vice versa)
	 * 
	 * @param       ix 		the index of the column specfication in the ALTER 
	 *						statement-- currently we allow only one.
	 * @exception	StandardException, thrown on error.
	 */
    private void modifyIdentityState(int ix)
			throws StandardException						 
	{
		ColumnDescriptor oldColumnDescriptor = td.getColumnDescriptor(columnInfo[ix].name);
		int columnPosition = oldColumnDescriptor.getPosition();
        boolean wasGeneratedAlways = oldColumnDescriptor.isAutoincAlways();
        boolean willBeGeneratedAlways =
          (columnInfo[ix].action == ColumnInfo.MODIFY_COLUMN_GENERATED_ALWAYS);

        // nothing to do if the identity column already behaves the right way
        if (wasGeneratedAlways == willBeGeneratedAlways) { return; }

        UUID defaultUUID = willBeGeneratedAlways ? null : dd.getUUIDFactory().createUUID();

        /* Get a ColumnDescriptor reflecting the new default */
		ColumnDescriptor newColumnDescriptor = new ColumnDescriptor
          (
           columnInfo[ix].name,
           columnPosition,
           oldColumnDescriptor.getType(),
           columnInfo[ix].defaultValue,
           columnInfo[ix].defaultInfo,
           td,
           defaultUUID,
           oldColumnDescriptor.getAutoincStart(),
           oldColumnDescriptor.getAutoincInc(),
//IC see: https://issues.apache.org/jira/browse/DERBY-6903
//IC see: https://issues.apache.org/jira/browse/DERBY-6904
//IC see: https://issues.apache.org/jira/browse/DERBY-6905
//IC see: https://issues.apache.org/jira/browse/DERBY-6906
//IC see: https://issues.apache.org/jira/browse/DERBY-534
           ColumnDefinitionNode.MODIFY_AUTOINCREMENT_ALWAYS_VS_DEFAULT,
           oldColumnDescriptor.getAutoincCycle()
           );

		// Update the ColumnDescriptor with new default info
		dd.dropColumnDescriptor(td.getUUID(), columnInfo[ix].name, tc);
		dd.addDescriptor(newColumnDescriptor, td,
						 DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);
    }
  
    /**
     * routine to process compress table or ALTER TABLE <t> DROP COLUMN <c>;
     * <p>
     * Uses class level variable "compressTable" to determine if processing
     * compress table or drop column:
     *     if (!compressTable)
     *         must be drop column.
     * <p>
     * Handles rebuilding of base conglomerate and all necessary indexes.
     **/
    private void compressTable()
		throws StandardException
	{
		long					newHeapConglom;
		Properties				properties = new Properties();
		RowLocation				rl;

		if (SanityManager.DEBUG)
		{
			if (lockGranularity != '\0')
			{
				SanityManager.THROWASSERT(
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
					"lockGranularity expected to be '\0', not " + 
                    lockGranularity);
			}
			SanityManager.ASSERT(! compressTable || columnInfo == null,
				"columnInfo expected to be null");
			SanityManager.ASSERT(constraintActions == null,
				"constraintActions expected to be null");
		}

//IC see: https://issues.apache.org/jira/browse/DERBY-2661
		ExecRow emptyHeapRow  = td.getEmptyExecRow();
        int[]   collation_ids = td.getColumnCollationIds();

//IC see: https://issues.apache.org/jira/browse/DERBY-2537
		compressHeapCC = 
            tc.openConglomerate(
                td.getHeapConglomerateId(),
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_SERIALIZABLE);

		rl = compressHeapCC.newRowLocationTemplate();

		// Get the properties on the old heap
		compressHeapCC.getInternalTablePropertySet(properties);
		compressHeapCC.close();
		compressHeapCC = null;

		// Create an array to put base row template
		baseRow = new ExecRow[bulkFetchSize];
		baseRowArray = new DataValueDescriptor[bulkFetchSize][];
		validRow = new boolean[bulkFetchSize];

		/* Set up index info */
        getAffectedIndexes();
//IC see: https://issues.apache.org/jira/browse/DERBY-4899

		// Get an array of RowLocation template
		compressRL = new RowLocation[bulkFetchSize];
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
		indexRows  = new ExecIndexRow[numIndexes];
		if (!compressTable)
		{
            // must be a drop column, thus the number of columns in the
            // new template row and the collation template is one less.
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
			ExecRow newRow = 
                activation.getExecutionFactory().getValueRow(
                    emptyHeapRow.nColumns() - 1);

            int[]   new_collation_ids = new int[collation_ids.length - 1];

			for (int i = 0; i < newRow.nColumns(); i++)
			{
				newRow.setColumn(
                    i + 1, 
                    i < droppedColumnPosition - 1 ?
                        emptyHeapRow.getColumn(i + 1) :
                        emptyHeapRow.getColumn(i + 1 + 1));

                new_collation_ids[i] = 
                    collation_ids[
                        (i < droppedColumnPosition - 1) ? i : (i + 1)];
			}

			emptyHeapRow = newRow;
			collation_ids = new_collation_ids;
		}
		setUpAllSorts(emptyHeapRow, rl);

		// Start by opening a full scan on the base table.
		openBulkFetchScan(td.getHeapConglomerateId());

		// Get the estimated row count for the sorters
		estimatedRowCount = compressHeapGSC.getEstimatedRowCount();

		// Create the array of base row template
		for (int i = 0; i < bulkFetchSize; i++)
		{
			// create a base row template
//IC see: https://issues.apache.org/jira/browse/DERBY-2661
			baseRow[i] = td.getEmptyExecRow();
			baseRowArray[i] = baseRow[i].getRowArray();
			compressRL[i] = compressHeapGSC.newRowLocationTemplate();
		}


		newHeapConglom = 
            tc.createAndLoadConglomerate(
                "heap",
                emptyHeapRow.getRowArray(),
                null, //column sort order - not required for heap
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
                collation_ids,
                properties,
                TransactionController.IS_DEFAULT,
                this,
                (long[]) null);

		closeBulkFetchScan();

		// Set the "estimated" row count
		ScanController compressHeapSC = tc.openScan(
							newHeapConglom,
							false,
							TransactionController.OPENMODE_FORUPDATE,
							TransactionController.MODE_TABLE,
                            TransactionController.ISOLATION_SERIALIZABLE,
							(FormatableBitSet) null,
							(DataValueDescriptor[]) null,
							0,
							(Qualifier[][]) null,
							(DataValueDescriptor[]) null,
							0);
		
		compressHeapSC.setEstimatedRowCount(rowCount);

		compressHeapSC.close();
		compressHeapSC = null; // RESOLVE DJD CLEANUP

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

		// Update all indexes
		if (compressIRGs.length > 0)
		{
			updateAllIndexes(newHeapConglom, dd);
		}

		/* Update the DataDictionary
		 * RESOLVE - this will change in 1.4 because we will get
		 * back the same conglomerate number
		 */
		// Get the ConglomerateDescriptor for the heap
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
		long oldHeapConglom       = td.getHeapConglomerateId();
		ConglomerateDescriptor cd = 
            td.getConglomerateDescriptor(oldHeapConglom);

		// Update sys.sysconglomerates with new conglomerate #
		dd.updateConglomerateDescriptor(cd, newHeapConglom, tc);

        // Now that the updated information is available in the system tables,
        // we should invalidate all statements that use the old conglomerates
        dm.invalidateFor(td, DependencyManager.COMPRESS_TABLE, lcc);

		// Drop the old conglomerate
		tc.dropConglomerate(oldHeapConglom);
		cleanUp();
	}
	
	/* 
	 * TRUNCATE TABLE  TABLENAME; (quickly removes all the rows from table and
	 * it's correctponding indexes).
	 * Truncate is implemented by dropping the existing conglomerates(heap,indexes) and recreating a
	 * new ones  with the properties of dropped conglomerates. Currently Store
	 * does not have support to truncate existing conglomerated until store
	 * supports it , this is the only way to do it.
	 * Error Cases: Truncate error cases same as other DDL's statements except
	 * 1)Truncate is not allowed when the table is references by another table.
	 * 2)Truncate is not allowed when there are enabled delete triggers on the table.
	 * Note: Because conglomerate number is changed during recreate process all the statements will be
	 * marked as invalide and they will get recompiled internally on their next
	 * execution. This is okay because truncate makes the number of rows to zero
	 * it may be good idea to recompile them becuase plans are likely to be
	 * incorrect. Recompile is done internally by Derby, user does not have
	 * any effect.
	 */
    private void truncateTable()
		throws StandardException
	{
		ExecRow					emptyHeapRow;
		long					newHeapConglom;
		Properties				properties = new Properties();
		RowLocation				rl;

		if (SanityManager.DEBUG)
		{
			if (lockGranularity != '\0')
			{
				SanityManager.THROWASSERT(
					"lockGranularity expected to be '\0', not " + lockGranularity);
			}
			SanityManager.ASSERT(columnInfo == null,
				"columnInfo expected to be null");
			SanityManager.ASSERT(constraintActions == null,
				 "constraintActions expected to be null");
		}


        // Truncate table is not allowed if there are any tables referencing it.
        // except if it is self referencing, or if the constraint is deferred
        // and the ON DELETE action is NO ACTION.
        for(ConstraintDescriptor cd : dd.getConstraintDescriptors(td)) {
			if (cd instanceof ReferencedKeyConstraintDescriptor)
			{
                final ReferencedKeyConstraintDescriptor rfcd =
                    (ReferencedKeyConstraintDescriptor)cd;

                for (ConstraintDescriptor fkcd :
                     rfcd.getNonSelfReferencingFK(ConstraintDescriptor.ENABLED))
                {
                    final ForeignKeyConstraintDescriptor fk =
                            (ForeignKeyConstraintDescriptor)fkcd;

//IC see: https://issues.apache.org/jira/browse/DERBY-6668
                    throw StandardException.newException
                        ( SQLState.LANG_NO_TRUNCATE_ON_FK_REFERENCE_TABLE, td.getName() );
				}
			}
		}

		//truncate is not allowed when there are enabled DELETE triggers
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        for (TriggerDescriptor trd : dd.getTriggerDescriptors(td)) {
			if (trd.listensForEvent(TriggerDescriptor.TRIGGER_EVENT_DELETE) &&
				trd.isEnabled())
			{
				throw
					StandardException.newException(SQLState.LANG_NO_TRUNCATE_ON_ENABLED_DELETE_TRIGGERS,
												   td.getName(),trd.getName());	
			}
		}

		//gather information from the existing conglomerate to create new one.
//IC see: https://issues.apache.org/jira/browse/DERBY-2661
		emptyHeapRow = td.getEmptyExecRow();
		compressHeapCC = tc.openConglomerate(
								td.getHeapConglomerateId(),
                                false,
                                TransactionController.OPENMODE_FORUPDATE,
                                TransactionController.MODE_TABLE,
                                TransactionController.ISOLATION_SERIALIZABLE);

		rl = compressHeapCC.newRowLocationTemplate();
		// Get the properties on the old heap
		compressHeapCC.getInternalTablePropertySet(properties);
		compressHeapCC.close();
		compressHeapCC = null;

		//create new conglomerate
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
		newHeapConglom = 
            tc.createConglomerate(
                "heap",
                emptyHeapRow.getRowArray(),
                null, //column sort order - not required for heap
                td.getColumnCollationIds(),
                properties,
                TransactionController.IS_DEFAULT);
		
		/* Set up index info to perform truncate on them*/
//IC see: https://issues.apache.org/jira/browse/DERBY-4899
        getAffectedIndexes();
		if(numIndexes > 0)
		{
			indexRows = new ExecIndexRow[numIndexes];
			ordering  = new ColumnOrdering[numIndexes][];
			collation = new int[numIndexes][];

			for (int index = 0; index < numIndexes; index++)
			{
//IC see: https://issues.apache.org/jira/browse/DERBY-5530
                IndexRowGenerator curIndex = compressIRGs[index];
				// create a single index row template for each index
                indexRows[index] = curIndex.getIndexRowTemplate();
                curIndex.getIndexRow(emptyHeapRow, 
											  rl, 
											  indexRows[index],
											  (FormatableBitSet) null);
				/* For non-unique indexes, we order by all columns + the RID.
				 * For unique indexes, we just order by the columns.
				 * No need to try to enforce uniqueness here as
				 * index should be valid.
				 */
                int[] baseColumnPositions = curIndex.baseColumnPositions();

                boolean[] isAscending = curIndex.isAscending();

				int numColumnOrderings;
				numColumnOrderings = baseColumnPositions.length + 1;
				ordering[index]    = new ColumnOrdering[numColumnOrderings];
//IC see: https://issues.apache.org/jira/browse/DERBY-5530
                collation[index]   = curIndex.getColumnCollationIds(
                                                td.getColumnDescriptorList());

				for (int ii =0; ii < numColumnOrderings - 1; ii++) 
				{
					ordering[index][ii] = 
                        new IndexColumnOrder(ii, isAscending[ii]);
				}
				ordering[index][numColumnOrderings - 1] = 
                    new IndexColumnOrder(numColumnOrderings - 1);
			}
		}

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

		// truncate  all indexes
		if(numIndexes > 0)
		{
			long[] newIndexCongloms = new long[numIndexes];
			for (int index = 0; index < numIndexes; index++)
			{
				updateIndex(newHeapConglom, dd, index, newIndexCongloms);
			}
		}

		// Update the DataDictionary
		// Get the ConglomerateDescriptor for the heap
		long oldHeapConglom = td.getHeapConglomerateId();
		ConglomerateDescriptor cd = td.getConglomerateDescriptor(oldHeapConglom);

		// Update sys.sysconglomerates with new conglomerate #
		dd.updateConglomerateDescriptor(cd, newHeapConglom, tc);

        // Now that the updated information is available in the system tables,
        // we should invalidate all statements that use the old conglomerates
        dm.invalidateFor(td, DependencyManager.TRUNCATE_TABLE, lcc);

		// Drop the old conglomerate
		tc.dropConglomerate(oldHeapConglom);
		cleanUp();
	}


	/**
	 * Update all of the indexes on a table when doing a bulk insert
	 * on an empty table.
	 *
	 * @exception StandardException					thrown on error
	 */
	private void updateAllIndexes(long newHeapConglom, 
								  DataDictionary dd)
		throws StandardException
    {
		long[] newIndexCongloms = new long[numIndexes];

		/* Populate each index (one at a time or all at once). */
		if (sequential)
		{
			// First sorter populated during heap compression
			if (numIndexes >= 1)
			{
				updateIndex(newHeapConglom, dd, 0, newIndexCongloms);
			}
			for (int index = 1; index < numIndexes; index++)
			{
				// Scan heap and populate next sorter
				openBulkFetchScan(newHeapConglom);
				while (getNextRowFromRowSource() != null)
				{
					objectifyStreamingColumns();
					insertIntoSorter(index, compressRL[currentCompressRow - 1]);
				}
				updateIndex(newHeapConglom, dd, index, newIndexCongloms);
				closeBulkFetchScan();
			}
		}
		else
		{
			for (int index = 0; index < numIndexes; index++)
			{
				updateIndex(newHeapConglom, dd, index, newIndexCongloms);
			}
		}
	}

	private void updateIndex(
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
    long            newHeapConglom, 
    DataDictionary  dd,
    int             index, 
    long[]          newIndexCongloms)
		throws StandardException
	{
		Properties properties = new Properties();

		// Get the ConglomerateDescriptor for the index
		ConglomerateDescriptor cd = 
            td.getConglomerateDescriptor(indexConglomerateNumbers[index]);

		// Build the properties list for the new conglomerate
		ConglomerateController indexCC = 
            tc.openConglomerate(
                indexConglomerateNumbers[index],
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Get the properties on the old index
		indexCC.getInternalTablePropertySet(properties);

		/* Create the properties that language supplies when creating the
		 * the index.  (The store doesn't preserve these.)
		 */
		int indexRowLength = indexRows[index].nColumns();
		properties.put("baseConglomerateId", Long.toString(newHeapConglom));
		if (cd.getIndexDescriptor().isUnique())
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
			properties.put(
                "nUniqueColumns", Integer.toString(indexRowLength - 1));
		}
		else
		{
			properties.put(
                "nUniqueColumns", Integer.toString(indexRowLength));
		}
//IC see: https://issues.apache.org/jira/browse/DERBY-532
        if (  cd.getIndexDescriptor().isUniqueWithDuplicateNulls() &&
             !cd.getIndexDescriptor().hasDeferrableChecking() )
		{
			properties.put(
                "uniqueWithDuplicateNulls", Boolean.toString(true));
		}

		properties.put(
            "rowLocationColumn", Integer.toString(indexRowLength - 1));
		properties.put(
            "nKeyFields", Integer.toString(indexRowLength));

		indexCC.close();

		// We can finally drain the sorter and rebuild the index
		// Populate the index.
		
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        RowLocationRetRowSource cCount;
		boolean                 statisticsExist  = false;

		if (!truncateTable)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-2486
			sorters[index].completedInserts();
			sorters[index] = null;

			if (td.statisticsExist(cd))
			{
				cCount = 
                    new CardinalityCounter(
                            tc.openSortRowSource(sortIds[index]));

				statisticsExist = true;
			}
			else
            {
				cCount = 
                    new CardinalityCounter(
                            tc.openSortRowSource(sortIds[index]));
            }

            newIndexCongloms[index] = 
                tc.createAndLoadConglomerate(
                    "BTREE",
                    indexRows[index].getRowArray(),
                    ordering[index],
                    collation[index],
                    properties,
                    TransactionController.IS_DEFAULT,
                    cCount,
                    (long[]) null);

			//For an index, if the statistics already exist, then drop them.
			//The statistics might not exist for an index if the index was
			//created when the table was empty.
            //
            //For all alter table actions, including ALTER TABLE COMPRESS,
			//for both kinds of indexes (ie. one with preexisting statistics 
            //and with no statistics), create statistics for them if the table 
            //is not empty. 
			if (statisticsExist)
				dd.dropStatisticsDescriptors(td.getUUID(), cd.getUUID(), tc);
			
			long numRows;
			if ((numRows = ((CardinalityCounter)cCount).getRowCount()) > 0)
			{
				long[] c = ((CardinalityCounter)cCount).getCardinality();
				for (int i = 0; i < c.length; i++)
				{
					StatisticsDescriptor statDesc =
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
						new StatisticsDescriptor(
                            dd, 
                            dd.getUUIDFactory().createUUID(), 
                            cd.getUUID(), 
                            td.getUUID(), 
                            "I", 
                            new StatisticsImpl(numRows, c[i]), 
                            i + 1);

					dd.addDescriptor(
                            statDesc, 
                            null,   // no parent descriptor
							DataDictionary.SYSSTATISTICS_CATALOG_NUM,
							true,   // no error on duplicate.
                            tc);	
				}
			}
		}
        else
		{
            newIndexCongloms[index] = 
                tc.createConglomerate(
                    "BTREE",
                    indexRows[index].getRowArray(),
                    ordering[index],
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
                    collation[index],
                    properties,
                    TransactionController.IS_DEFAULT);


			//on truncate drop the statistics because we know for sure 
			//rowscount is zero and existing statistic will be invalid.
			if (td.statisticsExist(cd))
				dd.dropStatisticsDescriptors(td.getUUID(), cd.getUUID(), tc);
		}

		/* Update the DataDictionary
		 *
		 * Update sys.sysconglomerates with new conglomerate #, we need to
		 * update all (if any) duplicate index entries sharing this same
		 * conglomerate.
		 */
		dd.updateConglomerateDescriptor(
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
            td.getConglomerateDescriptors(indexConglomerateNumbers[index]),
            newIndexCongloms[index], 
            tc);

		// Drop the old conglomerate
		tc.dropConglomerate(indexConglomerateNumbers[index]);

	}


	/**
	 * Get info on the indexes on the table being compressed. 
	 *
	 * @exception StandardException		Thrown on error
	 */
    private void getAffectedIndexes()
		throws StandardException
	{
		IndexLister	indexLister = td.getIndexLister( );

		/* We have to get non-distinct index row generaters and conglom numbers
		 * here and then compress it to distinct later because drop column
		 * will need to change the index descriptor directly on each index
		 * entry in SYSCONGLOMERATES, on duplicate indexes too.
		 */
		compressIRGs = indexLister.getIndexRowGenerators();
		numIndexes = compressIRGs.length;
		indexConglomerateNumbers = indexLister.getIndexConglomerateNumbers();

		if (! (compressTable || truncateTable))		// then it's drop column
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
		    ArrayList<ConstantAction> newCongloms = new ArrayList<ConstantAction>();
			for (int i = 0; i < compressIRGs.length; i++)
			{
				int[] baseColumnPositions = compressIRGs[i].baseColumnPositions();
				int j;
				for (j = 0; j < baseColumnPositions.length; j++)
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
					if (baseColumnPositions[j] == droppedColumnPosition) break;
				if (j == baseColumnPositions.length)	// not related
					continue;
					
				if (baseColumnPositions.length == 1 || 
					(behavior == StatementType.DROP_CASCADE && compressIRGs[i].isUnique()))
				{
					numIndexes--;
					/* get first conglomerate with this conglom number each time
					 * and each duplicate one will be eventually all dropped
					 */
					ConglomerateDescriptor cd = td.getConglomerateDescriptor
												(indexConglomerateNumbers[i]);

					dropConglomerate(cd, td, true, newCongloms, activation,
						activation.getLanguageConnectionContext());

					compressIRGs[i] = null;		// mark it
					continue;
				}
				// give an error for unique index on multiple columns including
				// the column we are to drop (restrict), such index is not for
				// a constraint, because constraints have already been handled
				if (compressIRGs[i].isUnique())
				{
					ConglomerateDescriptor cd = td.getConglomerateDescriptor
												(indexConglomerateNumbers[i]);
					throw StandardException.newException(SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT,
										dm.getActionString(DependencyManager.DROP_COLUMN),
										columnInfo[0].name, "UNIQUE INDEX",
										cd.getConglomerateName() );
				}
			}

			/* If there are new backing conglomerates which must be
			 * created to replace a dropped shared conglomerate
			 * (where the shared conglomerate was dropped as part
			 * of a "drop conglomerate" call above), then create
			 * them now.  We do this *after* dropping all dependent
			 * conglomerates because we don't want to waste time
			 * creating a new conglomerate if it's just going to be
			 * dropped again as part of another "drop conglomerate"
			 * call.
			 */
            createNewBackingCongloms(newCongloms, indexConglomerateNumbers);
//IC see: https://issues.apache.org/jira/browse/DERBY-4899

			IndexRowGenerator[] newIRGs = new IndexRowGenerator[numIndexes];
			long[] newIndexConglomNumbers = new long[numIndexes];
			collation = new int[numIndexes][]; 
//IC see: https://issues.apache.org/jira/browse/DERBY-6890

			for (int i = 0, j = 0; i < numIndexes; i++, j++)
			{
				while (compressIRGs[j] == null)
					j++;

				// Setup collation id array to be passed in on call to create index.
				collation[i] = 
					compressIRGs[j].getColumnCollationIds(
						td.getColumnDescriptorList());

				int[] baseColumnPositions = compressIRGs[j].baseColumnPositions();
				newIRGs[i] = compressIRGs[j];
				newIndexConglomNumbers[i] = indexConglomerateNumbers[j];

				boolean[] isAscending = compressIRGs[j].isAscending();
				boolean reMakeArrays = false;
				boolean rewriteBaseColumnPositions = false;
				int size = baseColumnPositions.length;
				for (int k = 0; k < size; k++)
				{
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
					if (baseColumnPositions[k] > droppedColumnPosition)
                    {
						baseColumnPositions[k]--;
//IC see: https://issues.apache.org/jira/browse/DERBY-3177
                        rewriteBaseColumnPositions = true;
                    }
					else if (baseColumnPositions[k] == droppedColumnPosition)
					{
						baseColumnPositions[k] = 0;		// mark it
						reMakeArrays = true;
					}
				}

                if ( rewriteBaseColumnPositions )
                {
                    compressIRGs[j].setBaseColumnPositions( baseColumnPositions );
                }
                
				if (reMakeArrays)
				{
					size--;
					int[] newBCP = new int[size];
					boolean[] newIsAscending = new boolean[size];
					int[] newCollation = new int[collation[i].length - 1];
					for (int k = 0, step = 0; k < size; k++)
					{
						if (step == 0 && baseColumnPositions[k + step] == 0)
							step++;
						newBCP[k] = baseColumnPositions[k + step];
						newIsAscending[k] = isAscending[k + step];
//IC see: https://issues.apache.org/jira/browse/DERBY-6890
						newCollation[k] = collation[i][k + step];
					}
					IndexDescriptor id = compressIRGs[j].getIndexDescriptor();
					id.setBaseColumnPositions(newBCP);
					id.setIsAscending(newIsAscending);
					id.setNumberOfOrderedColumns(id.numberOfOrderedColumns() - 1);
					collation[i] = newCollation;
				}
			}
			compressIRGs = newIRGs;
			indexConglomerateNumbers = newIndexConglomNumbers;
		}
		else
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-3352
			collation = new int[numIndexes][]; 
			for (int i = 0; i < numIndexes; i++)
			{
				collation[i] = 
					compressIRGs[i].getColumnCollationIds(
						td.getColumnDescriptorList());
			}
		}

		/* Now we are done with updating each index descriptor entry directly
		 * in SYSCONGLOMERATES (for duplicate index as well), from now on, our
		 * work should apply ONLY once for each real conglomerate, so we
		 * compress any duplicate indexes now.
		 */
		Object[] compressIndexResult = 
            compressIndexArrays(indexConglomerateNumbers, compressIRGs);

		if (compressIndexResult != null)
		{
			indexConglomerateNumbers = (long[]) compressIndexResult[1];
			compressIRGs = (IndexRowGenerator[]) compressIndexResult[2];
			numIndexes = indexConglomerateNumbers.length;
		}

		indexedCols = new FormatableBitSet(compressTable || truncateTable ? td.getNumberOfColumns() + 1 :
												  td.getNumberOfColumns());
		for (int index = 0; index < numIndexes; index++)
		{
			int[] colIds = compressIRGs[index].getIndexDescriptor().baseColumnPositions();

			for (int index2 = 0; index2 < colIds.length; index2++)
			{
				indexedCols.set(colIds[index2]);
			}
		}
	}

	/**
	 * Iterate through the received list of CreateIndexConstantActions and
	 * execute each one, It's possible that one or more of the constant
	 * actions in the list has been rendered "unneeded" by the time we get
	 * here (because the index that the constant action was going to create
	 * is no longer needed), so we have to check for that.
	 *
	 * @param newConglomActions Potentially empty list of constant actions
	 *   to execute, if still needed
	 * @param ixCongNums Optional array of conglomerate numbers; if non-null
	 *   then any entries in the array which correspond to a dropped physical
	 *   conglomerate (as determined from the list of constant actions) will
	 *   be updated to have the conglomerate number of the newly-created
	 *   physical conglomerate.
	 */
	private void createNewBackingCongloms(ArrayList<ConstantAction> newConglomActions,
//IC see: https://issues.apache.org/jira/browse/DERBY-4899
                                          long [] ixCongNums)
		throws StandardException
	{
		int sz = newConglomActions.size();
		for (int i = 0; i < sz; i++)
		{
			CreateIndexConstantAction ca =
				(CreateIndexConstantAction)newConglomActions.get(i);

			if (dd.getConglomerateDescriptor(ca.getCreatedUUID()) == null)
			{
				/* Conglomerate descriptor was dropped after
				 * being selected as the source for a new
				 * conglomerate, so don't create the new
				 * conglomerate after all.  Either we found
				 * another conglomerate descriptor that can
				 * serve as the source for the new conglom,
				 * or else we don't need a new conglomerate
				 * at all because all constraints/indexes
				 * which shared it had a dependency on the
				 * dropped column and no longer exist.
				 */
				continue;
			}

			executeConglomReplacement(ca, activation);
			long oldCongNum = ca.getReplacedConglomNumber();
			long newCongNum = ca.getCreatedConglomNumber();

			/* The preceding call to executeConglomReplacement updated all
			 * relevant ConglomerateDescriptors with the new conglomerate
			 * number *WITHIN THE DATA DICTIONARY*.  But the table
			 * descriptor that we have will not have been updated.
			 * There are two approaches to syncing the table descriptor
			 * with the dictionary: 1) refetch the table descriptor from
			 * the dictionary, or 2) update the table descriptor directly.
			 * We choose option #2 because the caller of this method (esp.
			 * getAffectedIndexes()) has pointers to objects from the
			 * table descriptor as it was before we entered this method.
			 * It then changes data within those objects, with the
			 * expectation that, later, those objects can be used to
			 * persist the changes to disk.  If we change the table
			 * descriptor here the objects that will get persisted to
			 * disk (from the table descriptor) will *not* be the same
			 * as the objects that were updated--so we'll lose the updates
			 * and that will in turn cause other problems.  So we go with
			 * option #2 and just change the existing TableDescriptor to
			 * reflect the fact that the conglomerate number has changed.
			 */
			ConglomerateDescriptor [] tdCDs =
				td.getConglomerateDescriptors(oldCongNum);

			for (int j = 0; j < tdCDs.length; j++)
				tdCDs[j].setConglomerateNumber(newCongNum);

			/* If we received a list of index conglomerate numbers
			 * then they are the "old" numbers; see if any of those
			 * numbers should now be updated to reflect the new
			 * conglomerate, and if so, update them.
			 */
			if (ixCongNums != null)
			{
				for (int j = 0; j < ixCongNums.length; j++)
				{
					if (ixCongNums[j] == oldCongNum)
						ixCongNums[j] = newCongNum;
				}
			}
		}
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
		ordering        = new ColumnOrdering[numIndexes][];
		needToDropSort  = new boolean[numIndexes];
		sortIds         = new long[numIndexes];

		/* For each index, build a single index row and a sorter. */
		for (int index = 0; index < numIndexes; index++)
		{
			// create a single index row template for each index
			indexRows[index] = compressIRGs[index].getIndexRowTemplate();


			// Get an index row based on the base row
			// (This call is only necessary here because we need to pass a 
            // template to the sorter.)
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
			compressIRGs[index].getIndexRow(
                sourceRow, rl, indexRows[index], (FormatableBitSet) null);

			/* For non-unique indexes, we order by all columns + the RID.
			 * For unique indexes, we just order by the columns.
			 * No need to try to enforce uniqueness here as
			 * index should be valid.
			 */
			int[]       baseColumnPositions = 
                compressIRGs[index].baseColumnPositions();
			boolean[]   isAscending         = 
                compressIRGs[index].isAscending();
			int         numColumnOrderings  = 
                baseColumnPositions.length + 1;

			/* We can only reuse the wrappers when doing an
			 * external sort if there is only 1 index.  Otherwise,
			 * we could get in a situation where 1 sort reuses a
			 * wrapper that is still in use in another sort.
			 */
			boolean reuseWrappers = (numIndexes == 1);

			SortObserver    sortObserver = 
                new BasicSortObserver(
                        false, false, indexRows[index], reuseWrappers);

			ordering[index] = new ColumnOrdering[numColumnOrderings];
			for (int ii =0; ii < numColumnOrderings - 1; ii++) 
			{
				ordering[index][ii] = new IndexColumnOrder(ii, isAscending[ii]);
			}
			ordering[index][numColumnOrderings - 1] = 
                new IndexColumnOrder(numColumnOrderings - 1);

			// create the sorters
			sortIds[index] = 
                tc.createSort(
                   (Properties)null, 
                    indexRows[index].getRowArrayClone(),
                    ordering[index],
                    sortObserver,
                    false,			        // not in order
                    estimatedRowCount,		// est rows	
                    -1				        // est row size, -1 means no idea	
                    );
		}
	
        sorters = new SortController[numIndexes];

		// Open the sorts
		for (int index = 0; index < numIndexes; index++)
		{
			sorters[index] = tc.openSort(sortIds[index]);
			needToDropSort[index] = true;
		}
	}

	// RowSource interface
	
	/** 
	 * @see RowSource#getValidColumns
	 */
	public FormatableBitSet getValidColumns()
	{
		// All columns are valid
		return null;
	}
	
	/** 
	 * @see RowSource#getNextRowFromRowSource
	 * @exception StandardException on error
	 */
	public DataValueDescriptor[] getNextRowFromRowSource()
		throws StandardException
	{
		currentRow = null;
		// Time for a new bulk fetch?
		if ((! doneScan) &&
			(currentCompressRow == bulkFetchSize || !validRow[currentCompressRow]))
		{
            int bulkFetched =
                    compressHeapGSC.fetchNextGroup(baseRowArray, compressRL);
//IC see: https://issues.apache.org/jira/browse/DERBY-673

			doneScan = (bulkFetched != bulkFetchSize);
			currentCompressRow = 0;
			rowCount += bulkFetched;
			for (int index = 0; index < bulkFetched; index++)
			{
				validRow[index] = true;
			}
			for (int index = bulkFetched; index < bulkFetchSize; index++)
			{
				validRow[index] = false;
			}
		}

		if (validRow[currentCompressRow])
		{
			if (compressTable)
            {
				currentRow = baseRow[currentCompressRow];
            }
			else
			{
				if (currentRow == null)
                {
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
					currentRow = 
                        activation.getExecutionFactory().getValueRow(
                            baseRowArray[currentCompressRow].length - 1);
                }

				for (int i = 0; i < currentRow.nColumns(); i++)
				{
					currentRow.setColumn(
                        i + 1, 
                        i < droppedColumnPosition - 1 ?
                            baseRow[currentCompressRow].getColumn(i+1) :
                            baseRow[currentCompressRow].getColumn(i+1+1));
				}
			}
			currentCompressRow++;
		}

 		if (currentRow != null)
		{
			/* Let the target preprocess the row.  For now, this
			 * means doing an in place clone on any indexed columns
			 * to optimize cloning and so that we don't try to drain
			 * a stream multiple times.
			 */
			if (compressIRGs.length > 0)
			{
				/* Do in-place cloning of all of the key columns */
				currentRow =  currentRow.getClone(indexedCols);
			}

			return currentRow.getRowArray();
		}

		return null;
	}

	/**
	 * @see RowSource#needsToClone
	 */
	public boolean needsToClone()
	{
		return(true);
	}

	/** 
	 * @see RowSource#closeRowSource
	 */
	public void closeRowSource()
	{
		// Do nothing here - actual work will be done in close()
	}


	// RowLocationRetRowSource interface

	/**
	 * @see RowLocationRetRowSource#needsRowLocation
	 */
	public boolean needsRowLocation()
	{
		// Only true if table has indexes
		return (numIndexes > 0);
	}

    public boolean needsRowLocationForDeferredCheckConstraints()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-532
        return false;
    }

    /**
	 * @see RowLocationRetRowSource#rowLocation
	 * @exception StandardException on error
	 */
	public void rowLocation(RowLocation rl)
		throws StandardException
	{
		/* Set up sorters, etc. if 1st row and there are indexes */
		if (compressIRGs.length > 0)
		{
			objectifyStreamingColumns();

			/* Put the row into the indexes.  If sequential, 
			 * then we only populate the 1st sorter when compressing
			 * the heap.
			 */
			int maxIndex = compressIRGs.length;
			if (maxIndex > 1 && sequential)
			{
				maxIndex = 1;
			}
			for (int index = 0; index < maxIndex; index++)
			{
				insertIntoSorter(index, rl);
			}
		}
	}

	private void objectifyStreamingColumns()
		throws StandardException
	{
		// Objectify any the streaming columns that are indexed.
		for (int i = 0; i < currentRow.getRowArray().length; i++)
		{
			/* Object array is 0-based,
			 * indexedCols is 1-based.
			 */
			if (! indexedCols.get(i + 1))
			{
				continue;
			}

			if (currentRow.getRowArray()[i] instanceof StreamStorable)
			{
//IC see: https://issues.apache.org/jira/browse/DERBY-673
                currentRow.getRowArray()[i].getObject();
			}
		}
	}

	private void insertIntoSorter(int index, RowLocation rl)
		throws StandardException
	{
		// Get a new object Array for the index
		indexRows[index].getNewObjectArray();
		// Associate the index row with the source row
		compressIRGs[index].getIndexRow(currentRow, 
//IC see: https://issues.apache.org/jira/browse/DERBY-4520
                                        (RowLocation) rl.cloneValue(false),
										indexRows[index],
										(FormatableBitSet) null);

		// Insert the index row into the matching sorter
		sorters[index].insert(indexRows[index].getRowArray());
	}

	/**
	 *
	 * @exception StandardException		Thrown on error
	 */
	private void	cleanUp() throws StandardException
	{
		if (compressHeapCC != null)
		{
			compressHeapCC.close();
			compressHeapCC = null;
		}

		if (compressHeapGSC != null)
		{
			closeBulkFetchScan();
		}

		// Close each sorter
		if (sorters != null)
		{
			for (int index = 0; index < compressIRGs.length; index++)
			{
				if (sorters[index] != null)
				{
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
	}

	// class implementation

	/**
	 * Return the "semi" row count of a table.  We are only interested in
	 * whether the table has 0, 1 or &gt; 1 rows.
	 *
	 *
	 * @return Number of rows (0, 1 or &gt; 1) in table.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	private int getSemiRowCount(TransactionController tc)
		throws StandardException
	{
		int			   numRows = 0;

		ScanController sc = tc.openScan(td.getHeapConglomerateId(),
						 false,	// hold
						 0,	    // open read only
                         TransactionController.MODE_TABLE,
                         TransactionController.ISOLATION_SERIALIZABLE,
						 RowUtil.EMPTY_ROW_BITSET, // scanColumnList
						 null,	// start position
						 ScanController.GE,      // startSearchOperation
						 null, // scanQualifier
						 null, //stop position - through last row
						 ScanController.GT);     // stopSearchOperation

		while (sc.next())
		{
			numRows++;

			// We're only interested in whether the table has 0, 1 or > 1 rows
			if (numRows == 2)
			{
				break;
			}
		}
		sc.close();

		return numRows;
	}

	/**
	 * Update a new column with its default.
	 * We could do the scan ourself here, but
	 * instead we get a nested connection and
	 * issue the appropriate update statement.
	 *
	 * @param columnDescriptor  catalog descriptor for the column
	 *
	 * @exception StandardException if update to default fails
	 */
    private void updateNewColumnToDefault(ColumnDescriptor columnDescriptor)
//IC see: https://issues.apache.org/jira/browse/DERBY-4899
            throws StandardException {
        String  columnName = columnDescriptor.getColumnName();

		/* Need to use delimited identifiers for all object names
		 * to ensure correctness.
		 */
        String updateStmt = "UPDATE " +
                IdUtil.mkQualifiedName(td.getSchemaName(), td.getName()) +
                " SET " + IdUtil.normalToDelimited(columnName) + "=DEFAULT";


		AlterTableConstantAction.executeUpdate(lcc, updateStmt);
	}

	private static void executeUpdate(LanguageConnectionContext lcc, String updateStmt) throws StandardException
	{
		PreparedStatement ps = lcc.prepareInternalStatement(updateStmt);

        // This is a substatement; for now, we do not set any timeout
        // for it. We might change this behaviour later, by linking
        // timeout to its parent statement's timeout settings.
//IC see: https://issues.apache.org/jira/browse/DERBY-3897
		ResultSet rs = ps.executeSubStatement(lcc, true, 0L);
		rs.close();
	}

	/**
	 * computes the minimum/maximum value in a column of a table.
	 */
    private long getColumnMax(TableDescriptor td, String columnName,
//IC see: https://issues.apache.org/jira/browse/DERBY-4899
                              long increment)
            throws StandardException {
		String maxStr = (increment > 0) ? "MAX" : "MIN";
//IC see: https://issues.apache.org/jira/browse/DERBY-5157
        String maxStmt = "SELECT  " + maxStr + "(" +
                IdUtil.normalToDelimited(columnName) + ") FROM " +
                IdUtil.mkQualifiedName(td.getSchemaName(), td.getName());

		PreparedStatement ps = lcc.prepareInternalStatement(maxStmt);

        // This is a substatement, for now we do not set any timeout for it
        // We might change this later by linking timeout to parent statement
//IC see: https://issues.apache.org/jira/browse/DERBY-3897
		ResultSet rs = ps.executeSubStatement(lcc, false, 0L);
		DataValueDescriptor[] rowArray = rs.getNextRow().getRowArray();
		rs.close();
		rs.finish();

		return rowArray[0].getLong();
	}					

	private void openBulkFetchScan(long heapConglomNumber)
		throws StandardException
	{
		doneScan = false;
		compressHeapGSC = tc.openGroupFetchScan(
                            heapConglomNumber,
							false,	// hold
							0,	// open base table read only
                            TransactionController.MODE_TABLE,
                            TransactionController.ISOLATION_SERIALIZABLE,
							null,    // all fields as objects
							(DataValueDescriptor[]) null,	// startKeyValue
							0,		// not used when giving null start posn.
							null,	// qualifier
							(DataValueDescriptor[]) null,	// stopKeyValue
							0);		// not used when giving null stop posn.
	}

	private void closeBulkFetchScan()
		throws StandardException
	{
		compressHeapGSC.close();
		compressHeapGSC = null;
	}

	/**
	 * Make sure that the columns are non null
	 * If any column is nullable, check that the data is null.
	 *
	 * @param	columnNames	names of columns to be checked
	 * @param	nullCols	true if corresponding column is nullable
	 * @param	numRows		number of rows in the table
	 * @param	lcc		language context
	 * @param	errorMsg	error message to use for exception
	 *
	 * @return true if any nullable columns found (nullable columns must have
	 *		all non null data or exception is thrown
	 * @exception StandardException on error 
	 */
	private boolean validateNotNullConstraint
	(
		String							columnNames[],
		boolean							nullCols[],
		int								numRows,
		LanguageConnectionContext		lcc,
		String							errorMsg
	)
		throws StandardException
	{
		boolean foundNullable = false;
        StringBuilder constraintText = new StringBuilder();
//IC see: https://issues.apache.org/jira/browse/DERBY-673

		/* 
		 * Check for nullable columns and create a constraint string which can
		 * be used in validateConstraint to check whether any of the
		 * data is null.  
		 */
		for (int colCtr = 0; colCtr < columnNames.length; colCtr++)
		{
			ColumnDescriptor cd = td.getColumnDescriptor(columnNames[colCtr]);

			if (cd == null)
			{
				throw StandardException.newException(SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE,
														columnNames[colCtr],
														td.getName());
			}

			if (cd.getType().isNullable())
			{
				if (numRows > 0)
				{
					// already found a nullable column so add "AND" 
					if (foundNullable)
						constraintText.append(" AND ");
					// Delimiting the column name is important in case the
					// column name uses lower case characters, spaces, or
					// other unusual characters.
					constraintText.append(
//IC see: https://issues.apache.org/jira/browse/DERBY-673
                        IdUtil.normalToDelimited(columnNames[colCtr]));
                    constraintText.append(" IS NOT NULL ");
				}
				foundNullable = true;
				nullCols[colCtr] = true;
			}
		}

		/* if the table has nullable columns and isn't empty 
		 * we need to validate the data
		 */
		if (foundNullable && numRows > 0)
		{
			if (!ConstraintConstantAction.validateConstraint(
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                    (String) null,
                    constraintText.toString(),
                    null, /* not used for not nullable constraints yet */
                    td,
                    lcc,
                    false,
                    false /* not used for not nullable constraints yet */))
			{	
//IC see: https://issues.apache.org/jira/browse/DERBY-3523
				if (errorMsg.equals(SQLState.LANG_NULL_DATA_IN_PRIMARY_KEY_OR_UNIQUE_CONSTRAINT))
				{	//alter table add primary key
					 //soft upgrade mode
					throw StandardException.newException(
						SQLState.LANG_NULL_DATA_IN_PRIMARY_KEY_OR_UNIQUE_CONSTRAINT, 
						td.getQualifiedName());
				}
				else if (errorMsg.equals(SQLState.LANG_NULL_DATA_IN_PRIMARY_KEY)) 
				{	//alter table add primary key
					throw StandardException.newException(
						SQLState.LANG_NULL_DATA_IN_PRIMARY_KEY,
						td.getQualifiedName());
				}
				else 
				{	//alter table modify column not null
					throw StandardException.newException(
						SQLState.LANG_NULL_DATA_IN_NON_NULL_COLUMN, 
						td.getQualifiedName(), columnNames[0]);
				}
			}
		}
		return foundNullable;
	}

	/**
	 * Get rid of duplicates from a set of index conglomerate numbers and
	 * index descriptors.
	 *
	 * @param	indexCIDS	array of index conglomerate numbers
	 * @param	irgs		array of index row generaters
	 *
	 * @return value:		If no duplicates, returns NULL; otherwise,
	 *						a size-3 array of objects, first element is an
	 *						array of duplicates' indexes in the input arrays;
	 *						second element is the compact indexCIDs; third
	 *						element is the compact irgs.
	 */
	private Object[] compressIndexArrays(
								long[] indexCIDS,
								IndexRowGenerator[] irgs)
	{
		/* An efficient way to compress indexes.  From one end of workSpace,
		 * we save unique conglom IDs; and from the other end we save
		 * duplicate indexes' indexes.  We save unique conglom IDs so that
		 * we can do less amount of comparisons.  This is efficient in
		 * space as well.  No need to use hash table.
		 */
		long[] workSpace = new long[indexCIDS.length];
		int j = 0, k = indexCIDS.length - 1;
		for (int i = 0; i < indexCIDS.length; i++)
		{
			int m;
			for (m = 0; m < j; m++)		// look up our unique set
			{
				if (indexCIDS[i] == workSpace[m])	// it's a duplicate
				{
					workSpace[k--] = i;		// save dup index's index
					break;
				}
			}
			if (m == j)
				workSpace[j++] = indexCIDS[i];	// save unique conglom id
		}
		if (j < indexCIDS.length)		// duplicate exists
		{
			long[] newIndexCIDS = new long[j];
			IndexRowGenerator[] newIrgs = new IndexRowGenerator[j];
			int[] duplicateIndexes = new int[indexCIDS.length - j];
			k = 0;
			// do everything in one loop
			for (int m = 0, n = indexCIDS.length - 1; m < indexCIDS.length; m++)
			{
				// we already gathered our indexCIDS and duplicateIndexes
				if (m < j)
					newIndexCIDS[m] = workSpace[m];
				else
					duplicateIndexes[indexCIDS.length - m - 1] = (int) workSpace[m];

				// stack up our irgs, indexSCOCIs, indexDCOCIs
				if ((n >= j) && (m == (int) workSpace[n]))
					n--;
				else
				{
					newIrgs[k] = irgs[m];
					k++;
				}
			}

			// construct return value
			Object[] returnValue = new Object[3]; // [indexSCOCIs == null ? 3 : 5];
			returnValue[0] = duplicateIndexes;
			returnValue[1] = newIndexCIDS;
			returnValue[2] = newIrgs;
			return returnValue;
		}
		else		// no duplicates
			return null;
	}

    public void offendingRowLocation(
//IC see: https://issues.apache.org/jira/browse/DERBY-532
            RowLocation rl, long containdId) throws StandardException {
        if (SanityManager.DEBUG) {
            SanityManager.NOTREACHED();
        }
    }

}
