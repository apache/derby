/*

   Derby - Class org.apache.derby.impl.sql.execute.AlterTableConstantAction

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

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.StreamStorable;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptorList;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import	org.apache.derby.iapi.sql.dictionary.ConstraintDescriptorList;
import org.apache.derby.iapi.sql.dictionary.ReferencedKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.DefaultDescriptor;
import org.apache.derby.iapi.sql.dictionary.IndexLister;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.StatisticsDescriptor;
import org.apache.derby.iapi.sql.dictionary.DependencyDescriptor;
import org.apache.derby.iapi.sql.dictionary.CheckConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.GenericDescriptorList;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;
import org.apache.derby.impl.sql.catalog.DDColumnDependableFinder;

import org.apache.derby.iapi.sql.StatementType;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.depend.Dependency;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.iapi.sql.depend.ProviderInfo;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;

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

import org.apache.derby.iapi.types.NumberDataValue;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.Statement;
import org.apache.derby.iapi.sql.PreparedStatement;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.IndexDescriptor;
import org.apache.derby.catalog.DependableFinder;

import org.apache.derby.catalog.types.DefaultInfoImpl;
import org.apache.derby.catalog.types.StatisticsImpl;
import org.apache.derby.catalog.types.ReferencedColumnsDescriptorImpl;

import java.sql.SQLException;
import java.util.Properties;
import java.util.Enumeration;

import org.apache.derby.iapi.services.io.FormatableBitSet;

import java.util.List;
import java.util.Iterator;

/**
 *	This class  describes actions that are ALWAYS performed for an
 *	ALTER TABLE Statement at Execution time.
 *
 *	@author Jerry Brenner.
 */

class AlterTableConstantAction extends DDLSingleTableConstantAction
 implements RowLocationRetRowSource
{

	protected	SchemaDescriptor			sd;
	protected	String						tableName;
	protected	UUID						schemaId;
	protected	int							tableType;
	protected	long						tableConglomerateId;
	protected	ColumnInfo[]				columnInfo;
	protected	ConstraintConstantAction[]	constraintActions;
	protected	char						lockGranularity;
	private	boolean					compressTable;
	private	boolean					sequential;
	private int						behavior;

	// Alter table compress and Drop column
	private boolean					doneScan;
	private boolean[]				needToDropSort;
	private boolean[]				validRow;
	private	int						bulkFetchSize = 16;
	private	int						currentCompressRow;
	private int						numIndexes;
	private int						rowCount;
	private long					estimatedRowCount;
	private long[]					indexConglomerateNumbers;
	private	long[]					sortIds;
	private FormatableBitSet					indexedCols;
	private ConglomerateController	compressHeapCC;
	private ExecIndexRow[]			indexRows;
	private ExecRow[]				baseRow;
	private ExecRow					currentRow;
	private	GroupFetchScanController compressHeapGSC;
	private IndexRowGenerator[]		compressIRGs;
	private	DataValueDescriptor[][]			baseRowArray;
	private RowLocation[]			compressRL;
	private SortController[]		sorters;
	private int						columnPosition;
	private ColumnOrdering[][]		ordering;

	private	TableDescriptor 		td;


	//truncate table
	private boolean                 truncateTable;

	// CONSTRUCTORS
		private LanguageConnectionContext lcc;
		private DataDictionary dd;
		private DependencyManager dm;
		private TransactionController tc;
		private Activation activation;

	/**
	 *	Make the AlterAction for an ALTER TABLE statement.
	 *
	 *  @param sd			descriptor for the schema that table lives in.
	 *  @param tableName	Name of table.
	 *	@param tableId		UUID of table
	 *	@param tableConglomerateId	heap conglomerate number of table
	 *  @param tableType	Type of table (e.g., BASE).
	 *  @param columnInfo	Information on all the columns in the table.
	 *  @param constraintActions	ConstraintConstantAction[] for constraints
	 * @param lockGranularity	The lock granularity.
	 *	@param compressTable	Whether or not this is a compress table
	 *	@param behavior		drop behavior for dropping column
	 *	@param sequential	If compress table/drop column, whether or not sequential
	 *  @param truncateTable	Whether or not this is a truncate table
	 */
	AlterTableConstantAction(
								SchemaDescriptor	sd,
								String			tableName,
								UUID			tableId,
								long			tableConglomerateId,
								int				tableType,
								ColumnInfo[]	columnInfo,
								ConstraintConstantAction[] constraintActions,
								char			lockGranularity,
								boolean			compressTable,
								int				behavior,
								boolean			sequential,
								boolean         truncateTable)
	{
		super(tableId);
		this.sd = sd;
		this.tableName = tableName;
		this.tableConglomerateId = tableConglomerateId;
		this.tableType = tableType;
		this.columnInfo = columnInfo;
		this.constraintActions = constraintActions;
		this.lockGranularity = lockGranularity;
		this.compressTable = compressTable;
		this.behavior = behavior;
		this.sequential = sequential;
		this.truncateTable = truncateTable;

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
	 *	This is the guts of the Execution-time logic for ALTER TABLE.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		TransactionController tc = lcc.getTransactionExecute();

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
		// SYSCOLUMNS etc datadictionary tables (track 879) and phantom table
		// descriptor, in which case table shape could be changed by a
		// concurrent thread doing add/drop column (track 3804 and 3825)

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

		if(truncateTable)
			dm.invalidateFor(td, DependencyManager.TRUNCATE_TABLE, lcc);
		else
			dm.invalidateFor(td, DependencyManager.ALTER_TABLE, lcc);
		execGuts( activation );
	}


	/**
	  *	Wrapper for this DDL action. Factored out so that our child, 
	  * RepAlterTableConstantAction
	  *	could enjoy the benefits of the startWriting() method above.
	  *
	  *	@param	dd			the data dictionary
	  *
	  * @exception StandardException		Thrown on failure
	  */
	public void	execGuts( Activation activation)
						throws StandardException
	{
		ColumnDescriptor			columnDescriptor;
		int							numRows = 0;
		boolean						tableNeedsScanning = false;
		boolean						tableScanned = false;

		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		TransactionController tc = lcc.getTransactionExecute();

		// Save the TableDescriptor off in the Activation
		activation.setDDLTableDescriptor(td);

		/*
		** If the schema descriptor is null, then
		** we must have just read ourselves in.  
		** So we will get the corresponding schema
		** descriptor from the data dictionary.
		*/
		if (sd == null)
		{
			sd = getAndCheckSchemaDescriptor(dd, schemaId, "ALTER TABLE");
		}
		

		/* Prepare all dependents to invalidate.  (This is there chance
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
				// Don't allow user to add non-nullable column to non-empty table
				if (numRows > 0)
				{
					throw StandardException.newException(SQLState.LANG_ADDING_NON_NULL_COLUMN_TO_NON_EMPTY_TABLE, 
									td.getQualifiedName());
				}
				tableScanned = true;
			}

			// for each related column, stuff system.column
			for (int ix = 0; ix < columnInfo.length; ix++)
			{
				ColumnDescriptorList cdl = new ColumnDescriptorList();

				/* If there is a default value, use it, otherwise use null */
				
				// Are we adding a new column or modifying a default?
				
				if (columnInfo[ix].action == ColumnInfo.CREATE)
				{
					addNewColumnToTable(activation, ix);
				}
				else if (columnInfo[ix].action == 
						 ColumnInfo.MODIFY_COLUMN_DEFAULT)
				{
					modifyColumnDefault(activation, ix);
				}
				else if (columnInfo[ix].action == 
						 ColumnInfo.MODIFY_COLUMN_TYPE)
				{
					modifyColumnType(activation, ix);
				}
				else if (columnInfo[ix].action == 
						 ColumnInfo.MODIFY_COLUMN_CONSTRAINT)
				{
					modifyColumnConstraint(activation, columnInfo[ix].name, true);
				}
				else if (columnInfo[ix].action == 
						 ColumnInfo.MODIFY_COLUMN_CONSTRAINT_NOT_NULL)
				{
					if (! tableScanned)
					{
						tableScanned = true;
						numRows = getSemiRowCount(tc);
					}
					// check that the data in the column is not null
					String colNames[] = new String[1];
					colNames[0] = columnInfo[ix].name;
					boolean nullCols[] = new boolean[1];

					/* note validateNotNullConstraint returns true if the
					 * column is nullable
					 */
					if (validateNotNullConstraint(colNames, nullCols, 
							numRows, lcc, SQLState.LANG_NULL_DATA_IN_NON_NULL_COLUMN))
					{
						/* nullable column - modify it to be not null
						 * This is O.K. at this point since we would have
						 * thrown an exception if any data was null
						 */
						modifyColumnConstraint(activation, columnInfo[ix].name, false);
					}
				}
				else if (columnInfo[ix].action == ColumnInfo.DROP)
				{
					dropColumnFromTable(activation, ix);
				}
				else if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT(
							  "Unexpected action in AlterTableConstantAction");
				}
			}
		}

		/* Create/Drop any constraints */
		if (constraintActions != null)
		{
			for (int conIndex = 0; conIndex < constraintActions.length; conIndex++)
			{
				ConstraintConstantAction cca = constraintActions[conIndex];

				if (cca instanceof CreateConstraintConstantAction)
				{
					int constraintType = cca.getConstraintType();

					/* Some constraint types require special checking:
					 *		Check		- table must be empty, for now
					 *		Primary Key - table cannot already have a primary key
					 */
					switch (constraintType)
					{
						case DataDictionary.PRIMARYKEY_CONSTRAINT:
							// Check to see if a constraint of the same type already exists
							ConstraintDescriptorList cdl = dd.getConstraintDescriptors(td);
							if (cdl.getPrimaryKey() != null)
							{
								throw StandardException.newException(SQLState.LANG_ADD_PRIMARY_KEY_FAILED1, 
											td.getQualifiedName());
							}
							if (! tableScanned)
							{
								tableScanned = true;
								numRows = getSemiRowCount(tc);
							}

							break;
						case DataDictionary.CHECK_CONSTRAINT:
							if (! tableScanned)
							{
								tableScanned = true;
								numRows = getSemiRowCount(tc);
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
											td,
											lcc, true);
							}
							break;
					}
				}
				else
				{
					if (SanityManager.DEBUG)
					{
						if (!(cca instanceof DropConstraintConstantAction))
						{
							SanityManager.THROWASSERT("constraintActions[" + conIndex + 
							"] expected to be instanceof DropConstraintConstantAction not " +
							cca.getClass().getName());
						}
					}
				}
				constraintActions[conIndex].executeConstantAction(activation);
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
						"lockGranularity expected to be 'T'or 'R', not " + lockGranularity);
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
			compressTable(activation);
		}

		// Are we doing a truncate table?
		if (truncateTable)
		{
			truncateTable(activation);
		}

		
	}

	/**
	 * Workhorse for adding a new column to a table.
	 *
	 * @param 	dd 			the data dictionary.
	 * @param   ix 			the index of the column specfication in the ALTER 
	 *						statement-- currently we allow only one.
	 * @exception StandardException 	thrown on failure.
	 */
	private void addNewColumnToTable(Activation activation, 
									 int ix) 
	        throws StandardException
	{
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		TransactionController tc = lcc.getTransactionExecute();

		ColumnDescriptor columnDescriptor = 
			td.getColumnDescriptor(columnInfo[ix].name);
		DataValueDescriptor storableDV;
		int colNumber = td.getMaxColumnID() + ix;
		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

		/* We need to verify that the table does not have an existing
		 * column with the same name before we try to add the new
		 * one as addColumnDescriptor() is a void method.
		 */
		if (columnDescriptor != null)
		{
			throw 
				StandardException.newException(
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
		tc.addColumnToConglomerate(td.getHeapConglomerateId(), colNumber, 
								   storableDV);

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
		columnDescriptor = new ColumnDescriptor(
												   columnInfo[ix].name,
												   colNumber + 1,
												   columnInfo[ix].dataType,
												   columnInfo[ix].defaultValue,
												   columnInfo[ix].defaultInfo,
												   td,
												   defaultUUID,
												   columnInfo[ix].autoincStart,
												   columnInfo[ix].autoincInc,
												   columnInfo[ix].autoincInc != 0
												   );

		dd.addDescriptor(columnDescriptor, td,
						 DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);

		// now add the column to the tables column descriptor list.
		td.getColumnDescriptorList().add(columnDescriptor);

		if (columnDescriptor.isAutoincrement())
		{
			updateNewAutoincrementColumn(activation, columnInfo[ix].name,
										 columnInfo[ix].autoincStart,
										 columnInfo[ix].autoincInc);
		}

		// Update the new column to its default, if it has a non-null default
		if (columnDescriptor.hasNonNullDefault())
		{
			updateNewColumnToDefault(activation,
								columnInfo[ix].name,
								columnInfo[ix].defaultInfo.getDefaultText(),
								lcc);
				
			DefaultDescriptor defaultDescriptor = new DefaultDescriptor(dd, defaultUUID, td.getUUID(), 
										 colNumber + 1);

			/* Create stored dependencies for each provider to default */
			ProviderInfo[] providerInfo = ((DefaultInfoImpl) columnInfo[ix].defaultInfo).getProviderInfo();
			int providerInfoLength = (providerInfo == null) ? 0 : providerInfo.length;
			for (int provIndex = 0; provIndex < providerInfoLength; 
				 provIndex++)
			{
				Provider provider = null;
				
				/* We should always be able to find the Provider */
				try 
				{
					provider = (Provider) providerInfo[provIndex].
						getDependableFinder().
						getDependable(
									  providerInfo[provIndex].getObjectId());
				}	
				catch (java.sql.SQLException te)
				{	
					if (SanityManager.DEBUG)
					{
						SanityManager.THROWASSERT("unexpected java.sql.SQLException - " + te);
					}	
				}	
				dm.addDependency(defaultDescriptor, provider, lcc.getContextManager());
			}	
		}	
	}

	/**
	 * Workhorse for dropping a column from a table.
	 *
	 * @param 	dd 			the data dictionary.
	 * @param   ix 			the index of the column specfication in the ALTER 
	 *						statement-- currently we allow only one.
	 * @exception StandardException 	thrown on failure.
	 */
	private void dropColumnFromTable(Activation activation,
									 int ix) 
	        throws StandardException
	{
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		TransactionController tc = lcc.getTransactionExecute();


		ColumnDescriptor columnDescriptor = 
			td.getColumnDescriptor(columnInfo[ix].name);

		// We already verified this in bind, but do it again
		if (columnDescriptor == null)
		{
			throw 
				StandardException.newException(
									    SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE, 
										columnInfo[ix].name,
										td.getQualifiedName());
		}

		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();
		ColumnDescriptorList tab_cdl = td.getColumnDescriptorList();
		int size = tab_cdl.size();

		// can NOT drop a column if it is the only one in the table
		if (size == 1)
		{
			throw StandardException.newException(SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT,
							dm.getActionString(DependencyManager.DROP_COLUMN),
							"THE *LAST* COLUMN " + columnInfo[ix].name,
							"TABLE",
							td.getQualifiedName() );
		}

		columnPosition = columnDescriptor.getPosition();
		boolean cascade = (behavior == StatementType.DROP_CASCADE);

		FormatableBitSet toDrop = new FormatableBitSet(size + 1);
		toDrop.set(columnPosition);
		td.setReferencedColumnMap(toDrop);

		dm.invalidateFor(td, cascade ?
							 DependencyManager.DROP_COLUMN_CASCADE :
							 DependencyManager.DROP_COLUMN, lcc);
					
		// If column has a default we drop the default and any dependencies
		if (columnDescriptor.getDefaultInfo() != null)
		{
			DefaultDescriptor defaultDesc = columnDescriptor.getDefaultDescriptor(dd);
			dm.clearDependencies(lcc, defaultDesc);
		}

		// need to deal with triggers if has referencedColumns
		GenericDescriptorList tdl = dd.getTriggerDescriptors(td);
		Enumeration descs = tdl.elements();
		while (descs.hasMoreElements())
		{
			TriggerDescriptor trd = (TriggerDescriptor) descs.nextElement();
			int[] referencedCols = trd.getReferencedCols();
			if (referencedCols == null)
				continue;
			int refColLen = referencedCols.length, j;
			boolean changed = false;
			for (j = 0; j < refColLen; j++)
			{
				if (referencedCols[j] > columnPosition)
					changed = true;
				else if (referencedCols[j] == columnPosition)
				{
					if (cascade)
					{
						DropTriggerConstantAction.dropTriggerDescriptor(lcc, dm, dd, tc, trd, activation);
						activation.addWarning(
							StandardException.newWarning(SQLState.LANG_TRIGGER_DROPPED,
								trd.getName(), td.getName()));
					}
					else
					{	// we'd better give an error if don't drop it,
						// otherwsie there would be unexpected behaviors
						throw StandardException.newException(SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT,
										dm.getActionString(DependencyManager.DROP_COLUMN),
										columnInfo[ix].name, "TRIGGER",
										trd.getName() );
					}
					break;
				}
			}

			// change triggers to refer to columns in new positions
			if (j == refColLen && changed)
			{
				dd.dropTriggerDescriptor(trd, tc);
				for (j = 0; j < refColLen; j++)
				{
					if (referencedCols[j] > columnPosition)
						referencedCols[j]--;
				}
				dd.addDescriptor(trd, sd,
								 DataDictionary.SYSTRIGGERS_CATALOG_NUM,
								 false, tc);
			}
		}

		ConstraintDescriptorList csdl = dd.getConstraintDescriptors(td);
		int csdl_size = csdl.size();

		// we want to remove referenced primary/unique keys in the second
		// round.  This will ensure that self-referential constraints will
		// work OK.
		int tbr_size = 0;
		ConstraintDescriptor[] toBeRemoved = new ConstraintDescriptor[csdl_size];

		// let's go downwards, don't want to get messed up while removing
		for (int i = csdl_size - 1; i >= 0; i--)
		{
			ConstraintDescriptor cd = csdl.elementAt(i);
			int[] referencedColumns = cd.getReferencedColumns();
			int numRefCols = referencedColumns.length, j;
			boolean changed = false;
			for (j = 0; j < numRefCols; j++)
			{
				if (referencedColumns[j] > columnPosition)
					changed = true;
				if (referencedColumns[j] == columnPosition)
					break;
			}
			if (j == numRefCols)			// column not referenced
			{
				if ((cd instanceof CheckConstraintDescriptor) && changed)
				{
					dd.dropConstraintDescriptor(td, cd, tc);
					for (j = 0; j < numRefCols; j++)
					{
						if (referencedColumns[j] > columnPosition)
							referencedColumns[j]--;
					}
					((CheckConstraintDescriptor) cd).setReferencedColumnsDescriptor(new ReferencedColumnsDescriptorImpl(referencedColumns));
					dd.addConstraintDescriptor(cd, tc);
				}
				continue;
			}

			if (! cascade)
			{
				if (numRefCols > 1 || cd.getConstraintType() == DataDictionary.PRIMARYKEY_CONSTRAINT)
				{
					throw StandardException.newException(SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT,
										dm.getActionString(DependencyManager.DROP_COLUMN),
										columnInfo[ix].name, "CONSTRAINT",
										cd.getConstraintName() );
				}
			}

			if (cd instanceof ReferencedKeyConstraintDescriptor)
			{
				// restrict will raise an error in invalidate if really referenced
				toBeRemoved[tbr_size++] = cd;
				continue;
			}

			// drop now in all other cases
			dm.invalidateFor(cd, DependencyManager.DROP_CONSTRAINT,
									lcc);
			DropConstraintConstantAction.dropConstraintAndIndex(dm, td, dd,
							 cd, tc, activation, true);
			activation.addWarning(StandardException.newWarning(SQLState.LANG_CONSTRAINT_DROPPED,
				cd.getConstraintName(), td.getName()));
		}

		for (int i = tbr_size - 1; i >= 0; i--)
		{
			ConstraintDescriptor cd = toBeRemoved[i];
			DropConstraintConstantAction.dropConstraintAndIndex(dm, td, dd, cd,
						tc, activation, false);
			activation.addWarning(StandardException.newWarning(SQLState.LANG_CONSTRAINT_DROPPED,
					cd.getConstraintName(), td.getName()));

			if (cascade)
			{
				ConstraintDescriptorList fkcdl = dd.getForeignKeys(cd.getUUID());
				for (int j = 0; j < fkcdl.size(); j++)
				{
					ConstraintDescriptor fkcd = (ConstraintDescriptor) fkcdl.elementAt(j);
					dm.invalidateFor(fkcd,
									DependencyManager.DROP_CONSTRAINT,
									lcc);

					DropConstraintConstantAction.dropConstraintAndIndex(
						dm, fkcd.getTableDescriptor(), dd, fkcd, tc, activation, true);
					activation.addWarning(StandardException.newWarning(SQLState.LANG_CONSTRAINT_DROPPED,
						fkcd.getConstraintName(), fkcd.getTableDescriptor().getName()));
				}
			}

			dm.invalidateFor(cd, DependencyManager.DROP_CONSTRAINT, lcc);
			dm.clearDependencies(lcc, cd);
		}

		compressTable(activation);

		// drop the column from syscolumns 
		dd.dropColumnDescriptor(td.getUUID(), columnInfo[ix].name, tc);
		ColumnDescriptor[] cdlArray = new ColumnDescriptor[size - columnDescriptor.getPosition()];

		for (int i = columnDescriptor.getPosition(), j = 0; i < size; i++, j++)
		{
			ColumnDescriptor cd = (ColumnDescriptor) tab_cdl.elementAt(i);
			dd.dropColumnDescriptor(td.getUUID(), cd.getColumnName(), tc);
			cd.setPosition(i);
			cdlArray[j] = cd;
		}
		dd.addDescriptorArray(cdlArray, td,
							  DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);

		List deps = dd.getProvidersDescriptorList(td.getObjectID().toString());
		for (Iterator depsIterator = deps.listIterator(); depsIterator.hasNext();)
		{
			DependencyDescriptor depDesc = (DependencyDescriptor) depsIterator.next();
			DependableFinder finder = depDesc.getProviderFinder();
			if (finder instanceof DDColumnDependableFinder)
			{
				DDColumnDependableFinder colFinder = (DDColumnDependableFinder) finder;
				FormatableBitSet oldColumnBitMap = new FormatableBitSet(colFinder.getColumnBitMap());
				FormatableBitSet newColumnBitMap = new FormatableBitSet(oldColumnBitMap);
				newColumnBitMap.clear();
				int bitLen = oldColumnBitMap.getLength();
				for (int i = 0; i < bitLen; i++)
				{
					if (i < columnPosition && oldColumnBitMap.isSet(i))
						newColumnBitMap.set(i);
					if (i > columnPosition && oldColumnBitMap.isSet(i))
						newColumnBitMap.set(i - 1);
				}
				if (newColumnBitMap.equals(oldColumnBitMap))
					continue;
				dd.dropStoredDependency(depDesc, tc);
				colFinder.setColumnBitMap(newColumnBitMap.getByteArray());
				dd.addDescriptor(depDesc, null,
								 DataDictionary.SYSDEPENDS_CATALOG_NUM,
								 true, tc);
			}
		}
	}

	private void modifyColumnType(Activation activation,
								  int ix)
		throws StandardException						  
	{
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		TransactionController tc = lcc.getTransactionExecute();

		ColumnDescriptor columnDescriptor = 
			td.getColumnDescriptor(columnInfo[ix].name),
			newColumnDescriptor = null;

		newColumnDescriptor = 
			new ColumnDescriptor(columnInfo[ix].name,
									columnDescriptor.getPosition(),
									columnInfo[ix].dataType,
									columnDescriptor.getDefaultValue(),
									columnDescriptor.getDefaultInfo(),
									td,
									columnDescriptor.getDefaultUUID(),
								    columnInfo[ix].autoincStart,
								    columnInfo[ix].autoincInc,
								    columnInfo[ix].autoincInc != 0
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
	private void modifyColumnConstraint(Activation activation, 
										String colName,
										boolean nullability)
		throws StandardException								
	{
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		TransactionController tc = lcc.getTransactionExecute();

		ColumnDescriptor columnDescriptor = 
			td.getColumnDescriptor(colName),
			newColumnDescriptor = null;
		DataTypeDescriptor dataType = columnDescriptor.getType();

		// set nullability
		dataType.setNullability(nullability);

		newColumnDescriptor = 
			 new ColumnDescriptor(colName,
									columnDescriptor.getPosition(),
									dataType,
									columnDescriptor.getDefaultValue(),
									columnDescriptor.getDefaultInfo(),
									td,
									columnDescriptor.getDefaultUUID(),
									columnDescriptor.getAutoincStart(),
									columnDescriptor.getAutoincInc(),
									columnDescriptor.getAutoincInc() != 0);
		


		// Update the ColumnDescriptor with new default info
		dd.dropColumnDescriptor(td.getUUID(), colName, tc);
		dd.addDescriptor(newColumnDescriptor, td,
						 DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);
		
	}
	/**
	 * Workhorse for modifying the default value of a column.
	 * 
	 * @param 		activation 		activation
	 * @param       ix 		the index of the column specfication in the ALTER 
	 *						statement-- currently we allow only one.
	 * @exception	StandardException, thrown on error.
	 */
	private void modifyColumnDefault(Activation activation,
									 int ix)
			throws StandardException						 
	{
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		TransactionController tc = lcc.getTransactionExecute();

		ColumnDescriptor columnDescriptor = 
			td.getColumnDescriptor(columnInfo[ix].name);
		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();
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
												   columnInfo[ix].autoincInc,
												   columnInfo[ix].autoincInc != 0
												   );

		// Update the ColumnDescriptor with new default info
		dd.dropColumnDescriptor(td.getUUID(), columnInfo[ix].name, tc);
		dd.addDescriptor(columnDescriptor, td,
						 DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);
	
		if (columnInfo[ix].autoincInc != 0)
		{
			// adding an autoincrement default-- calculate the maximum value 
			// of the autoincrement column.
			long maxValue = getColumnMax(activation, td, columnInfo[ix].name,
										 columnInfo[ix].autoincInc,
										 columnInfo[ix].autoincStart);
			dd.setAutoincrementValue(tc, td.getUUID(), columnInfo[ix].name,
									 maxValue, true);
		}

		// Add default info for new default, if non-null
		if (columnDescriptor.hasNonNullDefault())
		{
			DefaultDescriptor defaultDescriptor =
				new DefaultDescriptor(dd, defaultUUID, 
										 td.getUUID(), 
										 columnPosition);
		
			/* Create stored dependencies for each provider to default */
			ProviderInfo[] providerInfo = ((DefaultInfoImpl) columnInfo[ix].defaultInfo).getProviderInfo();
			int providerInfoLength = (providerInfo == null) ? 0 : providerInfo.length;
			for (int provIndex = 0; provIndex < providerInfoLength; provIndex++)
			{
				Provider provider = null;
				
				/* We should always be able to find the Provider */
				try 
				{
					provider = (Provider) providerInfo[provIndex].
						getDependableFinder().
						getDependable(
									  providerInfo[provIndex].getObjectId());
				}	
				catch(java.sql.SQLException te)
				{
					if (SanityManager.DEBUG)
					{
						SanityManager.THROWASSERT("unexpected java.sql.SQLException - " + te);
					}
				}
				dm.addDependency(defaultDescriptor, provider, 
								 lcc.getContextManager());
			}
		}
	}


	/* NOTE: compressTable can also be called for 
	 * ALTER TABLE <t> DROP COLUMN <c>;
	 */
	private void compressTable(Activation activation)
		throws StandardException
	{
		ExecRow					emptyHeapRow;
		long					newHeapConglom;
		Properties				properties = new Properties();
		RowLocation				rl;
		this.lcc = activation.getLanguageConnectionContext();
		this.dd = lcc.getDataDictionary();
		this.dm = dd.getDependencyManager();
		this.tc = lcc.getTransactionExecute();
		this.activation = activation;

		if (SanityManager.DEBUG)
		{
			if (lockGranularity != '\0')
			{
				SanityManager.THROWASSERT(
					"lockGranularity expected to be '\0', not " + lockGranularity);
			}
			SanityManager.ASSERT(! compressTable || columnInfo == null,
				"columnInfo expected to be null");
			SanityManager.ASSERT(constraintActions == null,
				"constraintActions expected to be null");
		}
		emptyHeapRow = td.getEmptyExecRow(lcc.getContextManager());
		compressHeapCC = tc.openConglomerate(
								td.getHeapConglomerateId(),
                                false,
                                TransactionController.OPENMODE_FORUPDATE,
                                TransactionController.MODE_TABLE,
                                TransactionController.ISOLATION_SERIALIZABLE);

		// invalidate any prepared statements that
		// depended on this table (including this one)
		// bug 3653 has threads that start up and block on our lock, but do
		// not see they have to recompile their plan.    We now invalidate earlier
		// however they still might recompile using the old conglomerate id before we
		// commit our DD changes.
		//
		dm.invalidateFor(td, DependencyManager.COMPRESS_TABLE, lcc);


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
		getAffectedIndexes(activation);
		// Get an array of RowLocation template
		compressRL = new RowLocation[bulkFetchSize];
		indexRows = new ExecIndexRow[numIndexes];
		if (! compressTable)
		{
			ExecRow newRow = activation.getExecutionFactory().getValueRow(emptyHeapRow.nColumns() - 1);
			for (int i = 0; i < newRow.nColumns(); i++)
			{
				newRow.setColumn(i + 1, i < columnPosition - 1 ?
										 emptyHeapRow.getColumn(i + 1) :
										 emptyHeapRow.getColumn(i + 1 + 1));
			}
			emptyHeapRow = newRow;
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
			baseRow[i] = td.getEmptyExecRow(lcc.getContextManager());
			baseRowArray[i] = baseRow[i].getRowArray();
			compressRL[i] = compressHeapGSC.newRowLocationTemplate();
		}


		newHeapConglom = tc.createAndLoadConglomerate(
									"heap",
									emptyHeapRow.getRowArray(),
									null, //column sort order - not required for heap
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
		long oldHeapConglom = td.getHeapConglomerateId();
		ConglomerateDescriptor cd = td.getConglomerateDescriptor(oldHeapConglom);

		// Update sys.sysconglomerates with new conglomerate #
		dd.updateConglomerateDescriptor(cd, newHeapConglom, tc);
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
	 * incorrect. Recompile is done internally by cloudscape, user does not have
	 * any effect.
	 */
	private void truncateTable(Activation activation)
		throws StandardException
	{
		ExecRow					emptyHeapRow;
		long					newHeapConglom;
		Properties				properties = new Properties();
		RowLocation				rl;
		this.lcc = activation.getLanguageConnectionContext();
		this.dd = lcc.getDataDictionary();
		this.dm = dd.getDependencyManager();
		this.tc = lcc.getTransactionExecute();
		this.activation = activation;

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


		//truncate table is not allowed if there are any tables referencing it.
		//except if it is self referencing.
		ConstraintDescriptorList cdl = dd.getConstraintDescriptors(td);
		for(int index = 0; index < cdl.size(); index++)
		{
			ConstraintDescriptor cd = cdl.elementAt(index);
			if (cd instanceof ReferencedKeyConstraintDescriptor)
			{
				ReferencedKeyConstraintDescriptor rfcd = (ReferencedKeyConstraintDescriptor) cd;
				if(rfcd.hasNonSelfReferencingFK(ConstraintDescriptor.ENABLED))
				{
					throw StandardException.newException(SQLState.LANG_NO_TRUNCATE_ON_FK_REFERENCE_TABLE,td.getName());
				}
			}
		}

		//truncate is not allowed when there are enabled DELETE triggers
		GenericDescriptorList tdl = dd.getTriggerDescriptors(td);
		Enumeration descs = tdl.elements();
		while (descs.hasMoreElements())
		{
			TriggerDescriptor trd = (TriggerDescriptor) descs.nextElement();
			if (trd.listensForEvent(TriggerDescriptor.TRIGGER_EVENT_DELETE) &&
				trd.isEnabled())
			{
				throw
					StandardException.newException(SQLState.LANG_NO_TRUNCATE_ON_ENABLED_DELETE_TRIGGERS,
												   td.getName(),trd.getName());	
			}
		}

		//gather information from the existing conglomerate to create new one.
		emptyHeapRow = td.getEmptyExecRow(lcc.getContextManager());
		compressHeapCC = tc.openConglomerate(
								td.getHeapConglomerateId(),
                                false,
                                TransactionController.OPENMODE_FORUPDATE,
                                TransactionController.MODE_TABLE,
                                TransactionController.ISOLATION_SERIALIZABLE);

		// invalidate any prepared statements that
		// depended on this table (including this one)
		// bug 3653 has threads that start up and block on our lock, but do
		// not see they have to recompile their plan.    We now invalidate earlier
		// however they still might recompile using the old conglomerate id before we
		// commit our DD changes.
		//
		dm.invalidateFor(td, DependencyManager.TRUNCATE_TABLE, lcc);

		rl = compressHeapCC.newRowLocationTemplate();
		// Get the properties on the old heap
		compressHeapCC.getInternalTablePropertySet(properties);
		compressHeapCC.close();
		compressHeapCC = null;

		//create new conglomerate
		newHeapConglom = tc.createConglomerate(
									"heap",
									emptyHeapRow.getRowArray(),
									null, //column sort order - not required for heap
									properties,
									TransactionController.IS_DEFAULT);
		
		/* Set up index info to perform truncate on them*/
		getAffectedIndexes(activation);
		if(numIndexes > 0)
		{
			indexRows = new ExecIndexRow[numIndexes];
			ordering = new ColumnOrdering[numIndexes][];
			for (int index = 0; index < numIndexes; index++)
			{
				// create a single index row template for each index
				indexRows[index] = compressIRGs[index].getIndexRowTemplate();
				compressIRGs[index].getIndexRow(emptyHeapRow, 
											  rl, 
											  indexRows[index],
											  (FormatableBitSet) null);
				/* For non-unique indexes, we order by all columns + the RID.
				 * For unique indexes, we just order by the columns.
				 * No need to try to enforce uniqueness here as
				 * index should be valid.
				 */
				int[] baseColumnPositions = compressIRGs[index].baseColumnPositions();
				boolean[] isAscending = compressIRGs[index].isAscending();
				int numColumnOrderings;
				numColumnOrderings = baseColumnPositions.length + 1;
				ordering[index] = new ColumnOrdering[numColumnOrderings];
				for (int ii =0; ii < numColumnOrderings - 1; ii++) 
				{
					ordering[index][ii] = new IndexColumnOrder(ii, isAscending[ii]);
				}
				ordering[index][numColumnOrderings - 1] = new IndexColumnOrder(numColumnOrderings - 1);
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

	private void updateIndex(long newHeapConglom, DataDictionary dd,
							 int index, long[] newIndexCongloms)
		throws StandardException
	{
		ConglomerateController indexCC;
		Properties properties = new Properties();
		ConglomerateDescriptor cd;
		// Get the ConglomerateDescriptor for the index
		cd = td.getConglomerateDescriptor(indexConglomerateNumbers[index]);

		// Build the properties list for the new conglomerate
		indexCC = tc.openConglomerate(
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
			properties.put("nUniqueColumns", 
						   Integer.toString(indexRowLength - 1));
		}
		else
		{
			properties.put("nUniqueColumns", 
						   Integer.toString(indexRowLength));
		}
		properties.put("rowLocationColumn", 
						Integer.toString(indexRowLength - 1));
		properties.put("nKeyFields", Integer.toString(indexRowLength));

		indexCC.close();

		// We can finally drain the sorter and rebuild the index
		// RESOLVE - all indexes are btrees right now
		// Populate the index.
		
		RowLocationRetRowSource cCount = null;
		boolean updateStatistics = false;
		if(!truncateTable)
		{
			sorters[index].close();
			sorters[index] = null;

			if (td.statisticsExist(cd))
			{
				cCount = new CardinalityCounter(tc.openSortRowSource(sortIds[index]));
				updateStatistics = true;
			}
			else
				cCount = tc.openSortRowSource(sortIds[index]);

			newIndexCongloms[index] = tc.createAndLoadConglomerate(
								   "BTREE",
								   indexRows[index].getRowArray(),
								   ordering[index],
								   properties,
								   TransactionController.IS_DEFAULT,
								   cCount,
								   (long[]) null);

		}else
		{
			newIndexCongloms[index] = tc.createConglomerate(
								   "BTREE",
									indexRows[index].getRowArray(),
									ordering[index],
									properties,
								   TransactionController.IS_DEFAULT);


			//on truncate drop the statistics because we know for sure 
			//rowscount is zero and existing statistic will be invalid.
			if (td.statisticsExist(cd))
				dd.dropStatisticsDescriptors(td.getUUID(), cd.getUUID(), tc);
		}

		if (updateStatistics)
		{
			dd.dropStatisticsDescriptors(td.getUUID(), cd.getUUID(), tc);
			long numRows;
			if ((numRows = ((CardinalityCounter)cCount).getRowCount()) > 0)
			{
				long[] c = ((CardinalityCounter)cCount).getCardinality();
				for (int i = 0; i < c.length; i++)
				{
					StatisticsDescriptor statDesc = 
						new StatisticsDescriptor(dd, dd.getUUIDFactory().createUUID(),
													cd.getUUID(), td.getUUID(), "I", new StatisticsImpl(numRows, c[i]),
													i + 1);
					dd.addDescriptor(statDesc, null, // no parent descriptor
									 DataDictionary.SYSSTATISTICS_CATALOG_NUM,
									 true, tc);	// no error on duplicate.
				}
			}
		}

		/* Update the DataDictionary
		 * RESOLVE - this will change in 1.4 because we will get
		 * back the same conglomerate number
		 *
		 * Update sys.sysconglomerates with new conglomerate #, we need to
		 * update all (if any) duplicate index entries sharing this same
		 * conglomerate.
		 */
		dd.updateConglomerateDescriptor(
				td.getConglomerateDescriptors(indexConglomerateNumbers[index]),
				newIndexCongloms[index], tc);

		// Drop the old conglomerate
		tc.dropConglomerate(indexConglomerateNumbers[index]);
	}


	/**
	 * Get info on the indexes on the table being compress. 
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */
	private void getAffectedIndexes(Activation activation)
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
			for (int i = 0; i < compressIRGs.length; i++)
			{
				int[] baseColumnPositions = compressIRGs[i].baseColumnPositions();
				int j;
				for (j = 0; j < baseColumnPositions.length; j++)
					if (baseColumnPositions[j] == columnPosition) break;
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
					DropIndexConstantAction.dropIndex(dm, dd, tc, cd, td, activation);

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
			IndexRowGenerator[] newIRGs = new IndexRowGenerator[numIndexes];
			long[] newIndexConglomNumbers = new long[numIndexes];

			for (int i = 0, j = 0; i < numIndexes; i++, j++)
			{
				while (compressIRGs[j] == null)
					j++;

				int[] baseColumnPositions = compressIRGs[j].baseColumnPositions();
				newIRGs[i] = compressIRGs[j];
				newIndexConglomNumbers[i] = indexConglomerateNumbers[j];

				boolean[] isAscending = compressIRGs[j].isAscending();
				boolean reMakeArrays = false;
				int size = baseColumnPositions.length;
				for (int k = 0; k < size; k++)
				{
					if (baseColumnPositions[k] > columnPosition)
						baseColumnPositions[k]--;
					else if (baseColumnPositions[k] == columnPosition)
					{
						baseColumnPositions[k] = 0;		// mark it
						reMakeArrays = true;
					}
				}
				if (reMakeArrays)
				{
					size--;
					int[] newBCP = new int[size];
					boolean[] newIsAscending = new boolean[size];
					for (int k = 0, step = 0; k < size; k++)
					{
						if (step == 0 && baseColumnPositions[k + step] == 0)
							step++;
						newBCP[k] = baseColumnPositions[k + step];
						newIsAscending[k] = isAscending[k + step];
					}
					IndexDescriptor id = compressIRGs[j].getIndexDescriptor();
					id.setBaseColumnPositions(newBCP);
					id.setIsAscending(newIsAscending);
					id.setNumberOfOrderedColumns(id.numberOfOrderedColumns() - 1);
				}
			}
			compressIRGs = newIRGs;
			indexConglomerateNumbers = newIndexConglomNumbers;
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
	 * Set up to update all of the indexes on a table when doing a bulk insert
	 * on an empty table.
	 *
	 * @exception StandardException					thrown on error
	 */
	private void setUpAllSorts(ExecRow sourceRow,
							   RowLocation rl)
		throws StandardException
    {
		ordering = new ColumnOrdering[numIndexes][];

		needToDropSort = new boolean[numIndexes];
		sortIds = new long[numIndexes];

		/* For each index, build a single index row and a sorter. */
		for (int index = 0; index < numIndexes; index++)
		{
			// create a single index row template for each index
			indexRows[index] = compressIRGs[index].getIndexRowTemplate();

			// Get an index row based on the base row
			// (This call is only necessary here because we need to pass a template to the sorter.)
			compressIRGs[index].getIndexRow(sourceRow, 
											  rl, 
											  indexRows[index],
											  (FormatableBitSet) null);

			/* For non-unique indexes, we order by all columns + the RID.
			 * For unique indexes, we just order by the columns.
			 * No need to try to enforce uniqueness here as
			 * index should be valid.
			 */
			int[] baseColumnPositions = compressIRGs[index].baseColumnPositions();
			boolean[] isAscending = compressIRGs[index].isAscending();
			int numColumnOrderings;
			SortObserver sortObserver = null;
			/* We can only reuse the wrappers when doing an
			 * external sort if there is only 1 index.  Otherwise,
			 * we could get in a situation where 1 sort reuses a
			 * wrapper that is still in use in another sort.
			 */
			boolean reuseWrappers = (numIndexes == 1);
			numColumnOrderings = baseColumnPositions.length + 1;
			sortObserver = new BasicSortObserver(false, false, 
												 indexRows[index],
												 reuseWrappers);
			ordering[index] = new ColumnOrdering[numColumnOrderings];
			for (int ii =0; ii < numColumnOrderings - 1; ii++) 
			{
				ordering[index][ii] = new IndexColumnOrder(ii, isAscending[ii]);
			}
			ordering[index][numColumnOrderings - 1] = new IndexColumnOrder(numColumnOrderings - 1);

			// create the sorters
			sortIds[index] = tc.createSort(
							   (Properties)null, 
								indexRows[index].getRowArrayClone(),
								ordering[index],
								sortObserver,
								false,			// not in order
								estimatedRowCount,		// est rows	
								-1				// est row size, -1 means no idea	
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
			int bulkFetched = 0;

			bulkFetched = compressHeapGSC.fetchNextGroup(baseRowArray, compressRL);

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
				currentRow = baseRow[currentCompressRow];
			else
			{
				if (currentRow == null)
					currentRow = activation.getExecutionFactory().getValueRow(baseRowArray[currentCompressRow].length - 1);
				for (int i = 0; i < currentRow.nColumns(); i++)
				{
					currentRow.setColumn(i + 1, i < columnPosition - 1 ?
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
				((DataValueDescriptor) currentRow.getRowArray()[i]).getObject();
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
									    (RowLocation) rl.cloneObject(), 
										indexRows[index],
										(FormatableBitSet) null);

		// Insert the index row into the matching sorter
		sorters[index].insert(indexRows[index].getRowArray());
	}

	/**
	 * @see ResultSet#cleanUp
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	cleanUp() throws StandardException
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
					sorters[index].close();
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
	 * whether the table has 0, 1 or > 1 rows.
	 *
	 *
	 * @return Number of rows (0, 1 or > 1) in table.
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
	 * @param columnName		column name
	 * @param defaultText		default text
	 * @param lcc				the language connection context
	 *
	 * @exception StandardException if update to default fails
	 */
	private void updateNewColumnToDefault
	(
		Activation activation,
		String							columnName,
		String							defaultText,
		LanguageConnectionContext		lcc
	)
		throws StandardException
	{
		/* Need to use delimited identifiers for all object names
		 * to ensure correctness.
		 */
		String updateStmt = "UPDATE \"" + td.getSchemaName() + "\".\"" +
							td.getName() + "\" SET \"" +
							 columnName + "\" = " + defaultText;


		AlterTableConstantAction.executeUpdate(lcc, updateStmt);
	}

	private static void executeUpdate(LanguageConnectionContext lcc, String updateStmt) throws StandardException
	{
		PreparedStatement ps = lcc.prepareInternalStatement(updateStmt);
		ResultSet rs = ps.execute(lcc, true);
		rs.close();
		rs.finish();
	}

	/**
	 * computes the minimum/maximum value in a column of a table.
	 */
	private long getColumnMax(Activation activation, TableDescriptor td, String columnName, 
							  long increment, long initial)
							  throws StandardException
	{
		String maxStr = (increment > 0) ? "MAX" : "MIN";
		String maxStmt = "SELECT " + maxStr + "(\"" + columnName + "\")"  +
				"FROM \"" + td.getSchemaName() + "\".\"" + td.getName() + "\"";


		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		PreparedStatement ps = lcc.prepareInternalStatement(maxStmt);

		ResultSet rs = ps.execute(lcc, false);
		DataValueDescriptor[] rowArray = rs.getNextRow().getRowArray();
		rs.close();
		rs.finish();

		return rowArray[0].getLong();
	}					

	private void dropAllColumnDefaults(UUID tableId, DataDictionary dd)
		throws StandardException
	{
		ColumnDescriptorList cdl = td.getColumnDescriptorList();
		int					 cdlSize = cdl.size();
		
		for(int index = 0; index < cdlSize; index++)
		{
			ColumnDescriptor cd = (ColumnDescriptor) cdl.elementAt(index);

			// If column has a default we drop the default and
			// any dependencies
			if (cd.getDefaultInfo() != null)
			{
				DefaultDescriptor defaultDesc = cd.getDefaultDescriptor(dd);
				dm.clearDependencies(lcc, defaultDesc);
			}
		}
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
	 * Update values in a new autoincrement column being added to a table.
	 * This is similar to updateNewColumnToDefault whereby we issue an
	 * update statement using a nested connection. The UPDATE statement 
	 * uses a static method in ConnectionInfo (which is not documented) 
	 * which returns the next value to be inserted into the autoincrement
	 * column.
	 *
	 * @param columnName autoincrement column name that is being added.
	 * @param initial    initial value of the autoincrement column.
	 * @param increment  increment value of the autoincrement column.
	 *
	 * @see #updateNewColumnToDefault
	 */
	private void updateNewAutoincrementColumn(Activation activation, String columnName, long initial,
											 long increment)
		throws StandardException
	{
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();

		// Don't throw an error in bind when we try to update the 
		// autoincrement column.
		lcc.setAutoincrementUpdate(true);

		lcc.autoincrementCreateCounter(td.getSchemaName(),
									   td.getName(),
									   columnName, new Long(initial),
									   increment, 0);
		// the sql query is.
		// UPDATE table 
		//  set ai_column = ConnectionInfo.nextAutoincrementValue(
		//							schemaName, tableName, 
		//							columnName)
		String updateStmt = "UPDATE \"" + td.getSchemaName() + "\".\"" +
			td.getName() + "\" SET \"" + columnName + "\" = " + 
			"org.apache.derby.iapi.db.ConnectionInfo::" + 
			"nextAutoincrementValue(" + 
			"'" + td.getSchemaName() + "'" + "," +
			"'" + td.getName() +  "'" + "," +
			"'" + columnName + "'" + ")";



		try
		{
			AlterTableConstantAction.executeUpdate(lcc, updateStmt);
		}
		catch (StandardException se)
		{
			if (se.getMessageId().equals(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE))
			{
				// If overflow, override with more meaningful message.
				throw StandardException.newException(SQLState.LANG_AI_OVERFLOW,
													 se,
													 td.getName(),
													 columnName);
			}
			throw se;
		}
		finally
		{
			// and now update the autoincrement value.
			lcc.autoincrementFlushCache(td.getUUID());
			lcc.setAutoincrementUpdate(false);		
		}

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
		StringBuffer constraintText = new StringBuffer();

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
					constraintText.append(columnNames[colCtr] + " IS NOT NULL ");
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
									(String) null,
									constraintText.toString(),
									td,
									lcc,
									false))
			{	
				if (errorMsg.equals(SQLState.LANG_NULL_DATA_IN_PRIMARY_KEY))
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

}

