/*

   Derby - Class org.apache.derby.impl.sql.execute.InsertResultSet

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

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.services.stream.InfoStreams;
import org.apache.derby.iapi.services.io.StreamStorable;
import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.StatementUtil;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.StatisticsDescriptor;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.depend.DependencyManager;

import org.apache.derby.iapi.sql.ResultColumnDescriptor ;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.RowChanger;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.sql.execute.TargetResultSet;

import org.apache.derby.iapi.types.NumberDataValue;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.LanguageProperties;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.GroupFetchScanController;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowLocationRetRowSource;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.SortObserver;
import org.apache.derby.iapi.store.access.SortController;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.impl.sql.execute.AutoincrementCounter;
import	org.apache.derby.impl.sql.execute.InternalTriggerExecutionContext;

import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.types.StatisticsImpl;
import org.apache.derby.iapi.db.TriggerExecutionContext;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.util.StringUtil;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;
//import java.math.BigDecimal;

/**
 * Insert the rows from the source into the specified
 * base table. This will cause constraints to be checked
 * and triggers to be executed based on the c's and t's
 * compiled into the insert plan.
 */
public class InsertResultSet extends DMLWriteResultSet implements TargetResultSet
{
	// RESOLVE. Embarassingly large public state. If we could move the Replication
	// code into the same package, then these variables could be protected.

	// passed in at construction time
                                                 
	private	NoPutResultSet			sourceResultSet;
	public  NoPutResultSet			savedSource;
	public	InsertConstantAction	constants;
	private GeneratedMethod			checkGM;
	private long					heapConglom;

	//following is for jdbc3.0 feature auto generated keys resultset
	public  ResultSet			autoGeneratedKeysResultSet;
	private	TemporaryRowHolderImpl	autoGeneratedKeysRowsHolder;

	// divined at run time

    public	ResultDescription 		resultDescription;
	private RowChanger 				rowChanger;

	public	TransactionController 	tc;
	public	ExecRow					row;

	public	LanguageConnectionContext			lcc;
	
	public	boolean					userSpecifiedBulkInsert;
	public	boolean					bulkInsertPerformed;

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
	private Hashtable				indexConversionTable;

	// indexedCols is 1-based
	private FormatableBitSet					indexedCols;
	private ConglomerateController	bulkHeapCC;

	protected DataDictionary			dd;
	protected TableDescriptor			td;
		
	private ExecIndexRow[]			indexRows;
	private ExecRow					fullTemplate;
	private	long[]					sortIds;
	private RowLocationRetRowSource[]
                                    rowSources;
	private	ScanController			bulkHeapSC;
	private ColumnOrdering[][]		ordering;
	private SortController[]		sorters;
	private	TemporaryRowHolderImpl	rowHolder;
	private RowLocation				rl;

	private	boolean					hasBeforeStatementTrigger;
	private	boolean					hasBeforeRowTrigger;
	private	BulkTableScanResultSet	tableScan;

	private int						numOpens;
	private boolean					firstExecute;

	// cached across open()s
	private	FKInfo[]				fkInfoArray;
	private	TriggerInfo				triggerInfo;
	private RISetChecker 			fkChecker;
	private TriggerEventActivator	triggerActivator;

	/**
	 * keeps track of autoincrement values that are generated by 
	 * getSetAutoincrementValues.
	 */
	private NumberDataValue				aiCache[];

	/**
	 * If set to true, implies that this (rep)insertresultset has generated
	 * autoincrement values. During refresh for example, the autoincrement
	 * values are not generated but sent from the source to target or 
	 * vice-versa.
	 */
	protected boolean 				autoincrementGenerated;
	private long					identityVal;  //support of IDENTITY_LOCAL_VAL function
	private boolean					setIdentity;
	

	/**
     * Returns the description of the inserted rows.
     * REVISIT: Do we want this to return NULL instead?
	 */
	public ResultDescription getResultDescription()
	{
	    return resultDescription;
	}

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
			RowLocation rlClone = (RowLocation) rowLocation.cloneObject();

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
		//System.out.println("preprocessrow is called ");
		/*
		** We can process before row triggers now.  All other
		** triggers can only be fired after we have inserted
		** all our rows.
		*/
		if (hasBeforeRowTrigger)
		{
			// RESOLVE
			// Possibly dead code-- if there are triggers we don't do bulk insert.
			rowHolder.truncate();
			rowHolder.insert(execRow);
			triggerActivator.notifyEvent(TriggerEvents.BEFORE_INSERT,
											(CursorResultSet)null,
											rowHolder.getResultSet());
		} 

		if (checkGM != null && !hasBeforeStatementTrigger)
		{
			evaluateCheckConstraints();
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

	/**
	  *	Run the check constraints against the current row. Raise an error if
	  * a check constraint is violated.
	  *
	  * @return Nothing.
	  * @exception StandardException thrown on error
	  */
	private	void	evaluateCheckConstraints()
		throws StandardException
	{
		if (checkGM != null)
		{

			// Evaluate the check constraints. The expression evaluation
			// will throw an exception if there is a violation, so there
			// is no need to check the result of the expression.
			checkGM.invoke(activation);
		}
	}

    /*
     * class interface
     *
     */
    /**
	 *
	 * @exception StandardException		Thrown on error
     */
    public InsertResultSet(NoPutResultSet source, 
						   GeneratedMethod checkGM,
						   Activation activation)
		throws StandardException
    {
		super(activation);
		sourceResultSet = source;
		constants = (InsertConstantAction) constantAction;
		this.checkGM = checkGM;
		heapConglom = constants.conglomId; 

		lcc = activation.getLanguageConnectionContext();
        tc = activation.getTransactionController();
		fkInfoArray = constants.getFKInfo( lcc.getExecutionContext() );
		triggerInfo = constants.getTriggerInfo(lcc.getExecutionContext());
		
		/*
		** If we have a before statement trigger, then
		** we cannot check constraints inline.
		*/
		hasBeforeStatementTrigger = (triggerInfo != null) ?
				triggerInfo.hasTrigger(true, false) :
				false;

		hasBeforeRowTrigger = (triggerInfo != null) ?
				triggerInfo.hasTrigger(true, true) :
				false;

        resultDescription = sourceResultSet.getResultDescription();

		// Is this a bulkInsert or regular insert?
		String insertMode = constants.getProperty("insertMode");
		
		RowLocation[] rla;
		if ((rla = constants.getAutoincRowLocation()) != null)
		{
			aiCache = 
				new NumberDataValue[rla.length];
			for (int i = 0; i < resultDescription.getColumnCount(); i++)
			{
				if (rla[i] == null)
					continue;
				ResultColumnDescriptor rcd = 
					resultDescription.getColumnDescriptor(i + 1);
				aiCache[i] = (NumberDataValue)rcd.getType().getNull();
			}
		}

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
					TriggerDescriptor td = triggerInfo.getTriggerArray()[0];
					throw StandardException.newException(SQLState.LANG_NO_BULK_INSERT_REPLACE_WITH_TRIGGER_DURING_EXECUTION, constants.getTableName(), td.getName());
				}
			}
		}

		//System.out.println("new InsertResultSet " + sourceResultSet.getClass());
	}
	
	/**
		@exception StandardException Standard Cloudscape error policy
	*/
	public void open() throws StandardException
	{
		// Remember if this is the 1st execution
		firstExecute = (rowChanger == null);

		autoincrementGenerated = false;

		dd = lcc.getDataDictionary();

		/*
		** verify the auto-generated key columns list(ie there are no invalid column
		** names or positions). This is done at at execution time because for a precompiled
		** insert statement, user can specify different column selections for
		** auto-generated keys.
		*/
		if(activation.getAutoGeneratedKeysResultsetMode())
		{
			if (activation.getAutoGeneratedKeysColumnIndexes() != null)
				verifyAutoGeneratedColumnsIndexes(activation.getAutoGeneratedKeysColumnIndexes());
			else  if (activation.getAutoGeneratedKeysColumnNames() != null)
				verifyAutoGeneratedColumnsNames(activation.getAutoGeneratedKeysColumnNames());
		}
		rowCount = 0;

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
			long baseTableConglom = bulkInsertCore(lcc, heapConglom);

			if (hasBeforeStatementTrigger)
			{	
				tableScan = getTableScanResultSet(baseTableConglom); 

				// fire BEFORE trigger, do this before checking constraints
				triggerActivator.notifyEvent(TriggerEvents.BEFORE_INSERT, 
												(CursorResultSet)null,
												tableScan);
			
				// if we have a check constraint, we have
				// to do it the hard way now before we get
				// to our AFTER triggers.
				if (checkGM != null)
				{
					tableScan = getTableScanResultSet(baseTableConglom); 

					try
					{
						ExecRow currRow = null;
						while ((currRow = tableScan.getNextRowCore()) != null)
						{
							// we have to set the source row so the check constraint
							// sees the correct row.
							sourceResultSet.setCurrentRow(currRow);
							evaluateCheckConstraints();
						}
					} finally
					{
						sourceResultSet.clearCurrentRow();
					}
				}
			}
			
			bulkValidateForeignKeys(tc, lcc.getContextManager());
	
			// if we have an AFTER trigger, let 'er rip
			if ((triggerInfo != null) && 
				(triggerInfo.hasTrigger(false, true) ||
				 triggerInfo.hasTrigger(false, false))) 
			{
				triggerActivator.notifyEvent(TriggerEvents.AFTER_INSERT,
										(CursorResultSet)null,
										getTableScanResultSet(baseTableConglom)); 
			}
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

		/* autoGeneratedResultset for JDBC3. Nulled after statement execution is over
		(ie after it is saved off in LocalSatement object) */
		if (activation.getAutoGeneratedKeysResultsetMode())
			autoGeneratedKeysResultSet = autoGeneratedKeysRowsHolder.getResultSet();
		else
			autoGeneratedKeysResultSet = null;

		cleanUp();

		if (aiCache != null)
		{
			Hashtable aiHashtable = new Hashtable();
			int numColumns = aiCache.length;
			// this insert updated ai values, store them in some persistent
			// place so that I can see these values.
			for (int i = 0; i < numColumns; i++)
			{
				if (aiCache[i] == null)
					continue;
				aiHashtable.put(AutoincrementCounter.makeIdentity(
								  constants.getSchemaName(),
								  constants.getTableName(),
								  constants.getColumnName(i)),
								new Long(aiCache[i].getLong()));
			}
			InternalTriggerExecutionContext itec =
				(InternalTriggerExecutionContext)lcc.getTriggerExecutionContext();
			if (itec == null)
				lcc.copyHashtableToAIHT(aiHashtable);
			else
				itec.copyHashtableToAIHT(aiHashtable);
		}	

		endTime = getCurrentTimeMillis();
	}

	/*
	 * Verify that the auto-generated columns list (by position) has valid
	 * column positions for the table.
	 */
	private void verifyAutoGeneratedColumnsIndexes(int[] columnIndexes)
		throws StandardException
	{
		int size = columnIndexes.length;
		TableDescriptor td = dd.getTableDescriptor(constants.targetUUID);

		// all 1-based column ids.
		for (int i = 0; i < size; i++)
		{
			if (td.getColumnDescriptor(columnIndexes[i]) == null)
				throw StandardException.newException(SQLState.LANG_COLUMN_POSITION_NOT_FOUND, new Integer(columnIndexes[i]));
		}
	}

	/*
	 * If user didn't provide columns list for auto-generated columns, then only include
	 * columns with auto-generated values in the resultset. Those columns would be ones
	 * with default value defined.
	 */
	private int[] generatedColumnPositionsArray()
		throws StandardException
	{
		TableDescriptor td = dd.getTableDescriptor(constants.targetUUID);
		ColumnDescriptor cd;
		int size = td.getMaxColumnID();

		int[] generatedColumnPositionsArray = new int[size];
		int generatedColumnNumbers = 0;
		for (int i=0; i<size; i++) {
			generatedColumnPositionsArray[i] = -1;
		}

		for (int i=0; i<size; i++) {
			cd = td.getColumnDescriptor(i+1);
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

	/*
	 * Remove duplicate columns from the array. Then use this array to generate a sub-set
	 * of insert resultset to be returned for JDBC3.0 getGeneratedKeys() call.
	 */
	private int[] uniqueColumnPositionArray(int[] columnIndexes)
		throws StandardException
	{
		int size = columnIndexes.length;
		TableDescriptor td = dd.getTableDescriptor(constants.targetUUID);

		//create an array of integer (the array size = number of columns in table)
		// valid column positions are 1...getMaxColumnID()
		int[] uniqueColumnIndexes = new int[td.getMaxColumnID()];

		int uniqueColumnNumbers = 0;


		//At the end of following loop, the uniqueColumnIndexes elements will not be 0 for user
		//selected auto-generated columns.
		for (int i=0; i<size; i++) {
			if (uniqueColumnIndexes[columnIndexes[i] - 1] == 0) {
				uniqueColumnNumbers++;
				uniqueColumnIndexes[columnIndexes[i] - 1] = columnIndexes[i];
			}
		}
		int[] returnUniqueColumnIndexes = new int[uniqueColumnNumbers];

		//return just the column positions which are not marked 0 in the uniqueColumnIndexes array
		for (int i=0, j=0; i<uniqueColumnIndexes.length; i++) {
			if (uniqueColumnIndexes[i] != 0)
				returnUniqueColumnIndexes[j++] = uniqueColumnIndexes[i];
		}

		return returnUniqueColumnIndexes;
	}

	/**
	 * Verify that the auto-generated columns list (by name) has valid
	 * column names for the table. If all the column names are valid,
	 * convert column names array to corresponding column positions array
	 * Save that column positions array in activation. We do this to simplify the
	 * rest of the logic(it only has to deal with column positions here after).
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error if invalid column
   * name in the list.
	 */
	private void verifyAutoGeneratedColumnsNames(String[] columnNames)
		throws StandardException
	{
		int size = columnNames.length;
		int columnPositions[] = new int[size];

 		TableDescriptor td = dd.getTableDescriptor(constants.targetUUID);
		ColumnDescriptor cd;

		for (int i = 0; i < size; i++)
		{
			if (columnNames[i] == null)
				throw StandardException.newException(SQLState.LANG_COLUMN_NAME_NOT_FOUND, columnNames[i]);
			cd = td.getColumnDescriptor(columnNames[i]);
			if (cd == null)
				throw StandardException.newException(SQLState.LANG_COLUMN_NAME_NOT_FOUND, columnNames[i]);
			else
				columnPositions[i] = cd.getPosition();
		}
		activation.setAutoGeneratedKeysResultsetInfo(columnPositions, null);
	}

	/**
	 * @see ResultSet#getAutoGeneratedKeysResultset
	 */
	public ResultSet getAutoGeneratedKeysResultset()
	{
		return autoGeneratedKeysResultSet;
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
		long startValue = 0;
		NumberDataValue dvd;
		int index = columnPosition - 1;	// all our indices are 0 based.

		/* As in DB2, only for single row insert: insert into t1(c1) values (..) do
		 * we return the correct most recently generated identity column value.  For
		 * multiple row insert, or insert with sub-select, the return value is non-
		 * deterministic, and is the previous return value of the IDENTITY_VAL_LOCAL
		 * function, before the insert statement.  Also, DB2 can have at most 1 identity
		 * column per table.  The return value won't be affected either if Cloudscape
		 * table has more than one identity columns.
		 */
		setIdentity = (! autoincrementGenerated) && isSourceRowResultSet();
		autoincrementGenerated = true;

  		if (bulkInsert)
  		{
			ColumnDescriptor cd = td.getColumnDescriptor(columnPosition);
			long ret;

			// for bulk insert we have the table descriptor
			//			System.out.println("in bulk insert");
			if (aiCache[index].isNull())
			{
				if (bulkInsertReplace)
				{
					startValue = cd.getAutoincStart();
				}
				else
				{
					dvd = dd.getSetAutoincrementValue(
						    constants.autoincRowLocation[index],
							tc, false, aiCache[index], true);
					startValue = dvd.getLong();
				}
				lcc.autoincrementCreateCounter(td.getSchemaName(),
											   td.getName(),
											   cd.getColumnName(),
											   new Long(startValue),
											   increment,
											   columnPosition);
			
			}  		
			ret = lcc.nextAutoincrementValue(td.getSchemaName(),
											 td.getName(),
											 cd.getColumnName());
			aiCache[columnPosition - 1].setValue(ret);
		}	

		else
		{
			NumberDataValue newValue;
			TransactionController nestedTC = null, tcToUse = tc;

			try
			{
				nestedTC = tc.startNestedUserTransaction(false);
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
						   tcToUse, true, aiCache[index], (tcToUse == tc));
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

				if (se.getMessageId().equals(SQLState.LOCK_TIMEOUT))
				{
					// if we couldn't do this with a nested xaction, retry with
					// parent-- we need to wait this time!
					newValue = dd.getSetAutoincrementValue(
									constants.autoincRowLocation[index],
									tc, true, aiCache[index], true);
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
					nestedTC.commit();
					nestedTC.destroy();
				}
			}
			aiCache[index] = newValue;
			if (setIdentity)
				identityVal = newValue.getLong();
		}

		return aiCache[index];
		
	}

	// Is sourceResultSet a RowResultSet (values clause)?
	private boolean isSourceRowResultSet ()
	{
		boolean isRow = false;
		if (sourceResultSet instanceof NormalizeResultSet)
			isRow = (((NormalizeResultSet) sourceResultSet).source instanceof RowResultSet);
		return isRow;
	}

	// Do the work for a "normal" insert
	private void normalInsertCore(LanguageConnectionContext lcc, boolean firstExecute)
		throws StandardException
	{
		boolean	firstDeferredRow = true;
		ExecRow	deferredRowBuffer = null;

		/* Get or re-use the row changer.
		 * NOTE: We need to set ourself as the top result set
		 * if this is not the 1st execution.  (Done in constructor
		 * for 1st execution.)
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
		else
		{
			lcc.getStatementContext().setTopResultSet(this, subqueryTrackingArray);
		}

		/* decode lock mode for the execution isolation level */
		int lockMode = UpdateResultSet.decodeLockMode(lcc, constants.lockMode);

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
				fkChecker = new RISetChecker(tc, fkInfoArray);
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
			rowHolder = new TemporaryRowHolderImpl(tc, properties, resultDescription);
			rowChanger.setRowHolder(rowHolder);
		}

		int[] columnIndexes = null;
		if (firstExecute && activation.getAutoGeneratedKeysResultsetMode())
		{
			ResultDescription rd;
			Properties properties = new Properties();
			columnIndexes = activation.getAutoGeneratedKeysColumnIndexes();

			// Get the properties on the old heap
			rowChanger.getHeapConglomerateController().getInternalTablePropertySet(properties);

			if ( columnIndexes != null) {//use user provided column positions array
				columnIndexes = uniqueColumnPositionArray(columnIndexes);
			} else { //prepare array of auto-generated keys for the table since user didn't provide any
				columnIndexes = generatedColumnPositionsArray();
			}

			rd = lcc.getLanguageFactory().getResultDescription(resultDescription,columnIndexes);
			autoGeneratedKeysRowsHolder = new TemporaryRowHolderImpl(tc, properties, rd);
		}


		while ( row != null )
		{
			if (activation.getAutoGeneratedKeysResultsetMode())
				autoGeneratedKeysRowsHolder.insert(getCompactRow(row, columnIndexes));

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
				// Evaluate any check constraints on the row
				evaluateCheckConstraints();

				if (fkChecker != null)
				{
					fkChecker.doFKCheck(row);
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
				rowChanger.insertRow(row);
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

		/*
		** If it's a deferred insert, scan the temporary conglomerate and
		** insert the rows into the permanent conglomerates using rowChanger.
		*/
		if (constants.deferred)
		{
			if (triggerInfo != null)
			{
				Vector v = null;
				if (aiCache != null)
				{
					v = new Vector();
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
										tc, 
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
												rowHolder.getResultSet());
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
					evaluateCheckConstraints();
					rowChanger.insertRow(deferredRowBuffer);
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
						fkChecker.doFKCheck(deferredRowBuffer);
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
										rowHolder.getResultSet());
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
    }

	/*
	 * Take the input row and return a new compact ExecRow
	 * using the column positions provided in columnIndexes.
	 * Copies references, no cloning.
	 */
	private ExecRow getCompactRow
	(
		ExecRow 					inputRow, 
		int[] 						columnIndexes
	)
		throws StandardException
	{
		ExecRow outRow;
		int numInputCols = inputRow.nColumns();

		if (columnIndexes == null)
		{
			outRow = new ValueRow(numInputCols);
			Object[] src = inputRow.getRowArray();
			Object[] dst = outRow.getRowArray();
			System.arraycopy(src, 0, dst, 0, src.length);
			return outRow;
		}

		int numOutputCols = columnIndexes.length;

		outRow = new ValueRow(numOutputCols);
		for (int i = 0; i < numOutputCols; i++)
		{
			outRow.setColumn(i+1,
				inputRow.getColumn(columnIndexes[i]));
		}

		return outRow;
	}

	// Do the work for a bulk insert
	private long bulkInsertCore(LanguageConnectionContext lcc,
								long oldHeapConglom)
		throws StandardException
	{
		fullTemplate = constants.getEmptyHeapRow(lcc);
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

		if (triggerInfo != null)
		{
			triggerActivator = new TriggerEventActivator(lcc, 
										tc, 
										constants.targetUUID,
										triggerInfo,
										TriggerExecutionContext.INSERT_EVENT,
										activation, null);
		}

		/*
		** If we have a before row trigger, then we
		** are going to use a row holder pass to our
		** trigger.
		*/
		if (hasBeforeRowTrigger && rowHolder != null)
		{
			rowHolder = new TemporaryRowHolderImpl(tc, properties, resultDescription);
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

		dd = lcc.getDataDictionary();
		td = dd.getTableDescriptor(constants.targetUUID);

		/* Do the bulk insert - only okay to reuse the
		 * same conglomerate if bulkInsert.
		 */
		long[] loadedRowCount = new long[1];
		if (bulkInsertReplace)
		{
			newHeapConglom = tc.createAndLoadConglomerate(
										"heap",
										fullTemplate.getRowArray(),
										null, //column sort order - not required for heap
										properties,
										TransactionController.IS_DEFAULT,
										sourceResultSet,
										loadedRowCount);
		}
		else
		{
			newHeapConglom = tc.recreateAndLoadConglomerate(
										"heap",
										false,
										fullTemplate.getRowArray(),
										null, //column sort order - not required for heap
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
			return oldHeapConglom;
		}

		// Find out how many rows were inserted
		rowCount = (int) loadedRowCount[0];

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

		lcc.autoincrementFlushCache(constants.targetUUID);

		// invalidate any prepared statements that
		// depended on this table (including this one)
		DependencyManager dm = dd.getDependencyManager();

		dm.invalidateFor(td, DependencyManager.BULK_INSERT, lcc);

		
		// Update all indexes
		if (constants.irgs.length > 0)
		{
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

		return newHeapConglom;
	}

	/*
	** Bulk Referential Integrity Checker
	*/
	private void bulkValidateForeignKeys(TransactionController tc, ContextManager cm)
		throws StandardException
	{
		FKInfo 			fkInfo;

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

		for (int i = 0; i < fkInfoArray.length; i++)
		{
			fkInfo = fkInfoArray[i];

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
						pkConglom = ((Long)indexConversionTable.get(
									new Long(fkInfo.refConglomNumber))).longValue();
						fkConglom = ((Long)indexConversionTable.get(
										new Long(fkInfo.fkConglomNumbers[index]))).longValue();
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
						Long pkConglomLong = (Long)indexConversionTable.get(
												new Long(fkInfo.refConglomNumber));
						Long fkConglomLong = (Long)indexConversionTable.get(
										new Long(fkInfo.fkConglomNumbers[index]));
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
							tc, cm, fkInfoArray[i], fkConglom, pkConglom, 
							fkInfo.fkConstraintNames[index]);
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
				Long fkConglom = (Long)indexConversionTable.get(
										new Long(fkInfo.fkConglomNumbers[0]));
				bulkValidateForeignKeysCore(
						tc, cm, fkInfoArray[i], fkConglom.longValue(),
						fkInfo.refConglomNumber, fkInfo.fkConstraintNames[0]);
			}
		}
	}

	private void bulkValidateForeignKeysCore(
						TransactionController tc, ContextManager cm, 
						FKInfo fkInfo, long fkConglom, long pkConglom,
						String fkConstraintName)
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
                        tc.MODE_TABLE,				 // doesn't matter, 
                                                     //   already locked
                        tc.ISOLATION_READ_COMMITTED, // doesn't matter, 
                                                     //   already locked
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
								tc.MODE_TABLE :
								tc.MODE_RECORD,
						tc.ISOLATION_READ_COMMITTED,	// read committed is 
                                                        //    good enough
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
				RIBulkChecker riChecker = new RIBulkChecker(refScan, 
											fkScan, 
											template, 	
											true, 				// fail on 1st failure
											(ConglomerateController)null,
											firstFailedRow);
	
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
				fkScan = null;
			}
			if (refScan != null)
			{
				refScan.close();
				refScan = null;
			}
		}
	}

	/**
	 * Make a template row with the correct columns.
	 */
	private ExecRow makeIndexTemplate(FKInfo fkInfo, ExecRow fullTemplate, ContextManager cm)
		throws StandardException
	{
		ExecRow newRow = RowUtil.getEmptyIndexRow(fkInfo.colArray.length+1, cm);

		DataValueDescriptor[] templateColArray = fullTemplate.getRowArray();
		DataValueDescriptor[] newRowColArray   = newRow.getRowArray();

		int i;
		for (i = 0; i < fkInfo.colArray.length; i++)
		{
			newRowColArray[i] = 
                (templateColArray[fkInfo.colArray[i] - 1]).getClone();
		}

		newRowColArray[i] = 
            (DataValueDescriptor) fkInfo.rowLocation.cloneObject();

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

		ordering = new ColumnOrdering[numIndexes][];
		needToDropSort = new boolean[numIndexes];
		sortIds = new long[numIndexes];
		rowSources = new RowLocationRetRowSource[numIndexes];
		// indexedCols is 1-based
		indexedCols = new FormatableBitSet(numColumns + 1);


		/* For each index, build a single index row and a sorter. */
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
			// (This call is only necessary here because we need to pass a template to the sorter.)
			constants.irgs[index].getIndexRow(sourceRow, 
											  rl, 
											  indexRows[index],
											  (FormatableBitSet) null);

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
			SortObserver sortObserver = null;
			/* We can only reuse the wrappers when doing an
			 * external sort if there is only 1 index.  Otherwise,
			 * we could get in a situation where 1 sort reuses a
			 * wrapper that is still in use in another sort.
			 */
			boolean reuseWrappers = (numIndexes == 1);
			if (cd.getIndexDescriptor().isUnique())
			{
				numColumnOrderings = baseColumnPositions.length;
				String[] columnNames = getColumnNames(baseColumnPositions);

				String indexOrConstraintName = cd.getConglomerateName();
				if (cd.isConstraint()) // so, the index is backing up a constraint
				{
					ConstraintDescriptor conDesc = dd.getConstraintDescriptor(td,
                                                                      cd.getUUID());
					indexOrConstraintName = conDesc.getConstraintName();
				}
				sortObserver = new UniqueIndexSortObserver(
														false, // don't clone rows
														cd.isConstraint(), 
														indexOrConstraintName,
														indexRows[index],
														reuseWrappers,
														td.getName());
			}
			else
			{
				numColumnOrderings = baseColumnPositions.length + 1;
				sortObserver = new BasicSortObserver(false, false, 
													 indexRows[index],
													 reuseWrappers);
			}
			ordering[index] = new ColumnOrdering[numColumnOrderings];
			for (int ii =0; ii < isAscending.length; ii++) 
			{
				ordering[index][ii] = new IndexColumnOrder(ii, isAscending[ii]);
			}
			if (numColumnOrderings > isAscending.length)
				ordering[index][isAscending.length] = new IndexColumnOrder(isAscending.length);

			// create the sorters
			sortIds[index] = tc.createSort(
								(Properties)null, 
								indexRows[index].getRowArrayClone(),
								ordering[index],
								sortObserver,
								false,			// not in order
								(int) sourceResultSet.getEstimatedRowCount(),		// est rows	
								-1				// est row size, -1 means no idea	
								);
			needToDropSort[index] = true;
		}

		sorters = new SortController[numIndexes];

		// Open the sorts
		for (int index = 0; index < numIndexes; index++)
		{
			sorters[index] = tc.openSort(sortIds[index]);
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

		indexConversionTable = new Hashtable(numIndexes);
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
			properties.put("rowLocationColumn", 
							Integer.toString(indexRowLength - 1));
			properties.put("nKeyFields", Integer.toString(indexRowLength));

			indexCC.close();

			// We can finally drain the sorter and rebuild the index
			// RESOLVE - all indexes are btrees right now
			// Populate the index.
			sorters[index].close();
			sorters[index] = null;
			rowSources[index] = new CardinalityCounter(tc.openSortRowSource(sortIds[index]));
			newIndexCongloms[index] = tc.createAndLoadConglomerate(
										"BTREE",
										indexRows[index].getRowArray(),
										ordering[index],
										properties,
										TransactionController.IS_DEFAULT,
										rowSources[index],
										(long[]) null);

			CardinalityCounter cCount = (CardinalityCounter)rowSources[index];
			long numRows;
			if ((numRows = cCount.getRowCount()) > 0)
			{
				long[] c = cCount.getCardinality();
				DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

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

			indexConversionTable.put(new Long(constants.indexCIDS[index]),
									new Long(newIndexCongloms[index]));
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
		super.close();
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
	 * @return Nothing
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
	 * @return Nothing.
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
		ExecIndexRow[]		indexRows = new ExecIndexRow[numIndexes];
		ExecRow				baseRows = null;
		ColumnOrdering[][]	ordering = new ColumnOrdering[numIndexes][];
		int					numColumns = td.getNumberOfColumns();

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
		baseRows = activation.getExecutionFactory().getValueRow(numReferencedColumns);

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
						fullTemplate.getColumn(index + 1).getClone());
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
			indexRows[index] = constants.irgs[index].getIndexRowTemplate();

			// Get an index row based on the base row
			// (This call is only necessary here because we need to pass a template to the sorter.)
			constants.irgs[index].getIndexRow(baseRows, 
											  rl, 
											  indexRows[index],
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
			SortObserver sortObserver = null;
			if (cd.getIndexDescriptor().isUnique())
			{
				numColumnOrderings = baseColumnPositions.length;
				String[] columnNames = getColumnNames(baseColumnPositions);

				String indexOrConstraintName = cd.getConglomerateName();
				if (cd.isConstraint()) // so, the index is backing up a constraint
				{
					ConstraintDescriptor conDesc = dd.getConstraintDescriptor(td,
                                                                      cd.getUUID());
					indexOrConstraintName = conDesc.getConstraintName();
				}
				sortObserver = new UniqueIndexSortObserver(
														false, // don't clone rows
														cd.isConstraint(), 
														indexOrConstraintName,
														indexRows[index],
														true,
														td.getName());
			}
			else
			{
				numColumnOrderings = baseColumnPositions.length + 1;
				sortObserver = new BasicSortObserver(false, false, 
													 indexRows[index],
													 true);
			}
			ordering[index] = new ColumnOrdering[numColumnOrderings];
			for (int ii =0; ii < isAscending.length; ii++) 
			{
				ordering[index][ii] = new IndexColumnOrder(ii, isAscending[ii]);
			}
			if (numColumnOrderings > isAscending.length)
				ordering[index][isAscending.length] = new IndexColumnOrder(isAscending.length);

			// create the sorters
			sortIds[index] = tc.createSort(
								(Properties)null, 
								indexRows[index].getRowArrayClone(),
								ordering[index],
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
		SortController[]	sorters = new SortController[numIndexes];
		for (int index = 0; index < numIndexes; index++)
		{
			sorters[index] = tc.openSort(sortIds[index]);
			sorters[index].close();
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
			newIndexCongloms[index] = tc.createAndLoadConglomerate(
										"BTREE",
										indexRows[index].getRowArray(),
										null, //default column sort order 
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
			tableScan = new BulkTableScanResultSet(
							conglomId,
							tc.getStaticCompiledConglomInfo(conglomId),
							activation,
							new MyRowAllocator(fullTemplate),	// result row allocator
							0,						// result set number
							(GeneratedMethod)null, 	// start key getter
							0, 						// start search operator
							(GeneratedMethod)null,	// stop key getter
							0, 						// start search operator
							false,
							(Qualifier[][])null,	// qualifiers
							"tableName",
							(String)null,			// index name
							false,					// is constraint
							false,					// for update
							-1,						// saved object for referenced bitImpl
							-1,
							tc.MODE_TABLE,
							true,					// table locked
							tc.ISOLATION_READ_COMMITTED,
							LanguageProperties.BULK_FETCH_DEFAULT_INT,	// rows per read
							false,					// not a 1 row per scan
							0d,						// estimated rows
							0d, 					// estimated cost
							(GeneratedMethod)null);	// close cleanup
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

	public void finish() throws StandardException {
		sourceResultSet.finish();
		super.finish();
	}


	// inner class to be our row template constructor
	class MyRowAllocator implements GeneratedMethod
	{
		private ExecRow row;
		MyRowAllocator(ExecRow row)
		{
			this.row = row;
		}

		public Object invoke(Object ref)
		{
			return row.getClone();
		}
	}
}

