/*

   Derby - Class org.apache.derby.impl.sql.compile.FromBaseTable

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

package	org.apache.derby.impl.sql.compile;

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.derby.catalog.IndexDescriptor;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.FormatableIntHolder;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.LanguageProperties;
import org.apache.derby.iapi.sql.compile.AccessPath;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.JoinStrategy;
import org.apache.derby.iapi.sql.compile.OptimizablePredicate;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.RequiredRowOrdering;
import org.apache.derby.iapi.sql.compile.RowOrdering;
import org.apache.derby.iapi.sql.compile.TagFilter;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptorList;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.ViewDescriptor;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.StoreCostController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.transaction.TransactionControl;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.util.JBitSet;
import org.apache.derby.iapi.util.StringUtil;

// Temporary until user override for disposable stats has been removed.
import org.apache.derby.impl.services.daemon.IndexStatisticsDaemonImpl;
import org.apache.derby.impl.sql.catalog.SYSUSERSRowFactory;

/**
 * A FromBaseTable represents a table in the FROM list of a DML statement,
 * as distinguished from a FromSubquery, which represents a subquery in the
 * FROM list. A FromBaseTable may actually represent a view.  During parsing,
 * we can't distinguish views from base tables. During binding, when we
 * find FromBaseTables that represent views, we replace them with FromSubqueries.
 * By the time we get to code generation, all FromSubqueries have been eliminated,
 * and all FromBaseTables will represent only true base tables.
 * <p>
 * <B>Positioned Update</B>: Currently, all columns of an updatable cursor
 * are selected to deal with a positioned update.  This is because we don't
 * know what columns will ultimately be needed from the UpdateNode above
 * us.  For example, consider:<pre><i>
 *
 * 	get c as 'select cint from t for update of ctinyint'
 *  update t set ctinyint = csmallint
 *
 * </pre></i> Ideally, the cursor only selects cint.  Then,
 * something akin to an IndexRowToBaseRow is generated to
 * take the CursorResultSet and get the appropriate columns
 * out of the base table from the RowLocation returned by the
 * cursor.  Then the update node can generate the appropriate
 * NormalizeResultSet (or whatever else it might need) to
 * get things into the correct format for the UpdateResultSet.
 * See CurrentOfNode for more information.
 *
 */

class FromBaseTable extends FromTable
{
	static final int UNSET = -1;

    /**
     * Whether or not we have checked the index statistics for staleness.
     * Used to avoid performing the check multiple times per compilation.
     */
    private boolean hasCheckedIndexStats;

	TableName		tableName;
	TableDescriptor	tableDescriptor;

	ConglomerateDescriptor		baseConglomerateDescriptor;
	ConglomerateDescriptor[]	conglomDescs;

	int				updateOrDelete;
	
	/*
	** The number of rows to bulkFetch.
	** Initially it is unset.  If the user
	** uses the bulkFetch table property,	
	** it is set to that.  Otherwise, it
	** may be turned on if it isn't an updatable
	** cursor and it is the right type of
	** result set (more than 1 row expected to
	** be returned, and not hash, which does its
	** own bulk fetch, and subquery).
	*/
	int 			bulkFetch = UNSET;

    /*
    ** Used to validate deferred check constraints.
    ** It is the conglomerate number of the target inserted into or updated
    ** when a violation was detected but deferred.
    */
    private long            targetTableCID;
    private boolean         validatingCheckConstraint = false;

	/* We may turn off bulk fetch for a variety of reasons,
	 * including because of the min optimization.  
	 * bulkFetchTurnedOff is set to true in those cases.
	 */
	boolean			bulkFetchTurnedOff;
	
	/* Whether or not we are going to do execution time "multi-probing"
	 * on the table scan for this FromBaseTable.
	 */
	boolean			multiProbing = false;

	private double	singleScanRowCount;

	private FormatableBitSet referencedCols;
	private ResultColumnList templateColumns;

	/* A 0-based array of column names for this table used
	 * for optimizer trace.
	 */
	private String[] columnNames;

	// true if we are to do a special scan to retrieve the last value
	// in the index
	private boolean specialMaxScan;

	// true if we are to do a distinct scan
	private boolean distinctScan;

	/**
	 *Information for dependent table scan for Referential Actions
	 */
	private boolean raDependentScan;
	private String raParentResultSetId;
	private long fkIndexConglomId;	
	private int[] fkColArray;

	/**
	 * Restriction as a PredicateList
	 */
	PredicateList baseTableRestrictionList;
	PredicateList nonBaseTableRestrictionList;
	PredicateList restrictionList;
	PredicateList storeRestrictionList;
	PredicateList nonStoreRestrictionList;
	PredicateList requalificationRestrictionList;

    static final int UPDATE = 1;
    static final int DELETE = 2;

	/* Variables for EXISTS FBTs */
	private boolean	existsBaseTable;
	private boolean	isNotExists;  //is a NOT EXISTS base table
	private JBitSet dependencyMap;

	private boolean getUpdateLocks;

    // true if we are running with sql authorization and this is the SYSUSERS table
    private boolean authorizeSYSUSERS;

    // non-null if we need to return a row location column
    private String  rowLocationColumnName;

	/**
     * Constructor for a table in a FROM list. Parameters are as follows:
	 *
     * @param tableName         The name of the table
     * @param correlationName   The correlation name
     * @param derivedRCL        The derived column list
     * @param tableProperties   The Properties list associated with the table.
     * @param cm                The context manager
     */
    FromBaseTable(TableName tableName,
                  String correlationName,
                  ResultColumnList derivedRCL,
                  Properties tableProperties,
                  ContextManager cm)
    {
        super(correlationName, tableProperties, cm);
        this.tableName = tableName;
        setResultColumns( derivedRCL );
        setOrigTableName(this.tableName);
        templateColumns = getResultColumns();
    }

    /**
     * Initializer for a table in a FROM list. Parameters are as follows:
	 *
     * @param tableName     The name of the table
     * @param correlationName   The correlation name
     * @param updateOrDelete    Table is being updated/deleted from.
     * @param derivedRCL        The derived column list
     * @param cm               The context manager
	 */
    FromBaseTable(TableName tableName,
                  String correlationName,
                  int updateOrDelete,
                  ResultColumnList derivedRCL,
                  ContextManager cm)
	{
        super(correlationName, null, cm);
        this.tableName = tableName;
        this.updateOrDelete = updateOrDelete;
        setResultColumns( derivedRCL );
		setOrigTableName(this.tableName);
		templateColumns = getResultColumns();
	}

    /** Set the name of the row location column */
    void    setRowLocationColumnName( String rowLocationColumnName )
    {
        this.rowLocationColumnName = rowLocationColumnName;
    }

    /**
	 * no LOJ reordering for base table.
	 */
    @Override
    boolean LOJ_reorderable(int numTables)
				throws StandardException
	{
		return false;
	}

    @Override
    JBitSet LOJgetReferencedTables(int numTables)
				throws StandardException
	{
		JBitSet map = new JBitSet(numTables);
		fillInReferencedTableMap(map);
		return map;
	}

	/*
	 * Optimizable interface.
	 */

	/**
	 * @see org.apache.derby.iapi.sql.compile.Optimizable#nextAccessPath
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public boolean nextAccessPath(Optimizer optimizer,
									OptimizablePredicateList predList,
									RowOrdering rowOrdering)
					throws StandardException
	{
		String userSpecifiedIndexName = getUserSpecifiedIndexName();
		AccessPath ap = getCurrentAccessPath();
		ConglomerateDescriptor currentConglomerateDescriptor =
												ap.getConglomerateDescriptor();

        if ( optimizerTracingIsOn() )
        { getOptimizerTracer().traceNextAccessPath( getExposedName(), ((predList == null) ? 0 : predList.size()) ); }

		/*
		** Remove the ordering of the current conglomerate descriptor,
		** if any.
		*/
		rowOrdering.removeOptimizable(getTableNumber());

		// RESOLVE: This will have to be modified to step through the
		// join strategies as well as the conglomerates.

		if (userSpecifiedIndexName != null)
		{
			/*
			** User specified an index name, so we should look at only one
			** index.  If there is a current conglomerate descriptor, and there
			** are no more join strategies, we've already looked at the index,
			** so go back to null.
			*/
			if (currentConglomerateDescriptor != null)
			{
				if ( ! super.nextAccessPath(optimizer,
											predList,
											rowOrdering) )
				{
					currentConglomerateDescriptor = null;
				}
			}
			else
			{
                if ( optimizerTracingIsOn() )
                { getOptimizerTracer().traceLookingForSpecifiedIndex( userSpecifiedIndexName, tableNumber ); }

				if (StringUtil.SQLToUpperCase(userSpecifiedIndexName).equals("NULL"))
				{
					/* Special case - user-specified table scan */
					currentConglomerateDescriptor =
						tableDescriptor.getConglomerateDescriptor(
										tableDescriptor.getHeapConglomerateId()
									);
				}
				else
				{
					/* User-specified index name */
					getConglomDescs();
				
					for (int index = 0; index < conglomDescs.length; index++)
					{
						currentConglomerateDescriptor = conglomDescs[index];
						String conglomerateName =
							currentConglomerateDescriptor.getConglomerateName();
						if (conglomerateName != null)
						{
							/* Have we found the desired index? */
							if (conglomerateName.equals(userSpecifiedIndexName))
							{
								break;
							}
						}
					}

					/* We should always find a match */
					if (SanityManager.DEBUG)
					{
						if (currentConglomerateDescriptor == null)
						{
							SanityManager.THROWASSERT(
								"Expected to find match for forced index " +
								userSpecifiedIndexName);
						}
					}
				}

				if ( ! super.nextAccessPath(optimizer,
											predList,
											rowOrdering))
				{
					if (SanityManager.DEBUG)
					{
						SanityManager.THROWASSERT("No join strategy found");
					}
				}
			}
		}
		else
		{
			if (currentConglomerateDescriptor != null)
			{
				/* 
				** Once we have a conglomerate descriptor, cycle through
				** the join strategies (done in parent).
				*/
				if ( ! super.nextAccessPath(optimizer,
											predList,
											rowOrdering))
				{
					/*
					** When we're out of join strategies, go to the next
					** conglomerate descriptor.
					*/
					currentConglomerateDescriptor = getNextConglom(currentConglomerateDescriptor);

					/*
					** New conglomerate, so step through join strategies
					** again.
					*/
					resetJoinStrategies(optimizer);

					if ( ! super.nextAccessPath(optimizer,
												predList,
												rowOrdering))
					{
						if (SanityManager.DEBUG)
						{
							SanityManager.THROWASSERT("No join strategy found");
						}
					}
				}
			}
			else
			{
				/* Get the first conglomerate descriptor */
				currentConglomerateDescriptor = getFirstConglom();

				if ( ! super.nextAccessPath(optimizer,
											predList,
											rowOrdering))
				{
					if (SanityManager.DEBUG)
					{
						SanityManager.THROWASSERT("No join strategy found");
					}
				}
			}
		}

		if (currentConglomerateDescriptor == null)
		{
            if ( optimizerTracingIsOn() ) { getOptimizerTracer().traceNoMoreConglomerates( tableNumber ); }
		}
		else
		{
			currentConglomerateDescriptor.setColumnNames(columnNames);

            if ( optimizerTracingIsOn() ) { getOptimizerTracer().traceConsideringConglomerate( currentConglomerateDescriptor, tableNumber ); }
		}

		/*
		** Tell the rowOrdering that what the ordering of this conglomerate is
		*/
		if (currentConglomerateDescriptor != null)
		{
			if ( ! currentConglomerateDescriptor.isIndex())
			{
				/* If we are scanning the heap, but there
				 * is a full match on a unique key, then
				 * we can say that the table IS NOT unordered.
				 * (We can't currently say what the ordering is
				 * though.)
				 */
				if (! isOneRowResultSet(predList))
				{
                    if ( optimizerTracingIsOn() )
                    { getOptimizerTracer().traceAddingUnorderedOptimizable( ((predList == null) ? 0 : predList.size()) ); }

					rowOrdering.addUnorderedOptimizable(this);
				}
				else
				{
                    if ( optimizerTracingIsOn() ) { getOptimizerTracer().traceScanningHeapWithUniqueKey(); }
				}
			}
			else
			{
				IndexRowGenerator irg =
							currentConglomerateDescriptor.getIndexDescriptor();

				int[] baseColumnPositions = irg.baseColumnPositions();
				boolean[] isAscending = irg.isAscending();

				for (int i = 0; i < baseColumnPositions.length; i++)
				{
					/*
					** Don't add the column to the ordering if it's already
					** an ordered column.  This can happen in the following
					** case:
					**
					**		create index ti on t(x, y);
					**		select * from t where x = 1 order by y;
					**
					** Column x is always ordered, so we want to avoid the
					** sort when using index ti.  This is accomplished by
					** making column y appear as the first ordered column
					** in the list.
					*/
					if ( ! rowOrdering.orderedOnColumn(isAscending[i] ?
													RowOrdering.ASCENDING :
													RowOrdering.DESCENDING,
													getTableNumber(),
													baseColumnPositions[i]))
					{
						rowOrdering.nextOrderPosition(isAscending[i] ?
													RowOrdering.ASCENDING :
													RowOrdering.DESCENDING);

						rowOrdering.addOrderedColumn(isAscending[i] ?
													RowOrdering.ASCENDING :
													RowOrdering.DESCENDING,
													getTableNumber(),
													baseColumnPositions[i]);
					}
				}
			}	
		}

		ap.setConglomerateDescriptor(currentConglomerateDescriptor);

		return currentConglomerateDescriptor != null;
	}

	/** Tell super-class that this Optimizable can be ordered */
    @Override
	protected boolean canBeOrdered()
	{
		return true;
	}

	/**
	 * @see org.apache.derby.iapi.sql.compile.Optimizable#optimizeIt
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public CostEstimate optimizeIt(
				Optimizer optimizer,
				OptimizablePredicateList predList,
				CostEstimate outerCost,
				RowOrdering rowOrdering)
			throws StandardException
	{
		optimizer.costOptimizable(
							this,
							tableDescriptor,
							getCurrentAccessPath().getConglomerateDescriptor(),
							predList,
							outerCost);

		// The cost that we found from the above call is now stored in the
		// cost field of this FBT's current access path.  So that's the
		// cost we want to return here.
		return getCurrentAccessPath().getCostEstimate();
	}

	/** @see org.apache.derby.iapi.sql.compile.Optimizable#getTableDescriptor */
    @Override
	public TableDescriptor getTableDescriptor()
	{
		return tableDescriptor;
	}


	/** @see org.apache.derby.iapi.sql.compile.Optimizable#isMaterializable 
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public boolean isMaterializable()
		throws StandardException
	{
		/* base tables are always materializable */
		return true;
	}


	/**
	 * @see org.apache.derby.iapi.sql.compile.Optimizable#pushOptPredicate
	 *
	 * @exception StandardException		Thrown on error
	 */

    @Override
	public boolean pushOptPredicate(OptimizablePredicate optimizablePredicate)
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(optimizablePredicate instanceof Predicate,
				"optimizablePredicate expected to be instanceof Predicate");
		}

		/* Add the matching predicate to the restrictionList */
		restrictionList.addPredicate((Predicate) optimizablePredicate);

		return true;
	}

	/**
	 * @see org.apache.derby.iapi.sql.compile.Optimizable#pullOptPredicates
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public void pullOptPredicates(
								OptimizablePredicateList optimizablePredicates)
					throws StandardException
	{
		for (int i = restrictionList.size() - 1; i >= 0; i--) {
			optimizablePredicates.addOptPredicate(
									restrictionList.getOptPredicate(i));
			restrictionList.removeOptPredicate(i);
		}
	}

	/** 
	 * @see org.apache.derby.iapi.sql.compile.Optimizable#isCoveringIndex
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public boolean isCoveringIndex(ConglomerateDescriptor cd) throws StandardException
	{
		boolean coveringIndex = true;
		IndexRowGenerator	irg;
		int[]				baseCols;
		int					colPos;

		/* You can only be a covering index if you're an index */
		if ( ! cd.isIndex())
			return false;

		irg = cd.getIndexDescriptor();
		baseCols = irg.baseColumnPositions();

		/* First we check to see if this is a covering index */
        for (ResultColumn rc : getResultColumns())
		{
			/* Ignore unreferenced columns */
			if (! rc.isReferenced())
			{
				continue;
			}

			/* Ignore constants - this can happen if all of the columns
			 * were projected out and we ended up just generating
			 * a "1" in RCL.doProject().
			 */
			if (rc.getExpression() instanceof ConstantNode)
			{
				continue;
			}

			coveringIndex = false;

			colPos = rc.getColumnPosition();

			/* Is this column in the index? */
			for (int i = 0; i < baseCols.length; i++)
			{
				if (colPos == baseCols[i])
				{
					coveringIndex = true;
					break;
				}
			}

			/* No need to continue if the column was not in the index */
			if (! coveringIndex)
			{
				break;
			}
		}
		return coveringIndex;
	}

	/** @see org.apache.derby.iapi.sql.compile.Optimizable#verifyProperties 
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public void verifyProperties(DataDictionary dDictionary)
		throws StandardException
	{
		if (tableProperties == null)
		{
			return;
		}
		/* Check here for:
		 *		invalid properties key
		 *		index and constraint properties
		 *		non-existent index
		 *		non-existent constraint
		 *		invalid joinStrategy
		 *		invalid value for hashInitialCapacity
		 *		invalid value for hashLoadFactor
		 *		invalid value for hashMaxCapacity
		 */
		boolean indexSpecified = false;
		boolean constraintSpecified = false;
		ConstraintDescriptor consDesc = null;
        Enumeration<?> e = tableProperties.keys();

        StringUtil.SQLEqualsIgnoreCase(tableDescriptor.getSchemaName(), "SYS");
		while (e.hasMoreElements())
		{
			String key = (String) e.nextElement();
			String value = (String) tableProperties.get(key);

			if (key.equals("index"))
			{
				// User only allowed to specify 1 of index and constraint, not both
				if (constraintSpecified)
				{
					throw StandardException.newException(SQLState.LANG_BOTH_FORCE_INDEX_AND_CONSTRAINT_SPECIFIED, 
								getBaseTableName());
				}
				indexSpecified = true;

				/* Validate index name - NULL means table scan */
				if (! StringUtil.SQLToUpperCase(value).equals("NULL"))
				{
					ConglomerateDescriptor cd = null;
					ConglomerateDescriptor[] cds = tableDescriptor.getConglomerateDescriptors();

					for (int index = 0; index < cds.length; index++)
					{
						cd = cds[index];
						String conglomerateName = cd.getConglomerateName();
						if (conglomerateName != null)
						{
							if (conglomerateName.equals(value))
							{
								break;
							}
						}
						// Not a match, clear cd
						cd = null;
					}

					// Throw exception if user specified index not found
					if (cd == null)
					{
						throw StandardException.newException(SQLState.LANG_INVALID_FORCED_INDEX1, 
										value, getBaseTableName());
					}
					/* Query is dependent on the ConglomerateDescriptor */
					getCompilerContext().createDependency(cd);
				}
			}
			else if (key.equals("constraint"))
			{
				// User only allowed to specify 1 of index and constraint, not both
				if (indexSpecified)
				{
					throw StandardException.newException(SQLState.LANG_BOTH_FORCE_INDEX_AND_CONSTRAINT_SPECIFIED, 
								getBaseTableName());
				}
				constraintSpecified = true;

				if (! StringUtil.SQLToUpperCase(value).equals("NULL"))
				{
					consDesc = 
						dDictionary.getConstraintDescriptorByName(
									tableDescriptor, (SchemaDescriptor)null, value,
									false);

					/* Throw exception if user specified constraint not found
					 * or if it does not have a backing index.
					 */
					if ((consDesc == null) || ! consDesc.hasBackingIndex())
					{
						throw StandardException.newException(SQLState.LANG_INVALID_FORCED_INDEX2, 
										value, getBaseTableName());
					}

					/* Query is dependent on the ConstraintDescriptor */
					getCompilerContext().createDependency(consDesc);
				}
			}
			else if (key.equals("joinStrategy"))
			{
				userSpecifiedJoinStrategy = StringUtil.SQLToUpperCase(value);
			}
			else if (key.equals("hashInitialCapacity"))
			{
				initialCapacity = getIntProperty(value, key);

				// verify that the specified value is valid
				if (initialCapacity <= 0)
				{
					throw StandardException.newException(SQLState.LANG_INVALID_HASH_INITIAL_CAPACITY, 
							String.valueOf(initialCapacity));
				}
			}
			else if (key.equals("hashLoadFactor"))
			{
				try
				{
					loadFactor = Float.parseFloat(value);
				}
				catch (NumberFormatException nfe)
				{
					throw StandardException.newException(SQLState.LANG_INVALID_NUMBER_FORMAT_FOR_OVERRIDE, 
							value, key);
				}

				// verify that the specified value is valid
				if (loadFactor <= 0.0 || loadFactor > 1.0)
				{
					throw StandardException.newException(SQLState.LANG_INVALID_HASH_LOAD_FACTOR, 
							value);
				}
			}
			else if (key.equals("hashMaxCapacity"))
			{
				maxCapacity = getIntProperty(value, key);

				// verify that the specified value is valid
				if (maxCapacity <= 0)
				{
					throw StandardException.newException(SQLState.LANG_INVALID_HASH_MAX_CAPACITY, 
							String.valueOf(maxCapacity));
				}
			}
			else if (key.equals("bulkFetch"))
			{
				bulkFetch = getIntProperty(value, key);

				// verify that the specified value is valid
				if (bulkFetch <= 0)
				{
					throw StandardException.newException(SQLState.LANG_INVALID_BULK_FETCH_VALUE, 
							String.valueOf(bulkFetch));
				}
			
				// no bulk fetch on updatable scans
				if (forUpdate())
				{
					throw StandardException.newException(SQLState.LANG_INVALID_BULK_FETCH_UPDATEABLE);
				}
			}
            else if (key.equals("validateCheckConstraint")) {
                // the property "validateCheckConstraint" is read earlier
                // cf. isValidatingCheckConstraint
            }
			else
			{
				// No other "legal" values at this time
				throw StandardException.newException(SQLState.LANG_INVALID_FROM_TABLE_PROPERTY, key,
					"index, constraint, joinStrategy");
			}
		}

		/* If user specified a non-null constraint name(DERBY-1707), then  
		 * replace it in the properties list with the underlying index name to 
		 * simplify the code in the optimizer.
		 * NOTE: The code to get from the constraint name, for a constraint
		 * with a backing index, to the index name is convoluted.  Given
		 * the constraint name, we can get the conglomerate id from the
		 * ConstraintDescriptor.  We then use the conglomerate id to get
		 * the ConglomerateDescriptor from the DataDictionary and, finally,
		 * we get the index name (conglomerate name) from the ConglomerateDescriptor.
		 */
		if (constraintSpecified && consDesc != null)
		{
			ConglomerateDescriptor cd = 
				dDictionary.getConglomerateDescriptor(
					consDesc.getConglomerateId());
			String indexName = cd.getConglomerateName();

			tableProperties.remove("constraint");
			tableProperties.put("index", indexName);
		}
	}

    private boolean isValidatingCheckConstraint() throws StandardException {
        if (tableProperties == null) {
            return false;
        }

        for (Enumeration<?> e = tableProperties.keys(); e.hasMoreElements();) {
            String key = (String)e.nextElement();
            String value = (String) tableProperties.get(key);
            if (key.equals("validateCheckConstraint")) {
                targetTableCID = getLongProperty(value, key);
                validatingCheckConstraint = true;
                return true;
            }
        }
        return false;
    }

	/** @see org.apache.derby.iapi.sql.compile.Optimizable#getBaseTableName */
    @Override
	public String getBaseTableName()
	{
		return tableName.getTableName();
	}

	/** @see org.apache.derby.iapi.sql.compile.Optimizable#startOptimizing */
    @Override
	public void startOptimizing(Optimizer optimizer, RowOrdering rowOrdering)
	{
		AccessPath ap = getCurrentAccessPath();
		AccessPath bestAp = getBestAccessPath();
		AccessPath bestSortAp = getBestSortAvoidancePath();

		ap.setConglomerateDescriptor((ConglomerateDescriptor) null);
		bestAp.setConglomerateDescriptor((ConglomerateDescriptor) null);
		bestSortAp.setConglomerateDescriptor((ConglomerateDescriptor) null);
		ap.setCoveringIndexScan(false);
		bestAp.setCoveringIndexScan(false);
		bestSortAp.setCoveringIndexScan(false);
		ap.setLockMode(0);
		bestAp.setLockMode(0);
		bestSortAp.setLockMode(0);

		/*
		** Only need to do this for current access path, because the
		** costEstimate will be copied to the best access paths as
		** necessary.
		*/
        CostEstimate costEst = getCostEstimate(optimizer);
        ap.setCostEstimate(costEst);

		/*
		** This is the initial cost of this optimizable.  Initialize it
		** to the maximum cost so that the optimizer will think that
		** any access path is better than none.
		*/
        costEst.setCost(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE);

		super.startOptimizing(optimizer, rowOrdering);
	}

	/** @see org.apache.derby.iapi.sql.compile.Optimizable#convertAbsoluteToRelativeColumnPosition */
    @Override
	public int convertAbsoluteToRelativeColumnPosition(int absolutePosition)
	{
		return mapAbsoluteToRelativeColumnPosition(absolutePosition);
	}

	/**
     * <p>
     * Estimate the cost of scanning this {@code FromBaseTable} using the
     * given predicate list with the given conglomerate.
     * </p>
     *
     * <p>
     * If the table contains little data, the cost estimate might be adjusted
     * to make it more likely that an index scan will be preferred to a table
     * scan, and a unique index will be preferred to a non-unique index. Even
     * though such a plan may be slightly suboptimal when seen in isolation,
     * using indexes, unique indexes in particular, needs fewer locks and
     * allows more concurrency.
     * </p>
     *
	 * @see org.apache.derby.iapi.sql.compile.Optimizable#estimateCost
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public CostEstimate estimateCost(OptimizablePredicateList predList,
									ConglomerateDescriptor cd,
									CostEstimate outerCost,
									Optimizer optimizer,
									RowOrdering rowOrdering)
			throws StandardException
	{
		double cost;
		boolean statisticsForTable = false;
		boolean statisticsForConglomerate = false;
		/* unknownPredicateList contains all predicates whose effect on
		 * cost/selectivity can't be calculated by the store.
		 */
		PredicateList unknownPredicateList = null;

		if (optimizer.useStatistics() && predList != null)
		{
			/* if user has specified that we don't use statistics,
			   pretend that statistics don't exist.
			*/
			statisticsForConglomerate = tableDescriptor.statisticsExist(cd);
			statisticsForTable = tableDescriptor.statisticsExist(null);
            unknownPredicateList = new PredicateList(getContextManager());
			predList.copyPredicatesToOtherList(unknownPredicateList);
            // If not already done, check if this table has indexes and if
            // their statistics need to get updated.
            if (!hasCheckedIndexStats) {
                hasCheckedIndexStats = true;
                // Only mark if a base table and there are indexes. Skip VTIs,
                // system tables, subqueries etc.
                // The case where we have a table with a single-column unique
                // index is pretty common, so avoid engaging the istat
                // daemon if that's the only index on the table.
                if (qualifiesForStatisticsUpdateCheck(tableDescriptor)) {
                    tableDescriptor.markForIndexStatsUpdate(baseRowCount());
                }
            }
        }

        AccessPath currAccessPath = getCurrentAccessPath();
		JoinStrategy currentJoinStrategy = 
            currAccessPath.getJoinStrategy();

        if ( optimizerTracingIsOn() ) { getOptimizerTracer().traceEstimatingCostOfConglomerate( cd, tableNumber ); }

		/* Get the uniqueness factory for later use (see below) */
		double tableUniquenessFactor =
				optimizer.uniqueJoinWithOuterTable(predList);

		boolean oneRowResultSetForSomeConglom = isOneRowResultSet(predList);

		/* Get the predicates that can be used for scanning the base table */
		baseTableRestrictionList.removeAllElements();

		currentJoinStrategy.getBasePredicates(predList,	
									   baseTableRestrictionList,
									   this);
									
		/* RESOLVE: Need to figure out how to cache the StoreCostController */
		StoreCostController scc = getStoreCostController(cd);

        CostEstimate costEst = getScratchCostEstimate(optimizer);

		/* First, get the cost for one scan */

		/* Does the conglomerate match at most one row? */
		if (isOneRowResultSet(cd, baseTableRestrictionList))
		{
			/*
			** Tell the RowOrdering that this optimizable is always ordered.
			** It will figure out whether it is really always ordered in the
			** context of the outer tables and their orderings.
			*/
			rowOrdering.optimizableAlwaysOrdered(this);

			singleScanRowCount = 1.0;

			/* Yes, the cost is to fetch exactly one row */
			// RESOLVE: NEED TO FIGURE OUT HOW TO GET REFERENCED COLUMN LIST,
			// FIELD STATES, AND ACCESS TYPE
			cost = scc.getFetchFromFullKeyCost(
										(FormatableBitSet) null,
										0);

            if ( optimizerTracingIsOn() ) { getOptimizerTracer().traceSingleMatchedRowCost( cost, tableNumber ); }

            costEst.setCost(cost, 1.0d, 1.0d);

			/*
			** Let the join strategy decide whether the cost of the base
			** scan is a single scan, or a scan per outer row.
			** NOTE: The multiplication should only be done against the
			** total row count, not the singleScanRowCount.
			*/
            double newCost = costEst.getEstimatedCost();

			if (currentJoinStrategy.multiplyBaseCostByOuterRows())
			{
				newCost *= outerCost.rowCount();
			}

            costEst.setCost(
				newCost,
                costEst.rowCount() * outerCost.rowCount(),
                costEst.singleScanRowCount());

			/*
			** Choose the lock mode.  If the start/stop conditions are
			** constant, choose row locking, because we will always match
			** the same row.  If they are not constant (i.e. they include
			** a join), we decide whether to do row locking based on
			** the total number of rows for the life of the query.
			*/
			boolean constantStartStop = true;
			for (int i = 0; i < predList.size(); i++)
			{
				OptimizablePredicate pred = predList.getOptPredicate(i);

				/*
				** The predicates are in index order, so the start and
				** stop keys should be first.
				*/
				if ( ! (pred.isStartKey() || pred.isStopKey()))
				{
					break;
				}

				/* Stop when we've found a join */
				if ( ! pred.getReferencedMap().hasSingleBitSet())
				{
					constantStartStop = false;
					break;
				}
			}

			if (constantStartStop)
			{
                currAccessPath.setLockMode(
											TransactionController.MODE_RECORD);

                if ( optimizerTracingIsOn() ) { getOptimizerTracer().traceConstantStartStopPositions(); }
			}
			else
			{
                setLockingBasedOnThreshold(optimizer, costEst.rowCount());
			}

            if (optimizerTracingIsOn()) {
                getOptimizerTracer().traceCostOfNScans(
                    tableNumber,
                    outerCost.rowCount(),
                    costEst );
            }

			/* Add in cost of fetching base row for non-covering index */
			if (cd.isIndex() && ( ! isCoveringIndex(cd) ) )
			{
				double singleFetchCost =
						getBaseCostController().getFetchFromRowLocationCost(
																(FormatableBitSet) null,
																0);

                // The estimated row count is always 1 here, although the
                // index scan may actually return 0 rows, depending on whether
                // or not the predicates match a key. It is assumed that a
                // match is more likely than a miss, hence the row count is 1.

                // Note (DERBY-6011): Alternative (non-unique) indexes may come
                // up with row counts lower than 1 because they multiply with
                // the selectivity, especially if the table is almost empty.
                // This makes the optimizer prefer non-unique indexes if there
                // are not so many rows in the table. We still want to use the
                // unique index in that case, as the performance difference
                // between the different scans on a small table is small, and
                // the unique index is likely to lock fewer rows and reduce
                // the chance of deadlocks. Therefore, we compensate by
                // making the row count at least 1 for the non-unique index.
                // See reference to DERBY-6011 further down in this method.

                cost = singleFetchCost * costEst.rowCount();

                costEst.setEstimatedCost(
                                costEst.getEstimatedCost() + cost);

                if ( optimizerTracingIsOn() ) { getOptimizerTracer().traceNonCoveringIndexCost( cost, tableNumber ); }
			}
		}
		else
		{
			/* Conglomerate might match more than one row */

			/*
			** Some predicates are good for start/stop, but we don't know
			** the values they are being compared to at this time, so we
			** estimate their selectivity in language rather than ask the
			** store about them .  The predicates on the first column of
			** the conglomerate reduce the number of pages and rows scanned.
			** The predicates on columns after the first reduce the number
			** of rows scanned, but have a much smaller effect on the number
			** of pages scanned, so we keep track of these selectivities in
			** two separate variables: extraFirstColumnSelectivity and
			** extraStartStopSelectivity. (Theoretically, we could try to
			** figure out the effect of predicates after the first column
			** on the number of pages scanned, but it's too hard, so we
			** use these predicates only to reduce the estimated number of
			** rows.  For comparisons with known values, though, the store
			** can figure out exactly how many rows and pages are scanned.)
			**
			** Other predicates are not good for start/stop.  We keep track
			** of their selectvities separately, because these limit the
			** number of rows, but not the number of pages, and so need to
			** be factored into the row count but not into the cost.
			** These selectivities are factored into extraQualifierSelectivity.
			**
			** statStartStopSelectivity (using statistics) represents the 
			** selectivity of start/stop predicates that can be used to scan 
			** the index. If no statistics exist for the conglomerate then 
			** the value of this variable remains at 1.0
			** 
			** statCompositeSelectivity (using statistics) represents the 
			** selectivity of all the predicates (including NonBaseTable 
			** predicates). This represents the most educated guess [among 
			** all the wild surmises in this routine] as to the number
			** of rows that will be returned from this joinNode.
			** If no statistics exist on the table or no statistics at all
			** can be found to satisfy the predicates at this join opertor,
			** then statCompositeSelectivity is left initialized at 1.0
			*/
			double extraFirstColumnSelectivity = 1.0d;
			double extraStartStopSelectivity = 1.0d;
			double extraQualifierSelectivity = 1.0d;
			double extraNonQualifierSelectivity = 1.0d;
			double statStartStopSelectivity = 1.0d;
			double statCompositeSelectivity = 1.0d;

			int	   numExtraFirstColumnPreds = 0;
			int	   numExtraStartStopPreds = 0;
			int	   numExtraQualifiers = 0;
			int	   numExtraNonQualifiers = 0;

			/*
			** It is possible for something to be a start or stop predicate
			** without it being possible to use it as a key for cost estimation.
			** For example, with an index on (c1, c2), and the predicate
			** c1 = othertable.c3 and c2 = 1, the comparison on c1 is with
			** an unknown value, so we can't pass it to the store.  This means
			** we can't pass the comparison on c2 to the store, either.
			**
			** The following booleans keep track of whether we have seen
			** gaps in the keys we can pass to the store.
			*/
			boolean startGap = false;
			boolean stopGap = false;
			boolean seenFirstColumn = false;

			/*
			** We need to figure out the number of rows touched to decide
			** whether to use row locking or table locking.  If the start/stop
			** conditions are constant (i.e. no joins), the number of rows
			** touched is the number of rows per scan.  But if the start/stop
			** conditions contain a join, the number of rows touched must
			** take the number of outer rows into account.
			*/
			boolean constantStartStop = true;
			boolean startStopFound = false;

			/* Count the number of start and stop keys */
			int startKeyNum = 0;
			int stopKeyNum = 0;
			OptimizablePredicate pred;
			int predListSize;

			if (predList != null)
				predListSize = baseTableRestrictionList.size();
			else
				predListSize = 0;

			int startStopPredCount = 0;
			ColumnReference firstColumn = null;
			for (int i = 0; i < predListSize; i++)
			{
				pred = baseTableRestrictionList.getOptPredicate(i);
				boolean startKey = pred.isStartKey();
				boolean stopKey = pred.isStopKey();
				if (startKey || stopKey)
				{
					startStopFound = true;

					if ( ! pred.getReferencedMap().hasSingleBitSet())
					{
						constantStartStop = false;
					}

					boolean knownConstant =
						pred.compareWithKnownConstant(this, true);
					if (startKey)
					{
						if (knownConstant && ( ! startGap ) )
						{
							startKeyNum++;
  							if (unknownPredicateList != null)
  								unknownPredicateList.removeOptPredicate(pred);
						}
						else
						{
							startGap = true;
						}
					}

					if (stopKey)
					{
						if (knownConstant && ( ! stopGap ) )
						{
							stopKeyNum++;
  							if (unknownPredicateList != null)
  								unknownPredicateList.removeOptPredicate(pred);
						}
						else
						{
							stopGap = true;
						}
					}

					/* If either we are seeing startGap or stopGap because start/stop key is
					 * comparison with non-constant, we should multiply the selectivity to
					 * extraFirstColumnSelectivity.  Beetle 4787.
					 */
					if (startGap || stopGap)
					{
						// Don't include redundant join predicates in selectivity calculations
						if (baseTableRestrictionList.isRedundantPredicate(i))
							continue;

						if (startKey && stopKey)
							startStopPredCount++;

						if (pred.getIndexPosition() == 0)
						{
							extraFirstColumnSelectivity *=
														pred.selectivity(this);
							if (! seenFirstColumn)
							{
								ValueNode relNode = ((Predicate) pred).getAndNode().getLeftOperand();
								if (relNode instanceof BinaryRelationalOperatorNode)
									firstColumn = ((BinaryRelationalOperatorNode) relNode).getColumnOperand(this);
								seenFirstColumn = true;
							}
						}
						else
						{
							extraStartStopSelectivity *= pred.selectivity(this);
							numExtraStartStopPreds++;
						}
					}
				}
				else
				{
					// Don't include redundant join predicates in selectivity calculations
					if (baseTableRestrictionList.isRedundantPredicate(i))
					{
						continue;
					}

					/* If we have "like" predicate on the first index column, it is more likely
					 * to have a smaller range than "between", so we apply extra selectivity 0.2
					 * here.  beetle 4387, 4787.
					 */
					if (pred instanceof Predicate)
					{
						ValueNode leftOpnd = ((Predicate) pred).getAndNode().getLeftOperand();
						if (firstColumn != null && leftOpnd instanceof LikeEscapeOperatorNode)
						{
							LikeEscapeOperatorNode likeNode = (LikeEscapeOperatorNode) leftOpnd;
							if (likeNode.getLeftOperand().requiresTypeFromContext())
							{
								ValueNode receiver = ((TernaryOperatorNode) likeNode).getReceiver();
								if (receiver instanceof ColumnReference)
								{
									ColumnReference cr = (ColumnReference) receiver;
									if (cr.getTableNumber() == firstColumn.getTableNumber() &&
										cr.getColumnNumber() == firstColumn.getColumnNumber())
										extraFirstColumnSelectivity *= 0.2;
								}
							}
						}
					}

					if (pred.isQualifier())
					{
						extraQualifierSelectivity *= pred.selectivity(this);
						numExtraQualifiers++;
					}
					else
					{
						extraNonQualifierSelectivity *= pred.selectivity(this);
						numExtraNonQualifiers++;
					}

					/*
					** Strictly speaking, it shouldn't be necessary to
					** indicate a gap here, since there should be no more
					** start/stop predicates, but let's do it, anyway.
					*/
					startGap = true;
					stopGap = true;
				}
			}

			if (unknownPredicateList != null)
			{
				statCompositeSelectivity = unknownPredicateList.selectivity(this);
				if (statCompositeSelectivity == -1.0d)
					statCompositeSelectivity = 1.0d;
			}
			
            if (seenFirstColumn && (startStopPredCount > 0))
            {
                if (statisticsForConglomerate) {
                    statStartStopSelectivity =
                        tableDescriptor.selectivityForConglomerate(cd, 
                            startStopPredCount);				
                } else if (cd.isIndex())  {
                    //DERBY-3790 (Investigate if request for update 
                    // statistics can be skipped for certain kind of 
                    // indexes, one instance may be unique indexes based 
                    // on one column.) But as found in DERBY-6045 (in list
                    // multi-probe by primary key not chosen on tables with
                    // >256 rows), even though we do not keep the 
                    // statistics for single-column unique indexes, we 
                    // should improve the selectivity of such an index
                    // when the index is being considered by the optimizer.
                    IndexRowGenerator irg = cd.getIndexDescriptor();
                    if (irg.isUnique() 
                            && irg.numberOfOrderedColumns() == 1
                            && startStopPredCount == 1) {
                        statStartStopSelectivity = (1/(double)baseRowCount());
                    }
                }
            }

			/*
			** Factor the non-base-table predicates into the extra
			** non-qualifier selectivity, since these will restrict the
			** number of rows, but not the cost.
			*/
			extraNonQualifierSelectivity *=
				currentJoinStrategy.nonBasePredicateSelectivity(this, predList);

			/* Create the start and stop key arrays, and fill them in */
			DataValueDescriptor[] startKeys;
			DataValueDescriptor[] stopKeys;

			if (startKeyNum > 0)
				startKeys = new DataValueDescriptor[startKeyNum];
			else
				startKeys = null;

			if (stopKeyNum > 0)
				stopKeys = new DataValueDescriptor[stopKeyNum];
			else
				stopKeys = null;

			startKeyNum = 0;
			stopKeyNum = 0;
			startGap = false;
			stopGap = false;

			/* If we have a probe predicate that is being used as a start/stop
			 * key then ssKeySourceInList will hold the InListOperatorNode
			 * from which the probe predicate was built.
			 */
			InListOperatorNode ssKeySourceInList = null;
			for (int i = 0; i < predListSize; i++)
			{
				pred = baseTableRestrictionList.getOptPredicate(i);
				boolean startKey = pred.isStartKey();
				boolean stopKey = pred.isStopKey();

				if (startKey || stopKey)
				{
					/* A probe predicate is only useful if it can be used as
					 * as a start/stop key for _first_ column in an index
					 * (i.e. if the column position is 0).  That said, we only
					 * allow a single start/stop key per column position in
					 * the index (see PredicateList.orderUsefulPredicates()).
					 * Those two facts combined mean that we should never have
					 * more than one probe predicate start/stop key for a given
					 * conglomerate.
					 */
					if (SanityManager.DEBUG)
					{
						if ((ssKeySourceInList != null) &&
							((Predicate)pred).isInListProbePredicate())
						{
							SanityManager.THROWASSERT(
							"Found multiple probe predicate start/stop keys" +
							" for conglomerate '" + cd.getConglomerateName() +
							"' when at most one was expected.");
						}
					}

					/* By passing "true" in the next line we indicate that we
					 * should only retrieve the underlying InListOpNode *if*
					 * the predicate is a "probe predicate".
					 */
					ssKeySourceInList = ((Predicate)pred).getSourceInList(true);
					boolean knownConstant = pred.compareWithKnownConstant(this, true);

					if (startKey)
					{
						if (knownConstant && ( ! startGap ) )
						{
							startKeys[startKeyNum] = pred.getCompareValue(this);
							startKeyNum++;
						}
						else
						{
							startGap = true;
						}
					}

					if (stopKey)
					{
						if (knownConstant && ( ! stopGap ) )
						{
							stopKeys[stopKeyNum] = pred.getCompareValue(this);
							stopKeyNum++;
						}
						else
						{
							stopGap = true;
						}
					}
				}
				else
				{
					startGap = true;
					stopGap = true;
				}
			}

			int startOperator;
			int stopOperator;

			if (baseTableRestrictionList != null)
			{
				startOperator = baseTableRestrictionList.startOperator(this);
				stopOperator = baseTableRestrictionList.stopOperator(this);
			}
			else
			{
				/*
				** If we're doing a full scan, it doesn't matter what the
				** start and stop operators are.
				*/
				startOperator = ScanController.NA;
				stopOperator = ScanController.NA;
			}

			/*
			** Get a row template for this conglomerate.  For now, just tell
			** it we are using all the columns in the row.
			*/
			DataValueDescriptor[] rowTemplate = 
                getRowTemplate(cd, getBaseCostController());

			/* we prefer index than table scan for concurrency reason, by a small
			 * adjustment on estimated row count.  This affects optimizer's decision
			 * especially when few rows are in table. beetle 5006. This makes sense
			 * since the plan may stay long before we actually check and invalidate it.
			 * And new rows may be inserted before we check and invalidate the plan.
			 * Here we only prefer index that has start/stop key from predicates. Non-
			 * constant start/stop key case is taken care of by selectivity later.
			 */
			long baseRC = (startKeys != null || stopKeys != null) ? baseRowCount() : baseRowCount() + 5;

			scc.getScanCost(
					currentJoinStrategy.scanCostType(),
					baseRC,
                    1,
					forUpdate(),
					(FormatableBitSet) null,
					rowTemplate,
					startKeys,
					startOperator,
					stopKeys,
					stopOperator,
					false,
					0,
                    costEst);

			/* initialPositionCost is the first part of the index scan cost we get above.
			 * It's the cost of initial positioning/fetch of key.  So it's unrelated to
			 * row count of how many rows we fetch from index.  We extract it here so that
			 * we only multiply selectivity to the other part of index scan cost, which is
			 * nearly linear, to make cost calculation more accurate and fair, especially
			 * compared to the plan of "one row result set" (unique index). beetle 4787.
			 */
			double initialPositionCost = 0.0;
			if (cd.isIndex())
			{
				initialPositionCost = scc.getFetchFromFullKeyCost((FormatableBitSet) null, 0);
				/* oneRowResultSetForSomeConglom means there's a unique index, but certainly
				 * not this one since we are here.  If store knows this non-unique index
				 * won't return any row or just returns one row (eg., the predicate is a
				 * comparison with constant or almost empty table), we do minor adjustment
				 * on cost (affecting decision for covering index) and rc (decision for
				 * non-covering). The purpose is favoring unique index. beetle 5006.
				 */
                if (oneRowResultSetForSomeConglom && costEst.rowCount() <= 1)
				{
                    costEst.setCost(costEst.getEstimatedCost() * 2,
                                         costEst.rowCount() + 2,
                                         costEst.singleScanRowCount() + 2);
				}
			}

            if ( optimizerTracingIsOn() )
            {
                getOptimizerTracer().traceCostOfConglomerateScan
                    (
                     tableNumber,
                     cd,
                     costEst,
                     numExtraFirstColumnPreds,
                     extraFirstColumnSelectivity,
                     numExtraStartStopPreds,
                     extraStartStopSelectivity,
                     startStopPredCount,
                     statStartStopSelectivity,
                     numExtraQualifiers,
                     extraQualifierSelectivity,
                     numExtraNonQualifiers,
                     extraNonQualifierSelectivity
                     );
            }

			/* initial row count is the row count without applying
			   any predicates-- we use this at the end of the routine
			   when we use statistics to recompute the row count.
			*/
            double initialRowCount = costEst.rowCount();

			if (statStartStopSelectivity != 1.0d)
			{
				/*
				** If statistics exist use the selectivity computed 
				** from the statistics to calculate the cost. 
				** NOTE: we apply this selectivity to the cost as well
				** as both the row counts. In the absence of statistics
				** we only applied the FirstColumnSelectivity to the 
				** cost.
				*/
                costEst.setCost(
                    scanCostAfterSelectivity(costEst.getEstimatedCost(),
                                             initialPositionCost,
                                             statStartStopSelectivity,
                                             oneRowResultSetForSomeConglom),
                    costEst.rowCount() * statStartStopSelectivity,
                    costEst.singleScanRowCount() *
                    statStartStopSelectivity);
                
                if (optimizerTracingIsOn()) {
                    getOptimizerTracer().
                        traceCostIncludingStatsForIndex(costEst, tableNumber);
                }
			}
			else
			{
				/*
				** Factor in the extra selectivity on the first column
				** of the conglomerate (see comment above).
				** NOTE: In this case we want to apply the selectivity to both
				** the total row count and singleScanRowCount.
				*/
				if (extraFirstColumnSelectivity != 1.0d)
				{
                    costEst.setCost(
                         scanCostAfterSelectivity(costEst.getEstimatedCost(),
												  initialPositionCost,
												  extraFirstColumnSelectivity,
												  oneRowResultSetForSomeConglom),
                         costEst.rowCount() * extraFirstColumnSelectivity,
                         costEst.singleScanRowCount() *
                             extraFirstColumnSelectivity);

                    if (optimizerTracingIsOn()) {
                        getOptimizerTracer().
                            traceCostIncludingExtra1stColumnSelectivity(
                                costEst, tableNumber);
                    }
				}

				/* Factor in the extra start/stop selectivity (see comment above).
				 * NOTE: In this case we want to apply the selectivity to both
				 * the row count and singleScanRowCount.
				 */
				if (extraStartStopSelectivity != 1.0d)
				{
                    costEst.setCost(
                        costEst.getEstimatedCost(),
                        costEst.rowCount() * extraStartStopSelectivity,
                        costEst.singleScanRowCount() *
                            extraStartStopSelectivity);

                    if (optimizerTracingIsOn()) {
                        getOptimizerTracer().traceCostIncludingExtraStartStop(
                            costEst, tableNumber);
                    }
				}
			}

			/* If the start and stop key came from an IN-list "probe predicate"
			 * then we need to adjust the cost estimate.  The probe predicate
			 * is of the form "col = ?" and we currently have the estimated
			 * cost of probing the index a single time for "?".  But with an
			 * IN-list we don't just probe the index once; we're going to
			 * probe it once for every value in the IN-list.  And we are going
			 * to potentially return an additional row (or set of rows) for
			 * each probe.  To account for this "multi-probing" we take the
			 * costEstimate and multiply each of its fields by the size of
			 * the IN-list.
			 *
			 * Note: If the IN-list has duplicate values then this simple
			 * multiplication could give us an elevated cost (because we
			 * only probe the index for each *non-duplicate* value in the
			 * IN-list).  But for now, we're saying that's okay.
			 */
			if (ssKeySourceInList != null)
			{
				int listSize = ssKeySourceInList.getRightOperandList().size();
                double rc = costEst.rowCount() * listSize;
                double ssrc = costEst.singleScanRowCount() * listSize;

				/* If multiplication by listSize returns more rows than are
				 * in the scan then just use the number of rows in the scan.
				 */
                costEst.setCost(
                    costEst.getEstimatedCost() * listSize,
					rc > initialRowCount ? initialRowCount : rc,
					ssrc > initialRowCount ? initialRowCount : ssrc);
			}

			/*
			** Figure out whether to do row locking or table locking.
			**
			** If there are no start/stop predicates, we're doing full
			** conglomerate scans, so do table locking.
			*/
			if (! startStopFound)
			{
                currAccessPath.setLockMode(
											TransactionController.MODE_TABLE);

                if ( optimizerTracingIsOn() ) { getOptimizerTracer().traceNoStartStopPosition(); }
			}
			else
			{
				/*
				** Figure out the number of rows touched.  If all the
				** start/stop predicates are constant, the number of
				** rows touched is the number of rows per scan.
				** This is also true for join strategies that scan the
				** inner table only once (like hash join) - we can
				** tell if we have one of those, because
				** multiplyBaseCostByOuterRows() will return false.
				*/
                double rowsTouched = costEst.rowCount();

				if ( (! constantStartStop) &&
					 currentJoinStrategy.multiplyBaseCostByOuterRows())
				{
					/*
					** This is a join where the inner table is scanned
					** more than once, so we have to take the number
					** of outer rows into account.  The formula for this
					** works out as follows:
					**
					**	total rows in table = r
					**  number of rows touched per scan = s
					**  number of outer rows = o
					**  proportion of rows touched per scan = s / r
					**  proportion of rows not touched per scan =
					**										1 - (s / r)
					**  proportion of rows not touched for all scans =
					**									(1 - (s / r)) ** o
					**  proportion of rows touched for all scans =
					**									1 - ((1 - (s / r)) ** o)
					**  total rows touched for all scans =
					**							r * (1 - ((1 - (s / r)) ** o))
					**
					** In doing these calculations, we must be careful not
					** to divide by zero.  This could happen if there are
					** no rows in the table.  In this case, let's do table
					** locking.
					*/
					double r = baseRowCount();
					if (r > 0.0)
					{
                        double s = costEst.rowCount();
						double o = outerCost.rowCount();
						double pRowsNotTouchedPerScan = 1.0 - (s / r);
						double pRowsNotTouchedAllScans =
										Math.pow(pRowsNotTouchedPerScan, o);
						double pRowsTouchedAllScans =
										1.0 - pRowsNotTouchedAllScans;
						double rowsTouchedAllScans =
										r * pRowsTouchedAllScans;

						rowsTouched = rowsTouchedAllScans;
					}
					else
					{
						/* See comments in setLockingBasedOnThreshold */
						rowsTouched = optimizer.tableLockThreshold() + 1;
					}
				}

				setLockingBasedOnThreshold(optimizer, rowsTouched);
			}

			/*
			** If the index isn't covering, add the cost of getting the
			** base row.  Only apply extraFirstColumnSelectivity and extraStartStopSelectivity
			** before we do this, don't apply extraQualifierSelectivity etc.  The
			** reason is that the row count here should be the number of index rows
			** (and hence heap rows) we get, and we need to fetch all those rows, even
			** though later on some of them may be filtered out by other predicates.
			** beetle 4787.
			*/
			if (cd.isIndex() && ( ! isCoveringIndex(cd) ) )
			{
				double singleFetchCost =
						getBaseCostController().getFetchFromRowLocationCost(
																(FormatableBitSet) null,
																0);

                // The number of rows we expect to fetch from the base table.
                double rowsToFetch = costEst.rowCount();

                if (oneRowResultSetForSomeConglom) {
                    // DERBY-6011: We know that there is a unique index, and
                    // that there are predicates that guarantee that at most
                    // one row will be fetched from the unique index. The
                    // unique alternative always has 1 as estimated row count
                    // (see reference to DERBY-6011 further up in this method),
                    // even though it could actually return 0 rows.
                    //
                    // If the alternative that's being considered here has
                    // expected row count less than 1, it is going to have
                    // lower estimated cost for fetching base rows. We prefer
                    // unique indexes, as they lock fewer rows and allow more
                    // concurrency. Therefore, make sure the cost estimate for
                    // this alternative includes at least fetching one row from
                    // the base table.
                    rowsToFetch = Math.max(1.0d, rowsToFetch);
                }

                cost = singleFetchCost * rowsToFetch;

                costEst.setEstimatedCost(
                                costEst.getEstimatedCost() + cost);

                if (optimizerTracingIsOn()) {
                    getOptimizerTracer().traceCostOfNoncoveringIndex(
                        costEst, tableNumber);
                }
			}

			/* Factor in the extra qualifier selectivity (see comment above).
			 * NOTE: In this case we want to apply the selectivity to both
			 * the row count and singleScanRowCount.
			 */
			if (extraQualifierSelectivity != 1.0d)
			{
                costEst.setCost(
                        costEst.getEstimatedCost(),
                        costEst.rowCount() * extraQualifierSelectivity,
                        costEst.singleScanRowCount() *
                            extraQualifierSelectivity);

                if (optimizerTracingIsOn()) {
                    getOptimizerTracer().
                        traceCostIncludingExtraQualifierSelectivity(
                            costEst, tableNumber);
                }
			}

            singleScanRowCount = costEst.singleScanRowCount();

			/*
			** Let the join strategy decide whether the cost of the base
			** scan is a single scan, or a scan per outer row.
			** NOTE: In this case we only want to multiply against the
			** total row count, not the singleScanRowCount.
			** NOTE: Do not multiply row count if we determined that
			** conglomerate is a 1 row result set when costing nested
			** loop.  (eg, we will find at most 1 match when probing
			** the hash table.)
			*/
            double newCost = costEst.getEstimatedCost();
            double rowCnt = costEst.rowCount();

			/*
			** RESOLVE - If there is a unique index on the joining
			** columns, the number of matching rows will equal the
			** number of outer rows, even if we're not considering the
			** unique index for this access path. To figure that out,
			** however, would require an analysis phase at the beginning
			** of optimization. So, we'll always multiply the number
			** of outer rows by the number of rows per scan. This will
			** give us a higher than actual row count when there is
			** such a unique index, which will bias the optimizer toward
			** using the unique index. This is probably OK most of the
			** time, since the optimizer would probably choose the
			** unique index, anyway. But it would be better if the
			** optimizer set the row count properly in this case.
			*/
			if (currentJoinStrategy.multiplyBaseCostByOuterRows())
			{
				newCost *= outerCost.rowCount();
			}

            rowCnt *= outerCost.rowCount();
			initialRowCount *= outerCost.rowCount();


			/*
			** If this table can generate at most one row per scan,
			** the maximum row count is the number of outer rows.
			** NOTE: This does not completely take care of the RESOLVE
			** in the above comment, since it will only notice
			** one-row result sets for the current join order.
			*/
			if (oneRowResultSetForSomeConglom)
			{
                if (outerCost.rowCount() < rowCnt)
				{
                    rowCnt = outerCost.rowCount();
				}
			}

			/*
			** The estimated cost may be too high for indexes, if the
			** estimated row count exceeds the maximum. Only do this
			** if we're not doing a full scan, and the start/stop position
			** is not constant (i.e. we're doing a join on the first column
			** of the index) - the reason being that this is when the
			** cost may be inaccurate.
			*/
			if (cd.isIndex() && startStopFound && ( ! constantStartStop ) )
			{
				/*
				** Does any table outer to this one have a unique key on
				** a subset of the joining columns? If so, the maximum number
				** of rows that this table can return is the number of rows
				** in this table times the number of times the maximum number
				** of times each key can be repeated.
				*/
				double scanUniquenessFactor = 
				  optimizer.uniqueJoinWithOuterTable(baseTableRestrictionList);
				if (scanUniquenessFactor > 0.0)
				{
					/*
					** A positive uniqueness factor means there is a unique
					** outer join key. The value is the reciprocal of the
					** maximum number of duplicates for each unique key
					** (the duplicates can be caused by other joining tables).
					*/
					double maxRows =
							((double) baseRowCount()) / scanUniquenessFactor;
                    if (rowCnt > maxRows)
					{
						/*
						** The estimated row count is too high. Adjust the
						** estimated cost downwards proportionately to
						** match the maximum number of rows.
						*/
                        newCost *= (maxRows / rowCnt);
					}
				}
			}

			/* The estimated total row count may be too high */
			if (tableUniquenessFactor > 0.0)
			{
				/*
				** A positive uniqueness factor means there is a unique outer
				** join key. The value is the reciprocal of the maximum number
				** of duplicates for each unique key (the duplicates can be
				** caused by other joining tables).
				*/
				double maxRows =
							((double) baseRowCount()) / tableUniquenessFactor;
                if (rowCnt > maxRows)
				{
					/*
					** The estimated row count is too high. Set it to the
					** maximum row count.
					*/
                    rowCnt = maxRows;
				}
			}

            costEst.setCost(
				newCost,
                rowCnt,
                costEst.singleScanRowCount());


            if (optimizerTracingIsOn()) {
                getOptimizerTracer().traceCostOfNScans(
                    tableNumber, outerCost.rowCount(), costEst);
            }

			/*
			** Now figure in the cost of the non-qualifier predicates.
			** existsBaseTables have a row count of 1
			*/
			double rc = -1, src = -1;
			if (existsBaseTable)
				rc = src = 1;
			// don't factor in extraNonQualifierSelectivity in case of oneRowResultSetForSomeConglom
			// because "1" is the final result and the effect of other predicates already considered
			// beetle 4787
			else if (extraNonQualifierSelectivity != 1.0d)
			{
                rc = oneRowResultSetForSomeConglom ?
                    costEst.rowCount() :
                    costEst.rowCount() * extraNonQualifierSelectivity;

                src = costEst.singleScanRowCount() *
                      extraNonQualifierSelectivity;
			}
			if (rc != -1) // changed
			{
                costEst.setCost(costEst.getEstimatedCost(), rc, src);

                if (optimizerTracingIsOn()) {
                    getOptimizerTracer().
                        traceCostIncludingExtraNonQualifierSelectivity(
                            costEst, tableNumber);
                }
			}
			
		recomputeRowCount:
			if (statisticsForTable && !oneRowResultSetForSomeConglom &&
				(statCompositeSelectivity != 1.0d))
			{
				/* if we have statistics we should use statistics to calculate 
				   row  count-- if it has been determined that this table 
				   returns one row for some conglomerate then there is no need 
				   to do this recalculation
				*/

				double compositeStatRC = initialRowCount * statCompositeSelectivity;
                if ( optimizerTracingIsOn() )
                { getOptimizerTracer().traceCompositeSelectivityFromStatistics( statCompositeSelectivity ); }

				if (tableUniquenessFactor > 0.0)
				{
					/* If the row count from the composite statistics
					   comes up more than what the table uniqueness 
					   factor indicates then lets stick with the current
					   row count.
					*/
					if (compositeStatRC > (baseRowCount() *
										   tableUniquenessFactor))
						
					{
						
						break recomputeRowCount;
					}
				}
				
				/* set the row count and the single scan row count
				   to the initialRowCount. initialRowCount is the product
				   of the RC from store * RC of the outerCost.
				   Thus RC = initialRowCount * the selectivity from stats.
				   SingleRC = RC / outerCost.rowCount().
				*/
                costEst.setCost(costEst.getEstimatedCost(),
									 compositeStatRC,
									 (existsBaseTable) ? 
									 1 : 
									 compositeStatRC / outerCost.rowCount());

                if (optimizerTracingIsOn()) {
                    getOptimizerTracer().
                        traceCostIncludingCompositeSelectivityFromStats(
                            costEst, tableNumber);
                }
			}
		}

		/* Put the base predicates back in the predicate list */
		currentJoinStrategy.putBasePredicates(predList,
									   baseTableRestrictionList);
        return costEst;
	}

	private double scanCostAfterSelectivity(double originalScanCost,
											double initialPositionCost,
											double selectivity,
											boolean anotherIndexUnique)
			throws StandardException
	{
		/* If there's another paln using unique index, its selectivity is 1/r
		 * because we use row count 1.  This plan is not unique index, so we make
		 * selectivity at least 2/r, which is more fair, because for unique index
		 * we don't use our selectivity estimates.  Unique index also more likely
		 * locks less rows, hence better concurrency.  beetle 4787.
		 */
		if (anotherIndexUnique)
		{
			double r = baseRowCount();
			if (r > 0.0)
			{
				double minSelectivity = 2.0 / r;
				if (minSelectivity > selectivity)
					selectivity = minSelectivity;
			}
		}
		
		/* initialPositionCost is the first part of the index scan cost we get above.
		 * It's the cost of initial positioning/fetch of key.  So it's unrelated to
		 * row count of how many rows we fetch from index.  We extract it here so that
		 * we only multiply selectivity to the other part of index scan cost, which is
		 * nearly linear, to make cost calculation more accurate and fair, especially
		 * compared to the plan of "one row result set" (unique index). beetle 4787.
		 */
		double afterInitialCost = (originalScanCost - initialPositionCost) *
				 				selectivity;
		if (afterInitialCost < 0)
			afterInitialCost = 0;
		return initialPositionCost + afterInitialCost;
	}

	private void setLockingBasedOnThreshold(
					Optimizer optimizer, double rowsTouched)
	{
		/* In optimizer we always set it to row lock (unless there's no
		 * start/stop key found to utilize an index, in which case we do table
		 * lock), it's up to store to upgrade it to table lock.  This makes
		 * sense for the default read committed isolation level and update
		 * lock.  For more detail, see Beetle 4133.
		 */
		getCurrentAccessPath().setLockMode(
									TransactionController.MODE_RECORD);
	}

	/** @see org.apache.derby.iapi.sql.compile.Optimizable#isBaseTable */
    @Override
	public boolean isBaseTable()
	{
		return true;
	}

	/** @see org.apache.derby.iapi.sql.compile.Optimizable#forUpdate */
    @Override
	public boolean forUpdate()
	{
		/* This table is updatable if it is the
		 * target table of an update or delete,
		 * or it is (or was) the target table of an
		 * updatable cursor.
		 */
		return (updateOrDelete != 0) || isCursorTargetTable() || getUpdateLocks;
	}

	/** @see org.apache.derby.iapi.sql.compile.Optimizable#initialCapacity */
    @Override
	public int initialCapacity()
	{
		return initialCapacity;
	}

	/** @see org.apache.derby.iapi.sql.compile.Optimizable#loadFactor */
    @Override
	public float loadFactor()
	{
		return loadFactor;
	}

	/**
	 * @see org.apache.derby.iapi.sql.compile.Optimizable#memoryUsageOK
	 */
    @Override
	public boolean memoryUsageOK(double rowCount, int maxMemoryPerTable)
			throws StandardException
	{
		return super.memoryUsageOK(singleScanRowCount, maxMemoryPerTable);
	}

	/**
	 * @see org.apache.derby.iapi.sql.compile.Optimizable#isTargetTable
	 */
    @Override
	public boolean isTargetTable()
	{
		return (updateOrDelete != 0);
	}

	/**
	 * @see org.apache.derby.iapi.sql.compile.Optimizable#uniqueJoin
	 */
    @Override
	public double uniqueJoin(OptimizablePredicateList predList)
					throws StandardException
	{
		double retval = -1.0;
		PredicateList pl = (PredicateList) predList;
		int numColumns = getTableDescriptor().getNumberOfColumns();
        int tableNo = getTableNumber();

		// This is supposed to be an array of table numbers for the current
		// query block. It is used to determine whether a join is with a
		// correlation column, to fill in eqOuterCols properly. We don't care
		// about eqOuterCols, so just create a zero-length array, pretending
		// that all columns are correlation columns.
		int[] tableNumbers = new int[0];
		JBitSet[] tableColMap = new JBitSet[1];
		tableColMap[0] = new JBitSet(numColumns + 1);

        pl.checkTopPredicatesForEqualsConditions(tableNo,
												null,
												tableNumbers,
												tableColMap,
												false);

		if (supersetOfUniqueIndex(tableColMap))
		{
			retval =
				getBestAccessPath().getCostEstimate().singleScanRowCount();
		}

		return retval;
	}

	/**
	 * @see org.apache.derby.iapi.sql.compile.Optimizable#isOneRowScan
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public boolean isOneRowScan() 
		throws StandardException
	{
		/* EXISTS FBT will never be a 1 row scan.
		 * Otherwise call method in super class.
		 */
		if (existsBaseTable)
		{
			return false;
		}
		

		return super.isOneRowScan();
	}

	/**
	 * @see org.apache.derby.iapi.sql.compile.Optimizable#legalJoinOrder
	 */
    @Override
	public boolean legalJoinOrder(JBitSet assignedTableMap)
	{
		// Only an issue for EXISTS FBTs
		if (existsBaseTable)
		{
			/* Have all of our dependencies been satisfied? */
			return assignedTableMap.contains(dependencyMap);
		}
		return true;
	}

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */

    @Override
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return "tableName: " +
				(tableName != null ? tableName.toString() : "null") + "\n" +
				"tableDescriptor: " + tableDescriptor + "\n" +
				"updateOrDelete: " + updateOrDelete + "\n" +
				(tableProperties != null ?
					tableProperties.toString() : "null") + "\n" +
				"existsBaseTable: " + existsBaseTable + "\n" +
				"dependencyMap: " +
				(dependencyMap != null 
						? dependencyMap.toString() 
						: "null") + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

	/**
	 * Does this FBT represent an EXISTS FBT.
	 *
	 * @return Whether or not this FBT represents
	 *			an EXISTS FBT.
	 */
	boolean getExistsBaseTable()
	{
		return existsBaseTable;
	}

	/**
	 * Set whether or not this FBT represents an
	 * EXISTS FBT.
	 *
 	 * @param existsBaseTable Whether or not an EXISTS FBT.
	 * @param dependencyMap	  The dependency map for the EXISTS FBT.
 	 * @param isNotExists     Whether or not for NOT EXISTS, more specifically.
	 */
	void setExistsBaseTable(boolean existsBaseTable, JBitSet dependencyMap, boolean isNotExists)
	{
		this.existsBaseTable = existsBaseTable;
		this.isNotExists = isNotExists;

		/* Set/clear the dependency map as needed */
		if (existsBaseTable)
		{
			this.dependencyMap = dependencyMap;
		}
		else
		{
			this.dependencyMap = null;
		}
	}

	/**
	 * Clear the bits from the dependency map when join nodes are flattened
	 *
	 * @param locations	list of bit numbers to be cleared
	 */
    void clearDependency(List<Integer> locations)
	{
		if (this.dependencyMap != null)
		{
			for (int i = 0; i < locations.size() ; i++)
                this.dependencyMap.clear(locations.get(i).intValue());
		}
	}

	/**
	 * Set the table properties for this table.
	 *
	 * @param tableProperties	The new table properties.
	 */
    void setTableProperties(Properties tableProperties)
	{
		this.tableProperties = tableProperties;
	}

	/**
	 * Bind the table in this FromBaseTable.
	 * This is where view resolution occurs
	 *
	 * @param dataDictionary	The DataDictionary to use for binding
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @return	ResultSetNode	The FromTable for the table or resolved view.
	 *
	 * @exception StandardException		Thrown on error
	 */

    @Override
    ResultSetNode bindNonVTITables(DataDictionary dataDictionary,
						   FromList fromListParam) 
					throws StandardException
	{
        tableName.bind();

        TableDescriptor tabDescr = bindTableDescriptor();

        if (tabDescr.getTableType() == TableDescriptor.VTI_TYPE) {
			ResultSetNode vtiNode = mapTableAsVTI(
                    tabDescr,
					getCorrelationName(),
					getResultColumns(),
					getProperties(),
					getContextManager());
			return vtiNode.bindNonVTITables(dataDictionary, fromListParam);
		}	
		
		ResultColumnList	derivedRCL = getResultColumns();
  
		// make sure there's a restriction list
        restrictionList = new PredicateList(getContextManager());
        baseTableRestrictionList = new PredicateList(getContextManager());


		CompilerContext compilerContext = getCompilerContext();

		/* Generate the ResultColumnList */
		setResultColumns( genResultColList() );
		templateColumns = getResultColumns();

		/* Resolve the view, if this is a view */
        if (tabDescr.getTableType() == TableDescriptor.VIEW_TYPE)
		{
			FromSubquery                fsq;
			ResultSetNode				rsn;
			ViewDescriptor				vd;
			CreateViewNode				cvn;
			SchemaDescriptor			compSchema;

			/* Get the associated ViewDescriptor so that we can get 
			 * the view definition text.
			 */
            vd = dataDictionary.getViewDescriptor(tabDescr);

			/*
			** Set the default compilation schema to be whatever
			** this schema this view was originally compiled against.
			** That way we pick up the same tables no matter what
			** schema we are running against.
			*/
			compSchema = dataDictionary.getSchemaDescriptor(vd.getCompSchemaId(), null);

			compilerContext.pushCompilationSchema(compSchema);
	
			try
			{
		
				/* This represents a view - query is dependent on the ViewDescriptor */
				compilerContext.createDependency(vd);
	
				cvn = (CreateViewNode)
				          parseStatement(vd.getViewText(), false);

				rsn = cvn.getParsedQueryExpression();

				/* If the view contains a '*' then we mark the views derived column list
				 * so that the view will still work, and return the expected results,
				 * if any of the tables referenced in the view have columns added to
				 * them via ALTER TABLE.  The expected results means that the view
				 * will always return the same # of columns.
				 */
				if (rsn.getResultColumns().containsAllResultColumn())
				{
					getResultColumns().setCountMismatchAllowed(true);
				}
				//Views execute with definer's privileges and if any one of 
				//those privileges' are revoked from the definer, the view gets
				//dropped. So, a view can exist in Derby only if it's owner has
				//all the privileges needed to create one. In order to do a 
				//select from a view, a user only needs select privilege on the
				//view and doesn't need any privilege for objects accessed by
				//the view. Hence, when collecting privilege requirement for a
				//sql accessing a view, we only need to look for select privilege
				//on the actual view and that is what the following code is
				//checking.
                for (ResultColumn rc : getResultColumns()) {
                    if (isPrivilegeCollectionRequired()) {
						compilerContext.addRequiredColumnPriv( rc.getTableColumnDescriptor());
                    }
				}

                fsq = new FromSubquery(
					rsn,
                    cvn.getOrderByList(),
                    cvn.getOffset(),
                    cvn.getFetchFirst(),
                    cvn.hasJDBClimitClause(),
					(correlationName != null) ? 
                        correlationName : getOrigTableName().getTableName(), 
					getResultColumns(),
					tableProperties,
					getContextManager());
				// Transfer the nesting level to the new FromSubquery
				fsq.setLevel(level);
				//We are getting ready to bind the query underneath the view. Since
				//that query is going to run with definer's privileges, we do not
				//need to collect any privilege requirement for that query. 
				//Following call is marking the query to run with definer 
				//privileges. This marking will make sure that we do not collect
				//any privilege requirement for it.
                CollectNodesVisitor<QueryTreeNode> cnv =
                    new CollectNodesVisitor<QueryTreeNode>(QueryTreeNode.class);

                fsq.accept(cnv);

                for (QueryTreeNode node : cnv.getList()) {
                    node.disablePrivilegeCollection();
                }

				fsq.setOrigTableName(this.getOrigTableName());

				// since we reset the compilation schema when we return, we
				// need to save it for use when we bind expressions:
				fsq.setOrigCompilationSchema(compSchema);
				ResultSetNode fsqBound =
					fsq.bindNonVTITables(dataDictionary, fromListParam);

				/* Do error checking on derived column list and update "exposed"
				 * column names if valid.
				 */
				if (derivedRCL != null) {
					fsqBound.getResultColumns().propagateDCLInfo(
						derivedRCL,
						origTableName.getFullTableName());
				}

				return fsqBound;
			}
			finally
			{
				compilerContext.popCompilationSchema();
			}
		}
		else
		{
			/* This represents a table - query is dependent on the TableDescriptor */
            compilerContext.createDependency(tabDescr);

			/* Get the base conglomerate descriptor */
			baseConglomerateDescriptor =
                tabDescr.getConglomerateDescriptor(
                    tabDescr.getHeapConglomerateId()
					);

            // Bail out if the descriptor couldn't be found. The conglomerate
            // probably doesn't exist anymore.
            if (baseConglomerateDescriptor == null) {
                throw StandardException.newException(
                        SQLState.STORE_CONGLOMERATE_DOES_NOT_EXIST,
                        Long.valueOf(tabDescr.getHeapConglomerateId()));
            }

			/* Build the 0-based array of base column names. */
			columnNames = getResultColumns().getColumnNames();

			/* Do error checking on derived column list and update "exposed"
			 * column names if valid.
			 */
			if (derivedRCL != null)
			{
                getResultColumns().propagateDCLInfo(derivedRCL, 
											    origTableName.getFullTableName());
			}

			/* Assign the tableNumber */
			if (tableNumber == -1)  // allow re-bind, in which case use old number
				tableNumber = compilerContext.getNextTableNumber();
		}

        //
        // Only the DBO can select from SYS.SYSUSERS.
        //
        authorizeSYSUSERS =
            dataDictionary.usesSqlAuthorization() &&
            tabDescr.getUUID().toString().equals(
                SYSUSERSRowFactory.SYSUSERS_UUID );

        if ( authorizeSYSUSERS )
        {
            String  databaseOwner = dataDictionary.getAuthorizationDatabaseOwner();
            String  currentUser = getLanguageConnectionContext().getStatementContext().getSQLSessionContext().getCurrentUser();

            if ( !databaseOwner.equals( currentUser ) )
            {
                throw StandardException.newException( SQLState.DBO_ONLY );
            }
        }

		return this;
	}

    /**
     * Return a node that represents invocation of the virtual table for the
     * given table descriptor. The mapping of the table descriptor to a specific
     * VTI class name will occur as part of the "init" phase for the
     * NewInvocationNode that we create here.
     *
     * Currently only handles no argument VTIs corresponding to a subset of the
     * diagnostic tables. (e.g. lock_table). The node returned is a FROM_VTI
     * node with a passed in NEW_INVOCATION_NODE representing the class, with no
     * arguments. Other attributes of the original FROM_TABLE node (such as
     * resultColumns) are passed into the FROM_VTI node.
     */
    private ResultSetNode mapTableAsVTI(
            TableDescriptor td,
            String correlationName,
            ResultColumnList resultColumns,
            Properties tableProperties,
            ContextManager cm)
        throws StandardException {


        // The fact that we pass a non-null table descriptor to the following
        // call is an indication that we are mapping to a no-argument VTI. Since
        // we have the table descriptor we do not need to pass in a TableName.
        // See NewInvocationNode for more.
        final List<ValueNode> emptyList = Collections.emptyList();
        MethodCallNode newNode = new NewInvocationNode(
                null, // TableName
                td, // TableDescriptor
                emptyList,
                false,
                cm);

        QueryTreeNode vtiNode;

        if (correlationName != null) {
            vtiNode = new FromVTI(
                    newNode,
                    correlationName,
                    resultColumns,
                    tableProperties,
                    cm);
        } else {
            TableName exposedName = newNode.makeTableName(td.getSchemaName(),
                    td.getDescriptorName());

            vtiNode = new FromVTI(
                    newNode,
                    null, /* correlationName */
                    resultColumns,
                    tableProperties,
                    exposedName,
                    cm);
        }

        return (ResultSetNode) vtiNode;
    }

	/** 
	 * Determine whether or not the specified name is an exposed name in
	 * the current query block.
	 *
	 * @param name	The specified name to search for as an exposed name.
	 * @param schemaName	Schema name, if non-null.
	 * @param exactMatch	Whether or not we need an exact match on specified schema and table
	 *						names or match on table id.
	 *
	 * @return The FromTable, if any, with the exposed name.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    FromTable getFromTableByName(String name, String schemaName, boolean exactMatch)
		throws StandardException
	{
		// ourSchemaName can be null if correlation name is specified.
		String ourSchemaName = getOrigTableName().getSchemaName();
		String fullName = (schemaName != null) ? (schemaName + '.' + name) : name;

		/* If an exact string match is required then:
		 *	o  If schema name specified on 1 but not both then no match.
		 *  o  If schema name not specified on either, compare exposed names.
		 *  o  If schema name specified on both, compare schema and exposed names.
		 */
		if (exactMatch)
		{

			if ((schemaName != null && ourSchemaName == null) ||
				(schemaName == null && ourSchemaName != null))
			{
				return null;
			}

			if (getExposedName().equals(fullName))
			{
				return this;
			}

			return null;
		}

		/* If an exact string match is not required then:
		 *  o  If schema name specified on both, compare schema and exposed names.
		 *  o  If schema name not specified on either, compare exposed names.
		 *	o  If schema name specified on column but not table, then compare
		 *	   the column's schema name against the schema name from the TableDescriptor.
		 *	   If they agree, then the column's table name must match the exposed name
		 *	   from the table, which must also be the base table name, since a correlation
		 *	   name does not belong to a schema.
		 *  o  If schema name not specified on column then just match the exposed names.
		 */
		// Both or neither schema name specified
		if (getExposedName().equals(fullName))
		{
			return this;
		}
		else if ((schemaName != null && ourSchemaName != null) ||
				 (schemaName == null && ourSchemaName == null))
		{
			return null;
		}

		// Schema name only on column
		// e.g.:  select w1.i from t1 w1 order by test2.w1.i;  (incorrect)
		if (schemaName != null && ourSchemaName == null)
		{
			// Compare column's schema name with table descriptor's if it is
			// not a synonym since a synonym can be declared in a different
			// schema.
			if (tableName.equals(origTableName) && 
					! schemaName.equals(tableDescriptor.getSchemaDescriptor().getSchemaName()))
			{
				return null;
			}

			// Compare exposed name with column's table name
			if (! getExposedName().equals(name))
			{
				return null;
			}

			// Make sure exposed name is not a correlation name
			if (! getExposedName().equals(getOrigTableName().getTableName()))
			{
				return null;
			}

			return this;
		}

		/* Schema name only specified on table. Compare full exposed name
		 * against table's schema name || "." || column's table name.
		 */
		if (! getExposedName().equals(getOrigTableName().getSchemaName() + "." + name))
		{
			return null;
		}

		return this;
	}


	/**
	  *	Bind the table descriptor for this table.
	  *
	  * If the tableName is a synonym, it will be resolved here.
	  * The original table name is retained in origTableName.
	  * 
	  * @exception StandardException		Thrown on error
	  */
	private	TableDescriptor	bindTableDescriptor()
		throws StandardException
	{
		String schemaName = tableName.getSchemaName();
		SchemaDescriptor sd = getSchemaDescriptor(schemaName);

		tableDescriptor = getTableDescriptor(tableName.getTableName(), sd);
		if (tableDescriptor == null)
		{
			// Check if the reference is for a synonym.
			TableName synonymTab = resolveTableToSynonym(tableName);
			if (synonymTab == null)
				throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND, tableName);
			
			tableName = synonymTab;
			sd = getSchemaDescriptor(tableName.getSchemaName());

			tableDescriptor = getTableDescriptor(synonymTab.getTableName(), sd);
			if (tableDescriptor == null)
				throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND, tableName);
		}

		return	tableDescriptor;
	}


	/**
	 * Bind the expressions in this FromBaseTable.  This means binding the
	 * sub-expressions, as well as figuring out what the return type is for
	 * each expression.
	 *
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    void bindExpressions(FromList fromListParam)
					throws StandardException
	{
		/* No expressions to bind for a FromBaseTable.
		 * NOTE - too involved to optimize so that this method
		 * doesn't get called, so just do nothing.
		 */
	}

	/**
	 * Bind the result columns of this ResultSetNode when there is no
	 * base table to bind them to.  This is useful for SELECT statements,
	 * where the result columns get their types from the expressions that
	 * live under them.
	 *
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @exception StandardException		Thrown on error
	 */

    @Override
    void bindResultColumns(FromList fromListParam)
				throws StandardException
	{
		/* Nothing to do, since RCL bound in bindNonVTITables() */
	}

	/**
	 * Try to find a ResultColumn in the table represented by this FromBaseTable
	 * that matches the name in the given ColumnReference.
	 *
	 * @param columnReference	The columnReference whose name we're looking
	 *				for in the given table.
	 *
	 * @return	A ResultColumn whose expression is the ColumnNode
	 *			that matches the ColumnReference.
	 *		Returns null if there is no match.
	 *
	 * @exception StandardException		Thrown on error
	 */

    @Override
    ResultColumn getMatchingColumn(ColumnReference columnReference)
            throws StandardException
	{
		ResultColumn	resultColumn = null;
		TableName		columnsTableName;
		TableName		exposedTableName;

		columnsTableName = columnReference.getQualifiedTableName();

        if(columnsTableName != null) {
            if(columnsTableName.getSchemaName() == null && correlationName == null)
            {
                columnsTableName.bind();
            }
        }
		/*
		** If there is a correlation name, use that instead of the
		** table name.
		*/
        exposedTableName = getExposedTableName();

        if (exposedTableName.getSchemaName() == null
                && correlationName == null) {
            exposedTableName.bind();
        }

		/*
		** If the column did not specify a name, or the specified name
		** matches the table we're looking at, see whether the column
		** is in this table.
		*/
		if (columnsTableName == null || columnsTableName.equals(exposedTableName))
		{
            //
            // The only way that we can be looking up a column reference BEFORE
            // the base table is bound is if we are binding a reference inside an argument
            // to a VTI/tableFunction. See DERBY-5779. This can happen in the following
            // query:
            //
            // select tt.*
            //     from
            //         ( select tablename from table (syscs_diag.space_table( systabs.tablename )) as t2 ) tt,
            //         sys.systables systabs
            //     where systabs.tabletype = 'T' and systabs.tableid = tt.tableid;
            //
            if ( getResultColumns() == null )
            {
                throw StandardException.newException
                    ( SQLState.LANG_BAD_TABLE_FUNCTION_PARAM_REF, columnReference.getColumnName() );
            }
            
			resultColumn = getResultColumns().getResultColumn(columnReference.getColumnName());
            /* Did we find a match? */
			if (resultColumn != null)
			{
				columnReference.setTableNumber(tableNumber);
                columnReference.setColumnNumber(
                    resultColumn.getColumnPosition());

                // set the column-referenced bit if this is not the row location column
				if (
                    (tableDescriptor != null) &&
                    ( (rowLocationColumnName == null) || !(rowLocationColumnName.equals( columnReference.getColumnName() )) )
                    )
				{
                    //
                    // Add a privilege for this column if the bind() phase of an UPDATE
                    // statement marked it as a selected column. see DERBY-6429.
                    //
                    if ( columnReference.isPrivilegeCollectionRequired() )
                    {
                        if ( columnReference.taggedWith( TagFilter.NEED_PRIVS_FOR_UPDATE_STMT ) )
                        {
                            getCompilerContext().addRequiredColumnPriv
                                ( tableDescriptor.getColumnDescriptor( columnReference.getColumnName() ) );
                        }
                    }
                         
					FormatableBitSet referencedColumnMap = tableDescriptor.getReferencedColumnMap();
					if (referencedColumnMap == null)
						referencedColumnMap = new FormatableBitSet(
									tableDescriptor.getNumberOfColumns() + 1);
					referencedColumnMap.set(resultColumn.getColumnPosition());
					tableDescriptor.setReferencedColumnMap(referencedColumnMap);
				}
			}
		}

		return resultColumn;
	}

	/**
	 * Preprocess a ResultSetNode - this currently means:
	 *	o  Generating a referenced table map for each ResultSetNode.
	 *  o  Putting the WHERE and HAVING clauses in conjunctive normal form (CNF).
	 *  o  Converting the WHERE and HAVING clauses into PredicateLists and
	 *	   classifying them.
	 *  o  Ensuring that a ProjectRestrictNode is generated on top of every 
	 *     FromBaseTable and generated in place of every FromSubquery.  
	 *  o  Pushing single table predicates down to the new ProjectRestrictNodes.
	 *
	 * @param numTables			The number of tables in the DML Statement
	 * @param gbl				The group by list, if any
	 * @param fromList			The from list, if any
	 *
	 * @return ResultSetNode at top of preprocessed tree.
	 *
	 * @exception StandardException		Thrown on error
	 */

    @Override
    ResultSetNode preprocess(int numTables,
									GroupByList gbl,
									FromList fromList)
								throws StandardException
	{
        //
        // We're done with binding, so we should know which columns
        // are referenced. We check to see if SYSUSERS.PASSWORD is referenced.
        // Even the DBO is not allowed to SELECT that column.
        // This is to prevent us from instantiating the password as a
        // String. See DERBY-866.
        // We do this check before optimization because the optimizer may
        // change the result column list as it experiments with different access paths.
        // At preprocess() time, the result column list should be the columns in the base
        // table.
        //
        if ( authorizeSYSUSERS )
        {
            int passwordColNum = SYSUSERSRowFactory.PASSWORD_COL_NUM;

            FormatableBitSet    refCols = getResultColumns().getReferencedFormatableBitSet( false, true, false );

            if (
                (refCols.getLength() >= passwordColNum ) && refCols.isSet( passwordColNum - 1 )
               )
            {
                throw StandardException.newException
                    ( SQLState.HIDDEN_COLUMN, SYSUSERSRowFactory.TABLE_NAME, SYSUSERSRowFactory.PASSWORD_COL_NAME );
            }
        }
        
        /* Generate the referenced table map */
		setReferencedTableMap( new JBitSet(numTables) );
		getReferencedTableMap().set(tableNumber);

        return genProjectRestrict(numTables);
	}

	/** 
	 * Put a ProjectRestrictNode on top of each FromTable in the FromList.
	 * ColumnReferences must continue to point to the same ResultColumn, so
	 * that ResultColumn must percolate up to the new PRN.  However,
	 * that ResultColumn will point to a new expression, a VirtualColumnNode, 
	 * which points to the FromTable and the ResultColumn that is the source for
	 * the ColumnReference.  
	 * (The new PRN will have the original of the ResultColumnList and
	 * the ResultColumns from that list.  The FromTable will get shallow copies
	 * of the ResultColumnList and its ResultColumns.  ResultColumn.expression
	 * will remain at the FromTable, with the PRN getting a new 
	 * VirtualColumnNode for each ResultColumn.expression.)
	 * We then project out the non-referenced columns.  If there are no referenced
	 * columns, then the PRN's ResultColumnList will consist of a single ResultColumn
	 * whose expression is 1.
	 *
	 * @param numTables			Number of tables in the DML Statement
	 *
	 * @return The generated ProjectRestrictNode atop the original FromTable.
	 *
	 * @exception StandardException		Thrown on error
	 */

    @Override
	protected ResultSetNode genProjectRestrict(int numTables)
				throws StandardException
	{
		/* We get a shallow copy of the ResultColumnList and its 
		 * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
		 */
		ResultColumnList prRCList = getResultColumns();
		setResultColumns( getResultColumns().copyListAndObjects() );
		getResultColumns().setIndexRow( baseConglomerateDescriptor.getConglomerateNumber(), forUpdate() );

		/* Replace ResultColumn.expression with new VirtualColumnNodes
		 * in the ProjectRestrictNode's ResultColumnList.  (VirtualColumnNodes include
		 * pointers to source ResultSetNode, this, and source ResultColumn.)
		 * NOTE: We don't want to mark the underlying RCs as referenced, otherwise
		 * we won't be able to project out any of them.
		 */
		prRCList.genVirtualColumnNodes(this, getResultColumns(), false);

		/* Project out any unreferenced columns.  If there are no referenced 
		 * columns, generate and bind a single ResultColumn whose expression is 1.
		 */
		prRCList.doProjection();

		/* Finally, we create the new ProjectRestrictNode */
        ProjectRestrictNode result =
                new ProjectRestrictNode(
								this,
								prRCList,
								null,	/* Restriction */
								null,   /* Restriction as PredicateList */
								null,	/* Project subquery list */
								null,	/* Restrict subquery list */
								null,
                                getContextManager());

        if (isValidatingCheckConstraint()) {
            CompilerContext cc = getCompilerContext();

            if ((cc.getReliability() &
                 // Internal feature: throw if used on app level
                 CompilerContext.INTERNAL_SQL_ILLEGAL) != 0) {

                throw StandardException.newException(
                        SQLState.LANG_SYNTAX_ERROR, "validateCheckConstraint");
            }

            result.setValidatingCheckConstraints(targetTableCID);
        }
        return result;
	}

	/**
	 * @see ResultSetNode#changeAccessPath
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    ResultSetNode changeAccessPath() throws StandardException
	{
		ResultSetNode	retval;
		AccessPath ap = getTrulyTheBestAccessPath();
		ConglomerateDescriptor trulyTheBestConglomerateDescriptor = 
							 					ap.getConglomerateDescriptor();
		JoinStrategy trulyTheBestJoinStrategy = ap.getJoinStrategy();
        Optimizer opt = ap.getOptimizer();

        if (optimizerTracingIsOn()) {
            getOptimizerTracer().traceChangingAccessPathForTable(tableNumber);
        }

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(
				trulyTheBestConglomerateDescriptor != null,
				"Should only modify access path after conglomerate has been chosen.");
		}

		/*
		** Make sure user-specified bulk fetch is OK with the chosen join
		** strategy.
		*/
		if (bulkFetch != UNSET)
		{
			if ( ! trulyTheBestJoinStrategy.bulkFetchOK())
			{
				throw StandardException.newException(SQLState.LANG_INVALID_BULK_FETCH_WITH_JOIN_TYPE, 
											trulyTheBestJoinStrategy.getName());
			}
			// bulkFetch has no meaning for hash join, just ignore it
			else if (trulyTheBestJoinStrategy.ignoreBulkFetch())
			{
				disableBulkFetch();
			}
			// bug 4431 - ignore bulkfetch property if it's 1 row resultset
			else if (isOneRowResultSet())
			{
				disableBulkFetch();
			}
		}

		// bulkFetch = 1 is the same as no bulk fetch
		if (bulkFetch == 1)
		{
			disableBulkFetch();
		}

		/* Remove any redundant join clauses.  A redundant join clause is one
		 * where there are other join clauses in the same equivalence class
		 * after it in the PredicateList.
		 */
		restrictionList.removeRedundantPredicates();

		/*
		** Divide up the predicates for different processing phases of the
		** best join strategy.
		*/
        storeRestrictionList = new PredicateList(getContextManager());
        nonStoreRestrictionList = new PredicateList(getContextManager());
        requalificationRestrictionList = new PredicateList(getContextManager());

        trulyTheBestJoinStrategy.divideUpPredicateLists(
											this,
											restrictionList,
											storeRestrictionList,
											nonStoreRestrictionList,
											requalificationRestrictionList,
											getDataDictionary());

		/* Check to see if we are going to do execution-time probing
		 * of an index using IN-list values.  We can tell by looking
		 * at the restriction list: if there is an IN-list probe
		 * predicate that is also a start/stop key then we know that
		 * we're going to do execution-time probing.  In that case
		 * we disable bulk fetching to minimize the number of non-
		 * matching rows that we read from disk.  RESOLVE: Do we
		 * really need to completely disable bulk fetching here,
		 * or can we do something else?
		 */
        for (Predicate pred : restrictionList)
		{
			if (pred.isInListProbePredicate() && pred.isStartKey())
			{
				disableBulkFetch();
				multiProbing = true;
				break;
			}
		}

		/*
		** Consider turning on bulkFetch if it is turned
		** off.  Only turn it on if it is a not an updatable
		** scan and if it isn't a oneRowResultSet, and
		** not a subquery, and it is OK to use bulk fetch
		** with the chosen join strategy.  NOTE: the subquery logic
		** could be more sophisticated -- we are taking
		** the safe route in avoiding reading extra
		** data for something like:
		**
		**	select x from t where x in (select y from t)
	 	**
		** In this case we want to stop the subquery
		** evaluation as soon as something matches.
		*/
		if (trulyTheBestJoinStrategy.bulkFetchOK() &&
			!(trulyTheBestJoinStrategy.ignoreBulkFetch()) &&
			! bulkFetchTurnedOff &&
			(bulkFetch == UNSET) && 
			!forUpdate() && 
			!isOneRowResultSet() &&
            getLevel() == 0 &&
            !validatingCheckConstraint)
		{
			bulkFetch = getDefaultBulkFetch();	
		}

		/* Statement is dependent on the chosen conglomerate. */
		getCompilerContext().createDependency(
				trulyTheBestConglomerateDescriptor);

		/* No need to modify access path if conglomerate is the heap */
		if ( ! trulyTheBestConglomerateDescriptor.isIndex())
		{
			/*
			** We need a little special logic for SYSSTATEMENTS
			** here.  SYSSTATEMENTS has a hidden column at the
			** end.  When someone does a select * we don't want
			** to get that column from the store.  So we'll always
			** generate a partial read bitSet if we are scanning
			** SYSSTATEMENTS to ensure we don't get the hidden
			** column.
			*/
			boolean isSysstatements = tableName.equals("SYS","SYSSTATEMENTS");
			/* Template must reflect full row.
			 * Compact RCL down to partial row.
			 */
			templateColumns = getResultColumns();
			referencedCols = getResultColumns().getReferencedFormatableBitSet(isCursorTargetTable(), isSysstatements, false);
			setResultColumns( getResultColumns().compactColumns(isCursorTargetTable(), isSysstatements) );
			return this;
		}
		
		/* No need to go to the data page if this is a covering index */
		/* Derby-1087: use data page when returning an updatable resultset */
		if (ap.getCoveringIndexScan() && (!isCursorTargetTable()))
		{
			/* Massage resultColumns so that it matches the index. */
			setResultColumns
                (
                 newResultColumns
                 (
                  getResultColumns(),
                  trulyTheBestConglomerateDescriptor,
                  baseConglomerateDescriptor,
                  false
                  )
                 );

			/* We are going against the index.  The template row must be the full index row.
			 * The template row will have the RID but the result row will not
			 * since there is no need to go to the data page.
			 */
			templateColumns = newResultColumns(getResultColumns(),
				 							 trulyTheBestConglomerateDescriptor,
											 baseConglomerateDescriptor,
											 false);
			templateColumns.addRCForRID();

			// If this is for update then we need to get the RID in the result row
			if (forUpdate())
			{
				getResultColumns().addRCForRID();
			}
			
			/* Compact RCL down to the partial row.  We always want a new
			 * RCL and FormatableBitSet because this is a covering index.  (This is 
			 * because we don't want the RID in the partial row returned
			 * by the store.)
			 */
			referencedCols = getResultColumns().getReferencedFormatableBitSet(isCursorTargetTable(),true, false);
			setResultColumns( getResultColumns().compactColumns(isCursorTargetTable(),true) );

			getResultColumns().setIndexRow(
				baseConglomerateDescriptor.getConglomerateNumber(), 
				forUpdate());

			return this;
		}

		/* Statement is dependent on the base conglomerate if this is 
		 * a non-covering index. 
		 */
		getCompilerContext().createDependency(baseConglomerateDescriptor);

		/*
		** On bulkFetch, we need to add the restrictions from
		** the TableScan and reapply them  here.
		*/
		if (bulkFetch != UNSET)
		{
			restrictionList.copyPredicatesToOtherList(
												requalificationRestrictionList);
		}

		/*
		** We know the chosen conglomerate is an index.  We need to allocate
		** an IndexToBaseRowNode above us, and to change the result column
		** list for this FromBaseTable to reflect the columns in the index.
		** We also need to shift "cursor target table" status from this
		** FromBaseTable to the new IndexToBaseRowNow (because that's where
		** a cursor can fetch the current row).
		*/
		ResultColumnList newResultColumns =
			newResultColumns(getResultColumns(),
							trulyTheBestConglomerateDescriptor,
							baseConglomerateDescriptor,
							true
							);

		/* Compact the RCL for the IndexToBaseRowNode down to
		 * the partial row for the heap.  The referenced BitSet
		 * will reflect only those columns coming from the heap.
		 * (ie, it won't reflect columns coming from the index.)
		 * NOTE: We need to re-get all of the columns from the heap
		 * when doing a bulk fetch because we will be requalifying
		 * the row in the IndexRowToBaseRow.
		 */
		// Get the BitSet for all of the referenced columns
		FormatableBitSet indexReferencedCols = null;
        FormatableBitSet heapReferencedCols;

		if ((bulkFetch == UNSET) && 
			(requalificationRestrictionList == null || 
			 requalificationRestrictionList.size() == 0))
		{
			/* No BULK FETCH or requalification, XOR off the columns coming from the heap 
			 * to get the columns coming from the index.
			 */
			indexReferencedCols = getResultColumns().getReferencedFormatableBitSet(isCursorTargetTable(), true, false);
			heapReferencedCols = getResultColumns().getReferencedFormatableBitSet(isCursorTargetTable(), true, true);
			if (heapReferencedCols != null)
			{
				indexReferencedCols.xor(heapReferencedCols);
			}
		}
		else
		{
			// BULK FETCH or requalification - re-get all referenced columns from the heap
			heapReferencedCols = getResultColumns().getReferencedFormatableBitSet(isCursorTargetTable(), true, false) ;
		}
		ResultColumnList heapRCL = getResultColumns().compactColumns(isCursorTargetTable(), false);
		heapRCL.setIndexRow
            (
             baseConglomerateDescriptor.getConglomerateNumber(), 
             forUpdate()
             );
        retval = new IndexToBaseRowNode(this,
										baseConglomerateDescriptor,
										heapRCL,
                                        isCursorTargetTable(),
										heapReferencedCols,
										indexReferencedCols,
										requalificationRestrictionList,
                                        forUpdate(),
										tableProperties,
										getContextManager());

		/*
		** The template row is all the columns.  The
		** result set is the compacted column list.
		*/
		setResultColumns( newResultColumns );

		templateColumns = newResultColumns(getResultColumns(),
			 							   trulyTheBestConglomerateDescriptor,
										   baseConglomerateDescriptor,
										   false);
		/* Since we are doing a non-covered index scan, if bulkFetch is on, then
		 * the only columns that we need to get are those columns referenced in the start and stop positions
		 * and the qualifiers (and the RID) because we will need to re-get all of the other
		 * columns from the heap anyway.
		 * At this point in time, columns referenced anywhere in the column tree are 
		 * marked as being referenced.  So, we clear all of the references, walk the 
		 * predicate list and remark the columns referenced from there and then add
		 * the RID before compacting the columns.
		 */
		if (bulkFetch != UNSET)
		{
			getResultColumns().markAllUnreferenced();
			storeRestrictionList.markReferencedColumns();
			if (nonStoreRestrictionList != null)
			{
				nonStoreRestrictionList.markReferencedColumns();
			}
		}
		getResultColumns().addRCForRID();
		templateColumns.addRCForRID();

		// Compact the RCL for the index scan down to the partial row.
		referencedCols = getResultColumns().getReferencedFormatableBitSet(isCursorTargetTable(), false, false);
		setResultColumns( getResultColumns().compactColumns(isCursorTargetTable(), false) );
		getResultColumns().setIndexRow(
				baseConglomerateDescriptor.getConglomerateNumber(), 
				forUpdate());

		/* We must remember if this was the cursorTargetTable
 		 * in order to get the right locking on the scan.
		 */
		getUpdateLocks = isCursorTargetTable();
		setCursorTargetTable( false );

		return retval;
	}

	/**
	 * Create a new ResultColumnList to reflect the columns in the
	 * index described by the given ConglomerateDescriptor.  The columns
	 * in the new ResultColumnList are based on the columns in the given
	 * ResultColumnList, which reflects the columns in the base table.
	 *
	 * @param oldColumns	The original list of columns, which reflects
	 *						the columns in the base table.
	 * @param idxCD			The ConglomerateDescriptor, which describes
	 *						the index that the new ResultColumnList will
	 *						reflect.
	 * @param heapCD		The ConglomerateDescriptor for the base heap
	 * @param cloneRCs		Whether or not to clone the RCs
	 *
	 * @return	A new ResultColumnList that reflects the columns in the index.
	 *
	 * @exception StandardException		Thrown on error
	 */
	private ResultColumnList newResultColumns(
										ResultColumnList oldColumns,
										ConglomerateDescriptor idxCD,
										ConglomerateDescriptor heapCD,
										boolean cloneRCs)
						throws StandardException
	{
		IndexRowGenerator	irg = idxCD.getIndexDescriptor();
		int[]				baseCols = irg.baseColumnPositions();
        ResultColumnList newCols = new ResultColumnList((getContextManager()));

		for (int i = 0; i < baseCols.length; i++)
		{
			int	basePosition = baseCols[i];
			ResultColumn oldCol = oldColumns.getResultColumn(basePosition);
			ResultColumn newCol;

			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(oldCol != null, 
							"Couldn't find base column "+basePosition+
							"\n.  RCL is\n"+oldColumns);
			}

			/* If we're cloning the RCs its because we are
			 * building an RCL for the index when doing
			 * a non-covering index scan.  Set the expression
			 * for the old RC to be a VCN pointing to the
			 * new RC.
			 */
			if (cloneRCs)
			{
				newCol = oldCol.cloneMe();
                oldCol.setExpression(new VirtualColumnNode(
						this,
						newCol,
                        oldCol.getVirtualColumnId(),
						getContextManager()));
			}
			else
			{
				newCol = oldCol;
			}

			newCols.addResultColumn(newCol);
		}

		/*
		** The conglomerate is an index, so we need to generate a RowLocation
		** as the last column of the result set.  Notify the ResultColumnList
		** that it needs to do this.  Also tell the RCL whether this is
		** the target of an update, so it can tell the conglomerate controller
		** when it is getting the RowLocation template.  
		*/
		newCols.setIndexRow(heapCD.getConglomerateNumber(), forUpdate());

		return newCols;
	}

	/**
	 * Generation on a FromBaseTable creates a scan on the
	 * optimizer-selected conglomerate.
	 *
	 * @param acb	The ActivationClassBuilder for the class being built
	 * @param mb	the execute() method to be built
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    void generate(ActivationClassBuilder acb, MethodBuilder mb)
							throws StandardException
	{
        if ( rowLocationColumnName != null )
        {
            getResultColumns().conglomerateId = tableDescriptor.getHeapConglomerateId();
        }

		generateResultSet( acb, mb );

		/*
		** Remember if this base table is the cursor target table, so we can
		** know which table to use when doing positioned update and delete
		*/
		if (isCursorTargetTable())
		{
			acb.rememberCursorTarget(mb);
		}
	}

	/**
	 * Generation on a FromBaseTable for a SELECT. This logic was separated
	 * out so that it could be shared with PREPARE SELECT FILTER.
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The execute() method to be built
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    void generateResultSet(ExpressionClassBuilder acb, MethodBuilder mb)
							throws StandardException
	{
		/* We must have been a best conglomerate descriptor here */
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(
			getTrulyTheBestAccessPath().getConglomerateDescriptor() != null);

		/* Get the next ResultSet #, so that we can number this ResultSetNode, its
		 * ResultColumnList and ResultSet.
		 */
		assignResultSetNumber();

		/*
		** If we are doing a special scan to get the last row
		** of an index, generate it separately.
		*/
		if (specialMaxScan)
		{
	   		generateMaxSpecialResultSet(acb, mb);
			return;
		}

		/*
		** If we are doing a special distinct scan, generate
		** it separately.
		*/
		if (distinctScan)
		{
	   		generateDistinctScan(acb, mb);
			return;
		}
		
		/*
		 * Referential action dependent table scan, generate it
		 * seperately.
		 */

		if(raDependentScan)
		{
			generateRefActionDependentTableScan(acb, mb);
			return;

		}
	
		JoinStrategy trulyTheBestJoinStrategy =
			getTrulyTheBestAccessPath().getJoinStrategy();

		// the table scan generator is what we return
		acb.pushGetResultSetFactoryExpression(mb);

		int nargs = getScanArguments(acb, mb);

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null,
			trulyTheBestJoinStrategy.resultSetMethodName(
                (bulkFetch != UNSET), multiProbing, validatingCheckConstraint),
			ClassName.NoPutResultSet, nargs);

		/* If this table is the target of an update or a delete, then we must 
		 * wrap the Expression up in an assignment expression before 
		 * returning.
		 * NOTE - scanExpress is a ResultSet.  We will need to cast it to the
		 * appropriate subclass.
		 * For example, for a DELETE, instead of returning a call to the 
		 * ResultSetFactory, we will generate and return:
		 *		this.SCANRESULTSET = (cast to appropriate ResultSet type) 
		 * The outer cast back to ResultSet is needed so that
		 * we invoke the appropriate method.
		 *										(call to the ResultSetFactory)
		 */
		if ((updateOrDelete == UPDATE) || (updateOrDelete == DELETE))
		{
			mb.cast(ClassName.CursorResultSet);
			mb.putField(acb.getRowLocationScanResultSetName(), ClassName.CursorResultSet);
			mb.cast(ClassName.NoPutResultSet);
		}
	}

	/**
	 * Get the final CostEstimate for this ResultSetNode.
	 *
	 * @return	The final CostEstimate for this ResultSetNode.
	 */
    @Override
    CostEstimate getFinalCostEstimate()
	{
		return getTrulyTheBestAccessPath().getCostEstimate();
	}
	
        /* helper method used by generateMaxSpecialResultSet and
         * generateDistinctScan to return the name of the index if the 
         * conglomerate is an index. 
         * @param cd   Conglomerate for which we need to push the index name
         * @param mb   Associated MethodBuilder
         * @throws StandardException
         */
    private void pushIndexName(ConglomerateDescriptor cd, MethodBuilder mb)
          throws StandardException
    {
        if (cd.isConstraint()) {
            DataDictionary dd = getDataDictionary();
            ConstraintDescriptor constraintDesc =
                    dd.getConstraintDescriptor(tableDescriptor, cd.getUUID());
            mb.push(constraintDesc.getConstraintName());
        } else if (cd.isIndex())  {
            mb.push(cd.getConglomerateName());
        } else {
            // If the conglomerate is the base table itself, make sure we push
            // null. Before the fix for DERBY-578, we would push the base table
            // name and this was just plain wrong and would cause statistics
            // information to be incorrect.
            mb.pushNull("java.lang.String");
        }
    }
	
    private void generateMaxSpecialResultSet(
            ExpressionClassBuilder  acb,
            MethodBuilder mb) throws StandardException
    {
		ConglomerateDescriptor cd = getTrulyTheBestAccessPath().getConglomerateDescriptor();
        CostEstimate costEst = getFinalCostEstimate();
		int colRefItem = (referencedCols == null) ?
						-1 :
						acb.addItem(referencedCols);
		boolean tableLockGranularity = tableDescriptor.getLockGranularity() == TableDescriptor.TABLE_LOCK_GRANULARITY;
	
		/*
		** getLastIndexKeyResultSet
		** (
		**		activation,			
		**		resultSetNumber,			
		**		resultRowAllocator,			
		**		conglomereNumber,			
		**		tableName,
		**		optimizeroverride			
		**		indexName,			
		**		colRefItem,			
		**		lockMode,			
		**		tableLocked,
		**		isolationLevel,
		**		optimizerEstimatedRowCount,
		**		optimizerEstimatedRowCost,
		**	);
		*/

		acb.pushGetResultSetFactoryExpression(mb);

		acb.pushThisAsActivation(mb);
		mb.push(getResultSetNumber());
        mb.push(acb.addItem(
                            getResultColumns().buildRowTemplate(referencedCols, false)));
		mb.push(cd.getConglomerateNumber());
		mb.push(tableDescriptor.getName());
		//User may have supplied optimizer overrides in the sql
		//Pass them onto execute phase so it can be shown in 
		//run time statistics.
		if (tableProperties != null)
			mb.push(org.apache.derby.iapi.util.PropertyUtil.sortProperties(tableProperties));
		else
			mb.pushNull("java.lang.String");
                pushIndexName(cd, mb);
		mb.push(colRefItem);
		mb.push(getTrulyTheBestAccessPath().getLockMode());
		mb.push(tableLockGranularity);
		mb.push(getCompilerContext().getScanIsolationLevel());
        mb.push(costEst.singleScanRowCount());
        mb.push(costEst.getEstimatedCost());

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getLastIndexKeyResultSet",
					ClassName.NoPutResultSet, 13);


	}

	private void generateDistinctScan
	(
		ExpressionClassBuilder	acb,
		MethodBuilder mb
	) throws StandardException
	{
		ConglomerateDescriptor cd = getTrulyTheBestAccessPath().getConglomerateDescriptor();
        CostEstimate costEst = getFinalCostEstimate();
		int colRefItem = (referencedCols == null) ?
						-1 :
						acb.addItem(referencedCols);
		boolean tableLockGranularity = tableDescriptor.getLockGranularity() == TableDescriptor.TABLE_LOCK_GRANULARITY;
	
		/*
		** getDistinctScanResultSet
		** (
		**		activation,			
		**		resultSetNumber,			
		**		resultRowAllocator,			
		**		conglomereNumber,			
		**		tableName,
		**		optimizeroverride			
		**		indexName,			
		**		colRefItem,			
		**		lockMode,			
		**		tableLocked,
		**		isolationLevel,
		**		optimizerEstimatedRowCount,
		**		optimizerEstimatedRowCost,
		**		closeCleanupMethod
		**	);
		*/

		/* Get the hash key columns and wrap them in a formattable */
        int[] hashKeyCols;

        hashKeyCols = new int[getResultColumns().size()];
		if (referencedCols == null)
		{
            for (int index = 0; index < hashKeyCols.length; index++)
			{
                hashKeyCols[index] = index;
			}
		}
		else
		{
			int index = 0;
			for (int colNum = referencedCols.anySetBit();
					colNum != -1;
					colNum = referencedCols.anySetBit(colNum))
			{
                hashKeyCols[index++] = colNum;
			}
		}

		FormatableIntHolder[] fihArray = 
                FormatableIntHolder.getFormatableIntHolders(hashKeyCols);
		FormatableArrayHolder hashKeyHolder = new FormatableArrayHolder(fihArray);
		int hashKeyItem = acb.addItem(hashKeyHolder);
		long conglomNumber = cd.getConglomerateNumber();
		StaticCompiledOpenConglomInfo scoci = getLanguageConnectionContext().
												getTransactionCompile().
													getStaticCompiledConglomInfo(conglomNumber);

		acb.pushGetResultSetFactoryExpression(mb);

     	acb.pushThisAsActivation(mb);
		mb.push(conglomNumber);
		mb.push(acb.addItem(scoci));
        mb.push(acb.addItem(
                            getResultColumns().buildRowTemplate(referencedCols, false)));
		mb.push(getResultSetNumber());
		mb.push(hashKeyItem);
		mb.push(tableDescriptor.getName());
		//User may have supplied optimizer overrides in the sql
		//Pass them onto execute phase so it can be shown in 
		//run time statistics.
		if (tableProperties != null)
			mb.push(org.apache.derby.iapi.util.PropertyUtil.sortProperties(tableProperties));
		else
			mb.pushNull("java.lang.String");
		pushIndexName(cd, mb);
		mb.push(cd.isConstraint());
		mb.push(colRefItem);
		mb.push(getTrulyTheBestAccessPath().getLockMode());
		mb.push(tableLockGranularity);
		mb.push(getCompilerContext().getScanIsolationLevel());
        mb.push(costEst.singleScanRowCount());
        mb.push(costEst.getEstimatedCost());
		
		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getDistinctScanResultSet",
							ClassName.NoPutResultSet, 16);
	}


	/**
	 * Generation on a FromBaseTable for a referential action dependent table.
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The execute() method to be built
	 *
	 * @exception StandardException		Thrown on error
	 */

	private void generateRefActionDependentTableScan
	(
		ExpressionClassBuilder	acb,
		MethodBuilder mb
	) throws StandardException
	{

		acb.pushGetResultSetFactoryExpression(mb);

		//get the parameters required to do a table scan
		int nargs = getScanArguments(acb, mb);

		//extra parameters required to create an dependent table result set.
		mb.push(raParentResultSetId);  //id for the parent result set.
		mb.push(fkIndexConglomId);
		mb.push(acb.addItem(fkColArray));
		mb.push(acb.addItem(getDataDictionary().getRowLocationTemplate(
                      getLanguageConnectionContext(), tableDescriptor)));

		int argCount = nargs + 4;
		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getRaDependentTableScanResultSet",
							ClassName.NoPutResultSet, argCount);

		if ((updateOrDelete == UPDATE) || (updateOrDelete == DELETE))
		{
			mb.cast(ClassName.CursorResultSet);
			mb.putField(acb.getRowLocationScanResultSetName(), ClassName.CursorResultSet);
			mb.cast(ClassName.NoPutResultSet);
		}

	}



	private int getScanArguments(ExpressionClassBuilder acb,
										  MethodBuilder mb)
		throws StandardException
	{
        // Put the result row template in the saved objects.
        int resultRowTemplate =
            acb.addItem(getResultColumns().buildRowTemplate(referencedCols, false));

		// pass in the referenced columns on the saved objects
		// chain
		int colRefItem = -1;
		if (referencedCols != null)
		{
			colRefItem = acb.addItem(referencedCols);
		}

		// beetle entry 3865: updateable cursor using index
		int indexColItem = -1;
		if (isCursorTargetTable() || getUpdateLocks)
		{
			ConglomerateDescriptor cd = getTrulyTheBestAccessPath().getConglomerateDescriptor();
			if (cd.isIndex())
			{
				int[] baseColPos = cd.getIndexDescriptor().baseColumnPositions();
				boolean[] isAscending = cd.getIndexDescriptor().isAscending();
				int[] indexCols = new int[baseColPos.length];
				for (int i = 0; i < indexCols.length; i++)
					indexCols[i] = isAscending[i] ? baseColPos[i] : -baseColPos[i];
				indexColItem = acb.addItem(indexCols);
			}
		}

        AccessPath ap = getTrulyTheBestAccessPath();
		JoinStrategy trulyTheBestJoinStrategy =	ap.getJoinStrategy();

		/*
		** We can only do bulkFetch on NESTEDLOOP
		*/
		if (SanityManager.DEBUG)
		{
			if ( ( ! trulyTheBestJoinStrategy.bulkFetchOK()) &&
				(bulkFetch != UNSET))
			{
				SanityManager.THROWASSERT("bulkFetch should not be set "+
								"for the join strategy " +
								trulyTheBestJoinStrategy.getName());
			}
		}

		int nargs = trulyTheBestJoinStrategy.getScanArgs(
											getLanguageConnectionContext().getTransactionCompile(),
											mb,
											this,
											storeRestrictionList,
											nonStoreRestrictionList,
											acb,
											bulkFetch,
											resultRowTemplate,
											colRefItem,
											indexColItem,
											getTrulyTheBestAccessPath().
																getLockMode(),
											(tableDescriptor.getLockGranularity() == TableDescriptor.TABLE_LOCK_GRANULARITY),
											getCompilerContext().getScanIsolationLevel(),
											ap.getOptimizer().getMaxMemoryPerTable(),
											multiProbing
											);

		return nargs;
	}

	/**
	 * Convert an absolute to a relative 0-based column position.
	 *
	 * @param absolutePosition	The absolute 0-based column position.
	 *
	 * @return The relative 0-based column position.
	 */
	private int mapAbsoluteToRelativeColumnPosition(int absolutePosition)
	{
		if (referencedCols == null)
		{
			return absolutePosition;
		}

		/* setBitCtr counts the # of columns in the row,
		 * from the leftmost to the absolutePosition, that will be
		 * in the partial row returned by the store.  This becomes
		 * the new value for column position.
		 */
		int setBitCtr = 0;
		int bitCtr = 0;
		for ( ; 
			 bitCtr < referencedCols.size() && bitCtr < absolutePosition; 
			 bitCtr++)
		{
			if (referencedCols.get(bitCtr))
			{
				setBitCtr++;
			}
		}
		return setBitCtr;
	}

	/**
	 * Get the exposed name for this table, which is the name that can
	 * be used to refer to it in the rest of the query.
	 *
	 * @return	The exposed name of this table.
	 *
	 */
    @Override
    String getExposedName()
	{
		if (correlationName != null)
			return correlationName;
		else
			return getOrigTableName().getFullTableName();
	}
	
	/**
	 * Get the exposed table name for this table, which is the name that can
	 * be used to refer to it in the rest of the query.
	 *
	 * @return	TableName The exposed name of this table.
	 *
	 * @exception StandardException  Thrown on error
	 */
	TableName getExposedTableName() throws StandardException  
	{
		if (correlationName != null)
			return makeTableName(null, correlationName);
		else
			return getOrigTableName();
	}
	
	/**
	 * Return the table name for this table.
	 *
	 * @return	The table name for this table.
	 */

    TableName getTableNameField()
	{
		return tableName;
	}

	/**
	 * Return a ResultColumnList with all of the columns in this table.
	 * (Used in expanding '*'s.)
	 * NOTE: Since this method is for expanding a "*" in the SELECT list,
	 * ResultColumn.expression will be a ColumnReference.
	 *
	 * @param allTableName		The qualifier on the "*"
	 *
	 * @return ResultColumnList	List of result columns from this table.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    ResultColumnList getAllResultColumns(TableName allTableName)
			throws StandardException
	{
		return getResultColumnsForList(allTableName, getResultColumns(), 
				getOrigTableName());
	}

	/**
	 * Build a ResultColumnList based on all of the columns in this FromBaseTable.
	 * NOTE - Since the ResultColumnList generated is for the FromBaseTable,
	 * ResultColumn.expression will be a BaseColumnNode.
	 *
	 * @return ResultColumnList representing all referenced columns
	 *
	 * @exception StandardException		Thrown on error
	 */
    ResultColumnList genResultColList()
			throws StandardException
	{
		ResultColumn	 			resultColumn;
		ValueNode		 			valueNode;

		/* Cache exposed name for this table.
		 * The exposed name becomes the qualifier for each column
		 * in the expanded list.
		 */
        TableName exposedName = getExposedTableName();

		/* Add all of the columns in the table */
        ResultColumnList rcList = new ResultColumnList((getContextManager()));
		ColumnDescriptorList cdl = tableDescriptor.getColumnDescriptorList();
		int					 cdlSize = cdl.size();

		for (int index = 0; index < cdlSize; index++)
		{
			/* Build a ResultColumn/BaseColumnNode pair for the column */
            ColumnDescriptor colDesc = cdl.elementAt(index);
			//A ColumnDescriptor instantiated through SYSCOLUMNSRowFactory only has 
			//the uuid set on it and no table descriptor set on it. Since we know here
			//that this columnDescriptor is tied to tableDescriptor, set it so using
			//setTableDescriptor method. ColumnDescriptor's table descriptor is used
			//to get ResultSetMetaData.getTableName & ResultSetMetaData.getSchemaName
			colDesc.setTableDescriptor(tableDescriptor);

            valueNode = new BaseColumnNode(colDesc.getColumnName(),
									  		exposedName,
											colDesc.getType(),
											getContextManager());
            resultColumn =
                new ResultColumn(colDesc, valueNode, getContextManager());

			/* Build the ResultColumnList to return */
			rcList.addResultColumn(resultColumn);
		}

        // add a row location column as necessary
        if ( rowLocationColumnName != null )
        {
            CurrentRowLocationNode  rowLocationNode = new CurrentRowLocationNode( getContextManager() );
            ResultColumn    rowLocationColumn = new ResultColumn
                ( rowLocationColumnName, rowLocationNode, getContextManager() );
            rowLocationColumn.markGenerated();
            rowLocationNode.bindExpression( null, null, null );
            rowLocationColumn.bindResultColumnToExpression();
            rcList.addResultColumn( rowLocationColumn );
        }

		return rcList;
	}

	/**
	 * Augment the RCL to include the columns in the FormatableBitSet.
	 * If the column is already there, don't add it twice.
	 * Column is added as a ResultColumn pointing to a 
	 * ColumnReference.
	 *
	 * @param inputRcl			The original list
	 * @param colsWeWant		bit set of cols we want
	 *
	 * @return ResultColumnList the rcl
	 *
	 * @exception StandardException		Thrown on error
	 */
    ResultColumnList addColsToList
	(
		ResultColumnList	inputRcl,
		FormatableBitSet				colsWeWant
	)
			throws StandardException
	{
		ResultColumn	 			resultColumn;
		TableName		 			exposedName;

		/* Cache exposed name for this table.
		 * The exposed name becomes the qualifier for each column
		 * in the expanded list.
		 */
		exposedName = getExposedTableName();

		/* Add all of the columns in the table */
        ResultColumnList newRcl = new ResultColumnList((getContextManager()));
		ColumnDescriptorList cdl = tableDescriptor.getColumnDescriptorList();
		int					 cdlSize = cdl.size();

		for (int index = 0; index < cdlSize; index++)
		{
			/* Build a ResultColumn/BaseColumnNode pair for the column */
            ColumnDescriptor cd = cdl.elementAt(index);
			int position = cd.getPosition();

			if (!colsWeWant.get(position))
			{
				continue;
			}

			if ((resultColumn = inputRcl.getResultColumn(position)) == null)
			{	
                ColumnReference cr = new ColumnReference(cd.getColumnName(),
												exposedName,
												getContextManager());
                if ( (getMergeTableID() != ColumnReference.MERGE_UNKNOWN ) )
                {
                    cr.setMergeTableID( getMergeTableID() );
                }

                resultColumn =
                        new ResultColumn(cd, cr, getContextManager());
			}

			/* Build the ResultColumnList to return */
			newRcl.addResultColumn(resultColumn);
		}

		return newRcl;
	}

	/**
	 * Return a TableName node representing this FromTable.
	 * @return a TableName node representing this FromTable.
	 * @exception StandardException		Thrown on error
	 */
    @Override
    TableName getTableName()
			throws StandardException
	{
        TableName tn = super.getTableName();

        if (tn != null && tn.getSchemaName() == null
                && correlationName == null) {
            tn.bind();
        }

		return (tn != null ? tn : tableName);
	}

	/**
		Mark this ResultSetNode as the target table of an updatable
		cursor.
	 */
    @Override
    boolean markAsCursorTargetTable()
	{
		setCursorTargetTable( true );
		return true;
	}

	/**
	 * Is this a table that has a FOR UPDATE
	 * clause? 
	 *
	 * @return true/false
	 */
    @Override
	protected boolean cursorTargetTable()
	{
		return isCursorTargetTable();
	}

	/**
	 * Mark as updatable all the columns in the result column list of this
	 * FromBaseTable that match the columns in the given update column list.
	 *
	 * @param updateColumns		A ResultColumnList representing the columns
	 *							to be updated.
	 */
	void markUpdated(ResultColumnList updateColumns)
	{
		getResultColumns().markUpdated(updateColumns);
	}

	/**
	 * Search to see if a query references the specifed table name.
	 *
	 * @param name		Table name (String) to search for.
	 * @param baseTable	Whether or not name is for a base table
	 *
	 * @return	true if found, else false
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    boolean referencesTarget(String name, boolean baseTable)
		throws StandardException
	{
		return baseTable && name.equals(getBaseTableName());
	}

	/**
	 * Return true if the node references SESSION schema tables (temporary or permanent)
	 *
	 * @return	true if references SESSION schema tables, else false
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public boolean referencesSessionSchema()
		throws StandardException
	{
		//If base table is a SESSION schema table, then return true. 
		return isSessionSchema(tableDescriptor.getSchemaDescriptor());
	}


	/**
	 * Return whether or not the underlying ResultSet tree will return
	 * a single row, at most.  This method is intended to be used during
	 * generation, after the "truly" best conglomerate has been chosen.
	 * This is important for join nodes where we can save the extra next
	 * on the right side if we know that it will return at most 1 row.
	 *
	 * @return Whether or not the underlying ResultSet tree will return a single row.
	 * @exception StandardException		Thrown on error
	 */
    @Override
    boolean isOneRowResultSet() throws StandardException
	{
		// EXISTS FBT will only return a single row
		if (existsBaseTable)
		{
			return true;
		}

		/* For hash join, we need to consider both the qualification
		 * and hash join predicates and we consider them against all
		 * conglomerates since we are looking for any uniqueness
		 * condition that holds on the columns in the hash table, 
		 * otherwise we just consider the predicates in the 
		 * restriction list and the conglomerate being scanned.

		 */
		AccessPath ap = getTrulyTheBestAccessPath();
		JoinStrategy trulyTheBestJoinStrategy = ap.getJoinStrategy();
		PredicateList pl;

		if (trulyTheBestJoinStrategy.isHashJoin())
		{
            pl = new PredicateList(getContextManager());

			if (storeRestrictionList != null)
			{
				pl.nondestructiveAppend(storeRestrictionList);
			}
			if (nonStoreRestrictionList != null)
			{
				pl.nondestructiveAppend(nonStoreRestrictionList);
			}
			return isOneRowResultSet(pl);
		}
		else
		{
			return isOneRowResultSet(getTrulyTheBestAccessPath().
										getConglomerateDescriptor(),
									 restrictionList);
		}
	}

	/**
	 * Return whether or not this is actually a EBT for NOT EXISTS.
	 */
    @Override
    boolean isNotExists()
	{
		return isNotExists;
	}

    boolean isOneRowResultSet(OptimizablePredicateList predList)
            throws StandardException
	{
		ConglomerateDescriptor[] cds = tableDescriptor.getConglomerateDescriptors();

		for (int index = 0; index < cds.length; index++)
		{
			if (isOneRowResultSet(cds[index], predList))
			{
				return true;
			}
		}

		return false;
	}

	/**
	 * Determine whether or not the columns marked as true in
	 * the passed in array are a superset of any unique index
	 * on this table.  
	 * This is useful for subquery flattening and distinct elimination
	 * based on a uniqueness condition.
	 *
	 * @param eqCols	The columns to consider
	 *
	 * @return Whether or not the columns marked as true are a superset
	 */
	protected boolean supersetOfUniqueIndex(boolean[] eqCols)
		throws StandardException
	{
		ConglomerateDescriptor[] cds = tableDescriptor.getConglomerateDescriptors();

		/* Cycle through the ConglomerateDescriptors */
		for (int index = 0; index < cds.length; index++)
		{
			ConglomerateDescriptor cd = cds[index];

			if (! cd.isIndex())
			{
				continue;
			}
			IndexDescriptor id = cd.getIndexDescriptor();

			if (! id.isUnique())
			{
				continue;
			}

			int[] keyColumns = id.baseColumnPositions();

			int inner = 0;
			for ( ; inner < keyColumns.length; inner++)
			{
				if (! eqCols[keyColumns[inner]])
				{
					break;
				}
			}

			/* Did we get a full match? */
			if (inner == keyColumns.length)
			{
				return true;
			}
		}

		return false;
	}

	/**
	 * Determine whether or not the columns marked as true in
	 * the passed in join table matrix are a superset of any single column unique index
	 * on this table.  
	 * This is useful for distinct elimination
	 * based on a uniqueness condition.
	 *
	 * @param tableColMap	The columns to consider
	 *
	 * @return Whether or not the columns marked as true for one at least
	 * 	one table are a superset
	 */
	protected boolean supersetOfUniqueIndex(JBitSet[] tableColMap)
		throws StandardException
	{
		ConglomerateDescriptor[] cds = tableDescriptor.getConglomerateDescriptors();

		/* Cycle through the ConglomerateDescriptors */
		for (int index = 0; index < cds.length; index++)
		{
			ConglomerateDescriptor cd = cds[index];

			if (! cd.isIndex())
			{
				continue;
			}
			IndexDescriptor id = cd.getIndexDescriptor();

			if (! id.isUnique())
			{
				continue;
			}

			int[] keyColumns = id.baseColumnPositions();
			int numBits = tableColMap[0].size();
			JBitSet keyMap = new JBitSet(numBits);
			JBitSet resMap = new JBitSet(numBits);

			int inner = 0;
			for ( ; inner < keyColumns.length; inner++)
			{
				keyMap.set(keyColumns[inner]);
			}
			int table = 0;
			for ( ; table < tableColMap.length; table++)
			{
				resMap.setTo(tableColMap[table]);
				resMap.and(keyMap);
				if (keyMap.equals(resMap))
				{
					tableColMap[table].set(0);
					return true;
				}
			}

		}

		return false;
	}

	/**
	 * Get the lock mode for the target table heap of an update or delete
	 * statement.  It is not always MODE_RECORD.  We want the lock on the
	 * heap to be consistent with optimizer and eventually system's decision.
	 * This is to avoid deadlock (beetle 4318).  During update/delete's
	 * execution, it will first use this lock mode we return to lock heap to
	 * open a RowChanger, then use the lock mode that is the optimizer and
	 * system's combined decision to open the actual source conglomerate.
	 * We've got to make sure they are consistent.  This is the lock chart (for
	 * detail reason, see comments below):
	 *		BEST ACCESS PATH			LOCK MODE ON HEAP
	 *   ----------------------		-----------------------------------------
	 *			index					  row lock
	 *
	 *			heap					  row lock if READ_COMMITTED, 
     *			                          REPEATBLE_READ, or READ_UNCOMMITTED &&
     *			                          not specified table lock otherwise, 
     *			                          use optimizer decided best acess 
     *			                          path's lock mode
	 *
	 * @return	The lock mode
	 */
    @Override
    int updateTargetLockMode()
	{
		/* if best access path is index scan, we always use row lock on heap,
		 * consistent with IndexRowToBaseRowResultSet's openCore().  We don't
		 * need to worry about the correctness of serializable isolation level
		 * because index will have previous key locking if it uses row locking
		 * as well.
		 */
		if (getTrulyTheBestAccessPath().getConglomerateDescriptor().isIndex())
			return TransactionController.MODE_RECORD;

		/* we override optimizer's decision of the lock mode on heap, and
		 * always use row lock if we are read committed/uncommitted or 
         * repeatable read isolation level, and no forced table lock.  
         *
         * This is also reflected in TableScanResultSet's constructor, 
         * KEEP THEM CONSISTENT!  
         *
         * This is to improve concurrency, while maintaining correctness with 
         * serializable level.  Since the isolation level can change between 
         * compilation and execution if the statement is cached or stored, we 
         * encode both the SERIALIZABLE lock mode and the non-SERIALIZABLE
         * lock mode in the returned lock mode if they are different.
		 */
		int isolationLevel = 
            getLanguageConnectionContext().getCurrentIsolationLevel();


		if ((isolationLevel != TransactionControl.SERIALIZABLE_ISOLATION_LEVEL) &&
			(tableDescriptor.getLockGranularity() != 
					TableDescriptor.TABLE_LOCK_GRANULARITY))
		{
			int lockMode = getTrulyTheBestAccessPath().getLockMode();
			if (lockMode != TransactionController.MODE_RECORD)
				lockMode = (lockMode & 0xff) << 16;
			else
				lockMode = 0;
			lockMode += TransactionController.MODE_RECORD;

			return lockMode;
		}

		/* if above don't apply, use optimizer's decision on heap's lock
		 */
		return getTrulyTheBestAccessPath().getLockMode();
	}

	/**
	 * Return whether or not the underlying ResultSet tree
	 * is ordered on the specified columns.
	 * RESOLVE - This method currently only considers the outermost table 
	 * of the query block.
	 * RESOLVE - We do not currently push method calls down, so we don't
	 * worry about whether the equals comparisons can be against a variant method.
	 *
	 * @param	crs					The specified ColumnReference[]
	 * @param	permuteOrdering		Whether or not the order of the CRs in the array can be permuted
     * @param   fbtHolder           List that is to be filled with the FromBaseTable
	 *
	 * @return	Whether the underlying ResultSet tree
	 * is ordered on the specified column.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    boolean isOrderedOn(ColumnReference[] crs, boolean permuteOrdering, List<FromBaseTable> fbtHolder)
				throws StandardException
	{
		/* The following conditions must be met, regardless of the value of permuteOrdering,
		 * in order for the table to be ordered on the specified columns:
		 *	o  Each column is from this table. (RESOLVE - handle joins later)
		 *	o  The access path for this table is an index.
		 */
		// Verify that all CRs are from this table
		for (int index = 0; index < crs.length; index++)
		{
			if (crs[index].getTableNumber() != tableNumber)
			{
				return false;
			}
		}
		// Verify access path is an index
		ConglomerateDescriptor cd = getTrulyTheBestAccessPath().getConglomerateDescriptor();
		if (! cd.isIndex())
		{
			return false;
		}

		// Now consider whether or not the CRs can be permuted
		boolean isOrdered;
		if (permuteOrdering)
		{
			isOrdered = isOrdered(crs, cd);
		}
		else
		{
			isOrdered = isStrictlyOrdered(crs, cd);
		}

        if (fbtHolder != null)
		{
            fbtHolder.add(this);
		}

		return isOrdered;
	}

	/**
	 * Turn off bulk fetch
	 */
	void disableBulkFetch()
	{
		bulkFetchTurnedOff = true;
		bulkFetch = UNSET;
	}

	/**
	 * Do a special scan for max.
	 */
	void doSpecialMaxScan()
	{
		if (SanityManager.DEBUG)
		{
			if ((restrictionList.size() != 0) || 
				(storeRestrictionList.size() != 0) ||
				(nonStoreRestrictionList.size() != 0))
			{
				SanityManager.THROWASSERT("shouldn't be setting max special scan because there is a restriction");
			}
		}
		specialMaxScan = true;
	}

	/**
	 * Is it possible to do a distinct scan on this ResultSet tree.
	 * (See SelectNode for the criteria.)
	 *
	 * @param distinctColumns the set of distinct columns
	 * @return Whether or not it is possible to do a distinct scan on this ResultSet tree.
	 */
    @Override
    boolean isPossibleDistinctScan(Set<BaseColumnNode> distinctColumns)
	{
		if ((restrictionList != null && restrictionList.size() != 0)) {
			return false;
		}

		HashSet<ValueNode> columns = new HashSet<ValueNode>();

        for (ResultColumn rc : getResultColumns()) {
			columns.add(rc.getExpression());
		}

		return columns.equals(distinctColumns);
	}

	/**
	 * Mark the underlying scan as a distinct scan.
	 */
    @Override
	void markForDistinctScan()
	{
		distinctScan = true;
	}


	/**
	 * @see ResultSetNode#adjustForSortElimination
	 */
    @Override
	void adjustForSortElimination()
	{
		/* NOTE: IRTBR will use a different method to tell us that
		 * it cannot do a bulk fetch as the ordering issues are
		 * specific to a FBT being under an IRTBR as opposed to a
		 * FBT being under a PRN, etc.
		 * So, we just ignore this call for now.
		 */
	}

	/**
	 * @see ResultSetNode#adjustForSortElimination
	 */
    @Override
	void adjustForSortElimination(RequiredRowOrdering rowOrdering)
		throws StandardException
	{
		/* We may have eliminated a sort with the assumption that
		 * the rows from this base table will naturally come back
		 * in the correct ORDER BY order. But in the case of IN
		 * list probing predicates (see DERBY-47) the predicate
		 * itself may affect the order of the rows.  In that case
		 * we need to notify the predicate so that it does the
		 * right thing--i.e. so that it preserves the natural
		 * ordering of the rows as expected from this base table.
		 * DERBY-3279.
		 */
		if (restrictionList != null)
			restrictionList.adjustForSortElimination(rowOrdering);
	}

	/**
	 * Return whether or not this index is ordered on a permutation of the specified columns.
	 *
	 * @param	crs		The specified ColumnReference[]
	 * @param	cd		The ConglomerateDescriptor for the chosen index.
	 *
	 * @return	Whether or not this index is ordered exactly on the specified columns.
	 *
	 * @exception StandardException		Thrown on error
	 */
	private boolean isOrdered(ColumnReference[] crs, ConglomerateDescriptor cd)
						throws StandardException
	{
		/* This table is ordered on a permutation of the specified columns if:
		 *  o  For each key column, until a match has been found for all of the
		 *	   ColumnReferences, it is either in the array of ColumnReferences
		 *	   or there is an equality predicate on it.
		 *	   (NOTE: It is okay to exhaust the key columns before the ColumnReferences
		 *	   if the index is unique.  In other words if we have CRs left over after
		 *	   matching all of the columns in the key then the table is considered ordered
		 *	   iff the index is unique. For example:
		 *		i1 on (c1, c2), unique
		 *		select distinct c3 from t1 where c1 = 1 and c2 = ?; 
		 *	   is ordered on c3 since there will be at most 1 qualifying row.)
		 */
		boolean[] matchedCRs = new boolean[crs.length];

		int nextKeyColumn = 0;
		int[] keyColumns = cd.getIndexDescriptor().baseColumnPositions();

		// Walk through the key columns
		for ( ; nextKeyColumn < keyColumns.length; nextKeyColumn++)
		{
			boolean currMatch = false;
			// See if the key column is in crs
			for (int nextCR = 0; nextCR < crs.length; nextCR++)
			{
				if (crs[nextCR].getColumnNumber() == keyColumns[nextKeyColumn])
				{
					matchedCRs[nextCR] = true;
					currMatch = true;
					break;
				}
			}

			// Advance to next key column if we found a match on this one
			if (currMatch)
			{
				continue;
			}

			// Stop search if there is no equality predicate on this key column
			if (! storeRestrictionList.hasOptimizableEqualityPredicate(this, keyColumns[nextKeyColumn], true))
			{
				break;
			}
		}

		/* Count the number of matched CRs. The table is ordered if we matched all of them. */
		int numCRsMatched = 0;
		for (int nextCR = 0; nextCR < matchedCRs.length; nextCR++)
		{
			if (matchedCRs[nextCR])
			{
				numCRsMatched++;
			}
		}

		if (numCRsMatched == matchedCRs.length)
		{
			return true;
		}

		/* We didn't match all of the CRs, but if
		 * we matched all of the key columns then
		 * we need to check if the index is unique.
		 */
		if (nextKeyColumn == keyColumns.length)
		{
			if (cd.getIndexDescriptor().isUnique())
			{
				return true;
			}
			else
			{
				return false;
			}
		}
		else
		{
			return false;
		}
	}

	/**
	 * Return whether or not this index is ordered on a permutation of the specified columns.
	 *
	 * @param	crs		The specified ColumnReference[]
	 * @param	cd		The ConglomerateDescriptor for the chosen index.
	 *
	 * @return	Whether or not this index is ordered exactly on the specified columns.
	 *
	 * @exception StandardException		Thrown on error
	 */
	private boolean isStrictlyOrdered(ColumnReference[] crs, ConglomerateDescriptor cd)
						throws StandardException
	{
		/* This table is ordered on the specified columns in the specified order if:
		 *  o  For each ColumnReference, it is either the next key column or there
		 *	   is an equality predicate on all key columns prior to the ColumnReference.
		 *	   (NOTE: If the index is unique, then it is okay to have a suffix of
		 *	   unmatched ColumnReferences because the set is known to be ordered. For example:
		 *		i1 on (c1, c2), unique
		 *		select distinct c3 from t1 where c1 = 1 and c2 = ?; 
		 *	   is ordered on c3 since there will be at most 1 qualifying row.)
		 */
		int nextCR = 0;
		int nextKeyColumn = 0;
		int[] keyColumns = cd.getIndexDescriptor().baseColumnPositions();

		// Walk through the CRs
		for ( ; nextCR < crs.length; nextCR++)
		{
			/* If we've walked through all of the key columns then
			 * we need to check if the index is unique.
			 * Beetle 4402
			 */
			if (nextKeyColumn == keyColumns.length)
			{
				if (cd.getIndexDescriptor().isUnique())
				{
					break;
				}
				else
				{
					return false;
				}
			}
			if (crs[nextCR].getColumnNumber() == keyColumns[nextKeyColumn])
			{
				nextKeyColumn++;
				continue;
			}
			else 
			{
				while (crs[nextCR].getColumnNumber() != keyColumns[nextKeyColumn])
				{
					// Stop if there is no equality predicate on this key column
					if (! storeRestrictionList.hasOptimizableEqualityPredicate(this, keyColumns[nextKeyColumn], true))
					{
						return false;
					}

					// Advance to the next key column
					nextKeyColumn++;

					/* If we've walked through all of the key columns then
					 * we need to check if the index is unique.
					 */
					if (nextKeyColumn == keyColumns.length)
					{
						if (cd.getIndexDescriptor().isUnique())
						{
							break;
						}
						else
						{
							return false;
						}
					}
				}
			}
		}
		return true;
	}

	/**
	 * Is this a one-row result set with the given conglomerate descriptor?
	 */
	private boolean isOneRowResultSet(ConglomerateDescriptor cd,
									OptimizablePredicateList predList)
		throws StandardException
	{
		if (predList == null)
		{
			return false;
		}

		if (SanityManager.DEBUG)
		{
			if (! (predList instanceof PredicateList))
			{
				SanityManager.THROWASSERT(
					"predList should be a PredicateList, but is a " +
					predList.getClass().getName()
				);
			}
		}

        PredicateList restrictList = (PredicateList) predList;

		if (! cd.isIndex())
		{
			return false;
		}

		IndexRowGenerator irg =
			cd.getIndexDescriptor();

		// is this a unique index
		if (! irg.isUnique())
		{
			return false;
		}

		int[] baseColumnPositions = irg.baseColumnPositions();

		// Do we have an exact match on the full key
		for (int index = 0; index < baseColumnPositions.length; index++)
		{
			// get the column number at this position
			int curCol = baseColumnPositions[index];

			/* Is there a pushable equality predicate on this key column?
			 * (IS NULL is also acceptable)
			 */
            if (! restrictList.hasOptimizableEqualityPredicate(
                        this, curCol, true))
			{
				return false;
			}

		}

		return true;
	}

	private int getDefaultBulkFetch()
		throws StandardException
	{
		int valInt;
		String valStr = PropertyUtil.getServiceProperty(
						  getLanguageConnectionContext().getTransactionCompile(),
						  LanguageProperties.BULK_FETCH_PROP,
						  LanguageProperties.BULK_FETCH_DEFAULT);
							
		valInt = getIntProperty(valStr, LanguageProperties.BULK_FETCH_PROP);

		// verify that the specified value is valid
		if (valInt <= 0)
		{
			throw StandardException.newException(SQLState.LANG_INVALID_BULK_FETCH_VALUE, 
					String.valueOf(valInt));
		}

		/*
		** If the value is <= 1, then reset it
		** to UNSET -- this is how customers can
		** override the bulkFetch default to turn
		** it off.
		*/
		return (valInt <= 1) ?
			UNSET : valInt;
	}

	private String getUserSpecifiedIndexName()
	{
		String retval = null;

		if (tableProperties != null)
		{
			retval = tableProperties.getProperty("index");
		}

		return retval;
	}

	/*
	** RESOLVE: This whole thing should probably be moved somewhere else,
	** like the optimizer or the data dictionary.
	*/
	private StoreCostController getStoreCostController(
										ConglomerateDescriptor cd)
			throws StandardException
	{
		return getCompilerContext().getStoreCostController(cd.getConglomerateNumber());
	}

	private StoreCostController getBaseCostController()
			throws StandardException
	{
		return getStoreCostController(baseConglomerateDescriptor);
	}

	private boolean gotRowCount = false;
	private long rowCount = 0;
	private long baseRowCount() throws StandardException
	{
		if (! gotRowCount)
		{
			StoreCostController scc = getBaseCostController();
			rowCount = scc.getEstimatedRowCount();
			gotRowCount = true;
		}

		return rowCount;
	}

	private DataValueDescriptor[] getRowTemplate(
    ConglomerateDescriptor  cd,
    StoreCostController     scc)
			throws StandardException
	{
		/*
		** If it's for a heap scan, just get all the columns in the
		** table.
		*/
		if (! cd.isIndex())
			return templateColumns.buildEmptyRow().getRowArray();

		/* It's an index scan, so get all the columns in the index */
		ExecRow emptyIndexRow = templateColumns.buildEmptyIndexRow(
														tableDescriptor,
														cd,
														scc,
														getDataDictionary());

		return emptyIndexRow.getRowArray();
	}

	private ConglomerateDescriptor getFirstConglom()
		throws StandardException
	{
		getConglomDescs();
		return conglomDescs[0];
	}

	private ConglomerateDescriptor getNextConglom(ConglomerateDescriptor currCD)
	{
		int index = 0;

		for ( ; index < conglomDescs.length; index++)
		{
			if (currCD == conglomDescs[index])
			{
				break;
			}
		}

		if (index < conglomDescs.length - 1)
		{
			return conglomDescs[index + 1];
		}
		else
		{
			return null;
		}
	}

	private void getConglomDescs()
		throws StandardException
	{
		if (conglomDescs == null)
		{
			conglomDescs = tableDescriptor.getConglomerateDescriptors();
		}
	}


	/**
	 * set the Information gathered from the parent table that is 
     * required to perform a referential action on dependent table.
	 */
    @Override
    void setRefActionInfo(long fkIndexConglomId,
								 int[]fkColArray, 
								 String parentResultSetId,
								 boolean dependentScan)
	{


		this.fkIndexConglomId = fkIndexConglomId;
		this.fkColArray = fkColArray;
		this.raParentResultSetId = parentResultSetId;
		this.raDependentScan = dependentScan;
	}

	/**
	 * Accept the visitor for all visitable children of this node.
	 * 
	 * @param v the visitor
	 *
	 * @exception StandardException on error
	 */
    @Override
	void acceptChildren(Visitor v)
	
		throws StandardException
	{
		super.acceptChildren(v);

		if (nonStoreRestrictionList != null) {
			nonStoreRestrictionList.accept(v);
		}
		
		if (restrictionList != null) {
			restrictionList.accept(v);
		}

		if (nonBaseTableRestrictionList != null) {
			nonBaseTableRestrictionList.accept(v);
		}

		if (requalificationRestrictionList != null) {
			requalificationRestrictionList.accept(v);
		}

        if (tableName != null) {
            tableName = (TableName) tableName.accept(v);
        }
	}

    /**
     * Tells if the given table qualifies for a statistics update check in the
     * current configuration.
     *
     * @param td the table to check
     * @return {@code true} if qualified, {@code false} if not
     */
    private boolean qualifiesForStatisticsUpdateCheck(TableDescriptor td)
            throws StandardException {
        int qualifiedIndexes = 0;
        // Only base tables qualifies.
        if (td.getTableType() == TableDescriptor.BASE_TABLE_TYPE) {
            IndexStatisticsDaemonImpl istatDaemon = (IndexStatisticsDaemonImpl)
                    getDataDictionary().getIndexStatsRefresher(false);
            // Usually only tables with at least one non-unique index or
            // multi-column unique indexes qualify, but soft-upgrade mode is a
            // special case (as is the temporary user override available).
            // TODO: Rewrite if-logic when the temporary override is removed.
            if (istatDaemon == null) { // Read-only database
                qualifiedIndexes = 0;
            } else if (istatDaemon.skipDisposableStats) {
                qualifiedIndexes = td.getQualifiedNumberOfIndexes(2, true);
            } else {
                qualifiedIndexes = td.getTotalNumberOfIndexes();
            }
        }
        return (qualifiedIndexes > 0);
    }
}
