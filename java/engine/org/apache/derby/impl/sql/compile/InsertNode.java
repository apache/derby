/*

   Derby - Class org.apache.derby.impl.sql.compile.InsertNode

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

import java.util.Properties;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.StatementType;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.IgnoreFilter;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.conn.Authorizer;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptorList;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.IndexLister;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecRowBuilder;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.util.StringUtil;
import org.apache.derby.impl.sql.execute.FKInfo;
import org.apache.derby.vti.DeferModification;

/**
 * An InsertNode is the top node in a query tree for an
 * insert statement.
 * <p>
 * After parsing, the node contains
 *   targetTableName: the target table for the insert
 *   collist: a list of column names, if specified
 *   queryexpr: the expression being inserted, either
 *				a values clause or a select form; both
 *			    of these are represented via the SelectNode,
 *				potentially with a TableOperatorNode such as
 *				UnionNode above it.
 * <p>
 * After binding, the node has had the target table's
 * descriptor located and inserted, and the queryexpr
 * and collist have been massaged so that they are identical
 * to the table layout.  This involves adding any default
 * values for missing columns, and reordering the columns
 * to match the table's ordering of them.
 * <p>
 * After optimizing, ...
 */
public final class InsertNode extends DMLModStatementNode
{
    private     ResultColumnList    targetColumnList;
    private     boolean             deferred;
	public		ValueNode			checkConstraints;
	public		Properties			targetProperties;
	public		FKInfo				fkInfo;
	protected	boolean				bulkInsert;
	private 	boolean				bulkInsertReplace;
	private     OrderByList         orderByList;
    private     ValueNode           offset;
    private     ValueNode           fetchFirst;
    private     boolean           hasJDBClimitClause; // true if using JDBC limit/offset escape syntax

	protected   RowLocation[] 		autoincRowLocation;
	/**
     * Constructor for an InsertNode.
	 *
     * @param targetName         The name of the table/VTI to insert into
     * @param insertColumns      A ResultColumnList with the names of the
	 *			columns to insert into.  May be null if the
	 *			user did not specify the columns - in this
	 *			case, the binding phase will have to figure
	 *			it out.
     * @param queryExpression    The query expression that will generate
     *                           the rows to insert into the given table
     * @param matchingClause   Non-null if this DML is part of a MATCHED clause of a MERGE statement.
     * @param targetProperties   The properties specified on the target table
     * @param orderByList        The order by list for the source result set,
     *                           null if no order by list
     * @param offset             The value of a <result offset clause> if
     *                           present
     * @param fetchFirst         The value of a <fetch first clause> if present
     * @param hasJDBClimitClause True if the offset/fetchFirst clauses come
     *                           from JDBC limit/offset escape syntax
     * @param cm                 The context manager
	 */
    InsertNode(
            QueryTreeNode    targetName,
            ResultColumnList insertColumns,
            ResultSetNode    queryExpression,
            MatchingClauseNode matchingClause,
            Properties       targetProperties,
            OrderByList      orderByList,
            ValueNode        offset,
            ValueNode        fetchFirst,
            boolean          hasJDBClimitClause,
            ContextManager   cm)
	{
		/* statementType gets set in super() before we've validated
		 * any properties, so we've kludged the code to get the
		 * right statementType for a bulk insert replace.
		 */
        super(queryExpression, matchingClause, getStatementType(targetProperties), cm);
        setTarget(targetName);
        targetColumnList = insertColumns;
        this.targetProperties = targetProperties;
        this.orderByList = orderByList;
        this.offset = offset;
        this.fetchFirst = fetchFirst;
        this.hasJDBClimitClause = hasJDBClimitClause;

		/* Remember that the query expression is the source to an INSERT */
		getResultSetNode().setInsertSource();
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
            try {
                return ( (targetTableName!=null) ? targetTableName : targetVTI.getTableName() ).toString() + "\n"
                    + targetProperties + "\n"
                    + super.toString();
            } catch (org.apache.derby.iapi.error.StandardException e) {
                return "tableName: <not_known>\n"
                    + targetProperties + "\n"
                    + super.toString();
            }
		}
		else
		{
			return "";
		}
	}

    @Override
    String statementToString()
	{
		return "INSERT";
	}

	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 */
    @Override
    void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG)
		{
			super.printSubNodes(depth);

			if (targetTableName != null)
			{
				printLabel(depth, "targetTableName: ");
				targetTableName.treePrint(depth + 1);
			}

			if (targetColumnList != null)
			{
				printLabel(depth, "targetColumnList: ");
				targetColumnList.treePrint(depth + 1);
			}

			if (orderByList != null) {
				printLabel(depth, "orderByList: ");
				orderByList.treePrint(depth + 1);
			}

            if (offset != null) {
                printLabel(depth, "offset:");
                offset.treePrint(depth + 1);
            }

            if (fetchFirst != null) {
                printLabel(depth, "fetch first/next:");
                fetchFirst.treePrint(depth + 1);
            }

			/* RESOLVE - need to print out targetTableDescriptor */
		}
	}

	/**
	 * Bind this InsertNode.  This means looking up tables and columns and
	 * getting their types, and figuring out the result types of all
	 * expressions, as well as doing view resolution, permissions checking,
	 * etc.
	 * <p>
	 * Binding an insert will also massage the tree so that
	 * the collist and select column order/number are the
	 * same as the layout of the table in the store. 
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public void bindStatement() throws StandardException
	{
		// We just need select privilege on the expressions
		getCompilerContext().pushCurrentPrivType( Authorizer.SELECT_PRIV);

        FromList fromList = new FromList(
                getOptimizerFactory().doJoinOrderOptimization(),
                getContextManager());

		/* If any underlying ResultSetNode is a SelectNode, then we
		 * need to do a full bind(), including the expressions
		 * (since the fromList may include a FromSubquery).
		 */
        DataDictionary dataDictionary = getDataDictionary();
		super.bindResultSetsWithTables(dataDictionary);

		/*
		** Get the TableDescriptor for the table we are inserting into
		*/
		verifyTargetTable();

		// Check the validity of the targetProperties, if they exist
		if (targetProperties != null)
		{
			verifyTargetProperties(dataDictionary);
		}

		/*
		** Get the resultColumnList representing the columns in the base
		** table or VTI. We don't bother adding any permission checks here
        ** because they are assumed by INSERT permission on the table.
		*/
        IgnoreFilter    ignorePermissions = new IgnoreFilter();
        getCompilerContext().addPrivilegeFilter( ignorePermissions );
		getResultColumnList();
        getCompilerContext().removePrivilegeFilter( ignorePermissions );

		/* If we have a target column list, then it must have the same # of
		 * entries as the result set's RCL.
		 */
		if (targetColumnList != null)
		{
			/*
			 * Normalize synonym qualifers for column references.
			 */
			if (synonymTableName != null)
			{
				normalizeSynonymColumns ( targetColumnList, targetTableName );
			}
			
			/* Bind the target column list */
			getCompilerContext().pushCurrentPrivType( getPrivType());
			if (targetTableDescriptor != null)
			{
				targetColumnList.bindResultColumnsByName(targetTableDescriptor,
														(DMLStatementNode) this);
			}
			else
			{
				targetColumnList.bindResultColumnsByName(targetVTI.getResultColumns(), targetVTI,
														this);
			}	
			getCompilerContext().popCurrentPrivType();
        }

		/* Verify that all underlying ResultSets reclaimed their FromList */
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(fromList.size() == 0,
				"fromList.size() is expected to be 0, not " + 
				fromList.size() +
				" on return from RS.bindExpressions()");
		}

        /* Replace any DEFAULTs with the associated tree, or flag DEFAULTs if
         * not allowed (inside top level set operator nodes). Subqueries are
         * checked for illegal DEFAULTs elsewhere.
         */
        boolean isTableConstructor =
            (resultSet instanceof UnionNode &&
             ((UnionNode)resultSet).tableConstructor()) ||
            resultSet instanceof RowResultSetNode;

        //
        // For the MERGE statement, DEFAULT expressions in the SELECT node
        // may have been replaced with generated expressions already.
        //
        ResultColumnList    tempRCL = resultSet.getResultColumns();
        boolean defaultsWereReplaced = false;
        for ( int i = 0; i < tempRCL.size(); i++ )
        {
            ResultColumn    rc = tempRCL.getResultColumn( i+1 );
            if ( rc.wasDefaultColumn() ) { defaultsWereReplaced = true; }
        }

        resultSet.replaceOrForbidDefaults(targetTableDescriptor,
                                          targetColumnList,
                                          isTableConstructor);

		/* Bind the expressions now that the result columns are bound 
		 * NOTE: This will be the 2nd time for those underlying ResultSets
		 * that have tables (no harm done), but it is necessary for those
		 * that do not have tables.  It's too hard/not work the effort to
		 * avoid the redundancy.
		 */
		super.bindExpressions();

        //
        // At this point, we have added permissions checks for the driving query.
        // Now add a check for INSERT privilege on the target table.
        //
        if (isPrivilegeCollectionRequired())
        {
            getCompilerContext().pushCurrentPrivType( getPrivType());
            getCompilerContext().addRequiredTablePriv( targetTableDescriptor );
            getCompilerContext().popCurrentPrivType();
        }

        // Now stop adding permissions checks.
        getCompilerContext().addPrivilegeFilter( ignorePermissions );

		/*
		** If the result set is a union, it could be a table constructor.
		** Bind any nulls in the result columns of the table constructor
		** to the types of the table being inserted into.
		**
		** The types of ? parameters in row constructors and table constructors
		** in an INSERT statement come from the result columns.
		**
		** If there is a target column list, use that instead of the result
		** columns for the whole table, since the columns in the result set
		** correspond to the target column list.
		*/
		if (targetColumnList != null)
		{
			if (resultSet.getResultColumns().visibleSize() > targetColumnList.size())
				throw StandardException.newException(SQLState.LANG_DB2_INVALID_COLS_SPECIFIED); 
			resultSet.bindUntypedNullsToResultColumns(targetColumnList);
			resultSet.setTableConstructorTypes(targetColumnList);
		}
		else
		{
			if (resultSet.getResultColumns().visibleSize() > resultColumnList.size())
				throw StandardException.newException(SQLState.LANG_DB2_INVALID_COLS_SPECIFIED); 
			resultSet.bindUntypedNullsToResultColumns(resultColumnList);
			resultSet.setTableConstructorTypes(resultColumnList);
		}

		/* Bind the columns of the result set to their expressions */
		resultSet.bindResultColumns(fromList);

		int resCols = resultSet.getResultColumns().visibleSize();
		DataDictionary dd = getDataDictionary();
		if (targetColumnList != null)
		{
			if (targetColumnList.size() != resCols)
				throw StandardException.newException(SQLState.LANG_DB2_INVALID_COLS_SPECIFIED); 
		}
		else 
		{
			if (targetTableDescriptor != null &&
						targetTableDescriptor.getNumberOfColumns() != resCols)
				throw StandardException.newException(SQLState.LANG_DB2_INVALID_COLS_SPECIFIED); 
		}

		/* See if the ResultSet's RCL needs to be ordered to match the target
		 * list, or "enhanced" to accommodate defaults.  It can only need to
		 * be ordered if there is a target column list.  It needs to be
		 * enhanced if there are fewer source columns than there are columns
		 * in the table.
		 */
		boolean inOrder = true;
		int numTableColumns = resultColumnList.size();

		/* colMap[] will be the size of the target list, which could be larger
		 * than the current size of the source list.  In that case, the source
		 * list will be "enhanced" to include defaults.
		 */
		int[] colMap = new int[numTableColumns];

		// set the fields to an unused value
		for (int i = 0; i < colMap.length; i++) 
		{
			colMap[i] = -1;
		}

		/* Create the source/target list mapping */
		if (targetColumnList != null)
		{
			/*
			** There is a target column list, so the result columns might
			** need to be ordered.  Step through the target column list
			** and remember the position in the target table of each column.
			** Remember if any of the columns are out of order.
			*/
			int targetSize = targetColumnList.size();
			for (int index = 0; index < targetSize; index++)
			{
                int position = targetColumnList.elementAt(index).
												columnDescriptor.getPosition();

				if (index != position-1)
				{
					inOrder = false;
				}

				// position is 1-base; colMap indexes and entries are 0-based.
				colMap[position-1] = index;
			}
		}
		else
		{
			/*
			** There is no target column list, so the result columns in the
			** source are presumed to be in the same order as the target
			** table.
			*/
			for (int position = 0;
				position < resultSet.getResultColumns().visibleSize();
				position++)
			{
				colMap[position] = position;
			}
		}

		// Bind the ORDER BY columns
		if (orderByList != null)
		{
			orderByList.pullUpOrderByColumns(resultSet);

			// The select list may have new columns now, make sure to bind
			// those.
			super.bindExpressions();

			orderByList.bindOrderByColumns(resultSet);
		}

        bindOffsetFetch(offset, fetchFirst);

		resultSet = enhanceAndCheckForAutoincrement( resultSet, inOrder, colMap, defaultsWereReplaced );

		resultColumnList.checkStorableExpressions(resultSet.getResultColumns());
		/* Insert a NormalizeResultSetNode above the source if the source
		 * and target column types and lengths do not match.
 		 */
		if (! resultColumnList.columnTypesAndLengthsMatch(
												resultSet.getResultColumns()))
		{
            
            resultSet = new NormalizeResultSetNode(
                resultSet, resultColumnList, null, false, getContextManager());
		}

		if (targetTableDescriptor != null)
		{
			ResultColumnList sourceRCL = resultSet.getResultColumns();
			sourceRCL.copyResultColumnNames(resultColumnList);

            /* bind all generation clauses for generated columns */
            parseAndBindGenerationClauses
                ( dataDictionary, targetTableDescriptor, sourceRCL, resultColumnList, false, null );
            
			/* Get and bind all constraints on the table */
			checkConstraints = bindConstraints(dataDictionary,
                                                getOptimizerFactory(),
												targetTableDescriptor,
												null,
												sourceRCL,
												(int[]) null,
												(FormatableBitSet) null,
											    true);  /* we always include
														 * triggers in core language */
	
			/* Do we need to do a deferred mode insert */
			/* 
		 	** Deferred if:
			**	If the target table is also a source table
			**	Self-referencing foreign key constraint 
			**	trigger
			*/
			if (resultSet.referencesTarget(
									targetTableDescriptor.getName(), true) ||
				 requiresDeferredProcessing())
			{
				deferred = true;

				/* Disallow bulk insert replace when target table
				 * is also a source table.
				 */
				if (bulkInsertReplace &&
					resultSet.referencesTarget(
									targetTableDescriptor.getName(), true))
				{
					throw StandardException.newException(SQLState.LANG_INVALID_BULK_INSERT_REPLACE, 
									targetTableDescriptor.getQualifiedName());
				}
			}

			/* Get the list of indexes on the table being inserted into */
			getAffectedIndexes(targetTableDescriptor);
			TransactionController tc = 
				getLanguageConnectionContext().getTransactionCompile();

			autoincRowLocation = 
				dd.computeAutoincRowLocations(tc, targetTableDescriptor);
		}
		else
		{
            deferred = VTIDeferModPolicy.deferIt( DeferModification.INSERT_STATEMENT,
                                                  targetVTI,
                                                  null,
                                                  resultSet);
		}
        
		getCompilerContext().popCurrentPrivType();
	}

	/**
	 * Process ResultSet column lists for projection and autoincrement.
	 *
	 * This method recursively descends the result set node tree. When
	 * it finds a simple result set, it processes any autoincrement
	 * columns in that rs by calling checkAutoIncrement. When it finds
	 * a compound result set, like a Union or a PRN, it recursively
	 * descends to the child(ren) nodes. Union nodes can arise due to
	 * multi-rows in VALUES clause), PRN nodes can arise when the set
	 * of columns being inserted is a subset of the set of columns in 
	 * the table.
	 *
	 * In addition to checking for autoincrement columns in the result set,
	 * we may need to enhance and re-order the column list to match the
	 * column list of the table we are inserting into. This work is handled
	 * by ResultsetNode.enhanceRCLForInsert.
	 *
	 * Note that, at the leaf level, we need to enhance the RCL first, then
	 * check for autoincrement columns. At the non-leaf levels, we have
	 * to enhance the RCL, but we don't have to check for autoincrement
	 * columns, since they only occur at the leaf level.
	 *
	 * This way, all ColumnDescriptor of all rows will be set properly.
	 *
	 * @param resultSet			current node in the result set tree
	 * @param inOrder			FALSE if the column list needs reordering
	 * @param colMap            correspondence between RCLs
	 * @param defaultsWereReplaced  true if DEFAULT clauses were replaced with generated expressions
	 * @return a node representing the source for the insert
	 *
	 * @exception StandardException Thrown on error
	 */
	ResultSetNode enhanceAndCheckForAutoincrement
        (
         ResultSetNode resultSet,
         boolean inOrder,
         int[] colMap,
         boolean    defaultsWereReplaced
         )
		throws StandardException
	{
		/*
		 * Some implementation notes:
		 * 
		 * colmap[x] == y means that column x in the target table
		 * maps to column y in the source result set.
		 * colmap[x] == -1 means that column x in the target table
		 * maps to its default value.
		 * both colmap indexes and values are 0-based.
		 *
		 * if the list is in order and complete, we don't have to change
		 * the tree. If it is not, then we call RSN.enhanceRCLForInsert() 
		 * which will reorder ("enhance") the source RCL within the same RSN)
		 *
		 * one thing we do know is that all of the resultsets underneath
		 * us have their resultColumn names filled in with the names of
		 * the target table columns.  That makes generating the mapping
		 * "easier" -- we simply generate the names of the target table columns
		 * that are included.  For the missing columns, we generate default
		 * value expressions.
		 */

		resultSet = resultSet.enhanceRCLForInsert(this, inOrder, colMap);

		// Forbid overrides for generated columns and identity columns that
		// are defined as GENERATED ALWAYS.
		if ((resultSet instanceof UnionNode) &&
				((UnionNode) resultSet).tableConstructor()) {
			// If this is a multi-row table constructor, we are not really
			// interested in the result column list of the top-level UnionNode.
			// The interesting RCLs are those of the RowResultSetNode children
			// of the UnionNode, and they have already been checked from
			// UnionNode.enhanceRCLForInsert(). Since the RCL of the UnionNode
			// doesn't tell whether or not DEFAULT is specified at the leaf
			// level, we need to skip it here to avoid false positives.
		} else {
			resultColumnList.forbidOverrides( resultSet.getResultColumns(), defaultsWereReplaced );
		}

		return resultSet;
	}

    @Override
	int getPrivType()
	{
		return Authorizer.INSERT_PRIV;
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
		boolean returnValue = false;

		//If this node references a SESSION schema table, then return true. 
		if (targetTableDescriptor != null)
			returnValue = isSessionSchema(targetTableDescriptor.getSchemaDescriptor());

		if (returnValue == false)
			returnValue = resultSet.referencesSessionSchema();

		return returnValue;
	}

	/**
	 * Verify that the target properties that we are interested in
	 * all hold valid values.
	 * NOTE: Any target property which is valid but cannot be supported
	 * due to a target database, etc. will be turned off quietly.
	 *
	 * @param dd	The DataDictionary
	 *
	 * @exception StandardException		Thrown on error
	 */
	private void verifyTargetProperties(DataDictionary dd)
		throws StandardException
	{
		// The only property that we're currently interested in is insertMode
		String insertMode = targetProperties.getProperty("insertMode");
		if (insertMode != null)
		{
			String upperValue = StringUtil.SQLToUpperCase(insertMode);
			if (! upperValue.equals("BULKINSERT") &&
				! upperValue.equals("REPLACE"))
			{
				throw StandardException.newException(SQLState.LANG_INVALID_INSERT_MODE, 
								insertMode,
								targetTableName);
			}
			else
			{
				/* Turn off bulkInsert if it is on and we can't support it. */
				if (! verifyBulkInsert(dd, upperValue))
				{
					targetProperties.remove("insertMode");
				}
				else
				{
					/* Now we know we're doing bulk insert */
					bulkInsert = true;

					if (upperValue.equals("REPLACE"))
					{
						bulkInsertReplace = true;
					}

					// Validate the bulkFetch property if specified
					String bulkFetchStr = targetProperties.getProperty("bulkFetch");
					if (bulkFetchStr != null)
					{
						int bulkFetch = getIntProperty(bulkFetchStr, "bulkFetch");

						// verify that the specified value is valid
						if (bulkFetch <= 0)
						{
							throw StandardException.newException(SQLState.LANG_INVALID_BULK_FETCH_VALUE,
									String.valueOf(bulkFetch));
						}
					}
				}
			}
		}
	}

	/**
	 * Do the bind time checks to see if bulkInsert is allowed on
	 * this table.  bulkInsert is disallowed at bind time for:
	 *		o  target databases
	 *		o  (tables with triggers?)
	 * (It is disallowed at execution time if the table has at
	 * least 1 row in it or if it is a deferred mode insert.)
	 *
	 * @param dd	The DataDictionary
	 * @param mode	The insert mode
	 *
	 * @return Whether or not bulkInsert is allowed.
	 *
	 * @exception StandardException		Thrown on error
	 */
	private boolean verifyBulkInsert(DataDictionary dd, String mode)
		throws StandardException
	{
		return true;
	}

	/**
	 * Compile constants that Execution will use
	 *
	 * @exception StandardException		Thrown on failure
	 */
    @Override
    public ConstantAction makeConstantAction() throws StandardException
	{

		/* Different constant actions for base tables and updatable VTIs */
		if (targetTableDescriptor != null)
		{
			// Base table

			long heapConglomId = targetTableDescriptor.getHeapConglomerateId();
			TransactionController tc = 
				getLanguageConnectionContext().getTransactionCompile();
			int numIndexes = (targetTableDescriptor != null) ?
								indexConglomerateNumbers.length : 0;
			StaticCompiledOpenConglomInfo[] indexSCOCIs = 
				new StaticCompiledOpenConglomInfo[numIndexes];

			for (int index = 0; index < numIndexes; index++)
			{
				indexSCOCIs[index] = tc.getStaticCompiledConglomInfo(indexConglomerateNumbers[index]);
			}

			/*
			** If we're doing bulk insert, do table locking regardless of
			** what the optimizer decided.  This is because bulk insert is
			** generally done with a large number of rows into an empty table.
			** We also do table locking if the table's lock granularity is
			** set to table.
			*/
			if (bulkInsert ||
				targetTableDescriptor.getLockGranularity() == TableDescriptor.TABLE_LOCK_GRANULARITY)
			{
				lockMode = TransactionController.MODE_TABLE;
			}

			return	getGenericConstantActionFactory().getInsertConstantAction
				( targetTableDescriptor,
				  heapConglomId,
				  tc.getStaticCompiledConglomInfo(heapConglomId),
				  indicesToMaintain,
				  indexConglomerateNumbers,
				  indexSCOCIs,
				  indexNames,
				  deferred,
				  false,
				  targetTableDescriptor.getUUID(),
				  lockMode,
				  null, null, 
				  targetProperties,
				  getFKInfo(),
				  getTriggerInfo(),
				  resultColumnList.getStreamStorableColIds(targetTableDescriptor.getNumberOfColumns()),
				  getIndexedCols(),
				  (UUID) null,
				  null,
				  null,
				  resultSet.isOneRowResultSet(), 
				  autoincRowLocation,
				  inMatchingClause()
				  );
		}
		else
		{
			/* Return constant action for VTI
			 * NOTE: ConstantAction responsible for preserving instantiated
			 * VTIs for in-memory queries and for only preserving VTIs
			 * that implement Serializable for SPSs.
			 */
			return	getGenericConstantActionFactory().getUpdatableVTIConstantAction( DeferModification.INSERT_STATEMENT,
						deferred);
		}
	}

	/**
	 * Create a boolean[] to track the (0-based) columns which are indexed.
	 *
	 * @return A boolean[] to track the (0-based) columns which are indexed.
	 *
	 * @exception StandardException		Thrown on failure
	 */
    boolean[] getIndexedCols() throws StandardException
	{
		/* Create a boolean[] to track the (0-based) columns which are indexed */
		boolean[] indexedCols = new boolean[targetTableDescriptor.getNumberOfColumns()];
		for (int index = 0; index < indicesToMaintain.length; index++)
		{
			int[] colIds = indicesToMaintain[index].getIndexDescriptor().baseColumnPositions();

			for (int index2 = 0; index2 < colIds.length; index2++)
			{
				indexedCols[colIds[index2] - 1] = true;
			}
		}

		return indexedCols;
	}

	/**
     * {@inheritDoc}
     * <p>
     * Remove any duplicate ORDER BY columns and push an ORDER BY if present
     * down to the source result set, before calling super.optimizeStatement.
     * </p>
	 */
    @Override
	public void optimizeStatement() throws StandardException
	{
        resultSet.pushQueryExpressionSuffix();
		// Push the order by list down to the ResultSet
		if (orderByList != null)
		{
			// If we have more than 1 ORDERBY columns, we may be able to
			// remove duplicate columns, e.g., "ORDER BY 1, 1, 2".
			if (orderByList.size() > 1)
			{
				orderByList.removeDupColumns();
			}

			resultSet.pushOrderByList(orderByList);

			orderByList = null;
		}

        resultSet.pushOffsetFetchFirst( offset, fetchFirst, hasJDBClimitClause );

		super.optimizeStatement();
        
        //
        // If the insert stream involves a table function, attempt the bulk-insert
        // optimization. See https://issues.apache.org/jira/browse/DERBY-4789
        // We perform this check after optimization because the table function may be
        // wrapped in a view, which is only expanded at optimization time.
        //
        HasTableFunctionVisitor tableFunctionVisitor = new HasTableFunctionVisitor();
        this.accept( tableFunctionVisitor );
        // DERBY-5614: See if the target is a global temporary table (GTT),
        // in which case we don't support bulk insert.
        if ( tableFunctionVisitor.hasNode() &&
                !isSessionSchema(targetTableDescriptor.getSchemaDescriptor())) {
            requestBulkInsert();
        }
    }

    /**
     * Request bulk insert optimization at run time.
     */
    private void requestBulkInsert()
    {
        if ( targetProperties == null ) { targetProperties = new Properties(); }

        // Set bulkInsert if insertMode not already set. For the import procedures,
        // the insertMode property may be set already
        String key = "insertMode";
        String value = "bulkInsert";

        if ( targetProperties.getProperty( key ) == null )
        { targetProperties.put( key, value ); }

        bulkInsert = true;
    }

	/**
	 * Code generation for insert
	 * creates an expression for:
	 *   ResultSetFactory.getInsertResultSet(resultSet.generate(ps), generationClausesResult, checkConstrainResult, this )
	 *
	 * @param acb	The ActivationClassBuilder for the class being built
	 * @param mb the method  for the execute() method to be built
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    void generate(ActivationClassBuilder acb, MethodBuilder mb)
							throws StandardException
	{
		// If the DML is on the temporary table, generate the code to
		// mark temporary table as modified in the current UOW. After
		// DERBY-827 this must be done in execute() since
		// createResultSet() will only be called once.
		generateCodeForTemporaryTable(acb);

		/* generate the parameters */
		generateParameterValueSet(acb);
		// Base table
		if (targetTableDescriptor != null)
		{
			/*
			** Generate the insert result set, giving it either the original
			** source or the normalize result set, the constant action,
			** and "this".
			*/

			acb.pushGetResultSetFactoryExpression(mb);

			// arg 1
            if ( inMatchingClause() )
            {
                matchingClause.generateResultSetField( acb, mb );
            }
            else
            {
                resultSet.generate( acb, mb );
            }

			// arg 2 generate code to evaluate generation clauses
			generateGenerationClauses( resultColumnList, resultSet.getResultSetNumber(), false, acb, mb );

			// arg 3 generate code to evaluate CHECK CONSTRAINTS
			generateCheckConstraints( checkConstraints, acb, mb );

            // arg 4 row template used by bulk insert
            if (bulkInsert) {
                ColumnDescriptorList cdl =
                        targetTableDescriptor.getColumnDescriptorList();
                ExecRowBuilder builder =
                        new ExecRowBuilder(cdl.size(), false);
                for (int i = 0; i < cdl.size(); i++) {
                    ColumnDescriptor cd = cdl.get(i);
                    builder.setColumn(i + 1, cd.getType());
                }
                mb.push(acb.addItem(builder));
            } else {
                mb.push(-1);
            }

            mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null,
                    "getInsertResultSet", ClassName.ResultSet, 4);
		}
		else
		{
			/* Generate code for the VTI
			 * NOTE: we need to create a dummy cost estimate for the
			 * targetVTI since we never optimized it.
			 * RESOLVEVTI - we will have to optimize it in order to 
			 * push predicates into the VTI.
			 */
			targetVTI.assignCostEstimate(resultSet.getNewCostEstimate());

			/*
			** Generate the insert VTI result set, giving it either the original
			** source or the normalize result set, the constant action,
			*/
			acb.pushGetResultSetFactoryExpression(mb);

			// arg 1
			resultSet.generate(acb, mb);

			// arg 2
			targetVTI.generate(acb, mb);

			mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getInsertVTIResultSet", ClassName.ResultSet, 2);
		}
	}

	/**
	 * Return the type of statement, something from
	 * StatementType.
	 *
	 * @return the type of statement
	 */
    @Override
	protected final int getStatementType()
	{
		return StatementType.INSERT;
	}

	/**
	 * Return the statement type, where it is dependent on
	 * the targetProperties.  (insertMode = replace causes
	 * statement type to be BULK_INSERT_REPLACE.
	 *
	 * @return the type of statement
	 */
    static int getStatementType(Properties targetProperties)
	{
		int retval = StatementType.INSERT;

		// The only property that we're currently interested in is insertMode
		String insertMode = (targetProperties == null) ? null : targetProperties.getProperty("insertMode");
		if (insertMode != null)
		{
			String upperValue = StringUtil.SQLToUpperCase(insertMode);
			if (upperValue.equals("REPLACE"))
			{
				retval = StatementType.BULK_INSERT_REPLACE;
			}
		}
		return retval;
	}

	/**
	 * Get the list of indexes on the table being inserted into.  This
	 * is used by INSERT.  This is an optimized version of what
	 * UPDATE and DELETE use. 
	 *
	 * @param td	TableDescriptor for the table being inserted into
	 *				or deleted from
	 *
	 * @exception StandardException		Thrown on error
	 */
	private void getAffectedIndexes
	(
		TableDescriptor 	td
	)	
					throws StandardException
	{
		IndexLister	indexLister = td.getIndexLister( );

		indicesToMaintain = indexLister.getDistinctIndexRowGenerators();
		indexConglomerateNumbers = indexLister.getDistinctIndexConglomerateNumbers();
		indexNames = indexLister.getDistinctIndexNames();

		/* Add dependencies on all indexes in the list */
		ConglomerateDescriptor[]	cds = td.getConglomerateDescriptors();
		CompilerContext cc = getCompilerContext();

 		for (int index = 0; index < cds.length; index++)
		{
			cc.createDependency(cds[index]);
		}
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

		if (targetColumnList != null)
		{
			targetColumnList.accept(v);
		}
	}

} // end of class InsertNode
