/*

   Derby - Class org.apache.derby.impl.sql.compile.UpdateNode

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

import java.lang.reflect.Modifier;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.derby.catalog.DefaultInfo;
import org.apache.derby.catalog.UUID;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.ClassName;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.StatementType;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.TagFilter;
import org.apache.derby.iapi.sql.conn.Authorizer;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.CheckConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptorList;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptorList;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptorList;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.vti.DeferModification;

/**
 * An UpdateNode represents an UPDATE statement.  It is the top node of the
 * query tree for that statement.
 * For positioned update, there may be no from table specified.
 * The from table will be derived from the cursor specification of
 * the named cursor.
 *
 */

public final class UpdateNode extends DMLModGeneratedColumnsStatementNode
{
	//Note: These are public so they will be visible to
	//the RepUpdateNode.
    int[]               changedColumnIds;
    boolean             deferred;
    ValueNode           checkConstraints;
	
	protected FromTable			targetTable;
	protected FormatableBitSet 			readColsBitSet;
	protected boolean 			positionedUpdate;

	/* Column name for the RowLocation in the ResultSet */
    static final String COLUMNNAME = "###RowLocationToUpdate";

	/**
     * Constructor for an UpdateNode.
	 *
	 * @param targetTableName	The name of the table to update
     * @param resultSet         The ResultSet that we will generate
     * @param matchingClause   Non-null if this DML is part of a MATCHED clause of a MERGE statement.
     * @param cm                The context manager
	 */
    UpdateNode
        (
         TableName targetTableName,
         ResultSetNode resultSet,
         MatchingClauseNode matchingClause,
         ContextManager cm
         )
	{
        super( resultSet, matchingClause, cm );
        this.targetTableName = targetTableName;
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
			return targetTableName.toString() + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

    @Override
    String statementToString()
	{
		return "UPDATE";
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

			/* RESOLVE - need to print out targetTableDescriptor */
		}
	}

	/**
	 * Bind this UpdateNode.  This means looking up tables and columns and
	 * getting their types, and figuring out the result types of all
	 * expressions, as well as doing view resolution, permissions checking,
	 * etc.
	 * <p>
	 * Binding an update will also massage the tree so that
	 * the ResultSetNode has a set of columns to contain the old row
	 * value, followed by a set of columns to contain the new row
	 * value, followed by a column to contain the RowLocation of the
	 * row to be updated.
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
		TableName					cursorTargetTableName = null;
		CurrentOfNode       		currentOfNode = null;
		ResultColumnList			afterColumns = null;

		DataDictionary dataDictionary = getDataDictionary();

		// check if targetTable is a synonym
		if (targetTableName != null)
		{
			TableName synonymTab = resolveTableToSynonym(this.targetTableName);
			if (synonymTab != null)
			{
				this.synonymTableName = targetTableName;
				this.targetTableName  = synonymTab;
			}
		}

        //
        // First step in associating added columns with the TARGET table of
        // a MERGE statement. Here we identify the columns which were NOT ADDED.
        //
        if ( inMatchingClause() ) { tagOriginalResultSetColumns(); }

        // collect lists of objects which will require privilege checks
        ArrayList<String>   explicitlySetColumns = getExplicitlySetColumns();
        List<CastNode> allCastNodes = collectAllCastNodes();
        tagPrivilegedNodes();

        // tell the compiler to only add privilege checks for nodes which have been tagged
        TagFilter   tagFilter = new TagFilter( TagFilter.NEED_PRIVS_FOR_UPDATE_STMT );
        getCompilerContext().addPrivilegeFilter( tagFilter );
		
		bindTables(dataDictionary);

		// wait to bind named target table until the cursor
		// binding is done, so that we can get it from the
		// cursor if this is a positioned update.

		// for positioned update, get the cursor's target table.
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT((resultSet!=null && resultSet instanceof SelectNode), 
				"Update must have a select result set");
		}

		SelectNode sel;
		sel = (SelectNode)resultSet;
		targetTable = (FromTable) sel.fromList.elementAt(0);

		if (targetTable instanceof CurrentOfNode) 
		{	
			positionedUpdate = true;
			currentOfNode = (CurrentOfNode) targetTable;
			cursorTargetTableName = currentOfNode.getBaseCursorTargetTableName();

			// instead of an assert, we might say the cursor is not updatable.
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(cursorTargetTableName != null);
			}
		}

		if (targetTable instanceof FromVTI)
		{
			targetVTI = (FromVTI) targetTable;
			targetVTI.setTarget();
		}
		else
		{
			// positioned update can leave off the target table.
			// we get it from the cursor supplying the position.
			if (targetTableName == null)
			{
				// verify we have current of
				if (SanityManager.DEBUG)
					SanityManager.ASSERT(cursorTargetTableName!=null);

				targetTableName = cursorTargetTableName;
			}
			// for positioned update, we need to verify that
			// the named table is the same as the cursor's target.
			else if (cursorTargetTableName != null)
			{
				// this match requires that the named table in the update
				// be the same as a correlation name in the cursor.
				if ( !targetTableName.equals(cursorTargetTableName))
				{
					throw StandardException.newException(SQLState.LANG_CURSOR_UPDATE_MISMATCH, 
						targetTableName,
						currentOfNode.getCursorName());
				}
			}
		}
		
		// because we verified that the tables match
		// and we already bound the cursor or the select,
		// the table descriptor should always be found.
		verifyTargetTable();

        // add UPDATE_PRIV on all columns on the left side of SET operators
        addUpdatePriv( explicitlySetColumns );

		/* OVERVIEW - We generate a new ResultColumn, CurrentRowLocation(), and
		 * prepend it to the beginning of the source ResultColumnList.  This
		 * will tell us which row(s) to update at execution time.  However,
		 * we must defer prepending this generated column until the other
		 * ResultColumns are bound since there will be no ColumnDescriptor
		 * for the generated column.  Thus, the sequence of actions is:
		 *
		 *		o  Bind existing ResultColumnList (columns in SET clause)
		 *		o  If this is a positioned update with a FOR UPDATE OF list,
		 *		   then verify that all of the target columns are in the
		 *		   FOR UPDATE OF list.
		 *		o  Get the list of indexes that need to be updated.
		 *		o  Create a ResultColumnList of all the columns in the target
		 *		   table - this represents the old row.
		 *		o  If we don't know which columns are being updated, 
	 	 *		   expand the original ResultColumnList to include all the
		 *		   columns in the target table, and sort it to be in the
		 *		   order of the columns in the target table.  This represents
		 *		   the new row.  Append it to the ResultColumnList representing
		 *		   the old row.
		 *		o  Construct the changedColumnIds array sorted by column position.
		 *		o  Generate the read column bit map and append any columns
		 *		   needed for index maint, etc.
		 *		o  Generate a new ResultColumn for CurrentRowLocation() and 
		 *		   mark it as a generated column.
		 *		o  Append the new ResultColumn to the ResultColumnList
		 *		   (This must be done before binding the expressions, so
		 *		   that the proper type info gets propagated to the new 
		 *		   ResultColumn.)
		 *		o  Bind the expressions.
		 *		o  Bind the generated ResultColumn.
		 */

		/* Verify that all underlying ResultSets reclaimed their FromList */
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(fromList.size() == 0,
				"fromList.size() is expected to be 0, not " + 
				fromList.size() +
				" on return from RS.bindExpressions()");
		}

        //
        // Add generated columns whose generation clauses mention columns
        // in the user's original update list.
        //
        ColumnDescriptorList    addedGeneratedColumns = new ColumnDescriptorList();
        ColumnDescriptorList    affectedGeneratedColumns = new ColumnDescriptorList();
        addGeneratedColumns
            ( targetTableDescriptor, resultSet, affectedGeneratedColumns, addedGeneratedColumns );
        
		/*
		** The current result column list is the one supplied by the user.
		** Mark these columns as "updated", so we can tell later which
		** columns are really being updated, and which have been added
		** but are not really being updated.
		*/
		resultSet.getResultColumns().markUpdated();

		/* Prepend CurrentRowLocation() to the select's result column list. */
		if (SanityManager.DEBUG)
            SanityManager.ASSERT((resultSet.getResultColumns() != null),	
							  "resultColumns is expected not to be null at bind time");

		/*
		** Get the result FromTable, which should be the only table in the
	 	** from list.
		*/
        /*
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(resultSet.getFromList().size() == 1,
                    "More than one table in result from list in an update.");
        }
        */
		/* Normalize the SET clause's result column list for synonym */
		if (synonymTableName != null)
			normalizeSynonymColumns( resultSet.getResultColumns(), targetTable );
		
		/* Bind the original result columns by column name */
		normalizeCorrelatedColumns( resultSet.getResultColumns(), targetTable );

		resultSet.bindResultColumns
            (
             targetTableDescriptor,
             targetVTI,
             resultSet.getResultColumns(), this,
             fromList
             );

        // don't allow overriding of generation clauses
        forbidGenerationOverrides( resultSet.getResultColumns(),
								   addedGeneratedColumns );
        
        //DERBY-6414(Incorrect handling when using an UPDATE to SET an 
        // identity column to DEFAULT)
        //The bug is fixed only for Derby 10.11 and higher. Starting 10.11,
        // we have started using sequence generator to create unique ids
        //If we fix this jira for prior releases, we will need to maintain
        // the code for old way of generating unique ids.
        if (dataDictionary.checkVersion( DataDictionary.DD_VERSION_DERBY_10_11, null )) {
            //Replace any DEFAULTs with the associated tree for the default if
            // allowed, otherwise throw an exception
            resultSet.getResultColumns().replaceOrForbidDefaults(
            		targetTableDescriptor, 
            		resultSet.getResultColumns(), true);
            resultSet.getResultColumns().checkForInvalidDefaults();
            resultSet.getResultColumns().forbidOverrides(resultSet.getResultColumns());
        } else {
    		LanguageConnectionContext lcc = getLanguageConnectionContext();
    		if (lcc.getAutoincrementUpdate() == false)
    			resultSet.getResultColumns().forbidOverrides(null);
        }

		/*
		** Mark the columns in this UpdateNode's result column list as
		** updateable in the ResultColumnList of the table being updated.
		** only do this for FromBaseTables - if the result table is a
		** CurrentOfNode, it already knows what columns in its cursor
		** are updateable.
		*/
		boolean allColumns = false;
		if (targetTable instanceof FromBaseTable)
		{
			((FromBaseTable) targetTable).markUpdated(
												resultSet.getResultColumns());
		}
		else if
            (
             (targetTable instanceof FromVTI) ||
             (targetTable instanceof FromSubquery)
             )
		{
            resultColumnList = resultSet.getResultColumns();
		}
		else
		{
			/*
			** Positioned update: WHERE CURRENT OF
			*/
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(currentOfNode != null, "currentOfNode is null");
			}

			ExecPreparedStatement	 cursorStmt = currentOfNode.getCursorStatement();

			/*
			** If there is no update column list, we need to build
			** out the result column list to have all columns.
			*/
            if (!cursorStmt.hasUpdateColumns())
			{
				/*
				** Get the resultColumnList representing ALL of the columns in the 
				** base table.  This is the "before" portion of the result row.
				*/
				getResultColumnList();

				/*
				** Add the "after" portion of the result row.  This is the update
				** list augmented to include every column in the target table.
				** Those columns that are not being updated are set to themselves.
				** The expanded list will be in the order of the columns in the base
				** table.
				*/
				afterColumns = resultSet.getResultColumns().expandToAll(
													targetTableDescriptor,
													targetTable.getTableName());
	
				/*
				** Need to get all indexes here since we aren't calling
				** getReadMap().
				*/
				getAffectedIndexes(targetTableDescriptor, 
									(ResultColumnList)null, (FormatableBitSet)null); 
				allColumns = true;
			}
			else
			{
				/* Check the updatability */
                resultSet.getResultColumns().checkColumnUpdateability(
                        cursorStmt, currentOfNode.getCursorName());
			}
		}

		changedColumnIds = getChangedColumnIds(resultSet.getResultColumns());

        //
        // Trigger transition tables are implemented as VTIs. This short-circuits some
        // necessary steps if the source table of a MERGE statement is a trigger
        // transition table. The following boolean is meant to prevent that short-circuiting.
        //
        boolean needBaseColumns = (targetVTI == null) || inMatchingClause();
        
		/*
		** We need to add in all the columns that are needed
		** by the constraints on this table.  
		*/
		if (!allColumns && needBaseColumns)
		{
			getCompilerContext().pushCurrentPrivType( Authorizer.NULL_PRIV);
			try
			{
				readColsBitSet = new FormatableBitSet();
				FromBaseTable fbt = getResultColumnList(resultSet.getResultColumns());

				afterColumns = resultSet.getResultColumns().copyListAndObjects();

				readColsBitSet = getReadMap(dataDictionary, 
										targetTableDescriptor, 
                                        afterColumns, affectedGeneratedColumns );

				afterColumns = fbt.addColsToList(afterColumns, readColsBitSet);
                
				resultColumnList = fbt.addColsToList(resultColumnList, readColsBitSet);

				/*
				** If all bits are set, then behave as if we chose all
				** in the first place
				*/
				int i = 1;
				int size = targetTableDescriptor.getMaxColumnID();
				for (; i <= size; i++)
				{
					if (!readColsBitSet.get(i))
					{
						break;
					}
				}

				if (i > size)
				{
					readColsBitSet = null;
				}	
			}
			finally
			{
				getCompilerContext().popCurrentPrivType();
			}
		}

        ValueNode rowLocationNode;

		if (needBaseColumns)
		{
			/* Append the list of "after" columns to the list of "before" columns,
			 * preserving the afterColumns list.  (Necessary for binding
			 * check constraints.)
			 */
			resultColumnList.appendResultColumns(afterColumns, false);

			/* Generate the RowLocation column */
            rowLocationNode = new CurrentRowLocationNode(getContextManager());
        }
        else
        {
           rowLocationNode = new NumericConstantNode(
                   TypeId.getBuiltInTypeId(Types.INTEGER), 0,
                   getContextManager());
        }
            
        ResultColumn rowLocationColumn = new ResultColumn(
                COLUMNNAME, rowLocationNode, getContextManager());
        rowLocationColumn.markGenerated();

        /* Append to the ResultColumnList */
        resultColumnList.addResultColumn(rowLocationColumn);

		/*
		 * The last thing that we do to the generated RCL is to clear
		 * the table name out from each RC. See comment on 
		 * checkTableNameAndScrubResultColumns().
		 */
		checkTableNameAndScrubResultColumns(resultColumnList);

		/* Set the new result column list in the result set */
		resultSet.setResultColumns(resultColumnList);

        //
        // Second step in associating added columns with the TARGET table of
        // a MERGE statement. Here we associate the columns which were not originally tagged.
        //
        if ( inMatchingClause() ) { associateAddedColumns(); }

        // SQL 2011, section 6.10, SR 4b.
        SelectNode.checkNoWindowFunctions(resultSet, "<update source>");

		/* Bind the expressions */
		super.bindExpressions();

		/* Bind untyped nulls directly under the result columns */
		resultSet.
			getResultColumns().
				bindUntypedNullsToResultColumns(resultColumnList);

        /* Bind the new ResultColumn */
        rowLocationColumn.bindResultColumnToExpression();

		resultColumnList.checkStorableExpressions();

		/* Insert a NormalizeResultSetNode above the source if the source
		 * and target column types and lengths do not match.
		 */
		if (! resultColumnList.columnTypesAndLengthsMatch())
 		{
            resultSet = new NormalizeResultSetNode(
                resultSet, resultColumnList, null, true, getContextManager());
			
								
 			if (hasCheckConstraints(dataDictionary, targetTableDescriptor) || hasGenerationClauses( targetTableDescriptor ) )
 			{
 				/* Get and bind all check constraints and generated columns on the columns
	 			 * being updated.  We want to bind the check constraints and
	 			 * generated columns against
	 			 * the after columns.  We need to bind against the portion of the
	 			 * resultColumns in the new NormalizeResultSet that point to 
	 			 * afterColumns.  Create an RCL composed of just those RCs in
	 			 * order to bind the check constraints.
	 			 */
	 			int afterColumnsSize = afterColumns.size();
                afterColumns = new ResultColumnList(getContextManager());
	 			ResultColumnList normalizedRCs = resultSet.getResultColumns();
	 			for (int index = 0; index < afterColumnsSize; index++)
	 			{
	 				afterColumns.addElement(normalizedRCs.elementAt(index + afterColumnsSize));
	 			}
			}
		}

        if( null != targetVTI && !inMatchingClause() )
		{
            deferred = VTIDeferModPolicy.deferIt( DeferModification.UPDATE_STATEMENT,
                                                  targetVTI,
                                                  resultColumnList.getColumnNames(),
                                                  sel.getWhereClause());
		}
        else // not VTI
        {
            /* we always include triggers in core language */
            boolean hasTriggers = (getAllRelevantTriggers(dataDictionary, targetTableDescriptor, 
                                                          changedColumnIds, true).size() > 0);

			ResultColumnList sourceRCL = hasTriggers ? resultColumnList : afterColumns;

            /* bind all generation clauses for generated columns */
            parseAndBindGenerationClauses
                ( dataDictionary, targetTableDescriptor, afterColumns, resultColumnList, true, resultSet );

            /* Get and bind all constraints on the columns being updated */
            checkConstraints = bindConstraints(
                dataDictionary,
                getOptimizerFactory(),
                targetTableDescriptor,
                null,
                sourceRCL,
                changedColumnIds,
                readColsBitSet,
                true, /* we always include triggers in core language */
                new boolean[1]); // dummy

            /* If the target table is also a source table, then
             * the update will have to be in deferred mode
             * For updates, this means that the target table appears in a
             * subquery.  Also, self referencing foreign keys are
             * deferred.  And triggers cause an update to be deferred.
             */
            if (resultSet.subqueryReferencesTarget(
                targetTableDescriptor.getName(), true) ||
                requiresDeferredProcessing())
            {
                deferred = true;
            }
            TransactionController tc = 
                    getLanguageConnectionContext().getTransactionCompile();

            autoincRowLocation = 
                    dataDictionary.computeAutoincRowLocations(tc, targetTableDescriptor);
        }

		identitySequenceUUIDString = getUUIDofSequenceGenerator();

		getCompilerContext().popCurrentPrivType();

        getCompilerContext().removePrivilegeFilter( tagFilter );

        //
        // Add USAGE privilege for all CASTs to UDTs mentioned in the WHERE clause and
        // on the right side of SET operators.
        //
        for ( CastNode value : allCastNodes )
        {
            addUDTUsagePriv( value );
        }
    } // end of bind()

    @Override
	int getPrivType()
	{
		return Authorizer.UPDATE_PRIV;
	}

    /**
     * Get the names of the explicitly set columns, that is, the columns on the left side
     * of SET operators.
     */
    private ArrayList<String>   getExplicitlySetColumns()
        throws StandardException
    {
        ArrayList<String>   result = new ArrayList<String>();
        ResultColumnList    rcl = resultSet.getResultColumns();

        for ( int i = 0; i < rcl.size(); i++ )
        {
            result.add( rcl.elementAt( i ).getName() );
        }

        return result;
    }

    /**
     * Associate all added columns with the TARGET table of the enclosing
     * MERGE statement.
     */
    private void    associateAddedColumns()
        throws StandardException
    {
        for ( ColumnReference cr : collectAllResultSetColumns() )
        {
            if ( !cr.taggedWith( TagFilter.ORIG_UPDATE_COL ) )
            {
                cr.setMergeTableID( ColumnReference.MERGE_TARGET );
            }
        }
    }

    /**
     * Tag the original columns mentioned in the result list.
     */
    private void    tagOriginalResultSetColumns()
        throws StandardException
    {
        for ( ColumnReference cr : collectAllResultSetColumns() )
        {
            cr.addTag( TagFilter.ORIG_UPDATE_COL );
        }
    }

    /**
     * Collect all of the result set columns.
     */
    private List<ColumnReference>   collectAllResultSetColumns()
        throws StandardException
    {
        CollectNodesVisitor<ColumnReference> crVisitor =
            new CollectNodesVisitor<ColumnReference>(ColumnReference.class);
        resultSet.getResultColumns().accept( crVisitor );

        return crVisitor.getList();
    }

    /**
     * Collect all of the CastNodes in the WHERE clause and on the right side
     * of SET operators. Later on, we will need to add permissions for all UDTs
     * mentioned by these nodes.
     */
    private List<CastNode>    collectAllCastNodes()
        throws StandardException
    {
        CollectNodesVisitor<CastNode> getCasts =
            new CollectNodesVisitor<CastNode>(CastNode.class);

        // process the WHERE clause
        ValueNode   whereClause = ((SelectNode) resultSet).whereClause;
        if ( whereClause != null ) { whereClause.accept( getCasts ); }

        // process the right sides of the SET operators
        ResultColumnList    rcl = resultSet.getResultColumns();
        for ( int i = 0; i < rcl.size(); i++ )
        {
            rcl.elementAt( i ).getExpression().accept( getCasts );
        }

        return getCasts.getList();
    }

    /**
     * Tag all of the nodes which may require privilege checks.
     * These are various QueryTreeNodes in the WHERE clause and on the right
     * side of SET operators.
     */
    private void    tagPrivilegedNodes()
        throws StandardException
    {
        ArrayList<QueryTreeNode>    result = new ArrayList<QueryTreeNode>();

        SelectNode  selectNode = (SelectNode) resultSet;

        // add this node so that addUpdatePriv() and addUDTUsagePriv() will work
        result.add( this );

        // process the WHERE clause
        ValueNode   whereClause = selectNode.whereClause;
        if ( whereClause !=  null ) { collectPrivilegedNodes( result, whereClause ); }

        // process the right sides of the SET operators
        ResultColumnList    rcl = resultSet.getResultColumns();
        for ( int i = 0; i < rcl.size(); i++ )
        {
            collectPrivilegedNodes( result, rcl.elementAt( i ).getExpression() );
        }

        // now tag all the nodes we collected
        for ( QueryTreeNode expr : result )
        {
            expr.addTag( TagFilter.NEED_PRIVS_FOR_UPDATE_STMT );
        }
    }

    /**
     * Add to an evolving list all of the nodes under an expression which may require privilege checks.
     */
    private void    collectPrivilegedNodes
        ( ArrayList<QueryTreeNode> result, QueryTreeNode expr )
        throws StandardException
    {
        // get all column references
        CollectNodesVisitor<ColumnReference> getCRs =
            new CollectNodesVisitor<ColumnReference>(ColumnReference.class);
        expr.accept( getCRs );
        result.addAll( getCRs.getList() );

        // get all function references
        CollectNodesVisitor<StaticMethodCallNode> getSMCNs =
            new CollectNodesVisitor<StaticMethodCallNode>(StaticMethodCallNode.class);
        expr.accept( getSMCNs );
        result.addAll( getSMCNs.getList() );

        // get all FromBaseTables in order to bulk-get their selected columns
        CollectNodesVisitor<FromBaseTable> getFBTs =
            new CollectNodesVisitor<FromBaseTable>(FromBaseTable.class);
        expr.accept( getFBTs );
        result.addAll( getFBTs.getList() );
    }

    /**
     * Add UPDATE_PRIV on all columns on the left side of SET operators.
     */
    private void    addUpdatePriv( ArrayList<String> explicitlySetColumns )
        throws StandardException
    {
        if ( !isPrivilegeCollectionRequired() ) { return; }
        
        CompilerContext cc = getCompilerContext();

        cc.pushCurrentPrivType( Authorizer.UPDATE_PRIV );
        try {
            for ( String columnName : explicitlySetColumns )
            {
                ColumnDescriptor    cd = targetTableDescriptor.getColumnDescriptor( columnName );
                cc.addRequiredColumnPriv( cd );
            }
        }
        finally
        {
            cc.popCurrentPrivType();
        }
    }

    /**
     * Add privilege checks for UDTs referenced by this statement.
     */

    
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
		//If this node references a SESSION schema table, then return true. 
		return(resultSet.referencesSessionSchema());

	}

	/**
	 * Compile constants that Execution will use
	 *
	 * @exception StandardException		Thrown on failure
	 */
    @Override
    public ConstantAction makeConstantAction() throws StandardException
	{
		/*
		** Updates are also deferred if they update a column in the index
		** used to scan the table being updated.
		*/
		if ( !deferred && !inMatchingClause() )
		{
			ConglomerateDescriptor updateCD =
										targetTable.
											getTrulyTheBestAccessPath().
												getConglomerateDescriptor();

			if (updateCD != null && updateCD.isIndex())
			{
				int [] baseColumns =
						updateCD.getIndexDescriptor().baseColumnPositions();

				if (resultSet.
						getResultColumns().
										updateOverlaps(baseColumns))
				{
					deferred = true;
				}
			}
		}

        if( null == targetTableDescriptor)
		{
			/* Return constant action for VTI
			 * NOTE: ConstantAction responsible for preserving instantiated
			 * VTIs for in-memory queries and for only preserving VTIs
			 * that implement Serializable for SPSs.
			 */
			return	getGenericConstantActionFactory().getUpdatableVTIConstantAction( DeferModification.UPDATE_STATEMENT,
						deferred, changedColumnIds);
		}

        int lckMode = inMatchingClause() ?
            TransactionController.MODE_RECORD : resultSet.updateTargetLockMode();
		long heapConglomId = targetTableDescriptor.getHeapConglomerateId();
		TransactionController tc = 
			getLanguageConnectionContext().getTransactionCompile();
		StaticCompiledOpenConglomInfo[] indexSCOCIs = 
			new StaticCompiledOpenConglomInfo[indexConglomerateNumbers.length];

		for (int index = 0; index < indexSCOCIs.length; index++)
		{
			indexSCOCIs[index] = tc.getStaticCompiledConglomInfo(indexConglomerateNumbers[index]);
		}

		/*
		** Do table locking if the table's lock granularity is
		** set to table.
		*/
		if (targetTableDescriptor.getLockGranularity() == TableDescriptor.TABLE_LOCK_GRANULARITY)
		{
            lckMode = TransactionController.MODE_TABLE;
		}


		return	getGenericConstantActionFactory().getUpdateConstantAction
            ( targetTableDescriptor,
			  tc.getStaticCompiledConglomInfo(heapConglomId),
			  indicesToMaintain,
			  indexConglomerateNumbers,
			  indexSCOCIs,
			  indexNames,
			  deferred,
			  targetTableDescriptor.getUUID(),
              lckMode,
			  false,
			  changedColumnIds, null, null, 
			  getFKInfo(),
			  getTriggerInfo(),
			  (readColsBitSet == null) ? (FormatableBitSet)null : new FormatableBitSet(readColsBitSet),
			  getReadColMap(targetTableDescriptor.getNumberOfColumns(),readColsBitSet),
			  resultColumnList.getStreamStorableColIds(targetTableDescriptor.getNumberOfColumns()),
			  (readColsBitSet == null) ? 
				  targetTableDescriptor.getNumberOfColumns() :
				  readColsBitSet.getNumBitsSet(),			
			  positionedUpdate,
			  resultSet.isOneRowResultSet(),
			  autoincRowLocation,
			  inMatchingClause(),
			  identitySequenceUUIDString
			  );
	}

	/**
	 * Updates are deferred if they update a column in the index
	 * used to scan the table being updated.
	 */
	protected void setDeferredForUpdateOfIndexColumn()
	{
		/* Don't bother checking if we're already deferred */
		if (! deferred )
		{
			/* Get the conglomerate descriptor for the target table */
			ConglomerateDescriptor updateCD =
										targetTable.
											getTrulyTheBestAccessPath().
												getConglomerateDescriptor();

			/* If it an index? */
			if (updateCD != null && updateCD.isIndex())
			{
				int [] baseColumns =
						updateCD.getIndexDescriptor().baseColumnPositions();

				/* Are any of the index columns updated? */
				if (resultSet.
						getResultColumns().
										updateOverlaps(baseColumns))
				{
					deferred = true;
				}
			}
		}
	}

	/**
	 * Code generation for update.
	 * The generated code will contain:
	 *		o  A static member for the (xxx)ResultSet with the RowLocations	and
	 *		   new update values
	 *		o  The static member will be assigned the appropriate ResultSet within
	 *		   the nested calls to get the ResultSets.  (The appropriate cast to the
	 *		   (xxx)ResultSet will be generated.)
	 *		o  The CurrentRowLocation() in SelectNode's select list will generate
	 *		   a new method for returning the RowLocation as well as a call to
	 *		   that method when generating the (xxx)ResultSet.
	 *
	 * @param acb	The ActivationClassBuilder for the class being built
	 * @param mb	The method for the execute() method to be built
	 *
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
		if(!isDependentTable)
			generateParameterValueSet(acb);


		/* Create the static declaration for the scan ResultSet which generates the
		 * RowLocations to be updated
		 * RESOLVE - Need to deal with the type of the static member.
		 */
		acb.newFieldDeclaration(Modifier.PRIVATE, 
								ClassName.CursorResultSet, 
								acb.newRowLocationScanResultSetName());

		/*
		** Generate the update result set, giving it either the original
		** source or the normalize result set, the constant action.
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

        if( null != targetVTI && !inMatchingClause() )
        {
			targetVTI.assignCostEstimate(resultSet.getNewCostEstimate());
            mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getUpdateVTIResultSet", ClassName.ResultSet, 1);
		}
        else
        {
			// arg 2 generate code to evaluate generation clauses
			generateGenerationClauses( resultColumnList, resultSet.getResultSetNumber(), true, acb, mb );

            // generate code to evaluate CHECK CONSTRAINTS
            generateCheckConstraints( checkConstraints, acb, mb ); // arg 3

            if(isDependentTable)
            {
                mb.push(acb.addItem(makeConstantAction()));
                mb.push(acb.addItem(makeResultDescription()));
                mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getDeleteCascadeUpdateResultSet",
                              ClassName.ResultSet, 5);
            }else
            {
                mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getUpdateResultSet",
                              ClassName.ResultSet, 3);
            }
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
		return StatementType.UPDATE;
	}


	/**
	 * Gets the map of all columns which must be read out of the base table.
	 * These are the columns needed to<UL>:
	 *		<LI>maintain indices</LI>
	 *		<LI>maintain foreign keys</LI>
	 *		<LI>maintain generated columns</LI>
	 *		<LI>support Replication's Delta Optimization</LI></UL>
	 * <p>
	 * The returned map is a FormatableBitSet with 1 bit for each column in the
	 * table plus an extra, unsued 0-bit. If a 1-based column id must
	 * be read from the base table, then the corresponding 1-based bit
	 * is turned ON in the returned FormatableBitSet.
	 * <p> 
	 * <B>NOTE</B>: this method is not expected to be called when
	 * all columns are being updated (i.e. updateColumnList is null).
	 *
	 * @param dd				the data dictionary to look in
	 * @param baseTable		the base table descriptor
	 * @param updateColumnList the rcl for the update. CANNOT BE NULL
	 * @param affectedGeneratedColumns columns whose generation clauses mention columns being updated
	 *
	 * @return a FormatableBitSet of columns to be read out of the base table
	 *
	 * @exception StandardException		Thrown on error
	 */
    FormatableBitSet getReadMap
	(
		DataDictionary		dd,
		TableDescriptor		baseTable,
		ResultColumnList	updateColumnList,
        ColumnDescriptorList    affectedGeneratedColumns
	)
		throws StandardException
	{
		boolean[]	needsDeferredProcessing = new boolean[1];
		needsDeferredProcessing[0] = requiresDeferredProcessing();

        ArrayList<ConglomerateDescriptor> conglomerates = new ArrayList<ConglomerateDescriptor>();
		relevantCdl = new ConstraintDescriptorList();
        relevantTriggers =  new TriggerDescriptorList();

		FormatableBitSet	columnMap = getUpdateReadMap
            (
             dd, baseTable, updateColumnList, conglomerates, relevantCdl,
             relevantTriggers, needsDeferredProcessing, affectedGeneratedColumns );

        markAffectedIndexes(conglomerates);

		adjustDeferredFlag( needsDeferredProcessing[0] );

		return	columnMap;
	}


	/**
	 * Construct the changedColumnIds array. Note we sort its entries by
	 * columnId.
	 */
	private int[] getChangedColumnIds(ResultColumnList rcl)
	{
		if (rcl == null) { return (int[])null; }
		else { return rcl.sortMe(); }
	}
    /**
	  *	Builds a bitmap of all columns which should be read from the
	  *	Store in order to satisfy an UPDATE statement.
	  *
	  *	Is passed a list of updated columns. Does the following:
	  *
	  *	1)	finds all indices which overlap the updated columns
	  *	2)	adds the index columns to a bitmap of affected columns
	  *	3)	adds the index descriptors to a list of conglomerate
	  *		descriptors.
	  *	4)	finds all constraints which overlap the updated columns
	  *		and adds the constrained columns to the bitmap
	  *	5)	finds all triggers which overlap the updated columns.
	  *	6)	Go through all those triggers from step 5 and for each one of
	  *     those triggers, follow the rules below to decide which columns
	  *     should be read.
	  *       Rule1)If trigger column information is null, then read all the
	  *       columns from trigger table into memory irrespective of whether
	  *       there is any trigger action column information. 2 egs of such
	  *       triggers
	  *         create trigger tr1 after update on t1 for each row values(1);
	  *         create trigger tr1 after update on t1 referencing old as oldt
	  *         	for each row insert into t2 values(2,oldt.j,-2); 
	  *       Rule2)If trigger column information is available but no trigger 
	  *       action column information is found and no REFERENCES clause is
	  *       used for the trigger, then read all the columns identified by 
	  *       the trigger column. eg 
	  *         create trigger tr1 after update of c1 on t1 
	  *         	for each row values(1);
	  *       Rule3)If trigger column information and trigger action column
	  *       information both are not null, then only those columns will be
	  *       read into memory. This is possible only for triggers created in
	  *       release 10.9 or higher(with the exception of 10.7.1.1 where we
	  *       did collect that information but because of corruption caused
	  *       by those changes, we do not use the information collected by
	  *       10.7). Starting 10.9, we are collecting trigger action column 
	  *       informatoin so we can be smart about what columns get read 
	  *       during trigger execution. eg
	  *         create trigger tr1 after update of c1 on t1 
	  *         	referencing old as oldt for each row 
	  *         	insert into t2 values(2,oldt.j,-2);
	  *       Rule4)If trigger column information is available but no trigger 
	  *       action column information is found but REFERENCES clause is used
	  *       for the trigger, then read all the columns from the trigger 
	  *       table. This will cover soft-upgrade scenario for triggers created 
	  *       pre-10.9. 
	  *       eg trigger created prior to 10.9
	  *         create trigger tr1 after update of c1 on t1 
	  *         	referencing old as oldt for each row 
	  *         	insert into t2 values(2,oldt.j,-2);
	  *	7)	adds the triggers to an evolving list of triggers
	  *	8)	finds all generated columns whose generation clauses mention
      *        the updated columns and adds all of the mentioned columns
	  *
	  *	@param	dd	Data Dictionary
	  *	@param	baseTable	Table on which update is issued
	  *	@param	updateColumnList	a list of updated columns
      * @param  conglomerates       OUT: list of affected indices
	  *	@param	relevantConstraints	IN/OUT. Empty list is passed in. We hang constraints on it as we go.
	  *	@param	relevantTriggers	IN/OUT. Passed in as an empty list. Filled in as we go.
	  *	@param	needsDeferredProcessing	IN/OUT. true if the statement already needs
	  *									deferred processing. set while evaluating this
	  *									routine if a trigger or constraint requires
	  *									deferred processing
	  *	@param	affectedGeneratedColumns columns whose generation clauses mention updated columns
	  *
	  * @return a FormatableBitSet of columns to be read out of the base table
	  *
	  * @exception StandardException		Thrown on error
	  */
    static FormatableBitSet getUpdateReadMap
	(
		DataDictionary		dd,
		TableDescriptor				baseTable,
		ResultColumnList			updateColumnList,
        List<ConglomerateDescriptor>     conglomerates,
		ConstraintDescriptorList	relevantConstraints,
        TriggerDescriptorList       relevantTriggers,
		boolean[]					needsDeferredProcessing,
        ColumnDescriptorList    affectedGeneratedColumns
	)
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(updateColumnList != null, "updateColumnList is null");
		}

		int		columnCount = baseTable.getMaxColumnID();
		FormatableBitSet	columnMap = new FormatableBitSet(columnCount + 1);

		/*
		** Add all the changed columns.  We don't strictly
		** need the before image of the changed column in all cases,
		** but it makes life much easier since things are set
		** up around the assumption that we have the before
		** and after image of the column.
		*/
		int[]	changedColumnIds = updateColumnList.sortMe();

		for (int ix = 0; ix < changedColumnIds.length; ix++)
		{
			columnMap.set(changedColumnIds[ix]);
		}

		/* 
		** Get a list of the indexes that need to be 
		** updated.  ColumnMap contains all indexed
		** columns where 1 or more columns in the index
		** are going to be modified.
		*/
        DMLModStatementNode.getXAffectedIndexes(
                baseTable, updateColumnList, columnMap, conglomerates);
 
		/* 
		** Add all columns needed for constraints.  We don't
		** need to bother with foreign key/primary key constraints
		** because they are added as a side effect of adding
		** their indexes above.
		*/
		baseTable.getAllRelevantConstraints(
            StatementType.UPDATE,
            changedColumnIds,
            needsDeferredProcessing,
            relevantConstraints);

		int rclSize = relevantConstraints.size();
		for (int index = 0; index < rclSize; index++)
		{
			ConstraintDescriptor cd = relevantConstraints.elementAt(index);
			if (cd.getConstraintType() != DataDictionary.CHECK_CONSTRAINT)
			{
				continue;
			}

			int[] refColumns = ((CheckConstraintDescriptor)cd).getReferencedColumns();
			for (int i = 0; i < refColumns.length; i++)
			{
				columnMap.set(refColumns[i]);
			}
		}

        //
        // Add all columns mentioned by generation clauses which are affected
        // by the columns being updated.
        //
        addGeneratedColumnPrecursors( baseTable, affectedGeneratedColumns, columnMap );
        
		/*
	 	* If we have any UPDATE triggers, then we will follow the 4 rules
	 	* mentioned in the comments at the method level.
	 	*/
		baseTable.getAllRelevantTriggers( StatementType.UPDATE, changedColumnIds, relevantTriggers );

		if (relevantTriggers.size() > 0)
		{
			needsDeferredProcessing[0] = true;
			
			boolean needToIncludeAllColumns = false;
			// If we are dealing with database created in 10.8 and prior,
			// then we must be in soft upgrade mode. For such databases,
			// we do not want to do any column reading optimization.
			//
			// For triggers created in 10.7.1.1, we kept track of trigger 
			// action columns used through the REFERENCING clause. That 
			// information was gathered so we could be smart about what
			// columns from trigger table should be read during trigger
			// execution. But those changes in code resulted in data
			// corruption DERBY-5121. Because of that, we took out the
			// column read optimization changes from codeline for next 
			// release of 10.7 and 10.8 codeline.
			// But we can still have triggers created in 10.7.1.1 with
			// trigger action column information in SYSTRIGGERS. 
			// In 10.9, we are reimplementing what columns should be read
			// from the trigger table during trigger execution. But we do
			// not want this column optimization changes to be used in soft 
			// upgrade mode for a 10.8 or prior database so that we can
			// go back to the older release if that's what the user chooses
			// after the soft-upgrade.
			boolean in10_9_orHigherVersion = dd.checkVersion(DataDictionary.DD_VERSION_DERBY_10_9,null);

            for (TriggerDescriptor trd : relevantTriggers) {
				if (in10_9_orHigherVersion) {
					// See if we can avoid reading all the columns from the
					// trigger table.
	                int[] referencedColsInTriggerAction = trd.getReferencedColsInTriggerAction();
	                int[] triggerCols = trd.getReferencedCols();
	                if (triggerCols == null || triggerCols.length == 0) {
	                        for (int i=0; i < columnCount; i++) {
	                                columnMap.set(i+1);
	                        }
	                        //This trigger is not defined on specific columns 
	                        // so we will have to read all the columns from the
	                        // trigger table. Now, there is no need to go 
	                        // through the rest of the triggers because we are
	                        // going to read all the columns anyways.
	                        break;
	                } else {
	                        if (referencedColsInTriggerAction == null ||
	                                        referencedColsInTriggerAction.length == 0) {
	                                //Does this trigger have REFERENCING clause defined on it
	                                if (!trd.getReferencingNew() && !trd.getReferencingOld()) {
	                                	//The trigger does not use trigger action columns through
	                                	//the REFERENCING clause so we need to read just the
	                                	//trigger columns
                                        for (int ix = 0; ix < triggerCols.length; ix++)
                                        {
                                                columnMap.set(triggerCols[ix]);
                                        }
	                                } else {
	                                	//The trigger has REFERENCING clause defined on it
	                                	// so it might be used them in trigger action.
	                                	// We should just go ahead and read all the
	                                	// columns from the trigger table. Now, there is 
	                                	// no need to go through the rest of the triggers 
	                                	// because we are going to read all the columns 
	                                	// anyways.
	        	                        needToIncludeAllColumns = true;
	        	                        break;
	                                }
	                        } else {
	                        	//This trigger has both trigger columns and
	                        	// trigger action columns(getting used through
	                        	// the REFERENCING clause). Read only those
	                        	// columns because that's all we need from
	                        	// trigger table for the trigger execution.
	                                for (int ix = 0; ix < triggerCols.length; ix++)
	                                {
	                                        columnMap.set(triggerCols[ix]);
	                                }
	                                for (int ix = 0; ix < referencedColsInTriggerAction.length; ix++)
	                                {
	                                        columnMap.set(referencedColsInTriggerAction[ix]);
	                                }
	                        }
	                }			
	            } else {
	            	//We are in soft upgrade mode working with 10.8 or lower 
	            	// database.
	                //Does this trigger have REFERENCING clause defined on it
	                if (!trd.getReferencingNew() && !trd.getReferencingOld())
	                        continue;
	                else
	                {
	                        needToIncludeAllColumns = true;
	                        break;
	                }
	            }
        }

        if (needToIncludeAllColumns) {
                for (int i = 1; i <= columnCount; i++)
                {
                        columnMap.set(i);
                }
			}
			
		}

		return	columnMap;
	}

    /**
     * Add all of the columns mentioned by the generation clauses of generated
     * columns. The generated columns were added when we called
     * addGeneratedColumns earlier on.
     */
    private static  void    addGeneratedColumnPrecursors
	(
     TableDescriptor         baseTable,
     ColumnDescriptorList    affectedGeneratedColumns,
     FormatableBitSet        columnMap
	)
		throws StandardException
	{
        int                                 generatedColumnCount = affectedGeneratedColumns.size();
        
        for ( int gcIdx = 0; gcIdx < generatedColumnCount; gcIdx++ )
        {
            ColumnDescriptor    gc = affectedGeneratedColumns.elementAt( gcIdx );
            String[]                       mentionedColumnNames = gc.getDefaultInfo().getReferencedColumnNames();
            int[]                       mentionedColumns = baseTable.getColumnIDs( mentionedColumnNames );
            int                         mentionedColumnCount = mentionedColumns.length;

            for ( int mcIdx = 0; mcIdx < mentionedColumnCount; mcIdx++ )
            {
                columnMap.set( mentionedColumns[ mcIdx ] );
                
            }   // done looping through mentioned columns
            
        }   // done looping through affected generated columns

    }
     
    /**
     * Add generated columns to the update list as necessary. We add
     * any column whose generation clause mentions columns already
     * in the update list. We fill in a list of all generated columns affected
     * by this update. We also fill in a list of all generated columns which we
     * added to the update list.
     */
    private void    addGeneratedColumns
	(
		TableDescriptor				baseTable,
        ResultSetNode               updateSet,
        ColumnDescriptorList    affectedGeneratedColumns,
        ColumnDescriptorList    addedGeneratedColumns
	)
		throws StandardException
	{
        ResultColumnList     updateColumnList = updateSet.getResultColumns();
        ColumnDescriptorList generatedColumns = baseTable.getGeneratedColumns();
        HashSet<String>      updatedColumns = new HashSet<String>();
        UUID                 tableID = baseTable.getObjectID();
        
        for (ResultColumn rc : updateColumnList)
		{
            updatedColumns.add( rc.getName() );
		}

        for (ColumnDescriptor gc : generatedColumns)
        {
            DefaultInfo defaultInfo = gc.getDefaultInfo();
            String[] mentionedColumnNames =
                    defaultInfo.getReferencedColumnNames();
            int mentionedColumnCount = mentionedColumnNames.length;

            // handle the case of setting a generated column to the DEFAULT
            // literal
            if ( updatedColumns.contains( gc.getColumnName() ) ) { affectedGeneratedColumns.add( tableID, gc ); }

            // figure out if this generated column is affected by the
            // update
            for (String mcn : mentionedColumnNames)
            {
                if ( updatedColumns.contains( mcn ) )
                {
                    // Yes, we are updating one of the columns mentioned in
                    // this generation clause.
                    affectedGeneratedColumns.add( tableID, gc );
                    
                    // If the generated column isn't in the update list yet,
                    // add it.
                    if ( !updatedColumns.contains( gc.getColumnName() ) )
                    {
                        addedGeneratedColumns.add( tableID, gc );
                        
                        // we will fill in the real value later on in parseAndBindGenerationClauses();
                        ValueNode dummy =
                            new UntypedNullConstantNode(getContextManager());
                        ResultColumn newResultColumn = new ResultColumn(
                            gc.getType(), dummy, getContextManager());
                        newResultColumn.setColumnDescriptor( baseTable, gc );
                        newResultColumn.setName( gc.getColumnName() );

                        updateColumnList.addResultColumn( newResultColumn );
                    }
                    
                    break;
                }
            }   // done looping through mentioned columns

        }   // done looping through generated columns
    }
     

	/*
	 * Force correlated column references in the SET clause to have the
	 * name of the base table. This dances around the problem alluded to
	 * in scrubResultColumn().
	 */
	private	void	normalizeCorrelatedColumns( ResultColumnList rcl, FromTable fromTable )
		throws StandardException
	{
		String		correlationName = fromTable.getCorrelationName();

		if ( correlationName == null ) { return; }

		TableName	tableNameNode;

		if ( fromTable instanceof CurrentOfNode )
		{ tableNameNode = ((CurrentOfNode) fromTable).getBaseCursorTargetTableName(); }
		else { tableNameNode = makeTableName( null, fromTable.getBaseTableName() ); }
		
        for (ResultColumn column : rcl)
		{
			ColumnReference	reference = column.getReference();

			if ( (reference != null) && correlationName.equals( reference.getTableName() ) )
			{
				reference.setQualifiedTableName( tableNameNode );
			}
		}
		
	}

	/**
	 * Check table name and then clear it from the result set columns.
	 * 
	 * @exception StandardExcepion if invalid column/table is specified.
	 */
	private void checkTableNameAndScrubResultColumns(ResultColumnList rcl) 
			throws StandardException
	{
        for (ResultColumn column : rcl)
		{
			boolean foundMatchingTable = false;			

            //
            // The check for whether we are in the matching clause fixes
            // the bug tracked by MergeStatementTest.test_060_transitionTableSimpleColumn().
            // That bug was addressed by derby-3155-53-aa-transitionSimpleColumn.diff.
            //
			if ( (column.getTableName() != null) && (!inMatchingClause()) ) {
                for (ResultSetNode rsn : ((SelectNode)resultSet).fromList) {
                    FromTable fromTable = (FromTable)rsn;

					final String tableName;
					if ( fromTable instanceof CurrentOfNode ) { 
						tableName = ((CurrentOfNode)fromTable).
								getBaseCursorTargetTableName().getTableName();
					} else { 
						tableName = fromTable.getBaseTableName();
					}

					if (column.getTableName().equals(tableName)) {
						foundMatchingTable = true;
						break;
					}
				}

				if (!foundMatchingTable) {
					throw StandardException.newException(
							SQLState.LANG_COLUMN_NOT_FOUND, 
							column.getTableName() + "." + column.getName());
				}
			}

			/* The table name is
			 * unnecessary for an update.  More importantly, though, it
			 * creates a problem in the degenerate case with a positioned
			 * update.  The user must specify the base table name for a
			 * positioned update.  If a correlation name was specified for
			 * the cursor, then a match for the ColumnReference would not
			 * be found if we didn't null out the name.  (Aren't you
			 * glad you asked?)
			 */
			column.clearTableName();
		}
	}

	/**
	 * Normalize synonym column references to have the name of the base table. 
	 *
	 * @param rcl	    The result column list of the target table
	 * @param fromTable The table name to set the column refs to
	 * 
	 * @exception StandardException		Thrown on error
	 */
	private	void normalizeSynonymColumns(
    ResultColumnList    rcl, 
    FromTable           fromTable)
		throws StandardException
	{
		if (fromTable.getCorrelationName() != null) 
        { 
            return; 
        }
		
		TableName tableNameNode;
		if (fromTable instanceof CurrentOfNode)
		{ 
			tableNameNode = 
                ((CurrentOfNode) fromTable).getBaseCursorTargetTableName(); 
		}
		else 
		{ 
			tableNameNode = makeTableName(null, fromTable.getBaseTableName()); 
		}
		
		super.normalizeSynonymColumns(rcl, tableNameNode);
	}
    
    /**
     * Do not allow generation clauses to be overriden. Throws an exception if
     * the user attempts to override the value of a generated column.  The only
     * value allowed in a generated column is DEFAULT. We will use
     * addedGeneratedColumns list to pass through the generated columns which
     * have already been added to the update list.
     *
     * @param targetRCL  the row in the table being UPDATEd
     * @param addedGeneratedColumns generated columns which the compiler added
     *        earlier on
     * @throws StandardException on error
     */
    private void forbidGenerationOverrides(
        ResultColumnList targetRCL,
        ColumnDescriptorList addedGeneratedColumns)
            throws StandardException
    {
        int  count = targetRCL.size();

        ResultColumnList    resultRCL = resultSet.getResultColumns();

        for ( int i = 0; i < count; i++ )
        {
            ResultColumn rc = targetRCL.elementAt( i );

            // defaults may already have been substituted for MERGE statements
            if ( rc.wasDefaultColumn() ) { continue; }
            
            if ( rc.hasGenerationClause() )
            {
                ValueNode   resultExpression =
                    resultRCL.elementAt( i ).getExpression();

                if ( !( resultExpression instanceof DefaultNode) )
                {
                    //
                    // We may have added the generation clause
                    // ourselves. Here we forgive ourselves for this
                    // pro-active behavior.
                    //
                    boolean allIsForgiven = false;

                    String columnName =
                        rc.getTableColumnDescriptor().getColumnName();

                    int addedCount = addedGeneratedColumns.size();

                    for ( int j = 0; j < addedCount; j++ )
                    {
                        String addedColumnName = addedGeneratedColumns.
                            elementAt(j).getColumnName();

                        if ( columnName.equals( addedColumnName ) )
                        {
                            allIsForgiven = true;
                            break;
                        }
                    }
                    if ( allIsForgiven ) { continue; }

                    throw StandardException.newException
                        (SQLState.LANG_CANT_OVERRIDE_GENERATION_CLAUSE,
                         rc.getName() );
                }
                else
                {
                    // Skip this step if we're working on an update
                    // statement. For updates, the target list has already
                    // been enhanced.
                    continue;
                }
            }
        }
    }
} // end of UpdateNode
