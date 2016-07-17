/*

   Derby - Class org.apache.derby.impl.sql.compile.ResultSetNode

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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.derby.catalog.types.DefaultInfoImpl;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.OptimizerFactory;
import org.apache.derby.iapi.sql.compile.Parser;
import org.apache.derby.iapi.sql.compile.RequiredRowOrdering;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DefaultDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.util.JBitSet;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * A ResultSetNode represents a result set, that is, a set of rows.  It is
 * analogous to a ResultSet in the LanguageModuleExternalInterface.  In fact,
 * code generation for a a ResultSetNode will create a "new" call to a
 * constructor for a ResultSet.
 *
 */

public abstract class ResultSetNode extends QueryTreeNode
{
	private int					resultSetNumber;
	/* Bit map of referenced tables under this ResultSetNode */
	private JBitSet				referencedTableMap;
	private ResultColumnList	resultColumns;
	private boolean				statementResultSet;
	private boolean				cursorTargetTable;
	private boolean				insertSource;

	private CostEstimate 		costEstimate;
	private CostEstimate		scratchCostEstimate;
	private Optimizer			optimizer;

	// Final cost estimate for this result set node, which is the estimate
	// for this node with respect to the best join order for the top-level
	// query. Subclasses will set this value where appropriate.
	private CostEstimate		candidateFinalCostEstimate;

    ResultSetNode(ContextManager cm) {
        super(cm);
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return  This object as a String
     */
    @Override
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return 	"resultSetNumber: " + resultSetNumber + "\n" +
				"referencedTableMap: " +
				(referencedTableMap != null 
						? referencedTableMap.toString() 
						: "null") + "\n" +
				"statementResultSet: " + statementResultSet + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
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

			if (resultColumns != null)
			{
				printLabel(depth, "resultColumns: ");
				resultColumns.treePrint(depth + 1);
			}
		}
	}

    /** Return true if this is a statement result set */
    boolean isStatementResultSet() { return statementResultSet; }

    /** Return true if this is a cursor target table */
    boolean isCursorTargetTable() { return cursorTargetTable; }

    /** Set whether this is a cursor target table */
    void    setCursorTargetTable( boolean yesOrNo ) { cursorTargetTable = yesOrNo; }

    /** Get the scratch estimate */
    CostEstimate    getScratchCostEstimate() { return scratchCostEstimate; }

    /** Set the scratch estimate */
    void    setScratchCostEstimate( CostEstimate ce ) { scratchCostEstimate = ce; }

	/**
	 * Get the resultSetNumber in this ResultSetNode. Expected to be set during
	 * generate(). Must be public in order to satisfy the Optimizable contract.
	 *
	 * @return int 	The resultSetNumber.
	 */

    public  int getResultSetNumber()
	{
		return resultSetNumber;
	}
    
    void    setResultSetNumber( int rsn ) { resultSetNumber = rsn; }
    

	/**
	 * Get the CostEstimate for this ResultSetNode.
	 *
	 * @return	The CostEstimate for this ResultSetNode.
	 */
    CostEstimate getCostEstimate()
	{
		return costEstimate;
	}

    /** Set the CostEstimate for this ResultSetNode */
    void    setCostEstimate( CostEstimate ce ) { costEstimate = ce; }

	/**
	 * Get the final CostEstimate for this ResultSetNode.
	 *
	 * @return	The final CostEstimate for this ResultSetNode.
	 */
    CostEstimate getFinalCostEstimate()
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			if (candidateFinalCostEstimate == null)
			{
				SanityManager.THROWASSERT(
					"candidateFinalCostEstimate is not expected to be null for " +
					getClass().getName());
			}
		}
		return candidateFinalCostEstimate;
	}

    /** Get the final cost estimate which we've set so far */
    CostEstimate    getCandidateFinalCostEstimate() { return candidateFinalCostEstimate; }

    /** Set the final cost estimate */
    void    setCandidateFinalCostEstimate( CostEstimate ce ) { candidateFinalCostEstimate = ce; }

	/**
	 * Assign the next resultSetNumber to the resultSetNumber in this ResultSetNode. 
	 * Expected to be done during generate().
	 *
	 * @exception StandardException		Thrown on error
	 */

    void assignResultSetNumber() throws StandardException
	{
		resultSetNumber = getCompilerContext().getNextResultSetNumber();
		resultColumns.setResultSetNumber(resultSetNumber);
	}

	/**
	 * Bind the non VTI tables in this ResultSetNode.  This includes getting their
	 * descriptors from the data dictionary and numbering them.
	 *
	 * @param dataDictionary	The DataDictionary to use for binding
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @return	ResultSetNode
	 *
	 * @exception StandardException		Thrown on error
	 */

    ResultSetNode bindNonVTITables(DataDictionary dataDictionary,
							FromList fromListParam) 
							throws StandardException {
		return this;
	}


	/**
	 * Bind the VTI tables in this ResultSetNode.  This includes getting their
	 * descriptors from the data dictionary and numbering them.
	 *
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @return	ResultSetNode
	 *
	 * @exception StandardException		Thrown on error
	 */

    ResultSetNode bindVTITables(FromList fromListParam)
		throws StandardException {
		return this;
	}

	/**
	 * Bind the expressions in this ResultSetNode.  This means binding the
	 * sub-expressions, as well as figuring out what the return type is for
	 * each expression.
	 *
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @exception StandardException		Thrown on error
	 */
    void bindExpressions(FromList fromListParam)
					throws StandardException
	{
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(false, 
					"bindExpressions() is not expected to be called for " + 
					this.getClass().toString());
	}

	/**
	 * Bind the expressions in this ResultSetNode if it has tables.  This means binding the
	 * sub-expressions, as well as figuring out what the return type is for
	 * each expression.
	 *
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @exception StandardException		Thrown on error
	 */
    void bindExpressionsWithTables(FromList fromListParam)
					throws StandardException
	{
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(false, 
					"bindExpressionsWithTables() is not expected to be called for " + 
					this.getClass().toString());
	}

	/**
	 * Bind the expressions in the target list.  This means binding the
	 * sub-expressions, as well as figuring out what the return type is
	 * for each expression.  This is useful for EXISTS subqueries, where we
	 * need to validate the target list before blowing it away and replacing
	 * it with a SELECT true.
	 *
	 * @exception StandardException		Thrown on error
	 */

    void bindTargetExpressions(FromList fromListParam)
					throws StandardException
	{
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(false, 
					"bindTargetExpressions() is not expected to be called for " + 
					this.getClass().toString());
	}

	/**
	 * Set the type of each parameter in the result column list for this
	 * table constructor.
	 *
	 * @param typeColumns	The ResultColumnList containing the desired result
	 *						types.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void setTableConstructorTypes(ResultColumnList typeColumns)
			throws StandardException
	{
        // VALUES clause needs special handling that's taken care of in a
        // sub-class. For all other nodes, just go through the result columns
        // and set the type for dynamic parameters.
        for (int i = 0; i < resultColumns.size(); i++) {
            ResultColumn rc = resultColumns.elementAt(i);
            ValueNode re = rc.getExpression();

            if (re != null && re.requiresTypeFromContext()) {
                ResultColumn typeCol = typeColumns.elementAt(i);
                re.setType(typeCol.getTypeServices());
            }
        }
	}

	/**
	 * Remember that this node is the source result set for an INSERT.
	 */
    void setInsertSource()
	{
		insertSource = true;
	}

    /** Return true if this is the source result set for an INSERT */
    boolean isInsertSource() { return insertSource; }
    
	/**
	 * Verify that a SELECT * is valid for this type of subquery.
	 *
	 * @param outerFromList	The FromList from the outer query block(s)
	 * @param subqueryType	The subquery type
	 *
	 * @exception StandardException		Thrown on error
	 */
    void verifySelectStarSubquery(FromList outerFromList, int subqueryType)
					throws StandardException
{
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(false, 
					"verifySelectStarSubquery() is not expected to be called for " + 
					this.getClass().toString());
	}

	/**
	 * Expand "*" into a ResultColumnList with all of the columns
	 * in the table's result list.
	 *
	 * @param allTableName		The qualifier on the "*"
	 *
	 * @return ResultColumnList The expanded list, or {@code null} if
	 * {@code allTableName} is non-null and doesn't match a table name in
	 * this result set
	 *
	 * @exception StandardException		Thrown on error
	 */
    ResultColumnList getAllResultColumns(TableName allTableName)
					throws StandardException
	{
		if (SanityManager.DEBUG)
		SanityManager.THROWASSERT(
							 "getAllResultColumns() not expected to be called for " + this.getClass().getName() + this);
		return null;
	}

	/**
	 * Try to find a ResultColumn in the table represented by this FromTable
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

    ResultColumn getMatchingColumn(
						ColumnReference columnReference)
						throws StandardException
	{
		if (SanityManager.DEBUG)
		SanityManager.THROWASSERT(
							 "getMatchingColumn() not expected to be called for " + this);
		return null;
	}

	/**
	 * Set the result column for the subquery to a boolean true,
	 * Useful for transformations such as
	 * changing:
	 *		where exists (select ... from ...) 
	 * to:
	 *		where (select true from ...)
	 *
	 * NOTE: No transformation is performed if the ResultColumn.expression is
	 * already the correct boolean constant.
	 * 
	 * @param onlyConvertAlls	Boolean, whether or not to just convert *'s
	 * @return ResultSetNode whose resultColumn was transformed; defaults
	 *  to "this" here, but can be overridden by subclasses.
	 *
	 * @exception StandardException		Thrown on error
	 */
    ResultSetNode setResultToBooleanTrueNode(boolean onlyConvertAlls)
		throws StandardException
	{
		ResultColumn		resultColumn;

		/* We need to be able to handle both ResultColumn and AllResultColumn
		 * since they are peers.
		 */
		if (resultColumns.elementAt(0) instanceof AllResultColumn)
		{
           resultColumn = new ResultColumn("", null, getContextManager());
		}
		else if (onlyConvertAlls)
		{
			return this;
		}
		else
		{
            resultColumn = resultColumns.elementAt(0);
	
			/* Nothing to do if query is already select TRUE ... */
            if (resultColumn.getExpression().isBooleanTrue() &&
                    resultColumns.size() == 1)
			{
				return this;
			}
		}
		
       BooleanConstantNode booleanNode =
                new BooleanConstantNode(true, getContextManager());

		resultColumn.setExpression(booleanNode);
		resultColumn.setType(booleanNode.getTypeServices());
		/* VirtualColumnIds are 1-based, RCLs are 0-based */
		resultColumn.setVirtualColumnId(1);
		resultColumns.setElementAt(resultColumn, 0);
		return this;
	}

	/**
	 * Get the FromList.  Create and return an empty FromList.  (Subclasses
	 * which actuall have FromLists will override this.)  This is useful because
	 * there is a FromList parameter to bindExpressions() which is used as
	 * the common FromList to bind against, allowing us to support
	 * correlation columns under unions in subqueries.
	 *
	 * @return FromList
	 * @exception StandardException		Thrown on error
	 */
    FromList getFromList()
		throws StandardException
	{
        return new FromList(getOptimizerFactory().doJoinOrderOptimization(),
                            getContextManager());
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

    void bindResultColumns(FromList fromListParam)
				throws StandardException
	{
		resultColumns.bindResultColumnsToExpressions();
	}

	/**
	 * Bind the result columns for this ResultSetNode to a base table.
	 * This is useful for INSERT and UPDATE statements, where the
	 * result columns get their types from the table being updated or
	 * inserted into.
	 * If a result column list is specified, then the verification that the 
	 * result column list does not contain any duplicates will be done when
	 * binding them by name.
	 *
	 * @param targetTableDescriptor	The TableDescriptor for the table being
	 *				updated or inserted into
	 * @param targetColumnList	For INSERT statements, the user
	 *					does not have to supply column
	 *					names (for example, "insert into t
	 *					values (1,2,3)".  When this
	 *					parameter is null, it means that
	 *					the user did not supply column
	 *					names, and so the binding should
	 *					be done based on order.  When it
	 *					is not null, it means do the binding
	 *					by name, not position.
	 * @param statement			Calling DMLStatementNode (Insert or Update)
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @exception StandardException		Thrown on error
	 */

    void bindResultColumns(TableDescriptor targetTableDescriptor,
            FromVTI targetVTI, ResultColumnList targetColumnList,
            DMLStatementNode statement, FromList fromListParam)
				throws StandardException
	{
		/* For insert select, we need to expand any *'s in the
		 * select before binding the result columns
		 */
		if (this instanceof SelectNode)
		{
			resultColumns.expandAllsAndNameColumns(((SelectNode)this).fromList);
		}

		/* If specified, copy the result column names down to the 
		 * source's target list.
		 */
		if (targetColumnList != null)
		{
			resultColumns.copyResultColumnNames(targetColumnList);
		}

		if (targetColumnList != null)
		{
			if (targetTableDescriptor != null)
			{
				resultColumns.bindResultColumnsByName(
                        targetTableDescriptor, statement);
			}
			else
			{
				resultColumns.bindResultColumnsByName(
						targetVTI.getResultColumns(), targetVTI, statement);
			}
		}
		else
			resultColumns.bindResultColumnsByPosition(targetTableDescriptor);
	}

	/**
	 * Bind untyped nulls to the types in the given ResultColumnList.
	 * This is used for binding the nulls in row constructors and
	 * table constructors.  In all other cases (as of the time of
	 * this writing), we do nothing.
	 *
	 * @param rcl	The ResultColumnList with the types to bind nulls to
	 *
	 * @exception StandardException		Thrown on error
	 */
    void bindUntypedNullsToResultColumns(ResultColumnList rcl)
				throws StandardException
	{
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

    ResultSetNode preprocess(int numTables,
									GroupByList gbl,
									FromList fromList)
								throws StandardException
	{
		if (SanityManager.DEBUG)
		SanityManager.THROWASSERT(
					"preprocess() not expected to be called for " + getClass().toString());
		return null;
	}

    /**
     * Find the unreferenced result columns and project them out.
     */
    void projectResultColumns() throws StandardException
    {
        // It is only necessary for joins
    }

	/**
	 * Ensure that the top of the RSN tree has a PredicateList.
	 *
	 * @param numTables			The number of tables in the query.
	 * @return ResultSetNode	A RSN tree with a node which has a PredicateList on top.
	 *
	 * @exception StandardException		Thrown on error
	 */
    ResultSetNode ensurePredicateList(int numTables)
		throws StandardException
	{
		if (SanityManager.DEBUG)
		SanityManager.THROWASSERT(
					"ensurePredicateList() not expected to be called for " + getClass().toString());
		return null;
	}

	/**
	 * Add a new predicate to the list.  This is useful when doing subquery
	 * transformations, when we build a new predicate with the left side of
	 * the subquery operator and the subquery's result column.
	 *
	 * @param predicate		The predicate to add
	 *
	 * @return ResultSetNode	The new top of the tree.
	 *
	 * @exception StandardException		Thrown on error
	 */
    ResultSetNode addNewPredicate(Predicate predicate)
			throws StandardException
	{
		if (SanityManager.DEBUG)
		SanityManager.THROWASSERT(
					"addNewPredicate() not expected to be called for " + getClass().toString());
		return null;
	}

	/**
	 * Evaluate whether or not the subquery in a FromSubquery is flattenable.  
	 * Currently, a FSqry is flattenable if all of the following are true:
	 *		o  Subquery is a SelectNode. (ie, not a RowResultSetNode or a UnionNode)
	 *		o  It contains no top level subqueries.  (RESOLVE - we can relax this)
	 *		o  It does not contain a group by or having clause
	 *		o  It does not contain aggregates.
	 *
	 * @param fromList	The outer from list
	 *
	 * @return boolean	Whether or not the FromSubquery is flattenable.
	 */
    boolean flattenableInFromSubquery(FromList fromList)
	{
		if (SanityManager.DEBUG)
		SanityManager.THROWASSERT(
					"flattenableInFromSubquery() not expected to be called for " + getClass().toString());
		return false;
	}

	/**
	 * Get a parent ProjectRestrictNode above us.
	 * This is useful when we need to preserve the
	 * user specified column order when reordering the
	 * columns in the distinct when we combine
	 * an order by with a distinct.
	 *
	 * @return A parent ProjectRestrictNode to do column reordering
	 *
	 * @exception StandardException		Thrown on error
	 */
	ResultSetNode genProjectRestrictForReordering()
				throws StandardException
	{
		/* We get a shallow copy of the ResultColumnList and its 
		 * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
		 */
        ResultColumnList prRCList = resultColumns;
		resultColumns = resultColumns.copyListAndObjects();

		/* Replace ResultColumn.expression with new VirtualColumnNodes
		 * in the ProjectRestrictNode's ResultColumnList.  (VirtualColumnNodes include
		 * pointers to source ResultSetNode, this, and source ResultColumn.)
		 * NOTE: We don't want to mark the underlying RCs as referenced, otherwise
		 * we won't be able to project out any of them.
		 */
		prRCList.genVirtualColumnNodes(this, resultColumns, false);
		/* Finally, we create the new ProjectRestrictNode */
        return new ProjectRestrictNode(
								this,
								prRCList,
								null,	/* Restriction */
								null,   /* Restriction as PredicateList */
								null,	/* Project subquery list */
								null,	/* Restrict subquery list */
								null,
								getContextManager()	 );
	}

	/**
	 * Optimize a ResultSetNode. This means choosing the best access
	 * path for each table under the ResultSetNode, among other things.
	 * 
	 * The only RSNs that need to implement their own optimize() are a 
	 * SelectNode and those RSNs that can appear above a SelectNode in the 
	 * query tree.  Currently, a ProjectRestrictNode is the only RSN that 
	 * can appear above a SelectNode.
	 *
	 * @param dataDictionary	The DataDictionary to use for optimization
	 * @param predicates		The PredicateList to apply.
	 * @param outerRows			The number of outer joining rows
	 *
	 * @return	ResultSetNode	The top of the optimized query tree
	 *
	 * @exception StandardException		Thrown on error
	 */

    ResultSetNode optimize(DataDictionary dataDictionary,
								  PredicateList predicates,
								  double outerRows) 
				throws StandardException	
	{
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(false, 
					"optimize() is not expected to be called for " + 
					this.getClass().toString());
		return null;
	}

	/**
	 * Modify the access paths according to the decisions the optimizer
	 * made.  This can include adding project/restrict nodes,
	 * index-to-base-row nodes, etc.
	 *
	 * @return	The modified query tree
	 *
	 * @exception StandardException		Thrown on error
	 */
    ResultSetNode modifyAccessPaths() throws StandardException
	{
		/* Default behavior is to do nothing */
		return this;
	}

	/**
	 * Modify the access paths according to the decisions the optimizer
	 * made.  This can include adding project/restrict nodes,
	 * index-to-base-row nodes, etc.
	 *
	 * @param predList A list of optimizable predicates that should
	 *  be pushed to this ResultSetNode, as determined by optimizer.
	 * @return The modified query tree
	 * @exception StandardException        Thrown on error
	 */
    ResultSetNode modifyAccessPaths(PredicateList predList)
		throws StandardException
	{
		// Default behavior is to call the no-arg version of this method.
		return modifyAccessPaths();
	}

	ResultColumnDescriptor[] makeResultDescriptors()
	{
	    return resultColumns.makeResultDescriptors();
	}

	/*
	** Check whether the column lengths and types of the result columns
	** match the expressions under those columns.  This is useful for
	** INSERT and UPDATE statements.  For SELECT statements this method
	** should always return true.  There is no need to call this for a
	** DELETE statement.
	**
	** @return	true means all the columns match their expressions,
	**		false means at least one column does not match its
	**		expression
	*/

	boolean columnTypesAndLengthsMatch()
		throws StandardException
	{
		return resultColumns.columnTypesAndLengthsMatch();
	}

	/**
	 * Set the resultColumns in this ResultSetNode
	 *
	 * @param newRCL		The new ResultColumnList for this ResultSetNode
	 */
    void setResultColumns(ResultColumnList newRCL)
	{
		resultColumns = newRCL;
	}

	/**
	 * Get the resultColumns for this ResultSetNode
	 *
	 * @return ResultColumnList for this ResultSetNode
	 */
    ResultColumnList getResultColumns()
	{
		return resultColumns;
	}

	/**
	 * Set the referencedTableMap in this ResultSetNode
	 *
	 * @param newRTM	The new referencedTableMap for this ResultSetNode
	 */
    void setReferencedTableMap(JBitSet newRTM)
	{
		referencedTableMap = newRTM;
	}

	/**
	 * Get the referencedTableMap for this ResultSetNode. Must be public
     * in order to satisfy the Optimizable contract.
	 *
	 * @return JBitSet	Referenced table map for this ResultSetNode
	 */
	public JBitSet getReferencedTableMap()
	{
		return referencedTableMap;
	}

	/**
	 * Fill the referencedTableMap with this ResultSetNode.
	 *
	 * @param passedMap	The table map to fill in.
	 */
    void fillInReferencedTableMap(JBitSet passedMap)
	{
	}

	/**
	 * Check for (and reject) ? parameters directly under the ResultColumns.
	 * This is done for SELECT statements.
	 *
	 * @exception StandardException		Thrown if a ? parameter found
	 *									directly under a ResultColumn
	 */

    void rejectParameters() throws StandardException
	{
		/* Okay if no resultColumns yet - means no parameters there */
		if (resultColumns != null)
		{
			resultColumns.rejectParameters();
		}
	}

	/**
	 * Check for (and reject) XML values directly under the ResultColumns.
	 * This is done for SELECT/VALUES statements.  We reject values
	 * in this case because JDBC does not define an XML type/binding
	 * and thus there's no standard way to pass such a type back
	 * to a JDBC application.
	 *
	 * @exception StandardException		Thrown if an XML value found
	 *									directly under a ResultColumn
	 */
    void rejectXMLValues() throws StandardException
	{
		if (resultColumns != null)
		{
			resultColumns.rejectXMLValues();
		}
	}

	/**
	 * Rename generated result column names as '1', '2' etc... These will be the result
	 * column names seen by JDBC clients.
	 */
    void renameGeneratedResultNames() throws StandardException
	{
		for (int i=0; i<resultColumns.size(); i++)
		{
            ResultColumn rc = resultColumns.elementAt(i);
			if (rc.isNameGenerated())
				rc.setName(Integer.toString(i+1));
		}
	}

	/**
		This method is overridden to allow a resultset node to know
		if it is the one controlling the statement -- i.e., it is
		the outermost result set node for the statement.
	 */
    void markStatementResultSet()
	{
		statementResultSet = true;
	}

	/**
	 * This ResultSet is the source for an Insert.  The target RCL
	 * is in a different order and/or a superset of this RCL.  In most cases
	 * we will add a ProjectRestrictNode on top of the source with an RCL that
	 * matches the target RCL.
	 * NOTE - The new or enhanced RCL will be fully bound.
	 *
	 * @param target            the target node for the insert
	 * @param inOrder           are source cols in same order as target cols?
	 * @param colMap			int array representation of correspondence between
	 *							RCLs - colmap[i] = -1 -&gt; missing in current RCL
	 *								   colmap[i] = j -&gt; targetRCL(i) &lt;-&gt; thisRCL(j+1)
	 * @return a node that replaces this node and whose RCL matches the target
	 * RCL. May return this node if no changes to the RCL are needed, or if the
	 * RCL is modified in-place.
	 *
	 * @exception StandardException		Thrown on error
	 */
	ResultSetNode enhanceRCLForInsert(
			InsertNode target, boolean inOrder, int[] colMap)
		throws StandardException
	{
		if (!inOrder ||
                resultColumns.visibleSize() < target.resultColumnList.size()) {
			return generateProjectRestrictForInsert(target, colMap);
		}
		return this;
	}

	/**
	 * Generate an RCL that can replace the original RCL of this node to
	 * match the RCL of the target for the insert.
	 *
	 * @param target the target node for the insert
	 * @param colMap int array representation of correspondence between
	 *        RCLs - colmap[i] = -1 -&gt; missing in current RCL
	 *               colmap[i] = j -&gt; targetRCL(i) &lt;-&gt; thisRCL(j+1)
	 * @return an RCL that matches the target RCL
	 */
	ResultColumnList getRCLForInsert(InsertNode target, int[] colMap)
			throws StandardException
	{
		// our newResultCols are put into the bound form straight away.
        ResultColumnList
                newResultCols = new ResultColumnList(getContextManager());

		/* Create a massaged version of the source RCL.
		 * (Much simpler to build new list and then assign to source,
		 * rather than massage the source list in place.)
		 */
		int numTargetColumns = target.resultColumnList.size();
		for (int index = 0; index < numTargetColumns; index++)
		{
            ResultColumn newResultColumn;

			if (colMap[index] != -1)
			{
				// getResultColumn uses 1-based positioning, so offset the colMap entry appropriately
				newResultColumn = resultColumns.getResultColumn(colMap[index]+1);
			}
			else
			{
				newResultColumn = genNewRCForInsert(
						target.targetTableDescriptor,
						target.targetVTI,
						index + 1,
						target.getDataDictionary());
			}

			newResultCols.addResultColumn(newResultColumn);
		}

		return newResultCols;
	}

	/**
	 * Generate the RC/expression for an unspecified column in an insert.
	 * Use the default if one exists.
	 *
	 * @param targetTD			Target TableDescriptor if the target is not a VTI, null if a VTI.
     * @param targetVTI         Target description if it is a VTI, null if not a VTI
	 * @param columnNumber		The column number
	 * @param dataDictionary	The DataDictionary
	 * @return	The RC/expression for the unspecified column.
	 *
	 * @exception StandardException		Thrown on error
	 */
	private ResultColumn genNewRCForInsert(TableDescriptor targetTD,
                                   FromVTI targetVTI,
                                   int columnNumber,
								   DataDictionary dataDictionary)
		throws StandardException
	{
        ResultColumn newResultColumn;

		// the i-th column's value was not specified, so create an
		// expression containing its default value (null for now)
		// REVISIT: will we store trailing nulls?

        if( targetVTI != null)
        {
            newResultColumn = targetVTI.getResultColumns().getResultColumn( columnNumber);
            newResultColumn = newResultColumn.cloneMe();
            newResultColumn.setExpressionToNullNode();
        }
        else
        {
            // column position is 1-based, index is 0-based.
            ColumnDescriptor colDesc = targetTD.getColumnDescriptor(columnNumber);
            DataTypeDescriptor colType = colDesc.getType();

            // Check for defaults
            DefaultInfoImpl defaultInfo = (DefaultInfoImpl) colDesc.getDefaultInfo();
	    
	    //Column has constant default value , 
	    //if it have defaultInfo and not be autoincrement.
            if (defaultInfo != null && ! colDesc.isAutoincrement())
            {
                //RESOLVEPARAMETER - skip the tree if we have the value
                /*
                  if (defaultInfo.getDefaultValue() != null)
                  {
                  }
                  else
                */
                {
                    if ( colDesc.hasGenerationClause() )
                    {
                        // later on we will revisit the generated columns and bind
                        // their generation clauses
                        newResultColumn = createGeneratedColumn( targetTD, colDesc );
                    }
                    else
                    {
                        // Generate the tree for the default
                        String defaultText = defaultInfo.getDefaultText();
                        ValueNode defaultTree = parseDefault(defaultText);
                        defaultTree = defaultTree.bindExpression
                            (getFromList(), (SubqueryList) null, (List<AggregateNode>) null);
                        newResultColumn = new ResultColumn(
                                defaultTree.getTypeServices(),
                                defaultTree,
                                getContextManager());
                    }

                    DefaultDescriptor defaultDescriptor = colDesc.getDefaultDescriptor(dataDictionary);
                    if (SanityManager.DEBUG)
                    {
                        SanityManager.ASSERT(defaultDescriptor != null,
                                             "defaultDescriptor expected to be non-null");
                    }
                    getCompilerContext().createDependency(defaultDescriptor); 
                }
            }
            else if (colDesc.isAutoincrement())
            {
                newResultColumn =
                        new ResultColumn(colDesc, null, getContextManager());
                newResultColumn.setAutoincrementGenerated();
            }
            else
            {
                newResultColumn = new ResultColumn(
                        colType, getNullNode(colType), getContextManager());
            }
        }

		// Mark the new RC as generated for an unmatched column in an insert
		newResultColumn.markGeneratedForUnmatchedColumnInInsert();

		return newResultColumn;
	}

	/**
	 * Generate a ProjectRestrictNode to put on top of this node if it's the
	 * source for an insert, and the RCL needs reordering and/or addition of
	 * columns in order to match the target RCL.
	 *
	 * @param target the target node for the insert
	 * @param colMap int array representation of correspondence between
	 *        RCLs - colmap[i] = -1 -&gt; missing in current RCL
	 *               colmap[i] = j -&gt; targetRCL(i) &lt;-&gt; thisRCL(j+1)
	 * @return a ProjectRestrictNode whos RCL matches the target RCL
	 */
	private ResultSetNode generateProjectRestrictForInsert(
			InsertNode target, int[] colMap)
		throws StandardException
	{
		// our newResultCols are put into the bound form straight away.
        ResultColumnList
                newResultCols = new ResultColumnList(getContextManager());

		int numTargetColumns = target.resultColumnList.size();

		/* Create a massaged version of the source RCL.
		 * (Much simpler to build new list and then assign to source,
		 * rather than massage the source list in place.)
		 */
		for (int index = 0; index < numTargetColumns; index++)
		{
			ResultColumn	newResultColumn;
			ResultColumn	oldResultColumn;
			ColumnReference newColumnReference;

			if (colMap[index] != -1)
			{
				// getResultColumn uses 1-based positioning, so offset the
				// colMap entry appropriately
				oldResultColumn =
						resultColumns.getResultColumn(colMap[index] + 1);

                newColumnReference = new ColumnReference(
                        oldResultColumn.getName(), null, getContextManager());

                DataTypeDescriptor  dtd = oldResultColumn.getType();
                if ( dtd == null )
                {
                    ColumnDescriptor    cd = target.targetTableDescriptor.getColumnDescriptor( index + 1 );
                    dtd = cd.getType();
                }

				/* The ColumnReference points to the source of the value */
				newColumnReference.setSource(oldResultColumn);
				// colMap entry is 0-based, columnId is 1-based.
				newColumnReference.setType( dtd );

				// Source of an insert, so nesting levels must be 0
				newColumnReference.setNestingLevel(0);
				newColumnReference.setSourceLevel(0);

				// because the insert already copied the target table's
				// column descriptors into the result, we grab it from there.
				// alternatively, we could do what the else clause does,
				// and look it up in the DD again.
               newResultColumn = new ResultColumn(
						dtd,
						newColumnReference,
						getContextManager());
			}
			else
			{
				newResultColumn = genNewRCForInsert(
						target.targetTableDescriptor,
						target.targetVTI,
						index + 1,
						target.getDataDictionary());
			}

			newResultCols.addResultColumn(newResultColumn);
		}

		/* The generated ProjectRestrictNode now has the ResultColumnList
		 * in the order that the InsertNode expects.
		 * NOTE: This code here is an exception to several "rules":
		 *		o  This is the only ProjectRestrictNode that is currently
		 *		   generated outside of preprocess().
		 *	    o  The UnionNode is the only node which is not at the
		 *		   top of the query tree which has ColumnReferences under
		 *		   its ResultColumnList prior to expression push down.
		 */
        return new ProjectRestrictNode(this,
                                       newResultCols,
                                       null,
                                       null,
                                       null,
                                       null,
                                       null,
                                       getContextManager());
	}

	/**
	  * Create a ResultColumn for a column with a generation clause.
	  */
    private  ResultColumn    createGeneratedColumn
        (
         TableDescriptor    targetTD,
         ColumnDescriptor   colDesc
         )
        throws StandardException
    {
        ValueNode dummy = new UntypedNullConstantNode(getContextManager());
        ResultColumn newResultColumn =
            new ResultColumn(colDesc.getType(), dummy, getContextManager());
        newResultColumn.setColumnDescriptor( targetTD, colDesc );

        return newResultColumn;
    }

	/**
	  *	Parse a default and turn it into a query tree.
	  *
	  *	@param	defaultText			Text of Default.
	  *
	  * @return	The parsed default as a query tree.
	  *
	  * @exception StandardException		Thrown on failure
	  */
	public	ValueNode	parseDefault
	(
		String				defaultText
    )
		throws StandardException
	{
		Parser						p;
		ValueNode					defaultTree;
		LanguageConnectionContext	lcc = getLanguageConnectionContext();

		/* Get a Statement to pass to the parser */

		/* We're all set up to parse. We have to build a compilable SQL statement
		 * before we can parse -  So, we goober up a VALUES defaultText.
		 */
		String values = "VALUES " + defaultText;
		
		/*
		** Get a new compiler context, so the parsing of the select statement
		** doesn't mess up anything in the current context (it could clobber
		** the ParameterValueSet, for example).
		*/
		CompilerContext newCC = lcc.pushCompilerContext();

		p = newCC.getParser();
				
		/* Finally, we can call the parser */
		// Since this is always nested inside another SQL statement, so topLevel flag
		// should be false
		Visitable qt = p.parseStatement(values);
		if (SanityManager.DEBUG)
		{
			if (! (qt instanceof CursorNode))
			{
				SanityManager.THROWASSERT(
					"qt expected to be instanceof CursorNode, not " +
					qt.getClass().getName());
			}
			CursorNode cn = (CursorNode) qt;
			if (! (cn.getResultSetNode() instanceof RowResultSetNode))
			{
				SanityManager.THROWASSERT(
					"cn.getResultSetNode() expected to be instanceof RowResultSetNode, not " +
					cn.getResultSetNode().getClass().getName());
			}
		}

        defaultTree = ((CursorNode) qt).getResultSetNode().getResultColumns().
                elementAt(0).getExpression();

		lcc.popCompilerContext(newCC);

		return	defaultTree;
	}

	/**
	 * Make a ResultDescription for use in a ResultSet.
	 * This is useful when generating/executing a NormalizeResultSet, since
	 * it can appear anywhere in the tree.
	 *
	 * @return	A ResultDescription for this ResultSetNode.
	 */

	public ResultDescription makeResultDescription()
	{
	    ResultColumnDescriptor[] colDescs = makeResultDescriptors();

	    return getExecutionFactory().getResultDescription(colDescs, null);
	}

	/**
		Determine if this result set is updatable or not, for a cursor
		(i.e., is it a cursor-updatable select).  This returns false
		and we expect selectnode to refine it for further checking.
	 *
	 * @exception StandardException		Thrown on error
	 */
	boolean isUpdatableCursor(DataDictionary dd) throws StandardException
	{
		if (SanityManager.DEBUG)
		SanityManager.DEBUG("DumpUpdateCheck","cursor is not a select result set");
		return false;
	}

	/**
		return the target table of an updatable cursor result set.
		since this is not updatable, just return null.
	 */
	FromTable getCursorTargetTable()
	{
		return null;
	}

	/**
		Mark this ResultSetNode as the target table of an updatable
		cursor.  Most types of ResultSetNode can't be target tables.
		@return true if the target table supports positioned updates.
	 */
    boolean markAsCursorTargetTable()
	{
		return false;
	}

	/**
		Mark this ResultSetNode as *not* the target table of an updatable
		cursor.
	 */
	void notCursorTargetTable()
	{
		cursorTargetTable = false;
	}

	/** 
	 * Put a ProjectRestrictNode on top of this ResultSetNode.
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
	 *
	 * This is useful for UNIONs, where we want to generate a DistinctNode above
	 * the UnionNode to eliminate the duplicates, because DistinctNodes expect
	 * their immediate child to be a PRN.
	 *
	 * @return The generated ProjectRestrictNode atop the original ResultSetNode.
	 *
	 * @exception StandardException		Thrown on error
	 */

    ResultSetNode genProjectRestrict()
				throws StandardException
	{
		/* We get a shallow copy of the ResultColumnList and its 
		 * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
		 */
        ResultColumnList prRCList = resultColumns;
		resultColumns = resultColumns.copyListAndObjects();

		/* Replace ResultColumn.expression with new VirtualColumnNodes
		 * in the ProjectRestrictNode's ResultColumnList.  (VirtualColumnNodes include
		 * pointers to source ResultSetNode, this, and source ResultColumn.)
		 */
		prRCList.genVirtualColumnNodes(this, resultColumns);

		/* Finally, we create the new ProjectRestrictNode */
        return new ProjectRestrictNode(
								this,
								prRCList,
								null,	/* Restriction */
								null,   /* Restriction as PredicateList */
								null,	/* Project subquery list */
								null,	/* Restrict subquery list */
								null,
                                getContextManager());
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

    ResultSetNode genProjectRestrict(int numTables)
				throws StandardException
	{
		return genProjectRestrict();
	}

	
    
    /**
	 * Generate the code for a NormalizeResultSet.
	   The call must push two items before calling this method
	   <OL>
	   <LI> pushGetResultSetFactoryExpression
	   <LI> the expression to normalize
	   </OL>
	 *
	 * @param acb				The ActivationClassBuilder
	 * @param mb				The method to put the generated code in
	 * @param resultSetNumber	The result set number for the NRS
	 * @param resultDescription	The ERD for the ResultSet
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
    void generateNormalizationResultSet(
						ActivationClassBuilder acb,
						MethodBuilder mb, 
						int resultSetNumber,
						ResultDescription resultDescription)
			throws StandardException
	{
		int erdNumber = acb.addItem(resultDescription);

		// instance and first arg are pushed by caller

		mb.push(resultSetNumber);
		mb.push(erdNumber);
		mb.push(getCostEstimate().rowCount());
		mb.push(getCostEstimate().getEstimatedCost());
		mb.push(false);

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getNormalizeResultSet",
					ClassName.NoPutResultSet, 6);
	}

	/**
	 * The optimizer's decision on the access path for a result set
	 * may require the generation of extra result sets.  For example,
	 * if it chooses an index for a FromBaseTable, we need an IndexToBaseRowNode
	 * above the FromBaseTable (and the FromBaseTable has to change its
	 * column list to match the index.
	 *
	 * This method in the parent class does not generate any extra result sets.
	 * It may be overridden in child classes.
	 *
	 * @return	A ResultSetNode tree modified to do any extra processing for
	 *			the chosen access path
	 *
	 * @exception StandardException		Thrown on error
	 */
    ResultSetNode changeAccessPath() throws StandardException
	{
		return this;
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
    boolean referencesTarget(String name, boolean baseTable)
		throws StandardException
	{
		return false;
	}

	/**
	 * Return whether or not this ResultSetNode contains a subquery with a
	 * reference to the specified target.
	 * 
	 * @param name	The table name.
	 *
	 * @return boolean	Whether or not a reference to the table was found.
	 *
	 * @exception StandardException		Thrown on error
	 */
	boolean subqueryReferencesTarget(String name, boolean baseTable)
		throws StandardException
	{
		return false;
	}

	/**
	 * Return whether or not the underlying ResultSet tree will return
	 * a single row, at most.
	 * This is important for join nodes where we can save the extra next
	 * on the right side if we know that it will return at most 1 row.
	 *
	 * @return Whether or not the underlying ResultSet tree will return a single row.
	 * @exception StandardException		Thrown on error
	 */
    boolean isOneRowResultSet() throws StandardException
	{
		// Default is false
		return false;
	}

	/**
	 * Return whether or not the underlying ResultSet tree is for a NOT EXISTS
	 * join.
	 *
	 * @return Whether or not the underlying ResultSet tree if for NOT EXISTS.
	 */
    boolean isNotExists()
	{
		// Default is false
		return false;
	}

	/**
	 * Get the optimizer for this result set.
	 * 
	 * @return If this.optimizer has has already been created by the
	 *  getOptimizer() method above, then return it; otherwise,
	 *  return null.
	 */
	protected OptimizerImpl getOptimizerImpl()
	{
		// Note that the optimizer might be null because it's possible that
		// we'll get here before any calls to getOptimizer() were made, which
		// can happen if we're trying to save a "best path" but we haven't
		// actually found one yet.  In that case we just return the "null"
		// value; the caller must check for it and behave appropriately.
		// Ex. see TableOperatorNode.addOrLoadBestPlanMapping().
		return (OptimizerImpl)optimizer;
	}

    /** Get the optimizer being used on this result set */
    Optimizer   getOptimizer()  { return optimizer; }

    /** Set the optimizer for use on this result set */
    void    setOptimizer( Optimizer opt ) { optimizer = opt; }

	/**
	 * Get a cost estimate to use for this ResultSetNode.
	 *
	 * @exception StandardException		Thrown on error
	 */
	protected CostEstimate getNewCostEstimate()
			throws StandardException
	{
		OptimizerFactory optimizerFactory = getLanguageConnectionContext().getOptimizerFactory();
		return optimizerFactory.getCostEstimate();
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

		if (resultColumns != null)
		{
			resultColumns = (ResultColumnList)resultColumns.accept(v);
		}
	}

	/**
	 * Consider materialization for this ResultSet tree if it is valid and cost effective
	 * (It is not valid if incorrect results would be returned.)
	 *
	 * @return Top of the new/same ResultSet tree.
	 *
	 * @exception StandardException		Thrown on error
	 */
    ResultSetNode considerMaterialization(JBitSet outerTables)
		throws StandardException
	{
		return this;
	}

	/**
	 * Return whether or not to materialize this ResultSet tree.
	 *
	 * @return Whether or not to materialize this ResultSet tree.
	 *			would return valid results.
	 *
	 * @exception StandardException		Thrown on error
	 */
    boolean performMaterialization(JBitSet outerTables)
		throws StandardException
	{
		return false;
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
    FromTable getFromTableByName(String name, String schemaName, boolean exactMatch)
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("getFromTableByName() not expected to be called for " +
									  getClass().getName());
		}
		return null;
	}

	/**
	 * Decrement (query block) level (0-based) for 
	 * all of the tables in this ResultSet tree.
	 * This is useful when flattening a subquery.
	 *
	 * @param decrement	The amount to decrement by.
	 */
	abstract void decrementLevel(int decrement);

	/**
	 * Push the order by list down from the cursor node
	 * into its child result set so that the optimizer
	 * has all of the information that it needs to 
     * consider sort avoidance. Presumes a new level
     * has been initialized by {@link #pushQueryExpressionSuffix()}.
	 *
	 * @param orderByList	The order by list
	 */
	void pushOrderByList(OrderByList orderByList)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("pushOrderByList() not expected to be called for " +
									  getClass().getName());
		}
	}


    /**
     * Push down the offset and fetch first parameters, if any. This method
     * should be overridden by the result sets that need this. Presumes a new
     * level has been initialized by {@link #pushQueryExpressionSuffix()}.
     *
     * @param offset    the OFFSET, if any
     * @param fetchFirst the OFFSET FIRST, if any
     * @param hasJDBClimitClause true if the clauses were added by (and have the semantics of) a JDBC limit clause
     */
    void pushOffsetFetchFirst( ValueNode offset, ValueNode fetchFirst, boolean hasJDBClimitClause )
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.THROWASSERT(
                "pushOffsetFetchFirst() not expected to be called for " +
                getClass().getName());
        }
    }

	/**
	 * General logic shared by Core compilation and by the Replication Filter
	 * compiler. A couple ResultSets (the ones used by PREPARE SELECT FILTER)
	 * implement this method.
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the expression will go into
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */

    void generateResultSet(ExpressionClassBuilder acb, MethodBuilder mb)
									throws StandardException
	{
		if (SanityManager.DEBUG)
        {
            SanityManager.THROWASSERT(
                "generateResultSet() not expected to be called for " +
                getClass().getName());
        }
	}


	/**
	 * Get the lock mode for the target of an update statement
	 * (a delete or update).  The update mode will always be row for
	 * CurrentOfNodes.  It will be table if there is no where clause.
	 *
	 * @see TransactionController
	 *
	 * @return	The lock mode
	 */
    int updateTargetLockMode()
	{
		return TransactionController.MODE_TABLE;
	}

	/**
	 * Mark this node and its children as not being a flattenable join.
	 */
	void notFlattenableJoin()
	{
	}

	/**
	 * Return whether or not the underlying ResultSet tree
	 * is ordered on the specified columns.
	 * RESOLVE - This method currently only considers the outermost table 
	 * of the query block.
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
    boolean isOrderedOn(ColumnReference[] crs, boolean permuteOrdering, List<FromBaseTable> fbtHolder)
				throws StandardException
	{
		return false;
	}

	/**
	 * Return whether or not this ResultSet tree is guaranteed to return
	 * at most 1 row based on heuristics.  (A RowResultSetNode and a
	 * SELECT with a non-grouped aggregate will return at most 1 row.)
	 *
	 * @return Whether or not this ResultSet tree is guaranteed to return
	 * at most 1 row based on heuristics.
	 */
	boolean returnsAtMostOneRow()
	{
		return false;
	}

    /**
     * Replace any DEFAULTs with the associated tree for the default if
     * allowed, or flag (when inside top level set operator nodes). Subqueries
     * are checked for illegal DEFAULTs elsewhere.
     *
     * @param ttd   The TableDescriptor for the target table.
     * @param tcl   The RCL for the target table.
     * @param allowDefaults true if allowed
     *
     * @exception StandardException         Thrown on error
     */
    void replaceOrForbidDefaults(TableDescriptor ttd,
                                 ResultColumnList tcl,
                                 boolean allowDefaults)
        throws StandardException
    {
        if (SanityManager.DEBUG) {
            SanityManager.THROWASSERT(
                "replaceOrForbidDefaults() not expected to be called for " +
                this.getClass().getName());
        }
    }

	/**
	 * Is it possible to do a distinct scan on this ResultSet tree.
	 * (See SelectNode for the criteria.)
	 *
	 * @param distinctColumns the set of distinct columns
	 * @return Whether or not it is possible to do a distinct scan on this ResultSet tree.
	 */
    boolean isPossibleDistinctScan(Set<BaseColumnNode> distinctColumns)
	{
		return false;
	}

	/**
	 * Mark the underlying scan as a distinct scan.
	 */
	void markForDistinctScan()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"markForDistinctScan() not expected to be called for " +
				getClass().getName());
		}
	}

	/**
	 * Notify the underlying result set tree that the optimizer has chosen
	 * to "eliminate" a sort.  Sort elimination can happen as part of
	 * preprocessing (see esp. SelectNode.preprocess(...)) or it can happen
	 * if the optimizer chooses an access path that inherently returns the
	 * rows in the correct order (also known as a "sort avoidance" plan).
	 * In either case we drop the sort and rely on the underlying result set
	 * tree to return its rows in the correct order.
	 *
	 * For most types of ResultSetNodes we automatically get the rows in the
	 * correct order if the sort was eliminated. One exception to this rule,
	 * though, is the case of an IndexRowToBaseRowNode, for which we have
	 * to disable bulk fetching on the underlying base table.  Otherwise
	 * the index scan could return rows out of order if the base table is
	 * updated while the scan is "in progress" (i.e. while the result set
	 * is open).
	 *
	 * In order to account for this (and potentially other, similar issues
	 * in the future) this method exists to notify the result set node that
	 * it is expected to return rows in the correct order.  The result set
	 * can then take necessary action to satsify this requirement--such as
	 * disabling bulk fetch in the case of IndexRowToBaseRowNode.
	 *
	 * All of that said, any ResultSetNodes for which we could potentially
	 * eliminate sorts should override this method accordingly.  So we don't
	 * ever expect to get here.
	 */
	void adjustForSortElimination()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"adjustForSortElimination() not expected to be called for " +
				getClass().getName());
		}
	}

	/**
	 * Same goal as adjustForSortElimination above, but this version
	 * takes a RequiredRowOrdering to allow nodes to adjust based on
	 * the ORDER BY clause, if needed.
	 */
	void adjustForSortElimination(RequiredRowOrdering rowOrdering)
		throws StandardException
	{
		/* Default is to ignore the row ordering; subclasses must
		 * override if they need to use it.
		 */
		adjustForSortElimination();
	}

	/**
	 * Count the number of distinct aggregates in the list.
	 * By 'distinct' we mean aggregates of the form:
	 *	<I>SELECT MAX(DISTINCT x) FROM T</I>
	 *
	 * @return number of aggregates
	 */
    static int numDistinctAggregates(List<AggregateNode> aggregates)
	{
		int		count = 0;
        int     size = aggregates.size();

		for (int index = 0; index < size; index++)
		{
            if ((aggregates.get(index)).isDistinct()) {
                count++;
            }
		}
		
		return count;
	}

	// It may be we have a SELECT view underneath a LOJ.
	// Return null for now.. we don't do any optimization.
    JBitSet LOJgetReferencedTables(int numTables)
				throws StandardException
	{
		if (this instanceof FromTable)
		{
			if (((FromTable)this).tableNumber != -1)
			{
				JBitSet map = new JBitSet(numTables);
				map.set(((FromTable)this).tableNumber);
				return map;
			}
		}

		return null;
	}

    /**
     * For ease of pushing order by, offset and fetch first/next clauses into
     * nodes. Clauses on the same nesting level have the same index in the
     * lists, so at any level, any of the lists' elements may be empty. For
     * example,
     *
     * {@code (select * from t order by a fetch next 5 rows only) order by b}
     *
     * would have
     * <pre>
     *            obl[0] = "order by a",
     *            offset[0] = null,
     *            fetchFirst[0] = "next 5 rows"
     * </pre>
     * and
     * <pre>
     *            obl[1] = "order by b",
     *            offset[1] = null
     *            fetchFirst[1] = null
     * </pre>
     *
     * When starting pushing clauses for a new level, always remember to do a
     * {@link #push} before adding the clauses via {@link #setOffset}, {@link
     * #setFetchFirst}, {@link #setOrderByList} and {@link
     * #setHasJDBCLimitClause}.
     */
    static class QueryExpressionClauses {
        private final List<OrderByList> obl = new ArrayList<OrderByList>();
        private final List<ValueNode> offset = new ArrayList<ValueNode>();
        private final List<ValueNode> fetchFirst = new ArrayList<ValueNode>();
        private final List<Boolean> hasJDBCLimitClause = new ArrayList<Boolean>();

        public QueryExpressionClauses() {
            // Push one level initially; that way we won't get out-of-range
            // errors when checking in cases where we have no clauses.
            // When pushing, we reuse unused levels anyway.
            push();
        }

        int size() {
            return obl.size();
        }

        void push() {
            int s = size();

            if (s > 0 &&
                 obl.get(s-1) == null &&
                 offset.get(s-1) == null &&
                 fetchFirst.get(s-1) == null) {
                // Reuse this level's holders since no clauses are present
                // here "hasJDBCLimitClause" implies offset or fetchFirst are
                // not null, so we don't need to check that.
                if (SanityManager.DEBUG) {
                    SanityManager.ASSERT(hasJDBCLimitClause.get(s-1) == null);
                }
            } else {
                // Push holders for a new level
                obl.add(null);
                offset.add(null);
                fetchFirst.add(null);
                hasJDBCLimitClause.add(null);
            }
        }

        void setOrderByList(OrderByList obl) {
            this.obl.set(size() - 1, obl);
        }

        void setOffset(ValueNode v) {
            this.offset.set(size() - 1, v);
        }

        void setFetchFirst(ValueNode v) {
            this.fetchFirst.set(size() - 1, v);
        }

        void setHasJDBCLimitClause(Boolean b) {
            this.hasJDBCLimitClause.set(size() - 1, b);
        }

        OrderByList getOrderByList(int i) {
            return obl.get(i);
        }

        void setOrderByList(int i, OrderByList obl) {
            this.obl.set(i, obl);
        }

        ValueNode getOffset(int i) {
            return offset.get(i);
        }

        void setOffset(int i, ValueNode v) {
            this.offset.set(i, v);
        }

        ValueNode getFetchFirst(int i) {
            return fetchFirst.get(i);
        }

        void setFetchFirst(int i, ValueNode v) {
            this.fetchFirst.set(i, v);
        }


        Boolean[] getHasJDBCLimitClause() {
            return hasJDBCLimitClause.toArray(new Boolean[1]);
        }

        boolean hasOffsetFetchFirst() {
            for (ValueNode o : offset) {
                if (o != null) {
                    return true;
                }
            }

            for (ValueNode ff : fetchFirst) {
                if (ff != null) {
                    return true;
                }
            }

            return false;
        }
    }

    /**
     * Set up a new level for order by and fetch/offset clauses.
     * See Javadoc for {@link ResultSetNode.QueryExpressionClauses}.
     * Overridden by implementors of pushOrderByNode, pushOffsetFetchFirst.
     */
    void pushQueryExpressionSuffix() {
        if (SanityManager.DEBUG) {
            SanityManager.NOTREACHED();
        }
    }

    void printQueryExpressionSuffixClauses(
        int depth,
        QueryExpressionClauses qec) {

        for (int i = 0; i < qec.size(); i++) {
            OrderByList obl = qec.getOrderByList(i);
            if ( obl != null ) {
                printLabel(depth, "orderByLists[" + i + "]:");
                obl.treePrint(depth + 1);
            }

            ValueNode offset = qec.getOffset(i);
            if (offset != null) {
                printLabel(depth, "offset:");
                offset.treePrint(depth + 1);
            }

            ValueNode fetchFirst = qec.getFetchFirst(i);
            if (fetchFirst != null) {
                printLabel(depth, "fetch first/next:");
                fetchFirst.treePrint(depth + 1);
            }

            Boolean hasJDBCLimitClause = qec.getHasJDBCLimitClause()[i];
            if (hasJDBCLimitClause != null) {
                printLabel(depth,
                           "hasJDBCLimitClause:" + hasJDBCLimitClause + "\n");
            }
        }
    }
}
