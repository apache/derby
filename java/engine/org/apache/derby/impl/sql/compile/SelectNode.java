/*

   Derby - Class org.apache.derby.impl.sql.compile.SelectNode

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

import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.sql.conn.Authorizer;

import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;


import org.apache.derby.iapi.reference.Limits;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.util.JBitSet;

import java.util.Vector;
import java.util.HashSet;

/**
 * A SelectNode represents the result set for any of the basic DML
 * operations: SELECT, INSERT, UPDATE, and DELETE.  (A RowResultSetNode
 * will be used for an INSERT with a VALUES clause.)  For INSERT - SELECT,
 * any of the fields in a SelectNode can be used (the SelectNode represents
 * the SELECT statement in the INSERT - SELECT).  For UPDATE and
 * DELETE, there will be one table in the fromList, and the groupByList
 * fields will be null. For both INSERT and UPDATE,
 * the resultColumns in the selectList will contain the names of the columns
 * being inserted into or updated.
 *
 */

public class SelectNode extends ResultSetNode
{
	/**
	 * List of tables in the FROM clause of this SELECT
	 */
	FromList	fromList;
	FromTable targetTable;

	/* Aggregate Vectors for select and where clauses */
	Vector	selectAggregates ;
	Vector	whereAggregates;
	Vector  havingAggregates;
	/**
	 * The ValueNode for the WHERE clause must represent a boolean
	 * expression.  The binding phase will enforce this - the parser
	 * does not have enough information to enforce it in all cases
	 * (for example, user methods that return boolean).
	 */
	ValueNode	whereClause;
	ValueNode	originalWhereClause;

	/**
	 * List of result columns in GROUP BY clause
	 */
	GroupByList	groupByList;

	/**
	 * List of windows.
	 */
	WindowList windows;

	/**
	 * List of window function calls (e.g. ROW_NUMBER, AVG(i), DENSE_RANK).
	 */
	Vector windowFuncCalls;

	/** User specified a group by without aggregates and we turned 
	 * it into a select distinct 
	 */
	private boolean wasGroupBy;
	
	/* List of columns in ORDER BY list */
	OrderByList orderByList;
	boolean		orderByQuery ;

    ValueNode   offset;  // OFFSET n ROWS, if given
    ValueNode   fetchFirst; // FETCH FIRST n ROWS ONLY, if given


	/* PredicateLists for where clause */
	PredicateList wherePredicates;

	/* SubqueryLists for select where and having clauses */
	SubqueryList  selectSubquerys;
	SubqueryList  whereSubquerys;
	SubqueryList  havingSubquerys;

	/* Whether or not we are only binding the target list */
	private boolean bindTargetListOnly;

	private boolean isDistinct;

	private boolean orderByAndDistinctMerged;

	boolean originalWhereClauseHadSubqueries;
	
	/* Copy of fromList prior to generating join tree */
	private FromList preJoinFL;

	ValueNode havingClause;
	
	private int nestingLevel;
	public void init(Object selectList,
			  Object aggregateVector,
			  Object fromList,
			  Object whereClause,
			  Object groupByList,
			  Object havingClause,
			  Object windowDefinitionList)
			throws StandardException
	{
		/* RESOLVE - remove aggregateList from constructor.
		 * Consider adding selectAggregates and whereAggregates 
		 */
		resultColumns = (ResultColumnList) selectList;
		if (resultColumns != null)
			resultColumns.markInitialSize();
		this.fromList = (FromList) fromList;
		this.whereClause = (ValueNode) whereClause;
		this.originalWhereClause = (ValueNode) whereClause;
		this.groupByList = (GroupByList) groupByList;
		this.havingClause = (ValueNode)havingClause;

		// This initially represents an explicit <window definition list>, as
		// opposed to <in-line window specifications>, see 2003, 6.10 and 6.11.
		// <in-line window specifications> are added later, see right below for
		// in-line window specifications used in window functions in the SELECT
		// column list and in genProjectRestrict for such window specifications
		// used in window functions in ORDER BY.
		this.windows = (WindowList)windowDefinitionList;

		bindTargetListOnly = false;
		
		this.originalWhereClauseHadSubqueries = false;
		if (this.whereClause != null){
			CollectNodesVisitor cnv = 
				new CollectNodesVisitor(SubqueryNode.class, SubqueryNode.class);
			this.whereClause.accept(cnv);
			if (!cnv.getList().isEmpty()){
				this.originalWhereClauseHadSubqueries = true;
			}
		}

		if (resultColumns != null) {

			// Collect window functions used in result columns, and check them
			// for any <in-line window specification>s.

			CollectNodesVisitor cnvw =
				new CollectNodesVisitor(WindowFunctionNode.class);
			resultColumns.accept(cnvw);
			windowFuncCalls = cnvw.getList();

			for (int i=0; i < windowFuncCalls.size(); i++) {
				WindowFunctionNode wfn =
					(WindowFunctionNode)windowFuncCalls.elementAt(i);

				// Some window function, e.g. ROW_NUMBER() contains an inline
				// window specification, so we add it to our list of window
				// definitions.

				if (wfn.getWindow() instanceof WindowDefinitionNode) {
					// Window function call contains an inline definition, add
					// it to our list of windows.
					windows = addInlinedWindowDefinition(windows, wfn);
				} else {
					// a window reference, bind it later.

					if (SanityManager.DEBUG) {
						SanityManager.ASSERT(
							wfn.getWindow() instanceof WindowReferenceNode);
					}
				}
			}
		}
	}

	private WindowList addInlinedWindowDefinition (WindowList wl,
												   WindowFunctionNode wfn) {
		WindowDefinitionNode wdn = (WindowDefinitionNode)wfn.getWindow();

		if (wl == null) {
			// This is the first window we see, so initialize list.
			wl = new WindowList();
			wl.setContextManager(getContextManager());
		}

		WindowDefinitionNode equiv = wdn.findEquivalentWindow(wl);

		if (equiv != null) {
			// If the window is equivalent an existing one, optimize
			// it away.

			wfn.setWindow(equiv);
		} else {
			// remember this window for posterity

			wl.addWindow((WindowDefinitionNode)wfn.getWindow());
		}

		return wl;
	}

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return "isDistinct: "+ isDistinct + "\n"+
				super.toString();
		}
		else
		{
			return "";
		}
	}

	public String statementToString()
	{
		return "SELECT";
	}

	public void makeDistinct()
	{
		isDistinct = true;
	}

	public void clearDistinct()
	{
		isDistinct = false;
	}

	boolean hasDistinct()
	{
		return isDistinct;
	}

	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 */

	public void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG)
		{
			super.printSubNodes(depth);

			if (selectSubquerys != null)
			{
				printLabel(depth, "selectSubquerys: ");
				selectSubquerys.treePrint(depth + 1);
			}

			printLabel(depth, "fromList: ");

			if (fromList != null)
			{
				fromList.treePrint(depth + 1);
			}

			if (whereClause != null)
			{
				printLabel(depth, "whereClause: ");
				whereClause.treePrint(depth + 1);
			}

			if ((wherePredicates != null) &&wherePredicates.size() > 0)
			{
				printLabel(depth, "wherePredicates: ");
				wherePredicates.treePrint(depth + 1);
			}

			if (whereSubquerys != null)
			{
				printLabel(depth, "whereSubquerys: ");
				whereSubquerys.treePrint(depth + 1);
			}

			if (groupByList != null) {
				printLabel(depth, "groupByList:");
				groupByList.treePrint(depth + 1);
			}

			if (havingClause != null) {
				printLabel(depth, "havingClause:");
				havingClause.treePrint(depth + 1);
			}

			if (orderByList != null) {
				printLabel(depth, "orderByList:");
				orderByList.treePrint(depth + 1);
			}

			if (preJoinFL != null)
			{
				printLabel(depth, "preJoinFL: ");
				preJoinFL.treePrint(depth + 1);
			}

			if (windows != null)
			{
				printLabel(depth, "windows: ");
				windows.treePrint(depth + 1);
			}
		}
	}

	/**
	 * Return the fromList for this SelectNode.
	 *
	 * @return FromList	The fromList for this SelectNode.
	 */
	public FromList getFromList()
	{
		return fromList;
	}

	/**
	 * Find colName in the result columns and return underlying columnReference.
	 * Note that this function returns null if there are more than one FromTable
	 * for this SelectNode and the columnReference needs to be directly under
	 * the resultColumn. So having an expression under the resultSet would cause
	 * returning null.
	 *
	 * @param	colName		Name of the column
	 *
	 * @return	ColumnReference	ColumnReference to the column, if found
	 */
	public ColumnReference findColumnReferenceInResult(String colName)
					throws StandardException
	{
		if (fromList.size() != 1)
			return null;

		// This logic is similar to SubQueryNode.singleFromBaseTable(). Refactor
		FromTable ft = (FromTable) fromList.elementAt(0);
		if (! ((ft instanceof ProjectRestrictNode) &&
		 		((ProjectRestrictNode) ft).getChildResult() instanceof FromBaseTable) &&
				!(ft instanceof FromBaseTable))
			return null;

		// Loop through the result columns looking for a match
		int rclSize = resultColumns.size();
		for (int index = 0; index < rclSize; index++)
		{
			ResultColumn rc = (ResultColumn) resultColumns.elementAt(index);
			if (! (rc.getExpression() instanceof ColumnReference))
				return null;

			ColumnReference crNode = (ColumnReference) rc.getExpression();

			if (crNode.columnName.equals(colName))
				return (ColumnReference) crNode.getClone();
		}

		return null;
	}

	/**
	 * Return the whereClause for this SelectNode.
	 *
	 * @return ValueNode	The whereClause for this SelectNode.
	 */
	public ValueNode getWhereClause()
	{
		return whereClause;
	}

	/**
	 * Return the wherePredicates for this SelectNode.
	 *
	 * @return PredicateList	The wherePredicates for this SelectNode.
	 */
	public PredicateList getWherePredicates()
	{
		return wherePredicates;
	}

	/**
	 * Return the selectSubquerys for this SelectNode.
	 *
	 * @return SubqueryList	The selectSubquerys for this SelectNode.
	 */
	public SubqueryList getSelectSubquerys()
	{
		return selectSubquerys;
	}

	/**
	 * Return the whereSubquerys for this SelectNode.
	 *
	 * @return SubqueryList	The whereSubquerys for this SelectNode.
	 */
	public SubqueryList getWhereSubquerys()
	{
		return whereSubquerys;
	}

	/**
	 * Bind the tables in this SelectNode.  This includes getting their
	 * TableDescriptors from the DataDictionary and numbering the FromTables.
	 * NOTE: Because this node represents the top of a new query block, we bind
	 * both the non VTI and VTI tables under this node in this method call.
	 *
	 * @param dataDictionary	The DataDictionary to use for binding
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @return	ResultSetNode
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ResultSetNode bindNonVTITables(DataDictionary dataDictionary,
						   FromList fromListParam) 
					throws StandardException
	{
		int fromListSize = fromList.size();
		

		wherePredicates = (PredicateList) getNodeFactory().getNode(
											C_NodeTypes.PREDICATE_LIST,
											getContextManager());
		preJoinFL = (FromList) getNodeFactory().getNode(
									C_NodeTypes.FROM_LIST,
									getNodeFactory().doJoinOrderOptimization(),
									getContextManager());

		/* Set the nesting level in the fromList */
		if (fromListParam.size() == 0)
		{
			nestingLevel = 0;
		}
		else
		{
			nestingLevel = ((FromTable) fromListParam.elementAt(0)).getLevel() + 1;
		}
		fromList.setLevel(nestingLevel);

		/* Splice a clone of our FromList on to the beginning of fromListParam, before binding
		 * the tables, for correlated column resolution in VTIs.
		 */
		for (int index = 0; index < fromListSize; index++)
		{
			fromListParam.insertElementAt(fromList.elementAt(index), 0);
		}

		// Now bind our from list
		fromList.bindTables(dataDictionary, fromListParam);

		/* Restore fromListParam */
		for (int index = 0; index < fromListSize; index++)
		{
			fromListParam.removeElementAt(0);
		}
		return this;
	}

	/**
	 * Bind the expressions in this SelectNode.  This means binding the
	 * sub-expressions, as well as figuring out what the return type is
	 * for each expression.
	 *
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void bindExpressions(FromList fromListParam)
					throws StandardException
	{
		int fromListParamSize = fromListParam.size();
		int fromListSize = fromList.size();
		int numDistinctAggs;

		if (SanityManager.DEBUG)
		SanityManager.ASSERT(fromList != null && resultColumns != null,
			"Both fromList and resultColumns are expected to be non-null");

		/* NOTE - a lot of this code would be common to bindTargetExpression(),
		 * so we use a private boolean to share the code instead of duplicating
		 * it.  bindTargetExpression() is responsible for toggling the boolean.
		 */
		if (! bindTargetListOnly)
		{
			/* Bind the expressions in FromSubquerys, JoinNodes, etc. */
			fromList.bindExpressions( fromListParam );
		}

		selectSubquerys = (SubqueryList) getNodeFactory().getNode(
											C_NodeTypes.SUBQUERY_LIST,
											getContextManager());
		selectAggregates = new Vector();

		/* Splice our FromList on to the beginning of fromListParam, before binding
		 * the expressions, for correlated column resolution.
		 */
		for (int index = 0; index < fromListSize; index++)
		{
			fromListParam.insertElementAt(fromList.elementAt(index), index);
		}

		// In preparation for resolving window references in expressions, we
		// make the FromList carry the set of explicit window definitions.
		//
		// E.g. "select row_number () from r, .. from t window r as ()"
		//
		// Here the expression "row_number () from r" needs to be bound to r's
		// definition. Window functions can also in-line window specifications,
		// no resolution is necessary. See also
		// WindowFunctionNode.bindExpression.

		fromListParam.setWindows(windows);

		resultColumns.bindExpressions(fromListParam, 
									  selectSubquerys,
									  selectAggregates);

		/* We're done if we're only binding the target list.
		 * (After we restore the fromList, of course.)
		 */
		if (bindTargetListOnly)
		{
			for (int index = 0; index < fromListSize; index++)
			{
				fromListParam.removeElementAt(0);
			}
			return;
		}

		whereAggregates = new Vector();
		whereSubquerys = (SubqueryList) getNodeFactory().getNode(
												C_NodeTypes.SUBQUERY_LIST,
												getContextManager());
        
        CompilerContext cc = getCompilerContext();
        
		if (whereClause != null)
		{
			cc.pushCurrentPrivType( Authorizer.SELECT_PRIV);

            int previousReliability = orReliability( CompilerContext.WHERE_CLAUSE_RESTRICTION );
			whereClause = whereClause.bindExpression(fromListParam, 
										whereSubquerys,
										whereAggregates);
            cc.setReliability( previousReliability );
			
			/* RESOLVE - Temporarily disable aggregates in the HAVING clause.
			** (We may remove them in the parser anyway.)
			** RESOLVE - Disable aggregates in the WHERE clause.  Someday
			** Aggregates will be allowed iff they are in a subquery
			** of the having clause and they correlate to an outer
			** query block.  For now, aggregates are not supported
			** in the WHERE clause at all.
			** Note: a similar check is made in JoinNode.
			*/
			if (whereAggregates.size() > 0)
			{
				throw StandardException.newException(SQLState.LANG_NO_AGGREGATES_IN_WHERE_CLAUSE);
			}

			/* If whereClause is a parameter, (where ?/where -?/where +?), then we should catch it and throw exception
			 */
			if (whereClause.isParameterNode())
				throw StandardException.newException(SQLState.LANG_NON_BOOLEAN_WHERE_CLAUSE, "PARAMETER" );
			
			whereClause = whereClause.checkIsBoolean();
			getCompilerContext().popCurrentPrivType();

			checkNoWindowFunctions(whereClause, "WHERE");
		}

		if (havingClause != null)
        {
            int previousReliability = orReliability( CompilerContext.HAVING_CLAUSE_RESTRICTION );

			havingAggregates = new Vector();
			havingSubquerys = (SubqueryList) getNodeFactory().getNode(
					C_NodeTypes.SUBQUERY_LIST,
					getContextManager());
			havingClause.bindExpression(
					fromListParam, havingSubquerys, havingAggregates);
			havingClause = havingClause.checkIsBoolean();
			checkNoWindowFunctions(havingClause, "HAVING");
            
            cc.setReliability( previousReliability );
		}
		
		/* Restore fromList */
		for (int index = 0; index < fromListSize; index++)
		{
			fromListParam.removeElementAt(0);
		}

		if (SanityManager.DEBUG) {
		SanityManager.ASSERT(fromListParam.size() == fromListParamSize,
			"fromListParam.size() = " + fromListParam.size() +
			", expected to be restored to " + fromListParamSize);
		SanityManager.ASSERT(fromList.size() == fromListSize,
			"fromList.size() = " + fromList.size() +
			", expected to be restored to " + fromListSize);
		}

		/* If query is grouped, bind the group by list. */
		if (groupByList != null)
		{
			Vector gbAggregateVector = new Vector();

			groupByList.bindGroupByColumns(this,
										   gbAggregateVector);

			/*
			** There should be no aggregates in the Group By list.
			** We don't expect any, but just to be on the safe side
			** we will check under sanity.
			*/
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(gbAggregateVector.size() == 0,
						"Unexpected Aggregate vector generated by Group By clause");
			}

			checkNoWindowFunctions(groupByList, "GROUP BY");
		}
		/* If ungrouped query with aggregates in SELECT list, verify
		 * that all result columns are valid aggregate expressions -
		 * no column references outside of an aggregate.
		 * If grouped query with aggregates in SELECT list, verify that all
		 * result columns are either grouping expressions or valid
		 * grouped aggregate expressions - the only column references
		 * allowed outside of an aggregate are columns in expressions in 
		 * the group by list.
		 */
		if (groupByList != null || selectAggregates.size() > 0)
		{

  			VerifyAggregateExpressionsVisitor visitor = 
  				new VerifyAggregateExpressionsVisitor(groupByList);
			resultColumns.accept(visitor);
		}       

		/*
		** RESOLVE: for now, only one distinct aggregate is supported
		** in the select list.
		*/
		numDistinctAggs = numDistinctAggregates(selectAggregates);
		if (groupByList == null && numDistinctAggs > 1)
		{
			throw StandardException.newException(SQLState.LANG_USER_AGGREGATE_MULTIPLE_DISTINCTS);
		}
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
	public void bindExpressionsWithTables(FromList fromListParam)
					throws StandardException
	{
		/* We have tables, so simply call bindExpressions() */
		bindExpressions(fromListParam);
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

	public void bindTargetExpressions(FromList fromListParam)
					throws StandardException
	{
		/*
		 * With a FromSubquery in the FromList we cannot bind target expressions 
		 * at this level (DERBY-3321)
		 */
		CollectNodesVisitor cnv = new CollectNodesVisitor(FromSubquery.class, 
														  FromSubquery.class);
		fromList.accept(cnv);
		if (!cnv.getList().isEmpty()){		
			bindTargetListOnly = false;
		} else {
			bindTargetListOnly = true;				
		}		
		bindExpressions(fromListParam);
		bindTargetListOnly = false;
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

	public void bindResultColumns(FromList fromListParam)
				throws StandardException
	{
		/* We first bind the resultColumns for any FromTable which
		 * needs its own binding, such as JoinNodes.
		 * We pass through the fromListParam without adding our fromList
		 * to it, since the elements in our fromList can only be correlated
		 * with outer query blocks.
		 */
		fromList.bindResultColumns(fromListParam);
		super.bindResultColumns(fromListParam);
		/* Only 1012 elements allowed in select list */
		if (resultColumns.size() > Limits.DB2_MAX_ELEMENTS_IN_SELECT_LIST)
		{
			throw StandardException.newException(SQLState.LANG_TOO_MANY_ELEMENTS);
		}

        // DERBY-4407: A derived table must have at least one column.
        if (resultColumns.size() == 0)
        {
            throw StandardException.newException(
                    SQLState.LANG_EMPTY_COLUMN_LIST);
        }
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

	public void bindResultColumns(TableDescriptor targetTableDescriptor,
					FromVTI targetVTI,
					ResultColumnList targetColumnList,
					DMLStatementNode statement,
					FromList fromListParam)
				throws StandardException
	{
		/* We first bind the resultColumns for any FromTable which
		 * needs its own binding, such as JoinNodes.
		 * We pass through the fromListParam without adding our fromList
		 * to it, since the elements in our fromList can only be correlated
		 * with outer query blocks.
		 */
		fromList.bindResultColumns(fromListParam);
		super.bindResultColumns(targetTableDescriptor,
								targetVTI,
								targetColumnList, statement,
								fromListParam);
	}

	/** 
	 * Push an expression into this SELECT (and possibly down into
	 * one of the tables in the FROM list).  This is useful when
	 * trying to push predicates into unflattened views or
	 * derived tables.
	 *
	 * @param predicate	The predicate that we attempt to push
	 *
	 * @exception StandardException		Thrown on error
	 */
	void pushExpressionsIntoSelect(Predicate predicate)
		throws StandardException
	{
		wherePredicates.pullExpressions(referencedTableMap.size(), predicate.getAndNode());
		fromList.pushPredicates(wherePredicates);
	}


	/**
	 * Verify that a SELECT * is valid for this type of subquery.
	 *
	 * @param outerFromList	The FromList from the outer query block(s)
	 * @param subqueryType	The subquery type
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void verifySelectStarSubquery(FromList outerFromList, int subqueryType) 
					throws StandardException
	{
		if (! ((ResultColumn) resultColumns.elementAt(0) instanceof AllResultColumn) )
		{
			return;
		}

		/* Select * currently only valid for EXISTS/NOT EXISTS.
		 * NOT EXISTS does not appear prior to preprocessing.
		 */
		if (subqueryType != SubqueryNode.EXISTS_SUBQUERY)
		{
			throw StandardException.newException(SQLState.LANG_CANT_SELECT_STAR_SUBQUERY);
		}

		/* If the AllResultColumn is qualified, then we have to verify
		 * that the qualification is a valid exposed name.
		 * NOTE: The exposed name can come from an outer query block.
		 */
		String		fullTableName;
			
		fullTableName = ((AllResultColumn) resultColumns.elementAt(0)).getFullTableName();

		if (fullTableName != null)
		{
			if (fromList.getFromTableByName(fullTableName, null, true) == null &&
				outerFromList.getFromTableByName(fullTableName, null, true) == null)
			{
				if (fromList.getFromTableByName(fullTableName, null, false) == null &&
					outerFromList.getFromTableByName(fullTableName, null, false) == null)
				{
					throw StandardException.newException(SQLState.LANG_EXPOSED_NAME_NOT_FOUND, fullTableName);
				}
			}
		}
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
	protected FromTable getFromTableByName(String name, String schemaName, boolean exactMatch)
		throws StandardException
	{
		return fromList.getFromTableByName(name, schemaName, exactMatch);
	}

	/**
	 * Check for (and reject) ? parameters directly under the ResultColumns.
	 * This is done for SELECT statements.
	 *
	 * @exception StandardException		Thrown if a ? parameter found
	 *									directly under a ResultColumn
	 */

	public void rejectParameters() throws StandardException
	{
		super.rejectParameters();
		fromList.rejectParameters();
	}

	/**
	 * Push the order by list down from the cursor node
	 * into its child result set so that the optimizer
	 * has all of the information that it needs to 
	 * consider sort avoidance.
	 *
	 * @param orderByList	The order by list
	 */
	void pushOrderByList(OrderByList orderByList)
	{
		this.orderByList = orderByList;
		// remember that there was an order by list
		orderByQuery = true;
	}

    /**
     * Push down the offset and fetch first parameters to this node.
     *
     * @param offset    the OFFSET, if any
     * @param fetchFirst the OFFSET FIRST, if any
     */
    void pushOffsetFetchFirst(ValueNode offset, ValueNode fetchFirst)
    {
        this.offset = offset;
        this.fetchFirst = fetchFirst;
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
	 * @param numTables			The number of tables in the DML Statement
	 * @param gbl				The outer group by list, if any
	 * @param fl			The from list, if any
	 *
	 * @return The generated ProjectRestrictNode atop the original FromTable.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ResultSetNode preprocess(int numTables,
									GroupByList gbl,
									FromList fl)
								throws StandardException
	{
		ResultSetNode newTop = this;

		/* Put the expression trees in conjunctive normal form.
		 * NOTE - This needs to occur before we preprocess the subqueries
		 * because the subquery transformations assume that any subquery operator 
		 * negation has already occurred.
		 */
		whereClause = normExpressions(whereClause);
		// DERBY-3257. We need to normalize the having clause as well, because 
		// preProcess expects CNF.
		havingClause = normExpressions(havingClause);
		
		/**
		 * This method determines if (1) the query is a LOJ, and (2) if the LOJ is a candidate for
		 * reordering (i.e., linearization).  The condition for LOJ linearization is:
		 * 1. either LOJ or ROJ in the fromList, i.e., no INNER, NO FULL JOINs
		 * 2. ON clause must be equality join between left and right operands and in CNF (i.e., AND is allowed)
		 */
		boolean anyChange = fromList.LOJ_reorderable(numTables);
		if (anyChange)
		{
			FromList afromList = (FromList) getNodeFactory().getNode(C_NodeTypes.FROM_LIST,
																	 getNodeFactory().doJoinOrderOptimization(),
																	 getContextManager());
			bindExpressions(afromList);
            fromList.bindResultColumns(afromList);
		}

		/* Preprocess the fromList.  For each FromTable, if it is a FromSubquery
		 * then we will preprocess it, replacing the FromSubquery with a
		 * ProjectRestrictNode. If it is a FromBaseTable, then we will generate
		 * the ProjectRestrictNode above it.
		 */
		fromList.preprocess(numTables, groupByList, whereClause);

		/* selectSubquerys is always allocated at bind() time */
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(selectSubquerys != null,
				"selectSubquerys is expected to be non-null");
		}

		/* Preprocess the RCL after the from list so that
		 * we can flatten/optimize any subqueries in the
		 * select list.
		 */
		resultColumns.preprocess(numTables, 
								 fromList, whereSubquerys,
								 wherePredicates);

		/* Preprocess the expressions.  (This is necessary for subqueries.
		 * This is also where we do tranformations such as for LIKE.)
		 *
		 * NOTE: We do this after preprocessing the fromList so that, for
		 * quantified subqueries, the join expression with the outer
		 * expression will be pushable (to be pushable, the ColumnReference
		 * has to point to a VirtualColumnNode, and not to a BaseColumnNode).
		 */
		if (whereClause != null)
		{
			// DERBY-3301
			// Mark subqueries that are part of the where clause as such so
			// that we can avoid flattening later, particularly for nested 
			// WHERE EXISTS subqueries.
			if (whereSubquerys != null){
				whereSubquerys.markWhereSubqueries();
			}
			whereClause.preprocess(numTables,
								   fromList, whereSubquerys,
								   wherePredicates);
		}

		/* Preprocess the group by list too. We need to compare 
		 * expressions in the group by list with the select list and we 
		 * can't rewrite one and not the other.
		 */
		if (groupByList != null)
		{
			groupByList.preprocess(numTables, fromList, whereSubquerys, wherePredicates);
		}
		
		if (havingClause != null) {
		    // DERBY-3257 
		    // Mark  subqueries that are part of the having clause as 
		    // such so we can avoid flattenning later. Having subqueries
		    // cannot be flattened because we cannot currently handle
		    // column references at the same source level.
		    // DERBY-3257 required we normalize the having clause which
		    // triggered flattening because SubqueryNode.underTopAndNode
		    // became true after normalization.  We needed another way to
		    // turn flattening off. Perhaps the long term solution is
		    // to avoid this restriction all together but that was beyond
		    // the scope of this bugfix.
		    havingSubquerys.markHavingSubqueries();
		    havingClause = havingClause.preprocess(
					numTables, fromList, havingSubquerys, wherePredicates);
		}
		
		/* Pull apart the expression trees */
		if (whereClause != null)
		{
			wherePredicates.pullExpressions(numTables, whereClause);
			whereClause = null;
		}

		/* RESOLVE - Where should we worry about expression pull up for
		 * expensive predicates?
		 */

		// Flatten any flattenable FromSubquerys or JoinNodes
		fromList.flattenFromTables(resultColumns, 
								   wherePredicates, 
								   whereSubquerys,
                                   groupByList,
                                   havingClause);

		if (wherePredicates != null && wherePredicates.size() > 0 && fromList.size() > 0)
		{
			// Perform various forms of transitive closure on wherePredicates
			if (fromList.size() > 1)
			{
				performTransitiveClosure(numTables);
			}

			if (orderByList != null)
			{
				// Remove constant columns from order by list.  Constant
				// columns are ones that have equality comparisons with
				// constant expressions (e.g. x = 3)
				orderByList.removeConstantColumns(wherePredicates);
				/*
				** It's possible for the order by list to shrink to nothing
				** as a result of removing constant columns.  If this happens,
				** get rid of the list entirely.
				*/
				if (orderByList.size() == 0)
				{
					orderByList = null;
                    resultColumns.removeOrderByColumns();
				}
			}
		}

		/* A valid group by without any aggregates or a having clause
		 * is equivalent to a distinct without the group by.  We do the transformation
		 * in order to simplify the group by code.
		 */
		if (groupByList != null &&
			havingClause == null &&
			selectAggregates.size() == 0 &&
			whereAggregates.size() == 0)
		{
			isDistinct = true;
			groupByList = null;
			wasGroupBy = true;
		}

		/* Consider distinct elimination based on a uniqueness condition.
		 * In order to do this:
		 *	o  All top level ColumnReferences in the select list are
		 *	   from the same base table.  (select t1.c1, t2.c2 + t3.c3 is 
		 *	   okay - t1 is the table of interest.)
		 *  o  If the from list is a single table then the columns in the
		 *	   select list plus the columns in the where clause that are
		 *	   in = comparisons with constants or parameters must be a
		 *	   superset of any unique index.
		 *  o  If the from list has multiple tables then at least 1 table
		 *	   meet the following - the set of columns in = comparisons
		 *	   with constants or parameters is a superset of any unique
		 *	   index on the table.  All of the other tables must meet
		 *	   the following - the set of columns in = comparisons with
		 *	   constants, parameters or join columns is a superset of
		 *	   any unique index on the table.  If the table from which
		 *	   the columns in the select list are coming from is in this
		 *     later group then the rule for it is to also include
		 *     the columns in the select list in the set of columns that
		 *     needs to be a superset of the unique index.  Whew!
		 */
		if (isDistinct && groupByList == null)
		{
			int distinctTable =	resultColumns.allTopCRsFromSameTable();
			
			if (distinctTable != -1)
			{
				if (fromList.returnsAtMostSingleRow(resultColumns, 
											   whereClause, wherePredicates,
											   getDataDictionary()))
				{
					isDistinct = false;
				}
			}

			/* If we were unable to eliminate the distinct and we have
			 * an order by then we can consider eliminating the sort for the
			 * order by.  All of the columns in the order by list must
			 * be ascending in order to do this.  There are 2 cases:
			 *	o	The order by list is an in order prefix of the columns
			 *		in the select list.  In this case the output of the
			 *		sort from the distinct will be in the right order
			 *		so we simply eliminate the order by list.
			 *	o	The order by list is a subset of the columns in the
			 *		the select list.  In this case we need to reorder the
			 *		columns in the select list so that the ordering columns
			 *		are an in order prefix of the select list and put a PRN
			 *		above the select so that the shape of the result set
			 *		is as expected.
			 */
			if (isDistinct && orderByList != null && orderByList.allAscending())
			{
				/* Order by list currently restricted to columns in select
				 * list, so we will always eliminate the order by here.
				 */
				if (orderByList.isInOrderPrefix(resultColumns))
				{
					orderByList = null;
				}
				else 
				{
					/* Order by list is not an in order prefix of the select list
					 * so we must reorder the columns in the the select list to
					 * match the order by list and generate the PRN above us to
					 * preserve the expected order.
					 */
					newTop = genProjectRestrictForReordering();
 					orderByList.resetToSourceRCs();
					resultColumns = orderByList.reorderRCL(resultColumns);
					newTop.getResultColumns().removeOrderByColumns();
					orderByList = null;
				}
				orderByAndDistinctMerged = true;
			}
		}

		/*
		 * Push predicates that are pushable.
		 *
		 * NOTE: We pass the wherePredicates down to the new PRNs here,
		 * so they can pull any clauses and possibily push them down further.
		 * NOTE: We wait until all of the FromTables have been preprocessed
		 * until we attempt to push down predicates, because we cannot push down
		 * a predicate if the immediate source of any of its column references
		 * is not a ColumnReference or a VirtualColumnNode.
		 */
		fromList.pushPredicates(wherePredicates);

		/* Set up the referenced table map */
		referencedTableMap = new JBitSet(numTables);
		int flSize = fromList.size();
		for (int index = 0; index < flSize; index++)
		{
			referencedTableMap.or(((FromTable) fromList.elementAt(index)).
													getReferencedTableMap());
		}

		/* Copy the referenced table map to the new tree top, if necessary */
		if (newTop != this)
		{
			newTop.setReferencedTableMap((JBitSet) referencedTableMap.clone());
		}


		if (orderByList != null) {

			// Collect window function calls and in-lined window definitions
			// contained in them from the orderByList.

			CollectNodesVisitor cnvw =
				new CollectNodesVisitor(WindowFunctionNode.class);
			orderByList.accept(cnvw);
			Vector wfcInOrderBy = cnvw.getList();

			for (int i=0; i < wfcInOrderBy.size(); i++) {
				WindowFunctionNode wfn =
					(WindowFunctionNode)wfcInOrderBy.elementAt(i);
				windowFuncCalls.add(wfn);


				if (wfn.getWindow() instanceof WindowDefinitionNode) {
					// Window function call contains an inline definition, add
					// it to our list of windows.
					windows = addInlinedWindowDefinition(windows, wfn);

				} else {
					// a window reference, should be bound already.

					if (SanityManager.DEBUG) {
						SanityManager.ASSERT(
							false,
							"a window reference, should be bound already");
					}
				}
			}
		}


		return newTop;
	}

	/**
	 * Peform the various types of transitive closure on the where clause.
	 * The 2 types are transitive closure on join clauses and on search clauses.
	 * Join clauses will be processed first to maximize benefit for search clauses.
	 *
	 * @param numTables		The number of tables in the query
	 *
	 * @exception StandardException		Thrown on error
	 */
	private void performTransitiveClosure(int numTables)
		throws StandardException
	{
		// Join clauses
		wherePredicates.joinClauseTransitiveClosure(numTables, fromList, getCompilerContext());

		// Search clauses
		wherePredicates.searchClauseTransitiveClosure(numTables, fromList.hashJoinSpecified());
	}

	/** Put the expression trees in conjunctive normal form 
	 *
     * @param boolClause clause to normalize
     * 
	 * @exception StandardException		Thrown on error
	 */
	private ValueNode normExpressions(ValueNode boolClause)
				throws StandardException
	{
		/* For each expression tree:
		 *	o Eliminate NOTs (eliminateNots())
		 *	o Ensure that there is an AndNode on top of every
		 *	  top level expression. (putAndsOnTop())
		 *	o Finish the job (changeToCNF())
		 */
		if (boolClause != null)
		{
			boolClause = boolClause.eliminateNots(false);
			if (SanityManager.DEBUG)
			{
				if (!(boolClause.verifyEliminateNots()) )
				{
					boolClause.treePrint();
					SanityManager.THROWASSERT(
						"boolClause in invalid form: " + boolClause); 
				}
			}
			boolClause = boolClause.putAndsOnTop();
			if (SanityManager.DEBUG)
			{
				if (! ((boolClause instanceof AndNode) &&
					   (boolClause.verifyPutAndsOnTop())) )
				{
					boolClause.treePrint();
					SanityManager.THROWASSERT(
						"boolClause in invalid form: " + boolClause); 
				}
			}
			boolClause = boolClause.changeToCNF(true);
			if (SanityManager.DEBUG)
			{
				if (! ((boolClause instanceof AndNode) &&
					   (boolClause.verifyChangeToCNF())) )
				{
					boolClause.treePrint();
					SanityManager.THROWASSERT(
						"boolClause in invalid form: " + boolClause); 
				}
			}
		}

		return boolClause;
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
	public ResultSetNode addNewPredicate(Predicate predicate)
			throws StandardException
	{
		wherePredicates.addPredicate(predicate);
		return this;
	}

	/**
	 * Evaluate whether or not the subquery in a FromSubquery is flattenable.  
	 * Currently, a FSqry is flattenable if all of the following are true:
	 *		o  Subquery is a SelectNode. (ie, not a RowResultSetNode or a UnionNode)
	 *		o  It contains a single table in its FROM list.
	 *		o  It contains no subqueries in the SELECT list.
	 *		o  It does not contain a group by or having clause
	 *		o  It does not contain aggregates.
	 *		o  It is not a DISTINCT.
	 *      o  It does not have an ORDER BY clause (pushed from FromSubquery).
	 *
	 * @param fromList	The outer from list
	 *
	 * @return boolean	Whether or not the FromSubquery is flattenable.
	 */
	public boolean flattenableInFromSubquery(FromList fromList)
	{
		if (isDistinct) 
		{
			return false;
		}
		if (this.fromList.size() > 1)
		{
			return false;
		}

		/* Don't flatten (at least for now) if selectNode's SELECT list contains a subquery */
		if ((selectSubquerys != null) && 
			 (selectSubquerys.size() > 0))
		{
			return false;
		}

		/* Don't flatten if selectNode contains a group by or having clause */
		if ((groupByList != null) || (havingClause != null))
		{
			return false;
		}

		/* Don't flatten if select list contains something that isn't cloneable.
		 */
		if (! resultColumns.isCloneable())
		{
			return false;
		}

		/* Don't flatten if selectNode contains an aggregate */
		if ((selectAggregates != null) && 
			 (selectAggregates.size() > 0))
		{
			return false;
		}

		/* Don't flatten if selectNode now has an order by */
		if ((orderByList != null) &&
			 (orderByList.size() > 0))
		{
			return false;
		}

        /* Don't flatten if selectNode has OFFSET or FETCH */
        if ((offset     != null) ||
            (fetchFirst != null))
        {
            return false;
        }

		return true;
	}

	/**
	 * Replace this SelectNode with a ProjectRestrictNode,
	 * since it has served its purpose.
	 *
	 * @param origFromListSize	The size of the original FROM list, before
	 *							generation of join tree.
	 * @return ResultSetNode	new ResultSetNode atop the query tree.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ResultSetNode genProjectRestrict(int origFromListSize)
				throws StandardException
	{
		boolean				eliminateSort = false;
		PredicateList		restrictionList;
		ResultColumnList	prRCList;
		ResultSetNode		prnRSN;

		prnRSN = (ResultSetNode) getNodeFactory().getNode(
								C_NodeTypes.PROJECT_RESTRICT_NODE,
								fromList.elementAt(0),	/* Child ResultSet */
								resultColumns,		/* Projection */
								whereClause,			/* Restriction */
								wherePredicates,/* Restriction as PredicateList */
								selectSubquerys,/* Subquerys in Projection */
								whereSubquerys,	/* Subquerys in Restriction */
								null,
								getContextManager()	 );

		/*
		** If we have aggregates OR a select list we want
		** to generate a GroupByNode.  In the case of a
		** scalar aggregate we have no grouping columns.
		**
		** JRESOLVE: what about correlated aggregates from another
		** block.
		*/ 
		if (((selectAggregates != null) && (selectAggregates.size() > 0)) 
			|| (groupByList != null))
		{
			Vector aggs = selectAggregates;
			if (havingAggregates != null && !havingAggregates.isEmpty()) {
				havingAggregates.addAll(selectAggregates);
				aggs = havingAggregates;
			}
			GroupByNode gbn = (GroupByNode) getNodeFactory().getNode(
												C_NodeTypes.GROUP_BY_NODE,
												prnRSN,
												groupByList,
												aggs,
												havingClause,
												havingSubquerys,
												null,
												new Integer(nestingLevel),
												getContextManager());
			gbn.considerPostOptimizeOptimizations(originalWhereClause != null);
			gbn.assignCostEstimate(optimizer.getOptimizedCost());

			groupByList = null;
			prnRSN  = gbn.getParent();

			// Remember whether or not we can eliminate the sort.
			eliminateSort = eliminateSort || gbn.getIsInSortedOrder();
		}


		if (windows != null) {

			// Now we add a window result set wrapped in a PRN on top of what
			// we currently have.

			if (windows.size() > 1) {
				throw StandardException.newException(
					SQLState.LANG_WINDOW_LIMIT_EXCEEDED);
			}

			WindowNode wn = (WindowNode)windows.elementAt(0);

			WindowResultSetNode wrsn =
				(WindowResultSetNode)getNodeFactory().getNode(
					C_NodeTypes.WINDOW_RESULTSET_NODE,
					prnRSN,
					wn,
					windowFuncCalls,
					new Integer(nestingLevel),
					getContextManager());

			prnRSN = wrsn.getParent();
			wrsn.assignCostEstimate(optimizer.getOptimizedCost());
		}


		// if it is distinct, that must also be taken care of.
		if (isDistinct)
		{
			// We first verify that a distinct is valid on the
			// RCL.
			resultColumns.verifyAllOrderable();

			/* See if we can push duplicate elimination into the store
			 * via a hash scan.  This is possible iff:
			 *	o  A single table query
			 *	o  We haven't merged the order by and distinct sorts.
			 *	   (Results do not have to be in a particular order.)
			 *	o  All entries in the select's RCL are ColumnReferences.
			 *	o  No predicates (This is because we currently do not
			 *	   differentiate between columns referenced in the select
			 *	   list and columns referenced in other clauses.  In other
			 *	   words, the store will do duplicate elimination based on
			 *	   all referenced columns.)
			 *	   RESOLVE - We can change this to be all referenced columns
			 *	   have to be in the select list.  In that case, we need to
			 *	   refine which predicates are allowed.  Basically, all predicates
			 *	   must have been pushed down to the index/table scan.(If we make
			 *	   this change, then we need to verify that non of the columns in
			 *	   the predicates are correlated columns.)
			 *	o  NOTE: The implementation of isPossibleDistinctScan() will return
			 *	   false if there is an IndexRowToBaseRow above the 
			 *	   FromBaseTable.  This is because all of a table's columns must come
			 *	   from the same conglomerate in order to get consistent data.
			 */
			boolean distinctScanPossible = false;
			if (origFromListSize == 1 && !orderByAndDistinctMerged)
			{
				boolean simpleColumns = true;
				HashSet distinctColumns = new HashSet();
				int size = resultColumns.size();
				for (int i = 1; i <= size; i++) {
					BaseColumnNode bc = resultColumns.getResultColumn(i).getBaseColumnNode();
					if (bc == null) {
						simpleColumns = false;
						break;
					}
					distinctColumns.add(bc);
				}
				if (simpleColumns && prnRSN.isPossibleDistinctScan(distinctColumns)) {
					prnRSN.markForDistinctScan();
					distinctScanPossible = true;
				}
			}

			if (!distinctScanPossible)
			{
				/* We can't do a distinct scan. Determine if we can filter out 
				 * duplicates without a sorter. 
				 */
				boolean inSortedOrder = isOrderedResult(resultColumns, prnRSN, !(orderByAndDistinctMerged));
				prnRSN = (ResultSetNode) getNodeFactory().getNode(
											C_NodeTypes.DISTINCT_NODE,
											prnRSN,
											new Boolean(inSortedOrder),
											null,
											getContextManager());
				prnRSN.costEstimate = costEstimate.cloneMe();

				// Remember whether or not we can eliminate the sort.
				eliminateSort = eliminateSort || inSortedOrder;
			}
		}

		/* Generate the OrderByNode if a sort is still required for
		 * the order by.
		 */

		if (orderByList != null)
		{
			if (orderByList.getSortNeeded())
			{
				prnRSN = (ResultSetNode) getNodeFactory().getNode(
												C_NodeTypes.ORDER_BY_NODE,
												prnRSN,
												orderByList,
												null,
												getContextManager());
				prnRSN.costEstimate = costEstimate.cloneMe();
			}

			// There may be columns added to the select projection list
			// a query like:
			// select a, b from t group by a,b order by a+b
			// the expr a+b is added to the select list.  
			int orderBySelect = this.getResultColumns().getOrderBySelect();
			if (orderBySelect > 0)
			{
				// Keep the same RCL on top, since there may be references to
				// its result columns above us, i.e. in this query:
                //
				// select sum(j),i from t group by i having i
				//             in (select i from t order by j)
				//
				ResultColumnList topList = prnRSN.getResultColumns();
				ResultColumnList newSelectList = topList.copyListAndObjects();
				prnRSN.setResultColumns(newSelectList);

				topList.removeOrderByColumns();
				topList.genVirtualColumnNodes(prnRSN, newSelectList);
				prnRSN = (ResultSetNode) getNodeFactory().getNode(
								C_NodeTypes.PROJECT_RESTRICT_NODE,
								prnRSN,
								topList,
								null,
								null,
								null,
								null,
								null,
								getContextManager());
			}
		}

        if (offset != null || fetchFirst != null) {
            // Keep the same RCL on top, since there may be references to
            // its result columns above us.
            ResultColumnList topList = prnRSN.getResultColumns();
            ResultColumnList newSelectList = topList.copyListAndObjects();
            prnRSN.setResultColumns(newSelectList);
            topList.genVirtualColumnNodes(prnRSN, newSelectList);
            prnRSN = (RowCountNode)getNodeFactory().getNode(
                C_NodeTypes.ROW_COUNT_NODE,
                prnRSN,
                topList,
                offset,
                fetchFirst,
                getContextManager());
        }


		if (wasGroupBy &&
			resultColumns.numGeneratedColumnsForGroupBy() > 0 &&
			windows == null) // windows handling already added a PRN which
							 // obviates this.
		{
			// This case takes care of columns generated for group by's which 
			// will need to be removed from the final projection. Note that the
			// GroupByNode does remove generated columns but in certain cases
			// we dispense with a group by and replace it with a distinct instead.
			// So in a query like:
			// select c1 from t group by c1, c2
			// we would have added c2 to the projection list which will have to be 
			// projected out.
			//

			// Keep the same RCL on top, since there may be
			// references to its result columns above us, e.g. in this query:
			//
			// select sum(j),i from t group by i having i
			//             in (select i from t group by i,j )
			//
			ResultColumnList topList = prnRSN.getResultColumns();
			ResultColumnList newSelectList = topList.copyListAndObjects();
			prnRSN.setResultColumns(newSelectList);

			topList.removeGeneratedGroupingColumns();
			topList.genVirtualColumnNodes(prnRSN, newSelectList);
			prnRSN = (ResultSetNode) getNodeFactory().getNode(
						C_NodeTypes.PROJECT_RESTRICT_NODE,
						prnRSN,
						topList,
						null,
						null,
						null,
						null,
						null,
						getContextManager());
		}

		if (!(orderByList != null && orderByList.getSortNeeded()) && orderByQuery)
		{
			// Remember whether or not we can eliminate the sort.
			eliminateSort = true;
		}

		/* If we were able to eliminate the sort during optimization then
		 * we must tell the underlying tree.  At minimum, this means no
		 * group fetch on an index under an IndexRowToBaseRow since that
		 * that could lead to incorrect results.  (Bug 2347.)
		 */
		if (eliminateSort)
		{
			prnRSN.adjustForSortElimination(orderByList);
		}

		/* Set the cost of this node in the generated node */
		prnRSN.costEstimate = costEstimate.cloneMe();

		return prnRSN;
	}

	/**
	 * Is the result of this node an ordered result set.  An ordered result set
	 * means that the results from this node will come in a known sorted order.
	 * This means that the data is ordered according to the order of the elements in the RCL.
	 * Today, the data is considered ordered if:
	 *		o The RCL is composed entirely of CRs or ConstantNodes
	 *		o The underlying tree is ordered on the CRs in the order in which
	 *			they appear in the RCL, taking equality predicates into account.
	 * Future Enhancements:
	 *		o The prefix will not be required to be in order.  (We will need to 
	 *		  reorder the RCL and generate a PRN with an RCL in the expected order.)
	 *
	 * @return boolean	Whether or not this node returns an ordered result set.
	 *
	 * @exception StandardException		Thrown on error
	 */
	private boolean isOrderedResult(ResultColumnList resultColumns,
										  ResultSetNode newTopRSN,
										  boolean permuteOrdering)
		throws StandardException
	{
		int rclSize = resultColumns.size();

		/* Not ordered if RCL contains anything other than a ColumnReference
		 * or a ConstantNode.
		 */
		int numCRs = 0;
		for (int index = 0; index < rclSize; index++)
		{
			ResultColumn rc = (ResultColumn) resultColumns.elementAt(index);
			if (rc.getExpression() instanceof ColumnReference)
			{
				numCRs++;
			}
			else if (! (rc.getExpression() instanceof ConstantNode))
			{
				return false;
			}
		}

		// Corner case, all constants
		if (numCRs == 0)
		{
			return true;
		}

		ColumnReference[] crs = new ColumnReference[numCRs];

		// Now populate the CR array and see if ordered
		int crsIndex = 0;
		for (int index = 0; index < rclSize; index++)
		{
			ResultColumn rc = (ResultColumn) resultColumns.elementAt(index);
			if (rc.getExpression() instanceof ColumnReference)
			{
				crs[crsIndex++] = (ColumnReference) rc.getExpression();
			}
		}

		return newTopRSN.isOrderedOn(crs, permuteOrdering, (Vector)null);
	}

	/**
	 * Ensure that the top of the RSN tree has a PredicateList.
	 *
	 * @param numTables			The number of tables in the query.
	 * @return ResultSetNode	A RSN tree with a node which has a PredicateList on top.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ResultSetNode ensurePredicateList(int numTables) 
		throws StandardException
	{
		return this;
	}

	/**
	 * Optimize this SelectNode.  This means choosing the best access path
	 * for each table, among other things.
	 *
	 * @param dataDictionary	The DataDictionary to use for optimization
	 * @param predicateList		The predicate list to optimize against
	 * @param outerRows			The number of outer joining rows
	 *
	 * @return	ResultSetNode	The top of the optimized tree
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ResultSetNode optimize(DataDictionary dataDictionary,
								  PredicateList	predicateList,
								  double outerRows) 
				throws StandardException
	{
		Optimizer		 optimizer;

		/* Optimize any subquerys before optimizing the underlying result set */

		/* selectSubquerys is always allocated at bind() time */
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(selectSubquerys != null,
			"selectSubquerys is expected to be non-null");

		/* If this select node is the child of an outer node that is
		 * being optimized, we can get here multiple times (once for
		 * every permutation that is done for the outer node).  With
		 * DERBY-805, we can add optimizable predicates to the WHERE
		 * list as part of this method; thus, before proceeding we
		 * need go through and remove any opt predicates that we added
		 * to our WHERE list the last time we were here; if we don't
		 * do that, we'll end up with the same predicates in our
		 * WHERE list multiple times, which can lead to incorrect
		 * optimization.
		 */

		if (wherePredicates != null)
		{
			// Iterate backwards because we might be deleting entries.
			for (int i = wherePredicates.size() - 1; i >= 0; i--)
			{
				if (((Predicate)wherePredicates.elementAt(i)).isScopedForPush())
					wherePredicates.removeOptPredicate(i);
			}
		}

		/* Get a new optimizer */

		/* With DERBY-805 we take any optimizable predicates that
		 * were pushed into this node and we add them to the list of
		 * predicates that we pass to the optimizer, thus allowing
		 * the optimizer to use them when choosing an access path
		 * for this SELECT node.  We do that by adding the predicates
		 * to our WHERE list, since the WHERE predicate list is what
		 * we pass to the optimizer for this select node (see below).
		 * We have to pass the WHERE list directly (as opposed to
		 * passing a copy) because the optimizer is only created one
		 * time; it then uses the list we pass it for the rest of the
		 * optimization phase and finally for "modifyAccessPaths()".
		 * Since the optimizer can update/modify the list based on the
		 * WHERE predicates (such as by adding internal predicates or
		 * by modifying the actual predicates themselves), we need
		 * those changes to be applied to the WHERE list directly for
		 * subsequent processing (esp. for modification of the access
		 * path).  Note that by adding outer opt predicates directly
		 * to the WHERE list, we're changing the semantics of this
		 * SELECT node.  This is only temporary, though--once the
		 * optimizer is done with all of its work, any predicates
		 * that were pushed here will have been pushed even further
		 * down and thus will have been removed from the WHERE list
		 * (if it's not possible to push them further down, then they
		 * shouldn't have made it this far to begin with).
		 */
		if (predicateList != null)
		{
			if (wherePredicates == null) {
				wherePredicates = (PredicateList) getNodeFactory().getNode(
						C_NodeTypes.PREDICATE_LIST,
						getContextManager());
			}

			Predicate pred = null;
			int sz = predicateList.size();
			for (int i = sz - 1; i >= 0; i--)
			{
				// We can tell if a predicate was pushed into this select
				// node because it will have been "scoped" for this node
				// or for some result set below this one.
				pred = (Predicate)predicateList.getOptPredicate(i);
				if (pred.isScopedToSourceResultSet())
				{
					// If we're pushing the predicate down here, we have to
					// remove it from the predicate list of the node above
					// this select, in order to keep in line with established
					// push 'protocol'.
					wherePredicates.addOptPredicate(pred);
					predicateList.removeOptPredicate(pred);
				}
			}
		}

		optimizer = getOptimizer(fromList,
								wherePredicates,
								dataDictionary,
								orderByList);
		optimizer.setOuterRows(outerRows);

		/* Optimize this SelectNode */
		while (optimizer.getNextPermutation())
		{
			while (optimizer.getNextDecoratedPermutation())
			{
				optimizer.costPermutation();
			}
		}

		/* When we're done optimizing, any scoped predicates that
		 * we pushed down the tree should now be sitting again
		 * in our wherePredicates list.  Put those back in the
		 * the list from which we received them, to allow them
		 * to be "pulled" back up to where they came from.
		 */
		if (wherePredicates != null)
		{
			Predicate pred = null;
			for (int i = wherePredicates.size() - 1; i >= 0; i--)
			{
				pred = (Predicate)wherePredicates.getOptPredicate(i);
				if (pred.isScopedForPush())
				{
					predicateList.addOptPredicate(pred);
					wherePredicates.removeOptPredicate(pred);
				}
			}
		}

		/* Get the cost */
		costEstimate = optimizer.getOptimizedCost();

		/* Update row counts if this is a scalar aggregate */
		if ((selectAggregates != null) && (selectAggregates.size() > 0)) 
		{
			costEstimate.setEstimatedRowCount((long) outerRows);
			costEstimate.setSingleScanRowCount(1);
		}

		selectSubquerys.optimize(dataDictionary, costEstimate.rowCount());

		if (whereSubquerys != null && whereSubquerys.size() > 0)
		{
			whereSubquerys.optimize(dataDictionary, costEstimate.rowCount());
		}
		
		if (havingSubquerys != null && havingSubquerys.size() > 0) {
			havingSubquerys.optimize(dataDictionary, costEstimate.rowCount());
		}

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
	public ResultSetNode modifyAccessPaths(PredicateList predList)
		throws StandardException
	{
		// Take the received list of predicates and propagate them to the
		// predicate list for this node's optimizer.  Then, when we call
		// optimizer.modifyAccessPaths(), the optimizer will have the
		// predicates and can push them down as necessary, according
		// the join order that it has chosen.

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(optimizer != null,
				"SelectNode's optimizer not expected to be null when " +
				"modifying access paths.");
		}

		((OptimizerImpl)optimizer).addScopedPredicatesToList(predList);
		return modifyAccessPaths();
	}

	/**
	 * Modify the access paths according to the choices the optimizer made.
	 *
	 * @return	A QueryTree with the necessary modifications made
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ResultSetNode modifyAccessPaths() throws StandardException
	{
		int				 origFromListSize = fromList.size();
		ResultColumnList leftRCList;
		ResultColumnList rightRCList;
		ResultSetNode	 leftResultSet;
		ResultSetNode	 rightResultSet;

		/*
		** Modify the access path for each Optimizable, as necessary
		**
		** This should be the same optimizer we got above.
		*/
		optimizer.modifyAccessPaths();

		// Load the costEstimate for the final "best" join order.
		costEstimate = optimizer.getFinalCost();

		if (SanityManager.DEBUG)
		{
			// When we optimized this select node, we may have added pushable
			// outer predicates to the wherePredicates list for this node
			// (see the optimize() method above).  When we did so, we said
			// that all such predicates should have been removed from the
			// where list by the time optimization was completed.   So we
			// check that here, just to be safe.  NOTE: We do this _after_
			// calling optimizer.modifyAccessPaths(), because it's only in
			// that call that the scoped predicates are officially pushed
			// and thus removed from the list.
			if (wherePredicates != null)
			{
				Predicate pred = null;
				for (int i = wherePredicates.size() - 1; i >= 0; i--)
				{
					pred = (Predicate)wherePredicates.getOptPredicate(i);
					if (pred.isScopedForPush())
					{
						SanityManager.THROWASSERT("Found scoped predicate " +
							pred.binaryRelOpColRefsToString() +
							" in WHERE list when no scoped predicates were" +
							" expected.");
					}
				}
			}
		}

		selectSubquerys.modifyAccessPaths();

		if (whereSubquerys != null && whereSubquerys.size() > 0)
		{
			whereSubquerys.modifyAccessPaths();
		}
		
		if (havingSubquerys != null && havingSubquerys.size() > 0) {
			havingSubquerys.modifyAccessPaths();
		}

		/* Build a temp copy of the current FromList for sort elimination, etc. */
		preJoinFL.removeAllElements();
		preJoinFL.nondestructiveAppend(fromList);

		/* Now we build a JoinNode tree from the bottom up until there is only
		 * a single entry in the fromList and that entry points to the top of
		 * the JoinNode tree.
		 *
		 * While there is still more than 1 entry in the list, create a JoinNode
		 * which points to the 1st 2 entries in the list.  This JoinNode becomes 
		 * the new 1st entry in the list and the 2nd entry is deleted.  The
		 * old 1st and 2nd entries will get shallow copies of their 
		 * ResultColumnLists.  The JoinNode's ResultColumnList will be the
		 * concatenation of the originals from the old 1st and 2nd entries.
		 * The virtualColumnIds will be updated to reflect there new positions
		 * and each ResultColumn.expression will be replaced with a new
		 * VirtualColumnNode.
		 */
		while (fromList.size() > 1)
		{
			/* Get left's ResultColumnList, assign shallow copy back to it
			 * and create new VirtualColumnNodes for the original's 
			 * ResultColumn.expressions.
			 */
			leftResultSet = (ResultSetNode) fromList.elementAt(0);
			leftRCList = leftResultSet.getResultColumns();
			leftResultSet.setResultColumns(leftRCList.copyListAndObjects());
			leftRCList.genVirtualColumnNodes(leftResultSet, leftResultSet.resultColumns);

			/* Get right's ResultColumnList, assign shallow copy back to it,
			 * create new VirtualColumnNodes for the original's 
			 * ResultColumn.expressions and increment the virtualColumnIds.
			 * (Right gets appended to left, so only right's ids need updating.)
			 */
			rightResultSet = (ResultSetNode) fromList.elementAt(1);
			rightRCList = rightResultSet.getResultColumns();
			rightResultSet.setResultColumns(rightRCList.copyListAndObjects());
			rightRCList.genVirtualColumnNodes(rightResultSet, rightResultSet.resultColumns);
			rightRCList.adjustVirtualColumnIds(leftRCList.size());

			/* Concatenate the 2 ResultColumnLists */
			leftRCList.nondestructiveAppend(rightRCList);

			/* Now we're finally ready to generate the JoinNode and have it
			 * replace the 1st 2 entries in the FromList.
			 */
			fromList.setElementAt(
						 (JoinNode) getNodeFactory().getNode(
												C_NodeTypes.JOIN_NODE,
												leftResultSet,
												rightResultSet,
												null,
												null,
												leftRCList,
												null,
												//user supplied optimizer overrides
												fromList.properties,
												getContextManager()
												),
							0
						);

			fromList.removeElementAt(1);
		}

		return genProjectRestrict(origFromListSize);
	}

	/**
	 * Get the final CostEstimate for this SelectNode.
	 *
	 * @return	The final CostEstimate for this SelectNode, which is
	 * 			the final cost estimate for the best join order of
	 *          this SelectNode's optimizer.
	 */
	public CostEstimate getFinalCostEstimate()
		throws StandardException
	{
		return optimizer.getFinalCost();
	}

	/**
		Determine if this select is updatable or not, for a cursor.
	 */
	boolean isUpdatableCursor(DataDictionary dd) throws StandardException
	{
		TableDescriptor	targetTableDescriptor;

		if (isDistinct)
		{
			if (SanityManager.DEBUG)
			SanityManager.DEBUG("DumpUpdateCheck","cursor select has distinct");
			return false;
		}

		if ((selectAggregates == null) || (selectAggregates.size() > 0))
		{
			return false;
		}

		if (groupByList != null || havingClause != null)
		{
			return false;
		}

		if (SanityManager.DEBUG)
		SanityManager.ASSERT(fromList!=null, "select must have from tables");
		if (fromList.size() != 1)
		{
			if (SanityManager.DEBUG)
			SanityManager.DEBUG("DumpUpdateCheck","cursor select has more than one from table");
			return false;
		}

		targetTable = (FromTable)(fromList.elementAt(0));

		if (targetTable instanceof FromVTI) {

			return ((FromVTI) targetTable).isUpdatableCursor();
		}

		if (! (targetTable instanceof FromBaseTable))
		{
			if (SanityManager.DEBUG)
			SanityManager.DEBUG("DumpUpdateCheck","cursor select has non base table as target table");
			return false;
		}


 		/* Get the TableDescriptor and verify that it is not for a
 		 * view or a system table.  
 		 * NOTE: We need to use the base table name for the table.
 		 *		 Simplest way to get it is from a FromBaseTable.  We
 		 *		 know that targetTable is a FromBaseTable because of check
 		 *		 just above us.
		 * NOTE: We also need to use the base table's schema name; otherwise
		 *		we will think it is the default schema Beetle 4417
 		 */
 		targetTableDescriptor = getTableDescriptor(
 						((FromBaseTable)targetTable).getBaseTableName(), 
 			getSchemaDescriptor(((FromBaseTable)targetTable).getTableNameField().getSchemaName()));
 		if (targetTableDescriptor.getTableType() == TableDescriptor.SYSTEM_TABLE_TYPE)
 		{
 			if (SanityManager.DEBUG)
 			SanityManager.DEBUG("DumpUpdateCheck","cursor select is on system table");
 			return false;
 		}
 		if (targetTableDescriptor.getTableType() == TableDescriptor.VIEW_TYPE)
 		{
 			if (SanityManager.DEBUG)
 			SanityManager.DEBUG("DumpUpdateCheck","cursor select is on view");
			return false;
		}
		if ((getSelectSubquerys() != null) &&
			(getSelectSubquerys().size() != 0))
		{
			if (SanityManager.DEBUG)
			SanityManager.DEBUG("DumpUpdateCheck","cursor select has subquery in SELECT list");
			return false;
		}

		if ((getWhereSubquerys() != null) &&
			(getWhereSubquerys().size() != 0))
		{
			if (SanityManager.DEBUG)
			SanityManager.DEBUG("DumpUpdateCheck","cursor select has subquery in WHERE clause");
			return false;
		}

		return true;
	}

	/**
		Assumes that isCursorUpdatable has been called, and that it
		is only called for updatable cursors.
	 */
	FromTable getCursorTargetTable()
	{
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(targetTable!=null,
			"must call isUpdatableCursor() first, and must be updatable");
		return targetTable;
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
	public boolean referencesTarget(String name, boolean baseTable)
		throws StandardException
	{
		if (fromList.referencesTarget(name, baseTable) ||
			(selectSubquerys != null && selectSubquerys.referencesTarget(name, baseTable)) ||
			(whereSubquerys != null && whereSubquerys.referencesTarget(name, baseTable))
		   )
		{
			return true;
		}
		return false;
	}

	/**
	 * Return whether or not this ResultSetNode contains a subquery with a
	 * reference to the specified target table.
	 * 
	 * @param name	The table name.
	 * @param baseTable	Whether or not table is a base table.
	 *
	 * @return boolean	Whether or not a reference to the table was found.
	 *
	 * @exception StandardException		Thrown on error
	 */
	boolean subqueryReferencesTarget(String name, boolean baseTable)
		throws StandardException
	{
		if ((selectSubquerys != null && selectSubquerys.referencesTarget(name, baseTable)) ||
			(whereSubquerys != null && whereSubquerys.referencesTarget(name, baseTable))
		   )
		{
			return true;
		}
		return false;
	}

	/**
	 * Bind any untyped null nodes to the types in the given ResultColumnList.
	 *
	 * @param bindingRCL	The ResultColumnList with the types to bind to.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void bindUntypedNullsToResultColumns(ResultColumnList bindingRCL)
				throws StandardException
	{
		fromList.bindUntypedNullsToResultColumns(bindingRCL);
	}

	/**
	 * Decrement (query block) level (0-based) for 
	 * all of the tables in this ResultSet tree.
	 * This is useful when flattening a subquery.
	 *
	 * @param decrement	The amount to decrement by.
	 */
	void decrementLevel(int decrement)
	{
		/* Decrement the level in the tables */
		fromList.decrementLevel(decrement);
		selectSubquerys.decrementLevel(decrement);
		whereSubquerys.decrementLevel(decrement);
		/* Decrement the level in any CRs in predicates
		 * that are interesting to transitive closure.
		 */
		wherePredicates.decrementLevel(fromList, decrement);
	}
	
	/**
	 * Determine whether or not this subquery,
	 * the SelectNode is in a subquery, can be flattened
	 * into the outer query block based on a uniqueness condition.
	 * A uniqueness condition exists when we can guarantee
	 * that at most 1 row will qualify in each table in the
	 * subquery.  This is true if every table in the from list is
	 * (a base table and the set of columns from the table that
	 * are in equality comparisons with expressions that do not
	 * include a column from the same table is a superset of any unique index
	 * on the table) or an ExistsBaseTable.
	 *
	 * @param additionalEQ	Whether or not the column returned
	 *						by this select, if it is a ColumnReference,
	 *						is in an equality comparison.
	 *
	 * @return	Whether or not this subquery can be flattened based
	 *			on a uniqueness condition.
	 *
	 * @exception StandardException		Thrown on error
	 */
	boolean uniqueSubquery(boolean additionalEQ)
		throws StandardException
	{
		ColumnReference additionalCR = null;
		ResultColumn	rc = (ResultColumn) getResultColumns().elementAt(0);

		/* Figure out if we have an additional ColumnReference
		 * in an equality comparison.
		 */
		if (additionalEQ &&
			rc.getExpression() instanceof ColumnReference)
		{
			additionalCR = (ColumnReference) rc.getExpression();

			/* ColumnReference only interesting if it is 
			 * not correlated.
			 */
			if (additionalCR.getCorrelated())
			{
				additionalCR = null;
			}
		}

		return fromList.returnsAtMostSingleRow((additionalCR == null) ? null : getResultColumns(), 
											   whereClause, wherePredicates,
											   getDataDictionary());
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
	public int updateTargetLockMode()
	{
		/* Do row locking if there is a restriction */
		return fromList.updateTargetLockMode();
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
		return (groupByList == null && selectAggregates != null && selectAggregates.size() != 0);
	}

	/**
	 * Return true if the node references SESSION schema tables (temporary or permanent)
	 *
	 * @return	true if references SESSION schema tables, else false
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean referencesSessionSchema()
		throws StandardException
	{
		if (fromList.referencesSessionSchema() ||
			(selectSubquerys != null && selectSubquerys.referencesSessionSchema()) ||
			(whereSubquerys != null && whereSubquerys.referencesSessionSchema()))
					return true;

		return false;
	}

	/**
	 * Accept the visitor for all visitable children of this node.
	 * 
	 * @param v the visitor
	 *
	 * @exception StandardException on error
	 */
	void acceptChildren(Visitor v)
		throws StandardException
	{
		super.acceptChildren(v);

		if (fromList != null)
		{
			fromList = (FromList)fromList.accept(v);
		}

		if (whereClause != null)
		{
			whereClause = (ValueNode)whereClause.accept(v);
		}		

		if (wherePredicates != null)
		{
			wherePredicates = (PredicateList)wherePredicates.accept(v);
		}		

		if (havingClause != null) {
			havingClause = (ValueNode)havingClause.accept(v);
		}
	}

	/**
	 * @return true if there are aggregates in the select list.
	 */
	public boolean hasAggregatesInSelectList() 
	{
		return !selectAggregates.isEmpty();
	}

	/**
	 * Used by SubqueryNode to avoid flattening of a subquery if a window is
	 * defined on it. Note that any inline window definitions should have been
	 * collected from both the selectList and orderByList at the time this
	 * method is called, so the windows list is complete. This is true after
	 * preprocess is completed.
	 *
	 * @return true if this select node has any windows on it
	 */
	public boolean hasWindows()
	{
		return windows != null;
	}


	public static void checkNoWindowFunctions(QueryTreeNode clause,
											   String clauseName)
			throws StandardException {

		// Clause cannot contain window functions except inside subqueries
		HasNodeVisitor visitor = new HasNodeVisitor(WindowFunctionNode.class,
													SubqueryNode.class);
		clause.accept(visitor);

		if (visitor.hasNode()) {
			throw StandardException.newException(
				SQLState.LANG_WINDOW_FUNCTION_CONTEXT_ERROR,
				clauseName);
		}
	}

    /**
     * {@inheritDoc}
     *
     * A no-op for SelectNode.
     */
    void replaceOrForbidDefaults(TableDescriptor ttd,
                                 ResultColumnList tcl,
                                 boolean allowDefaults)
        throws StandardException
    {
    }
}
