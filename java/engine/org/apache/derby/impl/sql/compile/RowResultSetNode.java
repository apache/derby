/*

   Derby - Class org.apache.derby.impl.sql.compile.RowResultSetNode

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

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.OptimizableList;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizablePredicate;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.RequiredRowOrdering;
import org.apache.derby.iapi.sql.compile.RowOrdering;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.Row;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;
import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.store.access.Qualifier;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.util.JBitSet;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import java.util.Enumeration;
import java.util.Properties;
import java.util.Vector;

/**
 * A RowResultSetNode represents the result set for a VALUES clause.
 *
 * @author Jerry Brenner
 */

public class RowResultSetNode extends FromTable
{
	SubqueryList subquerys;
	Vector		 aggregateVector;
	OrderByList	 orderByList;

	/**
	 * Initializer for a RowResultSetNode.
	 *
	 * @param valuesClause	The result column list for the VALUES clause.
	 * @param tableProperties	Properties list associated with the table
	 */
	public void init(Object valuesClause, Object tableProperties)
	{
		super.init(null, tableProperties);
		resultColumns = (ResultColumnList) valuesClause;
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
			return 	"orderByList: " + 
				(orderByList != null ? orderByList.toString() : "null") + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

	public String statementToString()
	{
		return "VALUES";
	}

	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 *
	 * @return	Nothing
	 */

	public void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG)
		{
			super.printSubNodes(depth);

			if (subquerys != null)
			{
				printLabel(depth, "subquerys: ");
				subquerys.treePrint(depth + 1);
			}
		}
	}


	/*
	 *  Optimizable interface
	 */

	/**
	 * @see Optimizable#estimateCost
	 *
	 * @exception StandardException		Thrown on error
	 */
	public CostEstimate estimateCost(OptimizablePredicateList predList,
										ConglomerateDescriptor cd,
										CostEstimate outerCost,
										Optimizer optimizer,
										RowOrdering rowOrdering)
								throws StandardException
	{
		/*
		** Assume for now that the cost of a VALUES clause is zero, with one row
		** fetched.  Is this true, and if not, does it make a difference?
		** There's nothing to optimize in this case.
		*/
		if (costEstimate == null)
		{
			costEstimate = optimizer.newCostEstimate();
		}

		costEstimate.setCost(0.0d, 1.0d, 1.0d);

		/* A single row is always ordered */
		rowOrdering.optimizableAlwaysOrdered(this);

		return costEstimate;
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

	public ResultSetNode bindNonVTITables(DataDictionary dataDictionary, 
							FromList fromListParam) 
					throws StandardException
	{
		/* Assign the tableNumber */
		if (tableNumber == -1)  // allow re-bind, in which case use old number
			tableNumber = getCompilerContext().getNextTableNumber();

		/* VALUES clause has no tables, so nothing to do */
		return this;
	}

	/**
	 * Bind the expressions in this RowResultSetNode.  This means binding the
	 * sub-expressions, as well as figuring out what the return type is
	 * for each expression.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void bindExpressions(FromList fromListParam)
					throws StandardException
	{
		int nestingLevel;

		subquerys = (SubqueryList) getNodeFactory().getNode(
										C_NodeTypes.SUBQUERY_LIST,
										getContextManager());

		aggregateVector = new Vector();

		/* Verify that there are no DEFAULTs in the RCL.
		 * DEFAULT is only valid for an insert, and it has
		 * already been coverted into the tree by the time we get here.
		 * The grammar allows:
		 *		VALUES DEFAULT;
		 * so we need to check for that here and throw an exception if found.
		 */
		resultColumns.checkForInvalidDefaults();

		/* Believe it or not, a values clause can contain correlated column references
		 * and subqueries.  In order to get correlated column resolution working 
		 * correctly, we need to set our nesting level to be 1 deeper than the current
		 * level and push ourselves into the FROM list.  
		 */

		/* Set the nesting level in this node */
		if (fromListParam.size() == 0)
		{
			nestingLevel = 0;
		}
		else
		{
			nestingLevel = ((FromTable) fromListParam.elementAt(0)).getLevel() + 1;
		}
		setLevel(nestingLevel);
		fromListParam.insertElementAt(this, 0);
		resultColumns.bindExpressions(fromListParam, subquerys,
									  aggregateVector);
		// Pop ourselves back out of the FROM list
		fromListParam.removeElementAt(0);

		if (aggregateVector.size() > 0)
		{
			throw StandardException.newException(SQLState.LANG_NO_AGGREGATES_IN_WHERE_CLAUSE);
		}
	}

	/**
	 * Bind the expressions in this ResultSetNode if it has tables.  This means binding the
	 * sub-expressions, as well as figuring out what the return type is for
	 * each expression.
	 *
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void bindExpressionsWithTables(FromList fromListParam)
					throws StandardException
	{
		/* We don't have any tables, so just return */
		return;
	}

	/**
	 * Bind the expressions in the target list.  This means binding the
	 * sub-expressions, as well as figuring out what the return type is
	 * for each expression.  This is useful for EXISTS subqueries, where we
	 * need to validate the target list before blowing it away and replacing
	 * it with a SELECT true.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void bindTargetExpressions(FromList fromListParam)
					throws StandardException
	{
		bindExpressions(fromListParam);
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
		/*
		** If bindingRCL is null, then we are
		** under a cursor node that is inferring
		** its RCL from us.  It passes null to
		** get union to use both sides of the union
		** for the check.  Anyway, since there is
		** nothing under us but an RCL, just pass
		** in our RCL.
		*/
		if (bindingRCL == null)
			bindingRCL = resultColumns;

		resultColumns.bindUntypedNullsToResultColumns(bindingRCL);
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

	public ResultColumn getMatchingColumn(
						ColumnReference columnReference)
						throws StandardException
	{
		return null;
	}

	/**
	 * Get the exposed name for this table, which is the name that can
	 * be used to refer to it in the rest of the query.
	 *
	 * @return	The exposed name of this table.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public String getExposedName() throws StandardException
	{
		return null;
	}

	/**
	 * Verify that a SELECT * is valid for this type of subquery.
	 *
	 * @param outerFromList	The FromList from the outer query block(s)
	 * @param subqueryType	The subquery type
	 *
	 * @return	None
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void verifySelectStarSubquery(FromList outerFromList, int subqueryType) 
					throws StandardException
	{
		return; 
	}

	/**
	 * Push the order by list down from the cursor node
	 * into its child result set so that the optimizer
	 * has all of the information that it needs to 
	 * consider sort avoidance.
	 *
	 * @param orderByList	The order by list
	 *
	 * @return Nothing.
	 */
	void pushOrderByList(OrderByList orderByList)
	{
		this.orderByList = orderByList;
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
	 * @param gbl				The group by list, if any
	 * @param fromList			The from list, if any
	 *
	 * @return The generated ProjectRestrictNode atop the original FromTable.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ResultSetNode preprocess(int numTables,
									GroupByList gbl,
									FromList fromList)
								throws StandardException
	{

		if (subquerys.size() > 0)
		{
			subquerys.preprocess(
								numTables,
								(FromList) getNodeFactory().getNode(
									C_NodeTypes.FROM_LIST,
									getNodeFactory().doJoinOrderOptimization(),
									getContextManager()),
								(SubqueryList) getNodeFactory().getNode(
													C_NodeTypes.SUBQUERY_LIST,
													getContextManager()),
								(PredicateList) getNodeFactory().getNode(
													C_NodeTypes.PREDICATE_LIST,
													getContextManager()));
		}

		/* Allocate a dummy referenced table map */ 
		referencedTableMap = new JBitSet(numTables);
		referencedTableMap.set(tableNumber);
		return this;
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
		return genProjectRestrict(numTables);
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
		PredicateList		predList;
		ResultColumnList	prRCList;
		ResultSetNode		newPRN;
		
		/* We are the body of a quantified predicate subquery.  We
		 * need to generate (and return) a PRN above us so that there will be
		 * a place to attach the new predicate.
		 */

		/* We get a shallow copy of the ResultColumnList and its 
		 * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
		 */
		prRCList = resultColumns;
		resultColumns = resultColumns.copyListAndObjects();

		/* Replace ResultColumn.expression with new VirtualColumnNodes
		 * in the ProjectRestrictNode's ResultColumnList.  (VirtualColumnNodes include
		 * pointers to source ResultSetNode, this, and source ResultColumn.)
		 */
		prRCList.genVirtualColumnNodes(this, resultColumns);

		/* Put the new predicate in a list */
		predList = (PredicateList) getNodeFactory().getNode(
										C_NodeTypes.PREDICATE_LIST,
										getContextManager());
		predList.addPredicate(predicate);

		/* Finally, we create the new ProjectRestrictNode */
		return (ResultSetNode) getNodeFactory().getNode(
								C_NodeTypes.PROJECT_RESTRICT_NODE,
								this,
								prRCList,
								null,	/* Restriction */
								predList,   /* Restriction as PredicateList */
								null,	/* Project subquery list */
								null,	/* Restrict subquery list */
								tableProperties,
								getContextManager()				 );
	}

	/**
	 * Evaluate whether or not the subquery in a FromSubquery is flattenable.  
	 * Currently, a FSqry is flattenable if all of the following are true:
	 *		o  Subquery is a SelectNode or a RowResultSetNode (not a UnionNode)
	 *		o  It contains no top level subqueries.  (RESOLVE - we can relax this)
	 *		o  It does not contain a group by or having clause
	 *		o  It does not contain aggregates.
	 *		o  There is at least one result set in the from list that is
	 *		   not a RowResultSetNode (the reason is to avoid having
	 *		   an outer SelectNode with an empty FromList.
	 *
	 * @param fromList	The outer from list
	 *
	 * @return boolean	Whether or not the FromSubquery is flattenable.
	 */
	public boolean flattenableInFromSubquery(FromList fromList)
	{
		if ((subquerys != null) &&
			(subquerys.size() > 0))
		{
			return false;
		}

		if ((aggregateVector != null) &&
			(aggregateVector.size() > 0))
		{
			return false;
		}

		/*
		** Don't flatten if select list contains something
		** that isn't clonable
		*/
		if ( ! resultColumns.isCloneable())
		{
			return false;
		}

		boolean nonRowResultSetFound = false;
		int flSize = fromList.size();
		for (int index = 0; index < flSize; index++)
		{
			FromTable ft = (FromTable) fromList.elementAt(index);

			if (ft instanceof FromSubquery)
			{
				ResultSetNode subq = ((FromSubquery) ft).getSubquery();
				if ( ! (subq instanceof RowResultSetNode))
				{
					nonRowResultSetFound = true;
					break;
				}
			}
			else
			{
				nonRowResultSetFound = true;
				break;
			}
		}

		return nonRowResultSetFound;
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
		/*
		** Get an optimizer.  The only reason we need one is to get a
		** CostEstimate object, so we can represent the cost of this node.
		** This seems like overkill, but it's just an object allocation...
		*/
		Optimizer optimizer =
					getOptimizer(
								(FromList) getNodeFactory().getNode(
									C_NodeTypes.FROM_LIST,
									getNodeFactory().doJoinOrderOptimization(),
									getContextManager()),
								predicateList,
								dataDictionary,
								(RequiredRowOrdering) null);
		costEstimate = optimizer.newCostEstimate();

		// RESOLVE: THE COST SHOULD TAKE SUBQUERIES INTO ACCOUNT
		costEstimate.setCost(0.0d, outerRows, outerRows);

		subquerys.optimize(dataDictionary, outerRows);
		return this;
	}

	/**
	 * @see Optimizable#modifyAccessPath
	 *
	 * @exception StandardException		Thrown on error
	 */
	public Optimizable modifyAccessPath(JBitSet outerTables) throws StandardException
	{
		/* For most types of Optimizable, do nothing */
		return (Optimizable) modifyAccessPaths();
	}

	/**
	 * @see ResultSetNode#modifyAccessPaths
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ResultSetNode modifyAccessPaths() throws StandardException
	{
		ResultSetNode treeTop = this;

		subquerys.modifyAccessPaths();

		/* Generate the OrderByNode if a sort is still required for
		 * the order by.
		 */
		if (orderByList != null)
		{
			treeTop = (ResultSetNode) getNodeFactory().getNode(
											C_NodeTypes.ORDER_BY_NODE,
											treeTop,
											orderByList,
											tableProperties,
											getContextManager());
		}
		return treeTop;
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
		return true;
	}

    /**
     * The generated ResultSet will be:
     *
     *      RowResultSet -- for the VALUES clause
     *
	 *
	 * @return		A compiled Expression returning a ResultSet
	 *
	 * @exception StandardException		Thrown on error
     */
	public void generate(ActivationClassBuilder acb,
								MethodBuilder mb)
							throws StandardException
	{
		if (SanityManager.DEBUG)
        SanityManager.ASSERT(resultColumns != null, "Tree structure bad");

		/*
		** Check and see if everything below us is a constant or not.
		** If so, we'll let execution know that it can do some caching.
		** Before we do the check, we are going to temporarily set
		*/
		boolean canCache = canWeCacheResults();

		/* Get the next ResultSet #, so that we can number this ResultSetNode, its
		 * ResultColumnList and ResultSet.
		 */
		assignResultSetNumber();

	    // we are dealing with
		// VALUES(value1, value2, value3)
	    // so we generate a RowResultSet to return the values listed.

		// we can reduce the tree to one RowResultSet
    	// since there is nothing but the resultColumns

    	// RowResultSet takes the row-generating function
    	// so we generate one and get back the expression
    	// pointing to it.
	    //
		// generate the expression to return, which is:
		// ResultSetFactory.getRowResultSet(this, planX.exprN)
		// [planX is the name of the class being generated,
    	// exprN is the name of the function being generated.]

		acb.pushGetResultSetFactoryExpression(mb);

		acb.pushThisAsActivation(mb);
		resultColumns.generate(acb, mb);
		mb.push(canCache);
		mb.push(resultSetNumber);
		mb.push(costEstimate.rowCount());
		mb.push(costEstimate.getEstimatedCost());
		closeMethodArgument(acb, mb);

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getRowResultSet", ClassName.NoPutResultSet, 7);
	}

	/**
	 * Replace any DEFAULTs with the associated tree for the default.
	 *
	 * @param ttd	The TableDescriptor for the target table.
	 * @param tcl	The RCL for the target table.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void replaceDefaults(TableDescriptor ttd, ResultColumnList tcl) 
		throws StandardException
	{
		resultColumns.replaceDefaults(ttd, tcl);
	}

	/**
	 * Optimize any subqueries that haven't been optimized any where
	 * else.  This is useful for a RowResultSetNode as a derived table
	 * because it doesn't get optimized otherwise.
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void optimizeSubqueries(DataDictionary dd, double rowCount)
		throws StandardException
	{
		subquerys.optimize(dd, rowCount);
	}

	/**
	 * Notify the underlying result set tree that the result is
	 * ordering dependent.  (For example, no bulk fetch on an index
	 * if under an IndexRowToBaseRow.)
	 *
	 * @return Nothing.
	 */
	void markOrderingDependent()
	{
	}

	/*
	** Check and see if everything below us is a constant or not.
	** If so, we'll let execution know that it can do some caching.
	** Before we do the check, we are going to temporarily set
	** ParameterNodes to CONSTANT.  We do this because we know
	** that we can cache a row with a parameter value and get
	** the param column reset by the user setting a param, so
	** we can skip over parameter nodes.  We are doing this
	** extra work to optimize inserts of the form:
	**
	** prepare: insert into mytab values (?,?);
	** setParam
	** execute()
	** setParam
	** execute()
	*/
	private boolean canWeCacheResults() throws StandardException
	{

		/*
		** Check the tree below us
		*/
		HasVariantValueNodeVisitor visitor = 
			new HasVariantValueNodeVisitor(Qualifier.QUERY_INVARIANT, true);

		super.accept(visitor);
		boolean canCache = !visitor.hasVariant();

		return canCache;
	}
}
