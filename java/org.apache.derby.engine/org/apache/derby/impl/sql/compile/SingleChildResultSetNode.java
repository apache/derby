/*

   Derby - Class org.apache.derby.impl.sql.compile.SingleChildResultSetNode

//IC see: https://issues.apache.org/jira/browse/DERBY-1377
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

import java.util.List;
import java.util.Properties;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.AccessPath;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.RequiredRowOrdering;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.util.JBitSet;

/**
 * A SingleChildResultSetNode represents a result set with a single child.
 *
 */

abstract class SingleChildResultSetNode extends FromTable
{
	/**
	 * ResultSetNode under the SingleChildResultSetNode
	 */
	ResultSetNode	childResult;

	// Does this node have the truly... for the underlying tree
	protected boolean hasTrulyTheBestAccessPath;

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    SingleChildResultSetNode(
            ResultSetNode childResult,
            Properties tableProperties,
            ContextManager cm) {

        super(null /* correlationName */, tableProperties, cm);
        this.childResult = childResult;

        /* Propagate the child's referenced table map, if one exists */
        if (childResult.getReferencedTableMap() != null) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
            setReferencedTableMap
                ( (JBitSet)childResult.getReferencedTableMap().clone() );
        }
    }

	/** @see Optimizable#getTrulyTheBestAccessPath */
    @Override
	public AccessPath getTrulyTheBestAccessPath()
	{
		if (hasTrulyTheBestAccessPath)
		{
			return super.getTrulyTheBestAccessPath();
		}

		if (childResult instanceof Optimizable)
			return ((Optimizable) childResult).getTrulyTheBestAccessPath();

		return super.getTrulyTheBestAccessPath();
	}

	/**
	 * Return the childResult from this node.
	 *
	 * @return ResultSetNode	The childResult from this node.
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ResultSetNode getChildResult()
	{
		return childResult;
	}

	/**
	 * Set the childResult for this node.
	 *
	 * @param childResult 	The new childResult for this node.
	 */
	void setChildResult(ResultSetNode childResult)
	{
		this.childResult = childResult;
	}

	/**
	 * @see Optimizable#pullOptPredicates
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public void pullOptPredicates(
								OptimizablePredicateList optimizablePredicates)
			throws StandardException
	{
		if (childResult instanceof Optimizable)
		{
			((Optimizable) childResult).pullOptPredicates(optimizablePredicates);
		}
	}

	/** @see Optimizable#forUpdate */
    @Override
	public boolean forUpdate()
	{
		if (childResult instanceof Optimizable)
		{
			return ((Optimizable) childResult).forUpdate();
		}
		else
		{
			return super.forUpdate();
		}
	}

	/**
	 * @see Optimizable#initAccessPaths
	 */
    @Override
	public void initAccessPaths(Optimizer optimizer)
	{
		super.initAccessPaths(optimizer);
		if (childResult instanceof Optimizable)
		{
			((Optimizable) childResult).initAccessPaths(optimizer);
		}
	}

	/**
	 * @see Optimizable#updateBestPlanMap
	 *
	 * Makes a call to add/load/remove a plan mapping for this node,
	 * and then makes the necessary call to recurse on this node's
	 * child, in order to ensure that we've handled the full plan
	 * all the way down this node's subtree.
	 */
    @Override
	public void updateBestPlanMap(short action,
		Object planKey) throws StandardException
	{
		super.updateBestPlanMap(action, planKey);

		// Now walk the child.  Note that if the child is not an
		// Optimizable and the call to child.getOptimizerImpl()
		// returns null, then that means we haven't tried to optimize
		// the child yet.  So in that case there's nothing to
		// add/load.

		if (childResult instanceof Optimizable)
		{
			((Optimizable)childResult).
				updateBestPlanMap(action, planKey);
		}
		else if (childResult.getOptimizerImpl() != null)
		{
			childResult.getOptimizerImpl().
				updateBestPlanMaps(action, planKey);
		}
	}

	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG)
		{
			super.printSubNodes(depth);

			if (childResult != null)
			{
				printLabel(depth, "childResult: ");
				childResult.treePrint(depth + 1);
			}
		}
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
		return childResult.referencesTarget(name, baseTable);
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
		return childResult.referencesSessionSchema();
	}

	/**
	 * Set the (query block) level (0-based) for this FromTable.
	 *
	 * @param level		The query block level for this FromTable.
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void setLevel(int level)
	{
		super.setLevel(level);
		if (childResult instanceof FromTable)
		{
			((FromTable) childResult).setLevel(level);
		}
	}

	/**
	 * Return whether or not this ResultSetNode contains a subquery with a
	 * reference to the specified target.
	 * 
	 * @param name	The table name.
	 * @param baseTable	Whether or not the name is for a base table.
	 *
	 * @return boolean	Whether or not a reference to the table was found.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	boolean subqueryReferencesTarget(String name, boolean baseTable)
		throws StandardException
	{
		return childResult.subqueryReferencesTarget(name, baseTable);
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
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ResultSetNode preprocess(int numTables,
									GroupByList gbl,
									FromList fromList) 
								throws StandardException
	{
		childResult = childResult.preprocess(numTables, gbl, fromList);

		/* Build the referenced table map */
		setReferencedTableMap( (JBitSet) childResult.getReferencedTableMap().clone() );
//IC see: https://issues.apache.org/jira/browse/DERBY-6464

		return this;
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
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ResultSetNode addNewPredicate(Predicate predicate)
			throws StandardException
	{
		childResult = childResult.addNewPredicate(predicate);
		return this;
	}

	/**
	 * Push expressions down to the first ResultSetNode which can do expression
	 * evaluation and has the same referenced table map.
	 * RESOLVE - This means only pushing down single table expressions to
	 * DistinctNodes today.  Once we have a better understanding of how
	 * the optimizer will work, we can push down join clauses.
	 *
	 * @param predicateList	The PredicateList.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void pushExpressions(PredicateList predicateList)
					throws StandardException
	{
		if (childResult instanceof FromTable)
		{
			((FromTable) childResult).pushExpressions(predicateList);
		}
	}

	/**
	 * Evaluate whether or not the subquery in a FromSubquery is flattenable.  
	 * Currently, a FSqry is flattenable if all of the following are true:
	 *		o  Subquery is a SelectNode. 
	 *		o  It contains no top level subqueries.  (RESOLVE - we can relax this)
	 *		o  It does not contain a group by or having clause
	 *		o  It does not contain aggregates.
	 *
	 * @param fromList	The outer from list
	 *
	 * @return boolean	Whether or not the FromSubquery is flattenable.
	 */
    @Override
    boolean flattenableInFromSubquery(FromList fromList)
	{
		/* Flattening currently involves merging predicates and FromLists.
		 * We don't have a FromList, so we can't flatten for now.
		 */
		/* RESOLVE - this will introduce yet another unnecessary PRN */
		return false;
	}

	/**
	 * Ensure that the top of the RSN tree has a PredicateList.
	 *
	 * @param numTables			The number of tables in the query.
	 * @return ResultSetNode	A RSN tree with a node which has a PredicateList on top.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ResultSetNode ensurePredicateList(int numTables)
		throws StandardException
	{
		return this;
	}

	/**
	 * Optimize this SingleChildResultSetNode.  
	 *
	 * @param dataDictionary	The DataDictionary to use for optimization
	 * @param predicates		The PredicateList to optimize.  This should
	 *							be a join predicate.
	 * @param outerRows			The number of outer joining rows
	 *
	 * @return	ResultSetNode	The top of the optimized subtree
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ResultSetNode optimize(DataDictionary dataDictionary,
								  PredicateList predicates,
								  double outerRows) 
					throws StandardException
	{
		/* We need to implement this method since a NRSN can appear above a
		 * SelectNode in a query tree.
		 */
		childResult = childResult.optimize(
										dataDictionary,
										predicates,
										outerRows);

//IC see: https://issues.apache.org/jira/browse/DERBY-6464
        setCostEstimate( getOptimizerFactory().getCostEstimate() );
		getCostEstimate().setCost(childResult.getCostEstimate().getEstimatedCost(),
							childResult.getCostEstimate().rowCount(),
							childResult.getCostEstimate().singleScanRowCount());

		return this;
	}

	/**
	 * @see ResultSetNode#modifyAccessPaths
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ResultSetNode modifyAccessPaths() throws StandardException
	{
		childResult = childResult.modifyAccessPaths();

		return this;
	}

	/**
	 * @see ResultSetNode#changeAccessPath
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ResultSetNode changeAccessPath() throws StandardException
	{
		childResult = childResult.changeAccessPath();
		return this;
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
		return childResult.getFromTableByName(name, schemaName, exactMatch);
	}

	/**
	 * Decrement (query block) level (0-based) for this FromTable.
	 * This is useful when flattening a subquery.
	 *
	 * @param decrement	The amount to decrement by.
	 */
    @Override
	void decrementLevel(int decrement)
	{
		super.decrementLevel(decrement);
		childResult.decrementLevel(decrement);
	}

	/**
	 * Get the lock mode for the target of an update statement
	 * (a delete or update).  The update mode will always be row for
	 * CurrentOfNodes.  It will be table if there is no where clause.
	 *
	 * @return	The lock mode
	 */
    @Override
    int updateTargetLockMode()
	{
		return childResult.updateTargetLockMode();
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
    @Override
    boolean isOrderedOn(ColumnReference[] crs, boolean permuteOrdering, List<FromBaseTable> fbtHolder)
				throws StandardException
	{
        return childResult.isOrderedOn(crs, permuteOrdering, fbtHolder);
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
    @Override
    boolean isOneRowResultSet() throws StandardException
	{
		// Default is false
		return childResult.isOneRowResultSet();
	}

	/**
	 * Return whether or not the underlying ResultSet tree is for a NOT EXISTS join.
	 *
	 * @return Whether or not the underlying ResultSet tree is for a NOT EXISTS.
	 */
    @Override
    boolean isNotExists()
	{
		return childResult.isNotExists();
	}

	/**
	 * Determine whether we need to do reflection in order to do the projection.  
	 * Reflection is only needed if there is at least 1 column which is not
	 * simply selecting the source column.
	 *
	 * @return	Whether or not we need to do reflection in order to do
	 *			the projection.
	 */
	protected boolean reflectionNeededForProjection()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
		return ! (getResultColumns().allExpressionsAreColumns(childResult));
	}

	/**
	 * @see ResultSetNode#adjustForSortElimination
	 */
    @Override
	void adjustForSortElimination()
	{
		childResult.adjustForSortElimination();
	}

	/**
	 * @see ResultSetNode#adjustForSortElimination
	 */
    @Override
	void adjustForSortElimination(RequiredRowOrdering rowOrdering)
		throws StandardException
	{
		childResult.adjustForSortElimination(rowOrdering);
	}

	/**
	 * Get the final CostEstimate for this node.
	 *
	 * @return	The final CostEstimate for this node, which is
	 * 			the final cost estimate for the child node.
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    CostEstimate getFinalCostEstimate()
		throws StandardException
	{
		/*
		** The cost estimate will be set here if either optimize() or
		** optimizeIt() was called on this node.  It's also possible
		** that optimization was done directly on the child node,
		** in which case the cost estimate will be null here.
		*/
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
		if (getCostEstimate() == null)
        {
			return childResult.getFinalCostEstimate();
        }
		else
		{
			return getCostEstimate();
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
//IC see: https://issues.apache.org/jira/browse/DERBY-4421
	void acceptChildren(Visitor v)
		throws StandardException
	{
		super.acceptChildren(v);

//IC see: https://issues.apache.org/jira/browse/DERBY-4421
		if (childResult != null)
		{
			childResult = (ResultSetNode)childResult.accept(v);
		}
	}
}
