/*

   Derby - Class org.apache.derby.impl.sql.compile.JoinNode

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

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizablePredicate;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.RowOrdering;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.ClassName;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;

import org.apache.derby.iapi.util.JBitSet;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import java.util.Properties;
import java.util.Vector;

/**
 * A JoinNode represents a join result set for either of the basic DML
 * operations: SELECT and INSERT.  For INSERT - SELECT, any of the
 * fields in a JoinNode can be used (the JoinNode represents
 * the (join) SELECT statement in the INSERT - SELECT).  For INSERT,
 * the resultColumns in the selectList will contain the names of the columns
 * being inserted into or updated.
 *
 * @author Jeff Lichtman
 */

public class JoinNode extends TableOperatorNode
{
	/* Join semantics */
	public static final int INNERJOIN = 1;
	public static final int CROSSJOIN = 2;
	public static final int LEFTOUTERJOIN = 3;
	public static final int RIGHTOUTERJOIN = 4;
	public static final int FULLOUTERJOIN = 5;
	public static final int UNIONJOIN = 6;

	private boolean optimized;

	private PredicateList leftPredicateList;
	private PredicateList rightPredicateList;

	protected boolean flattenableJoin = true;
	Vector				aggregateVector;
	SubqueryList		subqueryList;
	ValueNode			joinClause;
	boolean	            joinClauseNormalized;
	PredicateList		joinPredicates;
	ResultColumnList	usingClause;


	/**
	 * Initializer for a JoinNode.
	 *
	 * @param leftResult	The ResultSetNode on the left side of this join
	 * @param rightResult	The ResultSetNode on the right side of this join
	 * @param onClause		The ON clause
	 * @param usingClause	The USING clause
	 * @param selectList	The result column list for the join
	 * @param tableProperties	Properties list associated with the table
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void init(
					Object leftResult,
					Object rightResult,
					Object onClause,
					Object usingClause,
					Object selectList,
					Object tableProperties)
			throws StandardException
	{
		super.init(leftResult, rightResult, tableProperties);
		resultColumns = (ResultColumnList) selectList;
		joinClause = (ValueNode) onClause;
		joinClauseNormalized = false;
		this.usingClause = (ResultColumnList) usingClause;

		/* JoinNodes can be generated in the parser or at the end of optimization.
		 * Those generated in the parser do not have resultColumns yet.
		 */
		if (resultColumns != null)
		{
			/* A longer term assertion */
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT((leftResultSet.getReferencedTableMap() != null &&
									  rightResultSet.getReferencedTableMap() != null) ||
									 (leftResultSet.getReferencedTableMap() == null &&
	 								  rightResultSet.getReferencedTableMap() == null),
					"left and right referencedTableMaps are expected to either both be non-null or both be null");
			}

			/* Build the referenced table map (left || right) */
			if (leftResultSet.getReferencedTableMap() != null)
			{
				referencedTableMap = (JBitSet) leftResultSet.getReferencedTableMap().clone();
				referencedTableMap.or((JBitSet) rightResultSet.getReferencedTableMap());
			}
		}
		joinPredicates = (PredicateList) getNodeFactory().getNode(
											C_NodeTypes.PREDICATE_LIST,
											getContextManager());
	}

	/*
	 *  Optimizable interface
	 */

	/**
	 * @see org.apache.derby.iapi.sql.compile.Optimizable#optimizeIt
	 *
	 * @exception StandardException		Thrown on error
	 */
	public CostEstimate optimizeIt(
							Optimizer optimizer,
							OptimizablePredicateList predList,
							CostEstimate outerCost,
							RowOrdering rowOrdering)
			throws StandardException
	{
		optimizer.trace(Optimizer.CALLING_ON_JOIN_NODE, 0, 0, 0.0, null);

		/*
		** RESOLVE: Most types of Optimizables only implement estimateCost(),
		** and leave it up to optimizeIt() in FromTable to figure out the
		** total cost of the join.  For joins, though, we want to figure out
		** the best plan for the join knowing how many outer rows there are -
		** it could affect the join strategy significantly.  So we implement
		** optimizeIt() here, which overrides the optimizeIt() in FromTable.
		** This assumes that the join strategy for which this join node is
		** the inner table is a nested loop join, which will not be a valid
		** assumption when we implement other strategies like materialization
		** (hash join can work only on base tables).
		*/

		/* RESOLVE - Need to figure out how to really optimize this node. */

		// RESOLVE: NEED TO SET ROW ORDERING OF SOURCES IN THE ROW ORDERING
		// THAT WAS PASSED IN.
		leftResultSet = optimizeSource(
							optimizer,
							leftResultSet,
							getLeftPredicateList(),
							outerCost);

		/* Move all joinPredicates down to the right.
		 * RESOLVE - When we consider the reverse join order then
		 * we will have to pull them back up and then push them
		 * down to the other side when considering the reverse
		 * join order.
		 * RESOLVE - This logic needs to be looked at when we
		 * implement full outer join.
		 */
		// Walk joinPredicates backwards due to possible deletes
		for (int index = joinPredicates.size() - 1; index >= 0; index --)
		{
			JBitSet	  curBitSet;
			Predicate predicate;

			predicate = (Predicate) joinPredicates.elementAt(index);
			if (! predicate.getPushable())
			{
				continue;
			}
			joinPredicates.removeElementAt(index);
			getRightPredicateList().addElement(predicate);
		}

		rightResultSet = optimizeSource(
							optimizer,
							rightResultSet,
							getRightPredicateList(),
							leftResultSet.getCostEstimate());

		costEstimate = getCostEstimate(optimizer);

		/*
		** We add the costs for the inner and outer table, but the number
		** of rows is that for the inner table only.
		*/
		costEstimate.setCost(
			leftResultSet.getCostEstimate().getEstimatedCost() +
			rightResultSet.getCostEstimate().getEstimatedCost(),
			rightResultSet.getCostEstimate().rowCount(),
			rightResultSet.getCostEstimate().rowCount());

		/*
		** Some types of joins (e.g. outer joins) will return a different
		** number of rows than is predicted by optimizeIt() in JoinNode.
		** So, adjust this value now. This method does nothing for most
		** join types.
		*/
		adjustNumberOfRowsReturned(costEstimate);

		/*
		** Get the cost of this result set in the context of the whole plan.
		*/
		getCurrentAccessPath().
			getJoinStrategy().
				estimateCost(
							this,
							predList,
							(ConglomerateDescriptor) null,
							outerCost,
							optimizer,
							costEstimate
							);

		optimizer.considerCost(this, predList, costEstimate, outerCost);

		/* Optimize subqueries only once, no matter how many times we're called */
		if ( (! optimized) && (subqueryList != null))
		{
			/* RESOLVE - Need to figure out how to really optimize this node.
		 	* Also need to figure out the pushing of the joinClause.
		 	*/
			subqueryList.optimize(optimizer.getDataDictionary(),
									costEstimate.rowCount());
			subqueryList.modifyAccessPaths();
		}

		optimized = true;

		return costEstimate;
	}

	/**
	 * @see Optimizable#pushOptPredicate
	 *
	 * @exception StandardException		Thrown on error
	 */

	public boolean pushOptPredicate(OptimizablePredicate optimizablePredicate)
			throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(optimizablePredicate instanceof Predicate,
				"optimizablePredicate expected to be instanceof Predicate");
			SanityManager.ASSERT(! optimizablePredicate.hasSubquery() &&
								 ! optimizablePredicate.hasMethodCall(),
				"optimizablePredicate either has a subquery or a method call");
		}

		/* Add the matching predicate to the joinPredicates */
		joinPredicates.addPredicate((Predicate) optimizablePredicate);

		/* Remap all of the ColumnReferences to point to the
		 * source of the values.
		 */
		RemapCRsVisitor rcrv = new RemapCRsVisitor(true);
		((Predicate) optimizablePredicate).getAndNode().accept(rcrv);

		return true;
	}

	/**
	 * @see Optimizable#modifyAccessPath
	 *
	 * @exception StandardException		Thrown on error
	 */
	public Optimizable modifyAccessPath(JBitSet outerTables) throws StandardException
	{
		super.modifyAccessPath(outerTables);

		/* By the time we're done here, both the left and right
		 * predicate lists should be empty because we pushed everything
		 * down.
		 */
		if (SanityManager.DEBUG)
		{
			if (getLeftPredicateList().size() != 0)
			{
				SanityManager.THROWASSERT(
					"getLeftPredicateList().size() expected to be 0, not " +
					getLeftPredicateList().size());
			}
			if (getRightPredicateList().size() != 0)
			{
				SanityManager.THROWASSERT(
					"getRightPredicateList().size() expected to be 0, not " +
					getRightPredicateList().size());
			}
		}

		return this;
	}


	/**
	 *  Some types of joins (e.g. outer joins) will return a different
	 *  number of rows than is predicted by optimizeIt() in JoinNode.
	 *  So, adjust this value now. This method does nothing for most
	 *  join types.
	 */
	protected void adjustNumberOfRowsReturned(CostEstimate costEstimate)
	{
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
	public ResultColumnList getAllResultColumns(String allTableName)
			throws StandardException
	{
		/* We need special processing when there is a USING clause.
	 	 * The resulting table will be the join columns from
		 * the outer table followed by the non-join columns from 
		 * left side plus the non-join columns from the right side.
		 */
		if (usingClause == null)
		{
			return getAllResultColumnsNoUsing(allTableName);
		}

		/* Get the logical left side of the join.
		 * This is where the join columns come from.
		 * (For RIGHT OUTER JOIN, the left is the right
		 * and the right is the left and the JOIN is the NIOJ).
		 */
		ResultSetNode	logicalLeftRS = getLogicalLeftResultSet();

		// Get the join columns
		ResultColumnList joinRCL = logicalLeftRS.getAllResultColumns(
										null).
											getJoinColumns(usingClause);

		// Get the left and right RCLs
		ResultColumnList leftRCL = leftResultSet.getAllResultColumns(allTableName); 
		ResultColumnList rightRCL = rightResultSet.getAllResultColumns(allTableName); 

		/* Chop the join columns out of the both left and right.
		 * Thanks to the ANSI committee, the join columns 
		 * do not belong to either table.
		 */
		if (leftRCL != null)
		{
			leftRCL.removeJoinColumns(usingClause);
		}
		if (rightRCL != null)
		{
			rightRCL.removeJoinColumns(usingClause);
		}

		/* If allTableName is null, then we want to return the splicing
		 * of the join columns followed by the non-join columns from
		 * the left followed by the non-join columns from the right.
		 * If not, then at most 1 side should match.
		 * NOTE: We need to make sure that the RC's VirtualColumnIds
		 * are correct (1 .. size).
		 */
		if (leftRCL == null)
		{
			rightRCL.resetVirtualColumnIds();
			return rightRCL;
		}
		else if (rightRCL == null)
		{
			leftRCL.resetVirtualColumnIds();
			return leftRCL;
		}
		else
		{
			/* Both sides are non-null.  This should only happen
			 * if allTableName is null.
			 */
			if (SanityManager.DEBUG)
			{
				if (allTableName != null)
				{
					SanityManager.THROWASSERT(
						"allTableName (" + allTableName + 
						") expected to be null");
				}
			}
			joinRCL.destructiveAppend(leftRCL);
			joinRCL.destructiveAppend(rightRCL);
			joinRCL.resetVirtualColumnIds();
			return joinRCL;
		}
	}

	/**
	 * Return a ResultColumnList with all of the columns in this table.
	 * (Used in expanding '*'s.)
	 * NOTE: Since this method is for expanding a "*" in the SELECT list,
	 * ResultColumn.expression will be a ColumnReference.
	 * NOTE: This method is handles the case when there is no USING clause.
	 * The caller handles the case when there is a USING clause.
	 *
	 * @param allTableName		The qualifier on the "*"
	 *
	 * @return ResultColumnList	List of result columns from this table.
	 *
	 * @exception StandardException		Thrown on error
	 */
	private ResultColumnList getAllResultColumnsNoUsing(String allTableName)
			throws StandardException
	{
		ResultColumnList leftRCL = leftResultSet.getAllResultColumns(allTableName); 
		ResultColumnList rightRCL = rightResultSet.getAllResultColumns(allTableName); 
		/* If allTableName is null, then we want to return the spliced
		 * left and right RCLs.  If not, then at most 1 side should match.
		 */
		if (leftRCL == null)
		{
			return rightRCL;
		}
		else if (rightRCL == null)
		{
			return leftRCL;
		}
		else
		{
			/* Both sides are non-null.  This should only happen
			 * if allTableName is null.
			 */
			if (SanityManager.DEBUG)
			{
				if (allTableName != null)
				{
					SanityManager.THROWASSERT(
						"allTableName (" + allTableName + 
						") expected to be null");
				}
			}

			// Return a spliced copy of the 2 lists
			ResultColumnList tempList =
								(ResultColumnList) getNodeFactory().getNode(
												C_NodeTypes.RESULT_COLUMN_LIST,
												getContextManager());
			tempList.nondestructiveAppend(leftRCL);
			tempList.nondestructiveAppend(rightRCL);
			return tempList;
		}
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

	public ResultColumn getMatchingColumn(ColumnReference columnReference) throws StandardException
	{
		/* Get the logical left and right sides of the join.
		 * (For RIGHT OUTER JOIN, the left is the right
		 * and the right is the left and the JOIN is the NIOJ).
		 */
		ResultSetNode	logicalLeftRS = getLogicalLeftResultSet();
		ResultSetNode	logicalRightRS = getLogicalRightResultSet();
		ResultColumn	leftRC = null;
		ResultColumn	resultColumn = null;
		ResultColumn	rightRC = null;
		ResultColumn	usingRC = null;

		leftRC = logicalLeftRS.getMatchingColumn(columnReference);

		if (leftRC != null)
		{
			resultColumn = leftRC;

			/* Find out if the column is in the using clause */
			if (usingClause != null)
			{
				usingRC = usingClause.getResultColumn(leftRC.getName());
			}
		}

		/* We only search on the right if the column isn't in the
		 * USING clause.
		 */
		if (usingRC == null)
		{
			rightRC = logicalRightRS.getMatchingColumn(columnReference);
		}

		if (rightRC != null)
		{
			/* We must catch ambiguous column references for joins here,
			 * since FromList only checks for ambiguous references between
			 * nodes, not within a node.
			 */
			if (leftRC != null)
			{
				throw StandardException.newException(SQLState.LANG_AMBIGUOUS_COLUMN_NAME, 
						 columnReference.getFullColumnName());
			}
			resultColumn = rightRC;
		}

		/* Insert will bind the underlying result sets which have
		 * tables twice. On the 2nd bind, resultColumns != null,
		 * we must return the RC from the JoinNode's RCL which is above
		 * the RC that we just found above.  (Otherwise, the source
		 * for the ColumnReference will be from the wrong ResultSet
		 * at generate().)
		 */
		if (resultColumns != null)
		{
			int rclSize = resultColumns.size();
			for (int index = 0; index < rclSize; index++)
			{
				ResultColumn rc = (ResultColumn) resultColumns.elementAt(index);
				VirtualColumnNode vcn = (VirtualColumnNode) rc.getExpression();
				if (resultColumn == vcn.getSourceColumn())
				{
					resultColumn = rc;
					break;
				}
			}
		}

		return resultColumn;
	}

	/**
	 * Bind the result columns of this ResultSetNode when there is no
	 * base table to bind them to.  This is useful for SELECT statements,
	 * where the result columns get their types from the expressions that
	 * live under them.
	 *
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void bindResultColumns(FromList fromListParam)
					throws StandardException
	{
		super.bindResultColumns(fromListParam);

		/* Now we build our RCL */
		buildRCL();

		/* We cannot bind the join clause until after we've bound our
		 * result columns. This is because the resultColumns from the
		 * children are propagated and merged to create our resultColumns
		 * in super.bindRCs().  If we bind the join clause prior to that
		 * call, then the ColumnReferences in the join clause will point
		 * to the children's RCLs at the time that they are bound, but
		 * will end up pointing above themselves, to our resultColumns,
		 * after the call to super.bindRCS().
		 */
		deferredBindExpressions(fromListParam);
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
	 * @return	Nothing
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
		super.bindResultColumns(targetTableDescriptor,
								targetVTI,
								targetColumnList, statement, 
								fromListParam);

		/* Now we build our RCL */
		buildRCL();

		/* We cannot bind the join clause until after we've bound our
		 * result columns. This is because the resultColumns from the
		 * children are propagated and merged to create our resultColumns
		 * in super.bindRCs().  If we bind the join clause prior to that
		 * call, then the ColumnReferences in the join clause will point
		 * to the children's RCLs at the time that they are bound, but
		 * will end up pointing above themselves, to our resultColumns,
		 * after the call to super.bindRCS().
		 */
		deferredBindExpressions(fromListParam);
	}

	/**
	 * Build the RCL for this node.  We propagate the RCLs up from the
	 * children and splice them to form this node's RCL.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	private void buildRCL() throws StandardException
	{
		/* NOTE - we only need to build this list if it does not already 
		 * exist.  This can happen in the degenerate case of an insert
		 * select with a join expression in a derived table within the select.
		 */
		if (resultColumns != null)
		{
			return;
		}

		ResultColumnList leftRCL;
		ResultColumnList rightRCL;
		ResultColumnList tmpRCL;

		/* We get a shallow copy of the left's ResultColumnList and its 
		 * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
		 */
		resultColumns = leftResultSet.getResultColumns();
		leftRCL = resultColumns.copyListAndObjects();
		leftResultSet.setResultColumns(leftRCL);

		/* Replace ResultColumn.expression with new VirtualColumnNodes
		 * in the ProjectRestrictNode's ResultColumnList.  (VirtualColumnNodes include
		 * pointers to source ResultSetNode, this, and source ResultColumn.)
		 */
		resultColumns.genVirtualColumnNodes(leftResultSet, leftRCL, false);

		/*
		** If this is a right outer join, we can get nulls on the left side,
		** so change the types of the left result set to be nullable.
		*/
		if (this instanceof HalfOuterJoinNode && ((HalfOuterJoinNode)this).isRightOuterJoin())
		{
			resultColumns.setNullability(true);
		}

		/* Now, repeat the process with the right's RCL */
		tmpRCL = rightResultSet.getResultColumns();
		rightRCL = tmpRCL.copyListAndObjects();
		rightResultSet.setResultColumns(rightRCL);

		/* Replace ResultColumn.expression with new VirtualColumnNodes
		 * in the ProjectRestrictNode's ResultColumnList.  (VirtualColumnNodes include
		 * pointers to source ResultSetNode, this, and source ResultColumn.)
		 */
		tmpRCL.genVirtualColumnNodes(rightResultSet, rightRCL, false);
		tmpRCL.adjustVirtualColumnIds(resultColumns.size());

		/*
		** If this is a left outer join, we can get nulls on the right side,
		** so change the types of the right result set to be nullable.
		*/
		if (this instanceof HalfOuterJoinNode && !((HalfOuterJoinNode)this).isRightOuterJoin())
		{
			tmpRCL.setNullability(true);
		}
		
		/* Now we append the propagated RCL from the right to the one from
		 * the left and call it our own.
		 */
		resultColumns.nondestructiveAppend(tmpRCL);
	}

	private void deferredBindExpressions(FromList fromListParam)
				throws StandardException
	{
		/* Bind the expressions in the join clause */
		subqueryList = (SubqueryList) getNodeFactory().getNode(
											C_NodeTypes.SUBQUERY_LIST,
											getContextManager());
		aggregateVector = new Vector();

		/* ON clause */
		if (joinClause != null)
		{
			/* Create a new fromList with only left and right children before
			 * binding the join clause. Valid column references in the join clause
			 * are limited to columns from the 2 tables being joined. This
			 * algorithm enforces that.
			 */
			FromList	fromList = (FromList) getNodeFactory().getNode(
									C_NodeTypes.FROM_LIST,
									getNodeFactory().doJoinOrderOptimization(),
									getContextManager());
			fromList.addElement((FromTable) leftResultSet);
			fromList.addElement((FromTable) rightResultSet);

			/* First bind with all tables in the from clause, to detect ambiguous
			 * references. Push the left and right children to the front of the
			 * fromListParam before binding the join clause.  (We will
			 * remove it before returning.)  Valid column references in
			 * the join clause are limited to columns from the 2 tables being
			 * joined
			 */
			fromListParam.insertElementAt(rightResultSet, 0);
			fromListParam.insertElementAt(leftResultSet, 0);
			joinClause = joinClause.bindExpression(
									  fromListParam, subqueryList,
									  aggregateVector);

			/* Now bind with two tables being joined. If this raises column not found exception,
			 * then we have a reference to other tables in the from clause. Raise invalid
			 * ON clause error to match DB2.
			 */
			try {
				joinClause = joinClause.bindExpression(
									  fromList, subqueryList,
									  aggregateVector);
			} catch (StandardException se) {
				if (se.getSQLState().equals(SQLState.LANG_COLUMN_NOT_FOUND))
					throw StandardException.newException(SQLState.LANG_DB2_ON_CLAUSE_INVALID); 
				throw se;
			}

			/* DB2 doesn't allow subquerries in the ON clause */
			if (subqueryList.size() > 0)
				throw StandardException.newException(SQLState.LANG_DB2_ON_CLAUSE_INVALID); 
			/*
			** We cannot have aggregates in the ON clause.
			** In the future, if we relax this, we'll need
			** to be able to pass the aggregateVector up
			** the tree.
			*/
			if (aggregateVector.size() > 0)
			{
				throw StandardException.newException(SQLState.LANG_NO_AGGREGATES_IN_ON_CLAUSE);
			}

			fromListParam.removeElementAt(0);
			fromListParam.removeElementAt(0);
		}
		/* USING clause */
		else if (usingClause != null)
		{
			/* Build a join clause from the usingClause, using the
			 * exposed names in the left and right RSNs.
			 * For each column in the list, we generate 2 ColumnReferences,
			 * 1 for the left and 1 for the right.  We bind each of these
			 * to the appropriate side and build an equality predicate
			 * between the 2.  We bind the = and AND nodes by hand because
			 * we have to bind the ColumnReferences a side at a time.
			 * We need to bind the CRs a side at a time to ensure that
			 * we don't find an bogus ambiguous column reference. (Bug 377)
			 */
			joinClause = (AndNode) getNodeFactory().getNode(
												C_NodeTypes.AND_NODE,
												null,
												null,
												getContextManager());
			AndNode	currAnd = (AndNode) joinClause;
			ValueNode trueNode = (ValueNode) getNodeFactory().getNode(
											C_NodeTypes.BOOLEAN_CONSTANT_NODE,
											Boolean.TRUE,
											getContextManager());

			int usingSize = usingClause.size();
			for (int index = 0; index < usingSize; index++)
			{
				BinaryComparisonOperatorNode equalsNode;
				ColumnReference leftCR;
				ColumnReference rightCR;
				ResultColumn	rc = (ResultColumn) usingClause.elementAt(index);

				/* currAnd starts as first point of insertion (leftOperand == null)
				 * and becomes last point of insertion.
				 */
				if (currAnd.getLeftOperand() != null)
				{
					currAnd.setRightOperand(
						(AndNode) getNodeFactory().getNode(
												C_NodeTypes.AND_NODE,
												null,
												null,
												getContextManager()));
					currAnd = (AndNode) currAnd.getRightOperand();
				}

				/* Create and bind the left CR */
				fromListParam.insertElementAt(leftResultSet, 0);
				leftCR = (ColumnReference) getNodeFactory().getNode(
							C_NodeTypes.COLUMN_REFERENCE,
							rc.getName(),
							((FromTable) leftResultSet).getTableName(),
							getContextManager()); 
				leftCR = (ColumnReference) leftCR.bindExpression(
									  fromListParam, subqueryList,
									  aggregateVector);
				fromListParam.removeElementAt(0);

				/* Create and bind the right CR */
				fromListParam.insertElementAt(rightResultSet, 0);
				rightCR = (ColumnReference) getNodeFactory().getNode(
							C_NodeTypes.COLUMN_REFERENCE,
							rc.getName(),
							((FromTable) rightResultSet).getTableName(),
							getContextManager()); 
				rightCR = (ColumnReference) rightCR.bindExpression(
									  fromListParam, subqueryList,
									  aggregateVector);
				fromListParam.removeElementAt(0);

				/* Create and insert the new = condition */
				equalsNode = (BinaryComparisonOperatorNode)
								getNodeFactory().getNode(
										C_NodeTypes.BINARY_EQUALS_OPERATOR_NODE,
										leftCR,
										rightCR,
										getContextManager());
				equalsNode.bindComparisonOperator();

				currAnd.setLeftOperand(equalsNode);
				/* The right deep chain of AndNodes ends in a BinaryTrueNode.
				 * NOTE: We set it for every AndNode, even though we will
				 * overwrite it if this is not the last column in the list,
				 * because postBindFixup() expects both the AndNode to have
				 * both the left and right operands.
				 */
				currAnd.setRightOperand(trueNode);
				currAnd.postBindFixup();
			 }
		}

		if (joinClause != null)
		{
			/* If joinClause is a parameter, (where ?), then we assume
			 * it will be a nullable boolean.
			 */
			if (joinClause.isParameterNode())
			{
				joinClause.setType(new DataTypeDescriptor(TypeId.BOOLEAN_ID, true));
			}
			
			/*
			** Is the datatype of the JOIN clause BOOLEAN?
			**
			** NOTE: This test is not necessary in SQL92 entry level, because
			** it is syntactically impossible to have a non-Boolean JOIN clause
			** in that level of the standard.  But we intend to extend the
			** language to allow Boolean user functions in the JOIN clause,
			** so we need to test for the error condition.
			*/
			TypeId joinTypeId = joinClause.getTypeId();

			/* If the where clause is not a built-in type, then generate a bound 
			 * conversion tree to a built-in type.
			 */
			if (! joinTypeId.systemBuiltIn())
			{
				joinClause = joinClause.genSQLJavaSQLTree();
			}

			if (! joinClause.getTypeServices().getTypeId().equals(
					TypeId.BOOLEAN_ID))
			{
				throw StandardException.newException(SQLState.LANG_NON_BOOLEAN_JOIN_CLAUSE, 
						joinClause.getTypeServices().getTypeId().getSQLTypeName()
						);
			}
		}
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
		ResultSetNode newTreeTop;
		
		newTreeTop = super.preprocess(numTables, gbl, fromList);

		/* Put the expression trees in conjunctive normal form.
		 * NOTE - This needs to occur before we preprocess the subqueries
		 * because the subquery transformations assume that any subquery operator 
		 * negation has already occurred.
		 */
		if (joinClause != null)
		{
			normExpressions();

			/* Preprocess any subqueries in the join clause */
			if (subqueryList != null)
			{
				/* RESOLVE - In order to flatten a subquery in
				 * the ON clause of an inner join we'd have to pass
				 * the various lists from the outer select through to
				 * ResultSetNode.preprocess() and overload
				 * normExpressions in HalfOuterJoinNode.  That's not
				 * worth the effort, so we say that the ON clause
				 * is not under a top level AND in normExpressions()
				 * to ensure that subqueries in the ON clause do not
				 * get flattened.  That allows us to pass empty lists
				 * to joinClause.preprocess() because we know that no
				 * flattening will take place. (Bug #1206)
				 */
				joinClause.preprocess(
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

			/* Pull apart the expression trees */
			joinPredicates.pullExpressions(numTables, joinClause);
			joinPredicates.categorize();
			joinClause = null;
		}

		return newTreeTop;
	}

    /**
     * Find the unreferenced result columns and project them out. This is used in pre-processing joins
     * that are not flattened into the where clause.
     */
    void projectResultColumns() throws StandardException
    {
        leftResultSet.projectResultColumns();
        rightResultSet.projectResultColumns();
        resultColumns.pullVirtualIsReferenced();
        super.projectResultColumns();
    }
    
	/** Put the expression trees in conjunctive normal form 
	 *
	 * @return None.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void normExpressions()
				throws StandardException
	{
		if (joinClauseNormalized == true) return;

		/* For each expression tree:
		 *	o Eliminate NOTs (eliminateNots())
		 *	o Ensure that there is an AndNode on top of every
		 *	  top level expression. (putAndsOnTop())
		 *	o Finish the job (changeToCNF())
		 */
		joinClause = joinClause.eliminateNots(false);
		if (SanityManager.DEBUG)
		{
			if (!(joinClause.verifyEliminateNots()) )
			{
				joinClause.treePrint();
				SanityManager.THROWASSERT(
					"joinClause in invalid form: " + joinClause); 
			}
		}
		joinClause = joinClause.putAndsOnTop();
		if (SanityManager.DEBUG)
		{
			if (! ((joinClause instanceof AndNode) &&
				   (joinClause.verifyPutAndsOnTop())) )
			{
				joinClause.treePrint();
				SanityManager.THROWASSERT(
					"joinClause in invalid form: " + joinClause); 
			}
		}
		/* RESOLVE - ON clause is temporarily "not under a top
		 * top level AND" until we figure out how to deal with
		 * subqueries in the ON clause. (Bug 1206)
		 */
		joinClause = joinClause.changeToCNF(false);
		if (SanityManager.DEBUG)
		{
			if (! ((joinClause instanceof AndNode) &&
				   (joinClause.verifyChangeToCNF())) )
			{
				joinClause.treePrint();
				SanityManager.THROWASSERT(
					"joinClause in invalid form: " + joinClause); 
			}
		}

		joinClauseNormalized = true;
	}

	/**
	 * Push expressions down to the first ResultSetNode which can do expression
	 * evaluation and has the same referenced table map.
	 * RESOLVE - This means only pushing down single table expressions to
	 * DistinctNodes today.  Once we have a better understanding of how
	 * the optimizer will work, we can push down join clauses.
	 *
	 * @param outerPredicateList	The PredicateList from the outer RS.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void pushExpressions(PredicateList outerPredicateList)
					throws StandardException
	{
		FromTable		leftFromTable = (FromTable) leftResultSet;
		FromTable		rightFromTable = (FromTable) rightResultSet;

		/* OuterJoinNodes are responsible for overriding this
		 * method since they have different rules about where predicates
		 * can be applied.
		 */
		if (SanityManager.DEBUG)
		{
			if (this instanceof HalfOuterJoinNode)
			{
				SanityManager.THROWASSERT(
					"JN.pushExpressions() not expected to be called for " +
					getClass().getName());
			}
		}

		/* We try to push "pushable" 
		 * predicates to 1 of 3 places:
		 *	o Predicates that only reference tables
		 *	  on the left are pushed to the leftPredicateList.
		 *	o Predicates that only reference tables
		 *	  on the right are pushed to the rightPredicateList.
		 *	o Predicates which reference tables on both
		 *	  sides (and no others) are pushed to 
		 *	  the joinPredicates and may be pushed down
		 *	  further during optimization.
		 */
		// Left only
		pushExpressionsToLeft(outerPredicateList);
		leftFromTable.pushExpressions(getLeftPredicateList());
		// Right only
		pushExpressionsToRight(outerPredicateList);
		rightFromTable.pushExpressions(getRightPredicateList());
		// Join predicates
		grabJoinPredicates(outerPredicateList);

		/* By the time we're done here, both the left and right
		 * predicate lists should be empty because we pushed everything
		 * down.
		 */
		if (SanityManager.DEBUG)
		{
			if (getLeftPredicateList().size() != 0)
			{
				SanityManager.THROWASSERT(
					"getLeftPredicateList().size() expected to be 0, not " +
					getLeftPredicateList().size());
			}
			if (getRightPredicateList().size() != 0)
			{
				SanityManager.THROWASSERT(
					"getRightPredicateList().size() expected to be 0, not " +
					getRightPredicateList().size());
			}
		}
	}

	protected void pushExpressionsToLeft(PredicateList outerPredicateList)
		throws StandardException
	{
		FromTable		leftFromTable = (FromTable) leftResultSet;

		JBitSet		leftReferencedTableMap = leftFromTable.getReferencedTableMap();

		/* Build a list of the single table predicates on left result set
		 * that we can push down 
		 */
		// Walk outerPredicateList backwards due to possible deletes
		for (int index = outerPredicateList.size() - 1; index >= 0; index --)
		{
			JBitSet	  curBitSet;
			Predicate predicate;

			predicate = (Predicate) outerPredicateList.elementAt(index);
			if (! predicate.getPushable())
			{
				continue;
			}

			curBitSet = predicate.getReferencedSet();
			
			/* Do we have a match? */
			if (leftReferencedTableMap.contains(curBitSet))
			{
				/* Add the matching predicate to the push list */
				getLeftPredicateList().addPredicate(predicate);

				/* Remap all of the ColumnReferences to point to the
				 * source of the values.
				 * The tree is something like:
				 *			PRN1
				 *			  |
				 *			 JN (this)
				 *		   /    \
				 *		PRN2	PRN3
				 *        |       |
				 *		FBT1	FBT2
				 *
				 * The ColumnReferences start off pointing to the RCL off of
				 * PRN1.  For optimization, we want them to point to the
				 * RCL off of PRN2.  In order to do that, we remap them
				 * twice here.  If optimization pushes them down to the
				 * base table, it will remap them again.
				 */
				RemapCRsVisitor rcrv = new RemapCRsVisitor(true);
				predicate.getAndNode().accept(rcrv);
				predicate.getAndNode().accept(rcrv);

				/* Remove the matching predicate from the outer list */
				outerPredicateList.removeElementAt(index);
			}
		}
	}

	private void pushExpressionsToRight(PredicateList outerPredicateList)
		throws StandardException
	{
		FromTable		rightFromTable = (FromTable) rightResultSet;

		JBitSet		rightReferencedTableMap = rightFromTable.getReferencedTableMap();

		/* Build a list of the single table predicates on right result set
		 * that we can push down 
		 */
		// Walk outerPredicateList backwards due to possible deletes
		for (int index = outerPredicateList.size() - 1; index >= 0; index --)
		{
			JBitSet	  curBitSet;
			Predicate predicate;

			predicate = (Predicate) outerPredicateList.elementAt(index);
			if (! predicate.getPushable())
			{
				continue;
			}

			curBitSet = predicate.getReferencedSet();
			
			/* Do we have a match? */
			if (rightReferencedTableMap.contains(curBitSet))
			{
				/* Add the matching predicate to the push list */
				getRightPredicateList().addPredicate(predicate);

				/* Remap all of the ColumnReferences to point to the
				 * source of the values.
				 * The tree is something like:
				 *			PRN1
				 *			  |
				 *			 JN (this)
				 *		   /    \
				 *		PRN2	PRN3
				 *        |       |
				 *		FBT1	FBT2
				 *
				 * The ColumnReferences start off pointing to the RCL off of
				 * PRN1.  For optimization, we want them to point to the
				 * RCL off of PRN3.  In order to do that, we remap them
				 * twice here.  If optimization pushes them down to the
				 * base table, it will remap them again.
				 */
				RemapCRsVisitor rcrv = new RemapCRsVisitor(true);
				predicate.getAndNode().accept(rcrv);
				predicate.getAndNode().accept(rcrv);

				/* Remove the matching predicate from the outer list */
				outerPredicateList.removeElementAt(index);
			}
		}
	}

	private void grabJoinPredicates(PredicateList outerPredicateList)
		throws StandardException
	{
		FromTable		leftFromTable = (FromTable) leftResultSet;
		FromTable		rightFromTable = (FromTable) rightResultSet;

		JBitSet		leftReferencedTableMap = leftFromTable.getReferencedTableMap();
		JBitSet		rightReferencedTableMap = rightFromTable.getReferencedTableMap();

		/* Build a list of the join predicates that we can push down */
		// Walk outerPredicateList backwards due to possible deletes
		for (int index = outerPredicateList.size() - 1; index >= 0; index --)
		{
			JBitSet	  curBitSet;
			Predicate predicate;

			predicate = (Predicate) outerPredicateList.elementAt(index);
			if (! predicate.getPushable())
			{
				continue;
			}

			curBitSet = predicate.getReferencedSet();
			
			/* Do we have a match? */
			JBitSet innerBitSet = (JBitSet) rightReferencedTableMap.clone();
			innerBitSet.or(leftReferencedTableMap);
			if (innerBitSet.contains(curBitSet))
			{
				/* Add the matching predicate to the push list */
				joinPredicates.addPredicate(predicate);

				/* Remap all of the ColumnReferences to point to the
				 * source of the values.
				 * The tree is something like:
				 *			PRN1
				 *			  |
				 *			 JN (this)
				 *		   /    \
				 *		PRN2	PRN3
				 *        |       |
				 *		FBT1	FBT2
				 *
				 * The ColumnReferences start off pointing to the RCL off of
				 * PRN1.  For optimization, we want them to point to the
				 * RCL off of PRN2 or PRN3.  In order to do that, we remap them
				 * twice here.  If optimization pushes them down to the
				 * base table, it will remap them again.
				 */
				RemapCRsVisitor rcrv = new RemapCRsVisitor(true);
				predicate.getAndNode().accept(rcrv);
				predicate.getAndNode().accept(rcrv);

				/* Remove the matching predicate from the outer list */
				outerPredicateList.removeElementAt(index);
			}
		}
	}

	/**
	 * Flatten this JoinNode into the outer query block. The steps in
	 * flattening are:
	 *	o  Mark all ResultColumns as redundant, so that they are "skipped over"
	 *	   at generate().
	 *	o  Append the joinPredicates to the outer list.
	 *	o  Create a FromList from the tables being joined and return 
	 *	   that list so that the caller will merge the 2 lists 
	 *
	 * @param rcl				The RCL from the outer query
	 * @param outerPList		PredicateList to append wherePredicates to.
	 * @param sql				The SubqueryList from the outer query
	 * @param gbl				The group by list, if any
	 *
	 * @return FromList		The fromList from the underlying SelectNode.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public FromList flatten(ResultColumnList rcl,
							PredicateList outerPList,
							SubqueryList sql,
							GroupByList gbl)

			throws StandardException
	{
		/* OuterJoinNodes should never get here.
		 * (They can be transformed, but never
		 * flattened directly.)
		 */
		if (SanityManager.DEBUG)
		{
			if (this instanceof HalfOuterJoinNode)
			{
				SanityManager.THROWASSERT(
					"JN.flatten() not expected to be called for " +
					getClass().getName());
			}
		}

		/* Build a new FromList composed of left and right children 
		 * NOTE: We must call FL.addElement() instead of FL.addFromTable()
		 * since there is no exposed name. (And even if there was,
		 * we could care less about unique exposed name checking here.)
		 */
		FromList	fromList = (FromList) getNodeFactory().getNode(
									C_NodeTypes.FROM_LIST,
									getNodeFactory().doJoinOrderOptimization(),
									getContextManager());
		fromList.addElement((FromTable) leftResultSet);
		fromList.addElement((FromTable) rightResultSet);

		/* Mark our RCL as redundant */
		resultColumns.setRedundant();

		/* Remap all ColumnReferences from the outer query to this node.
		 * (We replace those ColumnReferences with clones of the matching
		 * expression in the left and right's RCL.
		 */
		rcl.remapColumnReferencesToExpressions();
		outerPList.remapColumnReferencesToExpressions();
		if (gbl != null)
		{
			gbl.remapColumnReferencesToExpressions();
		}

		if (joinPredicates.size() > 0)
		{
			outerPList.destructiveAppend(joinPredicates);
		}

		if (subqueryList != null && subqueryList.size() > 0)
		{
			sql.destructiveAppend(subqueryList);
		}

		return fromList;
	}

	/**
	 * Currently we don't reordering any outer join w/ inner joins.
	 */
	public boolean LOJ_reorderable(int numTables)
				throws StandardException
	{
		return false;
	}

	/**
	 * Transform any Outer Join into an Inner Join where applicable.
	 * (Based on the existence of a null intolerant
	 * predicate on the inner table.)
	 *
	 * @param predicateTree	The predicate tree for the query block
	 *
	 * @return The new tree top (OuterJoin or InnerJoin).
	 *
	 * @exception StandardException		Thrown on error
	 */
	public FromTable transformOuterJoins(ValueNode predicateTree, int numTables)
		throws StandardException
	{
		/* Can't flatten if no predicates in where clause. */
		if (predicateTree == null)
		{
			return this;
		}

		/* See if left or right sides can be transformed */
		leftResultSet = ((FromTable) leftResultSet).transformOuterJoins(predicateTree, numTables);
		rightResultSet = ((FromTable) rightResultSet).transformOuterJoins(predicateTree, numTables);

		return this;
	}

    /**
     * For joins, the tree will be (nodes are left out if the clauses
     * are empty):
     *
     *      ProjectRestrictResultSet -- for the having and the select list
     *      SortResultSet -- for the group by list
     *      ProjectRestrictResultSet -- for the where and the select list (if no group or having)
     *      the result set for the fromList
     *
	 *
	 * @exception StandardException		Thrown on error
     */
	public void generate(ActivationClassBuilder acb,
								MethodBuilder mb)
							throws StandardException
	{
		generateCore(acb, mb, INNERJOIN, null, null);
	}

    /**
     * Generate the code for a qualified join node.
	 *
	 * @exception StandardException		Thrown on error
     */
	public void generateCore(ActivationClassBuilder acb,
								   MethodBuilder mb,
								   int joinType) 
			throws StandardException
	{
		generateCore(acb, mb, joinType, joinClause, subqueryList);
	}

	/**
	 * Do the generation work for the join node hierarchy.
	 *
	 * @param acb			The ActivationClassBuilder
	 * @param mb the method the code is to go into
	 * @param joinType		The join type
	 * @param joinClause	The join clause, if any
	 * @param subquerys		The list of subqueries in the join clause, if any
	 *
	 * @return Expression	The generated Expression
	 *
	 * @exception StandardException		Thrown on error
	 */
	protected void generateCore(ActivationClassBuilder acb,
									  MethodBuilder mb,
									  int joinType,
									  ValueNode joinClause,
									  SubqueryList subquerys)
			throws StandardException
	{
		/* Put the predicates back into the tree */
		if (joinPredicates != null)
		{
			joinClause = joinPredicates.restorePredicates();
			joinPredicates = null;
		}

		/* Get the next ResultSet #, so that we can number this ResultSetNode, its
		 * ResultColumnList and ResultSet.
		 */
		assignResultSetNumber();

		/* Set the point of attachment in all subqueries attached
		 * to this node.
		 */
		if (subquerys != null && subquerys.size() > 0)
		{
			subquerys.setPointOfAttachment(resultSetNumber);
		}

		// build up the tree.

		/* Generate the JoinResultSet */
		/* Nested loop and hash are the only join strategy currently supporteds.  
		 * Right outer joins are transformed into left outer joins.
		 */
		String			joinResultSetString;

		if (joinType == LEFTOUTERJOIN)
		{
			joinResultSetString = 
				((Optimizable) rightResultSet).getTrulyTheBestAccessPath().
					getJoinStrategy().halfOuterJoinResultSetMethodName();
		}
		else
		{
			joinResultSetString = 
				((Optimizable) rightResultSet).getTrulyTheBestAccessPath().
					getJoinStrategy().joinResultSetMethodName();
		}

		acb.pushGetResultSetFactoryExpression(mb);
		int nargs = getJoinArguments(acb, mb, joinClause);
		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, joinResultSetString, ClassName.NoPutResultSet, nargs);
	}

	/**
	 * Get the arguments to the join result set.
	 *
	 * @param acb	The ActivationClassBuilder for the class we're building.
	 * @param mb the method the generated code is going into
	 * @param joinClause	The join clause, if any
	 *
	 * @return	The array of arguments to the join result set
	 *
	 * @exception StandardException		Thrown on error
	 */
	private int getJoinArguments(ActivationClassBuilder acb,
											MethodBuilder mb,
											ValueNode joinClause)
							throws StandardException
	{
		int numArgs = getNumJoinArguments();

		leftResultSet.generate(acb, mb); // arg 1
		mb.push(leftResultSet.resultColumns.size()); // arg 2
		rightResultSet.generate(acb, mb); // arg 3
		mb.push(rightResultSet.resultColumns.size()); // arg 4
		acb.pushThisAsActivation(mb); // arg 5

		// Get the cost estimate if we don't have one yet
		if (costEstimate == null)
		{
			costEstimate = getNewCostEstimate();
			costEstimate.setCost(
				leftResultSet.getFinalCostEstimate().getEstimatedCost() +
				rightResultSet.getFinalCostEstimate().getEstimatedCost(),
				rightResultSet.getFinalCostEstimate().rowCount(),
				rightResultSet.getFinalCostEstimate().rowCount());
		}

		// for the join clause, we generate an exprFun
		// that evaluates the expression of the clause
		// against the current row of the child's result.
		// if the join clause is empty, we generate a function
		// that just returns true. (Performance tradeoff: have
		// this function for the empty join clause, or have
		// all non-empty join clauses check for a null at runtime).

   		// generate the function and initializer:
   		// Note: Boolean lets us return nulls (boolean would not)
   		// private Boolean exprN()
   		// {
   		//   return <<joinClause.generate(ps)>>;
   		// }
   		// static Method exprN = method pointer to exprN;

		// if there is no join clause, we just pass a null Expression.
		if (joinClause == null)
		{
		   	mb.pushNull(ClassName.GeneratedMethod); // arg 6
		}
		else
		{
			// this sets up the method and the static field.
			// generates:
			// 	Object userExprFun { }
			MethodBuilder userExprFun = acb.newUserExprFun();

			// join clause knows it is returning its value;

			/* generates:
			 *    return <joinClause.generate(acb)>;
			 * and adds it to userExprFun
			 */
			joinClause.generate(acb, userExprFun);
			userExprFun.methodReturn();

			// we are done modifying userExprFun, complete it.
			userExprFun.complete();

	   		// join clause is used in the final result set as an access of the new static
   			// field holding a reference to this new method.
			// generates:
			//	ActivationClass.userExprFun
			// which is the static field that "points" to the userExprFun
			// that evaluates the where clause.
   			acb.pushMethodReference(mb, userExprFun); // arg 6
		}

		mb.push(resultSetNumber); // arg 7

		addOuterJoinArguments(acb, mb);

		// Does right side return a single row
		oneRowRightSide(acb, mb);

		// estimated row count
		mb.push(costEstimate.rowCount());

		// estimated cost
		mb.push(costEstimate.getEstimatedCost());

		closeMethodArgument(acb, mb);

		return numArgs;

	}

	protected void oneRowRightSide(ActivationClassBuilder acb,
									   MethodBuilder mb)
		throws StandardException
	{
		mb.push(rightResultSet.isOneRowResultSet());
		mb.push(rightResultSet.isNotExists());  //join is for NOT EXISTS
	}

	/**
	 * Return the number of arguments to the join result set.  This will
	 * be overridden for other types of joins (for example, outer joins).
	 */
	protected int getNumJoinArguments()
	{
		return 12;
	}

	/**
	 * Generate	and add any arguments specifict to outer joins.
	 * (Expected to be overriden, where appropriate, in subclasses.)
	 *
	 * @param acb		The ActivationClassBuilder
	 * @param mb the method  the generated code is to go into
	 *
	 * return The number of args added
	 *
	 * @exception StandardException		Thrown on error
	 */
	 protected int addOuterJoinArguments(ActivationClassBuilder acb,
											MethodBuilder mb)
		 throws StandardException
	 {
		 return 0;
	 }

	/** 
	 * Convert the joinType to a string.
	 *
	 * @param int			The joinType as an int.
	 *
	 * @return String		The joinType as a String.
	 */
	public static String joinTypeToString(int joinType)
	{
		switch(joinType)
		{
			case INNERJOIN:
				return "INNER JOIN";

			case CROSSJOIN:
				return "CROSS JOIN";

			case LEFTOUTERJOIN:
				return "LEFT OUTER JOIN";

			case RIGHTOUTERJOIN:
				return "RIGHT OUTER JOIN";

			case FULLOUTERJOIN:
				return "FULL OUTER JOIN";

			case UNIONJOIN:
				return "UNION JOIN";

			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.ASSERT(false,	"Unexpected joinType");
				}
				return null;
		}
	}

	protected PredicateList getLeftPredicateList() throws StandardException
	{
		if (leftPredicateList == null)
			leftPredicateList = (PredicateList) getNodeFactory().getNode(
													C_NodeTypes.PREDICATE_LIST,
													getContextManager());

		return leftPredicateList;
	}

	protected PredicateList getRightPredicateList() throws StandardException
	{
		if (rightPredicateList == null)
			rightPredicateList = (PredicateList) getNodeFactory().getNode(
													C_NodeTypes.PREDICATE_LIST,
													getContextManager());

		return rightPredicateList;
	}

	/**
	 * Get the lock mode for the target of an update statement
	 * (a delete or update).  The update mode will always be row for
	 * CurrentOfNodes.  It will be table if there is no where clause.
	 *
	 * @return	The lock mode
	 */
	public int updateTargetLockMode()
	{
		/* Always use row locking if we have a join node.
		 * We can only have a join node if there is a subquery that
		 * got flattened, hence there is a restriction.
		 */
		return TransactionController.MODE_RECORD;
	}

	/**
	 * Mark this node and its children as not being a flattenable join.
	 *
	 * @return Nothing.
	 */
	void notFlattenableJoin()
	{
		flattenableJoin = false;
		leftResultSet.notFlattenableJoin();
		rightResultSet.notFlattenableJoin();
	}

	/**
	 * Is this FromTable a JoinNode which can be flattened into 
	 * the parents FromList.
	 *
	 * @return boolean		Whether or not this FromTable can be flattened.
	 */
	public boolean isFlattenableJoinNode()
	{
		return flattenableJoin;
	}

	/**
	 * Return whether or not the underlying ResultSet tree
	 * is ordered on the specified columns.
	 * RESOLVE - This method currently only considers the outermost table 
	 * of the query block.
	 *
	 * @param	crs					The specified ColumnReference[]
	 * @param	permuteOrdering		Whether or not the order of the CRs in the array can be permuted
	 * @param	fbtVector			Vector that is to be filled with the FromBaseTable	
	 *
	 * @return	Whether the underlying ResultSet tree
	 * is ordered on the specified column.
	 *
	 * @exception StandardException		Thrown on error
	 */
	boolean isOrderedOn(ColumnReference[] crs, boolean permuteOrdering, Vector fbtVector)
				throws StandardException
	{
		/* RESOLVE - easiest thing for now is to only consider the leftmost child */
		return leftResultSet.isOrderedOn(crs, permuteOrdering, fbtVector);
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

			if (subqueryList != null)
			{
				printLabel(depth, "subqueryList: ");
				subqueryList.treePrint(depth + 1);
			}

			if (joinClause != null)
			{
				printLabel(depth, "joinClause: ");
				joinClause.treePrint(depth + 1);
			}

			if (joinPredicates.size() != 0)
			{
				printLabel(depth, "joinPredicates: ");
				joinPredicates.treePrint(depth + 1);
			}

			if (usingClause != null)
			{
				printLabel(depth, "usingClause: ");
				usingClause.treePrint(depth + 1);
			}
		}
	}

	void setSubqueryList(SubqueryList subqueryList)
	{
		this.subqueryList = subqueryList;
	}

	void setAggregateVector(Vector aggregateVector)
	{
		this.aggregateVector = aggregateVector;
	}

	/**
	 * Return the logical left result set for this qualified
	 * join node.
	 * (For RIGHT OUTER JOIN, the left is the right
	 * and the right is the left and the JOIN is the NIOJ).
	 */
	ResultSetNode getLogicalLeftResultSet()
	{
		return leftResultSet;
	}

	/**
	 * Return the logical right result set for this qualified
	 * join node.
	 * (For RIGHT OUTER JOIN, the left is the right
	 * and the right is the left and the JOIN is the NIOJ).
	 */
	ResultSetNode getLogicalRightResultSet()
	{
		return rightResultSet;
	}

	/**
	 * Accept a visitor, and call v.visit()
	 * on child nodes as necessary.  
	 * 
	 * @param v the visitor
	 *
	 * @exception StandardException on error
	 */
	public Visitable accept(Visitor v) 
		throws StandardException
	{
		if (v.skipChildren(this))
		{
			return v.visit(this);
		}

		Visitable returnNode = super.accept(v);

		if (resultColumns != null && !v.stopTraversal())
		{
			resultColumns = (ResultColumnList)resultColumns.accept(v);
		}

		if (joinClause != null && !v.stopTraversal())
		{
			joinClause = (ValueNode)joinClause.accept(v);
		}

		if (usingClause != null && !v.stopTraversal())
		{
			usingClause = (ResultColumnList)usingClause.accept(v);
		}

		return returnNode;
	}

	// This method returns the table references in Join node, and this may be
	// needed for LOJ reordering.  For example, we may have the following query:
	//       (T JOIN S) LOJ (X LOJ Y) 
    // The top most LOJ may be a join betw T and X and thus we can reorder the
	// LOJs.  However, as of 10/2002, we don't reorder LOJ mixed with join.
	public JBitSet LOJgetReferencedTables(int numTables)
				throws StandardException
	{
		JBitSet map = new JBitSet(numTables);

		map = (JBitSet) leftResultSet.LOJgetReferencedTables(numTables);
		if (map == null) return null;
		else map.or((JBitSet) rightResultSet.LOJgetReferencedTables(numTables));

		return map;
	}
	
}
