/*

   Derby - Class org.apache.derby.impl.sql.compile.HalfOuterJoinNode

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
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;

import org.apache.derby.iapi.util.JBitSet;

import java.util.Properties;

/**
 * An HalfOuterJoinNode represents a left or a right outer join result set.
 * Right outer joins are always transformed into left outer joins during
 * preprocessing for simplicity.
 *
 * @author Jerry Brenner
 */

public class HalfOuterJoinNode extends JoinNode
{
	private boolean rightOuterJoin;
	private boolean transformed = false;

	/**
	 * Initializer for a HalfOuterJoinNode.
	 *
	 * @param leftResult		The ResultSetNode on the left side of this join
	 * @param rightResult		The ResultSetNode on the right side of this join
	 * @param onClause			The ON clause
	 * @param usingClause		The USING clause
	 * @param rightOuterJoin	Whether or not this node represents a user
	 *							specified right outer join
	 * @param tableProperties	Properties list associated with the table
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void init(
							Object leftResult,
							Object rightResult,
							Object onClause,
							Object usingClause,
							Object rightOuterJoin,
							Object tableProperties)
		throws StandardException
	{
		super.init(
				leftResult,
				rightResult,
				onClause,
				usingClause,
				null,
				tableProperties);
		this.rightOuterJoin = ((Boolean) rightOuterJoin).booleanValue();

		/* We can only flatten an outer join
		 * using the null intolerant predicate xform.
		 * In that case, we will return an InnerJoin.
		 */
		flattenableJoin = false;
	}

	/*
	 *  Optimizable interface
	 */

	/**
	 * @see Optimizable#pushOptPredicate
	 *
	 * @exception StandardException		Thrown on error
	 */

	public boolean pushOptPredicate(OptimizablePredicate optimizablePredicate)
			throws StandardException
	{
		/* We should never push the predicate to joinPredicates as in JoinNode.  joinPredicates
		 * should only be predicates relating the two joining tables.  In the case of half join,
		 * it is biased.  If the general predicate (not join predicate) contains refernce to right
		 * result set, and if doesn't qualify, we shouldn't return the row for the result to be
		 * correct, but half join will fill right side NULL and return the row.  So we can only
		 * push predicate to the left, as we do in "pushExpression".  bug 5055
		 */
		FromTable		leftFromTable = (FromTable) leftResultSet;
		if (leftFromTable.getReferencedTableMap().contains(optimizablePredicate.getReferencedMap()))
			return leftFromTable.pushOptPredicate(optimizablePredicate);
		return false;
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
			return 	"rightOuterJoin: " + rightOuterJoin + "\n" +
				"transformed: " + transformed + "\n" + 
				super.toString();
		}
		else
		{
			return "";
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

		/* Transform right outer joins to the equivalent left outer join */
		if (rightOuterJoin)
		{
			/* Verify that a user specifed right outer join is transformed into
			 * a left outer join exactly once.
			 */
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(! transformed,
					"Attempting to transform a right outer join multiple times");
			}

			ResultSetNode tmp = leftResultSet;

			leftResultSet = rightResultSet;
			rightResultSet = tmp;
			transformed = true;
		}
		
		newTreeTop = super.preprocess(numTables, gbl, fromList);

		return newTreeTop;
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

		/* We only try to push single table predicates to the left.
		 * Pushing them to the right would give incorrect semantics.
		 * We use the logic for pushing down single table predicates here.
		 */
		pushExpressionsToLeft(outerPredicateList);

		/* Push the pushable outer join predicates to the right.  This is done
		 * bottom up, hence at the end of this method, so that outer join
		 * conditions only get pushed down 1 level.
		 * We use the optimizer's logic for pushing down join clause here.
		 */
		// Walk joinPredicates backwards due to possible deletes
		for (int index = joinPredicates.size() - 1; index >= 0; index --)
		{
			Predicate predicate;

			predicate = (Predicate) joinPredicates.elementAt(index);
			if (! predicate.getPushable())
			{
				continue;
			}

			getRightPredicateList().addPredicate(predicate);

			/* Remove the matching predicate from the outer list */
			joinPredicates.removeElementAt(index);
		}

		/* Recurse down both sides of tree */
		PredicateList	noPredicates =
						(PredicateList) getNodeFactory().getNode(
											C_NodeTypes.PREDICATE_LIST,
											getContextManager());
		leftFromTable.pushExpressions(getLeftPredicateList());
		rightFromTable.pushExpressions(noPredicates);
	}

	/**
	 * This method determines if (1) the query is a LOJ, and (2) if the LOJ is a candidate for
	 * reordering (i.e., linearization).  The condition for LOJ linearization is:
	 * 1. only LOJ in the fromList, i.e., no INNER, no FULL JOINs, no ROJs
	 * 2. ON clause must be equality join between left and right operands and in CNF (i.e., AND is allowed)
	 */
	public boolean LOJ_reorderable(int numTables)
		throws StandardException
	{
		boolean anyChange = false;

		ResultSetNode logicalLeftResultSet;  // row-preserving side
		ResultSetNode logicalRightResultSet; // null-producing side

		// Figure out which is the row-preserving side and which is
		// null-producing side.
		if (rightOuterJoin)
		{ // right outer join
			logicalLeftResultSet  = rightResultSet;
			logicalRightResultSet = leftResultSet;
		}
		else 
		{
			logicalLeftResultSet  = leftResultSet;
			logicalRightResultSet = rightResultSet;
		}
		
		// Redundantly normalize the ON predicate (it will also be called in preprocess()).
		super.normExpressions();

		// This is a very simple LOJ of base tables. Do nothing.
		if (logicalLeftResultSet instanceof FromBaseTable &&
			logicalRightResultSet instanceof FromBaseTable)
			return anyChange;

		// Recursively check if we can reordering LOJ, and build the table
		// references. Note that joins may have been reordered and therefore the
		// table references need to be recomputed.
		if (logicalLeftResultSet instanceof HalfOuterJoinNode)
		{
			anyChange =	((HalfOuterJoinNode)logicalLeftResultSet).LOJ_reorderable(numTables) || anyChange;
		}
		else if (!(logicalLeftResultSet instanceof FromBaseTable))
		{// left operand must be either a base table or another LOJ
			// In principle, we don't care about the left operand.  However, we
			// need to re-bind the resultColumns.  If the left operand is a
			// view, we may have to re-bind the where clause etc...
			// We ran into difficulty for the following query:
			//  create view v8 (cv, bv, av) as (select c, b, a from t union select f, e, d from s);
			//  select * from v8 left outer join (s left outer join r on (f = i)) on (e=v8.bv);
			return anyChange;
		}

		if (logicalRightResultSet instanceof HalfOuterJoinNode)
		{
			anyChange = ((HalfOuterJoinNode)logicalRightResultSet).LOJ_reorderable(numTables) || anyChange;
		}
		else if (!(logicalRightResultSet instanceof FromBaseTable))
		{// right operand must be either a base table or another LOJ
			return anyChange;
		}

		// It is much easier to do LOJ reordering if there is no ROJ.
		// However, we ran into some problem downstream when we transform an ROJ
		// into LOJ -- transformOuterJoin() didn't expect ROJ to be transformed
		// into LOJ alread.  So, we skip optimizing ROJ at the moment.
		if (rightOuterJoin || (logicalRightResultSet instanceof HalfOuterJoinNode && 
							   ((HalfOuterJoinNode)logicalRightResultSet).rightOuterJoin))
		{
			return LOJ_bindResultColumns(anyChange);
		}

		// Build the data structure for testing/doing LOJ reordering.
		// Fill in the table references on row-preserving and null-producing sides.
		// It may be possible that either operand is a complex view.
		JBitSet				NPReferencedTableMap; // Null-producing
		JBitSet				RPReferencedTableMap; // Row-preserving

		RPReferencedTableMap = logicalLeftResultSet.LOJgetReferencedTables(numTables);
		NPReferencedTableMap = logicalRightResultSet.LOJgetReferencedTables(numTables);

		if ((RPReferencedTableMap == null || NPReferencedTableMap == null) &&
			anyChange)
		{
			return LOJ_bindResultColumns(anyChange);
		}
			
		// Check if the predicate is equality predicate in CNF (i.e., AND only)
		// and left/right column references must come from either operand.
		// That is, we don't allow:
		// 1. A=A
		// 2. 1=1
		// 3. B=C where both B and C are either from left or right operand.

		// we probably need to make the joinClause "left-deep" so that we can
		// walk it easier.
		BinaryRelationalOperatorNode equals;
		ValueNode leftCol;
		ValueNode rightCol;
		AndNode   and;
		ValueNode left;
		ValueNode vn = joinClause;
		while (vn instanceof AndNode)
		{
			and = (AndNode) vn;
			left = and.getLeftOperand();

			// Make sure that this is an equijoin of the form "C = D" where C
			// and D references tables from both left and right operands.
			if (left instanceof RelationalOperator &&
				((ValueNode)left).isBinaryEqualsOperatorNode())
			{
				equals = (BinaryRelationalOperatorNode) left;
				leftCol = equals.getLeftOperand();
				rightCol = equals.getRightOperand();

				if (!( leftCol instanceof ColumnReference && rightCol instanceof ColumnReference))
					return LOJ_bindResultColumns(anyChange);

				boolean refCheck = false;
				boolean leftOperandCheck = false;

				if (RPReferencedTableMap.get(((ColumnReference)leftCol).getTableNumber()))
				{
					refCheck = true;
					leftOperandCheck = true;
				}
				else if (NPReferencedTableMap.get(((ColumnReference)leftCol).getTableNumber()))
				{
					refCheck = true;
				}

				if (refCheck == false)
					return LOJ_bindResultColumns(anyChange);

				refCheck = false;
				if (leftOperandCheck == false && RPReferencedTableMap.get(((ColumnReference)rightCol).getTableNumber()))
				{
					refCheck = true;
				}
				else if (leftOperandCheck == true && NPReferencedTableMap.get(((ColumnReference)rightCol).getTableNumber()))
				{
					refCheck = true;
				}

				if (refCheck == false)
					return LOJ_bindResultColumns(anyChange);
			}
			else return LOJ_bindResultColumns(anyChange); //  get out of here

			vn = and.getRightOperand();
		}

		// Check if the logical right resultset is a composite inner and as such
		// that this current LOJ can be pushed through it.
		boolean       push = false;
		// logical right operand is another LOJ... so we may be able to push the
		// join
		if (logicalRightResultSet instanceof HalfOuterJoinNode)
		{
			// get the Null-producing operand of the child
			JBitSet  logicalNPRefTableMap = ((HalfOuterJoinNode)logicalRightResultSet).LOJgetNPReferencedTables(numTables);

			// does the current LOJ join predicate reference
			// logicalNPRefTableMap?  If not, we can push the current
			// join.
			vn = joinClause;
			push = true;
			while (vn instanceof AndNode)
			{
				and = (AndNode) vn;
				left = and.getLeftOperand();
				equals = (BinaryRelationalOperatorNode) left;
				leftCol = equals.getLeftOperand();
				rightCol = equals.getRightOperand();

				if (logicalNPRefTableMap.get(((ColumnReference)leftCol).getTableNumber()) ||
					logicalNPRefTableMap.get(((ColumnReference)rightCol).getTableNumber()))
				{
					push = false;
					break;
				}

				vn = and.getRightOperand();
			}
		}

		// Push the current LOJ into the next level
		if (push)
		{
			// For safety, check the JoinNode data members: they should null or
			// empty list before we proceed.
			if (super.subqueryList.size() != 0 ||
				((JoinNode)logicalRightResultSet).subqueryList.size() != 0 ||
				super.joinPredicates.size() != 0 ||
				((JoinNode)logicalRightResultSet).joinPredicates.size() != 0 ||
				super.usingClause != null ||
				((JoinNode)logicalRightResultSet).usingClause != null)
				return LOJ_bindResultColumns(anyChange); //  get out of here

			anyChange = true; // we are reordering the LOJs.

			ResultSetNode tmp = logicalLeftResultSet;
			ResultSetNode LChild, RChild;

			//            this LOJ
			//            /      \
			//  logicalLeftRS   LogicalRightRS
			//                   /     \
			//                LChild  RChild
			// becomes
			//
			//               this LOJ
			//               /      \
			//     LogicalRightRS   RChild
			//           /     \
			// logicalLeftRS   LChild  <<<  we need to be careful about this order
			//                              as the "LogicalRightRS may be a ROJ
			//

			// handle the lower level LOJ node
			LChild = ((HalfOuterJoinNode)logicalRightResultSet).leftResultSet;
			RChild = ((HalfOuterJoinNode)logicalRightResultSet).rightResultSet;

			((HalfOuterJoinNode)logicalRightResultSet).rightResultSet = LChild;
			((HalfOuterJoinNode)logicalRightResultSet).leftResultSet  = tmp;

			// switch the ON clause
			vn = joinClause;
			joinClause   = ((HalfOuterJoinNode)logicalRightResultSet).joinClause;
			((HalfOuterJoinNode)logicalRightResultSet).joinClause = vn;

			// No need to switch HalfOuterJoinNode data members for now because
			// we are handling only LOJ.
			// boolean local_rightOuterJoin = rightOuterJoin;
			// boolean local_transformed    = transformed;
			// rightOuterJoin = ((HalfOuterJoinNode)logicalRightResultSet).rightOuterJoin;
			// transformed    = ((HalfOuterJoinNode)logicalRightResultSet).transformed;
			// ((HalfOuterJoinNode)logicalRightResultSet).rightOuterJoin = local_rightOuterJoin;
			// ((HalfOuterJoinNode)logicalRightResultSet).transformed    = local_transformed;

			FromList localFromList = (FromList) getNodeFactory().getNode(
																		 C_NodeTypes.FROM_LIST,
																		 getNodeFactory().doJoinOrderOptimization(),
																		 getContextManager());

			// switch LOJ nodes: by handling the current LOJ node
			leftResultSet  = logicalRightResultSet;
			rightResultSet = RChild;

			// rebuild the result columns and re-bind column references
			((HalfOuterJoinNode)leftResultSet).resultColumns = null;
			((JoinNode)leftResultSet).bindResultColumns(localFromList); // localFromList is empty

			// left operand must be another LOJ, try again until a fixpoint
			boolean localChange = ((HalfOuterJoinNode)leftResultSet).LOJ_reorderable(numTables);

			// rebuild the result columns and re-bind column references for 'this'
			return LOJ_bindResultColumns(anyChange);
		}

		return LOJ_bindResultColumns(anyChange);
	}

	// This method re-binds the result columns which may be referenced in the ON
	// clause in this node.
	public boolean LOJ_bindResultColumns(boolean anyChange)
		throws StandardException
	{
		if (anyChange)
		{
			this.resultColumns = null;
			FromList localFromList = (FromList) getNodeFactory().getNode(C_NodeTypes.FROM_LIST,
																		 getNodeFactory().doJoinOrderOptimization(),
																		 getContextManager());
			((JoinNode)this).bindResultColumns(localFromList);
		}
		return anyChange;
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
		ResultSetNode innerRS;

		if (predicateTree == null)
		{
			/* We can't transform this node, so tell both sides of the 
			 * outer join that they can't get flattened into outer query block.
			 */
			leftResultSet.notFlattenableJoin();
			rightResultSet.notFlattenableJoin();
			return this;
		}

		super.transformOuterJoins(predicateTree, numTables);

		JBitSet innerMap = new JBitSet(numTables);
		if (rightOuterJoin)
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(! transformed,
					"right OJ not expected to be transformed into left OJ yet");
			}
			innerRS = leftResultSet;
		}
		else
		{
			innerRS = rightResultSet;
		}

		innerRS.fillInReferencedTableMap(innerMap);

		/* Walk predicates looking for 
		 * a null intolerant predicate on the inner table.
		 */
		ValueNode vn = predicateTree;
		while (vn instanceof AndNode)
		{
			AndNode and = (AndNode) vn;
			ValueNode left = and.getLeftOperand();

			/* Skip IS NULL predicates as they are not null intolerant */
			if (left.isInstanceOf(C_NodeTypes.IS_NULL_NODE))
			{
				vn = and.getRightOperand();
				continue;
			}

			/* Only consider predicates that are relops */
			if (left instanceof RelationalOperator)
			{
				JBitSet refMap = new JBitSet(numTables);
				/* Do not consider method calls, 
				 * conditionals, field references, etc. */
				if (! (left.categorize(refMap, true)))
				{
					vn = and.getRightOperand();
					continue;
				}

				/* If the predicate is a null intolerant predicate
				 * on the right side then we can flatten to an
				 * inner join.  We do the xform here, flattening
				 * will happen later.
				 */
				for (int bit = 0; bit < numTables; bit++)
				{
					if (refMap.get(bit) && innerMap.get(bit))
					{
						// OJ -> IJ
						JoinNode ij =  (JoinNode)
											getNodeFactory().getNode(
												C_NodeTypes.JOIN_NODE,
												leftResultSet,
												rightResultSet,
												joinClause,
												null,
												resultColumns,
												null,
												getContextManager());
						ij.setTableNumber(tableNumber);
						ij.setSubqueryList(subqueryList);
						ij.setAggregateVector(aggregateVector);
						return ij;
					}
				}
			}

			vn = and.getRightOperand();
		}

		/* We can't transform this node, so tell both sides of the 
		 * outer join that they can't get flattened into outer query block.
		 */
		leftResultSet.notFlattenableJoin();
		rightResultSet.notFlattenableJoin();

		return this;
	}

	/** @see JoinNode#adjustNumberOfRowsReturned */
	protected void adjustNumberOfRowsReturned(CostEstimate costEstimate)
	{
		/*
		** An outer join returns at least as many rows as in the outer
		** table. Even if this started as a right outer join, it will
		** have been transformed to a left outer join by this point.
		*/
		CostEstimate outerCost = getLeftResultSet().getCostEstimate();

		if (costEstimate.rowCount() < outerCost.rowCount())
		{
			costEstimate.setCost(costEstimate.getEstimatedCost(),
								 outerCost.rowCount(),
								 outerCost.rowCount());
		}
	}

    /**
     * Generate the code for an inner join node.
	 *
	 * @exception StandardException		Thrown on error
     */
	public void generate(ActivationClassBuilder acb,
								MethodBuilder mb)
						throws StandardException
	{
		/* Verify that a user specifed right outer join is transformed into
		 * a left outer join exactly once.
		 */
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(rightOuterJoin == transformed,
				"rightOuterJoin (" + rightOuterJoin +
				") is expected to equal transformed (" + transformed + ")");
		}
		super.generateCore(acb, mb, LEFTOUTERJOIN);
	}

	/**
	 * Generate	and add any arguments specifict to outer joins.
	 * Generate	the methods (and add them as parameters) for
	 * returning an empty row from 1 or more sides of an outer join,
	 * if required.  Pass whether or not this was originally a
	 * right outer join.
	 *
	 * @param acb		The ActivationClassBuilder
	 * @param mb the method the generate code is to go into
	 *
	 * return The args that have been added
	 *
	 * @exception StandardException		Thrown on error
	 */
	 protected int addOuterJoinArguments(ActivationClassBuilder acb,
											MethodBuilder mb)
		 throws StandardException
	 {
		/* Nulls always generated from the right */
		rightResultSet.getResultColumns().generateNulls(acb, mb);

		/* Was this originally a right outer join? */
		mb.push(rightOuterJoin);

		return 2;
	 }

	/**
	 * Return the number of arguments to the join result set.
	 */
	protected int getNumJoinArguments()
	{
		/* We add two more arguments than the superclass does */
		return super.getNumJoinArguments() + 2;
	}

	protected void oneRowRightSide(ActivationClassBuilder acb,
									   MethodBuilder mb)
	{
		// always return false for now
		mb.push(false);
		mb.push(false);  //isNotExists?
	}

	/**
	 * Return the logical left result set for this qualified
	 * join node.
	 * (For RIGHT OUTER JOIN, the left is the right
	 * and the right is the left and the JOIN is the NIOJ).
	 */
	ResultSetNode getLogicalLeftResultSet()
	{
		if (rightOuterJoin)
		{
			return rightResultSet;
		}
		else
		{
			return leftResultSet;
		}
	}

	/**
	 * Return the logical right result set for this qualified
	 * join node.
	 * (For RIGHT OUTER JOIN, the left is the right
	 * and the right is the left and the JOIN is the NIOJ).
	 */
	ResultSetNode getLogicalRightResultSet()
	{
		if (rightOuterJoin)
		{
			return leftResultSet;
		}
		else
		{
			return rightResultSet;
		}
	}

	/**
	 * Return true if right outer join or false if left outer join
	 * Used to set Nullability correctly in JoinNode
	 */
	public boolean isRightOuterJoin()
	{
		return rightOuterJoin;
	}

	// return the Null-producing table references
	public JBitSet LOJgetNPReferencedTables(int numTables)
				throws StandardException
	{
		if (rightOuterJoin && !transformed)
			return (JBitSet) leftResultSet.LOJgetReferencedTables(numTables);
		else
			return (JBitSet) rightResultSet.LOJgetReferencedTables(numTables);
	}
}
