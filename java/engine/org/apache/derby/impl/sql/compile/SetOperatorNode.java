/*

   Derby - Class org.apache.derby.impl.sql.compile.SetOperatorNode

   Copyright 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.compile.AccessPath;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizablePredicate;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.iapi.util.JBitSet;

import java.util.HashMap;

/**
 * A SetOperatorNode represents a UNION, INTERSECT, or EXCEPT in a DML statement. Binding and optimization
 * preprocessing is the same for all of these operations, so they share bind methods in this abstract class.
 *
 * The class contains a boolean telling whether the operation should eliminate
 * duplicate rows.
 *
 * @author Jeff Lichtman
 */

public abstract class SetOperatorNode extends TableOperatorNode
{
	/**
	** Tells whether to eliminate duplicate rows.  all == TRUE means do
	** not eliminate duplicates, all == FALSE means eliminate duplicates.
	*/
	boolean			all;

	OrderByList orderByList;

	// List of scoped predicates for pushing during optimization.
	PredicateList leftOptPredicates;
	PredicateList rightOptPredicates;

	// List of original (unscoped) predicates that we tried to push
	// during the most recent phase of optimization.
	PredicateList pushedPredicates;

	// Mapping of original predicates to scoped predicates, used to
	// avoid re-scoping predicates unnecessarily.
	HashMap leftScopedPreds;
	HashMap rightScopedPreds;

	/**
	 * Initializer for a SetOperatorNode.
	 *
	 * @param leftResult		The ResultSetNode on the left side of this union
	 * @param rightResult		The ResultSetNode on the right side of this union
	 * @param all				Whether or not this is an ALL.
	 * @param tableProperties	Properties list associated with the table
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void init(
					Object leftResult,
					Object rightResult,
					Object all,
					Object tableProperties)
			throws StandardException
	{
		super.init(leftResult, rightResult, tableProperties);

		this.all = ((Boolean) all).booleanValue();

		/* resultColumns cannot be null, so we make a copy of the left RCL
		 * for now.  At bind() time, we need to recopy the list because there
		 * may have been a "*" in the list.  (We will set the names and
		 * column types at that time, as expected.)
		 */
		resultColumns = leftResultSet.getResultColumns().copyListAndObjects();
	}

	/**
	 * @see Optimizable#modifyAccessPath
	 *
	 * @exception StandardException		Thrown on error
	 */
	public Optimizable modifyAccessPath(JBitSet outerTables,
		PredicateList predList) throws StandardException
	{
		// When we optimized this node we attempted to push predicates down to
		// the children, which means the best access path for the children
		// might depend on those predicates.  So now that we're preparing
		// to generate the best paths, we have to push those same predicates
		// down again (this is the last time) so that the children can use
		// them as appropriate. NOTE: If our final choice for join strategy
		// is a hash join, then we do not push the predicates because we'll
		// need them to be at this level in order to find out which of them
		// is the equijoin predicate that is required by hash join.
		if ((predList != null) &&
			!getTrulyTheBestAccessPath().getJoinStrategy().isHashJoin())
		{
			for (int i = predList.size() - 1; i >= 0; i--)
				if (pushOptPredicate(predList.getOptPredicate(i)))
					predList.removeOptPredicate(i);
		}

		/*
		 * It's possible that we tried to push a predicate down to this node's
		 * children but failed to do so.  This can happen if this node's
		 * children both match the criteria for pushing a predicate (namely,
		 * they reference base tables) but the children's children do not.
		 * Ex.
		 *  select * from
		 *    (select i,j from t2 UNION
		 *      values (1,1),(2,2),(3,3),(4,4) UNION
		 *      select i,j from t1
		 *    ) x0 (i,j),
		 *    t5 where x0.i = t5.i;
		 *
		 * This will yield a tree resembling the following:
		 *
		 *                     UNION
		 *                    /     \
		 *               UNION     SELECT (T1)
		 *              /     \
		 *        SELECT (T2)  VALUES
		 *
		 * In this case the top UNION ("this") will push the predicate down,
		 * but the second UNION will _not_ push the predicate because
		 * it can't be pushed to the VALUES clause.  This means that
		 * after we're done modifying the paths for "this" node (the top
		 * UNION), the predicate will still be sitting in our leftOptPredicates
		 * list.  If that's the case, then we have to make sure the predicate,
		 * which was _not_ enforced in the left child, is enforced at this
		 * level.  We do that by generating a ProjectRestrictNode above this
		 * node.  Yes, this means the predicate will actually be applied
		 * twice to the right child (in this case), but that's okay as it
		 * won't affect the results.
		 */

		// Get the cost estimate for this node so that we can put it in
		// the new ProjectRestrictNode, if one is needed.
		CostEstimate ce = getFinalCostEstimate();

		// Modify this node's access paths.
		ResultSetNode topNode = (ResultSetNode)modifyAccessPath(outerTables);

		/* Now see if there are any left over predicates; if so, then we
		 * have to generate a ProjectRestrictNode.  Note: we walk the
		 * entire chain of UnionNodes (if there is a chain) and see if
		 * any UnionNode at any level has un-pushed predicates; if so, then
		 * we use a PRN to enforce the predicate at this, the top-most
		 * UnionNode.
		 */
		if (hasUnPushedPredicates())
		{
			// When we generate the project restrict node, we pass in the
			// "pushedPredicates" list because that has the predicates in
			// _unscoped_ form, which means they are intended for _this_
			// node instead of this node's children.  That's exactly what
			// we want.
			ResultSetNode prnRSN = (ResultSetNode) getNodeFactory().getNode(
				C_NodeTypes.PROJECT_RESTRICT_NODE,
				topNode,					// Child ResultSet
				topNode.getResultColumns(),	// Projection
				null,						// Restriction
				pushedPredicates,			// Restriction as PredicateList
				null,						// Subquerys in Projection
				null,						// Subquerys in Restriction
				null,						// Table properties
				getContextManager());
			prnRSN.costEstimate = ce.cloneMe();
			prnRSN.setReferencedTableMap(topNode.getReferencedTableMap());
			topNode = prnRSN;
		}

		return (Optimizable)topNode;
	}

	/**
	 * @see org.apache.derby.iapi.sql.compile.Optimizable#pushOptPredicate
	 *
	 * Take a predicate and push it down to both the left AND right result
	 * sets.  Return "true" if we successfully pushed it to both sides,
	 * and "false" otherwise.  The assumption is that if we return "true",
	 * the caller will take the predicate and remove it from its own list
	 * of predicates to evaluate; if we return false, then the predicate
	 * will be evaluated at the level of the caller.  So returning "false"
	 * means that the left and right result sets for this node will be fully
	 * returned, and then the predicate will be evaluated against the
	 * <set-operator> of those result sets (as of DERBY-805, the only set
	 * operator calling this method is UnionNode).  If we can push the
	 * predicate down to both children, though, we can evaluate it closer
	 * to store, which means that each child result set returns only the
	 * correctly qualified rows, and thus the calling set operator will
	 * have a smaller result set on which to operate, which can boost
	 * performance.
	 *
	 * That said, if we can't push the predicate to _both_ sides, we don't
	 * push it at all.  The reason is that if we push to one side but not
	 * to the other, we would have to ask the question of whether we should
	 * return "true" (meaning that the predicate would be removed from the
	 * caller's list and thus would _not_ be evaluated at the <set-operator>
	 * level) or "false" (meaning that the caller would keep the predicate
	 * and evaluate it at the <set-operator> level).  Depending on the query
	 * in question, both answers could end up returning incorrect results.
	 *
	 * For example, if we push it to the right but not to the left, then
	 * leave it in the caller's list, the optimizer for the caller might
	 * decide to use the predicate to do a hash join with some outer result
	 * set (if the predicate is an equijoin predicate).  That would result
	 * in materialization of the calling node and of its children--but since
	 * we pushed a predicate that depends on the outer table down into the
	 * right child, materialization of the right child will only return the
	 * rows that join with the _first_ row of the outer result set, which 
	 * is wrong.
	 *
	 * If, on the other hand, we push the predicate to one side and then tell
	 * the caller to remove it from its list, the side to which we did _not_
	 * push the predicate could return rows that aren't qualified.  Then,
	 * since the caller removed the predicate from its list, it (the caller)
	 * will not evaluate the predicate on its own result set--and thus we
	 * can end up returning rows that we weren't supposed to return.
	 * 
	 * So all of that said, only push (and return "true") if we think we
	 * can push the predicate to both sides.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean pushOptPredicate(OptimizablePredicate optimizablePredicate)
		throws StandardException
	{
		// This method was added to SetOperatorNode as part of DERBY-805,
		// which was only targeted for UnionNodes.  So for now, we don't
		// do anything if "this" isn't a Union.  This check can be removed
		// when support for other SetOperators is added.
		if (!(this instanceof UnionNode))
			return false;

		// We only handle certain types of predicates here; if the received
		// predicate doesn't qualify, then don't push it.
		Predicate pred = (Predicate)optimizablePredicate;
		if (!pred.pushableToSubqueries())
			return false;

		// Check to see if the child nodes reference any base tables; if either
		// child does not reference at least one base table, then we don't try
		// to push the predicate.
		boolean canPush = false;

		JBitSet tableNums = new JBitSet(getReferencedTableMap().size());
		BaseTableNumbersVisitor btnVis =
			new BaseTableNumbersVisitor(tableNums);

		// Check the left child.
		leftResultSet.accept(btnVis);
		canPush = (tableNums.getFirstSetBit() != -1);

		/* If we can't push it to _both_ children, then we don't push at all.
		 * RESOLVE: We can add the ability to push a predicate to one side
		 * only by putting a ProjectRestrictNode between the union node and
		 * the child as a place to park the predicate. To make things simple,
		 * we might want to always put ProjectRestrictNodes under both sides
		 * of the union during preprocessing (i.e. after binding but before
		 * optimization). In some cases the extra nodes won't be needed, but
		 * PRNs (and the corresponding ProjectRestrictResultSets) are cheap.
		 * Also, we could eliminate unnecessary ProjectRestrictNodes at the
		 * end of optimization (possibly in modifyAccessPaths()).  Until all
		 * of that is implemented, though, we only push if we can push to
		 * both sides...
		 */
		if (!canPush)
			return false;

		// Check the right child.
		tableNums.clearAll();
		rightResultSet.accept(btnVis);
		canPush = (tableNums.getFirstSetBit() != -1);
		if (!canPush)
			return false;

		BinaryRelationalOperatorNode opNode =
			(BinaryRelationalOperatorNode)pred.getAndNode().getLeftOperand();

		// Note: we assume we only get here for predicates with col refs on
		// both sides; if that ever changes, the following cast will need
		// to be updated accordingly.
		boolean opWasRemapped = 
			((ColumnReference)opNode.getLeftOperand()).hasBeenRemapped();

		/* If there is a ProjectRestrictNode directly above this node,
		 * then the predicate in question may have been remapped to this
		 * SetOperatorNode before we got here (see pushOptPredicate() in
		 * ProjectRestrictNode).  If we leave it mapped when we try to
		 * get scoped predicates for the left and right result sets, the
		 * underlying column references of the scoped predicates will
		 * effectively be doubly-mapped (i.e. mapped more than once), which
		 * can cause problems at code generation time.  So we 1) un-remap
		 * the predicate here, 2) get the scoped predicates, and then
		 * 3) remap the predicate again at the end of this method.
		 */
		RemapCRsVisitor rcrv = null;
		if (opWasRemapped)
		{
			rcrv = new RemapCRsVisitor(false);
			pred.getAndNode().accept(rcrv);
		}

		// Get a list of all of the underlying base tables that this node
		// references.  We pass this down when scoping so that we can tell
		// if the operands are actually supposed to be scoped to _this_
		// node's children.  Note that in order for the predicate to
		// have been pushed this far, at least one of its operands must
		// apply to this node--we don't know which one it is, though,
		// so we use this tableNums info to figure that out.
		tableNums.clearAll();
		this.accept(btnVis);

		/* What we want to do here is push the predicate to the left/right
		 * child.  That means that we need to find the equivalent column(s)
		 * in each child.
		 * Ex:
		 * 
		 *  select * from
		 *    (select i,j from t1 union select i,j from t2) X1,
		 *    (select a,b from t3 union select a,b from t4) X2
		 *  where X1.j = X2.b;
		 *
		 * In this example, X1.j maps to "t1" for the left side of the
		 * union (X1) and "t2" for the right side of the union.  So we have
		 * to get versions of the predicate that are appropriate to each
		 * side.  That's what the call to getPredScopedForResultSet()
		 * in the following code does.
		 */

		// See if we already have a scoped version of the predicate cached,
		// and if so just use that.
		Predicate scopedPred = null;
		if (leftScopedPreds == null)
			leftScopedPreds = new HashMap();
		else
			scopedPred = (Predicate)leftScopedPreds.get(pred);
		if (scopedPred == null)
		{
			scopedPred = pred.getPredScopedForResultSet(
				tableNums, leftResultSet);
			leftScopedPreds.put(pred, scopedPred);
		}

		// Add the scoped predicate to our list for the left child.
		getLeftOptPredicateList().addOptPredicate(scopedPred);

		scopedPred = null;
		if (rightScopedPreds == null)
			rightScopedPreds = new HashMap();
		else
			scopedPred = (Predicate)rightScopedPreds.get(pred);
		if (scopedPred == null)
		{
			scopedPred = pred.getPredScopedForResultSet(
				tableNums, rightResultSet);
			rightScopedPreds.put(pred, scopedPred);
		}

		// Add the scoped predicate to our list for the right child.
		getRightOptPredicateList().addOptPredicate(scopedPred);

		// Restore the original predicate to the way it was before we got
		// here--i.e. remap it again if needed.
		if (opWasRemapped)
		{
			rcrv = new RemapCRsVisitor(true);
			pred.getAndNode().accept(rcrv);
		}

		// Add the predicate (in its original form) to our list of predicates
		// that we've pushed during this phase of optimization.  We need to
		// keep this list of pushed predicates around so that we can do
		// a "pull" of them later, if needed.  We also need this list for
		// cases where predicates are not pushed all the way down; see
		// modifyAccessPaths() in this class for more.
		if (pushedPredicates == null)
			pushedPredicates = new PredicateList();

		pushedPredicates.addOptPredicate(pred);
		return true;
	}

	/**
	 * @see Optimizable#pullOptPredicates
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void pullOptPredicates(
		OptimizablePredicateList optimizablePredicates)
		throws StandardException
	{
		if (pushedPredicates == null)
		// we didn't push anything, so nothing to pull.
			return;

		// It's possible that we tried to push a predicate down to this
		// SetOperatorNode's children but weren't actually able to do so
		// (see modifyAccessPaths() in this class for details on when that
		// can happen).  In that case the predicates will still be sitting
		// in the left/right predicate list; we can ignore them here by
		// just discarding them.  When it comes time to modifyAccessPaths,
		// though, we'll handle them correctly--i.e. we'll generate a
		// ProjectRestrictNode over this node to ensure the predicates are
		// enforced.

		if (leftOptPredicates != null)
			leftOptPredicates.removeAllElements();

		if (rightOptPredicates != null)
			rightOptPredicates.removeAllElements();

		/* Note that predicates which have been explicitly scoped should
		 * not be pulled.  The reason is that a scoped predicate can only
		 * be pushed to a specific, target result set.  When it comes time
		 * to pull the predicate up, there's no need to pull the scoped
		 * predicate because it, by definition, was only intended for this
		 * specific result set and therefore cannot be pushed anywhere else.
		 * So at "pull" time, we can just discard the scoped predicates.  We
		 * do, however, need to pull the original, unscoped predicate from
		 * which the scoped predicate was created because we can potentially
		 * push that predicate elsewhere
		 */
		Predicate pred = null;
		for (int i = 0; i < pushedPredicates.size(); i++)
		{
			pred = (Predicate)pushedPredicates.getOptPredicate(i);
			if (pred.isScopedForPush())
				continue;
			optimizablePredicates.addOptPredicate(pred);
		}

		// We're done with the pushedPredicates list, so clear it out
		// in preparation for another phase of optimization.
		pushedPredicates.removeAllElements();
	}

	/**
	 * It's possible that we tried to push predicates to this node's
	 * children but failed to do so. This can happen if this node's
	 * children both satisfy the criteria for pushing a predicate
	 * (namely, they reference base tables) but the children's
	 * children do not (see modifyAccessPaths() above for an example
	 * of how that can happen).  So this method will walk the chain
	 * of nodes beneath this one and determine if any SetOperatorNode
	 * at any level has predicates that were not successfully pushed
	 * to both of its children (note: this currently only applies
	 * to UnionNodes).
	 *
	 * @return True if any UnionNode (or actually, any SetOperatorNode)
	 *  in the chain of SetOperatorNodes (starting with this one) has
	 *  unpushed predicates; false otherwise.
	 */
	protected boolean hasUnPushedPredicates()
	{
		// Check this node.
		if (((leftOptPredicates != null) && (leftOptPredicates.size() > 0)) ||
			((rightOptPredicates != null) && (rightOptPredicates.size() > 0)))
		{
			return true;
		}

		// Now check the children.
		if ((leftResultSet instanceof SetOperatorNode) &&
			((SetOperatorNode)leftResultSet).hasUnPushedPredicates())
		{
			return true;
		}

		return ((rightResultSet instanceof SetOperatorNode) &&
			((SetOperatorNode)rightResultSet).hasUnPushedPredicates());
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
			return 	"all: " + all + "\n" +
				"orderByList: " + 
				(orderByList != null ? orderByList.toString() : "null") + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
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
	}

	/**
	 * Build the RCL for this node.  We propagate the RCL up from the
	 * left child to form this node's RCL.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	private void buildRCL() throws StandardException
	{
		/* Verify that both sides of the union have the same # of columns in their
		 * RCL.
		 */
		if (leftResultSet.getResultColumns().size() !=
			rightResultSet.getResultColumns().size())
		{
			throw StandardException.newException(SQLState.LANG_UNION_UNMATCHED_COLUMNS,
                                                 getOperatorName());
		}

		/* We need to recreate resultColumns for this node, since there
		 * may have been 1 or more *'s in the left's SELECT list.
		 */
		resultColumns = leftResultSet.getResultColumns().copyListAndObjects();

		/* Create new expressions with the dominant types after verifying
		 * union compatibility between left and right sides.
		 */
		resultColumns.setUnionResultExpression(rightResultSet.getResultColumns(), tableNumber, level, getOperatorName());
	}

	/**
	 * Bind the result columns of a table constructor to the types in the
	 * given ResultColumnList.  Use when inserting from a table constructor,
	 * and there are nulls in the values clauses.
	 *
	 * @param rcl	The ResultColumnList with the types to bind to
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public void bindUntypedNullsToResultColumns(ResultColumnList rcl)
				throws StandardException
	{
		/*
		** If the RCL from the parent is null, then
		** the types are coming from the union itself.
		** So we have to cross check the two child
		** rcls.
		*/
		if (rcl == null)
		{
			ResultColumnList lrcl = rightResultSet.getResultColumns();
			ResultColumnList rrcl = leftResultSet.getResultColumns();

			leftResultSet.bindUntypedNullsToResultColumns(rrcl);
			rightResultSet.bindUntypedNullsToResultColumns(lrcl);
		}
		else	
		{
			leftResultSet.bindUntypedNullsToResultColumns(rcl);
			rightResultSet.bindUntypedNullsToResultColumns(rcl);
		}			
	}

	/**
	 * Get the parameter types from the given RowResultSetNode into the
	 * given array of types.  If an array position is already filled in,
	 * don't clobber it.
	 *
	 * @param types	The array of types to fill in
	 * @param rrsn	The RowResultSetNode from which to take the param types
	 *
	 * @return	The number of new types found in the RowResultSetNode
	 */
	int getParamColumnTypes(DataTypeDescriptor[] types, RowResultSetNode rrsn)
	{
		int	numTypes = 0;

		/* Look for columns where we have not found a non-? yet. */
		for (int i = 0; i < types.length; i++)
		{
			if (types[i] == null)
			{
				ResultColumn rc =
					(ResultColumn) rrsn.getResultColumns().elementAt(i);
				if ( ! (rc.getExpression().isParameterNode()))
				{
					types[i] = rc.getExpressionType();
					numTypes++;
				}
			}
		}

		return numTypes;
	}

	/**
	 * Set the type of each ? parameter in the given RowResultSetNode
	 * according to its ordinal position in the given array of types.
	 *
	 * @param types	An array of types containing the proper type for each
	 *				? parameter, by ordinal position.
	 * @param rrsn	A RowResultSetNode that could contain ? parameters whose
	 *				types need to be set.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void setParamColumnTypes(DataTypeDescriptor[] types, RowResultSetNode rrsn)
					throws StandardException
	{
		/*
		** Look for ? parameters in the result column list
		** of each RowResultSetNode
		*/
		ResultColumnList rrcl = rrsn.getResultColumns();
		int rrclSize = rrcl.size();
		for (int index = 0; index < rrclSize; index++)
		{
			ResultColumn	rc = (ResultColumn) rrcl.elementAt(index);

			if (rc.getExpression().isParameterNode())
			{
				/*
				** We found a ? - set its type to the type from the
				** type array.
				*/
				((ParameterNode) rc.getExpression()).setDescriptor(
											types[index]);
			}
		}
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
		leftResultSet.bindTargetExpressions(fromListParam);
		rightResultSet.bindTargetExpressions(fromListParam);
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
	 * @return The preprocessed ResultSetNode that can be optimized
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ResultSetNode preprocess(int numTables,
									GroupByList gbl,
									FromList fromList)
								throws StandardException
	{
		ResultSetNode newTop = this;

		/* RESOLVE - what does numTables and referencedTableMap mean here? */
		leftResultSet = leftResultSet.preprocess(numTables, gbl, fromList);
		rightResultSet = rightResultSet.preprocess(numTables, gbl, fromList);

		/* Build the referenced table map (left || right) */
		referencedTableMap = (JBitSet) leftResultSet.getReferencedTableMap().clone();
		referencedTableMap.or((JBitSet) rightResultSet.getReferencedTableMap());

		/* If this is a UNION without an all and we have
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
		if ((! all) && orderByList != null && orderByList.allAscending())
		{
			/* Order by list currently restricted to columns in select
			 * list, so we will always eliminate the order by here.
			 */
			if (orderByList.isInOrderPrefix(resultColumns))
			{
				orderByList = null;
			}
			/* RESOLVE - We currently only eliminate the order by if it is
			 * a prefix of the select list.  We do not currently do the 
			 * elimination if the order by is not a prefix because the code
			 * doesn't work.  The problem has something to do with the
			 * fact that we generate additional nodes between the union
			 * and the PRN (for reordering that we would generate here)
			 * when modifying the access paths.  VCNs under the PRN can be
			 * seen as correlated since their source resultset is the Union
			 * which is no longer the result set directly under them.  This
			 * causes the wrong code to get generated. (jerry - 11/3/98)
			 * (bug 59)
			 */
		}

		return newTop;
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
		/* Check both sides - SELECT * is not valid on either side */
		leftResultSet.verifySelectStarSubquery(outerFromList, subqueryType);
		rightResultSet.verifySelectStarSubquery(outerFromList, subqueryType);
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
		/* We search both sides for a TableOperatorNode (join nodes)
		 * but only the left side for a UnionNode.
		 */
		return leftResultSet.getFromTableByName(name, schemaName, exactMatch);
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
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void setResultToBooleanTrueNode(boolean onlyConvertAlls)
				throws StandardException
	{
		super.setResultToBooleanTrueNode(onlyConvertAlls);
		leftResultSet.setResultToBooleanTrueNode(onlyConvertAlls);
		rightResultSet.setResultToBooleanTrueNode(onlyConvertAlls);
	}

	/**
	 * This ResultSet is the source for an Insert.  The target RCL
	 * is in a different order and/or a superset of this RCL.  In most cases
	 * we will reorder and/or add defaults to the current RCL so that is
	 * matches the target RCL.  Those RSNs whose generate() method does
	 * not handle projects will insert a PRN, with a new RCL which matches
	 * the target RCL, above the current RSN.
	 * NOTE - The new or enhanced RCL will be fully bound.
	 *
	 * @param numTargetColumns	# of columns in target RCL
	 * @param colMap[]			int array representation of correspondence between
	 *							RCLs - colmap[i] = -1 -> missing in current RCL
	 *								   colmap[i] = j -> targetRCL(i) <-> thisRCL(j+1)
	 * @param dataDictionary	DataDictionary to use
	 * @param targetTD			TableDescriptor for target if the target is not a VTI, null if a VTI
     * @param targetVTI         Target description if it is a VTI, null if not a VTI
	 *
	 * @return ResultSetNode	The new top of the tree
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ResultSetNode enhanceRCLForInsert(int numTargetColumns, int[] colMap, 
											 DataDictionary dataDictionary,
											 TableDescriptor targetTD,
                                             FromVTI targetVTI)
			throws StandardException
	{
		// our newResultCols are put into the bound form straight away.
		ResultColumnList newResultCols =
								(ResultColumnList) getNodeFactory().getNode(
												C_NodeTypes.RESULT_COLUMN_LIST,
												getContextManager());
		int numResultSetColumns = resultColumns.size();

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
				// getResultColumn uses 1-based positioning, so offset the colMap entry appropriately
				oldResultColumn = resultColumns.getResultColumn(colMap[index]+1);

				newColumnReference = (ColumnReference) getNodeFactory().getNode(
												C_NodeTypes.COLUMN_REFERENCE,
												oldResultColumn.getName(),
												null,
												getContextManager());
				/* The ColumnReference points to the source of the value */
				newColumnReference.setSource(oldResultColumn);
				// colMap entry is 0-based, columnId is 1-based.
				newColumnReference.setType(oldResultColumn.getExpressionType());

				// Source of an insert, so nesting levels must be 0
				newColumnReference.setNestingLevel(0);
				newColumnReference.setSourceLevel(0);

				// because the insert already copied the target table's
				// column descriptors into the result, we grab it from there.
				// alternatively, we could do what the else clause does,
				// and look it up in the DD again.
				newResultColumn = (ResultColumn) getNodeFactory().getNode(
						C_NodeTypes.RESULT_COLUMN,
						oldResultColumn.getType(),
						newColumnReference,
						getContextManager());
			}
			else
			{
				newResultColumn = genNewRCForInsert(targetTD, targetVTI, index + 1, dataDictionary);
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
		return (ResultSetNode) getNodeFactory().getNode(
									C_NodeTypes.PROJECT_RESTRICT_NODE,
									this,
									newResultCols,
									null,
									null,
									null,
									null,
									tableProperties,
									getContextManager());
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
	public boolean flattenableInFromSubquery(FromList fromList)
	{
		/* Unions in FromSubquerys are not flattenable.	 */
		return false;
	}

	/**
	 * Return whether or not to materialize this ResultSet tree.
	 *
	 * @return Whether or not to materialize this ResultSet tree.
	 *			would return valid results.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean performMaterialization(JBitSet outerTables)
		throws StandardException
	{
		// RESOLVE - just say no to materialization right now - should be a cost based decision
		return false;

		/* Actual materialization, if appropriate, will be placed by our parent PRN.
		 * This is because PRN might have a join condition to apply.  (Materialization
		 * can only occur before that.
		 */
		//return true;
	}

    /**
     * @return the operator name: "UNION", "INTERSECT", or "EXCEPT"
     */
    abstract String getOperatorName();

	/**
	 * Retrieve the list of optimizable predicates that are
	 * targeted for the left child.  Create a new (empty)
	 * list if the list is null.
	 */
	protected PredicateList getLeftOptPredicateList()
		throws StandardException
	{
		if (leftOptPredicates == null) {
			leftOptPredicates =
				(PredicateList) getNodeFactory().getNode(
					C_NodeTypes.PREDICATE_LIST,
					getContextManager());
		}

		return leftOptPredicates;
	}

	/**
	 * Retrieve the list of optimizable predicates that are
	 * targeted for the right child.  Create a new (empty)
	 * list if the list is null.
	 */
	protected PredicateList getRightOptPredicateList()
		throws StandardException
	{
		if (rightOptPredicates == null) {
			rightOptPredicates =
				(PredicateList) getNodeFactory().getNode(
					C_NodeTypes.PREDICATE_LIST,
					getContextManager());
		}

		return rightOptPredicates;
	}

}
