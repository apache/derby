/*

   Derby - Class org.apache.derby.impl.sql.compile.ProjectRestrictNode

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

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizablePredicate;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.OptimizableList;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.compile.RequiredRowOrdering;
import org.apache.derby.iapi.sql.compile.RowOrdering;
import org.apache.derby.iapi.sql.compile.AccessPath;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;
import org.apache.derby.impl.sql.compile.ActivationClassBuilder;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.catalog.types.ReferencedColumnsDescriptorImpl;
import org.apache.derby.iapi.util.JBitSet;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import java.util.Properties;
import java.util.HashSet;
import java.util.Set;

/**
 * A ProjectRestrictNode represents a result set for any of the basic DML
 * operations: SELECT, INSERT, UPDATE, and DELETE.  For INSERT with
 * a VALUES clause, restriction will be null. For both INSERT and UPDATE,
 * the resultColumns in the selectList will contain the names of the columns
 * being inserted into or updated.
 *
 * NOTE: A ProjectRestrictNode extends FromTable since it can exist in a FromList.
 *
 */

public class ProjectRestrictNode extends SingleChildResultSetNode
{
	/**
	 * The ValueNode for the restriction to be evaluated here.
	 */
	public ValueNode	restriction;

	/**
	 * Constant expressions to be evaluated here.
	 */
	ValueNode	constantRestriction = null;

	/**
	 * Restriction as a PredicateList
	 */
	public PredicateList restrictionList;

	/**
	 * List of subqueries in projection
	 */
	SubqueryList projectSubquerys;

	/**
	 * List of subqueries in restriction
	 */
	SubqueryList restrictSubquerys;

	private boolean accessPathModified;

	private boolean accessPathConsidered;

	private boolean childResultOptimized;

	private boolean materialize;

	/* Should we get the table number from this node,
	 * regardless of the class of our child.
	 */
	private boolean getTableNumberHere;

	/**
	 * Initializer for a ProjectRestrictNode.
	 *
	 * @param childResult	The child ResultSetNode
	 * @param projection	The result column list for the projection
	 * @param restriction	An expression representing the restriction to be 
	 *					    evaluated here.
	 * @param restrictionList Restriction as a PredicateList
	 * @param projectSubquerys List of subqueries in the projection
	 * @param restrictSubquerys List of subqueries in the restriction
	 * @param tableProperties	Properties list associated with the table
	 */

	public void init(
							Object childResult,
			 				Object projection,
							Object restriction,
							Object restrictionList,
							Object projectSubquerys,
							Object restrictSubquerys,
							Object tableProperties)
	{
		super.init(childResult, tableProperties);
		resultColumns = (ResultColumnList) projection;
		this.restriction = (ValueNode) restriction;
		this.restrictionList = (PredicateList) restrictionList;
		this.projectSubquerys = (SubqueryList) projectSubquerys;
		this.restrictSubquerys = (SubqueryList) restrictSubquerys;

		/* A PRN will only hold the tableProperties for
		 * a result set tree if its child is not an
		 * optimizable.  Otherwise, the properties will
		 * be transferred down to the child.
		 */
		if (tableProperties != null &&
			 (childResult instanceof Optimizable))
		{
			((Optimizable) childResult).setProperties(getProperties());
			setProperties((Properties) null);
		}
	}

	/*
	 *  Optimizable interface
	 */

	/**
		@see Optimizable#nextAccessPath
		@exception StandardException	Thrown on error
	 */
	public boolean nextAccessPath(Optimizer optimizer,
									OptimizablePredicateList predList,
									RowOrdering rowOrdering)
			throws StandardException
	{
		/*
		** If the child result set is an optimizable, let it choose its next
		** access path.  If it is not an optimizable, we have to tell the
		** caller that there is an access path the first time we are called
		** for this position in the join order, and that there are no more
		** access paths for subsequent calls for this position in the join
		** order.  The startOptimizing() method is called once on each
		** optimizable when it is put into a join position.
		*/
		if (childResult instanceof Optimizable)
		{
			return ((Optimizable) childResult).nextAccessPath(optimizer,
																restrictionList,
																rowOrdering);
		}
		else
		{
			return super.nextAccessPath(optimizer, predList, rowOrdering);
		}
	}

	/** @see Optimizable#rememberAsBest 
		@exception StandardException	Thrown on error
	 */
	public void rememberAsBest(int planType, Optimizer optimizer)
		throws StandardException
	{
		super.rememberAsBest(planType, optimizer);
		if (childResult instanceof Optimizable)
			((Optimizable) childResult).rememberAsBest(planType, optimizer);
	}

	/* Don't print anything for a PRN, as their
	 * child has the interesting info.
	 */
	void printRememberingBestAccessPath(int planType, AccessPath bestPath)
	{
	}

	/** @see Optimizable#startOptimizing */
	public void startOptimizing(Optimizer optimizer, RowOrdering rowOrdering)
	{
		if (childResult instanceof Optimizable)
		{
			((Optimizable) childResult).startOptimizing(optimizer, rowOrdering);
		}
		else
		{
			accessPathConsidered = false;

			super.startOptimizing(optimizer, rowOrdering);
		}
	}

	/** @see Optimizable#getTableNumber */
	public int getTableNumber()
	{
		/* GROSS HACK - We need to get the tableNumber after
		 * calling modifyAccessPaths() on the child when doing
		 * a hash join on an arbitrary result set.  The problem
		 * is that the child will always be an optimizable at this
		 * point.  So, we 1st check to see if we should get it from
		 * this node.  (We set the boolean to true in the appropriate
		 * place in modifyAccessPaths().)
		 */
		if (getTableNumberHere)
		{
			return super.getTableNumber();
		}

		if (childResult instanceof Optimizable)
			return ((Optimizable) childResult).getTableNumber();

		return super.getTableNumber();
	}

	/**
	 * @see Optimizable#optimizeIt
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
		/*
		** RESOLVE: Most types of Optimizables only implement estimateCost(),
		** and leave it up to optimizeIt() in FromTable to figure out the
		** total cost of the join.  A ProjectRestrict can have a non-Optimizable
		** child, though, in which case we want to tell the child the
		** number of outer rows - it could affect the join strategy
		** significantly.  So we implement optimizeIt() here, which overrides
		** the optimizeIt() in FromTable.  This assumes that the join strategy
		** for which this join node is the inner table is a nested loop join,
		** which will not be a valid assumption when we implement other
		** strategies like materialization (hash join can work only on
		** base tables).  The join strategy for a base table under a
		** ProjectRestrict is set in the base table itself.
		*/

		CostEstimate childCost;

		costEstimate = getCostEstimate(optimizer);

		/*
		** Don't re-optimize a child result set that has already been fully
		** optimized.  For example, if the child result set is a SelectNode,
		** it will be changed to a ProjectRestrictNode, which we don't want
		** to re-optimized.
		*/
		// NOTE: TO GET THE RIGHT COST, THE CHILD RESULT MAY HAVE TO BE
		// OPTIMIZED MORE THAN ONCE, BECAUSE THE NUMBER OF OUTER ROWS
		// MAY BE DIFFERENT EACH TIME.
		// if (childResultOptimized)
		// 	return costEstimate;

		// It's possible that a call to optimize the left/right will cause
		// a new "truly the best" plan to be stored in the underlying base
		// tables.  If that happens and then we decide to skip that plan
		// (which we might do if the call to "considerCost()" below decides
		// the current path is infeasible or not the best) we need to be
		// able to revert back to the "truly the best" plans that we had
		// saved before we got here.  So with this next call we save the
		// current plans using "this" node as the key.  If needed, we'll
		// then make the call to revert the plans in OptimizerImpl's
		// getNextDecoratedPermutation() method.
		updateBestPlanMap(ADD_PLAN, this);

		/* If the childResult is instanceof Optimizable, then we optimizeIt.
		 * Otherwise, we are going into a new query block.  If the new query
		 * block has already had its access path modified, then there is
		 * nothing to do.  Otherwise, we must begin the optimization process
		 * anew on the new query block.
		 */
		if (childResult instanceof Optimizable)
		{
			childCost = ((Optimizable) childResult).optimizeIt(
															optimizer,
															restrictionList,
															outerCost,
															rowOrdering);
			/* Copy child cost to this node's cost */
			costEstimate.setCost(
							childCost.getEstimatedCost(),
							childCost.rowCount(),
							childCost.singleScanRowCount());


			// Note: we don't call "optimizer.considerCost()" here because
			// a) the child will make that call as part of its own
			// "optimizeIt()" work above, and b) the child might have
			// different criteria for "considering" (i.e. rejecting or
			// accepting) a plan's cost than this ProjectRestrictNode does--
			// and we don't want to override the child's decision.  So as
			// with most operations in this class, if the child is an
			// Optimizable, we just let it do its own work and make its
			// own decisions.
		}
		else if ( ! accessPathModified)
		{
			if (SanityManager.DEBUG)
			{
				if (! ((childResult instanceof SelectNode) ||
								 (childResult instanceof RowResultSetNode)))
				{
					SanityManager.THROWASSERT(
						"childResult is expected to be instanceof " +
						"SelectNode or RowResultSetNode - it is a " +
						childResult.getClass().getName());
				}
			}
			childResult = childResult.optimize(optimizer.getDataDictionary(), 
											   restrictionList,
											   outerCost.rowCount());

			/* Copy child cost to this node's cost */
			childCost = childResult.costEstimate;

			costEstimate.setCost(
							childCost.getEstimatedCost(),
							childCost.rowCount(),
							childCost.singleScanRowCount());

			/* Note: Prior to the fix for DERBY-781 we had calls here
			 * to set the cost estimate for BestAccessPath and
			 * BestSortAvoidancePath to equal costEstimate.  That used
			 * to be okay because prior to DERBY-781 we would only
			 * get here once (per join order) for a given SelectNode/
			 * RowResultSetNode and thus we could safely say that the
			 * costEstimate from the most recent call to "optimize()"
			 * was the best one so far (because we knew that we would
			 * only call childResult.optimize() once).  Now that we
			 * support hash joins with subqueries, though, we can get
			 * here twice per join order: once when the optimizer is
			 * considering a nested loop join with this PRN, and once
			 * when it is considering a hash join.  This means we can't
			 * just arbitrarily use the cost estimate for the most recent
			 * "optimize()" as the best cost because that may not
			 * be accurate--it's possible that the above call to
			 * childResult.optimize() was for a hash join, but that
			 * we were here once before (namely for nested loop) and
			 * the cost of the nested loop is actually less than
			 * the cost of the hash join.  In that case it would
			 * be wrong to use costEstimate as the cost of the "best"
			 * paths because it (costEstimate) holds the cost of
			 * the hash join, not of the nested loop join.  So with
			 * DERBY-781 the following calls were removed:
			 *   getBestAccessPath().setCostEstimate(costEstimate);
			 *   getBestSortAvoidancePath().setCostEstimate(costEstimate);
			 * If costEstimate *does* actually hold the estimate for
			 * the best path so far, then we will set BestAccessPath
			 * and BestSortAvoidancePath as needed in the following
			 * call to "considerCost".
			 */

			// childResultOptimized = true;

			/* RESOLVE - ARBITRARYHASHJOIN - Passing restriction list here, as above, is correct.
			 * However,  passing predList makes the following work:
			 *	select * from t1, (select * from t2) c properties joinStrategy = hash where t1.c1 = c.c1;
			 * The following works with restrictionList:
			 *	select * from t1, (select c1 + 0 from t2) c(c1) properties joinStrategy = hash where t1.c1 = c.c1;
			 */
			optimizer.considerCost(this, restrictionList, getCostEstimate(), outerCost);
		}

		return costEstimate;
	}

	/**
	 * @see Optimizable#feasibleJoinStrategy
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean feasibleJoinStrategy(OptimizablePredicateList predList,
										Optimizer optimizer)
					throws StandardException
	{
		AccessPath ap;

		/* The child being an Optimizable is a special case.  In that
		 * case, we want to get the current access path and join strategy
		 * from the child.  Otherwise, we want to get it from this node.
		 */
		if (childResult instanceof Optimizable)
		{
			// With DERBY-805 it's possible that, when considering a nested
			// loop join with this PRN, we pushed predicates down into the
			// child if the child is a UNION node.  At this point, though, we
			// may be considering doing a hash join with this PRN instead of a
			// nested loop join, and if that's the case we need to pull any
			// predicates back up so that they can be searched for equijoins
			// that will in turn make the hash join possible.  So that's what
			// the next call does.  Two things to note: 1) if no predicates
			// were pushed, this call is a no-op; and 2) if we get here when
			// considering a nested loop join, the predicates that we pull
			// here (if any) will be re-pushed for subsequent costing/ 
			// optimization as necessary (see OptimizerImpl.costPermutation(),
			// which will call this class's optimizeIt() method and that's
			// where the predicates are pushed down again).
			if (childResult instanceof UnionNode)
				((UnionNode)childResult).pullOptPredicates(restrictionList);

			return ((Optimizable) childResult).
				feasibleJoinStrategy(restrictionList, optimizer);
		}
		else
		{
			return super.feasibleJoinStrategy(restrictionList, optimizer);
		}
	}

	/** @see Optimizable#getCurrentAccessPath */
	public AccessPath getCurrentAccessPath()
	{
		if (childResult instanceof Optimizable)
			return ((Optimizable) childResult).getCurrentAccessPath();

		return super.getCurrentAccessPath();
	}

	/** @see Optimizable#getBestAccessPath */
	public AccessPath getBestAccessPath()
	{
		if (childResult instanceof Optimizable)
			return ((Optimizable) childResult).getBestAccessPath();

		return super.getBestAccessPath();
	}

	/** @see Optimizable#getBestSortAvoidancePath */
	public AccessPath getBestSortAvoidancePath()
	{
		if (childResult instanceof Optimizable)
			return ((Optimizable) childResult).getBestSortAvoidancePath();

		return super.getBestSortAvoidancePath();
	}

	/** @see Optimizable#getTrulyTheBestAccessPath */
	public AccessPath getTrulyTheBestAccessPath()
	{
		/* The childResult will always be an Optimizable
		 * during code generation.  If the childResult was
		 * not an Optimizable during optimization, then this node
		 * will have the truly the best access path, so we want to
		 * return it from this node, rather than traversing the tree.
		 * This can happen for non-flattenable derived tables.
		 * Anyway, we note this state when modifying the access paths.
		 */
		if (hasTrulyTheBestAccessPath)
		{
			return super.getTrulyTheBestAccessPath();
		}

		if (childResult instanceof Optimizable)
			return ((Optimizable) childResult).getTrulyTheBestAccessPath();

		return super.getTrulyTheBestAccessPath();
	}

	/** @see Optimizable#rememberSortAvoidancePath */
	public void rememberSortAvoidancePath()
	{
		if (childResult instanceof Optimizable)
			((Optimizable) childResult).rememberSortAvoidancePath();
		else
			super.rememberSortAvoidancePath();
	}

	/** @see Optimizable#considerSortAvoidancePath */
	public boolean considerSortAvoidancePath()
	{
		if (childResult instanceof Optimizable)
			return ((Optimizable) childResult).considerSortAvoidancePath();

		return super.considerSortAvoidancePath();
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

		/* Add the matching predicate to the restrictionList */
		if (restrictionList == null)
		{
			restrictionList = (PredicateList) getNodeFactory().getNode(
													C_NodeTypes.PREDICATE_LIST,
													getContextManager());
		}
		restrictionList.addPredicate((Predicate) optimizablePredicate);

		/* Remap all of the ColumnReferences to point to the
		 * source of the values.
		 */
		Predicate pred = (Predicate)optimizablePredicate;

		/* If the predicate is scoped then the call to "remapScopedPred()"
		 * will do the necessary remapping for us and will return true;
		 * otherwise, we'll just do the normal remapping here.
		 */
		if (!pred.remapScopedPred())
		{
			RemapCRsVisitor rcrv = new RemapCRsVisitor(true);
			pred.getAndNode().accept(rcrv);
		}

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
        // DERBY-4001: Don't pull predicates if this node is part of a NOT
        // EXISTS join. For example, in the query below, if we allowed the
        // predicate 1<>1 (always false) to be pulled, no rows would be
        // returned, whereas it should return all the rows in table T.
        // SELECT * FROM T WHERE NOT EXISTS (SELECT * FROM T WHERE 1<>1)
		if (restrictionList != null && !isNotExists())
		{
			// Pull up any predicates that may have been pushed further
			// down the tree during optimization.
			if (childResult instanceof UnionNode)
				((UnionNode)childResult).pullOptPredicates(restrictionList);

			RemapCRsVisitor rcrv = new RemapCRsVisitor(false);
			for (int i = restrictionList.size() - 1; i >= 0; i--)
			{
				OptimizablePredicate optPred =
					restrictionList.getOptPredicate(i);
				((Predicate) optPred).getAndNode().accept(rcrv);
				optimizablePredicates.addOptPredicate(optPred);
				restrictionList.removeOptPredicate(i);
			}
		}
	}

	/**
	 * @see Optimizable#modifyAccessPath
	 *
	 * @exception StandardException		Thrown on error
	 */
	public Optimizable modifyAccessPath(JBitSet outerTables) 
		throws StandardException
	{
		boolean origChildOptimizable = true;

		/* It is okay to optimize most nodes multiple times.  However,
		 * modifying the access path is something that should only be done
		 * once per node.  One reason for this is that the predicate list
		 * will be empty after the 1st call, and we assert that it should
		 * be non-empty.  Multiple calls to modify the access path can
		 * occur when there is a non-flattenable FromSubquery (or view).
		 */
		if (accessPathModified)
		{
			return this;
		}

		/*
		** Do nothing if the child result set is not optimizable, as there
		** can be nothing to modify.
		*/
		boolean alreadyPushed = false;
		if ( ! (childResult instanceof Optimizable))
		{
			// Remember that the original child was not Optimizable
			origChildOptimizable = false;

			/* When we optimized the child we passed in our restriction list
			 * so that scoped predicates could be pushed further down the
			 * tree.  We need to do the same when modifying the access
			 * paths to ensure we generate the same plans the optimizer
			 * chose.
			 */
			childResult = childResult.modifyAccessPaths(restrictionList);

			/* Mark this node as having the truly ... for
			 * the underlying tree.
			 */
			hasTrulyTheBestAccessPath = true;

			/* Replace this PRN with a HRN if we are doing a hash join */
			if (trulyTheBestAccessPath.getJoinStrategy().isHashJoin())
			{
				if (SanityManager.DEBUG)
				{
					SanityManager.ASSERT(restrictionList != null,
						"restrictionList expected to be non-null");
					SanityManager.ASSERT(restrictionList.size() != 0,
							"restrictionList.size() expected to be non-zero");
				}
				/* We're doing a hash join on an arbitary result set.
				 * We need to get the table number from this node when
				 * dividing up the restriction list for a hash join.
				 * We need to explicitly remember this.
				 */
				getTableNumberHere = true;
			}
			else
			{
				/* We consider materialization into a temp table as a last step.
				 * Currently, we only materialize VTIs that are inner tables
				 * and can't be instantiated multiple times.  In the future we
				 * will consider materialization as a cost based option.
				 */
				return (Optimizable) considerMaterialization(outerTables);
			}
		}

		/* If the child is not a FromBaseTable, then we want to
		 * keep going down the tree.  (Nothing to do at this node.)
		 */
		else if (!(childResult instanceof FromBaseTable))
		{
			/* Make sure that we have a join strategy */
			if (trulyTheBestAccessPath.getJoinStrategy() == null)
			{
				trulyTheBestAccessPath = (AccessPathImpl) ((Optimizable) childResult).getTrulyTheBestAccessPath();
			}

			// If the childResult is a SetOperatorNode (esp. a UnionNode),
			// then it's possible that predicates in our restrictionList are
			// supposed to be pushed further down the tree (as of DERBY-805).
			// We passed the restrictionList down when we optimized the child
			// so that the relevant predicates could be pushed further as part
			// of the optimization process; so now that we're finalizing the
			// paths, we need to do the same thing: i.e. pass restrictionList
			// down so that the predicates that need to be pushed further
			// _can_ be pushed further.
			if (childResult instanceof SetOperatorNode) {
				childResult = (ResultSetNode)
					((SetOperatorNode) childResult).modifyAccessPath(
						outerTables, restrictionList);

				// Take note of the fact that we already pushed predicates
				// as part of the modifyAccessPaths call.  This is necessary
				// because there may still be predicates in restrictionList
				// that we intentionally decided not to push (ex. if we're
				// going to do hash join then we chose to not push the join
				// predicates).  Whatever the reason for not pushing the
				// predicates, we have to make sure we don't inadvertenly
				// push them later (esp. as part of the "pushUsefulPredicates"
				// call below).
				alreadyPushed = true;
			}
			else {
				childResult = 
					(ResultSetNode) ((FromTable) childResult).
						modifyAccessPath(outerTables);
			}
		}

		// If we're doing a hash join with _this_ PRN (as opposed to
		// with this PRN's child) then we don't attempt to push
		// predicates down.  There are two reasons for this: 1)
		// we don't want to push the equijoin predicate that is
		// required for the hash join, and 2) if we're doing a
		// hash join then we're going to materialize this node,
		// but if we push predicates before materialization, we
		// can end up with incorrect results (esp. missing rows).
		// So don't push anything in this case.
		boolean hashJoinWithThisPRN = hasTrulyTheBestAccessPath &&
			(trulyTheBestAccessPath.getJoinStrategy() != null) &&
			trulyTheBestAccessPath.getJoinStrategy().isHashJoin();

		if ((restrictionList != null) && !alreadyPushed && !hashJoinWithThisPRN)
		{
			restrictionList.pushUsefulPredicates((Optimizable) childResult);
		}

		/*
		** The optimizer's decision on the access path for the child result
		** set may require the generation of extra result sets.  For
		** example, if it chooses an index, we need an IndexToBaseRowNode
		** above the FromBaseTable (and the FromBaseTable has to change
		** its column list to match that of the index.
		*/
		if (origChildOptimizable)
		{
			childResult = childResult.changeAccessPath();
		}
		accessPathModified = true;

		/*
		** Replace this PRN with a HTN if a hash join
		** is being done at this node.  (Hash join on a scan
		** is a special case and is handled at the FBT.)
		*/
		if (trulyTheBestAccessPath.getJoinStrategy() != null &&
			trulyTheBestAccessPath.getJoinStrategy().isHashJoin())
		{
			return replaceWithHashTableNode();
		}

		/* We consider materialization into a temp table as a last step.
		 * Currently, we only materialize VTIs that are inner tables
		 * and can't be instantiated multiple times.  In the future we
		 * will consider materialization as a cost based option.
		 */
		return (Optimizable) considerMaterialization(outerTables);
	}

	/**
	 * This method creates a HashTableNode between the PRN and
	 * it's child when the optimizer chooses hash join on an
	 * arbitrary (non-FBT) result set tree.
	 * We divide up the restriction list into 3 parts and
	 * distribute those parts as described below.
	 * 
	 * @return The new (same) top of our result set tree.
	 * @exception StandardException		Thrown on error
	 */
	private Optimizable replaceWithHashTableNode()
		throws StandardException
	{
		// If this PRN has TTB access path for its child, store that access
		// path in the child here, so that we can find it later when it
		// comes time to generate qualifiers for the hash predicates (we
		// need the child's access path when generating qualifiers; if we
		// don't pass the path down here, the child won't be able to find
		// it).
		if (hasTrulyTheBestAccessPath)
		{
			((FromTable)childResult).trulyTheBestAccessPath =
				(AccessPathImpl)getTrulyTheBestAccessPath();

			// If the child itself is another SingleChildResultSetNode
			// (which is also what a ProjectRestrictNode is), then tell
			// it that it is now holding TTB path for it's own child.  Again,
			// this info is needed so that child knows where to find the
			// access path at generation time.
			if (childResult instanceof SingleChildResultSetNode)
			{
				((SingleChildResultSetNode)childResult)
					.hasTrulyTheBestAccessPath = hasTrulyTheBestAccessPath;

				// While we're at it, add the PRN's table number to the
				// child's referenced map so that we can find the equijoin
				// predicate.  We have to do this because the predicate
				// will be referencing the PRN's tableNumber, not the
				// child's--and since we use the child as the target
				// when searching for hash keys (as can be seen in
				// HashJoinStrategy.divideUpPredicateLists()), the child
				// should know what this PRN's table number is.  This
				// is somewhat bizarre since the child doesn't
				// actually "reference" this PRN, but since the child's
				// reference map is used when searching for the equijoin
				// predicate (see "buildTableNumList" in
				// BinaryRelationalOperatorNode), this is the simplest
				// way to pass this PRN's table number down.
				childResult.getReferencedTableMap().set(tableNumber);
			}
		}

		/* We want to divide the predicate list into 3 separate lists -
		 *	o predicates against the source of the hash table, which will
		 *	  be applied on the way into the hash table (searchRestrictionList)
		 *  o join clauses which are qualifiers and get applied to the
		 *	  rows in the hash table on a probe (joinRestrictionList)
		 *	o non-qualifiers involving both tables which will get
		 *	  applied after a row gets returned from the HTRS (nonQualifiers)
		 *
		 * We do some unnecessary work when doing this as we want to reuse
		 * as much existing code as possible.  The code that we are reusing
		 * was originally built for hash scans, hence the unnecessary
		 * requalification list.
		 */
		PredicateList searchRestrictionList =
								(PredicateList) getNodeFactory().getNode(
													C_NodeTypes.PREDICATE_LIST,
													getContextManager());
		PredicateList joinQualifierList =
								(PredicateList) getNodeFactory().getNode(
													C_NodeTypes.PREDICATE_LIST,
													getContextManager());
		PredicateList requalificationRestrictionList =
								(PredicateList) getNodeFactory().getNode(
													C_NodeTypes.PREDICATE_LIST,
													getContextManager());
		trulyTheBestAccessPath.getJoinStrategy().divideUpPredicateLists(
											this,
											restrictionList,
											searchRestrictionList,
											joinQualifierList,
											requalificationRestrictionList,
											getDataDictionary());

		/* Break out the non-qualifiers from HTN's join qualifier list and make that
		 * the new restriction list for this PRN.
		 */
		restrictionList = (PredicateList) getNodeFactory().getNode(
											C_NodeTypes.PREDICATE_LIST,
											getContextManager());
        /* For non-base table, we remove first 2 lists from requal list to avoid adding duplicates.
         */
		for (int i = 0; i < searchRestrictionList.size(); i++)
			requalificationRestrictionList.removeOptPredicate((Predicate) searchRestrictionList.elementAt(i));
		for (int i = 0; i < joinQualifierList.size(); i++)
			requalificationRestrictionList.removeOptPredicate((Predicate) joinQualifierList.elementAt(i));

		joinQualifierList.transferNonQualifiers(this, restrictionList); //purify joinQual list
		requalificationRestrictionList.copyPredicatesToOtherList(restrictionList); //any residual

		ResultColumnList	htRCList;

		/* We get a shallow copy of the child's ResultColumnList and its 
		 * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
		 */
		htRCList = childResult.getResultColumns();
		childResult.setResultColumns(htRCList.copyListAndObjects());

		/* Replace ResultColumn.expression with new VirtualColumnNodes
		 * in the HTN's ResultColumnList.  (VirtualColumnNodes include
		 * pointers to source ResultSetNode, this, and source ResultColumn.)
		 * NOTE: We don't want to mark the underlying RCs as referenced, otherwise
		 * we won't be able to project out any of them.
		 */
		htRCList.genVirtualColumnNodes(childResult, childResult.getResultColumns(), false);

		/* The CRs for this side of the join in both the searchRestrictionList
		 * the joinQualifierList now point to the HTN's RCL.  We need them
		 * to point to the RCL in the child of the HTN.  (We skip doing this for
		 * the joinQualifierList as the code to generate the Qualifiers does not
		 * care.)
		 */
		RemapCRsVisitor rcrv = new RemapCRsVisitor(true);
		searchRestrictionList.accept(rcrv);

		/* We can finally put the HTN between ourself and our old child. */
		childResult = (ResultSetNode) getNodeFactory().getNode(
											C_NodeTypes.HASH_TABLE_NODE,
											childResult,
											tableProperties, 
											htRCList,
											searchRestrictionList, 
											joinQualifierList,
											trulyTheBestAccessPath, 
											getCostEstimate(),
											projectSubquerys,
											restrictSubquerys,
											hashKeyColumns(),
											getContextManager());
		return this;
	}

	/** @see Optimizable#verifyProperties 
	 * @exception StandardException		Thrown on error
	 */
	public void verifyProperties(DataDictionary dDictionary)
		throws StandardException
	{
		/* Table properties can be attached to this node if
		 * its child is not an optimizable, otherwise they
		 * are attached to its child.
		 */

		if (childResult instanceof Optimizable)
		{
			((Optimizable) childResult).verifyProperties(dDictionary);
		}
		else
		{
			super.verifyProperties(dDictionary);
		}
	}

	/**
	 * @see Optimizable#legalJoinOrder
	 */
	public boolean legalJoinOrder(JBitSet assignedTableMap)
	{
		if (childResult instanceof Optimizable)
		{
			return ((Optimizable) childResult).legalJoinOrder(assignedTableMap);
		}
		else
		{
			return true;
		}
	}

	/**
	 * @see Optimizable#uniqueJoin
	 *
	 * @exception StandardException		Thrown on error
	 */
	public double uniqueJoin(OptimizablePredicateList predList)
					throws StandardException
	{
		if (childResult instanceof Optimizable)
		{
			return ((Optimizable) childResult).uniqueJoin(predList);
		}
		else
		{
			return super.uniqueJoin(predList);
		}
	}

	/**
	 * Return the restriction list from this node.
	 *
	 * @return	The restriction list from this node.
	 */
	PredicateList getRestrictionList()
	{
		return restrictionList;
	}

	/** 
	 * Return the user specified join strategy, if any for this table.
	 *
	 * @return The user specified join strategy, if any for this table.
	 */
	String getUserSpecifiedJoinStrategy()
	{
		if (childResult instanceof FromTable)
		{
			return ((FromTable) childResult).getUserSpecifiedJoinStrategy();
		}
		else
		{
			return userSpecifiedJoinStrategy;
		}
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

			if (restriction != null)
			{
				printLabel(depth, "restriction: ");
				restriction.treePrint(depth + 1);
			}

			if (restrictionList != null)
			{
				printLabel(depth, "restrictionList: ");
				restrictionList.treePrint(depth + 1);
			}

			if (projectSubquerys != null)
			{
				printLabel(depth, "projectSubquerys: ");
				projectSubquerys.treePrint(depth + 1);
			}

			if (restrictSubquerys != null)
			{
				printLabel(depth, "restrictSubquerys: ");
				restrictSubquerys.treePrint(depth + 1);
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
		childResult = childResult.preprocess(numTables, gbl, fromList);

		/* Build the referenced table map */
		referencedTableMap = (JBitSet) childResult.getReferencedTableMap().clone();

		return this;
	}

	/**
	 * Push expressions down to the first ResultSetNode which can do expression
	 * evaluation and has the same referenced table map.
	 * RESOLVE - This means only pushing down single table expressions to
	 * ProjectRestrictNodes today.  Once we have a better understanding of how
	 * the optimizer will work, we can push down join clauses.
	 *
	 * @param predicateList	The PredicateList.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void pushExpressions(PredicateList predicateList)
					throws StandardException
	{
		PredicateList	pushPList = null;

		if (SanityManager.DEBUG)
		SanityManager.ASSERT(predicateList != null,
							 "predicateList is expected to be non-null");

		/* Push single table predicates down to the left of an outer
		 * join, if possible.  (We need to be able to walk an entire
		 * join tree.)
		 */
		if (childResult instanceof JoinNode)
		{
			((FromTable) childResult).pushExpressions(predicateList);

		}

		/* Build a list of the single table predicates that we can push down */
		pushPList = predicateList.getPushablePredicates(referencedTableMap);

		/* If this is a PRN above a SelectNode, probably due to a 
		 * view or derived table which couldn't be flattened, then see
		 * if we can push any of the predicates which just got pushed
		 * down to our level into the SelectNode.
		 */
		if (pushPList != null &&
				(childResult instanceof SelectNode))
		{
			SelectNode childSelect = (SelectNode)childResult;

			if ( (childSelect.hasWindows()  &&
				  childSelect.orderByList != null) ) {
				// We can't push down if there is an ORDER BY and a window
				// function because that would make ROW_NUMBER give wrong
				// result:
				// E.g.
				//     SELECT * from (SELECT ROW_NUMBER() OVER (), j FROM T
				//                    ORDER BY j) WHERE j=5
				//
			} else {
				pushPList.pushExpressionsIntoSelect((SelectNode) childResult,
													false);
			}
		}


		/* DERBY-649: Push simple predicates into Unions. It would be up to UnionNode
		 * to decide if these predicates can be pushed further into underlying SelectNodes
		 * or UnionNodes.  Note, we also keep the predicateList at this
		 * ProjectRestrictNode in case the predicates are not pushable or only
		 * partially pushable.
		 *
		 * It is possible to expand this optimization in UnionNode later.
		 */
		if (pushPList != null && (childResult instanceof UnionNode))
			((UnionNode)childResult).pushExpressions(pushPList);

		if (restrictionList == null)
		{
			restrictionList = pushPList;
		}
		else if (pushPList != null && pushPList.size() != 0)
		{
			/* Concatenate the 2 PredicateLists */
			restrictionList.destructiveAppend(pushPList);
		}

		/* RESOLVE - this looks like the place to try to try to push the 
		 * predicates through the ProjectRestrict.  Seems like we should
		 * "rebind" the column references and reset the referenced table maps
		 * in restrictionList and then call childResult.pushExpressions() on
		 * restrictionList.
		 */
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
		if (restrictionList == null)
		{
			restrictionList = (PredicateList) getNodeFactory().getNode(
													C_NodeTypes.PREDICATE_LIST,
													getContextManager());
		}
		restrictionList.addPredicate(predicate);
		return this;
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
	public boolean flattenableInFromSubquery(FromList fromList)
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
	public ResultSetNode ensurePredicateList(int numTables) 
		throws StandardException
	{
		return this;
	}

	/**
	 * Optimize this ProjectRestrictNode.  
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

	public ResultSetNode optimize(DataDictionary dataDictionary,
								  PredicateList predicates,
								  double outerRows) 
					throws StandardException
	{
		/* We need to implement this method since a PRN can appear above a
		 * SelectNode in a query tree.
		 */
		childResult = childResult.optimize(dataDictionary,
											restrictionList,
											outerRows);

		Optimizer optimizer = getOptimizer(
								(FromList) getNodeFactory().getNode(
									C_NodeTypes.FROM_LIST,
									getNodeFactory().doJoinOrderOptimization(),
									this,
									getContextManager()),
								predicates,
								dataDictionary,
								(RequiredRowOrdering) null);

		// RESOLVE: SHOULD FACTOR IN THE NON-OPTIMIZABLE PREDICATES THAT
		// WERE NOT PUSHED DOWN
		costEstimate = optimizer.newCostEstimate();

		costEstimate.setCost(childResult.getCostEstimate().getEstimatedCost(),
							childResult.getCostEstimate().rowCount(),
							childResult.getCostEstimate().singleScanRowCount());

		return this;
	}

	/**
	 * Get the CostEstimate for this ProjectRestrictNode.
	 *
	 * @return	The CostEstimate for this ProjectRestrictNode, which is
	 * 			the cost estimate for the child node.
	 */
	public CostEstimate getCostEstimate()
	{
		/*
		** The cost estimate will be set here if either optimize() or
		** optimizeIt() was called on this node.  It's also possible
		** that optimization was done directly on the child node,
		** in which case the cost estimate will be null here.
		*/
		if (costEstimate == null)
			return childResult.getCostEstimate();
		else
		{
			return costEstimate;
		}
	}

	/**
	 * Get the final CostEstimate for this ProjectRestrictNode.
	 *
	 * @return	The final CostEstimate for this ProjectRestrictNode, which is
	 * 			the final cost estimate for the child node.
	 */
	public CostEstimate getFinalCostEstimate()
		throws StandardException
	{
		if (finalCostEstimate != null)
		// we already set it, so just return it.
			return finalCostEstimate;

		// If the child result set is an Optimizable, then this node's
		// final cost is that of the child.  Otherwise, this node must
		// hold "trulyTheBestAccessPath" for it's child so we pull
		// the final cost from there.
		if (childResult instanceof Optimizable)
			finalCostEstimate = childResult.getFinalCostEstimate();
		else
			finalCostEstimate = getTrulyTheBestAccessPath().getCostEstimate();

		return finalCostEstimate;
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
		if (SanityManager.DEBUG)
        SanityManager.ASSERT(resultColumns != null, "Tree structure bad");

        //
        // If we are projecting and restricting the stream from a table
        // function, then give the table function all of the information that
        // it needs in order to push the projection and qualifiers into
        // the table function. See DERBY-4357.
        //
        if ( childResult instanceof FromVTI )
        {
            ((FromVTI) childResult).computeProjectionAndRestriction( restrictionList );
        }

		generateMinion( acb, mb, false);
	}

	/**
	 * General logic shared by Core compilation.
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the expression will go into
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void generateResultSet(ExpressionClassBuilder acb,
										   MethodBuilder mb)
									throws StandardException
	{
		generateMinion( acb, mb, true);
	}

	/**
	 * Logic shared by generate() and generateResultSet().
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the expression will go into
	 *
	 * @exception StandardException		Thrown on error
	 */

	private void generateMinion(ExpressionClassBuilder acb,
									 MethodBuilder mb, boolean genChildResultSet)
									throws StandardException
	{
		/* If this ProjectRestrict doesn't do anything, bypass its generation.
		 * (Remove any true and true predicates first, as they could be left
		 * by the like transformation.)
		 */
		if (restrictionList != null && restrictionList.size() > 0)
		{
			restrictionList.eliminateBooleanTrueAndBooleanTrue();
		}

		if (nopProjectRestrict())
		{
			generateNOPProjectRestrict();
			if (genChildResultSet)
				childResult.generateResultSet(acb, mb);
			else
				childResult.generate((ActivationClassBuilder)acb, mb);
			costEstimate = childResult.getFinalCostEstimate();
			return;
		}

		// build up the tree.

		/* Put the predicates back into the tree */
		if (restrictionList != null)
		{
			constantRestriction = restrictionList.restoreConstantPredicates();
			// Remove any redundant predicates before restoring
			restrictionList.removeRedundantPredicates();
			restriction = restrictionList.restorePredicates();
			/* Allow the restrictionList to get garbage collected now
			 * that we're done with it.
			 */
			restrictionList = null;
		}

		// for the restriction, we generate an exprFun
		// that evaluates the expression of the clause
		// against the current row of the child's result.
		// if the restriction is empty, simply pass null
		// to optimize for run time performance.

   		// generate the function and initializer:
   		// Note: Boolean lets us return nulls (boolean would not)
   		// private Boolean exprN()
   		// {
   		//   return <<restriction.generate(ps)>>;
   		// }
   		// static Method exprN = method pointer to exprN;




		// Map the result columns to the source columns
		int[] mapArray = resultColumns.mapSourceColumns();
		int mapArrayItem = acb.addItem(new ReferencedColumnsDescriptorImpl(mapArray));

		/* Will this node do a projection? */
		boolean doesProjection = true;

		/* Does a projection unless same # of columns in same order
		 * as child.
		 */
		if ( (! reflectionNeededForProjection()) && 
		    mapArray != null && 
			mapArray.length == childResult.getResultColumns().size())
		{
			/* mapArray entries are 1-based */
			int index = 0;
			for ( ; index < mapArray.length; index++)
			{
				if (mapArray[index] != index + 1)
				{
					break;
				}
			}
			if (index == mapArray.length)
			{
				doesProjection = false;
			}
		}



		/* Generate the ProjectRestrictSet:
		 *	arg1: childExpress - Expression for childResultSet
		 *  arg2: Activation
		 *  arg3: restrictExpress - Expression for restriction
		 *  arg4: projectExpress - Expression for projection
		 *  arg5: resultSetNumber
		 *  arg6: constantExpress - Expression for constant restriction
		 *			(for example, where 1 = 2)
		 *  arg7: mapArrayItem - item # for mapping of source columns
		 *  arg8: reuseResult - whether or not the result row can be reused
		 *						(ie, will it always be the same)
		 *  arg9: doesProjection - does this node do a projection
		 *  arg10: estimated row count
		 *  arg11: estimated cost
		 *  arg12: close method
		 */

		acb.pushGetResultSetFactoryExpression(mb);
		if (genChildResultSet)
			childResult.generateResultSet(acb, mb);
		else
			childResult.generate((ActivationClassBuilder)acb, mb);

		/* Get the next ResultSet #, so that we can number this ResultSetNode, its
		 * ResultColumnList and ResultSet.
		 */
		assignResultSetNumber();
		
		/* Set the point of attachment in all subqueries attached
		 * to this node.
		 */
		if (projectSubquerys != null && projectSubquerys.size() > 0)
		{
			projectSubquerys.setPointOfAttachment(resultSetNumber);
		}
		if (restrictSubquerys != null && restrictSubquerys.size() > 0)
		{
			restrictSubquerys.setPointOfAttachment(resultSetNumber);
		}

		// Load our final cost estimate.
		costEstimate = getFinalCostEstimate();

		// if there is no restriction, we just want to pass null.
		if (restriction == null)
		{
		   	mb.pushNull(ClassName.GeneratedMethod);
		}
		else
		{
			// this sets up the method and the static field.
			// generates:
			// 	Object userExprFun { }
			MethodBuilder userExprFun = acb.newUserExprFun();

			// restriction knows it is returning its value;

			/* generates:
			 *    return  <restriction.generate(acb)>;
			 * and adds it to userExprFun
			 * NOTE: The explicit cast to DataValueDescriptor is required
			 * since the restriction may simply be a boolean column or subquery
			 * which returns a boolean.  For example:
			 *		where booleanColumn
			 */
			restriction.generateExpression(acb, userExprFun);
			userExprFun.methodReturn();

			// we are done modifying userExprFun, complete it.
			userExprFun.complete();

	   		// restriction is used in the final result set as an access of the new static
   			// field holding a reference to this new method.
			// generates:
			//	ActivationClass.userExprFun
			// which is the static field that "points" to the userExprFun
			// that evaluates the where clause.
   			acb.pushMethodReference(mb, userExprFun);
		}

		/* Determine whether or not reflection is needed for the projection.
		 * Reflection is not needed if all of the columns map directly to source
		 * columns.
		 */
		if (reflectionNeededForProjection())
		{
			// for the resultColumns, we generate a userExprFun
			// that creates a new row from expressions against
			// the current row of the child's result.
			// (Generate optimization: see if we can simply
			// return the current row -- we could, but don't, optimize
			// the function call out and have execution understand
			// that a null function pointer means take the current row
			// as-is, with the performance trade-off as discussed above.)

			/* Generate the Row function for the projection */
			resultColumns.generateCore(acb, mb, false);
		}
		else
		{
		   	mb.pushNull(ClassName.GeneratedMethod);
		}
		
		mb.push(resultSetNumber);

		// if there is no constant restriction, we just want to pass null.
		if (constantRestriction == null)
		{
		   	mb.pushNull(ClassName.GeneratedMethod);
		}
		else
		{
			// this sets up the method and the static field.
			// generates:
			// 	userExprFun { }
			MethodBuilder userExprFun = acb.newUserExprFun();

			// restriction knows it is returning its value;

			/* generates:
			 *    return <restriction.generate(acb)>;
			 * and adds it to userExprFun
			 * NOTE: The explicit cast to DataValueDescriptor is required
			 * since the restriction may simply be a boolean column or subquery
			 * which returns a boolean.  For example:
			 *		where booleanColumn
			 */
			constantRestriction.generateExpression(acb, userExprFun);

			userExprFun.methodReturn();

			// we are done modifying userExprFun, complete it.
			userExprFun.complete();

	   		// restriction is used in the final result set as an access
			// of the new static field holding a reference to this new method.
			// generates:
			//	ActivationClass.userExprFun
			// which is the static field that "points" to the userExprFun
			// that evaluates the where clause.
   			acb.pushMethodReference(mb, userExprFun);
		}
		
		mb.push(mapArrayItem);
		mb.push(resultColumns.reusableResult());
		mb.push(doesProjection);
		mb.push(costEstimate.rowCount());
		mb.push(costEstimate.getEstimatedCost());

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getProjectRestrictResultSet",
					ClassName.NoPutResultSet, 10);
	}

	/**
	 * Determine whether this ProjectRestrict does anything.  If it doesn't
	 * filter out any rows or columns, it's a No-Op.
	 *
	 * @return	true if this ProjectRestrict is a No-Op.
	 */
	boolean nopProjectRestrict()
	{
		/*
		** This ProjectRestrictNode is not a No-Op if it does any
		** restriction.
		*/
		if ( (restriction != null) || (constantRestriction != null) ||
			 (restrictionList != null && restrictionList.size() > 0) )
		{
			return false;
		}

		ResultColumnList	childColumns = childResult.getResultColumns();
		ResultColumnList	PRNColumns = this.getResultColumns();

		/*
		** The two lists have the same numbers of elements.  Are the lists
		** identical?  In other words, is the expression in every ResultColumn
		** in the PRN's RCL a ColumnReference that points to the same-numbered
		** column?
		*/
		if (PRNColumns.nopProjection(childColumns))
			return true;

		return false;
	}

	/**
	 * Bypass the generation of this No-Op ProjectRestrict, and just generate
	 * its child result set.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void generateNOPProjectRestrict()
			throws StandardException
	{
		this.getResultColumns().setRedundant();
	}

	/**
	 * Consider materialization for this ResultSet tree if it is valid and cost effective
	 * (It is not valid if incorrect results would be returned.)
	 *
	 * @return Top of the new/same ResultSet tree.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ResultSetNode considerMaterialization(JBitSet outerTables)
		throws StandardException
	{
		childResult = childResult.considerMaterialization(outerTables);
		if (childResult.performMaterialization(outerTables))
		{
			MaterializeResultSetNode	mrsn;
			ResultColumnList			prRCList;

			/* If the restriction contians a ColumnReference from another
			 * table then the MRSN must go above the childResult.  Otherwise we can put
			 * it above ourselves. (The later is optimal since projection and restriction 
			 * will only happen once.)
			 * Put MRSN above PRN if any of the following are true:
			 *	o  PRN doesn't have a restriction list
			 *	o  PRN's restriction list is empty 
			 *  o  Table's referenced in PRN's restriction list are a subset of
			 *	   table's referenced in PRN's childResult.  (NOTE: Rather than construct
			 *     a new, empty JBitSet before checking, we simply clone the childResult's
			 *	   referencedTableMap.  This is done for code simplicity and will not 
			 *	   affect the result.)
			 */
			ReferencedTablesVisitor rtv = new ReferencedTablesVisitor(
												(JBitSet) childResult.getReferencedTableMap().clone());
			boolean emptyRestrictionList = (restrictionList == null || restrictionList.size() == 0);
			if (! emptyRestrictionList)
			{
				restrictionList.accept(rtv);
			}
			if (emptyRestrictionList ||
				childResult.getReferencedTableMap().contains(rtv.getTableMap()))
			{
				/* We get a shallow copy of the ResultColumnList and its 
				 * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
				 */
				prRCList = resultColumns;
				setResultColumns(resultColumns.copyListAndObjects());

				/* Replace ResultColumn.expression with new VirtualColumnNodes
				 * in the NormalizeResultSetNode's ResultColumnList.  (VirtualColumnNodes include
				 * pointers to source ResultSetNode, this, and source ResultColumn.)
				 */
				prRCList.genVirtualColumnNodes(this, resultColumns);

				/* Finally, we create the new MaterializeResultSetNode */
				mrsn = (MaterializeResultSetNode) getNodeFactory().getNode(
									C_NodeTypes.MATERIALIZE_RESULT_SET_NODE,
									this,
									prRCList,
									tableProperties,
									getContextManager());
				// Propagate the referenced table map if it's already been created
				if (referencedTableMap != null)
				{
					mrsn.setReferencedTableMap((JBitSet) referencedTableMap.clone());
				}
				return mrsn;
			}
			else
			{
				/* We get a shallow copy of the ResultColumnList and its 
				 * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
				 */
				prRCList = childResult.getResultColumns();
				childResult.setResultColumns(prRCList.copyListAndObjects());

				/* Replace ResultColumn.expression with new VirtualColumnNodes
				 * in the MaterializeResultSetNode's ResultColumnList.  (VirtualColumnNodes include
				 * pointers to source ResultSetNode, this, and source ResultColumn.)
				 */
				prRCList.genVirtualColumnNodes(childResult, childResult.getResultColumns());

				/* RESOLVE - we need to push single table predicates down so that
				 * they get applied while building the MaterializeResultSet.
				 */

				/* Finally, we create the new MaterializeResultSetNode */
				mrsn = (MaterializeResultSetNode) getNodeFactory().getNode(
									C_NodeTypes.MATERIALIZE_RESULT_SET_NODE,
									childResult,
									prRCList,
									tableProperties,
									getContextManager());
				// Propagate the referenced table map if it's already been created
				if (childResult.getReferencedTableMap() != null)
				{
					mrsn.setReferencedTableMap((JBitSet) childResult.getReferencedTableMap().clone());
				}
				childResult = mrsn;
			}
		}

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
	protected FromTable getFromTableByName(String name, String schemaName, boolean exactMatch)
		throws StandardException
	{
		return childResult.getFromTableByName(name, schemaName, exactMatch);
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
		if (restriction != null || constantRestriction != null)
		{
			return TransactionController.MODE_RECORD;
		}
		else
		{
			return childResult.updateTargetLockMode();
		}
	}

	/**
	 * Is it possible to do a distinct scan on this ResultSet tree.
	 * (See SelectNode for the criteria.)
	 *
	 * @param distinctColumns the set of distinct columns
	 * @return Whether or not it is possible to do a distinct scan on this ResultSet tree.
	 */
	boolean isPossibleDistinctScan(Set distinctColumns)
	{
		if (restriction != null || 
			(restrictionList != null && restrictionList.size() != 0))
		{
			return false;
		}

		HashSet columns = new HashSet();
		for (int i = 0; i < resultColumns.size(); i++) {
			ResultColumn rc = (ResultColumn) resultColumns.elementAt(i);
			BaseColumnNode bc = rc.getBaseColumnNode();
			if (bc == null) return false;
			columns.add(bc);
		}

		return columns.equals(distinctColumns) && childResult.isPossibleDistinctScan(distinctColumns);
	}

	/**
	 * Mark the underlying scan as a distinct scan.
	 */
	void markForDistinctScan()
	{
		childResult.markForDistinctScan();
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

		if (restriction != null)
		{
			restriction = (ValueNode)restriction.accept(v);
		}

		if (restrictionList != null)
		{
			restrictionList = (PredicateList)restrictionList.accept(v);
		}
	}



	/**
	 * set the Information gathered from the parent table that is 
	 * required to peform a referential action on dependent table.
	 */
	public void setRefActionInfo(long fkIndexConglomId, 
								 int[]fkColArray, 
								 String parentResultSetId,
								 boolean dependentScan)
	{
		childResult.setRefActionInfo(fkIndexConglomId,
								   fkColArray,
								   parentResultSetId,
								   dependentScan);
	}

	public void setRestriction(ValueNode restriction) {
		this.restriction = restriction;
	}

	/**
	 * Push the order by list down from InsertNode into its child result set so
	 * that the optimizer has all of the information that it needs to consider
	 * sort avoidance.
	 *
	 * @param orderByList	The order by list
	 */
	void pushOrderByList(OrderByList orderByList)
	{
		childResult.pushOrderByList(orderByList);
	}

    /**
     * Push down the offset and fetch first parameters, if any, to the
     * underlying child result set.
     *
     * @param offset    the OFFSET, if any
     * @param fetchFirst the OFFSET FIRST, if any
     */
    void pushOffsetFetchFirst(ValueNode offset, ValueNode fetchFirst)
    {
        childResult.pushOffsetFetchFirst(offset, fetchFirst);
    }
}
