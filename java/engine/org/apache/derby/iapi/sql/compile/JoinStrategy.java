/*

   Derby - Class org.apache.derby.iapi.sql.compile.JoinStrategy

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.sql.compile;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.error.StandardException;

/**
 * A JoinStrategy represents a strategy like nested loop, hash join,
 * merge join, etc.  It tells the optimizer whether the strategy is
 * feasible in a given situation, how much the strategy costs, whether
 * the strategy requires the data from the source result sets to be ordered,
 * etc.
 */

public interface JoinStrategy {
	/**
	 * Is this join strategy feasible under the circumstances?
	 *
	 * @param innerTable	The inner table of the join
	 * @param predList		The predicateList for the join
	 * @param optimizer		The optimizer to use
	 *
	 * @return	true means the strategy is feasible, false means it isn't
	 *
	 * @exception StandardException		Thrown on error
	 */
	boolean feasible(Optimizable innerTable,
					 OptimizablePredicateList predList,
					 Optimizer optimizer
					 )
			throws StandardException;

	/**
	 * Is it OK to use bulk fetch with this join strategy?
	 */
	boolean bulkFetchOK();

	/**
	 * Should we just ignore bulk fetch with this join strategy?
	 */
	boolean ignoreBulkFetch();

	/**
	 * Returns true if the base cost of scanning the conglomerate should be
	 * multiplied by the number of outer rows.
	 */
	boolean multiplyBaseCostByOuterRows();

	/**
	 * Get the base predicates for this join strategy.  The base predicates
	 * are the ones that can be used while scanning the table.  For some
	 * join strategies (for example, nested loop), all predicates are base
	 * predicates.  For other join strategies (for example, hash join),
	 * the base predicates are those that involve comparisons with constant
	 * expressions.
	 *
	 * Also, order the base predicates according to the order in the
	 * proposed conglomerate descriptor for the inner table.
	 *
	 * @param predList	The predicate list to pull from.
	 * @param basePredicates	The list to put the base predicates in.
	 * @param innerTable	The inner table of the join
	 *
	 * @return	The base predicate list.  If no predicates are pulled,
	 *			it may return the source predList without doing anything.
	 *
	 * @exception StandardException		Thrown on error
	 */
	OptimizablePredicateList getBasePredicates(
								OptimizablePredicateList predList,
								OptimizablePredicateList basePredicates,
								Optimizable innerTable)
						throws StandardException;

	/**
	 * Get the extra selectivity of the non-base predicates (those that were
	 * left in the predicate list by getBasePredicates() that are not
	 * applied to the scan of the base conglomerate.
	 *
	 * NOTE: For some types of join strategy, it may not remove any predicates
	 * from the original predicate list.  The join strategy is expected to
	 * know when it does this, and to return 1.0 as the extra selectivity
	 * in these cases.
	 *
	 * @param innerTable	The inner table of the join.
	 * @param predList	The original predicate list that was passed to
	 *					getBasePredicates(), from which some base predicates
	 *					may have been pulled.
	 *
	 * @return	The extra selectivity due to non-base predicates
	 */
	double nonBasePredicateSelectivity(Optimizable innerTable,
										OptimizablePredicateList predList);

	/**
	 * Put back and base predicates that were removed from the list by
	 * getBasePredicates (see above).
	 *
	 * NOTE: Those join strategies that treat all predicates as base
	 *		 predicates may treat the get and put methods as no-ops.
	 *
	 * @param predList	The list of predicates to put the base predicates
	 *					back in.
	 * @param basePredicates	The base predicates to put back in the list.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void putBasePredicates(OptimizablePredicateList predList,
							OptimizablePredicateList basePredicates)
					throws StandardException;
	/**
	 * Get the estimated cost for the join.
	 *
	 * @param predList		The predicate list for the join
	 * @param innerTable	The inner table to join with
	 * @param cd			The conglomerate descriptor (if appropriate) to get
	 *						the cost of
	 * @param outerCost		The estimated cost of the part of the plan outer
	 *						to the inner table
	 * @param optimizer		The optimizer to use to help estimate the cost
	 * @param costEstimate	The estimated cost of doing a single scan of the
	 *						inner table, to be filled in with the cost of
	 *						doing the join.
	 *
	 * @exception StandardException		Thrown on error
	 */
	void estimateCost(Optimizable innerTable,
						OptimizablePredicateList predList,
						ConglomerateDescriptor cd,
						CostEstimate outerCost,
						Optimizer optimizer,
						CostEstimate costEstimate)
		throws StandardException;

	/**
	 * Get the estimated memory usage for this join strategy, given
	 * the number of rows and the memory usage per row.
	 */
	double memoryUsage(double memoryPerRow, double rowCount);

	/** Get the name of this join strategy */
	String getName();

	/** Get the costing type, for use with StoreCostController.getScanCost */
	int scanCostType();

	/**
	 * Get the name of the result set method for base table scans
	 *
	 * @param bulkFetch		True means bulk fetch is being done on the inner
	 *						table
	 */
	String resultSetMethodName(boolean bulkFetch);

	/**
	 * Get the name of the join result set method for the join
	 */
	String joinResultSetMethodName();

	/**
	 * Get the name of the join result set method for the half outerjoin
	 */
	String halfOuterJoinResultSetMethodName();

	/**
	 * Get the appropriate arguments to the scan for this type of join.
	 *
	 * @param tc	The TransactionController
	 * @param mb	The method to generate the arguments in
	 * @param innerTable	The inner table of the join
	 * @param storeRestrictionList	The predicate list to be evaluated in the
	 *								store
	 * @param nonStoreRestrictionList	The predicate list to be evaluated
	 *									outside of the store
	 * @param acb	The expression class builder for the activation class
	 *				we're building
	 * @param bulkFetch	The amount of bulk fetch to do
	 * @param resultRowAllocator	A completed method to allocate the result row
	 * @param colRefItem	The item number of the column reference bit map
	 * @param lockMode		The lock mode to use when scanning the table
	 *						(see TransactionController).
	 * @param tableLocked	Whether or not the table is marked (in sys.systables)
	 *						as always using table locking
	 * @param isolationLevel		Isolation level specified (or not) for scans
	 * @param closeMethodArgument	An Expression that evaluates to a method
	 *								to call when the result set is closed.
	 *
	 * @return	Count of the expressions pushed to use as the parameters to the
	 *			result set for the inner table
	 *
	 * @exception StandardException		Thrown on error
	 */
	int getScanArgs(TransactionController tc,
							MethodBuilder mb,
							Optimizable innerTable,
							OptimizablePredicateList storeRestrictionList,
							OptimizablePredicateList nonStoreRestrictionList,
							ExpressionClassBuilderInterface acb,
							int bulkFetch,
							MethodBuilder resultRowAllocator,
							int colRefItem,
							int indexColItem,
							int lockMode,
							boolean tableLocked,
							int isolationLevel
							)
					throws StandardException;

	/**
	 * Divide up the predicates into different lists for different phases
	 * of the operation. When this method is called, all of the predicates
	 * will be in restrictionList.  The effect of this method is to
	 * remove all of the predicates from restrictionList except those that
	 * will be pushed down to the store as start/stop predicates or
	 * Qualifiers.  The remaining predicates will be put into
	 * nonBaseTableRestrictionList.
	 *
	 * All predicate lists will be ordered as necessary for use with
	 * the conglomerate.
	 *
	 * Some operations (like hash join) materialize results, and so
	 * require requalification of rows when doing a non-covering index
	 * scan.  The predicates to use for requalification are copied into
	 * baseTableRestrictionList.
	 *
	 * @param innerTable	The inner table of the join
	 * @param originalRestrictionList	Initially contains all predicates.
	 *									This method removes predicates from
	 *									this list and moves them to other
	 *									lists, as appropriate.
	 * @param storeRestrictionList	To be filled in with predicates to
	 *								be pushed down to store.
	 * @param nonStoreRestrictionList	To be filled in with predicates
	 *									that are not pushed down to the
	 *									store.
	 * @param requalificationRestrictionList	Copy of predicates used to
	 *											re-qualify rows, if necessary.
	 * @param dd			The DataDictionary
	 *
	 * @exception StandardException		Thrown on error
	 */
	void divideUpPredicateLists(
						Optimizable innerTable,
						OptimizablePredicateList originalRestrictionList,
						OptimizablePredicateList storeRestrictionList,
						OptimizablePredicateList nonStoreRestrictionList,
						OptimizablePredicateList requalificationRestrictionList,
						DataDictionary			 dd)
				throws StandardException;

	/**
	 * Is this a form of hash join?
	 *
	 * @return Whether or not this strategy is a form
	 * of hash join.
	 */
	public boolean isHashJoin();

	/**
	 * Is materialization built in to the join strategy?
	 *
	 * @return Whether or not materialization is built in to the join strategy
	 */
	public boolean doesMaterialization();
}
