/*

   Derby - Class org.apache.derby.impl.sql.compile.NestedLoopJoinStrategy

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

package org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.ExpressionClassBuilderInterface;
import org.apache.derby.iapi.sql.compile.JoinStrategy;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.OptimizablePredicate;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;

import org.apache.derby.iapi.store.access.StoreCostController;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;

public class NestedLoopJoinStrategy extends BaseJoinStrategy {
	public NestedLoopJoinStrategy() {
	}


	/**
	 * @see JoinStrategy#feasible
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean feasible(Optimizable innerTable,
							OptimizablePredicateList predList,
							Optimizer optimizer
							)
					throws StandardException 
	{
		/* Nested loop is feasible, except in the corner case
		 * where innerTable is a VTI that cannot be materialized
		 * (because it has a join column as a parameter) and
		 * it cannot be instantiated multiple times.
		 * RESOLVE - Actually, the above would work if all of 
		 * the tables outer to innerTable were 1 row tables, but
		 * we don't have that info yet, and it should probably
		 * be hidden in inner table somewhere.
		 * NOTE: A derived table that is correlated with an outer
		 * query block is not materializable, but it can be
		 * "instantiated" multiple times because that only has
		 * meaning for VTIs.
		 */
		if (innerTable.isMaterializable())
		{
			return true;
		}
		if (innerTable.supportsMultipleInstantiations())
		{
			return true;
		}
		return false;
	}

	/** @see JoinStrategy#multiplyBaseCostByOuterRows */
	public boolean multiplyBaseCostByOuterRows() {
		return true;
	}

	/**
	 * @see JoinStrategy#getBasePredicates
	 *
	 * @exception StandardException		Thrown on error
	 */
	public OptimizablePredicateList getBasePredicates(
									OptimizablePredicateList predList,
									OptimizablePredicateList basePredicates,
									Optimizable innerTable)
							throws StandardException {
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(basePredicates == null ||
								 basePredicates.size() == 0,
				"The base predicate list should be empty.");
		}

		if (predList != null) {
			predList.transferAllPredicates(basePredicates);
			basePredicates.classify(innerTable,
				innerTable.getCurrentAccessPath().getConglomerateDescriptor());
		}

		return basePredicates;
	}

	/** @see JoinStrategy#nonBasePredicateSelectivity */
	public double nonBasePredicateSelectivity(
										Optimizable innerTable,
										OptimizablePredicateList predList) {
		/*
		** For nested loop, all predicates are base predicates, so there
		** is no extra selectivity.
		*/
		return 1.0;
	}
	
	/**
	 * @see JoinStrategy#putBasePredicates
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void putBasePredicates(OptimizablePredicateList predList,
									OptimizablePredicateList basePredicates)
					throws StandardException {
		for (int i = basePredicates.size() - 1; i >= 0; i--) {
			OptimizablePredicate pred = basePredicates.getOptPredicate(i);

			predList.addOptPredicate(pred);
			basePredicates.removeOptPredicate(i);
		}
	}

	/* @see JoinStrategy#estimateCost */
	public void estimateCost(Optimizable innerTable,
							 OptimizablePredicateList predList,
							 ConglomerateDescriptor cd,
							 CostEstimate outerCost,
							 Optimizer optimizer,
							 CostEstimate costEstimate) {
		costEstimate.multiply(outerCost.rowCount(), costEstimate);

		optimizer.trace(Optimizer.COST_OF_N_SCANS, innerTable.getTableNumber(), 0, outerCost.rowCount(),
						costEstimate);
	}

	/** @see JoinStrategy#maxCapacity */
	public int maxCapacity( int userSpecifiedCapacity,
                            int maxMemoryPerTable,
                            double perRowUsage) {
		return Integer.MAX_VALUE;
	}

	/** @see JoinStrategy#getName */
	public String getName() {
		return "NESTEDLOOP";
	}

	/** @see JoinStrategy#scanCostType */
	public int scanCostType() {
		return StoreCostController.STORECOST_SCAN_NORMAL;
	}

	/** @see JoinStrategy#resultSetMethodName */
	public String resultSetMethodName(boolean bulkFetch, boolean multiprobe) {
		if (bulkFetch)
			return "getBulkTableScanResultSet";
		else if (multiprobe)
			return "getMultiProbeTableScanResultSet";
		else
			return "getTableScanResultSet";
	}

	/** @see JoinStrategy#joinResultSetMethodName */
	public String joinResultSetMethodName() {
		return "getNestedLoopJoinResultSet";
	}

	/** @see JoinStrategy#halfOuterJoinResultSetMethodName */
	public String halfOuterJoinResultSetMethodName() {
		return "getNestedLoopLeftOuterJoinResultSet";
	}

	/**
	 * @see JoinStrategy#getScanArgs
	 *
	 * @exception StandardException		Thrown on error
	 */
	public int getScanArgs(
							TransactionController tc,
							MethodBuilder mb,
							Optimizable innerTable,
							OptimizablePredicateList storeRestrictionList,
							OptimizablePredicateList nonStoreRestrictionList,
							ExpressionClassBuilderInterface acbi,
							int bulkFetch,
							MethodBuilder resultRowAllocator,
							int colRefItem,
							int indexColItem,
							int lockMode,
							boolean tableLocked,
							int isolationLevel,
							int maxMemoryPerTable,
							boolean genInListVals
							)
						throws StandardException {
		ExpressionClassBuilder acb = (ExpressionClassBuilder) acbi;
		int numArgs;

		if (SanityManager.DEBUG) {
			if (nonStoreRestrictionList.size() != 0) {
				SanityManager.THROWASSERT(
					"nonStoreRestrictionList should be empty for " +
					"nested loop join strategy, but it contains " +
					nonStoreRestrictionList.size() +
					" elements");
			}
		}

		/* If we're going to generate a list of IN-values for index probing
		 * at execution time then we push TableScanResultSet arguments plus
		 * two additional arguments: 1) the list of IN-list values, and 2)
		 * a boolean indicating whether or not the IN-list values are already
		 * sorted.
		 */
		if (genInListVals)
		{
			numArgs = 26;
		}
		else if (bulkFetch > 1)
		{
			numArgs = 25;
		}
		else
		{
			numArgs = 24 ;
		}

		fillInScanArgs1(tc, mb,
										innerTable,
										storeRestrictionList,
										acb,
										resultRowAllocator);

		if (genInListVals)
			((PredicateList)storeRestrictionList).generateInListValues(acb, mb);

		if (SanityManager.DEBUG)
		{
			/* If we're not generating IN-list values with which to probe
			 * the table then storeRestrictionList should not have any
			 * IN-list probing predicates.  Make sure that's the case.
			 */
			if (!genInListVals)
			{
				Predicate pred = null;
				for (int i = storeRestrictionList.size() - 1; i >= 0; i--)
				{
					pred = (Predicate)storeRestrictionList.getOptPredicate(i);
					if (pred.isInListProbePredicate())
					{
						SanityManager.THROWASSERT("Found IN-list probing " +
							"predicate (" + pred.binaryRelOpColRefsToString() +
							") when no such predicates were expected.");
					}
				}
			}
		}

		fillInScanArgs2(mb,
						innerTable,
						bulkFetch,
						colRefItem,
						indexColItem,
						lockMode,
						tableLocked,
						isolationLevel);

		return numArgs;
	}

	/**
	 * @see JoinStrategy#divideUpPredicateLists
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void divideUpPredicateLists(
					Optimizable				 innerTable,
					OptimizablePredicateList originalRestrictionList,
					OptimizablePredicateList storeRestrictionList,
					OptimizablePredicateList nonStoreRestrictionList,
					OptimizablePredicateList requalificationRestrictionList,
					DataDictionary			 dd
					) throws StandardException
	{
		/*
		** All predicates are store predicates.  No requalification is
		** necessary for non-covering index scans.
		*/
		originalRestrictionList.setPredicatesAndProperties(storeRestrictionList);
	}

	/**
	 * @see JoinStrategy#doesMaterialization
	 */
	public boolean doesMaterialization()
	{
		return false;
	}

	public String toString() {
		return getName();
	}

	/**
	 * Can this join strategy be used on the
	 * outermost table of a join.
	 *
	 * @return Whether or not this join strategy
	 * can be used on the outermose table of a join.
	 */
	protected boolean validForOutermostTable()
	{
		return true;
	}
}
