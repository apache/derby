/*

   Derby - Class org.apache.derby.impl.sql.compile.HashJoinStrategy

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

import java.util.ArrayList;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.cache.ClassSize;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.shared.common.util.ArrayUtil;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.io.FormatableIntHolder;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.ExpressionClassBuilderInterface;
import org.apache.derby.iapi.sql.compile.JoinStrategy;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizablePredicate;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.store.access.StoreCostController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.util.JBitSet;

class HashJoinStrategy extends BaseJoinStrategy {
    HashJoinStrategy() {
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
		ConglomerateDescriptor cd = null;

		/* If the innerTable is a VTI, then we
		 * must check to see if there are any
		 * join columns in the VTI's parameters.
		 * If so, then hash join is not feasible.
		 */
		if (! innerTable.isMaterializable())
		{
            if ( innerTable.optimizerTracingIsOn() ) { innerTable.getOptimizerTracer().traceSkipUnmaterializableHashJoin(); }
			return false;
		}

		/* Don't consider hash join on the target table of an update/delete.
		 * RESOLVE - this is a temporary restriction.  Problem is that we
		 * do not put RIDs into the row in the hash table when scanning
		 * the heap and we need them for a target table.
		 */
		if (innerTable.isTargetTable())
		{
			return false;
		}

		/* If the predicate given by the user _directly_ references
		 * any of the base tables _beneath_ this node, then we
		 * cannot safely use the predicate for a hash because the
		 * predicate correlates two nodes at different nesting levels. 
		 * If we did a hash join in this case, materialization of
		 * innerTable could lead to incorrect results--and in particular,
		 * results that are missing rows.  We can check for this by
		 * looking at the predicates' reference maps, which are set based
		 * on the initial query (as part of pre-processing).  Note that
		 * by the time we get here, it's possible that a predicate's
		 * reference map holds table numbers that do not agree with the
		 * table numbers of the column references used by the predicate.
		 * That's okay--this occurs as a result of "remapping" predicates
		 * that have been pushed down the query tree.  And in fact
		 * it's a good thing because, by looking at the column reference's
		 * own table numbers instead of the predicate's referenced map,
		 * we are more readily able to find equijoin predicates that
		 * we otherwise would not have found.
		 *
		 * Note: do not perform this check if innerTable is a FromBaseTable
		 * because a base table does not have a "subtree" to speak of.
		 */
		if ((predList != null) && (predList.size() > 0) &&
			!(innerTable instanceof FromBaseTable))
		{
			FromTable ft = (FromTable)innerTable;

			// First get a list of all of the base tables in the subtree
			// below innerTable.
			JBitSet tNums = new JBitSet(ft.getReferencedTableMap().size());
			BaseTableNumbersVisitor btnVis = new BaseTableNumbersVisitor(tNums);
			ft.accept(btnVis);

			// Now get a list of all table numbers referenced by the
			// join predicates that we'll be searching.
			JBitSet pNums = new JBitSet(tNums.size());

            for (int i = 0; i < predList.size(); i++)
			{
                Predicate pred = (Predicate)predList.getOptPredicate(i);
				if (pred.isJoinPredicate())
					pNums.or(pred.getReferencedSet());
			}

			// If tNums and pNums have anything in common, then at
			// least one predicate in the list refers directly to
			// a base table beneath this node (as opposed to referring
			// just to this node), which means it's not safe to do a
			// hash join.
			tNums.and(pNums);
			if (tNums.getFirstSetBit() != -1)
				return false;
		}

		if (innerTable.isBaseTable())
		{
			/* Must have an equijoin on a column in the conglomerate */
			cd = innerTable.getCurrentAccessPath().getConglomerateDescriptor();
		}
		
		/* Look for equijoins in the predicate list */
        int[] hashKeyColumns = findHashKeyColumns(
                innerTable,
                cd,
                predList);

		if (SanityManager.DEBUG)
		{
            if ( innerTable.optimizerTracingIsOn() )
            {
                if (hashKeyColumns == null)
                {
                    innerTable.getOptimizerTracer().traceSkipHashJoinNoHashKeys();
                }
                else
                {
                    innerTable.getOptimizerTracer().traceHashKeyColumns( ArrayUtil.copy( hashKeyColumns ) );
                }
            }
		}

		if (hashKeyColumns == null)
		{
			return false;
		}

		return true;
	}

	/** @see JoinStrategy#ignoreBulkFetch */
    @Override
	public boolean ignoreBulkFetch() {
		return true;
	}

	/** @see JoinStrategy#multiplyBaseCostByOuterRows */
	public boolean multiplyBaseCostByOuterRows() {
		return false;
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
			SanityManager.ASSERT(basePredicates.size() == 0,
				"The base predicate list should be empty.");
		}

		for (int i = predList.size() - 1; i >= 0; i--) {
			OptimizablePredicate pred = predList.getOptPredicate(i);

			if (innerTable.getReferencedTableMap().contains(pred.getReferencedMap()))
			{
				basePredicates.addOptPredicate(pred);
				predList.removeOptPredicate(i);
			}
		}

		basePredicates.classify(
				innerTable,
				innerTable.getCurrentAccessPath().getConglomerateDescriptor());

		return basePredicates;
	}

	/** @see JoinStrategy#nonBasePredicateSelectivity */
	public double nonBasePredicateSelectivity(
										Optimizable innerTable,
										OptimizablePredicateList predList) 
	throws StandardException {
		double retval = 1.0;

		if (predList != null) {
			for (int i = 0; i < predList.size(); i++) {
				// Don't include redundant join predicates in selectivity calculations
				if (predList.isRedundantPredicate(i))
				{
					continue;
				}

				retval *= predList.getOptPredicate(i).selectivity(innerTable);
			}
		}

		return retval;
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

	/** @see JoinStrategy#estimateCost */
	public void estimateCost(Optimizable innerTable,
							 OptimizablePredicateList predList,
							 ConglomerateDescriptor cd,
							 CostEstimate outerCost,
							 Optimizer optimizer,
							 CostEstimate costEstimate) {
		/*
		** The cost of a hash join is the cost of building the hash table.
		** There is no extra cost per outer row, so don't do anything here.
		*/
	}

	/** @see JoinStrategy#maxCapacity */
	public int maxCapacity( int userSpecifiedCapacity,
                            int maxMemoryPerTable,
                            double perRowUsage) {
        if( userSpecifiedCapacity >= 0)
            return userSpecifiedCapacity;
        perRowUsage += ClassSize.estimateHashEntrySize();
        if( perRowUsage <= 1)
            return maxMemoryPerTable;
        return (int)(maxMemoryPerTable/perRowUsage);
	}

	/** @see JoinStrategy#getName */
	public String getName() {
		return "HASH";
	}

	/** @see JoinStrategy#scanCostType */
	public int scanCostType() {
		return StoreCostController.STORECOST_SCAN_SET;
	}

	/** @see JoinStrategy#getOperatorSymbol */
    public  String  getOperatorSymbol() { return "#"; }


	/** @see JoinStrategy#resultSetMethodName */
    public String resultSetMethodName(
            boolean bulkFetch,
            boolean multiprobe,
            boolean validatingCheckConstraint) {
		return "getHashScanResultSet";
	}

	/** @see JoinStrategy#joinResultSetMethodName */
	public String joinResultSetMethodName() {
		return "getHashJoinResultSet";
	}

	/** @see JoinStrategy#halfOuterJoinResultSetMethodName */
	public String halfOuterJoinResultSetMethodName() {
		return "getHashLeftOuterJoinResultSet";
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
							int resultRowTemplate,
							int colRefItem,
							int indexColItem,
							int lockMode,
							boolean tableLocked,
							int isolationLevel,
							int maxMemoryPerTable,
							boolean genInListVals
							)
						throws StandardException
	{
		/* We do not currently support IN-list "multi-probing" for hash scans
		 * (though we could do so in the future).  So if we're doing a hash
		 * join then we shouldn't have any IN-list probe predicates in the
		 * store restriction list at this point.  The reason is that, in the
		 * absence of proper multi-probing logic, such predicates would act
		 * as restrictions on the rows read from disk.  That would be wrong
		 * because a probe predicate is of the form "col = <val>" where <val>
		 * is the first value in the IN-list.  Enforcement of that restriction
		 * would lead to incorrect results--we need to return all rows having
		 * any value that appears in the IN-list, not just those rows matching
		 * the first value.  Checks elsewhere in the code should ensure that
		 * no probe predicates have made it this far, but if we're running in
		 * SANE mode it doesn't hurt to verify.
		 */
		if (SanityManager.DEBUG)
		{
			for (int i = storeRestrictionList.size() - 1; i >= 0; i--)
			{
                Predicate pred =
                        (Predicate)storeRestrictionList.getOptPredicate(i);
				if (pred.isInListProbePredicate())
				{
					SanityManager.THROWASSERT("Found IN-list probing " +
						"(" + pred.binaryRelOpColRefsToString() +
						") while generating HASH join, which should " +
						"not happen.");
				}
			}
		}

		ExpressionClassBuilder acb = (ExpressionClassBuilder) acbi;

		fillInScanArgs1(tc,
										mb,
										innerTable,
										storeRestrictionList,
										acb,
										resultRowTemplate);

		nonStoreRestrictionList.generateQualifiers(acb,	mb, innerTable, true);
		mb.push(innerTable.initialCapacity());
		mb.push(innerTable.loadFactor());
		mb.push(innerTable.maxCapacity( (JoinStrategy) this, maxMemoryPerTable));
		/* Get the hash key columns and wrap them in a formattable */
		int[] hashKeyColumns = innerTable.hashKeyColumns();
		FormatableIntHolder[] fihArray = 
				FormatableIntHolder.getFormatableIntHolders(hashKeyColumns); 
		FormatableArrayHolder hashKeyHolder = new FormatableArrayHolder(fihArray);
		int hashKeyItem = acb.addItem(hashKeyHolder);
		mb.push(hashKeyItem);

		fillInScanArgs2(mb,
						innerTable,
						bulkFetch,
						colRefItem,
						indexColItem,
						lockMode,
						tableLocked,
						isolationLevel);

		return 28;
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
		** If we are walking a non-covering index, then all predicates that
		** get evaluated in the HashScanResultSet, whether during the building
		** or probing of the hash table, need to be evaluated at both the
		** IndexRowToBaseRowResultSet and the HashScanResultSet to ensure
		** that the rows materialized into the hash table still qualify when
		** we go to read the row from the heap.  This also includes predicates
        ** that are not qualifier/start/stop keys (hence not in store/non-store
        ** list).
		*/
		originalRestrictionList.copyPredicatesToOtherList(
            requalificationRestrictionList);

		ConglomerateDescriptor cd =
			innerTable.getTrulyTheBestAccessPath().getConglomerateDescriptor();

		/* For the inner table of a hash join, then divide up the predicates:
         *
		 *	o restrictionList	- predicates that get applied when creating 
		 *						  the hash table (single table clauses)
         *
		 *  o nonBaseTableRestrictionList
		 *						- those that get applied when probing into the 
		 *						  hash table (equijoin clauses on key columns,
		 *						  ordered by key column position first, followed
		 *						  by any other join predicates. (All predicates
         *						  in this list are qualifiers which can be 
         *						  evaluated in the store).
         *
		 *  o baseTableRL		- Only applicable if this is not a covering 
         *                        index.  In that case, we will need to 
         *                        requalify the data page.  Thus, this list 
         *                        will include all predicates.
		 */

		// Build the list to be applied when creating the hash table
		originalRestrictionList.transferPredicates(
									storeRestrictionList,
									innerTable.getReferencedTableMap(),
									innerTable);

		/* 
         * Eliminate any non-qualifiers that may have been pushed, but
         * are redundant and not useful for hash join.
         * 
         * For instance "in" (or other non-qualifier) was pushed down for 
         * start/stop key, * but for hash join, it may no longer be because 
         * previous key column may have been disqualified (eg., correlation).  
         * We simply remove 
         * such non-qualifier ("in") because we left it as residual predicate 
         * anyway.  It's easier/safer to filter it out here than detect it 
         * ealier (and not push it down). Beetle 4316.
         *
         * Can't filter out OR list, as it is not a residual predicate, 
		 */
		for (int i = storeRestrictionList.size() - 1; i >= 0; i--)
		{
			Predicate p1 = (Predicate) storeRestrictionList.getOptPredicate(i);

           
            if (!p1.isStoreQualifier() && !p1.isStartKey() && !p1.isStopKey())
            {
				storeRestrictionList.removeOptPredicate(i);
            }
		}

		for (int i = originalRestrictionList.size() - 1; i >= 0; i--)
		{
			Predicate p1 = 
                (Predicate) originalRestrictionList.getOptPredicate(i);

            if (!p1.isStoreQualifier())
				originalRestrictionList.removeOptPredicate(i);
		}

		/* Copy the rest of the predicates to the non-store list */
		originalRestrictionList.copyPredicatesToOtherList(
													nonStoreRestrictionList);

		/* If innerTable is ProjectRestrictNode, we need to use its child
		 * to find hash key columns, this is because ProjectRestrictNode may
		 * not have underlying node's every result column as result column,
		 * and the predicate's column reference was bound to the underlying
		 * node's column position.  Also we have to pass in the 
	 	 * ProjectRestrictNode rather than the underlying node to this method
		 * because a predicate's referencedTableMap references the table number
		 * of the ProjectRestrictiveNode.  And we need this info to see if
		 * a predicate is in storeRestrictionList that can be pushed down.
		 * Beetle 3458.
		 */
		Optimizable hashTableFor = innerTable;
		if (innerTable instanceof ProjectRestrictNode)
		{
			ProjectRestrictNode prn = (ProjectRestrictNode) innerTable;
			if (prn.getChildResult() instanceof Optimizable)
				hashTableFor = (Optimizable) (prn.getChildResult());
		}
		int[] hashKeyColumns = findHashKeyColumns(hashTableFor,
												cd,
												nonStoreRestrictionList);
		if (hashKeyColumns != null)
		{
			innerTable.setHashKeyColumns(hashKeyColumns);
		}
		else
		{
			String name;
			if (cd != null && cd.isIndex())
			{
				name = cd.getConglomerateName();
			}
			else
			{
				name = innerTable.getBaseTableName();
			}

			throw StandardException.newException(SQLState.LANG_HASH_NO_EQUIJOIN_FOUND, 
						name,
						innerTable.getBaseTableName());
		}

		// Mark all of the predicates in the probe list as qualifiers
		nonStoreRestrictionList.markAllPredicatesQualifiers();

		int[] conglomColumn = new int[hashKeyColumns.length];
		if (cd != null && cd.isIndex())
		{
			/*
			** If the conglomerate is an index, get the column numbers of the
			** hash keys in the base heap.
			*/
			for (int index = 0; index < hashKeyColumns.length; index++)
			{
				conglomColumn[index] =
				  cd.getIndexDescriptor().baseColumnPositions()[hashKeyColumns[index]];
			}
		}
		else
		{
			/*
			** If the conglomerate is a heap, the column numbers of the hash
			** key are the column numbers returned by findHashKeyColumns().
			**
			** NOTE: Must switch from zero-based to one-based
			*/
			for (int index = 0; index < hashKeyColumns.length; index++)
			{
				conglomColumn[index] = hashKeyColumns[index] + 1;
			}
		}

		/* Put the equality predicates on the key columns for the hash first.
		 * (Column # is columns[colCtr] from above.)
		 */
		for (int index = hashKeyColumns.length - 1; index >= 0; index--)
		{
			nonStoreRestrictionList.putOptimizableEqualityPredicateFirst(
					innerTable,
					conglomColumn[index]);
		}
	}

	/**
	 * @see JoinStrategy#isHashJoin
	 */
    @Override
	public boolean isHashJoin()
	{
		return true;
	}

	/**
	 * @see JoinStrategy#doesMaterialization
	 */
	public boolean doesMaterialization()
	{
		return true;
	}

	/**
	 * Find the hash key columns, if any, to use with this join.
	 *
	 * @param innerTable	The inner table of the join
	 * @param cd			The conglomerate descriptor to use on inner table
	 * @param predList		The predicate list to look for the equijoin in
	 *
	 * @return	the numbers of the hash key columns, or null if no hash key column
	 *
	 * @exception StandardException		Thrown on error
	 */
	private int[] findHashKeyColumns(Optimizable innerTable,
									ConglomerateDescriptor cd,
									OptimizablePredicateList predList)
				throws StandardException
	{
		if (predList == null)
			return (int[]) null;

		/* Find the column to use as the hash key.
		 * (There must be an equijoin condition on this column.)
		 * If cd is null, then Optimizable is not a scan.
		 * For indexes, we start at the first column in the key
		 * and walk the key columns until we find the first one with
		 * an equijoin condition on it.  We do essentially the same
		 * for heaps.  (From column 1 through column n.)
		 */
        int[] columns;
		if (cd == null)
		{
			columns = new int[innerTable.getNumColumnsReturned()];
			for (int j = 0; j < columns.length; j++)
			{
				columns[j] = j + 1;
			}
		}
		else if (cd.isIndex())
		{
			columns = cd.getIndexDescriptor().baseColumnPositions();
		}
		else
		{
			columns =
				new int[innerTable.getTableDescriptor().getNumberOfColumns()];
			for (int j = 0; j < columns.length; j++)
			{
				columns[j] = j + 1;
			}
		}

        // Build a list of all the hash key columns
        ArrayList<Integer> hashKeys = new ArrayList<Integer>();
        for (int colCtr = 0; colCtr < columns.length; colCtr++)
		{
			// Is there an equijoin condition on this column?
			if (predList.hasOptimizableEquijoin(innerTable, columns[colCtr]))
			{
                hashKeys.add(colCtr);
			}
		}

        // Convert the list into an int[], if there are hash key columns
        if (hashKeys.isEmpty())
        {
            return null;
        }

        int[] keyCols = new int[hashKeys.size()];
        for (int index = 0; index < keyCols.length; index++)
        {
            keyCols[index] = hashKeys.get(index).intValue();
        }
        return keyCols;
	}

    @Override
	public String toString() {
		return getName();
	}
}
