/*

   Derby - Class org.apache.derby.impl.sql.compile.HashJoinStrategy

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

package org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.ExpressionClassBuilderInterface;
import org.apache.derby.iapi.sql.compile.JoinStrategy;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.OptimizablePredicate;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;

import org.apache.derby.iapi.store.access.StoreCostController;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;
import org.apache.derby.impl.sql.compile.ProjectRestrictNode;
import org.apache.derby.impl.sql.compile.Predicate;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.io.FormatableIntHolder;

import java.util.Vector;

public class HashJoinStrategy extends BaseJoinStrategy {
	public HashJoinStrategy() {
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
		int[] hashKeyColumns = null;

		ConglomerateDescriptor cd = null;

		/* If the innerTable is a VTI, then we
		 * must check to see if there are any
		 * join columns in the VTI's parameters.
		 * If so, then hash join is not feasible.
		 */
		if (! innerTable.isMaterializable())
		{

			optimizer.trace(Optimizer.HJ_SKIP_NOT_MATERIALIZABLE, 0, 0, 0.0,
							null);
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

		if (innerTable.isBaseTable())
		{
			/* Must have an equijoin on a column in the conglomerate */
			cd = innerTable.getCurrentAccessPath().getConglomerateDescriptor();
		}
		
		/* Look for equijoins in the predicate list */
		hashKeyColumns = findHashKeyColumns(
											innerTable,
											cd,
											predList);

		if (SanityManager.DEBUG)
		{
			if (hashKeyColumns == null)
			{
				optimizer.trace(Optimizer.HJ_SKIP_NO_JOIN_COLUMNS, 0, 0, 0.0, null);
			}
			else
			{
				optimizer.trace(Optimizer.HJ_HASH_KEY_COLUMNS, 0, 0, 0.0, hashKeyColumns);
			}
		}

		if (hashKeyColumns == null)
		{
			return false;
		}

		return true;
	}

	/** @see JoinStrategy#ignoreBulkFetch */
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
										OptimizablePredicateList predList) {
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

	/** @see JoinStrategy#memoryUsage */
	public double memoryUsage(double memoryPerRow, double rowCount) {
		return memoryPerRow * rowCount;
	}

	/** @see JoinStrategy#getName */
	public String getName() {
		return "HASH";
	}

	/** @see JoinStrategy#scanCostType */
	public int scanCostType() {
		return StoreCostController.STORECOST_SCAN_SET;
	}

	/** @see JoinStrategy#resultSetMethodName */
	public String resultSetMethodName(boolean bulkFetch) {
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
							MethodBuilder resultRowAllocator,
							int colRefItem,
							int indexColItem,
							int lockMode,
							boolean tableLocked,
							int isolationLevel
							)
						throws StandardException {
		ExpressionClassBuilder acb = (ExpressionClassBuilder) acbi;

		fillInScanArgs1(tc,
										mb,
										innerTable,
										storeRestrictionList,
										acb,
										resultRowAllocator);

		nonStoreRestrictionList.generateQualifiers(acb,	mb, innerTable, true);
		mb.push(innerTable.initialCapacity());
		mb.push(innerTable.loadFactor());
		mb.push(innerTable.maxCapacity());
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
		int[] columns = null;
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

		// Build a Vector of all the hash key columns
		int colCtr;
		Vector hashKeyVector = new Vector();
		for (colCtr = 0; colCtr < columns.length; colCtr++)
		{
			// Is there an equijoin condition on this column?
			if (predList.hasOptimizableEquijoin(innerTable, columns[colCtr]))
			{
				hashKeyVector.addElement(new Integer(colCtr));
			}
		}

		// Convert the Vector into an int[], if there are hash key columns
		if (hashKeyVector.size() > 0)
		{
			int[] keyCols = new int[hashKeyVector.size()];
			for (int index = 0; index < keyCols.length; index++)
			{
				keyCols[index] = ((Integer) hashKeyVector.elementAt(index)).intValue();
			}
			return keyCols;
		}
		else
			return (int[]) null;
	}

	public String toString() {
		return getName();
	}
}
