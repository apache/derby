/*

   Derby - Class org.apache.derby.impl.sql.compile.Level2OptimizerImpl

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.compile.JoinStrategy;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizableList;
import org.apache.derby.iapi.sql.compile.OptimizablePredicate;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.RequiredRowOrdering;
import org.apache.derby.iapi.sql.compile.RowOrdering;
import org.apache.derby.iapi.sql.compile.AccessPath;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.util.JBitSet;

import org.apache.derby.impl.sql.compile.OptimizerImpl;
import org.apache.derby.impl.sql.compile.CostEstimateImpl;

import java.util.Properties;

/**
 * This is the Level 2 Optimizer.
 */

public class Level2OptimizerImpl extends OptimizerImpl
{
	private LanguageConnectionContext lcc;

	Level2OptimizerImpl(OptimizableList optimizableList, 
				  OptimizablePredicateList predicateList,
				  DataDictionary dDictionary,
				  boolean ruleBasedOptimization,
				  boolean noTimeout,
				  boolean useStatistics,
				  int maxMemoryPerTable,
				  JoinStrategy[] joinStrategies,
				  int tableLockThreshold,
				  RequiredRowOrdering requiredRowOrdering,
				  int numTablesInQuery,
				  LanguageConnectionContext lcc)
		throws StandardException
	{
		super(optimizableList, predicateList, dDictionary,
			  ruleBasedOptimization, noTimeout, useStatistics, maxMemoryPerTable,
			  joinStrategies, tableLockThreshold, requiredRowOrdering,
			  numTablesInQuery);

		// Remember whether or not optimizer trace is on;
		optimizerTrace = lcc.getOptimizerTrace();
		optimizerTraceHtml = lcc.getOptimizerTraceHtml();
		this.lcc = lcc;

		// Optimization started
		if (optimizerTrace)
		{
			trace(STARTED, 0, 0, 0.0, null);
		}
	}

	/** @see Optimizer#getLevel */
	public int getLevel()
	{
		return 2;
	}

	/** @see Optimizer#newCostEstimate */
	public CostEstimate newCostEstimate()
	{
		return new Level2CostEstimateImpl();
	}

	public CostEstimateImpl getNewCostEstimate(double theCost,
							double theRowCount,
							double theSingleScanRowCount)
	{
		return new Level2CostEstimateImpl(theCost, theRowCount, theSingleScanRowCount);
	}

	// Optimzer trace
	public void trace(int traceFlag, int intParam1, int intParam2,
					  double doubleParam, Object objectParam1)
	{
		ConglomerateDescriptor cd;
		String cdString;
		String traceString = null;

		// We can get called from outside optimizer when tracing is off
		if (!optimizerTrace)
		{
			return;
		}

		switch (traceFlag)
		{
			case STARTED:
				traceString = 
					"Optimization started at time " +
					timeOptimizationStarted +
					" using optimizer " + this.hashCode();
				break;

			case TIME_EXCEEDED:
				traceString =
					"Optimization time exceeded at time " +
							currentTime + "\n" + bestCost();
				break;

			case NO_TABLES:
				traceString = "No tables to optimize.";
				break;

			case COMPLETE_JOIN_ORDER:
				traceString = "We have a complete join order.";
				break;

			case COST_OF_SORTING:
				traceString = 
					"Cost of sorting is " + sortCost;
				break;

			case NO_BEST_PLAN:
				traceString =
					"No best plan found.";
				break;

			case MODIFYING_ACCESS_PATHS:
				traceString = 
					"Modifying access paths using optimizer " + this.hashCode();
				break;

			case SHORT_CIRCUITING:
				String basis = (timeExceeded) ? "time exceeded" : "cost";
				Optimizable thisOpt =
					optimizableList.getOptimizable(
										proposedJoinOrder[joinPosition]);
				if (thisOpt.getBestAccessPath().getCostEstimate() == null)
					basis = "no best plan found";
				traceString = 
					"Short circuiting based on " + basis +
					" at join position " + joinPosition;
				break;

			case SKIPPING_JOIN_ORDER:
				traceString = buildJoinOrder("\n\nSkipping join order: ", true, intParam1,
											 proposedJoinOrder);
				break;

			case ILLEGAL_USER_JOIN_ORDER:
				traceString =
					"User specified join order is not legal.";
				break;

			case USER_JOIN_ORDER_OPTIMIZED:
				traceString =
					"User-specified join order has now been optimized.";
				break;

			case CONSIDERING_JOIN_ORDER:
				traceString = buildJoinOrder("\n\nConsidering join order: ", false, intParam1,
											 proposedJoinOrder);
				break;

			case TOTAL_COST_NON_SA_PLAN:
				traceString =
					"Total cost of non-sort-avoidance plan is " +
						currentCost;
				break;

			case TOTAL_COST_SA_PLAN:
				traceString =
					"Total cost of sort avoidance plan is " +
						currentSortAvoidanceCost;
				break;

			case TOTAL_COST_WITH_SORTING:
				traceString = 
					"Total cost of non-sort-avoidance plan with sort cost added is " + currentCost;
				break;

			case CURRENT_PLAN_IS_SA_PLAN:
				traceString = 
					"Current plan is a sort avoidance plan." + 
					"\n\tBest cost is : " + bestCost +
					"\n\tThis cost is : " + currentSortAvoidanceCost;
				break;

			case CHEAPEST_PLAN_SO_FAR:
				traceString = 
					"This is the cheapest plan so far.";
				break;

			case PLAN_TYPE:
				traceString = 
					"Plan is a " +
					(intParam1 == Optimizer.NORMAL_PLAN ?
						"normal" : "sort avoidance") +
					" plan.";
				break;

			case COST_OF_CHEAPEST_PLAN_SO_FAR:
				traceString = 
					"Cost of cheapest plan is " + currentCost;
				break;

			case SORT_NEEDED_FOR_ORDERING:
				traceString = 
					"Sort needed for ordering: " + 
						(intParam1 != Optimizer.SORT_AVOIDANCE_PLAN) +
						"\n\tRow ordering: " +
						requiredRowOrdering;
				break;

			case REMEMBERING_BEST_JOIN_ORDER:
				traceString = buildJoinOrder("\n\nRemembering join order as best: ", false, intParam1,
											 bestJoinOrder);
				break;

			case SKIPPING_DUE_TO_EXCESS_MEMORY:
				traceString = 
					"Skipping access path due to excess memory usage of " +
					doubleParam +
					" bytes - maximum is " +
					maxMemoryPerTable;
				break;

			case COST_OF_N_SCANS:
				traceString =
						"Cost of " + doubleParam +
						" scans is: " +
						objectParam1 +
						" for table " +
						intParam1;
				break;

			case HJ_SKIP_NOT_MATERIALIZABLE:
				traceString = "Skipping HASH JOIN because optimizable is not materializable";
				break;

			case HJ_SKIP_NO_JOIN_COLUMNS:
				traceString = "Skipping HASH JOIN because there are no hash key columns";
				break;

			case HJ_HASH_KEY_COLUMNS:
				int[] hashKeyColumns = (int []) objectParam1;
				traceString = "# hash key columns = " + hashKeyColumns.length;
				for (int index = 0; index < hashKeyColumns.length; index++)
				{
					traceString = 
						"\n" + traceString + "hashKeyColumns[" + index +
						"] = " + hashKeyColumns[index];
				}
				break;

			case CALLING_ON_JOIN_NODE:
				traceString = "Calling optimizeIt() for join node";
				break;

			case CONSIDERING_JOIN_STRATEGY:
				JoinStrategy js = (JoinStrategy) objectParam1;
				traceString = 
					"\nConsidering join strategy " +
					js + " for table " + intParam1;
				break;

			case REMEMBERING_BEST_ACCESS_PATH:
				traceString = 
					"Remembering access path " +
					objectParam1 +
					" as truly the best for table " +
					intParam1 + 
					" for plan type " +
					(intParam2 == Optimizer.NORMAL_PLAN ?
										" normal " : "sort avoidance") +
					"\n";
				break;

			case NO_MORE_CONGLOMERATES:
				traceString =
					"No more conglomerates to consider for table " +
					intParam1;
				break;

			case CONSIDERING_CONGLOMERATE:
				cd = (ConglomerateDescriptor) objectParam1;
				cdString = dumpConglomerateDescriptor(cd);
				traceString =
					"\nConsidering conglomerate " +
					cdString +
					" for table " +
					intParam1;
				break;

			case SCANNING_HEAP_FULL_MATCH_ON_UNIQUE_KEY:
				traceString = "Scanning heap, but we have a full match on a unique key.";
				break;

			case ADDING_UNORDERED_OPTIMIZABLE:
				traceString = "Adding unordered optimizable, # of predicates = " + intParam1;
				break;

			case CHANGING_ACCESS_PATH_FOR_TABLE:
				traceString = "Changing access path for table " + intParam1;
				break;

			case TABLE_LOCK_NO_START_STOP:
				traceString = "Lock mode set to MODE_TABLE because no start or stop position";
				break;

			case NON_COVERING_INDEX_COST:
				traceString = 
					"Index does not cover query - cost including base row fetch is: " +
					doubleParam +
					" for table " + intParam1;
				break;

			case ROW_LOCK_ALL_CONSTANT_START_STOP:
				traceString = 
					"Lock mode set to MODE_RECORD because all start and stop positions are constant";
				break;

			case ESTIMATING_COST_OF_CONGLOMERATE:
				cd = (ConglomerateDescriptor) objectParam1;
				cdString = dumpConglomerateDescriptor(cd);
				traceString =
					"Estimating cost of conglomerate: " +
					costForTable(cdString, intParam1);
				break;
				
			case LOOKING_FOR_SPECIFIED_INDEX:
				traceString = 
					"Looking for user-specified index: " +
					objectParam1 + " for table " +
					intParam1;
				break;

			case MATCH_SINGLE_ROW_COST:
				traceString =
					"Guaranteed to match a single row - cost is: " +
					doubleParam + " for table " + intParam1;
				break;

			case COST_INCLUDING_EXTRA_1ST_COL_SELECTIVITY:
				traceString = costIncluding(
								"1st column", objectParam1, intParam1);
				traceString =
					"Cost including extra first column selectivity is : " +
					objectParam1 + " for table " + intParam1;
				break;

			case CALLING_NEXT_ACCESS_PATH:
				traceString =
					"Calling nextAccessPath() for base table " +
					objectParam1 + " with " + intParam1 + " predicates.";
				break;

			case TABLE_LOCK_OVER_THRESHOLD:
				traceString = lockModeThreshold("MODE_TABLE", "greater",
												doubleParam, intParam1);
				break;

			case ROW_LOCK_UNDER_THRESHOLD:
				traceString = lockModeThreshold("MODE_RECORD", "less",
												doubleParam, intParam1);
				break;

			case COST_INCLUDING_EXTRA_START_STOP:
				traceString = costIncluding(
								"start/stop", objectParam1, intParam1);
				break;

			case COST_INCLUDING_EXTRA_QUALIFIER_SELECTIVITY:
				traceString = costIncluding(
								"qualifier", objectParam1, intParam1);
				break;

			case COST_INCLUDING_EXTRA_NONQUALIFIER_SELECTIVITY:
				traceString = costIncluding(
								"non-qualifier", objectParam1, intParam1);
				break;

   		    case COST_INCLUDING_COMPOSITE_SEL_FROM_STATS:
				traceString = costIncluding("selectivity from statistics",
											objectParam1, intParam1);
				break;

			case COST_INCLUDING_STATS_FOR_INDEX:
				traceString = costIncluding("statistics for index being considered", 
											objectParam1, intParam1);
				break;
		    case COMPOSITE_SEL_FROM_STATS:
				traceString = "Selectivity from statistics found. It is " +
					doubleParam;
				break;

			case COST_OF_NONCOVERING_INDEX:
				traceString =
					"Index does not cover query: cost including row fetch is: " +
					costForTable(objectParam1, intParam1);
				break;

			case REMEMBERING_JOIN_STRATEGY:
				traceString =
					"\nRemembering join strategy " + objectParam1 +
					" as best for table " + intParam1;
				break;

			case REMEMBERING_BEST_ACCESS_PATH_SUBSTRING:
				traceString =
					"in best access path";
				break;

			case REMEMBERING_BEST_SORT_AVOIDANCE_ACCESS_PATH_SUBSTRING:
				traceString =
					"in best sort avoidance access path";
				break;

			case REMEMBERING_BEST_UNKNOWN_ACCESS_PATH_SUBSTRING:
				traceString =
					"in best unknown access path";
				break;

			case COST_OF_CONGLOMERATE_SCAN1:
				cd = (ConglomerateDescriptor) objectParam1;
				cdString = dumpConglomerateDescriptor(cd);
				traceString =
					"Cost of conglomerate " +
					cdString +
					" scan for table number " +
					intParam1 + " is : ";
				break;

			case COST_OF_CONGLOMERATE_SCAN2:
				traceString =
					objectParam1.toString();
				break;

			case COST_OF_CONGLOMERATE_SCAN3:
				traceString =
					"\tNumber of extra first column predicates is : " +
					intParam1 +
					", extra first column selectivity is : " +
					doubleParam;
				break;

			case COST_OF_CONGLOMERATE_SCAN4:
				traceString =
					"\tNumber of extra start/stop predicates is : " +
					intParam1 +
					", extra start/stop selectivity is : " +
					doubleParam;
				break;

			case COST_OF_CONGLOMERATE_SCAN5:
				traceString =
					"\tNumber of extra qualifiers is : " +
					intParam1 +
					", extra qualifier selectivity is : " +
					doubleParam;
				break;

			case COST_OF_CONGLOMERATE_SCAN6:
				traceString =
					"\tNumber of extra non-qualifiers is : " +
					intParam1 +
					", extra non-qualifier selectivity is : " +
					doubleParam;
				break;

		    case COST_OF_CONGLOMERATE_SCAN7:
				traceString = 
					"\tNumber of start/stop statistics predicates is : " +
					intParam1 + 
					", statistics start/stop selectivity is : " +
					doubleParam;
				break;
		}
		if (SanityManager.DEBUG)
		{
			if (traceString == null)
			{
				SanityManager.THROWASSERT(
					"traceString expected to be non-null");
			}
		}
		lcc.appendOptimizerTraceOutput(traceString + "\n");
	}

	private String costForTable(Object cost, int tableNumber)
	{
		return cost + " for table " + tableNumber;
	}

	private String bestCost()
	{
		return "Best cost = " + bestCost + "\n";
	}

	private String buildJoinOrder(String prefix, boolean addJoinOrderNumber,
								  int joinOrderNumber, int[] joinOrder)
	{
		String joinOrderString = prefix;

		for (int i = 0; i <= joinPosition; i++)
		{
			joinOrderString = joinOrderString + " " + joinOrder[i];
		}
		if (addJoinOrderNumber)
		{
			joinOrderString = joinOrderString + " " + joinOrderNumber;
		}

		return joinOrderString + " with assignedTableMap = " + assignedTableMap + "\n\n";
	}

	private String lockModeThreshold(
						String lockMode, String relop,
						double rowCount, int threshold)
	{
		return
			"Lock mode set to " + lockMode + 
			" because estimated row count of " + rowCount +
			" " + relop + " than threshold of " + threshold;
	}

	private String costIncluding(
					String selectivityType, Object objectParam1, int intParam1)
	{
		return
			"Cost including extra " + selectivityType +
			" start/stop selectivity is : " +
			costForTable(objectParam1, intParam1);
	}

	private String dumpConglomerateDescriptor(ConglomerateDescriptor cd)
	{
		if (SanityManager.DEBUG)
		{
			return cd.toString();
		}

		String		keyString = "";
		String[]	columnNames = cd.getColumnNames();

		if (cd.isIndex() && columnNames != null )
		{
			IndexRowGenerator irg = cd.getIndexDescriptor();

			int[] keyColumns = irg.baseColumnPositions();

			keyString = ", key columns = {" + columnNames[keyColumns[0] - 1];
			for (int index = 1; index < keyColumns.length; index++)
			{
				keyString = keyString + ", " + columnNames[keyColumns[index] - 1];
			}
			keyString = keyString + "}";
		}

		return "CD: conglomerateNumber = " + cd.getConglomerateNumber() +
			   " name = " + cd.getConglomerateName() +
			   " uuid = " + cd.getUUID() +
			   " indexable = " + cd.isIndex() +
			   keyString;
	}
}
