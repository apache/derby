/*

   Derby - Class org.apache.derby.impl.sql.compile.DefaultOptTrace

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

import java.io.PrintWriter;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.AccessPath;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.JoinStrategy;
import org.apache.derby.iapi.sql.compile.OptTrace;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizableList;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.RequiredRowOrdering;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.util.JBitSet;

/**
 * This is the default optimizer tracing logic for use when a custom
 * tracer wasn't specified.
 */
public  class   DefaultOptTrace implements  OptTrace
{
    ////////////////////////////////////////////////////////////////////////
    //
    //	CONSTANTS
    //
    ////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////
    //
    //	STATE
    //
    ////////////////////////////////////////////////////////////////////////

    private StringBuilder _buffer;
    
    ////////////////////////////////////////////////////////////////////////
    //
    //	CONSTRUCTOR
    //
    ////////////////////////////////////////////////////////////////////////

    /** Make a DefaultOptTrace */
    public  DefaultOptTrace()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
        _buffer = new StringBuilder();
    }

    ////////////////////////////////////////////////////////////////////////
    //
    //	OptTrace BEHAVIOR
    //
    ////////////////////////////////////////////////////////////////////////

    public  void    traceStartStatement( String statementText )
    {
        appendTraceString( statementText );
    }
    
    public  void    traceStartQueryBlock( long timeOptimizationStarted, int optimizerID, OptimizableList optimizableList )
    {
        appendTraceString
            (
             "Optimization started at time " + 
             timeOptimizationStarted +
             " using optimizer " + optimizerID
             );
    }

    public  void    traceEndQueryBlock() {}

    public  void    traceTimeout( long currentTime, CostEstimate bestCost )
    {
        appendTraceString
            (
             "Optimization time exceeded at time " +
             currentTime + "\n" + bestCost
             );
    }
   
    public  void    traceVacuous()
    {
        appendTraceString( "No tables to optimize." );
    }

    public  void    traceCompleteJoinOrder()
    {
        appendTraceString( "We have a complete join order." );
    }

    public  void    traceSortCost( CostEstimate sortCost, CostEstimate currentCost )
    {
        appendTraceString( "Cost of sorting is " + sortCost );
        appendTraceString( "Total cost of non-sort-avoidance plan with sort cost added is " + currentCost );
    }

    public  void    traceNoBestPlan()
    {
        appendTraceString( "No best plan found." );
    }

    public  void    traceModifyingAccessPaths( int optimizerID )
    {
        appendTraceString( "Modifying access paths using optimizer " + optimizerID );
    }

    public  void    traceShortCircuiting( boolean timeExceeded, Optimizable thisOpt, int joinPosition )
    {
        String basis = (timeExceeded) ? "time exceeded" : "cost";
        if ( thisOpt.getBestAccessPath().getCostEstimate() == null ) { basis = "no best plan found"; }
        
        appendTraceString( "Short circuiting based on " + basis + " at join position " + joinPosition );
    }
    
    public  void    traceSkippingJoinOrder
        ( int nextOptimizable, int joinPosition, int[] proposedJoinOrder, JBitSet assignedTableMap )
    {
        appendTraceString
            (
             reportJoinOrder( "\n\nSkipping join order: ", true, nextOptimizable, joinPosition, proposedJoinOrder, assignedTableMap )
             );
    }

    public  void    traceIllegalUserJoinOrder()
    {
        appendTraceString( "User specified join order is not legal." );
    }
    
    public  void    traceUserJoinOrderOptimized()
    {
        appendTraceString( "User-specified join order has now been optimized." );
    }

    public  void    traceJoinOrderConsideration( int joinPosition, int[] proposedJoinOrder, JBitSet assignedTableMap )
    {
        appendTraceString
            (
             reportJoinOrder( "\n\nConsidering join order: ", false, 0, joinPosition, proposedJoinOrder, assignedTableMap )
             );
    }

    public  void    traceCostWithoutSortAvoidance( CostEstimate currentCost )
    {
        appendTraceString( "Total cost of non-sort-avoidance plan is " + currentCost );
    }

    public  void    traceCostWithSortAvoidance( CostEstimate currentSortAvoidanceCost )
    {
        appendTraceString( "Total cost of sort avoidance plan is " + currentSortAvoidanceCost );
    }

    public  void    traceCurrentPlanAvoidsSort( CostEstimate bestCost, CostEstimate currentSortAvoidanceCost )
    {
        appendTraceString
            (
             "Current plan is a sort avoidance plan." + 
             "\n\tBest cost is : " + bestCost +
             "\n\tThis cost is : " + currentSortAvoidanceCost
             );
    }

    public  void    traceCheapestPlanSoFar( int planType, CostEstimate currentCost )
    {
        appendTraceString( "This is the cheapest plan so far." );
        appendTraceString
            (
             "Plan is a " +
             (planType == Optimizer.NORMAL_PLAN ? "normal" : "sort avoidance") +
             " plan."
             );
        appendTraceString( "Cost of cheapest plan is " + currentCost );
    }

    public  void    traceSortNeededForOrdering( int planType, RequiredRowOrdering requiredRowOrdering )
    {
        appendTraceString
            (
             "Sort needed for ordering: " + (planType != Optimizer.SORT_AVOIDANCE_PLAN) +
             "\n\tRow ordering: " + requiredRowOrdering
             );
    }

    public  void    traceRememberingBestJoinOrder
//IC see: https://issues.apache.org/jira/browse/DERBY-6211
        ( int joinPosition, int[] bestJoinOrder, int planType, CostEstimate planCost, JBitSet assignedTableMap )
    {
        appendTraceString
            (
             reportJoinOrder( "\n\nRemembering join order as best: ", false, 0, joinPosition, bestJoinOrder, assignedTableMap )
             );
    }

    public  void    traceSkippingBecauseTooMuchMemory( int maxMemoryPerTable )
    {
        appendTraceString( "Skipping access path due to excess memory usage, maximum is " + maxMemoryPerTable );
    }

    public  void    traceCostOfNScans( int tableNumber, double rowCount, CostEstimate cost )
    {
        appendTraceString
            (
             "Cost of " + rowCount + " scans is: " + cost +
             " for table " + tableNumber
             );
    }

    public  void    traceSkipUnmaterializableHashJoin()
    {
        appendTraceString( "Skipping HASH JOIN because optimizable is not materializable" );
    }

    public  void    traceSkipHashJoinNoHashKeys()
    {
        appendTraceString( "Skipping HASH JOIN because there are no hash key columns" );
    }

    public  void    traceHashKeyColumns( int[] hashKeyColumns )
    {
        String  traceString = "# hash key columns = " + hashKeyColumns.length;
        for (int index = 0; index < hashKeyColumns.length; index++)
        {
            traceString = 
                "\n" + traceString + "hashKeyColumns[" + index +
                "] = " + hashKeyColumns[index];
        }

        appendTraceString( traceString );
    }

    public  void    traceOptimizingJoinNode()
    {
        appendTraceString( "Calling optimizeIt() for join node" );
    }

    public  void    traceConsideringJoinStrategy( JoinStrategy js, int tableNumber )
    {
        appendTraceString
            (
             "\nConsidering join strategy " + js +
             " for table " + tableNumber
             );
    }

    public  void    traceRememberingBestAccessPath( AccessPath accessPath, int tableNumber, int planType )
    {
        appendTraceString
            (
             "Remembering access path " + accessPath +
             " as truly the best for table " + tableNumber + 
             " for plan type " + (planType == Optimizer.NORMAL_PLAN ? " normal " : "sort avoidance") +
             "\n"
             );
    }

    public  void    traceNoMoreConglomerates( int tableNumber )
    {
        appendTraceString( "No more conglomerates to consider for table " + tableNumber );
    }

    public  void    traceConsideringConglomerate( ConglomerateDescriptor cd, int tableNumber )
    {
        appendTraceString
            (
             "\nConsidering conglomerate " + reportConglomerateDescriptor( cd ) +
             " for table " + tableNumber
             );
    }

    public  void    traceScanningHeapWithUniqueKey()
    {
        appendTraceString( "Scanning heap, but we have a full match on a unique key." );
    }

    public  void    traceAddingUnorderedOptimizable( int predicateCount )
    {
        appendTraceString( "Adding unordered optimizable, # of predicates = " + predicateCount );
    }

    public  void    traceChangingAccessPathForTable( int tableNumber )
    {
        appendTraceString( "Changing access path for table " + tableNumber );
    }

    public  void    traceNoStartStopPosition()
    {
        appendTraceString( "Lock mode set to MODE_TABLE because no start or stop position" );
    }

    public  void    traceNonCoveringIndexCost( double cost, int tableNumber )
    {
        appendTraceString
            (
             "Index does not cover query - cost including base row fetch is: " + cost +
             " for table " + tableNumber
             );
    }

    public  void    traceConstantStartStopPositions()
    {
        appendTraceString( "Lock mode set to MODE_RECORD because all start and stop positions are constant" );
    }

    public  void    traceEstimatingCostOfConglomerate( ConglomerateDescriptor cd, int tableNumber )
    {
        String  cdString = reportConglomerateDescriptor( cd );
        appendTraceString
            (
             "Estimating cost of conglomerate: " +
             reportCostForTable( cdString, tableNumber )
             );

    }

    public  void    traceLookingForSpecifiedIndex( String indexName, int tableNumber )
    {
        appendTraceString
            (
             "Looking for user-specified index: " + indexName +
             " for table " + tableNumber
             );
    }

    public  void    traceSingleMatchedRowCost( double cost, int tableNumber )
    {
        appendTraceString
            (
             "Guaranteed to match a single row - cost is: " + cost +
             " for table " + tableNumber
             );
    }

    public  void    traceCostIncludingExtra1stColumnSelectivity( CostEstimate cost, int tableNumber )
    {
        appendTraceString
            (
             "Cost including extra first column selectivity is : " + cost +
             " for table " + tableNumber
             );
    }

    public  void    traceNextAccessPath( String baseTable, int predicateCount )
    {
        appendTraceString
            (
             "Calling nextAccessPath() for base table " + baseTable +
             " with " + predicateCount + " predicates."
             );
    }

    public  void    traceCostIncludingExtraStartStop( CostEstimate cost, int tableNumber )
    {
        appendTraceString( reportCostIncluding( "start/stop", cost, tableNumber ) );
    }

    public  void    traceCostIncludingExtraQualifierSelectivity( CostEstimate cost, int tableNumber )
    {
        appendTraceString( reportCostIncluding( "qualifier", cost, tableNumber ) );
    }

    public  void    traceCostIncludingExtraNonQualifierSelectivity( CostEstimate cost, int tableNumber )
    {
        appendTraceString( reportCostIncluding( "non-qualifier", cost, tableNumber ) );
    }

    public  void    traceCostOfNoncoveringIndex( CostEstimate cost, int tableNumber )
    {
        appendTraceString
            (
             "Index does not cover query: cost including row fetch is: " +
             reportCostForTable( cost, tableNumber )
             );
    }

    public  void    traceRememberingJoinStrategy( JoinStrategy joinStrategy, int tableNumber )
    {
        appendTraceString
            (
             "\nRemembering join strategy " + joinStrategy +
             " as best for table " + tableNumber
             );
    }

    public  void    traceRememberingBestAccessPathSubstring( AccessPath ap, int tableNumber )
    {
        appendTraceString( "in best access path" );
    }

    public  void    traceRememberingBestSortAvoidanceAccessPathSubstring( AccessPath ap, int tableNumber )
    {
        appendTraceString( "in best sort avoidance access path" );
    }

    public  void    traceRememberingBestUnknownAccessPathSubstring( AccessPath ap, int tableNumber )
    {
        appendTraceString( "in best unknown access path" );
    }

    public  void    traceCostOfConglomerateScan
        (
         int    tableNumber,
         ConglomerateDescriptor cd,
         CostEstimate   costEstimate,
         int    numExtraFirstColumnPreds,
         double    extraFirstColumnSelectivity,
         int    numExtraStartStopPreds,
         double    extraStartStopSelectivity,
         int    startStopPredCount,
         double    statStartStopSelectivity,
         int    numExtraQualifiers,
         double    extraQualifierSelectivity,
         int    numExtraNonQualifiers,
         double    extraNonQualifierSelectivity
         )
    {
        appendTraceString
            (
             "Cost of conglomerate " + reportConglomerateDescriptor( cd ) +
             " scan for table number " + tableNumber + " is : "
             );
        appendTraceString( costEstimate.toString() );
        appendTraceString
            (
             "\tNumber of extra first column predicates is : " + numExtraFirstColumnPreds +
             ", extra first column selectivity is : " + extraFirstColumnSelectivity
             );
        appendTraceString
            (
             "\tNumber of extra start/stop predicates is : " + numExtraStartStopPreds +
             ", extra start/stop selectivity is : " + extraStartStopSelectivity
             );
        appendTraceString
            (
             "\tNumber of start/stop statistics predicates is : " + startStopPredCount +
             ", statistics start/stop selectivity is : " + statStartStopSelectivity
             );
        appendTraceString
            (
             "\tNumber of extra qualifiers is : " + numExtraQualifiers +
             ", extra qualifier selectivity is : " + extraQualifierSelectivity
             );
        appendTraceString
            (
             "\tNumber of extra non-qualifiers is : " + numExtraNonQualifiers +
             ", extra non-qualifier selectivity is : " + extraNonQualifierSelectivity
             );
    }

    public  void    traceCostIncludingCompositeSelectivityFromStats( CostEstimate cost, int tableNumber )
    {
        appendTraceString( reportCostIncluding( "selectivity from statistics", cost, tableNumber ) );
    }

    public  void    traceCompositeSelectivityFromStatistics( double statCompositeSelectivity )
    {
        appendTraceString( "Selectivity from statistics found. It is " + statCompositeSelectivity );
    }

    public  void    traceCostIncludingStatsForIndex( CostEstimate cost, int tableNumber )
    {
        appendTraceString( reportCostIncluding( "statistics for index being considered", cost, tableNumber ) );
    }

    public  void    printToWriter( PrintWriter out )
    {
        out.println( _buffer.toString() );
    }
    
    ////////////////////////////////////////////////////////////////////////
    //
    //	REPORTING MINIONS
    //
    ////////////////////////////////////////////////////////////////////////

	private String reportJoinOrder
        (
         String prefix,
         boolean addJoinOrderNumber,
         int joinOrderNumber,
         int joinPosition,
         int[] joinOrder,
         JBitSet    assignedTableMap
         )
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
        StringBuilder joinOrderString = new StringBuilder();
        joinOrderString.append(prefix);

		for (int i = 0; i <= joinPosition; i++)
		{
			joinOrderString.append(" ").append(joinOrder[i]);
		}
		if (addJoinOrderNumber)
		{
			joinOrderString.append(" ").append(joinOrderNumber);
		}

        joinOrderString.append(" with assignedTableMap = ").append(assignedTableMap).append("\n\n");
        return joinOrderString.toString();
	}

	private String reportConglomerateDescriptor( ConglomerateDescriptor cd )
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
    
	private String reportCostForTable( Object cost, int tableNumber )
	{
		return cost + " for table " + tableNumber;
	}

	private String reportCostIncluding( String selectivityType, CostEstimate cost, int tableNumber )
	{
		return
			"Cost including extra " + selectivityType +
			" start/stop selectivity is : " +
			reportCostForTable( cost, tableNumber );
	}

    /** Append a string to the optimizer trace */
    private void    appendTraceString( String traceString )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
        _buffer.append( traceString );
        _buffer.append( "\n" );
    }

}
