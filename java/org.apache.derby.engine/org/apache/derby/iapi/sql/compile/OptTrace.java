/*

   Derby - Class org.apache.derby.iapi.sql.compile.OptTrace

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

package org.apache.derby.iapi.sql.compile;

import java.io.PrintWriter;

import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.util.JBitSet;

/**
 * Interface for optimizer tracing.
 */
public  interface   OptTrace
{
    ////////////////////////////////////////////////////////////////////////
    //
    //	CONSTANTS
    //
    ////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////
    //
    //	BEHAVIOR
    //
    ////////////////////////////////////////////////////////////////////////

    /** Start the start of tracing a statement. */
    public  void    traceStartStatement( String statementText );

    /** Start optimizer tracing for a query block. */
    public  void    traceStartQueryBlock( long timeOptimizationStarted, int optimizerID, OptimizableList optimizableList );

    /** End tracing the optimization of a query block. */
    public  void    traceEndQueryBlock();

    /** Say that the optimizer ran out of time. */
    public  void    traceTimeout( long currentTime, CostEstimate bestCost );

    /** Say that there's nothing to optimizer. */
    public  void    traceVacuous();

    /** Say that we have a complete join order. */
    public  void    traceCompleteJoinOrder();

    /** Report the sort cost. */
    public  void    traceSortCost( CostEstimate sortCost, CostEstimate currentCost );

    /** Say that we couldn't find a best plan. */
    public  void    traceNoBestPlan();

    /** Say that we're modifying access paths. */
    public  void    traceModifyingAccessPaths( int optimizerID );

    /** Say that we short-circuited a join order. */
    public  void    traceShortCircuiting( boolean timeExceeded, Optimizable thisOpt, int joinPosition );

    /** Say that we're skipping the join order starting with the next optimizable. */
    public  void    traceSkippingJoinOrder( int nextOptimizable, int joinPosition, int[] proposedJoinOrder, JBitSet assignedTableMap );

    /** Say that the user specified an impossible join order. */
    public  void    traceIllegalUserJoinOrder();

    /** Say that we have optimized the user-specified join order. */
    public  void    traceUserJoinOrderOptimized();

    /** Say that we're considering a join order. */
    public  void    traceJoinOrderConsideration( int joinPosition, int[] proposedJoinOrder, JBitSet assignedTableMap );

    /** Report the cost without a sort. */
    public  void    traceCostWithoutSortAvoidance( CostEstimate currentCost );

    /** Report the cost with sort avoidance.*/
    public  void    traceCostWithSortAvoidance( CostEstimate currentSortAvoidanceCost );

    /** Say that the current plan avoids a sort. */
    public  void    traceCurrentPlanAvoidsSort( CostEstimate bestCost, CostEstimate currentSortAvoidanceCost );

    /** Report the cost of the cheapest plan so far. */
    public  void    traceCheapestPlanSoFar( int planType, CostEstimate currentCost );

    /** Report that this plan needs a sort */
    public  void    traceSortNeededForOrdering( int planType, RequiredRowOrdering requiredRowOrdering );

    /** Say that we are remembering the current plan as the best join order so far. */
    public  void    traceRememberingBestJoinOrder
        ( int joinPosition, int[] bestJoinOrder, int planType, CostEstimate planCost, JBitSet assignedTableMap );

    /** Say that we are skipping a plan because it consumes too much memory. */
    public  void    traceSkippingBecauseTooMuchMemory( int maxMemoryPerTable );

    /** Report the cost of scanning a table a certain number of times */
    public  void    traceCostOfNScans( int tableNumber, double rowCount, CostEstimate cost );

    /** Say that we won't consider a hash join because the result can't be materialized */
    public  void    traceSkipUnmaterializableHashJoin();

    /** Say we won't consider a hash join because there are no hash key columns. */
    public  void    traceSkipHashJoinNoHashKeys();

    /** Report the columns being traced */
    public  void    traceHashKeyColumns( int[] hashKeyColumns );

    /** Say that we're starting to optimize a join node */
    public  void    traceOptimizingJoinNode();

    /** Say that we're considering a particular join strategy on a particular table. */
    public  void    traceConsideringJoinStrategy( JoinStrategy js, int tableNumber );

    /** Report that we've found a best access path. */
    public  void    traceRememberingBestAccessPath( AccessPath accessPath, int tableNumber, int planType );

    /** Say that we have exhausted the conglomerate possibilities for a table. */
    public  void    traceNoMoreConglomerates( int tableNumber );

    /** Report that we are considering a conglomerate for a table. */
    public  void    traceConsideringConglomerate( ConglomerateDescriptor cd, int tableNumber );

    /** Say that we're considering scanning a heap even though we have a unique key match. */
    public  void    traceScanningHeapWithUniqueKey();

    /** Say that we're adding an unordered optimizable. */
    public  void    traceAddingUnorderedOptimizable( int predicateCount );

    /** Say that we're considering a different access path for a table. */
    public  void    traceChangingAccessPathForTable( int tableNumber );

    /** Say that we're setting the lock mode to MODE_TABLE because there is no start/stop position. */
    public  void    traceNoStartStopPosition();

    /** Say that we're considering a non-covering index. */
    public  void    traceNonCoveringIndexCost( double cost, int tableNumber );

    /** Say that we're setting the lock mode to MODE_RECORD because the start and stop positions are all constant. */
    public  void    traceConstantStartStopPositions();

    /** Report the cost of using a particular conglomerate to scan a table. */
    public  void    traceEstimatingCostOfConglomerate( ConglomerateDescriptor cd, int tableNumber );

    /** Say that we're looking for an index specified by optimizer hints. */
    public  void    traceLookingForSpecifiedIndex( String indexName, int tableNumber );

    /** Report the cost of a scan which will match exactly one row. */
    public  void    traceSingleMatchedRowCost( double cost, int tableNumber );

    /** Report the cost of a scan given the selectivity of an extra first column. */
    public  void    traceCostIncludingExtra1stColumnSelectivity( CostEstimate cost, int tableNumber );

    /** Report that we are advancing to the next access path for the table. */
    public  void    traceNextAccessPath( String baseTable, int predicateCount );

    /** Report the cost if we include an extra start/stop. */
    public  void    traceCostIncludingExtraStartStop( CostEstimate cost, int tableNumber );

    /**  Report the cost if we include an extra qualifier. */
    public  void    traceCostIncludingExtraQualifierSelectivity( CostEstimate cost, int tableNumber );

    /** Report the cost if we include an extra non-qualifier. */
    public  void    traceCostIncludingExtraNonQualifierSelectivity( CostEstimate cost, int tableNumber );

    /** Report the cost of using a non-covering index. */
    public  void    traceCostOfNoncoveringIndex( CostEstimate cost, int tableNumber );

    /** Say that we've found a new best join strategy for the table. */
    public  void    traceRememberingJoinStrategy( JoinStrategy joinStrategy, int tableNumber );

    /** Report the best access path for the table so far. */
    public  void    traceRememberingBestAccessPathSubstring( AccessPath ap, int tableNumber );

    /** Report the best sort-avoiding access path for this table so far. */
    public  void    traceRememberingBestSortAvoidanceAccessPathSubstring( AccessPath ap, int tableNumber );

    /** Report an optimizer failure, e.g., while optimizing an outer join */
    public  void    traceRememberingBestUnknownAccessPathSubstring( AccessPath ap, int tableNumber );

    /** Report the cost of a conglomerate scan. */
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
         );

    /** Report the cost based on selectivities coming out of SYSSTATISTICS. */
    public  void    traceCostIncludingCompositeSelectivityFromStats( CostEstimate cost, int tableNumber );

    /** Report the selectivity calculated from SYSSTATISTICS. */
    public  void    traceCompositeSelectivityFromStatistics( double statCompositeSelectivity );

    /** Report the cost based on index statistics. */
    public  void    traceCostIncludingStatsForIndex( CostEstimate cost, int tableNumber );
    
    /** Print the trace so far. */
    public  void    printToWriter( PrintWriter out );

}
