/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.DummyOptTrace

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.io.PrintWriter;

import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.util.JBitSet;
import org.apache.derby.iapi.sql.compile.AccessPath;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.JoinStrategy;
import org.apache.derby.iapi.sql.compile.OptTrace;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizableList;
import org.apache.derby.iapi.sql.compile.RequiredRowOrdering;

/**
 * Dummy implementation of OptTrace to test the loading of custom
 * trace logic for the Optimizer.
 */
public  class   DummyOptTrace   implements  OptTrace
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

    private StringBuffer    _buffer;
    private static  String  _fullTrace;

    ////////////////////////////////////////////////////////////////////////
    //
    //	CONSTRUCTOR
    //
    ////////////////////////////////////////////////////////////////////////

    /** 0-arg constructor required by OptTrace contract */
    public  DummyOptTrace()
    {
        _buffer = new StringBuffer();
    }

    ////////////////////////////////////////////////////////////////////////
    //
    //	FUNCTION FOR RETRIEVING THE FULL TRACE
    //
    ////////////////////////////////////////////////////////////////////////

    public  static  String  fullTrace()    { return _fullTrace; }

    ////////////////////////////////////////////////////////////////////////
    //
    //	BEHAVIOR
    //
    ////////////////////////////////////////////////////////////////////////

    //
    // We only provide implementation for these methods.
    //
    public  void    traceStartStatement( String statementText ) { _buffer.append( "<text>" + statementText + "</text>" ); }
    public  void    printToWriter( PrintWriter out )   { _fullTrace = _buffer.toString(); }

    //
    // Don't need to bother implementing the rest of the behavior.
    //
    public  void    traceStartQueryBlock( long timeOptimizationStarted, int optimizerID, OptimizableList optimizableList ) {}
    public  void    traceEndQueryBlock() {}
    public  void    traceTimeout( long currentTime, CostEstimate bestCost ) {}
    public  void    traceVacuous() {}
    public  void    traceCompleteJoinOrder() {}
    public  void    traceSortCost( CostEstimate sortCost, CostEstimate currentCost ) {}
    public  void    traceNoBestPlan() {}
    public  void    traceModifyingAccessPaths( int optimizerID ) {}
    public  void    traceShortCircuiting( boolean timeExceeded, Optimizable thisOpt, int joinPosition ) {}
    public  void    traceSkippingJoinOrder( int nextOptimizable, int joinPosition, int[] proposedJoinOrder, JBitSet assignedTableMap ) {}
    public  void    traceIllegalUserJoinOrder() {}
    public  void    traceUserJoinOrderOptimized() {}
    public  void    traceJoinOrderConsideration( int joinPosition, int[] proposedJoinOrder, JBitSet assignedTableMap ) {}
    public  void    traceCostWithoutSortAvoidance( CostEstimate currentCost ) {}
    public  void    traceCostWithSortAvoidance( CostEstimate currentSortAvoidanceCost ) {}
    public  void    traceCurrentPlanAvoidsSort( CostEstimate bestCost, CostEstimate currentSortAvoidanceCost ) {}
    public  void    traceCheapestPlanSoFar( int planType, CostEstimate currentCost ) {}
    public  void    traceSortNeededForOrdering( int planType, RequiredRowOrdering requiredRowOrdering ) {}
    public  void    traceRememberingBestJoinOrder
        ( int joinPosition, int[] bestJoinOrder, int planType, CostEstimate planCost, JBitSet assignedTableMap ) {}
    public  void    traceSkippingBecauseTooMuchMemory( int maxMemoryPerTable ) {}
    public  void    traceCostOfNScans( int tableNumber, double rowCount, CostEstimate cost ) {}
    public  void    traceSkipUnmaterializableHashJoin() {}
    public  void    traceSkipHashJoinNoHashKeys() {}
    public  void    traceHashKeyColumns( int[] hashKeyColumns ) {}
    public  void    traceOptimizingJoinNode() {}
    public  void    traceConsideringJoinStrategy( JoinStrategy js, int tableNumber ) {}
    public  void    traceRememberingBestAccessPath( AccessPath accessPath, int tableNumber, int planType ) {}
    public  void    traceNoMoreConglomerates( int tableNumber ) {}
    public  void    traceConsideringConglomerate( ConglomerateDescriptor cd, int tableNumber ) {}
    public  void    traceScanningHeapWithUniqueKey() {}
    public  void    traceAddingUnorderedOptimizable( int predicateCount ) {}
    public  void    traceChangingAccessPathForTable( int tableNumber ) {}
    public  void    traceNoStartStopPosition() {}
    public  void    traceNonCoveringIndexCost( double cost, int tableNumber ) {}
    public  void    traceConstantStartStopPositions() {}
    public  void    traceEstimatingCostOfConglomerate( ConglomerateDescriptor cd, int tableNumber ) {}
    public  void    traceLookingForSpecifiedIndex( String indexName, int tableNumber ) {}
    public  void    traceSingleMatchedRowCost( double cost, int tableNumber ) {}
    public  void    traceCostIncludingExtra1stColumnSelectivity( CostEstimate cost, int tableNumber ) {}
    public  void    traceNextAccessPath( String baseTable, int predicateCount ) {}
    public  void    traceCostIncludingExtraStartStop( CostEstimate cost, int tableNumber ) {}
    public  void    traceCostIncludingExtraQualifierSelectivity( CostEstimate cost, int tableNumber ) {}
    public  void    traceCostIncludingExtraNonQualifierSelectivity( CostEstimate cost, int tableNumber ) {}
    public  void    traceCostOfNoncoveringIndex( CostEstimate cost, int tableNumber ) {}
    public  void    traceRememberingJoinStrategy( JoinStrategy joinStrategy, int tableNumber ) {}
    public  void    traceRememberingBestAccessPathSubstring( AccessPath ap, int tableNumber ) {}
    public  void    traceRememberingBestSortAvoidanceAccessPathSubstring( AccessPath ap, int tableNumber ) {}
    public  void    traceRememberingBestUnknownAccessPathSubstring( AccessPath ap, int tableNumber ) {}
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
         )  {}
    public  void    traceCostIncludingCompositeSelectivityFromStats( CostEstimate cost, int tableNumber ) {}
    public  void    traceCompositeSelectivityFromStatistics( double statCompositeSelectivity ) {}
    public  void    traceCostIncludingStatsForIndex( CostEstimate cost, int tableNumber ) {}

    
    ////////////////////////////////////////////////////////////////////////
    //
    //	NESTED SUBCLASS WHICH IS MISSING THE 0-ARG CONSTRUCTOR
    //
    ////////////////////////////////////////////////////////////////////////

    public  static  final   class   BadSubclass extends DummyOptTrace
    {
        public  BadSubclass( int dummy ) {}
    }
}
