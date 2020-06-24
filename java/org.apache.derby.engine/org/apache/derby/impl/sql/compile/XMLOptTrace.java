/*

   Derby - Class org.apache.derby.impl.sql.compile.XMLOptTrace

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
import java.util.Date;
import java.util.Stack;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.sql.compile.AccessPath;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.JoinStrategy;
import org.apache.derby.iapi.sql.compile.OptTrace;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizableList;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.OptimizerPlan;
import org.apache.derby.iapi.sql.compile.RequiredRowOrdering;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.UniqueTupleDescriptor;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.iapi.util.JBitSet;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Optimizer tracer which produces output in an xml format.
 */
//IC see: https://issues.apache.org/jira/browse/DERBY-6211
class   XMLOptTrace implements  OptTrace
{
    ////////////////////////////////////////////////////////////////////////
    //
    //	CONSTANTS
    //
    ////////////////////////////////////////////////////////////////////////

    // statement tags
    private static  final   String  STMT = "statement";
    private static  final   String  STMT_ID = "stmtID";
    private static  final   String  STMT_TEXT = "stmtText";

    // query block tags
    private static  final   String  QBLOCK = "queryBlock";
    private static  final   String  QBLOCK_OPTIMIZER_ID = "qbOptimizerID";
    private static  final   String  QBLOCK_START_TIME = "qbStartTime";
    private static  final   String  QBLOCK_ID = "qbID";
    private static  final   String  QBLOCK_OPTIMIZABLE = "qbOptimizable";
    private static  final   String  QBLOCK_OPT_TABLE_NUMBER = "qboTableNumber";
    private static  final   String  QBLOCK_TIMEOUT = "qbTimeout";
    private static  final   String  QBLOCK_VACUOUS = "qbVacuous";
    private static  final   String  QBLOCK_SORT_COST = "qbSortCost";
    private static  final   String  QBLOCK_TOTAL_COST = "qbTotalCost";
    private static  final   String  QBLOCK_NO_BEST_PLAN = "qbNoBestPlan";
    private static  final   String  QBLOCK_SKIP = "qbSkip";

    // join order tags
    private static  final   String  JO = "joinOrder";
    private static  final   String  JO_COMPLETE = "joComplete";
    private static  final   String  JO_SLOT = "joSlot";

    // decoration tags
    private static  final   String  DECORATION = "decoration";
    private static  final   String  DECORATION_CONGLOM_NAME = "decConglomerateName";
    private static  final   String  DECORATION_KEY = "decKey";
    private static  final   String  DECORATION_TABLE_NAME = "decTableName";
    private static  final   String  DECORATION_JOIN_STRATEGY = "decJoinStrategy";
    private static  final   String  DECORATION_SKIP = "decSkip";
    private static  final   String  DECORATION_CONGLOM_COST = "decConglomerateCost";
    private static  final   String  DECORATION_FIRST_COLUMN_SELECTIVITY = "decExtraFirstColumnPreds";
    private static  final   String  DECORATION_EXTRA_START_STOP_SELECTIVITY = "decExtraFirstStartStopPreds";
    private static  final   String  DECORATION_START_STOP_SELECTIVITY = "decStartStopPred";
    private static  final   String  DECORATION_EXTRA_QUALIFIERS = "decExtraQualifiers";
    private static  final   String  DECORATION_EXTRA_NON_QUALIFIERS = "decExtraNonQualifiers";

    // skip tags
    private static  final   String  SKIP_REASON = "skipReason";

    // plan cost tags
    private static  final   String  PC = "planCost";
    private static  final   String  PC_TYPE = "pcType";
    private static  final   String  PC_COMPLETE = "pcComplete";
    private static  final   String  PC_AVOID_SORT= "pcAvoidSort";
    private static  final   String  PC_SUMMARY= "pcSummary";

    // CostEstimate tags
    private static  final   String  CE_ESTIMATED_COST = "ceEstimatedCost";
    private static  final   String  CE_ROW_COUNT = "ceEstimatedRowCount";
    private static  final   String  CE_SINGLE_SCAN_ROW_COUNT = "ceSingleScanRowCount";

    // selectivity tags
    private static  final   String  SEL_COUNT = "selCount";
    private static  final   String  SEL_SELECTIVITY = "selSelectivity";

    // distinguish table function names from conglomerate names
    private static  final   String  TABLE_FUNCTION_FLAG = "()";
    
    //
    // Statement and view for declaring a table function which reads the planCost element.
    // This table function is an instance of the XmlVTI and assumes that you have
    // already declared an ArrayList user-type and an asList factory function for it.
    //
    static  final   String  PLAN_COST_VTI =
        "create function planCost\n" +
        "(\n" +
        "    xmlResourceName varchar( 32672 ),\n" +
        "    rowTag varchar( 32672 ),\n" +
        "    parentTags ArrayList,\n" +
        "    childTags ArrayList\n" +
        ")\n" +
        "returns table\n" +
        "(\n" +
        "    text varchar( 32672 ),\n" +
        "    stmtID    int,\n" +
        "    qbID   int,\n" +
        "    complete  boolean,\n" +
        "    summary   varchar( 32672 ),\n" +
        "    type        varchar( 50 ),\n" +
        "    estimatedCost        double,\n" +
        "    estimatedRowCount    bigint\n" +
        ")\n" +
        "language java parameter style derby_jdbc_result_set no sql\n" +
        "external name 'org.apache.derby.vti.XmlVTI.xmlVTI'\n";

    static  final   String  PLAN_COST_VIEW =
        "create view planCost as\n" +
        "select *\n" +
        "from table\n" +
        "(\n" +
        "    planCost\n" +
        "    (\n" +
        "        'FILE_URL',\n" +
        "        'planCost',\n" +
        "        asList( '" + STMT_TEXT + "', '" + STMT_ID + "', '" + QBLOCK_ID + "' ),\n" +
//IC see: https://issues.apache.org/jira/browse/DERBY-6211
        "        asList( '" + PC_COMPLETE + "', '" + PC_SUMMARY + "', '" + PC_TYPE + "', '" +
        CE_ESTIMATED_COST + "', '" + CE_ROW_COUNT + "' )\n" +
        "     )\n" +
        ") v\n";
        
    ////////////////////////////////////////////////////////////////////////
    //
    //	NESTED CLASSES
    //
    ////////////////////////////////////////////////////////////////////////

    public  static  final   class   QueryBlock
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6211
        final   int                 queryBlockID;
        final   OptimizableList optimizableList;
        final   Element         queryBlockElement;
        
        Element         currentJoinsElement;
        int[]              currentJoinOrder;
        Element         currentBestPlan;

        // reset per join order
        JoinStrategy    currentDecorationStrategy;
        Element         currentDecoration;

        public  QueryBlock
            (
             int    queryBlockID,
             OptimizableList    optimizableList,
             Element    queryBlockElement
             )
        {
            this.queryBlockID = queryBlockID;
            this.optimizableList = optimizableList;
            this.queryBlockElement = queryBlockElement;
        }
    }

    ////////////////////////////////////////////////////////////////////////
    //
    //	STATE
    //
    ////////////////////////////////////////////////////////////////////////

    private Document    _doc;
    private Element         _root;
    
    private Element         _currentStatement;
    private int                 _currentStatementID;
    private QueryBlock      _currentQueryBlock;
    private int                 _maxQueryID;


    // pushed and popped on query block boundaries
    private Stack<QueryBlock>  _queryBlockStack;

    // context
    private ContextManager  _cm;
    private LanguageConnectionContext   _lcc;

    ////////////////////////////////////////////////////////////////////////
    //
    //	CONSTRUCTOR
    //
    ////////////////////////////////////////////////////////////////////////

    /** 0-arg constructor required by OptTrace contract */
    public  XMLOptTrace()
        throws ParserConfigurationException
    {
        _doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
        _root = createElement( null, "optimizerTrace", null );
        _doc.appendChild( _root );
    }

    ////////////////////////////////////////////////////////////////////////
    //
    //	BEHAVIOR
    //
    ////////////////////////////////////////////////////////////////////////

    public  void    traceStartStatement( String statementText )
    {
        _currentStatementID++;
//IC see: https://issues.apache.org/jira/browse/DERBY-6211
        _maxQueryID = 0;
        _currentQueryBlock = null;
        _queryBlockStack = new Stack<QueryBlock>();
        
        _currentStatement = createElement( _root, STMT, null );
        _currentStatement .setAttribute( STMT_ID, Integer.toString( _currentStatementID ) );

        createElement( _currentStatement, STMT_TEXT, statementText );
    }
    
    public  void    traceStartQueryBlock( long timeOptimizationStarted, int optimizerID, OptimizableList optimizableList )
    {
        _maxQueryID++;
        if ( _currentQueryBlock != null ) { _queryBlockStack.push( _currentQueryBlock ); }

        Element queryElement = createElement( _currentStatement, QBLOCK, null );
        queryElement.setAttribute( QBLOCK_OPTIMIZER_ID, Integer.toString( optimizerID ) );
        queryElement.setAttribute( QBLOCK_START_TIME, formatTimestamp( timeOptimizationStarted ) );
        queryElement.setAttribute( QBLOCK_ID, Integer.toString( _maxQueryID ) );

        _currentQueryBlock = new QueryBlock( _maxQueryID, optimizableList, queryElement );

        if ( optimizableList != null )
        {
            for ( int i = 0; i < optimizableList.size(); i++ )
            {
                Optimizable opt = optimizableList.getOptimizable( i );

                if ( _cm == null )
                {
//IC see: https://issues.apache.org/jira/browse/DERBY-6211
                    _cm = ((QueryTreeNode) opt).getContextManager();
                    _lcc = (LanguageConnectionContext) _cm.getContext( LanguageConnectionContext.CONTEXT_ID );
                }
                
                Element optElement = createElement
                    ( queryElement, QBLOCK_OPTIMIZABLE, getOptimizableName( opt ).getFullSQLName() );
                optElement.setAttribute( QBLOCK_OPT_TABLE_NUMBER, Integer.toString( opt.getTableNumber() ) );
            }
        }
    }
    
    public  void    traceEndQueryBlock()
    {
        if ( _queryBlockStack.size() > 0 )
        {
            _currentQueryBlock = _queryBlockStack.pop();
        }
    }

    public  void    traceTimeout( long currentTime, CostEstimate bestCost )
    {
        Element timeout = createElement( _currentQueryBlock.queryBlockElement, QBLOCK_TIMEOUT, null );
        formatCost( timeout, bestCost );
    }
    
    public  void    traceVacuous()
    {
        createElement( _currentQueryBlock.queryBlockElement, QBLOCK_VACUOUS, null );
    }
    
    public  void    traceCompleteJoinOrder()
    {
        if ( _currentQueryBlock.currentJoinsElement != null )
        { _currentQueryBlock.currentJoinsElement.setAttribute( JO_COMPLETE, "true" ); }
    }
    
    public  void    traceSortCost( CostEstimate sortCost, CostEstimate currentCost )
    {
        Element sc = createElement( _currentQueryBlock.queryBlockElement, QBLOCK_SORT_COST, null );
        formatCost( sc, sortCost );
            
        Element tcis = createElement( _currentQueryBlock.queryBlockElement, QBLOCK_TOTAL_COST, null );
        formatCost( tcis, currentCost );
    }
    
    public  void    traceNoBestPlan()
    {
        createElement( _currentQueryBlock.queryBlockElement, QBLOCK_NO_BEST_PLAN, null );
    }
    
    public  void    traceModifyingAccessPaths( int optimizerID ) {}
    
    public  void    traceShortCircuiting( boolean timeExceeded, Optimizable thisOpt, int joinPosition ) {}
    
    public  void    traceSkippingJoinOrder( int nextOptimizable, int joinPosition, int[] proposedJoinOrder, JBitSet assignedTableMap )
    {
        Optimizable opt = _currentQueryBlock.optimizableList.getOptimizable( nextOptimizable );
//IC see: https://issues.apache.org/jira/browse/DERBY-6211

        Element skip = formatSkip
            (
             _currentQueryBlock.queryBlockElement, QBLOCK_SKIP,
//IC see: https://issues.apache.org/jira/browse/DERBY-6211
             "Useless join order. " + getOptimizableName( opt ).getFullSQLName() + " depends on tables after it in the join order"
             );
        formatJoinOrder( skip, proposedJoinOrder );
    }
    
    public  void    traceIllegalUserJoinOrder() {}
    public  void    traceUserJoinOrderOptimized() {}
    
    public  void    traceJoinOrderConsideration( int joinPosition, int[] proposedJoinOrder, JBitSet assignedTableMap )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6211
        _currentQueryBlock.currentJoinsElement = createElement( _currentQueryBlock.queryBlockElement, JO, null );
        _currentQueryBlock.currentJoinOrder = proposedJoinOrder;

        _currentQueryBlock.currentDecorationStrategy = null;
        _currentQueryBlock.currentDecoration = null;

        formatJoinOrder( _currentQueryBlock.currentJoinsElement, proposedJoinOrder );
    }

    public  void    traceCostWithoutSortAvoidance( CostEstimate currentCost )
    {
        formatPlanCost
            (
             _currentQueryBlock.currentJoinsElement, "withoutSortAvoidance",
             _currentQueryBlock.currentJoinOrder, Optimizer.NORMAL_PLAN, currentCost
             );
    }
    
    public  void    traceCostWithSortAvoidance( CostEstimate currentSortAvoidanceCost )
    {
        formatPlanCost
            (
//IC see: https://issues.apache.org/jira/browse/DERBY-6211
             _currentQueryBlock.currentJoinsElement, "withSortAvoidance",
             _currentQueryBlock.currentJoinOrder, Optimizer.SORT_AVOIDANCE_PLAN, currentSortAvoidanceCost
             );
    }
    
    public  void    traceCurrentPlanAvoidsSort( CostEstimate bestCost, CostEstimate currentSortAvoidanceCost ) {}
    public  void    traceCheapestPlanSoFar( int planType, CostEstimate currentCost ) {}
    public  void    traceSortNeededForOrdering( int planType, RequiredRowOrdering requiredRowOrdering ) {}
    
    public  void    traceRememberingBestJoinOrder
        ( int joinPosition, int[] bestJoinOrder, int planType, CostEstimate planCost, JBitSet assignedTableMap )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6211
        if ( _currentQueryBlock.currentBestPlan != null )
        { _currentQueryBlock.queryBlockElement.removeChild( _currentQueryBlock.currentBestPlan ); }
        _currentQueryBlock.currentBestPlan = formatPlanCost
            ( _currentQueryBlock.queryBlockElement, "bestPlan", bestJoinOrder, planType, planCost );
    }
    
    public  void    traceSkippingBecauseTooMuchMemory( int maxMemoryPerTable )
    {
        formatSkip
            ( _currentQueryBlock.currentDecoration, DECORATION_SKIP, "Exceeds limit on memory per table: " + maxMemoryPerTable );
    }
    
    public  void    traceCostOfNScans( int tableNumber, double rowCount, CostEstimate cost ) {}
    
    public  void    traceSkipUnmaterializableHashJoin()
    {
        formatSkip
            ( _currentQueryBlock.currentDecoration, DECORATION_SKIP, "Hash strategy not possible because table is not materializable" );
    }
    
    public  void    traceSkipHashJoinNoHashKeys()
    {
        formatSkip( _currentQueryBlock.currentDecoration, DECORATION_SKIP, "No hash keys" );
    }
    
    public  void    traceHashKeyColumns( int[] hashKeyColumns ) {}
    public  void    traceOptimizingJoinNode() {}
    
    public  void    traceConsideringJoinStrategy( JoinStrategy js, int tableNumber )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6211
        _currentQueryBlock.currentDecorationStrategy = js;
    }
    
    public  void    traceRememberingBestAccessPath( AccessPath accessPath, int tableNumber, int planType ) {}
    public  void    traceNoMoreConglomerates( int tableNumber ) {}
    
    public  void    traceConsideringConglomerate( ConglomerateDescriptor cd, int tableNumber )
    {
        Optimizable opt = getOptimizable( tableNumber );
        
        _currentQueryBlock.currentDecoration = createElement( _currentQueryBlock.currentJoinsElement, DECORATION, null );
//IC see: https://issues.apache.org/jira/browse/DERBY-6211

        _currentQueryBlock.currentDecoration.setAttribute( DECORATION_CONGLOM_NAME, cd.getConglomerateName() );
        _currentQueryBlock.currentDecoration.setAttribute( DECORATION_TABLE_NAME, getOptimizableName( opt ).toString() );
        _currentQueryBlock.currentDecoration.setAttribute
            ( DECORATION_JOIN_STRATEGY, _currentQueryBlock.currentDecorationStrategy.getName() );
        
		String[]	columnNames = cd.getColumnNames();

		if ( cd.isIndex() && (columnNames != null) )
		{
			int[]   keyColumns = cd.getIndexDescriptor().baseColumnPositions();

            for ( int i = 0; i < keyColumns.length; i++ )
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-6211
                createElement( _currentQueryBlock.currentDecoration, DECORATION_KEY, columnNames[ keyColumns[ i ] - 1 ] );
            }
		}
    }
    
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
         )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6211
        Element cost = createElement( _currentQueryBlock.currentDecoration, DECORATION_CONGLOM_COST, null );
        cost.setAttribute( "name", cd.getConglomerateName() );

        formatCost( cost, costEstimate );
        formatSelectivity( cost, DECORATION_FIRST_COLUMN_SELECTIVITY, numExtraFirstColumnPreds, extraFirstColumnSelectivity );
        formatSelectivity( cost, DECORATION_EXTRA_START_STOP_SELECTIVITY, numExtraStartStopPreds, extraStartStopSelectivity );
        formatSelectivity( cost, DECORATION_START_STOP_SELECTIVITY, startStopPredCount, statStartStopSelectivity );
        formatSelectivity( cost, DECORATION_EXTRA_QUALIFIERS, numExtraQualifiers, extraQualifierSelectivity );
        formatSelectivity( cost, DECORATION_EXTRA_NON_QUALIFIERS, numExtraNonQualifiers, extraNonQualifierSelectivity );
    }
    
    public  void    traceCostIncludingCompositeSelectivityFromStats( CostEstimate cost, int tableNumber ) {}
    public  void    traceCompositeSelectivityFromStatistics( double statCompositeSelectivity ) {}
    public  void    traceCostIncludingStatsForIndex( CostEstimate cost, int tableNumber ) {}

    public  void    printToWriter( PrintWriter out )
    {
        try {
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource source = new DOMSource( _doc );
            StreamResult result = new StreamResult( out );

            // pretty-print
            transformer.setOutputProperty( OutputKeys.OMIT_XML_DECLARATION, "no" );
            transformer.setOutputProperty( OutputKeys.METHOD, "xml" );
            transformer.setOutputProperty( OutputKeys.INDENT, "yes" );
            transformer.setOutputProperty( OutputKeys.ENCODING, "UTF-8" );
            transformer.setOutputProperty( "{http://xml.apache.org/xslt}indent-amount", "4" );
            
            transformer.transform( source, result );
            
        }   catch (Throwable t) { printThrowable( t ); }
    }

    ////////////////////////////////////////////////////////////////////////
    //
    //	MINIONS
    //
    ////////////////////////////////////////////////////////////////////////

    /** Get the Optimizable with the given tableNumber */
    private Optimizable getOptimizable( int tableNumber )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6211
        for ( int i = 0; i < _currentQueryBlock.optimizableList.size(); i++ )
        {
            Optimizable candidate = _currentQueryBlock.optimizableList.getOptimizable( i );
            
            if ( tableNumber == candidate.getTableNumber() )    { return candidate; }
        }

        return null;
    }

    /** Get the name of an optimizable */
    private TableName    getOptimizableName( Optimizable optimizable )
    {
        try {
            if ( isBaseTable( optimizable ) )
            {
                ProjectRestrictNode prn = (ProjectRestrictNode) optimizable;
                TableDescriptor td = 
                    ((FromBaseTable) prn.getChildResult()).getTableDescriptor();
//IC see: https://issues.apache.org/jira/browse/DERBY-6211
                return makeTableName( td.getSchemaName(), td.getName(), _cm );
            }
//IC see: https://issues.apache.org/jira/browse/DERBY-6267
            else if ( OptimizerImpl.isTableFunction( optimizable ) )
            {
                ProjectRestrictNode prn = (ProjectRestrictNode) optimizable;
                AliasDescriptor ad =
                    ((StaticMethodCallNode) ((FromVTI) prn.getChildResult()).
                        getMethodCall() ).ad;
//IC see: https://issues.apache.org/jira/browse/DERBY-6211
                return makeTableName( ad.getSchemaName(), ad.getName(), _cm );
            }
            else if ( isFromTable( optimizable ) )
            {
                TableName   retval = ((FromTable) ((ProjectRestrictNode) optimizable).getChildResult()).getTableName();
//IC see: https://issues.apache.org/jira/browse/DERBY-6211

                if ( retval !=  null ) { return retval; }
            }
        }
        catch (StandardException e)
        {
            // Technically, an exception could occur here if the table name
            // was not previously bound and if an error occured while binding it.
            // But the optimizable should have been bound long before optimization,
            // so this should not be a problem.
        }

        String  nodeClass = optimizable.getClass().getName();
        String  unqualifiedName = nodeClass.substring( nodeClass.lastIndexOf( "." ) + 1 );

//IC see: https://issues.apache.org/jira/browse/DERBY-6211
        return makeTableName( null, unqualifiedName, _cm );
    }

    /** Return true if the optimizable is a base table */
    private boolean isBaseTable( Optimizable optimizable )
    {
        if ( !( optimizable instanceof ProjectRestrictNode ) ) { return false; }

        ResultSetNode   rsn = ((ProjectRestrictNode) optimizable).getChildResult();

        return ( rsn instanceof FromBaseTable );
    }

    /** Return true if the optimizable is a FromTable */
    private boolean isFromTable( Optimizable optimizable )
    {
        if ( !( optimizable instanceof ProjectRestrictNode ) ) { return false; }

        ResultSetNode   rsn = ((ProjectRestrictNode) optimizable).getChildResult();

        return ( rsn instanceof FromTable );
    }

    /** Make a TableName */
    private TableName   makeTableName(
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
            String schemaName, String unqualifiedName, ContextManager cm )
    {
        TableName result = new TableName(schemaName, unqualifiedName, cm);

        return result;
    }

    /** Print an exception to the log file */
    private void    printThrowable( Throwable t )
    {
        t.printStackTrace( Monitor.getStream().getPrintWriter() );
    }

    /** Create an element and add it to a parent */
    private Element createElement( Element parent, String tag, String content )
    {
        Element child = null;
        
        try {
            child = _doc.createElement( tag );
            if ( parent != null) { parent.appendChild( child ); }
            if ( content != null ) { child.setTextContent( content ); }
        }
        catch (Throwable t) { printThrowable( t ); }

        return child;
    }

    /** Turn a timestamp into a human-readable string */
    private String  formatTimestamp( long timestamp ) { return (new Date( timestamp )).toString(); }

    /** Create an element explaining that we're skipping some processing */
    private Element formatSkip( Element parent, String skipTag, String reason )
    {
        Element skip = createElement( parent, skipTag, null );
        skip.setAttribute( SKIP_REASON, reason );

        return skip;
    }
    
    /** Turn a CostEstimate for a join order into a human-readable element */
    private Element formatPlanCost( Element parent, String type, int[] planOrder, int planType, CostEstimate raw )
    {
        Element cost = createElement( parent, PC, null );

        cost.setAttribute( PC_TYPE, type );
        if ( isComplete( planOrder ) ) { cost.setAttribute( PC_COMPLETE, "true" ); }
        if ( planType == Optimizer.SORT_AVOIDANCE_PLAN ) { cost.setAttribute( PC_AVOID_SORT, "true" ); }

//IC see: https://issues.apache.org/jira/browse/DERBY-6211
        createElement( cost, PC_SUMMARY, formatPlanSummary( planOrder, planType ) );
        formatCost( cost, raw );

        return cost;
    }

    /** Return true if the join order has been completely filled in */
    private boolean isComplete( int[] joinOrder )
    {
        if ( joinOrder == null ) { return false; }
        if ( joinOrder.length < _currentQueryBlock.optimizableList.size() ) { return false; }
//IC see: https://issues.apache.org/jira/browse/DERBY-6211

        for ( int i = 0; i < joinOrder.length; i++ )
        {
            if ( joinOrder[ i ] < 0 ) { return false; }
        }

        return true;
    }

    /** Format a CostEstimate as subelements of a parent */
    private void    formatCost( Element costElement, CostEstimate raw )
    {
        createElement( costElement, CE_ESTIMATED_COST, Double.toString( raw.getEstimatedCost() ) );
        createElement( costElement, CE_ROW_COUNT, Long.toString( raw.getEstimatedRowCount() ) );
        createElement( costElement, CE_SINGLE_SCAN_ROW_COUNT, Double.toString( raw.singleScanRowCount() ) );
    }

    /** Format selectivity subelement */
    private void    formatSelectivity( Element parent, String tag, int count, double selectivity )
    {
        Element child = createElement( parent, tag, null );
        child.setAttribute( SEL_COUNT, Integer.toString( count ) );
        child.setAttribute( SEL_SELECTIVITY, Double.toString( selectivity ) );
    }

    /** Format a join order list */
    private void    formatJoinOrder( Element parent, int[] proposedJoinOrder )
    {
        if ( proposedJoinOrder != null )
        {
            for ( int idx = 0; idx < proposedJoinOrder.length; idx++ )
            {
                int     optimizableNumber = proposedJoinOrder[ idx ];
                if ( optimizableNumber >= 0 )
                {
//IC see: https://issues.apache.org/jira/browse/DERBY-6211
                    Optimizable optimizable = _currentQueryBlock.optimizableList.getOptimizable( optimizableNumber );
//IC see: https://issues.apache.org/jira/browse/DERBY-6211
                    createElement( parent, JO_SLOT, getOptimizableName( optimizable ).getFullSQLName() );
                }
            }
        }
    }


    /**
     * <p>
     * Produce a string representation of the plan being considered now.
     * The string has the following grammar:
     * </p>
     *
     * <pre>
     * join :== factor OP factor
     *
     * OP :== "*" | "#"
     *
     * factor :== factor | conglomerateName
     * </pre>
     */
    private String  formatPlanSummary( int[] planOrder, int planType )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6211
        try {
            OptimizerPlan   plan = null;
        
            StringBuilder   buffer = new StringBuilder();
            boolean     avoidSort = (planType == Optimizer.SORT_AVOIDANCE_PLAN);

            // a negative optimizable number indicates the end of the plan
            int planLength = 0;
            for ( ; planLength < planOrder.length; planLength++ )
            {
                if ( planOrder[ planLength ] < 0 ) { break; }
            }

            for ( int i = 0; i < planLength; i++ )
            {
                int     listIndex = planOrder[ i ];

//IC see: https://issues.apache.org/jira/browse/DERBY-6211
                if ( listIndex >= _currentQueryBlock.optimizableList.size() )
                {
                    // should never happen!
                    buffer.append( "{ UNKNOWN LIST INDEX " + listIndex + " } " );
                    continue;
                }

                Optimizable optimizable = _currentQueryBlock.optimizableList.getOptimizable( listIndex );
            
                AccessPath  ap = avoidSort ?
                    optimizable.getBestSortAvoidancePath() : optimizable.getBestAccessPath();
                JoinStrategy    js = ap.getJoinStrategy();
                UniqueTupleDescriptor   utd = OptimizerImpl.isTableFunction( optimizable ) ?
                    ((StaticMethodCallNode) ((FromVTI) ((ProjectRestrictNode) optimizable).getChildResult()).getMethodCall()).ad :
                    ap.getConglomerateDescriptor();

//IC see: https://issues.apache.org/jira/browse/DERBY-6211
                OptimizerPlan   current =   (utd == null) ?
                    new OptimizerPlan.DeadEnd( getOptimizableName( optimizable ).toString() ) :
                    OptimizerPlan.makeRowSource( utd, _lcc.getDataDictionary() );

                if ( plan != null )
                {
                    current = new OptimizerPlan.Join( js, plan, current );
                }

                plan = current;
            }

            return plan.toString();
        }
        catch (Exception e)
        {
            return e.getMessage();
        }
    }

}
