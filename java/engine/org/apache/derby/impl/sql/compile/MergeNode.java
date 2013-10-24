/*

   Derby - Class org.apache.derby.impl.sql.compile.MergeNode

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

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptorList;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.util.IdUtil;

/**
 * <p>
 * A MergeNode represents a MERGE statement. The statement looks like
 * this...
 * </p>
 *
 * <pre>
 * MERGE INTO targetTable
 * USING sourceTable
 * ON searchCondition
 * matchingClause1 ... matchingClauseN
 * </pre>
 *
 * <p>
 * ...where each matching clause looks like this...
 * </p>
 *
 * <pre>
 * WHEN MATCHED [ AND matchingRefinement ] THEN DELETE
 * </pre>
 *
 * <p>
 * ...or
 * </p>
 *
 * <pre>
 * WHEN MATCHED [ AND matchingRefinement ] THEN UPDATE SET col1 = expr1, ... colM = exprM
 * </pre>
 *
 * <p>
 * ...or
 * </p>
 *
 * <pre>
 * WHEN NOT MATCHED [ AND matchingRefinement ] THEN INSERT columnList VALUES valueList
 * </pre>
 *
 * <p>
 * The Derby compiler essentially rewrites this statement into a driving left join
 * followed by a series of DELETE/UPDATE/INSERT actions. The left join looks like
 * this:
 * </p>
 *
 * <pre>
 * SELECT selectList FROM sourceTable LEFT OUTER JOIN targetTable ON searchCondition
 * </pre>
 *
 * <p>
 * The selectList of the driving left join consists of the following:
 * </p>
 *
 * <ul>
 * <li>All of the columns mentioned in the searchCondition.</li>
 * <li>All of the columns mentioned in the matchingRefinement clauses.</li>
 * <li>All of the columns mentioned in the SET clauses and the INSERT columnLists and valueLists.</li>
 * <li>All additional columns needed for the triggers and foreign keys fired by the DeleteResultSets
 * and UpdateResultSets constructed for the WHEN MATCHED clauses.</li>
 * <li>All additional columns needed to build index rows and evaluate generated columns
 * needed by the UpdateResultSets constructed for the WHEN MATCHED...THEN UPDATE clauses.</li>
 * <li>A trailing targetTable.RowLocation column.</li>
 * </ul>
 *
 * <p>
 * The matchingRefinement expressions are bound and generated against the
 * FromList of the driving left join. Dummy DeleteNode, UpdateNode, and InsertNode
 * statements are independently constructed in order to bind and generate the DELETE/UPDATE/INSERT
 * actions.
 * </p>
 *
 * <p>
 * At execution time, the targetTable.RowLocation column is used to determine
 * whether a given driving row matches. The row matches iff targetTable.RowLocation is not null.
 * The driving row is then assigned to the
 * first DELETE/UPDATE/INSERT action to which it applies. The relevant columns from
 * the driving row are extracted and buffered in a temporary table specific to that
 * DELETE/UPDATE/INSERT action. After the driving left join has been processed,
 * the DELETE/UPDATE/INSERT actions are run in order, each taking its corresponding
 * temporary table as its source ResultSet.
 * </p>
 */

public final class MergeNode extends DMLModStatementNode
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   int SOURCE_TABLE_INDEX = 0;
    private static  final   int TARGET_TABLE_INDEX = 1;

	private static final String TARGET_ROW_LOCATION_NAME = "###TargetRowLocation";

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    // constructor args
    private FromBaseTable   _targetTable;
    private FromTable   _sourceTable;
    private ValueNode   _searchCondition;
    private ArrayList<MatchingClauseNode>   _matchingClauses;

    // filled in at bind() time
    private FromList                _leftJoinFromList;

    // filled in at generate() time
    private ConstantAction      _constantAction;
    private CursorNode          _leftJoinCursor;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Constructor for a MergeNode.
     * </p>
     */
    public  MergeNode
        (
         FromTable          targetTable,
         FromTable          sourceTable,
         ValueNode          searchCondition,
         ArrayList<MatchingClauseNode>  matchingClauses,
         ContextManager     cm
         )
        throws StandardException
    {
        super( null, null, cm );

        if ( !( targetTable instanceof FromBaseTable) ) { notBaseTable(); }
        else { _targetTable = (FromBaseTable) targetTable; }
        
        _sourceTable = sourceTable;
        _searchCondition = searchCondition;
        _matchingClauses = matchingClauses;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // bind() BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    @Override
	public void bindStatement() throws StandardException
	{
        DataDictionary  dd = getDataDictionary();

        FromList    dummyFromList = new FromList( getContextManager() );
        FromBaseTable   dummyTargetTable = new FromBaseTable
            (
             _targetTable.tableName,
             _targetTable.correlationName,
             null,
             null,
             getContextManager()
             );
        FromTable       dummySourceTable = cloneSourceTable();
        
        // source and target may not have the same correlation names
        if ( getExposedName( dummyTargetTable ).equals( getExposedName( dummySourceTable ) ) )
        {
            throw StandardException.newException( SQLState.LANG_SAME_EXPOSED_NAME );
        }

        dummyFromList.addFromTable( dummySourceTable );
        dummyFromList.addFromTable( dummyTargetTable );
        dummyFromList.bindTables( dd, new FromList( getOptimizerFactory().doJoinOrderOptimization(), getContextManager() ) );
        
        if ( !targetIsBaseTable( dummyTargetTable ) ) { notBaseTable(); }

        for ( MatchingClauseNode mcn : _matchingClauses )
        {
            mcn.bind( dd, this, dummyFromList, dummyTargetTable );
        }
        
        bindLeftJoin( dd );

        // re-bind the matchingRefinement clauses now that we have result set numbers
        // from the driving left join.
        for ( MatchingClauseNode mcn : _matchingClauses )
        {
            mcn.bindRefinement( this, _leftJoinFromList );
        }
        
        for ( MatchingClauseNode mcn : _matchingClauses )
        {
            if ( mcn.isUpdateClause() || mcn.isInsertClause() )
            {
                throw StandardException.newException( SQLState.NOT_IMPLEMENTED, "MERGE" );
            }
        }
	}

    /** Get the exposed name of a FromTable */
    private String  getExposedName( FromTable ft ) throws StandardException
    {
        return ft.getTableName().getTableName();
    }

    /**
     * Bind the driving left join select.
     * Stuffs the left join SelectNode into the resultSet variable.
     */
    private void    bindLeftJoin( DataDictionary dd )   throws StandardException
    {
        CompilerContext cc = getCompilerContext();
        final int previousReliability = cc.getReliability();
        
        try {
            cc.setReliability( previousReliability | CompilerContext.SQL_IN_ROUTINES_ILLEGAL );
            
            HalfOuterJoinNode   hojn = new HalfOuterJoinNode
                (
                 _sourceTable,
                 _targetTable,
                 _searchCondition,
                 null,
                 false,
                 null,
                 getContextManager()
                 );

            _leftJoinFromList = hojn.makeFromList( true, true );
            _leftJoinFromList.bindTables( dd, new FromList( getOptimizerFactory().doJoinOrderOptimization(), getContextManager() ) );

            if ( !sourceIsBase_View_or_VTI() )
            {
                throw StandardException.newException( SQLState.LANG_SOURCE_NOT_BASE_VIEW_OR_VTI );
            }

            FromList    topFromList = new FromList( getOptimizerFactory().doJoinOrderOptimization(), getContextManager() );
            topFromList.addFromTable( hojn );

            // preliminary binding of the matching clauses to resolve column
            // referneces. this ensures that we can add all of the columns from
            // the matching refinements to the SELECT list of the left join.
            // we re-bind the matching clauses when we're done binding the left join
            // because, at that time, we have result set numbers needed for
            // code generation.
            for ( MatchingClauseNode mcn : _matchingClauses )
            {
                mcn.bindRefinement( this, _leftJoinFromList );
            }
        
            ResultColumnList    selectList = buildSelectList();

            // calculate the offsets into the SELECT list which define the rows for
            // the WHEN [ NOT ] MATCHED  actions
            for ( MatchingClauseNode mcn : _matchingClauses )
            {
                mcn.bindThenColumns( selectList );
            }            
            
            resultSet = new SelectNode
                (
                 selectList,
                 topFromList,
                 null,      // where clause
                 null,      // group by list
                 null,      // having clause
                 null,      // window list
                 null,      // optimizer plan override
                 getContextManager()
                 );

            // Wrap the SELECT in a CursorNode in order to finish binding it.
            _leftJoinCursor = new CursorNode
                (
                 "SELECT",
                 resultSet,
                 null,
                 null,
                 null,
                 null,
                 false,
                 CursorNode.READ_ONLY,
                 null,
                 getContextManager()
                 );
            _leftJoinCursor.bindStatement();
        }
        finally
        {
            // Restore previous compiler state
            cc.setReliability( previousReliability );
        }
    }

    /** Throw a "not base table" exception */
    private void    notBaseTable()  throws StandardException
    {
        throw StandardException.newException( SQLState.LANG_TARGET_NOT_BASE_TABLE );
    }

    /** Build the select list for the left join */
    private ResultColumnList    buildSelectList() throws StandardException
    {
        HashMap<String,ColumnReference> drivingColumnMap = new HashMap<String,ColumnReference>();
        getColumnsInExpression( drivingColumnMap, _searchCondition );
        
        for ( MatchingClauseNode mcn : _matchingClauses )
        {
            mcn.getColumnsInExpressions( this, drivingColumnMap );
            getColumnsFromList( drivingColumnMap, mcn.getBufferedColumns() );
        }

        ResultColumnList    selectList = new ResultColumnList( getContextManager() );

        // add all of the columns from the source table which are mentioned
        addColumns( (FromTable) _leftJoinFromList.elementAt( SOURCE_TABLE_INDEX ), drivingColumnMap, selectList );
        // add all of the columns from the target table which are mentioned
        addColumns( (FromTable) _leftJoinFromList.elementAt( TARGET_TABLE_INDEX ), drivingColumnMap, selectList );

        addTargetRowLocation( selectList );

        return selectList;
    }

    /** Add the target table's row location to the left join's select list */
    private void    addTargetRowLocation( ResultColumnList selectList )
        throws StandardException
    {
        // tell the target table to generate a row location column
        _targetTable.setRowLocationColumnName( TARGET_ROW_LOCATION_NAME );
        
        TableName   fromTableName = _targetTable.getTableName();
        ColumnReference cr = new ColumnReference
                ( TARGET_ROW_LOCATION_NAME, fromTableName, getContextManager() );
        ResultColumn    rowLocationColumn = new ResultColumn( (String) null, cr, getContextManager() );
        rowLocationColumn.markGenerated();

        selectList.addResultColumn( rowLocationColumn );
    }

    /** Return true if the target table is a base table */
    private boolean targetIsBaseTable( FromBaseTable targetTable ) throws StandardException
    {
        FromBaseTable   fbt = targetTable;
        TableDescriptor desc = fbt.getTableDescriptor();
        if ( desc == null ) { return false; }

        return ( desc.getTableType() == TableDescriptor.BASE_TABLE_TYPE );
    }

    /** Return true if the source table is a base table, view, or table function */
    private boolean sourceIsBase_View_or_VTI() throws StandardException
    {
        if ( _sourceTable instanceof FromVTI ) { return true; }
        if ( !( _sourceTable instanceof FromBaseTable) ) { return false; }

        FromBaseTable   fbt = (FromBaseTable) _sourceTable;
        TableDescriptor desc = fbt.getTableDescriptor();
        if ( desc == null ) { return false; }

        switch( desc.getTableType() )
        {
        case TableDescriptor.BASE_TABLE_TYPE:
        case TableDescriptor.VIEW_TYPE:
            return true;

        default:
            return false;
        }
    }

    /** Clone the source table for binding the MATCHED clauses */
    private FromTable   cloneSourceTable() throws StandardException
    {
        if ( _sourceTable instanceof FromVTI )
        {
            FromVTI source = (FromVTI) _sourceTable;

            return new FromVTI
                (
                 source.methodCall,
                 source.correlationName,
                 source.resultColumns,
                 null,
                 source.exposedName,
                 getContextManager()
                 );
        }
        else if ( _sourceTable instanceof FromBaseTable )
        {
            FromBaseTable   source = (FromBaseTable) _sourceTable;
            return new FromBaseTable
                (
                 source.tableName,
                 source.correlationName,
                 null,
                 null,
                 getContextManager()
                 );
        }
        else
        {
            throw StandardException.newException( SQLState.LANG_SOURCE_NOT_BASE_VIEW_OR_VTI );
        }
    }

    /**
     * <p>
     * Add to an evolving select list the columns from the indicated table.
     * </p>
     */
    private void    addColumns
        (
         FromTable  fromTable,
         HashMap<String,ColumnReference> drivingColumnMap,
         ResultColumnList   selectList
         )
        throws StandardException
    {
        String[]    columnNames = getColumns( getExposedName( fromTable ), drivingColumnMap );
        
        for ( int i = 0; i < columnNames.length; i++ )
        {
            ColumnReference cr = new ColumnReference
                ( columnNames[ i ], fromTable.getTableName(), getContextManager() );
            ResultColumn    rc = new ResultColumn( (String) null, cr, getContextManager() );
            selectList.addResultColumn( rc );
        }
    }

    /** Get the column names from the table with the given table number, in sorted order */
    private String[]    getColumns( String exposedName, HashMap<String,ColumnReference> map )
    {
        ArrayList<String>   list = new ArrayList<String>();

        for ( ColumnReference cr : map.values() )
        {
            if ( exposedName.equals( cr.getTableName() ) ) { list.add( cr.getColumnName() ); }
        }

        String[]    retval = new String[ list.size() ];
        list.toArray( retval );
        Arrays.sort( retval );

        return retval;
    }
    
    /** Add the columns in the matchingRefinement clause to the evolving map */
    void    getColumnsInExpression
        ( HashMap<String,ColumnReference> map, ValueNode expression )
        throws StandardException
    {
        if ( expression == null ) { return; }

        CollectNodesVisitor<ColumnReference> getCRs =
            new CollectNodesVisitor<ColumnReference>(ColumnReference.class);

        expression.accept(getCRs);
        List<ColumnReference> colRefs = getCRs.getList();

        getColumnsFromList( map, colRefs );
    }

    /** Add a list of columns to the the evolving map */
    private void    getColumnsFromList
        ( HashMap<String,ColumnReference> map, ResultColumnList rcl )
        throws StandardException
    {
        ArrayList<ColumnReference>  colRefs = new ArrayList<ColumnReference>();

        for ( int i = 0; i < rcl.size(); i++ )
        {
            ResultColumn    rc = rcl.elementAt( i );
            ColumnReference cr = rc.getReference();
            if ( cr != null ) { colRefs.add( cr ); }
        }

        getColumnsFromList( map, colRefs );
    }
    
    /** Add a list of columns to the the evolving map */
    private void    getColumnsFromList
        ( HashMap<String,ColumnReference> map, List<ColumnReference> colRefs )
        throws StandardException
    {
        for ( ColumnReference cr : colRefs )
        {
            if ( cr.getTableName() == null )
            {
                ResultColumn    rc = _leftJoinFromList.bindColumnReference( cr );
                TableName       tableName = new TableName( null, rc.getTableName(), getContextManager() );
                cr = new ColumnReference( cr.getColumnName(), tableName, getContextManager() );
            }

            String  key = makeDCMKey( cr.getTableName(), cr.getColumnName() );
            if ( map.get( key ) == null )
            {
                map.put( key, cr );
            }
        }
    }

    /** Make a HashMap key for a column in the driving column map of the LEFT JOIN */
    private String  makeDCMKey( String tableName, String columnName )
    {
        return IdUtil.mkQualifiedName( tableName, columnName );
    }

    /** Boilerplate for binding an expression against a FromList */
    void bindExpression( ValueNode value, FromList fromList )
        throws StandardException
    {
        CompilerContext cc = getCompilerContext();
        final int previousReliability = cc.getReliability();
        
        try {
            cc.setReliability( previousReliability | CompilerContext.SQL_IN_ROUTINES_ILLEGAL );
            
            value.bindExpression
                (
                 fromList,
                 new SubqueryList( getContextManager() ),
                 new ArrayList<AggregateNode>()
                 );
        }
        finally
        {
            // Restore previous compiler state
            cc.setReliability( previousReliability );
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // optimize() BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    @Override
	public void optimizeStatement() throws StandardException
	{
		/* First optimize the left join */
		_leftJoinCursor.optimizeStatement();

		/* In language we always set it to row lock, it's up to store to
		 * upgrade it to table lock.  This makes sense for the default read
		 * committed isolation level and update lock.  For more detail, see
		 * Beetle 4133.
		 */
		//lockMode = TransactionController.MODE_RECORD;

        // now optimize the INSERT/UPDATE/DELETE actions
        for ( MatchingClauseNode mcn : _matchingClauses )
        {
            mcn.optimize();
        }
	}
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // generate() BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    @Override
    void generate( ActivationClassBuilder acb, MethodBuilder mb )
							throws StandardException
	{
        int     clauseCount = _matchingClauses.size();

		/* generate the parameters */
		generateParameterValueSet(acb);

        acb.pushGetResultSetFactoryExpression( mb );

        // arg 1: the driving left join 
        _leftJoinCursor.generate( acb, mb );

        ConstantAction[]    clauseActions = new ConstantAction[ clauseCount ];
        for ( int i = 0; i < clauseCount; i++ )
        {
            MatchingClauseNode  mcn = _matchingClauses.get( i );

            mcn.generate( acb, i );
            clauseActions[ i ] = mcn.makeConstantAction( acb );
        }
        _constantAction = getGenericConstantActionFactory().getMergeConstantAction( clauseActions );
        
        mb.callMethod
            ( VMOpcode.INVOKEINTERFACE, (String) null, "getMergeResultSet", ClassName.ResultSet, 1 );
	}
    
    @Override
    public ConstantAction makeConstantAction() throws StandardException
	{
		return _constantAction;
	}

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // Visitable BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

	/**
	 * Accept the visitor for all visitable children of this node.
	 * 
	 * @param v the visitor
	 *
	 * @exception StandardException on error
	 */
    @Override
	void acceptChildren(Visitor v)
		throws StandardException
	{
        if ( _leftJoinCursor != null )
        {
            _leftJoinCursor.acceptChildren( v );
        }
        else
        {
            super.acceptChildren( v );

            _targetTable.accept( v );
            _sourceTable.accept( v );
            _searchCondition.accept( v );
        }
        
        for ( MatchingClauseNode mcn : _matchingClauses )
        {
            mcn.accept( v );
        }
	}

    @Override
    String statementToString()
	{
		return "MERGE";
	}
}
