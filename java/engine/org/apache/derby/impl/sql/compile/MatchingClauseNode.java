/*

   Derby - Class org.apache.derby.impl.sql.compile.MatchingClauseNode

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

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.derby.catalog.types.DefaultInfoImpl;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.LocalField;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

/**
 * Node representing a WHEN MATCHED or WHEN NOT MATCHED clause
 * in a MERGE statement.
 *
 */

public class MatchingClauseNode extends QueryTreeNode
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String  CURRENT_OF_NODE_NAME = "$MERGE_CURRENT";

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    // filled in by the constructor
    private ValueNode           _matchingRefinement;
    private ResultColumnList    _updateColumns;
    private ResultColumnList    _insertColumns;
    private ResultColumnList    _insertValues;

    //
    // filled in at bind() time
    //

    /** the INSERT/UPDATE/DELETE statement of this WHEN [ NOT ] MATCHED clause */
    private DMLModStatementNode _dml;

    /** the columns in the temporary conglomerate which drives the INSERT/UPDATE/DELETE */
    private ResultColumnList        _thenColumns;
    private int[]                           _deleteColumnOffsets;

    // Filled in at generate() time
    private int                             _clauseNumber;
    private String                          _actionMethodName;
    private String                          _resultSetFieldName;
    private String                          _rowMakingMethodName;
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS/FACTORY METHODS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Constructor called by factory methods.
     */
    private MatchingClauseNode
        (
         ValueNode  matchingRefinement,
         ResultColumnList   updateColumns,
         ResultColumnList   insertColumns,
         ResultColumnList   insertValues,
         ContextManager     cm
         )
        throws StandardException
    {
        super( cm );
        
        _matchingRefinement = matchingRefinement;
        _updateColumns = updateColumns;
        _insertColumns = insertColumns;
        _insertValues = insertValues;
    }

    /** Make a WHEN MATCHED ... THEN UPDATE clause */
    static  MatchingClauseNode   makeUpdateClause
        (
         ValueNode  matchingRefinement,
         ResultColumnList   updateColumns,
         ContextManager     cm
         )
        throws StandardException
    {
        return new MatchingClauseNode( matchingRefinement, updateColumns, null, null, cm );
    }

    /** Make a WHEN MATCHED ... THEN DELETE clause */
    static  MatchingClauseNode   makeDeleteClause
        (
         ValueNode  matchingRefinement,
         ContextManager     cm
         )
        throws StandardException
    {
        return new MatchingClauseNode( matchingRefinement, null, null, null, cm );
    }

    /** Make a WHEN NOT MATCHED ... THEN INSERT clause */
    static  MatchingClauseNode   makeInsertClause
        (
         ValueNode  matchingRefinement,
         ResultColumnList   insertColumns,
         ResultColumnList   insertValues,
         ContextManager     cm
         )
        throws StandardException
    {
        return new MatchingClauseNode( matchingRefinement, null, insertColumns, insertValues, cm );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ACCESSORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Return true if this is a WHEN MATCHED ... UPDATE clause */
    boolean isUpdateClause()    { return (_updateColumns != null); }
    
    /** Return true if this is a WHEN NOT MATCHED ... INSERT clause */
    boolean isInsertClause()    { return (_insertValues != null); }
    
    /** Return true if this is a WHEN MATCHED ... DELETE clause */
    boolean isDeleteClause()    { return !( isUpdateClause() || isInsertClause() ); }

    /** Return the bound DML statement--returns null if called before binding */
    DMLModStatementNode getDML()    { return _dml; }

    /**
     * Return the list of columns which form the rows of the ResultSet which drive
     * the INSERT/UPDATE/DELETE actions.
     */
    ResultColumnList    getBufferedColumns() { return _thenColumns; }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // bind() BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Bind this WHEN [ NOT ] MATCHED clause against the parent JoinNode */
    void    bind
        (
         DataDictionary dd,
         MergeNode mergeNode,
         FromList fullFromList,
         FromBaseTable targetTable
         )
        throws StandardException
    {
        _thenColumns = new ResultColumnList( getContextManager() );

        if ( isDeleteClause() ) { bindDelete( dd, fullFromList, targetTable ); }
        if ( isUpdateClause() ) { bindUpdate( dd, fullFromList, targetTable ); }
        if ( isInsertClause() ) { bindInsert( dd, mergeNode, fullFromList, targetTable ); }
    }

    /** Bind the optional refinement condition in the MATCHED clause */
    void    bindRefinement( MergeNode mergeNode, FromList fullFromList ) throws StandardException
    {
        if ( _matchingRefinement != null )
        {
            mergeNode.bindExpression( _matchingRefinement, fullFromList );
        }
    }

    /** Re-bind various clauses and lists once we have ResultSet numbers for the driving left join */
    void    bindResultSetNumbers( MergeNode mergeNode, FromList fullFromList ) throws StandardException
    {
        bindRefinement( mergeNode, fullFromList );
    }

    /** Collect the columns mentioned by expressions in this MATCHED clause */
    void    getColumnsInExpressions
        (
         MergeNode  mergeNode,
         HashMap<String,ColumnReference> drivingColumnMap
         )
        throws StandardException
    {
        if ( _matchingRefinement != null )
        {
            mergeNode.getColumnsInExpression( drivingColumnMap, _matchingRefinement );
        }

        if ( isUpdateClause() )
        {
            // get all columns mentioned on the right side of SET operators in WHEN MATCHED ... THEN UPDATE clauses
            for ( ResultColumn rc : _updateColumns )
            {
                mergeNode.getColumnsInExpression( drivingColumnMap, rc.getExpression() );
            }
        }
        else if ( isInsertClause() )
        {
            // get all columns mentioned in the VALUES subclauses of WHEN NOT MATCHED ... THEN INSERT clauses
            for ( ResultColumn rc : _insertValues )
            {
                mergeNode.getColumnsInExpression( drivingColumnMap, rc.getExpression() );
            }
        }
    }
    
    ////////////////////// UPDATE ///////////////////////////////

    /** Bind a WHEN MATCHED ... THEN UPDATE clause */
    private void    bindUpdate
        (
         DataDictionary dd,
         FromList fullFromList,
         FromTable targetTable
         )
        throws StandardException
    {
        bindSetClauses( fullFromList, targetTable );
        
        SelectNode  selectNode = new SelectNode
            (
             _updateColumns,
             fullFromList,
             null,      // where clause
             null,      // group by list
             null,      // having clause
             null,      // window list
             null,      // optimizer plan override
             getContextManager()
             );
        _dml = new UpdateNode( targetTable.getTableName(), selectNode, this, getContextManager() );

        _dml.bindStatement();
    }
    
    /** Bind the SET clauses of an UPDATE action */
    private void    bindSetClauses
        (
         FromList fullFromList,
         FromTable targetTable
         )
        throws StandardException
    {
        // needed to make the UpdateNode bind
        _updateColumns.replaceOrForbidDefaults( targetTable.getTableDescriptor(), _updateColumns, true );

        bindExpressions( _updateColumns, fullFromList );
    }

    ////////////////////// DELETE ///////////////////////////////

    /** Bind a WHEN MATCHED ... THEN DELETE clause */
    private void    bindDelete
        (
         DataDictionary dd,
         FromList fullFromList,
         FromBaseTable targetTable
         )
        throws StandardException
    {
        CurrentOfNode   currentOfNode = CurrentOfNode.makeForMerge
            ( CURRENT_OF_NODE_NAME, targetTable, getContextManager() );
        FromList        fromList = new FromList( getContextManager() );
        fromList.addFromTable( currentOfNode );
        SelectNode      selectNode = new SelectNode
            (
             null,
             fromList, /* FROM list */
             null, /* WHERE clause */
             null, /* GROUP BY list */
             null, /* having clause */
             null, /* window list */
             null, /* optimizer plan override */
             getContextManager()
             );
        _dml = new DeleteNode( targetTable.getTableName(), selectNode, this, getContextManager() );

        _dml.bindStatement();

        buildThenColumnsForDelete();
    }

    /**
     * <p>
     * Construct the signature of the temporary table which drives the
     * INSERT/UPDATE/DELETE action.
     * </p>
     */
    private void    buildThenColumnsForDelete()
        throws StandardException
    {
        ResultColumnList    dmlSignature = _dml.resultSet.resultColumns;
        for ( int i = 0; i < dmlSignature.size(); i++ )
        {
            ResultColumn    origRC = dmlSignature.elementAt( i );
            ResultColumn    newRC;
            ValueNode       expression = origRC.getExpression();

            if ( expression instanceof ColumnReference )
            {
                ColumnReference cr = (ColumnReference) ((ColumnReference) expression).getClone();
                newRC = new ResultColumn( cr, cr, getContextManager() );
            }
            else
            {
                newRC = origRC.cloneMe();
            }
            _thenColumns.addResultColumn( newRC );
        }
    }

    /**
     * <p>
     * Calculate the 1-based offsets which define the rows which will be buffered up
     * for a DELETE action at run-time. The rows are constructed
     * from the columns in the SELECT list of the driving left joins. This method
     * calculates an array of offsets into the SELECT list. The columns at those
     * offsets will form the row which is buffered up for the DELETE
     * action.
     * </p>
     */
    void    bindDeleteThenColumns( ResultColumnList selectList )
        throws StandardException
    {
        int     bufferedCount = _thenColumns.size();
        int     selectCount = selectList.size();
        
        _deleteColumnOffsets = new int[ bufferedCount ];

        for ( int bidx = 0; bidx < bufferedCount; bidx++ )
        {
            ResultColumn    bufferedRC = _thenColumns.elementAt( bidx );
            ValueNode       bufferedExpression = bufferedRC.getExpression();

            _deleteColumnOffsets[ bidx ] = getSelectListOffset( selectList, bufferedExpression );
        }
    }

    ////////////////////// INSERT ///////////////////////////////

    /** Bind a WHEN NOT MATCHED ... THEN INSERT clause */
    private void    bindInsert
        (
         DataDictionary dd,
         MergeNode  mergeNode,
         FromList fullFromList,
         FromTable targetTable
         )
        throws StandardException
    {
        bindInsertValues( fullFromList, targetTable );
        
        // the VALUES clause may not mention columns in the target table
        FromList    targetTableFromList = new FromList( getOptimizerFactory().doJoinOrderOptimization(), getContextManager() );
        targetTableFromList.addElement( fullFromList.elementAt( 0 ) );
        bindExpressions( _insertValues, targetTableFromList );
        if ( _matchingRefinement != null )
        {
            mergeNode.bindExpression( _matchingRefinement, targetTableFromList );
        }
        
        SelectNode  selectNode = new SelectNode
            (
             _insertValues,      // select list
             fullFromList,
             null,      // where clause
             null,      // group by list
             null,      // having clause
             null,      // window list
             null,      // optimizer plan override
             getContextManager()
             );
        _dml = new InsertNode
            (
             targetTable.getTableName(),
             _insertColumns,
             selectNode,
             this,      // in NOT MATCHED clause
             null,      // targetProperties
             null,      // order by cols
             null,      // offset
             null,      // fetch first
             false,     // has JDBC limit clause
             getContextManager()
             );

        _dml.bindStatement();

        buildInsertThenColumns( targetTable );
    }

    /**  Bind the values in the INSERT list */
    private void    bindInsertValues
        (
         FromList fullFromList,
         FromTable targetTable
         )
        throws StandardException
    {
        if ( _insertColumns.size() != _insertValues.size() )
        {
            throw StandardException.newException( SQLState.LANG_DB2_INVALID_COLS_SPECIFIED ); 
        }
        
        TableDescriptor td = targetTable.getTableDescriptor();

        // forbid illegal values for identity columns
        for ( int i = 0; i <_insertValues.size(); i++ )
        {
            ResultColumn    rc = _insertValues.elementAt( i );
            String          columnName = _insertColumns.elementAt( i ).exposedName;
            ValueNode       expr = rc.getExpression();
            ColumnDescriptor    cd = td.getColumnDescriptor( columnName );

            // if the column isn't in the table, this will be sorted out when we bind
            // the InsertNode
            if ( cd == null ) { continue; }

            // DEFAULT is the only value allowed for a GENERATED ALWAYS AS IDENTITY column
            if ( cd.isAutoincAlways() && !(expr instanceof DefaultNode) )
            {
                throw StandardException.newException( SQLState.LANG_AI_CANNOT_MODIFY_AI, columnName );
            }

            // NULL is illegal as the value for any identity column
            if ( cd.isAutoincrement() && (expr instanceof UntypedNullConstantNode) )
            {
                throw StandardException.newException( SQLState.LANG_NULL_INTO_NON_NULL, columnName );
            }
        }
        
        // needed to make the SelectNode bind
        _insertValues.replaceOrForbidDefaults( targetTable.getTableDescriptor(), _insertColumns, true );
        bindExpressions( _insertValues, fullFromList );
    }
    
    /** Construct the row in the temporary table which drives an INSERT action */
    private void    buildInsertThenColumns( FromTable targetTable )
        throws StandardException
    {
        TableDescriptor td = targetTable.getTableDescriptor();

        _thenColumns = _dml.resultSet.resultColumns.copyListAndObjects();

        //
        // Here we set up for the evaluation of expressions in the temporary table
        // which drives the INSERT action. If we were actually generating the dummy SELECT
        // for the DML action, the work would normally be done there. But we don't generate
        // that SELECT. So we do the following here:
        //
        // 1) If a column has a value specified in the WHEN [ NOT ] MATCHED clause, then we use it.
        //     There is some special handling to make the DEFAULT value work for identity columns.
        //
        // 2) Otherwise, if the column has a default, then we plug it in.
        //
        for ( int i = 0; i < _thenColumns.size(); i++ )
        {
            ColumnDescriptor    cd = td.getColumnDescriptor( i + 1 );
            ResultColumn    origRC = _thenColumns.elementAt( i );
            String              columnName = origRC.getName();
            
            boolean         changed = false;

            //
            // VirtualColumnNodes are skipped at code-generation time. This can result in
            // NPEs when evaluating generation expressions. Replace VirtualColumnNodes with
            // UntypedNullConstantNodes, except for identity columns, which require special
            // handling below.
            //
            if ( !origRC.isAutoincrement() && (origRC.getExpression() instanceof VirtualColumnNode) )
            {
                origRC.setExpression( new UntypedNullConstantNode( getContextManager() ) );
            }

            for ( int ic = 0; ic < _insertColumns.size(); ic++ )
            {
                ResultColumn    icRC = _insertColumns.elementAt( ic );

                if ( columnName.equals( icRC.getName() ) )
                {
                    ResultColumn    newRC = null;
                    
                    // replace DEFAULT for a generated or identity column
                    ResultColumn    insertRC =_insertValues.elementAt( ic );

                    if ( insertRC.wasDefaultColumn() || (insertRC.getExpression() instanceof UntypedNullConstantNode ) )
                    {
                       if ( !cd.isAutoincrement() )
                        {
                            //
                            // Eliminate column references under identity columns. They
                            // will mess up the code generation.
                            //
                            ValueNode   expr = origRC.getExpression();
                            if ( expr instanceof ColumnReference )
                            {
                                origRC.setExpression( new UntypedNullConstantNode( getContextManager() ) );
                            }
                            continue;
                        }

                        ColumnReference autoGenCR = new ColumnReference( columnName, targetTable.getTableName(), getContextManager() );
                        ResultColumn    autoGenRC = new ResultColumn( autoGenCR, null, getContextManager() );
                        VirtualColumnNode autoGenVCN = new VirtualColumnNode( targetTable, autoGenRC, i + 1, getContextManager() );

                        newRC = new ResultColumn( autoGenCR, autoGenVCN, getContextManager() );

                        // set the type so that buildThenColumnSignature() will function correctly
                        newRC.setType( origRC.getTypeServices() );
                    }
                    else
                    {
                        newRC = insertRC.cloneMe();
                        newRC.setType( origRC.getTypeServices() );
                    }

                    newRC.setVirtualColumnId( origRC.getVirtualColumnId() );
                    _thenColumns.setElementAt( newRC, i  );
                    changed = true;
                    break;
                }
            }

            // plug in defaults if we haven't done so already
            if ( !changed )
            {
                DefaultInfoImpl     defaultInfo = (DefaultInfoImpl) cd.getDefaultInfo();

				if ( (defaultInfo != null) && !defaultInfo.isGeneratedColumn() && !cd.isAutoincrement() )
				{
                    _thenColumns.setDefault( origRC, cd, defaultInfo );
                    changed = true;
				}
            }

            // set the result column name correctly for buildThenColumnSignature()
            ResultColumn    finalRC = _thenColumns.elementAt( i );
            finalRC.setName( cd.getColumnName() );
        }   // end loop through _thenColumns
    }

    ////////////////////// bind() MINIONS ///////////////////////////////

    /** Boilerplate for binding a list of ResultColumns against a FromList */
    private void bindExpressions( ResultColumnList rcl, FromList fromList )
        throws StandardException
    {
        CompilerContext cc = getCompilerContext();
        final int previousReliability = cc.getReliability();
        
        try {
            cc.setReliability( previousReliability | CompilerContext.SQL_IN_ROUTINES_ILLEGAL );
            
            rcl.bindExpressions
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

    /**
     * <p>
     * Bind the row which will go into the temporary table at run-time.
     * </p>
     */
    void    bindThenColumns( ResultColumnList selectList )
        throws StandardException
    {
        if ( isDeleteClause() ) { bindDeleteThenColumns( selectList ); }
        else if ( isUpdateClause() )
        {
            throw StandardException.newException( SQLState.NOT_IMPLEMENTED, "MERGE" );
        }
    }

    /**
     * <p>
     * Find a column reference in the SELECT list of the driving left join
     * and return its 1-based offset into that list.  Returns -1 if the column
     * can't be found.
     * </p>
     */
    private int getSelectListOffset( ResultColumnList selectList, ValueNode bufferedExpression )
        throws StandardException
    {
        int                 selectCount = selectList.size();

        if ( bufferedExpression instanceof ColumnReference )
        {
            ColumnReference bufferedCR = (ColumnReference) bufferedExpression;
            String              tableName = bufferedCR.getTableName();
            String              columnName = bufferedCR.getColumnName();

            // loop through the SELECT list to find this column reference
            for ( int sidx = 0; sidx < selectCount; sidx++ )
            {
                ResultColumn    selectRC = selectList.elementAt( sidx );
                ValueNode       selectExpression = selectRC.getExpression();
                ColumnReference selectCR = selectExpression instanceof ColumnReference ?
                    (ColumnReference) selectExpression : null;

                if ( selectCR != null )
                {
                    if (
                        tableName.equals( selectCR.getTableName() ) &&
                        columnName.equals( selectCR.getColumnName() )
                        )
                    {
                        return sidx + 1;
                    }
                }
            }
        }
        else if ( bufferedExpression instanceof CurrentRowLocationNode )
        {
            //
            // There is only one RowLocation in the SELECT list, the row location for the
            // tuple from the target table. The RowLocation is always the last column in
            // the SELECT list.
            //
            return selectCount;
        }

        return -1;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // optimize() BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Optimize the INSERT/UPDATE/DELETE action.
     * </p>
     */
    void    optimize()  throws StandardException
    {
        _dml.optimizeStatement();
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // generate() BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ConstantAction makeConstantAction( ActivationClassBuilder acb )
        throws StandardException
	{
        // generate the clause-specific refinement
        String  refinementName = null;
        if ( _matchingRefinement != null )
        {
            MethodBuilder userExprFun = acb.newUserExprFun();

            _matchingRefinement.generateExpression( acb, userExprFun );
            userExprFun.methodReturn();
		
            // we are done modifying userExprFun, complete it.
            userExprFun.complete();

            refinementName = userExprFun.getName();
        }
        
        return	getGenericConstantActionFactory().getMatchingClauseConstantAction
            (
             getClauseType(),
             refinementName,
             buildThenColumnSignature(),
             _rowMakingMethodName,
             _deleteColumnOffsets,
             _resultSetFieldName,
             _actionMethodName,
             _dml.makeConstantAction()
             );
	}
    private int getClauseType()
    {
        if ( isUpdateClause() ) { return ConstantAction.WHEN_MATCHED_THEN_UPDATE; }
        else if ( isInsertClause() ) { return ConstantAction.WHEN_NOT_MATCHED_THEN_INSERT; }
        else { return ConstantAction.WHEN_MATCHED_THEN_DELETE; }
    }

    /**
     * <p>
     * Build the signature of the row which will go into the temporary table.
     * </p>
     */
    private ResultDescription    buildThenColumnSignature()
        throws StandardException
    {
        ResultColumnDescriptor[]  cells = _thenColumns.makeResultDescriptors();

        return getLanguageConnectionContext().getLanguageFactory().getResultDescription( cells, "MERGE" );
    }

    /**
     * <p>
     * Generate a method to invoke the INSERT/UPDATE/DELETE action. This method
     * will be called at runtime by MatchingClauseConstantAction.executeConstantAction().
     * </p>
     */
    void    generate
        (
         ActivationClassBuilder acb,
         ResultColumnList selectList,
         HalfOuterJoinNode  hojn,
         int    clauseNumber
         )
        throws StandardException
    {
        _clauseNumber = clauseNumber;

        if ( isInsertClause() ) { generateInsertRow( acb, selectList, hojn ); }
        
        _actionMethodName = "mergeActionMethod_" + _clauseNumber;
        
        MethodBuilder mb = acb.getClassBuilder().newMethodBuilder
            (
             Modifier.PUBLIC,
             ClassName.ResultSet,
             _actionMethodName
             );
        mb.addThrownException(ClassName.StandardException);

        remapConstraints();

        // now generate the action into this method
        _dml.generate( acb, mb );
        
        mb.methodReturn();
        mb.complete();
    }

    /**
     * <p>
     * Re-map ColumnReferences in constraints to point into the row from the
     * temporary table. This is where the row will be stored when constraints
     * are being evaluated.
     * </p>
     */
    private void    remapConstraints()
        throws StandardException
    {
        if( !isInsertClause() ) { return; }
        else
        {
            CollectNodesVisitor<ColumnReference> getCRs =
                new CollectNodesVisitor<ColumnReference>(ColumnReference.class);

            ValueNode   checkConstraints = ((InsertNode) _dml).checkConstraints;

            if ( checkConstraints != null )
            {
                checkConstraints.accept(getCRs);
                List<ColumnReference> colRefs = getCRs.getList();
                for ( ColumnReference cr : colRefs )
                {
                    cr.getSource().setResultSetNumber( NoPutResultSet.TEMPORARY_RESULT_SET_NUMBER );
                }
            }            
        }
    }

    /**
     * <p>
     * Adds a field to the generated class to hold the ResultSet of buffered rows
     * which drive the INSERT/UPDATE/DELETE action. Generates code to push
     * the contents of that field onto the stack.
     * </p>
     */
    void    generateResultSetField( ActivationClassBuilder acb, MethodBuilder mb )
        throws StandardException
    {
        _resultSetFieldName = "mergeResultSetField_" + _clauseNumber;
        
        // make the field public so we can stuff it at execution time
        LocalField  resultSetField = acb.newFieldDeclaration( Modifier.PUBLIC, ClassName.NoPutResultSet, _resultSetFieldName );

        //
        // At runtime, MatchingClauseConstantAction.executeConstantAction()
        // will stuff the resultSetField with the temporary table which collects
        // the rows relevant to this action. We want to push the value of resultSetField
        // onto the stack, where it will be the ResultSet argument to the constructor
        // of the actual INSERT/UPDATE/DELETE action.
        //
        mb.getField( resultSetField );
    }
    
    /**
     * <p>
     * Generate a method to build a row for the temporary table for INSERT actions.
     * The method stuffs each column in the row with the result of the corresponding
     * expression built out of columns in the current row of the driving left join.
     * The method returns the stuffed row.
     * </p>
     */
    void    generateInsertRow
        (
         ActivationClassBuilder acb,
         ResultColumnList selectList,
         HalfOuterJoinNode  hojn
         )
        throws StandardException
    {
        // point expressions in the temporary row at the columns in the
        // result column list of the driving left join.
        adjustThenColumns( selectList, hojn );
        
        _rowMakingMethodName = "mergeRowMakingMethod_" + _clauseNumber;
        
        MethodBuilder mb = acb.getClassBuilder().newMethodBuilder
            (
             Modifier.PUBLIC,
             ClassName.ExecRow,
             _rowMakingMethodName
             );
        mb.addThrownException(ClassName.StandardException);

        _thenColumns.generateEvaluatedRow( acb, mb, false, true );
    }

    /**
     * <p>
     * Point the column references in the temporary row at corresponding
     * columns returned by the driving left join.
     * </p>
     */
    void    adjustThenColumns
        (
         ResultColumnList selectList,
         HalfOuterJoinNode  hojn
         )
        throws StandardException
    {
        ResultColumnList    leftJoinResult = hojn.resultColumns;
        CollectNodesVisitor<ColumnReference> getCRs =
            new CollectNodesVisitor<ColumnReference>( ColumnReference.class );
        _thenColumns.accept( getCRs );

        for ( ColumnReference cr : getCRs.getList() )
        {
            ResultColumn    leftJoinRC = leftJoinResult.elementAt( getSelectListOffset( selectList, cr ) - 1 );
            cr.setSource( leftJoinRC );
        }
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
		super.acceptChildren( v );

        if ( _matchingRefinement != null ) { _matchingRefinement.accept( v ); }
        if ( _updateColumns != null ) { _updateColumns.accept( v ); }
        if ( _insertColumns != null ) { _insertColumns.accept( v ); }
        if ( _insertValues != null ) { _insertValues.accept( v ); }

        if ( _dml != null ) { _dml.accept( v ); }
	}

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */
    @Override
	public String toString()
	{
        if ( isUpdateClause() ) { return "UPDATE"; }
        else if ( isInsertClause() ) { return "INSERT"; }
        else { return "DELETE"; }
	}

}
