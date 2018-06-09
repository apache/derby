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
import java.util.HashSet;
import java.util.List;
import org.apache.derby.catalog.types.DefaultInfoImpl;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.ClassName;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.LocalField;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.IgnoreFilter;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptorList;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;

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

    //
    // Filled in by the constructor.
    //
    private ValueNode           _matchingRefinement;
    private ResultColumnList    _updateColumns;
    private ResultColumnList    _insertColumns;
    private ResultColumnList    _insertValues;

    //
    // Filled in at bind() time.
    //

    // the INSERT/UPDATE/DELETE statement of this WHEN [ NOT ] MATCHED clause
    private DMLModStatementNode _dml;

    // the columns in the temporary conglomerate which drives the INSERT/UPDATE/DELETE
    private ResultColumnList        _thenColumns;

    //
    // Filled in at generate() time.
    //
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

    /**
     * Return the list of columns which form the rows of the ResultSet which drive
     * the INSERT/UPDATE/DELETE actions.
     */
    ResultColumnList    getThenColumns() { return _thenColumns; }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // bind() BEHAVIOR CALLED BY MergeNode
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Bind this WHEN [ NOT ] MATCHED clause against the parent MergeNode */
    void    bind
        (
         DataDictionary dd,
         MergeNode mergeNode,
         FromList fullFromList,
         FromBaseTable targetTable
         )
        throws StandardException
    {
        //
        // Although the SQL Standard allows subqueries in the WHEN [ NOT ] MATCHED clauses,
        // we don't support them yet. That is because code-generation for those clauses breaks
        // if they contain subqueries. That, in turn, is because we don't completely optimize those
        // clauses. If we improve Derby so that we do completely optimize the WHEN [ NOT ] MATCHED clauses,
        // then we can consider enabling subqueries in them.
        //
        forbidSubqueries();

        _thenColumns = new ResultColumnList( getContextManager() );

        if ( isDeleteClause() ) { bindDelete( dd, fullFromList, targetTable ); }
        if ( isUpdateClause() ) { bindUpdate( dd, mergeNode, fullFromList, targetTable ); }
        if ( isInsertClause() ) { bindInsert( dd, mergeNode, fullFromList, targetTable ); }
    }

    /** Bind the optional refinement condition in the MATCHED clause */
    void    bindRefinement( MergeNode mergeNode, FromList fullFromList ) throws StandardException
    {
        if ( _matchingRefinement != null )
        {
            FromList    fromList = fullFromList;

            //
            // For an INSERT action, the WHEN NOT MATCHED refinement can only
            // mention columns in the source table.
            //
            if ( isInsertClause() )
            {
                fromList = new FromList( getOptimizerFactory().doJoinOrderOptimization(), getContextManager() );
                fromList.addElement( fullFromList.elementAt( MergeNode.SOURCE_TABLE_INDEX ) );
            }

            mergeNode.bindExpression( _matchingRefinement, fromList );
        }
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
            mergeNode.getColumnsInExpression( drivingColumnMap, _matchingRefinement, ColumnReference.MERGE_UNKNOWN );
        }

        if ( isUpdateClause() )
        {
            TableName   targetTableName = mergeNode.getTargetTable().getTableName();

            //
            // Get all columns mentioned on both sides of SET operators in WHEN MATCHED ... THEN UPDATE clauses.
            // We need the left side because UPDATE needs before and after images of columns.
            // We need the right side because there may be columns in the expressions there.
            //
            for ( ResultColumn rc : _updateColumns )
            {
                mergeNode.getColumnsInExpression( drivingColumnMap, rc.getExpression(), ColumnReference.MERGE_UNKNOWN );

                ColumnReference leftCR = new ColumnReference( rc.getName(), targetTableName, getContextManager() );
                mergeNode.addColumn( drivingColumnMap, leftCR, ColumnReference.MERGE_TARGET );
            }
        }
        else if ( isInsertClause() )
        {
            // get all columns mentioned in the VALUES subclauses of WHEN NOT MATCHED ... THEN INSERT clauses
            for ( ResultColumn rc : _insertValues )
            {
                mergeNode.getColumnsInExpression( drivingColumnMap, rc.getExpression(), ColumnReference.MERGE_UNKNOWN );
            }
        }
        else if ( isDeleteClause() )
        {
            // add all of the THEN columns
            mergeNode.getColumnsFromList( drivingColumnMap, _thenColumns, ColumnReference.MERGE_TARGET );
        }
    }
    
    ////////////////
    //
    // BIND UPDATE
    //
    ////////////////

    /** Bind a WHEN MATCHED ... THEN UPDATE clause */
    private void    bindUpdate
        (
         DataDictionary dd,
         MergeNode  mergeNode,
         FromList fullFromList,
         FromBaseTable targetTable
         )
        throws StandardException
    {
        ResultColumnList    setClauses = realiasSetClauses( targetTable );
        bindSetClauses( mergeNode, fullFromList, targetTable, setClauses );

        TableName   tableName = targetTable.getTableNameField();
        FromList    selectFromList = fullFromList;

        SelectNode  selectNode = new SelectNode
            (
             setClauses,
             selectFromList,
             null,      // where clause
             null,      // group by list
             null,      // having clause
             null,      // window list
             null,      // optimizer plan override
             getContextManager()
             );
        _dml = new UpdateNode( tableName, selectNode, this, getContextManager() );

        _dml.bindStatement();

        //
        // Don't add USAGE privilege on user-defined types.
        //
        boolean wasSkippingTypePrivileges = getCompilerContext().skipTypePrivileges( true );
            
        //
        // Split the update row into its before and after images.
        //
        ResultColumnList    beforeColumns = new ResultColumnList( getContextManager() );
        ResultColumnList    afterColumns = new ResultColumnList( getContextManager() );
        ResultColumnList    fullUpdateRow = getBoundSelectUnderUpdate().getResultColumns();
        
        // the full row is the before image, the after image, and a row location
        int     rowSize = fullUpdateRow.size() / 2;

        // split the row into before and after images
        for ( int i = 0; i < rowSize; i++ )
        {
            ResultColumn    origBeforeRC = fullUpdateRow.elementAt( i );
            ResultColumn    origAfterRC = fullUpdateRow.elementAt( i + rowSize );
            ResultColumn    beforeRC = origBeforeRC.cloneMe();
            ResultColumn    afterRC = origAfterRC.cloneMe();

            beforeColumns.addResultColumn( beforeRC );
            afterColumns.addResultColumn( afterRC );
        }

        buildThenColumnsForUpdate( fullFromList, targetTable, fullUpdateRow, beforeColumns, afterColumns );

        getCompilerContext().skipTypePrivileges( wasSkippingTypePrivileges );
    }

    /**
     * <p>
     * Due to discrepancies in how names are resolved by SELECT and UPDATE,
     * we have to force the left side of SET clauses to use the same table identifiers
     * as the right sides of the SET clauses.
     * </p>
     */
    private ResultColumnList    realiasSetClauses
        (
         FromBaseTable targetTable
         )
        throws StandardException
    {
        ResultColumnList    rcl = new ResultColumnList( getContextManager() );
        for ( int i = 0; i < _updateColumns.size(); i++ )
        {
            ResultColumn    setRC = _updateColumns.elementAt( i );
            TableName   tableName = targetTable.getTableName();
            ColumnReference newTargetColumn = new ColumnReference
                (
                 setRC.getReference().getColumnName(),
                 tableName,
                 getContextManager()
                 );
            newTargetColumn.setMergeTableID( ColumnReference.MERGE_TARGET );
            ResultColumn    newRC = new ResultColumn
                (
                 newTargetColumn,
                 setRC.getExpression(),
                 getContextManager()
                 );
            rcl.addResultColumn( newRC );
        }

        return rcl;
    }
    
    /**
     * <p>
     * Get the bound SELECT node under the dummy UPDATE node.
     * This may not be the source result set of the UPDATE node. That is because a ProjectRestrictNode
     * may have been inserted on top of it by DEFAULT handling. This method
     * exists to make the UPDATE actions of MERGE statements behave like ordinary
     * UPDATE statements in this situation. The behavior is actually wrong. See
     * DERBY-6414. Depending on how that bug is addressed, we may be able
     * to remove this method eventually.
     * </p>
     */
    private ResultSetNode    getBoundSelectUnderUpdate()
        throws StandardException
    {
        ResultSetNode   candidate = _dml.resultSet;

        while ( candidate != null )
        {
            if ( candidate instanceof SelectNode ) { return candidate; }
            else if ( candidate instanceof SingleChildResultSetNode )
            {
                candidate = ((SingleChildResultSetNode) candidate).getChildResult();
            }
            else    { break; }
        }
        
        // don't understand what's going on
        throw StandardException.newException( SQLState.NOT_IMPLEMENTED );
    }
    
    /** Bind the SET clauses of an UPDATE action */
    private void    bindSetClauses
        (
         MergeNode mergeNode,
         FromList fullFromList,
         FromTable targetTable,
         ResultColumnList   setClauses
         )
        throws StandardException
    {
        // needed to make the UpdateNode bind
        setClauses.replaceOrForbidDefaults( targetTable.getTableDescriptor(), _updateColumns, true );

        bindExpressions( setClauses, fullFromList );

        //
        // For column resolution later on, columns on the left side
        // of SET operators are associated with the TARGET table.
        //
        for ( int i = 0; i < _updateColumns.size(); i++ )
        {
            ResultColumn    rc = _updateColumns.elementAt( i );
            ColumnReference  cr = rc.getReference();
            cr.setMergeTableID( ColumnReference.MERGE_TARGET );
        }

        // Now associate the columns on the right side of SET operators.
        List<ColumnReference> colRefs = getColumnReferences( _updateColumns );
        for ( ColumnReference cr : colRefs )
        {
            mergeNode.associateColumn( fullFromList, cr, ColumnReference.MERGE_UNKNOWN );
        }
    }

    /**
     * <p>
     * Construct the row in the temporary table which drives an UPDATE action.
     * Unlike a DELETE, whose temporary row is just a list of copied columns, the
     * temporary row for UPDATE may contain complex expressions which must
     * be code-generated later on.
     * </p>
     */
    private void    buildThenColumnsForUpdate
        (
         FromList fullFromList,
         FromTable targetTable,
         ResultColumnList   fullRow,
         ResultColumnList beforeRow,
         ResultColumnList afterValues
         )
        throws StandardException
    {
        TableDescriptor td = targetTable.getTableDescriptor();
        HashSet<String> changedColumns = getChangedColumnNames();
        HashSet<String> changedGeneratedColumns = getChangedGeneratedColumnNames( td, changedColumns );
        
        _thenColumns = fullRow.copyListAndObjects();

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
            ResultColumn    origRC = _thenColumns.elementAt( i );

            boolean isAfterColumn = (i >= beforeRow.size());

            // skip the final RowLocation column of an UPDATE
            boolean isRowLocation = isRowLocation( origRC );
            ValueNode   origExpr = origRC.getExpression();

            if ( isRowLocation ) { continue; }

            String              columnName = origRC.getName();
            ColumnDescriptor    cd = td.getColumnDescriptor( columnName );
            boolean         changed = false;

            //
            // This handles the case that a GENERATED BY DEFAULT identity column is being
            // set to the keyword DEFAULT. This causes the UPDATE action of a MERGE statement
            // to have the same wrong behavior as a regular UPDATE statement. See derby-6414.
            //
            if ( cd.isAutoincrement() && (origRC.getExpression() instanceof NumericConstantNode) )
            {
                DataValueDescriptor numericValue = ((NumericConstantNode) origRC.getExpression()).getValue();
                
                if ( numericValue == null )
                {
                    ResultColumn    newRC = makeAutoGenRC( targetTable, origRC, i+1 );
                    newRC.setVirtualColumnId( origRC.getVirtualColumnId() );
                    _thenColumns.setElementAt( newRC, i  );

                    continue;
                }
            }

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

            //
            // Generated columns need special handling. The value needs to be recalculated
            // under the following circumstances:
            //
            // 1) It's the after image of the column
            //
            // 2) AND the statement causes the value to change.
            //
            // Otherwise, the value should be set to whatever is in the row coming out
            // of the driving left join.
            //
            if ( cd.hasGenerationClause() )
            {
                if ( isAfterColumn && changedGeneratedColumns.contains( columnName ) )
                {
                    // Set the expression to something that won't choke ResultColumnList.generateEvaluatedRow().
                    // The value will be a Java null at execution time, which will cause the value
                    // to be re-generated.
                    origRC.setExpression( new UntypedNullConstantNode( getContextManager() ) );
                }
                else
                {
                    ColumnReference cr = new ColumnReference
                        ( columnName, targetTable.getTableName(), getContextManager() );
                    origRC.setExpression( cr );

                    // remove the column descriptor in order to turn off hasGenerationClause()
                    origRC.setColumnDescriptor( null, null );
                }
                
                continue;
            }

            if ( isAfterColumn )
            {
                for ( int ic = 0; ic < beforeRow.size(); ic++ )
                {
                    ResultColumn    icRC = beforeRow.elementAt( ic );

                    if ( columnName.equals( icRC.getName() ) )
                    {
                        ResultColumn    newRC = null;
                    
                        // replace DEFAULT for a generated or identity column
                        ResultColumn    valueRC = afterValues.elementAt( ic );

                        if ( valueRC.wasDefaultColumn() || (valueRC.getExpression() instanceof UntypedNullConstantNode ) )
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

                            newRC = makeAutoGenRC( targetTable, origRC, i+1 );
                        }
                        else
                        {
                            newRC = valueRC.cloneMe();
                            newRC.setType( origRC.getTypeServices() );
                        }

                        newRC.setVirtualColumnId( origRC.getVirtualColumnId() );
                        _thenColumns.setElementAt( newRC, i  );
                        changed = true;
                        break;
                    }
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
            
            //
            // Turn off the autogenerated bit for identity columns so that
            // ResultColumnList.generateEvaluatedRow() doesn't try to compile
            // code to generate values for the before images in UPDATE rows.
            // This logic will probably need to be revisited as part of fixing derby-6414.
            //
            finalRC.resetAutoincrementGenerated();
        }   // end loop through _thenColumns
    }

    /** Get the names of the columns explicitly changed by SET clauses */
    private HashSet<String> getChangedColumnNames()
        throws StandardException
    {
        HashSet<String> result = new HashSet<String>();

        for ( int i = 0; i < _updateColumns.size(); i++ )
        {
            String  columnName = _updateColumns.elementAt( i ).getName();
            result.add( columnName );
        }

        return result;
    }

    /**
     * <p>
     * Get the names of the generated columns which are changed
     * by the UPDATE statement. These are the generated columns which
     * match one of the following conditions:
     * </p>
     *
     * <ul>
     * <li>Are explicitly mentioned on the left side of a SET clause.</li>
     * <li>Are built from other columns which are explicitly mentioned on the left side of a SET clause.</li>
     * </ul>
     */
    private HashSet<String> getChangedGeneratedColumnNames
        (
         TableDescriptor    targetTableDescriptor,
         HashSet<String>    changedColumnNames  // columns which are explicitly mentioned on the left side of a SET clause
         )
        throws StandardException
    {
        HashSet<String> result = new HashSet<String>();

        for ( ColumnDescriptor cd : targetTableDescriptor.getColumnDescriptorList() )
        {
            if ( !cd.hasGenerationClause() ) { continue; }

            if ( changedColumnNames.contains( cd.getColumnName() ) )
            {
                result.add( cd.getColumnName() );
                continue;
            }

            String[]    referencedColumns = cd.getDefaultInfo().getReferencedColumnNames();

            for ( String referencedColumnName : referencedColumns )
            {
                if ( changedColumnNames.contains( referencedColumnName ) )
                {
                    result.add( referencedColumnName );
                    break;
                }
            }
        }

        return result;
    }


    ////////////////
    //
    // BIND DELETE
    //
    ////////////////

    /** Bind a WHEN MATCHED ... THEN DELETE clause */
    private void    bindDelete
        (
         DataDictionary dd,
         FromList fullFromList,
         FromBaseTable targetTable
         )
        throws StandardException
    {
        //
        // Don't add any privileges until we bind the DELETE.
        //
        IgnoreFilter    ignorePermissions = new IgnoreFilter();
        getCompilerContext().addPrivilegeFilter( ignorePermissions );
            
        FromBaseTable   deleteTarget = new FromBaseTable
            ( targetTable.getTableNameField(), null, null, null, getContextManager() );
        FromList    dummyFromList = new FromList( getContextManager() );
        dummyFromList.addFromTable( deleteTarget );
        dummyFromList.bindTables( dd, new FromList( getOptimizerFactory().doJoinOrderOptimization(), getContextManager() ) );
 
        CurrentOfNode   currentOfNode = CurrentOfNode.makeForMerge
            ( CURRENT_OF_NODE_NAME, deleteTarget, getContextManager() );
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
        _dml = new DeleteNode( targetTable.getTableNameField(), selectNode, this, getContextManager() );

        // ready to add permissions
        getCompilerContext().removePrivilegeFilter( ignorePermissions );

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
        ResultColumnList    dmlSignature = _dml.resultSet.getResultColumns();
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

    ////////////////
    //
    // BIND INSERT
    //
    ////////////////

    /** Bind a WHEN NOT MATCHED ... THEN INSERT clause */
    private void    bindInsert
        (
         DataDictionary dd,
         MergeNode  mergeNode,
         FromList fullFromList,
         FromBaseTable targetTable
         )
        throws StandardException
    {
        ResultColumnList    selectList = new ResultColumnList( getContextManager() );
        for ( int i = 0; i < _insertValues.size(); i++ )
        {
            ResultColumn    rc = _insertValues.elementAt( i );
            selectList.addResultColumn( rc.cloneMe() );
        }
        selectList.replaceOrForbidDefaults( targetTable.getTableDescriptor(), _insertColumns, true );

        bindExpressions( selectList, fullFromList );
        
        bindInsertValues( fullFromList, targetTable );

        // the VALUES clause may not mention columns in the target table
        FromList    sourceTableFromList = new FromList( getOptimizerFactory().doJoinOrderOptimization(), getContextManager() );
        sourceTableFromList.addElement( fullFromList.elementAt( MergeNode.SOURCE_TABLE_INDEX ) );
        bindExpressions( _insertValues, sourceTableFromList );
        
        SelectNode  selectNode = new SelectNode
            (
             selectList,      // select list
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
             targetTable.getTableNameField(),
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

        buildThenColumnsForInsert( fullFromList, targetTable, _dml.resultSet.getResultColumns(), _insertColumns, _insertValues );
    }

    /**  Bind the values in the INSERT list */
    private void    bindInsertValues
        (
         FromList fullFromList,
         FromTable targetTable
         )
        throws StandardException
    {
        TableDescriptor td = targetTable.getTableDescriptor();

        // construct a full insert column list if insert columns weren't specified
        if ( _insertColumns == null )  { _insertColumns = buildFullColumnList( td ); }

        if ( _insertColumns.size() != _insertValues.size() )
        {
            throw StandardException.newException( SQLState.LANG_DB2_INVALID_COLS_SPECIFIED ); 
        }
        
        // forbid illegal values for identity columns
        for ( int i = 0; i <_insertValues.size(); i++ )
        {
            ResultColumn    rc = _insertValues.elementAt( i );
            String          columnName = _insertColumns.elementAt( i ).getName();
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

    /**
     * <p>
     * Build the full column list for a table.
     * </p>
     */
    private ResultColumnList    buildFullColumnList( TableDescriptor td )
        throws StandardException
    {
        ResultColumnList    result = new ResultColumnList( getContextManager() );
		ColumnDescriptorList cdl = td.getColumnDescriptorList();
		int					 cdlSize = cdl.size();

		for ( int index = 0; index < cdlSize; index++ )
		{
            ColumnDescriptor colDesc = cdl.elementAt( index );
            ColumnReference columnRef = new ColumnReference( colDesc.getColumnName(), null, getContextManager() );
            ResultColumn    resultColumn = new ResultColumn
                (
                 columnRef,
                 null,
                 getContextManager()
                 );
            
            result.addResultColumn( resultColumn );
        }

        return result;
    }
    
    /**
     * <p>
     * Construct the row in the temporary table which drives an INSERT action.
     * Unlike a DELETE, whose temporary row is just a list of copied columns, the
     * temporary row for INSERT may contain complex expressions which must
     * be code-generated later on.
     * </p>
     */
    private void    buildThenColumnsForInsert
        (
         FromList fullFromList,
         FromTable targetTable,
         ResultColumnList   fullRow,
         ResultColumnList insertColumns,
         ResultColumnList insertValues
         )
        throws StandardException
    {
        //
        // Don't add USAGE privilege on user-defined types just because we're
        // building the THEN columns.
        //
        boolean wasSkippingTypePrivileges = getCompilerContext().skipTypePrivileges( true );
        TableDescriptor td = targetTable.getTableDescriptor();

        _thenColumns = fullRow.copyListAndObjects();

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
            ResultColumn    origRC = _thenColumns.elementAt( i );

            String              columnName = origRC.getName();
            ColumnDescriptor    cd = td.getColumnDescriptor( columnName );
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

            if ( cd.hasGenerationClause() )
            {
                origRC.setExpression( new UntypedNullConstantNode( getContextManager() ) );
                continue;
            }

            for ( int ic = 0; ic < insertColumns.size(); ic++ )
            {
                ResultColumn    icRC = insertColumns.elementAt( ic );

                if ( columnName.equals( icRC.getName() ) )
                {
                    ResultColumn    newRC = null;
                    
                    // replace DEFAULT for a generated or identity column
                    ResultColumn    valueRC = insertValues.elementAt( ic );

                    if ( valueRC.wasDefaultColumn() || (valueRC.getExpression() instanceof UntypedNullConstantNode ) )
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

                        newRC = makeAutoGenRC( targetTable, origRC, i+1 );
                    }
                    else
                    {
                        newRC = valueRC.cloneMe();
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

        getCompilerContext().skipTypePrivileges( wasSkippingTypePrivileges );
    }

    /**
     * <p>
     * Make a ResultColumn for an identity column which is being set to the DEFAULT
     * value. This special ResultColumn will make it through code generation so that it
     * will be calculated when the INSERT/UPDATE action is run.
     * </p>
     */
    private ResultColumn    makeAutoGenRC
        (
         FromTable targetTable,
         ResultColumn   origRC,
         int    virtualColumnID
         )
        throws StandardException
    {
        String              columnName = origRC.getName();
        ColumnReference autoGenCR = new ColumnReference( columnName, targetTable.getTableName(), getContextManager() );
        ResultColumn    autoGenRC = new ResultColumn( autoGenCR, null, getContextManager() );
        VirtualColumnNode autoGenVCN = new VirtualColumnNode( targetTable, autoGenRC, virtualColumnID, getContextManager() );
        ResultColumn    newRC = new ResultColumn( autoGenCR, autoGenVCN, getContextManager() );

        // set the type so that buildThenColumnSignature() will function correctly
        newRC.setType( origRC.getTypeServices() );

        return newRC;
    }


    /////////////////
    //
    // BIND MINIONS
    //
    /////////////////

    /** Boilerplate for binding a list of ResultColumns against a FromList */
    private void bindExpressions( ResultColumnList rcl, FromList fromList )
        throws StandardException
    {
        CompilerContext cc = getCompilerContext();
        final int previousReliability = cc.getReliability();

        boolean wasSkippingTypePrivileges = cc.skipTypePrivileges( true );
        cc.setReliability( previousReliability | CompilerContext.SQL_IN_ROUTINES_ILLEGAL );
        
        try {
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
            cc.skipTypePrivileges( wasSkippingTypePrivileges );
        }
    }

    /**
     * <p>
     * Forbid subqueries in WHEN [ NOT ] MATCHED clauses.
     * </p>
     */
    private void    forbidSubqueries()
        throws StandardException
    {
        forbidSubqueries( _matchingRefinement );
        forbidSubqueries( _updateColumns );
        forbidSubqueries( _insertColumns );
        forbidSubqueries( _insertValues );
    }
    private void    forbidSubqueries( ResultColumnList rcl )
        throws StandardException
    {
        if ( rcl != null )
        {
            for ( int i = 0; i < rcl.size(); i++ )
            {
                forbidSubqueries( rcl.elementAt( i ) );
            }
        }
    }
    private void    forbidSubqueries( ValueNode expr )
        throws StandardException
    {
        if ( expr != null )
        {
            CollectNodesVisitor<SubqueryNode> getSubqueries =
                new CollectNodesVisitor<SubqueryNode>(SubqueryNode.class);
            expr.accept( getSubqueries );
            if ( getSubqueries.getList().size() > 0 )
            {
                throw StandardException.newException( SQLState.LANG_NO_SUBQUERIES_IN_MATCHED_CLAUSE );
            }
        }
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
         ResultSetNode  generatedScan,
         HalfOuterJoinNode  hojn,
         int    clauseNumber
         )
        throws StandardException
    {
        _clauseNumber = clauseNumber;

        adjustMatchingRefinement( selectList, generatedScan );
        
        generateInsertUpdateRow( acb, selectList, generatedScan, hojn );
        
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
        if( isDeleteClause()) { return; }
        else
        {
            ValueNode   checkConstraints = isInsertClause() ?
                ((InsertNode) _dml).checkConstraints :
                ((UpdateNode) _dml).checkConstraints;

            if ( checkConstraints != null )
            {
                List<ColumnReference> colRefs = getColumnReferences( checkConstraints );
                for ( ColumnReference cr : colRefs )
                {
                    cr.getSource().setResultSetNumber( NoPutResultSet.TEMPORARY_RESULT_SET_NUMBER );
                }
            }            
        }
    }

    /**
     * <p>
     * Adds a field to the generated class to hold the ResultSet of "then" rows
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
     * Generate a method to build a row for the temporary table for INSERT/UPDATE actions.
     * The method stuffs each column in the row with the result of the corresponding
     * expression built out of columns in the current row of the driving left join.
     * The method returns the stuffed row.
     * </p>
     */
    private void    generateInsertUpdateRow
        (
         ActivationClassBuilder acb,
         ResultColumnList selectList,
         ResultSetNode  generatedScan,
         HalfOuterJoinNode  hojn
         )
        throws StandardException
    {
        // point expressions in the temporary row at the columns in the
        // result column list of the driving left join.
        adjustThenColumns( selectList, generatedScan, hojn );
        
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
     * Point the column references in the matching refinement at the corresponding
     * columns returned by the driving left join.
     * </p>
     */
    private void    adjustMatchingRefinement
        (
         ResultColumnList selectList,
         ResultSetNode  generatedScan
         )
        throws StandardException
    {
        if ( _matchingRefinement != null )
        {
            useGeneratedScan( selectList, generatedScan, _matchingRefinement );
        }
    }
    
    /**
     * <p>
     * Point the column references in the temporary row at the corresponding
     * columns returned by the driving left join.
     * </p>
     */
    private void    adjustThenColumns
        (
         ResultColumnList selectList,
         ResultSetNode  generatedScan,
         HalfOuterJoinNode  hojn
         )
        throws StandardException
    {
        ResultColumnList    leftJoinResult = generatedScan.getResultColumns();

        useGeneratedScan( selectList, generatedScan, _thenColumns );

        //
        // For an UPDATE action, the final column in the temporary row is the
        // RowLocation. Point it at the last column in the row returned by the left join.
        //
        int                 lastRCSlot = _thenColumns.size() - 1;
        ResultColumn    lastRC = _thenColumns.elementAt( lastRCSlot );

        if ( isRowLocation( lastRC ) )
        {
            ResultColumn    lastLeftJoinRC = leftJoinResult.elementAt( leftJoinResult.size() - 1 );
            ValueNode       value = lastLeftJoinRC.getExpression();
            String              columnName = lastLeftJoinRC.getName();
            ColumnReference rowLocationCR = new ColumnReference
                ( columnName, hojn.getTableName(), getContextManager() );

            rowLocationCR.setSource( lastLeftJoinRC );
            
            ResultColumn    rowLocationRC = new ResultColumn( columnName, rowLocationCR, getContextManager() );

            _thenColumns.removeElementAt( lastRCSlot );
            _thenColumns.addResultColumn( rowLocationRC );
        }
    }

    /**
     * <p>
     * Point a node's ColumnReferences into the row returned by the driving left join.
     * </p>
     */
    private void    useGeneratedScan
        (
         ResultColumnList selectList,
         ResultSetNode  generatedScan,
         QueryTreeNode  node
         )
        throws StandardException
    {
        ResultColumnList    leftJoinResult = generatedScan.getResultColumns();

        for ( ColumnReference cr : getColumnReferences( node ) )
        {
            ResultColumn    leftJoinRC = leftJoinResult.elementAt( getSelectListOffset( selectList, cr ) - 1 );
            cr.setSource( leftJoinRC );
        }
    }
    
    /**
     * <p>
     * Find a column reference in the SELECT list of the driving left join
     * and return its 1-based offset into that list.  Returns -1 if the column
     * can't be found.
     * </p>
     */
    private int getSelectListOffset( ResultColumnList selectList, ValueNode thenExpression )
        throws StandardException
    {
        int                 selectCount = selectList.size();

        if ( thenExpression instanceof ColumnReference )
        {
            ColumnReference thenCR = (ColumnReference) thenExpression;
            String              thenCRName = thenCR.getColumnName();
            int                 thenCRMergeTableID = getMergeTableID( thenCR );

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
                        ( getMergeTableID( selectCR ) == thenCRMergeTableID) &&
                        thenCRName.equals( selectCR.getColumnName() )
                        )
                    {
                        return sidx + 1;
                    }
                }
            }
            
            if (SanityManager.DEBUG)
            {
                SanityManager.THROWASSERT
                    (
                     "Can't find select list column corresponding to " + thenCR.getSQLColumnName() +
                     " with merge table id = " + thenCRMergeTableID
                     );
            }
        }
        else if ( thenExpression instanceof CurrentRowLocationNode )
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

    /** Find the MERGE table id of the indicated column */
    private int getMergeTableID( ColumnReference cr )
    {
        int                 mergeTableID = cr.getMergeTableID();

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT
                (
                 ( (mergeTableID == ColumnReference.MERGE_SOURCE) || (mergeTableID == ColumnReference.MERGE_TARGET) ),
                 "Column " + cr.getSQLColumnName() + " has illegal MERGE table id: " + mergeTableID
                 );
        }

        return mergeTableID;
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
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 */
    @Override
    void printSubNodes( int depth )
	{
		if (SanityManager.DEBUG)
		{
			super.printSubNodes( depth );

            if ( _matchingRefinement != null )
            {
                printLabel( depth, "matchingRefinement: " );
                _matchingRefinement.treePrint( depth + 1 );
            }

            if ( _updateColumns != null )
            {
                printLabel( depth, "updateColumns: " );
                _updateColumns.treePrint( depth + 1 );
            }

            if ( _insertColumns != null )
            {
                printLabel( depth, "insertColumns: " );
                _insertColumns.treePrint( depth + 1 );
            }

            if ( _insertValues != null )
            {
                printLabel( depth, "insertValues: " );
                _insertValues.treePrint( depth + 1 );
            }
		}
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

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Get a list of column references in an expression */
    private List<ColumnReference>   getColumnReferences( QueryTreeNode expression )
        throws StandardException
    {
        CollectNodesVisitor<ColumnReference> getCRs =
            new CollectNodesVisitor<ColumnReference>(ColumnReference.class);

        expression.accept(getCRs);
        
        return getCRs.getList();
    }

    /** Return true if the ResultColumn represents a RowLocation */
    private boolean isRowLocation( ResultColumn rc ) throws StandardException
    {
        if ( rc.getExpression() instanceof CurrentRowLocationNode ) { return true; }

        DataTypeDescriptor  dtd = rc.getTypeServices();
        if ( (dtd != null) && (dtd.getTypeId().isRefTypeId()) ) { return true; }

        return false;
    }

    @Override
    public boolean referencesSessionSchema() throws StandardException {
        return referencesSessionSchema(_matchingRefinement)
                || referencesSessionSchema(_updateColumns)
                || referencesSessionSchema(_insertColumns)
                || referencesSessionSchema(_insertValues);
    }

    private static boolean referencesSessionSchema(QueryTreeNode node)
            throws StandardException {
        return node != null && node.referencesSessionSchema();
    }
}
