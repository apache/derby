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

import java.util.ArrayList;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;

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

    // filled in at bind() time
    private DMLModStatementNode _dml;

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
    {
        super( cm );
        
        _matchingRefinement = matchingRefinement;
        _updateColumns = updateColumns;
        _insertColumns = insertColumns;
        _insertValues = insertValues;
    }

    /** Make a WHEN MATCHED ... THEN UPDATE clause */
    public  static  MatchingClauseNode   makeUpdateClause
        (
         ValueNode  matchingRefinement,
         ResultColumnList   updateColumns,
         ContextManager     cm
         )
    {
        return new MatchingClauseNode( matchingRefinement, updateColumns, null, null, cm );
    }

    /** Make a WHEN MATCHED ... THEN DELETE clause */
    public  static  MatchingClauseNode   makeDeleteClause
        (
         ValueNode  matchingRefinement,
         ContextManager     cm
         )
    {
        return new MatchingClauseNode( matchingRefinement, null, null, null, cm );
    }

    /** Make a WHEN NOT MATCHED ... THEN INSERT clause */
    public  static  MatchingClauseNode   makeInsertClause
        (
         ValueNode  matchingRefinement,
         ResultColumnList   insertColumns,
         ResultColumnList   insertValues,
         ContextManager     cm
         )
    {
        return new MatchingClauseNode( matchingRefinement, null, insertColumns, insertValues, cm );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ACCESSORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Return true if this is a WHEN MATCHED ... UPDATE clause */
    public  boolean isUpdateClause()    { return (_updateColumns != null); }
    
    /** Return true if this is a WHEN NOT MATCHED ... INSERT clause */
    public  boolean isInsertClause()    { return (_insertValues != null); }
    
    /** Return true if this is a WHEN MATCHED ... DELETE clause */
    public  boolean isDeleteClause()    { return !( isUpdateClause() || isInsertClause() ); }

    /** Return the bound DML statement--returns null if called before binding */
    public  DMLModStatementNode getDML()    { return _dml; }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // bind() BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Bind this WHEN [ NOT ] MATCHED clause against the parent JoinNode */
    public void    bind( JoinNode joinNode, FromTable targetTable )
        throws StandardException
    {
        String  clauseType = isInsertClause() ? "WHEN NOT MATCHED" : "WHEN MATCHED";

        // For WHEN NOT MATCHED clauses, the target table is not in scope.
        boolean useTargetTable = !isInsertClause();

        if ( _matchingRefinement != null )
        {
            _matchingRefinement = joinNode.bindExpression
                ( _matchingRefinement, true, useTargetTable, clauseType );
        }

        if ( isDeleteClause() ) { bindDelete( joinNode, targetTable ); }
        if ( isUpdateClause() ) { bindUpdate( joinNode, targetTable ); }
        if ( isInsertClause() ) { bindInsert( joinNode, targetTable ); }
    }

    /** Bind a WHEN MATCHED ... THEN UPDATE clause */
    private void    bindUpdate( JoinNode joinNode, FromTable targetTable )
        throws StandardException
    {
        SelectNode  selectNode = new SelectNode
            (
             _updateColumns,
             joinNode.makeFromList( true, true ),
             null,      // where clause
             null,      // group by list
             null,      // having clause
             null,      // window list
             null,      // optimizer plan override
             getContextManager()
             );
        _dml = new UpdateNode( targetTable.getTableName(), selectNode, true, getContextManager() );

        _dml.bindStatement();
    }
    
    /** Bind a WHEN MATCHED ... THEN DELETE clause */
    private void    bindDelete( JoinNode joinNode, FromTable targetTable )
        throws StandardException
    {
        SelectNode  selectNode = new SelectNode
            (
             null,      // select list
             joinNode.makeFromList( true, true ),
             null,      // where clause
             null,      // group by list
             null,      // having clause
             null,      // window list
             null,      // optimizer plan override
             getContextManager()
             );
        _dml = new DeleteNode( targetTable.getTableName(), selectNode, getContextManager() );

        _dml.bindStatement();
    }

    /** Bind a WHEN NOT MATCHED ... THEN INSERT clause */
    private void    bindInsert( JoinNode joinNode, FromTable targetTable )
        throws StandardException
    {
        // needed to make the SelectNode bind
        _insertValues.replaceOrForbidDefaults( targetTable.getTableDescriptor(), _insertColumns, true );

        // the VALUES clause may not mention columns in the target table
        _insertValues.bindExpressions
            (
             joinNode.makeFromList( true, false ),
             new SubqueryList( getContextManager() ),
             new ArrayList<AggregateNode>()
             );
        
        SelectNode  selectNode = new SelectNode
            (
             _insertValues,      // select list
             joinNode.makeFromList( true, true ),
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
             null,      // targetProperties
             null,      // order by cols
             null,      // offset
             null,      // fetch first
             false,     // has JDBC limit clause
             getContextManager()
             );

        _dml.bindStatement();
    }

}
