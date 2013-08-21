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

import java.util.ArrayList;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

/**
 * <p>
 * A MergeNode represents a MERGE statement.  It is the top node of the
 * query tree for that statement. The driving result set for a MERGE statement
 * is essentially the following:
 * </p>
 *
 * <pre>
 * sourceTable LEFT OUTER JOIN targetTable ON searchCondition
 * </pre>
 */

public final class MergeNode extends DMLModStatementNode
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

    private FromTable   _targetTable;
    private FromTable   _sourceTable;
    private ValueNode   _searchCondition;
    private ArrayList<MatchingClauseNode>   _matchingClauses;

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
        super( null, cm );

        _targetTable = targetTable;
        _sourceTable = sourceTable;
        _searchCondition = searchCondition;
        _matchingClauses = matchingClauses;

        makeJoin();
    }

    /**
     * <p>
     * Construct the left outer join which will drive the execution.
     * </p>
     */
    private void    makeJoin() throws StandardException
    {
        resultSet = new HalfOuterJoinNode
            (
             _sourceTable,
             _targetTable,
             _searchCondition,
             null,
             false,
             null,
             getContextManager()
             );
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

        //
        // Bind the left join. This binds _targetTable and _sourceTable.
        //
        bind( dd );

        bindSearchCondition();

        if ( !targetIsBaseTable() )
        {
            throw StandardException.newException( SQLState.LANG_TARGET_NOT_BASE_TABLE );
        }

        if ( !sourceIsBase_View_or_VTI() )
        {
            throw StandardException.newException( SQLState.LANG_SOURCE_NOT_BASE_VIEW_OR_VTI );
        }

        // source and target may not have the same correlation names
        if ( getExposedName( _targetTable ).equals( getExposedName( _sourceTable ) ) )
        {
            throw StandardException.newException( SQLState.LANG_SAME_EXPOSED_NAME );
        }

        for ( MatchingClauseNode mcn : _matchingClauses )
        {
            mcn.bind( (JoinNode) resultSet, _targetTable );
        }

        throw StandardException.newException( SQLState.NOT_IMPLEMENTED, "MERGE" );
	}

    /** Get the exposed name of a FromTable */
    private String  getExposedName( FromTable ft ) throws StandardException
    {
        return ft.getTableName().getTableName();
    }

    /**  Bind the search condition, the ON clause of the left join */
    private void    bindSearchCondition()   throws StandardException
    {
        FromList    fromList = new FromList
            ( getOptimizerFactory().doJoinOrderOptimization(), getContextManager() );

        resultSet.bindResultColumns( fromList );
    }

    /** Return true if the target table is a base table */
    private boolean targetIsBaseTable() throws StandardException
    {
        if ( !( _targetTable instanceof FromBaseTable) ) { return false; }

        FromBaseTable   fbt = (FromBaseTable) _targetTable;
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

}
