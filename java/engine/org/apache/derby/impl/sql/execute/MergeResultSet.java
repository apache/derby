/*

   Derby - Class org.apache.derby.impl.sql.execute.MergeResultSet

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;

/**
 * INSERT/UPDATE/DELETE a target table based on how it outer joins
 * with a driving table. For a description of how Derby processes
 * the MERGE statement, see the header comment on MergeNode.
 */
class MergeResultSet extends NoRowsResultSetImpl 
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

    private NoPutResultSet          _drivingLeftJoin;
    private MergeConstantAction _constants;

    private ExecRow                 _row;
    private long                        _rowCount;
    private TemporaryRowHolderImpl[]    _thenRows;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Construct from a driving left join and an Activation.
     */
    MergeResultSet
        (
         NoPutResultSet drivingLeftJoin, 
         Activation activation
         )
        throws StandardException
    {
        super( activation );
        _drivingLeftJoin = drivingLeftJoin;
        _constants = (MergeConstantAction) activation.getConstantAction();
        _thenRows = new TemporaryRowHolderImpl[ _constants.matchingClauseCount() ];
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    @Override
    public final long   modifiedRowCount() { return _rowCount + RowUtil.getRowCountBase(); }

    public void open() throws StandardException
    {
        setup();

        boolean rowsFound = collectAffectedRows();
        if ( !rowsFound )
        {
            activation.addWarning( StandardException.newWarning( SQLState.LANG_NO_ROW_FOUND ) );
        }

        // now execute the INSERT/UPDATE/DELETE actions
        int         clauseCount = _constants.matchingClauseCount();
        for ( int i = 0; i < clauseCount; i++ )
        {
            _constants.getMatchingClause( i ).executeConstantAction( activation, _thenRows[ i ] );
        }

        cleanUp();
        endTime = getCurrentTimeMillis();
    }

    @Override
    void  setup() throws StandardException
    {
        super.setup();

        int         clauseCount = _constants.matchingClauseCount();
        for ( int i = 0; i < clauseCount; i++ )
        {
            _constants.getMatchingClause( i ).init();
        }

        _rowCount = 0L;
        _drivingLeftJoin.openCore();
    }
    
    /**
     * Clean up resources and call close on data members.
     */
    public void close() throws StandardException
    {
        close( false );
    }

    public void cleanUp() throws StandardException
    {
        int         clauseCount = _constants.matchingClauseCount();
        for ( int i = 0; i < clauseCount; i++ )
        {
            TemporaryRowHolderImpl  thenRows = _thenRows[ i ];
            if ( thenRows != null )
            {
                thenRows.close();
                _thenRows[ i ] = null;
            }
            
            _constants.getMatchingClause( i ).cleanUp();
        }
    }


    public void finish() throws StandardException
    {
        _drivingLeftJoin.finish();
        super.finish();
    }

    /**
     * <p>
     * Loop through the rows in the driving left join.
     * </p>
     */
    boolean  collectAffectedRows() throws StandardException
    {
        DataValueDescriptor     rlColumn;
        RowLocation             baseRowLocation;
        boolean rowsFound = false;

        while ( true )
        {
            // may need to objectify stream columns here.
            // see DMLWriteResultSet.getNextRowCoure(NoPutResultSet)
            _row =  _drivingLeftJoin.getNextRowCore();
            if ( _row == null ) { break; }

            // By convention, the last column for the driving left join contains a data value
            // containing the RowLocation of the target row.

            rowsFound = true;

            rlColumn = _row.getColumn( _row.nColumns() );
            baseRowLocation = null;

            boolean matched = false;
            if ( rlColumn != null )
            {
                if ( !rlColumn.isNull() )
                {
                    matched = true;
                    baseRowLocation = (RowLocation) rlColumn.getObject();
                }
            }

            // find the first clause which applies to this row
            MatchingClauseConstantAction    matchingClause = null;
            int         clauseCount = _constants.matchingClauseCount();
            int         clauseIdx = 0;
            for ( ; clauseIdx < clauseCount; clauseIdx++ )
            {
                MatchingClauseConstantAction    candidate = _constants.getMatchingClause( clauseIdx );
                boolean isWhenMatchedClause = false;
                
                switch ( candidate.clauseType() )
                {
                case ConstantAction.WHEN_MATCHED_THEN_UPDATE:
                case ConstantAction.WHEN_MATCHED_THEN_DELETE:
                    isWhenMatchedClause = true;
                    break;
                }

                boolean considerClause = (matched == isWhenMatchedClause);

                if ( considerClause )
                {
                    if ( candidate.evaluateRefinementClause( activation ) )
                    {
                        matchingClause = candidate;
                        break;
                    }
                }
            }

            if ( matchingClause != null )
            {
                _thenRows[ clauseIdx ] = matchingClause.bufferThenRow( activation, _thenRows[ clauseIdx ], _row );
                _rowCount++;
            }
        }

        return rowsFound;
    }

}
