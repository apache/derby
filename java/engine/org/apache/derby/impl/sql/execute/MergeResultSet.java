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
import org.apache.derby.iapi.services.io.StreamStorable;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.BackingStoreHashtable;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.SQLRef;
import org.apache.derby.shared.common.sanity.SanityManager;

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

	private BackingStoreHashtable		_subjectRowIDs;
    
	private int						_numOpens;
    
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

		if (_numOpens++ == 0)
		{
			_drivingLeftJoin.openCore();
		}
		else
		{
			_drivingLeftJoin.reopenCore();
		}

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

        if ( _drivingLeftJoin != null ) { _drivingLeftJoin.close(); }

        if ( _subjectRowIDs != null )
        {
            _subjectRowIDs.close();
            _subjectRowIDs = null;
        }
        
		_numOpens = 0;
    }


    public void finish() throws StandardException
    {
        if ( _drivingLeftJoin != null ) { _drivingLeftJoin.finish(); }
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
            SQLRef             baseRowLocation = null;

            boolean matched = false;
            if ( rlColumn != null )
            {
                if ( !rlColumn.isNull() )
                {
                    matched = true;
                    
                    // change the HeapRowLocation into a SQLRef, something which the
                    // temporary table can (de)serialize correctly
                    baseRowLocation = new SQLRef( (RowLocation) rlColumn.getObject() );
                    _row.setColumn( _row.nColumns(), baseRowLocation );
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
                // this will raise an exception if the row is being touched more than once
                if ( baseRowLocation != null ) { addSubjectRow( baseRowLocation ); }

                //
                // This bit of defensive code materializes large streams before they
                // are handed off to the WHEN [ NOT ] MATCHED clauses. By the time
                // that those clauses operate, the driving left join has been closed and
                // the streams can't be materialized.
                //
                for ( int i = 0; i < _row.nColumns(); i++ )
                {
                    DataValueDescriptor dvd = _row.getColumn( i + 1 );
                    if ( dvd instanceof StreamStorable )
                    {
                        if ( dvd.hasStream() )
                        {
                            _row.setColumn( i + 1, dvd.cloneValue( true ) );
                        }
                    }
                }
                
                _thenRows[ clauseIdx ] = matchingClause.bufferThenRow( activation, _thenRows[ clauseIdx ], _row );
                _rowCount++;
            }
        }

        return rowsFound;
    }

    /**
     * <p>
     * Add another subject row id to the evolving hashtable of affected target rows.
     * The concept of a subject row is defined by the 2011 SQL Standard, part 2,
     * section 14.12 (merge statement), general rule 6. A row in the target table
     * is a subject row if it joins to the source table on the main search condition
     * and if the joined row satisfies the matching refinement condition for
     * some WHEN MATCHED clause. A row in the target table may only be a
     * subject row once. That is, a given target row may only qualify for UPDATE
     * or DELETE processing once. If it qualifies for more than one UPDATE or DELETE
     * action, then the Standard requires us to raise a cardinality violation.
     * </p>
     *
     * @param   subjectRowID    The location of the subject row.
     *
	 * @exception StandardException A cardinality exception is thrown if we've already added this subject row.
     */
    private void    addSubjectRow( SQLRef subjectRowID ) throws StandardException
    {
        if ( _subjectRowIDs == null ) { createSubjectRowIDhashtable(); }

        if ( _subjectRowIDs.get( subjectRowID ) != null )
        {
            throw StandardException.newException( SQLState.LANG_REDUNDANT_SUBJECT_ROW );
        }
        else
        {
            DataValueDescriptor[] row = new DataValueDescriptor[] { subjectRowID };

            _subjectRowIDs.putRow( true, row, null );
        }
    }

    /**
     * <p>
     * Create a BackingStoreHashtable to hold the ids of subject rows.
     * </p>
     */
    private void    createSubjectRowIDhashtable()   throws StandardException
    {
		final int[] keyCols = new int[] { 0 };

		_subjectRowIDs = new BackingStoreHashtable
            (
             getActivation().getLanguageConnectionContext().getTransactionExecute(),
             null,          // no row source. we'll fill the hashtable as we go along
             keyCols,
             false,         // duplicate handling doesn't matter. we probe for duplicates and error out if we find one
             -1,            // who knows what the row count will be
             HashScanResultSet.DEFAULT_MAX_CAPACITY,
             HashScanResultSet.DEFAULT_INITIAL_CAPACITY,
             HashScanResultSet.DEFAULT_MAX_CAPACITY,
             false,         // null keys aren't relevant. the row id is always non-null
             false          // discard after commit
             );
    }

}
