/*

   Derby - Class org.apache.derby.impl.sql.execute.RowCountResultSet

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

import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.DataValueDescriptor;



/**
 * This result set implements the filtering of rows needed for the <result
 * offset clause> and the <fetch first clause>.  It sits on top of the normal
 * SELECT's top result set, but under any ScrollInsensitiveResultSet needed for
 * cursors. The latter positioning is needed for the correct functioning of
 * <result offset clause> and <fetch first clause> in the presence of
 * scrollable and/or updatable result sets and CURRENT OF cursors.
 *
 * It is only ever generated if at least one of the two clauses is present.
 */
class RowCountResultSet extends NoPutResultSetImpl
    implements CursorResultSet
{
    // set in constructor and not altered during
    // life of object.
    final NoPutResultSet source;
    final private boolean runTimeStatsOn;
    private long offset;
    private long fetchFirst;
    final private GeneratedMethod offsetMethod;
    final private GeneratedMethod fetchFirstMethod;
    final private boolean hasJDBClimitClause;

    /**
     * True if we haven't yet fetched any rows from this result set.
     * Will be reset on close so the result set is ready to reuse.
     */
    private boolean virginal;

    /**
     * Holds the number of rows returned so far in this round of using the
     * result set.  Will be reset on close so the result set is ready to reuse.
     */
    private long rowsFetched;

    /**
     * RowCountResultSet constructor
     *
     * @param s               The source result set being filtered
     * @param a               The activation for this result set,
     *                        which provides the context for the row
     *                        allocation operation
     * @param resultSetNumber The resultSetNumber for the ResultSet
     * @param offsetMethod   Generated method
     * @param fetchFirstMethod Generated method
     * @param hasJDBClimitClause True if offset/fetchFirst clauses were added by JDBC LIMIT escape syntax
     * @param optimizerEstimatedRowCount
     *                        Estimated total # of rows by optimizer
     * @param optimizerEstimatedCost
     *                        Estimated total cost by optimizer
     * @exception StandardException Standard error policy
     */
    RowCountResultSet
        (NoPutResultSet s,
         Activation a,
         int resultSetNumber,
         GeneratedMethod offsetMethod,
         GeneratedMethod fetchFirstMethod,
         boolean hasJDBClimitClause,
         double optimizerEstimatedRowCount,
         double optimizerEstimatedCost)
            throws StandardException {

        super(a,
              resultSetNumber,
              optimizerEstimatedRowCount,
              optimizerEstimatedCost);

        this.offsetMethod = offsetMethod;
        this.fetchFirstMethod = fetchFirstMethod;
        this.hasJDBClimitClause = hasJDBClimitClause;

        source = s;

        virginal = true;
        rowsFetched = 0;

        /* Remember whether or not RunTimeStatistics is on */
        runTimeStatsOn =
            getLanguageConnectionContext().getRunTimeStatisticsMode();
        recordConstructorTime();
    }

    //
    // NoPutResultSet interface
    //

    /**
     * Open a scan on the table. scan parameters are evaluated
     * at each open, so there is probably some way of altering
     * their values...
     *
     * @exception StandardException thrown if cursor finished.
     */
    public void openCore() throws StandardException {

        boolean constantEval = true;

        beginTime = getCurrentTimeMillis();

        source.openCore();
        isOpen = true;

        numOpens++;

        openTime += getElapsedMillis(beginTime);
    }

    /**
     * Reopen a scan on the table. scan parameters are evaluated
     * at each open, so there is probably some way of altering
     * their values...
     *
     * @exception StandardException thrown if cursor finished.
     */
    public void reopenCore() throws StandardException {

        boolean constantEval = true;

        beginTime = getCurrentTimeMillis();

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(isOpen,
                                 "RowCountResultSet not open, cannot reopen");

        source.reopenCore();

        virginal = true;
        rowsFetched = 0;
        isOpen = true;

        numOpens++;

        openTime += getElapsedMillis(beginTime);
    }

    /**
     * Return the requested values computed from the next row (if any)
     * <p>
     * @exception StandardException thrown on failure.
     * @exception StandardException ResultSetNotOpen thrown if not yet open.
     *
     * @return the next row in the result
     */
    public ExecRow  getNextRowCore() throws StandardException {
	if( isXplainOnlyMode() )
		return null;

        ExecRow result = null;

        beginTime = getCurrentTimeMillis();

        if (virginal) {
            if (offsetMethod != null) {
                DataValueDescriptor offVal
                    = (DataValueDescriptor)offsetMethod.invoke(activation);

                if (offVal.isNotNull().getBoolean()) {
                    offset = offVal.getLong();

                    if (offset < 0) {
                        throw StandardException.newException(
                            SQLState.LANG_INVALID_ROW_COUNT_OFFSET,
                            Long.toString(offset));
                    } else {
                        offset = offVal.getLong();
                    }
                } else {
                    throw StandardException.newException(
                        SQLState.LANG_ROW_COUNT_OFFSET_FIRST_IS_NULL,
                        "OFFSET");
                }
            } else {
                // not given
                offset = 0;
            }


            if (fetchFirstMethod != null) {
                DataValueDescriptor fetchFirstVal
                    = (DataValueDescriptor)fetchFirstMethod.invoke(activation);

                if (fetchFirstVal.isNotNull().getBoolean()) {

                    fetchFirst = fetchFirstVal.getLong();

                    //
                    // According to section 13.4.6 of the JDBC 4.1 MR spec, you
                    // can specify a LIMIT of 0. This means that you want all rows
                    // to be returned from the OFFSET onwards. This diverges from
                    // the SQL Standard treatment of the FETCH FIRST clause. For the
                    // SQL Standard, a FETCH FIRST value of 0 rows is supposed to
                    // raise an error. See the functional spec attached to DERBY-5488.
                    // Here we translate a JDBC LIMIT of 0 into a FETCH FIRST value of
                    // Long.MAX_VALUE so that all rows will be returned from OFFSET onwards.
                    //
                    if ( hasJDBClimitClause && (fetchFirst == 0) ) { fetchFirst = Long.MAX_VALUE; }

                    if (fetchFirst < 1) {
                        throw StandardException.newException(
                            SQLState.LANG_INVALID_ROW_COUNT_FIRST,
                            Long.toString(fetchFirst));
                    }
                } else {
                    throw StandardException.newException(
                        SQLState.LANG_ROW_COUNT_OFFSET_FIRST_IS_NULL,
                        "FETCH FIRST/NEXT");
                }
            }

            if (offset > 0) {
                // Only skip rows the first time around
                virginal = false;

                long offsetCtr = offset;

                do {
                    result = source.getNextRowCore();
                    offsetCtr--;

                    if (result != null && offsetCtr >= 0) {
                        rowsFiltered++;
                    } else {
                        break;
                    }
                } while (true);
            } else {
                if (fetchFirstMethod != null && rowsFetched >= fetchFirst) {
                    result = null;
                } else {
                    result = source.getNextRowCore();
                }
            }
        } else {
            if (fetchFirstMethod != null && rowsFetched >= fetchFirst) {
                result = null;
            } else {
                result = source.getNextRowCore();
            }
        }


        if (result != null) {
            rowsFetched++;
            rowsSeen++;
        }

        setCurrentRow(result);

        if (runTimeStatsOn) {
            if (! isTopResultSet) {
                 // This is simply for RunTimeStats.  We first need to get the
                 // subquery tracking array via the StatementContext
                StatementContext sc = activation.getLanguageConnectionContext().
                    getStatementContext();
                subqueryTrackingArray = sc.getSubqueryTrackingArray();
            }

            nextTime += getElapsedMillis(beginTime);
        }
        return result;
    }

    /**
     * Return the total amount of time spent in this ResultSet
     *
     * @param type
     *    CURRENT_RESULTSET_ONLY - time spent only in this ResultSet
     *    ENTIRE_RESULTSET_TREE  - time spent in this ResultSet and below.
     *
     * @return long     The total amount of time spent (in milliseconds).
     */
    public long getTimeSpent(int type) {
        long totTime = constructorTime + openTime + nextTime + closeTime;

        if (type == CURRENT_RESULTSET_ONLY) {
            return  totTime - source.getTimeSpent(ENTIRE_RESULTSET_TREE);
        } else {
            return totTime;
        }
    }

    // ResultSet interface

    /**
     * @see org.apache.derby.iapi.sql.ResultSet#close
     */
    public void close() throws StandardException {

        beginTime = getCurrentTimeMillis();
        if ( isOpen ) {

            // we don't want to keep around a pointer to the
            // row ... so it can be thrown away.
            // REVISIT: does this need to be in a finally
            // block, to ensure that it is executed?
            clearCurrentRow();
            source.close();

            super.close();
        } else {
            if (SanityManager.DEBUG) {
                SanityManager.DEBUG("CloseRepeatInfo",
                                    "Close of RowCountResultSet repeated");
            }
        }

        // Reset state for result set reuse, if any
        virginal = true;
        rowsFetched = 0;

        closeTime += getElapsedMillis(beginTime);
    }


    /**
     * @see org.apache.derby.iapi.sql.ResultSet#finish
     */
    public void finish() throws StandardException {
        source.finish();
        finishAndRTS();
    }


    /**
     * @see org.apache.derby.iapi.sql.ResultSet#clearCurrentRow
     */
    public final void clearCurrentRow()
    {
        currentRow = null;
        activation.clearCurrentRow(resultSetNumber);

        // Added this since we need it to keep in synch for updatable result
        // sets/cursors; this result set needs to be "transparent" in such
        // cases, cf. getCurrentRow which gets the current row from the source
        // as well.
        source.clearCurrentRow();
    }



    //
    // CursorResultSet interface
    //

    /**
     * Gets information from its source.
     *
     * @see org.apache.derby.iapi.sql.execute.CursorResultSet#getRowLocation
     */
    public RowLocation getRowLocation() throws StandardException {

        return ( (CursorResultSet)source ).getRowLocation();
    }


    /**
     * Gets information from source
     *
     * @see org.apache.derby.iapi.sql.execute.CursorResultSet#getCurrentRow
     * @return the last row returned.
     */

    /* RESOLVE - this should return activation.getCurrentRow(resultSetNumber),
     * once there is such a method.  (currentRow is redundant)
     */
    public ExecRow getCurrentRow() throws StandardException
    {
        return ( (CursorResultSet)source ).getCurrentRow();
        // return currentRow;
    }

    /**
     * Override of NoPutResultSetImpl method. Ask the source.
     */
    public boolean isForUpdate() {
        return source.isForUpdate();
    }


    /**
     * Return underlying result set (the source og this result set) if it is a
     * ProjectRestrictResultSet, else null.
     */
    public ProjectRestrictResultSet getUnderlyingProjectRestrictRS() {
        if (source instanceof ProjectRestrictResultSet) {
            return (ProjectRestrictResultSet)source;
        } else {
            return null;
        }
    }

}
