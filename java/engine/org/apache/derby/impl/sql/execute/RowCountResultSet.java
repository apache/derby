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

import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.types.RowLocation;



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

    /**
     * RowCountResultSet constructor
     *
     * @param s               The source result set being filtered
     * @param a               The activation for this result set,
     *                        which provides the context for the row
     *                        allocation operation
     * @param resultSetNumber The resultSetNumber for the ResultSet
     * @param offset          The offset value (0 by default)
     * @param fetchFirst      The fetch first value (-1 if not in use)
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
         long offset,
         long fetchFirst,
         double optimizerEstimatedRowCount,
         double optimizerEstimatedCost)
        throws StandardException
    {
        super(a,
              resultSetNumber,
              optimizerEstimatedRowCount,
              optimizerEstimatedCost);

        source = s;

        this.offset = offset;
        this.fetchFirst = fetchFirst;

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

        ExecRow result = null;

        beginTime = getCurrentTimeMillis();

        if (offset > 0) {
            do {
                result = source.getNextRowCore();
                offset--;

                if (result != null && offset >= 0) {
                    rowsFiltered++;
                } else {
                    break;
                }

            } while (true);

            // only skip row first time
            offset = 0;
        } else {

            if (fetchFirst != -1 && rowsSeen >= fetchFirst) {
                result = null;
            } else {
                result = source.getNextRowCore();
            }
        }


        if (result != null) {
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
