/*

   Derby - Class org.apache.derby.impl.sql.execute.MultiProbeTableScanResultSet

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

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.compile.RowOrdering;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;

import org.apache.derby.iapi.types.DataValueDescriptor;

// These are for javadoc "@see" tags.
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.sql.execute.ResultSetFactory;

/**
 * Result set that fetches rows from a scan by "probing" the underlying
 * table with a given list of values.  Repeated calls to getNextRowCore()
 * will first return all rows matching probeValues[0], then all rows matching
 * probeValues[1], and so on (duplicate probe values are ignored).  Once all
 * matching rows for all values in probeValues have been returned, the call
 * to getNextRowCore() will return null, thereby ending the scan. The
 * expectation is that this kind of result set only ever appears beneath
 * some other top-level result set (esp. IndexRowToBaseRowResultSet), in
 * which case all result sets higher up in the result set tree will just
 * see a stream of rows satisfying the list of probe values.
 *
 * Currently this type of result is used for evaluation of IN lists, where
 * the user wants to retrieve all rows for which some target column has a
 * value that equals one of values in the IN list.  In that case the IN list
 * values are represented by the probeValues array.
 *
 * Most of the work for this class is inherited from TableScanResultSet. 
 * This class overrides four public methods and two protected methods
 * from TableScanResultSet.  In all cases the methods here set probing
 * state and then call the corresponding methods on "super".
 */
class MultiProbeTableScanResultSet extends TableScanResultSet
    implements CursorResultSet
{
    /** The values with which we will probe the table. */
    protected DataValueDescriptor [] probeValues;
    /**
     * The values with which we will probe the table, as they were passed to
     * the constructor. We need to keep them unchanged in case the result set
     * is reused when a statement is re-executed (see DERBY-827).
     */
    protected DataValueDescriptor [] origProbeValues;

    /**
     * 0-based position of the <b>next</b> value to lookup w.r.t. the probe
     * values list.
     */
    protected int probeValIndex;

    /**
     * Indicator as to which type of sort we need: ASCENDING, DESCENDING,
     * or NONE (NONE is represented by "RowOrdering.DONTCARE" and is used
     * for cases where all necessary sorting occurred at compilation time).
     */
    private int sortRequired;

    /**
     * Constructor.  Just save off the relevant probing state and pass
     * everything else up to TableScanResultSet.
     * 
     * @see ResultSetFactory#getMultiProbeTableScanResultSet
     * @exception StandardException thrown on failure to open
     */
    MultiProbeTableScanResultSet(long conglomId,
        StaticCompiledOpenConglomInfo scoci, Activation activation, 
        GeneratedMethod resultRowAllocator, 
        int resultSetNumber,
        GeneratedMethod startKeyGetter, int startSearchOperator,
        GeneratedMethod stopKeyGetter, int stopSearchOperator,
        boolean sameStartStopPosition,
        Qualifier[][] qualifiers,
        DataValueDescriptor [] probingVals,
        int sortRequired,
        String tableName,
        String userSuppliedOptimizerOverrides,
        String indexName,
        boolean isConstraint,
        boolean forUpdate,
        int colRefItem,
        int indexColItem,
        int lockMode,
        boolean tableLocked,
        int isolationLevel,
        boolean oneRowScan,
        double optimizerEstimatedRowCount,
        double optimizerEstimatedCost)
            throws StandardException
    {
        /* Note: We use '1' as rows per read because we do not currently
         * allow bulk fetching when multi-probing.  If that changes in
         * the future then we will need to update rowsPerRead accordingly.
         */
        super(conglomId,
            scoci,
            activation,
            resultRowAllocator,
            resultSetNumber,
            startKeyGetter,
            startSearchOperator,
            stopKeyGetter,
            stopSearchOperator,
            sameStartStopPosition,
            qualifiers,
            tableName,
            userSuppliedOptimizerOverrides,
            indexName,
            isConstraint,
            forUpdate,
            colRefItem,
            indexColItem,
            lockMode,
            tableLocked,
            isolationLevel,
            1, // rowsPerRead
            oneRowScan,
            optimizerEstimatedRowCount,
            optimizerEstimatedCost);

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(
                (probingVals != null) && (probingVals.length > 0),
                "No probe values found for multi-probe scan.");
        }

        this.origProbeValues = probingVals;
        this.sortRequired = sortRequired;
    }

    /**
     * @see NoPutResultSet#openCore
     */
    public void openCore() throws StandardException
    {
        /* If the probe values are not already sorted then sort them now.  This
         * allows us to skip over duplicate probe values (otherwise we could
         * end up with duplicate rows in the result set).
         *
         * Note: If all of the probe values were provided as constants then we
         * sorted them at compile time (during preprocessing) so we don't have
         * to do it now. But if one or more was specified as a param then we
         * have to do the sort here, at execution time, because this is the
         * only point at which we know what values the parameters have.
         */
        if (sortRequired == RowOrdering.DONTCARE)
        {
            /* DONTCARE really means that the values are already sorted
             * in ascending order, and that's good enough.
             */
            probeValues = origProbeValues;
        }
        else
        {
            /* RESOLVE: For some reason sorting the probeValues array
             * directly leads to incorrect parameter value assignment when
             * executing a prepared statement multiple times.  Need to figure
             * out why (maybe related to DERBY-827?).  In the meantime, if
             * we're going to sort the values we use clones.  This is not
             * ideal, but it works for now.
             */
            DataValueDescriptor [] pVals =
                new DataValueDescriptor[origProbeValues.length];

            for (int i = 0; i < pVals.length; i++)
                pVals[i] = origProbeValues[i].getClone();

            if (sortRequired == RowOrdering.ASCENDING)
                java.util.Arrays.sort(pVals);
            else
            {
                // Sort the values in DESCENDING order.
                java.util.Arrays.sort(
                    pVals, java.util.Collections.reverseOrder());
            }

            probeValues = pVals;
        }

        probeValIndex = 0;
        super.openCore();
    }

    /**
     * Open the scan controller
     *
     * @param tc transaction controller; will open one if null.
     * @exception StandardException thrown on failure to open
     */
    protected void openScanController(TransactionController tc)
        throws StandardException
    {
        /* If we're opening the scan controller for the first time then
         * we want to use the first value in the (now sorted) list as
         * the start/stop key.  That's what we pass in here.
         */
        openScanController(tc, probeValues[0]);

        /* probeValIndex should be the index of the *next* value to
         * use.  Since we just positioned ourselves at the 0th probe
         * value with the above call, the next value we want is the
         * one at index "1".
         */
        probeValIndex = 1;
    }

    /**
     * @see NoPutResultSet#reopenCore
     */
    public void reopenCore() throws StandardException
    {
        /* There are two scenarios for which we reopen this kind of scan:
         *
         *   A - The first is for join processing.  In this case we have
         * a(nother) row from some outer table and we want to reopen this
         * scan to look for rows matching the new outer row.
         *
         *   B - The second is for multi-probing.  Here we want to reopen
         * the scan on this table to look for rows matching the next value
         * in the probe list.
         *
         * If we are reopening the scan for scenario A (join processing)
         * then we need to reset our position within the probe list. 
         * If we are reopening the scan for scenario B then we do *not*
         * want to reset our position within the probe list because that
         * position tells us where to find the next probe value.
         *
         * The way we tell the difference between the two scenarios is
         * by looking at our current position in the probe list (i.e. the
         * value of probeValIndex): if our current position is beyond the
         * length of the probe list then we know that we are reopening the
         * scan for scenario A.  Or put another away, we should never get
         * here for scenario B if probeValIndex is greater than or equal
         * to the length of the probe list.  The reason is that the call
         * to reopenCore() for scenario B will only ever happen when
         * moreInListVals() returns true--and in that case we know that
         * probeValIndex will be less than the length of the probeValues.
         */
        if (probeValIndex >= probeValues.length)
            probeValIndex = 0;

        super.reopenCore();
    }

    /**
     * Reopen the scan controller
     *
     * @exception StandardException thrown on failure to open
     */
    protected void reopenScanController() throws StandardException
    {
        /* If we're looking for the first value in the probe list, then
         * reset the row scan count.  Otherwise leave it unchanged since
         * we're just continuing an already-opened scan.  Note that we
         * have to do this check *before* we call getNextProbeValue()
         * because that method will increment probeValIndex.
         */
        if (probeValIndex == 0)
            rowsThisScan = 0;

        DataValueDescriptor pv = null;
        if (moreInListVals())
        {
            pv = getNextProbeValue();
            if (pv == null)
            {
                /* We'll get here when we've exhausted the probe list. In
                 * that case leave the scan as it is, which effectively
                 * means we are done.
                 */
                return;
            }
        }

        reopenScanController(pv);
    }

    /**
     * Return the next row (if any) from the scan (if open).
     *
     * More specifically we do the following:
     *
     *  1 - See if we have a row to read from the current scan position.
     *    If so, return that row (done).
     *
     *  2 - If there are no more rows to read from the current scan
     *    position AND if there are more probe values to look at,
     *    then a) reposition the scan using the next probe value
     *    as the start/stop key and b) go back to step 1.  Otherwise
     *    proceed to step 3.
     *    
     *  3 - Return null (no more rows).
     *
     * Note that step 1 is important for cases where multiple rows in this
     * table match a single probe value.  In such a scenario we have to
     * be sure that we do *not* move on to the next probe value until
     * we have returned all of the rows for the _current_ probe value.
     *
     * @exception StandardException thrown on failure to get next row
     */
    public ExecRow getNextRowCore() throws StandardException
    {
        checkCancellationFlag();

        // Step 1.
        ExecRow result = super.getNextRowCore();

        // Steps 2, 1, 2, 1, 2, ...
        while ((result == null) && moreInListVals())
        {
            /* Repositioning the scan (if needed) is simply a matter of
             * reopening the core scan again. As part of that method we will
             * figure out what the next probe value should be (and thus
             * where to position the scan).
             */
            reopenCore();
            result = super.getNextRowCore();
        }

        // Step 3: result will be null if there are no more rows.
        return result;
    }

    /**
     * @see NoPutResultSet#close
     */
    public void close() throws StandardException
    {
        /* We'll let TableScanResultSet track the time it takes to close up,
         * so no timing here.
         */
        super.close();

        /* Note: We can't set probeValues == null here because we may end
         * up reopening this scan again later, in which case we'll need the
         * list of probe values.
         */
    }

    /**
     * Figure out whether or not we can (re-)position the scan
     * controller based on the next value in probeValues.  This
     * will return false when we have exhausted the probe list
     * (i.e. when we've gone through all of the values).
     */
    private boolean moreInListVals()
    {
        return (probeValIndex < probeValues.length);
    }

    /**
     * Return the next non-duplicate value from the probe list.
     * Assumption is that the list is sorted so that duplicates
     * appear next to each other, and that probeValIndex is the
     * index of the next value. If we've exhausted the probe list
     * then just return null.
     */
    private DataValueDescriptor getNextProbeValue()
        throws StandardException
    {
        int ctr = probeValIndex;

        // Skip over duplicate values.
        while ((ctr > 0) && (ctr < probeValues.length) &&
            probeValues[probeValIndex-1].equals(probeValues[ctr]))
        {
            ctr++;
        }

        probeValIndex = ctr;
        if (probeValIndex < probeValues.length)
            return probeValues[probeValIndex++];

        return null;
    }
}
