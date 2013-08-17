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

import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.compile.RowOrdering;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
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
     * Tells whether or not we should skip the next attempt to (re)open the
     * scan controller. If it is {@code true} it means that the previous call
     * to {@link #initStartAndStopKey()} did not find a new probe value, which
     * means that the probe list is exhausted and we shouldn't perform a scan.
     */
    private boolean skipNextScan;

    /**
     * Constructor.  Just save off the relevant probing state and pass
     * everything else up to TableScanResultSet.
     * 
     * @see ResultSetFactory#getMultiProbeTableScanResultSet
     * @exception StandardException thrown on failure to open
     */
    MultiProbeTableScanResultSet(long conglomId,
        StaticCompiledOpenConglomInfo scoci, Activation activation, 
        int resultRowTemplate,
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
            resultRowTemplate,
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
                pVals[i] = origProbeValues[i].cloneValue(false);

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
     * @see NoPutResultSet#reopenCore
     */
    public void reopenCore() throws StandardException
    {
        reopenCore(false);
    }

    /**
     * There are two scenarios for which we reopen this kind of scan:
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
     * That said, this method does the work of reopenCore() using
     * the received boolean to determine which of the two scenarios
     * we are in.  Note that if our current position (i.e. the value
     * of probeValIndex) is beyond the length of the probe list then
     * we know that we are reopening the scan for scenario A.  Or put
     * another away, we should never get here for scenario B if
     * probeValIndex is greater than or equal to the length of the
     * probe list.  The reason is that the call to reopenCore() for
     * scenario B will only ever happen when moreInListVals() returns
     * true--and in that case we know that probeValIndex will be less
     * than the length of the probeValues.  But the opposite is not
     * true: i.e. it is *not* safe to say that a probeValIndex which
     * is less than the length of probe list is always for scenario
     * B.  That's not true because it's possible that the join to
     * which this scan belongs is a "oneRowRightSide" join, meaning
     * that this, the "right" side scan, will be "interrupted" after
     * we return a single row for the current outer row.  If we then
     * come back with a new outer row we need to reset our position--
     * even though probeValIndex will be less than probeValues.length
     * in that case.  DERBY-3603.
     */
    private void reopenCore(boolean forNextProbe) throws StandardException
    {
        if (!forNextProbe)
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
        // TableScanResultSet.reopenScanController() will reset rowsThisScan
        // because it thinks this is a completely new scan. However, we want
        // it to reflect the total number of rows seen in the multi-probe
        // scan, so we keep the original value and restore it after reopening
        // the controller. Instead, we reset rowsThisScan to 0 each time
        // initStartAndStopKey() is called on the first probe value.
        long rows = rowsThisScan;
        super.reopenScanController();
        rowsThisScan = rows;
    }

    /**
     * Initialize the start key and the stop key used in the scan. Both keys
     * will be set to the probe value. If no new probe value was found (the
     * probe list was exhausted), the flag skipNextScan will be {@code true}
     * when the method returns to prevent a new scan from being reopened with
     * a missing or incorrect probe value.
     */
    void initStartAndStopKey() throws StandardException {

        // Make sure the fields are initialized with a placeholder.
        // startPosition and stopPosition will always be non-null in a
        // MultiProbeTableScanResultSet, and they will always be initialized
        // to the first value in the probe list. They will be changed to
        // the actual probe value later in this method.
        super.initStartAndStopKey();

        /* If we're looking for the first value in the probe list, then
         * reset the row scan count.  Otherwise leave it unchanged since
         * we're just continuing an already-opened scan.  Note that we
         * have to do this check *before* we call getNextProbeValue()
         * because that method will increment probeValIndex.
         */
        if (probeValIndex == 0)
            rowsThisScan = 0;

        DataValueDescriptor[] startPositionRow = startPosition.getRowArray();
        DataValueDescriptor[] stopPositionRow = stopPosition.getRowArray();

        DataValueDescriptor probeValue = getNextProbeValue();

		/* If we have a probe value then we do the "probe" by positioning
		 * the scan at the first row matching the value.  The way to do
		 * that is to use the value as a start key, which is what will
		 * happen if we plug it into first column of "startPositionRow".
		 * So in this case startPositionRow[0] functions as a "place-holder"
		 * for the probe value.  The same goes for stopPositionRow[0].
		 *
		 * Note that it *is* possible for a start/stop key to contain more
		 * than one column (ex. if we're scanning a multi-column index). In
		 * that case we plug probeValue into the first column of the start
		 * and/or stop key and leave the rest of the key as it is.  As an
		 * example, assume we have the following predicates:
		 *
		 *    ... where d in (1, 20000) and b > 200 and b <= 500
		 *
		 * And assume further that we have an index defined on (d, b).
		 * In this case it's possible that we have TWO start predicates
		 * and TWO stop predicates: the IN list will give us "d = probeVal",
		 * which is a start predicate and a stop predicate; then "b > 200"
		 * may give us a second start predicate, while "b <= 500" may give
		 * us a second stop predicate.  So in this situation we want our
		 * start key to be:
		 *
		 *    (probeValue, 200)
		 *
		 * and our stop key to be:
		 *
		 *    (probeValue, 500).
		 *
		 * This will effectively limit the scan so that it only returns
		 * rows whose "D" column equals probeValue and whose "B" column
		 * falls in the range of 200 thru 500.
		 *
		 * Note: Derby currently only allows a single start/stop predicate
		 * per column. See PredicateList.orderUsefulPredicates().
		 */
        if (probeValue != null) {
            startPositionRow[0] = probeValue;
            if (!sameStartStopPosition) {
                stopPositionRow[0] = startPositionRow[0];
            }
        }

        // If we didn't find a new probe value, the probe list is exhausted,
        // and we shouldn't open a new scan. skipScan() will detect this and
        // prevent (re)openScanController() from being called.
        skipNextScan = (probeValue == null);
    }

    /**
     * Check if the scan should be skipped. It should be skipped if (1)
     * {@link #initStartAndStopKey()} exhausted the probe list, or (2) the scan
     * should return no results because of nulls in the start key or stop key.
     * See {@link NoPutResultSetImpl#skipScan(ExecIndexRow,ExecIndexRow)} for
     * details about (2).
     *
     * @param startPosition the key on which to start the scan
     * @param stopPosition the key on which to stop the scan
     * @return {@code true} if scan should be skipped, {@code false} otherwise
     */
    protected boolean skipScan(
            ExecIndexRow startPosition, ExecIndexRow stopPosition)
		throws StandardException
    {
        return skipNextScan || super.skipScan(startPosition, stopPosition);
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
		if( isXplainOnlyMode() )
			return null;

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
            reopenCore(true);
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
