/*

   Derby - Class org.apache.derby.impl.sql.execute.WindowResultSet

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

import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.iapi.sql.execute.ExecRowBuilder;

/**
 * WindowResultSet
 *
 * This ResultSet handles a window function ResultSet.
 *
 * The ResultSet is opened using openCore().  Each row is fetched and any
 * restrictions evaluated for each row in a do-while loop in getNextRowCore().
 * The ResultSet is closed using closeCore().
 *
 */
class WindowResultSet extends NoPutResultSetImpl
{
    private GeneratedMethod restriction = null;

    /**
     * Source result set,
     */
    public NoPutResultSet source = null;


    /**
     * Cumulative time needed to evaluate any restriction on this result set.
     */
    public long restrictionTime;

    private FormatableBitSet referencedColumns;
    private ExecRow allocatedRow;
    private long rownumber;

    /**
     *  Constructor
     *
     *  @param  activation          The activation
     *  @param  source              Source result set
     *  @param  rowAllocator
     *  @param  resultSetNumber     The resultSetNumber
     *  @param  erdNumber           Int for ResultDescription
	                                (so it can be turned back into an object)
     *  @param  restriction         Restriction
     *  @param  optimizerEstimatedRowCount  The optimizer's estimated number
     *                                      of rows.
     *  @param  optimizerEstimatedCost      The optimizer's estimated cost
     */

    WindowResultSet(Activation activation,
        NoPutResultSet         source,
        int                    rowAllocator,
        int                    resultSetNumber,
        int                    erdNumber,
        GeneratedMethod        restriction,
        double                 optimizerEstimatedRowCount,
        double                 optimizerEstimatedCost)
//IC see: https://issues.apache.org/jira/browse/DERBY-6003
      throws StandardException
    {

        super(activation,
            resultSetNumber,
            optimizerEstimatedRowCount,
            optimizerEstimatedCost);

        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(activation != null,
                                 "activation expected to be non-null");
            SanityManager.ASSERT(resultSetNumber >= 0,
                                 "resultSetNumber expected to be >= 0");
        }

        this.restriction = restriction;
        this.source = source;
        this.rownumber = 0;

        ExecPreparedStatement ps = activation.getPreparedStatement();
//IC see: https://issues.apache.org/jira/browse/DERBY-6003

        this.allocatedRow = ((ExecRowBuilder) ps.getSavedObject(rowAllocator))
                .build(activation.getExecutionFactory());

        if (erdNumber != -1) {
            this.referencedColumns =
                (FormatableBitSet) ps.getSavedObject(erdNumber);
        }

        recordConstructorTime();
    }


    /**
     * Open this ResultSet.
     *
     * @exception StandardException thrown if cursor finished.
     */
    public void openCore() throws StandardException {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(!isOpen,
                "WindowResultSet already open");
        }

        beginTime = getCurrentTimeMillis();

        /* Call into the source openCore() */
        source.openCore();

        isOpen = true;
        rownumber = 0;
        numOpens++;
        openTime += getElapsedMillis(beginTime);
    }

    /**
     * Reopen this ResultSet.
     *
     * @exception StandardException thrown if cursor finished.
     */
    public void reopenCore() throws StandardException {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(isOpen, "WindowResultSet already open");
        }

        beginTime = getCurrentTimeMillis();

        /* Reopen the source */
        source.reopenCore();

        rownumber = 0;
        numOpens++;
        openTime += getElapsedMillis(beginTime);
    }

    /**
     * Return the requested values computed from the next row (if any) for which
     * the restriction evaluates to true.
     * <p>
     * Restriction and projection parameters are evaluated for each row.
     *
     * @exception StandardException thrown on failure.
     * @exception StandardException ResultSetNotOpen thrown if not yet open.
     *
     * @return the next row in the result
     */
    public ExecRow getNextRowCore() throws StandardException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6216
		if( isXplainOnlyMode() )
			return null;

        ExecRow sourceRow = null;
        ExecRow retval = null;
        boolean restrict = false;
        DataValueDescriptor restrictBoolean;
        long beginRT = 0;

        beginTime = getCurrentTimeMillis();

        if (!isOpen) {
            throw StandardException.newException(
                SQLState.LANG_RESULT_SET_NOT_OPEN, "next");
        }

        /*
         * Loop until we get a row from the source that qualifies, or there are
         * no more rows to qualify. For each iteration fetch a row from the
         * source, and evaluate against the restriction if any.
         */
        ExecRow tmpRow = null;

        do {
            sourceRow = source.getNextRowCore();

            if (sourceRow != null) {
                this.rownumber++;
                tmpRow = getAllocatedRow();
                populateFromSourceRow(sourceRow, tmpRow);
                setCurrentRow(tmpRow);

                /* Evaluate any restrictions */
                restrictBoolean = (DataValueDescriptor) ((restriction == null) ?
                                    null : restriction.invoke(activation));

                restrictionTime += getElapsedMillis(beginRT);

                // if the result is null, we make it false --
                // so the row won't be returned.
                restrict = (restrictBoolean == null) ||
                    ((!restrictBoolean.isNull()) &&
                    restrictBoolean.getBoolean());

                if (!restrict) {
                    rowsFiltered++;
                    clearCurrentRow();
                }

                /* Update the run time statistics */
                rowsSeen++;
                retval = currentRow;
            } else {
                clearCurrentRow();
                retval = null;
            }
        } while ((sourceRow != null) && (!restrict));

        nextTime += getElapsedMillis(beginTime);
        return retval;
    }

    /**
     * If the result set has been opened, close the open scan, else throw.
     *
     * @exception StandardException thrown on error
     */
    public void close() throws StandardException {
        beginTime = getCurrentTimeMillis();

        if (isOpen) {
            clearCurrentRow();

            /*
             * Make sure to close the source
             */
            source.close();
            super.close();

        } else if (SanityManager.DEBUG) {
            SanityManager.DEBUG("CloseRepeatInfo",
                                "Close of WindowResultSet repeated");
        }

        closeTime += getElapsedMillis(beginTime);
    }



    /**
     * Copy columns from srcrow into destrow, or insert ROW_NUMBER.
     * <p/>
     * <b>FIXME</b>
     * This is temporary. Window function treatment needs to generalized to
     * work for other window functions.
     *
     * @exception StandardException thrown on failure to open
     */
    public void populateFromSourceRow(ExecRow srcrow, ExecRow destrow)
        throws StandardException
    {
        int srcindex = 1;

        try {
            DataValueDescriptor[] columns = destrow.getRowArray();
            for (int index = 0; index < columns.length; index++) {

                if (referencedColumns != null &&
                        !referencedColumns.get(index)) {
                    columns[index].setValue((long)this.rownumber);
                } else {
                    destrow.setColumn(index+1, srcrow.getColumn(srcindex));
                    srcindex++;
                }
            }
        } catch (StandardException se) {
            throw se;
        } catch (Throwable t) {
            throw StandardException.unexpectedUserException(t);
        }
    }

    /**
     * Return the total amount of time spent in this ResultSet
     *
     * @param type  CURRENT_RESULTSET_ONLY - time spent only in this ResultSet
     *          ENTIRE_RESULTSET_TREE  - time spent in this ResultSet and below.
     *
     * @return long     The total amount of time spent (in milliseconds).
     */
    public long getTimeSpent(int type) {
        long totTime = constructorTime + openTime + nextTime + closeTime;

        if (type == NoPutResultSet.CURRENT_RESULTSET_ONLY) {
            return totTime - source.getTimeSpent(ENTIRE_RESULTSET_TREE);
        } else {
            return totTime;
        }
    }

    /**
     * Cache the ExecRow for this result set.
     *
     * @return The cached ExecRow for this ResultSet
     *
     * @exception StandardException thrown on failure.
     */
    private ExecRow getAllocatedRow() throws StandardException {
        return allocatedRow;
    }
}
