/*

   Derby - Class org.apache.derby.impl.sql.execute.ValidateCheckConstraintResultSet

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

import org.apache.derby.iapi.error.ExceptionUtil;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * Special result set used when checking deferred CHECK constraints.  Activated
 * by a special {@code --DERBY_PROPERTY validateCheckConstraint=<conglomId>}
 * override on a SELECT query, cf DeferredConstraintsMemory#validateCheck.  It
 * relies on having a correct row location set prior to invoking {@code
 * getNewtRowCore}, cf. the special code path in
 * {@code ProjectRestrictResultSet#getNextRowCore} activated by
 * {@code #validatingCheckConstraint}.
 *
 */
final class ValidateCheckConstraintResultSet extends TableScanResultSet
    implements CursorResultSet, Cloneable
{

    ValidateCheckConstraintResultSet(long conglomId,
        StaticCompiledOpenConglomInfo scoci,
        Activation activation,
        int resultRowTemplate,
        int resultSetNumber,
        GeneratedMethod startKeyGetter, int startSearchOperator,
        GeneratedMethod stopKeyGetter, int stopSearchOperator,
        boolean sameStartStopPosition,
        Qualifier[][] qualifiers,
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
        int rowsPerRead,
        boolean oneRowScan,
        double optimizerEstimatedRowCount,
        double optimizerEstimatedCost)
            throws StandardException
    {
        super(conglomId,
                scoci,
                activation,
                resultRowTemplate,
                resultSetNumber,
                startKeyGetter, startSearchOperator,
                stopKeyGetter, stopSearchOperator,
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
                rowsPerRead,
                oneRowScan,
                optimizerEstimatedRowCount,
                optimizerEstimatedCost);
    }

    /**
     * Return the current row (if any) from the base table scan, positioned
     * correctly by our caller (ProjectRestrictNode). It overrides
     * getNextRowCore from TableSCanResultSet, by using "fetch" instead of
     * "fetchNext" on the underlying controller, otherwise it's identical.
     * (This means it's probably over-general for the usage we have of it,
     * but it felt safer to keep the code as similar as possible.)
     * @return the row retrieved
     * @exception StandardException thrown on failure to get next row
     */
    @Override
    public ExecRow getNextRowCore() throws StandardException    {
        if (isXplainOnlyMode()) {
            return null;
        }

        checkCancellationFlag();

        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(scanRepositioned);
        }

        if (currentRow == null || scanRepositioned) {
            currentRow = getCompactRow(candidate, accessedCols, isKeyed);
        }

        beginTime = getCurrentTimeMillis();

        ExecRow result = null;

        if (isOpen  && !nextDone) {
            // Only need to do 1 next per scan for 1 row scans.
            nextDone = oneRowScan;

            if (scanControllerOpened) {
                boolean moreRows = true;

                while (moreRows) {
                    try {
                        scanController.fetch(candidate.getRowArray());
                    } catch (StandardException e) {
                        // Offending rows may have been deleted in the
                        // transaction.  As for compress, we won't even get here
                        // since we use a normal SELECT query then.
                        if (e.getSQLState().equals(
                                ExceptionUtil.getSQLStateFromIdentifier(
                                        SQLState.AM_RECORD_NOT_FOUND))) {
                            moreRows = false;
                            break;
                        } else {
                            throw e;
                        }
                    }

                    rowsSeen++;
                    rowsThisScan++;

                    /*
                    ** Skip rows where there are start or stop positioners
                    ** that do not implement ordered null semantics and
                    ** there are columns in those positions that contain
                    ** null.
                    ** No need to check if start and stop positions are the
                    ** same, since all predicates in both will be ='s,
                    ** and hence evaluated in the store.
                    */
                    if ((! sameStartStopPosition) && skipRow(candidate)) {
                        rowsFiltered++;
                        continue;
                    }

                    /* beetle 3865, updateable cursor use index. If we have a
                     * hash table that holds updated records, and we hit it
                     * again, skip it, and remove it from hash since we can't
                     * hit it again, and we have a space in hash, so can stop
                     * scanning forward.
                     */
                    if (past2FutureTbl != null) {
                        RowLocation rowLoc = (RowLocation)currentRow.getColumn(
                            currentRow.nColumns());
                        if (past2FutureTbl.remove(rowLoc) != null) {
                            continue;
                        }
                    }

                    result = currentRow;
                    break;
                }

                /*
                ** If we just finished a full scan of the heap, update
                ** the number of rows in the scan controller.
                **
                ** NOTE: It would be more efficient to only update the
                ** scan controller if the optimizer's estimated number of
                ** rows were wrong by more than some threshold (like 10%).
                ** This would require a little more work than I have the
                ** time for now, however, as the row estimate that is given
                ** to this result set is the total number of rows for all
                ** scans, not the number of rows per scan.
                */
                if (! moreRows) {
                    setRowCountIfPossible(rowsThisScan);
                    currentRow = null;
                }
            }
        }

        setCurrentRow(result);
        currentRowIsValid = true;
        scanRepositioned = false;
        qualify = true;

        nextTime += getElapsedMillis(beginTime);
        return result;
    }
}
