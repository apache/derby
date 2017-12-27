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

import org.apache.derby.shared.common.error.ExceptionUtil;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;

/**
 * Special result set used when checking deferred CHECK constraints.  Activated
 * by a special {@code --DERBY_PROPERTY validateCheckConstraint=<baseTableUUIDString>}
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

    @Override
    boolean loopControl(boolean moreRows) throws StandardException {
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
             } else {
                 throw e;
             }
         }
         return moreRows;
    }
}
