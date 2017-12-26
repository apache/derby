/*
 
   Derby - Class org.apache.derby.impl.sql.execute.UniqueWithDuplicateNullsIndexSortObserver
 
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

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.BackingStoreHashtable;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * UniqueWithDuplicateNullsIndexSortObserver is implementation of BasicSortObserver for
 * eliminating non null duplicates from the backing index of unique constraint.
 * This class is implemented to check for special case of distinct sorting where
 * duplicate keys are allowed only if there is a null in the key part.
 */
public class UniqueWithDuplicateNullsIndexSortObserver extends BasicSortObserver {
    
    private final String                    indexOrConstraintName;
    private final String                    tableName;
    private final boolean                   deferrable;
    private final boolean                   deferred;
    private final LanguageConnectionContext lcc;
    private final UUID                      constraintId;
    private BackingStoreHashtable           deferredDuplicates;
    /**
     * Constructs an object of UniqueWithDuplicateNullsIndexSortObserver
     * 
     * @param lcc     Language Connection context
     * @param constraintId Id of the constraint (only used for
                      deferrable constraints)
     * @param doClone If true, then rows that are retained
     * 		by the sorter will be cloned.  This is needed
     * 		if language is reusing row wrappers.
     * @param deferrable deferrable constraint
     * @param deferred constraint mode is deferred
     * @param indexOrConstraintName name of index of constraint
     * @param execRow	ExecRow to use as source of clone for store.
     * @param reuseWrappers	Whether or not we can reuse the wrappers
     * @param tableName name of the table
     */
    public UniqueWithDuplicateNullsIndexSortObserver(
            LanguageConnectionContext lcc,
            UUID constraintId,
            boolean doClone,
            boolean deferrable,
            boolean deferred,
            String  indexOrConstraintName,
            ExecRow execRow,
            boolean reuseWrappers,
            String  tableName) {
        super(doClone, false, execRow, reuseWrappers);
        this.lcc = lcc;
        this.constraintId = constraintId;
        this.deferrable = deferrable;
        this.deferred = deferred;
        this.indexOrConstraintName = indexOrConstraintName;
        this.tableName = tableName;
    }
    
    /**
     * Methods to check if the duplicate key can be inserted or not. It throws 
     * exception if the duplicates has no null part in the key. 
     * @param in new key
     * @param dup the new key is duplicate of this key
     * @return DVD [] if there is at least one null in
     * the key else throws StandardException
     * @throws StandardException is the duplicate key has all non null parts
     */
    @Override
    public DataValueDescriptor[] insertDuplicateKey(DataValueDescriptor[] in,
            DataValueDescriptor[] dup) throws StandardException {
        for (int i = 0; i < in.length; i++) {
            if (in [i].isNull()) {
                return super.insertDuplicateKey(in, dup);
            }
        }
        StandardException se = null;
        se = StandardException.newException(
                SQLState.LANG_DUPLICATE_KEY_CONSTRAINT,
                indexOrConstraintName, tableName);
        throw se;
    }

    @Override
    public boolean deferred() {
        return deferred;
    }

    @Override
    public boolean deferrable() {
        return deferrable;
    }

    @Override
    public void rememberDuplicate(DataValueDescriptor[] row)
            throws StandardException {
        deferredDuplicates = DeferredConstraintsMemory.rememberDuplicate(
                lcc,
                deferredDuplicates,
                constraintId,
                row);
    }

}
