/*

   Derby - Class org.apache.derby.impl.sql.execute.UniqueIndexSortObserver

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
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.BackingStoreHashtable;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * Unique index aggregator.  Enforces uniqueness when
 * creating a unique index or constraint.
 *
 */
class UniqueIndexSortObserver extends BasicSortObserver 
{
    private final boolean                   deferrable;
    private final boolean                   deferred;
    private final String                    indexOrConstraintName;
    private final String                    tableName;
    private final LanguageConnectionContext lcc;
    private final UUID                      constraintId;
    private BackingStoreHashtable           deferredDuplicates;

    public UniqueIndexSortObserver(
            LanguageConnectionContext lcc,
            UUID constraintId,
            boolean doClone,
            boolean deferrable,
            boolean deferred,
            String indexOrConstraintName,
            ExecRow execRow,
            boolean reuseWrappers,
            String tableName)
	{
        super(doClone, !deferred, execRow, reuseWrappers);
        this.lcc = lcc;
        this.constraintId = constraintId;
        this.deferrable = deferrable;
        this.deferred = deferred;
		this.indexOrConstraintName = indexOrConstraintName;
		this.tableName = tableName;
	}

    @Override
	public DataValueDescriptor[] insertDuplicateKey(
    DataValueDescriptor[]   in, 
    DataValueDescriptor[]   dup)
		throws StandardException
	{
		StandardException se = null;
		se = StandardException.newException(
				SQLState.LANG_DUPLICATE_KEY_CONSTRAINT, indexOrConstraintName, tableName);
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
