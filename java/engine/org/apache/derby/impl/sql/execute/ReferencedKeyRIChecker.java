/*

   Derby - Class org.apache.derby.impl.sql.execute.ReferencedKeyRIChecker

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

import java.util.Enumeration;
import org.apache.derby.catalog.UUID;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.StatementType;
import org.apache.derby.iapi.sql.StatementUtil;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.BackingStoreHashtable;
import org.apache.derby.iapi.store.access.KeyHasher;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLLongint;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * A Referential Integrity checker for a change
 * to a referenced key (primary or unique).   Makes
 * sure that all the referenced key row is not
 * referenced by any of its foreign keys.  see 
 * ForeignKeyRIChecker for the code that validates
 * changes to foreign keys.
 */
public class ReferencedKeyRIChecker extends GenericRIChecker
{
    private ScanController refKeyIndexScan = null;

    /**
     * Key mapping used when storing referenced (PK, unique) keys under
     * deferred row processing and deferred key constraint (PK, unique).
     */
    private final DataValueDescriptor[] refKey =
            new DataValueDescriptor[numColumns];

    /**
     * We save away keys with a counter in this hash table, so we know how many
     * instances of a key (duplicates) have been deleted/modified, cf usage
     * in {@link #postCheck()}. Initialized on demand.
     */
    private BackingStoreHashtable deletedKeys = null;

	/**
     * @param lcc       the language connection context
	 * @param tc		the xact controller
	 * @param fkinfo	the foreign key information 
	 *
	 * @exception StandardException		Thrown on failure
	 */
    ReferencedKeyRIChecker(LanguageConnectionContext lcc,
                           TransactionController tc,
                           FKInfo fkinfo) throws StandardException
	{
        super(lcc, tc, fkinfo);

		if (SanityManager.DEBUG)
		{
			if (fkInfo.type != FKInfo.REFERENCED_KEY)
			{
				SanityManager.THROWASSERT("invalid type "+fkInfo.type+
					" for a ReferencedKeyRIChecker");
			}
		} 
	}

	/**
	 * Check that the row either has a null column(s), or
	 * has no corresponding foreign keys.
	 * <p> 
	 * If a foreign key is found, an exception is thrown.
	 * If not, the scan is closed.
	 *
     * @param a     the activation
	 * @param row	the row to check
     * @param restrictCheckOnly
     *              {@code true} if the check is relevant only for RESTRICTED
     *              referential action.
     * @param deferredRowReq
     *              For referenced keys: The required number of duplicates that
     *              need to be present. Only used if {@code postCheck==false}.
     *
	 * @exception StandardException on unexpected error, or
	 *		on a primary/unique key violation
	 */
    @Override
    void doCheck(Activation a,
                 ExecRow row,
                 boolean restrictCheckOnly,
                 int deferredRowReq) throws StandardException
	{
		/*
		** If any of the columns are null, then the
		** check always succeeds.
		*/
		if (isAnyFieldNull(row))
		{
			return;
		}

        if (fkInfo.refConstraintIsDeferrable) {
            // We may have more than one row if the referenced constraint is
            // deferred, if so, all is good: no foreign key constraints can be
            // violated. DERBY-6559
            if (lcc.isEffectivelyDeferred(
                    lcc.getCurrentSQLSessionContext(a),
                    fkInfo.refConstraintID)) {
                if (restrictCheckOnly) {
                    rememberKey(row);
                    return;
                } else {
                    // It *is* a deferred constraint and it is *not* a deferred
                    // rows code path, so go see if we have enough rows
                    if (isDuplicated(row, deferredRowReq)) {
                        return;
                    }
                }
            }
        }

		/*
		** Otherwise, should be no rows found.
	 	** Check each conglomerate.
		*/
		ScanController scan;

		for (int i = 0; i < fkInfo.fkConglomNumbers.length; i++)
		{
			
			if(restrictCheckOnly)
			{
				if(fkInfo.raRules[i] != StatementType.RA_RESTRICT)
					continue;
			}

			scan = getScanController(fkInfo.fkConglomNumbers[i], fkScocis[i], fkDcocis[i], row);
			if (scan.next())
			{
				close();

                final UUID fkId = fkInfo.fkIds[i];

                // Only considering deferring if we don't have RESTRICT, i.e.
                // NO ACTION. CASCADE and SET NULL handled elsewhere.
                if (fkInfo.deferrable[i] &&
                    fkInfo.raRules[i] != StatementType.RA_RESTRICT &&
                        lcc.isEffectivelyDeferred(
                                lcc.getCurrentSQLSessionContext(a), fkId)) {
                    deferredRowsHashTable =
                            DeferredConstraintsMemory.rememberFKViolation(
                                    lcc,
                                    deferredRowsHashTable,
                                    fkInfo.fkIds[i],
                                    indexQualifierRow.getRowArray(),
                                    fkInfo.schemaName,
                                    fkInfo.tableName);
                } else {

                    StandardException se = StandardException.newException(
                            SQLState.LANG_FK_VIOLATION,
                            fkInfo.fkConstraintNames[i],
                            fkInfo.tableName,
                            StatementUtil.typeName(fkInfo.stmtType),
                            RowUtil.toString(row, fkInfo.colArray));

                    throw se;
                }
			}
			/*
			** Move off of the current row to release any locks.
			*/
			scan.next();
		}
	}


    /**
     * Remember the deletion of this key, it may cause a RESTRICT
     * foreign key violation, cf. logic in @{link #postCheck}.
     * @param rememberRow
     * @throws StandardException
     */
    private void rememberKey(ExecRow rememberRow) throws StandardException {
        if (deletedKeys == null) {
            // key: all columns (these are index rows, or a row containing a
            // row location)
            identityMap = new int[numColumns];

            for (int i = 0; i < numColumns; i++) {
                identityMap[i] = i;
            }

            deletedKeys = new BackingStoreHashtable(
                    tc,
                    null,
                    identityMap,
                    true, // remove duplicates: no need for more copies:
                    // one is enough to know what to look for on commit
                    -1,
                    HashScanResultSet.DEFAULT_MAX_CAPACITY,
                    HashScanResultSet.DEFAULT_INITIAL_CAPACITY,
                    HashScanResultSet.DEFAULT_MAX_CAPACITY,
                    false,
                    false);

        }

        DataValueDescriptor[] row = rememberRow.getRowArray();
        for (int i = 0; i < numColumns; i++) {
            refKey[i] = row[fkInfo.colArray[i] - 1];
        }

        Object hashKey = KeyHasher.buildHashKey(refKey, identityMap);

        DataValueDescriptor[] savedRow =
                (DataValueDescriptor[])deletedKeys.remove(hashKey);

        if (savedRow == null) {
            savedRow = new DataValueDescriptor[numColumns + 1];
            System.arraycopy(refKey, 0, savedRow, 0, numColumns);
            savedRow[numColumns] = new SQLLongint(1);
        } else {
            savedRow[numColumns] = new SQLLongint(
                ((SQLLongint)savedRow[numColumns]).getLong() + 1);
        }

        deletedKeys.putRow(false, savedRow, null);
    }

    /**
     * Check that we have at least one more row in the referenced
     * table table containing a key than the number of projected deletes of that
     * key. Only used when the referenced constraint id deferred and with
     * RESTRICT mode
     *
     * @throws StandardException Standard error policy
     */
    public void postCheck() throws StandardException
    {
        if (!fkInfo.refConstraintIsDeferrable) {
            return;
        }

        int indexOfFirstRestrict = -1;

        for (int i = 0; i < fkInfo.fkConglomNumbers.length; i++) {
            if (fkInfo.raRules[i] == StatementType.RA_RESTRICT) {
                indexOfFirstRestrict = i;
                break;
            }
        }

        if (indexOfFirstRestrict == -1) {
            return;
        }

        if (deletedKeys != null) {
            final Enumeration<?> e = deletedKeys.elements();

            while (e.hasMoreElements()) {
                final DataValueDescriptor[] row =
                        (DataValueDescriptor[])e.nextElement();
                final DataValueDescriptor[] key =
                        new DataValueDescriptor[row.length - 1];
                System.arraycopy(row, 0, key, 0, key.length);

                // The number of times this key is to be deleted,
                // we need at least one more if for Fk constraint to hold.
                final long requiredCount = row[row.length - 1].getLong() + 1;

                if (!isDuplicated(key, requiredCount)) {
                    int[] oneBasedIdentityMap = new int[numColumns];

                    for (int i = 0; i < numColumns; i++) {
                        // Column numbers are numbered from 1 and
                        // call to RowUtil.toString below expects that
                        // convention.
                        oneBasedIdentityMap[i] = i + 1;
                    }

                    StandardException se = StandardException.newException(
                            SQLState.LANG_FK_VIOLATION,
                            fkInfo.fkConstraintNames[indexOfFirstRestrict],
                            fkInfo.tableName,
                            StatementUtil.typeName(fkInfo.stmtType),
                            RowUtil.toString(row, oneBasedIdentityMap));
                    throw se;
                }
            }
        }
    }


    private boolean isDuplicated(ExecRow row, int deferredRowReq)
            throws StandardException {
        final DataValueDescriptor[] indexRowArray = row.getRowArray();

        for (int i = 0; i < numColumns; i++)
        {
            // map the columns into the PK form
            refKey[i] = indexRowArray[fkInfo.colArray[i] - 1];
        }

        return isDuplicated(refKey, deferredRowReq);
    }


    private boolean isDuplicated(DataValueDescriptor[] key, long deferredRowReq)
            throws StandardException {
        if (refKeyIndexScan == null) {
            refKeyIndexScan = tc.openScan(
                    fkInfo.refConglomNumber,
                    false,                  // no hold over commit
                    0,                      // read only
                    TransactionController.MODE_RECORD,
                                            // record locking

                    // Use repeatable read here, since we rely on the row being
                    // present till we commit if we accept this row as being
                    // the one the upholds the constraint when we delete ours.
                    TransactionController.ISOLATION_REPEATABLE_READ,

                    (FormatableBitSet)null, // retrieve all fields
                    key,                    // startKeyValue
                    ScanController.GE,      // startSearchOp
                    null,                   // qualified
                    key,                    // stopKeyValue
                    ScanController.GT);     // stopSearchOp
        } else {
            refKeyIndexScan.reopenScan(
                      key,                  // startKeyValue
                      ScanController.GE,    // startSearchOp
                      null,                 // qualifier
                      key,                  // stopKeyValue
                      ScanController.GT);   // stopSearchOp
        }


        boolean foundRow = refKeyIndexScan.next();

        while (--deferredRowReq > 0 && foundRow) {
            foundRow =refKeyIndexScan.next();
        }

        if (deferredRowReq == 0 && foundRow) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Clean up all scan controllers and other resources
     *
     * @exception StandardException on error
     */
    @Override
    void close()
        throws StandardException {

        if (refKeyIndexScan != null) {
            refKeyIndexScan.close();
            refKeyIndexScan = null;
        }

        if (deletedKeys != null) {
            deletedKeys.close();
            deletedKeys = null;
        }

        identityMap = null;

        super.close();
    }

}





