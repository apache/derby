/*

   Derby - Class org.apache.derby.impl.sql.execute.DeferredDuplicates

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
import java.util.HashMap;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.store.access.BackingStoreHashtable;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.shared.common.reference.SQLState;

public class DeferredDuplicates
{
    /**
     * Sometimes we need to save duplicate rows before we know the id
     * of the constraint index, so we assign the duplicates row a
     * temporary constraint id (UNDEFINED_CONGLOMERATE) and fix it up
     * later, see associateDuplicatesWithConglomerate.
     */
    public final static long UNDEFINED_CONGLOMERATE = -1;

    /**
     * Save the contents of an constraint supporting index row in a
     * hash table (which may spill to disk) for later checking,
     * typically on transaction commit, or upon request.
     *
     * @param tc        the transaction controller
     * @param indexCID  the identity of the index conglomerate which supports
     *                  the deferred constraint
     * @param deferredRowsHashTable
     *                  client cached value
     * @param lcc       the language connection context
     * @param insertRow the duplicate row to be saved in the hash table
     *                  for later checking
     * @return the hash table (for caching by client to minimize lookups)
     * @throws StandardException standard error policy
     */
    public static BackingStoreHashtable rememberDuplicate(
            TransactionController tc,
            long indexCID,
            BackingStoreHashtable deferredRowsHashTable,
            LanguageConnectionContext lcc,
            DataValueDescriptor[] insertRow) throws StandardException {

        // Don't copy the RowLocation, we don't need it:
        final int keyLength = insertRow.length -1 ;

        if (deferredRowsHashTable == null) {
            // Use the backing hashtable for this index's deferred rows
            // by checking the transaction's map of such backing hash
            // tables (one per index conglomerate).  Use it if it
            // exists, else make a new one.

            final HashMap<Long, BackingStoreHashtable> hashTables =
                    lcc.getDeferredHashTables();
            deferredRowsHashTable =
                    hashTables.get(Long.valueOf(indexCID));

            if (deferredRowsHashTable == null) {
                deferredRowsHashTable = makeHashTable(tc, keyLength);
                hashTables.put(
                        Long.valueOf(indexCID),
                        deferredRowsHashTable);
            }
        }

        DataValueDescriptor[] hashRowArray = new DataValueDescriptor[keyLength];
        System.arraycopy(insertRow, 0, hashRowArray, 0, keyLength);
        deferredRowsHashTable.putRow(true, hashRowArray, null);

        return deferredRowsHashTable;
    }

    /**
     * See javadoc for {@link #UNDEFINED_CONGLOMERATE}.
     *
     * @param lcc language connection context
     * @param indexCID the id of the index conglomerate supporting the
     *                 deferred constraint
     */
    public static void associateDuplicatesWithConglomerate(
            LanguageConnectionContext lcc,
            long indexCID) {

        final HashMap<Long, BackingStoreHashtable> hashTables =
                    lcc.getDeferredHashTables();

        if (hashTables == null) {
            return; // no duplicates recorded in this transaction
        }

        BackingStoreHashtable ht = hashTables.remove(
                Long.valueOf(UNDEFINED_CONGLOMERATE));

        if (ht != null) {
            hashTables.put(indexCID, ht);
        } // else no duplicates recorded for this index
    }

    /**
     * The information for deferred rows needs updating if the underlying
     * index is rebuilt, for example on bulk insert for import.
     * @param lcc the language connection context needed to find the
     *            deferred rows information if any
     * @param oldIndexCID the old id of the supporting index
     * @param newIndexCID the new id of the supporting index after recreation
     */
    public static void updateIndexCID(
            LanguageConnectionContext lcc,
            long oldIndexCID,
            long newIndexCID) {
        final HashMap<Long, BackingStoreHashtable> hashTables =
                    lcc.getDeferredHashTables();

        if (hashTables == null) {
            return; // no duplicates recorded in this transaction
        }

        BackingStoreHashtable ht = hashTables.remove(
                Long.valueOf(oldIndexCID));

        if (ht != null) {
            hashTables.put(Long.valueOf(newIndexCID), ht);
        }
    }

    private static BackingStoreHashtable makeHashTable(
            TransactionController tc,
            int cols) throws StandardException {

        // key: all columns (these are index rows)
        int[] keyCols = new int[cols];

        for (int i = 0; i < cols; i++) {
            keyCols[i] = i;
        }

        return new BackingStoreHashtable(
                tc,
                null,
                keyCols,
                true, // remove duplicates: no need for more copies:
                      // one is enough to know what to look for on commit
                -1,
                HashScanResultSet.DEFAULT_MAX_CAPACITY,
                HashScanResultSet.DEFAULT_INITIAL_CAPACITY,
                HashScanResultSet.DEFAULT_MAX_CAPACITY,
                false,
                false);
    }

    public static void validate(TransactionController tc,
                                long indexCID,
                                LanguageConnectionContext lcc,
                                BackingStoreHashtable ht,
                                boolean rollbackOnError)
            throws StandardException {

        Enumeration e = ht.elements();
        while (e.hasMoreElements()) {
            DataValueDescriptor[] key = (DataValueDescriptor[])e.nextElement();
            // FIXME: This is not very efficient: we could sort the rows in the
            // hash table, and then check all rows using a single scan.
            ScanController indexSC = null;
            boolean sawException = false;

            try {
                indexSC = tc.openScan(
                    indexCID,
                    false,
                    0, // read only
                    TransactionController.MODE_RECORD,
                    TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK,
                    (FormatableBitSet)null, // retrieve all fields
                    key,
                    ScanController.GE, // startSearchOp
                    null,
                    key,
                    ScanController.GT);

                if (indexSC.next()) {
                    if (indexSC.next()) {
                        // two matching rows found, constraint violated
                        final DataDictionary dd = lcc.getDataDictionary();
                        final ConglomerateDescriptor cd =
                                dd.getConglomerateDescriptor(indexCID);
                        final TableDescriptor td =
                                dd.getTableDescriptor(cd.getTableID());
                        final ConstraintDescriptor conDesc =
                                dd.getConstraintDescriptor(td, cd.getUUID());

                        throw StandardException.newException(
                            rollbackOnError ?
                                SQLState.LANG_DEFERRED_DUPLICATE_KEY_CONSTRAINT_T :
                                SQLState.LANG_DEFERRED_DUPLICATE_KEY_CONSTRAINT_S,
                            conDesc.getConstraintName(),
                            td.getName());
                    } // else exactly one row contains key: OK
                } else {
                    // No rows contain key: OK, must have been deleted later
                    // in transaction, or we got here due to pessimistic
                    // assumption on a timeout while checking on the insert.
                }
            } catch (StandardException se) {
                sawException = true;
                throw se;
            } finally {
                // Clean up resource usage
                try {
                    if (indexSC != null) {
                        indexSC.close();
                    }
                } catch (StandardException ie) {
                    if (!sawException) {
                        throw ie;
                    } // else: can't let it shadow preceding exception
                }
            }
        }
    }
}
