/*

   Derby - Class org.apache.derby.impl.sql.execute.DeferredConstraintsMemory

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
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.PreparedStatement;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.BackingStoreHashtable;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.SQLRef;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * This class provides support for deferrable constraints. When the constraint
 * mode is deferred, any violation of the constraint should not be flagged
 * until the constraint mode is switched back to immediate, which may happen by
 * explicitly setting the constraint mode to immediate, or implicitly at commit
 * time. It may also happen implicitly when returning from a stored procedure
 * if the constraint mode is immediate in the caller context.
 * <p>
 * The approach taken in Derby to support deferred constraints is to make a note
 * when the violation happens (at insert or update time), and then remember that
 * violation until the mode switches back as described above.  We note exactly
 * which rows cause violations, so checking can happen as quickly as possible
 * when we get there. The core mechanism used to remember the violations as well
 * as the deferred checking is embodied in this class.
 *
 */
final public class DeferredConstraintsMemory
{
    /**
     * For unique and primary key constraints, sometimes we need to save
     * duplicate rows before we know the id of the constraint index, so we
     * assign the duplicates row a temporary constraint id
     * (UNDEFINED_CONGLOMERATE) and fix it up later.
     * @see #associateDuplicatesWithConglomerate
     */
    public final static long UNDEFINED_CONGLOMERATE = -1;

    /**
     * Save the contents of an constraint supporting index row in a
     * hash table (which may spill to disk) for later checking,
     * typically on transaction commit, or upon request.
     *
     * @param lcc       the language connection context
     * @param deferredRowsHashTable
     *                  client cached value
     * @param indexCID  the identity of the index conglomerate which supports
     *                  the deferred constraint
     * @param insertRow the duplicate row to be saved in the hash table
     *                  for later checking
     * @return the hash table (for caching by client to minimize lookups)
     * @throws StandardException standard error policy
     */
    public static BackingStoreHashtable rememberDuplicate(
            final LanguageConnectionContext lcc,
                  BackingStoreHashtable deferredRowsHashTable,
            final long indexCID,
            final DataValueDescriptor[] insertRow) throws StandardException {

        // Don't copy the RowLocation, we don't need it:
        final int keyLength = insertRow.length -1 ;

        if (deferredRowsHashTable == null) {
            // Use the backing hashtable for this index's deferred rows
            // by checking the transaction's map of such backing hash
            // tables (one per index conglomerate).  Use it if it
            // exists, else make a new one.

            final HashMap<Long, ValidationInfo> hashTables =
                lcc.getDeferredHashTables();
            final ValidationInfo vi = hashTables.get(Long.valueOf(indexCID));

            if (vi == null) {
                deferredRowsHashTable =
                  makeDeferredHashTable(lcc.getTransactionExecute(), keyLength);

                hashTables.put(
                    Long.valueOf(indexCID),
                    new UniquePkInfo(deferredRowsHashTable));
            } else {
                deferredRowsHashTable = vi.infoRows;
            }
        }

        DataValueDescriptor[] hashRowArray = new DataValueDescriptor[keyLength];
        System.arraycopy(insertRow, 0, hashRowArray, 0, keyLength);
        deferredRowsHashTable.putRow(true, hashRowArray, null);

        return deferredRowsHashTable;
    }

    /**
     * Save the row location of an offending row (one or more check constraints
     * were violated) in a hash table (which may spill to disk) for later
     * checking, typically on transaction commit, or upon request.
     *
     * The row locations are subject to invalidation, cf.
     * {@code CheckInfo#setInvalidatedRowLocations}.
     *
     * @param basetableCID  the identity of the base table conglomerate for
     *                  which we have seen a violated deferred check constraint
     * @param schemaName the schema of the target table
     * @param tableName the target table name
     * @param deferredCheckViolations
     *                  client cached value
     * @param lcc       the language connection context
     * @param violatingCheckConstraints offending constraint(s)
     * @param offendingRow the duplicate row to be saved in the hash table
     *                  for later checking
     * @return the hash table (for caching by client to minimize lookups)
     * @throws StandardException standard error policy
     */
    public static BackingStoreHashtable rememberCheckViolations(
            final LanguageConnectionContext lcc,
            final long basetableCID,
            final String schemaName,
            final String tableName,
                  BackingStoreHashtable deferredCheckViolations,
            final List<UUID> violatingCheckConstraints,
            final RowLocation offendingRow) throws StandardException {

        if (violatingCheckConstraints.isEmpty()) {
            return null;
        }

        if (deferredCheckViolations == null) {
            // Use the backing hashtable for this base tables deferred rows
            // by checking the transaction's map of such backing hash
            // tables (one per base table conglomerate).  Use it if it
            // exists, else make a new one.

            final HashMap<Long, ValidationInfo> hashTables =
                    lcc.getDeferredHashTables();
            final CheckInfo vi =
                    (CheckInfo)hashTables.get(Long.valueOf(basetableCID));

            if (vi == null) {
                // size 1 below: the row location in the target table of the
                // offending row
                deferredCheckViolations =
                        makeDeferredHashTable(lcc.getTransactionExecute(), 1);

                hashTables.put(Long.valueOf(basetableCID),
                               new CheckInfo(deferredCheckViolations,
                                   schemaName,
                                   tableName,
                                   violatingCheckConstraints));
            } else {
                vi.addCulprits(violatingCheckConstraints);
                deferredCheckViolations = vi.infoRows;
            }
        }

        final DataValueDescriptor[] hashRowArray = new DataValueDescriptor[1];
        hashRowArray[0] = new SQLRef(offendingRow).cloneValue(true);
        deferredCheckViolations.putRow(true, hashRowArray, null);

        return deferredCheckViolations;
    }


    /**
     * After having belatedly learned the identity of the conglomerate, we now
     * associate the conglomerate id information with the saved duplicates
     * memory. Used for unique and primary key constraints.
     * See {@link #UNDEFINED_CONGLOMERATE}.
     *
     * @param lcc language connection context
     * @param indexCID the id of the index conglomerate supporting the
     *                 deferred constraint
     */
    public static void associateDuplicatesWithConglomerate(
            final LanguageConnectionContext lcc,
            final long indexCID) {

        final HashMap<Long, ValidationInfo> hashTables =
                    lcc.getDeferredHashTables();

        if (hashTables == null) {
            return; // no duplicates recorded in this transaction
        }

        final ValidationInfo ht = hashTables.remove(
                Long.valueOf(UNDEFINED_CONGLOMERATE));

        if (ht != null) {
            hashTables.put(indexCID, ht);
        } // else no duplicates recorded for this index
    }

    /**
     * The conglomerate id for an index with deferred row checking needs
     * updating in the memory if the underlying index is rebuilt, for example
     * on bulk insert for import.
     *
     * @param lcc the language connection context needed to find the
     *            deferred rows information if any
     * @param oldIndexCID the old id of the supporting index
     * @param newIndexCID the new id of the supporting index after recreation
     */
    public static void updateIndexCID(
            final LanguageConnectionContext lcc,
            final long oldIndexCID,
            final long newIndexCID) {

        final HashMap<Long, ValidationInfo> hashTables =
                    lcc.getDeferredHashTables();

        if (hashTables == null) {
            return; // no duplicates recorded in this transaction
        }

        final ValidationInfo ht = hashTables.remove(
                Long.valueOf(oldIndexCID));

        if (ht != null) {
            hashTables.put(Long.valueOf(newIndexCID), ht);
        }
    }

    private static BackingStoreHashtable makeDeferredHashTable(
            final TransactionController tc,
            final int cols) throws StandardException {

        // key: all columns (these are index rows, or a row containing a
        // row location)
        final int[] keyCols = new int[cols];

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

    private static BackingStoreHashtable makeDeferredCheck(
            final TransactionController tc,
            final int cols) throws StandardException {

        // key: all columns (these are index rows)
        final int[] keyCols = new int[cols];

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

    /**
     * Validate one primary key or unique constraint
     *
     * @param lcc       The language connection context
     * @param indexCID  The conglomerate id of the index backing the constraint
     * @param ht        Cached saved rows if any
     * @param rollbackOnError {@code true} if we should roll back the
     *                  transaction if we see a violation of the constraint
     * @throws StandardException
     */
    public static void validateUniquePK(
            final LanguageConnectionContext lcc,
            final long indexCID,
            final BackingStoreHashtable ht,
            final boolean rollbackOnError) throws StandardException {

        final TransactionController tc = lcc.getTransactionExecute();
        final Enumeration<?> e = ht.elements();

        while (e.hasMoreElements()) {
            final DataValueDescriptor[] key =
                    (DataValueDescriptor[])e.nextElement();

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


    /**
     * Validate one or more CHECK constraints on a table.
     * <p>
     * Implementation note: We remember violations for a row by storing its row
     * location in a disk based hash table, similar to what we do for the index
     * location for primary key and unique constraints. As far as which
     * constraints caused which violations, constraints are only presently
     * remembered as having caused "one or more violations", i.e. for any
     * violating row, we do not know at checking time exactly which constraint
     * caused a problem for that exact row. So, for any given constraint which
     * was violated in the transaction we visit all rows which had one or more
     * violations and check again. This could be improved upon by remembering
     * for each violating row the exact set of constraint(s) that saw a
     * violation. Still, this is much faster than a full table scan in most use
     * cases.  We use a special internal query option to achieve this.  The row
     * locations may not be usable if Derby does a compress or otherwise makes
     * them invalid. In that case we resort to a full table scan.
     * @see ValidateCheckConstraintResultSet
     *
     * @param lcc          The language connection context
     * @param baseTableCID The conglomerate id of the base table for which
     *                     we want to validate check constraints.
     * @param constraintId If not {@code null}, check only for this
     *                     constraint.  This is used when switching mode to
     *                     immediate.  If {@code null}, we check all check
     *                     constraints, i.e.  at commit or if we have {@code
     *                     SET CONSTRAINT ALL IMMEDIATE}.
     * @param ci           The constraints info for the table we need
     *                     to perform the check on.
     * @param rollbackOnError If {@code true} and a violation occurs, throw
     *                     and exception that will cause rollback.
     * @throws StandardException
     *                     Default error policy
     */
    public static void validateCheck(
            final LanguageConnectionContext lcc,
            final long baseTableCID,
            final UUID constraintId,
            final CheckInfo ci,
            final boolean rollbackOnError) throws StandardException {

        final TransactionController tc = lcc.getTransactionExecute();
        final DataDictionary dd = lcc.getDataDictionary();
        final SchemaDescriptor sd = dd.getSchemaDescriptor(
                ci.schemaName, tc, true);

        if (sd == null) {
            if (SanityManager.DEBUG) {
                // dropping of a schema shouold drop any tables and their
                // constraints, which in turn should drop any deferred
                // constraint memory of them.
                SanityManager.NOTREACHED();
            } else {
                return;
            }
        }

        final TableDescriptor td = dd.getTableDescriptor(ci.tableName, sd, tc);

        if (td == null) {
            if (SanityManager.DEBUG) {
                // dropping of a table shouold drop any
                // constraints, which in turn should drop any deferred
                // constraint memory of them. Renaming of a table with
                // constrants is not presently allowed. FIXME: use UUID
                // instead of string here, more stable reference.
                SanityManager.NOTREACHED();
            }
        } else {
            for (UUID id : ci.culprits) {
                if (constraintId == null || constraintId.equals(id)) {
                    final ConstraintDescriptor cd =
                        dd.getConstraintDescriptor(id);
                    final StringBuilder checkStmt = new StringBuilder();
                    checkStmt.append("SELECT 1 FROM ");
                    checkStmt.append(td.getQualifiedName());

                    // If a compress has happened in this transaction, we can't
                    // trust the rowLocations, so make a full table scan. If
                    // not, we optimize by using a special result set type
                    // which utilized the saved away row locations for the
                    // offending rows, so we only visit those when checking.
                    // I.e. other rows are known to be good a priori.
                    if (!ci.isInvalidated()) {
                        checkStmt.append(
                            " --DERBY-PROPERTIES joinStrategy=nestedLoop, " +
                                "                    index=null, " +
                                "                    validateCheckConstraint=");
                        checkStmt.append(Long.toString(baseTableCID));
                        checkStmt.append('\n');
                    }

                    checkStmt.append(" WHERE NOT(");
                    checkStmt.append(cd.getConstraintText());
                    checkStmt.append(')');

                    BasicNoPutResultSetImpl rs = null;
                    final PreparedStatement ps = lcc.prepareInternalStatement(
                        lcc.getDefaultSchema(),
                        checkStmt.toString(),
                        true,
                        true);
                    try {
                        rs = (BasicNoPutResultSetImpl)ps.execute(
                            ps.getActivation(lcc, false), false, 0L);
                        final ExecRow row = rs.getNextRowCore();

                        if (row != null) {
                            //check constraint violated

                            throw StandardException.newException(
                                rollbackOnError ?
                                    SQLState.LANG_DEFERRED_CHECK_CONSTRAINT_T :
                                    SQLState.LANG_DEFERRED_CHECK_CONSTRAINT_S,
                                cd.getConstraintName(),
                                td.getName(),
                                cd.getConstraintText());
                        }
                    } finally {
                        if (rs != null) {
                            try {
                                rs.close();
                            } catch (StandardException e) {}
                        }
                    }
                }
            }
        }
    }


    /**
     * Class hierarchy carrying the information we need to validate
     * some deferred constraint. For unique and primary key constraints, we
     * only need the index row. For check constraints we need the target table
     * name, schema name and some other info also.
     */
    abstract public static class ValidationInfo {
        public final BackingStoreHashtable infoRows;

        public ValidationInfo(final BackingStoreHashtable infoRows) {
            this.infoRows = infoRows;
        }
    }

    /**
     * Info needed for unique and primary key constraints
     */
    public static class UniquePkInfo extends ValidationInfo {
        public UniquePkInfo(final BackingStoreHashtable infoRows) {
            super(infoRows);
        }
    }

    /**
     * Info needed for check constraints
     */
    public static class CheckInfo extends ValidationInfo {
        final private String schemaName;
        final private String tableName;

        private List<UUID> culprits; // constraints that were violated
                                     // for this table so far
        private boolean invalidatedDueToCompress;

        public CheckInfo(
                final BackingStoreHashtable infoRows,
                final String schemaName,
                final String tableName,
                final List<UUID> culprits) {

            super(infoRows);
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.culprits = new ArrayList<UUID>(culprits);
        }

        public void setInvalidatedRowLocations() {
            invalidatedDueToCompress = true;
        }

        public boolean isInvalidated() {
            return invalidatedDueToCompress;
        }

        public void addCulprits(List<UUID> newCulprits) {
            final Set<UUID> old = new HashSet<UUID>(culprits);
            old.addAll(newCulprits);
            culprits = new ArrayList<UUID>(old);
        }

        public List<UUID> getCulprints() {
            return culprits;
        }
    }
}
