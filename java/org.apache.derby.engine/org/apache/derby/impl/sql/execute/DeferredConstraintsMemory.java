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
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.derby.catalog.UUID;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.PreparedStatement;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.SQLSessionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.ForeignKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.KeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ReferencedKeyConstraintDescriptor;
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
     * Save the contents of an constraint supporting index row in a
     * hash table (which may spill to disk) for later checking,
     * typically on transaction commit, or upon request.
     *
     * @param lcc       the language connection context
     * @param deferredRowsHashTable
     *                  client cached value
     * @param constraintId the id of the unique or primary key constraint
     * @param insertRow the duplicate row to be saved in the hash table
     *                  for later checking
     * @return the hash table (for caching by client to minimize lookups)
     * @throws StandardException standard error policy
     */
    public static BackingStoreHashtable rememberDuplicate(
            final LanguageConnectionContext lcc,
                  BackingStoreHashtable deferredRowsHashTable,
//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
            UUID constraintId,
            final DataValueDescriptor[] insertRow) throws StandardException {

        // Don't copy the RowLocation, we don't need it:
        final int keyLength = insertRow.length -1 ;

        if (deferredRowsHashTable == null) {
            // Use the backing hashtable for this index's deferred rows
            // by checking the transaction's map of such backing hash
            // tables (one per index conglomerate).  Use it if it
            // exists, else make a new one.

//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
            final HashMap<UUID, ValidationInfo> hashTables =
                lcc.getDeferredHashTables();
            final ValidationInfo vi = hashTables.get(constraintId);

            if (vi == null) {
                deferredRowsHashTable =
                  makeDeferredHashTable(lcc.getTransactionExecute(), keyLength);

                hashTables.put(
                    constraintId,
                    new UniquePkInfo(deferredRowsHashTable, constraintId));
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
     * @param basetableId the id of the target table
     * @param schemaName the schema of the target table
     * @param tableName the target table name
     * @param deferredCheckViolations
     *                  client cached value
     * @param lcc       the language connection context
     * @param violatingCheckConstraints offending constraint(s)
     * @param offendingRow the duplicate row to be saved in the hash table
     *                  for later checking
     * @param result OUT parameter: the allocated CheckInfo
     * @return the hash table (for caching by client to minimize lookups)
     * @throws StandardException standard error policy
     */
    public static BackingStoreHashtable rememberCheckViolations(
            final LanguageConnectionContext lcc,
//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
            UUID  basetableId,
            final String schemaName,
            final String tableName,
                  BackingStoreHashtable deferredCheckViolations,
            final List<UUID> violatingCheckConstraints,
//IC see: https://issues.apache.org/jira/browse/DERBY-532
            final RowLocation offendingRow,
            CheckInfo[] result) throws StandardException {

        if (violatingCheckConstraints.isEmpty()) {
            return null;
        }

        if (deferredCheckViolations == null) {
            // Use the backing hashtable for this base tables deferred rows
            // by checking the transaction's map of such backing hash
            // tables (one per base table conglomerate).  Use it if it
            // exists, else make a new one.

//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
            final HashMap<UUID, ValidationInfo> hashTables =
                    lcc.getDeferredHashTables();
            final CheckInfo vi = (CheckInfo) hashTables.get(basetableId);

            if (vi == null) {
                // size 1 below: the row location in the target table of the
                // offending row
                deferredCheckViolations =
                        makeDeferredHashTable(lcc.getTransactionExecute(), 1);
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                CheckInfo ci = new CheckInfo(deferredCheckViolations,
                                   schemaName,
                                   tableName,
                                   violatingCheckConstraints);
//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
                hashTables.put(basetableId, ci);
                result[0] = ci;
            } else {
                vi.addCulprits(violatingCheckConstraints);
                deferredCheckViolations = vi.infoRows;
                result[0] = vi;
            }
        }

        final DataValueDescriptor[] hashRowArray = new DataValueDescriptor[1];
        hashRowArray[0] = new SQLRef(offendingRow).cloneValue(true);
        deferredCheckViolations.putRow(true, hashRowArray, null);

        return deferredCheckViolations;
    }


    public static Enumeration<Object> getDeferredCheckConstraintLocations(
//IC see: https://issues.apache.org/jira/browse/DERBY-532
            Activation activation,
            UUID validatingBaseTableUUID) throws StandardException {
//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665

        CheckInfo ci = (DeferredConstraintsMemory.CheckInfo)activation.
                getLanguageConnectionContext().
                getDeferredHashTables().get( validatingBaseTableUUID );
        return ci.infoRows.elements();
    }

    /**
     * Make note of a violated foreign key constraint, i.e. the referenced
     * key is not present
     *
     * @param lcc the language connection context
     * @param deferredRowsHashTable cached client copy
     * @param fkId the UUID of the foreign key constraint
     * @param indexRow the row in the supporting index which contains
     *        the key which is not present in the referenced index.
     * @param schemaName the schema of the table
     * @param tableName the table being modified that has a FK.
     * @return value to cache
     * @throws StandardException
     */
    public static BackingStoreHashtable rememberFKViolation(
            final LanguageConnectionContext lcc,
                  BackingStoreHashtable deferredRowsHashTable,
            final UUID fkId,
            final DataValueDescriptor[] indexRow,
            String schemaName,
            String tableName) throws StandardException {

        if (deferredRowsHashTable == null) {
            // Use the backing hashtable for this index's deferred rows
            // by checking the transaction's map of such backing hash
            // tables (one per index conglomerate).  Use it if it
            // exists, else make a new one.

//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
            final HashMap<UUID, ValidationInfo> hashTables =
                lcc.getDeferredHashTables();
            final ValidationInfo vi = hashTables.get(fkId);

            if (vi == null) {
                deferredRowsHashTable = makeDeferredHashTable(
                    lcc.getTransactionExecute(), indexRow.length);

                hashTables.put(
                    fkId,
                    new ForeignKeyInfo(deferredRowsHashTable, fkId,
                                       schemaName, tableName));
            } else {
                deferredRowsHashTable = vi.infoRows;
            }
        }

        DataValueDescriptor[] hashRowArray =
            new DataValueDescriptor[indexRow.length];
        System.arraycopy(indexRow, 0, hashRowArray, 0, indexRow.length);
        deferredRowsHashTable.putRow(true, hashRowArray, null);

        return deferredRowsHashTable;
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

    public static void compressOrTruncate(
            LanguageConnectionContext lcc,
            UUID tableId,
            String tableName) throws StandardException {

//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
        final HashMap<UUID, DeferredConstraintsMemory.ValidationInfo> vis =
                lcc.getDeferredHashTables();
        final TableDescriptor td =
                lcc.getDataDictionary().getTableDescriptor(tableId);
        final DeferredConstraintsMemory.ValidationInfo vi =
                vis.get( tableId );

        if (td == null) {
            throw StandardException.newException(
                    SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, tableName);
        }

        if (vi != null &&
                vi instanceof DeferredConstraintsMemory.CheckInfo) {
            // We can not use row locations when re-visiting offending
            // rows in this table, since we are truncating or compressing.
            ((DeferredConstraintsMemory.CheckInfo)vi).
                    setInvalidatedRowLocations();
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

        public abstract void possiblyValidateOnReturn(
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                LanguageConnectionContext lcc,
                SQLSessionContext nested,
                SQLSessionContext caller) throws StandardException;

        public abstract void validateConstraint(
                LanguageConnectionContext lcc,
                UUID constraintId,
                boolean rollbackOnError) throws StandardException;
    }

    /**
     * Info needed for unique and primary key constraints
     */
    private static class UniquePkInfo extends ValidationInfo {
        private final UUID constraintId;

        public UniquePkInfo(BackingStoreHashtable infoRows, UUID constraintId) {
            super(infoRows);
//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
            this.constraintId = constraintId;
        }

        @Override
        public final void possiblyValidateOnReturn(
                LanguageConnectionContext lcc,
                SQLSessionContext nested,
                SQLSessionContext caller) throws StandardException {

                if (lcc.isEffectivelyDeferred(caller, constraintId)) {
                    // the constraint is also deferred in the calling context
                    return;
                }

                validateUniquePK(lcc, infoRows, true);
        }

        /**
         * Validate one primary key or unique constraint
         *
         * @param lcc       The language connection context
         * @param constraintId Not used by this constraint type
         * @param rollbackOnError {@code true} if we should roll back the
         *                  transaction if we see a violation of the constraint
         * @throws StandardException
         */
        @Override
        public final void validateConstraint(
                LanguageConnectionContext lcc,
                UUID constraintId,
                boolean rollbackOnError) throws StandardException {

            validateUniquePK(
                    lcc, this.infoRows, rollbackOnError);
        }

        private void validateUniquePK(
                final LanguageConnectionContext lcc,
                final BackingStoreHashtable ht,
                final boolean rollbackOnError) throws StandardException {

            final TransactionController tc = lcc.getTransactionExecute();
            final Enumeration<?> e = ht.elements();

            DataDictionary dd = lcc.getDataDictionary();
            KeyConstraintDescriptor cd = (KeyConstraintDescriptor)
                    dd.getConstraintDescriptor(constraintId);

            if (cd == null) {
                // Constraint dropped, nothing to do.
                return;
            }

            long indexCID = cd.getIndexConglomerateDescriptor(dd)
                              .getConglomerateNumber();

            while (e.hasMoreElements()) {
                final DataValueDescriptor[] key =
                        (DataValueDescriptor[])e.nextElement();

                // FIXME: This is not very efficient: we could sort the rows in
                // the hash table, and then check all rows using a single scan.
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
                            throw StandardException.newException(
                                rollbackOnError ?
                                SQLState.
                                    LANG_DEFERRED_DUPLICATE_KEY_CONSTRAINT_T :
                                SQLState.
                                    LANG_DEFERRED_DUPLICATE_KEY_CONSTRAINT_S,
//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
                                cd.getConstraintName(),
                                cd.getTableDescriptor().getName());
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

        @Override
        public void possiblyValidateOnReturn(
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                LanguageConnectionContext lcc,
                SQLSessionContext nested,
                SQLSessionContext caller) throws StandardException {

            // check if any of the constraints involved is immediate on
            // the outside
            boolean allEffectivelyDeferred = true;

            for (UUID uid : getCulprints()) {
                if (!lcc.isEffectivelyDeferred(caller, uid) &&
                        lcc.isEffectivelyDeferred(nested, uid)) {

                    // at least one check constraint changed back
                    // from being deferred to immediate, so check
                    // all immediates

                    // FIXME: could be optimized if we knew
                    // exactly which constraints failed under the
                    // deferred regime: that might save us from
                    // checking in a few cases.
                    allEffectivelyDeferred = false;
                    break;
                }
            }

            if (allEffectivelyDeferred) {
                return;
            }

//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
            validateCheck(lcc, null, true);
        }

        /**
         * Validate one or more CHECK constraints on a table.
         * <p>
         * Implementation note: We remember violations for a row by storing its
         * row location in a disk based hash table, similar to what we do for
         * the index location for primary key and unique constraints. As far as
         * which constraints caused which violations, constraints are only
         * presently remembered as having caused "one or more violations",
         * i.e. for any violating row, we do not know at checking time exactly
         * which constraint caused a problem for that exact row. So, for any
         * given constraint which was violated in the transaction we visit all
         * rows which had one or more violations and check again. This could be
         * improved upon by remembering for each violating row the exact set of
         * constraint(s) that saw a violation. Still, this is much faster than
         * a full table scan in most use cases.  We use a special internal
         * query option to achieve this.  The row locations may not be usable
         * if Derby does a compress or otherwise makes them invalid. In that
         * case we resort to a full table scan.</p>
         * @see ValidateCheckConstraintResultSet
         *
         * @param lcc          The language connection context
         * @param constraintId If not {@code null}, check only for this
         *                     constraint.  This is used when switching mode to
         *                     immediate.  If {@code null}, we check all check
         *                     constraints, i.e.  at commit or if we have {@code
         *                     SET CONSTRAINT ALL IMMEDIATE}.
         * @param rollbackOnError If {@code true} and a violation occurs, throw
         *                     and exception that will cause rollback.
         * @throws StandardException
         *                     Default error policy
         */
        @Override
        public final void validateConstraint(
                LanguageConnectionContext lcc,
                UUID constraintId,
                boolean rollbackOnError) throws StandardException {

//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
            validateCheck(lcc, constraintId, rollbackOnError);
        }

        private void validateCheck(
                final LanguageConnectionContext lcc,
                final UUID constraintId,
                final boolean rollbackOnError) throws StandardException {

            final TransactionController tc = lcc.getTransactionExecute();
            final DataDictionary dd = lcc.getDataDictionary();
            final SchemaDescriptor sd = dd.getSchemaDescriptor(
                    schemaName, tc, true);

            if (sd == null) {
                // Schema dropped, nothing to do
//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
                return;
            }

            final TableDescriptor td = dd.getTableDescriptor(tableName, sd, tc);

            if (td == null) {
                // Nothing to do, table dropped
            } else {
                final String baseTableUUIDString = td.getUUID().toString();
                for (UUID id : culprits) {
                    if (constraintId == null || constraintId.equals(id)) {
                        final ConstraintDescriptor cd =
                                dd.getConstraintDescriptor(id);

                        if (cd == null) {
                            // Constraint dropped, nothing to do.
                            break;
                        }

                        final StringBuilder checkStmt = new StringBuilder();
                        checkStmt.append("SELECT 1 FROM ");
                        checkStmt.append(td.getQualifiedName());

                        // If a compress has happened in this transaction, we
                        // can't trust the rowLocations, so make a full table
                        // scan. If not, we optimize by using a special result
                        // set type which utilized the saved away row locations
                        // for the offending rows, so we only visit those when
                        // checking.  I.e. other rows are known to be good a
                        // priori.
                        if (!isInvalidated()) {
                            checkStmt.append(
                               " --DERBY-PROPERTIES joinStrategy=nestedLoop, " +
                               "                    index=null, " +
                               "                    validateCheckConstraint=");
//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
                            checkStmt.append( baseTableUUIDString );
                            checkStmt.append('\n');
                        }

                        checkStmt.append(" WHERE NOT(");
                        checkStmt.append(cd.getConstraintText());
                        checkStmt.append(')');

                        BasicNoPutResultSetImpl rs = null;
                        
                        final PreparedStatement ps =
                            lcc.prepareInternalStatement(
                                lcc.getDefaultSchema(),
                                checkStmt.toString(),
                                true,
                                true);

//IC see: https://issues.apache.org/jira/browse/DERBY-6666
                        StatementContext statementContext = null;
                        
                        try {
                            statementContext =
                                    lcc.pushStatementContext(true,
                                            true,
                                            checkStmt.toString(),
                                            null,
                                            false, 0L);
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
                                   td.getQualifiedName(),
                                   cd.getConstraintText());
                            }
                        } finally {
//IC see: https://issues.apache.org/jira/browse/DERBY-6666
                            if (statementContext != null) {
                                lcc.popStatementContext(statementContext, null);
                            }
                            
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
    }

    private static class ForeignKeyInfo extends ValidationInfo {
        /**
         * the UUID of the foreign constraint
         */
        private final UUID fkId;

        final private String schemaName;
        final private String tableName;

        public ForeignKeyInfo(
                final BackingStoreHashtable infoRows,
                UUID fkId,
                String schemaName,
                String tableName) {

            super(infoRows);
            this.fkId = fkId;
            this.tableName = tableName;
            this.schemaName = schemaName;
        }

        public UUID getFkId() {
            return fkId;
        }

        @Override
        public void possiblyValidateOnReturn(
                LanguageConnectionContext lcc,
                SQLSessionContext nested,
                SQLSessionContext caller) throws StandardException {

            if (lcc.isEffectivelyDeferred(caller, getFkId())) {
                // the constraint is also deferred in the calling context
                return;
            }

//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
            validateForeignKey(lcc, true);
        }

        @Override
        public final void validateConstraint(
                LanguageConnectionContext lcc,
                UUID constraintId,
                boolean rollbackOnError) throws StandardException {

            validateForeignKey(lcc, rollbackOnError);

        }

        private void validateForeignKey(
            LanguageConnectionContext lcc,
            boolean rollbackOnError) throws StandardException {

            // First check if the offending row is still present,
            // if so, check that the referenced row exists. To do this we open
            // two index scans below.

            TransactionController tc = lcc.getTransactionExecute();

            DataDictionary dd = lcc.getDataDictionary();
//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665

            ForeignKeyConstraintDescriptor cd =
                (ForeignKeyConstraintDescriptor)
                    dd.getConstraintDescriptor(fkId);

            // If the foreign key has been dropped, there is nothing to do.
            // See DERBY-6670
            //
            if (cd == null) {
                return;
            }

            ReferencedKeyConstraintDescriptor rcd =
                    cd.getReferencedConstraint();

            long[] cids = {
                cd.getIndexConglomerateDescriptor(dd).getConglomerateNumber(),
                rcd.getIndexConglomerateDescriptor(dd).getConglomerateNumber()

            };

            final Enumeration<?> e = infoRows.elements();

            while (e.hasMoreElements()) {
                final DataValueDescriptor[] key =
                        (DataValueDescriptor[])e.nextElement();

                // FIXME: This is not very efficient: we could sort the rows in
                // the hash table, and then check all rows using a single scan.
                ScanController indexSC = null;
                boolean violation = false;

                for (int idx = 0; idx < 2; idx++) {
                    boolean sawException = false;

                    try {
                        indexSC = tc.openScan(
                                cids[idx],
                                false,
                                0, // read only
                                TransactionController.MODE_RECORD,
                                TransactionController.ISOLATION_READ_COMMITTED,
                                (FormatableBitSet)null, // retrieve all fields
                                key,
                                ScanController.GE, // startSearchOp
                                null,
                                key,
                                ScanController.GT);

                        if (idx == 0) {
                            if (indexSC.next()) {
                                // The row with the PK still exists, so we need
                                // to check the referenced table's index
                            } else {
                                // No rows contain key: OK, must have been
                                // deleted later in transaction, or we got here
                                // due to pessimistic assumption on a timeout
                                // while checking on the insert.  In any case,
                                // no need to check the referenced key, so
                                // leave.
                                break;
                            }
                        } else {
                            if (indexSC.next()) {
                                // We found the referenced key, all is good
                            } else {
                                // We didn't find it and we know it is present
                                // as a PK, so we have a violation.
                                violation = true;
                            }
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

                if (violation) {
                    final SchemaDescriptor sd =
                            dd.getSchemaDescriptor(schemaName, tc, true);

                    final TableDescriptor td  =
                            dd.getTableDescriptor(tableName, sd, tc);

                    final TableDescriptor rtd = rcd.getTableDescriptor();

                    throw StandardException.newException(
                            rollbackOnError ?
                                    SQLState.LANG_DEFERRED_FK_CONSTRAINT_T :
                                    SQLState.LANG_DEFERRED_FK_CONSTRAINT_S,
                            cd.getConstraintName(),
                            td.getQualifiedName(),
                            rcd.getConstraintName(),
                            rtd.getQualifiedName(),
                            RowUtil.toString(key));
                }
            }
        }
    }
}
