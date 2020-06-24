/*
 * Derby - Class org.apache.derby.impl.sql.execute.ScanResultSet
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.derby.impl.sql.execute;

import org.w3c.dom.Element;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecRowBuilder;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.transaction.TransactionControl;
import org.apache.derby.iapi.types.RowLocation;

/**
 * Abstract <code>ResultSet</code> class for <code>NoPutResultSet</code>s which
 * contain a scan. Returns rows that may be a column sub-set of the
 * rows in the underlying object to be scanned. If accessedCols is
 * not null then a sub-set of columns will be fetched from the underlying
 * object (usually into the candidate row object), then the returned
 * rows will be a compacted form of that row, with the not-fetched columns
 * moved out. If accessedCols is null then the full row will be returned.
 * <BR>
 * Example: if accessedCols indicates that we want to retrieve columns
 * 1 and 4, then candidate row will have space for 5
 * columns (because that's the size of the rows in the underlying object),
 * but only cols "1" and "4" will have values:
 * <BR>
 * <pre>
 *     0    1    2    3    4
 *  [  - , COL1,  - ,  - , COL4 ]
 *  </pre>
 *  <BR>
 * Rows returned by this ScanResultSet will have the values:
 * <BR>
 * <pre>
 *     0     1
 *  [ COL1, COL4 ]
 * </pre>
 */
abstract class ScanResultSet extends NoPutResultSetImpl {

    /** If true, the table is marked as table locked in SYS.SYSTABLES. */
    private final boolean tableLocked;
    /** If true, the isolation level is unspecified and must be refreshed on
     * each open. */
    private final boolean unspecifiedIsolationLevel;
    /** The lock mode supplied through the constructor. */
    private final int suppliedLockMode;
    /** Tells whether the isolation level needs to be updated. */
    private boolean isolationLevelNeedsUpdate;

    /** The actual lock mode used. */
    int lockMode;
    /** The scan isolation level. */
    int isolationLevel;

    /** Object used to create and reset the candidate row. */
    final ExecRowBuilder resultRowBuilder;
//IC see: https://issues.apache.org/jira/browse/DERBY-6003

    /** The candidate row, matches the shape of the rows in
     * the underlying object to be scanned.
     */
    final ExecRow candidate;
    
    /**
     * If not null indicates the subset of columns that
     * need to be pulled from the underlying object to be scanned.
     * Set from the PreparedStatement's saved objects, if it exists.
     */
    protected FormatableBitSet accessedCols;

    /** true if the scan should pick up row locations */
    protected boolean fetchRowLocations = false;

	public String tableName;
	public String indexName;

    /**
     * Construct a <code>ScanResultSet</code>.
     *
     * @param activation the activation
     * @param resultSetNumber number of the result set (unique within statement)
     * @param resultRowTemplate identifier of saved object for row template
     * @param lockMode lock mode (record or table)
     * @param tableLocked true if marked as table locked in SYS.SYSTABLES
     * @param isolationLevel language isolation level for the result set
     * @param colRefItem Identifier of saved object for accessedCols,
     * -1 if need to fetch all columns.
     * @param optimizerEstimatedRowCount estimated row count
     * @param optimizerEstimatedCost estimated cost
     */
    ScanResultSet(Activation activation, int resultSetNumber,
                  int resultRowTemplate,
                  int lockMode, boolean tableLocked, int isolationLevel,
                  int colRefItem,
                  double optimizerEstimatedRowCount,
                  double optimizerEstimatedCost) throws StandardException {
        super(activation, resultSetNumber,
              optimizerEstimatedRowCount,
              optimizerEstimatedCost);

        this.tableLocked = tableLocked;
        suppliedLockMode = lockMode;

//IC see: https://issues.apache.org/jira/browse/DERBY-6206
        if (isolationLevel == TransactionControl.UNSPECIFIED_ISOLATION_LEVEL) {
            unspecifiedIsolationLevel = true;
            isolationLevel = getLanguageConnectionContext().getCurrentIsolationLevel();
        } else {
            unspecifiedIsolationLevel = false;
        }

        this.lockMode = getLockMode(isolationLevel);
        this.isolationLevel =
            translateLanguageIsolationLevel(isolationLevel);

        ExecPreparedStatement ps = activation.getPreparedStatement();
//IC see: https://issues.apache.org/jira/browse/DERBY-6003

        // Create a candidate row.
        resultRowBuilder =
                (ExecRowBuilder) ps.getSavedObject(resultRowTemplate);
        candidate = resultRowBuilder.build(activation.getExecutionFactory());

        this.accessedCols = colRefItem != -1 ?
            (FormatableBitSet) ps.getSavedObject(colRefItem) : null;
    }

    /**
     * Initialize the isolation level and the lock mode. If the result set was
     * constructed with an explicit isolation level, or if the isolation level
     * has already been initialized, this is a no-op. All sub-classes should
     * invoke this method from their <code>openCore()</code> methods.
     */
    void initIsolationLevel() {
        if (isolationLevelNeedsUpdate) {
            int languageLevel = getLanguageConnectionContext().getCurrentIsolationLevel();
            lockMode = getLockMode(languageLevel);
            isolationLevel = translateLanguageIsolationLevel(languageLevel);
            isolationLevelNeedsUpdate = false;
        }
    }

    /**
     * Get the lock mode based on the language isolation level. Always do row
     * locking unless the isolation level is serializable or the table is
     * marked as table locked.
     *
     * @param languageLevel the (language) isolation level
     * @return lock mode
     */
    private int getLockMode(int languageLevel) {
        /* NOTE: always do row locking on READ COMMITTED/UNCOMITTED scans,
         * unless the table is marked as table locked (in sys.systables)
         * This is to improve concurrency.  Also see FromBaseTable's
         * updateTargetLockMode (KEEP THESE TWO PLACES CONSISTENT!
         * bug 4318).
         */
        /* NOTE: always do row locking on READ COMMITTED/UNCOMMITTED
         *       and repeatable read scans unless the table is marked as
         *       table locked (in sys.systables).
         *
         *       We always get instantaneous locks as we will complete
         *       the scan before returning any rows and we will fully
         *       requalify the row if we need to go to the heap on a next().
         */
        if (tableLocked ||
                (languageLevel ==
//IC see: https://issues.apache.org/jira/browse/DERBY-6206
                     TransactionControl.SERIALIZABLE_ISOLATION_LEVEL)) {
            return suppliedLockMode;
        } else {
            return TransactionController.MODE_RECORD;
        }
    }

    /** Determine whether this scan should return row locations */
    protected   void    setRowLocationsState()
        throws StandardException
    {
        fetchRowLocations =
            (
             (indexName == null) &&
             (candidate.nColumns() > 0) &&
             ( candidate.getColumn( candidate.nColumns() ) instanceof RowLocation )
             );
    }

    /**
     * Translate isolation level from language to store.
     *
     * @param languageLevel language isolation level
     * @return store isolation level
     */
    private int translateLanguageIsolationLevel(int languageLevel) {

        switch (languageLevel) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6206
        case TransactionControl.READ_UNCOMMITTED_ISOLATION_LEVEL:
            return TransactionController.ISOLATION_READ_UNCOMMITTED;
        case TransactionControl.READ_COMMITTED_ISOLATION_LEVEL:
            /*
             * Now we see if we can get instantaneous locks
             * if we are getting share locks.
             * (For example, we can get instantaneous locks
             * when doing a bulk fetch.)
             */
            if (!canGetInstantaneousLocks()) {
                return TransactionController.ISOLATION_READ_COMMITTED;
            }
            return TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK;
//IC see: https://issues.apache.org/jira/browse/DERBY-6206
        case TransactionControl.REPEATABLE_READ_ISOLATION_LEVEL:
            return TransactionController.ISOLATION_REPEATABLE_READ;
        case TransactionControl.SERIALIZABLE_ISOLATION_LEVEL:
            return TransactionController.ISOLATION_SERIALIZABLE;
        }

        if (SanityManager.DEBUG) {
            SanityManager.THROWASSERT("Unknown isolation level - " +
                                      languageLevel);
        }

        return 0;
    }

    /**
     * Can we get instantaneous locks when getting share row
     * locks at READ COMMITTED.
     */
    abstract boolean canGetInstantaneousLocks();

    /**
     * Return the isolation level of the scan in the result set.
     */
    public int getScanIsolationLevel() {
        return isolationLevel;
    }

    /**
     * Close the result set.
     *
     * @exception StandardException if an error occurs
     */
    public void close() throws StandardException {
        // need to update isolation level on next open if it was unspecified
        isolationLevelNeedsUpdate = unspecifiedIsolationLevel;
        // Prepare row array for reuse (DERBY-827).
        candidate.resetRowArray();
        super.close();
    }
    
    public Element toXML( Element parentNode, String tag ) throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6266
        Element myNode = super.toXML( parentNode, tag );
        if ( tableName != null ) { myNode.setAttribute( "tableName", tableName ); }
        if ( indexName != null ) { myNode.setAttribute( "indexName", indexName ); }
        
        return myNode;
    }
}
