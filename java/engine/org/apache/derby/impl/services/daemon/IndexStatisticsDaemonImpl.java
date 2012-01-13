/*

   Derby - Class org.apache.derby.impl.services.daemon.IndexStatisticsDaemonImpl

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
package org.apache.derby.impl.services.daemon;

import java.io.PrintWriter;
import java.sql.Connection;
import java.util.ArrayList;

import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.types.StatisticsImpl;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.db.Database;
import org.apache.derby.iapi.error.ExceptionSeverity;
import org.apache.derby.iapi.error.ShutdownException;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.daemon.IndexStatisticsDaemon;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.StatisticsDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.GroupFetchScanController;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.util.InterruptStatus;

/**
 * Daemon acting as a coordinator for creating and updating index cardinality
 * statistics.
 * <p>
 * The need for updated statistics is currently determined when compiling a
 * SELECT query. The unit of work is then scheduled with this daemon, and the
 * work itself will be carried out in a separate thread. If the worker thread
 * doesn't exist it is created, if it is idle the unit of work will be
 * processed immediately, and if it is busy the unit of work has to wait in the
 * queue.
 * <p>
 * The daemon code has a notion of a background task. If the update is run as a
 * background task, it will try to affect other activity in the Derby database
 * as little as possible. As far as possible, it will not set locks on the
 * conglomerates it scans, and if it needs to take locks it will give up
 * immediately if the locks cannot be obtained. In some cases it will also roll
 * back to release locks already taken, ad then retry. Since we are accessing
 * shared structures the background work may still interfere with the user
 * activity in the database due to locking, but all such operations carried out
 * by the daemon are of short duration.
 * <p>
 * The high level flow of an update to index statistics is:
 * <ol>
 *      <li>schedule update (the only action carried out by the user thread)<li>
 *      <li>for each index:</li>
 *      <ol>
 *          <li>scan index</li>
 *          <li>invalidate statements dependent on current statistics</li>
 *          <li>drop existing statistics</li>
 *          <li>add new statistics</li>
 *      </ol>
 * </ol>
 * <p>
 * List of possible improvements:
 * <ol>
 *      <li>Reduce potential impact of multiple invalidations (per table),
 *          probably by finding a way to invalidate only once after all indexes
 *          for a table have had their statistics updated. So far invalidation
 *          has proven to be the most difficult piece of the puzzle due to the
 *          interaction with the data dictionary and sensitivity to concurrent
 *          activity for the table.</li>
 * </ol>
 * <p>
 * <em>Implementation notes:</em> List of potential cleanups before going into
 * a release:
 * <ol>
 *      <li>Consider removing all tracing code. May involve improving logging
 *          if parts of the trace output is valuable enough.</li>
 * </ol>
 */
public class IndexStatisticsDaemonImpl
        implements IndexStatisticsDaemon, Runnable {

    private static final boolean AS_BACKGROUND_TASK = true;
    private static final boolean AS_EXPLICIT_TASK = false;
    /** Maximum number of work units allowed in the queue. */
    // TODO: Replace with constant after testing/tuning phase.
    private static final int MAX_QUEUE_LENGTH;
    static {
        MAX_QUEUE_LENGTH = PropertyUtil.getSystemInt(
                Property.STORAGE_AUTO_INDEX_STATS_DEBUG_QUEUE_SIZE,
                Property.STORAGE_AUTO_INDEX_STATS_DEBUG_QUEUE_SIZE_DEFAULT);
    }

    private final HeaderPrintWriter logStream;
    /** Tells if logging is enabled. */
    private final boolean doLog;
    // TODO: Consider removing the trace functionality after testing/tuning.
    /** Tells if tracing is enabled. */
    private final boolean doTrace;
    /** Tells if traces are written to the Derby log file. */
    private final boolean traceToDerbyLog;
    /** Tells if traces are written to standard out. */
    private final boolean traceToStdOut;

    /** Tells if the daemon has been disabled. */
    // @GuardedBy("queue")
    private boolean daemonDisabled;
    /** The context manager for the worker thread. */
    private final ContextManager ctxMgr;
    /** The language connection context for the worker thread. */
    private LanguageConnectionContext daemonLCC;
    /**
     * The database object for the database we are handling automatic index
     * statistics update for.
     */
    private final Database db;
    /** The name of the database owner. */
    private final String dbOwner;
    private final String databaseName;

    /**
     * A list of tables that shall have their index statistics updated.
     * Note that the descriptor isn't removed before the work has
     * been completed.
     */
    private final ArrayList queue = new ArrayList(MAX_QUEUE_LENGTH);
    /**
     * The thread in which the index statistics refresh operation is being
     * executed, if any. Created as needed, but there will only be one
     * thread doing the work. The thread is allowed to die since it is assumed
     * that index statistics regeneration is rather infrequent.
     */
    //@GuardedBy("queue")
    private Thread runningThread;

    /**
     * Number of consecutive errors, used as a metric to decide if the damoen
     * should be automatically shut down.
     */
    private int errorsConsecutive;
    // Counters used for tracing / logging (wu = work unit).
    private long errorsUnknown;
    private long errorsKnown;
    private long wuProcessed;
    private long wuScheduled;
    private long wuRejectedDup; // Duplicates
    private long wuRejectedFQ; // Full queue
    private long wuRejectedOther; // Daemon disabled

    /** Specifies when the daemon was created. */
    private final long timeOfCreation;
    /**
     * The period of time (ms) for which the daemon has been doing active work.
     */
    private long runTime;

    /**
     * Creates a new daemon.
     *
     * @param log the log to write to
     * @param doLog whether to log activity information
     * @param traceLevel whether, and to where, trace information should be
     *      written ("off|log|stdout|both")
     * @param db the database ("off|log|stdout|both")
     * @param userName the name of the database owner
     * @param databaseName the name of the database (not stored in the db obj)
     */
    public IndexStatisticsDaemonImpl(HeaderPrintWriter log, boolean doLog,
                                     String traceLevel, Database db,
                                     String userName, String databaseName) {
        // Make sure we can log errors.
        if (log == null) {
            throw new IllegalArgumentException("log stream cannot be null");
        }
        // Configure logging/tracing
        this.logStream = log;
        this.doLog = doLog;
        this.traceToDerbyLog = (traceLevel.equalsIgnoreCase("both") ||
                traceLevel.equalsIgnoreCase("log"));
        this.traceToStdOut = (traceLevel.equalsIgnoreCase("both") ||
                traceLevel.equalsIgnoreCase("stdout"));
        this.doTrace = traceToDerbyLog || traceToStdOut;

        this.db = db;
        this.dbOwner = userName;
        this.databaseName = databaseName;
        this.ctxMgr = ContextService.getFactory().newContextManager();
        this.timeOfCreation = System.currentTimeMillis();
        trace(0, "created{log=" + doLog + ", traceLog=" +
                traceToDerbyLog + ", traceOut=" + traceToStdOut +
                ", createThreshold=" +
                TableDescriptor.ISTATS_CREATE_THRESHOLD +
                ", absdiffThreshold=" +
                TableDescriptor.ISTATS_ABSDIFF_THRESHOLD +
                ", lndiffThreshold=" +
                TableDescriptor.ISTATS_LNDIFF_THRESHOLD +
                ", queueLength=" + MAX_QUEUE_LENGTH +
                "}) -> " + databaseName);
    }

    /**
     * Schedules an update of the index statistics for the specified table.
     * <p>
     * Assume the descriptor will be valid until we get around to generate
     * the statistics. If it turns out to be invalid, it will be discarded.
     *
     * @param td base table descriptor to update index statistics for
     */
    public void schedule(TableDescriptor td) {
        String schedulingReason = td.getIndexStatsUpdateReason();
        synchronized (queue) {
            if (acceptWork(td)) {
                // Add the work description for the given table.
                queue.add(td);
                wuScheduled++;
                log(AS_BACKGROUND_TASK, td,
                        "update scheduled" +
                        (schedulingReason == null
                            ? ""
                            : ", reason=[" + schedulingReason + "]") +
                        " (queueSize=" + queue.size() + ")");
                // If we're idle, fire off the worker thread.
                if (runningThread == null) {
                    runningThread = new Thread(this, "index-stat-thread");
                    // Make the thread a daemon thread, we don't want it to stop
                    // the JVM from exiting. This is a precaution.
                    runningThread.setDaemon(true);
                    runningThread.start();
                }
            }
        }
    }

    /**
     * Determines if the given work can be accepted.
     *
     * @param td the table descriptor to check
     * @return {@code true} if work can be accepted, {@code false} if not.
     */
    //@GuardedBy("queue")
    private boolean acceptWork(TableDescriptor td) {
        // Don't allow unbounded growth.
        boolean accept = !(daemonDisabled || queue.size() >= MAX_QUEUE_LENGTH);
        if (accept && !queue.isEmpty()) {
            // See if work is already scheduled for this table. If so, we
            // give the already scheduled or in progress task precedence.
            String table = td.getName();
            String schema = td.getSchemaName();
            // Since the queue size is limited, iterating through it to find
            // duplicates should yield acceptable performance. Also, we don't
            // look for duplicates if the queue is already full.
            for (int i=0; i < queue.size(); i++) {
                TableDescriptor work = (TableDescriptor)queue.get(i);
                if (work.tableNameEquals(table, schema)) {
                    accept = false;
                    break;
                }
            }
        }

        // If the work was rejected, trace it.
        if (!accept) {
            String msg = td.getQualifiedName() + " rejected, ";
            if (daemonDisabled) {
                wuRejectedOther++;
                msg += "daemon disabled";
            } else if (queue.size() >= MAX_QUEUE_LENGTH) {
                wuRejectedFQ++;
                msg += "queue full";
            } else {
                wuRejectedDup++;
                msg += "duplicate";
            }
            trace(1, msg);
        }
        return accept;
    }

    /**
     * Generates index statistics for all indexes associated with the given
     * table descriptor.
     * <p>
     * This method is run as a background task.
     *
     * @param lcc connection context to use to perform the work
     * @param td target base table descriptor
     * @throws StandardException if accessing the conglomerates fail
     */
    private void generateStatistics(LanguageConnectionContext lcc,
                                    TableDescriptor td)
            throws StandardException {
        trace(1, "processing " + td.getQualifiedName());
        boolean lockConflictSeen = false;
        while (true) {
            try {
                ConglomerateDescriptor[] cds = td.getConglomerateDescriptors();
                updateIndexStatsMinion(lcc, td, cds, AS_BACKGROUND_TASK);
                break;
            } catch (StandardException se) {

                // At this level, we retry the whole operation. If this happens,
                // it normally means that a lengthy operation, or possibly DDL,
                // is taking place (for instance compress table). We retry only
                // once, but wait rather long before doing so.
                // Note that some lower level operations may have tried to
                // aquire the locks several times already, and we may not be
                // able to complete the work if we get here.

                if (se.isLockTimeout() && !lockConflictSeen) {

                    trace(1, "locks unavailable, retrying");
                    lockConflictSeen = true;
                    lcc.internalRollback(); // Get rid of any locks
                    sleep(1000);

                } else {
                    // Rethrow exception, because:
                    //   o error is not a lock timeout
                    //           - or -
                    //   o too many lock timeouts
                    // Transaction will be cleaned up elsewhere.
                    throw se;
                }
            }
        }
    }

    /** Return true if we are being shutdown */
    private boolean isShuttingDown() {
        synchronized (queue) {
            if (daemonDisabled || daemonLCC == null){
                return true;
            } else {
                return !daemonLCC.getDatabase().isActive();
            }
        }
    }

    /**
     * Updates the index statistics for the given table and the specified
     * indexes.
     *
     * @param lcc language connection context used to perform the work
     * @param td the table to update index stats for
     * @param cds the conglomerates to update statistics for (non-index
     *      conglomerates will be ignored)
     * @param asBackgroundTask whether the updates are done automatically as
     *      part of a background task or if explicitly invoked by the user
     * @throws StandardException if something goes wrong
     */
    private void updateIndexStatsMinion(LanguageConnectionContext lcc,
                                        TableDescriptor td,
                                        ConglomerateDescriptor[] cds,
                                        boolean asBackgroundTask)
            throws StandardException {
        // Extract/derive information from the table descriptor
        long[] conglomerateNumber = new long[cds.length];
        ExecIndexRow[] indexRow = new ExecIndexRow[cds.length];
        UUID[] objectUUID = new UUID[cds.length];

        TransactionController tc = lcc.getTransactionExecute();
        ConglomerateController heapCC =
            tc.openConglomerate(td.getHeapConglomerateId(), false,
                    0,
                    TransactionController.MODE_RECORD,
                    asBackgroundTask
                        ? TransactionController.ISOLATION_READ_UNCOMMITTED
                        : TransactionController.ISOLATION_REPEATABLE_READ
                );
        try
        {
            for (int i = 0; i < cds.length; i++)
            {
                if (!cds[i].isIndex())
                {
                    conglomerateNumber[i] = -1;
                    continue;
                }

                conglomerateNumber[i] = cds[i].getConglomerateNumber();

                objectUUID[i] = cds[i].getUUID();

                indexRow[i] =
                    cds[i].getIndexDescriptor().getNullIndexRow(
                        td.getColumnDescriptorList(),
                        heapCC.newRowLocationTemplate());
            }
        }
        finally
        {
            heapCC.close();
        }

        // [x][0] = conglomerate number, [x][1] = start time, [x][2] = stop time
        long[][] scanTimes = new long[conglomerateNumber.length][3];
        int sci = 0;
        for (int indexNumber = 0;
             indexNumber < conglomerateNumber.length;
             indexNumber++)
        {
            if (conglomerateNumber[indexNumber] == -1)
                continue;

            // Check if daemon has been disabled.
            if (asBackgroundTask) {
                if (isShuttingDown()) {
                    break;
                }
            }

            scanTimes[sci][0] = conglomerateNumber[indexNumber];
            scanTimes[sci][1] = System.currentTimeMillis();
            // Subtract one for the RowLocation added for indexes.
            int numCols = indexRow[indexNumber].nColumns() - 1;
            long[] cardinality = new long[numCols];
            KeyComparator cmp = new KeyComparator(indexRow[indexNumber]);

            /* Read uncommitted, with record locking. Actually CS store may
               not hold record locks */
            GroupFetchScanController gsc =
                tc.openGroupFetchScan(
                        conglomerateNumber[indexNumber],
                        false,  // hold
                        0,
                        TransactionController.MODE_RECORD, // locking
                        TransactionController.ISOLATION_READ_UNCOMMITTED,
                        null,   // scancolumnlist-- want everything.
                        null,   // startkeyvalue-- start from the beginning.
                        0,
                        null,   // qualifiers, none!
                        null,   // stopkeyvalue,
                        0);

            try
            {
                int     rowsFetched           = 0;
                boolean giving_up_on_shutdown = false;

                while ((rowsFetched = cmp.fetchRows(gsc)) > 0)
                {
                    // DERBY-5108
                    // Check if daemon has been disabled, and if so stop
                    // scan and exit asap.  On shutdown the system will
                    // send interrupts, but the system currently will
                    // recover from these during the scan and allow the
                    // scan to finish. Checking here after each group
                    // I/O that is processed as a convenient point.
                    if (asBackgroundTask) {
                        if (isShuttingDown()) {
                            giving_up_on_shutdown = true;
                            break;
                        }
                    }

                    for (int i = 0; i < rowsFetched; i++)
                    {
                        int whichPositionChanged = cmp.compareWithPrevKey(i);
                        if (whichPositionChanged >= 0) {
                            for (int j = whichPositionChanged; j < numCols; j++)
                                cardinality[j]++;
                        }
                    }

                } // while

                if (giving_up_on_shutdown)
                    break;

                gsc.setEstimatedRowCount(cmp.getRowCount());
            } // try
            finally
            {
                gsc.close();
                gsc = null;
            }
            scanTimes[sci++][2] = System.currentTimeMillis();

            // We have scanned the indexes, so let's give this a few attempts
            // before giving up.
            int retries = 0;
            while (true) {
                try {
                    writeUpdatedStats(lcc, td, objectUUID[indexNumber],
                            cmp.getRowCount(), cardinality, asBackgroundTask);
                    break;
                } catch (StandardException se) {

                    retries++;

                    if (se.isLockTimeout() && retries < 3) {
                        trace(2, "lock timeout when writing stats, retrying");
                        sleep(100*retries);
                    } else {
                        // Rethrow exception, because:
                        //   o error is not a lock timeout
                        //           - or -
                        //   o too many lock timeouts
                        throw se;
                    }
                }
            }
        }

        log(asBackgroundTask, td, fmtScanTimes(scanTimes));
    }

    /**
     * Writes updated statistics for the specified index to the data dictionary.
     *
     * @param lcc connection context to use to perform the work
     * @param td the base table
     * @param index the index of the base table
     * @param numRows number of rows in the base table
     * @param cardinality the number of unique values in the index (per number
     *      of leading columns)
     * @param asBackgroundTask whether the update is done automatically as
     *      part of a background task or if explicitly invoked by the user
     * @throws StandardException if updating the data dictionary fails
     */
    private void writeUpdatedStats(LanguageConnectionContext lcc,
                                   TableDescriptor td, UUID index,
                                   long numRows, long[] cardinality,
                                   boolean asBackgroundTask)
            throws StandardException {
        TransactionController tc = lcc.getTransactionExecute();
        trace(1, "writing new stats (xid=" + tc.getTransactionIdString() + ")");
        UUID table = td.getUUID();
        DataDictionary dd = lcc.getDataDictionary();
        UUIDFactory uf = dd.getUUIDFactory();

        // Update the heap row count estimate.
        setHeapRowEstimate(tc, td.getHeapConglomerateId(), numRows);
        // Invalidate statments accessing the given table.
        // Note that due to retry logic, swithcing the data dictionary to
        // write mode is done inside invalidateStatements.
        invalidateStatements(lcc, td, asBackgroundTask);
        // Drop existing index statistics for this index.
        dd.dropStatisticsDescriptors(table, index, tc);

        // Don't write statistics if the table is empty.
        if (numRows == 0) {
            trace(2, "empty table, no stats written");
        } else {
            // Construct and add the statistics entries.
            for (int i=0; i < cardinality.length; i++) {
                StatisticsDescriptor statDesc = new StatisticsDescriptor(
                        dd, uf.createUUID(), index, table, "I",
                     new StatisticsImpl(numRows, cardinality[i]),
                     i+1);
                dd.addDescriptor(statDesc, null,
                        DataDictionary.SYSSTATISTICS_CATALOG_NUM, true, tc);
            }

            // Log some information.
            ConglomerateDescriptor cd = dd.getConglomerateDescriptor(index);
            log(asBackgroundTask, td,
                    "wrote stats for index "  + 
                    (cd == null ? "n/a" : cd.getDescriptorName()) +
                    " (" + index + "): rows=" + numRows +
                    ", card=" + cardToStr(cardinality));

            // DERBY-5045: When running as a background task, we don't take
            // intention locks that prevent dropping the table or its indexes.
            // So there is a possibility that this index was dropped before
            // we wrote the statistics to the SYSSTATISTICS table. If the table
            // isn't there anymore, issue a rollback to prevent inserting rows
            // for non-existent indexes in SYSSTATISTICS.
            if (asBackgroundTask && cd == null) {
                log(asBackgroundTask, td,
                    "rolled back index stats because index has been dropped");
                lcc.internalRollback();
            }
        }

        // Only commit tx as we go if running as background task.
        if (asBackgroundTask) {
            lcc.internalCommit(true);
        }
    }

    /**
     * Performs an invalidation action for the given table (the event being
     * statistics update).
     *
     * @param lcc connection context to use to perform the work
     * @param td the table to invalidate for
     * @param asBackgroundTask whether the update is done automatically as
     *      part of a background task or if explicitly invoked by the user
     * @throws StandardException if the invalidation request fails
     */
    private void invalidateStatements(LanguageConnectionContext lcc,
                                      TableDescriptor td,
                                      boolean asBackgroundTask)
            throws StandardException {
        // Invalidate compiled statements accessing the table.
        DataDictionary dd = lcc.getDataDictionary();
        DependencyManager dm = dd.getDependencyManager();
        boolean inWrite = false;
        int retries = 0;
        while (true) {
            try {
                if (!inWrite) {
                    dd.startWriting(lcc);
                    inWrite = true;
                }
                dm.invalidateFor(
                        td, DependencyManager.UPDATE_STATISTICS, lcc);
                trace(1, "invalidation completed");
                break;
            } catch (StandardException se) {
                // Special handling when running as background task.

                if (se.isLockTimeout() && asBackgroundTask && retries < 3) {
                    retries++;
                    // If this is the first time we retry, don't roll back.
                    // If we already waited once, but still didn't get the
                    // locks, back down by releasing our own locks.
                    if (retries > 1) {
                        trace(2, "releasing locks");
                        lcc.internalRollback();
                        inWrite = false;
                    }
                    trace(2, "lock timeout when invalidating");
                    sleep(100*(1+retries)); // adaptive sleeping...
                } else {
                    // Throw exception because of one of:
                    //  o it isn't a lock timeout
                    //          - or -
                    //  o we gave up retrying
                    //          - or -
                    //  o we are running in explicit mode
                    trace(1, "invalidation failed");
                    throw se;
                }
            }
        }
    }

    /**
     * Sets the row estimate for the heap conglomerate.
     *
     * @param tc transaction to use
     * @param tableId the heap table
     * @param rowEstimate estimate of number of rows in the table
     * @throws StandardException if accessing the table fails
     */
    private void setHeapRowEstimate(TransactionController tc, long tableId,
                                    long rowEstimate)
            throws StandardException {
        // DERBY-4116: If we know the row count, update the store estimated row
        // count for the table.
        ScanController sc = tc.openScan(
                tableId,
                false,  // hold
                0,      // openMode: for read
                TransactionController.MODE_RECORD, // locking
                TransactionController.ISOLATION_READ_UNCOMMITTED, //iso level
                null,   // scancolumnlist-- want everything.
                null,   // startkeyvalue-- start from the beginning.
                0,
                null,   // qualifiers, none!
                null,   // stopkeyvalue,
                0);

        try {
            sc.setEstimatedRowCount(rowEstimate);
        } finally {
            sc.close();
        }
    }

    /**
     * Drives the statistics generation.
     * <p>
     * This method will be run in a separate thread, and it will keep working
     * as long as there is work to do. When the queue is exhausted, the method
     * will exit (the thread dies).
     */
    public void run() {
        final long runStart = System.currentTimeMillis();
        ContextService ctxService = null;
        // Implement the outer-level exception handling here.
        try {
            // DERBY-5088: Factory-call may fail.
            ctxService = ContextService.getFactory();
            ctxService.setCurrentContextManager(ctxMgr);
            processingLoop();
        } catch (ShutdownException se) {
            // The database is/has been shut down.
            // Log processing statistics and exit.
            trace(1, "swallowed shutdown exception: " + extractIstatInfo(se));
            stop();
            ctxMgr.cleanupOnError(se, db.isActive());
        } catch (RuntimeException re) {
            // DERBY-4037
            // Extended filtering of runtime exceptions during shutdown:
            //  o assertions raised by debug jars
            //  o runtime exceptions, like NPEs, raised by production jars -
            //    happens because the background thread interacts with store
            //    on a lower level
            if (!isShuttingDown()) {
                log(AS_BACKGROUND_TASK, null, re,
                        "runtime exception during normal operation");
                throw re;
            }
            trace(1, "swallowed runtime exception during shutdown: " +
                    extractIstatInfo(re));
        } finally {
            if (ctxService != null) {
                ctxService.resetCurrentContextManager(ctxMgr);
            }
            runTime += (System.currentTimeMillis() - runStart);
            trace(0, "worker thread exit");
        }
    }

    /**
     * Main processing loop which will compute statistics until the queue
     * of scheduled work units has been drained.
     */
    private void processingLoop() {
        // If we don't have a connection to the database, create one.
        if (daemonLCC == null) {
            try {
                daemonLCC =
                        db.setupConnection(ctxMgr, dbOwner, null, databaseName);
                // Initialize the lcc/transaction.
                // TODO: Would be nice to name the transaction.
                daemonLCC.setIsolationLevel(
                        Connection.TRANSACTION_READ_UNCOMMITTED);
                // Don't wait for any locks.
                daemonLCC.getTransactionExecute().setNoLockWait(true);
            } catch (StandardException se) {
                log(AS_BACKGROUND_TASK, null, se,
                        "failed to initialize index statistics updater");
                return;
            }
        }

        TransactionController tc = null;
        try {
            tc = daemonLCC.getTransactionExecute();
            trace(0, "worker thread started (xid=" +
                    tc.getTransactionIdString() + ")");

            TableDescriptor td = null;
            long start = 0;
            while (true) {
                synchronized (queue) {
                    if (daemonDisabled) {
                        // Clean the lcc and exit.
                        try {
                            tc.destroy();
                        } catch (ShutdownException se) {
                            // Ignore
                        }
                        tc = null;
                        daemonLCC = null;
                        queue.clear();
                        trace(1, "daemon disabled");
                        break;
                    }
                    if (queue.isEmpty()) {
                        trace(1, "queue empty");
                        break;
                    }
                    td = (TableDescriptor)queue.get(0);
                }
                try {
                    start = System.currentTimeMillis();
                    generateStatistics(daemonLCC, td);
                    wuProcessed++;
                    // Reset consecutive error counter.
                    errorsConsecutive = 0;
                    log(AS_BACKGROUND_TASK, td, "generation complete (" +
                            ((System.currentTimeMillis() - start))  + " ms)");
                } catch (StandardException se) {
                    errorsConsecutive++;
                    // Assume handling of fatal errors will clean up properly.
                    // For less severe errors, rollback tx to clean up.
                    if (!handleFatalErrors(ctxMgr, se)) {
                        boolean handled = handleExpectedErrors(td, se);
                        if (!handled) {
                            handled = handleUnexpectedErrors(td, se);
                        }
                        daemonLCC.internalRollback();
                        if (SanityManager.DEBUG) {
                            SanityManager.ASSERT(handled);
                        }
                    }
                } finally {
                    // Whatever happened, discard the unit of work.
                    synchronized (queue) {
                        // Queue may have been cleared due to shutdown.
                        if (!queue.isEmpty()) {
                            queue.remove(0);
                        }
                    }
                    // If we have seen too many consecutive errors, disable
                    // the daemon. 50 was chosen based on gut-feeling...
                    // Hopefully it can withstand shortlived "hick-ups", but
                    // will cause shutdown if there is a real problem.
                    // Create an exception to force logging of the message.
                    if (errorsConsecutive >= 50) {
                        log(AS_BACKGROUND_TASK, null,
                                new IllegalStateException("degraded state"),
                                "shutting down daemon, " + errorsConsecutive +
                                " consecutive errors seen");
                        stop();
                    }
                }
            }
        } catch (StandardException se) {
            log(AS_BACKGROUND_TASK, null, se, "thread died");
            // Do nothing, just let the thread die.
        } finally {
            synchronized (queue) {
                runningThread = null;
            }
            if (daemonLCC != null && !daemonLCC.isTransactionPristine()) {
                if (SanityManager.DEBUG) {
                    SanityManager.THROWASSERT("transaction not pristine");
                }
                log(AS_BACKGROUND_TASK, null,
                        "transaction not pristine - forcing rollback");
                try {
                    daemonLCC.internalRollback();
                } catch (StandardException se) {
                    // Log, then continue.
                    log(AS_BACKGROUND_TASK, null, se, "forced rollback failed");
                }
            }
        }
    }

    /**
     * Runs the statistics update sequence explicitly as requested by the user.
     *
     * @param lcc connection context to use to perform the work
     * @param td the base table
     * @param cds the indexes to update (non-index conglomerates are ignored)
     * @param runContext the context in which the operation is run (i.e.
     *      'ALTER TABLE', may be {@code null})
     * @throws StandardException if updating the index statistics fails
     */
    public void runExplicitly(LanguageConnectionContext lcc,
                              TableDescriptor td,
                              ConglomerateDescriptor[] cds,
                              String runContext)
            throws StandardException {
        updateIndexStatsMinion(lcc, td, cds, AS_EXPLICIT_TASK);
        trace(0, "explicit run completed" + (runContext != null
                                        ? " (" + runContext + "): "
                                        : ": ") +
                                    td.getQualifiedName());
    }

    /**
     * Stops the daemon.
     * <p>
     * Will also clear the queue and print runtime statistics to the log the
     * first time the method is invoked.
     */
    public void stop() {
        Thread threadToWaitFor = null;
        // Controls execution of last cleanup step outside of the synchronized
        // block. Should only be done once, and this is ensured by the guard on
        // 'queue' and the value of 'daemonDisabled'.
        boolean clearContext = false;

        synchronized (queue) {
            if (!daemonDisabled) {
                clearContext = true;
                StringBuffer sb = new StringBuffer(100);
                sb.append("stopping daemon, active=").
                        append(runningThread != null).
                        append(", work/age=").append(runTime).append('/').
                        append(System.currentTimeMillis() - timeOfCreation).
                        append(' ');
                appendRunStats(sb);
                log(AS_BACKGROUND_TASK, null, sb.toString());
                // If there is no running thread and the daemon lcc is still
                // around, destroy the transaction and clear the lcc reference.
                if (runningThread == null && daemonLCC != null &&
                        !isShuttingDown()) {
                    // try/catch as safe-guard against shutdown race condition.
                    try {
                        daemonLCC.getTransactionExecute().destroy();
                    } catch (ShutdownException se) {
                        // Ignore
                    }
                    daemonLCC = null;
                }
                daemonDisabled = true;
                threadToWaitFor = runningThread;
                runningThread = null;
                queue.clear();
            }
        }

        // Wait for the currently running thread, if there is one. Must do
        // this outside of the synchronized block so that we don't deadlock
        // with the thread.
        if (threadToWaitFor != null) {
            while (true) {
                try {
                    threadToWaitFor.join();
                    break;
                } catch (InterruptedException ie) {
                    InterruptStatus.setInterrupted();
                }
            }

        }

        // DERBY-5447: Remove the context only after the running daemon thread
        //             (if any) has been shut down to avoid Java deadlocks
        //             when closing the container handles obtained with this
        //             context.
        if (clearContext) {
            // DERBY-5336: Trigger cleanup code to remove the context
            //             from the context service. This pattern was
            //             copied from BasicDaemon.
            ctxMgr.cleanupOnError(StandardException.normalClose(), false);
        }
    }


    /**
     * Handles fatal errors that will cause the daemon to be shut down.
     *
     * @param cm context manager
     * @param se the exception to handle
     * @return {@code true} if the error was handled, {@code false} otherwise
     */
    private boolean handleFatalErrors(ContextManager cm, StandardException se) {
        boolean disable = false;
        if (SQLState.DATA_CONTAINER_READ_ONLY.equals(se.getMessageId())) {
            // We are not allowed to write into the database, most likely the
            // data dictionary. No point to keep doing work we can't gain from.
            disable = true;
        } else if (isShuttingDown() ||
                se.getSeverity() >= ExceptionSeverity.DATABASE_SEVERITY) {
            // DERBY-4037: Swallow exceptions raised during shutdown.
            // The database or system is going down. Probably handled elsewhere
            // but disable daemon anyway.
            trace(1, "swallowed exception during shutdown: " +
                    extractIstatInfo(se));
            disable = true;
            cm.cleanupOnError(se, db.isActive());
        }

        if (disable) {
            daemonLCC.getDataDictionary().disableIndexStatsRefresher();
        }
        return disable;
    }

    /**
     * Handles expected errors.
     * <p>
     * The logging of expected errors is for observability purposes only. The
     * daemon is capable of dealing with these errors, and no interaction from
     * the user is expected.
     *
     * @param se the exception to handle
     * @return {@code true} if the error was handled, {@code false} otherwise
     */
    private boolean handleExpectedErrors(TableDescriptor td,
                                         StandardException se) {
        String state = se.getMessageId();
        // Accept that the heap/index/conglomerate has been deleted since the
        // work for it was scheduled. Just ignore the unit of work and continue.
        if (SQLState.STORE_CONGLOMERATE_DOES_NOT_EXIST.equals(state) ||
            SQLState.HEAP_CONTAINER_NOT_FOUND.equals(state) ||
            SQLState.FILE_IO_INTERRUPTED.equals(state) ||
            se.isLockTimeout()) {

            errorsKnown++;
            log(AS_BACKGROUND_TASK, td, "generation aborted (reason: " +
                    state + ") {" + extractIstatInfo(se) + "}");
            return true;
        }
        return false;
    }

    /**
     * Handles unexpected errors.
     * <p>
     * Unexpected errors are error conditions the daemon isn't set up to handle
     * specifically. For this reason the stack trace will be logged to allow
     * for later investigation.
     * <p>
     * In general it is expected that the daemon will be able to recover by
     * dropping the current unit of work and move on to the next one (if any).
     *
     * @param se the exception to handle
     * @return {@code true} if the error was handled, {@code false} otherwise
     */
    private boolean handleUnexpectedErrors(TableDescriptor td,
                                           StandardException se) {
        errorsUnknown++;
        log(AS_BACKGROUND_TASK, td, se, "generation failed");
        return true;
    }

    /**
     * Puts the current thread to sleep for maximum {@code ms} milliseconds.
     * <p>
     * No guarantee is provided for the minimum amount of time slept. If
     * interrupted, the interrupt flag will be set again.
     *
     * @param ms target sleep time
     */
    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            InterruptStatus.setInterrupted();
        }
    }

    /** Format array of scan durations as a string. */
    private static String fmtScanTimes(long[][] timings) {
        // timings[x] = [conglomId, start, end]
        StringBuffer sb = new StringBuffer("scan durations (");
        for (int i=0; i < timings.length && timings[i][0] > 0; i++) {
            sb.append('c').append(timings[i][0]).append('=');
            // Handle corner-case where the scans are aborted due to the
            // index statistics daemon being shut down under us.
            if (timings[i][2] == 0) {
                sb.append("ABORTED,");   
            } else {
                long duration = timings[i][2] - timings[i][1];
                sb.append(duration).append("ms,");
            }
        }
        sb.deleteCharAt(sb.length() -1).append(")");
        return sb.toString();
    }

    /** @see #log(boolean, TableDescriptor, Throwable, String)  */
    private void log(boolean asBackgroundTask, TableDescriptor td, String msg) {
        log(asBackgroundTask, td, null, msg);
    }

    /**
     * Logs the information given.
     * <p>
     * Note that if {@code asBackgroundTask} is false, nothing will be logged
     * currently.
     *
     * @param asBackgroundTask {@code true} if logging for the background
     *      daemon automatically updating stats, {@code false} if not
     * @param td current table descriptor being worked on, may be {@code null}
     * @param t raised error, may be {@code null}
     * @param msg the message to log
     */
    private void log(boolean asBackgroundTask, TableDescriptor td, Throwable t,
            String msg) {
        if (asBackgroundTask && (doLog || t != null)) {
            PrintWriter pw = null;
            String hdrMsg = "{istat} " +
                    (td == null ? "" : td.getQualifiedName() + ": ") + msg;
            if (t != null) {
                pw = new PrintWriter(logStream.getPrintWriter(), false);
                pw.print(logStream.getHeader().getHeader());
                pw.println(hdrMsg);
                t.printStackTrace(pw);
                pw.flush();
            } else {
                logStream.printlnWithHeader(hdrMsg);
            }
        }
    }

    // @GuardedBy("this")
    private final StringBuffer tsb = new StringBuffer();
    private synchronized void trace(int indentLevel, String msg) {
        if (doTrace) {
            tsb.setLength(0);
            tsb.append("{istat,trace@").append(hashCode()).append("} ");
            for (int i=0; i < indentLevel; i++) {
                tsb.append("    ");
            }
            tsb.append(msg).append(' ');
            if (indentLevel == 0) {
                appendRunStats(tsb);
            }
            if (traceToDerbyLog && logStream != null) {
                logStream.printlnWithHeader(tsb.toString());
            }
            if (traceToStdOut) {
                System.out.println(tsb.toString());
            }
        }
    }

    /**
     * Appends runtime statistics to the given string buffer.
     *
     * @param sb the string buffer to append to
     */
    // @GuardedBy("queue")
    private void appendRunStats(StringBuffer sb) {
        // Print rather detailed and cryptic stats.
        sb.append("[q/p/s=").append(queue.size()). // current queue size
                append('/').append(wuProcessed). // scheduled
                append('/').append(wuScheduled). //processed
                append(",err:k/u/c=").
                append(errorsKnown). // known errors
                append('/').append(errorsUnknown). // unexpected errors
                append('/').append(errorsConsecutive). // consecutive errors
                append(",rej:f/d/o=").
                append(wuRejectedFQ). // rejected, full queue
                append('/').append(wuRejectedDup). // rejected, duplicate
                append('/').append(wuRejectedOther). // rejected, other
                append(']');
    }

    /**
     * Produces a textual representation of the cardinality numbers.
     *
     * @param cardinality index cardinality
     * @return A string.
     */
    private static String cardToStr(long[] cardinality) {
        if (cardinality.length == 1) {
            return "[" + Long.toString(cardinality[0]) + "]";
        } else {
            StringBuffer sb = new StringBuffer("[");
            for (int i=0; i < cardinality.length; i++) {
                sb.append(cardinality[i]).append(',');
            }
            sb.deleteCharAt(sb.length() -1).append(']');
            return sb.toString();
        }
    }

    /** Purely for debugging, to avoid printing too much info. */
    private static String extractIstatInfo(Throwable t) {
        String istatClass = IndexStatisticsDaemonImpl.class.getName();
        StackTraceElement[] stack = t.getStackTrace();
        String trace = "<no stacktrace>";
        String sqlState = "";
        for (int i=0; i < stack.length ; i++) {
            StackTraceElement ste = stack[i];
            if (ste.getClassName().startsWith(istatClass)) {
                trace = ste.getMethodName() + "#" + ste.getLineNumber();
                if (i > 0) {
                    ste = stack[i -1];
                    trace += " -> " + ste.getClassName() + "." +
                            ste.getMethodName() + "#" + ste.getLineNumber();
                }
                break;
            }
        }
        if (t instanceof StandardException) {
            sqlState = ", SQLSTate=" + ((StandardException)t).getSQLState();
        }
        return "<" + t.getClass() + ", msg=" + t.getMessage() + sqlState +
                "> " + trace;
    }

    /**
     * Support class used to compare keys when scanning indexes.
     */
    //@NotThreadSafe
    private static class KeyComparator {

        /** Number of rows fetched per iteration. */
        private static final int FETCH_SIZE = 16;
        private final DataValueDescriptor[][] rowBufferArray;
        private DataValueDescriptor[] lastUniqueKey;
        private DataValueDescriptor[] curr;
        private DataValueDescriptor[] prev;
        private int rowsReadLastRead = -1;
        private long numRows;

        /**
         * Creates a key comparator for the given index.
         *
         * @param ir index row (template)
         */
        public KeyComparator(ExecIndexRow ir) {
            rowBufferArray = new DataValueDescriptor[FETCH_SIZE][];
            rowBufferArray[0] = ir.getRowArray(); // 1 gets old objects.
            lastUniqueKey = ir.getRowArrayClone();
        }

        /**
         * Fetches rows from the scan controller.
         *
         * @param gsc the scan controller
         * @return Number of rows fetched.
         * @throws StandardException if fetching rows fails
         */
        public int fetchRows(GroupFetchScanController gsc)
                throws StandardException {
            // Save state (and optimize) for next read.
            // Assumes that we always read as many rows as we can per iteration.
            if (rowsReadLastRead == FETCH_SIZE) {
                // Reuse curr to reference the current last unique key.
                curr = rowBufferArray[FETCH_SIZE - 1];
                // Reuse the old last unique row array for the coming fetch.
                rowBufferArray[FETCH_SIZE - 1] = lastUniqueKey;
                // Finally we update the pointer to the last unique key.
                lastUniqueKey = curr;
            }
            rowsReadLastRead = gsc.fetchNextGroup(rowBufferArray, null);
            return rowsReadLastRead;
        }

        /**
         * Compares the key at the specified index with the previous key.
         *
         * @param index row index
         * @return {@code -1} if the current and previous key are identical,
         *      the index of the changed part of the key otherwise
         *      ([0, key length>)
         * @throws StandardException if comparing the two keys fails
         */
        public int compareWithPrevKey(int index)
                throws StandardException {
            if (index > rowsReadLastRead) {
                throw new IllegalStateException(
                        "invalid access, rowsReadLastRead=" + rowsReadLastRead +
                        ", index=" + index + ", numRows=" + numRows);
            }
            numRows++;
            // First row ever is always a distinct key.
            if (numRows == 1) {
                return 0;
            }

            prev = (index == 0) ? lastUniqueKey
                                : rowBufferArray[index - 1];
            curr = rowBufferArray[index];
            DataValueDescriptor dvd;
            // no point trying to do rowlocation; hence - 1
            for (int i = 0; i < (prev.length - 1); i++) {
                dvd = (DataValueDescriptor)prev[i];

                // NULLs are counted as unique values.
                if (dvd.isNull() || prev[i].compare(curr[i]) != 0) {
                  return i;
                }
            }
            return -1;
        }

        /**
         * Returns the number of rows fetched.
         *
         * @return Number of rows fetched.
         */
        public long getRowCount() {
            return numRows;
        }
    }
}
