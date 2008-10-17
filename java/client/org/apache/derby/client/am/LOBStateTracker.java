/*

   Derby - Class org.apache.derby.client.am.LOBStateTracker

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*/
package org.apache.derby.client.am;

import java.util.Arrays;

/**
 * An object that tracks the state of large objects (LOBs) for the current row
 * in a result set.
 * <p>
 * A LOB's state is either unpublished or published. When a LOB is published, it
 * means that the end-user has been given a reference to the LOB object. This
 * implies that the LOB cannot be automatically freed/released when the
 * result set position changes (i.e. typically {@code rs.next()}), because the
 * LOB object must be kept valid/alive until the transaction is ended or the
 * LOB object is explicitly freed.
 * <p>
 * This class covers two types of functionality regarding LOBs;
 * <ul>
 *      <li>Keep track of whether a LOB column has been published or not.</li>
 *      <li>Release LOB locators on the server.</li>
 * </ul>
 * Both functionalities will be disabled if the server doesn't support locators.
 * If locators are enabled, they will be freed when {@link #checkCurrentRow} is
 * called.
 * <p>
 * The tracker has a notion of current row. The current row is changed by
 * calling {@link #checkCurrentRow checkCurrentRow}. The owner of the tracker
 * is repsonsible for invoking the method at the correct time, and only when
 * the cursor is positioned on a valid data row. The method must be invoked
 * before the cursor changes the position. Note that calling the method
 * {@link #discardState discardState} makes {@code checkCurrentRow} ignore all
 * LOBs on the subsequent call.
 */
class LOBStateTracker {

    /**
     * Instance to use when there are no LOBs in the result set, or when the
     * server doesn't support locators.
     */
    public static final LOBStateTracker NO_OP_TRACKER =
            new LOBStateTracker(new int[0], new boolean[0], false);
    /** 1-based column indexes for the LOBs to track. */
    private final int[] columns;
    /** Tells whether the LOB is Blob or a Clob. */
    private final boolean[] isBlob;
    /** Tells whether the LOB colum has been published for the current row. */
    private final boolean[] published;
    /**
     * Tells whether locators shall be released. This will be {@code false} if
     * locators are not supported by the server.
     */
    private final boolean doRelease;
    /**
     * The last locator values seen when releasing. These values are used to
     * detect if {@linkplain #checkCurrentRow} is being executed more than once
     * on the same row.
     */
    private final int[] lastLocatorSeen;

    /**
     * Creates a LOB state tracker for the specified configuration.
     *
     * @param lobIndexes the 1-based indexes of the LOB columns
     * @param isBlob whether the LOB is a Blob or a Clob
     * @param doRelease whether locators shall be released
     * @see #NO_OP_TRACKER
     */
    LOBStateTracker(int[] lobIndexes, boolean[] isBlob, boolean doRelease) {
        this.columns = lobIndexes;
        this.isBlob = isBlob;
        this.published = new boolean[columns.length];
        this.doRelease = doRelease;
        // Zero is an invalid locator, don't fill with a valid value.
        this.lastLocatorSeen = new int[columns.length];
    }

    /**
     * Checks the current row, updating state and releasing locators on the
     * server as required.
     * <p>
     * This method should only be called once per valid row in the result set.
     *
     * @param cursor the cursor object to use for releasing the locators
     * @throws SqlException if releasing the locators on the server fails
     */
    void checkCurrentRow(Cursor cursor)
            throws SqlException {
        if (this.doRelease) {
            CallableLocatorProcedures procs = cursor.getLocatorProcedures();
            for (int i=0; i < this.columns.length; i++) {
                // Note the conversion from 1-based to 0-based index when
                // checking if the column has a NULL value.
                if (!this.published[i] && !cursor.isNull_[this.columns[i] -1]) {
                    // Fetch the locator so we can free it.
                    int locator = cursor.locator(this.columns[i]);
                    if (locator == this.lastLocatorSeen[i]) {
                        // We are being called on the same row twice...
                        return;
                    } else if (locator == Lob.INVALID_LOCATOR) {
                        // The locator is invalid, probably because the
                        // database is running in soft upgrade mode and
                        // doesn't have the neccessary stored procedures.
                        // Don't try to release an invalid locator.
                        return;
                    }
                    this.lastLocatorSeen[i] = locator;
                    if (this.isBlob[i]) {
                        procs.blobReleaseLocator(locator);
                    } else {
                        procs.clobReleaseLocator(locator);
                    }
                }
            }
            // Reset state for the next row.
            Arrays.fill(this.published, false);
        }
    }

    /**
     * Discards all recorded dynamic state about LOBs.
     * <p>
     * Typically called after connection commit or rollback, as those operations
     * will release all locators on the server automatically. There is no need
     * to release them from the client side in this case.
     */
    void discardState() {
        if (this.doRelease) {
            // Force the state to published for all LOB columns.
            // This will cause checkCurrentRow to ignore all LOBs on the next
            // invocation. The method markAsPublished cannot be called before
            // after checkCurrentRow has been called again.
            Arrays.fill(this.published, true);
        }
    }

    /**
     * Marks the specified column of the current row as published, which implies
     * that the tracker should not release the associated locator.
     * <p>
     * Columns must be marked as published when a LOB object is created on
     * the client, to avoid releasing the corresponding locator too early.
     *
     * @param index 1-based column index
     */
    void markAsPublished(int index) {
        if (this.doRelease) {
            int internalIndex = Arrays.binarySearch(this.columns, index);
            this.published[internalIndex] = true;
        }
    }
}
