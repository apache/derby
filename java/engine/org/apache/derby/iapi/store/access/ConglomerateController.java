/*

   Derby - Class org.apache.derby.iapi.store.access.ConglomerateController

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.iapi.store.access;

import org.apache.derby.iapi.store.access.RowUtil;

import org.apache.derby.iapi.services.io.Storable;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;

import java.util.Properties;


/**

A conglomerate is an abstract storage structure (they
correspond to access methods).  The ConglomerateController interface
is the interface that access manager clients can use to manipulate
the contents of the underlying conglomerate.
<p>
Each conglomerate holds a set of rows.  Each row has a row location.
The conglomerate provides methods for:
<ul>
<li>
Inserting rows,
<li>
Fetching, deleting, and replacing entire rows by row location, and
<li>
fetching and updating individual columns of a row identified by row
location.
</ul>
<p>
Conglomerates do not provide any mechanism for associative access to
rows within the conglomerate; this type of access is provided by scans
via the ScanController interface.
<p>
Although all conglomerates have the same interface, they have different
implementations.  The implementation of a conglomerate determines some
of its user-visible semantics; for example whether the rows are ordered
or what the types of the rows' columns must be.  The implementation is
specified by an implementation id.  Currently there are two implementations,
"heap", and "btree".  The details of their behavior are specified in their
implementation documentation.  (Currently, only "heap" is implemented).
<p>
All conglomerate operations are subject to the transactional isolation
of the transaction they were opened from.  Transaction rollback will
close all conglomerates.  Transaction commit will close all non-held
conglomerates.
<p>
Scans are opened from a TransactionController.
<P>
A ConglomerateController can handle partial rows. Partial rows
are described in RowUtil.

@see TransactionController#openConglomerate
@see RowUtil
*/

public interface ConglomerateController extends ConglomPropertyQueryable
{
    public static final int ROWISDUPLICATE = 1;

    /**
     * Close the conglomerate controller.
     * <p>
     * Close the conglomerate controller.  Callers must not use
	 * the conglomerate controller after calling close.  It is
	 * strongly recommended that callers clear out the reference
	 * after closing, e.g., 
	 * <p>
	 * <blockquote><pre>
	 * ConglomerateController cc;
	 * cc.close;
	 * cc = null;
	 * </pre></blockquote>
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void close()
        throws StandardException;

    /**
     * Close conglomerate controller as part of terminating a transaction.
     * <p>
     * Use this call to close the conglomerate controller resources as part of
     * committing or aborting a transaction.  The normal close() routine may 
     * do some cleanup that is either unnecessary, or not correct due to the 
     * unknown condition of the controller following a transaction ending error.
     * Use this call when closing all controllers as part of an abort of a 
     * transaction.
     * <p)
     * This call is meant to only be used internally by the Storage system,
     * clients of the storage system should use the simple close() interface.
     * <p>
     * RESOLVE (mikem) - move this call to ConglomerateManager so it is
     * obvious that non-access clients should not call this.
     *
     * @param closeHeldController     If true, means to close controller even if
     *                                it has been opened to be kept opened 
     *                                across commit.  This is
     *                                used to close these controllers on abort.
     *
	 * @return boolean indicating that the close has resulted in a real close
     *                 of the controller.  A held scan will return false if 
     *                 called by closeForEndTransaction(false), otherwise it 
     *                 will return true.  A non-held scan will always return 
     *                 true.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    boolean closeForEndTransaction(boolean closeHeldScan)
		throws StandardException;

    /**
    Check consistency of a conglomerate.

    Checks the consistency of the data within a given conglomerate, does not
    check consistency external to the conglomerate (ie. does not check that 
    base table row pointed at by a secondary index actually exists).

    Raises a StandardException on first consistency problem. 
    
	@exception StandardException Standard exception policy.
    **/
    void checkConsistency()
		throws StandardException;

    /**
    Delete a row from the conglomerate.  
	@return Returns true if delete was successful, false if the record pointed
	at no longer represents a valid record.
	@exception StandardException Standard exception policy.
    **/
    boolean delete(RowLocation loc)
		throws StandardException;

    /**
     * Fetch the (partial) row at the given location.
     * <p>
     *
	 * @param loc             The "RowLocation" which describes the exact row
     *                        to fetch from the table.
	 * @param destRow         The row to read the data into.
	 * @param validColumns    A description of which columns to return from
     *                        row on the page into "destRow."  destRow
     *                        and validColumns work together to
     *                        describe the row to be returned by the fetch - 
     *                        see RowUtil for description of how these three 
     *                        parameters work together to describe a fetched 
     *                        "row".
     *
	 * @return Returns true if fetch was successful, false if the record 
     *         pointed at no longer represents a valid record.
     *
	 * @exception  StandardException  Standard exception policy.
     *
	 * @see RowUtil
     **/
    boolean fetch(
    RowLocation             loc, 
    DataValueDescriptor[]   destRow, 
    FormatableBitSet                 validColumns) 
		throws StandardException;

    /**
     * Fetch the (partial) row at the given location.
     * <p>
     *
	 * @param loc             The "RowLocation" which describes the exact row
     *                        to fetch from the table.
	 * @param destRow         The row to read the data into.
	 * @param validColumns    A description of which columns to return from
     *                        row on the page into "destRow."  destRow
     *                        and validColumns work together to
     *                        describe the row to be returned by the fetch - 
     *                        see RowUtil for description of how these three 
     *                        parameters work together to describe a fetched 
     *                        "row".
	 * @param waitForLock     If false, then the call will throw a lock timeout
     *                        exception immediately, if the lock can not be
     *                        granted without waiting.  If true call will 
     *                        act exactly as fetch() interface with no 
     *                        waitForLock parameter.
     *
	 * @return Returns true if fetch was successful, false if the record 
     *         pointed at no longer represents a valid record.
     *
	 * @exception  StandardException  Standard exception policy.
     *
	 * @see RowUtil
     **/
    boolean fetch(
    RowLocation loc, 
    DataValueDescriptor[]   destRow, 
    FormatableBitSet     validColumns,
    boolean     waitForLock) 
		throws StandardException;

    /**
     * Fetch the (partial) row at the given location.
     * <p>
     * RESOLVE - interface NOT SUPPORTED YET!!!!!
     *
	 * @param loc             The "RowLocation" which describes the exact row
     *                        to fetch from the table.
	 * @param destRow         The row to read the data into.
	 * @param validColumns    A description of which columns to return from
     *                        row on the page into "destRow."  destRow,
     *                        and validColumns work together to
     *                        describe the row to be returned by the fetch - 
     *                        see RowUtil for description of how these three 
     *                        parameters work together to describe a fetched 
     *                        "row".
	 * @param qualifier       An array of qualifiers which, 
     *                        applied to each key, restrict the rows returned 
     *                        by the scan.  Rows for which any one of the 
     *                        qualifiers returns false are not returned by 
     *                        the scan. If null, all rows are returned.  
     *                        Qualifiers can only reference columns which are 
     *                        included in the scanColumnList.  The column id 
     *                        that a qualifier returns in the column id the 
     *                        table, not the column id in the partial row being
     *                        returned.  See openScan() for description of how 
     *                        qualifiers are applied.
     *
	 * @return Returns true if fetch was successful, false if the record 
     *         pointed at no longer represents a valid record.
     *
	 * @exception  StandardException  Standard exception policy.
     *
	 * @see RowUtil
     **/
    /*
    boolean fetch(
    RowLocation             loc, 
    DataValueDescriptor[]   destRow, 
    FormatableBitSet                 validColumns, 
    Qualifier[][]           qualifier)
		throws StandardException;
    */

	/**
    Insert a row into the conglomerate.

    @param row The row to insert into the conglomerate.  The stored
	representations of the row's columns are copied into a new row
	somewhere in the conglomerate.

	@return Returns 0 if insert succeeded.  Returns 
    ConglomerateController.ROWISDUPLICATE if conglomerate supports uniqueness
    checks and has been created to disallow duplicates, and the row inserted
    had key columns which were duplicate of a row already in the table.  Other
    insert failures will raise StandardException's.

	@exception StandardException Standard exception policy.
	@see RowUtil
    **/
	int insert(DataValueDescriptor[]    row) 
		throws StandardException;

    /**
     * insert row and fetch it's row location in one operation.
     * <p>
     * Insert a row into the conglomerate, and store its location in 
     * the provided destination row location.  The row location must be of the
     * correct type for this conglomerate (a new row location of the correct 
     * type can be obtained from newRowLocationTemplate()).
     *
     * @param row           The row to insert into the conglomerate.  The 
     *                      stored representations of the row's columns are 
     *                      copied into a new row somewhere in the conglomerate.
     *
     * @param destRowLocation The rowlocation to read the inserted row location
     *                      into.
     *
	 * @exception  StandardException  Standard exception policy.
     *
	 * @see RowUtil
     **/
	void insertAndFetchLocation(
    DataValueDescriptor[]   row, 
    RowLocation             destRowLocation)
		throws StandardException;

    /**
	Return whether this is a keyed conglomerate.
	**/
	boolean isKeyed();


    public static final int LOCK_READ         = (0x00000000);
    public static final int LOCK_UPD          = (0x00000001);
    public static final int LOCK_INS          = (0x00000002);
    public static final int LOCK_INS_PREVKEY  = (0x00000004);
    public static final int LOCK_UPDATE_LOCKS = (0x00000008);

    /**
     * Lock the given row location.
     * <p>
     * Should only be called by access.
     * <p>
     * This call can be made on a ConglomerateController that was opened
     * for locking only.
     * <p>
     * RESOLVE (mikem) - move this call to ConglomerateManager so it is
     * obvious that non-access clients should not call this.
     *
	 * @return true if lock was granted, only can be false if wait was false.
     *
	 * @param loc           The "RowLocation" of the exact row to lock.
     * @param lock_oper     For what operation are we requesting the lock, this
     *                      should be one of the following 4 options:
     *                      LOCK_READ [read lock], 
     *                      (LOCK_INS | LOCK_UPD) [ lock for insert], 
     *                      (LOCK_INSERT_PREVKEY | LOCK_UPD) [lock for 
     *                      previous key to insert],
     *                      (LOCK_UPD) [lock for delete or replace]
     *                      (LOCK_UPD | LOCK_UPDATE_LOCKS) [lock scan for 
     *                          update, will upgrade lock later if actual update
     *                          is take place]
     * @param wait          Should the lock call wait to be granted?
     * @param lock_duration If set to TransactionManager.LOCK_INSTANT_DURATION,
     *                      then lock will be released immediately after being
     *                      granted.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    boolean lockRow(
    RowLocation     loc,
    int             lock_oper,
    boolean         wait,
    int             lock_duration)
        throws StandardException;

    /**
     * Lock the given record id/page num pair.
     * <p>
     * Should only be called by access, to lock "special" locks formed from
     * the Recordhandle.* reserved constants for page specific locks.
     * <p>
     * This call can be made on a ConglomerateController that was opened
     * for locking only.
     * <p>
     * RESOLVE (mikem) - move this call to ConglomerateManager so it is
     * obvious that non-access clients should not call this.
     *
	 * @return true if lock was granted, only can be false if wait was false.
     *
	 * @param loc           The "RowLocation" of the exact row to lock.
     * @param forUpdate     Lock the record for read or write.
     * @param forInsert     Lock the record for insert?
     * @param wait          Should the lock call wait to be granted?
     * @param lock_duration If set to TransactionManager.LOCK_INSTANT_DURATION,
     *                      then lock will be released immediately after being
     *                      granted.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    boolean lockRow(
    long            page_num,
    int             record_id,
    int             lock_oper,
    boolean         wait,
    int             lock_duration)
        throws StandardException;

    /**
     * UnLock the given row location.
     * <p>
     * Should only be called by access.
     * <p>
     * This call can be made on a ConglomerateController that was opened
     * for locking only.
     * <p>
     * RESOLVE (mikem) - move this call to ConglomerateManager so it is
     * obvious that non-access clients should not call this.
     *
	 * @param loc           The "RowLocation" which describes the row to unlock.
     * @param forUpdate     Row was locked for read or update.
     * @param row_qualified Row was qualified and returned to the user.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void unlockRowAfterRead(
    RowLocation     loc,
    boolean         forUpdate,
    boolean         row_qualified)
        throws StandardException;

	/**
	Return a row location object of the correct type to be
	used in calls to insertAndFetchLocation.
	@exception StandardException Standard exception policy.
	**/
	RowLocation newRowLocationTemplate()
		throws StandardException;

	/**
    Replace the (partial) row at the given location.  
	@return true if update was successful, returns false if the update 
	fails because the record pointed at no longer represents a valid record.
	@exception StandardException Standard exception policy.
	@see RowUtil
    **/
    boolean replace(
    RowLocation             loc, 
    DataValueDescriptor[]   row, 
    FormatableBitSet                 validColumns)
		throws StandardException;

    /**
    Get information about space used by the conglomerate.
    **/
    SpaceInfo getSpaceInfo()
        throws StandardException;

    /**
     * Dump debugging output to error log.
     * <p>
     * Dump information about the conglomerate to error log.
     * This is only for debugging purposes, does nothing in a delivered 
     * system, currently.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    void debugConglomerate()
		throws StandardException;
}
