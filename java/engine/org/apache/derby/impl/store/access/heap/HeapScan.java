/*

   Derby - Class org.apache.derby.impl.store.access.heap.HeapScan

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

package org.apache.derby.impl.store.access.heap;


/**

  A heap scan object represents an instance of an scan on a heap conglomerate.

**/

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.Storable;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;
import org.apache.derby.iapi.store.access.conglomerate.ScanManager;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;

import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowUtil;
import org.apache.derby.iapi.store.access.ScanInfo;
import org.apache.derby.iapi.store.access.ScanController;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.impl.store.access.conglomerate.ConglomerateUtil;
import org.apache.derby.impl.store.access.conglomerate.GenericScanController;
import org.apache.derby.impl.store.access.conglomerate.RowPosition;

import org.apache.derby.iapi.store.access.BackingStoreHashtable;
import org.apache.derby.iapi.services.io.FormatableBitSet;

import java.util.Hashtable;
import java.util.Vector;

class HeapScan 
    extends GenericScanController implements ScanManager
{

    /**************************************************************************
     * Constants of HeapScan
     **************************************************************************
     */

    /**************************************************************************
     * Fields of HeapScan
     **************************************************************************
     */

    /**
     * A 1 element array to turn fetchNext and fetch calls into 
     * fetchNextGroup calls.
     **/
    private DataValueDescriptor[][] fetchNext_one_slot_array = 
        new DataValueDescriptor[1][];


    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */

	/**
	 ** The only constructor for a heap scan returns a scan in the
	 ** closed state, the caller must call open.
	 **/
	
	public HeapScan()
	{
	}

    /**************************************************************************
     * Protected concrete impl of abstract methods of 
     *     GenericController class:
     **************************************************************************
     */
    protected void queueDeletePostCommitWork(
    RowPosition pos)
        throws StandardException
    {
        TransactionManager xact_mgr = open_conglom.getXactMgr();

        xact_mgr.addPostCommitWork(
            new HeapPostCommit(
                xact_mgr.getAccessManager(), 
                (Heap) open_conglom.getConglomerate(),
                pos.current_page.getPageNumber()));
    }

    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */
    protected void setRowLocationArray(
    RowLocation[]   rowloc_array,
    int             index,
    RowPosition     pos)
        throws StandardException
    {
        if (rowloc_array[index] == null)
        {
            rowloc_array[index] = new HeapRowLocation(pos.current_rh);
        }
        else
        {
            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(
                    rowloc_array[index] instanceof HeapRowLocation);
            }

            ((HeapRowLocation)rowloc_array[index]).setFrom(pos.current_rh);
        }
    }

    /**
    Fetch the row at the next position of the Scan.

    If there is a valid next position in the scan then
	the value in the template storable row is replaced
	with the value of the row at the current scan
	position.  The columns of the template row must
	be of the same type as the actual columns in the
	underlying conglomerate.

    The resulting contents of templateRow after a fetchNext() 
    which returns false is undefined.

    The result of calling fetchNext(row) is exactly logically
    equivalent to making a next() call followed by a fetch(row)
    call.  This interface allows implementations to optimize 
    the 2 calls if possible.

    @param fetch_row The template row into which the value
	of the next position in the scan is to be stored.

    @return True if there is a next position in the scan,
	false if there isn't.

	@exception StandardException Standard exception policy.
    **/
    public boolean fetchNext(DataValueDescriptor[] fetch_row)
		throws StandardException
	{
        // Turn this call into a group fetch of a 1 element group.
        if (fetch_row == null)
            fetchNext_one_slot_array[0] = RowUtil.EMPTY_ROW;
        else
            fetchNext_one_slot_array[0] = fetch_row;

        boolean ret_val = 
            fetchRows(
                fetchNext_one_slot_array, 
                (RowLocation[]) null,
                (BackingStoreHashtable) null,
                1,
                (int[]) null) == 1;

        return(ret_val);
    }


    /**
	@see ScanController#next
	**/
    public boolean next()
		throws StandardException
	{
        // if there is no row template from the caller, we need to
        // read the row into something, Use the scratch row.
        // We could optimize this, if there are no qualifiers and read
        // into a zero column row, but callers should be using fetchNext()
        // instead.
        fetchNext_one_slot_array[0] = open_conglom.getRuntimeMem().get_scratch_row();

        boolean ret_val = 
            fetchRows(
                fetchNext_one_slot_array, 
                (RowLocation[]) null,
                (BackingStoreHashtable) null,
                1,
                (int[]) null) == 1;

        return(ret_val);
	}


    /**************************************************************************
     * Public Methods of ScanController interface:
     **************************************************************************
     */

    /**
	@see ScanController#fetchLocation
	**/
	public void fetchLocation(RowLocation templateLocation)
		throws StandardException
	{
		if (open_conglom.getContainer() == null  || 
            scan_position.current_rh == null)
        {
            throw StandardException.newException(
                    SQLState.HEAP_SCAN_NOT_POSITIONED);
        }
		HeapRowLocation hrl = (HeapRowLocation) templateLocation;
		hrl.setFrom(scan_position.current_rh);
	}

    public int fetchNextGroup(
    DataValueDescriptor[][] row_array,
    RowLocation[]           rowloc_array)
        throws StandardException
	{
        return(
            fetchRows(
                row_array, 
                rowloc_array,
                (BackingStoreHashtable) null,
                row_array.length,
                (int[]) null));
    }


    /**
     * Return ScanInfo object which describes performance of scan.
     * <p>
     * Return ScanInfo object which contains information about the current
     * scan.
     * <p>
     *
     * @see ScanInfo
     *
	 * @return The ScanInfo object which contains info about current scan.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public ScanInfo getScanInfo()
		throws StandardException
    {
        return(new HeapScanInfo(this));
    }

    /**
    Reposition the current scan.  This call is semantically the same as if
    the current scan had been closed and a openScan() had been called instead.
    The scan is reopened against the same conglomerate, and the scan
    is reopened with the same "scan column list", "hold" and "forUpdate"
    parameters passed in the original openScan.  
    <p>
    The statistics gathered by the scan are not reset to 0 by a reopenScan(),
    rather they continue to accumulate.
    <p>
    Note that this operation is currently only supported on Heap conglomerates.
    Also note that order of rows within are heap are not guaranteed, so for
    instance positioning at a RowLocation in the "middle" of a heap, then
    inserting more data, then continuing the scan is not guaranteed to see
    the new rows - they may be put in the "beginning" of the heap.

	@param startRowLocation  An existing RowLocation within the conglomerate,
    at which to position the start of the scan.  The scan will begin at this
    location and continue forward until the end of the conglomerate.  
    Positioning at a non-existent RowLocation (ie. an invalid one or one that
    had been deleted), will result in an exception being thrown when the 
    first next operation is attempted.

	@param qualifier An array of qualifiers which, applied
	to each key, restrict the rows returned by the scan.  Rows
	for which any one of the qualifiers returns false are not
	returned by the scan. If null, all rows are returned.

	@exception StandardException Standard exception policy.
    **/
	public void reopenScanByRowLocation(
    RowLocation startRowLocation,
    Qualifier qualifier[][])
        throws StandardException
    {
        reopenScanByRecordHandle(
            ((HeapRowLocation) startRowLocation).getRecordHandle(
                 open_conglom.getContainer()),
            qualifier);
    }


	/*
	** Methods of ScanManager
	*/

    /**
     * Do work necessary to maintain the current position in the scan.
     * <p>
     * The latched page in the conglomerate "congomid" is changing, do
     * whatever is necessary to maintain the current position of the scan.
     * For some conglomerates this may be a no-op.
     * <p>
     *
     * @param conglom   Conglomerate being changed.
     * @param page      Page in the conglomerate being changed.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void savePosition(Conglomerate conglom, Page page)
        throws StandardException
	{
        // RESOLVE (mikem), under the current implementation all scans within
        // a transaction are called rather than just the ones with the right
        // conglom.  For now just have heaps ignore the call. 
        
		// throw HeapOperationException.unimplementedFeature();
        return;
	}
}
