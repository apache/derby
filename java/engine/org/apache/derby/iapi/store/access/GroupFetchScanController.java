/*

   Derby - Class org.apache.derby.iapi.store.access.GroupFetchScanController

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.services.io.Storable;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.services.io.FormatableBitSet;

import java.util.Hashtable;

/**
  This scan controller can only be used for group fetch, no update
  operations are supported, use ScanController if you need scan interfaces
  other than group fetch.
  <p>
  In general group fetch will be more efficient than using the 
  ScanController fetchNext() interface to get rows one at a time.  The
  performance comes from reducing the per call overhead of getting
  a row.  Also this interface can, depending on the requested isolation
  level, possibly do more efficient locking.
  <p>
  Group fetch scans are opened from a TransactionController.

  @see TransactionController#openScan
  @see RowCountable
  @see GenericScanController

**/

public interface GroupFetchScanController extends GenericScanController
{
    /**
     * Fetch the next N rows from the table.
     * <p>
     * The client allocates an array of N rows and passes it into the
     * fetchNextSet() call.  The client must at least allocate a row and
     * set row_array[0] to this row.  The client can optionally either leave
     * the rest of array entries null, or allocate rows to the slots.
     * If access finds an entry to be null, and wants to read a row into
     * it, it will allocate a row to the slot.  Once fetchNextGroup() returns
     * "ownership" of the row passes back to the client, access will not 
     * keep references to the allocated row.  Expected usage is that 
     * the client will specify an array of some number (say 10), and then 
     * only allocate a single row.  This way if only 1 row qualifies only
     * one row will have been allocated.
     * <p>
     * This routine does the equivalent of N 
     * fetchNext() calls, filling in each of the rows in the array.
     * Locking is performed exactly as if the N fetchNext() calls had
     * been made.
     * <p>
     * It is up to Access how many rows to return.  fetchNextGroup() will
     * return how many rows were filled in.  If fetchNextGroup() returns 0
     * then the scan is complete, (ie. the scan is in the same state as if
     * fetchNext() had returned false).  If the scan is not complete then
     * fetchNext() will return (1 <= row_count <= N).
     * <p>
     * The current position of the scan is undefined if fetchNextSet()
     * is used (ie. mixing fetch()/fetchNext() and fetchNextSet() calls
     * in a single scan does not work).  This is because a fetchNextSet()
     * request for 5 rows from a heap where the first 2 rows qualify, but
     * no other rows qualify will result in the scan being positioned at
     * the end of the table, while if 5 rows did qualify the scan will be
     * positioned on the 5th row.
     * <p>
     * If the row loc array is non-null then for each row fetched into
     * the row array, a corresponding fetchLocation() call will be made to
     * fill in the rowloc_array.  This array, like the row array can be 
     * initialized with only one non-null RowLocation and access will 
     * allocate the rest on demand.
     * <p>
     * Qualifiers, start and stop positioning of the openscan are applied
     * just as in a normal scan. 
     * <p>
     * The columns of the row will be the standard columns returned as
     * part of a scan, as described by the validColumns - see openScan for
     * description.
     * <p>
     * Expected usage:
     *
     * // allocate an array of 5 empty rows
     * DataValueDescriptor[][] row_array = allocate_row_array(5);
     * int row_cnt = 0;
     *
     * scan = openScan();
     *
     * while ((row_cnt = scan.fetchNextSet(row_array, null) != 0)
     * {
     *     // I got "row_cnt" rows from the scan.  These rows will be
     *     // found in row_array[0] through row_array[row_cnt - 1]
     * }
     *
     * <p>
	 * @return The number of qualifying rows found and copied into the 
     *         provided array of rows.  If 0 then the scan is complete, 
     *         otherwise the return value will be: 
     *         1 <= row_count <= row_array.length
     *
     * @param row_array         The array of rows to copy rows into.  
     *                          row_array[].length must >= 1.   The first entry
     *                          must be non-null destination rows, other entries
     *                          may be null and will be allocated by access
     *                          if needed.
     *
     * @param rowloc_array      If non-null, the array of row locations to 
     *                          copy into.  If null, no row locations are
     *                          retrieved.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public int fetchNextGroup(
    DataValueDescriptor[][] row_array,
    RowLocation[]           rowloc_array)
        throws StandardException;

    /**
    Move to the next position in the scan.  If this is the first
    call to next(), the position is set to the first row.
    Returns false if there is not a next row to move to.
    It is possible, but not guaranteed, that this method could return 
    true again, after returning false, if some other operation in the same 
    transaction appended a row to the underlying conglomerate.

    @return True if there is a next position in the scan,
	false if there isn't.

	@exception StandardException Standard exception policy.
    **/
    boolean next()
		throws StandardException;
}
