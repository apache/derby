/*

   Derby - Class org.apache.derby.iapi.store.access.ScanController

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

import org.apache.derby.iapi.services.io.Storable;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.services.io.FormatableBitSet;

/**

  A scan is the mechanism for iterating over the rows in a conglomerate,
  the scan controller is the interface through which access clients
  control the underlying scan.  An instance of a scan controller can 
  be thought of as an open scan.
  <p>
  Scans are opened from a TransactionController.
  <P>
  A ScanController can handle partial rows. Partial rows
  are described in RowUtil.
  <BR>
  A scan controller is opened with a FormatableBitSet that describes the
  columns that need to be returned on a fetch call. This FormatableBitSet
  need not include any columns referenced in the qualifers, start
  and/or stop keys.

  @see TransactionController#openScan
  @see GenericScanController
  @see RowCountable
  @see RowUtil

**/

public interface ScanController extends GenericScanController
{
    /**

    GE is used to position a scan at values greater than or or equal to the
    given key in the scan.  This positioning argument refers to the order 
    within the scan (not necessarily actual compare calls on the datatypes).  
    "greater" than is interpreted in terms of the 
    current conglomerate and scan.  For instance, a btree may be ordered
    ascending on an int, in that case a 2 is "greater" than 1 in a forward
    scan on that index, and 1 is "greater" than 2 in a backward scan.
    If the btree was ordered descending on an int then 1 is "greater" than
    2 in a forward scan on that index, and 2 is "greater" than 1 in a backward
    scan.

    @see TransactionController#openScan
    */

    /* The value of this must be the same value returned by the Orderable
     * interface when a key is > than another key.
     */
    public static final int GE = 1;

    /**
    GT is used to position a scan at values greater than the given key.
    This positioning argument refers to the order 
    within the scan (not necessarily actual compare calls on the datatypes).  
    "greater" than is interpreted in terms of the 
    current conglomerate and scan.  For instance, a btree may be ordered
    ascending on an int, in that case a 2 is "greater" than 1 in a forward
    scan on that index, and 1 is "greater" than 2 in a backward scan.
    If the btree was ordered descending on an int then 1 is "greater" than
    2 in a forward scan on that index, and 2 is "greater" than 1 in a backward
    scan.

    @see TransactionController#openScan
    */
    /* The value of this must be the same value returned by the Orderable
     * interface when a key is < than another key.
     */
    public static final int GT = -1;

    /**
    NA - argument is unused in call.  For some scans the key is set to null
    to indicate no start or stop position, in those cases the position 
    operator is ignored.

    @see TransactionController#openScan
    */
    /* The value of this must be the same value returned by the Orderable
     * interface when a key is < than another key.
     */
    public static final int NA = 0;

    /**
    Delete the row at the current position of the scan.

    @return true if the delete was successful,
	false if the current position is no longer valid (ie. if it was already
	deleted).

	@exception StandardException Standard exception policy.
    **/
    boolean delete()
		throws StandardException;

    /**
     * A call to allow client to indicate that current row does not qualify.
     * <p>
     * Indicates to the ScanController that the current row does not
     * qualify for the scan.  If the isolation level of the scan allows, 
     * this may result in the scan releasing the lock on this row.
     * <p>
     * Note that some scan implimentations may not support releasing locks on 
     * non-qualifying rows, or may delay releasing the lock until sometime
     * later in the scan (ie. it may be necessary to keep the lock until 
     * either the scan is repositioned on the next row or page).
     * <p>
     * This call should only be made while the scan is positioned on a current
     * valid row.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    void didNotQualify() throws StandardException;

    /**
    Returns true if the current position of the scan still qualifies
    under the set of qualifiers passed to the openScan().  When called
    this routine will reapply all qualifiers against the row currently
    positioned and return true if the row still qualifies.  If the row
    has been deleted or no longer passes the qualifiers then this routine
    will return false.
    
    This case can come about if the current scan
    or another scan on the same table in the same transaction 
    deleted the row or changed columns referenced by the qualifier after 
    the next() call which positioned the scan at this row.  

    Note that for comglomerates which don't support update, like btree's, 
    there is no need to recheck the qualifiers.

    The results of a fetch() performed on a scan positioned on 
    a deleted row are undefined, note that this can happen even if next()
    has returned true (for instance the client can delete the row, or if
    using read uncommitted another thread can delete the row after the
    next() call but before the fetch).

	@exception StandardException Standard exception policy.
    **/
    boolean doesCurrentPositionQualify()
		throws StandardException;

    /**
    Fetch the (partial) row at the current position of the Scan.
	The value in the destRow storable row is replaced
	with the value of the row at the current scan
	position.  The columns of the destRow row must
	be of the same type as the actual columns in the
	underlying conglomerate. The number of elements in
	fetch must be compatible with the number of scan columns
	requested at the openScan call time.
	<BR>
	A fetch can return a sub-set of the scan columns reqested
	at scan open time by supplying a destRow will less elements
	than the number of requested columns. In this case the N leftmost
	of the requested columns are fetched, where N = destRow.length.
	In the case where all columns are rested and N = 2 then columns 0 and 1
	are returned. In the case where the openScan FormatableBitSet requested columns
	1, 4 and 7, then columns 1 and 4 would be fetched when N = 2.
	<BR>

    The results of a fetch() performed on a scan after next() has returned
    false are undefined.

    A fetch() performed on a scan positioned on 
    a deleted row will throw a StandardException with 
    state = SQLState.AM_RECORD_NOT_FOUND.  Note that this can happen even if 
    next() has returned true (for instance the client can delete the row, or if
    using read uncommitted another thread can delete the row after the
    next() call but before the fetch).

    @param destRow The row into which the value of the current 
    position in the scan is to be stored.

	@exception StandardException Standard exception policy.
	@see RowUtil
    **/
    void fetch(DataValueDescriptor[] destRow)
		throws StandardException;

    /**
    Fetch the (partial) row at the next position of the Scan.

    If there is a valid next position in the scan then
	the value in the destRow storable row is replaced
	with the value of the row at the current scan
	position.  The columns of the destRow row must
	be of the same type as the actual columns in the
	underlying conglomerate.

    The resulting contents of destRow after a fetchNext() 
    which returns false is undefined.

    The result of calling fetchNext(row) is exactly logically
    equivalent to making a next() call followed by a fetch(row)
    call.  This interface allows implementations to optimize 
    the 2 calls if possible.

    @param destRow The destRow row into which the value
	of the next position in the scan is to be stored.

    @return True if there is a next position in the scan,
	false if there isn't.

	@exception StandardException Standard exception policy.
	@see ScanController#fetch
	@see RowUtil
    **/
    boolean fetchNext(DataValueDescriptor[] destRow)
		throws StandardException;

	/**
	Fetch the location of the current position in the scan.
	The destination location is replaced with the location
	corresponding to the current position in the scan.
	The destination location must be of the correct actual
	type to accept a location from the underlying conglomerate
	location.

    The results of a fetchLocation() performed on a scan after next() has 
    returned false are undefined.

    The results of a fetchLocation() performed on a scan positioned on 
    a deleted row are undefined, note that this can happen even if next()
    has returned true (for instance the client can delete the row, or if
    using read uncommitted another thread can delete the row after the
    next() call but before the fetchLocation).

	@exception StandardException Standard exception policy.
	**/
	void fetchLocation(RowLocation destRowLocation)
		throws StandardException;

    /**
    Returns true if the current position of the scan is at a 
    deleted row.  This case can come about if the current scan
    or another scan on the same table in the same transaction 
    deleted the row after the next() call which positioned the
    scan at this row.  

    The results of a fetch() performed on a scan positioned on 
    a deleted row are undefined.

	@exception StandardException Standard exception policy.
    **/
    boolean isCurrentPositionDeleted()
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

    /**
    Replace the (partial) row at the current position of the scan.

    @return true if the replace was successful,
	false if the current position is no longer valid (ie. if it was deleted).

	@exception StandardException Standard exception policy.
	@see RowUtil
    **/

    boolean replace(DataValueDescriptor[] row, FormatableBitSet validColumns)
		throws StandardException;
}
