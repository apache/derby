/*

   Derby - Class org.apache.derby.iapi.store.raw.Page

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

package org.apache.derby.iapi.store.raw;

import org.apache.derby.iapi.services.io.FormatableBitSet;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;

import org.apache.derby.iapi.store.access.Qualifier;

import org.apache.derby.iapi.types.DataValueDescriptor;


/**
	A Page contains an ordered set of records which are the stored form of rows.
	A record is a stream of bytes created from a row array. The record
	contains one or more fields, fields have a one to one correlation with
	the DataValueDescriptor's contained within a row array.
  <P>
	A Page represents <B>exclusive</B> access to a data page within a container.
	Exclusive access is released by calling the unlatch() method, once that 
    occurs the caller must no longer use the Page reference.
	<P>
	Several of the methods in Page take a RecordHandle as an argument. 
    RecordHandles are obtained from a Page, while holding exclusive access of 
    Page or a from a previous exclusive access of a Page representing the same 
    data page.
	All RecordHandle's used as arguments to methods (with the exception of 
    recordExists()) must be valid for the current state of the page. If they 
    are not valid then the method will throw an exception. A caller can ensure 
    that a record handle is valid by:
	<UL>
	<LI> Obtaining the handle during this exclusive access of this page
	<LI> Checking the record still exists with the method recordExists()
	<LI> Not using a handle after a delete().
	</UL>
	<P>
	Several of the methods in Page take a slot number as an argument.  A slot 
    always correspond to a record, which may be deleted or undeleted.

    <BR>
	MT - Latched - In general every method requires the page to be latched.

  <P>
  <B>Latching</B>
  <P>
  All page methods which are not valid for a latched page throw an
  exception if the page is not latched.  [@exception clauses on all
  the methods should be updated to reflect this].

  <P>
  <B>Aux Objects</B>
  <BR>
  The page cache will manage a client object along with the page as long
  as it remains in cache.  This object is called the "aux object".  The 
  aux object is associated with the page with setAuxObject(), and can be
  retreived later with getAuxObject().  The aux object will remain valid
  as long as the page is latched, but callers cannot assume that an aux
  object will ever stick around once the page is unlatched.  However, the
  page manager promises to call pageBeingEvicted() once before clearing
  the aux reference from the page.

	@see Object
	@see ContainerHandle
	@see RecordHandle
	@see AuxObject
*/

public interface Page  
{

    /**************************************************************************
     * Constants of the class
     **************************************************************************
     */

    /**
     * The slot number of the first slot.  This is guaranteed to be zero.
     **/
	public static final int FIRST_SLOT_NUMBER   = 0;
	
    /**
     * A slot number guaranteed to be invalid.
     **/
	public static final int INVALID_SLOT_NUMBER = -1;
	
    /**
     * Return the page number of this page. 
     * <p>
     * Page numbers are unique within a container and start at 
     * ContainerHandle.FIRST_PAGE_NUMBER and increment by 1 regardless of the 
     * page size.
     * <p>
     *
     * <BR> MT - Latched
     *
     * @see ContainerHandle
     *
	 * @return The page number of this page.
     **/
	public long getPageNumber();

    /**************************************************************************
     * Public Methods of This class: record handle interface.
     *     the following interfaces to page use the record Id or record handle
     *     (rather than the slot interface).
     **************************************************************************
     */

    /**
     * Return an invalid record handle.
     * <p>
     *
	 * @return an invalid record handle.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public RecordHandle getInvalidRecordHandle();

    /**
     * Return a record handle for the given constant record id.
     * <p>
     * Return a record handle that doesn't represent a record but rather has 
     * a special meaning.  Used for special cases like creating a key 
     * specific to the page, but not specific to a row on the page.
     * <p>
     * See RecordHandle interface for a list of "special record handles."
     *
     * @see RecordHandle
     *
	 * @return The created record handle.
     *
     * @param recordHandleConstant the special recordId
     *
	 * @exception StandardException if input is not a special record identifier.
     **/
	public RecordHandle makeRecordHandle(int recordHandleConstant) 
		 throws	StandardException;

    /**
     * Get a record handle from a previously stored record id.
     * <p>
     * Get a record handle from a previously stored record identifier that was
     * obtained from a RecordHandle.
     * <p>
     * <BR> MT - Latched
     *
	 * @return A valid record handle or null if the record no longer exists.
     *
     * @param recordId previously stored recordId.
     *
     * @see RecordHandle#getId
     **/
	RecordHandle getRecordHandle(int recordId);

    /**
     * does the record still exist on the page?
     * <p>
     * If "ignoreDelete" is true and the record handle represents a record on 
     * the page (either marked deleted or not) return true.  If "ignoreDelete" 
     * is false return true if the record handle represents a record on the 
     * page and the record is not marked as deleted.  Return false otherwise.
     *
     * <BR> MT - Latched
     *
	 * @return boolean indicating if the record still exists on the page.
     *
     * @param handle        handle of the record to look for.
     * @param ignoreDelete  if true, then routine will return true even if the
     *                      row is marked deleted.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	boolean recordExists(RecordHandle handle, boolean ignoreDelete) 
		 throws StandardException;

    /**
     * Fetch and lock a non-deleted record.
     * <p>
     * Lock and fetch a non-deleted record identified by a RecordHandle.  
     * Reads data from the page into row.
     * <P>
     * <B>Locking Policy</B>
     * <BR>
     * Calls the lockRecordForRead() method of the LockingPolicy object
     * passed to the openContainer() call before the record is accessed.
     * <BR>
     * The page latch may be released and re-latched within this method.
     * This will occur if the record lock has to be waited for.
     *
     * @param handle        Handle to record.
     * @param row           Row to be filled in with data from the record.
     * @param validColumns  a bit map of which columns in the row is to be 
     *                      fetched.  ValidColumns will not be changed by 
     *                      RawStore.
     * @param forUpdate     true if the intention is to update this record, 
     *                      false otherwise.
     *
     * @return A handle to the record, null if the record has been deleted.
     *
     * @exception StandardException	Standard Cloudscape error policy, 
     *                              a statemente level exception is thrown if
     *                              the record handle does not match a record 
     *                              on the page.
     *
     * @see Page#delete
     * @see LockingPolicy
     **/
	RecordHandle fetch(
    RecordHandle            handle, 
    Object[]   row, 
    FormatableBitSet                 validColumns, 
    boolean                 forUpdate)
		throws StandardException;

    /**
     * Is it likely that an insert will fit on this page?
     * <p>
     * Return true if there is a good chance an insert will fit on this page, 
     * false otherwise.  If this returns true then an insert may still fail by 
     * throwing an exception or by returning null, see insertAtSlot for details.
     * It is very probable that this call is much faster than the version that 
     * takes a row. In situations where it is expected that the 
     * majority of times a row will fit on a page this method should be used 
     * and the null return handled from insert/insertAtSlot.
     *
     * <BR>
     * MT - latched
     *
	 * @return true if it is likely an insert will fit on the page.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	boolean spaceForInsert() 
        throws StandardException;

    /**
     * will insert of this row fit on this page?
     * <p>
     * Return true if this record is guaranteed to be inserted successfully 
     * using insert() or insertAtSlot(). This guarantee is only valid if the 
     * following conditions are fulfilled before an insert is called with t
     * his row.
     * <UL>
     * <LI> The page is not unlatched
     * <LI> The page is not modified in any way, ie. no updates or other inserts
     * <LI> The row is not modified in such a way that would change its 
     *      storage size
     * </UL>
     *
     * <BR>
     * MT - latched
     *
	 * @return true if insert of this row will fit on this page.
     *
     * @param row                   The row to check for insert.
     * @param validColumns          bit map to interpret valid columns in row.
     * @param overflowThreshold     The percentage of the page to use for the
     *                              insert.  100 means use 100% of the page,
     *                              50 means use 50% of page (ie. make sure
     *                              2 rows fit per page).
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	boolean spaceForInsert(
    Object[]   row, 
    FormatableBitSet                 validColumns, 
    int                     overflowThreshold) 
        throws StandardException;

    /**
     * Insert a record anywhere on the page.
     * <P>
     *
     * <B>Locking Policy</B>
     * <BR>
     * Calls the lockRecordForWrite() method of the LockingPolicy object
     * passed to the openContainer() call before the record is inserted.
     * <BR>
     * MT - latched
     *
     * @param row           The row version of the data
     * @param validColumns  a bit map of which columns in the row is valid.  
     *                      ValidColumns will not be changed by RawStore.
     * @param insertFlag    see values for insertFlag below.
     *
     * @return A RecordHandle representing the new record.
     *
     * @exception StandardException	Standard Cloudscape error policy
     * @exception StandardException The container was not opened in update mode.
     * @exception StandardException Row cannot fit on the page or row is null.
     **/
	RecordHandle insert(
    Object[]   row, 
    FormatableBitSet                 validColumns,
    byte                    insertFlag, 
    int                     overflowThreshold)
		throws StandardException;

	/**
		Update the complete record identified by the record handle.

	*/
    /**
     * Update the record identified by the record handle.
     * <p>
     * Update the record, the new column values are found in row[] and if
     * validColumns is not-null, only use the columns indicated as valid in
     * the bit set.
     * <p>
     * <BR>
     * The page latch may be released and re-latched within this method.
     * This will occur if the record lock has to be waited for.
     *
     * @param handle        the record handle
     * @param row           The row version of the data
     * @param validColumns  A bit map of which columns in the row is valid.  
     *                      ValidColumns will not be changed by RawStore.
     *
     * @return true if the record is updated.  
     *         False if it is not because the record is already deleted.
     *
     * @exception StandardException	Standard Cloudscape error policy
     * @exception StandardException The container was not opened in update mode.
     * @exception StandardException If the record handle does not match 
     *                              a record on the page.
     *
     * @see Page#updateAtSlot
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	boolean update(
    RecordHandle            handle, 
    Object[]   row, 
    FormatableBitSet                 validColumns)
		throws StandardException;

    /**
     * Mark the record identified by position as deleted.
     * <p>
     * Mark the record identified by position as deleted. The record may be 
     * undeleted sometime later using undelete() by any transaction that sees 
     * the record.
     * <p>
     * <B>Locking Policy</B>
     * <P>
     * Calls the lockRecordForWrite() method of the LockingPolicy object
     * passed to the openContainer() call before the record is deleted.
     *
     * <BR>
     * The page latch may be released and re-latched within this method.
     * This will occur if the record lock has to be waited for.
     *
     * @param handle    record Handle to record
     * @param undo      if logical undo may be necessary, a function pointer to
     *                  the access code where the logical undo logic resides.
     *                  Null if logical undo is not necessary.
     *
     * @return true if the record was updated.  
     *         False if it wasn't because it is already deleted.
     *
     * @exception StandardException	Standard Cloudscape error policy
     * @exception StandardException The container was not opened in update mode.
     * @exception StandardException If the record handle does not match 
     *                              a record on the page.
     *
     * @see Page#deleteAtSlot
     * @see LockingPolicy
     **/
	public boolean delete(
    RecordHandle    handle, 
    LogicalUndo     undo)
		throws StandardException;

    /**
     * Fetch the number of fields in a record. 
     * <p>
     * <B>Locking Policy</B>
     * <P>
     * No locks are obtained.
     *
     * <BR>
     * MT - latched
     *
     * @param record Handle to deleted or non-deleted record
     *
     * @return the number of fields in the record
     *
     * @exception StandardException	Standard Cloudscape error policy, a 
     *                              statement level exception is thrown if the 
     *                              record handle does not match a record on 
     *                              the page.
     **/
	public int fetchNumFields(RecordHandle handle)
		 throws StandardException;

    /**************************************************************************
     * Public Methods of This class: slot interface.
     *     the following interfaces to page use the slot number 
     *     (rather than the record handle interface).
     **************************************************************************
     */


    /**
     * Get the slot number.
     * <p>
     * Get the slot number of a record on a latched page using its record 
     * handle.
     *
     * <P><B>Note</B>
     * The slot number is only good for as long as the page is latched.
     *
     * <BR>
     * MT - latched
     *
     * @param handle the record handle
     *
     * @return the slot number
     *
     * @exception StandardException	Standard Cloudscape error policy
     **/
    int getSlotNumber(RecordHandle handle) 
        throws StandardException;

    /**
     * Get the record handle of row at slot.
     * <p>
     * Get the record handle of a record on a latched page using its slot 
     * number.
     *
     * <BR>
     * MT - latched
     *
     * @param slot the slot number
     *
     * @return the record handle.
     *
     * @exception StandardException	Standard Cloudscape error policy
     **/
	RecordHandle getRecordHandleAtSlot(int slot) 
        throws StandardException;

    /**
     * Find slot for record with an id greater than the passed in identifier.
     * <p>
     * Find the slot for the first record on the page with an id greater than 
     * the passed in identifier.
     *
     * <BR>
     * Returns the slot of the first record on the page with an id greater than
     * the one passed in.  Usefulness of this functionality depends on the 
     * client's use of the raw store interfaces.  If all "new" records are 
     * always inserted at the end of the page, and the raw store continues to
     * guarantee that all record id's will be allocated in increasing order on 
     * a given page (assuming a PAGE_REUSABLE_RECORD_ID container), then a page
     * is always sorted in record id order.  For instance current heap tables 
     * function this way.  If the client ever inserts at a particular slot 
     * number, rather than at the "end" then the record id's will not be sorted.
     * <BR>
     * In the case where all record id's are always sorted on a page, then this
     * routine can be used by scan's which "lose" their position because the 
     * row they have as a position was purged.  They can reposition their scan 
     * at the "next" row after the row that is now missing from the table.
     * <BR>
     * This method returns the record regardless of its deleted status.
     * <BR>
     * MT - latched
     * 
     * @param handle record handle to find the next higher id.
     *
     * @return  record id of the first record on the page with a record id 
     *          higher than the one passed in.  If no such record exists, 
     *          -1 is returned.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	int getNextSlotNumber(RecordHandle handle) 
        throws StandardException;

	/**
		Insert a record at the specified slot. 
		<P>
	 */
    /**
     * Insert a record at the specified slot. 
     * <p>
     * All records that occupy FIRST_SLOT_NUMBER to (slot - 1) are not moved. 
     * <BR>
     * All records that occupy slot to (recordCount() - 1) are moved up one 
     * slot. 
     * <BR>
     * The new record is inserted at the specified slot. <BR>
     * If slot == FIRST_SLOT_NUMBER, then the new record will be inserted at 
     * the first slot. <BR>
     * If slot == recordCount(), then the record is inserted in a new slot, no
     * records are moved. <BR>
     *
     * If slot is > recordCount() or if slot < FIRST_SLOT_NUMBER, an exception
     * will be thrown.
     *
     * <P><B>Space Policy</B><BR>
     * If the row will not fit on a page then:
     * <UL>
     * <LI> an exception is thrown if the page has no other rows, this is an 
     *      indication that the row could never fit on a page in this container.
     * <LI> null is returned if there are other rows on the page, this is an 
     *      indication that the row can potentially be inserted successfully 
     *      onto an empty page.
     * </UL>
     *
     * <P>
     * <B>Locking Policy</B>
     * <BR>
     * Calls the lockRecordForWrite() method of the LockingPolicy object passed
     * to the openContainer() call before the record is inserted.
     * <BR>
     * MT - latched
     *
     * @param slot          The specified slot
     * @param row           The row version of the data
     * @param undo          if logical undo may be necessary, a function pointer
     *                      to the access code where the logical undo logic 
     *                      resides. Null if logical undo is not necessary.
     * @param validColumns  a bit map of which columns in the row is valid.  
     *                      ValidColumns will not be changed by RawStore.
     * @param insertFlag    if INSERT_UNDO_WITH_PURGE set, then the undo of this
     *                      insert will purge the row rather than mark it as 
     *                      deleted, which is the default bahavior for 
     *                      insertAtSlot and insert.
     *
     * @return A RecordHandle representing the new record, or null if the row 
     *         will not fit on a non-empty page.
     *
     * @exception StandardException	Standard Cloudscape error policy
     * @exception StandardException The container was not opened in update mode.
     * @exception StandardException The row cannot fit on the page
     *
     * @see LogicalUndo
     * @see LogicalUndoable
     **/
	RecordHandle insertAtSlot(
    int                     slot, 
    Object[]   row, 
    FormatableBitSet                 validColumns, 
    LogicalUndo             undo,
    byte                    insertFlag, 
    int                     overflowThreshold)
		throws StandardException;

	/**
		Values for insertFlag:
		
	*/
    /**
     * Values for insertFlag:
     * <p>
     *
     * INSERT_INITIAL			- flag initializer
     *
     * INSERT_DEFAULT			- default insert behavior, if the record does
     *                            not fit on the page where the insert 
     *                            operation is called, an error will be 
     *                            returned, instead of overflowing the record.
     *
     * INSERT_UNDO_WITH_PURGE	- if this is set, then the undo of this insert 
     *                            will purge the row rather than mark it as 
     *                            deleted, which is the default behaviro for 
     *                            insertAtSlot and insert.
     *
     * INSERT_CONDITIONAL		- if this flag is set, then, the overflow is 
     *                            conditional.  The record will be overflowed 
     *                            only if it exceeds the threshold specified 
     *                            by the properties, or the parameter.
     *
     * INSERT_OVERFLOW			- if this flag is set, then the insert 
     *                            operation will overflow the record if it does
     *                            not fit on the page.  
     *
     * INSERT_FOR_SPLIT		    - a record is being updated that causes new 
     *                            portions to be inserted *and* the last new 
     *                            portion needs to point to an existing portion.
     *
     * Rules for the insert flags:
     * 1. If INSERT_DEFAULT is set, INSERT_CONDITIONAL and INSERT_OVERFLOW 
     *    will be ignored
     * 2. INSERT_UNDO_WITH_PURGE can be set with any of the other 3 flags.
     * 3. If INSERT_OVERFLOW is not set, INSERT_CONDITIONAL will be ignored.  
     *    But, it is not necessary to set INSERT_CONDITIONAL when setting 
     *    INSERT_OVERFLOW.
     * 4. If INSERT_DEFAULT, INSERT_OVERFLOW both are not set, then, default 
     *    insert action will be taken, i.e. no overflow will be allowed.
     **/
	static final byte INSERT_INITIAL =		   (byte) 0x00;	// init the flag
	static final byte INSERT_DEFAULT =		   (byte) 0x01;	// default flag
	static final byte INSERT_UNDO_WITH_PURGE = (byte) 0x02;	// purge row on undo
	static final byte INSERT_CONDITIONAL =     (byte) 0x04;	// conditional 
                                                            // insert
	static final byte INSERT_OVERFLOW =		   (byte) 0x08;	// insert with 
                                                            // possible overflow
	static final byte INSERT_FOR_SPLIT =	   (byte) 0x10;	// rawstore only


    /**
     * Fetch a record located in the passed in slot
     * <p>
     * Fetch a record located in the passed in slot and fill-in the passed in 
     * StorebleRow and the Object columns contained within. If row
     * is null then the record is locked but is not fetched.
     * <BR>
     * This interface allows the caller to either return a deleted row or not. 
     * If "ignoreDelete" is set to true, fetch the record regardless of whether 
     * it is deleted or not (same as above fetchFromSlot).  However, if  
     * "ignoreDelete" is set to false and the and the slot correspond to a 
     * deleted row, null is returned.
     * <BR>
     * If a non-null Qualifier list is provided then the qualifier array will 
     * be applied to the row and the row will only be returned if the row 
     * qualifies, otherwise null will be returned.  Values in the columns of 
     * row may or may not be altered while trying to apply the qualifiers, if 
     * null is returned the state of the columns is undefined.  If a null 
     * Qualifier list is provided then no qualification is applied.
     * <BR>
     * If a non-null record handle is passed in, it is assumed that the record 
     * handle corresponds to the record in the slot.  If record handle is null,
     * a record handle will be manufactured and returned if the record is not 
     * deleted or if "ignoreDelete" is true.  This parameter is here for the 
     * case where the caller have already manufactured the record handle for 
     * locking or other purposes so it would make sense for the page to avoid 
     * creating a new record handle object if possible.
     *
     *
     * @param rh           the record handle of the row.  If non-null it must 
     *                     refer to the same record as the slot.  
     * @param slot         the slot number
     * @param row          Row to be filled in with information from record.
     * @param fetchDesc    A structure to efficiently carry a set of parameters
     *                     needed to describe the fetch, these include:
     *                     
     *                     validColumns - A bit map of which columns in the 
     *                     row to be fetched.  ValidColumns will not be 
     *                     changed by RawStore.
     *
     *                     qualifier_list - 
     *                     A list of Qualifiers to apply to the row to see if
     *                     the row should be returned.
     *
     *                     An array of qualifiers which restrict whether or not
     *                     the row should be returned by the fetch.  Rows for 
     *                     which any one of the qualifiers returns false are 
     *                     not returned by the fetch. If null, no qualification
     *                     is done and the requested columns of the rows are 
     *                     returned.  Qualifiers can only reference columns 
     *                     which are included in the scanColumnList.  The 
     *                     column id that a qualifier returns is the column id
     *                     the table, not the column id in the partial row 
     *                     being returned.  
     *                     qualifier_scratch_space - 
     *                     An array of int's that matches the size of the 
     *                     row[] array.  Used to process qualifiers, if no
     *                     qualifiers are input then array need not be 
     *                     input.  Passed in rather than allocated so that
     *                     space can be allocated a single time in a scan.
     *                     If not passed in then raw store will allocate and
     *                     deallocate per call.
     *
     * @param ignoreDelete if true, return row regardless of whether it is 
     *                     deleted or not.  If false, only return non-deleted 
     *                     row.
     *
     * @return A handle to the record.
     *
     * @exception StandardException	Standard Cloudscape error policy
     *
     * @see LockingPolicy
     **/
	public RecordHandle fetchFromSlot(
    RecordHandle            rh, 
    int                     slot, 
    Object[]                row,
    FetchDescriptor         fetchDesc,
    boolean                 ignoreDelete)
        throws StandardException;


	/**
		Fetch a single field from a deleted or non-deleted record.
		Fills in the passed in Object column with the field
		identified by fieldid if column is not null, otherwise the record
		is locked but not fetched.
		<BR>
		The fieldId of the first field is 0.
		If the fieldId is >= the number of fields on the record, 
		column is restored to null
		<P>
		<B>Locking Policy</B>
		<BR>
			No locks are obtained. It is up to the caller to obtain the correct locks.
		<BR>

		It is guaranteed that the page latch is not released by this method

		@param slot is the slot number
		@param fieldId is the column id
		@param column is to be filled in with information from the record.
		@param forUpdate true if the intention is to update this record, false otherwise.

		@return the Handle to the record that is locked

		@exception StandardException	Standard Cloudscape error policy, a 
                                        statement level exception is thrown if
                                        the slot is not on the page.

		@see Page#fetchFromSlot
		@see LockingPolicy
	 */
	public RecordHandle fetchFieldFromSlot(
    int                 slot, 
    int                 fieldId, 
    Object column)
		throws StandardException;

    /**
     * Test if a record is deleted.
     * <p>
     *
     * <P>
     * <B>Locking Policy</B>
     * <BR>
     * No locks are obtained.
     *
     * <BR>
     * It is guaranteed that the page latch is not released by this method
     *
     * @param slot slot of record to be tested.
     *
     * @exception StandardException	Standard Cloudscape error policy, a 
     *                              statement level exception is thrown if the 
     *                              slot is not on the page.
     **/
	public boolean isDeletedAtSlot(int slot)
		 throws StandardException;

	/**		
		Update a field within the record, replacing its current value with
		the stored representation of newValue. Record is identified by slot.
		If the field does not exist then it is added to the record, but only if
		(fieldId - 1) exists.

		<BR><B>RESOLVE</B> right now it throws an exception if fieldId is not 
		already on the record, not add the next one as advertised.

		<P>
		<B>Locking Policy</B>
		<P>
		Calls the lockRecordForWrite() method of the LockingPolicy object
		passed to the openContainer() call before the record is updated.

		<BR>
		It is guaranteed that the page latch is not released by this method
		

		@param slot is the slot number
		@param fieldId is the column id
		@param newValue has the new colum value to be stored in the record
		@param undo if logical undo may be necessary, a function pointer to the
		access code where the logical undo logic resides. Null if logical undo
		is not necessary.

		@return a Handle to the updated record.

		@exception StandardException	Standard Cloudscape error policy, a
                                        statement level exception is thrown if
		                                the slot is not on the page, or if the 
                                        record is deleted, or if the fieldId 
                                        is not on the record and (fieldId - 1)
                                        does not exist.

		@exception StandardException 
		The container was not opened in update mode.

		@see LockingPolicy
		@see LogicalUndo
		@see LogicalUndoable

	*/
	public RecordHandle updateFieldAtSlot(
    int                 slot, 
    int                 fieldId, 
    Object newValue, 
    LogicalUndo         undo)
		throws StandardException;


    /**
     * Fetch the number of fields in a record.
     * <p>
     *
     * <P>
     * <B>Locking Policy</B>
     * <P>
     * No locks are obtained.
     *
     * <BR>
     * It is guaranteed that the page latch is not released by this method
     *
     * @param slot is the slot number
     *
     * @return the number of fields in the record
     *
     * @exception StandardException	Standard Cloudscape error policy
     **/
	public int fetchNumFieldsAtSlot(int slot)
		 throws StandardException;

	/**
		Mark the record identified by slot as deleted or undeleted according to the
		delete flag.


	*/
    /**
     * Mark the record at slot as deleted or undeleted according to delete flag.
     * <p>
     *
     * <P>
     * <B>Locking Policy</B>
     * <P>
     * Calls the lockRecordForWrite() method of the LockingPolicy object passed
     * to the openContainer() call before the record is deleted.  If record 
     * already deleted, and an attempt is made to delete it, an exception is 
     * thrown.  If record not deleted, and an attempt is made to undelete it, 
     * an exception is thrown.
     *
     * <BR>
     * MT - latched
     *
     * @return a Handle to the deleted/undeleted record.
     *
     * @param slot      is the slot number
     * @param delete    true if this record is to be deleted false if this 
     *                  deleted record is to be marked undeleted
     * @param undo      if logical undo may be necessary, a function pointer to
     *                  the access code where the logical undo logic resides.
     *                  Null if logical undo is not necessary.
     * 
     * @exception StandardException	Standard Cloudscape error policy
     * @exception StandardException The container was not opened in update mode.
     * @exception StandardException A statement level exception is thrown when 
     *                              trying to delete an already deleted record,
     *                              or undelete a not deleted record.
     *
     * @exception StandardException A statement level exception is thrown if 
     *                              the slot is not on the page.
     *
     * @see LockingPolicy
     * @see Page#delete
     * @see LogicalUndo
     * @see LogicalUndoable
     *
     **/
	public RecordHandle deleteAtSlot(
    int         slot, 
    boolean     delete, 
    LogicalUndo undo)
		 throws StandardException;


    /**
     * Purge the row(s) from page.
     * <p>
     * Purge the row(s) from page, get rid of the row(s) and slot(s) - 
     * <B>USE WITH CAUTION</B>, 
     * please see entire description of this operation before attempting to 
     * use this.
     *
     * Starting from the specified slot, n rows will be purged. That is, rows 
     * that occupies from slot to slot+n-1 will be purged from the page.
     *
     * <P>
     * <B>Locking Policy</B>
     * <P>
     * Calls the lockRecordForWrite() method of the LockingPolicy object passed
     * to the openContainer() call before the records are purged.
     * <P>
     *
     * <B>NOTE : CAVEAT</B><BR>
     * This operation will physically get rid of the row from the page, so if a
     * subsequent operation on this page uses a slot that has been purged, then
     * the undo of this operation will fail.  It is only safe to use this 
     * operation if the caller knows that it has exclusive access to the page 
     * for the duration of the transaction, i.e, effectively holding a page 
     * lock on the page
     * <P>
     * <B>NOTE</B><BR>
     * Outstanding handles to purged rows are no longer valid, accessing them 
     * will cause an exception to be thrown.
     *
     * <BR>
	 *<B>NOTE : Data Logging for Purges</B><BR>
	 * @param needDataLogged is used to specify whether data is required to be
	 * logged for purge operatios. Data Logging is required 
	 * Only if the row can be reused or required for key search if a purge is
	 * rolled back;(rollback can occur if the system crashes in the middle of
	 * purges or some unexpected error condiditions  rolled back.
	 * For example: 
	 * 1)Btree expects the data to be there if a purge is rolled back;needDataLogged=true
	 * 2)Heaps does not care if data exist because only operation that can occur
	 * on a row whose purge rolled back is purging again.(needDataLogged=false)
	 * 
     * MT - latched
     *
     *
     * @param slot	    the starting slot number
     * @param numpurges	number of slots to purge. 
     *                  If <= 0, just returns as a no-op.
	 * @param needDataLogged  if set to true data is logged for purges else only headers.
	 *
     * @exception StandardException	Standard Cloudscape error policy
     * @see LockingPolicy
     **/
	public void purgeAtSlot(
    int slot, 
    int n,
	boolean needDataLogged) throws StandardException;


    /**
     * move rows from one page to another, purging in the process.
     * <p>
     *
     * Move from this page slot[src_slot] to slot[src_slot+num_rows-1] to 
     * destPage slot[dest_slot] to slot[dest_slot + num_rows - 1], in that 
     * order. Both this page and destPage must be latched and from the same 
     * container with the same page and record format.
     *
     * <BR>Slot[src_slot] to slot[src_slot+numrows-1] will be purged from this 
     * page.  RecordId on the dest page will be brand new and not in any 
     * particular order or range.  RecordId of the purged rows in this page is 
     * never reused.  Deleted and undeleted rows are copied over just the same.
     *
     * Exception will be thrown if this page does not have all the rows in the 
     * moved over range.  
     *
     * <BR><B>RESOLVE: reserve space now not copied over because in btree, a
     * row never shrinks.  When this routine is called by heap or by some page
     * which will have shrunken row, then we need to add that </B>
     *
     * <BR>DestPage must have at least dest_slot row occupying slot[0] to
     * slot[dest_slot-1].  DestPage must have enough space to take the copied
     * over data.  Rows that occupied slot number > dest_slot will be moved up
     * the slot (I.e., slot[dest_slot] -> slot[dest_slot + num_rows]).  
     *
     * <BR>If this operation rolls back, this page (the src page) will get the
     * rows back and the dest page will purge the rows that were copied - this
     * is as if the rows were inserted into the dest page with 
     * INSERT_UNDO_WITH_PURGE.
     *
     * <P>
     * <B>Locking Policy</B>
     * <P>
     * Calls the lockRecordForWrite() method of the LockingPolicy object
     * passed to the openContainer() call before the rows are copied over and 
     * bore the records are purged.  I.e, for num_rows moved, there will be
     * 2*num_rows calls to lockRecordForWrite.
     * <P>
     *
     * <P><B>Use with caution</B>
     * <BR>As with a normal purge, no space is reserved on this page for 
     * rollback of the purge, so you must commit before inserting any rows 
     * onto this page - unless those inserts are INSERT_UNDO_WITH_PURGE.
     *
     * @param destPage the page to copy to
     * @param src_slot start copying from this slot
     * @param num_rows copy and purge this many rows from this page
     * @param dest_slot copying into this slot of destPage
     *
     * @exception StandardException Standard Cloudscape error policy
     **/
	public void copyAndPurge(
    Page    destPage, 
    int     src_slot, 
    int     num_rows, 
    int     dest_slot)
		 throws StandardException;

	/**
		Update the complete record identified by the slot.

		<P>
		<B>Locking Policy</B>
		<P>
		Calls the lockRecordForWrite() method of the LockingPolicy object
		passed to the openContainer() call before the record is undeleted.
		If record already deleted, an exception is thrown.

		<BR>
		It is guaranteed that the page latch is not released by this method

		@return a Handle to the updated record.
		@param slot is the slot number
		@param validColumns a bit map of which columns in the row is valid.
		ValidColumns will not be changed by RawStore.

		@exception StandardException	Standard Cloudscape error policy
		@exception StandardException The container was not opened in update mode.
		@exception StandardException if the slot is not on the page.

		@see Page#update
	*/
	RecordHandle updateAtSlot(
    int                     slot, 
    Object[]   row, 
    FormatableBitSet                 validColumns)
		throws StandardException;

	/*
		Page operations
	*/

	/**
		Unlatch me, the page is exclusivly latched by its current user until
		this method call is made.
		<BR>
		After using this method the caller must throw away the
		reference to the Page object, e.g.
		<PRE>
			ref.unlatch();
			ref = null;
		</PRE>
		<BR>
		The page will be released automatically at the close of the
		container if this method is not called explictly.

		<BR>
		MT - latched

	*/
	public void unlatch();



	/**
		Return the number of records on the page. The returned count includes rows that are deleted,
		i.e. it is the same as the number of slots on the page.

		<BR>
		MT - latched

		@exception StandardException	Standard Cloudscape error policy
	*/

	public int recordCount() throws StandardException;

	/**
		Return the number of records on this page that are <B> not </B> marked as deleted.
		
		 <BR>
		MT - latched

		@exception StandardException	Standard Cloudscape error policy
	*/

	public int nonDeletedRecordCount() throws StandardException;

	/**
	  Set the aux object for this page.
	  To clear the auxObject in the page, pass in a null AuxObject.
	  If the AuxObject has already been set, this method will
	  call auxObjectInvalidated() on the old aux objkect and replace it with aux.

		<BR>
		MT - latched

	  @see AuxObject
	**/
	public void setAuxObject(AuxObject aux);

	/**
	  Retrieve this page's aux object, returning null if there isn't one. The reference returned
	  must only be used while the page is latched, once unlatch is called the reference to the
	  aux object must be discarded.

		<BR> MT - latched

	  @see AuxObject
	**/
	public AuxObject getAuxObject();

	/**
		Returns true if the page is latched. Only intended to be used as a Sanity check. Callers must
		discard Page references once unlatch is called.

		<BR>
		MT - latched
	*/


	/*
	 * time stamp - for those implmentation that supports it
	 */

	/**
		Set the time stamp to what is on page at this instance.  No op if this
		page does not support time stamp.

		@exception StandardException Standard Cloudscape error policy.
	*/
	void setTimeStamp(PageTimeStamp ts) throws StandardException;


	/**
		Return a time stamp that can be used to identify the page of this
		specific instance.  For pages that don't support timestamp, returns
		null.
	*/
	PageTimeStamp currentTimeStamp();

	/**
		See if timeStamp for this page is the same as the current
		instance of the page.  Null timeStamp never equals the instance of the
		page.

		@param ts the time stamp gotten from an earlier call to this page's
		getTimeStamp
		@return true if timestamp is the same
		@exception StandardException Standard Cloudscape error policy.

		@see PageTimeStamp
	*/
	boolean equalTimeStamp(PageTimeStamp ts) throws StandardException;

	public boolean isLatched();

    public static final String DIAG_PAGE_SIZE        = "pageSize";
    public static final String DIAG_RESERVED_SPACE   = "reserveSpace";
    public static final String DIAG_MINIMUM_REC_SIZE = "minRecSize";
    public static final String DIAG_BYTES_FREE       = "bytesFree";
    public static final String DIAG_BYTES_RESERVED   = "bytesReserved";
    public static final String DIAG_NUMOVERFLOWED    = "numOverFlowed";
    public static final String DIAG_ROWSIZE          = "rowSize";
    public static final String DIAG_MINROWSIZE       = "minRowSize";
    public static final String DIAG_MAXROWSIZE       = "maxRowSize";
    public static final String DIAG_PAGEOVERHEAD     = "pageOverhead";
    public static final String DIAG_SLOTTABLE_SIZE   = "slotTableSize";
}
