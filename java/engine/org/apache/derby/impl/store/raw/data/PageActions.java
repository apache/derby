/*

   Derby - Class org.apache.derby.impl.store.raw.data.PageActions

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

package org.apache.derby.impl.store.raw.data;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.DynamicByteArrayOutputStream;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;

import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;

import org.apache.derby.iapi.types.DataValueDescriptor;


public interface PageActions 
{

    /**
     * Set the Delete status of the record at the given slot.
     * <p>
     * Subclass that implements this method has to call 
     * BasePage.setDeleteStatus to update the delete status on the in-memory 
     * slot table.
     * <p>
     * <BR> MT - latched, page is latched when this methods is called.
     * @param t				    The transaction
     * @param page				the target page
     * @param slot				the slot number of the record 
     * @param recordId			the recordID of the record 
     * @param delete			set the delete status to this value
     * @param undo				logical undo logic if necessary
     *
     * @exception StandardException	Standard Cloudscape error policy
     * @see org.apache.derby.iapi.store.raw.Page#deleteAtSlot
     **/
	public void actionDelete(
    RawTransaction  t, 
    BasePage        page, 
    int             slot, 
    int             recordId, 
    boolean         delete, 
    LogicalUndo     undo)
		throws StandardException;


    /**
     * Update record at the given slot with this row.
     * <p>
     * <BR> MT - latched, page is latched when this methods is called.
     *
     * @param RawTransaction	The transaction
     * @param page			the updated page
     * @param slot			the slot number of the record 
     * @param recordId		the recordID of the record 
     * @param row			The new storable row
     * @param validColumns	the columns that needs to be updated
     * @param realStartColumn	the first column that is updated
     * @param logBuffer		where to prepare the log record
     * @param realSpaceOnPage	??
     * @param headRowHandle	the record handle of the head row
     *
     * @return the next column to update in the row or,
     *         -1 if the update has been completed.
     *
     * @exception StandardException	Standard Cloudscape error policy
     *
     * @see org.apache.derby.iapi.store.raw.Page#updateAtSlot
     **/
	public int actionUpdate(
    RawTransaction          t, 
    BasePage                page, 
    int                     slot, 
    int                     recordId,
    Object[]   row, 
    FormatableBitSet                 validColumns,
    int                     realStartColumn, 
    DynamicByteArrayOutputStream  logBuffer, 
    int                     realSpaceOnPage, 
    RecordHandle            headRowHandle)
		throws StandardException;

    /**
     * Purge the record at the given slot.
     * <p>
     * Subclass that implements this method has to remove the slot from the 
     * base page in-memory slot table (removeAndShiftDown).
     * <p>
     * <BR> MT - latched, page is latched when this methods is called.
     *
     * @param t				The transaction
     * @param slot			the starting slot number of the record 
     * @param num_rows		how many rows to purge
     * @param recordIds		the recordIDs of the record (an array of num_rows)
     *
     * @exception StandardException	Standard Cloudscape error policy
     *
     * @see org.apache.derby.iapi.store.raw.Page#purgeAtSlot
     **/
	public void actionPurge(
    RawTransaction  t, 
    BasePage        page, 
    int             slot, 
    int             num_rows, 
    int[]           recordIds,
	boolean         logData)
		throws StandardException;

    /**
     * Update a field of the record at the given slot with this value.
     * <p>
     *
     * <BR> MT - latched, page is latched when this methods is called.
     *
     * @param RawTransaction	The transaction
     * @param slot			the slot number of the record 
     * @param recordId		the recordID of the record 
     * @param fieldId		the fieldId of the value
     * @param value			the new value for the field
     * @param undo          if logical undo may be necessary, a function 
     *                      pointer to the access code where the logical undo 
     *                      logic resides.   Null if logical undo is not 
     *                      necessary.
     *
     * @exception StandardException	Standard Cloudscape error policy
     *
     * @see org.apache.derby.iapi.store.raw.Page#updateFieldAtSlot
     *
     **/
	public void actionUpdateField(
    RawTransaction          t, 
    BasePage                page, 
    int                     slot, 
    int                     recordId, 
    int                     fieldId, 
    Object     newValue, 
    LogicalUndo             undo)
		throws StandardException;

    /**
     * Insert record at the given slot with this recordId. 
     * <p>
     *
     * <BR> MT - latched, page is latched when this methods is called.
     *
     * @param RawTransaction	The transaction
     * @param slot			    the slot number of the record 
     * @param recordId		    the recordID of the record 
     * @param row			    The storable row
     * @param undo              if logical undo may be necessary, a function 
     *                          pointer to the access code where the logical 
     *                          undo logic resides.   Null if logical undo is 
     *                          not necessary.
     * @param insertFlag		see Page value for insertFlag
     *
     * @exception StandardException	Standard Cloudscape error policy
     *
     * @see org.apache.derby.iapi.store.raw.Page#insertAtSlot
     **/
	public int actionInsert(
    RawTransaction          t, 
    BasePage                page, 
    int                     slot, 
    int                     recordId,
    Object[]                row, 
    FormatableBitSet                 validColumns,
    LogicalUndo             undo, 
    byte                    insertFlag, 
    int                     startColumn, 
    boolean                 isLongColumn,
    int                     realStartColumn, 
    DynamicByteArrayOutputStream  logBuffer, 
    int                     realSpaceOnPage, 
    int                     overflowThreshold)
		throws StandardException;

    /**
     * Copy num_rows from srcPage into deestpage
     * <p>
     * Longer descrption of routine.
     * <p>
     * @param RawTransaction	The transaction
     * @param destPage			the destination page
     * @param srcPage			the source page
     * @param destSlot			starting slot # of destination page to copy to
     * @param numRows			the number of rows to be copied
     * @param srcSlot			starting slot number of source page to copy from
     * @param recordIds		    an array of record ids to use in the 
     *                          destination page
     *
     * @exception StandardException Standard Cloudscape policy.
     **/
	public void actionCopyRows(
    RawTransaction  t, 
    BasePage        destPage, 
    BasePage        srcPage, 
    int             destSlot, 
    int             numRows, 
    int             srcSlot, 
    int[]           recordIds)
		throws StandardException;

    /**
     * Invalidate the page due to deallocation.
     * Short one line description of routine.
     * <p>
     * Invalidate the page due to deallocation - this is the action on the page
     * that is being deallocated as opposed to the action on the allocation 
     * page.
     * <p>
     *
     * @param RawTransaction	The transaction
     * @param page				that page to be invalidated
     *
     * @exception StandardException Standard Cloudscape policy.  
     **/
	public void actionInvalidatePage(
    RawTransaction  t, 
    BasePage        page)
		 throws StandardException;

    /**
     * Initialize the page due to allocation.
     * <p>
     * Initialize the page due to allocation - this page could be brand new or 
     * it could be being re-allocated.
     * <p>
     *
     * @param RawTransaction	The transaction
     * @param page				that page to be initialized
     * @param initFlag			flags set to values in BasePage.INIT_PAGE_* 
     *                          which indicates how the new page is to be
     *                          initialized.
     * @param pageFormatId		The format Id of the page being initialized.
     *
     * @exception StandardException Standard Cloudscape policy.
     **/
	public void actionInitPage(
    RawTransaction  t, 
    BasePage        page, 
    int             initFlag, 
    int             pageFormatId, 
    long            pageOffset)
		 throws StandardException;

    /**
     * Shrink the reserved space to the new value.  
     * <p>
     * Shrink the reserved space to the new value.  This action is not undoable.
     * <p>
     * @param RawTransaction	The transaction
     * @param page				that page to be initialized
     * @param slot				the slot number of the record 
     * @param recordId			the recordID of the record 
     * @param newValue			the new reserved space value
     *
     * @exception StandardException Unexpected exception from the implementation
     *
     **/
	 public void actionShrinkReservedSpace(
     RawTransaction t, 
     BasePage       page,
     int            slot, 
     int            recordId, 
     int            newValue, 
     int            oldValue)
		 throws StandardException;
}
