/*

   Derby - Class org.apache.derby.impl.store.raw.data.UpdateFieldOperation

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

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.impl.store.raw.data.BasePage;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;

import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.xact.RawTransaction; 

import org.apache.derby.iapi.store.raw.log.LogInstant;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.CompressedNumber;
import org.apache.derby.iapi.util.ByteArray;
import org.apache.derby.iapi.services.io.DynamicByteArrayOutputStream;

import java.io.OutputStream;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import org.apache.derby.iapi.services.io.LimitObjectInput;


/**
	Represents the update of a particular field of a row on a page.

	<PRE>
	@format_id	LOGOP_UPDATE_FIELD
		the formatId is written by FormatIdOutputStream when this object is
		written out by writeObject
	@purpose	update a field of a record on the page
	@upgrade
	@disk_layout
		LogicalPageOperation	the super class
		doMeSlot(CompressedInt) the slot of the record being updated
		fieldId(CompressedInt)	the recordId of the record being updated

		OptionalData	The after image of the column (length included),
						follow by the old image of the record (length
						included).  If this is logically undoable, then the
						before image of the entire row is logged
	@end_format
	</PRE>
*/
public final class UpdateFieldOperation extends LogicalPageOperation 
{

	protected int			doMeSlot;	// insert slot - only valid during a doMe() operation
	protected int			fieldId;

	transient protected ByteArray preparedLog;

	public UpdateFieldOperation(
    RawTransaction      t, 
    BasePage            page, 
    int                 slot, 
    int                 recordId, 
    int                 fieldId, 
    Object column, 
    LogicalUndo         undo) 
		throws StandardException
	{
		super(page, undo, recordId);

		this.doMeSlot = slot;
		this.fieldId = fieldId;

		try {
			writeOptionalDataToBuffer(t, column);
		} catch (IOException ioe) {
			throw StandardException.newException(
                    SQLState.DATA_UNEXPECTED_EXCEPTION, ioe);
		}
	}

	/*
	 * Formatable methods
	 */

	// no-arg constructor, required by Formatable 
	public UpdateFieldOperation() { super(); }

	public void writeExternal(ObjectOutput out) throws IOException 
	{
		super.writeExternal(out);
		CompressedNumber.writeInt(out, doMeSlot);
		CompressedNumber.writeInt(out, fieldId);
	}

	/**
		Read this in
		@exception IOException error reading from log stream
		@exception ClassNotFoundException log stream corrupted
	*/
	public void readExternal(ObjectInput in) 
		 throws IOException, ClassNotFoundException
	{
		super.readExternal(in);
		doMeSlot = CompressedNumber.readInt(in);
		fieldId = CompressedNumber.readInt(in);
	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.LOGOP_UPDATE_FIELD;
	}

	/*
	 * Loggable methods
	 */
	/**
		Change the value of a field.

		@exception IOException Can be thrown by any of the methods of ObjectInput.
		@exception StandardException Standard Cloudscape policy.

		@see org.apache.derby.iapi.store.raw.Loggable#doMe
	*/
	public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in)
		 throws StandardException, IOException 
	{
		this.page.storeField(instant, doMeSlot, fieldId, in);
	}
	
	/*
	 * Undoable methods
	 */

	/**
		Restore field to its old value.

		@exception IOException Can be thrown by any of the methods of ObjectInput.
		@exception StandardException Standard Cloudscape policy.

		@see LogicalPageOperation#undoMe
	*/
	public void undoMe(Transaction xact, BasePage undoPage, int undoRecordId,
					   LogInstant CLRInstant, LimitObjectInput in)
		 throws StandardException, IOException
	{
		int slot = 
            undoPage.findRecordById(undoRecordId, Page.FIRST_SLOT_NUMBER);
		
		if (SanityManager.DEBUG)
		{
			// if the record Id has changed, the page had better changed
			// this can only happen during recovery since in run time undo,
			// this resetRecordHandle gets called and this object have the new
			// page number and recordId
			if (undoRecordId != this.recordId)
				if (undoPage.getPageNumber() == getPageId().getPageNumber())
					SanityManager.THROWASSERT(
									 "recordId changed from " + this.recordId +
									 " to " + undoRecordId +
									 " but page number did not change " +
									 undoPage.getPageNumber());

			if (slot == -1)
				SanityManager.THROWASSERT(
					"recordId " +
					undoRecordId +
					" not found on page " +
					undoPage.getPageNumber());
		}

		undoPage.skipField((java.io.ObjectInput) in);	// skip the after image of the column
		undoPage.storeField(CLRInstant, slot, fieldId, in);
		undoPage.setAuxObject(null);
	}

	/*
	 * LogicalUndoable methods
	 */


	/**
		Restore the row stored in the optional data of the log record.

		@exception IOException error reading from log stream
		@exception StandardException Standard Cloudscape error policy
	*/
	public void restoreLoggedRow(Object[] row, LimitObjectInput in)
		throws StandardException, IOException
	{
		BasePage p = null;

		try {
			// the optional data is written by the page in the same format it
			// stores record on the page, 
			// only a page knows how to restore a logged row back to a storable row
			// first get the page where the insert went even though the row may no
			// longer be there
			p = (BasePage)(getContainer().getPage(getPageId().getPageNumber()));

			// skip over the before and after image of the column, position the
			// input stream at the entire row
			p.skipField(in);	// AI of the column
			p.skipField(in);	// BI of the column

			p.restoreRecordFromStream(in, row);

			// RESOLVE: this returns the BI of the row, what we need is the AI
			// of the row.  We need to someone splice in the AI of the column
			// into the storable row.

		} finally {

			if (p != null) {
				p.unlatch();
				p = null;
			}
		}
	}

	/*
	 * method to support BeforeImageLogging
	 */

	/**
	 * restore the before image of the page
	 *
	 * @exception StandardException Standard Cloudscape Error Policy
	 * @exception IOException problem reading the complete log record from the
	 * input stream
	 */
	public void restoreMe(Transaction xact, BasePage undoPage, LogInstant CLRInstant, LimitObjectInput in)
		 throws StandardException, IOException
	{
		int slot = undoPage.findRecordById(recordId, Page.FIRST_SLOT_NUMBER);
		if (SanityManager.DEBUG)
		{
			if ( ! getPageId().equals(undoPage.getPageId()))
				SanityManager.THROWASSERT(
								"restoreMe cannot restore to a different page. "
								 + "doMe page:" + getPageId() + " undoPage:" + 
								 undoPage.getPageId());
			if (slot != doMeSlot)
				SanityManager.THROWASSERT(
								"restoreMe cannot restore to a different slot. "
								 + "doMe slot:" + doMeSlot + " undoMe slot: " +
								 slot + " recordId:" + recordId);
		}

		undoPage.skipField(in);	// skip the after image of the column
		undoPage.storeField(CLRInstant, slot, fieldId, in);
		undoPage.setAuxObject(null);
	}

	/*
		methods to support prepared log
		
		the following two methods should not be called during recover
	*/

	public ByteArray getPreparedLog()
	{
		return (this.preparedLog);
	}

	/**
	  Write the old column value and and new column value as optional data.
	  If logical undo, writes out the entire row's before image.

		@exception IOException Can be thrown by any of the methods of ObjectOutput.
		@exception StandardException Standard Cloudscape policy.
	*/
	private void writeOptionalDataToBuffer(
    RawTransaction      t, 
    Object column)
		throws StandardException, IOException
	{

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(this.page != null);
		}

		DynamicByteArrayOutputStream logBuffer = t.getLogBuffer();
		int optionalDataStart = logBuffer.getPosition();

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(optionalDataStart == 0,
				"Buffer for writing optional data should start at position 0");
		}
				
		this.page.logColumn(doMeSlot, fieldId, column, logBuffer, 100); // the after image of the column
		this.page.logField(doMeSlot, fieldId, logBuffer); // the BI of the column
		if (undo != null)
		{
			// RESOLVE: we want the AFTER image of the row, not the BEFORE
			// image.   This works for now because only btree needs a logical
			// undoable updateField and it always update only the pointer field
			// to point to something else.
			//
			// But in the future, it needs to be changed. 

			this.page.logRecord(doMeSlot, BasePage.LOG_RECORD_DEFAULT,
								recordId, (FormatableBitSet) null, logBuffer,
								(RecordHandle)null); 
			// log the BI of the entire row

		}

		int optionalDataLength = logBuffer.getPosition() - optionalDataStart;

		if (SanityManager.DEBUG) {
			if (optionalDataLength != logBuffer.getUsed())
				SanityManager.THROWASSERT("wrong optional data length, optionalDataLength = "
					+ optionalDataLength + ", logBuffer.getUsed() = " + logBuffer.getUsed());
		}

		// set the position to the beginning of the buffer
		logBuffer.setPosition(optionalDataStart);

		this.preparedLog = new ByteArray(logBuffer.getByteArray(), optionalDataStart,
			optionalDataLength);
	}

	/**
	  DEBUG: Print self.
	*/
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return super.toString() + 
				"UpdateField : " + 
				" Slot=" + doMeSlot +
				" recordId=" + recordId +
				" fieldId=" + fieldId;
		}
		else
			return null;
	}

}
