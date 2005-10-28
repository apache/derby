/*

   Derby - Class org.apache.derby.impl.store.raw.data.DeleteOperation

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

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.impl.store.raw.data.BasePage;

import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;

import org.apache.derby.iapi.store.raw.Compensation;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.xact.RawTransaction; 

import org.apache.derby.iapi.store.raw.log.LogInstant;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.services.io.CompressedNumber;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.util.ByteArray;
import org.apache.derby.iapi.services.io.DynamicByteArrayOutputStream;

import java.io.OutputStream;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import org.apache.derby.iapi.services.io.LimitObjectInput;


/**
	Represents a delete (or undelete) of a record in a page.

	<PRE>
	@format_id	LOGOP_DELETE
		the formatId is written by FormatIdOutputStream when this object is
		written out by writeObject
	@purpose	delete a record from a page.
	@upgrade
	@disk_layout
		LogicalPageOperation	the super class
		doMeSlot(CompressedInt)	the slot of the record to delete
		delete(boolean)			if true, delete, else undelete

		OptionalData		if we need logical undo, write the row that was
							deleted as the optional data.  If we don't need
							logical undo, no optional data
	@end_format
	</PRE>
*/
public final class DeleteOperation extends LogicalPageOperation
{
	protected int			doMeSlot;		// delete slot - only valid during a doMe() operation
	protected boolean		delete;			// set record as deleted if true, undeleted if false

	transient protected ByteArray preparedLog;

	public DeleteOperation(RawTransaction t, BasePage page, int slot, int recordId, 
						   boolean delete, LogicalUndo undo)
		throws StandardException
	{
		super(page, undo, recordId);

		doMeSlot = slot;
		this.delete = delete;

		try {
			writeOptionalDataToBuffer(t);
		} catch (IOException ioe) {
			throw StandardException.newException(
                    SQLState.DATA_UNEXPECTED_EXCEPTION, ioe);
		}
	}

	/*
	 * Formatable methods
	 */

	// no-arg constructor, required by Formatable 
	public DeleteOperation() { super(); }

	/**
		Write this out.
		@exception IOException error writing to log stream
	*/
	public void writeExternal(ObjectOutput out) throws IOException 
	{
		super.writeExternal(out);
		CompressedNumber.writeInt(out, doMeSlot);
		out.writeBoolean(delete);
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
		delete = in.readBoolean();
	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.LOGOP_DELETE;
	}

	/*
	 * Loggable methods
	 */
	/**
		Mark the record as deleted on the page.

		@exception IOException Can be thrown by any of the methods of ObjectInput.
		@exception StandardException Standard Cloudscape policy.

		@see org.apache.derby.iapi.store.raw.Loggable#doMe
	*/
	public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in)
		 throws StandardException, IOException 
	{
		this.page.setDeleteStatus(instant, doMeSlot, delete);
	}
	
	/*
	 * Undoable methods
	 */

	/**
		Mark the record as not deleted, and then fix up the in-memory copy
		of the page.  
		All logical undo logic has already been taken care of by generateUndo.

		@exception StandardException Thrown by methods I call
		@exception IOException Thrown by methods I call

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
					undoPage.getPageNumber() + 
                    undoPage);
		}

		undoPage.setDeleteStatus(CLRInstant, slot, !delete);

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
	public void restoreLoggedRow(
    Object[]   row, 
    LimitObjectInput        in)
		throws StandardException, IOException
	{
		Page p = null;

		try {
			// the optional data is written by the page in the same format it
			// stores record on the page, 
			// only a page knows how to restore a logged row back to a storable row
			// first get the page where the insert went even though the row may no
			// longer be there
			p = getContainer().getPage(getPageId().getPageNumber());


			((BasePage)p).restoreRecordFromStream(in, row);

		} finally {

			if (p != null) {
				p.unlatch();
				p = null;
			}
		}
	}

	/*
	 * PageBasicOperation method to support BeforeImageLogging
	 */

	/**
	 * restore the before image of the page
	 *
	 * @exception StandardException Standard Cloudscape Error Policy
	 * @exception IOException problem reading the complete log record from the
	 * input stream
	 */
	public void restoreMe(Transaction xact, BasePage undoPage, LogInstant CLRinstant, LimitObjectInput in)
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
		undoPage.setDeleteStatus(CLRinstant, slot, !delete);
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
	    if logical undo, writes out the row that was deleted

		@exception IOException Can be thrown by any of the methods of ObjectOutput
		@exception StandardException Standard Cloudscape policy.		
	*/
	private void writeOptionalDataToBuffer(RawTransaction t)
		throws StandardException, IOException
	{

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(this.page != null);
		}

		DynamicByteArrayOutputStream logBuffer = t.getLogBuffer();
		int optionalDataStart = logBuffer.getPosition();

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(optionalDataStart == 0,
				"Buffer for writing the optional data should start at position 0");
		}

		if (undo != null)
			this.page.logRecord(doMeSlot, BasePage.LOG_RECORD_DEFAULT,
								recordId,  (FormatableBitSet) null, logBuffer,
								(RecordHandle)null); 
		
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

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return super.toString() +
				" Delete :" + 
				" Slot=" + doMeSlot + 
				" recordId=" + recordId +
				" delete=" + delete;
		}
		else
			return null;
	}
}
