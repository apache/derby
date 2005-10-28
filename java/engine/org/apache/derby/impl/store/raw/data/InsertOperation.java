/*

   Derby - Class org.apache.derby.impl.store.raw.data.InsertOperation

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
import org.apache.derby.impl.store.raw.data.ReclaimSpace;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;

import org.apache.derby.iapi.store.raw.PageKey;
import org.apache.derby.iapi.store.raw.Compensation;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.services.io.CompressedNumber;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.util.ByteArray;
import org.apache.derby.iapi.services.io.DynamicByteArrayOutputStream;
import org.apache.derby.iapi.services.io.DynamicByteArrayOutputStream;

import java.io.OutputStream;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import org.apache.derby.iapi.services.io.LimitObjectInput;


/**	
	Represents an insert of a record onto a page.

	<PRE>
	@format_id	LOGOP_INSERT
		the formatId is written by FormatIdOutputStream when this object is
		written out by writeObject
	@purpose	insert a row onto a page
	@upgrade
	@disk_layout
		LogicalPageOperation the superclass
		doMeSlot(CompressedInt) which slot to operate on
		insertFlat(byte)		to undo with purge or with delete

		OptionalData	The after image of the row to be inserted.
	@end_format
	</PRE>
    @see Page#insertAtSlot
*/
public final class InsertOperation extends LogicalPageOperation 
{

	protected int			doMeSlot;	// insert slot - only valid during doMe()
	protected byte			insertFlag;	// see page insertFlag
	
	/** next column that need to be inserted. */
	transient protected int			startColumn; 

	transient protected ByteArray preparedLog;

	// yyz: revisit later, whether we need preparedLog, maybe everything will be prepared...
	public InsertOperation(
    RawTransaction              t, 
    BasePage                    page, 
    int                         slot, 
    int                         recordId,
    Object[]       row, 
    FormatableBitSet                     validColumns,
    LogicalUndo                 undo, 
    byte                        insertFlag, 
    int                         startColumn, 
    boolean                     isLongColumn,
    int                         realStartColumn, 
    DynamicByteArrayOutputStream      logBuffer, 
    int                         realSpaceOnPage, 
    int                         overflowThreshold) 
		throws StandardException
	{
		super(page, undo, recordId);

		this.doMeSlot = slot;
		this.insertFlag = insertFlag;
		this.startColumn = startColumn;

		try {
			writeOptionalDataToBuffer(t, logBuffer, row, validColumns,
				isLongColumn, realStartColumn, realSpaceOnPage, overflowThreshold);
		} catch (IOException ioe) {
			throw StandardException.newException(
                    SQLState.DATA_UNEXPECTED_EXCEPTION, ioe);
		}
	}

	/*
	 * Formatable methods
	 */

	// no-arg constructor, required by Formatable 
	public InsertOperation() { super(); }

	/**
		Write this out.
		@exception IOException error writing to log stream
	*/
	public void writeExternal(ObjectOutput out) throws IOException 
	{
		super.writeExternal(out);
		CompressedNumber.writeInt(out, doMeSlot);
		out.writeByte(insertFlag);

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
		insertFlag = in.readByte();
	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.LOGOP_INSERT;
	}

	/*
	 * Loggable methods
	 */
	/**
		@exception IOException Can be thrown by any of the methods of ObjectInput.
		@exception StandardException Standard Cloudscape policy.		
	  
		@see org.apache.derby.iapi.store.raw.Loggable#doMe
	*/
	public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in)
		 throws StandardException, IOException 
	{
		this.page.storeRecord(instant, doMeSlot, true, in);
	}

	/*
	 * PageOperation methods
	 */

	/**
		Undo the insert by simply marking the just inserted record as deleted.
		All logical undo logic has already been taken care of by generateUndo.

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

		if ((insertFlag & Page.INSERT_UNDO_WITH_PURGE) != 0)
		{
			undoPage.purgeRecord(CLRInstant, slot, undoRecordId);

			RawTransaction rxact = (RawTransaction)xact;

			// If we purged the last row off an overflow page, reclaim that
			// page - we have to do this post transaction termination because we
			// are underneath the log right now and cannot interrupt the log
			// stream.
			if (rxact.handlesPostTerminationWork() &&
				undoPage.isOverflowPage() && undoPage.recordCount() == 0)
			{
				ReclaimSpace work = 
					new ReclaimSpace(ReclaimSpace.PAGE, (PageKey)undoPage.getIdentity(),
									 rxact.getDataFactory(), true /* service ASAP */);
				rxact.addPostTerminationWork(work);
			}
		}
		else
		{
			undoPage.setDeleteStatus(CLRInstant, slot, true);
		}

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
	 * method to support BeforeImageLogging
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
		if (SanityManager.DEBUG)
		{
			int slot = undoPage.findRecordById(recordId,Page.FIRST_SLOT_NUMBER);

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
		insertFlag |= Page.INSERT_UNDO_WITH_PURGE;

		// undo the insert with purge.
		undoMe(xact, undoPage, recordId, CLRinstant, in);
	}

	/*
		methods to support prepared log
		
		the following two methods should not be called during recover
	*/

	public ByteArray getPreparedLog()
	{
		return (this.preparedLog);
	}


	public int getNextStartColumn()
	{
		return (this.startColumn);
	}
	
	/**
		Writes out the row that is to be inserted as the optional data.

		@exception IOException Can be thrown by any of the methods of ObjectOutput
		@exception StandardException Standard Cloudscape policy.		
	*/
	private void writeOptionalDataToBuffer(
    RawTransaction          t, 
    DynamicByteArrayOutputStream  logBuffer,
    Object[]   row, 
    FormatableBitSet                 validColumns, 
    boolean                 isLongColumn,
    int                     realStartColumn, 
    int                     realSpaceOnPage, 
    int                     overflowThreshold)
		throws StandardException, IOException
	{

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(this.page != null);
		}

		DynamicByteArrayOutputStream localLogBuffer = null;
		if (logBuffer != null) {
			localLogBuffer = (DynamicByteArrayOutputStream) logBuffer;
		} else {
			realStartColumn = -1;
			realSpaceOnPage = -1;
			localLogBuffer = t.getLogBuffer();
		}
		
		if (isLongColumn) {
			this.startColumn = this.page.logLongColumn(doMeSlot, recordId,
				row[0], localLogBuffer);
		} else {
			this.startColumn = this.page.logRow(doMeSlot, true, recordId,
				row, validColumns, localLogBuffer, this.startColumn, insertFlag,
				realStartColumn, realSpaceOnPage, overflowThreshold);
		}

		int optionalDataStart = localLogBuffer.getBeginPosition();
		int optionalDataLength = localLogBuffer.getPosition() - optionalDataStart;

		this.preparedLog = new ByteArray (localLogBuffer.getByteArray(), optionalDataStart,
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
			"Insert : " +
			" Slot=" + doMeSlot +
			" recordId=" + recordId;
		}
		else
			return null;
	}

}
