/*

   Derby - Class org.apache.derby.impl.store.raw.data.UpdateOperation

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

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.raw.xact.RawTransaction; 

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.util.ByteArray;
import org.apache.derby.iapi.services.io.CompressedNumber;
import org.apache.derby.iapi.services.io.DynamicByteArrayOutputStream;
import org.apache.derby.iapi.services.io.DynamicByteArrayOutputStream;

import java.io.OutputStream;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import org.apache.derby.iapi.services.io.LimitObjectInput;


/**
	Represents the update of a particular row on a page.

	<PRE>
	@format_id	LOGOP_UPDATE
		the formatId is written by FormatIdOutputStream when this object is
		written out by writeObject
	@purpose	update a record on the page
	@upgrade
	@disk_layout
		PhysicalPageOperation	the super class
		doMeSlot(CompressedInt)	the slot the updated record is in
		recordId(CompressedInt) the recordId of the updated record

		OptionalData	The new image of the record (length included), 
						follow by the old image of the record (length included)
	@end_format
	</PRE>
*/

public final class UpdateOperation extends PhysicalPageOperation {

	protected int			doMeSlot;	// record slot - only valid during a doMe() operation
	protected int			recordId;	// record id
	transient protected int nextColumn;	// next column that needs to be updated in a row.

	transient protected ByteArray preparedLog;
	
	public UpdateOperation(
    RawTransaction              t, 
    BasePage                    page, 
    int                         slot, 
    int                         recordId,
    Object[]       row, 
    FormatableBitSet                     validColumns,
    int                         realStartColumn, 
    DynamicByteArrayOutputStream      logBuffer, 
    int                         realSpaceOnPage, 
    RecordHandle                headRowHandle)
		throws StandardException
	{
		super(page);

		this.doMeSlot = slot;
		this.recordId = recordId;
		this.nextColumn = -1;
		
		// RESOLVE SRW-DJD/YYZ
		try {
			writeOptionalDataToBuffer(t, (DynamicByteArrayOutputStream) logBuffer,
				row, validColumns, realStartColumn,
				realSpaceOnPage, headRowHandle);
		} catch (IOException ioe) {
			throw StandardException.newException(
                    SQLState.DATA_UNEXPECTED_EXCEPTION, ioe);
		}

	}

	/*
	 * Formatable methods
	 */

	// no-arg constructor, required by Formatable 
	public UpdateOperation() { super(); }

	public void writeExternal(ObjectOutput out) throws IOException 
	{
		super.writeExternal(out);
		CompressedNumber.writeInt(out, doMeSlot);
		CompressedNumber.writeInt(out, recordId);
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
		recordId = CompressedNumber.readInt(in);
	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.LOGOP_UPDATE;
	}

	/**
		Return the last column of the row this operation logged
	*/
	public int getNextStartColumn() {
		return nextColumn;
	}

	/*
	 * Loggable methods
	 */
	/**
		Store the new record directly over the old record, the implementation
		of storeRecord is responsible for removing any old data.

		@exception StandardException Thrown by methods I call
		@exception IOException Thrown by methods I call

		@see BasePage#storeRecord
		@see org.apache.derby.iapi.store.raw.Loggable#doMe
	*/
	public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in)
		 throws StandardException, IOException 
	{
		this.page.storeRecord(instant, doMeSlot, false, in);
	}
	

	/*
	 * PhysicalPageOperation methods
	 */

	/**
		Store the old record directly over the new record, the implementation
		of storeRecord is responsible for removing any new data.

		@exception StandardException Thrown by methods I call
		@exception IOException Thrown by methods I call

		@see BasePage#storeRecord
		@see PhysicalPageOperation#undoMe
	*/
	public void undoMe(Transaction xact, BasePage undoPage,
					   LogInstant CLRInstant, LimitObjectInput in)
		 throws StandardException, IOException 
	{

		int slot = undoPage.findRecordById(recordId, Page.FIRST_SLOT_NUMBER);

		// skip the after image of the record
		undoPage.skipRecord(in);

		undoPage.storeRecord(CLRInstant, slot, false, in);
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
		Write out the changed colums of new record (from the row) followed by 
        changed columns of the old record (from the page).

		@exception StandardException Thrown by methods I call
		@exception IOException Thrown by methods I call
	*/
	private void writeOptionalDataToBuffer(
    RawTransaction                  t, 
    DynamicByteArrayOutputStream    logBuffer,
    Object[]           row, 
    FormatableBitSet                         validColumns,
    int                             realStartColumn, 
    int                             realSpaceOnPage, 
    RecordHandle                    headRowHandle)
		throws StandardException, IOException
	{

		if (SanityManager.DEBUG) 
        {
			SanityManager.ASSERT(this.page != null);
		}

		if (realStartColumn == (-1)) 
        {
			logBuffer = t.getLogBuffer();
		}

		int optionalDataStart = logBuffer.getPosition();

		if (SanityManager.DEBUG) 
        {

            SanityManager.ASSERT(
                (realStartColumn != -1 || optionalDataStart == 0),
                "Buffer for writing optional data should start at position 0");
        }


		this.nextColumn = 
            this.page.logRow(
                doMeSlot, false, recordId, row, validColumns,
                logBuffer, 0, Page.INSERT_OVERFLOW, realStartColumn,
                realSpaceOnPage, 100);

		FormatableBitSet loggedColumns = validColumns;

        // If this update results in moving columns off the current page to
        // another page, then we must log the before image values of the columns
        // being moved (deleted from this page) in addition to logging the
        // columns actually being changed as part of the update.

        if ((nextColumn != -1) && (validColumns != null))
        {
            // if nextColumn is not -1, then this must be an update which moves
            // columns off of the current page.  If validColumns == null then
            // we are logging all of the before image columns anyway.

            // get total number of fields of the old record.
			int numberFields = page.getHeaderAtSlot(doMeSlot).getNumberFields();

            // create new bit map, copying all bits that were set in original
			loggedColumns = new FormatableBitSet(validColumns);

            // make sure there is room in the bit map to add the columns being
            // deleted from the end of the row.
            // The important thing is that endField must be at least as big as
            // the number of columns in the entire record (including previous
            // pages of a long row) up to the end of this page.
            int endField = nextColumn + numberFields;
            loggedColumns.grow(endField);
            // now include all columns being deleted.
            // This actually sets too many bits in this bit set but
            // logRecord will just ignore the extra bits.
			for (int i = nextColumn; i < endField; i++)
            {
				loggedColumns.set(i);
			}
        }

		// log the old version of the changed data
		this.page.logRecord(
            doMeSlot, BasePage.LOG_RECORD_FOR_UPDATE,
            recordId, loggedColumns, logBuffer, headRowHandle);

        // get length of all the optional data.
		optionalDataStart = logBuffer.getBeginPosition();
		int optionalDataLength = logBuffer.getPosition() - optionalDataStart;

		// set the position to the beginning of the buffer
		logBuffer.setPosition(optionalDataStart);

		this.preparedLog = new ByteArray(
            logBuffer.getByteArray(), optionalDataStart, optionalDataLength);
	}

	/*
	 * PageBasicOperation
	 */

	/**
	 * restore the before image of the page
	 *
	 * @exception StandardException Standard Cloudscape Error Policy
	 * @exception IOException problem reading the complete log record from the
	 * input stream
	 */
	public void restoreMe(Transaction xact, BasePage undoPage,
					   LogInstant CLRInstant, LimitObjectInput in)
		 throws StandardException, IOException 
	{
		undoMe(xact, undoPage, CLRInstant, in);
	}


	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return super.toString() + 
			"Update " +
			" Slot=" + doMeSlot + 
			" recordId=" + recordId;
		}
		else
			return null;
	}
}
