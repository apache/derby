/*

   Derby - Class org.apache.derby.impl.store.raw.data.PurgeOperation

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

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.Storable;

import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.xact.RawTransaction; 

import org.apache.derby.iapi.store.raw.log.LogInstant;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;

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
	USE WITH EXTREME Caution: Purge records from a Page.

	Represents purging of a range of rows from the page.

	<PRE>
	@format_id	LOGOP_PURGE
		the formatId is written by FormatIdOutputStream when this object is
		written out by writeObject
	@purpose	purge num_rows from the page
	@upgrade
	@disk_layout
		PagePhysicalOperation	the super class
		slot(CompressedInt)	the slot to start purging
		num_rows(CompressedInt)	number of rows rows to purge
		recordIds(CompressedInt[num_rows]) the recordIds of the purged rows

		OptionalData	the before images of the rows that were purged
	@end_format
	</PRE>

   @see Page#purgeAtSlot
*/
public final class PurgeOperation extends PhysicalPageOperation {

	protected int	slot;	// purge num_rows records starting at this slot
							// caller must guarentee that during undo of the
							// log record, this slot is the correct slot to
							// re-insert the purged record 
	protected int 	num_rows;
	protected int[]	recordIds;	// record Id


	transient protected ByteArray preparedLog;

	public PurgeOperation(RawTransaction t, BasePage page, int slot, int
						  num_rows, int[] recordIds, boolean needDataLogged)
		throws StandardException
	{
		super(page);

		this.slot = slot;
		this.num_rows = num_rows;
		this.recordIds = recordIds;

		try {
			writeOptionalDataToBuffer(t, needDataLogged);
		} catch (IOException ioe) {
			throw StandardException.newException(
                    SQLState.DATA_UNEXPECTED_EXCEPTION, ioe);
		}
	
	}

	/*
	 * Formatable methods
	 */

	// no-arg constructor, required by Formatable 
	public PurgeOperation() { super(); }

	public void writeExternal(ObjectOutput out) throws IOException 
	{
		super.writeExternal(out);

		CompressedNumber.writeInt(out, slot);
		CompressedNumber.writeInt(out, num_rows);

		for (int i = 0; i < num_rows; i++)
			CompressedNumber.writeInt(out, recordIds[i]);
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
		slot = CompressedNumber.readInt(in);
		num_rows = CompressedNumber.readInt(in);

		recordIds = new int[num_rows];
		for (int i = 0; i < num_rows; i++)
			recordIds[i] = CompressedNumber.readInt(in);
	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.LOGOP_PURGE;
	}


	/*
	 * Loggable methods
	 */
	/**
		Apply the purge operation to the page.

		@exception IOException Can be thrown by any of the methods of ObjectInput.
		@exception StandardException Standard Cloudscape policy.		

		@see org.apache.derby.iapi.store.raw.Loggable#doMe
	*/
	public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in) 
		 throws StandardException, IOException 
	{
		// purge the records in the stored version
		// we need to remove from high to low because the slots will be moved down
		// as soon as one is removed.

		// we could get the slot with the recordId but that will be a waste
		// since the page was never unlatch and the slot number is good

		for (int i = num_rows-1; i >= 0; i--)
		{
			this.page.purgeRecord(instant, slot+i, recordIds[i]);
		}
	}

	/*
	 * PhysicalPageOperation methods
	 */
	
	/**
		Undo the purge operation on the page.

		@exception IOException Can be thrown by any of the methods of ObjectInput.
		@exception StandardException Standard Cloudscape policy.		

		@see PhysicalPageOperation#undoMe
	*/
	public void undoMe(Transaction xact, BasePage undoPage,
					   LogInstant CLRInstant, LimitObjectInput in)
		 throws StandardException, IOException 
	{
		for (int i = 0; i < num_rows; i++)
		{
			undoPage.storeRecord(CLRInstant, slot+i, true, in);
		}
		undoPage.setAuxObject(null);
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

	/*
		methods to support prepared log
		
		the following two methods should not be called during recover
	*/

	public ByteArray getPreparedLog()
	{
		return (this.preparedLog);
	}

	/**
		Write out the purged record from the page.  Used for undo only.

		@exception IOException Can be thrown by any of the methods of ObjectOutput.
		@exception StandardException Standard Cloudscape policy.		
	*/
	private void writeOptionalDataToBuffer(RawTransaction t, boolean needDataLogged)
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

		for (int i = 0; i < num_rows; i++)
		{
			if(needDataLogged)
			{
				this.page.logRecord(i+slot, BasePage.LOG_RECORD_DEFAULT, 
									recordIds[i],  (FormatableBitSet) null, logBuffer,
									(RecordHandle)null);
			}else
			{
				this.page.logRecord(i+slot, BasePage.LOG_RECORD_FOR_PURGE, 
									recordIds[i],  (FormatableBitSet) null, logBuffer,
									(RecordHandle)null);
			}
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
			String str = super.toString() + 
				"Purge : " + num_rows + " slots starting at " + slot;

			for (int i = 0; i < num_rows; i++)
			{
				str += " (recordId=" + recordIds[i] + ")";
			}
			return str;
		}	
		else
			return null;
	}
}
