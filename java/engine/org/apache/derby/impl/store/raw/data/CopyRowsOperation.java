/*

   Derby - Class org.apache.derby.impl.store.raw.data.CopyRowsOperation

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

import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.raw.xact.RawTransaction; 

import org.apache.derby.iapi.error.StandardException;

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
	Represents copying num_rows from one page to another page.

	<PRE>
	@format_id	LOGOP_COPY_ROWS
		the formatId is written by FormatIdOutputStream when this object is
		written out by writeObject
	@purpose	copy some rows from one page to another
	@upgrade
	@disk_layout
		PhysicalPageOperation the superclass
		num_rows(CompressedInt)	number of rows to copy
		destSlot(CompressedInt)	the first slot number at the destination page
		recordIds(CompressedInt[num_rows]) the recordIds at the destination page

		OptionalData	the after image of the rows to be inserted into the
						destination page
	@end_format
	</PRE>
*/
public class CopyRowsOperation extends PhysicalPageOperation {

	protected int	num_rows;
	protected int	destSlot;		// copy into this page starting from destSlot
	protected int[] recordIds;  // num_rows of recordIds (use these for undo)
	protected int[] reservedSpace;	// number of bytes to reserve for each row.

	transient protected ByteArray preparedLog; 

	public CopyRowsOperation(RawTransaction t, BasePage destPage, BasePage srcPage, 
							 int destSlot, int num_rows,
							 int srcSlot, int[] recordIds)
		throws StandardException
	{
		super(destPage);

		this.num_rows = num_rows;
		this.destSlot = destSlot;
		this.recordIds = recordIds;

		try {
			reservedSpace = new int[num_rows];
			for (int i = 0; i < num_rows; i++) {
				reservedSpace[i] = srcPage.getReservedCount(i + srcSlot);
			}

			writeOptionalDataToBuffer(t, srcPage, srcSlot);
		} catch (IOException ioe) {
			throw StandardException.newException(
                    SQLState.DATA_UNEXPECTED_EXCEPTION, ioe);
		}
	}

	/*
	 * Formatable methods
	 */

	// no-arg constructor, required by Formatable 
	public CopyRowsOperation() { super(); }
/*
	public CopyRowsOperation(BasePage destPage) { super(destPage); }
*/
	public void writeExternal(ObjectOutput out) throws IOException 
	{
		super.writeExternal(out);

		CompressedNumber.writeInt(out, num_rows);
		CompressedNumber.writeInt(out, destSlot);
		
		for (int i = 0; i < num_rows; i++) {
			CompressedNumber.writeInt(out, recordIds[i]);
			CompressedNumber.writeInt(out, reservedSpace[i]);
		}
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

		num_rows = CompressedNumber.readInt(in);
		destSlot = CompressedNumber.readInt(in);

		recordIds = new int[num_rows];
		reservedSpace = new int[num_rows];
		for (int i = 0; i < num_rows; i++) {
			recordIds[i] = CompressedNumber.readInt(in);
			reservedSpace[i] = CompressedNumber.readInt(in);
		}
	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.LOGOP_COPY_ROWS;
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
		/*
		 * this operation's do me will bump the page version by more than 1
		 */
		for (int i = 0; i < num_rows; i++)
		{
			page.storeRecord(instant, destSlot+i, true, in);

			if (reservedSpace[i] > 0)
				page.reserveSpaceForSlot(instant, destSlot + i, reservedSpace[i]);
		}
	}

	/*
	 * PhysicalPageOperation method
	 */

	/**
	    to undo this operation, purge all records that were copied over.

		@exception IOException Can be thrown by any of the methods of ObjectInput.
		@exception StandardException	Standard Cloudscape error policy
		@see PhysicalPageOperation#undoMe
	 */
	public void undoMe(Transaction xact, BasePage undoPage,
					   LogInstant CLRInstant, LimitObjectInput in)
		 throws StandardException, IOException 
	{
		// purge the records in the stored version
		// since we search for each recordId, it doesn't matter whether we
		// purge from high to low.  In most cases, it will be in order so do it
		// from high to low to save some work.

		int slot;

		for (int i = num_rows-1; i >= 0; i--)
		{
			slot = undoPage.findRecordById(recordIds[i], i);
			undoPage.purgeRecord(CLRInstant, slot, recordIds[i]);
		}

		undoPage.setAuxObject(null);
	}

	/*
	 * PageBasicOperation method to support BeforeImageLogging
	 */

	/**
	 * restore the before image of the page
	 *
	 * @exception IOException problem reading the complete log record from the input stream
	 * @exception StandardException Standard Cloudscape Error Policy
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
		Write the rows that are to be copied into this page

		@exception IOException Can be thrown by any of the methods of ObjectOutput.
		@exception StandardException Standard Cloudscape policy.		

	*/
	private void writeOptionalDataToBuffer(RawTransaction t, BasePage srcPage, int srcSlot)
		throws StandardException, IOException
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(this.page != null);
			SanityManager.ASSERT(srcPage != null);
		}

		DynamicByteArrayOutputStream logBuffer = t.getLogBuffer();
		int optionalDataStart = logBuffer.getPosition();

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(optionalDataStart == 0,
				"Buffer for writing the optional data should start at position 0");
		}

		// check to make sure the destination page have the necessary space to
		// take the rows
		int[] spaceNeeded = new int[num_rows];
		int startPosition = logBuffer.getPosition();

		for (int i = 0; i < num_rows; i++)
		{
			// the recordId passed in is the record Id this row will have at
			// the destination page, not the record Id this row has on the
			// srcPage.
			srcPage.logRecord(i + srcSlot, BasePage.LOG_RECORD_DEFAULT,
							  recordIds[i],  (FormatableBitSet) null, logBuffer,
							  (RecordHandle)null);
			spaceNeeded[i] = logBuffer.getPosition() - startPosition;
			startPosition = logBuffer.getPosition();

			// now spaceNeeded[i] has the actual record size.  However, the src
			// page may actually leave more space for the record due to
			// reserved space.  Because we want to copy the reserve space as well,
			// we need to take into account that amount.
			spaceNeeded[i] += reservedSpace[i];
 		}

		// page is the destination page.
		if (!page.spaceForCopy(num_rows, spaceNeeded))
        {
			throw StandardException.newException(
                    SQLState.DATA_NO_SPACE_FOR_RECORD);
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
				"CopyRows : " + num_rows + " to slots starting at " + destSlot;

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
