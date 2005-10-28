/*

   Derby - Class org.apache.derby.impl.store.raw.data.InitPageOperation

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

import org.apache.derby.impl.store.raw.data.BasePage;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.store.raw.log.LogInstant;

import org.apache.derby.iapi.services.io.CompressedNumber;

import java.io.OutputStream;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import org.apache.derby.iapi.services.io.LimitObjectInput;

/**
	This operation initializes the page that is being allocated,
	this operation does not change the alloc page information.

	<PRE>
	@format_id	LOGOP_INIT_PAGE
		the formatId is written by FormatIdOutputStream when this object is
		written out by writeObject
	@purpose	initialized a page
	@upgrade
	@disk_layout
		PhysicalPageOperation the superclass
		nextRecordId(CompressedInt)	the next recordId this page should give out
		initFlag(CompressedInt)		initialization flag: reuse, overflow
		pageformat(int)				the page's formatId

		OptionalData	none
	@end_format
	</PRE>
*/
public final class InitPageOperation extends PhysicalPageOperation
{
	protected int	nextRecordId; // next recordId
	protected int	initFlag;
	protected int	pageFormatId;
	protected long	pageOffset;

	protected boolean reuse;	// is this page being initialize for reuse, or for first time
	protected boolean overflowPage; // is this page an overflow page

	public InitPageOperation(BasePage page, int flag, int formatid, 
							 long offset)
		 throws StandardException
	{
		super(page);

		initFlag = flag;
		pageFormatId = formatid;
		pageOffset = offset;

		// unless we specified recordId should be reusable, when we reuse a
		// page, we keep incrementing the existing recordId
		if ((initFlag & BasePage.INIT_PAGE_REUSE_RECORDID) == 0)
			nextRecordId = page.newRecordId();
		else
			nextRecordId = RecordHandle.FIRST_RECORD_ID;
	}

	/*
	 * Formatable methods
	 */

	// no-arg constructor, required by Formatable 
	public InitPageOperation() { super(); }

	/**
		Write this out.
		@exception IOException error writing to log stream
	*/
	public void writeExternal(ObjectOutput out) throws IOException
	{
		super.writeExternal(out);
		CompressedNumber.writeInt(out, nextRecordId);
		CompressedNumber.writeInt(out, initFlag);
		CompressedNumber.writeLong(out, pageOffset);
		out.writeInt(pageFormatId);
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
		nextRecordId = CompressedNumber.readInt(in);
		initFlag = CompressedNumber.readInt(in);
		pageOffset = CompressedNumber.readLong(in);
		pageFormatId = in.readInt();
	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.LOGOP_INIT_PAGE;
	}
	/*
	 * Loggable methods
	 */
	/**
		Mark the page as valid, and clear out any crud from the page

		@exception IOException Can be thrown by any of the methods of ObjectInput.
		@exception StandardException Standard Cloudscape policy.

		@see org.apache.derby.iapi.store.raw.Loggable#doMe
	*/
	public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in)
		 throws StandardException, IOException 
	{
		boolean overflowPage = ((initFlag & BasePage.INIT_PAGE_OVERFLOW) != 0);
		boolean reuse = ((initFlag & BasePage.INIT_PAGE_REUSE) != 0);

		this.page.initPage(instant,
						   BasePage.VALID_PAGE,
						   nextRecordId,
						   overflowPage, reuse);
	}

	/*
	 * Override PageBasicOperation's getPageForLoadTran
	 */
	/**
		If we are in load tran, this page may not exist for the container yet.
		We need to create it first.

		This routine is called as the last resort of find page, the container
		handle has already been found and it is not dropped.

		@exception StandardException Standard Cloudscape policy.
	*/
	protected BasePage getPageForLoadTran(Transaction xact)
		 throws StandardException
	{
		BasePage p = super.getPageForLoadTran(xact);
		if (p != null)
			return p;

		// create the page
		// RESOLVE: we need the page format to properly recreate an Alloc page
		// NEED TO UPGRADE this log record.
		p = (BasePage)containerHdl.reCreatePageForLoadTran(
						pageFormatId,								   
						getPageId().getPageNumber(), 
						pageOffset);
		return p;
	}
	

	/*
	 * PhysicalPageOperation method
	 */

	/**
		Mark the page as free

		@exception StandardException Thrown by methods I call
		@exception IOException Thrown by methods I call

		@see PhysicalPageOperation#undoMe
	*/
	public void undoMe(Transaction xact, BasePage undoPage, LogInstant CLRInstant, 
					   LimitObjectInput in)
		 throws StandardException, IOException 
	{
		undoPage.setPageStatus(CLRInstant, BasePage.INVALID_PAGE);
		// only set the page to invalid, cannot wipe out the page to zero's
		// becuase recovery may need to redo some operations that depend on the
		// content of the page.

		undoPage.setAuxObject(null);
	}


	/*
	 * PageBasicOperation methods
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
			boolean overflowPage = ((initFlag & BasePage.INIT_PAGE_OVERFLOW) != 0);
			boolean reuse = ((initFlag & BasePage.INIT_PAGE_REUSE) != 0);

			return super.toString() + "Init Page.  Overflow = "
				+ overflowPage + " reuse " + reuse + " nextRecordId " + nextRecordId;
		}
		else
			return null;
	}

}
