/*

   Derby - Class org.apache.derby.impl.store.raw.data.ChainAllocPageOperation

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

import org.apache.derby.impl.store.raw.data.PhysicalPageOperation;
import org.apache.derby.impl.store.raw.data.BasePage;

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.data.RawContainerHandle;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.log.LogInstant;

import org.apache.derby.iapi.services.io.CompressedNumber;

import java.io.OutputStream;
import java.io.ObjectOutput;
import java.io.IOException;
import org.apache.derby.iapi.services.io.LimitObjectInput;
import java.io.ObjectInput;


// Allocation page operation - to allocate, deallocate or free a page
public final class ChainAllocPageOperation extends PhysicalPageOperation
{

	protected long newAllocPageNum;	// the next alloc page's page number
	protected long newAllocPageOffset; // the next alloc page's page offset 

	public ChainAllocPageOperation(AllocPage allocPage, long pageNumber, long pageOffset)
		 throws StandardException
	{
		super(allocPage);

		newAllocPageNum = pageNumber;
		newAllocPageOffset = pageOffset;
	}
	
	/*
	 * Formatable methods
	 */

	// no-arg constructor, required by Formatable 
	public ChainAllocPageOperation() { super(); }

	public void writeExternal(ObjectOutput out) throws IOException 
	{
		super.writeExternal(out);
		CompressedNumber.writeLong(out, newAllocPageNum);
		CompressedNumber.writeLong(out, newAllocPageOffset);
	}

	/**
		@exception IOException error reading from log stream
		@exception ClassNotFoundException cannot read object from input
	*/
	public void readExternal(ObjectInput in)
		 throws IOException, ClassNotFoundException
	{
		super.readExternal(in);
		newAllocPageNum = CompressedNumber.readLong(in);
		newAllocPageOffset = CompressedNumber.readLong(in);
	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.LOGOP_CHAIN_ALLOC_PAGE;
	}

	/*
	 * Loggable methods
	 */
	/** 
		Link the next alloc page into the page chain
		@exception StandardException container Handle is not active
	*/
	public final void doMe(Transaction tran, LogInstant instant, LimitObjectInput in) 
		 throws StandardException
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(this.page instanceof AllocPage);
		}

		((AllocPage)page).chainNextAllocPage(instant, newAllocPageNum, newAllocPageOffset);
	}

	/*
	 * Undoable methods
	 */

	/**
		Unlink the next alloc page from the page chain

		@exception StandardException Thrown by methods I call
		@see PhysicalPageOperation#undoMe
	*/
	public void undoMe(Transaction xact, BasePage undoPage, LogInstant CLRInstant, 
					   LimitObjectInput in)
		 throws StandardException
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(undoPage != null, "undo Page null");
			SanityManager.ASSERT(undoPage instanceof AllocPage, 
								 "undo Page is not an allocPage");
		}

		((AllocPage)undoPage).chainNextAllocPage(CLRInstant,
											 ContainerHandle.INVALID_PAGE_NUMBER, 
											 0 /* undefine */);
	}

	/*
	 * method to support BeforeImageLogging
	 */
	public void restoreMe(Transaction xact, BasePage undoPage, LogInstant CLRinstant, LimitObjectInput in)
	{
		// nobody should be calling this since there is no
		// BI_AllocPageOperation
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("cannot call restoreMe on BI_ChainAllocPageOperation");
	}


	/** debug */
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			String str = super.toString();
			str += " Chain new alloc page number " + newAllocPageNum + " at " +
				newAllocPageOffset + " to " + getPageId();
			return str;
		}
		else
			return null;
	}

}

