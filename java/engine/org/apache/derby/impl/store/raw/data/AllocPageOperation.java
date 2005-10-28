/*

   Derby - Class org.apache.derby.impl.store.raw.data.AllocPageOperation

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
import org.apache.derby.iapi.store.raw.data.RawContainerHandle;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.log.LogInstant;

import org.apache.derby.iapi.services.io.CompressedNumber;

import java.io.OutputStream;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import org.apache.derby.iapi.services.io.LimitObjectInput;


// Allocation page operation - to allocate, deallocate or free a page
public final class AllocPageOperation extends PhysicalPageOperation
{

	protected long newPageNumber; // new page's number
	protected int doStatus;		// what the doMe operation should set the status to
	protected int undoStatus;	// what the undoMe operation should set the status to

	public AllocPageOperation(AllocPage allocPage, long pageNumber, int doStatus, int undoStatus)
		 throws StandardException
	{
		super(allocPage);

		newPageNumber = pageNumber;
		this.doStatus = doStatus;
		this.undoStatus = undoStatus;
	}
	
	/*
	 * Formatable methods
	 */

	// no-arg constructor, required by Formatable 
	public AllocPageOperation() { super(); }

	public void writeExternal(ObjectOutput out) throws IOException 
	{
		super.writeExternal(out);
		CompressedNumber.writeLong(out, newPageNumber);
		CompressedNumber.writeInt(out, doStatus);
		CompressedNumber.writeInt(out, undoStatus);
	}

	/**
	    @exception IOException error reading from log stream
		@exception ClassNotFoundException cannot read object from input
	*/
	public void readExternal(ObjectInput in)
		 throws IOException, ClassNotFoundException
	{
		super.readExternal(in);
		newPageNumber = CompressedNumber.readLong(in);
		doStatus = CompressedNumber.readInt(in);
		undoStatus = CompressedNumber.readInt(in);
	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.LOGOP_ALLOC_PAGE;
	}

	/*
	 * Loggable methods
	 */
	/** 
	    Allocate/deallocate/free this page number
		@exception StandardException container Handle is not active
	*/
	public final void doMe(Transaction tran, LogInstant instant, LimitObjectInput in) 
		 throws StandardException
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(this.page instanceof AllocPage);
		}

		((AllocPage)page).setPageStatus(instant, newPageNumber, doStatus);
	}

	/*
	 * Undoable methods
	 */

	/**
		Allocate/deallocate/free this page number.

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

		// RESOLVE: maybe a free page operation should not be undoable.
		// Who is going to free that page again?  If we don't undo it, it may
		// be problem if we ever free a page in the same transaction that we
		// deallocate it, in that case, if we don't rollback the free, then we
		// can't rollback the deallcoate.
		((AllocPage)undoPage).setPageStatus(CLRInstant, newPageNumber, undoStatus);
	}

	/*
	 * method to support BeforeImageLogging
	 */
	public void restoreMe(Transaction xact, BasePage undoPage, LogInstant CLRinstant, LimitObjectInput in)
	{
		// nobody should be calling this since there is no
		// BI_AllocPageOperation
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("cannot call restoreMe on BI_AllocPageOperation");
	}


	/** debug */
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			String str = super.toString();
			str += " Change page allocation status of " + newPageNumber + 
				" to " + doStatus + "(undo " + undoStatus + ")";
			return str;
		}
		else
			return null;
	}

}

