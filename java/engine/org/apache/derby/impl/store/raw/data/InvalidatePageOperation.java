/*

   Derby - Class org.apache.derby.impl.store.raw.data.InvalidatePageOperation

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.store.raw.data;

import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.store.raw.log.LogInstant;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import org.apache.derby.iapi.services.io.LimitObjectInput;

/**
	Represents invalidating a page due to deallocation.  
	This operation invalidates the page that is being deallocated, as opposed
	to deallocatePage that happens on the alloc page.

	<PRE>
	@derby.formatId	LOGOP_INVALIDATE_PAGE
		the formatId is written by FormatIdOutputStream when this object is
		written out by writeObject
	@derby.purpose	invalidate a page
	@derby.upgrade
	@derby.diskLayout
		PhysicalPageOperation the superclass
		OptionalData	none
	@derby.endFormat
	</PRE>
*/
public final class InvalidatePageOperation extends PhysicalPageOperation
{
    InvalidatePageOperation(BasePage page)
	{
		super(page);
	}

	/*
	 * Formatable methods
	 */

	// no-arg constructor, required by Formatable 
	public InvalidatePageOperation() { super(); }


	/*
	 * If this page can be reused in the same transaction (of if committed
	 * transaction needs to be undone, then we need the before image of the
	 * page.  Right now, the transaction that deallocate a page must commit
	 * before the page can be freed and reused, so we don't need to log the 
	 * before image of the page
	 */
	public void writeExternal(ObjectOutput out) throws IOException 
	{
		// RESOLVE: may need to write out the page BI, see comment above
		super.writeExternal(out);
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
	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.LOGOP_INVALIDATE_PAGE;
	}

	/*
	 * Loggable methods
	 */
	/**
		Mark the page as being invalidated

		@exception IOException Can be thrown by any of the methods of ObjectInput.
		@exception StandardException Standard Derby policy.

		@see org.apache.derby.iapi.store.raw.Loggable#doMe
	*/
	public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in)
		 throws StandardException, IOException 
	{
		this.page.setPageStatus(instant, BasePage.INVALID_PAGE);
	}

	/*
	 * PhysicalPageOperation
	 */

	/**
		Mark the page as being valid

		@exception StandardException Thrown by methods I call
		@exception IOException Thrown by methods I call

		@see PhysicalPageOperation#undoMe
	*/
	public void undoMe(Transaction xact, BasePage undoPage, LogInstant CLRInstant, 
					   LimitObjectInput in)
		 throws StandardException, IOException 
	{
		undoPage.setPageStatus(CLRInstant, BasePage.VALID_PAGE);
	}


	/*
	 * PageBasicOperation
	 */

	/**
	 * restore the before image of the page
	 *
	 * @exception StandardException Standard Derby Error Policy
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
			return super.toString() + "Invalidate Page - it has been deallocated";
		else
			return null;
	}

}
