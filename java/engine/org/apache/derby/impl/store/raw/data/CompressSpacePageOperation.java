/*

   Derby - Class org.apache.derby.impl.store.raw.data.ChainAllocPageOperation

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


/**

Log operation to implement compressing space from a container and returning
it to the operating system.

**/

public class CompressSpacePageOperation extends PhysicalPageOperation
{
    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */

    /**
     * The new highest page on this allocation page.  The number is the
     * offset of the page in the array of pages maintained by this 
     * allocation page, for instance a value of 0 indicates all page except
     * the first one are to be truncated.  If all pages are truncated then 
     * the offset is set to -1.
     **/
	protected int newHighestPage;	    

    /**
     * The number of allocated pages in this allocation page prior to 
     * the truncate.  Note that all pages from NewHighestPage+1 through
     * newHighestPage+num_pages_truncated should be FREE.
     **/
	protected int num_pages_truncated; 

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */
	public CompressSpacePageOperation(
    AllocPage   allocPage, 
    int         highest_page, 
    int         num_truncated)
		 throws StandardException
	{
		super(allocPage);

        newHighestPage      = highest_page;
        num_pages_truncated = num_truncated;
	}
	
    /**************************************************************************
     * Public Methods of Formatable interface.
     **************************************************************************
     */

	// no-arg constructor, required by Formatable 
	public CompressSpacePageOperation() { super(); }

	public void writeExternal(ObjectOutput out) throws IOException 
	{
		super.writeExternal(out);
		if( !(this instanceof CompressSpacePageOperation10_2) )
		{
			out.writeInt(newHighestPage);
			CompressedNumber.writeInt(out, num_pages_truncated);
		}
	}

	/**
		@exception IOException error reading from log stream
		@exception ClassNotFoundException cannot read object from input
	*/
	public void readExternal(ObjectInput in)
		 throws IOException, ClassNotFoundException
	{
		super.readExternal(in);
		if( !(this instanceof CompressSpacePageOperation10_2) )
		{
			newHighestPage      = in.readInt();
			num_pages_truncated = CompressedNumber.readInt(in);
		}
	}

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.LOGOP_COMPRESS_SPACE;
	}

    /**************************************************************************
     * Public Methods of Loggable interface.
     **************************************************************************
     */

    /**
     * Compress space from container.
     * <p>
     * Compress the indicate space from the container, returning the free
     * pages to the OS.  Update the allocation page to reflect the file
     * change.
     *
     * @param tran      transaction doing the operation.
     * @param instant   log instant for this operation.
     * @param in        unused by this log operation.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public final void doMe(
    Transaction         tran, 
    LogInstant          instant, 
    LimitObjectInput    in) 
		 throws StandardException
	{
		if (SanityManager.DEBUG) 
        {
			SanityManager.ASSERT(this.page instanceof AllocPage);
		}

		((AllocPage)page).compressSpace(
             instant, newHighestPage, num_pages_truncated);
	}

    /**************************************************************************
     * Public Methods of Undoable interface.
     **************************************************************************
     */

    /**
     * Compress space undo.
     * <p>
     *
	 * @exception StandardException Thrown by methods I call 
     * @see PhysicalPageOperation#undoMe
     **/
	public void undoMe(
    Transaction         xact, 
    BasePage            undoPage, 
    LogInstant          CLRInstant, 
    LimitObjectInput    in)
		 throws StandardException
	{
		if (SanityManager.DEBUG) 
        {
			SanityManager.ASSERT(undoPage != null, "undo Page null");
			SanityManager.ASSERT(
                undoPage instanceof AllocPage, 
				"undo Page is not an allocPage");
		}

		((AllocPage)undoPage).undoCompressSpace(
             CLRInstant, newHighestPage, num_pages_truncated);
	}

	/*
	 * method to support BeforeImageLogging
	 */
	public void restoreMe(
    Transaction         xact, 
    BasePage            undoPage, 
    LogInstant          CLRinstant, 
    LimitObjectInput    in)
	{
		// nobody should be calling this since there is no corresponding 
        // BI operation.
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT(
                "cannot call restoreMe on CompressSpaceOperation.");
	}


	/** debug */
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			String str = super.toString();
			str += " CompressSpaceOperation: " + 
                "newHighestPage = " + newHighestPage +
                ";num_pages_truncated = " + num_pages_truncated +
				" to " + getPageId();

			return str;
		}
		else
			return null;
	}
}
