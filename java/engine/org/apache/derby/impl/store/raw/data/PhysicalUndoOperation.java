/*

   Derby - Class org.apache.derby.impl.store.raw.data.PhysicalUndoOperation

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

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.store.raw.Compensation;
import org.apache.derby.iapi.store.raw.Loggable;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.Undoable;

import org.apache.derby.iapi.store.raw.log.LogInstant;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.util.ByteArray;

import java.io.OutputStream;
import java.io.ObjectInput;
import java.io.IOException;
import org.apache.derby.iapi.services.io.LimitObjectInput;


/**
	PhysicalUndoOperation is a compensation operation that rolls back the change of
	an Undo-able operation.  A PhysicalUndoOperation itself is not undo-able, i.e,
	it is loggable but not undoable.

	<PRE>
	@format_id	LOGOP_PAGE_PHYSICAL_UNDO
		the formatId is written by FormatIdOutputStream when this object is
		written out by writeObject
	@purpose	update a physiacl log operation
	@upgrade
	@disk_layout
		PageBasicOperation	the super class
		OptionalData	none (compensation operation never have optional data)
	@end_format
	</PRE>

*/
public class PhysicalUndoOperation extends PageBasicOperation implements Compensation {

	/** The operation to be rolled back */
	transient private	PhysicalPageOperation undoOp; 

	protected PhysicalUndoOperation(BasePage page)
	{
		super(page);
	}

	/** Set up a compensation operation during run time rollback */
	public PhysicalUndoOperation(BasePage page, PhysicalPageOperation op)
	{
		super(page);
		undoOp = op;
	}

	/**
		Return my format identifier.
	*/

	// no-arg constructor, required by Formatable 
	public PhysicalUndoOperation() { super(); }

	public int getTypeFormatId() {
		return StoredFormatIds.LOGOP_PAGE_PHYSICAL_UNDO;
	}

	// no fields, therefore no writeExternal or readExternal

	/** 
		Compensation methods
	*/

	/** Set up a PageUndoOperation during recovery redo. */
	public void setUndoOp(Undoable op)
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(op instanceof PhysicalPageOperation);
		}
		undoOp = (PhysicalPageOperation)op;
	}


	/**
		Loggable methods
	*/

	/** Apply the undo operation, in this implementation of the
		RawStore, it can only call the undoMe method of undoOp

		@param xact			the Transaction that is doing the rollback
		@param instant		the log instant of this undo operation
		@param in			optional data
		@param dataLengt	optional data length

		@exception IOException Can be thrown by any of the methods of InputStream.
		@exception StandardException Standard Cloudscape policy.

	 */
	public final void doMe(Transaction xact, LogInstant instant, LimitObjectInput in) 
		 throws StandardException, IOException
	{

		long oldversion = 0;		// sanity check
		LogInstant oldLogInstant = null; // sanity check
		if (SanityManager.DEBUG)
		{
			oldLogInstant = this.page.getLastLogInstant();
			oldversion = this.page.getPageVersion();

			SanityManager.ASSERT(oldversion == this.getPageVersion());
			SanityManager.ASSERT(oldLogInstant == null || instant == null 
							 || oldLogInstant.lessThan(instant));
		}

		// if this is called during runtime rollback, PageOp.generateUndo found
		// the page and have it latched there.
		// if this is called during recovery redo, this.needsRedo found the page and
		// have it latched here
		//
		// in either case, this.page is the correct page and is latched.
		//
		undoOp.undoMe(xact, this.page, instant, in);

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(oldversion < this.page.getPageVersion());
			if (instant != null &&
				! instant.equals(this.page.getLastLogInstant()))
				SanityManager.THROWASSERT(
								 "my class name is " + getClass().getName() +
								 " undoOp is " + undoOp.getClass().getName() );
		}

		releaseResource(xact);
	}

	/* make sure resource found in undoOp is released */
	public void releaseResource(Transaction xact)
	{
		if (undoOp != null)
			undoOp.releaseResource(xact);
		super.releaseResource(xact);
	}

	/* Undo operation is a COMPENSATION log operation */
	public int group()
	{
		return super.group() | Loggable.COMPENSATION | Loggable.RAWSTORE;
	}

	public final ByteArray getPreparedLog() {
		// should never ever write optional data because this implementation of
		// the recovery system  will never read this and pass this on to dome.
		// Instead, the optional data of the undoOp will be used - since
		// this.doMe can only call undoOP.undoMe, this has no use for any
		// optional data.
		return (ByteArray) null;
	}

	public void restoreMe(Transaction xact, BasePage undoPage,
						  LogInstant CLRinstant, LimitObjectInput in)
	{
		// Not undoable
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("cannot call restore me on PhysicalUndoOperation");
	}

	/**
	  DEBUG: Print self.
	*/
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			String str = "CLR (Physical Undo): " + super.toString();
			if (undoOp != null)
				str += "\n" + undoOp.toString();
			else
				str += "undo Operation not set";

			return str;
		}
		else
			return null;
	}

}
