/*

   Derby - Class org.apache.derby.impl.store.raw.data.PhysicalPageOperation

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

import org.apache.derby.iapi.store.raw.Compensation;

import org.apache.derby.iapi.store.raw.log.LogInstant;

import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.Undoable;

import org.apache.derby.iapi.error.StandardException;

import java.io.InputStream;
import java.io.ObjectInput;
import java.io.IOException;
import org.apache.derby.iapi.services.io.LimitObjectInput;

/**
	An abstract class that is used for physical log operation.  A physical log
	operation is one where the undo of the operation must be applied to the
	same page as the original operation, and the undo operation must store the
	byte image of the row(s) changed to its before image.  (If a logical page
	operation happened to the page or if another transaction altered other rows
	on the page, the undo of this operation will only restore the before image
	of the row(s) affected).

	<PRE>
	@format_id	no format id, an abstract class.
	@purpose	provide methods for physical undo
	@upgrade
	@disk_layout
		PageBasicOperation	the super class
	@end_format
	</PRE>
*/

public abstract class PhysicalPageOperation extends PageBasicOperation implements Undoable
{
	protected PhysicalPageOperation(BasePage page)
	{
		super(page);
	}

	/*
	 * Formatable methods
	 */

	// no-arg constructor, required by Formatable 
	public PhysicalPageOperation() { super(); }

	// no fields, therefore no writeExternal or readExternal

	/**
		Undoable method
	*/

	/** 
	  Generate a Compensation (PageUndoOperation) that will rollback the
	  changes of this page operation. If this Page operation cannot or need not
	  be rolled back (redo only), overwrite this function to return null.

	  <P><B>Note</B><BR> For operation that needs logical undo, use
	  LogicalUndoOperation instead</B>  This implementation just finds
	  the same page that the PageOperation was applied on - i.e., only works
	  for undo on the same page.  

	  <P>During recovery redo, the logging system is page oriented and will use
	  the pageID stored in the PageUndoOperation to find the page.  The
	  page will be latched and released using the default findpage and
	  releaseResource - this.releaseResource() will still be called so it has
	  to know not to release any resource it did not acquire.

	  @param xact	the transaction doing the compensating
	  @param in		optional input
	  @param dataLength optional input length

	  @return the compensation operation that will rollback this change 

	  @exception StandardException Standard Cloudscape policy.

	  @see PageBasicOperation
	  @see Undoable#generateUndo
	  
    */
	public Compensation generateUndo(Transaction xact, LimitObjectInput in)
		 throws StandardException
	{
		// findpage will have the page latched.
		// CompensationOperation.doMe must call this.releaseResource the page
		// when it is done 
		BasePage undoPage = findpage(xact);

		// Needs to pre-dirty this page so that if a checkpoint is taken any
		// time after the CLR is sent to the log stream, it will wait for the
		// actual undo to happen on the page.  We need this to preserve the
		// integrity of the redoLWM.
		undoPage.preDirty();

		return new PhysicalUndoOperation(undoPage, this);
	}


	/**
		Undo the change indicated by this log operation and optional data.
		The page the undo should apply to is the latched undoPage, the
		recordId is the same as the roll forward operation.  
		
		<BR><B>In this RawStore implementation, should only only be called via
		CompOp.doMe</B>.

		<P> The available() method of in indicates how much data can be read, i.e.
		how much was originally written.

		@param xact			the Transaction doing the rollback
		@param undoPage		the page to rollback changes on
		@param CLRinstant	the log instant of this (PageUndo) operation
		@param in			optional data for the rollback operation

		@exception IOException Can be thrown by any of the methods of ObjectInput.
		@exception StandardException Standard Cloudscape policy.		
	*/
	abstract public void undoMe(Transaction xact, BasePage undoPage,
								   LogInstant CLRinstant, LimitObjectInput in) 
		 throws StandardException, IOException;


}

