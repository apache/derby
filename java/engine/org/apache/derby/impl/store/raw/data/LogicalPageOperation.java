/*

   Derby - Class org.apache.derby.impl.store.raw.data.LogicalPageOperation

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

import org.apache.derby.impl.store.raw.data.RecordId;
import org.apache.derby.impl.store.raw.data.BasePage;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.Compensation;
import org.apache.derby.iapi.store.raw.LogicalUndoable;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.Undoable;

import org.apache.derby.iapi.store.raw.data.RawContainerHandle;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.store.raw.log.LogInstant;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.services.io.CompressedNumber;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import org.apache.derby.iapi.services.io.LimitObjectInput;

/**
	An abstract class that is used for logical log operation.  A logical log
	operation is one where the undo of the operation may be applied to a
	different page than the original operation.

	<PRE>
	@format_id	no format id, an abstract class.
	@purpose	provide methods for logical undo
	@upgrade
	@disk_layout
		PageBasicOperation	the super class
		recordId(CompressedInt)	the recordId this operation affects
		undo(LogicalUndo)		the piece of code that can figure out which page 
								the row has moved into
		OptionalData	none
	@end_format
	</PRE>

*/
public abstract class LogicalPageOperation 
    extends PageBasicOperation implements LogicalUndoable
{

	protected LogicalUndo undo; // Callback to access for logical undo.
								// If non-null, then logical undo is necessary
								// for this operation.  If null, then the
								// operation really only needs physical undo

	protected int			recordId;	// record id - this is what the
										// recordId is during the doMe time, it
										// may have been changed now since the
										// record may move to another page.

	// no-arg constructor, required by Formatable 
	public LogicalPageOperation() { super(); }

	protected LogicalPageOperation(BasePage page, LogicalUndo undo, int recordId)
	{
		super(page);
		this.undo = undo;
		this.recordId = recordId;
	}

	/*
	 * Formatable methods
	 */


	public void writeExternal(ObjectOutput out) throws IOException 
	{
		super.writeExternal(out);
		CompressedNumber.writeInt(out, recordId);
		out.writeObject(undo);
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
		recordId = CompressedNumber.readInt(in);
		undo = (LogicalUndo)in.readObject();
	}


	/**
		Undoable method
	*/

	/** 
		Generate a Compensation (PageUndoOperation) that will rollback the
		changes of this page operation. If this Page operation cannot or need not
		be rolled back (redo only), overwrite this function to return null.

		@see LogicalUndo
		@see #findLogicalPage
		@exception StandardException Standard Cloudscape policy.
		@exception IOException Method may read from ObjectInput
	*/

	public Compensation generateUndo(Transaction xact, LimitObjectInput in)
		 throws StandardException, IOException
	{
		// if logical undo is not necessary, use normal physical undo 
		if (undo == null)
		{
			BasePage undoPage = findpage(xact);

			// Needs to pre-dirty this page so that if a checkpoint is taken
			// any time after the CLR is sent to the log stream, it will wait
			// for the actual undo to happen on the page.  We need this to
			// preserve the integrity of the redoLWM.
			undoPage.preDirty();

			return new LogicalUndoOperation(undoPage, recordId, this);
		}
		else
		{
			if (SanityManager.DEBUG)
			{
				// Sanity check to make sure logical undo is not called inside
				// internal transaction
				RawTransaction rtran = (RawTransaction)xact;
				rtran.checkLogicalOperationOk();
			}

			BasePage logicalUndoPage = findLogicalPage(xact, undo, in);

			// Needs to pre-dirty this page so that if a checkpoint is taken
			// any time after the CLR is sent to the log stream, it will wait
			// for the actual undo to happen on the page.  We need this to
			// preserve the integrity of the redoLWM.
			logicalUndoPage.preDirty();

			// find logical page is going to call undo.findUndo to find the
			// right page to apply the CLR to.  If the record has changed,
			// logicalUndo should have resetRecordHandle to reset the page
			// number and the recordId to the new record location.  We need to
			// store both of these in the clr since during recovery redo,
			// undo.findUndo is not called.
			return  new LogicalUndoOperation(logicalUndoPage, recordId, this);

		}
	}


	/*
	 * LogicalUndoable methods
	 * These methods are called by undo.findUndo to extract information out of
	 * the log record for the purpose of logical undo.
	 */

	/**
		Return the container handle where the log operated on
	*/
	public ContainerHandle getContainer()
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(containerHdl != null, "accessing null container handle");
		}

		return containerHdl;
	}

	/**
		After the logical undo logic figures out where the real record that
		needs roll back is, reset this log operation to refer to that record
	*/
	public void resetRecordHandle(RecordHandle rh)
	{
		resetPageNumber(rh.getPageNumber());
		recordId = rh.getId();
	}


	/**
		Return the record handle that correspond to the record that was changed
		during roll forward.  This is used as a hint by logical undo as a good
		place to look for the record to apply the roll back.
	*/
	public RecordHandle getRecordHandle()
	{
		return new RecordId(getPageId(), recordId);
	}

    /**************************************************************************
     * Public Methods of RePreparable Interface:
     **************************************************************************
     */

    /**
     * reclaim locks associated with the changes in this log record.
     * <p>
	 * @param locking_policy  The locking policy to use to claim the locks.
     * 
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void reclaimPrepareLocks(
    Transaction     t,
    LockingPolicy   locking_policy)
		throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.DEBUG_PRINT("", "reclaimPrepareLocks().");
            SanityManager.ASSERT(getRecordHandle() != null);
        }

        ContainerHandle ch = t.openContainer(
            getPageId().getContainerId(), locking_policy, 
            (ContainerHandle.MODE_FORUPDATE          | 
             ContainerHandle.MODE_OPEN_FOR_LOCK_ONLY | 
             ContainerHandle.MODE_LOCK_NOWAIT));

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(ch != null);
        }

        if (ch != null)
            ch.close();

        /*
        // get the intent lock on the container.
        boolean lock_granted = 
            locking_policy.lockContainer(
                t, 
                getContainer(), 
                false,          // don't wait for the lock, it is bug if a 
                                // lock has to wait while reclaiming locks 
                                // during recovery.
                true);          // lock for update.

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(lock_granted);
        }
        
        */
        // get the row lock on the c.
        boolean lock_granted = 
            locking_policy.lockRecordForWrite(
                t, 
                getRecordHandle(), 
                false,          // default is not for insert. 
                false);         // don't wait for the lock, it is bug if a 
                                // lock has to wait while reclaiming locks 
                                // during recovery.

        releaseResource(t);

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(lock_granted);
        }
    }

	/*
	 * method specific to this class
	 */

	/** 
	  Find the page that the rollback operation should be applied to.

	  <P>The actual logical log operation is expected to implement
	  Undoable.generateUndo.  This utility function findLogicalPage is provided
	  for the common case scenario of using a LogicalUndo interface to find the
	  undo page.  The subclass that implements Undoable.generateUndo can use
	  this function to find the logical page with its LogicalUndo callback function.
	  This method can be used with the default releaseResource().

	  <P>During recovery redo, the logging system is page oriented and will use
	  the pageID stored in the PageUndoOperation to find the page.  The
	  page will be latched and released using the default findpage and
	  releaseResource - this.releaseResource() will still be called so it has
	  to know not to release any resource it did not acquire.

	  @param xact	the transaction doing the compensating
	  @param in		optional input

	  @return the compensation operation that will rollback this change 

		@exception StandardException Standard Cloudscape error policy
		@exception IOException Method may read from ObjectInput

	  @see PageBasicOperation
	  @see Undoable#generateUndo
	  @see  org.apache.derby.iapi.store.raw.Loggable#releaseResource
	

    */
	private BasePage findLogicalPage(Transaction xact, LogicalUndo undo,
									   LimitObjectInput in)
		 throws StandardException, IOException
	{
		releaseResource(xact);

		if (SanityManager.DEBUG) {
			// the try,finally code makes these assumptions.
			SanityManager.ASSERT(containerHdl == null);
			SanityManager.ASSERT(page == null);
		}

		boolean okExit = false;

		try {

			// open the container
			RawTransaction rtran = (RawTransaction)xact;

			containerHdl = rtran.openDroppedContainer
				(getPageId().getContainerId(), (LockingPolicy) null);

			if (SanityManager.DEBUG) {
				SanityManager.ASSERT(containerHdl != null, "cannot open container");
				SanityManager.ASSERT(containerHdl.getContainerStatus() != RawContainerHandle.COMMITTED_DROP,
								 "finding a page for undo in a committed dropped container");
			}

			page = (BasePage)(undo.findUndo(xact, this, in));

			if (SanityManager.DEBUG) {
				SanityManager.ASSERT(page != null, "findUndo returns null page");
				SanityManager.ASSERT(page.getPageNumber() == getPageId().getPageNumber(),
								"undo.findUndo did not reset the log op's recordHandle");
			}

			// if you add code here then ensure that you handle page unlatching in the
			// backout code.

			okExit = true;
		} finally {

			if (!okExit) {

				if (containerHdl != null) {
					containerHdl.close();
					containerHdl = null;
				}
			}

			// no need to unlatch page here because is page is valid no
			// exceptions can be thrown, until some adds code after the findUndo.
		}

		foundHere = true;
		return page;
	}


	/**
		Undo the change indicated by this log operation and optional data.
		The undoPage and undoRecordId is the page, record the undo should apply to.
		The undoRecorId differs from the roll forward recordId if the undoPage
		differs from the page the roll forward operation was applied to, in
		other words, the record moved to another page and the recordId changed.

		<BR>A logical operation can at most deal with 1 record.

		<P> The available() method of in indicates how much data can be read, i.e.
		how much was originally written.

		<BR><B>In this RawStore implementation, should only only be called via
		CompOp.doMe</B>.


		@param xact			the Transaction doing the rollback
		@param undoPage		the page to rollback changes on
		@param undoRecordId	the recordId to rollback changes on
		@param CLRinstant	the log instant of this (PageUndo) operation
		@param in			optional data for the rollback operation

		@exception IOException Can be thrown by any of the methods of ObjectInput.
		@exception StandardException Standard Cloudscape policy.		
	*/
	abstract public void undoMe(Transaction xact, BasePage undoPage, int undoRecordId,
								LogInstant CLRinstant, LimitObjectInput in) 
		 throws StandardException, IOException;


}
