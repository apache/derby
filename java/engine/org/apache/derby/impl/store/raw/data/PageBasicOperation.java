/*

   Derby - Class org.apache.derby.impl.store.raw.data.PageBasicOperation

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

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Loggable;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RePreparable;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.PageKey;

import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.store.raw.data.RawContainerHandle;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.raw.RawStoreFactory;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.services.io.CompressedNumber;
import org.apache.derby.iapi.util.ByteArray;
import org.apache.derby.iapi.services.io.DynamicByteArrayOutputStream;
import org.apache.derby.iapi.services.property.PropertyUtil;

import java.io.InputStream;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import org.apache.derby.iapi.services.io.LimitObjectInput;

/**
    A PageBasicOperation changed the content of a page, this is the root class of all
	page oriented operation. Each PageBasicOperation record change(s)
	that apply to <B>one and only one page</B>.  The pageID that is changed
	must be recorded in the log operation - in other words, redo
	must be physical (or more correctly, in Gray's term, physiological, since
	changes are logical <B>within</B> a page).
	<BR>Undo can be logical, but the undo logic must be hidden in
	generateUndo. By the time a compensation operation is logged as a
	LogOperation, the page that needs roll back must be determined.

	<PRE>
	@format_id	no format id, an abstract class.
	@purpose	provide methods for logical undo
	@upgrade
	@disk_layout
		pageId(PageKey)			the page this operation applies to
		pageVersion(CompressedLong)	the page version this operation applied to
		OptionalData	none
	@end_format
	</PRE>

	@see Loggable
*/

public abstract class PageBasicOperation implements Loggable, RePreparable 
{


	/* page info this operation changed */
	private PageKey     pageId;
	private long        pageVersion;


	/* runtime page and data necessary to maintain it */
	transient protected BasePage	        page;
	transient protected RawContainerHandle	containerHdl;
	transient protected boolean	            foundHere;

	protected PageBasicOperation(BasePage page) 
    {
		if (SanityManager.DEBUG) 
        {
			SanityManager.ASSERT(
                page != null, 
                "cannot create page operation on a null page pointer");
		}

        // runtime info
		this.page = page;

        // info which will be logged.
		pageId      = page.getPageId();
		pageVersion = page.getPageVersion();
	}

	// no-arg constructor, required by Formatable
	public PageBasicOperation() 
    {
	}

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return "Page Operation: " + pageId.toString() +
				" pageVersion " + pageVersion + " : ";
		}
		else
			return null;
	}

	/*
	 * Formatable methods
	 */


	public void writeExternal(ObjectOutput out) throws IOException
	{
		pageId.writeExternal(out);
		CompressedNumber.writeLong(out, pageVersion);
	}

	public void readExternal(ObjectInput in) 
        throws IOException, ClassNotFoundException
	{
		pageId = PageKey.read(in);

		pageVersion = CompressedNumber.readLong(in);
	}

	/*
	 * Loggable methods
	 */

	/** Returns true if this op should be redone during recovery redo,
	    if so, get and latched the page.

		@exception StandardException Standard Cloudscape policy.
	 */
	public final boolean needsRedo(Transaction xact)
		 throws StandardException
	{
		if (findpage(xact) == null)	// committed dropped container
			return false;

		long pversion = page.getPageVersion();
		if (pversion == pageVersion)
			return true;

		releaseResource(xact);

		if (pversion > pageVersion)
			return false;
		else
			throw StandardException.newException(
                    SQLState.DATA_MISSING_LOG, pageId, 
                    new Long(pversion), 
                    new Long(pageVersion));
	}

	/** Release latched page and any other resources acquired during a previous
		findpage, safe to call multiple times.

		In this RawStore implementataion, resource is acquired by a log
		operation in one of two places
		<nl>
		<li> during runtime or recovery undo in PageOperation.generateUndo()
		<li> during recovery redo in PageBasicOperation.needsRedo()
		</nl>
	 */
	public void releaseResource(Transaction xact)
	{
		if (!foundHere)			// don't release anything not found by this
			return;

		if (page != null)
		{
			page.unlatch();
			page = null;
		}

		if (containerHdl != null)
		{
			containerHdl.close();
			containerHdl = null;
		}

		foundHere = false;
	}

	/**
		A page operation is a RAWSTORE log record
	*/
	public int group()
	{
		return(Loggable.RAWSTORE | Loggable.XA_NEEDLOCK);
	}

	/**
		the default for optional data is set to null.  If an operation has optional data,
		the operation need to prepare the optional data for this method.

		WARNING: If a log operation extends this class, and the operation has optional data,
		it MUST overwrite this method to return a ByteArray that contains the optional data. 

		@exception StandardException Standard Cloudscape policy.
	*/
	public ByteArray getPreparedLog() throws StandardException
	{
		return (ByteArray) null;
	}

    /**************************************************************************
     * Public Methods of RePreparable Interface:
     **************************************************************************
     */

    /**
     * reclaim locks associated with the changes in this log record.
     * <p>
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void reclaimPrepareLocks(
    Transaction     t,
    LockingPolicy   locking_policy)
		throws StandardException
    {
		if (SanityManager.DEBUG)
			SanityManager.DEBUG_PRINT("", "PageBasicOperation.reclaimPrepareLocks().");
    }

	/*
	 *	Methods specific to this class
	 */
		 
	/**
		Reset the pageNumber
	*/
	protected final void resetPageNumber(long pageNumber)
	{
		pageId = new PageKey(pageId.getContainerId(), pageNumber);
	}

	protected final PageKey getPageId() {
		return pageId;
	}

	/** Find the page the operation applies to and latch it, this only
	    uses the segmentId, containerId, and pageId stored in this log
		record to find the page.

		@return null if container is dropped and committed (possibly
		stubbified), else return the latched page

		@exception StandardException Standard Cloudscape policy.
	 */
	public final BasePage findpage(Transaction xact) throws StandardException 
	{
		releaseResource(xact);

		RawTransaction rtran = (RawTransaction)xact;
		containerHdl = rtran.openDroppedContainer(pageId.getContainerId(),
			(LockingPolicy) null);

		if (containerHdl == null)
        {
			throw StandardException.newException(
                    SQLState.DATA_CONTAINER_VANISHED, pageId.getContainerId());
        }

		foundHere = true;

		// if container is dropped and committed, cannot look at any page, 
        // it may be a container stub
		if (containerHdl.getContainerStatus() == RawContainerHandle.COMMITTED_DROP)
		{
			releaseResource(xact);
			return null;
		}

		StandardException getPageException = null;
		try
		{
			// get and latch page - we don't know the status of the page or what
			// kind of page we are looking for, get any type of page
			page = (BasePage)(containerHdl.getAnyPage(pageId.getPageNumber()));
		}
		catch (StandardException se)
		{
			getPageException = se;
		}
			
		//Try to initialize the page if page not found exception occurs during
		//recovery and the page version is zero(Init Page).
		//We do this if derby.storage.patchInitPageRecoverError is set.
		if (page == null && getPageException != null && pageVersion == 0)
			if (PropertyUtil.getSystemBoolean(RawStoreFactory.PATCH_INITPAGE_RECOVER_ERROR))
				page = getPageForLoadTran(xact);
		
		// maybe we are in rollforward recovery and this is an init page operation,
		// give subclass a chance to create the page
		if (page == null && getPageException != null)
		{
			//if are rolloforward recovery reload the page using load tran methods
			//that initialize the page. because in rollforward recovery, we 
			//might be actually recreating the page container did not exist 
			//in the backup when we started the rollforward recovery.

			if (rtran.inRollForwardRecovery())
			{
				if (SanityManager.DEBUG) 
					if(SanityManager.DEBUG_ON("LoadTran"))
						SanityManager.DEBUG_PRINT(
											  "Trace", "got null page " + pageId + 
											  " and getPageException, now attempt last ditch effort");

				page = getPageForLoadTran(xact);
				
				if (SanityManager.DEBUG) 
					if(SanityManager.DEBUG_ON("LoadTran"))
						SanityManager.DEBUG_PRINT(
											  "Trace"," getPageForLoadTran, got page=" + 
											  (page != null));
			}	
		}

		if (page == null)
		{
			if (getPageException != null)
            {
				throw getPageException;	// that is the original error
            }
			else
            {
				throw StandardException.newException(
                        SQLState.DATA_MISSING_PAGE, pageId);
            }
		}

		return page;
	}

	/**
		Subclass (e.g., init page) that wishes to do something about missing
		pages in load tran should override this method to return the page

		@exception StandardException Cloudscape Standard error policy
	 */
	protected BasePage getPageForLoadTran(Transaction xact)
		 throws StandardException
	{
		return null;
	}

	public final Page getPage() {
		return page;
	}

	public final long getPageVersion() {
		return pageVersion;
	}


	/**
		Undo the change indicated by this log operation and optional data.
		The page the undo should apply to is the latched undoPage.
		The undoPage must be the same page as the doMe page and the undo
		operation must restore the before image of the row that changed.  

		<BR> this can only be used under special circumstances: namely
		table level locking, and no internal or nested transaction, and all
		operations are rollec back with restoreMe instead of undoMe.

		<BR><B>This method is here to support BeforeImageLogging</B>

		@param xact			the Transaction doing the rollback
		@param undoPage		the page to rollback changes on
		@param undoRecordId	the recordId to rollback changes on
		@param CLRinstant	the log instant of this (PageUndo) operation
		@param in			optional data for the rollback operation

		@exception IOException Can be thrown by any of the methods of ObjectInput.
		@exception StandardException Standard Cloudscape policy.		
	*/
	abstract public void restoreMe(Transaction xact, BasePage undoPage,
								LogInstant CLRinstant, LimitObjectInput in) 
		 throws StandardException, IOException;


}
