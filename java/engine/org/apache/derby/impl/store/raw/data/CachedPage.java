/*

   Derby - Class org.apache.derby.impl.store.raw.data.CachedPage

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

import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.PageKey;

import org.apache.derby.iapi.services.cache.Cacheable;
import org.apache.derby.iapi.services.cache.CacheManager;
import org.apache.derby.iapi.services.context.ContextService;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.error.ExceptionSeverity;
import java.io.IOException;

/**
	A base page that is cached.

	Since there are multiple page formats, use this abstract class to implement
	cacheable interface.


*/

public abstract class CachedPage extends BasePage implements Cacheable
{
	protected boolean		alreadyReadPage;	// set to true when the page was read by another class

	protected byte[]		pageData;		// the actual page data - this is
											// the 'buffer' in the buffer cache

	// The isDirty flag indicates if the pageData or pageHeader has been
	// modified.  The preDirty flag indicates that the pageData or the
	// pageHeader is about to be modified.  The reason for these 2 flags
	// instead of just one is to accomodate checkpoint.  After a clean
	// (latched) page sends a log record to the log stream but before that page
	// is dirtied by the log operation, a checkpoint could be taken.  If so,
	// then the redoLWM will be after the log record but, without preDirty, the
	// cache cleaning will not have waited for the change.  So the preDirty bit
	// is to stop the cache cleaning from skipping over this (latched) page
	// even though it has not really been modified yet.  

	protected boolean		isDirty;		// must be set to true
								// whenever the pageData array is touched directly
								// or indirectly.

	protected boolean		preDirty;		// set to true if the page is clean
								// and the pageData array is about to be
								// touched directly or indirectly.


	protected int		initialRowCount; // keep a running count of rows for
										 // estimated row count.

	private long 		containerRowCount;	// the number of rows in the
											// container when this page is read
											// from disk 

	/*
	** These fields are immutable and can be used by the subclasses directly.
	*/

	/**
		The page cache I live in.

		<BR> MT - Immutable
	*/
	protected CacheManager		pageCache;

	/**
		The container cache my container lives in.

		<BR> MT - Immutable
	*/
	protected CacheManager		containerCache;

	/**
		My factory class.

		<BR> MT - Immutable - 
	*/
	protected BaseDataFileFactory		dataFactory;  // my factory class.


	protected static final int PAGE_FORMAT_ID_SIZE = 4;

	/*
	 * the page need to be written and synced to disk 
	 */
	public static final int WRITE_SYNC = 1;

	/*
	 * the page need to be write to disk but not synced
	 */
	public static final int WRITE_NO_SYNC = 2;

	public CachedPage()
	{
		super();
	}

	public final void setFactory(BaseDataFileFactory factory) {
		dataFactory = factory;
		pageCache = factory.getPageCache();
		containerCache = factory.getContainerCache();
	}

	/**
		Initialize the object, ie. perform work normally perfomed in constructor.
		Called by setIdentity() and createIdentity().
	*/
	protected void initialize()
	{
		super.initialize();
		isDirty = false;
		preDirty = false;
		initialRowCount = 0;
		containerRowCount = 0;
	}

	/*
	** Methods of Cacheable
	*/

	/**
		Find the container and then read the page from that container.

		@return always true, higher levels have already checked the page number is
		valid for an open.

		@exception StandardException Standard Cloudscape policy.

		@see Cacheable#setIdentity
	*/
	public Cacheable setIdentity(Object key) throws StandardException {

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(key instanceof PageKey);
		}

		initialize();

		PageKey newIdentity = (PageKey) key;

		FileContainer myContainer = 
				(FileContainer) containerCache.find(newIdentity.getContainerId());
		setContainerRowCount(myContainer.getEstimatedRowCount(0));

		try
		{
			if (!alreadyReadPage)
				readPage(myContainer, newIdentity);	// read in the pageData array from disk
			else
				alreadyReadPage = false;

			// if the formatID on disk is not the same as this page instance's
			// format id, instantiate the real page object
			int fmtId = getTypeFormatId();

			int onPageFormatId = FormatIdUtil.readFormatIdInteger(pageData);
			if (fmtId != onPageFormatId)
			{
				return changeInstanceTo(onPageFormatId, newIdentity).setIdentity(key);
			}

			// this is the correct page instance
			initFromData(myContainer, newIdentity);
		}
		finally
		{
			containerCache.release(myContainer);
			myContainer = null;
		}

		fillInIdentity(newIdentity);

		initialRowCount = 0;

		return this;
	}

	/**
		Find the container and then create the page in that container.

		@return new page, higher levels have already checked the page number is
		valid for an open.

		@exception StandardException Standard Cloudscape policy.

		@see Cacheable#createIdentity
	*/
	public Cacheable createIdentity(Object key, Object createParameter) throws StandardException {

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(key instanceof PageKey);
		}

		initialize();

		PageKey newIdentity = (PageKey) key;

		int[] createArgs = (int[]) createParameter;

		if (createArgs[0] == -1)
        {
			throw StandardException.newException(
                    SQLState.DATA_UNKNOWN_PAGE_FORMAT, newIdentity);
        }

		// createArgs[0] contains the interger form of the formatId 
		// if it is not the same as this instance's formatId, instantiate the
		// real page object
		if (createArgs[0] != getTypeFormatId())
		{
			return changeInstanceTo(createArgs[0], newIdentity).createIdentity(key, createParameter);
		}
		
		// this is the correct page instance
		initializeHeaders(5);
		createPage(newIdentity, createArgs);

		fillInIdentity(newIdentity);

		initialRowCount = 0;

		/*
		 * if we need to grow the container and the page has not been
		 * preallocated , writing page before the log is written so that we
		 * know if there is an IO error - like running out of disk space - then
		 * we don't write out the log record, because if we do, it may fail
		 * after the log goes to disk and then the database may not be
		 * recoverable. 
		 *
		 * WRITE_SYNC is used when we create the page without first
		 *	preallocating it 
		 * WRITE_NO_SYNC is used when we are preallocating the page - there
		 *	will be a SYNC call after all the pages are preallocated
		 * 0 means creating a page that has already been preallocated.
		 */
		if ((createArgs[1] & WRITE_SYNC) != 0 ||
			(createArgs[1] & WRITE_NO_SYNC) != 0)
			writePage(newIdentity, (createArgs[1] & WRITE_SYNC) != 0);

		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON(FileContainer.SPACE_TRACE))
			{
				String syncFlag = ((createArgs[1] & WRITE_SYNC) != 0) ? "Write_Sync" :
					(((createArgs[1] & WRITE_NO_SYNC) != 0) ? "Write_NO_Sync" : 
					 "No_write");
				SanityManager.DEBUG(FileContainer.SPACE_TRACE,
									"creating new page " + newIdentity + 
									" with " + syncFlag);
			}
		}

		return this;
	}

	/*
	 * this object is instantiated to the wrong subtype of cachedPage, 
	 * this routine will create an object with the correct subtype, transfer all 
	 * pertinent information from this to the new correct object.
	 */
	private CachedPage changeInstanceTo(int fid, PageKey newIdentity)
		 throws StandardException
	{
		CachedPage realPage;
		try 
        {
			realPage = 
                (CachedPage) Monitor.newInstanceFromIdentifier(fid);

		} 
        catch (StandardException se) 
        {
            if (se.getSeverity() > ExceptionSeverity.STATEMENT_SEVERITY)
            {
                throw se;
            }
            else
            {
                throw StandardException.newException(
                    SQLState.DATA_UNKNOWN_PAGE_FORMAT, se, newIdentity);
            }
		}

		realPage.setFactory(dataFactory);

		// avoid creating the data buffer if possible, transfer it to the new 
        // page if this is the first time the page buffer is used, then 
        // createPage will create the page array with the correct page size
		if (this.pageData != null) {
			realPage.alreadyReadPage = true;
			realPage.usePageBuffer(this.pageData);
		}

		// this page should not be used any more, null out all its content and
		// wait for GC to clean it up  

		//destroyPage();	// let this subtype have a chance to get rid of stuff
		//this.pageData = null;	// this instance no longer own the data array
		//this.pageCache = null;
		//this.dataFactory = null;
		//this.containerCache = null;

		return realPage;
	}

	/**
		Has the page or its header been modified or about to be modified.
		See comment on class header on meaning of isDirty and preDirty bits.

		@see Cacheable#isDirty
	*/
	public boolean isDirty() {

		synchronized (this) {
			return isDirty || preDirty;
		}
	}

	/**
		Has the page or its header been modified.
		See comment on class header on meaning of isDirty and preDirty bits.
	*/
	public boolean isActuallyDirty() {

		synchronized (this) {
			return isDirty;
		}
		
	}

	/**
		The page or its header is about to be modified.
		See comment on class header on meaning of isDirty and preDirty bits.
	*/
	public void preDirty()
	{
		synchronized (this) {
			if (!isDirty)
				preDirty = true;
		}
	}

	/**
		Ensure that container row count is updated if it is too out of sync
	 */
	protected void releaseExclusive()
	{
		// look at dirty bit without latching
		// if this page actually has > 1/8 rows of the entire container, then
		// consider updating the row count if it is different
		// Since allocation page has recordCount of zero, the if clause will
		// never be true for an allocation page.
		if (isDirty && !isOverflowPage() &&
			(containerRowCount / 8) < recordCount())
		{
			int currentRowCount = internalNonDeletedRecordCount();	
			int delta = currentRowCount-initialRowCount;
			int posDelta = delta > 0 ? delta : (-delta);
			if ((containerRowCount/8) < posDelta)
			{
				// we are actually doing quite a bit of change, 
				// update container row count
				FileContainer myContainer = null;

				try
				{
					myContainer = (FileContainer) containerCache.find(identity.getContainerId());
					if (myContainer != null)
					{
						myContainer.updateEstimatedRowCount(delta);
						setContainerRowCount(myContainer.getEstimatedRowCount(0));
						initialRowCount = currentRowCount;

						// since I have the container, might as well update the
						// unfilled information
						myContainer.trackUnfilledPage(identity.getPageNumber(),
													  unfilled());
					}
				}
				catch(StandardException se)
				{
					// do nothing
				}
				finally
				{
					if (myContainer != null)
						containerCache.release(myContainer);
				}

			}
		}

		super.releaseExclusive();
	}

	protected void setDirty() {
		synchronized (this) {
			isDirty = true;
			preDirty = false;
		}
	}


	/**
		Write the page to disk.

		@exception StandardException  Error writing the page,

		@see Cacheable#clean
	*/

    /**
     * Write the page to disk.
     * <p>
     * MP - In a simple world we would just not allow clean until it held the
     *      latch on the page.  But in order to fit into the cache system, we 
     *      don't have enough state around to just make clean() latch the page 
     *      while doing the I/O - but we still need someway to insure that no
     *      changes happen to the page while the I/O is taking place.  
     *      Also someday it would be fine to allow reads of this page
     *      while the I/O was taking place.  
     *
     *      So first 
     *
	 * @return The identifier to be used to open the conglomerate later.
     *
	 * @exception  StandardException  Error writing the page.
     *
     * @see Cacheable#clean
     **/
	public void clean(boolean remove) throws StandardException {

		// must wait for the page to be unlatched
		synchronized (this) {

			if (!isDirty())
				return;

			// is someone else cleaning it
			while (inClean) {
				try {
					wait();
				} catch (InterruptedException ie) {
					throw StandardException.interrupt(ie);
				}
			}

			if (!isDirty())
				return;

			inClean = true;

			// If page is in LATCHED state (as opposed to UNLATCH or PRELATCH)
            // wait for the page to move to UNLATCHED state.  See Comments in
            // Generic/BasePage.java describing the interaction of inClean,
            // (owner != null), and preLatch.
			while ((owner != null) && !preLatch) 
            {
				try {
						wait();
				} catch (InterruptedException ie) 
				{
					inClean = false;
					throw StandardException.interrupt(ie);
				}
			}

			// The page is now effectively latched by the cleaner.
			// We only want to clean the page if the page is actually dirtied,
			// not when it is just pre-dirtied.
			if (!isActuallyDirty()) {
				preDirty = false; // the person who latched it gives up the
								  // latch without really dirtying the page
				inClean = false;
				notifyAll();
				return;
			}
		}

		try
		{
			writePage(getPageId(), false);
		}
		catch(StandardException se)
		{
			throw dataFactory.markCorrupt(se);
		}
		finally
		{
			// if there is something wrong in writing out the page, do not leave
			// it inClean state or it will block the next cleaner forever

			synchronized (this) {

				inClean = false;
				notifyAll();
			}
		}
	}

	public void clearIdentity() {
		alreadyReadPage = false;
		super.clearIdentity();
	}

	private void readPage(FileContainer myContainer, PageKey newIdentity) throws StandardException 
	{
		int pagesize = myContainer.getPageSize();
		setPageArray(pagesize);

		for(int i=0;;){
			try {

				myContainer.readPage(newIdentity.getPageNumber(), pageData);
				break;

			} catch (IOException ioe) {
				i++;	

								
				// we try to read four times because there might have been
				// thread interrupts when we tried to read the data.
				if(i>4){
			
					// page cannot be physically read
	
					StandardException se = 
						StandardException.newException(
								   SQLState.FILE_READ_PAGE_EXCEPTION, 
								   ioe, newIdentity, new Integer(pagesize));

					//if we are in rollforward recovery, it is possible that
					//this page actually does not exist on the disk yet because
					//the log record we are proccessing now is actually create page,
					//we will recreate the page if we are in rollforward
					//recovery, so just throw the exception.
						
				   if(dataFactory.getLogFactory().inRFR())
					   throw se;

					if (SanityManager.DEBUG)
                    {
                            throw dataFactory.markCorrupt(se);
					}
				}
			}
		}
	}


	private void writePage(PageKey identity, boolean syncMe) 
		 throws StandardException 
	{

		writeFormatId(identity); // make subclass write the page format

		writePage(identity);	// let subclass have a chance to write any cached
								// data to page data array

		// force WAL - and check to see if database is corrupt or is frozen.
		// last log Instant may be null if the page is being forced
		// to disk on a createPage (which violates the WAL protocol actually).
		// See FileContainer.newPage
		LogInstant flushLogTo = getLastLogInstant();
		dataFactory.flush(flushLogTo);

		if (flushLogTo != null) {					
			clearLastLogInstant();
		}


		// find the container and file access object
		FileContainer myContainer = (FileContainer) containerCache.find(identity.getContainerId());

		if (myContainer != null) {
			try {

				myContainer.writePage(identity.getPageNumber(), pageData, syncMe);

				//
				// Do some in memory unlogged bookkeeping tasks while we have
				// the container. 
				//

				if (!isOverflowPage() && isDirty())
				{

					// let the container knows whether this page is a not filled,
					// non-overflow page
					myContainer.trackUnfilledPage(identity.getPageNumber(),
												  unfilled());

					// if this is not an overflow page, see if the page's row
					// count has changed since it come into the cache.
					//
					// if the page is not invalid, row count is 0.  Otherwise,
					// count non-deleted records on page.
					//
					// Cannot call nonDeletedRecordCount because the page is
					// unlatched now even though nobody is changing it
					int currentRowCount = internalNonDeletedRecordCount();

					if (currentRowCount != initialRowCount)
					{
						myContainer.updateEstimatedRowCount(currentRowCount-initialRowCount);
						setContainerRowCount(myContainer.getEstimatedRowCount(0));
						initialRowCount = currentRowCount;
					}
				}

			} catch (IOException ioe) {
				// page cannot be written
				throw StandardException.newException(
                    SQLState.FILE_WRITE_PAGE_EXCEPTION, 
                    ioe, identity, new Integer(myContainer.getPageSize()));
			}
			finally
			{
				containerCache.release(myContainer);
				myContainer = null;
			}
		} 
		else
		{
			StandardException nested = StandardException.newException(SQLState.DATA_CONTAINER_VANISHED, identity.getContainerId());
			throw dataFactory.markCorrupt(
                StandardException.newException(
                    SQLState.FILE_WRITE_PAGE_EXCEPTION, nested, 
                    identity, new Integer(myContainer.getPageSize())));
		}

		synchronized (this) {
			isDirty = false;
			preDirty = false;
		}

	}

	public void setContainerRowCount(long rowCount)
	{
		containerRowCount = rowCount;
	}



	/*
	** if the page size is different from the page buffer, then make a
	** new page buffer and make subclass use the new page buffer
	*/

	protected void setPageArray(int pageSize) throws StandardException
	{
		if ((pageData == null) || (pageData.length != pageSize)) {
			pageData = new byte[pageSize];

			if (pageData == null || pageData.length != pageSize)
            {
				throw StandardException.newException(
                        SQLState.DATA_OBJECT_ALLOCATION_FAILED, "PAGE");
            }
				
			usePageBuffer(pageData);
		}
	}

	/* methods for subclass of cached page */

	// use a new pageData buffer, initialize in memory structure that depend on
	// the pageData's size.  The actual disk data may not have not been read in
	// yet so don't look at the content of the buffer
	protected abstract void usePageBuffer(byte[] buffer);


	// initialize in memory structure using the read in buffer in pageData
	protected abstract void initFromData(FileContainer container, PageKey id) throws StandardException;


	// create the page
	protected abstract void createPage(PageKey id, int[] args) throws StandardException;

	// page is about to be written, write everything to pageData array
	protected abstract void writePage(PageKey id) throws StandardException;		

	// write out the formatId to the pageData
	protected abstract void writeFormatId(PageKey identity) throws StandardException;


}
