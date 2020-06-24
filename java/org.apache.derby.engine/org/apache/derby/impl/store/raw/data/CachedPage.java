/*

   Derby - Class org.apache.derby.impl.store.raw.data.CachedPage

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

import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.raw.PageKey;

import org.apache.derby.iapi.services.cache.Cacheable;
import org.apache.derby.iapi.services.cache.CacheManager;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.iapi.services.io.FormatIdUtil;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.error.ExceptionSeverity;

import org.apache.derby.iapi.util.InterruptStatus;

import java.io.IOException;

/**
	A base page that is cached.

	Since there are multiple page formats, use this abstract class to implement
	cacheable interface.

*/

public abstract class CachedPage extends BasePage implements Cacheable
{
	protected boolean   alreadyReadPage;    // true when page read by another 
                                            // class

	protected byte[]    pageData;		    // the actual page data - this is
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

	protected boolean		isDirty;		// must be set to true whenever the
                                            // pageData array is touched 
                                            // directly or indirectly.

	protected boolean		preDirty;		// set to true if the page is clean
								            // and the pageData array is about 
                                            // to be touched directly or 
                                            // indirectly.


	protected int		initialRowCount;    // keep a running count of rows for
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

	public final void setFactory(BaseDataFileFactory factory) 
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-758
		dataFactory     = factory;
		pageCache       = factory.getPageCache();
		containerCache  = factory.getContainerCache();
	}

	/**
        Initialize a CachedPage.
        <p>
		Initialize the object, ie. perform work normally perfomed in 
        constructor.  Called by setIdentity() and createIdentity().
	*/
	protected void initialize()
	{
		super.initialize();
//IC see: https://issues.apache.org/jira/browse/DERBY-758
		isDirty             = false;
		preDirty            = false;
		initialRowCount     = 0;
		containerRowCount   = 0;
	}

	/*
	** Methods of Cacheable
	*/

    /**
     * Find the container and then read the page from that container.
     * <p>
     * This is the way new pages enter the page cache.
     * <p>
     *
	 * @return always true, higher levels have already checked the page number 
     *         is valid for an open.
     *
     * @exception StandardException Standard Derby policy.
     *
     * @see Cacheable#setIdentity
     **/
	public Cacheable setIdentity(Object key) 
        throws StandardException 
    {
		if (SanityManager.DEBUG) 
        {
			SanityManager.ASSERT(key instanceof PageKey);
		}

		initialize();

		PageKey newIdentity = (PageKey) key;

		FileContainer myContainer = 
            (FileContainer) containerCache.find(newIdentity.getContainerId());
//IC see: https://issues.apache.org/jira/browse/DERBY-758

		setContainerRowCount(myContainer.getEstimatedRowCount(0));

		try
		{
			if (!alreadyReadPage)
            {
                // Fill in the pageData array by reading bytes from disk.
				readPage(myContainer, newIdentity);	
            }
			else
            {
                // pageData array already filled
				alreadyReadPage = false;
            }

			// if the formatID on disk is not the same as this page instance's
			// format id, instantiate the real page object
			int fmtId = getTypeFormatId();

			int onPageFormatId = FormatIdUtil.readFormatIdInteger(pageData);
			if (fmtId != onPageFormatId)
			{
//IC see: https://issues.apache.org/jira/browse/DERBY-758
				return changeInstanceTo(
                            onPageFormatId, newIdentity).setIdentity(key);
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
     * Find the container and then create the page in that container.
     * <p>
     * This is the process of creating a new page in a container, in that
     * case no need to read the page from disk - just need to initialize it
     * in the cache.
     * <p>
     *
	 * @return new page, higher levels have already checked the page number is 
     *         valid for an open.
     *
     * @param key               Which page is this?
     * @param createParameter   details needed to create page like size, 
     *                          format id, ...
     *
	 * @exception  StandardException  Standard exception policy.
     *
     * @see Cacheable#createIdentity
     **/
	public Cacheable createIdentity(
//IC see: https://issues.apache.org/jira/browse/DERBY-758
    Object  key, 
    Object  createParameter) 
        throws StandardException 
    {

		if (SanityManager.DEBUG) 
        {
			SanityManager.ASSERT(key instanceof PageKey);
		}

		initialize();

		PageKey newIdentity = (PageKey) key;

//IC see: https://issues.apache.org/jira/browse/DERBY-3589
		PageCreationArgs createArgs = (PageCreationArgs) createParameter;
        int formatId = createArgs.formatId;

		if (formatId == -1)
        {
			throw StandardException.newException(
//IC see: https://issues.apache.org/jira/browse/DERBY-3725
                    SQLState.DATA_UNKNOWN_PAGE_FORMAT_2, 
                    newIdentity,
                    org.apache.derby.iapi.util.StringUtil.hexDump(pageData));
        }

		// createArgs[0] contains the integer form of the formatId 
		// if it is not the same as this instance's formatId, instantiate the
		// real page object
//IC see: https://issues.apache.org/jira/browse/DERBY-3589
		if (formatId != getTypeFormatId())
		{
			return(
                changeInstanceTo(formatId, newIdentity).createIdentity(
                        key, createParameter));
		}
		
		// this is the correct page instance
		initializeHeaders(5);
		createPage(newIdentity, createArgs);

		fillInIdentity(newIdentity);

		initialRowCount = 0;

		/*
		 * if we need to grow the container and the page has not been
		 * preallocated, writing page before the log is written so that we
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
        int syncFlag = createArgs.syncFlag;
//IC see: https://issues.apache.org/jira/browse/DERBY-3589
		if ((syncFlag & WRITE_SYNC) != 0 ||
			(syncFlag & WRITE_NO_SYNC) != 0)
			writePage(newIdentity, (syncFlag & WRITE_SYNC) != 0);

		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON(FileContainer.SPACE_TRACE))
			{
				String sync =
                    ((syncFlag & WRITE_SYNC) != 0)     ? "Write_Sync" :
					(((syncFlag & WRITE_NO_SYNC) != 0) ? "Write_NO_Sync" :
					                                          "No_write");
//IC see: https://issues.apache.org/jira/browse/DERBY-758

				SanityManager.DEBUG(
                    FileContainer.SPACE_TRACE,
                    "creating new page " + newIdentity + " with " + sync);
			}
		}

		return this;
	}

    /**
     * Convert this page to requested type, as defined by input format id.
     * <p>
     * The current cache entry is a different format id than the requested
     * type, change it.  This object is instantiated to the wrong subtype of 
     * cachedPage, this routine will create an object with the correct subtype,
     * and transfer all pertinent information from this to the new correct 
     * object.
     * <p>
     *
	 * @return The new object created with the input fid and transfered info.
     *
     * @param fid          The format id of the new page.
     * @param newIdentity  The key of the new page.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
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
//IC see: https://issues.apache.org/jira/browse/DERBY-3725
                    SQLState.DATA_UNKNOWN_PAGE_FORMAT_2, 
                    newIdentity,
                    org.apache.derby.iapi.util.StringUtil.hexDump(pageData));
            }
		}

		realPage.setFactory(dataFactory);

		// avoid creating the data buffer if possible, transfer it to the new 
        // page if this is the first time the page buffer is used, then 
        // createPage will create the page array with the correct page size
//IC see: https://issues.apache.org/jira/browse/DERBY-758
		if (this.pageData != null) 
        {
			realPage.alreadyReadPage = true;
			realPage.usePageBuffer(this.pageData);
		}

        // RESOLVE (12/15/06) - the following code is commented out, but
        // not sure why.

		// this page should not be used any more, null out all its content and
		// wait for GC to clean it up  

		//destroyPage();// let this subtype have a chance to get rid of stuff
		//this.pageData = null;	// this instance no longer own the data array
		//this.pageCache = null;
		//this.dataFactory = null;
		//this.containerCache = null;

		return realPage;
	}

    /**
     * Is the page dirty?
     * <p>
     * The isDirty flag indicates if the pageData or pageHeader has been
     * modified.  The preDirty flag indicates that the pageData or the
     * pageHeader is about to be modified.  The reason for these 2 flags
     * instead of just one is to accomodate checkpoint.  After a clean
     * (latched) page sends a log record to the log stream but before that page
     * is dirtied by the log operation, a checkpoint could be taken.  If so,
     * then the redoLWM will be after the log record but, without preDirty, the
     * cache cleaning will not have waited for the change.  So the preDirty bit
     * is to stop the cache cleaning from skipping over this (latched) page
     * even though it has not really been modified yet.  
     *
	 * @return true if the page is dirty.
     *
     * @see Cacheable#isDirty
     **/
	public boolean isDirty() 
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-758
		synchronized (this) 
        {
			return isDirty || preDirty;
		}
	}

    /**
     * Has the page or its header been modified.
     * <p>
     * See comment on class header on meaning of isDirty and preDirty bits.
     * <p>
     *
	 * @return true if changes have actually been made to the page in memory.
     **/
	public boolean isActuallyDirty() 
    {
		synchronized (this) 
        {
			return isDirty;
		}
	}

    /**
     * Set state to indicate the page or its header is about to be modified.
     * <p>
     * See comment on class header on meaning of isDirty and preDirty bits.
     **/
	public void preDirty()
	{
		synchronized (this) 
        {
			if (!isDirty)
				preDirty = true;
		}
	}

    /**
     * Set state to indicate the page or its header has been modified.
     * <p>
     * See comment on class header on meaning of isDirty and preDirty bits.
     * <p>
     **/
	protected void setDirty() 
    {
		synchronized (this) 
        {
			isDirty  = true;
			preDirty = false;
		}
	}

    /**
     * exclusive latch on page is being released.
     * <p>
     * The only work done in CachedPage is to update the row count on the
     * container if it is too out of sync.
     **/
	protected void releaseExclusive()
	{
		// look at dirty bit without latching, the updating of the row
        // count is just an optimization so does not need the latch.
        //
		// if this page actually has > 1/8 rows of the entire container, then
		// consider updating the row count if it is different.
        //
        // No need to special case allocation pages because it has recordCount 
        // of zero, thus the if clause will never be true for an allocation 
        // page.
		if (isDirty && !isOverflowPage() &&
			(containerRowCount / 8) < recordCount())
		{
			int currentRowCount = internalNonDeletedRecordCount();	
			int delta           = currentRowCount-initialRowCount;
			int posDelta        = delta > 0 ? delta : (-delta);

			if ((containerRowCount/8) < posDelta)
			{
				// This pages delta row count represents a significant change
                // with respect to current container row count so update 
                // container row count
				FileContainer myContainer = null;

				try
				{
					myContainer = (FileContainer) 
                        containerCache.find(identity.getContainerId());

					if (myContainer != null)
					{
						myContainer.updateEstimatedRowCount(delta);
						setContainerRowCount(
                                myContainer.getEstimatedRowCount(0));

						initialRowCount = currentRowCount;

						// since I have the container, might as well update the
						// unfilled information
						myContainer.trackUnfilledPage(
                            identity.getPageNumber(), unfilled());
					}
				}
				catch (StandardException se)
				{
					// do nothing, not sure what could fail but this update
                    // is just an optimization so no need to throw error.
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
     *
	 * @exception  StandardException  Error writing the page.
     *
     * @see Cacheable#clean
     **/
	public void clean(boolean remove) throws StandardException 
    {

		// must wait for the page to be unlatched
//IC see: https://issues.apache.org/jira/browse/DERBY-758
		synchronized (this) 
        {
			if (!isDirty())
				return;

			// is someone else cleaning it
			while (inClean) 
            {
				try 
                {
					wait();
				} 
                catch (InterruptedException ie) 
                {
                    InterruptStatus.setInterrupted();
				}
			}

            // page is not "inClean" by other thread at this point.

			if (!isDirty())
				return;

			inClean = true;

			// If page is in LATCHED state (as opposed to UNLATCH or PRELATCH)
            // wait for the page to move to UNLATCHED state.  See Comments in
            // Generic/BasePage.java describing the interaction of inClean,
            // (owner != null), and preLatch.
			while ((owner != null) && !preLatch) 
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-758
				try 
                { 
                    wait();
				} 
                catch (InterruptedException ie) 
				{
//IC see: https://issues.apache.org/jira/browse/DERBY-4741
//IC see: https://issues.apache.org/jira/browse/DERBY-4741
                    InterruptStatus.setInterrupted();
				}
			}

			// The page is now effectively latched by the cleaner.
			// We only want to clean the page if the page is actually dirtied,
			// not when it is just pre-dirtied.
//IC see: https://issues.apache.org/jira/browse/DERBY-758
			if (!isActuallyDirty()) 
            {
                // the person who latched it gives up the
                // latch without really dirtying the page
				preDirty = false; 
				inClean  = false;
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
            // If we get an error while trying to write a page, current
            // recovery system requires that entire DB is shutdown.  Then
            // when system is rebooted we will run redo recovery which 
            // if it does not encounter disk errors will guarantee to recover
            // to a transaction consistent state.  If this write is a 
            // persistent device problem, redo recovery will likely fail
            // attempting to the same I/O.  Mark corrupt will stop all further
            // writes of data and log by the system.
			throw dataFactory.markCorrupt(se);
		}
		finally
		{
			// if there is something wrong in writing out the page, 
            // do not leave it inClean state or it will block the next cleaner 
            // forever

//IC see: https://issues.apache.org/jira/browse/DERBY-758
			synchronized (this) 
            {
				inClean = false;
				notifyAll();
			}
		}
	}

	public void clearIdentity() 
    {
		alreadyReadPage = false;
		super.clearIdentity();
	}

    /**
     * read the page from disk into this CachedPage object.
     * <p>
     * A page is read in from disk into the pageData array of this object,
     * and then put in the cache.
     * <p>
     *
     * @param myContainer the container to read the page from.
     * @param newIdentity indentity (ie. page number) of the page to read
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	private void readPage(
    FileContainer   myContainer, 
    PageKey         newIdentity) 
        throws StandardException 
	{
		int pagesize = myContainer.getPageSize();

        // we will reuse the existing page array if it is same size, the
        // cache does support caching various sized pages.
		setPageArray(pagesize);

		for (int io_retry_count = 0;;)
        {
			try 
            {
				myContainer.readPage(newIdentity.getPageNumber(), pageData);
				break;
			} 
            catch (IOException ioe) 
            {
				io_retry_count++;	
								
				// Retrying read I/O's has been found to be successful sometimes
                // in completing the read without having to fail the calling
                // query, and in some cases avoiding complete db shutdown.
                // Some situations are:
                //     spurious interrupts being sent to thread by clients.
                //     unreliable hardware like a network mounted file system.
                //
                // The only option other than retrying is to fail the I/O 
                // immediately and throwing an error, thus performance cost
                // not really a consideration.
                //
                // The retry max of 4 is arbitrary, but has been enough that
                // not many read I/O errors have been reported.
				if (io_retry_count > 4)
                {
					// page cannot be physically read
	
					StandardException se = 
						StandardException.newException(
								   SQLState.FILE_READ_PAGE_EXCEPTION, 
								   ioe, newIdentity, pagesize);
//IC see: https://issues.apache.org/jira/browse/DERBY-6856

						
//IC see: https://issues.apache.org/jira/browse/DERBY-758
				    if (dataFactory.getLogFactory().inRFR())
                    {
                        //if in rollforward recovery, it is possible that this 
                        //page actually does not exist on the disk yet because
                        //the log record we are proccessing now is actually 
                        //creating the page, we will recreate the page if we 
                        //are in rollforward recovery, so just throw the 
                        //exception.
                        throw se;
                    }
                    else
                    {
                        if (SanityManager.DEBUG)
                        {
                            // by shutting down system in debug mode, maybe
                            // we can catch root cause of the interrupt.
                            throw dataFactory.markCorrupt(se);
                        }
                        else
                        {
                            // No need to shut down runtime database on read
                            // error in delivered system, throwing exception 
                            // should be enough.  Thrown exception has nested
                            // IO exception which is root cause of error.
                            throw se;
                        }
					}
				}
			}
		}
	}


    /**
     * write the page from this CachedPage object to disk.
     * <p>
     *
     * @param identity indentity (ie. page number) of the page to read
     * @param syncMe      does the write of this single page have to be sync'd?
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	private void writePage(
//IC see: https://issues.apache.org/jira/browse/DERBY-758
    PageKey identity, 
    boolean syncMe) 
		 throws StandardException 
	{

        // make subclass write the page format
		writeFormatId(identity); 

        // let subclass have a chance to write any cached data to page data 
        // array
		writePage(identity);	 

		// force WAL - and check to see if database is corrupt or is frozen.
		// last log Instant may be null if the page is being forced
		// to disk on a createPage (which violates the WAL protocol actually).
		// See FileContainer.newPage
		LogInstant flushLogTo = getLastLogInstant();
		dataFactory.flush(flushLogTo);

//IC see: https://issues.apache.org/jira/browse/DERBY-758
		if (flushLogTo != null) 
        {					
			clearLastLogInstant();
		}


		// find the container and file access object
		FileContainer myContainer = 
            (FileContainer) containerCache.find(identity.getContainerId());

//IC see: https://issues.apache.org/jira/browse/DERBY-3215
		if (myContainer == null)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-758
			StandardException nested =
				StandardException.newException(
					SQLState.DATA_CONTAINER_VANISHED,
					identity.getContainerId());
			throw dataFactory.markCorrupt(
				StandardException.newException(
					SQLState.FILE_WRITE_PAGE_EXCEPTION, nested,
//IC see: https://issues.apache.org/jira/browse/DERBY-3215
					identity));
		}

		try
		{
			myContainer.writePage(
				identity.getPageNumber(), pageData, syncMe);

			//
			// Do some in memory unlogged bookkeeping tasks while we have
			// the container.
			//

			if (!isOverflowPage() && isDirty())
			{

				// let the container knows whether this page is a not
				// filled, non-overflow page
//IC see: https://issues.apache.org/jira/browse/DERBY-758
				myContainer.trackUnfilledPage(
					identity.getPageNumber(), unfilled());

				// if this is not an overflow page, see if the page's row
				// count has changed since it come into the cache.
				//
				// if the page is not invalid, row count is 0.	Otherwise,
				// count non-deleted records on page.
				//
				// Cannot call nonDeletedRecordCount because the page is
				// unlatched now even though nobody is changing it
				int currentRowCount = internalNonDeletedRecordCount();

				if (currentRowCount != initialRowCount)
				{
//IC see: https://issues.apache.org/jira/browse/DERBY-758
					myContainer.updateEstimatedRowCount(
						currentRowCount - initialRowCount);

					setContainerRowCount(
						myContainer.getEstimatedRowCount(0));

					initialRowCount = currentRowCount;
				}
			}

		}
		catch (IOException ioe)
		{
			// page cannot be written
			throw StandardException.newException(
				SQLState.FILE_WRITE_PAGE_EXCEPTION,
//IC see: https://issues.apache.org/jira/browse/DERBY-3215
				ioe, identity);
		}
		finally
		{
			containerCache.release(myContainer);
			myContainer = null;
		}

		synchronized (this) 
        {
            // change page state to not dirty after the successful write
			isDirty     = false;
			preDirty    = false;
		}
	}

	public void setContainerRowCount(long rowCount)
	{
		containerRowCount = rowCount;
	}

	/**
	** if the page size is different from the page buffer, then make a
	** new page buffer and make subclass use the new page buffer
	*/
	protected void setPageArray(int pageSize)
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-758
		if ((pageData == null) || (pageData.length != pageSize)) 
        {
            // Give a chance for garbage collection to free
            // the old array before the new array is allocated.
            // Just in case memory is low.
            pageData = null; 
			pageData = new byte[pageSize];
		}

        // Always call usePageBuffer(), even when we reuse the buffer, so that
        // totalSpace and friends are recalculated (DERBY-3116).
        usePageBuffer(pageData);
	}


    /**
	 * Returns the page data array used to write on disk version.
     *
     * <p>
	 * returns the page data array, that is actually written to the disk,
	 * when the page is cleaned from the page cache.  Takes care of flushing
     * in-memory information to the array (like page header and format id info).
     * <p>
     *
	 * @return The array of bytes that is the on disk version of page.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	protected byte[] getPageArray() throws StandardException 
	{
        // make subclass write the page format
		writeFormatId(identity); 
//IC see: https://issues.apache.org/jira/browse/DERBY-758

        // let subclass have a chance to write any cached
        // data to page data array
		writePage(identity);	

		return pageData;
	}

	/* methods for subclass of cached page */

	// use a new pageData buffer, initialize in memory structure that depend on
	// the pageData's size.  The actual disk data may not have not been read in
	// yet so don't look at the content of the buffer
	protected abstract void usePageBuffer(byte[] buffer);


	// initialize in memory structure using the read in buffer in pageData
	protected abstract void initFromData(FileContainer container, PageKey id) 
        throws StandardException;

//IC see: https://issues.apache.org/jira/browse/DERBY-758

	// create the page
	protected abstract void createPage(PageKey id, PageCreationArgs args)
        throws StandardException;

	// page is about to be written, write everything to pageData array
	protected abstract void writePage(PageKey id) throws StandardException;		

	// write out the formatId to the pageData
	protected abstract void writeFormatId(PageKey identity) 
        throws StandardException;
}
