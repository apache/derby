/*

   Derby - Class org.apache.derby.impl.store.raw.data.AllocPage

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

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.TypedFormat;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.Loggable;
import org.apache.derby.iapi.store.raw.PageKey;
import org.apache.derby.iapi.store.raw.PageTimeStamp;
import org.apache.derby.iapi.store.raw.RawStoreFactory;

import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;

import org.apache.derby.impl.store.raw.data.BaseContainerHandle;
import org.apache.derby.impl.store.raw.data.BasePage;
import org.apache.derby.impl.store.raw.data.PageVersion;

import java.io.IOException;

import java.io.ObjectOutput;
import java.io.ObjectInput;

import org.apache.derby.iapi.services.io.ArrayInputStream;

/**
	An allocation page of the file container.
	<P>
	This class extends a normal Stored page, with the exception that a hunk of
	space may be 'borrowed' by the file container to store the file header.
	<P>
	The borrowed space is not visible to the alloc page even though it is
	present in the page data array.  It is accessed directly by the
	FileContainer.  Any change made to the borrowed space is not managed or
	seen by the allocation page.
	<P
	The reason for having this borrowed space is so that the container header
	does not need to have a page of its own.

	<P><B>Page Format</B><BR>
	An allocation page extends a stored page, the on disk format is different
	from a stored page in that N bytes are 'borrowed' by the container and the
	page header of an allocation page will be slightly bigger than a normal
	stored page.  This N bytes are stored between the page header and the 
    record space.
	<P>
	The reason why this N bytes can't simply be a row is because it needs to be
	statically accessible by the container object to avoid a chicken and egg
	problem of the container object needing to instantiate an alloc page object
	before it can be objectified, and an alloc page object needing to
	instantiate a container object before it can be objectified.  So this N
	bytes must be stored outside of the normal record interface yet it must be
	settable because only the first alloc page has this borrowed space.  Other
	(non-first) alloc page have N == 0.

	<PRE>
                             <-- borrowed ->
	+----------+-------------+---+---------+-------------------+-------------+--------+
	| FormatId | page header | N | N bytes | alloc extend rows | slot offset |checksum|
	+----------+-------------+---+---------+-------------------+-------------+--------+
	</PRE>

	N is a byte that indicates the size of the borrowed space.  Once an alloc
	page is initialized, the value of N cannot change.
	<P>
	The maximum space that can be borrowed by the container is 256 bytes.
	<P>
	The allocation page are of the same page size as any other pages in the
	container. The first allocation page of the FileContainer starts at the
	first physical byte of the container.  Subsequent allocation pages are
	chained via the nextAllocPageOffset.  Each allocation page is expected to
	manage at least 1000 user pages (for 1K page size) so this chaining may not
	be a severe performance hit.  The logical -> physical mapping of an
	allocation page is stored in the previous allocation page.  The container
	object will need to maintain this mapping.
	<P>
	The following fields are stored in the page header
	<PRE>
	@format_id	RAW_STORE_ALLOC_PAGE
	@purpose	manage page allocation
	@upgrade
	@disk_layout
		FormatId(int)
		StoredPageHeader	see StoredPage
		nextAllocPageNubmer(long)	the next allocation page's number
		nextAllocPageOffset(long)	the file offset of the next allocation page
		reserved1(long)				reserved for future usage
		reserved2(long)				reserved for future usage
		reserved3(long)				reserved for future usage
		reserved4(long)				reserved for future usage
		N(byte)						the size of the borrowed container info
		containerInfo(byte[N])		the content of the borrowed container info
		AllocExtent					the one and only extent on this alloc page

	@end_format
	</PRE>

	<P>
	The allocation page contains allocation extent rows.  In this first cut
	implementation, there is only 1 allocation extent row per allocation page.
	<P>
	The allocation extent row is an externalizable object and is directly
	written on to the page by the alloc page.  In other words, it will not be
	converted in to a storeableRow.  This is to cut down overhead, enhance
	performance and gives more control of the size and layout of the allocation
	extent row to the alloc page.
	<P>
	<HR WIDTH="100%">
	<BR> DETAIL implmentation notes <BR>
	<HR WIDTH="100%">
	<P>
	Create Container - an embryonic allocation page is formatted on disk by a
	spcial static function to avoid instantiating a full AllocPage object.
	This embryonic allocation has enough information that it can find the
	file header and not much else.  Then the allocation page is perperly
	initiated by creating the first extent.
	<P>
	Open Container - A static AllocPage method will be used to read off the
	container information directly from disk.  Even if
	the first alloc page (page 0) is already in the page cache, it will not be
	used because cleaning the alloc page will introduce a deadlock if the
	container is not in the container cache.  Long term, the first alloc page
	should probably live in the container cache rather than in the page cache.
	<P>
	Get Page - The first alloc page (page 0) will be read into the page cache.
	Continue to follow the alloc page chain until the alloc page that manages
	the specified page is found.  From the alloc page, the physical offset of
	the specified page is located.
	<P>
	Cleaning alloc page - the alloc page is written out the same way any page
	is written out.  The container object will provide a call back to the alloc
	page to write the current version of the container object back into the
	borrowed space before the alloc page itself is written out.
	<P>
	Cleaning the container object - get the the first alloc page, dirty it and
	clean it (which will cause it to call the container object to write itself
	out into the borrowed space).  The versioning of the container is
	independent of the versioning of the alloc page.  The container version is
	stored inside the borrowed space and is opaque to the alloc page.
	<P>
	For the fields in an allocation extent row

	@see AllocExtent
*/


public class AllocPage extends StoredPage
{
	/*
	 * typed format
	 */
	public static final int FORMAT_NUMBER = StoredFormatIds.RAW_STORE_ALLOC_PAGE;
	// format Id must fit in 4 bytes

	/**
		Return my format identifier.
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.RAW_STORE_ALLOC_PAGE;
	}

	/*****************************************************************
	 * alloc page header
	 *****************************************************************/
	private long nextAllocPageNumber;	// if isLast, nextAllocPageNumber == INVALID_PAGE_NUMBER
	private long nextAllocPageOffset;
	private long reserved1;
	private long reserved2;
	private long reserved3;
	private long reserved4;

	private AllocExtent extent;

	private int borrowedSpace;

	/*****************************************************************
	 * constants
	 *****************************************************************/

	/*
	 * allocation page header
	 * 8 bytes	long	next alloc page number
	 * 8 bytes	long	next alloc page physical offset
	 * 8 bytes  long	reserved1
	 * 8 bytes  long	reserved2
	 * 8 bytes  long	reserved3
	 * 8 bytes  long	reserved4
	 */
	protected static final int ALLOC_PAGE_HEADER_OFFSET =
		StoredPage.PAGE_HEADER_OFFSET + StoredPage.PAGE_HEADER_SIZE;

	protected static final int ALLOC_PAGE_HEADER_SIZE = 8+8+(4*8);

	/* borrowed_SPACE_OFFSET is where the borrowed space len is kept */
	protected static final int BORROWED_SPACE_OFFSET =
		ALLOC_PAGE_HEADER_OFFSET + ALLOC_PAGE_HEADER_SIZE;

	/* size of the borrowed space length */
	protected static final int BORROWED_SPACE_LEN = 1; // 1 byte to store the containerInfo length

	/*
	 * BORROWED_SPACE_OFFSET + BORROWED_SPACE_LEN is the beginning offset of
	 * the borrowed space
	 */

	/*
	 * the entire borrowed space must live within MAX_BORROWED_SPACE of the
	 * alloc page
	 */
	protected static final int MAX_BORROWED_SPACE =
			RawStoreFactory.PAGE_SIZE_MINIMUM / 5; // cannot take more then 1/5 of the page

	public AllocPage()
	{
		super();
	}

	/*
	 * overwriting StoredPage methods
	 */

	protected int getMaxFreeSpace() {

		// the maximum free space is reduced by the allocation page header the
		// size of the borrowed space.  In all allocation page except the first
		// one, there is no borrowed space and this is indeed the max free
		// space.  In the first allocation page, need to further subtract
		// the borrowed space

		return super.getMaxFreeSpace() - ALLOC_PAGE_HEADER_SIZE -
			BORROWED_SPACE_LEN - borrowedSpace;
	}


	/*
	 * Methods of cachedPage - create, read and write up a page
	 * Overwriting StoredPage's CachedPage methods
	 */

    /**
     * Create a new alloc page.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	protected void createPage(PageKey newIdentity, int[] args) 
		 throws StandardException
	{

		super.createPage(newIdentity, args);

		// args[0] is the format id
		// args[1] is whether to sync the page to disk or not
		// args[2] is the pagesize (used by StoredPage)
		// args[3] is the spareSize (used by StoredPage)
		// args[4] is the number of bytes to reserve for container header
		// args[5] is the minimumRecordSize
		// NOTE: the arg list here must match the one in FileContainer
		int pageSize = args[2];
		int minimumRecordSize = args[5];
		borrowedSpace = args[4];

		if (SanityManager.DEBUG)
		{
			// MAX_BORROWED_SPACE can't be bigger than what can be represented in 1 byte space
			SanityManager.ASSERT(MAX_BORROWED_SPACE <= 255);

            if (!(borrowedSpace + BORROWED_SPACE_LEN + BORROWED_SPACE_OFFSET 
                    < MAX_BORROWED_SPACE))
            {
                SanityManager.THROWASSERT(
                    "borrowedSpace too big = " + borrowedSpace);
            }
			SanityManager.ASSERT(pageData != null);
		}
		pageData[BORROWED_SPACE_OFFSET] = (byte)borrowedSpace;

		// remember that the borrowed space have been wiped out now, it
		// needs to be put back when the page is written out.
		// blot out borrowed space before checksum is verified
		if (borrowedSpace > 0)
		{
			clearSection(BORROWED_SPACE_OFFSET + BORROWED_SPACE_LEN, borrowedSpace);
		}

		// init the rest of the header and the allocation extent
		nextAllocPageNumber = ContainerHandle.INVALID_PAGE_NUMBER;
		nextAllocPageOffset = 0;
		reserved1 = reserved2 = reserved3 = reserved4 = 0;

		// calculate how much space we have left for the extent map
		int maxSpace = getMaxFreeSpace();

		// the pages this extent is going to manage starts from pageNum+1
		// starting physical offset is pageSize*(pageNum+1) since we have
		// no logical to physical mapping yet...
		extent = createExtent(newIdentity.getPageNumber()+1, pageSize, 0 /* pagesAlloced */, maxSpace);
	}

	private AllocExtent createExtent(long pageNum, int pageSize, int pagesAlloced, int availspace)
	{
		int maxPages = AllocExtent.MAX_RANGE(availspace);

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(maxPages > 8, "cannot manage > 8 pages");


		if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON(TEST_MULTIPLE_ALLOC_PAGE))
            {
                maxPages = 2;		// 2 pages per alloc page
            }
        }

		return new AllocExtent(pageNum*pageSize,	 		// starting offset
							   pageNum,					// extent start page number
							   pagesAlloced,				// #pages already allocated
							   pageSize, 					// page size
							   maxPages);					// max #pages to manage
	}

	/**
		Initialize in memory structure using the buffer in pageData

		@exception StandardException If the page cannot be read correctly, or is inconsistent.
	*/
	protected void initFromData(FileContainer myContainer, PageKey newIdentity)
		 throws StandardException
	{
		if (pageData.length < BORROWED_SPACE_OFFSET + BORROWED_SPACE_LEN)
        {
			throw dataFactory.markCorrupt(
                StandardException.newException(
                    SQLState.DATA_CORRUPT_PAGE, newIdentity));
        }

		byte n = pageData[BORROWED_SPACE_OFFSET];
		borrowedSpace = (int)n;

		if (pageData.length < BORROWED_SPACE_OFFSET + BORROWED_SPACE_LEN + n)
        {
			throw dataFactory.markCorrupt(
                StandardException.newException(
                    SQLState.DATA_CORRUPT_PAGE, newIdentity));
        }

		// blot out borrowed space before checksum is verified
		if (borrowedSpace > 0)
		{
			clearSection(BORROWED_SPACE_OFFSET + BORROWED_SPACE_LEN, borrowedSpace);
		}

		super.initFromData(myContainer, newIdentity);

		try
		{
			// now init alloc page header fields
			readAllocPageHeader();

			// now init the allocation extent - read it from offset
			int offset = BORROWED_SPACE_OFFSET + BORROWED_SPACE_LEN + borrowedSpace;
			extent = readExtent(offset);
		}
		catch (IOException ioe)
		{
			throw dataFactory.markCorrupt(
                StandardException.newException(
                    SQLState.DATA_CORRUPT_PAGE, ioe, newIdentity));
		}
		catch (ClassNotFoundException cnfe)
		{
			throw dataFactory.markCorrupt(
                StandardException.newException(
                    SQLState.DATA_CORRUPT_PAGE, cnfe, newIdentity));
		}
	}

	/**
		Write the page out

		@exception StandardException If the page cannot be written
	*/
	protected void writePage(PageKey identity) throws StandardException
	{
		try
		{
			updateAllocPageHeader(); // write out the next alloc page chain

			// blot out borrowed space before checksum is calculated - even
			// though the page is first read in with borrowed space blotted
			// out, whenever this page got cleaned the container will overlay
			// the container info in the borrowed space.
			int n = (int)pageData[BORROWED_SPACE_OFFSET];
            if (SanityManager.DEBUG)
            {
    			if (n != borrowedSpace)
    				SanityManager.THROWASSERT(
                        "different borrowed space " + n + ", " + borrowedSpace);
	    	}
			if (n > 0)
			{
				clearSection(BORROWED_SPACE_OFFSET + BORROWED_SPACE_LEN, n);
			}

			int offset = BORROWED_SPACE_OFFSET + BORROWED_SPACE_LEN + n;
			writeExtent(offset);
		}
		catch (IOException ioe)
		{
			// i/o methods on the byte array have thrown an IOException
			throw dataFactory.markCorrupt(
                StandardException.newException(
                    SQLState.DATA_CORRUPT_PAGE, ioe, identity));
		}

		// let store page write out the rest and do the checksum
		super.writePage(identity);
	}


	private void readAllocPageHeader() throws IOException
	{
		ArrayInputStream lrdi = rawDataIn;
		lrdi.setPosition(ALLOC_PAGE_HEADER_OFFSET);

		nextAllocPageNumber = lrdi.readLong();
		nextAllocPageOffset = lrdi.readLong();
		reserved1 = lrdi.readLong();
		reserved2 = lrdi.readLong();
		reserved3 = lrdi.readLong();
		reserved4 = lrdi.readLong();
	}

	private void updateAllocPageHeader() throws IOException
	{
		// rawDataOut and logicalDataOut are defined by StoredPage
		rawDataOut.setPosition(ALLOC_PAGE_HEADER_OFFSET);
		logicalDataOut.writeLong(nextAllocPageNumber);
		logicalDataOut.writeLong(nextAllocPageOffset);
		logicalDataOut.writeLong(0); // reserved1
		logicalDataOut.writeLong(0); // reserved2
		logicalDataOut.writeLong(0); // reserved3
		logicalDataOut.writeLong(0); // reserved4
	}

	private AllocExtent readExtent(int offset)
		 throws IOException, ClassNotFoundException
	{
		ArrayInputStream lrdi = rawDataIn;
		rawDataIn.setPosition(offset);
		AllocExtent newExtent = new AllocExtent();
		newExtent.readExternal(lrdi);

		// in 1.3 or later, make sure the upgrade from before 1.3 is legal.
		if (SanityManager.DEBUG)
		{
			int max_range = newExtent.MAX_RANGE(getMaxFreeSpace());
			long extent_start = newExtent.getFirstPagenum();
			long extent_end = newExtent.getExtentEnd();

			// extent_start + max_range - 1 is the absolute last page this
			// extent can hope to manage.  See if it did the calculation
			// correctly after upgrade.

            if ((extent_start+max_range-1) < extent_end)
            {
                SanityManager.THROWASSERT(
                    "extent range exceed what extent's space can handle ");
            }
		}

		return newExtent;
	}

	private void writeExtent(int offset) throws IOException
	{
		// rawDataOut and logicalDataOut are defined by StoredPage
		rawDataOut.setPosition(offset);
		extent.writeExternal(logicalDataOut);
	}

	/*
	 * borrowed space management
	 */

	/**
		Write the container information into the container information area.

		@param containerInfo the container information

		@param epage the allocation page data which may not be fully formed,
		but is guarenteed to be big enough to cover the area inhibited by the
		container info

		@param create if create, write out the length of the container info
		also. Else check to make sure epage's original container info is of the
		same length

		@exception StandardException Cloudscape standard error policy
	*/
	public static void WriteContainerInfo(byte[] containerInfo,
										  byte[] epage,
										  boolean create)
		 throws StandardException
	{
		int N = (containerInfo == null) ? 0 : containerInfo.length;

		if (SanityManager.DEBUG)
		{
			if (create)
				SanityManager.ASSERT(
                    containerInfo != null, "containerInfo is null");

			SanityManager.ASSERT(epage != null, "page array is null");

            if (!((containerInfo == null) ||
                  ((containerInfo.length + BORROWED_SPACE_OFFSET + 
                      BORROWED_SPACE_LEN) < epage.length))) 
            {
                SanityManager.THROWASSERT(
                    "containerInfo too big for page array: " + 
                    containerInfo.length);
            }

			if (BORROWED_SPACE_OFFSET + BORROWED_SPACE_LEN + N >=
															MAX_BORROWED_SPACE)
				SanityManager.THROWASSERT(
								 "exceed max borrowable space: " + N);
        }

        if ((N + BORROWED_SPACE_LEN + BORROWED_SPACE_OFFSET) > epage.length)
        {
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT(
				   "exceed max borrowable space on page: " + N);
		}

		if (create)
		{
			epage[BORROWED_SPACE_OFFSET] = (byte)N;
		}
		else
		{
			int oldN = (int)epage[BORROWED_SPACE_OFFSET];
			if (oldN != N)
            {
				throw StandardException.newException(
                        SQLState.DATA_CHANGING_CONTAINER_INFO, 
                        new Long(oldN), 
                        new Long(N));
            }
		}

		if (N != 0)
			System.arraycopy(containerInfo, 0, epage,
							 BORROWED_SPACE_OFFSET + BORROWED_SPACE_LEN,
							 N);
	}

	/**
		Extract the container information from epage.

		@param containerInfo where to put the extracted information

		@param epage the allocation page which has the container information.
		Epage may not be fully formed, but is guarenteed to be big enough to
		cover the area inhibited by the container info
	*/
	public static void ReadContainerInfo(byte[] containerInfo,
										 byte[] epage)
	{
		int N = (int)epage[BORROWED_SPACE_OFFSET];

		if (SanityManager.DEBUG)
		{
			if (N != containerInfo.length)
				SanityManager.THROWASSERT("N not what is expected : " +  N);

			if (BORROWED_SPACE_OFFSET + BORROWED_SPACE_LEN + N
								 						>= MAX_BORROWED_SPACE)
            {
				SanityManager.THROWASSERT("exceed max borrowable space: " + N);
            }
		}

		if (N != 0)
			System.arraycopy(epage, BORROWED_SPACE_OFFSET+BORROWED_SPACE_LEN,
							 containerInfo, 0, N);
	}


	/*
	 * specific methods to AllocPage
	 */

	/**
		Return the next free page number after given page number 
	 */
	public long nextFreePageNumber(long pnum)
	{
		return extent.getFreePageNumber(pnum);
	}


	/**
		Add a page which is managed by this alloc page.
		Return the page number of the newly added page.

		<BR> MT - thread aware (latched)

		@param container (future) allows the alloc page to call back to the
			container to grow the container by creating and syncing multiple
			pages at once
		@param ntt the nested top action that is the allocation transaction.
			NTT will comit before the user transaction
		@param userHandle the container handle that is opened by the user
			transaction.  Use the userHandle to latch the new page so that
			it may remain latched after NTT is committed so the user
			transaction can guarentee to have an empty page

		@exception StandardException If the page cannot be added
	*/
	public void addPage(FileContainer mycontainer, long newPageNumber, 
						RawTransaction ntt, BaseContainerHandle userHandle) throws StandardException
	{
		// RESOLVED:
		// 
		// to prevent allocating a free page before the freeing transaction has
		// commit, need to grab the DEALLOCATE_PROTECTION_HANDLE
		// the lock probably should be gotten in FileContainer 
		// and not here

		// page allocation is logged under the nested top action
		owner.getAllocationActionSet().
			actionAllocatePage(
                ntt, this, newPageNumber, 
                AllocExtent.ALLOCATED_PAGE, AllocExtent.FREE_PAGE);
	}


	/*
		Deallocate page
	*/
	public void deallocatePage(BaseContainerHandle userHandle, long pageNumber)
		 throws StandardException
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
		}

		// RESOLVED:
		//
		// to prevent this page from being freed before this transaction commits,
		// need to grab the DEALLOCATE_PROTECTION_HANDLE lock on the
		// deallocated page
		// the lock probably should be gotten in FileContainer 
		// and not here

		owner.getAllocationActionSet().
			actionAllocatePage(userHandle.getTransaction(),
							   this, pageNumber, AllocExtent.DEALLOCATED_PAGE,
							   AllocExtent.ALLOCATED_PAGE); 
	}

	/*
	 * update unfilled page information
	 * We will be using inputExtent's unfilledPage bitmap as the new bitmap, so
	 * caller of this routine need to not touch the bitmap after this call (in
	 * other words, call this ONLY in allocationCache invalidate and throw away
	 * the reference to the bitImpl)
	 */
	protected void updateUnfilledPageInfo(AllocExtent inputExtent) 
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched());
		}

		// update the unfilled page bit map unlogged - it is just a hint, not
		// worth logging it - don't dirty the page either, since we didn't log
		// it.  It will be dirtied soon enough by addPage or deallocPage,
		// that is the only reasons why we are invalidataing the
		// allocation cache and updating the unfilled page info. 
		// If we dirty the page, the BI will be copied to the side log
		extent.updateUnfilledPageInfo(inputExtent);

	}

	public boolean canAddFreePage(long lastAllocatedPage)
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(isLatched());

		if (extent.isRetired())
			return false;

		// if we want to try allocating not from the beginning of the bit map
		// and this alloc page is before that point and this is not the last
		// alloc page, then skip over this alloc page
		if (lastAllocatedPage != ContainerHandle.INVALID_PAGE_NUMBER && 
			extent.getLastPagenum() <= lastAllocatedPage && 
			!isLast())
			return false;

		// Else we either want to start examining from this alloc page, or this
		// is the last page, see if we can add a page.
		return extent.canAddFreePage(lastAllocatedPage);
	}

	public long getNextAllocPageOffset()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(
                !isLast(), "next alloc page not present for last alloc page");

			SanityManager.ASSERT(isLatched());
		}

		return nextAllocPageOffset;
	}

	public void chainNewAllocPage(BaseContainerHandle allocHandle,
								  long newAllocPageNum, long newAllocPageOffset)
		 throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(isLatched());
			if (SanityManager.DEBUG_ON(FileContainer.SPACE_TRACE))
				SanityManager.DEBUG(FileContainer.SPACE_TRACE,
									"chaining new alloc page " +
									newAllocPageNum + " to " +
									getPageNumber());
		}

		owner.getAllocationActionSet().
			actionChainAllocPage(allocHandle.getTransaction(),
								 this, newAllocPageNum, newAllocPageOffset);
	}

	public long getNextAllocPageNumber()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(isLatched());
			SanityManager.ASSERT(
                !isLast(), "next alloc page not present for last alloc page");
		}
		return nextAllocPageNumber;
	}

	public boolean isLast()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(isLatched());
		return nextAllocPageNumber == ContainerHandle.INVALID_PAGE_NUMBER;
	}

	/*
	 * get the last pagenumber currently managed by this alloc page
	 */
	public long getLastPagenum()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(isLatched());

		return extent.getLastPagenum();
	}

	/*
	 * get the largest page number this alloc page can manage. 
	 * This is the different from the last pagenumber currently managed by this
	 * alloc page unless the alloc page is full and all the pages have been
	 * allocated 
	 */
	public long getMaxPagenum()
	{
		return extent.getExtentEnd();
	}

	/*
	 * get the last preallocated pagenumber managed by this alloc page
	 */
	protected long getLastPreallocPagenum()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(isLatched());

		return extent.getLastPreallocPagenum();
	}


	protected int getPageStatus(long pageNumber)
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(isLatched());

		return extent.getPageStatus(pageNumber);
	}


	/**
		Do the actual page allocation/deallocation/ree underneath a log operation.
		Change the page status to new status

		@exception StandardException If the page cannot be allocated
	*/
	protected void setPageStatus(LogInstant instant, long pageNumber, int newStatus) throws StandardException
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(isLatched(), "page is not latched");
			SanityManager.ASSERT(extent != null, "extent is null");
		}

		logAction(instant);

		switch(newStatus)
		{
		case AllocExtent.ALLOCATED_PAGE:
			extent.allocPage(pageNumber);
			break;
		case AllocExtent.DEALLOCATED_PAGE:
			extent.deallocPage(pageNumber);
			break;
		case AllocExtent.FREE_PAGE:
			extent.deallocPage(pageNumber);
			break;
		}
	}

	/**
		Chain the next page number and offset underneath a log record

		@exception StandardException Standard Cloudscape error policy
	*/
	protected void chainNextAllocPage(LogInstant instant,
									  long newAllocPageNum,
									  long newAllocPageOffset)
		 throws StandardException
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(isLatched(), "page is not latched");

		logAction(instant);

		nextAllocPageNumber = newAllocPageNum;
		nextAllocPageOffset = newAllocPageOffset;
	}

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			String str = "*** Alloc page ***\n" + super.toString();
			return str;
		}
		else
			return null;
	}


	/**
		Return a copy of the allocExtent to be cached by the container.
		the container must take care to maintain its coherency by
		invalidating the cache before any update.
	*/
	protected AllocExtent getAllocExtent()
	{
		return extent;

		// return new AllocExtent(extent);
	}

	/**
		Preallocate user page if needed.

		@param myContainer the container object
		@param preAllocThreshold start preallocating after this threshold
		@param preAllocSize preallocate this number of pages 
	*/
	protected void preAllocatePage(FileContainer myContainer,
								   int preAllocThreshold,
								   int preAllocSize)
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(isLatched(), "page is not latched");

		long lastPreallocatedPagenum = extent.getLastPreallocPagenum();

		if (lastPreallocatedPagenum < preAllocThreshold)
			return;

		// don't pre-allocate more than we the extent can handle - this is
		// because if I preallocate the next alloc page as a stored page,
		// that's going to be problem when we try to get it as an alloc page
		// later.  We don't handle changing from a store page type to an alloc
		// page type on disk very well.
		if (extent.getExtentEnd() < (lastPreallocatedPagenum+preAllocSize))
			preAllocSize = (int)(extent.getExtentEnd() - lastPreallocatedPagenum);

		if (preAllocSize <= 0)
			return;

		// pre-allocate - only a container knows how to write pages
		// preAllocSize may exceed what this allocation page can really
		// handle, but no harm done.  The next allocation page will benefit
		// from the work we have done...
		int n = myContainer.preAllocate(lastPreallocatedPagenum, preAllocSize);
		
		if (n > 0) 				// successfully preallocated some pages
		{
			// this is purely a performance issue during runtime.  During
			// recovery, any page that is actually initialized will have its
			// own initPage log record.  Update extent's preAllocpageNumber
			// unlogged.
			//
			// We could have logged a redo-only log record, but we are counting
			// on myContainer.preAllocate to do the right thing if we recovered
			// and have out of date preallocate information.  A reason why
			// logging this is undesirable is that the alloc page may think the
			// preallocation happened, but the container may actually choose to
			// lie about it - if it thinks there is no advantage in actually
			// doing the I/O now.  So best to leave it alone.
			extent.setLastPreallocPagenum(lastPreallocatedPagenum + n);

			// don't dirty the page - the new preAlloc page number is only set
			// in memory.  A page should only get 'dirtied' by a log operation,
			// we are cheating here.  Same with updating the extentStatus bit
			// without logging.
		}

	}



	/*********************************************************************
	 * Extent Testing
	 *
	 * Use these strings to simulate error conditions for
	 * testing purposes.
	 *
	 *********************************************************************/
	public static final String TEST_MULTIPLE_ALLOC_PAGE = SanityManager.DEBUG ? "TEST_MULTI_ALLOC_PAGE" : null;

}

