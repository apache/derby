/*

   Derby - Class org.apache.derby.impl.store.raw.data.AllocationCache

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
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.raw.ContainerHandle;

import org.apache.derby.impl.store.raw.data.BaseContainerHandle;
/**
	An auxiliary object to cache the allocation information for a file container.
	<B>Only a FileContainer should use this object</B>
	<P>
	The allocation cache contains an array of AllocExtents and 3 arrays of longs:
	<OL><LI>ExtentPageNums[i] is the page number of the i'th extent
	<LI>lowRange[i] is the smallest page number managed by extent i
	<LI>hiRange[i] is the largest page number managed by extent i
	</OL>
	<P>
	Note thate extentPageNums and lowRange does not change once the extent has
	been created, but hiRange will change for the last extent as more pages are
	allocated.
	<P>
	Extents can be individually invalidated or the entire cache (all extends)
	can be invalidated at once.
	<P> MT - unsafe
    Synrhonized access to all methods must be enforced by the caller of 
	AllocationCache
*/
class AllocationCache
{
	private int numExtents;
	private long[] lowRange;	// lowRange[i] to hiRange[i] defines the
	private long[] hiRange;		// smallest and largest logical page number
								// manages by extent i

	private boolean[] isDirty;	// changes to the in memory allocation cache
	private AllocExtent[] extents;
	private long[] extentPageNums;
	private boolean isValid;

	protected AllocationCache()
	{
		numExtents = 0;
		isValid = false;
	}

	/* reset the allocation cache in case when filecontainer object is reused */
	protected void reset()
	{
		numExtents = 0;
		isValid = false;

		if (lowRange != null)
		{
			for (int i = 0; i < lowRange.length; i++)
			{
				lowRange[i] = ContainerHandle.INVALID_PAGE_NUMBER;
				hiRange[i] = ContainerHandle.INVALID_PAGE_NUMBER;
				extentPageNums[i] = ContainerHandle.INVALID_PAGE_NUMBER;
				extents[i] = null;
				isDirty[i] = false;
			}
		}
	}

	/**
	  Get the page number for the allocation page that is managing this page number
	*/
	protected long getAllocPageNumber(BaseContainerHandle handle, 
								   long pageNumber, 
								   long firstAllocPageNumber)
		 throws StandardException
	{
		// try to see if we can figure this out without validating the cache
		for (int i = 0; i < numExtents; i++)
		{
			if (lowRange[i] <= pageNumber && pageNumber <= hiRange[i])
				return extentPageNums[i];
		}

		if (!isValid)
		{
			/* can't find the page. Validate the cache first, then try to find it again */
			validate(handle, firstAllocPageNumber);
			
			for (int i = 0; i < numExtents; i++)
			{
				if (lowRange[i] <= pageNumber && pageNumber <= hiRange[i])
					return extentPageNums[i];
			}
		}
		return ContainerHandle.INVALID_PAGE_NUMBER;
	}

	/**
	  Get the last (allocated) page of the container
	  */
	protected long getLastPageNumber(BaseContainerHandle handle, long firstAllocPageNumber)
		 throws StandardException
	{
		if (!isValid)
			validate(handle, firstAllocPageNumber);
		return hiRange[numExtents-1];
	}

	/**
	  Set the page number to be unfilled
	 */
	protected void trackUnfilledPage(long pagenumber, boolean unfilled)
	{
		// do not validate the alloc cache just for the purpose of updating the
		// unfilled bit
		if (!isValid ||  numExtents <= 0)
		{
			return;
		}

		// we are calling this without getting the allocCache semaphore - be
		// careful that extents[i] will go null at any time.
		for (int i = 0; i < numExtents; i++)
		{
			if (lowRange[i] <= pagenumber && pagenumber <= hiRange[i])
			{
				AllocExtent ext = extents[i];
				if (ext != null &&
					ext.trackUnfilledPage(pagenumber, unfilled) &&
					extents[i] != null)
				{
					isDirty[i] = true;
				}
					
				break;
			}
		}
	}

	protected long getUnfilledPageNumber(BaseContainerHandle handle, 
										 long firstAllocPageNumber,
										 long pagenum)
		 throws StandardException
	{
		// get the next unfilled page number
		if (!isValid)
		{
			validate(handle, firstAllocPageNumber);
		}

		if (pagenum == ContainerHandle.INVALID_PAGE_NUMBER)
		{
			for (int i = 0; i < numExtents; i++)
			{
				if (extents[i] != null)
					return extents[i].getUnfilledPageNumber(pagenum);
			}
		}
		else
		{
			for (int i = 0; i < numExtents; i++)
			{
				if (pagenum <= hiRange[i])
				{
					if (extents[i] != null)
						return extents[i].getUnfilledPageNumber(pagenum);
				}
			}
		}

		return ContainerHandle.INVALID_PAGE_NUMBER;
	}

    /**
    returns estimated number of allocated pages
    **/
	protected long getEstimatedPageCount(BaseContainerHandle handle,
										 long firstAllocPageNumber)
		 throws StandardException
	{
		if (!isValid)
			validate(handle, firstAllocPageNumber);

		long estPageCount = 0;

		for (int i = 0; i < numExtents; i++)
		{
			if (extents[i] != null)
				estPageCount += extents[i].getAllocatedPageCount();
		}
		return estPageCount;
	}


    protected SpaceInformation getAllPageCounts(
        BaseContainerHandle handle,
        long firstAllocPageNumber)
		    throws StandardException
    {
        long currAllocPages = 0;
        long numAllocatedPages = 0;
        long numFreePages = 0;
        long numUnfilledPages = 0;

		if (!isValid)
			validate(handle, firstAllocPageNumber);

		for (int i = 0; i < numExtents; i++)
		{
			if (extents[i] != null)
            {
                currAllocPages = extents[i].getAllocatedPageCount();
				numAllocatedPages += currAllocPages;
                numUnfilledPages += extents[i].getUnfilledPageCount();
                numFreePages += (extents[i].getTotalPageCount() - currAllocPages);
            }

            if (SanityManager.DEBUG)
                SanityManager.ASSERT(numUnfilledPages <= numAllocatedPages,
                    "more unfilled pages than allocated pages on extent ");
		}
        return new SpaceInformation(
            numAllocatedPages,
            numFreePages,
            numUnfilledPages);
    }


	/* invalidate all extents */
	protected void invalidate()
	{
		if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON(FileContainer.SPACE_TRACE))
            {
                SanityManager.DEBUG(
                    FileContainer.SPACE_TRACE, "alloc cache invalidated");
            }
        }


		for (int i = 0; i < numExtents; i++)
		{
			isDirty[i] = false;
			extents[i] = null;
		}

		isValid = false;

	}

	/* invalidate the extent that is managed by this alloc page */
	protected void invalidate(AllocPage allocPage, long allocPagenum)
		 throws StandardException
	{
		if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON(FileContainer.SPACE_TRACE))
            {
                SanityManager.DEBUG(
                    FileContainer.SPACE_TRACE, "alloc cache for page " + 
                    allocPagenum + " invalidated");
            }
        }

		isValid = false;

		if (numExtents == 0)
			return;

		for (int i = 0; i < numExtents; i++)
		{
			if (extentPageNums[i] == allocPagenum)
			{
				// update unfilled page info
				if (allocPage != null && extents[i] != null &&
					isDirty[i])
				{
					// replace unFilledPage bitmap with the one in the allocation
					// cache, which has the more current information
					// call this ONLY in invalidate, when the reference to the
					// extent is about to be nulled out
					allocPage.updateUnfilledPageInfo(extents[i]);
					isDirty[i] = false;
				}

				extents[i] = null;
				return;
			}
		}

		// handle the case where a new alloc page that has never been entered
		// into the cache is asked to be invalidated
		if (allocPagenum > hiRange[numExtents-1])
			return;

		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("cannot find extent managed by " +
									  allocPagenum);


	}

	/* invalidate the last extent */
	protected void invalidateLastExtent()
	{
		if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON(FileContainer.SPACE_TRACE))
            {
                SanityManager.DEBUG(
                    FileContainer.SPACE_TRACE,
                        "last extent (" + extentPageNums[numExtents-1] + 
                            ") invalidated");
            }
        }

		isValid = false;

		if (numExtents > 0)
			extents[numExtents - 1] = null;
	}

	/**
	  Get the last valid page of the file container.  A valid page is one that
	  is not deallocated or freed.
	*/
	protected long getLastValidPage(BaseContainerHandle handle,
									long firstAllocPageNumber)
		 throws StandardException
	{
		AllocExtent extent = null;
		int extentNumber;
		long lastValidPageNumber = ContainerHandle.INVALID_PAGE_NUMBER;

		if (!isValid)
			validate(handle, firstAllocPageNumber);

		if (numExtents == 0)	// no extent at all, no page in the container
			return ContainerHandle.INVALID_PAGE_NUMBER;

		// start from the last extent, goes backward till a valid page is found

		for (extentNumber = numExtents-1;
			 extentNumber >= 0;
			 extentNumber--)
		{
			extent = extents[extentNumber];
			lastValidPageNumber = extent.getLastValidPageNumber();
			if (lastValidPageNumber != ContainerHandle.INVALID_PAGE_NUMBER)
				break;
		}
		return lastValidPageNumber;
	}

	/*
	  Get the next page (after pageNumber) that is valid 
	  */
	protected long getNextValidPage(BaseContainerHandle handle, 
									long pageNumber, 
									long firstAllocPageNumber)
		 throws StandardException
	{
		int extentNumber;

		if (!isValid)
			validate(handle, firstAllocPageNumber);

		if (numExtents == 0)	// no extent at all, no page in the container
			return ContainerHandle.INVALID_PAGE_NUMBER;

		// find the extent whose hiRange is > pageNumber.  Most of the time,
		// this is the extent this pageNumber is in, but some times, when
		// pageNumber == hiRange of extent i, extent i+1 is found.
		AllocExtent extent = null;
		for (extentNumber = 0; extentNumber < numExtents; extentNumber++)
		{
			if (pageNumber < hiRange[extentNumber])
			{
				extent = extents[extentNumber];
				break;
			}
		}

		if (extent == null)		// extent has been invalidated or not there
		{
			// the cache is valid and up to date, 
			// the only reason why we cannot find an extent is if this is the
			// last valid page of the container
			return ContainerHandle.INVALID_PAGE_NUMBER;
		}

		// extent == extents[extentNumber]
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(extent == extents[extentNumber]);

		// we found an extent which may contain a valid page that is of higher
		// pagenumber than the passed in page number.  Still need to walk the
		// extent array to make sure 

		long nextValidPage = ContainerHandle.INVALID_PAGE_NUMBER;

		while(extentNumber < numExtents)
		{
			extent = extents[extentNumber] ;
			nextValidPage = extent.getNextValidPageNumber(pageNumber);
			if (nextValidPage != ContainerHandle.INVALID_PAGE_NUMBER)
				break;

			extentNumber++;
		}
		return nextValidPage;

	}

	/**
	  Get the page status of a page
	*/
	protected int getPageStatus(BaseContainerHandle handle, long pageNumber,
								long firstAllocPageNumber)
		 throws StandardException
	{
		AllocExtent extent = null;

		for (int i = 0; i < numExtents; i++)
		{
			if (lowRange[i] <= pageNumber && pageNumber <= hiRange[i])
			{
				extent = extents[i];
				break;
			}
		}

		if (extent == null)
		{
			if (SanityManager.DEBUG) {
				if (isValid) {

					SanityManager.DEBUG_PRINT("trace", "Allocation cache is " + 
								(isValid ? "Valid" : "Invalid"));
					
					for (int i = 0; i < numExtents; i++) {
						SanityManager.DEBUG_PRINT("trace", "Extent " + i + " at " + extentPageNums[i] +
										" range is " + lowRange[i] + " to " + hiRange[i]);
						if (extents[i] == null)
							SanityManager.DEBUG_PRINT("trace", "extent is null");
						else
							SanityManager.DEBUG_PRINT("trace", extents[i].toDebugString());

					}

					SanityManager.THROWASSERT("valid cache cannot find page "+pageNumber);
				}
			}

			if (!isValid)
				validate(handle, firstAllocPageNumber);
			// try again

			for (int i = 0; i < numExtents; i++)
			{
				if (lowRange[i] <= pageNumber && pageNumber <= hiRange[i])
				{
					extent = extents[i];
					break;
				}
			}

			if (SanityManager.DEBUG)
				if (extent == null)
					SanityManager.THROWASSERT("valid cache cannot find page " +
												pageNumber);
		}

		return extent.getPageStatus(pageNumber);
	}

	/**
	  Validate the cache, find all alloc pages and fill in the arrays
	  */
	private void validate(BaseContainerHandle handle, long firstAllocPageNumber)
		 throws StandardException
	{
		if (numExtents == 0)	// never been initialized, read it all in
		{
			long pagenum = firstAllocPageNumber;

			while(!isValid)
			{
				growArrays(++numExtents);

				Object obj = handle.getAllocPage(pagenum);

				if (SanityManager.DEBUG)
				{
					if (obj == null)
						SanityManager.THROWASSERT(
							"cannot find " + numExtents +
							" alloc page at " + pagenum);
					if ( ! (obj instanceof AllocPage))
						SanityManager.THROWASSERT(
							"page at " + pagenum +
							" is not an allocPage, is a " +
							obj.getClass().getName());
				}	

				AllocPage allocPage = (AllocPage)obj;
				setArrays(numExtents-1, allocPage);

				if (allocPage.isLast())
					isValid = true;
				else
					// get next alloc page
					pagenum = allocPage.getNextAllocPageNumber();

				allocPage.unlatch();
			}
		}
		else		// has been initialized before, but is now invalidated
		{
			for (int i = 0; i < numExtents-1; i++)
			{
				if (extents[i] == null)	// reinitialize this extent
				{
					AllocPage allocPage = 
                        (AllocPage)handle.getAllocPage(extentPageNums[i]);

					setArrays(i, allocPage);

					if (SanityManager.DEBUG)
					{
						if (i < numExtents-1)
                        {
                            if (extentPageNums[i+1] != 
                                    allocPage.getNextAllocPageNumber())
                            {
                                SanityManager.THROWASSERT(
                                    "bad alloc page - " +
                                    ";extentPageNums[i+1] = " + 
                                        extentPageNums[i+1] +
                                    ";allocPage.getNextAllocPageNumber() = " + 
                                        allocPage.getNextAllocPageNumber());
                            }
                        }
					}

					allocPage.unlatch();
				}
			}
			// always get the last alloc page to see if the number of alloc
			// pages remain the same
			long pagenum = extentPageNums[numExtents-1];
			while (!isValid)
			{
				AllocPage allocPage = (AllocPage)handle.getAllocPage(pagenum);

				if (extents[numExtents-1] == null)
					setArrays(numExtents-1, allocPage);

				if (!allocPage.isLast())
				{
					growArrays(++numExtents);
					pagenum = allocPage.getNextAllocPageNumber();
				}
				else
					isValid = true;

				allocPage.unlatch();
			}
		}
	}

	/* shorthand to set the 4 array values */
	private void setArrays(int i, AllocPage allocPage)
	{
		if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON(FileContainer.SPACE_TRACE))
            {
                SanityManager.DEBUG(
                    FileContainer.SPACE_TRACE, 
                    "Alloc page " + i + " at " + allocPage.getPageNumber() + 
                    " updated");
            }
        }

		AllocExtent extent = allocPage.getAllocExtent();
		extents[i] = extent;
		lowRange[i] = extent.getFirstPagenum();
		hiRange[i] = extent.getLastPagenum();
		extentPageNums[i] = allocPage.getPageNumber();
	}

	/* shorthand to grow the 4 arrays to the desired size */
	private void growArrays(int size)
	{
		int oldLength;

		if (lowRange == null || lowRange.length == 0)
			oldLength = 0;
		else 
			oldLength = lowRange.length;

		if (oldLength >= size)	// no need to grow
			return;

		long[] saveLow = lowRange;
		long[] saveHi = hiRange;
		AllocExtent[] saveExtents = extents;
		boolean[] saveDirty = isDirty;
		long[] savePageNums = extentPageNums;

		lowRange = new long[size];
		hiRange = new long[size];
		isDirty = new boolean[size];
		extents = new AllocExtent[size];
		extentPageNums = new long[size];

		if (oldLength > 0)
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(oldLength == saveHi.length);
				SanityManager.ASSERT(oldLength == saveExtents.length);
				SanityManager.ASSERT(oldLength == savePageNums.length);
			}
			System.arraycopy(saveLow, 	  0, lowRange, 	0, saveLow.length);
			System.arraycopy(saveHi,	  0, hiRange, 	0, saveHi.length);
			System.arraycopy(saveDirty,	  0, isDirty,   0, saveDirty.length);
			System.arraycopy(saveExtents, 0, extents,	0, saveExtents.length);
			System.arraycopy(savePageNums,0,extentPageNums,0, savePageNums.length);
		}

		for (int i = oldLength; i < size; i++)
		{
			lowRange[i] = ContainerHandle.INVALID_PAGE_NUMBER;
			hiRange[i] = ContainerHandle.INVALID_PAGE_NUMBER;
			isDirty[i] = false;
			extentPageNums[i] = ContainerHandle.INVALID_PAGE_NUMBER;
			extents[i] = null;
		}
	}

	/** 
	  dump the allocation cache information
	*/
	protected void dumpAllocationCache()
	{
		if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON(FileContainer.SPACE_TRACE))
            {
                SanityManager.DEBUG(FileContainer.SPACE_TRACE, 
                                    "Allocation cache is " + 
                                    (isValid ? "Valid" : "Invalid"));
                for (int i = 0; i < numExtents; i++)
                {
                    SanityManager.DEBUG(
                        FileContainer.SPACE_TRACE, 
                        "Extent " + i + " at " + extentPageNums[i] +
                        " range is " + lowRange[i] + " to " + hiRange[i]);

                    if (extents[i] == null)
                    {
                        SanityManager.DEBUG(
                            FileContainer.SPACE_TRACE, "extent is null");
                    }
                    else
                    {
                        SanityManager.DEBUG(
                            FileContainer.SPACE_TRACE, 
                            extents[i].toDebugString());
                    }
                }
            }
        }
	}

}

