/*

   Derby - Class org.apache.derby.impl.store.raw.data.AllocExtent

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

import org.apache.derby.iapi.services.io.FormatableBitSet;

import java.io.Externalizable;
import java.io.IOException;

import java.io.ObjectOutput;
import java.io.ObjectInput;

/**

	An allocation extent row manages the page status of page in the extent.
	AllocExtent is externalizable and is written to the AllocPage directly,
	without being converted to a row first.
	<P>
	<PRE>
	@format_id	none, format implied by AllocPage's format
	@purpose	manage page status of page in extent
	@upgrade
	@disk_layout
		extentOffset(long) the begin physical byte offset of the first page of this extent
		extentStart(long) the first logical page mananged by this extent.
		extentEnd(long) the last page this extent can ever hope to manage 
		extentLength(int) the number of pages allocated in this extent
		extentStatus(int) status bits for the whole extent.
				HAS_DEALLOCATED - most likely, this extent has a deallocated 
                        page somewhere
						If !HAD_DEALLOCATED, the extent has no deallocated page
				HAS_FREE - most likely, this extent has a free page somewhere
						If !HAS_FREE, there is no free page in the extent
				ALL_FREE - most likely, this extent only has free pages, good 
                        candidate for shrinking the file.
						If !ALL_FREE, the extent is not all free
				HAS_UNFILLED_PAGES - most likely, this extent has unfilled pages.
						if !HAS_UNFILLED_PAGES, all pages are filled
				KEEP_UNFILLED_PAGES - this extent keeps track of unfilled pages
						(post v1.3).  If not set, this extent has no notion of
						unfilled page and has no unFilledPage bitmap.
				NO_DEALLOC_PAGE_MAP - this extents do not have a dealloc and a
						free page bit maps.  Prior to 2.0, there are 2 bit
						maps, a deallocate page bit map and a free page bit
						map.  Cloudscape 2.0 and later merged the dealloc page
						bit map into the free page bit map.
				RETIRED - this extent contains only 'retired' pages, never use 
                        any page from this extent.  The pages don't actually 
                        exist, i.e., it maps to nothing (physicalOffset is 
                        garbage).  The purpose of this extent is to blot out a 
                        range of logical page numbers that no longer exists 
                        for this container.  Use this to reuse a physical page
                        when a logical page has exhausted all recordId or for
                        logical pages that has been shrunk out.
		preAllocLength(int)  the number of pages that have been preallocated
		reserved1(int)
		reserved2(long)	reserved for future use
		reserved3(long)	reserved for future use
		FreePages(bit)	bitmap of free pages
				Bit[i] is ON iff page i is free for immediate (re)use.
		[
		    on disk version before 2.0
				deAllocPages(bit) bitmap of deallocated pages
				Bit[i] is ON iff page i has been deallocated.
		]
		unFilledPages(bit)	bitmap of pages that has free space
				Bit[i] is ON if page i is likely to be < 1/2 full

		org.apache.derby.iapi.services.io.FormatableBitSet is used to store the bit map.  
            FormatableBitSet is an externalizable class.

	@end_format

	<PRE>
	A page can have the following logical state:
	<BR>Free - a page that is free to be used
	<BR>Valid - a page that is currently in use
	<P>
	There is another type of transitional pages which pages that have been
	allocated on disk but has not yet been used.  These pages are Free.
	<P>
	Bit[K] freePages
		Bit[i] is ON iff page i maybe free for reuse.  User must get the
		dealloc page lock on the free page to make sure the transaction.
	<P>
	K is the size of the bit array, it must be >= length.

	@see AllocPage
*/


public class AllocExtent implements Externalizable
{
	private long extentOffset;	// begin physical offset
	private long extentStart;	// first logical page number
	private long extentEnd;		// last logical page number
	// page[extentStart] to page[extentEnd] are the pages in the range of this
	// alloc extent.
	// page[exentStart] to page[extentStart+extentLength-1] are actually
	// allocated in this extent
	// when the extent is completely allocated,
	// extentEnd == extentStart+extentLength -1

	private int extentLength;	// number of pages allocated in the extent

	int extentStatus;

	private int preAllocLength;

	private int reserved1;
	private long reserved2;
	private long reserved3;

	// extent Status bits
	private static final int HAS_DEALLOCATED = 0x1;
	private static final int HAS_FREE = 0x2;
	private static final int ALL_FREE = 0x4;
	private static final int HAS_UNFILLED_PAGES = 0x10;
	private static final int KEEP_UNFILLED_PAGES = 0x10000000;
	private static final int NO_DEALLOC_PAGE_MAP = 0x20000000;
	private static final int RETIRED = 0x8;

	/**
		public Per Page status
	*/
	protected static final int ALLOCATED_PAGE = 0;
	protected static final int DEALLOCATED_PAGE = 1;
	protected static final int FREE_PAGE = 2;


	// a page which is not a freePage is a regular old
	// allocated page.  Only an allocated page can be unFilled.
	FormatableBitSet freePages;
	FormatableBitSet unFilledPages;

	/**
		Statically calculates how many pages this extent can manage given the
		availspace number of bytes to store this extent in

		if read/writeExternal changes, this must change too
	*/
	protected static int MAX_RANGE(int availspace)
	{
		/* extent Offset, Start, End, Length, Status, preAllocLength, reserved1,2,3 */
		int bookkeeping = 	8 /* offset */ +
							8 /* start */ +
							8 /* end */ +
							4 /* length */ +
							4 /* status */ +
							4 /* preAllocLength */ +
							4 /* reserved1 */ +
							8 /* reserved2 */ +
							8 /* reserved3 */;
		availspace -= bookkeeping;

		// each bit array is allowed to the 1/3 the remaining space
		availspace /= 3;

		if (availspace <= 0)
			return 0;

		// ask bit array how many bits it can store in this amount of space
		return FormatableBitSet.maxBitsForSpace(availspace);
	}

	/*
	 * methods
	 */

	/*
	 * ctors
	 */
	protected AllocExtent(long offset, // physical offset
					   long start,  // starting logical page number
					   int length,  // how many pages are in this extent
					   int pagesize, // size of all the pages in the extent
					   int maxlength) // initial size of the bit map arrays
	{
		if (SanityManager.DEBUG)
		{
			if (length > maxlength)
				SanityManager.THROWASSERT(
							"length " + length + " > maxlength " + maxlength);
		}


		this.extentOffset = offset;
		this.extentStart = start;
		this.extentEnd = start+maxlength-1;

		this.extentLength = length;
		preAllocLength = extentLength;

		if (length > 0)
			extentStatus = HAS_FREE | ALL_FREE ;
		else
			extentStatus = 0;

		extentStatus |= KEEP_UNFILLED_PAGES; // v1.3 or beyond
		extentStatus |= NO_DEALLOC_PAGE_MAP; // v2.0 or beyond

		int numbits = (1+(length/8))*8;
		if (numbits > maxlength)
			numbits = maxlength;

		freePages = new FormatableBitSet(numbits);
		unFilledPages = new FormatableBitSet(numbits);

		// by definition, all pages are free to begin with, no pages are
		// deallocated and no page is unfilled
		for (int i = 0; i < length; i++)
			freePages.set(i);
	}

	/*
	  Copy constructor
	*/
	protected AllocExtent(AllocExtent original)
	{
		extentOffset = original.extentOffset;
		extentStart	 = original.extentStart;
		extentEnd	 = original.extentEnd;
		extentLength = original.extentLength;
		extentStatus = original.extentStatus;
		preAllocLength = original.preAllocLength;

		freePages = new FormatableBitSet(original.freePages);
		unFilledPages = new FormatableBitSet(original.unFilledPages);
	}


	/*
	 * Methods of Externalizable
	 */
	public AllocExtent()
	{
	}


	public void writeExternal(ObjectOutput out) throws IOException
	{

		// any change to this routine must change maxRange
		out.writeLong(extentOffset);
		out.writeLong(extentStart);
		out.writeLong(extentEnd);
		out.writeInt(extentLength);
		out.writeInt(extentStatus);
		out.writeInt(preAllocLength);
		out.writeInt(0);		// reserved1
		out.writeLong(0);		// reserved2
		out.writeLong(0);		// reserved3

		freePages.writeExternal(out);
		unFilledPages.writeExternal(out);
	}

	public void readExternal(ObjectInput in)
		 throws IOException, ClassNotFoundException
	{
		// any change to this routine must change maxRange
		extentOffset = in.readLong();
		extentStart	= in.readLong();
		extentEnd	= in.readLong();
		extentLength = in.readInt();
		extentStatus = in.readInt();
		preAllocLength = in.readInt();
		reserved1 = in.readInt();
		reserved2 = in.readLong();
		reserved3 = in.readLong();

		freePages = new FormatableBitSet();	// don't know how to point to it
		freePages.readExternal(in);

		// this extent is created before 2.0
		if ((extentStatus & NO_DEALLOC_PAGE_MAP) == 0)	
		{
			FormatableBitSet deAllocPages = new FormatableBitSet();
			deAllocPages.readExternal(in);
			// fold this into free page bit map
			freePages.or(deAllocPages);
			extentStatus |= NO_DEALLOC_PAGE_MAP; // dealloc page map has been merged
		}

		if ((extentStatus & KEEP_UNFILLED_PAGES) == KEEP_UNFILLED_PAGES)
		{
			unFilledPages = new FormatableBitSet();
			unFilledPages.readExternal(in);
		}
		else					// before we keep track of unfilled pages pre 1.3
		{
			// make sure there are enough space
			unFilledPages = new FormatableBitSet(freePages.getLength());
			extentStatus |= KEEP_UNFILLED_PAGES; // now we keep track of them
		}
		
	}


	/*
	 * methods specific to allocExtent
	 */


	/*
	 * write operation that is called underneath the log
	 *
	 * page goes thru the following transition:
	 * ALLOCATED_PAGE <-> deallocated page -> free page <-> ALLOCATED_PAGE
	 *
	 */

	/**
		Allocate this page - this is called underneath the log record

		@exception StandardException Standard Cloudscape error policy
	*/
	protected void allocPage(long pagenum) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			if (pagenum > getLastPagenum())
			{
				// adding a brand new page, it should be at most one off from the last page
				if (pagenum > extentEnd)
					SanityManager.THROWASSERT("pagenum " + pagenum +
									 " is out of beyond my range (" +
										extentStart + "," + extentEnd + ")");
				if (pagenum != getLastPagenum() + 1)
					SanityManager.THROWASSERT(
						"skipping pages, lastPageNumber = " + getLastPagenum() +
						 " pageNumber = " + pagenum + "\n");
			}
			else
			{
				// reuseing a page, make sure it is in range and is not already in use
				checkInRange(pagenum);

				int bitnum = (int)(pagenum-extentStart);

				// either the pagenum is now free (do) or deallocated (undo)
				if (!freePages.isSet(bitnum))
                {
					SanityManager.THROWASSERT(
                        "trying to re-allocate a page ( " +
                        pagenum + " ) that is already allocated ");
                }
			}
		}

		// don't know if we are redoing (from free -> valid)
		// or undoing (from dealloc -> valid), reset them both
		int bitnum = (int)(pagenum-extentStart);

		if (bitnum >= freePages.getLength())	// expand the bit map
		{
			int numbits = (1+(bitnum/8))*8;
			if (numbits > (int)(extentEnd - extentStart + 1))
				numbits = (int)(extentEnd - extentStart + 1);

			freePages.grow(numbits);
			unFilledPages.grow(numbits);
		}

		// the first page to be allocated has pagenum == extentStart.
		int numPageAlloced = (int)(pagenum-extentStart+1);

		if (numPageAlloced > extentLength)
		{
			extentLength = numPageAlloced;
		}

		freePages.clear(bitnum);

		// do not set the unfilled bit on a newly allocated page because
		// we only keep track of unfilled HEAD page, not unfilled overflow
		// page. 
	}

	/**
	    Deallocate logical page pagenum - this is called underneath the log record.
		pagenum must be a page managed by this extent and it must be valid

		@exception StandardException Standard Cloudscape error policy
	*/
	protected void deallocPage(long pagenum) throws StandardException
	{
		int bitnum = (int)(pagenum-extentStart);

		// the pagenum must now be either valid (do) or free (undo)
		if (SanityManager.DEBUG)
		{
			if (freePages.isSet(bitnum))
				SanityManager.THROWASSERT(
						"trying to deallocate a deallocated page " + pagenum);
		}

		freePages.set(bitnum);
		unFilledPages.clear(bitnum); // deallocated page is never unfilled

		setExtentFreePageStatus(true);
	}


	protected long getExtentEnd()
	{
		return extentEnd;
	}


	/*
	 * read operation that is called above the log
	 */

	/**
		Get a page number that is free
	*/
	protected long getFreePageNumber(long pnum)
	{
		// if we can reuse page, do so, otherwise add a brand new page
		if (mayHaveFreePage())
		{
			// The last allocated page may be from a previous alloc extent, but
			// if that extent is full and we are the first extent that can
			// accomodate a new page, we may be picked.  In that case, pnum may
			// be before the start of this extent.
			int i = (pnum < extentStart) ? freePages.anySetBit() : 
				freePages.anySetBit((int)(pnum-extentStart));

			if (i != -1)
			{
                if (SanityManager.DEBUG)
                {
 		    		if (i >= extentLength)
 		    			SanityManager.THROWASSERT("returned bit = " + i +
			    						 " extent length = " + extentLength);
			    }

				return i+extentStart;
			}

			// the hint is wrong, no free page in the extent
			// do this unlogged, it is just a hint, don't care if it is lost
			if (pnum < extentStart)
				setExtentFreePageStatus(false);
		}

		// maximally, we can have up to extendEnd page
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(extentStart+extentLength <= extentEnd);

		// need to add a brand new page, current end of extent is at page
		// extentStart+extentLength-1;
		return extentStart+extentLength;
	}


	/**
		Get the physical offset of pagenum.
		If deallocOK is true, then even if pagenum is deallocated, it is OK.
		If deallocOK is false, then an exception is thrown if pagenum is
		deallocated.

		An exception is always thrown if pagenum is a free page

		@exception StandardException Standard Cloudscape error policy
	 */
	protected long getPageOffset(long pagenum, int pagesize, boolean deallocOK) throws StandardException
	{
		return pagenum * pagesize;
	}

	/**
		Return the status of this extent
	*/
	protected boolean isRetired()
	{
		return ((extentStatus & RETIRED) != 0);
	}

	private boolean mayHaveFreePage()
	{
		return ((extentStatus & HAS_FREE) != 0);
	}

	private void setExtentFreePageStatus(boolean hasFree)
	{
		if (hasFree)
			extentStatus |= HAS_FREE;
		else
			extentStatus &= ~HAS_FREE;
	}

	protected boolean canAddFreePage(long lastAllocatedPage)
	{
		// the last page to be allocated == extentEnd
		if (extentStart + extentLength <= extentEnd)
			return true;

		// else, check to see if this may have any free page
		if (!mayHaveFreePage())
			return false;

		// we may have a free page, but that is not certain, double check
		if (lastAllocatedPage < extentStart)
			return (freePages.anySetBit() != -1);
		else
			return ((freePages.anySetBit((int)(lastAllocatedPage-extentStart))) != -1);
	}

	/**
		Return the status of a particular page
	*/
	protected int getPageStatus(long pagenum)
	{
		if (SanityManager.DEBUG)
			checkInRange(pagenum);

		int status = 0;
		int bitnum = (int)(pagenum-extentStart);

		if (freePages.isSet(bitnum))
			status = FREE_PAGE;
		else
			status = ALLOCATED_PAGE;

		return status;
	}


	/**
		Get the first logical page number managed by this extent.
	*/
	protected long getFirstPagenum()
	{
		return extentStart;
	}

	/**
		Get the last logical page number managed by this extent.
	*/
	protected long getLastPagenum()
	{
		return extentStart+extentLength-1;
	}


	/*
	 * page preallocation 
	 */

	/**
	 * get the last preallocated pagenumber managed by this alloc page
	 */
	protected long getLastPreallocPagenum()
	{
		if (extentLength > preAllocLength)
			preAllocLength = extentLength;

		return extentStart + preAllocLength - 1 ;
	}

	/**
		preallocated N pages, passed in the last preallocated page number.
	*/
	protected void setLastPreallocPagenum(long preAllocPagenum)
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(preAllocPagenum >= getLastPreallocPagenum(),
                "setLastPreallocPagenum set to small prealloc length than before");

		// cannot prealloc more than this extent can handle
		if (preAllocPagenum > extentEnd)
			preAllocPagenum = extentEnd;

		preAllocLength = (int)(preAllocPagenum - extentStart + 1);
	}


	/*
	  Get the logical page number that is bigger than prevPageNumber
	  and is a valid page.  If no such page in this extent, return
	  ContainerHandle.INVALID_PAGE_HANDLE
	*/
	protected long getNextValidPageNumber(long prevPageNumber)
	{
		long pageNum;
		long lastpage = getLastPagenum();

		if (prevPageNumber < extentStart)
			pageNum = extentStart;
		else
			pageNum = prevPageNumber +1;

		while(pageNum <= lastpage)
		{
			int status = getPageStatus(pageNum);
			if (status == ALLOCATED_PAGE)
				break;
			pageNum++;
		}

		if (pageNum > lastpage)
			pageNum = ContainerHandle.INVALID_PAGE_NUMBER;
		return pageNum;
	}


	protected long getLastValidPageNumber()
	{
		long pageNum = getLastPagenum();
		while(pageNum >= extentStart)
		{
			int status = getPageStatus(pageNum);
			if (status == ALLOCATED_PAGE)
				break;
			pageNum--;
		}
		if (pageNum < extentStart)
			pageNum = ContainerHandle.INVALID_PAGE_NUMBER;
		return pageNum;
	}

	private void checkInRange(long pagenum)
	{
		if (SanityManager.DEBUG)
			if (pagenum < extentStart || pagenum >= extentStart+extentLength)
				SanityManager.THROWASSERT(
                    "pagenum " + pagenum + " out of range");
	}

	protected void updateUnfilledPageInfo(AllocExtent inputExtent)
	{
		if (SanityManager.DEBUG)
		{
            if (inputExtent.unFilledPages.getLength() != 
                    unFilledPages.getLength())
            {
                SanityManager.THROWASSERT(
                    "inputExtent's unfilled page length " +
                    inputExtent.unFilledPages.getLength() + 
                    " != extent's unfilled page length " + 
                    unFilledPages.getLength());
            }
		}

		// just use the passed in inputExtent, we know (wink wink) that the
		// unfilled page info is being updated just when the allocation cache
		// is being invalidated.  Nobody is going to have a reference to the
		// inputExtent after this so is it save to share the FormatableBitSet.

		// if we cannot guarentee that the inputExtent will be unchanged by the
		// caller, we need to copy it 		
		//		unFilledPages = new FormatableBitSet(inputExtent.unFilledPages);
		// Right now, just reference it directly
		unFilledPages = inputExtent.unFilledPages;

		if (unFilledPages.anySetBit() >= 0)
			extentStatus |= HAS_UNFILLED_PAGES;
		else
			extentStatus &= ~HAS_UNFILLED_PAGES;
	}

	/*
		Keep track of unfilled pages, if the extent changed, returns true.
	 */
	protected boolean trackUnfilledPage(long pagenumber, boolean unfilled)
	{
		checkInRange(pagenumber);

		int bitnum = (int)(pagenumber-extentStart);

		boolean bitSet = unFilledPages.isSet(bitnum);
		if (unfilled != bitSet)
		{
			if (unfilled)
			{
				unFilledPages.set(bitnum);
				extentStatus |= HAS_UNFILLED_PAGES;
			}
			else
				unFilledPages.clear(bitnum);
			return true;
		}

		return false;
	}

	/**
		Get a page number that is unfilled, pagenum is the last page that was
		rejected.
	 */
	protected long getUnfilledPageNumber(long pagenum)
	{
		if ((extentStatus & HAS_UNFILLED_PAGES) == 0)
			return ContainerHandle.INVALID_PAGE_NUMBER;

		int i = unFilledPages.anySetBit();

		if (i != -1)
		{
			if (i+extentStart != pagenum)
				return i+extentStart;
			else
			{
				// unfortunately, we found the same page number that
				// was rejected.  It would be unwise to unset bit
				// pagenum because just because it was rejected does not mean
				// the page is full, the row we are trying to insert may just
				// be too big.  If we unset it, we will never find that page
				// again even though it may be a perfectly good page for any
				// other row.  Just get the next set bit.
				i = unFilledPages.anySetBit(i);
				if (i != -1)
					return i+extentStart;
			}
		}

		return ContainerHandle.INVALID_PAGE_NUMBER;

	}

	/**
		Get the number of used page in this extent
	 */
	protected int getAllocatedPageCount()
	{
		// allocated page is one which is not free or deallocated.
		int allocatedPageCount = extentLength;

		if (!mayHaveFreePage())
			return allocatedPageCount;


		byte[] free = freePages.getByteArray();
		int numBytes = free.length;

		for (int i = 0; i < numBytes; i++)
		{
			if (free[i] != 0)
			{
				for (int j = 0; j < 8; j++)
				{
					if (((1 << j) & free[i]) != 0)
						allocatedPageCount--;
				}
			}
		}

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(allocatedPageCount >= 0,
								 "number of allocated page < 0");

		return allocatedPageCount;
	}


	/**
		Get the number of unfilled pages in this extent
	 */
	protected int getUnfilledPageCount()
	{
        int unfilledPageCount = 0;
        int freePagesSize = freePages.size();

        for (int i = 0; i < unFilledPages.size(); i++)
        {
            if (unFilledPages.isSet(i) &&
                (i >= freePagesSize || !freePages.isSet(i)))
                unfilledPageCount++;
        }

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(unfilledPageCount >= 0,
								 "number of unfilled pages < 0");

        return unfilledPageCount;
 	}


	/**
		Get the total number of pages in this extent
	 */
	protected int getTotalPageCount()
	{
        return extentLength;
    }

	protected String toDebugString()
	{
		if (SanityManager.DEBUG)
		{
			String str =
				"------------------------------------------------------------------------------\n" +
				"Extent map of from page " + extentStart + " to page " + extentEnd + "\n";

			for (long i = extentStart; i < extentStart+extentLength; i++)
			{
				str += "\tpage " + i + ": ";
				switch(getPageStatus(i))
				{
				case FREE_PAGE: str += "free page\n"; break;
				case ALLOCATED_PAGE: str += "valid, in use page\n"; break;
				}

		        // int bitnum = (int)(i-extentStart);
        		// if (unFilledPages.isSet(bitnum))
                //    str += "          page is estimated to be unfilled\n";
			}

			if (getLastPagenum() < extentEnd)
				str += "\tFrom " + getLastPagenum() + " to " + extentEnd +
					" are un-allocated pages\n";

			str += "------------------------------------------------------------------------------\n";

			return str;
		}
		else
			return null;
	}

}
