/*

   Derby - Class org.apache.derby.impl.store.raw.data.D_StoredPage

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.services.diag.Diagnosticable;
import org.apache.derby.iapi.services.diag.DiagnosticableGeneric;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.Qualifier;

import org.apache.derby.iapi.store.raw.FetchDescriptor;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;

import org.apache.derby.iapi.types.DataValueDescriptor;

import java.util.Properties;
import java.io.PrintStream;
import java.io.IOException;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.CounterOutputStream;
import org.apache.derby.iapi.services.io.NullOutputStream;

/**

The D_StoredPage class provides diagnostic information about the StoredPage
class.  Currently this info includes:
  o a dump of the page.
  o page size of the page.
  o bytes free on the page.
  o bytes reserved on the page.

**/

public class D_StoredPage implements Diagnosticable
{
    protected StoredPage page;

    public D_StoredPage()
    {
    }

    /* Private/Protected methods of This class: */

	/*
	** Methods of Diagnosticable
	*/
    public void init(Object obj)
    {
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(obj instanceof StoredPage);

        page = (StoredPage) obj;
    }

    /**
     * Provide a string dump of the StoredPage.
     * <p>
     * RESOLVE - once the "Diagnostic" interface is accepted move the
     * string dumping code into this routine from it's current place in
     * the StoredPage code.
     * <p>
     *
	 * @return string dump of the StoredPage
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public String diag()
        throws StandardException
    {
        return(page.toString());
    }

    /**
     * Provide detailed diagnostic information about a StoredPage.
     * <p>
     * Currently supports 3 types of information:
     *   Page.DIAG_PAGE_SIZE      - page size.
     *   Page.DIAG_BTYES_FREE     - # of free bytes on the page.
     *   Page.DIAG_BYTES_RESERVED - # of reserved bytes on the page.
     * <p>
     *
	 * @return string dump of the StoredPage
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void diag_detail(Properties prop) 
        throws StandardException
    {
        String prop_value = null;

        // currently only support 2 properties - pageSize and freeBytes
        if (prop.getProperty(Page.DIAG_PAGE_SIZE) != null)
        {
            // set the page size diag string
            prop.put(Page.DIAG_PAGE_SIZE, Integer.toString(page.getPageSize()));
        }

        if (prop.getProperty(Page.DIAG_BYTES_FREE) != null)
        {
            int space_available = page.freeSpace;

            // set the page free diag string
            prop.put(Page.DIAG_BYTES_FREE, Integer.toString(space_available));
        }

        if (prop.getProperty(Page.DIAG_BYTES_RESERVED) != null)
        {
            int reservedSpace = (page.totalSpace * page.spareSpace / 100);
            reservedSpace = Math.min(reservedSpace, page.freeSpace);
            
            // set the reserved space diag string.
            prop.put(
                Page.DIAG_BYTES_RESERVED, Integer.toString(reservedSpace));
        }

        if (prop.getProperty(Page.DIAG_RESERVED_SPACE) != null)
        {
            // DIAG_RESERVED_SPACE is the % of the page to reserve during 
            // insert for expansion.

            prop.put(
                Page.DIAG_RESERVED_SPACE, Integer.toString(page.spareSpace));
        }

        if (prop.getProperty(Page.DIAG_MINIMUM_REC_SIZE) != null)
        {
            // DIAG_MINIMUM_REC_SZE is the minimum number of bytes per row 
            // to reserve at insert time for a record.

            prop.put(
                Page.DIAG_MINIMUM_REC_SIZE, 
                Integer.toString(page.minimumRecordSize));
        }

        if (prop.getProperty(Page.DIAG_PAGEOVERHEAD) != null)
        {
            // DIAG_PAGEOVERHEAD is the amount of space needed by the page 
            // for it's internal info.

            prop.put(
                Page.DIAG_PAGEOVERHEAD, 
                Integer.toString(page.getPageSize() - page.getMaxFreeSpace()));
        }

        if (prop.getProperty(Page.DIAG_SLOTTABLE_SIZE) != null)
        {
            // DIAG_SLOTTABLE_SIZE is the amount of space needed by the page 
            // for the current slot table.

            // RESOLVE - it would be better to call a StoredPage variable or
            // interface.
            int slotEntrySize = page.getSlotsInUse() * 3 * 
                ((page.getPageSize() >= 65536) ? 
                    StoredPage.LARGE_SLOT_SIZE : StoredPage.SMALL_SLOT_SIZE);

            prop.put(Page.DIAG_SLOTTABLE_SIZE, Integer.toString(slotEntrySize));
        }

        // loop through slot table and determine row size's and overflow recs.
        int  overflow_count = 0;
        int  row_size       = 0;
        long min_rowsize    = 0;
        long max_rowsize    = 0;
        long record_size    = 0;

        if (page.getSlotsInUse() > 0)
        {
            min_rowsize = Long.MAX_VALUE;
            max_rowsize = Long.MIN_VALUE;

            for (int slot = 0; slot < page.getSlotsInUse(); slot++)
            {
                try
                {
                    if (page.getIsOverflow(slot))
                    {
                        if (SanityManager.DEBUG)
                            SanityManager.DEBUG_PRINT("OVER", 
                                "Slot (" + slot + ") is overflow record of page:" +
                                page);
                        overflow_count++;
                    }
                    record_size = page.getRecordLength(slot);
                    row_size += record_size;

                    min_rowsize = Math.min(min_rowsize, record_size);
                    max_rowsize = Math.max(max_rowsize, record_size);
                }
                catch (Throwable t)
                {
                    System.out.println("Got error from getIsOverflow().");
                }

            }
        }

        if (prop.getProperty(Page.DIAG_NUMOVERFLOWED) != null)
        {
            // DIAG_NUMOVERFLOWED is the number of over flow rows on this page.

            prop.put(Page.DIAG_NUMOVERFLOWED, Integer.toString(overflow_count));
        }

        if (prop.getProperty(Page.DIAG_ROWSIZE) != null)
        {
            // sum of the record lengths on this page.

            prop.put(Page.DIAG_ROWSIZE, Integer.toString(row_size));
        }

        if (prop.getProperty(Page.DIAG_MINROWSIZE) != null)
        {
            // minimum length record on this page.

            prop.put(Page.DIAG_MINROWSIZE, Long.toString(min_rowsize));
        }

        if (prop.getProperty(Page.DIAG_MAXROWSIZE) != null)
        {
            // maximum length record on this page.

            prop.put(Page.DIAG_MAXROWSIZE, Long.toString(max_rowsize));
        }
    }



	/**
		Checks the slot table.

		1) checks the number of slot entries matches the record count
		2) checks the slot table lengths match the field lengths

	    @exception  StandardException  Standard exception policy.
	*/
	public boolean checkSlotTable(PrintStream out) throws StandardException, IOException {

		boolean ok = true;

		int slotCount = page.getSlotsInUse();
		int recordCount = page.recordCount();

		if (slotCount != recordCount) {
			out.println("CORRUPT PAGE: slot count mismatch: slot count " + slotCount
				+ " record count " + recordCount);
			ok = false;
		}

		for (int slot = 0; slot < slotCount; slot++) {

			int recordLength = page.getRecordPortionLength(slot);


			CounterOutputStream counter = new CounterOutputStream();
			counter.setOutputStream(new NullOutputStream());

			int recordId = 
                page.fetchFromSlot(
                    null, 
                    slot, 
                    new DataValueDescriptor[0], 
                    (FetchDescriptor) null, true).getId();

			page.logRecord(slot, page.LOG_RECORD_DEFAULT, recordId,
						   (FormatableBitSet) null, counter, (RecordHandle)null);

			int actualLength = counter.getCount();

			if (actualLength != recordLength) {
				out.println(
                    "CORRUPT PAGE: record length mismatch at slot " + slot);
				out.println("              slot entry length " + recordLength);
				out.println("              actual     length " + actualLength);
				ok = false;
			}				

		}


		return ok;

	}

	public String pageHeaderToString()
	{
		return "page id " + page.getIdentity() + 
				" Overflow: " + page.isOverflowPage() +
				" PageVersion: " + page.getPageVersion() +
				" SlotsInUse: " + page.getSlotsInUse() +
				" PageStatus: " + page.getPageStatus() + 
				" NextId: " + page.newRecordId() + "\n";
	}

}
