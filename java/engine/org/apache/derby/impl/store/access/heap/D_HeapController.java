/*

   Derby - Class org.apache.derby.impl.store.access.heap.D_HeapController

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

package org.apache.derby.impl.store.access.heap;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.services.diag.Diagnosticable;
import org.apache.derby.iapi.services.diag.DiagnosticableGeneric;
import org.apache.derby.iapi.services.diag.DiagnosticUtil;


import java.util.Properties;


/**

  The HeapController_D class implements the Diagnostics protocol for the
  HeapController class.

**/

class TableStats
{
    public int num_pages          = 0; // number of pages in heap.
    public int num_overflow_pgs   = 0; // number of overflow pages heap.
    public int num_entries        = 0; // number recs on page
    public int num_deleted        = 0; // number of recs on page marked deleted.
    public long max_pageno        = 0; // biggest page number allocated
    public long num_free_bytes    = 0; // number of free bytes on the pages.
    public long num_res_bytes     = 0; // number of reserved bytes on the pages.
    public long num_overflow_rows = 0; // number of over flow rows on page.
    public long num_rowsize_bytes = 0; // number of bytes in rows.
    public long min_rowsize_bytes = Long.MAX_VALUE; // length of shortest row.
    public long max_rowsize_bytes = Long.MIN_VALUE; // length of longest row.
}

public class D_HeapController extends DiagnosticableGeneric
{
    /* Private/Protected methods of This class: */
    private static void diag_page(
    Page        page,
    Properties  prop,
    TableStats  stat)
        throws StandardException
    {
        stat.num_pages++;
        stat.num_entries += page.recordCount();
        stat.num_deleted += 
            (page.recordCount() - page.nonDeletedRecordCount());
        stat.max_pageno = Math.max(stat.max_pageno, page.getPageNumber());

        DiagnosticUtil.findDiagnostic(page).diag_detail(prop);

        // number of free bytes on page.
        int free_bytes = 
            Integer.parseInt(prop.getProperty(Page.DIAG_BYTES_FREE));

        stat.num_free_bytes += free_bytes;

        // number of bytes reserved on page.
        int res_bytes = 
            Integer.parseInt(prop.getProperty(Page.DIAG_BYTES_RESERVED));

        stat.num_res_bytes += res_bytes;

        // overflow rows.
        int overflow = 
            Integer.parseInt(prop.getProperty(Page.DIAG_NUMOVERFLOWED));

        stat.num_overflow_rows += overflow;

        // size of rows.
        int rowsize = 
            Integer.parseInt(prop.getProperty(Page.DIAG_ROWSIZE));

        stat.num_rowsize_bytes += rowsize;

        // minimum row size.
        int min_rowsize = 
            Integer.parseInt(prop.getProperty(Page.DIAG_MINROWSIZE));

        if (min_rowsize != 0)
            stat.min_rowsize_bytes = 
                Math.min(stat.min_rowsize_bytes, min_rowsize);

        // maximum row size.
        int max_rowsize = 
            Integer.parseInt(prop.getProperty(Page.DIAG_MAXROWSIZE));

        stat.max_rowsize_bytes = Math.max(stat.max_rowsize_bytes, max_rowsize);
    }

    private static String out_summary(
    String  hdr,
    long    value,
    double  ratio,
    String  ratio_desc)
    {
        String double_str = "" + ratio;
        String short_str;

        if (ratio > 0.001)
        {
            short_str = double_str.substring(
                0, 
                Math.min(double_str.lastIndexOf(".") + 3, double_str.length()));
        }
        else 
        {
            short_str = "NA";
        }

        return(
            "\t" + hdr + value + ".\t(" + short_str + 
            " " + ratio_desc + ").\n");
    }
            

    private static String diag_tabulate(
    Properties  prop,
    TableStats stat)
    {
        String ret_string   = new String();

        // Totals:
        ret_string += 
            "Heap conglom has:\n" + 
            "\t" + prop.getProperty(Page.DIAG_PAGE_SIZE) + " bytes per page\n" +
            "\t" + stat.num_pages           + " total used pages (" +
                (Integer.parseInt(prop.getProperty(Page.DIAG_PAGE_SIZE)) * 
                     stat.num_pages) + 
                " bytes)\n"            +
            "\tmaximum page number   = " + stat.max_pageno + ".\n"         +
            "\treserved space %      = " + prop.getProperty(Page.DIAG_RESERVED_SPACE) + "%.\n"         +
            "\tminimum record size   = " + prop.getProperty(Page.DIAG_MINIMUM_REC_SIZE) + ".\n"         +
            "\tminimum record length = " + stat.min_rowsize_bytes + ".\n" +
            "\tmaximum record length = " + stat.max_rowsize_bytes + ".\n" +
            "\t# of bytes in rows    = " + stat.num_rowsize_bytes + "." +
                "\t(" + 
                    (stat.num_entries == 0 ? 
                         0 : (stat.num_rowsize_bytes / stat.num_entries)) + 
                " bytes/row).\n"                                            +
            out_summary(
                "# of reserved bytes   = ", 
                stat.num_res_bytes,
                (stat.num_res_bytes / stat.num_pages),
                "reserved bytes/page") +
            out_summary(
                "# of free bytes       = ",
                stat.num_free_bytes,
                (stat.num_free_bytes / stat.num_pages),
                "free bytes/page")  +
            out_summary(
                "# of total records    = ",
                stat.num_entries,
                (((double) stat.num_entries) / stat.num_pages),
                "records/page") +
            out_summary(
                "# of overflow records = ",
                stat.num_overflow_rows,
                (((double) stat.num_overflow_rows) / stat.num_pages),
                "overflow records/page") +
            out_summary(
                "# of deleted records  = ",
                stat.num_deleted,
                (((double) stat.num_deleted) / stat.num_pages),
                "deleted records/page"); 

        return(ret_string);
    }

	/*
	** Methods of Diagnosticable
	*/
    public void init(Object obj)
    {
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(obj instanceof HeapController);

        super.init(obj);
    }


    /**
     * Default implementation of diagnostic on the object.
     * <p>
     * This routine returns a string with whatever diagnostic information
     * you would like to provide about this object.
     * <p>
     * This routine should be overriden by a real implementation of the
     * diagnostic information you would like to provide.
     * <p>
     *
	 * @return A string with diagnostic information about the object.
     *
     * @exception StandardException  Standard cloudscape exception policy
     **/
    public String diag()
        throws StandardException
    {
        long pageid;
        ContainerHandle container = 
            ((HeapController) this.diag_object).getOpenConglom().getContainer();

        TableStats stat = new TableStats();

        // ask page to provide diag info:
        Properties prop = new Properties();
        prop.put(Page.DIAG_PAGE_SIZE,        "");
        prop.put(Page.DIAG_BYTES_FREE,       "");
        prop.put(Page.DIAG_BYTES_RESERVED,   "");
        prop.put(Page.DIAG_RESERVED_SPACE,   "");
        prop.put(Page.DIAG_MINIMUM_REC_SIZE, "");
        prop.put(Page.DIAG_NUMOVERFLOWED,    "");
        prop.put(Page.DIAG_ROWSIZE,          "");
        prop.put(Page.DIAG_MINROWSIZE,       "");
        prop.put(Page.DIAG_MAXROWSIZE,       "");

        // scan all pages in the heap gathering summary stats in stat
        Page page = container.getFirstPage();

        while (page != null)
        {
            this.diag_page(page, prop, stat);
            pageid = page.getPageNumber();
            page.unlatch();
            page = container.getNextPage(pageid);
        }

        return(this.diag_tabulate(prop, stat));
    }
}
