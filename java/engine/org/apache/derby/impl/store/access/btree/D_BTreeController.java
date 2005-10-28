/*

   Derby - Class org.apache.derby.impl.store.access.btree.D_BTreeController

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

package org.apache.derby.impl.store.access.btree;

import org.apache.derby.iapi.reference.Property;

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.diag.Diagnosticable;
import org.apache.derby.iapi.services.diag.DiagnosticableGeneric;
import org.apache.derby.iapi.services.diag.DiagnosticUtil;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.db.Database;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.Page;

import java.util.Properties;

/**

  A BTreeDiag class is a "helper" class for the rest of the btree generic
  code.  It is separated into a separate class so that it can be compiled
  out if necessary (or loaded at runtime if necessary).

  <p>
  more info.
**/
class LevelInfo
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
    public long num_slottab_bytes = 0; // number of bytes in slot table.
    public long min_rowsize_bytes = Long.MAX_VALUE; // length of shortest row.
    public long max_rowsize_bytes = Long.MIN_VALUE; // length of longest row.
}

public class D_BTreeController extends DiagnosticableGeneric
{
    /* Private/Protected methods of This class: */
    private static void diag_page(
    OpenBTree   open_btree,
    ControlRow  control_row, 
    Properties  prop,
    LevelInfo   level_info[])
        throws StandardException
    {
        LevelInfo li    = level_info[control_row.getLevel()];
        Page      page  = control_row.page;



        li.num_pages++;
        li.num_entries += (page.recordCount() - 1);
        li.num_deleted += (page.recordCount() - page.nonDeletedRecordCount());
        li.max_pageno  = Math.max(li.max_pageno, page.getPageNumber());

        DiagnosticUtil.findDiagnostic(page).diag_detail(prop);


        DiagnosticUtil.findDiagnostic(page).diag_detail(prop);

        // number of free bytes on page.
        int free_bytes = 
            Integer.parseInt(prop.getProperty(Page.DIAG_BYTES_FREE));

        li.num_free_bytes += free_bytes;

        // number of bytes reserved on page.
        int res_bytes = 
            Integer.parseInt(prop.getProperty(Page.DIAG_BYTES_RESERVED));

        li.num_res_bytes += res_bytes;

        // overflow rows.
        int overflow = 
            Integer.parseInt(prop.getProperty(Page.DIAG_NUMOVERFLOWED));

        li.num_overflow_rows += overflow;

        // size of rows.
        int rowsize = 
            Integer.parseInt(prop.getProperty(Page.DIAG_ROWSIZE));

        li.num_rowsize_bytes += rowsize;

        // size of slot table.
        int slottable_size = 
            Integer.parseInt(prop.getProperty(Page.DIAG_SLOTTABLE_SIZE));

        li.num_slottab_bytes += slottable_size;

        // minimum row size.
        int min_rowsize = 
            Integer.parseInt(prop.getProperty(Page.DIAG_MINROWSIZE));

        li.min_rowsize_bytes = Math.min(li.min_rowsize_bytes, min_rowsize);

        // maximum row size.
        int max_rowsize = 
            Integer.parseInt(prop.getProperty(Page.DIAG_MAXROWSIZE));

        li.max_rowsize_bytes = Math.max(li.max_rowsize_bytes, max_rowsize);
    }

    private static void diag_level(
    OpenBTree   open_btree,
    ControlRow  control_row, 
    Properties  prop,
    LevelInfo   level_info[])
        throws StandardException
    {
        ControlRow      child = null;

        diag_page(open_btree, control_row, prop, level_info);

        try
        {
            child = control_row.getLeftChild(open_btree);

            if (child != null)
            {
                // this is a branch page.
                if (SanityManager.DEBUG)
                    SanityManager.ASSERT(
                        control_row instanceof BranchControlRow);

                BranchControlRow branch = (BranchControlRow) control_row;

                diag_level(open_btree, child, prop, level_info);
                child.release();
                child = null;

                int numslots = branch.page.recordCount();
                for (int slot = 1; slot < numslots; slot++)
                {
                    child = branch.getChildPageAtSlot(open_btree, slot);
                    diag_level(open_btree, child, prop, level_info);
                    child.release();
                    child = null;
                }
            }
        }
        finally
        {
            if (child != null)
                child.release();
        }

        return;
    }

    private static String out_summary(
    String  hdr,
    long    value,
    double  ratio,
    String  ratio_desc)
    {
        String double_str = "" + ratio;

        String short_str = double_str.substring(
            0, Math.min(double_str.lastIndexOf(".") + 3, double_str.length()));

        return(
            "\t" + hdr + value + ".\t(" + short_str + 
            " " + ratio_desc + ").\n");
    }

    private static String diag_onelevel(
    Properties  prop,
    LevelInfo   li)
    {
        String ret_string   = new String();

        ret_string += 
            "Btree conglom has:\n" + 
            "\t" + prop.getProperty(Page.DIAG_PAGE_SIZE) + " bytes per page\n" +
            "\t" + li.num_pages           + " total used pages (" +
                (Integer.parseInt(prop.getProperty(Page.DIAG_PAGE_SIZE)) * 
                     li.num_pages) + 
                " bytes)\n"            +
            "\tmaximum page number   = " + li.max_pageno + ".\n"         +
            "\treserved space %      = " + prop.getProperty(Page.DIAG_RESERVED_SPACE) + "%.\n"         +
            "\tminimum record size   = " + prop.getProperty(Page.DIAG_MINIMUM_REC_SIZE) + ".\n"         +
            "\tpage overhead bytes   = " + prop.getProperty(Page.DIAG_PAGEOVERHEAD) + " bytes per page.\n"         +
            "\tminimum record length = " + li.min_rowsize_bytes + ".\n" +
            "\tmaximum record length = " + li.max_rowsize_bytes + ".\n" +
            "\t# of bytes in rows    = " + li.num_rowsize_bytes + "." +
                "\t(" + 
                (li.num_entries == 0 ? 
                     0 : (li.num_rowsize_bytes / li.num_entries)) + 
                " bytes/row).\n"                                  +
            out_summary(
                "# of reserved bytes   = ", 
                li.num_res_bytes,
                (li.num_res_bytes / li.num_pages),
                "reserved bytes/page") +
            out_summary(
                "# of free bytes       = ",
                li.num_free_bytes,
                (li.num_free_bytes / li.num_pages),
                "free bytes/page")  +
            out_summary(
                "# of slot table bytes = ",
                li.num_slottab_bytes,
                (li.num_slottab_bytes / li.num_pages),
                "slot table bytes/page")  +
            out_summary(
                "# of reserved+free+row+slot bytes = ",
                (li.num_rowsize_bytes +
                 li.num_res_bytes     +
                 li.num_free_bytes     +
                 li.num_slottab_bytes),
                ((li.num_rowsize_bytes +
                  li.num_res_bytes     +
                  li.num_free_bytes     +
                  li.num_slottab_bytes) / li.num_pages),
                "summed bytes/page")  +
            out_summary(
                "# of total records    = ",
                li.num_entries,
                (((double) li.num_entries) / li.num_pages),
                "records/page") +
            out_summary(
                "# of overflow records = ",
                li.num_overflow_rows,
                (((double) li.num_overflow_rows) / li.num_pages),
                "overflow records/page") +
            out_summary(
                "# of deleted records  = ",
                li.num_deleted,
                (((double) li.num_deleted) / li.num_pages),
                "deleted records/page"); 

        return(ret_string);
    }
            

    private static String diag_tabulate(
    Properties  prop,
    LevelInfo   level_info[])
    {
        String ret_string   = new String();
        LevelInfo   total   = new LevelInfo();

        // first tabulate totals for all levels
        
        for (int level = 0; level < level_info.length; level++) 
        {
            LevelInfo li = level_info[level];

            total.num_pages         += li.num_pages; 
            total.num_overflow_pgs  += li.num_overflow_pgs; 
            total.num_entries       += li.num_entries; 
            total.num_deleted       += li.num_deleted; 
            total.max_pageno        = Math.max(total.max_pageno, li.max_pageno);
            total.num_free_bytes    += li.num_free_bytes; 
            total.num_res_bytes     += li.num_res_bytes; 
            total.num_overflow_rows += li.num_overflow_rows; 
            total.num_rowsize_bytes += li.num_rowsize_bytes; 
            total.num_slottab_bytes += li.num_slottab_bytes; 
            total.min_rowsize_bytes = 
                Math.min(total.min_rowsize_bytes, li.min_rowsize_bytes);
            total.max_rowsize_bytes = 
                Math.max(total.max_rowsize_bytes, li.max_rowsize_bytes);
        }

        ret_string +=
            "Btree conglom has:\n" + 
            "\t" + prop.getProperty(Page.DIAG_PAGE_SIZE) + " bytes per page\n" +
            "\t" + total.num_pages           + " total used pages (" +
                (Integer.parseInt(prop.getProperty(Page.DIAG_PAGE_SIZE)) * 
                     total.num_pages) + 
                " bytes)\n"            +
            "\tmaximum page number   = " + total.max_pageno + ".\n"         +
            "\treserved space %      = " + prop.getProperty(Page.DIAG_RESERVED_SPACE) + "%.\n"         +
            "\tminimum record size   = " + prop.getProperty(Page.DIAG_MINIMUM_REC_SIZE) + ".\n"         +
            "\tpage overhead bytes   = " + prop.getProperty(Page.DIAG_PAGEOVERHEAD) + " bytes per page.\n";

        // Format Totals:
        ret_string += diag_onelevel(prop, total);

        // Format Totals by level:

        // Totals by level:
        for (int level = 0; level < level_info.length; level++) 
        {
            LevelInfo   li = level_info[level];

            ret_string += "level[" + level + "] stats:\n";

            ret_string += diag_onelevel(prop, li);
        }

        return(ret_string);
    }

    private static String olddiag_tabulate(
    Properties  prop,
    LevelInfo   level_info[])
    {
        String ret_string   = new String();
        long   total_pages  = 0;
        long   total_res    = 0;

        for (int level = 0; level < level_info.length; level++) 
        {
            total_pages += level_info[level].num_pages;
        }


        // Totals:
        ret_string += 
            "Btree conglom has:\n" + 
            "\t" + prop.getProperty(Page.DIAG_PAGE_SIZE) + " bytes per page\n" +
            "\t" + total_pages               + " total pages ("                +
                (Integer.parseInt(prop.getProperty(Page.DIAG_PAGE_SIZE)) * 
                     total_pages) + " bytes)\n"                                +
            "\t" + level_info.length         + " total levels\n"               +
            "\t" + level_info[0].num_entries + " total user records\n";

        // Totals by level:
        for (int level = 0; level < level_info.length; level++) 
        {
            LevelInfo   li = level_info[level];

            ret_string += "level[" + level + "] stats:\n";

            ret_string += 
                "\t# of pages           = " + li.num_pages      + ".\n" +
                "\t# of entries         = " + li.num_entries    + ".  " +
                "(" + (li.num_entries / li.num_pages) + " entries/page).\n" +
                "\t# of deleted entries = " + li.num_deleted    + ".  " +
                "(" + (li.num_deleted / li.num_pages) + " deleted/page).\n" +
                "\t# of free bytes      = " + li.num_res_bytes + ".  " +
                "(" + (li.num_res_bytes / li.num_pages) + " reserved bytes/page).\n" +
                "\t# of free bytes      = " + li.num_free_bytes + ".  " +
                "(" + (li.num_free_bytes / li.num_pages) + " free bytes/page).\n" +
                "\t# of slot table bytes= " + li.num_slottab_bytes + ".  " +
                "(" + (li.num_slottab_bytes / li.num_pages) + " slot table bytes/page).\n";
        }

        return(ret_string);
    }

	/*
	** Methods of Diagnosticable
	*/
    public void init(Object obj)
    {
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(obj instanceof BTreeController);

        super.init(obj);
    }

    /**
     * Default implementation of diagnostic on the object.
     * <p>
     * This routine returns a string with whatever diagnostic information
     * you would like to provide about this object.
     * <p>
     * This routine returns a summary table of information about pages in
     * each level of the btree.  It tells the height of the tree, the 
     * average free and reserved bytes per level, and the page size.
     * <p>
     *
	 * @return A string with diagnostic information about the object.
     *
     * @exception StandardException  Standard cloudscape exception policy
     **/
    public String diag()
        throws StandardException
    {
        OpenBTree   open_btree  = (BTreeController) this.diag_object;
        ControlRow  root        = null;
        int         tree_height;
        LevelInfo   level_info[] = null;
        String      diag_info    = new String();

        
        try
        {
            tree_height = open_btree.getHeight();
            root = ControlRow.Get(open_btree, BTree.ROOTPAGEID);

            // Allocate a LevelInfo array with one entry per level of the tree.
            level_info = new LevelInfo[tree_height];
            for (int level = 0; level < level_info.length; level++) 
                level_info[level] = new LevelInfo();

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
            prop.put(Page.DIAG_PAGEOVERHEAD,     "");
            prop.put(Page.DIAG_SLOTTABLE_SIZE,   "");

            diag_level(open_btree, root, prop, level_info);


            diag_info = diag_tabulate(prop, level_info);
        }
        finally
        {
            if (root != null)
                root.release();
        }

        return(diag_info);
    }
}
