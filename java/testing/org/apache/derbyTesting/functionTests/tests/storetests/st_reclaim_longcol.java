/*

   Derby - Class org.apache.derbyTesting.functionTests.harness.procedure

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.functionTests.tests.storetests;


import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derbyTesting.functionTests.tests.store.BaseTest;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.Arrays;

import org.apache.derby.tools.ij;


/**

The purpose of this test space reclamation of long rows and long columns.
This addresses DERBY-670.

The main issue is that previous to fixes for DERBY-670, space reclamation
was only automatically queued when the last row on a page was deleted.  In
the case of long columns, the actual row on the main page can be quite small
as the long data is streamed onto other pages.  So the table can grow 
unexpectedly quite large before the default space reclamation kicks in.  The
change queues space reclamation in the case of long columns (blob/clob),
imediately post commit of the single delete.

The testing strategy is to loop doing insert, delete, commit of a blob for
a number of iterations and check that the actual size of the table is 
reasonable.  A sleep will be added to allow time for post commit to catch up
as the test may be run in a number of environments with varying performance
of background activities.

**/

public class st_reclaim_longcol extends BaseTest
{
    static boolean verbose = false;

    public st_reclaim_longcol()
    {
    }


    /**
     * Create the base table.
     **/
    private static void setup()
        throws Exception
    {
    }

    /**
     * Test reclaim of a single deleted blob on a page with non-deleted rows.
     * <p>
     * loops through inserting alternating long and short column rows resulting
     * in pages with 1 short and one long.  Deletes the long column row and
     * tests that space from the long column row is reclaimed even though
     * there are non-deleted rows on the page.
     **/
    private static final int SHORT_BLOB_SIZE = 10;
    public void test1(Connection conn, int blob_size, int num_rows)
        throws SQLException
    {
        byte[]  long_byteVal    = new byte[blob_size];
        byte[]  short_byteVal   = new byte[10];

        beginTest(
            conn, 
            "test1:insert/delete of " + num_rows + 
                " rows with blob(" + blob_size + ")"); 

        Arrays.fill(long_byteVal,  (byte)'L');
        Arrays.fill(short_byteVal, (byte)'S');

        createTable(
            conn, 
            "longcol", 
            "create table longcol (id int primary key not null, val blob(" + 
            blob_size + "))");

        conn.commit();

        PreparedStatement ins_stmt = 
            conn.prepareStatement("insert into longcol values (?, ?)");
        PreparedStatement del_stmt = 
            conn.prepareStatement("delete from longcol where id = ?");

        // worst case is a mixture of rows with long columns and those without.
        // Insert of row with a long column always first goes onto a new 
        // page by itself, but subsequent non-long column rows can be inserted
        // on that page.  Then when the long column row is deleted - before the
        // change - it and all it's chain won't get reclaimed until all rows
        // on the page get deleted.

        // now do insert/delete/commit for subsequent rows.  Before fix the
        // space used in the table will grow until the deleted rows do not
        // fit on the first page.  And even then before the fix the rows
        // on the first page are never reclaimed as the 1st one is never
        // deleted.
        for (int iter = 1; iter < num_rows; iter++)
        {
            // insert the long blob
            ins_stmt.setInt(  1, iter);
            ins_stmt.setBytes(2, long_byteVal);
            ins_stmt.executeUpdate();

            // insert the short blob
            ins_stmt.setInt(  1, -(iter));
            ins_stmt.setBytes(2, short_byteVal);
            ins_stmt.executeUpdate();

            // delete the long blob
            del_stmt.setInt(1, iter);
            del_stmt.executeUpdate();

            // commit the xact, post commit should kick in to reclaim the
            // blob space sometime after the commit.
            conn.commit();

            // sleep, just in case on this machine background
            // post commit is slow.
            try
            {
                Thread.sleep(20);
            }
            catch (Exception ex)
            {
                // just ignore interupts of sleep.
            }
        }

        int[] sp_info = getSpaceInfo(conn, "APP", "LONGCOL", true);

        int total_pages = 
            sp_info[SPACE_INFO_NUM_ALLOC] + sp_info[SPACE_INFO_NUM_FREE];

        int total_expected_page_max = 12 + num_rows;

        int MAX_WAIT_FOR_BG_THREAD = 100000;
        int ms_waited              = 20;

        while (total_pages > total_expected_page_max)
        {
            if (ms_waited < MAX_WAIT_FOR_BG_THREAD)
            {
                // The result is dependent on background activity which may
                // differ from machine to machine.  Loop, sleeping in this
                // thread to allow background thread to run.

                try
                {
                    ms_waited += 1000;
                    Thread.sleep(1000);
                }
                catch (Exception ex)
                {
                    // just ignore interupts of sleep.
                }

                sp_info = getSpaceInfo(conn, "APP", "LONGCOL", true);

                total_pages = 
                    sp_info[SPACE_INFO_NUM_ALLOC] + 
                    sp_info[SPACE_INFO_NUM_FREE];
            }
            else
            {
                // for the above test case we expect the following space:
                //     page 0
                //     page 1 
                //     free space from 1 blob - 9 pages per blob
                //     allocated page per long/short blob insert.  Each long
                //         inserts onto a new page to try and fit it entirely
                //         on a page.  Then the short blob goes to last inserted
                //         page.  This process repeats.  The previous pages are
                //         marked "half-filled" and can be used in future for
                //         short rows that don't fit on the last page inserted.

                System.out.println(
                    "Test 1 failed, expected less than " + 
                    total_expected_page_max + " pages - count is:\n" +
                    "free pages     : "   + sp_info[SPACE_INFO_NUM_FREE] +
                    "\nallocated pages: " + sp_info[SPACE_INFO_NUM_ALLOC] +
                    "\nWaited " + ms_waited + "ms. for background work.");

                break;
            }
        }

        if (verbose)
        {
            System.out.println(
                "Space information after " + num_rows + 
                "insert/delete pairs of rows in longcol table containing " + 
                blob_size + "blobs:");

            System.out.println("isindex = "   + sp_info[SPACE_INFO_IS_INDEX]);
            System.out.println("num_alloc = " + sp_info[SPACE_INFO_NUM_ALLOC]);
            System.out.println("num_free = "  + sp_info[SPACE_INFO_NUM_FREE]);
            System.out.println("page_size = " + sp_info[SPACE_INFO_PAGE_SIZE]);
            System.out.println(
                "estimspacesaving = " + sp_info[SPACE_INFO_ESTIMSPACESAVING]);
        }

        endTest(
            conn, 
            "test1:insert/delete of " + num_rows + 
                " rows with blob(" + blob_size + ")"); 
    }

    /**
     * Test reclaim of sequence of deleted blobs.
     * <p>
     * Simulates a "queue" of work of input "work_size".  Inserts "work_size"
     * elements, and then subsequently in each transaction inserts a new 
     * work item and deletes the oldest work item.  Checks that the used
     * space reaches a steady state, rather than constantly growing.
     *
     **/
    public void test2(
    Connection  conn, 
    int         blob_size, 
    int         work_size, 
    int         total_work)
        throws SQLException
    {
        byte[]  long_byteVal    = new byte[blob_size];
        byte[]  short_byteVal   = new byte[10];

        beginTest(
            conn, 
            "test2:queue of " + work_size + 
                " rows with blob(" + blob_size + "), total_work = " + 
                total_work); 

        Arrays.fill(long_byteVal,  (byte)'L');
        Arrays.fill(short_byteVal, (byte)'S');

        createTable(
            conn, 
            "longcol", 
            "create table longcol (id int primary key not null, val blob(" + 
            blob_size + "))");

        conn.commit();

        PreparedStatement ins_stmt = 
            conn.prepareStatement("insert into longcol values (?, ?)");
        PreparedStatement del_stmt = 
            conn.prepareStatement("delete from longcol where id = ?");

        // insert the "work_size" number of elements into the table
        for (int iter = 0; iter < work_size; iter++)
        {
            // insert the long blob
            ins_stmt.setInt(  1, iter);
            ins_stmt.setBytes(2, long_byteVal);
            ins_stmt.executeUpdate();

            // commit the xact, post commit should kick in to reclaim the
            // blob space sometime after the commit.
        }
        conn.commit();


        // for each subsequent work item, queue it to the end and delete
        // the oldes existing work item.
        for (int iter = work_size; iter < total_work; iter++)
        {
            // insert the long blob
            ins_stmt.setInt(  1, iter);
            ins_stmt.setBytes(2, long_byteVal);
            ins_stmt.executeUpdate();


            // delete the long blob
            del_stmt.setInt(1, iter - work_size - 1);
            del_stmt.executeUpdate();

            // commit the xact, post commit should kick in to reclaim the
            // blob space sometime after the commit.
            conn.commit();

            try
            {
                Thread.sleep(20);
            }
            catch (Exception ex)
            {
                // just ignore interupts of sleep.
            }
        }


        int[] sp_info = getSpaceInfo(conn, "APP", "LONGCOL", true);

        int total_pages = 
            sp_info[SPACE_INFO_NUM_ALLOC] + sp_info[SPACE_INFO_NUM_FREE];

        // Expect at least allocated pages * 10 for each item in work_size, 
        // plus some overhead for 1st page and such.
        // Free page count depends on how quick post commit can free before
        // subsequent insert, and very likely is machine/jvm/os dependent. In
        // my testing adding a sleep of 100 ms. to the above insert/delete
        // loop changed free from 60 to 30.  Minimum is 10 for the one row 
        // that is deleted in the same xact as the first inserted row in the 
        // insert/delete loop.  The 30 below is expected allocate of 10
        // per work size, and then a guess at how fast post commit can keep
        // up with free pages.  Run the test with total_work reasonably 
        // bigger than worksize, something like work_size=5 and total_work >100
        int total_expected_page_max = 30 * work_size; 

        int MAX_WAIT_FOR_BG_THREAD = 100000;
        int ms_waited              = 20;

        while (total_pages > total_expected_page_max)
        {
            if (ms_waited < MAX_WAIT_FOR_BG_THREAD)
            {
                // The result is dependent on background activity which may
                // differ from machine to machine.  Loop, sleeping in this
                // thread to allow background thread to run.

                try
                {
                    ms_waited += 1000;
                    Thread.sleep(1000);
                }
                catch (Exception ex)
                {
                    // just ignore interupts of sleep.
                }

                sp_info = getSpaceInfo(conn, "APP", "LONGCOL", true);

                total_pages = 
                    sp_info[SPACE_INFO_NUM_ALLOC] + 
                    sp_info[SPACE_INFO_NUM_FREE];
            }
            else
            {

                System.out.println(
                    "Test 2 failed, expected less than " + 
                    total_expected_page_max + " pages - count is:\n" +
                    "free pages     : "   + sp_info[SPACE_INFO_NUM_FREE] +
                    "\nallocated pages: " + sp_info[SPACE_INFO_NUM_ALLOC] +
                    "\nWaited " + ms_waited + "ms. for background work.");

                break;
            }
        }

        if (verbose)
        {
            System.out.println("Space information:");

            System.out.println("isindex = "   + sp_info[SPACE_INFO_IS_INDEX]);
            System.out.println("num_alloc = " + sp_info[SPACE_INFO_NUM_ALLOC]);
            System.out.println("num_free = "  + sp_info[SPACE_INFO_NUM_FREE]);
            System.out.println("page_size = " + sp_info[SPACE_INFO_PAGE_SIZE]);
            System.out.println(
                "estimspacesaving = " + sp_info[SPACE_INFO_ESTIMSPACESAVING]);
        }

        // Run another iteration of the work loop, by now memory should 
        // have gotten to constant.
        for (int iter = work_size + total_work; iter < (total_work * 2); iter++)
        {
            // insert the long blob
            ins_stmt.setInt(  1, iter);
            ins_stmt.setBytes(2, long_byteVal);
            ins_stmt.executeUpdate();


            // delete the long blob
            del_stmt.setInt(1, iter - work_size - 1);
            del_stmt.executeUpdate();

            // commit the xact, post commit should kick in to reclaim the
            // blob space sometime after the commit.
            conn.commit();

            try
            {
                Thread.sleep(100);
            }
            catch (Exception ex)
            {
                // just ignore interupts of sleep.
            }
        }


        int[] second_sp_info = getSpaceInfo(conn, "APP", "LONGCOL", true);

        int second_total_pages = 
            sp_info[SPACE_INFO_NUM_ALLOC] + sp_info[SPACE_INFO_NUM_FREE];

        if (total_pages != second_total_pages)
        {
            System.out.println(
                "Test failed, expected constant memory after second run." +
                "initial total = " + total_pages +
                "second total = " + second_total_pages);
        }

        if (verbose)
        {
            System.out.println("Space information:");

            System.out.println("isindex = "   + sp_info[SPACE_INFO_IS_INDEX]);
            System.out.println("num_alloc = " + sp_info[SPACE_INFO_NUM_ALLOC]);
            System.out.println("num_free = "  + sp_info[SPACE_INFO_NUM_FREE]);
            System.out.println("page_size = " + sp_info[SPACE_INFO_PAGE_SIZE]);
            System.out.println(
                "estimspacesaving = " + sp_info[SPACE_INFO_ESTIMSPACESAVING]);
        }

        endTest(
            conn, 
            "test2:queue of " + work_size + 
                " rows with blob(" + blob_size + "), total_work = " + 
                total_work); 
    }


    public void testList(Connection conn)
        throws SQLException
    {
        test1(conn, 250000, 20);
        test2(conn, 250000, 5, 500);
    }

    public static void main(String[] argv) 
        throws Throwable
    {
        st_reclaim_longcol test = new st_reclaim_longcol();

        ij.getPropertyArg(argv); 
        Connection conn = ij.startJBMS();

        try
        {
            test.testList(conn);
        }
        catch (SQLException sqle)
        {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(
                System.out, sqle);
			sqle.printStackTrace(System.out);
		}
    }
}
