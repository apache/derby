/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.storetests.st_reclaim_longcol

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


import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.Arrays;

import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;


/**

The purpose of this test space reclamation of long rows and long columns.
This addresses DERBY-670.

The main issue is that previous to fixes for DERBY-670, space reclamation
was only automatically queued when the last row on a page was deleted.  In
the case of long columns, the actual row on the main page can be quite small
as the long data is streamed onto other pages.  So the table can grow 
unexpectedly quite large before the default space reclamation kicks in.  The
change queues space reclamation in the case of long columns (blob/clob),
immediately post commit of the single delete.

The testing strategy is to loop doing insert, delete, commit of a blob for
a number of iterations and check that the actual size of the table is 
reasonable.  A sleep will be added to allow time for post commit to catch up
as the test may be run in a number of environments with varying performance
of background activities.

**/

public class st_reclaim_longcol extends BaseJDBCTestCase
{
    static boolean verbose = false;

    public st_reclaim_longcol(String name) {
        super(name);
    }

    public static Test suite() {
        return new CleanDatabaseTestSetup(
                TestConfiguration.embeddedSuite(st_reclaim_longcol.class));
    }

    /**
     * Test reclaim of a single deleted blob on a page with non-deleted rows.
     * <p>
     * loops through inserting alternating long and short column rows resulting
     * in pages with 1 short and one long.  Deletes the long column row and
     * tests that space from the long column row is reclaimed even though
     * there are non-deleted rows on the page.
     **/
    private void test1(int blob_size, int num_rows) throws SQLException
    {
        byte[]  long_byteVal    = new byte[blob_size];
        byte[]  short_byteVal   = new byte[10];

        println(
            "test1:insert/delete of " + num_rows + 
                " rows with blob(" + blob_size + ")"); 

        Arrays.fill(long_byteVal,  (byte)'L');
        Arrays.fill(short_byteVal, (byte)'S');

        Statement s = createStatement();
        dropTable("LONGCOL");
        s.execute(
            "create table longcol (id int primary key not null, val blob(" + 
            blob_size + "))");

        commit();

        PreparedStatement ins_stmt = 
            prepareStatement("insert into longcol values (?, ?)");
        PreparedStatement del_stmt = 
            prepareStatement("delete from longcol where id = ?");

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
        
        // keep track of what we think should be worst case number
        // of allocated pages based on total number of rows in the page
        // after long column is deleted.  Table starts with 1 page, then 
        // on each iteration the worst case is that the new short row goes
        // onto a new page.  Start out with 5 for overhead.
        int worst_case_max_allocated = 5;

      
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

            // in the worst case each new set of 2 rows will result in
            // the one undeleted row getting put on a new page.
            worst_case_max_allocated++;

            // commit the xact, post commit should kick in to reclaim the
            // blob space sometime after the commit.
            commit();

            // after each commit give the background thread a chance to 
            // reclaim the deleted rows.
            wait_for_max_allocated("test1", worst_case_max_allocated);
        }

        // get total pages = allocated pages + free pages
        ResultSet rs = getSpaceTable("LONGCOL");
        assertTrue("Space table was empty", rs.next());

        int allocated = rs.getInt("NUMALLOCATEDPAGES");
        int free = rs.getInt("NUMFREEPAGES");
        int total_pages = allocated + free;

        println("Space information after " + num_rows +
                "insert/delete pairs of rows in longcol table containing " +
                blob_size + "blobs:");
        printCurrentRow(rs);

        JDBC.assertEmpty(rs);  // There should only be one row.

        int total_expected_page_max = 12 + num_rows;

        if (total_pages > total_expected_page_max)
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

            fail(
                "Test 1 failed, expected less than " + 
                total_expected_page_max + " pages - count is:\n" +
                "free pages     : " + free +
                "\nallocated pages: " + allocated);
        }

        commit();
    }

    /**
     * DERBY-1913
     * <p>
     * test2 is too sensitive to machine speed and background thread
     * processing.  It would be better suited as a long running stress
     * test if someone has the inclination.  Disabling this test for
     * now.  test1 covers the original intent to test that blobs are
     * immediately marked for post commit on individual delete, rather
     * than waiting for all rows on a page to be deleted.
     **/
    private void test2(
    int         blob_size, 
    int         work_size, 
    int         total_work)
        throws SQLException
    {
        byte[]  long_byteVal    = new byte[blob_size];
        byte[]  short_byteVal   = new byte[10];

        println(
            "test2:queue of " + work_size + 
                " rows with blob(" + blob_size + "), total_work = " + 
                total_work); 

        Arrays.fill(long_byteVal,  (byte)'L');
        Arrays.fill(short_byteVal, (byte)'S');

        Statement s = createStatement();
        dropTable("LONGCOL");
        s.execute(
            "create table longcol (id int primary key not null, val blob(" + 
            blob_size + "))");

        PreparedStatement ins_stmt = 
            prepareStatement("insert into longcol values (?, ?)");
        PreparedStatement del_stmt = 
            prepareStatement("delete from longcol where id = ?");

        // insert the "work_size" number of elements into the table
        for (int iter = 0; iter < work_size; iter++)
        {
            // insert the long blob
            ins_stmt.setInt(  1, iter);
            ins_stmt.setBytes(2, long_byteVal);
            ins_stmt.executeUpdate();

        }
        commit();


        // for each subsequent work item, queue it to the end and delete
        // the oldest existing work item.
        for (int iter = work_size; iter < total_work; iter++)
        {
            // insert the long blob
            ins_stmt.setInt(  1, iter);
            ins_stmt.setBytes(2, long_byteVal);
            ins_stmt.executeUpdate();

            // delete the long blob
            del_stmt.setInt(1, iter - work_size);
            del_stmt.executeUpdate();

            // commit the xact, post commit should kick in to reclaim the
            // blob space sometime after the commit.
            commit();
        }



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

        // Wait for background thread to convert all deleted rows to 
        // free pages.  Total number of free pages is very variable, dependent
        // on availability of background cpu, so just wait to get under
        // an expected max of allocated pages.  Expect 10 allocated pages per
        // item in work size and add 5 pages for misc overhead.
        wait_for_max_allocated("test2", (10 * work_size) + 5);

        ResultSet rs = getSpaceTable("LONGCOL");
        assertTrue("Space table was empty", rs.next());

        int total_pages =
                rs.getInt("NUMALLOCATEDPAGES") + rs.getInt("NUMFREEPAGES");

        println("Space information:");
        printCurrentRow(rs);
        JDBC.assertEmpty(rs);

        commit();

        // Run another iteration of the work loop, by now memory should 
        // have gotten to constant.
        for (int iter = total_work; iter < (total_work * 2); iter++)
        {
            // insert the long blob
            ins_stmt.setInt(  1, iter);
            ins_stmt.setBytes(2, long_byteVal);
            ins_stmt.executeUpdate();

            // delete the long blob
            del_stmt.setInt(1, iter - work_size);
            del_stmt.executeUpdate();

            // commit the xact, post commit should kick in to reclaim the
            // blob space sometime after the commit.
            commit();
        }

        // Wait for background thread to convert all deleted rows to 
        // free pages.  Total number of free pages is very variable, dependent
        // on availability of background cpu, so just wait to get under
        // an expected max of allocated pages.  Expect 10 allocated pages per
        // item in work size and add 5 pages for misc overhead.
        wait_for_max_allocated("test2_2", (10 * work_size) + 5);

        rs = getSpaceTable("LONGCOL");
        assertTrue("Space table was empty", rs.next());

        int second_total_pages =
                rs.getInt("NUMALLOCATEDPAGES") + rs.getInt("NUMFREEPAGES");

        println("Space information:");
        printCurrentRow(rs);
        JDBC.assertEmpty(rs);

        commit();

        // This could fail due to machine variability, leaving it for now
        // as I have not seen this failure reported.
        if (total_pages != second_total_pages)
        {
            fail(
                "Test 2 failed, expected constant memory after second run." +
                "initial total = " + total_pages +
                "second total = " + second_total_pages);
        }
    }

    /**
     * Invoke SYSCS_DIAG.SPACE_TABLE on the specified table in the current
     * schema.
     */
    private ResultSet getSpaceTable(String table) throws SQLException {
        PreparedStatement ps = prepareStatement(
                "select * from table(syscs_diag.space_table(?)) t "
                        + "where isindex = 0");
        ps.setString(1, table);
        return ps.executeQuery();
    }

    /**
     * Print the value of all columns in the current row of the specified
     * result set, if debugging is enabled.
     */
    private void printCurrentRow(ResultSet rs) throws SQLException {
        if (TestConfiguration.getCurrent().isVerbose()) {
            ResultSetMetaData rsmd = rs.getMetaData();
            for (int col = 1; col <= rsmd.getColumnCount(); col++) {
                println(rsmd.getColumnName(col) + ": " + rs.getObject(col));
            }
        }
    }

    /**
     * wait for background thread to convert allocated pages to free pages
     * <p>
     * Wait until the total number of allocated pages is &lt;= alloc_wait_count.
     * The expectation is that the test has performed some deletes and 
     * committed allowing the background task converted empty allocated pages
     * with only deleted rows into free pages.
     *
     * On an machine with some idle processors only a short wait should
     * be necessary.  But on machines with lots of load, possibly other
     * tests running just sleeping does not guarantee background thread
     * an immediate chance to run.  Without this extra wait some nightly's
     * were seeing failures, see DERBY-1913.
     **/
    private void wait_for_max_allocated(
    String      test_name,
    int         alloc_wait_count)
        throws SQLException 
    {
        // an initial 1/10 of second which should work for most environments.
        sleep(100);

        Integer save_total_alloc = null;

        // wait for maximum 100 seconds.

        // wait 10 seconds and give up if it has mad no progress.
        int max_wait_for_bg_thread = 10000;
        int ms_waited              = 100;

        while (true)
        {
            ResultSet rs = getSpaceTable("LONGCOL");
            assertTrue("Space table was empty", rs.next());
            int total_alloc = rs.getInt("NUMALLOCATEDPAGES");
            int free = rs.getInt("NUMFREEPAGES");
            JDBC.assertEmpty(rs);

            if (total_alloc <= alloc_wait_count) {
                // The number of allocated pages has shrunk enough. Break
                // out of the loop.
                break;
            }

            // Save the first count so that we can see if we've made
            // progress later.
            if (save_total_alloc == null) {
                save_total_alloc = total_alloc;
            }

            if (ms_waited < max_wait_for_bg_thread)
            {
                // The result is dependent on background activity which may
                // differ from machine to machine.  Loop, sleeping in this
                // thread to allow background thread to run.

                ms_waited += 1000;
                sleep(1000);

            }
            else if (total_alloc < save_total_alloc)
            {
                // background thread did make progress, give it another
                // 10 seconds.
                save_total_alloc = total_alloc;
                max_wait_for_bg_thread += 10000;
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

                fail(
                    "Test " + test_name + 
                    " failed in wait_for_max_allocated(), expected less than " + 
                    alloc_wait_count + " allocated pages:\n" +
                    "free pages     : "   + free +
                    "\nallocated pages: " + total_alloc +
                    "\nWaited " + ms_waited + "ms. for background work.");
            }
        }
    }

    public void testList() throws SQLException
    {
        setAutoCommit(false);

        test1(250000, 20);

        // DERBY-1913 - disabling test2 as it is too sensitive to background
        // processing.
        // test2(250000, 5, 500);
    }
}
