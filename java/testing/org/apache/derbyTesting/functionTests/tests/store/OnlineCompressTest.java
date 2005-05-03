/*

   Derby - Class org.apache.derbyTesting.functionTests.harness.procedure

   Copyright 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.store;

import org.apache.derby.iapi.db.OnlineCompress;

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.derby.tools.ij;


public class OnlineCompressTest extends BaseTest
{
    boolean verbose = true;

    OnlineCompressTest()
    {
    }

    /**
     * call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE() system procedure.
     * <p>
     * Utility test function to call the system procedure.
     *
     **/
    private void callCompress(
    Connection  conn,
    String      schemaName,
    String      tableName,
    boolean     purgeRows,
    boolean     defragmentRows,
    boolean     truncateEnd)
        throws SQLException
    {
        CallableStatement cstmt = 
            conn.prepareCall(
                "call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE(?, ?, ?, ?, ?)");
        cstmt.setString(1, schemaName);
        cstmt.setString(2, tableName);
        cstmt.setInt   (3, purgeRows      ? 1 : 0);
        cstmt.setInt   (4, defragmentRows ? 1 : 0);
        cstmt.setInt   (5, truncateEnd    ? 1 : 0);

        cstmt.execute();

        conn.commit();
    }

    /**
     * call the space table vti.
     * <p>
     * Utility test function to call the space table vti to get information
     * about allocated and free pages.  Information is passed back in an
     * int array as follows:
     *   is_index                 = ret_info[0];
     *   num_alloc                = ret_info[1];
     *   num_free                 = ret_info[2];
     *   page_size                = ret_info[3];
     *   estimate_space_savings   = ret_info[4];
     * <p>
     *
	 * @return the space information about the table.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    private static final int SPACE_INFO_IS_INDEX        = 0;
    private static final int SPACE_INFO_NUM_ALLOC       = 1;
    private static final int SPACE_INFO_NUM_FREE        = 2;
    private static final int SPACE_INFO_PAGE_SIZE       = 3;
    private static final int SPACE_INFO_ESTIMSPACESAVING = 4;
    private int[] getSpaceInfo(
    Connection  conn,
    String      schemaName,
    String      tableName)
		throws SQLException
    {
        String stmt_str = 
            "select conglomeratename, isindex, numallocatedpages, numfreepages, pagesize, estimspacesaving from new org.apache.derby.diag.SpaceTable('" +
            tableName + "') t where isindex = 0";
        PreparedStatement space_stmt = conn.prepareStatement(stmt_str);
        ResultSet rs = space_stmt.executeQuery();

        if (!rs.next())
        {
            if (SanityManager.DEBUG)
            {
                SanityManager.THROWASSERT(
                    "No rows returned from space table query on table: " +
                    schemaName + "." + tableName);
            }
        }

        int[] ret_info = new int[5];
        String conglomerate_name        = rs.getString(1);
        for (int i = 0; i < 5; i++)
        {
            ret_info[i] = rs.getInt(i + 2);
        }

        if (rs.next())
        {
            if (SanityManager.DEBUG)
            {
                SanityManager.THROWASSERT(
                    "More than one row returned from space query on table: " +
                    schemaName + "." + tableName);
            }
        }

        if (verbose)
        {
            System.out.println(
                "Space information for " + schemaName + "." + tableName + ":");
            System.out.println("isindex = " + ret_info[SPACE_INFO_IS_INDEX]);
            System.out.println("num_alloc = " + ret_info[SPACE_INFO_NUM_ALLOC]);
            System.out.println("num_free = " + ret_info[SPACE_INFO_NUM_FREE]);
            System.out.println("page_size = " + ret_info[SPACE_INFO_PAGE_SIZE]);
            System.out.println(
                "estimspacesaving = " + ret_info[SPACE_INFO_ESTIMSPACESAVING]);
        }

        rs.close();

        conn.commit();

        return(ret_info);
    }


    /**
     * Determine if inplace compress did it's job.
     * <p>
     * Figuring out if inplace compress in a fully reproducible way is hard
     * because derby has background threads which when given a chance do some
     * of the space reclamation work that this routine does, so the absolute
     * number of pages sometimes varies depending on machine/OS/JVM issues.
     * <p>
     * The approach here is to verify that at least N pages where reclaimed,
     * assuming other varience is an acceptable difference based on background
     * thread activity.  
     * <p>
     *
	 * @return The identifier to be used to open the conglomerate later.
     *
     **/
    private boolean checkBaseTableSpaceParameters(
    Connection  conn,
    String      schemaName,
    String      tableName,
    boolean     check_allocated_pages,
    int         max_allocated_pages,
    boolean     check_free_pages,
    int         max_free_pages)
		throws SQLException
    {
        int[] ret_info = getSpaceInfo(conn, schemaName, tableName);

        int    is_index                 = ret_info[0];
        int    num_alloc                = ret_info[1];
        int    num_free                 = ret_info[2];
        int    page_size                = ret_info[3];
        int    estimate_space_savings   = ret_info[4];

        return(true);
    }



    private void createAndLoadTable(
    Connection  conn,
    boolean     create_table,
    String      tblname,
    int         num_rows)
        throws SQLException
    {
        if (create_table)
        {
            Statement s = conn.createStatement();

            s.execute(
                "create table " + tblname + 
                    "(keycol int, indcol1 int, indcol2 int, indcol3 int, data1 varchar(2000), data2 varchar(2000))");
            s.close();
        }

        PreparedStatement insert_stmt = 
            conn.prepareStatement(
                "insert into " + tblname + " values(?, ?, ?, ?, ?, ?)");

        char[]  data1_data = new char[500];
        char[]  data2_data = new char[500];

        for (int i = 0; i < data1_data.length; i++)
        {
            data1_data[i] = 'a';
            data2_data[i] = 'b';
        }

        String  data1_str = new String(data1_data);
        String  data2_str = new String(data2_data);

        for (int i = 0; i < num_rows; i++)
        {
            insert_stmt.setInt(1, i);               // keycol
            insert_stmt.setInt(2, i * 10);          // indcol1
            insert_stmt.setInt(3, i * 100);         // indcol2
            insert_stmt.setInt(4, -i);              // indcol3
            insert_stmt.setString(5, data1_str);   // data1_data
            insert_stmt.setString(6, data2_str);   // data2_data

            insert_stmt.execute();
        }

        if (create_table)
        {
            Statement s = conn.createStatement();

            s.execute(
                "create index " + tblname + "_idx_keycol on " + tblname +
                    "(keycol)");
            s.execute(
                "create index " + tblname + "_idx_indcol1 on " + tblname +
                    "(indcol1)");
            s.execute(
                "create index " + tblname + "_idx_indcol2 on " + tblname +
                    "(indcol2)");
            s.execute(
                "create unique index " + tblname + "_idx_indcol3 on " + tblname +
                    "(indcol3)");
            s.close();
        }

        conn.commit();
    }

    private void executeQuery(
    Connection  conn,
    String      stmt_str)
        throws SQLException
    {
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(stmt_str);
        conn.commit();
    }

    private void log_wrong_count(
    String  error_msg,
    String  table_name,
    int     num_rows,
    int     expected_val,
    int     actual_val,
    int[]   before_info,
    int[]   after_info)
    {
        System.out.println(error_msg);
        System.out.println("ERROR: for " + num_rows + " row  test. Expected " + expected_val + ", but got " + actual_val );
    }


    private void row_count_based_tests(
    Connection  conn,
    boolean     create_table,
    boolean     drop_table,
    String      schemaName,
    String      table_name,
    int         num_rows) 
        throws SQLException 
    {
        testProgress(
            "begin " + num_rows + " row test, create = " + 
                create_table + ", drop = " + drop_table + ".");


        createAndLoadTable(conn, create_table, table_name, num_rows);

        if (verbose)
            testProgress("Calling compress.");

        // compress with no deletes should not affect size
        int[] ret_before = getSpaceInfo(conn, "APP", table_name);
        callCompress(conn, "APP", table_name, true, true, true);
        int[] ret_after  = getSpaceInfo(conn, "APP", table_name);

        if (ret_after[SPACE_INFO_NUM_ALLOC] != ret_before[SPACE_INFO_NUM_ALLOC])
        {
            log_wrong_count(
                "Expected no alloc page change.", 
                table_name, num_rows, 
                ret_before[SPACE_INFO_NUM_ALLOC], 
                ret_after[SPACE_INFO_NUM_ALLOC],
                ret_before, ret_after);
        }

        if (verbose)
            testProgress("calling consistency checker.");

        if (!checkConsistency(conn, schemaName, table_name))
        {
            logError("conistency check failed.");
        }

        testProgress("no delete case complete.");

        // delete all the rows.
        ret_before = getSpaceInfo(conn, "APP", table_name);
        executeQuery(conn, "delete from " + table_name);
        conn.commit();

        if (verbose)
            testProgress("deleted all rows, now calling compress.");

        callCompress(conn, "APP", table_name, true, true, true);
        ret_after  = getSpaceInfo(conn, "APP", table_name);

        // An empty table has 2 pages, one allocation page and the 1st page
        // which will have a system row in it.  The space vti only reports
        // a count of the user pages so the count is 1.
        if (ret_after[SPACE_INFO_NUM_ALLOC] != 1)
        {
            log_wrong_count(
                "Expected all pages to be truncated.",
                table_name, num_rows, 1, ret_after[SPACE_INFO_NUM_ALLOC],
                ret_before, ret_after);
        }

        if (verbose)
            testProgress("calling consistency checker.");

        if (!checkConsistency(conn, schemaName, table_name))
        {
            logError("conistency check failed.");
        }

        testProgress("delete all rows case succeeded.");


        if (drop_table)
            executeQuery(conn, "drop table " + table_name);

        conn.commit();

        testProgress("end " + num_rows + " row test.");
    }

    /**
     * Test 1 alloc page test cases.
     * <p>
     * loop through testing interesting row count cases.  The cases are
     * 0  rows     - basic edge case, 2 page table: 1 alloc, 1 user page
     * 1  row      - another edge case, 2 page table: 1 alloc, 1 user page
     * TODO        - 3 page table case: 1 alloc, 1 user page, 1 user page freed
     * 10000 rows  - reasonable number of pages to test out, still 1 alloc page
     *
     * These tests can be run relatively quickly, not a lot of rows needed.
     * <p>
     *
     **/
    private void test1(Connection conn) 
        throws SQLException 
    {
        beginTest(conn, "test1");

        int[] test_cases = {0, 1, 50, 10000};

        for (int i = 0; i < test_cases.length; i++)
        {
            // first create new table and run the tests.
            row_count_based_tests(
                conn, true, false, "APP", "TEST1", test_cases[i]);

            // now rerun tests on existing table, which had all rows deleted
            // and truncated.
            row_count_based_tests(
                conn, false, true, "APP", "TEST1", test_cases[i]);
        }

        endTest(conn, "test1");
    }

    public void testList(Connection conn)
        throws SQLException
    {
        test1(conn);
    }

    public static void main(String[] argv) 
        throws Throwable
    {
        OnlineCompressTest test = new OnlineCompressTest();

   		ij.getPropertyArg(argv); 
        Connection conn = ij.startJBMS();
        System.out.println("conn 2 from ij.startJBMS() = " + conn);
        conn.setAutoCommit(false);

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
