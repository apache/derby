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
    boolean verbose = false;

    public OnlineCompressTest()
    {
    }

    /**
     * call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE() system procedure.
     * <p>
     * Utility test function to call the system procedure.
     *
     **/
    protected void callCompress(
    Connection  conn,
    String      schemaName,
    String      tableName,
    boolean     purgeRows,
    boolean     defragmentRows,
    boolean     truncateEnd,
    boolean     commit_operation)
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

        if (commit_operation)
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
    String      tableName,
    boolean     commit_xact)
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

        if (commit_xact)
            conn.commit();

        return(ret_info);
    }


    /**
     * Create and load a table.
     * <p>
     * If create_table is set creates a test data table with indexes.
     * Loads num_rows into the table.  This table defaults to 32k page size.
     * This schema fits 25 rows per page
     * <p>
     *
     *
     * @param conn          Connection to use for sql execution.
     * @param create_table  If true, create new table - otherwise load into
     *                      existing table.
     * @param tblname       table to use.
     * @param num_rows      number of rows to add to the table.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    protected void createAndLoadTable(
    Connection  conn,
    boolean     create_table,
    String      tblname,
    int         num_rows,
    int         start_value)
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

        int row_count = 0;
        try
        {
            for (int i = start_value; row_count < num_rows; row_count++, i++)
            {
                insert_stmt.setInt(1, i);               // keycol
                insert_stmt.setInt(2, i * 10);          // indcol1
                insert_stmt.setInt(3, i * 100);         // indcol2
                insert_stmt.setInt(4, -i);              // indcol3
                insert_stmt.setString(5, data1_str);    // data1_data
                insert_stmt.setString(6, data2_str);    // data2_data

                insert_stmt.execute();
            }
        }
        catch (SQLException sqle)
        {
            System.out.println(
                "Exception while trying to insert row number: " + row_count);
            throw sqle;
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

    /**
     * Create and load a table with long columns and long rows.
     * <p>
     * If create_table is set creates a test data table with indexes.
     * Loads num_rows into the table.  This table defaults to 32k page size.
     * <p>
     * schema of table:
     *     keycol   int, 
     *     longcol1 clob(200k),
     *     longrow1 varchar(10000),
     *     longrow2 varchar(10000),
     *     longrow3 varchar(10000),
     *     longrow4 varchar(10000),
     *     indcol1  int, 
     *     indcol2  int, 
     *     indcol3  int, 
     *     data1    varchar(2000), 
     *     data2    varchar(2000)
     *     longrow5 varchar(10000),
     *     longrow6 varchar(10000),
     *     longrow7 varchar(10000),
     *     longrow8 varchar(10000),
     *     longcol2 clob(200k),
     *
     *
     * @param conn          Connection to use for sql execution.
     * @param create_table  If true, create new table - otherwise load into
     *                      existing table.
     * @param tblname       table to use.
     * @param num_rows      number of rows to add to the table.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    private void createAndLoadLongTable(
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
                " (keycol   int, longcol1 clob(200k), longrow1 varchar(10000), longrow2 varchar(10000), longrow3 varchar(10000), longrow4 varchar(10000), indcol1  int, indcol2  int, indcol3  int, data1    varchar(2000), data2    varchar(2000), longrow5 varchar(10000), longrow6 varchar(10000), longrow7 varchar(10000), longrow8 varchar(10000), longcol2 clob(200k))");
            s.close();
        }

        PreparedStatement insert_stmt = 
            conn.prepareStatement(
                "insert into " + tblname + " values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

        char[]  data1_data = new char[500];
        char[]  data2_data = new char[500];

        for (int i = 0; i < data1_data.length; i++)
        {
            data1_data[i] = 'a';
            data2_data[i] = 'b';
        }
        String  data1_str = new String(data1_data);
        String  data2_str = new String(data2_data);

        // some data to force row to be bigger than a page, ie. long row
        char[] data3_data = new char[10000];
        char[] data4_data = new char[10000];

        for (int i = 0; i < data3_data.length; i++)
        {
            data3_data[i] = 'c';
            data4_data[i] = 'd';
        }
        String  data3_str = new String(data3_data);
        String  data4_str = new String(data4_data);

        // some data for the long columns
        char[] data5_data = new char[200000];
        char[] data6_data = new char[200000];

        for (int i = 0; i < data5_data.length; i++)
        {
            data5_data[i] = 'e';
            data6_data[i] = 'f';
        }

        String  data5_str = new String(data5_data);
        String  data6_str = new String(data6_data);

        for (int i = 0; i < num_rows; i++)
        {
            insert_stmt.setInt(1, i);               // keycol
            insert_stmt.setString(2, data5_str);    // longcol1
            insert_stmt.setString(3, data3_str);    // longrow1
            insert_stmt.setString(4, data3_str);    // longrow2
            insert_stmt.setString(5, data3_str);    // longrow3
            insert_stmt.setString(6, data3_str);    // longrow4
            insert_stmt.setInt(7, i * 10);          // indcol1
            insert_stmt.setInt(8, i * 100);         // indcol2
            insert_stmt.setInt(9, -i);              // indcol3
            insert_stmt.setString(10, data1_str);   // data1_data
            insert_stmt.setString(11, data2_str);   // data2_data
            insert_stmt.setString(12, data4_str);   // longrow5
            insert_stmt.setString(13, data4_str);   // longrow6
            insert_stmt.setString(14, data4_str);   // longrow7
            insert_stmt.setString(15, data4_str);   // longrow8
            insert_stmt.setString(16, data5_str);   // longcol2

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
        System.out.println("before_info:");
        System.out.println(
        "    IS_INDEX         =" + before_info[SPACE_INFO_IS_INDEX]     + 
        "\n    NUM_ALLOC        =" + before_info[SPACE_INFO_NUM_ALLOC]    +
        "\n    NUM_FREE         =" + before_info[SPACE_INFO_NUM_FREE]     +
        "\n    PAGE_SIZE        =" + before_info[SPACE_INFO_PAGE_SIZE]    +
        "\n    ESTIMSPACESAVING =" + before_info[SPACE_INFO_ESTIMSPACESAVING]);
        System.out.println("after_info:");
        System.out.println(
        "    IS_INDEX         =" + after_info[SPACE_INFO_IS_INDEX]     + 
        "\n    NUM_ALLOC        =" + after_info[SPACE_INFO_NUM_ALLOC]    +
        "\n    NUM_FREE         =" + after_info[SPACE_INFO_NUM_FREE]     +
        "\n    PAGE_SIZE        =" + after_info[SPACE_INFO_PAGE_SIZE]    +
        "\n    ESTIMSPACESAVING =" + after_info[SPACE_INFO_ESTIMSPACESAVING]);
    }


    private void deleteAllRows(
    Connection  conn,
    boolean     create_table,
    boolean     long_table,
    String      schemaName,
    String      table_name,
    int         num_rows) 
        throws SQLException 
    {
        testProgress(
            "begin deleteAllRows," + num_rows + " row test, create = " + 
                create_table + ".");


        if (long_table)
            createAndLoadLongTable(conn, create_table, table_name, num_rows);
        else
            createAndLoadTable(conn, create_table, table_name, num_rows, 0);

        if (verbose)
            testProgress("Calling compress.");

        // compress with no deletes should not affect size
        int[] ret_before = getSpaceInfo(conn, "APP", table_name, true);
        callCompress(conn, "APP", table_name, true, true, true, true);
        int[] ret_after  = getSpaceInfo(conn, "APP", table_name, true);

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
        ret_before = getSpaceInfo(conn, "APP", table_name, true);
        executeQuery(conn, "delete from " + table_name, true);

        if (verbose)
            testProgress("deleted all rows, now calling compress.");

        callCompress(conn, "APP", table_name, true, true, true, true);
        ret_after  = getSpaceInfo(conn, "APP", table_name, true);

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

        conn.commit();

        testProgress("end deleteAllRows," + num_rows + " row test.");
    }

    private void simpleDeleteAllRows(
    Connection  conn,
    boolean     create_table,
    boolean     long_table,
    String      schemaName,
    String      table_name,
    int         num_rows) 
        throws SQLException 
    {
        testProgress(
            "begin simpleDeleteAllRows," + num_rows + " row test, create = " + 
                create_table + ".");


        if (long_table)
            createAndLoadLongTable(conn, create_table, table_name, num_rows);
        else
            createAndLoadTable(conn, create_table, table_name, num_rows, 0);

        if (verbose)
            testProgress("Calling compress.");

        // compress with no deletes should not affect size
        int[] ret_before = getSpaceInfo(conn, "APP", table_name, true);
        callCompress(conn, "APP", table_name, true, true, true, true);
        int[] ret_after  = getSpaceInfo(conn, "APP", table_name, true);

        if (ret_after[SPACE_INFO_NUM_ALLOC] != ret_before[SPACE_INFO_NUM_ALLOC])
        {
            log_wrong_count(
                "Expected no alloc page change.", 
                table_name, num_rows, 
                ret_before[SPACE_INFO_NUM_ALLOC], 
                ret_after[SPACE_INFO_NUM_ALLOC],
                ret_before, ret_after);
        }

        testProgress("no delete case complete.");

        // delete all the rows.
        ret_before = getSpaceInfo(conn, "APP", table_name, true);
        executeQuery(conn, "delete from " + table_name, true);

        if (verbose)
            testProgress("deleted all rows, now calling compress.");

        callCompress(conn, "APP", table_name, true, true, true, true);
        ret_after  = getSpaceInfo(conn, "APP", table_name, true);

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

        testProgress("delete all rows case succeeded.");

        conn.commit();

        testProgress("end simple deleteAllRows," + num_rows + " row test.");
    }

    /**
     * Check/exercise purge pass phase.
     * <p>
     * Assumes that either test creates the table, or called on an empty
     * table with no committed deleted rows or free pages in the middle of
     * the table in it.
     * <p>
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    private void checkPurgePhase(
    Connection  conn,
    boolean     create_table,
    boolean     long_table,
    String      schemaName,
    String      table_name,
    int         num_rows) 
        throws SQLException 
    {
        testProgress(
            "begin checkPurgePhase" + num_rows + " row test, create = " + 
                create_table + ".");

        if (long_table)
            createAndLoadLongTable(conn, create_table, table_name, num_rows);
        else
            createAndLoadTable(conn, create_table, table_name, num_rows, 0);

        // dump_table(conn, schemaName, table_name, false);

        // delete all the rows, but don't commit the delete
        int[] ret_before = getSpaceInfo(conn, "APP", table_name, false);
        executeQuery(conn, "delete from " + table_name, false);


        // dump_table(conn, schemaName, table_name, false);

        // Purge pass on non-committed deleted rows should do nothing.  

        // System.out.println("lock info before compress call:\n " + get_lock_info(conn, true));

        // Calling compress with just the "purge" pass option, no commit called.
        callCompress(conn, "APP", table_name, true, false, false, false);

        int[] ret_after  = getSpaceInfo(conn, "APP", table_name, false);

        // expect no change in the number of allocated pages!
        if (ret_after[SPACE_INFO_NUM_ALLOC] != ret_before[SPACE_INFO_NUM_ALLOC])
        {
            log_wrong_count(
                "Expected no alloc page change(1).", 
                table_name, num_rows, 
                ret_before[SPACE_INFO_NUM_ALLOC], 
                ret_after[SPACE_INFO_NUM_ALLOC],
                ret_before, ret_after);
        }

        // expect no change in the number of free pages, if there are there
        // is a problem with purge locking recognizing committed deleted rows.
        if (ret_after[SPACE_INFO_NUM_FREE] != ret_before[SPACE_INFO_NUM_FREE])
        {
            log_wrong_count(
                "Expected no free page change(1).", 
                table_name, num_rows, 
                ret_before[SPACE_INFO_NUM_FREE], 
                ret_after[SPACE_INFO_NUM_FREE],
                ret_before, ret_after);
        }

        // Test that it is ok to call multiple purge passes in single xact.

        // Calling compress with just the "purge" pass option, no commit called.
        callCompress(conn, "APP", table_name, true, false, false, false);
        ret_after  = getSpaceInfo(conn, "APP", table_name, false);

        // expect no change in the number of allocated pages!
        if (ret_after[SPACE_INFO_NUM_ALLOC] != ret_before[SPACE_INFO_NUM_ALLOC])
        {
            log_wrong_count(
                "Expected no alloc page change(2).", 
                table_name, num_rows, 
                ret_before[SPACE_INFO_NUM_ALLOC], 
                ret_after[SPACE_INFO_NUM_ALLOC],
                ret_before, ret_after);
        }

        // expect no change in the number of free pages, if there are there
        // is a problem with purge locking recognizing committed deleted rows.
        if (ret_after[SPACE_INFO_NUM_FREE] != ret_before[SPACE_INFO_NUM_FREE])
        {
            log_wrong_count(
                "Expected no free page change(2).", 
                table_name, num_rows, 
                ret_before[SPACE_INFO_NUM_FREE], 
                ret_after[SPACE_INFO_NUM_FREE],
                ret_before, ret_after);
        }

        // since table was just loaded a defragment pass also should
        // not find anything to do.
        
        // Calling compress with just the "defragment" option, no commit called.

        // currently the defragment option requires a table level lock in
        // the nested user transaction, which will conflict and cause a
        // lock timeout.

        try
        {
            callCompress(conn, "APP", table_name, false, true, false, false);
            
            logError("Defragment pass did not get a lock timeout.");
        }
        catch (SQLException sqle)
        {
            // ignore exception.
        }

        ret_after  = getSpaceInfo(conn, "APP", table_name, false);

        if (ret_after[SPACE_INFO_NUM_ALLOC] != ret_before[SPACE_INFO_NUM_ALLOC])
        {
            log_wrong_count(
                "Expected no alloc page change(3).", 
                table_name, num_rows, 
                ret_before[SPACE_INFO_NUM_ALLOC], 
                ret_after[SPACE_INFO_NUM_ALLOC],
                ret_before, ret_after);
        }
        if (ret_after[SPACE_INFO_NUM_FREE] != ret_before[SPACE_INFO_NUM_FREE])
        {
            log_wrong_count(
                "Expected no free page change(3).", 
                table_name, num_rows, 
                ret_before[SPACE_INFO_NUM_FREE], 
                ret_after[SPACE_INFO_NUM_FREE],
                ret_before, ret_after);
        }


        // make sure table is back to all deleted row state.  lock timeout
        // will abort transaction.

        // delete all rows and commit.
        executeQuery(conn, "delete from " + table_name, true);

        // compress all space and commit.
        callCompress(conn, "APP", table_name, true, true, true, true);

        // add back all rows and commit.
        if (long_table)
            createAndLoadLongTable(conn, create_table, table_name, num_rows);
        else
            createAndLoadTable(conn, create_table, table_name, num_rows, 0);
        conn.commit();

        // delete all rows, and NO commit.
        executeQuery(conn, "delete from " + table_name, false);


        // Calling compress with just the truncate option, may change allocated
        // and free page count as they system may have preallocated pages to
        // the end of the file as part of the load.  The file can't shrink
        // any more than the free page count before the compress.

        // running the truncate pass only.  If it compresses anything it is
        // just the preallocated pages at end of the file.

        // currently the defragment option requires a table level lock in
        // the nested user transaction, which will conflict and cause a
        // lock timeout.


        ret_before = getSpaceInfo(conn, "APP", table_name, false);
        callCompress(conn, "APP", table_name, false, false, true, false);
        ret_after  = getSpaceInfo(conn, "APP", table_name, false);

        // expect no change in the number of allocated pages!
        if (ret_after[SPACE_INFO_NUM_ALLOC] != ret_before[SPACE_INFO_NUM_ALLOC])
        {
            log_wrong_count(
                "Expected no alloc page change(4).", 
                table_name, num_rows, 
                ret_before[SPACE_INFO_NUM_ALLOC], 
                ret_after[SPACE_INFO_NUM_ALLOC],
                ret_before, ret_after);
        }

        // The only space that truncate only pass can free are free pages 
        // located at end of file, so after free space can be anywhere from 
        // what it was before to 0 pages.
        if (ret_after[SPACE_INFO_NUM_FREE] > ret_before[SPACE_INFO_NUM_FREE])
        {
            log_wrong_count(
                "Expected no increase in free pages(4).", 
                table_name, num_rows, 
                ret_before[SPACE_INFO_NUM_FREE], 
                ret_after[SPACE_INFO_NUM_FREE],
                ret_before, ret_after);
        }

        // now commit the deletes, run all phases and make sure empty table
        // results.
        conn.commit();

        // check the table.  Note that this will accumulate locks and
        // will commit the transaction.
        if (!checkConsistency(conn, schemaName, table_name))
        {
            logError("conistency check failed.");
        }

        // test running each phase in order.
        callCompress(conn, "APP", table_name, true,  false, false, false);
        callCompress(conn, "APP", table_name, false, true,  false, false);
        callCompress(conn, "APP", table_name, false, false, true , false);
        ret_after  = getSpaceInfo(conn, "APP", table_name, false);

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
        if (ret_after[SPACE_INFO_NUM_FREE] != 0)
        {
            log_wrong_count(
                "Expected no free page after all pages truncated.",
                table_name, num_rows, 1, ret_after[SPACE_INFO_NUM_ALLOC],
                ret_before, ret_after);
        }

        if (verbose)
            testProgress("calling consistency checker.");

        if (!checkConsistency(conn, schemaName, table_name))
        {
            logError("conistency check failed.");
        }

        testProgress("end checkPurgePhase" + num_rows + " row test.");
    }

    /**
     * Test 1 - various # page tests, regular row/columns
     * <p>
     * perform a number of insert/delete/compress operations on a variety
     * of sized tables, use space allocation information to verify that
     * compression is happening and use consistency checker to verify that
     * tables and indexes are all valid following the operations.
     * <p>
     * loop through testing interesting row count cases.  The cases are
     * 0    rows  - basic edge case, 2 page table: 1 alloc, 1 user page
     * 1    row   - another edge case, 2 page table: 1 alloc, 1 user page
     * 50   rows  - 3 page table case: 1 alloc, 1 user page, 1 user page freed
     * 4000 rows  - reasonable number of pages to test out, still 1 alloc page
     *
     * note that row numbers greater than 4000 may lead to lock escalation
     * issues, if queries like "delete from x" are used to delete all the 
     * rows.
     *
     * <p>
     *
     **/
    private void test1(
    Connection  conn,
    String      test_name,
    String      table_name)
        throws SQLException 
    {
        beginTest(conn, test_name);

        int[] test_cases = {0, 1, 50, 4000};

        for (int i = 0; i < test_cases.length; i++)
        {
            // first create new table and run the tests.
            deleteAllRows(
                conn, true, false, "APP", table_name, test_cases[i]);

            // now rerun tests on existing table, which had all rows deleted
            // and truncated.
            deleteAllRows(
                conn, false, false, "APP", table_name, test_cases[i]);

            checkPurgePhase(
                conn, false, false, "APP", table_name, test_cases[i]);

            executeQuery(conn, "drop table " + table_name, true);
        }

        endTest(conn, test_name);
    }

    /**
     * Test 2 - check repeated delete tests.
     * <p>
     * There was a timing error where test1 would usually pass, but 
     * repeated execution of this test found a timing problem with
     * allocation using an "unallocated" page and getting an I/O error.
     *
     **/
    private void test2(
    Connection  conn,
    String      test_name,
    String      table_name)
        throws SQLException 
    {
        beginTest(conn, test_name);

        int[] test_cases = {4000};

        for (int i = 0; i < test_cases.length; i++)
        {
            // first create new table and run the tests.
            simpleDeleteAllRows(
                conn, true, false, "APP", table_name, test_cases[i]);

            for (int j = 0; j < 100; j++)
            {

                // now rerun tests on existing table, which had all rows deleted
                // and truncated.
                deleteAllRows(
                    conn, false, false, "APP", table_name, test_cases[i]);
            }

            executeQuery(conn, "drop table " + table_name, true);
        }

        endTest(conn, test_name);
    }



    /**
     * Test 3 - various # page tests, long row and long columns
     * <p>
     * perform a number of insert/delete/compress operations on a variety
     * of sized tables, use space allocation information to verify that
     * compression is happening and use consistency checker to verify that
     * tables and indexes are all valid following the operations.
     * <p>
     * loop through testing interesting row count cases.  The cases are
     * 0    rows  - basic edge case
     * 1    row   - another edge case
     * 100  rows  - ~50 meg table
     * 4000 rows  - ~2 gig table
     *
     * note that row numbers greater than 4000 may lead to lock escalation
     * issues, if queries like "delete from x" are used to delete all the 
     * rows.
     *
     * <p>
     *
     **/
    private void test3(
    Connection  conn,
    String      test_name,
    String      table_name)
        throws SQLException 
    {
        beginTest(conn, test_name);

        // note that 500 rows took 30 minutes on a ~1.5 ghz laptop
        int[] test_cases = {1, 2, 50};

        for (int i = 0; i < test_cases.length; i++)
        {
            // first create new table and run the tests.
            deleteAllRows(
                conn, true, true, "APP", table_name, test_cases[i]);

            // now rerun tests on existing table, which had all rows deleted
            // and truncated.
            deleteAllRows(
                conn, false, true, "APP", table_name, test_cases[i]);

            checkPurgePhase(
                conn, false, true, "APP", table_name, test_cases[i]);

            executeQuery(conn, "drop table " + table_name, true);
        }

        endTest(conn, test_name);
    }

    /**
     * Test 4 - check repeated delete tests.
     * <p>
     * There was a timing error where test1 would usually pass, but 
     * repeated execution of this test found a timing problem with
     * allocation using an "unallocated" page and getting an I/O error.
     *
     **/
    private void test4(
    Connection  conn,
    String      test_name,
    String      table_name)
        throws SQLException 
    {
        beginTest(conn, test_name);

        int[] test_cases = {4000};

        for (int i = 0; i < test_cases.length; i++)
        {

            for (int j = 0; j < 100; j++)
            {
                // first create new table and run the tests.
                simpleDeleteAllRows(
                    conn, true, false, "APP", table_name, test_cases[i]);

                // now rerun tests on existing table, which had all rows deleted
                // and truncated.
                deleteAllRows(
                    conn, false, false, "APP", table_name, test_cases[i]);

                executeQuery(conn, "drop table " + table_name, true);
            }

        }

        endTest(conn, test_name);
    }



    public void testList(Connection conn)
        throws SQLException
    {
        test1(conn, "test1", "TEST1");
        // test2(conn, "test2", "TEST2");
        test3(conn, "test3", "TEST3");
        // test4(conn, "test2", "TEST2");
    }

    public static void main(String[] argv) 
        throws Throwable
    {
        OnlineCompressTest test = new OnlineCompressTest();

   		ij.getPropertyArg(argv); 
        Connection conn = ij.startJBMS();
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
