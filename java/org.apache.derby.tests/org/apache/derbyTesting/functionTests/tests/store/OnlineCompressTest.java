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

package org.apache.derbyTesting.functionTests.tests.store;

import org.apache.derby.shared.common.sanity.SanityManager;

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

//IC see: https://issues.apache.org/jira/browse/DERBY-132
        if (commit_operation)
            conn.commit();
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
//IC see: https://issues.apache.org/jira/browse/DERBY-361
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
     * Create and load a table with large columns.
     * <p>
     * If create_table is set creates a test data table with indexes.
     * Loads num_rows into the table.  This table defaults to 32k page size.
     * <p>
     *
     *
     * @param conn          Connection to use for sql execution.
     * @param create_table  If true, create new table - otherwise load into
     *                      existing table.
     * @param tblname       table to use.
     * @param num_rows      number of rows to add to the table.
     * @param start_value   Starting number from which num_rows are inserted
     * @exception  StandardException  Standard exception policy.
     **/
    protected void createAndLoadLargeTable(
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

	    // Derby-606. Note that this table is currently only used by Test6.
	    // Test6 needs data be to spread over 2 AllocExtents
	    // and this table schema is chosen so that the required scenario
	    // is exposed in minimum test execution time.
            s.execute(
                "create table " + tblname + 
                    "(keycol int, indcol1 int, indcol2 int, data1 char(24), data2 char(24), data3 char(24)," +
			    "data4 char(24), data5 char(24), data6 char(24), data7 char(24), data8 char(24)," + 
			    "data9 char(24), data10 char(24), inddec1 decimal(8), indcol3 int, indcol4 int, data11 varchar(50))");
            s.close();
        }

        PreparedStatement insert_stmt = 
            conn.prepareStatement(
                "insert into " + tblname + " values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

        char[]  data1_data = new char[24];
        char[]  data2_data = new char[24];
        char[]  data3_data = new char[24];
        char[]  data4_data = new char[24];
        char[]  data5_data = new char[24];
        char[]  data6_data = new char[24];
        char[]  data7_data = new char[24];
        char[]  data8_data = new char[24];
        char[]  data9_data = new char[24];
        char[]  data10_data = new char[24];
        char[]  data11_data = new char[50];

        for (int i = 0; i < data1_data.length; i++) 
	{
            data1_data[i] = 'a';
            data2_data[i] = 'b';
            data3_data[i] = 'c';
            data4_data[i] = 'd';
            data5_data[i] = 'e';
            data6_data[i] = 'f';
            data7_data[i] = 'g';
            data8_data[i] = 'h';
            data9_data[i] = 'i';
            data10_data[i] = 'j';
	}
	for( int i=0; i < data11_data.length; i++) 
	{
	    data11_data[i] = 'z';
        }

        String  data1_str = new String(data1_data);
        String  data2_str = new String(data2_data);
        String  data3_str = new String(data3_data);
        String  data4_str = new String(data4_data);
        String  data5_str = new String(data5_data);
        String  data6_str = new String(data6_data);
        String  data7_str = new String(data7_data);
        String  data8_str = new String(data8_data);
        String  data9_str = new String(data9_data);
        String  data10_str = new String(data10_data);
        String  data11_str = new String(data11_data);

        int row_count = 0;
        try
        {
            for (int i = start_value; row_count < num_rows; row_count++, i++)
            {
                insert_stmt.setInt(1, i);               // keycol
                insert_stmt.setInt(2, i * 10);          // indcol1
                insert_stmt.setInt(3, i * 100);         // indcol2
                insert_stmt.setString(4, data1_str);    // data1_data
                insert_stmt.setString(5, data2_str);    // data2_data
                insert_stmt.setString(6, data3_str);    // data3_data
                insert_stmt.setString(7, data4_str);    // data4_data
                insert_stmt.setString(8, data5_str);    // data5_data
                insert_stmt.setString(9, data6_str);    // data6_data
                insert_stmt.setString(10, data7_str);    // data7_data
                insert_stmt.setString(11, data8_str);    // data8_data
                insert_stmt.setString(12, data9_str);    // data9_data
                insert_stmt.setString(13, data10_str);    // data10_data
                insert_stmt.setInt(14, i * 20);          // indcol3
                insert_stmt.setInt(15, i * 200);         // indcol4
		insert_stmt.setInt(16, i * 50);
                insert_stmt.setString(17, data11_str);    // data11_data

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
//IC see: https://issues.apache.org/jira/browse/DERBY-132
        "    IS_INDEX         =" + before_info[SPACE_INFO_IS_INDEX]     + 
        "\n    NUM_ALLOC        =" + before_info[SPACE_INFO_NUM_ALLOC]    +
        "\n    NUM_FREE         =" + before_info[SPACE_INFO_NUM_FREE]     +
//IC see: https://issues.apache.org/jira/browse/DERBY-1187
//IC see: https://issues.apache.org/jira/browse/DERBY-1188
        "\n    NUM_UNFILLED     =" + before_info[SPACE_INFO_NUM_UNFILLED] +
        "\n    PAGE_SIZE        =" + before_info[SPACE_INFO_PAGE_SIZE]    +
        "\n    ESTIMSPACESAVING =" + before_info[SPACE_INFO_ESTIMSPACESAVING]);
        System.out.println("after_info:");
        System.out.println(
        "    IS_INDEX         =" + after_info[SPACE_INFO_IS_INDEX]       + 
        "\n    NUM_ALLOC        =" + after_info[SPACE_INFO_NUM_ALLOC]    +
        "\n    NUM_FREE         =" + after_info[SPACE_INFO_NUM_FREE]     +
        "\n    NUM_UNFILLED     =" + after_info[SPACE_INFO_NUM_UNFILLED] +
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

        callWaitForPostCommit(conn);

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

//IC see: https://issues.apache.org/jira/browse/DERBY-132
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

        callWaitForPostCommit(conn);

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
//IC see: https://issues.apache.org/jira/browse/DERBY-361

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

        callWaitForPostCommit(conn);
//IC see: https://issues.apache.org/jira/browse/DERBY-6502

        // compress all space and commit.
        callCompress(conn, "APP", table_name, true, true, true, true);

        // add back all rows and commit.
        if (long_table)
            createAndLoadLongTable(conn, create_table, table_name, num_rows);
        else
//IC see: https://issues.apache.org/jira/browse/DERBY-361
//IC see: https://issues.apache.org/jira/browse/DERBY-361
//IC see: https://issues.apache.org/jira/browse/DERBY-361
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

        callWaitForPostCommit(conn);
//IC see: https://issues.apache.org/jira/browse/DERBY-6502

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

    /**
     * Create and load table for test5.
     * <p>
     * schema of table:
     *     keycol   int, 
     *     onehalf  int, 
     *     onethird int, 
     *     c        varchar(300)
     *
     * @param conn          Connection to use for sql execution.
     * @param schemaName the schema to use.
     * @param table_name the table to use.
     * @param num_rows      number of rows to add to the table.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    private void test5_load(
//IC see: https://issues.apache.org/jira/browse/DERBY-1187
//IC see: https://issues.apache.org/jira/browse/DERBY-1188
    Connection  conn,
    String      schemaName,
    String      table_name,
    int         num_rows)
        throws SQLException
    {
        Statement s = conn.createStatement();

        s.execute(
            "create table " + table_name + 
            " (keycol integer primary key, onehalf integer, onethird integer, c varchar(300))");
        s.close();

        PreparedStatement insert_stmt = 
            conn.prepareStatement(
                "insert into " + table_name + " values(?, ?, ?, ?)");

        char[]  data1_data = new char[200];

        for (int i = 0; i < data1_data.length; i++)
        {
            data1_data[i] = 'b';
        }
        String  data1_str = new String(data1_data);

        for (int i = 0; i < num_rows; i++)
        {
            insert_stmt.setInt(1, i);               // keycol
            insert_stmt.setInt(2, i % 2);           // onehalf:  0 or 1 
            insert_stmt.setInt(3, i % 3);           // onethird: 0, 1, or 3
            insert_stmt.setString(4, data1_str);    // c
            insert_stmt.execute();
        }

        conn.commit();
    }
    
    /**
     * Execute test5, simple defragement test. 
     * <p>
     * o delete every other row, defragment
     * o delete every third row, defragment
     * o delete last 1000 rows, defragment
     * o delete first 512 rows, defragment.
     * <p>
     * run test with at least 2000 rows.
     **/
    private void test5_run(
    Connection  conn,
    String      schemaName,
    String      table_name,
    int         num_rows)
        throws SQLException
    {
        testProgress("begin test5: " + num_rows + " row test.");

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

        // DELETE EVERY OTHER ROW, COMPRESS, CHECK
        //
        //

        // delete all the rows every other row.
        ret_before = getSpaceInfo(conn, "APP", table_name, true);
        executeQuery(
            conn, "delete from " + table_name + " where onehalf = 0", true);

        callWaitForPostCommit(conn);
//IC see: https://issues.apache.org/jira/browse/DERBY-6502

        if (verbose)
            testProgress("deleted every other row, now calling compress.");

        callCompress(conn, "APP", table_name, true, true, true, true);
        ret_after  = getSpaceInfo(conn, "APP", table_name, true);

        if (total_pages(ret_after) != total_pages(ret_before))
        {
            // currently deleting every other row does not add free or unfilled
            // pages to the container so defragment has nowhere to put the rows.

            log_wrong_count(
                "Expected no truncation.",
                table_name, num_rows, 1, ret_after[SPACE_INFO_NUM_ALLOC],
                ret_before, ret_after);
        }

        if (verbose)
            testProgress("calling consistency checker.");

        if (!checkConsistency(conn, schemaName, table_name))
        {
            logError("conistency check failed.");
        }

        // DELETE EVERY THIRD ROW in original dataset, COMPRESS, CHECK
        //
        //

        // delete every third row
        ret_before = getSpaceInfo(conn, "APP", table_name, true);
        executeQuery(
            conn, "delete from " + table_name + " where onethird = 0", true);

        callWaitForPostCommit(conn);
//IC see: https://issues.apache.org/jira/browse/DERBY-6502

        if (verbose)
            testProgress("deleted every third row, now calling compress.");

        callCompress(conn, "APP", table_name, true, true, true, true);
        ret_after  = getSpaceInfo(conn, "APP", table_name, true);

        if (total_pages(ret_after) != total_pages(ret_before))
        {
            // currently deleting every third row does not create any free 
            // or unfilled pages so defragment has no place to move rows.
            log_wrong_count(
                "Expected no truncation.",
                table_name, num_rows, 1, ret_after[SPACE_INFO_NUM_ALLOC],
                ret_before, ret_after);
        }

        if (verbose)
            testProgress("calling consistency checker.");

        if (!checkConsistency(conn, schemaName, table_name))
        {
            logError("conistency check failed.");
        }

        // DELETE top "half" of rows in original dataset, COMPRESS, CHECK
        //
        //

        // delete top "half" of the rows in the original dataset.
        ret_before = getSpaceInfo(conn, "APP", table_name, true);
        executeQuery(
            conn, "delete from " + table_name + " where keycol > " + 
            (num_rows / 2), true);

        callWaitForPostCommit(conn);
//IC see: https://issues.apache.org/jira/browse/DERBY-6502

        if (verbose)
            testProgress("deleted top half of the rows, now calling compress.");

        callCompress(conn, "APP", table_name, true, true, true, true);
        ret_after  = getSpaceInfo(conn, "APP", table_name, true);

        // compress should be able to clean up about 1/2 of the pages.
        if (verbose)
        {
            log_wrong_count(
                "deleted top half keys, spaceinfo:",
                table_name, num_rows, 
                ((total_pages(ret_before) / 2) + 2),
                ret_after[SPACE_INFO_NUM_ALLOC],
                ret_before, ret_after);
        }

        if (total_pages(ret_after) > ((total_pages(ret_before) / 2) + 2))
        {
            log_wrong_count(
                "Expected at least " + 
                (ret_before[SPACE_INFO_NUM_ALLOC] / 2 + 2) +
                " pages to be truncated.",
                table_name, num_rows, 1, ret_after[SPACE_INFO_NUM_ALLOC],
                ret_before, ret_after);
        }

        if (verbose)
            testProgress("calling consistency checker.");

        if (!checkConsistency(conn, schemaName, table_name))
        {
            logError("conistency check failed.");
        }

        // DELETE 1st 500 rows in original dataset, COMPRESS, CHECK
        //
        //

        // delete keys less than 500
        ret_before = getSpaceInfo(conn, "APP", table_name, true);
        executeQuery(
            conn, "delete from " + table_name + " where keycol < 500 ", true);

        callWaitForPostCommit(conn);
//IC see: https://issues.apache.org/jira/browse/DERBY-6502

        if (verbose)
            testProgress("deleted keys < 500, now calling compress.");

        callCompress(conn, "APP", table_name, true, true, true, true);
        ret_after  = getSpaceInfo(conn, "APP", table_name, true);

        if (verbose)
        {
            log_wrong_count(
                "deleted bottom 500 keys, spaceinfo:",
                table_name, num_rows, 
                (total_pages(ret_before) - 33),
                ret_after[SPACE_INFO_NUM_ALLOC],
                ret_before, ret_after);
        }

        // The bottom 500 keys, assuming 4k pages, takes about 33 pages
        if (total_pages(ret_after) > (total_pages(ret_before) - 33))
        {
            log_wrong_count(
                "Expected at least 33 pages reclaimed.",
                table_name, num_rows, 1, ret_after[SPACE_INFO_NUM_ALLOC],
                ret_before, ret_after);
        }

        if (verbose)
            testProgress("calling consistency checker.");

        if (!checkConsistency(conn, schemaName, table_name))
        {
            logError("conistency check failed.");
        }


        conn.commit();

        testProgress("end test5: " + num_rows + " row test.");
    }

    /**
     * Cleanup after test5_run
     **/
    private void test5_cleanup(
    Connection  conn,
    String      schemaName,
    String      table_name,
    int         num_rows)
        throws SQLException
    {
        executeQuery(conn, "drop table " + table_name, true);
    }

    /**
     * Test 5 - simple defragment test.
     * <p>
     * Create dataset and then:
     * o delete every other row, defragment
     * o delete every third row, defragment
     * o delete last 1000 rows, defragment
     * o delete first 512 rows, defragment.
     * <p>
     * run test with at least 2000 rows.
     *
     **/
    private void test5(
    Connection  conn,
    String      test_name,
    String      table_name)
        throws SQLException 
    {
        beginTest(conn, test_name);

        int[] test_cases = {2000, 10000};

        for (int i = 0; i < test_cases.length; i++)
        {
            test5_load(conn, "APP", table_name, test_cases[i]);
            test5_run(conn, "APP", table_name, test_cases[i]);
            test5_cleanup(conn, "APP", table_name, test_cases[i]);
        }

        endTest(conn, test_name);
    }

    /**
     * Test 6 - Online compress test for table that spans more than 1 AllocExtent.
     * <p>
     * Create dataset with Data spread over more than 1 AllcExtent and then:
     * delete enough rows so that the last AllocExtent is empty.
     * Try OnlineCompress with Purge, Defragment and Truncate
     * <p>
     * run test with at least 103000 rows.
     *
     **/
    private void test6(
    Connection  conn,
    String      test_name,
    String      table_name)
        throws SQLException 
    {
        beginTest(conn, test_name);

        int[] noRows = {104000};

        for (int i = 0; i < noRows.length; i++)
        {
            // first create new table and run the tests.
            createAndLoadLargeTable(conn, true, table_name, noRows[i], 0);

        if (verbose)
            testProgress("Calling compress.");

        // compress with no deletes should not affect size
        int[] ret_before = getSpaceInfo(conn, "APP", table_name, true);
//IC see: https://issues.apache.org/jira/browse/DERBY-132
        callCompress(conn, "APP", table_name, true, true, true, true);
        int[] ret_after  = getSpaceInfo(conn, "APP", table_name, true);

        if (ret_after[SPACE_INFO_NUM_ALLOC] != ret_before[SPACE_INFO_NUM_ALLOC])
        {
            log_wrong_count(
                "Expected no alloc page change.", 
                table_name, noRows[i], 
                ret_before[SPACE_INFO_NUM_ALLOC], 
                ret_after[SPACE_INFO_NUM_ALLOC],
                ret_before, ret_after);
        }

        testProgress("no delete case complete.");

        // delete all the rows.
//IC see: https://issues.apache.org/jira/browse/DERBY-132
        ret_before = getSpaceInfo(conn, "APP", table_name, true);
        executeQuery(conn, "delete from " + table_name, true);

        callWaitForPostCommit(conn);
//IC see: https://issues.apache.org/jira/browse/DERBY-6502
//IC see: https://issues.apache.org/jira/browse/DERBY-6502

        conn.commit();
//IC see: https://issues.apache.org/jira/browse/DERBY-2549

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
                table_name, noRows[i], 1, ret_after[SPACE_INFO_NUM_ALLOC],
                ret_before, ret_after);
        }

        testProgress("delete all rows case succeeded.");

        testProgress("end simple deleteAllRows," + noRows[i] + " row test.");

            executeQuery(conn, "drop table " + table_name, true);
        }

        endTest(conn, test_name);
    }

    /**
     * Test 7 - Online compress test for fetching more rows than buffer limit.
     * <p>
     * For smaller row size, if number of rows per page is more than max buffer
     * size, then check if the remaining rows are also fetched for Compress 
     * Operation
     * <p>
     **/
    private void test7(
//IC see: https://issues.apache.org/jira/browse/DERBY-2549
    Connection  conn,
    String      test_name,
    String      table_name)
        throws SQLException 
    {
        beginTest(conn, test_name);

        Statement s = conn.createStatement();

        s.execute("create table " + table_name + "(keycol int)");
        s.close();
        PreparedStatement insert_stmt = 
            conn.prepareStatement("insert into " + table_name + " values(?)");
        try
        {
            for (int i = 0; i < 1200; i++)
            {
                insert_stmt.setInt(1, i);

                insert_stmt.execute();
            }
        }
        catch (SQLException sqle)
        {
            System.out.println(
                "Exception while trying to insert a row");
            throw sqle;
        }
        conn.commit();

        // delete the front rows leaving the last 200.  Post commit may reclaim
        // space on pages where all rows are deleted.  
        executeQuery(
            conn, "delete from " + table_name + " where keycol < 1000", true);

        callWaitForPostCommit(conn);
//IC see: https://issues.apache.org/jira/browse/DERBY-6502
//IC see: https://issues.apache.org/jira/browse/DERBY-6502

        conn.commit();

        if (verbose)
            testProgress("deleted first 1000 rows, now calling compress.");

        callCompress(conn, "APP", table_name, true, true, true, true);

        testProgress("delete rows case succeeded.");

        executeQuery(conn, "drop table " + table_name, true);

        endTest(conn, test_name);
    }

    public void testList(Connection conn)
        throws SQLException
    {
        test1(conn, "test1", "TEST1");
        // test2(conn, "test2", "TEST2");
        test3(conn, "test3", "TEST3");
        // test4(conn, "test4", "TEST4");
//IC see: https://issues.apache.org/jira/browse/DERBY-1187
//IC see: https://issues.apache.org/jira/browse/DERBY-1188
        test5(conn, "test5", "TEST5");
//IC see: https://issues.apache.org/jira/browse/DERBY-2549
        test6(conn, "test6", "TEST6");
        test7(conn, "test7", "TEST7");
    }

    public static void callWaitForPostCommit(Connection conn) 
//IC see: https://issues.apache.org/jira/browse/DERBY-6502
            throws SQLException {
        CallableStatement cstmt = 
                conn.prepareCall(
                    "call wait_for_post_commit()");
        cstmt.execute();
        cstmt.close();
    }
    
    public static void main(String[] argv) 
        throws Throwable
    {
        OnlineCompressTest test = new OnlineCompressTest();

   		ij.getPropertyArg(argv); 
        Connection conn = ij.startJBMS();
        
//IC see: https://issues.apache.org/jira/browse/DERBY-6502
        Statement stmt = conn.createStatement();
        // Create a procedure to be called before checking on contents
        // to ensure that the background worker thread has completed 
        // all the post-commit work.
        stmt.execute(
            "CREATE PROCEDURE WAIT_FOR_POST_COMMIT() " +
            "LANGUAGE JAVA EXTERNAL NAME " +
            "'org.apache.derbyTesting.functionTests.util." +
            "T_Access.waitForPostCommitToFinish' " +
            "PARAMETER STYLE JAVA");
        conn.setAutoCommit(false);
        stmt.close();

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
