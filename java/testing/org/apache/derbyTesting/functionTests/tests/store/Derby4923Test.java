package org.apache.derbyTesting.functionTests.tests.store;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

import org.apache.derby.shared.common.sanity.SanityManager;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestSuite;

import java.util.Arrays;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;


/*
Class org.apache.derbyTesting.functionTests.tests.store.Derby4923Test

Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/


/**

Test to reproduce DERBY-4923, An expanding update to the 2nd piece of a long
row which is on a page with 0 bytes free fails with an nospc.U error.  The
code should always reserve enough space in this case to in the worst case
change the row piece to another long pointer to another page.

**/

public class Derby4923Test extends StoreBaseTest
{
    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */

    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */

    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */

    /**************************************************************************
     * Public Methods of XXXX class:
     **************************************************************************
     */

    public Derby4923Test(String name) 
    {
        super(name);
    }

    
    /**
     * DERBY-4923 test case
     * <p>
     * The update error occurs with the following:
     *   o update of a long row which requires an update on it's overflow page
     *   o 4k page
     *   o the long row has 2 pieces, the second is a blob column and is 
     *     located on an overflow page.
     *   o the overflow page has no space on the page.
     *   o the row id's are large enough such that they require 2 bytes to 
     *     store (ie. greater than 64)
     *   o the piece located on the page already takes up the minimum size
     *     allowed for a row piece on an overflow page.
     *
     *
     * In order to get to this one needs multiple rows on the overflow page,
     * so that they can eat up the free space on the page.  The original user
     * report gave me multiple occurences of the issue.  It always was
     * on a overflow page that was full (sometimes 2 and 3 rows).  The reported
     * case was 4k pages and I was not able to reproduce on 32k.  The updage
     * of the blob column was greater than 4k.
     *
     * The test does the following:
     * o drop/create table
     * o insert 2000 small rows which will fill up a few pages with rows and
     *   make the next record id on those rows bigger than 64.
     * o delete all those rows.
     * o run compress to reclaim the space on all the pages and free up the 
     *   pages so they can be used for overflow pages.
     * o insert 3 short rows that will all end up on same main page.
     * o expand row 1 so that it takes up most of main page.
     * o update row 2 such that it becomes a long row with just the blob column
     *   on an overflow page.  And subsequently shrink the overflow portion
     *   such that it takes the minimum space allowed on an overflow page.
     *   end up on same overflow page.
     * o update row 3 such that it becomes a long row with overflow on same 
     *   page as row 2 with just the blob column
     *   on an overflow page.  Use a length so that it uses all free space on 
     *   the overflow page.
     * o Now update row 2 blob to make it bigger than 4k which causes the
     *   bug.
     **/
    public void testDERBY_4923()
        throws SQLException, java.lang.InterruptedException
    {

        // page 0 - container info/bit map, does not affect test

        // page 1 - 
        // row on it that can never be deleted so this page never can be
        // made free.

        Statement stmt = createStatement();

        PreparedStatement insert_stmt = 
            prepareStatement("INSERT INTO TESTBADUPDATE VALUES(?, ?)");

        PreparedStatement delete_stmt = 
            prepareStatement("DELETE FROM TESTBADUPDATE");

        PreparedStatement update_stmt = 
            prepareStatement("UPDATE TESTBADUPDATE set value = ? where id = ?");

        // insert a bunch of rows to use up record id's on pages and then delete
        // them all so that free pages have large record ids.
        //

        byte[] pad_blob = new byte[1];
        for (int i = 1000; i < 2000; i++)
        {
            insert_stmt.setInt(     1, i);
            insert_stmt.setBytes(   2, pad_blob);
            insert_stmt.executeUpdate();
        }
        commit();
        delete_stmt.executeUpdate();
        commit();

        // reclaim all the space from the deletes but don't shrink the file
        // as we want pages left with big record id's.
        callCompress(
            stmt.getConnection(),
            "APP",
            "TESTBADUPDATE",
            true,
            true,
            false,
            true);
        commit();

        // insert 3 rows that will fit on same main page.
        pad_blob = new byte[1];

        for (int i = 1; i < 4; i++)
        {
            insert_stmt.setInt(     1, i);
            insert_stmt.setBytes(   2, pad_blob);
            insert_stmt.executeUpdate();
        }

        commit();

        // expand row id 1 such that it fills most of the page

        pad_blob = new byte[3000];
        Arrays.fill(pad_blob, (byte) 0x70);
        update_stmt.setBytes(   1, pad_blob);
        update_stmt.setInt(     2, 1);
        update_stmt.executeUpdate();
        commit();


        // now expand rows 2 and 3 each becomes a "long row", with
        // first column on main page with a pointer to overflow page, and each
        // 2nd column exists in full on the overflow page.  Want
        // each overflow to end up on same page.  
        //

        // xpand row 2 so it becomes a long row and then shrink the space back
        // on overflow page.
        pad_blob = new byte[1500];
        Arrays.fill(pad_blob, (byte) 0x70);
        update_stmt.setBytes(   1, pad_blob);
        update_stmt.setInt(     2, 2);
        update_stmt.executeUpdate();
        commit();

        pad_blob = new byte[4];
        Arrays.fill(pad_blob, (byte) 0x70);
        update_stmt.setBytes(   1, pad_blob);
        update_stmt.setInt(     2, 2);
        update_stmt.executeUpdate();
        commit();

        /*
        callCompress(
            stmt.getConnection(),
            "APP",
            "TESTBADUPDATE",
            true,
            true,
            false,
            true);
        */

        // xpand row 3 so it becomes a long row and fills the overflow page
        // on overflow page.
        pad_blob = new byte[3988];
        Arrays.fill(pad_blob, (byte) 0x70);
        update_stmt.setBytes(   1, pad_blob);
        update_stmt.setInt(     2, 3);
        update_stmt.executeUpdate();
        commit();

        // see if we can update the column of row 2 to expand it, this causes
        // the bug.
        pad_blob = new byte[19680];
        Arrays.fill(pad_blob, (byte) 0x70);
        update_stmt.setBytes(   1, pad_blob);
        update_stmt.setInt(     2, 2);
        update_stmt.executeUpdate();
        commit();

        stmt.close();
        insert_stmt.close();
        update_stmt.close();
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

    
    protected static Test baseSuite(String name) 
    {
        TestSuite suite = new TestSuite(name);
        suite.addTestSuite(Derby4923Test.class);
        return new CleanDatabaseTestSetup(
                DatabasePropertyTestSetup.setLockTimeouts(suite, 2, 4)) 
        {
            /**
             * Creates the tables used in the test cases.
             * @exception SQLException if a database error occurs
             */
            protected void decorateSQL(Statement stmt) throws SQLException
            {
                Connection conn = stmt.getConnection();

                CallableStatement cSt = 
                    conn.prepareCall(
                        "call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
                        "'derby.storage.pageSize', '4096')");
                cSt.execute();

                // create a table, with pagesize setting it will be 4k page size
                stmt.executeUpdate(
                    "CREATE TABLE TESTBADUPDATE (id int, value blob(1M))");


                conn.setAutoCommit(false);
            }
        };
    }

    public static Test suite() 
    {
        TestSuite suite = new TestSuite("Derby4923Test");
        suite.addTest(baseSuite("Derby4923Test:embedded"));
        return suite;
    }
}
