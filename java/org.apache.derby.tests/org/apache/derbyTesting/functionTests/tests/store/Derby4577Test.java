package org.apache.derbyTesting.functionTests.tests.store;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;


/*
Class org.apache.derbyTesting.functionTests.tests.store.Derby4577Test
//IC see: https://issues.apache.org/jira/browse/DERBY-4770

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

Test to reproduce DERBY-4577, An expanding update fails with an nospc.U error.

**/

public class Derby4577Test extends StoreBaseTest
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

    public Derby4577Test(String name) 
    {
        super(name);
    }

    
    /**
     * DERBY-4577 test case
     * <p>
     * The update error occurs with the following:
     *   o update of a long row which requires an update on it's overflow page
     *   o The portion of the long row on the overflow page needs to have 
     *     max(row size, reserved space) + free space on page &lt;= 12  
     *     (12 causes the error, other values might also).
     *
     * In order to get to this one needs multiple rows on the overflow page,
     * so that they can eat up the free space on the page.  This test 
     * simulates the overflow page state that I got from running the test case 
     * associated with DERBY-2286.  I could only repro on a fast dual core
     * linux machine.  I repro'd a few times and it always had 3 rows on
     * the page: one that had a long column pointer, one that had a long
     * row pointer, and one that had the blob on the page eating up most of
     * the space on the overflow page.  
     *
     * The test does the following:
     * o drop/create table
     * o insert 3 rows of interest that will all fit on 1st page, with 1 byte
     *     blob columns.
     * o insert a dummy row that will fill up the rest of the 1st page.
     * o update 1st 3 rows so that they are now all long rows that share the
     *   same overflow page.
     * o update row 1 so that it now has a long column, this actually shrinks
     *   it on this overflow page.
     * o update row 2 so that blob column is bigger, but still less than a page
     *   this results in row 2 getting another long row pointer to a page that
     *   holds the new blob value.  Again this actually shrinks the row piece
     *   on the overflow page in question.
     * o update row 3 so that it's overflow piece fills up all the remaining
     *   space on the overflow page.
     * o finally update row 1's long column, this update causes the bug.  The
     *   no space error should never be thrown to a user on an update.  The only
     *   time an error of this type is allowed is if the actual disk is full.
     *
     **/
    public void testDERBY_4577()
        throws SQLException
    {

        // page 0 - container info/bit map, does not affect test

        // page 1 - 
        // row on it that can never be deleted so this page never can be
        // made free.

        Statement stmt = createStatement();

        PreparedStatement insert_stmt = 
            prepareStatement("INSERT INTO testBadUpdate VALUES(?, ?)");

        PreparedStatement update_stmt = 
            prepareStatement("UPDATE testBadUpdate set value = ? where id = ?");

        // insert 3 rows that will fit on same main page.
        byte[] pad_blob = new byte[1];

        for (int i = 0; i < 3; i++)
        {
            insert_stmt.setInt(     1, i);
            insert_stmt.setBytes(   2, pad_blob);
            insert_stmt.executeUpdate();
        }

        commit();

        // insert a row that fills rest of main page
        pad_blob = new byte[32000];
        insert_stmt.setInt(     1, 3);
        insert_stmt.setBytes(   2, pad_blob);
        insert_stmt.executeUpdate();

        // now expand each of the rows so that each becomes a "long row", with
        // first column on main page with a pointer to overflow page, and each
        // 2nd column exists in full on the overflow page.  Want
        // each overflow to end up on same page.  
        pad_blob = new byte[4000];
        for (int i = 0; i < 3; i++)
        {
            update_stmt.setBytes(   1, pad_blob);
            update_stmt.setInt(     2, i);
            update_stmt.executeUpdate();
        }

        commit();

        // eat up the rest of space on main page, by expanding the 4th row.
        pad_blob = new byte[32566];
        update_stmt.setBytes(   1, pad_blob);
        update_stmt.setInt(     2, 3);
        update_stmt.executeUpdate();
        commit();

        // expand row 1 so that it's blob column becomes a long column
        pad_blob = new byte[60000];
        update_stmt.setBytes(   1, pad_blob);
        update_stmt.setInt(     2, 0);
        update_stmt.executeUpdate();
        commit();

        // expand row 2 so that it's blob column becomes another long row
        // pointer.
        pad_blob = new byte[32500];
        update_stmt.setBytes(   1, pad_blob);
        update_stmt.setInt(     2, 1);
        update_stmt.executeUpdate();
        commit();
        
        // expand row 3 so that it's blob column becomes 32649 long.
        //  was 32000
        pad_blob = new byte[32646];
        update_stmt.setBytes(   1, pad_blob);
        update_stmt.setInt(     2, 2);
        update_stmt.executeUpdate();
        commit();

        // see if we can update the long column of row 1
        pad_blob = new byte[120000];
        update_stmt.setBytes(   1, pad_blob);
        update_stmt.setInt(     2, 0);
        update_stmt.executeUpdate();
        commit();

        stmt.close();
        insert_stmt.close();
        update_stmt.close();
    }

    public void testSmallRow1()
        throws SQLException
    {
        Statement stmt = createStatement();

        // setup has created:
        // CREATE TABLE testSmallRow1 (id char(1))
        //     should be a 4k page size.

        PreparedStatement insert_stmt = 
            prepareStatement("INSERT INTO testSmallRow1 VALUES(?)");

        // insert more than 3 pages of rows.
        insert_stmt.setString(1, "a");

        for (int i = 0; i < 4000; i++)
        {
            insert_stmt.executeUpdate();
        }
        insert_stmt.close();
        commit();

        // create an index to test btree handling of short key.
        stmt.executeUpdate("CREATE INDEX idx1 on testSmallRow1(id)");

        // Check the consistency of the indexes
        ResultSet rs = stmt.executeQuery(
            "VALUES SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'TESTSMALLROW1')");
        String [][] expRS = new String [][] {{"1"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        // now test on new table with an index during the inserts.

        // create an index to test btree handling of short key.
        stmt.executeUpdate("CREATE INDEX idx2 on testSmallRow2(id)");

        insert_stmt = 
            prepareStatement("INSERT INTO testSmallRow2 VALUES(?)");

        // insert more than 3 pages of rows.
        insert_stmt.setString(1, "a");

        for (int i = 0; i < 4000; i++)
        {
            insert_stmt.executeUpdate();
        }
        insert_stmt.close();
        commit();

        // Check the consistency of the indexes
        rs = stmt.executeQuery(
            "VALUES SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'TESTSMALLROW2')");
        expRS = new String [][] {{"1"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        // DDL that caused a bug while trying to fix derby 4577, data is null

        // create an index to test btree handling of short key.
        stmt.executeUpdate("CREATE INDEX idx3 on testSmallRow3(id)");

        insert_stmt = 
            prepareStatement("INSERT INTO testSmallRow3 VALUES(?, ?)");

        // insert more than 3 pages of rows.
        insert_stmt.setString(1, null);

        for (int i = 0; i < 100; i++)
        {
            insert_stmt.setInt(2, i);
            insert_stmt.executeUpdate();
        }
        commit();

        stmt.executeUpdate("UPDATE testSmallRow3 set id = null where id2 > 1");

        // Deleting rows from root of btree which will then force purges on the
        // page before it does a split.  The purges force the raw store 
        // through reclaim space on page code path.
        stmt.executeUpdate("DELETE from testSmallRow3 where id2 = 40 or id2 = 41 or id2 = 80 or id2 = 81");
        commit();

        insert_stmt.setString(1, null);
        for (int i = 101; i < 600; i++)
        {
            insert_stmt.executeUpdate();
            insert_stmt.setInt(2, i);
        }
        
        insert_stmt.close();
        commit();

        // Check the consistency of the indexes
        rs = stmt.executeQuery(
            "VALUES SYSCS_UTIL.SYSCS_CHECK_TABLE('APP', 'TESTSMALLROW3')");
        expRS = new String [][] {{"1"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        stmt.close();
    }
    
    protected static Test baseSuite(String name) 
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite(name);
        suite.addTestSuite(Derby4577Test.class);
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

                // create a table, with blob it will be 32k page size
                stmt.executeUpdate(
                    "CREATE TABLE testBadUpdate (id int, value blob(1M))");

                stmt.executeUpdate(
                    "CREATE TABLE testSmallRow1 (id char(1))");

                stmt.executeUpdate(
                    "CREATE TABLE testSmallRow2 (id char(1))");

                stmt.executeUpdate(
                    "CREATE TABLE testSmallRow3 (id char(20), id2 int)");

                conn.setAutoCommit(false);
            }
        };
    }

    public static Test suite() 
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("Derby4577Test");
        suite.addTest(baseSuite("Derby4577Test:embedded"));
        return suite;
    }
}
