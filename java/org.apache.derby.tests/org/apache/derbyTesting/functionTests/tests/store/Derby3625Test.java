package org.apache.derbyTesting.functionTests.tests.store;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Assert;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;


/*
Class org.apache.derbyTesting.functionTests.tests.jdbc4.Derby3650Test

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

Test to reproduce DERBY-3625, failure in inline compress, in some 
circumstances depending on exact size of data and state of pages during
the defragment phase.  

Would throw following error:

ERROR XSDA3: Limitation: Record cannot be updated or inserted due to lack of 
space on the page. Use the parameters derby.storage.pageSize and/or 
derby.storage.pageReservedSpace to work around this limitation.^M

**/

public class Derby3625Test extends StoreBaseTest
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

    public Derby3625Test(String name) 
    {
        super(name);
    }
    
    /**
     * DERBY-3625 test case
     * <p>
     * Derby 3625 is caused by a bug where compress calculates the space
     * needed to fit a moved row from page A to B, and assumes that the space
     * required on page B is the same on page A.  The problem is that in 
     * some cases due to the stored format of the changing record id the space
     * required on B may be more than A.  In the case where there is exactly
     * enough space by the initial calculation the move fails because one or
     * 3 more bytes may be necessary to make the move and the compress fails.
     * <p>
     * To test:
     *   fill page 1 with dummy rows, page 1 has a special control row on it
     *       so it can't ever be empty so use page 2 instead.
     *   fill page 2 with dummy rows such and empty it such that the 
     *       next row id on it is greater that 64 which takes 2 bytes to store 
     *       vs. 1 for rowid's less * that 64.
     *   fill page 3 and 4 with some dummy rows which will be deleted to give
     *       compress table room to work during defragment.
     *   fill page 4 with 2 rows which fit on page 2 with 1 byte stored record
     *        id's but will not fit with 2 byte stored record id's.  
     *        These will not be deleted and the bug is exercised as 
     *        defragment tries to move these rows to page 2 after it has 
     *        been reclaimed as a free page.
     **/
    public void testTwoToOneByteCase()
        throws SQLException
    {
        PreparedStatement insert_stmt = 
            prepareStatement("INSERT INTO testCompress VALUES(?, ?, ?)");

        // page 0 - container info/bit map, does not affect test

        // page 1 - fill it up and leave rows on it.  page 1 has a special
        // row on it that can never be deleted so this page never can be
        // made free.

        // insert one blob padded row that will fill page 1
        byte[] pad_blob = new byte[32630];
        insert_stmt.setInt(     1, 1);
        insert_stmt.setBytes(   2, pad_blob);
        insert_stmt.setString(  3, "page 1");
        insert_stmt.executeUpdate();

        // page 2 - fill it completely with enough rows such that future
        // rows will force a 2 byte row id, ie. more than 64 rows.  Later
        // in this test all the rows will be deleted from this page so that
        // the page is on the free list for compress defragment to use it.

        pad_blob = new byte[302];
        insert_stmt.setInt(     1, 2);
        insert_stmt.setBytes(   2, pad_blob);
        insert_stmt.setString(  3, "page 2");
        for (int i = 0; i < 98; i++)
        {
            insert_stmt.executeUpdate();
        }

        // page 3 - fill it for another free page.
        insert_stmt.setInt(     1, 3);
        insert_stmt.setBytes(   2, pad_blob);
        insert_stmt.setString(  3, "page 3");
        for (int i = 0; i < 98; i++)
        {
            insert_stmt.executeUpdate();
        }

        // page 4 -  2 rows, with one byte free.  When these are moved to 
        // a free page with bigger rowid's they will take 2 more bytes and
        // will not both fit on the page.
        //
        // I didn't track it down, but for some reason I could not fill a page
        // completely if there was only one row on the page, it kept turning
        // the blob column into a long row.  I was just picking magic numbers
        // for the blob column to make it fit.
        //
        // With 2 rows I was able to fill the page up to one empty byte.  
        // Then with the bug the first row would move to page 2 which is
        // now free but take one more byte than it did on this page.  And
        // finally when the second row was moved it would think it would fit
        // but throw an exception when the rowid compressed version would
        // cause it to be one byte bigger than the original row.
        pad_blob = new byte[100];
        insert_stmt.setInt(     1, 4);
        insert_stmt.setBytes(   2, pad_blob);
        insert_stmt.setString(  3, "page 4");
        insert_stmt.executeUpdate();
        pad_blob = new byte[32534];
        insert_stmt.setInt(     1, 4);
        insert_stmt.setBytes(   2, pad_blob);
        insert_stmt.setString(  3, "page 4");
        insert_stmt.executeUpdate();

        commit();

        int space_info[] = getSpaceInfo("APP", "TESTCOMPRESS", true);

        // space after initial insert setup should be 4 pages
        // 0 - container info - not reflected in allocated page count, 
        // 1 - dummy data left on the page, 
        // 2 - bunch of short records to be deleted to make free page
        // 3 - bunch of short records to be deleted to make free page
        // 4 - short and long record to exercise bug.
        Assert.assertEquals(
            "wrong allocated page count in test setup", 
            4, space_info[SPACE_INFO_NUM_ALLOC]);

        Statement stmt = createStatement();

        // Delete rows on page 2 and 3 to allow defragment to try and move
        // the page 4 row up.
        stmt.executeUpdate("DELETE FROM testCompress where id = 2 or id = 3");
        commit();

        // Before fixing the bug, this compress call would throw the 
        // following exception:
        //
        // ERROR XSDA3: Limitation: Record cannot be updated or inserted due 
        // to lack of space on the page. Use the parameters 
        // derby.storage.pageSize and/or derby.storage.pageReservedSpace to 
        // work around this limitation.

        CallableStatement call_compress = 
            prepareCall(
                "CALL SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE(?, ?, 1, 1, 1)");

        call_compress.setString(1, "APP");
        call_compress.setString(2, "TESTCOMPRESS");
        call_compress.executeUpdate();

        commit();

        space_info = getSpaceInfo("APP", "TESTCOMPRESS", true);

        // space after the test should be 3 pages: 
        // 0 - container info - not reflected in allocated page count, 
        // 1 - dummy data left on the page, 
        // 2 - one short record, but long record did not fit
        // 3 - long record on an empty page.
        Assert.assertEquals(
            "wrong allocated page count", 3, space_info[SPACE_INFO_NUM_ALLOC]);

        insert_stmt.close();
    }
    
    protected static Test baseSuite(String name) 
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite(name);
        suite.addTestSuite(Derby3625Test.class);
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

                CallableStatement set_dbprop =  conn.prepareCall(
                    "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?, ?)");
                set_dbprop.setString(1,"derby.storage.pageReservedSpace");
                set_dbprop.setString(2,"0");
                set_dbprop.executeUpdate();
                
                // create a table, with blob it will be 32k page size
                stmt.executeUpdate(
                    "CREATE TABLE testCompress " +
                        "(id int, padcol blob(1M), c varchar(200))");

                set_dbprop.setString(2, null);
                set_dbprop.executeUpdate();

                set_dbprop.close();

                conn.setAutoCommit(false);
            }
        };
    }

    public static Test suite() 
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("Derby3625Test");
        suite.addTest(baseSuite("Derby36625Test:embedded"));
        return suite;
    }
}
