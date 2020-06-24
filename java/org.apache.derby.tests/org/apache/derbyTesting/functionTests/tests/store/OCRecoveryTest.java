/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.store.OCRecoveryTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */

package org.apache.derbyTesting.functionTests.tests.store;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.TestConfiguration;

public class OCRecoveryTest extends BaseJDBCTestCase {

    private static final String tableName = "RECTEST1";
    
    public OCRecoveryTest(String name) {
        super(name);
    }

    public static Test suite()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("OCRecoveryTest");
        //DERBY-4647 exec does not work on weme
        if (BaseTestCase.isJ9Platform())
            return suite;
        suite.addTest(decorateTest());
        return suite;
    }

    private static Test decorateTest()
    {
        Test test = TestConfiguration.embeddedSuite(
                        OCRecoveryTest.class);
        // using a singleUseDatabaseDecorator which should not create
        // the database until the first connection is made
//IC see: https://issues.apache.org/jira/browse/DERBY-5382
        return TestConfiguration.singleUseDatabaseDecorator(test,
            "OCRecoveryDB");
    }

    public void testOCRecovery() throws Exception
    {
        
        // Now call forked processes - each of these will do something,
        // then *not* shutdown, forcing the next one to recover the
        // database.
        // Pass in the name of the database to be used.
        assertLaunchedJUnitTestMethod("org.apache.derbyTesting." +
                "functionTests.tests.store.OCRecoveryTest.launchOCRecovery_1",
                "OCRecoveryDB");
//IC see: https://issues.apache.org/jira/browse/DERBY-5382
        assertLaunchedJUnitTestMethod("org.apache.derbyTesting." +
                "functionTests.tests.store.OCRecoveryTest.launchOCRecovery_2",
                "OCRecoveryDB");
        assertLaunchedJUnitTestMethod("org.apache.derbyTesting." +
                "functionTests.tests.store.OCRecoveryTest.launchOCRecovery_3",
                "OCRecoveryDB");
        assertLaunchedJUnitTestMethod("org.apache.derbyTesting." +
                "functionTests.tests.store.OCRecoveryTest.launchOCRecovery_4",
                "OCRecoveryDB");
    }

    public void launchOCRecovery_1() throws Exception
    {
        // setup to test restart recovery of online compress.  Real work
        // is done in next method launchOCRecovery_2 which will run restart
        // recovery on the work done in this step.
        // open a connection to the database, this should create the db
//IC see: https://issues.apache.org/jira/browse/DERBY-5382
        getConnection();
        setAutoCommit(false);        
//IC see: https://issues.apache.org/jira/browse/DERBY-5382
        createAndLoadTable(tableName, true, 5000, 0);
        Statement st = createStatement();
        st.executeUpdate("DELETE FROM " + tableName);
        commit();
        callCompress(tableName, true, true, true, true);
        st.close();        
    }

    public void launchOCRecovery_2() throws Exception
    {
        setAutoCommit(false);
        assertCheckTable(tableName);
        // make sure we can add data to the existing table after redo
        // recovery.
        createAndLoadTable(tableName, false, 6000, 0);
        assertCheckTable(tableName);
        String table_name =  tableName + "_2";
        // setup to test redo recovery on: 
        //      create table, delete rows, compress, commit
        createAndLoadTable(table_name, true, 2000, 0);
        Statement st = createStatement();
        st.executeUpdate("DELETE FROM " + tableName);
        commit();
        callCompress(tableName, true, true, true, true);

        st.close();
    }

    public void launchOCRecovery_3() throws SQLException
    {
        setAutoCommit(false);
        String table_name =  tableName + "_2";
        assertCheckTable(table_name);
        // make sure we can add data to the existing table after redo
        // recovery.
        createAndLoadTable(tableName, false, 2000, 0);
        assertCheckTable(tableName);

        // setup to test redo recovery on: 
        //      add more rows, delete rows, commit, compress, no commit
        createAndLoadTable(table_name, false, 4000, 2000);
        Statement st = createStatement();
        st.executeUpdate("DELETE FROM " + table_name);
        commit();
        callCompress(table_name, true, true, true, false);
        st.close();
    }
    
    public void launchOCRecovery_4() throws SQLException
    {
        // oc_rec3 left the table  with no rows, but compress command
        // did not commit.
        setAutoCommit(false);
        String table_name =  tableName + "_2";
        assertCheckTable(table_name);
        // make sure we can add data to the existing table after redo
        // recovery.
        createAndLoadTable(table_name, false, 6000, 0);
        assertCheckTable(table_name);
    }
    
    /**
     * Create and load a table.
     * <p>
     * If create_table is set creates a test data table with indexes.
     * Loads num_rows into the table.  This table defaults to 32k page size.
     * This schema fits 25 rows per page
     * <p>
     *
     * @param create_table  If true, create new table - otherwise load into
     *                      existing table.
     * @param tblname       table to use.
     * @param num_rows      number of rows to add to the table.
     *
     * @exception  SQLException  Standard exception policy.
     **/
    private void createAndLoadTable(
    String      tblname,
    boolean     create_table,
    int         num_rows,
    int         start_value)
        throws SQLException
    {
        if (create_table)
        {
            Statement s = createStatement();

            s.execute(
                "CREATE TABLE " + tblname + 
                    "(keycol int, indcol1 int, indcol2 int, indcol3 int, " +
                    "data1 varchar(2000), data2 varchar(2000))");
            s.close();
            println("table created: " + tblname);
        }

        PreparedStatement insert_stmt = 
                prepareStatement(
                    "INSERT INTO " + tblname + " VALUES(?, ?, ?, ?, ?, ?)");

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

        if (create_table)
        {
            Statement s = createStatement();

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
                "create unique index " + tblname + "_idx_indcol3 on " + 
                    tblname + "(indcol3)");
            s.close();
        }
        commit();
    }
    
    /**
     * call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE() system procedure.
     **/
    private void callCompress(
    String      tableName,
    boolean     purgeRows,
    boolean     defragmentRows,
    boolean     truncateEnd,
    boolean     commit_operation)
        throws SQLException
    {
        CallableStatement cstmt = 
                prepareCall(
                "call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE(?, ?, ?, ?, ?)");
        cstmt.setString(1, getTestConfiguration().getUserName());
        cstmt.setString(2, tableName);
        cstmt.setInt   (3, purgeRows      ? 1 : 0);
        cstmt.setInt   (4, defragmentRows ? 1 : 0);
        cstmt.setInt   (5, truncateEnd    ? 1 : 0);
        cstmt.execute();
        if (commit_operation) {
            commit();
        }
    }

}
