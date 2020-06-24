/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.Changes10_3

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
package org.apache.derbyTesting.functionTests.tests.upgradeTests;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.tests.jdbcapi.BlobStoredProcedureTest;
import org.apache.derbyTesting.functionTests.tests.jdbcapi.ClobStoredProcedureTest;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SupportFilesSetup;


/**
 * Upgrade test cases for changes made in 10.3.
 * If the old version is 10.3 or later then these tests
 * will not be run.

 * <BR>
 * 10.3 Upgrade issues
 */
public class Changes10_3 extends UpgradeChange {

    private static  final   String  UNKNOWN_PROCEDURE = "42Y03";
   
    /**
     * Return the suite of tests to test the changes made in 10.3.
     * @param phase an integer that indicates the current phase in
     *              the upgrade test.
     * @return the test suite created.
     */   
    public static Test suite(int phase) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("Upgrade changes for 10.3");
        
        suite.addTestSuite(Changes10_3.class);
        
        //Add the tests for the Stored procedures related to the locator
        //implementation of the LOB related JDBC methods. This needs to be done
        //only during the hard(full) upgrade phase.
//IC see: https://issues.apache.org/jira/browse/DERBY-2385
        if(phase == PH_HARD_UPGRADE) {
//IC see: https://issues.apache.org/jira/browse/DERBY-2632
            if (JDBC.vmSupportsJDBC3())
            {
                //Tests for the Blob related locator StoredProcedures
                suite.addTestSuite(BlobStoredProcedureTest.class);
                //Tests for the Clob related locator StoredProcedures
                suite.addTestSuite(ClobStoredProcedureTest.class);
            }
        }
        
//IC see: https://issues.apache.org/jira/browse/DERBY-378
        return new SupportFilesSetup((Test) suite);
    }

    public Changes10_3(String name) {
        super(name);
    }
    
    /**
     * Verify the compilation schema is nullable after upgrade to 10.3
     * or later. (See DERBY-630)
     * @throws SQLException
     */
    public void testCompilationSchema() throws SQLException
    {        
        switch (getPhase())
        {
            case PH_CREATE:
            case PH_POST_SOFT_UPGRADE:
                // 10.0-10.2 inclusive had the system schema incorrect.
                if (!oldAtLeast(10, 3))
                    return;
                break;
        }

        DatabaseMetaData dmd = getConnection().getMetaData();

        ResultSet rs = dmd.getColumns(null, "SYS", "SYSSTATEMENTS", "COMPILATIONSCHEMAID");
        rs.next();
        assertEquals("SYS.SYSSTATEMENTS.COMPILATIONSCHEMAID IS_NULLABLE",
                        "YES", rs.getString("IS_NULLABLE"));
        rs.close();

        rs = dmd.getColumns(null, "SYS", "SYSVIEWS", "COMPILATIONSCHEMAID");
        rs.next();
        assertEquals("SYS.SYSVIEWS.COMPILATIONSCHEMAID IS_NULLABLE",
                        "YES", rs.getString("IS_NULLABLE"));
    }
    /**
     * In 10.3: We will write a LogRecord with a different format 
     * that can also write negative values.
     * 
     * Verify here that a 10.2 Database does not malfunction from this and
     * 10.2 Databases will work with the old LogRecord format.
     */
    public void testNegValueSupportedLogRecord()
//IC see: https://issues.apache.org/jira/browse/DERBY-2217
        throws SQLException
    {
        switch(getPhase()) {
            case PH_CREATE: {
//IC see: https://issues.apache.org/jira/browse/DERBY-2611

                // This case is derived from OnlineCompressTest.test6.
                Statement s = createStatement();
                s.execute("create table case606(keycol int, indcol1 int,"+
                    "indcol2 int, data1 char(24), data2 char(24), " +
                    "data3 char(24)," +
                    "data4 char(24), data5 char(24), data6 char(24),"+
                    "data7 char(24), data8 char(24), data9 char(24)," + 
                    "data10 char(24), inddec1 decimal(8), indcol3 int,"+
                    "indcol4 int, data11 varchar(50))");
                s.close();

                break;
            }

            case PH_SOFT_UPGRADE:
                // in place compress was added in 10.1 release, don't check
                // upgrade of it from 10.0 release.
                if (!oldAtLeast(10, 1))
                    return;

                // Ensure that the old Log Record format is written
                // by Newer release without throwing any exceptions.
                checkDataToCase606(0, 2000);
                break;

            case PH_POST_SOFT_UPGRADE:
                // in place compress was added in 10.1 release, don't check
                // upgrade of it from 10.0 release.
                if (!oldAtLeast(10, 1))
                    return;

                // We are now back to Old release
                checkDataToCase606(0, 1000);
                break;

            case PH_HARD_UPGRADE:
                // in place compress was added in 10.1 release, don't check
                // upgrade of it from 10.0 release.
                if (!oldAtLeast(10, 1))
                    return;

                // Create the Derby606 bug scenario and test that
                // the error does not occur in Hard Upgrade
                checkDataToCase606(0, 94000);

                break;
        }
    }
    
    private void checkDataToCase606(int start_value, int end_value)
            throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-2217
        getConnection().setAutoCommit(false);
        Statement s = createStatement();
        PreparedStatement insert_stmt = prepareStatement("insert into case606 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        char[] data_dt = new char[24];
        char[] data_dt2 = new char[50];
        for (int i = 0; i < data_dt.length; i++)
            data_dt[i] = 'a';
        for (int i = 0; i < data_dt2.length; i++)
            data_dt2[i] = 'z';
        String data1_str = new String(data_dt);
        String data2_str = new String(data_dt2);

        for (int i = start_value; i < end_value; i++) {
            insert_stmt.setInt(1, i); // keycol
            insert_stmt.setInt(2, i * 10); // indcol1
            insert_stmt.setInt(3, i * 100); // indcol2
            insert_stmt.setString(4, data1_str); // data1_data
            insert_stmt.setString(5, data1_str); // data2_data
            insert_stmt.setString(6, data1_str); // data3_data
            insert_stmt.setString(7, data1_str); // data4_data
            insert_stmt.setString(8, data1_str); // data5_data
            insert_stmt.setString(9, data1_str); // data6_data
            insert_stmt.setString(10, data1_str); // data7_data
            insert_stmt.setString(11, data1_str); // data8_data
            insert_stmt.setString(12, data1_str); // data9_data
            insert_stmt.setString(13, data1_str); // data10_data
            insert_stmt.setInt(14, i * 20); // indcol3
            insert_stmt.setInt(15, i * 200); // indcol4
            insert_stmt.setInt(16, i * 50);
            insert_stmt.setString(17, data2_str); // data11_data

            insert_stmt.execute();
        }
        insert_stmt.close();
        commit();

        s.execute("delete from case606 where case606.keycol > 10000");
        commit();
        
        String schema = getTestConfiguration().getUserName();

        s.execute(
                "call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE('"
                 + schema + "', 'CASE606',1,1,1)");
        s.close();
        commit();

    }



    /**
     * Simple test to ensure new import/export procedures added in 10.3 
     * are working on hard upgrade to 10.3 from previous derby versions.
     */
    public void testImportExportLobsProcedures()
//IC see: https://issues.apache.org/jira/browse/DERBY-378
        throws SQLException
    {
    
        switch(getPhase()) {
        case PH_CREATE: {
            Statement s = createStatement();
            s.execute("create table iet1(id int , content clob , pic blob)");
            s.executeUpdate("insert into iet1 values " + 
                            "(1, 'SQL Tips', cast(X'4231a2' as blob))");
            s.close();
            commit();
            break;
        }
        case PH_SOFT_UPGRADE: {
            // new import export procedure should not be found 
            // on soft-upgrade.
            Statement s = createStatement();
            assertStatementError("42Y03", s, 
//IC see: https://issues.apache.org/jira/browse/DERBY-378
                "call SYSCS_UTIL.SYSCS_EXPORT_TABLE_LOBS_TO_EXTFILE" +  
                "(null , 'IET1' , 'iet1.del' , null, " + 
                "null, null, 'iet1_lobs.dat')");
            s.close();
            break;
        }
        case PH_POST_SOFT_UPGRADE: 
            break;
        case PH_HARD_UPGRADE: {
            //  main file used to perform import/export.
            String fileName =  
                (SupportFilesSetup.getReadWrite("iet1.del")).getPath();
            // external file name used to store lobs.
            String lobsFileName =
                (SupportFilesSetup.getReadWrite("iet1_lobs.dat")).getPath();

            Statement s = createStatement();

	    //DERBY-2925: need to delete existing files first.
            SupportFilesSetup.deleteFile(fileName);
            SupportFilesSetup.deleteFile(lobsFileName);

            s.execute(
//IC see: https://issues.apache.org/jira/browse/DERBY-378
                "call SYSCS_UTIL.SYSCS_EXPORT_TABLE_LOBS_TO_EXTFILE" +  
                "(null , 'IET1' , '"  +  fileName  + 
                "' , null, null, null, '" + lobsFileName + "')");
            s.execute("call SYSCS_UTIL.SYSCS_IMPORT_TABLE_LOBS_FROM_EXTFILE(" + 
                      "null, 'IET1' , '" + fileName + 
                      "', null, null, null, 0)");

	    //DERBY-2925: need to delete existing files first.
            SupportFilesSetup.deleteFile(fileName);
            SupportFilesSetup.deleteFile(lobsFileName);

            s.execute("call SYSCS_UTIL.SYSCS_EXPORT_QUERY_LOBS_TO_EXTFILE(" +
                      "'select * from IET1', '" +  fileName + 
                      "' , null, null, null, '" + lobsFileName + "')");
            s.execute("call SYSCS_UTIL.SYSCS_IMPORT_DATA_LOBS_FROM_EXTFILE(" + 
                      "null, 'IET1','ID, CONTENT, PIC', '1,2,3'," + 
                      "'" + fileName +"', null, null, null, 1)") ;
            
            // verify table has correct data after performing import/export.
            ResultSet rs = s.executeQuery("select * from iet1");
            JDBC.assertFullResultSet(rs, new String[][]
                {{"1", "SQL Tips", "4231a2"},
                 {"1", "SQL Tips", "4231a2"}});
            s.close();
            break;
        }
        
        }
    }

    /**
     * Ensure that the new policy-file-reloading procedure works after
     * hard upgrade to 10.3 from previous derby versions.
     */
    public void testPolicyReloadingProcedure()
//IC see: https://issues.apache.org/jira/browse/DERBY-2466
        throws SQLException
    {
        int         currentPhase = getPhase();
    
        switch( currentPhase )
        {
            
            case PH_CREATE:
            case PH_SOFT_UPGRADE: 
            case PH_POST_SOFT_UPGRADE: 
                assertPolicyReloaderDoesNotExist();
                break;
                
            case PH_HARD_UPGRADE:
                assertPolicyReloaderExists();
                break;
            
            default:
                throw new SQLException( "Unknown upgrade phase: " + currentPhase );
         
        }
    }

    /**
     * Verify that the policy-reloading procedure exists.
     */
    private void assertPolicyReloaderExists()
        throws SQLException
    {
        tryReloading( true, null );
    }
    
    /**
     * Verify whether the policy-reloading procedure exists.
     */
    private void assertPolicyReloaderDoesNotExist()
        throws SQLException
    {
        tryReloading( false, UNKNOWN_PROCEDURE );
    }
    
    /**
     * Call the policy reloading procedure.
     */
    private void tryReloading( boolean shouldSucceed, String expectedSQLState )
        throws SQLException
    {
        boolean didSucceed = false;
        
        try {
            Statement s = createStatement();
            s.execute("call SYSCS_UTIL.SYSCS_RELOAD_SECURITY_POLICY()");

            didSucceed = true;
        }
        catch (SQLException se)
        {
            assertSQLState( expectedSQLState, se );
        }

        assertEquals( "Reloading results.", shouldSucceed, didSucceed );
    }

    /**
     * Check if we can open the heap.
     * <p>
     * This test just does a simple select to verify that 10.3 heap conglomerate
     * format id's are working right for all the various upgrade scenarios.
     **/
    private void checkNewHeap(
    String  tableName,
    String  value)
        throws SQLException
    {
        // verify table has correct data after performing import/export.
        Statement s = createStatement();
        ResultSet rs = s.executeQuery("select * from " + tableName);
        JDBC.assertFullResultSet(rs, new String[][] {{value}});
        s.close();
        rs.close();
    }

    /**
     * Test that new format id for Heap is not used in soft upgrade.
     **/
    public void testNewHeap()
        throws SQLException
    {
        // create tables in all 3 phases: boot old db, after 1st soft upgrade,
        // and after hard upgrade.
        switch (getPhase())
        {
            case PH_CREATE: 
            {
                // setup create of testNewHeap1 in old db

                Statement s = createStatement();
                s.execute("create table testNewHeap1(keycol char(20))");
                s.close();
                PreparedStatement insert_stmt = 
                    prepareStatement("insert into testNewHeap1 values(?)");;
                insert_stmt.setString(1, "create"); 
                insert_stmt.execute();
                insert_stmt.close();

                break;
            }

            case PH_SOFT_UPGRADE:
            {
                // setup create of testNewHeap2 once soft upgrade to current
                // version has happened.

                Statement s = createStatement();
                s.execute("create table testNewHeap2(keycol char(20))");
                s.close();
                PreparedStatement insert_stmt = 
                    prepareStatement("insert into testNewHeap2 values(?)");;
                insert_stmt.setString(1, "soft"); 
                insert_stmt.execute();
                insert_stmt.close();

                break;
            }

            case PH_HARD_UPGRADE:
            {
                // setup create of testNewHeap3 once hard upgrade to current
                // version has happened.

                Statement s = createStatement();
                s.execute("create table testNewHeap3(keycol char(20))");
                s.close();
                PreparedStatement insert_stmt = 
                    prepareStatement("insert into testNewHeap3 values(?)");
                insert_stmt.setString(1, "hard"); 
                insert_stmt.execute();
                insert_stmt.close();

                break;
            }
        }

        // Now verify you can access the tables 
//IC see: https://issues.apache.org/jira/browse/DERBY-2611
        switch (getPhase())
        {
            case PH_CREATE: 
            {
                checkNewHeap("testNewHeap1", "create");
                break;
            }
            case PH_SOFT_UPGRADE:
            {
                checkNewHeap("testNewHeap1", "create");
                checkNewHeap("testNewHeap2", "soft");
                break;
            }
            case PH_POST_SOFT_UPGRADE:
            {
                checkNewHeap("testNewHeap1", "create");
                checkNewHeap("testNewHeap2", "soft");
                break;
            }
            case PH_HARD_UPGRADE:
            {
                checkNewHeap("testNewHeap1", "create");
                checkNewHeap("testNewHeap2", "soft");
                checkNewHeap("testNewHeap3", "hard");
                break;
            }
        }

    }
    
}
