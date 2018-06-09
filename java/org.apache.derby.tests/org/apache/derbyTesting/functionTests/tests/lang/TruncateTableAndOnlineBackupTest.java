/*
 * Class org.apache.derbyTesting.functionTests.tests.lang.TruncateTableAndOnlineBackupTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.derbyTesting.functionTests.tests.lang;

import java.io.File;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;
import org.apache.derbyTesting.functionTests.util.SQLStateConstants;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests interaction of TRUNCATE TABLE and Online Backup.
 * See also DERBY-5213
 * The test needs to test:
 * o uncommitted truncate table followed by online backup; 
 *   then access the backup copy and access the table.
 *   expected behavior: should see the old data.
 * o uncommitted truncate table, followed by online backup that keeps logs,
 *   then commit the truncate, then access the table in the backup. 
 *   expected behavior: should see old data in backup.
 */

public class TruncateTableAndOnlineBackupTest  extends BaseJDBCTestCase {
    
    static String home = null; // derby.system.home
    final static String dbName = "TTOB_db";
//    final static String dbName2 = dbName + "2";
    final static String backupDir = "TTOB_backup";
        
    public TruncateTableAndOnlineBackupTest(String name) {
        super(name);
    }

    public static Test suite() {
        BaseTestSuite suite =
            new BaseTestSuite("TruncateTableAndOnlineBackupTest");

        suite.addTest(baseSuite("TruncateTableAndOnlineBackupTest:Embedded"));
        //suite.addTest(TestConfiguration
        //        .clientServerDecorator(baseSuite("TruncateTableAndOnlineBackupTest:Client")));
        return TestConfiguration.singleUseDatabaseDecorator(suite,
                dbName);

        //return suite;
    }
    
    protected static Test baseSuite(String name) {
        BaseTestSuite suite = new BaseTestSuite(name);
        suite.addTestSuite(TruncateTableAndOnlineBackupTest.class);
        return new CleanDatabaseTestSetup(suite); 
    }
    
    public void setUp() throws Exception {
        getConnection();
        home = getSystemProperty("derby.system.home");

        Statement stmt = createStatement();
        stmt.executeUpdate("create table truncable(i int)");
        PreparedStatement ps = getConnection().prepareStatement(
                "insert into truncable values (?)");

        // insert some data
        getConnection().setAutoCommit(false);
        for (int i=1; i <= 1000; i++) {
            ps.setInt(1,i);
            ps.executeUpdate();
        }
        getConnection().commit();
    }
    
    /*
     * Drop the table truncable that was created in setUp().
     */
    public void tearDown() throws Exception
    {
        getConnection().createStatement().execute("drop table truncable");
        super.tearDown();
    }

    /*  uncommitted truncate table followed by online backup; 
     *  then access the backup copy and access the table.
     *  expected behavior: should see the old data.
     */
    public void testUncommittedTruncateBasicBackup() throws Exception {

        setAutoCommit(false);
        Statement s = createStatement();

        // check...we should have 1000 rows
        JDBC.assertFullResultSet(
                s.executeQuery("select count(*) from truncable"),
                new String[][]{{"1000"}});

        // truncate the table, but do not commit
        s.executeUpdate("truncate table truncable");

        // check...we should have no rows
        ResultSet rs = s.executeQuery("select * from truncable");
        JDBC.assertEmpty(rs);
        
        CallableStatement cs = prepareCall
            ("CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE(?)");
        String fullBackupDir = home + "/" + backupDir;
        cs.setString(1, fullBackupDir);
        cs.execute();

        // check contents of table in backup dir

        final DataSource ds2 = JDBCDataSource.getDataSource(fullBackupDir+"/"+dbName);
        final Connection con2 = ds2.getConnection();
        Statement s2 = con2.createStatement();
        // check...we should have 1000 rows because truncate table was not committed
        JDBC.assertFullResultSet(
                s2.executeQuery("select count(*) from truncable"),
                new String[][]{{"1000"}});

        con2.close();

        // close down both
        final DataSource[] srcs =
                new DataSource[] {JDBCDataSource.getDataSource(),
                    JDBCDataSource.getDataSource(fullBackupDir+"/"+dbName)};

        for (int i=0; i < srcs.length; i++) {
            JDBCDataSource.setBeanProperty(
                    srcs[i], "connectionAttributes", "shutdown=true");

            try {
                srcs[i].getConnection();
                fail("shutdown failed: expected exception");
            } catch (SQLException e) {
                assertSQLState(
                    "database shutdown",
                    SQLStateConstants.CONNECTION_EXCEPTION_CONNECTION_FAILURE,
                    e);
            }
        }

        assertDirectoryDeleted(new File(fullBackupDir));
    }

    /*   uncommitted truncate table, followed by online backup that keeps logs,
     *   then commit the truncate, then access the table in the backup. 
     *   expected behavior: should see old data in backup.
     */
    public void testUncommittedTruncateBackupEnableLog() throws Exception {

        setAutoCommit(false);
        Statement s = createStatement();

        // check...we should have 1000 rows
        JDBC.assertFullResultSet(
                s.executeQuery("select count(*) from truncable"),
                new String[][]{{"1000"}});

        // truncate the table, but do not commit
        s.executeUpdate("truncate table truncable");

        // check...we should have no rows
        ResultSet rs = s.executeQuery("select * from truncable");
        JDBC.assertEmpty(rs);
        
        CallableStatement cs = prepareCall
            ("CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE(?,1)");
        String fullBackupDir = home + "/" + backupDir;
        cs.setString(1, fullBackupDir);
        cs.execute();

        // now commit - this will commit the truncate table
        commit();
        
        // check contents of table in backup dir
        final DataSource ds2 = JDBCDataSource.getDataSource(fullBackupDir+"/"+dbName);
        final Connection con2 = ds2.getConnection();
        Statement s2 = con2.createStatement();
        // we should have 1000 rows because truncate table was not committed
        JDBC.assertFullResultSet(
                s2.executeQuery("select count(*) from truncable"),
                new String[][]{{"1000"}});
        con2.close();

        // backup again, to a different dir
        String fullBackupDir2=fullBackupDir+"2";
        cs.setString(1, fullBackupDir2);
        cs.execute();

        // connect to the second backed up database, but this time,
        // we should not have any rows.
        final DataSource ds3 = JDBCDataSource.getDataSource(fullBackupDir2+"/"+dbName);
        final Connection con3 = ds3.getConnection();
        Statement s3 = con3.createStatement();
        rs = s3.executeQuery("select * from truncable");
        JDBC.assertEmpty(rs);

        // close down all
        final DataSource[] srcs =
                new DataSource[] {JDBCDataSource.getDataSource(),
                    JDBCDataSource.getDataSource(fullBackupDir+"/"+dbName),
                    JDBCDataSource.getDataSource(fullBackupDir2+"/"+dbName)};

        for (int i=0; i < srcs.length; i++) {
            JDBCDataSource.setBeanProperty(
                    srcs[i], "connectionAttributes", "shutdown=true");

            try {
                srcs[i].getConnection();
                fail("shutdown failed: expected exception");
            } catch (SQLException e) {
                assertSQLState(
                    "database shutdown",
                    SQLStateConstants.CONNECTION_EXCEPTION_CONNECTION_FAILURE,
                    e);
            }
        }

        assertDirectoryDeleted(new File(fullBackupDir));
        assertDirectoryDeleted(new File(fullBackupDir2));
    }    
    
    /*  uncommitted truncate table followed by online backup; 
     *  then access the backup copy and access the table.
     *  expected behavior: should see the old data.
     */
    public void testTruncateFreezeUnfreeze() throws Exception {

        setAutoCommit(false);
        Statement s = createStatement();

        // check...we should have 1000 rows
        JDBC.assertFullResultSet(
                s.executeQuery("select count(*) from truncable"),
                new String[][]{{"1000"}});

        // truncate the table, but do not commit
        s.executeUpdate("truncate table truncable");

        // check...we should have no rows
        ResultSet rs = s.executeQuery("select * from truncable");
        JDBC.assertEmpty(rs);

        // freeze the database
        s.execute("call syscs_util.syscs_freeze_database()");

        // Now copy the database directory
        String fullBackupDir = backupDir + "2";
        File DbDir = new File(home, dbName);
        File fullBackupDbDir = new File(home, fullBackupDir );
        PrivilegedFileOpsForTests.copy(DbDir, fullBackupDbDir);
        
        // At this point, writing to the original database is blocked.
        // Try to read from the original database. Should work, we should still
        // be connected, and read access is allowed during the freeze.
        rs = s.executeQuery("select * from truncable");
        JDBC.assertEmpty(rs);

        // connect to backed-up database
        final DataSource ds2 = JDBCDataSource.getDataSource(fullBackupDir);
        final Connection con2 = ds2.getConnection();
        Statement s2 = con2.createStatement();
        // check...we should have 1000 rows because truncate table was not committed
        // before the freeze and copy
        JDBC.assertFullResultSet(
                s2.executeQuery("select count(*) from truncable"),
                new String[][]{{"1000"}});

        // unfreeze our original database
        s.execute("call syscs_util.syscs_unfreeze_database()");

        // ensure we can read and write now.
        rs = s.executeQuery("select * from truncable");
        JDBC.assertEmpty(rs);
        s.executeUpdate("insert into truncable values(2001)");
        JDBC.assertFullResultSet(
                s.executeQuery("select count(*) from truncable"),
                new String[][]{{"1"}});

        // rollback, should rollback the truncate - then 
        // select again from the org db - should have 1000 rows again instead of 1
        // then select again from the backup db, should still have 1000 rows.
        rollback();
        JDBC.assertFullResultSet(
                s.executeQuery("select count(*) from truncable"),
                new String[][]{{"1000"}});
        JDBC.assertFullResultSet(
                s2.executeQuery("select count(*) from truncable"),
                new String[][]{{"1000"}});

        s2.close();
        con2.close();

        // close down both databases
        final DataSource[] srcs =
                new DataSource[] {JDBCDataSource.getDataSource(),
                ds2};

        for (int i=0; i < srcs.length; i++) {
            JDBCDataSource.setBeanProperty(
                    srcs[i], "connectionAttributes", "shutdown=true");
            try {
                srcs[i].getConnection();
                fail("shutdown failed: expected exception");
            } catch (SQLException e) {
                assertSQLState(
                    "database shutdown",
                    SQLStateConstants.CONNECTION_EXCEPTION_CONNECTION_FAILURE,
                    e);
            }
        }

        assertDirectoryDeleted(new File(home + "/"+fullBackupDir));
    }    
}
