/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.Changes10_2

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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;

/**
 * Upgrade test cases for changes made in 10.2.
 * If the old version is 10.2 or later then these tests
 * will not be run.

 * <BR>
 * 10.2 Upgrade issues
 * <UL>
 * <LI> testTriggerInternalVTI - Check internal re-write of triggers
 * does not break triggers in soft upgrade mode.
 * <LI> testReusableRecordIdSequenceNumber - Test reuseable record
 * identifiers does not cause issues in soft upgrade
 * <LI> testGrantRevokeStatements - Check G/R not allowed in soft upgrade.
 * <LI> testDatabaseOwner - test that on a hard upgrade database owner is set.
 * </UL>
 */
public class Changes10_2 extends UpgradeChange {
    
    public static Test suite() {
        TestSuite suite = new TestSuite("Upgrade changes for 10.2");
        
        suite.addTestSuite(Changes10_2.class);
        
        // Encryption only support on J2SE or higher.
        if (JDBC.vmSupportsJDBC3())
        {
            suite.addTest(new Changes10_2("changeEncryptionFromNone"));
            suite.addTest(new Changes10_2("changeEncryptionFromEncryptedDatabase"));
        }
        
        return suite;
    }

    public Changes10_2(String name) {
        super(name);
    }
    
    /**
     * Triger (internal) VTI
     * 10.2 - Check that a statement trigger created in 10.0
     * or 10.1 can be executed in 10.2 and that a statement
     * trigger created in soft upgrade in 10.2 can be used
     * in older releases.
     * 
     * The VTI implementing statement triggers changed in
     * 10.2 from implementations of ResultSet to implementations
     * of PreparedStatement. See DERBY-438. The internal
     * api for the re-written action statement remains the
     * same. The re-compile of the trigger on version changes
     * should automatically switch between the two implementations.
     *
     * @throws SQLException
     */
    public void testTriggerInternalVTI()
                                    throws SQLException {
                
        
        Statement s = createStatement();

        boolean modeDb2SqlOptional = oldAtLeast(10, 3);

        switch (getPhase()) {
        case PH_CREATE:
            s.execute("CREATE TABLE D438.T438(a int, b varchar(20), c int)");
            s.execute("INSERT INTO D438.T438 VALUES(1, 'DERBY-438', 2)");
            s.execute("CREATE TABLE D438.T438_T1(a int, b varchar(20))");
            s.execute("CREATE TABLE D438.T438_T2(a int, c int)");
            s.execute(
               "create trigger D438.T438_ROW_1 after UPDATE on D438.T438 " +
               "referencing new as n old as o " + 
               "for each row "+ 
               (modeDb2SqlOptional?"":"mode db2sql ") +
               "insert into D438.T438_T1(a, b) values (n.a, n.b || '_ROW')");
            s.executeUpdate(
               "create trigger D438.T438_STMT_1 after UPDATE on D438.T438 " +
               "referencing new_table as n " + 
               "for each statement "+ 
               (modeDb2SqlOptional?"":"mode db2sql ") +
               "insert into D438.T438_T1(a, b) select n.a, n.b || '_STMT' from n"); 
            
            commit();
            break;
            
        case PH_SOFT_UPGRADE:
            s.execute(
               "create trigger D438.T438_ROW_2 after UPDATE on D438.T438 " +
               "referencing new as n old as o " + 
               "for each row "+ 
               (modeDb2SqlOptional?"":"mode db2sql ") +
               "insert into D438.T438_T2(a, c) values (n.a, n.c + 100)");
             s.executeUpdate(
                "create trigger D438.T438_STMT_2 after UPDATE on D438.T438 " +
                "referencing new_table as n " + 
                "for each statement "+ 
               (modeDb2SqlOptional?"":"mode db2sql ") +
                "insert into D438.T438_T2(a, c) select n.a, n.c + 4000 from n"); 
                 
            commit();
            break;
        case PH_POST_SOFT_UPGRADE:
            break;
        case PH_HARD_UPGRADE:
           break;
        }
        
        // Test the firing of the triggers
        s.executeUpdate("UPDATE D438.T438 set c = c + 1");
        commit();
        
        ResultSet rs = s.executeQuery("SELECT a,b from D438.T438_T1 ORDER BY 2");
        JDBC.assertFullResultSet(rs, new String[][]
                {{"1", "DERBY-438_ROW"},
                {"1", "DERBY-438_STMT"}});
        rs.close();
        
        rs = s.executeQuery("SELECT a,c from D438.T438_T2 ORDER BY 2");
        if (getPhase() == PH_CREATE)
        {
            // expect no rows since the trigger that populates
            // the table is defined in soft upgrade.
            assertFalse(rs.next());
        }
        else
        {
            JDBC.assertFullResultSet(rs, new String[][] {
                    {"1", Integer.toString(2 + 100 + getPhase() + 1)},
                    {"1", Integer.toString(2 + 4000 + getPhase() + 1)}});
            
        }
        rs.close();
            
        s.executeUpdate("DELETE FROM D438.T438_T1");
        s.executeUpdate("DELETE FROM D438.T438_T2");
        commit();
       
        s.close();
    }
    
    /**
     * In 10.2: We will write a ReusableRecordIdSequenceNumber in the 
     * header of a FileContaienr.
     * 
     * Verify here that a 10.1 Database does not malfunction from this.
     * 10.1 Databases should ignore the field.
     */
    public void testReusableRecordIdSequenceNumber()
        throws SQLException
    {
        boolean runCompress = oldAtLeast(10, 1);

        switch(getPhase()) {
        case PH_CREATE: {
            Statement s = createStatement();
            s.execute("create table CT1(id int)");
            s.execute("insert into CT1 values 1,2,3,4,5,6,7,8,9,10");
            s.close();
            commit();
            break;
        }
        case PH_SOFT_UPGRADE:
            if (runCompress) {
                PreparedStatement ps = prepareStatement
                    ("call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE(?,?,?,?,?)");
                ps.setString(1, "APP"); // schema
                ps.setString(2, "CT1");  // table name
                ps.setInt(3, 1); // purge
                ps.setInt(4, 1); // defragment rows
                ps.setInt(5, 1); // truncate end
                ps.executeUpdate();
                ps.close();
               commit();
            }
            break;
        case PH_POST_SOFT_UPGRADE: {
            // We are now back to i.e 10.1
            Statement s = createStatement();
            ResultSet rs = s.executeQuery("select * from CT1");
            while (rs.next()) {
                rs.getInt(1);
            }
            s.execute("insert into CT1 values 11,12,13,14,15,16,17,18,19");
            s.close();
            commit();
            break;
        }
        case PH_HARD_UPGRADE:
            break;
        }
    }
    
    /**
     * Simple test of if GRANT/REVOKE statements are handled
     * correctly in terms of being allowed in soft upgrade.
     * @throws SQLException 
     *
     */
    public void testGrantRevokeStatements() throws SQLException
    {
        Statement s = createStatement();
        switch(getPhase()) {
        // 
        case PH_CREATE:
        case PH_POST_SOFT_UPGRADE:
            // was syntax error in 10.0,10.1
            assertStatementError("42X01", s,
                "GRANT SELECT ON TABLE1 TO USER1");
            assertStatementError("42X01", s,
                "REVOKE SELECT ON TABLE1 FROM USER1");

            break;
            
        case PH_SOFT_UPGRADE:
            // require hard upgrade
            assertStatementError(SQLSTATE_NEED_UPGRADE, s,
                "GRANT SELECT ON TABLE1 TO USER1");
            assertStatementError(SQLSTATE_NEED_UPGRADE, s,
                "REVOKE SELECT ON TABLE1 FROM USER1");

            break;
            
        case PH_HARD_UPGRADE:
            // not supported because SQL authorization not set
            assertStatementError("42Z60", s,
                "GRANT SELECT ON TABLE1 TO USER1");
            assertStatementError("42Z60", s,
                "REVOKE SELECT ON TABLE1 FROM USER1");
            break;
        }
        s.close();
    }
    
    /**
     * This method lists the schema names and authorization ids in 
     * SYS.SCHEMAS table. This is to test that the owner of system schemas is 
     * changed from pseudo user "DBA" to the user invoking upgrade. 
     * 
     * @throws SQLException
     */
    public void testDatabaseOwnerChange() throws SQLException
    {
        switch (getPhase())
        {
        case PH_CREATE:
        case PH_SOFT_UPGRADE:
        case PH_POST_SOFT_UPGRADE:
            checkSystemSchemasOwner("DBA");
            break;

        case PH_HARD_UPGRADE:
            checkSystemSchemasOwner(getTestConfiguration().getUserName());
            break;
        }
    }
    
    private void checkSystemSchemasOwner(String name) throws SQLException
    {
        Statement s = createStatement();
        ResultSet rs = s.executeQuery(
                "select AUTHORIZATIONID, SCHEMANAME from SYS.SYSSCHEMAS " +
                   "WHERE SCHEMANAME LIKE 'SYS%' OR " +
                   "SCHEMANAME IN ('NULLID', 'SQLJ')");
        
        while (rs.next()) {
            assertEquals("AUTHORIZATIONID not valid for " + rs.getString(2),
                    name, rs.getString(1));
        }
        
        rs.close();
        s.close();
    }
    
    /**
     * This method checks that some system routines are granted public access 
     * after a full upgrade.
     * 
     * @throws SQLException
     */
    public void testSystemRoutinePermissions() throws SQLException
    {
        switch (getPhase())
        {
        case PH_CREATE:
        case PH_SOFT_UPGRADE:
        case PH_POST_SOFT_UPGRADE:
            break;

        case PH_HARD_UPGRADE:
            Statement s = createStatement();
            ResultSet rs = s.executeQuery("select A.ALIAS FROM " +
                    "SYS.SYSROUTINEPERMS R, SYS.SYSALIASES A " +
                    "WHERE R.ALIASID = A.ALIASID AND " +
                    "R.GRANTEE = 'PUBLIC' AND " +
                    "R.GRANTOR = '"
                        + getTestConfiguration().getUserName() + "'" +
                    " ORDER BY 1");
            
            JDBC.assertFullResultSet(rs, new String[][]
                    {{"SYSCS_COMPRESS_TABLE"},
                    {"SYSCS_DROP_STATISTICS"},
                    {"SYSCS_GET_RUNTIMESTATISTICS"},
                    {"SYSCS_INPLACE_COMPRESS_TABLE"},
                    {"SYSCS_MODIFY_PASSWORD"},
                    {"SYSCS_PEEK_AT_IDENTITY"},
                    {"SYSCS_PEEK_AT_SEQUENCE"},
                    {"SYSCS_SET_RUNTIMESTATISTICS"},
                    {"SYSCS_SET_STATISTICS_TIMING"},
                    {"SYSCS_UPDATE_STATISTICS"}}
                    );

            rs.close();
            s.close();
            break;
        }
    }
    
    /**
     * Run the change encryption test against a
     * non-encrypted database. Test that changing the encryption
     * is only allowed if the database has been hard-upgraded. 
     * This test assumes it has its own single use database, which
     * will not be booted by the general upgrade test setup.
     * @throws SQLException
     */

    public void changeEncryptionFromNone() throws SQLException
    {
        DataSource ds = JDBCDataSource.getDataSourceLogical("NO_ENCRYPT_10_2");
        
        switch (getPhase())
        {
        case PH_CREATE:
            // create the database if it was not already created.
            JDBCDataSource.setBeanProperty(ds, "createDatabase", "create");
            ds.getConnection().close();
            break;
        case PH_SOFT_UPGRADE:
            JDBCDataSource.setBeanProperty(ds, "connectionAttributes",
                    "dataEncryption=true;bootPassword=xyz1234abc");
            
            try {
                ds.getConnection();
                fail("open re-encrypted connection in soft upgrade");
            } catch (SQLException e) {
                assertSQLState("XJ040", e);
                e = e.getNextException();
                assertNotNull(e);
                assertSQLState("XCL47", e);
            }
            break;
            
            
        case PH_POST_SOFT_UPGRADE:
            // Should be able to successfully connect to it
            // using the old setup.
            ds.getConnection().close();
            break;
            
        case PH_HARD_UPGRADE:
            // On hard upgrade should be able to connect to it
            // changing the encryption.
            // Note we have to explicitly upgrade additional databases.
            JDBCDataSource.setBeanProperty(ds, "connectionAttributes",
            "upgrade=true;dataEncryption=true;bootPassword=haRD1234upGrAde");
            ds.getConnection().close();
            
            // Shutdown the database.
            JDBCDataSource.clearStringBeanProperty(ds, "connectionAttributes");
            JDBCDataSource.shutdownDatabase(ds);
            
            // Reboot with no boot password, should fail
            try {
                ds.getConnection();
                fail("open re-encrypted connection without password");
            } catch (SQLException e) {
                assertSQLState("XJ040", e);
                e = e.getNextException();
                assertNotNull(e);
                assertSQLState("XBM06", e);
            }
            
            // And connect successfully.
            JDBCDataSource.setBeanProperty(ds, "connectionAttributes",
                       "bootPassword=haRD1234upGrAde");
            ds.getConnection().close();
            break;
        }
    }
    /**
     * Run the change encryption test against a
     * encrypted database. Test that changing the encryption
     * is only allowed if the database has been hard-upgraded. 
     * This test assumes it has its own single use database, which
     * will not be booted by the general upgrade test setup.
     * @throws SQLException
     */

    public void changeEncryptionFromEncryptedDatabase() throws SQLException
    {
        DataSource ds = JDBCDataSource.getDataSourceLogical("ENCRYPT_10_2");
        
        switch (getPhase())
        {
        case PH_CREATE:
            // create the database encrypted
            JDBCDataSource.setBeanProperty(ds, "createDatabase", "create");
            JDBCDataSource.setBeanProperty(ds, "connectionAttributes",
                 "dataEncryption=true;bootPassword=old862phRase");
            ds.getConnection().close();
            break;
        case PH_SOFT_UPGRADE:
            JDBCDataSource.setBeanProperty(ds, "connectionAttributes",
                    "bootPassword=old862phRase;newBootPassword=new902pHrAse");
            
            try {
                ds.getConnection();
                fail("open re-encrypted connection in soft upgrade");
            } catch (SQLException e) {
                assertSQLState("XJ040", e);
                e = e.getNextException();
                assertNotNull(e);
                assertSQLState("XCL47", e);
            }
            break;
            
            
        case PH_POST_SOFT_UPGRADE:
            // Should be able to successfully connect to it
            // using the old setup.
            JDBCDataSource.setBeanProperty(ds, "connectionAttributes",
                 "bootPassword=old862phRase");
            ds.getConnection().close();
            break;
            
        case PH_HARD_UPGRADE:
            // On hard upgrade should be able to connect to it
            // changing the encryption.
            // Note we have to explicitly upgrade additional databases.
            JDBCDataSource.setBeanProperty(ds, "connectionAttributes",
            "upgrade=true;bootPassword=old862phRase;newBootPassword=hard924pHrAse");
            ds.getConnection().close();
            
            // Shutdown the database.
            JDBCDataSource.clearStringBeanProperty(ds, "connectionAttributes");
            JDBCDataSource.shutdownDatabase(ds);
            
            // Reboot with no boot password, should fail
            try {
                ds.getConnection();
                fail("open re-encrypted connection without password");
            } catch (SQLException e) {
                assertSQLState("XJ040", e);
                e = e.getNextException();
                assertNotNull(e);
                assertSQLState("XBM06", e);
            }
            
            // And connect successfully.
            JDBCDataSource.setBeanProperty(ds, "connectionAttributes",
                       "bootPassword=hard924pHrAse");
            ds.getConnection().close();
            break;
        }
    }
}
