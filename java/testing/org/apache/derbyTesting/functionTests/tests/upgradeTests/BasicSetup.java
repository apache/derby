/*

Derby - Class org.apache.dertbyTesting.functionTests.tests.upgradeTests.BasicSetup

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

import org.apache.derby.iapi.services.io.DerbyIOException;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Basic fixtures and setup for the upgrade test, not
 * tied to any specific release.
 */
public class BasicSetup extends UpgradeChange {
    
    public static Test suite() {
        TestSuite suite = new TestSuite("Upgrade basic setup");
        
        suite.addTestSuite(BasicSetup.class);
        
        return suite;
    }

    public BasicSetup(String name) {
        super(name);
    }
      
    /**
     * Simple test of the triggers. Added for DERBY-4835
     */
    public void testTriggerBasic() throws SQLException
    {
        Statement s = createStatement();
        switch (getPhase())
        {
        case PH_CREATE:
            s.executeUpdate("CREATE TABLE Trigger_t1 " +
            		"(c1 INTEGER NOT NULL GENERATED ALWAYS " +
            		"AS IDENTITY (START WITH 1, INCREMENT BY 1), " +
            		"max_size INTEGER NOT NULL, "+
            		"CONSTRAINT c1_pk PRIMARY KEY (c1))");
            s.executeUpdate("CREATE TABLE Trigger_t2 "+
            		"(c1 INTEGER DEFAULT 0 NOT NULL)");
            s.executeUpdate("CREATE TRIGGER gls_blt_trg "+
            		"AFTER INSERT ON Trigger_t1 FOR EACH ROW MODE DB2SQL "+
            		"INSERT INTO Trigger_t2(c1) "+
            		"VALUES ( (select max(c1) from Trigger_t1))");
            s.executeUpdate("INSERT INTO Trigger_t1(max_size) "+
            		"VALUES(20)");
            break;
        case PH_SOFT_UPGRADE:
            s.executeUpdate("INSERT INTO Trigger_t1(max_size) "+
    		"VALUES(20)");
            break;
        case PH_POST_SOFT_UPGRADE:
            // DERBY-5105: The post soft upgrade phase may fail with
            // NoSuchMethodError if the old version suffers from DERBY-4835.
            // Only execute this part of the test for versions that don't
            // have this problem.
            if (!oldSuffersFromDerby4835()) {
                s.executeUpdate("INSERT INTO Trigger_t1(max_size) " +
                                "VALUES(20)");
            }
            break;
        case PH_HARD_UPGRADE:
            s.executeUpdate("INSERT INTO Trigger_t1(max_size) "+
    		"VALUES(20)");
            break;
        }
        s.close();
    }

    /**
     * Check if the old version from which we upgrade suffers from DERBY-4835.
     */
    private boolean oldSuffersFromDerby4835() {
        // DERBY-4835 exists on 10.5 and 10.6 prior to 10.5.3.2 and 10.6.2.3.
        return (oldAtLeast(10, 5) && oldLessThan(10, 5, 3, 2)) ||
                (oldAtLeast(10, 6) && oldLessThan(10, 6, 2, 3));
    }

    /**
     * Simple test of the old version from the meta data.
     */
    public void testOldVersion() throws SQLException
    {              
        switch (getPhase())
        {
        case PH_CREATE:
        case PH_POST_SOFT_UPGRADE:
            DatabaseMetaData dmd = getConnection().getMetaData();
            assertEquals("Old major (driver): ",
                    getOldMajor(), dmd.getDriverMajorVersion());
            assertEquals("Old minor (driver): ",
                    getOldMinor(), dmd.getDriverMinorVersion());
            assertEquals("Old major (database): ",
                    getOldMajor(), dmd.getDatabaseMajorVersion());
            assertEquals("Old minor (database): ",
                    getOldMinor(), dmd.getDatabaseMinorVersion());
            break;
        }
    }
    
    /**
     * Test general DML. Just execute some INSERT/UPDATE/DELETE
     * statements in all phases to see that generally the database works.
     * @throws SQLException
     */
    public void testDML() throws SQLException {
        
        final int phase = getPhase();
        
        Statement s = createStatement();
        
        switch (phase) {
        case PH_CREATE:
            s.executeUpdate("CREATE TABLE PHASE" +
                                                "(id INT NOT NULL, ok INT)");
            s.executeUpdate("CREATE TABLE TABLE1" +
                        "(id INT NOT NULL PRIMARY KEY, name varchar(200))");
            break;
        case PH_SOFT_UPGRADE:
            break;
        case PH_POST_SOFT_UPGRADE:
            break;
        case PH_HARD_UPGRADE:
            break;
        }
        s.close();
    
        PreparedStatement ps = prepareStatement(
                "INSERT INTO PHASE(id) VALUES (?)");
        ps.setInt(1, phase);
        ps.executeUpdate();
        ps.close();
        
        ps = prepareStatement("INSERT INTO TABLE1 VALUES (?, ?)");
        for (int i = 1; i < 20; i++)
        {
            ps.setInt(1, i + (phase * 100));
            ps.setString(2, "p" + phase + "i" + i);
            ps.executeUpdate();
        }
        ps.close();
        ps = prepareStatement("UPDATE TABLE1 set name = name || 'U' " +
                                    " where id = ?");
        for (int i = 1; i < 20; i+=3)
        {
            ps.setInt(1, i + (phase * 100));
            ps.executeUpdate();
        }
        ps.close();
        ps = prepareStatement("DELETE FROM TABLE1 where id = ?");
        for (int i = 1; i < 20; i+=4)
        {
            ps.setInt(1, i + (phase * 100));
            ps.executeUpdate();
        }
        ps.close();
        commit();
    }

    /**
     * Make sure table created in soft upgrade mode can be 
     * accessed after shutdown.  DERBY-2931
     * @throws SQLException
     */
    public void testCreateTable() throws SQLException
    {
        
        Statement stmt = createStatement();
        try {
            stmt.executeUpdate("DROP table t");
        } catch (SQLException se) {
            // ignore table does not exist error on
            // on drop table.
            assertSQLState("42Y55",se ); 
        }
        stmt.executeUpdate("CREATE TABLE T (I INT)");
        TestConfiguration.getCurrent().shutdownDatabase();
        stmt = createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * from t");
        JDBC.assertEmpty(rs);  
        rs.close();
    }
    

    /**
     * Test table with index can be read after
     * shutdown DERBY-2931
     * @throws SQLException
     */
    public void testIndex() throws SQLException 
    {
        Statement stmt = createStatement();
        try {
            stmt.executeUpdate("DROP table ti");
        } catch (SQLException se) {
            // ignore table does not exist error on
            // on drop table.
            assertSQLState("42Y55",se ); 
        }
        stmt.executeUpdate("CREATE TABLE TI (I INT primary key not null)");
        stmt.executeUpdate("INSERT INTO  TI values(1)");
        stmt.executeUpdate("INSERT INTO  TI values(2)");
        stmt.executeUpdate("INSERT INTO  TI values(3)");
        TestConfiguration.getCurrent().shutdownDatabase();
        stmt = createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * from TI ORDER BY I");
        JDBC.assertFullResultSet(rs, new String[][] {{"1"},{"2"},{"3"}});
        rs.close();        
    }

    
    /**
     * Ensure that after hard upgrade (with the old version)
     * we can no longer connect to the database.
     */
    public void noConnectionAfterHardUpgrade()
    {              
        switch (getPhase())
        {
        case PH_POST_HARD_UPGRADE:
            try {
                    getConnection();
                } catch (SQLException e) {
                    // Check the innermost of the nested exceptions
                    SQLException sqle = getLastSQLException(e);
                    String sqlState = sqle.getSQLState();
                	// while beta, XSLAP is expected, if not beta, XSLAN
                	if (!(sqlState.equals("XSLAP")) && !(sqlState.equals("XSLAN")))
                		fail("expected an error indicating no connection");
                }
            break;
        }
    }  
 
    
    /**
     * DERBY-5249 table created with primary and foreign key can't be dropped
     * Test currently disabled. Remove the x from the name to enable the 
     * test once the bug is fixed.
     * 
     */
    public void testDropTableAfterUpgradeWithConstraint() throws SQLException {
        final int phase = getPhase();

        Statement s = createStatement();

        switch (phase) {
        case PH_CREATE:
            s.executeUpdate("CREATE SCHEMA S");
            s.executeUpdate("CREATE TABLE S.RS (R_TYPE_ID VARCHAR(64) "
                    + "NOT NULL)");
            s.executeUpdate("ALTER TABLE S.RS ADD CONSTRAINT PK_RS "
                    + "PRIMARY KEY (R_TYPE_ID)");
            s.executeUpdate("CREATE TABLE S.R_TYPE_ID (R_TYPE_ID "
                    + "VARCHAR(64) NOT NULL)");
            s.executeUpdate("ALTER TABLE S.R_TYPE_ID ADD CONSTRAINT "
                    + "PK_R_TYPE_ID PRIMARY KEY (R_TYPE_ID)");
            s.executeUpdate("ALTER TABLE S.RS ADD CONSTRAINT "
                    + "FK_RS_TYPEID FOREIGN KEY (R_TYPE_ID) REFERENCES "
                    + "S.R_TYPE_ID (R_TYPE_ID) ON DELETE CASCADE ON "
                    + "UPDATE NO ACTION");
            /*
             * With 10.0 and early 10.1 releases a duplicate conglomerate entry
             * shows in sys.sysconglomerates for the primary key PK_RS. It can
             * be seen with this query.
             
                Utilities.showResultSet(s.executeQuery(
                        "select c.constraintname, c.constraintid,  cong.conglomerateid, cong.conglomeratename  from sys.sysconglomerates cong, sys.syskeys k, sys.sysconstraints c where c.constraintname = 'PK_RS' and c.constraintid =k.constraintid and k.conglomerateid = cong.conglomerateid "
              ));
            */
            break;
        case PH_SOFT_UPGRADE:
            s.executeUpdate("ALTER TABLE S.RS DROP CONSTRAINT FK_RS_TYPEID");
            s.executeUpdate("ALTER TABLE S.R_TYPE_ID DROP CONSTRAINT "
                    + "PK_R_TYPE_ID");
            s.executeUpdate("ALTER TABLE S.RS DROP CONSTRAINT PK_RS");
            s.executeUpdate("DROP TABLE S.RS");
            s.executeUpdate("DROP TABLE S.R_TYPE_ID");
            s.executeUpdate("DROP SCHEMA S RESTRICT");
            break;
        case PH_POST_SOFT_UPGRADE:
            break;
        case PH_HARD_UPGRADE:
            break;
        }

    }

    /**
     * Test that triggers that use XML operators work after upgrade. The
     * first fix for DERBY-3870 broke upgrade of such triggers because the
     * old execution plans failed to deserialize on the new version.
     * Even though DERBY-3870 fix was not fully backported. This test
     * and code relevant to deserialization was backported with DERBY-5289
     */
    public void xmlTestTriggerWithXMLOperators() throws SQLException {
        if (!oldAtLeast(10, 3)) {
            // Before 10.3, the CREATE TRIGGER statement used in the test
            // failed with a syntax error. Skip the test for older versions.
            return;
        }

        if (getPhase() == PH_POST_SOFT_UPGRADE && oldSuffersFromDerby4835()) {
            // DERBY-5263: Executing the trigger will fail after soft upgrade
            // in all the versions that suffer from DERBY-4835. Skip the test.
            return;
        }

        Statement s = createStatement();

        if (getPhase() == PH_CREATE) {
            // Create test tables and a trigger that uses XML operators with
            // the old version.
            s.execute("create table d3870_t1(i int, x varchar(100))");
            s.execute("create table d3870_t2(i int)");
            try {
                s.execute("create trigger d3870_tr after insert on d3870_t1 " +
                          "for each statement insert into d3870_t2 " +
                          "select i from d3870_t1 where " +
                          "xmlexists('//a' passing by ref " +
                          "xmlparse(document x preserve whitespace))");
            } catch (SQLException sqle) {
                // The CREATE TRIGGER statement will fail if the XML classpath
                // requirements aren't satisfied for the old version. That's
                // OK, but we'll have to skip the test for this combination.
                assertSQLState("XML00", sqle);
                return;
            }
        } else {
            // Delete the rows to start the test from a known state in each
            // of the phases.
            s.executeUpdate("delete from d3870_t1");
            s.executeUpdate("delete from d3870_t2");
        }

        // Check if the trigger exists. It won't exist if the XML requirements
        // weren't satisfied for the old version. If we don't have the trigger,
        // we skip the rest of the test.
        ResultSet rs = s.executeQuery(
            "select 1 from sys.systriggers where triggername = 'D3870_TR'");
        boolean hasTrigger = rs.next();
        rs.close();

        // Verify that the trigger works both before and after upgrade.
        if (hasTrigger) {
            s.execute("insert into d3870_t1 values " +
                      "(1, '<a/>'), (2, '<b/>'), (3, '<c/>')");

            JDBC.assertSingleValueResultSet(
                    s.executeQuery("select * from d3870_t2"), "1");
        }
    }
    
    /**
     * DERBY-5289 Upgrade could fail during upgrade with triggers due to 
     * failure reading serializable or SQLData object
     * @throws SQLException
     */
    public void testDERBY5289TriggerUpgradeFormat() throws SQLException {
        // if the old version suffers from DERBY-4835 we 
        // cannot run this test because the database won't boot
        // on soft upgrade and none of the fixtures will run.
        if (oldSuffersFromDerby4835())
            return;
        Statement s = createStatement();
        switch (getPhase())
        {
            case PH_CREATE:
                s.executeUpdate("CREATE TABLE D5289TABLE1 (COL1 VARCHAR(5))");
                s.executeUpdate("CREATE TABLE D5289TABLE2 (COL2 VARCHAR(5))");
                s.executeUpdate("CREATE TABLE D5289TABLE3 (COL3 VARCHAR(5))");
                s.executeUpdate("CREATE TRIGGER D5289T1_UPDATED AFTER UPDATE " +
                        "ON D5289TABLE1 REFERENCING OLD AS OLD NEW AS NEW FOR " +
                        "EACH ROW MODE DB2SQL UPDATE D5289TABLE2 SET COL2 = NEW.COL1 WHERE " +
                        "COL2 = OLD.COL1");
                s.executeUpdate("CREATE TRIGGER D5289T2_UPDATED AFTER UPDATE " + 
                        "ON D5289TABLE2 REFERENCING NEW AS NEW FOR EACH " +
                        "ROW MODE DB2SQL INSERT INTO D5289TABLE3(COL3) VALUES('ccc')");
                s.executeUpdate("insert into D5289TABLE1(COL1) values ('aaa') ");
                s.executeUpdate("insert into D5289TABLE2(COL2) values ('aaa') ");
                s.executeUpdate("UPDATE D5289TABLE1 SET COL1 = 'bbb'");
                assertDERBY5289ResultsAndDelete();
                break;
            case PH_SOFT_UPGRADE:   
                s.executeUpdate("insert into D5289TABLE1(COL1) values ('aaa')");
                s.executeUpdate("insert into D5289TABLE2(COL2) values ('aaa')");
                s.executeUpdate("UPDATE D5289TABLE1 SET COL1 = 'bbb'");
                assertDERBY5289ResultsAndDelete();                
                break;
            case PH_POST_SOFT_UPGRADE:
                // If old version suffers from DERBY-5289, we can't run this part of the 
                // DERBY-5289 won't go in until 10.8.2.0
                s.executeUpdate("insert into D5289TABLE1(COL1) values ('aaa')");
                s.executeUpdate("insert into D5289TABLE2(COL2) values ('aaa') ");
                s.executeUpdate("UPDATE D5289TABLE1 SET COL1 = 'bbb'");
                assertDERBY5289ResultsAndDelete();
                break;
            case PH_HARD_UPGRADE:
                s.executeUpdate("insert into D5289TABLE1(COL1) values ('aaa')");
                s.executeUpdate("insert into D5289TABLE2(COL2) values ('aaa') ");
                s.executeUpdate("UPDATE D5289TABLE1 SET COL1 = 'bbb'");
                assertDERBY5289ResultsAndDelete();
                break;
        }
    }

    /**
     * Private helper method for fixture testDERBY5289TriggerUpgradeFormat
     * to check and cleanup date in each phase.
     * 
     * @throws SQLException
     */
    private void assertDERBY5289ResultsAndDelete() throws SQLException {
        Statement s = createStatement();
        JDBC.assertFullResultSet(s.executeQuery("SELECT * FROM D5289TABLE1"), 
                new String[][] {{"bbb"}});        
        JDBC.assertFullResultSet(s.executeQuery("SELECT * FROM D5289TABLE2"),
                new String[][] {{"bbb"}});
        JDBC.assertFullResultSet(s.executeQuery("SELECT * FROM D5289TABLE3"), 
                new String[][] {{"ccc"}});
        s.executeUpdate("DELETE FROM D5289TABLE1");
        s.executeUpdate("DELETE FROM D5289TABLE2");
        s.executeUpdate("DELETE FROM D5289TABLE3");
        commit();  
    }
}
