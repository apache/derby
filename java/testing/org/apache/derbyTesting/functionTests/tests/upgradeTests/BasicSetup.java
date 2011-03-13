/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.BasicSetup

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
}
