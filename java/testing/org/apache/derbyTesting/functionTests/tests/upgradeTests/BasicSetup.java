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
import java.util.ArrayList;

import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetStream;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.XML;

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

        if (XML.classpathMeetsXMLReqs()) {
            // Only test XML operators if they are supported by the version
            // we upgrade to.
            suite.addTest(new BasicSetup("xmlTestTriggerWithXMLOperators"));
        }

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

    public void testDERBY5121TriggerTest2() throws Exception
    {
        Statement s = createStatement();
        boolean modeDb2SqlOptional = oldAtLeast(10, 3);
    	String updateSQL = "update media "+
    	"set name = 'Mon Liza', description = 'Something snarky.' " +
    	"where mediaID = 1";
        
        switch ( getPhase() )
        {
        case PH_CREATE: // create with old version
        	s.execute("create table folder ( "+
        			"folderType	int	not null, folderID	int	not null, "+
        			"folderParent int, folderName varchar(50) not null)");
        	s.execute("create table media ( " +
        			"mediaID int not null, name varchar(50)	not null, "+
        			"description clob not null, mediaType varchar(50), "+
        			"mediaContents	blob, folderID int not null	default 7)");
        	s.execute("create trigger mediaInsrtDupTrgr " +
        			"after INSERT on media referencing new as nr "+
        			"for each ROW "+
        			(modeDb2SqlOptional?"":"MODE DB2SQL ") +
        			"values( nr.folderID, 7, nr.name)");
        	s.execute("create trigger mediaUpdtDupTrgr " +
        			"after UPDATE of folderID, name on media " +
        			"referencing new as nr "+
        			"for each ROW "+
        			(modeDb2SqlOptional?"":"MODE DB2SQL ") +
        			"values( nr.folderID, 7, nr.name)");
        	s.executeUpdate("insert into folder(folderType, folderID, "+
        			"folderParent, folderName ) "+
        			"values ( 7, 7, null, 'media' )");
        	s.executeUpdate("insert into media(mediaID, name, description)"+
        			"values (1, 'Mona Lisa', 'A photo of the Mona Lisa')");
        	if (oldIs(10,7,1,1))
                assertStatementError(  "XCL12", s, updateSQL );
        	else
        		s.executeUpdate(updateSQL);
        	break;

        case PH_SOFT_UPGRADE:
    		s.executeUpdate(updateSQL);
        	break;
        	
        case PH_POST_SOFT_UPGRADE:
        	//Derby 10.7.1.1 is not going to work because UPDATE sql should
        	// have read all the columns from the trigger table but it did
        	// not and hence trigger can't find the column it needs from the
        	// trigger table
        	if (oldIs(10,7,1,1))
                assertStatementError(  "S0022", s, updateSQL );
        	else
        		s.executeUpdate(updateSQL);
        	break;
        case PH_HARD_UPGRADE:
    		s.executeUpdate(updateSQL);
        	break;
        case PH_POST_HARD_UPGRADE:
    		s.executeUpdate(updateSQL);
        	s.executeUpdate("drop table media");
        	s.executeUpdate("drop table folder");
        	break;
        }
    }

    /**
     * Changes made for DERBY-1482 caused corruption which is being logged 
     *  under DERBY-5121. The issue is that the generated trigger action
     *  sql could be looking for columns (by positions, not names) in
     *  incorrect positions. With DERBY-1482, trigger assumed that the
     *  runtime resultset that they will get will only have trigger columns
     *  and trigger action columns used through the REFERENCING column.
     *  That is an incorrect assumption because the resultset could have
     *  more columns if the triggering sql requires more columns. DERBY-1482
     *  changes are in 10.7 and higher codelines. Because of this bug, the
     *  changes for DERBY-1482 have been backed out from 10.7 and 10.8
     *  codelines so they now match 10.6 and earlier releases. This in 
     *  other words means that the resultset presented to the trigger
     *  will have all the columns from the trigger table and the trigger
     *  action generated sql should look for the columns in the trigger
     *  table by their absolution column position in the trigger table.
     *  This disabling of code will make sure that all the future triggers
     *  get created correctly. The existing triggers at the time of 
     *  upgrade (to the releases with DERBY-1482 backout changes in them)
     *  will get marked invalid and when they fire next time around,
     *  the regenerated sql for them will be generated again and they
     *  will start behaving correctly. So, it is highly recommended that
     *  we upgrade 10.7.1.1 to next point release of 10.7 or to 10.8
     * @throws Exception
     */
    public void testDERBY5121TriggerDataCorruption() throws Exception
    {
        Statement s = createStatement();
        ResultSet rs;
        boolean modeDb2SqlOptional = oldAtLeast(10, 3);
        
        switch ( getPhase() )
        {
        case PH_CREATE: // create with old version
        	//The following test case is for testing in different upgrade
        	// phases what happens to buggy trigger created with 10.7.1.1. 
        	// Such triggers will get fixed
        	// 1)in hard upgrade when they get fired next time around.
        	// 2)in soft upgrade if they get fired during soft upgrade session.
        	//For all the other releases, we do not generate buggy triggers
        	// and hence everything should work just fine during all phases
        	// of upgrade including the CREATE time
            s.execute("CREATE TABLE UPGRADE_tab1(id int, name varchar(20))");
            s.execute("CREATE TABLE UPGRADE_tab2(" +
            		"name varchar(20) not null, " +
            		"description int not null, id int)");
            s.execute("create trigger UPGRADE_Trg1 " +
            		"after UPDATE of name on UPGRADE_tab2 " +
            		"referencing new as nr for each ROW "+
                    (modeDb2SqlOptional?"":"MODE DB2SQL ") +
                    "insert into UPGRADE_tab1 values ( nr.id, nr.name )");
            //load data into trigger table
            s.execute("insert into UPGRADE_tab2(name,description) "+
            		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update UPGRADE_tab2 " +
        			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from UPGRADE_tab1");
        	//If we are testing 10.7.1.1, which is where DERBY-5121 was
        	// detected, we will find that the trigger did not insert
        	// the correct data thus causing the corruption. For all the
        	// earlier releases, we do not have DERBY-5121 and hence
        	// trigger will insert the correct data.
        	if (oldIs(10,7,1,1))
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{"1","Another name"}});        		
        	else
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{null,"Another name"}});
        	s.execute("delete from UPGRADE_tab1");
        	s.execute("delete from UPGRADE_tab2");

        	//Following test is to test that the buggy triggers created in 
        	// 10.7.1.1 will continue to exhibit incorrect behavior if they 
        	// do not get fired during soft upgrade and the database is taken
        	// back to 10.7.1.1
            s.execute("CREATE TABLE POSTSFT_UPGRD_tab1(id int, name varchar(20))");
            s.execute("CREATE TABLE POSTSFT_UPGRD_tab2(" +
            		"name varchar(20) not null, " +
            		"description int not null, id int)");
            //We want this trigger to fire only for post hard upgrade
            s.execute("create trigger POSTSFT_UPGRD_Trg1 " +
            		"after UPDATE of name on POSTSFT_UPGRD_tab2 " +
            		"referencing new as nr for each ROW "+
                    (modeDb2SqlOptional?"":"MODE DB2SQL ") +
                    "insert into POSTSFT_UPGRD_tab1 values ( nr.id, nr.name )");
            //load data into trigger table
            s.execute("insert into POSTSFT_UPGRD_tab2(name,description) "+
    		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update POSTSFT_UPGRD_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from POSTSFT_UPGRD_tab1");
        	//If we are testing 10.7.1.1, which is where DERBY-5121 was
        	// detected, we will find that the trigger did not insert
        	// the correct data thus causing the corruption. For all the
        	// earlier releases, we do not have DERBY-5121 and hence
        	// trigger will insert the correct data.
        	if (oldIs(10,7,1,1))
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{"1","Another name"}});
        	else
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{null,"Another name"}});
        	s.execute("delete from POSTSFT_UPGRD_tab1");
        	s.execute("delete from POSTSFT_UPGRD_tab2");

        	//Following test is to test that the buggy triggers created in
        	// 10.7.1.1 will get fixed when they get upgraded to 10.8 and 
        	// higher
            s.execute("CREATE TABLE HARD_UPGRADE_tab1(id int, name varchar(20))");
            s.execute("CREATE TABLE HARD_UPGRADE_tab2(" +
            		"name varchar(20) not null, " +
            		"description int not null, id int)");
            s.execute("create trigger HARD_UPGRADE_Trg1 " +
            		"after UPDATE of name on HARD_UPGRADE_tab2 " +
            		"referencing new as nr for each ROW "+
                    (modeDb2SqlOptional?"":"MODE DB2SQL ") +
                    "insert into HARD_UPGRADE_tab1 values ( nr.id, nr.name )");
            //load data into trigger table
            s.execute("insert into HARD_UPGRADE_tab2(name,description) "+
    		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update HARD_UPGRADE_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from HARD_UPGRADE_tab1");
        	//If we are testing 10.7.1.1, which is where DERBY-5121 was
        	// detected, we will find that the trigger did not insert
        	// the correct data thus causing the corruption. For all the
        	// earlier releases, we do not have DERBY-5121 and hence
        	// trigger will insert the correct data.
        	if (oldIs(10,7,1,1))
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{"1","Another name"}});        		
        	else
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{null,"Another name"}});
        	s.execute("delete from HARD_UPGRADE_tab1");
        	s.execute("delete from HARD_UPGRADE_tab2");

        	//Following test is to test that the buggy triggers created in
        	// 10.7.1.1 will get fixed when they get upgraded to 10.8 and 
        	// higher even if they did not get fired during the session which
        	// did the upgrade
            s.execute("CREATE TABLE POSTHRD_UPGRD_tab1(id int, name varchar(20))");
            s.execute("CREATE TABLE POSTHRD_UPGRD_tab2(" +
            		"name varchar(20) not null, " +
            		"description int not null, id int)");
            //We want this trigger to fire only for post hard upgrade
            s.execute("create trigger POSTHRD_UPGRD_Trg1 " +
            		"after UPDATE of name on POSTHRD_UPGRD_tab2 " +
            		"referencing new as nr for each ROW "+
                    (modeDb2SqlOptional?"":"MODE DB2SQL ") +
                    "insert into POSTHRD_UPGRD_tab1 values ( nr.id, nr.name )");
            //load data into trigger table
            s.execute("insert into POSTHRD_UPGRD_tab2(name,description) "+
    		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update POSTHRD_UPGRD_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from POSTHRD_UPGRD_tab1");
        	//If we are testing 10.7.1.1, which is where DERBY-5121 was
        	// detected, we will find that the trigger did not insert
        	// the correct data thus causing the corruption. For all the
        	// earlier releases, we do not have DERBY-5121 and hence
        	// trigger will insert the correct data.
        	if (oldIs(10,7,1,1))
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{"1","Another name"}});
        	else
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{null,"Another name"}});
        	s.execute("delete from POSTHRD_UPGRD_tab1");
        	s.execute("delete from POSTHRD_UPGRD_tab2");
            break;
            
        case PH_SOFT_UPGRADE:
        	//Following test case shows that the buggy trigger created in
        	// 10.7.1.1 got fixed when it got fired in soft upgrade mode
            //load data into trigger table
            s.execute("insert into UPGRADE_tab2(name,description) "+
    		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update UPGRADE_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from UPGRADE_tab1");
            JDBC.assertFullResultSet(rs,
               		new String[][]{{null,"Another name"}});
        	s.execute("delete from UPGRADE_tab1");
        	s.execute("delete from UPGRADE_tab2");
        	s.execute("drop trigger UPGRADE_Trg1");

        	//Following test case shows that the trigger created during
        	// soft upgrade mode behave correctly and will not exhibit
        	// the buggy behavior of 10.7.1.1
        	s.execute("create trigger UPGRADE_Trg1 " +
            		"after UPDATE of name on UPGRADE_tab2 " +
            		"referencing new as nr for each ROW "+
                    (modeDb2SqlOptional?"":"MODE DB2SQL ") +
                    "insert into UPGRADE_tab1 values ( nr.id, nr.name )");
            //load data into trigger table
            s.execute("insert into UPGRADE_tab2(name,description) "+
            		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update UPGRADE_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from UPGRADE_tab1");
            JDBC.assertFullResultSet(rs,
               		new String[][]{{null,"Another name"}});
        	s.execute("delete from UPGRADE_tab1");
        	s.execute("delete from UPGRADE_tab2");
            break;

        case PH_POST_SOFT_UPGRADE: 
        	//Following test shows that because the buggy trigger created in
        	// 10.7.1.1 was fired during the soft upgrade mode, it has gotten
        	// fixed and it will work correctly in all the releaes
            //load data into trigger table
            s.execute("insert into UPGRADE_tab2(name,description) "+
    		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update UPGRADE_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from UPGRADE_tab1");
            JDBC.assertFullResultSet(rs,
               		new String[][]{{null,"Another name"}});
        	s.execute("delete from UPGRADE_tab1");
        	s.execute("delete from UPGRADE_tab2");
        	s.execute("drop trigger UPGRADE_Trg1");

        	//Following test case says that if we are back to 10.7.1.1 after
        	// soft upgrade, we will continue to create buggy triggers. The
        	// only solution to this problem is to upgrade to a release that
        	// fixes DERBY-5121
        	s.execute("create trigger UPGRADE_Trg1 " +
            		"after UPDATE of name on UPGRADE_tab2 " +
            		"referencing new as nr for each ROW "+
                    (modeDb2SqlOptional?"":"MODE DB2SQL ") +
                    "insert into UPGRADE_tab1 values ( nr.id, nr.name )");
            //load data into trigger table
            s.execute("insert into UPGRADE_tab2(name,description) "+
            		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update UPGRADE_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from UPGRADE_tab1");
        	//If we are testing 10.7.1.1, which is where DERBY-5121 was
        	// detected, we will find that the trigger did not insert
        	// the correct data thus causing the corruption. For all the
        	// earlier releases, we do not have DERBY-5121 and hence
        	// trigger will insert the correct data.
        	if (oldIs(10,7,1,1))
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{"1","Another name"}});        		
        	else
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{null,"Another name"}});
        	s.execute("delete from UPGRADE_tab1");
        	s.execute("delete from UPGRADE_tab2");

        	//Following shows that the triggers that didn't get fired during
        	// soft upgrade will continue to exhibit incorrect behavior in
        	// 10.7.1.1. The only solution to this problem is to upgrade to a 
        	// release that fixes DERBY-5121
        	//load data into trigger table
            s.execute("insert into POSTSFT_UPGRD_tab2(name,description) "+
            		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update POSTSFT_UPGRD_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from POSTSFT_UPGRD_tab1");
        	if (oldIs(10,7,1,1))
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{"1","Another name"}});        		
        	else
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{null,"Another name"}});
        	s.execute("delete from POSTSFT_UPGRD_tab1");
        	s.execute("delete from POSTSFT_UPGRD_tab2");

        	//Following shows that the triggers that didn't get fired during
        	// soft upgrade will continue to exhibit incorrect behavior in
        	// 10.7.1.1. The only solution to this problem is to upgrade to a 
        	// release that fixes DERBY-5121
            //load data into trigger table
            s.execute("insert into HARD_UPGRADE_tab2(name,description) "+
    		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update HARD_UPGRADE_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from HARD_UPGRADE_tab1");
        	if (oldIs(10,7,1,1))
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{"1","Another name"}});        		
        	else
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{null,"Another name"}});
        	s.execute("delete from HARD_UPGRADE_tab1");
        	s.execute("delete from HARD_UPGRADE_tab2");

        	//Following shows that the triggers that didn't get fired during
        	// soft upgrade will continue to exhibit incorrect behavior in
        	// 10.7.1.1. The only solution to this problem is to upgrade to a 
        	// release that fixes DERBY-5121
            //load data into trigger table
            s.execute("insert into POSTHRD_UPGRD_tab2(name,description) "+
    		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update POSTHRD_UPGRD_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from POSTHRD_UPGRD_tab1");
        	if (oldIs(10,7,1,1))
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{"1","Another name"}});        		
        	else
                JDBC.assertFullResultSet(rs,
                   		new String[][]{{null,"Another name"}});
        	s.execute("delete from POSTHRD_UPGRD_tab1");
        	s.execute("delete from POSTHRD_UPGRD_tab2");
            break;
            
        case PH_HARD_UPGRADE:
        	//Following test shows that the buggy trigger created with 10.7.1.1
        	// will get fixed after hard upgrade. Following trigger was fired
        	// during soft upgrade and post soft upgrade
            //load data into trigger table
            s.execute("insert into UPGRADE_tab2(name,description) "+
    		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update UPGRADE_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from UPGRADE_tab1");
            JDBC.assertFullResultSet(rs,
               		new String[][]{{null,"Another name"}});
        	s.execute("delete from UPGRADE_tab1");
        	s.execute("delete from UPGRADE_tab2");

        	//Following test shows that the buggy trigger created with 10.7.1.1
        	// will get fixed after hard upgrade. Following trigger was never
        	// fired in soft upgrade mode
            //load data into trigger table
            s.execute("insert into HARD_UPGRADE_tab2(name,description) "+
    		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update HARD_UPGRADE_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from HARD_UPGRADE_tab1");
            JDBC.assertFullResultSet(rs,
               		new String[][]{{null,"Another name"}});
        	s.execute("delete from HARD_UPGRADE_tab1");
        	s.execute("delete from HARD_UPGRADE_tab2");
            break;
            
        case PH_POST_HARD_UPGRADE:
        	//Following test shows that the buggy trigger created with 10.7.1.1
        	// will get fixed after hard upgrade. Following trigger was fired
        	// during soft upgrade and post soft upgrade & during hard upgrade
            //load data into trigger table
            //load data into trigger table
            s.execute("insert into UPGRADE_tab2(name,description) "+
    		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update UPGRADE_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from UPGRADE_tab1");
            JDBC.assertFullResultSet(rs,
               		new String[][]{{null,"Another name"}});
        	s.execute("delete from UPGRADE_tab1");
        	s.execute("delete from UPGRADE_tab2");

        	//Following test shows that the buggy trigger created with 10.7.1.1
        	// will get fixed after hard upgrade. Following trigger was never
        	// fired in soft upgrade mode but was fired during hard upgrade
            //load data into trigger table
            //load data into trigger table
            s.execute("insert into HARD_UPGRADE_tab2(name,description) "+
    		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update HARD_UPGRADE_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from HARD_UPGRADE_tab1");
            JDBC.assertFullResultSet(rs,
               		new String[][]{{null,"Another name"}});
        	s.execute("delete from HARD_UPGRADE_tab1");
        	s.execute("delete from HARD_UPGRADE_tab2");

        	//Following test shows that the buggy trigger created with 10.7.1.1
        	// will get fixed after hard upgrade. This is the first time this
        	// trigger got fired after it's creation in 10.7.1.1 CREATE mode
            //load data into trigger table
            //load data into trigger table
            s.execute("insert into POSTHRD_UPGRD_tab2(name,description) "+
    		"values ( 'Foo1 Name', 0 )");
            //Cause the trigger to fire
        	s.execute("update POSTHRD_UPGRD_tab2 " +
			"set name = 'Another name' , description = 1");
        	rs = s.executeQuery("select * from POSTHRD_UPGRD_tab1");
            JDBC.assertFullResultSet(rs,
               		new String[][]{{null,"Another name"}});
        	s.execute("delete from POSTHRD_UPGRD_tab1");
        	s.execute("delete from POSTHRD_UPGRD_tab2");
            break;
        }
    }

    //This test creates a table with LOB column and insets large data
    // into that column. There is a trigger defined on this table
    // but the trigger does not need access to the LOB column. In 10.8 
    // and prior releases, even though we don't need the LOB column to
    // execute the trigger, we still read all the columns from the 
    // trigger table when the trigger fired. With 10.9, only the columns 
    // required by the firing triggers are read from the trigger table
    // and hence for our test here, LOB column will not be materialized. 
    //In 10.8 and prior releases, the trigger defined in this test can
    // run into OOM errors depending on how much heap is available to
    // the upgrade test. But in 10.9 and higher, that won't happen
    // because LOB is never read into memory for the trigger being
    // used by this test.
    public void atestTriggersWithLOBcolumns() throws Exception
    {
        Statement s = createStatement();
        ResultSet rs;
        boolean modeDb2SqlOptional = oldAtLeast(10, 3);
    	final int lobsize = 50000*1024;
        
        switch ( getPhase() )
        {
        case PH_CREATE: // create with old version
    		s.execute("create table table1LOBtest (id int, status smallint, bl blob(2G))");
    		PreparedStatement ps = prepareStatement(
    		"insert into table1LOBtest values (?, 0, ?)");
    		ps.setInt(1, 1);
            ps.setBinaryStream(2, new LoopingAlphabetStream(lobsize), lobsize);
            ps.executeUpdate();
            
    		s.execute("create table table2LOBtest (id int, updates int default 0)");
    		ps = prepareStatement(
    				"insert into table2LOBtest (id) values (?)");
    		ps.setInt(1, 1);
            ps.executeUpdate();

            s.execute("create trigger trigger1 after update of status on table1LOBtest referencing " +
    				"new as n_row for each row " +
        			(modeDb2SqlOptional?"":"MODE DB2SQL ") +
    				"update table2LOBtest set updates = updates + 1 where table2LOBtest.id = n_row.id");
            break;
            
        case PH_HARD_UPGRADE:
            //In 10.8 and prior releases, the trigger defined in this test can
            // run into OOM errors depending on how much heap is available to
            // the upgrade test. The reason for this is that 10.8 and prior
        	// read all the columns from the trigger table whether or not the
        	// firing triggers needed them. Since the table in this test has a
        	// LOB column, it can cause 10.8 and prior to run into OOM. But in 
        	// 10.9 and higher, that won't happen because LOB is never accessed
        	// by the trigger defined here, it will not be read into memory and
        	// will not cause OOM. For this reason, there is an IF condition
        	// below before we issue a triggering sql which could result into
        	// OOM in 10.8 and prior
        	if ((getConnection().getMetaData().getDatabaseMajorVersion() >= 10) &&
        	(getConnection().getMetaData().getDatabaseMinorVersion() >= 9))
        	{
        		ps = prepareStatement(
        				"update table1LOBtest set status = 1 where id = 1");
        		ps.executeUpdate();
        	}
            break;
        }
    }
    
    final   int TEST_COUNT = 0;
    final   int FAILURES = TEST_COUNT + 1;
    final   String  A_COL = "a";
    final   String  B_COL = "b";

    //This test has been contributed by Rick Hillegas for DERBY-5121
    // The test exhaustively walks through all subsets and permutations 
    // of columns for a trigger which inserts into a side table based on 
    // updates to a master table.
    public void testExhuastivePermutationOfTriggerColumns() throws Exception
    {
        final   int STATUS_COUNTERS = FAILURES + 1;
        int columnCount = 3;
        int[][]   powerSet = constructPowerSet( columnCount );
        int[][] permutations = permute( powerSet );
        int[]   statusCounters = new int[ STATUS_COUNTERS ];

        switch ( getPhase() )
        {
        case PH_CREATE: // create with old version
            for ( int triggerCols = 0; triggerCols < powerSet.length; triggerCols++ )
            {
                for ( int perm = 0; perm < permutations.length; perm++ )
                {
                    createT1( powerSet[ triggerCols ], permutations[ perm ] );
                    createT2( columnCount, powerSet[ triggerCols ], permutations[ perm ]  );
                    createTrigger( powerSet[ triggerCols ], permutations[ perm ] );
                }
            }
        	break;

        case PH_SOFT_UPGRADE:
            for ( int triggerCols = 0; triggerCols < powerSet.length; triggerCols++ )
            {
                for ( int perm = 0; perm < permutations.length; perm++ )
                {
                    for ( int i = 0; i < permutations.length; i++ )
                    {
                        runTrigger( statusCounters, columnCount, powerSet[ triggerCols ], permutations[ perm ], permutations[ i ] );
                    }
                }
            }
        	break;
        }
        summarize( statusCounters );
    }
    
    //Start of helper methods for testExhuastivePermutationOfTriggerColumns

    ////////////////////////
    //
    // make power set of N
    //
    ////////////////////////

    private int[][] constructPowerSet( int count )
    {
    	java.util.ArrayList list = new java.util.ArrayList();
        boolean[]           inclusions = new boolean[ count ];

        include( list, 0, inclusions );
        
        int[][] result = new int[ list.size() ][];
        list.toArray( result );

        return result;
    }

    private void    include( ArrayList list, int idx, boolean[] inclusions )
    {
        if ( idx >= inclusions.length )
        {
            int totalLength = inclusions.length;
            int count = 0;
            for ( int i = 0; i < totalLength; i++ )
            {
                if ( inclusions[ i ] ) { count++; }
            }

            if ( count > 0 )
            {
                int[]   result = new int[ count ];
                int     index = 0;
                for ( int i = 0; i < totalLength; i++ )
                {
                    if ( inclusions[ i ] ) { result[ index++ ] = i; }
                }
                
                list.add( result );
            }

            return;
        }

        include( list, idx, inclusions, false );
        include( list, idx, inclusions, true );
    }

    private void    include( ArrayList list, int idx, boolean[] inclusions, boolean currentCell )
    {
        inclusions[ idx++ ] = currentCell;

        // this is where the recursion happens
        include( list, idx, inclusions );
    }

    ////////////////////////////////////////////////
    //
    // create all permutations of an array of numbers
    //
    ////////////////////////////////////////////////
    private int[][] permute( int[][] original )
    {
        ArrayList list = new ArrayList();

        for ( int i = 0; i < original.length; i++ )
        {
            permute( list, new int[0], original[ i ] );
        }
        
        int[][] result = new int[ list.size() ][];
        list.toArray( result );

        return result;
    }

    private void   permute( ArrayList list, int[] start, int[] remainder )
    {
        int startLength = start.length;
        int remainderLength = remainder.length;
        
        for ( int idx = 0; idx < remainder.length; idx++ )
        {
            int[] newStart = new int[ startLength + 1 ];
            for ( int i = 0; i < startLength; i++ ) { newStart[ i ] = start[ i ]; }
            newStart[ startLength ] = remainder[ idx ];

            if ( remainderLength <= 1 ) { list.add( newStart ); }
            else
            {
                int[]   newRemainder = new int[ remainderLength - 1 ];
                int     index = 0;
                for ( int i = 0; i < remainderLength; i++ )
                {
                    if ( i != idx ) { newRemainder[ index++ ] = remainder[ i ]; }
                }

                // this is where the recursion happens
                permute( list, newStart, newRemainder );
            }
        }   // end loop through all remainder elements
    }

    private String  columnName( String stub, int idx ) { return (stub + '_' + idx ); }

    private void createT1(int[] triggerCols, int[] permutation )
    throws Exception
    {
        StringBuffer   buffer = new StringBuffer();
        buffer.append( "create table " + makeTableName( "t1", triggerCols, permutation ) + "( " );
        for ( int i = 0; i < permutation.length; i++ )
        {
            if ( i > 0 ) { buffer.append( ", " ); }
            buffer.append( columnName( B_COL, i ) );
            buffer.append( " int" );
        }
        buffer.append( " )" );
        Statement s = createStatement();
        s.execute(buffer.toString());
    }    
    
    private void    createT2(int columnCount, int[] triggerCols, int[] permutation  )
    throws Exception
    {
        StringBuffer   buffer = new StringBuffer();
        buffer.append( "create table " + makeTableName( "t2", triggerCols, permutation ) + "( " );
        for ( int i = 0; i < columnCount; i++ )
        {
            if ( i > 0 ) { buffer.append( ", " ); }
            buffer.append( columnName( A_COL, i ) );
            buffer.append( " int" );
        }
        buffer.append( " )" );
        Statement s = createStatement();
        s.execute(buffer.toString());
    }

    private String  makeTableName( String stub, int[] triggerCols, int[] permutation )
    {
        StringBuffer   buffer = new StringBuffer();

        buffer.append( stub );
        for ( int i = 0; i < triggerCols.length; i++ )
        {
            buffer.append( "_" );
            buffer.append( triggerCols[ i ] );
        }
       buffer.append( "__" );
        for ( int i = 0; i < permutation.length; i++ )
        {
            buffer.append( "_" );
            buffer.append( permutation[ i ] );
        }

        return buffer.toString();
    }

    private void    createTrigger(int[] triggerCols, int[] permutation )
    throws Exception
    {
        boolean modeDb2SqlOptional = oldAtLeast(10, 3);
        StringBuffer   buffer = new StringBuffer();
        buffer.append( "create trigger " + makeTriggerName( "UTrg", triggerCols, permutation ) + " after update of " );
        for ( int i = 0; i < triggerCols.length; i++ )
        {
            if ( i > 0 ) { buffer.append( ", " ); }
            buffer.append( columnName( A_COL, triggerCols[ i ] ) );
        }
        		
        buffer.append( "\n\ton " + makeTableName( "t2", triggerCols, permutation ) + " referencing new as nr for each row " );
        buffer.append( modeDb2SqlOptional?"":"\n\tMODE DB2SQL ");
        buffer.append( "\n\tinsert into " + makeTableName( "t1", triggerCols, permutation ) + " values ( " );
        for ( int i = 0; i < permutation.length; i++ )
        {
            if ( i > 0 ) { buffer.append( ", " ); }
            buffer.append( "nr." + columnName( A_COL, permutation[ i ] ) );
        }
        buffer.append( " )" );

        Statement s = createStatement();
        s.execute(buffer.toString());
    }

    private String  makeTriggerName( String stub, int[] triggerCols, int[] permutation )
    {
        StringBuffer   buffer = new StringBuffer();

        buffer.append( stub );
        for ( int i = 0; i < triggerCols.length; i++ )
        {
            buffer.append( "_" );
            buffer.append( triggerCols[ i ] );
        }
        buffer.append( "__" );
        for ( int i = 0; i < permutation.length; i++ )
        {
            buffer.append( "_" );
            buffer.append( permutation[ i ] );
        }
        
        return buffer.toString();
    }

    private int[]   getResults( int rowLength, String text )
        throws Exception
    {
        PreparedStatement   ps = prepareStatement(text );
        ResultSet               rs = ps.executeQuery();

        if ( !rs.next() ) { return new int[0]; }

        int[]                       result = new int[ rowLength ];
        for ( int i = 0; i < rowLength; i++ )
        {
            result[ i ] = rs.getInt( i + 1 );
        }

        rs.close();
        ps.close();

        return result;
    }

    private boolean overlap( int[] left, int[] right )
    {
        for ( int i = 0; i < left.length; i++ )
        {
            for ( int j = 0; j < right.length; j++ )
            {
                if ( left[ i ] == right[ j ] )
                {
                    //println( true, stringify( left ) + " overlaps " + stringify( right ) );
                    return true;
                }
            }
        }

        //println( true, stringify( left ) + " DOES NOT overlap " + stringify( right ) );
        return false;
    }

    private void    vetData
    ( int[] statusCounters, int[] triggerCols, int[] permutation, int[] updateColumns, String updateStatement )
    throws Exception
    {
        String  t1Name = makeTableName( "t1", triggerCols, permutation );
        String  t2Name = makeTableName( "t2", triggerCols, permutation );
        int     rowLength = permutation.length;
        int[]   t1Row = getResults( rowLength, "select * from " + t1Name );

        if ( !overlap( triggerCols, updateColumns ) )
        {
            if ( t1Row.length != 0 )
            {
                fail
                    (
                     statusCounters,
                     triggerCols,
                     permutation,
                     updateColumns,
                     "No row should have been inserted into t1! updateStatement = '" + updateStatement + "' and t1Row = " + stringify( t1Row )
                     );
            }

            return;
        }
        
        StringBuffer   buffer = new StringBuffer();
        buffer.append( "select " );
        for ( int i = 0; i < permutation.length; i++ )
        {
            if ( i > 0 ) { buffer.append( ", " ); }
            buffer.append( columnName( A_COL, permutation[ i ] ) );
        }
        buffer.append( " from " + t2Name );
        int[]   t2Row = getResults( rowLength, buffer.toString() );

        if ( !stringify( t1Row ).equals( stringify( t2Row ) ) )
        {
            String  detail = "Wrong data inserted into t1! " +
                "updateStatement = '" + updateStatement + "'. " +
                "Expected " + stringify( t2Row ) +
                " but found " + stringify( t1Row );
                
            fail( statusCounters, triggerCols, permutation, updateColumns, detail );
        }
    }

    private void    runTrigger( int[] statusCounters, int columnCount, int[] triggerCols, int[] permutation, int[] updateColumns )
    throws Exception
    {
        statusCounters[ TEST_COUNT ]++;

        loadData( columnCount, triggerCols, permutation );
        String  updateStatement = updateData( statusCounters, triggerCols, permutation, updateColumns );
        vetData( statusCounters, triggerCols, permutation, updateColumns, updateStatement );
    }

    private void    loadData( int columnCount, int[] triggerCols, int[] permutation )
    throws Exception
    {
        String  t1Name = makeTableName( "t1", triggerCols, permutation );
        String  t2Name = makeTableName( "t2", triggerCols, permutation );
        Statement s = createStatement();
        s.execute("delete from " + t1Name);
        s.execute("delete from " + t2Name);
        
        StringBuffer   buffer = new StringBuffer();
        buffer.append( "insert into " + t2Name + " values ( " );
        for ( int i = 0; i < columnCount; i++ )
        {
            if ( i > 0 ) { buffer.append( ", " ); }
            buffer.append( i );
        }
        buffer.append( " )" );
        s.execute(buffer.toString());
    }
    
    private String    updateData( int[] statusCounters, int[] triggerCols, int[] permutation, int[] updateColumns )
    throws Exception
    {
        String  t2Name = makeTableName( "t2", triggerCols, permutation );

        StringBuffer   buffer = new StringBuffer();
        buffer.append( "update " + t2Name + " set " );
        for ( int i = 0; i < updateColumns.length; i++ )
        {
            if ( i > 0 ) { buffer.append( ", " ); }
            buffer.append( columnName( A_COL, updateColumns[ i ] ) );
            buffer.append( " = " );
            buffer.append( (100 + i) );
        }

        String  updateStatement = buffer.toString();

        try {
            Statement s = createStatement();
            s.execute(updateStatement);
        }
        catch (SQLException se)
        {
            fail
                (
                 statusCounters,
                 triggerCols,
                 permutation,
                 updateColumns,
                 "Update statement failed! updateStatement = '" + updateStatement
                 );
        }

        return updateStatement;
    }

    private void    fail( int[] statusCounters, int[] triggerCols, int[] permutation, int[] updateColumns, String detail )
    {
        statusCounters[ FAILURES ]++;
        
        String  message = "FAILED for triggerCols = " +
            stringify( triggerCols ) +
            " and permutation = " + stringify( permutation ) +
            " and updateColumns = " + stringify( updateColumns ) +
            ". " + detail;

        System.out.println( message );
    }
    
    private void    summarize( int[] statusCounters )
    {
        int testCount = statusCounters[ TEST_COUNT ];
        int failures = statusCounters[ FAILURES ];

        if ( failures != 0 )
        {
        	System.out.println( "FAILURE! " + testCount + " test cases run, of which " + failures + " failed." );
        }
    }

    private String    stringify( int[][] array )
    {
        StringBuffer   buffer = new StringBuffer();

        buffer.append( "[" );
        for ( int i = 0; i < array.length; i++ )
        {
            buffer.append( "\n\t" );
            buffer.append( stringify( array[ i ] ) );
        }
        buffer.append( "\n]\n" );

        return buffer.toString();
    }

    private String  stringify( int[] array )
    {
        StringBuffer   buffer = new StringBuffer();

        buffer.append( "[" );
        for ( int j = 0; j < array.length; j++ )
        {
            if ( j > 0 ) { buffer.append( ", " ); }
            buffer.append( array[ j ] );
        }
        buffer.append( "]" );

        return buffer.toString();
    }
    //End of helper methods for testExhuastivePermutationOfTriggerColumns

    /**
     * Test that triggers that use XML operators work after upgrade. The
     * first fix for DERBY-3870 broke upgrade of such triggers because the
     * old execution plans failed to deserialize on the new version.
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
                if (! oldLessThan(10,8,2,0)) {
                    s.executeUpdate("insert into D5289TABLE1(COL1) values ('aaa')");
                    s.executeUpdate("insert into D5289TABLE2(COL2) values ('aaa') ");
                    s.executeUpdate("UPDATE D5289TABLE1 SET COL1 = 'bbb'");
                    assertDERBY5289ResultsAndDelete();
                }
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
