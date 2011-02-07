/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.Changes10_7

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

import org.apache.derbyTesting.junit.SupportFilesSetup;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Set;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.JDBC;


/**
 * Upgrade test cases for 10.7.
 * If the old version is 10.7 or later then these tests
 * will not be run.
 * <BR>
    10.7 Upgrade issues

    <UL>
    <LI>BOOLEAN data type support expanded.</LI>
    </UL>

 */
public class Changes10_7 extends UpgradeChange
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static final String SYNTAX_ERROR = "42X01";
    private static final String  UPGRADE_REQUIRED = "XCL47";
    private static final String  GRANT_REVOKE_WITH_LEGACY_ACCESS = "42Z60";

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public Changes10_7(String name)
    {
        super(name);
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Return the suite of tests to test the changes made in 10.7.
     * @param phase an integer that indicates the current phase in
     *              the upgrade test.
     * @return the test suite created.
     */
    public static Test suite(int phase) {
        TestSuite suite = new TestSuite("Upgrade test for 10.7");

        suite.addTestSuite(Changes10_7.class);
        return new SupportFilesSetup((Test) suite);
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Make sure that that database is at level 10.7 in order to enjoy
     * extended support for the BOOLEAN datatype.
     */
    public void testBoolean() throws SQLException
    {
        String booleanValuedFunction =
            "create function f_4655( a varchar( 100 ) ) returns boolean\n" +
            "language java parameter style java no sql deterministic\n" +
            "external name 'Z.getBooleanValue'\n";

        Statement s = createStatement();

        switch ( getPhase() )
        {
        case PH_CREATE: // create with old version
        case PH_POST_SOFT_UPGRADE: // soft-downgrade: boot with old version after soft-upgrade
            assertFalse(getSupportedTypes().contains("BOOLEAN"));
            assertStatementError(  SYNTAX_ERROR, s, booleanValuedFunction );
            break;

        case PH_SOFT_UPGRADE: // boot with new version and soft-upgrade
            assertFalse(getSupportedTypes().contains("BOOLEAN"));
            assertStatementError( UPGRADE_REQUIRED, s, booleanValuedFunction );
            break;
            
        case PH_HARD_UPGRADE: // boot with new version and hard-upgrade
            assertTrue(getSupportedTypes().contains("BOOLEAN"));
            s.execute( booleanValuedFunction );
            break;
        }
        
        s.close();
    }

    /**
     * Get the names of all supported types, as reported by
     * {@code DatabaseMetaData.getTypeInfo()}.
     *
     * @return a set with the names of all supported types in the loaded
     * version of Derby
     */
    private Set getSupportedTypes() throws SQLException {
        HashSet types = new HashSet();
        ResultSet rs = getConnection().getMetaData().getTypeInfo();
        while (rs.next()) {
            types.add(rs.getString("TYPE_NAME"));
        }
        rs.close();
        return types;
    }
    
    /**
     * Make sure that DERBY-4998 changes do not break backward compatibility.
     * 
     * It creates triggers in old release with trigger action columns getting
     * used through the REFERENCING clause. Those triggers in soft upgrade
     * mode, post soft upgrade mode and hard upgrade mode do not get detected 
     * by ALTER TABLE DROP COLUMN because prior to 10.7, we did not keep 
     * information about trigger action columns getting used through the 
     * REFERENCING clause.
     */
    public void testAlterTableDropColumnAndTriggerAction() throws Exception
    {
    	if (!oldAtLeast(10, 3)) return;
    	
        Statement s = createStatement();
        ResultSet rs;

        switch ( getPhase() )
        {
        case PH_CREATE: // create with old version
        	//Create 2 tables for each of the upgrade phases which are 
        	//a)soft upgrade b)post soft upgrade and c)hard upgrade
        	//For each of the upgrade phase, one table will be used for 
        	//ALTER TABLE DROP COLUMN RESTRICT and the second table will
        	//be used for ALTER TABLE DROP COLUMN CASCADE

        	//Following 2 tables and triggers are for soft upgrade mode check
        	s.execute("CREATE TABLE DERBY4998_SOFT_UPGRADE_RESTRICT(c11 int, c12 int)");
            s.execute("INSERT INTO DERBY4998_SOFT_UPGRADE_RESTRICT VALUES (1,10)");
            //Create a trigger in the older release where the database has been
            //created. The trigger action uses a column in trigger action
            //through REFERENCING clause
            s.execute("CREATE TRIGGER DERBY4998_SOFT_UPGRADE_RESTRICT_tr1 " +
            		"AFTER UPDATE OF c12 " +
            		"ON DERBY4998_SOFT_UPGRADE_RESTRICT REFERENCING OLD AS oldt " +
            		"FOR EACH ROW " +
                    "SELECT oldt.c11 from DERBY4998_SOFT_UPGRADE_RESTRICT");
            s.executeUpdate("UPDATE DERBY4998_SOFT_UPGRADE_RESTRICT SET c12=c12+1");

            s.execute("CREATE TABLE DERBY4998_soft_upgrade_cascade(c11 int, c12 int)");
            s.execute("INSERT INTO DERBY4998_soft_upgrade_cascade VALUES (1,10)");
            //Create a trigger in the older release where the database has been
            //created. The trigger action uses a column in trigger action
            //through REFERENCING clause
            s.execute("CREATE TRIGGER DERBY4998_soft_upgrade_cascade_tr1 " +
            		"AFTER UPDATE OF c12 " +
            		"ON DERBY4998_soft_upgrade_cascade REFERENCING OLD AS oldt " +
            		"FOR EACH ROW " +
                    "SELECT oldt.c11 from DERBY4998_soft_upgrade_cascade");
            s.executeUpdate("UPDATE DERBY4998_soft_upgrade_cascade SET c12=c12+1");

        	//Following 2 tables and triggers are for post-soft upgrade mode
            //check
            s.execute("CREATE TABLE DERBY4998_postsoft_upgrade_restrict(c11 int, c12 int)");
            s.execute("INSERT INTO DERBY4998_postsoft_upgrade_restrict VALUES (1,10)");
            //Create a trigger in the older release where the database has been
            //created. The trigger action uses a column in trigger action
            //through REFERENCING clause
            s.execute("CREATE TRIGGER DERBY4998_postsoft_upgrade_restrict_tr1 " +
            		"AFTER UPDATE OF c12 " +
            		"ON DERBY4998_postsoft_upgrade_restrict REFERENCING OLD AS oldt " +
            		"FOR EACH ROW " +
                    "SELECT oldt.c11 from DERBY4998_postsoft_upgrade_restrict");
            s.executeUpdate("UPDATE DERBY4998_postsoft_upgrade_restrict SET c12=c12+1");

            s.execute("CREATE TABLE DERBY4998_postsoft_upgrade_cascade(c11 int, c12 int)");
            s.execute("INSERT INTO DERBY4998_postsoft_upgrade_cascade VALUES (1,10)");
            //Create a trigger in the older release where the database has been
            //created. The trigger action uses a column in trigger action
            //through REFERENCING clause
            s.execute("CREATE TRIGGER DERBY4998_postsoft_upgrade_cascade_tr1 " +
            		"AFTER UPDATE OF c12 " +
            		"ON DERBY4998_postsoft_upgrade_cascade REFERENCING OLD AS oldt " +
            		"FOR EACH ROW " +
                    "SELECT oldt.c11 from DERBY4998_postsoft_upgrade_cascade");
            s.executeUpdate("UPDATE DERBY4998_postsoft_upgrade_cascade SET c12=c12+1");

        	//Following 2 tables and triggers are for hard upgrade mode check
            s.execute("CREATE TABLE DERBY4998_hard_upgrade_restrict(c11 int, c12 int)");
            s.execute("INSERT INTO DERBY4998_hard_upgrade_restrict VALUES (1,10)");
            //Create a trigger in the older release where the database has been
            //created. The trigger action uses a column in trigger action
            //through REFERENCING clause
            s.execute("CREATE TRIGGER DERBY4998_hard_upgrade_restrict_tr1 " +
            		"AFTER UPDATE OF c12 " +
            		"ON DERBY4998_hard_upgrade_restrict REFERENCING OLD AS oldt " +
            		"FOR EACH ROW " +
                    "SELECT oldt.c11 from DERBY4998_hard_upgrade_restrict");
            s.executeUpdate("UPDATE DERBY4998_hard_upgrade_restrict SET c12=c12+1");

            s.execute("CREATE TABLE DERBY4998_hard_upgrade_cascade(c11 int, c12 int)");
            s.execute("INSERT INTO DERBY4998_hard_upgrade_cascade VALUES (1,10)");
            //Create a trigger in the older release where the database has been
            //created. The trigger action uses a column in trigger action
            //through REFERENCING clause
            s.execute("CREATE TRIGGER DERBY4998_hard_upgrade_cascade_tr1 " +
            		"AFTER UPDATE OF c12 " +
            		"ON DERBY4998_hard_upgrade_cascade REFERENCING OLD AS oldt " +
            		"FOR EACH ROW " +
                    "SELECT oldt.c11 from DERBY4998_hard_upgrade_cascade");
            s.executeUpdate("UPDATE DERBY4998_hard_upgrade_cascade SET c12=c12+1");
            break;

        case PH_SOFT_UPGRADE: // boot with new version and soft-upgrade
        	//The tables created with 10.6 and prior versions will exhibit
        	//incorrect behavoir
        	incorrectBehaviorForDropColumn("DERBY4998_SOFT_UPGRADE_RESTRICT",
        			"DERBY4998_SOFT_UPGRADE_RESTRICT_TR1", "RESTRICT");
        	incorrectBehaviorForDropColumn("DERBY4998_SOFT_UPGRADE_CASCADE",
        			"DERBY4998_SOFT_UPGRADE_CASCADE_TR1", "CASCADE");

        	//Even though we are in soft upgrade mode using Derby 10.7 release,
        	//newly created triggers will still not keep track of trigger
        	//action columns referenced through REFERENCING clause because
        	//that will break the backward compatibility when this db gets
        	//used with earlier Derby version again after soft upgrade.
        	//Show this with an example
            s.execute("CREATE TABLE DERBY4998_SOFT_UPGRADE_NEW_TABLE_RESTRICT(c11 int, c12 int)");
            s.execute("INSERT INTO DERBY4998_SOFT_UPGRADE_NEW_TABLE_RESTRICT VALUES (1,10)");
            //Create a trigger in the older release where the database has been
            //created. The trigger action uses a column in trigger action
            //through REFERENCING clause
            s.execute("CREATE TRIGGER DERBY4998_SOFT_UPGRADE_NEW_TABLE_RESTRICT_tr1 " +
            		"AFTER UPDATE OF c12 " +
            		"ON DERBY4998_SOFT_UPGRADE_NEW_TABLE_RESTRICT REFERENCING OLD AS oldt " +
            		"FOR EACH ROW " +
                    "SELECT oldt.c11 from DERBY4998_SOFT_UPGRADE_NEW_TABLE_RESTRICT");
            s.executeUpdate("UPDATE DERBY4998_SOFT_UPGRADE_NEW_TABLE_RESTRICT SET c12=c12+1");
        	incorrectBehaviorForDropColumn("DERBY4998_SOFT_UPGRADE_NEW_TABLE_RESTRICT",
        			"DERBY4998_SOFT_UPGRADE_NEW_TABLE_RESTRICT_TR1", "RESTRICT");
        	
            s.execute("CREATE TABLE DERBY4998_SOFT_UPGRADE_NEW_TABLE_CASCADE(c11 int, c12 int)");
            s.execute("INSERT INTO DERBY4998_SOFT_UPGRADE_NEW_TABLE_CASCADE VALUES (1,10)");
            //Create a trigger in the older release where the database has been
            //created. The trigger action uses a column in trigger action
            //through REFERENCING clause
            s.execute("CREATE TRIGGER DERBY4998_SOFT_UPGRADE_NEW_TABLE_CASCADE_TR1 " +
            		"AFTER UPDATE OF c12 " +
            		"ON DERBY4998_SOFT_UPGRADE_NEW_TABLE_CASCADE REFERENCING OLD AS oldt " +
            		"FOR EACH ROW " +
                    "SELECT oldt.c11 from DERBY4998_SOFT_UPGRADE_NEW_TABLE_CASCADE");
            s.executeUpdate("UPDATE DERBY4998_SOFT_UPGRADE_NEW_TABLE_CASCADE SET c12=c12+1");
        	incorrectBehaviorForDropColumn("DERBY4998_SOFT_UPGRADE_NEW_TABLE_CASCADE",
        			"DERBY4998_SOFT_UPGRADE_NEW_TABLE_CASCADE_TR1", "RESTRICT");
            break;

        case PH_POST_SOFT_UPGRADE: 
        	// soft-downgrade: boot with old version after soft-upgrade

        	//The tables created with 10.6 and prior versions will continue to 
        	//exhibit incorrect behavoir
        	incorrectBehaviorForDropColumn("DERBY4998_POSTSOFT_UPGRADE_RESTRICT",
        			"DERBY4998_POSTSOFT_UPGRADE_RESTRICT_TR1", "RESTRICT");
        	incorrectBehaviorForDropColumn("DERBY4998_POSTSOFT_UPGRADE_CASCADE",
        			"DERBY4998_POSTSOFT_UPGRADE_CASCADE_TR1", "CASCADE");
        	
        	//We are back to pre-10.7 version after the soft upgrade. 
        	//ALTER TABLE DROP COLUMN will continue to behave incorrectly
        	//and will not detect the trigger actions referencing the column
        	//being dropped through the REFERENCING clause
            s.execute("CREATE TABLE DERBY4998_POST_SOFT_UPGRADE_NEW_TABLE_RESTRICT(c11 int, c12 int)");
            s.execute("INSERT INTO DERBY4998_POST_SOFT_UPGRADE_NEW_TABLE_RESTRICT VALUES (1,10)");
            //Create a trigger in the older release where the database has been
            //created. The trigger action uses a column in trigger action
            //through REFERENCING clause
            s.execute("CREATE TRIGGER DERBY4998_POST_SOFT_UPGRADE_NEW_TABLE_RESTRICT_TR1 " +
            		"AFTER UPDATE OF c12 " +
            		"ON DERBY4998_POST_SOFT_UPGRADE_NEW_TABLE_RESTRICT REFERENCING OLD AS oldt " +
            		"FOR EACH ROW " +
                    "SELECT oldt.c11 from DERBY4998_POST_SOFT_UPGRADE_NEW_TABLE_RESTRICT");
            s.executeUpdate("UPDATE DERBY4998_POST_SOFT_UPGRADE_NEW_TABLE_RESTRICT SET c12=c12+1");
        	incorrectBehaviorForDropColumn("DERBY4998_POST_SOFT_UPGRADE_NEW_TABLE_RESTRICT",
        			"DERBY4998_POST_SOFT_UPGRADE_NEW_TABLE_RESTRICT_TR1", "RESTRICT");
        	
            s.execute("CREATE TABLE DERBY4998_POST_SOFT_UPGRADE_NEW_TABLE_CASCADE(c11 int, c12 int)");
            s.execute("INSERT INTO DERBY4998_POST_SOFT_UPGRADE_NEW_TABLE_CASCADE VALUES (1,10)");
            //Create a trigger in the older release where the database has been
            //created. The trigger action uses a column in trigger action
            //through REFERENCING clause
            s.execute("CREATE TRIGGER DERBY4998_POST_SOFT_UPGRADE_NEW_TABLE_CASCADE_TR1 " +
            		"AFTER UPDATE OF c12 " +
            		"ON DERBY4998_POST_SOFT_UPGRADE_NEW_TABLE_CASCADE REFERENCING OLD AS oldt " +
            		"FOR EACH ROW " +
                    "SELECT oldt.c11 from DERBY4998_POST_SOFT_UPGRADE_NEW_TABLE_CASCADE");
            s.executeUpdate("UPDATE DERBY4998_POST_SOFT_UPGRADE_NEW_TABLE_CASCADE SET c12=c12+1");
        	incorrectBehaviorForDropColumn("DERBY4998_POST_SOFT_UPGRADE_NEW_TABLE_CASCADE",
        			"DERBY4998_POST_SOFT_UPGRADE_NEW_TABLE_CASCADE_TR1", "RESTRICT");
            break;

        case PH_HARD_UPGRADE: // boot with new version and hard-upgrade
        	//The tables created with 10.6 and prior versions will exhibit
        	//incorrect behavior. Even though the database is at 10.7 level,
        	//the triggers created with prior Derby releases did not keep
        	//track of trigger action columns referenced through REFERENCING
        	//clause.
        	incorrectBehaviorForDropColumn("DERBY4998_HARD_UPGRADE_RESTRICT",
        			"DERBY4998_HARD_UPGRADE_RESTRICT_TR1", "RESTRICT");
        	incorrectBehaviorForDropColumn("DERBY4998_HARD_UPGRADE_CASCADE",
        			"DERBY4998_HARD_UPGRADE_CASCADE_TR1", "CASCADE");
        	
        	//Create 2 new tables now that the database has been upgraded to 
        	//10.7 Notice that newly created tables will be able to detect
        	//trigger action reference to column through REFERENCING clause.
        	//This is because starting 10.7, for new triggers, we have
        	//started keeping track of trigger action columns referenced 
        	//through REFERENCING clause.
            s.execute("CREATE TABLE DERBY4998_HARD_UPGRADE_NEW_TABLE_RESTRICT(c11 int, c12 int)");
            s.execute("INSERT INTO DERBY4998_HARD_UPGRADE_NEW_TABLE_RESTRICT VALUES (1,10)");
            //Create a trigger in the older release where the database has been
            //created. The trigger action uses a column in trigger action
            //through REFERENCING clause
            s.execute("CREATE TRIGGER DERBY4998_HARD_UPGRADE_NEW_TABLE_RESTRICT_tr1 " +
            		"AFTER UPDATE OF c12 " +
            		"ON DERBY4998_HARD_UPGRADE_NEW_TABLE_RESTRICT REFERENCING OLD AS oldt " +
            		"FOR EACH ROW " +
                    "SELECT oldt.c11 from DERBY4998_HARD_UPGRADE_NEW_TABLE_RESTRICT");
            s.executeUpdate("UPDATE DERBY4998_HARD_UPGRADE_NEW_TABLE_RESTRICT SET c12=c12+1");
            assertStatementError("X0Y25", s,
            		" alter table DERBY4998_HARD_UPGRADE_NEW_TABLE_RESTRICT " +
            		" drop column c11 restrict");
            rs = s.executeQuery(
            " select triggername from sys.systriggers where " +
            "triggername='DERBY4998_HARD_UPGRADE_NEW_TABLE_RESTRICT_TR1'");
            JDBC.assertFullResultSet(rs, 
            		new String[][]{{"DERBY4998_HARD_UPGRADE_NEW_TABLE_RESTRICT_TR1"}});
            s.execute("CREATE TABLE DERBY4998_HARD_UPGRADE_NEW_TABLE_CASCADE(c11 int, c12 int)");
            s.execute("INSERT INTO DERBY4998_HARD_UPGRADE_NEW_TABLE_CASCADE VALUES (1,10)");
            //Create a trigger in the older release where the database has been
            //created. The trigger action uses a column in trigger action
            //through REFERENCING clause
            s.execute("CREATE TRIGGER DERBY4998_HARD_UPGRADE_NEW_TABLE_CASCADE_TR1 " +
            		"AFTER UPDATE OF c12 " +
            		"ON DERBY4998_HARD_UPGRADE_NEW_TABLE_CASCADE REFERENCING OLD AS oldt " +
            		"FOR EACH ROW " +
                    "SELECT oldt.c11 from DERBY4998_HARD_UPGRADE_NEW_TABLE_CASCADE");
            s.executeUpdate("UPDATE DERBY4998_HARD_UPGRADE_NEW_TABLE_CASCADE SET c12=c12+1");
            s.executeUpdate("alter table DERBY4998_HARD_UPGRADE_NEW_TABLE_CASCADE " +
            		" drop column c11 CASCADE");
            checkWarning(s, "01502");
            JDBC.assertEmpty(s.executeQuery(
                    " select triggername from sys.systriggers where " +
                    "triggername='DERBY4998_HARD_UPGRADE_NEW_TABLE_CASCADE_TR1'"));
            break;
        }
    }

    //ALTER TABLE DROP COLUMN in not detected the trigger column dependency for
    //columns being used through the REFERENCING clause for triggers created
    //prior to 10.7 release
    private void incorrectBehaviorForDropColumn(String tableName,
    		String triggerName, String restrictOrCascade) throws SQLException {
        Statement s = createStatement();
        ResultSet rs;
        
        //ALTER TABLE DROP COLUMN of a column used in the trigger action
        //through REFERENCING clause does not detect the trigger 
        //dependency in older releases.
        //RESTRICT won't give any error for dependent trigger and will
        //drop column c11 even though it is getting used in trigger action
        //and will leave the invalid trigger in the system. 
        //CASCADE won't give any warning for dependent trigger and will
        //drop column c11 even though it is getting used in trigger action
        //and will leave the invalid trigger in the system. 
        s.executeUpdate("ALTER TABLE " + tableName + " DROP COLUMN c11 " +
        		restrictOrCascade);
        rs =
            s.executeQuery(
            " select triggername from sys.systriggers where " +
            "triggername='" + triggerName + "'");
        JDBC.assertFullResultSet(rs, new String[][]{{triggerName}});
    }

    private void checkWarning(Statement st, String expectedWarning)
            throws Exception {
        SQLWarning sqlWarn = null;

        sqlWarn = st.getWarnings();
        if (sqlWarn == null) {
            sqlWarn = getConnection().getWarnings();
        }
        assertNotNull("Expected warning but found none", sqlWarn);
        assertSQLState(expectedWarning, sqlWarn);
    }

    /**
     * Make sure that DERBY-1482 changes do not break backward compatibility
     */
    public void testTriggers() throws SQLException
    {
        Statement s = createStatement();
        ResultSet rs;
        boolean modeDb2SqlOptional = oldAtLeast(10, 3);

        switch ( getPhase() )
        {
        case PH_CREATE: // create with old version
            s.execute("CREATE TABLE DERBY1482_table1(c11 int, c12 int)");
            s.execute("INSERT INTO DERBY1482_table1 VALUES (1,10)");
            s.execute("CREATE TABLE DERBY1482_table2(c21 int, c22 int)");
            s.execute("CREATE TABLE DERBY1482_table3(c31 int, c32 int)");
            s.execute("CREATE TABLE DERBY1482_table4(c41 int, c42 int)");
            s.execute("CREATE TABLE DERBY1482_table5(c51 int, c52 int)");
            //Create the first trigger in the older release where the
            //database has been created. Every update of DERBY1482_table1.c12
            //will cause an insert into DERBY1482_table2 through this trigger tr1.
            s.execute("CREATE TRIGGER tr1 AFTER UPDATE OF c12 " +
            		"ON DERBY1482_table1 REFERENCING OLD AS oldt " +
            		"FOR EACH ROW " +
                    (modeDb2SqlOptional?"":"MODE DB2SQL ") +
                    "INSERT INTO DERBY1482_table2 VALUES(-1, oldt.c12)");
            
            //Now do an update which will fire trigger tr1
            s.executeUpdate("UPDATE DERBY1482_table1 SET c12=-1 WHERE c11=1");
            //Verify that trigger tr1 has inserted one row in DERBY1482_table2
            rs = s.executeQuery("SELECT COUNT(*) FROM DERBY1482_table2");
            JDBC.assertFullResultSet(rs, new String[][]{{"1"}});
            break;

        case PH_SOFT_UPGRADE: // boot with new version and soft-upgrade
            //Now do an update while in the soft upgrade. This should
        	//fire trigger tr1
            s.executeUpdate("UPDATE DERBY1482_table1 SET c12=-1 WHERE c11=1");
            //Verify that now we have 2 rows in DERBY1482_table2 because trigger tr1
            //has fired twice so far. Once in PH_CREATE phase and once
            //in PH_SOFT_UPGRADE phase
            rs = s.executeQuery("SELECT COUNT(*) FROM DERBY1482_table2");
            JDBC.assertFullResultSet(rs, new String[][]{{"2"}});
            //Create trigger tr2 in soft upgrade mode. DERBY-1482 changes
            //will not put anything about trigger action columns in 
            //SYSTRIGGERS to maintain backward compatibility. Only 10.7
            //and up recognize additional information about trigger action
            //columns in SYSTRIGGERS.
            s.execute("CREATE TRIGGER tr2 AFTER UPDATE OF c12 ON DERBY1482_table1 " +
            		"REFERENCING OLD AS oldt FOR EACH ROW " +
                    (modeDb2SqlOptional?"":"MODE DB2SQL ") +
            		"INSERT INTO DERBY1482_table3 VALUES(-1, oldt.c12)");
            //Now do an update which will fire triggers tr1 and tr2
            s.executeUpdate("UPDATE DERBY1482_table1 SET c12=-1 WHERE c11=1");
            //Verify that trigger tr1 has inserted one more row in DERBY1482_table2
            rs = s.executeQuery("SELECT COUNT(*) FROM DERBY1482_table2");
            JDBC.assertFullResultSet(rs, new String[][]{{"3"}});
            //Verify that trigger tr2 has inserted one row in DERBY1482_table3
            rs = s.executeQuery("SELECT COUNT(*) FROM DERBY1482_table3");
            JDBC.assertFullResultSet(rs, new String[][]{{"1"}});
            break;

        case PH_POST_SOFT_UPGRADE: // soft-downgrade: boot with old version after soft-upgrade
            //Now do an update when we are back with the older release
        	//after the soft upgrade. This should fire trigger tr1 and tr2
            s.executeUpdate("UPDATE DERBY1482_table1 SET c12=-1 WHERE c11=1");
            //Verify that now we have 4 rows in DERBY1482_table2 and 2 rows in DERBY1482_table3
            rs = s.executeQuery("SELECT COUNT(*) FROM DERBY1482_table2");
            JDBC.assertFullResultSet(rs, new String[][]{{"4"}});
            rs = s.executeQuery("SELECT COUNT(*) FROM DERBY1482_table3");
            JDBC.assertFullResultSet(rs, new String[][]{{"2"}});
            //Create trigger tr3 with the older release. Triggers created in
            //soft-upgrade mode and with older release should work fine.
            s.execute("CREATE TRIGGER tr3 AFTER UPDATE OF c12 ON DERBY1482_table1 " +
            		"REFERENCING OLD AS oldt FOR EACH ROW " +
                    (modeDb2SqlOptional?"":"MODE DB2SQL ") +
            		"INSERT INTO DERBY1482_table4 VALUES(-1, oldt.c12)");
            //Now do an update which will fire triggers tr1, tr2 and tr3
            s.executeUpdate("UPDATE DERBY1482_table1 SET c12=-1 WHERE c11=1");
            //Verify that trigger tr1 has inserted one more row in DERBY1482_table2
            rs = s.executeQuery("SELECT COUNT(*) FROM DERBY1482_table2");
            JDBC.assertFullResultSet(rs, new String[][]{{"5"}});
            //Verify that trigger tr2 has inserted one more row in DERBY1482_table3
            rs = s.executeQuery("SELECT COUNT(*) FROM DERBY1482_table3");
            JDBC.assertFullResultSet(rs, new String[][]{{"3"}});
            //Verify that trigger tr3 has inserted one row in DERBY1482_table4
            rs = s.executeQuery("SELECT COUNT(*) FROM DERBY1482_table4");
            JDBC.assertFullResultSet(rs, new String[][]{{"1"}});
            break;

        case PH_HARD_UPGRADE: // boot with new version and hard-upgrade
        	//Do an update after we have hard upgraded to 10.7 and make sure
        	//that all the triggers (created with older release and created
        	//in soft-upgrade mode) work fine.
            s.executeUpdate("UPDATE DERBY1482_table1 SET c12=-1 WHERE c11=1");
            //Verify that now we have 6 rows in DERBY1482_table2, 4 rows in DERBY1482_table3, 2 rows in DERBY1482_table4
            rs = s.executeQuery("SELECT COUNT(*) FROM DERBY1482_table2");
            JDBC.assertFullResultSet(rs, new String[][]{{"6"}});
            rs = s.executeQuery("SELECT COUNT(*) FROM DERBY1482_table3");
            JDBC.assertFullResultSet(rs, new String[][]{{"4"}});
            rs = s.executeQuery("SELECT COUNT(*) FROM DERBY1482_table4");
            JDBC.assertFullResultSet(rs, new String[][]{{"2"}});
            //Create trigger DERBY1482_table4 in the hard-upgraded db.
            s.execute("CREATE TRIGGER tr4 AFTER UPDATE OF c12 ON DERBY1482_table1 " +
            		"REFERENCING OLD AS oldt FOR EACH ROW " +
                    (modeDb2SqlOptional?"":"MODE DB2SQL ") +
            		"INSERT INTO DERBY1482_table5 VALUES(-1, oldt.c12)");
            //All 4 triggers tr1, tr2, tr3 and tr4 should fire 
            //Now do an update which will fire all 4 triggers tr1,tr2,tr3,tr4
            s.executeUpdate("UPDATE DERBY1482_table1 SET c12=-1 WHERE c11=1");
            //Verify that trigger tr1 has inserted one more row in DERBY1482_table2
            rs = s.executeQuery("SELECT COUNT(*) FROM DERBY1482_table2");
            JDBC.assertFullResultSet(rs, new String[][]{{"7"}});
            //Verify that trigger tr2 has inserted one more row in DERBY1482_table3
            rs = s.executeQuery("SELECT COUNT(*) FROM DERBY1482_table3");
            JDBC.assertFullResultSet(rs, new String[][]{{"5"}});
            //Verify that trigger tr3 has inserted one more row in DERBY1482_table4
            rs = s.executeQuery("SELECT COUNT(*) FROM DERBY1482_table4");
            JDBC.assertFullResultSet(rs, new String[][]{{"3"}});
            //Verify that trigger tr4 has inserted one row in DERBY1482_table5
            rs = s.executeQuery("SELECT COUNT(*) FROM DERBY1482_table5");
            JDBC.assertFullResultSet(rs, new String[][]{{"1"}});
            break;
        }
        s.close();
    }

    /**
     * Make sure that that database is at level 10.7 in order to enjoy
     * routines with specified EXTERNAL SECURITY INVOKER or DEFINER.
     */
    public void testExternalSecuritySpecification() throws SQLException
    {
        String functionWithDefinersRights =
            "create function f_4551( a varchar( 100 ) ) returns int\n" +
            "language java parameter style java reads sql data\n" +
            "external security definer\n" +
            "external name 'Z.getIntValue'\n";

        Statement s = createStatement();

        switch ( getPhase() )
        {
        case PH_CREATE: // create with old version
        case PH_POST_SOFT_UPGRADE:
            // soft-downgrade: boot with old version after soft-upgrade
            assertStatementError(
                SYNTAX_ERROR, s, functionWithDefinersRights );
            break;

        case PH_SOFT_UPGRADE: // boot with new version and soft-upgrade
            assertStatementError(
                UPGRADE_REQUIRED, s, functionWithDefinersRights );
            break;

        case PH_HARD_UPGRADE: // boot with new version and hard-upgrade.
            // Syntax now accepted and dictionary level ok, but
            // sqlAuthorization not enabled (a priori) - expected.
            assertStatementError(GRANT_REVOKE_WITH_LEGACY_ACCESS,
                                 s, functionWithDefinersRights );
            break;
        }

        s.close();
    }

}
