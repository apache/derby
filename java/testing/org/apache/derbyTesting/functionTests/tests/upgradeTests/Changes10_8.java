/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.Changes10_8

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

import java.sql.Statement;
import java.sql.ResultSet;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.JDBC;


/**
 * Upgrade test cases for 10.8.
 * If the old version is 10.8 or later then these tests
 * will not be run.
 * <BR>
    10.8 Upgrade issues

    <UL>
    <LI>BOOLEAN data type support expanded.</LI>
    </UL>

 */
public class Changes10_8 extends UpgradeChange
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

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

    public Changes10_8(String name)
    {
        super(name);
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Return the suite of tests to test the changes made in 10.8.
     * @param phase an integer that indicates the current phase in
     *              the upgrade test.
     * @return the test suite created.
     */
    public static Test suite(int phase) {
        TestSuite suite = new TestSuite("Upgrade test for 10.8");

        suite.addTestSuite(Changes10_8.class);
        return new SupportFilesSetup((Test) suite);
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////
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
}
