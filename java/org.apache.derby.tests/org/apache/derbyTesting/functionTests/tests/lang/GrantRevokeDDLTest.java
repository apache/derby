/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.GrantRevokeDDLTest

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;


public final class GrantRevokeDDLTest extends BaseJDBCTestCase {

    private static  final   String      TEST_DBO = "TEST_DBO";
    private static  final   String      RUTH = "RUTH";

	private static String[] users = { "TEST_DBO", "george", "sam", 
			"monica", "swiper", "sam", "satheesh", "bar",
			"mamta4", "mamta3", "mamta2", "mamta1", "sammy",
            "user5", "user4", "user3", "user2", "user1", RUTH
	};

    public static  final   String  NO_GENERIC_PERMISSION = "42504";
    public static  final   String  NO_SELECT_OR_UPDATE_PERMISSION = "42502";
    public static  final   String  NO_TABLE_PERMISSION = "42500";
	
    /**
     * Public constructor required for running test as standalone JUnit.
     */
    public GrantRevokeDDLTest(String name)
    {
        super(name);
    }

    public static Test suite()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        if (JDBC.vmSupportsJSR169()) {
             // return empty suite;
            return new BaseTestSuite("GrantRevokeDDLTest");
        }

        //test uses triggers and procedures that use DriverManager.
        BaseTestSuite suite = new BaseTestSuite(
            GrantRevokeDDLTest.class, "GrantRevokeDDL Test");

	    Test test = new SupportFilesSetup(suite);
	    test = new CleanDatabaseTestSetup(test);
	    test = DatabasePropertyTestSetup.builtinAuthentication(
				test, users, "grantrevokeddl");
        test = TestConfiguration.sqlAuthorizationDecorator(test);
        
        return test;
    }
    
    public void testGrantRevokeDDL() throws Exception
    {
        ResultSet rs = null;
        SQLWarning sqlWarn = null;

        CallableStatement cSt;
        Statement st = createStatement();

        String [][] expRS;
        String [] expColNames;
        
        // Invalidating all the stored statements by dbo will work
//IC see: https://issues.apache.org/jira/browse/DERBY-5578
        cSt = prepareCall(
            "call SYSCS_UTIL.SYSCS_INVALIDATE_STORED_STATEMENTS()");
        assertUpdateCount(cSt, 0);
        cSt.close();
        
        Connection satConnection = openUserConnection("satheesh");
        Statement st_satConnection = satConnection.createStatement();
        
        // Try invalidating all the stored statements by user other than the
        //  dbo. That will fail because dbo has not granted execute permission
        //  to any one yet on 
        //  SYSCS_UTIL.SYSCS_INVALIDATE_STORED_STATEMENTS
        cSt = satConnection.prepareCall(
            "call SYSCS_UTIL.SYSCS_INVALIDATE_STORED_STATEMENTS()");
        assertStatementError("42504", cSt);
        cSt.close();

        //Have dbo grant execute permission on 
        // SYSCS_UTIL.SYSCS_INVALIDATE_STORED_STATEMENTS()
        // to one user
        st.executeUpdate(
                " grant execute on procedure "
                + "SYSCS_UTIL.SYSCS_INVALIDATE_STORED_STATEMENTS to satheesh");
        cSt = satConnection.prepareCall(
                "call SYSCS_UTIL.SYSCS_INVALIDATE_STORED_STATEMENTS()");
        assertUpdateCount(cSt, 0);
        cSt.close();
        //Have the dbo revoke the execute privilege on 
        // SYSCS_UTIL.SYSCS_INVALIDATE_STORED_STATEMENTS()
        st.executeUpdate(
                "revoke execute on procedure " +
                "SYSCS_UTIL.SYSCS_INVALIDATE_STORED_STATEMENTS " +
                "from satheesh restrict");
        cSt = satConnection.prepareCall(
                "call SYSCS_UTIL.SYSCS_INVALIDATE_STORED_STATEMENTS()");
        assertStatementError("42504", cSt);
        cSt.close();
        
        // Test table privileges
        
        st = createStatement();
        
        st.executeUpdate("create schema authorization satheesh");
        
        st_satConnection.executeUpdate(
            "create table satheesh.tsat(i int not null primary "
//IC see: https://issues.apache.org/jira/browse/DERBY-6429
            + "key, j int, noselect int)");
        
        st_satConnection.executeUpdate(
            " create index tsat_ind on satheesh.tsat(j)");
        
        st_satConnection.executeUpdate(
            " create table satheesh.table1 (a int, b int, c char(10))");
        
        st_satConnection.executeUpdate(
            " grant select on satheesh.tsat to public");
        
        st_satConnection.executeUpdate(
            " grant insert on satheesh.tsat to foo");
        
        st_satConnection.executeUpdate(
            " grant delete on satheesh.tsat to foo");
        
        st_satConnection.executeUpdate(
            " grant update on satheesh.tsat to foo");
        
        st_satConnection.executeUpdate(
            " grant update(i) on satheesh.tsat to bar");
        
        rs = st_satConnection.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select GRANTEE, GRANTOR, SELECTPRIV, DELETEPRIV, INSERTPRIV, UPDATEPRIV, REFERENCESPRIV, TRIGGERPRIV from sys.systableperms ORDER BY GRANTEE, GRANTOR");
        
        expColNames = new String [] {"GRANTEE", "GRANTOR", "SELECTPRIV", "DELETEPRIV", "INSERTPRIV", "UPDATEPRIV", "REFERENCESPRIV", "TRIGGERPRIV"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"FOO", "SATHEESH", "N", "y", "y", "y", "N", "N"},
            {"PUBLIC", "SATHEESH", "y", "N", "N", "N", "N", "N"},
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        Connection barConnection = openUserConnection("bar");
        Statement st_barConnection = barConnection.createStatement();
  
        // Following revokes should fail. Only owner can revoke 
        // permissions
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42506", st_barConnection,
            "revoke select on satheesh.tsat from public");
        
        assertStatementError("42506", st_barConnection,
            " revoke insert on satheesh.tsat from foo");
        
        assertStatementError("42506", st_barConnection,
            " revoke update(i) on satheesh.tsat from foo");
        
        assertStatementError("42506", st_barConnection,
            " revoke update on satheesh.tsat from foo");
        
        assertStatementError("42506", st_barConnection,
            " revoke delete on satheesh.tsat from foo");
        
        // set connection satConnection
        
        // Revoke table permissions not granted already. This 
        // should raise warnings.
        
        st_satConnection.executeUpdate(
            "revoke trigger on satheesh.tsat from foo");
        
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_satConnection != null))
                sqlWarn = st_satConnection.getWarnings();
            if (sqlWarn == null)
                sqlWarn = satConnection.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01006", sqlWarn);
            sqlWarn = null;
        }
        
        st_satConnection.executeUpdate(
            " revoke references on satheesh.tsat from foo");
        
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_satConnection != null))
                sqlWarn = st_satConnection.getWarnings();
            if (sqlWarn == null)
                sqlWarn = satConnection.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01006", sqlWarn);
            sqlWarn = null;
        }
        
        // This should raise warnings for bar
        
        st_satConnection.executeUpdate(
            "revoke insert on satheesh.tsat from foo, bar");
        
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_satConnection != null))
                sqlWarn = st_satConnection.getWarnings();
            if (sqlWarn == null)
                sqlWarn = satConnection.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01006", sqlWarn);
            sqlWarn = null;
        }
        
        // This should raise warnings for both foo and bar
        
        st_satConnection.executeUpdate(
            "revoke insert on satheesh.tsat from foo, bar");
        
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_satConnection != null))
                sqlWarn = st_satConnection.getWarnings();
            if (sqlWarn == null)
                sqlWarn = satConnection.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01006", sqlWarn);
            sqlWarn = null;
        }
        
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_satConnection != null))
                sqlWarn = st_satConnection.getWarnings();
            if (sqlWarn == null)
                sqlWarn = satConnection.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01006", sqlWarn);
            sqlWarn = null;
        }
        
        st_satConnection.executeUpdate(
            " grant insert on satheesh.tsat to foo");
        
        // Following revokes should revoke permissions
        
        st_satConnection.executeUpdate(
            "revoke update on satheesh.tsat from foo");
        
        st_satConnection.executeUpdate(
            " revoke delete on satheesh.tsat from foo");
        
        // Check success by looking at systableperms directly for now
        
        rs = st_satConnection.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            "select GRANTEE, GRANTOR, SELECTPRIV, DELETEPRIV, INSERTPRIV, UPDATEPRIV, REFERENCESPRIV, TRIGGERPRIV from sys.systableperms order by GRANTEE, GRANTOR");
        
        expColNames = new String [] {"GRANTEE", "GRANTOR", "SELECTPRIV", "DELETEPRIV", "INSERTPRIV", "UPDATEPRIV", "REFERENCESPRIV", "TRIGGERPRIV"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            { "FOO", "SATHEESH", "N", "N", "y", "N", "N", "N"},
            { "PUBLIC", "SATHEESH", "y", "N", "N", "N", "N", "N"},
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st_satConnection.executeUpdate(
            " revoke insert on satheesh.tsat from foo");
        
        st_satConnection.executeUpdate(
            " revoke select on satheesh.tsat from public");
        
        // Check success by looking at systableperms directly for now
        
        rs = st_satConnection.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            "select GRANTEE, GRANTOR, SELECTPRIV, DELETEPRIV, INSERTPRIV, UPDATEPRIV, REFERENCESPRIV, TRIGGERPRIV from sys.systableperms ORDER BY GRANTEE, GRANTOR");
        
        expColNames = new String [] {"GRANTEE", "GRANTOR", "SELECTPRIV", "DELETEPRIV", "INSERTPRIV", "UPDATEPRIV", "REFERENCESPRIV", "TRIGGERPRIV"};
        JDBC.assertColumnNames(rs, expColNames);
        
        JDBC.assertEmpty(rs);
        
        // Test routine permissions
        
        st_satConnection.executeUpdate(
            "CREATE FUNCTION F_ABS(P1 INT) RETURNS INT NO "
            + "SQL RETURNS NULL ON NULL INPUT EXTERNAL NAME "
            + "'java.lang.Math.abs' LANGUAGE JAVA PARAMETER STYLE JAVA");
        
        // Revoke routine permission not granted already. This 
        // should raise a warning.
        
        st_satConnection.executeUpdate(
            "revoke execute on function F_ABS(int) from bar RESTRICT");
        
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_satConnection != null))
                sqlWarn = st_satConnection.getWarnings();
            if (sqlWarn == null)
                sqlWarn = satConnection.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01006", sqlWarn);
            sqlWarn = null;
        }
        
        st_satConnection.executeUpdate(
            " grant execute on function F_ABS to foo");
        
        st_satConnection.executeUpdate(
            " grant execute on function F_ABS(int) to bar");
        
        st_satConnection.executeUpdate(
            " revoke execute on function F_ABS(int) from bar RESTRICT");
        
        st_satConnection.executeUpdate(
            " drop function f_abs");
        
        // DERBY-4714
        
        st_satConnection.executeUpdate(
            "CREATE FUNCTION F_4714(P1 BOOLEAN) RETURNS BOOLEAN NO "
            + "SQL RETURNS NULL ON NULL INPUT EXTERNAL NAME "
            + "'org.apache.derbyTesting.functionTests.tests.lang.BooleanValuesTest.booleanValue' LANGUAGE JAVA PARAMETER STYLE JAVA");

        st_satConnection.executeUpdate(
            " grant execute on function F_4714(boolean) to bar");
        st_satConnection.executeUpdate(
            " revoke execute on function F_4714(boolean) from bar restrict");

        st_satConnection.executeUpdate(
            " drop function F_4714");
        
        // Tests with views
        
        st_satConnection.executeUpdate(
            "create view v1 as select * from tsat");
        
        st_satConnection.executeUpdate(
            " grant select on v1 to bar");
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42509", st_satConnection,
            " grant insert on v1 to foo");
        
        assertStatementError("42509", st_satConnection,
            " grant update on v1 to public");
        
        // Tests for synonym. Not supported currently.
        
        st_satConnection.executeUpdate(
            "create synonym mySym for satheesh.tsat");
        
        // Expected to fail
        
        assertStatementError("42X05", st_satConnection,
            "grant select on mySym to bar");
        
        assertStatementError("42X05", st_satConnection,
            " grant insert on mySym to foo");
        
        st_satConnection.executeUpdate(
            " CREATE FUNCTION F_ABS(P1 INT) RETURNS INT NO "
            + "SQL RETURNS NULL ON NULL INPUT EXTERNAL NAME "
            + "'java.lang.Math.abs' LANGUAGE JAVA PARAMETER STYLE JAVA");
        
        rs = st_satConnection.executeQuery(
            " values f_abs(-5)");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"5"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Test for AUTHORIZATION option for create schema 
        // GrantRevoke TODO: Need to enforce who can create which 
        // schema. More negative test cases need to be added once 
        // enforcing is done.
        
        getConnection().createStatement().executeUpdate(
            "CREATE SCHEMA MYDODO AUTHORIZATION DODO");
        
        getConnection().createStatement().executeUpdate(
            " CREATE SCHEMA AUTHORIZATION DERBY");
        
        rs = st_satConnection.executeQuery(
            " select SCHEMANAME, AUTHORIZATIONID from sys.sysschemas where schemaname not "
//IC see: https://issues.apache.org/jira/browse/DERBY-6446
            + "like 'SYS%' and schemaname not like 'TEST_DBO%' ORDER BY SCHEMANAME");
        
        expColNames = new String [] {"SCHEMANAME", "AUTHORIZATIONID"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"APP", "APP"},
            {"DERBY", "DERBY"},
            {"MYDODO", "DODO"},
            {"NULLID", "TEST_DBO"},
            {"SATHEESH", "SATHEESH"},
            {"SQLJ", "TEST_DBO"},
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Now connect as different user and try to do DDLs in 
        // schema owned by satheesh
        
        Connection swiperConnection = openUserConnection("swiper");
        Statement st_swiperConnection = swiperConnection.createStatement();
        
        st_swiperConnection.executeUpdate(
            " create table swiperTab (i int, j int)");
        
        st_swiperConnection.executeUpdate(
            " insert into swiperTab values (1,1)");
        
        st_swiperConnection.executeUpdate(
            " set schema satheesh");
        
        // All these DDLs should fail.
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42507", st_swiperConnection,
            "create table NotMyTable (i int, j int)");
        
        assertStatementError("42507", st_swiperConnection,
            " drop table tsat");
        
        assertStatementError("42507", st_swiperConnection,
            " drop index tsat_ind");
        
        assertStatementError("42507", st_swiperConnection,
            " create view myview as select * from satheesh.tsat");
        
        assertStatementError("42507", st_swiperConnection,
            " CREATE FUNCTION FuncNotMySchema(P1 INT) RETURNS INT "
            + "NO SQL RETURNS NULL ON NULL INPUT EXTERNAL NAME "
            + "'java.lang.Math.abs' LANGUAGE JAVA PARAMETER STYLE JAVA");
        
        assertStatementError("42507", st_swiperConnection,
            " alter table tsat add column k int");
        
        st_swiperConnection.executeUpdate(
            " create table swiper.mytab ( i int, j int)");
        
        st_swiperConnection.executeUpdate(
            " set schema swiper");
        
        // Some simple DML tests. Should all fail.
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_swiperConnection,
            "select * from satheesh.tsat");
        
        assertStatementError("42500", st_swiperConnection,
//IC see: https://issues.apache.org/jira/browse/DERBY-6429
            " insert into satheesh.tsat(i, j) values (1, 2)");
        
        assertStatementError("42502", st_swiperConnection,
            " update satheesh.tsat set i=j");
        
        assertStatementError("42502", st_swiperConnection,
            " create table my_tsat (i int not null, c char(10), "
            + "constraint fk foreign key(i) references satheesh.tsat)");
        
        // set connection satConnection
        //ij(SWIPERCONNECTION)> -- Now grant some permissions to 
        // swiper
        
        
        st_satConnection.executeUpdate(
//IC see: https://issues.apache.org/jira/browse/DERBY-6429
            " grant select(i, j), update(j) on tsat to swiper");
        
        st_satConnection.executeUpdate(
            " grant all privileges on table1 to swiper");
        
        st_satConnection.executeUpdate(
            " grant references on tsat to swiper");
        
        // set connection swiperConnection
        
        // Now some of these should pass
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_swiperConnection,
            "select * from satheesh.tsat");
        
        rs = st_swiperConnection.executeQuery(
            " select i from satheesh.tsat");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        JDBC.assertEmpty(rs);
        
        assertStatementError("42502", st_swiperConnection,
//IC see: https://issues.apache.org/jira/browse/DERBY-6429
            " select i from satheesh.tsat where noselect=2");
        
        rs = st_swiperConnection.executeQuery(
            " select i from satheesh.tsat where 2 > (select "
            + "count(i) from satheesh.tsat)");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        JDBC.assertEmpty(rs);
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_swiperConnection,
            " select i from satheesh.tsat where 2 > (select "
//IC see: https://issues.apache.org/jira/browse/DERBY-6429
            + "count(noselect) from satheesh.tsat)");
        
        rs = st_swiperConnection.executeQuery(
            " select i from satheesh.tsat where 2 > (select "
            + "count(*) from satheesh.tsat)");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        JDBC.assertEmpty(rs);
        
        assertUpdateCount(st_swiperConnection, 0,
            " update satheesh.tsat set j=j+1");
        
        assertUpdateCount(st_swiperConnection, 0,
            " update satheesh.tsat set j=2 where i=2");
        
        assertStatementError("42502", st_swiperConnection,
//IC see: https://issues.apache.org/jira/browse/DERBY-6429
            " update satheesh.tsat set j=2 where noselect=1");
        
        rs = st_swiperConnection.executeQuery(
            " select * from satheesh.table1");
        
        expColNames = new String [] {"A", "B", "C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        JDBC.assertEmpty(rs);
        
        rs = st_swiperConnection.executeQuery(
            " select c from satheesh.table1 t1, satheesh.tsat t2 "
            + "where t1.a = t2.i");
        
        expColNames = new String [] {"C"};
        JDBC.assertColumnNames(rs, expColNames);
        
        JDBC.assertEmpty(rs);
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_swiperConnection,
            " select b from satheesh.table1 t1, satheesh.tsat t2 "
//IC see: https://issues.apache.org/jira/browse/DERBY-6429
            + "where t1.a = t2.noselect");
        
        rs = st_swiperConnection.executeQuery(
            " select * from satheesh.table1, (select i from "
            + "satheesh.tsat) table2");
        
        expColNames = new String [] {"A", "B", "C", "I"};
        JDBC.assertColumnNames(rs, expColNames);
                
        JDBC.assertEmpty(rs);
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_swiperConnection,
//IC see: https://issues.apache.org/jira/browse/DERBY-6429
            " select * from satheesh.table1, (select noselect from "
            + "satheesh.tsat) table2");
        
        st_swiperConnection.executeUpdate(
            "update satheesh.tsat set j=i");
                
        st_swiperConnection.executeUpdate(
            " create table my_tsat (i int not null, c char(10), "
            + "constraint fk foreign key(i) references satheesh.tsat)");
        
        // set connection swiperConnection
        // Some TRIGGER privilege checks. See GrantRevoke.java for 
        // more tests
        
        
        // Should fail
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42500", st_swiperConnection,
            "create trigger trig_sat1 after update on "
            + "satheesh.tsat for each statement values 1");
        
        assertStatementError("42500", st_swiperConnection,
            " create trigger trig_sat2 no cascade before delete "
            + "on satheesh.tsat for each statement values 1");
        
        // set connection satConnection
        //ij(SWIPERCONNECTION)> -- Grant trigger privilege
        
        
        st_satConnection.executeUpdate(
            " grant trigger on tsat to swiper");
        
        // set connection swiperConnection
        //ij(SATCONNECTION)> -- Try now
        
        
        st_swiperConnection.executeUpdate(
            " create trigger trig_sat1 after update on "
            + "satheesh.tsat for each statement values 1");
        
        st_swiperConnection.executeUpdate(
            " create trigger trig_sat2 no cascade before delete "
            + "on satheesh.tsat for each statement values 1");
        
        st_swiperConnection.executeUpdate(
            " drop trigger trig_sat1");
        
        st_swiperConnection.executeUpdate(
            " drop trigger trig_sat2");
        
        // set connection satConnection
        //ij(SWIPERCONNECTION)> -- Now revoke and try again
        
        
        st_satConnection.executeUpdate(
            " revoke trigger on tsat from swiper");
        
        // set connection swiperConnection
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42500", st_swiperConnection,
            " create trigger trig_sat1 after update on "
            + "satheesh.tsat for each statement values 1");
        
        assertStatementError("42500", st_swiperConnection,
            " create trigger trig_sat2 no cascade before delete "
            + "on satheesh.tsat for each statement values 1");
        
        // set connection satConnection
        //ij(SWIPERCONNECTION)> -- Now grant access to public and 
        // try again
        
        
        st_satConnection.executeUpdate(
            " grant trigger on tsat to public");
        
        // set connection swiperConnection
        
        st_swiperConnection.executeUpdate(
            " create trigger trig_sat1 after update on "
            + "satheesh.tsat for each statement values 1");
        
        st_swiperConnection.executeUpdate(
            " create trigger trig_sat2 no cascade before delete "
            + "on satheesh.tsat for each statement values 1");
        
        st_swiperConnection.executeUpdate(
            " drop trigger trig_sat1");
        
        st_swiperConnection.executeUpdate(
            " drop trigger trig_sat2");

        // set connection satConnection
        
        // clean up

        st_satConnection.executeUpdate(
            " drop view v1");
        
        st_satConnection.executeUpdate(
            " drop table tsat");
         
        st_satConnection.executeUpdate(
            " drop table table1");
        
        //ij(SWIPERCONNECTION)> -- Some simple routine tests. See 
        // GrantRevoke.java for more tests
        
        
        rs = st_satConnection.executeQuery(
            " values f_abs(-5)");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"5"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_satConnection.executeQuery(
            " select f_abs(-4) from sys.systables where "
            + "tablename like 'SYSTAB%'");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"4"},
            {"4"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection swiperConnection
        //ij(SATCONNECTION)> -- Same tests should fail
        
        
        st_swiperConnection.executeUpdate(
            " set schema satheesh");
        
        assertStatementError("42504", st_swiperConnection,
            " values f_abs(-5)");
        
        assertStatementError("42504", st_swiperConnection,
            " select f_abs(-4) from sys.systables where "
            + "tablename like 'SYSTAB%'");
        
        // set connection satConnection
        // Now grant execute permission 
        // and try again
        
        
        st_satConnection.executeUpdate(
            " grant execute on function f_abs to swiper");
        
        // set connection swiperConnection
        
        // Should pass now
        
        rs = st_swiperConnection.executeQuery(
            "values f_abs(-5)");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"5"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_swiperConnection.executeQuery(
            " select f_abs(-4) from sys.systables where "
            + "tablename like 'SYSTAB%'");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"4"},
            {"4"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection satConnection
        //ij(SWIPERCONNECTION)> -- Now revoke permission and try
        
        
        st_satConnection.executeUpdate(
            " revoke execute on function f_abs from swiper RESTRICT");
        
        // set connection swiperConnection
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42504", st_swiperConnection,
            " values f_abs(-5)");
        
        assertStatementError("42504", st_swiperConnection,
            " select f_abs(-4) from sys.systables where "
            + "tablename like 'SYSTAB%'");
        
        // set connection satConnection
        //ij(SWIPERCONNECTION)> -- Now try public permission
        
        
        st_satConnection.executeUpdate(
            " grant execute on function f_abs to public");
        
        // set connection swiperConnection
        
        // Should pass again
        
        rs = st_swiperConnection.executeQuery(
            "values f_abs(-5)");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"5"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_swiperConnection.executeQuery(
            " select f_abs(-4) from sys.systables where "
            + "tablename like 'SYSTAB%'");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"4"},
            {"4"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // some more cleanup

        st_satConnection.executeUpdate("drop synonym satheesh.mySym");
        st_satConnection.executeUpdate("drop function satheesh.f_abs");
        
        // set connection swiperConnection
        // Test schema creation authorization checks
        
        
        // Negative tests. Should all fail
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42508", st_swiperConnection,
            "create schema myFriend");
        
        assertStatementError("42508", st_swiperConnection,
            " create schema mySchema authorization me");
        
        assertStatementError("42508", st_swiperConnection,
            " create schema myschema authorization swiper");
        
        Connection CONNECTION0 = openUserConnection("sam");
        Statement st_CONNECTION0 = CONNECTION0.createStatement();
      
        assertStatementError("42508", st_CONNECTION0,
            " create schema sam authorization swiper");
        
        // Should pass
        
        st_CONNECTION0.executeUpdate(
            "create schema authorization sam");
        
        Connection CONNECTION1 = openUserConnection("george");
        Statement st_CONNECTION1 = CONNECTION1.createStatement();
      
        st_CONNECTION1.executeUpdate(
            " create schema george");
        
        // set connection satConnection
        //ij(CONNECTION1)> -- Now try as DBA (satheesh)
        
        
        st.executeUpdate(
            " create schema myFriend");
        
        st.executeUpdate(
            " create schema mySchema authorization me");
        
        st.executeUpdate(
            " create schema authorization testSchema");
        
        rs = st.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6446
            " select SCHEMANAME, AUTHORIZATIONID from sys.sysschemas where schemaname not like 'TEST_DBO%' order by SCHEMANAME");
        
        expColNames = new String [] {"SCHEMANAME", "AUTHORIZATIONID"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"APP", "APP"},
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            {"DERBY", "DERBY"},
            {"GEORGE", "GEORGE"},
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            {"MYDODO", "DODO"},
            {"MYFRIEND", "TEST_DBO"},
            {"MYSCHEMA", "ME"},
            {"NULLID", "TEST_DBO"},
            {"SAM", "SAM"},
            {"SATHEESH", "SATHEESH"},
            {"SQLJ", "TEST_DBO"},
            {"SWIPER", "SWIPER"},
            {"SYS", "TEST_DBO"},
            {"SYSCAT", "TEST_DBO"},
            {"SYSCS_DIAG", "TEST_DBO"},
            {"SYSCS_UTIL", "TEST_DBO"},
            {"SYSFUN", "TEST_DBO"},
            {"SYSIBM", "TEST_DBO"},
            {"SYSPROC", "TEST_DBO"},
            {"SYSSTAT", "TEST_DBO"},
            {"TESTSCHEMA", "TESTSCHEMA"},
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // don't need satheesh schema anymore.
        st.executeUpdate("drop schema satheesh restrict");
 
        // set connection swiperConnection
        // Test implicit creation of 
        // schemas.. Should fail
        
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42508", st_swiperConnection,
            " create table mywork.t1(i int)");
        
        assertStatementError("42508", st_swiperConnection,
            " create view mywork.v1 as select * from swiper.swiperTab");
        
        // Implicit schema creation should only work if creating 
        // own schema
        
        Connection monicaConnection = openUserConnection("monica");
        Statement st_monicaConnection = monicaConnection.createStatement();
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42508", st_monicaConnection,
            " create table mywork.t1 ( i int)");
        
        st_monicaConnection.executeUpdate(
            " create table monica.shouldPass(c char(10))");
        
        // set connection swiperConnection
        //ij(MONICACONNECTION)> -- Check if DBA can ignore all 
        // privilege checks
        
        
        st_swiperConnection.executeUpdate(
            " set schema swiper");
        
        st_swiperConnection.executeUpdate(
            " revoke select on swiperTab from satheesh");
        
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_swiperConnection != null))
                sqlWarn = st_swiperConnection.getWarnings();
            if (sqlWarn == null)
                sqlWarn = swiperConnection.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01006", sqlWarn);
            sqlWarn = null;
        }
        
        st_swiperConnection.executeUpdate(
            " revoke insert on swiperTab from satheesh");
        
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_swiperConnection != null))
                sqlWarn = st_swiperConnection.getWarnings();
            if (sqlWarn == null)
                sqlWarn = swiperConnection.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01006", sqlWarn);
            sqlWarn = null;
        }
        
        // Should still work, as DBA
        
        rs = st.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            "select * from swiper.swiperTab order by i,j");
        
        expColNames = new String [] {"I", "J"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st.executeUpdate(
            " insert into swiper.swiperTab values (2,2)");
        
        rs = st.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select * from swiper.swiperTab order by i,j");
        
        expColNames = new String [] {"I", "J"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"},
            {"2", "2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st.executeUpdate(
            " grant select on swiper.swiperTab to sam");
        
        st.executeUpdate(
            " revoke insert on swiper.swiperTab from satheesh");
        
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st != null))
                sqlWarn = st.getWarnings();
            if (sqlWarn == null)
                sqlWarn = getConnection().getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01006", sqlWarn);
            sqlWarn = null;
        }
        
        // Test system routines. Some don't need explicit grant 
        // and others do allowing for only DBA use by default
        
        
        // Try granting or revoking from system tables. Should fail
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42509", st,
            "grant select on sys.systables to sam");
        
        assertStatementError("42509", st,
            " grant delete on sys.syscolumns to sam");
        
        assertStatementError("42509", st,
            " grant update(alias) on sys.sysaliases to swiper");
        
        assertStatementError("42509", st,
            " revoke all privileges on sys.systableperms from public");
        
        assertStatementError("42509", st,
            " revoke trigger on sys.sysroutineperms from sam");
        
        // Try granting or revoking from system routines that is 
        // expected fail
        
        assertStatementError("42509", st,
            "grant execute on procedure sysibm.sqlprocedures to sam");
        
        assertStatementError("42509", st,
            " revoke execute on procedure sysibm.sqlcamessage "
            + "from public restrict");
        
        // Try positive tests
        
        Connection samConnection = openUserConnection("sam");
        Statement st_samConnection = samConnection.createStatement();
      
        st_samConnection.executeUpdate(
            " create table samTable(i int)");
        
        st_samConnection.executeUpdate(
            " insert into samTable values 1,2,3,4,5,6,7");
        
        // Following should pass... PUBLIC should have access to these
        
        cSt = samConnection.prepareCall(
            "call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        assertUpdateCount(cSt, 0);
        
        cSt = samConnection.prepareCall(
            " call SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1)");
        assertUpdateCount(cSt, 0);
        
        rs = st_samConnection.executeQuery(
            " values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        JDBC.assertDrainResults(rs, 1);

        cSt = samConnection.prepareCall(
            " call SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('SAM','SAMTABLE',null)");
        assertUpdateCount(cSt, 0);
        cSt.close();
        
        cSt = samConnection.prepareCall(
            " call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('SAM', 'SAMTABLE', 1)");
        assertUpdateCount(cSt, 0);
//IC see: https://issues.apache.org/jira/browse/DERBY-2735
        cSt.close();
        
        cSt = samConnection.prepareCall(
            " call "
            + "SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE('SAM', "
            + "'SAMTABLE', 1, 1, 1)");
        assertUpdateCount(cSt, 0);
        cSt.close();
        
        // Try compressing tables not owned.
        
        cSt = samConnection.prepareCall(
            "call SYSCS_UTIL.SYSCS_COMPRESS_TABLE('SWIPER', 'MYTAB', 1)");
        assertStatementError("38000", cSt);
//IC see: https://issues.apache.org/jira/browse/DERBY-2735
        cSt.close();
        
        cSt = samConnection.prepareCall(
            " call "
            + "SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE('SWIPER', "
            + "'MYTAB', 1, 1, 1)");
        assertStatementError("38000", cSt);
        cSt.close();

        // Try updating statistics of table not owned.
        cSt = samConnection.prepareCall(
        " call SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('SWIPER','MYTAB',null)");
        assertStatementError("38000", cSt);
        cSt.close();
        
        // Try other system routines. All should fail
        
        cSt = samConnection.prepareCall(
            "call SYSCS_UTIL.SYSCS_EXPORT_TABLE('SAM', "
            + "'SAMTABLE' , 'extinout/table.dat', null, null, null)");
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42504", cSt);
//IC see: https://issues.apache.org/jira/browse/DERBY-2735
        cSt.close();
        
        cSt = samConnection.prepareCall(
            " call "
            + "SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storag"
            + "e.pageSize', '4096')");
        assertStatementError("42504", cSt);
        cSt.close();
        
        assertStatementError("42504", st_samConnection,
            " values "
            + "SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.storag"
            + "e.pageSize')");

        PreparedStatement psgua = samConnection.prepareStatement(
            "VALUES SYSCS_UTIL.SYSCS_GET_USER_ACCESS(CURRENT_USER)");
        
        assertStatementError("42504", psgua);
        psgua.close();

        cSt = samConnection.prepareCall(
             "CALL SYSCS_UTIL.SYSCS_SET_USER_ACCESS(CURRENT_USER, NULL)");
        assertStatementError("42504", cSt);
        cSt.close();
        
//IC see: https://issues.apache.org/jira/browse/DERBY-2772
        cSt = samConnection.prepareCall(
        "CALL SYSCS_UTIL.SYSCS_EMPTY_STATEMENT_CACHE()");
            assertStatementError("42504", cSt);
        cSt.close();
        
        
        
        // set connection satConnection
        // Try after DBA grants permissions
        
        
        st.executeUpdate(
            " grant execute on procedure "
            + "SYSCS_UTIL.SYSCS_EXPORT_TABLE to public");
        
        st.executeUpdate(
            " grant execute on procedure "
            + "SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY to sam");
        
        st.executeUpdate(
            " grant execute on function "
            + "SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY to sam");
        
        // Now these should pass
        
        cSt = samConnection.prepareCall(
            "call SYSCS_UTIL.SYSCS_EXPORT_TABLE('SAM', "
            + "'SAMTABLE' , 'extinout/table.dat', null, null, null)");
        
        cSt = samConnection.prepareCall(
            " call "
            + "SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storag"
            + "e.pageSize', '4096')");
        assertUpdateCount(cSt, 0);
        
        rs = st_samConnection.executeQuery(
            " values "
            + "SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.storag"
            + "e.pageSize')");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"4096"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
    
        // revoke the previously granted permissions, these
        // are tested again in testGrantRevokeDDL2
        st.executeUpdate(
                " revoke execute on procedure "
                + "SYSCS_UTIL.SYSCS_EXPORT_TABLE from public restrict");
            
        st.executeUpdate(
                " revoke execute on procedure "
                + "SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY from sam restrict");
            
        st.executeUpdate(
                " revoke execute on function "
                + "SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY from sam restrict");
            
        // grant one permission on table to user1 and another 
        // permission to user3, then grant another permission on 
        // that same table to user1 and user2(this is the first 
        // permission to user2 on the table) and user3 (this user 
        // already has the permission being granted). Notice that 
        // the first 2 grant statements created a row in 
        // SYSTABLEPERMS for user1 and user3. Third grant is going 
        // to update the pre-existing row for user1. The third 
        // grant is going to insert a new row for user2 in 
        // SYSTABLEPERMS and the third grant is going to be a no-op 
        // for user3. So, basically, this is to test that one 
        // single grant statment can update and insert and no-op 
        // rows into SYSTABLEPERMS for different users.
        
        Connection mamta1 = openUserConnection("mamta1");
        Statement st_mamta1 = mamta1.createStatement();

        st_mamta1.executeUpdate(
            " create table t11 (c111 int not null primary key)");
        
        st_mamta1.executeUpdate(
            " insert into t11 values(1)");
        
        st_mamta1.executeUpdate(
            " grant select on t11 to mamta2");
        
        st_mamta1.executeUpdate(
            " grant insert on t11 to mamta3");
        
        st_mamta1.executeUpdate(
            " grant insert on t11 to mamta2, mamta3, mamta4");
        
        Connection mamta2 = openUserConnection("mamta2");
        Statement st_mamta2 = mamta2.createStatement();
        
        rs = st_mamta2.executeQuery(
            " select * from mamta1.t11");
        
        expColNames = new String [] {"C111"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st_mamta2.executeUpdate(
            " insert into mamta1.t11 values(2)");
        
        rs = st_mamta2.executeQuery(
            " select * from mamta1.t11 order by c111");
        
        expColNames = new String [] {"C111"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        Connection mamta3 = openUserConnection("mamta3");
        Statement st_mamta3 = mamta3.createStatement();
 
        // following select will fail because no permissions
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_mamta3,
            "select * from mamta1.t11");
        
        st_mamta3.executeUpdate(
            " insert into mamta1.t11 values(3)");
        
        Connection mamta4 = openUserConnection("mamta4");
        Statement st_mamta4 = mamta4.createStatement();
        
        // following select will fail because no permissions
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_mamta4,
            "select * from mamta1.t11");
        
        st_mamta4.executeUpdate(
            " insert into mamta1.t11 values(4)");
        
        // set connection mamta1
        
        st_mamta1.executeUpdate(
            " revoke all privileges on t11 from PUBLIC");
        
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_mamta1 != null))
                sqlWarn = st_mamta1.getWarnings();
            if (sqlWarn == null)
                sqlWarn = mamta1.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01006", sqlWarn);
            sqlWarn = null;
        }
        
        rs = st_mamta1.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select * from mamta1.t11 order by c111");
        
        expColNames = new String [] {"C111"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"3"},
            {"4"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st_mamta1.executeUpdate(
            " drop table t11");
        
        // set connection mamta1
        // now test the column level permissions
        
        
        st_mamta1.executeUpdate(
            " create table t11 (c111 int not null primary key, "
            + "c112 int, c113 int, c114 int)");
        
        st_mamta1.executeUpdate(
            " insert into t11 values(1,1,1,1)");
        
        st_mamta1.executeUpdate(
            " grant select(c111) on t11 to mamta2");
        
        st_mamta1.executeUpdate(
            " grant select(c112) on t11 to mamta2, mamta3");
        
        st_mamta1.executeUpdate(
            " grant update(c112) on t11 to mamta2, mamta3, mamta4");
        
        st_mamta1.executeUpdate(
            " grant update on t11 to mamta2");
        
        // set connection mamta2
        
        assertUpdateCount(st_mamta2, 1,
            " update mamta1.t11 set c113 = 2 where c111=1");
        
        rs = st_mamta2.executeQuery(
            " select c111,c112 from mamta1.t11");
        
        expColNames = new String [] {"C111", "C112"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // following will fail because no select permissions on 
        // all the columns
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_mamta2,
            "select * from mamta1.t11");
        
        // set connection mamta3
        
        // following will fail because no update permission on 
        // column c113
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_mamta3,
            "update mamta1.t11 set c113=3");
        
        rs = st_mamta3.executeQuery(
            " select c112 from mamta1.t11");
        
        expColNames = new String [] {"C112"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta4
        
        // following will fail because no select permission on 
        // column c112
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_mamta4,
            "select c112 from mamta1.t11");
        
        // set connection mamta1
        
        rs = st_mamta1.executeQuery(
            " select * from mamta1.t11");
        
        expColNames = new String [] {"C111", "C112", "C113", "C114"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", "2", "1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st_mamta1.executeUpdate(
            " revoke select on t11 from mamta2, mamta3, mamta4");
        
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_mamta1 != null))
                sqlWarn = st_mamta1.getWarnings();
            if (sqlWarn == null)
                sqlWarn = mamta1.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01006", sqlWarn);
            sqlWarn = null;
        }
        
        st_mamta1.executeUpdate(
            " revoke update(c111, c112) on t11 from mamta2, "
            + "mamta3, mamta4");
        
        st_mamta1.executeUpdate(
            " drop table t11");
        
        // set connection mamta1
        // Testing views to make sure we collect their depedencies 
        // on privileges in SYSDEPENDS table
        
        
        st_mamta1.executeUpdate(
            " create table t11 (c111 int not null primary key)");
        
        st_mamta1.executeUpdate(
            " insert into t11 values(1)");
        
        st_mamta1.executeUpdate(
            " insert into t11 values(2)");
        
        rs = st_mamta1.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select * from t11 order by c111");
        
        expColNames = new String [] {"C111"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st_mamta1.executeUpdate(
            " create table t12 (c121 int, c122 char)");
        
        st_mamta1.executeUpdate(
            " insert into t12 values (1,'1')");
        
        rs = st_mamta1.executeQuery(
            " select * from t12");
        
        expColNames = new String [] {"C121", "C122"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st_mamta1.executeUpdate(
            " create table t13 (c131 int, c132 char)");
        
        st_mamta1.executeUpdate(
            " insert into t13 values (1,'1')");
        
        rs = st_mamta1.executeQuery(
            " select * from t13");
        
        expColNames = new String [] {"C131", "C132"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st_mamta1.executeUpdate(
            " grant select on t12 to mamta2");
        
        st_mamta1.executeUpdate(
            " grant select on t11 to public");
        
        // set connection mamta2
        
        // both of following will pass because mamt2 has has 
        // required privileges because of PUBLIC select access of 
        // mamta1.t11.
        
        st_mamta2.executeUpdate(
            "create view v21 as select t1.c111, t2.c122 from "
            + "mamta1.t11 as t1, mamta1.t12 as t2");
        
        st_mamta2.executeUpdate(
            " create view v22 as select * from mamta1.t11");
        
        st_mamta2.executeUpdate(
            " create view v23 as select * from mamta1.t12");
        
        // set connection mamta1
        
        // When the create view v23 from mamta2's session is 
        // executed in mamta1, there will be only    one row in 
        // sysdepends for view v23. That row will be for view's 
        // dependency on t12.    There will be no row for privilege 
        // dependency because table t12 is owned by the same    
        // user who is creating the view v23 and hence there is no 
        // privilege required.
        
        st_mamta1.executeUpdate(
            "create view v23 as select * from mamta1.t12");
        
        // set connection satConnection
        //ij(MAMTA1)> -- satConnection is dba and hence doesn't 
        // need explicit privileges to access ojects in any schema 
        // within the database
        
        
        // since test_dbo is dba, following will not fail 
        // even if test_dbo has no explicit privilege to 
        // mamta2.v22
        
        st.executeUpdate(
            "create view v11 as select * from mamta2.v22");
        
        // set connection mamta3
        
        st_mamta3.executeUpdate(
            " create table t31(c311 int)");
        
        // since mamta3 is not dba, following will fail because no 
        // access to mamta2.v22
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_mamta3,
            "create view v31 as select * from mamta2.v22");
        
        // mamta3 has access to mamta1.t11 since there is PUBLIC 
        // select access on that table but there is no access to 
        // mamta2.v22
        
        assertStatementError("42502", st_mamta3,
            "create view v32 as select v22.c111 as a, t11.c111 "
            + "as b from mamta2.v22 v22, mamta1.t11 t11");
        
        // Try to create a view with no privilege to more than one 
        // object.
        
        assertStatementError("42502", st_mamta3,
            "create view v33 as select v22.c111 as a, t11.c111 "
            + "as b from mamta2.v22 v22, mamta1.t11 t11, mamta2.v21");
        
        // set connection mamta2
        //ij(MAMTA3)> -- connect as mamta2 and give select 
        // privilege on v22 to mamta3
        
        
        // should fail
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("4250A", st_mamta2,
            "grant select on v22 to mamta3");
        
        // set connection mamta3
        
        // should fail
        
        assertStatementError("42502", st_mamta3,
            "create view v31 as select * from mamta2.v22");
        
        // following will fail because mamta3 has no access to v22
        
        assertStatementError("42502", st_mamta3,
            "create view v32 as select v22.c111 as a, t11.c111 "
            + "as b from mamta2.v22 v22, mamta1.t11 t11");
        
        // following will still fail because mamta3 doesn't have 
        // access to mamta1.t12.c121
        
        assertStatementError("42502", st_mamta3,
            "create view v33 as select v22.c111 as a, t12.c121 "
            + "as b from mamta2.v22 v22, mamta1.t12 t12");
        
        // set connection mamta2
        //ij(MAMTA3)> -- connect as mamta2 and give select 
        // privilege on v23 to mamta3
        
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("4250A", st_mamta2,
            " grant select on v23 to mamta3");
        
        // set connection mamta3
        
        // should fail
        
        assertStatementError("42502", st_mamta3,
            "create view v34 as select * from mamta2.v23");
        
        // should fail
        
        assertStatementError("42X05", st_mamta3,
            "create view v35 as select * from v34");
       
        // set connection mamta1
        //ij(MAMTA3)> -- Write some views based on a routine
        
        
        assertStatementError("42Y55", st_mamta1,
            " drop function f_abs1");
        
        st_mamta1.executeUpdate(
            " CREATE FUNCTION F_ABS1(P1 INT) RETURNS INT NO "
            + "SQL RETURNS NULL ON NULL INPUT EXTERNAL NAME "
            + "'java.lang.Math.abs' LANGUAGE JAVA PARAMETER STYLE JAVA");
        
        rs = st_mamta1.executeQuery(
            " values f_abs1(-5)");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"5"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertStatementError("X0X05", st_mamta1,
            " drop view v11");
        
        st_mamta1.executeUpdate(
            " create view v11(c111) as values mamta1.f_abs1(-5)");
        
        st_mamta1.executeUpdate(
            " grant select on v11 to mamta2");
        
        rs = st_mamta1.executeQuery(
            " select * from v11");
        
        expColNames = new String [] {"C111"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"5"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta2
        
        assertStatementError("X0X05", st_mamta2,
            " drop view v24");
        
        st_mamta2.executeUpdate(
            " create view v24 as select * from mamta1.v11");
        
        rs = st_mamta2.executeQuery(
            " select * from v24");
        
        expColNames = new String [] {"C111"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"5"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertStatementError("X0X05", st_mamta2,
            " drop view v25");
        
        // following will fail because no execute permissions on 
        // mamta1.f_abs1
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42504", st_mamta2,
            "create view v25(c251) as (values mamta1.f_abs1(-1))");
        
        // set connection mamta1
        
        st_mamta1.executeUpdate(
            " grant execute on function f_abs1 to mamta2");
        
        // set connection mamta2
        
        // this view creation will pass now because have execute 
        // privileges on the function
        
        st_mamta2.executeUpdate(
            "create view v25(c251) as (values mamta1.f_abs1(-1))");
        
        rs = st_mamta2.executeQuery(
            " select * from v25");
        
        expColNames = new String [] {"C251"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta1
        
        // try revoke execute privilege. Since there are dependent 
        // objects, the revoke shold fail
        
        assertStatementError("X0Y23", st_mamta1,
            "revoke execute on function f_abs1 from mamta2 restrict");
        
        // set connection mamta2
        //ij(MAMTA1)> -- drop the dependent objects on the execute 
        // privilege and then try to revoke the execute privilege
        
        
        st_mamta2.executeUpdate(
            " drop view v25");
        
        // set connection mamta1
        
        // revoke execute privilege should pass this time because 
        // no dependents on that permission.
        
        st_mamta1.executeUpdate(
            "revoke execute on function f_abs1 from mamta2 restrict");
        
        // set connection mamta2
        
        // following select should still pass because v24 is not 
        // directly dependent on the execute permission.   It gets 
        // to the routine via view v11 which will be run with 
        // definer's privileges and definer of   view v11 is also 
        // the owner of the routine
        
        rs = st_mamta2.executeQuery(
            "select * from v24");
        
        expColNames = new String [] {"C111"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"5"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // cleanup
        
        st_mamta2.executeUpdate(
            "drop view v24");
        
        // set connection mamta1
        
        st_mamta1.executeUpdate(
            " drop view v11");
        
        st_mamta1.executeUpdate(
            " drop function f_abs1");
        
        // set connection mamta1
        // try column level privileges and views In this test, 
        // user has permission on one column but not on the other
        
        
        st_mamta1.executeUpdate(
            " create table t14(c141 int, c142 int)");
        
        st_mamta1.executeUpdate(
            " insert into t14 values (1,1), (2,2)");
        
        st_mamta1.executeUpdate(
            " grant select(c141) on t14 to mamta2");
        
        // set connection mamta2
        
        // following will fail because no access on column 
        // mamta1.t14.c142
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_mamta2,
            "create view v26 as (select * from mamta1.t14 where c142=1)");
        
        // following will fail for the same reason
        
        assertStatementError("42502", st_mamta2,
            "create view v26 as (select c141 from mamta1.t14 "
            + "where c142=1)");
        
        // following will pass because view is based on column 
        // that it can access
        
        st_mamta2.executeUpdate(
            "create view v27 as (select c141 from mamta1.t14)");
        
        rs = st_mamta2.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select * from v27 order by c141");
        
        expColNames = new String [] {"C141"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta1
        
        // give access to all the columns in t14 to mamta2
        
        st_mamta1.executeUpdate(
            "grant select on t14 to mamta2");
        
        // set connection mamta2
        
        // now following will pass
        
        st_mamta2.executeUpdate(
            "create view v26 as (select c141 from mamta1.t14 "
            + "where c142=1)");
        
        rs = st_mamta2.executeQuery(
            " select * from v26");
        
        expColNames = new String [] {"C141"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta1
        //ij(MAMTA2)> -- in this column level privilege test, 
        // there is a user level permission on one column   and a 
        // PUBLIC level on the other column.
        
        
        st_mamta1.executeUpdate(
            " create table t15(c151 int, c152 int)");
        
        st_mamta1.executeUpdate(
            " insert into t15 values(1,1),(2,2)");
        
        st_mamta1.executeUpdate(
            " grant select(c151) on t15 to mamta2");
        
        st_mamta1.executeUpdate(
            " grant select(c152) on t15 to public");
        
        // set connection mamta2
        
        st_mamta2.executeUpdate(
            " create view v28 as (select c152 from mamta1.t15 "
            + "where c151=1)");
        
        // set connection mamta1
        //ij(MAMTA2)> -- write some view based tests and revoke 
        // privileges to see if the right thing happens View tests 
        // test1  A simple test where a user creates a view based 
        // on objects in other schemas and revoke privilege on one 
        // of those  objects will drop the view
        
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t11ViewTest");
        
        st_mamta1.executeUpdate(
            " create table t11ViewTest (c111 int not null primary key)");
        
        st_mamta1.executeUpdate(
            " insert into t11ViewTest values(1)");
        
        st_mamta1.executeUpdate(
            " insert into t11ViewTest values(2)");
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t12ViewTest");
        
        st_mamta1.executeUpdate(
            " create table t12ViewTest (c121 int, c122 char)");
        
        st_mamta1.executeUpdate(
            " insert into t12ViewTest values (1,'1')");
        
        // user mamta2 is going to create a view based on 
        // following grants
        
        st_mamta1.executeUpdate(
            "grant select on t12ViewTest to mamta2");
        
        st_mamta1.executeUpdate(
            " grant select on t11ViewTest to public");
        
        // set connection mamta2
        
        assertStatementError("X0X05", st_mamta2,
            " drop view v21ViewTest");
        
        // will succeed because all the required privileges are in 
        // place
        
        st_mamta2.executeUpdate(
            "create view v21ViewTest as select t1.c111, t2.c122 "
            + "from mamta1.t11ViewTest as t1, mamta1.t12ViewTest as t2");
        
        rs = st_mamta2.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select * from v21ViewTest order by c111, c122");
        
        expColNames = new String [] {"C111", "C122"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"},
            {"2", "1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta1
        
        // this revoke should drop the dependent view in schema mamta2
        
        st_mamta1.executeUpdate(
            "revoke select on t11ViewTest from public");
        
        // set connection mamta2
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_mamta1 != null))
                sqlWarn = st_mamta1.getWarnings();
            if (sqlWarn == null)
                sqlWarn = mamta1.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01501", sqlWarn);
            sqlWarn = null;
        }
        
        
        // the view shouldn't exist anymore because one of the 
        // privileges required by it was revoked
        
        assertStatementError("42X05", st_mamta2,
            "select * from v21ViewTest");
        
        // set connection mamta1
        
        // this revoke should not impact any objects because none 
        // depend on it
        
        st_mamta1.executeUpdate(
            "revoke select on t12ViewTest from mamta2");
        
        // set connection mamta2
        
        assertStatementError("42X05", st_mamta2,
            " select * from v21ViewTest");
        
        // set connection mamta1
        //ij(MAMTA2)> -- cleanup
        
        
        st_mamta1.executeUpdate(
            " drop table t11ViewTest");
        
        st_mamta1.executeUpdate(
            " drop table t12ViewTest");
        
        // set connection mamta1
        // View tests test2  Let the dba create a view in schema 
        // mamta2 (owned by user mamta2). The view's definition 
        // accesses    objects from schema mamta1. The owner of 
        // schema mamta2 does not have access to objects in schema 
        // mamta1    but the create view by dba does not fail 
        // because dba has access to all the objects.  mamta2 will 
        // have access to the view created by the dba because 
        // mamta2 is owner of the schema "mamta2" and    it has 
        // access to all the objects created in it's schema, 
        // whether they were created by mamta2 or the dba.  user 
        // mamta2 is owner of the schema mamta2 because user mamta2 
        // was the first one to create an object in    schema 
        // mamta2 earlier in this test.  Any other user (except the 
        // dba) will need to get explicit select privileges on the 
        // view in order to access it
        
        
        // Note that mamta1 is creating couple tables but has not 
        // granted permissions on those tables to anyone
        
        assertStatementError("42Y55", st_mamta1,
            "drop table t11ViewTest");
        
        st_mamta1.executeUpdate(
            " create table t11ViewTest (c111 int not null primary key)");
        
        st_mamta1.executeUpdate(
            " insert into t11ViewTest values(1)");
        
        st_mamta1.executeUpdate(
            " insert into t11ViewTest values(2)");
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t12ViewTest");
        
        st_mamta1.executeUpdate(
            " create table t12ViewTest (c121 int, c122 char)");
        
        st_mamta1.executeUpdate(
            " insert into t12ViewTest values (1,'1')");
        
        // set connection satConnection
        //ij(MAMTA1)> -- connect as dba
        
        
        // dba is creating a view in schema owned by another user. 
        // dba can create objects anywhere and access objects from 
        // anywhere
        
        st.executeUpdate(
            "create view mamta2.v21ViewTest as select t1.c111, "
            + "t2.c122 from mamta1.t11ViewTest as t1, "
            + "mamta1.t12ViewTest as t2");
        
        // dba can do select from that view
        
        rs = st.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            "select * from mamta2.v21ViewTest order by c111");
        
        expColNames = new String [] {"C111", "C122"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"},
            {"2", "1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta2
        
        // the schema owner can do a select from an object that is 
        // part of it's schema even though it was created by the dba
        
        rs = st_mamta2.executeQuery(
            "select * from v21ViewTest order by c111");
        
        expColNames = new String [] {"C111", "C122"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"},
            {"2", "1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta3
        
        // mamta3 has not been granted select privileges on 
        // mamta2.v21ViewTest
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_mamta3,
            "select * from mamta2.v21ViewTest");
        
        // set connection mamta2
        
        // give select privileges on the view to mamta3, should fail
        
        assertStatementError("4250A", st_mamta2,
            "grant select on v21ViewTest to mamta3");
        
        // set connection mamta3
        
        // select from mamta2.v21ViewTest will fail for mamta3 
        // because mamta3 has no select privilege on mamta2.v21ViewTest
        
        assertStatementError("42502", st_mamta3,
            "select * from mamta2.v21ViewTest");
        
        // set connection satConnection
        
        // have the dba take away select privilege on 
        // mamta2.v21ViewTest from mamta3
        
        st.executeUpdate(
            "revoke select on mamta2.v21ViewTest from mamta3");
        
        // set connection mamta3
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st != null))
                sqlWarn = st.getWarnings();
            if (sqlWarn == null)
                sqlWarn = getConnection().getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01006", sqlWarn);
            sqlWarn = null;
        }
        
        
        // select from mamta2.v21ViewTest will fail this time for 
        // mamta3 because dba took away the select privilege on 
        // mamta2.v21ViewTest
        
        assertStatementError("42502", st_mamta3,
            "select * from mamta2.v21ViewTest");
        
        // set connection mamta2
        //ij(MAMTA3)> -- cleanup
        
        
        st_mamta2.executeUpdate(
            " drop view v21ViewTest");
        
        // set connection mamta1
        
        st_mamta1.executeUpdate(
            " drop table t12ViewTest");
        
        st_mamta1.executeUpdate(
            " drop table t11ViewTest");
        
        // set connection mamta1
        // View tests test3  Create a view that relies on table 
        // level and column permissions and see that view gets 
        // dropped correctly when any of the    required privilege 
        // is revoked
        
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t11ViewTest");
        
        st_mamta1.executeUpdate(
            " create table t11ViewTest (c111 int not null primary key)");
        
        st_mamta1.executeUpdate(
            " insert into t11ViewTest values(1)");
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t12ViewTest");
        
        st_mamta1.executeUpdate(
            " create table t12ViewTest (c121 int, c122 char)");
        
        st_mamta1.executeUpdate(
            " insert into t12ViewTest values (1,'1')");
        
        st_mamta1.executeUpdate(
            " grant select (c111) on t11ViewTest to mamta3");
        
        st_mamta1.executeUpdate(
            " grant select (c121, c122) on t12ViewTest to public");
        
        // set connection mamta2
        
        assertStatementError("42Y55", st_mamta2,
            " drop table t21ViewTest");
        
        st_mamta2.executeUpdate(
            " create table t21ViewTest (c211 int)");
        
        st_mamta2.executeUpdate(
            " insert into t21ViewTest values(1)");
        
        st_mamta2.executeUpdate(
            " grant select on t21ViewTest to mamta3");
        
        // set connection mamta3
        
        assertStatementError("X0X05", st_mamta3,
            " drop view v31ViewTest");
        
        st_mamta3.executeUpdate(
            " create view v31ViewTest as select t2.c122, t1.*, "
            + "t3.* from mamta1.t11ViewTest as t1, "
            + "mamta1.t12ViewTest as t2,mamta2.t21ViewTest as t3 "
            + "where t1.c111 = t3.c211");
        
        rs = st_mamta3.executeQuery(
            " select * from v31ViewTest");
        
        expColNames = new String [] {"C122", "C111", "C211"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", "1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta1
        
        // revoke a column level privilege. It should drop the view
        
        st_mamta1.executeUpdate(
            "revoke select(c122) on t12ViewTest from public");
        
        // set connection mamta3
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_mamta1 != null))
                sqlWarn = st_mamta1.getWarnings();
            if (sqlWarn == null)
                sqlWarn = mamta1.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01501", sqlWarn);
            sqlWarn = null;
        }
        
        
        // the view got dropped because of revoke issued earlier
        
        assertStatementError("42X05", st_mamta3,
            "select * from v31ViewTest");
        
        // set connection mamta2
        //ij(MAMTA3)> -- cleanup
        
        
        st_mamta2.executeUpdate(
            " drop table t21ViewTest");
        
        // set connection mamta1
        
        st_mamta1.executeUpdate(
            " drop table t12ViewTest");
        
        st_mamta1.executeUpdate(
            " drop table t11ViewTest");
        
        // set connection mamta1
        // View tests test4  Create a view that relies on a 
        // user-level table privilege and a user-level column 
        // privilege.   There also exists a PUBLIC-level column 
        // privilege but objects at the creation time always first  
        //  look for the required privilege at the user 
        // level(DERBY-1632). This behavior can be confirmed by the 
        //   following test case where when PUBLIC-level column 
        // privilege is revoked, it does not impact the   view in 
        // anyway because the view is relying on user-level column 
        // privilege. Confirm that object   is relying on 
        // user-level privilege by revoking the user-level 
        // privilege and that should drop the object
        
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t11ViewTest");
        
        st_mamta1.executeUpdate(
            " create table t11ViewTest (c111 int not null primary key)");
        
        st_mamta1.executeUpdate(
            " insert into t11ViewTest values(1)");
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t12ViewTest");
        
        st_mamta1.executeUpdate(
            " create table t12ViewTest (c121 int, c122 char)");
        
        st_mamta1.executeUpdate(
            " insert into t12ViewTest values (1,'1')");
        
        st_mamta1.executeUpdate(
            " grant select (c111) on t11ViewTest to mamta3, public");
        
        st_mamta1.executeUpdate(
            " grant select (c121, c122) on t12ViewTest to public");
        
        // set connection mamta2
        
        assertStatementError("42Y55", st_mamta2,
            " drop table t21ViewTest");
        
        st_mamta2.executeUpdate(
            " create table t21ViewTest (c211 int)");
        
        st_mamta2.executeUpdate(
            " insert into t21ViewTest values(1)");
        
        st_mamta2.executeUpdate(
            " grant select on t21ViewTest to mamta3, mamta5");
        
        // set connection mamta3
        
        assertStatementError("X0X05", st_mamta3,
            " drop view v31ViewTest");
        
        st_mamta3.executeUpdate(
            " create view v31ViewTest as select t2.c122, t1.*, "
            + "t3.* from mamta1.t11ViewTest as t1, "
            + "mamta1.t12ViewTest as t2,mamta2.t21ViewTest as t3 "
            + "where t1.c111 = t3.c211");
        
        rs = st_mamta3.executeQuery(
            " select * from v31ViewTest");
        
        expColNames = new String [] {"C122", "C111", "C211"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", "1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta1
        
        // revoke public level privilege. Should not impact the 
        // view because user objects always rely on user level 
        // privilege.   If no user level privilege is found at 
        // create object time, then PUBLIC level privilege (if 
        // there) is used.   If there is no privilege granted at 
        // user level or public level at create object time, the 
        // create sql will fail   DERBY-1632
        
        st_mamta1.executeUpdate(
            "revoke select(c111) on t11ViewTest from public");
        
        // set connection mamta3
        
        // still exists because privileges required by it are not 
        // revoked
        
        rs = st_mamta3.executeQuery(
            "select * from v31ViewTest");
        
        expColNames = new String [] {"C122", "C111", "C211"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", "1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta1
        
        // this revoke should drop the view mamta3.v31ViewTest
        
        st_mamta1.executeUpdate(
            "revoke select(c111) on t11ViewTest from mamta3");
        
        // set connection mamta3
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_mamta1 != null))
                sqlWarn = st_mamta1.getWarnings();
            if (sqlWarn == null)
                sqlWarn = mamta1.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01501", sqlWarn);
            sqlWarn = null;
        }
        
        
        // View shouldn't exist anymore
        
        assertStatementError("42X05", st_mamta3,
            "select * from v31ViewTest");
        
        // set connection mamta2
        //ij(MAMTA3)> -- cleanup
        
        
        st_mamta2.executeUpdate(
            " drop table t21ViewTest");
        
        // set connection mamta1
        
        st_mamta1.executeUpdate(
            " drop table t12ViewTest");
        
        st_mamta1.executeUpdate(
            " drop table t11ViewTest");
        
        // set connection mamta1
        // View tests test5 Create a view that relies on a SELECT 
        // privilege on only one column of a table. revoke SELECT 
        // privilege on  another column in that table and it ends 
        // up dropping the view. This is happening because the 
        // revoke privilege  work is not completely finished and 
        // any dependent object on that permission type for table's 
        // columns  get dropped when a revoke privilege is issued 
        // against any column of that table
        
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t11ViewTest");
        
        st_mamta1.executeUpdate(
            " create table t11ViewTest (c111 int not null "
            + "primary key, c112 int)");
        
        st_mamta1.executeUpdate(
            " insert into t11ViewTest values(1,1)");
        
        st_mamta1.executeUpdate(
            " grant select (c111, c112) on t11ViewTest to mamta2");
        
        // set connection mamta2
        
        assertStatementError("X0X05", st_mamta2,
            " drop view v21ViewTest");
        
        st_mamta2.executeUpdate(
            " create view v21ViewTest as select c111 from "
            + "mamta1.t11ViewTest");
        
        // set connection mamta1
        //ij(MAMTA2)> -- notice that the view above needs SELECT 
        // privilege on column c111 of mamta1.t11ViewTest and does 
        // not care about column c112
        
        
        // the revoke below ends up dropping the view 
        // mamta2.v21ViewTest eventhough the view does not depend 
        // on column c112 This will be fixed in a subsequent patch 
        // for revoke privilege
        
        st_mamta1.executeUpdate(
            "revoke select (c111) on t11ViewTest from mamta2");
        
        // set connection mamta2
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_mamta1 != null))
                sqlWarn = st_mamta1.getWarnings();
            if (sqlWarn == null)
                sqlWarn = mamta1.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01501", sqlWarn);
            sqlWarn = null;
        }
        
        
        assertStatementError("42X05", st_mamta2,
            " select * from v21ViewTest");
        
        // set connection mamta1
        //ij(MAMTA2)> -- cleanup
        
        
        st_mamta1.executeUpdate(
            " drop table t11ViewTest");
        
        // set connection mamta1
        // View tests test6  Create a view that requires a 
        // privilege. grant select on the view to another user.    
        // Let that user create a trigger based on the granted 
        // view.    Now if the privilege is revoked from the view 
        // owner, the view gets dropped, as    expected. But I had 
        // also expected the trigger to fail the next time it gets 
        // fired    because view used by it doesn't exist anymore. 
        // But because of a bug in Derby,    DERBY-1613(A trigger 
        // does not get invalidated when the view used by it is 
        // dropped),    during some runs of this test, the trigger 
        // continues to fire successfully and    during other runs 
        // of this test, it gives the error that the view does    
        // not exist anymore. Seems like this is timing related 
        // issue. So, may see    diffs in this particular test 
        // until DERBY-1613 is resolved. After the    resolution of 
        // DERBY-1613, the insert trigger will always fail after 
        // the view    gets dropped because of the revoke privilege.
        
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t11TriggerTest");
        
        st_mamta1.executeUpdate(
            " create table t11TriggerTest (c111 int not null "
            + "primary key, c112 int)");
        
        st_mamta1.executeUpdate(
            " insert into t11TriggerTest values(1,1)");
        
        st_mamta1.executeUpdate(
            " insert into t11TriggerTest values(2,2)");
        
        st_mamta1.executeUpdate(
            " grant select on t11TriggerTest to mamta2");
        
        // set connection mamta2
        
        st_mamta2.executeUpdate(
            " create view v21ViewTest as select * from "
            + "mamta1.t11TriggerTest");
        
        // should fail
        
        assertStatementError("4250A", st_mamta2,
            "grant select on v21ViewTest to mamta3");
        
        rs = st_mamta2.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select * from v21ViewTest order by c111");
        
        expColNames = new String [] {"C111", "C112"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"},
            {"2", "2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta3
        
        assertStatementError("42Y55", st_mamta3,
            " drop table t31TriggerTest");
        
        st_mamta3.executeUpdate(
            " create table t31TriggerTest (c311 int)");
        
        assertStatementError("42Y55", st_mamta3,
            " drop table t32TriggerTest");
        
        st_mamta3.executeUpdate(
            " create table t32TriggerTest (c321 int)");
        
        // following should fail because not all the privileges 
        // are in place
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_mamta3,
            "create trigger tr31t31TriggerTest after insert on "
            + "t31TriggerTest for each statement insert into "
            + "t32TriggerTest values (select c111 from "
            + "mamta2.v21ViewTest where c112=1)");
        
        st_mamta3.executeUpdate(
            " insert into t31TriggerTest values(1)");
        
        rs = st_mamta3.executeQuery(
            " select * from t31TriggerTest");
        
        expColNames = new String [] {"C311"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_mamta3.executeQuery(
            " select * from t32TriggerTest");
        
        expColNames = new String [] {"C321"};
        JDBC.assertColumnNames(rs, expColNames);
        
        JDBC.assertEmpty(rs);
        
        // set connection mamta1
        
        // This will drop the dependent view
        
        st_mamta1.executeUpdate(
            "revoke select on t11TriggerTest from mamta2");
        
        // set connection mamta2
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_mamta1 != null))
                sqlWarn = st_mamta1.getWarnings();
            if (sqlWarn == null)
                sqlWarn = mamta1.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01501", sqlWarn);
            sqlWarn = null;
        }
        
        
        assertStatementError("42X05", st_mamta2,
            " select * from v21ViewTest");
        
        // set connection mamta3
        
        // During some runs of this test, the trigger continues to 
        // fire even though the view used by it  has been dropped. 
        // (DERBY-1613) During other runs of this test, the trigger 
        // gives error as expected about the missing view.  After 
        // DERBY-1613 is fixed, we should consistently get error 
        // from insert below because the  insert trigger can't find 
        // the view it uses.
        
        st_mamta3.executeUpdate(
            "insert into t31TriggerTest values(1)");
        
        rs = st_mamta3.executeQuery(
            " select * from t31TriggerTest");
        
        expColNames = new String [] {"C311"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_mamta3.executeQuery(
            " select * from t32TriggerTest");
        
        expColNames = new String [] {"C321"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertEmpty(rs);
        
        // set connection mamta3
        // cleanup
        
        
        st_mamta3.executeUpdate(
            " drop table t31TriggerTest");
        
        st_mamta3.executeUpdate(
            " drop table t32TriggerTest");
        
        // set connection mamta1
        
        st_mamta1.executeUpdate(
            " drop table t11TriggerTest");
        
        // set connection mamta1
        // View tests test7 - negative test  Create a view that 
        // relies on a user level table privilege. The view will 
        // depend on the user level table privilege.     Later 
        // grant the table privilege at the PUBLIC level too. So, 
        // there are 2 privileges available and the view     relies 
        // on one of those privileges. Later, revoke the user level 
        // table privilege. This will end up dropping the     view 
        // although there is another privilege available at PUBLIC 
        // level which can cover the view's requirements of     
        // privileges. But Derby does not support this automatic 
        // switching of privilege reliance on another available     
        // privilege when revoke is issued. DERBY-1632
        
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t11ViewTest");
        
        st_mamta1.executeUpdate(
            " create table t11ViewTest (c111 int not null primary key)");
        
        st_mamta1.executeUpdate(
            " insert into t11ViewTest values(1)");
        
        st_mamta1.executeUpdate(
            " insert into t11ViewTest values(2)");
        
        st_mamta1.executeUpdate(
            " grant select on t11ViewTest to mamta2");
        
        // set connection mamta2
        
        assertStatementError("X0X05", st_mamta2,
            " drop view v21ViewTest");
        
        st_mamta2.executeUpdate(
            " create view v21ViewTest as select * from mamta1.t11ViewTest");
        
        rs = st_mamta2.executeQuery(
            " select * from v21ViewTest order by c111");
        
        expColNames = new String [] {"C111"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta1
        
        // grant the privilege required by mamta2.v21ViewTest at 
        // PUBLIC level
        
        st_mamta1.executeUpdate(
            "grant select on t11ViewTest to PUBLIC");
        
        // now revoke the privilege that view is currently 
        // dependent on. This will end up dropping the view even 
        // though there is   same privilege available at the PUBLIC 
        // level
        
        st_mamta1.executeUpdate(
            "revoke select on t11ViewTest from mamta2");
        
        // set connection mamta2
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_mamta1 != null))
                sqlWarn = st_mamta1.getWarnings();
            if (sqlWarn == null)
                sqlWarn = mamta1.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01501", sqlWarn);
            sqlWarn = null;
        }
        
        
        // view doesn't exist anymore
        
        assertStatementError("42X05", st_mamta2,
            "select * from v21ViewTest");
        
        // Issuing the create view again will work because 
        // required privilege is available at PUBLIC level
        
        st_mamta2.executeUpdate(
            "create view v21ViewTest as select * from mamta1.t11ViewTest");
        
        // view is back in action
        
        rs = st_mamta2.executeQuery(
            "select * from v21ViewTest order by c111");
        
        expColNames = new String [] {"C111"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta1
        
        // verify that view above is dependent on PUBLIC level 
        // privilege, revoke the PUBLIC level privilege and   check 
        // if the view got dropped automatically
        
        st_mamta1.executeUpdate(
            "revoke select on t11ViewTest from PUBLIC");
        
        // set connection mamta2
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_mamta1 != null))
                sqlWarn = st_mamta1.getWarnings();
            if (sqlWarn == null)
                sqlWarn = mamta1.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01501", sqlWarn);
            sqlWarn = null;
        }
        
        
        // view doesn't exist anymore
        
        assertStatementError("42X05", st_mamta2,
            "select * from v21ViewTest");
        
        // set connection mamta1
        //ij(MAMTA2)> --cleanup
        
        
        st_mamta1.executeUpdate(
            " drop table t11ViewTest");
        
        // set connection mamta1
        // View tests test8 - negative test  This test is similar 
        // to test7 above. Create a view that relies on a column 
        // level privilege. Later on, grant the    same privilege 
        // at table level. Now, revoke the column level privilege. 
        // The view will get dropped automatically even    though 
        // there is a covering privilege available at the table 
        // level.(DERBY-1632)
        
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t11ViewTest");
        
        st_mamta1.executeUpdate(
            " create table t11ViewTest (c111 int not null primary key)");
        
        st_mamta1.executeUpdate(
            " insert into t11ViewTest values(1)");
        
        st_mamta1.executeUpdate(
            " insert into t11ViewTest values(2)");
        
        st_mamta1.executeUpdate(
            " grant select(c111) on t11ViewTest to mamta2");
        
        // set connection mamta2
        
        assertStatementError("X0X05", st_mamta2,
            " drop view v21ViewTest");
        
        st_mamta2.executeUpdate(
            " create view v21ViewTest as select c111 from "
            + "mamta1.t11ViewTest");
        
        // set connection mamta1
        
        // grant the privilege required by mamta2.v21ViewTest at 
        // table level
        
        st_mamta1.executeUpdate(
            "grant select on t11ViewTest to mamta2");
        
        // now revoke the privilege that view is currently 
        // dependent on. This will end up dropping the view even 
        // though there is   same privilege available at the table 
        // level
        
        st_mamta1.executeUpdate(
            "revoke select(c111) on t11ViewTest from mamta2");
        
        // set connection mamta2
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_mamta1 != null))
                sqlWarn = st_mamta1.getWarnings();
            if (sqlWarn == null)
                sqlWarn = mamta1.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01501", sqlWarn);
            sqlWarn = null;
        }
        
        
        // view doesn't exist anymore
        
        assertStatementError("42X05", st_mamta2,
            "select * from v21ViewTest");
        
        // Issuing the create view again will work because 
        // required privilege is available at table level
        
        st_mamta2.executeUpdate(
            "create view v21ViewTest as select * from mamta1.t11ViewTest");
        
        // view is back in action
        
        rs = st_mamta2.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            "select * from v21ViewTest order by c111");
        
        expColNames = new String [] {"C111"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta1
        
        // verify that view above is dependent on table level 
        // privilege, revoke the table level privilege and   check 
        // if the view got dropped automatically
        
        st_mamta1.executeUpdate(
            "revoke select on t11ViewTest from mamta2");
        
        // set connection mamta2
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_mamta1 != null))
                sqlWarn = st_mamta1.getWarnings();
            if (sqlWarn == null)
                sqlWarn = mamta1.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01501", sqlWarn);
            sqlWarn = null;
        }
        
        
        // view doesn't exist anymore
        
        assertStatementError("42X05", st_mamta2,
            "select * from v21ViewTest");
        
        // set connection mamta1
        //ij(MAMTA2)> --cleanup
        
        
        st_mamta1.executeUpdate(
            " drop table t11ViewTest");
        
        // set connection mamta1
        // View tests test9 - negative test Have SELECT privilege 
        // available both at column level and table level. When an 
        // object is created which requires the  SELECT privilege, 
        // Derby is designed to pick up the table level privilege 
        // first. Later, when the table level  privilege is revoke, 
        // the object gets dropped. The object really should start 
        // depending on the available column  level privilege. 
        // DERBY-1632
        
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t11ViewTest");
        
        st_mamta1.executeUpdate(
            " create table t11ViewTest (c111 int not null primary key)");
        
        st_mamta1.executeUpdate(
            " insert into t11ViewTest values(1)");
        
        st_mamta1.executeUpdate(
            " insert into t11ViewTest values(2)");
        
        st_mamta1.executeUpdate(
            " grant select(c111) on t11ViewTest to mamta2");
        
        st_mamta1.executeUpdate(
            " grant select on t11ViewTest to mamta2");
        
        // set connection mamta2
        
        assertStatementError("X0X05", st_mamta2,
            " drop view v21ViewTest");
        
        // this view will depend on the table level SELECT privilege
        
        st_mamta2.executeUpdate(
            "create view v21ViewTest as select c111 from "
            + "mamta1.t11ViewTest");
        
        // set connection mamta1
        
        // this ends up dropping the view mamta2.v21ViewTest 
        // (DERBY-1632). Instead, the view should have started 
        // depending on the available  column level SELECT privilege.
        
        st_mamta1.executeUpdate(
            "revoke select on t11ViewTest from mamta2");
        
        // set connection mamta2
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_mamta1 != null))
                sqlWarn = st_mamta1.getWarnings();
            if (sqlWarn == null)
                sqlWarn = mamta1.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01501", sqlWarn);
            sqlWarn = null;
        }
        
        
        // view doesn't exist anymore
        
        assertStatementError("42X05", st_mamta2,
            "select * from v21ViewTest");
        
        // set connection mamta1
        //ij(MAMTA2)> --cleanup
        
        
        st_mamta1.executeUpdate(
            " drop table t11ViewTest");
        
        // set connection mamta1
        // View tests test10 - negative test  Create a view that 
        // relies on some privileges. Create another view based on 
        // that view. A revoke privilege on privilege    required 
        // by the first view will fail because there is another 
        // view dependent on the first view. This is because    
        // Derby currently does not support cascade view drop 
        // (DERBY-1631)
        
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t11ViewTest");
        
        st_mamta1.executeUpdate(
            " create table t11ViewTest (c111 int not null primary key)");
        
        st_mamta1.executeUpdate(
            " insert into t11ViewTest values(1)");
        
        st_mamta1.executeUpdate(
            " insert into t11ViewTest values(2)");
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t12ViewTest");
        
        st_mamta1.executeUpdate(
            " create table t12ViewTest (c121 int, c122 char)");
        
        st_mamta1.executeUpdate(
            " insert into t12ViewTest values (1,'1')");
        
        // grant permissions to mamta2 so mamta2 can create a view 
        // based on these objects
        
        st_mamta1.executeUpdate(
            "grant select on t11ViewTest to mamta2");
        
        st_mamta1.executeUpdate(
            " grant select on t12ViewTest to mamta2");
        
        // set connection mamta2
        
        st_mamta2.executeUpdate(
            " create view v21ViewTest as select t1.c111, t2.c122 "
            + "from mamta1.t11ViewTest as t1, mamta1.t12ViewTest as t2");
        
        rs = st_mamta2.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select * from v21ViewTest order by c111");
        
        expColNames = new String [] {"C111", "C122"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"},
            {"2", "1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // grant permission to mamta3, should fail
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("4250A", st_mamta2,
            "grant select on v21ViewTest to mamta3");
        
        // set connection mamta3
        
        assertStatementError("42502", st_mamta3,
            " create view v31ViewTest as select * from mamta2.v21ViewTest");
        
        assertStatementError("42X05", st_mamta3,
            " select * from v31ViewTest");
        
        // set connection mamta1
        
        // revoke the privilege from mamta2, should be ok, 
        // previous view is not created.
        
        st_mamta1.executeUpdate(
            "revoke select on t11ViewTest from mamta2");
        
        // set connection mamta2
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_mamta1 != null))
                sqlWarn = st_mamta1.getWarnings();
            if (sqlWarn == null)
                sqlWarn = mamta1.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01501", sqlWarn);
            sqlWarn = null;
        }
        
        
        // this view is not created, should fail
        
        assertStatementError("42X05", st_mamta2,
            "select * from v21ViewTest");
        
        // set connection mamta3
        
        // drop the dependent view
        
        assertStatementError("X0X05", st_mamta3,
            "drop view v31ViewTest");
        
        // set connection mamta1
        
        // revoke privilege will succeed this time and will drop 
        // the dependent view on that privilege
        
        st_mamta1.executeUpdate(
            "revoke select on t11ViewTest from mamta2");
        
        // set connection mamta2
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_mamta1 != null))
                sqlWarn = st_mamta1.getWarnings();
            if (sqlWarn == null)
                sqlWarn = mamta1.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01006", sqlWarn);
            sqlWarn = null;
        }
        
        
        // view doesn't exist anymore
        
        assertStatementError("42X05", st_mamta2,
            "select * from v21ViewTest");
        
        // set connection mamta1
        //ij(MAMTA2)> -- cleanup
        
        
        st_mamta1.executeUpdate(
            " drop table t12ViewTest");
        
        st_mamta1.executeUpdate(
            " drop table t11ViewTest");
        
        // set connection mamta1
        // Constraint test test1 Give a constraint privilege at 
        // table level to a user. Let user define a foreign key 
        // constraint based on that privilege.  Later revoke that 
        // references privilege and make sure that foreign key 
        // constraint gets dropped
        
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t11ConstraintTest");
        
        st_mamta1.executeUpdate(
            " create table t11ConstraintTest (c111 int not null "
            + "primary key)");
        
        st_mamta1.executeUpdate(
            " insert into t11ConstraintTest values(1)");
        
        st_mamta1.executeUpdate(
            " insert into t11ConstraintTest values(2)");
        
        st_mamta1.executeUpdate(
            " grant references on t11ConstraintTest to mamta2");
        
        // set connection mamta2
        
        assertStatementError("42Y55", st_mamta2,
            " drop table t21ConstraintTest");
        
        st_mamta2.executeUpdate(
            " create table t21ConstraintTest (c211 int "
            + "references mamta1.t11ConstraintTest, c212 int)");
        
        st_mamta2.executeUpdate(
            " insert into t21ConstraintTest values(1,1)");
        
        // should fail because the foreign key constraint will fail
        
        assertStatementError("23503", st_mamta2,
            "insert into t21ConstraintTest values(3,1)");
        
        // set connection mamta1
        
        st_mamta1.executeUpdate(
            " revoke references on t11ConstraintTest from mamta2");
        
        // set connection mamta2
        
        // will pass because the foreign key constraint got 
        // dropped because of revoke statement
        
        st_mamta2.executeUpdate(
            "insert into t21ConstraintTest values(3,1)");
        
        // set connection mamta2
        // cleanup
        
        
        st_mamta2.executeUpdate(
            " drop table t21ConstraintTest");
        
        // set connection mamta1
        
        st_mamta1.executeUpdate(
            " drop table t11ConstraintTest");
        
        // set connection mamta1
        // Constraint test test2 Have user mamta1 give a 
        // references privilege to mamta3. Have user mamta2 give a 
        // references privilege to mamta3. Have mamta3 create a 
        // table with 2 foreign key constraints relying on both 
        // these granted privileges. Revoke one of those privileges 
        // and make sure that the foreign key constraint defined 
        // based on that privilege gets dropped. Now revoke the 2nd 
        // references privilege and make sure that remaining 
        // foreign key constraint gets dropped
        
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t11ConstraintTest");
        
        st_mamta1.executeUpdate(
            " create table t11ConstraintTest (c111 int not null "
            + "primary key)");
        
        st_mamta1.executeUpdate(
            " insert into t11ConstraintTest values(1)");
        
        st_mamta1.executeUpdate(
            " insert into t11ConstraintTest values(2)");
        
        st_mamta1.executeUpdate(
            " grant references on t11ConstraintTest to mamta3");
        
        // set connection mamta2
        
        assertStatementError("42Y55", st_mamta2,
            " drop table t21ConstraintTest");
        
        st_mamta2.executeUpdate(
            " create table t21ConstraintTest (c111 int not null "
            + "primary key)");
        
        st_mamta2.executeUpdate(
            " insert into t21ConstraintTest values(1)");
        
        st_mamta2.executeUpdate(
            " insert into t21ConstraintTest values(2)");
        
        st_mamta2.executeUpdate(
            " grant references on t21ConstraintTest to mamta3");
        
        // set connection mamta3
        
        assertStatementError("42Y55", st_mamta3,
            " drop table t31ConstraintTest");
        
        st_mamta3.executeUpdate(
            " create table t31ConstraintTest (c311 int "
            + "references mamta1.t11ConstraintTest, c312 int "
            + "references mamta2.t21ConstraintTest)");
        
        rs = st_mamta3.executeQuery(
            " select * from t31ConstraintTest");
        
        expColNames = new String [] {"C311", "C312"};
        JDBC.assertColumnNames(rs, expColNames);
        
        JDBC.assertEmpty(rs);
        
        st_mamta3.executeUpdate(
            " insert into t31ConstraintTest values(1,1)");
        
        // following should fail because it violates the foreign 
        // key reference by column c312
        
        assertStatementError("23503", st_mamta3,
            "insert into t31ConstraintTest values(1,3)");
        
        // following should fail because it violates the foreign 
        // key reference by column c311
        
        assertStatementError("23503", st_mamta3,
            "insert into t31ConstraintTest values(3,1)");
        
        // following should fail because it violates the foreign 
        // key reference by column c311 and c312
        
        assertStatementError("23503", st_mamta3,
            "insert into t31ConstraintTest values(3,4)");
        
        // set connection mamta2
        
        // the following revoke should drop the foreign key 
        // reference by column t31ConstraintTest.c312
        
        st_mamta2.executeUpdate(
            "revoke references on t21ConstraintTest from mamta3");
        
        // set connection mamta3
        
        // verify that foreign key reference by column 
        // t31ConstraintTest.c312 got dropped by inserting a row. 
        // following should pass
        
        st_mamta3.executeUpdate(
            "insert into t31ConstraintTest values(1,3)");
        
        // following should still fail because foreign key 
        // reference by column c311 is still around
        
        assertStatementError("23503", st_mamta3,
            "insert into t31ConstraintTest values(3,1)");
        
        // set connection mamta1
        
        // now drop the references privilege so that the only 
        // foreign key reference on table mamta3.t31ConstraintTest 
        // will get dropped
        
        st_mamta1.executeUpdate(
            "revoke references on t11ConstraintTest from mamta3");
        
        // set connection mamta3
        
        // verify that foreign key reference by column 
        // t31ConstraintTest.c311 got dropped by inserting a row. 
        // following should pass
        
        st_mamta3.executeUpdate(
            "insert into t31ConstraintTest values(3,1)");
        
        // no more foreign key references left and hence following 
        // should pass
        
        st_mamta3.executeUpdate(
            "insert into t31ConstraintTest values(3,3)");
        
        // cleanup
        
        st_mamta3.executeUpdate(
            "drop table t31ConstraintTest");
        
        // set connection mamta2
        
        st_mamta2.executeUpdate(
            " drop table t21ConstraintTest");
        
        // set connection mamta1
        
        st_mamta1.executeUpdate(
            " drop table t11ConstraintTest");
        
        // set connection mamta1
        // Constraint test test3 Have mamta1 grant REFERENCES 
        // privilege on one of it's tables to mamta2 Have mamta2 
        // create a table with primary which references mamta1's 
        // granted REFERENCES privilege Have mamta2 grant 
        // REFERENCES privilege on that table to user mamta3 Have 
        // mamta3 create a table which references mamta2's granted 
        // REFERENCES privilege Now revoke of granted REFERENCES 
        // privilege by mamta1 should drop the foreign key 
        // reference  by mamta2's table t21ConstraintTest. It 
        // should not impact the foreign key reference by  mamta3's 
        // table t31ConstraintTest. a)mamta1.t11ConstraintTest 
        // (primary key) b)mamta2.t21ConstraintTest (primary key 
        // references t11ConstraintTest) c)mamta3.t31ConstraintTest 
        // (primary key references t21ConstraintTest)
        
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t11ConstraintTest");
        
        st_mamta1.executeUpdate(
            " create table t11ConstraintTest (c111 int not null "
            + "primary key)");
        
        st_mamta1.executeUpdate(
            " insert into t11ConstraintTest values(1)");
        
        st_mamta1.executeUpdate(
            " insert into t11ConstraintTest values(2)");
        
        st_mamta1.executeUpdate(
            " grant references on t11ConstraintTest to mamta2");
        
        // set connection mamta2
        
        assertStatementError("42Y55", st_mamta2,
            " drop table t21ConstraintTest");
        
        st_mamta2.executeUpdate(
            " create table t21ConstraintTest (c111 int not null "
            + "primary key references mamta1.t11ConstraintTest)");
        
        st_mamta2.executeUpdate(
            " insert into t21ConstraintTest values(1)");
        
        st_mamta2.executeUpdate(
            " insert into t21ConstraintTest values(2)");
        
        // following should fail because of foreign key constraint 
        // failure
        
        assertStatementError("23503", st_mamta2,
            "insert into t21ConstraintTest values(3)");
        
        st_mamta2.executeUpdate(
            " grant references on t21ConstraintTest to mamta3");
        
        // set connection mamta3
        
        assertStatementError("42Y55", st_mamta3,
            " drop table t31ConstraintTest");
        
        st_mamta3.executeUpdate(
            " create table t31ConstraintTest (c311 int "
            + "references mamta2.t21ConstraintTest)");
        
        rs = st_mamta3.executeQuery(
            " select * from t31ConstraintTest");
        
        expColNames = new String [] {"C311"};
        JDBC.assertColumnNames(rs, expColNames);
        
        JDBC.assertEmpty(rs);
        
        st_mamta3.executeUpdate(
            " insert into t31ConstraintTest values (1)");
        
        // following should fail because of foreign key constraint 
        // failure
        
        assertStatementError("23503", st_mamta3,
            "insert into t31ConstraintTest values (4)");
        
        // set connection mamta1
        
        // This revoke should drop foreign key constraint on 
        // mamta2.t21ConstraintTest   This revoke should not impact 
        // the foeign key constraint on mamta3.t31ConstraintTest
        
        st_mamta1.executeUpdate(
            "revoke references on t11ConstraintTest from mamta2");
        
        // set connection mamta2
        
        // because the foreign key reference got revoked, no 
        // constraint violation check will be done
        
        st_mamta2.executeUpdate(
            "insert into t21ConstraintTest values(3)");
        
        // set connection mamta3
        
        // Make sure the foreign key constraint on 
        // t31ConstraintTest is still active
        
        st_mamta3.executeUpdate(
            "insert into t31ConstraintTest values(3)");
        
        // because the foreign key constraint is still around, 
        // following should fail
        
        assertStatementError("23503", st_mamta3,
            "insert into t31ConstraintTest values(4)");
        
        // set connection mamta3
        // cleanup
        
        
        st_mamta3.executeUpdate(
            " drop table t31ConstraintTest");
        
        // set connection mamta2
        
        st_mamta2.executeUpdate(
            " drop table t21ConstraintTest");
        
        // set connection mamta1
        
        st_mamta1.executeUpdate(
            " drop table t11ConstraintTest");
        
        // set connection mamta1
        // Constraint test test4 Grant a REFERENCES permission at 
        // public level, create constraint, grant same permission 
        // at user level   and take away the public level 
        // permission. It ends up dropping the constraint. DERBY-1632
        
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t11ConstraintTest");
        
        st_mamta1.executeUpdate(
            " create table t11ConstraintTest (c111 int not null "
            + "primary key)");
        
        st_mamta1.executeUpdate(
            " insert into t11ConstraintTest values(1)");
        
        st_mamta1.executeUpdate(
            " insert into t11ConstraintTest values(2)");
        
        st_mamta1.executeUpdate(
            " grant references on t11ConstraintTest to PUBLIC");
        
        // set connection mamta2
        
        assertStatementError("42Y55", st_mamta2,
            " drop table t21ConstraintTest");
        
        st_mamta2.executeUpdate(
            " create table t21ConstraintTest (c111 int not null "
            + "primary key, constraint fk foreign key(c111) "
            + "references mamta1.t11ConstraintTest)");
        
        st_mamta2.executeUpdate(
            " insert into t21ConstraintTest values(1)");
        
        st_mamta2.executeUpdate(
            " insert into t21ConstraintTest values(2)");
        
        // following should fail because of foreign key constraint 
        // failure
        
        assertStatementError("23503", st_mamta2,
            "insert into t21ConstraintTest values(3)");
        
        // set connection mamta1
        
        // grant REFERENCES permission again but this time at user 
        // level
        
        st_mamta1.executeUpdate(
            "grant references on t11ConstraintTest to mamta2");
        
        // Now, revoke REFERENCES permission which was granted at 
        // PUBLIC level, This drops the constraint.   DERBY-1632. 
        // This should be fixed at some point so that constraint 
        // won't get dropped, instead   it will start depending on 
        // same privilege available at user-level
        
        st_mamta1.executeUpdate(
            "revoke references on t11ConstraintTest from PUBLIC");
        
        // set connection mamta2
        
        // because the foreign key reference got revoked, no 
        // constraint violation check will be done
        
        st_mamta2.executeUpdate(
            "insert into t21ConstraintTest values(3)");
        
        // set connection mamta2
        // cleanup
        
        
        st_mamta2.executeUpdate(
            " drop table t21ConstraintTest");
        
        // set connection mamta1
        
        st_mamta1.executeUpdate(
            " drop table t11ConstraintTest");
        
        // set connection mamta1
        // Constraint test test5 Grant refrences privilege and 
        // select privilege on a table. Have a constraint depend on 
        // the references   privilege. Later, a revoke of select 
        // privilege will end up dropping the constraint which 
        // shouldn't   happen. This will be addressed in a 
        // subsequent patch
        
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t11ConstraintTest");
        
        st_mamta1.executeUpdate(
            " create table t11ConstraintTest (c111 int not null "
            + "primary key)");
        
        st_mamta1.executeUpdate(
            " insert into t11ConstraintTest values(1)");
        
        st_mamta1.executeUpdate(
            " insert into t11ConstraintTest values(2)");
        
        st_mamta1.executeUpdate(
            " grant references on t11ConstraintTest to PUBLIC");
        
        st_mamta1.executeUpdate(
            " grant select on t11ConstraintTest to PUBLIC");
        
        // set connection mamta2
        
        assertStatementError("42Y55", st_mamta2,
            " drop table t21ConstraintTest");
        
        st_mamta2.executeUpdate(
            " create table t21ConstraintTest (c111 int not null "
            + "primary key, constraint fk foreign key(c111)   "
            + "references mamta1.t11ConstraintTest)");
        
        st_mamta2.executeUpdate(
            " insert into t21ConstraintTest values(1)");
        
        st_mamta2.executeUpdate(
            " insert into t21ConstraintTest values(2)");
        
        // following should fail because of foreign key constraint 
        // failure
        
        assertStatementError("23503", st_mamta2,
            "insert into t21ConstraintTest values(3)");
        
        // set connection mamta1
        
        // revoke of select privilege is going to drop the 
        // constraint which is incorrect. Will be handled in a 
        // later patch
        
        st_mamta1.executeUpdate(
            "revoke select on t11ConstraintTest from PUBLIC");
        
        // set connection mamta2
        
        // following should have failed but it doesn't because 
        // foreign key constraint got dropped by revoke select 
        // privilege Will be fixed in a subsequent patch
        
        st_mamta2.executeUpdate(
            "insert into t21ConstraintTest values(3)");
        
        // set connection mamta2
        // cleanup
        
        
        st_mamta2.executeUpdate(
            " drop table t21ConstraintTest");
        
        // set connection mamta1
        
        st_mamta1.executeUpdate(
            " drop table t11ConstraintTest");
        
        // set connection mamta1
        // Constraint test test6 Have a primary key and a unique 
        // key on a table and grant reference on both. Have another 
        // table rely on unique  key references privilege to create 
        // a foreign key constraint. Later, the revoke of primary 
        // key reference will end up  dropping the foreign key 
        // constraint. This will be fixed in a subsequent patch 
        // (same as test5)
        
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t11ConstraintTest");
        
        st_mamta1.executeUpdate(
            " create table t11ConstraintTest (c111 int not null "
            + "primary key, c112 int not null unique, c113 int)");
        
        st_mamta1.executeUpdate(
            " insert into t11ConstraintTest values(1,1,1)");
        
        st_mamta1.executeUpdate(
            " insert into t11ConstraintTest values(2,2,1)");
        
        st_mamta1.executeUpdate(
            " grant references(c111, c112) on t11ConstraintTest to PUBLIC");
        
        // set connection mamta2
        
        assertStatementError("42Y55", st_mamta2,
            " drop table t21ConstraintTest");
        
        st_mamta2.executeUpdate(
            " create table t21ConstraintTest (c111 int not null "
            + "primary key, constraint fk foreign key(c111)   "
            + "references mamta1.t11ConstraintTest(c112))");
        
        st_mamta2.executeUpdate(
            " insert into t21ConstraintTest values(1)");
        
        st_mamta2.executeUpdate(
            " insert into t21ConstraintTest values(2)");
        
        // following should fail because of foreign key constraint 
        // failure
        
        assertStatementError("23503", st_mamta2,
            "insert into t21ConstraintTest values(3)");
        
        // set connection mamta1
        
        // revoke of references privilege on c111 which is not 
        // used by foreign key constraint on t21ConstraintTest ends 
        // up dropping that  foreign key constraint. This Will be 
        // handled in a later patch
        
        st_mamta1.executeUpdate(
            "revoke references(c111) on t11ConstraintTest from PUBLIC");
        
        // set connection mamta2
        
        // following should have failed but it doesn't because 
        // foreign key constraint got dropped by revoke references 
        // privilege Will be fixed in a subsequent patch
        
        st_mamta2.executeUpdate(
            "insert into t21ConstraintTest values(3)");
        
        // set connection mamta2
        // cleanup
        
        
        st_mamta2.executeUpdate(
            " drop table t21ConstraintTest");
        
        // set connection mamta1
        
        st_mamta1.executeUpdate(
            " drop table t11ConstraintTest");
        
        // set connection mamta1
        // Miscellaneous test test1 Have multiple objects depends 
        // on a privilege and make sure they all get dropped when 
        // that privilege is revoked.
        
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t11MiscTest");
        
        st_mamta1.executeUpdate(
            " create table t11MiscTest (c111 int, c112 int, c113 int)");
        
        st_mamta1.executeUpdate(
            " grant select, update, trigger on t11MiscTest to "
            + "mamta2, mamta3");
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t12MiscTest");
        
        st_mamta1.executeUpdate(
            " create table t12MiscTest (c121 int, c122 int)");
        
        st_mamta1.executeUpdate(
            " grant select on t12MiscTest to mamta2");
        
        // set connection mamta2
        
        assertStatementError("X0X05", st_mamta2,
            " drop view v21MiscTest");
        
        st_mamta2.executeUpdate(
            " create view v21MiscTest as select * from "
            + "mamta1.t11MiscTest, mamta1.t12MiscTest where c111=c121");
        
        rs = st_mamta2.executeQuery(
            " select * from v21MiscTest");
        
        expColNames = new String [] {"C111", "C112", "C113", "C121", "C122"};
        JDBC.assertColumnNames(rs, expColNames);
        
        JDBC.assertEmpty(rs);
        
        // set connection mamta3
        
        assertStatementError("X0X05", st_mamta3,
            " drop view v31MiscTest");
        
        st_mamta3.executeUpdate(
            " create view v31MiscTest as select c111 from "
            + "mamta1.t11MiscTest");
        
        rs = st_mamta3.executeQuery(
            " select * from v31MiscTest");
        
        expColNames = new String [] {"C111"};
        JDBC.assertColumnNames(rs, expColNames);
        
        JDBC.assertEmpty(rs);
        
        // set connection mamta1
        
        // this should drop both the dependent views
        
        st_mamta1.executeUpdate(
            "revoke select, update on t11MiscTest from mamta2, mamta3");
        
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_mamta1 != null))
                sqlWarn = st_mamta1.getWarnings();
            if (sqlWarn == null)
                sqlWarn = mamta1.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01501", sqlWarn);
            sqlWarn = null;
        }
        
        // set connection mamta2
        
        // should fail because it got dropped as part of revoke 
        // statement
        
        assertStatementError("42X05", st_mamta2,
            "select * from v21MiscTest");
        
        // set connection mamta3
        
        // should fail because it got dropped as part of revoke 
        // statement
        
        assertStatementError("42X05", st_mamta3,
            "select * from v31MiscTest");
        
        // set connection mamta1
        //ij(MAMTA3)> -- cleanup
        
        
        st_mamta1.executeUpdate(
            " drop table t11MiscTest");
        
        st_mamta1.executeUpdate(
            " drop table t12MiscTest");
        
        // set connection mamta1
        // create trigger privilege collection TriggerTest first 
        // grant one column level privilege at user level and 
        // another at public level and then define the trigger
        
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t11TriggerTest");
        
        st_mamta1.executeUpdate(
            " create table t11TriggerTest (c111 int not null "
            + "primary key, c112 int)");
        
        st_mamta1.executeUpdate(
            " insert into t11TriggerTest values(1,1)");
        
        st_mamta1.executeUpdate(
            " insert into t11TriggerTest values(2,2)");
        
        st_mamta1.executeUpdate(
            " grant select(c111) on t11TriggerTest to mamta2");
        
        st_mamta1.executeUpdate(
            " grant select(c112) on t11TriggerTest to public");
        
        // set connection mamta2
        
        assertStatementError("42Y55", st_mamta2,
            " drop table t21TriggerTest");
        
        st_mamta2.executeUpdate(
            " create table t21TriggerTest (c211 int)");
        
        assertStatementError("42Y55", st_mamta2,
            " drop table t22TriggerTest");
        
        st_mamta2.executeUpdate(
            " create table t22TriggerTest (c221 int)");
        
        // following should pass because all the privileges are in 
        // places
        
        st_mamta2.executeUpdate(
            "create trigger tr21t21TriggerTest after insert on "
            + "t21TriggerTest for each statement insert into "
            + "t22TriggerTest values (select c111 from "
            + "mamta1.t11TriggerTest where c112=1)");
        
        st_mamta2.executeUpdate(
            " insert into t21TriggerTest values(1)");
        
        rs = st_mamta2.executeQuery(
            " select * from t21TriggerTest");
        
        expColNames = new String [] {"C211"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_mamta2.executeQuery(
            " select * from t22TriggerTest");
        
        expColNames = new String [] {"C221"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st_mamta2.executeUpdate(
            " drop table t21TriggerTest");
        
        st_mamta2.executeUpdate(
            " drop table t22TriggerTest");
        
        // set connection mamta1
        //ij(MAMTA2)> -- grant all the privileges at the table 
        // level and then define the trigger
        
        
        st_mamta1.executeUpdate(
            " drop table t11TriggerTest");
        
        st_mamta1.executeUpdate(
            " create table t11TriggerTest (c111 int not null primary key)");
        
        st_mamta1.executeUpdate(
            " insert into t11TriggerTest values(1)");
        
        st_mamta1.executeUpdate(
            " insert into t11TriggerTest values(2)");
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t12RoutineTest");
        
        st_mamta1.executeUpdate(
            " create table t12RoutineTest (c121 int)");
        
        st_mamta1.executeUpdate(
            " insert into t12RoutineTest values (1),(2)");
        
        st_mamta1.executeUpdate(
            " grant select on t11TriggerTest to mamta2");
        
        st_mamta1.executeUpdate(
            " grant insert on t12RoutineTest to mamta2");
        
        rs = st_mamta1.executeQuery(
            " select * from t11TriggerTest order by c111");
        
        expColNames = new String [] {"C111"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_mamta1.executeQuery(
            " select * from t12RoutineTest order by c121");
        
        expColNames = new String [] {"C121"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta2
        
        st_mamta2.executeUpdate(
            " create table t21TriggerTest (c211 int)");
        
        // following should pass because all the privileges are in 
        // places
        
        st_mamta2.executeUpdate(
            "create trigger tr21t21TriggerTest after insert on "
            + "t21TriggerTest for each statement insert into "
            + "mamta1.t12RoutineTest values (select c111 from "
            + "mamta1.t11TriggerTest where c111=1)");
        
        // this insert's trigger will cause a new row in 
        // mamta1.t12RoutineTest
        
        st_mamta2.executeUpdate(
            "insert into t21TriggerTest values(1)");
        
        rs = st_mamta2.executeQuery(
            " select * from t21TriggerTest");
        
        expColNames = new String [] {"C211"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta1
        
        rs = st_mamta1.executeQuery(
            " select * from t11TriggerTest order by c111");
        
        expColNames = new String [] {"C111"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_mamta1.executeQuery(
            " select * from t12RoutineTest order by c121");
        
        expColNames = new String [] {"C121"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"1"},
            {"2"},
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta2
        
        // following should fail because mamta2 doesn't have 
        // trigger permission on mamta1.t11TriggerTest
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42500", st_mamta2,
            "create trigger tr11t11TriggerTest after insert on "
            + "mamta1.t11TriggerTest for each statement insert into "
            + "mamta1.t12RoutineTest values (1)");
        
        // set connection mamta1
        
        st_mamta1.executeUpdate(
            " grant trigger on t11TriggerTest to mamta2");
        
        // set connection mamta2
        
        // following will pass now because mamta2 has trigger 
        // permission on mamta1.t11TriggerTest
        
        st_mamta2.executeUpdate(
            "create trigger tr11t11TriggerTest after insert on "
            + "mamta1.t11TriggerTest for each statement insert into "
            + "mamta1.t12RoutineTest values (1)");
        
        // following will fail becuae mamta2 has TRIGGER privilege 
        // but not INSERT privilege on mamta1.t11TriggerTest
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42500", st_mamta2,
            "insert into mamta1.t11TriggerTest values(3)");
        
        // set connection mamta1
        
        assertUpdateCount(st_mamta1, 2,
            " delete from t11TriggerTest");
        
        assertUpdateCount(st_mamta1, 3,
            " delete from t12RoutineTest");
        
        st_mamta1.executeUpdate(
            " insert into mamta1.t11TriggerTest values(3)");
        
        rs = st_mamta1.executeQuery(
            " select * from t11TriggerTest");
        
        expColNames = new String [] {"C111"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_mamta1.executeQuery(
            " select * from t12RoutineTest");
        
        expColNames = new String [] {"C121"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);

        st_mamta2.executeUpdate(
            " drop trigger tr21t21TriggerTest");

        st_mamta1.executeUpdate(
            " drop table t11TriggerTest");
        
        st_mamta1.executeUpdate(
            " drop table t12RoutineTest");
        
        // set connection mamta1
        // Test routine and trigger combination. Thing to note is 
        // triggers always   run with definer's privileges whereas 
        // routines always run with   session user's privileges
        
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t12RoutineTest");
        
        st_mamta1.executeUpdate(
            " create table t12RoutineTest (c121 int)");
        
        st_mamta1.executeUpdate(
            " insert into t12RoutineTest values (1),(2)");
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t13TriggerTest");
        
        st_mamta1.executeUpdate(
            " create table t13TriggerTest (c131 int)");
        
        st_mamta1.executeUpdate(
            " insert into t13TriggerTest values (1),(2)");
        
        st_mamta1.executeUpdate(
            " grant select on t12RoutineTest to mamta3");
        
        st_mamta1.executeUpdate(
            " grant insert on t13TriggerTest to mamta3");
        
        assertStatementError("42Y55", st_mamta1,
            " drop function selectFromSpecificSchema");
        
        st_mamta1.executeUpdate(
            " CREATE FUNCTION selectFromSpecificSchema (P1 "
            + "INT) RETURNS INT RETURNS NULL ON NULL INPUT EXTERNAL "
            + "NAME "
            + "'org.apache.derbyTesting.functionTests.util.ProcedureTest.selectFromSpecificSchema' LANGUAGE JAVA "
            + "PARAMETER STYLE JAVA");
        
        st_mamta1.executeUpdate(
            " grant execute on function selectFromSpecificSchema "
            + "to mamta3");
        
        // set connection mamta3
        
        assertStatementError("42Y55", st_mamta3,
            " drop table t31TriggerTest");
        
        st_mamta3.executeUpdate(
            " create table t31TriggerTest(c11 int)");
        
        // following will pass because all the required privileges 
        // are in place for mamta3
        
        st_mamta3.executeUpdate(
            "create trigger tr31t31 after insert on "
            + "t31TriggerTest for each statement insert into "
            + "mamta1.t13TriggerTest values (values "
            + "mamta1.selectFromSpecificSchema(1))");
        
        // following insert will cause a row to be inserted into 
        // mamta1.t13TriggerTest if the session user    has SELECT 
        // privilege on mamta1.t12RoutineTest. This shows that 
        // although triggers execute    with definer privileges, 
        // routines always execute with session user's privilege, 
        // even when    called by an object which runs with 
        // definer's privilege
        
        st_mamta3.executeUpdate(
            "insert into t31TriggerTest values(1)");
        
        rs = st_mamta3.executeQuery(
            " select * from t31TriggerTest");
        
        expColNames = new String [] {"C11"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta1
        
        rs = st_mamta1.executeQuery(
            " select * from t12RoutineTest order by c121");
        
        expColNames = new String [] {"C121"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_mamta1.executeQuery(
            " select * from t13TriggerTest order by c131");
        
        expColNames = new String [] {"C131"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"1"},
            {"2"},
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta2
        
        // will fail because mamta2 doesn't have INSERT privilege 
        // on mamta3.t31TriggerTest
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42500", st_mamta2,
            "insert into mamta3.t31TriggerTest values(1)");
        
        // set connection mamta3
        
        st_mamta3.executeUpdate(
            " grant insert on t31TriggerTest to mamta2");
        
        // set connection mamta2
        
        // should still fail because trigger on 
        // mamta3.t31TriggerTest accesses a routine which   
        // accesses a table on which mamta2 doesn't have SELECT 
        // privilege on. mamta3 doesn't   need execute privilege on 
        // routine because it is getting accessed by trigger which 
        // runs   with the definer privilege. But the routine 
        // itself never runs with definer privilege and   hence the 
        // session user needs access to objects accessed by the 
        // routine.
        
        assertStatementError("38000", st_mamta2,
            "insert into mamta3.t31TriggerTest values(1)");
        
        // set connection mamta1
        
        st_mamta1.executeUpdate(
            " grant select on t12RoutineTest to mamta2");
        
        // set connection mamta2
        
        // mamta2 got the SELECT privilege on 
        // mamta1.t12RoutineTest and hence following insert should pass
        
        st_mamta2.executeUpdate(
            "insert into mamta3.t31TriggerTest values(1)");
        
        // set connection mamta3
        
        rs = st_mamta3.executeQuery(
            " select * from t31TriggerTest");
        
        expColNames = new String [] {"C11"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta1
        
        rs = st_mamta1.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select * from t12RoutineTest order by c121");
        
        expColNames = new String [] {"C121"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_mamta1.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select * from t13TriggerTest order by c131");
        
        expColNames = new String [] {"C131"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"1"},
            {"1"},
            {"2"},
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertUpdateCount(st_mamta1, 4,
            " delete from t13TriggerTest");
        
        // Trying to revoke execute privilege below will fail 
        // because mamta3 has created a trigger based on that 
        // permission. Derby supports only RESTRICT form of revoke 
        // execute. Which means that it can be revoked only if 
        // there are no objects relying on that permission
        
        assertStatementError("X0Y25", st_mamta1,
            "revoke execute on function selectFromSpecificSchema "
            + "from mamta3 restrict");
        
        // set connection mamta2
        //ij(MAMTA1)> -- now try the insert and make sure the 
        // insert trigger still fires
        
        
        st_mamta2.executeUpdate(
            " insert into mamta3.t31TriggerTest values(1)");
        
        // set connection mamta1
        
        // If number of rows returned by following select is 1, 
        // then we know insert trigger did get fire. Insert's 
        // trigger's action is to insert into following table.
        
        rs = st_mamta1.executeQuery(
            "select * from t13TriggerTest");
        
        expColNames = new String [] {"C131"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta3
        
        // drop the trigger manually
        
        st_mamta3.executeUpdate(
            "drop trigger tr31t31");
        
        // set connection mamta1
        
        // Now, we should be able to revoke execute permission on 
        // routine because there are no dependent objects on that 
        // permission
        
        st_mamta1.executeUpdate(
            "revoke execute on function selectFromSpecificSchema "
            + "from mamta3 restrict");
        
        // set connection mamta3
        
        // cleanup
        
        st_mamta3.executeUpdate(
            "drop table t31TriggerTest");
        
        // set connection mamta1
        
        // cleanup
        
        st_mamta1.executeUpdate(
            "drop table t12RoutineTest");
        
        st_mamta1.executeUpdate(
            " drop table t13TriggerTest");
        
        st_mamta1.executeUpdate(
            " drop function selectFromSpecificSchema");
        
        // set connection mamta1
        // Test routine and view combination. Thing to note is 
        // views always   run with definer's privileges whereas 
        // routines always run with   session user's privileges. 
        // So, eventhough a routine might be   getting accessed by 
        // a view which is running with definer's   privileges, 
        // during the routine execution, the session user's   
        // privileges will get used.
        
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t12RoutineTest");
        
        st_mamta1.executeUpdate(
            " create table t12RoutineTest (c121 int)");
        
        st_mamta1.executeUpdate(
            " insert into t12RoutineTest values (1),(2)");
        
        st_mamta1.executeUpdate(
            " grant select on t12RoutineTest to mamta3");
        
        assertStatementError("42Y55", st_mamta1,
            " drop function selectFromSpecificSchema");
        
        st_mamta1.executeUpdate(
            " CREATE FUNCTION selectFromSpecificSchema (P1 "
            + "INT) RETURNS INT RETURNS NULL ON NULL INPUT EXTERNAL "
            + "NAME "
            + "'org.apache.derbyTesting.functionTests.util.ProcedureTest.selectFromSpecificSchema' LANGUAGE JAVA "
            + "PARAMETER STYLE JAVA");
        
        st_mamta1.executeUpdate(
            " grant execute on function selectFromSpecificSchema "
            + "to mamta3");
        
        // set connection mamta3
        
        assertStatementError("X0X05", st_mamta3,
            " drop view v21ViewTest");
        
        // following will succeed because mamta3 has EXECUTE 
        // privileges on the function
        
        st_mamta3.executeUpdate(
            "create view v21ViewTest(c211) as values "
            + "mamta1.selectFromSpecificSchema(1)");
        
        rs = st_mamta3.executeQuery(
            " select * from v21ViewTest");
        
        expColNames = new String [] {"C211"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("4250A", st_mamta3,
            " grant select on v21ViewTest to mamta2");
        
        // set connection mamta2
        
        // Although mamta2 has SELECT privileges on 
        // mamta3.v21ViewTest, mamta2 doesn't have    SELECT 
        // privileges on table mamta1.t12RoutineTest accessed by 
        // the routine    (which is underneath the view) and hence 
        // select from view will fail
        
        assertStatementError("42502", st_mamta2,
            "select * from mamta3.v21ViewTest");
        
        // set connection mamta1
        
        st_mamta1.executeUpdate(
            " grant select  on t12RoutineTest to mamta2");
        
        // set connection mamta2
        
        // should fail
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_mamta2,
            "select * from mamta3.v21ViewTest");
        
        // set connection mamta1
        //ij(MAMTA2)> -- In this test, the trigger is accessing a 
        // view. Any user that has insert privilege  on trigger 
        // table will be able to make an insert even if that user 
        // doesn't have  privileges on objects referenced by the 
        // trigger.
        
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t11TriggerTest");
        
        st_mamta1.executeUpdate(
            " create table t11TriggerTest (c111 int not null primary key)");
        
        st_mamta1.executeUpdate(
            " insert into t11TriggerTest values(1)");
        
        st_mamta1.executeUpdate(
            " insert into t11TriggerTest values(2)");
        
        st_mamta1.executeUpdate(
            " grant select on t11TriggerTest to mamta2");
        
        // set connection mamta2
        
        assertStatementError("X0X05", st_mamta2,
            " drop view v21ViewTest");
        
        st_mamta2.executeUpdate(
            " create view v21ViewTest as select * from "
            + "mamta1.t11TriggerTest");
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("4250A", st_mamta2,
            " grant select on v21ViewTest to mamta4");
        
        // set connection mamta3
        
        assertStatementError("42Y55", st_mamta3,
            " drop table t31TriggerTest");
        
        st_mamta3.executeUpdate(
            " create table t31TriggerTest (c311 int)");
        
        st_mamta3.executeUpdate(
            " grant insert on t31TriggerTest to mamta4");
        
        // set connection mamta4
        
        assertStatementError("42Y07", st_mamta4,
            " drop table t41TriggerTest");
        
        st_mamta4.executeUpdate(
            " create table t41TriggerTest (c411 int)");
        
        assertStatementError("42X94", st_mamta4,
            " drop trigger tr41t41");
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_mamta4,
            " create trigger tr41t41 after insert on "
            + "t41TriggerTest for each statement insert into "
            + "mamta3.t31TriggerTest (select * from mamta2.v21ViewTest)");
        
        st_mamta4.executeUpdate(
            " insert into t41TriggerTest values(1)");
        
        st_mamta4.executeUpdate(
            " insert into t41TriggerTest values(2)");
        
        rs = st_mamta4.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select * from t41TriggerTest order by c411");
        
        expColNames = new String [] {"C411"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta1
        
        rs = st_mamta1.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select * from t11TriggerTest order by c111");
        
        expColNames = new String [] {"C111"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta2
        
        rs = st_mamta2.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select * from v21ViewTest order by c111");
        
        expColNames = new String [] {"C111"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta3
        
        rs = st_mamta3.executeQuery(
            " select * from t31TriggerTest");
        
        expColNames = new String [] {"C311"};
        JDBC.assertColumnNames(rs, expColNames);
        
        JDBC.assertEmpty(rs);
        
        // will fail because no permissions on mamta4.t41TriggerTest
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42500", st_mamta3,
            "insert into mamta4.t41TriggerTest values(1)");
        
        // will fail because no permissions on mamta2.v21ViewTest
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_mamta3,
            "select * from mamta2.v21ViewTest");
        
        // will fail because no permissions on mamta1.t11TriggerTest
        
        assertStatementError("42502", st_mamta3,
            "select * from mamta1.t11TriggerTest");
        
        // set connection mamta4
        
        st_mamta4.executeUpdate(
            " grant insert on t41TriggerTest to mamta3");
        
        // set connection mamta3
        
        // although mamta3 doesn't have access to the objects 
        // referenced by the insert trigger   following insert will 
        // still pass because triggers run with definer's privileges.
        
        st_mamta3.executeUpdate(
            "insert into mamta4.t41TriggerTest values(1)");
        
        // set connection mamta1
        //ij(MAMTA3)> -- Test constraints
        
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t11ConstraintTest");
        
        st_mamta1.executeUpdate(
            " create table t11ConstraintTest (c111 int not null "
            + "primary key)");
        
        st_mamta1.executeUpdate(
            " insert into t11ConstraintTest values(1)");
        
        st_mamta1.executeUpdate(
            " insert into t11ConstraintTest values(2)");
        
        st_mamta1.executeUpdate(
            " grant references on t11ConstraintTest to mamta3");
        
        // set connection mamta2
        
        assertStatementError("42Y55", st_mamta2,
            " drop table t21ConstraintTest");
        
        st_mamta2.executeUpdate(
            " create table t21ConstraintTest (c111 int not null "
            + "primary key)");
        
        st_mamta2.executeUpdate(
            " insert into t21ConstraintTest values(1)");
        
        st_mamta2.executeUpdate(
            " insert into t21ConstraintTest values(2)");
        
        st_mamta2.executeUpdate(
            " grant references on t21ConstraintTest to mamta3");
        
        // set connection mamta3
        
        st_mamta3.executeUpdate(
            " create table t31ConstraintTest (c311 int "
            + "references mamta1.t11ConstraintTest, c312 int "
            + "references mamta2.t21ConstraintTest)");
        
        st_mamta3.executeUpdate(
            " drop table t31ConstraintTest");
        
        // set connection mamta1
        //ij(MAMTA3)> -- multi-key foreign key constraint and the 
        // REFERENCES privilege granted at user level. This should 
        // cause only   one row in SYSDEPENDS for REFERENCES privilege.
        
        
        st_mamta1.executeUpdate(
            " drop table t11ConstraintTest");
        
        st_mamta1.executeUpdate(
            " create table t11ConstraintTest (c111 int not null, "
            + "c112 int not null, primary key (c111, c112))");
        
        st_mamta1.executeUpdate(
            " grant references on t11ConstraintTest to mamta3");
        
        // set connection mamta3
        
        assertStatementError("42Y55", st_mamta3,
            " drop table t31ConstraintTest");
        
        st_mamta3.executeUpdate(
            " create table t31ConstraintTest (c311 int, c312 "
            + "int, foreign key(c311, c312) references "
            + "mamta1.t11ConstraintTest)");
        
        st_mamta3.executeUpdate(
            " drop table t31ConstraintTest");
        
        // set connection mamta1
        //ij(MAMTA3)> -- Same test as above with multi-key foreign 
        // key constraint but one column REFERENCES privilege 
        // granted at user level   and other column REFERENCES 
        // privilege granted at PUBLIC level. This should cause two 
        // rows in SYSDEPENDS for REFERENCES privilege.
        
        
        st_mamta1.executeUpdate(
            " drop table t11ConstraintTest");
        
        st_mamta1.executeUpdate(
            " create table t11ConstraintTest (c111 int not null, "
            + "c112 int not null, primary key (c111, c112))");
        
        st_mamta1.executeUpdate(
            " grant references(c111) on t11ConstraintTest to mamta3");
        
        st_mamta1.executeUpdate(
            " grant references(c112) on t11ConstraintTest to PUBLIC");
        
        // set connection mamta3
        //ij(MAMTA1)> --connect 
        // 'jdbc:derby:c:/dellater/dbmaintest2;create=true' user 
        // 'mamta3' as mamta3
        
        
        assertStatementError("42Y55", st_mamta3,
            " drop table t31ConstraintTest");
        
        st_mamta3.executeUpdate(
            " create table t31ConstraintTest (c311 int,  c312 "
            + "int, foreign key(c311, c312) references "
            + "mamta1.t11ConstraintTest)");
        
        st_mamta3.executeUpdate(
            " drop table t31ConstraintTest");
        
        // Same test as above with multi-key foreign key 
        // constraint, one column REFERENCES privilege granted at 
        // user level   and other column REFERENCES privilege 
        // granted at PUBLIC level. This should cause two rows in 
        // SYSDEPENDS for REFERENCES privilege.   But foreign key 
        // reference is added using alter table rather than at 
        // create table time
        
        st_mamta3.executeUpdate(
            "create table t31constrainttest(c311 int, c312 int)");
        
        st_mamta3.executeUpdate(
            " alter table t31constrainttest add foreign key "
            + "(c311, c312) references mamta1.t11constrainttest");
        
        st_mamta3.executeUpdate(
            " drop table t31ConstraintTest");
        
        // create the table again, but this time one foreign key 
        // constraint on one table with single column primary key 
        // and   another foreign key constraint on another table 
        // with multi-column primary key
        
        st_mamta3.executeUpdate(
            "create table t31constrainttest(c311 int, c312 int, "
            + "c313 int references mamta2.t21ConstraintTest)");
        
        st_mamta3.executeUpdate(
            " alter table t31constrainttest add foreign key "
            + "(c311, c312) references mamta1.t11constrainttest");
        
        // set connection mamta1
        //ij(MAMTA3)> -- revoke of TRIGGERS and other privileges 
        // should drop dependent triggers
        
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t11TriggerRevokeTest");
        
        st_mamta1.executeUpdate(
            " create table t11TriggerRevokeTest (c111 int not "
            + "null primary key)");
        
        st_mamta1.executeUpdate(
            " insert into t11TriggerRevokeTest values(1),(2)");
        
        // mamta2 is later going to create an insert trigger on 
        // t11TriggerRevokeTest
        
        st_mamta1.executeUpdate(
            "grant TRIGGER on t11TriggerRevokeTest to mamta2");
        
        // set connection mamta2
        
        assertStatementError("42Y55", st_mamta2,
            " drop table t21TriggerRevokeTest");
        
        st_mamta2.executeUpdate(
            " create table t21TriggerRevokeTest (c211 int)");
        
        // following will pass because mamta2 has trigger 
        // permission on mamta1.t11TriggerRevokeTest
        
        st_mamta2.executeUpdate(
            "create trigger tr11t11 after insert on "
            + "mamta1.t11TriggerRevokeTest for each "
            + "statement insert into t21TriggerRevokeTest values(99)");
        
        // no data in the table in which trigger is going to insert
        
        rs = st_mamta2.executeQuery(
            "select * from t21TriggerRevokeTest");
        
        expColNames = new String [] {"C211"};
        JDBC.assertColumnNames(rs, expColNames);
        
        JDBC.assertEmpty(rs);
        
        // set connection mamta1
        
        // insert trigger will fire
        
        st_mamta1.executeUpdate(
            "insert into t11TriggerRevokeTest values(3)");
        
        // set connection mamta2
        
        // trigger inserted one row into following table
        
        rs = st_mamta2.executeQuery(
            "select * from t21TriggerRevokeTest");
        
        expColNames = new String [] {"C211"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"99"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta1
        
        // this revoke is going to drop dependent trigger
        
        st_mamta1.executeUpdate(
            "revoke trigger on t11TriggerRevokeTest from mamta2");
        
        // following insert won't fire an insert trigger because 
        // one doesn't exist
        
        st_mamta1.executeUpdate(
            "insert into t11TriggerRevokeTest values(4)");
        
        // set connection mamta2
        
        // no more rows inserted since last check
        
        rs = st_mamta2.executeQuery(
            "select * from t21TriggerRevokeTest");
        
        expColNames = new String [] {"C211"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"99"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // following attempt to create insert trigger again will 
        // fail because trigger privilege has been revoked.
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42500", st_mamta2,
            "create trigger tr11t11 after insert on "
            + "mamta1.t11TriggerRevokeTest for each "
            + "statement insert into t21TriggerRevokeTest values(99)");
        
        // set connection mamta1
        
        st_mamta1.executeUpdate(
            " grant trigger on t11TriggerRevokeTest to mamta2");
        
        // set connection mamta2
        
        // following attempt to create insert trigger again will 
        // pass because mamta2 has got the necessary trigger privilege.
        
        st_mamta2.executeUpdate(
            "create trigger tr11t11 after insert on "
            + "mamta1.t11TriggerRevokeTest for each "
            + "statement insert into t21TriggerRevokeTest values(99)");
        
        rs = st_mamta2.executeQuery(
            " select * from t21TriggerRevokeTest");
        
        expColNames = new String [] {"C211"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"99"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta1
        
        // insert trigger should get fired
        
        st_mamta1.executeUpdate(
            "insert into t11TriggerRevokeTest values(5)");
        
        // set connection mamta2
        
        // Should be one more row since last check because insert 
        // trigger got fired
        
        rs = st_mamta2.executeQuery(
            "select * from t21TriggerRevokeTest");
        
        expColNames = new String [] {"C211"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"99"},
            {"99"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st_mamta2.executeUpdate(
            " drop trigger tr11t11");

        st_mamta2.executeUpdate(
            " drop table t21TriggerRevokeTest");
        
        // set connection mamta1
        
        // this revoke is going to drop dependent trigger
        
        st_mamta1.executeUpdate(
            "revoke trigger on t11TriggerRevokeTest from mamta2");
        
        // following insert won't fire an insert trigger because 
        // one doesn't exist
        
        st_mamta1.executeUpdate(
            "insert into t11TriggerRevokeTest values(6)");
        
        // cleanup
        
        st_mamta1.executeUpdate(
            "drop table t11TriggerRevokeTest");
        
        // set connection mamta1
        // Define a trigger on a table, then revoke a privilege on 
        // the table which trigger doesn't really depend on. The 
        // trigger still gets dropped automatically. This will be 
        // fixed in subsequent patch
        
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t11TriggerRevokeTest");
        
        st_mamta1.executeUpdate(
            " create table t11TriggerRevokeTest (c111 int not "
            + "null primary key)");
        
        st_mamta1.executeUpdate(
            " insert into t11TriggerRevokeTest values(1),(2)");
        
        st_mamta1.executeUpdate(
            " grant SELECT on t11TriggerRevokeTest to mamta2");
        
        // mamta2 is later going to create an insert trigger on 
        // t11TriggerRevokeTest
        
        st_mamta1.executeUpdate(
            "grant TRIGGER on t11TriggerRevokeTest to mamta2");
        
        // set connection mamta2
        
        assertStatementError("42Y55", st_mamta2,
            " drop table t21TriggerRevokeTest");
        
        st_mamta2.executeUpdate(
            " create table t21TriggerRevokeTest (c211 int)");
        
        // following will pass because mamta2 has trigger 
        // permission on mamta1.t11TriggerRevokeTest
        
        st_mamta2.executeUpdate(
            "create trigger tr11t11 after insert on "
            + "mamta1.t11TriggerRevokeTest for each "
            + "statement insert into t21TriggerRevokeTest values(99)");
        
        // no data in the table in which trigger is going to insert
        
        rs = st_mamta2.executeQuery(
            "select * from t21TriggerRevokeTest");
        
        expColNames = new String [] {"C211"};
        JDBC.assertColumnNames(rs, expColNames);
        
        JDBC.assertEmpty(rs);
        
        // set connection mamta1
        
        // insert trigger will fire
        
        st_mamta1.executeUpdate(
            "insert into t11TriggerRevokeTest values(3)");
        
        // set connection mamta2
        
        // trigger inserted one row into following table
        
        rs = st_mamta2.executeQuery(
            "select * from t21TriggerRevokeTest");
        
        expColNames = new String [] {"C211"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"99"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta1
        
        // this revoke is going to drop dependent trigger on the 
        // table although dependent trigger does not need this 
        // particular permission WILL FIX THIS IN A SUBSEQUENT 
        // PATCH****************************************************
        // ************************************
        
        st_mamta1.executeUpdate(
            "revoke SELECT on t11TriggerRevokeTest from mamta2");
        
        // following insert won't fire an insert trigger because 
        // one doesn't exist
        
        st_mamta1.executeUpdate(
            "insert into t11TriggerRevokeTest values(4)");
        
        // set connection mamta2
        
        // no more rows inserted since last check
        
        rs = st_mamta2.executeQuery(
            "select * from t21TriggerRevokeTest");
        
        expColNames = new String [] {"C211"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"99"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // following attempt to create insert trigger again will 
        // pas because TRIGGER privilege was never revoked.
        
        st_mamta2.executeUpdate(
            "create trigger tr11t11 after insert on "
            + "mamta1.t11TriggerRevokeTest for each "
            + "statement insert into t21TriggerRevokeTest values(99)");
        
        // set connection mamta1
        
        // insert trigger should get fired
        
        st_mamta1.executeUpdate(
            "insert into t11TriggerRevokeTest values(5)");
        
        // set connection mamta2
        
        // Should be one more row since last check because insert 
        // trigger is back in action
        
        rs = st_mamta2.executeQuery(
            "select * from t21TriggerRevokeTest");
        
        expColNames = new String [] {"C211"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"99"},
            {"99"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);

        st_mamta2.executeUpdate(
            " drop trigger tr11t11");

        st_mamta2.executeUpdate(
            " drop table t21TriggerRevokeTest");
        
        // set connection mamta1
        
        // this revoke is going to drop dependent trigger
        
        st_mamta1.executeUpdate(
            "revoke trigger on t11TriggerRevokeTest from mamta2");
        
        // following insert won't fire an insert trigger because 
        // one doesn't exist
        
        st_mamta1.executeUpdate(
            "insert into t11TriggerRevokeTest values(6)");
        
        // cleanup
        
        st_mamta1.executeUpdate(
            "drop table t11TriggerRevokeTest");
        
        // set connection mamta1
        // Define couple triggers on a table relying on privilege 
        // on different tables. If a revoke is issued, only the 
        // dependent triggers   should get dropped, the rest of the 
        // triggers should stay active.
        
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t11TriggerRevokeTest");
        
        st_mamta1.executeUpdate(
            " create table t11TriggerRevokeTest (c111 int)");
        
        st_mamta1.executeUpdate(
            " insert into t11TriggerRevokeTest values(1),(2)");
        
        st_mamta1.executeUpdate(
            " grant INSERT on t11TriggerRevokeTest to mamta2");
        
        assertStatementError("42Y55", st_mamta1,
            " drop table t12TriggerRevokeTest");
        
        st_mamta1.executeUpdate(
            " create table t12TriggerRevokeTest (c121 int)");
        
        st_mamta1.executeUpdate(
            " insert into t12TriggerRevokeTest values(1),(2)");
        
        st_mamta1.executeUpdate(
            " grant INSERT on t12TriggerRevokeTest to mamta2");
        
        // set connection mamta2
        
        assertStatementError("42Y55", st_mamta2,
            " drop table t21TriggerRevokeTest");
        
        st_mamta2.executeUpdate(
            " create table t21TriggerRevokeTest (c211 int)");
        
        st_mamta2.executeUpdate(
            " insert into t21TriggerRevokeTest values(1)");
        
        // following will pass because mamta2 has required 
        // permissions on mamta1.t11TriggerRevokeTest
        
        st_mamta2.executeUpdate(
            "create trigger tr211t21 after insert on "
            + "t21TriggerRevokeTest for each statement insert into "
            + "mamta1.t11TriggerRevokeTest values(99)");
        
        // following will pass because mamta2 has required 
        // permissions on mamta1.t11TriggerRevokeTest
        
        st_mamta2.executeUpdate(
            "create trigger tr212t21 after insert on "
            + "t21TriggerRevokeTest for each statement insert into "
            + "mamta1.t12TriggerRevokeTest values(99)");
        
        st_mamta2.executeUpdate(
            " insert into t21TriggerRevokeTest values(1)");
        
        // set connection mamta1
        
        // there should be 1 new row in each of the tables because 
        // of 2 insert triggers
        
        rs = st_mamta1.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            "select * from t11TriggerRevokeTest order by c111");
        
        expColNames = new String [] {"C111"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"99"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_mamta1.executeQuery(
            " select * from t12TriggerRevokeTest");
        
        expColNames = new String [] {"C121"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"99"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertUpdateCount(st_mamta1, 3,
            " delete from t11TriggerRevokeTest");
        
        assertUpdateCount(st_mamta1, 3,
            " delete from t12TriggerRevokeTest");
        
        // only one trigger(tr211t21) should get dropped because 
        // of following revoke
        
        st_mamta1.executeUpdate(
            "revoke insert on t11TriggerRevokeTest from mamta2");
        
        // set connection mamta2
        
        st_mamta2.executeUpdate(
            " insert into t21TriggerRevokeTest values(1)");
        
        // set connection mamta1
        
        // there should be no row in this table
        
        rs = st_mamta1.executeQuery(
            "select * from t11TriggerRevokeTest");
        
        expColNames = new String [] {"C111"};
        JDBC.assertColumnNames(rs, expColNames);
        
        JDBC.assertEmpty(rs);
        
        // there should be one new row in mamta1.t12TriggerRevokeTest
        
        rs = st_mamta1.executeQuery(
            "select * from t12TriggerRevokeTest");
        
        expColNames = new String [] {"C121"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"99"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta2
        //ij(MAMTA1)> -- cleanup
        
        
        st_mamta2.executeUpdate(
            " drop table t21TriggerRevokeTest");
        
        // set connection mamta1
        
        st_mamta1.executeUpdate(
            " drop table t12TriggerRevokeTest");
        
        st_mamta1.executeUpdate(
            " drop table t11TriggerRevokeTest");
        
        //- Test automatic dropping of dependent permission 
        // descriptors when objects they refer to is dropped.- 
        // Dropping of a table, for example, should drop all table 
        // and column permission descriptors on it.
        
        st_mamta1.executeUpdate(
            "create table newTable(i int, j int, k int)");
        
        st_mamta1.executeUpdate(
            " grant select, update(j) on newTable to sammy");
        
        st_mamta1.executeUpdate(
            " grant references, delete on newTable to user1");
        
        // Try with a view
        
        st_mamta1.executeUpdate(
            "create view myView as select * from newTable");
        
        st_mamta1.executeUpdate(
            " grant select on myView to sammy");
        
        rs = st_mamta1.executeQuery(
            " select GRANTEE, GRANTOR, SELECTPRIV, DELETEPRIV, INSERTPRIV, UPDATEPRIV, REFERENCESPRIV, TRIGGERPRIV from sys.systableperms where "
            + "grantee='SAMMY' or grantee='USER1' order by GRANTEE, GRANTOR");
        
        expColNames = new String [] {"GRANTEE", "GRANTOR", "SELECTPRIV", "DELETEPRIV", "INSERTPRIV", "UPDATEPRIV", "REFERENCESPRIV", "TRIGGERPRIV"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"SAMMY", "MAMTA1", "y", "N", "N", "N", "N", "N"},
            {"SAMMY", "MAMTA1", "y", "N", "N", "N", "N", "N"},
            {"USER1", "MAMTA1", "N", "y", "N", "N", "y", "N"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_mamta1.executeQuery(
            " select GRANTEE, GRANTOR, TYPE, COLUMNS from sys.syscolperms where "
            + "grantee='SAMMY' or grantee='USER1'");
        
        expColNames = new String [] {"GRANTEE", "GRANTOR", "TYPE", "COLUMNS"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"SAMMY", "MAMTA1", "u", "{1}"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st_mamta1.executeUpdate(
            " drop view myView");
        
        rs = st_mamta1.executeQuery(
            " select GRANTEE, GRANTOR, SELECTPRIV, DELETEPRIV, INSERTPRIV, UPDATEPRIV, REFERENCESPRIV, TRIGGERPRIV from sys.systableperms where "
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            + "grantee='SAMMY' or grantee='USER1' order by GRANTEE, GRANTOR");
        
        expColNames = new String [] {"GRANTEE", "GRANTOR", "SELECTPRIV", "DELETEPRIV", "INSERTPRIV", "UPDATEPRIV", "REFERENCESPRIV", "TRIGGERPRIV"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"SAMMY", "MAMTA1", "y", "N", "N", "N", "N", "N"},
            {"USER1", "MAMTA1", "N", "y", "N", "N", "y", "N"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st_mamta1.executeUpdate(
            " drop table newTable");
        
        rs = st_mamta1.executeQuery(
            " select GRANTEE, GRANTOR, SELECTPRIV, DELETEPRIV, INSERTPRIV, UPDATEPRIV, REFERENCESPRIV, TRIGGERPRIV from sys.systableperms where "
            + "grantee='SAMMY' or grantee='USER1'");
        
        expColNames = new String [] {"GRANTEE", "GRANTOR", "SELECTPRIV", "DELETEPRIV", "INSERTPRIV", "UPDATEPRIV", "REFERENCESPRIV", "TRIGGERPRIV"};
        JDBC.assertColumnNames(rs, expColNames);
        
        JDBC.assertEmpty(rs);
        
        rs = st_mamta1.executeQuery(
            " select GRANTEE, GRANTOR, TYPE, COLUMNS from sys.syscolperms where "
            + "grantee='SAMMY' or grantee='USER1'");
        
        expColNames = new String [] {"GRANTEE", "GRANTOR", "TYPE", "COLUMNS"};
        JDBC.assertColumnNames(rs, expColNames);
        
        JDBC.assertEmpty(rs);
        
        //- Try droping of a routine with permission descriptors. 
        // Should get dropped
        
        st_mamta1.executeUpdate(
            "CREATE FUNCTION newFunction(P1 INT) RETURNS "
            + "INT RETURNS NULL ON NULL INPUT EXTERNAL NAME "
            + "'org.apache.derbyTesting.functionTests.util.ProcedureTest.selectFromSpecificSchema'LANGUAGE JAVA "
            + "PARAMETER STYLE JAVA");
        
        st_mamta1.executeUpdate(
            " grant execute on function newFunction to sammy");
        
        st_mamta1.executeUpdate(
            " grant execute on function newFunction(INT) to user3");
        
        rs = st_mamta1.executeQuery(
            " select GRANTEE, GRANTOR, GRANTOPTION from sys.sysroutineperms where "
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            + "grantee='SAMMY' or grantee='USER3' order by GRANTEE, GRANTOR");
        
        expColNames = new String [] {"GRANTEE", "GRANTOR", "GRANTOPTION"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"SAMMY", "MAMTA1", "N"},
            {"USER3", "MAMTA1", "N"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st_mamta1.executeUpdate(
            " drop function newFunction");
        
        rs = st_mamta1.executeQuery(
            " select GRANTEE, GRANTOR, GRANTOPTION from sys.sysroutineperms where "
            + "grantee='SAMMY' or grantee='USER3'");
        
        expColNames = new String [] {"GRANTEE", "GRANTOR", "GRANTOPTION"};
        JDBC.assertColumnNames(rs, expColNames);
        
        JDBC.assertEmpty(rs);
        
        // Try the same tests after a permission descriptor is 
        // likely to have been cached
        
        st_mamta1.executeUpdate(
            "create table newTable(i int, j int, k int)");
        
        st_mamta1.executeUpdate(
            " grant select(i,j), delete on newTable to sammy");
        
        st_mamta1.executeUpdate(
            " CREATE FUNCTION F_ABS(P1 INT) RETURNS INT NO "
            + "SQL RETURNS NULL ON NULL INPUT EXTERNAL NAME "
            + "'java.lang.Math.abs' LANGUAGE JAVA PARAMETER STYLE JAVA");
        
        st_mamta1.executeUpdate(
            " grant execute on function f_abs to sammy");
        
        rs = st_mamta1.executeQuery(
            " select GRANTEE, GRANTOR, GRANTOPTION from sys.sysroutineperms where grantee='SAMMY'");
        
        expColNames = new String [] {"GRANTEE", "GRANTOR", "GRANTOPTION"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"SAMMY", "MAMTA1", "N"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_mamta1.executeQuery(
            " select GRANTEE, GRANTOR, TYPE, COLUMNS from sys.syscolperms where grantee='SAMMY'");
        
        expColNames = new String [] {"GRANTEE", "GRANTOR", "TYPE", "COLUMNS"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"SAMMY", "MAMTA1", "s", "{0, 1}"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_mamta1.executeQuery(
            " select GRANTEE, GRANTOR, SELECTPRIV, DELETEPRIV, INSERTPRIV, UPDATEPRIV, REFERENCESPRIV, TRIGGERPRIV from sys.systableperms where grantee='SAMMY'");
        
        expColNames = new String [] {"GRANTEE", "GRANTOR", "SELECTPRIV", "DELETEPRIV", "INSERTPRIV", "UPDATEPRIV", "REFERENCESPRIV", "TRIGGERPRIV"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"SAMMY", "MAMTA1", "N", "y", "N", "N", "N", "N"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Now connect as sammy and access database objects. That 
        // should create PermissionsDescriptors and cache them
        
        Connection sammyConnection = openUserConnection("sammy");
        Statement st_sammyConnection = sammyConnection.createStatement();
        
        st_sammyConnection.executeUpdate(
            " set schema mamta1");
        
        rs = st_sammyConnection.executeQuery(
            " select i,j from newTable");
        
        expColNames = new String [] {"I", "J"};
        JDBC.assertColumnNames(rs, expColNames);
        
        JDBC.assertEmpty(rs);
        
        rs = st_sammyConnection.executeQuery(
            " values f_abs(-5)");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"5"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta1
        
        st_mamta1.executeUpdate(
            " drop table newTable");
        
        st_mamta1.executeUpdate(
            " drop function f_abs");
        
        // Confirm rows in catalogs are gone
        
        rs = st_mamta1.executeQuery(
            "select GRANTEE, GRANTOR, GRANTOPTION from sys.sysroutineperms where grantee='SAMMY'");
        
        expColNames = new String [] {"GRANTEE", "GRANTOR", "GRANTOPTION"};
        JDBC.assertColumnNames(rs, expColNames);
        
        JDBC.assertEmpty(rs);
        
        rs = st_mamta1.executeQuery(
            " select GRANTEE, GRANTOR, TYPE, COLUMNS from sys.syscolperms where grantee='SAMMY'");
        
        expColNames = new String [] {"GRANTEE", "GRANTOR", "TYPE", "COLUMNS"};
        JDBC.assertColumnNames(rs, expColNames);
        
        JDBC.assertEmpty(rs);
        
        rs = st_mamta1.executeQuery(
            " select GRANTEE, GRANTOR, SELECTPRIV, DELETEPRIV, INSERTPRIV, UPDATEPRIV, REFERENCESPRIV, TRIGGERPRIV from sys.systableperms where grantee='SAMMY'");
        
        expColNames = new String [] {"GRANTEE", "GRANTOR", "SELECTPRIV", "DELETEPRIV", "INSERTPRIV", "UPDATEPRIV", "REFERENCESPRIV", "TRIGGERPRIV"};
        JDBC.assertColumnNames(rs, expColNames);
        
        JDBC.assertEmpty(rs);
        
        // DERBY-1608: Recognize new SYSFUN routines as system 
        // builtin routines Builtin functions don't need any 
        // permission checking. They are executable by all
        
        rs = st_mamta1.executeQuery(
            "VALUES { fn ACOS(0.0707) }");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1.5000372950430991"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_mamta1.executeQuery(
            " VALUES ACOS(0.0707)");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1.5000372950430991"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_mamta1.executeQuery(
            " VALUES PI()");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"3.141592653589793"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st_mamta1.executeUpdate(
            " create table SYSFUN_MATH_TEST (d double)");
        
        st_mamta1.executeUpdate(
            " insert into SYSFUN_MATH_TEST values null");
        
        st_mamta1.executeUpdate(
            " insert into SYSFUN_MATH_TEST values 0.67");
        
        st_mamta1.executeUpdate(
            " insert into SYSFUN_MATH_TEST values 1.34");
        
        rs = st_mamta1.executeQuery(
            " select cast (ATAN(d) as DECIMAL(6,3)) AS ATAN FROM "
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            + "SYSFUN_MATH_TEST order by atan");
        
        expColNames = new String [] {"ATAN"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0.590"},
            {"0.929"},
            {null},
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_mamta1.executeQuery(
            " select cast (COS(d) as DECIMAL(6,3)) AS COS FROM "
            + "SYSFUN_MATH_TEST order by cos");
        
        expColNames = new String [] {"COS"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0.228"},
            {"0.783"},
            {null},
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_mamta1.executeQuery(
            " select cast (SIN(d) as DECIMAL(6,3)) AS SIN FROM "
            + "SYSFUN_MATH_TEST order by sin");
        
        expColNames = new String [] {"SIN"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0.620"},
            {"0.973"},
            {null},
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_mamta1.executeQuery(
            " select cast (TAN(d) as DECIMAL(6,3)) AS TAN FROM "
            + "SYSFUN_MATH_TEST order by tan");
        
        expColNames = new String [] {"TAN"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0.792"},
            {"4.255"},
            {null},
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_mamta1.executeQuery(
            " select cast (DEGREES(d) as DECIMAL(6,3)) AS "
            + "DEGREES FROM SYSFUN_MATH_TEST order by degrees");
        
        expColNames = new String [] {"DEGREES"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"38.388"},
            {"76.776"},
            {null},
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_mamta1.executeQuery(
            " select cast (RADIANS(d) as DECIMAL(6,3)) AS "
            + "RADIANS FROM SYSFUN_MATH_TEST order by radians");
        
        expColNames = new String [] {"RADIANS"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0.011"},
            {"0.023"},
            {null},
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // DERBY-1538: Disable ability to GRANT or REVOKE from self
        
        st_mamta1.executeUpdate(
            "CREATE FUNCTION F_ABS(P1 INT) RETURNS INT NO "
            + "SQL RETURNS NULL ON NULL INPUT EXTERNAL NAME "
            + "'java.lang.Math.abs' LANGUAGE JAVA PARAMETER STYLE JAVA");
        
        st_mamta1.executeUpdate(
            " create table mamta1Table ( i int, j int)");
        
        // Try granting or revoking to mamta1. Should all fail
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42509", st_mamta1,
            "grant select on mamta1Table to mamta1");
        
        assertStatementError("42509", st_mamta1,
            " revoke select on mamta1Table from mamta1");
        
        assertStatementError("42509", st_mamta1,
            " grant execute on function f_abs to mamta1");
        
        assertStatementError("42509", st_mamta1,
            " revoke execute on function f_abs from mamta1 restrict");
        
        // set connection satConnection
        //ij(MAMTA1)> -- Connect as database owner. Even she can 
        // not grant to owner or revoke from owner
        
        
        st.executeUpdate(
            " set schema mamta1");
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42509", st,
            " grant select on mamta1Table to mamta1");
        
        assertStatementError("42509", st,
            " revoke select on mamta1Table from mamta1");
        
        assertStatementError("42509", st,
            " grant execute on function f_abs to mamta1");
        
        assertStatementError("42509", st,
            " revoke execute on function f_abs from mamta1 restrict");
        
        // But Grant/Revoke to another user should pass
        
        st.executeUpdate(
            "grant select on mamta1Table to randy");
        
        st.executeUpdate(
            " revoke select on mamta1Table from randy");
        
        st.executeUpdate(
            " grant execute on function f_abs to randy");
        
        st.executeUpdate(
            " revoke execute on function f_abs from randy restrict");
        
        // set connection mamta1
        
        st_mamta1.executeUpdate(
            " drop table mamta1Table");
        
        st_mamta1.executeUpdate(
            " drop function f_abs");
        
        // DERBY-1708 Test LOCK TABLE statement
        
        Connection user1 = openUserConnection("user1");
        Statement st_user1 = user1.createStatement();
        
        st_user1.executeUpdate(
            " create table t100 (i int)");
        
        Connection user2 = openUserConnection("user2");
        Statement st_user2 = user2.createStatement();
        
        user2.setAutoCommit(false);
        
        // expect errors
        
        assertStatementError("42500", st_user2,
            "lock table user1.t100 in exclusive mode");
        
        assertStatementError("42500", st_user2,
            " lock table user1.t100 in share mode");
        
        // set connection user1
        user1.commit();
        
        st_user1.executeUpdate(
            " grant select on t100 to user2");
        
        // set connection user2
        
        // ok
        
        st_user2.executeUpdate(
            "lock table user1.t100 in exclusive mode");
        
        st_user2.executeUpdate(
            " lock table user1.t100 in share mode");
        
        // set connection user1
        user1.commit();
        
        st_user1.executeUpdate(
            " revoke select on t100 from user2");
        
        // set connection user2
        
        // expect errors
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42500", st_user2,
            "lock table user1.t100 in exclusive mode");
        
        assertStatementError("42500", st_user2,
            " lock table user1.t100 in share mode");
        
        user2.commit();
        user2.setAutoCommit(true);
        
        // set connection user1
        //ij(USER2)> -- DERBY-1686
        
        
        st_user1.executeUpdate(
            " create table t1 (i int)");
        
        st_user1.executeUpdate(
            " insert into t1 values 1,2,3");
        
        st_user1.executeUpdate(
            " grant select on t1 to user2");
        
        // set connection user2
        
        st_user2.executeUpdate(
            " create view v1 as select * from user1.t1");
        
        // attempt to grant this view to others, should fail since 
        // user2 does not have grant privilege on object user1.t1
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42506", st_user2,
            "grant select on user1.t1 to user3");
        
        // expect error
        
        assertStatementError("4250A", st_user2,
            "grant select on v1 to user3");
        
        // set connection user2
        // cleanup
        
        
        st_user2.executeUpdate(
            " drop view v1");
        
        // set connection user1
        
        st_user1.executeUpdate(
            " drop table t1");
        
        // set connection user2
        user2.setAutoCommit(true);
        
        
        user2.setAutoCommit(true);
        
        // set connection mamta1
        //ij(USER2)> -- Simple test case for DERBY-1583: column 
        // privilege checking should not assume column descriptors 
        // have non-null table references.
        
        
        st_mamta1.executeUpdate(
            " create table t11TriggerRevokeTest (c111 int not "
            + "null primary key, c12 int)");
        
        st_mamta1.executeUpdate(
            " insert into t11TriggerRevokeTest values (1, 101), "
            + "(2, 202), (3, 303)");
        
        st_mamta1.executeUpdate(
            " grant TRIGGER on t11TriggerRevokeTest to mamta2");
        
        st_mamta1.executeUpdate(
            " create table t12TriggerRevokeTest (c121 int, c122 "
            + "int, c123 int)");
        
        st_mamta1.executeUpdate(
            " insert into t12TriggerRevokeTest values (10, 1010, "
            + "2010),(20,1020,2020)");
        
        st_mamta1.executeUpdate(
            " grant UPDATE(c122, c121) on t12TriggerRevokeTest to mamta2");
        
        // set connection mamta2
        
        st_mamta2.executeUpdate(
            " create trigger tr11t11 after insert on "
            + "mamta1.t11TriggerRevokeTest for each statement update "
            + "mamta1.t12TriggerRevokeTest set c122 = 99");
        
        // set connection mamta1
        
        rs = st_mamta1.executeQuery(
            " select * from t11TriggerRevokeTest order by c111");
        
        expColNames = new String [] {"C111", "C12"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "101"},
            {"2", "202"},
            {"3", "303"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_mamta1.executeQuery(
            " select * from t12TriggerRevokeTest order by c121");
        
        expColNames = new String [] {"C121", "C122", "C123"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"10", "1010", "2010"},
            {"20", "1020", "2020"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // This should fire the trigger, changing the c122 values 
        // to 99
        
        st_mamta1.executeUpdate(
            "insert into t11TriggerRevokeTest values(4, 404)");
        
        rs = st_mamta1.executeQuery(
            " select * from t11TriggerRevokeTest order by c111");
        
        expColNames = new String [] {"C111", "C12"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "101"},
            {"2", "202"},
            {"3", "303"},
            {"4", "404"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_mamta1.executeQuery(
            " select * from t12TriggerRevokeTest order by c121");
        
        expColNames = new String [] {"C121", "C122", "C123"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"10", "99", "2010"},
            {"20", "99", "2020"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // revoking the privilege should drop the trigger
        
        st_mamta1.executeUpdate(
            "revoke TRIGGER on t11TriggerRevokeTest from mamta2");
        
        assertUpdateCount(st_mamta1, 2,
            " update t12TriggerRevokeTest set c122 = 42");
        
        // now when we insert the trigger should NOT be fired, 
        // c122 values should be unchanged and so should be 42
        
        st_mamta1.executeUpdate(
            "insert into t11TriggerRevokeTest values (5,505)");
        
        rs = st_mamta1.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select * from t11TriggerRevokeTest order by c111");
        
        expColNames = new String [] {"C111", "C12"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "101"},
            {"2", "202"},
            {"3", "303"},
            {"4", "404"},
            {"5", "505"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_mamta1.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select * from t12TriggerRevokeTest order by c121");
        
        expColNames = new String [] {"C121", "C122", "C123"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"10", "42", "2010"},
            {"20", "42", "2020"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection mamta1
        // Simple test case for DERBY-1724, which is a different 
        // manifestation of DERBY-1583
        
        
        st_mamta1.executeUpdate(
            " create table t1001 (c varchar(1))");
        
        st_mamta1.executeUpdate(
            " insert into t1001 values 'a', 'b', 'c'");
        
        mamta1.setAutoCommit(false);
        
        st_mamta1.executeUpdate(
            " grant select on t1001 to mamta3");
        
        // set connection mamta2
        
        st_mamta2.executeUpdate(
            " create table ttt1 (i int)");
        
        st_mamta2.executeUpdate(
            " insert into ttt1 values 1");
        
        st_mamta2.executeUpdate(
            " grant all privileges on ttt1 to mamta1");
        
        // set connection mamta1
        
        rs = st_mamta1.executeQuery(
            " select * from mamta2.ttt1");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st_mamta1.executeUpdate(
            " insert into mamta2.ttt1 values 2");
        
        assertUpdateCount(st_mamta1, 2,
            " update mamta2.ttt1 set i = 888");
        
        mamta1.commit();
        mamta1.setAutoCommit(true);
        
        // set connection mamta1
        // Simple test case for DERBY-1589. The problem here 
        // involves dependency management between the FOREIGN KEY 
        // clause in the CREATE TABLE statement and the underlying 
        // table that the FK refers to. The statement must declare 
        // a dependency on the referenced table so that changes to 
        // the table cause invalidation of the statement's compiled 
        // plan. The test case below sets up such a situation by 
        // dropping the referenced table and recreating it and then 
        // re-issuing a statement with identical text to one which 
        // was issued earlier.
        
        
        st_mamta1.executeUpdate(
            " create table d1589t11ConstraintTest (c111 int not "
            + "null, c112 int not null, primary key (c111, c112))");
        
        st_mamta1.executeUpdate(
            " grant references on d1589t11ConstraintTest to mamta3");
        
        // set connection mamta3
        
        assertStatementError("42Y55", st_mamta3,
            " drop table d1589t31ConstraintTest");
        
        st_mamta3.executeUpdate(
            " create table d1589t31ConstraintTest (c311 int, "
            + "c312 int, foreign key(c311, c312) references "
            + "mamta1.d1589t11ConstraintTest)");
        
        st_mamta3.executeUpdate(
            " drop table d1589t31ConstraintTest");
        
        // set connection mamta1
        
        st_mamta1.executeUpdate(
            " drop table d1589t11ConstraintTest");
        
        st_mamta1.executeUpdate(
            " create table d1589t11ConstraintTest (c111 int not "
            + "null, c112 int not null, primary key (c111, c112))");
        
        st_mamta1.executeUpdate(
            " grant references(c111) on d1589t11ConstraintTest to mamta3");
        
        st_mamta1.executeUpdate(
            " grant references(c112) on d1589t11ConstraintTest to PUBLIC");
        
        // set connection mamta3
        
        st_mamta3.executeUpdate(
            " create table d1589t31ConstraintTest (c311 int, "
            + "c312 int, foreign key(c311, c312) references "
            + "mamta1.d1589t11ConstraintTest)");

        // DERBY-3743
//IC see: https://issues.apache.org/jira/browse/DERBY-3743
        st.executeUpdate(
            "CREATE FUNCTION F_ABS(P1 INT) RETURNS INT NO "
            + "SQL RETURNS NULL ON NULL INPUT EXTERNAL NAME "
            + "'java.lang.Math.abs' LANGUAGE JAVA PARAMETER STYLE JAVA");
        st.executeUpdate(
            " grant execute on function f_abs to mamta3");
        st_mamta3.executeUpdate(
            " create table dhw(i int check(mamta1.f_abs(i) > 0))");
        assertStatementError("23513", st_mamta3, "insert into dhw values 0");
        assertStatementError
            ("X0Y25", st,
             "revoke execute on function f_abs from mamta3 restrict");
        st_mamta3.executeUpdate(" drop table dhw");


        // DERBY-3743b, test 1: multiple constraints, one routine dep per
        // constraint.
//IC see: https://issues.apache.org/jira/browse/DERBY-3743
        st.executeUpdate(
            "CREATE FUNCTION F_ABS2(P1 INT) RETURNS INT NO "
            + "SQL RETURNS NULL ON NULL INPUT EXTERNAL NAME "
            + "'java.lang.Math.abs' LANGUAGE JAVA PARAMETER STYLE JAVA");
        st.executeUpdate(
            " grant execute on function f_abs to mamta3");
        st.executeUpdate(
            " grant execute on function f_abs2 to mamta3");
        st_mamta3.executeUpdate(
            "create table dhw(i int constraint a1 check(mamta1.f_abs(i) > 0)" +
                         ",j int constraint a2 check(mamta1.f_abs2(j) > 0))");
        assertStatementError(
            "23513", st_mamta3, "insert into dhw values (0,0)");
        assertStatementError
            ("X0Y25", st,
             "revoke execute on function f_abs from mamta3 restrict");
        assertStatementError
            ("X0Y25", st,
             "revoke execute on function f_abs2 from mamta3 restrict");
        st_mamta3.executeUpdate("alter table dhw drop constraint a2");
        st.executeUpdate(
            "revoke execute on function f_abs2 from mamta3 restrict");

        // check that a1 is still in place
        assertStatementError
            ("23513", st_mamta3, "insert into dhw values (0,1)");
        assertStatementError
            ("X0Y25", st,
             "revoke execute on function f_abs from mamta3 restrict");
        // remove  final constraint
        st_mamta3.executeUpdate("alter table dhw drop constraint a1");
        st.executeUpdate
            ("revoke execute on function f_abs from mamta3 restrict");
        st_mamta3.executeUpdate("insert into dhw values (0,0)");

        st_mamta3.executeUpdate(" drop table dhw");

        // DERBY-3743b, test 2: one constraint, multiple routine deps
        st.executeUpdate(
            " grant execute on function f_abs to mamta3");
        st.executeUpdate(
            " grant execute on function f_abs2 to mamta3");
        st_mamta3.executeUpdate(
            " create table dhw(i int constraint a check(" +
                              "mamta1.f_abs(i) + mamta1.f_abs2(i) > 0))");
        assertStatementError
            ("X0Y25", st,
             "revoke execute on function f_abs from mamta3 restrict");
        assertStatementError
            ("X0Y25", st,
             "revoke execute on function f_abs2 from mamta3 restrict");
        st_mamta3.executeUpdate("alter table dhw drop constraint a");

        st.executeUpdate
            ("revoke execute on function f_abs from mamta3 restrict");
        st.executeUpdate
            ("revoke execute on function f_abs2 from mamta3 restrict");

        st_mamta3.executeUpdate(" drop table dhw");

        st.executeUpdate("DROP FUNCTION F_ABS");
        st.executeUpdate("DROP FUNCTION F_ABS2");

        // set connection mamta2
        //ij(MAMTA3)> -- DERBY-1847 SELECT statement asserts with 
        // XJ001 when attempted to select a newly added column 
        // Grant access on 2 columns and then add another column to 
        // the table. The select on the new column by another user 
        // should complain about no permissions granted on that new 
        // column.
        
        
        st_mamta2.executeUpdate(
            " create table t1Derby1847 (c1 int, c2 int)");
        
        st_mamta2.executeUpdate(
            " grant select(c1,c2) on t1Derby1847 to mamta3");
        
        st_mamta2.executeUpdate(
            " alter table t1Derby1847 add c3 int");
        
        // set connection mamta3
        
        // should fail because mamta3 doesn't have any permission 
        // on this column in table mamta2.t1Derby1847
        
        assertStatementError("42502", st_mamta3,
            "select c3 from mamta2.t1Derby1847");
        
        // set connection mamta2
        
        st_mamta2.executeUpdate(
            " grant select on t1Derby1847 to mamta3");
        
        // set connection mamta3
        
        // should work now because mamta3 got select permission on 
        // new column in table mamta2.t1Derby1847 through table 
        // level select permission
        
        rs = st_mamta3.executeQuery(
            "select c3 from mamta2.t1Derby1847");
        
        expColNames = new String [] {"C3"};
        JDBC.assertColumnNames(rs, expColNames);
        
        JDBC.assertEmpty(rs);
        
        // set connection mamta2
        
        st_mamta2.executeUpdate(
            " revoke select on t1Derby1847 from mamta3");
        
        // set connection mamta3
        
        // should fail because mamta3 lost it's select permission 
        // on new column in table mamta2.t1Derby1847
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_mamta3,
            "select c3 from mamta2.t1Derby1847");
        
        // set connection mamta2
        
        st_mamta2.executeUpdate(
            " grant select(c3) on t1Derby1847 to mamta3");
        
        // set connection mamta3
        
        // should work now because mamta3 got select permission on 
        // new column in table mamta2.t1Derby1847 through column 
        // level select permission
        
        rs = st_mamta3.executeQuery(
            "select c3 from mamta2.t1Derby1847");
        
        expColNames = new String [] {"C3"};
        JDBC.assertColumnNames(rs, expColNames);
        
        JDBC.assertEmpty(rs);
        
        // set connection mamta2
        
        st_mamta2.executeUpdate(
            " drop table t1Derby1847");
        
        // set connection mamta3
        
        assertStatementError("42X05", st_mamta3,
            " select c3 from mamta2.t1Derby1847");
        
        // set connection user1
        //ij(MAMTA3)> -- DERBY-1716 Revoking select privilege from 
        // a user times out when that user still have a cursor open 
        // before the patch.
        
        
        assertStatementError("42Y55", st_user1,
            " drop table t1");
        
        st_user1.executeUpdate(
            " create table t1 (c varchar(1))");
        
        st_user1.executeUpdate(
            " insert into t1 values 'a', 'b', 'c'");
        
        st_user1.executeUpdate(
            " grant select on t1 to user2");
        
        // set connection user2
        
        user2.setAutoCommit(false);
        
        
        // set connection user1
        // -- repeat the scenario
        
        st_user1.executeUpdate(
            " grant select on t1 to user2");
        
        // set connection user2
        
        user2.setAutoCommit(false);
        
        PreparedStatement ps_crs1 = user2.prepareStatement(
            "select * from user1.t1");
        
        ResultSet crs1 = ps_crs1.executeQuery();
        
        crs1.next();
        assertEquals("a", crs1.getString(1));

        // set connection user1
        // should succeed without blocking
        
        st_user1.executeUpdate(
            "revoke select on t1 from user2");
        
        // set connection user2
        // still ok to fetch.
        
        crs1.next();
        assertEquals("b", crs1.getString(1));
        crs1.next();
        assertEquals("c", crs1.getString(1));
        crs1.close();
        ps_crs1.close();
        user2.commit();
        
        // next should fail since select privilege got revoked
        
        PreparedStatement ps_crs2 = user2.prepareStatement(
                "select * from user1.t1");
        
        try {
            ResultSet crs2 = ps_crs2.executeQuery();
        } catch (SQLException e) {
            assertSQLState("42502", e);
        }
        
        user2.setAutoCommit(true);
        
        // set connection user1
        // -- repeat the scenario
        
        st_user1.executeUpdate(
            " grant select on t1 to user2");
        
        // set connection user2
        
        user2.setAutoCommit(false);
        
        ps_crs1 = user2.prepareStatement(
            "select * from user1.t1");
        
        crs1 = ps_crs1.executeQuery();
        
        crs1.next();
        assertEquals("a", crs1.getString(1));

        // set connection user1
        // should succeed without blocking
        
        st_user1.executeUpdate(
            "revoke select on t1 from user2");
        
        // set connection user2
        // still ok to fetch.
        
        crs1.next();
        assertEquals("b", crs1.getString(1));
        crs1.next();
        assertEquals("c", crs1.getString(1));
        crs1.close();
        ps_crs1.close();
        user2.commit();
        
        // next should fail since select privilege got revoked
        
        ps_crs2 = user2.prepareStatement(
                "select * from user1.t1");
        
        try {
            ResultSet crs2 = ps_crs2.executeQuery();
        } catch (SQLException e) {
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
            assertSQLState("42502", e);
        }
        
        // set connection user1
        user1.setAutoCommit(true);
        
        
        // Another test for DERBY-1847: verify that columns field 
        // is updated correctly when adding a column to a table:
        
        st_user1.executeUpdate(
            "create table d1847_c (a int, b int, c int)");
        
        st_user1.executeUpdate(
            " grant select (a) on d1847_c to first_user");
        
        st_user1.executeUpdate(
            " grant update (b) on d1847_c to second_user");
        
        st_user1.executeUpdate(
            " grant select (c) on d1847_c to third_user");
        
        rs = st_user1.executeQuery(
            " select c.grantee, c.type, c.columns from "
            + "sys.syscolperms c, sys.systables t where c.tableid = "
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            + "t.tableid and t.tablename='D1847_C' order by grantee");
        
        expColNames = new String [] {"GRANTEE", "TYPE", "COLUMNS"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"FIRST_USER", "s", "{0}"},
            {"SECOND_USER", "u", "{1}"},
            {"THIRD_USER", "s", "{2}"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st_user1.executeUpdate(
            " alter table d1847_c add column d int");
        
        rs = st_user1.executeQuery(
            " select c.grantee, c.type, c.columns from "
            + "sys.syscolperms c, sys.systables t where c.tableid = "
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            + "t.tableid and t.tablename='D1847_C' order by GRANTEE");
        
        expColNames = new String [] {"GRANTEE", "TYPE", "COLUMNS"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"FIRST_USER", "s", "{0}"},
            {"SECOND_USER", "u", "{1}"},
            {"THIRD_USER", "s", "{2}"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // Drop everything
        
        st.executeUpdate("DROP VIEW TEST_DBO.V11");
        st.executeUpdate("DROP VIEW MAMTA1.V23");
        st.executeUpdate("DROP VIEW MAMTA2.V21");
        st.executeUpdate("DROP VIEW MAMTA2.V21VIEWTEST");
        st.executeUpdate("DROP VIEW MAMTA2.V22");
        st.executeUpdate("DROP VIEW MAMTA2.V23");
        st.executeUpdate("DROP VIEW MAMTA2.V26");
        st.executeUpdate("DROP VIEW MAMTA2.V27");
        st.executeUpdate("DROP VIEW MAMTA2.V28");
        st.executeUpdate("DROP VIEW MAMTA3.V21VIEWTEST");
        st.executeUpdate("DROP FUNCTION MAMTA1.SELECTFROMSPECIFICSCHEMA");
        st.executeUpdate("DROP TABLE MAMTA4.T41TRIGGERTEST");
        st.executeUpdate("DROP TABLE MAMTA3.D1589T31CONSTRAINTTEST");
        st.executeUpdate("DROP TABLE MAMTA3.T31");
        st.executeUpdate("DROP TABLE MAMTA3.T31CONSTRAINTTEST");
        st.executeUpdate("DROP TABLE MAMTA3.T31TRIGGERTEST");
        st.executeUpdate("DROP TABLE MAMTA2.T21CONSTRAINTTEST");
        st.executeUpdate("DROP TABLE MAMTA2.T21TRIGGERTEST");
        st.executeUpdate("DROP TABLE MAMTA2.TTT1");
        st.executeUpdate("DROP TABLE MAMTA1.SYSFUN_MATH_TEST");
        st.executeUpdate("DROP TABLE MAMTA1.D1589T11CONSTRAINTTEST");
        st.executeUpdate("DROP TABLE MAMTA1.T11CONSTRAINTTEST");
        st.executeUpdate("DROP TABLE MAMTA1.T11TRIGGERREVOKETEST");
        st.executeUpdate("DROP TABLE MAMTA1.T11TRIGGERTEST");
        st.executeUpdate("DROP TABLE MAMTA1.T11");
        st.executeUpdate("DROP TABLE MAMTA1.T12");
        st.executeUpdate("DROP TABLE MAMTA1.T12ROUTINETEST");
        st.executeUpdate("DROP TABLE MAMTA1.T12TRIGGERREVOKETEST");
        st.executeUpdate("DROP TABLE MAMTA1.T13");
        st.executeUpdate("DROP TABLE MAMTA1.T14");
        st.executeUpdate("DROP TABLE MAMTA1.T15");
        st.executeUpdate("DROP TABLE MAMTA1.T1001");
        st.executeUpdate("DROP TABLE MONICA.SHOULDPASS");
        st.executeUpdate("DROP TABLE SAM.SAMTABLE");
        st.executeUpdate("DROP TABLE SWIPER.MYTAB");
        st.executeUpdate("DROP TABLE SWIPER.MY_TSAT");
        st.executeUpdate("DROP TABLE SWIPER.SWIPERTAB");
        st.executeUpdate("DROP TABLE USER1.D1847_C");
        st.executeUpdate("DROP TABLE USER1.T1");
        st.executeUpdate("DROP TABLE USER1.T100");
        st.executeUpdate("DROP SCHEMA DERBY RESTRICT");
        st.executeUpdate("DROP SCHEMA GEORGE RESTRICT");
        st.executeUpdate("DROP SCHEMA MAMTA1 RESTRICT");
        st.executeUpdate("DROP SCHEMA MAMTA2 RESTRICT");
        st.executeUpdate("DROP SCHEMA MAMTA3 RESTRICT");
        st.executeUpdate("DROP SCHEMA MAMTA4 RESTRICT");
        st.executeUpdate("DROP SCHEMA MONICA RESTRICT");
        st.executeUpdate("DROP SCHEMA MYDODO RESTRICT");
        st.executeUpdate("DROP SCHEMA MYFRIEND RESTRICT");
        st.executeUpdate("DROP SCHEMA MYSCHEMA RESTRICT");
        st.executeUpdate("DROP SCHEMA SAM RESTRICT");
        st.executeUpdate("DROP SCHEMA SWIPER RESTRICT");
        st.executeUpdate("DROP SCHEMA TESTSCHEMA RESTRICT");
        st.executeUpdate("DROP SCHEMA TEST_DBO RESTRICT");
        st.executeUpdate("DROP SCHEMA USER1 RESTRICT");
        st.executeUpdate("DROP SCHEMA USER2 RESTRICT");
        
        //close Statements
        st_barConnection.close();
        st_CONNECTION0.close();
        st_CONNECTION1.close();
        st_mamta1.close();
        st_mamta2.close();
        st_mamta3.close();
        st_mamta4.close();
        st_monicaConnection.close();
        st_samConnection.close();
        st_swiperConnection.close();
        st_satConnection.close();
        st_sammyConnection.close();
        st_user1.close();
        st_user2.close();
        st.close();
        
        //close Connections
        barConnection.close();
        CONNECTION0.close();
        CONNECTION1.close();
        mamta1.close();
        mamta2.close();
        mamta3.close();
        mamta4.close();
        monicaConnection.close();
        samConnection.close();
        sammyConnection.close();
        satConnection.close();
        swiperConnection.close();
        user1.close();
        user2.close();
    }
    
        
    public void testGrantRevokeDDL2() throws SQLException {

    	ResultSet rs = null;
        SQLWarning sqlWarn = null;

        Statement st = createStatement();

        String [][] expRS;
        String [] expColNames;
        
        st.executeUpdate("CREATE SCHEMA AUTHORIZATION USER1");
        st.executeUpdate("CREATE SCHEMA AUTHORIZATION USER2");
    	
        Connection user1 = openUserConnection("user1");
        Statement st_user1 = user1.createStatement();
        
        Connection user2 = openUserConnection("user2");
        Statement st_user2 = user2.createStatement();
        
        Connection user3 = openUserConnection("user3");
        Statement st_user3 = user3.createStatement();
        
        Connection user4 = openUserConnection("user4");
        Statement st_user4 = user4.createStatement();

        Connection user5 = openUserConnection("user5");
        Statement st_user5 = user5.createStatement();

        // set connection user1
        //ij(USER5)> -- DERBY-1729 test grant and revoke in Java 
        // stored procedure with triggers. Java stored procedure 
        // that contains grant or revoke statement requires 
        // MODIFIES SQL DATA to execute. Since only 2 of the 8 Java 
        // stored procedures(which contains grant or revoke 
        // statement) are declared with MODIFIES SQL DATA, the rest 
        // are expected to fail in this test. setup the environment
        
        
        // table used in the procedures
                
        st_user1.executeUpdate(
            " create table t1 (i int primary key, b char(15))");
        
        st_user1.executeUpdate(
            " insert into t1 values (1, 'XYZ')");
        
        st_user1.executeUpdate(
            " insert into t1 values (2, 'XYZ')");
        
        st_user1.executeUpdate(
            " insert into t1 values (3, 'XYZ')");
        
        st_user1.executeUpdate(
            " insert into t1 values (4, 'XYZ')");
        
        st_user1.executeUpdate(
            " insert into t1 values (5, 'XYZ')");
        
        st_user1.executeUpdate(
            " insert into t1 values (6, 'XYZ')");
        
        st_user1.executeUpdate(
            " insert into t1 values (7, 'XYZ')");
        
        st_user1.executeUpdate(
            " insert into t1 values (8, 'XYZ')");
        
        // table used in this test
        
        assertStatementError("42Y55", st_user1,
            "drop table t2");
        
        st_user1.executeUpdate(
            " create table t2 (x integer, y integer)");
        
        st_user1.executeUpdate(
            " create procedure grant_select_proc1() parameter "
            + "style java dynamic result sets 0 language java NO "
            + "SQL external name "
            + "'org.apache.derbyTesting.functionTests.util.ProcedureTest.grantSelect'");
        
        st_user1.executeUpdate(
            " create procedure grant_select_proc2() parameter "
            + "style java dynamic result sets 0 language "
            + "java CONTAINS SQL external name "
            + "'org.apache.derbyTesting.functionTests.util.ProcedureTest.grantSelect'");
        
        st_user1.executeUpdate(
            " create procedure grant_select_proc3() parameter "
            + "style java dynamic result sets 0 language java READS "
            + "SQL DATA external name "
            + "'org.apache.derbyTesting.functionTests.util.ProcedureTest.grantSelect'");
        
        st_user1.executeUpdate(
            " create procedure grant_select_proc4() parameter "
            + "style java dynamic result sets 0 language "
            + "java MODIFIES SQL DATA external name "
            + "'org.apache.derbyTesting.functionTests.util.ProcedureTest.grantSelect'");
        
        st_user1.executeUpdate(
            " create procedure revoke_select_proc1() parameter "
            + "style java dynamic result sets 0 language java NO "
            + "SQL external name "
            + "'org.apache.derbyTesting.functionTests.util.ProcedureTest.revokeSelect'");
        
        st_user1.executeUpdate(
            " create procedure revoke_select_proc2() parameter "
            + "style java dynamic result sets 0 language "
            + "java CONTAINS SQL external name "
            + "'org.apache.derbyTesting.functionTests.util.ProcedureTest.revokeSelect'");
        
        st_user1.executeUpdate(
            " create procedure revoke_select_proc3() parameter "
            + "style java dynamic result sets 0 language java READS "
            + "SQL DATA external name "
            + "'org.apache.derbyTesting.functionTests.util.ProcedureTest.revokeSelect'");
        
        st_user1.executeUpdate(
            " create procedure revoke_select_proc4() parameter "
            + "style java dynamic result sets 0 language "
            + "java MODIFIES SQL DATA external name "
            + "'org.apache.derbyTesting.functionTests.util.ProcedureTest.revokeSelect'");
        
        // tests
        
        st_user1.executeUpdate(
            "create trigger grant_select_trig AFTER delete on "
            + "t1 for each STATEMENT call grant_select_proc1()");
        
        // should fail
        
        assertStatementError("38001", st_user1,
            "delete from t1 where i = 1");
        
        // check delete failed
        
        rs = st_user1.executeQuery(
            "select * from t1 where i = 1");
        
        expColNames = new String [] {"I", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "XYZ"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st_user1.executeUpdate(
            " drop trigger grant_select_trig");
        
        // set connection user2
        
        // should fail
        
        assertStatementError("42502", st_user2,
            "select * from user1.t1 where i = 1");
        
        // set connection user1
        
        st_user1.executeUpdate(
            " create trigger grant_select_trig AFTER delete on "
            + "t1 for each STATEMENT call grant_select_proc2()");
        
        // should fail
        
        assertStatementError("38002", st_user1,
            "delete from t1 where i = 2");
        
        // check delete failed
        
        rs = st_user1.executeQuery(
            "select * from t1 where i = 2");
        
        expColNames = new String [] {"I", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2", "XYZ"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st_user1.executeUpdate(
            " drop trigger grant_select_trig");
        
        // set connection user2
        
        // should fail
        
        assertStatementError("42502", st_user2,
            "select * from user1.t1 where i = 1");
        
        // set connection user1
        
        st_user1.executeUpdate(
            " create trigger grant_select_trig AFTER delete on "
            + "t1 for each STATEMENT call grant_select_proc3()");
        
        // should fail
        
        assertStatementError("38002", st_user1,
            "delete from t1 where i = 3");
        
        // check delete failed
        
        rs = st_user1.executeQuery(
            "select * from t1 where i = 3");
        
        expColNames = new String [] {"I", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"3", "XYZ"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st_user1.executeUpdate(
            " drop trigger grant_select_trig");
        
        // set connection user2
        
        // should fail
        
        assertStatementError("42502", st_user2,
            "select * from user1.t1 where i = 1");
        
        // set connection user1
        
        st_user1.executeUpdate(
            " create trigger grant_select_trig AFTER delete on "
            + "t1 for each STATEMENT call grant_select_proc4()");
        
        // ok
        
        assertUpdateCount(st_user1, 1,
            "delete from t1 where i = 4");
        
        // check delete
        
        rs = st_user1.executeQuery(
            "select * from t1 where i = 4");
        
        expColNames = new String [] {"I", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        st_user1.executeUpdate(
            " drop trigger grant_select_trig");
        
        // set connection user2
        
        // should be successful
        
        rs = st_user2.executeQuery(
            "select * from user1.t1 where i = 1");
        
        expColNames = new String [] {"I", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "XYZ"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection user1
        
        st_user1.executeUpdate(
            " create trigger revoke_select_trig AFTER delete on "
            + "t1 for each STATEMENT call revoke_select_proc1()");
        
        // should fail
        
        assertStatementError("38001", st_user1,
            "delete from t1 where i = 5");
        
        // check delete failed
        
        rs = st_user1.executeQuery(
            "select * from t1 where i = 5");
        
        expColNames = new String [] {"I", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"5", "XYZ"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st_user1.executeUpdate(
            " drop trigger revoke_select_trig");
        
        // set connection user2
        
        // should be successful
        
        rs = st_user2.executeQuery(
            "select * from user1.t1 where i = 1");
        
        expColNames = new String [] {"I", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "XYZ"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection user1
        
        st_user1.executeUpdate(
            " create trigger revoke_select_trig AFTER delete on "
            + "t1 for each STATEMENT call revoke_select_proc2()");
        
        // should fail
        
        assertStatementError("38002", st_user1,
            "delete from t1 where i = 6");
        
        // check delete failed
        
        rs = st_user1.executeQuery(
            "select * from t1 where i = 6");
        
        expColNames = new String [] {"I", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"6", "XYZ"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st_user1.executeUpdate(
            " drop trigger revoke_select_trig");
        
        // set connection user2
        
        // should be successful
        
        rs = st_user2.executeQuery(
            "select * from user1.t1 where i = 1");
        
        expColNames = new String [] {"I", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "XYZ"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection user1
        
        st_user1.executeUpdate(
            " create trigger revoke_select_trig AFTER delete on "
            + "t1 for each STATEMENT call revoke_select_proc3()");
        
        // should fail
        
        assertStatementError("38002", st_user1,
            "delete from t1 where i = 7");
        
        // check delete failed
        
        rs = st_user1.executeQuery(
            "select * from t1 where i = 7");
        
        expColNames = new String [] {"I", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"7", "XYZ"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st_user1.executeUpdate(
            " drop trigger revoke_select_trig");
        
        // set connection user2
        
        // should be successful
        
        rs = st_user2.executeQuery(
            "select * from user1.t1 where i = 1");
        
        expColNames = new String [] {"I", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "XYZ"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection user1
        
        st_user1.executeUpdate(
            " create trigger revoke_select_trig AFTER delete on "
            + "t1 for each STATEMENT call revoke_select_proc4()");
        
        // ok
        
        assertUpdateCount(st_user1, 1,
            "delete from t1 where i = 8");
        
        // check delete
        
        rs = st_user1.executeQuery(
            "select * from t1 where i = 8");
        
        expColNames = new String [] {"I", "B"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        st_user1.executeUpdate(
            " drop trigger revoke_select_trig");
        
        // set connection user2
        
        // should fail
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_user2,
            "select * from user1.t1 where i = 1");
        
        // set connection user1
        
        st_user1.executeUpdate(
            " drop table t2");
        
        st_user1.executeUpdate(
            " drop table t1");
        
        // set connection user1
        // 
        // ---------------------------------------------------------
        // ---------- table privileges (tp) 
        // ---------------------------------------------------------
        // ----------
        
        
        st_user1.executeUpdate(
            " create table t1 (c1 int primary key not null, c2 "
            + "varchar(10))");
        
        st_user1.executeUpdate(
            " create table t2 (c1 int primary key not null, c2 "
            + "varchar(10), c3 int)");
        
        st_user1.executeUpdate(
            " create index idx1 on t1(c2)");
        
        st_user1.executeUpdate(
            " insert into t1 values (1, 'a'), (2, 'b'), (3, 'c')");
        
        st_user1.executeUpdate(
            " insert into t2 values (1, 'Yip', 10)");
        
        rs = st_user1.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select * from t1 order by C1");
        
        expColNames = new String [] {"C1", "C2"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "a"},
            {"2", "b"},
            {"3", "c"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st_user1.executeUpdate(
            " CREATE FUNCTION F_ABS1(P1 INT) RETURNS INT NO "
            + "SQL RETURNS NULL ON NULL INPUT EXTERNAL NAME "
            + "'java.lang.Math.abs' LANGUAGE JAVA PARAMETER STYLE JAVA");
        
        rs = st_user1.executeQuery(
            " values f_abs1(-5)");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"5"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // grant on a non-existing table, expect error
        
        assertStatementError("42X05", st_user1,
            "grant select on table t0 to user2");
        
        // revoke on a non-existing table, expect error
        
        assertStatementError("42X05", st_user1,
            "revoke select on table t0 from user2");
        
        // grant more than one table, expect error
        
        assertStatementError("42X01", st_user1,
            "grant select on t0, t1 to user2");
        
        // revoke more than one table, expect error
        
        assertStatementError("42X01", st_user1,
            "revoke select on t0, t1 from user2");
        
        // revoking privilege that has not been granted, expect 
        // warning
        
        st_user1.executeUpdate(
            "revoke "
            + "select,insert,update,delete,trigger,references on "
            + "t1 from user2");
        
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_user1 != null))
                sqlWarn = st_user1.getWarnings();
            if (sqlWarn == null)
                sqlWarn = user1.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01006", sqlWarn);
            sqlWarn = null;
        }
        
        // syntax errors, expect errors
        
        assertStatementError("42X01", st_user1,
            "grant select on t1 from user2");
        
        assertStatementError("42X01", st_user1,
            " revoke select on t1 to user2");
        
        // redundant but ok
        
        st_user1.executeUpdate(
            "grant select, select on t1 to user2");
        
        st_user1.executeUpdate(
            " revoke select, select on t1 from user2");
        
        // set connection user2
        //ij(USER1)> -- switch to user2
        
        
        // test SELECT privilege, expect error
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_user2,
            "select * from user1.t1");
        
        // test INSERT privilege, expect error
        
        assertStatementError("42500", st_user2,
            "insert into user1.t1(c1) values 4");
        
        // test UPDATE privilege, expect error
        
        assertStatementError("42502", st_user2,
            "update user1.t1 set c1=10");
        
        // test DELETE privilege, expect error
        
        assertStatementError("42500", st_user2,
            "delete from user1.t1");
        
        // test REFERENCES privilege, expect error
        
        assertStatementError("42502", st_user2,
            "create table t2 (c1 int primary key not null, c2 "
            + "int references user1.t1)");
        
        // test TRIGGER privilege, expect error
        
        assertStatementError("42500", st_user2,
            "create trigger trigger1 after update on user1.t1 "
            + "for each statement values integer('123')");
        
        // try to DROP user1.idx1 index, expect error
        
        assertStatementError("42507", st_user2,
            "drop index user1.idx1");
        
        // try to DROP user1.t1 table, expect error
        
        assertStatementError("42507", st_user2,
            "drop table user1.t1");
        
        // non privileged user try to grant privileges on 
        // user1.t1, expect error
        
        assertStatementError("42506", st_user2,
            "grant "
            + "select,insert,delete,update,references,trigger on "
            + "user1.t1 to user2");
        
        // try to grant privileges for public on user1.t1, expect 
        // error
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42506", st_user2,
            "grant "
            + "select,insert,delete,update,references,trigger on "
            + "user1.t1 to public");
        
        // try to grant all privileges for user2 on user1.t1, 
        // expect error
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42506", st_user2,
            "grant ALL PRIVILEGES on user1.t1 to user2");
        
        // try to grant all privileges on user1.t1 to public, 
        // expect error
        
        assertStatementError("42506", st_user2,
            "grant ALL PRIVILEGES on user1.t1 to public");
        
        // try to revoke user1 from table user1.t1, expect error
        
        assertStatementError("42509", st_user2,
            "revoke "
            + "select,insert,delete,update,references,trigger on "
            + "user1.t1 from user1");
        
        // try to revoke all privileges from user1 on table 
        // user1.t1, expect error
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42509", st_user2,
            "revoke ALL PRIVILEGES on user1.t1 from user1");
        
        // try to revoke execute on a non-existing function on 
        // user1.t1, expect error
        
        assertStatementError("42509", st_user2,
            "revoke execute on function user1.f1 from user1 restrict");
        
        st_user2.executeUpdate(
            " create table t2 (c1 int)");
        
        // try revoking yourself from user2.t2, expect error
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42509", st_user2,
            "revoke select on t2 from user2");
        
        // try granting yourself again on user2.t2, expect error. Why?
        
        assertStatementError("42509", st_user2,
            "grant select on t2 to user2");
        
        // try granting yourself multiple times, expect error.  Why?
        
        assertStatementError("42509", st_user2,
            "grant insert on t2 to user2,user2,user2");
        
        // try to execute user1.F_ABS1, expect error
        
        assertStatementError("42504", st_user2,
            "values user1.F_ABS1(-9)");
        
        // set connection user1
        
        rs = st_user1.executeQuery(
            " select * from sys.systableperms");
        
        expColNames = new String [] {"TABLEPERMSID", "GRANTEE", "GRANTOR", "TABLEID", "SELECTPRIV", "DELETEPRIV", "INSERTPRIV", "UPDATEPRIV", "REFERENCESPRIV", "TRIGGERPRIV"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        rs = st_user1.executeQuery(
            " select * from sys.syscolperms");
        
        expColNames = new String [] {"COLPERMSID", "GRANTEE", "GRANTOR", "TABLEID", "TYPE", "COLUMNS"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        rs = st_user1.executeQuery(
            " select GRANTEE, GRANTOR, GRANTOPTION from sys.sysroutineperms");
        
        expColNames = new String [] {"GRANTEE", "GRANTOR", "GRANTOPTION"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"PUBLIC", "TEST_DBO", "N"},
            {"PUBLIC", "TEST_DBO", "N"},
            {"PUBLIC", "TEST_DBO", "N"},
            {"PUBLIC", "TEST_DBO", "N"},
            {"PUBLIC", "TEST_DBO", "N"},
            {"PUBLIC", "TEST_DBO", "N"},
            {"PUBLIC", "TEST_DBO", "N"},
            {"PUBLIC", "TEST_DBO", "N"},
            {"PUBLIC", "TEST_DBO", "N"},
            {"PUBLIC", "TEST_DBO", "N"},
            {"PUBLIC", "TEST_DBO", "N"},
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st_user1.executeUpdate(
            " grant select,update on table t1 to user2, user3");
        
        st_user1.executeUpdate(
            " grant execute on function F_ABS1 to user2");
        
        rs = st_user1.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select GRANTEE, GRANTOR, SELECTPRIV, DELETEPRIV, INSERTPRIV, UPDATEPRIV, REFERENCESPRIV, TRIGGERPRIV from sys.systableperms order by GRANTEE, GRANTOR");
        
        expColNames = new String [] {"GRANTEE", "GRANTOR", "SELECTPRIV", "DELETEPRIV", "INSERTPRIV", "UPDATEPRIV", "REFERENCESPRIV", "TRIGGERPRIV"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"USER2", "USER1", "y", "N", "N", "y", "N", "N"},
            {"USER3", "USER1", "y", "N", "N", "y", "N", "N"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_user1.executeQuery(
            " select * from sys.syscolperms");
        
        expColNames = new String [] {"COLPERMSID", "GRANTEE", "GRANTOR", "TABLEID", "TYPE", "COLUMNS"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);

        rs = st_user1.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select GRANTEE, GRANTOR, GRANTOPTION from sys.sysroutineperms order by GRANTEE, GRANTOR");
        
        expColNames = new String [] {"GRANTEE", "GRANTOR", "GRANTOPTION"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"PUBLIC", "TEST_DBO", "N"},
            {"PUBLIC", "TEST_DBO", "N"},
            {"PUBLIC", "TEST_DBO", "N"},
            {"PUBLIC", "TEST_DBO", "N"},
            {"PUBLIC", "TEST_DBO", "N"},
            {"PUBLIC", "TEST_DBO", "N"},
            {"PUBLIC", "TEST_DBO", "N"},
            {"PUBLIC", "TEST_DBO", "N"},
            {"PUBLIC", "TEST_DBO", "N"},
            {"PUBLIC", "TEST_DBO", "N"},
            {"PUBLIC", "TEST_DBO", "N"},
            {"USER2", "USER1", "N"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection user2
        
        // try to select from t1, ok
        
        rs = st_user2.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            "select * from user1.t1 order by C1");
        
        expColNames = new String [] {"C1", "C2"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "a"},
            {"2", "b"},
            {"3", "c"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // try to insert from t1, expect error
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42500", st_user2,
            "insert into user1.t1 values (5, 'e')");
        
        // ok
        
        rs = st_user2.executeQuery(
            "values user1.F_ABS1(-8)");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"8"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // ok
        
        assertUpdateCount(st_user2, 3,
            "update user1.t1 set c2 = 'user2'");
        
        // set connection user1
        
        // add a column to t1, user2 should still be able to select
        
        st_user1.executeUpdate(
            "alter table t1 add column c3 varchar(10)");
        
        // set connection user2
        
        // ok
        
        rs = st_user2.executeQuery(
            "select * from user1.t1 order by C1");
        
        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "user2", null},
            {"2", "user2", null},
            {"3", "user2", null}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // error
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42500", st_user2,
            "insert into user1.t1 values (2, 'abc', 'ABC')");
        
        // ok
        
        assertUpdateCount(st_user2, 3,
            "update user1.t1 set c3 = 'XYZ'");
        
        // set connection user3
        
        // try to select from t1, ok
        
        rs = st_user3.executeQuery(
            "select * from user1.t1 order by C1");
        
        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "user2", "XYZ"},
            {"2", "user2", "XYZ"},
            {"3", "user2", "XYZ"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // user3 does not have permission to execute, expect error
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42504", st_user3,
            "values user1.F_ABS1(-8)");
        
        // ok
        
        assertUpdateCount(st_user3, 3,
            "update user1.t1 set c2 = 'user3'");
        
        // set connection user1
        
        // expect warnings
        
        st_user1.executeUpdate(
            "revoke update(c2) on t1 from user3");
        
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_user1 != null))
                sqlWarn = st_user1.getWarnings();
            if (sqlWarn == null)
                sqlWarn = user1.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01006", sqlWarn);
            sqlWarn = null;
        }
        
        st_user1.executeUpdate(
            " revoke select(c2) on t1 from user3");
        
        // set connection user2
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_user1 != null))
                sqlWarn = st_user1.getWarnings();
            if (sqlWarn == null)
                sqlWarn = user1.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01006", sqlWarn);
            sqlWarn = null;
        }
        
        
        // ok
        
        assertUpdateCount(st_user2, 3,
            "update user1.t1 set c2 = 'user2'");
        
        // set connection user3
        
        // revoking part of table privilege raises warning, so ok
        
        assertUpdateCount(st_user3, 3,
            "update user1.t1 set c2 = 'user3'");
        
        // same as above
        
        rs = st_user3.executeQuery(
            "select * from user1.t1 order by C1");
        
        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "user3", "XYZ"},
            {"2", "user3", "XYZ"},
            {"3", "user3", "XYZ"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // same as above
        
        rs = st_user3.executeQuery(
            "select c2 from user1.t1");
        
        expColNames = new String [] {"C2"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"user3"},
            {"user3"},
            {"user3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection user1
        
        st_user1.executeUpdate(
            " grant select, update on t1 to PUBLIC");
        
        // set connection user3
        
        // ok, use PUBLIC
        
        rs = st_user3.executeQuery(
            "select * from user1.t1 order by C1");
        
        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "user3", "XYZ"},
            {"2", "user3", "XYZ"},
            {"3", "user3", "XYZ"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // ok, use PUBLIC
        
        assertUpdateCount(st_user3, 3,
            "update user1.t1 set c2 = 'user3'");
        
        // set connection user1
        
        st_user1.executeUpdate(
            " grant select on t1 to user3");
        
        // revoke select from PUBLIC
        
        st_user1.executeUpdate(
            "revoke select on t1 from PUBLIC");
        
        // set connection user3
        
        // ok, privileged
        
        rs = st_user3.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            "select * from user1.t1 order by C1");
        
        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "user3", "XYZ"},
            {"2", "user3", "XYZ"},
            {"3", "user3", "XYZ"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // ok, use PUBLIC
        
        assertUpdateCount(st_user3, 3,
            "update user1.t1 set c2 = 'user3'");
        
        // set connection user1
        
        st_user1.executeUpdate(
            " revoke select, update on t1 from user3");
        
        st_user1.executeUpdate(
            " revoke update on t1 from PUBLIC");
        
        // set connection user3
        
        // expect error
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_user3,
            "select * from user1.t1");
        
        // expect error
        
        assertStatementError("42502", st_user3,
            "update user1.t1 set c2 = 'user3'");
        
        // set connection test_dbo
        
        assertUpdateCount(st, 0,
            " declare global temporary table SESSION.t1(c1 int) "
            + "not logged");
        
        // expect error
        
        assertStatementError("XCL51", st,
            "grant select on session.t1 to user2");
        
        assertStatementError("XCL51", st,
            " revoke select on session.t1 from user2");
        
        // set connection user1
        // 
        // ---------------------------------------------------------
        // ---------- column privileges 
        // ---------------------------------------------------------
        // ----------
        
        
        st_user1.executeUpdate(
            " create table t3 (c1 int, c2 varchar(10), c3 int)");
        
        st_user1.executeUpdate(
            " create table t4 (c1 int, c2 varchar(10), c3 int)");
        
        // grant table select privilege then revoke partially
        
        st_user1.executeUpdate(
            "grant select, update on t3 to user2");
        
        // expect warning
        
        st_user1.executeUpdate(
            "revoke select(c1) on t3 from user2");
        
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_user1 != null))
                sqlWarn = st_user1.getWarnings();
            if (sqlWarn == null)
                sqlWarn = user1.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01006", sqlWarn);
            sqlWarn = null;
        }
        
        st_user1.executeUpdate(
            " revoke update(c2) on t3 from user2");
        
        // set connection user2
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_user1 != null))
                sqlWarn = st_user1.getWarnings();
            if (sqlWarn == null)
                sqlWarn = user1.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01006", sqlWarn);
            sqlWarn = null;
        }
        
        
        rs = st_user2.executeQuery(
            " select * from user1.t3");
        
        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        // set connection user1
        
        st_user1.executeUpdate(
            " grant select (c2, c3), update (c2), insert on t4 to user2");
        
        // set connection user2
        
        // expect error
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_user2,
            "select * from user1.t4");
        
        // expect error
        
        assertStatementError("42502", st_user2,
            "select c1 from user1.t4");
        
        // ok
        
        rs = st_user2.executeQuery(
            "select c2, c3 from user1.t4");
        
        expColNames = new String [] {"C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        // expect error
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_user2,
            "update user1.t4 set c1=10, c3=100");
        
        // ok
        
        assertUpdateCount(st_user2, 0,
            "update user1.t4 set c2='XYZ'");
        
        // set connection user1
        
        // set connection user1
        // DERBY-1847 alter table t4 add column c4 int set 
        // connection user2 expect error select c4 from user1.t4 ok 
        // select c2 from user1.t4
        
        
        // revoke all columns
        
        st_user1.executeUpdate(
            "revoke select, update on t4 from user2");
        
        // set connection user2
        
        // expect error
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_user2,
            "select c2 from user1.t4");
        
        // expect error
        
        assertStatementError("42502", st_user2,
            "update user1.t4 set c2='ABC'");
        
        // set connection user2
        // 
        // ---------------------------------------------------------
        // ---------- schemas 
        // ---------------------------------------------------------
        // ----------
        
        
        // expect error
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42508", st_user2,
            "create table myschema.t5 (i int)");
        
        // ok
        
        st_user2.executeUpdate(
            "create table user2.t5 (i int)");
        
        // expect error
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42508", st_user2,
            "CREATE SCHEMA w3 AUTHORIZATION user2");
        
        assertStatementError("42508", st_user2,
            " create table w3.t1 (i int)");
        
        // expect error, already exists
        
        assertStatementError("X0Y68", st_user2,
            "CREATE SCHEMA AUTHORIZATION user2");
        
        // expect error
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42508", st_user2,
            "CREATE SCHEMA myschema");
        
        // expect error
        
        assertStatementError("X0Y68", st_user2,
            "CREATE SCHEMA user2");
        
        // set connection user1
        
        // ok
        
        st.executeUpdate(
            "CREATE SCHEMA w3 AUTHORIZATION user2");
        
        st.executeUpdate(
            " CREATE SCHEMA AUTHORIZATION user6");
        
        st.executeUpdate(
            " CREATE SCHEMA myschema");
        
        // set connection user5
        //ij(USER1)> -- DERBY-1858
        
        
        // expect error
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42507", st_user5,
            "DROP SCHEMA w3 RESTRICT");
        
        // set connection user1
        // -- 
        // ---------------------------------------------------------
        // ---------- views 
        // ---------------------------------------------------------
        // ----------
        
        
        st.executeUpdate(
            "create view user1.sv1 as select * from sys.systables");
        
        // set connection user2
        
        // expect error
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_user2,
            "select tablename from user1.sv1");

        st.executeUpdate(
            " grant select on user1.sv1 to user2");
        
        // set connection user2
        
        // ok
        
        rs = st_user2.executeQuery(
            "select tablename from user1.sv1 order by tablename");
        
        expColNames = new String [] {"TABLENAME"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"SV1"},
            {"SYSALIASES"},
            {"SYSCHECKS"},
            {"SYSCOLPERMS"},
            {"SYSCOLUMNS"},
            {"SYSCONGLOMERATES"},
            {"SYSCONSTRAINTS"},
            {"SYSDEPENDS"},
            {"SYSDUMMY1"},
            {"SYSFILES"},
            {"SYSFOREIGNKEYS"},
            {"SYSKEYS"},
            {"SYSPERMS"},
            {"SYSROLES"},
            {"SYSROUTINEPERMS"},
            {"SYSSCHEMAS"},
            {"SYSSEQUENCES"},
            {"SYSSTATEMENTS"},
            {"SYSSTATISTICS"},
            {"SYSTABLEPERMS"},
            {"SYSTABLES"},
            {"SYSTRIGGERS"},
            {"SYSUSERS"},
            {"SYSVIEWS"},
            {"T1"},
            {"T2"},
            {"T2"},
            {"T3"},
            {"T4"},
            {"T5"},
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection user1
        
        st_user1.executeUpdate(
            " create table ta (i int)");
        
        st_user1.executeUpdate(
            " insert into ta values 1,2,3");
        
        st_user1.executeUpdate(
            " create view sva as select * from ta");
        
        st_user1.executeUpdate(
            " create table tb (j int)");
        
        st_user1.executeUpdate(
            " insert into tb values 2,3,4");
        
        st_user1.executeUpdate(
            " create view svb as select * from tb");
        
        st_user1.executeUpdate(
            " grant select on sva to user2");
        
        // set connection user2
        
        // expect error
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_user2,
            "create view svc (i) as select * from user1.sva "
            + "union select * from user1.svb");
        
        // set connection user1
        
        st_user1.executeUpdate(
            " grant select on svb to user2");
        
        // set connection user2
        
        // ok
        
        st_user2.executeUpdate(
            "create view svc (i) as select * from user1.sva "
            + "union select * from user1.svb");
        
        rs = st_user2.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select * from svc order by I");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"3"},
            {"4"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // -- DERBY-1715, DERBY-1631
        // - set connection user1
        // = create table t01 (i int)
        // - insert into t01 values 1
        // - grant select on t01 to user2
        // - set connection user2
        // - select * from user1.t01
        // - create view v01 as select * from user1.t01
        // - create view v02 as select * from user2.v01
        // - create view v03 as select * from user2.v02
        // - set connection user1
        // - revoke select on t01 from user2
        // - set connection user2
        // - select * from user1.t01
        // - select * from user2.v01
        // - select * from user2.v02
        // - select * from user2.v03
        // - drop view user2.v01
        // - drop view user2.v02
        // - drop view user3.v03 
        // -- grant all privileges then create the view
        
        
        st_user1.executeUpdate(
            " create table t01ap (i int)");
        
        st_user1.executeUpdate(
            " insert into t01ap values 1");
        
        st_user1.executeUpdate(
            " grant all privileges on t01ap to user2");
        
        // set connection user2
        
        // ok
        
        st_user2.executeUpdate(
            "create view v02ap as select * from user1.t01ap");
        
        // ok
        
        rs = st_user2.executeQuery(
            "select * from v02ap");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // expect error, don't have with grant option
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("4250A", st_user2,
            "grant select on user2.v02ap to user3");
        
        // set connection user3
        
        // expect error
        
        assertStatementError("42502", st_user3,
            "create view v03ap as select * from user2.v02ap");
        
        assertStatementError("42Y07", st_user3,
            " select * from v03ap");
        
        // expect error
        
        assertStatementError("42Y07", st_user3,
            "grant all privileges on v03ap to user4");
        
        // set connection user4
        
        // expect error
        
        assertStatementError("42Y07", st_user4,
            "create view v04ap as select * from user3.v03ap");
        
        assertStatementError("42Y07", st_user4,
            " select * from v04ap");
        
        // expect error
        
        assertStatementError("42Y07", st_user4,
            "grant select on v04ap to user2");
        
        // set connection user2
        
        assertStatementError("42Y07", st_user2,
            " select * from user4.v04ap");
        
        // set connection user4
        
        // expect error
        
        assertStatementError("42Y07", st_user4,
            "revoke select on v04ap from user2");
        
        // set connection user2
        
        // expect error
        
        assertStatementError("42Y07", st_user2,
            "select * from user4.v04ap");
        
        // set connection user1
        //ij(USER2)> -- 
        // ---------------------------------------------------------
        // ---------- references and constraints 
        // ---------------------------------------------------------
        // ----------
        
        
        assertStatementError("42Y55", st_user1,
            " drop table user1.rt1");
        
        assertStatementError("42Y55", st_user1,
            " drop table user2.rt2");
        
        st_user1.executeUpdate(
            " create table rt1 (c1 int not null primary key, c2 "
            + "int not null)");
        
        st_user1.executeUpdate(
            " insert into rt1 values (1, 10)");
        
        st_user1.executeUpdate(
            " insert into rt1 values (2, 20)");
        
        // set connection user2
        
        // expect error
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_user2,
            "create table rt2 (c1 int primary key not null, c2 "
            + "int not null, c3 int not null, constraint rt2fk "
            + "foreign key(c1) references user1.rt1)");
        
        // set connection user1
        
        st_user1.executeUpdate(
            " grant references on rt1 to user2");
        
        // set connection user2
        
        // ok
        
        st_user2.executeUpdate(
            "create table rt2 (c1 int primary key not null, c2 "
            + "int not null, c3 int not null, constraint rt2fk "
            + "foreign key(c2) references user1.rt1)");
        
        st_user2.executeUpdate(
            " insert into rt2 values (1,1,1)");
        
        // expect error
        
        assertStatementError("23503", st_user2,
            "insert into rt2 values (3,3,3)");
        
        // set connection user1
        
        st_user1.executeUpdate(
            " revoke references on rt1 from user2");
        
        // set connection user2
        
        // ok, fk constraint got dropped by revoke
        
        st_user2.executeUpdate(
            "insert into rt2 values (3,3,3)");
        
        rs = st_user2.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select * from rt2 order by C1, C2, C3");
        
        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", "1"},
            {"3", "3", "3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // expect errors
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_user2,
            "create table rt3 (c1 int primary key not null, c2 "
            + "int not null, c3 int not null, constraint rt3fk "
            + "foreign key(c1) references user1.rt1)");
        
        // set connection user1
        // test PUBLIC DERBY-1857
        // - set connection user1
        // - drop table user3.rt3
        // - drop table user2.rt2
        // - drop table user1.rt1
        // - create table rt1 (c1 int primary key not null, c2 int not null unique, c3 int not null)
        // - insert into rt1 values (1,1,1)
        // - insert into rt1 values (2,2,2)
        // - insert into rt1 values (3,3,3)
        // - grant references(c2, c1) on rt1 to PUBLIC
        // - set connection user2
        // - create table rt2 (c1 int primary key not null, constraint rt2fk foreign key(c1) references user1.rt1(c2) )
        // - insert into rt2 values (1), (2)
        // - set connection user3
        // - create table rt3 (c1 int primary key not null, constraint rt3fk foreign key(c1) references user1.rt1(c2) )
        // - insert into rt3 values (1), (2)
        // - set connection user1
        // - revoke references(c1) on rt1 from PUBLIC
        // - set connection user2
        // -- expect constraint error
        // -- insert into rt2 values (4)
        // - set connection user3
        // -- expect constraint error
        // -- insert into rt3 values (4)
        
        // test user privilege and PUBLIC
        
        // drop as dbo
        assertStatementError("42Y07", st,
            " drop table user3.rt3");
        
        st.executeUpdate(
            " drop table user2.rt2");
        
        st.executeUpdate(
            " drop table user1.rt1");
        
        st_user1.executeUpdate(
            " create table rt1 (c1 int primary key not null, c2 int)");
        
        st_user1.executeUpdate(
            " insert into rt1 values (1,1), (2,2)");
        
        st_user1.executeUpdate(
            " grant references on rt1 to PUBLIC, user2, user3");
        
        // set connection user2
        
        st_user2.executeUpdate(
            " create table rt2 (c1 int primary key not null, "
            + "constraint rt2fk foreign key(c1) references user1.rt1)");
        
        st_user2.executeUpdate(
            " insert into rt2 values (1), (2)");
        
        // set connection user3
        
        st_user3.executeUpdate(
            " create table rt3 (c1 int primary key not null, "
            + "constraint rt3fk foreign key(c1) references user1.rt1)");
        
        st_user3.executeUpdate(
            " insert into rt3 values (1), (2)");
        
        // set connection user1
        
        // ok, use the privilege granted to user2
        
        st_user1.executeUpdate(
            "revoke references on rt1 from PUBLIC");
        
        // ok, user3 got no privileges, so rt3fk should get dropped.
        
        st_user1.executeUpdate(
            "revoke references on rt1 from user3");
        
        // set connection user2
        
        // expect error, FK enforced.
        
        assertStatementError("23503", st_user2,
            "insert into rt2 values (3)");
        
        // set connection user3
        
        // ok
        
        st_user3.executeUpdate(
            "insert into rt3 values (3)");
        
        // test multiple FKs
//IC see: https://issues.apache.org/jira/browse/DERBY-3376
//IC see: https://issues.apache.org/jira/browse/DERBY-1736
//IC see: https://issues.apache.org/jira/browse/DERBY-1589
        st_user3.executeUpdate("drop table user3.rt3");
        st_user2.executeUpdate("drop table user2.rt2");
        st_user1.executeUpdate("drop table user1.rt1");
        // set connection user1
        st_user1.executeUpdate(
            "create table rt1 (c1 int primary key not null, c2 int)");
        st_user1.executeUpdate("insert into rt1 values (1,1), (2,2)");
        st_user1.executeUpdate(
            "grant references on rt1 to PUBLIC, user2, user3");
        // set connection user2
        // XJ001 occurred at create table rt2..
        st_user2.executeUpdate(
            "create table rt2 (c1 int primary key not null," +
            " constraint rt2fk foreign key(c1) references user1.rt1)");
        st_user2.executeUpdate("insert into rt2 values (1), (2)");
        st_user2.executeUpdate("grant references on rt2 to PUBLIC, user3");
        // set connection user3
        st_user3.executeUpdate(
            "create table rt3 (c1 int primary key not null," +
            " constraint rt3fk1 foreign key(c1) references user1.rt1," +
            " constraint rt3fk2 foreign key(c1) references user2.rt2)");
        st_user3.executeUpdate("insert into rt3 values (1), (2)");
        // set connection user1 
        // rt3fk1 should get dropped.
        st_user1.executeUpdate("revoke references on rt1 from PUBLIC");
        st_user1.executeUpdate("revoke references on rt1 from user3");
        // set connection user2
        st_user2.executeUpdate("revoke references on rt2 from PUBLIC");
        // expect error:
        // ERROR 23503: INSERT on table 'RT2' caused a violation of foreign
        // key constraint 'RT2FK' for key (3).
        assertStatementError("23503", st_user2, "insert into rt2 values (3)");
        // set connection user3
        // expect error, user3 references privilege, rt3fk2 still in effect
        assertStatementError("23503", st_user3, "insert into rt3 values (3)");
        // set connection user2
        st_user2.executeUpdate("revoke references on rt2 from user3");
        // set connection user3
        // ok, rt3fk2 should be dropped.
        st_user3.executeUpdate("insert into rt3 values (3) ");

        // ---------------------------------------------------------
        // ---------- routines and standard builtins 
        // ---------------------------------------------------------
        // ----------
        
        
        st_user1.executeUpdate(
            " CREATE FUNCTION F_ABS2(P1 INT) RETURNS INT NO "
            + "SQL RETURNS NULL ON NULL INPUT EXTERNAL NAME "
            + "'java.lang.Math.abs' LANGUAGE JAVA PARAMETER STYLE JAVA");
        
        // syntax error
        
        assertStatementError("42X01", st_user1,
            "grant execute on F_ABS2 to user2");
        
        // F_ABS2 is not a procedure, expect errors
        
        assertStatementError("42Y03", st_user1,
            "grant execute on procedure F_ABS2 to user2");
        
        // set connection user2
        
        // expect errors
        
        assertStatementError("42504", st_user2,
            "values user1.F_ABS1(10) + user1.F_ABS2(-10)");
        
        // set connection user1
        
        // ok
        
        st_user1.executeUpdate(
            "grant execute on function F_ABS2 to user2");
        
        // set connection user2
        
        // ok
        
        rs = st_user2.executeQuery(
            "values user1.F_ABS1(10) + user1.F_ABS2(-10)");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"20"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // expect errors
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42509", st_user2,
            "revoke execute on function ABS from user2 restrict");
        
        assertStatementError("42X01", st_user2,
            " revoke execute on function AVG from user2 restrict");
        
        assertStatementError("42509", st_user2,
            " revoke execute on function LENGTH from user2 restrict");
        
        // set connection user1
        
        // ok
        
        st_user1.executeUpdate(
            "revoke execute on function F_ABS2 from user2 restrict");
        
        st_user1.executeUpdate(
            " revoke execute on function F_ABS1 from user2 restrict");
        
        // set connection user2
        
        // expect error
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42504", st_user2,
            "values user1.F_ABS1(10) + user1.F_ABS2(-10)");
        
        // set connection user1
        
        // ok
        
        st_user1.executeUpdate(
            "grant execute on function F_ABS1 to PUBLIC");
        
        st_user1.executeUpdate(
            " grant execute on function F_ABS2 to PUBLIC");
        
        // set connection user2
        
        // ok
        
        rs = st_user2.executeQuery(
            "values user1.F_ABS1(10) + user1.F_ABS2(-10)");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"20"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection user1
        //ij(USER2)> -- 
        // ---------------------------------------------------------
        // ---------- system tables 
        // ---------------------------------------------------------
        // ----------
        
        
        // not allowed. expect errors, sanity check
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42509", st_user1,
            "grant ALL PRIVILEGES on sys.sysaliases to user2");
        
        assertStatementError("42509", st_user1,
            " grant ALL PRIVILEGES on sys.syschecks to user2");
        
        assertStatementError("42509", st_user1,
            " grant ALL PRIVILEGES on sys.syscolperms to user2");
        
        assertStatementError("42509", st_user1,
            " grant ALL PRIVILEGES on sys.syscolumns to user2");
        
        assertStatementError("42509", st_user1,
            " grant ALL PRIVILEGES on sys.sysconglomerates to user2");
        
        assertStatementError("42509", st_user1,
            " grant ALL PRIVILEGES on sys.sysconstraints to user2");
        
        assertStatementError("42509", st_user1,
            " grant ALL PRIVILEGES on sys.sysdepends to user2");
        
        assertStatementError("42509", st_user1,
            " grant ALL PRIVILEGES on sys.sysfiles to user2");
        
        assertStatementError("42509", st_user1,
            " grant ALL PRIVILEGES on sys.sysforeignkeys to user2");
        
        assertStatementError("42509", st_user1,
            " grant ALL PRIVILEGES on sys.syskeys to user2");
        
        assertStatementError("42509", st_user1,
            " grant ALL PRIVILEGES on sys.sysroutineperms to user2");
        
        assertStatementError("42509", st_user1,
            " grant ALL PRIVILEGES on sys.sysschemas to user2");
        
        assertStatementError("42509", st_user1,
            " grant ALL PRIVILEGES on sys.sysstatistics to user2");
        
        assertStatementError("42509", st_user1,
            " grant ALL PRIVILEGES on sys.sysstatements to user2");
        
        assertStatementError("42509", st_user1,
            " grant ALL PRIVILEGES on sys.systableperms to user2");
        
        assertStatementError("42509", st_user1,
            " grant ALL PRIVILEGES on sys.systables to user2");
        
        assertStatementError("42509", st_user1,
            " grant ALL PRIVILEGES on sys.systriggers to user2");
        
        assertStatementError("42509", st_user1,
            " grant ALL PRIVILEGES on sys.sysviews to user2");
        
        assertStatementError("42509", st_user1,
            " grant ALL PRIVILEGES on syscs_diag.lock_table to user2");
        
        assertStatementError("42509", st_user1,
            " grant select on sys.sysaliases to user2, public");
        
        assertStatementError("42509", st_user1,
            " grant select on sys.syschecks to user2, public");
        
        assertStatementError("42509", st_user1,
            " grant select on sys.syscolperms to user2, public");
        
        assertStatementError("42509", st_user1,
            " grant select on sys.syscolumns to user2, public");
        
        assertStatementError("42509", st_user1,
            " grant select on sys.sysconglomerates to user2, public");
        
        assertStatementError("42509", st_user1,
            " grant select on sys.sysconstraints to user2, public");
        
        assertStatementError("42509", st_user1,
            " grant select on sys.sysdepends to user2, public");
        
        assertStatementError("42509", st_user1,
            " grant select on sys.sysfiles to user2, public");
        
        assertStatementError("42509", st_user1,
            " grant select on sys.sysforeignkeys to user2, public");
        
        assertStatementError("42509", st_user1,
            " grant select on sys.syskeys to user2, public");
        
        assertStatementError("42509", st_user1,
            " grant select on sys.sysroutineperms to user2, public");
        
        assertStatementError("42509", st_user1,
            " grant select on sys.sysschemas to user2, public");
        
        assertStatementError("42509", st_user1,
            " grant select on sys.sysstatistics to user2, public");
        
        assertStatementError("42509", st_user1,
            " grant select on sys.sysstatements to user2, public");
        
        assertStatementError("42509", st_user1,
            " grant select on sys.systableperms to user2, public");
        
        assertStatementError("42509", st_user1,
            " grant select on sys.systables to user2, public");
        
        assertStatementError("42509", st_user1,
            " grant select on sys.systriggers to user2, public");
        
        assertStatementError("42509", st_user1,
            " grant select on sys.sysviews to user2, public");
        
        assertStatementError("42509", st_user1,
            " grant select on syscs_diag.lock_table to user2, public");
        
        assertStatementError("42509", st_user1,
            " revoke ALL PRIVILEGES on sys.sysaliases from user2");
        
        assertStatementError("42509", st_user1,
            " revoke ALL PRIVILEGES on sys.syschecks from user2");
        
        assertStatementError("42509", st_user1,
            " revoke ALL PRIVILEGES on sys.syscolperms from user2");
        
        assertStatementError("42509", st_user1,
            " revoke ALL PRIVILEGES on sys.syscolumns from user2");
        
        assertStatementError("42509", st_user1,
            " revoke ALL PRIVILEGES on sys.sysconglomerates from user2");
        
        assertStatementError("42509", st_user1,
            " revoke ALL PRIVILEGES on sys.sysconstraints from user2");
        
        assertStatementError("42509", st_user1,
            " revoke ALL PRIVILEGES on sys.sysdepends from user2");
        
        assertStatementError("42509", st_user1,
            " revoke ALL PRIVILEGES on sys.sysfiles from user2");
        
        assertStatementError("42509", st_user1,
            " revoke ALL PRIVILEGES on sys.sysforeignkeys from user2");
        
        assertStatementError("42509", st_user1,
            " revoke ALL PRIVILEGES on sys.syskeys from user2");
        
        assertStatementError("42509", st_user1,
            " revoke ALL PRIVILEGES on sys.sysroutineperms from user2");
        
        assertStatementError("42509", st_user1,
            " revoke ALL PRIVILEGES on sys.sysschemas from user2");
        
        assertStatementError("42509", st_user1,
            " revoke ALL PRIVILEGES on sys.sysstatistics from user2");
        
        assertStatementError("42509", st_user1,
            " revoke ALL PRIVILEGES on sys.sysstatements from user2");
        
        assertStatementError("42509", st_user1,
            " revoke ALL PRIVILEGES on sys.systableperms from user2");
        
        assertStatementError("42509", st_user1,
            " revoke ALL PRIVILEGES on sys.systables from user2");
        
        assertStatementError("42509", st_user1,
            " revoke ALL PRIVILEGES on sys.systriggers from user2");
        
        assertStatementError("42509", st_user1,
            " revoke ALL PRIVILEGES on sys.sysviews from user2");
        
        assertStatementError("42509", st_user1,
            " revoke ALL PRIVILEGES on syscs_diag.lock_table from user2");
        
        assertStatementError("42509", st_user1,
            " revoke select on sys.sysaliases from user2, public");
        
        assertStatementError("42509", st_user1,
            " revoke select on sys.syschecks from user2, public");
        
        assertStatementError("42509", st_user1,
            " revoke select on sys.syscolperms from user2, public");
        
        assertStatementError("42509", st_user1,
            " revoke select on sys.syscolumns from user2, public");
        
        assertStatementError("42509", st_user1,
            " revoke select on sys.sysconglomerates from user2, public");
        
        assertStatementError("42509", st_user1,
            " revoke select on sys.sysconstraints from user2, public");
        
        assertStatementError("42509", st_user1,
            " revoke select on sys.sysdepends from user2, public");
        
        assertStatementError("42509", st_user1,
            " revoke select on sys.sysfiles from user2, public");
        
        assertStatementError("42509", st_user1,
            " revoke select on sys.sysforeignkeys from user2, public");
        
        assertStatementError("42509", st_user1,
            " revoke select on sys.syskeys from user2, public");
        
        assertStatementError("42509", st_user1,
            " revoke select on sys.sysroutineperms from user2, public");
        
        assertStatementError("42509", st_user1,
            " revoke select on sys.sysschemas from user2, public");
        
        assertStatementError("42509", st_user1,
            " revoke select on sys.sysstatistics from user2, public");
        
        assertStatementError("42509", st_user1,
            " revoke select on sys.sysstatements from user2, public");
        
        assertStatementError("42509", st_user1,
            " revoke select on sys.systableperms from user2, public");
        
        assertStatementError("42509", st_user1,
            " revoke select on sys.systables from user2, public");
        
        assertStatementError("42509", st_user1,
            " revoke select on sys.systriggers from user2, public");
        
        assertStatementError("42509", st_user1,
            " revoke select on sys.sysviews from user2, public");
        
        assertStatementError("42509", st_user1,
            " revoke select on syscs_diag.lock_table from user2, public");
        
        // set connection user3
        //ij(USER1)> -- 
        // ---------------------------------------------------------
        // ---------- built-in functions and procedures and 
        // routines 
        // ---------------------------------------------------------
        // ----------
        
        
        // test sqlj, only db owner have privileges by default 
        // expect errors
        
        CallableStatement cSt3 = user3.prepareCall(
            "CALL SQLJ.INSTALL_JAR ('bogus.jar','user2.bogus',0)");
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42504", cSt3);
        
        cSt3 = user3.prepareCall(
            " CALL SQLJ.REPLACE_JAR ('bogus1.jar', 'user2.bogus')");
        assertStatementError("42504", cSt3);
        
        cSt3 = user3.prepareCall(
            " CALL SQLJ.REMOVE_JAR  ('user2.bogus', 0)");
        assertStatementError("42504", cSt3);
        
        // test backup routines, only db owner have privileges by 
        // default expect errors
        
        cSt3 = user3.prepareCall(
            "CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE('backup1')");
        assertStatementError("42504", cSt3);
        
        cSt3 = user3.prepareCall(
            " CALL "
            + "SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCH"
            + "IVE_MODE('backup3', 1)");
        assertStatementError("42504", cSt3);
        
        cSt3 = user3.prepareCall(
            " CALL "
            + "SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCH"
            + "IVE_MODE_NOWAIT('backup4', 1)");
        assertStatementError("42504", cSt3);
        
        // test admin routines, only db owner have privileges by 
        // default
        
        cSt3 = user3.prepareCall(
            "CALL SYSCS_UTIL.SYSCS_FREEZE_DATABASE()");
        assertStatementError("42504", cSt3);
        
        cSt3 = user3.prepareCall(
            " CALL SYSCS_UTIL.SYSCS_UNFREEZE_DATABASE()");
        assertStatementError("42504", cSt3);
        
        cSt3 = user3.prepareCall(
            " CALL SYSCS_UTIL.SYSCS_DISABLE_LOG_ARCHIVE_MODE(1)");
        assertStatementError("42504", cSt3);
        
        cSt3 = user3.prepareCall(
            " CALL SYSCS_UTIL.SYSCS_CHECKPOINT_DATABASE()");
        assertStatementError("42504", cSt3);
        
        // set connection user1
        //ij(USER3)> -- test statistical routines, available for 
        // everyone by default
        
        
        // ok
        
        CallableStatement cSt2 = user2.prepareCall(
            "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        assertUpdateCount(cSt2, 0);
        
        cSt2 = user2.prepareCall(
            " CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1)");
        assertUpdateCount(cSt2, 0);
        
        rs = st_user2.executeQuery(
            " values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"Statement Name:"},
            {"null"},
            {"Statement Text:"},
            {"CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1)"},
            {"Parse Time: 0"},
            {"Bind Time: 0"},
            {"Optimize Tim&"}
        };
        
        JDBC.assertDrainResults(rs, 1);
        
        cSt2 = user2.prepareCall(
            " CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0)");
        assertUpdateCount(cSt2, 0);
        
        cSt2 = user2.prepareCall(
            " CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(0)");
        assertUpdateCount(cSt2, 0);
        
        // set connection user3
        //ij(USER1)> -- ok
        
        
        cSt3 = user3.prepareCall(
            " CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        assertUpdateCount(cSt3, 0);
        
        cSt3 = user3.prepareCall(
            " CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1)");
        assertUpdateCount(cSt3, 0);
        
        rs = st_user3.executeQuery(
            " values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
        
        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"Statement Name:"},
            {"null"},
            {"Statement Text:"},
            {"CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1)"},
            {"Parse Time: 0"},
            {"Bind Time: 0"},
            {"Optimize Tim&"}
        };
        
        JDBC.assertDrainResults(rs, 1);
        
        cSt3 = user3.prepareCall(
            " CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0)");
        assertUpdateCount(cSt3, 0);
        
        cSt3 = user3.prepareCall(
            " CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(0)");
        assertUpdateCount(cSt3, 0);
        
        // test import/export, only db owner have privileges by 
        // default
        
        st_user3.executeUpdate(
            "create table TABLEIMP1 (i int)");
        
        st_user3.executeUpdate(
            " create table TABLEEXP1 (i int)");
        
        st_user3.executeUpdate(
            " insert into TABLEEXP1 values 1,2,3,4,5");
        
        cSt3 = user3.prepareCall(
            " CALL SYSCS_UTIL.SYSCS_EXPORT_TABLE ('USER3', "
            + "'TABLEEXP1', 'myfile.del', null, null, null)");
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42504", cSt3);
        
        cSt3 = user3.prepareCall(
            " CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE ('USER3', "
            + "'TABLEIMP1', 'myfile.del', null, null, null, 0)");
        assertStatementError("42504", cSt3);
        
        cSt3 = user3.prepareCall(
            " CALL SYSCS_UTIL.SYSCS_EXPORT_QUERY('select * from "
            + "user3.TABLEEXP1','myfile.del', null, null, null)");
        assertStatementError("42504", cSt3);
        
        cSt3 = user3.prepareCall(
            " CALL SYSCS_UTIL.SYSCS_IMPORT_DATA ('USER3', "
            + "'TABLEIMP1', null, '1,3,4', 'myfile.del', null, "
            + "null, null,0)");
        assertStatementError("42504", cSt3);
        
        // test property handling routines, only db owner have 
        // privileges by default expect errors
        
        cSt3 = user3.prepareCall(
            "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY "
            + "('derby.locks.deadlockTimeout', '10')");
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42504", cSt3);
        
        assertStatementError("42504", st_user3,
            " VALUES "
            + "SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.locks."
            + "deadlockTimeout')");
        
        // test compress routines, everyone have privilege as long 
        // as the user owns the schema ok
        
        cSt3 = user3.prepareCall(
            "CALL SYSCS_UTIL.SYSCS_COMPRESS_TABLE('USER3', "
            + "'TABLEEXP1', 1)");
        assertUpdateCount(cSt3, 0);
        
        cSt3 = user3.prepareCall(
            " call "
            + "SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE('USER3', "
            + "'TABLEEXP1', 1, 1, 1)");
        assertUpdateCount(cSt3, 0);
        
        // test check table routines, only db owner have privilege 
        // by default
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42504", st_user3,
            "VALUES SYSCS_UTIL.SYSCS_CHECK_TABLE('USER3', 'TABLEEXP1')");
        
        // set connection user1
        //ij(USER3)> -- 
        // ---------------------------------------------------------
        // ---------- synonyms 
        // ---------------------------------------------------------
        // ----------
        
        
        st_user1.executeUpdate(
            " create synonym s1 for user1.t1");
        
        st_user1.executeUpdate(
            " create index ii1 on user1.t1(c2)");
        
        if (usingEmbedded())
        {
            if ((sqlWarn == null) && (st_user1 != null))
                sqlWarn = st_user1.getWarnings();
            if (sqlWarn == null)
                sqlWarn = user1.getWarnings();
            assertNotNull("Expected warning but found none", sqlWarn);
            assertSQLState("01504", sqlWarn);
            sqlWarn = null;
        }
        
        // not supported yet, expect errors
        
        assertStatementError("42X05", st_user1,
            "grant select on s1 to user2");
        
        assertStatementError("42X05", st_user1,
            " grant insert on s1 to user2");
        
        assertStatementError("42X05", st_user1,
            " revoke select on s1 from user2");
        
        assertStatementError("42X05", st_user1,
            " revoke insert on s1 from user2");
        
        // set connection user2
        
        // expect errors
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42507", st_user2,
            "drop synonym user1.s1");
        
        assertStatementError("42X65", st_user2,
            " drop index user1.ii1");
        
        // set connection user1
        //ij(USER2)> -- 
        // ---------------------------------------------------------
        // ---------- transactions and lock table stmt 
        // ---------------------------------------------------------
        // ----------
        
        
        st_user1.executeUpdate(
            " create table t1000 (i int)");
        
        user1.setAutoCommit(false);
        
        st_user1.executeUpdate(
            " grant select on t1000 to user2");
        
        // NOTE: This fails with lock timeout because grants
        //       are always executed with isolation level
        //       repeatable read. For the purposes of this
        //       test, we will not verify the lock timeout
        // set connection user2
        //
        // assertStatementError("40XL1", st_user2,
        //    " select * from user1.t1000");
        
        // set connection user1
        user1.commit();
        
        // set connection user2        
        // ok
        
        rs = st_user2.executeQuery(
            "select * from user1.t1000");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        // set connection user1
        
        st_user1.executeUpdate(
            " revoke select on t1000 from user2");
        
        // set connection user2
        
        //assertStatementError("40XL1", st_user2,
        //    " select * from user1.t1000");
        
        // set connection user1
        user1.commit();
        
        // set connection user2
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_user2,
            " select * from user1.t1000");
        
        user2.setAutoCommit(false);
        
        // should fail
        
        assertStatementError("42500", st_user2,
            "lock table user1.t1000 in share mode");
        
        // should fail
        
        assertStatementError("42500", st_user2,
            "lock table user1.t1000 in exclusive mode");
        
        user2.commit();
        // set connection user1
        user2.setAutoCommit(true);
        
        
        st_user1.executeUpdate(
            " grant select on t1000 to user2");
        
        // set connection user2
        user1.rollback();
        
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42502", st_user2,
            " select * from user1.t1000");
        
        // set connection user1
        
        st_user1.executeUpdate(
            " grant select on t1000 to user2");
        
        user1.commit();
        st_user1.executeUpdate(
            " revoke select on t1000 from user2");
        
        // set connection user2
        user1.rollback();
        
        
        rs = st_user2.executeQuery(
            " select * from user1.t1000");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        // set connection user1
        
        user1.setAutoCommit(true);
        
        st_user1.executeUpdate(
            " drop table t1000");
        
        // set connection user1
        
        st_user1.executeUpdate(
            " create table t1000 (c varchar(1))");
        
        st_user1.executeUpdate(
            " insert into t1000 values 'a', 'b', 'c'");
        
        st_user1.executeUpdate(
            " grant select on t1000 to user3");
        
        // set connection user2
        
        st_user2.executeUpdate(
            " create table t1001 (i int)");
        
        st_user2.executeUpdate(
            " insert into t1001 values 1");
        
        // execute these next few as dbo, select not granted
        // to other users        
        rs = st.executeQuery(
            " select * from user2.t1001");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st.executeUpdate(
            " insert into user2.t1001 values 2");
        
        assertUpdateCount(st, 2,
            " update user2.t1001 set i = 888");
        
        st.executeUpdate(
            " drop table user1.t1000");
        
        st.executeUpdate(
            " drop table user2.t1001");
        
        user1.commit();
        user1.setAutoCommit(true);
        
        // set connection user1
        // 
        // ---------------------------------------------------------
        // ---------- cursors 
        // ---------------------------------------------------------
        // --
        // -- DERBY-1716
        // - set connection user1
        // - drop table t1001
        // - create table t1001 (c varchar(1))
        // - insert into t1001 values 'a', 'b', 'c'
        // - grant select on t1001 to user3
        // - set connection user3
        // - autocommit off
        // - GET CURSOR crs1 AS 'select * from user1.t1001'
        // - next crs1
        // - set connection user1
        // -- revoke select privilege while user3 still have an open cursor
        // - revoke select on t1001 from user3
        // - set connection user3
        // - next crs1
        // - next crs1
        // - close crs1
        // - autocommit on 
        // ---------------------------------------------------------
        // ---------- rename table 
        // ---------------------------------------------------------
        // ----------
        
        
        assertStatementError("42Y55", st_user1,
            " drop table user1.rta");
        
        assertStatementError("42Y55", st_user1,
            " drop table user2.rtb");
        
        st_user1.executeUpdate(
            " create table rta (i int)");
        
        st_user1.executeUpdate(
            " grant select on rta to user2");
        
        // set connection user2
        
        rs = st_user2.executeQuery(
            " select * from user1.rta");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        // set connection user1
        
        st_user1.executeUpdate(
            " rename table rta to rtb");
        
        // set connection user1
        
        // expect error
        
        assertStatementError("42X05", st_user1,
            "select * from user1.rta");
        
        // ok
        
        rs = st_user1.executeQuery(
            "select * from user1.rtb");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        // set connection user2
        
        // expect error
        
        assertStatementError("42X05", st_user2,
            "select * from user1.rta");
        
        // ok
        
        rs = st_user2.executeQuery(
            "select * from user1.rtb");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        // set connection user2
        // 
        // ---------------------------------------------------------
        //  DB owner power =) 
        // ---------------------------------------------------------
        
        // NOTE: DBO is now TEST_DBO, not user1 as in previous test.        
        
        st_user2.executeUpdate(
            " create table ttt1 (i int)");
        
        st_user2.executeUpdate(
            " insert into ttt1 values 1");
        
        // set connection user3
        
        st_user3.executeUpdate(
            " create table ttt1 (i int)");
        
        st_user3.executeUpdate(
            " insert into ttt1 values 10");
        
        // set connection dbo
        
        // the following actions are ok
        
        rs = st.executeQuery(
            "select * from user2.ttt1");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st.executeUpdate(
            " insert into user2.ttt1 values 2");
        
        assertUpdateCount(st, 2,
            " update user2.ttt1 set i = 888");
        
        assertUpdateCount(st, 2,
            " delete from user2.ttt1");
        
        st.executeUpdate(
            " drop table user2.ttt1");
        
        rs = st.executeQuery(
            " select * from user3.ttt1");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"10"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st.executeUpdate(
            " insert into user3.ttt1 values 20");
        
        assertUpdateCount(st, 2,
            " update user3.ttt1 set i = 999");
        
        assertUpdateCount(st, 2,
            " delete from user3.ttt1");
        
        st.executeUpdate(
            " drop table user3.ttt1");
        
        // set connection user4
        
        st_user4.executeUpdate(
            " create table ttt1 (i int)");
        
        // set connection dbo
        
        st.executeUpdate(
            " drop table user4.ttt1");
        
        // set connection user2
        
        // DERBY-1858 expect error
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42507", st_user2,
            "drop schema user4 restrict");
        
        // set connection dbo
        
        // ok
        
        st.executeUpdate(
            "drop schema user4 restrict");
        
        // end user1 <--> dbo
        
        // ---------------------------------------------------------
        // ---------- Statement preparation 
        // ---------------------------------------------------------
        // ----------
        
        
        st_user1.executeUpdate(
            " create table ttt2 (i int)");
        
        st_user1.executeUpdate(
            " insert into ttt2 values 8");
        
        // set connection user2
        
        // prepare statement, ok
        
        PreparedStatement pSt2 = user2.prepareStatement(
            "select * from user1.ttt2");
        
        // expect error
        
        assertStatementError("42502", pSt2);
        
        // set connection user1
        
        
        st_user1.executeUpdate(
            " grant select on ttt2 to user2");
        
        // set connection user2
        
        // prepare statement, ok
        
        pSt2 = user2.prepareStatement(
            "select * from user1.ttt2");
        
        // ok
        
        rs = pSt2.executeQuery();
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"8"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection user1
        
        st_user1.executeUpdate(
            " revoke select on ttt2 from user2");
        
        // set connection user2
        
        // expect error
        
        assertStatementError("42502", pSt2);

        // -----------------------------------
        // Now try with column level privilege
        // DERBY-3736
//IC see: https://issues.apache.org/jira/browse/DERBY-3736
        st_user1.executeUpdate(
            " grant select(i) on ttt2 to user2");

        // set connection user2
        // prepare statement, ok

        pSt2 = user2.prepareStatement(
            "select * from user1.ttt2");

        // ok

        rs = pSt2.executeQuery();
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"8"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        // set connection user1

        st_user1.executeUpdate(
            " revoke select(i) on ttt2 from user2");

        // set connection user2
        // expect error

        assertStatementError("42502", pSt2);
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633

        // end of test case for DERBY-3736
        // --------------------------------

        // set connection user2
        // 
        // ---------------------------------------------------------
        // ---------- Misc 
        // ---------------------------------------------------------
        // ----------
        
        
        st_user2.executeUpdate(
            " create table tshared0 (i int)");
        
        // set connection user1
        //ij(USER2)> -- db owner tries to revoke select access 
        // from user2
        
        
        // expect error
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("42509", st_user1,
            "revoke select on user2.tshared0 from user2");
        
        // set connection user2
        
        rs = st_user2.executeQuery(
            " select * from user2.tshared0");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        // set connection user2
        
        st_user2.executeUpdate(
            " create table tshared1 (i int)");
        
        st_user2.executeUpdate(
            " grant select, insert, delete, update on tshared1 "
            + "to user3, user4, user5");
        
        // set connection user3
        
        st_user3.executeUpdate(
            " create table tshared1 (i int)");
        
        st_user3.executeUpdate(
            " grant select, insert, delete, update on tshared1 "
            + "to user2, user4, user5");
        
        // set connection user2
        
        st_user2.executeUpdate(
            " insert into user3.tshared1 values 1,2,3");
        
        assertUpdateCount(st_user2, 3,
            " update user3.tshared1 set i = 888");
        
        rs = st_user2.executeQuery(
            " select * from user3.tshared1");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"888"},
            {"888"},
            {"888"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertUpdateCount(st_user2, 3,
            " delete from user3.tshared1");
        
        st_user2.executeUpdate(
            " insert into user3.tshared1 values 1,2,3");
        
        // set connection user3
        
        st_user3.executeUpdate(
            " insert into user2.tshared1 values 3,2,1");
        
        assertUpdateCount(st_user3, 3,
            " update user2.tshared1 set i = 999");
        
        rs = st_user3.executeQuery(
            " select * from user2.tshared1");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"999"},
            {"999"},
            {"999"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        assertUpdateCount(st_user3, 3,
            " delete from user2.tshared1");
        
        st_user3.executeUpdate(
            " insert into user2.tshared1 values 3,2,1");
        
        // set connection dbo
        
        assertUpdateCount(st, 3,
            " update user2.tshared1 set i = 1000");
        
        assertUpdateCount(st, 3,
            " update user3.tshared1 set i = 1001");
        
        assertUpdateCount(st, 3,
            " delete from user2.tshared1");
        
        assertUpdateCount(st, 3,
            " delete from user3.tshared1");
        
        st.executeUpdate(
            " insert into user2.tshared1 values 0,1,2,3");
        
        st.executeUpdate(
            " insert into user3.tshared1 values 4,3,2,1");
        
        // set connection user4
        
        rs = st_user4.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select * from user2.tshared1 order by I");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0"},
            {"1"},
            {"2"},
            {"3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_user4.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select * from user3.tshared1 order by I");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"3"},
            {"4"},
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        st_user4.executeUpdate(
            " create view vshared1 as select * from "
            + "user2.tshared1 union select * from user3.tshared1");
        
        st_user4.executeUpdate(
            " create view vshared2 as select * from "
            + "user2.tshared1 intersect select * from user3.tshared1");
        
        st_user4.executeUpdate(
            " create view vshared3 as select * from "
            + "user2.tshared1 except select * from user3.tshared1");
        
        st_user4.executeUpdate(
            " create view vshared4(i) as select * from "
            + "user3.tshared1 union values 0");
        
        st_user4.executeUpdate(
            " insert into user2.tshared1 select * from user3.tshared1");
        
        rs = st_user4.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select * from vshared1 order by I");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0"},
            {"1"},
            {"2"},
            {"3"},
            {"4"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_user4.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select * from vshared2 order by I");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"3"},
            {"4"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_user4.executeQuery(
            " select * from vshared3");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_user4.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select * from vshared4 order by I");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"0"},
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            {"1"},
            {"2"},
            {"3"},
            {"4"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // expect errors
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1828
//IC see: https://issues.apache.org/jira/browse/DERBY-2633
        assertStatementError("4250A", st_user4,
            "grant select on vshared1 to user5");
        
        assertStatementError("4250A", st_user4,
            " grant select on vshared2 to user5");
        
        assertStatementError("4250A", st_user4,
            " grant select on vshared3 to user5");
        
        assertStatementError("4250A", st_user4,
            " grant select on vshared4 to user5");
        
        // set connection user5
        
        assertStatementError("42502", st_user5,
            " select * from user4.vshared1");
        
        assertStatementError("42502", st_user5,
            " select * from user4.vshared2");
        
        assertStatementError("42502", st_user5,
            " select * from user4.vshared3");
        
        assertStatementError("42502", st_user5,
            " select * from user4.vshared4");
        
        // set connection user1
        
        // set connection user1
        // 
        // ---------------------------------------------------------
        // ---------- triggers 
        // ---------------------------------------------------------
        // ----------
        
        
        // expect error
        
        assertStatementError("42X01", st_user1,
            "create trigger tt0a after insert on t1 for each "
            + "statement grant select on t1 to user2");
        
        // expect error
        
        assertStatementError("42X01", st_user1,
            "create trigger tt0b after insert on t1 for each "
            + "statement revoke select on t1 from user2");
        
        // same schema in trigger action
        
        assertStatementError("42Y55", st_user1,
            "drop table t6");
        
        st_user1.executeUpdate(
            " create table t6 (c1 int not null primary key, c2 int)");
        
        st_user1.executeUpdate(
            " grant trigger on t6 to user2");
        
        // set connection user2
        
        assertStatementError("42Y55", st_user2,
            " drop table t7");
        
        st_user2.executeUpdate(
            " create table t7 (c1 int, c2 int, c3 int)");
        
        st_user2.executeUpdate(
            " insert into t7 values (1,1,1)");
        
        st_user2.executeUpdate(
            " create trigger tt1 after insert on user1.t6 for "
            + "each statement update user2.t7 set c2 = 888");
        
        st_user2.executeUpdate(
            " create trigger tt2 after insert on user1.t6 for "
            + "each statement insert into user2.t7 values (2,2,2)");
        
        // set connection user1
        
        st_user1.executeUpdate(
            " insert into t6 values (1, 10)");
        
        rs = st.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select * from user2.t7 order by C1");
        
        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "888", "1"},
            {"2", "2", "2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection user1
        // different schema in trigger action this testcase is 
        // causing NPE - DERBY-1583
        
        
        assertStatementError("42Y55", st_user1,
            " drop table t8");
        
        assertStatementError("42Y55", st_user1,
            " drop table t9");
        
        st_user1.executeUpdate(
            " create table t8 (c1 int not null primary key, c2 int)");
        
        st_user1.executeUpdate(
            " create table t9 (c1 int, c2 int, c3 int)");
        
        st_user1.executeUpdate(
            " insert into user1.t8 values (1,1)");
        
        st_user1.executeUpdate(
            " insert into user1.t9 values (10,10,10)");
        
        st_user1.executeUpdate(
            " grant trigger on t8 to user2");
        
        st_user1.executeUpdate(
            " grant update(c2, c1), insert on t9 to user2");
        
        // set connection user2
        
        st_user2.executeUpdate(
            " create trigger tt3 after insert on user1.t8 for "
            + "each statement update user1.t9 set c2 = 888");
        
        st_user2.executeUpdate(
            " create trigger tt4 after insert on user1.t8 for "
            + "each statement insert into user1.t9 values (2,2,2)");
        
        // set connection user1
        
        // expect error
        
        assertStatementError("23505", st_user1,
            "insert into user1.t8 values (1, 10)");
        
        // ok
        
        st_user1.executeUpdate(
            "insert into user1.t8 values (2, 20)");
        
        rs = st_user1.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select * from user1.t9 order by C1");
        
        expColNames = new String [] {"C1", "C2", "C3"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"2", "2", "2"},
            {"10", "888", "10"},
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // grant all privileges then create trigger, then revoke 
        // the trigger privilege
        
        assertStatementError("42Y55", st_user1,
            "drop table t10");
        
        assertStatementError("42Y55", st_user1,
            " drop table t11");
        
        st_user1.executeUpdate(
            " create table t10 (i int, j int)");
        
        st_user1.executeUpdate(
            " insert into t10 values (1,1), (2,2)");
        
        st_user1.executeUpdate(
            " create table t11 (i int)");
        
        st_user1.executeUpdate(
            " grant all privileges on t10 to user2");
        
        st_user1.executeUpdate(
            " grant all privileges on t11 to user2");
        
        // set connection user2
        
        // ok
        
        st_user2.executeUpdate(
            "create trigger tt5 after update on user1.t10 for "
            + "each statement insert into user1.t11 values 1");
        
        st_user2.executeUpdate(
            " create trigger tt6 after update of i on user1.t10 "
            + "for each statement insert into user1.t11 values 2");
        
        st_user2.executeUpdate(
            " create trigger tt7 after update of j on user1.t10 "
            + "for each statement insert into user1.t11 values 3");
        
        assertUpdateCount(st_user2, 2,
            " update user1.t10 set i=10");
        
        rs = st_user2.executeQuery(
            " select * from user1.t10 order by I, J");
        
        expColNames = new String [] {"I", "J"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"10", "1"},
            {"10", "2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_user2.executeQuery(
            " select * from user1.t11 order by I");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection user1
        
        // triggers get dropped
        
        st_user1.executeUpdate(
            "revoke trigger on t10 from user2");
        
        // set connection user2
        
        assertUpdateCount(st_user2, 2,
            " update user1.t10 set i=20");
        
        rs = st_user2.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select * from user1.t10 order by I, J");
        
        expColNames = new String [] {"I", "J"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"20", "1"},
            {"20", "2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_user2.executeQuery(
            " select * from user1.t11 order by I");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // set connection user1
        
        st_user1.executeUpdate(
            " grant trigger on t10 to user2");
        
        // set connection user2
        
        st_user2.executeUpdate(
            " create trigger tt8 after update of j on user1.t10 "
            + "for each statement delete from user1.t11");
        
        assertUpdateCount(st_user2, 2,
            " update user1.t10 set j=100");
        
        rs = st_user2.executeQuery(
            " select * from user1.t10");
        
        expColNames = new String [] {"I", "J"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"20", "100"},
            {"20", "100"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_user2.executeQuery(
            " select * from user1.t11");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        assertUpdateCount(st_user2, 2,
            " delete from user1.t10");
        
        assertUpdateCount(st_user2, 0,
            " delete from user1.t11");
        
        // set connection user1
        //ij(USER2)> -- test trigger, view and function combo
        
        
        st_user1.executeUpdate(
            " drop function F_ABS1");
        
        st_user1.executeUpdate(
            " CREATE FUNCTION F_ABS1(P1 INT) RETURNS INT NO "
            + "SQL RETURNS NULL ON NULL INPUT EXTERNAL NAME "
            + "'java.lang.Math.abs' LANGUAGE JAVA PARAMETER STYLE JAVA");
        
        st_user1.executeUpdate(
            " grant execute on function F_ABS1 to user5");
        
        st_user1.executeUpdate(
            " grant trigger,insert,update,delete,select on t10 to user5");
        
        st_user1.executeUpdate(
            " grant trigger,insert,update,delete,select on t11 to user5");
        
        assertStatementError("X0X05", st_user1,
            " drop view v");
        
        st_user1.executeUpdate(
            " create view v(i) as values 888");
        
        st_user1.executeUpdate(
            " grant select on v to user5");
        
        // set connection user5
        
        st_user5.executeUpdate(
            " create trigger tt9 after insert on user1.t10 for "
            + "each statement insert into user1.t11 values "
            + "(user1.F_ABS1(-5))");
        
        st_user5.executeUpdate(
            " create trigger tt10 after insert on user1.t10 for "
            + "each statement insert into user1.t11 select * from user1.v");
        
        st_user5.executeUpdate(
            " insert into user1.t10 values (1,1)");
        
        rs = st_user5.executeQuery(
            " select * from user1.t10");
        
        expColNames = new String [] {"I", "J"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st_user5.executeQuery(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
            " select * from user1.t11 order by I");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"5"},
            {"888"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // -- Related to DERBY-1631 cannot revoke 
        // execution on F_ABS1 due to X0Y25 (object 
        // dependencies)
        //
        // - revoke execute on function F_ABS1 from user5 restrict
        // - set connection user5
        // - insert into user1.t10 values (2,2)
        // - select * from user1.t10
        // - select * from user1.t11
        // - set connection user1
        // - revoke select on v from user5
        // - set connection user5
        // - insert into user1.t10 values (3,3)
        // - select * from user1.t10
        // - select * from user1.t11
        // - set connection user1
        // - drop view v
        
        // drop everything left over
        st.executeUpdate("DROP PROCEDURE USER1.GRANT_SELECT_PROC1");
        st.executeUpdate("DROP PROCEDURE USER1.GRANT_SELECT_PROC2");
        st.executeUpdate("DROP PROCEDURE USER1.GRANT_SELECT_PROC3");
        st.executeUpdate("DROP PROCEDURE USER1.GRANT_SELECT_PROC4");
        st.executeUpdate("DROP PROCEDURE USER1.REVOKE_SELECT_PROC1");
        st.executeUpdate("DROP PROCEDURE USER1.REVOKE_SELECT_PROC2");
        st.executeUpdate("DROP PROCEDURE USER1.REVOKE_SELECT_PROC3");
        st.executeUpdate("DROP PROCEDURE USER1.REVOKE_SELECT_PROC4");
        st.executeUpdate("DROP TRIGGER USER2.TT1");
        st.executeUpdate("DROP TRIGGER USER2.TT2");
        st.executeUpdate("DROP TRIGGER USER2.TT3");
        st.executeUpdate("DROP TRIGGER USER2.TT4");
        st.executeUpdate("DROP TRIGGER USER2.TT8");
        st.executeUpdate("DROP TRIGGER USER5.TT9");
        st.executeUpdate("DROP TRIGGER USER5.TT10");
        st.executeUpdate("DROP FUNCTION USER1.F_ABS1");
        st.executeUpdate("DROP FUNCTION USER1.F_ABS2");
        st.executeUpdate("DROP VIEW USER4.VSHARED4");
        st.executeUpdate("DROP VIEW USER4.VSHARED3");
        st.executeUpdate("DROP VIEW USER4.VSHARED2");
        st.executeUpdate("DROP VIEW USER4.VSHARED1");
        st.executeUpdate("DROP VIEW USER2.V02AP");
        st.executeUpdate("DROP VIEW USER2.SVC");
        st.executeUpdate("DROP VIEW USER1.SVB");
        st.executeUpdate("DROP VIEW USER1.SVA");
        st.executeUpdate("DROP VIEW USER1.SV1");
        st.executeUpdate("DROP VIEW USER1.V");
        st.executeUpdate("DROP SYNONYM USER1.S1");
        st.executeUpdate("DROP TABLE USER3.RT3");
        st.executeUpdate("DROP TABLE USER3.TSHARED1");
        st.executeUpdate("DROP TABLE USER3.TABLEEXP1");
        st.executeUpdate("DROP TABLE USER3.TABLEIMP1");
        st.executeUpdate("DROP TABLE USER2.T5");
        st.executeUpdate("DROP TABLE USER2.T2");
        st.executeUpdate("DROP TABLE USER2.TSHARED0");
        st.executeUpdate("DROP TABLE USER2.T7");
        st.executeUpdate("DROP TABLE USER2.TSHARED1");
        st.executeUpdate("DROP TABLE USER2.RT2");
        st.executeUpdate("DROP TABLE USER1.T4");
        st.executeUpdate("DROP TABLE USER1.RTB");
        st.executeUpdate("DROP TABLE USER1.TA ");
        st.executeUpdate("DROP TABLE USER1.T10");
        st.executeUpdate("DROP TABLE USER1.T2");
        st.executeUpdate("DROP TABLE USER1.TB");
        st.executeUpdate("DROP TABLE USER1.T6");
        st.executeUpdate("DROP TABLE USER1.T8");
        st.executeUpdate("DROP TABLE USER1.TTT2");
        st.executeUpdate("DROP TABLE USER1.T11");
        st.executeUpdate("DROP TABLE USER1.T9");
        st.executeUpdate("DROP TABLE USER1.T01AP");
        st.executeUpdate("DROP TABLE USER1.T1 ");
        st.executeUpdate("DROP TABLE USER1.T3");
        st.executeUpdate("DROP TABLE USER1.RT1");
        st.executeUpdate("DROP SCHEMA USER1 RESTRICT");
        st.executeUpdate("DROP SCHEMA USER2 RESTRICT");
        st.executeUpdate("DROP SCHEMA USER3 RESTRICT");
        st.executeUpdate("DROP SCHEMA USER4 RESTRICT");
        st.executeUpdate("DROP SCHEMA USER5 RESTRICT");
        st.executeUpdate("DROP SCHEMA USER6 RESTRICT");
        st.executeUpdate("DROP SCHEMA MYSCHEMA RESTRICT");
        st.executeUpdate("DROP SCHEMA W3 RESTRICT");

        // close Statements
        st_user5.close();
        st_user4.close();
        st_user3.close();
        st_user2.close();
        st_user1.close();
        st.close();
        
        // and connections
        user5.close();
        user4.close();
        user3.close();
        user2.close();
        user1.close();
    }

    /**
     * Test the situation where a REVOKE leads to the dropping of
     * a foreign key's backing conglomerate when that conglomerate
     * is shared by other indexes/constraints.  If that happens
     * then a new backing conglomerate must be created (or at least,
     * the old one should be updated accordingly).  Note: Such
     * dropping of a foreign key's shared conglomerate is not
     * actually possible at the moment, but this test exercises the
     * logic that checks for such a situation and ensures that it
     * works correctly (i.e. that it does not attempt to create a
     * a new/updated conglomerate).
     *
     * If DERBY-2204 and/or DERBY-3300 is implemented, then this
     * fixture can be modified to actually test the drop and re-
     * create of a new backing conglomerate as the result of a
     * REVOKE--but for now that's not (shoudn't be) possible.
     */
    public void testRevokeDropsFKWithSharedConglom() throws SQLException
    {
        Connection mamta1 = openUserConnection("mamta1");
        Statement st_mamta1 = mamta1.createStatement();

        st_mamta1.execute(
            "create table pkt1 (i int not null, j int not null)");
        st_mamta1.execute(
            "alter table pkt1 add constraint pkOne primary key (i, j)");
        st_mamta1.execute("insert into pkt1 values (1, 2), (3, 4)");
        st_mamta1.execute("grant references on pkt1 to mamta2");

        st_mamta1.execute(
            "create table pkt2 (i int not null, j int not null)");
        st_mamta1.execute(
            "alter table pkt2 add constraint pkTwo primary key (i, j)");
        st_mamta1.execute("insert into pkt2 values (1, 2), (2, 3)");
        st_mamta1.execute("grant references on pkt2 to mamta2");

        // set connection mamta2

        Connection mamta2 = openUserConnection("mamta2");
        Statement st_mamta2 = mamta2.createStatement();

        st_mamta2.execute("create table fkt2 (i int, j int)");

        st_mamta2.execute("alter table fkt2 add constraint" +
            " fkOne foreign key (i, j) references mamta1.pkt1");

        st_mamta2.execute("alter table fkt2 add constraint" +
            " fkDup foreign key (i, j) references mamta1.pkt2");

        /* This should be fine because both foreign key constraints
         * are satisfied.
         */
        st_mamta2.execute("insert into fkt2 values(1, 2)");

        // This should fail because fkOne is violated.
        assertStatementError(
            "23503", st_mamta2, "insert into fkt2 values (2, 3)");

        // This should fail because fkDup is violated.
        assertStatementError(
            "23503", st_mamta2, "insert into fkt2 values (3, 4)");

        /* Now revoke the REFERENCES privilege on PKT1 from mamta2.
         * This will cause fkOne to be dropped.  Since fkDup
         * shares a conglomerate with fkOne, when we drop fkOne
         * we should _not_ drop its backing physical conglomerate
         * because fkDup still needs it.
         */

        st_mamta1.execute("revoke references on pkt1 from mamta2");

        // This one should pass because fkOne has been dropped.
        st_mamta2.execute("insert into fkt2 values (2, 3)");

        /* This one should still fail because fkDup is still
         * around and the row (3, 3) violates it.
         */
        assertStatementError(
            "23503", st_mamta2, "insert into fkt2 values (3, 4)");

        /* Sanity check that a query which uses the conglomerate
         * backing fkDup will still execute properly.
         */
        JDBC.assertUnorderedResultSet(st_mamta2.executeQuery(
            "select * from fkt2 --DERBY-PROPERTIES constraint=FKDUP"),
            new String [][] {{"1", "2"}, {"2", "3"}});

        st_mamta2.execute("drop table fkt2");
//IC see: https://issues.apache.org/jira/browse/DERBY-5102
        st_mamta2.execute("drop schema mamta2 restrict");
        st_mamta1.execute("drop table pkt2");
        st_mamta1.execute("drop table pkt1");
        st_mamta1.execute("drop schema mamta1 restrict");
        st_mamta2.close();
        st_mamta1.close();
        mamta2.close();
        mamta1.close();
    }


    /**
     * DERBY-4191
     * Make sure that we check for atleast table level select privilege or
     * any column level select privilege for following kind of queries
     * select count(*) from t1
     * select count(1) from t1
     * select 1 from t1
     * select t1.c1 from t1, t2
     */
    public void testMinimumSelectPrivilegeRequirement() throws SQLException {
        Connection user1 = openUserConnection("user1");
        Statement user1St = user1.createStatement();

        Connection user2 = openUserConnection("user2");
        Statement user2St = user2.createStatement();

        ResultSet rs = null;

        //user1 creates table t4191 and t4191_table2
        user1St.executeUpdate("create table t4191(x int, y int)");
        user1St.executeUpdate("create table t4191_table2(z int)");
        user1St.executeUpdate("create table t4191_table3(c31 int, c32 int)");
        user1St.executeUpdate("create view view_t4191_table3(v31, v32) " +
        		"as select c31, c32 from t4191_table3");

        user1St.execute("grant update on t4191_table3 to public");
        user1St.execute("grant insert on t4191_table3 to public");
        user1St.execute("grant delete on t4191_table3 to public");
        //none of following DMLs will work because there is no select
        //privilege available on the view to user2.
        assertStatementError("42502", user2St, "update user1.t4191_table3 "+
        		"set c31 = ( select max(v31) from user1.view_t4191_table3 )");
        assertStatementError("42502", user2St, "update user1.t4191_table3 "+
        		"set c31 = ( select count(*) from user1.view_t4191_table3 )");
        assertStatementError("42502", user2St, "update user1.t4191_table3 "+
        		"set c31 = ( select 1 from user1.view_t4191_table3 )");
        //Following should succeed
        user2St.execute("delete from user1.t4191_table3");

        //Grant select privilege on view so the above DMLs will start working
        user1St.execute("grant select on view_t4191_table3 to public");
        user2St.execute("update user1.t4191_table3 "+
        		"set c31 = ( select max(v31) from user1.view_t4191_table3 )");
        user2St.execute("update user1.t4191_table3 "+
        		"set c31 = ( select count(*) from user1.view_t4191_table3 )");
        user2St.execute("update user1.t4191_table3 "+
        		"set c31 = ( select 1 from user1.view_t4191_table3 )");

        // None of following selects will work because there is no select
        // privilege available to user2 yet. Each row in the array contains
        // a statement and the expected results or update count if the
        // minimum select privilege had been granted.
        Object[][] requireMinimumSelectPrivilege = {
            { "select count(*) from user1.t4191",          new String[][] {{"0"}} },
            { "select count(1) from user1.t4191",          new String[][] {{"0"}} },
            { "select 1 from user1.t4191",                 new String[0][] },
            { "select 1 from user1.t4191 for update",      new String[0][] },
            { "select 1 from user1.t4191 union values 2",  new String[][] {{"2"}} },
            { "values 1 union select 1 from user1.t4191",  new String[][] {{"1"}} },
            { "values (select count(*) from user1.t4191)", new String[][] {{"0"}} },
            { "values (select count(1) from user1.t4191)", new String[][] {{"0"}} },
            { "values ((select 1 from user1.t4191))",      new String[][] {{null}} },
            { "values exists(select 1 from user1.t4191)",  new String[][] {{"false"}} },
            { "values exists(select * from user1.t4191)",  new String[][] {{"false"}} },
            { "select count(*) from (select 1 from user1.t4191) s", new String[][] {{"0"}} },
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
            { "insert into user1.t4191_table3 select 1, 2 from user1.t4191", 0 },
            { "update user1.t4191_table3 set c31 = 1 where exists (select * from user1.t4191)", 0 },
            { "delete from user1.t4191_table3 where exists (select * from user1.t4191)", 0 },
        };

        for (int i = 0; i < requireMinimumSelectPrivilege.length; i++) {
            String sql = (String) requireMinimumSelectPrivilege[i][0];
            assertStatementError("42500", user2St, sql);
        }

        // Should fail because there is no select privilege on column Y.
        assertStatementError("42502", user2St, "select count(y) from user1.t4191");
        //update below should fail because user2 does not have update 
        //privileges on user1.t4191
        assertStatementError("42502", user2St, "update user1.t4191 set x=0");
        //update with subquery should fail too
        assertStatementError("42502", user2St, "update user1.t4191 set x=" +
		" ( select max(x) + 2 from user1.t4191 )");
        assertStatementError("42502", user2St, "update user1.t4191 set x=" +
		" ( select z from user1.t4191_table2 )");

        // Grant select on user1.t4191(x) to user2 and now the above
        // statements, which previously failed because they didn't have
        // the minimum select privilege on the table, will work.
        user1St.execute("grant select(x) on t4191 to user2");

//IC see: https://issues.apache.org/jira/browse/DERBY-6411
        for (int i = 0; i < requireMinimumSelectPrivilege.length; i++) {
            String sql = (String) requireMinimumSelectPrivilege[i][0];
            Object expectedResult = requireMinimumSelectPrivilege[i][1];
            if (expectedResult instanceof Integer) {
                assertUpdateCount(
                    user2St, ((Integer) expectedResult).intValue(), sql);
            } else {
                JDBC.assertFullResultSet(user2St.executeQuery(sql),
                                         (String[][]) expectedResult);
            }
        }

        //user2 does not have select privilege on 2nd column from user1.t4191
        assertStatementError("42502", user2St, "select count(y) from user1.t4191");
        //user2 does not have any select privilege on user1.table t4191_table2
        assertStatementError("42500", user2St, "select x from user1.t4191_table2, user1.t4191");
        
        //grant select privilege on a column in user1.table t4191_table2 to user2
        user1St.execute("grant select(z) on t4191_table2 to user2");
        //now the following should run fine without any privilege issues
        rs = user2St.executeQuery("select x from user1.t4191_table2, user1.t4191");
        JDBC.assertEmpty(rs);
        
        //revoke some column level privileges from user2
        user1St.execute("revoke select(x) on t4191 from user2");
        user1St.execute("revoke select(z) on t4191_table2 from user2");
        //update below should fail because user2 does not have update 
        //privileges on user1.t4191
        assertStatementError("42502", user2St, "update user1.t4191 set x=0");
        //update with subquery should fail too
        assertStatementError("42502", user2St, "update user1.t4191 set x=" +
		" ( select max(x) + 2 from user1.t4191 )");
        assertStatementError("42502", user2St, "update user1.t4191 set x=" +
		" ( select z from user1.t4191_table2 )");
        //grant update on user1.t4191 to user2
        user1St.execute("grant update on t4191 to user2");
        //following update will now work because it has the required update
        //privilege
        assertUpdateCount(user2St, 0, "update user1.t4191 set x=0");
        //folowing will still fail because there is no select privilege on 
        //user1.t4191(x)
        assertStatementError("42502", user2St, "update user1.t4191 set x=" +
		" ( select max(x) + 2 from user1.t4191 )");
        //following update will fail because there is no select privilege
        //on user1.t4191_table2
        assertStatementError("42502", user2St, "update user1.t4191 set x=" +
		" ( select z from user1.t4191_table2 )");
        user1St.execute("grant select(y) on t4191 to user2");
        //folowing will still fail because there is no select privilege on 
        //user1.t4191(x)
        assertStatementError("42502", user2St, "update user1.t4191 set x=" +
		" ( select max(x) + 2 from user1.t4191 )");
        user1St.execute("grant select(x) on t4191 to user2");
        //following will now work because we have all the required privileges
        assertUpdateCount(user2St, 0, "update user1.t4191 set x=" +
		" ( select max(x) + 2 from user1.t4191 )");
        //folowing will still fail because there is no select privilege on 
        //user1.t4191(x)
        assertStatementError("42502", user2St, "update user1.t4191 set x=" +
		" ( select z from user1.t4191_table2 )");
        user1St.execute("grant select on t4191_table2 to user2");
        //following will now pass
        assertUpdateCount(user2St, 0, "update user1.t4191 set x=" +
		" ( select z from user1.t4191_table2 )");

        //take away select privilege from one column and grant privilege on
        //another column in user1.t4191 to user2
        user1St.execute("revoke select(x) on t4191 from user2");
        //the following update will work because we still have update
        //privilege granted to user2
        assertUpdateCount(user2St, 0, "update user1.t4191 set x=0");
        //but following update won't work because there are no select
        //privileges available to user2 on user1.t4191(x)
        assertStatementError("42502", user2St, "update user1.t4191 set x=" +
		" ( select max(x) + 2 from user1.t4191 )");
        user1St.execute("grant select(y) on t4191 to user2");
        //following update still won't work because the select is granted on
        //user1.t4191(y) and not user1.t4191(x)
        assertStatementError("42502", user2St, "update user1.t4191 set x=" +
		" ( select max(x) + 2 from user1.t4191 )");
        //following queries will still work because there is still a 
        //select privilege on user1.t4191 available to user2
        rs = user2St.executeQuery("select count(*) from user1.t4191");
//IC see: https://issues.apache.org/jira/browse/DERBY-6411
        JDBC.assertSingleValueResultSet(rs, "0");
        rs = user2St.executeQuery("select count(1) from user1.t4191");
        JDBC.assertSingleValueResultSet(rs, "0");
        rs = user2St.executeQuery("select 1 from user1.t4191");
        JDBC.assertEmpty(rs);
        rs = user2St.executeQuery("select count(y) from user1.t4191");
        JDBC.assertSingleValueResultSet(rs, "0");
        //grant select privilege on user1.t4191(x) back to user2 so following
        //update can succeed
        user1St.execute("grant select(x) on t4191 to user2");
        assertUpdateCount(user2St, 0, "update user1.t4191 set x=" +
		" ( select max(x) + 2 from user1.t4191 )");

        user1St.execute("drop table t4191");
//IC see: https://issues.apache.org/jira/browse/DERBY-5102
        user1St.execute("drop table t4191_table2");
        user1St.execute("drop view view_t4191_table3");
        user1St.execute("drop table t4191_table3");
        user1St.execute("drop schema user1 restrict");
        user1.close();
        user2.close();
}


    /**
     * DERBY-3266
     */
    public void testGlobalTempTables() throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-3266
        Connection dbo  = getConnection();
        Statement dboSt = createStatement();

        Connection george = openUserConnection("george");
        Statement georgeSt = george.createStatement();

        ResultSet rs = null;

        // Dbo creates a global temporary table
        dboSt.executeUpdate("declare global temporary table t1(i int, j int) " +
                            "on commit preserve rows not logged");
        dboSt.executeUpdate("insert into session.t1 values (1,1),(1,1)");
        rs = dboSt.executeQuery("select * from session.t1");
        JDBC.assertFullResultSet(rs, new String [][] {{"1", "1"}, {"1", "1"}} );
        dboSt.executeUpdate("drop table session.t1");

        // Dbo creates a physical schema SESSION and a table with another name
        // than the global temporary table
        dboSt.executeUpdate("create schema session");
        dboSt.executeUpdate("create table session.t2(i int)");
        dboSt.executeUpdate("insert into session.t2 values 2,22");
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
        rs = dboSt.executeQuery("select * from session.t2 order by I");
        JDBC.assertFullResultSet(rs, new String [][] {{"2"}, {"22"}} );

        // Dbo creates a global temporary table with the same name as the
        // physical table in SESSION; see that global temporary table
        // overshadows the physical table.
        dboSt.executeUpdate("declare global temporary table t2(i int, j int) " +
                            "on commit preserve rows not logged");
        dboSt.executeUpdate("insert into session.t2 values (222,222),(2,2)");
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
        rs = dboSt.executeQuery("select * from session.t2 order by i");
        JDBC.assertFullResultSet(rs,
                                 new String [][] { {"2", "2"}, {"222", "222"}, } );

        // Non-dbo tries to access the physical table in SESSION schema (has no
        // privilege, so should get authorization error).
        assertStatementError("42502", georgeSt, "select * from session.t2");

        // Non-dbo tries to create a physical table in SESSION SCHEMA (has no
        // privilege, so should get authorization error).
        assertStatementError("42507", georgeSt,
                             "create table session.t3(i int)");

        // Non-dbo creates a global temporary table
        georgeSt.executeUpdate
            ("declare global temporary table t4(i int, j int) " +
             "on commit preserve rows not logged");
        georgeSt.executeUpdate("insert into session.t4 values (4,4),(44,44)");
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
        rs = georgeSt.executeQuery("select * from session.t4 order by i");
        JDBC.assertFullResultSet(rs,
                                 new String [][] {{"4", "4"}, {"44", "44"}} );

        // Another non-dbo connection can not see the global temporary table
        Connection monica = openUserConnection("monica");
        Statement monicaSt = monica.createStatement();
        assertStatementError("42X05",
                             monicaSt,
                             "select * from session.t4");

        // Original non-dbo drops the temporary table
        georgeSt.executeUpdate("drop table session.t4");


        // Dbo in new connection can still see physical table again
        dbo.close();
        dbo = getConnection();
        dboSt = dbo.createStatement();
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
        rs = dboSt.executeQuery("select * from session.t2 order by i");
        JDBC.assertFullResultSet(rs, new String [][] {{"2"}, {"22"}} );

        // close result sets
        rs.close();

        // Drop the objects created in this test case
//IC see: https://issues.apache.org/jira/browse/DERBY-5102
        dboSt.execute("drop table session.t2");
        dboSt.execute("drop schema session restrict");

        // close statements
        dboSt.close();
        georgeSt.close();
        monicaSt.close();

        // close connections
        dbo.close();
        george.close();
        monica.close();
    }
    
    /**
     * DERBY-4502
     *
     * Allow creation of views against system tables when authorization is enabled.
     */
    public void test_derby_4502() throws Exception
    {
        Connection mamta1 = openUserConnection("mamta1");
        Statement st_mamta1 = mamta1.createStatement();

        st_mamta1.execute(
            "create view v_4502( a ) as select tablename from sys.systables");

        // Clean up
//IC see: https://issues.apache.org/jira/browse/DERBY-5102
        st_mamta1.execute("drop view v_4502");
        st_mamta1.execute("drop schema mamta1 restrict");
    }

    // DERBY-5044 During alter table drop column, we recompile all the 
    //  dependent trigger's action plans to see if they are dependent
    //  on the column being dropped. The database may have been created
    //  with authorization on and hence different actions might require
    //  relevant privileges. This test will ensure that during the
    //  recompile of trigger action, we will not loose the privilege
    //  requirements for the triggers
    public void testAlterTablePrivilegesIntace() throws Exception {
        Connection user1Connection = openUserConnection("user1");
        Statement st_user1Connection = user1Connection.createStatement();
        
        st_user1Connection.executeUpdate(
        		"create table user1.t11 (c111 int, c112 int, c113 int)");
        st_user1Connection.executeUpdate(
        		"create table user1.t12 (c121 int, c122 int)");
        st_user1Connection.executeUpdate(
        		"create table user1.t13 (c131 int, c132 int)");        
        st_user1Connection.executeUpdate(
                " insert into user1.t11 values(1,2,3)");
        st_user1Connection.executeUpdate(
                " grant trigger on user1.t12 to user2");
        st_user1Connection.executeUpdate(
                " grant update(c112, c113) on user1.t11 to user2");
        st_user1Connection.executeUpdate(
                " grant select on user1.t11 to user2");
        st_user1Connection.executeUpdate(
                " grant insert on user1.t13 to user2");

        Connection user2Connection = openUserConnection("user2");
        Statement st_user2Connection = user2Connection.createStatement();
        st_user2Connection.executeUpdate(
                "create trigger tr1t12 after insert on user1.t12 " +
                "for each row mode db2sql " +
                "update user1.t11 set c112=222");
        st_user2Connection.executeUpdate(
                "create trigger tr2t12 after insert on user1.t12 " +
                "for each row mode db2sql " +
                "insert into user1.t13(c131, c132) " +
                "select c111, c113 from user1.t11");

        JDBC.assertFullResultSet(
        		st_user1Connection.executeQuery(" select * from user1.t11"),
                new String[][]{{"1","2","3"}});
        JDBC.assertEmpty(st_user1Connection.executeQuery(
                " select * from user1.t13"));
		st_user1Connection.executeUpdate(" insert into user1.t12 values(91,91)");
        JDBC.assertFullResultSet(
        		st_user1Connection.executeQuery(" select * from user1.t11"),
                new String[][]{{"1","222","3"}});
        JDBC.assertFullResultSet(
        		st_user1Connection.executeQuery(" select * from user1.t13"),
                new String[][]{{"1","3"}});
        st_user1Connection.executeUpdate(
                "delete from user1.t11");        
        st_user1Connection.executeUpdate(
                "delete from user1.t13");        
        st_user1Connection.executeUpdate(
                " insert into user1.t11 values(1,2,3)");
  
        assertStatementError("X0Y25", st_user1Connection,
                "alter table t11 drop column c112 restrict");
        JDBC.assertFullResultSet(
        		st_user1Connection.executeQuery(" select * from user1.t11"),
                new String[][]{{"1","2","3"}});
        JDBC.assertEmpty(st_user1Connection.executeQuery(
                " select * from user1.t13"));
		st_user1Connection.executeUpdate(" insert into user1.t12 values(92,92)");
        JDBC.assertFullResultSet(
        		st_user1Connection.executeQuery(" select * from user1.t11"),
                new String[][]{{"1","222","3"}});
        JDBC.assertFullResultSet(
        		st_user1Connection.executeQuery(" select * from user1.t13"),
                new String[][]{{"1","3"}});
        st_user1Connection.executeUpdate(
                "delete from user1.t11");        
        st_user1Connection.executeUpdate(
                "delete from user1.t13");        
        st_user1Connection.executeUpdate(
                " insert into user1.t11 values(1,2,3)");
        
        st_user1Connection.executeUpdate("alter table t11 drop column c112");
        JDBC.assertFullResultSet(
        		st_user1Connection.executeQuery(" select * from user1.t11"),
                new String[][]{{"1","3"}});
        JDBC.assertEmpty(st_user1Connection.executeQuery(
                " select * from user1.t13"));
		st_user1Connection.executeUpdate(" insert into user1.t12 values(93,93)");
        JDBC.assertFullResultSet(
        		st_user1Connection.executeQuery(" select * from user1.t11"),
                new String[][]{{"1","3"}});
        JDBC.assertFullResultSet(
        		st_user1Connection.executeQuery(" select * from user1.t13"),
                new String[][]{{"1","3"}});
        st_user1Connection.executeUpdate(
                "delete from user1.t11");        
        st_user1Connection.executeUpdate(
                "delete from user1.t13");        
        st_user1Connection.executeUpdate(
                " insert into user1.t11 values(1,3)");
        
        st_user1Connection.executeUpdate(
        		"revoke insert on table user1.t13 from user2");
        JDBC.assertFullResultSet(
        		st_user1Connection.executeQuery(" select * from user1.t11"),
                new String[][]{{"1","3"}});
        JDBC.assertEmpty(st_user1Connection.executeQuery(
                " select * from user1.t13"));
		st_user1Connection.executeUpdate(" insert into user1.t12 values(94,94)");
        JDBC.assertFullResultSet(
        		st_user1Connection.executeQuery(" select * from user1.t11"),
                new String[][]{{"1","3"}});
        JDBC.assertEmpty(st_user1Connection.executeQuery(
        		" select * from user1.t13"));
        st_user1Connection.executeUpdate(
        		"drop table user1.t11");
        st_user1Connection.executeUpdate(
        		"drop table user1.t12");
        st_user1Connection.executeUpdate(
        		"drop table user1.t13");

//IC see: https://issues.apache.org/jira/browse/DERBY-5409
        st_user1Connection.executeUpdate("drop schema user1 restrict");
        st_user2Connection.executeUpdate("drop schema user2 restrict");
    }

    // DERBY-5044 During alter table drop column, we recompile all the 
    //  dependent trigger's action plans to see if they are dependent
    //  on the column being dropped. Some of these triggers may have
    //  been created by a user different than one doing the alter table.
    //  The test below shows that we are able to handle such a case
    //  and able to detect trigger dependencies even if they are created
    //  by a different user
    public void testAlterTableWithPrivileges() throws Exception {
        Connection user1Connection = openUserConnection("user1");
        Statement st_user1Connection = user1Connection.createStatement();
        
        st_user1Connection.executeUpdate(
        		"create table user1.t11 (c111 int, c112 int)");
        st_user1Connection.executeUpdate(
        		"create table user1.t12 (c121 int, c122 int)");
        
        Connection user2Connection = openUserConnection("user2");
        Statement st_user2Connection = user2Connection.createStatement();
  
        // following create trigger fails because it is getting created on 
        //  non-granted object
        assertStatementError("42500", st_user2Connection,
            "create trigger tr1t12 after insert on user1.t12 for each row " +
            "mode db2sql insert into user1.t11(c112) values (1)");
        
        st_user1Connection.executeUpdate(
        		" grant insert on user1.t11 to user2");
        st_user1Connection.executeUpdate(
        		" grant trigger on user1.t12 to user2");
        
        // following create trigger should pass because user2 now has necessary
        //  privileges
        st_user2Connection.executeUpdate(
                "create trigger tr1t12 after insert on user1.t12 " +
                "for each row mode db2sql " +
                "insert into user1.t11(c112) values (1)");
        
        st_user1Connection.executeUpdate(
                " insert into user1.t12 values(91,91)");
        JDBC.assertFullResultSet(
//IC see: https://issues.apache.org/jira/browse/DERBY-6441
        		st_user1Connection.executeQuery(" select * from user1.t11 order by c111"),
                new String[][]{{null, "1"}});
        
        // following should fail because there is a dependent trigger on 
        //  t11.c112 and drop column is getting done in restrict mode
        assertStatementError("X0Y25", st_user1Connection,
                "alter table t11 drop column c112 restrict");
        st_user1Connection.executeUpdate(
                " insert into user1.t12 values(92,92)");
        JDBC.assertFullResultSet(
                st_user1Connection.executeQuery(" select * from user1.t11"),
                new String[][]{{null, "1"}, {null,"1"}});
        // following should pass because drop column is getting done in 
        //  cascade mode and so the dependent trigger will be dropped
        st_user1Connection.executeUpdate(
                "alter table t11 drop column c112");        
        //No new row will be inserted into user1.t11 because the trigger has
        //  been dropped
        st_user1Connection.executeUpdate(
                " insert into user1.t12 values(93,93)");
        JDBC.assertFullResultSet(
        		st_user1Connection.executeQuery(" select * from user1.t11"),
                new String[][]{{null}, {null}});
        st_user1Connection.executeUpdate(
                "drop table user1.t11");
        st_user1Connection.executeUpdate(
                "drop table user1.t12");

//IC see: https://issues.apache.org/jira/browse/DERBY-5409
        st_user1Connection.executeUpdate("drop schema user1 restrict");
        st_user2Connection.executeUpdate("drop schema user2 restrict");
    }

    /**
     * Test that UPDATE statements require the correct privileges as
     * described on DERBY-6429. Tables are referenced in SET and WHERE clauses.
     */
    public void test_6429_tables()
//IC see: https://issues.apache.org/jira/browse/DERBY-6429
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );

        //
        // Schema
        //
        goodStatement
            (
             dboConnection,
             "create table t1_simple_6429(x int, y int, z int)"
             );
        goodStatement
            (
             dboConnection,
             "create view v1_simple_6429(a, b) as select x, y from t1_simple_6429"
             );
        goodStatement
            (
             dboConnection,
             "create type SelectHashMap_6429 external name 'java.util.HashMap' language java\n"
             );
        goodStatement
            (
             dboConnection,
             "create type CheckHashMap_6429 external name 'java.util.HashMap' language java\n"
             );
        goodStatement
            (
             dboConnection,
             "create type WhereHashMap_6429 external name 'java.util.HashMap' language java\n"
             );
        goodStatement
            (
             dboConnection,
             "create function generationFunction_6429( rawValue int ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'java.lang.Math.abs'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function setFunction_6429( hashMap SelectHashMap_6429, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function checkFunction_6429( hashMap CheckHashMap_6429, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function whereFunction_6429( hashMap WhereHashMap_6429, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create derby aggregate setAggregate_6429 for int\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.ModeAggregate'\n"
             );
        goodStatement
            (
             dboConnection,
             "create derby aggregate whereAggregate_6429 for int\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.ModeAggregate'\n"
             );
        goodStatement
            (
             dboConnection,
             "create procedure addHistoryRow_6429\n" +
             "(\n" +
             "    actionString varchar( 20 ),\n" +
             "    actionValue int\n" +
             ")\n" +
             "language java parameter style java reads sql data\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.addHistoryRow'\n"
             );
        goodStatement
            (
             dboConnection,
             "create table primaryTable_6429\n" +
             "(\n" +
             "    key1 int,\n" +
             "    key2 int,\n" +
             "    primary key( key1, key2 )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table setTable_6429\n" +
             "(\n" +
             "    a int\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table whereTable_6429\n" +
             "(\n" +
             "    a int\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table updateTable_6429\n" +
             "(\n" +
             "    updateColumn int,\n" +
             "    selectColumn SelectHashMap_6429,\n" +
             "    untouchedGenerationSource int,\n" +
             "    generatedColumn generated always as ( updateColumn + generationFunction_6429( untouchedGenerationSource ) ),\n" +
             "    untouchedCheckSource CheckHashMap_6429,\n" +
             "    untouchedForeignSource int,\n" +
             "    untouchedBeforeTriggerSource int,\n" +
             "    untouchedAfterTriggerSource int,\n" +
             "    whereColumn WhereHashMap_6429,\n" +
             "    check ( updateColumn > checkFunction_6429( untouchedCheckSource, 'foo' ) ),\n" +
             "    foreign key ( updateColumn, untouchedForeignSource ) references primaryTable_6429( key1, key2 )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger beforeUpdateTrigger_6429\n" +
             "no cascade before update of updateColumn on updateTable_6429\n" +
             "referencing old as old\n" +
             "for each row\n" +
             "call addHistoryRow_6429( 'before', old.updateColumn + old.untouchedBeforeTriggerSource )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger afterUpdateTrigger_6429\n" +
             "after update of updateColumn on updateTable_6429\n" +
             "referencing old as old\n" +
             "for each row\n" +
             "call addHistoryRow_6429( 'before', old.updateColumn + old.untouchedAfterTriggerSource )\n"
             );

        //
        // Permissions
        //
        goodStatement
            (
             dboConnection,
             "grant update on t1_simple_6429 to ruth"
             );
        Permission[]    permissions = new Permission[]
        {
            new Permission( "execute on function setFunction_6429", NO_GENERIC_PERMISSION ),
            new Permission( "execute on function whereFunction_6429", NO_GENERIC_PERMISSION ),
            new Permission( "usage on derby aggregate setAggregate_6429", NO_GENERIC_PERMISSION ),
            new Permission( "usage on derby aggregate whereAggregate_6429", NO_GENERIC_PERMISSION ),
            new Permission( "update ( updateColumn ) on updateTable_6429", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select ( selectColumn ) on updateTable_6429", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select ( whereColumn ) on updateTable_6429", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select ( a ) on setTable_6429", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select( a ) on whereTable_6429", NO_SELECT_OR_UPDATE_PERMISSION ),
        };
        for ( Permission permission : permissions )
        {
            grant_6429( dboConnection, permission.text );
        }

        // Should fail because ruth doesn't have SELECT privilege on column y.
        expectExecutionError
            ( ruthConnection, NO_SELECT_OR_UPDATE_PERMISSION,
              "update test_dbo.t1_simple_6429 set x = y" );

        // Should fail because ruth doesn't have SELECT permission on v1_simple_6429.a
        String  simpleViewUpdate =
            "update test_dbo.t1_simple_6429\n" +
            "  set x =\n" +
            "  ( select b from test_dbo.v1_simple_6429 where a = 1 )\n";
        expectExecutionError
            ( ruthConnection, NO_SELECT_OR_UPDATE_PERMISSION, simpleViewUpdate );

        // Succeeds after we grant ruth that permission.
        goodStatement
            (
             dboConnection,
             "grant select on v1_simple_6429 to ruth"
             );
        goodStatement( ruthConnection, simpleViewUpdate );

        // Should fail because ruth doesn't have SELECT permission on t1_simple_6429.z
        String  simpleViewUpdate2 =
            "update test_dbo.t1_simple_6429 g\n" +
            "  set x =\n" +
            "  ( select b from test_dbo.v1_simple_6429 where a = g.z )\n";
        expectExecutionError
            ( ruthConnection, NO_SELECT_OR_UPDATE_PERMISSION, simpleViewUpdate2 );

        // Succeeds after we grant ruth that permission.
        goodStatement
            (
             dboConnection,
             "grant select( z ) on t1_simple_6429 to ruth"
             );
        goodStatement( ruthConnection, simpleViewUpdate2 );

        //
        // Try adding and dropping privileges.
        //
        String  update =
            "update test_dbo.updateTable_6429\n" +
            "    set updateColumn =\n" +
            "        test_dbo.setFunction_6429( selectColumn, 'foo' ) +\n" +
            "        ( select test_dbo.setAggregate_6429( a ) from test_dbo.setTable_6429 )\n" +
            "where\n" +
            "    test_dbo.whereFunction_6429( whereColumn, 'foo' ) >\n" +
            "    ( select test_dbo.whereAggregate_6429( a ) from test_dbo.whereTable_6429 )\n";

        goodStatement( ruthConnection, update );

        //
        // Verify that revoking each permission in isolation raises
        // the correct error.
        //
        for ( Permission permission : permissions )
        {
            vetPermission_6429( permission, dboConnection, ruthConnection, update );
        }
        
        //
        // Drop schema.
        //
        goodStatement
            (
             dboConnection,
             "drop view v1_simple_6429"
             );
        goodStatement
            (
             dboConnection,
             "drop table t1_simple_6429"
             );
        goodStatement
            (
             dboConnection,
             "drop table updateTable_6429"
             );
        goodStatement
            (
             dboConnection,
             "drop table whereTable_6429"
             );
        goodStatement
            (
             dboConnection,
             "drop table setTable_6429"
             );
        goodStatement
            (
             dboConnection,
             "drop table primaryTable_6429"
             );
        goodStatement
            (
             dboConnection,
             "drop procedure addHistoryRow_6429"
             );
        goodStatement
            (
             dboConnection,
             "drop derby aggregate whereAggregate_6429 restrict"
             );
        goodStatement
            (
             dboConnection,
             "drop derby aggregate setAggregate_6429 restrict"
             );
        goodStatement
            (
             dboConnection,
             "drop function whereFunction_6429"
             );
        goodStatement
            (
             dboConnection,
             "drop function checkFunction_6429"
             );
        goodStatement
            (
             dboConnection,
             "drop function setFunction_6429"
             );
        goodStatement
            (
             dboConnection,
             "drop function generationFunction_6429"
             );
        goodStatement
            (
             dboConnection,
             "drop type WhereHashMap_6429 restrict"
             );
        goodStatement
            (
             dboConnection,
             "drop type CheckHashMap_6429 restrict"
             );
        goodStatement
            (
             dboConnection,
             "drop type SelectHashMap_6429 restrict"
             );
        
    }
    /**
     * Verify that the update fails with the correct error after you revoke
     * a permission and that the update succeeds after you add the permission back.
     */
    private void    vetPermission_6429
        (
         Permission permission,
         Connection dboConnection,
         Connection ruthConnection,
         String update
         )
        throws Exception
    {
        revoke_6429( dboConnection, permission.text );
        expectExecutionError( ruthConnection, permission.sqlStateWhenMissing, update );
        grant_6429( dboConnection, permission.text );
        goodStatement( ruthConnection, update );
    }
    private void    grant_6429( Connection conn, String permission )
        throws Exception
    {
        String  command = "grant " + permission + " to ruth";

        goodStatement( conn, command );
    }
    private void    revoke_6429( Connection conn, String permission )
        throws Exception
    {
        String  command = "revoke " + permission + " from ruth";
        if ( permission.startsWith( "execute" ) || permission.startsWith( "usage" ) )   { command += " restrict"; }

        goodStatement( conn, command );
    }
    
    /**
     * Test that UPDATE statements require the correct privileges as
     * described on DERBY-6429. Views are referenced in SET and WHERE clauses.
     */
    public void test_6429_views()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );

        //
        // Schema
        //
        goodStatement
            (
             dboConnection,
             "create type SelectHashMap_6429_2 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type CheckHashMap_6429_2 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type WhereHashMap_6429_2 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create function generationFunction_6429_2( rawValue int ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'java.lang.Math.abs'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function setFunction_6429_2( hashMap SelectHashMap_6429_2, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function checkFunction_6429_2( hashMap CheckHashMap_6429_2, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function whereFunction_6429_2( hashMap WhereHashMap_6429_2, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create derby aggregate setAggregate_6429_2 for int\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.ModeAggregate'\n"
             );
        goodStatement
            (
             dboConnection,
             "create derby aggregate whereAggregate_6429_2 for int\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.ModeAggregate'\n"
             );
        goodStatement
            (
             dboConnection,
             "create table primaryTable_6429_2\n" +
             "(\n" +
             "    key1 int,\n" +
             "    key2 int,\n" +
             "    primary key( key1, key2 )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table setTable_6429_2\n" +
             "(\n" +
             "    a int\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create view setView_6429_2( setViewCol ) as select a from setTable_6429_2"
             );
        goodStatement
            (
             dboConnection,
             "create table whereTable_6429_2\n" +
             "(\n" +
             "    a int\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create view whereView_6429_2( whereViewCol ) as select a from whereTable_6429_2"
             );
        goodStatement
            (
             dboConnection,
             "create table updateTable_6429_2\n" +
             "(\n" +
             "    updateColumn int,\n" +
             "    selectColumn SelectHashMap_6429_2,\n" +
             "    untouchedGenerationSource int,\n" +
             "    generatedColumn generated always as ( updateColumn + generationFunction_6429_2( untouchedGenerationSource ) ),\n" +
             "    untouchedCheckSource CheckHashMap_6429_2,\n" +
             "    untouchedForeignSource int,\n" +
             "    untouchedBeforeTriggerSource int,\n" +
             "    untouchedAfterTriggerSource int,\n" +
             "    whereColumn WhereHashMap_6429_2,\n" +
             "    check ( updateColumn > checkFunction_6429_2( untouchedCheckSource, 'foo' ) ),\n" +
             "    foreign key ( updateColumn, untouchedForeignSource ) references primaryTable_6429_2( key1, key2 )\n" +
             ")\n"
             );

        Permission[]    permissions = new Permission[]
        {
            new Permission( "execute on function setFunction_6429_2", NO_GENERIC_PERMISSION ),
            new Permission( "execute on function whereFunction_6429_2", NO_GENERIC_PERMISSION ),
            new Permission( "usage on derby aggregate setAggregate_6429_2", NO_GENERIC_PERMISSION ),
            new Permission( "usage on derby aggregate whereAggregate_6429_2", NO_GENERIC_PERMISSION ),
            new Permission( "update ( updateColumn ) on updateTable_6429_2", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select ( selectColumn ) on updateTable_6429_2", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select ( whereColumn ) on updateTable_6429_2", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select ( setViewCol ) on setView_6429_2", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select( whereViewCol ) on whereView_6429_2", NO_SELECT_OR_UPDATE_PERMISSION ),
        };
        for ( Permission permission : permissions )
        {
            grant_6429( dboConnection, permission.text );
        }

        //
        // Try adding and dropping privileges.
        //
        String  update =
            "update test_dbo.updateTable_6429_2\n" +
            "    set updateColumn =\n" +
            "        test_dbo.setFunction_6429_2( selectColumn, 'foo' ) +\n" +
            "        ( select test_dbo.setAggregate_6429_2( setViewCol ) from test_dbo.setView_6429_2 )\n" +
            "where\n" +
            "    test_dbo.whereFunction_6429_2( whereColumn, 'foo' ) >\n" +
            "    ( select test_dbo.whereAggregate_6429_2( whereViewCol ) from test_dbo.whereView_6429_2 )\n";

        goodStatement( ruthConnection, update );

        //
        // Verify that revoking each permission in isolation raises
        // the correct error.
        //
        for ( Permission permission : permissions )
        {
            vetPermission_6429( permission, dboConnection, ruthConnection, update );
        }
        
        //
        // Drop schema.
        //
        goodStatement
            (
             dboConnection,
             "drop table updateTable_6429_2"
             );
        goodStatement
            (
             dboConnection,
             "drop view whereView_6429_2"
             );
        goodStatement
            (
             dboConnection,
             "drop table whereTable_6429_2"
             );
        goodStatement
            (
             dboConnection,
             "drop view setView_6429_2"
             );
        goodStatement
            (
             dboConnection,
             "drop table setTable_6429_2"
             );
        goodStatement
            (
             dboConnection,
             "drop table primaryTable_6429_2"
             );
        goodStatement
            (
             dboConnection,
             "drop derby aggregate whereAggregate_6429_2 restrict"
             );
        goodStatement
            (
             dboConnection,
             "drop derby aggregate setAggregate_6429_2 restrict"
             );
        goodStatement
            (
             dboConnection,
             "drop function whereFunction_6429_2"
             );
        goodStatement
            (
             dboConnection,
             "drop function checkFunction_6429_2"
             );
        goodStatement
            (
             dboConnection,
             "drop function setFunction_6429_2"
             );
        goodStatement
            (
             dboConnection,
             "drop function generationFunction_6429_2"
             );
        goodStatement
            (
             dboConnection,
             "drop type WhereHashMap_6429_2 restrict"
             );
        goodStatement
            (
             dboConnection,
             "drop type CheckHashMap_6429_2 restrict"
             );
        goodStatement
            (
             dboConnection,
             "drop type SelectHashMap_6429_2 restrict"
             );

    }
    
    /**
     * Test that UPDATE statements require the correct privileges as
     * described on DERBY-6429. Table functions are referenced in SET and WHERE clauses.
     */
    public void test_6429_tableFunctions()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );

        //
        // Schema
        //
        goodStatement
            (
             dboConnection,
             "create type SelectHashMap_6429_3 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type CheckHashMap_6429_3 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type WhereHashMap_6429_3 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create function generationFunction_6429_3( rawValue int ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'java.lang.Math.abs'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function setFunction_6429_3( hashMap SelectHashMap_6429_3, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function checkFunction_6429_3( hashMap CheckHashMap_6429_3, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function whereFunction_6429_3( hashMap WhereHashMap_6429_3, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create derby aggregate setAggregate_6429_3 for int\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.ModeAggregate'\n"
             );
        goodStatement
            (
             dboConnection,
             "create derby aggregate whereAggregate_6429_3 for int\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.ModeAggregate'\n"
             );
        goodStatement
            (
             dboConnection,
             "create table primaryTable_6429_3\n" +
             "(\n" +
             "    key1 int,\n" +
             "    key2 int,\n" +
             "    primary key( key1, key2 )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create function setTableFunction_6429_3()\n" +
             "returns table( x int, y int, z int, w int )\n" +
             "language java\n" +
             "parameter style derby_jdbc_result_set\n" +
             "no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.RestrictedVTITest.integerList'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function whereTableFunction_6429_3()\n" +
             "returns table( x int, y int, z int, w int )\n" +
             "language java\n" +
             "parameter style derby_jdbc_result_set\n" +
             "no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.RestrictedVTITest.integerList'\n"
             );
        goodStatement
            (
             dboConnection,
             "create table updateTable_6429_3\n" +
             "(\n" +
             "    updateColumn int,\n" +
             "    selectColumn SelectHashMap_6429_3,\n" +
             "    untouchedGenerationSource int,\n" +
             "    generatedColumn generated always as ( updateColumn + generationFunction_6429_3( untouchedGenerationSource ) ),\n" +
             "    untouchedCheckSource CheckHashMap_6429_3,\n" +
             "    untouchedForeignSource int,\n" +
             "    untouchedBeforeTriggerSource int,\n" +
             "    untouchedAfterTriggerSource int,\n" +
             "    whereColumn WhereHashMap_6429_3,\n" +
             "    check ( updateColumn > checkFunction_6429_3( untouchedCheckSource, 'foo' ) ),\n" +
             "    foreign key ( updateColumn, untouchedForeignSource ) references primaryTable_6429_3( key1, key2 )\n" +
             ")\n"
             );

        Permission[]    permissions = new Permission[]
        {
            new Permission( "execute on function setFunction_6429_3", NO_GENERIC_PERMISSION ),
            new Permission( "execute on function whereFunction_6429_3", NO_GENERIC_PERMISSION ),
            new Permission( "usage on derby aggregate setAggregate_6429_3", NO_GENERIC_PERMISSION ),
            new Permission( "usage on derby aggregate whereAggregate_6429_3", NO_GENERIC_PERMISSION ),
            new Permission( "update ( updateColumn ) on updateTable_6429_3", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select ( selectColumn ) on updateTable_6429_3", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select ( whereColumn ) on updateTable_6429_3", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "execute on function setTableFunction_6429_3", NO_GENERIC_PERMISSION ),
            new Permission( "execute on function whereTableFunction_6429_3", NO_GENERIC_PERMISSION ),
        };
        for ( Permission permission : permissions )
        {
            grant_6429( dboConnection, permission.text );
        }

        //
        // Try adding and dropping privileges.
        //
        String  update =
            "update test_dbo.updateTable_6429_3\n" +
            "  set updateColumn =\n" +
            "    test_dbo.setFunction_6429_3( selectColumn, 'foo' ) + \n" +
            "    ( select test_dbo.setAggregate_6429_3( x ) from table( test_dbo.setTableFunction_6429_3() ) stf )\n" +
            "where test_dbo.whereFunction_6429_3( whereColumn, 'foo' ) >\n" +
            "    ( select test_dbo.whereAggregate_6429_3( x ) from table ( test_dbo.whereTableFunction_6429_3() ) wtf )\n";

        goodStatement( ruthConnection, update );

        //
        // Verify that revoking each permission in isolation raises
        // the correct error.
        //
        for ( Permission permission : permissions )
        {
            vetPermission_6429( permission, dboConnection, ruthConnection, update );
        }
        
        //
        // Drop schema.
        //
        goodStatement
            (
             dboConnection,
             "drop table updateTable_6429_3"
             );
        goodStatement
            (
             dboConnection,
             "drop function whereTableFunction_6429_3"
             );
        goodStatement
            (
             dboConnection,
             "drop function setTableFunction_6429_3"
             );
        goodStatement
            (
             dboConnection,
             "drop table primaryTable_6429_3"
             );
        goodStatement
            (
             dboConnection,
             "drop derby aggregate whereAggregate_6429_3 restrict"
             );
        goodStatement
            (
             dboConnection,
             "drop derby aggregate setAggregate_6429_3 restrict"
             );
        goodStatement
            (
             dboConnection,
             "drop function whereFunction_6429_3"
             );
        goodStatement
            (
             dboConnection,
             "drop function checkFunction_6429_3"
             );
        goodStatement
            (
             dboConnection,
             "drop function setFunction_6429_3"
             );
        goodStatement
            (
             dboConnection,
             "drop function generationFunction_6429_3"
             );
        goodStatement
            (
             dboConnection,
             "drop type WhereHashMap_6429_3 restrict"
             );
        goodStatement
            (
             dboConnection,
             "drop type CheckHashMap_6429_3 restrict"
             );
        goodStatement
            (
             dboConnection,
             "drop type SelectHashMap_6429_3 restrict"
             );
    }
    
    /**
     * Test that INSERT statements require the correct privileges as
     * described on DERBY-6434.
     */
    public void test_6434_insert()
//IC see: https://issues.apache.org/jira/browse/DERBY-6434
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );

        //
        // Schema
        //
        goodStatement
            (
             dboConnection,
             "create type GenerationType_6434 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type CheckType_6434 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type SelectType_6434 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create function generationFunction_6434( hashMap GenerationType_6434, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function checkFunction_6434( hashMap CheckType_6434, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function selectFunction_6434( hashMap SelectType_6434, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create derby aggregate selectAggregate_6434 for int\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.ModeAggregate'\n"
             );
        goodStatement
            (
             dboConnection,
             "create sequence sequence_6434"
             );
        goodStatement
            (
             dboConnection,
             "create procedure addHistoryRow_6434\n" +
             "(\n" +
             "    actionString varchar( 20 ),\n" +
             "    actionValue int\n" +
             ")\n" +
             "language java parameter style java reads sql data\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.addHistoryRow'\n"
             );
        goodStatement
            (
             dboConnection,
             "create table primaryTable_6434\n" +
             "(\n" +
             "    key1 int primary key\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table selectTable_6434\n" +
             "(\n" +
             "    selectColumn int,\n" +
             "    selectColumn2 SelectType_6434\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table insertTable_6434\n" +
             "(\n" +
             "    insertColumn int references primaryTable_6434( key1 ),\n" +
             "    privatePrimaryColumn int primary key,\n" +
             "    privateGenerationSource GenerationType_6434,\n" +
             "    privateForeignSource int,\n" +
             "    privateCheckSource CheckType_6434,\n" +
             "    privateBeforeTriggerSource int,\n" +
             "    privateAfterTriggerSource int,\n" +
             "    generatedColumn generated always as ( insertColumn + generationFunction_6434( privateGenerationSource, 'foo' ) ),\n" +
             "    check ( insertColumn > checkFunction_6434( privateCheckSource, 'foo' ) )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table foreignTable_6434\n" +
             "(\n" +
             "    key1 int references insertTable_6434( privatePrimaryColumn )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger beforeInsertTrigger_6434\n" +
             "no cascade before insert on insertTable_6434\n" +
             "referencing new as new\n" +
             "for each row\n" +
             "call addHistoryRow_6434( 'before', new.insertColumn + new.privateBeforeTriggerSource )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger afterInsertTrigger_6434\n" +
             "after insert on insertTable_6434\n" +
             "referencing new as new\n" +
             "for each row\n" +
             "call addHistoryRow_6434( 'before', new.insertColumn + new.privateAfterTriggerSource )\n"
             );

        //
        // Privileges
        //
        Permission[]    permissions = new Permission[]
        {
            new Permission( "insert on insertTable_6434", NO_TABLE_PERMISSION ),
            new Permission( "usage on sequence sequence_6434", NO_GENERIC_PERMISSION ),
            new Permission( "execute on function selectFunction_6434", NO_GENERIC_PERMISSION ),
            new Permission( "usage on derby aggregate selectAggregate_6434", NO_GENERIC_PERMISSION ),
            new Permission( "select on selectTable_6434", NO_SELECT_OR_UPDATE_PERMISSION ),
        };
        for ( Permission permission : permissions )
        {
            grant_6429( dboConnection, permission.text );
        }

        //
        // Try adding and dropping privileges.
        //
        String  insert =
            "insert into test_dbo.insertTable_6434( insertColumn, privatePrimaryColumn )\n" +
            "    select next value for test_dbo.sequence_6434, test_dbo.selectFunction_6434( selectColumn2, 'foo' )\n" +
            "    from test_dbo.selectTable_6434\n" +
            "    where selectColumn > ( select test_dbo.selectAggregate_6434( selectColumn ) from test_dbo.selectTable_6434 )\n";

        goodStatement( ruthConnection, insert );
        
        //
        // Verify that revoking each permission in isolation raises
        // the correct error.
        //
        for ( Permission permission : permissions )
        {
            vetPermission_6429( permission, dboConnection, ruthConnection, insert );
        }

        //
        // Drop schema
        //
        goodStatement
            (
             dboConnection,
             "drop trigger afterInsertTrigger_6434"
             );
        goodStatement
            (
             dboConnection,
             "drop trigger beforeInsertTrigger_6434"
             );
        goodStatement
            (
             dboConnection,
             "drop table selectTable_6434"
             );
        goodStatement
            (
             dboConnection,
             "drop table foreignTable_6434"
             );
        goodStatement
            (
             dboConnection,
             "drop table insertTable_6434"
             );
        goodStatement
            (
             dboConnection,
             "drop table primaryTable_6434"
             );
        goodStatement
            (
             dboConnection,
             "drop procedure addHistoryRow_6434"
             );
        goodStatement
            (
             dboConnection,
             "drop sequence sequence_6434 restrict"
             );
        goodStatement
            (
             dboConnection,
             "drop derby aggregate selectAggregate_6434 restrict"
             );
        goodStatement
            (
             dboConnection,
             "drop function selectFunction_6434"
             );
        goodStatement
            (
             dboConnection,
             "drop function checkFunction_6434"
             );
        goodStatement
            (
             dboConnection,
             "drop function generationFunction_6434"
             );
        goodStatement
            (
             dboConnection,
             "drop type SelectType_6434 restrict"
             );
        goodStatement
            (
             dboConnection,
             "drop type CheckType_6434 restrict"
             );
        goodStatement
            (
             dboConnection,
             "drop type GenerationType_6434 restrict"
             );
    }
    
    /**
     * Test that DELETE statements require the correct privileges as
     * described on DERBY-6434.
     */
    public void test_6434_delete()
//IC see: https://issues.apache.org/jira/browse/DERBY-6434
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );

        //
        // Schema
        //
        goodStatement
            (
             dboConnection,
             "create type SelectType_6434_2 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type BeforeTriggerType_6434_2 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type AfterTriggerType_6434_2 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create function selectFunction_6434_2( hashMap SelectType_6434_2, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function beforeTriggerFunction_6434_2( hashMap BeforeTriggerType_6434_2, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function afterTriggerFunction_6434_2( hashMap AfterTriggerType_6434_2, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create derby aggregate selectAggregate_6434_2 for int\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.ModeAggregate'\n"
             );
        goodStatement
            (
             dboConnection,
             "create procedure addHistoryRow_6434_2\n" +
             "(\n" +
             "    actionString varchar( 20 ),\n" +
             "    actionValue int\n" +
             ")\n" +
             "language java parameter style java reads sql data\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest.addHistoryRow'\n"
             );
        goodStatement
            (
             dboConnection,
             "create table primaryTable_6434_2\n" +
             "(\n" +
             "    key1 int primary key\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table selectTable_6434_2\n" +
             "(\n" +
             "    selectColumn int,\n" +
             "    selectColumn2 SelectType_6434_2\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table deleteTable_6434_2\n" +
             "(\n" +
             "    privateForeignColumn int references primaryTable_6434_2( key1 ),\n" +
             "    privatePrimaryColumn int primary key,\n" +
             "    privateBeforeTriggerSource BeforeTriggerType_6434_2,\n" +
             "    privateAfterTriggerSource AfterTriggerType_6434_2,\n" +
             "    publicSelectColumn int\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table foreignTable_6434_2\n" +
             "(\n" +
             "    key1 int references deleteTable_6434_2( privatePrimaryColumn )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger beforeDeleteTrigger_6434_2\n" +
             "no cascade before delete on deleteTable_6434_2\n" +
             "referencing old as old\n" +
             "for each row\n" +
             "call addHistoryRow_6434_2( 'before', beforeTriggerFunction_6434_2( old.privateBeforeTriggerSource, 'foo' ) )\n"
             );
        goodStatement
            (
             dboConnection,
             "create trigger afterDeleteTrigger_6434_2\n" +
             "after delete on deleteTable_6434_2\n" +
             "referencing old as old\n" +
             "for each row\n" +
             "call addHistoryRow_6434_2( 'after', afterTriggerFunction_6434_2( old.privateAfterTriggerSource, 'foo' ) )\n"
             );

        //
        // Privileges
        //
        Permission[]    permissions = new Permission[]
        {
            new Permission( "delete on deleteTable_6434_2", NO_TABLE_PERMISSION ),
            new Permission( "execute on function selectFunction_6434_2", NO_GENERIC_PERMISSION ),
            new Permission( "usage on derby aggregate selectAggregate_6434_2", NO_GENERIC_PERMISSION ),
            new Permission( "select on selectTable_6434_2", NO_SELECT_OR_UPDATE_PERMISSION ),
            new Permission( "select ( publicSelectColumn ) on deleteTable_6434_2", NO_SELECT_OR_UPDATE_PERMISSION ),
        };
        for ( Permission permission : permissions )
        {
            grant_6429( dboConnection, permission.text );
        }

        //
        // Try adding and dropping privileges.
        //
        String  delete =
            "delete from test_dbo.deleteTable_6434_2\n" +
            "where publicSelectColumn =\n" +
            "(\n" +
            "    select test_dbo.selectAggregate_6434_2( selectColumn )\n" +
            "    from test_dbo.selectTable_6434_2\n" +
            "    where test_dbo.selectFunction_6434_2( selectColumn2, 'foo' ) < 100\n" +
            ")\n";

        goodStatement( ruthConnection, delete );
        
        //
        // Verify that revoking each permission in isolation raises
        // the correct error.
        //
        for ( Permission permission : permissions )
        {
            vetPermission_6429( permission, dboConnection, ruthConnection, delete );
        }

        //
        // Drop schema
        //
        goodStatement
            (
             dboConnection,
             "drop table foreignTable_6434_2"
             );
        goodStatement
            (
             dboConnection,
             "drop table deleteTable_6434_2"
             );
        goodStatement
            (
             dboConnection,
             "drop table selectTable_6434_2"
             );
        goodStatement
            (
             dboConnection,
             "drop table primaryTable_6434_2"
             );
        goodStatement
            (
             dboConnection,
             "drop procedure addHistoryRow_6434_2"
             );
        goodStatement
            (
             dboConnection,
             "drop derby aggregate selectAggregate_6434_2 restrict"
             );
        goodStatement
            (
             dboConnection,
             "drop function afterTriggerFunction_6434_2"
             );
        goodStatement
            (
             dboConnection,
             "drop function beforeTriggerFunction_6434_2"
             );
        goodStatement
            (
             dboConnection,
             "drop function selectFunction_6434_2"
             );
        goodStatement
            (
             dboConnection,
             "drop type AfterTriggerType_6434_2 restrict"
             );
        goodStatement
            (
             dboConnection,
             "drop type BeforeTriggerType_6434_2 restrict"
             );
        goodStatement
            (
             dboConnection,
             "drop type SelectType_6434_2 restrict"
             );
    }
    
    /**
     * Test that INSERT statements driven by SELECTs require the correct privileges as
     * described on DERBY-6434.
     */
    public void test_6434_select()
//IC see: https://issues.apache.org/jira/browse/DERBY-6434
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );

        //
        // Schema
        //
        goodStatement
            (
             dboConnection,
             "create type SourceValueType_6434_3 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type TargetValueType_6434_3 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create function sourceValueExtractor_6434_3( hashMap SourceValueType_6434_3, hashKey varchar( 32672 ) ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.getIntValue'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function sourceValueMaker_6434_3( hashKey varchar( 32672 ), hashValue int ) returns SourceValueType_6434_3\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.makeHashMap'\n"
             );
        goodStatement
            (
             dboConnection,
             "create function targetValueMaker_6434_3( hashKey varchar( 32672 ), hashValue int ) returns TargetValueType_6434_3\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.makeHashMap'\n"
             );
        goodStatement
            (
             dboConnection,
             "create table targetTable_6434_3( a TargetValueType_6434_3 )"
             );
        goodStatement
            (
             dboConnection,
             "create table sourceTable_6434_3( b SourceValueType_6434_3 )"
             );

        //
        // Privileges
        //
        goodStatement
            (
             dboConnection,
             "grant insert on targetTable_6434_3 to ruth"
             );
        goodStatement
            (
             dboConnection,
             "grant execute on function sourceValueExtractor_6434_3 to ruth"
             );
        goodStatement
            (
             dboConnection,
             "grant execute on function sourceValueMaker_6434_3 to ruth"
             );
        goodStatement
            (
             dboConnection,
             "grant execute on function targetValueMaker_6434_3 to ruth"
             );
        goodStatement
            (
             dboConnection,
             "grant select on sourceTable_6434_3 to ruth"
             );

        // the problem SELECT-driven INSERT
        goodStatement
            (
             ruthConnection,
             "insert into test_dbo.targetTable_6434_3\n" +
             "  select test_dbo.targetValueMaker_6434_3( 'bar', test_dbo.sourceValueExtractor_6434_3( b, 'foo' ) )\n" +
             "  from test_dbo.sourceTable_6434_3\n"
             );

        // make sure that privilege checks are still needed for explicit casts
        expectExecutionError
            (
             ruthConnection,
             NO_GENERIC_PERMISSION,
             "select * from test_dbo.sourceTable_6434_3\n" +
             "where ( cast( null as test_dbo.SourceValueType_6434_3 ) ) is not null\n"
             );

        //
        // Drop schema
        //
        goodStatement
            (
             dboConnection,
             "drop table sourceTable_6434_3"
             );
        goodStatement
            (
             dboConnection,
             "drop table targetTable_6434_3"
             );
        goodStatement
            (
             dboConnection,
             "drop function targetValueMaker_6434_3"
             );
        goodStatement
            (
             dboConnection,
             "drop function sourceValueMaker_6434_3"
             );
        goodStatement
            (
             dboConnection,
             "drop function sourceValueExtractor_6434_3"
             );
        goodStatement
            (
             dboConnection,
             "drop type TargetValueType_6434_3 restrict"
             );
        goodStatement
            (
             dboConnection,
             "drop type SourceValueType_6434_3 restrict"
             );
    }
    
    /**
     * Test that INSERT and UPDATEs run CHECK constraints with definer's rights.
     */
    public void test_6432()
//IC see: https://issues.apache.org/jira/browse/DERBY-6434
//IC see: https://issues.apache.org/jira/browse/DERBY-6432
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );

        //
        // Schema
        //
        goodStatement
            (
             dboConnection,
             "create function absoluteValue_6432( inputValue int ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'java.lang.Math.abs'\n"
             );
        goodStatement
            (
             dboConnection,
             "create type hashmap_6432 external name 'java.util.HashMap' language java\n"
             );
        goodStatement
            (
             dboConnection,
             "create function makeHashmap_6432() returns hashmap_6432\n" +
             "language java parameter style java no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.makeHashMap'\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t1_check_function_6432\n" +
             "(\n" +
             "    a int check ( absoluteValue_6432( a ) > 100 )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t1_check_type_6432\n" +
             "(\n" +
             "    a hashmap_6432 check( (a is null) or (a is not null) )\n" +
             ")\n"
             );

        //
        // Data
        //
        goodStatement
            (
             dboConnection,
             "insert into t1_check_function_6432( a ) values -101"
             );
        goodStatement
            (
             dboConnection,
             "insert into t1_check_type_6432( a ) values ( makeHashmap_6432() )"
             );

        //
        // Privileges
        //
        goodStatement
            (
             dboConnection,
             "grant insert on t1_check_function_6432 to ruth"
             );
        goodStatement
            (
             dboConnection,
             "grant update on t1_check_function_6432 to ruth"
             );
        goodStatement
            (
             dboConnection,
             "grant insert on t1_check_type_6432 to ruth"
             );
        goodStatement
            (
             dboConnection,
             "grant update on t1_check_type_6432 to ruth"
             );

        //
        // Succeeds after the changes made by DERBY-6429 and DERBY-6434.
        //
        goodStatement
            (
             ruthConnection,
             "insert into test_dbo.t1_check_function_6432 values ( -102 )"
             );
        goodStatement
            (
             ruthConnection,
             "update test_dbo.t1_check_function_6432 set a = -103"
             );
        goodStatement
            (
             ruthConnection,
             "insert into test_dbo.t1_check_type_6432 values ( null )"
             );
        goodStatement
            (
             ruthConnection,
             "update test_dbo.t1_check_type_6432 set a = null"
             );

        //
        // Drop schema
        //
        goodStatement
            (
             dboConnection,
             "drop table t1_check_type_6432"
             );
        goodStatement
            (
             dboConnection,
             "drop table t1_check_function_6432"
             );
        goodStatement
            (
             dboConnection,
             "drop function makeHashmap_6432"
             );
        goodStatement
            (
             dboConnection,
             "drop type hashmap_6432 restrict"
             );
        goodStatement
            (
             dboConnection,
             "drop function absoluteValue_6432"
             );
    }
    
    /**
     * Test that INSERT and UPDATEs run generation expressions with definer's rights.
     */
    public void test_6433()
//IC see: https://issues.apache.org/jira/browse/DERBY-6434
//IC see: https://issues.apache.org/jira/browse/DERBY-6433
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );

        //
        // Schema
        //
        goodStatement
            (
             dboConnection,
             "create function absoluteValue_6433( inputValue int ) returns int\n" +
             "language java parameter style java deterministic no sql\n" +
             "external name 'java.lang.Math.abs'\n"
             );
        goodStatement
            (
             dboConnection,
             "create type hashmap_6433 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create function makeHashMap_6423() returns hashmap_6433\n" +
             "language java parameter style java no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.makeHashMap'\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t1_generated_function_6433\n" +
             "(\n" +
             "    a int,\n" +
             "    b int generated always as ( absoluteValue_6433( a ) )\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "create table t1_generated_type_6433\n" +
             "(\n" +
             "    a hashmap_6433,\n" +
             "    b boolean generated always as ( a is null )\n" +
             ")\n"
             );
        
        //
        // Data
        //
        goodStatement
            (
             dboConnection,
             "insert into t1_generated_function_6433( a ) values -101"
             );
        goodStatement
            (
             dboConnection,
             "insert into t1_generated_type_6433( a ) values ( makeHashMap_6423() )"
             );

        //
        // Privileges
        //
        goodStatement
            (
             dboConnection,
             "grant insert on t1_generated_function_6433 to ruth"
             );
        goodStatement
            (
             dboConnection,
             "grant update on t1_generated_function_6433 to ruth"
             );
        goodStatement
            (
             dboConnection,
             "grant insert on t1_generated_type_6433 to ruth"
             );
        goodStatement
            (
             dboConnection,
             "grant update on t1_generated_type_6433 to ruth"
             );
        
        //
        // Verify that granted permissions are sufficient for ruth
        // to INSERT and UPDATE the table.
        //
        goodStatement
            (
             ruthConnection,
             "insert into test_dbo.t1_generated_function_6433( a ) values ( -102 )"
             );
        goodStatement
            (
             ruthConnection,
             "update test_dbo.t1_generated_function_6433 set a = -103"
             );
        goodStatement
            (
             ruthConnection,
             "insert into test_dbo.t1_generated_type_6433( a ) values ( null )"
             );
        goodStatement
            (
             ruthConnection,
             "update test_dbo.t1_generated_type_6433 set a = null"
             );
        
        //
        // Drop schema
        //
        goodStatement
            (
             dboConnection,
             "drop table t1_generated_type_6433"
             );
        goodStatement
            (
             dboConnection,
             "drop table t1_generated_function_6433"
             );
        goodStatement
            (
             dboConnection,
             "drop function makeHashMap_6423"
             );
        goodStatement
            (
             dboConnection,
             "drop type hashmap_6433 restrict"
             );
        goodStatement
            (
             dboConnection,
             "drop function absoluteValue_6433"
             );
    }

    /**
     * Test that SELECT does not require USAGE privilege on the user-defined
     * types of the table columns.
     */
    public void test_6491()
//IC see: https://issues.apache.org/jira/browse/DERBY-6491
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );

        //
        // Schema
        //
        goodStatement
            (
             dboConnection,
             "create type SourceUnreferencedType_6491 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create type SourceValueType_6491 external name 'java.util.HashMap' language java"
             );
        goodStatement
            (
             dboConnection,
             "create table sourceTable_6491\n" +
             "(\n" +
             "    sourceUnreferencedColumn SourceUnreferencedType_6491,\n" +
             "    sourceValueColumn SourceValueType_6491\n" +
             ")\n"
             );
        goodStatement
            (
             dboConnection,
             "grant select( sourceValueColumn ) on sourceTable_6491 to ruth"
             );

        // test that SELECT is the only privilege needed
        goodStatement
            (
             ruthConnection,
             "select sourceValueColumn from test_dbo.sourceTable_6491"
             );

        //
        // Drop schema
        //
        goodStatement
            (
             dboConnection,
             "drop table sourceTable_6491"
             );
        goodStatement
            (
             dboConnection,
             "drop type SourceUnreferencedType_6491 restrict"
             );
        goodStatement
            (
             dboConnection,
             "drop type SourceValueType_6491 restrict"
             );
    }
    
}
