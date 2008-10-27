/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.Changes10_5

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

import org.apache.derbyTesting.junit.JDBCDataSource;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.CallableStatement;

import javax.sql.DataSource;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Upgrade test cases for 10.5.
 * If the old version is 10.5 or later then these tests
 * will not be run.
 * <BR>
    10.5 Upgrade issues

    <UL>
    <LI> testUpdateStatisticsProcdure - DERBY-269
    Make sure that SYSCS_UTIL.SYSCS_UPDATE_STATISTICS can only be run in Derby
    10.5 and higher.
    </UL>

 */
public class Changes10_5 extends UpgradeChange {

    private static  final   String  BAD_SYNTAX = "42X01";

    public Changes10_5(String name) {
        super(name);
    }

    /**
     * Return the suite of tests to test the changes made in 10.5.
     * @param phase an integer that indicates the current phase in
     *              the upgrade test.
     * @return the test suite created.
     */
    public static Test suite(int phase) {
        TestSuite suite = new TestSuite("Upgrade test for 10.5");

        suite.addTestSuite(Changes10_5.class);
        return new SupportFilesSetup((Test) suite);
    }

    /**
     * Make sure that SYSCS_UTIL.SYSCS_UPDATE_STATISTICS can only be run in 
     * Derby 10.5 and higher. 
     * DERBY-269
     * Test added for 10.5.
     * @throws SQLException
     *
     */
    public void testUpdateStatisticsProcdure() throws SQLException
    {
    	Statement s;
        switch (getPhase())
        {
        case PH_CREATE:
            s = createStatement();
            s.execute("CREATE TABLE DERBY_269(c11 int, c12 char(20))");
            s.execute("INSERT INTO DERBY_269 VALUES(1, 'DERBY-269')");
            s.execute("CREATE INDEX I1 ON DERBY_269(c12)");
            s.close();
            break;

        case PH_SOFT_UPGRADE:
        case PH_POST_SOFT_UPGRADE:
            // new update statistics procedure should not be found
            // on soft-upgrade.
            s = createStatement();
            assertStatementError("42Y03", s,
                    "call SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                    "('APP', 'DERBY_269', null)");
            assertStatementError("42Y03", s,
                    "call SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
                    "('APP', 'DERBY_269', 'I1')");
            s.close();
            break;

        case PH_HARD_UPGRADE:
        	//We are at Derby 10.5 release and hence should find the
        	//update statistics procedure
            s = createStatement();
            s.execute("call SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
            		"('APP', 'DERBY_269', null)");
            s.execute("call SYSCS_UTIL.SYSCS_UPDATE_STATISTICS" +
            		"('APP', 'DERBY_269', 'I1')");
            s.close();
            break;
        }
    }

    /**
     * Test that the DETERMINISTIC keyword is not allowed until you
     * hard-upgrade to 10.5.
     *
     */
    public void testDeterminismKeyword() throws SQLException
    {
        String  sqlstate = null;
        
        switch (getPhase())
        {
        case PH_SOFT_UPGRADE:
            sqlstate = SQLSTATE_NEED_UPGRADE;
            break;
            
        case PH_POST_SOFT_UPGRADE:
            sqlstate = BAD_SYNTAX;
            break;

        case PH_HARD_UPGRADE:
            sqlstate = null;
            break;

        default:
            return;
        }
        
        possibleError
            (
             sqlstate,
             "create function f_3570_12()\n" +
             "returns int\n" +
             "language java\n" +
             "parameter style java\n" +
             "deterministic\n" +
             "no sql\n" +
             "external name 'foo.bar.wibble'\n"
             );
        possibleError
            (
             sqlstate,
             "create procedure p_3570_13()\n" +
             "language java\n" +
             "not deterministic\n" +
             "parameter style java\n" +
             "modifies sql data\n" +
             "external name 'foo.bar.wibble'\n"
             );
    }

    /**
     * Test that generation clauses are not allowed until you
     * hard-upgrade to 10.5.
     *
     */
    public void testGenerationClauses() throws SQLException
    {
        String  sqlstate = null;
        
        switch (getPhase())
        {
        case PH_SOFT_UPGRADE:
            sqlstate = SQLSTATE_NEED_UPGRADE;
            break;
            
        case PH_POST_SOFT_UPGRADE:
            sqlstate = BAD_SYNTAX;
            break;

        case PH_HARD_UPGRADE:
            sqlstate = null;
            break;

        default:
            return;
        }
        
        possibleError
            (
             sqlstate,
             "create table t_genCol_2( a int, b int generated always as ( -a ), c int )"
             );
    }

    /**
     * <p>
     * Run a statement. If the sqlstate is not null, then we expect that error.
     * </p>
     */
    private void    possibleError( String sqlstate, String text )
        throws SQLException
    {
        if ( sqlstate != null )
        {
            assertCompileError( sqlstate, text );
        }
        else
        {
            Statement   s = createStatement();
            s.execute( text );
            s.close();
        }
    }

    /**
     * Check that you must be hard-upgraded to 10.5 or later in order to use
     * SQL roles
     * @throws SQLException
     *
     */
    public void testSQLRolesBasic() throws SQLException
    {
        // The standard upgrade database doesn't have sqlAuthorization
        // set, so we can only check if the system tables for roles is
        // present.

        Statement s = createStatement();
        String createRoleText = "create role foo";

        if (getOldMajor() == 10 && getOldMinor() == 4) {
            // In 10.4 the roles commands were present but just gave "not
            // implemented".
            switch (getPhase()) {
            case PH_CREATE:
                assertStatementError("0A000", s, createRoleText );
                break;

            case PH_SOFT_UPGRADE:
                // needs hard upgrade
                assertStatementError("XCL47", s, createRoleText );
                break;

            case PH_POST_SOFT_UPGRADE:
                assertStatementError("0A000", s, createRoleText );
                break;

            case PH_HARD_UPGRADE:
                // not supported because SQL authorization not set
                assertStatementError("42Z60", s, createRoleText );
                break;
            }

        } else {
            switch (getPhase()) {
                case PH_CREATE:
                    assertStatementError("42X01", s, createRoleText );
                    break;

                case PH_SOFT_UPGRADE:
                    // needs hard upgrade
                    assertStatementError("XCL47", s, createRoleText );
                    break;

                case PH_POST_SOFT_UPGRADE:
                    assertStatementError("42X01", s, createRoleText );
                    break;

                case PH_HARD_UPGRADE:
                    // not supported because SQL authorization not set
                    assertStatementError("42Z60", s, createRoleText );
                    break;
            }
        }


        s.close();
    }

    /**
     * Check that when hard-upgraded to 10.5 or later SQL roles can be
     * declared if DB has sqlAuthorization.
     * @throws SQLException
     *
     */
    public void testSQLRoles() throws SQLException
    {
        // Do rudimentary sanity checking: that we can create, meaningfully use
        // and drop roles. If so, we can presume SYS.SYSROLES has been upgraded
        // correctly. If upgrading from 10.4, SYS.SYSROLES are already present,
        // but roles were not activated, cf. test in POST_SOFT_UPGRADE.

        DataSource ds = JDBCDataSource.getDataSourceLogical("ROLES_10_5");
        Connection conn = null;
        Statement s = null;
        boolean supportSqlAuthorization = oldAtLeast(10, 2);

        JDBCDataSource.setBeanProperty(ds, "user", "garfield");
        JDBCDataSource.setBeanProperty(ds, "password", "theCat");

        switch (getPhase()) {
        case PH_CREATE:
            // Create the database if it was not already created.
            JDBCDataSource.setBeanProperty(ds, "createDatabase", "create");
            conn = ds.getConnection();

            // Make the database have std security, and define
            // a database user for the database owner).
            CallableStatement cs = conn.prepareCall(
                "call syscs_util.syscs_set_database_property(?,?)");

            cs.setString(1, "derby.connection.requireAuthentication");
            cs.setString(2, "true");
            cs.execute();

            cs.setString(1, "derby.authentication.provider");
            cs.setString(2, "BUILTIN");
            cs.execute();

            cs.setString(1, "derby.database.sqlAuthorization");
            cs.setString(2, "true");
            cs.execute();

            cs.setString(1, "derby.database.propertiesOnly");
            cs.setString(2, "true");
            cs.execute();

            cs.setString(1, "derby.user.garfield");
            cs.setString(2, "theCat");
            cs.execute();

            cs.setString(1, "derby.user.jon");
            cs.setString(2, "theOwner");
            cs.execute();

            conn.close();

            JDBCDataSource.shutdownDatabase(ds);
            break;

        case PH_SOFT_UPGRADE:
            /* We can't always do soft upgrade, because when
             * sqlAuthorization is set and we are coming from a
             * pre-10.2 database, connecting will fail with a message
             * to hard upgrade before setting sqlAuthorization, so we
             * skip this step.
             */
            if (oldAtLeast(10,2)) {
                // needs hard upgrade
                conn = ds.getConnection();
                s = conn.createStatement();

                assertStatementError("XCL47", s, "create role foo" );
                conn.close();

                JDBCDataSource.shutdownDatabase(ds);
            }
            break;

        case PH_POST_SOFT_UPGRADE:
            conn = ds.getConnection();
            s = conn.createStatement();

            if (getOldMajor() == 10 && getOldMinor() == 4) {
                // not implemented
                assertStatementError("0A000", s, "create role foo" );
            } else {
                // syntax error
                assertStatementError("42X01", s, "create role foo" );
            }

            conn.close();

            JDBCDataSource.shutdownDatabase(ds);
            break;

        case PH_HARD_UPGRADE:
            JDBCDataSource.setBeanProperty(
                ds, "connectionAttributes", "upgrade=true");
            conn = ds.getConnection();
            s = conn.createStatement();

            // Roles should work; basic sanity test

            // garfield is dbo
            s.execute("create role foo");
            s.execute("create table cats(specie varchar(30))");
            s.execute("insert into cats " +
                      "values 'lynx', 'tiger', 'persian', 'garfield'");
            s.execute("grant select on cats to foo");
            s.execute("grant foo to jon");

            // Connect as jon (not owner) and employ jon's newfound role
            JDBCDataSource.clearStringBeanProperty(ds, "connectionAttributes");
            JDBCDataSource.setBeanProperty(ds, "user", "jon");
            JDBCDataSource.setBeanProperty(ds, "password", "theOwner");
            Connection jon = ds.getConnection();

            Statement jonStm = jon.createStatement();
            // Still, no privilege available for poor jon..
            assertStatementError
                ("42502", jonStm, "select * from garfield.cats");

            jonStm.execute("set role foo");
            // Now, though:
            jonStm.execute("select * from garfield.cats");
            jonStm.close();
            jon.close();

            s.execute("drop table cats");
            s.execute("drop role foo");
            conn.close();

            println("Roles work after hard upgrade");

            // Owner garfield shuts down
            JDBCDataSource.setBeanProperty(ds, "user", "garfield");
            JDBCDataSource.setBeanProperty(ds, "password", "theCat");
            JDBCDataSource.shutdownDatabase(ds);
            break;
        }
    }
}
