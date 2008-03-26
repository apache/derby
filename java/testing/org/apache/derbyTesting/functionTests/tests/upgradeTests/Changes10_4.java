/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.Changes10_4

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

import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.SupportFilesSetup;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.CallableStatement;
import java.sql.Types;

import javax.sql.DataSource;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Upgrade test cases for 10.4.
 * If the old version is 10.4 or later then these tests
 * will not be run.
 * <BR>
    10.4 Upgrade issues

    <UL>
    <LI> testMetaDataQueryRunInSYScompilationSchema - DERBY-2946 
    Make sure that metadata queries get run with SYS schema as the current 
    compilation schema rather than a user schema as the current compilation 
    schema. This is because if the user is inside a user schema in a collated 
    database and if the meta data query gets run inside the user schema, then 
    we will run into collation mismatch errors for a subclause like following 
    in the WHERE clause.
    P.SELECTPRIV = 'Y' 
    The reason for error is that the left hand side of the = operation will 
    have collation type of UCS_BASIC because that column belongs to a table 
    from system schema. But the collation type of the right hand will be 
    territory based if the current compilation schema is user schema. But if 
    the current compilation schema is set to SYS schema, then right hand side 
    will also have collation of UCS_BASIC and hence there won't be any 
    collation mismatch. 
    Background info : character string constants pick up the collation of the
    current compilation schema. 
    </UL>

 */
public class Changes10_4 extends UpgradeChange {

    public Changes10_4(String name) {
        super(name);
    }
    
    /**
     * Return the suite of tests to test the changes made in 10.4.
     * @param phase an integer that indicates the current phase in
     *              the upgrade test.
     * @return the test suite created.
     */   
    public static Test suite(int phase) {
        TestSuite suite = new TestSuite("Upgrade test for 10.4");
        
        suite.addTestSuite(Changes10_4.class);
        return new SupportFilesSetup((Test) suite);
    }
    
    /**
     * Check that even though we have set schema to a user schema, the 
     * metadata queries get run with compilation schema as SYS.
     * DERBY-2946
     * Test added for 10.4.
     * @throws SQLException 
     *
     */
    public void testMetaDataQueryRunInSYScompilationSchema() throws SQLException
    {
    	//This test is for databases with territory based collation. That
    	//feature was added in 10.3 codeline and hence there is no point in
    	//doing any testing with pre-10.3 databases.
        if (!oldAtLeast(10, 3))
        	return;

        DataSource ds = JDBCDataSource.getDataSourceLogical("COLLATED_DB_10_3");
        
        switch (getPhase())
        {
        case PH_CREATE:
            // create the database if it was not already created. Note the
        	// JDBC url attributes.
            JDBCDataSource.setBeanProperty(
                    ds, "ConnectionAttributes", "create=true;territory=no;collation=TERRITORY_BASED");
            ds.getConnection().close();
            break;
            
        case PH_SOFT_UPGRADE:
        case PH_POST_SOFT_UPGRADE:
        case PH_HARD_UPGRADE:
            Connection con = ds.getConnection();
        	//First make the current schema as a user schema. And then run a 
        	//metadata query to make sure that it runs fine. If it does (which
        	//is the expected behavior), then it will mean that the metadata
        	//query is getting run with SYS as the compilation schema rather
        	//than the current schema which is APP.
            Statement s = con.createStatement();
            s.execute("SET SCHEMA APP");

            DatabaseMetaData dmd = con.getMetaData();
            ResultSet rs = dmd.getTables(null,"APP",null,null);
            JDBC.assertDrainResults(rs);
            s.close();
            break;
        }
    }


    /**
     * Check that you must be hard-upgraded to 10.4 or later in order to declare
     * table functions.
     * @throws SQLException 
     *
     */
    public void testTableFunctionDeclaration() throws SQLException
    {
        Statement       s = createStatement();
        String          createTableFunctionText =
            "create function svnLogReader( logFileName varchar( 32672 ) )\n" +
            "returns TABLE\n" +
            "  (\n" +
            "     XID varchar( 15 ),\n" +
            "     committer    varchar( 20 ),\n" +
            "     commit_time  timestamp,\n" +
            "     line_count   varchar( 10 ),\n" +
            "     description  varchar( 32672 )\n" +
            "  )\n" +
            "language java\n" +
            "parameter style DERBY_JDBC_RESULT_SET\n" +
            "no sql\n" +
            "external name 'org.apache.derbyDemo.vtis.example.SubversionLogVTI.subversionLogVTI'\n"
            ;

        switch (getPhase())
        {
        case PH_CREATE:
            assertStatementError("42X01", s, createTableFunctionText );
            break;
            
        case PH_SOFT_UPGRADE:
            assertStatementError("XCL47", s, createTableFunctionText );
            break;
            
        case PH_POST_SOFT_UPGRADE:
            assertStatementError("42X01", s, createTableFunctionText );
            break;
            
        case PH_HARD_UPGRADE:
            s.execute( createTableFunctionText );
            break;
        }

        s.close();
    }
    
    /**
     * Test that routine parameters and return types are
     * handled correctly with 10.4 creating a procedure
     * in soft-upgrade. 10.4 simplified the stored
     * format of the types by ensuring the catalog
     * type was written. See DERBY-2917 for details.
     * 
     * @throws SQLException 
     *
     */
    public void testRoutineParameters() throws SQLException
    {
        

        switch (getPhase())
        {
        case PH_CREATE:
          break;
            
        case PH_SOFT_UPGRADE:
            Statement s = createStatement();
            s.execute("CREATE FUNCTION TYPES_10_4" +
                    "(A INTEGER) RETURNS CHAR(10) " +
                    "LANGUAGE JAVA " +
                    "PARAMETER STYLE JAVA " +
                    "NO SQL " +
                    "EXTERNAL NAME 'java.lang.Integer.toHexString'");
           // fall through to test it
            
        case PH_HARD_UPGRADE:
        case PH_POST_SOFT_UPGRADE:
            PreparedStatement ps = prepareStatement(
                    "VALUES TYPES_10_4(?)");
            ps.setInt(1, 48879);
            // Don't use the single value check method here
            // because we want to check the returned value
            // was converted to its correct type of CHAR(10)
            // (so no trimming of values)
            JDBC.assertFullResultSet(ps.executeQuery(),
                    new Object[][] {{"beef      "}}, false);
            break;
        }
    }

    /**
     * Check that you must be hard-upgraded to 10.4 or later in order to use
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

        switch (getPhase())
            {
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

        s.close();
    }

    /**
     * Check that when hard-upgraded to 10.4 or later SQL roles can be
     * declared if DB has sqlAuthorization.
     * @throws SQLException
     *
     */
    public void testSQLRoles() throws SQLException
    {
        // Do rudimentary sanity checking: that we can create and drop roles
        // when we are database owner. If so, we can presume SYS.SYSROLES
        // has been upgraded correctly.

        DataSource ds = JDBCDataSource.getDataSourceLogical("ROLES_10_4");
        String createRoleText = "create role foo";
        String dropRoleText   = "drop role foo";
        Connection conn = null;
        Statement s = null;
        boolean supportSqlAuthorization = oldAtLeast(10, 2);

        JDBCDataSource.setBeanProperty(ds, "user", "garfield");
        JDBCDataSource.setBeanProperty(ds, "password", "theCat");

        switch (getPhase()) {
        case PH_CREATE:
            // create the database if it was not already created.
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

                assertStatementError("XCL47", s, createRoleText );
                conn.close();

                JDBCDataSource.shutdownDatabase(ds);
            }
            break;

        case PH_POST_SOFT_UPGRADE:
            conn = ds.getConnection();
            s = conn.createStatement();

            // syntax error
            assertStatementError("42X01", s, createRoleText );
            conn.close();

            JDBCDataSource.shutdownDatabase(ds);
            break;

        case PH_HARD_UPGRADE:
            JDBCDataSource.setBeanProperty(
                ds, "connectionAttributes", "upgrade=true");
            conn = ds.getConnection();
            s = conn.createStatement();

            // should work now
            try {
                s.execute(createRoleText);
            } catch (SQLException e) {
                fail("can't create role on hard upgrade");
            }

            s.execute(dropRoleText);
            conn.close();

            JDBCDataSource.clearStringBeanProperty(ds, "connectionAttributes");
            JDBCDataSource.shutdownDatabase(ds);
            break;
        }
    }
    

    /**
     * Tests whether or not indexes are exibiting their expected behaviour.
     *
     * @param s         Statement 
     * @param tableName table name to be used for testing
     * @param val       value to be used for insertion
     * @param nulls     if nulls are allowed 
     * @param duplicate if duplicates are allowed
     */
    private void testIndexes (Statement s, String tableName, int val,
            boolean nulls, boolean duplicate) throws Exception {

        //clean table first
        s.executeUpdate("delete from " + tableName);
        s.executeUpdate("insert into " + tableName + " values (" + val +")");

        // verify insert of input value results in expected duplicate behavior
        try {
            s.executeUpdate(
                "insert into " + tableName + " values (" + val +")");

            if (!duplicate)
                fail("no duplicates allowed");
        }
        catch (SQLException e) {
            //throw 23505
            if (!duplicate) {
                assertSQLState("duplicate record", "23505", e);
            }
            else {
                throw e;
            }
        }

        // verify insert of a null value results in expected behavior
        try {
            s.executeUpdate("insert into " + tableName + " values (null)");
            if (!nulls)
                fail("null in a not null column");
        }
        catch (SQLException e) {
            if (!nulls) { //23502
                assertSQLState("null in not null column", "23502", e);
            }
            else {
                throw e;
            }
        }

        // if nulls are allowed, verify expected behavior of 2nd null insert.
        if (!nulls)
            return;
        try {
            s.executeUpdate("insert into " + tableName + " values (null)");
            if (!duplicate)
                fail("no duplicates allowed");
        }
        catch (SQLException e) {
        if (!duplicate) {
                assertSQLState("duplicate record", "23505", e);
            }
            else {
                throw e;
            }
        }
    }
    
    /**
     * Creates tables to test indexes during and after soft and hard 
     * upgrades
     * @param s Statement
     * @param prefix prefix for table names
     */
    private void createTablesForIndexTesting (Statement s, String prefix) 
                                    throws Exception {

        // unique index on non-null column
        s.executeUpdate("create table  " + prefix + "_indextest1" +
                "(col1 integer not null)"); 
        s.executeUpdate("create unique index "+ prefix
                + "_uinn on " + prefix + "_indextest1 (col1)");

        // unique index on nullable column
        s.executeUpdate("create table  " + prefix + "_indextest2" +
                "(col1 integer)"); 
        s.executeUpdate("create unique index "+ prefix
                + "_uin on " + prefix + "_indextest2 (col1)");

        // non-unique index on non-nullable column
        s.executeUpdate("create table  " + prefix + "_indextest3" +
                "(col1 integer not null)");     
        s.executeUpdate("create index "+ prefix
                + "_nuinn on " + prefix + "_indextest3 (col1)");

        // non-unique index on nullable column
        s.executeUpdate("create table  " + prefix + "_indextest4" +
                "(col1 integer)"); 
        s.executeUpdate("create index "+ prefix
                + "_nuin on " + prefix + "_indextest4 (col1)");

        // behavior of unique constraint on non-nullable columns 
        // should not change, should match behavior of unique index
        // on non-null column.
        s.executeUpdate("create table  " + prefix + "_indextest5" +
                "(col1 integer not null)"); 
        //try creating index without seting column as not null 
        s.executeUpdate("alter table " + prefix + "_indextest5 " +
                "add constraint " + 
                prefix + "_cons1 unique(col1)");
    }
    /**
     * Test index created before upgrades to insure their behaviour 
     * remains same after soft and hard upgrades.  This is an index
     * test and does not apply to constraint behavior.
     */
    public void testIndexes () throws Exception {
        Statement s = createStatement();
        switch (getPhase()) {
            case PH_CREATE:
                //create tables for testing in soft and hard upgrade
                createTablesForIndexTesting (s, "ph_create");
                //test if indexes are created properly
                testIndexes(s, "ph_create_indextest1", 1, false, false);
                testIndexes(s, "ph_create_indextest2", 1, true, false);
                testIndexes(s, "ph_create_indextest3", 1, false, true);
                testIndexes(s, "ph_create_indextest4", 1, true, true);
                testIndexes(s, "ph_create_indextest5", 1, false, false);
                break;
                
            case PH_SOFT_UPGRADE:
                //test old tables
                testIndexes(s, "ph_create_indextest1", 2, false, false);
                testIndexes(s, "ph_create_indextest2", 2, true, false);
                testIndexes(s, "ph_create_indextest3", 2, false, true);
                testIndexes(s, "ph_create_indextest4", 2, true, true);
                testIndexes(s, "ph_create_indextest5", 2, false, false);
                //create one more set of tables
                createTablesForIndexTesting (s, "ph__soft_upg");
                //test newly created tables
                testIndexes(s, "ph__soft_upg_indextest1", 2, false, false);
                testIndexes(s, "ph__soft_upg_indextest2", 2, true, false);
                testIndexes(s, "ph__soft_upg_indextest3", 2, false, true);
                testIndexes(s, "ph__soft_upg_indextest4", 2, true, true);
                testIndexes(s, "ph__soft_upg_indextest5", 2, false, false);
                break;
                
            case PH_POST_SOFT_UPGRADE:
                //test tables created so far
                testIndexes(s, "ph_create_indextest1", 3, false, false);
                testIndexes(s, "ph_create_indextest2", 3, true, false);
                testIndexes(s, "ph_create_indextest3", 3, false, true);
                testIndexes(s, "ph_create_indextest4", 3, true, true);
                testIndexes(s, "ph_create_indextest5", 3, false, false);

                
                testIndexes(s, "ph__soft_upg_indextest1", 3, false, false);
                testIndexes(s, "ph__soft_upg_indextest2", 3, true, false);
                testIndexes(s, "ph__soft_upg_indextest3", 3, false, true);
                testIndexes(s, "ph__soft_upg_indextest4", 3, true, true);
                testIndexes(s, "ph__soft_upg_indextest5", 3, false, false);
                break;
                
            case PH_HARD_UPGRADE:
                //test tables created so far
                testIndexes(s, "ph_create_indextest1", 4, false, false);
                testIndexes(s, "ph_create_indextest2", 4, true, false);
                testIndexes(s, "ph_create_indextest3", 4, false, true);
                testIndexes(s, "ph_create_indextest4", 4, true, true);
                testIndexes(s, "ph_create_indextest5", 4, false, false);

                
                testIndexes(s, "ph__soft_upg_indextest1", 4, false, false);
                testIndexes(s, "ph__soft_upg_indextest2", 4, true, false);
                testIndexes(s, "ph__soft_upg_indextest3", 4, false, true);
                testIndexes(s, "ph__soft_upg_indextest4", 4, true, true);
                testIndexes(s, "ph__soft_upg_indextest5", 5, false, false);
                
                //create final set of tables
                createTablesForIndexTesting (s, "ph_upgrade");
                //test them
                testIndexes(s, "ph_upgrade_indextest1", 4, false, false);
                testIndexes(s, "ph_upgrade_indextest2", 4, true, false);
                testIndexes(s, "ph_upgrade_indextest3", 4, false, true);
                testIndexes(s, "ph_upgrade_indextest4", 4, true, true);
                testIndexes(s, "ph_upgrade_indextest5", 4, false, false);
                break;
        }
        s.close();
    }
    /**
     * Tests Unique constraint in soft and hard upgrade enviornment. 
     * Under soft upgrade environment creation of unique constrant 
     * over nullable columns and setting columns from unique constraint
     * to null should fail. Also the constraint created during soft 
     * upgrade run should work fine when running under previous version.
     * @throws SQLException at any unexpected failure.
     */
    public void testUniqueConstraint () throws SQLException {
        Statement s = createStatement();
        switch (getPhase()) {
            case PH_CREATE:
                //create a table with unique constraint
                s.executeUpdate(
                    "create table constraintest1 (i integer not null" +
                    ", j integer, k integer, constraint ucon unique (i))");
                //and one without constraint
                s.executeUpdate("create table constraintest2 (i integer" +
                        ", j integer, k integer)");
                s.executeUpdate("create table constraintest3 (" +
                        "i integer not null, j integer, k integer)");
                break;
            case PH_SOFT_UPGRADE:
                //try setting i to nullable 42Z20
                assertStatementError("42Z20", s, "alter table constraintest1 " +
                        "alter column i null");
                //try creating index without seting column as not null 
                assertStatementError("42831", s, "alter table constraintest2 " +
                        "add constraint ucon1 unique(i, j)");
                //this should work fine
                s.execute ("alter table constraintest3 add " +
                        "constraint ucon2 unique(i)");
                break;
            case PH_POST_SOFT_UPGRADE:
                //try violating ucon2
                s.executeUpdate ("insert into constraintest3 (i) values (1)");
                //duplicate key violation 23505
                assertStatementError ("23505", s, 
                            "insert into constraintest3 (i) values (1)");
                break;
            case PH_HARD_UPGRADE:
                //test unique constraint
                s.executeUpdate ("insert into constraintest1 (i) values (2)");
                //duplicate key violation 23505
                assertStatementError ("23505", s, 
                            "insert into constraintest1 (i) values (2)");
                //should able to drop nullablity
                s.executeUpdate("alter table constraintest1 " +
                        "alter column i null");
                //try creating index without seting column as not null
                s.executeUpdate("alter table constraintest2 " +
                        "add constraint ucon1 unique(i, j)");
                //test null values in unique constraint
                s.executeUpdate ("insert into constraintest1 (j) values (1)");
                s.executeUpdate ("insert into constraintest1 (j) values (1)");
                //test null values in new index
                s.executeUpdate ("insert into constraintest2 (j) values (1)");
                s.executeUpdate ("insert into constraintest2 (j) values (1)");
                break;
        }
        s.close();
    }
}
