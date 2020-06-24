package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test for CURRENT SCHEMA and optional DB2 compatible SET SCHEMA statement
 * test SET SCHEMA syntax variations syntax is:
 * <p>
 * <pre>
 *    SET [CURRENT] SCHEMA [=] (<identifier> | USER | ? | '<string>')
 *    SET CURRENT SQLID [=] (<identifier> | USER | ? | '<string>')
 * </pre>
 */
public final class CurrentSchemaTest extends BaseJDBCTestCase {

    /**
     * Public constructor required for running test as standalone JUnit.
     * @param name test name
     */
    public CurrentSchemaTest(String name)
    {
        super(name);
    }

    public static Test suite()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("CurrentSchemaTest");
        suite.addTest(TestConfiguration.defaultSuite(CurrentSchemaTest.class));
        return suite;
    }

    public void testCurrentSchema() throws Exception
    {
        ResultSet rs;
        ResultSetMetaData rsmd;

        PreparedStatement pSt;
        final Statement st = createStatement();

        String [][] expRS;
        String [] expColNames;

        assertCurrentSchema(st, "APP");

        st.executeUpdate("set schema sys");
        assertCurrentSchema(st, "SYS");

        assertStatementError("X0Y68", st, "create schema app");

        st.executeUpdate("set current schema app");
        assertCurrentSchema(st, "APP");

        st.executeUpdate("set schema = sys");
        assertCurrentSchema(st, "SYS");

        st.executeUpdate("set current schema = app");
        assertCurrentSchema(st, "APP");

        st.executeUpdate("set schema sys");

        //
        // user should use default schema if no user set
        //
        st.executeUpdate("set schema user");
        assertCurrentSchema(st, "APP");

        //
        // see what user does when there is a user
        //
        st.executeUpdate("create schema judy");
        Connection judy = openUserConnection("judy");
        Statement jst = judy.createStatement();

        jst.executeUpdate("set schema app");
        assertCurrentSchema(jst, "APP");

        jst.executeUpdate("set schema user");
        assertCurrentSchema(jst, "JUDY");

        judy.close();

        //
        // check for default
        //
        assertCurrentSchema(st, "APP");

        //
        // Check that current sqlid works as a synonym
        //
        rs = st.executeQuery("values current sqlid");
        expRS = new String [][]{{"APP"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        //
        // Check that sqlid still works as an identifer
        //
        st.executeUpdate("create table sqlid(sqlid int)");
        st.executeUpdate("drop table sqlid");

        //
        // Check that set current sqlid works
        //
        st.executeUpdate("set current sqlid judy");
        assertCurrentSchema(st, "JUDY");

        //
        // Check that set sqlid doesn't work (not DB2 compatible) - should get
        // error
        assertStatementError("42X01", st, "set sqlid judy");

        //
        // Change schema and make sure that the current schema is correct
        //
        st.executeUpdate("set schema sys");
        assertCurrentSchema(st, "SYS");

        st.executeUpdate("set schema app");

        //
        // Try using ? outside of a prepared statement
        //
        assertStatementError("07000", st, "set schema ?");

        //
        // Use set schema in a prepared statement
        //
        setAutoCommit(false);

        pSt = prepareStatement("set schema ?");

        //
        // Should get error with no parameters
        //
        assertStatementError("07000", pSt);

        //
        // Should get error if null is used
        //
        st.executeUpdate("create table t1(name varchar(128))");
        st.executeUpdate("insert into t1 values(null)");
        rs = st.executeQuery("select name from t1");
        rs.next();
        pSt.setObject(1, rs.getObject(1));
        assertStatementError("42815", pSt);

        //
        // Should get error if schema doesn't exist
        //
        rs = st.executeQuery("values('notthere')");
        rs.next();
        pSt.setObject(1, rs.getObject(1));
        assertStatementError("42Y07", pSt);

        //
        // Should error with empty string
        //
        rs = st.executeQuery("values('')");
        rs.next();
        pSt.setObject(1, rs.getObject(1));
        assertStatementError("42Y07", pSt);

        //
        // Should get error if wrong case used
        //
        rs = st.executeQuery("values('sys')");
        rs.next();
        pSt.setObject(1, rs.getObject(1));
        assertStatementError("42Y07", pSt);

        //
        // Should get error if too many parameters
        //
        rs = st.executeQuery("values('sys','app')");
        rs.next();
        rsmd = rs.getMetaData();

        try {
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                pSt.setObject(i, rs.getObject(i));
            }
        } catch (SQLException e) {
            if (usingDerbyNetClient()) {
                assertSQLState("XCL14", e);
            } else {
                assertSQLState("XCL13", e);
            }
        }

        //
        // USER should return an error as it is interpreted as a
        // string constant not an identifier
        //
        rs = st.executeQuery("values('USER')");
        rs.next();
        pSt.setObject(1, rs.getObject(1));
        assertStatementError("42Y07", pSt);

        //
        // Try positive test
        //
        rs = st.executeQuery("values('SYS')");
        rs.next();
        pSt.setObject(1, rs.getObject(1));
        assertUpdateCount(pSt, 0);
        assertCurrentSchema(st, "SYS");

        rollback();
        setAutoCommit(true);

        //
        // Try current schema in a number of statements types
        //
        st.executeUpdate("set schema app");
        st.executeUpdate("create table t1 ( a varchar(128))");

        //
        // insert
        //
        st.executeUpdate("insert into t1 values (current schema)");
        rs = st.executeQuery("select * from t1");
        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]{{"APP"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("set schema judy");
        st.executeUpdate("insert into app.t1 values (current schema)");
        rs = st.executeQuery("select * from app.t1");
        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);
        expRS = new String [][]
        {
            {"APP"},
            {"JUDY"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        //
        // delete where clause
        //
        assertUpdateCount(st, 1,"delete from app.t1 where a = current schema");
        rs = st.executeQuery("select * from app.t1");
        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"APP"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("set current schema app");

        //
        // Target list
        //
        rs = st.executeQuery("select current schema from t1");

        expColNames = new String [] {"1"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"APP"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        //
        // where clause
        //
        rs = st.executeQuery("select * from t1 where a = current schema");

        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]{{"APP"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        //
        // update statement
        //
        assertUpdateCount(st, 1, "delete from t1");
        st.executeUpdate("insert into t1 values ('test')");
        rs = st.executeQuery("select * from t1");

        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]{{"test"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        assertUpdateCount(st, 1, "update t1 set a = current schema");

        rs = st.executeQuery("select * from t1");
        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]{{"APP"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("set schema judy");
        assertUpdateCount(st, 1, "update app.t1 set a = current schema");

        rs = st.executeQuery("select * from app.t1");

        expColNames = new String [] {"A"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]{{"JUDY"}};
        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("set schema app");
        st.executeUpdate("drop table t1");

        //
        // Column default
        //
        st.executeUpdate("set schema APP");

        st.executeUpdate(
            " create table t1 ( a int, b varchar(128) default "
            + "current schema)");

        st.executeUpdate("insert into t1 (a) values (1)");
        st.executeUpdate("set schema SYS");
        st.executeUpdate("insert into app.t1 (a) values (1)");
        st.executeUpdate("set schema judy");
        st.executeUpdate("insert into app.t1 (a) values (1)");
        st.executeUpdate("set schema APP");

        rs = st.executeQuery("select * from t1");

        expColNames = new String [] {"A", "B"};
        JDBC.assertColumnNames(rs, expColNames);

        expRS = new String [][]
        {
            {"1", "APP"},
            {"1", "SYS"},
            {"1", "JUDY"}
        };

        JDBC.assertFullResultSet(rs, expRS, true);

        st.executeUpdate("drop table t1");

        //
        // Check constraint - this should fail
        //
        assertStatementError("42Y39", st,
            "create table t1 ( a varchar(128), check (a = "
            + "current schema))");

        assertStatementError("42Y39", st,
            " create table t1 ( a varchar(128), check (a = "
            + "current sqlid))");

        //
        // Try mix case
        //
        st.executeUpdate("create schema \"MiXCase\"");
        st.executeUpdate("set schema \"MiXCase\"");
        assertCurrentSchema(st, "MiXCase");

        st.executeUpdate("set schema app");
        assertCurrentSchema(st, "APP");

        st.executeUpdate("set schema 'MiXCase'");
        assertCurrentSchema(st, "MiXCase");

        //
        // Following should get error - schema not found
        //
        assertStatementError("42Y07", st, "set schema 'MIXCASE'");
        assertStatementError("42Y07", st, "set schema mixcase");

        //
        // Try long schema names (maximum schema identifier length
        // has been changed to 30 as part of DB2 compatibility work)
        //
        st.executeUpdate("create schema t23456789012345678901234567890");
        assertCurrentSchema(st, "MiXCase");

        st.executeUpdate("set schema app");
        assertCurrentSchema(st, "APP");

        st.executeUpdate("set schema t23456789012345678901234567890");
        assertCurrentSchema(st, "T23456789012345678901234567890");

        st.executeUpdate(" set schema app");
        assertCurrentSchema(st, "APP");

        st.executeUpdate("set schema 'T23456789012345678901234567890'");
        assertCurrentSchema(st, "T23456789012345678901234567890");

        st.executeUpdate("set schema app");
        assertCurrentSchema(st, "APP");

        setAutoCommit(false);

        pSt = prepareStatement("set schema ?");
        rs = st.executeQuery("values('T23456789012345678901234567890')");
        rs.next();
        pSt.setObject(1, rs.getObject(1));
        assertUpdateCount(pSt, 0);

        assertCurrentSchema(st, "T23456789012345678901234567890");

        //
        // The following should fail - 129 length
        //
        assertStatementError("42622", st,
            "create schema "
            + "TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT"
            + "TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT"
            + "TTTTTTTTTTTTTTTTTTTTTTTTT");

        assertStatementError("42622", st,
            " set schema "
            + "TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT"
            + "TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT"
            + "TTTTTTTTTTTTTTTTTTTTTTTTT");

        assertStatementError("42622", st,
            " set schema "
            + "'TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT"
            + "TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT"
            + "TTTTTTTTTTTTTTTTTTTTTTTTTT'");

        rs = st.executeQuery(
                "values('TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT"
                + "TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT"
                + "TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT')");
        rs.next();
        pSt.setObject(1, rs.getObject(1));
        assertStatementError("42815", pSt);

        rollback();
        setAutoCommit(true);

        //
        // Clean up
        //
        st.executeUpdate("drop schema judy restrict");

        assertStatementError("42622", st,
            " drop schema "
            + "TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT"
            + "TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT"
            + "TTTTTTTTTTTTTTTTTTTTTTTTT restrict");

        rollback();
        st.close();
    }

    private void assertCurrentSchema(Statement st, String schema)
        throws SQLException {

        JDBC.assertFullResultSet(
            st.executeQuery("values current schema"),
            new String [][]{{schema}},
            true);
    }
}
