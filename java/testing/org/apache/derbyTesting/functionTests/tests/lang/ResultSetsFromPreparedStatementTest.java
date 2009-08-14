/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.ResultSetsFromPreparedStatementTest

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

import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import java.sql.SQLException;

import java.io.PrintStream;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.RuntimeStatisticsParser;
import org.apache.derbyTesting.junit.SQLUtilities;

// TODO:
// - Add parameters to all PreparedStatements that support it
// - special name for the ps being tested?

/**
 * Tests repeated instantiation of internal ResultSet types from
 * PreparedStatements. This test will be a good sanity check when
 * experimenting with re-use of internal ResultSet objects.
 */
public class ResultSetsFromPreparedStatementTest extends BaseJDBCTestCase
{
    // ---------------------------------------------------------------
    // Data model (taken from lang/refActions1.sql)
    public static final Integer i1 = new Integer(1);
    public static final Integer i2 = new Integer(2);
    public static final Integer i3 = new Integer(3);
    public static final Integer i4 = new Integer(4);
    public static final Integer i5 = new Integer(5);
    public static final Integer i6 = new Integer(6);
    public static final Integer i7 = new Integer(7);
    public static final Integer i8 = new Integer(8);
    public static final Integer i9 = new Integer(9);
    public static final Integer i10 = new Integer(10);
    public static final Integer i11 = new Integer(11);
    public static final Integer i12 = new Integer(12);
    public static final Integer i13 = new Integer(13);
    public static final Integer i14 = new Integer(14);
    public static final Integer i15 = new Integer(15);
    public static final Integer i16 = new Integer(16);
    public static final Integer i17 = new Integer(17);
    public static final Integer i18 = new Integer(18);
    public static final Integer i19 = new Integer(19);
    public static final Integer i20 = new Integer(20);

    public static final String k55 = "K55";
    public static final String k52 = "K52";
    public static final String k51 = "K51";

    public static final String ashok  = "ASHOK     ";
    public static final String john   = "JOHN      ";
    public static final String robin  = "ROBIN     ";
    public static final String joe1   = "JOE1      ";
    public static final String joe2   = "JOE2      ";
    public static final String hamid  = "HAMID     ";
    public static final String truong = "TRUONG    ";
    public static final String larry1 = "LARRY1    ";
    public static final String larry2 = "LARRY2    ";
    public static final String bobbie = "BOBBIE    ";
    public static final String roger  = "ROGER     ";
    public static final String jim    = "JIM       ";
    public static final String dan    = "DAN       ";
    public static final String sam1   = "SAM1      ";
    public static final String sam2   = "SAM2      ";
    public static final String guy    = "GUY       ";
    public static final String don    = "DON       ";
    public static final String monica = "MONICA    ";
    public static final String lily1  = "LILY1     ";
    public static final String lily2  = "LILY2     ";

    // dept rows
    public static final Object[] db_dept =  { i1, k55, "DB        " };
    public static final Object[] ofc_dept = { i2, k52, "OFC       " };
    public static final Object[] cs_dept =  { i3, k51, "CS        " };

    // dept table
    public static final Object[][] dept = { db_dept, ofc_dept, cs_dept };

    // emp rows
    public static final Object[] ashok_emp = { i1, ashok, null, k51 };
    public static final Object[] john_emp = { i2, john, ashok, k51 };
    public static final Object[] robin_emp = { i3, robin, ashok, k51};
    public static final Object[] joe1_emp = { i4, joe1, ashok, k51};
    public static final Object[] joe2_emp = { i5, joe2, ashok, k51};
    public static final Object[] hamid_emp = { i6, hamid, john, k55 };
    public static final Object[] truong_emp = { i7, truong, hamid, k55 };
    public static final Object[] larry1_emp = { i8, larry1, hamid, k55 };
    public static final Object[] larry2_emp = { i9, larry2, hamid, k55 };
    public static final Object[] bobbie_emp = { i10, bobbie, hamid, k55 };
    public static final Object[] roger_emp = { i11, roger, robin, k52 };
    public static final Object[] jim_emp = { i12, jim, roger, k52 };
    public static final Object[] dan_emp = { i13, dan, roger, k52 };
    public static final Object[] sam1_emp = { i14, sam1, roger, k52 };
    public static final Object[] sam2_emp = { i15, sam2, roger, k52 };
    public static final Object[] guy_emp = { i16, guy, john, k55 };
    public static final Object[] don_emp = { i17, don, guy, k55 };
    public static final Object[] monica_emp = { i18, monica, guy, k55 };
    public static final Object[] lily1_emp = { i19, lily1, guy, k55 };
    public static final Object[] lily2_emp = { i20, lily2, guy, k55 };

    // emp table
    public static final Object[][] emp = {
        ashok_emp, john_emp, robin_emp, joe1_emp, joe2_emp, hamid_emp,
        truong_emp, larry1_emp, larry2_emp, bobbie_emp, roger_emp, jim_emp,
        dan_emp, sam1_emp, sam2_emp, guy_emp, don_emp, monica_emp, lily1_emp,
        lily2_emp
    };

    private static final String[] mgrs = {
        ashok, john, hamid, robin, roger, guy
    };

    private static final String[] non_mgrs = {
        joe1, joe2, truong, larry1, larry2, bobbie, jim, dan,
        sam1, sam2, don, monica, lily1, lily2
    };

    // ---------------------------------------------------------------
    // Utility methods (and constants). Some are public to make them
    // accessible to the decorator
    public static final String SCHEMA = "db2test";
    public static final String CT = "create table ";
    public static final String DS =
        " (c0 int, dno char(3) not null primary key, dname char(10))";
    public static final String ES =
        " (c0 int, name char(10) not null primary key, mgrname char(10)";
    public static final String DNO     = " dno char(3)";

    public static String ref(String table) {
        return " references "+table;
    }

    public static final String CAS     = " on delete cascade";
    public static final String SETN    = " on delete set null";

    /** Secondary connection. Used if something needs to be executed in a
     * separate transaction. */
    private Connection c2;
    private Connection c3;


    private static final long DERBY_DEFAULT_TIMEOUT = 60;

    /**
     * Creates a String containing an insert statement for the
     * specified table containing the specified number of '?'
     * characters.
     * @param table the name of the table
     * @param params the number of params to insert
     * @return an insert statement (as String)
     */
    public static String insertInto(String table, int params) {
        StringBuffer tmp = new StringBuffer("insert into "+table+" values ( ?");
        while (--params > 0) {
            tmp.append(", ?");
        }
        tmp.append(")");
        return tmp.toString();
    }

    private static String insertFrom(String dst, String src) {
        return ("insert into "+dst+" select * from "+src);
    }

    /**
     * Create table for this fixture.  The table is filled with data
     * from the specified source table.
     * @param name the table to create
     * @param signature the signature (columns) of the new table
     * @param src the name of the source table (used to fill the new table)
     */
    private void createTestTable(String name, String signature,
                                 String src) throws SQLException {
        Statement s = createStatement();
        s.execute(CT+name+signature);
        s.execute(insertFrom(name, src));
        s.close();
    }

    /**
     * Apply a PreparedStatement repeatedly with the set of parameter
     * vectors. (Any null params are assumed to be of type CHAR).
     * @param action the ps to execute
     * @param table an array of parameter vectors to use for each
     * execution of the PreparedStatement
     */
    public static void apply(PreparedStatement action, Object[][] table)
        throws SQLException {
        for (int row = 0; row < table.length; ++row) {
            for (int col = 0; col < table[row].length; ++col) {
                Object obj = table[row][col];
                if (obj == null) {
                    action.setNull(col+1,java.sql.Types.CHAR);
                    continue;
                }
                action.setObject(col+1, obj);
            }
            action.execute();
        }
    }

    /**
     * Iterates over an array of row vectors, comparing each to the
     * data in the RS using assertRow. Always closes the RS, even when
     * an exception is thrown. Assertion failures are intercepted and
     * 'dumpDiff' is used to print the differences between the RS and
     * the expected values to System.err.
     * @param message a message from the caller
     * @param expected array of row vectors
     * @param returned the resultset to verify
     */
    private static void assertResultSet(String message,
                                        Object[][] expected,
                                        ResultSet returned)
            throws SQLException
    {
        int i = 0;
        boolean moreRows = false;
        try {
            for (; i < expected.length && (moreRows=returned.next()); ++i) {
                assertRow(message + "(row " +(i+1)+", ",
                          expected[i],
                          returned);
            }
            assertEquals(message+" too few rows, ", expected.length, i);
            moreRows = returned.next(); ++i;
            assertFalse(message+" too many rows, expected:<"+expected.length+
                        "> but was at least:<"+i+">", moreRows);
        }
        catch (junit.framework.AssertionFailedError af) {
            System.err.println(af);
            dumpDiff(expected, i, returned, moreRows, System.err);
            throw af;
        }
        finally {
            returned.close();
        }
    }

    /**
     * Iterates over a row vector, comparing each to the corrsponding
     * column in the ResultSet. The i'th entry in the row vector is
     * compared (using assertEquals) to the return value from
     * getObject(i) on the ResultSet.
     * @param message info from the caller
     * @param expected the expected row vector
     * @param returned the resultset to verify
     */
    private static void assertRow(String message,
                                  Object[] expected,
                                  ResultSet returned) throws SQLException {
        final ResultSetMetaData rmd = returned.getMetaData();
        assertEquals(message+" columns:", expected.length,
                     rmd.getColumnCount());
        for (int i = 0; i < expected.length; ++i) {
            assertEquals(message+
                         rmd.getColumnLabel(i+1)+") ",
                         expected[i],
                         returned.getObject(i+1));
        }
    }

    /**
     * Prints a ResultSet to a PrintStream. The first line is a
     * heading with name and type of each column. Each row is printed
     * as a comma-separated list of columns.  The printed value of a
     * column is getObject(i).toString(). Closes the RS when
     * done. Intended for debugging purposes.
     * @param dumpee the ResultSet to dump
     * @param stream the stream to dump the ResultSet to
     */
    private static void dump(ResultSet dumpee,
                             PrintStream stream) throws SQLException {
        final ResultSetMetaData rm = dumpee.getMetaData();
        final int colCount = rm.getColumnCount();
        for (int c = 1; c <= colCount; ++c) {
            stream.print("" + rm.getColumnLabel(c) + " " +
                             rm.getColumnTypeName(c) + ", ");
        }
        stream.println("");
        while (dumpee.next()) {
            for (int c = 1; c <= colCount; ++c) {
                stream.print("" + dumpee.getObject(c) + ", ");
            }
            stream.println("");
        }
        dumpee.close();
    }

    /**
     * Prints a diff between a ResultSet and an expected Object[][]
     * value to a PrintStream. The first line is a heading with name
     * and type of each column. Each row is printed as a
     * comma-separated list of columns. The printed value of a column
     * is getObject(i).toString(). <p>
     *
     * If the expected value does not match the value from the RS, the
     * expected value is printed followed by the actual value in angle
     * brackets.  The comparion starts from 'fromRow' (zero-based row
     * index). Unmatched rows are printed with 'null' for the missing
     * values. <p>
     *
     * dumpee must be positioned on a valid row, or moreRows must be
     * false.  Closes the RS when done.
     * @param expected the expected value of the RS
     * @param fromRow row to start comparison from
     * @param dumpee the ResultSet to dump
     * @param moreRows true if there are more rows in the RS
     * @param stream the stream to dump the ResultSet to
     */
    private static void dumpDiff(Object[][] expected, int fromRow,
                                 ResultSet dumpee, boolean moreRows,
                                 PrintStream stream) throws SQLException {
        final ResultSetMetaData rm = dumpee.getMetaData();
        final int colCount = rm.getColumnCount();
        for (int c = 1; c <= colCount; ++c) {
            stream.print("" + rm.getColumnLabel(c) + " " +
                             rm.getColumnTypeName(c) + ", ");
        }
        stream.println("");

        for (; moreRows || fromRow < expected.length; ++fromRow) {
            for (int c = 1; c <= colCount; ++c) {
                final Object e =
                    (fromRow<expected.length?expected[fromRow][c-1]:null);
                final Object ret = (moreRows?dumpee.getObject(c):null);
                stream.print(e);
                if (e == null || ret == null || !ret.equals(e)) {
                    stream.print("<" + ret +">");
                }
                stream.print(", ");
            }
            stream.println("");
            moreRows = dumpee.next();
        }
        dumpee.close();
    }

    /**
     * Prints a ResultSet to a PrintStream in the form of an
     * Object[][] constant that can be used as "expected outcome" in
     * assertions.  Closes the ResultSet when done. Experimental and
     * not tested for data types other than String and Integer.
     * @param dumpee the ResultSet to dump
     * @param stream the stream to dump the ResultSet to
     */
    private static void dumpObjectArray(ResultSet dumpee,
                                          PrintStream stream)
        throws SQLException {
        final ResultSetMetaData rm = dumpee.getMetaData();
        final int colCount = rm.getColumnCount();
        int rows = 0;
        String rowPrefix = "";
        while (dumpee.next()) {
            ++rows;
            stream.print(rowPrefix+"{ ");
            rowPrefix = ",\n";
            String colPrefix = "";
            for (int c = 1; c <= colCount; ++c) {
                stream.print(colPrefix); colPrefix = ", ";
                final Object theObject = dumpee.getObject(c);
                if (theObject == null) {
                    stream.print("null");
                    continue;
                }
                if (theObject instanceof String) {
                    stream.print("\""+theObject+"\"");
                    continue;
                }
                stream.print("new " + rm.getColumnClassName(c) +
                             "("+ theObject + ")");
            }
            stream.print(" }");
        }
        if (rows > 0) stream.println("");
        else stream.println("<empty ResultSet>");
        dumpee.close();
    }

    /**
     * Check whether a table is locked exclusively.
     *
     * @param table name of the table
     * @return whether the table is locked exclusively
     */
    private boolean hasTableXLock(String table) throws SQLException {
        PreparedStatement ps = prepareStatement(
            "select count(*) from syscs_diag.lock_table where tablename=? " +
            "and CAST(type AS CHAR(5))='TABLE' and CAST(mode AS CHAR(1))='X'");
        ps.setString(1, table);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        int count = rs.getInt(1);
        rs.close();
        ps.close();
        return count != 0;
    }

    // ---------------------------------------------------------------
    // Framework methods
    protected void setUp() throws Exception {
        Statement s1 = createStatement();
        s1.executeUpdate("set schema " + SCHEMA);
        s1.close();
        getConnection().setAutoCommit(false);

        c2 = openDefaultConnection();
        Statement s2 = c2.createStatement();
        s2.executeUpdate("set schema " + SCHEMA);
        s2.close();
        c2.setAutoCommit(false);
    }

    protected void tearDown() throws Exception {
        rollback();
        c2.rollback();
        c2.close();
        c2 = null;

        // Some fixtures create tables named emp or dept, or views on
        // them. Drop them, but ignore errors since they might not exist.
        Statement s = createStatement();
        try { s.executeUpdate("drop view vemp"); } catch (SQLException e) {}
        try { s.executeUpdate("drop view vdept"); } catch (SQLException e) {}
        try { s.executeUpdate("drop table emp"); } catch (SQLException e) {}
        try { s.executeUpdate("drop table emp2"); } catch (SQLException e) {}
        try { s.executeUpdate("drop table dept"); } catch (SQLException e) {}

        // DERBY-4330 tables:
        try {
            if (c3 != null && !c3.isClosed()) {
                c3.rollback();
                c3.close();
            }
        } catch (SQLException e) {
        }

        try { s.executeUpdate(
                "drop table APP.FILECHANGES"); } catch (SQLException e) {}
        try { s.executeUpdate(
                "drop table APP.CHANGESETS"); } catch (SQLException e) {}
        try { s.executeUpdate(
                "drop table APP.AUTHORS"); } catch (SQLException e) {}
        try { s.executeUpdate(
                "drop table APP.FILES"); } catch (SQLException e) {}
        try { s.executeUpdate(
                "drop table APP.REPOSITORIES"); } catch (SQLException e) {}
        try { s.executeUpdate(
                "drop table APP.FILECHANGES_2"); } catch (SQLException e) {}

        try { setTimeout(DERBY_DEFAULT_TIMEOUT); } catch (SQLException e) {}

        s.close();
        commit();

        super.tearDown();
    }

    /**
     * Set up a common environment for all fixtures. Creates the
     * schema and the raw data tables 'dept_data' and 'emp_data'.
     * @return a suite containing all tests
     */
    public static Test suite()
    {
        TestSuite suite = new TestSuite
            ("Create ResultSets from PreparedStatements");
        suite.addTestSuite(ResultSetsFromPreparedStatementTest.class);

        TestSetup wrapper = new CleanDatabaseTestSetup(suite) {
                protected void decorateSQL(Statement s) throws SQLException
                {
                    Connection c = s.getConnection();
                    s.execute("create schema " +
                              ResultSetsFromPreparedStatementTest.SCHEMA);
                    s.execute("set schema " +
                              ResultSetsFromPreparedStatementTest.SCHEMA);
                    s.execute(ResultSetsFromPreparedStatementTest.CT+"dept_data"+
                              ResultSetsFromPreparedStatementTest.DS);

                    s.execute(ResultSetsFromPreparedStatementTest.CT+"emp_data"+
                              ResultSetsFromPreparedStatementTest.ES+","+
                              ResultSetsFromPreparedStatementTest.DNO+")");

                    c.commit();

                    PreparedStatement ps = c.prepareStatement
                        (ResultSetsFromPreparedStatementTest.insertInto
                         ("dept_data",3));

                    ResultSetsFromPreparedStatementTest.apply(ps, dept);
                    c.commit();
                    ps.close();

                    ps = c.prepareStatement
                        (ResultSetsFromPreparedStatementTest.insertInto
                         ("emp_data",4));

                    ResultSetsFromPreparedStatementTest.apply(ps, emp);
                    c.commit();
                    ps.close();

                    // No, cannot do this here. Will crash
                    // CleanDatabaseTestSetup.setUp()
                    // c.close();
                }
            };
        return wrapper;
    }

    /**
     * Standard JUnit constructor
     */
    public ResultSetsFromPreparedStatementTest(String name)
    {
        super(name);
    }


    // ---------------------------------------------------------------
    // Fixtures for special ResultSets
    /**
     * Test SetTransactionResultSet
     */
    public void testSetTransactionResultSet() throws Exception {
        // SetTransactionResultSet
        PreparedStatement[] setIsoLevel = new PreparedStatement[] {
            prepareStatement("set current isolation = read uncommitted"),
            prepareStatement("set current isolation = read committed"),
            prepareStatement("set current isolation = rs"),
            prepareStatement("set current isolation = serializable")
        };
        int[] expectedIsoLevel = new int[] {
            Connection.TRANSACTION_READ_UNCOMMITTED,
            Connection.TRANSACTION_READ_COMMITTED,
            Connection.TRANSACTION_REPEATABLE_READ,
            Connection.TRANSACTION_SERIALIZABLE
        };
        Connection c = getConnection();

        for (int i = 0; i < 20; ++i) {
            for (int iso = 0; iso < setIsoLevel.length; ++iso) {
                setIsoLevel[iso].execute();
                assertEquals("i="+i+" iso="+iso,expectedIsoLevel[iso],
                             c.getTransactionIsolation());
            }
        }
        for (int iso = 0; iso < setIsoLevel.length; ++iso) {
            setIsoLevel[iso].close();
        }
    }

    /**
     * Test CallStatementResultSet
     */
    public void testCallStatementResultSet() throws Exception {
        // CallStatementResultSet
        CallableStatement cs = prepareCall
            ("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?, ?)");
        cs.setString(1, "some.property.name");
        PreparedStatement ps = prepareStatement
            ("values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY"+
             "('some.property.name')");
        for (int i = 0; i < 20; ++i) {
            final Integer I = new Integer(i);
            cs.setObject(2, I);
            cs.execute();
            ResultSet rs = ps.executeQuery();
            assertResultSet("i=?="+i, new Object[][] {
                                { I.toString() }
                            }, rs);
            // Re-execute cs with the same parameter
            cs.execute();
            rs = ps.executeQuery();
            assertResultSet("Ri=?="+i, new Object[][] {
                                { I.toString() }
                            }, rs);

        }
        cs.close();
        ps.close();
    }

    /**
     * Test VTIResultSet
     */
    public void testVTIResultSet() throws Exception {
        PreparedStatement ps = prepareStatement
            ("select tablename, schemaname from sys.systables "+
             "inner join sys.sysschemas on "+
             "sys.systables.schemaid = sys.sysschemas.schemaid");
        ResultSet rs = ps.executeQuery();
        // VTIResultSet, ScrollInsensitiveResultSet,
        ps = prepareStatement
            ("select st.conglomeratename from "+
             "TABLE(SYSCS_DIAG.SPACE_TABLE(?,?)) st where st.isindex = 0");
        while (rs.next()) {
            ps.setString(1, rs.getString(2));
            ps.setString(2, rs.getString(1));
            ResultSet rs2 = ps.executeQuery();
            assertResultSet("BooHoo", new Object[][] {
                                { rs.getString(1) }
                            }, rs2);
            // Re-execute ps with the same parameters
            rs2 = ps.executeQuery();
            assertResultSet("Re-exec", new Object[][] {
                                { rs.getString(1) }
                            }, rs2);

        }
        rs.close();
        ps.close();
    }

//     /**
//      * Test InsertVTIResultSet TODO
//      */
//     public void testInsertVTIResultSet() throws Exception {
//     }
//     /**
//      * Test DeleteVTIResultSet TODO
//      */
//     public void testDeleteVTIResultSet() throws Exception {
//     }
//     /**
//      * Test UpdateVTIResultSet TODO
//      */
//     public void testUpdateVTIResultSet() throws Exception {
//     }
//     /**
//      * Test MaterializedResultSet TODO
//      */
//     public void testMaterializedResultSet() throws Exception {
//     }


    // Fixtures for distinct/aggregate ResultSets
    /**
     * Test ScalarAggregateResultSet
     */
    public void testScalarAggregateResultSet() throws Exception {
        createTestTable("emp", ES+","+DNO+")", "emp_data");
        PreparedStatement del =
            prepareStatement("delete from emp where mgrname = ?");

        // BulkTableScanResultSet, ProjectRestrictResultSet,
        // ScalarAggregateResultSet, ScrollInsensitiveResultSet
        PreparedStatement ps = prepareStatement
            ("select max(c0) from emp where mgrname = ?");

        testScalarAggregateResultSet(ps, del);

        Statement s = createStatement();
        s.executeUpdate("drop table emp");
        createTestTable("emp", ES+","+DNO+")", "emp_data");

        // re-run test on the new table (statements will be re-prepared)
        testScalarAggregateResultSet(ps, del);

        // restore data
        s.executeUpdate("delete from emp");
        s.executeUpdate(insertFrom("emp", "emp_data"));
        commit();

        // make sure another transaction has exclusive locks
        Statement s2 = c2.createStatement();
        s2.executeUpdate("update emp set c0 = c0");

        // re-run test with different isolation level (will get lock timeout
        // with other isolation levels)
        getConnection().
            setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        // don't execute the delete statement since it would try to obtain
        // exclusive locks and time out
        testScalarAggregateResultSet(ps, null);

        ps.close();
        del.close();
        s.close();
        s2.close();
    }

    private void testScalarAggregateResultSet(PreparedStatement ps,
                                              PreparedStatement del)
            throws SQLException
    {
        for (int i = 0; i < non_mgrs.length; ++i) {
            ps.setObject(1,non_mgrs[i]);
            ResultSet rs = ps.executeQuery();
            rs.next();
            assertNull(rs.getObject(1));
            assertFalse(rs.next());
            // Re-execute ps with the same parameters
            rs = ps.executeQuery();
            rs.next();
            assertNull(rs.getObject(1));
            assertFalse(rs.next());
            rs.close();
        }

        if (del == null) {
            return;
        }

        Object [][][] m = new Object[][][] {
            {{i5}}, {{i16}}, {{i10}}, {{i11}}, {{i15}}, {{i20}}
        };
        for (int i = 0; i < mgrs.length; ++i) {
            ps.setObject(1,mgrs[i]);
            ResultSet rs = ps.executeQuery();
            assertResultSet("i="+i+" ?="+mgrs[i], m[i], rs);
            // Re-execute ps with the same parameters
            rs = ps.executeQuery();
            assertResultSet("Ri="+i+" ?="+mgrs[i], m[i], rs);
            del.setString(1,mgrs[i]);
            del.execute();
        }
    }

    /**
     * Test LastIndexKeyResultSet
     */
    public void testLastIndexKeyResultSet() throws Exception {
        createTestTable("emp", ES+","+DNO+")", "emp_data");

        // LastIndexKeyResultSet, ProjectRestrictResultSet,
        // ScalarAggregateResultSet, ScrollInsensitiveResultSet,
        PreparedStatement tst = prepareStatement
            ("select ? || max(name) from emp");

        // DeleteResultSet,ProjectRestrictResultSet, TableScanResultSet
        PreparedStatement del = prepareStatement
            ("delete from emp where name = ?");

        testLastIndexKeyResultSet(tst, del);

        Statement s = createStatement();
        s.executeUpdate("drop table emp");
        createTestTable("emp", ES+","+DNO+")", "emp_data");

        // re-execute on the new table
        testLastIndexKeyResultSet(tst, del);

        // restore data
        s.executeUpdate("delete from emp");
        s.executeUpdate(insertFrom("emp", "emp_data"));
        commit();

        // make sure another transaction has exclusive locks
        Statement s2 = c2.createStatement();
        s2.executeUpdate("update emp set c0 = c0");

        // re-execute with different isolation level (will get lock timeout
        // with other isolation levels)
        getConnection().
            setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        tst.setString(1, "0_");
        JDBC.assertFullResultSet(tst.executeQuery(),
                                 new String[][] {{"0_" + truong}},
                                 false);

        tst.close();
        del.close();
        s.close();
        s2.close();
    }

    private void testLastIndexKeyResultSet(PreparedStatement tst,
                                           PreparedStatement del)
            throws SQLException
    {
        Object[][][] expected = new Object[][][] {
            {{"0_"+truong}}, {{"1_"+sam2}}, {{"2_"+sam1}}, {{"3_"+roger}},
            {{"4_"+robin}}, {{"5_"+monica}}, {{"6_"+lily2}}, {{"7_"+lily1}},
            {{"8_"+larry2}}, {{"9_"+larry1}}, {{"10_"+john}}, {{"11_"+joe2}},
            {{"12_"+joe1}}, {{"13_"+jim}}, {{"14_"+hamid}}, {{"15_"+guy}},
            {{"16_"+don}}, {{"17_"+dan}}, {{"18_"+bobbie}}, {{"19_"+ashok}},
            {{ null }}
        };

        for (int i = 0; i < expected.length; ++i) {
            tst.setString(1, new Integer(i).toString()+"_");
            ResultSet rs = tst.executeQuery();
            assertResultSet("?="+i+"_", expected[i], rs);

            // Re-execute tst with the same parameters
            rs = tst.executeQuery();
            assertResultSet("R ?="+i+"_", expected[i], rs);

            String victim = (String)expected[i][0][0];

            if (victim == null) { continue; }
            del.setString(1, victim.substring(victim.indexOf('_')+1));
            del.execute();
        }
    }

    /**
     * Test DistinctScanResultSet
     */
    public void testDistinctScanResultSet() throws Exception {
        createTestTable("emp", ES+","+DNO+")", "emp_data");
        PreparedStatement del =
            prepareStatement("delete from emp where mgrname = ?");

        // DistinctScanResultSet, ScrollInsensitiveResultSet
        PreparedStatement tst = prepareStatement
            ("select ? || T.dm from "+
             "(select distinct mgrname dm from emp) as T");

        testDistinctScanResultSet(tst, del);

        Statement s = createStatement();
        s.executeUpdate("drop table emp");
        createTestTable("emp", ES+","+DNO+")", "emp_data");

        // re-execute on the new table
        testDistinctScanResultSet(tst, del);

        // restore data
        s.executeUpdate("delete from emp");
        s.executeUpdate(insertFrom("emp", "emp_data"));
        commit();

        // make sure another transaction has exclusive locks
        Statement s2 = c2.createStatement();
        s2.executeUpdate("update emp set c0 = c0");

        // re-execute with different isolation level (will get lock timeout
        // with other isolation levels)
        getConnection().
            setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        tst.setString(1, "0_");
        // don't check contents, only that the query doesn't hang
        JDBC.assertDrainResults(tst.executeQuery(), 7);

        tst.close();
        del.close();
        s.close();
        s2.close();
    }

    private void testDistinctScanResultSet(PreparedStatement tst,
                                           PreparedStatement del)
            throws SQLException
    {
        Object[][][] expected = new Object[][][] {
            {{ "0_"+roger }, { "0_"+john }, { "0_"+robin },
             { "0_"+guy },{ "0_"+hamid },{ "0_"+ashok },{ null }},
            {{ "1_"+roger },{ "1_"+john },{ "1_"+robin },
             { "1_"+guy },{ "1_"+hamid },{ null }},
            {{ "2_"+roger },{ "2_"+robin },{ "2_"+guy },
             { "2_"+hamid },{ null }},
            {{ "3_"+roger },{ "3_"+robin },{ "3_"+guy },{ null }},
            {{ "4_"+roger },{ "4_"+guy },{ null }},
            {{ "5_"+guy },{ null }},
            {{ null }}
        };
        for (int i = 0; i < expected.length; ++i) {
            tst.setString(1,new Integer(i) +"_");
            ResultSet rs = tst.executeQuery();
            JDBC.assertUnorderedResultSet(rs, expected[i], false);

            // Re-execute tst with the same parameters
            rs = tst.executeQuery();
            JDBC.assertUnorderedResultSet(rs, expected[i], false);

            if (i < mgrs.length) {
                del.setObject(1, mgrs[i]);
                del.execute();
            }
        }
    }

    /**
     * Test DistinctScalarAggregateResultSet
     */
    public void testDistinctScalarAggregateResultSet() throws Exception {
        createTestTable("emp", ES+","+DNO+")", "emp_data");
        PreparedStatement del =
            prepareStatement("delete from emp where mgrname = ?");

        // DistinctScalarAggregateResultSet,
        // BulkTableScanResultSet, ProjectRestrictResultSet,
        // ScrollInsensitiveResultSet
        PreparedStatement tst = prepareStatement
            ("select count(distinct mgrname)+? from emp");

        testDistinctScalarAggregateResultSet(tst, del);

        Statement s = createStatement();
        s.executeUpdate("drop table emp");
        createTestTable("emp", ES+","+DNO+")", "emp_data");

        // re-execute on the new table
        testDistinctScalarAggregateResultSet(tst, del);

        // restore data
        s.executeUpdate("delete from emp");
        s.executeUpdate(insertFrom("emp", "emp_data"));
        commit();

        // make sure another transaction has exclusive locks
        Statement s2 = c2.createStatement();
        s2.executeUpdate("update emp set c0 = c0");

        // re-execute with different isolation level (will get lock timeout
        // with other isolation levels)
        getConnection().
            setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        tst.setInt(1, 0);
        JDBC.assertFullResultSet(tst.executeQuery(),
                                 new Integer[][] {{i6}},
                                 false);

        tst.close();
        del.close();
        s.close();
        s2.close();
    }

    private void testDistinctScalarAggregateResultSet(PreparedStatement tst,
                                                      PreparedStatement del)
            throws SQLException
    {
        Object[][][] expected = new Object[][][] {
            {{i6}}, {{i6}}, {{i6}}, {{i6}}, {{i6}}, {{i6}}, {{i6}}
        };
        for (int i = 0; i < expected.length; ++i) {
            tst.setInt(1,i);
            ResultSet rs = tst.executeQuery();
            assertResultSet("?="+i,expected[i],rs);
            // Re-execute tst with the same parameters
            rs = tst.executeQuery();
            assertResultSet("R?="+i,expected[i],rs);
            if (i < mgrs.length) {
                del.setString(1,mgrs[i]);
                del.execute();
            }
        }
    }

    /**
     * Test DistinctGroupedAggregateResultSet
     */
    public void testDistinctGroupedAggregateResultSet() throws Exception {
        createTestTable("emp", ES+","+DNO+")", "emp_data");
        PreparedStatement del =
            prepareStatement("delete from emp where mgrname = ?");

        // DistinctGroupedAggregateResultSet,
        // ProjectRestrictResultSet,ScrollInsensitiveResultSet,
        // TableScanResultSet
        PreparedStatement tst = prepareStatement
            ("select count(distinct mgrname) nummgrs, dno "+
             "from emp group by dno having dno <> ?");

        testDistinctGroupedAggregateResultSet(tst, del);

        Statement s = createStatement();
        s.executeUpdate("drop table emp");
        createTestTable("emp", ES+","+DNO+")", "emp_data");

        // re-execute on the new table
        testDistinctGroupedAggregateResultSet(tst, del);

        // restore data
        s.executeUpdate("delete from emp");
        s.executeUpdate(insertFrom("emp", "emp_data"));
        commit();

        // make sure another transaction has exclusive locks
        Statement s2 = c2.createStatement();
        s2.executeUpdate("update emp set c0 = c0");

        // re-execute with different isolation level (will get lock timeout
        // with other isolation levels)
        getConnection().
            setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        tst.setObject(1, dept[0][1]);
        JDBC.assertFullResultSet(tst.executeQuery(),
                                 new Object[][] {{i1, k51}, {i2, k52}},
                                 false);

        tst.close();
        del.close();
        s.close();
        s2.close();
    }

    private void testDistinctGroupedAggregateResultSet(PreparedStatement tst,
                                                       PreparedStatement del)
            throws SQLException
    {
        final Integer i0 = new Integer(0);
        Object[][][][] expected = new Object[][][][] {
            {
                {{ i1, k51 },{ i2, k52 }},
                {{ i1, k51 },{ i3, k55 }},
                {{ i2, k52 },{ i3, k55 }}
            },
            { // - ashok
                {{ i0, k51 },{ i2, k52 }},
                {{ i0, k51 },{ i3, k55 }},
                {{ i2, k52 },{ i3, k55 }}
            },
            { // - john
                {{ i0, k51 },{ i2, k52 }},
                {{ i0, k51 },{ i2, k55 }},
                {{ i2, k52 },{ i2, k55 }}
            },
            { // - hamid
                {{ i0, k51 },{ i2, k52 }},
                {{ i0, k51 },{ i1, k55 }},
                {{ i2, k52 },{ i1, k55 }}
            },
            { // - robin
                {{ i0, k51 },{ i1, k52 }},
                {{ i0, k51 },{ i1, k55 }},
                {{ i1, k52 },{ i1, k55 }}
            },
            { // - roger
                {{ i0, k51 }},
                {{ i0, k51 },{ i1, k55 }},
                {{ i1, k55 }}
            },
            { // - guy
                {{ i0, k51 }},
                {{ i0, k51 }},
                {}
            }
        };

        for (int i = 0; i < expected.length; ++i) {
            for (int d = 0; d < dept.length; ++d) {
                tst.setObject(1,dept[d][1]);
                ResultSet rs = tst.executeQuery();
                assertResultSet("i="+i+" d="+d+"("+dept[d][1]+")",
                                expected[i][d], rs);

                // Re-execute tst with the same parameters
                rs = tst.executeQuery();
                assertResultSet("Ri="+i+" d="+d+"("+dept[d][1]+")",
                                expected[i][d], rs);
            }
            if (i < mgrs.length) {
                del.setString(1,mgrs[i]);
                del.execute();
            }
        }
    }

    /**
     * Test GroupedAggregateResultSet
     */
    public void testGroupedAggregateResultSet() throws Exception {
        createTestTable("emp", ES+","+DNO+")", "emp_data");
        PreparedStatement del =
            prepareStatement("delete from emp where name = ?");

        // TableScanResultSet, ProjectRestrictResultSet,
        // GroupedAggregateResultSet, ScrollInsensitiveResultSet
        PreparedStatement tst = prepareStatement
            ("select max(name) maxemp, mgrname from emp "+
             "group by mgrname having mgrname <> ?");

        testGroupedAggregateResultSet(tst, del);

        Statement s = createStatement();
        s.executeUpdate("drop table emp");
        createTestTable("emp", ES+","+DNO+")", "emp_data");

        // re-execute on the new table
        testGroupedAggregateResultSet(tst, del);

        // restore data
        s.executeUpdate("delete from emp");
        s.executeUpdate(insertFrom("emp", "emp_data"));
        commit();

        // make sure another transaction has exclusive locks
        Statement s2 = c2.createStatement();
        s2.executeUpdate("update emp set c0 = c0");

        // re-execute with different isolation level (will get lock timeout
        // with other isolation levels)
        getConnection().
            setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        tst.setString(1, mgrs[0]);
        JDBC.assertDrainResults(tst.executeQuery(), 5);

        tst.close();
        del.close();
        s.close();
        s2.close();
    }

    private void testGroupedAggregateResultSet(PreparedStatement tst,
                                               PreparedStatement del)
            throws SQLException
    {
        Object[][][] expected = new Object[][][] {
            {
                { monica, guy },
                { truong, hamid },
                { hamid, john },
                { roger, robin },
                { sam2, roger }
            },
            { // -ashok
                { robin, ashok },
                { monica, guy },
                { truong, hamid },
                { roger, robin },
                { sam2, roger }
            },
            { // -ashok,john
                { robin, ashok },
                { monica, guy },
                { hamid, john },
                { roger, robin },
                { sam2, roger }
            },
            { // -ashok,john,hamid
                { robin, ashok },
                { monica, guy },
                { truong, hamid },
                { guy, john },
                { sam2, roger }
            },
            { // -ashok,john,hamid,robin
                { joe2, ashok },
                { monica, guy },
                { truong, hamid },
                { guy, john },
                { roger, robin },
            },
            { // -ashok,john,hamid,robin,roger
                { joe2, ashok },
                { truong, hamid },
                { guy, john },
                { sam2, roger }
            },
        };

        for (int i = 0; i < mgrs.length; ++i) {
            tst.setString(1,mgrs[i]);
            ResultSet rs = tst.executeQuery();
            assertResultSet("?="+mgrs[i], expected[i], rs);
            // Re-execute tst with the same parameters
            rs = tst.executeQuery();
            assertResultSet("R?="+mgrs[i], expected[i], rs);

            if (i < mgrs.length) {
                del.setString(1,mgrs[i]);
                del.execute();
            }
        }
    }



    // Fixtures for join ResultSets
    /**
     * Test NestedLoopResultSet
     */
    public void testNestedLoopResultSet() throws Exception {
        createTestTable("dept", DS, "dept_data");
        createTestTable("emp", ES+","+DNO+")", "emp_data");
        PreparedStatement del =
            prepareStatement("delete from emp where mgrname = ?");

        PreparedStatement tst = prepareStatement
            ("select T.name, T.mgrname, T.dno, dept.dname from dept, "+
             "(select * from emp where mgrname = ?) as T "+
             "where dept.dno = T.dno");

        testNestedLoopResultSet(tst, del);

        Statement s = createStatement();
        s.executeUpdate("drop table emp");
        createTestTable("emp", ES+","+DNO+")", "emp_data");

        // re-execute on the new table
        testNestedLoopResultSet(tst, del);

        // restore data
        s.executeUpdate("delete from emp");
        s.executeUpdate(insertFrom("emp", "emp_data"));
        commit();

        // make sure another transaction has exclusive locks
        Statement s2 = c2.createStatement();
        s2.executeUpdate("update emp set c0 = c0");

        // re-execute with different isolation level (will get lock timeout
        // with other isolation levels)
        getConnection().
            setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        tst.setString(1, mgrs[0]);
        JDBC.assertDrainResults(tst.executeQuery(), 4);

        tst.close();
        del.close();
        s.close();
        s2.close();
    }

    private void testNestedLoopResultSet(PreparedStatement tst,
                                         PreparedStatement del)
            throws SQLException
    {
        for (int i = 0; i < non_mgrs.length; ++i) {
            tst.setString(1,non_mgrs[i]);
            ResultSet rs = tst.executeQuery();
            assertFalse(rs.next());
            // Re-execute tst with the same parameters
            rs = tst.executeQuery();
            assertFalse(rs.next());
            rs.close();
        }
        Object[][][] results = new Object[][][] {
            {{ john, ashok, k51, "CS        " },
             { robin, ashok, k51, "CS        " },
             { joe1, ashok, k51, "CS        " },
             { joe2, ashok, k51, "CS        " }},

            {{ hamid, john, k55, "DB        " },
             { guy, john, k55, "DB        " }},

            {{ truong, hamid, k55, "DB        " },
             { larry1, hamid, k55, "DB        " },
             { larry2, hamid, k55, "DB        " },
             { bobbie, hamid, k55, "DB        " }},

            {{ roger, robin, k52, "OFC       " }},

            {{ jim, roger, k52, "OFC       " },
             { dan, roger, k52, "OFC       " },
             { sam1, roger, k52, "OFC       " },
             { sam2, roger, k52, "OFC       " }},

            {{ don, guy, k55, "DB        " },
             { monica, guy, k55, "DB        " },
             { lily1, guy, k55, "DB        " },
             { lily2, guy, k55, "DB        " }}
        };
        for (int i = 0; i < mgrs.length; ++i) {
            tst.setString(1,mgrs[i]);
            ResultSet rs = tst.executeQuery();
            assertResultSet("i="+i+" ?="+mgrs[i],results[i],rs);
            // Re-execute tst with the same parameters
            rs = tst.executeQuery();
            assertResultSet("Ri="+i+" ?="+mgrs[i],results[i],rs);
            del.setString(1,mgrs[i]);
            del.execute();
        }
    }

    /**
     * Test HashTableResultSet
     */
    public void testHashTableResultSet() throws Exception {
        createTestTable("dept", DS, "dept_data");
        createTestTable("emp", ES+","+DNO+")", "emp_data");
        PreparedStatement del =
            prepareStatement("delete from emp where mgrname = ?");

        Statement s = createStatement();
        s.execute("create view vemp as select * from emp");
        s.execute("create view vdept as select * from dept");
        // HashJoinResultSet, HashTableResultSet,
        // ProjectRestrictResultSet,ScrollInsensitiveResultSet,
        // TableScanResultSet
        PreparedStatement tst = prepareStatement
            ("select vemp.name, vemp.mgrname, vemp.dno, vdept.dname "+
             "from vemp inner join vdept on vemp.dno = vdept.dno "+
             "where mgrname = ?");

        testHashTableResultSet(tst, del);

        s.executeUpdate("drop view vemp");
        s.executeUpdate("drop view vdept");
        s.executeUpdate("drop table dept");
        createTestTable("dept", DS, "dept_data");
        s.executeUpdate("drop table emp");
        createTestTable("emp", ES+","+DNO+")", "emp_data");
        s.executeUpdate("create view vemp as select * from emp");
        s.executeUpdate("create view vdept as select * from dept");

        // re-execute on the new tables
        testHashTableResultSet(tst, del);

        // restore data
        s.executeUpdate("delete from emp");
        s.executeUpdate(insertFrom("emp", "emp_data"));
        commit();

        // make sure another transaction has exclusive locks
        Statement s2 = c2.createStatement();
        s2.executeUpdate("update emp set c0 = c0");

        // re-execute with different isolation level (will get lock timeout
        // with other isolation levels)
        getConnection().
            setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        tst.setString(1, mgrs[0]);
        JDBC.assertDrainResults(tst.executeQuery(), 4);

        tst.close();
        del.close();
        s.close();
        s2.close();
    }

    private void testHashTableResultSet(PreparedStatement tst,
                                        PreparedStatement del)
            throws SQLException
    {
        for (int i = 0; i < non_mgrs.length; ++i) {
            tst.setObject(1,non_mgrs[i]);
            ResultSet rs = tst.executeQuery();
            assertFalse(rs.next());
            // Re-execute tst with the same parameters
            rs = tst.executeQuery();
            assertFalse(rs.next());
            rs.close();
        }
        Object[][][] results = new Object[][][] {
            {{ john, ashok, k51, "CS        " },
             { robin, ashok, k51, "CS        " },
             { joe1, ashok, k51, "CS        " },
             { joe2, ashok, k51, "CS        " }},

            {{ hamid, john, k55, "DB        " },
             { guy, john, k55, "DB        " }},

            {{ truong, hamid, k55, "DB        " },
             { larry1, hamid, k55, "DB        " },
             { larry2, hamid, k55, "DB        " },
             { bobbie, hamid, k55, "DB        " }},

            {{ roger, robin, k52, "OFC       " }},

            {{ jim, roger, k52, "OFC       " },
             { dan, roger, k52, "OFC       " },
             { sam1, roger, k52, "OFC       " },
             { sam2, roger, k52, "OFC       " }},

            {{ don, guy, k55, "DB        " },
             { monica, guy, k55, "DB        " },
             { lily1, guy, k55, "DB        " },
             { lily2, guy, k55, "DB        " }}
        };
        for (int i = 0; i < mgrs.length; ++i) {
            tst.setObject(1,mgrs[i]);
            ResultSet rs = tst.executeQuery();
            assertResultSet("i="+i+" ?="+mgrs[i],results[i],rs);
            // Re-execute tst with the same parameters
            rs = tst.executeQuery();
            assertResultSet("Ri="+i+" ?="+mgrs[i],results[i],rs);
            del.setString(1,mgrs[i]);
            del.execute();
        }
    }

    /**
     * Test NestedLoopLeftOuterJoinResultSet
     */
    public void testNestedLoopLeftOuterJoinResultSet() throws Exception {
        createTestTable("dept", DS, "dept_data");
        createTestTable("emp", ES+","+DNO+")", "emp_data");
        PreparedStatement del =
            prepareStatement("delete from emp where mgrname = ?");
        // BulkTableScanResultSet,IndexRowToBaseRowResultSet,
        // NestedLoopLeftOuterJoinResultSet,
        // ProjectRestrictResultSet, ScrollInsensitiveResultSet,
        // TableScanResultSet
        PreparedStatement tst = prepareStatement
            ("select emp.name, emp.mgrname, emp.dno, dept.dname "+
             "from emp left outer join dept on emp.dno = dept.dno "+
             "where mgrname = ?");

        testNestedLoopLeftOuterJoinResultSet(tst, del);

        Statement s = createStatement();
        s.executeUpdate("drop table dept");
        createTestTable("dept", DS, "dept_data");
        s.executeUpdate("drop table emp");
        createTestTable("emp", ES+","+DNO+")", "emp_data");

        // re-execute on the new table
        testNestedLoopLeftOuterJoinResultSet(tst, del);

        // restore data
        s.executeUpdate("delete from emp");
        s.executeUpdate(insertFrom("emp", "emp_data"));
        commit();

        // make sure another transaction has exclusive locks
        Statement s2 = c2.createStatement();
        s2.executeUpdate("update emp set c0 = c0");

        // re-execute with different isolation level (will get lock timeout
        // with other isolation levels)
        getConnection().
            setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        tst.setString(1, mgrs[0]);
        JDBC.assertDrainResults(tst.executeQuery(), 4);

        tst.close();
        del.close();
        s.close();
        s2.close();
    }

    private void testNestedLoopLeftOuterJoinResultSet(PreparedStatement tst,
                                                      PreparedStatement del)
            throws SQLException
    {
        for (int i = 0; i < non_mgrs.length; ++i) {
            tst.setObject(1,non_mgrs[i]);
            ResultSet rs = tst.executeQuery();
            assertFalse(rs.next());
            rs = tst.executeQuery();
            assertFalse(rs.next());
            rs.close();
        }
        Object[][][] results = new Object[][][] {
            {{ john, ashok, k51, "CS        " },
             { robin, ashok, k51, "CS        " },
             { joe1, ashok, k51, "CS        " },
             { joe2, ashok, k51, "CS        " }},

            {{ hamid, john, k55, "DB        " },
             { guy, john, k55, "DB        " }},

            {{ truong, hamid, k55, "DB        " },
             { larry1, hamid, k55, "DB        " },
             { larry2, hamid, k55, "DB        " },
             { bobbie, hamid, k55, "DB        " }},

            {{ roger, robin, k52, "OFC       " }},

            {{ jim, roger, k52, "OFC       " },
             { dan, roger, k52, "OFC       " },
             { sam1, roger, k52, "OFC       " },
             { sam2, roger, k52, "OFC       " }},

            {{ don, guy, k55, "DB        " },
             { monica, guy, k55, "DB        " },
             { lily1, guy, k55, "DB        " },
             { lily2, guy, k55, "DB        " }}
        };
        for (int i = 0; i < mgrs.length; ++i) {
            tst.setObject(1,mgrs[i]);
            ResultSet rs = tst.executeQuery();
            assertResultSet("i="+i+" ?="+mgrs[i],results[i],rs);
            // Re-execute tst with the same parameters
            rs = tst.executeQuery();
            assertResultSet("Ri="+i+" ?="+mgrs[i],results[i],rs);
            del.setString(1,mgrs[i]);
            del.execute();
        }
    }

    /**
     * Test HashLeftOuterJoinResultSet
     */
    public void testHashLeftOuterJoinResultSet() throws Exception {
        createTestTable("emp", ES+","+DNO+")", "emp_data");
        createTestTable("emp2", ES+","+DNO+")", "emp_data");
        PreparedStatement del =
            prepareStatement("delete from emp where mgrname = ?");

        // BulkTableScanResultSet, HashLeftOuterJoinResultSet,
        // HashScanResultSet, ProjectRestrictResultSet,
        // ScrollInsensitiveResultSet, SortResultSet
        PreparedStatement tst = prepareStatement
            ("select distinct emp.* "+
             "from emp left outer join emp2 on emp.dno = emp2.dno "+
             "where emp.mgrname = ? order by emp.c0");

        testHashLeftOuterJoinResultSet(tst, del);

        Statement s = createStatement();
        s.executeUpdate("drop table emp");
        createTestTable("emp", ES+","+DNO+")", "emp_data");
        s.executeUpdate("drop table emp2");
        createTestTable("emp2", ES+","+DNO+")", "emp_data");

        // re-execute on the new table
        testHashLeftOuterJoinResultSet(tst, del);

        // restore data
        s.executeUpdate("delete from emp");
        s.executeUpdate(insertFrom("emp", "emp_data"));
        commit();

        // make sure another transaction has exclusive locks
        Statement s2 = c2.createStatement();
        s2.executeUpdate("update emp set c0 = c0");

        // re-execute with different isolation level (will get lock timeout
        // with other isolation levels)
        getConnection().
            setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        tst.setString(1, mgrs[0]);
        JDBC.assertDrainResults(tst.executeQuery(), 4);

        tst.close();
        del.close();
        s.close();
        s2.close();
    }

    private void testHashLeftOuterJoinResultSet(PreparedStatement tst,
                                                PreparedStatement del)
            throws SQLException
    {
        for (int i = 0; i < non_mgrs.length; ++i) {
            tst.setObject(1,non_mgrs[i]);
            ResultSet rs = tst.executeQuery();
            assertFalse(rs.next());
            // Re-execute tst with the same parameters
            rs = tst.executeQuery();
            assertFalse(rs.next());
            rs.close();
        }
        Object[][][] results = new Object[][][] {
            { john_emp, robin_emp, joe1_emp, joe2_emp },
            { hamid_emp, guy_emp },
            { truong_emp, larry1_emp, larry2_emp, bobbie_emp },
            { roger_emp },
            { jim_emp, dan_emp, sam1_emp, sam2_emp },
            { don_emp, monica_emp, lily1_emp, lily2_emp }
        };
        for (int i = 0; i < mgrs.length; ++i) {
            tst.setObject(1,mgrs[i]);
            ResultSet rs = tst.executeQuery();
            assertResultSet("i="+i+" ?="+mgrs[i],results[i],rs);
            // Re-execute tst with the same parameters
            rs = tst.executeQuery();
            assertResultSet("Ri="+i+" ?="+mgrs[i],results[i],rs);
            del.setString(1,mgrs[i]);
            del.execute();
        }
    }

    // Fixtures for update ResultSets
    /**
     * Test UpdateResultSet
     */
    public void testUpdateResultSet() throws Exception {
        createTestTable("dept", DS, "dept_data");
        createTestTable("emp", ES+","+DNO+")", "emp_data");

        // IndexRowToBaseRowResultSet, NormalizeResultSet,
        // ProjectRestrictResultSet, TableScanResultSet,
        // UpdateResultSet
        PreparedStatement tst = prepareStatement
            ("update dept set dname = ? where dno = ?");
        PreparedStatement sel = prepareStatement
            ("select dno, dname from dept order by c0");

        Object[][][] expected = new Object[][][] {
            { {k55, "DataBase  "}, {k52, "OFC       "}, {k51, "CS        "} },
            { {k55, "DataBase  "}, {k52, "Office    "}, {k51, "CS        "} },
            { {k55, "DataBase  "}, {k52, "Office    "}, {k51, "Computer S"} }
        };

        for (int i = 0; i < expected.length; ++i) {
            tst.setObject(1, expected[i][i][1]);
            tst.setObject(2, expected[i][i][0]);
            tst.executeUpdate();
            ResultSet rs = sel.executeQuery();
            assertResultSet("i="+i+" ?="+expected[i][i][1]+" ?="+
                            expected[i][i][0],expected[i],rs);
            // Re-execute with the same parameters
            tst.executeUpdate();
            rs = sel.executeQuery();
            assertResultSet("Ri="+i+" ?="+expected[i][i][1]+" ?="+
                            expected[i][i][0],expected[i],rs);
        }
        tst.close();
        sel.close();
    }

    /**
     * Test that an UpdateResultSet is able to detect changes in isolation
     * level between executions.
     */
    public void testUpdateResultSetWithIsolation() throws SQLException {
        createTestTable("dept", DS, "dept_data");
        commit();
        PreparedStatement ps = prepareStatement("update dept set c0 = c0");
        getConnection().setTransactionIsolation(
            Connection.TRANSACTION_SERIALIZABLE);
        assertEquals(3, ps.executeUpdate());
        // when serializable, UpdateResultSet uses table-level locking
        assertTrue(hasTableXLock("DEPT"));
        commit();
        // all locks released on commit
        assertFalse(hasTableXLock("DEPT"));
        getConnection().setTransactionIsolation(
            Connection.TRANSACTION_READ_COMMITTED);
        assertEquals(3, ps.executeUpdate());
        // when read committed, UpdateResultSet uses row-level locking
        assertFalse(hasTableXLock("DEPT"));
        ps.close();
    }

    /**
     * Test CurrentOfResultSet
     */
    public void testCurrentOfResultSet() throws Exception {
        createTestTable("dept", DS, "dept_data");
        createTestTable("emp", ES+","+DNO+")", "emp_data");

        PreparedStatement selForUpd = prepareStatement
            ("select * from dept for update of dname");
        selForUpd.setCursorName("C1");
        ResultSet rs = selForUpd.executeQuery();
        // CurrentOfResultSet, NormalizeResultSet,
        // ProjectRestrictResultSet, UpdateResultSet
        PreparedStatement tst = prepareStatement
            ("update dept set dname = ? where current of C1");

        PreparedStatement sel = prepareStatement("select dname from dept");

        Object[][][] expected = new Object[][][] {
            {{"foobar___0"},{"OFC       "},{"CS        "}},
            {{"foobar___0"},{"foobar___1"},{"CS        "}},
            {{"foobar___0"},{"foobar___1"},{"foobar___2"}}
        };

        for (int i = 0; i < expected.length; ++i) {
            assertTrue(rs.next());
            tst.setObject(1, expected[i][i][0]);
            tst.executeUpdate();
            ResultSet sel_rs = sel.executeQuery();
            assertResultSet("i="+i+" ?="+expected[i][i][0],
                            expected[i], sel_rs);
            // Re-execute tst with the same parameters
            tst.executeUpdate();
            sel_rs = sel.executeQuery();
            assertResultSet("Ri="+i+" ?="+expected[i][i][0],
                            expected[i], sel_rs);
        }
        assertFalse(rs.next());
        rs.close();
        tst.close();
        sel.close();
        selForUpd.close();
    }


    // Fixtures for delete ResultSets
    /**
     * Test DeleteResultSet
     */
    public void testDeleteResultSet() throws Exception {
        createTestTable("emp", ES+","+DNO+")", "emp_data");

        // DeleteResultSet, ProjectRestrictResultSet, TableScanResultSet
        PreparedStatement tst = prepareStatement
            ("delete from emp where mgrname = ?");
        PreparedStatement sel = prepareStatement
            ("select * from emp");

        for (int i = 0; i < non_mgrs.length; ++i) {
            tst.setObject(1,non_mgrs[i]);
            tst.execute();
            ResultSet rs = sel.executeQuery();
            assertResultSet("i="+i+" ?="+non_mgrs[i], emp, rs);
            // Re-execute tst with the same parameters
            tst.execute();
            rs = sel.executeQuery();
            assertResultSet("Ri="+i+" ?="+non_mgrs[i], emp, rs);
        }

        Object [][][] expected = new Object [][][] {
            { ashok_emp, hamid_emp, truong_emp, larry1_emp,
              larry2_emp, bobbie_emp, roger_emp, jim_emp,
              dan_emp, sam1_emp, sam2_emp, guy_emp, don_emp,
              monica_emp, lily1_emp, lily2_emp },

            { ashok_emp, truong_emp, larry1_emp, larry2_emp,
              bobbie_emp, roger_emp, jim_emp, dan_emp, sam1_emp,
              sam2_emp, don_emp, monica_emp, lily1_emp, lily2_emp },

            { ashok_emp, roger_emp, jim_emp, dan_emp, sam1_emp,
              sam2_emp, don_emp, monica_emp, lily1_emp, lily2_emp },

            { ashok_emp, jim_emp, dan_emp, sam1_emp, sam2_emp,
              don_emp, monica_emp, lily1_emp, lily2_emp },

            { ashok_emp, don_emp, monica_emp, lily1_emp, lily2_emp },
            { ashok_emp }
        };

        for (int i = 0; i < mgrs.length; ++i) {
            tst.setObject(1,mgrs[i]);
            tst.execute();
            ResultSet rs = sel.executeQuery();
            assertResultSet("i="+i+" ?="+non_mgrs[i], expected[i], rs);
            //dumpAsObjectArray(rs,System.err);
            // Re-execute tst with the same parameters
            tst.execute();
            rs = sel.executeQuery();
            assertResultSet("Ri="+i+" ?="+non_mgrs[i], expected[i], rs);
        }
        tst.close();
        sel.close();
    }
   /**
     * Test DeleteCascadeUpdateResultSet
     */
    public void testDeleteCascadeUpdateResultSet() throws Exception {
        createTestTable("dept", DS, "dept_data");
        createTestTable("emp",
                        ES+ref("emp")+SETN+","+DNO+ref("dept")+SETN+")",
                        "emp_data");

        PreparedStatement delMgr = prepareStatement
            ("delete from emp where mgrname = ?");

        // DeleteCascadeResultSet, DeleteCascadeUpdateResultSet,
        // NormalizeResultSet,ProjectRestrictResultSet,
        // RaDependentTableScanResultSet, TableScanResultSet
        // TODO: Parameters? Possible?
        PreparedStatement tst = prepareStatement("delete from emp");
        PreparedStatement ins = prepareStatement(insertFrom("emp", "emp_data"));
        PreparedStatement sel = prepareStatement("select * from emp");
        for (int i = 0; i < mgrs.length; ++i) {
            // Delete some rows so that del will get a different workload
            delMgr.setString(1,mgrs[i]);
            delMgr.execute();

            // Delete all remaining rows (this is what we test here)
            tst.execute();
            ResultSet rs = sel.executeQuery();
            assertFalse(rs.next());
            rs.close();
            // Fill table for next loop iteration
            ins.execute();
        }
        delMgr.close();
        tst.close();
        ins.close();
        sel.close();
    }


    // Fixtures for set operation ResultSets
    /**
     * Test SetOpResultSet intersect
     */
    public void testSetOpResultSet_intersect() throws Exception {
        createTestTable("emp", ES+","+DNO+")", "emp_data");

        // SetOpResultSet
        PreparedStatement tst = prepareStatement
            ("select * from emp where dno = ? intersect "+
             "select * from emp where mgrname = ?");

        Object[][][][] expected = new Object [][][][] {
            { // K55
                {}, // ashok
                { hamid_emp, guy_emp }, // john
                { truong_emp, larry1_emp, larry2_emp, bobbie_emp }, // hamid
                {}, // robin
                {}, // roger
                { don_emp, monica_emp, lily1_emp, lily2_emp } // guy
            },
            { // K52
                {}, // ashok
                {}, // john
                {}, // hamid
                { roger_emp }, // robin
                { jim_emp, dan_emp, sam1_emp, sam2_emp }, // roger
                {}, // guy
            },
            { // K51
                { john_emp, robin_emp, joe1_emp, joe2_emp }, // ashok
                {}, // john
                {}, // hamid
                {}, // robin
                {}, // roger
                {} // guy
            }
        };

        for (int d = 0; d < dept.length; ++d) {
            tst.setObject(1,dept[d][1]);
            for (int m = 0; m < mgrs.length; ++m) {
                tst.setString(2,mgrs[m]);
                ResultSet rs = tst.executeQuery();
                assertResultSet("?="+dept[d][1]+" ?="+mgrs[m],
                                expected[d][m], rs);

                // Re-execution of tst with the same parameters
                rs = tst.executeQuery();
                assertResultSet("R?="+dept[d][1]+" ?="+mgrs[m],
                                expected[d][m], rs);
            }
        }
        tst.close();
    }

    /**
     * Test SetOpResultSet except
     */
    public void testSetOpResultSet_except() throws Exception {
        createTestTable("emp", ES+","+DNO+")", "emp_data");

        // SetOpResultSet
        PreparedStatement tst = prepareStatement
            ("select * from emp where dno = ? except "+
             "select * from emp where mgrname = ?");

        Object[][][][] expected = new Object [][][][] {
            { // K55
                { hamid_emp, truong_emp, larry1_emp, larry2_emp, bobbie_emp,
                  guy_emp, don_emp, monica_emp, lily1_emp,
                  lily2_emp }, // ashok

                { truong_emp, larry1_emp, larry2_emp, bobbie_emp,
                  don_emp, monica_emp, lily1_emp, lily2_emp }, // john

                { hamid_emp, guy_emp, don_emp, monica_emp, lily1_emp,
                  lily2_emp }, // hamid

                { hamid_emp, truong_emp, larry1_emp, larry2_emp, bobbie_emp,
                  guy_emp, don_emp, monica_emp, lily1_emp,
                  lily2_emp}, // robin

                { hamid_emp, truong_emp, larry1_emp, larry2_emp, bobbie_emp,
                  guy_emp, don_emp, monica_emp, lily1_emp,
                  lily2_emp}, // roger

                { hamid_emp, truong_emp, larry1_emp, larry2_emp, bobbie_emp,
                  guy_emp } // guy
            },
            { // K52
                { roger_emp, jim_emp, dan_emp, sam1_emp, sam2_emp }, // ashok
                { roger_emp, jim_emp, dan_emp, sam1_emp, sam2_emp }, // john
                { roger_emp, jim_emp, dan_emp, sam1_emp, sam2_emp }, // hamid
                { jim_emp, dan_emp, sam1_emp, sam2_emp }, // robin
                { roger_emp }, // roger
                { roger_emp, jim_emp, dan_emp, sam1_emp, sam2_emp }, // guy
            },
            { // K51
                { ashok_emp }, // ashok
                { ashok_emp, john_emp, robin_emp, joe1_emp, joe2_emp }, // john
                { ashok_emp, john_emp, robin_emp, joe1_emp, joe2_emp }, // hamid
                { ashok_emp, john_emp, robin_emp, joe1_emp, joe2_emp }, // robin
                { ashok_emp, john_emp, robin_emp, joe1_emp, joe2_emp }, // roger
                { ashok_emp, john_emp, robin_emp, joe1_emp, joe2_emp } // guy
            }
        };

        for (int d = 0; d < dept.length; ++d) {
            tst.setObject(1,dept[d][1]);
            for (int m = 0; m < mgrs.length; ++m) {
                tst.setString(2,mgrs[m]);
                ResultSet rs = tst.executeQuery();
                assertResultSet("?="+dept[d][1]+" ?="+mgrs[m],
                                expected[d][m], rs);

                // Re-execution of tst with the same parameters
                rs = tst.executeQuery();
                assertResultSet("R?="+dept[d][1]+" ?="+mgrs[m],
                                expected[d][m], rs);
            }
        }
        tst.close();
    }

    /**
     * Test UnionResultSet
     */
    public void testUnionResultSet() throws Exception {
        createTestTable("dept", DS, "dept_data");
        createTestTable("emp", ES+","+DNO+")", "emp_data");

        // ScrollInsensitiveResultSet, SortResultSet,
        // TableScanResultSet, UnionResultSet
        PreparedStatement tst = prepareStatement
            ("(select * from emp where dno = ?) union "+
             "(select * from emp where mgrname = ?)");

        Object[][][][] expected = new Object [][][][] {
            { // K55
                { john_emp, robin_emp, joe1_emp, joe2_emp, hamid_emp,
                  truong_emp, larry1_emp, larry2_emp, bobbie_emp,
                  guy_emp, don_emp, monica_emp, lily1_emp,
                  lily2_emp }, // ashok

                { hamid_emp, truong_emp, larry1_emp, larry2_emp, bobbie_emp,
                  guy_emp, don_emp, monica_emp, lily1_emp, lily2_emp }, // john

                { hamid_emp, truong_emp, larry1_emp, larry2_emp, bobbie_emp,
                  guy_emp, don_emp, monica_emp, lily1_emp,
                  lily2_emp }, // hamid

                { hamid_emp, truong_emp, larry1_emp, larry2_emp, bobbie_emp,
                  roger_emp, guy_emp, don_emp, monica_emp, lily1_emp,
                  lily2_emp }, // robin

                { hamid_emp, truong_emp, larry1_emp, larry2_emp, bobbie_emp,
                  jim_emp, dan_emp, sam1_emp, sam2_emp, guy_emp, don_emp,
                  monica_emp, lily1_emp, lily2_emp  }, // roger

                { hamid_emp, truong_emp, larry1_emp, larry2_emp, bobbie_emp,
                  guy_emp, don_emp, monica_emp, lily1_emp,
                  lily2_emp } // guy
            },
            { // K52
                { john_emp, robin_emp, joe1_emp, joe2_emp,roger_emp,
                  jim_emp, dan_emp, sam1_emp, sam2_emp, }, // ashok
                { hamid_emp, roger_emp, jim_emp, dan_emp, sam1_emp, sam2_emp,
                  guy_emp }, // john
                { truong_emp, larry1_emp, larry2_emp, bobbie_emp,
                  roger_emp, jim_emp, dan_emp, sam1_emp, sam2_emp }, // hamid
                { roger_emp, jim_emp, dan_emp, sam1_emp, sam2_emp }, // robin
                { roger_emp, jim_emp, dan_emp, sam1_emp, sam2_emp }, // roger
                { roger_emp, jim_emp, dan_emp, sam1_emp, sam2_emp,
                  don_emp, monica_emp, lily1_emp, lily2_emp }, // guy
            },
            { // K51
                { ashok_emp, john_emp, robin_emp, joe1_emp, joe2_emp }, // ashok
                { ashok_emp, john_emp, robin_emp, joe1_emp, joe2_emp,
                  hamid_emp, guy_emp }, // john
                { ashok_emp, john_emp, robin_emp, joe1_emp, joe2_emp,
                  truong_emp, larry1_emp, larry2_emp, bobbie_emp }, // hamid
                { ashok_emp, john_emp, robin_emp, joe1_emp, joe2_emp,
                  roger_emp }, // robin
                { ashok_emp, john_emp, robin_emp, joe1_emp, joe2_emp,
                  jim_emp, dan_emp, sam1_emp, sam2_emp }, // roger
                { ashok_emp, john_emp, robin_emp, joe1_emp, joe2_emp,
                  don_emp, monica_emp, lily1_emp, lily2_emp } // guy
            }
        };

        for (int d = 0; d < dept.length; ++d) {
            tst.setObject(1,dept[d][1]);
            for (int m = 0; m < mgrs.length; ++m) {
                tst.setString(2,mgrs[m]);
                ResultSet rs = tst.executeQuery();
                assertResultSet("?="+dept[d][1]+" ?="+mgrs[m],
                                    expected[d][m], rs);

                // Re-execution of tst with the same parameters
                rs = tst.executeQuery();
                assertResultSet("R?="+dept[d][1]+" ?="+mgrs[m],
                                expected[d][m], rs);
            }
        }
        tst.close();
    }


    // Fixtures for binder ResultSets
    /**
     * Test OnceResultSet
     */
    public void testOnceResultSet() throws Exception {
        createTestTable("emp", ES+","+DNO+")", "emp_data");

        // ScrollInsensitiveResultSet, RowResultSet, UnionResultSet,
        // SortResultSet, OnceResultSet, BulkTableScanResultSet,
        // ScrollInsensitiveResultSet
        PreparedStatement tst1 = prepareStatement
            ("select * from emp where c0 = (values (1+?) union values (1+?))");

        for (int i = 0; i < emp.length; ++i) {
            tst1.setInt(1,i);
            tst1.setInt(2,i);
            ResultSet rs = tst1.executeQuery();
            assertResultSet("i="+i, new Object[][] {
                                emp[i]
                            }, rs);
            // Re-execute tst with the same parameters
            rs = tst1.executeQuery();
            assertResultSet("Ri="+i, new Object[][] {
                                emp[i]
                            }, rs);
        }
        tst1.close();

        // TableScanResultSet, ProjectRestrictResultSet, SortResultSet,
        // SetOpResultSet, OnceResultSet, IndexRowToBaseRowResultSet,
        // ScrollInsensitiveResultSet
        PreparedStatement tst2 = prepareStatement
            ("select * from emp where name = "+
             "(select name from emp where c0 <= ? intersect "+
             "select name from emp where c0 >= ?)");
        for (int i = 0; i < emp.length; ++i) {
            tst2.setInt(1,i+1);
            tst2.setInt(2,i+1);
            ResultSet rs = tst2.executeQuery();
            assertResultSet("i="+i, new Object[][] {
                                emp[i]
                            }, rs);
            // Re-execute tst with the same parameters
            rs = tst2.executeQuery();
            assertResultSet("Ri="+i, new Object[][] {
                                emp[i]
                            }, rs);
        }
        tst2.close();
    }

    /**
     * Test AnyResultSet
     */
    public void testAnyResultSet() throws Exception {
        createTestTable("emp", ES+","+DNO+")", "emp_data");

        PreparedStatement tst = prepareStatement
            ("select * from ( values 'EXISTED' ) as T(result) where exists "+
             "((select name from emp where c0 <= ?) intersect "+
             "(select name from emp where c0 >= ?))");

        Object [][] existed = {{ "EXISTED" }};
        Object [][] empty = {};
        for (int i = 0; i < emp.length; ++i) {
            tst.setInt(1,i+1);
            tst.setInt(2,i+1);
            ResultSet rs = tst.executeQuery();
            assertResultSet("?="+(i+1)+" ?="+(i+1), existed, rs);
            // Re-execute tst with the same parameters
            rs = tst.executeQuery();
            assertResultSet("R?="+(i+1)+" ?="+(i+1), existed, rs);

            // Make the sets are disjunct (DERBY-2370)
            tst.setInt(1,i);
            rs = tst.executeQuery();
            assertResultSet("?="+i+" ?="+(i+1), empty, rs);

            // Re-execute tst with the same parameters
            rs = tst.executeQuery();
            assertResultSet("R?="+i+" ?="+(i+1), empty, rs);
        }
        tst.close();
    }


    // Regression tests for DERBY-3343 (regression from the fix for DERBY-827)
    /**
     * Private helper method. Runs the same test for different
     * generated identity columns.
     * @param dataType SMALLINT, INT, or BIGINT
     * @param generatedType BY DEFAULT or ALWAYS
     * @throws Exception all errors passed on to JUnit
     */
    private void testGeneratedIdentity(String dataType, String generatedType) 
        throws Exception {
        Statement s = createStatement();
        s.execute("CREATE TABLE T(GI "+dataType+" PRIMARY KEY GENERATED "+
                  generatedType+
                  " AS IDENTITY (START WITH 5, INCREMENT BY 10), "+
                  "L VARCHAR(8))");
        PreparedStatement implicit = 
            prepareStatement("INSERT INTO T(L) VALUES('implicit')"); 
        implicit.executeUpdate();
        implicit.executeUpdate();
        implicit.executeUpdate();
        
        PreparedStatement explicit = 
            prepareStatement("INSERT INTO T(GI, L) "+
                             "VALUES(DEFAULT, 'explicit')"); 
        explicit.executeUpdate();
        explicit.executeUpdate();
        explicit.executeUpdate();
    } 
    public void testIntGeneratedByDefaultAsIdentity() throws Exception {
        testGeneratedIdentity("INT","BY DEFAULT");
    }
    public void testSmallintGeneratedByDefaultAsIdentity() throws Exception {
        testGeneratedIdentity("SMALLINT","BY DEFAULT");
    }
    public void testBigintGeneratedByDefaultAsIdentity() throws Exception {
        testGeneratedIdentity("BIGINT","BY DEFAULT");
    }

    public void testIntGeneratedAlwaysAsIdentity() throws Exception {
        testGeneratedIdentity("INT","ALWAYS");
    }
    public void testSmallintGeneratedAlwaysAsIdentity() throws Exception {
        testGeneratedIdentity("SMALLINT","ALWAYS");
    }
    public void testBigintGeneratedAlwaysAsIdentity() throws Exception {
        testGeneratedIdentity("BIGINT","ALWAYS");
    }

    /**
     * Tests that the {@code maxRows} setting takes effect, data is obtained
     * from a table select.
     */
    public void testSetMaxRowsTable()
            throws SQLException {
        createTestTable("emp", ES+","+DNO+")", "emp_data");
        PreparedStatement ps = prepareStatement("select * from emp_data",
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        // Just make sure we have enough rows in the result set first.
        // Execute without explicitly specifying maxRows.
        assertTrue(JDBC.assertDrainResults(ps.executeQuery()) >= 20);

        // Specify maxRows
        ps.setMaxRows(5);
        JDBC.assertDrainResults(ps.executeQuery(), 5);
        ps.setMaxRows(20);
        JDBC.assertDrainResults(ps.executeQuery(), 20);
    }

    /**
     * Tests that the {@code maxRows} setting takes effect, data is obtained
     * from a value clause.
     */
    public void testSetMaxRowsValues()
            throws SQLException {
        PreparedStatement ps = prepareStatement("values 0,1,2,3,4,5,6,7,8,9",
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        ps.setMaxRows(10);
        JDBC.assertDrainResults(ps.executeQuery(), 10);
        ps.setMaxRows(2);
        JDBC.assertDrainResults(ps.executeQuery(), 2);
    }

    // Regression tests for DERBY-4204 (regression from DERBY-827)
    /**
     * Private helper method that prepares and executes an SQL statement
     * multiple times and checks that the runtime statistics for the correct
     * statement is collected.
     *
     * @param sql the SQL text to prepare and execute
     */
    private void testRuntimeStatistics(String sql) throws SQLException {
        Statement s = createStatement();
        s.execute("call syscs_util.syscs_set_runtimestatistics(1)");
        PreparedStatement ps = prepareStatement(sql);
        for (int i = 0; i < 5; i++) {
            ps.execute();
            while (true) {
                if (ps.getMoreResults()) {
                    JDBC.assertDrainResults(ps.getResultSet());
                } else if (ps.getUpdateCount() == -1) {
                    // No result set and no update count means no more results
                    break;
                }
            }
            RuntimeStatisticsParser rtsp =
                    SQLUtilities.getRuntimeStatisticsParser(s);
            // Before DERBY-4204 the assert below would fail for some kinds
            // of statements because the statistics for the previous call to
            // SYSCS_GET_RUNTIMESTATISTICS() would be returned instead of the
            // statistics for the previously executed statement.
            assertTrue("Wrong statement", rtsp.findString(sql, 1));
        }
    }

    /**
     * Test that runtime statistics are collected on re-execution of a
     * SELECT statement.
     */
    public void testRuntimeStatisticsForSelect() throws SQLException {
        createTestTable("dept", DS, "dept_data");
        testRuntimeStatistics("select * from dept");
    }

    /**
     * Test that runtime statistics are collected on re-execution of an
     * UPDATE statement.
     */
    public void testRuntimeStatisticsForUpdate() throws SQLException {
        createTestTable("dept", DS, "dept_data");
        testRuntimeStatistics("update dept set dname = upper(dname)");
    }

    /**
     * Test that runtime statistics are collected on re-execution of an
     * INSERT statement.
     */
    public void testRuntimeStatisticsForInsert() throws SQLException {
        createTestTable("dept", DS, "dept_data");
        testRuntimeStatistics("insert into dept select * from dept where 1<>1");
    }

    /**
     * Test that runtime statistics are collected on re-execution of a
     * DELETE statement.
     */
    public void testRuntimeStatisticsForDelete() throws SQLException {
        createTestTable("dept", DS, "dept_data");
        testRuntimeStatistics("delete from dept");
    }

    /**
     * Test that runtime statistics are collected on re-execution of a
     * VALUES statement.
     */
    public void testRuntimeStatisticsForValues() throws SQLException {
        testRuntimeStatistics("values (1, 2, 3, 'this is a test')");
    }

    /**
     * Test that runtime statistics are collected on re-execution of a
     * CALL statement.
     */
    public void testRuntimeStatisticsForCall() throws SQLException {
        createTestTable("dept", DS, "dept_data");
        testRuntimeStatistics(
                "call syscs_util.syscs_compress_table(" +
                "current schema, 'DEPT', 1)");
    }


    public void testDerby4330_JoinResultSet()  throws SQLException {
        setTimeout(1);
        setSchema("APP");
        createDerby4330_join_tables();

        PreparedStatement ps = prepareStatement(
            "SELECT CS.REVISION, A.NAME, CS.TIME, CS.MESSAGE, F.PATH " +
            "FROM " +
            "CHANGESETS CS, FILECHANGES FC, " +
            "           REPOSITORIES R, FILES F, AUTHORS A " +
            "WHERE " +
            "F.REPOSITORY = R.ID AND A.REPOSITORY = R.ID AND " +
            "CS.REPOSITORY = R.ID AND CS.ID = FC.CHANGESET AND " +
            "F.ID = FC.FILE AND A.ID = CS.AUTHOR AND " +
            "EXISTS ( " +
            "SELECT 1 " +
            "FROM FILES F2 " +
            "WHERE " +
            "F2.ID = FC.FILE AND F2.REPOSITORY = R.ID) " +
            "ORDER BY CS.ID DESC");

        c3 = openDefaultConnection();
        c3.setAutoCommit(false);
        Statement stm2 = c3.createStatement();
        stm2.execute("LOCK TABLE FILECHANGES IN EXCLUSIVE MODE");
        stm2.close();

        try {
            ps.executeQuery();
            fail();
        } catch (SQLException e) {
            assertSQLState("Expected timeout", "40XL1", e);
        }

        c3.rollback();
        c3.close();

        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(rs.getString(2), "xyz"); // name
        assertFalse(rs.next());
        ps.close();

    }


    public void testDerby4330_UnionResultSet()  throws SQLException {
        setTimeout(1);
        setSchema("APP");
        createDerby4330_union_tables();

        PreparedStatement ps = prepareStatement(
            "SELECT * FROM (" +
            "SELECT * FROM FILECHANGES_2  UNION " +
            "SELECT * FROM FILECHANGES) X"); // locked file last

        PreparedStatement ps_inverse = prepareStatement(
            "SELECT * FROM (" +
            "SELECT * FROM FILECHANGES  UNION " + // locked file first
            "SELECT * FROM FILECHANGES_2) X");

        c3 = openDefaultConnection();
        c3.setAutoCommit(false);
        Statement stm2 = c3.createStatement();
        stm2.execute("LOCK TABLE FILECHANGES IN EXCLUSIVE MODE");
        stm2.close();

        try {
            ps.executeQuery();
            fail();
        } catch (SQLException e) {
            assertSQLState("Expected timeout", "40XL1", e);
        }

        try {
            ps_inverse.executeQuery();
            fail();
        } catch (SQLException e) {
            assertSQLState("Expected timeout", "40XL1", e);
        }

        c3.rollback();
        c3.close();

        ResultSet rs = ps.executeQuery();
        JDBC.assertFullResultSet(rs, new String[][]{{"1", "1", "1"}});

        rs = ps_inverse.executeQuery();
        JDBC.assertFullResultSet(rs, new String[][]{{"1", "1", "1"}});

        ps.close();
        ps_inverse.close();

    }


    public void testDerby4330_SetOpResultSet()  throws SQLException {
        setTimeout(1);
        setSchema("APP");
        createDerby4330_union_tables();

        String[] ops = {"EXCEPT", "INTERSECT"};
        String[][][] opExpectedRs = {null, {{"1", "1", "1"}}};

        for (int i=0; i < 2; i++) {
            PreparedStatement ps = prepareStatement(
                "SELECT * FROM (" +
                "SELECT * FROM FILECHANGES_2 " + ops[i] + " " +
                // locked file last
                "SELECT * FROM FILECHANGES) X ORDER BY ID");

            PreparedStatement ps_inverse = prepareStatement(
                "SELECT * FROM (" +
                 // locked file first:
                "SELECT * FROM FILECHANGES " + ops[i] + " " +
                "SELECT * FROM FILECHANGES_2) X ORDER BY ID");

            c3 = openDefaultConnection();
            c3.setAutoCommit(false);
            Statement stm2 = c3.createStatement();
            stm2.execute("LOCK TABLE FILECHANGES IN EXCLUSIVE MODE");
            stm2.close();

            try {
                ps.executeQuery();
                fail();
            } catch (SQLException e) {
                assertSQLState("Expected timeout", "40XL1", e);
            }

            try {
                ps_inverse.executeQuery();
                fail();
            } catch (SQLException e) {
                assertSQLState("Expected timeout", "40XL1", e);
            }

            c3.rollback();
            c3.close();

            ResultSet rs = ps.executeQuery();

            if (opExpectedRs[i] != null) {
                JDBC.assertFullResultSet(rs, opExpectedRs[i]);
            } else {
                JDBC.assertEmpty(rs);
            }

            rs = ps_inverse.executeQuery();

            if (opExpectedRs[i] != null) {
                JDBC.assertFullResultSet(rs, opExpectedRs[i]);
            } else {
                JDBC.assertEmpty(rs);
            }

            ps.close();
            ps_inverse.close();

        }

    }


    public void testDerby4330_GroupedAggregateResultSet()  throws SQLException {
        setTimeout(1);
        setSchema("APP");
        createDerby4330_union_tables();

        PreparedStatement ps = prepareStatement(
            "SELECT SUM(CHANGESET) from FILECHANGES GROUP BY FILE");

        c3 = openDefaultConnection();
        c3.setAutoCommit(false);
        Statement stm2 = c3.createStatement();
        // Next statement gives an exclusive write lock on a row in FILECHANGES:
        stm2.execute("INSERT INTO FILECHANGES(FILE,CHANGESET) VALUES (2,2)");
        stm2.close();

        try {
            ps.executeQuery();
            fail();
        } catch (SQLException e) {
            assertSQLState("Expected timeout", "40XL1", e);
        }

        c3.rollback();
        c3.close();

        ResultSet rs = ps.executeQuery();
        JDBC.assertFullResultSet(rs, new String[][]{{"1"}});

        ps.close();
    }


    public void testDerby4330_DistinctGroupedAggregateResultSet()
            throws SQLException
    {
        setTimeout(1);
        setSchema("APP");
        createDerby4330_union_tables();

        PreparedStatement ps = prepareStatement(
            "SELECT SUM(DISTINCT CHANGESET) from FILECHANGES GROUP BY FILE");

        c3 = openDefaultConnection();
        c3.setAutoCommit(false);
        Statement stm2 = c3.createStatement();
        // Next statement gives an exclusive write lock on a row in FILECHANGES:
        stm2.execute("INSERT INTO FILECHANGES(FILE,CHANGESET) VALUES (2,2)");
        stm2.close();

        try {
            ps.executeQuery();
            fail();
        } catch (SQLException e) {
            assertSQLState("Expected timeout", "40XL1", e);
        }

        c3.rollback();
        c3.close();

        ResultSet rs = ps.executeQuery();
        JDBC.assertFullResultSet(rs, new String[][]{{"1"}});

        ps.close();
    }


    public void testDerby4330_DistinctScalarAggregateResultSet()
            throws SQLException
    {
        setTimeout(1);
        setSchema("APP");
        createDerby4330_union_tables();

        PreparedStatement ps = prepareStatement(
            "SELECT SUM(DISTINCT CHANGESET) from FILECHANGES");

        c3 = openDefaultConnection();
        c3.setAutoCommit(false);
        Statement stm2 = c3.createStatement();
        // Next statement gives an exclusive write lock on a row in FILECHANGES:
        stm2.execute("INSERT INTO FILECHANGES(FILE,CHANGESET) VALUES (2,2)");
        stm2.close();

        try {
            ps.executeQuery();
            fail();
        } catch (SQLException e) {
            assertSQLState("Expected timeout", "40XL1", e);
        }

        c3.rollback();
        c3.close();

        ResultSet rs = ps.executeQuery();
        JDBC.assertFullResultSet(rs, new String[][]{{"1"}});

        ps.close();
    }


    private void setTimeout(long t) throws SQLException {
        Statement stm = createStatement();
        stm.execute("call syscs_util.syscs_set_database_property(" +
                    "'derby.locks.waitTimeout', '" + t + "')");
        stm.close();
    }


    private void createDerby4330_join_tables()  throws SQLException {
        Statement stm = createStatement();
        stm.execute(
            "CREATE TABLE REPOSITORIES (" +
            "ID INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY," +
            "PATH VARCHAR(32672) UNIQUE NOT NULL)");

        stm.execute(
            "INSERT INTO REPOSITORIES(PATH) VALUES ('r')");

        stm.execute(
            "CREATE TABLE FILES (" +
            "ID INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY," +
            "PATH VARCHAR(32672) NOT NULL," +
            "REPOSITORY INT NOT NULL REFERENCES REPOSITORIES" +
            "    ON DELETE CASCADE," +
            "UNIQUE (REPOSITORY, PATH))");

        stm.execute(
            "INSERT INTO FILES(PATH, REPOSITORY) VALUES ('/adsf',1)");

        stm.execute(
            "CREATE TABLE AUTHORS (" +
            "ID INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY," +
            "REPOSITORY INT NOT NULL REFERENCES REPOSITORIES " +
            "           ON DELETE CASCADE," +
            "NAME VARCHAR(32672) NOT NULL," +
            "UNIQUE (REPOSITORY, NAME))");

        stm.execute(
            "INSERT INTO AUTHORS(REPOSITORY, NAME) VALUES (1, 'xyz')");

        stm.execute(
            "CREATE TABLE CHANGESETS (" +
            "ID INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY," +
            "REPOSITORY INT NOT NULL REFERENCES REPOSITORIES " +
            "           ON DELETE CASCADE," +
            "REVISION VARCHAR(1024) NOT NULL," +
            "AUTHOR INT NOT NULL REFERENCES AUTHORS ON DELETE CASCADE," +
            "TIME TIMESTAMP NOT NULL," +
            "MESSAGE VARCHAR(32672) NOT NULL," +
            "UNIQUE (REPOSITORY, REVISION))");

        stm.execute(
            "INSERT INTO CHANGESETS(REPOSITORY, REVISION, " +
            "                       AUTHOR, TIME, MESSAGE)" +
            " VALUES (1,'',1,CURRENT_TIMESTAMP,'')");

        stm.execute(
            "CREATE TABLE FILECHANGES (" +
            "ID INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY," +
            "FILE INT NOT NULL REFERENCES FILES ON DELETE CASCADE," +
            "CHANGESET INT NOT NULL REFERENCES CHANGESETS ON DELETE CASCADE," +
            "UNIQUE (FILE, CHANGESET))");

        stm.execute("INSERT INTO FILECHANGES(FILE,CHANGESET) VALUES (1,1)");
        stm.close();
        commit();
    }


    private void createDerby4330_union_tables()  throws SQLException {
        Statement stm = createStatement();
        stm.execute("CREATE TABLE FILECHANGES (" +
                    "ID INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY," +
                    "FILE INT NOT NULL," +
                    "CHANGESET INT NOT NULL," +
                    "UNIQUE (FILE, CHANGESET))");

        stm.execute("CREATE TABLE FILECHANGES_2 (" +
                    "ID INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY," +
                    "FILE INT NOT NULL," +
                    "CHANGESET INT NOT NULL," +
                    "UNIQUE (FILE, CHANGESET))");

        stm.execute("INSERT INTO FILECHANGES(FILE,CHANGESET) VALUES (1,1)");
        stm.execute("INSERT INTO FILECHANGES_2(FILE,CHANGESET) VALUES (1,1)");
        stm.close();
        commit();
    }


    private void setSchema(String schema) throws SQLException {
        Statement stm = createStatement();
        stm.execute("SET SCHEMA " + schema);
        stm.close();
    }
}
