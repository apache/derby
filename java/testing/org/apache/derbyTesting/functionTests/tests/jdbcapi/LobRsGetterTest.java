/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.LobRsGetterTest

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */
package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.io.IOException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;

import junit.framework.Test;

import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetReader;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetStream;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests restrictions and special conditions that apply when calling result set
 * getters on LOB columns.
 * <p>
 * Some relevant JIRAs:
 * <ul> <li>
 * <a href="https://issues.apache.org/jira/browse/DERBY-3844">DERBY-3844</a>
 *      : has references to other JIRAs and some discussion</li>
 *      <li>
 * <a href="https://issues.apache.org/jira/browse/DERBY-5489">DERBY-5489</a>
 *      : triggered the writing of this test and the following cleanup</li>
 * </ul>
 */
public class LobRsGetterTest
    extends BaseJDBCTestCase {

    private static final String TABLE = "LOB_RS_GETTER_TEST";
    private static final boolean UNSUPPORTED = false;
    private static final boolean SUPPORTED = true;

    private static final int BLOB = 0;
    private static final int CLOB = 1;

    private static final int GET_BYTES = 0;
    private static final int GET_STRING = 1;
    private static final int GET_ASCII_STREAM = 2;
    private static final int GET_BINARY_STREAM = 3;
    private static final int GET_CHARACTER_STREAM = 4;
    private static final int GET_CLOB = 5;
    private static final int GET_BLOB = 6;
    private static final int GET_OBJECT = 7;

    private static final boolean X = SUPPORTED;
    /** Supported, but not marked as valid by the JDBC spec. */
    private static final boolean E = SUPPORTED; // visual que in table only
    private static final boolean u = UNSUPPORTED;

    /**
     * Lists the compatible getters for {@literal BLOB} and {@literal CLOB}.
     * <p>
     * Note that the getters {@code getNCharacterStream}, {@code getNString}
     * and {@code getNClob} aren't tested. They're not supported by Derby.
     * <p>
     * For notes on behavior when invoking the various character getters on
     * binary columns, see
     * <a href="http://db.apache.org/derby/papers/JDBCImplementation.html">
     * JDBCImplementation.html</a>.
     */
    private static final boolean[][] COMPATIBLE_GETTERS = new boolean[][] {
                                /*   B  C */
                                /*   L  L */
                                /*   O  O */
                                /*   B  B */
        /* getBytes             */ { X, u },
        /* getString            */ { E, X },
        /* getAsciiStream       */ { E, X },
        /* getBinaryStream      */ { X, u },
        /* getCharacterStream   */ { E, X },
        /* getClob              */ { u, X },
        /* getBlob              */ { X, u },
        /* getObject            */ { X, X },
    };

    /**
     * The names of the various getters used in this test.
     * <p>
     * The positions/indexes must correspond to those in
     * {@linkplain #COMPATIBLE_GETTERS}.
     */
    private static final String[] GETTER_NAMES = new String[] {
        "getBytes", "getString", "getAsciiStream", "getBinaryStream",
        "getCharacterStream", "getClob", "getBlob", "getObject"
    };

    public LobRsGetterTest(String name) {
        super(name);
    }

    /** Returns a suite with all tests running with both embedded and client. */
    public static Test suite() {
        Test suite = TestConfiguration.defaultSuite(
                LobRsGetterTest.class, false);
        return new CleanDatabaseTestSetup(suite) {
             protected void decorateSQL(Statement s)
                     throws SQLException {
                 Connection con = s.getConnection();
                 dropTable(con, TABLE);
                 // NOTE: Do not insert a lot of data into this table, many
                 //       of the tests iterate over all rows in the table.
                 s.executeUpdate("create table " + TABLE + "(" +
                         "id INT GENERATED ALWAYS AS IDENTITY, " +
                         "dBlob BLOB, dClob CLOB)");
                 // Insert a few rows with different characteristics:
                 // multi page LOB, single page LOB, NULL
                 PreparedStatement ps = con.prepareStatement(
                         "insert into " + TABLE +
                         "(dBlob, dClob) values (?,?)");
                 int mpSize = 173*1024; // 173 KB or KChars
                 int spSize = 300; // 300 B or chars
                 ps.setBinaryStream(1,
                         new LoopingAlphabetStream(mpSize), mpSize);
                 ps.setCharacterStream(2,
                         new LoopingAlphabetReader(mpSize), mpSize);
                 ps.executeUpdate();
                 ps.setBinaryStream(1,
                         new LoopingAlphabetStream(spSize), spSize);
                 ps.setCharacterStream(2,
                         new LoopingAlphabetReader(spSize), spSize);
                 ps.executeUpdate();
                 ps.setNull(1, Types.BLOB);
                 ps.setNull(2, Types.CLOB);
                 ps.executeUpdate();
                 // Make sure there are three rows.
                 JDBC.assertDrainResults(
                         s.executeQuery("select * from " + TABLE), 3);
                 ps.close();
                 s.close();
             }
        };
    }

    /**
     * Tests that all getters marked as supported don't throw an exception.
     */
    public void testBlobGettersSimple()
            throws SQLException {
        _testGettersSimple("dBlob", BLOB);
    }

    /**
     * Tests that all getters marked as supported don't throw an exception.
     */
    public void testClobGettersSimple()
            throws SQLException {
        _testGettersSimple("dClob", CLOB);
    }

    /**
     * Tests that all getters marked as unsupported throw an exception.
     */
    public void testBlobGettersSimpleNegative()
            throws SQLException {
        _testGettersSimpleNegative("dBlob", BLOB);
    }

    /**
     * Tests that all getters marked as unsupported throw an exception.
     */
    public void testClobGettersSimpleNegative()
            throws SQLException {
        _testGettersSimpleNegative("dClob", CLOB);
    }

    /**
     * Tests that multiple invocations of getters on the same column/row throw
     * an exception in most cases.
     * <p>
     * For now {@code getBytes} and {@code getString} are behaving differently
     * for BLOBs.
     */
    public void testBlobGettersMultiInvocation()
            throws SQLException {
        _testGettersMultiInvocation("dBlob", BLOB);
    }

    /**
     * Tests that multiple invocations of getters on the same column/row throw
     * an exception in most cases.
     * <p>
     * For now {@code getString} is behaving differently for CLOBs.
     */
    public void testClobGettersMultiInvocation()
            throws SQLException {
        _testGettersMultiInvocation("dClob", CLOB);
    }

    /**
     * Tests that {@code getBytes} throws exception when invoked after any
     * other getters than {@code getBytes} and {@code getString}.
     */
    public void testBlobGetXFollowedByGetBytes()
            throws SQLException {
        PreparedStatement ps = prepareStatement(
                "select dBlob from " + TABLE);
        for (int getter=0; getter < COMPATIBLE_GETTERS.length; getter++) {
            if (COMPATIBLE_GETTERS[getter][BLOB] == UNSUPPORTED) {
                continue;
            }
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            invokeGetter(rs, BLOB, getter);
            // The next call should fail unless the getter is getBytes.
            try {
                invokeGetter(rs, BLOB, GET_BYTES);
                assertTrue("getBytes should have failed after: " +
                        debugInfo(1, rs, BLOB, getter),
                        getter == GET_BYTES || getter == GET_STRING);
            } catch (SQLException sqle) {
                assertTrue(getter != GET_BYTES);
                assertSQLState(debugInfo(1, rs, BLOB, getter) +
                        " followed by getBytes", "XCL18", sqle);
            } finally {
                rs.close();
            }
        }
    }

    /**
     * Tests that {@code getString} throws exception when invoked after any
     * other getter than {@code getString}.
     */
    public void testClobGetXFollowedByGetString()
            throws SQLException {
        PreparedStatement ps = prepareStatement(
                "select dClob from " + TABLE);
        for (int getter=0; getter < COMPATIBLE_GETTERS.length; getter++) {
            if (COMPATIBLE_GETTERS[getter][CLOB] == UNSUPPORTED) {
                continue;
            }
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            invokeGetter(rs, CLOB, getter);
            // The next call should fail unless the getter is getString.
            try {
                invokeGetter(rs, CLOB, GET_STRING);
                assertTrue("getString should have failed after: " +
                        debugInfo(1, rs, CLOB, getter),
                        getter == GET_STRING);
            } catch (SQLException sqle) {
                assertTrue(getter != GET_STRING);
                assertSQLState(debugInfo(1, rs, CLOB, getter) +
                        " followed by getString", "XCL18", sqle);
            } finally {
                rs.close();
            }
        }
    }

    /**
     * Tests that data returned by the last BLOB getter invokation is correct.
     */
    public void testCorrectBlobDataWithMultiCall()
            throws IOException, SQLException {
        setAutoCommit(false);
        PreparedStatement psId = prepareStatement("select id from " + TABLE);
        String select = "select dBlob from " + TABLE + " where id = ?";
        PreparedStatement ps1 = prepareStatement(select);
        PreparedStatement ps2 = prepareStatement(select);
        ResultSet rsId = psId.executeQuery();
        ResultSet rs1;
        ResultSet rs2;
        while (rsId.next()) {
            ps1.setInt(1, rsId.getInt(1));
            ps2.setInt(1, rsId.getInt(1));

            // getBytes - getString - getBinaryStream
            rs1 = ps1.executeQuery();
            assertTrue(rs1.next());
            rs1.getBytes(1);
            rs1.getString(1);
            rs2 = ps2.executeQuery();
            assertTrue(rs2.next());
            assertEquals(rs2.getBinaryStream(1), rs1.getBinaryStream(1));
            rs1.close();
            rs2.close();

            // getString - getBytes - getBlob
            rs1 = ps1.executeQuery();
            assertTrue(rs1.next());
            rs1.getString(1);
            rs1.getBytes(1);
            rs2 = ps2.executeQuery();
            assertTrue(rs2.next());
            assertEquals(rs2.getBlob(1), rs1.getBlob(1));
            rs1.close();
            rs2.close();

            // getBytes - getString - getCharacterStream
            rs1 = ps1.executeQuery();
            assertTrue(rs1.next());
            rs1.getBytes(1);
            rs1.getString(1);
            rs2 = ps2.executeQuery();
            assertTrue(rs2.next());
            assertEquals(rs2.getCharacterStream(1), rs1.getCharacterStream(1));
            rs1.close();
            rs2.close();

            // getBytes - getString - getBytes
            rs1 = ps1.executeQuery();
            assertTrue(rs1.next());
            rs1.getBytes(1);
            rs1.getString(1);
            rs2 = ps2.executeQuery();
            assertTrue(rs2.next());
            assertTrue(Arrays.equals(rs2.getBytes(1), rs1.getBytes(1)));
            rs1.close();
            rs2.close();

            // getBytes - getString - getString
            rs1 = ps1.executeQuery();
            assertTrue(rs1.next());
            rs1.getBytes(1);
            rs1.getString(1);
            rs2 = ps2.executeQuery();
            assertTrue(rs2.next());
            assertEquals(rs2.getString(1), rs1.getString(1));
            rs1.close();
            rs2.close();

            // getString - getBytes - getObject
            rs1 = ps1.executeQuery();
            assertTrue(rs1.next());
            rs1.getString(1);
            rs1.getBytes(1);
            rs2 = ps2.executeQuery();
            assertTrue(rs2.next());
            Blob b1 = (Blob)rs1.getObject(1);
            Blob b2 = (Blob)rs2.getObject(1);
            assertEquals(b2, b1);
            rs1.close();
            rs2.close();

            // getBytes - getString - getAsciiStream
            rs1 = ps1.executeQuery();
            assertTrue(rs1.next());
            rs1.getBytes(1);
            rs1.getString(1);
            rs2 = ps2.executeQuery();
            assertTrue(rs2.next());
            assertEquals(rs2.getAsciiStream(1), rs1.getAsciiStream(1));
            rs1.close();
            rs2.close();
        }
        rollback();
    }

    /**
     * Tests that data returned by the last CLOB getter invokation is correct.
     */
    public void testCorrectClobDataWithMultiCall()
            throws IOException, SQLException {
        setAutoCommit(false);
        PreparedStatement psId = prepareStatement(
                "select id, dClob from " + TABLE);
        String select = "select dClob from " + TABLE + " where id = ?";
        PreparedStatement ps1 = prepareStatement(select);
        PreparedStatement ps2 = prepareStatement(select);
        ResultSet rsId = psId.executeQuery();
        ResultSet rs1;
        ResultSet rs2;
        while (rsId.next()) {
            ps1.setInt(1, rsId.getInt(1));
            ps2.setInt(1, rsId.getInt(1));

            // getString - getString
            rs1 = ps1.executeQuery();
            assertTrue(rs1.next());
            rs1.getString(1);
            rs2 = ps2.executeQuery();
            assertTrue(rs2.next());
            assertEquals(rs2.getString(1), rs1.getString(1));
            rs1.close();
            rs2.close();

            // getString - getCharacterStream
            rs1 = ps1.executeQuery();
            assertTrue(rs1.next());
            rs1.getString(1);
            rs2 = ps2.executeQuery();
            assertTrue(rs2.next());
            assertEquals(rs2.getCharacterStream(1), rs1.getCharacterStream(1));
            rs1.close();
            rs2.close();

            // getString - getClob
            rs1 = ps1.executeQuery();
            assertTrue(rs1.next());
            rs1.getString(1);
            rs2 = ps2.executeQuery();
            assertTrue(rs2.next());
            assertEquals(rs2.getClob(1), rs1.getClob(1));
            rs1.close();
            rs2.close();

            // getString - getObject
            rs1 = ps1.executeQuery();
            assertTrue(rs1.next());
            rs1.getString(1);
            rs2 = ps2.executeQuery();
            assertTrue(rs2.next());
            Clob b1 = (Clob)rs1.getObject(1);
            Clob b2 = (Clob)rs2.getObject(1);
            assertEquals(b2, b1);
            rs1.close();
            rs2.close();

            // getString - getAsciiStream
            rs1 = ps1.executeQuery();
            assertTrue(rs1.next());
            rs1.getString(1);
            rs2 = ps2.executeQuery();
            assertTrue(rs2.next());
            assertEquals(rs2.getAsciiStream(1), rs1.getAsciiStream(1));
            rs1.close();
            rs2.close();
        }
        rollback();
    }

    private void _testGettersMultiInvocation(String columnName, int typeIdx)
            throws SQLException {
        PreparedStatement ps = prepareStatement(
                "select " + columnName + " from " + TABLE);
        for (int getter=0; getter < COMPATIBLE_GETTERS.length; getter++) {
            boolean supported = COMPATIBLE_GETTERS[getter][typeIdx];
            if (!supported) {
                continue;
            }
            ResultSet rs = ps.executeQuery();
            rs.next();
            invokeGetter(rs, typeIdx, getter);
            try {
                invokeGetter(rs, typeIdx, getter);
                if (getter != GET_BYTES && getter != GET_STRING) {
                    fail("calling the getter twice should have failed: " +
                        GETTER_NAMES[getter] + " on " + typeName(typeIdx));
                }
            } catch (SQLException sqle) {
                assertSQLState("XCL18", sqle);
            }
        }
    }

    private void _testGettersSimpleNegative(String columnName, int typeIdx)
            throws SQLException {
        PreparedStatement ps = prepareStatement(
                "select " + columnName + " from " + TABLE);
        for (int getter=0; getter < COMPATIBLE_GETTERS.length; getter++) {
            boolean supported = COMPATIBLE_GETTERS[getter][typeIdx];
            if (supported) {
                continue;
            }
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                try {
                    invokeGetter(rs, typeIdx, getter);
                    if (!rs.wasNull()) {
                        fail("expected getter to fail on non-NULL value: " +
                                debugInfo(1, rs, typeIdx, getter));
                    }
                } catch (SQLException sqle) {
                    assertSQLState("22005", sqle);
                }
            }
            rs.close();
        }
    }

    private void _testGettersSimple(String columnName, int typeIdx)
            throws SQLException {
        PreparedStatement ps = prepareStatement(
                "select " + columnName + " from " + TABLE);
        for (int getter=0; getter < COMPATIBLE_GETTERS.length; getter++) {
            boolean supported = COMPATIBLE_GETTERS[getter][typeIdx];
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                try {
                    invokeGetter(rs, typeIdx, getter);
                    // We check for NULL before invoking the getter, so an
                    // exception won't be thrown when invoking an unsupported
                    // getter when the field value is NULL.
                    if (!supported) {
                        assertTrue("should not have worked: " +
                                    debugInfo( 1, rs, typeIdx, getter),
                                rs.wasNull());
                    }
                } catch (SQLException sqle) {
                    if (!supported) {
                        assertSQLState("22005", sqle);
                    } else {
                        throw sqle;
                    }
                }
            }
            rs.close();
        }
        ps.close();
    }

    /** Invokes the specified getter on column 1 of the result set. */
    private void invokeGetter(ResultSet rs, int typeIdx, int getter)
            throws SQLException {
        invokeGetter(1, rs, typeIdx, getter);
    }

    /** Invokes the specified getter on the given result set 1-based index. */
    private void invokeGetter(int column, ResultSet rs, int typeIdx, int getter)
            throws SQLException {
        println("invoking " + debugInfo(column, rs, typeIdx, getter));
        Object ret;
        switch (getter) {
            case GET_BYTES:
                ret = rs.getBytes(column);
                break;
            case GET_STRING:
                ret = rs.getString(column);
                break;
            case GET_ASCII_STREAM:
                ret = rs.getAsciiStream(column);
                break;
            case GET_BINARY_STREAM:
                ret = rs.getBinaryStream(column);
                break;
            case GET_CHARACTER_STREAM:
                ret = rs.getCharacterStream(column);
                break;
            case GET_CLOB:
                ret = rs.getClob(column);
                break;
            case GET_BLOB:
                ret = rs.getBlob(column);
                break;
            case GET_OBJECT:
                ret = rs.getObject(column);
                break;
            default:
                fail("unsupported getter index: " + getter);
                // Help the compiler a little.
                throw new IllegalStateException();
        }
        if (rs.wasNull()) {
            assertNull(ret);
        } else {
            assertNotNull(ret);
        }
    }

    /** Returns the type name of the given test class specific type index. */
    private String typeName(int typeIdx) {
        switch (typeIdx) {
            case BLOB:
                return "BLOB";
            case CLOB:
                return  "CLOB";
            default:
                return "UNKNOWN";
        }
    }

    /**
     * Generates a debug string telling which getter failed.
     *
     * @param colIdx 1-based column index accessed on the result set
     * @param rs the result set accessed
     * @param typeIdx test class specific type (BLOB or CLOB)
     * @param getter test class specific getter index
     * @return Descriptive string.
     * @throws SQLException if accessing result set meta data fails
     */
    private String debugInfo(int colIdx, ResultSet rs, int typeIdx, int getter)
            throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        return GETTER_NAMES[getter] + " on " + typeName(typeIdx) +
                " (meta:col=" + colIdx +
                ",type=" + meta.getColumnTypeName(colIdx) +
                ",name=" + meta.getColumnName(colIdx) + ")";
    }
}
