/*
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.CallableTest
   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at
  
     http://www.apache.org/licenses/LICENSE-2.0
  
   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.
 */

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.BatchUpdateException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;   // Used by testUpdateLongBinaryProc
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test the CallableStatement interface. 
 * <p>
 * This test converts the old derbynet/callable.java test to JUnit. It 
 * exercises the CallableStatement interface with various combinations of
 *
 *   IN, OUT and INOUT registered parameters,
 *   SQL functions and procedures, and
 *   different data types.
 */
public class CallableTest extends BaseJDBCTestCase {

    /**
     * Routines that should be created before the tests are run and
     * dropped when the tests have finished. 
     */
    private static final String[] ROUTINES = {
       
        "CREATE PROCEDURE TWO_IN_ONE_OUT_PROC " +
        "(IN P1 INT, IN P2 INT, OUT P3 INT) " +
        "NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '" +
        CallableTest.class.getName() + ".twoInOneOutProc'",

        "CREATE FUNCTION ONE_IN_ONE_OUT_FUNC (P1 INT) RETURNS INT " +
        "NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '" +
        CallableTest.class.getName() + ".oneInOneOutFunc'",

        "CREATE FUNCTION NO_IN_ONE_OUT_FUNC() RETURNS INT " +
        "NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '" +
        CallableTest.class.getName() + ".noInOneOutFunc'",

        "CREATE PROCEDURE SYSTEM_OUT_PRINTLN_PROC() " +
        "NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '" +
        CallableTest.class.getName() + ".systemOutPrintlnProc'",

        "CREATE PROCEDURE UPDATE_LONGVARBINARY_PROC " +
        "(P1 VARCHAR(10000) FOR BIT DATA) MODIFIES SQL DATA " +
        "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '" +
        CallableTest.class.getName() + ".updateLongVarbinaryProc'",

        "CREATE PROCEDURE NUMERIC_BOUNDARIES_PROC " +
        "(OUT P1 DECIMAL(31,15), OUT P2 DECIMAL(31,15), " +
        "OUT P3 DECIMAL(31,15)) " +
        "READS SQL DATA LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '" +
        CallableTest.class.getName() + ".numericBoundariesProc'",

        "CREATE PROCEDURE NUMERIC_TYPES_IN_AND_OUT_PROC " +
        "(IN P1 SMALLINT, IN P2 INT, IN P3 BIGINT, " +
        "IN P4 REAL, IN P5 DOUBLE, IN P6 DECIMAL(6,3), " +
        "OUT O1 SMALLINT, OUT O2 INT, OUT O3 BIGINT, " +
        "OUT O4 REAL, OUT O5 DOUBLE, OUT O6 DECIMAL(6,3) ) " +
        "NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '" +
        CallableTest.class.getName() + ".manyTypesInAndOutProc'",

        "CREATE PROCEDURE NON_NUMERIC_TYPES_IN_AND_OUT_PROC " +
        "(IN P1 DATE, IN P2 TIME, IN P3 TIMESTAMP, " +
        "IN P4 VARCHAR(20) FOR BIT DATA, " +
        "OUT O1 DATE, OUT O2 TIME, OUT O3 TIMESTAMP, " +
        "OUT O4 VARCHAR(20) FOR BIT DATA) " +
        "NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '" +
        CallableTest.class.getName() + ".manyTypesInAndOutProc'",

        "CREATE PROCEDURE MANY_TYPES_INOUT_PROC " +
        "(IN P1 SMALLINT, INOUT P2 SMALLINT, IN P3 INT, INOUT P4 INT, " +
        "IN P5 BIGINT, INOUT P6 BIGINT, IN P7 REAL, INOUT P8 REAL, " +
        "IN P9 DOUBLE, INOUT P10 DOUBLE, IN P11 TIME, INOUT P12 TIME) " +
        "NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '" +
        CallableTest.class.getName() + ".manyTypesInoutProc'",

        "CREATE PROCEDURE BIGDECIMAL_IN_AND_OUT_PROC " +
        "(IN P1 DECIMAL(14,4), OUT P2 DECIMAL(14,4), " +
        "IN P3 DECIMAL(14,4), OUT P4 DECIMAL(14,4), " +
        "OUT P5 DECIMAL(14,4), OUT P6 DECIMAL(14,4), " +
        "OUT P7 DECIMAL(14,4), OUT P8 DECIMAL(14,4), " +
        "OUT P9 DECIMAL(14,4)) EXTERNAL NAME '" + 
        CallableTest.class.getName() + ".bigDecimalInAndOutProc' " +
        "NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA",
//IC see: https://issues.apache.org/jira/browse/DERBY-2304

        "CREATE PROCEDURE BATCH_UPDATE_PROC " +
        "(P1 INT, P2 INT) MODIFIES SQL DATA " +
        "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '" +
        CallableTest.class.getName() + ".batchUpdateProc'"
    };

    /**
     * Tables that should be created before the tests are run and
     * dropped when the tests have finished. The first element in each row
     * is the name of the table and the second element is the SQL text that
     * creates it.
     */
    private static final String[][] TABLES = {
        // LONGVARBINARY_TABLE is used by UPDATE_LONGVARBINARY_PROC
        { "LONGVARBINARY_TABLE", 
          "CREATE TABLE LONGVARBINARY_TABLE (lvbc Long varchar for bit data)" },

        // NUMERIC_BOUNDARIES_TABLE is used by NUMERIC_BOUNDARIES_PROC
        { "NUMERIC_BOUNDARIES_TABLE", 
          "CREATE TABLE NUMERIC_BOUNDARIES_TABLE " +
          "(maxcol NUMERIC(31,15), mincol NUMERIC(15,15), nulcol NUMERIC)"},

        // BATCH_UPDATE is used by BATCH_UPDATE_PROC
//IC see: https://issues.apache.org/jira/browse/DERBY-2304
        { "BATCH_TABLE", 
          "CREATE TABLE BATCH_TABLE " +
          "(id int, tag varchar(32), " +
          "idval int constraint idval_ck check (idval >= 0))"},
    };

    /**
     * Creates a new <code>CallableTest</code> instance.
     *
     * @param name name of the test
     */
    public CallableTest(String name) {
        super(name);
    }

    public static Test suite() {
        BaseTestSuite suite = new BaseTestSuite("CallableTest");
//IC see: https://issues.apache.org/jira/browse/DERBY-6590

        suite.addTest(baseSuite("CallableTest:embedded"));

        suite.addTest(TestConfiguration.clientServerDecorator(
            baseSuite("CallableTest:client")));

        // Test with ConnectionPoolDataSource on client in order to exercise
        // LogicalCallableStatement (DERBY-5871).
        suite.addTest(TestConfiguration.clientServerDecorator(
            TestConfiguration.connectionCPDecorator(
                baseSuite("CallableTest:logical"))));

        // Test with XADataSource on embedded in order to exercise
        // BrokeredCallableStatement (DERBY-5854).
        suite.addTest(TestConfiguration.connectionXADecorator(
            baseSuite("CallableTest:brokered")));

        return suite;
    }

    private static Test baseSuite(String name) {

        BaseTestSuite suite = new BaseTestSuite(name);
//IC see: https://issues.apache.org/jira/browse/DERBY-6590

        // Add tests that every JVM should be able to run.
        suite.addTestSuite(CallableTest.class);

        // Add tests that require JDBC 3 
        if (JDBC.vmSupportsJDBC3()) {

            // Tests that require DriverManager.
            suite.addTest
                (new CallableTest("xtestUpdateLongBinaryProc"));

            // Tests that require DriverManager and batch update.
//IC see: https://issues.apache.org/jira/browse/DERBY-2304
            suite.addTest
                (new CallableTest("xtestBatchUpdate"));
            suite.addTest
                (new CallableTest("xtestBatchUpdateError"));

            // Tests that get/set BigDecimal
            suite.addTest
                (new CallableTest("xtestBigDecimalInAndOutProc"));
            suite.addTest
                (new CallableTest("xtestNumericTypesInAndOutProc"));

            // Test that both requires DriverManager and BigDecimal
            suite.addTest
                (new CallableTest("xtestNumericBoundariesProc"));
        }


        return new CleanDatabaseTestSetup(suite) 
        {
            /**
            * Creates the tables and the stored procedures used in the test
            * cases.
            * @throws SQLException 
            */
            protected void decorateSQL(Statement s) throws SQLException
            {
                for (int i = 0; i < ROUTINES.length; i++) {
                    s.execute(ROUTINES[i]);
                }
                for (int i = 0; i < TABLES.length; i++) {
                    s.execute(TABLES[i][1]);
                }
            }
        };
    } // End baseSuite

    /**
     * Sets up the connection for a test case and clears all tables
     * used in the test cases.
     * @throws SQLException 
     */
    public void setUp() throws SQLException
    {
        Connection conn = getConnection();
        conn.setAutoCommit(false);
        Statement s = createStatement();
        for (int i = 0; i < TABLES.length; i++) {
            s.execute("DELETE FROM " + TABLES[i][0]);
        }
        s.close();
        conn.commit();
    }

    // TESTS

    /**
     * Calls a SQL procedure with two input parameters and one output.
     * @throws SQLException 
     */
    public void testTwoInOneOutProc() throws SQLException
    {
        CallableStatement cs = prepareCall
            ("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
        cs.setInt(1, 6);
        cs.setInt(2, 9);
        cs.registerOutParameter (3, java.sql.Types.INTEGER);
        cs.execute();
        assertEquals("Sum of 6 and 9", 15, cs.getInt(3));

    }

    /**
     * Calls a SQL function with one input parameter and one output.
     * @throws SQLException 
     */
    public void testOneInOneOutFunc() throws SQLException
    {
        CallableStatement cs = prepareCall
            ("? = call ONE_IN_ONE_OUT_FUNC (?)");
        cs.registerOutParameter (1, java.sql.Types.INTEGER);
        cs.setInt(2, 6);
        cs.execute();
        assertEquals("Square of 6 then plus 6", 42, cs.getInt(1));
    }

    /**
     * Calls a SQL function that takes no input parameter and returns one 
     * output.
     * @throws SQLException 
     */
    public void testNoInOneOutFunc() throws SQLException
    {
        CallableStatement cs = prepareCall
            ("? = call NO_IN_ONE_OUT_FUNC()");
        cs.registerOutParameter (1, java.sql.Types.INTEGER);
        cs.execute();
        assertEquals("NO_IN_ONE_OUT_FUNC output value", 55, cs.getInt(1));

    }

    public void testIsolationLevelChangeAfterFunctionCall()
//IC see: https://issues.apache.org/jira/browse/DERBY-3496
            throws SQLException {
        CallableStatement cs = prepareCall("? = CALL NO_IN_ONE_OUT_FUNC()");
        cs.registerOutParameter(1, java.sql.Types.INTEGER);
        cs.execute();
        assertEquals(55, cs.getInt(1));
        getConnection().setTransactionIsolation(
            Connection.TRANSACTION_SERIALIZABLE);
    }

    /**
     * Calls a SQL procedure that outputs a message with System.out.println.
     * Converted from the original test, but initially disabled because of the
     * message output to system out. Easily enabled by changing method name to
     * remove the initial "norun_" (the name becomes testSystemOutPrintlnProc).
     * @throws SQLException 
     */
    public void norun_testSystemOutPrintlnProc() throws SQLException
    {
        CallableStatement cs = prepareCall
            ("call SYSTEM_OUT_PRINTLN_PROC()");
        cs.execute();
        cs.close();
    }

    /**
     * Calls a SQL procedure that takes numeric IN and OUT parameters.
     * Excluded from JSR169/j2ME, which doesn't support get/set BigDecimal yet.
     * @throws SQLException 
     */
    public void xtestNumericTypesInAndOutProc() throws SQLException
    {
        CallableStatement cs = prepareCall
            ("call NUMERIC_TYPES_IN_AND_OUT_PROC(?,?,?,?,?,?,?,?,?,?,?,?)");

        cs.setShort(1, (short) 3);
        cs.setInt(2, 4);
        cs.setLong(3, 5);
        cs.setFloat(4, (float) 6.0);
        cs.setDouble(5, 7.0);
        cs.setBigDecimal(6, new BigDecimal("88.88"));

        cs.registerOutParameter (7, java.sql.Types.SMALLINT);
        cs.registerOutParameter (8, java.sql.Types.INTEGER);
        cs.registerOutParameter (9, java.sql.Types.BIGINT);
        cs.registerOutParameter (10, java.sql.Types.REAL);
        cs.registerOutParameter (11, java.sql.Types.DOUBLE);
        cs.registerOutParameter (12, java.sql.Types.DECIMAL);

        cs.execute();

        assertEquals("OUT short", (short) 3, cs.getShort(7));
        assertEquals("OUT int"  , 4, cs.getInt(8));
        assertEquals("OUT long" , 5, cs.getLong(9));
        assertEquals("OUT float" , (float) 6.0, cs.getFloat(10), .0001);
        assertEquals("OUT double" , 7.0, cs.getDouble(11), .0001);
        assertDecimalSameValue("OUT decimal", "88.88", cs.getBigDecimal(12));

        // test that setObject() does the right thing for BigDecimal. see derby-5488.
        cs.setObject(3, new BigDecimal( "10" ) );
        cs.execute();
        assertEquals("OUT long" , 10, cs.getLong(9));

        // test that setObject() does the right thing for BigInteger. see derby-5488.
        cs.setObject(3, new BigInteger( "11" ) );
        cs.execute();
        assertEquals("OUT long" , 11, cs.getLong(9));
    }

    /**
     * Calls a SQL procedure that takes non-numeric IN and OUT parameters.
     * @throws SQLException 
     */
    public void testNonNumericTypesInAndOutProc() throws SQLException
    {
        CallableStatement cs = prepareCall
            ("call NON_NUMERIC_TYPES_IN_AND_OUT_PROC(?,?,?,?,?,?,?,?)");

        cs.setDate(1, Date.valueOf("2002-05-12"));
        cs.setTime(2, Time.valueOf("10:05:02"));
        cs.setTimestamp(3, Timestamp.valueOf("2002-05-12 10:05:02.000000000"));
        byte[] ba = new byte[2];
        ba[0] = 1;
        ba[1] = 2;
        cs.setBytes(4, ba);

        cs.registerOutParameter (5, java.sql.Types.DATE);
        cs.registerOutParameter (6, java.sql.Types.TIME);
        cs.registerOutParameter (7, java.sql.Types.TIMESTAMP);
        cs.registerOutParameter (8, java.sql.Types.VARBINARY);

        cs.execute();

        assertEquals("OUT date", Date.valueOf("2002-05-12"), cs.getDate(5));
        assertEquals("OUT time"  , Time.valueOf("10:05:02"), cs.getTime(6));
        assertEquals("OUT timestamp" , 
            Timestamp.valueOf("2002-05-12 10:05:02.000000000"), 
            cs.getTimestamp(7));
        assertTrue(Arrays.equals(ba, cs.getBytes(8)));
    }

    /**
     * Test that the getters and setters for Date, Time and Timestamp work as
     * expected when given a Calendar argument. Test case for DERBY-4615.
     */
    public void testTimeAndDateWithCalendar() throws SQLException {
        // Create calendars for some time zones to use when testing the
        // setter methods.
        Calendar[] cal1 = {
            Calendar.getInstance(), // local calendar
            Calendar.getInstance(TimeZone.getTimeZone("GMT")),
            Calendar.getInstance(TimeZone.getTimeZone("Europe/Oslo")),
            Calendar.getInstance(TimeZone.getTimeZone("Asia/Hong_Kong")),
        };

        // Use calendars for the same time zones in the getters, but create
        // clones so that we don't get interference between the calendars.
        Calendar[] cal2 = (Calendar[]) cal1.clone();
        for (int i = 0; i < cal2.length; i++) {
            cal2[i] = (Calendar) cal2[i].clone();
        }

        // Now test all the combinations.
        for (int i = 0; i < cal1.length; i++) {
            for (int j = 0; j < cal2.length; j++) {
                testTimeAndDateWithCalendar(cal1[i], cal2[j]);
            }
        }
    }

    /**
     * Private helper for {@link #testTimeAndDateWithCalendar()}. This method
     * calls a procedure that takes Date, Time and Timestamp arguments and
     * returns the exact same values. Call the setters with one calendar and
     * the getters with another calendar, and verify that the expected
     * conversion between time zones has happened.
     *
     * @param cal1 the calendar to use for the setter methods
     * @param cal2 the calendar to use for the getter methods
     */
    private void testTimeAndDateWithCalendar(Calendar cal1, Calendar cal2)
            throws SQLException
    {
        println("Running " + getName() + "() with " +
                cal1.getTimeZone().getDisplayName() + " and " +
                cal2.getTimeZone().getDisplayName());

        CallableStatement cs = prepareCall(
                "call NON_NUMERIC_TYPES_IN_AND_OUT_PROC(?,?,?,?,?,?,?,?)");

        Date d = Date.valueOf("2010-04-14");
        Time t = Time.valueOf("12:23:24");
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        ts.setNanos(123456789);

        cs.setDate(1, d, cal1);
        cs.setTime(2, t, cal1);
        cs.setTimestamp(3, ts, cal1);
        cs.setNull(4, Types.VARBINARY); // we don't care about VARBINARY here

        cs.registerOutParameter (5, java.sql.Types.DATE);
        cs.registerOutParameter (6, java.sql.Types.TIME);
        cs.registerOutParameter (7, java.sql.Types.TIMESTAMP);
        cs.registerOutParameter (8, java.sql.Types.VARBINARY);

        cs.execute();

        assertSameDate(d, cal1, cs.getDate(5, cal2), cal2);
        assertSameTime(t, cal1, cs.getTime(6, cal2), cal2);
//IC see: https://issues.apache.org/jira/browse/DERBY-5172
        vetTimestamp(ts, cal1, cs.getTimestamp(7, cal2), cal2);
    }

    /**
     * Assert that two {@code java.util.Date} values have the same
     * representation of their date components (year, month and day) in their
     * respective time zones.
     *
     * @param expected the expected date
     * @param cal1 a calendar representing the time zone of the expected date
     * @param actual the actual date
     * @param cal2 a calendar representing the time zone of the actual date
     */
    private void assertSameDate(java.util.Date expected, Calendar cal1,
                                java.util.Date actual, Calendar cal2) {
        cal1.clear();
        cal1.setTime(expected);
        int expectedYear = cal1.get(Calendar.YEAR);
        int expectedMonth = cal1.get(Calendar.MONTH);
        int expectedDay = cal1.get(Calendar.DAY_OF_MONTH);

        cal2.clear();
        cal2.setTime(actual);
        assertEquals("year", expectedYear, cal2.get(Calendar.YEAR));
        assertEquals("month", expectedMonth, cal2.get(Calendar.MONTH));
        assertEquals("day", expectedDay, cal2.get(Calendar.DAY_OF_MONTH));
    }

    /**
     * Assert that two {@code java.util.Date} values have the same
     * representation of their time components (hour, minute, second) in their
     * respective time zones.
     *
     * @param expected the expected time
     * @param cal1 a calendar representing the time zone of the expected time
     * @param actual the actual time
     * @param cal2 a calendar representing the time zone of the actual time
     */
    private void assertSameTime(java.util.Date expected, Calendar cal1,
                                java.util.Date actual, Calendar cal2) {
        cal1.clear();
        cal1.setTime(expected);
        int expectedHour = cal1.get(Calendar.HOUR_OF_DAY);
        int expectedMinute = cal1.get(Calendar.MINUTE);
        int expectedSecond = cal1.get(Calendar.SECOND);

        cal2.clear();
        cal2.setTime(actual);
        assertEquals("hour", expectedHour, cal2.get(Calendar.HOUR_OF_DAY));
        assertEquals("minute", expectedMinute, cal2.get(Calendar.MINUTE));
        assertEquals("second", expectedSecond, cal2.get(Calendar.SECOND));
    }

    /**
     * Check that a {@code Timestamp} value is as expected when it has been
     * passed to a stored procedure and read back again from that procedure,
     * using different calendars for the {@code setTimestamp()} and
     * {@code getTimestamp()} calls.
     *
     * @param original the original timestamp that was passed to the procedure
     * @param cal1 the calendar object passed to {@code setTimestamp()} before
     * calling the procedure
     * @param returned the timestamp returned from the procedure
     * @param cal2 the calendar object passed to {@code getTimestamp()} when
     * reading the value returned by the procedure
     */
    private void vetTimestamp(Timestamp original, Calendar cal1,
                              Timestamp returned, Calendar cal2) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5172

        // Initialize cal1 with values from the original timestamp.
        cal1.clear();
        cal1.setTime(original);

        // The stored procedure itself doesn't have any knowledge about the
        // calendar passed to setTimestamp() or getTimestamp(), so it will
        // see the timestamp in the local timezone. Find out what it looks
        // like in this intermediate state.
        Calendar intermediate = Calendar.getInstance();
        intermediate.set(cal1.get(Calendar.YEAR),
                cal1.get(Calendar.MONTH),
                cal1.get(Calendar.DATE),
                cal1.get(Calendar.HOUR_OF_DAY),
                cal1.get(Calendar.MINUTE),
                cal1.get(Calendar.SECOND));

        // The returned timestamp will be based on the values in the
        // intermediate representation, but using the calendar passed to
        // getTimestamp().
        cal2.clear();
        cal2.set(intermediate.get(Calendar.YEAR),
                intermediate.get(Calendar.MONTH),
                intermediate.get(Calendar.DATE),
                intermediate.get(Calendar.HOUR_OF_DAY),
                intermediate.get(Calendar.MINUTE),
                intermediate.get(Calendar.SECOND));

        // Construct a new timestamp with the value we expect to be returned
        // from getTimestamp().
        Timestamp expected = new Timestamp(cal2.getTimeInMillis());
        expected.setNanos(original.getNanos());

        // Compare it with the actually returned value.
        assertEquals(expected, returned);
    }

    /**
     * Calls a SQL procedure that takes INOUT parameters of various types.
     * @throws SQLException 
     */
    public void testManyTypesInoutProc() throws SQLException
    {
        CallableStatement cs = prepareCall
            ("call MANY_TYPES_INOUT_PROC(?,?,?,?,?,?,?,?,?,?,?,?)");

        cs.registerOutParameter (2, java.sql.Types.SMALLINT);
        cs.registerOutParameter (4, java.sql.Types.INTEGER);
        cs.registerOutParameter (6, java.sql.Types.BIGINT);
        cs.registerOutParameter (8, java.sql.Types.REAL);
        cs.registerOutParameter (10, java.sql.Types.DOUBLE);
        cs.registerOutParameter (12, java.sql.Types.TIME);

        cs.setShort(1, (short)6);
        cs.setShort(2, (short)9);
        cs.setInt(3, 6);
        cs.setInt(4, 9);
        cs.setLong(5, (long)99999);
        cs.setLong(6, (long)88888888);
        cs.setFloat(7, (float)6.123453);
        cs.setFloat(8, (float)77777);
        cs.setDouble(9, (double)6.123453);
        cs.setDouble(10, (double)8888888888888.01234);
        cs.setTime(11, Time.valueOf("11:06:03"));
        cs.setTime(12, Time.valueOf("10:05:02"));

        cs.execute();

        assertEquals("Short: Sum of 6 + 9", 15, cs.getShort(2));
        assertEquals("Int: Sum of 6 + 9", 15, cs.getInt(4));
        assertEquals("Long: Sum of 99999 + 88888888", 88988887, cs.getLong(6));
        assertEquals("Float: Sum of 6.123453 and 77777" , (float) 77783.123453,
            cs.getFloat(8), .000001);
        assertEquals("Double: Sum of Sum of 6.987654 and 8888888888888.01234",
            8.888888888894135e12, cs.getDouble(10), .000001);
        assertEquals("Time: changed to", Time.valueOf("11:06:03"), 
            cs.getTime(12));

    }

    /**
     * Calls a SQL procedure that updates a long varbinary column.
     * Uses DriverManager, so this test requires JDBC 2 DriverManager support.
     * @throws SQLException 
     */
    public void xtestUpdateLongBinaryProc() throws SQLException
    {
        // Insert a row with an initial value that will be updated later.
        Statement stmt = createStatement();
        stmt.executeUpdate(
            "INSERT INTO LONGVARBINARY_TABLE VALUES(X'010305')");

        // Build up a byte array that will replace the initial value.
        int bytearrsize = 50;
        byte[] bytearr=new byte[bytearrsize];
        String sbyteval=null;
        for (int count=0;count<bytearrsize;count++)
        {
            sbyteval=Integer.toString(count%255);
            bytearr[count]=Byte.parseByte(sbyteval);
        }

        // Update the value in the database.
        CallableStatement cstmt = prepareCall(
            "CALL UPDATE_LONGVARBINARY_PROC(?)");
        cstmt.setObject(1,bytearr,java.sql.Types.LONGVARBINARY);
        cstmt.executeUpdate();
        
        // Retrieve the updated value and verify it's correct.
        ResultSet rs = stmt.executeQuery(
            "SELECT LVBC FROM LONGVARBINARY_TABLE");
        assertNotNull("SELECT from LONGVARBINARY_TABLE", rs);

        while (rs.next())
        {
            byte[] retvalue = (byte[]) rs.getObject(1);
            assertTrue(Arrays.equals(bytearr, retvalue));
        }
    }

    /**
     * Batches up calls to a SQL procedure that updates a value in a table.
     * Uses DriverManager and Batch calls, so requires JDBC 2 support.
     * @throws SQLException 
     */
    public void xtestBatchUpdate() throws SQLException
    {
        // Setup table data
//IC see: https://issues.apache.org/jira/browse/DERBY-2304
        Statement stmt = createStatement();
        stmt.executeUpdate("INSERT INTO BATCH_TABLE VALUES(1, 'STRING_1',10)");
        stmt.executeUpdate("INSERT INTO BATCH_TABLE VALUES(2, 'STRING_2',0)");
        stmt.executeUpdate("INSERT INTO BATCH_TABLE VALUES(3, 'STRING_3',0)");
        stmt.executeUpdate("INSERT INTO BATCH_TABLE VALUES(4, 'STRING_4a',0)");
        stmt.executeUpdate("INSERT INTO BATCH_TABLE VALUES(4, 'STRING_4b',0)");

        // Setup batch to modify value to 10 * the id (makes verification easy).
        CallableStatement cstmt = prepareCall("CALL BATCH_UPDATE_PROC(?,?)");
        cstmt.setInt(1,2);  // Id 2's value will be updated to 20.
        cstmt.setInt(2,20);
        cstmt.addBatch();
        cstmt.setInt(1,3);  // Id 3's value will be updated to 30.
        cstmt.setInt(2,30);
        cstmt.addBatch();
        cstmt.setInt(1,4);  // Two rows will be updated to 40 for id 4.
        cstmt.setInt(2,40);
        cstmt.addBatch();
        cstmt.setInt(1,5);  // No rows updated (no id 5).
        cstmt.setInt(2,50);
        cstmt.addBatch();

        int[] updateCount=null;
        try {
            updateCount = cstmt.executeBatch();
            assertEquals("updateCount length", 4, updateCount.length);

            for(int i=0; i< updateCount.length; i++){
                if (usingEmbedded()) {
                    assertEquals("Batch updateCount", 0, updateCount[0]);
                }
                else if (usingDerbyNetClient()) {
                    assertEquals("Batch updateCount", -1, updateCount[0]);
                }
            }
        } catch (BatchUpdateException b) {
            assertSQLState("Unexpected SQL State", b.getSQLState(), b);
        }

        // Retrieve the updated values and verify they are correct.
        ResultSet rs = stmt.executeQuery(
            "SELECT id, tag, idval FROM BATCH_TABLE order by id, tag");
        assertNotNull("SELECT from BATCH_TABLE", rs);

        while (rs.next())
        {
            assertEquals(rs.getString(2), rs.getInt(1)*10, rs.getInt(3));
        }
    }


    /**
     * Batches up many calls to a SQL procedure that updates a value in a table.
     * All calls should succeed, except for one that should fail with a check
     * constraint violation.
     * Uses DriverManager and Batch calls, so requires JDBC 2 support.
     * @throws SQLException 
     */
    public void xtestBatchUpdateError() throws SQLException
    {
        // Setup table data
        Statement stmt = createStatement();
        stmt.executeUpdate("INSERT INTO BATCH_TABLE VALUES(1, 'STRING_1',0)");
        stmt.executeUpdate("INSERT INTO BATCH_TABLE VALUES(2, 'STRING_2',0)");
        stmt.executeUpdate("INSERT INTO BATCH_TABLE VALUES(3, 'STRING_3',0)");
        stmt.executeUpdate("INSERT INTO BATCH_TABLE VALUES(4, 'STRING_4',0)");

        // Setup batch to modify values.
        CallableStatement cstmt = prepareCall("CALL BATCH_UPDATE_PROC(?,?)");
        cstmt.setInt(1,1);  // Set id 1's value to 10
        cstmt.setInt(2,10);
        cstmt.addBatch();
        cstmt.setInt(1,2);  // Set id 2's value to -5 (should fail)
        cstmt.setInt(2,-5);
        cstmt.addBatch();
        cstmt.setInt(1,3);  // Set id 3's value to 30.
        cstmt.setInt(2,30);
        cstmt.addBatch();
        cstmt.setInt(1,4);  // Set id 4's value to 40.
        cstmt.setInt(2,40);
        cstmt.addBatch();

        int[] updateCount=null;

        try {
            updateCount = cstmt.executeBatch();
            fail("Expected batchExecute to fail");
        } catch (BatchUpdateException b) {

            if (usingEmbedded()) {
                assertSQLState("38000", b.getSQLState(), b);
            }
            else if (usingDerbyNetClient()) {
                assertSQLState("XJ208", b.getSQLState(), b);
            }

            updateCount = b.getUpdateCounts();

            /* The updateCount is different for embedded and client because
             * the embedded driver stops processing the batch after the
             * failure, while the client driver continues processing (see
             * DERBY-2301).
             */
            if (usingEmbedded()) {
                assertEquals("updateCount length", 1, updateCount.length);
                assertEquals("Batch updateCount", 0, updateCount[0]);
            }
            else if (usingDerbyNetClient()) {
                assertEquals("updateCount length", 4, updateCount.length);
                for(int i=0; i< updateCount.length; i++){
                    if(i == 1) // The second command in the batch failed.
                        assertEquals("Batch updateCount", -3, updateCount[i]);
                    else
                        assertEquals("Batch updateCount", -1, updateCount[i]);
                }
            }
        }

        // Make sure the right rows in the table were updated.
        ResultSet rs = stmt.executeQuery(
            "SELECT id, tag, idval FROM BATCH_TABLE order by id, tag");
        assertNotNull("SELECT from BATCH_TABLE", rs);

        while (rs.next())
        {
            /* Embedded and client results should be the same for the first
             * two rows (the changed row for the first successful command in 
             * the batch, followed by the unchanged row for the second command,
             * which failed).
             * After the first two rows, results are different because the
             * rest of the commands in the batch executed for client, but not
             * for embedded.
             */
            switch(rs.getInt(1)) 
            {
                case 1:
                    assertEquals(rs.getString(2), 10, rs.getInt(3));
                    break;
                case 2:
                    assertEquals(rs.getString(2), 0, rs.getInt(3));
                    break;
                default:
                    if (usingEmbedded()) {
                        assertEquals(rs.getString(2), 0, rs.getInt(3));
                    }
                    else if (usingDerbyNetClient()) {
                        assertEquals(rs.getString(2), rs.getInt(1)*10, 
                            rs.getInt(3));
                    }
                    break;
            }
        }
    }

    /**
     * Calls a SQL procedure that populates OUT parameters with minimum, 
     * maximum, and null values fetched from a table with numeric columns. 
     * Pre-history: long, long ago this test was added to exercise a problem
     * with converting BigDecimal to packed decimal, which left the Network 
     * Server cpu bound.
     * Excluded from environments than don't have JDBC 2 DriverManager.
     * Excluded from JSR169/j2ME, which doesn't support get/set BigDecimal yet.
     * @throws SQLException 
     */
    public void xtestNumericBoundariesProc() throws SQLException
    {
        // Populate the test table
        String SqlStatement= 
            "insert into NUMERIC_BOUNDARIES_TABLE " +
            "values(999999999999999, 0.000000000000001, null)";
        Statement stmt = createStatement();
        stmt.executeUpdate(SqlStatement);

        // SELECT the values back by calling the SQL procedure.
        CallableStatement cstmt = prepareCall(
            "CALL NUMERIC_BOUNDARIES_PROC(?,?,?)");
        cstmt.registerOutParameter(1,java.sql.Types.NUMERIC,15);
        cstmt.registerOutParameter(2,java.sql.Types.NUMERIC,15);
        cstmt.registerOutParameter(3,java.sql.Types.NUMERIC,15);

        cstmt.execute();

        assertDecimalSameValue("OUT 1", "999999999999999.000000000000000", 
            cstmt.getBigDecimal(1));
        assertDecimalSameValue("OUT 2", "0.000000000000001", 
            cstmt.getBigDecimal(2));
        assertNull("Expected OUT 3 to be null", cstmt.getBigDecimal(3));

    }

    /**
     * Calls a SQL procedure with BigDecimal IN and OUT parameters.
     * Excluded from JSR169/j2ME, which doesn't support get/set BigDecimal yet.
     * @throws SQLException 
     */
    public void xtestBigDecimalInAndOutProc() throws SQLException
    {
        CallableStatement cs = prepareCall
            ("CALL BIGDECIMAL_IN_AND_OUT_PROC (?, ?, ?, ?, ?, ?, ?, ?, ?)");
        cs.setBigDecimal(1, new BigDecimal("33.333"));
        cs.registerOutParameter (2, java.sql.Types.DECIMAL);
        cs.setBigDecimal(3, new BigDecimal("-999.999999"));
        cs.registerOutParameter (4, java.sql.Types.DECIMAL);
        cs.registerOutParameter (5, java.sql.Types.DECIMAL);
        cs.registerOutParameter (6, java.sql.Types.DECIMAL);
        cs.registerOutParameter (7, java.sql.Types.DECIMAL);
        cs.registerOutParameter (8, java.sql.Types.DECIMAL);
        cs.registerOutParameter (9, java.sql.Types.DECIMAL);
        cs.execute();

        assertDecimalSameValue("OUT 2", "33.3330",        cs.getBigDecimal(2));
        assertDecimalSameValue("OUT 4", "-33332.9966",    cs.getBigDecimal(4));
        assertDecimalSameValue("OUT 5", "-966.6669",      cs.getBigDecimal(5));
        assertDecimalSameValue("OUT 6", "0.0000",         cs.getBigDecimal(6));
        assertDecimalSameValue("OUT 7", "0.0000",         cs.getBigDecimal(7));
        assertDecimalSameValue("OUT 8", "99999999.0000",  cs.getBigDecimal(8));
        assertDecimalSameValue("OUT 9", "-99999999.0000", cs.getBigDecimal(9));
    }

    /**
     * Wrapper for BigDecimal compareTo.
     * Called by the xtestBigDecimalInAndOutProc,
     * xtestNumericTypesInAndOutProc, and
     * xtestNumericBoundariesProc, methods.
     */
    public void assertDecimalSameValue(String message, String expected_s, 
//IC see: https://issues.apache.org/jira/browse/DERBY-2304
        BigDecimal actual)
    {
        BigDecimal expected = (new BigDecimal(expected_s));
        assertTrue(message + 
            " expected:<" + expected + "> but was:<" + actual.toString() + ">", 
            expected.compareTo(actual)==0);
    }

    // SQL ROUTINES (functions and procedures)

    /** 
     * External code for the ONE_IN_ONE_OUT_FUNC SQL function, which squares 
     * the value of the input arg, then adds the input arg to that result.
     *
     * @param p1 integer input argument to be used in calculation
     */
    public static int oneInOneOutFunc (int p1)
    {
        return (p1 * p1) + p1;
    }

    /**
     * External code for the TWO_IN_ONE_OUT_PROC SQL procedure, which sets the 
     * value of the third arg to the sum of the first two.
     *
     * @param p1 integer input parameter to be used in calculation
     * @param p2 integer input parameter to be used in calculation
     * @param p3 integer output parameter that stores result of the calculation
     */
    public static void twoInOneOutProc (int p1, int p2, int[] p3)
    {
        p3[0] = p1 + p2;
    }

    /**
     * External code for the NO_IN_ONE_OUT_FUNC SQL function, which takes no 
     * parameters and returns the value 55.
     */
    public static int noInOneOutFunc ()
    {
        return 55;
    }

    /**
     * External code for the SYSTEM_OUT_PRINTLN_PROC SQL procedure, which
     * outputs a message to System out.
     */
    public static void systemOutPrintlnProc()
    {
        System.out.println("I'm doing something here...");
    }

    /**
     * External code for the UPDATE_LONGVARBINARY_PROC SQL procedure, which 
     * sets the value of the Long varbinary column in the LONGVARBINARY_TABLE 
     * table given the input parameter.
     *
     * @param in_param input parameter to be used for database update
     * @exception SQLException if a database error occurs
     */
    public static void updateLongVarbinaryProc (byte[] in_param) 
        throws SQLException
    {
        Connection conn = 
            DriverManager.getConnection("jdbc:default:connection");
        PreparedStatement ps = 
            conn.prepareStatement("update LONGVARBINARY_TABLE set lvbc=?");

        ps.setBytes(1,in_param);
        ps.executeUpdate();

        ps.close();
        conn.close();
    } 

    /**
     * External code for the NUMERIC_BOUNDARIES_PROC SQL procedure, which 
     * fetches max, min, and null values from a table with numeric columns.
     *
     * @param param1 output parameter that returns maxcol value
     * @param param2 output parameter that returns mincol value
     * @param param3 output parameter that returns nulcol value
     * @exception SQLException if a database error occurs
     */
    public static void numericBoundariesProc (BigDecimal[] param1,
        BigDecimal[] param2, BigDecimal[] param3) throws SQLException
    {
        Connection conn = 
            DriverManager.getConnection("jdbc:default:connection");
        Statement stmt = conn.createStatement();

        ResultSet rs = stmt.executeQuery
            ("select maxcol, mincol, nulcol from NUMERIC_BOUNDARIES_TABLE");

        if (rs.next())
        {
            param1[0]=rs.getBigDecimal(1);
            param2[0]=rs.getBigDecimal(2);
            param3[0]=rs.getBigDecimal(3);
        }
        else
        {
            throw new SQLException("Data not found");
        }

        rs.close();
        stmt.close();
        conn.close();
    } 


    /**
     * External code for the BIGDECIMAL_IN_AND_OUT_PROC SQL procedure, which
     * tests INT and OUT parameters with the BigDecimal data type.
     *
     * @param bd1   input parameter
     * @param bdr1  output parameter set to bd1 * bd2
     * @param bd2   input parameter
     * @param bdr2  output parameter set to bd1 + bd2
     * @param bdr3  output parameter set to a fixed value
     * @param bdr4  output parameter set to a fixed value
     * @param bdr5  output parameter set to a fixed value
     * @param bdr6  output parameter set to a fixed value
     * @param bdr7  output parameter set to a fixed value
     *
     */
    public static void bigDecimalInAndOutProc (BigDecimal bd1, 
        BigDecimal bdr1[], BigDecimal bd2, BigDecimal bdr2[], 
        BigDecimal bdr3[], BigDecimal bdr4[], BigDecimal bdr5[], 
        BigDecimal bdr6[], BigDecimal bdr7[])
    {
        bdr1[0] = bd1;
        bdr2[0] = bd1.multiply(bd2);
        bdr3[0] = bd1.add(bd2);
        bdr4[0] = new BigDecimal(".00000");
        bdr5[0] = new BigDecimal("-.00000");
        bdr6[0] = new BigDecimal("99999999.");
        bdr7[0] = new BigDecimal("-99999999.");
    }

    /**
     * External code for the NUMERIC_TYPES_IN_AND_OUT_PROC SQL procedure, 
     * which tests IN and OUT parameters with many numeric types.
     * Also tests method overload for manyTypesInAndOutProc.
     *
     * @param s     short      input parameter
     * @param i     int        input parameter
     * @param l     long       input parameter
     * @param f     float      input parameter
     * @param d     double     input parameter
     * @param bd    BigDecimal input parameter
     * @param sr    short      output parameter
     * @param ir    int        output parameter
     * @param lr    long       output parameter
     * @param fr    float      output parameter
     * @param dr    double     output parameter
     * @param bdr   BigDecimal output parameter
     *
     */
    public static void manyTypesInAndOutProc (
        short s,    int i,    long l,   float f,   double d,   BigDecimal bd, 
        short[] sr, int[] ir, long[] lr,float[] fr,double[] dr,BigDecimal[] bdr
    )
    {
        sr[0] = s;
        ir[0] = i;
        lr[0] = l;
        fr[0] = f;
        dr[0] = d;
        bdr[0] = bd;
    }

    /**
     * External code for the NON_NUMERIC_TYPES_IN_AND_OUT_PROC SQL procedure,
     * which tests IN / OUT parameters with many non-numeric types.
     * Also tests method overload for manyTypesInAndOutProc.
     *
     * @param dt    date       input parameter
     * @param t     time       input parameter
     * @param ts    timestamp  input parameter
     * @param ba    byte       input parameter
     * @param dtr   date       output parameter
     * @param tr    time       output parameter
     * @param tsr   timestamp  output parameter
     * @param bar   byte       output parameter
     *
     */
    public static void manyTypesInAndOutProc (
        Date dt,    Time t,    Timestamp ts,    byte[] ba,
        Date[] dtr, Time[] tr, Timestamp[] tsr, byte[][] bar
    )
    {
        dtr[0] = dt;
        tr[0] = t;
        tsr[0] = ts;
        bar[0] = ba;
    }

    /**
     * External code for the MANY_TYPES_INOUT_PROC SQL procedure, which tests 
     * INOUT parameters with many types.
     *
     * @param s1    short  input parameter
     * @param s2    short  output parameter
     * @param p1    int    input parameter
     * @param p2    int    output parameter
     * @param l1    long   input parameter
     * @param l2    long   output parameter
     * @param f1    float  input parameter
     * @param f2    float  output parameter
     * @param d1    double input parameter
     * @param d2    double output parameter
     * @param t1    time   input parameter
     * @param t2    time   output parameter
     */
    public static void manyTypesInoutProc (
        short s1, short s2[], int p1, int p2[], long l1, long l2[],
        float f1, float f2[], double d1, double d2[], Time t1, Time t2[]
    )
    {
        p2[0] = p1 + p2[0];
        s2[0] = (short) (s1 + s2[0]);
        l2[0] = l1 + l2[0];
        f2[0] = f1 + f2[0];
        d2[0] = d1 + d2[0];
        t2[0] = t1;
    }

    /**
     * External code for the BATCH_UPDATE_PROC SQL procedure, which updates 
     * data in a table for a given id.
     * Called by xtestBatchUpdateProc.
     *
     * @param id Id to be updated
     * @param id_newval New value to which the id should be updated
     * @exception SQLException if a database error occurs
     */
    public static void batchUpdateProc (int id, int id_newval)
//IC see: https://issues.apache.org/jira/browse/DERBY-2304
        throws SQLException
    {
        Connection conn = 
            DriverManager.getConnection("jdbc:default:connection");
        PreparedStatement ps = conn.prepareStatement
            ("update BATCH_TABLE set idval=? where id=?");

        ps.setInt(1, id_newval);
        ps.setInt(2, id);
        ps.executeUpdate();
        ps.close();
        conn.close();
    }
    
      
//jdbc 3.0 test methods
    
    public void testsetURL() throws SQLException, MalformedURLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6291
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    		URL domain = new URL("http://www.apache.org");
    	    cs.setURL("URL",domain);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetNull() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    	    cs.setNull("P1",1);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetBoolean() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    	    cs.setBoolean("P1",true);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetByte() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NUMERIC_TYPES_IN_AND_OUT_PROC (?,?,?,?,?,?, ?,?,?,?,?,?)");
    	try {
    	    cs.setByte("P1",(byte)1);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetShort() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NUMERIC_TYPES_IN_AND_OUT_PROC (?,?,?,?,?,?, ?,?,?,?,?,?)");
    	try {
    	    cs.setShort("P1",(short)1);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    
    public void testsetInt() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    	    cs.setInt("P1", 6);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetLong() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    	    cs.setLong("P1", (long)6000);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetFloat() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NUMERIC_TYPES_IN_AND_OUT_PROC (?,?,?,?,?,?, ?,?,?,?,?,?)");
    	try {
    	    cs.setFloat("P1",(float)6.123453);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetDouble() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NUMERIC_TYPES_IN_AND_OUT_PROC (?,?,?,?,?,?, ?,?,?,?,?,?)");
    	try {
    	    cs.setDouble("P5",(double)6.123453);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetDecimal() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NUMERIC_TYPES_IN_AND_OUT_PROC (?,?,?,?,?,?, ?,?,?,?,?,?)");
    	try {
    	    cs.setBigDecimal("P6",new BigDecimal("33.333"));
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetString() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    	    cs.setString("P4","test");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetBytes() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    	    cs.setBytes("P1",new byte[] { (byte)0xe0, 0x4f, (byte)0xd0,
    	    	    0x20, (byte)0xea, 0x3a, 0x69, 0x10, (byte)0xa2, (byte)0xd8, 0x08, 0x00, 0x2b,
    	    	    0x30, 0x30, (byte)0x9d });
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetDate() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NON_NUMERIC_TYPES_IN_AND_OUT_PROC (?,?,?,?,?,?,?,?)");
    	try {
    	    cs.setDate("P1",Date.valueOf("2013-07-13"));
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetTime() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NON_NUMERIC_TYPES_IN_AND_OUT_PROC (?,?,?,?,?,?,?,?)");
    	try {
    	    cs.setTime("P2",Time.valueOf("10:05:02"));
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetTimestamp() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NON_NUMERIC_TYPES_IN_AND_OUT_PROC (?,?,?,?,?,?,?,?)");
    	try {
    	    cs.setTimestamp("P3",Timestamp.valueOf("2002-05-12 10:05:02.000000000"));
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetAsciiStream() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    		String str = "This is a String";
    		InputStream is = new ByteArrayInputStream(str.getBytes());
    	    cs.setAsciiStream("PAscii",is,1);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetBinaryStream() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    		String str = "This is a String";
    		InputStream is = new ByteArrayInputStream(str.getBytes());
    	    cs.setBinaryStream("PBinary",is,1);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetObject() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    	    cs.setObject("P1","Object",1,2);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetObject1() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    	    cs.setObject("P1","Object",1);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetObject2() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    	    cs.setObject("P1","Object");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetCharacterStream() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    		String str = "This is a String";
    		InputStream is = new ByteArrayInputStream(str.getBytes());
    		BufferedReader br = new BufferedReader(new InputStreamReader(is));
    	    cs.setCharacterStream("P1",br,1);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetDate1() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NON_NUMERIC_TYPES_IN_AND_OUT_PROC (?,?,?,?,?,?,?,?)");
    	try {
    	    cs.setDate("P1",Date.valueOf("2013-07-13"),Calendar.getInstance());
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetTime1() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NON_NUMERIC_TYPES_IN_AND_OUT_PROC (?,?,?,?,?,?,?,?)");
    	try {
    	    cs.setTime("P2",Time.valueOf("10:05:02"),Calendar.getInstance());
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetTimestamp1() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NON_NUMERIC_TYPES_IN_AND_OUT_PROC (?,?,?,?,?,?,?,?)");
    	try {
    	    cs.setTimestamp("P3",Timestamp.valueOf("2002-05-12 10:05:02.000000000"),Calendar.getInstance());
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetNull1() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    	    cs.setNull("P1",1,"Null");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testgetString() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NON_NUMERIC_TYPES_IN_AND_OUT_PROC(?,?,?,?,?,?,?,?)");
    	try {
    	    cs.getString("P4");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testgetBoolean() throws SQLException
    {
    	CallableStatement cs =prepareCall("call NON_NUMERIC_TYPES_IN_AND_OUT_PROC(?,?,?,?,?,?,?,?)");
    	try {
    	    cs.getBoolean("P4");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testgetByte() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NUMERIC_TYPES_IN_AND_OUT_PROC(?,?,?,?,?,?,?,?,?,?,?,?)");
    	try {
    	    cs.getByte("P1");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testgetShort() throws SQLException
    {
    	CallableStatement cs =prepareCall("call NUMERIC_TYPES_IN_AND_OUT_PROC(?,?,?,?,?,?,?,?,?,?,?,?)");
    	try {
    	    cs.getShort("P1");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testgetInt() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NON_NUMERIC_TYPES_IN_AND_OUT_PROC (?,?,?,?,?,?,?,?)");
    	try {
    	    cs.getInt("P4");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testgetLong() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NUMERIC_TYPES_IN_AND_OUT_PROC(?,?,?,?,?,?,?,?,?,?,?,?)");
    	try {
    	    cs.getLong("P3");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testgetFloat() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NUMERIC_TYPES_IN_AND_OUT_PROC(?,?,?,?,?,?,?,?,?,?,?,?)");
    	try {
    	    cs.getFloat("P5");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testgetDouble() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NUMERIC_TYPES_IN_AND_OUT_PROC(?,?,?,?,?,?,?,?,?,?,?,?)");
    	try {
    	    cs.getDouble("P5");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testgetBytes() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NUMERIC_TYPES_IN_AND_OUT_PROC(?,?,?,?,?,?,?,?,?,?,?,?)");
    	try {
    	    cs.getBytes("P1");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testgetDate() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NON_NUMERIC_TYPES_IN_AND_OUT_PROC(?,?,?,?,?,?,?,?)");
    	try {
    	    cs.getDate("P1");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testgetTime() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NON_NUMERIC_TYPES_IN_AND_OUT_PROC(?,?,?,?,?,?,?,?)");
    	try {
    	    cs.getTime("P2");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testgetTimestamp() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NON_NUMERIC_TYPES_IN_AND_OUT_PROC(?,?,?,?,?,?,?,?)");
    	try {
    	    cs.getTimestamp("P3");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testgetObject() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NON_NUMERIC_TYPES_IN_AND_OUT_PROC(?,?,?,?,?,?,?,?)");
    	try {
    	    cs.getObject("P4");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testgetBigDecimal() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NUMERIC_TYPES_IN_AND_OUT_PROC(?,?,?,?,?,?,?,?,?,?, ?,?)");
    	try {
    	    cs.getBigDecimal("P6");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
   
    public void testgetObject1() throws SQLException, MalformedURLException
    {
    	Map<String, Class<?>> format = new HashMap<String, Class<?>>();
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    	    cs.getObject("P1",format);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testgetRef() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NON_NUMERIC_TYPES_IN_AND_OUT_PROC(?, ?,?,?,?, ?, ?,?)");
    	try {
    	    cs.getRef("P4");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testgetBlob() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NON_NUMERIC_TYPES_IN_AND_OUT_PROC(?, ?,?,?,?, ?, ?,?)");
    	try {
    	    cs.getBlob("P4");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testgetClob1() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NON_NUMERIC_TYPES_IN_AND_OUT_PROC(?, ?,?,?,?, ?, ?,?)");
    	try {
    	    cs.getClob("P4");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testgetArray() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NON_NUMERIC_TYPES_IN_AND_OUT_PROC(?, ?,?,?,?, ?, ?,?)");
    	try {
    	    cs.getArray("P4");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testgetDate1() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NON_NUMERIC_TYPES_IN_AND_OUT_PROC(?, ?,?,?,?, ?, ?,?)");
    	try {
    	    cs.getDate("P1",Calendar.getInstance());
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testgetTime1() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NON_NUMERIC_TYPES_IN_AND_OUT_PROC(?, ?,?,?,?, ?, ?,?)");
    	try {
    	    cs.getTime("P2",Calendar.getInstance());
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testgetTimestamp1() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NON_NUMERIC_TYPES_IN_AND_OUT_PROC(?, ?,?,?,?, ?, ?,?)");
    	try {
    	    cs.getTimestamp("P3",Calendar.getInstance());
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testgetURL() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NON_NUMERIC_TYPES_IN_AND_OUT_PROC(?,?,?,?,?, ?, ?,?)");
    	try {
    	    cs.getURL("P4");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testregisterOutParameter() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    	    cs.registerOutParameter("String",1);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testregisterOutParameter1() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    	    cs.registerOutParameter("String",1,2);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testregisterOutParameter2() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    	    cs.registerOutParameter("String",1);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testregisterOutParameter3() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    	    cs.registerOutParameter("String1",1,"String2");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    //test  methods for jdbc 4.0
    
    public void testgetCharacterStream() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NON_NUMERIC_TYPES_IN_AND_OUT_PROC (?,?,?,?,?,?,?,?)");
    	try {
    	    cs.getCharacterStream("P4");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    
    public void testgetNCharacterStream() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NON_NUMERIC_TYPES_IN_AND_OUT_PROC (?, ?, ?,?,?, ?, ?,?)");
    	try {
    	    cs.getNCharacterStream("P4");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    
    public void testgetNString() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NON_NUMERIC_TYPES_IN_AND_OUT_PROC (?, ?, ?,?,?, ?, ?,?)");
    	try {
    	    cs.getNString("P4");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testgetRowId() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NON_NUMERIC_TYPES_IN_AND_OUT_PROC (?, ?, ?,?,?, ?, ?,?)");
    	try {
    	    cs.getRowId("P4");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetRowId() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    	    cs.setRowId("P1",cs.getRowId("P2"));
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetBlob() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    	    cs.setBlob("P1",cs.getBlob("P2"));
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetClob() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    	    cs.setClob("P1",cs.getClob("P2"));
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetNString() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    	    cs.setNString("P1","value");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetNCharacterStream1() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    		String str = "This is a String";
    		InputStream is = new ByteArrayInputStream(str.getBytes());
    		BufferedReader br = new BufferedReader(new InputStreamReader(is));
    	    cs.setNCharacterStream("P1",br);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetNCharacterStream2() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    		String str = "This is a String";
    		InputStream is = new ByteArrayInputStream(str.getBytes());
    		BufferedReader br = new BufferedReader(new InputStreamReader(is));
    	    cs.setNCharacterStream("P1",br,(long)345678788);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetNClob() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    		cs.setNClob("P1",cs.getNClob(1));
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetClob1() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    		String str = "This is a String";
    		InputStream is = new ByteArrayInputStream(str.getBytes());
    		BufferedReader br = new BufferedReader(new InputStreamReader(is));
    		cs.setNClob("P1",br);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetClob2() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    		String str = "This is a String";
    		InputStream is = new ByteArrayInputStream(str.getBytes());
    		BufferedReader br = new BufferedReader(new InputStreamReader(is));
    		cs.setNClob("P1",br,(long)345678788);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetClobInputStream1() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    		String str = "This is a String";
    		InputStream is = new ByteArrayInputStream(str.getBytes());
    		cs.setBlob("P1",is);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetClobInputStream2() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    		String str = "This is a String";
    		InputStream is = new ByteArrayInputStream(str.getBytes());
    		cs.setBlob("P1",is,(long)345678788);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetNClobInput1() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    		String str = "This is a String";
    		InputStream is = new ByteArrayInputStream(str.getBytes());
    		BufferedReader br = new BufferedReader(new InputStreamReader(is));
    		cs.setNClob("P1",br);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetNClobInput2() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    		String str = "This is a String";
    		InputStream is = new ByteArrayInputStream(str.getBytes());
    		BufferedReader br = new BufferedReader(new InputStreamReader(is));
    		cs.setNClob("P1",br,(long)345678788);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    
    public void testgetNClob() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NON_NUMERIC_TYPES_IN_AND_OUT_PROC (?,?,?,?,?,?,?,?)");
    	try {
    	    cs.getNClob("P4");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetSQLXML() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    	    cs.setSQLXML("P1",cs.getSQLXML(1));
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testgetSQLXML() throws SQLException
    {
    	CallableStatement cs = prepareCall("call NON_NUMERIC_TYPES_IN_AND_OUT_PROC (?,?,?,?,?,?,?,?)");
    	try {
    	    cs.getSQLXML("P4");
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetAsciiStream1() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    		String str = "This is a String";
    		InputStream is = new ByteArrayInputStream(str.getBytes());
    	    cs.setAsciiStream("P1",is);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetAsciiStream2() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    		String str = "This is a String";
    		InputStream is = new ByteArrayInputStream(str.getBytes());
    	    cs.setAsciiStream("P1",is,(long)345678788);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetBinaryStream1() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    		String str = "This is a String";
    		InputStream is = new ByteArrayInputStream(str.getBytes());
    	    cs.setBinaryStream("P1",is);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
   public void testsetBinaryStream2() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?, ?, ?)");
    	try {
    		String str = "This is a String";
    		InputStream is = new ByteArrayInputStream(str.getBytes());
    	    cs.setBinaryStream("P1",is,(long)345678788);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetCharacterStream1() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?,?,?)");
    	try {
    		String str = "This is a String";
    		InputStream is = new ByteArrayInputStream(str.getBytes());
    		BufferedReader br = new BufferedReader(new InputStreamReader(is));
    	    cs.setCharacterStream("P1",br);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    
    public void testsetCharacterStream2() throws SQLException
    {
    	CallableStatement cs = prepareCall("call TWO_IN_ONE_OUT_PROC (?,?,?)");
    	try {
    		String str = "This is a String";
    		InputStream is = new ByteArrayInputStream(str.getBytes());
    		BufferedReader br = new BufferedReader(new InputStreamReader(is));
    	    cs.setCharacterStream("P1",br,(long)345678788);
    	    fail("should have failed");
    	} catch (SQLFeatureNotSupportedException e) {
    	    assertSQLState("0A000", e);
    	}
    }
    


}
    
    


