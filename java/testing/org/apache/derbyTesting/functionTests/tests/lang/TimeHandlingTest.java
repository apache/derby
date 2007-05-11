/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.TimeHandlingTest

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

import java.io.UnsupportedEncodingException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Random;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;


public class TimeHandlingTest extends BaseJDBCTestCase {
    
    /**
     * All the functions or expressions that result in
     * a TIME value with the same value as CURRENT_TIME.
     */
    private static final String[] CURRENT_TIME_FUNCTIONS =
    {"CURRENT TIME", "CURRENT_TIME"};
    
    /**
     * All the functions or expressions that result in
     * a TIMESTAMP value with the same value as CURRENT_TIMESTAMP.
     */
    private static final String[] CURRENT_TIMESTAMP_FUNCTIONS =
    {"CURRENT TIMESTAMP", "CURRENT_TIMESTAMP"};    

    /**
     * Time to sleep that will result in different TIME values.
     */
    private static final long SLEEP_TIME = 2000;
    
    /**
     * Calendar for testing returned values.
     */
    private Calendar cal;


    /**
     * Runs the tests in the default embedded configuration and then
     * the client server configuration.
     */
    public static Test suite()
    {
        TestSuite suite = new TestSuite(TimeHandlingTest.class);
        
        suite.addTest(TestConfiguration.clientServerSuite(TimeHandlingTest.class));
        
        return new CleanDatabaseTestSetup(suite) {
            protected void decorateSQL(Statement s) throws SQLException {
               
                s.execute("CREATE FUNCTION SLEEP() RETURNS INTEGER" +
                        " LANGUAGE JAVA PARAMETER STYLE JAVA" +
                        " NO SQL " +
                        " EXTERNAL NAME '"+
                        TimeHandlingTest.class.getName().concat(".sleep'"));
                
                s.execute("CREATE TABLE TIME_ALL (ID INT," +
                        " C_T TIME," +
                        " C_D DATE," +
                        " C_TS TIMESTAMP)");
                
                for (int f = 0; f < CURRENT_TIME_FUNCTIONS.length; f++)
                {
                    s.execute("ALTER TABLE TIME_ALL ADD COLUMN" +
                            " D_T" + f + " TIME WITH DEFAULT " +
                            CURRENT_TIME_FUNCTIONS[f]);
                }
                for (int f = 0; f < CURRENT_TIMESTAMP_FUNCTIONS.length; f++)
                {
                    s.execute("ALTER TABLE TIME_ALL ADD COLUMN" +
                            " D_TS" + f + " TIMESTAMP WITH DEFAULT " +
                            CURRENT_TIMESTAMP_FUNCTIONS[f]);
                }
           }
        };
    }
    
    /**
     * Method for SQL SLEEP function. Sleeps for the time 
     * that will result in a change in
     * System.currentTimeMillis and a Derby TIME value.
     * @throws InterruptedException
     */
    public static int sleep() throws InterruptedException {
        Thread.sleep(SLEEP_TIME);
        return 0;
    }

    public TimeHandlingTest(String name) {
        super(name);
    }
    
    /**
     * Simple set up, just get a Calendar
     * and ensure the table T_ALL is empty.
     * @throws SQLException 
     * @throws UnsupportedEncodingException 
     */
    protected void setUp() throws UnsupportedEncodingException, SQLException
    {
        cal = Calendar.getInstance();
        Statement s  = createStatement();
        s.executeUpdate("DELETE FROM TIME_ALL");
        s.close();
    }
    
    /**
     * Test inserting and selecting of TIME values.
     * A set of random TIME values are inserted along with an
     * identifer that encodes the time value. The values are then
     * fetched and compared to a value calculated from the identifier.
     * The returned values are fetched using checkTimeValue thus inheriting
     * all the checks within that method.
     * <BR>
     * 
     * @throws SQLException
     * @throws UnsupportedEncodingException 
     */
    public void testInertTime() throws SQLException, UnsupportedEncodingException
    {
        getConnection().setAutoCommit(false);
        // Insert a set of time values, 


        Random r = new Random();

        // Insert 500 TIME values using a PreparedStatement,
        // but randomly selecting the way the value is inserted
        // between:
        //  java.sql.Time object
        //  String representation hh:mm:ss from Time.toString()
        //  String representation hh.mm.ss
        
        // prime number used to select the way the
        // selected value is inserted.
        final int itk = 71;

        PreparedStatement ps = prepareStatement(
           "INSERT INTO TIME_ALL(ID, C_T) VALUES (?, ?)");
 
        for (int i = 0; i < 500; i++) {
            
            // Just some big range from zero upwards
            int id = r.nextInt(1000000);
            ps.setInt(1, id);
            
            Time ct = getCodedTime(id);
           
            switch ((id % itk) % 3)
            {
            case 0: // Insert using Time object
                ps.setTime(2, ct);
                break;
            case 1: // Insert using String provided by Time.toString() (hh:mm:ss)
                ps.setString(2, ct.toString());
                break;
            case 2: // Insert using String format (hh.mm.ss)
                ps.setString(2, ct.toString().replace(':', '.'));
                break;
            default:
               fail("not reached");
               
             }
            ps.executeUpdate();
        }
        ps.close();
        commit();
        
        Statement s = createStatement();
        
        ResultSet rs = s.executeQuery("SELECT ID, C_T FROM TIME_ALL");
        int rowCount = 0;
        while (rs.next())
        {
            int id = rs.getInt(1);
            Time t = checkTimeValue(rs, 2);          
            assertTimeEqual(getCodedTime(id), t);
            rowCount++;
        }
        rs.close();
        s.close(); 
        commit();
        
        assertEquals(rowCount, 500);
    }

    /**
     * Return a time simply encoded from an integer identifier
     * and a set of fixed encoding keys, each a prime number.
     * This allows a random value to be inserted into a table
     * as a TIME and an INTEGER and thus checked for consistency
     * on a SELECT.
     * @param id
     */
    private Time getCodedTime(int id)
    {
        final int hk = 17;
        final int mk = 41;
        final int sk = 67;

        int hour = (id % hk) % 24;
        int min = (id % mk) % 60;
        int sec = (id % sk) % 60;
        
        return getTime19700101(hour, min ,sec);
    }

    /**
     * Tests for CURRENT TIME and CURRENT_TIME.
     * A set of tests that ensure the CURRENT TIME maintains
     * a single value for the life time of a statement and
     * that (subject to the resolution) the returned value
     * is correctly between the start time of the statement
     * execution and the first fetch or completion.
     * <BR>
     * 
     * @throws SQLException
     * @throws InterruptedException 
     */
    public void testCurrentTime() throws SQLException, InterruptedException
    {      
        currentFunctionTests(Types.TIME, CURRENT_TIME_FUNCTIONS);      
    }
    
    /**
     * Tests for CURRENT TIMESTAMP functions.
     * A set of tests that ensure the CURRENT TIMESTAMP maintains
     * a single value for the life time of a statement and
     * that (subject to the resolution) the returned value
     * is correctly between the start time of the statement
     * execution and the first fetch or completion.
     * @throws SQLException
     * @throws InterruptedException 
     */
    public void testCurrentTimestamp() throws SQLException, InterruptedException
    {      
        currentFunctionTests(Types.TIMESTAMP, CURRENT_TIMESTAMP_FUNCTIONS);      
    }    
    /**
     * Test all the current timedate functions passed in that
     * return the specified type. Generic function that checks
     * the functions' are all identical in various situations
     * and that the have the correct value, and change across
     * executions.
     * 
     * @param jdbcType JDBC type, Types.TIME, DATE or TIMESTAMP.
     * @param functions List of functions or expressions that map to the
     * current time date value and return the specified type.
     * @throws SQLException
     * @throws InterruptedException 
     */
    private void currentFunctionTests(int jdbcType, String[] functions)
    throws SQLException, InterruptedException
    {
        Statement s = createStatement();

        // Single value returned by each function.
        for (int f = 0; f < functions.length; f++) {
            checkCurrentQuery(jdbcType, s, "VALUES " + functions[f],
                    new int[] {1}, 1);
        }
        
        // Create text for a single row in a VALUES clause,
        // each function represented once.
        StringBuffer rb = new StringBuffer("(");
        for (int f = 0; f < functions.length; f++) {
            if (f != 0)
                rb.append(", ");
            rb.append(functions[f]);
        }
        rb.append(")");
        String row = rb.toString();
        
        int[] columns = new int[functions.length];
        for (int f = 0; f < columns.length; f++)
            columns[f] = f + 1;
        
        // All the functions as a single row, all return the same value
        String sql = "VALUES " + row;
        checkCurrentQuery(jdbcType, s, sql, columns, functions.length);

        
        // Check they produce the same value across multiple rows
        sql = "VALUES " + row + "," + row + "," + row;
        checkCurrentQuery(jdbcType, s, sql, columns, 3 * functions.length);

        // Check they produce the same value across multiple rows
        // with a forced sleep within row creaton
        String sleepRow = row.substring(0, row.length() - 1)
             + ", SLEEP())";
        
        sql =  "VALUES " + sleepRow + "," + sleepRow + "," + sleepRow;
        checkCurrentQuery(jdbcType, s, sql, columns, 3 * functions.length);

        
        // Check behaviour in non-queries.
        String ccol = null;
        String dcol = null;
        switch (jdbcType)
        {
        case Types.TIME:
            dcol = "D_T";
            ccol = "C_T";
            break;
        case Types.TIMESTAMP:
            dcol = "D_TS";
            ccol = "C_TS";
            break;            
        case Types.DATE:
            dcol = "D_D";
            ccol = "C_D";
            break; 
        default:
            fail("Unexpected JDBC Type " + jdbcType);
        }
        
        // All the functions as multiple rows,  one function per row.
        StringBuffer rm = new StringBuffer();
        for (int f = 0; f < functions.length; f++) {
            if (f != 0)
                rm.append(", ");
            rm.append(functions[f]);
        }
        String mrow = rm.toString();
        
        // Select list with all the columns of this type
        StringBuffer sb = new StringBuffer();
        sb.append(ccol); // Column without the defaul
        for (int f = 0; f < functions.length; f++) {
            sb.append(", ");
            sb.append(dcol);
            sb.append(f);
        }
        String typeColumnList = sb.toString();
        String selectAllType = "SELECT " + typeColumnList + " FROM TIME_ALL";
        
        int[] tableColumns = new int[columns.length + 1];
        for (int i = 0; i < tableColumns.length; i++)
            tableColumns[i] = i+1;
                    
        // Insert multiple rows, one per function
        // Check all the inserted value and the default
        // columns have the same value.
        String insert = "INSERT INTO TIME_ALL(" + ccol + ") VALUES " + mrow;
        s.executeUpdate("DELETE FROM TIME_ALL");
        long start = System.currentTimeMillis();
        s.executeUpdate(insert);
        long end = System.currentTimeMillis();
        ResultSet rs = s.executeQuery(selectAllType);
        rs.next();
        checkCurrentMultiple(jdbcType, start, end, rs, tableColumns, 
                functions.length * (functions.length + 1));
        rs.close();
        
        // Insert of multiple rows from a query with a delay
        // All the functions as multiple rows,  one function per row
        // with a SLEEP as the first column.
        sb = new StringBuffer();
        for (int f = 0; f < functions.length; f++) {
            if (f != 0)
                sb.append(", ");
            sb.append("(SLEEP(), ");
            sb.append(functions[f]);
            sb.append(")");
        }
        String mSleepRow = sb.toString();
        
        insert = "INSERT INTO TIME_ALL(ID, " + ccol + ") " +
          " SELECT * FROM TABLE (VALUES " +
          mSleepRow +
          ") AS T";
       
        s.executeUpdate("DELETE FROM TIME_ALL");
        start = System.currentTimeMillis();
        s.executeUpdate(insert);
        end = System.currentTimeMillis();
        rs = s.executeQuery(selectAllType);
        rs.next();
        checkCurrentMultiple(jdbcType, start, end, rs, tableColumns,
                functions.length * (functions.length + 1));
        rs.close();
        
        // Ensure a PreparedStatement (psI) resets its current time correctly
        // and does not get stuck with a single value for all executions.
        PreparedStatement psQ = prepareStatement(
                selectAllType + " WHERE ID = ?");
        
        Object last = null;
        for (int f = 0; f < functions.length; f++) {
            PreparedStatement psI = prepareStatement("INSERT INTO TIME_ALL(ID, " +
                    ccol + ")" +
                    " VALUES (?, " + functions[f] +")");
            s.executeUpdate("DELETE FROM TIME_ALL");

            for (int i = 1; i <=3; i++) {
               psI.setInt(1, i);
               psQ.setInt(1, i);   
               start = System.currentTimeMillis();
               psI.executeUpdate();
               end = System.currentTimeMillis();

               rs = psQ.executeQuery();
               rs.next();
               Object next = checkCurrentMultiple(jdbcType, start, end, rs,
                       tableColumns, functions.length + 1);
               rs.close();
               
               if (last != null) {
                   // This check is redundant because the last and next have
                   // been checked they are within limit of the start and end.
                   // But why not check it.
                   assertFalse("CURRENT value not changed over executions",
                           last.equals(next));
               }
               last = next;
                     
               // Ensure the next execution is meant to get a different value
               Thread.sleep(SLEEP_TIME);
            }
            psI.close();

        }

        psQ.close();
        s.close();
    }
    
    /**
     * Execute a query that uses CURRENT expressions directly.
     * The time returned for these values should be between the
     * start of execution and after the return from the first rs.next().
     * @param sqlType
     * @param s
     * @param sql
     * @param columns
     * @param expectedCount
     * @throws SQLException
     */
    private void checkCurrentQuery(int sqlType,
            Statement s, String sql, int[] columns, int expectedCount)
    throws SQLException
    {
        long start = System.currentTimeMillis();
        ResultSet rs = s.executeQuery(sql);
        rs.next();
        long end = System.currentTimeMillis();
        checkCurrentMultiple(sqlType, start, end, rs,
                columns, expectedCount);
        rs.close();       
    }
    
    /**
     * Check the validity of all CURRENT time values returned and
     * that they are identical.
     * @param jdbcType Types.TIME or TIMESTAMP
     * @param start Start of window for valid value.
     * @param end End of window for valid value.
     * @param rs Result set positioned  on row.
     * @param columns Columns holding current values.
     * @param expectedCount Total number of values exected to see
     * (row count times column count)
     * @throws SQLException
     */
    private Object checkCurrentMultiple(int jdbcType, long start, long end, ResultSet rs,
            int[] columns, int expectedCount) throws SQLException
   {
        switch (jdbcType)
        {
        case Types.TIME:
            return checkCurrentTimeMultiple(start, end, rs, columns, expectedCount);
        case Types.TIMESTAMP:
            return checkCurrentTimestampMultiple(start, end, rs, columns, expectedCount);
        default:
            fail("Unexpected type " + jdbcType);
        return null;
        }
  }
    
    /**
     * Check a set of rows and columns with values set to CURRENT TIME
     * in a single statement are the same.
     * @param start Start time for the statement that set the values.
     * @param end End time for the statement that set the values.
     * @param rs ResultSet positioned on the first row.
     * @param columns Set of columns holding the TIME values
     * @param expectedCount Number of values we are execpted to check.
     * @throws SQLException
     */
    private Time checkCurrentTimeMultiple(long start, long end, ResultSet rs,
            int[] columns, int expectedCount) throws SQLException
    {   
        // Result set is positioned on starting row
        // Since all values must be the same since they are based upon
        // CURRENT TIME from a single statement, pick one as the base
        // and compare the rest to it.
        Time base = checkCurrentTimeValue(start, end, rs, columns[0]);
        assertNotNull(base);
        int count = 1;
        
        // check the remaining columns on this row.
        for (int i = 1; i < columns.length; i++)
        {
            Time t = checkCurrentTimeValue(start, end, rs, columns[i]);
            assertEquals("CURENT TIME changed during execution", base, t);
            count++;
        }
        
        // now check all columns on any remaining rows
        while (rs.next()) {
            for (int i = 0; i < columns.length; i++)
            {
                Time t = checkCurrentTimeValue(start, end, rs, columns[i]);
                assertEquals("CURENT TIME changed during execution", base, t);
                count++;
            }
        }
        
        assertEquals(expectedCount, count);
        
        return base;
    }
    /**
     * Check a set of rows and columns with values set to CURRENT TIMESTAMP
     * in a single statement are the same.
     * @param start Start time for the statement that set the values.
     * @param end End time for the statement that set the values.
     * @param rs ResultSet positioned on the first row.
     * @param columns Set of columns holding the TIME values
     * @param expectedCount Number of values we are execpted to check.
     * @throws SQLException
     */
    private Timestamp checkCurrentTimestampMultiple(long start, long end, ResultSet rs,
            int[] columns, int expectedCount) throws SQLException
    {   
        // Result set is positioned on starting row
        // Since all values must be the same since they are based upon
        // CURRENT TIME from a single statement, pick one as the base
        // and compare the rest to it.
        Timestamp base = checkCurrentTimestampValue(start, end, rs, columns[0]);
        assertNotNull(base);
        int count = 1;
        
        // check the remaining columns on this row.
        for (int i = 1; i < columns.length; i++)
        {
            Timestamp ts = checkCurrentTimestampValue(start, end, rs, columns[i]);
            assertEquals("CURENT TIMESTAMP changed during execution", base, ts);
            count++;
        }
        
        // now check all columns on any remaining rows
        while (rs.next()) {
            for (int i = 0; i < columns.length; i++)
            {
                Timestamp ts = checkCurrentTimestampValue(start, end, rs, columns[i]);
                assertEquals("CURENT TIMESTAMP changed during execution", base, ts);
                count++;
            }
        }
        
        assertEquals(expectedCount, count);
        
        return base;
    }    
    /**
     * Check the consistency of a ResultSet column that returns
     * a TIME value. Can be used for any column of type TIME.
     * 
     * @param rs ResultSet holding the column, positioned on a row
     * @param column Column with the TIME value.
     * @return Returns the Time object obtained from the column.
     * @throws SQLException
     */
    private Time checkTimeValue(ResultSet rs, int column) throws SQLException
    {
        assertEquals(java.sql.Types.TIME, rs.getMetaData().getColumnType(column));
        
        try {
            rs.getDate(column);
            fail("ResultSet.getDate() succeeded on TIME column");
        } catch (SQLException e) {
            assertSQLState("22005", e);
        }
  
        Time tv = rs.getTime(column);
        assertEquals(tv == null, rs.wasNull());
        
        Object ov = rs.getObject(column);
        assertEquals(ov == null, rs.wasNull());
        
        if (tv == null) {
            assertNull(ov);
            return null;
        }
        
        assertTrue(ov instanceof java.sql.Time);
        assertEquals(tv, ov);
         
        // Check the date portion is set to 1970/01/01
        assertTime1970(tv);
        cal.clear();
        cal.setTime(tv);
        
        // Check the milli-seconds portion is 0
        // Derby does not support that precision in TIME
        assertEquals(0, cal.get(Calendar.MILLISECOND));
        
        long now = System.currentTimeMillis();
        Timestamp tsv = rs.getTimestamp(column);
        long now2 = System.currentTimeMillis();
        assertNotNull(tsv);
        assertFalse(rs.wasNull());
        
        // Check the TIME portion is set to the same as tv
        assertTimeEqual(tv, tsv);
        
        // DERBY-1811, DERBY-889 being fixed could add tests
        // Check the returned date portion is the current date
        // using the value from 'now' and 'now2'. Double check
        // just in case this test runs at midnight.
        if (!(isDateEqual(now, tsv) || isDateEqual(now2, tsv)))
        {
            fail("TIME to java.sql.Timestamp does not contain current date " + tsv);
        }
        
        String sv = rs.getString(column);
        assertNotNull(sv);
        assertFalse(rs.wasNull());
        
        // Assert the string converted back into a Time matches the Time returned.
        assertEquals("ResultSet String converted to java.sql.Time mismatch",
                tv, getTime19700101(sv, cal));
        
        return tv;
    }
    
    /**
     * Check the consistency of a ResultSet column that returns
     * a TIMESTAMP value. Can be used for any column of type TIMESTAMP.
     * 
     * @param rs ResultSet holding the column, positioned on a row
     * @param column Column with the TIMESTAMP value.
     * @return Returns the Time object obtained from the column.
     * @throws SQLException
     */
    private Timestamp checkTimestampValue(ResultSet rs, int column) throws SQLException
    {
        assertEquals(java.sql.Types.TIMESTAMP,
                rs.getMetaData().getColumnType(column));
        
  
        Timestamp tsv = rs.getTimestamp(column);
        assertEquals(tsv == null, rs.wasNull());
       
        Object ov = rs.getObject(column);
        assertEquals(ov == null, rs.wasNull());
        
        if (tsv == null) {
            assertNull(ov);
            return null;
        }

        assertTrue(ov instanceof java.sql.Timestamp);
        assertEquals(tsv, ov);
       
        Time tv = rs.getTime(column);
        assertNotNull(tv);
        assertFalse(rs.wasNull());
 
        // Check the date portion is set to 1970/01/01
        assertTime1970(tv);

        // Check the TIME portion is set to the same as tv
        // DERBY-1816 java.sql.Time values from TIMESTAMP
        // colummns lose their precision with client.
        if (!usingDerbyNetClient())
            assertTimeEqual(tv, tsv);
               
        String sv = rs.getString(column);
        assertNotNull(sv);
        assertFalse(rs.wasNull());
        
        // Assert the string converted back into a Time matches the Time returned.
        assertEquals("ResultSet String converted to java.sql.Timestamp mismatch",
                tsv, Timestamp.valueOf(sv));
        
        return tsv;
    }    

    /**
     * Check the consistency of a ResultSet column that returns
     * CURRENT TIME or a value set from CURRENT TIME.
     * 
     * @param start Time the statement settng the value was executed
     * @param end Time after first rs.next() or update statement was executed
     * @param rs ResultSet holding the column, positioned on a row
     * @param column Column with the timestamp.
     * @return Returns the Time object obtained from the column.
     * @throws SQLException
     */
    private Time checkCurrentTimeValue(long start, long end,
            ResultSet rs, int column) throws SQLException
    {       
        Time tv = checkTimeValue(rs, column);

        // The time returned should be between the value
        // of start and end (inclusive of both)
        
        Time st = getTime19700101(start, cal);
        Time et = getTime19700101(end, cal);
        
        
        if (st.after(et)) {
            // Gone back in time!
            // Well test was running around midnight and the
            // time for the start time is equal to or before 23:59:59
            // and end time is equal to or after  00:00:00
            
            assertTrue("CURRENT TIME outside of range when test crossing midnight",
               (tv.equals(st) || tv.after(st))
               || (tv.equals(et) || tv.before(et)));
        }
        else
        {
            // End time is after or equal to start time, expected case.

            // The returned time must not be before the
            // start time or after the end time.
            assertFalse("CURRENT TIME before start of statement", tv.before(st));
            assertFalse("CURRENT TIME after end of statement", tv.after(et));       
        }
        
        return tv;
    }
    /**
     * Check the consistency of a ResultSet column that returns
     * CURRENT TIMESTAMP or a value set from CURRENT TIMESTAMP.
     * 
     * @param start Time the statement settng the value was executed
     * @param end Time after first rs.next() or update statement was executed
     * @param rs ResultSet holding the column, positioned on a row
     * @param column Column with the timestamp.
     * @return Returns the Timestamp object obtained from the column.
     * @throws SQLException
     */
    private Timestamp checkCurrentTimestampValue(long start, long end,
            ResultSet rs, int column) throws SQLException
    {       
        Timestamp tsv = checkTimestampValue(rs, column);

        // The time returned should be between the value
        // of start and end (inclusive of both)
        
        Timestamp st = new Timestamp(start);
        Timestamp et = new Timestamp(end);
        
        
        if (st.after(et)) {
            // Gone back in time!
            // Well test was running around midnight and the
            // time for the start time is equal to or before 23:59:59
            // and end time is equal to or after  00:00:00
            
            assertTrue("CURRENT TIME outside of range when test crossing midnight",
               (tsv.equals(st) || tsv.after(st))
               || (tsv.equals(et) || tsv.before(et)));
        }
        else
        {
            // End time is after or equal to start time, expected case.

            // The returned time must not be before the
            // start time or after the end time.
            assertFalse("CURRENT TIME before start of statement", tsv.before(st));
            assertFalse("CURRENT TIME after end of statement", tsv.after(et));       
        }
        
        return tsv;
    }

    /**
     * Create a Time object that has its date components
     * set to 1970/01/01 and its time to match the time
     * represented by h, m and s. This matches Derby by
     * setting the milli-second component to zero.
     * <BR>
     * Note that the Time(long) constructor for java.sql.Time
     * does *not* set the date component to 1970/01/01.
     * This is a requirement for JDBC java.sql.Time values though
     */
    private Time getTime19700101(int hour, int min, int sec)
    {
        cal.clear();
        cal.set(1970, Calendar.JANUARY, 1);
        cal.set(Calendar.MILLISECOND, 0);
        
        cal.set(Calendar.HOUR_OF_DAY, hour);
        cal.set(Calendar.MINUTE, min);
        cal.set(Calendar.SECOND, sec);
        
        Time to =  new Time(cal.getTime().getTime());
        assertTime1970(to);
        return to;
    }
    
    /**
     * Create a Time object that has its date components
     * set to 1970/01/01 and its time to match the time
     * represented by t and cal. This matches Derby by
     * setting the milli-second component to zero.
     * <BR>
     * Note that the Time(long) constructor for java.sql.Time
     * does *not* set the date component to 1970/01/01.
     * This is a requirement for JDBC java.sql.Time values though
     */
    private Time getTime19700101(long t, Calendar cal)
    {
        cal.clear();
        // JDK 1.3 can't call this!
        // cal.setTimeInMillis(t);
        cal.setTime(new Date(t));
        cal.set(1970, Calendar.JANUARY, 1);
        cal.set(Calendar.MILLISECOND, 0);
        
        Time to =  new Time(cal.getTime().getTime());
        assertTime1970(to);
        return to;
    }
    
    /**
     * Create a Time object that has its date components
     * set to 1970/01/01 and its time to match the time
     * represented by t and cal. This matches Derby by
     * setting the milli-second component to zero.
     * <BR>
     * Note that the Time(long) constructor for java.sql.Time
     * does *not* set the date component to 1970/01/01.
     * This is a requirement for JDBC java.sql.Time values though
     */
    private Time getTime19700101(String s, Calendar cal)
    {
        cal.clear();
        // JDK 1.3 can't call this!
        // cal.setTimeInMillis(t);
        cal.setTime(Time.valueOf(s));
        cal.set(1970, Calendar.JANUARY, 1);
        cal.set(Calendar.MILLISECOND, 0);
        
        Time to =  new Time(cal.getTime().getTime());
        assertTime1970(to);
        return to;
    }
    
    /**
     * Javadoc for java.sql.Time states the components of
     * date for a java.sql.Time value must be set to January 1, 1970.
     * Note that the java.sql.Time class does not enforce this,
     * it is up to the driver.
     * @param t
     */
    private void assertTime1970(Time t) {
        
        /* Cannot do this because all these methods
         * throw IllegalArgumentException by definition,
         * see java.sql.Time javadoc.
 
        assertEquals(1970, t.getYear());
        assertEquals(0, t.getMonth());
        assertEquals(1, t.getDate());
        */
        cal.clear();
        cal.setTime(t);
        
        assertEquals(1970, cal.get(Calendar.YEAR));
        assertEquals(Calendar.JANUARY, cal.get(Calendar.MONTH));
        assertEquals(1, cal.get(Calendar.DATE));
    }
    
    /**
     * Assert the SQL time portion of two SQL JDBC type
     * types are equal.
     * @param tv1 the first time to compare
     * @param tv2 the second time to compare
     */
    private void assertTimeEqual(java.util.Date tv1, java.util.Date tv2)
    {
        cal.clear();
        cal.setTime(tv1);
                
        int hour = cal.get(Calendar.HOUR_OF_DAY);
        int min = cal.get(Calendar.MINUTE);
        int sec = cal.get(Calendar.SECOND);
        int ms = cal.get(Calendar.MILLISECOND);
                        
        // Check the time portion is set to the same as tv
        cal.clear();
        cal.setTime(tv2);
        assertEquals(hour, cal.get(Calendar.HOUR_OF_DAY));
        assertEquals(min, cal.get(Calendar.MINUTE));
        assertEquals(sec, cal.get(Calendar.SECOND));
        assertEquals(ms, cal.get(Calendar.MILLISECOND));
    }
    
    /**
     * Check if the date portion of a Timestamp value
     * is equal to the date portion of a time value
     * represented in milli-seconds since 1970.
     */
    private boolean isDateEqual(long d, Timestamp tsv)
    {
        cal.clear();
        cal.setTime(new java.util.Date(d));
        int day = cal.get(Calendar.DAY_OF_MONTH);
        int month = cal.get(Calendar.MONTH);
        int year = cal.get(Calendar.YEAR);
        
        cal.clear();
        cal.setTime(tsv);
        
        return day == cal.get(Calendar.DAY_OF_MONTH)
           && month == cal.get(Calendar.MONTH)
           && year == cal.get(Calendar.YEAR);   
    }
}
