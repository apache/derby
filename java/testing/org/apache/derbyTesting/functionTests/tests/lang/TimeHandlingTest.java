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

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;


public class TimeHandlingTest extends BaseJDBCTestCase {

    private static long SLEEP_TIME = 2000;
    
    private Calendar cal;
    
    // Determine a minimal sleep time that that consistently, based upon
    // ten tries, results in a change in System.currentTimeMillis.
    static {
        for (int ms = 50; ms <= 2000; ms +=50)
        {
            boolean seenChange = true;
            for (int i = 0; i < 10; i++) {
                long start = System.currentTimeMillis();
                try {
                    Thread.sleep(ms);
                } catch (InterruptedException e) {
                    seenChange = false;
                }
                long end = System.currentTimeMillis();
                
                if (start == end) {
                    seenChange = false;
                    break;
                }
            }
            
            if (seenChange) {
                SLEEP_TIME = ms;
                break;
            }
        }
    }
    
    public static Test suite()
    {
        TestSuite suite = new TestSuite(TimeHandlingTest.class);
        
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
                        " C_TS TIMESTAMP," +
                        " D_T TIME DEFAULT CURRENT TIME," +
                        " D_D DATE  DEFAULT CURRENT DATE," +
                        " D_TS TIMESTAMP DEFAULT CURRENT TIMESTAMP)");

            }
        };
    }
    
    /**
     * Method for SQL SLEEP function. Sleeps for the time determined
     * at class initialization that will result in a change in
     * System.currentTimeMillis.
     * @return
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
     * Simple set up, just get a Calendar.
     */
    protected void setUp()
    {
        cal = Calendar.getInstance();
    }
    
    /**
     * Tests for CURRENT TIME and CURRENT_TIME.
     * A set of tests that ensure the CURRENT TIME maintains
     * a single value for the life time of a statement.
     * @throws SQLException
     */
    public void testCurrentTime() throws SQLException
    {
        Statement s = createStatement();
        
        // Simple CURRENT_TIME
        long start = System.currentTimeMillis();
        ResultSet rs = s.executeQuery("VALUES CURRENT TIME");
        rs.next();
        long end = System.currentTimeMillis();
        checkCurrentTimeValue(start, end, rs, 1);        
        rs.close();
        
        // Alternate CURRENT TIME
        start = System.currentTimeMillis();
        rs = s.executeQuery("VALUES CURRENT_TIME");
        rs.next();
        end = System.currentTimeMillis();
        checkCurrentTimeValue(start, end, rs, 1);
        rs.close();
        
        // Check they produce the same value
        start = System.currentTimeMillis();
        rs = s.executeQuery("VALUES (CURRENT_TIME, CURRENT TIME)");
        rs.next();
        end = System.currentTimeMillis();
        checkCurrentTimeMultiple(start, end, rs, new int[] {1,2}, 2);
        rs.close();

        // Check they produce the same value across multiple rows
        start = System.currentTimeMillis();
        rs = s.executeQuery(
                "VALUES (CURRENT_TIME, CURRENT TIME), (CURRENT_TIME, CURRENT TIME),(CURRENT_TIME, CURRENT TIME)");
        rs.next();
        end = System.currentTimeMillis();       
        checkCurrentTimeMultiple(start, end, rs, new int[] {1,2}, 6);
        rs.close();
        
        // Check they produce the same value across multiple rows
        // with a forced sleep within row creaton
        start = System.currentTimeMillis();
        rs = s.executeQuery(
                "VALUES (CURRENT_TIME, CURRENT TIME, SLEEP())," +
                " (CURRENT_TIME, CURRENT TIME, SLEEP())," +
                " (CURRENT_TIME, CURRENT TIME, SLEEP())");
        rs.next();
        end = System.currentTimeMillis();       
        checkCurrentTimeMultiple(start, end, rs, new int[] {1,2}, 6);
        rs.close(); 
        
        // Check behaviour in non-queries.
        
        // Simple insert of a single row
        s.executeUpdate("DELETE FROM TIME_ALL");
        start = System.currentTimeMillis();
        s.executeUpdate("INSERT INTO TIME_ALL(C_T) VALUES CURRENT TIME");
        end = start = System.currentTimeMillis();
        rs = s.executeQuery("SELECT C_T, D_T FROM TIME_ALL");
        rs.next();
        checkCurrentTimeMultiple(start, end, rs, new int[] {1,2}, 2);
        rs.close();
        
        // Insert of multiple rows
        s.executeUpdate("DELETE FROM TIME_ALL");
        start = System.currentTimeMillis();
        s.executeUpdate("INSERT INTO TIME_ALL(C_T) VALUES CURRENT TIME, CURRENT TIME, CURRENT TIME, CURRENT_TIME");
        end = start = System.currentTimeMillis();
        rs = s.executeQuery("SELECT C_T, D_T FROM TIME_ALL");
        rs.next();
        checkCurrentTimeMultiple(start, end, rs, new int[] {1,2}, 8);
        rs.close();
        
        // Insert of multiple rows from a query with a delay
        s.executeUpdate("DELETE FROM TIME_ALL");
        start = System.currentTimeMillis();
        s.executeUpdate("INSERT INTO TIME_ALL(ID, C_T) " +
                " SELECT * FROM TABLE (VALUES " +
                " (SLEEP(), CURRENT TIME), " +
                " (SLEEP(), CURRENT TIME), " +
                " (SLEEP(), CURRENT TIME), " +
                " (SLEEP(), CURRENT TIME)) AS T");
        end = start = System.currentTimeMillis();
        rs = s.executeQuery("SELECT C_T, D_T FROM TIME_ALL");
        rs.next();
        checkCurrentTimeMultiple(start, end, rs, new int[] {1,2}, 8);
        rs.close();        
         
        s.close();
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
    private void checkCurrentTimeMultiple(long start, long end, ResultSet rs,
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
        if (tv == null)
            return null;
        
        // Check the date portion is set to 1970/01/01
        assertTime19700101(tv);
        cal.clear();
        cal.setTime(tv);
        
        int hour = cal.get(Calendar.HOUR_OF_DAY);
        int min = cal.get(Calendar.MINUTE);
        int sec = cal.get(Calendar.SECOND);
        
        // Check the milli-seconds portion is 0
        // Derby does not support that precision in TIME
        assertEquals(0, cal.get(Calendar.MILLISECOND));
        
        long now = System.currentTimeMillis();
        Timestamp tsv = rs.getTimestamp(column);
        assertNotNull(tsv);
        assertFalse(rs.wasNull());
        
        // Check the TIME portion is set to the same as tv
        cal.clear();
        cal.setTime(tsv);
        assertEquals(hour, cal.get(Calendar.HOUR_OF_DAY));
        assertEquals(min, cal.get(Calendar.MINUTE));
        assertEquals(sec, cal.get(Calendar.SECOND));
        assertEquals(0, cal.get(Calendar.MILLISECOND));
        
        // DERBY-1811, DERBY-889 being fixed could add tests
        // here to check the returned date portion is the current date
        // using the value from 'now'.
        
        
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
     * CURRENT TIME or a value set from CURRENT TIME.
     * 
     * @param start Time the query was executed
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
        assertTime19700101(to);
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
        assertTime19700101(to);
        return to;
    }
    
    /**
     * Javadoc for java.sql.Time states the components of
     * date for a java.sql.Time value must be set to January 1, 1970.
     * Note that the java.sql.Time class does not enforce this,
     * it is up to the driver.
     * @param t
     */
    private void assertTime19700101(Time t){
        
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
}
