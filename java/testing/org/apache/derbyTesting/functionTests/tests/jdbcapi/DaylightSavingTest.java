/*
 * Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.DaylightSavingTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.TimeZoneTestSetup;

/**
 * This class contains tests that verify the correct handling of
 * {@code java.sql.Date}, {@code java.sql.Time} and {@code java.sql.Timestamp}
 * across DST changes.
 */
public class DaylightSavingTest extends BaseJDBCTestCase {
    public DaylightSavingTest(String name) {
        super(name);
    }

    public static Test suite() {
        // Run the test in a fixed timezone so that we know exactly what time
        // DST is observed.
        return new TimeZoneTestSetup(
                TestConfiguration.defaultSuite(DaylightSavingTest.class),
                "America/Chicago");
    }

    /**
     * Regression test case for DERBY-4582. Timestamps that were converted
     * to GMT before they were stored in the database used to come out wrong
     * on the network client if the timestamp represented a time near the
     * switch to DST in the local timezone.
     */
    public void testConversionToGMTAroundDSTChange() throws SQLException {
        Statement s = createStatement();
        s.execute("CREATE TABLE DERBY4582(" +
                "ID INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, " +
                "TS TIMESTAMP, T TIME, D DATE, T2 TIME, D2 DATE)");

        // Switch from CST to CDT in 2010 happened at 2010-03-14 02:00:00 CST,
        // or 2010-03-14 08:00:00 GMT, so create some times/dates around that
        // time.
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        cal.clear();
        cal.set(Calendar.YEAR, 2010);
        cal.set(Calendar.MONTH, Calendar.MARCH);
        cal.set(Calendar.DAY_OF_MONTH, 12);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 1);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        // Create times for each hour in 2010-03-12 -- 2010-03-15 (GMT).
        Timestamp[] timestamps = new Timestamp[24 * 4];
        Time[] times = new Time[timestamps.length];
        Date[] dates = new Date[timestamps.length];
        for (int i = 0; i < timestamps.length; i++) {
            long time = cal.getTimeInMillis();
            timestamps[i] = new Timestamp(time);
            times[i] = new Time(time);
            dates[i] = new Date(time);
            cal.setTimeInMillis(time + 3600000); // move one hour forward
        }

        // Store the GMT representations of the times.
        PreparedStatement insert = prepareStatement(
                "INSERT INTO DERBY4582(TS, T, D, T2, D2) VALUES (?,?,?,?,?)");
        for (int i = 0; i < timestamps.length; i++) {
            Timestamp ts = timestamps[i];
            Time t = times[i];
            Date d = dates[i];
            // Set the TIMESTAMP/TIME/DATE values TS/T/D with their respective
            // setter methods.
            insert.setTimestamp(1, ts, cal);
            insert.setTime(2, t, cal);
            insert.setDate(3, d, cal);
            // Set the TIME/DATE values T2/D2 with setTimestamp() to verify
            // that this alternative code path also works.
            insert.setTimestamp(4, ts, cal);
            insert.setTimestamp(5, ts, cal);
            insert.execute();
        }

        // Now see that we get the same values back.
        ResultSet rs = s.executeQuery("SELECT * FROM DERBY4582 ORDER BY ID");
        for (int i = 0; i < timestamps.length; i++) {
            assertTrue("found only " + i + " rows", rs.next());
            assertEquals("ID", i + 1, rs.getInt(1));
            assertEquals("TS", timestamps[i], rs.getTimestamp(2, cal));
            assertEquals("T", stripDate(times[i], cal), rs.getTime(3, cal));
            assertEquals("D", stripTime(dates[i], cal), rs.getDate(4, cal));
            // T2 and D2 should have the same values as T and D.
            assertEquals("T2", stripDate(times[i], cal), rs.getTime(5, cal));
            assertEquals("D2", stripTime(dates[i], cal), rs.getDate(6, cal));
        }
        JDBC.assertEmpty(rs);

        // Also check that we get the expected values when we get TIME or DATE
        // with getTimestamp(), or TIMESTAMP with getTime() or getDate()
        rs = s.executeQuery("SELECT ID,T,D,TS,TS FROM DERBY4582 ORDER BY ID");
        for (int i = 0; i < timestamps.length; i++) {
            assertTrue("found only " + i + " rows", rs.next());
            assertEquals("ID", i + 1, rs.getInt(1));
            assertEquals("TIME AS TIMESTAMP",
                    timeToTimestamp(stripDate(times[i], cal), cal),
                    rs.getTimestamp(2, cal));
            assertEquals("DATE AS TIMESTAMP",
                    dateToTimestamp(stripTime(dates[i], cal), cal),
                    rs.getTimestamp(3, cal));
            assertEquals("TIMESTAMP AS TIME",
                    stripDate(timestamps[i], cal),
                    rs.getTime(4, cal));
            assertEquals("TIMESTAMP AS DATE",
                    stripTime(timestamps[i], cal),
                    rs.getDate(5, cal));
        }
        JDBC.assertEmpty(rs);
    }

    /**
     * Strip away the date component from a {@code java.util.Date} and return
     * it as a {@code java.sql.Time}, so that it can be compared with a time
     * value returned by Derby. Derby will set the date component of the time
     * value to 1970-01-01, so let's do the same here.
     *
     * @param time the time value whose date component to strip away
     * @param cal the calendar used to store the time in the database originally
     * @return a time value that represents the same time of the day as
     * {@code time} in the calendar {@code cal}, but with the date component
     * normalized to 1970-01-01
     */
    private static Time stripDate(java.util.Date time, Calendar cal) {
        cal.clear();
        cal.setTime(time);
        cal.set(1970, Calendar.JANUARY, 1);
        return new Time(cal.getTimeInMillis());
    }

    /**
     * Strip away the time component from a {@code java.util.Date} and return
     * it as a {@code java.sql.Date}, so that it can be compared with a date
     * value returned by Derby. Derby will set the time component of the date
     * value to 00:00:00.0, so let's do the same here.
     *
     * @param date the date whose time component to strip away
     * @param cal the calendar used to store the date in the database originally
     * @return a date value that represents the same day as {@code date} in the
     * calendar {@code cal}, but with the time component normalized to
     * 00:00:00.0
     */
    private static Date stripTime(java.util.Date date, Calendar cal) {
        cal.clear();
        cal.setTime(date);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return new Date(cal.getTimeInMillis());
    }

    /**
     * Convert a time value to a timestamp. The date component of the timestamp
     * should be set to the current date in the specified calendar, see
     * DERBY-889 and DERBY-1811.
     *
     * @param time the time value to convert
     * @param cal the calendar in which the conversion should be performed
     * @return a timestamp
     */
    private static Timestamp timeToTimestamp(Time time, Calendar cal) {
        // Get the current date in the specified calendar.
        cal.clear();
        cal.setTimeInMillis(System.currentTimeMillis());
        int year = cal.get(Calendar.YEAR);
        int month = cal.get(Calendar.MONTH);
        int day = cal.get(Calendar.DAY_OF_MONTH);

        // Construct a timestamp based on the current date and the specified
        // time value.
        cal.clear();
        cal.setTime(time);
        cal.set(year, month, day);

        return new Timestamp(cal.getTimeInMillis());
    }

    /**
     * Convert a date value to a timestamp. The time component of the timestamp
     * will be set to 00:00:00.0.
     *
     * @param date the date value to convert
     * @param cal the calendar in which the conversion should be performed
     * @return a timestamp
     */
    private static Timestamp dateToTimestamp(Date date, Calendar cal) {
        cal.clear();
        cal.setTime(date);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return new Timestamp(cal.getTimeInMillis());
    }
}
