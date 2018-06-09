/*
 * Derby - Class org.apache.derbyTesting.functionTests.tests.lang.Derby5866TriggerOrderTest
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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.TimeZoneTestSetup;

/**
 * <p>
 * Test that triggers for a specific event execute in the order in which they
 * were defined. This is a regression test case for DERBY-5866, where triggers
 * were seen to fire in a nondeterministic order if the system clock was too
 * coarse-grained and gave the triggers identical creation time stamps. It
 * also tests that triggers fire in the correct order when the triggers are
 * created in different time zones, or right before or after daylight saving.
 * </p>
 */
public class Derby5866TriggerOrderTest extends BaseJDBCTestCase {

    private final static TimeZone TIMEZONE =
            TimeZone.getTimeZone("Europe/Oslo");

    private final static String OVERRIDE_TIME_PROP =
            "derby.debug.overrideTriggerCreationTimestamp";

    public Derby5866TriggerOrderTest(String name) {
        super(name);
    }

    public static Test suite() {
        Test test = new CleanDatabaseTestSetup(
            TestConfiguration.embeddedSuite(Derby5866TriggerOrderTest.class));
        return new TimeZoneTestSetup(test, TIMEZONE);
    }

    @Override
    protected void tearDown() throws Exception {
        // Reset the time zone after each test case, since the test case
        // may have altered it.
        TimeZoneTestSetup.setDefault(TIMEZONE);

        // Clear the system property that overrides the trigger creation
        // timestamps.
        removeSystemProperty(OVERRIDE_TIME_PROP);

        super.tearDown();
    }

    /**
     * Test that triggers fire in the correct order if the time zone changes
     * between two CREATE TRIGGER operations in a way that makes it look like
     * the second trigger was created before the first trigger.
     */
    public void testTimeZoneChange() throws SQLException {
        setAutoCommit(false);

        Statement s = createStatement();
        s.execute("create table t1(x int)");
        s.execute("create table t2(x int generated always as identity, "
                + "y varchar(128))");

        // Create the first trigger while in the GMT time zone.
        s.execute("create trigger tr1 after insert on t1 "
                + "insert into t2(y) values 'I won! :)'");

        // Travel back in time. Sort of... At least that's how it's perceived
        // until TIMESTAMP WITH TIMEZONE is supported, and SYSTRIGGERS is
        // updated to use it (DERBY-5974).
        TimeZoneTestSetup.setDefault(TimeZone.getTimeZone("GMT-8:00"));
        s.execute("create trigger tr2 after insert on t1 "
                + "insert into t2(y) values 'I lost... :('");

        // Fire the triggers.
        s.execute("insert into t1 values 1");

        // Check which of the triggers was executed first. It should have been
        // the trigger that was defined first. Before DERBY-5866, they fired
        // in the opposite order.
        JDBC.assertFullResultSet(s.executeQuery("select * from t2 order by x"),
                                 new String[][] {
                                     { "1", "I won! :)" },
                                     { "2", "I lost... :(" },
                                 });
    }

    /**
     * Test that triggers fire in the correct order if the clock shows the
     * same creation time for all the triggers.
     */
    public void testEqualTimestamps() throws SQLException {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        testSpecificTimestamps(now, now, now);
    }

    /**
     * Test that the triggers fire in creation order even if the clock goes
     * backwards.
     */
    public void testReversedTimestamps() throws SQLException {
        long now = System.currentTimeMillis();
        testSpecificTimestamps(new Timestamp(now), new Timestamp(now - 1),
                               new Timestamp(now - 2), new Timestamp(now - 3));
    }

    /**
     * Test that triggers fire in the correct order if they are created around
     * the daylight saving time switchover.
     */
    public void testCrossDaylightSaving() throws SQLException {
        // Use a GMT-based calendar to prevent ambiguities. For example, with
        // a CET-based calendar, it would be ambiguous whether 2014-10-26
        // 02:45:00 means 2014-10-26 02:45:00 CET or 2014-10-26 02:45:00 CEST.
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

        // 15 min before Central European Time switches to DST.
        cal.set(2014, Calendar.MARCH, 30, 0, 45, 0);

        // Test that triggers are ordered correctly if they are created
        // 15 min before switch and 15 min after switch.
        testSpecificTimestamps(new Timestamp(cal.getTimeInMillis()),
                               new Timestamp(cal.getTimeInMillis() + 1800000));

        // 15 min before Central European Time switches from DST.
        cal.clear();
        cal.set(2014, Calendar.OCTOBER, 26, 0, 45, 0);

        // Test that triggers are ordered correctly if they are created
        // 15 min before switch and 15 min after switch.
        testSpecificTimestamps(new Timestamp(cal.getTimeInMillis()),
                               new Timestamp(cal.getTimeInMillis() + 1800000));

        // Last millisecond before switch to DST.
        cal.clear();
        cal.set(2014, Calendar.MARCH, 30, 0, 59, 59);
        cal.set(Calendar.MILLISECOND, 999);
        Timestamp ts = new Timestamp(cal.getTimeInMillis());
        testSpecificTimestamps(ts, ts, ts);

        // Last millisecond before switch from DST.
        cal.clear();
        cal.set(2014, Calendar.OCTOBER, 26, 0, 59, 59);
        cal.set(Calendar.MILLISECOND, 999);
        ts = new Timestamp(cal.getTimeInMillis());
        testSpecificTimestamps(ts, ts, ts);
    }

    /**
     * Test that triggers created before the epoch (Jan 1 1970) fire in the
     * correct order.
     */
    public void testPreEpoch() throws SQLException {
        // 24 hours before the epoch
        Timestamp ts = new Timestamp(-3600L * 24 * 1000);
        testSpecificTimestamps(ts, ts, ts);

        // Test with some non-zero fractions as well.

        ts.setNanos(123000000);
        testSpecificTimestamps(ts, ts, ts);

        ts.setNanos(567000000);
        testSpecificTimestamps(ts, ts, ts);

        ts.setNanos(999000000);
        testSpecificTimestamps(ts, ts, ts);
    }

    /**
     * Helper method that creates triggers with the specified creation
     * timestamps and verifies that they fire in creation order. The creation
     * timestamps can only be overridden in debug builds. When running in a
     * non-debug build, this method will simply create the triggers without
     * overriding the creation timestamps, and verify that they fire in the
     * expected order.
     */
    private void testSpecificTimestamps(Timestamp... timestamps)
            throws SQLException {
        setAutoCommit(false);

        Statement s = createStatement();

        s.execute("create table t1(x int)");
        s.execute("create table t2(x int generated always as identity, y int)");

        // Create the triggers.
        for (int i = 0; i < timestamps.length; i++) {
            overrideTriggerCreationTime(timestamps[i]);
            s.execute("create trigger tr" + (i + 1) + " after insert on t1 "
                    + "insert into t2(y) values " + (i + 1));
        }

        // Fire the triggers.
        s.execute("insert into t1 values 1");

        // Verify that the triggers executed in the correct order.
        ResultSet rs = s.executeQuery("select * from t2 order by x");
        for (int i = 1; i <= timestamps.length; i++) {
            if (rs.next()) {
                assertEquals("X", i, rs.getInt("X"));
                assertEquals("Y", i, rs.getInt("Y"));
            } else {
                fail("Row " + i + " was missing");
            }
        }
        JDBC.assertEmpty(rs);

        // Verify that the CREATIONTIMESTAMP column in SYS.SYSTRIGGERS is
        // monotonically increasing.
        PreparedStatement ps = prepareStatement(
                "select * from sys.sysschemas natural join sys.systriggers "
                + "where schemaname = ? and triggername like 'TR%' "
                + "order by creationtimestamp");
        ps.setString(1, getTestConfiguration().getUserName());
        rs = ps.executeQuery();
        Timestamp prev = null;
        for (int i = 1; i <= timestamps.length; i++) {
            assertTrue(rs.next());
            assertEquals("TR" + i, rs.getString("TRIGGERNAME"));
            Timestamp ts = rs.getTimestamp("CREATIONTIMESTAMP");
            assertNotNull(ts);
            if (prev != null && !prev.before(ts)) {
                fail(prev + " expected to be before " + ts);
            }
            prev = ts;
        }
        JDBC.assertEmpty(rs);

        rollback();
    }

    /**
     * Set a system property that makes the next CREATE TRIGGER operation
     * use the specified timestamp instead of the current time when
     * constructing the creation timestamp.
     */
    private void overrideTriggerCreationTime(Timestamp ts) {
        setSystemProperty(OVERRIDE_TIME_PROP, String.valueOf(ts.getTime()));
    }
}
