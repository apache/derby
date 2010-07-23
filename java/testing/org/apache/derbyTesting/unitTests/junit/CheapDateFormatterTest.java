/*

   Derby - Class org.apache.derbyTesting.unitTests.junit.CheapDateFormatterTest

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

package org.apache.derbyTesting.unitTests.junit;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derby.iapi.util.CheapDateFormatter;
import org.apache.derbyTesting.junit.BaseTestCase;

/**
 * Unit tests for the CheapDateFormatter class.
 */
public class CheapDateFormatterTest extends BaseTestCase {
    public CheapDateFormatterTest(String name) {
        super(name);
    }

    public static Test suite() {
        return new TestSuite(CheapDateFormatterTest.class);
    }

    /**
     * Tests for the {@code formatDate()} method.
     */
    public void testFormatDate() throws ParseException {
        assertDateString("1970-01-01 00:00:00.000 GMT"); // Epoch

        // DERBY-4752: Times the first day in a leap year used to be
        // formatted with month 13 in the previous year. Verify that this
        // works correctly for the first day of the leap year 2016.
        assertDateString("2015-12-31 23:59:59.999 GMT");
        assertDateString("2016-01-01 00:00:00.000 GMT");
        assertDateString("2016-01-01 00:00:00.001 GMT");
        assertDateString("2016-01-01 08:31:23.456 GMT");
        assertDateString("2016-01-02 12:00:00.000 GMT");

        // DERBY-4752: We used to get a one day skew each time we passed a
        // year divisible by four that was not a leap year (like 2100, 2200,
        // 2300, 2500, ...).
        assertDateString("2100-05-17 14:10:44.701 GMT");
        assertDateString("2927-06-07 00:00:00.000 GMT");
        assertDateString("9999-12-31 23:59:59.999 GMT");

        // DERBY-4752: Years divisible by 100, but not by 400, are not leap
        // years. Still, formatDate() used to return February 29 for the
        // following dates:
        assertDateString("2100-03-01 12:00:00.000 GMT");
        assertDateString("2200-03-02 12:00:00.000 GMT");
        assertDateString("2300-03-03 12:00:00.000 GMT");
        assertDateString("2500-03-04 12:00:00.000 GMT");

        // Year 8000 will be a leap year, unless a better calendar system
        // has been devised by then.
        assertDateString("8000-02-28 12:00:00.000 GMT");
        assertDateString("8000-02-29 12:00:00.000 GMT");
        assertDateString("8000-01-03 12:00:00.000 GMT");
    }

    /**
     * Convert a date string to a long representing milliseconds since Epoch,
     * feed that value to CheapDateFormatter.formatDate(), and verify that
     * the exact same date string is returned.
     *
     * @param date a string representing the date to test
     * @throws ParseException if the date string cannot be parsed
     */
    private void assertDateString(String date) throws ParseException {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z");
        long time = df.parse(date).getTime();
        assertEquals(date, CheapDateFormatter.formatDate(time));
    }
}
