/*
 * Derby - Class org.apache.derby.client.am.DateTimeValue
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.derby.client.am;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * This class represents a date or time value as it is represented in the
 * database. In contrast to {@code java.sql.Date}, {@code java.sql.Time} and
 * {@code java.sql.Timestamp}, which are based on {@code java.util.Date}, this
 * class does <b>not</b> represent the time as an offset from midnight,
 * January 1, 1970 GMT. Instead, it holds each component (year, month, day,
 * hour, minute, second, nanosecond) as it would have been represented in a
 * given calendar. Since it does not hold information about the time zone for
 * the time it represents, it does not point to a well-defined point in time
 * without being used together with a {@code java.util.Calendar} object.
 */
public class DateTimeValue {
    private final int year;
    private final int month;
    private final int day;
    private final int hours;
    private final int minutes;
    private final int seconds;
    private final int nanos;

    /**
     * Construct a {@code DateTimeValue} from a {@code java.util.Calendar}.
     *
     * @param cal the calendar from which to get the values of the fields
     * @param nanoFraction the nano second fraction of a second (the
     * milliseconds will be taken from {@code cal}, so only the six least
     * significant digits of this value are used)
     */
    private DateTimeValue(Calendar cal, int nanoFraction) {
        year = cal.get(Calendar.YEAR);
        month = cal.get(Calendar.MONTH);
        day = cal.get(Calendar.DAY_OF_MONTH);
        hours = cal.get(Calendar.HOUR_OF_DAY);
        minutes = cal.get(Calendar.MINUTE);
        seconds = cal.get(Calendar.SECOND);

        // In practice, we could probably just use nanoFraction directly here,
        // when it's set but since java.util.Calendar allows time zone offsets
        // to be specified in milliseconds, let's get the time zone adjusted
        // millisecond component too.
        int millis = cal.get(Calendar.MILLISECOND);
        nanos = (millis * 1000000) + (nanoFraction % 1000000);
    }

    /**
     * Create an instance from a {@code java.sql.Timestamp} using the specified
     * {@code java.util.Calendar}.
     */
    DateTimeValue(Date date, Calendar cal) {
        this(initCalendar(cal, date), 0);
    }

    /**
     * Create an instance from a {@code java.sql.Time} using the specified
     * {@code java.util.Calendar}.
     */
    DateTimeValue(Time time, Calendar cal) {
        this(initCalendar(cal, time), 0);
    }

    /**
     * Create an instance from a {@code java.sql.Timestamp} using the specified
     * {@code java.util.Calendar}.
     */
    DateTimeValue(Timestamp ts, Calendar cal) {
        this(initCalendar(cal, ts), ts.getNanos());
    }

    /**
     * Create an instance from a {@code java.sql.Date} using the default
     * calendar.
     */
    public DateTimeValue(Date date) {
        this(date, Calendar.getInstance());
    }

    /**
     * Create an instance from a {@code java.sql.Time} using the default
     * calendar.
     */
    public DateTimeValue(Time time) {
        this(time, Calendar.getInstance());
    }

    /**
     * Create an instance from a {@code java.sql.Timestamp} using the default
     * calendar.
     */
    public DateTimeValue(Timestamp ts) {
        this(ts, Calendar.getInstance());
    }

    /**
     * Set the time of a calendar.
     *
     * @param cal the calendar
     * @param date an object representing the new time of the calendar
     * @return the calendar (same as {@code cal})
     */
    private static Calendar initCalendar(Calendar cal, java.util.Date date) {
        cal.clear();
        cal.setTime(date);
        return cal;
    }

    /**
     * Get the year component.
     */
    public int getYear() {
        return year;
    }

    /**
     * Get the month component. First month is 0 ({@code Calendar.JANUARY}).
     */
    public int getMonth() {
        return month;
    }

    /**
     * Get day of month component. First day of the month is 1.
     */
    public int getDayOfMonth() {
        return day;
    }

    /**
     * Get hour of day component (24 hour clock).
     */
    public int getHours() {
        return hours;
    }

    /**
     * Get minute component.
     */
    public int getMinutes() {
        return minutes;
    }

    /**
     * Get second component.
     */
    public int getSeconds() {
        return seconds;
    }

    /**
     * Get nanosecond component.
     */
    public int getNanos() {
        return nanos;
    }
}
