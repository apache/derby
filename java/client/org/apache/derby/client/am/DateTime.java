/*

   Derby - Class org.apache.derby.client.am.DateTime

   Copyright (c) 2001, 2005 The Apache Software Foundation or its licensors, where applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*/
package org.apache.derby.client.am;


/**
 * High performance converters from date/time byte encodings to JDBC Date, Time and Timestamp objects.
 * <p/>
 * Using this class for direct date/time conversions from bytes offers superior performance over the alternative method
 * of first constructing a Java String from the encoded bytes, and then using {@link java.sql.Date#valueOf
 * java.sql.Date.valueOf()}, {@link java.sql.Time#valueOf java.sql.Time.valueOf()} or {@link java.sql.Timestamp#valueOf
 * java.sql.Timestamp.valueOf()}.
 * <p/>
 */
public class DateTime {

    // Hide the default constructor
    private DateTime() {
    }

    private static final int dateRepresentationLength = 10;
    private static final int timeRepresentationLength = 8;
    private static final int timestampRepresentationLength = 26;

    // *********************************************************
    // ********** Output converters (byte[] -> class) **********
    // *********************************************************

    /**
     * Expected character representation is DERBY string representation of a date, which is in one of the following
     * format.
     */
    public static final java.sql.Date dateBytesToDate(byte[] buffer,
                                                      int offset,
                                                      java.sql.Date recyclableDate) {
        int year, month, day;

        String date = new String(buffer, offset, DateTime.dateRepresentationLength);
        int yearIndx, monthIndx, dayIndx;
        if (date.charAt(4) == '-') {
            // JIS format: yyyy-mm-dd.
            yearIndx = 0;
            monthIndx = 5;
            dayIndx = 8;
        } else {
            throw new java.lang.IllegalArgumentException("Unsupported date format!");
        }

        int zeroBase = ((int) '0');
        // Character arithmetic is used rather than
        // the less efficient Integer.parseInt (date.substring()).
        year =
                1000 * (((int) date.charAt(yearIndx)) - zeroBase) +
                100 * (((int) date.charAt(yearIndx + 1)) - zeroBase) +
                10 * (((int) date.charAt(yearIndx + 2)) - zeroBase) +
                (((int) date.charAt(yearIndx + 3)) - zeroBase) -
                1900;
        month =
                10 * (((int) date.charAt(monthIndx)) - zeroBase) +
                (((int) date.charAt(monthIndx + 1)) - zeroBase) -
                1;
        day =
                10 * (((int) date.charAt(dayIndx)) - zeroBase) +
                (((int) date.charAt(dayIndx + 1)) - zeroBase);

        if (recyclableDate == null) {
            return new java.sql.Date(year, month, day);
        } else {
            recyclableDate.setYear(year);
            recyclableDate.setMonth(month);
            recyclableDate.setDate(day);
            return recyclableDate;
        }
    }

    /**
     * Expected character representation is DERBY string representation of a time, which is in one of the following
     * format: hh.mm.ss.
     */
    public static final java.sql.Time timeBytesToTime(byte[] buffer,
                                                      int offset,
                                                      java.sql.Time recyclableTime) {
        int hour, minute, second;

        String time = new String(buffer, offset, DateTime.timeRepresentationLength);
        int zeroBase = ((int) '0');

        // compute hour.
        hour =
                10 * (((int) time.charAt(0)) - zeroBase) +
                (((int) time.charAt(1)) - zeroBase);
        // compute minute.
        minute =
                10 * (((int) time.charAt(3)) - zeroBase) +
                (((int) time.charAt(4)) - zeroBase);
        // compute second.
        second =
                10 * (((int) time.charAt(6)) - zeroBase) +
                (((int) time.charAt(7)) - zeroBase);

        if (recyclableTime == null) {
            return new java.sql.Time(hour, minute, second);
        } else {
            recyclableTime.setHours(hour);
            recyclableTime.setMinutes(minute);
            recyclableTime.setSeconds(second);
            return recyclableTime;
        }
    }

    /**
     * Expected character representation is DERBY string representation of a timestamp:
     * <code>yyyy-mm-dd-hh.mm.ss.ffffff</code>.
     */
    public static final java.sql.Timestamp timestampBytesToTimestamp(byte[] buffer,
                                                                     int offset,
                                                                     java.sql.Timestamp recyclableTimestamp) {
        int year, month, day, hour, minute, second, fraction;

        String timestamp = new String(buffer, offset, DateTime.timestampRepresentationLength);
        int zeroBase = ((int) '0');

        year =
                1000 * (((int) timestamp.charAt(0)) - zeroBase) +
                100 * (((int) timestamp.charAt(1)) - zeroBase) +
                10 * (((int) timestamp.charAt(2)) - zeroBase) +
                (((int) timestamp.charAt(3)) - zeroBase) -
                1900;
        month =
                10 * (((int) timestamp.charAt(5)) - zeroBase) +
                (((int) timestamp.charAt(6)) - zeroBase) -
                1;
        day =
                10 * (((int) timestamp.charAt(8)) - zeroBase) +
                (((int) timestamp.charAt(9)) - zeroBase);
        hour =
                10 * (((int) timestamp.charAt(11)) - zeroBase) +
                (((int) timestamp.charAt(12)) - zeroBase);
        minute =
                10 * (((int) timestamp.charAt(14)) - zeroBase) +
                (((int) timestamp.charAt(15)) - zeroBase);
        second =
                10 * (((int) timestamp.charAt(17)) - zeroBase) +
                (((int) timestamp.charAt(18)) - zeroBase);
        fraction =
                100000 * (((int) timestamp.charAt(20)) - zeroBase) +
                10000 * (((int) timestamp.charAt(21)) - zeroBase) +
                1000 * (((int) timestamp.charAt(22)) - zeroBase) +
                100 * (((int) timestamp.charAt(23)) - zeroBase) +
                10 * (((int) timestamp.charAt(24)) - zeroBase) +
                (((int) timestamp.charAt(25)) - zeroBase);

        if (recyclableTimestamp == null) {
            return new java.sql.Timestamp(year, month, day, hour, minute, second, fraction * 1000);
        } else {
            recyclableTimestamp.setYear(year);
            recyclableTimestamp.setMonth(month);
            recyclableTimestamp.setDate(day);
            recyclableTimestamp.setHours(hour);
            recyclableTimestamp.setMinutes(minute);
            recyclableTimestamp.setSeconds(second);
            recyclableTimestamp.setNanos(fraction * 1000);
            return recyclableTimestamp;
        }
    }

    // ********************************************************
    // ********** Input converters (class -> byte[]) **********
    // ********************************************************

    /**
     * The returned character representation is in JDBC date format: <code>yyyy-mm-dd</code> date format in DERBY string
     * representation of a date.
     */
    public static final int dateToDateBytes(byte[] buffer,
                                            int offset,
                                            java.sql.Date date) throws ConversionException {
        int year = date.getYear() + 1900;
        if (year > 9999) {
            throw new ConversionException("Year exceeds the maximum \"9999\".");
        }
        int month = date.getMonth() + 1;
        int day = date.getDate();

        char[] dateChars = new char[DateTime.dateRepresentationLength];
        int zeroBase = (int) '0';
        dateChars[0] = (char) (year / 1000 + zeroBase);
        dateChars[1] = (char) ((year % 1000) / 100 + zeroBase);
        dateChars[2] = (char) ((year % 100) / 10 + zeroBase);
        dateChars[3] = (char) (year % 10 + +zeroBase);
        dateChars[4] = '-';
        dateChars[5] = (char) (month / 10 + zeroBase);
        dateChars[6] = (char) (month % 10 + zeroBase);
        dateChars[7] = '-';
        dateChars[8] = (char) (day / 10 + zeroBase);
        dateChars[9] = (char) (day % 10 + zeroBase);
        byte[] dateBytes = (new String(dateChars)).getBytes();
        System.arraycopy(dateBytes, 0, buffer, offset, DateTime.dateRepresentationLength);

        return DateTime.dateRepresentationLength;
    }

    /**
     * The returned character representation is in JDBC time escape format: <code>hh:mm:ss</code>, which is the same as
     * JIS time format in DERBY string representation of a time.
     */
    public static final int timeToTimeBytes(byte[] buffer,
                                            int offset,
                                            java.sql.Time time) {
        int hour = time.getHours();
        int minute = time.getMinutes();
        int second = time.getSeconds();

        char[] timeChars = new char[DateTime.timeRepresentationLength];
        int zeroBase = (int) '0';
        timeChars[0] = (char) (hour / 10 + zeroBase);
        timeChars[1] = (char) (hour % 10 + +zeroBase);
        timeChars[2] = ':';
        timeChars[3] = (char) (minute / 10 + zeroBase);
        timeChars[4] = (char) (minute % 10 + zeroBase);
        timeChars[5] = ':';
        timeChars[6] = (char) (second / 10 + zeroBase);
        timeChars[7] = (char) (second % 10 + zeroBase);
        byte[] timeBytes = (new String(timeChars)).getBytes();
        System.arraycopy(timeBytes, 0, buffer, offset, DateTime.timeRepresentationLength);

        return DateTime.timeRepresentationLength;
    }

    /**
     * The returned character representation is in DERBY string representation of a timestamp:
     * <code>yyyy-mm-dd-hh.mm.ss.ffffff</code>.
     */
    public static final int timestampToTimestampBytes(byte[] buffer,
                                                      int offset,
                                                      java.sql.Timestamp timestamp) throws ConversionException {
        int year = timestamp.getYear() + 1900;
        if (year > 9999) {
            throw new ConversionException("Year exceeds the maximum \"9999\".");
        }
        int month = timestamp.getMonth() + 1;
        int day = timestamp.getDate();
        int hour = timestamp.getHours();
        int minute = timestamp.getMinutes();
        int second = timestamp.getSeconds();
        int microsecond = timestamp.getNanos() / 1000;

        char[] timestampChars = new char[DateTime.timestampRepresentationLength];
        int zeroBase = (int) '0';
        timestampChars[0] = (char) (year / 1000 + zeroBase);
        timestampChars[1] = (char) ((year % 1000) / 100 + zeroBase);
        timestampChars[2] = (char) ((year % 100) / 10 + zeroBase);
        timestampChars[3] = (char) (year % 10 + +zeroBase);
        timestampChars[4] = '-';
        timestampChars[5] = (char) (month / 10 + zeroBase);
        timestampChars[6] = (char) (month % 10 + zeroBase);
        timestampChars[7] = '-';
        timestampChars[8] = (char) (day / 10 + zeroBase);
        timestampChars[9] = (char) (day % 10 + zeroBase);
        timestampChars[10] = '-';
        timestampChars[11] = (char) (hour / 10 + zeroBase);
        timestampChars[12] = (char) (hour % 10 + zeroBase);
        timestampChars[13] = '.';
        timestampChars[14] = (char) (minute / 10 + zeroBase);
        timestampChars[15] = (char) (minute % 10 + zeroBase);
        timestampChars[16] = '.';
        timestampChars[17] = (char) (second / 10 + zeroBase);
        timestampChars[18] = (char) (second % 10 + zeroBase);
        timestampChars[19] = '.';
        timestampChars[20] = (char) (microsecond / 100000 + zeroBase);
        timestampChars[21] = (char) ((microsecond % 100000) / 10000 + zeroBase);
        timestampChars[22] = (char) ((microsecond % 10000) / 1000 + zeroBase);
        timestampChars[23] = (char) ((microsecond % 1000) / 100 + zeroBase);
        timestampChars[24] = (char) ((microsecond % 100) / 10 + zeroBase);
        timestampChars[25] = (char) (microsecond % 10 + zeroBase);

        byte[] timestampBytes = (new String(timestampChars)).getBytes();
        System.arraycopy(timestampBytes, 0, buffer, offset, DateTime.timestampRepresentationLength);

        return DateTime.timestampRepresentationLength;
    }

    // *********************************************************
    // ******* CROSS output converters (byte[] -> class) *******
    // *********************************************************

    /**
     * Expected character representation is DERBY string representation of a date, which is in one of the following
     * format.
     */
    public static final java.sql.Timestamp dateBytesToTimestamp(byte[] buffer,
                                                                int offset,
                                                                java.sql.Timestamp recyclableTimestamp) {
        int year, month, day;

        String date = new String(buffer, offset, DateTime.dateRepresentationLength);
        int yearIndx, monthIndx, dayIndx;

        yearIndx = 0;
        monthIndx = 5;
        dayIndx = 8;

        int zeroBase = ((int) '0');
        // Character arithmetic is used rather than
        // the less efficient Integer.parseInt (date.substring()).
        year =
                1000 * (((int) date.charAt(yearIndx)) - zeroBase) +
                100 * (((int) date.charAt(yearIndx + 1)) - zeroBase) +
                10 * (((int) date.charAt(yearIndx + 2)) - zeroBase) +
                (((int) date.charAt(yearIndx + 3)) - zeroBase) -
                1900;
        month =
                10 * (((int) date.charAt(monthIndx)) - zeroBase) +
                (((int) date.charAt(monthIndx + 1)) - zeroBase) -
                1;
        day =
                10 * (((int) date.charAt(dayIndx)) - zeroBase) +
                (((int) date.charAt(dayIndx + 1)) - zeroBase);

        if (recyclableTimestamp == null) {
            return new java.sql.Timestamp(year, month, day, 0, 0, 0, 0);
        } else {
            recyclableTimestamp.setYear(year);
            recyclableTimestamp.setMonth(month);
            recyclableTimestamp.setDate(day);
            recyclableTimestamp.setHours(0);
            recyclableTimestamp.setMinutes(0);
            recyclableTimestamp.setSeconds(0);
            recyclableTimestamp.setNanos(0);
            return recyclableTimestamp;
        }
    }

    /**
     * Expected character representation is DERBY string representation of a time, which is in one of the following
     * format.
     */
    public static final java.sql.Timestamp timeBytesToTimestamp(byte[] buffer,
                                                                int offset,
                                                                java.sql.Timestamp recyclableTimestamp) {
        int hour, minute, second;

        String time = new String(buffer, offset, DateTime.timeRepresentationLength);
        int zeroBase = ((int) '0');

        // compute hour.
        hour =
                10 * (((int) time.charAt(0)) - zeroBase) +
                (((int) time.charAt(1)) - zeroBase);
        // compute minute.
        minute =
                10 * (((int) time.charAt(3)) - zeroBase) +
                (((int) time.charAt(4)) - zeroBase);
        // compute second   JIS format: hh:mm:ss.
        second =
                10 * (((int) time.charAt(6)) - zeroBase) +
                (((int) time.charAt(7)) - zeroBase);

        if (recyclableTimestamp == null) {
            return new java.sql.Timestamp(0, 0, 1, hour, minute, second, 0);
        } else {
            recyclableTimestamp.setYear(0);
            recyclableTimestamp.setMonth(0);
            recyclableTimestamp.setDate(1);
            recyclableTimestamp.setHours(hour);
            recyclableTimestamp.setMinutes(minute);
            recyclableTimestamp.setSeconds(second);
            recyclableTimestamp.setNanos(0);
            return recyclableTimestamp;
        }
    }

    /**
     * Expected character representation is DERBY string representation of a timestamp:
     * <code>yyyy-mm-dd-hh.mm.ss.ffffff</code>.
     */
    public static final java.sql.Date timestampBytesToDate(byte[] buffer,
                                                           int offset,
                                                           java.sql.Date recyclableDate) {
        int year, month, day;

        String timestamp = new String(buffer, offset, DateTime.timestampRepresentationLength);
        int zeroBase = ((int) '0');

        year =
                1000 * (((int) timestamp.charAt(0)) - zeroBase) +
                100 * (((int) timestamp.charAt(1)) - zeroBase) +
                10 * (((int) timestamp.charAt(2)) - zeroBase) +
                (((int) timestamp.charAt(3)) - zeroBase) -
                1900;
        month =
                10 * (((int) timestamp.charAt(5)) - zeroBase) +
                (((int) timestamp.charAt(6)) - zeroBase) -
                1;
        day =
                10 * (((int) timestamp.charAt(8)) - zeroBase) +
                (((int) timestamp.charAt(9)) - zeroBase);

        if (recyclableDate == null) {
            return new java.sql.Date(year, month, day);
        } else {
            recyclableDate.setYear(year);
            recyclableDate.setMonth(month);
            recyclableDate.setDate(day);
            return recyclableDate;
        }
    }

    /**
     * Expected character representation is DERBY string representation of a timestamp:
     * <code>yyyy-mm-dd-hh.mm.ss.ffffff</code>.
     */
    public static final java.sql.Time timestampBytesToTime(byte[] buffer,
                                                           int offset,
                                                           java.sql.Time recyclableTime) {
        int hour, minute, second;

        String timestamp = new String(buffer, offset, DateTime.timestampRepresentationLength);
        int zeroBase = ((int) '0');

        hour =
                10 * (((int) timestamp.charAt(11)) - zeroBase) +
                (((int) timestamp.charAt(12)) - zeroBase);
        minute =
                10 * (((int) timestamp.charAt(14)) - zeroBase) +
                (((int) timestamp.charAt(15)) - zeroBase);
        second =
                10 * (((int) timestamp.charAt(17)) - zeroBase) +
                (((int) timestamp.charAt(18)) - zeroBase);

        if (recyclableTime == null) {
            return new java.sql.Time(hour, minute, second);
        } else {
            recyclableTime.setYear(hour);
            recyclableTime.setMonth(minute);
            recyclableTime.setDate(second);
            return recyclableTime;
        }
    }

    // *********************************************************
    // ******* CROSS input converters (class -> byte[]) ********
    // *********************************************************

    /**
     * The returned character representation is in JDBC date escape format: <code>yyyy-mm-dd</code>, which is the same
     * as JIS date format in DERBY string representation of a date.
     */
    public static final int timestampToDateBytes(byte[] buffer,
                                                 int offset,
                                                 java.sql.Timestamp timestamp) throws ConversionException {
        int year = timestamp.getYear() + 1900;
        if (year > 9999) {
            throw new ConversionException("Year exceeds the maximum \"9999\".");
        }
        int month = timestamp.getMonth() + 1;
        int day = timestamp.getDate();

        char[] dateChars = new char[DateTime.dateRepresentationLength];
        int zeroBase = (int) '0';
        dateChars[0] = (char) (year / 1000 + zeroBase);
        dateChars[1] = (char) ((year % 1000) / 100 + zeroBase);
        dateChars[2] = (char) ((year % 100) / 10 + zeroBase);
        dateChars[3] = (char) (year % 10 + +zeroBase);
        dateChars[4] = '-';
        dateChars[5] = (char) (month / 10 + zeroBase);
        dateChars[6] = (char) (month % 10 + zeroBase);
        dateChars[7] = '-';
        dateChars[8] = (char) (day / 10 + zeroBase);
        dateChars[9] = (char) (day % 10 + zeroBase);
        byte[] dateBytes = (new String(dateChars)).getBytes();
        System.arraycopy(dateBytes, 0, buffer, offset, DateTime.dateRepresentationLength);

        return DateTime.dateRepresentationLength;
    }

    /**
     * The returned character representation is in JDBC time escape format: <code>hh:mm:ss</code>, which is the same as
     * JIS time format in DERBY string representation of a time.
     */
    public static final int timestampToTimeBytes(byte[] buffer,
                                                 int offset,
                                                 java.sql.Timestamp timestamp) {
        int hour = timestamp.getHours();
        int minute = timestamp.getMinutes();
        int second = timestamp.getSeconds();

        char[] timeChars = new char[DateTime.timeRepresentationLength];
        int zeroBase = (int) '0';
        timeChars[0] = (char) (hour / 10 + zeroBase);
        timeChars[1] = (char) (hour % 10 + +zeroBase);
        timeChars[2] = ':';
        timeChars[3] = (char) (minute / 10 + zeroBase);
        timeChars[4] = (char) (minute % 10 + zeroBase);
        timeChars[5] = ':';
        timeChars[6] = (char) (second / 10 + zeroBase);
        timeChars[7] = (char) (second % 10 + zeroBase);
        byte[] timeBytes = (new String(timeChars)).getBytes();
        System.arraycopy(timeBytes, 0, buffer, offset, DateTime.timeRepresentationLength);

        return DateTime.timeRepresentationLength;
    }

    /**
     * The returned character representation is in DERBY string representation of a timestamp:
     * <code>yyyy-mm-dd-hh.mm.ss.ffffff</code>.
     */
    public static final int dateToTimestampBytes(byte[] buffer,
                                                 int offset,
                                                 java.sql.Date date) throws ConversionException {
        int year = date.getYear() + 1900;
        if (year > 9999) {
            throw new ConversionException("Year exceeds the maximum \"9999\".");
        }
        int month = date.getMonth() + 1;
        int day = date.getDate();

        char[] timestampChars = new char[DateTime.timestampRepresentationLength];
        int zeroBase = (int) '0';
        timestampChars[0] = (char) (year / 1000 + zeroBase);
        timestampChars[1] = (char) ((year % 1000) / 100 + zeroBase);
        timestampChars[2] = (char) ((year % 100) / 10 + zeroBase);
        timestampChars[3] = (char) (year % 10 + +zeroBase);
        timestampChars[4] = '-';
        timestampChars[5] = (char) (month / 10 + zeroBase);
        timestampChars[6] = (char) (month % 10 + zeroBase);
        timestampChars[7] = '-';
        timestampChars[8] = (char) (day / 10 + zeroBase);
        timestampChars[9] = (char) (day % 10 + zeroBase);
        timestampChars[10] = '-';
        timestampChars[11] = '0';
        timestampChars[12] = '0';
        timestampChars[13] = '.';
        timestampChars[14] = '0';
        timestampChars[15] = '0';
        timestampChars[16] = '.';
        timestampChars[17] = '0';
        timestampChars[18] = '0';
        timestampChars[19] = '.';
        timestampChars[20] = '0';
        timestampChars[21] = '0';
        timestampChars[22] = '0';
        timestampChars[23] = '0';
        timestampChars[24] = '0';
        timestampChars[25] = '0';

        byte[] timestampBytes = (new String(timestampChars)).getBytes();
        System.arraycopy(timestampBytes, 0, buffer, offset, DateTime.timestampRepresentationLength);

        return DateTime.timestampRepresentationLength;
    }

    /**
     * The returned character representation is in DERBY string representation of a timestamp:
     * <code>yyyy-mm-dd-hh.mm.ss.ffffff</code>.
     */
    public static final int timeToTimestampBytes(byte[] buffer,
                                                 int offset,
                                                 java.sql.Time time) {
        int hour = time.getHours();
        int minute = time.getMinutes();
        int second = time.getSeconds();

        char[] timestampChars = new char[DateTime.timestampRepresentationLength];
        int zeroBase = (int) '0';
        timestampChars[0] = '1';
        timestampChars[1] = '9';
        timestampChars[2] = '0';
        timestampChars[3] = '0';
        timestampChars[4] = '-';
        timestampChars[5] = '0';
        timestampChars[6] = '1';
        timestampChars[7] = '-';
        timestampChars[8] = '0';
        timestampChars[9] = '1';
        timestampChars[10] = '-';
        timestampChars[11] = (char) (hour / 10 + zeroBase);
        timestampChars[12] = (char) (hour % 10 + zeroBase);
        timestampChars[13] = '.';
        timestampChars[14] = (char) (minute / 10 + zeroBase);
        timestampChars[15] = (char) (minute % 10 + zeroBase);
        timestampChars[16] = '.';
        timestampChars[17] = (char) (second / 10 + zeroBase);
        timestampChars[18] = (char) (second % 10 + zeroBase);
        timestampChars[19] = '.';
        timestampChars[20] = '0';
        timestampChars[21] = '0';
        timestampChars[22] = '0';
        timestampChars[23] = '0';
        timestampChars[24] = '0';
        timestampChars[25] = '0';

        byte[] timestampBytes = (new String(timestampChars)).getBytes();
        System.arraycopy(timestampBytes, 0, buffer, offset, DateTime.timestampRepresentationLength);

        return DateTime.timestampRepresentationLength;
    }
}

