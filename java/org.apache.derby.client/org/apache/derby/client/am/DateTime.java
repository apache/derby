/*

   Derby - Class org.apache.derby.client.am.DateTime

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
package org.apache.derby.client.am;

import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.reference.DRDAConstants;

import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import org.apache.derby.client.net.Typdef;


/**
 * <p>
 * High performance converters from date/time byte encodings to JDBC Date, Time and Timestamp objects.
 * </p>
 * <p>
 * Using this class for direct date/time conversions from bytes offers superior performance over the alternative method
 * of first constructing a Java String from the encoded bytes, and then using {@link java.sql.Date#valueOf
 * java.sql.Date.valueOf()}, {@link java.sql.Time#valueOf java.sql.Time.valueOf()} or {@link java.sql.Timestamp#valueOf
 * java.sql.Timestamp.valueOf()}.
 * </p>
 */
public class DateTime {

    // Hide the default constructor
    private DateTime() {
    }

    private static final int dateRepresentationLength = 10;
    private static final int timeRepresentationLength = 8;
    private static final int timestampRepresentationLength = DRDAConstants.DRDA_TIMESTAMP_LENGTH;

    // *********************************************************
    // ********** Output converters (byte[] -> class) **********
    // *********************************************************

    /**
     * Expected character representation is DERBY string representation of a date, 
     * which is in JIS format: <code> yyyy-mm-dd </code>
     * 
     * @param buffer    
     * @param offset    
     * @param recyclableCal
     * @param encoding            encoding of buffer data
     * @return  Date translated from  buffer with specified encoding
     */
    static final Date dateBytesToDate(byte[] buffer,
            int offset,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            Calendar recyclableCal,
            Charset encoding) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6231

        int year, month, day;

        String date = new String(buffer, offset, 
                DateTime.dateRepresentationLength,encoding);
        int yearIndx, monthIndx, dayIndx;
        if (date.charAt(4) == '-') {
            // JIS format: yyyy-mm-dd.
            yearIndx = 0;
            monthIndx = 5;
            dayIndx = 8;
        } else {
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            throw new IllegalArgumentException(
//IC see: https://issues.apache.org/jira/browse/DERBY-847
                SqlException.getMessageUtil().getTextMessage(
                    SQLState.LANG_FORMAT_EXCEPTION));
        }

        int zeroBase = ((int) '0');
        // Character arithmetic is used rather than
        // the less efficient Integer.parseInt (date.substring()).
        year =
                1000 * (((int) date.charAt(yearIndx)) - zeroBase) +
                100 * (((int) date.charAt(yearIndx + 1)) - zeroBase) +
                10 * (((int) date.charAt(yearIndx + 2)) - zeroBase) +
                (((int) date.charAt(yearIndx + 3)) - zeroBase);

        month =
                10 * (((int) date.charAt(monthIndx)) - zeroBase) +
                (((int) date.charAt(monthIndx + 1)) - zeroBase) -
                1;
        day =
                10 * (((int) date.charAt(dayIndx)) - zeroBase) +
                (((int) date.charAt(dayIndx + 1)) - zeroBase);

        Calendar cal = getCleanCalendar(recyclableCal);
        cal.set(year, month, day);
        return new Date(cal.getTimeInMillis());
    }

    
    /**
     * Expected character representation is DERBY string representation of time,
     * which is in the format: <code> hh.mm.ss </code>
     * @param buffer
     * @param offset
     * @param recyclableCal
     * @param encoding           encoding of buffer
     * @return  Time translated from buffer with specified encoding
     */
    static final Time timeBytesToTime(byte[] buffer,
                                      int offset,
                                      Calendar recyclableCal,
                                      Charset encoding)
    {
        int hour, minute, second;

        String time = new String(buffer, offset, 
                DateTime.timeRepresentationLength, encoding);
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

        Calendar cal = getCleanCalendar(recyclableCal);
        cal.set(1970, Calendar.JANUARY, 1, hour, minute, second);
        return new Time(cal.getTimeInMillis());
    }

    /**
     * See getTimestampLength() for an explanation of how timestamps are formatted.
     * 
     * @param buffer
     * @param offset
     * @param recyclableCal
     * @param encoding                encoding of buffer
     * @param supportsTimestampNanoseconds true if the server supports nanoseconds in timestamps
     * @return TimeStamp translated from buffer with specified encoding
     */
    static final Timestamp timestampBytesToTimestamp(
//IC see: https://issues.apache.org/jira/browse/DERBY-6231
            byte[] buffer, int offset, Calendar recyclableCal,
            Charset encoding, boolean supportsTimestampNanoseconds)
    {
        int year, month, day, hour, minute, second, fraction;
        String timestamp = new String
            ( buffer, offset, getTimestampLength( supportsTimestampNanoseconds ), encoding );
       
        Calendar cal = getCleanCalendar(recyclableCal);

        /* java.sql.Timestamp has nanosecond precision, so we have to keep
         * the parsed nanoseconds value and use that to set nanos.
         */
        int nanos = parseTimestampString(timestamp, cal, supportsTimestampNanoseconds);
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        Timestamp ts = new Timestamp(cal.getTimeInMillis());
        ts.setNanos( nanos );
        return ts;
    }

    /**
     * Parse a String of the form <code>yyyy-mm-dd-hh.mm.ss.ffffff[fff]</code>
     * and store the various fields into the received Calendar object.
     *
     * @param timestamp Timestamp value to parse, as a String.
     * @param cal Calendar into which to store the parsed fields.  Should not be null.
     * @param supportsTimestampNanoseconds true if the server supports nanoseconds in timestamps
     *
     * @return The nanoseconds field as parsed from the timestamp string.
     *  This cannot be set in the Calendar object but we still want to
     *  preserve the value, in case the caller needs it (for example, to
     *  create a java.sql.Timestamp with nanosecond precision).
     */
    private static int parseTimestampString(String timestamp,
        Calendar cal, boolean supportsTimestampNanoseconds )
    {
        int zeroBase = ((int) '0');

        cal.set(Calendar.YEAR,
                1000 * (((int) timestamp.charAt(0)) - zeroBase) +
                100 * (((int) timestamp.charAt(1)) - zeroBase) +
                10 * (((int) timestamp.charAt(2)) - zeroBase) +
                (((int) timestamp.charAt(3)) - zeroBase));

        cal.set(Calendar.MONTH,
                10 * (((int) timestamp.charAt(5)) - zeroBase) +
                (((int) timestamp.charAt(6)) - zeroBase) - 1);

        cal.set(Calendar.DAY_OF_MONTH,
                10 * (((int) timestamp.charAt(8)) - zeroBase) +
                (((int) timestamp.charAt(9)) - zeroBase));

        cal.set(Calendar.HOUR,
                10 * (((int) timestamp.charAt(11)) - zeroBase) +
                (((int) timestamp.charAt(12)) - zeroBase));

        cal.set(Calendar.MINUTE,
                10 * (((int) timestamp.charAt(14)) - zeroBase) +
                (((int) timestamp.charAt(15)) - zeroBase));

        cal.set(Calendar.SECOND,
                10 * (((int) timestamp.charAt(17)) - zeroBase) +
                (((int) timestamp.charAt(18)) - zeroBase));

        int nanos = 
                100000000 * (((int) timestamp.charAt(20)) - zeroBase) +
                10000000 * (((int) timestamp.charAt(21)) - zeroBase) +
                1000000 * (((int) timestamp.charAt(22)) - zeroBase) +
                100000 * (((int) timestamp.charAt(23)) - zeroBase) +
                10000 * (((int) timestamp.charAt(24)) - zeroBase) +
                1000 * (((int) timestamp.charAt(25)) - zeroBase);
 
        if ( supportsTimestampNanoseconds )
        {
            nanos += 100 * (((int) timestamp.charAt(26)) - zeroBase);
            nanos += 10 * (((int) timestamp.charAt(27)) - zeroBase);
            nanos += (((int) timestamp.charAt(28)) - zeroBase);
        }
        
        /* The "ffffff[fff]" that we parsed is nanoseconds.  In order to
         * capture that information inside of the MILLISECOND field
         * we have to divide by 1000000.
         */
        cal.set(Calendar.MILLISECOND, nanos / 1000000);
        
        return nanos;
    }

    // ********************************************************
    // ********** Input converters (class -> byte[]) **********
    // ********************************************************

    /**
     * Date is converted to a char representation in JDBC date format: <code>yyyy-mm-dd</code> date format
     * and then converted to bytes using UTF8 encoding
     * @param buffer  bytes in UTF8 encoding of the date
     * @param offset  write into the buffer from this offset 
     * @param date    date value
     * @return DateTime.dateRepresentationLength. This is the fixed length in 
     * bytes taken to represent the date value
     * @throws SqlException on error
     */
    public static final int dateToDateBytes(byte[] buffer,
                                            int offset,
                                            DateTimeValue date)
//IC see: https://issues.apache.org/jira/browse/DERBY-6231
    throws SqlException {
        int year = date.getYear();
        if (year > 9999) {
            throw new SqlException(null,
                new ClientMessageId(SQLState.YEAR_EXCEEDS_MAXIMUM),
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                year, "9999");
        }
        int month = date.getMonth() + 1;
        int day = date.getDayOfMonth();

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
        
        // Network server expects to read the date parameter value bytes with
        // UTF-8 encoding.  Reference - DERBY-1127
        // see DRDAConnThread.readAndSetParams
        byte[] dateBytes = (new String(dateChars)).getBytes(Typdef.UTF8ENCODING);
        System.arraycopy(dateBytes, 0, buffer, offset, DateTime.dateRepresentationLength);

        return DateTime.dateRepresentationLength;
    }

    /**
     * java.sql.Time is converted to character representation which is in JDBC time escape
     * format: <code>hh:mm:ss</code>, which is the same as JIS time format in DERBY string 
     * representation of a time.  The char representation is converted to bytes using UTF8 
     * encoding.
     * @param buffer  bytes in UTF8 encoding of the time
     * @param offset  write into the buffer from this offset 
     * @param time  java.sql.Time value
     * @return DateTime.timeRepresentationLength. This is the fixed length in 
     * bytes taken to represent the time value
     */
    public static final int timeToTimeBytes(byte[] buffer,
                                            int offset,
                                            DateTimeValue time)
    {
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
        
        // Network server expects to read the time parameter value bytes with
        // UTF-8 encoding.  Reference - DERBY-1127
        // see DRDAConnThread.readAndSetParams
        byte[] timeBytes = (new String(timeChars)).getBytes(Typdef.UTF8ENCODING);
        System.arraycopy(timeBytes, 0, buffer, offset, DateTime.timeRepresentationLength);

        return DateTime.timeRepresentationLength;
    }

    /**
     * See getTimestampLength() for an explanation of how timestamps are formatted.
     *
     * @param buffer  bytes in UTF8 encoding of the timestamp
     * @param offset  write into the buffer from this offset 
     * @param timestamp  timestamp value
     * @param supportsTimestampNanoseconds true if the server supports nanoseconds in timestamps
     * @return DateTime.timestampRepresentationLength. This is the fixed  length in bytes, taken to represent the timestamp value
     * @throws SqlException on error
     */
    public static final int timestampToTimestampBytes(byte[] buffer,
                                                      int offset,
                                                      DateTimeValue timestamp,
                                                      boolean supportsTimestampNanoseconds) 
//IC see: https://issues.apache.org/jira/browse/DERBY-6231
    throws SqlException {
        int year = timestamp.getYear();
        if (year > 9999) {
            throw new SqlException(null,
                new ClientMessageId(SQLState.YEAR_EXCEEDS_MAXIMUM),
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                year, "9999");
        }
        int month = timestamp.getMonth() + 1;
        int day = timestamp.getDayOfMonth();
        int hour = timestamp.getHours();
        int minute = timestamp.getMinutes();
        int second = timestamp.getSeconds();
        int microsecond = timestamp.getNanos() / 1000;

        int arrayLength = getTimestampLength( supportsTimestampNanoseconds );
        char[] timestampChars = new char[ arrayLength ];
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
        
        if ( supportsTimestampNanoseconds )
        {
            int nanosecondsOnly = timestamp.getNanos() % 1000;
            
            timestampChars[ 26 ] = (char) (nanosecondsOnly / 100 + zeroBase);
            timestampChars[ 27 ] = (char) ((nanosecondsOnly % 100) / 10 + zeroBase);
            timestampChars[ 28 ] = (char) (nanosecondsOnly % 10 + zeroBase);
        }

        // Network server expects to read the timestamp parameter value bytes with
        // UTF-8 encoding.  Reference - DERBY-1127
        // see DRDAConnThread.readAndSetParams
        String newtimestampString = new String(timestampChars);
        byte[] timestampBytes = newtimestampString.getBytes(Typdef.UTF8ENCODING);
        System.arraycopy(timestampBytes, 0, buffer, offset, arrayLength);

        return arrayLength;
    }

    // *********************************************************
    // ******* CROSS output converters (byte[] -> class) *******
    // *********************************************************

    
    /**
     * Expected character representation is DERBY string representation of a date
     * which is in JIS format: <code> yyyy-mm-dd </code>
     * 
     * @param buffer
     * @param offset
     * @param recyclableCal
     * @param encoding                encoding of buffer
     * @return Timestamp translated from buffer with specified encoding
     */
    static final Timestamp dateBytesToTimestamp(byte[] buffer,
                                                                int offset,
                                                                Calendar recyclableCal,
//IC see: https://issues.apache.org/jira/browse/DERBY-6231
                                                                Charset encoding)
    {
        int year, month, day;

        String date = new String(buffer, offset, DateTime.dateRepresentationLength,
                encoding);
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
                (((int) date.charAt(yearIndx + 3)) - zeroBase);

        month =
                10 * (((int) date.charAt(monthIndx)) - zeroBase) +
                (((int) date.charAt(monthIndx + 1)) - zeroBase) -
                1;
        day =
                10 * (((int) date.charAt(dayIndx)) - zeroBase) +
                (((int) date.charAt(dayIndx + 1)) - zeroBase);

        Calendar cal = getCleanCalendar(recyclableCal);
        cal.set(year, month, day, 0, 0, 0);
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        Timestamp ts = new Timestamp(cal.getTimeInMillis());
        ts.setNanos(0);
        return ts;
    }

    
    /**
     *  Expected character representation is DERBY string representation of time
     * which is in the format: <code> hh.mm.ss </code>
     * 
     * @param buffer
     * @param offset
     * @param recyclableCal
     * @param encoding                 encoding of buffer
     * @return Timestamp translated from buffer with specified encoding 
     * 
     */
    static final Timestamp timeBytesToTimestamp(byte[] buffer,
                                                int offset,
//IC see: https://issues.apache.org/jira/browse/DERBY-6231
//IC see: https://issues.apache.org/jira/browse/DERBY-6231
                                                Calendar recyclableCal,
                                                Charset encoding)
{
        int hour, minute, second;

        String time = new String(buffer, offset, 
                DateTime.timeRepresentationLength, encoding);
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

        // The SQL standard specifies that the date portion of the returned
        // timestamp should be set to the current date. See DERBY-889 for
        // more details.
        Calendar cal = getCleanCalendar(recyclableCal);
        cal.setTime(new java.util.Date());

        // Now override the time fields with the values we parsed.
        cal.set(Calendar.HOUR_OF_DAY, hour);
        cal.set(Calendar.MINUTE, minute);
        cal.set(Calendar.SECOND, second);

        // Derby's resolution for the TIME type is only seconds.
        cal.set(Calendar.MILLISECOND, 0);
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        return new Timestamp(cal.getTimeInMillis());
    }
    
    
    /**
     * See getTimestampLength() for an explanation of how timestamps are formatted.
     * 
     * @param buffer
     * @param offset
     * @param recyclableCal
     * @param encoding             encoding of buffer
     * @return Date translated from buffer with specified encoding
     */
    static final Date timestampBytesToDate(byte[] buffer,
                                           int offset,
//IC see: https://issues.apache.org/jira/browse/DERBY-6231
                                           Calendar recyclableCal,
                                           Charset encoding)
     {
        int year, month, day;

        String timestamp = new String(buffer, offset, 
                DateTime.timestampRepresentationLength, encoding);
        int zeroBase = ((int) '0');

        year =
                1000 * (((int) timestamp.charAt(0)) - zeroBase) +
                100 * (((int) timestamp.charAt(1)) - zeroBase) +
                10 * (((int) timestamp.charAt(2)) - zeroBase) +
                (((int) timestamp.charAt(3)) - zeroBase);

        month =
                10 * (((int) timestamp.charAt(5)) - zeroBase) +
                (((int) timestamp.charAt(6)) - zeroBase) -
                1;
        day =
                10 * (((int) timestamp.charAt(8)) - zeroBase) +
                (((int) timestamp.charAt(9)) - zeroBase);

        Calendar cal = getCleanCalendar(recyclableCal);
        cal.set(year, month, day);
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        return new Date(cal.getTimeInMillis());
    }

   
    /**
     * See getTimestampLength() for an explanation of how timestamps are formatted.
     * 
     * @param buffer
     * @param offset
     * @param recyclableCal
     * @param encoding            encoding of buffer
     * @return  Time translated from buffer with specified Encoding
     */
    static final Time timestampBytesToTime(byte[] buffer,
                                           int offset,
//IC see: https://issues.apache.org/jira/browse/DERBY-6231
                                           Calendar recyclableCal,
                                           Charset encoding)
    {
        /* When getting a java.sql.Time object from a TIMESTAMP value we
         * need to preserve the milliseconds from the timestamp.
         * 
         * Note: a Derby SQL TIME value has by definition resolution of only
         * a second so its millisecond value is always zero.  However,
         * java.sql.Time is not a direct mapping to the SQL Type; rather, it's
         * a JDBC type, and the JDBC java.sql.Time class has a precision of
         * milliseconds.  So when converting from a SQL TIMESTAMP we should
         * retain the millisecond precision.  DERBY-1816.
         *
         * In order to accomplish this we parse *all* fields of the timestamp
         * into a Calendar object, then create the java.sql.Time object from
         * that Calendar. This allows us to preserve the sub-second resolution
         * that is parsed from the timestamp. 
         */
 
        String timestamp = new String(buffer, offset, 
                DateTime.timestampRepresentationLength, encoding);
       
        Calendar cal = getCleanCalendar(recyclableCal);

        /* Note that "parseTimestampString()" returns microseconds but we
         * ignore micros because java.sql.Time only has millisecond precision.
         */
        parseTimestampString(timestamp, cal, false);

        /* Java API indicates that the date components of a Time value
         * must be set to January 1, 1970. So override those values now.
         */
        cal.set(1970, Calendar.JANUARY, 1);
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        return new Time(cal.getTimeInMillis());
    }

    /**
     * Return a clean (i.e. all values cleared out) Calendar object
     * that can be used for creating Time, Timestamp, and Date objects.
     * If the received Calendar object is non-null, then just clear
     * that and return it.
     *
     * @param recyclableCal Calendar object to use if non-null.
     */
    private static Calendar getCleanCalendar(Calendar recyclableCal)
    {
        if (recyclableCal != null)
        {
            recyclableCal.clear();
            return recyclableCal;
        }

        /* Default GregorianCalendar initializes to current time.
         * Make sure we clear that out before returning, per the
         * contract of this method.
         */
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        Calendar result = new GregorianCalendar();
        result.clear();
        return result;
    }

    // *********************************************************
    // ******* CROSS input converters (class -> byte[]) ********
    // *********************************************************

    /**
     * Return the length of a timestamp depending on whether timestamps
     * should have full nanosecond precision or be truncated to just microseconds.
     * java.sql.Timestamp is converted to a character representation which is a DERBY string 
     * representation of a timestamp converted to bytes using UTF8 encoding.
     * For Derby 10.6 and above, this is <code>yyyy-mm-dd-hh.mm.ss.fffffffff</code>.
     * For Derby 10.5 and below, this is <code>yyyy-mm-dd-hh.mm.ss.ffffff</code>. See DERBY-2602.
     * and then converted to bytes using UTF8 encoding
     *
     * @param supportsTimestampNanoseconds true if the connection supports nanoseconds in timestamps
     *
     * @return the timestamp length
     */
    public static int getTimestampLength( boolean supportsTimestampNanoseconds )
    {
        return supportsTimestampNanoseconds ?
//IC see: https://issues.apache.org/jira/browse/DERBY-4614
            DRDAConstants.JDBC_TIMESTAMP_LENGTH : DRDAConstants.DRDA_OLD_TIMESTAMP_LENGTH;
    }

}

