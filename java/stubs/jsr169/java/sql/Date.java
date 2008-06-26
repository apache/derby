/* 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package java.sql;

import java.text.SimpleDateFormat;

/**
 * A Date class which can consume and produce dates in SQL Date format.
 * <p>
 * The SQL date format represents a date as yyyy-mm-dd. Note that this date
 * format only deals with year, month and day values. There are no values for
 * hours, minutes, seconds.
 * <p>
 * This contrasts with regular java.util.Date values, which include time values
 * for hours, minutes, seconds, milliseconds.
 * <p>
 * Time points are handled as millisecond values - milliseconds since the epoch,
 * January 1st 1970, 00:00:00.000 GMT. Time values passed to the java.sql.Date
 * class are "normalized" to the time 00:00:00.000 GMT on the date implied by
 * the time value.
 */
public class Date extends java.util.Date {

    private static final long serialVersionUID = 1511598038487230103L;

    /**
     * Creates a Date which corresponds to the day implied by the supplied
     * theDate milliseconds time value.
     * 
     * @param theDate -
     *            a time value in milliseconds since the epoch - January 1 1970
     *            00:00:00 GMT. The time value (hours, minutes, seconds,
     *            milliseconds) stored in the Date object is adjusted to
     *            correspond to 00:00:00 GMT on the day implied by the supplied
     *            time value.
     */
    public Date(long theDate) {
        super(normalizeTime(theDate));
    }


    /**
     * Produces a string representation of the Date in SQL format
     * 
     * @return a string representation of the Date in SQL format - "yyyy-mm-dd".
     */
    public String toString() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd"); //$NON-NLS-1$
        return dateFormat.format(this);
    }

    /**
     * Creates a Date from a string representation of a date in SQL format.
     * 
     * @param dateString
     *            the string representation of a date in SQL format -
     *            "yyyy-mm-dd".
     * @return the Date object
     * @throws IllegalArgumentException
     *             if the format of the supplied string does not match the SQL
     *             format.
     */
    public static Date valueOf(String dateString) {
        return new Date( 0L );
    }

    /*
     * Private method which normalizes a Time value, removing all low
     * significance digits corresponding to milliseconds, seconds, minutes and
     * hours, so that the returned Time value corresponds to 00:00:00 GMT on a
     * particular day.
     */
    private static long normalizeTime(long theTime) {
        return theTime;
    }
}
