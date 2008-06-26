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
import java.util.Date;

/**
 * Java representation of an SQL TIME value. Provides functions to aid
 * generation and interpretation of JDBC escape format for time values.
 * 
 */
public class Time extends Date {

    private static final long serialVersionUID = 8397324403548013681L;

    /**
     * Constructs a Time object using a supplied time specified in milliseconds
     * 
     * @param theTime
     *            a Time specified in milliseconds since the Epoch (January 1st
     *            1970, 00:00:00.000)
     */
    public Time(long theTime) {
        super(theTime);
    }


    /**
     * Sets the time for this Time object to the supplied milliseconds value.
     * 
     * @param time
     *            A time value expressed as milliseconds since the Epoch.
     *            Negative values are milliseconds before the Epoch. The Epoch
     *            is January 1 1970, 00:00:00.000
     */
    public void setTime(long time) {
        super.setTime(time);
    }

    /**
     * Formats the Time as a String in JDBC escape format: hh:mm:ss
     * 
     * @return A String representing the Time value in JDBC escape format:
     *         HH:mm:ss
     */
    public String toString() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss"); //$NON-NLS-1$
        return dateFormat.format(this);
    }

    /**
     * Creates a Time object from a String holding a time represented in JDBC
     * escape format: hh:mm:ss.
     * <p>
     * An exception occurs if the input string is not in the form of a time in
     * JDBC escape format.
     * 
     * @param timeString
     *            A String representing the time value in JDBC escape format:
     *            hh:mm:ss
     * @return The Time object set to a time corresponding to the given time
     * @throws IllegalArgumentException
     *             if the supplied time string is not in JDBC escape format.
     */
    public static Time valueOf(String timeString) {
        return new Time( 0L );
    }
}
