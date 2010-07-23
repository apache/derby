/*

   Derby - Class org.apache.derby.iapi.util.CheapDateFormatter

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

package org.apache.derby.iapi.util;

import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

/**
 * This class contains static methods for formatting dates into Strings.
 * It can be used where standard Date formatting is judged to be too
 * expensive.
 */
public class CheapDateFormatter {
    private static final TimeZone GMT = TimeZone.getTimeZone("GMT");

	/**
	 * This method formats the current date into a String. The input is
	 * a long representing the number of milliseconds since Jan. 1, 1970.
	 * The output is a String in the form yyyy-mm-dd hh:mm:ss.ddd GMT.
	 *
	 * The purpose of this class is to format date strings without paying
	 * the price of instantiating ResourceBundles and Locales, which the
	 * java.util.Date class does whenever you format a date string.
	 * As a result, the output of this class is not localized, it does
	 * not take the local time zone into account, and it is possible that
	 * it will not be as accurate as the standard Date class. It is OK
	 * to use this method when, for example, formatting timestamps to
	 * write to db2j.LOG, but not for manipulating dates in language
	 * processing.
	 *
	 * @param time	The current time in milliseconds since Jan. 1, 1970
	 *
	 * @return The date formatted as yyyy-mm-dd hh:mm:ss.ddd GMT.
	 */
	public static String formatDate(long time) {
        // Get a GMT calendar with a well-known locale to help us calculate
        // the components of the date.
        Calendar cal = Calendar.getInstance(GMT, Locale.US);
        cal.setTimeInMillis(time);

        int year = cal.get(Calendar.YEAR);
        int month = cal.get(Calendar.MONTH) + 1; // convert 0-based to 1-based
        int days = cal.get(Calendar.DAY_OF_MONTH);
        int hours = cal.get(Calendar.HOUR_OF_DAY);
        int minutes = cal.get(Calendar.MINUTE);
        int seconds = cal.get(Calendar.SECOND);
        int millis = cal.get(Calendar.MILLISECOND);

		return year + "-" +
				twoDigits(month) + "-" +
				twoDigits(days) + " " +
				twoDigits(hours) + ":" +
				twoDigits(minutes) + ":" +
				twoDigits(seconds) + "." +
				threeDigits(millis) + " GMT";
	}

	private static String twoDigits(int val) {
		String retval;

		if (val < 10) {
			retval = "0" + val;
		} else {
			retval = Integer.toString(val);
		}

		return retval;
	}

	private static String threeDigits(int val) {
		String retval;

		if (val < 10) {
			retval = "00" + val;
		} else if (val < 100) {
			retval = "0" + val;
		} else {
			retval = Integer.toString(val);
		}

		return retval;
	}
}
