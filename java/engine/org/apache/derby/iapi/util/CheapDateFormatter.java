/*

   Derby - Class org.apache.derby.iapi.util.CheapDateFormatter

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.util;

/**
 * This class contains static methods for formatting dates into Strings.
 * It can be used where standard Date formatting is judged to be too
 * expensive.
 */
public class CheapDateFormatter {
	static final long SECONDS  = 1000L;
	static final long MINUTES = SECONDS * 60L;
	static final long HOURS = MINUTES * 60L;
	static final long DAYS = HOURS * 24L;
	static final long NORMAL_YEAR = DAYS * 365L;
	static final long LEAP_YEAR = NORMAL_YEAR + DAYS;
	static final long FOURYEARS = (NORMAL_YEAR * 3L) + LEAP_YEAR;
	static final long END_OF_FIRST_YEAR = NORMAL_YEAR;
	static final long END_OF_SECOND_YEAR = END_OF_FIRST_YEAR + LEAP_YEAR;
	static final long END_OF_THIRD_YEAR = END_OF_SECOND_YEAR + NORMAL_YEAR;
	static final int[] DAYS_IN_MONTH = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
	static final int FEBRUARY = 1;

	/**
	 * This method formats the current date into a String. The input is
	 * a long representing the number of milliseconds since Jan. 1, 1970.
	 * The output is a String in the form yyyy/mm/dd hh:mm:ss.ddd GMT.
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
	 * @return The date formatted as yyyy/mm/dd hh:mm:ss.ddd GMT.
	 */
	public static String formatDate(long time) {
		// Assume not a leap year until we know otherwise
		boolean leapYear = false;

		// How many four year periods since Jan. 1, 1970?
		long year = ((time / FOURYEARS) * 4L);

		// How much time is left over after the four-year periods?
		long leftover = time % FOURYEARS;
		time -= (year / 4L) * FOURYEARS;

		year += 1970L;

		// Does time extend past end of first year in four-year period?
		if (leftover >= END_OF_FIRST_YEAR) {
			year++;
			time -= NORMAL_YEAR;
		}

		// Does time extend past end of second year in four-year period?
		if (leftover >= END_OF_SECOND_YEAR) {
			year++;
			time -= NORMAL_YEAR;
		}

		// Does time extend past end of third year in four-year period?
		if (leftover >= END_OF_THIRD_YEAR) {
			year++;
			time -= LEAP_YEAR;
		}

		// It's a leap year if divisible by 4, unless divisible by 100,
		// unless divisible by 400.
		if ((year % 4L) == 0) {
			if ((year % 100L) == 0) {
				if ((year % 400L) == 0) {
					leapYear = true;
				}
			}
			leapYear = true;
		}

		// What day of the year is this, starting at 1?
		long days = (time / DAYS) + 1;

		// What month is this, starting at 1?
		int month = 1;
		for (int i = 0; i < DAYS_IN_MONTH.length; i++) {
			int daysInMonth;

			if (leapYear && (i == FEBRUARY)) {
				// February has 29 days in a leap year
				daysInMonth = 29;
			} else {
				// Get number of days in next month
				daysInMonth = DAYS_IN_MONTH[i];
			}

			// Is date after the month we are looking at?
			if (days > daysInMonth) {
				// Count number of months
				month++;

				// Subtract number of days in month
				days -= daysInMonth;
			} else {
				// Don't bother to look any more - the date is within
				// the current month.
				break;
			}
		}

		// How much time is left after days are accounted for?
		time %= DAYS;

		long hours = time / HOURS;

		// How much time is left after hours are accounted for?
		time %= HOURS;

		long minutes = time / MINUTES;

		// How much time is left after minutes are accounted for?
		time %= MINUTES;

		long seconds = time / SECONDS;

		// How much time is left after seconds are accounted for?
		time %= SECONDS;

		return year + "-" +
				twoDigits(month) + "-" +
				twoDigits(days) + " " +
				twoDigits(hours) + ":" +
				twoDigits(minutes) + ":" +
				twoDigits(seconds) + "." +
				threeDigits(time) + " GMT";
	}

	private static String twoDigits(long val) {
		String retval;

		if (val < 10) {
			retval = "0" + val;
		} else {
			retval = Long.toString(val);
		}

		return retval;
	}

	private static String threeDigits(long val) {
		String retval;

		if (val < 10) {
			retval = "00" + val;
		} else if (val < 100) {
			retval = "0" + val;
		} else {
			retval = Long.toString(val);
		}

		return retval;
	}
}
