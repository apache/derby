/*

   Derby - Class org.apache.derby.iapi.types.DateTimeDataValue

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.types;

import org.apache.derby.iapi.error.StandardException;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public interface DateTimeDataValue extends DataValueDescriptor
{
	public static final int YEAR_FIELD = 0;
	public static final int MONTH_FIELD = 1;
	public static final int DAY_FIELD = 2;
	public static final int HOUR_FIELD = 3;
	public static final int MINUTE_FIELD = 4;
	public static final int SECOND_FIELD = 5;

	/**
	 * Get the year number out of a date.
	 *
	 * @param result	The result of the previous call to this method, null
	 *					if not called yet.
	 *
	 * @return	A NumberDataValue containing the year number.
	 *
	 * @exception StandardException		Thrown on error
	 */
	NumberDataValue getYear(NumberDataValue result)
							throws StandardException;

	/**
	 * Get the month number out of a date.
	 *
	 * @param result	The result of the previous call to this method, null
	 *					if not called yet.
	 *
	 * @return	A NumberDataValue containing the month number.
	 *
	 * @exception StandardException		Thrown on error
	 */
	NumberDataValue getMonth(NumberDataValue result)
							throws StandardException;

	/**
	 * Get the day of the month.
	 *
	 * @param result	The result of the previous call to this method, null
	 *					if not called yet.
	 *
	 * @return	A NumberDataValue containing the day of the month.
	 *
	 * @exception StandardException		Thrown on error
	 */
	NumberDataValue getDate(NumberDataValue result)
							throws StandardException;

	/**
	 * Get the hour of the day out of a time or timestamp.
	 *
	 * @param result	The result of the previous call to this method, null
	 *					if not called yet.
	 *
	 * @return	A NumberDataValue containing the hour of the day.
	 *
	 * @exception StandardException		Thrown on error
	 */
	NumberDataValue getHours(NumberDataValue result)
							throws StandardException;

	/**
	 * Get the minute of the hour out of a time or timestamp.
	 *
	 * @param result	The result of the previous call to this method, null
	 *					if not called yet.
	 *
	 * @return	A NumberDataValue containing the minute of the hour.
	 *
	 * @exception StandardException		Thrown on error
	 */
	NumberDataValue getMinutes(NumberDataValue result)
							throws StandardException;

	/**
	 * Get the second of the minute out of a time or timestamp.
	 *
	 * @param result	The result of the previous call to this method, null
	 *					if not called yet.
	 *
	 * @return	A NumberDataValue containing the second of the minute.
	 *
	 * @exception StandardException		Thrown on error
	 */
	NumberDataValue getSeconds(NumberDataValue result)
							throws StandardException;
}

