/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.types
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.types;

import org.apache.derby.iapi.error.StandardException;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public interface DateTimeDataValue extends DataValueDescriptor
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
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

