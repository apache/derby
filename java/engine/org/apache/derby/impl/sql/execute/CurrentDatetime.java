/*

   Derby - Class org.apache.derby.impl.sql.execute.CurrentDatetime

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.sql.execute;

/* can't import due to name overlap:
import java.util.Date;
*/
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
	CurrentDatetime provides execution support for ensuring
	that the current datetime is evaluated only once for a
	statement. The same value is returned for every
	CURRENT_DATE, CURRENT_TIME, and CURRENT_TIMESTAMP in the
	statement.
	<p>
	This is expected to be used by an activation and its
	result set, and so 'forget' must be called whenever you
	want to reuse the CurrentDatetime object for additional
	executions of the statement.

	@author ames
 */
public class CurrentDatetime {

	/**
		Holds the current datetime on the first evaluation of a current function
		in a statement, which contains all available fields.
	 */
	private java.util.Date currentDatetime;
	/**
		Holds the SQL DATE version of the current datetime.
	 */
	private Date currentDate;
	/**
		Holds the SQL TIME version of the current datetime.
	 */
	private Time currentTime;
	/**
		Holds the SQL TIMESTAMP version of the current datetime.
	 */
	private Timestamp currentTimestamp;

	/**
		The constructor is public; note we wait until evaluation to
		put any values into the fields.
	 */
	public CurrentDatetime() {
	}

	// class implementation
	final private void setCurrentDatetime() {
		if (currentDatetime == null)
			currentDatetime = new java.util.Date();
	}

	// class interface

	public Date getCurrentDate() {
		if (currentDate == null) {
			setCurrentDatetime();
			currentDate = new Date(currentDatetime.getTime());
		}
		return currentDate;
	}

	public Time getCurrentTime() {
		if (currentTime == null) {
			setCurrentDatetime();
			currentTime = new Time(currentDatetime.getTime());
		}
		return currentTime;
	}

	public Timestamp getCurrentTimestamp() {
		if (currentTimestamp == null) {
			setCurrentDatetime();
			currentTimestamp = new Timestamp(currentDatetime.getTime());
		}
		return currentTimestamp;
	}

	/**
		This is called prior to each execution of the statement, to
		ensure that it starts over with a new current datetime value.
	 */
	public void forget() {
		currentDatetime = null;
		currentDate = null;
		currentTime = null;
		currentTimestamp = null;
	}

}
