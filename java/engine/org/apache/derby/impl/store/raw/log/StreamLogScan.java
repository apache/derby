/*

   Derby - Class org.apache.derby.impl.store.raw.log.StreamLogScan

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

package org.apache.derby.impl.store.raw.log;

import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.raw.log.LogScan;
import org.apache.derby.iapi.store.raw.xact.TransactionId;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.io.ArrayInputStream;

import java.io.InputStream;
import java.io.IOException;

/**
	LogScan provides methods to read a log record and get its LogInstant
	in an already defined scan.  A logscan also needs to know how to advance to
	the next log record.
*/

public interface StreamLogScan extends LogScan {

	/**
		Get the next record in the scan and place its data in the passed in
		array.  The scan is advanced to the next log record.
	    If the input array is of insufficient size, getNextRecord must expand
		the array to accomodate the log record.  User can optionally pass in a
		transaction Id and a group mask.  If provided, only log record that
		matches the transaction Id and the group mask is returned.

		@param input the ArrayInputStream to put the log record
		@param logicalInput the logical input stream that is attached to input
		@param tranId if non-null, only log record that equals tranId will be
		returned.  If null, log records are not filtered on transaction Id. 
		@param groupmask, if non-zero, only log record whose Loggable's group
		value is included in the groupmask is returned.  groupmask can be a bit
		wise OR of many Loggable groups.  If zero, log records are not filtered
		on the Loggable's group.

		@return an object that represents the log record, return null if the
		scan has completed. 

		@exception StandardException Standard Clooudscape error policy
		@exception IOException       Some I/O exception raised during reading 
                                     the log record.
	*/
	public LogRecord getNextRecord(ArrayInputStream input, 
								   TransactionId tranId, 
								   int groupmask) 
		 throws StandardException, IOException;


	/**
		Get the instant of the record just retrieved with getNextRecord(). 
		@return INVALID_LOG_INSTANT if no records have been returned yet or
		the scan has completed.
	*/
	public long getInstant();

	/**
		Get the log instant that is right after the record just retrieved with
		getNextRecord().  Only valid for a forward scan and on a successful
		retrieval.

		@return INVALID_LOG_INSTANT if this is not a FORWARD scan or, no
		record have been returned yet or the scan has completed.
	*/
	public long getLogRecordEnd();
	
	/**
	   @return true if  fuzzy log end found during forward scan, this happens
	   if there was a partially written log records before the crash.
	*/
	public boolean isLogEndFuzzy();

	/**
	    Get the LogInstant for the record just retrieved with getNextRecord().
		@return null if no records have been returned yet or the scan has
		completed.
		*/
	public LogInstant getLogInstant();

	/**
		Reset the scan to the given LogInstant so that getNextRecord get the
		log record AFTER the given LogInstant.

		@param instant the log instant to reset to

		@exception IOException       Some I/O exception raised when accessing 
                                     the log file
		@exception StandardException reset to illegal position or beyond the
		                             limit of the scan.
	*/
	public void resetPosition(LogInstant instant) 
		 throws IOException, StandardException;

	/**
		Close this log scan.
	*/
	public void close();
}
