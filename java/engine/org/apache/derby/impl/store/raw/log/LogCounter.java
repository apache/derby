/*

   Derby - Class org.apache.derby.impl.store.raw.log.LogCounter

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

package org.apache.derby.impl.store.raw.log;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.access.DatabaseInstant;
import org.apache.derby.iapi.services.io.CompressedNumber;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
	A very simple log instant implementation.

	Within the stored log record a log counter is represented as a long,
	hence the getValueAsLong() method. Outside the LogFactory the instant
	is passed around as a LogCounter (through its LogInstant interface).

	The way the long is encoded is such that < == > correctly tells if
	one log instant is lessThan, equals or greater than another.

*/
public class LogCounter implements LogInstant {

	/********************************************************
	**
	**	This class implements Formatable. That means that it
	**	can write itself to and from a formatted stream. If
	**	you add more fields to this class, make sure that you
	**	also write/read them with the writeExternal()/readExternal()
	**	methods.
	**
	**	If, between releases, you add more fields to this class,
	**	then you should bump the version number emitted by the getTypeFormatId()
	**	method.
	**
	********************************************************/
	
	/** A well defined value of an invalid log instant. */
	public static final long INVALID_LOG_INSTANT = 0;


	// reserve top 10 bits in log file number for future use
	public static final long MAX_LOGFILE_NUMBER	=	(long)0x003FFFFFL;	// 4194303
	private static final long FILE_NUMBER_SHIFT	= 32;

	// reserve top 4 bits in log file size for future use
	public static final long MAX_LOGFILE_SIZE	= 		(long)0x0FFFFFFFL; // 268435455
	private static final long FILE_POSITION_MASK	= 	(long)0x7FFFFFFFL;

	private long fileNumber;
	private long filePosition;

	// contructors
	public LogCounter(long value) {
		fileNumber = getLogFileNumber(value);
		filePosition = getLogFilePosition(value);
	}

	public LogCounter(long fileNumber, long position) {

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(fileNumber > 0, "illegal fileNumber");
			SanityManager.ASSERT(position > 0, "illegal file position");

			SanityManager.ASSERT(position < MAX_LOGFILE_SIZE,
							 "log file position exceeded max log file size");
			SanityManager.ASSERT(fileNumber < MAX_LOGFILE_NUMBER,
							 "log file number exceeded max log file number");
		}

		this.fileNumber = fileNumber;
		this.filePosition = position;
	}

	/**
	 * Public niladic constructor needed for Formatable interface.
	 */
	public LogCounter() {}
	
	/** 
		Static functions that can only be used inside the RawStore's log
		factory which passes the log counter around encoded as a long
	*/

	// make a log instant from 2 longs and return a long which is the long
	// representatin of a LogCounter
	static public final long makeLogInstantAsLong(long filenum, long filepos)
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(filenum > 0, "illegal fileNumber");
			SanityManager.ASSERT(filepos > 0, "illegal file position");

			SanityManager.ASSERT(filepos < MAX_LOGFILE_SIZE,
							 "log file position exceeded max log file size");
			SanityManager.ASSERT(filenum < MAX_LOGFILE_NUMBER,
							 "log file number exceeded max log file number");
		}

		return ((filenum << FILE_NUMBER_SHIFT) | filepos);
	}


	static public final long getLogFilePosition(long valueAsLong)
	{
		return valueAsLong & FILE_POSITION_MASK;
	}

	static public final long getLogFileNumber(long valueAsLong)
	{
		return valueAsLong >>> FILE_NUMBER_SHIFT;
	}

	/** LogScan methods */

	public boolean lessThan(DatabaseInstant other) {
		LogCounter compare = (LogCounter)other;

		return (fileNumber == compare.fileNumber) ?
			filePosition < compare.filePosition :
			fileNumber < compare.fileNumber;
	}

	public boolean equals(Object other) {
		if (this == other)
			return true;

		if (!(other instanceof LogCounter))
			return false;

		LogCounter compare = (LogCounter)other;

		return fileNumber == compare.fileNumber &&
			filePosition == compare.filePosition;
	}

    public DatabaseInstant next() {
        return new LogCounter( makeLogInstantAsLong(fileNumber, filePosition) + 1);
    }
    
    public DatabaseInstant prior() {
        return new LogCounter( makeLogInstantAsLong(fileNumber, filePosition) - 1);
    }
    
	public int hashCode() {
		return (int) (filePosition ^ fileNumber);
	}

	public String toString() {
		return "(" + fileNumber + "," + filePosition + ")";
	}

	public static String toDebugString(long instant)
	{
		if (SanityManager.DEBUG)
			return "(" + getLogFileNumber(instant) + "," + getLogFilePosition(instant) + ")";
		else
			return null;
	}

	/**
		These following methods are only intended to be called by an
		implementation of a log factory. All other uses of this object should
		only see it as a log instant.
	*/
	public long getValueAsLong() {
		return makeLogInstantAsLong(fileNumber, filePosition);
	}

	public long getLogFilePosition()
	{
		 return filePosition;
	}

	public long getLogFileNumber()
	{
		return fileNumber;
	}

	
	/*
	 * methods for the Formatable interface
	 */

	/**
	 * Read this in.
	 * @exception IOException error reading from log stream
	 * @exception ClassNotFoundException corrupted log stream
	 */
	public void readExternal(ObjectInput oi) throws IOException, ClassNotFoundException {
		fileNumber = CompressedNumber.readLong(oi);
		filePosition = CompressedNumber.readLong(oi);
	}
	
	/**
	 * Write this out.
	 * @exception IOException error writing to log stream
	 */
	public void writeExternal(ObjectOutput oo) throws IOException {
		CompressedNumber.writeLong(oo,fileNumber);
		CompressedNumber.writeLong(oo,filePosition);
	}
	
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.LOG_COUNTER; }

}
