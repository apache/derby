/*

   Derby - Class org.apache.derby.impl.store.raw.log.FlushedScan

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

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.MessageId;

import org.apache.derby.impl.store.raw.log.LogCounter;
import org.apache.derby.impl.store.raw.log.LogRecord;
import org.apache.derby.impl.store.raw.log.StreamLogScan;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.raw.log.LogFactory;
import org.apache.derby.iapi.store.raw.xact.TransactionId;
import org.apache.derby.iapi.services.io.ArrayInputStream;

import org.apache.derby.io.StorageRandomAccessFile;

import java.io.IOException;

/**

	Scan the the log which is implemented by a series of log files.n
	This log scan knows how to move across log file if it is positioned at
	the boundary of a log file and needs to getNextRecord.

	<PRE>
	4 bytes - length of user data, i.e. N
	8 bytes - long representing log instant
	N bytes of supplied data
	4 bytes - length of user data, i.e. N
	</PRE>

*/
public class FlushedScan implements StreamLogScan {

	private StorageRandomAccessFile scan;		// an output stream to the log file
	LogToFile logFactory; 				// log factory knows how to to skip
										// from log file to log file

	boolean open;						// true if the scan is open

	long currentLogFileNumber; 			// the log file the scan is currently on

	long currentLogFileFirstUnflushedPosition;
	                                    // The length of the unflushed portion
										// of the current log file. This is the
										// length of the file for all but the
										// last log file.

	long currentInstant;				// the log instant the scan is
										// currently on - only valid after a
										// successful getNextRecord

	long firstUnflushed = -1;			// scan until we reach the first
										// unflushed byte in the log.
	long firstUnflushedFileNumber;
	long firstUnflushedFilePosition;

	//RESOLVE: This belongs in a shared place.
	static final int LOG_REC_LEN_BYTE_LENGTH = 4;

	public FlushedScan(LogToFile logFactory, long startAt)
		 throws StandardException
	{
        if (SanityManager.DEBUG)
        {
    		SanityManager.ASSERT(startAt != LogCounter.INVALID_LOG_INSTANT,
	    						 "cannot start scan on an invalid log instant");
	    }

		try
		{
			currentLogFileNumber = LogCounter.getLogFileNumber(startAt);
			this.logFactory = logFactory;
			scan =  logFactory.getLogFileAtPosition(startAt);
			setFirstUnflushed();
			open = true;
			currentInstant = LogCounter.INVALID_LOG_INSTANT; // set at getNextRecord
		}

		catch (IOException ioe)
		{
			throw logFactory.markCorrupt(
                    StandardException.newException(SQLState.LOG_IO_ERROR, ioe));
		}
	}

	/*
	** Methods of LogScan
	*/

	/**
		Read a log record into the byte array provided.  Resize the input
		stream byte array if necessary.

		@return the length of the data written into data, or -1 if the end of the
		scan has been reached.

		@exception StandardException Standard Cloudscape error policy
	*/
	public LogRecord getNextRecord(ArrayInputStream input, 
								   TransactionId tranId, 
								   int groupmask)
		 throws StandardException
	{
		try
		{
			boolean candidate;
			int peekAmount = LogRecord.formatOverhead() + LogRecord.maxGroupStoredSize();
			if (tranId != null)
				peekAmount += LogRecord.maxTransactionIdStoredSize(tranId);
			int readAmount;		// the number of bytes actually read

			LogRecord lr;

			do
			{
				if (!open || !positionToNextRecord()) 
					return null;

				int checkLength;

				// this log record is a candidate unless proven otherwise
				lr = null;
				candidate = true;
				readAmount = -1;

				currentInstant = scan.readLong();
				byte[] data = input.getData();
				if (data.length < nextRecordLength)
				{
					// make a new array of sufficient size and reset the arrary
					// in the input stream
					data = new byte[nextRecordLength];
					input.setData(data);
				}

				if (logFactory.databaseEncrypted())
				{
					scan.readFully(data, 0, nextRecordLength);
					int len = logFactory.decrypt(data, 0, nextRecordLength, data, 0);
					if (SanityManager.DEBUG)
						SanityManager.ASSERT(len == nextRecordLength);
					input.setLimit(0, len);

				}
				else // no need to decrypt, only get the group and tid if we filter 
				{
					if (groupmask == 0 && tranId == null)
					{
						// no filter, get the whole thing
						scan.readFully(data, 0, nextRecordLength);
						input.setLimit(0, nextRecordLength);
					}
					else
					{
						// Read only enough so that group and the tran id is in
						// the data buffer.  Group is stored as compressed int
						// and tran id is stored as who knows what.  read min
						// of peekAmount or nextRecordLength
						readAmount = (nextRecordLength > peekAmount) ?
							peekAmount : nextRecordLength; 

						// in the data buffer, we now have enough to peek
						scan.readFully(data, 0, readAmount);
						input.setLimit(0, readAmount);

					}
				}

				lr = (LogRecord) input.readObject();

				if (groupmask != 0 || tranId != null)
				{
					if (groupmask != 0 && (groupmask & lr.group()) == 0)
						candidate = false; // no match, throw this log record out 

					if (candidate && tranId != null)
					{
						TransactionId tid = lr.getTransactionId();
						if (!tid.equals(tranId)) // nomatch
							candidate = false; // throw this log record out
					}

					// if this log record is not filtered out, we need to read
					// in the rest of the log record to the input buffer.
					// Except if it is an encrypted database, in which case the
					// entire log record have already be read in for
					// decryption.

					if (candidate && !logFactory.databaseEncrypted())
					{
						// read the rest of the log into the buffer
						if (SanityManager.DEBUG)
							SanityManager.ASSERT(readAmount > 0);

						if (readAmount < nextRecordLength)
						{
							// Need to remember where we are because the log
							// record may have read part of it off the input
							// stream already and that position is lost when we
							// set limit again.
							int inputPosition = input.getPosition();

							scan.readFully(data, readAmount,
										   nextRecordLength-readAmount); 

							input.setLimit(0, nextRecordLength);
							input.setPosition(inputPosition);
						}
					}
				}

				if (candidate || logFactory.databaseEncrypted())
				{
					checkLength = scan.readInt();

					if (SanityManager.DEBUG)
					{
						SanityManager.ASSERT(checkLength == nextRecordLength, "log currupted");
					}
				}
				else // chances are, we haven't read all of the log record, skip it
				{
					// the starting record position is in the currentInstant,
					// calculate the next record starting position using that
					// and the nextRecordLength
					long nextRecordStartPosition =
						LogCounter.getLogFilePosition(currentInstant) +
						nextRecordLength + LogToFile.LOG_RECORD_OVERHEAD;

					scan.seek(nextRecordStartPosition);
				}

			} while (candidate == false);

			return lr;
		}
		catch (ClassNotFoundException cnfe)
		{
			throw logFactory.markCorrupt(
                StandardException.newException(SQLState.LOG_CORRUPTED, cnfe));
		}
		catch (IOException ioe)
		{
			throw logFactory.markCorrupt(
                StandardException.newException(SQLState.LOG_IO_ERROR, ioe));
		}
	}

	/**
		Reset the scan to the given LogInstant.

		@param instant the position to reset to
		@exception IOException scan cannot access the log at the new position.
	*/
	public void resetPosition(LogInstant instant) throws IOException
	{
        if (SanityManager.DEBUG)
        {
    		SanityManager.THROWASSERT("Unsupported feature");
    	}
	}

	/**
		Get the log instant that is right after the record just retrived
		@return INVALID_LOG_INSTANT if this is not a FORWARD scan or, no
		record have been returned yet or the scan has completed.
	*/
	public long getLogRecordEnd()
	{
        if (SanityManager.DEBUG)
        {
    		SanityManager.THROWASSERT("Unsupported feature");
    	}
		return LogCounter.INVALID_LOG_INSTANT;
	}

	
	/**
	   returns true if there is partially writen log records before the crash 
	   in the last log file. Partiall wrires are identified during forward 
	   scans for log recovery.
	 */
	public boolean isLogEndFuzzy()
	{
		if (SanityManager.DEBUG)
        {
    		SanityManager.THROWASSERT("Unsupported feature");
    	}
		return false;
	}

	/**
		Return the log instant (as an integer) the scan is currently on - this is the log
		instant of the log record that was returned by getNextRecord.
	*/
	public long getInstant()
	{
		return currentInstant;
	}

	/**
		Return the log instant the scan is currently on - this is the log
		instant of the log record that was returned by getNextRecord.
	*/
	public LogInstant getLogInstant()
	{
		if (currentInstant == LogCounter.INVALID_LOG_INSTANT)
			return null;
		else
			return new LogCounter(currentInstant);
	}

	/**
		Close the scan.
	*/
	public void close()
	{
		if (scan != null)
		{
			try
			{
				scan.close();
			}
			catch (IOException ioe)
			{}

			scan = null;
		}
		currentInstant = LogCounter.INVALID_LOG_INSTANT;
		open = false;
	}

	/*
	  Private methods.
	  */
	private void setFirstUnflushed()
		 throws StandardException, IOException
	{
		LogInstant firstUnflushedInstant =
			logFactory.getFirstUnflushedInstant();
		firstUnflushed = ((LogCounter)firstUnflushedInstant).getValueAsLong();
		firstUnflushedFileNumber = LogCounter.getLogFileNumber(firstUnflushed);
		firstUnflushedFilePosition = LogCounter.getLogFilePosition(firstUnflushed);

		setCurrentLogFileFirstUnflushedPosition();
	}

	private void setCurrentLogFileFirstUnflushedPosition()
		 throws IOException
	{
		/*
		  Note we get the currentLogFileLength without synchronization.
		  This is safe because one of the following cases apply:

		  <OL>
		  <LI> The end of the flushed section of the log is in another file.
		  In this case the end of the current file will not change.
		  <LI> The end of the log is in this file. In this case we
		  end our scan at the firstUnflushedInstant and do not use
		  currentLogFileLength.
		  </OL>
		  */
		if (currentLogFileNumber == firstUnflushedFileNumber)
			currentLogFileFirstUnflushedPosition = firstUnflushedFilePosition;
		else if (currentLogFileNumber < firstUnflushedFileNumber)
			currentLogFileFirstUnflushedPosition = scan.length();
		else
        {
			// RESOLVE 
		   	throw new IOException(
                MessageService.getTextMessage(MessageId.LOG_BAD_START_INSTANT));
		}
	}

	private void switchLogFile()
		 throws StandardException
	{
		try
		{
			readNextRecordLength = false;
			scan.close();
			scan = null;
			scan = logFactory.getLogFileAtBeginning(++currentLogFileNumber);
			setCurrentLogFileFirstUnflushedPosition();
		}

		catch (IOException ioe)
		{
			throw logFactory.markCorrupt(
                StandardException.newException(SQLState.LOG_IO_ERROR, ioe));
		}
	}

	/**
	  The length of the next record. Read from scan and set by
	  currentLogFileHasUnflushedRecord. This is used to retain the length of a
	  log record in the case currentLogFileHasUnflushedRecord reads the length
	  and determines that some bytes in the log record are not yet flushed.
	  */
	int nextRecordLength;

	/**
	  Flag to indicate that the length of the next log record has been read by
	  currentLogFileHasUnflushedRecord.

	  This flag gets reset in two ways:

	  <OL>
	  <LI> currentLogFileHasUnflushedRecord determines that the entire log
	  record is flushed and returns true. In this case getNextRecord reads and
	  returns the log record.
	  <LI> we switch log files --due to a partial log record at the end of an
	  old log file.
	  </OL>
	  */
	boolean readNextRecordLength;

	private boolean currentLogFileHasUnflushedRecord()
		 throws IOException
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(scan != null, "scan is null");
		long curPos = scan.getFilePointer();

		if (!readNextRecordLength)
		{
			if (curPos + LOG_REC_LEN_BYTE_LENGTH >
				                 currentLogFileFirstUnflushedPosition)
				return false;

			nextRecordLength = scan.readInt();
			curPos+=4;
			readNextRecordLength = true;
		}

		if (nextRecordLength==0) return false;

		int bytesNeeded =
			nextRecordLength + LOG_REC_LEN_BYTE_LENGTH;

		if (curPos + bytesNeeded > currentLogFileFirstUnflushedPosition)
		{
			return false;
		}
		else
		{
			readNextRecordLength = false;
			return true;
		}
	}

	private boolean positionToNextRecord()
		 throws StandardException, IOException
	{
		//If the flushed section of the current log file contains our record we
		//simply return.
		if (currentLogFileHasUnflushedRecord()) return true;

		//Update our cached copy of the first unflushed instant.
		setFirstUnflushed();

		//In the call to setFirstUnflushed, we may have noticed that the current
		//log file really does contain our record. If so we simply return.
		if (currentLogFileHasUnflushedRecord()) return true;

		//Our final chance of finding a record is if we are not scanning the log
		//file with the last flushed instant we can switch logfiles. Note that
		//we do this in a loop to cope with empty log files.
		while(currentLogFileNumber < firstUnflushedFileNumber)
		{
			  switchLogFile();
		      if (currentLogFileHasUnflushedRecord()) return true;
		}

		//The log contains no more flushed log records so we return false.
		currentInstant = LogCounter.INVALID_LOG_INSTANT;
		return false;
	}
}
