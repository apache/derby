/*

   Derby - Class org.apache.derby.impl.store.raw.log.Scan

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

import org.apache.derby.iapi.services.io.ArrayInputStream;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.raw.log.LogInstant;

import org.apache.derby.iapi.store.raw.xact.TransactionId;

import org.apache.derby.impl.store.raw.log.LogCounter;
import org.apache.derby.impl.store.raw.log.LogRecord;
import org.apache.derby.impl.store.raw.log.StreamLogScan;

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

public class Scan implements StreamLogScan {

	// value for scanDirection
	public static final byte FORWARD = 1;
	public static final byte BACKWARD = 2;
	public static final byte BACKWARD_FROM_LOG_END = 4;

	private StorageRandomAccessFile scan;		// an output stream to the log file
	private LogToFile logFactory; 		// log factory knows how to to skip
										// from log file to log file

	private long currentLogFileNumber; 	// the log file the scan is currently on

	private long currentLogFileLength;	// the size of the current log file
										// used only for FORWARD scan to determine when
										// to switch the next log file

	private long knownGoodLogEnd; // For FORWARD scan only
								// during recovery, we need to determine the end
								// of the log.  Everytime a complete log record
								// is read in, knownGoodLogEnd is set to the
								// log instant of the next log record if it is
								// on the same log file.
								// 
								// only valid afer a successfull getNextRecord
								// on a FOWARD scan. 


	private long currentInstant;		// the log instant the scan is
										// currently on - only valid after a
										// successful getNextRecord

	private long stopAt;				// scan until we find a log record whose 
										// log instance < stopAt if we scan BACKWARD
										// log instance > stopAt if we scan FORWARD
										// log instance >= stopAt if we scan FORWARD_FLUSHED


	private byte scanDirection; 		// BACKWARD or FORWARD

	private boolean fuzzyLogEnd = false;   //get sets to true during forward scan
 	                                      //for recovery, if there were
	                                      //partial writes at the end of the log before crash;
	                                      //during forward scan for recovery.


	/**
	    For backward scan, we expect a scan positioned at the end of the next log record.
		For forward scan, we expect a scan positioned at the beginning of the next log record.

		For forward flushed scan, we expect stopAt to be the instant for the
		   first not-flushed log record. Like any forward scan, we expect a scan
		   positioned at the beginning of the next log record.

		@exception StandardException Standard Cloudscape error policy
		@exception IOException cannot access the log at the new position.
	*/
	public Scan(LogToFile logFactory, long startAt, LogInstant stopAt, byte direction)
		 throws IOException, StandardException
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(startAt != LogCounter.INVALID_LOG_INSTANT, 
								 "cannot start scan on an invalid log instant");

		this.logFactory = logFactory;
		currentLogFileNumber = LogCounter.getLogFileNumber(startAt);
		currentLogFileLength = -1;
		knownGoodLogEnd = LogCounter.INVALID_LOG_INSTANT;// set at getNextRecord for FORWARD scan
		currentInstant = LogCounter.INVALID_LOG_INSTANT; // set at getNextRecord
		if (stopAt != null)
			this.stopAt = ((LogCounter) stopAt).getValueAsLong();
		else
			this.stopAt = LogCounter.INVALID_LOG_INSTANT;

		switch(direction)
		{
		case FORWARD:
			scan =  logFactory.getLogFileAtPosition(startAt);
			scanDirection = FORWARD;

			if (SanityManager.DEBUG)
				if (scan == null)
					SanityManager.THROWASSERT(
						"scan null at " + LogCounter.toDebugString(startAt));

			// NOTE: just get the length of the file without syncing.
			// this only works because the only place forward scan is used
			// right now is on recovery redo and nothing is being added to 
			// the current log file.  When the forward scan is used for some
			// other purpose, need to sync access to the end of the log
			currentLogFileLength = scan.length();
			break;

		case BACKWARD:
			// startAt is at the front of the log record, for backward
			// scan we need to be positioned at the end of the log record
			scan =  logFactory.getLogFileAtPosition(startAt);
			int logsize = scan.readInt();

			// skip forward over the log record and all the overhead, but remember
			// we just read an int off the overhead
			scan.seek(scan.getFilePointer() + logsize + LogToFile.LOG_RECORD_OVERHEAD - 4);
			scanDirection = BACKWARD;
			break;

		case BACKWARD_FROM_LOG_END:
			// startAt is at the end of the log, no need to skip the log record
			scan =  logFactory.getLogFileAtPosition(startAt);
			scanDirection = BACKWARD;
			break;

		}
	}

	/*
	** Methods of StreamLogScan
	*/

	/**
		Read the next log record.
		Switching log to a previous log file if necessary, 
		Resize the input stream byte array if necessary.  
		@see StreamLogScan#getNextRecord

		@return the next LogRecord, or null if the end of the
		scan has been reached.

		@exception StandardException Standard Cloudscape error policy
	*/
	public LogRecord getNextRecord(ArrayInputStream input, 
							 TransactionId tranId, 
							 int groupmask)
		 throws StandardException
	{
		if (scan == null)
			return null;

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(scanDirection != 0, "scan has been secretly closed!");

		LogRecord lr = null;
		try
		{
			if (scanDirection == BACKWARD)
				lr = getNextRecordBackward(input, tranId, groupmask);
			else if (scanDirection == FORWARD)
				lr = getNextRecordForward(input, tranId, groupmask);

			return lr;

		}
		catch (IOException ioe)
		{
			if (SanityManager.DEBUG)
				ioe.printStackTrace();

			throw logFactory.markCorrupt(
                StandardException.newException(SQLState.LOG_CORRUPTED, ioe));
		}
		catch (ClassNotFoundException cnfe)
		{
			if (SanityManager.DEBUG)
				cnfe.printStackTrace();

			throw logFactory.markCorrupt(
                StandardException.newException(SQLState.LOG_CORRUPTED, cnfe));
		}
		finally
		{
			if (lr == null)
				close();		// no more log record, close the scan
		}

	}

	/**
		Read the previous log record.
		Switching log to a previous log file if necessary, 
		Resize the input stream byte array if necessary.  
		@see StreamLogScan#getNextRecord

		Side effects include: 
				on a successful read, setting currentInstant.
				on a log file switch, setting currentLogFileNumber.

		@return the previous LogRecord, or null if the end of the
		scan has been reached.
	*/
	private LogRecord getNextRecordBackward(ArrayInputStream input, 
									  TransactionId tranId,  
									  int groupmask) 
		 throws StandardException, IOException, ClassNotFoundException
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(scanDirection == BACKWARD, "can only called by backward scan");

		// scan is positioned just past the last byte of the record, or
		// right at the beginning of the file (end of the file header)
		// may need to switch log file

		boolean candidate;
		// if we have filtering, peek at the group and/or the transaction id,
		// do them in one read rather than 2 reads.
		int peekAmount = LogRecord.formatOverhead() + LogRecord.maxGroupStoredSize();
		if (tranId != null)
			peekAmount += LogRecord.maxTransactionIdStoredSize(tranId);

		int readAmount;			// the number of bytes actually read

		LogRecord lr;
		long curpos = scan.getFilePointer();

		do
		{
			// this log record is a candidate unless proven otherwise
			candidate = true; 
			lr = null;
			readAmount = -1;

			if (curpos == LogToFile.LOG_FILE_HEADER_SIZE)
			{
				// don't go thru the trouble of switching log file if we
				// will have gone past stopAt
				if (stopAt != LogCounter.INVALID_LOG_INSTANT &&
					LogCounter.getLogFileNumber(stopAt) == currentLogFileNumber)
				{
					if (SanityManager.DEBUG)
                    {
                        if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
                        {
                            SanityManager.DEBUG(LogToFile.DBG_FLAG, 
                                "stopping at " + currentLogFileNumber);
                        }
                    }

					return null;  // no more log record
				}
				
				// figure out where the last log record is in the previous
				// log file
				scan.seek(LogToFile.LOG_FILE_HEADER_PREVIOUS_LOG_INSTANT_OFFSET);
				long previousLogInstant = scan.readLong();
				scan.close();

				if (SanityManager.DEBUG)
				{
					SanityManager.ASSERT(previousLogInstant != LogCounter.INVALID_LOG_INSTANT,
									 "scanning backward beyond the first log file");
					if (currentLogFileNumber != 
							LogCounter.getLogFileNumber(previousLogInstant) + 1)
						SanityManager.THROWASSERT(
						"scanning backward but get incorrect log file number " + 
						 "expected " + (currentLogFileNumber -1) + 
						 "get " +
						 LogCounter.getLogFileNumber(previousLogInstant));

					SanityManager.ASSERT(LogCounter.getLogFilePosition(previousLogInstant) > 
									 LogToFile.LOG_FILE_HEADER_SIZE,
									 "scanning backward encounter completely empty log file");

					SanityManager.DEBUG(LogToFile.DBG_FLAG, 
									"scanning backwards from log file " +
									currentLogFileNumber + ", switch to (" + 
									LogCounter.getLogFileNumber(previousLogInstant) + "," +
									LogCounter.getLogFilePosition(previousLogInstant) + ")"
									);
				}

				// log file switch, set this.currentLogFileNumber
				currentLogFileNumber = LogCounter.getLogFileNumber(previousLogInstant);

				scan = logFactory.getLogFileAtPosition(previousLogInstant);

				// scan is located right past the last byte of the last log
				// record in the previous log file.  currentLogFileNumber is
				// set.  We asserted that the scan is not located right at the
				// end of the file header, in other words, there is at least
				// one log record in this log file.
				curpos = scan.getFilePointer();

				// if the log file happens to be empty skip and proceed. 
				// ideally this case should never occur because log switch is
				// not suppose to happen on an empty log file. 
				// But it is safer to put following check incase if it ever
				// happens to avoid any recovery issues. 
				if (curpos == LogToFile.LOG_FILE_HEADER_SIZE)
					continue;
			}

			scan.seek(curpos - 4);
			int recordLength = scan.readInt(); // get the length after the log record

			// calculate where this log record started.
			// include the eight bytes for the long log instant at the front
			// the four bytes of length in the front and the four bytes we just read
			long recordStartPosition = curpos - recordLength -
				LogToFile.LOG_RECORD_OVERHEAD; 

			if (SanityManager.DEBUG)
			{
				if (recordStartPosition < LogToFile.LOG_FILE_HEADER_SIZE)
					SanityManager.THROWASSERT(
								 "next position " + recordStartPosition +
								 " recordLength " + recordLength + 
								 " current file position " + scan.getFilePointer());

				scan.seek(recordStartPosition);

				// read the length before the log record and check it against the
				// length after the log record
				int checkLength = scan.readInt();

				if (checkLength != recordLength)
				{
					long inst = LogCounter.makeLogInstantAsLong(currentLogFileNumber, recordStartPosition);

					throw logFactory.markCorrupt(
                        StandardException.newException(
                            SQLState.LOG_RECORD_CORRUPTED, 
                            new Long(checkLength),
                            new Long(recordLength),
                            new Long(inst),
                            new Long(currentLogFileNumber)));
				}
			}
			else
			{
				// skip over the length in insane
				scan.seek(recordStartPosition+4);
			}

			// scan is positioned just before the log instant
			// read the current log instant - this is the currentInstant if we have not
			// exceeded the scan limit
			currentInstant = scan.readLong();

			if (SanityManager.DEBUG)
			{
				// sanity check the current instant against the scan position
				if (LogCounter.getLogFileNumber(currentInstant) !=
					currentLogFileNumber ||
					LogCounter.getLogFilePosition(currentInstant) !=
					recordStartPosition)
					SanityManager.THROWASSERT(
								 "Wrong LogInstant on log record " +
								LogCounter.toDebugString(currentInstant) + 
								 " version real position (" +
								 currentLogFileNumber + "," +
								 recordStartPosition + ")");
			}


			// if stopAt == INVALID_LOG_INSTANT, no stop instant, read till
			// nothing more can be read.  Else check scan limit
			if (currentInstant < stopAt && stopAt != LogCounter.INVALID_LOG_INSTANT)
			{
				currentInstant = LogCounter.INVALID_LOG_INSTANT;
				return null;	// we went past the stopAt
			}


			byte[] data = input.getData();

			if (data.length < recordLength)
			{
				// make a new array of sufficient size and reset the arrary
				// in the input stream
				data = new byte[recordLength];
				input.setData(data);
			}

			// If the log is encrypted, we must do the filtering after reading
			// and decrypting the record.
			if (logFactory.databaseEncrypted())
			{
				scan.readFully(data, 0, recordLength);
				int len = logFactory.decrypt(data, 0, recordLength, data, 0);
				if (SanityManager.DEBUG)
					SanityManager.ASSERT(len == recordLength);
				input.setLimit(0, recordLength);
			}
			else // no need to decrypt, only get the group and tid if we filter 
			{
				if (groupmask == 0 && tranId == null)
				{
					// no filter, get the whole thing
					scan.readFully(data, 0, recordLength);
					input.setLimit(0, recordLength);
				}
				else
				{
					// Read only enough so that group and the tran id is in
					// the data buffer.  Group is stored as compressed int
					// and tran id is stored as who knows what.  read min
					// of peekAmount or recordLength
					readAmount = (recordLength > peekAmount) ?
						peekAmount : recordLength; 

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

					if (readAmount < recordLength)
					{
						// Need to remember where we are because the log
						// record may have read part of it off the input
						// stream already and that position is lost when we
						// set limit again.
						int inputPosition = input.getPosition();

						scan.readFully(data, readAmount,
									   recordLength-readAmount); 

						input.setLimit(0, recordLength);
						input.setPosition(inputPosition);
					}
				}
			}

			// go back to the start of the log record so that the next time
			// this method is called, it is positioned right past the last byte
			// of the record.
			curpos = recordStartPosition;
			scan.seek(curpos);

		} while (candidate == false);

		return lr;

	}

	/**
		Read the next log record.
		Switching log to a previous log file if necessary, 
		Resize the input stream byte array if necessary.  
		@see StreamLogScan#getNextRecord

		Side effects include: 
				on a successful read, setting currentInstant, knownGoodLogEnd
				on a log file switch, setting currentLogFileNumber, currentLogFileLength.
				on detecting a fuzzy log end that needs clearing, it will call
				logFactory to clear the fuzzy log end.

		@return the next LogRecord, or null if the end of the
		scan has been reached.
	*/
	private LogRecord getNextRecordForward(ArrayInputStream input, 
									 TransactionId tranId,  
									 int groupmask)
		 throws StandardException,  IOException, ClassNotFoundException
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(scanDirection == FORWARD, "can only called by forward scan");

		// NOTE:
		//
		// if forward scan, scan is positioned at the first byte of the
		// next record, or the end of file - note the the 'end of file'
		// is defined at the time the scan is initialized.  If we are
		// on the current log file, it may well have grown by now...
		//
		// This is not a problem in reality because the only forward
		// scan on the log now is recovery redo and the log does not
		// grow.  If in the future, a foward scan of the log is used
		// for some other reasons, need to keep this in mind.
		//

		// first we need to make sure the entire log record is on the
		// log, or else this is a fuzzy log end.

		// RESOLVE: can get this from knownGoodLogEnd if this is not the first
		// time getNext is called.  Probably just as fast to call
		// scan.getFilePointer though...
		long recordStartPosition = scan.getFilePointer();

		boolean candidate;

		// if we have filtering, peek at the group and/or the transaction id,
		// do them in one read rather than 2 reads.
		int peekAmount = LogRecord.formatOverhead() + LogRecord.maxGroupStoredSize();
		if (tranId != null)
			peekAmount += LogRecord.maxTransactionIdStoredSize(tranId);

		int readAmount;			// the number of bytes actually read

		LogRecord lr;

		do
		{
			// this log record is a candidate unless proven otherwise
			candidate = true;
			lr = null;
			readAmount = -1;

			// if we are not right at the end but this position + 4 is at
			// or exceeds the end, we know we don't have a complete log
			// record.  This is the log file and chalk it up as the fuzzy
			// end.
			if (recordStartPosition + 4 > currentLogFileLength)
			{
				// since there is no end of log file marker, we are at the
				// end of the log.
				if (SanityManager.DEBUG)
                {
                    if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
                    {
                        SanityManager.DEBUG(LogToFile.DBG_FLAG, 
                            "detected fuzzy log end on log file " + 
                                currentLogFileNumber + 
                            " record start position " + recordStartPosition + 
                            " file length " + currentLogFileLength);
                    }
                }
				
				//if  recordStartPosition == currentLogFileLength
				//there is NO fuzz, it just a properly ended log 
				//without the end marker. 
				if(recordStartPosition != currentLogFileLength)
					fuzzyLogEnd = true ;

				// don't bother to write the end of log file marker because
				// if it is not overwritten by the next log record then
				// the next time the database is recovered it will come
				// back right here
				return null;
			}

			// read in the length before the log record
			int recordLength = scan.readInt();

			while (recordLength == 0 || recordStartPosition + recordLength +
				   LogToFile.LOG_RECORD_OVERHEAD > currentLogFileLength) 
			{
				// if recordLength is zero or the log record goes beyond the
				// current file, then we have detected the end of a log file.
				//
				// If recordLength == 0 then we know that this log file has either
				// been properly switched or it had a 1/2 written log record which 
				// was subsequently cleared by clearFuzzyEnd.
				//
				// If recordLength != 0 but log record goes beyond the current log
				// file, we have detected a fuzzy end.  This is the last log file
				// since we will clear it by clearFuzzyEnd.

				if (recordLength != 0) // this is a fuzzy log end
				{
					if (SanityManager.DEBUG)
                    {
                        if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
                        {
                            SanityManager.DEBUG(
                                LogToFile.DBG_FLAG, 
                                "detected fuzzy log end on log file " + 
                                    currentLogFileNumber + 
                                " record start position " + 
                                    recordStartPosition + 
                                " file length " + currentLogFileLength + 
								" recordLength=" + recordLength );
                        }
                    }

					fuzzyLogEnd = true;
					scan.close();
					scan = null;

					return null;
				}

				// recordLength == 0

				if (SanityManager.DEBUG) 
                {
                    if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
                    {
                        if (recordStartPosition + 4 == currentLogFileLength)
                        {
                            SanityManager.DEBUG(LogToFile.DBG_FLAG, 
                                "detected proper log end on log file " + 
                                currentLogFileNumber);
                        }
                        else
                        {
                            SanityManager.DEBUG(LogToFile.DBG_FLAG, 
                                    "detected zapped log end on log file " + 
                                        currentLogFileNumber +
                                    " end marker at " + 
                                        recordStartPosition + 
                                    " real end at " + currentLogFileLength);
                        }
                    }
				}
				
				// don't go thru the trouble of switching log file if we
				// have will have gone past stopAt if we want to stop here
				if (stopAt != LogCounter.INVALID_LOG_INSTANT &&
					LogCounter.getLogFileNumber(stopAt) == currentLogFileNumber)
				{
					return null;
				}

				//
				// we have a log end marker and we don't want to stop yet, switch
				// log file
				//
				scan.close();

				// set this.currentLogFileNumber
				scan = logFactory.getLogFileAtBeginning(++currentLogFileNumber);
				if (scan == null) // we have seen the last log file
				{
					return null;
				}

				if (SanityManager.DEBUG) 
                {
                    if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
                    {
                        SanityManager.DEBUG(LogToFile.DBG_FLAG, 
                            "switched to next log file " + 
                            currentLogFileNumber);
                    }
                }

				// scan is position just past the log header
				recordStartPosition = scan.getFilePointer();

				// set this.currentLogFileLength
				currentLogFileLength = scan.length();

				if (recordStartPosition+4 >= currentLogFileLength) // empty log file
				{
					if (SanityManager.DEBUG)
                    {
                        if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
                        {
                            SanityManager.DEBUG(LogToFile.DBG_FLAG, 
                                "log file " + currentLogFileNumber + 
                                " is empty");
                        }
                    }

					// ideally, we would want to start writing on this new
					// empty log file, but the scan is closed and there is
					// no way to tell the difference between an empty log
					// file and a log file which is not there.  We will be
					// writing to the end of the previous log file instead
					// but when we next switch the log, the empty log file
					// will be written over.

					return null;
				}

				// we have successfully switched to the next log file.
				// scan is positioned just before the next log record
				// see if this one is written in entirety
				recordLength = scan.readInt();
			}

			// we know the entire log record is on this log file

			// read the current log instant
			currentInstant = scan.readLong();
			
			/*check if the current instant happens is less than the last one. 
			 *This can happen if system crashed before writing the log instant
			 *completely. If the instant is partially written it will be less
			 *than the last one and should be the last record that was suppose to
			 *get written. Currentlt preallocated files are filled with zeros,
			 *this should hold good.
			 *Note: In case of Non-preallocated files earlier check with log
			 * file lengths should have found the end. But in prellocated files, log file
			 *length is not sufficiant to find the log end. This check 
			 *is must to find the end in preallocated log files. 
			 */
			if(currentInstant < knownGoodLogEnd)
			{
				fuzzyLogEnd = true ;
				return null;
			}

			// sanity check it 
			if (SanityManager.DEBUG)
			{
				if (LogCounter.getLogFileNumber(currentInstant) !=
					currentLogFileNumber ||
					LogCounter.getLogFilePosition(currentInstant) !=
					recordStartPosition)
					SanityManager.THROWASSERT(
							  "Wrong LogInstant on log record " +
								LogCounter.toDebugString(currentInstant) + 
								 " version real position (" +
								 currentLogFileNumber + "," +
								 recordStartPosition + ")");
			}


			// if stopAt == INVALID_LOG_INSTANT, no stop instant, read till
			// nothing more can be read.  Else check scan limit
			if (stopAt != LogCounter.INVALID_LOG_INSTANT && currentInstant > stopAt)
			{
				currentInstant = LogCounter.INVALID_LOG_INSTANT;
				return null;			// we went past the stopAt
			}

			// read in the log record
			byte[] data = input.getData();

			if (data.length < recordLength)
			{
				// make a new array of sufficient size and reset the arrary
				// in the input stream
				data = new byte[recordLength];
				input.setData(data);
			}

			// If the log is encrypted, we must do the filtering after
			// reading and decryptiong the record.

			if (logFactory.databaseEncrypted())
			{
				scan.readFully(data, 0, recordLength);
				int len = logFactory.decrypt(data, 0, recordLength, data, 0);
				if (SanityManager.DEBUG)
					SanityManager.ASSERT(len == recordLength);

				input.setLimit(0, len);
			}
			else // no need to decrypt, only get the group and tid if we filter 
			{
				if (groupmask == 0 && tranId == null)
				{
					// no filter, get the whole thing
					scan.readFully(data, 0, recordLength);
					input.setLimit(0, recordLength);
				}
				else
				{
					// Read only enough so that group and the tran id is in
					// the data buffer.  Group is stored as compressed int
					// and tran id is stored as who knows what.  read min
					// of peekAmount or recordLength
					readAmount = (recordLength > peekAmount) ?
						peekAmount : recordLength; 

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

					if (readAmount < recordLength)
					{
						// Need to remember where we are because the log
						// record may have read part of it off the input
						// stream already and that position is lost when we
						// set limit again.
						int inputPosition = input.getPosition();

						scan.readFully(data, readAmount,
									   recordLength-readAmount); 

						input.setLimit(0, recordLength);
						input.setPosition(inputPosition);
					}
				}
			}

			/*check if the logrecord length written before and after the 
			 *log record are equal, if not the end of of the log is reached.
			 *This can happen if system crashed before writing the length field 
			 *in the end of the records completely. If the length is partially
			 *written or not written at all  it will not match with length written 
			 *in the beginning of the log record. Currentlt preallocated files
			 *are filled with zeros, log record length can never be zero; 
			 *if the lengths are not matching, end of the properly written log
			 *is reached.
			 *Note: In case of Non-preallocated files earlier fuzzy case check with log
			 * file lengths should have found the end. But in prellocated files, log file
			 *length is not sufficiant to find the log end. This check 
			 *is must to find the end in preallocated log files. 
			 */
			// read the length after the log record and check it against the
			// length before the log record, make sure we go to the correct
			// place for skipped log record.
			if (!candidate)
				scan.seek(recordStartPosition - 4);
			int checkLength = scan.readInt();
			if (checkLength != recordLength && checkLength < recordLength)
			{


				//lengh written in the end of the log record should be always
				//less then the length written in the beginning if the log
				//record was half written before the crash.
				if(checkLength < recordLength)
				{
					fuzzyLogEnd = true ;
					return null;
				}else
				{
				
					//If checklength > recordLength then it can be not be a partial write
					//probablly it is corrupted for some reason , this should never
					//happen throw error in debug mode. In non debug case , let's
					//hope it's only is wrong and system can proceed. 
						
					if (SanityManager.DEBUG)
					{	
						throw logFactory.markCorrupt
						(StandardException.newException(
							SQLState.LOG_RECORD_CORRUPTED, 
                            new Long(checkLength),
                            new Long(recordLength),
                            new Long(currentInstant),
                            new Long(currentLogFileNumber)));

					}
					
					//In non debug case, do nothing , let's hope it's only
					//length part that is incorrect and system can proceed. 
				}

			}

			// next record start position is right after this record
			recordStartPosition += recordLength + LogToFile.LOG_RECORD_OVERHEAD;
			knownGoodLogEnd = LogCounter.makeLogInstantAsLong
								(currentLogFileNumber, recordStartPosition);
			
			
			if (SanityManager.DEBUG)
			{
				if (recordStartPosition != scan.getFilePointer())
					SanityManager.THROWASSERT(
									 "calculated end " + recordStartPosition + 
									 " != real end " + scan.getFilePointer());
			}
			else
			{
				// seek to the start of the next log record
				scan.seek(recordStartPosition);
			}

			// the scan is now positioned just past this log record and right
			// at the beginning of the next log record
		} while (candidate == false) ;

		return lr;
	}


	/**
		Reset the scan to the given LogInstant.

		@param instant the position to reset to
		@exception IOException scan cannot access the log at the new position.
		@exception StandardException cloudscape standard error policy
	*/

	public void resetPosition(LogInstant instant) 
		 throws IOException, StandardException
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(instant != null);

		long instant_long = ((LogCounter)instant).getValueAsLong();

		if ((instant_long == LogCounter.INVALID_LOG_INSTANT) ||
			(stopAt != LogCounter.INVALID_LOG_INSTANT &&
			 (scanDirection == FORWARD && instant_long > stopAt) ||
			 (scanDirection == FORWARD && instant_long < stopAt)))
		{
			close();

			throw StandardException.newException(
                    SQLState.LOG_RESET_BEYOND_SCAN_LIMIT, 
                    instant, new LogCounter(stopAt));
		}
		else
		{
			long fnum = ((LogCounter)instant).getLogFileNumber();

			if (fnum != currentLogFileNumber)
			{
				if (SanityManager.DEBUG) 
                {
                    if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
                    {
                        SanityManager.DEBUG(LogToFile.DBG_FLAG, 
										"Scan " + scanDirection +
										" resetting to " + instant + 
										" need to switch log from " + 
										currentLogFileNumber + " to " + fnum);
                    }
				}

				scan.close();
				scan = logFactory.getLogFileAtPosition(instant_long);

				currentLogFileNumber= fnum;

				if (scanDirection == FORWARD)
				{
					// NOTE: 
					//
					// just get the length of the file without syncing.
					// this only works because the only place forward scan is used
					// right now is on recovery redo and nothing is being added to 
					// the current log file.  When the forward scan is used for some
					// other purpose, need to sync access to the end of the log
					//
					currentLogFileLength = scan.length();
				}
			}
			else

			{
				long fpos = ((LogCounter)instant).getLogFilePosition();
				scan.seek(fpos);

				//
				//RESOLVE: Can this be optimized? Does it belong here.
				currentLogFileLength = scan.length();

				if (SanityManager.DEBUG)
                {
                    if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
                    {
                        SanityManager.DEBUG(LogToFile.DBG_FLAG, 
										"Scan reset to " + instant);
                    }
				}
			}


			currentInstant = instant_long;

			//scan is being reset, it is possibly that, scan is doing a random 
			//access of the log file. set the knownGoodLogEnd to  the instant
			//scan 	is being reset to.
			//Note: reset gets called with undo forward scan for CLR processing during 
			//recovery, if this value is not reset checks to find the end of log 
			//getNextRecordForward() will fail because undoscan scans log file
			//back & forth to redo CLR's.
			knownGoodLogEnd = currentInstant;

			if (SanityManager.DEBUG)
            {
                if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
                {
                    SanityManager.DEBUG(LogToFile.DBG_FLAG, 
                        "Scan.getInstant reset to " + currentInstant + 
                        LogCounter.toDebugString(currentInstant));
                }
			}
		}
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
		Return the log instant at the end of the log record on the current
		LogFile in the form of a log instant
	*/
	public long getLogRecordEnd()
	{
		return knownGoodLogEnd;
	}

	/**
	   returns true if there is partially writen log records before the crash 
	   in the last log file. Partiall wrires are identified during forward 
	   redo scans for log recovery.
	*/
	public boolean isLogEndFuzzy()
	{
		return fuzzyLogEnd;
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

		logFactory = null;
		currentLogFileNumber = -1;
		currentLogFileLength = -1;
		knownGoodLogEnd = LogCounter.INVALID_LOG_INSTANT;
		currentInstant = LogCounter.INVALID_LOG_INSTANT;
		stopAt = LogCounter.INVALID_LOG_INSTANT;
		scanDirection = 0;
	}

}
