/*

   Derby - Class org.apache.derby.impl.store.raw.log.FileLogger

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

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.store.raw.RawStoreFactory;

import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.raw.log.Logger;
import org.apache.derby.iapi.store.raw.log.LogScan;

import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.store.raw.xact.TransactionFactory;
import org.apache.derby.iapi.store.raw.xact.TransactionId;

import org.apache.derby.iapi.store.raw.Compensation;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Loggable;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RePreparable;
import org.apache.derby.iapi.store.raw.Undoable;

import org.apache.derby.iapi.services.io.FormatIdOutputStream;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.i18n.MessageService;

import org.apache.derby.iapi.services.io.ArrayInputStream;
import org.apache.derby.iapi.services.io.ArrayOutputStream;
import org.apache.derby.iapi.util.ByteArray;
import org.apache.derby.iapi.services.io.DynamicByteArrayOutputStream;

import org.apache.derby.iapi.services.io.LimitObjectInput;
import java.io.IOException;

import org.apache.derby.impl.store.raw.data.InitPageOperation;

/**
	Write log records to a log file as a stream
	(ie. log records added to the end of the file, no concept of pages).
<P>
	The format of a log record that is not a compensation operation is
	<PRE>
	@format_id	no formatId, format is implied by the log file format and the
		log record content.
	@purpose	the log record and optional data
	@upgrade
	@disk_layout
		Log Record
			(see org.apache.derby.impl.store.raw.log.LogRecord)
		length(int)	length of optional data
		optionalData(byte[length]) optional data written by the log record
	@end_format
	</PRE> <HR WIDTH="100%"> 

	<P>	The form of a log record that is a compensation operation is
	<PRE>
	@format_id	no formatId, format is implied by the log file format and the
	log record content.
	@purpose	undo a previous log record
	@upgrade
	@disk_layout
		Log Record that contains the compenstation operation
			(see org.apache.derby.impl.store.raw.log.LogRecord)
		undoInstant(long) the log instant of the operation that is to be rolled back
			The undo instant is logically part of the LogRecord but is written
			by the logger because it is used and controlled by the rollback
			code but not by the log operation.
		There is no optional data in a compensation operation, all data
		necessary for the rollback must be stored in the operation being
		undone.
	@end_format
	</PRE>

    <BR>

	<P>Multithreading considerations:<BR>
	Logger must be MT-safe.	Each RawTransaction has its own private
	FileLogger object. Each logger has a logOutputBuffer and a log input
	buffer which are used to read and write to the log.  Since multiple
	threads can be in the same transaction, fileLogger must be synchronized.

	@see LogRecord
*/

public class FileLogger implements Logger {

	private LogRecord		 logRecord;

	protected byte[] encryptionBuffer;
	private DynamicByteArrayOutputStream logOutputBuffer;
	private FormatIdOutputStream logicalOut;

	private ArrayInputStream logIn;

	private LogToFile logFactory;	// actually writes the log records.

	/**
		Make a new Logger with its own log record buffers
		MT - not needed for constructor
	*/
	public FileLogger(LogToFile logFactory) {

		this.logFactory = logFactory;
		logOutputBuffer = new DynamicByteArrayOutputStream(1024); // init size 1K
		logicalOut = new FormatIdOutputStream(logOutputBuffer);

		// logIn and logOutputBuffer must share the same buffer because they
		// combined to form an IO stream to access the same log record.
		// 
		// Before each use of logIn, you must reset logIn's data to the
		// byte array you want to read from.
		//
		// To log a record, set logIn's data to point to logOutputBuffer's
		// byte array when you know you have everything you need in the output
		// buffer, then set limit on logIn and send it to the log operation's
		// doMe.
		//
		// Keep in mind the dynamic nature of the logOutputBuffer which means
		// it could switch buffer from underneath the logOutputBuffer on every
		// write.
		logIn = new ArrayInputStream();
 
		logRecord = new LogRecord();

	}

	/**
		Close the logger.
		MT - caller provide synchronization
		(RESOLVE: not called by anyone ??)
	*/
	public void close() throws IOException
	{
		if (logOutputBuffer != null)
		{
			logOutputBuffer.close();
			logOutputBuffer = null;
		}

		logIn = null;
		logFactory = null;

		logicalOut = null;

		logRecord = null;
	}

	/*
	** Methods of Logger
	*/

	/**
		Writes out a log record to the log stream, and call its doMe method to
		apply the change to the rawStore.
		<BR>Any optional data the doMe method need is first written to the log
		stream using operation.writeOptionalData, then whatever is written to
		the log stream is passed back to the operation for the doMe method.

		<P>MT - there could be multiple threads running in the same raw
		transactions and they can be calling the same logger to log different
		log operations.  This whole method is synchronized to make sure log
		records are logged one at a time.

		@param xact the transaction logging the change
		@param operation the log operation
		@return the instant in the log that can be used to identify the log
		record

		@exception StandardException Cloudscape Standard error policy
	*/
	public synchronized LogInstant logAndDo(RawTransaction xact, Loggable operation) 
		 throws StandardException 
	{
		boolean isLogPrepared = false;

		boolean inUserCode = false;
		byte[] preparedLog;

		try {

			logOutputBuffer.reset();

			// always use the short Id, only the BeginXact log record contains
			// the XactId (long form)
			TransactionId transactionId = xact.getId();

			// write out the log header with the operation embedded
			// this is by definition not a compensation log record,
			// those are called thru the logAndUndo interface
			logRecord.setValue(transactionId, operation);

			inUserCode = true;
			logicalOut.writeObject(logRecord);
			inUserCode = false;

			int optionalDataLength = 0;
			int optionalDataOffset = 0;
			int completeLength = 0;

			ByteArray preparedLogArray = operation.getPreparedLog();
			if (preparedLogArray != null) {

				preparedLog = preparedLogArray.getArray();
				optionalDataLength = preparedLogArray.getLength();
				optionalDataOffset = preparedLogArray.getOffset();

				// There is a race condition if the operation is a begin tran in
				// that between the time the beginXact log record is written to
				// disk and the time the transaction object is updated in the
				// beginXact.doMe method, other log records may be written.
				// This will render the transaction table in an inconsistent state
				// since it may think a later transaction is the earliest
				// transaction or it may think that there is no active transactions
				// where there is a bunch of them sitting on the log.
				//
				// Similarly, there is a race condition for endXact, i.e.,
				// 1) endXact is written to the log, 
				// 2) checkpoint gets that (committed) transaction as the
				//		firstUpdateTransaction
				// 3) the transaction calls postComplete, nulling out itself
				// 4) checkpoint tries to access a closed transaction object
				//
				// The solution is to sync between the time a begin tran or end
				// tran log record is sent to the log stream and its doMe method is
				// called to update the transaction table and in memory state
				//
				// We only need to serialized the begin and end Xact log records
				// because once a transaction has been started and in the
				// transaction table, its order and transaction state does not
				// change.
				//
				// Use the logFactory as the sync object so that a checkpoint can
				// take its snap shot of the undoLWM before or after a transaction
				// is started, but not in the middle. (see LogToFile.checkpoint)
				//

				// now set the input limit to be the optional data.  
				// This limits amount of data availiable to logIn that doMe can
				// use
				logIn.setData(preparedLog);
				logIn.setPosition(optionalDataOffset);
				logIn.setLimit(optionalDataLength);

				if (SanityManager.DEBUG)
				{
					if ((optionalDataLength) != logIn.available())
						SanityManager.THROWASSERT(
							" stream not set correctly " +
							optionalDataLength + " != " +
							 logIn.available());
				}

			} else {
				preparedLog = null;
				optionalDataLength = 0;
			}

			logicalOut.writeInt(optionalDataLength);
			completeLength = logOutputBuffer.getPosition() + optionalDataLength;


			LogInstant logInstant = null;
			int encryptedLength = 0; // in case of encryption, we need to pad

			try
			{
				if (logFactory.databaseEncrypted())
				{
					// we must pad the encryption data to be multiple of block
					// size, which is logFactory.getEncryptionBlockSize()
					encryptedLength = completeLength;
					if ((encryptedLength % logFactory.getEncryptionBlockSize()) != 0)
						encryptedLength = encryptedLength + logFactory.getEncryptionBlockSize() - (encryptedLength % logFactory.getEncryptionBlockSize());

					if (encryptionBuffer == null || 
						encryptionBuffer.length < encryptedLength)
						encryptionBuffer = new byte[encryptedLength];

					System.arraycopy(logOutputBuffer.getByteArray(), 0, 
									 encryptionBuffer, 0, completeLength-optionalDataLength);

					if (optionalDataLength > 0)
						System.arraycopy(preparedLog, optionalDataOffset, 
									 encryptionBuffer,
									 completeLength-optionalDataLength, optionalDataLength);

					// do not bother to clear out the padding area 
					int len = 
						logFactory.encrypt(encryptionBuffer, 0, encryptedLength, 
										   encryptionBuffer, 0);

					if (SanityManager.DEBUG)
						SanityManager.ASSERT(len == encryptedLength, 
							"encrypted log buffer length != log buffer len");
				}

				if ((operation.group() & (Loggable.FIRST | Loggable.LAST)) != 0)
				{
					synchronized (logFactory)
					{
						long instant = 0;

						if (logFactory.databaseEncrypted())
						{
							// encryption has completely drained both the the
							// logOuputBuffer array and the preparedLog array
							instant = logFactory.
								appendLogRecord(encryptionBuffer, 0, 
												encryptedLength, null, 
												-1, 0);
						}
						else
						{
							instant = logFactory.
								appendLogRecord(logOutputBuffer.getByteArray(),
												0, completeLength, preparedLog,
												optionalDataOffset,
												optionalDataLength);
						}
						logInstant = new LogCounter(instant);

						operation.doMe(xact, logInstant, logIn);
					}
				}
				else
				{
					long instant = 0;

					if (logFactory.databaseEncrypted())
					{
						// encryption has completely drained both the the
						// logOuputBuffer array and the preparedLog array
						instant = logFactory.
							appendLogRecord(encryptionBuffer, 0, 
											encryptedLength, null, -1, 0);
					}
					else
					{
						instant = logFactory.
							appendLogRecord(logOutputBuffer.getByteArray(), 0,
											completeLength, preparedLog,
											optionalDataOffset,
											optionalDataLength); 
					}

					logInstant = new LogCounter(instant);

					operation.doMe(xact, logInstant, logIn);
				}

			}
			catch (StandardException se)
			{
				throw logFactory.markCorrupt(
                        StandardException.newException(
                            SQLState.LOG_DO_ME_FAIL, se, operation));
			}
			catch (IOException ioe)
			{
				throw logFactory.markCorrupt(
                        StandardException.newException(
                            SQLState.LOG_DO_ME_FAIL, ioe, operation));
			}
			finally
			{
				logIn.clearLimit();
			}

			if (SanityManager.DEBUG)
            {
                if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
                {
                    SanityManager.DEBUG(
                        LogToFile.DBG_FLAG, 
                        "Write log record: tranId=" + transactionId.toString() +
                        " instant: " + logInstant.toString() + " length: " +
                        completeLength + "\n" + operation + "\n");
                }
			}
			return logInstant;
		}

		catch (IOException ioe) 
		{
			// error writing to the log buffer
			if (inUserCode)
            {
				throw StandardException.newException(
                        SQLState.LOG_WRITE_LOG_RECORD, ioe, operation);
            }
			else
            {
				throw StandardException.newException(
                        SQLState.LOG_BUFFER_FULL, ioe, operation);
            }
		}

	}

	/**
		Writes out a compensation log record to the log stream, and call its
		doMe method to undo the change of a previous log operation.

		<P>MT - Not needed. A transaction must be single threaded thru undo, each
		RawTransaction has its own logger, therefore no need to synchronize.
		The RawTransaction must handle synchronizing with multiple threads
		during rollback.

		@param xact the transaction logging the change
		@param compensation the compensation log operation
		@param undoInstant the log instant of the operation that is to be
		rolled back
		@param in optional data input for the compenastion doMe method
		@param dataLength optional data length

		@return the instant in the log that can be used to identify the log
		record

		@exception StandardException Cloudscape Standard error policy
	 */
	public LogInstant logAndUndo(RawTransaction xact, 
								 Compensation compensation,
								 LogInstant undoInstant,
								 LimitObjectInput in)
		 throws StandardException
	{
		boolean inUserCode = false;

		try {
			logOutputBuffer.reset();

			TransactionId transactionId = xact.getId();

			// write out the log header with the operation embedded
			logRecord.setValue(transactionId, compensation);

			inUserCode = true;
			logicalOut.writeObject(logRecord);
			inUserCode = false;

			// write out the undoInstant
			logicalOut.writeLong(((LogCounter)undoInstant).getValueAsLong());

			// in this implemetaion, there is no optional data for the
			// compensation operation.  Optional data for the rollback comes
			// from the undoable operation - and is passed into this call.
			int completeLength = logOutputBuffer.getPosition();
			long instant = 0;
			
			if (logFactory.databaseEncrypted())
			{
				// we must pad the encryption data to be multiple of block
				// size, which is logFactory.getEncryptionBlockSize()
				int encryptedLength = completeLength;
				if ((encryptedLength % logFactory.getEncryptionBlockSize()) != 0)
					encryptedLength = encryptedLength + logFactory.getEncryptionBlockSize() - (encryptedLength % logFactory.getEncryptionBlockSize());

				if (encryptionBuffer == null || 
					encryptionBuffer.length < encryptedLength)
					encryptionBuffer = new byte[encryptedLength];

				System.arraycopy(logOutputBuffer.getByteArray(), 0, 
								 encryptionBuffer, 0, completeLength);

				// do not bother to clear out the padding area 
				int len = 
					logFactory.encrypt(encryptionBuffer, 0, encryptedLength,
									   encryptionBuffer, 0);

				if (SanityManager.DEBUG)
					SanityManager.ASSERT(len == encryptedLength, 
						"encrypted log buffer length != log buffer len");

				instant = logFactory.
					appendLogRecord(encryptionBuffer,
									0, encryptedLength, null, 0, 0);
			}
			else
			{
				instant = logFactory.
					appendLogRecord(logOutputBuffer.getByteArray(), 
									0, completeLength, null, 0, 0);
			}

			LogInstant logInstant = new LogCounter(instant);

			if (SanityManager.DEBUG)
            {
                if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
                {
                    SanityManager.DEBUG(
                        LogToFile.DBG_FLAG, 
                        "Write CLR: Xact: " + transactionId.toString() +
                        "clrinstant: " + logInstant.toString() + 
                        " undoinstant " + undoInstant + "\n");
                }
			}

			try
			{
				// in and dataLength contains optional data that was written 
				// to the log during a previous call to logAndDo.
				compensation.doMe(xact, logInstant, in);
			}
			catch (StandardException se)
			{
				throw logFactory.markCorrupt(
                        StandardException.newException(
                            SQLState.LOG_DO_ME_FAIL, se, compensation));
			}
			catch (IOException ioe)
			{
				throw logFactory.markCorrupt(
                        StandardException.newException(
                            SQLState.LOG_DO_ME_FAIL, ioe, compensation));
			}

			return logInstant;

		}
		catch (IOException ioe)
		{
			if (inUserCode)
            {
				throw StandardException.newException(
                        SQLState.LOG_WRITE_LOG_RECORD, ioe, compensation);
            }
			else
            {
				throw StandardException.newException(
                        SQLState.LOG_BUFFER_FULL, ioe, compensation);
            }
		}
	}

	/**
		Flush the log up to the given log instant.

		<P>MT - not needed, wrapper method

		@exception StandardException cannot sync log file
	*/
	public void flush(LogInstant where) 
		 throws StandardException
	{
		if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
            {
                SanityManager.DEBUG(
                    LogToFile.DBG_FLAG, "Flush log to " + where.toString());
            }
		}

		logFactory.flush(where);
	}

	/**
		Flush all outstanding log to disk.

		<P>MT - not needed, wrapper method

		@exception StandardException cannot sync log file
	*/
	public void flushAll () throws StandardException
	{
		logFactory.flushAll();
	}

    /**
     * During recovery re-prepare a transaction.
     * <p>
     * After redo() and undo(), this routine is called on all outstanding 
     * in-doubt (prepared) transactions.  This routine re-acquires all 
     * logical write locks for operations in the xact, and then modifies
     * the transaction table entry to make the transaction look as if it
     * had just been prepared following startup after recovery.
     * <p>
     *
     * @param t                 is the transaction performing the re-prepare
     * @param prepareId         is the transaction ID to be re-prepared
     * @param prepareStopAt     is where the log instant (inclusive) where the 
     *                          re-prepare should stop.
     * @param prepareStartAt    is the log instant (inclusive) where re-prepare 
     *                          should begin, this is normally the log instant 
     *                          of the last log record of the transaction that 
     *                          is to be re-prepare.  If null, then re-prepare 
     *                          starts from the end of the log.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public void reprepare(
    RawTransaction  t,
    TransactionId   prepareId,
    LogInstant      prepareStopAt,
    LogInstant      prepareStartAt) 
        throws StandardException
	{

		if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
            {
                if (prepareStartAt != null)
                {
                    SanityManager.DEBUG(
                    LogToFile.DBG_FLAG, 
                    "----------------------------------------------------\n" +
                    "\nBegin of RePrepare : " + prepareId.toString() + 
                        "start at " + prepareStartAt.toString() + 
                        " stop at " + prepareStopAt.toString()  +
                    "\n----------------------------------------------------\n");
                }
                else
                {
                    SanityManager.DEBUG(
                    LogToFile.DBG_FLAG, 
                    "----------------------------------------------------\n" +
                    "\nBegin of Reprepare: " + prepareId.toString() + 
                        "start at end of log stop at " + 
                        prepareStopAt.toString() +
                    "\n----------------------------------------------------\n");
                }
            }
		}

		// statistics
		int clrskipped      = 0;
		int logrecordseen   = 0;

		RePreparable lop          = null;

		// stream to read the log record - initial size 4096, scanLog needs
		// to resize if the log record is larger than that.
		ArrayInputStream    rawInput    = null;

		try
		{
            StreamLogScan scanLog;

			if (prepareStartAt == null)
            {
                // don't know where to start, scan from end of log
				scanLog = 
                    (StreamLogScan) logFactory.openBackwardsScan(prepareStopAt);
            }
			else
			{
				if (prepareStartAt.lessThan(prepareStopAt)) 
                {
                    // nothing to prepare!
					return;
                }

				scanLog = (StreamLogScan)
					logFactory.openBackwardsScan(
                        ((LogCounter) prepareStartAt).getValueAsLong(),
                        prepareStopAt);
			}

			if (SanityManager.DEBUG)
				SanityManager.ASSERT(
                    scanLog != null, "cannot open log for prepare");

			rawInput    = new ArrayInputStream(new byte[4096]);

			LogRecord record;

			while ((record = 
                    scanLog.getNextRecord(rawInput, prepareId, 0)) 
                       != null) 
			{ 
				if (SanityManager.DEBUG)
				{
					SanityManager.ASSERT(
                        record.getTransactionId().equals(prepareId),
					    "getNextRecord return unqualified log rec for prepare");
				}

				logrecordseen++;

				if (record.isCLR())
				{
					clrskipped++;

                    // the loggable is still in the input stream, get rid of it
					record.skipLoggable(); 

					// read the prepareInstant
					long prepareInstant = rawInput.readLong();

					if (SanityManager.DEBUG)
                    {
                        if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
                        {
                            SanityManager.DEBUG(
                                LogToFile.DBG_FLAG, 
                                    "Skipping over CLRs, reset scan to " + 
                                    LogCounter.toDebugString(prepareInstant));
                        }
					}

					scanLog.resetPosition(new LogCounter(prepareInstant));
					// scanLog now positioned at the beginning of the log
					// record that was rolled back by this CLR.
					// The scan is a backward one so getNextRecord will skip
					// over the record that was rolled back and go to the one
					// previous to it

					continue;
				}

                if (record.requiresPrepareLocks())
                {
                    lop = record.getRePreparable();
                }
                else
                {
                    continue;
                }

				if (lop != null)
				{
                    // Reget locks based on log record.  reclaim all locks with
                    // a serializable locking policy, since we are only 
                    // reclaiming write locks, isolation level does not matter
                    // much.

                    lop.reclaimPrepareLocks(
                        t,
                        t.newLockingPolicy(
                            LockingPolicy.MODE_RECORD, 
                            TransactionController.ISOLATION_REPEATABLE_READ, 
                            true));

					if (SanityManager.DEBUG)
                    {
                        if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
                        {
                            SanityManager.DEBUG(
                                LogToFile.DBG_FLAG, 
                                "Reprepare log record at instant " +
                                scanLog.getInstant() + " : " + lop);
                        }
                    }

				}
			}

		}
		catch (ClassNotFoundException cnfe)
		{
			throw logFactory.markCorrupt(
                StandardException.newException(SQLState.LOG_CORRUPTED, cnfe));
		}
	    catch (IOException ioe) 
		{
			throw logFactory.markCorrupt(
                StandardException.newException(
                    SQLState.LOG_READ_LOG_FOR_UNDO, ioe));
		}
		catch (StandardException se)
		{
			throw 
                logFactory.markCorrupt(
                    StandardException.newException(
                        SQLState.LOG_UNDO_FAILED, se,  
                        prepareId, lop, (Object) null));
		}
		finally
		{
			if (rawInput != null)
			{
				try
				{
					rawInput.close();
				}
				catch (IOException ioe)
				{
					throw logFactory.markCorrupt(
                        StandardException.newException(
                            SQLState.LOG_READ_LOG_FOR_UNDO, ioe, prepareId));
				}
			}
		}

		if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
            {
                SanityManager.DEBUG(LogToFile.DBG_FLAG, "Finish prepare" +
                                    ", clr skipped = " + clrskipped + 
                                    ", record seen = " + logrecordseen + "\n");
            }
        }

		if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
            {
                SanityManager.DEBUG(
                    LogToFile.DBG_FLAG, 
                    "----------------------------------------------------\n" +
                    "End of recovery rePrepare\n" + 
                    ", clr skipped = " + clrskipped + 
                    ", record seen = " + logrecordseen + 
                    "\n----------------------------------------------------\n");
            }
        }
	}


	/**
		Undo a part of or the entire transaction.  Begin rolling back the log
		record at undoStartAt and stopping at (inclusive) the log record at
		undoStopAt.

		<P>MT - Not needed. A transaction must be single threaded thru undo, 
        each RawTransaction has its own logger, therefore no need to 
        synchronize.  The RawTransaction must handle synchronizing with 
        multiple threads during rollback.

		@param t 			the transaction that needs to be rolled back
		@param undoId 		the transaction ID
		@param undoStopAt	the last log record that should be rolled back
		@param undoStartAt	the first log record that should be rolled back

		@exception StandardException	Standard Cloudscape error policy

		@see Logger#undo
	  */
	public void undo(
    RawTransaction  t, 
    TransactionId   undoId, 
    LogInstant      undoStopAt, 
    LogInstant      undoStartAt)
		throws StandardException 
	{

		if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
            {
                if (undoStartAt != null)
                {
                    SanityManager.DEBUG(
                        LogToFile.DBG_FLAG, 
                        "\nUndo transaction: " + undoId.toString() + 
                        "start at " + undoStartAt.toString() + 
                        " stop at " + undoStopAt.toString() );
                }
                else
                {
                    SanityManager.DEBUG(
                        LogToFile.DBG_FLAG, 
                        "\nUndo transaction: " + undoId.toString() + 
                        "start at end of log stop at " + undoStopAt.toString());
                }
            }
        }

		// statistics
		int clrgenerated  = 0;
		int clrskipped    = 0;
		int logrecordseen = 0;

		StreamLogScan scanLog;
		Compensation  compensation = null;
		Undoable      lop          = null;

		// stream to read the log record - initial size 4096, scanLog needs
		// to resize if the log record is larget than that.
		ArrayInputStream    rawInput   = null;

		try
		{
			if (undoStartAt == null)	
            {
                // don't know where to start, rollback from end of log

				scanLog = (StreamLogScan)
					logFactory.openBackwardsScan(undoStopAt);
            }
			else
			{
				if (undoStartAt.lessThan(undoStopAt))
                {
                    // nothing to undo!
					return;
                }

				long undoStartInstant = 
                    ((LogCounter) undoStartAt).getValueAsLong();

				scanLog = (StreamLogScan)
					logFactory.openBackwardsScan(undoStartInstant, undoStopAt);
			}

			if (SanityManager.DEBUG)
				SanityManager.ASSERT(
                    scanLog != null, "cannot open log for undo");

			rawInput   = new ArrayInputStream(new byte[4096]);

			LogRecord record;

			while ((record = 
                    scanLog.getNextRecord(rawInput, undoId, 0)) 
                        != null) 
			{ 
				if (SanityManager.DEBUG)
				{
					SanityManager.ASSERT(
                        record.getTransactionId().equals(undoId),
                        "getNextRecord return unqualified log record for undo");
				}

				logrecordseen++;

				if (record.isCLR())
				{
					clrskipped++;

                    // the loggable is still in the input stream, get rid of it
					record.skipLoggable(); 

					// read the undoInstant
					long undoInstant = rawInput.readLong();

					if (SanityManager.DEBUG)
                    {
                        if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
                        {
                            SanityManager.DEBUG(
                                LogToFile.DBG_FLAG, 
                                "Skipping over CLRs, reset scan to " + 
                                LogCounter.toDebugString(undoInstant));
                        }
                    }


					scanLog.resetPosition(new LogCounter(undoInstant));

					// scanLog now positioned at the beginning of the log
					// record that was rolled back by this CLR.
					// The scan is a backward one so getNextRecord will skip
					// over the record that was rolled back and go to the one
					// previous to it

					continue;
				}

				lop = record.getUndoable();

				if (lop != null)
				{
					int optionalDataLength = rawInput.readInt();
					int savePosition = rawInput.getPosition();
					rawInput.setLimit(savePosition, optionalDataLength);
	
					compensation = lop.generateUndo(t, rawInput);

					if (SanityManager.DEBUG)
                    {
                        if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
                        {
                            SanityManager.DEBUG(
                                LogToFile.DBG_FLAG, 
                                "Rollback log record at instant " +
                                LogCounter.toDebugString(scanLog.getInstant()) +
                                " : " + lop);
                        }
                    }

					clrgenerated++;

					if (compensation != null)
					{
						// generateUndo may have read stuff off the
						// stream, reset it for the undo operation.
						rawInput.setLimit(savePosition, optionalDataLength);

						// log the compensation op that rolls back the 
                        // operation at this instant 
						t.logAndUndo(
                            compensation, new LogCounter(scanLog.getInstant()),
                            rawInput);

						compensation.releaseResource(t);
						compensation = null;
					}

					// if compensation is null, log operation is redo only
				}
				// if this is not an undoable operation, continue with next log
				// record
			}
		}
		catch (ClassNotFoundException cnfe)
		{
			throw logFactory.markCorrupt(
                StandardException.newException(SQLState.LOG_CORRUPTED, cnfe));
		}
	    catch (IOException ioe) 
		{
			throw logFactory.markCorrupt(
                StandardException.newException(
                    SQLState.LOG_READ_LOG_FOR_UNDO, ioe));
		}
		catch (StandardException se)
		{
            // TODO (4327) - exceptions caught here are nested in the exception
            // below but for some reason the nested exceptions are not logged 
            // or reported in any way.

			throw logFactory.markCorrupt(
                StandardException.newException(
                    SQLState.LOG_UNDO_FAILED, se, undoId, lop, compensation));
		}
		finally
		{
			if (compensation != null) 
            {
                // errored out
				compensation.releaseResource(t);
            }

			if (rawInput != null)
			{
				try
				{
					rawInput.close();
				}
				catch (IOException ioe)
				{
					throw logFactory.markCorrupt(
                        StandardException.newException(
                            SQLState.LOG_READ_LOG_FOR_UNDO, ioe, undoId));
				}
			}
		}

		if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
            {
                SanityManager.DEBUG(
                    LogToFile.DBG_FLAG, 
                        "Finish undo" +
                        ", clr generated = " + clrgenerated +
                        ", clr skipped = " + clrskipped + 
                        ", record seen = " + logrecordseen + "\n");
            }
        }
	}


	/**
		Recovery Redo loop.

		<P> The log stream is scanned from the beginning (or
		from the undo low water mark of a checkpoint) forward until the end.
		The purpose of the redo pass is to repeat history, i.e, to repeat
		exactly the same set of changes the rawStore went thru right before it
		stopped.   With each log record that is encountered in the redo pass:
		<OL>
		<LI>if it isFirst(), then the transaction factory is called upon to
		    create a new transaction object.
		<LI>if it needsRedo(), its doMe() is called (if it is a compensation
		    operation, then the undoable operation needs to be created first
            before the doMe is called).
		<LI>if it isComplete(), then the transaction object is closed.
		</OL>

		<P> MT - caller provides synchronization

		@param logData          - a scratch area to put the log record in
		@param rawStoreFactory  - the raw store factory
		@param transFactory     - the transaction factory
		@param redoStart        - if > 0, a checkpoint was found.
		                          Start the log scan from here to rebuild 
                                  transaction table.
		@param redoLWM          - if checkpoint seen, starting from this point
                                  on, apply redo if necessary

		@return the log instant of the next log record (or the instant just
		after the last log record).  This is used to determine where the log
		truly ends

		@exception StandardException Standard Cloudscape error policy
		@exception IOException error reading log file
		@exception ClassNotFoundException log file corrupted

		@see LogToFile#recover
	 */
	protected long redo(
    RawTransaction      recoveryTransaction,
    TransactionFactory  transFactory,
    StreamLogScan       redoScan,
    long                redoLWM,
    long                ttabInstant)
		 throws IOException, StandardException, ClassNotFoundException
	{
		// begin debug info
		if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
            {
                SanityManager.DEBUG(
                    LogToFile.DBG_FLAG, 
                    "In recovery redo, redoLWM = " + redoLWM);
            }
        }

		int scanCount    = 0;
        int redoCount    = 0;
        int prepareCount = 0; 
        int clrCount     = 0;
        int btranCount   = 0;
        int etranCount   = 0;

		// end debug info

		TransactionId tranId = null;

        // the current log instant
		long instant = LogCounter.INVALID_LOG_INSTANT;

		//////////////////////////////////////////////////////////////////////
		// During redo time, the byte array in the logOutputBuffer is not used.
		// Use it to read the log record - if it is not big enough, scan
		// will resize it.  We could create a brand new log input stream that
		// has nothing to do with logIn or logOutputBuffer but that seem like
		// a waste of memory.
		//////////////////////////////////////////////////////////////////////
		logIn.setData(logOutputBuffer.getByteArray());

		// use this scan to reconstitute operation to be undone
		// when we see a CLR in the redo scan
		StreamLogScan undoScan  = null;
		Loggable      op        = null;
		long          logEnd    = 0;  // we need to determine the log's true end

		try 
        {

			// scan the log forward in redo pass and go to the end
			LogRecord record;
			while((record = 
                    redoScan.getNextRecord(logIn, null, 0)) 
                        != null)
			{
				scanCount++;
				long undoInstant = 0;

				// last known good instant
				instant = redoScan.getInstant();

				// last known good log end
				logEnd = redoScan.getLogRecordEnd();


				// NOTE NOTE -- be very careful about the undoInstant, it is
				// read off the input stream in this debug section.
				// if we change the log format we will need to change the way
				// the undo instant is gotten.  Also, once it is read off, it
				// should not be read from the stream any more
				// NOTE NOTE
				if (SanityManager.DEBUG)
                {
                    if (SanityManager.DEBUG_ON(LogToFile.DUMP_LOG_ONLY) ||
                        SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))

                    {
                        if (SanityManager.DEBUG_ON(LogToFile.DUMP_LOG_ONLY))
                            SanityManager.DEBUG_SET(LogToFile.DBG_FLAG);

                        op = record.getLoggable();
                        tranId = record.getTransactionId();
                        if (record.isCLR())	
                        {
                            // !!!!!!! this moves the file pointer
                            undoInstant = logIn.readLong();

                            SanityManager.DEBUG(
                                LogToFile.DBG_FLAG, 
                                "scanned " + tranId + " : " + op + 
                                " instant = " + 
                                    LogCounter.toDebugString(instant) + 
                                " undoInstant : " + 
                                    LogCounter.toDebugString(undoInstant));
                        }
                        else
                        {
                            SanityManager.DEBUG(
                                LogToFile.DBG_FLAG, 
                                "scanned " + tranId + " : " + op + 
                                " instant = " + 
                                    LogCounter.toDebugString(instant)
                                + " logEnd = " + 
                                    LogCounter.toDebugString(logEnd) 
                                + " logIn at " + logIn.getPosition() 
                                + " available " + logIn.available());
                        }

                        // we only want to dump the log, don't touch it
                        if (SanityManager.DEBUG_ON(LogToFile.DUMP_LOG_ONLY))
                            continue;
                    }
                }

				// if the redo scan is between the undoLWM and redoLWM, we only
				// need to redo begin and end tran.  Everything else has
				// already been flushed by checkpoint
				if (redoLWM != 
                        LogCounter.INVALID_LOG_INSTANT && instant < redoLWM)
				{
					if (!(record.isFirst()      || 
                          record.isComplete()   || 
                          record.isPrepare()))
                    {
						continue;
                    }
				}

				// get the transaction
				tranId = record.getTransactionId();

				// if this transaction is known to the transaction factory, make
                // the recoveryTransaction assume its identitiy and properties
                // otherwise, make it known to the transaction factory
				if (!transFactory.findTransaction(tranId, recoveryTransaction))
				{
					// transaction not found

					if (redoLWM != LogCounter.INVALID_LOG_INSTANT && 
						instant < redoLWM &&
                        (record.isPrepare() || record.isComplete()))
					{
						// What is happening here is that a transaction that
						// started before the undoLWM has commited by the time
						// the checkpoint undoLWM was taken.  Hence, we only
						// see the tail end of its log record and its endXact
                        // record. 
						// 
						// NOTE:
						// Since we didn't see its beginXact, we cannot do the
						// endXact's doMe either.  Also if the endXact, is
                        // actually just a prepare, we don't need to do
                        // anything as the transaction will commit or abort
                        // prior to point we are recovering to.
						// If it is deemed necessary to do the endXact's doMe, 
                        // then we should start the transaction right here. 
                        // For now, just completely ignore this transaction
						// 
						etranCount++;

						continue;
					}

					if ((ttabInstant == LogCounter.INVALID_LOG_INSTANT) && 
                        !record.isFirst())
                    {
						throw StandardException.newException(
                            SQLState.LOG_UNEXPECTED_RECOVERY_PROBLEM,
                            MessageService.getTextMessage(MessageId.LOG_RECORD_NOT_FIRST,tranId));

                    }

					if (SanityManager.DEBUG)
					{
						// if we dumped the transaction table but see a non 
                        // BeginXact record after the transaction table dump 
                        // instant, error.
						if (ttabInstant != LogCounter.INVALID_LOG_INSTANT)
						{
							if (instant > ttabInstant && !record.isFirst())
                            {
								SanityManager.THROWASSERT(
								"log record is Not first but transaction " + 
                                "is not in transaction table (2) : " + tranId);
                            }

							// If we dump the transaction table and the table
							// does not have the transaction, and we see this
							// beginXact before the ttab instant, we could have
							// igored it because we "know" that we should see
							// the endXact before the ttab instant also.
							// Leave it in just in case.
						}
					}
					btranCount++;

					// the long transaction ID is embedded in the beginXact log
					// record.  The short ID is stored in the log record.
					recoveryTransaction.setTransactionId(
                        record.getLoggable(), tranId);

				}
				else				
				{
                    // recoveryTransaction found
                    
					if ((ttabInstant == LogCounter.INVALID_LOG_INSTANT) && 
                         record.isFirst())
                    {
						throw StandardException.newException(
                            SQLState.LOG_UNEXPECTED_RECOVERY_PROBLEM,
                            MessageService.getTextMessage(MessageId.LOG_RECORD_FIRST,
                                tranId));

 
                    }

					if (SanityManager.DEBUG)
					{
						if (ttabInstant != LogCounter.INVALID_LOG_INSTANT &&
							instant > ttabInstant &&
							record.isFirst())
                        {
							SanityManager.THROWASSERT(
								"log record is first but transaction is " + 
                                "already in transaction table (3): " + tranId);
                        }

                        if (record.isPrepare())
                            prepareCount++;
					}

					// if we have a transaction table dumped with the
					// checkpoint log record, then during the redo scan we may
					// see the beginXact of a transaction which is already in
                    // the transaction table, just ignore it if it is after the
					// redoLWM but before the transaction table instant.  We
					// still need to redo any database changes but since the
					// transaction is already recorded in the transaction
					// table, ignore it.
					//
					if (record.isFirst())
					{
						btranCount++;
						continue;
					}
				}
				
				op = record.getLoggable();

				if (SanityManager.DEBUG)
                {
                    if (!record.isCLR())
                    {
                        if (logIn.available() < 4)
                        {
                            SanityManager.THROWASSERT(
                              "not enough bytes read in : " + 
                                  logIn.available() + 
                              " for " + op + " instant " + 
                                  LogCounter.toDebugString(instant));
                        }
                    }
                }

				if (SanityManager.DEBUG)
                {
					SanityManager.ASSERT(
                        !recoveryTransaction.handlesPostTerminationWork(),
					 	"recovery transaction handles post termination work");
                }

				if (op.needsRedo(recoveryTransaction))
				{
					redoCount++;

					if (record.isCLR())	
					{
						clrCount++;

						// the log operation is not complete, the operation to
						// undo is stashed away at the undoInstant.
						// Reconstitute that first.

						if (SanityManager.DEBUG)
							SanityManager.ASSERT(op instanceof Compensation);


                        // this value may be set by sanity xxxx
						if (undoInstant == 0) 
							undoInstant = logIn.readLong();

						if (undoScan == null)
						{
							undoScan = (StreamLogScan)
								logFactory.openForwardsScan(
                                    undoInstant,(LogInstant)null);
						}
						else
						{
							undoScan.resetPosition(new LogCounter(undoInstant));
						}

						// undoScan now positioned at the beginning of the log
						// record was rolled back by this CLR.  
						// The scan is a forward one so getNextRecord will get 
                        // the log record that needs to be rolled back.

						// reuse the buffer in logIn and logIn since CLR 
                        // has no optional data and has no use for them anymore 
						logIn.clearLimit();
						LogRecord undoRecord =
							undoScan.getNextRecord(logIn, null, 0);

						Undoable undoOp = undoRecord.getUndoable();

						if (SanityManager.DEBUG)
						{
							SanityManager.DEBUG(
                                LogToFile.DBG_FLAG, 
                                "Redoing CLR: undoInstant = " + 
                                    LogCounter.toDebugString(undoInstant) +
                                " clrinstant = " + 
                                    LogCounter.toDebugString(instant));

							SanityManager.ASSERT(
                                undoRecord.getTransactionId().equals(tranId));

							SanityManager.ASSERT(undoOp != null);
						}

						((Compensation)op).setUndoOp(undoOp);
					}

					// at this point, logIn points to the optional
					// data of the loggable that is to be redone or to be
					// rolled back
					
					if (SanityManager.DEBUG)
                    {
                        if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
                        {
                            SanityManager.DEBUG(
                                LogToFile.DBG_FLAG, 
                                "redoing " + op + 
                                " instant = " + 
                                LogCounter.toDebugString(instant));
                        }
                    }

					int dataLength = logIn.readInt();
					logIn.setLimit(logIn.getPosition(), dataLength);
										
					// even though the log has already been written, we need to
					// tie the page to the log stream so that if redo failed
					// for some reasons, the log factory's corruption will stop
					// the corrupt page from flushing to disk.

					op.doMe(
                        recoveryTransaction, 
                        new LogCounter(instant), logIn);

					op.releaseResource(recoveryTransaction);

					op = null;
				}

				// RESOLVE: to speed up undo, may want to update the 
				// LastLogInstant in the transaction table.  
				// Right now, undo always start from the end of the log.

				// one last thing, if this is the last log record of the
				// transaction, then commit the transaction and clean up
				//
				// 'commit' even though the transaction maybe a rollback
				// because we already did all the rollback work when redoing
				// the CLRs.  Commit will only flush the log if this session
				// has written any transaction, so in this case, it is a noop.
				if (record.isComplete())
				{
					etranCount++;

					if (SanityManager.DEBUG)
						SanityManager.ASSERT(
                            !recoveryTransaction.handlesPostTerminationWork(),
                            "recovery xact handles post termination work");

					recoveryTransaction.commit();
				}
			}
		}
		catch (StandardException se)
		{
            throw StandardException.newException(
                    SQLState.LOG_REDO_FAILED, se, op);
		}
		finally
		{
			// close all the io streams
			redoScan.close();
			redoScan = null;

			if (undoScan != null)
			{
				undoScan.close();
				undoScan = null;
			}

			if (op != null)
				op.releaseResource(recoveryTransaction);

		}

		if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
            {
                SanityManager.DEBUG(
                    LogToFile.DBG_FLAG, 
                    "----------------------------------------------------\n" +
                    "End of recovery redo\n" + 
                    "Scanned = " + scanCount + " log records" +
                    ", redid = " + redoCount +
                    " ( clr = " + clrCount + " )" +
                    " begintran = " + btranCount +
                    " endtran = " + etranCount + 
                    " preparetran = " + prepareCount + 
                    "\n log ends at " + LogCounter.toDebugString(logEnd) +
                    "\n----------------------------------------------------\n");
            }
        }

		if (SanityManager.DEBUG)
		{
			// make sure logEnd and instant is consistent
			if (instant != LogCounter.INVALID_LOG_INSTANT)	
            {
				SanityManager.ASSERT(
                    LogCounter.getLogFileNumber(instant) ==
                         LogCounter.getLogFileNumber(logEnd) &&
                     LogCounter.getLogFilePosition(instant) <=
                         LogCounter.getLogFilePosition(logEnd));
            }
			else
            {
				SanityManager.ASSERT(logEnd == LogCounter.INVALID_LOG_INSTANT);
            }
		}

        // logEnd is the last good log record position in the log
		return logEnd;			
	}


	/**
		Read the next log record from the scan.

		<P>MT - caller must provide synchronization (right now, it is only
		called in recovery to find the checkpoint log record.  When this method
		is called by a more general audience, MT must be revisited).

		@param scan an opened log scan
		@param size estimated size of the log record

		@return the log operation that is the next in the scan, or null if no
		more log operation in the log scan

		@exception IOException	Error reading the log file
		@exception StandardException Standard Cloudscape error policy
		@exception ClassNotFoundException log corrupted
	 */
	protected Loggable readLogRecord(StreamLogScan scan, int size)
		 throws IOException, StandardException, ClassNotFoundException
	{
		Loggable lop = null;

		ArrayInputStream logInputBuffer = new ArrayInputStream(new byte[size]);

		LogRecord record = scan.getNextRecord(logInputBuffer, null, 0);
		if (record != null)
			lop = record.getLoggable();
		return lop;
	}

}
