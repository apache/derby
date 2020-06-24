/*

   Derby - Class org.apache.derby.impl.store.raw.log.LogAccessFile

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

package org.apache.derby.impl.store.raw.log;

import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.io.StorageRandomAccessFile;

import java.io.IOException;
import java.io.OutputStream;
import java.io.SyncFailedException;
import java.util.LinkedList;

import org.apache.derby.iapi.services.io.FormatIdOutputStream;
import org.apache.derby.iapi.services.io.ArrayOutputStream;
import org.apache.derby.iapi.store.replication.master.MasterFactory;
import org.apache.derby.iapi.store.raw.RawStoreFactory;

import org.apache.derby.iapi.util.InterruptStatus;

/**
	Wraps a RandomAccessFile file to provide buffering
	on log writes. Only supports the write calls
	required for the log!

	MT - unsafe.  Caller of this class must provide synchronization.  The one
	exception is with the log file access, LogAccessFile will touch the log
	only inside synchronized block protected by the semaphore, which is
	defined by the creator of this object.
	
    Write to the log buffers are allowed when there are free buffers even
    when dirty buffers are being written(flushed) to the disk by a different
	thread. Only one flush writes to log file at a time, other wait for it to finish.

	Except for flushLogAccessFile , SyncAccessLogFile other function callers
	must provide syncronization that will allow only one of them to write to 
    the buffers. 

    Log Buffers are used in circular fashion, each buffer moves through following stages: 
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
	freeBuffers --&gt; dirtyBuffers --&gt; freeBuffers. Movement of buffers from one
    stage to 	another stage is synchronized using	the object(this) of this class. 

	A Checksum log record that has the checksum value for the data that is
    being written to the disk is generated and written 	before the actual data. 
	Except for the large log records that does not fit into a single buffer, 
    checksum is calcualted for a group of log records that are in the buffer 
	when buffers is switched. Checksum log record is written into the reserved
	space in the beginning buffer. 

//IC see: https://issues.apache.org/jira/browse/DERBY-2939
    In case of a large log record that does not fit into a buffer, the
    checksum is written to the byte[] allocated for the big log
    record. 

	Checksum log records helps in identifying the incomplete log disk writes during 
    recovery. This is done by recalculating the checksum value for the data on
    the disk and comparing it to the the value stored in the checksum log
    record. 

*/
public class LogAccessFile 
{

    /**
     * The fixed size of a log record is 16 bytes:
     *     int   length             : 4 bytes
     *     long  instant            : 8 bytes
     *     int   trailing length    : 4 bytes
     **/
    private static final int            LOG_RECORD_FIXED_OVERHEAD_SIZE = 16;
	private static final int            LOG_RECORD_HEADER_SIZE = 12; //(length + instant)
	private static final int            LOG_RECORD_TRAILER_SIZE = 4; //trailing length 
    private static final int            LOG_NUMBER_LOG_BUFFERS = 3;


	private LinkedList<LogAccessFileBuffer>    freeBuffers;  //list of free buffers
	private LinkedList<LogAccessFileBuffer>    dirtyBuffers; //list of dirty buffers to flush
	private  LogAccessFileBuffer currentBuffer; //current active buffer
	private boolean flushInProgress = false;
	
	private final StorageRandomAccessFile  log;

	// log can be touched only inside synchronized block protected by
	// logFileSemaphore.
	private final Object            logFileSemaphore;

	static int                      mon_numWritesToLog;
	static int                      mon_numBytesToLog;

    // the MasterFactory that will accept log when in replication master mode
//IC see: https://issues.apache.org/jira/browse/DERBY-2977
    MasterFactory masterFac; 
    boolean inReplicationMasterMode = false;
    boolean inReplicationSlaveMode = false;

	//streams used to generated check sume log record ; see if there is any simpler way
	private ArrayOutputStream logOutputBuffer;
	private FormatIdOutputStream logicalOut;
	private long checksumInstant = -1;
	private int checksumLength;
	private int checksumLogRecordSize;      //checksumLength + LOG_RECORD_FIXED_OVERHEAD_SIZE
	private boolean writeChecksum; 
	private ChecksumOperation checksumLogOperation;
	private LogRecord checksumLogRecord;
	private LogToFile logFactory;
	private boolean databaseEncrypted=false;
		
	public LogAccessFile(LogToFile logFactory,
//IC see: https://issues.apache.org/jira/browse/DERBY-96
						 StorageRandomAccessFile    log, 
						 int                 bufferSize) 
    {
		if (SanityManager.DEBUG)
		{
			if(SanityManager.DEBUG_ON("LogBufferOff"))
				bufferSize = 10;	// make it very tiny
		}
		
		// Puts this LogAccessFile object in replication slave or
		// master mode if the database has such a role
		logFactory.checkForReplication(this);
//IC see: https://issues.apache.org/jira/browse/DERBY-3184

		this.log            = log;
		logFileSemaphore    = log;
		this.logFactory     = logFactory;
//IC see: https://issues.apache.org/jira/browse/DERBY-96

		if (SanityManager.DEBUG)
            SanityManager.ASSERT(LOG_NUMBER_LOG_BUFFERS >= 1);
				
		//initialize buffers lists
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
		freeBuffers = new LinkedList<LogAccessFileBuffer>();
		dirtyBuffers = new LinkedList<LogAccessFileBuffer>();


		//add all buffers to free list
        for (int i = 0; i < LOG_NUMBER_LOG_BUFFERS; i++)
        {
            LogAccessFileBuffer b = new LogAccessFileBuffer(bufferSize);
            freeBuffers.addLast(b);
        }

		currentBuffer = (LogAccessFileBuffer) freeBuffers.removeFirst();
		
		// Support for Transaction Log Checksum in Derby was added in 10.1
		// Check to see if the Store have been upgraded to 10.1 or later before
		// writing the checksum log records.  Otherwise recovery will fail
		// incase user tries to revert back to versions before 10.1 in 
		// soft upgrade mode. 
//IC see: https://issues.apache.org/jira/browse/DERBY-96
		writeChecksum = logFactory.checkVersion(RawStoreFactory.DERBY_STORE_MAJOR_VERSION_10, 
												RawStoreFactory.DERBY_STORE_MINOR_VERSION_1);

		// Checksums are received from the master if in slave replication mode
//IC see: https://issues.apache.org/jira/browse/DERBY-3184
		if (inReplicationSlaveMode) writeChecksum = false;
//IC see: https://issues.apache.org/jira/browse/DERBY-96
		if(writeChecksum)
		{
			/**
			 * setup structures that are required to write the checksum log records
			 * for a group of log records are being written to the disk. 
			 */
			checksumLogOperation = new ChecksumOperation();
			checksumLogOperation.init();
			checksumLogRecord = new LogRecord();

			// Note: Checksum log records are not related any particular transaction, 
			// they are written to store a checksum information identify
			// incomplete log record writes. No transacton id is set for this
			// log record. That is why a null argument is passed below 
			// setValue(..) call. 
			checksumLogRecord.setValue(null, checksumLogOperation);

			checksumLength = 
				checksumLogRecord.getStoredSize(checksumLogOperation.group(), null) + 
				checksumLogOperation.getStoredSize();

			// calculate checksum log operation length when the database is encrypted
			if (logFactory.databaseEncrypted())
			{
				checksumLength =  logFactory.getEncryptedDataLength(checksumLength);
				databaseEncrypted = true;
			}
			checksumLogRecordSize = checksumLength  + LOG_RECORD_FIXED_OVERHEAD_SIZE;

			//streams required to convert a log record to raw byte array. 
			logOutputBuffer = new ArrayOutputStream(); 
			logicalOut = new FormatIdOutputStream(logOutputBuffer);

			/** initialize the buffer with space reserved for checksum log record in
			 * the beginning of the log buffer; checksum record is written into
			 * this space when buffer is switched
			 */
		}else
		{
			//checksumming of transaction log feature is not in use. 
			checksumLogRecordSize = 0;
		}
		
		currentBuffer.init(checksumLogRecordSize);
	}


    /**
     * Write a single log record to the stream.
     * <p>
     * For performance pass all parameters rather into a specialized routine
     * rather than maintaining the writeInt, writeLong, and write interfaces
     * that this class provides as a standard OutputStream.  It will make it
     * harder to use other OutputStream implementations, but makes for less
     * function calls and allows optimizations knowing when to switch buffers.
     * <p>
     * This routine handles all log records which are smaller than one log
     * buffer.  If a log record is bigger than a log buffer it calls
     * writeUnbufferedLogRecord().
     * <p>
     * The log record written will always look the same as if the following
     * code had been executed:
     *     writeInt(length)
     *     writeLong(instant)
     *     write(data, data_offset, (length - optional_data_length) )
     *
     *     if (optional_data_length != 0)
     *         write(optional_data, optional_data_offset, optional_data_length)
     *
     *     writeInt(length)
     *
     * @param length                (data + optional_data) length bytes to write
     * @param instant               the log address of this log record.
     * @param data                  "from" array to copy "data" portion of rec
     * @param data_offset           offset in "data" to start copying from.
     * @param optional_data         "from" array to copy "optional data" from
     * @param optional_data_offset  offset in "optional_data" to start copy from
     * @param optional_data_length  length of optional data to copy.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void writeLogRecord(
    int     length,
    long    instant,
    byte[]  data,
    int     data_offset,
    byte[]  optional_data,
    int     optional_data_offset,
    int     optional_data_length)
        throws StandardException, IOException 
    {
        int total_log_record_length = length + LOG_RECORD_FIXED_OVERHEAD_SIZE;

//IC see: https://issues.apache.org/jira/browse/DERBY-2939
        if (total_log_record_length <= currentBuffer.bytes_free) {
            int newpos = appendLogRecordToBuffer(currentBuffer.buffer,
                                                 currentBuffer.position,
                                                 length, 
                                                 instant, 
                                                 data, 
                                                 data_offset,
                                                 optional_data,
                                                 optional_data_offset,
                                                 optional_data_length);
            currentBuffer.position = newpos;
            currentBuffer.bytes_free -= total_log_record_length;
//IC see: https://issues.apache.org/jira/browse/DERBY-2977
            currentBuffer.greatest_instant = instant;
            if (SanityManager.DEBUG) {
                int normalizedPosition = currentBuffer.position;
                if (writeChecksum) {
                    normalizedPosition -= checksumLogRecordSize;
                }
                SanityManager.ASSERT(
                    currentBuffer.bytes_free + normalizedPosition ==
                    currentBuffer.length,
                    "free_bytes and position do not add up to the total " +
                    "length of the buffer");
            }

        } else {
            /* The current log record will never fit in a single
             * buffer. The reason is that reserveSpaceForChecksum is
             * always called before writeLogRecord (see
             * LogToFile#appendLogRecord). When we reach this point,
             * reserveSpaceForChecksum has already found out that the
             * previous buffer did not have enough free bytes to store
             * this log record, and therefore switched to a fresh
             * buffer. Hence, currentBuffer is empty now, and
             * switching to the next free buffer will not help. Since
             * there is no way for this log record to fit into a
             * buffer, it is written to a new, big enough, byte[] and
             * then written to log file instead of writing it to
             * buffer.
             */

            // allocate a byte[] that is big enough to contain the
            // giant log record:
            int bigBufferLength =
                checksumLogRecordSize + total_log_record_length;
            byte[] bigbuffer = new byte[bigBufferLength];
            appendLogRecordToBuffer(bigbuffer, checksumLogRecordSize,
                                    length, 
                                    instant, 
                                    data, 
                                    data_offset,
                                    optional_data,
                                    optional_data_offset,
                                    optional_data_length);

            // write checksum to bigbuffer
            if(writeChecksum) {
                checksumLogOperation.reset();
                checksumLogOperation.update(bigbuffer, checksumLogRecordSize,
                                            total_log_record_length);

                writeChecksumLogRecord(bigbuffer);
            }

            // flush all buffers before writing the bigbuffer to the
            // log file.
            flushLogAccessFile();

            // Note:No Special Synchronization required here , There
            // will be nothing to write by flushDirtyBuffers that can
            // run in parallel to the threads that is executing this
            // code. Above flush call should have written all the
            // buffers and NO new log will get added until the
            // following direct log to file call finishes.

			// write the log record directly to the log file.
//IC see: https://issues.apache.org/jira/browse/DERBY-2977
            writeToLog(bigbuffer, 0, bigBufferLength, instant);
        }
    }

    /**
     * Append a log record to a byte[]. Typically, the byte[] will be
     * currentBuffer, but if a log record that is too big to fit in a
     * buffer is added, buff will be a newly allocated byte[].
     *
     * @param buff The byte[] the log record is appended to
     * @param pos The position in buff where the method will start to
     * append to
     * @param length (data + optional_data) length bytes to write
     * @param instant the log address of this log record.
     * @param data "from" array to copy "data" portion of rec
     * @param data_offset offset in "data" to start copying from.
     * @param optional_data "from" array to copy "optional data" from
     * @param optional_data_offset offset in "optional_data" to start copy from
     * @param optional_data_length length of optional data to copy.
     *
     * @see LogAccessFile#writeLogRecord
     */
    private int appendLogRecordToBuffer(byte[] buff, int pos,
                                        int length,
                                        long instant,
                                        byte[] data,
                                        int data_offset,
                                        byte[] optional_data,
                                        int optional_data_offset,
                                        int optional_data_length) {

        pos = writeInt(length, buff, pos);
        pos = writeLong(instant, buff, pos);

        int data_length = length - optional_data_length;
        System.arraycopy(data, data_offset,
                         buff, pos,
                         data_length);
        pos += data_length;

        if (optional_data_length != 0) {
            System.arraycopy(optional_data, optional_data_offset, 
                             buff, pos, 
                             optional_data_length);
            pos += optional_data_length;
        }

        pos = writeInt(length, buff, pos);

        return pos;
    }



	private final int writeInt(int i , byte b[], int p)
	{
	
        b[p++] = (byte) ((i >>> 24) & 0xff); 
        b[p++] = (byte) ((i >>> 16) & 0xff); 
        b[p++] = (byte) ((i >>> 8) & 0xff); 
        b[p++] = (byte) (i & 0xff);	
		return p;
	}


	private final int writeLong(long l , byte b[], int p)
	{
		b[p++] = (byte) (((int)(l >>> 56)) & 0xff); 
        b[p++] = (byte) (((int)(l >>> 48)) & 0xff); 
        b[p++] = (byte) (((int)(l >>> 40)) & 0xff); 
        b[p++] = (byte) (((int)(l >>> 32)) & 0xff); 
        b[p++] = (byte) (((int)(l >>> 24)) & 0xff); 
        b[p++] = (byte) (((int)(l >>> 16)) & 0xff); 
        b[p++] = (byte) (((int)(l >>> 8)) & 0xff); 
        b[p++] = (byte) (((int)l) & 0xff); 
//IC see: https://issues.apache.org/jira/browse/DERBY-96
		return p;
	}

	public void writeInt(int i) 
    {

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(currentBuffer.bytes_free >= 4);
		}
		
		currentBuffer.position = 
			writeInt(i , currentBuffer.buffer, currentBuffer.position);
		currentBuffer.bytes_free -= 4;
	}

	public void writeLong(long l) 
    {
		
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(currentBuffer.bytes_free >= 8);
		}
		
		currentBuffer.position = 
			writeLong(l , currentBuffer.buffer, currentBuffer.position);
		currentBuffer.bytes_free -= 8;
    }

	public void write(int b) 
    {
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(currentBuffer.bytes_free > 0);
		}
		
		currentBuffer.buffer[currentBuffer.position++] = (byte) b;
		currentBuffer.bytes_free--;
	}


	public void write(byte b[], int off, int len) 
    {
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(len <= currentBuffer.bytes_free);
		}
		
		System.arraycopy(b, off, currentBuffer.buffer, currentBuffer.position, len);
		currentBuffer.bytes_free -= len;
		currentBuffer.position += len;
	}


    /**
     * Write data from all dirty buffers into the log file.
     * <p>
     * A call for clients of LogAccessFile to insure that all privately buffered
     * data has been writen to the file - so that reads on the file using one
     * of the various scan classes will see
     * all the data which has been writen to this point.
     * <p>
     * Note that this routine only "writes" the data to the file, this does not
     * mean that the data has been synced to disk unless file was opened in
	 * WRITE SYNC mode(rws/rwd).  The only way to insure that is by calling
     * is to call syncLogAccessFile() after this call in Non-WRITE sync mode(rw)
	 * 
	 * <p>
	 * MT-Safe : parallel thereads can call this function, only one threads does
	 * the flush and the other threads waits for the one that is doing the flush to finish.
	 * Currently there are two possible threads that can call this function in parallel 
	 * 1) A Thread that is doing the commit
	 * 2) A Thread that is writing to the log and log buffers are full or
	 * a log records does not fit in a buffer. (Log Buffers
	 * full(switchLogBuffer() or a log record size that is greater than
	 * logbuffer size has to be writtern through writeToLog call directlty)
	 * Note: writeToLog() is not synchronized on the semaphore
	 * that is used to do  buffer management to allow writes 
	 * to the free buffers when flush is in progress.  
     **/
	protected void flushDirtyBuffers() throws IOException 
    {
        LogAccessFileBuffer buf = null;
		int noOfBuffers;
		int nFlushed= 0;
		try{
			synchronized(this)
			{
				/**if some one else flushing wait, otherwise it is possible 
				 * different threads will get different buffers and order can 
				 * not be determined.
				 * 
				 **/
				while(flushInProgress)
				{
					try{
						wait();
					}catch (InterruptedException ie) 
					{
                        InterruptStatus.setInterrupted();
					}
				}
		
				noOfBuffers = dirtyBuffers.size();
				if(noOfBuffers > 0)
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
					buf = dirtyBuffers.removeFirst();
				
				flushInProgress = true;
			}
			
			while(nFlushed < noOfBuffers)
			{
//IC see: https://issues.apache.org/jira/browse/DERBY-2977
				if (buf.position != 0) {
					writeToLog(buf.buffer, 0, buf.position, buf.greatest_instant);
				}

				nFlushed++;
				synchronized(this)
				{
					//add the buffer that was written previosly to the free list
					freeBuffers.addLast(buf);
					if(nFlushed < noOfBuffers)
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
						buf = dirtyBuffers.removeFirst();
					else
					{
						//see if we can flush more, that came when we are at it.
						//don't flush more than the total number of buffers,
						//that might lead to starvation of the current thread.
						int size = dirtyBuffers.size();
						if(size > 0 && nFlushed <= LOG_NUMBER_LOG_BUFFERS)
						{
							noOfBuffers += size;
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
							buf = dirtyBuffers.removeFirst();
						}
					}
				}
			}

				
		}finally{
			synchronized(this)
			{
				flushInProgress = false;
				notifyAll();
			}
		}
	}


	//flush all the the dirty buffers to disk
	public void flushLogAccessFile() throws IOException,  StandardException 
	{
		switchLogBuffer();
		flushDirtyBuffers();
	}

		
	/**
	 * Appends the current Buffer to the dirty Buffer list and assigns a free
	 * buffer to be the currrent active buffer . Flushing of the buffer
	 * to disk is delayed if there is a free buffer available. 
	 * dirty buffers will be  flushed to the disk   
	 * when  flushDirtyBuffers() is invoked by  a commit call 
	 * or when no more free buffers are available. 
	 */
	public void switchLogBuffer() throws IOException, StandardException  
    {

		synchronized(this)
		{
			// ignore empty buffer switch requests
//IC see: https://issues.apache.org/jira/browse/DERBY-96
			if(currentBuffer.position == checksumLogRecordSize)
				return;

			// calculate the checksum for the current log buffer 
			// and write the record to the space reserverd in 
			// the beginning of the buffer. 
//IC see: https://issues.apache.org/jira/browse/DERBY-2939
			if(writeChecksum)
			{
				checksumLogOperation.reset();
				checksumLogOperation.update(currentBuffer.buffer, checksumLogRecordSize, currentBuffer.position - checksumLogRecordSize);
				writeChecksumLogRecord(currentBuffer.buffer);
			}

			//add the current buffer to the flush buffer list
			dirtyBuffers.addLast(currentBuffer);

			//if there is No free buffer, flush the buffers to get a free one 
			if(freeBuffers.size() == 0) 
			{
				flushDirtyBuffers();
				//after the flush call there should be a free buffer
				//because this is only methods removes items from 
				//free buffers and removal is in synchronized block. 
			}


			// there should be free buffer available at this point.
			if (SanityManager.DEBUG)
				SanityManager.ASSERT(freeBuffers.size() > 0);

			//switch over to the next log buffer, let someone else write it.
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
			currentBuffer = freeBuffers.removeFirst();
			currentBuffer.init(checksumLogRecordSize);
//IC see: https://issues.apache.org/jira/browse/DERBY-96

			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(currentBuffer.position == checksumLogRecordSize);
				SanityManager.ASSERT(
									 currentBuffer.bytes_free == currentBuffer.length);
                SanityManager.ASSERT(currentBuffer.bytes_free > 0);
			}
		}
	}


    /**
     * Guarantee all writes up to the last call to flushLogAccessFile on disk.
     * <p>
     * A call for clients of LogAccessFile to insure that all data written
     * up to the last call to flushLogAccessFile() are written to disk.
     * This call will not return until those writes have hit disk.
     * <p>
     * Note that this routine may block waiting for I/O to complete so 
     * callers should limit the number of resource held locked while this
     * operation is called.  It is expected that the caller
     * Note that this routine only "writes" the data to the file, this does not
     * mean that the data has been synced to disk.  The only way to insure that
     * is to first call switchLogBuffer() and then follow by a call of sync().
     *
     **/
    public void syncLogAccessFile() 
        throws IOException, StandardException
    {
        for( int i=0; ; )
        {
            // 3311: JVM sync call sometimes fails under high load against NFS 
            // mounted disk.  We re-try to do this 20 times.
            try
            {
                synchronized( this)
                {
//IC see: https://issues.apache.org/jira/browse/DERBY-4963
                    log.sync();
                }

                // the sync succeed, so return
                break;
            }
            catch( SyncFailedException sfe )
            {
                i++;
                try
                {
                    // wait for .2 of a second, hopefully I/O is done by now
                    // we wait a max of 4 seconds before we give up
                    Thread.sleep( 200 ); 
                }
                catch( InterruptedException ie )
                {
//IC see: https://issues.apache.org/jira/browse/DERBY-4741
//IC see: https://issues.apache.org/jira/browse/DERBY-4741
                    InterruptStatus.setInterrupted();
                }

                if( i > 20 )
                    throw StandardException.newException(
//IC see: https://issues.apache.org/jira/browse/DERBY-336
                        SQLState.LOG_FULL, sfe);
            }
        }
    }

	/**
		The database is being marked corrupted, get rid of file pointer without
		writing out anything more.
	 */
	public void corrupt() throws IOException
	{
		synchronized(logFileSemaphore)
		{
			if (log != null)
				log.close();
		}
	}

	public void close() throws IOException, StandardException
    {
		if (SanityManager.DEBUG) 
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-96
			if (currentBuffer.position !=  checksumLogRecordSize)
				SanityManager.THROWASSERT(
				"Log file being closed with data still buffered " + 
                currentBuffer.position +  " " + currentBuffer.bytes_free);
		}

		flushLogAccessFile();

		synchronized(logFileSemaphore)
		{
			if (log != null)
				log.close();
		}
	}

    /**
     * Make this LogAccessFile pass chunks of log records (byte[]) to
     * the MasterFactory when the chunks are written to disk.
     * @param masterFac The MasterFactory service responsible for
     * controlling the master side replication behaviour.
     */
    protected void setReplicationMasterRole(MasterFactory masterFac) {
//IC see: https://issues.apache.org/jira/browse/DERBY-2977
        this.masterFac = masterFac;
        inReplicationMasterMode = true;
    }

    /**
     * Stop this LogAccessFile from passing chunks of log records to
     * the MasterFactory.
     */
    protected void stopReplicationMasterRole() {
        inReplicationMasterMode = false;
        masterFac = null;
    }

    /**
     * Method to put this LogAccessFile object in replication slave
     * mode, effectively disabling checksum writes.
     *
     * Because checksums are received from the replication master, the
     * slave can not be allowed to add it's own checksums - that would
     * invalidate the checksums and would stop the database from
     * recovering. Replication slave mode must therefore be set before
     * LogAccessFile decides whether to write it's own checksums, and
     * this method is therefore indirectly called from the constructor
     * of this class by calling LogFactory.checkForReplication
     *
     * If replication slave mode for the database is stopped after
     * this object has been created, checksums cannot be reenabled
     * without creating a new instance of this class. That is
     * conveniently handled as LogToFile.recover completes (which
     * automatically happens once replication slave mode is no longer
     * active)
     *
     * @see LogToFile#checkForReplication
     */
    protected void setReplicationSlaveRole() {
        inReplicationSlaveMode = true;
    }

	/* write to the log file */
	private void writeToLog(byte b[], int off, int len, long highestInstant)
		throws IOException
	{
		synchronized(logFileSemaphore)
		{
            if (log != null)
            {

                // Try to handle case where user application is throwing
                // random interrupts at Derby threads, retry in the case
                // of IO exceptions 5 times.  After that hope that it is 
                // a real disk problem - an IO error in a write to the log file
                // is going to take down the whole system, so seems worthwhile
                // to retry.
                for (int i = 0; ;i++)
                {
                    try 
                    {
                        log.write(b, off, len);
//IC see: https://issues.apache.org/jira/browse/DERBY-2977
                        if (inReplicationMasterMode) {
                            masterFac.appendLog(highestInstant,
                                                b, off, len);
                        }
                        break;
                    }
                    catch (IOException ioe)
                    {
                        // just fall through and rety the log write 1st 5 times.

                        if (i >= 5)
                            throw ioe;
                    }
                }
            }
		}

		if (SanityManager.DEBUG) 
        {
			mon_numWritesToLog++;
			mon_numBytesToLog += len;
		}
	}

	/**
	 * reserve the space for the checksum log record in the log file. 
     *
	 * @param  length           the length of the log record to be written
	 * @param  logFileNumber    current log file number 
	 * @param  currentPosition  current position in the log file. 
     *
	 * @return the space that is needed to write a checksum log record.
	 */
	protected long reserveSpaceForChecksum(int length, long logFileNumber, long currentPosition )
//IC see: https://issues.apache.org/jira/browse/DERBY-96
		throws StandardException, IOException 
	{

		int total_log_record_length = length + LOG_RECORD_FIXED_OVERHEAD_SIZE;
		boolean reserveChecksumSpace = false;
		
		/* checksum log record is calculated for a group of log 
		 * records that can fit in to a single buffer or for 
		 * a single record when it does not fit into 
		 * a fit into a buffer at all. When a new buffer 
		 * is required to write a log record, log space 
		 * has to be reserved before writing the log record
		 * becuase checksum is written in the before the 
		 * log records that are being checksummed. 
		 * What it also means is a real log instant has to be 
		 * reserved for writing the checksum log record in addition 
		 * to the log buffer space.
		 */
		

		/* reserve checkum space for new log records if a log buffer switch had
		 * happened before because of a explicit log flush requests(like commit)
		 * or a long record write 
		 */
		if(currentBuffer.position == checksumLogRecordSize)
		{
			// reserver space if log checksum feature is enabled.
			reserveChecksumSpace = writeChecksum;
		}
		else{
			if (total_log_record_length > currentBuffer.bytes_free)
			{
				// the log record that is going to be written is not 
				// going to fit in the current buffer, switch the 
				// log buffer to create buffer space for it. 
				switchLogBuffer();
				// reserve space if log checksum feature is enabled. 
				reserveChecksumSpace = writeChecksum;
			}
		}
		
		if(reserveChecksumSpace)
		{
			if (SanityManager.DEBUG)
			{
				// Prevoiusly reserved real checksum instant should have been
				// used, before an another one is generated. 
				SanityManager.ASSERT(checksumInstant == -1,  "CHECKSUM INSTANT IS GETTING OVER WRITTEN");
			}
			
			checksumInstant = LogCounter.makeLogInstantAsLong(logFileNumber, currentPosition);
			return  checksumLogRecordSize;
		}else
		{
			return 0 ;
		}
	}


	/**
	 * Generate the checkum log record and write it into the log
	 * buffer. The checksum applies to all bytes from this checksum
	 * log record to the next one. 
     * @param buffer The byte[] the checksum is written to. The
     * checksum is always written at the beginning of buffer.
	 */
	private void writeChecksumLogRecord(byte[] buffer)
//IC see: https://issues.apache.org/jira/browse/DERBY-2939
		throws IOException, StandardException{
		
		int    p    = 0; //checksum is written in the beginning of the buffer

		// writeInt(length)
		p = writeInt(checksumLength, buffer, p);
            
		// writeLong(instant)
		p = writeLong(checksumInstant, buffer, p);

		//write the checksum log operation  
		logOutputBuffer.setData(buffer);
		logOutputBuffer.setPosition(p);
		logicalOut.writeObject(checksumLogRecord);

		if(databaseEncrypted)
		{
			//encrypt the checksum log operation part.
			int len = 
//IC see: https://issues.apache.org/jira/browse/DERBY-2939
				logFactory.encrypt(buffer, LOG_RECORD_HEADER_SIZE, checksumLength, 
								   buffer, LOG_RECORD_HEADER_SIZE);
			
		   
			if (SanityManager.DEBUG)
				SanityManager.ASSERT(len == checksumLength, 
									 "encrypted log buffer length != log buffer len");
		}

		p = LOG_RECORD_HEADER_SIZE + checksumLength ;

		// writeInt(length) trailing
//IC see: https://issues.apache.org/jira/browse/DERBY-2939
		p = writeInt(checksumLength, buffer, p );
		
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(p == checksumLogRecordSize, "position=" + p  + "ckrecordsize=" + checksumLogRecordSize);
			if (SanityManager.DEBUG_ON(LogToFile.DBG_FLAG))
			{
				SanityManager.DEBUG(
									LogToFile.DBG_FLAG, 
									"Write log record: tranId=Null"  +
									" instant: " + LogCounter.toDebugString(checksumInstant) + " length: " +
									checksumLength + "\n" + checksumLogOperation + "\n");
			}
			checksumInstant = -1; 
		}

	}

    /** Return the length of a checksum record */
    public  int getChecksumLogRecordSize() { return checksumLogRecordSize; }


	protected void writeEndMarker(int marker) throws IOException, StandardException 
	{
		//flush all the buffers and then write the end marker.
		flushLogAccessFile();
		
		byte[] b    = currentBuffer.buffer;
		int    p    = 0; //end is written in the beginning of the buffer, no
						 //need to checksum a int write.
		p = writeInt(marker , b , p);
//IC see: https://issues.apache.org/jira/browse/DERBY-2977
		writeToLog(b, 0, p, -1); //end marker has no instant
	}

	
}








