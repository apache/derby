/*

   Derby - Class org.apache.derby.impl.store.raw.log.LogAccessFile

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

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.io.StorageRandomAccessFile;

import java.io.IOException;
import java.io.OutputStream;
import java.io.SyncFailedException;
import java.io.InterruptedIOException;
import java.util.LinkedList;


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
	freeBuffers --> dirtyBuffers --> freeBuffers. Movement of buffers from one
    stage to 	another stage is synchronized using	the object(this) of this class. 

*/
public class LogAccessFile extends OutputStream 
{

    /**
     * The fixed size of a log record is 16 bytes:
     *     int   length             : 4 bytes
     *     long  instant            : 8 bytes
     *     int   trailing length    : 4 bytes
     **/
    private static final int            LOG_RECORD_FIXED_OVERHEAD_SIZE = 16;

    private static final int            LOG_NUMBER_LOG_BUFFERS = 3;


	private LinkedList    freeBuffers;  //list of free buffers
	private LinkedList    dirtyBuffers; //list of dirty buffers to flush
	private  LogAccessFileBuffer currentBuffer; //current active buffer
	private boolean flushInProgress = false;

	private final StorageRandomAccessFile  log;

	// log can be touched only inside synchronized block protected by
	// logFileSemaphore.
	private final Object            logFileSemaphore;

	static int                      mon_numWritesToLog;
	static int                      mon_numBytesToLog;

	public LogAccessFile(
    StorageRandomAccessFile    log, 
    int                 bufferSize) throws IOException 
    {
		if (SanityManager.DEBUG)
		{
			if(SanityManager.DEBUG_ON("LogBufferOff"))
				bufferSize = 10;	// make it very tiny
		}
		
		this.log            = log;
		logFileSemaphore    = log;

		if (SanityManager.DEBUG)
            SanityManager.ASSERT(LOG_NUMBER_LOG_BUFFERS >= 1);
				
		//initialize buffers lists
		freeBuffers = new LinkedList();
		dirtyBuffers = new LinkedList();


		//add all buffers to free list
        for (int i = 0; i < LOG_NUMBER_LOG_BUFFERS; i++)
        {
            LogAccessFileBuffer b = new LogAccessFileBuffer(bufferSize);
            freeBuffers.addLast(b);
        }

		currentBuffer = (LogAccessFileBuffer) freeBuffers.removeFirst();

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

        if (total_log_record_length > currentBuffer.bytes_free && 
            total_log_record_length <= currentBuffer.buffer.length) 
        {
            // If the whole record will fit in an empty buffer, flush this
            // one now and put this record into the next one.
            switchLogBuffer();
        }

		if (total_log_record_length <= currentBuffer.bytes_free)
        {
            byte[] b    = currentBuffer.buffer;
            int    p    = currentBuffer.position;

            // writeInt(length)
            b[p++] = (byte) ((length >>> 24) & 0xff); 
            b[p++] = (byte) ((length >>> 16) & 0xff); 
            b[p++] = (byte) ((length >>>  8) & 0xff); 
            b[p++] = (byte) ((length       ) & 0xff);
            
            // writeLong(instant)
            b[p++] = (byte) (((int)(instant >>> 56)) & 0xff); 
            b[p++] = (byte) (((int)(instant >>> 48)) & 0xff); 
            b[p++] = (byte) (((int)(instant >>> 40)) & 0xff); 
            b[p++] = (byte) (((int)(instant >>> 32)) & 0xff); 
            b[p++] = (byte) (((int)(instant >>> 24)) & 0xff); 
            b[p++] = (byte) (((int)(instant >>> 16)) & 0xff); 
            b[p++] = (byte) (((int)(instant >>>  8)) & 0xff); 
            b[p++] = (byte) (((int)(instant       )) & 0xff); 

            // write(data, data_offset, length - optional_data_length)
            int transfer_length = (length - optional_data_length);
			System.arraycopy(data, data_offset, b, p, transfer_length);

            p += transfer_length;

            if (optional_data_length != 0)
            {
                // write(
                //   optional_data, optional_data_offset, optional_data_length);

                System.arraycopy(
                    optional_data, optional_data_offset, 
                    b,             p, 
                    optional_data_length);

                p += optional_data_length;
            }

            // writeInt(length)
            b[p++] = (byte) ((length >>> 24) & 0xff); 
            b[p++] = (byte) ((length >>> 16) & 0xff); 
            b[p++] = (byte) ((length >>>  8) & 0xff); 
            b[p++] = (byte) ((length       ) & 0xff);

            currentBuffer.position   = p;
            currentBuffer.bytes_free -= total_log_record_length;
		}
        else
        {
            writeInt(length);
            writeLong(instant);
            write(data, data_offset, length - optional_data_length);
            if (optional_data_length != 0)
            {
                write(
                    optional_data, optional_data_offset, optional_data_length);
            }
            writeInt(length);
		}
    }


	public void writeInt(int i) throws IOException 
    {
		if (currentBuffer.bytes_free < 4)
			switchLogBuffer();

		byte[] b = currentBuffer.buffer;
		int p = currentBuffer.position;

        b[p++] = (byte) ((i >>> 24) & 0xff); 
        b[p++] = (byte) ((i >>> 16) & 0xff); 
        b[p++] = (byte) ((i >>> 8) & 0xff); 
        b[p++] = (byte) (i & 0xff);

		currentBuffer.position = p;
		currentBuffer.bytes_free -= 4;
	}

	public void writeLong(long l) 
        throws IOException 
    {
		if (currentBuffer.bytes_free < 8)
			switchLogBuffer();

		byte[] b = currentBuffer.buffer;
 		int p = currentBuffer.position;
        b[p++] = (byte) (((int)(l >>> 56)) & 0xff); 
        b[p++] = (byte) (((int)(l >>> 48)) & 0xff); 
        b[p++] = (byte) (((int)(l >>> 40)) & 0xff); 
        b[p++] = (byte) (((int)(l >>> 32)) & 0xff); 
        b[p++] = (byte) (((int)(l >>> 24)) & 0xff); 
        b[p++] = (byte) (((int)(l >>> 16)) & 0xff); 
        b[p++] = (byte) (((int)(l >>> 8)) & 0xff); 
        b[p++] = (byte) (((int)l) & 0xff); 
		currentBuffer.position = p;
		currentBuffer.bytes_free -= 8;
    }

	public void write(int b) 
        throws IOException 
    {

		if (currentBuffer.bytes_free == 0)
			switchLogBuffer();

		currentBuffer.buffer[currentBuffer.position++] = (byte) b;
		currentBuffer.bytes_free--;
	}


	public void write(byte b[], int off, int len) 
        throws IOException 
    {

		if (len <= currentBuffer.bytes_free)  
        {
			// data fits in buffer
			System.arraycopy(b, off, currentBuffer.buffer, currentBuffer.position, len);
			currentBuffer.bytes_free -= len;
			currentBuffer.position += len;
			return;
		}
        else if (len <= currentBuffer.buffer.length) 
        {
            // some data will be cached
            System.arraycopy(b, off, currentBuffer.buffer, currentBuffer.position, currentBuffer.bytes_free);
            len -= currentBuffer.bytes_free;
            off += currentBuffer.bytes_free;
            currentBuffer.position += currentBuffer.bytes_free;
            currentBuffer.bytes_free = 0;
            switchLogBuffer();

            System.arraycopy(b, off, currentBuffer.buffer, 0, len);
            currentBuffer.position = len;
            currentBuffer.bytes_free -= len;	
        }
        else
        {
			
			//data will never fit in currentBuffer.buffer, write directly to log
			//flush all buffers before wrting directly to the log file. 
			flushLogAccessFile();

			//Note:No Special Synchronization required here , 
			//There will be nothing to write by flushDirtyBuffers that can run
			//in parallel to the threads that is executing this code. Above
			//flush call should have written all the buffers and NO new log will 
			//get added until the following direct log to file call finishes. 

			writeToLog(b, off, len);
			return;
		}
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
						//do nothing, let the flush request to complete.
						//because it possible that other thread which is
						//currently might have completed this request also ,
						//if exited  on interrupt and throw exception, can not
						//be sure whether this transaction is COMMITTED ot not.
					}
				}
		
				noOfBuffers = dirtyBuffers.size();
				if(noOfBuffers > 0)
					buf = (LogAccessFileBuffer) dirtyBuffers.removeFirst();
				
				flushInProgress = true;
			}
			
			while(nFlushed < noOfBuffers)
			{
				if (buf.position != 0)
					writeToLog(buf.buffer, 0, buf.position);

				nFlushed++;
				synchronized(this)
				{
					//add the buffer that was written previosly to the free list
					freeBuffers.addLast(buf);
					if(nFlushed < noOfBuffers)
						buf = (LogAccessFileBuffer) dirtyBuffers.removeFirst();
					else
					{
						//see if we can flush more, that came when we are at it.
						//don't flush more than the total number of buffers,
						//that might lead to starvation of the current thread.
						int size = dirtyBuffers.size();
						if(size > 0 && nFlushed <= LOG_NUMBER_LOG_BUFFERS)
						{
							noOfBuffers += size;
							buf = (LogAccessFileBuffer) dirtyBuffers.removeFirst();
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
	public void flushLogAccessFile() throws IOException 
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
	public void switchLogBuffer() throws IOException  
    {

		synchronized(this)
		{

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
			currentBuffer = (LogAccessFileBuffer) freeBuffers.removeFirst();
			currentBuffer.init();
     
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(currentBuffer.position == 0);
				SanityManager.ASSERT(
									 currentBuffer.bytes_free == currentBuffer.buffer.length);
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
                    log.sync( false);
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
                {   //does not matter weather I get interrupted or not
                }

                if( i > 20 )
                    throw StandardException.newException(
                        SQLState.LOG_FULL, sfe, null);
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

	public void close() throws IOException 
    {
		if (SanityManager.DEBUG) 
        {
			if (currentBuffer.position != 0)
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


	/* write to the log file */
	private void writeToLog(byte b[], int off, int len) throws IOException
	{
		synchronized(logFileSemaphore)
		{
            if (log != null)
            {
                // Try to handle case where user application is throwing
                // random interrupts at cloudscape threads, retry in the case
                // of IO exceptions 5 times.  After that hope that it is 
                // a real disk problem - an IO error in a write to the log file
                // is going to take down the whole system, so seems worthwhile
                // to retry.
                for (int i = 0; ;i++)
                {
                    try 
                    {
                        log.write(b, off, len);
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
}








