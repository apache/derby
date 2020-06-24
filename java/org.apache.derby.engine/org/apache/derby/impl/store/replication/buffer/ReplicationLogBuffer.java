/*

   Derby - Class org.apache.derby.impl.store.replication.buffer.ReplicationLogBuffer

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

package org.apache.derby.impl.store.replication.buffer;

import org.apache.derby.shared.common.sanity.SanityManager;

import java.util.NoSuchElementException;
import java.util.LinkedList;

import org.apache.derby.iapi.store.replication.master.MasterFactory;

/**
 * Used for the replication master role only. When a Derby instance
 * has the replication master role for a database 'x', all log records
 * that are written to the local log file are also appended to this
 * log buffer. The replication master service will consume chunks of
 * log from this buffer and send it to the Derby instance with the
 * slave role for 'x'.
 *
 * ReplicationLogBuffer consists of a number of LogBufferElements.
 * Elements that are not in use are in the freeBuffers list, while
 * elements that contains dirty log are in dirtyBuffers. Chunks of log records
 * are appended to the buffer element in currentDirtyBuffer. Hence,
 * the life cycle of buffer elements is:
 * freeBuffers -&gt; currentDirtyBuffer -&gt; dirtyBuffers -&gt; freeBuffers
 *
 * To append chunks of log records to the buffer, use appendLog(...)
 *
 * To consume chunks of log records, use next() followed by getData(),
 * getLastInstant() and getSize(). These get-methods throw
 * NoSuchElementException if next() returned false, meaning that there
 * were no dirty log at the time next() was called.
 *
 * Threads: ReplicationLogBuffer is threadsafe. It can be used by a
 * logger (LogToFile) and a log consumer (LogShipping service)
 * concurrently without further synchronization.
 * 
 * Important: If methods in this class calls methods outside this package
 * (e.g. MasterFactory#workToDo), make sure that deadlocks are not 
 * introduced. If possible, a call to any method in another package should be 
 * done without holding latches in this class.
 */

public class ReplicationLogBuffer {

    public static final int DEFAULT_NUMBER_LOG_BUFFERS = 10;

    private final LinkedList<LogBufferElement> dirtyBuffers;// LogBufferElements with unsent log
    private final LinkedList<LogBufferElement> freeBuffers; // currently unused LogBufferElements

    // the buffer we currently APPEND chunks of log records to
    private LogBufferElement currentDirtyBuffer;

    // used to GET data from this buffer. next() sets these
    private boolean validOutBuffer; // outBuffer contains valid data
    private byte[] outBufferData;   // the buffer contents
    private int outBufferStored;    // number of bytes currently stored
    private long outBufferLastInstant;// highest instant (LSN) in outBufferData

    // Two objects to synchronize on so that the logger (LogToFile)
    // and the log consumer (LogShipping service) can use the buffer
    // concurrently (although appendLog may conflict with next).
    // In cases where both latches are needed at the same time,
    // listLatch is always set first to avoid deadlock. listLatch is
    // used by appendLog and next to synchronize operations on
    // the free and dirty buffer lists and on currentDirtyBuffer.
    // outputLatch is used by next and getXXX to synchronize on the
    // output data variables
    private final Object listLatch = new Object();
    private final Object outputLatch = new Object();

    private int defaultBufferSize;

    // used to notify the master controller that a log buffer element is full 
    // and work needs to be done.
    private final MasterFactory mf;

    /**
     * Class constructor specifies the number of buffer elements
     * and the master controller that creates this replication log
     * buffer.
     *
     * @param bufferSize the default number of buffer elements
     * @param mf         Used to notify the master controller that a log buffer
     *                   element is full and work needs to be done.
     */
    public ReplicationLogBuffer(int bufferSize, MasterFactory mf) {
        defaultBufferSize = bufferSize;
        
        this.mf = mf;
//IC see: https://issues.apache.org/jira/browse/DERBY-3359

        outBufferData = new byte[bufferSize];
        outBufferStored = 0;
        outBufferLastInstant = 0;
        validOutBuffer = false; // no valid data in outBuffer yet

//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        dirtyBuffers = new LinkedList<LogBufferElement>();
        freeBuffers = new LinkedList<LogBufferElement>();

        for (int i = 0; i < DEFAULT_NUMBER_LOG_BUFFERS; i++){
            LogBufferElement b = new LogBufferElement(bufferSize);
            freeBuffers.addLast(b);
        }
        currentDirtyBuffer = freeBuffers.removeFirst();
    }

    /**
     * Append a chunk of log records to the log buffer.
     *
     * @param greatestInstant   the instant of the log record that was
     *                          added last to this chunk of log
     * @param log               the chunk of log records
     * @param logOffset         offset in log to start copy from
     * @param logLength         number of bytes to copy, starting
     *                          from logOffset
     *
     * @throws LogBufferFullException - thrown if there is not enough
     * free space in the buffer to store the chunk of log.
     **/
    public void appendLog(long greatestInstant,
//IC see: https://issues.apache.org/jira/browse/DERBY-2977
                          byte[] log, int logOffset, int logLength)
        throws LogBufferFullException{

        boolean switchedBuffer = false; 
        synchronized (listLatch) {
            if (currentDirtyBuffer == null) {
                switchDirtyBuffer();
                // either sets the currentDirtyBuffer to a buffer
                // element or throws a LogBufferFullException. No need to call
                // MasterFactory.workToDo becase switchDirtyBuffer will not add
                // a buffer to the dirty buffer list when currentDirtyBuffer 
                // is null
            }

            // switch buffer if current buffer does not have enough space
            // for the incoming data
//IC see: https://issues.apache.org/jira/browse/DERBY-2977
            if (logLength > currentDirtyBuffer.freeSize()) {
                switchDirtyBuffer();
                switchedBuffer = true;
            }

            if (logLength <= currentDirtyBuffer.freeSize()) {
                currentDirtyBuffer.appendLog(greatestInstant,
                                             log, logOffset, logLength);
            } else {
                // The chunk of log records requires more space than one
                // LogBufferElement with default size. Create a new big
                // enough LogBufferElement
                LogBufferElement current = new LogBufferElement(logLength);
                current.setRecyclable(false);
                current.appendLog(greatestInstant, log, logOffset, logLength);
                dirtyBuffers.addLast(current);
                // currentDirtyBuffer has already been handed over to
                // the dirtyBuffers list, and an empty one is in
                // place, so no need to touch currentDirtyBuffer here
            }
        }
        // DERBY-3472 - we need to release the listLatch before calling workToDo
        // to avoid deadlock with the logShipper thread
        if (switchedBuffer) {
            // Notify the master controller that a log buffer element is full 
            // and work needs to be done.
            mf.workToDo();
        }
    }

    /**
     * Sets the output data to that of the next (oldest) buffer
     * element in dirtyBuffers so that getData(), getLastInstant() and
     * getSize() return values from the next oldest chunk of log. Used
     * by the log consumer (the LogShipping service) to move to the
     * next chunk of log in the buffer.
     *
     * @return true if there is log in the buffer, resulting in valid
     * data for the get-methods
     */
    public boolean next() {
        synchronized (listLatch) {

            if (dirtyBuffers.size() == 0) {
                // if the current buffer has been written to, and
                // there are no other dirty buffers, it should be
                // moved to the dirtyBuffer list so that it can be
                // returned.
                try {
                    switchDirtyBuffer();
                    // No need to call MasterFactory.workToDo because the 
                    // caller of next() will perform the work required on the 
                    // buffer that was just moved to the dirty buffer list.
                } catch (LogBufferFullException lbfe) {
                    // should not be possible when dirtyBuffers.size() == 0
                    if (SanityManager.DEBUG){
                        SanityManager.THROWASSERT(
                            "Unexpected LogBufferFullException when trying "+
                            "to remove elements from the buffer", lbfe);
                    }
                }
            }

            synchronized (outputLatch) {
                if (dirtyBuffers.size() > 0 ) {
                    LogBufferElement current =
                        dirtyBuffers.removeFirst();
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

                    // The outBufferData byte[] should have the
                    // default size or the size of the current
                    // LogBufferElement if that is bigger than the
                    // default size.
                    int requiredOutBufferSize = Math.max(defaultBufferSize,
//IC see: https://issues.apache.org/jira/browse/DERBY-2926
                                                         current.size());
                    if (outBufferData.length != requiredOutBufferSize) {
                        // The current buffer has a different size
                        // than what we need it to be, so we resize.
                        outBufferData = new byte[requiredOutBufferSize];
                    }

                    // set the outBuffer data
                    System.arraycopy(current.getData(), 0, outBufferData, 0,
                                     current.size());
                    outBufferStored = current.size();
                    outBufferLastInstant = current.getLastInstant();

                    // recycle = false if the LogBufferElement has been
                    // used to store a very big chunk of log records
                    if (current.isRecyclable()) {
                        freeBuffers.addLast(current);
                    }

                    validOutBuffer = true;
                } else {
                    // No more dirty data to get
                    validOutBuffer = false;
                }
            }
        }

        return validOutBuffer;
    }

    /**
     * Returns a byte[] containing a chunk of serialized log records.
     * Always returns the log that was oldest at the time next() was
     * called last time. Use next() to move to the next chunk of log
     * records.
     *
     * @return A copy of the current byte[], which is a chunk of log
     * @throws NoSuchElementException if there was no log in the
     * buffer the last time next() was called.
     */
    public byte[] getData() throws NoSuchElementException{
        synchronized (outputLatch) {
            byte [] b = new byte[getSize()];
            if (validOutBuffer) {
                System.arraycopy(outBufferData, 0, b, 0, getSize());
                return b;
            } else {
                throw new NoSuchElementException();
            }
        }
    }

    /**
     * Method to determine whether or not the buffer had any log records
     * the last time next() was called.
     *
     * @return true if the buffer contained log records the last time
     * next() was called. False if not, or if next() has not been
     * called yet.
     */
    public boolean validData() {
        synchronized (outputLatch) {
            return validOutBuffer;
        }
    }

    /**
     * @return The number of bytes returned by getData
     * @throws NoSuchElementException if there was no log in the
     * buffer the last time next() was called.
     */
    public int getSize() throws NoSuchElementException{
        synchronized (outputLatch) {
            if (validOutBuffer) {
                return outBufferStored;
            } else {
                throw new NoSuchElementException();
            }
        }
    }

    /**
     * Can be used so that only the necessary log records are sent
     * when a flush(LogInstant flush_to_this) is called in the log
     * factory. Returns the highest log instant in the chunk of log that can 
     * be read with getData().
     *
     * @return The highest log instant in the chunk of log returned by
     * getData().
     * @throws NoSuchElementException if there was no log in the
     * buffer the last time next() was called.
     */
    public long getLastInstant() throws NoSuchElementException{
        synchronized (outputLatch) {
            if (validOutBuffer) {
                return outBufferLastInstant;
//IC see: https://issues.apache.org/jira/browse/DERBY-3359
            } else {
                throw new NoSuchElementException();
            }
        }
    }

    /**
     * Appends the currentDirtyBuffer to dirtyBuffers, and makes a
     * fresh buffer element from freeBuffers the currentDirtyBuffer.
     * Note: this method is not synchronized since all uses of it is
     * inside synchronized(listLatch) code blocks.
     *
     * @throws LogBufferFullException if the freeBuffers list is empty
     */
    private void switchDirtyBuffer() throws LogBufferFullException{

        // first, move currentDirtyBuffer to dirtyBuffers list.
        // do not switch if current buffer is empty
        if (currentDirtyBuffer != null && currentDirtyBuffer.size() > 0) {
            dirtyBuffers.addLast(currentDirtyBuffer);
            currentDirtyBuffer = null;
        }

        // second, make a buffer element from the freeBuffers list the
        // new currentDirtyBuffer. If currentDirtyBuffer != null, it
        // is empty and has therefore not been moved to dirtyBuffers
        if (currentDirtyBuffer == null) {
            try {
                currentDirtyBuffer =
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
                    freeBuffers.removeFirst();
                currentDirtyBuffer.init();
            } catch (NoSuchElementException nsee) {
                throw new LogBufferFullException();
            }
        }
    }

    /**
     * Used to calculate the Fill Information. The fill information
     * is a indicator of how full the buffer is at any point of time
     * fill information = (full buffers/Total Buffers)*100. The Fill
     * information ranges between 0-100 (both 0 and 100 inclusive).
     *
     * @return an integer value between 0-100 representing the fill
     *         information.
     */
    public int getFillInformation() {
//IC see: https://issues.apache.org/jira/browse/DERBY-3359
        return ((dirtyBuffers.size()*100)/DEFAULT_NUMBER_LOG_BUFFERS);
    }

}
