/*

   Derby - Class org.apache.derby.impl.services.replication.buffer.ReplicationLogBuffer

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

package org.apache.derby.impl.services.replication.buffer;

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.LinkedList;

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
 * elements that contains dirty log are in dirtyBuffers. Log records
 * are appended to the buffer element in currentDirtyBuffer. Hence,
 * the life cycle of buffer elements is:
 * freeBuffers -> currentDirtyBuffer -> dirtyBuffers -> freeBuffers
 *
 * To append log records to the buffer, use appendLogRecord(...)
 *
 * To consume chunks of log records, use next() followed by getData(),
 * getLastInstant() and getSize(). These get-methods throw
 * NoSuchElementException if next() returned false, meaning that there
 * were no dirty log at the time next() was called.
 *
 * Threads: ReplicationLogBuffer is threadsafe. It can be used by a
 * logger (LogToFile) and a log consumer (LogShipping service)
 * concurrently without further synchronization.
 */

public class ReplicationLogBuffer {

    private static final int DEFAULT_NUMBER_LOG_BUFFERS = 10;

    protected static final int LOG_RECORD_FIXED_OVERHEAD_SIZE = 24;
    // long instant           - 8
    // int dataLength         - 4
    // int dataOffset         - 4
    // int optionalDataLength - 4
    // int optionalDataOffset - 4

    private final LinkedList dirtyBuffers;// LogBufferElements with unsent log
    private final LinkedList freeBuffers; // currently unused LogBufferElements

    // the buffer we currently APPEND log records to
    private LogBufferElement currentDirtyBuffer;

    // used to GET data from this buffer. next() sets these
    private boolean validOutBuffer; // outBuffer contains valid data
    private byte[] outBufferData;   // the buffer contents
    private int outBufferStored;    // number of bytes currently stored
    private long outBufferLastInstant;// highest instant (LSN) in outBufferData

    // Two objects to synchronize on so that the logger (LogToFile)
    // and the log consumer (LogShipping service) can use the buffer
    // concurrently (although appendLogRecord may conflict with next).
    // In cases where both latches are needed at the same time,
    // listLatch is always set first to avoid deadlock. listLatch is
    // used by appendLogRecord and next to synchronize operations on
    // the free and dirty buffer lists and on currentDirtyBuffer.
    // outputLatch is used by next and getXXX to synchronize on the
    // output data variables
    private final Object listLatch = new Object();
    private final Object outputLatch = new Object();

    private int defaultBufferSize;

    public ReplicationLogBuffer(int bufferSize) {
        defaultBufferSize = bufferSize;

        outBufferData = new byte[bufferSize];
        outBufferStored = 0;
        outBufferLastInstant = 0;
        validOutBuffer = false; // no valid data in outBuffer yet

        dirtyBuffers = new LinkedList();
        freeBuffers = new LinkedList();

        for (int i = 0; i < DEFAULT_NUMBER_LOG_BUFFERS; i++){
            LogBufferElement b = new LogBufferElement(bufferSize);
            freeBuffers.addLast(b);
        }
        currentDirtyBuffer = (LogBufferElement)freeBuffers.removeFirst();
    }

    /**
     * Append a single log record to the log buffer.
     *
     * @param instant               the log address of this log record.
     * @param dataLength            number of bytes in data[]
     * @param dataOffset            offset in data[] to start copying from.
     * @param optionalDataLength    number of bytes in optionalData[]
     * @param optionalDataOffset    offset in optionalData[] to start copy from
     * @param data                  "from" array to copy "data" portion of rec
     * @param optionalData          "from" array to copy "optional data" from
     *
     * @throws LogBufferFullException - thrown if there is not enough
     * free space in the buffer to store the log record.
     **/
    public void appendLogRecord(long instant,
                                int dataLength,
                                int dataOffset,
                                int optionalDataLength,
                                int optionalDataOffset,
                                byte[] data,
                                byte[] optionalData)
        throws LogBufferFullException{

        /* format of log to write:
         *
         * (long)   instant
         * (int)    dataLength
         * (int)    dataOffset
         * (int)    optionalDataLength
         * (int)    optionalDataOffset
         * (byte[]) data
         * (byte[]) optionalData
         */

        int totalLength = dataLength + optionalDataLength +
                          LOG_RECORD_FIXED_OVERHEAD_SIZE;

        synchronized (listLatch) {
            if (currentDirtyBuffer == null) {
                switchDirtyBuffer();
                // either sets the currentDirtyBuffer to a buffer
                // element or throws a LogBufferFullException
            }

            // switch buffer if current buffer does not have enough space
            // for the incoming data
            if (totalLength > currentDirtyBuffer.freeSize()) {
                switchDirtyBuffer();
            }

            if (totalLength <= currentDirtyBuffer.freeSize()) {
                currentDirtyBuffer.appendLogRecord(instant,
                                                   dataLength,
                                                   dataOffset,
                                                   optionalDataLength,
                                                   optionalDataOffset,
                                                   data,
                                                   optionalData);
            } else {
                // The log record requires more space than one
                // LogBufferElement with default size. Create a new big
                // enough LogBufferElement
                LogBufferElement current = new LogBufferElement(totalLength);
                current.setRecyclable(false);
                current.appendLogRecord(instant,
                                        dataLength,
                                        dataOffset,
                                        optionalDataLength,
                                        optionalDataOffset,
                                        data,
                                        optionalData);
                dirtyBuffers.addLast(current);
                // currentDirtyBuffer has already been handed over to
                // the dirtyBuffers list, and an empty one is in
                // place, so no need to touch currentDirtyBuffer here
            }
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
                        (LogBufferElement)dirtyBuffers.removeFirst();

                    // The outBufferData byte[] should have the
                    // default size or the size of the current
                    // LogBufferElement if that is bigger than the
                    // default size.
                    int requiredOutBufferSize = Math.max(defaultBufferSize,
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
                    // used to store a single very big log record
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
            } else
                throw new NoSuchElementException();
        }
    }

    /**
     * Method to determine whether or not the buffer had log record
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
     * factory.
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
                    (LogBufferElement)freeBuffers.removeFirst();
                currentDirtyBuffer.init();
            } catch (NoSuchElementException nsee) {
                throw new LogBufferFullException();
            }
        }
    }

}
