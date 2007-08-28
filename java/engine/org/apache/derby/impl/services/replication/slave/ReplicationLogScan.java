/*

   Derby - Class org.apache.derby.impl.services.replication.slave.ReplicationLogScan

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

package org.apache.derby.impl.services.replication.slave;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.sanity.SanityManager;

import java.util.NoSuchElementException;

/**
 * <p>
 * Scan a chunk of log received from the master. The log chunk (byte[]
 * logToScan) is assumed to always contain (an unknown) number of
 * complete log records. If the last log record is incomplete,
 * something is wrong. This will raise a StandardException, and will
 * probably mean that replication has to be aborted. That decision is
 * not made here, though.
 * </p>
 * <p>
 * When a new chunk of log records is to be scanned, ReplicationLogScan
 * is initialized by calling init(...). Every time next() is called
 * after that, ReplicationLogScan reads a new log record from
 * logToScan. If next() returns true, the next log record was
 * successfully read. The information in this last read log record can
 * be retrieved by using the get-methods.
 * </p>
 * <p>
 * Threads: The class does not provide thread synchronization since it
 * is assumed that only one thread will be receiving and applying log
 * per replicated database when in slave mode. Since a
 * ReplicationLogScan object belongs to only one slave database, this
 * means that only one thread will access the object.
 * </p>
 * <p>
 * The format of the log chunk byte[] is defined in 
 * org.apache.derby.impl.services.replication.buffer.LogBufferElement
 * </p>
 * <p>
 * @see
 * org.apache.derby.impl.services.replication.buffer.LogBufferElement
 * org.apache.derby.impl.services.replication.buffer.LogBufferElement
 * </p>
 */

class ReplicationLogScan {

    private byte[] logToScan;     // the chunk of log to scan
    private int currentPosition;  // how long we have read in logToScan

    // Data for the latest log record read by next(). Use the
    // get-methods to retrieve these
    private long currentInstant;
    private int currentDataOffset;
    private int currentOptDataOffset;
    private byte[] currentData;
    private byte[] currentOptData;
    // validLogRecord = true when the above variables contain
    // meaningful data. false before next() is called for the first
    // time and after next() has reached the end of logToScan
    private boolean validLogRecord;

    protected ReplicationLogScan() { }

    /**
     * Set all variables to default values, and makes logToScan the
     * byte[] that will be scanned for log records.
     * @param logToScan A chunk of log records received from the
     * master
     */
    protected void init(byte[] logToScan) {
        this.logToScan = logToScan;

        currentPosition = 0;
        currentInstant = -1;
        currentDataOffset = -1;
        currentOptDataOffset = -1;
        currentData = null;
        currentOptData = null;
        validLogRecord = false;
    }

    /**
     * <p>
     * Read the next log record in logToScan. The information in this
     * log record can be read by using the getXXX methods.
     * </p>
     * <p>
     * Side effects: <br>
     * <br>
     * On a successful read (return true): setting currentInstant,
     * currentDataOffset, currentOptDataOffset, currentData,
     * currentOptData, validLogRecord = true. Also updates
     * currentPosition to point to the byte immediatly following the
     * log record.
     * </p>
     * <p>
     * If there are no more log records in logToScan (returns false) or
     * a problem occurs (throws StandardException): setting
     * validLogRecord = false
     * </p>
     * @return true if a log record was successfully read, false if end
     * of logToScan has been reached.
     * @throws StandardException if logToScan is found to be corrupt.
     */
    protected boolean next() throws StandardException {

        /* format of received log:
         *
         * (long)   instant
         * (int)    dataLength
         * (int)    dataOffset
         * (int)    optionalDataLength
         * (int)    optionalDataOffset
         * (byte[]) data
         * (byte[]) optionalData
         */

        if (SanityManager.DEBUG){
            SanityManager.ASSERT(logToScan.length >= currentPosition,
                                 "Outside log byte[] boundary");
        }

        if (currentPosition == logToScan.length) {
            // Last log record of this logToScan has already been
            // read, so we can simply return "false". Does not check
            // for >= because ">" would probably mean the log is
            // corrupt. If so, we want an exception to be thrown
            // instead (will be thrown by readXXX below). It should
            // not be possible for currentPosition to be greater than
            // logToScan.length if not an exception was thrown by the
            // previous next() call
            validLogRecord = false;
            return validLogRecord;
        }

        try {
            currentInstant = retrieveLong();       // (long) instant
            int currentDataLength = retrieveInt(); // (int)  dataLength
            currentDataOffset = retrieveInt();     // (int)  dataOffset
                                                   // (int)  optionalDataLength
            int currentOptDataLength = retrieveInt();
            currentOptDataOffset = retrieveInt();  // (int)  optionalDataOffset

            // (byte[]) data
            currentData = new byte[currentDataLength];
            retrieveBytes(currentData, currentDataLength);

            // (byte[]) optionalData
            currentOptData = new byte[currentOptDataLength];
            retrieveBytes(currentOptData, currentOptDataLength);

            validLogRecord = true;
        } catch(StandardException se){
            // Means that we have tried to read higher byte addresses
            // than the size of logToScan. That should not happen as
            // long as logToScan is not currupt. E.g., this could mean
            // that one of the data or optional data lengths were
            // wrong. No matter what caused us to be outside the
            // logToScan size, we will not be able to read more log
            // from this logToScan, and should probably abort the
            // whole replication due to corrupt data. That decision is
            // not made here, however.
            validLogRecord = false;
            throw se;
        }

        return validLogRecord;
    }

    /**
     * @return The instant of the log record read by the last call to
     * next().
     * @throws NoSuchElementException if next() has not been called or
     * if there are no more log records in this chunk of log. Should
     * never be thrown unless ReplicationLogScan is used in a wrong
     * way.
     */
    protected long getInstant() throws NoSuchElementException{
        if (validLogRecord) {
            return currentInstant;
        } else {
            throw new NoSuchElementException();
        }
    }

    /**
     * @return The number of bytes in the byte[] returned by getData()
     * for the log record read by the last call to next().
     * @throws NoSuchElementException if next() has not been called or
     * if there are no more log records in this chunk of log. Should
     * never be thrown unless ReplicationLogScan is used in a wrong
     * way.
     */
    protected int getDataLength() throws NoSuchElementException{
        if (validLogRecord) {
            return currentData.length;
        } else {
            throw new NoSuchElementException();
        }
    }

    /**
     * @return The offset in the byte[] returned by getData() for the
     * log record read by the last call to next().
     * @throws NoSuchElementException if next() has not been called or
     * if there are no more log records in this chunk of log. Should
     * never be thrown unless ReplicationLogScan is used in a wrong
     * way.
     */
    protected int getDataOffset() throws NoSuchElementException{
        if (validLogRecord) {
            return currentDataOffset;
        } else {
            throw new NoSuchElementException();
        }
    }

    /**
     * @return The number of bytes in the byte[] returned by
     * getOptData() for the log record read by the last call to next().
     * @throws NoSuchElementException if next() has not been called or
     * if there are no more log records in this chunk of log. Should
     * never be thrown unless ReplicationLogScan is used in a wrong
     * way.
     */
    protected int getOptDataLength() throws NoSuchElementException{
        if (validLogRecord) {
            return currentOptData.length;
        } else {
            throw new NoSuchElementException();
        }
    }

    /**
     * @return The offset in the byte[] returned by getOptData() for
     * the log record read by the last call to next().
     * @throws NoSuchElementException if next() has not been called or
     * if there are no more log records in this chunk of log. Should
     * never be thrown unless ReplicationLogScan is used in a wrong
     * way.
     */
    protected int getOptDataOffset() throws NoSuchElementException{
        if (validLogRecord) {
            return currentOptDataOffset;
        } else {
            throw new NoSuchElementException();
        }
    }

    /**
     * @return The data byte[] of the log record read by the last call
     * to next().
     * @throws NoSuchElementException if next() has not been called or
     * if there are no more log records in this chunk of log. Should
     * never be thrown unless ReplicationLogScan is used in a wrong
     * way.
     */
    protected byte[] getData() throws NoSuchElementException{
        if (validLogRecord) {
            return currentData;
        } else {
            throw new NoSuchElementException();
        }
    }

    /**
     * @return The optionalData byte[] of the log record read by the
     * last call to next().
     * @throws NoSuchElementException if next() has not been called or
     * if there are no more log records in this chunk of log. Should
     * never be thrown unless ReplicationLogScan is used in a wrong
     * way.
     */
    protected byte[] getOptData() throws NoSuchElementException{
        if (validLogRecord) {
            return currentOptData;
        } else {
            throw new NoSuchElementException();
        }
    }

    /**
     * @return Length of byte[] data + byte[] optionalData of the log
     * record read by the last call to next(). This is the same number
     * as is stored in the normal Derby transaction log as "lenght" in
     * the head and tail of each log record.
     * @throws NoSuchElementException if next() has not been called or
     * if there are no more log records in this chunk of log. Should
     * never be thrown unless ReplicationLogScan is used in a wrong
     * way.
     */
    protected int getTotalDataLength() throws NoSuchElementException{
        if (validLogRecord) {
            return currentData.length + currentOptData.length;
        } else {
            throw new NoSuchElementException();
        }
    }

    /**
     * @return true if next() has been called and the end of the log
     * chunk has not yet been reached. Returns the same answer as the
     * last call to next() did.
     */
    protected boolean hasValidLogRecord() {
        return validLogRecord;
    }

    /*
     * The retrieveXXX methods are used by next() to read a log record
     * from byte[] logToScan. The methods should be changed to
     * java.nio.ByteBuffer if it is later decided that replication
     * does not have to support j2me.
     */

    /**
     * Copy length number of bytes from logToScan into readInto.
     * Starts to copy from currentPosition. Also increments
     * currentPosition by length.
     * @param readInto The byte[] copied to
     * @param length The number of bytes copied from logToScan to readInto
     * @throws StandardException if there are less then length bytes
     * left to read in logToScan, meaning that the chunk of log is
     * corrupt.
     */
    private void retrieveBytes(byte[] readInto, int length)
        throws StandardException{

        if (SanityManager.DEBUG){
            SanityManager.ASSERT(logToScan.length >= currentPosition + length,
                                 "Trying to read more bytes than there are " +
                                 "in this logToScan");
            SanityManager.ASSERT(readInto.length == length,
                                 "readInto does not have a size of " + length +
                                 ", but a size of " + readInto.length);
        }

        try {
            System.arraycopy(logToScan, currentPosition, readInto, 0, length);
            currentPosition += length;
        } catch (ArrayIndexOutOfBoundsException aioobe) {
            throw StandardException.
                newException(SQLState.REPLICATION_LOG_CORRUPTED, aioobe);
        }
    }

    /**
     * Read an int from logToScan. Also increments currentPosition by
     * 4 (the number of bytes in an int).
     * @return an int read from logToScan
     * @throws StandardException if there are less then 4 bytes left
     * to read in logToScan, meaning that the chunk of log is corrupt.
     */
    private int retrieveInt() throws StandardException{
        if (SanityManager.DEBUG){
            SanityManager.ASSERT(logToScan.length >= currentPosition + 4,
                                 "Trying to read more bytes than there are " +
                                 "in this logToScan");
        }
        try {
            int i = ((logToScan[currentPosition++] << 24) 
                     + ((logToScan[currentPosition++] & 0xff) << 16) 
                     + ((logToScan[currentPosition++] & 0xff) << 8) 
                     + (logToScan[currentPosition++] & 0xff));
            return i;
        } catch (ArrayIndexOutOfBoundsException aioobe) {
            throw StandardException.
                newException(SQLState.REPLICATION_LOG_CORRUPTED, aioobe);
        }
    }

    /**
     * Read a long from logToScan. Also increments currentPosition by
     * 8 (the number of bytes in a long).
     * @return a long read from logToScan
     * @throws StandardException if there are less then 8 bytes left
     * to read in logToScan, meaning that the chunk of log is corrupt.
     */
    private long retrieveLong() throws StandardException{
        if (SanityManager.DEBUG){
            SanityManager.ASSERT(logToScan.length >= currentPosition + 8,
                                 "Trying to read more bytes than there are " +
                                 "in this logToScan");
        }
        try {
            long l = ((((long)logToScan[currentPosition++]) << 56) 
                      + ((logToScan[currentPosition++] & 0xffL) << 48)
                      + ((logToScan[currentPosition++] & 0xffL) << 40)
                      + ((logToScan[currentPosition++] & 0xffL) << 32)
                      + ((logToScan[currentPosition++] & 0xffL) << 24)
                      + ((logToScan[currentPosition++] & 0xff) << 16)
                      + ((logToScan[currentPosition++] & 0xff) << 8)
                      + (logToScan[currentPosition++] & 0xff));
            return l;
        } catch (ArrayIndexOutOfBoundsException aioobe) {
            throw StandardException.
                newException(SQLState.REPLICATION_LOG_CORRUPTED, aioobe);
        }
    }

}
