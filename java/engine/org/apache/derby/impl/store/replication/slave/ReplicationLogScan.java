/*

   Derby - Class org.apache.derby.impl.store.replication.slave.ReplicationLogScan

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

package org.apache.derby.impl.store.replication.slave;

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
 * successfully read. The information in this last read log record
 * either indicates that a log file switch has taken place on the
 * master (isLogSwitch() = true) or it is a normal log record which
 * information can be retrieved by using the get-methods (if
 * isLogRecord() = true).
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
 * org.apache.derby.impl.store.raw.log.LogAccessFile
 * </p>
 * @see org.apache.derby.impl.store.raw.log.LogAccessFile
 */

class ReplicationLogScan {

    private byte[] logToScan;     // the chunk of log to scan
    private int currentPosition;  // how long we have read in logToScan

    // Data for the latest log record read by next(). Use the
    // get-methods to retrieve these
    private long currentInstant;
    private int currentDataOffset;
    private byte[] currentData;

    /** hasInfo = true when the scan will return meaningful
     * information, either in the form of a log record (in which case
     * the above variables will be set), or when it has found a log
     * record indicating that a log file switch has taken place on the
     * master (isLogSwitch = true). hasInfo = false before next() is
     * called for the first time, after next() has reached the end of
     * logToScan and if an error occured when parsing logToScan (i.e.
     * when next() has thrown a StandardException)
     */
    private boolean hasInfo;

    /** true if the last read log record indicates a log switch, false
     * if it is a normal log record private boolean isLogSwitch;
     */
    private boolean isLogSwitch;

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
        currentData = null;
        isLogSwitch = false;
        hasInfo = false;
    }

    /**
     * <p>
     * Read the next log record in logToScan. The information in this
     * log record can be read by using the getXXX methods.
     * </p>
     * <p>
     * Side effects: <br>
     * <br>
     * On a successful read (return true): either...<br>
     *<br>
     * ... the scan read a log record indicating that a log file
     * switch has taken place on the master, in which case
     * isLogFileSwitch() returns true. In this case, getXXX will not
     * contain valid data. Asserts handle calls to these methods when
     * in sane mode. currentPosition is updated to point to the byte
     * immediately following this log file switch log record.<br>
     *<br>
     * ... or the scan read a normal log record, in which case
     * isLogRecord() returns true. Also sets currentInstant and
     * currentData, and updates currentPosition to point to the byte
     * immediatly following the log record. In this case, getXXX will
     * return meaningful information about the log record.
     * </p>
     * <p>
     * If there are no more log records in logToScan (returns false) or
     * a problem occurs (throws StandardException): setting
     * hasInfo = false
     * </p>
     * @return true if a log record was successfully read, false if end
     * of logToScan has been reached.
     * @throws StandardException if logToScan is found to be corrupted.
     */
    protected boolean next() throws StandardException {

        /* format of received log:
         *
         * (int)    total_length (data[].length + optionaldata[].length)
         * (long)   instant
         * (byte[]) data+optionaldata
         * (int)    total_length
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
            hasInfo = false;
            return hasInfo;
        }

        try {
            int currentLength = retrieveInt();   // (int)  dataLength

            if (currentLength == 0) { 
                // int value 0 is written to log to mark EOF. A length
                // of 0 therefore means that a log file switch has
                // taken place on the master
                isLogSwitch = true;
                hasInfo = true;
            } else {

                currentInstant = retrieveLong(); // (long) instant

                // (byte[]) data
                currentData = new byte[currentLength];
                retrieveBytes(currentData, currentLength);

                retrieveInt();                   // (int) trailing length

                isLogSwitch = false;
                hasInfo = true;
            }
        } catch(StandardException se){
            // Means that we have tried to read higher byte addresses
            // than the size of logToScan. That should not happen as
            // long as logToScan is not corrupted. E.g., this could mean
            // that the data length was
            // wrong. No matter what caused us to be outside the
            // logToScan size, we will not be able to read more log
            // from this logToScan, and should probably abort the
            // whole replication due to corrupted data. That decision is
            // not made here, however.
            hasInfo = false;
            throw se;
        }

        return hasInfo;
    }

    /**
     * @return The instant of the log record read by the last call to
     * next(). Only returns meaningful information if isLogRecord()
     * returns true.
     * @throws NoSuchElementException if next() has not been called or
     * if there are no more log records in this chunk of log. Should
     * never be thrown unless ReplicationLogScan is used in a wrong
     * way.
     */
    protected long getInstant() throws NoSuchElementException{
        if (!hasInfo) {
            throw new NoSuchElementException();
        }

        if (isLogSwitch) {
            if (SanityManager.DEBUG){
                SanityManager.THROWASSERT("Log switch log records " +
                                          "have no instant");
            }
            return -1;
        }

        return currentInstant;
    }

    /**
     * @return The number of bytes in the byte[] returned by getData()
     * for the log record read by the last call to next(). Only
     * returns meaningful information if isLogRecord() returns true.
     * @throws NoSuchElementException if next() has not been called or
     * if there are no more log records in this chunk of log. Should
     * never be thrown unless ReplicationLogScan is used in a wrong
     * way.
     */
    protected int getDataLength() throws NoSuchElementException{
        if (!hasInfo) {
            throw new NoSuchElementException();
        }

        if (isLogSwitch) {
            if (SanityManager.DEBUG){
                SanityManager.THROWASSERT("Log switch log records " +
                                          "have no length");
            }
            return -1;
        }

        return currentData.length;
    }

    /**
     * Method to get the data byte[] read by the last call to next().
     * Note that this byte[] contains both byte[] data and byte[]
     * optional_data from LogAccessFile. To split this byte[] into
     * data and optional_data, we would need to create a Loggable
     * object from it because the log does not provide information on
     * where to split. There is no need to split since this byte[]
     * will only be written to the slave log anyway. If it was split,
     * LogAccessFile would simply merge them when writing to file.
     *
     * @return The byte[] containing data+optional_data of the log
     * record read by the last call to next(). Only returns meaningful
     * information if isLogRecord() returns true.
     * @throws NoSuchElementException if next() has not been called or
     * if there are no more log records in this chunk of log. Should
     * never be thrown unless ReplicationLogScan is used in a wrong
     * way.
     */
    protected byte[] getData() throws NoSuchElementException{
        if (!hasInfo) {
            throw new NoSuchElementException();
        }

        if (isLogSwitch) {
            if (SanityManager.DEBUG){
                SanityManager.THROWASSERT("Log switch log records " +
                                          "have no data");
            }
            return null;
        }

        return currentData;
    }

    /**
     * Used to determine whether or not the last call to next() was
     * successful.
     * @return true if next() has been called and the end of the log
     * chunk has not yet been reached. Returns the same answer as the
     * last call to next() did. Use isLogFileSwitch() and
     * isLogRecord() to find out if the current log record indicates a
     * log file switch or is a normal log record.
     */
    protected boolean hasValidInformation() {
        return hasInfo;
    }

    /**
     * Used to determine whether the last call to next() read a log
     * record
     * @return true if the last call to next() found a normal log
     * record.
     * @throws NoSuchElementException if next() has not been called or
     * if there are no more log records in this chunk of log. Should
     * never be thrown unless ReplicationLogScan is used in a wrong
     * way.
     */
    protected boolean isLogRecord()  throws NoSuchElementException{
        if (!hasInfo) {
            throw new NoSuchElementException();
        }

        return !isLogSwitch;
    }

    /**
     * Used to determine whether the last call to next() found a log
     * file switch
     * @return true if the last call to next() found a log record
     * indicating a log file switch has taken place on the master.
     * @throws NoSuchElementException if next() has not been called or
     * if there are no more log records in this chunk of log. Should
     * never be thrown unless ReplicationLogScan is used in a wrong
     * way.
     */
    protected boolean isLogFileSwitch() throws NoSuchElementException{
        if (!hasInfo) {
            throw new NoSuchElementException();
        }

        return isLogSwitch;
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
