/*

   Derby - Class org.apache.derby.impl.jdbc.LOBStreamControl

   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.

 */
package org.apache.derby.impl.jdbc;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.security.AccessControlException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.SQLException;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.store.raw.data.DataFactory;
import org.apache.derby.io.StorageFile;
import org.apache.derby.io.StorageRandomAccessFile;
import org.apache.derby.shared.common.error.ExceptionUtil;

/**
 * This class acts as a layer of blob/clob repository (in memory or file).
 * The max bytes of data stored in memory depends on the way this
 * class is created. If the class is created with initial data, the buffer
 * size is set to the size of the byte array supplied. If no initial data
 * is supplied or if the initial data size is less than DEFAULT_MAX_BUF_SIZE,
 * The buffer size is set to DEFAULT_MAX_BUF_SIZE.
 * When write increases the data beyond this value a temporary file is created
 * and data is moved into that. If truncate reduces the size of the file below
 * initial buffer size (max of DEFAULT_MAX_BUF_SIZE and initial byte array size)
 * the data moved into memory.
 *
 * This class also creates InputStream and OutputStream which can be used to access
 * blob data irrespective of if its in memory or in file.
 */

class LOBStreamControl {
    private LOBFile tmpFile;
    private StorageFile lobFile;
    private byte [] dataBytes = new byte [0];
    private boolean isBytes = true;
    private final int bufferSize;
    private String dbName;
    private long updateCount;
    private static final int DEFAULT_MAX_BUF_SIZE = 4096;

    /**
     * Creates an empty LOBStreamControl.
     * @param dbName database name
     */
    LOBStreamControl (String dbName) {
        this.dbName = dbName;
        updateCount = 0;
        //default buffer size
        bufferSize = DEFAULT_MAX_BUF_SIZE;
    }

    /**
     * Creates a LOBStreamControl and initializes with a bytes array.
     * @param dbName database name
     * @param data initial value
     */
    LOBStreamControl (String dbName, byte [] data)
                throws IOException, SQLException, StandardException {
        this.dbName = dbName;
        updateCount = 0;
        bufferSize = Math.max (DEFAULT_MAX_BUF_SIZE, data.length);
        write (data, 0, data.length, 0);
    }

    private void init(byte [] b, long len)
                    throws IOException, SQLException, StandardException {
        try {
            AccessController.doPrivileged (new PrivilegedExceptionAction() {
                public Object run() throws IOException, StandardException {
                    Object monitor = Monitor.findService(
                            Property.DATABASE_MODULE, dbName);
                    DataFactory df =  (DataFactory) Monitor.findServiceModule(
                            monitor, DataFactory.MODULE);
                    //create a temporary file
                    lobFile =
                        df.getStorageFactory().createTemporaryFile("lob", null);
                    if (df.databaseEncrypted()) {
                        tmpFile = new EncryptedLOBFile (lobFile, df);
                    }
                    else
                        tmpFile = new LOBFile (lobFile);
                    return null;
                }
            });
        }
        catch (PrivilegedActionException pae) {
            Exception e = pae.getException();
            if (e instanceof StandardException)
                throw Util.generateCsSQLException ((StandardException) e);
            if (e instanceof IOException)
                throw (IOException) e;
            if (e instanceof RuntimeException)
                throw (RuntimeException) e;
            IOException ioe = new IOException (e.getMessage());
            ioe.initCause (e);
            throw ioe;
        }
        isBytes = false;
        //now this call will write into the file
        if (len != 0)
            write(b, 0, (int) len, 0);
        dataBytes = null;
    }

    private long updateData(byte[] bytes, int offset, int len, long pos)
    throws SQLException {
        if (dataBytes == null) {
            if ((int) pos == 0) {
                dataBytes = new byte [len];
                System.arraycopy(bytes, offset, dataBytes, (int) pos, len);
                return len;
            }
            else {
                //invalid postion
                throw Util.generateCsSQLException(
                        SQLState.BLOB_POSITION_TOO_LARGE, new Long(pos));
            }
        }
        else {
            if (pos > dataBytes.length) {
                //invalid postion
                throw Util.generateCsSQLException(
                        SQLState.BLOB_POSITION_TOO_LARGE, new Long(pos));
            }
            else {
                if (pos + len < dataBytes.length) {
                    System.arraycopy(bytes, offset, dataBytes, (int) pos, len);
                }
                else {
                    byte [] tmpBytes = new byte [len + (int) pos];
                    System.arraycopy(dataBytes, 0 , tmpBytes, 0, (int) pos);
                    System.arraycopy(bytes, offset, tmpBytes, (int) pos, len);
                    dataBytes = tmpBytes;
                }
            }
            return pos + len;
        }
    }

    private void isValidPostion(long pos)
                        throws SQLException, IOException {
        if (pos < 0)
            throw Util.generateCsSQLException(
                    SQLState.BLOB_NONPOSITIVE_LENGTH, new Long(pos + 1));
        if (pos > Integer.MAX_VALUE)
            throw Util.generateCsSQLException(
                    SQLState.BLOB_POSITION_TOO_LARGE, new Long(pos + 1));

        if (isBytes) {
            if (dataBytes == null) {
                if (pos != 0)
                    throw Util.generateCsSQLException(
                            SQLState.BLOB_POSITION_TOO_LARGE, new Long(pos + 1));
            } else if (dataBytes.length < pos)
                throw Util.generateCsSQLException(
                        SQLState.BLOB_POSITION_TOO_LARGE, new Long(pos + 1));
        } else {
            if (pos > tmpFile.length())
                throw Util.generateCsSQLException(
                        SQLState.BLOB_POSITION_TOO_LARGE, new Long(pos + 1));
        }
    }

    private void isValidOffset(int off, int length) throws SQLException {
        if (off < 0 || off > length)
            throw Util.generateCsSQLException(
                    SQLState.BLOB_INVALID_OFFSET, new Integer(off));
    }

    /**
     * Writes one byte.
     * @param b byte
     * @param pos
     * @return new postion
     * @throws IOException, SQLException, StandardException
     */
    synchronized long write(int b, long pos)
                throws IOException, SQLException, StandardException {
        isValidPostion(pos);
        updateCount++;
        if (isBytes) {
            if (pos < bufferSize) {
                byte [] bytes = {(byte) b};
                updateData(bytes, 0, 1, pos);
                return pos + 1;
            } else {
                init(dataBytes, pos);
            }
        }
        tmpFile.seek(pos);
        tmpFile.write(b);
        return tmpFile.getFilePointer();
    }

    /**
     * Writes part of the byte array.
     * @param b byte array
     * @param off offset from where to read from the byte array
     * @param len number of bytes to be copied
     * @param pos starting postion
     * @return new postion
     * @throws IOException, SQLException, StandardException
     */
    synchronized long write(byte[] b, int off, int len, long pos)
                        throws IOException, SQLException, StandardException {
        try {
            isValidPostion(pos);
            isValidOffset(off, b.length);
        }
        catch (SQLException e) {
            if (e.getSQLState().equals(
                    ExceptionUtil.getSQLStateFromIdentifier(
                                  SQLState.BLOB_INVALID_OFFSET)))
                    throw new ArrayIndexOutOfBoundsException (e.getMessage());
            throw e;
        }
        updateCount++;
        if (isBytes) {
            if (pos + len <= bufferSize)
                return updateData(b, off, len, pos);
            else {
                init(dataBytes, pos);
            }
        }
        tmpFile.seek(pos);
        tmpFile.write(b, off, len);
        return tmpFile.getFilePointer();
    }

    /**
     * Reads one byte.
     * @param pos postion from where to read
     * @return byte
     * @throws IOException, SQLException, StandardException
     */
    synchronized int read(long pos)
                throws IOException, SQLException, StandardException {
        isValidPostion(pos);
        if (isBytes) {
            if (dataBytes.length == pos)
                return -1;
            return dataBytes [(int) pos] & 0xff;
        }
        if (tmpFile.getFilePointer() != pos)
            tmpFile.seek(pos);
        try {
            return tmpFile.readByte() & 0xff;
        }
        catch (EOFException eof) {
            return -1;
        }
    }

    private int readBytes(byte [] b, int off, int len, long pos) {
        if (pos >= dataBytes.length)
            return -1;
        int lengthFromPos = dataBytes.length - (int) pos;
        int actualLength = len > lengthFromPos ? lengthFromPos : len;
        byte [] result = new byte[actualLength];
        System.arraycopy(dataBytes, (int) pos, b, off, actualLength);
        return actualLength;
    }

    /**
     * Reads bytes starting from 'position' into bytes array.
     * starting from 'offset'
     * @param buff array into the bytes will be copied
     * @param off offset from where the array has to be populated
     * @param len number of bytes to read
     * @param pos initial postion before reading
     * @return number new postion
     * @throws IOException, SQLException, StandardException
     */
    synchronized int read(byte[] buff, int off, int len, long pos)
    throws IOException, SQLException, StandardException {
        isValidPostion(pos);
        isValidOffset(off, buff.length);
        if (isBytes) {
            return readBytes(buff, off, len, pos);
        }
        tmpFile.seek(pos);
        return tmpFile.read (buff, off, len);
    }

    /**
     * returns input stream linked with this object.
     * @param pos initial postion
     * @return InputStream
     */
    InputStream getInputStream(long pos) {
        return new LOBInputStream(this, pos);
    }

    /**
     * returns output stream linked with this object
     * @param pos initial postion
     * @return OutputStream
     */
    OutputStream getOutputStream(long pos) {
        return new LOBOutputStream(this, pos);
    }

    /**
     * Returns length of data.
     * @return length
     * @throws IOException
     */
    long getLength() throws IOException {
        if (isBytes)
            return dataBytes.length;
        return tmpFile.length();
    }

    /**
     * Resets the size.
     * @param size new size should be smaller than exisiting size
     * @throws IOException, SQLException
     */
    synchronized void truncate(long size)
                        throws IOException, SQLException, StandardException {
        isValidPostion(size);
        if (isBytes) {
            byte [] tmpByte = new byte [(int) size];
            System.arraycopy(dataBytes, 0, tmpByte, 0, (int) size);
            dataBytes = tmpByte;
        } else {
            if (size < bufferSize) {
                dataBytes = new byte [(int) size];
                read(dataBytes, 0, dataBytes.length, 0);
                isBytes = true;
                tmpFile.close();
                tmpFile = null;
            } else {
                try {
                    tmpFile.setLength(size);
                }
                catch (StandardException se) {
                    Util.generateCsSQLException (se);
                }
            }
        }
    }

    /**
     * Copies bytes from stream to local storage.
     * @param inStream
     * @param length length to be copied
     * @throws IOException, SQLException, StandardException
     */
    synchronized void copyData(InputStream inStream,
            long length) throws IOException, SQLException, StandardException {
        byte [] data = new byte [bufferSize];
        long sz = 0;
        while (sz < length) {
            int len = (int) Math.min (length - sz, bufferSize);
            len = inStream.read(data, 0, len);
            if (len < 0)
                throw new EOFException("Reached end-of-stream " +
                        "prematurely at " + sz);
            write(data, 0, len, sz);
            sz += len;
        }
    }

    protected void finalize() throws Throwable {
        free();
    }

    /**
     * Invalidates all the variables and closes file handle if open.
     * @throws IOexception
     */
    void free() throws IOException {
        dataBytes = null;
        if (tmpFile != null) {
            tmpFile.close();
            try {
                AccessController.doPrivileged (new PrivilegedExceptionAction() {
                    public Object run() throws IOException {
                        lobFile.delete();
                        return null;
                    }
                });
            }
            catch (PrivilegedActionException pae) {
                Exception e = pae.getException();
                if (e instanceof IOException)
                    throw (IOException) e;
                if (e instanceof RuntimeException)
                    throw (RuntimeException) e;
                IOException ioe = new IOException (e.getMessage());
                ioe.initCause (e);
                throw ioe;
            }
        }
    }
    
    /**
     * Replaces a block of bytes in the middle of the LOB with a another block
     * of bytes, which may be of a different size.
     * <p>
     * The new byte array may not be be of same length as the original,
     * thus it may result in resizing the total lob.
     *
     * @param buf byte array which will be written inplace of old block
     * @param stPos inclusive starting position of current block
     * @param endPos exclusive end position of current block
     * @return Current position after write.
     * @throws IOExcepton if writing to temporary file fails
     * @throws StandardException
     * @throws SQLException
     */
    synchronized long replaceBytes (byte [] buf, long stPos, long endPos) 
                         throws IOException, SQLException, StandardException {
        long length = getLength();
        long finalLength = length - endPos + stPos + buf.length;
        if (isBytes) {
            if (finalLength > bufferSize) {
                init (dataBytes, stPos);
                write (buf, 0, buf.length, getLength());
                if (endPos < length)
                    write (dataBytes, (int) endPos, 
                            (int) (length - endPos), getLength());
            }
            else {
                byte [] tmpByte = new byte [(int) finalLength];
                System.arraycopy (dataBytes, 0, tmpByte, 0, (int) stPos);
                System.arraycopy (buf, 0, tmpByte, (int) stPos, (int) buf.length);
                if (endPos < length)
                    System.arraycopy (dataBytes, (int) endPos, tmpByte, 
                            (int) (stPos + buf.length), (int) (length - endPos));
                dataBytes = tmpByte;            
            }
        }
        else {
            //save over file handle and 
            //create new file with 0 size
            
            byte tmp [] = new byte [0];
            LOBFile oldFile = tmpFile;
            init (tmp, 0);
            byte [] tmpByte = new byte [1024];
            long sz = stPos;
            oldFile.seek(0);
            while (sz != 0) {
                int readLen = (int) Math.min (1024, sz);                
                int actualLength = oldFile.read (tmpByte, 0, readLen);
                if (actualLength == -1)
                    break;
                tmpFile.write (tmpByte, 0, actualLength);
                sz -= actualLength;
            }
            tmpFile.write (buf);
            oldFile.seek (endPos);
            int rdLen;
            if (endPos < length) {
                do {
                    rdLen = oldFile.read (tmpByte, 0, 1024);
                    if (rdLen == -1)
                        break;
                    tmpFile.write (tmpByte, 0, rdLen);
                }while (true);
            }            
        }
        updateCount++;
        return stPos + buf.length;
    }

    /**
     * Returns the running secquence number to check if the lob is updated since
     * last access.
     *
     * @return The current update sequence number.
     */
    long getUpdateCount() {
        return updateCount;
    }
}
