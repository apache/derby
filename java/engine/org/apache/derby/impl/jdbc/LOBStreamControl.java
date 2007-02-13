/*

   Derby - Class org.apache.derby.impl.jdbc.LOBStreamControl

   Copyright 2006 The Apache Software Foundation or its licensors, as applicable.

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
 * The max bytes of data stored in memory is MAX_BUF_SIZE. When write
 * increases the data beyond this value a temporary file is created and data
 * is moved into that. If truncate reduces the size of the file below
 * MAX_BUF_SIZE the data moved into memory.
 *
 * This class also creates Input- and OutputStream which can be used to access
 * blob data irrespective of if its in memory or in file.
 */

class LOBStreamControl {
    //private RandomAccessFile tmpFile;
    private StorageRandomAccessFile tmpFile;
    private StorageFile lobFile;
    private byte [] dataBytes = new byte [0];
    private boolean isBytes = true;
    //keeping max 4k bytes in memory
    //randomly selected value
    private final int MAX_BUF_SIZE = 4096;
    private String dbName;

    public LOBStreamControl (String dbName) {
        this.dbName = dbName;
    }

    private void init(byte [] b, long len) throws IOException, SQLException {
        try {
            AccessController.doPrivileged (new PrivilegedExceptionAction() {
                public Object run() throws IOException, StandardException {
                    Object monitor = Monitor.findService(
                            Property.DATABASE_MODULE, dbName);
                    DataFactory df =  (DataFactory) Monitor.findServiceModule(
                            monitor, DataFactory.MODULE);
                    lobFile =
                        df.getStorageFactory().createTemporaryFile("lob", null);
                    tmpFile = lobFile.getRandomAccessFile ("rw");
                    return null;
                }
            });
            //create a temporary file
        }
        catch (PrivilegedActionException pae) {
            Exception e = pae.getException();
            if (e instanceof StandardException)
                throw Util.generateCsSQLException ((StandardException) e);
            if (e instanceof IOException)
                throw (IOException) e;
            if (e instanceof RuntimeException)
                throw (RuntimeException) e;
        }
        isBytes = false;
        //now this call will write into the file
        if (len != 0)
            write(b, 0, (int) len, 0);
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

    private void isValidPostion(long pos) throws SQLException, IOException {
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

    private void isValidOffset(int off, int length) throws SQLException, IOException {
        if (off < 0 || off > length)
            throw Util.generateCsSQLException(
                    SQLState.BLOB_INVALID_OFFSET, new Integer(off));
    }

    /**
     * Writes one byte.
     * @param b byte
     * @param pos
     * @return new postion
     * @throws IOException, SQLException
     */
    synchronized long write(int b, long pos) throws IOException, SQLException {
        isValidPostion(pos);
        if (isBytes) {
            if (pos + 1 < MAX_BUF_SIZE) {
                byte [] bytes = {(byte) b};
                updateData(bytes, 0, 1, pos);
                return pos + 1;
            } else {
                init(dataBytes, pos);
            }
        }
        if (tmpFile.getFilePointer() != pos)
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
     * @throws IOException, SQLException
     */
    synchronized long write(byte[] b, int off, int len, long pos)
    throws IOException, SQLException {
        try {
            isValidPostion(pos);
            isValidOffset(off, b.length);
        }
        catch (SQLException e) {
            if (e.getSQLState().equals(
                    ExceptionUtil.getSQLStateFromIdentifier(
                                  SQLState.BLOB_INVALID_OFFSET)))
                    throw new ArrayIndexOutOfBoundsException (e.getMessage());
        }
        if (isBytes) {
            long finalLen = (dataBytes != null) ? dataBytes.length + b.length
                    : b.length;
            if (finalLen < MAX_BUF_SIZE)
                return updateData(b, off, len, pos);
            else {
                init(dataBytes, pos);
            }
        }
        if (tmpFile.getFilePointer() != pos)
            tmpFile.seek(pos);
        tmpFile.write(b, off, len);
        return tmpFile.getFilePointer();
    }

    /**
     * Writes byte array starting from pos.
     * @param b bytes array
     * @param pos starting postion
     * @return new position
     * @throws IOException, SQLException
     */
    synchronized long write(byte[] b, long pos)
    throws IOException, SQLException {
        isValidPostion(pos);
        if (isBytes) {
            long len = (dataBytes != null) ? dataBytes.length + b.length
                    : b.length;
            if (len < MAX_BUF_SIZE)
                return updateData(b, 0, b.length, pos);
            else {
                init(dataBytes, pos);
            }
        }
        if (tmpFile.getFilePointer() != pos)
            tmpFile.seek(pos);
        tmpFile.write(b);
        return tmpFile.getFilePointer();
    }

    /**
     * Reads one byte.
     * @param pos postion from where to read
     * @return byte
     * @throws IOException, SQLException
     */
    synchronized int read(long pos) throws IOException, SQLException {
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
        int lengthFromPos = dataBytes.length - (int) pos;
        int actualLength = len > lengthFromPos ? lengthFromPos : len;
        byte [] result = new byte[actualLength];
        System.arraycopy(dataBytes, (int) pos, b, off, actualLength);
        return actualLength;
    }

    /**
     * Copies bytes into byte array starting from pos.
     * @param b bytes array to copy data
     * @param pos starting postion
     * @return new postion
     * @throws IOException, SQLException
     */
    synchronized int read(byte[] b, long pos)
    throws IOException, SQLException {
        return read (b, 0, b.length, pos);
    }

    /**
     * Reads bytes starting from 'position' into bytes array.
     * starting from 'offset'
     * @param b array into the bytes will be copied
     * @param off offset from where the array has to be populated
     * @param len number of bytes to read
     * @param pos initial postion before reading
     * @return number new postion
     * @throws IOException, SQLException
     */
    synchronized int read(byte[] buff, int off, int len, long pos)
    throws IOException, SQLException {
        isValidPostion(pos);
        isValidOffset(off, buff.length);
        if (isBytes) {
            return readBytes(buff, off, len, pos);
        }
        if (tmpFile.getFilePointer() != pos)
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
    synchronized void truncate(long size) throws IOException, SQLException {
        isValidPostion(size);
        if (isBytes) {
            byte [] tmpByte = new byte [(int) size];
            System.arraycopy(dataBytes, 0, tmpByte, 0, (int) size);
        } else {
            if (size < Integer.MAX_VALUE && size < MAX_BUF_SIZE) {
                dataBytes = new byte [(int) size];
                read(dataBytes, 0);
                isBytes = true;
                tmpFile.close();
                tmpFile = null;
            } else
                tmpFile.setLength(size);
        }
    }

    /**
     * Copies bytes from stream to local storage.
     * @param inStream
     * @param pos length to be copied
     * @throws IOException, SQLException
     */
    synchronized void copyData(InputStream inStream,
            long length) throws IOException, SQLException {
        byte [] data = new byte [MAX_BUF_SIZE];
        long sz = 0;
        while (sz < length) {
            int len = (int) (((length - sz) >= MAX_BUF_SIZE) ? MAX_BUF_SIZE
                    : length - sz);
            inStream.read(data, 0, len);
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
            }
        }
    }
}
