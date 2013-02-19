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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UTFDataFormatException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.store.raw.data.DataFactory;
import org.apache.derby.io.StorageFile;
import org.apache.derby.iapi.error.ExceptionUtil;
import org.apache.derby.shared.common.reference.MessageId;

/**
 * This class acts as a layer of blob/clob repository (in memory or file).
 * The max bytes of data stored in memory depends on the way this
 * class is created. If the class is created with initial data, the buffer
 * size is set to the size of the byte array supplied, but no larger than
 * MAX_BUF_SIZE. If no initial data is supplied, or if the initial data size
 * is less than DEFAULT_BUF_SIZE, the buffer size is set to DEFAULT_BUF_SIZE.
 * When write increases the data beyond this value a temporary file is created
 * and data is moved into that. If truncate reduces the size of the file below
 * initial buffer size, the data is moved into memory.
 *
 * This class also creates InputStream and OutputStream which can be used to access
 * blob data irrespective of if its in memory or in file.
 */

class LOBStreamControl {
    private LOBFile tmpFile;
    private byte [] dataBytes = new byte [0];
    private boolean isBytes = true;
    private final int bufferSize;
    private final EmbedConnection conn;
    private long updateCount;
    private static final int DEFAULT_BUF_SIZE = 4096;
    private static final int MAX_BUF_SIZE = 32768;

    /**
     * Creates an empty LOBStreamControl.
     * @param conn Connection for this lob
     */
    LOBStreamControl (EmbedConnection conn) {
        this.conn = conn;
        updateCount = 0;
        //default buffer size
        bufferSize = DEFAULT_BUF_SIZE;
    }

    /**
     * Creates a LOBStreamControl and initializes with a bytes array.
     * @param conn Connection for this lob
     * @param data initial value
     */
    LOBStreamControl (EmbedConnection conn, byte [] data)
            throws IOException, StandardException {
        this.conn = conn;
        updateCount = 0;
        bufferSize =
            Math.min(Math.max(DEFAULT_BUF_SIZE, data.length), MAX_BUF_SIZE);
        write (data, 0, data.length, 0);
    }

    private void init(byte [] b, long len)
            throws IOException, StandardException {
        try {
            AccessController.doPrivileged (new PrivilegedExceptionAction() {
                public Object run() throws IOException, StandardException {
                    Object monitor = Monitor.findService(
                            Property.DATABASE_MODULE, conn.getDBName());
                    DataFactory df =  (DataFactory) Monitor.findServiceModule(
                            monitor, DataFactory.MODULE);
                    //create a temporary file
                    StorageFile lobFile =
                        df.getStorageFactory().createTemporaryFile("lob", null);
                    if (df.databaseEncrypted()) {
                        tmpFile = new EncryptedLOBFile (lobFile, df);
                    }
                    else
                        tmpFile = new LOBFile (lobFile);
                    conn.addLobFile(tmpFile);
                    return null;
                }
            });
        }
        catch (PrivilegedActionException pae) {
            Exception e = pae.getException();
            if (e instanceof StandardException)
                throw (StandardException)e;
            if (e instanceof IOException)
                throw (IOException) e;
            throw Util.newIOException(e);
        }
        isBytes = false;
        //now this call will write into the file
        if (len != 0)
            write(b, 0, (int) len, 0);
        dataBytes = null;
    }

    private long updateData(byte[] bytes, int offset, int len, long pos)
            throws StandardException {
        if (dataBytes == null) {
            if ((int) pos == 0) {
                dataBytes = new byte [len];
                System.arraycopy(bytes, offset, dataBytes, (int) pos, len);
                return len;
            }
            else {
                //invalid postion
                throw StandardException.newException(
                        SQLState.BLOB_POSITION_TOO_LARGE, new Long(pos));
            }
        }
        else {
            if (pos > dataBytes.length) {
                //invalid postion
                throw StandardException.newException(
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
            throws IOException, StandardException {
        if (pos < 0)
            throw StandardException.newException(
                    SQLState.BLOB_NONPOSITIVE_LENGTH, new Long(pos + 1));
        if (pos > Integer.MAX_VALUE)
            throw StandardException.newException(
                    SQLState.BLOB_POSITION_TOO_LARGE, new Long(pos + 1));

        if (isBytes) {
            if (dataBytes == null) {
                if (pos != 0)
                    throw StandardException.newException(
                            SQLState.BLOB_POSITION_TOO_LARGE, new Long(pos + 1));
            } else if (dataBytes.length < pos)
                throw StandardException.newException(
                        SQLState.BLOB_POSITION_TOO_LARGE, new Long(pos + 1));
        } else {
            if (pos > tmpFile.length())
                throw StandardException.newException(
                        SQLState.BLOB_POSITION_TOO_LARGE, new Long(pos + 1));
        }
    }

    private void isValidOffset(int off, int length) throws StandardException {
        if (off < 0 || off > length)
            throw StandardException.newException(
                    SQLState.BLOB_INVALID_OFFSET, new Integer(off));
    }

    /**
     * Writes one byte.
     * @param b byte
     * @param pos
     * @return new postion
     * @throws IOException, StandardException
     */
    synchronized long write(int b, long pos)
            throws IOException, StandardException {
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
     * Writes {@code len} bytes from the specified byte array to the LOB.
     *
     * @param b byte array
     * @param off offset from where to read from the byte array
     * @param len number of bytes to be written
     * @param pos starting position
     * @return The position after the bytes have been written to the LOB.
     * @throws IOException if writing to the LOB fails
     * @throws StandardException if writing to the LOB fails
     * @throws IndexOutOfBoundsException if {@code len} is larger than
     *       {@code b.length - off}
     */
    synchronized long write(byte[] b, int off, int len, long pos)
            throws IOException, StandardException {
        isValidPostion(pos);
        try {
            isValidOffset(off, b.length);
        } catch (StandardException e) {
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
     * @throws IOException, StandardException
     */
    synchronized int read(long pos)
            throws IOException, StandardException {
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
     * @throws IOException, StandardException
     */
    synchronized int read(byte[] buff, int off, int len, long pos)
            throws IOException, StandardException {
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
     * @throws IOException
     */
    synchronized void truncate(long size)
            throws IOException, StandardException {
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
                releaseTempFile(tmpFile);
                tmpFile = null;
            } else {
                tmpFile.setLength(size);
            }
        }
    }

    /**
     * Copies bytes from stream to local storage.
     * <p>
     * Note that specifying the length as {@code Long.MAX_VALUE} results in
     * reading data from the stream until EOF is reached, but no length checking
     * will be performed.
     *
     * @param inStream the stream to copy from
     * @param length number of bytes to be copied, or {@code Long.MAX_VALUE} to
     *      copy everything until EOF is reached
     * @throws IOException, StandardException
     */
    synchronized void copyData(InputStream inStream, long length)
            throws IOException, StandardException {
        byte [] data = new byte [bufferSize];
        long sz = 0;
        while (sz < length) {
            int len = (int) Math.min (length - sz, bufferSize);
            len = inStream.read(data, 0, len);
            if (len == -1) {
                if (length != Long.MAX_VALUE) {
                    // We reached EOF before all the requested bytes are read.
                    throw new EOFException(MessageService.getTextMessage(
                            MessageId.STREAM_PREMATURE_EOF,
                            new Long(length), new Long(sz)));
                } else {
                    // End of data, but no length checking.
                    break;
                }
            }
            write(data, 0, len, sz);
            sz += len;
        }
        // If we copied until EOF, and we read more data than the length of the
        // marker, see if we have a Derby end-of-stream marker.
        long curLength = getLength();
        if (length == Long.MAX_VALUE && curLength > 2) {
            byte[] eos = new byte[3];
            // Read the three last bytes, marker is 0xE0 0x00 0x00.
            read(eos, 0, 3, curLength -3);
            if ((eos[0] & 0xFF) == 0xE0 && (eos[1] & 0xFF) == 0x00 &&
                    (eos[2] & 0xFF) == 0x00) {
                // Remove Derby end-of-stream-marker.
                truncate(curLength -3);
            }
        }
    }

    /**
     * Copies UTF-8 encoded chars from a stream to local storage.
     * <p>
     * Note that specifying the length as {@code Long.MAX_VALUE} results in
     * reading data from the stream until EOF is reached, but no length checking
     * will be performed.
     *
     * @param utf8Stream the stream to copy from
     * @param charLength number of chars to be copied, or {@code Long.MAX_VALUE}
     *      to copy everything until EOF is reached
     * @return The number of characters copied.
     * @throws EOFException if EOF is reached prematurely
     * @throws IOException thrown on a number of error conditions
     * @throws StandardException if reading, writing or truncating the
     *      {@code LOBStreamControl}-object fails
     * @throws UTFDataFormatException if an invalid UTF-8 encoding is detected
     */
    synchronized long copyUtf8Data(final InputStream utf8Stream,
                                   final long charLength)
            throws IOException, StandardException {
        long charCount = 0; // Number of chars read
        int offset = 0;     // Where to start looking for the start of a char
        int read = 0;       // Number of bytes read
        final byte[] buf = new byte[bufferSize];
        while (charCount < charLength) {
            int readNow = utf8Stream.read(buf, 0,
                            (int)Math.min(buf.length, charLength - charCount));
            if (readNow == -1) {
                break;
            }
            // Count the characters.
            while (offset < readNow) {
                int c = buf[offset] & 0xFF;
                if ((c & 0x80) == 0x00) { // 8th bit not set (top bit)
                    offset++;
                } else if ((c & 0x60) == 0x40) { // 7th bit set, 6th bit unset
                    // Found char of two byte width.
                    offset += 2;
                } else if ((c & 0x70) == 0x60) { // 7th & 6th bit set, 5th unset
                    // Found char of three byte width.
                    offset += 3;
                } else {
                    // This shouldn't happen, as the data is coming from the
                    // store and is supposed to be well-formed.
                    // If it happens, fail and print some internal information.
                    throw new UTFDataFormatException("Invalid UTF-8 encoding: "
                            + Integer.toHexString(c) + ", charCount=" +
                            charCount + ", offset=" + offset);
                }
                charCount++;
            }
            offset -= readNow; // Starting offset for next iteration
            write(buf, 0, readNow, read);
            read += readNow;
        }
        // See if an EOF-marker ended the stream. Don't check if we have fewer
        // bytes than the marker length.
        long curLength = getLength();
        if (curLength > 2) {
            byte[] eos = new byte[3];
            // Read the three last bytes, marker is 0xE0 0x00 0x00.
            read(eos, 0, 3, curLength -3);
            if ((eos[0] & 0xFF) == 0xE0 && (eos[1] & 0xFF) == 0x00 &&
                    (eos[2] & 0xFF) == 0x00) {
                // Remove Derby end-of-stream-marker.
                truncate(curLength -3);
                charCount--;
            }
        }
        if (charLength != Long.MAX_VALUE && charCount != charLength) {
            throw new EOFException(MessageService.getTextMessage(
                    MessageId.STREAM_PREMATURE_EOF,
                    new Long(charLength), new Long(charCount)));
        }
        return charCount;
    }

    protected void finalize() throws Throwable {
        free();
    }

    private void deleteFile (StorageFile file) throws IOException {
        try {
            final StorageFile sf = file;
            AccessController.doPrivileged(new PrivilegedExceptionAction() {
                public Object run() throws IOException {
                    sf.delete();
                    return null;
                }
            });
        } catch (PrivilegedActionException pae) {
            Exception e = pae.getException();
            if (e instanceof IOException)
                throw (IOException) e;
            if (e instanceof RuntimeException)
                throw (RuntimeException) e;
            throw Util.newIOException(e);
        }
    }
    /**
     * Invalidates all the variables and closes file handle if open.
     * @throws IOexception
     */
    void free() throws IOException {
        dataBytes = null;
        if (tmpFile != null) {
            releaseTempFile(tmpFile);
            tmpFile = null;
        }
    }

    /**
     * Close and release all resources held by a temporary file. The file will
     * also be deleted from the file system and removed from the list of
     * {@code LOBFile}s in {@code EmbedConnection}.
     *
     * @param file the temporary file
     * @throws IOException if the file cannot be closed or deleted
     */
    private void releaseTempFile(LOBFile file) throws IOException {
        file.close();
        conn.removeLobFile(file);
        deleteFile(file.getStorageFile());
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
     */
    synchronized long replaceBytes (byte [] buf, long stPos, long endPos)
            throws IOException, StandardException {
        long length = getLength();
        if (isBytes) {
            long finalLength = length - endPos + stPos + buf.length;
            if (finalLength > bufferSize) {
                byte [] tmpBytes = dataBytes;
                init (tmpBytes, stPos);
                write (buf, 0, buf.length, getLength());
                if (endPos < length)
                    write (tmpBytes, (int) endPos, 
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
            releaseTempFile(oldFile);
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
