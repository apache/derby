/*

   Derby - Class org.apache.derby.impl.jdbc.LOBInputStream

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

import java.io.IOException;
import java.io.InputStream;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.shared.common.error.ExceptionUtil;

/**
 * This input stream is built on top of {@link LOBStreamControl}.
 * <p>
 * All the read methods are routed to {@link LOBStreamControl}.
 */

public class LOBInputStream extends InputStream {

    private boolean closed;
    private final LOBStreamControl control;
    private long pos;
    private long updateCount;

    LOBInputStream(LOBStreamControl control, long position) {
        closed = false;
        this.control = control;
        pos = position;
        updateCount = control.getUpdateCount ();
    }

    /**
     * Reads up to <code>len</code> bytes of data from the input stream into
     * an array of bytes.  An attempt is made to read as many as
     * <code>len</code> bytes, but a smaller number may be read.
     * The number of bytes actually read is returned as an integer.
     *
     * <p> This method blocks until input data is available, end of file is
     * detected, or an exception is thrown.
     *
     * <p> If <code>b</code> is <code>null</code>, a
     * <code>NullPointerException</code> is thrown.
     *
     * <p> If <code>off</code> is negative, or <code>len</code> is negative, or
     * <code>off+len</code> is greater than the length of the array
     * <code>b</code>, then an <code>IndexOutOfBoundsException</code> is
     * thrown.
     *
     * <p> If <code>len</code> is zero, then no bytes are read and
     * <code>0</code> is returned; otherwise, there is an attempt to read at
     * least one byte. If no byte is available because the stream is at end of
     * file, the value <code>-1</code> is returned; otherwise, at least one
     * byte is read and stored into <code>b</code>.
     *
     * <p> The first byte read is stored into element <code>b[off]</code>, the
     * next one into <code>b[off+1]</code>, and so on. The number of bytes read
     * is, at most, equal to <code>len</code>. Let <i>k</i> be the number of
     * bytes actually read; these bytes will be stored in elements
     * <code>b[off]</code> through <code>b[off+</code><i>k</i><code>-1]</code>,
     * leaving elements <code>b[off+</code><i>k</i><code>]</code> through
     * <code>b[off+len-1]</code> unaffected.
     *
     * <p> In every case, elements <code>b[0]</code> through
     * <code>b[off]</code> and elements <code>b[off+len]</code> through
     * <code>b[b.length-1]</code> are unaffected.
     *
     * <p> If the first byte cannot be read for any reason other than end of
     * file, then an <code>IOException</code> is thrown. In particular, an
     * <code>IOException</code> is thrown if the input stream has been closed.
     *
     * <p> The <code>read(b,</code> <code>off,</code> <code>len)</code> method
     * for class <code>InputStream</code> simply calls the method
     * <code>read()</code> repeatedly. If the first such call results in an
     * <code>IOException</code>, that exception is returned from the call to
     * the <code>read(b,</code> <code>off,</code> <code>len)</code> method.  If
     * any subsequent call to <code>read()</code> results in a
     * <code>IOException</code>, the exception is caught and treated as if it
     * were end of file; the bytes read up to that point are stored into
     * <code>b</code> and the number of bytes read before the exception
     * occurred is returned.  Subclasses are encouraged to provide a more
     * efficient implementation of this method.
     *
     * @param b     the buffer into which the data is read.
     * @param off   the start offset in array <code>b</code>
     *                   at which the data is written.
     * @param len   the maximum number of bytes to read.
     * @return the total number of bytes read into the buffer, or
     *             <code>-1</code> if there is no more data because the end of
     *             the stream has been reached.
     * @exception IOException  if an I/O error occurs.
     * @exception NullPointerException  if <code>b</code> is <code>null</code>.
     * @see java.io.InputStream#read()
     */
    public int read(byte[] b, int off, int len) throws IOException {
        if (closed)
            throw new IOException (
                   MessageService.getTextMessage(SQLState.LANG_STREAM_CLOSED));
        try {
            int ret = control.read(b, off, len, pos);
            if (ret != -1) {
                pos += ret;
                return ret;
            }
            return -1;
        } catch (StandardException se) {
            String state = se.getSQLState();
            if (state.equals(ExceptionUtil.getSQLStateFromIdentifier(
                                        SQLState.BLOB_POSITION_TOO_LARGE))) {
                return -1;
            } else if (state.equals(ExceptionUtil.getSQLStateFromIdentifier(
                                            SQLState.BLOB_INVALID_OFFSET))) {
                throw new ArrayIndexOutOfBoundsException(se.getMessage());
            } else {
                throw new IOException(se.getMessage());
            }
        }
    }

    /**
     * Closes this input stream and releases any system resources associated
     * with the stream.
     *
     * <p> The <code>close</code> method of <code>InputStream</code> does
     * nothing.
     *
     * @exception IOException  if an I/O error occurs.
     */
    public void close() throws IOException {
        closed = true;
    }

    /**
     * Reads the next byte of data from the input stream. The value byte is
     * returned as an <code>int</code> in the range <code>0</code> to
     * <code>255</code>. If no byte is available because the end of the stream
     * has been reached, the value <code>-1</code> is returned. This method
     * blocks until input data is available, the end of the stream is detected,
     * or an exception is thrown.
     *
     * <p> A subclass must provide an implementation of this method.
     *
     * @return the next byte of data, or <code>-1</code> if the end of the
     *             stream is reached.
     * @exception IOException  if an I/O error occurs.
     */
    public int read() throws IOException {
        if (closed)
            throw new IOException (
                   MessageService.getTextMessage (SQLState.LANG_STREAM_CLOSED));
        try {
            int ret = control.read(pos);
            if (ret != -1)
                pos += 1;
            return ret;
        } catch (StandardException se) {
            throw new IOException (se.getMessage());
        }
    }

    /**
     * Checks if underlying StreamControl has been updated.
     * @return if stream is modified since created
     */
    boolean isObsolete () {
        return updateCount != control.getUpdateCount();
    }
    
    /**
     * Reinitializes the stream and sets the current pointer to zero.
     */
    void reInitialize () {
        updateCount = control.getUpdateCount();
        pos = 0;
    }
    
    /**
     * Returns size of stream in bytes.
     * @return size of stream.
     */
    long length () throws IOException {
        return control.getLength();
    }
}
