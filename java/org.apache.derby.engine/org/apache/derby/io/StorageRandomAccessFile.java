/*

   Derby - Class org.apache.derby.io.StorageRandomAccessFile

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.EOFException;
import java.io.IOException;

/**
 * This interface abstracts an object that implements reading and writing on a random access
 * file. It extends DataInput and DataOutput, so it implicitly contains all the methods of those
 * interfaces. Any method in this interface that also appears in the java.io.RandomAccessFile class
 * should behave as the java.io.RandomAccessFile method does.
 *<p>
 * Each StorageRandomAccessFile has an associated file pointer, a byte offset in the file. All reading and writing takes
 * place at the file pointer offset and advances it.
 *<p>
 * An implementation of StorageRandomAccessFile need not be thread safe. The database engine
 * single-threads access to each StorageRandomAccessFile instance. Two threads will not access the
 * same StorageRandomAccessFile instance at the same time.
 *<p>
 * @see <a href="http://java.sun.com/j2se/1.4.2/docs/api/java/io/RandomAccessFile.html">java.io.RandomAccessFile</a>
 */
public interface StorageRandomAccessFile extends DataInput, DataOutput
{

    /**
     * Closes this file.
     *
     * @exception IOException - if an I/O error occurs.
     */
    public void close() throws IOException;

    /**
     * Get the current offset in this file.
     *
     * @return the current file pointer. 
     *
     * @exception IOException - if an I/O error occurs.
     */
    public long getFilePointer() throws IOException;

    /**
     * Gets the length of this file.
     *
     * @return the number of bytes this file. 
     *
     * @exception IOException - if an I/O error occurs.
     */
    public long length() throws IOException;

    /**
     * Set the file pointer. It may be moved beyond the end of the file, but this does not change
     * the length of the file. The length of the file is not changed until data is actually written..
     *
     * @param newFilePointer the new file pointer, measured in bytes from the beginning of the file.
     *
     * @exception IOException - if newFilePointer is less than 0 or an I/O error occurs.
     */
    public void seek(long newFilePointer) throws IOException;

    /**
     * Sets the length of this file, either extending or truncating it.
     *<p>
     * If the file is extended then the contents of the extension are not defined.
     *<p>
     * If the file is truncated and the file pointer is greater than the new length then the file pointer
     * is set to the new length.
     *
     * @param newLength The new file length.
     *
     * @exception IOException If an I/O error occurs.
     */
    public void setLength(long newLength) throws IOException;
    
    /**
     * Force any changes out to the persistent store. If the database is to be transient, that is, if the database
     * does not survive a restart, then the sync method implementation need not do anything.
     *
     *
     * @exception java.io.SyncFailedException if a possibly recoverable error occurs.
     * @exception IOException If an IO error occurs.
     */
    public void sync() throws IOException;

    /**
     * Reads up to <code>len</code> bytes of data from this file into an
     * array of bytes. This method blocks until at least one byte of input
     * is available.
     * <p>
     *
     * @param b     the buffer into which the data is read.
     * @param off   the start offset in array <code>b</code>
     *                   at which the data is written.
     * @param len   the maximum number of bytes read.
     * @return the total number of bytes read into the buffer, or
     *             <code>-1</code> if there is no more data because the end of
     *             the file has been reached.
     * @exception IOException If the first byte cannot be read for any reason
     * other than end of file, or if the random access file has been closed, or
     * if some other I/O error occurs.
     * @exception NullPointerException If <code>b</code> is <code>null</code>.
     * @exception IndexOutOfBoundsException If <code>off</code> is negative,
     * <code>len</code> is negative, or <code>len</code> is greater than
     * <code>b.length - off</code>
     */
    public int read(byte[] b, int off, int len) throws IOException;

    /** Clone this file abstraction */
    public  StorageRandomAccessFile clone();
}
