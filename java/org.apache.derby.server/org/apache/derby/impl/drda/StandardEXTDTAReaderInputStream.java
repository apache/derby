/*
   Derby - Class org.apache.derby.impl.drda.StandardEXTDTAReaderInputStream

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
package org.apache.derby.impl.drda;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Implementation of InputStream which get EXTDTA from the DDMReader.
 * This class can be used to stream LOBs from Network client to the
 * Network server.
 */
final class StandardEXTDTAReaderInputStream extends EXTDTAReaderInputStream 
{
    /**
     * Constructor.
     *
     * @param reader The reader to get data from
     * @param readStatusByte whether or not to read the trailing Derby-specific
     *      EXTDTA stream status byte
     * @exception DRDAProtocolException if thrown while initializing current 
     *                                  buffer.
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-2017
    StandardEXTDTAReaderInputStream(final DDMReader reader,
                                    boolean readStatusByte)
        throws DRDAProtocolException
    {
        super(false, readStatusByte);
        this.reader = reader;
        // Exclude the status byte in the byte count.
        if (readStatusByte) {
            this.remainingBytes = reader.getDdmLength() -1;
        } else {
            this.remainingBytes = reader.getDdmLength();
        }
        this.length = remainingBytes;
        // Make sure we read the product specific extension byte off the wire.
        // It will be read here if the value fits into a single DSS.
        this.currentBuffer = 
            reader.readLOBInitStream(remainingBytes + (readStatusByte ? 1 : 0));
    }

    /**
     * Reads the next byte of data from the input stream.
     * 
     * <p> This subclass of InputStream implements this method by reading
     * the next byte from the current buffer. If there is more data,
     * it will be requested a new buffer from the DDMReader.
     *
     * @return     the next byte of data, or <code>-1</code> if the end of the
     *             stream is reached.
     * @exception  IOException  if an I/O error occurs.
     * @see        java.io.InputStream#read()
     */
    public final int read() 
//IC see: https://issues.apache.org/jira/browse/DERBY-2017
            throws IOException {
        // Reuse the other read method for simplicity.
        byte[] b = new byte[1];
        int read = read(b);
        return (read == 1 ? b[0] : -1);
    }

    /**
     * Reads up to <code>len</code> bytes of data from the input stream into
     * an array of bytes.  An attempt is made to read as many as
     * <code>len</code> bytes, but a smaller number may be read, possibly
     * zero. The number of bytes actually read is returned as an integer.
     *
     * This subclass implements this method by calling this method on the 
     * current buffer, which is an instance of ByteArrayInputStream. If the
     * current buffer does not have any data, it will be requested a new
     * buffer from the DDMReader.
     *
     * @param      b     the buffer into which the data is read.
     * @param      off   the start offset in array <code>b</code>
     *                   at which the data is written.
     * @param      len   the maximum number of bytes to read.
     * @return     the total number of bytes read into the buffer, or
     *             <code>-1</code> if there is no more data because the end of
     *             the stream has been reached.
     * @exception  IOException  if an I/O error occurs.
     * @exception  NullPointerException  if <code>b</code> is <code>null</code>.
     * @see        java.io.InputStream#read(byte[], int, int)
     */
    public final int read(final byte[] b,
                          final int off,
                          int len) 
        throws IOException
    {
        if (remainingBytes <= 0) {
            return -1;
        }
        // Adjust length to avoid reading the trailing status byte.
//IC see: https://issues.apache.org/jira/browse/DERBY-2017
        len = (int)Math.min(remainingBytes, (long)len);
        int val = currentBuffer.read(b, off, len);
        if (val < 0) {
            nextBuffer();
            val = currentBuffer.read(b, off, len);
        }
        // If we are reading the last data byte, check the status byte.
        if (readStatusByte && val == remainingBytes) {
            if (currentBuffer.available() == 0) {
                // Fetch the last buffer (containing only the status byte).
                nextBuffer();
            }
            checkStatus(currentBuffer.read());
            // Sanity check.
            if (currentBuffer.read() != -1) {
                throw new IllegalStateException(
                        "Remaining bytes in buffer after status byte");
            }
        }
        remainingBytes -= val;
        return val;
    }

    /**
     * Returns the number of bytes that can be read (or skipped over) from
     * this input stream without blocking by the next caller of a method for
     * this input stream.  
     *
     * <p> This subclass implements this method by calling available on 
     *     the current buffer, which is a ByteInputStreamReader.
     *
     * @return     the number of bytes that can be read from this input stream
     *             without blocking.     
     */
    public final int available() 
    {
        if (remainingBytes <= 0) {
            return 0;
        }
        int inBuffer = currentBuffer.available();
        // Adjust for the status byte if required.
//IC see: https://issues.apache.org/jira/browse/DERBY-2017
        if (readStatusByte && inBuffer > remainingBytes) {
            inBuffer--;
        }
        return inBuffer;
    }

    /**
     * Returns the number of bytes returned by this stream.
     * <p>
     * The length includes data which has been already read at the invocation
     * time, but doesn't include any meta data (like the Derby-specific
     * EXTDTA status byte).
     *
     * @return The number of bytes that will be returned by this stream.
     */
    final long getLength() 
    {
        return length;
    }
    
    /**
     * Fetches the next buffer.
     *
     * @throws IOException if fetching the buffer fails
     */
    private void nextBuffer()
//IC see: https://issues.apache.org/jira/browse/DERBY-2017
            throws IOException {
        // Make sure we read the status byte off the wire if it was sent.
        long wireBytes = readStatusByte ? remainingBytes +1 : remainingBytes;
        currentBuffer = reader.readLOBContinuationStream(wireBytes);
    }

    /**
     * Cleans up and closes the stream.
     */
    protected void onClientSideStreamingError() {
        // Clean state and return -1 on subsequent calls.
        // The status byte is the last byte, so no need to drain the source.
        currentBuffer = null;
        remainingBytes = -1;
    }

    /** Length of stream */
    private final long length;
    
    /** DDMReader. Used to get more data. */
    private final DDMReader reader;

    /** Remaining bytes in stream */
    private long remainingBytes;

    /** Current data buffer */
    private ByteArrayInputStream currentBuffer;

}
