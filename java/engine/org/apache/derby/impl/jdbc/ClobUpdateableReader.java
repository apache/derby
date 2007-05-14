/*

   Derby - Class org.apache.derby.impl.jdbc.ClobUpdateableReader

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
import java.io.Reader;

/**
 * <code>ClobUpdateableReader</code> is used to create a <code>Reader</code>
 * over a <code>LOBInputStream</code>.
 * <p>
 * This class is aware that the underlying stream can be modified and
 * reinitializes itself if it detects any change in the stream. This
 * invalidates the cache so the changes are reflected immediately.
 *
 * @see LOBInputStream
 */
final class ClobUpdateableReader extends Reader {
    
    /** Reader accessing the Clob data and doing the work. */
    private Reader streamReader;
    /** Character position of this reader. */
    private long pos;
    /** Underlying stream of byte data. */
    private LOBInputStream stream;
    /** Connection object used to obtain synchronization-object. */
    private ConnectionChild conChild;
    
    /**
     * Constructs a <code>Reader</code> over a <code>LOBInputStream</code>.
     * @param stream underlying stream of byte data
     * @param conChild a connection object used to obtain synchronization-object
     * @throws IOException
     */
    ClobUpdateableReader (LOBInputStream stream, ConnectionChild conChild)
                                                        throws IOException {
        this.conChild = conChild;
        this.stream = stream;
        init (0);
    }
        
    /**
     * Reads chars into the cbuf char array. Changes made in uderlying storage 
     * will be reflected immidiatly from the corrent position.
     * @param cbuf buffer to read into
     * @param off offet of the cbuf array to start writing read chars
     * @param len number of chars to be read
     * @return number of bytes read
     * @throws IOException
     */
    public int read(char[] cbuf, int off, int len) throws IOException {
        if (stream.isObsolete()) {
            stream.reInitialize();
            init (pos);
        }
        int ret = streamReader.read (cbuf, off, len);
        if (ret >= 0) {
            pos += ret;
        }
        return ret;
    }

    /**
     * Closes the reader.
     * @throws IOException
     */
    public void close() throws IOException {
        streamReader.close();
    }
    
    /**
     * Initializes the streamReader and skips to the given position.
     * @param skip number of characters to skip to reach initial position
     * @throws IOException if a streaming error occurs
     */
    private void init(long skip) throws IOException {
        streamReader = new UTF8Reader (stream, 0, stream.length (), 
                                        conChild, 
                                conChild.getConnectionSynchronization());
        long remainToSkip = skip;
        while (remainToSkip > 0) {
            long skipBy = streamReader.skip(remainToSkip);
            remainToSkip -= skipBy;
        }
        pos = skip;
    }    
}
