/*
 *
 * Derby - Class ClobUpdateableReader
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package org.apache.derby.impl.jdbc;

import java.io.IOException;
import java.io.Reader;

/**
 * ClobUpdateableReader is used to create Reader over InputStream. This class is
 * aware that underlying stream can be modified and reinitializes itsef if it 
 * detects any change in stream hence invalidating the cache so the changes are 
 * reflected immidiatly.
 */

final class ClobUpdateableReader extends Reader {
    
    private Reader streamReader;
    private long pos;
    private LOBInputStream stream;
    private ConnectionChild conChild;
    
    /**
     * Constructs a Reader over a LOBInputStream.
     * @param stream 
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
     * @param skip 
     * @throws IOException
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
