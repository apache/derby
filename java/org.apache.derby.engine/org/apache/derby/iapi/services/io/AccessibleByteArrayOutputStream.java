/*

   Derby - Class org.apache.derby.iapi.services.io.AccessibleByteArrayOutputStream

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

package org.apache.derby.iapi.services.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * This allows us to get to the byte array to go back and
 * edit contents or get the array without having a copy made.
 <P>
   Since a copy is not made, users must be careful that no more
   writes are made to the stream if the array reference is handed off.
 * <p>
 * Users of this must make the modifications *before* the
 * next write is done, and then release their hold on the
 * array.
   
 */
public class AccessibleByteArrayOutputStream extends ByteArrayOutputStream {

	public AccessibleByteArrayOutputStream() {
		super();
	}

	public AccessibleByteArrayOutputStream(int size) {
		super(size);
	}

	/**
	 * The caller promises to set their variable to null
	 * before any other calls to write to this stream are made.
	   Or promises to throw away references to the stream before
	   passing the array reference out of its control.

       @return a byte array
	 */
	public byte[] getInternalByteArray() {
		return buf;
	}
    
    /**
     * Read the complete contents of the passed input stream
     * into this byte array.
     *
     * @param in The stream to read
     * @throws IOException on error
     */
    public void readFrom(InputStream in) throws IOException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-552
       byte[] buffer = new byte[8192];
        
        for(;;)
        {
            int read = in.read(buffer, 0, buf.length);
            if (read == -1)
                break;
            write(buffer, 0, read);
        }
    }
    
    /**
     * Return an InputStream that wraps the valid byte array.
     * Note that no copy is made of the byte array from the
     * input stream, it is up to the caller to ensure the correct
     * co-ordination.
     *
     * @return an InputStream wrapping the byte array
     */
    public InputStream getInputStream()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-5090
        return new ByteArrayInputStream(buf, 0, count);
    }
    
    /**
     * Copy an InputStream into an array of bytes and return
     * an InputStream against those bytes. The input stream
     * is copied until EOF is returned. This is useful to provide
     * streams to applications in order to isolate them from
     * Derby's internals.
     * 
     * @param in InputStream to be copied
     * @param bufferSize Initial size of the byte array
     * 
     * @return InputStream against the raw data.
     * 
     * @throws IOException Error reading the stream
     */
    public static InputStream copyStream(InputStream in, int bufferSize)
         throws IOException
    {
        AccessibleByteArrayOutputStream raw =
            new AccessibleByteArrayOutputStream(bufferSize);
        raw.readFrom(in);
        return raw.getInputStream();
    }
}
