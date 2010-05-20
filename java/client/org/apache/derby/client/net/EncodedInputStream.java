/*
    Derby - Class org.apache.derby.client.net.EncodedInputStream

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
package org.apache.derby.client.net;

import java.io.InputStream;
import java.io.Reader;
import java.io.OutputStreamWriter;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * Create an encoded stream from a <code>Reader</code>.
 *
 * This is an internal class, used to pass readers of characters as streams of
 * bytes. The characters will be represented according to the specified
 * encoding. It is up to the caller to ensure the specified encoding is
 * available, and in general only encodings available as default from Java 1.3
 * and up should be used.
 *
 * Currently, the encodings 'UTF8' and 'UTF-16BE' are used.
 * Streams are obtained by calling the static methods of this class,
 * for instance <code>createUTF8Stream</code>.
 */
public final class EncodedInputStream extends InputStream {

    /**
     * Create a UTF-8 encoded stream from the given <code>Reader</code>.
     *
     * @param reader the <code>Reader</code> to read characters from.
     * @return a byte-stream with UTF-8 encoded characters
     */
    public static EncodedInputStream createUTF8Stream(Reader reader) {
        return new EncodedInputStream(reader, 
                                      "UTF8",
                                      BUFFERED_CHAR_LEN,
                                      BUFFERED_CHAR_LEN*3);
    }

    /**
     * Create a UTF-16BE encoded stream from the given <code>Reader</code>.
     *
     * @param reader the <code>Reader</code> to read characters from.
     * @return a byte-stream with UTF-16BE encoded characters
     */
    static EncodedInputStream createUTF16BEStream(Reader reader) {
        return new EncodedInputStream(reader,
                                      "UTF-16BE",
                                      BUFFERED_CHAR_LEN,
                                      BUFFERED_CHAR_LEN*2);
    }
    
    private static final int BUFFERED_CHAR_LEN = 1024;
	private static final ByteArrayInputStream suspendMarker = new ByteArrayInputStream( new byte[ 0 ] );

    private Reader reader_;
    private final char[] decodedBuffer_;
    
    private OutputStreamWriter encodedStreamWriter_;
    private PublicBufferOutputStream encodedOutputStream_;
    
    private ByteArrayInputStream encodedInputStream_;
    
    /**
     * Create an encoded stream for the specified <code>Reader</code>.
     * 
     * @param reader the <code>Reader</code> to read characters from
     * @param encoding the encoding to use in the encoded stream
     * @param charBufferSize the size of the char buffer. This is the number
     *      of characters read at once from the <code>Reader</code>.
     * @param initialByteBufferSize the initial size of the byte buffer.
     *      holding the encoded bytes
     */
    private EncodedInputStream(Reader reader,
                               String encoding,
                               int charBufferSize,
                               int initialByteBufferSize) {
	
		reader_ = reader;
		decodedBuffer_ = new char[charBufferSize];

		encodedOutputStream_ = new PublicBufferOutputStream(
				initialByteBufferSize);
		
		try{
			encodedStreamWriter_ = new OutputStreamWriter(encodedOutputStream_,
                                                          encoding);
			
		}catch(UnsupportedEncodingException e){
			// Should never happen. It is up to the caller to ensure the
            // specified encoding is available.
            if (SanityManager.DEBUG) {
                SanityManager.THROWASSERT("Unavailable encoding specified: " +
                        encoding, e);
            }
		}
	
		encodedInputStream_ = suspendMarker;
	
    }


    private ByteArrayInputStream reEncode(Reader reader) 
		throws IOException
    {
	
		int count;
		do{
			count = reader.read(decodedBuffer_, 0, decodedBuffer_.length);
			
		}while(count == 0);
			
		if(count < 0)
			return null;
	
		encodedOutputStream_.reset();
		encodedStreamWriter_.write(decodedBuffer_,0,count);
		encodedStreamWriter_.flush();

		int encodedLength = encodedOutputStream_.size();
	
		return new ByteArrayInputStream(encodedOutputStream_.getBuffer(),
										0,
										encodedLength);
    }
    
    
    public int available() 
		throws IOException {
		
		if(encodedInputStream_ == suspendMarker)
			encodedInputStream_ = reEncode(reader_);

		if(encodedInputStream_ == null){
			return 0;
		}

		return encodedInputStream_.available();
	
    }
    

    public void close() 
		throws IOException {
	
		if(encodedInputStream_ != null ){
			encodedInputStream_.close();
			encodedInputStream_ = null;
		}

		if(reader_ != null ){
			reader_.close();
			reader_ = null;
		}

		if(encodedStreamWriter_ != null){
			encodedStreamWriter_.close();
			encodedStreamWriter_ = null;
		}
	
    }
    
    
    public int read() 
		throws IOException {
		
		if(encodedInputStream_ == suspendMarker)
			encodedInputStream_ = reEncode(reader_);

		if(encodedInputStream_ == null){
			return -1;
		}
	
		int c = encodedInputStream_.read();

		if(c > -1){
			return c;
	    
		}else{
			encodedInputStream_ = reEncode(reader_);
	    
			if(encodedInputStream_ == null){
				return -1;
			}
	    
			return encodedInputStream_.read();

		}
	
    }
    
    
    protected void finalize() throws IOException {
		close();
    }
    
    
    static class PublicBufferOutputStream extends ByteArrayOutputStream{
	
		PublicBufferOutputStream(int size){
			super(size);
		}

		public byte[] getBuffer(){
			return buf;
		}
	
    }
}
