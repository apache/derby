/*
 
Derby - Class org.apache.derby.impl.drda.ReEncodedInputStream

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
package org.apache.derby.impl.drda;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;

/**
 *
 * ReEncodedInputStream passes
 * stream from Reader, which is stream of decoded style, 
 * to user of this subclass of InputStream, which is stream of encoded style.
 *
 * The encoding of stream passed to user is limited to UTF8.
 *
 * This class will be used to pass stream, which is served as a Reader,
 * as a InputStream of a arbitrary encoding.
 *
 */
public class ReEncodedInputStream extends InputStream {

    private static final int BUFFERED_CHAR_LEN = 1024;

    private Reader reader_;
    private char[] decodedBuffer_;
    
    private OutputStreamWriter encodedStreamWriter_;
    private PublicBufferOutputStream encodedOutputStream_;
    
    private ByteArrayInputStream encodedInputStream_;
    
    public ReEncodedInputStream(Reader reader) 
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
    throws IOException {
    
    reader_ = reader;
    decodedBuffer_ = new char[BUFFERED_CHAR_LEN];

    encodedOutputStream_ = new PublicBufferOutputStream( BUFFERED_CHAR_LEN * 3 );
    encodedStreamWriter_ = new OutputStreamWriter(encodedOutputStream_,"UTF8");
    
    encodedInputStream_ = reEncode(reader_);
    
    }


    private ByteArrayInputStream reEncode(Reader reader) 
    throws IOException
    {
    
        int count;
//IC see: https://issues.apache.org/jira/browse/DERBY-1396
        do{
            count = reader.read(decodedBuffer_, 0, BUFFERED_CHAR_LEN);
            
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
    
    
    //
    // This method in java.lang.Object was deprecated as of build 167
    // of JDK 9. See DERBY-6932.
    //
    @SuppressWarnings("deprecation")
    protected void finalize() throws IOException {
    close();
    }
    
    
    private static class PublicBufferOutputStream extends ByteArrayOutputStream{
    
    PublicBufferOutputStream(int size){
        super(size);
    }

    public byte[] getBuffer(){
        return buf;
    }
    
    }

}


