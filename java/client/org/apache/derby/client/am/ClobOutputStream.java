/*

   Derby - Class org.apache.derby.client.am.ClobOutputStream

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

package org.apache.derby.client.am;


public class ClobOutputStream extends java.io.OutputStream {
    private Clob clob_;
    private long offset_;

    public ClobOutputStream(Clob clob, long offset) throws SqlException {
        clob_ = clob;
        offset_ = offset;
        
        /*
            offset_ starts from 1 while sqlLenth_=0
            in the case of a empty Clob hence check from
            offset_-1
         */
        if ((offset_-1) > clob_.sqlLength()) {
            throw new IndexOutOfBoundsException();
        }
    }

    public void write(int b) throws java.io.IOException {
        byte[] newByte = new byte[1];
        newByte[0] = (byte)b;
        writeBytes(newByte);
    }


    public void write(byte b[], int off, int len) throws java.io.IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if ((off < 0) || (off > b.length) || (len < 0) ||
                ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }

        byte[] newByte = new byte[len];
        System.arraycopy(b, off, newByte, 0, len);
        writeBytes(newByte);
    }


    private void writeBytes(byte b[])  throws java.io.IOException
    {
        // Since this is an OutputStream returned by Clob.setAsciiStream 
        // use Ascii encoding when creating the String from bytes
        String str = new String(b, "ISO-8859-1");
        clob_.string_ = clob_.string_.substring(0, (int) offset_ - 1);
        clob_.string_ = clob_.string_.concat(str);
        clob_.asciiStream_ = new java.io.StringBufferInputStream(clob_.string_);
        clob_.unicodeStream_ 
            = new java.io.StringBufferInputStream(clob_.string_);
        clob_.characterStream_ = new java.io.StringReader(clob_.string_);
        clob_.setSqlLength(clob_.string_.length());
        offset_ += b.length;
    }
}

