/*

   Derby - Class org.apache.derby.client.am.ClobWriter

   Copyright (c) 2001, 2005 The Apache Software Foundation or its licensors, where applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*/

package org.apache.derby.client.am;


public class ClobWriter extends java.io.Writer {
    private Clob clob_;
    private long offset_;

    public ClobWriter() {
    }

    public ClobWriter(Clob clob, long offset) throws SqlException {
        clob_ = clob;
        offset_ = offset;

        if (offset_ - 1 > clob_.sqlLength_) {
            throw new SqlException(clob_.agent_.logWriter_, "Invalid position: " + offset);
        }
    }

    public void write(int c) {
        StringBuffer sb = new StringBuffer(clob_.string_.substring(0, (int) offset_ - 1));
        sb.append(c);
        clob_.string_ = sb.toString();
        clob_.asciiStream_ = new java.io.StringBufferInputStream(clob_.string_);
        clob_.unicodeStream_ = new java.io.StringBufferInputStream(clob_.string_);
        clob_.characterStream_ = new java.io.StringReader(clob_.string_);
        clob_.sqlLength_ = clob_.string_.length();
        offset_ = clob_.sqlLength_ + 1;
    }

    public void write(char cbuf[], int off, int len) {
        if ((off < 0) || (off > cbuf.length) || (len < 0) ||
                ((off + len) > cbuf.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }
        StringBuffer sb = new StringBuffer(clob_.string_.substring(0, (int) offset_ - 1));
        sb.append(cbuf, off, len);
        clob_.string_ = sb.toString();
        clob_.asciiStream_ = new java.io.StringBufferInputStream(clob_.string_);
        clob_.unicodeStream_ = new java.io.StringBufferInputStream(clob_.string_);
        clob_.characterStream_ = new java.io.StringReader(clob_.string_);
        clob_.sqlLength_ = clob_.string_.length();
        offset_ = clob_.sqlLength_ + 1;
    }


    public void write(String str) {
        StringBuffer sb = new StringBuffer(clob_.string_.substring(0, (int) offset_ - 1));
        sb.append(str);
        clob_.string_ = sb.toString();
        clob_.asciiStream_ = new java.io.StringBufferInputStream(clob_.string_);
        clob_.unicodeStream_ = new java.io.StringBufferInputStream(clob_.string_);
        clob_.characterStream_ = new java.io.StringReader(clob_.string_);
        clob_.sqlLength_ = clob_.string_.length();
        offset_ = clob_.sqlLength_ + 1;
    }


    public void write(String str, int off, int len) {
        StringBuffer sb = new StringBuffer(clob_.string_.substring(0, (int) offset_ - 1));
        sb.append(str.substring(off, off + len));
        clob_.string_ = sb.toString();
        clob_.asciiStream_ = new java.io.StringBufferInputStream(clob_.string_);
        clob_.unicodeStream_ = new java.io.StringBufferInputStream(clob_.string_);
        clob_.characterStream_ = new java.io.StringReader(clob_.string_);
        clob_.sqlLength_ = clob_.string_.length();
        offset_ = clob_.sqlLength_ + 1;
    }

    public void flush() {
    }

    public void close() throws java.io.IOException {
    }
}

