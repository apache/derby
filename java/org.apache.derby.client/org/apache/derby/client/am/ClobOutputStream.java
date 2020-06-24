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

import java.io.IOException;
import java.io.OutputStream;


//IC see: https://issues.apache.org/jira/browse/DERBY-6125
class ClobOutputStream extends OutputStream {
    private ClientClob clob_;
    private long offset_;

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    ClobOutputStream(ClientClob clob, long offset) throws SqlException {
        clob_ = clob;
        offset_ = offset;
        
        /*
//IC see: https://issues.apache.org/jira/browse/DERBY-796
            offset_ starts from 1 while sqlLenth_=0
            in the case of a empty Clob hence check from
            offset_-1
         */
//IC see: https://issues.apache.org/jira/browse/DERBY-2540
        if ((offset_-1) > clob_.sqlLength()) {
            throw new IndexOutOfBoundsException();
        }
    }

    public void write(int b) throws IOException {
        byte[] newByte = new byte[1];
        newByte[0] = (byte)b;
        writeBytes(newByte);
    }

    public void write(byte b[], int off, int len) throws IOException {
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
//IC see: https://issues.apache.org/jira/browse/DERBY-2540
        writeBytes(newByte);
    }


    private void writeBytes(byte b[])  throws IOException
    {
        // Since this is an OutputStream returned by Clob.setAsciiStream 
        // use Ascii encoding when creating the String from bytes
//IC see: https://issues.apache.org/jira/browse/DERBY-1519
        String str = new String(b, "ISO-8859-1");
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        clob_.reInitForNonLocator(
                clob_.string_.substring(0, (int) offset_ - 1).concat(str));
        offset_ += b.length;
    }
}

