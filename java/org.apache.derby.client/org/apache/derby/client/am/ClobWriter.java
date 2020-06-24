/*

   Derby - Class org.apache.derby.client.am.ClobWriter

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
import java.io.Writer;
import org.apache.derby.shared.common.reference.SQLState;

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
class ClobWriter extends Writer {
    private final ClientClob clob_;
    private long offset_;

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    ClobWriter(ClientClob clob, long offset) throws SqlException {
        clob_ = clob;
        offset_ = offset;

//IC see: https://issues.apache.org/jira/browse/DERBY-2540
        if (offset_ - 1 > clob_.sqlLength()) {
            throw new SqlException(clob_.agent_.logWriter_, 
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                new ClientMessageId(SQLState.BLOB_INVALID_OFFSET), offset);
        }
    }

    public void write(int c) {
        StringBuffer sb = new StringBuffer(clob_.string_.substring(0, (int) offset_ - 1));
//IC see: https://issues.apache.org/jira/browse/DERBY-1245
//IC see: https://issues.apache.org/jira/browse/DERBY-1354
        sb.append((char)c);
//IC see: https://issues.apache.org/jira/browse/DERBY-2540
        updateClob(sb);
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
//IC see: https://issues.apache.org/jira/browse/DERBY-2540
        updateClob(sb);
    }


    public void write(String str) {
        StringBuffer sb = new StringBuffer(clob_.string_.substring(0, (int) offset_ - 1));
        sb.append(str);
        updateClob(sb);
    }


    public void write(String str, int off, int len) {
        StringBuffer sb = new StringBuffer(clob_.string_.substring(0, (int) offset_ - 1));
        sb.append(str.substring(off, off + len));
        updateClob(sb);
    }

    public void flush() {
    }

    public void close() throws IOException {
    }
    
    private void updateClob(StringBuffer sb) 
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        clob_.reInitForNonLocator(sb.toString());
        offset_ = clob_.string_.length() + 1;
    }
}

