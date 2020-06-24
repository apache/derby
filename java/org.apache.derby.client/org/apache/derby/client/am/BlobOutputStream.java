/*

   Derby - Class org.apache.derby.client.am.BlobOutputStream

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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;


//IC see: https://issues.apache.org/jira/browse/DERBY-6125
class BlobOutputStream extends OutputStream {
    private ClientBlob blob_;
    private long offset_;

    BlobOutputStream(ClientBlob blob, long offset) {
        blob_ = blob;
        offset_ = offset;
        
        /*
//IC see: https://issues.apache.org/jira/browse/DERBY-796
            offset_=1 while blob_.binaryString_.length - blob_.dataOffset_ = 0
            for a empty Blob hence check for offset_-1
         */
        if ((offset_-1) > (blob_.binaryString_.length - blob_.dataOffset_)) {
            throw new IndexOutOfBoundsException();
        }
    }

    public void write(int b) throws IOException
    {
        byte ba[] = {(byte )b};
        writeX(ba, 0, 1);
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
//IC see: https://issues.apache.org/jira/browse/DERBY-2540
        writeX(b, off, len);
    }

    private void writeX(byte b[], int off, int len) 
    {
        byte[] newbuf = new byte[(int) offset_ - 1 + len + blob_.dataOffset_];
        System.arraycopy(blob_.binaryString_, 0, 
                         newbuf, 0, (int )offset_ - 1 + blob_.dataOffset_);
        blob_.binaryString_ = newbuf;
        for (int i = 0; i < len; i++, offset_++) {
            blob_.binaryString_[(int )offset_ + blob_.dataOffset_ - 1] 
                = b[off + i];
        }
        blob_.binaryStream_ 
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            = new ByteArrayInputStream(blob_.binaryString_);
        blob_.setSqlLength(blob_.binaryString_.length - blob_.dataOffset_);
    }
}
