/*

   Derby - Class org.apache.derby.client.am.BlobOutputStream

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


public class BlobOutputStream extends java.io.OutputStream {
    private Blob blob_;
    private long offset_;

    public BlobOutputStream(Blob blob, long offset) {
        blob_ = blob;
        offset_ = offset;
        
        /*
            offset_=1 while blob_.binaryString_.length - blob_.dataOffset_ = 0
            for a empty Blob hence check for offset_-1
         */
        if ((offset_-1) > (blob_.binaryString_.length - blob_.dataOffset_)) {
            throw new IndexOutOfBoundsException();
        }
    }

    public void write(int b) throws java.io.IOException {

        byte[] newbuf = new byte[(int) offset_ + blob_.dataOffset_];
        System.arraycopy(blob_.binaryString_, 0, newbuf, 0, (int) offset_ - 1 + blob_.dataOffset_);
        blob_.binaryString_ = newbuf;
        blob_.binaryString_[(int) offset_ + blob_.dataOffset_ - 1] = (byte) b;
        blob_.binaryStream_ = new java.io.ByteArrayInputStream(blob_.binaryString_);
        blob_.sqlLength_ = blob_.binaryString_.length - blob_.dataOffset_;
        offset_++;
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
        byte[] newbuf = new byte[(int) offset_ - 1 + len + blob_.dataOffset_];
        System.arraycopy(blob_.binaryString_, 0, newbuf, 0, (int) offset_ - 1 + blob_.dataOffset_);
        blob_.binaryString_ = newbuf;
        for (int i = 0; i < len; i++, offset_++) {
            blob_.binaryString_[(int) offset_ + blob_.dataOffset_ - 1] = b[off + i];
        }
        blob_.binaryStream_ = new java.io.ByteArrayInputStream(blob_.binaryString_);
        blob_.sqlLength_ = blob_.binaryString_.length - blob_.dataOffset_;
    }
}

