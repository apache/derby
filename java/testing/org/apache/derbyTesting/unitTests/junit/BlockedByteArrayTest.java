/*

   Derby - Class org.apache.derbyTesting.unitTests.junit.BlockedByteArrayTest

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

package org.apache.derbyTesting.unitTests.junit;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derby.impl.io.vfmem.BlockedByteArray;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetStream;
import org.apache.derbyTesting.junit.BaseTestCase;

/**
 * Basic tests of the {@code BlockedByteArrayTest}.
 */
public class BlockedByteArrayTest
        extends BaseTestCase {

    public BlockedByteArrayTest(String name) {
        super(name);
    }

    public void testLengthNoInitialBlocksWriteSingleByte() {
        BlockedByteArray src = new BlockedByteArray();
        assertEquals(0, src.length());
        src.writeByte(0, (byte)1);
        assertEquals(1, src.length());
        for (int i=0; i < 66*1024; i++) {
            src.writeByte(1 + i, (byte)i);
            assertEquals(i +2, src.length());
        }
    }

    public void testLengthNoInitialBlocksWriteMultipleBytes4K() {
        BlockedByteArray src = new BlockedByteArray();
        byte[] buf = new byte[4*1024];
        Arrays.fill(buf, (byte)1);
        src.writeBytes(0, buf, 0, buf.length);
        assertEquals(buf.length, src.length());
        Arrays.fill(buf, (byte)2);
        src.writeBytes(buf.length, buf, 0, buf.length);
        assertEquals(2 * buf.length, src.length());
        src.writeByte(69, (byte)8);
        assertEquals(2 * buf.length, src.length());
    }

    public void testLengthNoInitialBlocksWriteMultipleBytes4KPlussAFew() {
        BlockedByteArray src = new BlockedByteArray();
        byte[] buf = new byte[4*1024+37];
        Arrays.fill(buf, (byte)1);
        src.writeBytes(0, buf, 0, buf.length);
        assertEquals(buf.length, src.length());
        Arrays.fill(buf, (byte)2);
        src.writeBytes(buf.length, buf, 0, buf.length);
        assertEquals(2 * buf.length, src.length());
        src.writeByte(54, (byte)7);
        assertEquals(2 * buf.length, src.length());
    }

    public void testReadArray()
            throws IOException {
        int size = 65*1024;
        BlockedByteArray src = createBlockedByteArray(size);
        byte[] buf = new byte[4*1024];
        int read = 0;
        while (read < size) {
            read += src.read(read, buf, 0, buf.length);
        }
        src = createBlockedByteArray(size);
        buf = new byte[2567];
        read = 0;
        while (read < size) {
            read += src.read(read, buf, 0, buf.length);
        }
        src = createBlockedByteArray(size);
        buf = new byte[16*1024];
        read = 0;
        while (read < size) {
            read += src.read(read, buf, 0, buf.length);
        }
    }

    public void testReadSingle()
            throws IOException {
        int size = 65*1024;
        BlockedByteArray src = createBlockedByteArray(size);
        int read = 0;
        while (src.read(read) != -1) {
            read++;
        }
    }

    public void testLength()
            throws IOException {
        BlockedByteArray src = createBlockedByteArray(0);
        assertEquals(0L, src.length());
        src.writeByte(0L, (byte)1);
        assertEquals(1L, src.length());
        src.writeByte(0L, (byte)1);
        assertEquals(1L, src.length());
        src.writeByte(9L, (byte)2);
        assertEquals(10L, src.length());
        byte[] bytes = new byte[4096];
        Arrays.fill(bytes, (byte)7);
        src.writeBytes(0L, bytes, 0, bytes.length);
        assertEquals(bytes.length, src.length());
        src.writeBytes(bytes.length, bytes, 0, bytes.length);
        assertEquals(2*bytes.length, src.length());

        // Test setLength
        src.setLength(55555);
        assertEquals(55555, src.length());
        src.setLength(44444);
        assertEquals(44444, src.length());
    }

    /**
     * Performs a series of capacity changes.
     *
     * @throws IOException if something goes wrong
     */
    public void testCapacityGrowth()
            throws IOException {
        BlockedByteArray src = createBlockedByteArray(0);
        src.setLength(1*1024*1024); // 1 MB
        src.setLength(10*1024*1024); // 10 MB
        src.setLength(5*1024*1024); // 5 MB
        src.setLength(7*1024*1024); // 7 MB
        assertEquals(7*1024*1024L, src.length());
        src.setLength(0); // 0 bytes
        assertEquals(0L, src.length());
        src.setLength(39*1024*1024); // 39 MB
        src.setLength(39*1024*1024+1); // 39 MB +1 B
        assertEquals(39*1024*1024+1L, src.length());
        src.setLength(39*1024*1024); // 39 MB
        assertEquals(39*1024*1024L, src.length());
        src.setLength(39*1024*1024); // 39 MB
        assertEquals(39*1024*1024L, src.length());
        src.setLength(-1); // Invalid value - causes array to be truncated.
        assertEquals(0L, src.length());
    }

    public static Test suite() {
        return new TestSuite(BlockedByteArrayTest.class);
    }

    /**
     * Creates a blocked byte array and fills it with data.
     *
     * @param length requested length
     * @return A filled blocked byte array.
     * @throws IOException if reading from the source fails
     */
    private BlockedByteArray createBlockedByteArray(long length)
            throws IOException {
        BlockedByteArray data = new BlockedByteArray();
        InputStream src = new LoopingAlphabetStream(length);
        byte[] buf = new byte[4*1024];
        long pos = 0;
        while (pos < length) {
            int readFromSrc = src.read(buf);
            pos += data.writeBytes(pos, buf, 0, readFromSrc);
        }
        return data;
    }
}
