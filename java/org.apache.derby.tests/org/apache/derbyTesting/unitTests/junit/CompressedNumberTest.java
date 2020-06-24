/*

   Derby - Class org.apache.derbyTesting.unitTests.junit.CompressedNumberTest

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

package org.apache.derbyTesting.unitTests.junit;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.derby.iapi.services.io.ArrayInputStream;
import org.apache.derby.iapi.services.io.ArrayOutputStream;
import org.apache.derby.iapi.services.io.CompressedNumber;

import junit.framework.TestCase;

/**
 * Test case for CompressedNumber.
 */
public class CompressedNumberTest extends TestCase {
    private static byte[] holder = new byte[8];
    private static  ArrayOutputStream aos = new ArrayOutputStream(holder);
    private static  DataOutput out = new DataOutputStream(aos);

    private static  ArrayInputStream ais = new ArrayInputStream(holder);
    private static  DataInput in = new DataInputStream(ais);
    private static  InputStream in_stream = ais;

    /**
     * Public constructor required for running test as stand alone JUnit.
     *
     * @param name
     *            name to present this test case.
     */
    public CompressedNumberTest(String name) {
        super(name);
    }

    public void testLong() throws IOException{
        long[] dataToTest = {0, 1, 2,
                0x3fff - 2, 0x3fff - 1,
                0x3fff,
                0x3fff + 1, 0x3fff + 2,
                0x3fffffff - 4, 0x3fffffff - 3, 0x3fffffff - 2, 0x3fffffff - 1,
                0x3fffffff,
                0x3fffffff + 1, 0x3fffffff + 2, 0x3fffffff + 3, 0x3fffffff + 4,
                0x70000000 - 2, 0x70000000 - 1,
                0x70000000,
                0x70000000 + 1, 0x70000000 + 2,
                Long.MAX_VALUE - 2, Long.MAX_VALUE - 1,
                Long.MAX_VALUE,
                };
        int[] length = {2, 2, 2,
//IC see: https://issues.apache.org/jira/browse/DERBY-3742
                2, 2,
                2,
                4, 4,
                4, 4, 4, 4,
                4,
                8, 8, 8, 8,
                8, 8,
                8,
                8, 8,
                8, 8,
                8,
        };

        for(int i = 0; i < dataToTest.length; i++){
            checkLong(dataToTest[i], length[i]);
        }
    }

    public void testLongWidely() throws IOException{
        for (long l = 0; l < 0xf0000; l++){
            if(l <= 0x3fff)
                checkLong(l, 2);
            else if(l <= 0x3fffffff)
                checkLong(l, 4);
            else
                checkLong(l, 8);
        }
    }

    public void testInt() throws IOException{
        int[] dataToTest = {0, 1, 2,
                0x3f - 4, 0x3f - 3, 0x3f - 2, 0x3f - 1,
                0x3f,
                0x3f + 1, 0x3f + 2, 0x3f + 3, 0x3f + 4,
                0x3f80 - 4, 0x3f80 - 3, 0x3f80 - 2, 0x3f80 - 1,
                0x3f80,
                0x3f80 + 1, 0x3f80 + 2, 0x3f80 + 3, 0x3f80 + 4,
                0x3fff - 4, 0x3fff - 3, 0x3fff - 2, 0x3fff - 1,
                0x3fff,
                0x3fff + 1, 0x3fff + 2, 0x3fff + 3, 0x3fff + 4,
                Integer.MAX_VALUE - 4, Integer.MAX_VALUE - 3,
                Integer.MAX_VALUE - 2, Integer.MAX_VALUE - 1,
                Integer.MAX_VALUE,
                };
        int[] length = { 1, 1, 1,
//IC see: https://issues.apache.org/jira/browse/DERBY-3742
                1, 1, 1, 1,
                1,
                2, 2, 2, 2,
                2, 2, 2, 2,
                2,
                2, 2, 2, 2,
                2, 2, 2, 2,
                2,
                4, 4, 4, 4,
                4, 4,
                4, 4,
                4,
        };

        for(int i = 0; i < dataToTest.length; i++){
            checkInt(dataToTest[i], length[i]);
        }
    }

    public void testIntWidely() throws IOException{
        for (int i = 0; i < 0xf0000; i++){
            if(i <= 0x3f)
                checkInt(i, 1);
            else if(i <= 0x3fff)
                checkInt(i, 2);
            else
                checkInt(i, 4);
        }

         //takes 30 minutes to run.
//         for (int i = 0; i < Integer.MAX_VALUE; i++) {
//             if(i <= 0x3f)
//                 checkInt(i, 1);
//             else if(i <= 0x3fff)
//                 checkInt(i, 2);
//             else
//                 checkInt(i, 4);
//         }
    }

    /**
     * Check whether CompressedNumber can work well on integer passed.
     *
     * @param i
     *            the integer to be checked.
     * @param expectedLength
     *            the length expected of i after compressed.
     * @throws IOException
     */
    private void checkInt(int i, int expectedLength) throws IOException {
        aos.setPosition(0);
        int length = CompressedNumber.writeInt(out, i);
        assertEquals("Invalid length after compressed", expectedLength, length);

        assertEquals("MISMATCH written bytes", length, aos.getPosition());

        assertEquals("MISMATCH sizeInt() bytes", length,
                     CompressedNumber.sizeInt(i));

        ais.setPosition(0);
        assertEquals("MISMATCH value readInt(DataInput)", i,
                     CompressedNumber.readInt(in));

        ais.setPosition(0);
        assertEquals("MISMATCH value readInt(DataInput)", i,
                     ais.readCompressedInt());

        ais.setPosition(0);
        assertEquals("MISMATCH value in readInt(InputStream)", i,
                     CompressedNumber.readInt(in_stream));

        assertEquals("MISMATCH frome readInt(byte[], offset)", i,
                     CompressedNumber.readInt(holder, 0));
    }


    /**
     * Check whether CompressedNumber can work well on long number passed.
     *
     * @param l
     *            the long number to be checked.
     * @param expectedLength
     *            the length expected of l after compressed.
     * @throws IOException
     */
    private void checkLong(long l, int expectedLength) throws IOException {
        aos.setPosition(0);
        int length = CompressedNumber.writeLong(out, l);
        assertEquals("Invalid length after compressed", expectedLength, length);
//IC see: https://issues.apache.org/jira/browse/DERBY-3742
//IC see: https://issues.apache.org/jira/browse/DERBY-3742

        assertEquals("MISMATCH written bytes", length, aos.getPosition());

        assertEquals("MISMATCH sizeLong() bytes", length,
                     CompressedNumber.sizeLong(l));

        assertEquals("MISMATCH in readLong(byte[], offset) value", l,
                     CompressedNumber.readLong(holder, 0));

        ais.setPosition(0);
        assertEquals("MISMATCH value in readLong(InputStream)", l,
                     CompressedNumber.readLong(in_stream));

        ais.setPosition(0);
        assertEquals("MISMATCH value in readLong(InputStream)", l, ais
                .readCompressedLong());

        ais.setPosition(0);
        assertEquals("MISMATCH value in readLong(DataInput)", l,
                     CompressedNumber.readLong(in));
    }
}
