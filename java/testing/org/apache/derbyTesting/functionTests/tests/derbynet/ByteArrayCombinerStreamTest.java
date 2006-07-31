/*
    Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.ByteArrayCombinerStreamTest

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
package org.apache.derbyTesting.functionTests.tests.derbynet;

import org.apache.derbyTesting.functionTests.util.BaseTestCase;

import org.apache.derby.client.am.ByteArrayCombinerStream;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Test functionality of <code>ByteArrayCombinerStream</code>.
 */
public class ByteArrayCombinerStreamTest
    extends BaseTestCase {

    private static final byte[] defaultArray = {
            65,66,67,68,69,70,71,72,73,74,75,76,
            77,78,79,80,81,82,83,84,85,86,87,88};

    private ByteArrayCombinerStream combiner;
    
    public ByteArrayCombinerStreamTest(String name) {
        super(name);
    }

    public void testCombineNullRead()
            throws IOException {
        combiner = new ByteArrayCombinerStream(null, 10);
        assertEquals(-1, combiner.read());
    }

    public void testCombineNullReadArray()
            throws IOException {
        combiner = new ByteArrayCombinerStream(null, 10);
        assertEquals(-1, combiner.read(new byte[10], 0, 10));
    }

    public void testCombineAvailableNull()
            throws IOException {
        combiner = new ByteArrayCombinerStream(null, -34);
        assertEquals(0, combiner.available()); 
    }

    public void testCombineAvailable4bytes()
            throws IOException {
        byte[] array = {65,66,77,79};
        ArrayList list = new ArrayList(1);
        list.add(array);
        combiner = new ByteArrayCombinerStream(list, 4);
        assertEquals(4, combiner.available()); 
    }
    
    public void testCombineOneArray()
            throws IOException {
        ArrayList list = new ArrayList(1);
        list.add(defaultArray);
        combiner = new ByteArrayCombinerStream(list, defaultArray.length);
        byte[] resArray = new byte[defaultArray.length];
        assertEquals(defaultArray.length, 
                     combiner.read(resArray,0, resArray.length));
        assertTrue(combiner.read() == -1);
        assertTrue(Arrays.equals(defaultArray, resArray));
    }

    public void testCominbe100SmallArrays()
            throws IOException {
        int arrays = 100;
        byte[] array = {65,66,77,79};
        ArrayList list = new ArrayList(arrays);
        long length = 0;
        for (int i=0; i < arrays; i++) {
            list.add(array);
            length += array.length;
        }
        byte[] targetArray = new byte[(int)length];
        int offset = 0;
        for (int i=0; i < arrays; i++) {
            System.arraycopy(array, 0, targetArray, offset, array.length);
            offset += array.length;
        }
        combiner = new ByteArrayCombinerStream(list, length);
        byte[] resArray = new byte[(int)length];
        assertEquals(length, combiner.read(resArray, 0, resArray.length));
        assertTrue(combiner.read() == -1);
        assertTrue(combiner.read() == -1);
        assertTrue(Arrays.equals(targetArray, resArray));
    }
    
    public void testTruncateDataFromOneArray()
            throws IOException {
        int length = defaultArray.length -5;
        ArrayList list = new ArrayList(1);
        list.add(defaultArray);
        byte[] targetArray = new byte[length];
        System.arraycopy(defaultArray, 0,
                         targetArray, 0, length);
        byte[] resArray = new byte[length];
        combiner = new ByteArrayCombinerStream(list, length);
        assertEquals(length, combiner.read(resArray, 0, length));
        assertTrue(combiner.read() == -1);
        assertTrue(Arrays.equals(targetArray, resArray));
    }
    
    public void testTruncateDataFromTwoArrays()
            throws IOException {
        int length = (defaultArray.length *2) -7;
        ArrayList list = new ArrayList(2);
        list.add(defaultArray);
        list.add(defaultArray);
        byte[] targetArray = new byte[length];
        System.arraycopy(defaultArray, 0,
                         targetArray, 0, defaultArray.length);
        System.arraycopy(defaultArray, 0,
                         targetArray, defaultArray.length, 
                         length - defaultArray.length);
        byte[] resArray = new byte[length];
        combiner = new ByteArrayCombinerStream(list, length);
        assertEquals(length, combiner.read(resArray, 0, length));
        assertTrue(combiner.read() == -1);
        assertTrue(Arrays.equals(targetArray, resArray));
    }
} // End class ByteArrayCombinerStreamTest 
