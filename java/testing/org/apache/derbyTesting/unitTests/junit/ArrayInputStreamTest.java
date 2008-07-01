/*

   Derby - Class org.apache.derbyTesting.unitTests.junit.ArrayInputStreamTest

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

import java.io.IOException;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derby.iapi.services.io.ArrayInputStream;
import org.apache.derbyTesting.junit.BaseTestCase;

/**
 * Unit tests for {@code org.apache.derby.iapi.services.io.ArrayInputStream}.
 */
public class ArrayInputStreamTest extends BaseTestCase {

    public static Test suite() {
        return new TestSuite(ArrayInputStreamTest.class);
    }

    public ArrayInputStreamTest(String name) {
        super(name);
    }

    /**
     * Test that we don't get an overflow when the argument to skip() is
     * Long.MAX_VALUE (DERBY-3739).
     */
    public void testSkipLongMaxValue() throws IOException {
        ArrayInputStream ais = new ArrayInputStream(new byte[1000]);
        assertEquals(1000, ais.skip(Long.MAX_VALUE));
        assertEquals(1000, ais.getPosition());
        ais.setPosition(1);
        assertEquals(999, ais.skip(Long.MAX_VALUE));
        assertEquals(1000, ais.getPosition());
    }

    /**
     * Test that we don't get an overflow when the argument to skipBytes() is
     * Integer.MAX_VALUE (DERBY-3739).
     */
    public void testSkipBytesIntMaxValue() throws IOException {
        ArrayInputStream ais = new ArrayInputStream(new byte[1000]);
        assertEquals(1000, ais.skipBytes(Integer.MAX_VALUE));
        assertEquals(1000, ais.getPosition());
        ais.setPosition(1);
        assertEquals(999, ais.skipBytes(Integer.MAX_VALUE));
        assertEquals(1000, ais.getPosition());
    }

    /**
     * Test that skip() returns 0 when the argument is negative (DERBY-3739).
     */
    public void testSkipNegative() throws IOException {
        ArrayInputStream ais = new ArrayInputStream(new byte[1000]);
        assertEquals(0, ais.skip(-1));
    }

    /**
     * Test that skipBytes() returns 0 when the argument is negative
     * (DERBY-3739).
     */
    public void testSkipBytesNegative() throws IOException {
        ArrayInputStream ais = new ArrayInputStream(new byte[1000]);
        assertEquals(0, ais.skipBytes(-1));
    }
}
