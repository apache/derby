/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.store.PositionedStoreStreamTest

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
package org.apache.derbyTesting.functionTests.tests.store;

import java.io.IOException;
import java.io.InputStream;

import java.util.Arrays;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.impl.jdbc.PositionedStoreStream;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetStream;
import org.apache.derbyTesting.junit.BaseTestCase;

/**
 * Tests of the repositioning logic in {@link PositionedStoreStream}.
 */
public class PositionedStoreStreamTest
    extends BaseTestCase {

    public PositionedStoreStreamTest(String name) {
        super(name);
    }

    /**
     * Verifies that reading after EOF doesn't change the position.
     */
    public void testPositionAfterEOFRead()
            throws IOException, StandardException {
        InputStream in = new LoopingAlphabetStream(10);
        PositionedStoreStream pss = new PositionedStoreStream(in);
        assertEquals(0, pss.getPosition());
        for (int i=0; i < 10; i++) {
            pss.read();
            assertEquals(i +1, pss.getPosition());
        }
        assertEquals(10, pss.getPosition());
        assertEquals(-1, pss.read());
        assertEquals(10, pss.getPosition());
        assertEquals(-1, pss.read());
        assertEquals(0, pss.skip(199));
        assertEquals(10, pss.getPosition());
        pss.resetStream();
        assertEquals(0, pss.getPosition());
    }

    /**
     * Verifies that reading after EOF doesn't change the position.
     */
    public void testPositionAfterEOFReadArray()
            throws IOException, StandardException {
        InputStream in = new LoopingAlphabetStream(10);
        PositionedStoreStream pss = new PositionedStoreStream(in);
        assertEquals(0, pss.getPosition());
        byte[] two = new byte[2];
        for (int i=0; i < 10; i += 2) {
            assertEquals(2, pss.read(two, 0, 2));
            assertEquals(i +2, pss.getPosition());
        }
        assertEquals(10, pss.getPosition());
        assertEquals(-1, pss.read(two, 0, 2));
        assertEquals(10, pss.getPosition());
        assertEquals(-1, pss.read(two, 0, 2));
        assertEquals(0, pss.skip(21));
        assertEquals(-1, pss.read());
        assertEquals(10, pss.getPosition());
        pss.resetStream();
        assertEquals(0, pss.getPosition());
    }

    /**
     * Reads the whole stream repeatedly in one go, and resets it after each
     * read.
     */
    public void testReadEverythingInOneGo()
            throws IOException, StandardException {
        InputStream in = new LoopingAlphabetStream(127);
        PositionedStoreStream pss = new PositionedStoreStream(in);
        byte[] b = new byte[256];
        for (int i=0; i < 3; i++) {
            Arrays.fill(b, (byte)-1);
            assertEquals(127, pss.read(b, 0, 256));
            assertEquals(-1, pss.read(b, 127, 10));
            assertEquals(-1, b[127]);
            assertTrue( -1 != b[126]);
            assertEquals('a', b[0]);
            pss.reposition(0);
        }
    }

    public void testRepositionForwards()
            throws IOException, StandardException {
        final long length = 20L;
        InputStream in = new LoopingAlphabetStream(length);
        PositionedStoreStream pss = new PositionedStoreStream(in);
        assertEquals(0, pss.getPosition());
        //  Position forwards one by one.
        for (int i=0; i < length; i++) {
            InputStream inComp = new LoopingAlphabetStream(length);
            pss.reposition(i);
            inComp.skip(i);
            assertEquals(inComp.read(), pss.read());
        }
        // Position forwards two by two.
        for (int i=1; i < length; i += 2) {
            InputStream inComp = new LoopingAlphabetStream(length);
            pss.reposition(i);
            inComp.skip(i);
            assertEquals(inComp.read(), pss.read());
        }
    }
    
    public void testRepositionBackwards()
            throws IOException, StandardException {
        final long length = 20L;
        InputStream in = new LoopingAlphabetStream(length);
        PositionedStoreStream pss = new PositionedStoreStream(in);
        assertEquals(0, pss.getPosition());
        // Position backwards one by one.
        for (int i=(int)length; i >= 0; i--) {
            InputStream inComp = new LoopingAlphabetStream(length);
            pss.reposition(i);
            inComp.skip(i);
            assertEquals(inComp.read(), pss.read());
        }
        // Position backwards two by two.
        for (int i=(int)length -1; i >= 0; i -= 2) {
            InputStream inComp = new LoopingAlphabetStream(length);
            pss.reposition(i);
            inComp.skip(i);
            assertEquals(inComp.read(), pss.read());
        }
    }

    /**
     * Executes a simple read sequence against the lower case modern latin
     * alphabet, which involves some repositioning.
     */
    public void testSimpleReadSequence()
            throws IOException, StandardException {
        InputStream in = new LoopingAlphabetStream(26);
        PositionedStoreStream pss = new PositionedStoreStream(in);
        assertEquals('a', pss.read());
        pss.reposition(9);
        assertEquals('j', pss.read());
        pss.reposition(10);
        assertEquals('k', pss.read());
        pss.reposition(9);
        assertEquals('j', pss.read());
        pss.reposition(25);
        assertEquals('z', pss.read());
        assertEquals(-1, pss.read());
        pss.reposition(1);
        assertEquals('b', pss.read());
        pss.reposition(1);
        assertEquals('b', pss.read());
    }

    /**
     * A regression test for DERBY-3735.
     * <p>
     * If the bug is present, the repositioning will cause an EOF-exception to
     * be thrown in {@code skipFully}.
     */
    public void testDerby3735()
            throws IOException, StandardException {
        InputStream in = new LoopingAlphabetStream(2);
        PositionedStoreStream pss = new PositionedStoreStream(in);
        byte[] b = new byte[2];
        for (int i=0; i < 10; i++) {
            // After the first iteration, the position will be decreased and
            // eventually become negative.
            pss.read(b);
            println("Position at iteration " + i + ": " + pss.getPosition());
        }
        // This failed, because we tried skipping more bytes than there are in
        // the stream to get to position 0.
        pss.reposition(0);
    }

    public static Test suite() {
        return new TestSuite(
                PositionedStoreStreamTest.class, "PositionedStoreStreamTest");
    }
}
