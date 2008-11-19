/*

   Derby - Class org.apache.derbyTesting.unitTests.junit.CharacterStreamDescriptorTest

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

import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derby.iapi.jdbc.CharacterStreamDescriptor;
import org.apache.derbyTesting.junit.BaseTestCase;

/**
 * Simple tests of the character stream descriptor class.
 */
public class CharacterStreamDescriptorTest
    extends BaseTestCase {

    public CharacterStreamDescriptorTest(String name) {
        super(name);
    }

    /**
     * Tests the default values set by the builder.
     */
    public void testDefaultValues() {
        CharacterStreamDescriptor.Builder b =
                new CharacterStreamDescriptor.Builder();
        CharacterStreamDescriptor csd = b.build();

        // Test the default values.
        assertEquals(false, csd.isBufferable());
        assertEquals(false, csd.isPositionAware());
        assertEquals(0, csd.getDataOffset());
        assertEquals(0, csd.getCurBytePos());
        assertEquals(1, csd.getCurCharPos());
        assertEquals(0, csd.getByteLength());
        assertEquals(0, csd.getCharLength());
        assertEquals(Long.MAX_VALUE, csd.getMaxCharLength());
        assertEquals(-1, csd.getStreamId());
    }

    public void testSetValues() {
        final long byteLength = 1023;
        final long charLength = 1023*2;
        final long curBytePos = 4;
        final long curCharPos = 2;
        final long dataOffset = 2;
        final long maxCharLen = 768;
        final int streamId = this.hashCode();

        CharacterStreamDescriptor.Builder b =
                new CharacterStreamDescriptor.Builder().bufferable(true).
                byteLength(byteLength).charLength(charLength).
                curBytePos(curBytePos).curCharPos(curCharPos).
                dataOffset(dataOffset).maxCharLength(maxCharLen).id(streamId);
        CharacterStreamDescriptor csd = b.build();

        // Test the values.
        assertEquals(true, csd.isBufferable());
        assertEquals(false, csd.isPositionAware());
        assertEquals(dataOffset, csd.getDataOffset());
        assertEquals(curBytePos, csd.getCurBytePos());
        assertEquals(curCharPos, csd.getCurCharPos());
        assertEquals(byteLength, csd.getByteLength());
        assertEquals(charLength, csd.getCharLength());
        assertEquals(maxCharLen, csd.getMaxCharLength());
        assertEquals(streamId, csd.getStreamId());

        // Set only a few values.
        csd = new CharacterStreamDescriptor.Builder().bufferable(true).
                positionAware(true). maxCharLength(maxCharLen).build();
        assertEquals(true, csd.isBufferable());
        assertEquals(true, csd.isPositionAware());
        assertEquals(maxCharLen, csd.getMaxCharLength());

        // Set data offset and update the character position accordingly.
        csd = new CharacterStreamDescriptor.Builder().bufferable(true).
                positionAware(true).dataOffset(dataOffset).
                curCharPos(CharacterStreamDescriptor.BEFORE_FIRST).build();
        assertEquals(true, csd.isBufferable());
        assertEquals(true, csd.isPositionAware());
        assertEquals(dataOffset, csd.getDataOffset());
        assertEquals(CharacterStreamDescriptor.BEFORE_FIRST,
                csd.getCurCharPos());

    }

    public static Test suite() {
        return new TestSuite(CharacterStreamDescriptorTest.class,
                "CharacterStreamDescriptorTest suite");
    }
}
