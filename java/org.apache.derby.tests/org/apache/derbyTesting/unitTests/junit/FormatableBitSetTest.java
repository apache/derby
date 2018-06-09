/*

   Derby - Class org.apache.derbyTesting.unitTests.junit.FormatableBitSetTest

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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import junit.framework.Test;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;


/**
 * This class is used to test the FormatableBitSet implementation.
 */
public class FormatableBitSetTest extends BaseTestCase {
    private byte[] bits24;
    private byte[] bits24C;

    private FormatableBitSet empty;
    private FormatableBitSet bitset18;
    private FormatableBitSet bitset18C;

    /**
     * <code>Integer.bitCount</code> method. Only available in JDK 1.5 or
     * later.
     */
    private final static Method bitCount;
    static {
        Method m = null;
        try {
            m = Integer.class.getMethod("bitCount", new Class[]{Integer.TYPE});
        } catch (Throwable t) {}
        bitCount = m;
    }

    /**
     * Create a test with the given name.
     *
     * @param name name of the test.
     */
    public FormatableBitSetTest(String name) {
        super(name);
    }

    /**
     *
     */
    public void setUp() {
        //1100 1110 0011 1100 0000
        bits24  = new byte[] { (byte)0xce, (byte)0x3c, 0x0 };

        // 0011 0001 1100 0011 1100
        bits24C = new byte[] { (byte)0x31, (byte)0xc3, (byte)0xc0 };

        empty = new FormatableBitSet();
        bitset18 = new FormatableBitSet(bits24);
        bitset18.shrink(18);
        bitset18C = new FormatableBitSet(bits24C);
        bitset18C.shrink(18);
    }

    /**
     * Release the resources that are used in this test
     *
     * @throws Exception
     */
    public void tearDown() throws Exception {
        empty = null;
        bits24 = null;
        bits24C = null;
        bitset18 = null;
        bitset18C = null;
        super.tearDown();
    }
    /**
     * Return a suite with all tests in this class (default suite)
     *
     * @throws Exception
     */
    public static Test suite() {
        BaseTestSuite ts = new BaseTestSuite(FormatableBitSetTest.class,
                             "FormatableBitSetTest suite");

        if (bitCount != null) {
            ts.addTest(new FormatableBitSetTest("numBitsSetInOneByte"));
            ts.addTest(new FormatableBitSetTest("numBitsSetInTwoBytes"));
        }

        return ts;
    }

    /**
     * Test case that does a sanity check of the setup
     */
    public void testSetup() {
        assertEquals(0,empty.getLength());
        assertEquals(0,empty.getLengthInBytes());
        assertEquals(0,empty.getNumBitsSet());
        assertTrue(empty.invariantHolds());
        assertEquals(0,empty.getByteArray().length);

        assertEquals(18,bitset18.getLength());
        assertEquals(3,bitset18.getLengthInBytes());
        assertEquals(9,bitset18.getNumBitsSet());
        assertTrue(bitset18.invariantHolds());
        assertEquals(bits24,bitset18.getByteArray());

        assertEquals(18,bitset18C.getLength());
        assertEquals(3,bitset18C.getLengthInBytes());
        assertEquals(9,bitset18C.getNumBitsSet());
        assertTrue(bitset18C.invariantHolds());
        assertEquals(bits24C,bitset18C.getByteArray());
    }

    // Test cases for single arg constructor
    public void testIntCtor0() {
        FormatableBitSet zeroBits = new FormatableBitSet(0);
        assertEquals(0,zeroBits.getLength());
        assertEquals(0,zeroBits.getLengthInBytes());
        assertEquals(0,zeroBits.getNumBitsSet());
        assertTrue(zeroBits.invariantHolds());
        assertEquals(0,zeroBits.getByteArray().length);
    }
    public void testIntCtor1() {
        FormatableBitSet oneBit = new FormatableBitSet(1);
        assertEquals(1,oneBit.getLength());
        assertEquals(1,oneBit.getLengthInBytes());
        assertEquals(0,oneBit.getNumBitsSet());
        assertTrue(oneBit.invariantHolds());
        assertEquals(1,oneBit.getByteArray().length);
    }
    public void testIntCtor8() {
        FormatableBitSet eightBits = new FormatableBitSet(8);
        assertEquals(8,eightBits.getLength());
        assertEquals(1,eightBits.getLengthInBytes());
        assertEquals(0,eightBits.getNumBitsSet());
        assertTrue(eightBits.invariantHolds());
        assertEquals(1,eightBits.getByteArray().length);
    }
    public void testIntCtor9() {
        FormatableBitSet nineBits = new FormatableBitSet(9);
        assertEquals(9,nineBits.getLength());
        assertEquals(2,nineBits.getLengthInBytes());
        assertEquals(0,nineBits.getNumBitsSet());
        assertTrue(nineBits.invariantHolds());
        assertEquals(2,nineBits.getByteArray().length);
    }
    public void testIntCtorNeg() {
        try { FormatableBitSet negBits = new FormatableBitSet(-1); fail(); }
        catch(IllegalArgumentException iae) {}
    }

    // Test cases for the copy constructor
    public void testEmptyCpyCtor() {
        FormatableBitSet emptyCpy = new FormatableBitSet(empty);
        assertEquals(0,emptyCpy.getLength());
        assertEquals(0,emptyCpy.getLengthInBytes());
        assertEquals(0,emptyCpy.getNumBitsSet());
        assertTrue(emptyCpy.invariantHolds());
    }
    public void testCpyCtor() {
        FormatableBitSet cpy = new FormatableBitSet(bitset18);
        assertEquals(18,cpy.getLength());
        assertEquals(3,cpy.getLengthInBytes());
        assertEquals(9,cpy.getNumBitsSet());
        assertEquals(0,cpy.compare(bitset18));
        assertTrue(cpy.equals(bitset18));
        assertTrue(cpy.invariantHolds());
        assertEquals(3,cpy.getByteArray().length);
    }

    // Test cases for grow(int)
    public void testGrowEmpty() {
        empty.grow(18);
        assertEquals(18,empty.getLength());
        assertEquals(3,empty.getLengthInBytes());
        assertEquals(0,empty.getNumBitsSet());
        assertTrue(bitset18.invariantHolds());
        assertEquals(3,empty.getByteArray().length);
    }
    public void testGrow() {
        bitset18.grow(25);
        assertEquals(25,bitset18.getLength());
        assertEquals(4,bitset18.getLengthInBytes());
        assertEquals(9,bitset18.getNumBitsSet());
        assertTrue(bitset18.invariantHolds());
        assertEquals(4,bitset18.getByteArray().length);
    }
    public void testGrowSmaller() {
        bitset18.grow(9);
        assertEquals(18,bitset18.getLength());
        assertEquals(3,bitset18.getLengthInBytes());
        assertEquals(9,bitset18.getNumBitsSet());
        assertTrue(bitset18.invariantHolds());
        assertEquals(3,bitset18.getByteArray().length);
    }
    public void testGrowNeg() {
        try { bitset18.grow(-9); fail(); }
        catch (IllegalArgumentException iae) {}
    }
    public void testGrow0() {
        empty.grow(0);
        assertEquals(0,empty.getLength());
        assertEquals(0,empty.getLengthInBytes());
        assertEquals(0,empty.getNumBitsSet());
        assertTrue(empty.invariantHolds());
        assertEquals(0,empty.getByteArray().length);
    }
    public void testGrow1() {
        empty.grow(1);
        assertEquals(1,empty.getLength());
        assertEquals(1,empty.getLengthInBytes());
        assertEquals(0,empty.getNumBitsSet());
        assertTrue(empty.invariantHolds());
        assertEquals(1,empty.getByteArray().length);
    }
    public void testGrow2() {
        empty.grow(2);
        assertEquals(2,empty.getLength());
        assertEquals(1,empty.getLengthInBytes());
        assertEquals(0,empty.getNumBitsSet());
        assertTrue(empty.invariantHolds());
        assertEquals(1,empty.getByteArray().length);
    }
    public void testGrow3() {
        empty.grow(3);
        assertEquals(3,empty.getLength());
        assertEquals(1,empty.getLengthInBytes());
        assertEquals(0,empty.getNumBitsSet());
        assertTrue(empty.invariantHolds());
        assertEquals(1,empty.getByteArray().length);
    }
    public void testGrow4() {
        empty.grow(4);
        assertEquals(4,empty.getLength());
        assertEquals(1,empty.getLengthInBytes());
        assertEquals(0,empty.getNumBitsSet());
        assertTrue(empty.invariantHolds());
        assertEquals(1,empty.getByteArray().length);
    }
    public void testGrow5() {
        empty.grow(5);
        assertEquals(5,empty.getLength());
        assertEquals(1,empty.getLengthInBytes());
        assertEquals(0,empty.getNumBitsSet());
        assertTrue(empty.invariantHolds());
        assertEquals(1,empty.getByteArray().length);
    }
    public void testGrow6() {
        empty.grow(6);
        assertEquals(6,empty.getLength());
        assertEquals(1,empty.getLengthInBytes());
        assertEquals(0,empty.getNumBitsSet());
        assertTrue(empty.invariantHolds());
        assertEquals(1,empty.getByteArray().length);
    }
    public void testGrow7() {
        empty.grow(7);
        assertEquals(7,empty.getLength());
        assertEquals(1,empty.getLengthInBytes());
        assertEquals(0,empty.getNumBitsSet());
        assertTrue(empty.invariantHolds());
        assertEquals(1,empty.getByteArray().length);
    }
    public void testGrow8() {
        empty.grow(8);
        assertEquals(8,empty.getLength());
        assertEquals(1,empty.getLengthInBytes());
        assertEquals(0,empty.getNumBitsSet());
        assertTrue(empty.invariantHolds());
        assertEquals(1,empty.getByteArray().length);
    }
    public void testGrow9() {
        empty.grow(9);
        assertEquals(9,empty.getLength());
        assertEquals(2,empty.getByteArray().length);
        assertEquals(0,empty.getNumBitsSet());
        assertTrue(empty.invariantHolds());
        assertEquals(2,empty.getLengthInBytes());
    }

    // Test cases for shrink(int)
    public void testShrinkEmpty() {
        empty.shrink(0);
        assertEquals(0,empty.getLength());
        assertEquals(0,empty.getLengthInBytes());
        assertEquals(0,empty.getNumBitsSet());
        assertTrue(empty.invariantHolds());
        assertEquals(0,empty.getByteArray().length);
    }
    public void testShrink() {
        bitset18.shrink(9);
        assertEquals(9,bitset18.getLength());
        assertEquals(2,bitset18.getLengthInBytes());
        assertEquals(5,bitset18.getNumBitsSet());
        assertTrue(bitset18.invariantHolds());
        assertEquals(2,bitset18.getByteArray().length);
    }
    public void testShrinkLarger() {
        try { bitset18.shrink(25); fail(); }
        catch (IllegalArgumentException iae) {}
    }
    public void testShrinkNeg() {
        try {
            bitset18.shrink(-9);
            fail();
        } catch (IllegalArgumentException iae) {}
    }
    public void testShrink0() {
        bitset18.shrink(0);
        assertEquals(0,bitset18.getLength());
        assertEquals(0,bitset18.getLengthInBytes());
        assertTrue(bitset18.invariantHolds());
        assertEquals(0,bitset18.getNumBitsSet());
        assertEquals(0,bitset18.getByteArray().length);
    }
    public void testShrink1() {
        bitset18.shrink(1);
        assertEquals(1,bitset18.getLength());
        assertEquals(1,bitset18.getLengthInBytes());
        assertEquals(1,bitset18.getNumBitsSet());
        assertTrue(bitset18.invariantHolds());
        assertEquals(1,bitset18.getByteArray().length);
    }
    public void testShrink2() {
        bitset18.shrink(2);
        assertEquals(2,bitset18.getLength());
        assertEquals(1,bitset18.getLengthInBytes());
        assertEquals(2,bitset18.getNumBitsSet());
        assertTrue(bitset18.invariantHolds());
        assertEquals(1,bitset18.getByteArray().length);
    }
    public void testShrink3() {
        bitset18.shrink(3);
        assertEquals(3,bitset18.getLength());
        assertEquals(1,bitset18.getLengthInBytes());
        assertEquals(2,bitset18.getNumBitsSet());
        assertTrue(bitset18.invariantHolds());
        assertEquals(1,bitset18.getByteArray().length);
    }
    public void testShrink4() {
        bitset18.shrink(4);
        assertEquals(4,bitset18.getLength());
        assertEquals(1,bitset18.getLengthInBytes());
        assertEquals(2,bitset18.getNumBitsSet());
        assertTrue(bitset18.invariantHolds());
        assertEquals(1,bitset18.getByteArray().length);
    }
    public void testShrink5() {
        bitset18.shrink(5);
        assertEquals(5,bitset18.getLength());
        assertEquals(1,bitset18.getLengthInBytes());
        assertEquals(3,bitset18.getNumBitsSet());
        assertTrue(bitset18.invariantHolds());
        assertEquals(1,bitset18.getByteArray().length);
    }
    public void testShrink6() {
        bitset18.shrink(6);
        assertEquals(6,bitset18.getLength());
        assertEquals(1,bitset18.getLengthInBytes());
        assertEquals(4,bitset18.getNumBitsSet());
        assertTrue(bitset18.invariantHolds());
        assertEquals(1,bitset18.getByteArray().length);
    }
    public void testShrink7() {
        bitset18.shrink(7);
        assertEquals(7,bitset18.getLength());
        assertEquals(1,bitset18.getLengthInBytes());
        assertEquals(5,bitset18.getNumBitsSet());
        assertTrue(bitset18.invariantHolds());
        assertEquals(1,bitset18.getByteArray().length);
    }
    public void testShrink8() {
        bitset18.shrink(8);
        assertEquals(8,bitset18.getLength());
        assertEquals(1,bitset18.getLengthInBytes());
        assertEquals(5,bitset18.getNumBitsSet());
        assertTrue(bitset18.invariantHolds());
        assertEquals(1,bitset18.getByteArray().length);
    }
    public void testShrink9() {
        bitset18.shrink(9);
        assertEquals(9,bitset18.getLength());
        assertEquals(2,bitset18.getLengthInBytes());
        assertEquals(5,bitset18.getNumBitsSet());
        assertTrue(bitset18.invariantHolds());
        assertEquals(2,bitset18.getByteArray().length);
    }
    public void testShrink10() {
        bitset18.shrink(10);
        assertEquals(10,bitset18.getLength());
        assertEquals(2,bitset18.getLengthInBytes());
        assertEquals(5,bitset18.getNumBitsSet());
        assertTrue(bitset18.invariantHolds());
        assertEquals(2,bitset18.getByteArray().length);
    }

    // Test cases for compare(FormatableBitSet)
    public void testCompareSameEmpty() {
        assertEquals(0,empty.compare(empty));
    }
    public void testCompareAnotherEmpty() {
        assertEquals(0,empty.compare(new FormatableBitSet()));
    }
    public void testCompare18Empty() {
        // Would expect -1 since empty is smaller than bitset18 (based
        //on documentation)
        //assertEquals(-1,bitset18.compare(new FormatableBitSet()));
        assertEquals(1,bitset18.compare(new FormatableBitSet()));
    }
    public void testCompareEmpty18() {
        // Would expect 1 since empty is smaller than bitset18 (based
        //on documentation)
        //assertEquals(1,empty.compare(bitset18));
        assertEquals(-1,empty.compare(bitset18));
    }
    public void testCompareToComplement() {
        assertEquals(1, bitset18.compare(bitset18C));
    }
    public void testCompareDifferentArray() {
        FormatableBitSet small = new FormatableBitSet(bitset18);
        small.shrink(9);
        assertTrue(small.invariantHolds());
        FormatableBitSet large = new FormatableBitSet(bitset18);
        large.grow(100);
        assertTrue(large.invariantHolds());
        large.shrink(9);
        assertTrue(large.invariantHolds());
        assertEquals(0,small.compare(large));
    }

    // Test cases for isSet(int)
    public void testIsSetEmpty() {
        try { empty.isSet(-8); fail(); } catch (IllegalArgumentException iae) {}
        try { empty.isSet(-1); fail(); } catch (IllegalArgumentException iae) {}
        try { empty.isSet(0); fail(); } catch (IllegalArgumentException iae) {}
    }
    public void testIsSet() {
        try { bitset18C.isSet(-8); fail(); }
        catch (IllegalArgumentException iae) {}

        try { bitset18C.isSet(-1); fail(); }
        catch (IllegalArgumentException iae) {}

        assertFalse(bitset18C.isSet(0));
        assertFalse(bitset18C.isSet(1));
        assertTrue(bitset18C.isSet(2));
        assertTrue(bitset18C.isSet(3));
        assertFalse(bitset18C.isSet(4));
        assertFalse(bitset18C.isSet(5));
        assertFalse(bitset18C.isSet(6));
        assertTrue(bitset18C.isSet(7));
        assertTrue(bitset18C.isSet(8));
        assertTrue(bitset18C.isSet(9));
        assertFalse(bitset18C.isSet(10));
        assertFalse(bitset18C.isSet(11));
        assertFalse(bitset18C.isSet(12));
        assertFalse(bitset18C.isSet(13));
        assertTrue(bitset18C.isSet(14));
        assertTrue(bitset18C.isSet(15));
        assertTrue(bitset18C.isSet(16));
        assertTrue(bitset18C.isSet(17));
        try { bitset18C.isSet(18); fail(); }
        catch (IllegalArgumentException iae) {}
    }

    // Test cases for set(int)
    public void testSetEmpty() {
        try { empty.set(-8); fail(); } catch (IllegalArgumentException iae) {}
        try { empty.set(-1); fail(); } catch (IllegalArgumentException iae) {}
        try { empty.set(0); fail(); } catch (IllegalArgumentException iae) {}
    }
    public void testSet() {
        try { bitset18.set(-8); fail(); }
        catch (IllegalArgumentException iae) {}
        try { bitset18.set(-1); fail(); }
        catch (IllegalArgumentException iae) {}
        bitset18.set(0);
        assertTrue(bitset18.invariantHolds());
        bitset18.set(1);
        assertTrue(bitset18.invariantHolds());
        try { bitset18.set(18); fail(); }
        catch (IllegalArgumentException iae) {}
    }

    // Test cases for clear(int)
    public void testClearEmpty() {
        try { empty.clear(-8); fail(); } catch (IllegalArgumentException iae) {}
        try { empty.clear(-1); fail(); } catch (IllegalArgumentException iae) {}
        try { empty.clear(0); fail(); } catch (IllegalArgumentException iae) {}
    }
    public void testClear() {
        try { bitset18.clear(-8); fail(); }
        catch (IllegalArgumentException iae) {}
        try { bitset18.clear(-1); fail(); }
        catch (IllegalArgumentException iae) {}
        bitset18.clear(0);
        assertTrue(bitset18.invariantHolds());
        bitset18.clear(1);
        assertTrue(bitset18.invariantHolds());
        try { bitset18.clear(18); fail(); }
        catch (IllegalArgumentException iae) {}
    }

    // Test cases for anySetBit()
    public void testAnySetBitEmpty() {
        assertEquals(empty.anySetBit(),-1);
    }
    public void testAnySetBit() {
        assertEquals(2,bitset18C.anySetBit());
        bitset18C.clear(2);
        assertEquals(3,bitset18C.anySetBit());
    }

    // Test cases for anySetBit(int)
    public void testAnySetBitBeyondBit() {
        assertEquals(4,bitset18.anySetBit(1));
    }
    public void testAnySetBitBeyondBitNeg() {
        assertEquals(1,bitset18.anySetBit(0));
        assertEquals(0,bitset18.anySetBit(-1));
        try { bitset18.anySetBit(-2); fail(); }
        catch (ArrayIndexOutOfBoundsException e) {}
        try { bitset18.anySetBit(-3); fail(); }
        catch (ArrayIndexOutOfBoundsException e) {}
    }
    public void testAnySetBitBeyondBitPastEnd() {
        assertEquals(-1, bitset18.anySetBit(18));
    }

    // Test cases for or(FormatableBitSet)
    public void testORWithNull() {
        FormatableBitSet cpy = new FormatableBitSet(bitset18);
        assertTrue(cpy.invariantHolds());
        bitset18.or(null);
        assertEquals(9,bitset18.getNumBitsSet());
        assertTrue(cpy.equals(bitset18));
    }
    public void testORWithEmpty() {
        FormatableBitSet cpy = new FormatableBitSet(bitset18);
        bitset18.or(empty);
        assertEquals(9,bitset18.getNumBitsSet());
        assertTrue(cpy.equals(bitset18));
        assertTrue(bitset18.invariantHolds());
    }
    public void testORWithComplement() {
        bitset18.or(bitset18C);
        assertEquals(bitset18.getNumBitsSet(),18);
        assertTrue(bitset18.invariantHolds());
    }
    public void testORWithSmaller() {
        bitset18C.shrink(9);
        bitset18.or(bitset18C);
        assertEquals(13,bitset18.getNumBitsSet());
        assertTrue(bitset18.invariantHolds());
    }
    public void testORWithLarger() {
        bitset18.shrink(9);
        bitset18.or(bitset18C);
        assertEquals(14,bitset18.getNumBitsSet());
        assertTrue(bitset18.invariantHolds());
    }

    // Test cases for and(FormatableBitSet)
    public void testANDWithNull() {
        bitset18.and(null);
        assertEquals(18,bitset18.getLength());
        assertEquals(3,bitset18.getLengthInBytes());
        assertEquals(3,bitset18.getByteArray().length);
        assertEquals(0,bitset18.getNumBitsSet());
        assertTrue(bitset18.invariantHolds());
    }
    public void testANDWithEmpty() {
        bitset18.and(new FormatableBitSet());
        assertEquals(0,bitset18.getNumBitsSet());
        assertTrue(bitset18.invariantHolds());
    }
    public void testANDWithComplement() {
        bitset18.and(bitset18C);
        assertEquals(0,bitset18.getNumBitsSet());
        assertTrue(bitset18.invariantHolds());
    }
    public void testANDWithSmaller() {
        bitset18C.shrink(9);
        bitset18.and(bitset18C);
        assertEquals(0,bitset18.getNumBitsSet());
        assertTrue(bitset18.invariantHolds());
    }
    public void testANDWithLarger() {
        bitset18.shrink(9);
        bitset18.and(bitset18C);
        assertEquals(0,bitset18.getNumBitsSet());
        assertTrue(bitset18.invariantHolds());
    }

    // Test cases for xor(FormatableBitSet)
    public void testXORWithNull() {
        FormatableBitSet cpy = new FormatableBitSet(bitset18);
        bitset18.xor(null);
        assertEquals(9,bitset18.getNumBitsSet());
        assertTrue(cpy.equals(bitset18));
        assertTrue(bitset18.invariantHolds());
    }
    public void testXORWithEmpty() {
        FormatableBitSet cpy = new FormatableBitSet(bitset18);
        bitset18.xor(empty);
        assertEquals(18,bitset18.getLength());
        assertEquals(3,bitset18.getLengthInBytes());
        assertEquals(9,bitset18.getNumBitsSet());
        assertTrue(cpy.equals(bitset18));
        assertTrue(bitset18.invariantHolds());
        assertEquals(3,bitset18.getByteArray().length);
    }
    public void testXORWithComplement() {
        bitset18.set(2);
        bitset18.set(3);
        bitset18.xor(bitset18C);
        assertEquals(16,bitset18.getNumBitsSet());
        assertFalse(bitset18.isSet(2));
        assertFalse(bitset18.isSet(3));
        assertTrue(bitset18.invariantHolds());
    }
    public void testXORWithSmaller() {
        bitset18C.shrink(9);
        bitset18.xor(bitset18C);
        assertEquals(18,bitset18.getLength());
        assertEquals(3,bitset18.getLengthInBytes());
        assertEquals(13,bitset18.getNumBitsSet());
        assertTrue(bitset18.invariantHolds());
        assertEquals(3,bitset18.getByteArray().length);
    }
    public void testXORWithLarger() {
        bitset18.shrink(9);
        bitset18.xor(bitset18C);
        assertEquals(14,bitset18.getNumBitsSet());
        assertTrue(bitset18.invariantHolds());
    }

    // count one-bits in a byte with Integer.bitCount()
    private static int bitsInByte(byte b) throws Exception {
        Integer arg = b & 0xff;
        Integer ret = (Integer) bitCount.invoke(null, new Object[] { arg });
        return ret.intValue();
    }

    // test getNumBitsSet() for a one-byte bit set
    public void numBitsSetInOneByte() throws Exception {
        for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; ++i) {
            final byte b = (byte) i;
            FormatableBitSet bs = new FormatableBitSet(new byte[] { b });
            assertEquals("invalid bit count for b=" + b,
                         bitsInByte(b), bs.getNumBitsSet());
        }
    }

    // test getNumBitsSet() for a two-byte bit set
    public void numBitsSetInTwoBytes() throws Exception {
        for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; ++i) {
            final byte b1 = (byte) i;
            final int bits1 = bitsInByte(b1);
            for (int j = Byte.MIN_VALUE; j <= Byte.MAX_VALUE; ++j) {
                final byte b2 = (byte) j;
                FormatableBitSet bs =
                    new FormatableBitSet(new byte[] { b1, b2 });
                assertEquals(
                    "invalid bit count for b1=" + b1 + " and b2=" + b2,
                    bits1 + bitsInByte(b2), bs.getNumBitsSet());
            }
        }
    }

    // Test case for writeExternal(ObjectOut) and readExternal(ObjectOut)
    public void testExternal() throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(buf);
        bitset18.writeExternal(oos);
        oos.flush();

        empty.readExternal
            (new ObjectInputStream(new ByteArrayInputStream
                                   (buf.toByteArray())));
        assertTrue(empty.equals(bitset18));
        assertTrue(empty.invariantHolds());
    }
}
