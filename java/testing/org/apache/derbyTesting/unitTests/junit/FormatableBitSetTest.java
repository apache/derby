/*

   Derby - Class org.apache.derbyTesting.unitTests.junit.FormatableBitSet

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

import junit.framework.*;

import org.apache.derby.iapi.services.io.FormatableBitSet;

import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.shared.common.sanity.AssertFailure;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;


/**
 * This class is used to test the FormatableBitSet implementation.
 */
public class FormatableBitSetTest extends TestCase {
    private byte[] bits24;
    private byte[] bits24C;

    private FormatableBitSet empty;
    private FormatableBitSet bitset18;
    private FormatableBitSet bitset18C;

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
        return new TestSuite(FormatableBitSetTest.class,
                             "FormatableBitSetTest suite");
    }

    /**
     * Test case that does a sanity check of the setup
     */
    public void testSetup() {
        assertEquals(0,empty.getLength());
        assertEquals(0,empty.getLengthInBytes());
        assertEquals(0,empty.getByteArray().length);
        assertEquals(0,empty.getNumBitsSet());

        assertEquals(18,bitset18.getLength());
        assertEquals(3,bitset18.getLengthInBytes());
        assertEquals(bits24,bitset18.getByteArray());
        assertEquals(9,bitset18.getNumBitsSet());

        assertEquals(18,bitset18C.getLength());
        assertEquals(3,bitset18C.getLengthInBytes());
        assertEquals(bits24C,bitset18C.getByteArray());
        assertEquals(9,bitset18C.getNumBitsSet());
    }

    // Test cases for single arg constructor
    public void testIntCtor0() {
        FormatableBitSet zeroBits = new FormatableBitSet(0);
        assertEquals(0,zeroBits.getLength());
        assertEquals(0,zeroBits.getLengthInBytes());
        assertEquals(0,zeroBits.getByteArray().length);
        assertEquals(0,zeroBits.getNumBitsSet());
    }
    public void testIntCtor1() {
        FormatableBitSet oneBit = new FormatableBitSet(1);
        assertEquals(1,oneBit.getLength());
        assertEquals(1,oneBit.getLengthInBytes());
        assertEquals(1,oneBit.getByteArray().length);
        assertEquals(0,oneBit.getNumBitsSet());
    }
    public void testIntCtor8() {
        FormatableBitSet eightBits = new FormatableBitSet(8);
        assertEquals(8,eightBits.getLength());
        assertEquals(1,eightBits.getLengthInBytes());
        assertEquals(1,eightBits.getByteArray().length);
        assertEquals(0,eightBits.getNumBitsSet());
    }
    public void testIntCtor9() {
        FormatableBitSet nineBits = new FormatableBitSet(9);
        assertEquals(9,nineBits.getLength());
        assertEquals(2,nineBits.getLengthInBytes());
        assertEquals(2,nineBits.getByteArray().length);
        assertEquals(0,nineBits.getNumBitsSet());
    }
    public void testIntCtorNeg() {
        // Should throw an exception?
        FormatableBitSet negBits = new FormatableBitSet(-1);
        assertEquals(-1,negBits.getLength());
        assertEquals(0,negBits.getLengthInBytes());
        assertEquals(0,negBits.getByteArray().length);
        assertEquals(0,negBits.getNumBitsSet());
    }

    // Test cases for the copy constructor
    public void testEmptyCpyCtor() {
        FormatableBitSet emptyCpy = new FormatableBitSet(empty);
        assertEquals(0,emptyCpy.getLength());
        assertEquals(0,emptyCpy.getLengthInBytes());
        // FAILURE - the byte array of the copy is not null
        //assertEquals(null,emptyCpy.getByteArray());
        assertEquals(0,emptyCpy.getNumBitsSet());
    }
    public void testCpyCtor() {
        FormatableBitSet cpy = new FormatableBitSet(bitset18);
        assertEquals(18,cpy.getLength());
        assertEquals(3,cpy.getLengthInBytes());
        assertEquals(3,cpy.getByteArray().length);
        assertEquals(9,cpy.getNumBitsSet());
        assertEquals(0,cpy.compare(bitset18));
        assertTrue(cpy.equals(bitset18));
    }

    // Test cases for grow(int)
    public void testGrowEmpty() {
        empty.grow(18);
        assertEquals(18,empty.getLength());
        assertEquals(3,empty.getLengthInBytes());
        assertEquals(3,empty.getByteArray().length);
        assertEquals(0,empty.getNumBitsSet());
    }
    public void testGrow() {
        bitset18.grow(25);
        assertEquals(25,bitset18.getLength());
        assertEquals(4,bitset18.getLengthInBytes());
        assertEquals(4,bitset18.getByteArray().length);
        assertEquals(9,bitset18.getNumBitsSet());
    }
    // OK - should fail?
    public void testGrowSmaller() {
        bitset18.grow(9);
        assertEquals(18,bitset18.getLength());
    }
    // OK - should fail?
    public void testGrowNeg() {
        bitset18.grow(-9);
        assertEquals(18,bitset18.getLength());
    }

    // Test cases for shrink(int)
    public void testShrinkEmpty() {
        empty.shrink(0);
        assertEquals(0,empty.getLength());
        assertEquals(0,empty.getLengthInBytes());
        assertEquals(0,empty.getByteArray().length);
        assertEquals(0,empty.getNumBitsSet());
    }
    public void testShrink() {
        bitset18.shrink(9);
        assertEquals(9,bitset18.getLength());
        assertEquals(2,bitset18.getLengthInBytes());
        assertEquals(2,bitset18.getByteArray().length);
        assertEquals(5,bitset18.getNumBitsSet());
    }
    // OK - should fail?
    public void testShrinkLarger() {
        bitset18.shrink(25);
        assertEquals(18,bitset18.getLength());
    }
    public void testShrinkNeg() {
        try {
            bitset18.shrink(-9);
            fail();
        } catch (ArrayIndexOutOfBoundsException e) {}
    }
    // Should be allowed?
    public void testShrink0() {
        try { bitset18.shrink(0); fail(); }
        catch (ArrayIndexOutOfBoundsException e) {}
        //      assertEquals(0,bitset18.getLength());
        //      assertEquals(0,bitset18.getLengthInBytes());
        //      assertEquals(0,bitset18.getByteArray().length);
        //      assertEquals(0,bitset18.getNumBitsSet());
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
        FormatableBitSet large = new FormatableBitSet(bitset18);
        large.grow(100);
        large.shrink(9);
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
        bitset18.set(1);
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
        bitset18.clear(1);
        try { bitset18.clear(18); fail(); }
        catch (IllegalArgumentException iae) {}
    }

    // Test cases for anySetBit()
    public void testAnySetBitEmpty() {
        // More reasonable to return -1 here ?
        try { empty.anySetBit(); fail(); } 
        catch (ArrayIndexOutOfBoundsException e) {}
        //assertEquals(empty.anySetBit(),-1);
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

        // Should be 0 or failure?
        assertEquals(10,bitset18.anySetBit(-2));
        // Should be 0 or failure?
        assertEquals(10,bitset18.anySetBit(-3));
    }
    public void testAnySetBitBeyondBitPastEnd() {
        if (SanityManager.DEBUG) {
            try { bitset18.anySetBit(18); fail(); } catch (AssertFailure af) {}
        }
        else {
            assertEquals(-1, bitset18.anySetBit(18));
        }
    }

    // Test cases for or(FormatableBitSet)
    public void testORWithNull() {
        FormatableBitSet cpy = new FormatableBitSet(bitset18);
        bitset18.or(null);
        assertEquals(9,bitset18.getNumBitsSet());
        assertTrue(cpy.equals(bitset18));
    }
    public void testORWithEmpty() {
        FormatableBitSet cpy = new FormatableBitSet(bitset18);
        bitset18.or(empty);
        assertEquals(9,bitset18.getNumBitsSet());
        assertTrue(cpy.equals(bitset18));
    }
    public void testORWithComplement() {
        bitset18.or(bitset18C);
        assertEquals(bitset18.getNumBitsSet(),18);
    }
    public void testORWithSmaller() {
        bitset18C.shrink(9);
        bitset18.or(bitset18C);
        assertEquals(13,bitset18.getNumBitsSet());
    }
    public void testORWithLarger() {
        bitset18.shrink(9);
        bitset18.or(bitset18C);
        assertEquals(14,bitset18.getNumBitsSet());
    }

    // Test cases for and(FormatableBitSet)
    public void testANDWithNull() {
        if (SanityManager.DEBUG) {
            try { bitset18.and(null); fail(); } catch (AssertFailure af) {}
        }
        else {
            try { bitset18.and(null); fail(); }
            catch (NullPointerException npe) {}
        }
    }
    public void testANDWithEmpty() {
        bitset18.and(new FormatableBitSet());
        assertEquals(0,bitset18.getNumBitsSet());
    }
    public void testANDWithComplement() {
        bitset18.and(bitset18C);
        assertEquals(0,bitset18.getNumBitsSet());
    }
    public void testANDWithSmaller() {
        bitset18C.shrink(9);
        bitset18.and(bitset18C);
        assertEquals(0,bitset18.getNumBitsSet());
    }
    public void testANDWithLarger() {
        bitset18.shrink(9);
        bitset18.and(bitset18C);
        assertEquals(0,bitset18.getNumBitsSet());
    }

    // Test cases for xor(FormatableBitSet)
    public void testXORWithNull() {
        try { bitset18.xor(null); fail(); } catch (NullPointerException npe) {}
    }
    public void testXORWithEmpty() {
        FormatableBitSet cpy = new FormatableBitSet(bitset18);
        if (SanityManager.DEBUG) {
            try { bitset18.xor(empty); fail(); } catch (AssertFailure af) {}
        }
        else {
            bitset18.xor(empty);
            assertEquals(0,empty.getLength());
            assertEquals(0,empty.getLengthInBytes());
            assertEquals(0,empty.getByteArray().length);
            assertEquals(0,empty.getNumBitsSet());
        }
        //assertEquals(9,bitset18.getNumBitsSet());
        //assertTrue(cpy.equals(bitset18));
    }
    public void testXORWithComplement() {
        bitset18.set(2);
        bitset18.set(3);
        bitset18.xor(bitset18C);
        assertEquals(16,bitset18.getNumBitsSet());
        assertFalse(bitset18.isSet(2));
        assertFalse(bitset18.isSet(3));
    }
    public void testXORWithSmaller() {
        bitset18C.shrink(9);
        if (SanityManager.DEBUG) {
            try { bitset18.xor(bitset18C); fail(); } catch (AssertFailure af) {}
        }
        else {
            bitset18.xor(bitset18C);
            assertEquals(18,bitset18.getLength());
            assertEquals(3,bitset18.getLengthInBytes());
            assertEquals(3,bitset18.getByteArray().length);
            assertEquals(13,bitset18.getNumBitsSet());

        }
        //assertEquals(13,bitset18.getNumBitsSet());
    }
    public void testXORWithLarger() {
        bitset18.shrink(9);
        if (SanityManager.DEBUG) {
            try { bitset18.xor(bitset18C); fail(); } catch (AssertFailure af) {}
        }
        else {
            try { bitset18.xor(bitset18C); fail(); }
            catch (IllegalArgumentException iae) {}
        }
        //assertEquals(14,bitset18.getNumBitsSet());
    }

    // Test case for writeExternal(ObjectOut) and readExternal(ObjectOut)
    public void testExternal() throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(buf);
        bitset18.writeExternal(oos);
        oos.flush();

        empty.readExternal(new ObjectInputStream(new ByteArrayInputStream(buf.toByteArray())));
        assertTrue(empty.equals(bitset18));
    }

    // ERROR - Negative array size argument
    // Not covered by other tests
    //     public void testReadExternalFromArray() throws IOException {
    //      ByteArrayOutputStream buf = new ByteArrayOutputStream();
    //      ObjectOutput oos = new ObjectOutputStream(buf);
    //      bitset18.writeExternal(oos);
    //     oos.flush();
    //     empty.readExternalFromArray(new ArrayInputStream(buf.toByteArray()));
    //     assertTrue(empty.equals(bitset18));
    //      }
}
