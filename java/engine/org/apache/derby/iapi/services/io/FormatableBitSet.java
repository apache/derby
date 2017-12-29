/*

   Derby - Class org.apache.derby.iapi.services.io.FormatableBitSet

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

package org.apache.derby.iapi.services.io;

import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.shared.common.util.ArrayUtil;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
 * FormatableBitSet is implemented as a packed array of bytes.
 *
 */

public final class FormatableBitSet implements Formatable, Cloneable
{

	/********************************************************
	**
	**	This class implements Formatable. That means that it
	**	can write itself to and from a formatted stream. If
	**	you add more fields to this class, make sure that you
	**	also write/read them with the writeExternal()/readExternal()
	**	methods.
	**
	**	If, inbetween releases, you add more fields to this class,
	**	then you should bump the version number emitted by the getTypeFormatId()
	**	method.
	**
	********************************************************/

	/**
	** Bits are stored as an array of bytes.
	** Bits are numbered starting at 0.  Bits
	** 0..7 go in byte[0], 8..15 in byte[1] and so on.
	** The number of bytes is tracked as part
	** of the byte array.  The number of bits
	** being used is derived by the number of
	** bytes being used and the number of bits
	** being used by the last byte.  The partially
	** unused byte is always byte[byte.length] with the
	** lowest bits being unused.
	**
	** Zero length bits are stored using a
	** zero length byte array, with all bits
	** marked as unused.
	*/
	// value is never null. An empty bitset is represented by a
	// zero-length array.
	private byte[]	value;
	private	byte	bitsInLastByte;

	private transient int	lengthAsBits;

	private final void checkPosition(int p) {
		if (p < 0 || lengthAsBits <= p) {
			throw new
				IllegalArgumentException("Bit position "+p+
										 " is outside the legal range");
		}
	}

	// Division, multiplication and remainder calcuation of a positive
	// number with a power of two can be done using shifts and bit
	// masking. The compiler attempts this optimization but since Java
	// does not have unsigned ints it will also have to create code to
	// handle negative values. In this class the argument is
	// frequently an array index or array length, which is known not
	// to be negative. These utility methods allow us to perform
	// "unsigned" operations with 8. Hopefully the extra function call
	// will be inlined by the compiler.
	private static int udiv8(int i) { return (i>>3); }
	private static byte umod8(int i) { return (byte)(i&0x7); }
	private static int umul8(int i) { return (i<<3); }

	/**
	 * Niladic Constructor
	 */
	public FormatableBitSet()
	{
		value = ArrayUtil.EMPTY_BYTE_ARRAY;
	}

	/**
	 * Constructs a Bit with the initial number of bits
	 */
	public FormatableBitSet(int numBits)
	{
		if (numBits < 0) {
			throw new
			IllegalArgumentException("Bit set size "+ numBits +
									 " is not allowed");
		}
		initializeBits(numBits);
	}

	private void initializeBits(int numBits)
	{
		int numBytes = numBytesFromBits(numBits);

		// the byte array is zero'ed out by the new operator
		value = new byte[numBytes];
		bitsInLastByte = numBitsInLastByte(numBits);
		lengthAsBits = numBits;
	}

	/**
	 * Constructs a Bit from an array of bytes.  Assume
	 * bytes are all being used.
	 *
	 * @param newValue	The array of bytes to make up the new Bit
	 */
	public FormatableBitSet(byte[] newValue)
	{
		value = ArrayUtil.copy( newValue );
		bitsInLastByte = 8;
		lengthAsBits = calculateLength(newValue.length);
	}

	/**
	 * Copy constructor
	 *
	 * @param original the FormatableBitSet to make a copy from
	 */
	public FormatableBitSet (FormatableBitSet original)
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(
                original != null, "cannot make copy from a null FormatableBitSet");

		bitsInLastByte = original.bitsInLastByte;
		lengthAsBits = original.lengthAsBits;

		int lenInBytes = FormatableBitSet.numBytesFromBits(original.lengthAsBits);
		value = new byte[lenInBytes];
		if (lenInBytes > 0)
			System.arraycopy(original.value, 0, value, 0, lenInBytes);
	}
			
	/*
	 * Cloneable
	 */
	public Object clone()
	{
		return new FormatableBitSet(this);
	}

	/**
	 * This method returns true if the following conditions hold:
	 * 1. The number of bits in the bitset will fit into the allocated
	 * byte array. 2. 'lengthAsBits' and 'bitsInLastByte' are
	 * consistent. 3. All unused bits in the byte array are
	 * unset. This represents an invariant for the class, so this
	 * method should always return true.
	 *
	 * The method is public, but is primarily intended for testing and
	 * ASSERTS.
	 * @return true if invariant holds, false otherwise
	 */
	public boolean invariantHolds() {
		// Check that all bits will fit in byte array
		final int arrayLengthAsBits = value.length*8;
		if (lengthAsBits > arrayLengthAsBits) { return false; }

		// Check consistency of 'lengthAsBits' and 'bitsInLastByte'
		final int partialByteIndex = (lengthAsBits-1)/8;
		if (bitsInLastByte != (lengthAsBits - (8*partialByteIndex))) {
			return false;
		}
		// Special case for empty bitsets since they will have
		// 'partialByteIndex'==0, but this isn't a legal index into
		// the byte array
		if (value.length==0) { return true; }

		// Check that the last used (possibly partial) byte doesn't
		// contain any unused bit positions that are set.
		byte partialByte = value[partialByteIndex];
		partialByte <<= bitsInLastByte;  // must be zero after shift

		// Check the remaining completely unused bytes (if any)
		for (int i = partialByteIndex+1; i < value.length; ++i) {
			partialByte |= value[i];
		}
		return (partialByte==0);
	}


	/**
	 * Get the length in bytes of a Bit value
	 *
	 * @return	The length in bytes of this value
	 */
	public int getLengthInBytes()
	{
		return FormatableBitSet.numBytesFromBits(lengthAsBits);
	}

	/**
	** Get the length in bits
	**
	** @return The length in bits for this value
	**
	** NOTE: could possibly be changed to a long.  As is
	** we are restricted to 2^(31-3) -&gt; 256meg instead
	** of 2^31 (Integer.MAX_VALUE) like other datatypes
	** (or 2 gig).  If it is ever changed to a long
	** be sure to change read/writeExternal which write
	** out the length in bits.
	*/
	public int getLength() {
		return lengthAsBits;
	}

	private int calculateLength(int realByteLength)
	{
		if (realByteLength == 0)
		{
			return 0;
		}

		return ((realByteLength - 1) * 8) + bitsInLastByte;
	}

	/**
	 * Get the length in bits -- alias for getLength()
	 *
	 * @return The length in bits for this value
	 */
	public int size()
	{
		return getLength();
	}

	/**
	 * Get the value of the byte array
	 *
	 * @return	The value of the byte array
	 */

	public byte[] getByteArray()
	{
		// In some cases the array is bigger than the actual number
		// of valid bytes.
		int realByteLength = getLengthInBytes();

		// Currently the case is that the return from this
		// call only includes the valid bytes.
		if (value.length != realByteLength) {
			byte[] data = new byte[realByteLength];
			System.arraycopy(value, 0, data, 0, realByteLength);

			value = data;
		}

		return ArrayUtil.copy( value );
	}

	/**
	 * Grow (widen) a FormatableBitSet so that it contains at least N
	 * bits. If the bitset already has more than n bits, this is a
	 * noop. Negative values of n are not allowed.
	 * ASSUMPTIONS: that all extra bits in the last byte are
	 * zero.
	 *
	 * @param n	The number of bits you want.  The bits are
	 *			always added as 0 and are appended to the
	 *			least significant end of the bit array.
	 *
	 */
	public void grow(int n)
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(invariantHolds(), "broken invariant");
		}

 		if (n < 0) {
 			throw new IllegalArgumentException("Bit set cannot grow from "+
 											   lengthAsBits+" to "+n+" bits");
 		}
		if (n <= lengthAsBits) {
 			return;
 		}

		int newNumBytes = FormatableBitSet.numBytesFromBits(n);

		// is there enough room in the existing array
		if (newNumBytes > value.length) {
			/*
			** We didn't have enough bytes in value, so we need
			** to create a bigger byte array and use that.
			*/
			byte[] newValue = new byte[newNumBytes];

			int oldNumBytes = getLengthInBytes();
			System.arraycopy(value, 0, newValue, 0, oldNumBytes);

			value = newValue;
		}
		bitsInLastByte = numBitsInLastByte(n);
		lengthAsBits = n;
	}

	/**
	 * Shrink (narrow) a FormatableBitSet to N bits. N may not be
	 * larger than the current bitset size, or negative.
	 *
	 * @param n	The number of bits the caller wants.  The
	 * 			bits are always removed from the
	 *			least significant end of the bit array.
	 */
	public void shrink(int n)
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(invariantHolds(), "broken invariant");
		}

		if (n < 0 || n > lengthAsBits) {
			throw new
				IllegalArgumentException("Bit set cannot shrink from "+
										 lengthAsBits+" to "+n+" bits");
		}

		final int firstUnusedByte = numBytesFromBits(n);
		bitsInLastByte = numBitsInLastByte(n);
		lengthAsBits = n;

		for (int i = firstUnusedByte; i < value.length; ++i) {
			value[i] = 0;
		}
		if (firstUnusedByte > 0) {
			// Mask out any left over bits in the
			// last byte.  Retain the highest bits.
			value[firstUnusedByte-1] &= 0xff00 >> bitsInLastByte;
		}
	}

	/*
	** Some of the operators required by SQL.  These could alternatively
	** be in SQLBit, but since they are so tightly bound to the implementation
	** rather than return something that undermines the encapsulation
	** of this type, i have chosen to put them in here.
	*/

	/**
	 * Bit equivalence.  Compare this with other.
	 * If the length is different, then cannot be
	 * equal so short circuit.  Otherwise, rely on
	 * compare().  Note that two zero length bits are
	 * considered equal.
	 *
	 * @param other	the other bit to compare to
	 *
	 * @return TRUE|FALSE
	 */
	public boolean equals(Object other)
	{
        if (other instanceof FormatableBitSet) 
        {
            FormatableBitSet that = (FormatableBitSet) other;
		    if (this.getLength() != that.getLength())
		    {
			    return false;
		    }

		    return (this.compare(that) == 0);
        }
        return false;
	}

	/**
	 * Bit comparison.  Compare this with other.
	 * Will always do a byte by byte compare.
	 *
	 * Given 2 similar bits of unequal lengths (x and y),
	 * where x.getLength() &lt; y.getLength() but where:
	 *
	 *	 x[0..x.getLength()] == y[0..x.getLength()]
	 *
	 * then x &lt; y.
	 *
	 *
	 * @param other the other bit to compare to
	 *
	 * @return -1	- if other &lt;  this
	 *			0	- if other == this
	 *			1	- if other &gt;  this
	 *
	 */
	public int compare(FormatableBitSet other)
	{

		int		otherCount, thisCount;
		int		otherLen, thisLen;
		byte[]	otherb;

		otherb = other.value;
		otherLen = other.getLengthInBytes();
		thisLen = getLengthInBytes();
		for (otherCount = 0, thisCount = 0;
				otherCount < otherLen && thisCount < thisLen;
				otherCount++, thisCount++)
		{
			if (otherb[otherCount] != this.value[thisCount])
				break;
		}

		/*
		** '==' if byte by byte comparison is identical and
		** exact same length in bits (not bytes).
		*/
		if ((otherCount == otherLen) && (thisCount == thisLen))
		{
				if (this.getLength() == other.getLength())
				{
					return 0;
				}

				/*
				** If subset of bits is identical, return 1
				** if other.getLength() > this.getLength(); otherwise,
				** -1
				*/
				return (other.getLength() < this.getLength()) ? 1 : -1;
		}

		if (otherCount == otherLen)
		{
			return 1;
		}
		else if (thisCount == thisLen)
		{
			return -1;
		}
		else
		{
			/*
			** Ok, we have a difference somewhere.  Now
			** we have to go to the trouble of converting
			** to a int and masking out the sign to get
			** a valid comparision because bytes are signed.
			*/
			int otherInt, thisInt;

			otherInt = (int)otherb[otherCount];
			otherInt &= (0x100 - 1);

			thisInt = (int)this.value[thisCount];
			thisInt &= (0x100 - 1);

			return (thisInt > otherInt) ? 1 : -1;

		}
	}

    /**
     * Produce a hash code by putting the value bytes into an int, exclusive OR'ing
     * if there are more than 4 bytes.
     *
     * @return the hash code
     */
    public int hashCode()
    {
        int code = 0;
        int i;
        int shift = 0;

		int byteLength = getLengthInBytes();
        for( i = 0; i < byteLength; i++)
        {
            code ^= (value[i] & 0xff)<<shift;
            shift += 8;
            if( 32 <= shift)
                shift = 0;
        }
        return code;
    }
    
	/**
	 * Bit isSet
	 *
	 * @param position	the bit to check
	 *
	 */
	public final boolean isSet(int position)
	{
		checkPosition(position);
		final int byteIndex = udiv8(position);
		final byte bitIndex = umod8(position);
		return ((value[byteIndex] & (0x80>>bitIndex)) != 0);
	}


	/**
	 * Bit get -- alias for isSet()
	 *
	 * @param position	the bit to check
	 *
	 */
	public final boolean get(int position)
	{
		return isSet(position);
	}
	
	/**
	 * Bit set
	 *
	 * @param position	the bit to set
	 *
	 */
	public void set(int position)
	{
		checkPosition(position);
		final int byteIndex = udiv8(position);
		final byte bitIndex = umod8(position);
		value[byteIndex] |= (0x80>>bitIndex);
	}

	/**
	 * Bit clear
	 *
	 * @param position	the bit to clear
	 *
	 */
	public void clear(int position)
	{
		checkPosition(position);
		final int byteIndex = udiv8(position);
		final byte bitIndex = umod8(position);
		value[byteIndex] &= ~(0x80>>bitIndex);
	}

	/**
	  Clear all the bits in this FormatableBitSet
	  */
	public void clear()
	{
		int byteLength = getLengthInBytes();
		for (int ix=0; ix < byteLength; ix++)
            value[ix] = 0;
	}


	/**
	* Figure out how many bytes are needed to
	* store the input number of bits.
	*
	* @param bits	bits
	*
	* @return	the number of bytes
	*/
	private static int numBytesFromBits(int bits) {
		return (bits + 7) >> 3;
	}

	/**
	* Figure out how many bits are in the last
	* byte from the total number of bits.
	*
	* @param	bits	bits
	*
	* @return	the number of bits
	*/
	private static byte
	numBitsInLastByte(int bits)
	{
		if (bits == 0) return 0;
		byte lastbits = umod8(bits);
		return (lastbits != 0 ? lastbits : 8);
	}

	/**
	 * Format the string into BitSet format: {0, 2, 4, 8} if bits 0, 2, 4, 8
	 * are set.
	 *
	 * @return A new String containing the formatted Bit value
	 */
	public String toString()
	{
		{
			// give it a reasonable size
			StringBuffer str = new StringBuffer(getLength()*8*3);
			str.append("{");
			boolean first = true;
			for (int inPosition = 0; inPosition < getLength(); inPosition++)
			{
				if (isSet(inPosition))
				{
					if (!first)
						str.append(", ");
					first = false;
					str.append(inPosition);
				}
			}
			str.append("}");
			return new String(str);
		}
	}



	/**
	 * Statically calculates how many bits can fit into the number of
	 * bytes if this Bit object is externalized.  Only valid for this
	 * implementation of Bit.
	 */
	public static int maxBitsForSpace(int numBytes)
	{
		return (numBytes - 4)*8;
		
	}

	/**
	 * A utility method which treats the byte argument as an 8-bit
	 * bitset and finds the first set bit in that byte. Assumes that
	 * at least one bit in v is set (v!=0).
	 * @param v a non-zero byte to check for set bits
	 * @return the zero-based index of the first set bit in the argument byte
	 */
	private static byte firstSet(byte v) {
		if ((v & 0x80) != 0) {
			return 0;
		}
		if ((v & 0x40) != 0) {
			return 1;
		}
		if ((v & 0x20) != 0) {
			return 2;
		}
		if ((v & 0x10) != 0) {
			return 3;
		}
		if ((v & 0x8) != 0) {
			return 4;
		}
		if ((v & 0x4) != 0) {
			return 5;
		}
		if ((v & 0x2) != 0) {
			return 6;
		}
		return 7;
	}

	/**
	 * If any bit is set, return the zero-based bit index of the first
	 * bit that is set. If no bit is set, return -1. By using
	 * anySetBit() and anySetBit(beyondBit), one can quickly go thru
	 * the entire bit array to return all set bit.
	 *
	 * @return the zero-based index of the first bit that is set, or
	 * -1 if no bit is set
	 */
	public int anySetBit() {
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(invariantHolds(), "broken invariant");
		}
		final int numbytes = getLengthInBytes();
		for (int i = 0; i < numbytes; ++i) {
			final byte v = value[i];
			if (v == 0) continue;
			return (umul8(i) + firstSet(v));
		}
		return -1;
	}

	/**
	 * Like anySetBit(), but return any set bit whose number is bigger than
	 * beyondBit. If no bit is set after beyondBit, -1 is returned. 
	 * By using anySetBit() and anySetBit(beyondBit), one can quickly go
	 * thru the entire bit array to return all set bit.
	 *
	 * @param beyondBit Only look at bit that is greater than this bit number.
	 *                  Supplying a value of -1 makes the call equivalent to
	 *                  anySetBit().
	 * @return the bit number of a bit that is set, or -1 if no bit after
	 * beyondBit is set
	 */
	public int anySetBit(int beyondBit) {
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(invariantHolds(), "broken invariant");
		}
		if (++beyondBit >= lengthAsBits) {
			return -1;
		}
		int i = udiv8(beyondBit);
		byte v = (byte)(value[i] << umod8(beyondBit));
		if (v != 0) {
			return (beyondBit + firstSet(v));
		}
		final int numbytes = getLengthInBytes();
		for (++i; i < numbytes; ++i) {
			v = value[i];
			if (v == 0) continue;
			return (umul8(i) + firstSet(v));
		}
		return -1;
	}

	/**
	 * Bitwise OR this FormatableBitSet with another
	 * FormatableBitSet. The result is stored in this bitset. The
	 * operand is unaffected. A null operand is treated as an empty
	 * bitset (i.e. a noop). A bitset that is smaller than its operand
	 * is expanded to the same size.
	 *
	 * @param otherBit bitset operand
	 */
	public void or(FormatableBitSet otherBit)
	{
		if (otherBit == null) {
			return;
		}
		int otherLength = otherBit.getLength();

		if (otherLength > getLength()) {
			grow(otherLength);
		}

		int obByteLen = otherBit.getLengthInBytes();
		for (int i = 0; i < obByteLen; ++i) {
			value[i] |= otherBit.value[i];
		}
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(invariantHolds(),"or() broke invariant");
		}
	}

    /**
     * Copy the bytes from another FormatableBitSet. Assumes that this bit set
     * is at least as large as the argument's bit set.
     */
    public  void    copyFrom( FormatableBitSet that )
    {
        System.arraycopy( that.getByteArray(), 0, value, 0, that.getLengthInBytes());
    }

	/**
	 * Bitwise AND this FormatableBitSet with another
	 * FormatableBitSet. The result is stored in this bitset. The
	 * operand is unaffected. A null operand is treated as an empty
	 * bitset (i.e. clearing this bitset). A bitset that is smaller
	 * than its operand is expanded to the same size.
	 * @param otherBit bitset operand
	 */
	public void and(FormatableBitSet otherBit)
	{
		if (otherBit == null) {
			clear();
			return;
		}
		int otherLength = otherBit.getLength();

		if (otherLength > getLength()) {
			grow(otherLength);
		}

		// Since this bitset is at least as large as the other bitset,
		// one can use the length of the other bitset in the iteration
		int byteLength = otherBit.getLengthInBytes();
		int i = 0;
		for (; i < byteLength; ++i) {
			value[i] &= otherBit.value[i];
		}

		// If the other bitset is shorter the excess bytes in this
		// bitset must be cleared
		byteLength = getLengthInBytes();
		for (; i < byteLength; ++i) {
			value[i] = 0;
		}
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(invariantHolds(),"and() broke invariant");
		}
	}

	/**
	 * Bitwise XOR this FormatableBitSet with another
	 * FormatableBitSet. The result is stored in this bitset. The
	 * operand is unaffected. A null operand is treated as an empty
	 * bitset (i.e. a noop). A bitset that is smaller than its operand
	 * is expanded to the same size.
	 * @param otherBit bitset operand
	 */
	public void xor(FormatableBitSet otherBit)
	{
		if (otherBit == null) {
			return;
		}
		int otherLength = otherBit.getLength();
		if (otherLength > getLength()) {
			grow(otherLength);
		}

		int obByteLen = otherBit.getLengthInBytes();
		for (int i = 0; i < obByteLen; ++i) {
			value[i] ^= otherBit.value[i];
		}
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(invariantHolds(),"xor() broke invariant");
		}
	}

	/**
	 * Get a count of the number of bits that are set.
	 *
	 * @return The number of bits that are set.
	 */
	public int getNumBitsSet()
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(invariantHolds(),"broken invariant");
		}
		int bitsSet = 0;
		final int numbytes = getLengthInBytes();
		for (int i = 0; i < numbytes; ++i) {
			byte v = value[i];

			// "Truth table", bits set in half-nibble (2 bits):
			//  A | A>>1 | A-=A>>1 | bits set
			// ------------------------------
			// 00 |  00  |    0    |    0
			// 01 |  00  |    1    |    1
			// 10 |  01  |    1    |    1
			// 11 |  01  |    2    |    2

			// Calculate bits set in each half-nibble in parallel
			//   |ab|cd|ef|gh|
			// - |>a|&c|&e|&g|>
			// ----------------
			// = |ij|kl|mn|op|
			v -= ((v >> 1) & 0x55);

			// Add the upper and lower half-nibbles together and store
			// in each nibble
			//  |&&|kl|&&|op|
			//+ |>>|ij|&&|mn|>>
			//-----------------
			//= |0q|rs|0t|uv|
			v = (byte)((v & 0x33) + ((v >> 2) & 0x33));

			// Add the nibbles together
			//  |&&&&|&tuv|
			//+ |>>>>|0qrs|>>>>
			//-----------------
			//= |0000|wxyz|
			v = (byte)((v & 0x7) + (v >> 4));
			bitsSet += v;
		}
		return bitsSet;
	}

	/////////////////////////////////////////////////////////
	//
	// EXTERNALIZABLE
	//
	/////////////////////////////////////////////////////////
	/**
	 * Format: <UL>
	 *		<LI>int		length in bits  </LI>
	 *		<LI>byte[]					</LI></UL>
	 *
	 * @see java.io.Externalizable#writeExternal
	*/
	public void writeExternal(ObjectOutput out) throws IOException
	{
		// never called when value is null
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(value != null);

		out.writeInt(getLength());
		int byteLen = getLengthInBytes();
		if (byteLen > 0)
		{
			out.write(value, 0, byteLen);
		}
	}

	/** 
	 * Note: gracefully handles zero length
	 * bits -- will create a zero length array
	 * with no bits being used.  Fortunately
	 * in.read() is ok with a zero length array
	 * so no special code.
	 * <p>
	 * WARNING: this method cannot be changed w/o
	 * changing SQLBit because SQLBit calls this
	 * directly w/o calling read/writeObject(), so
	 * the format id is not stored in that case.
	 *
	 * @see java.io.Externalizable#readExternal
	 */
	public void readExternal(ObjectInput in) throws IOException
	{
		int lenInBits;
		int lenInBytes;

		lenInBits = in.readInt();

		lenInBytes = FormatableBitSet.numBytesFromBits(lenInBits);


		/*
		** How can lenInBytes be zero?  The implication is
		** that lenInBits is zero.  Well, the reason this can
		** happen is that the store will reset our stream
		** out from underneath us if we are a Bit column that
		** overflows onto another page because it assumes that
		** we want to stream it in specially.  Because of this warped
		** API, our readInt() will return 0 even though our
		** writeExternal() did a writeInt(xxx).  The upshot
		** is that you should leave the following alone.
		*/

			value = new byte[lenInBytes];

			in.readFully(value);

			bitsInLastByte = numBitsInLastByte(lenInBits);

			lengthAsBits = lenInBits;
	}

	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.BITIMPL_V01_ID; }
}
