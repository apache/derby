/*

   Derby - Class org.apache.derby.iapi.services.io.FormatableBitSet

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.services.io;

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.io.InputStream;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
 * FormatableBitSet is implemented as a packed array of bytes.
 *
 * @author Jamie -- originally coded by Jeff
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
	private byte[]	value;
	private	short	bitsInLastByte;

	private transient int	lengthAsBits;

	/**
	 * Niladic Constructor
	 */
	public FormatableBitSet()
	{
	}

	/**
	 * Constructs a Bit with the initial number of bits
	 */
	public FormatableBitSet(int numBits)
	{
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
		value = newValue;
		bitsInLastByte = 8;
		lengthAsBits = calculateLength(newValue.length);
	}

	/**
	 * Constructs a Bit from an array of bytes.
	 *
	 * @param newValue	The array of bytes to make up the new Bit
	 * @param numBits	The number of bits
	 */
	public FormatableBitSet(byte[] newValue, int numBits)
	{
		bitsInLastByte = numBitsInLastByte(numBits);
		lengthAsBits = numBits;

		int lenInBytes = numBytesFromBits(numBits);

		if (lenInBytes == newValue.length) {
			value = newValue;
		} else {
			value = new byte[lenInBytes];
			System.arraycopy(newValue, 0, value, 0, newValue.length);
		}
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
	 * Get the length in bytes of a Bit value
	 *
	 * @return	The length in bytes of this value
	 */
	public int getLengthInBytes()
	{
		if (value == null)
		{
			return 0;
		}

		return FormatableBitSet.numBytesFromBits(lengthAsBits);
	}

	/**
	** Get the length in bits
	**
	** @return The length in bits for this value
	**
	** NOTE: could possibly be changed to a long.  As is
	** we are restricted to 2^(31-3) -> 256meg instead
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
		if (value == null)
			return null;

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

		return value;
	}

	/**
	 * Set the value of the byte array
	 *
	 * @return	The value of the byte array
	 */
	public boolean isNull()
	{
		return this.value == null;
	}

	/**
	 * Grow (widen) a FormatableBitSet to N bis
	 *
	 * @param n	The number of bits you want.  The bits are
	 *			always added as 0 and are appended to the
	 *			least significant end of the bit array.
	 *
	 * ASSUMPTIONS: that all extra bits in the last byte
	 * are zero.
	 */
	public void grow(int n)
	{
		if (n <= this.getLength())
			return;

		if (value == null)
		{
			initializeBits(n);
			return;
		}

		int delta = n - this.getLength();


		int oldNumBytes = getLengthInBytes();

		/*
		** If we have enough space in the left over bits,
		** then all we need to do is change the modulo.
		*/
		if ((oldNumBytes != 0) &&
		    (8 - this.bitsInLastByte) >= delta)
		{
			this.bitsInLastByte += delta;
			lengthAsBits = n;
			return;
		}

		int newNumBytes = FormatableBitSet.numBytesFromBits(n);

		// is there enough room in the existing array
		if (newNumBytes <= value.length) {
			// ensure the bits are zeroed
			for (int i = oldNumBytes; i <  newNumBytes; i++)
				value[i] = 0;
		} else {


			/*
			** We didn't have enough bytes in value, so we need
			** to create a bigger byte array and use that.
			*/
			byte[] newValue = new byte[newNumBytes];

			System.arraycopy(value, 0, newValue, 0, oldNumBytes);

			value = newValue;
		}
		bitsInLastByte = numBitsInLastByte(n);
		lengthAsBits = n;
	}

	/**
	 * Shrink (narrow) a FormatableBitSet to N bits
	 *
	 * @param n	The number of bits the caller wants.  The
	 * 			bits are always removed from the
	 *			least significant end of the bit array.
	 */
	public FormatableBitSet shrink(int n)
	{
		int		numBytes;
		int		lastByteNum;

		/*
		** Sanity check: we shouldn't shrink down to
		** nothing.
		*/
		if (SanityManager.DEBUG)
		{
			if (value == null)
			{
				SanityManager.THROWASSERT("Attempt to shrink a null Bit"+
						" -- caller should have known better probably");
				return null;
			}
		}

		if (n >= this.getLength())
		{
			return this;
		}


		lastByteNum = numBytesFromBits(n) - 1;
		bitsInLastByte = numBitsInLastByte(n);
		lengthAsBits = n;

		/*
		** Mask out any left over bits in the
		** last byte.  Retain the highest bits.
		*/
		if (bitsInLastByte != 8)
		{
			value[lastByteNum] &= 0xFF00 >> bitsInLastByte;
		}

		return this;
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
	public boolean equals(FormatableBitSet other)
	{
		if (this.getLength() != other.getLength())
		{
			return false;
		}

		return (this.compare(other) == 0);
	}

	/**
	 * Bit comparison.  Compare this with other.
	 * Will always do a byte by byte compare.
	 *
	 * Given 2 similar bits of unequal lengths (x and y),
	 * where x.getLength() < y.getLength() but where:
	 *
	 *	 x[0..x.getLength()] == y[0..x.getLength()]
	 *
	 * then x < y.
	 *
	 *
	 * @param other the other bit to compare to
	 *
	 * @return -1	- if other <  this
	 *			0	- if other == this
	 *			1	- if other >  this
	 *
	 */
	public int compare(FormatableBitSet other)
	{

		int		otherCount, thisCount;
		int		otherLen, thisLen;
		byte[]	otherb;

		otherb = other.value;
		/*
		** By convention, nulls sort low, and null == null
		*/
		if (this.value == null || otherb == null)
		{
			if (this.value != null)	// otherb == null
				return 1;
			if (otherb != null)		// this.value == null
				return -1;
			return 0;				// both null
		}

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
	 * Bit concatenation.
	 *
	 * @param other 	the other bit to append to this
	 *
	 * @return Bit -- the newly concatenated bit
	 *
	 */
	public FormatableBitSet concatenate(FormatableBitSet other)
	{
		int		newLen;
		int		otherLen;
		int		prevLen;
		int		prevLenBytes;
		int		newLenBytes;
		int     otherLenBytes;
		int		i, j;
		byte[]	newValue;
		byte[]	otherValue;
		int		shiftBits;
		int		inByte;


		prevLen = this.getLength();
		prevLenBytes = this.getLengthInBytes();
		otherLen = other.getLength();
		otherValue = other.getByteArray();
		otherLenBytes = other.getLengthInBytes();
		newLen = prevLen + otherLen;
		newLenBytes = numBytesFromBits(newLen);
		newValue = new byte[newLenBytes];


		/*
		** Copy over the entire array in this.value
		** to newLenBytes.
		*/
		for (i = 0; i < prevLenBytes; i++)
		{
			newValue[i] = this.value[i];
		}

		/*
		** Now if we have any bits left over
		** we need to shift them, and keep
		** shifting everything down.  Be careful
		** to handle the case where the bit
		** used to have length 0.
		*/
		shiftBits = (prevLen == 0) ? 8 : this.bitsInLastByte;
		for (j = 0; j < otherLenBytes; j++, i++)
		{
			if (shiftBits == 8)
			{
				newValue[i] = otherValue[j];
			}
			else
			{
				/*
				** Convert to an int because it will get converted
				** on the shift anyway.
				*/
				inByte = (int)otherValue[j];

				/*
				** Mask off the high bits in case they are now
				** turned on if we had the sign bit on.
				*/
				inByte &= (0x100 - 1);

				/*
				** Use the high order bits to finish off
				** the last byte
				*/
				newValue[i-1] |= (inByte >>> shiftBits);

	            /*
		        ** Start the next one with whatever is left, unless
		        ** there is nothing left.
	            */
	            if (i < newLenBytes)
				{
		            newValue[i] |= (inByte << (8 - shiftBits));
				}
			}
		}

		return new FormatableBitSet(newValue, newLen);
	}

    /**
     * Produce a hash code by putting the value bytes into an int, exclusive OR'ing
     * if there are more than 4 bytes.
     *
     * @return the hash code
     */
    public int hashCode()
    {
        if( null == value)
            return 0;
        
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
		if (SanityManager.DEBUG)
		{
			if (position >= this.getLength())
            {
                SanityManager.THROWASSERT(
                   "Attempt to get a bit position (" + position +
                   ")" +
                   "that exceeds the max length (" + this.getLength() + ")");
            }
		}

		try {

			int bytepos = position / 8;
			int bitpos = 7 - (position % 8);

			return ((value[bytepos] & (1 << bitpos)) != 0);

		} catch (ArrayIndexOutOfBoundsException e) {
			// Should not happen, handle it just in case not all cases are tested
			// by insane server.
			return false;
		}
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

		if (SanityManager.DEBUG)
		{
            if (position >= this.getLength())
            {
                SanityManager.THROWASSERT(
				   "Attempt to set a bit position that exceeds the max length ("
                   + this.getLength() + ")");
            }
		}

		// Should not happen, handle it just in case not all cases are tested
		// by insane server.
		if (position >= getLength())
			grow(position);

		int bytepos = position / 8;
		int bitpos = 7 - (position % 8);

		value[bytepos] |= (1 << bitpos);
	}

	/**
	 * Bit clear
	 *
	 * @param position	the bit to clear
	 *
	 */
	public void clear(int position)
	{
		int	bytepos;
		int	bitpos;

		if (SanityManager.DEBUG)
		{
            if (position >= this.getLength())
            {
                SanityManager.THROWASSERT(
                   "Attempt to set a bit position that exceeds the max length ("
                   + this.getLength() + ")");
            }
		}

		// Should not happen, handle it just in case not all cases are tested
		// by insane server.
		if (position >= getLength())
			grow(position);

		bytepos = position / 8;
		bitpos = 7 - (position % 8);

		value[bytepos] &= ~(1 << bitpos);
	}

	/**
	  Clear all the bits in this FormatableBitSet
	  */
	public void clear()
	{
		if (value == null) 
            return;

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
	protected static int
	numBytesFromBits(int bits)
	{
		return (bits == 0) ? 0 : ((bits - 1) / 8) + 1;
	}

	/**
	* Figure out how many bits are in the last
	* byte from the total number of bits.
	*
	* @param	bits	bits
	*
	* @return	the number of bits
	*/
	private static short
	numBitsInLastByte(int bits)
	{
		int modulo = bits % 8;
		return (short)((modulo == 0) ?
				((bits == 0) ? 0 : 8) :
				modulo);
	}

	/**
	 * Translate a hex character to a byte.
	 *
	 * @param hexChar	A character with the value [0-9a-fA-F].
	 *
	 * @return	A byte with the numeric value corresponding to the hex character
	 */
	private static byte
	hexCharToByte(char hexChar)
	{
		byte	byteValue;

		switch (hexChar)
		{
		  case '0':
			byteValue = 0;
			break;

		  case '1':
			byteValue = 1;
			break;

		  case '2':
			byteValue = 2;
			break;

		  case '3':
			byteValue = 3;
			break;

		  case '4':
			byteValue = 4;
			break;

		  case '5':
			byteValue = 5;
			break;

		  case '6':
			byteValue = 6;
			break;

		  case '7':
			byteValue = 7;
			break;

		  case '8':
			byteValue = 8;
			break;

		  case '9':
			byteValue = 9;
			break;

		  case 'a':
		  case 'A':
			byteValue = 0xA;
			break;

		  case 'b':
		  case 'B':
			byteValue = 0xB;
			break;

		  case 'c':
		  case 'C':
			byteValue = 0xC;
			break;

		  case 'd':
		  case 'D':
			byteValue = 0xD;
			break;

		  case 'e':
		  case 'E':
			byteValue = 0xE;
			break;

		  case 'f':
		  case 'F':
			byteValue = 0xF;
			break;

		  default:
			  if (SanityManager.DEBUG)
			  {
				  SanityManager.THROWASSERT("illegal char = " + hexChar);
			  }
		  	throw new IllegalArgumentException();
		}

		return byteValue;
	}

private static char[] decodeArray = {'0', '1', '2', '3', '4', '5', '6', '7',
								'8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

	/**
	 * Format the string into BitSet format: {0, 2, 4, 8} if bits 0, 2, 4, 8
	 * are set.
	 *
	 * @return A new String containing the formatted Bit value
	 */
	public String toString()
	{
		char[]	outChars;
		int		inPosition;
		int		outPosition;
		int 	inByte;

		if (value == null)
		{
			return null;
		}
		{
			// give it a reasonable size
			StringBuffer str = new StringBuffer(getLength()*8*3);
			str.append("{");
			boolean first = true;
			for (inPosition = 0; inPosition < getLength(); inPosition++)
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
	 * If any bit is set, return the bit number of a bit that is set.
	 * If no bit is set, return -1; 
	 *
	 * @return the bit number of a bit that is set, or -1 if no bit is set
	 */
	public int anySetBit()
	{
		int numbytes = getLengthInBytes();
		int bitpos;

		for (int i = 0; i < numbytes-1; i++)
		{
			if (value[i] != 0)
			{
				for (int j = 0; j < 8; j++)
				{
					bitpos = 7-j;
					if (((1 << bitpos) & value[i]) != 0)
						return ((i*8)+j);
				}
			}
		}


		// only the top part of the last byte is relevant
		byte mask = (byte)(0xFF << (8-bitsInLastByte));
		if ((value[numbytes-1] & mask) != 0)
		{
			for (int j = 0; j < bitsInLastByte; j++)
			{
				bitpos = 7-j;
				if (((1 << bitpos) & value[numbytes-1]) != 0)
					return ((numbytes-1)*8)+j;
			}
		}

		return -1;
	}

	/**
	 * Like anySetBit(), but return any set bit whose number is bigger than
	 * beyondBit. If no bit is set after beyondBit, -1 is returned. 
	 * By using anySetBit() and anySetBit(beyondBit), one can quickly go
	 * thru the entire bit array to return all set bit.
	 *
	 * @param beyondBit only look at bit that is greater than this bit number
	 * @return the bit number of a bit that is set, or -1 if no bit after
	 * beyondBit is set
	 */
	public int anySetBit(int beyondBit)
	{
		if (SanityManager.DEBUG)
		{
			if (beyondBit >= this.getLength())
                SanityManager.THROWASSERT(
                   "Attempt to access bit position that exceeds the max length ("
                    + this.getLength() + ")");
		}

		int startingBit = (beyondBit+1);

		// we have seen the last bit.
		if (startingBit >= this.getLength())
			return -1;

		int numbytes = getLengthInBytes();
		int startingByte = startingBit / 8;
		int startingBitpos = startingBit % 8;
		int bitpos;
		byte mask;

		// see if any bits in this byte is set, only the bottom part of the
		// first byte is relevant
		mask = (byte)(0xFF >> startingBitpos);

		if (startingByte == numbytes-1)	// starting byte == last byte 
			mask &= (byte)(0xFF << (8-bitsInLastByte));

		if ((value[startingByte] & mask ) != 0)
		{
			// I know we will see the bit before bitsInLastByte even if we are
			// at the last byte, no harm in going up to 8 in the loop
			for (int j = startingBitpos; j < 8; j++)
			{
				if (SanityManager.DEBUG)
				{
					if (startingByte == numbytes-1)
						SanityManager.ASSERT(j < bitsInLastByte,
								 "going beyond the last bit");
				}
				bitpos = 7-j;
				if (((1 << bitpos) & value[startingByte]) != 0)
				{
					return (startingByte*8+j);
				}
			}	
		}

		for (int i = (startingByte+1); i < numbytes-1; i++)
		{			
			if (value[i] != 0)
			{
				for (int j = 0; j < 8; j++)
				{
					bitpos = 7-j;
					if (((1 << bitpos) & value[i]) != 0)
					{
						return ((i*8)+j);
					}
				}
			}
		}
		
		// Last byte if there are more than one bytes.  Only the top part of
		// the last byte is relevant 
		if (startingByte != numbytes-1)
		{
			mask = (byte)(0xFF << (8-bitsInLastByte));

			if ((value[numbytes-1] & mask) != 0)
			{
				for (int j = 0; j < bitsInLastByte; j++)
				{
					bitpos = 7-j;
					if (((1 << bitpos) & value[numbytes-1]) != 0)
					{
						return ((numbytes-1)*8)+j;	
					}
				}
			}
		}

		return -1;

	}

	/**
	 * Bitwise OR this Bit with another Bit.
	 *
	 * @param otherBit the other Bit
	 * @see Bit#or
	 */
	public void or(FormatableBitSet otherBit)
	{
		if (otherBit == null || otherBit.getLength() == 0)
			return;

		int otherLength = otherBit.getLength();

		if (otherLength > getLength())
			grow(otherLength); // expand this bit 

		if (otherBit instanceof FormatableBitSet)
		{
			// we know the bit ordering, optimize this 
			FormatableBitSet ob = (FormatableBitSet)otherBit;
			int obByteLen = ob.getLengthInBytes();
			for (int i = 0; i < obByteLen-1; i++)
				value[i] |= ob.value[i];

			// do the last byte bit by bit
			for (int i = (obByteLen-1)*8; i < otherLength; i++)
				if (otherBit.isSet(i))
					set(i);
		}
		else
		{
			// we don't know the bit ordering, call thru the interface and go
			// thru bit by bit
			// this bit impl's length >= other bit's length

			for (int i = 0; i < otherLength; i++)
			{
				if (otherBit.isSet(i))
					set(i);
			}
		}
	}

	/**
	 * Bitwise AND this Bit with another Bit.
	 *
	 * @param otherBit the other Bit
	 * @see Bit#or
	 */
	public void and(FormatableBitSet otherBit)
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(otherBit != null, "cannot AND null with a FormatableBitSet");

		int otherLength = otherBit.getLength();

		// Supposedly cannot happen, but handle it just in case.
		if (otherLength > getLength())
			grow(otherLength); // expand this bit 

		if (otherLength < getLength())
		{
			// clear all bits that are not in the other bit
			int startingByte = (otherLength * 8) + 1;
			int len = getLengthInBytes();
			for (int i = startingByte; i < len; i++)
				value[i] = 0;

			for (int i = otherLength; i < startingByte*8; i++)
			{
				if (i < getLength())
					clear(i);
				else
					break;
			}
		}

		if (otherLength == 0)
			return;
			
		int length = otherBit.getLengthInBytes() < getLengthInBytes() ? 
			otherBit.getLengthInBytes() : getLengthInBytes();

		for (int i = 0; i < length; i++)
			value[i] &= otherBit.value[i];
	}

	/**
	 * Logically XORs this FormatableBitSet with the specified FormatableBitSet.
	 * @param set	The FormatableBitSet to be XORed with.
	 */
	public void xor(FormatableBitSet set)
	{
		if (SanityManager.DEBUG)
		{
			if (getLength() != set.getLength())
			{
				SanityManager.THROWASSERT("getLength() (" + getLength() +
					") and set.getLength() (" +
					set.getLength() +
					") expected to be the same");
			}
		}

		int setLength = set.getLength();
		for (int i = setLength; i-- > 0; )
		{
			if (isSet(i) && set.isSet(i))
			{
				clear(i);
			}
			else if (isSet(i) || set.isSet(i))
			{
				set(i);
			}
		}
	}

	/**
	 * Get a count of the number of bits that are set.
	 *
	 * @return The number of bits that are set.
	 */
	public int getNumBitsSet()
	{
		int count = 0;

		for (int index = getLength() - 1; index >= 0; index--)
		{
			if (isSet(index))
			{
				count++;
			}
		}

		return count;
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

	public void readExternalFromArray(ArrayInputStream in) throws IOException
	{
		int lenInBits = in.readInt();

		int lenInBytes = FormatableBitSet.numBytesFromBits(lenInBits);

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
