/*

   Derby - Class org.apache.derbyTesting.unitTests.util.BitUtil

   Copyright 1998, 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.unitTests.util;


/**
 * This class provides utility methods for 
 * converting byte arrays to hexidecimal Strings and manipulating BIT/BIT VARYING values as a
 * packed vector of booleans.
 * 
 * <P> The BIT/BIT VARYING methods are modeled after
 * some methods in the <I>java.util.BitSet</I> class. 
 * An alternative to using a SQL BIT (VARYING) column
 * in conjunction with the methods provided herein to
 * provide bit manipulation would be to use a serialized 
 * <I>java.util.BitSet</I> column instead.
 * <p>
 * This class contains the following static methods: <UL>
 * <LI> void <B>set</B>(byte[] bytes, int position) to set a bit</LI>
 * <LI> void <B>clear</B>(byte[] bytes, int position) to clear a bit</LI>
 * <LI> boolean <B>get</B>(byte[] bytes, int position) to get the 
 *		bit status </LI> </UL>
 * <p>
 * Since these methods effectively allow a SQL BIT to be
 * considered as an array of booleans, all offsets (position
 * parameters) are zero based.  So if you want to set
 * the first bit of a BIT type, you would use <I> 
 * set(MyBitColumn, 0) </I>.
 * <p> 
 * Examples: <UL>
 * <LI> SELECT BitUtil::get(bitcol, 2) FROM mytab </LI>
 * <LI> UPDATE mytab SET bitcol = BitUtil::set(bitcol, 2)  </LI>
 * <LI> UPDATE mytab SET bitcol = BitUtil::clear(bitcol, 2)  </LI> </UL>
 *
 */ 
public class BitUtil
{
	/**
	 * Set the bit at the specified position
	 *
	 * @param bytes		the byte array
	 * @param position	the bit to set, starting from zero
	 *
	 * @return the byte array with the set bit
	 *
	 * @exception IndexOutOfBoundsException on bad position
	 */
	public static byte[] set(byte[] bytes, int position)
	{
		if (position >= 0)
		{
			int bytepos = position >> 3;
			if (bytepos < bytes.length)
			{
				int bitpos = 7 - (position % 8);

				bytes[bytepos] |= (1 << bitpos);
				return bytes;
			}
		}
		throw new IndexOutOfBoundsException(Integer.toString(position));
	}

	/**
	 * Clear the bit at the specified position
	 *
	 * @param bytes		the byte array
	 * @param position	the bit to clear, starting from zero
	 *
	 * @return the byte array with the cleared bit
	 *
	 * @exception IndexOutOfBoundsException on bad position
	 */
	public static byte[] clear(byte[] bytes, int position)
	{
		if (position >= 0)
		{
			int bytepos = position >> 3;
			if (bytepos < bytes.length)
			{
				int bitpos = 7 - (position % 8);
				bytes[bytepos] &= ~(1 << bitpos);
				return bytes;
			}
		}
		
		throw new IndexOutOfBoundsException(Integer.toString(position));
	}

	/**
	 * Check to see if the specified bit is set
	 *
	 * @param bytes		the byte array
	 * @param position	the bit to check, starting from zero
	 *
	 * @return true/false
	 *
	 * @exception IndexOutOfBoundsException on bad position
	 */
	public static boolean get(byte[] bytes, int position)
	{
		if (position >= 0)
		{
			int bytepos = position >> 3;
			if (bytepos < bytes.length)
			{
				int bitpos = 7 - (position % 8);
				return ((bytes[bytepos] & (1 << bitpos)) != 0);
			}
		}
		throw new IndexOutOfBoundsException(Integer.toString(position));
	}

	private static char[] hex_table = {
                '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 
                'a', 'b', 'c', 'd', 'e', 'f'
            };

	/**
		Convert a byte array to a human-readable String for debugging purposes.
	*/
	public static String hexDump(byte[] data)
	{
            byte byte_value;


            StringBuffer str = new StringBuffer(data.length * 3);

            str.append("Hex dump:\n");

            for (int i = 0; i < data.length; i += 16)
            {
                // dump the header: 00000000: 
                String offset = Integer.toHexString(i);

                // "0" left pad offset field so it is always 8 char's long.
                for (int offlen = offset.length(); offlen < 8; offlen++) 
                    str.append("0");
                str.append(offset);
                str.append(":");

                // dump hex version of 16 bytes per line.
                for (int j = 0; (j < 16) && ((i + j) < data.length); j++)
                {
                    byte_value = data[i + j];

                    // add spaces between every 2 bytes.
                    if ((j % 2) == 0)
                        str.append(" ");

                    // dump a single byte.
                    byte high_nibble = (byte) ((byte_value & 0xf0) >>> 4); 
                    byte low_nibble  = (byte) (byte_value & 0x0f); 

                    str.append(hex_table[high_nibble]);
                    str.append(hex_table[low_nibble]);
                }

                // dump ascii version of 16 bytes
                str.append("  ");

                for (int j = 0; (j < 16) && ((i + j) < data.length); j++)
                {
                    char char_value = (char) data[i + j]; 

                    // RESOLVE (really want isAscii() or isPrintable())
                    if (Character.isLetterOrDigit(char_value))
                        str.append(String.valueOf(char_value));
                    else
                        str.append(".");
                }
                    
                // new line
                str.append("\n");
            }
            return(str.toString());

	}
}
