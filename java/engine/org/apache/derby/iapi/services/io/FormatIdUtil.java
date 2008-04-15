/*

   Derby - Class org.apache.derby.iapi.services.io.FormatIdUtil

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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
  Utility class with static methods for constructing and reading the byte array
  representation of format id's.

  <P>This utility supports a number of families of format ids. The byte array
  form of each family is a different length. In all cases the first two bits
  of the first byte indicate the family for an id. The list below describes
  each family and gives its two bit identifier in parens.

  <UL> 
  <LI> (0) - The format id is a one byte number between 0 and 63 inclusive. 
             The byte[] encoding fits in one byte.
  <LI> (1) - The format id is a two byte number between 16384 to 32767
             inclusive. The byte[] encoding stores the high order byte
			 first. 
  <LI> (2) - The format id is four byte number between 2147483648 and
             3221225471 inclusive. The byte[] encoding stores the high
			 order byte first.
  <LI> (3) - Future expansion.
  </UL>
 */
public final class FormatIdUtil
{
	private	static	final	int		BYTE_MASK = 0xFF;
	private	static	final	int		NIBBLE_MASK = 0xF;
	private	static	final	int		NIBBLE_SHIFT = 4;
	private	static	final	int		HEX_RADIX = 16;

	private FormatIdUtil() {
	}

	public static int getFormatIdByteLength(int formatId) {
			return 2;
	}

	public static void writeFormatIdInteger(DataOutput out, int formatId) throws IOException {
		out.writeShort(formatId);
	}

	public static int readFormatIdInteger(DataInput in)
		throws IOException {

		return in.readUnsignedShort();
	}

	public static int readFormatIdInteger(byte[] data) {

		int a = data[0];
		int b = data[1];
		return (((a & 0xff) << 8) | (b & 0xff));
	}

	public static String formatIdToString(int fmtId) {

		return Integer.toString(fmtId);
	}

	/**
	 * <p>
	 * Encode a byte array as a string.
	 * </p>
	 */
	public	static	String	toString( byte[] written, int count )
	{
		char[]	chars = new char[ count * 2 ];
		int		charIdx = 0;

		for ( int i = 0; i < count; i++ )
		{
			int		current = written[ i ] & BYTE_MASK;
			int		lowNibble = current & NIBBLE_MASK;
			int		highNibble = current >>> NIBBLE_SHIFT;

			chars[ charIdx++ ] = encodeNibble( lowNibble );
			chars[ charIdx++ ] = encodeNibble( highNibble );
		}

		return new String( chars );
	}

	/**
	 * <p>
	 * Decode a byte array which had been encoded as a string.
	 * </p>
	 */
	public	static	byte[]	fromString( String objString )
	{
		char[]	chars = objString.toCharArray();
		int		count = chars.length;
		byte[]	bytes = new byte[ count / 2 ];
		int		byteIdx = 0;

		for ( int i = 0; i < count; i = i + 2 )
		{
			int lowNibble = decodeNibble( chars[ i ] );
			int highNibble = decodeNibble( chars[ i + 1 ] );

			bytes[ byteIdx++ ] = (byte) ( ( highNibble << NIBBLE_SHIFT ) | lowNibble );
		}

		return bytes;
	}

	private	static	char	encodeNibble( int nibble )
	{
		return Character.forDigit( nibble, HEX_RADIX );
	}

	private	static	int		decodeNibble( char c )
	{
		return Character.digit( c, HEX_RADIX );
	}
    
}
