/*

   Derby - Class org.apache.derby.iapi.services.io.FormatIdUtil

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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
}
