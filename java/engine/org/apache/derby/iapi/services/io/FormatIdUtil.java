/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.io
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
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
