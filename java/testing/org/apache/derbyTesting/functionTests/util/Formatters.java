/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.util
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derbyTesting.functionTests.util;


public class Formatters { 
	/**
		IBM Copyright &copy notice.
	*/
	private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;

	static final char[] hexDigits = { '0', '1', '2', '3',
									  '4', '5', '6', '7',
									  '8', '9', 'A', 'B',
									  'C', 'D', 'E', 'F' };

	/** This method converts the non-ASCII characters in the input
	 *  parameter to unicode escape sequences.
	 * @param in    String to format
	 * @return String containing unicode escape sequences for non-ASCII chars
	 */
	public static String format(String in) {
		if (in == null)
			return null;

		StringBuffer out = new StringBuffer(in.length());
		char hexValue[] = new char[4];

		for (int i = 0; i < in.length(); i++) {
			char inChar = in.charAt(i);

			if (inChar < 128) {
				out.append(inChar);
			} else {
				out.append("\\u");

				int number = (int) inChar;

				int digit = number % 16;

				hexValue[3] = hexDigits[digit];

				number /= 16;

				digit = number % 16;

				hexValue[2] = hexDigits[digit];

				number /= 16;

				digit = number %16;

				hexValue[1] = hexDigits[digit];

				number /= 16;

				digit = number % 16;

				hexValue[0] = hexDigits[digit];

				out.append(hexValue);
			}
		}

		return out.toString();
	}


	/**
	 * repeatChar is used to create strings of varying lengths.
	 * called from various tests to test edge cases and such.
	 *
	 * @param c             character to repeat
	 * @param repeatCount   Number of times to repeat character
	 * @return              String of repeatCount characters c
	 */
   public static String repeatChar(String c, int repeatCount)
   {
	   char ch = c.charAt(0);

	   char[] chArray = new char[repeatCount];
	   for (int i = 0; i < repeatCount; i++)
	   {
		   chArray[i] = ch;
	   }

	   return new String(chArray);

   }

	/**
	 * Pads out a string to the specified size
	 *
	 * @param oldValue value to be padded
	 * @param size     size of resulting string
	 * @return oldValue padded with spaces to the specified size
	 */
	public static String padString(String oldValue, int size)
	{
		String newValue = oldValue;
		if (newValue != null)
		{
			char [] newCharArr = new char[size];					
			oldValue.getChars(0,oldValue.length(),newCharArr,0);
			java.util.Arrays.fill(newCharArr,oldValue.length(),
								  newCharArr.length -1, ' ');
			newValue = new String (newCharArr);
		}
			
		return newValue;
	}

}
