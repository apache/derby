/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.compile;

import	java.lang.Math;

/**
 * This utility class wraps a string, making it possible
 * to extract substrings, given byte offsets into the
 * original string.
 * 
 */
public class StringSlicer
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	private	char[]		charArray;
	private	int			charLength;

	/**
	 * Construct a StringSlicer from a String.
	 *
	 * @param	sourceString	Source string to be sliced.
	 */
	public StringSlicer( String sourceString)
	{
		if ( sourceString == null )
		{
			charArray = null;
			charLength = 0;
		}
		else
		{
			charArray = sourceString.toCharArray();
			charLength = charArray.length;
		}
	}

	/**
	 * Get the byte length of the string.
	 *
	 * @return	byte length of the string.
	 */
    public	int	getCharLength() { return charLength; }


	/**
	 * Get the substring between two byte offsets.
	 *
	 * If the beginning offset is past the end of the string,
	 * returns null. If the ending offset is past the end of
	 * the string, truncates the substring accordingly.
	 *
	 * @param	beginOffset		Start of substring.
	 * @param	endOffset		End of substring.
	 * @param   trimflag        true to trim leading and trailing spaces
	 *
	 * @return	specified substring
	 */
	public	String	slice( int beginOffset, int endOffset, boolean trimflag )
	{
		int		length;
		String	retval;

		if ( charLength == 0 || beginOffset >= charLength || endOffset < beginOffset )
		{ return null; }

		endOffset = Math.min( endOffset, charLength - 1 );
		length = (endOffset - beginOffset) + 1;

		retval = new String( charArray, beginOffset, length );

		// Trim leading and trailing spaces if trimflag is 
		// set (specifically for column defaults Beetle 3913)
		if (trimflag)
			retval = retval.trim();

		return retval;
	}


}

