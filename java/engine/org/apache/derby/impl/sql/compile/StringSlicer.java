/*

   Derby - Class org.apache.derby.impl.sql.compile.StringSlicer

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

