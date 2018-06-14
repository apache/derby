/*

   Derby - Class org.apache.derby.iapi.util.StringUtil

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

package org.apache.derby.iapi.util;

import java.util.Locale;

/**
	A set of public static methods for dealing with Strings
*/
public class StringUtil 
{
	/**
	 * Used to print out a string for error messages, 
	 * chops is off at 60 chars for historical reasons.
	 */
	public final static String formatForPrint(String input)
	{
		if (input.length() > 60)
		{
			StringBuffer tmp = new StringBuffer(input.substring(0, 60));
			tmp.append("&");
			input = tmp.toString();
		}
		return input;
	}

	/**
	 * A method that receive an array of Objects and return a 
	 * String array representation of that array.
	 */
	public static String [] toStringArray(Object [] objArray) {
		int idx;
		int len=objArray.length;
		String [] strArray=new String[len];

		for (idx=0 ; idx<len ; idx++) {
			strArray[idx]=objArray[idx].toString();
		}

		return strArray;
	}

	/**
		Get 7-bit ASCII character array from input String.
		The lower 7 bits of each character in the input string is assumed to be
		the ASCII character value.

     Hexadecimal - Character

     | 00 NUL| 01 SOH| 02 STX| 03 ETX| 04 EOT| 05 ENQ| 06 ACK| 07 BEL|
     | 08 BS | 09 HT | 0A NL | 0B VT | 0C NP | 0D CR | 0E SO | 0F SI |
     | 10 DLE| 11 DC1| 12 DC2| 13 DC3| 14 DC4| 15 NAK| 16 SYN| 17 ETB|
     | 18 CAN| 19 EM | 1A SUB| 1B ESC| 1C FS | 1D GS | 1E RS | 1F US |
     | 20 SP | 21  ! | 22  " | 23  # | 24  $ | 25  % | 26  &amp; | 27  ' |
     | 28  ( | 29  ) | 2A  * | 2B  + | 2C  , | 2D  - | 2E  . | 2F  / |
     | 30  0 | 31  1 | 32  2 | 33  3 | 34  4 | 35  5 | 36  6 | 37  7 |
     | 38  8 | 39  9 | 3A  : | 3B  ; | 3C  &lt; | 3D  = | 3E  &gt; | 3F  ? |
     | 40  @ | 41  A | 42  B | 43  C | 44  D | 45  E | 46  F | 47  G |
     | 48  H | 49  I | 4A  J | 4B  K | 4C  L | 4D  M | 4E  N | 4F  O |
     | 50  P | 51  Q | 52  R | 53  S | 54  T | 55  U | 56  V | 57  W |
     | 58  X | 59  Y | 5A  Z | 5B  [ | 5C  \ | 5D  ] | 5E  ^ | 5F  _ |
     | 60  ` | 61  a | 62  b | 63  c | 64  d | 65  e | 66  f | 67  g |
     | 68  h | 69  i | 6A  j | 6B  k | 6C  l | 6D  m | 6E  n | 6F  o |
     | 70  p | 71  q | 72  r | 73  s | 74  t | 75  u | 76  v | 77  w |
     | 78  x | 79  y | 7A  z | 7B  { | 7C  | | 7D  } | 7E  ~ | 7F DEL|

	 */
	public static byte[] getAsciiBytes(String input)
	{
		char[] c = input.toCharArray();
		byte[] b = new byte[c.length];
		for (int i = 0; i < c.length; i++)
			b[i] = (byte)(c[i] & 0x007F);

		return b;
	}

    /**
     * Trim off trailing blanks but not leading blanks
     *
     * @param str
     *
     * @return The input with trailing blanks stipped off
     */
    public static String trimTrailing( String str)
    {
        if( str == null)
            return null;
        int len = str.length();
        for( ; len > 0; len--)
        {
            if( ! Character.isWhitespace( str.charAt( len - 1)))
                break;
        }
        return str.substring( 0, len);
    } // end of trimTrailing


	/**
		Truncate a String to the given length with no warnings
		or error raised if it is bigger.
	
		@param	value String to be truncated
		@param	length	Maximum length of string

		@return Returns value if value is null or value.length() is less or equal to than length, otherwise a String representing
			value truncated to length.
	*/
	public static String truncate(String value, int length)
	{
		if (value != null && value.length() > length)
			value = value.substring(0, length);
		return value;
	}
	
	/**
	 * Return a slice (substring) of the passed in value, optionally trimmed.
	 * WARNING - endOffset is inclusive for historical reasons, unlike
	 * String.substring() which has an exclusive ending offset.
	 * @param value Value to slice, must be non-null.
	 * @param beginOffset Inclusive start character
	 * @param endOffset Inclusive end character
	 * @param trim To trim or not to trim
	 * @return Sliceed value.
	 */
	public static String slice(String value,
			int beginOffset, int endOffset,
			boolean trim)
	{
		String retval = value.substring(beginOffset, endOffset + 1);
		
		if (trim)
			retval = retval.trim();
		
		return retval;
	}


	private static char[] hex_table = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 
        'a', 'b', 'c', 'd', 'e', 'f'
    };


	/**
		Convert a byte array to a String with a hexidecimal format.
		The String may be converted back to a byte array using fromHexString.
		<BR>
		For each byte (b) two characaters are generated, the first character
		represents the high nibble (4 bits) in hexidecimal (<code>b &amp; 0xf0</code>), the second character
		represents the low nibble (<code>b &amp; 0x0f</code>).
		<BR>
		The byte at <code>data[offset]</code> is represented by the first two characters in the returned String.

		@param	data	byte array
		@param	offset	starting byte (zero based) to convert.
		@param	length	number of bytes to convert.

		@return the String (with hexidecimal format) form of the byte array
	*/
	public static String toHexString(byte[] data, int offset, int length)
	{
		StringBuffer s = new StringBuffer(length*2);
		int end = offset+length;

		for (int i = offset; i < end; i++)
		{
			int high_nibble = (data[i] & 0xf0) >>> 4;
			int low_nibble = (data[i] & 0x0f);
			s.append(hex_table[high_nibble]);
			s.append(hex_table[low_nibble]);
		}

		return s.toString();
	}

	/**
		Convert a hexidecimal string generated by toHexString() back
		into a byte array.

		@param s String to convert
		@param offset starting character (zero based) to convert.
		@param length number of characters to convert.

		@return the converted byte array. Returns null if the length is
		not a multiple of 2.
	*/
	public static byte[] fromHexString(String s, int offset, int length)
	{
		if ((length%2) != 0)
			return null;

		byte[] byteArray = new byte[length/2];

		int j = 0;
		int end = offset+length;
		for (int i = offset; i < end; i += 2)
		{
			int high_nibble = Character.digit(s.charAt(i), 16);
			int low_nibble = Character.digit(s.charAt(i+1), 16);

			if (high_nibble == -1 || low_nibble == -1)
			{
				// illegal format
				return null;
			}

			byteArray[j++] = (byte)(((high_nibble << 4) & 0xf0) | (low_nibble & 0x0f));
		}
		return byteArray;
	}
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

	// The functions below are used for uppercasing SQL in a consistent manner.
	// Derby will uppercase Turkish to the English locale to avoid i
	// uppercasing to an uppercase dotted i. In future versions, all 
	// casing will be done in English.   The result will be that we will get
	// only the 1:1 mappings  in 
	// http://www.unicode.org/Public/3.0-Update1/UnicodeData-3.0.1.txt
	// and avoid the 1:n mappings in 
	//http://www.unicode.org/Public/3.0-Update1/SpecialCasing-3.txt
	// 
	// Any SQL casing should use these functions


	/** Convert string to uppercase
	 * Always use the java.util.ENGLISH locale
	 * @param s   string to uppercase
	 * @return uppercased string
	 */
	public static String SQLToUpperCase(String s)
	{
			return s.toUpperCase(Locale.ENGLISH);
	}

	/** Compares two strings 
	 * Strings will be uppercased in english and compared
	 * equivalent to s1.equalsIgnoreCase(s2)
	 * throws NPE if s1 is null
	 *
	 * @param s1  first string to compare
	 * @param s2  second string to compare
	 *
	 * @return   true if the two upppercased ENGLISH values are equal
	 *           return false if s2 is null
	 */
	public static boolean SQLEqualsIgnoreCase(String s1, String s2)
	{
		if (s2 == null)
			return  false;
		else
			return SQLToUpperCase(s1).equals(SQLToUpperCase(s2));

	}


	/**
	 * Normalize a SQL identifer, up-casing if &lt;regular identifer&gt;,
	 * and handling of &lt;delimited identifer&gt; (SQL 2003, section 5.2).
	 * The normal form is used internally in Derby.
	 *
	 * @param id syntacically correct SQL identifier
	 */
	public static String normalizeSQLIdentifier(String id) {
		if (id.length() == 0) {
			return id;
		}

		if (id.charAt(0) == '"' &&
				id.length() >= 3   &&
				id.charAt(id.length() - 1) == '"') {
			// assume syntax is OK, thats is, any quotes inside are doubled:

			return StringUtil.compressQuotes(
				id.substring(1, id.length() - 1), "\"\"");

		} else {
			return StringUtil.SQLToUpperCase(id);
		}
	}


	/**
	 * Compress 2 adjacent (single or double) quotes into a single (s or d)
	 * quote when found in the middle of a String.
	 *
	 * NOTE:  """" or '''' will be compressed into "" or ''.
	 * This function assumes that the leading and trailing quote from a
	 * string or delimited identifier have already been removed.
	 * @param source string to be compressed
	 * @param quotes string containing two single or double quotes.
	 * @return String where quotes have been compressed
	 */
	public static String compressQuotes(String source, String quotes)
	{
		String	result = source;
		int		index;

		/* Find the first occurrence of adjacent quotes. */
		index = result.indexOf(quotes);

		/* Replace each occurrence with a single quote and begin the
		 * search for the next occurrence from where we left off.
		 */
		while (index != -1) {
			result = result.substring(0, index + 1) +
					 result.substring(index + 2);
			index = result.indexOf(quotes, index + 1);
		}

		return result;
	}

    /**
     * Quote a string so that it can be used as an identifier or a string
     * literal in SQL statements. Identifiers are surrounded by double quotes
     * and string literals are surrounded by single quotes. If the string
     * contains quote characters, they are escaped.
     *
     * @param source the string to quote
     * @param quote the character to quote the string with (' or &quot;)
     * @return a string quoted with the specified quote character
     * @see #quoteStringLiteral(String)
     * @see IdUtil#normalToDelimited(String)
     */
    static String quoteString(String source, char quote) {
        // Normally, the quoted string is two characters longer than the source
        // string (because of start quote and end quote).
        StringBuffer quoted = new StringBuffer(source.length() + 2);
        quoted.append(quote);
        for (int i = 0; i < source.length(); i++) {
            char c = source.charAt(i);
            // if the character is a quote, escape it with an extra quote
            if (c == quote) quoted.append(quote);
            quoted.append(c);
        }
        quoted.append(quote);
        return quoted.toString();
    }

    /**
     * Quote a string so that it can be used as a string literal in an
     * SQL statement.
     *
     * @param string the string to quote
     * @return the string surrounded by single quotes and with proper escaping
     * of any single quotes inside the string
     */
    public static String quoteStringLiteral(String string) {
        return quoteString(string, '\'');
    }

	/**
	 * Utility for formatting which bends a multi-line string into shape for
	 * outputting it in a context where there is <i>depth</i> tabs. Trailing
	 * newlines are discarded as well.
	 * <p>
	 * Replace     "^[\t]*" with "depth" number of tabs.<br>
	 * Replace     "\n+$" with "".
	 * Replace all "\n[\t]*" with "\n" + "depth" number of tabs.<br>
	 * </p>
	 * @param formatted string to sanitize
	 * @param depth indentation level the string is to be printed at (0,1,2..)
	 */
	public static String ensureIndent(String formatted, int depth) {
		StringBuffer indent = new StringBuffer();

		while (depth-- > 0) {
			indent.append("\t");
		}

		if (formatted == null) {
			return indent.toString() + "null";
		}

		/*
		 * Sadly, we can't use java.util.regexp here since it's not supported
		 * by Foundation 1.1
		 */

		formatted = doRegExpA(formatted, indent.toString());

		formatted = doRegExpB(formatted);

		formatted = doRegExpC(formatted, indent.toString());

		return formatted;
	}

	/**
	 * Reg.exp substitute:<br/>
	 * <p/>
	 * Pattern pat_a = Pattern.compile("\\A\\t*");<br/>
	 * Matcher m_a = pat_a.matcher(src);<br/>
	 * src = m_a.replaceFirst(indent.toString());<br/>
	 *
	 * @param src source string in which to substitute indent
	 * @param indent indentation to lead source
	 * @return new version of src after substitution
	 *
	 */
	private static String doRegExpA(String src, String indent) {
		StringBuffer result = new StringBuffer();
		int idx = 0;

		while (idx < src.length() && src.charAt(idx) == '\t') {
			idx++;
		}

		result.append(indent);
		result.append(src.substring(idx));
		return result.toString();
	}

	/**
	 * Reg.exp substitute:<br/>
	 * <p/>
	 * Pattern pat_b = Pattern.compile("\\n+\\Z");<br/>
	 * Matcher m_b = pat_b.matcher(formatted);<br/>
	 * formatted = m_b.replaceFirst("");<br/>
	 *
	 * @param src source string in which to substitute
	 * @return new version of src after substitution
	 *
	 */
	private static String doRegExpB(String src) {
		StringBuffer result = new StringBuffer();
		int idx = src.length() - 1;

		while (idx >= 0 && src.charAt(idx) == '\n') {
			idx--;
		}

		result.append(src.substring(0, idx + 1));
		return result.toString();
	}


	/**
	 * Reg.exp substitute:<br/>
	 * <p/>
	 * Pattern pat_c = Pattern.compile("\\n\\t*");<br/>
	 * Matcher m_c = pat_c.matcher(formatted);<br/>
	 * formatted = m_c.replaceAll("\n" + indent.toString());<br/>
	 *
	 * @param src source string in which to substitute indent
	 * @param indent indentation to lead source
	 * @return new version of src after substitution
	 *
	 */
	private static String doRegExpC(String src, String indent) {

		StringBuffer result = new StringBuffer();
		int idx = 0;

		while (idx < src.length()) {
			char c = src.charAt(idx);

			if (c == '\n') {
				result.append(c);
				int tabidx = idx + 1;

				while (tabidx < src.length() && src.charAt(tabidx) == '\t') {
					tabidx++;
				}

				result.append(indent);
				idx = tabidx;
			} else {
				result.append(c);
				idx++;
			}
		}
		return result.toString();
	}

    /**
     * Get the short database name from the canonical name.
     */
    public  static  String  shortDBName
        (
         String canonicalDBName,
         char   separatorChar
         )
    {
        int         lastSep = canonicalDBName.lastIndexOf( separatorChar );
        return canonicalDBName.substring( lastSep + 1 );
    }
    
}

