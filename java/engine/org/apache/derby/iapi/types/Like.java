/*

   Derby - Class org.apache.derby.iapi.types.Like

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.types;

// RESOLVE: MOVE THIS CLASS TO PROTOCOL (See LikeOperatorNode)

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;

import java.text.CollationElementIterator;
import java.text.Collator;
import java.text.RuleBasedCollator;
import java.util.Locale;

/**
	Like matching algorithm. Not too speedy for %s.

	SQL92 says the escape character can only and must be followed
	by itself, %, or _.  So if you choose % or _ as the escape character,
	you can no longer do that sort of matching.

	Not the most recent Like -- missing the unit tests

	@author ames
 */
public class Like {
	private static final char anyChar = '_';
	private static final char anyString = '%';

	private static final String SUPER_STRING = "\uffff";

	private Like() { // do not instantiate
	}

	/**
		@param val value to compare. if null, result is null.
		@param valLength length of val
		@param pat pattern to compare. if null, result is null.
		@param patLength length of pat
		@param escape escape character. Must be 1 char long.
			if null, no escape character is used.
		@param escapeLength length of escape

		@return null if val or pat null, otherwise true if match
		and false if not.
		@exception StandardException thrown if data invalid
	 */
	public static Boolean like
	(
		char[] 	val, 
		int 	valLength, 
		char[] 	pat, 
		int 	patLength, 
		char[] 	escape,
		int 	escapeLength
	) throws StandardException 
	{
		return like(val, 0, valLength, pat, 0, patLength, escape, escapeLength);
	}

	/**
		For national chars.
		@param val value to compare. if null, result is null.
		@param valLength length of val
		@param pat pattern to compare. if null, result is null.
		@param patLength length of pat
		@param escape escape character. Must be 1 char long.
			if null, no escape character is used.
		@param escapeLength length of escape
		@param collator	The collator to use.

		@return null if val or pat null, otherwise true if match
		and false if not.
		@exception StandardException thrown if data invalid
	 */
	public static Boolean like
	(
		int[] 	val, 
		int 	valLength, 
		int[] 	pat, 
		int 	patLength, 
		int[] 	escape,
		int 	escapeLength,
		RuleBasedCollator collator
	) throws StandardException 
	{
		return like(val, 0, valLength, pat, 0, patLength, escape, escapeLength, collator);
	}

	/* non-national chars */
	private static Boolean like
	(
		char[] 	val, 
		int 	vLoc, 	// start at val[vLoc]
		int 	vEnd, 	// end at val[vEnd]
		char[] 	pat, 
		int 	pLoc, 	// start at pat[pLoc]
		int 	pEnd, 	// end at pat[pEnd]
		char[] 	escape,
		int 	escapeLength
	) throws StandardException 
	{
		char escChar = ' ';
		boolean haveEsc = true;
		
		if (val == null) return null;
		if (pat == null) return null;

		if (escape == null)
		{
			haveEsc = false;
		}
		else
		{
			escChar = escape[0];
		}

		Boolean result;

		while (true) {

			if ((result = checkLengths(vLoc, vEnd, pLoc, pat, pEnd)) != null) 
			{
				return result;
			}

			// go until we get a special char in the pattern or hit EOS
			while (pat[pLoc] != anyChar && pat[pLoc] != anyString &&
					((! haveEsc) || pat[pLoc] != escChar)) {
				if (val[vLoc] == pat[pLoc]) 
				{
					vLoc++; pLoc++;
	
					result = checkLengths(vLoc, vEnd, pLoc, pat, pEnd);
					if (result != null) 
						return result;
				}
				else 
				{
					return Boolean.FALSE;
				}
			}

			// deal with escChar first, as it can be escaping a special char
			// and can be a special char itself.
			if (haveEsc && pat[pLoc] == escChar) {
				pLoc++;
				if (pLoc == pEnd) {
					throw StandardException.newException(SQLState.LANG_INVALID_ESCAPE_SEQUENCE);
				}
				if (pat[pLoc] != escChar &&
				    pat[pLoc] != anyChar &&
				    pat[pLoc] != anyString) {
					throw StandardException.newException(SQLState.LANG_INVALID_ESCAPE_SEQUENCE);
				}
				// regardless of the char in pat, it must match exactly:
				if (val[vLoc] == pat[pLoc]) {
					vLoc++; pLoc++;
	
					result = checkLengths(vLoc, vEnd, pLoc, pat, pEnd);
					if (result != null) 
						return result;
				}
				else return Boolean.FALSE;
			}
			else if (pat[pLoc] == anyChar) {
				// regardless of the char, it matches
				vLoc++; pLoc++;
	
				result = checkLengths(vLoc, vEnd, pLoc, pat, pEnd);
				if (result != null) 
					return result;
			}
			else if (pat[pLoc] == anyString) {
				// catch the simple cases -- end of the pattern or of the string
				if (pLoc+1 == pEnd)
					return Boolean.TRUE;

				// would return true, but caught in checkLengths above
				if (SanityManager.DEBUG)
					SanityManager.ASSERT(vLoc!=vEnd, 
						"Should have been found already");

				//if (vLoc == vEnd) // caught in checkLengths
					//return Boolean.TRUE;
				// check if remainder of pattern is anyString's
				// if escChar == anyString, we couldn't be here
				boolean anys = true;
				for (int i=pLoc+1;i<pEnd;i++)
					if (pat[i]!=anyString) {
						anys=false;
						break;
					}
				if (anys) return Boolean.TRUE;

				// pattern can match 0 or more chars in value.
				// to test that, we take the remainder of pattern and
				// apply it to ever-shorter  remainders of value until
				// we hit a match.

				// the loop never continues from this point -- we will
				// always generate an answer here.

				// REMIND: there are smarter ways to pick the remainders
				// and do this matching.

				// num chars left in value includes current char
				int vRem = vEnd - vLoc;

				int n=0;

				// num chars left in pattern excludes the anychar
				int minLen = getMinLen(pat, pLoc+1, pEnd, haveEsc, escChar);
				for (int i=vRem; i>=minLen; i--) 
				{
					Boolean restResult = Like.like(val,vLoc+n,vLoc+n+i,pat,pLoc+1,pEnd,escape,escapeLength);
					if (SanityManager.DEBUG)
					{
						if (restResult == null)
						{
							String vStr = new String(val,vLoc+n,i);
							String pStr = new String(pat,pLoc+1,pEnd-(pLoc+1));
							SanityManager.THROWASSERT("null result on like(value = "+vStr+", pat = "+pStr+")");
						}
					}
					if (restResult.booleanValue())
						return restResult;

					n++;
				}
				// none of the possibilities worked 
				return Boolean.FALSE;
			}
		}
	}

	/* national chars */
	private static Boolean like
	(
		int[] 	val, 
		int 	vLoc, 	// start at val[vLoc]
		int 	vEnd, 	// end at val[vEnd]
		int[] 	pat, 
		int 	pLoc, 	// start at pat[pLoc]
		int 	pEnd, 	// end at pat[pEnd]
		int[] 	escape,
		int 	escapeLength,
		RuleBasedCollator	collator
	) throws StandardException 
	{
		int[] escCharInts = null;
		boolean haveEsc = true;
		int[] anyCharInts = new int[1];	// assume only 1 int
		int[] anyStringInts = new int[1];	// assume only 1 int
		
		if (val == null) return null;
		if (pat == null) return null;

		if (escape == null)
		{
			haveEsc = false;
		}
		else
		{
			escCharInts = escape;
		}

		Boolean result;

		// get the collation integer representing "_"
		CollationElementIterator cei =
									collator.getCollationElementIterator("_");
		anyCharInts[0] = cei.next();
		{
			int nextInt;

			// There may be multiple ints representing this character
			while ((nextInt = cei.next()) != CollationElementIterator.NULLORDER)
			{
				int[] temp = new int[anyCharInts.length + 1];
				for (int index = 0; index < anyCharInts.length; index++)
				{
					temp[index] = anyCharInts[index];
				}
				temp[anyCharInts.length] = nextInt;
				anyCharInts = temp;
			}
		}
		// get the collation integer representing "%"
		cei = collator.getCollationElementIterator("%");
		anyStringInts[0] = cei.next();
		{
			int nextInt;

			// There may be multiple ints representing this character
			while ((nextInt = cei.next()) != CollationElementIterator.NULLORDER)
			{
				int[] temp = new int[anyStringInts.length + 1];
				for (int index = 0; index < anyStringInts.length; index++)
				{
					temp[index] = anyStringInts[index];
				}
				temp[anyStringInts.length] = nextInt;
				anyStringInts = temp;
			}
		}

		while (true) 
		{
			// returns null if more work to do, otherwise match Boolean
			result = checkLengths(vLoc, vEnd, pLoc, pat, pEnd, anyStringInts);
			if (result != null) 
				return result;

			// go until we get a special char in the pattern or hit EOS
			while ( (! matchSpecial(pat, pLoc, pEnd, anyCharInts)) &&
					(! matchSpecial(pat, pLoc, pEnd, anyStringInts)) &&
					((! haveEsc)
						|| (! matchSpecial(pat, pLoc, pEnd, escCharInts))))
			{
				if (val[vLoc] == pat[pLoc]) 
				{
					vLoc++; pLoc++;
	
					result = checkLengths(vLoc, vEnd, pLoc,
								pat, pEnd, anyStringInts);
					if (result != null) 
					{
						return result;
					}
				}
				else 
				{
					return Boolean.FALSE;
				}
			}

			// deal with escCharInt first, as it can be escaping a special char
			// and can be a special char itself.
			if (haveEsc && matchSpecial(pat, pLoc, pEnd, escCharInts))
			{
				pLoc += escCharInts.length;
				if (pLoc == pEnd) 
				{
					throw StandardException.newException(
						SQLState.LANG_INVALID_ESCAPE_SEQUENCE);
				}

				int[] specialInts = null;
				if (matchSpecial(pat, pLoc, pEnd, escCharInts))
				{
					specialInts = escCharInts;
				}
				if (matchSpecial(pat, pLoc, pEnd, anyCharInts))
				{
					specialInts = anyCharInts;
				}
				if (matchSpecial(pat, pLoc, pEnd, anyStringInts))
				{
					specialInts = anyStringInts;
				}
				if (specialInts == null)
				{
					throw StandardException.newException(SQLState.LANG_INVALID_ESCAPE_SEQUENCE);
				}
				// regardless of the char in pat, it must match exactly:
				for (int index = 0; index < specialInts.length; index++)
				{
					if (val[vLoc + index] != pat[pLoc + index])
					{
						return Boolean.FALSE;
					}
				}

				vLoc += specialInts.length; 
				pLoc += specialInts.length; 
	
				// returns null if more work to do, otherwise match Boolean
				result = checkLengths(vLoc, vEnd,
						pLoc, pat, pEnd, anyStringInts);

				if (result != null) 
					return result;
			}
			else if (matchSpecial(pat, pLoc, pEnd, anyCharInts))
			{
				// regardless of the char, it matches
				vLoc += anyCharInts.length; 
				pLoc += anyCharInts.length; 
	
				result = checkLengths(vLoc, vEnd, pLoc, pat, pEnd, anyStringInts);
				if (result != null) 
					return result;
			}
			else if (matchSpecial(pat, pLoc, pEnd, anyStringInts))
			{
				// catch the simple cases -- end of the pattern or of the string
				if (pLoc+1 == pEnd)
					return Boolean.TRUE;

				// would return true, but caught in checkLengths above
				if (SanityManager.DEBUG)
					SanityManager.ASSERT(vLoc!=vEnd, 
						"Should have been found already");

				if (vLoc == vEnd)
					return Boolean.TRUE;

				// check if remainder of pattern is anyString's
				// if escChar == anyString, we couldn't be here
				// If there is an escape in the pattern we break
				boolean allPercentChars = true;
				for (int i=pLoc+1;i<pEnd;i++)
				{
					if (! matchSpecial(pat, i, pEnd, anyStringInts))
					{
						allPercentChars=false;
						break;
					}
				}
				if (allPercentChars)
					return Boolean.TRUE;

				// pattern can match 0 or more chars in value.
				// to test that, we take the remainder of pattern and
				// apply it to ever-shorter  remainders of value until
				// we hit a match.

				// the loop never continues from this point -- we will
				// always generate an answer here.

				// REMIND: there are smarter ways to pick the remainders
				// and do this matching.

				// num chars left in value includes current char
				int vRem = vEnd - vLoc;

				int n=0;

				// num chars left in pattern excludes the anyString
				int minLen = getMinLen(pat, pLoc+1, pEnd, haveEsc, escCharInts, anyStringInts);
				for (int i=vRem; i>=minLen; i--) 
				{
					Boolean restResult = Like.like(val,vLoc+n,vLoc+n+i,pat,pLoc+1,pEnd,escape,escapeLength, collator);
					if (SanityManager.DEBUG)
					{
						if (restResult == null)
						{
							SanityManager.THROWASSERT("null result on like(vLoc+n = "+(vLoc+n)+", i = "+i+
													  ", pLoc+1 = " + (pLoc+1) + ", pEnd-(pLoc+1) = " + 
													  (pEnd-(pLoc+1)) + ")");
						}
					}
					if (restResult.booleanValue())
						return restResult;

					n++;
				}
				// none of the possibilities worked 
				return Boolean.FALSE;
			}
		}
	}

	/**
		Calculate the shortest length string that could match this pattern for non-national chars
	 */
	static int getMinLen(char[] pattern, int pStart, int pEnd, boolean haveEsc, char escChar) 
	{
		int m=0;
		for (int l = pStart; l<pEnd; ) 
		{
			if (haveEsc && pattern[l] == escChar) { // need one char
				l+=2;
				m++;
			}
			else if (pattern[l] == anyString) {
				l++; // anyString, nothing needed
			}
			else { // anyChar or other chars, need one char
				l++; m++;
			}
		}
		return m;
	}

	/**
		Calculate the shortest length string that could match this pattern for national chars
	 */
	static int getMinLen(int[] pattern, int pStart, int pEnd, boolean haveEsc, 
						 int[] escCharInts, int[] anyStringInts) 
	{
		int m=0;
		for (int l = pStart; l<pEnd; ) 
		{
			if (haveEsc && matchSpecial(pattern, l, pEnd, escCharInts))
			{ 
				l += escCharInts.length + 1;
				m += escCharInts.length;
			}
			else if (matchSpecial(pattern, l, pEnd, anyStringInts)) 
			{
				l += anyStringInts.length; // anyString, nothing needed
			}
			else 
			{ // anyChar or other chars, need one char
				l++; m++;
			}
		}
		return m;
	}

	/**
	 * checkLengths -- non-national chars 
	 *
	 * Returns null if we are not done.
	 * Returns true if we are at the end of our value and pattern
	 * Returns false if there is more pattern left but out of input value
	 *
	 * @param vLoc current index into char[] val
	 * @param vEnd end index or our value
	 * @param pLoc current index into our char[] pattern
	 * @param pat  pattern char []
	 * @param pEnd end index of our pattern []
	 */

	static Boolean checkLengths(int vLoc, int vEnd,
			int pLoc, char[] pat, int pEnd) 
	{
		if (vLoc == vEnd) 
		{
			if (pLoc == pEnd) 
			{
				return Boolean.TRUE;
			}
			else 
			{
				// if remainder of pattern is anyString chars, ok
				for (int i=pLoc; i<pEnd; i++) 
				{
					if (pat[i] != anyString)
					{
						return Boolean.FALSE; // more to match
					}
				}
				return Boolean.TRUE;
			}
		}
		else if (pLoc == pEnd)
		{
			return Boolean.FALSE; // ran out of pattern
		}
		else return null; // still have strings to match, not done
	}

	/**
	 * checkLengths -- national chars 
	 *
	 * Returns null if we are not done.
	 * Returns true if we are at the end of our value and pattern
	 * Returns false if there is more pattern left but out of input value
	 *
	 * @param vLoc current index into int[] val
	 * @param vEnd end index or our value
	 * @param pLoc current index into our int[] pattern
	 * @param pat  pattern int []
	 * @param pEnd end index of our pattern []
	 */

	static Boolean checkLengths(int vLoc, int vEnd,
			int pLoc, int[] pat, int pEnd, int[] anyStringInts) 
	{
		if (vLoc == vEnd) 
		{
			if (pLoc == pEnd) 
			{
				return Boolean.TRUE;
			}
			else 
			{
				// if remainder of pattern is anyString chars, ok
				for (int i=pLoc; i<pEnd; i += anyStringInts.length) 
				{
					if (! matchSpecial(pat, i, pEnd, anyStringInts))
					{
						return Boolean.FALSE;
					}
				}
				return Boolean.TRUE;
			}
		}
		else if (pLoc == pEnd)
		{
			return Boolean.FALSE; // ran out of pattern
		}
		else return null; // still have strings to match, not done
	}

	/**
	 * matchSpecial
	 *
	 *	check the pattern against the various special character arrays.
	 *  The array can be anyStringInts, anyCharInts or anyEscChars (always 1)
	 */

	private static boolean matchSpecial(int[] pat, int patStart, int patEnd, int[] specialInts)
	{
		//
		// multi-collation units per char can exceed the pattern length
		// and we fall around the 2nd if statement and falsely return true.
		//
		if (specialInts.length > patEnd - patStart)
		    return false;
		if (specialInts.length <= patEnd - patStart)
		{
			for (int index = 0; index < specialInts.length; index++)
			{
				if (pat[patStart + index] != specialInts[index])
				{
					return false; // more to match
				}
			}
		}
		return true;
	}


	/*
		Most typical interface for non-national chars
	 */
	public static Boolean like(char[] value, int valueLength, char[] pattern, int patternLength) throws StandardException { 
		if (value == null || pattern == null) return null;
		return like(value, valueLength, pattern, patternLength, null, 0);
	}

	/*
		Most typical interface for national chars
	 */
	public static Boolean like(int[] value, int valueLength, int[] pattern, int patternLength, RuleBasedCollator collator) 
		throws StandardException 
	{ 
		if (value == null || pattern == null) return null;
		return like(value, valueLength, pattern, patternLength, null, 0, collator);
	}

	// Methods for LIKE transformation at preprocess time:

	/**
	 * Determine whether or not this LIKE can be transformed into optimizable
	 * clauses.  It can if the pattern is non-null, of length > 0 and
	 * the first character is not a wild card.
	 *
	 * @param pattern	The right side of the LIKE
	 *
	 * @return	Whether or not the LIKE can be transformed
	 */

	public static boolean isOptimizable(String pattern)
	{
		if (pattern == null || (pattern.length() == 0))
		{
			return false;
		}

		// if we have pattern matching at start of string, no optimization
		char firstChar = pattern.charAt(0);

		return (firstChar != anyChar && firstChar != anyString);
	}

	public static String greaterEqualStringFromParameter(String pattern)
		throws StandardException {

		if (pattern == null)
			return null;

		return greaterEqualString(pattern, (String) null);
	}

	public static String greaterEqualStringFromParameterWithEsc(String pattern, String escape)
		throws StandardException {

		if (pattern == null)
			return null;

		return greaterEqualString(pattern, escape);
	}

	/**
	 * Return the substring from the pattern for the optimization >= clause.
	 *
	 * @param pattern	The right side of the LIKE
	 * @param escape	The escape clause
	 *
	 * @return	The String for the >= clause
	 */
	public static String greaterEqualString(String pattern, String escape)
	    throws StandardException
	{

		int firstAnyChar = pattern.indexOf(anyChar);
		int firstAnyString = pattern.indexOf(anyString);

		// 
		// For Escape we don't utilize any of the stylish code
		// below but brute force walk the pattern to find out
		// what is there, while stripping escapes
		//

		if ((escape != null) && (escape.length() != 0))
		{
			char escChar = escape.charAt(0);
			if (pattern.indexOf(escChar) != -1)
			{
				// we return a string stripping out the escape char
				// leaving the _? in place as normal chars.
                
				return greaterEqualString(pattern, escChar);
			}
			// drop through if no escape found
		}

		if (firstAnyChar == -1)
		{
			if (firstAnyString == -1)
			{
				return pattern;
			}
			else	// no _, found %
			{
				return pattern.substring(0, firstAnyString);
			}
		}
		else if (firstAnyString == -1)
		{
			return pattern.substring(0, firstAnyChar);
		}
		else
		{
			return pattern.substring(0, (firstAnyChar > firstAnyString) ? 
										firstAnyString :
										firstAnyChar);
		}
	}

    /** 
     *  greaterEqualString -- for Escape clause only
     *  
     *  Walk the pattern character by character
     *  @param pattern like pattern to build from
     *  @param escChar the escape character in the pattern
     */

	private static String greaterEqualString(String pattern, char escChar)
		throws StandardException
	{
		int patternLen = pattern.length();
		char[] patternChars = new char[patternLen];
		char[] result = new char[patternLen];
		pattern.getChars(0, patternLen, patternChars, 0);

		int r = 0;
		for (int p = 0; p < patternLen && r < patternLen; p++)
		{
            char c = patternChars[p];
		    if (c == escChar)
			{
				p++;		// don't copy the escape char

				// run out?
				if (p >= patternLen)
					throw StandardException.newException(
							SQLState.LANG_INVALID_ESCAPE_SEQUENCE);
				result[r++] = patternChars[p];
				continue;
			}

			// stop on first pattern matching char
			if (c == anyChar || c == anyString)
			{
				return new String(result, 0, r);
			}

			result[r++] = patternChars[p];
		}

        // no pattern chars
		return new String(result, 0, r);
	}

	/**
	 * stripEscapesNoPatternChars
	 *
	 * @param pattern	pattern String to search
	 * @param escChar	the escape character
	 *
	 * @return a stripped of ESC char string if no pattern chars, null otherwise
	 * @exception StandardException thrown if data invalid
	 */

	public static String
        stripEscapesNoPatternChars(String pattern, char escChar)
		throws StandardException
	{
		int patternLen = pattern.length();
		char[] patternChars = new char[patternLen];
		char[] result = new char[patternLen];
		pattern.getChars(0, patternLen, patternChars, 0);

		int r = 0;
		for (int p = 0; p < patternLen && r < patternLen; p++)
		{
			char c = pattern.charAt(p);
		    if (c == escChar)
			{
				p++;		// don't copy the escape char

				// run out?
				if (p >= patternLen)
					throw StandardException.newException(
							SQLState.LANG_INVALID_ESCAPE_SEQUENCE);
				result[r++] = patternChars[p];
				continue;
			}

			// die on first pattern matching char
			if (c == anyChar || c == anyString)
			{
				return null;
			}

			result[r++] = patternChars[p];
		}
		return new String(result, 0, r);
	}

	/**
	 * Return the substring from the pattern for the < clause.
	 * (NOTE: This may be null in the degenerate case where the
	 * last char before the first wild card can't be incremented.)
	 *
	 *		Optimized for ESCAPE
	 *
	 * This function uses the greaterThanString, and bumps its last
	 * character by one.   This string has no escape characters, they
	 * were stripped out, and ends just before any real pattern chars or
	 * at the end of the pattern literal.    See LikeEscapeOp*Node.preprocess.
	 *
	 * @param pattern	The right side of the LIKE
	 *
	 * @return	The String for the < clause
	 * @exception StandardException thrown if data invalid
	 */
	public static String lessThanString(String pattern)
		throws StandardException
	{
		//int		firstAnyChar = pattern.indexOf(anyChar);
		//int		firstAnyString = pattern.indexOf(anyString);
		int		lastUsableChar;
		int		patternLen;
		char	oldLastChar;
		char	newLastChar;
		char[]	charArray;

		if ((patternLen = pattern.length()) == 0)
		{
			// pattern is ""
			return null;
		}
		
		lastUsableChar = patternLen -1;
		oldLastChar = pattern.charAt(lastUsableChar);
		newLastChar = oldLastChar;
		newLastChar++;

		// Check for degenerate roll over
		if (newLastChar < oldLastChar)
		{
			return null;
		}

		charArray = pattern.substring(0, lastUsableChar + 1).toCharArray();
		charArray[lastUsableChar] = newLastChar;

		return new String(charArray);
	}

	public static String lessThanStringFromParameter(String pattern) throws StandardException {
		if (pattern == null)
			return null;
		return lessThanString(pattern, null);
	}

	public static String lessThanStringFromParameterWithEsc(String pattern, String escape)
		 throws StandardException
	{
		if (pattern == null)
			return null;
		return lessThanString(pattern, escape);
	}

	/**
	 * Return the substring from the pattern for the < clause.
	 * (NOTE: This may be null in the degenerate case where the
	 * last char before the first wild card can't be incremented.)
	 *
	 * 		This is unoptimized for ESCAPE.
	 *
	 *
	 * @param pattern	The right side of the LIKE
	 * @param escape	The escape clause
	 *
	 * @return	The String for the < clause
	 * @exception StandardException thrown if data invalid
	 */
	public static String lessThanString(String pattern, String escape)
		throws StandardException
	{
		int		lastUsableChar;
		int		patternLen;
		char	oldLastChar;
		char	newLastChar;
		char[]	charArray;
		char escChar = 'a';

		if ((patternLen = pattern.length()) == 0)
		{
			// pattern is ""
			return null;
		}

		if ((escape != null) && (escape.length() !=0))
		{
			escChar = escape.charAt(0);
		}

		/* Find the last non-wildcard character in the pattern
		 * and increment it.  In the most common case,
		 * "asdf%" becomes "asdg".  However, we need to 
		 * handle the following:
		 *
		 *	pattern			return
		 *	-------			------
		 *	""				SUPER_STRING (match against super string)
		 *	"%..."			SUPER_STRING (match against super string)
		 *	"_..."			SUPER_STRING (match against super string)
		 *	"asdf%"			"asdg"
		 */

		int		firstAnyChar = pattern.indexOf(anyChar);
		int		firstAnyString = pattern.indexOf(anyString);


		if (escape != null)
		{
			while (firstAnyChar > 0 && pattern.charAt(firstAnyChar-1) == escChar)
				firstAnyChar = pattern.indexOf(anyChar, firstAnyChar+1);
			while (firstAnyString > 0 && pattern.charAt(firstAnyString-1) == escChar)
				firstAnyString = pattern.indexOf(anyString, firstAnyString+1);
		}

		if (firstAnyChar == -1)
		{
			if (firstAnyString == -1)
			{
				lastUsableChar = pattern.length() -1;
			}
			else if (firstAnyString == 0)
			{
				// pattern is "%"
				return SUPER_STRING;
			}
			else
			{
				lastUsableChar = firstAnyString - 1;
			}
		}
		else if (firstAnyString == -1)
		{
			if (firstAnyChar == 0)
			{
				// pattern is "_"
				return SUPER_STRING;
			}
			lastUsableChar = firstAnyChar - 1;
		}
		else
		{
			// both _ and % present
			lastUsableChar = ((firstAnyChar > firstAnyString) ? 
										firstAnyString :
										firstAnyChar) - 1;

			if (lastUsableChar == -1)
			{
				// pattern starts with a wildcard
				return SUPER_STRING;
			}
		}

		oldLastChar = pattern.charAt(lastUsableChar);
		newLastChar = oldLastChar;
		newLastChar++;

		// Check for degenerate roll over
		if (newLastChar < oldLastChar)
		{
			return SUPER_STRING;
		}

		if (escape == null)
		{
			charArray = pattern.substring(0, lastUsableChar + 1).toCharArray();
			charArray[lastUsableChar] = newLastChar;

			return new String(charArray);
		}
		char[] patternChars = new char[patternLen];
		char[] result = new char[patternLen];
		pattern.getChars(0, patternLen, patternChars, 0) ;

		int r = 0;
		for (int p = 0; p < lastUsableChar; p++)
		{
		    if (patternChars[p] == escChar)
			{
				p++;		// don't copy the escape char

				// run out: eg.  Escape char last char?
				if (p >= patternLen-1)
					throw StandardException.newException(
							SQLState.LANG_INVALID_ESCAPE_SEQUENCE);
				result[r++] = patternChars[p];
				continue;
			}

			result[r++] = patternChars[p];
		}
		result[r++] = newLastChar;
		String gt = new String(result, 0, r);
		return gt;
	}
	
	/**
 	 * Return whether or not the like comparison is still needed after
	 * performing the like transformation on a constant string.  The
	 * comparison is not needed if the constant string is of the form:
	 *		CONSTANT%  (constant followed by a trailing %)
	 *
	 * @param pattern	The right side of the LIKE
	 *
	 * @return Whether or not the like comparison is still needed.
	 */
	public static boolean isLikeComparisonNeeded(String pattern)
	{
		int		firstAnyChar = pattern.indexOf(anyChar);
		int		firstAnyString = pattern.indexOf(anyString);

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(pattern.length() != 0,
				"pattern expected to be non-zero length");
		}

		// if no pattern matching characters, no LIKE needed
		if (firstAnyChar == -1 && firstAnyString == -1)
			return false;

		/* Needed if string containts anyChar */
		if (firstAnyChar != -1)
		{
			return true;
		}

		/* Needed if string contains and anyString in any place
		 * other than the last character.
		 */
		if (firstAnyString != pattern.length() - 1)
		{
			return true;
		}

		return false;
	}
}
