/*

   Derby - Class org.apache.derby.iapi.types.DateTimeParser

   Copyright 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;

/**
 * This class provides a simple regular expression parser for standard format dates, times, and timestamps
 */
class DateTimeParser
{

    private String str;
    private String trimmedString;
    private int len;
    private int fieldStart;
    private char currentSeparator;

    DateTimeParser( String str)
    {
        this.str = str;
        len = str.length();
    }

    /**
     * Parse the next integer.
     *
     * @param maxDigits the maximum number of digits
     * @param truncationAllowed If true then leading zeroes may be ommitted. If false then the integer must be
     *                          exactly ndigits long.
     * @param separator The separator at the end of the integer. If zero then the integer must be at the end of the string
     *                  but may be followed by spaces.
     * @param separatorRequired If true then the integer must be followed by the separator. If false then the integer
     *                          may terminate the string or be followed by the separator
     * @param isFraction If true then the returned integer will be multiplied by 10**(maxDigits - actualDigitCount)
     *
     * @return the integer.
     *
     * @exception StandardException invalid syntax.
     */
    int parseInt( int maxDigits, boolean truncationAllowed, char[] separator, boolean isFraction)
        throws StandardException
    {
        int number = 0;
        char c;
        int digitCount = 0;

        for( ; fieldStart < len; fieldStart++)
        {
            c = str.charAt( fieldStart);
            if( Character.isDigit( c))
            {
                if( digitCount >= maxDigits)
                    throw StandardException.newException( SQLState.LANG_DATE_SYNTAX_EXCEPTION);
                digitCount++;
                number = number*10 + Character.digit( c, 10);
            }
            else
                break;
        }
        if( truncationAllowed ? (digitCount == 0 && !isFraction) : (digitCount != maxDigits))
            throw StandardException.newException( SQLState.LANG_DATE_SYNTAX_EXCEPTION);

        updateCurrentSeparator();
        
        if( separator == null)
        {
            // separator not required
            if( fieldStart < len)
                fieldStart++;
        }
        else
        {
            int sepIdx;
            for( sepIdx = 0; sepIdx < separator.length; sepIdx++)
            {
                if( separator[sepIdx] != 0)
                {
                    if( currentSeparator == separator[sepIdx])
                    {
                        fieldStart++;
                        break;
                    }
                }
                else
                {
                    // separator[sepIdx] matches the end of the string
                    int j;
                    for( j = fieldStart; j < len; j++)
                    {
                        if( str.charAt( j) != ' ')
                            break;
                    }
                    if( j == len)
                    {
                        fieldStart = j;
                        break;
                    }
                }
            }
            if( sepIdx >= separator.length)
                throw StandardException.newException( SQLState.LANG_DATE_SYNTAX_EXCEPTION);
        }

        if( isFraction)
        {
            for(int i = digitCount; i < maxDigits; i++)
                number *= 10;
        }
        return number;
    } // end of parseInt

    /**
     * Determine if the next characters are one of a choice of strings.
     *
     * @param choices An array of strings.
     *
     * @return An index in choices.
     *
     * @exception StandardException if the next characters are not in choices.
     */
    int parseChoice( String[] choices) throws StandardException
    {
        for( int choiceIdx = 0; choiceIdx < choices.length; choiceIdx++)
        {
            String choice = choices[ choiceIdx];
            int choiceLen = choice.length();
            if( fieldStart + choiceLen <= len)
            {
                int i;
                for( i = 0; i < choiceLen; i++)
                {
                    if( choice.charAt( i) != str.charAt( fieldStart + i))
                        break;
                }
                if( i == choiceLen)
                {
                    fieldStart += choiceLen;
                    updateCurrentSeparator();
                    return choiceIdx;
                }
            }
        }
        throw StandardException.newException( SQLState.LANG_DATE_SYNTAX_EXCEPTION);
    } // end of parseChoice

    private void updateCurrentSeparator()
    {
        if( fieldStart >= len)
            currentSeparator = 0;
        else
        {
            currentSeparator = str.charAt( fieldStart);
            if( currentSeparator == ' ')
            {
                // Trailing spaces are always OK. See if we are really at the end
                for( int i = fieldStart + 1; i < len; i++)
                {
                    if( str.charAt( i) != ' ')
                        return;
                }
                currentSeparator = 0;
                fieldStart = len;
            }
        }
    } // end of updateCurrentSeparator

    /**
     * Check that we are at the end of the string: that the rest of the characters, if any, are blanks.
     *
     * @return the original string with trailing blanks trimmed off.
     * @exception StandardException if there are more non-blank characters.
     */
    String checkEnd() throws StandardException
    {
        int end = fieldStart;
        for( ; fieldStart < len; fieldStart++)
        {
            if( str.charAt( fieldStart) != ' ')
                throw StandardException.newException( SQLState.LANG_DATE_SYNTAX_EXCEPTION);
        }
        currentSeparator = 0;
        while( end > 0 && str.charAt( end - 1) == ' ')
            end--;
        trimmedString = (end == len) ? str : str.substring( 0, end);
        return trimmedString;
    } // end of checkEnd

    /**
     * Get the parsed string with trailing blanks removed. <b>This method is only valid after checkEnd
     * has been called.</b>
     *
     * @return The string with trailing blanks removed.
     */
    String getTrimmedString()
    {
        return trimmedString;
    }

    /**
     * @return the next separator, 0 if there are none
     */
    char nextSeparator()
    {
        for( int i = fieldStart + 1; i < len; i++)
        {
            char c = str.charAt( i);
            if( ! Character.isLetterOrDigit( c))
                return c;
        }
        return 0;
    }

    /**
     * @return the separator between the last parsed integer and the next integer, 0 if the parser is at
     *         the end of the string.
     */
    char getCurrentSeparator()
    {
        return currentSeparator;
    }
}
