/*

   Derby - Class org.apache.derby.iapi.types.WorkHorseForCollatorDatatypes
 
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

package org.apache.derby.iapi.types;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.BooleanDataValue;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;

import java.text.CollationElementIterator;
import java.text.CollationKey;
import java.text.RuleBasedCollator;

/**
 * WorkHorseForCollatorDatatypes class holds on to RuleBasedCollator,
 * and the base SQLChar object for the collation sensitive SQLChar,
 * SQLVarchar, SQLLongvarchar and SQLClob. This class uses RuleBasedCollator
 * and SQLChar object in the collation sensitive methods to do the comparison. 
 * The reason for encapsulating this here is that the collation version of 
 * SQLChar, SQLVarchar, SQLLongvarchar and SQLClob do not all have to duplicate  
 * the code for collation sensitive methods. Instead, they can simply delegate
 * the work to methods defined in this class. 
 */
final class WorkHorseForCollatorDatatypes  
{
	/** 
	 * Use this object for collation on character datatype. This collator
	 * object is passed as a parameter to the constructor.
	 */
	private RuleBasedCollator collatorForCharacterDatatypes;
	/**
	 * collatorForCharacterDatatypes will be used on this SQLChar to determine
	 * collationElementsForString. The collationElementsForString is used by
	 * the like method to do Collator specific comparison.
	 * This SQLChar object is passed as a parameter to the constructor.
	 */
	private SQLChar stringData;
	/**
	 * Following is the array holding a series of collation elements for the
	 * string. It will be used in the like method. This gets initialized when
	 * the like method is first invoked. 
	 */
	private int[]	collationElementsForString;
	/** 
	 * Number of valid collation elements in the array above. Note that this 
	 * might be smaller than the actual size of the array above. Gets 
	 * initialized when the like method is first invoked.
	 */
	private int		countOfCollationElements;

	// For null strings, cKey = null.
	private CollationKey cKey; 

	WorkHorseForCollatorDatatypes(
			RuleBasedCollator collatorForCharacterDatatypes,
			SQLChar stringData)
	{
		this.collatorForCharacterDatatypes = collatorForCharacterDatatypes;
		this.stringData = stringData;
	}
	
	/** @see SQLChar#stringCompare(SQLChar, SQLChar) */
	int stringCompare(SQLChar str1, SQLChar str2)
	throws StandardException
	{
		CollationKey ckey1 = str1.getCollationKey();
		CollationKey ckey2 = str2.getCollationKey();
		
		/*
		** By convention, nulls sort High, and null == null
		*/
		if (ckey1 == null || ckey2 == null)
		{
			if (ckey1 != null)	// str2 == null
				return -1;
			if (ckey2 != null)	// this == null
				return 1;
			return 0;			// both == null
		}

		return ckey1.compareTo(ckey2);
	}
	
	/**
	 * This method implements the like function for char (with no escape value).
	 * The difference in this method and the same method in SQLChar is that 
	 * here we use special Collator object to do the comparison rather than
	 * using the Collator object associated with the default jvm locale.
	 *
	 * @param pattern		The pattern to use
	 *
	 * @return	A SQL boolean value telling whether the first operand is
	 *			like the second operand
	 *
	 * @exception StandardException		Thrown on error
	 */
	BooleanDataValue like(DataValueDescriptor pattern)
								throws StandardException
	{
		Boolean likeResult;

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(
				pattern instanceof CollationElementsInterface,
				"Both the operands must be instances of CollationElementsInterface");
		likeResult = Like.like(stringData.getCharArray(), 
				stringData.getLength(), 
				((SQLChar)pattern).getCharArray(), 
				pattern.getLength(), 
				null, 
				0,
				collatorForCharacterDatatypes);

		return SQLBoolean.truthValue(stringData ,
									 pattern,
									 likeResult);
	}
	
	/**
	 * This method implements the like function for char with an escape value.
	 * 
	 * @param pattern		The pattern to use
	 * 
	 * @return	A SQL boolean value telling whether the first operand is
	 * 			like the second operand
	 *
	 * @exception StandardException		Thrown on error
	 */
	BooleanDataValue like(DataValueDescriptor pattern, 
			DataValueDescriptor escape)	throws StandardException
	{
		Boolean likeResult;

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(
							 pattern instanceof CollationElementsInterface &&
							 escape instanceof CollationElementsInterface,
			"All three operands must be instances of CollationElementsInterface");
		
		// ANSI states a null escape yields 'unknown' results 
		//
		// This method is only called when we have an escape clause, so this 
		// test is valid

		if (escape.isNull())
		{
			throw StandardException.newException(SQLState.LANG_ESCAPE_IS_NULL);
		}

		CollationElementsInterface escapeCharacter = (CollationElementsInterface) escape;

		if (escapeCharacter.getCollationElementsForString() != null && 
				(escapeCharacter.getCountOfCollationElements() != 1))
		{
			throw StandardException.newException(SQLState.LANG_INVALID_ESCAPE_CHARACTER,
					new String(escapeCharacter.toString()));
		}
		likeResult = Like.like(stringData.getCharArray(), 
				stringData.getLength(), 
				((SQLChar)pattern).getCharArray(), 
				pattern.getLength(), 
				((SQLChar)escape).getCharArray(), 
				escape.getLength(),
				collatorForCharacterDatatypes);

		return SQLBoolean.truthValue(stringData,
								 pattern,
								 likeResult);
	}

	/**
	 * Get the RuleBasedCollator which is getting used for collation sensitive
	 * methods. 
	 */
	RuleBasedCollator getCollatorForCollation()
	{
		return(collatorForCharacterDatatypes);
	}

	/**
	 * This method returns the count of collation elements for SQLChar object.
	 * It method will return the correct value only if method   
	 * getCollationElementsForString has been called previously on the SQLChar
	 * object. 
	 *
	 * @return count of collation elements for this instance of CollatorSQLChar
	 */
	int getCountOfCollationElements()
	{
		return countOfCollationElements;
	}

	/**
	 * This method translates the string into a series of collation elements.
	 * These elements will get used in the like method.
	 * 
	 * @return an array of collation elements for the string
	 * @throws StandardException
	 */
	int[] getCollationElementsForString()
		throws StandardException
	{
		if (stringData.isNull())
		{
			return (int[]) null;
		}



        // Caching of collationElementsForString is not working properly, in 
        // order to cache it needs to get invalidated everytime the container
        // type's value is changed - through any interface, eg: readExternal, 
        // setValue, ...  To get proper behavior, disabling caching, and will
        // file a performance enhancement to implement correct caching.
        collationElementsForString = null;
        countOfCollationElements   = 0;


		if (collationElementsForString != null)
		{
			return collationElementsForString;
		}

		// countOfCollationElements should always be 0 when 
        // collationElementsForString is null
		if (SanityManager.DEBUG)
		{
			if (countOfCollationElements != 0)
			{
				SanityManager.THROWASSERT(
					"countOfCollationElements expected to be 0, not " + 
                    countOfCollationElements);
			}
		}
        

		collationElementsForString = new int[stringData.getLength()];

		CollationElementIterator cei = 
            collatorForCharacterDatatypes.getCollationElementIterator(
                stringData.getString());

		int nextInt;
		while ((nextInt = cei.next()) != CollationElementIterator.NULLORDER)
		{
			/* Believe it or not, a String might have more
			 * collation elements than characters.
			 * So, we handle that case by increasing the int array
			 * by 5 and copying array elements.
			 */
			if (countOfCollationElements == collationElementsForString.length)
			{
				int[] expandedArray = new int[countOfCollationElements + 5];
				System.arraycopy(collationElementsForString, 0, expandedArray, 
						0, collationElementsForString.length);
				collationElementsForString = expandedArray;
			}
			collationElementsForString[countOfCollationElements++] = nextInt;
		}

		return collationElementsForString;
	}
}
