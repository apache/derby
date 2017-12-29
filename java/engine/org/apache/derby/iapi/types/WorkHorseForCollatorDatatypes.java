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

import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.sanity.SanityManager;

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

        if (escape.getLength() != 1 ||
                !escapeCharacter.hasSingleCollationElement())
		{
			throw StandardException.newException(SQLState.LANG_INVALID_ESCAPE_CHARACTER,
					escapeCharacter.toString());
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
     * Check if the string consists of a single collation element.
     * @return {@code true} iff it's a single collation element
     * @see CollationElementsInterface#hasSingleCollationElement()
     */
    boolean hasSingleCollationElement() throws StandardException {
        if (stringData.isNull()) {
            return false;
        }

        CollationElementIterator cei =
            collatorForCharacterDatatypes.getCollationElementIterator(
                stringData.getString());

        // First call next() to see that there is at least one element, and
        // then call next() to see that there is no more than one element.
        return cei.next() != CollationElementIterator.NULLORDER &&
                cei.next() == CollationElementIterator.NULLORDER;
    }
}
