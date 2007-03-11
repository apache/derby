/*

   Derby - Class org.apache.derby.iapi.types.CollatorSQLChar
 
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

import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;

import java.text.CollationElementIterator;
import java.text.RuleBasedCollator;

/**
 * CollatorSQLChar satisfies the DataValueDescriptor
 * interfaces (i.e., OrderableDataType). It implements an String holder,
 * e.g. for storing a column value; it can be specified
 * when constructed to not allow nulls. Nullability cannot be changed
 * after construction.
 * <p>
 * Because OrderableDataType is a subclass of DataType,
 * CollatorSQLChar can play a role in either a DataType/ValueRow
 * or a OrderableDataType/KeyRow, interchangeably.
 * 
 * This class differs from SQLChar based on how the 2 classes use different
 * collations to collate their data. SQLChar uses Derby's default collation
 * which is UCS_BASIC. Whereas, this class uses the RuleBasedCollator object 
 * that was passed to it in it's constructor and that RuleBasedCollator object  
 * decides the collation.
 * 
 * In Derby 10.3, this class will be passed a RuleBasedCollator which is based 
 * on the database's territory. In future releases of Derby, this class can be 
 * used to do other kinds of collation like case-insensitive collation etc by  
 * just passing an appropriate RuleBasedCollator object for that kind of 
 * collation.
 */
public class CollatorSQLChar
	extends SQLChar
{
	//Use this object for collation
	RuleBasedCollator rbc;
	//Following is the array holding a series of collation elements for the 
	//string. It will be used in the like method 
	private int[]	collationElementsForString;
	//number of valid collation elements in the array above. 
	private int		countOfCollationElements;

	/*
	 * constructors
	 */

	/**
		no-arg constructor, required by Formattable.
	*/
	public CollatorSQLChar()
	{
	}

	public CollatorSQLChar(String val, RuleBasedCollator rbc)
	{
		super(val);
		this.rbc = rbc;
	}

	/**
	 * Set the RuleBasedCollator for this instance of CollatorSQLChar. It will
	 * be used to do the collation.
	 * 
	 * @return an array of collation elements for the string
	 * @throws StandardException
	 */
	private void setCollator(RuleBasedCollator rbc)
	{
		this.rbc = rbc;
	}

	/**
	 * This method translates the string into a series of collation elements.
	 * These elements will get used in the like method.
	 * 
	 * @return an array of collation elements for the string
	 * @throws StandardException
	 */
	private int[] getCollationElementsForString()
		throws StandardException
	{
		if (isNull())
		{
			return (int[]) null;
		}

		if (collationElementsForString != null)
		{
			return collationElementsForString;
		}

		// countOfCollationElements should always be 0 when collationElementsForString is null
		if (SanityManager.DEBUG)
		{
			if (countOfCollationElements != 0)
			{
				SanityManager.THROWASSERT(
					"countOfCollationElements expected to be 0, not " + countOfCollationElements);
			}
		}

		collationElementsForString = new int[getLength()];

		CollationElementIterator cei = rbc.getCollationElementIterator(getString());
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


	/**
	 * This method returns the count of collation elements for this instance of
	 * CollatorSQLChar. This method will return the correct value only if  
	 * method getCollationElementsForString has been called previously on this 
	 * instance of CollatorSQLChar. 
	 *
	 * @return count of collation elements for this instance of CollatorSQLChar
	 */
	private int getCountOfCollationElements()
	{
		return countOfCollationElements;
	}

	/**
	 * This method implements the like function for char (with no escape value).
	 *
	 * @param pattern		The pattern to use
	 *
	 * @return	A SQL boolean value telling whether the first operand is
	 *			like the second operand
	 *
	 * @exception StandardException		Thrown on error
	 */
	public BooleanDataValue like(DataValueDescriptor pattern)
								throws StandardException
	{
		Boolean likeResult;

		CollatorSQLChar patternSQLChar = (CollatorSQLChar) pattern;
		likeResult = Like.like(getCollationElementsForString(),
				getCountOfCollationElements(),
				patternSQLChar.getCollationElementsForString(),
				patternSQLChar.getCountOfCollationElements(),
				rbc);

		return SQLBoolean.truthValue(this,
									 pattern,
									 likeResult);
	}

	/*
	 * DataValueDescriptor interface
	 */

	/**
	 * @see DataValueDescriptor#getClone
	 */
	public DataValueDescriptor getClone()
	{
		try
		{
			return new CollatorSQLChar(getString(), rbc);
		}
		catch (StandardException se)
		{
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT("Unexpected exception " + se);
			return null;
		}
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 */
	public DataValueDescriptor getNewNull()
	{
		CollatorSQLChar result = new CollatorSQLChar();
		result.setCollator(rbc);
		return result;
	}

	/*
	 * Storable interface
	 */

	/**
	 * @see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
	 */ 
	public int getTypeFormatId() {
		return StoredFormatIds.SQL_CHAR_WITH_NON_DEFAULT_COLLATION_ID;
	}
}
