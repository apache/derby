/*

   Derby - Class org.apache.derby.iapi.types.CollatorSQLVarchar
 
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

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;

import java.text.RuleBasedCollator;

/**
 * CollatorSQLVarchar class differs from SQLVarchar based on how the 2 classes  
 * use different collations to collate their data. SQLVarchar uses Derby's 
 * default collation which is UCS_BASIC. Whereas, this class uses the 
 * RuleBasedCollator object that was passed to it in it's constructor and that 
 * RuleBasedCollator object decides the collation.
 * 
 * In Derby 10.3, this class will be passed a RuleBasedCollator which is based 
 * on the database's territory. In future releases of Derby, this class can be 
 * used to do other kinds of collations like case-insensitive collation etc by  
 * just passing an appropriate RuleBasedCollator object for that kind of 
 * collation.
 */
public class CollatorSQLVarchar extends SQLVarchar implements CollationElementsInterface
{
	WorkHorseForCollatorDatatypes holderForCollationSensitiveInfo;

	/*
	 * constructors
	 */

	/**
		no-arg constructor, required by Formattable.
	*/
	public CollatorSQLVarchar()
	{
	}

	public CollatorSQLVarchar(String val, RuleBasedCollator collatorForCharacterDatatypes)
	{
		super(val);
		holderForCollationSensitiveInfo = 
			new WorkHorseForCollatorDatatypes(collatorForCharacterDatatypes, this);
	}

	/**
	 * Set the RuleBasedCollator for this instance of CollatorSQLVarchar. It will
	 * be used to do the collation.
	 */
	private void setCollator(RuleBasedCollator collatorForCharacterDatatypes)
	{
		holderForCollationSensitiveInfo = 
			new WorkHorseForCollatorDatatypes(collatorForCharacterDatatypes, this);
	}
	
	/** @see CollationElementsInterface#getCollationElementsForString */
	public int[] getCollationElementsForString() throws StandardException 
	{
		return holderForCollationSensitiveInfo.getCollationElementsForString();
	}

	/** @see CollationElementsInterface#getCountOfCollationElements */
	public int getCountOfCollationElements()
	{
		return holderForCollationSensitiveInfo.getCountOfCollationElements();
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
			return new CollatorSQLVarchar(getString(), 
					holderForCollationSensitiveInfo.getCollatorForCollation());
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
		CollatorSQLVarchar result = new CollatorSQLVarchar();
		result.setCollator(
				holderForCollationSensitiveInfo.getCollatorForCollation());
		return result;
	}

	/**
	 * This method implements the like function for char (with no escape value).
	 * The difference in this method and the same method in superclass is that
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
	public BooleanDataValue like(DataValueDescriptor pattern)
								throws StandardException
	{
		return(holderForCollationSensitiveInfo.like(pattern));
	}
}
