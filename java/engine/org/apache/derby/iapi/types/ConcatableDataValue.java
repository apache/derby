/*

   Derby - Class org.apache.derby.iapi.types.ConcatableDataValue

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

import org.apache.derby.iapi.error.StandardException;

/**
 * The ConcatableDataValue interface corresponds to the
 * SQL 92 string value data type.  It is implemented by
 * datatypes that have a length, and can be concatenated.
 * It is implemented by the character datatypes and the
 * bit datatypes.  
 *
 * The following methods are defined herein:
 *		charLength()
 *
 * The following is defined by the sub classes (bit and char)
 *		concatenate()
 * 
 * @author	jamie
 */
public interface ConcatableDataValue extends DataValueDescriptor, VariableSizeDataValue
{

	/**
	 * The SQL char_length() function.
	 *
	 * @param result	The result of a previous call to this method,
	 *					null if not called yet.
	 *
	 * @return	A NumberDataValue containing the result of the char_length
	 *
	 * @exception StandardException		Thrown on error
	 */
	public NumberDataValue charLength(NumberDataValue result)
							throws StandardException;

	/**
	 * substr() function matchs DB2 syntax and behaviour.
	 *
	 * @param start		Start of substr
	 * @param length	Length of substr
	 * @param result	The result of a previous call to this method,
	 *					null if not called yet.
	 * @param maxLen	Maximum length of the result string
	 *
	 * @return	A ConcatableDataValue containing the result of the substr()
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ConcatableDataValue substring(
				NumberDataValue start,
				NumberDataValue length,
				ConcatableDataValue result,
				int maxLen)
		throws StandardException;
}
