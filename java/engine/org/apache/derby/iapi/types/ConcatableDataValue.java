/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.types
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;

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
