/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.catalog
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.catalog;

/**
 *
 * An interface for describing an alias in Cloudscape systems.
 * 
 * In a Cloudscape system, an alias can be one of the following:
 * <ul>
 * <li>method alias
 * <li>class alias
 * <li>user-defined aggregate
 * </ul>
 *
 */
public interface AliasInfo
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
	/**
	 * Public statics for the various alias types as both char and String.
	 */
	public static final char ALIAS_TYPE_PROCEDURE_AS_CHAR		= 'P';
	public static final char ALIAS_TYPE_FUNCTION_AS_CHAR		= 'F';

	public static final String ALIAS_TYPE_PROCEDURE_AS_STRING		= "P";
	public static final String ALIAS_TYPE_FUNCTION_AS_STRING		= "F";

	/**
	 * Public statics for the various alias name spaces as both char and String.
	 */
	public static final char ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR	= 'P';
	public static final char ALIAS_NAME_SPACE_FUNCTION_AS_CHAR	= 'F';

	public static final String ALIAS_NAME_SPACE_PROCEDURE_AS_STRING	= "P";
	public static final String ALIAS_NAME_SPACE_FUNCTION_AS_STRING	= "F";

	/**
	 * Get the name of the static method that the alias 
	 * represents at the source database.  (Only meaningful for
	 * method aliases )
	 *
	 * @return The name of the static method that the alias 
	 * represents at the source database.
	 */
	public String getMethodName();
}
