/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.catalog
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.catalog;

/**
 *
 * An interface for describing types in Cloudscape systems.
 *	
 *	
 *	<p>The values in system catalog DATATYPE columns are of type
 *	TypeDescriptor.
 */

public interface TypeDescriptor
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
	///////////////////////////////////////////////////////////////////////
	//
	//	CONSTANTS
	//
	///////////////////////////////////////////////////////////////////////


	/**
	  The return value from getMaximumWidth() for types where the maximum
	  width is unknown.
	 */

	public	static	int MAXIMUM_WIDTH_UNKNOWN = -1;


	///////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	///////////////////////////////////////////////////////////////////////

	/**
	 * Get the jdbc type id for this type.  JDBC type can be
	 * found in java.sql.Types. 
	 *
	 * @return	a jdbc type, e.g. java.sql.Types.DECIMAL 
	 *
	 * @see Types
	 */
	public int getJDBCTypeId();

	/**
	  Returns the maximum width of the type.  This may have
	  different meanings for different types.  For example, with char,
	  it means the maximum number of characters, while with int, it
	  is the number of bytes (i.e. 4).

	  @return	the maximum length of this Type; -1 means "unknown/no max length"
	  */
	public	int			getMaximumWidth();


	/**
	  Returns the number of decimal digits for the type, if applicable.
	 
	  @return	The number of decimal digits for the type.  Returns
	 		zero for non-numeric types.
	  */
	public	int			getPrecision();


	/**
	  Returns the number of digits to the right of the decimal for
	  the type, if applicable.
	 
	  @return	The number of digits to the right of the decimal for
	 		the type.  Returns zero for non-numeric types.
	  */
	public	int			getScale();


	/**
	  Gets the nullability that values of this type have.
	  

	  @return	true if values of this type may be null. false otherwise
	  */
	public	boolean		isNullable();

	/**
	  Gets the name of this type.
	  

	  @return	the name of this type
	  */
	public	String		getTypeName();


	/**
	  Converts this type descriptor (including length/precision)
	  to a string suitable for appearing in a SQL type specifier.  E.g.
	 
	 			VARCHAR(30)

	  or

	             java.util.Hashtable 
	 
	 
	  @return	String version of type, suitable for running through
	 			a SQL Parser.
	 
	 */
	public 	String		getSQLstring();

}

