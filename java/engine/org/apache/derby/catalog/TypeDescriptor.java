/*

   Derby - Class org.apache.derby.catalog.TypeDescriptor

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.catalog;

/**
 *
 * An interface for describing types in Derby systems.
 *	
 *	
 *	<p>The values in system catalog DATATYPE columns are of type
 *	TypeDescriptor.
 */

public interface TypeDescriptor
{
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
	 * @see java.sql.Types
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
	  Returns the maximum width of the type IN BYTES.  This is the
	  maximum number of bytes that could be returned for this type
	  if the corresponding getXXX() method is used.  For example,
	  if we have a CHAR type, then we want the number of bytes
	  that would be returned by a ResultSet.getString() call.

	  @return	the maximum length of this Type IN BYTES;
				-1 means "unknown/no max length"
	  */
	public	int			getMaximumWidthInBytes();


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

	/**
	 * Get the collation type for this type. This api applies only to character
	 * string types. And its return value is valid only if the collation 
	 * derivation  of this type is "implicit" or "explicit". (In Derby 10.3,
	 * collation derivation can't be "explicit". Hence in Derby 10.3, this api
	 * should be used only if the collation derivation is "implicit". 
	 *
	 * @return	collation type which applies to character string types with
	 * collation derivation of "implicit" or "explicit". The possible return
	 * values in Derby 10.3 will be COLLATION_TYPE_UCS_BASIC
     * and COLLATION_TYPE_TERRITORY_BASED.
     * 
     * @see StringDataValue#COLLATION_TYPE_UCS_BASIC
     * @see StringDataValue#COLLATION_TYPE_TERRITORY_BASED
	 * 
	 */
	public int getCollationType();

	/**
	 * Set the collation type of this TypeDescriptor
	 * @param collationTypeValue This will be COLLATION_TYPE_UCS_BASIC
     * or COLLATION_TYPE_TERRITORY_BASED
     * 
     * @see StringDataValue#COLLATION_TYPE_UCS_BASIC
     * @see StringDataValue#COLLATION_TYPE_TERRITORY_BASED
	 */
	public void setCollationType(int collationTypeValue);

	/**
	 * Get the collation derivation for this type. This applies only for
	 * character string types. For the other types, this api should be
	 * ignored.
	 * 
	 * SQL spec talks about character string types having collation type and 
	 * collation derivation associated with them (SQL spec Section 4.2.2 
	 * Comparison of character strings). If collation derivation says explicit 
	 * or implicit, then it means that there is a valid collation type 
	 * associated with the charcter string type. If the collation derivation is 
	 * none, then it means that collation type can't be established for the 
	 * character string type.
	 * 
	 * 1)Collation derivation will be explicit if SQL COLLATE clause has been  
	 * used for character string type (this is not a possibility for Derby 10.3 
	 * because we are not planning to support SQL COLLATE clause in the 10.3
	 * release). 
	 * 
	 * 2)Collation derivation will be implicit if the collation can be 
	 * determined w/o the COLLATE clause eg CREATE TABLE t1(c11 char(4)) then 
	 * c11 will have collation of USER character set. Another eg, TRIM(c11) 
	 * then the result character string of TRIM operation will have collation 
	 * of the operand, c11.
	 * 
	 * 3)Collation derivation will be none if the aggregate methods are dealing 
	 * with character strings with different collations (Section 9.3 Data types 
	 * of results of aggregations Syntax Rule 3aii).
	 *  
	 * Collation derivation will be initialized to COLLATION_DERIVATION_NONE.
	 *  
	 * @return Should be COLLATION_DERIVATION_NONE or COLLATION_DERIVATION_IMPLICIT
     * 
     * @see StringDataValue#COLLATION_DERIVATION_NONE
     * @see StringDataValue#COLLATION_DERIVATION_IMPLICIT
     * @see StringDataValue#COLLATION_DERIVATION_EXPLICIT
	 */
	public int getCollationDerivation();

	/**
	 * Set the collation derivation of this DTD
	 * @param collationDerivationValue This will be 
	 * COLLATION_DERIVATION_NONE/COLLATION_DERIVATION_IMPLICIT/COLLATION_DERIVATION_EXPLICIT
	 * In Derby 10.3, we do not expect to get value COLLATION_DERIVATION_EXPLICIT.
     * 
     * @see StringDataValue#COLLATION_DERIVATION_NONE
     * @see StringDataValue#COLLATION_DERIVATION_IMPLICIT
     * @see StringDataValue#COLLATION_DERIVATION_EXPLICIT

	 */
	public void setCollationDerivation(int collationDerivationValue);

}

