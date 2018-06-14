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

import java.sql.Types;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.StringDataValue;

/**
 * TypeDescriptor represents a type in a system catalog, a
 * persistent type. Examples are columns in tables and parameters
 * for routines. A TypeDescriptor is immutable.
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
    
    /**
     * Catalog type for nullable INTEGER
     */
    TypeDescriptor INTEGER = DataTypeDescriptor.INTEGER.getCatalogType();

    /**
     * Catalog type for not nullable INTEGER
     */
    TypeDescriptor INTEGER_NOT_NULL =
        DataTypeDescriptor.INTEGER_NOT_NULL.getCatalogType();
    
    /**
     * Catalog type for nullable SMALLINT
     */
    TypeDescriptor SMALLINT = DataTypeDescriptor.SMALLINT.getCatalogType();
    
    /**
     * Catalog type for not nullable INTEGER
     */
    TypeDescriptor SMALLINT_NOT_NULL =
        DataTypeDescriptor.SMALLINT_NOT_NULL.getCatalogType();

    /**
     * Catalog type for nullable DOUBLE
     */
    TypeDescriptor DOUBLE = DataTypeDescriptor.DOUBLE.getCatalogType();


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
	 * Return true if this is a Row Multiset type
     *
     * @return true if this is a Row Multiset type
     */
	public	boolean isRowMultiSet();
    
	/**
	 * Return true if this is a user defined type
     *
     * @return true if this is a user defined type
     */
	public	boolean isUserDefinedType();
    
    /**
     * If this catalog type is a row multi-set type
     * then return its array of catalog types.
     * 
     * @return Catalog ypes comprising the row,
     * null if this is not a row type.
     */
    public TypeDescriptor[] getRowTypes();

    /**
     * If this catalog type is a row multi-set type
     * then return its array of column names.
     * 
     * @return Column names comprising the row,
     * null if this is not a row type.
     */
    public String[] getRowColumnNames();
}

