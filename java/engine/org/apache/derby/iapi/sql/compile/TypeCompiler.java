/*

   Derby - Class org.apache.derby.iapi.sql.compile.TypeCompiler

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

package org.apache.derby.iapi.sql.compile;

import org.apache.derby.iapi.services.loader.ClassFactory;

import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.compiler.LocalField;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.reference.DB2Limit;

/**
 * This interface defines methods associated with a TypeId that are used
 * by the compiler.
 */

public interface TypeCompiler
{
	/**
	 * Various fixed numbers related to datatypes.
	 */
	// Need to leave space for '-'
	public static final int LONGINT_MAXWIDTH_AS_CHAR	= 20;

	// Need to leave space for '-'
	public static final int INT_MAXWIDTH_AS_CHAR	= 11;

	// Need to leave space for '-'
	public static final int SMALLINT_MAXWIDTH_AS_CHAR	= 6;

	// Need to leave space for '-'
	public static final int TINYINT_MAXWIDTH_AS_CHAR	= 4;

	// Need to leave space for '-' and decimal point
	public static final int DOUBLE_MAXWIDTH_AS_CHAR		= 54;

	// Need to leave space for '-' and decimal point
	public static final int REAL_MAXWIDTH_AS_CHAR	= 25;

	public static final int DEFAULT_DECIMAL_PRECISION	= DB2Limit.DEFAULT_DECIMAL_PRECISION;
	public static final int DEFAULT_DECIMAL_SCALE 		= DB2Limit.DEFAULT_DECIMAL_SCALE;
	public static final int MAX_DECIMAL_PRECISION_SCALE = DB2Limit.MAX_DECIMAL_PRECISION_SCALE;

	public static final int BOOLEAN_MAXWIDTH_AS_CHAR	= 5;

	public static final String PLUS_OP 		= "+";
	public static final String DIVIDE_OP	= "/";
	public static final String MINUS_OP 	= "-";
	public static final String TIMES_OP 	= "*";
	public static final String SUM_OP 		= "sum";
	public static final String AVG_OP 		= "avg";
	public static final String MOD_OP		= "mod";

	/**
	 * Type resolution methods on binary operators
	 *
	 * @param descrFactory	A factory to generate the return value
	 * @param leftType	The type of the left parameter
	 * @param rightType	The type of the right parameter
	 * @param operator	The name of the operator (e.g. "+").
	 *
	 * @return	The type of the result
	 *
	 * @exception StandardException	Thrown on error
	 */

	DataTypeDescriptor	resolveArithmeticOperation(
							DataTypeDescriptor leftType,
							DataTypeDescriptor rightType,
							String operator
								)
							throws StandardException;

	/**
	 * Determine if this type can be compared to some other type
	 *
	 * @param otherType	The CompilationType of the other type to compare
	 *					this type to
	 * @param forEquals True if this is an = or <> comparison, false otherwise.
	 * @param cf		A ClassFactory
	 *
	 * @return	true if the types can be compared, false if comparisons between
	 *			the types are not allowed
	 */

	boolean				comparable(TypeId otherType,
                                   boolean forEquals,
                                   ClassFactory cf);



	/**
	 * Determine if this type can be CONVERTed to some other type
	 *
	 * @param otherType	The CompilationType of the other type to compare
	 *					this type to
	 *
	 * @param forDataTypeFunction  true if this is a type function that
	 *   requires more liberal behavior (e.g DOUBLE can convert a char but 
	 *   you cannot cast a CHAR to double.
	 *   
	 * @return	true if the types can be converted, false if conversion
	 *			is not allowed
	 */
	 boolean             convertible(TypeId otherType, 
									 boolean forDataTypeFunction);

	/**
	 * Determine if this type is compatible to some other type
	 * (e.g. COALESCE(thistype, othertype)).
	 *
	 * @param otherType	The CompilationType of the other type to compare
	 *					this type to
	 *
	 * @return	true if the types are compatible, false if not compatible
	 */
	boolean compatible(TypeId otherType);

	/**
	 * Determine if this type can have a value of another type stored into it.
	 * Note that direction is relevant here: the test is that the otherType
	 * is storable into this type.
	 *
	 * @param otherType	The TypeId of the other type to compare this type to
	 * @param cf		A ClassFactory
	 *
	 * @return	true if the other type can be stored in a column of this type.
	 */

	boolean				storable(TypeId otherType, ClassFactory cf);

	/**
	 * Get the name of the interface for this type.  For example, the interface
	 * for a SQLInteger is NumberDataValue.  The full path name of the type
	 * is returned.
	 *
	 * @return	The name of the interface for this type.
	 */
	String interfaceName();

	/**
	 * Get the name of the corresponding Java type.  For numerics and booleans
	 * we will get the corresponding Java primitive type.
	 e
	 * Each SQL type has a corresponding Java type.  When a SQL value is
	 * passed to a Java method, it is translated to its corresponding Java
	 * type.  For example, a SQL Integer will be mapped to a Java int, but
	 * a SQL date will be mapped to a java.sql.Date.
	 *
	 * @return	The name of the corresponding Java primitive type.
	 */
	String	getCorrespondingPrimitiveTypeName();

	/**
	 * Get the method name for getting out the corresponding primitive
	 * Java type from a DataValueDescriptor.
	 *
	 * @return String		The method call name for getting the
	 *						corresponding primitive Java type.
	 */
	String getPrimitiveMethodName();


	/**
	 * Get the name of the matching national char type.
	 *
	 * @return The name of the matching national char type.
	 */
	String getMatchingNationalCharTypeName();

	/**
	 * Generate the code necessary to produce a SQL null of the appropriate
	 * type. The stack must contain a DataValueFactory and a null or a value
	   of the correct type (interfaceName()).
	 *
	 * @param mb	The method to put the expression in
	 *
	 */

	void			generateNull(MethodBuilder mb);


	/**
	 * Generate the code necessary to produce a SQL value based on
	 * a value.  The value's type is assumed to match
	 * the type of this TypeId.  For example, a TypeId
	 * for the SQL int type should be given an value that evaluates
	 * to a Java int or Integer.
	 *
	 * If the type of the value is incorrect, the generated code will
	 * not work.

       The stack must contain
			data value factory
			value
	 *
	 */
	void			generateDataValue(MethodBuilder eb, LocalField field);

	/**
	 * Return the maximum width for this data type when cast to a char type.
	 *
	 * @param dts		The associated DataTypeDescriptor for this TypeId.
	 *
	 * @return int			The maximum width for this data type when cast to a char type.
	 */
	int getCastToCharWidth(DataTypeDescriptor dts);

}
