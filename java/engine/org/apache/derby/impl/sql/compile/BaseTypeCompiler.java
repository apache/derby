/*

   Derby - Class org.apache.derby.impl.sql.compile.BaseTypeCompiler

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

package org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.loader.ClassFactory;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;

import org.apache.derby.iapi.sql.compile.TypeCompiler;

import org.apache.derby.iapi.types.BitDataValue;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.ClassName;

import org.apache.derby.iapi.services.compiler.LocalField;
import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.services.classfile.VMOpcode;

import java.sql.Types;

/**
 * This is the base implementation of TypeCompiler
 *
 * @author Jeff
 */

public abstract class BaseTypeCompiler implements TypeCompiler
{
	TypeId correspondingTypeId;

	/**
	 * Get the method name for getting out the corresponding primitive
	 * Java type.
	 *
	 * @return String		The method call name for getting the
	 *						corresponding primitive Java type.
	 */
	public String getPrimitiveMethodName()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("getPrimitiveMethodName not applicable for " +
									  getClass().toString());
		}
		return null;
	}

	/** @see TypeCompiler#getMatchingNationalCharTypeName */
	public String getMatchingNationalCharTypeName()
	{
		return TypeId.NATIONAL_CHAR_NAME;
	}


	/**
	 * @see TypeCompiler#resolveArithmeticOperation
	 *
	 * @exception StandardException		Thrown on error
	 */
	public DataTypeDescriptor
	resolveArithmeticOperation(DataTypeDescriptor leftType,
								DataTypeDescriptor rightType,
								String operator)
							throws StandardException
	{
		throw StandardException.newException(SQLState.LANG_BINARY_OPERATOR_NOT_SUPPORTED, 
										operator,
										leftType.getTypeId().getSQLTypeName(),
										rightType.getTypeId().getSQLTypeName()
										);
	}

	/** @see TypeCompiler#generateNull */

	public void generateNull(MethodBuilder mb)
	{
		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null,
									nullMethodName(),
									interfaceName(),
									1);
	}

	/** @see TypeCompiler#generateDataValue */
	public final void generateDataValue(MethodBuilder mb,
										LocalField field)
	{
		String				interfaceName = interfaceName();

		if (this instanceof UserDefinedTypeCompiler)
		{
			// cast the value to an object for method resolution
			mb.upCast("java.lang.Object");
		}

		// push the second argument

		/* If fieldName is null, then there is no
		 * reusable wrapper (null), else we
		 * reuse the field.
		 */
		if (field == null)
		{
			mb.pushNull(interfaceName);
		}
		else
		{
			mb.getField(field);
		}


		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null,
							dataValueMethodName(),
							interfaceName,
							2);

		if (field != null)
		{
			/* Store the result of the method call in the field,
			 * so we can re-use the wrapper.
			 */
			mb.putField(field);
		}
	}

	protected abstract String nullMethodName();

	protected String dataValueMethodName()
	{
		return "getDataValue";
	}

	
	/**
	 * Determine whether thisType is storable in otherType due to otherType
	 * being a user type.
	 *
	 * @param thisType	The TypeId of the value to be stored
	 * @param otherType	The TypeId of the value to be stored in
	 * @param cm		Current ContextManager
	 *
	 * @return	true if thisType is storable in otherType
	 */
	protected boolean userTypeStorable(TypeId thisType,
							TypeId otherType,
							ClassFactory cf)
	{
		/*
		** If the other type is user-defined, use the java types to determine
		** assignability.
		*/
		if ( ! otherType.builtIn())
		{
			return cf.getClassInspector().assignableTo(
					thisType.getCorrespondingJavaTypeName(),
					otherType.getCorrespondingJavaTypeName());
		}

		return false;
	}

	/**
	 * Tell whether this numeric type can be compared to the given type.
	 *
	 * @param otherType	The TypeId of the other type.
	 */

	public boolean numberComparable(TypeId otherType,
									boolean forEquals,
									ClassFactory cf)
	{
		TypeCompiler otherTC = getTypeCompiler(otherType);

		/* Numbers can be compared to other numbers, 
		 * boolean and objects
		 */
		return otherType.isNumericTypeId() ||
				otherType.isBooleanTypeId() ||
				(otherType.userType() && otherTC.comparable(otherType,
															forEquals,
															cf));
	}

	
	/**
	 * Tell whether this numeric type can be converted to the given type.
	 *
	 * @param otherType	The TypeId of the other type.
	 * @param forDataTypeFunction  was this called from a scalarFunction like
	 *                             CHAR() or DOUBLE()
	 */
	public boolean numberConvertible(TypeId otherType, 
									 boolean forDataTypeFunction)
	{

		// Can't convert numbers to long types
		if (otherType.isLongConcatableTypeId())
			return false;

		// Numbers can only be converted to other numbers, 
		// and CHAR, (not VARCHARS or LONGVARCHAR). 
		// Only with the CHAR() or VARCHAR()function can they be converted.
		boolean retval =((otherType.isNumericTypeId()) ||
						 (otherType.isBooleanTypeId()) ||
						 (otherType.userType()));

		// For CHAR  Conversions, function can convert 
		// Floating types
		if (forDataTypeFunction)
			retval = retval || 
				(otherType.isFixedStringTypeId() &&
				(correspondingTypeId.isFloatingPointTypeId()));
	   
		retval = retval ||
			(otherType.isFixedStringTypeId() && 					  
			 (!correspondingTypeId.isFloatingPointTypeId()));
		
		return retval;

	}

	/**
	 * Tell whether this numeric type can be stored into from the given type.
	 *
	 * @param thisType	The TypeId of this type
	 * @param otherType	The TypeId of the other type.
	 * @param cf		A ClassFactory
	 */

	public boolean numberStorable(TypeId thisType,
									TypeId otherType,
									ClassFactory cf)
	{
		/*
		** Numbers can be stored into from other number types.
		** Also, user types with compatible classes can be stored into numbers.
		*/
		if ((otherType.isNumericTypeId())	||
			(otherType.isBooleanTypeId()))
			return true;

		/*
		** If the other type is user-defined, use the java types to determine
		** assignability.
		*/
		return userTypeStorable(thisType, otherType, cf);
	}


	/**
	 * Get the TypeId that corresponds to this TypeCompiler.
	 */
	protected TypeId getTypeId()
	{
		return correspondingTypeId;
	}

	/**
	 * Get the TypeCompiler that corresponds to the given TypeId.
	 */
	protected TypeCompiler getTypeCompiler(TypeId typeId)
	{
		return TypeCompilerFactoryImpl.staticGetTypeCompiler(typeId);
	}

	/**
	 * Set the TypeCompiler that corresponds to the given TypeId.
	 */
	void setTypeId(TypeId typeId)
	{
		correspondingTypeId = typeId;
	}

	/**
	 * Get the StoredFormatId from the corresponding
	 * TypeId.
	 *
	 * @return The StoredFormatId from the corresponding
	 * TypeId.
	 */
	protected int getStoredFormatIdFromTypeId()
	{
		return correspondingTypeId.getTypeFormatId();
	}


}





