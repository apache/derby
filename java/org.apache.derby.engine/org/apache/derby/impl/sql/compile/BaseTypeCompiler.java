/*

   Derby - Class org.apache.derby.impl.sql.compile.BaseTypeCompiler

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

package org.apache.derby.impl.sql.compile;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.LocalField;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.TypeCompiler;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;

/**
 * This is the base implementation of TypeCompiler
 *
 */

abstract class BaseTypeCompiler implements TypeCompiler
{
	private TypeId correspondingTypeId;

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

    /**
     * The caller will have pushed a DataValueFactory and a null or a value
     * of the correct type (interfaceName()). Thus upon entry the
     * stack looks like on of:
     * ...,dvf,ref
     * ...,dvf,null
     * 
     * This method then sets up to call the required method
     * on DataValueFactory using the nullMethodName().
     * The value left on the stack will be a DataValueDescriptor
     * of the correct type:
     * 
     * ...,dvd
     * 
     * @see TypeCompiler#generateNull(MethodBuilder, int)
     */
	public void generateNull(MethodBuilder mb, int collationType)
	{
        int argCount;
        if (pushCollationForDataValue(collationType))
        {
            mb.push(collationType);
            argCount = 2;
        }
        else
            argCount = 1;
        
		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null,
									nullMethodName(),
									interfaceName(),
                                    argCount);
	}

    
    /**
     * The caller will have pushed a DataValueFactory and  value
     * of that can be converted to the correct type, e.g. int
     * for a SQL INTEGER.
     *  
     * Thus upon entry the
     * stack looks like:
     * ...,dvf,value
     * 
     * If field is not null then it is used as the holder
     * of the generated DataValueDescriptor to avoid object
     * creations on multiple passes through this code.
     * The field may contain null or a valid value.
     * 
     * This method then sets up to call the required method
     * on DataValueFactory using the dataValueMethodName().
     * The value left on the stack will be a DataValueDescriptor
     * of the correct type:
     * 
     * If the field contained a valid value then generated
     * code will return that value rather than a newly created
     * object. If field was not-null then the generated code
     * will set the value of field to be the return from
     * the DataValueFactory method call. Thus if the field
     * was empty (set to null) when this code is executed it
     * will contain the newly generated value, otherwise it
     * will be reset to the same value.
     * 
     * ...,dvd
     * 
     * @see TypeCompiler#generateDataValue(MethodBuilder, int, LocalField)
     */
    public void generateDataValue(MethodBuilder mb, int collationType,
			LocalField field)
	{

            
		String				interfaceName = interfaceName();

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
        
        int argCount;
        if (pushCollationForDataValue(collationType))
        {
            mb.push(collationType);
            argCount = 3;
        }
        else
            argCount = 2;

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null,
							dataValueMethodName(),
							interfaceName,
                            argCount);

		if (field != null)
		{
			/* Store the result of the method call in the field,
			 * so we can re-use the wrapper.
			 */
			mb.putField(field);
		}
	}

    /**
        Return the method name to get a Derby DataValueDescriptor
        object of the correct type set to SQL NULL. The method named will
        be called with one argument: a holder object if pushCollationForDataValue()
        returns false, otherwise two arguments, the second being the
        collationType.
    */
    abstract String nullMethodName();

	/**
		Return the method name to get a Derby DataValueDescriptor
		object of the correct type and set it to a specific value.
        The method named will be called with two arguments, a value to set the
        returned value to and a holder object if pushCollationForDataValue()
        returns false. Otherwise three arguments, the third being the
        collationType.
        This implementation returns "getDataValue" to map
        to the overloaded methods
        DataValueFactory.getDataValue(type, dvd type)
	*/
	String dataValueMethodName()
	{
		return "getDataValue";
	}
    
    /**
     * Return true if the collationType is to be passed
     * to the methods generated by generateNull and
     * generateDataValue.
     * 
     * @param collationType Collation type of character values.
     * @return true collationType will be pushed, false collationType will be ignored.
     */
    boolean pushCollationForDataValue(int collationType)
    {
        return false;
    }

	
	/**
	 * Determine whether thisType is storable in otherType due to otherType
	 * being a user type.
	 *
	 * @param thisType	The TypeId of the value to be stored
	 * @param otherType	The TypeId of the value to be stored in
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
//IC see: https://issues.apache.org/jira/browse/DERBY-776
		if (otherType.userType())
		{
			return cf.getClassInspector().assignableTo(
					thisType.getCorrespondingJavaTypeName(),
					otherType.getCorrespondingJavaTypeName());
		}

		return false;
	}
	
	/**
	 * Tell whether this numeric type can be converted to the given type.
	 *
	 * @param otherType	The TypeId of the other type.
	 * @param forDataTypeFunction  was this called from a scalarFunction like
	 *                             CHAR() or DOUBLE()
	 */
    boolean numberConvertible(TypeId otherType,
									 boolean forDataTypeFunction)
	{
        if ( otherType.getBaseTypeId().isAnsiUDT() ) { return false; }
//IC see: https://issues.apache.org/jira/browse/DERBY-4469

		// Can't convert numbers to long types
		if (otherType.isLongConcatableTypeId())
			return false;

		// Numbers can only be converted to other numbers, 
		// and CHAR, (not VARCHARS or LONGVARCHAR). 
		// Only with the CHAR() or VARCHAR()function can they be converted.
		boolean retval =((otherType.isNumericTypeId()) ||
						 (otherType.userType()));

		// For CHAR  Conversions, function can convert 
		// Floating types
		if (forDataTypeFunction)
			retval = retval || 
				(otherType.isFixedStringTypeId() &&
				(getTypeId().isFloatingPointTypeId()));
	   
		retval = retval ||
			(otherType.isFixedStringTypeId() && 					  
			 (!getTypeId().isFloatingPointTypeId()));
		
		return retval;

	}

	/**
	 * Tell whether this numeric type can be stored into from the given type.
	 *
	 * @param thisType	The TypeId of this type
	 * @param otherType	The TypeId of the other type.
	 * @param cf		A ClassFactory
	 */

    boolean numberStorable(TypeId thisType,
									TypeId otherType,
									ClassFactory cf)
	{
        if ( otherType.getBaseTypeId().isAnsiUDT() ) { return false; }
//IC see: https://issues.apache.org/jira/browse/DERBY-4469

		/*
		** Numbers can be stored into from other number types.
		** Also, user types with compatible classes can be stored into numbers.
		*/
		if (otherType.isNumericTypeId()) { return true; }

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
		return getTypeId().getTypeFormatId();
	}
}





