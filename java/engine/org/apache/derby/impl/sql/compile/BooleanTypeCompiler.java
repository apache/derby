/*

   Derby - Class org.apache.derby.impl.sql.compile.BooleanTypeCompiler

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

import org.apache.derby.shared.common.reference.ClassName;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.sql.compile.TypeCompiler;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;

/**
 * This class implements TypeCompiler for the SQL BOOLEAN datatype.
 *
 */

public class BooleanTypeCompiler extends BaseTypeCompiler
{
	/**
	 * Tell whether this type (boolean) can be converted to the given type.
	 *
	 * @see TypeCompiler#convertible
	 */
	public boolean convertible(TypeId otherType, boolean forDataTypeFunction)
	{
        return (otherType.isStringTypeId() || otherType.isBooleanTypeId());
	}

        /**
         * Tell whether this type (boolean) is compatible with the given type.
         *
         * @param otherType     The TypeId of the other type.
         */
	public boolean compatible(TypeId otherType)
	{
		return convertible(otherType,false);
	}

	/** @see TypeCompiler#storable */
	public boolean storable(TypeId otherType, ClassFactory cf)
	{
		/* Are the types the same or is other type a string */
		if ( otherType.isBooleanTypeId() || otherType.isStringTypeId() )
		{
			return true;
		}

		/*
		** If the other type is user-defined, use the java types to determine
		** assignability.
		*/
		return userTypeStorable(getTypeId(), otherType, cf);
	}

	/** @see TypeCompiler#interfaceName */
	public String interfaceName()
	{
		return ClassName.BooleanDataValue;
	}

	/**
	 * @see TypeCompiler#getCorrespondingPrimitiveTypeName
	 */

	public String getCorrespondingPrimitiveTypeName()
	{
		/* Only numerics and booleans get mapped to Java primitives */
		return "boolean";
	}

	/**
	 * Get the method name for getting out the corresponding primitive
	 * Java type.
	 *
	 * @return String		The method call name for getting the
	 *						corresponding primitive Java type.
	 */
    @Override
	public String getPrimitiveMethodName()
	{
		return "getBoolean";
	}

	/**
	 * @see TypeCompiler#getCastToCharWidth
	 */
	public int getCastToCharWidth(DataTypeDescriptor dts)
	{
		return TypeCompiler.BOOLEAN_MAXWIDTH_AS_CHAR;
	}

	String nullMethodName()
	{
		return "getNullBoolean";
	}
}
