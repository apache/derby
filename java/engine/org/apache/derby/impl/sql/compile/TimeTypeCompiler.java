/*

   Derby - Class org.apache.derby.impl.sql.compile.TimeTypeCompiler

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

import org.apache.derby.iapi.services.loader.ClassFactory;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DateTimeDataValue;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.sql.compile.TypeCompiler;

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.sql.Types;
import org.apache.derby.iapi.reference.ClassName;

public class TimeTypeCompiler extends BaseTypeCompiler
{
	/* TypeCompiler methods */

	/**
	 * Dates are comparable to timestamps and to comparable
	 * user types.
	 *
	 * @param otherType the type of the instance to compare with this type.
	 * @param forEquals True if this is an = or <> comparison, false
	 *					otherwise.
	 * @param cf		A ClassFactory
	 * @return true if otherType is comparable to this type, else false.
	 */
	public boolean comparable(TypeId otherType,
                              boolean forEquals,
                              ClassFactory cf)
	{
		int otherJDBCTypeId = otherType.getJDBCTypeId();

		// Long types cannot be compared
		if (otherType.isLongConcatableTypeId())
			return false;

		if (otherJDBCTypeId == Types.TIME || otherType.isStringTypeId())
			return true;

		TypeCompiler otherTC = getTypeCompiler(otherType);

		/* User types know the rules for what can be compared to them */
		if (otherType.userType())
		{
			return otherTC.comparable(getTypeId(), forEquals, cf);
		}

		return false;
	}

	/**
	 * User types are convertible to other user types only if
	 * (for now) they are the same type and are being used to
	 * implement some JDBC type.  This is sufficient for
	 * date/time types; it may be generalized later for e.g.
	 * comparison of any user type with one of its subtypes.
	 *
	 * @see TypeCompiler#convertible 
	 */
	public boolean convertible(TypeId otherType,
							   boolean forDataTypeFunction)
	{

		if (otherType.isStringTypeId() && 
			(!otherType.isLOBTypeId()) &&
			!otherType.isLongVarcharTypeId())
		{
			return true;
		}


		/*
		** If same type, convert always ok.
		*/
		return (getStoredFormatIdFromTypeId() == 
				otherType.getTypeFormatId());
		   
	}

	/** @see TypeCompiler#compatible */
	public boolean compatible(TypeId otherType)
	{
		return convertible(otherType,false);
	}

	/**
	 * User types are storable into other user types that they
	 * are assignable to. The other type must be a subclass of
	 * this type, or implement this type as one of its interfaces.
	 *
	 * Built-in types are also storable into user types when the built-in
	 * type's corresponding Java type is assignable to the user type.
	 *
	 * @param otherType the type of the instance to store into this type.
	 * @param cf		A ClassFactory
	 * @return true if otherType is storable into this type, else false.
	 */
	public boolean storable(TypeId otherType, ClassFactory cf)
	{
		int	otherJDBCTypeId = otherType.getJDBCTypeId();

		if (otherJDBCTypeId == Types.TIME ||
			(otherJDBCTypeId == Types.CHAR) ||
			(otherJDBCTypeId == Types.VARCHAR))
		{
			return true;
		}

		return cf.getClassInspector().assignableTo(
			   otherType.getCorrespondingJavaTypeName(),
			   "java.sql.Time");
	}

	/** @see TypeCompiler#interfaceName */
	public String interfaceName()
	{
		return ClassName.DateTimeDataValue;
	}
			
	/**
	 * @see TypeCompiler#getCorrespondingPrimitiveTypeName
	 */

	public String getCorrespondingPrimitiveTypeName()
	{
		return "java.sql.Time";
	}

	/**
	 * @see TypeCompiler#getCastToCharWidth
	 */
	public int getCastToCharWidth(DataTypeDescriptor dts)
	{
		return 8;
	}

	public double estimatedMemoryUsage(DataTypeDescriptor dtd)
	{
		return 12.0;
	}

	protected String nullMethodName()
	{
		return "getNullTime";
	}
}
