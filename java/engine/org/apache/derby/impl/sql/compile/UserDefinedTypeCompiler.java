/*

   Derby - Class org.apache.derby.impl.sql.compile.UserDefinedTypeCompiler

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
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.sql.compile.TypeCompiler;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.reference.ClassName;

import java.sql.Types;

public class UserDefinedTypeCompiler extends BaseTypeCompiler
{
	/* TypeCompiler methods */

	/**
	 * User types are comparable to other user types only if
	 * (for now) they are the same type and are being used to
	 * implement some JDBC type.  This is sufficient for
	 * date/time types; it may be generalized later for e.g.
	 * comparison of any user type with one of its subtypes.
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
		if (forEquals)
			return true;

		try {
			Class thisClass = cf.getClassInspector().getClass(
									getTypeId().getCorrespondingJavaTypeName());

			return java.lang.Comparable.class.isAssignableFrom(thisClass);

		} catch (ClassNotFoundException cnfe) {
			return false;
		}
	}

	/**
	 * User types are convertible to other user types only if
	 * (for now) they are the same type and are being used to
	 * implement some JDBC type.  This is sufficient for
	 * date/time types; it may be generalized later for e.g.
	 * comparison of any user type with one of its subtypes.
	 *
	 * @param otherType 
	 * @param forDataTypeFunction
	 * @return true if otherType is convertible to this type, else false.
	 * 
	 *@see TypeCompiler#convertible
	 */
	public boolean convertible(TypeId otherType, boolean forDataTypeFunction)
	{
		/*
		** We are a user defined type, we are
		** going to have to let the client find out
		** the hard way.
		*/
		return true;
	}

	 /** @see TypeCompiler#compatible */
	public boolean compatible(TypeId otherType)
	{
		return convertible(otherType, false);
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
		return cf.getClassInspector().assignableTo(
			   otherType.getCorrespondingJavaTypeName(),
			   getTypeId().getCorrespondingJavaTypeName());
	}

	/** @see TypeCompiler#interfaceName */
	public String interfaceName()
	{
		return ClassName.UserDataValue;
	}
			
	/**
	 * @see TypeCompiler#getCorrespondingPrimitiveTypeName
	 */

	public String getCorrespondingPrimitiveTypeName()
	{
		return getTypeId().getCorrespondingJavaTypeName();
	}

	/**
	 * @see TypeCompiler#getCastToCharWidth
	 */
	public int getCastToCharWidth(DataTypeDescriptor dts)
	{
		// This is the maximum maximum width for user types
		return -1;
	}

	protected String nullMethodName()
	{
		return "getNullObject";
	}
}
