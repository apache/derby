/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.services.loader.ClassFactory;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.BooleanDataValue;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.sql.compile.TypeCompiler;

import org.apache.derby.catalog.types.BaseTypeIdImpl;
import org.apache.derby.iapi.reference.ClassName;

import java.sql.Types;

/**
 * This class implements TypeCompiler for the SQL BOOLEAN datatype.
 *
 * @author Jerry Brenner
 */

public class BooleanTypeCompiler extends BaseTypeCompiler
{
	/**
	 * Tell whether this type (boolean) can be compared to the given type.
	 *
	 * @param otherType	The TypeId of the other type.
	 */

	public boolean comparable(TypeId otherType,
                              boolean forEquals,
                              ClassFactory cf)
	{
		TypeId thisTypeId = getTypeId();
		TypeCompiler otherTypeCompiler = getTypeCompiler(otherType);

		/* Only allow comparison of Boolean with Boolean or string types */
		return otherType.getSQLTypeName().equals(thisTypeId.getSQLTypeName()) ||
				otherType.isStringTypeId() ||
				otherType.isNumericTypeId() ||
				(otherType.userType() &&
					otherTypeCompiler.comparable(thisTypeId, forEquals, cf));
	}

	/**
	 * Tell whether this type (boolean) can be converted to the given type.
	 *
	 * @see TypeCompiler#convertible
	 */
	public boolean convertible(TypeId otherType, boolean forDataTypeFunction)
	{
		int otherJDBCTypeId = otherType.getJDBCTypeId();

		if ((otherJDBCTypeId == Types.DATE) ||
			(otherJDBCTypeId == Types.TIME) ||
			(otherJDBCTypeId == Types.TIMESTAMP))
		{
			return false;
		}

		return true;
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
		/* Are the types the same or is other type a string or number type? */
		if (otherType.isBooleanTypeId() ||
				otherType.isStringTypeId() ||
				otherType.isNumericTypeId())
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

	protected String nullMethodName()
	{
		return "getNullBoolean";
	}
}
