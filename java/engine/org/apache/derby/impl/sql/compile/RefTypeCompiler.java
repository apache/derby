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

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;

import org.apache.derby.iapi.types.RefDataValue;

import org.apache.derby.iapi.sql.compile.TypeCompiler;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.reference.ClassName;

/**
 * This class implements TypeCompiler for the SQL REF datatype.
 *
 * @author Jeff Lichtman
 */

public class RefTypeCompiler extends BaseTypeCompiler
{
	/** @see TypeCompiler#getCorrespondingPrimitiveTypeName */
	public String getCorrespondingPrimitiveTypeName()
	{
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("getCorrespondingPrimitiveTypeName not implemented for SQLRef");
		return null;
	}

	/**
	 * @see TypeCompiler#getCastToCharWidth
	 */
	public int getCastToCharWidth(DataTypeDescriptor dts)
	{
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT( "getCastToCharWidth not implemented for SQLRef");
		return 0;
	}

	/** @see TypeCompiler#convertible */
	public boolean convertible(TypeId otherType, 
							   boolean forDataTypeFunction)
	{
		return false;
	}

	/**
	 * Tell whether this type is compatible with the given type.
	 *
	 * @see TypeCompiler#compatible */
	public boolean compatible(TypeId otherType)
	{
		return convertible(otherType,false);
	}


	/** @see TypeCompiler#comparable */
	public boolean comparable(TypeId otherType,
                              boolean forEquals,
                              ClassFactory cf)
	{
		return false;
	}

	/** @see TypeCompiler#storable */
	public boolean storable(TypeId otherType, ClassFactory cf)
	{
		return otherType.isRefTypeId();
	}

	/** @see TypeCompiler#interfaceName */
	public String interfaceName()
	{
		return ClassName.RefDataValue;
	}

	protected String nullMethodName()
	{
		return "getNullRef";
	}
}
