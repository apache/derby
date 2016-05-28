/*

   Derby - Class org.apache.derby.impl.sql.compile.RefTypeCompiler

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

import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.TypeCompiler;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;

/**
 * This class implements TypeCompiler for the SQL REF datatype.
 *
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

	String nullMethodName()
	{
		return "getNullRef";
	}
}
