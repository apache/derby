/*

   Derby - Class org.apache.derby.impl.sql.compile.BitTypeCompiler

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

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.BitDataValue;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.iapi.sql.compile.TypeCompiler;

import org.apache.derby.iapi.reference.ClassName;

import java.sql.Types;
import org.apache.derby.iapi.reference.JDBC20Translation;

/**
 * This class implements TypeCompiler for the SQL BIT datatype.
 *
 * @author Jamie
 */

public class BitTypeCompiler extends BaseTypeCompiler
{
        /**
         * Tell whether this type (bit) can be compared to the given type. // 
         *
		 * Bit Types can only be compared to Bit Types.
		 * Long Bit Types can not be compared
         * @param otherType     The TypeId of the other type.
         */

        public boolean comparable(TypeId otherType,
                                  boolean forEquals,
                                  ClassFactory cf)
		{

			if (correspondingTypeId.isLongConcatableTypeId() ||
				otherType.isLongConcatableTypeId())
				return false;

			TypeCompiler otherTC = getTypeCompiler(otherType);
			return (otherType.isBitTypeId() || 
					(otherType.userType() &&
					 otherTC.comparable(getTypeId(), forEquals, cf)));
        }
	
        /**
         * Tell whether this type (bit) can be converted to the given type.
         *
         * @see TypeCompiler#convertible
         */
        public boolean convertible(TypeId otherType, 
								   boolean forDataTypeFunction)
        {


			return (otherType.isBitTypeId() ||
					otherType.isBlobTypeId() ||
					otherType.isBooleanTypeId() ||
					otherType.userType());
		}

	
        /**
         * Tell whether this type (bit) is compatible with the given type.
         *
         * @param otherType     The TypeId of the other type.
         */
        public boolean compatible(TypeId otherType)
        {
        if (otherType.isBlobTypeId())
          return false;
        return (otherType.isBitTypeId());
        }

        /**
         * Tell whether this type (bit) can be stored into from the given type.
         *
         * @param otherType     The TypeId of the other type.
         * @param cf            A ClassFactory
         */

        public boolean storable(TypeId otherType, ClassFactory cf)
        {
        if (otherType.isBlobTypeId())
          return false;
				if (otherType.isBitTypeId())
				{
						return true;
				}

                /*
                ** If the other type is user-defined, use the java types to determine
                ** assignability.
                */
                return userTypeStorable(this.getTypeId(), otherType, cf);
        }

        /** @see TypeCompiler#interfaceName */
        public String interfaceName()
        {
                // may need to return different for Blob
                // however, since it the nullMethodName()
                // does not operate on a BitTypeCompiler object?
                // it should?
                return ClassName.BitDataValue;
        }

        /**
         * @see TypeCompiler#getCorrespondingPrimitiveTypeName
         */

        public String getCorrespondingPrimitiveTypeName()
        {
            return "byte[]";
        }

        /**
         * @see TypeCompiler#getCastToCharWidth
         */
        public int getCastToCharWidth(DataTypeDescriptor dts)
        {
                return dts.getMaximumWidth();
        }

        protected String nullMethodName()
        {
                int formatId = getStoredFormatIdFromTypeId();
                switch (formatId)
                {
                        case StoredFormatIds.BIT_TYPE_ID:
                                return "getNullBit";

                        case StoredFormatIds.LONGVARBIT_TYPE_ID:
                                return "getNullLongVarbit";

                        case StoredFormatIds.VARBIT_TYPE_ID:
                                return "getNullVarbit";

                        default:
                                if (SanityManager.DEBUG)
                                {
                                        SanityManager.THROWASSERT(
                                                "unexpected formatId in nullMethodName() - " + formatId);
                                }
                                return null;
                }
        }

        protected String dataValueMethodName()
        {
                int formatId = getStoredFormatIdFromTypeId();
                switch (formatId)
                {
                        case StoredFormatIds.BIT_TYPE_ID:
                                return "getBitDataValue";

                        case StoredFormatIds.LONGVARBIT_TYPE_ID:
                                return "getLongVarbitDataValue";

                        case StoredFormatIds.VARBIT_TYPE_ID:
                                return "getVarbitDataValue";

                        default:
                                if (SanityManager.DEBUG)
                                {
                                        SanityManager.THROWASSERT(
                                                "unexpected formatId in dataValueMethodName() - " + formatId);
                                }
                                return null;
                }
        }
}
