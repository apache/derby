/*

   Derby - Class org.apache.derby.impl.sql.compile.BitTypeCompiler

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
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.TypeCompiler;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;

/**
 * This class implements TypeCompiler for the SQL BIT datatype.
 *
 */

public class BitTypeCompiler extends BaseTypeCompiler
{
        /**
         * Tell whether this type (bit) can be converted to the given type.
         *
         * @see TypeCompiler#convertible
         */
        public boolean convertible(TypeId otherType, 
								   boolean forDataTypeFunction)
        {
            if ( otherType.getBaseTypeId().isAnsiUDT() ) { return false; }


			return (otherType.isBitTypeId() ||
					otherType.isBlobTypeId() ||
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

        String nullMethodName()
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

        @Override
        String dataValueMethodName()
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
