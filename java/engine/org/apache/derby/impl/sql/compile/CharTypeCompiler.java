/*

   Derby - Class org.apache.derby.impl.sql.compile.CharTypeCompiler

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

import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.iapi.sql.compile.TypeCompiler;

import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.util.StringUtil;

import java.sql.Types;
import org.apache.derby.iapi.reference.JDBC20Translation;

/**
 * This class implements TypeCompiler for the SQL char datatypes.
 *
 * @author Jeff Lichtman
 */

public final class CharTypeCompiler extends BaseTypeCompiler
{
        /**
         * Tell whether this type (char) can be compared to the given type.
		 * Long types can not be compared.
		 * VARCHAR AND CHAR can be compared to CHAR/VARCHAR/DATE/TIME/TIMESTAMP
		 *
         *
         * @param otherType     The TypeId of the other type.
         */

        public boolean comparable(TypeId otherType,
                                  boolean forEquals,
                                  ClassFactory cf)
		{
				
			// Long Types cannot be compared
			if (correspondingTypeId.isLongConcatableTypeId() ||
				otherType.isLongConcatableTypeId())
				return false;
			
			// CHAR and VARCHAR can compare to Strings or DATE/TIME/TIMESTAMP
			if((otherType.isStringTypeId() ||
				otherType.isDateTimeTimeStampTypeID() ||
				otherType.isBooleanTypeId()))
				return true;
			
			
			TypeCompiler otherTC = getTypeCompiler(otherType);
			return (otherType.userType() && otherTC.comparable(correspondingTypeId,
															   forEquals, cf));
		}
	
	
	   /**
         * Tell whether this type (char) can be converted to the given type.
         *
		 * @see TypeCompiler#convertible
         */
        public boolean convertible(TypeId otherType, boolean forDataTypeFunction)
        {
			// LONGVARCHAR can only be converted from  character types
			// or CLOB.
			if (correspondingTypeId.isLongVarcharTypeId())
			{
				return (otherType.isStringTypeId());
			}

			// The double function can convert CHAR and VARCHAR
			if (forDataTypeFunction && otherType.isDoubleTypeId())
				return (correspondingTypeId.isStringTypeId());

			// can't CAST to CHAR and VARCHAR from REAL or DOUBLE
			// or binary types
			// all other types are ok.
			if (otherType.isFloatingPointTypeId() || otherType.isBitTypeId() ||
				otherType.isBlobTypeId())
				return false;
						
			return true;
        }
	


	/**
	 * Tell whether this type (char) is compatible with the given type.
	 *
	 * @param otherType     The TypeId of the other type.
	 */
	public boolean compatible(TypeId otherType)
	{
		return (otherType.isStringTypeId() || (otherType.isDateTimeTimeStampTypeId() && !correspondingTypeId.isLongVarcharTypeId()));
		
	}

        /**
         * Tell whether this type (char) can be stored into from the given type.
         *
         * @param otherType     The TypeId of the other type.
         * @param cf            A ClassFactory
         */

        public boolean storable(TypeId otherType, ClassFactory cf)
        {
				// Same rules as cast except we can't assign from numbers
				if (convertible(otherType,false) && 
					!otherType.isBlobTypeId() &&
					!otherType.isNumericTypeId())
						return true;

                /*
                ** If the other type is user-defined, use the java types to determine
                ** assignability.
                */
                return userTypeStorable(getTypeId(), otherType, cf);
        }

        /** @see TypeCompiler#interfaceName */
        public String interfaceName()
        {
                return ClassName.StringDataValue;
        }

        /**
         * @see TypeCompiler#getCorrespondingPrimitiveTypeName
         */

        public String getCorrespondingPrimitiveTypeName()
        {
                /* Only numerics and booleans get mapped to Java primitives */
                return "java.lang.String";
        }

        /**
         * @see TypeCompiler#getCastToCharWidth
         */
        public int getCastToCharWidth(DataTypeDescriptor dts)
        {
                return dts.getMaximumWidth();
        }

        /** @see TypeCompiler#getMatchingNationalCharTypeName */
        public String getMatchingNationalCharTypeName()
        {
                int formatId = getStoredFormatIdFromTypeId();
                switch (formatId)
                {
                        case StoredFormatIds.CHAR_TYPE_ID:
                        case StoredFormatIds.NATIONAL_CHAR_TYPE_ID:
                                return TypeId.NATIONAL_CHAR_NAME;

                        case StoredFormatIds.LONGVARCHAR_TYPE_ID:
                        case StoredFormatIds.NATIONAL_LONGVARCHAR_TYPE_ID:
                                return TypeId.NATIONAL_LONGVARCHAR_NAME;

                        case StoredFormatIds.VARCHAR_TYPE_ID:
                        case StoredFormatIds.NATIONAL_VARCHAR_TYPE_ID:
                                return TypeId.NATIONAL_VARCHAR_NAME;

                        default:
                                if (SanityManager.DEBUG)
                                {
                                        SanityManager.THROWASSERT(
                                                "unexpected formatId in getMatchingNationalCharTypeName() - " + formatId);
                                }
                                return null;
                }
        }


        protected String nullMethodName()
        {
                int formatId = getStoredFormatIdFromTypeId();
                switch (formatId)
                {
                        case StoredFormatIds.CHAR_TYPE_ID:
                                return "getNullChar";

                        case StoredFormatIds.LONGVARCHAR_TYPE_ID:
                                return "getNullLongvarchar";

                        case StoredFormatIds.NATIONAL_CHAR_TYPE_ID:
                                return "getNullNationalChar";

                        case StoredFormatIds.NATIONAL_LONGVARCHAR_TYPE_ID:
                                return "getNullNationalLongvarchar";

                        case StoredFormatIds.NATIONAL_VARCHAR_TYPE_ID:
                                return "getNullNationalVarchar";

                        case StoredFormatIds.VARCHAR_TYPE_ID:
                                return "getNullVarchar";

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
                        case StoredFormatIds.CHAR_TYPE_ID:
                                return "getCharDataValue";

                        case StoredFormatIds.LONGVARCHAR_TYPE_ID:
                                return "getLongvarcharDataValue";

                        case StoredFormatIds.NATIONAL_CHAR_TYPE_ID:
                                return "getNationalCharDataValue";

                        case StoredFormatIds.NATIONAL_LONGVARCHAR_TYPE_ID:
                                return "getNationalLongvarcharDataValue";

                        case StoredFormatIds.NATIONAL_VARCHAR_TYPE_ID:
                                return "getNationalVarcharDataValue";

                        case StoredFormatIds.VARCHAR_TYPE_ID:
                                return "getVarcharDataValue";

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
