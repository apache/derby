/*

   Derby - Class org.apache.derby.impl.sql.compile.CLOBTypeCompiler

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

import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.compiler.LocalField;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.iapi.sql.compile.TypeCompiler;

import org.apache.derby.iapi.reference.ClassName;

/**
 * This class implements TypeCompiler for the SQL LOB types.
 *
 */

public class CLOBTypeCompiler extends BaseTypeCompiler
{
        /**
         * Tell whether this type (LOB) can be converted to the given type.
         *
         * @see TypeCompiler#convertible
         */
        public boolean convertible(TypeId otherType, 
								   boolean forDataTypeFunction)
        {
            // allow casting to any string
            return (otherType.isStringTypeId()) ;

        }

        /**
         * Tell whether this type (CLOB) is compatible with the given type.
         *
         * @param otherType     The TypeId of the other type.
         */
		public boolean compatible(TypeId otherType)
		{
				return convertible(otherType,false);
		}

	    /**
         * Tell whether this type (LOB) can be stored into from the given type.
         *
         * @param otherType     The TypeId of the other type.
         * @param cf            A ClassFactory
         */

        public boolean storable(TypeId otherType, ClassFactory cf)
        {
            // no automatic conversions at store time--but string
			// literals (or values of type CHAR/VARCHAR) are STORABLE
            // as clobs, even if the two types can't be COMPARED.
            return (otherType.isStringTypeId()) ;
        }

        /** @see TypeCompiler#interfaceName */
        public String interfaceName()
        {
            return ClassName.StringDataValue;
        }

        /**
         * @see TypeCompiler#getCorrespondingPrimitiveTypeName
         */

        public String getCorrespondingPrimitiveTypeName() {
            int formatId = getStoredFormatIdFromTypeId();
            switch (formatId) {
                case StoredFormatIds.CLOB_TYPE_ID:  return "java.sql.Clob";
                case StoredFormatIds.NCLOB_TYPE_ID: return "java.sql.Clob";
                default:
                    if (SanityManager.DEBUG)
                        SanityManager.THROWASSERT("unexpected formatId in getCorrespondingPrimitiveTypeName() - " + formatId);
                    return null;
            }
        }

        public String getMatchingNationalCharTypeName()
        {
            return TypeId.NCLOB_NAME;
        }

        /**
         * @see TypeCompiler#getCastToCharWidth
         */
        public int getCastToCharWidth(DataTypeDescriptor dts)
        {
                return dts.getMaximumWidth();
        }

        protected String nullMethodName() {
            int formatId = getStoredFormatIdFromTypeId();
            switch (formatId) {
                case StoredFormatIds.CLOB_TYPE_ID:  return "getNullClob";
                case StoredFormatIds.NCLOB_TYPE_ID: return "getNullNClob";
                default:
                    if (SanityManager.DEBUG)
                        SanityManager.THROWASSERT("unexpected formatId in nullMethodName() - " + formatId);
                    return null;
            }
        }

        protected String dataValueMethodName()
        {
            int formatId = getStoredFormatIdFromTypeId();
            switch (formatId) {
                case StoredFormatIds.CLOB_TYPE_ID:  return "getClobDataValue";
                case StoredFormatIds.NCLOB_TYPE_ID: return "getNClobDataValue";
                default:
                    if (SanityManager.DEBUG)
                        SanityManager.THROWASSERT("unexpected formatId in dataValueMethodName() - " + formatId);
                    return null;
                }
        }

    	/** @see TypeCompiler#generateDataValue(MethodBuilder, int, String, LocalField) */
    	public void generateDataValue(MethodBuilder mb, int collationType,
    			String className, LocalField field)
    	{
    		super.generateDataValue(mb, collationType, className, field);
    		generateCollationSensitiveDataValue(mb, collationType, className);
    	}

    	/** @see TypeCompiler#generateNull(MethodBuilder, int, String) */
    	public void generateNull(MethodBuilder mb, int collationType, 
    			String className)
    	{
    		super.generateNull(mb, collationType, className);
    		generateCollationSensitiveDataValue(mb, collationType, className);
    	}
}
