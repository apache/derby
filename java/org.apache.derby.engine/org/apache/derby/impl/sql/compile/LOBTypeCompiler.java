/*

   Derby - Class org.apache.derby.impl.sql.compile.LOBTypeCompiler

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
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.TypeCompiler;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;

/**
 * This class implements TypeCompiler for the SQL LOB types.
 *
 */

public class LOBTypeCompiler extends BaseTypeCompiler
{
    /**
     * Tell whether this type (LOB) can be converted to the given type.
     *
     * @see TypeCompiler#convertible
     */
    public boolean convertible(TypeId otherType,
            boolean forDataTypeFunction)
    {

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
        return  (otherType.isBlobTypeId());
    }

    /**
     * Tell whether this type (LOB) is compatible with the given type.
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
        // no automatic conversions at store time

        return  (otherType.isBlobTypeId());
    }

    /** @see TypeCompiler#interfaceName */
    public String interfaceName()
    {
        return ClassName.BitDataValue;
    }

    /**
     * @see TypeCompiler#getCorrespondingPrimitiveTypeName
     */

    public String getCorrespondingPrimitiveTypeName() {
        int formatId = getStoredFormatIdFromTypeId();
        switch (formatId) {
            case StoredFormatIds.BLOB_TYPE_ID:  return "java.sql.Blob";
            default:
                if (SanityManager.DEBUG) {
                    SanityManager.THROWASSERT(
                        "unexpected formatId " +
                        "in getCorrespondingPrimitiveTypeName() - " + formatId);
                }
                return null;
        }
    }

    /**
     * @see TypeCompiler#getCastToCharWidth
     */
    public int getCastToCharWidth(DataTypeDescriptor dts)
    {
        return dts.getMaximumWidth();
    }

    String nullMethodName() {
        int formatId = getStoredFormatIdFromTypeId();
        switch (formatId) {
            case StoredFormatIds.BLOB_TYPE_ID:  return "getNullBlob";
            default:
                if (SanityManager.DEBUG) {
                    SanityManager.THROWASSERT(
                        "unexpected formatId " +
                        "in nullMethodName() - " + formatId);
                }
                return null;
        }
    }

    @Override
    String dataValueMethodName()
    {
        int formatId = getStoredFormatIdFromTypeId();
        switch (formatId) {
            case StoredFormatIds.BLOB_TYPE_ID:  return "getBlobDataValue";
            default:
                if (SanityManager.DEBUG) {
                    SanityManager.THROWASSERT(
                        "unexpected formatId " +
                        "in dataValueMethodName() - " + formatId);
                }
                return null;
        }
    }
}
