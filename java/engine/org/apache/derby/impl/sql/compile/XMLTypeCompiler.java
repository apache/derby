/*

   Derby - Class org.apache.derby.impl.sql.compile.XMLTypeCompiler

   Copyright 2005 The Apache Software Foundation or its licensors, as applicable.

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
import org.apache.derby.iapi.sql.compile.TypeCompiler;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.iapi.reference.ClassName;

/**
 * This class implements TypeCompiler for the XML type.
 */

public class XMLTypeCompiler extends BaseTypeCompiler
{
    /**
     * Tell whether this type (XML) can be compared to the given type.
     * Says SQL/XML[2003] spec:
     *
     * 4.2.2 XML comparison and assignment
     * "XML values are not comparable."
     *
     * @param otherType The TypeId of the other type.
     */
    public boolean comparable(TypeId otherType,
                            boolean forEquals,
                            ClassFactory cs)
    {
        // An XML value cannot be compared to any type--
        // not even to other XML values.
        return false;
    }

    /**
     * Tell whether this type (XML) can be converted to the given type.
     *
     * An XML value can't be converted to any other type, per
     * SQL/XML[2003] 6.3 <cast specification>
     *
     * @see TypeCompiler#convertible
     */
    public boolean convertible(TypeId otherType, 
                            boolean forDataTypeFunction)
    {
        // An XML value cannot be converted to any non-XML type.  If
        // user wants to convert an XML value to a string, then
        // s/he must use the provided SQL/XML serialization operator
        // (namely, XMLSERIALIZE).
        return otherType.isXMLTypeId();
    }

    /**
     * Tell whether this type (XML) is compatible with the given type.
     *
     * @param otherType The TypeId of the other type.
     */
    public boolean compatible(TypeId otherType)
    {
        // An XML value is not compatible (i.e. cannot be "coalesced")
        // into any non-XML type.
        return otherType.isXMLTypeId();
    }

    /**
     * Tell whether this type (XML) can be stored into from the given type.
     * Only XML values can be stored into an XML type, per SQL/XML spec:
     *
     * 4.2.2 XML comparison and assignment
     * Values of XML type are assignable to sites of XML type.
     *
     * @param otherType The TypeId of the other type.
     * @param cf A ClassFactory
     */
    public boolean storable(TypeId otherType, ClassFactory cf)
    {
        // The only type of value that can be stored as XML
        // is an XML value.  Strings are not allowed.  If
        // the user wants to store a string value as XML,
        // s/he must use the provided XML parse operator
        // (namely, XMLPARSE) to parse the string into
        // XML.
        return otherType.isXMLTypeId();
    }

    /**
     * @see TypeCompiler#interfaceName
     */
    public String interfaceName() {
        return ClassName.XMLDataValue;
    }

    /**
     * @see TypeCompiler#getCorrespondingPrimitiveTypeName
     */
    public String getCorrespondingPrimitiveTypeName()
    {
        int formatId = getStoredFormatIdFromTypeId();
        if (formatId == StoredFormatIds.XML_TYPE_ID)
            return "org.apache.derby.iapi.types.XML";

        if (SanityManager.DEBUG) {
            SanityManager.THROWASSERT(
                "unexpected formatId in getCorrespondingPrimitiveTypeName(): "
                + formatId);
        }

        return null;
    }

    /**
     * @see TypeCompiler#getCastToCharWidth
     *
     * While it is true XML values can't be cast to char, this method
     * can get called before we finish type checking--so we return a dummy
     * value here and let the type check throw the appropriate error.
     */
    public int getCastToCharWidth(DataTypeDescriptor dts)
    {
        return -1;
    }

    /**
     * @see BaseTypeCompiler#nullMethodName
     */
    protected String nullMethodName()
    {
        int formatId = getStoredFormatIdFromTypeId();
        if (formatId == StoredFormatIds.XML_TYPE_ID)
            return "getNullXML";

        if (SanityManager.DEBUG) {
            SanityManager.THROWASSERT(
                "unexpected formatId in nullMethodName(): " + formatId);
        }

        return null;
    }

    /**
     * @see BaseTypeCompiler#dataValueMethodName
     */
    protected String dataValueMethodName()
    {
        int formatId = getStoredFormatIdFromTypeId();
        if (formatId == StoredFormatIds.XML_TYPE_ID)
            return "getXMLDataValue";

        if (SanityManager.DEBUG) {
            SanityManager.THROWASSERT(
                "unexpected formatId in dataValueMethodName() - " + formatId);
        }

        return null;
    }
}
