/*

   Derby - Class org.apache.derby.iapi.types.XMLDataValue

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

package org.apache.derby.iapi.types;

import org.apache.derby.iapi.error.StandardException;

public interface XMLDataValue extends DataValueDescriptor
{
    /*
     ** NOTE: Officially speaking, the XMLParse operator
     ** is not defined here; it is instead defined on the
     ** StringDataValue interface (and implemented in
     ** SQLChar.java) since it is called with a _String_
     ** operand, not with an XML operand.  That said,
     ** though, the implemention in SQLChar.java
     ** really just calls the "parseAndLoadXML" method that's
     ** defined on this interface, so it's this interface
     ** that really does the work.
     **
     ** XMLSerialize and XMLExists, on the other hand,
     ** are called with XML operands, and thus they
     ** can just be defined in this interface.
     */

    /**
     * Parse the received string value as XML.  If the
     * parse succeeds, store the string value as the
     * contents of this XML value.
     *
     * @param text The string value to check.
     * @param preserveWS Whether or not to preserve
     *  ignorable whitespace.
     * @return  If 'text' constitutes a valid XML document,
     *  it has been stored in this XML value and nothing
     *  is returned; otherwise, an exception is thrown.
     * @exception StandardException Thrown on parse error.
     */
    public void parseAndLoadXML(String xmlText, boolean preserveWS)
        throws StandardException;

    /**
     * The SQL/XML XMLSerialize operator.
     * Converts this XML value into a string with a user-specified
     * type, and returns that string via the received StringDataValue.
     * (if the received StringDataValue is non-null and of the
     * correct type; else, a new StringDataValue is returned).
     *
     * @param result The result of a previous call to this method,
     *  null if not called yet.
     * @param targetType The string type to which we want to serialize.
     * @param targetWidth The width of the target type.
     * @return A serialized (to string) version of this XML object,
     *  in the form of a StringDataValue object.
     * @exception StandardException Thrown on error
     */
    public StringDataValue XMLSerialize(StringDataValue result,
        int targetType, int targetWidth) throws StandardException;

    /**
     * The SQL/XML XMLExists operator.
     * Takes an XML query expression (as a string) and an XML
     * value and checks if at least one node in the XML
     * value matches the query expression.  NOTE: For now,
     * the query expression must be XPath only (XQuery not
     * supported).
     *
     * @param xExpr The query expression, as a string.
     * @param xml The XML value being queried.
     * @return True if the received query expression matches at
     *  least one node in the received XML value; unknown if
     *  either the query expression or the xml value is null;
     *  false otherwise.
     * @exception StandardException Thrown on error
     */
    public BooleanDataValue XMLExists(StringDataValue xExpr,
        XMLDataValue xml) throws StandardException;

    /**
     * Helper method for XMLExists.
     * See if the received XPath expression returns at least
     * one node when evaluated against _this_ XML value.
     *
     * @param xExpr The XPath expression.
     * @return True if at least one node in this XML value
     *  matches the received xExpr; false otherwise.
     */
    public boolean exists(String xExpr) throws StandardException;
}
