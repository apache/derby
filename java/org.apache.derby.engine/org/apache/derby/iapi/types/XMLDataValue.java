/*

   Derby - Class org.apache.derby.iapi.types.XMLDataValue

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

package org.apache.derby.iapi.types;

import org.apache.derby.shared.common.error.StandardException;

public interface XMLDataValue extends DataValueDescriptor
{
   /**
     * Method to parse an XML string and, if it's valid,
     * store the _serialized_ version locally and then return
     * this XMLDataValue.
     *
     * @param stringValue The string value to check.
     * @param preserveWS Whether or not to preserve
     *  ignorable whitespace.
     * @param sqlxUtil Contains SQL/XML objects and util
     *  methods that facilitate execution of XML-related
     *  operations
     * @return If 'text' constitutes a valid XML document,
     *  it has been stored in this XML value and this XML
     *  value returned; otherwise, an exception is thrown. 
     * @exception StandardException Thrown on error.
     */
    public XMLDataValue XMLParse(
            StringDataValue stringValue,
            boolean preserveWS,
            SqlXmlUtil sqlxUtil)
        throws StandardException;

    /**
     * The SQL/XML XMLSerialize operator.
     * Serializes this XML value into a string with a user-specified
     * character type, and returns that string via the received
     * StringDataValue (if the received StringDataValue is non-null
     * and of the correct type; else, a new StringDataValue is
     * returned).
     *
     * @param result The result of a previous call to this method,
     *  null if not called yet.
     * @param targetType The string type to which we want to serialize.
     * @param targetWidth The width of the target type.
     * @param targetCollationType The collation type of the target type.
     * @return A serialized (to string) version of this XML object,
     *  in the form of a StringDataValue object.
     * @exception StandardException Thrown on error
     */
    public StringDataValue XMLSerialize(StringDataValue result,
        int targetType, int targetWidth, int targetCollationType) 
    throws StandardException;

    /**
     * The SQL/XML XMLExists operator.
     * Checks to see if evaluation of the query expression contained
     * within the received util object against this XML value returns
     * at least one item. NOTE: For now, the query expression must be
     * XPath only (XQuery not supported) because that's what Xalan
     * supports.
     *
     * @param sqlxUtil Contains SQL/XML objects and util
     *  methods that facilitate execution of XML-related
     *  operations
     * @return True if evaluation of the query expression stored
     *  in sqlxUtil returns at least one node for this XML value;
     *  unknown if the xml value is NULL; false otherwise.
     * @exception StandardException Thrown on error
     */
    public BooleanDataValue XMLExists(SqlXmlUtil sqlxUtil)
		throws StandardException;

    /**
     * Evaluate the XML query expression contained within the received
     * util object against this XML value and store the results into
     * the received XMLDataValue "result" param (assuming "result" is
     * non-null; else create a new XMLDataValue).
     *
     * @param sqlxUtil Contains SQL/XML objects and util methods that
     *  facilitate execution of XML-related operations
     * @param result The result of a previous call to this method; null
     *  if not called yet.
     * @return An XMLDataValue whose content corresponds to the serialized
     *  version of the results from evaluation of the query expression.
     *  Note: this XMLDataValue may not be storable into Derby XML
     *  columns.
     * @exception StandardException thrown on error
     */
    public XMLDataValue XMLQuery(SqlXmlUtil sqlxUtil, XMLDataValue result)
		throws StandardException;
//IC see: https://issues.apache.org/jira/browse/DERBY-688
//IC see: https://issues.apache.org/jira/browse/DERBY-567

    /* ****
     * Helper classes and methods.
     * */

    /**
     * Set this XML value's qualified type.
     */
    public void setXType(int xtype);

    /**
     * Retrieve this XML value's qualified type.
     */
    public int getXType();

    /**
     * Take note of the fact this XML value represents an XML
     * sequence that has one or more top-level attribute nodes.
     */
    public void markAsHavingTopLevelAttr();

    /**
     * Return whether or not this XML value represents a sequence
     * that has one or more top-level attribute nodes.
     */
    public boolean hasTopLevelAttr();
}
