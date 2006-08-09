/*

   Derby - Class org.apache.derby.impl.sql.execute.SqlXmlExecutor

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.types.BooleanDataValue;
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.types.XML;
import org.apache.derby.iapi.types.XMLDataValue;
import org.apache.derby.iapi.types.SqlXmlUtil;

/**
 * This class is really just an execution time "utility" that
 * makes calls to methods on the XMLDataValue interface.  Instances
 * of this class are generated at execution time by the various
 * Derby XML operators--one instance for each row in the target
 * result set--and then the appropriate operator call is made on
 * that instance (see, for example, the generateExpression() methods
 * in UnaryOperatorNode and BinaryOperatorNode).  When an instance
 * of this class is instantiated, one of the arguments that can be
 * provided is an id that is used to retrieve an already-constructed
 * (from compilation time) instance of SqlXmlUtil from the current
 * Activation.  When it comes time to execute the operator, this class
 * just makes the appropriate call on the received XMLDataValue object
 * and passes in the SqlXmlUtil, from which the XMLDataValue can
 * retrieve compile-time objects.  The XMLDataValue can also make
 * calls to various XML-specific utilities on the SqlXmlUtil
 * object.
 *
 * Let's take an example.  Assume the statement that the user
 * wants to execute is:
 *
 *   select id from xtable
 *      where XMLEXISTS('/simple' PASSING BY REF xcol)
 *
 * At compilation time we will compile the expression "/simple"
 * and store the compiled version of the query into an instance
 * of SqlXmlUtil.  Then we will save that instance of SqlXmlUtil
 * as an object in the statement activation, from which we will
 * receive an id that can be used later to retrieve the object
 * (i.e. to retrieve the SqlXmlUtil).  Then, for *each* row
 * in xtable, we'll generate the following:
 *
 *  boolean result =
 *    (new SqlXmlExecutor(activation, compileTimeObjectId)).
 *      XMLExists("/simple", xcol);
 *
 * In other words, for each row we create a new instance of
 * this class and call "XMLExists" on that instance.  Then,
 * as seen below, we retrieve the SqlXmlUtil from the activation
 * and pass that into a call to "XMLExists" on the XML value
 * itself (i.e. xcol).  XMLDataValue.XMLExists() then uses the
 * methods and objects (which include the compiled query
 * expression for "/simple") defined on SqlXmlUtil to complete
 * the operation.
 * 
 * Okay, so why do we use this execution-time SqlXmlExecutor class
 * instead of just generating a call to XMLDataValue.XMLExists()
 * directly?  The reason is that we only want to compile the XML
 * query expression once per statement--and where possible we'd
 * also like to only generate re-usable XML-specific objects
 * once per statement, as well.  If instead we generated a call to
 * XMLDataValue.XMLExists() directly for each row, then we would
 * have to either pass in the expression string and have XMLDataValue
 * compile it, or we would have to compile the expression string
 * and then pass the compiled object into XMLDataValue--in either
 * case, we'd end up compiling the XML query expression (and creating
 * the corresponding XML-specific objects) once for each row in
 * the target result set.  By using the "saveObject" functionality
 * in Activation along with this SqlXmlExecutor class, we make
 * it so that we only have to compile the XML query expression and
 * create XML-specific objects once (at compile time), and then
 * we can re-use those objects for every row in the target
 * result set.  Yes, we're still creating an instance of this
 * class (SqlXmlExecutor) once per row, and yes we have to fetch
 * the appropriate SqlXmlUtil object once per row, but this is
 * still going to be cheaper than having to re-compile the query
 * expression and re-create XML objects for every row.
 * 
 * So in short, this class allows us to improve the execution-time
 * performance of XML operators by allowing us to create XML-
 * specific objects and compile XML query expressions once per
 * statement, instead of once per row.
 *
 * One final note: the reason this class is in this package
 * instead of the types package is that, in order to retrieve
 * the compile-time objects, we have to use the "getSavedObject()"
 * method on the Activation.  But the Activation class is part
 * of the SQL layer (org.apache.derby.iapi.sql.Activation) and
 * we want to keep the types layer independent of the SQL layer
 * because the types can be used during recovery before the SQL
 * system has booted.  So the next logical choices were the compile
 * package (impl.sql.compile) or the execution package; of those,
 * the execution package seems more appropriate since this
 * class is only instantiated and used during execution, not
 * during compilation.
 */

public class SqlXmlExecutor {

    // The activation from which we load the compile-time XML
    // objects (including the compiled XML query expression in
    // case of XMLEXISTS and XMLQUERY).
    private Activation activation;
    private int sqlXUtilId;

    // Target type and target width that were specified
    // for an XMLSERIALIZE operator.
    private int targetTypeId;
    private int targetMaxWidth;

    // Whether or not to preserve whitespace for XMLPARSE
    // operator.
    private boolean preserveWS;

    /**
     * Constructor 1: Used for XMLPARSE op.
     * @param activation Activation from which to retrieve saved objects
     * @param utilId Id by which we find saved objects in activation
     * @param preserveWS Whether or not to preserve whitespace
     */
    public SqlXmlExecutor(Activation activation, int utilId,
        boolean preserveWS)
    {
        this.activation = activation;
        this.sqlXUtilId = utilId;
        this.preserveWS = preserveWS;
    }

    /**
     * Constructor 2: Used for XMLSERIALIZE op.
     * @param targetTypeId The string type to which we want to serialize.
     * @param targetMaxWidth The max width of the target type.
     */
    public SqlXmlExecutor(int targetTypeId, int targetMaxWidth)
    {
        this.targetTypeId = targetTypeId;
        this.targetMaxWidth = targetMaxWidth;
    }

    /**
     * Constructor 3: Used for XMLEXISTS/XMLQUERY ops.
     * @param activation Activation from which to retrieve saved objects
     * @param utilId Id by which we find saved objects in activation
     */
    public SqlXmlExecutor(Activation activation, int utilId)
    {
        this.activation = activation;
        this.sqlXUtilId = utilId;
    }

    /**
     * Make the call to perform an XMLPARSE operation on the
     * received XML string and store the result in the received
     * XMLDataValue (or if it's null, create a new one).
     *
     * @param xmlText String to parse
     * @param result XMLDataValue in which to store the result
     * @return The received XMLDataValue with its content set to
     *  correspond to the received xmlText, if the text constitutes
     *  a valid XML document.  If the received XMLDataValue is
     *  null, then create a new one and set its content to
     *  correspond to the received xmlText.
     */
    public XMLDataValue XMLParse(StringDataValue xmlText, XMLDataValue result)
        throws StandardException
    {
        if (result == null)
            result = new XML();

        if (xmlText.isNull())
        {
            result.setToNull();
            return result;
        }

        return result.XMLParse(
            xmlText.getString(), preserveWS, getSqlXmlUtil());
    }

    /**
     * Make the call to perform an XMLSERIALIZE operation on the
     * received XML data value and store the result in the received
     * StringDataValue (or if it's null, create a new one).
     *
     * @param xmlVal XML value to serialize
     * @param result StringDataValue in which to store the result
     * @return A serialized (to string) version of this XML object,
     *  in the form of a StringDataValue object.  
     */
    public StringDataValue XMLSerialize(XMLDataValue xmlVal,
        StringDataValue result) throws StandardException
    {
        return xmlVal.XMLSerialize(result, targetTypeId, targetMaxWidth);
    }

    /**
     * Make the call to perform an XMLEXISTS operation on the
     * received XML data value.
     *
     * @param xExpr Query expression to be evaluated
     * @param xmlContext Context node against which to evaluate
     *  the expression.
     * @return True if evaluation of the query expression
     *  against xmlContext returns at least one item; unknown if
     *  either the xml value is NULL; false otherwise. 
     */
    public BooleanDataValue XMLExists(StringDataValue xExpr,
        XMLDataValue xmlContext) throws StandardException
    {
        return xmlContext.XMLExists(getSqlXmlUtil());
    }

    /**
     * Make the call to perform an XMLQUERY operation on the
     * received XML data value and store the result in the
     * received result holder (or, if it's null, create a
     * new one).
     *
     * @param xExpr Query expression to be evaluated
     * @param xmlContext Context node against which to evaluate
     *  the expression.
     * @param result XMLDataValue in which to store the result
     * @return The received XMLDataValue with its content set to
     *  result of evaulating the query expression against xmlContext.
     *  If the received XMLDataValue is null, then create a new one
     *  and set its content to correspond to the received xmlText.
     */
    public XMLDataValue XMLQuery(StringDataValue xExpr,
        XMLDataValue xmlContext, XMLDataValue result)
        throws StandardException
    {
        return xmlContext.XMLQuery(result, getSqlXmlUtil());
    }

    /**
     * Return the saved object in this.activation that corresponds to
     * this.sqlxUtilId.  Assumption is that those fields have been
     * set by the time we get here.
     */
    private SqlXmlUtil getSqlXmlUtil()
        throws StandardException
    {
        return (SqlXmlUtil)
            activation.getPreparedStatement().getSavedObject(sqlXUtilId);
    }

}
