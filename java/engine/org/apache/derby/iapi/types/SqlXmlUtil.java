/*

   Derby - Class org.apache.derby.iapi.types.SqlXmlUtil

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

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.sanity.SanityManager;

import java.util.Properties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import java.io.IOException;
import java.io.StringReader;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;

// -- JDBC 3.0 JAXP API classes.

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.Text;

import org.w3c.dom.xpath.XPathEvaluator;
import org.w3c.dom.xpath.XPathExpression;
import org.w3c.dom.xpath.XPathResult;

import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

/**
 * This class contains "utility" methods that work with XML-specific
 * objects that are only available if JAXP and/or Xalan are in
 * the classpath.
 *
 * NOTE: This class is only compiled with JDK 1.4 and higher since
 * the XML-related classes that it uses (JAXP and Xalan) are not
 * part of earlier JDKs.
 *
 * Having a separate class for this functionality is beneficial
 * for two reasons:
 *
 *    1. Allows us to allocate XML objects and compile an XML
 *       query expression a single time per statement, instead of
 *       having to do it for every row against which the query
 *       is evaluated.  An instance of this class is created at
 *       compile time and then passed to the appropriate operator
 *       implementation method in XML.java.
 *
 *    2. By keeping all XML-specific references in this one class, 
 *       we have a single "point of entry" to the XML objects--namely,
 *       the constructor for this class.  Thus, if we always make
 *       sure to check for the required XML classes _before_ calling
 *       this class's constructor, we can detect early on whether
 *       some classes (ex. Xalan) are missing, and can throw a friendly
 *       error up front, instead of a ClassNotFoundException somewhere
 *       deeper in the execution codepath.  The initial check for the
 *       required XML classes can be found in XML.checkXMLRequirements().
 *
 *       Note that we don't want to put references to XML-specific
 *       objects directly into XML.java because that class (XML.java) is
 *       instantiated anytime a table with an XML column is referenced.
 *       That would mean that if a user tried to select a non-XML column
 *       (ex. integer) from a table that had at least one XML column in
 *       it, the user would have to have JAXP and Xalan classes in
 *       his/her classpath--which we don't want.  Instead, by keeping
 *       all XML-specific objects in this one class, and then only
 *       instantiating this class when an XML operator is used (either
 *       implicitly or explicitly), we make it so that the user is only
 *       required to have XML-specific classes in his/her classpath
 *       _if_ s/he is trying to access or operate on XML values.
 */

public class SqlXmlUtil
{
    // Used to parse a string into an XML value (DOM); checks
    // the well-formedness of the string while parsing.
    private DocumentBuilder dBuilder;

    // Used to serialize an XML value according the standard
    // XML serialization rules.
    private Transformer serializer;

    // Classes used to compile and execute an XPath expression
    // against Xalan.
    private XPathExpression query;

    // Used to recompile the XPath expression when this formatable
    // object is reconstructed.  e.g.:  SPS 
    private String queryExpr;
    private String opName;
    private boolean recompileQuery;

    /**
     * <p>
     * An object representing the {@code BigDecimal.toPlainString()} method
     * if it's available on the platform. If it's not available, this field
     * will be initialized to {@code null}, and in that case the
     * {@code BigDecimal.toString()} method should be used instead without
     * reflection.
     * </p>
     *
     * <p>
     * The behaviour of the {@code toString()} method changed when
     * {@code toPlainString()} was introduced in Java SE 5. On older
     * platforms, it behaves just like {@code toPlainString()} does on
     * newer platforms. So when {@code toPlainString()} is not
     * available, it is safe to fall back to {@code toString()}. It
     * behaves differently on newer platforms, so we need to use
     * {@code toPlainString()} when it is available in order to get
     * consistent behaviour across all platforms.
     * </p>
     *
     * @see #numberToString(double)
     */
    private static final Method TO_PLAIN_STRING;
    static {
        Method m = null;
        try {
            m = BigDecimal.class.getMethod("toPlainString", new Class[0]);
        } catch (NoSuchMethodException nsme) {
            // Couldn't find the method, so we'll just fall back to toString()
            // on this platform.
        }
        TO_PLAIN_STRING = m;
    }

    /**
     * Constructor: Initializes objects required for parsing
     * and serializing XML values.  Since most XML operations
     * that require XML-specific classes perform both parsing
     * and serialization at some point, we just initialize the
     * objects up front.
     */
    public SqlXmlUtil() throws StandardException
    {
        try {

            /* Note: Use of DocumentBuilderFactory means that we get
             * whatever XML parser is the "default" for the JVM in
             * use--and thus, we don't have to hard-code the parser
             * name, nor do we have to require that the user have a
             * specific parser in his/her classpath.
             *
             * This DocumentBuilder is currently used for parsing
             * (esp. XMLPARSE), and the SQL/XML spec says that XMLPARSE
             * should NOT perform validation (SQL/XML[2006], 6.15:
             * "Perform a non-validating parse of a string to produce
             * an XML value.").   So we disable validation here, and
             * we also make the parser namespace aware.
             *
             * At some point in the future we will probably want to add
             * support for the XMLVALIDATE function--but until then, user
             * is unable to validate the XML values s/he inserts.
             *
             * Note that, even with validation turned off, XMLPARSE
             * _will_ still check the well-formedness of the values,
             * and it _will_ still process DTDs to get default values,
             * etc--but that's it; no validation errors will be thrown.
             */

            DocumentBuilderFactory dBF = null;
            try {

                dBF = DocumentBuilderFactory.newInstance();

            } catch (Throwable e) {

                /* We assume that if we get an error creating the
                 * DocumentBuilderFactory, it's because there's no
                 * JAXP implementation.  This can happen in the
                 * (admittedly unlikely) case where the classpath
                 * contains the JAXP _interfaces_
                 * and the Xalan classes but does not actually
                 * contain a JAXP _implementation_.  In that case the
                 * check in XML.checkXMLRequirements() will pass
                 * and this class (SqlXmlUtil) will be instantiated
                 * successfully--which is how we get to this constructor.
                 * But then attempts to create a DocumentBuilderFactory
                 * will fail, bringing us here.  Note that we can't
                 * check for a valid JAXP implementation in the
                 * XML.checkXMLRequirements() method because we
                 * always want to allow the XML.java class to be
                 * instantiated, even if the required XML classes
                 * are not present--and that means that it (the
                 * XML class) cannot reference DocumentBuilder nor
                 * any of the JAXP classes directly.
                 */
                 throw StandardException.newException(
                     SQLState.LANG_MISSING_XML_CLASSES, "JAXP");

            }

            dBF.setValidating(false);
            dBF.setNamespaceAware(true);

            // Load document builder that can be used for parsing XML.
            dBuilder = dBF.newDocumentBuilder();
            dBuilder.setErrorHandler(new XMLErrorHandler());

            // Load serializer for serializing XML into string according
            // XML serialization rules.
            loadSerializer();

        } catch (StandardException se) {

            // Just rethrow it.
            throw se;

        } catch (Throwable t) {

            /* Must be something caused by JAXP or Xalan; wrap it in a
             * StandardException and rethrow it. Note: we catch "Throwable"
             * here to catch as many external errors as possible in order
             * to minimize the chance of an uncaught JAXP/Xalan error (such
             * as a NullPointerException) causing Derby to fail in a more
             * serious way.  In particular, an uncaught Java exception
             * like NPE can result in Derby throwing "ERROR 40XT0: An
             * internal error was identified by RawStore module" for all
             * statements on the connection after the failure--which we
             * clearly don't want.  If we catch the error and wrap it,
             * though, the statement will fail but Derby will continue to
             * run as normal.
             */ 
            throw StandardException.newException(
                SQLState.LANG_UNEXPECTED_XML_EXCEPTION, t, t.getMessage());

        }

        // At construction time we don't have an XML query expression
        // to compile.  If one is required, we'll load/compile it later.
        query = null;
    }

    /**
     * Take the received string, which is an XML query expression,
     * compile it, and store the compiled query locally.  Note
     * that for now, we only support XPath because that's what
     * Xalan supports.
     *
     * @param queryExpr The XPath expression to compile
     */
    public void compileXQExpr(String queryExpr, String opName)
        throws StandardException
    {
        try {

            /* The following XPath constructor compiles the expression
             * as part of the construction process.  We pass a null
             * namespace resolver object so that the implementation will
             * provide one for us, which means prefixes will not be resolved
             * in the query (Xalan will just throw an error if a prefix
             * is used).  In the future we may want to revisit this
             * to make it easier for users to query based on namespaces.
             */
            XPathEvaluator eval = (XPathEvaluator)
                dBuilder.getDOMImplementation().getFeature("+XPath", "3.0");
            query = eval.createExpression(queryExpr, null);

            this.queryExpr = queryExpr;
            this.opName = opName;
            this.recompileQuery = false;

        } catch (Throwable te) {

            /* Something went wrong during compilation of the
             * expression; wrap the error and re-throw it.
             * Note: we catch "Throwable" here to catch as many
             * Xalan-produced errors as possible in order to
             * minimize the chance of an uncaught Xalan error
             * (such as a NullPointerException) causing Derby
             * to fail in a more serious way.  In particular, an
             * uncaught Java exception like NPE can result in
             * Derby throwing "ERROR 40XT0: An internal error was
             * identified by RawStore module" for all statements on
             * the connection after the failure--which we clearly
             * don't want.  If we catch the error and wrap it,
             * though, the statement will fail but Derby will
             * continue to run as normal. 
             */
            throw StandardException.newException(
                SQLState.LANG_XML_QUERY_ERROR, te, opName, te.getMessage());

        }
    }

    /**
     * Take a string representing an XML value and serialize it
     * according SQL/XML serialization rules.  Right now, we perform
     * this serialization by first parsing the string into a JAXP
     * Document object, and then applying the serialization semantics
     * to that Document.  That seems a bit inefficient, but neither
     * Xalan nor JAXP provides a more direct way to do this.
     *
     * @param xmlAsText String version of XML on which to perform
     *   serialization.
     * @return A properly serialized version of xmlAsText.
     */
    protected String serializeToString(String xmlAsText)
        throws Exception
    {
        ArrayList aList = new ArrayList();

        /* The call to dBuilder.parse() is a call to an external
         * (w.r.t. to Derby) JAXP parser.  If the received XML
         * text references an external DTD, then the JAXP parser
         * will try to read that external DTD.  Thus we wrap the
         * call to parse inside a privileged action to make sure
         * that the JAXP parser has the required permissions for
         * reading the DTD file.
         */
        try {

            final InputSource is = new InputSource(new StringReader(xmlAsText));
            aList.add(java.security.AccessController.doPrivileged(
                new java.security.PrivilegedExceptionAction()
                {
                    public Object run() throws IOException, SAXException
                    {
                        return dBuilder.parse(is);
                    }
                }));

        } catch (java.security.PrivilegedActionException pae) {

            /* Unwrap the privileged exception so that the user can
             * see what the underlying error is. For example, it could
             * be an i/o error from parsing the XML value, which can
             * happen if the XML value references an external DTD file
             * but the JAXP parser hits an i/o error when trying to read
             * the DTD.  In that case we want to throw the i/o error
             * itself so that it does not appear as a security exception
             * to the user.
             */
            throw pae.getException();

        }

        /* The second argument in the following call is for
         * catching cases where we have a top-level (parentless)
         * attribute node--but since we just created the list
         * with a single Document node, we already we know we
         * don't have a top-level attribute node in the list,
         * so we don't have to worry.  Hence the "null" here.
         */
        return serializeToString(aList, null);
    }

    /**
     * Take an array list (sequence) of XML nodes and/or string values
     * and serialize that entire list according to SQL/XML serialization
     * rules, which ultimately point to XML serialization rules as
     * defined by w3c.  As part of that serialization process we have
     * to first "normalize" the sequence.  We do that by iterating through
     * the list and performing the steps for "sequence normalization" as
     * defined here:
     *
     * http://www.w3.org/TR/xslt-xquery-serialization/#serdm
     *
     * This method primarily focuses on taking the steps for normalization;
     * for the rest of the serialization work, we just make calls on the
     * DOMSerializer class provided by Xalan.
     *
     * @param items List of items to serialize
     * @param xmlVal XMLDataValue into which the serialized string
     *  returned by this method is ultimately going to be stored.
     *  This is used for keeping track of XML values that represent
     *  sequences having top-level (parentless) attribute nodes.
     * @return Single string holding the serialized version of the
     *  normalized sequence created from the items in the received
     *  list.
     */
    protected String serializeToString(List items,
        XMLDataValue xmlVal) throws TransformerException
    {
        if ((items == null) || items.isEmpty()) {
        // nothing to do; return empty sequence.
            return "";
        }

        java.io.StringWriter sWriter = new java.io.StringWriter();

        // Serializer should have been set by now.
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(serializer != null,
                "Tried to serialize with uninitialized XML serializer.");
        }

        int sz = items.size();
        Object obj = null;

        /* Step 1: Empty sequence.  If we have an empty sequence then we
         * won't ever enter the for loop and the call to sWriter.toString()
         * at the end of this method will return an empty string, as
         * required.  Otherwise, for a non-empty sequence our "items"
         * list already corresponds to "S1".
         */

        // Iterate through the list and serialize each item.
        boolean lastItemWasString = false;
        for (int i = 0; i < sz; i++)
        {
            obj = items.get(i);
            // if it's a string, then this corresponds to some atomic
            // value, so just echo the string as it is.
            if (obj instanceof String)
            {
                /* Step 2: Atomic values.  If "obj" is a string then it
                 * corresponds to some atomic value whose "lexical
                 * representation" is obj.  So we just take that.
                 */

                if (lastItemWasString)
                {
                    /* Step 3: Adjacent strings.  If we have multiple adjacent
                     * strings then concatenate them with a single space
                     * between them.
                     */
                    sWriter.write(" ");
                }

                /* Step 4: Create a Text node from the adjacent strings.
                 * Since we're just going to serialize the Text node back
                 * into a string, we short-cut this step by skipping the
                 * creation of the Text node and just writing the string
                 * out directly to our serialized stream.
                 */
                sWriter.write((String)obj);
                lastItemWasString = true;
            }
            else if (obj instanceof Attr)
            {
                /* Step 7a: Attribute nodes.  If there is an Attribute node
                 * node in the sequence then we have to throw a serialization
                 * error.  NOTE: The rules say we also have to throw an error
                 * for Namespace nodes, but JAXP doesn't define a "Namespace"
                 * object per se; it just defines namespace prefixes and URIs
                 * on other Nodes.  So we just check for attributes.  If we
                 * find one then we take note of the fact that the result has
                 * a parentless attribute node and later, if the user calls
                 * XMLSERIALIZE on the received XMLDataValue we'll throw the
                 * error as required.  Note that we currently only get here
                 * for the XMLQUERY operator, which means we're serializing
                 * a result sequence returned from Xalan and we're going to
                 * store the serialized version into a Derby XML value.  In
                 * that case the serialization is an internal operation--and
                 * since the user didn't ask for it, we don't want to throw
                 * the serialization error here.  If we did, then whenever an
                 * XMLQUERY operation returned a result sequence with a top-
                 * level attribute in it, the user would see a serialization
                 * error. That's not correct since it is technically okay for
                 * the XMLQUERY operation to return a sequence with an attribute
                 * node; it's just not okay for a user to explicitly try to
                 * serialize that sequence. So instead of throwing the error
                 * here, we just take note of the fact that the sequence has
                 * a top-level attribute.  Then later, IF the user makes an
                 * explicit call to serialize the sequence, we'll throw the
                 * appropriate error (see XML.XMLSerialize()).
                 */
                if (xmlVal != null)
                    xmlVal.markAsHavingTopLevelAttr();
                serializer.transform(
                        new DOMSource((Node) obj), new StreamResult(sWriter));
                lastItemWasString = false;
            }
            else
            { // We have a Node, so try to serialize it.
                Node n = (Node)obj;
                if (n instanceof Text)
                {
                    /* Step 6: Combine adjacent text nodes into a single
                     * text node.  Since we're just going to serialize the
                     * Text node back into a string, we short-cut this step
                     * by skipping the creation of a new Text node and just
                     * writing the text value out directly to our serialized
                     * stream.  Step 6 also says that empty text nodes should
                     * be dropped--but if the text node is empty, the call
                     * to getNodeValue() will return an empty string and
                     * thus we've effectively "dropped" the text node from
                     * the serialized result.  Note: it'd be cleaner to just
                     * call "serialize()" on the Text node like we do for
                     * all other Nodes, but Xalan doesn't allow that.  So
                     * use the getNodeValue() method instead.
                     */
                    sWriter.write(n.getNodeValue());
                }
                else
                {
                    /* Steps 5 and 7b: Copy all non-attribute, non-text
                     * nodes to the "normalized sequence" and then serialize
                     * that normalized sequence.  We short-cut this by
                     * just letting Xalan do the serialization for every
                     * Node in the current list of items that wasn't
                     * "serialized" as an atomic value, attribute, or
                     * text node.
                     */
                    serializer.transform(
                            new DOMSource(n), new StreamResult(sWriter));
                }

                lastItemWasString = false;
            }
        }

        /* At this point sWriter holds the serialized version of the
         * normalized sequence that corresponds to the received list
         * of items.  So that's what we return.
         */
        sWriter.flush();
        return sWriter.toString();
    }

    /**
     * Evaluate this object's compiled XML query expression against
     * the received xmlContext.  Then if returnResults is false,
     * return an empty sequence (ArrayList) if evaluation yields
     * at least one item and return null if evaluation yields zero
     * items (the caller can then just check for null to see if the
     * query returned any items).  If returnResults is true, then return
     * return a sequence (ArrayList) containing all items returned
     * from evaluation of the expression.  This array list can contain
     * any combination of atomic values and XML nodes; it may also
     * be empty.
     *
     * Assumption here is that the query expression has already been
     * compiled and is stored in this.query.
     *
     * @param xmlContext The XML value against which to evaluate
     *  the stored (compiled) query expression
     * @param returnResults Whether or not to return the actual
     *  results of the query
     * @param resultXType The qualified XML type of the result
     *  of evaluating the expression, if returnResults is true.
     *  If the result is a sequence of exactly one Document node
     *  then this will be XML(DOCUMENT(ANY)); else it will be
     *  XML(SEQUENCE).  If returnResults is false, this value
     *  is ignored.
     * @return If returnResults is false then return an empty
     *  ArrayList if evaluation returned at least one item and return
     *  null otherwise.  If returnResults is true then return an
     *  array list containing all of the result items and return
     *  the qualified XML type via the resultXType parameter.
     * @exception Exception thrown on error (and turned into a
     *  StandardException by the caller).
     */
    protected List evalXQExpression(XMLDataValue xmlContext,
        boolean returnResults, int [] resultXType) throws Exception
    {
        // if this object is in an SPS, we need to recompile the query
        if (recompileQuery)
        {
        	compileXQExpr(queryExpr, opName);
        }

        // Make sure we have a compiled query.
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(
                (query != null),
                "Failed to locate compiled XML query expression.");
        }

        /* Create a DOM node from the xmlContext, since that's how
         * we feed the context to Xalan.  We do this by creating
         * a Document node using DocumentBuilder, which means that
         * the serialized form of the context node must be a string
         * value that is parse-able by DocumentBuilder--i.e. it must
         * constitute a valid XML document.  If that's true then
         * the context item's qualified type will be DOC_ANY.
         */
        if (xmlContext.getXType() != XML.XML_DOC_ANY)
        {
            throw StandardException.newException(
                SQLState.LANG_INVALID_XML_CONTEXT_ITEM,
                (returnResults ? "XMLQUERY" : "XMLEXISTS"));
        } 

        Document docNode = null;
        docNode = dBuilder.parse(
            new InputSource(
                new StringReader(xmlContext.getString())));

        // Evaluate the expresion using Xalan.
        XPathResult result = (XPathResult)
                query.evaluate(docNode, XPathResult.ANY_TYPE, null);

        if (!returnResults)
        {
            // We don't want to return the actual results, we just
            // want to know if there was at least one item in the
            // result sequence.
            switch (result.getResultType()) {
                case XPathResult.UNORDERED_NODE_ITERATOR_TYPE:
                case XPathResult.ORDERED_NODE_ITERATOR_TYPE:
                    if (result.iterateNext() == null) {
                        // We have an empty sequence, so return null.
                        return null;
                    } else {
                        // We have a non-empty sequence, so return a non-null
                        // list to indicate that we found at least one item.
                        return Collections.EMPTY_LIST;
                    }
                default:
                    // We have a single atomic value, which means the result is
                    // non-empty. So return a non-null list.
                    return Collections.EMPTY_LIST;
            }
        }

        // Else process the results.
        List itemRefs;
        switch (result.getResultType()) {
            case XPathResult.NUMBER_TYPE:
                // Single atomic number. Get its string value.
                String val = numberToString(result.getNumberValue());
                itemRefs = Collections.singletonList(val);
                break;
            case XPathResult.STRING_TYPE:
                // Single atomic string value.
                itemRefs = Collections.singletonList(result.getStringValue());
                break;
            case XPathResult.BOOLEAN_TYPE:
                // Single atomic boolean. Get its string value.
                itemRefs = Collections.singletonList(
                        String.valueOf(result.getBooleanValue()));
                break;
            case XPathResult.UNORDERED_NODE_ITERATOR_TYPE:
            case XPathResult.ORDERED_NODE_ITERATOR_TYPE:
                // We have a sequence. Get all nodes.
                itemRefs = new ArrayList();
                Node node;
                while ((node = result.iterateNext()) != null) {
                    itemRefs.add(node);
                }
                break;
            default:
                if (SanityManager.DEBUG) {
                    SanityManager.THROWASSERT(
                            "Don't know how to handle XPath result type " +
                            result.getResultType());
                }
                itemRefs = null;
        }

        /* Indicate what kind of XML result value we have.  If
         * we have a sequence of exactly one Document then it
         * is XMLPARSE-able and so we consider it to be of type
         * XML_DOC_ANY (which means we can store it in a Derby
         * XML column).
         */
        if ((itemRefs.size() == 1) && (itemRefs.get(0) instanceof Document))
            resultXType[0] = XML.XML_DOC_ANY;
        else
            resultXType[0] = XML.XML_SEQUENCE;

        return itemRefs;
    }

    /* ****
     * Helper classes and methods.
     * */

    /**
     * Create an instance of Xalan serializer for the sake of
     * serializing an XML value according the SQL/XML specification
     * for serialization.
     */
    private void loadSerializer() throws TransformerConfigurationException
    {
        // Set serialization properties.
        Properties props = new Properties();

        // SQL/XML[2006] 10.15:General Rules:6 says method is "xml".
        props.setProperty(OutputKeys.METHOD, "xml");

        /* Since the XMLSERIALIZE operator doesn't currently support
         * the DOCUMENT nor CONTENT keywords, SQL/XML spec says that
         * the default is CONTENT (6.7:Syntax Rules:2.a).  Further,
         * since the XMLSERIALIZE operator doesn't currently support the
         * <XML declaration option> syntax, the SQL/XML spec says
         * that the default for that option is "Unknown" (6.7:General
         * Rules:2.f).  Put those together and that in turn means that
         * the value of "OMIT XML DECLARATION" must be "Yes", as
         * stated in section 10.15:General Rules:8.c.  SO, that's what
         * we set here.
         *
         * NOTE: currently the only way to view the contents of an
         * XML column is by using an explicit XMLSERIALIZE operator.
         * This means that if an XML document is stored and it
         * begins with an XML declaration, the user will never be
         * able to _see_ that declaration after inserting the doc
         * because, as explained above, our current support for
         * XMLSERIALIZE dictates that the declaration must be
         * omitted.  Similarly, other transformations that may
         * occur from serialization (ex. entity replacement,
         * attribute order, single-to-double quotes, etc)) will
         * always be in effect for the string returned to the user;
         * the original form of the XML document, if different
         * from the serialized version, is not currently retrievable.
         */
        props.setProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");

        // We serialize everything as UTF-8 to match what we
        // store on disk.
        props.setProperty(OutputKeys.ENCODING, "UTF-8");

        // Load the serializer with the correct properties.
        serializer = TransformerFactory.newInstance().newTransformer();
        serializer.setOutputProperties(props);
        return;
    }

    /**
     * Convert a number returned by an XPath query to a string, following the
     * rules for the <a href="http://www.w3.org/TR/xpath/#function-string">
     * XPath string function</a>.
     *
     * @param d {@code double} representation of the number
     * @return {@code String} representation of the number
     */
    private static String numberToString(double d)
            throws IllegalAccessException, InvocationTargetException {
        if (Double.isNaN(d) || Double.isInfinite(d)) {
            // BigDecimal doesn't know how to handle NaN or +/- infinity, so
            // use Double to handle those cases.
            return Double.toString(d);
        } else {
            // Otherwise, use BigDecimal to format the number the way we want.
            // Ideally, we'd just return
            // BigDecimal.valueOf(d).stripTrailingZeros().toPlainString(),
            // but valueOf(double), stripTrailingZeros() and toPlainString()
            // were all introduced in Java 5, and we still need to support
            // older platforms.
            BigDecimal dec = new BigDecimal(Double.toString(d));

            // See how many trailing zeros we have after the decimal point.
            long unscaledValue = dec.unscaledValue().longValue();
            int scale = dec.scale();
            while (scale > 0 && unscaledValue % 10 == 0) {
                scale--;
                unscaledValue /= 10;
            }

            // If we have trailing zeros after the decimal point, remove them.
            if (scale != dec.scale()) {
                dec = BigDecimal.valueOf(unscaledValue, scale);
            }

            // Finally, convert the value to a string. The method
            // BigDecimal.toPlainString() formats the number the way we want
            // it, but it's only available on Java 5 and later. Luckily, on
            // older platforms, BigDecimal.toString() is defined the same way
            // as toPlainString(), so we can fall back to that method if
            // toPlainString() isn't available. toString() was redefined in
            // Java 5, so we cannot use toString() unconditionally, however.
            if (TO_PLAIN_STRING == null) {
                return dec.toString();
            } else {
                return (String) TO_PLAIN_STRING.invoke(dec, (Object[]) null);
            }
        }
    }

    /*
     ** The XMLErrorHandler class is just a generic implementation
     ** of the ErrorHandler interface.  It allows us to catch
     ** and process XML parsing errors in a graceful manner.
     */
    private class XMLErrorHandler implements ErrorHandler
    {
        public void error (SAXParseException exception)
            throws SAXException
        {
            throw new SAXException (exception);
        }

        public void fatalError (SAXParseException exception)
            throws SAXException
        {
            throw new SAXException (exception);
        }

        public void warning (SAXParseException exception)
            throws SAXException
        {
            throw new SAXException (exception);
        }
    }
}
