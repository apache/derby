/*

   Derby - Class org.apache.derby.iapi.types.XML

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

import org.apache.derby.iapi.services.cache.ClassSize;
import org.apache.derby.iapi.services.io.ArrayInputStream;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.StreamStorable;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.types.BooleanDataValue;

import org.apache.derby.iapi.reference.SQLState;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import java.io.InputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.StringReader;

import org.xml.sax.ErrorHandler;
import org.xml.sax.XMLReader;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.InputSource;

import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

import javax.xml.transform.Templates;
import javax.xml.transform.TransformerFactory;

import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.TemplatesHandler;
import javax.xml.transform.sax.TransformerHandler;

// Note that even though the following has a Xalan
// package name, it IS part of the JDK 1.4 API, and
// thus we can compile it without having Xalan in
// our classpath.
import org.apache.xalan.processor.TransformerFactoryImpl;

/**
 * This type implements the XMLDataValue interface and thus is
 * the type on which all XML related operations are executed.
 *
 * The first and simplest XML store implementation is a UTF-8
 * based one--all XML data is stored on disk as a UTF-8 string,
 * just like the other Derby string types.  In order to make
 * it possible for smarter XML implementations to exist in
 * the future, this class always writes an "XML implementation
 * id" to disk before writing the rest of its data.  When
 * reading the data, the impl id is read first and serves
 * as an indicator of how the rest of the data should be
 * read.
 *
 * So long as there's only one implementation (UTF-8)
 * the impl id can be ignored; but when smarter implementations
 * are written, the impl id will be the key to figuring out
 * how an XML value should be read, written, and processed.
 */
public class XML
    extends DataType implements XMLDataValue, StreamStorable
{
    // Id for this implementation.  Should be unique
    // across all XML type implementations.
    protected static final short UTF8_IMPL_ID = 0;

    // Parser class to use for parsing XML.  We use the
    // Xerces parser, so (for now) we require that Xerces
    // be in the user's classpath.  Note that we load
    // the Xerces class dynamically (using the class 
    // name) so that Derby will build even if Xerces
    // isn't in the build environment; i.e. Xerces is
    // only required if XML is actually going to be used
    // at runtime; it's not required for a successful
    // build nor for non-XML database use.
    protected static final String XML_PARSER_CLASS =
        "org.apache.xerces.parsers.SAXParser";

    // Guess at how much memory this type will take.
    private static final int BASE_MEMORY_USAGE =
        ClassSize.estimateBaseFromCatalog(XML.class);

    // The actual XML data in this implementation is just a simple
    // string, so this class really just wraps a SQLChar and
    // defers most calls to the corresponding calls on that
    // SQLChar.  Note that, even though a SQLChar is the
    // underlying implementation, an XML value is nonetheless
    // NOT considered comparable nor compatible with any of
    // Derby string types.
    private SQLChar xmlStringValue;

    // An XML reader for reading and parsing SAX events.
    protected XMLReader saxReader;

    // XSLT objects used when performing an XSLT query, which
    // is the query mechanism for this UTF8-based implementation.
    private static final String XPATH_PLACEHOLDER = "XPATH_PLACEHOLDER";
    private static final String QUERY_MATCH_STRING = "MATCH";
    private static String xsltStylesheet;
    private XMLReader xsltReader;
    private TransformerFactoryImpl saxTFactory;

    /**
     * Default constructor.
     */
    public XML()
    {
        xmlStringValue = null;
    }

    /**
     * Private constructor used for the getClone() method.
     * Takes a SQLChar and clones it.
     * @param val A SQLChar instance to clone and use for
     *  this XML value.
     */
    private XML(SQLChar val)
    {
        xmlStringValue = (val == null ? null : (SQLChar)val.getClone());
    }

    /* ****
     * DataValueDescriptor interface.
     * */

    /**
     * @see DataValueDescriptor#getClone
     */
    public DataValueDescriptor getClone()
    {
        return new XML(xmlStringValue);
    }

    /**
     * @see DataValueDescriptor#getNewNull
     */
    public DataValueDescriptor getNewNull()
    {
        return new XML();
    }

    /**
     * @see DataValueDescriptor#getTypeName
     */
    public String getTypeName()
    {
        return TypeId.XML_NAME;
    }

    /**
     * @see DataValueDescriptor#typePrecedence
     */
    public int typePrecedence()
    {
        return TypeId.XML_PRECEDENCE;
    }

    /**
     * @see DataValueDescriptor#getString
     */
    public String getString() throws StandardException
    {
        return (xmlStringValue == null) ? null : xmlStringValue.getString();
    }

    /**
     * @see DataValueDescriptor#getLength
     */
    public int    getLength() throws StandardException
    {
        return ((xmlStringValue == null) ? 0 : xmlStringValue.getLength());
    }

    /** 
     * @see DataValueDescriptor#estimateMemoryUsage
     */
    public int estimateMemoryUsage()
    {
        int sz = BASE_MEMORY_USAGE;
        if (xmlStringValue != null)
            sz += xmlStringValue.estimateMemoryUsage();
        return sz;
    }

    /**
     * @see DataValueDescriptor#readExternalFromArray
     */
    public void readExternalFromArray(ArrayInputStream in)
        throws IOException
    {
        if (xmlStringValue == null)
            xmlStringValue = new SQLChar();

        // Read the XML implementation id.  Right now there's
        // only one implementation (UTF-8 based), so we don't
        // use this value.  But if better implementations come
        // up in the future, we'll have to use this impl id to
        // figure out how to read the data.
        in.readShort();

        // Now just read the XML data as UTF-8.
        xmlStringValue.readExternalFromArray(in);
    }

    /**
     * @see DataValueDescriptor#setFrom
     */
    protected void setFrom(DataValueDescriptor theValue)
        throws StandardException
    {
        if (xmlStringValue == null)
            xmlStringValue = new SQLChar();
        xmlStringValue.setValue(theValue.getString());
    }

    /** 
     * @see DataValueDescriptor#setValueFromResultSet 
     */
    public final void setValueFromResultSet(
        ResultSet resultSet, int colNumber, boolean isNullable)
        throws SQLException
    {
        if (xmlStringValue == null)
            xmlStringValue = new SQLChar();
        xmlStringValue.setValue(resultSet.getString(colNumber));
    }

    /**
     * Compare two XML DataValueDescriptors.  NOTE: This method
     * should only be used by the database store for the purpose of
     * index positioning--comparisons of XML type are not allowed
     * from the language side of things.  That said, all store
     * wants to do is order the NULLs, so we don't actually
     * have to do a full comparison.  Just return an order
     * value based on whether or not this XML value and the
     * other XML value are null.  As mentioned in the "compare"
     * method of DataValueDescriptor, nulls are considered
     * equal to other nulls and less than all other values.
     *
     * An example of when this method might be used is if the
     * user executed a query like:
     *
     * select i from x_table where x_col is not null
     *
     * @see DataValueDescriptor#compare
     */
    public int compare(DataValueDescriptor other)
        throws StandardException
    {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(other instanceof XMLDataValue,
                "Store should NOT have tried to compare an XML value " +
                "with a non-XML value.");
        }

        if (isNull()) {
            if (other.isNull())
            // both null, so call them 'equal'.
                return 0;
            // This XML is 'less than' the other.
            return -1;
        }

        if (other.isNull())
        // This XML is 'greater than' the other.
            return 1;

        // Two non-null values: we shouldn't ever get here,
        // since that would necessitate a comparsion of XML
        // values, which isn't allowed.
        if (SanityManager.DEBUG) {
            SanityManager.THROWASSERT(
                "Store tried to compare two non-null XML values, " +
                "which isn't allowed.");
        }
        return 0;
    }

    /* ****
     * Storable interface, implies Externalizable, TypedFormat
     */

    /**
     * @see TypedFormat#getTypeFormatId
     *
     * From the engine's perspective, all XML implementations share
     * the same format id.
     */
    public int getTypeFormatId() {
        return StoredFormatIds.XML_ID;
    }

    /**
     * @see Storable#isNull
     */
    public boolean isNull()
    {
        return ((xmlStringValue == null) || xmlStringValue.isNull());
    }

    /**
     * @see Storable#restoreToNull
     */
    public void restoreToNull()
    {
        if (xmlStringValue != null)
            xmlStringValue.restoreToNull();
    }

    /**
     * Read an XML value from an input stream.
     * @param in The stream from which we're reading.
     */
    public void readExternal(ObjectInput in) throws IOException
    {
        if (xmlStringValue == null)
            xmlStringValue = new SQLChar();

        // Read the XML implementation id.  Right now there's
        // only one implementation (UTF-8 based), so we don't
        // use this value.  But if better implementations come
        // up in the future, we'll have to use this impl id to
        // figure out how to read the data.
        in.readShort();

        // Now just read the XML data as UTF-8.
        xmlStringValue.readExternal(in);
    }

    /**
     * Write an XML value. 
     * @param out The stream to which we're writing.
     */
    public void writeExternal(ObjectOutput out) throws IOException
    {
        // never called when value is null
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(!isNull());

        // Write out the XML store impl id.
        out.writeShort(UTF8_IMPL_ID);

        // Now write out the data.
        xmlStringValue.writeExternal(out);
    }

    /* ****
     * StreamStorable interface
     * */

    /**
     * @see StreamStorable#returnStream
     */
    public InputStream returnStream()
    {
        return
            (xmlStringValue == null) ? null : xmlStringValue.returnStream();
    }

    /**
     * @see StreamStorable#setStream
     */
    public void setStream(InputStream newStream)
    {
        if (xmlStringValue == null)
            xmlStringValue = new SQLChar();

        // The stream that we receive is for an XML data value,
        // which means it has an XML implementation id stored
        // at the front (we put it there when we wrote it out).
        // If we leave that there we'll get a failure when
        // our underlying SQLChar tries to read from the
        // stream, because the extra impl id will throw
        // off the UTF format.  So we need to read in (and
        // ignore) the impl id before using the stream.
        try {
            // 2 bytes equal a short, which is what an impl id is.
            newStream.read();
            newStream.read();
        } catch (Exception e) {
            if (SanityManager.DEBUG)
                SanityManager.THROWASSERT("Failed to read impl id" +
                    "bytes in setStream.");
        }

        // Now go ahead and use the stream.
        xmlStringValue.setStream(newStream);
    }

    /**
     * @see StreamStorable#loadStream
     */
    public void loadStream() throws StandardException
    {
        getString();
    }

    /* ****
     * XMLDataValue interface.
     * */

    /**
     * Method to parse an XML string and, if it's valid,
     * store the _parsed_ version for subsequent use.
     * @param text The string value to check.
     * @param preserveWS Whether or not to preserve
     *  ignorable whitespace.
     * @return  If 'text' constitutes a valid XML document,
     *  it has been stored in this XML value and nothing
     *  is returned; otherwise, an exception is thrown.
     * @exception StandardException Thrown on parse error.
     */
    public void parseAndLoadXML(String text, boolean preserveWS)
        throws StandardException
    {
        try {

            if (preserveWS) {
            // We're just going to use the text exactly as it
            // is, so we just need to see if it parses. 
                loadSAXReader();
                saxReader.parse(
                    new InputSource(new StringReader(text)));
            }
            else {
            // We don't support this yet, so we shouldn't
            // get here.
                if (SanityManager.DEBUG)
                    SanityManager.THROWASSERT("Tried to STRIP whitespace " +
                        "but we shouldn't have made it this far");
            }

        } catch (Exception xe) {
        // The text isn't a valid XML document.  Throw a StandardException
        // with the parse exception nested in it.
            throw StandardException.newException(
                SQLState.LANG_NOT_AN_XML_DOCUMENT, xe);
        }

        // If we get here, the text is valid XML so go ahead
        // and load/store it.
        if (xmlStringValue == null)
            xmlStringValue = new SQLChar();
        xmlStringValue.setValue(text);
        return;
    }

    /**
     * The SQL/XML XMLSerialize operator.
     * Converts this XML value into a string with a user-specified
     * type, and returns that string via the received StringDataValue
     * (if the received StringDataValue is non-null; else a new
     * StringDataValue is returned).
     * @param result The result of a previous call to this method,
     *    null if not called yet.
     * @param targetType The string type to which we want to serialize.
     * @param targetWidth The width of the target type.
     * @return A serialized (to string) version of this XML object,
     *  in the form of a StringDataValue object.
     * @exception StandardException    Thrown on error
     */
    public StringDataValue XMLSerialize(StringDataValue result,
        int targetType, int targetWidth) throws StandardException
    {
        if (result == null) {
            switch (targetType)
            {
                case Types.CHAR:        result = new SQLChar(); break;
                case Types.VARCHAR:     result = new SQLVarchar(); break;
                case Types.LONGVARCHAR: result = new SQLLongvarchar(); break;
                case Types.CLOB:        result = new SQLClob(); break;
                default:
                // Shouldn't ever get here, as this check was performed
                // at bind time.

                    if (SanityManager.DEBUG) {
                        SanityManager.THROWASSERT(
                            "Should NOT have made it to XMLSerialize " +
                            "with a non-string target type.");
                    }
                    return null;
            }
        }

        // Else we're reusing a StringDataValue.  We only reuse
        // the result if we're executing the _same_ XMLSERIALIZE
        // call on multiple rows.  That means that all rows
        // must have the same result type (targetType) and thus
        // we know that the StringDataValue already has the
        // correct type.  So we're set.

        if (this.isNull()) {
        // Attempts to serialize a null XML value lead to a null
        // result (SQL/XML[2003] section 10.13).
            result.setToNull();
            return result;
        }

        // Get the XML value as a string.  For this UTF-8 impl,
        // we already have it as a string, so just use that.
        result.setValue(xmlStringValue.getString());

        // Seems wrong to trunc an XML document, as it then becomes non-
        // well-formed and thus useless.  So we throw an error (that's
        // what the "true" in the next line says).
        result.setWidth(targetWidth, 0, true);
        return result;
    }

    /**
     * The SQL/XML XMLExists operator.
     * Takes an XML query expression (as a string) and an XML
     * value and checks if at least one node in the XML
     * value matches the query expression.  NOTE: For now,
     * the query expression must be XPath only (XQuery not
     * supported).
     * @param xExpr The query expression, as a string.
     * @param xml The XML value being queried.
     * @return True if the received query expression matches at
     *  least one node in the received XML value; unknown if
     *  either the query expression or the xml value is null;
     *  false otherwise.
     * @exception StandardException Thrown on error
     */
    public BooleanDataValue XMLExists(StringDataValue xExpr,
        XMLDataValue xml) throws StandardException
    {
        if ((xExpr == null) || xExpr.isNull())
        // If the query is null, we assume unknown.
            return SQLBoolean.unknownTruthValue();

        if ((xml == null) || xml.isNull())
        // Then per SQL/XML spec 8.4, we return UNKNOWN.
            return SQLBoolean.unknownTruthValue();

        return new SQLBoolean(xml.exists(xExpr.getString()));
    }

    /**
     * Helper method for XMLExists.
     * See if the received XPath expression returns at least
     * one node when evaluated against _this_ XML value.
     * @param xExpr The XPath expression.
     * @return True if at least one node in this XML value
     *  matches the received xExpr; false otherwise.
     */
    public boolean exists(String xExpr) throws StandardException
    {
        // NOTE: At some point we'll probably need to implement some
        // some kind of query cache so that we don't have to recompile
        // the same query over and over for every single XML row
        // in a table.  That's what we do right now...

        try {

            xExpr = replaceDoubleQuotes(xExpr);
            loadXSLTObjects();

            // Take our simple stylesheet and plug in the query.
            int pos = xsltStylesheet.indexOf(XPATH_PLACEHOLDER);
            StringBuffer stylesheet = new StringBuffer(xsltStylesheet);
            stylesheet.replace(pos, pos + XPATH_PLACEHOLDER.length(), xExpr);

            // Create a Templates ContentHandler to handle parsing of the 
            // stylesheet.
            TemplatesHandler templatesHandler = 
                saxTFactory.newTemplatesHandler();
            xsltReader.setContentHandler(templatesHandler);
    
            // Now parse the generic stylesheet we created.
            xsltReader.parse(
                new InputSource(new StringReader(stylesheet.toString())));

            // Get the Templates object (generated during the parsing of
            // the stylesheet) from the TemplatesHandler.
            Templates compiledQuery = templatesHandler.getTemplates();

            // Create a Transformer ContentHandler to handle parsing of 
            // the XML Source.  
            TransformerHandler transformerHandler 
                = saxTFactory.newTransformerHandler(compiledQuery);

            // Reset the XMLReader's ContentHandler to the TransformerHandler.
            xsltReader.setContentHandler(transformerHandler);

            // Create an ExistsHandler.  When the XSLT transformation
            // occurs, a period (".") will be thrown to this handler
            // (via a SAX 'characters' event) for every matching
            // node that XSLT finds.  This is how we know if a
            // match was found.
            ExistsHandler eH = new ExistsHandler();
            transformerHandler.setResult(new SAXResult(eH));

            // This call to "parse" is what does the query, because we
            // passed in an XSLT handler with the compiled query above.
            try {
                xsltReader.parse(
                    new InputSource(new StringReader(getString())));
            } catch (Throwable th) {
                if (th.getMessage().indexOf(
                    "SAXException: " + QUERY_MATCH_STRING) == -1)
                { // then this isn't the exception that means we have
                  // a match; so re-throw it.
                    throw new Exception(th.getMessage());
                }
            }

            // Did we have any matches?
            return eH.exists();

        } catch (Exception xe) {
        // We don't expect to get here.  Turn it into a
        // StandardException, then throw it.
            throw StandardException.newException(
                SQLState.LANG_UNEXPECTED_XML_EXCEPTION, xe);
        }
    }

    /* ****
     * Helper classes and methods.
     * */

    /**
     * Load an XMLReader for SAX events that can be used
     * for parsing XML data.
     *
     * This method is currently only used for XMLPARSE, and
     * the SQL/XML[2003] spec says that XMLPARSE should NOT
     * perform validation -- Seciont 6.11:
     *
     *    "Perform a non-validating parse of a character string to
     *    produce an XML value."
     *
     * Thus, we make sure to disable validation on the XMLReader
     * loaded here.  At some point in the future we will probably
     * want to add support for the XMLVALIDATE function--but until
     * then, user is unable to validate the XML values s/he inserts.
     *
     * Note that, even with validation turned off, XMLPARSE
     * _will_ still check the well-formedness of the values,
     * and it _will_ still process DTDs to get default values,
     * etc--but that's it; no validation errors will be thrown.
     *
     * For future reference: the features needed to perform
     * validation (with Xerces) are:
     *
     * http://apache.org/xml/features/validation/schema
     * http://apache.org/xml/features/validation/dynamic
     */
    protected void loadSAXReader() throws Exception
    {
        if (saxReader != null)
        // already loaded.
            return;

        // Get an instance of an XMLReader.
        saxReader = XMLReaderFactory.createXMLReader(XML_PARSER_CLASS);

        // Turn off validation, since it's not allowed by
        // SQL/XML[2003] spec.
        saxReader.setFeature(
            "http://xml.org/sax/features/validation", false);

        // Make the parser namespace aware.
        saxReader.setFeature(
            "http://xml.org/sax/features/namespaces", true);

        // We have to set the error handler in order to properly
        // receive the parse errors.
        saxReader.setErrorHandler(new XMLErrorHandler());
    }

    /**
     * Prepare for an XSLT query by loading the objects
     * required for such a query.  We should only have
     * to do this once per XML object.
     */
    private void loadXSLTObjects() throws SAXException
    {
        if (xsltReader != null)
        // we already loaded everything.
            return;

        // Instantiate a TransformerFactory.
        TransformerFactory tFactory = TransformerFactory.newInstance();

        // Cast the TransformerFactory to SAXTransformerFactory.
        saxTFactory = (TransformerFactoryImpl)tFactory;

        // Get an XML reader.
        xsltReader = XMLReaderFactory.createXMLReader(XML_PARSER_CLASS);

        // Make the parser namespace aware.  Note that because we
        // only support a small subset of SQL/XML, and because we
        // only allow XPath (as opposed to XQuery) expressions,
        // there is no way for a user to specify namespace
        // bindings as part of the XMLEXISTS operator.  This means
        // that in order to query for a node name, the user must
        // use the XPath functions "name()" and "local-name()"
        // in conjunction with XPath 1.0 'namespace' axis.  For
        // example:
        //
        // To see if any elements exist that have a specific name
        // with ANY namespace:
        //     //child::*[local-name()="someName"]
        //
        // To see if any elements exist that have a specific name
        // with NO namespace:
        //     //child::*[name()="someName"]
        //
        // To see if any elements exist that have a specific name
        // in a specific namespace:
        //     //child::*[local-name()=''someName'' and
        //        namespace::*[string()=''http://www.some.namespace'']]
        //
        xsltReader.setFeature(
            "http://xml.org/sax/features/namespaces", true);

        // Create a very simple XSLT stylesheet.  This stylesheet
        // will execute the XPath expression and, for every match,
        // write a period (".") to the ExistsHandler (see the exists()
        // method above).  Then, in order to see if at least one
        // node matches, we just check to see if the ExistsHandler
        // caught at least one 'characters' event.  If it did, then
        // we know we had a match.
        if (xsltStylesheet == null) {
            StringBuffer sb = new StringBuffer();
            sb.append("<xsl:stylesheet version=\"1.0\"\n");
            sb.append("xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\">\n");
            sb.append(" <xsl:template match=\"/\">\n"); // Search whole doc...
            sb.append("  <xsl:for-each select=\"");     // For every match...
            sb.append(XPATH_PLACEHOLDER);               // using XPath expr...
            sb.append("\">.</xsl:for-each>\n");         // Write a "."
            sb.append(" </xsl:template>\n");
            sb.append("</xsl:stylesheet>\n");
            xsltStylesheet = sb.toString();
        }
    }

    /**
     * Takes a string (which is an XPath query specified by
     * the user) and replaces any double quotes with single
     * quotes.  We have to do this because a double quote
     * in the XSLT stylesheet (which is where the user's
     * query ends up) will be parsed as a query terminator
     * thus will cause XSLT execution errors.
     * @param queryText Text in which we want to replace double
     *  quotes.
     * @return queryText with all double quotes replaced by
     *  single quotes.
     */
    private String replaceDoubleQuotes(String queryText)
    {
        int pos = queryText.indexOf("\"");
        if (pos == -1)
        // nothing to do.
            return queryText;

        StringBuffer sBuf = new StringBuffer(queryText);
        while (pos >= 0) {
            sBuf.replace(pos, pos+1, "'");
            pos = queryText.indexOf("\"", pos+1);
        }
        return sBuf.toString();
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

    /*
     ** The ExistsHandler is what we pass to the XSLT processor
     ** when we query.  The generic xsltStylesheet that we defined
     ** above will throw a 'characters' event for every matching
     ** node that is found by the XSLT transformation.  This
     ** handler is the one that catches the event, and thus
     ** it tells us whether or not we had a match.
     */
    private class ExistsHandler extends DefaultHandler
    {
        // Did we catch at least one 'characters' event?
        private boolean atLeastOneMatch;

        public ExistsHandler() {
            atLeastOneMatch = false;
        }

        /*
         * Catch a SAX 'characters' event, which tells us that
         * we had at least one matching node.
         */
        public void characters(char[] ch, int start, int length)
            throws SAXException
        {
            // If we get here, we had at least one matching node.
            // Since that's all we need to know, we don't have
            // to continue querying--we can stop the XSLT
            // transformation now by throwing a SAX exception.
            atLeastOneMatch = true;
            throw new SAXException(QUERY_MATCH_STRING);
        }

        /*
         * Tell whether or not this handler caught a match.
         */
        public boolean exists()
        {
            return atLeastOneMatch;
        }
    }
}
