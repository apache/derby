/*

Derby - Class org.apache.derby.vti.XmlVTI

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

package org.apache.derby.vti;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.XMLConstants;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

/**
 * <p>
 * This is a VTI designed to read XML files which are structured like row sets.
 * <p>
 * XML files parsed by this VTI are always processed with external entity
 * expansion disabled and secure parser processing enabled.
 * <p>
 * There are two invocation formats provided by this VTI.
 * <p>
 * One form of this VTI takes the following arguments. This form is useful when
 * all of the columns in the row can be constructed from data nested INSIDE the row Element.
 * </p>
 *
 * <ul>
 * <li>xmlResourceName - The name of an xml file.</li>
 * <li>rowTag - The tag of the element which contains the row-structured content.</li>
 * <li>childTags - The attributes and descendant elements inside the row element which should be treated as columns.</li>
 * </ul>
 *
 * <p>
 * Here is a sample declaration of this first form of the XmlVTI:
 * </p>
 *
 * <pre>
 * create function findbugs( xmlResourceName varchar( 32672 ), rowTag varchar( 32672 ), childTags varchar( 32672 )... )
 * returns table
 * (
 *      className   varchar( 32672 ),
 *      bugCount    int
 * )
 * language java parameter style derby_jdbc_result_set no sql
 * external name 'org.apache.derby.vti.XmlVTI.xmlVTI';
 * </pre>
 *
 * <p>
 * ...and here is a sample invocation:
 * </p>
 *
 * <pre>
 * create view findbugs as
 * select *
 * from table
 * (
 *      findbugs
 *      (
 *          'findbugs.xml',
 *          'ClassStats',
 *          'class', 'bugs'
 *      )
 *  ) v;
 * 
 * select * from findbugs where bugCount != 0;
 * </pre>
 *
 * <p>
 * A second form of this VTI takes the following arguments. This form is useful when
 * some of the columns in the row are "inherited" from outer elements inside which the
 * row element nests:
 * </p>
 *
 * <ul>
 * <li>xmlResourceName - The name of an xml file.</li>
 * <li>rowTag - The tag of the element which contains the row-structured content.</li>
 * <li>parentTags - Attributes and elements (to be treated as columns) from outer elements in which the rowTag is nested.</li>
 * <li>childTags - Attributes and elements (to be treated as columns) inside the row element.</li>
 * </ul>
 *
 *
 * <p>
 * Here is a sample declaration of this second form of the XmlVTI. Using the second form
 * involves declaring an ArrayList type and a factory method too:
 * </p>
 *
 * <pre>
 * create type ArrayList external name 'java.util.ArrayList' language java;
 * 
 * create function asList( cell varchar( 32672 ) ... ) returns ArrayList
 * language java parameter style derby no sql
 * external name 'org.apache.derby.vti.XmlVTI.asList';
 * 
 * create function optTrace
 * (
 *     xmlResourceName varchar( 32672 ),
 *     rowTag varchar( 32672 ),
 *     parentTags ArrayList,
 *     childTags ArrayList
 * )
 * returns table
 * (
 *     stmtID    int,
 *     queryID   int,
 *     complete  boolean,
 *     summary   varchar( 32672 ),
 *     type        varchar( 50 ),
 *     estimatedCost        double,
 *     estimatedRowCount    int
 * )
 * language java parameter style derby_jdbc_result_set no sql
 * external name 'org.apache.derby.vti.XmlVTI.xmlVTI';
 * 
 * create view optTrace as
 *        select *
 *        from table
 *        (
 *             optTrace
 *             (
 *                 '/Users/me/derby/mainline/z.xml',
 *                 'planCost',
 *                 asList( 'stmtID', 'queryID', 'complete' ),
 *                 asList( 'summary', 'type', 'estimatedCost', 'estimatedRowCount' )
 *             )
 *         ) v
 * ;
 * 
 * select * from optTrace
 * where stmtID = 6 and complete
 * order by estimatedCost;
 * </pre>
 */
public  class   XmlVTI  extends StringColumnVTI
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private String      _rowTag;
    private InputStream _xmlResource;

    private int             _rowIdx = -1;
    private int             _rowCount = -1;
    private String[]    _currentRow;
    
    private DocumentBuilder _builder;
    private NodeList    _rawRows;

    //
    // The first n column names are attribute/element tags from parent
    // elements. The trailing column names are attribute/element tags from
    // the row element or its children.
    //
    private int     _firstChildTagIdx;  // first attribute/element to be found in the row or below

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Build an XmlVTI.
     * </p>
     *
     * @param   xmlResource The xml source as an InputStream.
     * @param   rowTag  The tag of the master row Element.
     * @param   firstChildTagIdx    The first (0-based) tag from columnTags which is a child tag.
     * @param   columnTags  The tags which supply column data; all of the tag positions less than firstChildTagIdx come from Elements which are outer to the rowTag element; the remaining tags, starting at firstChildTagIdx, are tags of attributes or Elements inside the rowTag Element.
     */
    public  XmlVTI( InputStream xmlResource, String rowTag, int firstChildTagIdx, String... columnTags )
    {
        super( columnTags );

        _xmlResource = xmlResource;
        _rowTag = rowTag;
        _firstChildTagIdx = firstChildTagIdx;
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ENTRY POINTS (SQL FUNCTIONS)
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * This is the static method for creating functions from a file name and child tags
     *
     * @param fileName The file containing the XML text
     * @param rowTag The tag on the outermost element defining the row
     * @param childTags The nested attributes and elements corresponding to columns in the row
     *
     * @return a VTI to turn the XML text into rows
     * @throws Exception on error
     */
    public  static  XmlVTI  xmlVTI( String fileName, String rowTag, String... childTags )
        throws Exception
    {
        return xmlVTI( fileName, rowTag, null, asList( childTags ) );
    }
    
    /**
     * This is the static method for creating functions from an url and child tags
     *
     * @param urlString An URL which locates the XML text
     * @param rowTag The tag on the outermost element defining the row
     * @param childTags The nested attributes and elements corresponding to columns in the row
     *
     * @return a VTI to turn the XML text into rows
     * @throws Exception on error
     */
    public  static  XmlVTI  xmlVTIFromURL( String urlString, String rowTag, String... childTags )
        throws Exception
    {
        return xmlVTIFromURL( urlString, rowTag, null, asList( childTags ) );
    }
    
    /**
     * This is the static method for creating functions from a file name and both parent and child tags
     *
     * @param fileName Name of file which holds the XML text
     * @param rowTag The base element for the row
     * @param parentTags The names of parent tags
     * @param childTags The names of child tags
     *
     * @return a VTI to turn the XML text into rows
     * @throws Exception on error
     */
    public  static  XmlVTI  xmlVTI
        ( final String fileName, String rowTag, ArrayList<String> parentTags, ArrayList<String> childTags )
        throws Exception
    {
        FileInputStream fis = AccessController.doPrivileged
            (
             new PrivilegedAction<FileInputStream>()
             {
                 public FileInputStream run()
                 {
                     try {
                         return new FileInputStream( new File( fileName ) );
                     }
                     catch (IOException ioe) { throw new IllegalArgumentException( ioe.getMessage(), ioe ); }
                 }  
             }
           );
        return xmlVTI( fis, rowTag, parentTags, childTags );
    }

    /**
     * This is the static method for creating functions from an URL and both parent and child tags
     *
     * @param urlString An URL which locates the XML text
     * @param rowTag The base element for the row
     * @param parentTags The names of parent tags
     * @param childTags The names of child tags
     *
     * @return a VTI to turn the XML text into rows
     * @throws Exception on error
     */
    public  static  XmlVTI  xmlVTIFromURL
        ( final String urlString, String rowTag, ArrayList<String> parentTags, ArrayList<String> childTags )
        throws Exception
    {
        InputStream is = AccessController.doPrivileged
            (
             new PrivilegedAction<InputStream>()
             {
                 public InputStream run()
                 {
                     try {
                         return (new URL( urlString )).openStream();
                     }
                     catch (IOException ioe) { throw new IllegalArgumentException( ioe.getMessage(), ioe ); }
                 }  
             }
           );
        return xmlVTI( is, rowTag, parentTags, childTags );
    }

    /**
     * This is the static method for creating functions from an URL and both parent and child tags
     *
     * @param xmlResource The XML text as a stream
     * @param rowTag The base element for the row
     * @param parentTags The names of parent tags
     * @param childTags The names of child tags
     *
     * @return a VTI to turn the XML text into rows
     * @throws Exception on error
     */
    private  static  XmlVTI  xmlVTI
        ( InputStream xmlResource, String rowTag, ArrayList<String> parentTags, ArrayList<String> childTags )
        throws Exception
    {
        if ( parentTags == null ) { parentTags = new ArrayList<String>(); }
        if ( childTags == null ) { childTags = new ArrayList<String>(); }
        
        String[]    allTags = new String[ parentTags.size() + childTags.size() ];
        int     idx = 0;
        for ( String tag : parentTags ) { allTags[ idx++ ] = tag; }
        for ( String tag : childTags ) { allTags[ idx++ ] = tag; }
        
        return new XmlVTI( xmlResource, rowTag, parentTags.size(), allTags );
    }

    /**
     * Factory method to create an ArrayList&lt;String&gt;
     *
     * @param cells The items to put on the list
     *
     * @return a list containing those items
     */
    public  static  ArrayList<String>   asList( String... cells )
    {
        ArrayList<String>   retval = new ArrayList<String>();
        for ( String cell : cells ) { retval.add( cell ); }
        
        return retval;
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // StringColumnVTI BEHAVIOR TO BE IMPLEMENTED BY SUBCLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Get the string value of the column in the current row identified by the 1-based columnNumber.
     * </p>
     *
     * @param columnNumber The 1-based column index in the row
     *
     * @return the column value as a string
     * @throws SQLException on error
     */
    protected  String  getRawColumn( int columnNumber ) throws SQLException
    {
        try {
            return  _currentRow[ columnNumber - 1 ];
        } catch (Throwable t) { throw new SQLException( t.getMessage() ); }
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ResultSet BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  void close() throws SQLException
    {
        _builder = null;
        _rawRows = null;
    }

    public  ResultSetMetaData   getMetaData() throws SQLException
    {
        throw new SQLException( "Not implemented." );
    }
    
    public  boolean next() throws SQLException
    {
        try {
            if ( _rowIdx < 0 ) { readRows(); }

            if ( ++_rowIdx < _rowCount )
            {
                parseRow( _rowIdx );
                return true;
            }
            else { return false; }
        } catch (Throwable t)
        {
            throw new SQLException( t.getMessage(), t );
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    //////////////////////////
    //
    // XML MINIONS
    //
    //////////////////////////

    /**
     * <p>
     * Fault in the list of rows.
     * </p>
     */
    private    void    readRows() throws Exception
    {
        DocumentBuilderFactory  factory = DocumentBuilderFactory.newInstance();
        
	    factory.setFeature( XMLConstants.FEATURE_SECURE_PROCESSING, true );
	    factory.setFeature(
            "http://xml.org/sax/features/external-general-entities", false );

        _builder = factory.newDocumentBuilder();
        _builder.setErrorHandler(new XMLErrorHandler());

        Document        doc = _builder.parse( _xmlResource );
        Element             root = doc.getDocumentElement();
                         
        _rawRows = root.getElementsByTagName( _rowTag );
        _rowCount = _rawRows.getLength();
                         
        _xmlResource.close();
    }
    
    /**
     * <p>
     * Parse a row into columns.
     * </p>
     */
    private    void    parseRow( int rowNumber ) throws Exception
    {
        Element         rawRow = (Element) _rawRows.item( rowNumber );
        int                 columnCount = getColumnCount();
        
        _currentRow = new String[ columnCount ];

        for ( int i = 0; i < columnCount; i++ )
        {
            _currentRow[ i ] = findColumnValue( rawRow, i );
        }
    }

    /**
     * <p>
     * Find the value of a column inside an element. The columnNumber is 0-based.
     * </p>
     */
    private String  findColumnValue( Element rawRow, int columnNumber )
        throws Exception
    {
        // handle tags which are supposed to come from outer elements
        boolean     inParent = (columnNumber < _firstChildTagIdx);
        if ( inParent )
        {
            Node    parent = rawRow.getParentNode();
            if ( (parent == null ) || !( parent instanceof Element) ) { return null; }
            else { rawRow = (Element) parent; }
        }
        
        // first look for an attribute by the column name
        String      columnName = getColumnName( columnNumber + 1 );
        String      contents = rawRow.getAttribute( columnName );

        // missing attributes turn up as empty strings. make them null instead
        if ( "".equals( contents ) ) { contents = null; }

        // if there is not attribute by that name, then look for descendent elements by
        // that name. concatenate them all.
        if ( contents == null )
        {
            NodeList    children = rawRow.getElementsByTagName( columnName );

            if ( (children != null) && (children.getLength() > 0) )
            {
                int                 childCount = children.getLength();
                StringBuilder    buffer = new StringBuilder();

                for ( int j = 0; j < childCount; j++ )
                {
                    Element     child = (Element) children.item( j );
                    // separate values with spaces.
                    if (j != 0){ buffer.append(" "); }
                    buffer.append( squeezeText( child ) );
                }
                contents = buffer.toString();
            }
        }

        // recurse if looking in parent element
        if ( inParent && (contents == null) ) { return findColumnValue( rawRow, columnNumber ); }
        else { return contents; }
    }
    
    /**
     * <p>
     * Squeeze the text out of an Element.
     * </p>
     */
    private String squeezeText( Element node )
        throws Exception
    {
        String      text = null;
        Node        textChild = node.getFirstChild();

        if ( textChild != null ) { text = textChild.getNodeValue(); }

        return text;
    }


    /*
     ** The XMLErrorHandler class is just a generic implementation
     ** of the ErrorHandler interface.  It allows us to catch
     ** and process XML parsing errors in a graceful manner.
     */
    private class XMLErrorHandler implements ErrorHandler
    {
        private void closeInput()
        {
            try
            {
                if( _xmlResource != null )
                    _xmlResource.close();
            }
            catch (Exception ex)
            {
            }
        }

        public void error (SAXParseException exception)
            throws SAXException
        {
            closeInput();
            throw new SAXException (exception);
        }

        public void fatalError (SAXParseException exception)
            throws SAXException
        {
            closeInput();
            throw new SAXException (exception);
        }

        public void warning (SAXParseException exception)
            throws SAXException
        {
            closeInput();
            throw new SAXException (exception);
        }
    }

}

