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

import java.io.*;
import java.lang.reflect.*;
import java.net.URL;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import javax.xml.parsers.*;
import org.w3c.dom.*;

/**
 * <p>
 * This is a VTI designed to read XML files which are structured like row sets.
 * This VTI takes the following arguments:
 * </p>
 *
 * <ul>
 * <li>xmlResourceName - An URL identifying an xml resource.</li>
 * <li>rowTag - The tag of the element which contains the row-structured content.</li>
 * <li>childTags - The attributes and descendant elements inside the row element which should be treated as columns.</li>
 * </ul>
 *
 * <p>
 * Here is a sample declaration:
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
 *          'file:///Users/me/static-analysis/findbugs.xml',
 *          'ClassStats',
 *          'class', 'bugs'
 *      )
 *  ) v;
 * 
 * select * from findbugs where bugCount != 0;
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
    private String      _xmlResourceName;

    private int             _rowIdx = -1;
    private int             _rowCount = -1;
    private String[]    _currentRow;
    
    private DocumentBuilder _builder;
    private NodeList    _rawRows;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Build a XmlVTI given the name of an xml resource, the  tag of the row
     * element, and an array of attribute-names/element-tags underneath the row element
     * </p>
     */
    public  XmlVTI( String xmlResourceName, String rowTag, String... childTags )
    {
        super( childTags );

        _xmlResourceName = xmlResourceName;
        _rowTag = rowTag;
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ENTRY POINT (SQL FUNCTION)
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** This is the static method bound to the function */
    public  static  XmlVTI  xmlVTI( String xmlResourceName, String rowTag, String... childTags )
    {
        return new XmlVTI( xmlResourceName, rowTag, childTags );
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
            t.printStackTrace( System.out );
            throw new SQLException( t.getMessage() );
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
        
        _builder = factory.newDocumentBuilder();

        URL                 url = new URL( _xmlResourceName );
        InputStream     is = url.openStream();
        Document        doc = _builder.parse( is );
        Element             root = doc.getDocumentElement();

        _rawRows = root.getElementsByTagName( _rowTag );
        _rowCount = _rawRows.getLength();

        is.close();
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
            // first look for an attribute by the column name
            String      columnName = getColumnName( i + 1 );
            String      contents = rawRow.getAttribute( columnName );

            // if there is not attribute by that name, then look for descendent elements by
            // that name. concatenate them all.
            if ( (contents == null) ||  "".equals( contents ) )
            {
                NodeList    children = rawRow.getElementsByTagName( columnName );

                if ( (children != null) && (children.getLength() > 0) )
                {
                    int                 childCount = children.getLength();
                    StringBuffer    buffer = new StringBuffer();

                    for ( int j = 0; j < childCount; j++ )
                    {
                        Element     child = (Element) children.item( j );
                        // separate values with spaces.
                        if (j != 0)
                            buffer.append(" ");
                        buffer.append( squeezeText( child ) );
                    }
                    contents = buffer.toString();
                }
            }

            _currentRow[ i ] = contents;
        }
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


}

