/*

Derby - Class org.apache.derbyDemo.vtis.example.ApacheServerLogVTI

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

package org.apache.derbyDemo.vtis.example;

import java.sql.*;
import java.text.SimpleDateFormat;

import org.apache.derbyDemo.vtis.core.*;

/**
 * <p>
 * This is an XML-reading VTI which has been tweaked to handle
 * the formatting of timestamps and nulls found in Apache
 * server logs.
 * </p>
 *
  */
public  class   ApacheServerLogVTI  extends XmlVTI
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

    private SimpleDateFormat    _dateFormatter;
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Construct from the same arguments as our superclass.
     * </p>
     */
    public  ApacheServerLogVTI( String xmlResourceName, String rowTag, String[] childTags )
    {
        super( xmlResourceName, rowTag, childTags );
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ResultSet BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * The Apache Server's logs represent nulls as "-".
     * </p>
     */
    public String getString(int columnIndex) throws SQLException
    {
        String  columnValue = super.getString( columnIndex );

        if ( "-".equals( columnValue ) )
        {
            setWasNull();
            return null;
        }
        else { return columnValue; }
    }

    /**
     * <p>
     * The Apache Server's logs format timestamps thusly: "01/Jul/2002:17:31:19 +0200"
     * </p>
     */
    public java.sql.Timestamp getTimestamp(int columnIndex) throws SQLException
    {
        String          columnValue = getString( columnIndex );

        try {
            SimpleDateFormat    dateFormatter = getDateFormatter();
            java.util.Date              rawDate = dateFormatter.parse( columnValue );
            long                            time = rawDate.getTime();

            return new java.sql.Timestamp( time );            

        } catch (Throwable t) { throw new SQLException( t.getMessage() ); }
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * The Apache Server's logs format timestamps thusly: "01/Jul/2002:17:31:19 +0200"
     * </p>
     */
    private SimpleDateFormat    getDateFormatter()
    {
        if ( _dateFormatter == null )
        {
            _dateFormatter = new SimpleDateFormat( "dd/MMM/yyyy:HH:mm:ss Z" );
        }

        return _dateFormatter;
    }


}
