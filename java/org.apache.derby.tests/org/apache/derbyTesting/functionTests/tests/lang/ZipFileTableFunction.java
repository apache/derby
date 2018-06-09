/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.ZipFileTableFunction

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
package org.apache.derbyTesting.functionTests.tests.lang;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * <p>
 * Table Function for iterating through the entries in a zip file.
 * </p>
 */
public class ZipFileTableFunction extends EnumeratorTableFunction
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String[]    COLUMN_NAMES =
    {
        "name",
        "directory",
        "comment",
        "compressed_size",
        "crc",
        "size",
        "modification_time",
    };

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TABLE FUNCTION METHOD
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * This is the method which is registered as a table function.
     * </p>
     */
    public  static  ResultSet   zipFile( String fileName )
        throws SQLException
    {
        return new ZipFileTableFunction( fileName );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Construct from a zip file.
     * </p>
     */
    public  ZipFileTableFunction( String zipFileName )
        throws SQLException
    {
        super( COLUMN_NAMES );

        try {
            ZipFile    zipFile = new ZipFile( zipFileName );

            setEnumeration( zipFile.entries() );
            
        } catch (IOException ioe) { throw wrap( ioe ); }
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ABSTRACT BEHAVIOR TO BE IMPLEMENTED BY CHILDREN OF EnumeratorTableFunction
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  String[]    makeRow( Object obj ) throws SQLException
    {
        int             idx = 0;
        ZipEntry    zipEntry = (ZipEntry) obj;
        String      rawName = zipEntry.getName();
        int           stubIdx =  rawName.lastIndexOf( '/' );
        String      name;
        String      directoryName;

        if ( stubIdx < 0 )
        {
            name = rawName;
            directoryName = "";
        }
        else
        {
            name = rawName.substring( stubIdx + 1, rawName.length() );
            directoryName = rawName.substring( 0, stubIdx );
        }

        String[]    row = new String[ getColumnCount() ];

        row[ idx++ ] = name;
        row[ idx++ ] = directoryName;
        row[ idx++ ] = zipEntry.getComment();
        row[ idx++ ] = Long.toString( zipEntry.getCompressedSize() );
        row[ idx++ ] = Long.toString( zipEntry.getCrc() );
        row[ idx++ ] = Long.toString( zipEntry.getSize() );
        row[ idx++ ] = Long.toString( zipEntry.getTime() );

        return row;
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private SQLException wrap(Throwable t) { return new SQLException(t.getMessage(), t); }
}
