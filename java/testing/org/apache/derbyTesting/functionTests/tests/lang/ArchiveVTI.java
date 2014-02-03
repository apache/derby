/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.ArchiveVTI

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.derby.vti.AwareVTI;
import org.apache.derby.vti.ForeignTableVTI;
import org.apache.derby.vti.ForwardingVTI;
import org.apache.derby.vti.RestrictedVTI;
import org.apache.derby.vti.Restriction;
import org.apache.derby.vti.VTIContext;

/**
 * <p>
 * This table function acts like a union view on a set of archive tables.
 * The idea is that the old contents of a main table are periodically moved to
 * archive tables whose names start with $tableName$suffix. Each bulk
 * move of rows results in the creation of a new archive table. The archive
 * tables live in the same schema as the main table and have its shape. This
 * table function unions the main table together with all of its archived snapshots.
 * So, for instance, you might have the following set of tables, which this table
 * function unions together:
 * </p>
 *
 * <pre>
 *  T1
 *  T1_ARCHIVE_1
 *  T1_ARCHIVE_2
 *   ...
 *  T1_ARCHIVE_N
 * </pre>
 *
 * <p>
 * This table function may appear in user documentation. If you change the behavior
 * of this table function, make sure that you adjust the user documentation linked from
 * DERBY-6117.
 * </p>
 */
public class ArchiveVTI extends ForwardingVTI implements AwareVTI, RestrictedVTI
{
    ////////////////////////////////////////////////////////////////////////
    //
    //	CONSTANTS
    //
    ////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////
    //
    //	STATE
    //
    ////////////////////////////////////////////////////////////////////////

    private Connection  _connection;
    private String          _archiveSuffix;
    private VTIContext  _vtiContext;
    private ArrayList<String>   _tableNames;
    private int         _tableIdx;
    
    private String[]    _columnNames;
    private Restriction _restriction;

    /////////////////////////////////////////////////////////////////////////
    //
    //  TABLE FUNCTION
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Entry point for creating an ArchiveVTI which is bound to a Derby table function
     * by a CREATE FUNCTION statement which looks like this:
     * </p>
     *
     * <pre>
     * create function t1( archiveSuffix varchar( 32672 ) ) returns table
     * (
     *     keyCol int,
     *     aCol int,
     *     bCol int
     * )
     * language java parameter style derby_jdbc_result_set reads sql data
     * external name 'org.apache.derbyTesting.functionTests.tests.lang.ArchiveVTI.archiveVTI'
     * </pre>
     *
     * @param archiveSuffix All of the archive tables have names of the form $tablename$archiveSuffix.
     */
    public  static  ArchiveVTI  archiveVTI( String archiveSuffix ) throws SQLException
    { return new ArchiveVTI( archiveSuffix ); }

    ////////////////////////////////////////////////////////////////////////
    //
    //	CONSTRUCTOR
    //
    ////////////////////////////////////////////////////////////////////////

    /** Construct from the suffix which flags all of the relevant tables. */
    public  ArchiveVTI( String archiveSuffix )    throws SQLException
    {
        _connection = DriverManager.getConnection( "jdbc:default:connection" );
        _archiveSuffix = archiveSuffix;
    }

    /////////////////////////////////////////////////////////////////////////
    //
    //  AwareVTI BEHAVIOR
    //
    /////////////////////////////////////////////////////////////////////////

    public  VTIContext  getContext() { return _vtiContext; }
    public  void    setContext( VTIContext context )    { _vtiContext = context; }

    ////////////////////////////////////////////////////////////////////////
    //
    //	RestrictedVTI BEHAVIOR
    //
    ////////////////////////////////////////////////////////////////////////

    public  void    initScan
        ( String[] columnNames, Restriction restriction )
        throws SQLException
    {
        _columnNames = new String[ columnNames.length ];
        System.arraycopy( columnNames, 0, _columnNames, 0, columnNames.length );
        _restriction = restriction;
    }

    ////////////////////////////////////////////////////////////////////////
    //
    //	ResultSet BEHAVIOR
    //
    ////////////////////////////////////////////////////////////////////////

    public boolean next()   throws SQLException
    {
        if ( _tableNames == null )
        {
            getTableNames();
            _tableIdx = 0;
            loadResultSet();
        }

        while ( !super.next() )
        {
            _tableIdx++;
            if ( _tableIdx >= _tableNames.size() ) { return false; }
            loadResultSet();
        }

        return true;
    }

    public  void    close() throws SQLException
    {
        if ( getWrappedResultSet() != null ) { getWrappedResultSet().close(); }
        wrapResultSet( null );
        _connection = null;
    }

    ////////////////////////////////////////////////////////////////////////
    //
    //	UTILITY METHODS
    //
    ////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Get cursors on all the tables which we are going to union together.
     * </p>
     */
    private void    getTableNames() throws SQLException
    {
        _tableNames = new ArrayList<String>();
        _tableNames.add( getContext().vtiTable() );

        DatabaseMetaData    dbmd = getConnection().getMetaData();
        ResultSet   candidates = dbmd.getTables
            ( null, getContext().vtiSchema(), getContext().vtiTable() + _archiveSuffix + "%", null );

        while ( candidates.next() )
        {
            _tableNames.add( candidates.getString( "TABLE_NAME" ) );
        }
        candidates.close();
    }
    
    /**
     * <p>
     * Compile the query against the next table and use its ResultSet until
     * it's drained.
     * </p>
     */
    private void    loadResultSet() throws SQLException
    {
        if ( getWrappedResultSet() != null ) { getWrappedResultSet().close(); }
        
        ForeignTableVTI     nextRS = new ForeignTableVTI
            ( getContext().vtiSchema(), _tableNames.get( _tableIdx ), getConnection() );
        nextRS.initScan( _columnNames, _restriction );

        wrapResultSet( nextRS );
    }

    /**
     * <p>
     * Get this database session's connection to the database.
     * </p>
     */
    private Connection  getConnection() throws SQLException
    {
        return _connection;
    }
    
}
