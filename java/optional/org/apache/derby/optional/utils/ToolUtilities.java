/*

   Class org.apache.derby.optional.utils.ToolUtilities

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

package org.apache.derby.optional.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.shared.common.reference.SQLState;

/**
 * Utility functions shared across the optional tools.
 * 
 */
public class ToolUtilities
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

    ////////////////////////////////////////////////////////////////////////
    //
    //	ENTRY POINTS
    //
    ////////////////////////////////////////////////////////////////////////

    /**
     * Returns true if SQL authorization is enabled in the connected database.
     */
    public  static  boolean sqlAuthorizationEnabled( Connection conn )
        throws SQLException
    {
        try {
            ResultSet   rs;
        
            // first check to see if NATIVE authentication is on
            rs = conn.prepareStatement( "select count(*) from sys.sysusers" ).executeQuery();
            rs.next();
            try {
                if ( rs.getInt( 1 ) > 0 ) { return true; }
            }
            finally { rs.close(); }
        }
        catch (SQLException se)
        {
            if ( SQLState.DBO_ONLY.equals( se.getSQLState() ) ) { return true; }
        }
        
        ResultSet   rs = conn.prepareStatement
            (
             "values syscs_util.syscs_get_database_property( 'derby.database.sqlAuthorization' )"
             ).executeQuery();

        try {
            if ( !( rs.next() ) ) { return false; }

            return ( "true".equals( rs.getString( 1 ) ) );
        }
        finally { rs.close(); }
    }
    
    /**
     * <p>
     * Raise an exception if SQL authorization is enabled and the current user
     * isn't the DBO or the owner of the indicated schema or if the indicated schema
     * doesn't exist.
     * </p>
     */
    public static  void    mustBeOwner( Connection conn, String schema )
        throws SQLException
    {
        if ( !sqlAuthorizationEnabled( conn ) ) { return; }

        String  dbo = getOwner( conn, "SYS" );
        String  schemaOwner = getOwner( conn, schema );
        String  currentUser = getCurrentUser( conn );

        if (
            (schemaOwner != null) &&
            (
             schemaOwner.equals( currentUser ) ||
             dbo.equals( currentUser )
             )
            )   { return; }
        else
        {
            throw newSQLException( SQLState.LUCENE_MUST_OWN_SCHEMA );
        }
    }

    /**
     * <p>
     * Raise an exception if SQL authorization is enabled and the current user
     * isn't the DBO.
     * </p>
     */
    public static  void    mustBeDBO( Connection conn )
        throws SQLException
    {
        if ( !sqlAuthorizationEnabled( conn ) ) { return; }

        String  dbo = getOwner( conn, "SYS" );
        String  currentUser = getCurrentUser( conn );

        if ( dbo.equals( currentUser ) )   { return; }
        else
        {
            throw newSQLException( SQLState.DBO_ONLY );
        }
    }

    /** Get the current user */
    public static  String  getCurrentUser( Connection conn )
        throws SQLException
    {
        ResultSet   rs = conn.prepareStatement( "values current_user" ).executeQuery();
        try {
            rs.next();
            return rs.getString( 1 );
        } finally { rs.close(); }
    }

    /**
     * <p>
     * Get the owner of the indicated schema. Returns null if the schema doesn't exist.
     * </p>
     */
    public static  String  getOwner( Connection conn, String schema )
        throws SQLException
    {
        PreparedStatement   ps = conn.prepareStatement
            ( "select authorizationID from sys.sysschemas where schemaName = ?" );
        ps.setString( 1, derbyIdentifier( schema ) );

        ResultSet   rs = ps.executeQuery();
        try {
            if ( rs.next() ) { return rs.getString( 1 ); }
            else { return null; }
        } finally { rs.close(); }
    }

    /** Make a SQLException from a SQLState and optional args */
    public  static  SQLException    newSQLException( String sqlState, Object... args )
    {
        StandardException   se = StandardException.newException( sqlState, args );
        return sqlException( se );
    }
    
    /** Convert a raw string into a properly cased and escaped Derby identifier */
    public static  String  derbyIdentifier( String rawString )
        throws SQLException
    {
        try {
            return IdUtil.parseSQLIdentifier( rawString );
        }
        catch (StandardException se)  { throw ToolUtilities.sqlException( se ); }
    }

    /////////////////////////////////////////////////////////////////////
    //
    //  ERROR HANDLING
    //
    /////////////////////////////////////////////////////////////////////

    /** Turn a StandardException into a SQLException */
    public  static  SQLException    sqlException( StandardException se )
    {
        return PublicAPI.wrapStandardException( se );
    }

    /** Wrap an external exception */
    public  static  SQLException    wrap( Throwable t )
    {
        return sqlException( StandardException.plainWrapException( t ) );
    }
    
}

