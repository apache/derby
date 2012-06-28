/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.compatibility.AbstractCompatibilityTest

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
package org.apache.derbyTesting.functionTests.tests.compatibility;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.DerbyVersion;
import org.apache.derbyTesting.junit.Version;

/**
 * Abstract test case with common functionality often required when writing
 * JDBC client driver compatibility tests.
 */
abstract class AbstractCompatibilityTest
        extends BaseJDBCTestCase
{
    /////////////////////////////////////////////////////////////
    //
    //    CONSTANTS
    //
    /////////////////////////////////////////////////////////////

    public static final String SERVER_VERSION_FUNCTION = "getVMVersion";

    private static final String VERSION_PROPERTY = "java.version";

    /////////////////////////////////////////////////////////////
    //
    //    STATE
    //
    /////////////////////////////////////////////////////////////

    private static Version _clientVMLevel;        // level of client-side vm
    private static Version _serverVMLevel;        // level of server vm
    private static DerbyVersion _driverLevel;     // client rev level

    public AbstractCompatibilityTest(String name) {
        super(name);
    }

    /////////////////////////////////////////////////////////////
    //
    //    PUBLIC BEHAVIOR
    //
    /////////////////////////////////////////////////////////////

    public DerbyVersion getServerVersion() throws SQLException {
        return getServerVersion(getConnection());
    }

    /**
     * <p>
     * Get the version of the server.
     * </p>
     */
    protected static DerbyVersion getServerVersion(Connection con)
            throws SQLException {
        return DerbyVersion.parseVersionString(
                con.getMetaData().getDatabaseProductVersion());
    }

    /**
     * <p>
     * Get the version of the client.
     * </p>
     */
    public DerbyVersion getDriverVersion()
            throws SQLException {
        if (_driverLevel == null) {
            _driverLevel = DerbyVersion.parseVersionString(
                    getConnection().getMetaData().getDriverVersion());
        }
        return _driverLevel;
    }

    /**
     * <p>
     * Get the vm level of the server.
     * </p>
     */
    public    static    Version    getServerVMVersion()    { return _serverVMLevel; }

    /**
     * <p>
     * Get the vm level of the client.
     * </p>
     */
    public    Version    getClientVMVersion() { return _clientVMLevel; }

    /**
     * <p>
     *  Report whether the server supports ANSI UDTs.
     * </p>
     */
    public boolean serverSupportsUDTs()
            throws SQLException {
        return serverSupportsUDTs(getConnection());
    }

    public static boolean serverSupportsUDTs(Connection con)
            throws SQLException {
        return getServerVersion(con).atLeast( DerbyVersion._10_6 );
    }

    /////////////////////////////////////////////////////////////
    //
    //    DATABASE-SIDE FUNCTIONS
    //
    /////////////////////////////////////////////////////////////

    /**
     * <p>
     * Get the vm level of the server.
     * </p>
     */
    public    static    String    getVMVersion()
    {
        return System.getProperty( VERSION_PROPERTY );
    }

    // POTENTIALLY TEMPORARY METHODS - CLEANUP LATER

    /**
     * <p>
     * Assert two objects are equal, allowing nulls to be equal.
     * </p>
     */
    public    void    compareObjects( String message, Object left, Object right )
            throws SQLException
    {
        message = message + "\n\t expected = " + left + "\n\t actual = " + right;

        if ( left == null )
        {
            assertNull( message, right );
        }
        else
        {
            assertNotNull( message, right );

            if ( left instanceof byte[] ) { compareBytes( message, left, right ); }
            else if ( left instanceof java.util.Date ) { compareDates( message, left, right ); }
            else { assertTrue( message, left.equals( right ) ); }
        }
    }

    /**
     * <p>
     * Assert two byte arrays are equal, allowing nulls to be equal.
     * </p>
     */
    public    void    compareBytes( String message, Object left, Object right )
    {
        if ( left == null )    { assertNull( message, right ); }
        else { assertNotNull( right ); }

        if ( !(left instanceof byte[] ) ) { fail( message ); }
        if ( !(right instanceof byte[] ) ) { fail( message ); }

        byte[]    leftBytes = (byte[]) left;
        byte[]    rightBytes = (byte[]) right;
        int        count = leftBytes.length;

        assertEquals( message, count, rightBytes.length );

        for ( int i = 0; i < count; i++ )
        {
            assertEquals( message + "[ " + i + " ]", leftBytes[ i ], rightBytes[ i ] );
        }
    }

    /**
     * <p>
     * Assert two Dates are equal, allowing nulls to be equal.
     * </p>
     */
    public    void    compareDates( String message, Object left, Object right )
    {
        if ( left == null )    { assertNull( message, right ); }
        else { assertNotNull( right ); }

        if ( !(left instanceof java.util.Date ) ) { fail( message ); }
        if ( !(right instanceof java.util.Date ) ) { fail( message ); }

        assertEquals( message, left.toString(), right.toString() );
    }

    /**
     * <p>
     * Read a column from a ResultSet given its column name and expected jdbc
     * type. This method is useful if you are want to verify the getXXX() logic
     * most naturally fitting the declared SQL type.
     * </p>
     */
    private static final int JDBC_BOOLEAN = 16;
    protected    Object    getColumn( ResultSet rs, String columnName, int jdbcType )
        throws SQLException
    {
        Object        retval = null;

        switch( jdbcType )
        {
            case JDBC_BOOLEAN:
                retval = Boolean.valueOf(rs.getBoolean(columnName));
                break;

            case Types.BIGINT:
                retval = new Long( rs.getLong( columnName ) );
                break;

            case Types.BLOB:
                retval = rs.getBlob( columnName );
                break;

            case Types.CHAR:
            case Types.LONGVARCHAR:
            case Types.VARCHAR:
                retval = rs.getString( columnName );
                break;

            case Types.BINARY:
            case Types.LONGVARBINARY:
            case Types.VARBINARY:
                retval = rs.getBytes( columnName );
                break;

            case Types.CLOB:
                retval = rs.getClob( columnName );
                break;

            case Types.DATE:
                retval = rs.getDate( columnName );
                break;

            case Types.DECIMAL:
            case Types.NUMERIC:
                retval = rs.getBigDecimal( columnName );
                break;

            case Types.DOUBLE:
                retval = new Double( rs.getDouble( columnName ) );
                break;

            case Types.REAL:
                retval = new Float( rs.getFloat( columnName ) );
                break;

            case Types.INTEGER:
                retval = new Integer( rs.getInt( columnName ) );
                break;

            case Types.SMALLINT:
                retval = new Short( rs.getShort( columnName ) );
                break;

            case Types.TIME:
                retval = rs.getTime( columnName );
                break;

            case Types.TIMESTAMP:
                retval = rs.getTimestamp( columnName );
                break;

            default:
                fail( "Unknown jdbc type " + jdbcType + " used to retrieve column: " + columnName );
                break;
        }

        if ( rs.wasNull() ) { retval = null; }

        return retval;
    }

    /**
     * <p>
     * Stuff a PreparedStatement parameter given its 1-based parameter position
     * and expected jdbc type. This method is useful for testing the setXXX()
     * methods most natural for a declared SQL type.
     * </p>
     */
    protected    void    setParameter( PreparedStatement ps, int param, int jdbcType, Object value )
        throws SQLException
    {
        if ( value == null )
        {
            ps.setNull( param, jdbcType );

            return;
        }

        switch( jdbcType )
        {
            case JDBC_BOOLEAN:
                ps.setBoolean( param, ((Boolean) value ).booleanValue() );
                break;

            case Types.BIGINT:
                ps.setLong( param, ((Long) value ).longValue() );
                break;

            case Types.BLOB:
                ps.setBlob( param, ((java.sql.Blob) value ) );
                break;

            case Types.CHAR:
            case Types.LONGVARCHAR:
            case Types.VARCHAR:
                ps.setString( param, ((String) value ) );
                break;

            case Types.BINARY:
            case Types.LONGVARBINARY:
            case Types.VARBINARY:
                ps.setBytes( param, (byte[]) value );
                break;

            case Types.CLOB:
                ps.setClob( param, ((java.sql.Clob) value ) );
                break;

            case Types.DATE:
                ps.setDate( param, ((java.sql.Date) value ) );
                break;

            case Types.DECIMAL:
            case Types.NUMERIC:
                ps.setBigDecimal( param, ((java.math.BigDecimal) value ) );
                break;

            case Types.DOUBLE:
                ps.setDouble( param, ((Double) value ).doubleValue() );
                break;

            case Types.REAL:
                ps.setFloat( param, ((Float) value ).floatValue() );
                break;

            case Types.INTEGER:
                ps.setInt( param, ((Integer) value ).intValue() );
                break;

            case Types.SMALLINT:
                ps.setShort( param, ((Short) value ).shortValue() );
                break;

            case Types.TIME:
                ps.setTime( param, (java.sql.Time) value );
                break;

            case Types.TIMESTAMP:
                ps.setTimestamp( param, (java.sql.Timestamp) value );
                break;

            default:
                fail( "Unknown jdbc type: " + jdbcType );
                break;
        }

    }
    /**
     * <p>
     * Drop a function regardless of whether it exists. If the function does not
     * exist, don't log an error unless
     * running in debug mode. This method is to be used for reinitializing
     * a schema in case a previous test run failed to clean up after itself.
     * Do not use this method if you need to verify that the function really exists.
     * </p>
     */
    protected    void    dropFunction(String name )
    {
        dropSchemaObject(FUNCTION, name, false );
    }

    /**
     * <p>
     * Drop a procedure regardless of whether it exists. If the procedure does
     * not exist, don't log an error unless
     * running in debug mode. This method is to be used for reinitializing
     * a schema in case a previous test run failed to clean up after itself.
     * Do not use this method if you need to verify that the procedure really exists.
     * </p>
     */
    protected    void    dropProcedure(String name )
    {
        dropSchemaObject(PROCEDURE, name, false );
    }

    /**
     * <p>
     * Drop a UDT regardless of whether it exists. If the UDT does
     * not exist, don't log an error unless
     * running in debug mode. This method is to be used for reinitializing
     * a schema in case a previous test run failed to clean up after itself.
     * Do not use this method if you need to verify that the UDT really exists.
     * </p>
     */
    protected    void    dropUDT(String name )
    {
        dropSchemaObject(TYPE, name, true );
    }

    protected    void    dropSchemaObject(String genus, String objectName, boolean restrict )
    {
        try {
            String text = "drop " + genus + " " + objectName;
            if ( restrict ) { text = text + " restrict"; }
            PreparedStatement ps = prepareStatement(text );

            ps.execute();
            ps.close();
        }
        catch (SQLException e)
        {
        }

    }

    private    static    final    String    FUNCTION = "function";
    private    static    final    String    PROCEDURE = "procedure";
    private    static    final    String    TYPE = "type";
}
