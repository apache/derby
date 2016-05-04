/*

Derby - Class org.apache.derbyTesting.functionTests.util

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

/**
 * <p>
 * This class factors out utility methods (including assertion machinery)
 * for re-use by Derby JUnit tests. JUnit tests should extend this class.
 * </p>
 *
 */

package org.apache.derbyTesting.functionTests.util;

import java.io.*;
import java.math.*;
import java.sql.*;
import java.util.*;

import junit.framework.*;

public	class	DerbyJUnitTest	extends	TestCase
{
	/////////////////////////////////////////////////////////////
	//
	//	CONSTANTS
	//
	/////////////////////////////////////////////////////////////

	/** If you set this startup property to true, you will get chatty output. */
	public	static	final			String	DEBUG_FLAG = "drb.tests.debug";
	
	public	static	final			int		SUCCESS_EXIT = 0;
	public	static	final			int		FAILURE_EXIT = 1;

	public	static	final	String	DEFAULT_USER_NAME = "APP";
	public	static	final	String	DEFAULT_PASSWORD = "APP";
	public	static	final	String	DEFAULT_DATABASE_NAME = "wombat";

	// because java.sql.Types.BOOLEAN doesn't exist in jdbc 2.0
	protected	static	final			int		JDBC_BOOLEAN = 16;
	
	//
	// For dropping schema objects
	//
	private	static	final	String	TABLE = "table";
	private	static	final	String	FUNCTION = "function";
	private	static	final	String	PROCEDURE = "procedure";
	private	static	final	String	TYPE = "type";
	
	//
	// These are properties for the Derby connection URL.
	//
	private	static	final			String	CREATE_PROPERTY = "create=true";

	//
	// Indexes into the array of client-specific strings. E.g.,
	// DERBY_CLIENT, and EMBEDDED_CLIENT.
	//
	public	static	final			int		DATABASE_URL = 0;
	public	static	final			int		DRIVER_NAME = DATABASE_URL + 1;
	public	static	final			int		FRAMEWORK_NAME = DRIVER_NAME + 1;

	// indexed by DATABASE_URL and DRIVER_NAME
	private	static	final	String[]	DERBY_CLIENT =
	{
		"jdbc:derby://localhost:1527/",
		"org.apache.derby.jdbc.ClientDriver",
		"DerbyNetClient"
	};
	private	static	final	String[]	EMBEDDED_CLIENT =
	{
		"jdbc:derby:",
		"org.apache.derby.jdbc.EmbeddedDriver",
		"embedded"
	};

	public	static	final	String[][]	LEGAL_CLIENTS =
	{
		DERBY_CLIENT,
		EMBEDDED_CLIENT
	};
	
	/////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	/////////////////////////////////////////////////////////////

	private	static	boolean		_debug;					// if true, we print chatty diagnostics
	
	private	static	PrintStream	_outputStream = System.out;	// where to print debug output

	private	static	String		_databaseName;			// sandbox for tests
	private	static	String[]	_defaultClientSettings;	// one of the clients in
														// LEGAL_CLIENTS

	/////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTOR
	//
	/////////////////////////////////////////////////////////////
	
	/**
	 * <p>
	 * Vacuous constructor for JUnit machinery.
	 * </p>
	 */
	public	DerbyJUnitTest() {}

	/////////////////////////////////////////////////////////////
	//
	//	PUBLIC BEHAVIOR
	//
	/////////////////////////////////////////////////////////////

	/**
	 * <p>
	 * Return true if we're using the embedded driver.
	 * </p>
	 */
	public	boolean	usingEmbeddedClient() { return ( _defaultClientSettings == EMBEDDED_CLIENT ); }

	/**
	 * <p>
	 * Return true if we're using the derby client
	 * </p>
	 */
	public	boolean	usingDerbyClient() { return ( _defaultClientSettings == DERBY_CLIENT ); }

	/**
	 * <p>
	 * Get the client we're using.
	 * </p>
	 */
	public	static	String[]	getClientSettings() { return _defaultClientSettings; }

	/**
	 * <p>
	 * Set the client we're going to use.
	 * </p>
	 */
	public	static	void		setClient( String[] client ) { _defaultClientSettings = client; }

	/**
	 * <p>
	 * Set the database name.
	 * </p>
	 */
	public	static	void	setDatabaseName( String databaseName ) { _databaseName = databaseName; }
	
	/**
	 * <p>
	 * Force the debugging state. Useful for debugging under the test harness.
	 * </p>
	 */
	public	static	void	setDebug( boolean value ) { _debug = value; }

	/**
	 * <p>
	 * Look for the system property which tells us whether to run
	 * chattily.
	 * </p>
	 */
	public	static	boolean	parseDebug()
	{
		_debug = Boolean.getBoolean( DEBUG_FLAG );
		
		return true;
	}
		
	/**
	 * <p>
	 * Debug code to print chatty informational messages.
	 * </p>
	 */
	public	static	void	println( String text )
	{
		if ( _debug ) { alarm( text ); }
	}

	/**
	 * <p>
	 * Print a message regardless of whether we are running in debug mode.
	 * </p>
	 */
	public	static	void	alarm( String text )
	{
		_outputStream.println( text );
		_outputStream.flush();
	}

	/**
	 * <p>
	 * Print out a stack trace.
	 * </p>
	 */
	public	static	void	printStackTrace( Throwable t )
	{
		while ( t != null )
		{
			t.printStackTrace( _outputStream );

			if ( t instanceof SQLException )	{ t = ((SQLException) t).getNextException(); }
			else { break; }
		}
	}

	/////////////////////////////////////////////////////////////
	//
	//	CONNECTION MANAGEMENT
	//
	/////////////////////////////////////////////////////////////

	/**
	 * <p>
	 * Load a client driver, given its particulars.
	 * </p>
	 */
	protected	static	boolean	faultInDriver( String[] clientSettings )
	{
		String	currentClientName = clientSettings[ DRIVER_NAME ];
		
		try {
			Class.forName( currentClientName );

			return true;
		}
		catch (Exception e)
		{
			println( "Could not find " + currentClientName );
			return false;
		}
	}

	/**
	 * <p>
	 * Get a connection to a database, using the default client.
	 * </p>
	 */
	protected	static	Connection	getConnection()
		throws Exception
	{
		return getConnection( _defaultClientSettings, _databaseName, new Properties() );
	}
	/**
	 * <p>
	 * Get a connection to a database, using the specified client.
	 * </p>
	 */
	protected	static	Connection	getConnection
	(
	    String[]	clientSettings,
		String		databaseName,
		Properties	properties
	)
		throws Exception
	{
		faultInDriver( clientSettings );

		properties.put( "user", DEFAULT_USER_NAME );
		properties.put( "password", DEFAULT_PASSWORD );
		properties.put( "retreiveMessagesFromServerOnGetMessage", "true" );

		Connection		conn = DriverManager.getConnection
			( makeDatabaseURL( clientSettings, databaseName ), properties );

		println( "Connection is a " + conn.getClass().getName() );
		
		return conn;
	}

	/**
	 * <p>
	 * Cobble together a connection URL.
	 * </p>
	 */
	private	static	String	makeDatabaseURL( String[] clientSettings, String databaseName )
	{
		return clientSettings[ DATABASE_URL ] + databaseName;
	}

	/**
	 * <p>
	 * Create an empty database.
	 * </p>
	 */
	protected	void	createDB( String databaseName )
		throws Exception
	{
		String[]	clientSettings = getClientSettings();
		String		dbURL = makeDatabaseURL( clientSettings, databaseName );

		dbURL = dbURL + ';' + CREATE_PROPERTY;

		Properties	properties = new Properties();

		properties.put( "user", DEFAULT_USER_NAME );
		properties.put( "password", DEFAULT_PASSWORD );

		faultInDriver( clientSettings );

		Connection		conn = DriverManager.getConnection( dbURL, properties );

		conn.close();
	}

	///////////////
	//
	//	SQL MINIONS
	//
	///////////////

	/**
	 * <p>
	 * Execute DDL statement.
	 * </p>
	 */
	protected	static	void	executeDDL( Connection conn, String text )
		throws SQLException
	{
		PreparedStatement	ps = null;

		try {
			ps = prepare( conn, text );

			ps.execute();
		}
		finally { close( ps ); }
	}
	
	/**
	 * <p>
	 * Execute a SQL statement, given by the text argument. This thin
	 * wrapper around the JDBC machinery logs the statement text when
	 * running in debug mode.
	 * </p>
	 */
	protected	static	void	execute( Connection conn, String text )
		throws SQLException
	{
		PreparedStatement	ps = prepare( conn, text );

		ps.execute();
		close( ps );
	}

	/**
	 * <p>
	 * Prepare a SQL statement, given by the text argument. This thin
	 * wrapper around the JDBC machinery logs the statement text when
	 * running in debug mode.
	 * </p>
	 */
	protected	static	PreparedStatement	prepare( Connection conn, String text )
		throws SQLException
	{
		println( "Preparing: " + text );

		return conn.prepareStatement( text );
	}

	/**
	 * <p>
	 * Prepare a SQL call statement, given by the text argument. This thin
	 * wrapper around the JDBC machinery logs the statement text when
	 * running in debug mode.
	 * </p>
	 */
	protected	static	CallableStatement	prepareCall( Connection conn, String text )
		throws SQLException
	{
		println( "Preparing procedure call: '" + text + "'" );

		CallableStatement	cs = conn.prepareCall( text );

		return cs;
	}

	/**
	 * <p>
	 * Scour out all the rows from a table.
	 * </p>
	 */
	protected	static	void	truncateTable( Connection conn, String name )
		throws SQLException
	{
		PreparedStatement	ps = prepare( conn, "delete from " + name );

		ps.execute();
	}

	/**
	 * <p>
	 * Drop a table regardless of whether it exists. If the table does not
	 * exist, don't log an error unless
	 * running in debug mode. This method is to be used for reinitializing
	 * a schema in case a previous test run failed to clean up after itself.
	 * Do not use this method if you need to verify that the table really exists.
	 * </p>
	 */
	protected	static	void	dropTable( Connection conn, String name )
	{
		dropSchemaObject( conn, TABLE, name, false );
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
	protected	static	void	dropFunction( Connection conn, String name )
	{
		dropSchemaObject( conn, FUNCTION, name, false );
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
	protected	static	void	dropProcedure( Connection conn, String name )
	{
		dropSchemaObject( conn, PROCEDURE, name, false );
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
	protected	static	void	dropUDT( Connection conn, String name )
	{
		dropSchemaObject( conn, TYPE, name, true );
	}

	/**
	 * <p>
	 * Drop a schema object regardless of whether it exists. If the object does
	 * not exist, don't log an error unless
	 * running in debug mode. This method is to be used for reinitializing
	 * a schema in case a previous test run failed to clean up after itself.
	 * Do not use this method if you need to verify that the object really exists.
	 * </p>
	 */
	protected	static	void	dropSchemaObject( Connection conn, String genus, String objectName, boolean restrict )
	{
		PreparedStatement	ps = null;
		
		try {
            String text = "drop " + genus + " " + objectName;
            if ( restrict ) { text = text + " restrict"; }
			ps = prepare( conn, text );

			ps.execute();
		}
		catch (SQLException e)
		{
			if ( _debug ) { printStackTrace( e ); }
		}

		close( ps );
	}

	/**
	 * <p>
	 * Close a ResultSet. This method factors out the check for whether
	 * the ResultSet was created in the first place. This tidies up the
	 * caller's cleanup logic. If an error occurs, print it. Because this
	 * method swallows the exception after printing it, do not call this
	 * method if you want your test to halt on error.
	 * </p>
	 */
	protected	static	void	close( ResultSet rs )
	{
		try {
			if ( rs != null ) { rs.close(); }
		}
		catch (SQLException e) { printStackTrace( e ); }
	}	

	/**
	 * <p>
	 * Close a Statement. This method factors out the check for whether
	 * the Statement was created in the first place. This tidies up the
	 * caller's cleanup logic. If an error occurs, print it. Because this
	 * method swallows the exception after printing it, do not call this
	 * method if you want your test to halt on error.
	 * </p>
	 */
	protected	static	void	close( Statement statement )
	{
		try {
			if ( statement != null ) { statement.close(); }
		}
		catch (SQLException e) { printStackTrace( e ); }
	}

	/**
	 * <p>
	 * Close a Connection. This method factors out the check for whether
	 * the Connection was created in the first place. This tidies up the
	 * caller's cleanup logic. If an error occurs, print it. Because this
	 * method swallows the exception after printing it, do not call this
	 * method if you want your test to halt on error.
	 * </p>
	 */
	protected	static	void	close( Connection conn )
	{
		try {
			if ( conn != null ) { conn.close(); }
		}
		catch (SQLException e) { printStackTrace( e ); }
	}

	/**
	 * <p>
	 * Read a column from a ResultSet given its column name and expected jdbc
	 * type. This method is useful if you are want to verify the getXXX() logic
	 * most naturally fitting the declared SQL type.
	 * </p>
	 */
	protected	Object	getColumn( ResultSet rs, String columnName, int jdbcType )
		throws Exception
	{
		Object		retval = null;

		switch( jdbcType )
		{
		    case JDBC_BOOLEAN:
				retval = rs.getBoolean( columnName );
				break;
				
		    case Types.BIGINT:
				retval = rs.getLong( columnName );
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
				retval = rs.getDouble( columnName );
				break;
				
		    case Types.REAL:
				retval = rs.getFloat( columnName );
				break;
				
		    case Types.INTEGER:
				retval = rs.getInt( columnName );
				break;
				
		    case Types.SMALLINT:
				retval = rs.getShort( columnName );
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
	 * Read a column from a ResultSet given its column position
	 * and an expected Java type. This method is useful when
	 * comparing ResultSets against expected values.
	 * </p>
	 *
	 * @param	rs		The ResultSet to read.
	 * @param	param	The column number (1-based)
	 * @param	value	An object whose type is what we expect the column to be.
	 */
	protected	Object	getColumn( ResultSet rs, int param, Object value )
		throws Exception
	{
		Object		retval;
		
		if ( value == null )
		{
			retval = rs.getObject( param );
		}
		else if ( value instanceof Boolean ) { retval = rs.getBoolean( param ); }
		else if ( value instanceof Byte ) { retval = rs.getByte( param ); }
		else if ( value instanceof Short ) { retval = rs.getShort( param ); }
		else if ( value instanceof Integer ) { retval = rs.getInt( param ); }
		else if ( value instanceof Long ) { retval = rs.getLong( param ); }
		else if ( value instanceof Float ) { retval = rs.getFloat( param ); }
		else if ( value instanceof Double ) { retval = rs.getDouble( param ); }
		else if ( value instanceof String ) { retval = rs.getString( param ); }
		else if ( value instanceof BigDecimal ) { retval = rs.getBigDecimal( param ); }
		else { retval = rs.getObject( param ); }

		if ( rs.wasNull() ) { retval = null; }

		return retval;
	}
	
	/**
	 * <p>
	 * Read an output argument from a CallableStatement given its 1-based
	 * argument position and expected jdbc type. This is useful for
	 * exercising the getXXX() methods most natural to a declared SQL type.
	 * </p>
	 */
	protected	Object	getOutArg( CallableStatement cs, int arg, int jdbcType )
		throws Exception
	{
		Object		retval = null;

		switch( jdbcType )
		{
		    case JDBC_BOOLEAN:
				retval = cs.getBoolean( arg );
				break;
				
		    case Types.BIGINT:
				retval = cs.getLong( arg );
				break;
				
		    case Types.BLOB:
				retval = cs.getBlob( arg );
				break;
				
		    case Types.CHAR:
		    case Types.LONGVARCHAR:
		    case Types.VARCHAR:
				retval = cs.getString( arg );
				break;
				
		    case Types.BINARY:
		    case Types.LONGVARBINARY:
		    case Types.VARBINARY:
				retval = cs.getBytes( arg );
				break;
				
		    case Types.CLOB:
				retval = cs.getClob( arg );
				break;
				
		    case Types.DATE:
				retval = cs.getDate( arg );
				break;
				
		    case Types.DECIMAL:
		    case Types.NUMERIC:
				retval = cs.getBigDecimal( arg );
				break;
				
		    case Types.DOUBLE:
				retval = cs.getDouble( arg );
				break;
				
		    case Types.REAL:
				retval = cs.getFloat( arg );
				break;
				
		    case Types.INTEGER:
				retval = cs.getInt( arg );
				break;
				
		    case Types.SMALLINT:
				retval = cs.getShort( arg );
				break;
				
		    case Types.TIME:
				retval = cs.getTime( arg );
				break;
				
		    case Types.TIMESTAMP:
				retval = cs.getTimestamp( arg );
				break;
				
		    default:
				fail( "Unknown jdbc type " + jdbcType + " used to retrieve column: " + arg );
				break;
		}

		if ( cs.wasNull() ) { retval = null; }

		return retval;
	}

	/**
	 * <p>
	 * Stuff a PreparedStatement parameter given its 1-based parameter position
	 * and expected jdbc type. This method is useful for testing the setXXX()
	 * methods most natural for a declared SQL type.
	 * </p>
	 */
	protected	void	setParameter( PreparedStatement ps, int param, int jdbcType, Object value )
		throws Exception
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
				ps.setBlob( param, ((Blob) value ) );
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
				ps.setClob( param, ((Clob) value ) );
				break;
				
		    case Types.DATE:
				ps.setDate( param, ((java.sql.Date) value ) );
				break;
				
		    case Types.DECIMAL:
		    case Types.NUMERIC:
				ps.setBigDecimal( param, ((BigDecimal) value ) );
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
				ps.setTime( param, (Time) value );
				break;
				
		    case Types.TIMESTAMP:
				ps.setTimestamp( param, (Timestamp) value );
				break;
				
		    default:
				fail( "Unknown jdbc type: " + jdbcType );
				break;
		}

	}
	
	/**
	 * <p>
	 * Stuff a PreparedStatement parameter given its 1-based parameter position.
	 * The appropriate setXXX() method is determined by the Java type of the
	 * value being stuffed. This method is useful for testing setXXX() methods
	 * other than the most natural fit for the declared SQL type.
	 * </p>
	 */
	protected	void	setParameter( PreparedStatement ps, int param, Object value )
		throws Exception
	{
		if ( value == null )
		{
			ps.setObject( param, null );

			return;
		}

		if ( value instanceof Boolean ) {  ps.setBoolean( param, ((Boolean) value).booleanValue() ); }
		else if ( value instanceof Byte ) { ps.setByte( param, ((Byte) value).byteValue() ); }
		else if ( value instanceof Short ) { ps.setShort( param, ((Short) value).shortValue() ); }
		else if ( value instanceof Integer ) { ps.setInt( param, ((Integer) value).intValue() ); }
		else if ( value instanceof Long ) { ps.setLong( param, ((Long) value).longValue() ); }
		else if ( value instanceof Float ) { ps.setFloat( param, ((Float) value).floatValue() ); }
		else if ( value instanceof Double ) { ps.setDouble( param, ((Double) value).doubleValue() ); }
		else if ( value instanceof String ) { ps.setString( param, ((String) value) ); }
		else { ps.setObject( param, value ); }
	}
	

	////////////////////
	//
	//	QUERY GENERATION
	//
	////////////////////

	/**
	 * <p>
	 * Single quote a string. This is a helper routine for use in generating
	 * SQL text.
	 * </p>
	 */
	protected	String	singleQuote( String text )
	{
		return "'" + text + "'";
	}

	/////////////////////////////////////////////////////////////
	//
	//	EXTRA ASSERTIONS
	//
	/////////////////////////////////////////////////////////////

	/**
	 * <p>
	 * Assert the values of a whole row.
	 * </p>
	 */
	public	void	assertRow
		( ResultSet rs, Object[] expectedRow )
		throws Exception
	{
		int		count = expectedRow.length;

		for ( int i = 0; i < count; i++ )
		{
			int			columnNumber = i + 1;
			Object		expected = expectedRow[ i ];
			Object		actual = getColumn( rs, columnNumber, expected );

			compareObjects( "Column number " + columnNumber, expected, actual );
		}
	}


	/**
	 * <p>
	 * Assert a scalar result from a query.
	 * </p>
	 */
	public	void	assertScalar
		( Connection conn, String queryText, Object expectedResult )
		throws Exception
	{
		PreparedStatement	ps = prepare( conn, queryText );
		ResultSet			rs = ps.executeQuery();

		rs.next();

		assertColumnEquals( queryText, rs, 1, expectedResult );

		close( rs );
		close( ps );
	}

	/**
	 * <p>
	 * Assert the values in a ResultSet for a given column across all rows.
	 * </p>
	 */
	public	void	assertColumnEquals
		( ResultSet rs, int columnNumber, Object[] expectedValues )
		throws Exception
	{
		int		count = expectedValues.length;

		for ( int i = 0; i < count; i++ )
		{
			rs.next();
			assertColumnEquals( Integer.toString( i ), rs, columnNumber, expectedValues[ i ] );
		}
	}

	/**
	 * <p>
	 * Assert a column's value.
	 * </p>
	 */
	public	void	assertColumnEquals
		( String message, ResultSet rs, int columnNumber, Object expectedValue )
		throws Exception
	{
		Object	actualValue = getColumn( rs, columnNumber, expectedValue );

		compareObjects( message, expectedValue, actualValue );
	}

	/**
	 * <p>
	 * Assert two objects are equal, allowing nulls to be equal.
	 * </p>
	 */
	public	void	compareObjects( String message, Object left, Object right )
		throws Exception
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
	public	void	compareBytes( String message, Object left, Object right )
		throws Exception
	{
		if ( left == null )	{ assertNull( message, right ); }
		else { assertNotNull( right ); }

		if ( !(left instanceof byte[] ) ) { fail( message ); }
		if ( !(right instanceof byte[] ) ) { fail( message ); }

		byte[]	leftBytes = (byte[]) left;
		byte[]	rightBytes = (byte[]) right;
		int		count = leftBytes.length;

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
	public	void	compareDates( String message, Object left, Object right )
		throws Exception
	{
		if ( left == null )	{ assertNull( message, right ); }
		else { assertNotNull( right ); }

		if ( !(left instanceof java.util.Date ) ) { fail( message ); }
		if ( !(right instanceof java.util.Date ) ) { fail( message ); }

		assertEquals( message, left.toString(), right.toString() );
	}
	
}

