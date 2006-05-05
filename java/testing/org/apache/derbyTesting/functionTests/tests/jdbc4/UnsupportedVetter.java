 /**
 * Derby - org.apache.derbyTesting.functionTests.tests.jdbc4.UnsupportedVetter
 *
 * Copyright 2006 The Apache Software Foundation or its licensors, as
 * applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import java.io.*;
import java.sql.*;
import javax.sql.*;

import java.lang.reflect.*;
import java.util.*;
import junit.framework.*;

import java.net.URL;

import org.apache.derbyTesting.functionTests.util.BaseJDBCTestCase;
import org.apache.derbyTesting.functionTests.util.TestUtil;

/**
 * JUnit test which checks that only expected methods throw SQLFeatureNotSupporteException.
 */
public class UnsupportedVetter	extends BaseJDBCTestCase
{
	/////////////////////////////////////////////////////////////
	//
	//	CONSTANTS
	//
	/////////////////////////////////////////////////////////////

	public	static	final	String	SQL_PACKAGE_NAME = "java.sql";
	
	/////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	/////////////////////////////////////////////////////////////

	//
	// Table of methods which are allowed to raise
	// SQLFeatureNotSupportedException. Derived from the 1.6 Javadoc.
	//
	private	static	Exclusions[]	rawExcludables = new Exclusions[]
		{
		    new Exclusions
		    (
				java.sql.Connection.class,
				new MD[]
				{
						new MD( "createArray", new Class[] { String.class, Object[].class } ),
						new MD( "createNClob", new Class[] { } ),
						new MD( "createSQLXML", new Class[] { } ),
						new MD( "createStruct", new Class[] { String.class, Object[].class } ),
						new MD( "getTypeMap", new Class[] { } ),
						new MD( "prepareStatement", new Class[] { String.class, int[].class } ),
						new MD( "prepareStatement", new Class[] { String.class, String[].class } ),
						new MD( "setTypeMap", new Class[] { Map.class } ),
						} ),
		    new Exclusions
		    (
				java.sql.Statement.class,
				new MD[]
				{
						new MD( "cancel", new Class[] { } ),
				    new MD( "execute", new Class[] { String.class, int[].class } ),
						new MD( "execute", new Class[] { String.class, String[].class } ),
						new MD( "executeUpdate", new Class[] { String.class, int[].class } ),
						new MD( "executeUpdate", new Class[] { String.class, String[].class } )
						} )
		    ,

		    new Exclusions 
			(
				java.sql.PreparedStatement.class,
				new MD[]
				{
					new MD( "setArray", new Class[] { int.class, java.sql.Array.class } ),
						new MD( "setNCharacterStream", new Class[] { int.class, java.io.Reader.class, long.class } ),
						new MD( "setNClob", new Class[] { int.class, NClob.class } ),
						new MD( "setNClob", new Class[] { int.class, java.io.Reader.class, long.class } ),
						new MD( "setNString", new Class[] { int.class, String.class } ),
						new MD( "setRef", new Class[] { int.class, Ref.class } ),
						new MD( "setRowId", new Class[] { int.class, RowId.class } ),
						new MD( "setSQLXML", new Class[] { int.class, SQLXML.class } ),
					    new MD( "setURL", new Class[] { int.class, URL.class } ),
					    new MD( "setNull", new Class[] { int.class, int.class, String.class } ),
					    new MD( "setUnicodeStream", new Class[] { int.class, InputStream.class, int.class } ),
						} ),
			new Exclusions
			(
			    java.sql.CallableStatement.class,
				new MD[]
				{
					new MD( "getArray", new Class[] { int.class } ),
					new MD( "getArray", new Class[] { String.class } ),
					new MD( "getBigDecimal", new Class[] { String.class } ),
					new MD( "getBoolean", new Class[] { String.class } ),
					new MD( "getBlob", new Class[] { String.class } ),
					new MD( "getBoolean", new Class[] { String.class } ),
					new MD( "getByte", new Class[] { String.class } ),
					new MD( "getBytes", new Class[] { String.class } ),
					new MD( "getCharacterStream", new Class[] { String.class } ),
					new MD( "getClob", new Class[] { String.class } ),
					new MD( "getDate", new Class[] { String.class } ),
					new MD( "getDate", new Class[] { String.class, Calendar.class } ),
					new MD( "getDouble", new Class[] { String.class } ),
					new MD( "getFloat", new Class[] { String.class } ),
					new MD( "getInt", new Class[] { String.class } ),
					new MD( "getLong", new Class[] { String.class } ),
					new MD( "getNCharacterStream", new Class[] { int.class } ),
					new MD( "getNCharacterStream", new Class[] { String.class } ),
					new MD( "getNClob", new Class[] { int.class } ),
					new MD( "getNClob", new Class[] { String.class } ),
					new MD( "getNString", new Class[] { int.class } ),
					new MD( "getNString", new Class[] { String.class } ),
					new MD( "getObject", new Class[] { String.class } ),
					new MD( "getRef", new Class[] { int.class } ),
					new MD( "getRef", new Class[] { String.class } ),
					new MD( "getRowId", new Class[] { int.class } ),
					new MD( "getRowId", new Class[] { String.class } ),
					new MD( "getShort", new Class[] { String.class } ),
					new MD( "getSQLXML", new Class[] { int.class } ),
					new MD( "getSQLXML", new Class[] { String.class } ),
					new MD( "getString", new Class[] { String.class } ),
					new MD( "getTime", new Class[] { String.class } ),
					new MD( "getTime", new Class[] { String.class, java.util.Calendar.class } ),
					new MD( "getTimestamp", new Class[] { String.class } ),
					new MD( "getTimestamp", new Class[] { String.class, java.util.Calendar.class } ),
					new MD( "getURL", new Class[] { int.class } ),
					new MD( "getURL", new Class[] { String.class } ),
						new MD( "registerOutParameter", new Class[] { String.class, int.class } ),
						new MD( "registerOutParameter", new Class[] { String.class, int.class, int.class } ),
						new MD( "registerOutParameter", new Class[] { String.class, int.class, String.class } ),
						new MD( "registerOutParameter", new Class[] { int.class, int.class, String.class } ),
						new MD( "setArray", new Class[] { int.class, java.sql.Array.class } ),
						new MD( "setAsciiStream", new Class[] { String.class, java.io.InputStream.class, int.class } ),
						new MD( "setBigDecimal", new Class[] { String.class, java.math.BigDecimal.class } ),
						new MD( "setBinaryStream", new Class[] { String.class, java.io.InputStream.class, int.class } ),
						new MD( "setBlob", new Class[] { String.class, java.io.InputStream.class, long.class } ),
						new MD( "setBlob", new Class[] { String.class, Blob.class } ),
						new MD( "setBoolean", new Class[] { String.class, boolean.class } ),
						new MD( "setByte", new Class[] { String.class, byte.class } ),
						new MD( "setBytes", new Class[] { String.class, byte[].class } ),
						new MD( "setCharacterStream", new Class[] { String.class, java.io.Reader.class, int.class } ),
						new MD( "setClob", new Class[] { String.class, java.io.Reader.class, long.class } ),
						new MD( "setClob", new Class[] { String.class, Clob.class } ),
						new MD( "setDate", new Class[] { String.class, java.sql.Date.class } ),
						new MD( "setDate", new Class[] { String.class, java.sql.Date.class, Calendar.class } ),
						new MD( "setDouble", new Class[] { String.class, double.class} ),
						new MD( "setFloat", new Class[] { String.class, float.class } ),
						new MD( "setInt", new Class[] { String.class, int.class } ),
						new MD( "setLong", new Class[] { String.class, long.class } ),
						new MD( "setNCharacterStream", new Class[] { int.class, java.io.Reader.class, long.class } ),
						new MD( "setNCharacterStream", new Class[] { String.class, java.io.Reader.class, long.class } ),
						new MD( "setNClob", new Class[] { int.class, java.io.Reader.class, long.class } ),
						new MD( "setNClob", new Class[] { int.class, NClob.class } ),
						new MD( "setNClob", new Class[] { String.class, java.io.Reader.class, long.class } ),
						new MD( "setNClob", new Class[] { String.class, NClob.class } ),
						new MD( "setNString", new Class[] { int.class, String.class } ),
						new MD( "setNString", new Class[] { String.class, String.class } ),
						new MD( "setNull", new Class[] { String.class, int.class } ),
						new MD( "setNull", new Class[] { String.class, int.class, String.class } ),
						new MD( "setObject", new Class[] { String.class, Object.class } ),
						new MD( "setObject", new Class[] { String.class, Object.class, int.class } ),
						new MD( "setObject", new Class[] { String.class, Object.class, int.class, int.class } ),
						new MD( "setRef", new Class[] { int.class, Ref.class } ),
						new MD( "setRowId", new Class[] { int.class, RowId.class } ),
						new MD( "setRowId", new Class[] { String.class, RowId.class } ),
						new MD( "setSQLXML", new Class[] { int.class, SQLXML.class } ),
						new MD( "setSQLXML", new Class[] { String.class, SQLXML.class } ),
						new MD( "setShort", new Class[] { String.class, short.class } ),
						new MD( "setString", new Class[] { String.class, String.class } ),
						new MD( "setTime", new Class[] { String.class, Time.class } ),
						new MD( "setTime", new Class[] { String.class, Time.class, Calendar.class } ),
						new MD( "setTimestamp", new Class[] { String.class, Timestamp.class } ),
						new MD( "setTimestamp", new Class[] { String.class, Timestamp.class, Calendar.class } ),
						new MD( "setURL", new Class[] { int.class, URL.class } ),
						new MD( "setURL", new Class[] { String.class, URL.class } )
				}
			),
		    new Exclusions
		    (
				java.sql.ResultSet.class,
				new MD[]
				{
				    new MD( "getNCharacterStream", new Class[] { int.class } ),
						new MD( "getNCharacterStream", new Class[] { String.class } ),
						new MD( "getNString", new Class[] { int.class } ),
						new MD( "getNString", new Class[] { String.class } ),
						new MD( "getURL", new Class[] { int.class } ),
						new MD( "getURL", new Class[] { String.class } ),
						new MD( "getArray", new Class[] { int.class } ),
						new MD( "getArray", new Class[] { String.class } ),
						new MD( "getNClob", new Class[] { int.class } ),
						new MD( "getNClob", new Class[] { String.class } ),
						new MD( "getRef", new Class[] { int.class } ),
						new MD( "getRef", new Class[] { String.class } ),
						new MD( "getRowId", new Class[] { int.class } ),
						new MD( "getRowId", new Class[] { String.class } ),
						new MD( "getSQLXML", new Class[] { int.class } ),
						new MD( "getSQLXML", new Class[] { String.class } ),
						new MD( "getUnicodeStream", new Class[] { int.class } ),
						new MD( "getUnicodeStream", new Class[] { String.class } ),
						new MD( "refreshRow", new Class[] { } ),
						new MD( "updateArray", new Class[] { int.class, java.sql.Array.class } ),
						new MD( "updateArray", new Class[] { String.class, java.sql.Array.class } ),
						new MD( "updateNCharacterStream", new Class[] { int.class, java.io.Reader.class, int.class } ),
						new MD( "updateNCharacterStream", new Class[] { String.class, java.io.Reader.class, int.class } ),
						new MD( "updateNClob", new Class[] { int.class, NClob.class } ),
						new MD( "updateNClob", new Class[] { String.class, NClob.class } ),
						new MD( "updateNString", new Class[] { int.class, String.class } ),
						new MD( "updateNString", new Class[] { String.class, String.class } ),
						new MD( "updateRef", new Class[] { int.class, Ref.class } ),
						new MD( "updateRef", new Class[] { String.class, Ref.class } ),
						new MD( "updateRowId", new Class[] { int.class, RowId.class } ),
						new MD( "updateRowId", new Class[] { String.class, RowId.class } ),
						new MD( "updateSQLXML", new Class[] { int.class, SQLXML.class } ),
						new MD( "updateSQLXML", new Class[] { String.class, SQLXML.class } )
						} ),
			//
			// Lance Andersen, spec lead for JDBC4, says:
			// If you support a datatype, then you have to implement
			// all of its methods.
			//
			//		    new Exclusions
			//		    (
			//				java.sql.Blob.class,
			//				new MD[]
			//				{
			//				    new MD( "getBinaryStream", new Class[] { long.class, long.class } ),
			//						new MD( "setBinaryStream", new Class[] { long.class } ),
			//						new MD( "setBytes", new Class[] { long.class, byte[].class } ),
			//						new MD( "setBytes", new Class[] { long.class, byte[].class, int.class, int.class } ),
			//						new MD( "truncate", new Class[] { long.class } )
			//						} ),
			//		    new Exclusions
			//		    (
			//				java.sql.Clob.class,
			//				new MD[]
			//				{
			//				    new MD( "getCharacterStream", new Class[] { long.class, long.class } ),
			//						new MD( "setAsciiStream", new Class[] { long.class } ),
			//						new MD( "setCharacterStream", new Class[] { long.class } ),
			//						new MD( "setString", new Class[] { long.class, String.class } ),
			//						new MD( "setString", new Class[] { long.class, String.class, int.class, int.class } ),
			//						new MD( "truncate", new Class[] { long.class } )
			//						} )
		};

	//
	// This is the Hashtable where we keep the exclusions.
	//
	private	static	Hashtable< Class, HashSet<Method> >	excludableMap;
	
	/////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTOR
	//
	/////////////////////////////////////////////////////////////

    /**
     * Creates a new instance.
     */
    public UnsupportedVetter() { super("UnsupportedVetter"); }

	/////////////////////////////////////////////////////////////
	//
	//	ENTRY POINTS
	//
	/////////////////////////////////////////////////////////////

	/**
	 * <p>
	 * Find all methods in this framework which raise SQLFeatureNotSupportedException.
	 * </p>
	 */
	public	void	testSupportedMethods()
		throws Exception
	{
		CONFIG.setVerbosity( true );

		// Build map of interfaces to their methods which may raise SQLFeatureNotSupportedException.
		initializeExcludableMap();

		HashSet<String>	unsupportedList = new HashSet<String>();
		HashSet<String>	notUnderstoodList = new HashSet<String>();

		vetDataSource( unsupportedList, notUnderstoodList );
		vetConnectionPooledDataSource( unsupportedList, notUnderstoodList );
		vetXADataSource( unsupportedList, notUnderstoodList );

		//
		// Print methods which behave unexpectedly.
		//
		printUnsupportedList( unsupportedList );
		printNotUnderstoodList( notUnderstoodList );

		assertEquals
			( "These methods should not raise SQLFeatureNotSupportedException.",
			  0, unsupportedList.size() );
		assertEquals
			( "These methods raise exceptions we don't understand.",
			  0, notUnderstoodList.size() );
	}

	//
	// Find all the objects inside the DataSource and vet them.
	//
	private	void	vetDataSource
		( HashSet<String> unsupportedList, HashSet<String> notUnderstoodList )
		throws Exception
	{
		DataSource			ds = getDataSource();
		Connection			conn = ds.getConnection();

		vetObject( ds, unsupportedList, notUnderstoodList );

		connectionWorkhorse( conn, unsupportedList, notUnderstoodList );
	}

	//
	// Find all the objects inside the ConnectionPooledDataSource and vet them.
	//
	private	void	vetConnectionPooledDataSource
		( HashSet<String> unsupportedList, HashSet<String> notUnderstoodList )
		throws Exception
	{
		ConnectionPoolDataSource	ds = getConnectionPoolDataSource();
		PooledConnection			pc = ds.getPooledConnection
			(CONFIG.getUserName(), CONFIG.getUserPassword());
		Connection					conn = pc.getConnection();

		vetObject( ds, unsupportedList, notUnderstoodList );
		vetObject( pc, unsupportedList, notUnderstoodList );

		connectionWorkhorse( conn, unsupportedList, notUnderstoodList );
	}

	//
	// Find all the objects inside the XADataSource and vet them.
	//
	private	void	vetXADataSource
		( HashSet<String> unsupportedList, HashSet<String> notUnderstoodList )
		throws Exception
	{
		XADataSource				ds = getXADataSource();
		XAConnection				xaconn = ds.getXAConnection
			(CONFIG.getUserName(), CONFIG.getUserPassword());
		Connection					conn = xaconn.getConnection();

		vetObject( ds, unsupportedList, notUnderstoodList );
		vetObject( xaconn, unsupportedList, notUnderstoodList );

		connectionWorkhorse( conn, unsupportedList, notUnderstoodList );
	}

	//
	// Find all the methods for java.sql objects in the Connection which raise
	// SQLFeatureNotSupportedException.
	//
	private	void	connectionWorkhorse
		( Connection conn, HashSet<String> unsupportedList, HashSet<String> notUnderstoodList  )
		throws Exception
	{
		vetSavepoint( conn, unsupportedList, notUnderstoodList );
		vetLargeObjects( conn, unsupportedList, notUnderstoodList );
		
		DatabaseMetaData	dbmd = conn.getMetaData();
		PreparedStatement	ps = conn.prepareStatement
			( "select * from sys.systables where tablename = ?" );

		ps.setString( 1, "foo" );

		ParameterMetaData	parameterMetaData = ps.getParameterMetaData();
		ResultSet			rs = ps.executeQuery();
		ResultSetMetaData	rsmd = rs.getMetaData();
        Statement			stmt = conn.createStatement();

        CallableStatement	cs =
            conn.prepareCall("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0)");
        ParameterMetaData	csmd = cs.getParameterMetaData();

		//
		// The vetObject() method calls all of the methods in these objects
		// in a deterministic order, calling the close() method last.
		// Inspect these objects in an order which respects the fact that
		// the objects are closed as a result of calling vetObject().
		//
		vetObject( dbmd, unsupportedList, notUnderstoodList );
		vetObject( stmt, unsupportedList, notUnderstoodList );
		vetObject( csmd, unsupportedList, notUnderstoodList );
		vetObject( cs, unsupportedList, notUnderstoodList );
		vetObject( rsmd, unsupportedList, notUnderstoodList );
		vetObject( rs, unsupportedList, notUnderstoodList );
		vetObject( parameterMetaData, unsupportedList, notUnderstoodList );
		vetObject( ps, unsupportedList, notUnderstoodList );
		vetObject( conn, unsupportedList, notUnderstoodList );

		// No need to close the objects. They were closed by vetObject().
	}
	
	//
	// Examine Savepoints.
	//
	private	void	vetSavepoint
		( Connection conn, HashSet<String> unsupportedList, HashSet<String> notUnderstoodList  )
		throws Exception
	{
        conn.setAutoCommit( false );

        Savepoint			sp = conn.setSavepoint();
		
		vetObject( sp, unsupportedList, notUnderstoodList );
		
        conn.releaseSavepoint(sp);
	}

	//
	// Examine BLOBs and CLOBs.
	//
	private	void	vetLargeObjects
		( Connection conn, HashSet<String> unsupportedList, HashSet<String> notUnderstoodList  )
		throws Exception
	{
        Statement		stmt = conn.createStatement();

        stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, " +
                     "b BLOB(10), c CLOB(10))");
        stmt.execute("INSERT INTO t (id, b, c) VALUES (1, "+
                     "CAST (" + TestUtil.stringToHexLiteral("101010001101") +
                     "AS BLOB(10)), CAST ('hello' AS CLOB(10)))");

        ResultSet rs = stmt.executeQuery("SELECT id, b, c FROM t");

        rs.next();

        Blob		blob = rs.getBlob(2);
        Clob		clob = rs.getClob(3);

		vetObject( blob, unsupportedList, notUnderstoodList );
		vetObject( clob, unsupportedList, notUnderstoodList );

        stmt.close();
        conn.rollback();
	}


	/////////////////////////////////////////////////////////////
	//
	//	MINIONS
	//
	/////////////////////////////////////////////////////////////

	//
	// Initialize the hashtable of methods which are allowed to raise
	// SQLFeatureNotSupportedException.
	//
	private	void	initializeExcludableMap()
		throws Exception
	{
		excludableMap = new Hashtable< Class, HashSet<Method> >();
		
		int		count = rawExcludables.length;

		for ( int i = 0; i < count; i++ )
		{
			Exclusions		exclusions = rawExcludables[ i ];
			Class			iface = exclusions.getInterface();
			MD[]			mds = exclusions.getExcludedMethods();
			int				exclusionCount = mds.length;
			HashSet<Method>	excludedMethodSet = new HashSet<Method>();

			for ( int j = 0; j < exclusionCount; j++ )
			{
				MD		md = mds[ j ];

				Method	method = iface.getMethod( md.getMethodName(), md.getArgTypes() );

				if ( method == null ) { fail( "Unknown method: " + md.getMethodName() ); }

				excludedMethodSet.add( method );
			}

			excludableMap.put( iface, excludedMethodSet );
		}
	}

	//
	// Find all the methods from java.sql interfaces which are implemented by an object
	// and which raise SQLFeatureNotSupportedException.
	//
	private	void	vetObject
		( Object candidate, HashSet<String> unsupportedList, HashSet<String> notUnderstoodList )
		throws Exception
	{
		Class		myClass = candidate.getClass();

		vetInterfaces( candidate, myClass, unsupportedList, notUnderstoodList );
	}

	//
	// Find all the java.sql interfaces implemented by a class and find
	// the methods in those interfaces which raise
	// SQLFeatureNotSupportedException when called on the passed-in candidate object.
	//
	private	void	vetInterfaces
		( Object candidate, Class myClass,
		  HashSet<String> unsupportedList, HashSet<String> notUnderstoodList )
		throws Exception
	{
		Class		superClass = myClass.getSuperclass();

		if ( superClass != null )
		{ vetInterfaces( candidate, superClass, unsupportedList, notUnderstoodList ); }

		//
		// The contract for Class.getInterfaces() states that the interfaces
		// come back in a deterministic order, namely, in the order that
		// they were declared in the "extends" clause.
		//
		Class<?>[]	interfaces = myClass.getInterfaces();
		int			interfaceCount = interfaces.length;

		for ( int i = 0; i < interfaceCount; i++ )
		{
			Class<?>	iface = interfaces[ i ];

			if ( iface.getPackage().getName().equals( SQL_PACKAGE_NAME ) )
			{
				vetInterfaceMethods( candidate, iface, unsupportedList, notUnderstoodList );
			}

			vetInterfaces( candidate, iface, unsupportedList, notUnderstoodList );
		}
	}

	//
	// Examine all the methods in an interface to determine which ones
	// raise SQLFeatureNotSupportedException.
	//
	private	void	vetInterfaceMethods
		( Object candidate, Class iface,
		  HashSet<String> unsupportedList, HashSet<String> notUnderstoodList )
		throws Exception
	{
		Method[]	methods = sortMethods( iface );
		int			methodCount = methods.length;

		for ( int i = 0; i < methodCount; i++ )
		{
			Method	method = methods[ i ];

			vetMethod( candidate, iface, method, unsupportedList, notUnderstoodList );
		}
	}

	//
	// Return the methods of an interface in a deterministic
	// order. Class.getMethods() does not do us this favor.
	//
	private	Method[]	sortMethods( Class iface )
		throws Exception
	{
		Method[]			raw = iface.getMethods();
		int					count = raw.length;
		Method[]			cooked = new Method[ count ];
		MethodSortable[]	sortables = new MethodSortable[ count ];

		for ( int i = 0; i < count; i++ ) { sortables[ i ] = new MethodSortable( raw[ i ] ); }

		Arrays.sort( sortables );

		for ( int i = 0; i < count; i++ ) { cooked[ i ] = sortables[ i ].getMethod(); }

		return cooked;
	}

	//
	// Examine a single method to see if it raises SQLFeatureNotSupportedException.
	//
	private	void	vetMethod
		( Object candidate, Class iface, Method method,
		  HashSet<String> unsupportedList, HashSet<String> notUnderstoodList )
		throws Exception
	{
		try {
			method.invoke( candidate, getNullArguments( method.getParameterTypes() ) );

			// it's ok for the method to succeed
		}
		catch (Throwable e)
		{
			if ( e instanceof InvocationTargetException )
			{
				Throwable	cause = e.getCause();
				
				if ( cause instanceof SQLFeatureNotSupportedException )
				{
					boolean	isExcludable = isExcludable( method );

					if ( !isExcludable )
					{
					    StackTraceElement[] stack = 
						cause.getStackTrace();
						int i = 0;
						while(i < stack.length && !stack[i].getMethodName().
							  equals("notImplemented")){
								++i;
							}
							while(i < stack.length && stack[i].getMethodName().
								  equals("notImplemented")){
								++i;
							}
							if (i == stack.length) {
								//cause.printStackTrace();
							}
				     
							unsupportedList.add( candidate.getClass().getName() + ": " + method + "@" + (i==stack.length?"no source":cause.getStackTrace()[i]));
					} else {

					}
				}
				else if ( cause instanceof SQLException )
				{
					// swallow other SQLExceptions, caused by bogus args
				}
				else if ( cause instanceof NullPointerException )
				{
					// swallow other NPEs, caused by bogus args
				}
				else if ( cause instanceof ArrayIndexOutOfBoundsException )
				{
					// swallow these, caused by bogus args
				}
				else
				{
					notUnderstoodList.add
						( candidate.getClass().getName() + " " + method + " raises " + cause );
				}
				
			}
		}
	}

	//
	// Returns true if this method is allowed to raise SQLFeatureNotSupportedException.
	//
	private	boolean	isExcludable(Method method )
		throws Exception
	{
		Class				iface = method.getDeclaringClass();
		HashSet<Method>		excludableMethods = excludableMap.get( iface );

		if ( excludableMethods == null )
		{
			return false;
		}

		return excludableMethods.contains( method );
	}
	
    /**
     * Takes an array of classes and returns an array of objects with
     * null values compatible with the classes. Helper method for
     * converting a parameter list to an argument list.
     *
     * @param params a <code>Class[]</code> value
     * @return an <code>Object[]</code> value
     */
    private Object[] getNullArguments(Class[] params) {
        Object[] args = new Object[params.length];
        for (int i = 0; i < params.length; i++) {
            args[i] = getNullValueForType(params[i]);
        }
        return args;
    }

    /**
     * Returns a null value compatible with the class. For instance,
     * return <code>Boolean.FALSE</code> for primitive booleans, 0 for
     * primitive integers and <code>null</code> for non-primitive
     * types.
     *
     * @param type a <code>Class</code> value
     * @return a null value
     */
    private Object getNullValueForType(Class type)
	{
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == Boolean.TYPE) {
            return Boolean.FALSE;
        }
        if (type == Character.TYPE) {
            return new Character((char) 0);
        }
        if (type == Byte.TYPE) {
            return new Byte((byte) 0);
        }
        if (type == Short.TYPE) {
            return new Short((short) 0);
        }
        if (type == Integer.TYPE) {
            return new Integer(0);
        }
        if (type == Long.TYPE) {
            return new Long(0L);
        }
        if (type == Float.TYPE) {
            return new Float(0f);
        }
        if (type == Double.TYPE) {
            return new Double(0d);
        }
        fail("Don't know how to handle type " + type);
        return null;            // unreachable statement
    }

	// debug print the list of methods which throw SQLFeatureNotSupportedException
	private	void	printUnsupportedList( HashSet<String> unsupportedList )
	{
		int			count = unsupportedList.size();

		if ( count == 0 ) { return; }

		println( "--------------- UNSUPPORTED METHODS ------------------" );
		println( "--" );

		String[]	result = new String[ count ];

		unsupportedList.toArray( result );
		Arrays.sort( result );

		for ( int i = 0; i < count; i++ )
		{
			println( result[ i ] );
		}
	}

	// Debug print the list of method failures which we don't understand
	private	void	printNotUnderstoodList( HashSet<String> notUnderstoodList )
	{
		int			count = notUnderstoodList.size();

		if ( count == 0 ) { return; }

		println( "\n\n" );
		println( "--------------- NOT UNDERSTOOD METHODS ------------------" );
		println( "--" );

		String[]	result = new String[ count ];

		notUnderstoodList.toArray( result );
		Arrays.sort( result );

		for ( int i = 0; i < count; i++ )
		{
			println( result[ i ] );
		}
	}

	/////////////////////////////////////////////////////////////
	//
	//	INNER CLASSES
	//
	/////////////////////////////////////////////////////////////

	/**
	 * <p>
	 * Method descriptor. We abbreviate the name of this class to make
	 * arrays of these declarations compact and readable.
	 * </p>
	 */
	public	static	final	class	MD
	{
		private	String	_methodName;
		private	Class[]	_argTypes;

		/** Construct from methodName and argument types. */
		public	MD( String methodName, Class[] argTypes )
		{
			_methodName = methodName;
			_argTypes = argTypes;
		}

		/** Get the name of this method. */
		public	String	getMethodName() { return _methodName; }

		/** Get the types of the method's arguments */
		public	Class[]	getArgTypes() { return _argTypes; }
	}

	/**
	 * <p>
	 * Describes all of the methods for an interface which are allowed
	 * to raise SQLFeatureNotSupportedException.
	 * </p>
	 */
	public	static	final	class	Exclusions
	{
		private	Class	_class;
		private	MD[]	_excludedMethods;

		/** Construct from the interface and descriptors for the methods which
		 are allowed to raise SQLFeatureNotSupportedException */
		public	Exclusions( Class theInterface, MD[] excludedMethods )
		{
			_class = theInterface;
			_excludedMethods = excludedMethods;
		}
		
		/** Get the interface. */
		public	Class	getInterface() { return _class; }

		/** Get descriptors for the methods which may raise
			SQLFeatureNotSupportedException. */
		public	MD[]	getExcludedMethods() { return _excludedMethods; }
	}

	/**
	 * <p>
	 * Used for sorting methods, which don't come back from Class.getMethods()
	 * in a deterministic order. For extra credit, we put the close() method at
	 * the end of the sort order so that, when we invoke the sorted methods, we
	 * don't accidentally invalidate the receiver.
	 * </p>
	 */
	public	static	final	class	MethodSortable	implements	Comparable
	{
		private	Method	_method;

		/** Conjure out of a Method */
		public	MethodSortable( Method method ) { _method = method; }

		/** Get the wrapped Method */
		public	Method	getMethod() { return _method; }

		//////////////////////////////////////////////////
		//
		//	Comparable BEHAVIOR
		//
		//////////////////////////////////////////////////

		public	int	compareTo( Object other )
		{
			MethodSortable	that = (MethodSortable) other;
			boolean			thisIsClose = this.isCloseMethod();
			boolean			thatIsClose = that.isCloseMethod();

			// throw the close() method to the end of the sort order
			if ( thisIsClose )
			{
				if ( thatIsClose ) { return 0; }
				else { return 1; }
			}
			else if ( thatIsClose ) { return -1; }

			return this.toString().compareTo( that.toString() );
		}

		//////////////////////////////////////////////////
		//
		//	Object OVERRIDES
		//
		//////////////////////////////////////////////////

		public	String	toString() { return _method.toString(); }
		
		//////////////////////////////////////////////////
		//
		//	MINIONS
		//
		//////////////////////////////////////////////////

		// Returns true if the wrapped method is close().
		private	boolean	isCloseMethod()
		{
			return ( toString().startsWith( "close()" ) );
		}
		
	}
	
}

