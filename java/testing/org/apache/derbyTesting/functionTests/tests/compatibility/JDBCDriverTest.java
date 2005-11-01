/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.compatibility.JDBCDriverTest

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
/**
 * <p>
 * This JUnit test verifies the compatibility of Derby clients and
 * servers across Derby version levels and supported VMs.
 * </p>
 *
 * @author Rick
 */

package org.apache.derbyTesting.functionTests.tests.compatibility;

import java.io.*;
import java.math.*;
import java.sql.*;
import java.util.*;

import junit.framework.*;

import org.apache.derbyTesting.functionTests.util.DerbyJUnitTest;

public	class	JDBCDriverTest	extends	DerbyJUnitTest
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

	//
	// These are properties for the Derby connection URL.
	//
	private	static	final			String	VERSION_PROPERTY = "java.version";
	private	static	final			String	SERVER_URL = "jdbc:derby://localhost:1527/";
	private	static	final			String	CREATE_PROPERTY = "create=true";

	private	static	final			String	ALL_TYPES_TABLE = "allTypesTable";
	private	static	final			String	KEY_COLUMN = "keyCol";
	
	private	static	final			int		EXPECTED_CLIENT_COUNT = 1;

	//
	// Indexes into the array of client-specific strings. E.g., DB2JCC_CLIENT,
	// DERBY_CLIENT, and EMBEDDED_CLIENT.
	//
	private	static	final			int		DATABASE_URL = 0;
	private	static	final			int		DRIVER_NAME = DATABASE_URL + 1;

	//
	// Data values to be stuffed into columns of ALL_TYPES_TABLE.
	//
	private	static	final			byte[]	SAMPLE_BYTES =
		new byte[] { (byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5 };
	private	static	final			String	SAMPLE_STRING = "hello";

	//
	// These funny constants are defined this way to make the salient
	// facts of the COERCIONS table leap out at you.
	//
	private	static	final			boolean	Y = true;
	private	static	final			boolean	_ = false;

	// indexed by DATABASE_URL and DRIVER_NAME
	private	static	final	String[]	DB2JCC_CLIENT =
	{
		"jdbc:derby:net://localhost:1527/",
		"com.ibm.db2.jcc.DB2Driver"
	};
	private	static	final	String[]	DERBY_CLIENT =
	{
		"jdbc:derby://localhost:1527/",
		"org.apache.derby.jdbc.ClientDriver"
	};
	private	static	final	String[]	EMBEDDED_CLIENT =
	{
		"jdbc:derby:",
		"org.apache.derby.jdbc.EmbeddedDriver"
	};

	private	static	final	String[][]	LEGAL_CLIENTS =
	{
		DB2JCC_CLIENT,
		DERBY_CLIENT,
		EMBEDDED_CLIENT
	};
	
	private	static	final	String	DEFAULT_USER_NAME = "APP";
	private	static	final	String	DEFAULT_PASSWORD = "APP";

	// Supported versions of the db2jcc client.
	private	static	final	Version	IBM_2_4 = new Version( 2, 4 );

	// Supported versions of Derby.
	private	static	final	Version	DRB_10_0 = new Version( 10, 0 );
	private	static	final	Version	DRB_10_1 = new Version( 10, 1 );
	private	static	final	Version	DRB_10_2 = new Version( 10, 2 );

	// Supported VM versions.
	private	static	final	Version	VM_1_3 = new Version( 1, 3 );
	private	static	final	Version	VM_1_4 = new Version( 1, 4 );
	private	static	final	Version	VM_1_5 = new Version( 1, 5 );

	//
	// This table declares the datatypes supported by Derby and the earliest
	// versions of the Derby and the db2jcc client which support these
	// datatypes.
	//
	// If you add a type to this table, make sure you add a corresponding
	// column to the following row table. Also add a corresponding row to the
	// COERCIONS table.
	//
	private	static	final	TypeDescriptor[]	ALL_TYPES =
	{
		// 10.0 types
		
		new TypeDescriptor
		( Types.BIGINT,			"bigint",						IBM_2_4,	DRB_10_0,	VM_1_3 ),
		new TypeDescriptor
		( Types.BLOB,			"blob",							IBM_2_4,	DRB_10_0,	VM_1_3 ),
		new TypeDescriptor
		( Types.CHAR,			"char(5)",						IBM_2_4,	DRB_10_0,	VM_1_3 ),
		new TypeDescriptor
		( Types.BINARY,			"char(5) for bit data",			IBM_2_4,	DRB_10_0,	VM_1_3 ),
		new TypeDescriptor
		( Types.CLOB,			"clob",							IBM_2_4,	DRB_10_0,	VM_1_3 ),
		new TypeDescriptor
		( Types.DATE,			"date",							IBM_2_4,	DRB_10_0,	VM_1_3 ),
		new TypeDescriptor
		( Types.DECIMAL,		"decimal",						IBM_2_4,	DRB_10_0,	VM_1_3 ),
		new TypeDescriptor
		( Types.DOUBLE,			"double",						IBM_2_4,	DRB_10_0,	VM_1_3 ),
		new TypeDescriptor
		( Types.DOUBLE,			"double precision",				IBM_2_4,	DRB_10_0,	VM_1_3 ),
		new TypeDescriptor
		( Types.REAL,			"float(23)",					IBM_2_4,	DRB_10_0,	VM_1_3 ),
		new TypeDescriptor
		( Types.DOUBLE,			"float",						IBM_2_4,	DRB_10_0,	VM_1_3 ),
		new TypeDescriptor
		( Types.INTEGER,		"integer",						IBM_2_4,	DRB_10_0,	VM_1_3 ),
		new TypeDescriptor
		( Types.LONGVARCHAR,	"long varchar",					IBM_2_4,	DRB_10_0,	VM_1_3 ),
		new TypeDescriptor
		( Types.LONGVARBINARY,	"long varchar for bit data",	IBM_2_4,	DRB_10_0,	VM_1_3 ),
		new TypeDescriptor
		( Types.NUMERIC,		"numeric",						IBM_2_4,	DRB_10_0,	VM_1_3 ),
		new TypeDescriptor
		( Types.REAL,			"real",							IBM_2_4,	DRB_10_0,	VM_1_3 ),
		new TypeDescriptor
		( Types.SMALLINT,		"smallint",						IBM_2_4,	DRB_10_0,	VM_1_3 ),
		new TypeDescriptor
		( Types.TIME,			"time",							IBM_2_4,	DRB_10_0,	VM_1_3 ),
		new TypeDescriptor
		( Types.TIMESTAMP,		"timestamp",					IBM_2_4,	DRB_10_0,	VM_1_3 ),
		new TypeDescriptor
		( Types.VARCHAR,		"varchar(5)",					IBM_2_4,	DRB_10_0,	VM_1_3 ),
		new TypeDescriptor
		( Types.VARBINARY,		"varchar(5) for bit data",		IBM_2_4,	DRB_10_0,	VM_1_3 ),
	};

	//
	// This table needs to have the same number of entries as ALL_TYPES.
	// The testSanity() test case enforces this at run time.
	//
	private	static	final	Object[]	ROW_1 =
	{
		new Long( 1L ),
		new MyBlob( SAMPLE_BYTES ),
		SAMPLE_STRING,
		SAMPLE_BYTES,
		new MyClob( SAMPLE_STRING ),
		new java.sql.Date( 1L ),
		new BigDecimal( 1.0 ),
		new Double( 1.0 ),
		new Double( 1.0 ),
		new Float( (float) 1.0 ),
		new Double( 1.0 ),
		new Integer( 1 ),
		SAMPLE_STRING,
		SAMPLE_BYTES,
		new BigDecimal( 1.0 ),
		new Float( (float) 1.0 ),
		new Short( (short) 1 ),
		new Time( 1L ),
		new Timestamp( 1L ),
		SAMPLE_STRING,
		SAMPLE_BYTES,
	};

	//
	// This table needs to have the same number of rows as ALL_TYPES.
	// Each row in this table needs to have the same number of columns as
	// rows in ALL_TYPES. The testSanity() test case enforces this at run time.
	// Note how the funny synonyms for true and false
	// make the salient facts of this table leap out at you.
	//
	// The ugly class name T_CN is an abbreviation which makes it possible to
	// squeeze this table onto a readable screen.
	//
	// Please read the introductory comment top-to-bottom. 'Y' means a coercion
	// is legal; '_' means it isn't.
	//
	private	static	final	T_CN[]	COERCIONS =
	{
		//												  B|B|C|B|C|D|D|D|R|I|L|L|N|R|S|T|T|V|V
		//												  I|L|H|I|L|A|E|O|E|N|O|O|U|E|M|I|I|A|A
		//												  G|O|A|N|O|T|C|U|A|T|N|N|M|A|A|M|M|R|R
		//												  I|B|R|A|B|E|I|B|L|E|G|G|E|L|L|E|E|C|B
		//												  N|-|-|R|-|-|M|L|-|G|V|V|R|-|L|-|S|H|I
		//												  T|-|-|Y|-|-|A|E|-|E|A|A|I|-|I|-|T|A|N
		//												  -|-|-|-|-|-|L|-|-|R|R|R|C|-|N|-|A|R|A
		//												  -|-|-|-|-|-|-|-|-|-|C|B|-|-|T|-|M|-|R
		//												  -|-|-|-|-|-|-|-|-|-|H|I|-|-|-|-|P|-|Y
		//												  -|-|-|-|-|-|-|-|-|-|A|N|-|-|-|-|-|-|-
		//												  -|-|-|-|-|-|-|-|-|-|R|A|-|-|-|-|-|-|-
		//												  -|-|-|-|-|-|-|-|-|-|-|R|-|-|-|-|-|-|-
		//												  -|-|-|-|-|-|-|-|-|-|-|Y|-|-|-|-|-|-|-
		new T_CN( Types.BIGINT, new boolean[]			{ Y,_,Y,_,_,_,_,Y,Y,Y,Y,_,Y,Y,Y,_,_,Y,_, } ),
		new T_CN( Types.BLOB, new boolean[] 			{ _,Y,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_, } ),
		new T_CN( Types.CHAR, new boolean[]				{ _,_,Y,_,_,_,_,_,_,_,Y,_,_,_,_,_,_,Y,_, } ),
		new T_CN( Types.BINARY, new boolean[]			{ _,_,_,Y,_,_,_,_,_,_,_,Y,_,_,_,_,_,_,Y, } ),
		new T_CN( Types.CLOB, new boolean[]				{ _,_,_,_,Y,_,_,_,_,_,_,_,_,_,_,_,_,_,_, } ),
		new T_CN( Types.DATE, new boolean[]				{ _,_,_,_,_,Y,_,_,_,_,_,_,_,_,_,_,_,_,_, } ),
		new T_CN( Types.DECIMAL, new boolean[]			{ Y,_,_,_,_,_,Y,Y,Y,Y,Y,_,Y,Y,Y,_,_,Y,_, } ),
		new T_CN( Types.DOUBLE, new boolean[]			{ Y,_,_,_,_,_,Y,Y,Y,Y,Y,_,Y,Y,Y,_,_,Y,_, } ),
		new T_CN( Types.REAL, new boolean[]				{ Y,_,Y,_,_,_,Y,Y,Y,Y,Y,_,Y,Y,Y,_,_,Y,_, } ),
		new T_CN( Types.INTEGER, new boolean[]			{ Y,_,Y,_,_,_,Y,Y,Y,Y,Y,_,Y,Y,Y,_,_,Y,_, } ),
		new T_CN( Types.LONGVARCHAR, new boolean[]		{ _,_,Y,_,_,_,_,_,_,_,Y,_,_,_,_,_,_,Y,_, } ),
		new T_CN( Types.LONGVARBINARY, new boolean[]	{ _,_,_,_,_,_,_,_,_,_,_,Y,_,_,_,_,_,_,Y, } ),
		new T_CN( Types.NUMERIC, new boolean[]			{ Y,_,Y,_,_,_,Y,Y,Y,Y,Y,_,Y,Y,Y,_,_,Y,_, } ),
		new T_CN( Types.REAL, new boolean[]				{ Y,_,Y,_,_,_,Y,Y,Y,Y,Y,_,Y,Y,Y,_,_,Y,_, } ),
		new T_CN( Types.SMALLINT, new boolean[]			{ Y,_,Y,_,_,_,Y,Y,Y,Y,Y,_,Y,Y,Y,_,_,Y,_, } ),
		new T_CN( Types.TIME, new boolean[]				{ _,_,_,_,_,_,_,_,_,_,_,_,_,_,_,Y,_,_,_, } ),
		new T_CN( Types.TIMESTAMP, new boolean[]		{ _,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,Y,_,_, } ),
		new T_CN( Types.VARCHAR, new boolean[]			{ _,_,Y,_,_,_,_,_,_,_,Y,_,_,_,_,_,_,Y,_, } ),
		new T_CN( Types.VARBINARY, new boolean[]		{ _,_,_,_,_,_,_,_,_,_,_,Y,_,_,_,_,_,_,Y, } ),
	};

	/////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	/////////////////////////////////////////////////////////////

	private	static	boolean		_debug;					// if true, we print chatty diagnostics
	
	private	static	PrintStream	_outputStream = System.out;	// where to print debug output

	private	static	String[]	_defaultClientSettings;	// one of the clients in LEGAL_CLIENTS
	private	static	Driver		_driver;				// the corresponding jdbc driver
	private	static	String		_databaseName;			// sandbox for tests
	private	static	Version		_clientVMLevel;			// level of client-side vm
	private	static	Version		_driverLevel;			// client rev level
	private	static	Version		_serverLevel;			// server rev level

	// map derby type name to type descriptor
	private	static	HashMap		_types = new HashMap();	// maps Derby type names to TypeDescriptors

	// map jdbc type to index into COERCIONS
	private	static	HashMap		_coercionIndex = new HashMap();	// maps jdbc types to legal coercions

	/////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTOR
	//
	/////////////////////////////////////////////////////////////
	
	public	JDBCDriverTest() {}

	/////////////////////////////////////////////////////////////
	//
	//	JUnit BEHAVIOR
	//
	/////////////////////////////////////////////////////////////

	/**
	 * <p>
	 * JUnit boilerplate which adds as test cases all public methods
	 * whose names start with the string "test".
	 * </p>
	 */
	public static Test suite()
	{ 
		return new TestSuite( JDBCDriverTest.class ); 
	}


	/////////////////////////////////////////////////////////////
	//
	//	ENTRY POINT
	//
	/////////////////////////////////////////////////////////////

	/**
	 * <p>
	 * Run JDBC compatibility tests using either the specified client or
	 * the client that is visible
	 * on the classpath. If there is more than one client on the classpath,
	 * exits with an error.
	 * </p>
	 *
	 * <ul>
	 * <li>arg[ 0 ] = required name of database to connect to</li>
	 * <li>arg[ 1 ] = optional driver to use. if not specified, we'll look for a
	 *                client on the classpath</li>
	 * </ul>
	 */
	public static void main( String args[] )
		throws Exception
	{
		int			exitStatus = FAILURE_EXIT;
		
		if (
			   parseDebug() &&
			   parseArgs( args ) &&
			   parseVMLevel() &&
			   findClient() &&
			   findServer()
		   )
		{		
			TestResult	result = junit.textui.TestRunner.run( suite() );
			
			exitStatus = result.errorCount() + result.failureCount();
		}

		Runtime.getRuntime().exit( exitStatus );
	}

	/////////////////////////////////////////////////////////////
	//
	//	TEST ENTRY POINTS
	//
	/////////////////////////////////////////////////////////////

	/**
	 * <p>
	 * Sanity check the integrity of this test suite.
	 * </p>
	 */
	public	void	testSanity()
	{
		assertEquals( "ALL_TYPES.length == ROW_1.length", ALL_TYPES.length, ROW_1.length );

		// make sure there we completely describe the coercibility of every jdbc type
		int		coercionCount = COERCIONS.length;
		for ( int i = 0; i < coercionCount; i++ )
		{ assertEquals( "Coercion " + i, coercionCount, COERCIONS[ i ].getCoercions().length ); }
	}

	/**
	 * <p>
	 * Main test of jdbc drivers.
	 * </p>
	 */
	public	void	testJDBCDriver()
		throws Exception
	{
		Connection		conn = getConnection();
		
		dropSchema( conn );
		createSchema( conn );

		datatypesTest( conn );

		close( conn );
	}
	
	/////////////////////////////////////////////////////////////
	//
	//	TEST DATATYPES
	//
	/////////////////////////////////////////////////////////////

	//
	// Test that we can declare, insert, and select all datatypes that
	// are legal on the server. Test the metadata for these datatypes.
	//
	private	void	datatypesTest( Connection conn )
		throws Exception
	{
		Object[][]	rows = new Object[][] { makeNullRow(), ROW_1 };
		
		checkTypeDBMetadata( conn );
		stuffAllTypesTable( conn, rows );
		readAllTypesTable( conn, rows );
	}
	
	//
	// Verify that we get the correct DatabaseMetaData for all datatypes
	// that are legal on the server.
	//
	private	void	checkTypeDBMetadata( Connection conn )
		throws Exception
	{
		String				normalizedSchema = DEFAULT_USER_NAME.toUpperCase();
		String				normalizedTable = ALL_TYPES_TABLE.toUpperCase();
		DatabaseMetaData	dbmd = conn.getMetaData();

		ResultSet			rs = dbmd.getColumns
			( null, normalizedSchema, normalizedTable, "%" );

		println( "Pawing through metadata for " + normalizedSchema + '.' + normalizedTable );
		
		while( rs.next() )
		{
			String			columnName = rs.getString( "COLUMN_NAME" );
			int				actualJdbcType = rs.getInt( "DATA_TYPE" );
			TypeDescriptor	typeDesc = getType( columnName );

			if ( columnName.equals( KEY_COLUMN ) ) { continue; }

			StringBuffer	buffer = new StringBuffer();

			buffer.append( "[ " );
			buffer.append( rs.getString( "COLUMN_NAME" ) );
			buffer.append( ",\t" );
			buffer.append( "type( " + rs.getInt( "DATA_TYPE" ) + " ),\t" );
			buffer.append( rs.getString( "TYPE_NAME" ) );
			buffer.append( " ]" );

			println( buffer.toString() );
			
			assertEquals( columnName, typeDesc.getJdbcType(), actualJdbcType );
		}

		close( rs );
	}

	//
	// Verify that we can insert all datatypes that are legal on the
	// server.
	//
	private	void	stuffAllTypesTable( Connection conn, Object[][] rows )
		throws Exception
	{
		StringBuffer	masterBuffer = new StringBuffer();
		StringBuffer	columnBuffer = new StringBuffer();
		StringBuffer	valuesBuffer = new StringBuffer();
		int				columnNumber = 0;
		int				valuesNumber = 0;
		int				typeCount = ALL_TYPES.length;

		beginColumnList( columnBuffer );
		beginColumnList( valuesBuffer );

		addColumn( columnBuffer, columnNumber++, doubleQuote( KEY_COLUMN ) );
		addColumn( valuesBuffer, valuesNumber++, "?" );
		
		for ( int i = 0; i < typeCount; i++ )
		{
			TypeDescriptor	type = ALL_TYPES[ i ];
			
			if ( _serverLevel.atLeast( type.getDerbyVersion() ) )
			{
				String	typeName = type.getDerbyTypeName();
				String	columnDesc = doubleQuote( typeName );
				
				addColumn( columnBuffer, columnNumber++, columnDesc );
				addColumn( valuesBuffer, valuesNumber++, "?" );
			}
		}

		endColumnList( columnBuffer );
		endColumnList( valuesBuffer );

		masterBuffer.append( "insert into " + ALL_TYPES_TABLE + "\n" );
		masterBuffer.append( columnBuffer.toString() );
		masterBuffer.append( "values\n" );
		masterBuffer.append( valuesBuffer.toString() );

		PreparedStatement	ps = prepare( conn, masterBuffer.toString() );
		int					rowCount = rows.length;

		for ( int i = 0; i < rowCount; i++ )
		{
			setRow( ps, i + 1, rows[ i ] );
		}
		
		close( ps );
	}

	//
	// Verify that we can select all datatypes that are legal on the server.
	//
	private	void	readAllTypesTable( Connection conn, Object[][] rows )
		throws Exception
	{
		StringBuffer	buffer = new StringBuffer();
		int				columnNumber = 0;
		int				typeCount = ALL_TYPES.length;

		buffer.append( "select \n" );

		addColumn( buffer, columnNumber++, doubleQuote( KEY_COLUMN ) );
		
		for ( int i = 0; i < typeCount; i++ )
		{
			TypeDescriptor	type = ALL_TYPES[ i ];
			
			if ( _serverLevel.atLeast( type.getDerbyVersion() ) )
			{
				String	typeName = type.getDerbyTypeName();
				String	columnDesc = doubleQuote( typeName );
				
				addColumn( buffer, columnNumber++, columnDesc );
			}
		}

		buffer.append( "\nfrom " + ALL_TYPES_TABLE + "\n" );
		buffer.append( "order by " + doubleQuote( KEY_COLUMN ) );

		PreparedStatement	ps = prepare( conn, buffer.toString() );
		ResultSet			rs = ps.executeQuery();

		checkRSMD( rs );
		checkRows( rs, rows );
		
		close( rs );
		close( ps );
	}

	//
	// Verify that we get the correct ResultSetMetaData for all datatypes
	// which are legal on the server.
	//
	private	void	checkRSMD( ResultSet rs )
		throws Exception
	{
		ResultSetMetaData	rsmd = rs.getMetaData();
		int					columnCount = rsmd.getColumnCount();
		int					firstTastyColumn = 0;

		println( "Column count = " + columnCount );
		
		firstTastyColumn++;				// skip uninteresting key column

		for ( int i = firstTastyColumn; i < columnCount; i++ )
		{
			int				columnID = i + 1;
			String			columnName = rsmd.getColumnName( columnID );
			TypeDescriptor	typeDesc = getType( columnName );
			int				expectedType = rsmdTypeKludge( typeDesc.getJdbcType() );
			int				actualType = rsmd.getColumnType( columnID );
			
			println( "Checking type of " + columnName );

			assertEquals( columnName, expectedType, actualType );
		}
	}

	//
	// Verify that we select the values we
	// originally inserted into a table.
	//
	private	void	checkRows( ResultSet rs, Object[][] rows )
		throws Exception
	{
		int					rowCount = rows.length;

		for ( int i = 0; i < rowCount; i++ )
		{
			rs.next();
			checkRow( rs, rows[ i ] );
		}
	}

	//
	// Verify that we select the values we
	// originally inserted into a row.
	//
	private	void	checkRow( ResultSet rs, Object[] row )
		throws Exception
	{
		int				typeCount = ALL_TYPES.length;

		for ( int i = 0; i < typeCount; i++ )
		{
			TypeDescriptor	type = ALL_TYPES[ i ];
			
			if ( _serverLevel.atLeast( type.getDerbyVersion() ) )
			{
				String	columnName = type.getDerbyTypeName();
				Object	expectedValue = row[ i ];
				Object	actualValue = getColumn( rs, columnName, type );

				println( "Comparing column " + columnName + ": " + expectedValue + " to " + actualValue );
				compareObjects( columnName, expectedValue, actualValue );

				checkCoercions( rs, columnName, type );
			}
		}
	}

	//
	// Verify all legal jdbc coercions of a data value.
	//
	private	void	checkCoercions( ResultSet rs, String columnName, TypeDescriptor type )
		throws Exception
	{
		T_CN		coercionDesc = COERCIONS[ getCoercionIndex( type.getJdbcType() ) ];
		boolean[]	coercions = coercionDesc.getCoercions();
		int			count = coercions.length;
		int			legalCoercions = 0;

		println( "Checking coercions for " + columnName );
		
		for ( int i = 0; i < count; i++ )
		{
			if ( coercions[ i ] )
			{
				legalCoercions++;
				
				Object	retval = getColumn( rs, columnName, COERCIONS[ i ].getJdbcType() );
			}
		}

		println( "   Checked " + legalCoercions + " coercions for " + columnName );
	}
	
	//
	// This kludge compensates for the fact that the DRDA clients report
	// that NUMERIC columns are DECIMAL. See bug 584.
	//
	private	int	rsmdTypeKludge( int originalJDbcType )
	{
		// The embedded client does the right thing.
		if ( _defaultClientSettings == EMBEDDED_CLIENT ) { return originalJDbcType; }
		
		switch( originalJDbcType )
		{
			case Types.NUMERIC:	return Types.DECIMAL;

		    default:			return originalJDbcType;
		}
	}

	//
	// Insert a row into the ALL_TYPES table. The row contains all datatypes
	// that are legal on the server.
	//
	private	void	setRow( PreparedStatement ps, int keyValue, Object[] row )
		throws Exception
	{
		int				param = 1;
		int				typeCount = ALL_TYPES.length;

		ps.setInt( param++, keyValue );

		for ( int i = 0; i < typeCount; i++ )
		{
			TypeDescriptor	type = ALL_TYPES[ i ];
			Object			value = row[ i ];
			
			if ( _serverLevel.atLeast( type.getDerbyVersion() ) )
			{
				setParameter( ps, param++, type, value );
			}
		}

		ps.execute();
	}

	private	Object[]	makeNullRow()
	{
		return new Object[ ALL_TYPES.length ];
	}

	//
	// Index the TypeDescriptors by Derby type name.
	//
	private	void	buildTypeMap()
	{
		int				typeCount = ALL_TYPES.length;

		for ( int i = 0; i < typeCount; i++ ) { putType( ALL_TYPES[ i ] ); }
	}
	private	void	putType( TypeDescriptor type )
	{
		_types.put( type.getDerbyTypeName(), type );
	}

	//
	// Lookup TypeDescriptors by Derby tgype name.
	//
	private	TypeDescriptor	getType( String typeName )
	{
		if ( _types.size() == 0 ) { buildTypeMap(); }
		
		return (TypeDescriptor) _types.get( typeName );
	}

	//
	// Index legal coercions by jdbc type.
	//
	private	void	buildCoercionMap()
	{
		int				count = COERCIONS.length;

		for ( int i = 0; i < count; i++ ) { putCoercionIndex( i ); }
	}
	private	void	putCoercionIndex( int index )
	{
		_coercionIndex.put( new Integer( COERCIONS[ index ].getJdbcType() ), new Integer( index ) );
	}

	//
	// Lookup the legal coercions for a given jdbc type.
	//
	private	int	getCoercionIndex( int jdbcType )
	{
		if ( _coercionIndex.size() == 0 ) { buildCoercionMap(); }
		
		return ((Integer) _coercionIndex.get( new Integer( jdbcType ) )).intValue();
	}
	
	/////////////////////////////////////////////////////////////
	//
	//	MINIONS
	//
	/////////////////////////////////////////////////////////////
	
	///////////////////
	//
	//	TYPE MANAGEMENT
	//
	///////////////////
	
	//////////////////
	//
	//	SCHEMA MINIONS
	//
	//////////////////

	//
	// Create all the tables needed by our test cases.
	//
	private	void	createSchema( Connection conn )
		throws Exception
	{
		createAllTypesTable( conn );
	}

	//
	// Create the table for testing legal datatypes.
	//
	private	void	createAllTypesTable( Connection conn )
		throws Exception
	{
		StringBuffer	buffer = new StringBuffer();
		int				columnNumber = 0;
		int				typeCount = ALL_TYPES.length;

		buffer.append( "create table " + ALL_TYPES_TABLE + "\n" );
		beginColumnList( buffer );

		addColumn( buffer, columnNumber++, doubleQuote( KEY_COLUMN ) + "\tint" );
		
		for ( int i = 0; i < typeCount; i++ )
		{
			TypeDescriptor	type = ALL_TYPES[ i ];
			
			if ( _serverLevel.atLeast( type.getDerbyVersion() ) )
			{
				String	typeName = type.getDerbyTypeName();
				String	columnDesc = doubleQuote( typeName ) + '\t' + typeName;
				
				addColumn( buffer, columnNumber++, columnDesc );
			}
		}

		endColumnList( buffer );

		PreparedStatement	ps = prepare( conn, buffer.toString() );

		ps.execute();

		close( ps );
	}

	//
	// Helper methods for declaring a table.
	//
	private	void	beginColumnList( StringBuffer buffer )
	{
		buffer.append( "(\n" );
	}
	private	void	endColumnList( StringBuffer buffer )
	{
		buffer.append( "\n)\n" );
	}
	private	void	addColumn( StringBuffer buffer, int columnNumber, String text  )
	{
		if ( columnNumber > 0 ) { buffer.append( "," ); }

		buffer.append( "\n\t" );
		buffer.append( text );
	}
	
	//
	// Drop the tables used by our test cases.
	//
	private	void	dropSchema( Connection conn )
	{
		dropTable( conn, ALL_TYPES_TABLE );
	}
	private	void	dropTable( Connection conn, String tableName )
	{
		PreparedStatement	ps = null;
		
		try {
			ps = prepare( conn, "drop table " + tableName );

			ps.execute();
		}
		catch (SQLException e) {}

		close( ps );
	}

	///////////////
	//
	//	SQL MINIONS
	//
	///////////////

	/**
	 * <p>
	 * Create an empty database.
	 * </p>
	 */
	public	void	createDB( String databaseName )
		throws Exception
	{
		String[]	clientSettings = _defaultClientSettings;
		String		dbURL = makeDatabaseURL( clientSettings, databaseName );

		dbURL = dbURL + ';' + CREATE_PROPERTY;

		Properties	properties = new Properties();

		properties.put( "user", DEFAULT_USER_NAME );
		properties.put( "password", DEFAULT_PASSWORD );

		faultInDriver( clientSettings );

		Connection		conn = DriverManager.getConnection( dbURL, properties );

		conn.close();
	}

	// Get a connection to the server.
	private	static	Connection	getConnection()
		throws Exception
	{
		return getConnection( _defaultClientSettings, _databaseName, new Properties() );
	}
	private	static	Connection	getConnection
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

	// Build the connection URL.
	private	static	String	makeDatabaseURL( String[] clientSettings, String databaseName )
	{
		return clientSettings[ DATABASE_URL ] + databaseName;
	}

	//
	// Thin wrapper around jdbc layer to support debugging.
	//
	private	PreparedStatement	prepare( Connection conn, String text )
		throws SQLException
	{
		println( "Preparing: " + text );

		return conn.prepareStatement( text );
	}

	//
	// Logic for stuffing a data value into a column, given its type.
	//
	private	void	setParameter( PreparedStatement ps, int param, TypeDescriptor type, Object value )
		throws Exception
	{
		int		jdbcType = type.getJdbcType();

		if ( value == null )
		{
			ps.setNull( param, jdbcType );

			return;
		}

		switch( jdbcType )
		{
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
				fail( "Unknown jdbc type for Derby type: " + type.getDerbyTypeName() );
				break;
		}
	}
	
	//
	// Get a data value from a column, given its type.
	//
	private	Object	getColumn( ResultSet rs, String columnName, TypeDescriptor type )
		throws Exception
	{
		int			jdbcType = type.getJdbcType();

		return getColumn( rs, columnName, jdbcType );
	}
	private	Object	getColumn( ResultSet rs, String columnName, int jdbcType )
		throws Exception
	{
		Object		retval = null;

		switch( jdbcType )
		{
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

	//
	// SQL code generation minions 
	//
	private	String	doubleQuote( String text )
	{
		return '"' + text + '"';
	}

	//
	// Swallow uninteresting exceptions when disposing of jdbc objects.
	//
	private	void	close( ResultSet rs )
	{
		try {
			if ( rs != null ) { rs.close(); }
		}
		catch (SQLException e) {}
	}	
	private	void	close( Statement statement )
	{
		try {
			if ( statement != null ) { statement.close(); }
		}
		catch (SQLException e) {}
	}
	private	void	close( Connection conn )
	{
		try {
			if ( conn != null ) { conn.close(); }
		}
		catch (SQLException e) {}
	}

	///////////////////
	//
	//	GENERAL MINIONS
	//
	///////////////////
	
	// Debug code to print chatty informational messages.
	private	static	void	println( String text )
	{
		if ( _debug )
		{
			_outputStream.println( text );
			_outputStream.flush();
		}
	}

	// Print out a stack trace
	private	static	void	printStackTrace( Throwable t )
	{
		while ( t != null )
		{
			t.printStackTrace( _outputStream );

			if ( t instanceof SQLException )	{ t = ((SQLException) t).getNextException(); }
			else { break; }
		}
	}

	//
	// Return a meaningful exit status so that calling scripts can take
	// evasive action.
	//
	private	void	exit( int exitStatus )
	{
		Runtime.getRuntime().exit( exitStatus );
	}

	//////////////////////////
	//
	//	INITIALIZATION MINIONS
	//
	//////////////////////////
	
	//
	// Initialize client settings based on the client found.
	// Return true if one and only one client found, false otherwise.
	// We allow for the special case when we're running the embedded client
	// off the current compiled class tree rather than off product jars.
	//
	private	static	boolean	findClient()
		throws Exception
	{
		//
		// The client may have been specified on the command line.
		// In that case, we don't bother looking for a client on
		// the classpath.
		//
		if ( _defaultClientSettings != null ) { faultInDriver( _defaultClientSettings ); }
		else
		{
			String	currentClientName = null;
			int		legalCount = LEGAL_CLIENTS.length;
			int		foundCount = 0;

			for ( int i = 0; i < legalCount; i++ )
			{
				String[]	candidate = LEGAL_CLIENTS[ i ];

				if ( faultInDriver( candidate ) )
				{
					_defaultClientSettings = candidate;
					foundCount++;
				}
			}

			if ( foundCount != EXPECTED_CLIENT_COUNT )
			{
				throw new Exception( "Wrong number of drivers: " + foundCount );
			}
		}

		// Now make sure that the JDBC driver is what we expect

		try {
			_driver = DriverManager.getDriver( _defaultClientSettings[ DATABASE_URL ] );
			_driverLevel = new Version( _driver.getMajorVersion(), _driver.getMinorVersion() );
		}
		catch (SQLException e)
		{
			printStackTrace( e );
			
			throw new Exception
				( "Driver doesn't understand expected URL: " + _defaultClientSettings[ DATABASE_URL ] );
		}

		println
			(
			    "Driver " + _driver.getClass().getName() +
				" Version = " + _driverLevel
			);
		
		return true;
	}

	//
	// Initialize server settings. Assumes that you have called
	// findClient().
	//
	private	static	boolean	findServer()
		throws Exception
	{
		try {
			Connection			conn = getConnection();
			DatabaseMetaData	dmd = conn.getMetaData();
			String				dbProductVersion = dmd.getDatabaseProductVersion();

			_serverLevel = new Version( dbProductVersion );
		}
		catch (Exception e)
		{
			printStackTrace( e );
			
			throw new Exception( "Error lookup up server info: " + e.getMessage() );
		}
		
		println( "Server Version = " + _serverLevel );

		return true;
	}

	private	static	boolean	faultInDriver( String[] clientSettings )
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

	private	static	boolean	parseDebug()
	{
		_debug = Boolean.getBoolean( DEBUG_FLAG );

		return true;
	}
		
	private	static	boolean	parseVMLevel()
		throws Exception
	{
		String				vmVersion = System.getProperty( VERSION_PROPERTY );

		try {
			_clientVMLevel = new Version( vmVersion );
		}
		catch (NumberFormatException e)
		{
			throw new Exception( "Badly formatted vm version: " + vmVersion );
		}

		println( "VM Version = " + _clientVMLevel );

		return true;
	}

	private	static	boolean	parseArgs( String args[] )
		throws Exception
	{
		if ( ( args == null ) || (args.length == 0 ) )
		{ throw new Exception( "Missing database name." ); }
		
		_databaseName = args[ 0 ];

		if ( (args.length > 1) && !"".equals( args[ 1 ] ) )
		{
			String	desiredClientName = args[ 1 ];
			int		count = LEGAL_CLIENTS.length;

			for ( int i = 0; i < count; i++ )
			{
				String[]	candidate = LEGAL_CLIENTS[ i ];

				if ( desiredClientName.equals( candidate[ DRIVER_NAME ] ) )
				{
					_defaultClientSettings = candidate;
					break;
				}
			}

			if ( _defaultClientSettings == null )
			{
				throw new Exception
					( "Could not find client " + desiredClientName + " on the classpath." );
			}
		}
			
		return true;
	}

	
	/////////////////////////////////////////////////////////////
	//
	//	INNER CLASSES
	//
	/////////////////////////////////////////////////////////////

	/**
	 * <p>
	 * This helper class exposes an entry point for creating an empty database.
	 * </p>
	 */
	public	static	final	class	Creator
	{
		private	static	JDBCDriverTest	_driver = new JDBCDriverTest();
		
		/**
		 * <p>
		 * Wait for server to come up, then create the database.
		 * </p>
		 *
		 * <ul>
		 * <li>args[ 0 ] = name of database to create.</li>
		 * </ul>
		 */
		public	static	void	main( String[] args )
			throws Exception
		{
			String		databaseName = args[ 0 ];

			JDBCDriverTest.findClient();
			
			_driver.createDB( databaseName );
		}
		
	}

	/**
	 * <p>
	 * A class for storing a major and minor version number. This class
	 * assumes that more capable versions compare greater than less capable versions.
	 * </p>
	 */
	public	static	final	class	Version	implements	Comparable
	{
		private	int	_major;
		private	int	_minor;

		public	Version( int major, int minor )
		{
			constructorMinion( major, minor );
		}

		public	Version( String desc )
			throws NumberFormatException
		{
			StringTokenizer		tokens = new StringTokenizer( desc, "." );

			constructorMinion
				(
				    java.lang.Integer.parseInt( tokens.nextToken() ),
					java.lang.Integer.parseInt( tokens.nextToken() )
				);
		}

		private	void	constructorMinion( int major, int minor )
		{
			_major = major;
			_minor = minor;
		}

		/**
		 * <p>
		 * Returns true if this Version is at least as advanced
		 * as that Version.
		 * </p>
		 */
		public	boolean	atLeast( Version that )
		{
			return this.compareTo( that ) > -1;
		}


		////////////////////////////////////////////////////////
		//
		//	Comparable BEHAVIOR
		//
		////////////////////////////////////////////////////////

		public	int	compareTo( Object other )
		{
			if ( other == null ) { return -1; }
			if ( !( other instanceof Version ) ) { return -1; }

			Version	that = (Version) other;

			if ( this._major < that._major ) { return -1; }
			if ( this._major > that._major ) { return 1; }

			return this._minor - that._minor;
		}

		////////////////////////////////////////////////////////
		//
		//	Object OVERLOADS
		//
		////////////////////////////////////////////////////////
		
		public	String	toString()
		{
			return Integer.toString( _major ) + '.' + Integer.toString( _minor );
		}

		public	boolean	equals( Object other )
		{
			return (compareTo( other ) == 0);
		}

		public	int	hashCode()
		{
			return _major ^ _minor;
		}
		
	}

	/**
	 * <p>
	 * This helper class describes a legal datatype and the version of Derby
	 * and db2jcc where the datatype first appears.
	 * </p>
	 */
	public	static	final	class	TypeDescriptor
	{
		private	int		_jdbcType;
		private	String	_derbyTypeName;
		private	Version	_db2jccVersion;		// first db2jcc version which supports this type
		private	Version	_derbyVersion;		// first derby version which supports this type
		private	Version	_vmVersion;			// first vm (jdbc) version which supports this type

		public	TypeDescriptor
		(
		    int		jdbcType,
			String	derbyTypeName,
			Version	db2jccVersion,
			Version	derbyVersion,
			Version	vmVersion
		)
		{
			_jdbcType = jdbcType;
			_derbyTypeName = derbyTypeName;
			_db2jccVersion = db2jccVersion;
			_derbyVersion = derbyVersion;
			_vmVersion = vmVersion;
		}

		public	int		getJdbcType() 					{ return _jdbcType; }
		public	String	getDerbyTypeName()				{ return _derbyTypeName; }
		public	Version	getDb2jccVersion()				{ return _db2jccVersion; }
		public	Version	getDerbyVersion()				{ return _derbyVersion; }
		public	Version	getVMVersion()					{ return _vmVersion; }
	}

	/**
	 * <p>
	 * This helper class captures TypeCoercion logic. I have abbreviated it to
	 * this ugly class name so that the COERCIONS table will fit on a readable screen.
	 * </p>
	 */
	public	static	final	class	T_CN
	{
		private	int			_jdbcType;
		private	boolean[]	_coercions;

		public	T_CN( int jdbcType, boolean[] coercions )
		{
			_jdbcType = jdbcType;
			_coercions = coercions;
		}

		public	int			getJdbcType() 					{ return _jdbcType; }
		public	boolean[]	getCoercions() 					{ return _coercions; }
	}
	
	/**
	 * <p>
	 * A crude Blob implementation for datatype testing.
	 * </p>
	 */
	public	static	final	class	MyBlob	implements	Blob
	{
		private	byte[]	_bytes;

		public	MyBlob( byte[] bytes )
		{
			_bytes = bytes;
		}

		public	InputStream	getBinaryStream()
		{
			return new ByteArrayInputStream( _bytes );
		}

		public	byte[]	getBytes( long position, int length ) { return _bytes; }

		public	long	length() { return (long) _bytes.length; }

		public	long	position( Blob pattern, long start ) { return 0L; }
		public	long	position( byte[] pattern, long start ) { return 0L; }

		public	boolean	equals( Object other )
		{
			if ( other == null ) { return false; }
			if ( !( other instanceof Blob ) ) { return false; }

			Blob	that = (Blob) other;

			try {
				if ( this.length() != that.length() ) { return false; }

				InputStream	thisStream = this.getBinaryStream();
				InputStream	thatStream = that.getBinaryStream();

				while( true )
				{
					int		nextByte = thisStream.read();

					if ( nextByte < 0 ) { break; }
					if ( nextByte != thatStream.read() ) { return false; }
				}
			}
			catch (Exception e)
			{
				System.err.println( e.getMessage() );
				e.printStackTrace();
				return false;
			}

			return true;
		}

	}

	/**
	 * <p>
	 * A crude Clob implementation for datatype testing.
	 * </p>
	 */
	public	static	final	class	MyClob	implements	Clob
	{
		private	String	_contents;

		public	MyClob( String contents )
		{
			_contents = contents;
		}

		public	InputStream	getAsciiStream()
		{
			try {
				return new ByteArrayInputStream( _contents.getBytes( "UTF-8" ) );
			}
			catch (Exception e) { return null; }
		}

		public	Reader	getCharacterStream()
		{
			return new CharArrayReader( _contents.toCharArray() );
		}

		public	String	getSubString( long position, int length )
		{
			return _contents.substring( (int) position, length );
		}
		
		public	long	length() { return (long) _contents.length(); }

		public	long	position( Clob searchstr, long start ) { return 0L; }
		public	long	position( String searchstr, long start ) { return 0L; }

		public	boolean	equals( Object other )
		{
			if ( other == null ) { return false; }
			if ( !( other instanceof Clob ) ) { return false; }

			Clob	that = (Clob) other;

			try {
				if ( this.length() != that.length() ) { return false; }
			
				InputStream	thisStream = this.getAsciiStream();
				InputStream	thatStream = that.getAsciiStream();

				while( true )
				{
					int		nextByte = thisStream.read();

					if ( nextByte < 0 ) { break; }
					if ( nextByte != thatStream.read() ) { return false; }
				}
			}
			catch (Exception e)
			{
				System.err.println( e.getMessage() );
				e.printStackTrace();
				return false;
			}

			return true;
		}

	}

}
