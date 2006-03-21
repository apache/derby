/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.junitTests.lang.BooleanTest

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
 * This JUnit test verifies behavior of the BOOLEAN datatype.
 * </p>
 *
 * @author Rick
 */

package org.apache.derbyTesting.functionTests.tests.junitTests.lang;

import java.math.*;
import java.sql.*;
import java.util.*;

import junit.framework.*;

import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.BigDecimalHandler;
import org.apache.derbyTesting.functionTests.util.DerbyJUnitTest;

public	class	BooleanTest	extends	DerbyJUnitTest
{
	/////////////////////////////////////////////////////////////
	//
	//	CONSTANTS
	//
	/////////////////////////////////////////////////////////////

	private	static	final	String	BOOLEAN_TABLE = "BOOLCOL_499";
	private	static	final	String	CASTING_TABLE = "BOOLEANCASTS";

	private	static	final	Boolean	TRUE = Boolean.TRUE;
	private	static	final	Boolean	FALSE = Boolean.FALSE;

	private	static	final	String	NEGATE_BOOLEAN_FUNC = "negateBooleanFunc";
    private static boolean HAVE_BIG_DECIMAL;
	
    static{
    if(BigDecimalHandler.representation != BigDecimalHandler.BIGDECIMAL_REPRESENTATION)
        HAVE_BIG_DECIMAL = false;
    else
        HAVE_BIG_DECIMAL = true;
    }

	/////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	/////////////////////////////////////////////////////////////

	/////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTOR
	//
	/////////////////////////////////////////////////////////////
	
	public	BooleanTest() {}

	/////////////////////////////////////////////////////////////
	//
	//	DATABASE FUNCTIONS
	//
	/////////////////////////////////////////////////////////////

	public	static	Boolean	negateBooleanFunc( Boolean inValue )
		throws Exception
	{
		return new Boolean( !inValue.booleanValue() );
	}

	/////////////////////////////////////////////////////////////
	//
	//	ENTRY POINT
	//
	/////////////////////////////////////////////////////////////

	/**
	 * <p>
	 * Entry point for running this suite standalone.
	 * </p>
	 */
	public static void main( String args[] )
		throws Exception
	{
		runUnderOldHarness( args, suite() );
	}

	/////////////////////////////////////////////////////////////
	//
	//	JUnit BEHAVIOR
	//
	/////////////////////////////////////////////////////////////

	/**
	 * <p>
	 * JUnit boilerplate which adds as test cases all public methods
	 * whose names start with the string "test" in the named classes.
	 * When you want to add a new class of tests, just wire it into
	 * this suite.
	 * </p>
	 */
	public static Test suite()
	{
		TestSuite	testSuite = new TestSuite();

		testSuite.addTestSuite( BooleanTest.class );

		return testSuite;
	}

	/////////////////////////////////////////////////////////////
	//
	//	TEST ENTRY POINTS
	//
	/////////////////////////////////////////////////////////////

	/**
	 * <p>
	 * Tests for the BOOLEAN datatype.
	 * </p>
	 */
	public	void	testBoolean()
		throws Exception
	{
		//setDebug( true );
		
		Connection	conn = ij.startJBMS();

		createSchema( conn );
		populateTables( conn );
		verifyCatalogs( conn );
		verifySort( conn );

		// now verify casts
		truncateTable( conn, BOOLEAN_TABLE );
		castFromBoolean( conn );
		castToBoolean( conn );
		implicitCasts( conn );

		verifyBooleanInSelect( conn );
		verifyBooleanInValuesClause( conn );
	}

	// create schema for this test
	private	void	createSchema( Connection conn )
		throws Exception
	{
		dropTable( conn, BOOLEAN_TABLE );
		dropTable( conn, CASTING_TABLE );
		dropFunction( conn, NEGATE_BOOLEAN_FUNC );
		executeDDL
			(
			 conn,
			 "create table " + BOOLEAN_TABLE + "( keyCol int, booleanCol boolean )"
			);
		// verify that you can create an index on BOOLEAN columns
		executeDDL
			(
			 conn,
			 "create index boolIdx on " + BOOLEAN_TABLE + "( booleanCol )"
			);

		executeDDL
			(
			 conn,
			 "create table " + CASTING_TABLE +
			 "(\n" +
			 "keyCol               int,\n" +
			 "\n" +
			 "smallintCol          smallint,\n" +
			 "integerCol           integer,\n" +
			 "bigintCol            bigint,\n" +
			 "\n" +
			 "decimalCol           decimal,\n" +
			 "realCol              real,\n" +
			 "doubleCol            double,\n" +
			 "floatCol             float,\n" +
			 "\n" +
			 "charCol              char(5),\n" +
			 "varcharCol           varchar(5),\n" +
			 "longvarcharCol       long varchar,\n" +
			 "\n" +
			 "charforbitdataCol    char(5) for bit data,\n" +
			 "varcharforbitdataCol varchar(5) for bit data,\n" +
			 "\n" +
			 "clobCol              clob,\n" +
			 "blobCol              blob,\n" +
			 "\n" +
			 "dateCol              date,\n" +
			 "timeCol              time,\n" +
			 "timestampCol         timestamp\n" +
			 ")"
			 );

		executeDDL
			(
			 conn,
			 "create function " + NEGATE_BOOLEAN_FUNC +
			 "( inValue boolean ) returns boolean\n" +
			 "parameter style java no sql language java external name\n" +
			 "'" + getClass().getName() + "." + NEGATE_BOOLEAN_FUNC + "'"
			 );
	}

	// populate tables for this test
	private	void	populateTables( Connection conn )
		throws Exception
	{
		execute
			(
			 conn,
			 "insert into " + BOOLEAN_TABLE +
			 "( keyCol, booleanCol ) values\n" +
			 "( 1, true ), ( 2, false ),\n" +
			 "( 3, true ), ( 4, false ),\n" +
			 "( 5, true ), ( 6, false ),\n" +
			 "( 7, true ), ( 8, false ),\n" +
			 "( 9, null ), ( 10, unknown )"
			 );

		// the first tuple contains values which cast to true,
		// the second tuple contains values which cast to false,
		// the third tuple contains values which cast to null
		execute
			(
			 conn,
			 "insert into " + CASTING_TABLE +
			 " values\n" +
			 "(\n" +
			 "1,\n" +
			 "\n" +
			 "1,\n" +
			 "1,\n" +
			 "1,\n" +
			 "\n" +
			 "1.0,\n" +
			 "1.0,\n" +
			 "1.0,\n" +
			 "1.0,\n" +
			 "\n" +
			 "'true',\n" +
			 "'true',\n" +
			 "'true',\n" +
			 "\n" +
			 "X'0001',\n" +
			 "X'0001',\n" +
			 "\n" +
			 "cast('true' as clob),\n" +
			 "cast(X'0001' as blob),\n" +
			 "\n" +
			 "date('1992-01-01'),\n" +
			 "time('09:30:15'),\n" +
			 "timestamp('1997-06-30 01:01:01')\n" +
			 "),\n" +
			 "(\n" +
			 "2,\n" +
			 "\n" +
			 "0,\n" +
			 "0,\n" +
			 "0,\n" +
			 "\n" +
			 "0.0,\n" +
			 "0.0,\n" +
			 "0.0,\n" +
			 "0.0,\n" +
			 "\n" +
			 "'false',\n" +
			 "'false',\n" +
			 "'false',\n" +
			 "\n" +
			 "X'0000',\n" +
			 "X'0000',\n" +
			 "\n" +
			 "cast('false' as clob),\n" +
			 "cast(X'0000' as blob),\n" +
			 "\n" +
			 "date('1992-01-01'),\n" +
			 "time('09:30:15'),\n" +
			 "timestamp('1997-06-30 01:01:01')\n" +
			 "),\n" +
			 "(\n" +
			 "3,\n" +
			 "\n" +
			 "null,\n" +
			 "null,\n" +
			 "null,\n" +
			 "\n" +
			 "null,\n" +
			 "null,\n" +
			 "null,\n" +
			 "null,\n" +
			 "\n" +
			 "null,\n" +
			 "null,\n" +
			 "null,\n" +
			 "\n" +
			 "null,\n" +
			 "null,\n" +
			 "\n" +
			 "null,\n" +
			 "null,\n" +
			 "\n" +
			 "null,\n" +
			 "null,\n" +
			 "null\n" +
			 ")"
			 );
	}
	
	// verify that the boolean column is correctly typed in the catalogs
	private	void	verifyCatalogs( Connection conn )
		throws Exception
	{
		assertScalar
			(
			 conn,
			 
			 "select c.columndatatype\n" +
			 "from sys.syscolumns c, sys.systables t\n" +
			 "where t.tablename = " + singleQuote( BOOLEAN_TABLE ) + "\n" +
			 "and c.referenceid = t.tableid\n" +
			 "and c.columnname='BOOLEANCOL'",

			 "BOOLEAN"
			 );
	}
	
	// verify that nulls sort before false which sorts before true
		// create schema for this test
	private	void	verifySort( Connection conn )
		throws Exception
	{
		PreparedStatement	ps = prepare
			(
			 conn,
			 "select booleanCol from " + BOOLEAN_TABLE + " order by booleanCol"
			 );
		ResultSet			rs = ps.executeQuery();

		// 2 nulls, 4 falses, 4 trues
		assertColumnEquals
			(
			 rs,
			 1,
			 new Object[]
				{
					null, null,
					FALSE, FALSE, FALSE, FALSE,
					TRUE, TRUE, TRUE, TRUE
				}
			 );

		close( rs );
		close( ps );		
	}
	
	// verify casts from BOOLEAN to legal types
	private	void	castFromBoolean( Connection conn )
		throws Exception
	{
		execute
			(
			 conn,
			 "insert into " + BOOLEAN_TABLE +
			 "( keyCol, booleanCol ) values\n" +
			 "( 1, true ), ( 2, false ), ( 3, null )"
			 );

		PreparedStatement ps = null;
		if (HAVE_BIG_DECIMAL)
		{
			ps = prepare
			(
				conn,
				"select\n" +
				"keyCol\n" +
				", cast ( booleanCol as smallint )\n" +
				", cast ( booleanCol as integer )\n" +
				", cast ( booleanCol as bigint )\n" +
				",cast ( booleanCol as decimal )\n" +
				",cast ( booleanCol as real )\n" +
				",cast ( booleanCol as double )\n" +
				",cast ( booleanCol as float )\n" +
				",cast ( booleanCol as char(5) )\n" +
				",cast ( booleanCol as varchar(5) )\n" +
				",cast ( booleanCol as long varchar )\n" +
				",cast ( booleanCol as clob )\n" +
				"from " + BOOLEAN_TABLE + " order by keyCol"
			);
		}
		else
		{
			ps = prepare
			(
				conn,
				"select\n" +
				"keyCol\n" +
				", cast ( booleanCol as smallint )\n" +
				", cast ( booleanCol as integer )\n" +
				", cast ( booleanCol as bigint )\n" +
				",cast ( booleanCol as real )\n" +
				",cast ( booleanCol as double )\n" +
				",cast ( booleanCol as float )\n" +
				",cast ( booleanCol as char(5) )\n" +
				",cast ( booleanCol as varchar(5) )\n" +
				",cast ( booleanCol as long varchar )\n" +
				",cast ( booleanCol as clob )\n" +
				"from " + BOOLEAN_TABLE + " order by keyCol"
			);
		}
				
		ResultSet			rs = ps.executeQuery();

		rs.next();
		if (HAVE_BIG_DECIMAL)
		{
			assertRow
		    (
		     rs,
		     new Object[]
			    {
				    new Integer( 1 ),
				    new Integer( 1 ),
				    new Integer( 1 ),
				    new Long( 1L ),
				    new BigDecimal( 1.0 ),
				    new Float( 1.0 ),
				    new Double( 1.0 ),
				    new Float( 1.0 ),
				    "true ",
				    "true",
				    "true",
				    "true",
			    }
		     );
		}
		else
		{
		    assertRow
			    (
			     rs,
			     new Object[]
				    {
					    new Integer( 1 ),
					    new Integer( 1 ),
					    new Integer( 1 ),
					    new Long( 1L ),
					    new Float( 1.0 ),
					    new Double( 1.0 ),
					    new Float( 1.0 ),
					    "true ",
					    "true",
					    "true",
					    "true",
				    }
			     );
		}

		rs.next();
		if (HAVE_BIG_DECIMAL)
		{
			assertRow
				(
			     rs,
			     new Object[]
				    {
			     		new Integer( 2 ),
						new Integer( 0 ),
						new Integer( 0 ),
						new Long( 0L ),
						new BigDecimal( 0.0 ),
						new Float( 0.0 ),
						new Double( 0.0 ),
						new Float( 0.0 ),
						"false",
						"false",
						"false",
						"false",
				    }
				);
		}
		else
		{
			assertRow
				(
			     rs,
			     new Object[]
				    {
			     		new Integer( 2 ),
						new Integer( 0 ),
						new Integer( 0 ),
						new Long( 0L ),
						new Float( 0.0 ),
						new Double( 0.0 ),
						new Float( 0.0 ),
						"false",
						"false",
						"false",
						"false",
				    }
				);
		}
		
		rs.next();
		if (HAVE_BIG_DECIMAL)
		{
			assertRow
				(
				    rs,
					new Object[]
					{
					    new Integer( 3 ),
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
						null,
					}
				);
		}
		else
		{
			assertRow
			(
			    rs,
				new Object[]
				{
				    new Integer( 3 ),
					null,
					null,
					null,
					null,
					null,
					null,
					null,
					null,
					null,
					null,
				}
			);
		}
	
		close( rs );
		close( ps );			 
	}

	// verify casts to BOOLEAN from legal types
	private	void	castToBoolean( Connection conn )
		throws Exception
	{
		PreparedStatement	ps = prepare
			(
			 conn,
			 "select\n" +
			 "keyCol\n" +
			 ", cast (smallintcol as boolean)\n" +
			 ", cast (integercol as boolean)\n" +
			 ", cast (bigintcol as boolean)\n" +
			 ", cast (decimalcol as boolean)\n" +
			 ", cast (realcol as boolean)\n" +
			 ", cast (doublecol as boolean)\n" +
			 ", cast (floatcol as boolean)\n" +
			 ", cast (charcol as boolean)\n" +
			 ", cast (varcharcol as boolean)\n" +
			 "from " + CASTING_TABLE + " order by keyCol\n"
			 );
		ResultSet			rs = ps.executeQuery();

		rs.next();
		verifyCasts( rs, 2, 10, TRUE );
		rs.next();
		verifyCasts( rs, 2, 10, FALSE );
		rs.next();
		verifyCasts( rs, 2, 10, null );

		close( rs );
		close( ps );			 
	}

	// verify that the language adds the correct implicit casts
	private	void	implicitCasts( Connection conn )
		throws Exception
	{
		int	base = 100;
		
		base = verifyImplicitCast( conn, "smallintCol", base );
		base = verifyImplicitCast( conn, "integerCol", base );
		base = verifyImplicitCast( conn, "bigintCol", base );
		base = verifyImplicitCast( conn, "decimalCol", base );
		base = verifyImplicitCast( conn, "realCol", base );
		base = verifyImplicitCast( conn, "doubleCol", base );
		base = verifyImplicitCast( conn, "floatCol", base );
		base = verifyImplicitCast( conn, "charCol", base );
		base = verifyImplicitCast( conn, "varcharCol", base );
		base = verifyImplicitCast( conn, "longvarcharCol", base );

		//
		// If we try to implicitly cast from clob to boolean, we
		// get the following error:
		//
		// ERROR 22005: An attempt was made to get a data value of type 'boolean' from a data value of type 'CLOB'.
	}
	//
	// Test a single implicit cast. Increment the base for the next round.
	//
	private	int	verifyImplicitCast( Connection conn, String colName, int base )
		throws Exception
	{
		execute
			(
			 conn,
			 "insert into " + BOOLEAN_TABLE +
			 "( keyCol, booleanCol )\n" +
			 "select keyCol + " + base + ", " + colName + "\n" +
			 "from " + CASTING_TABLE
			 );
		PreparedStatement	ps = prepare
			(
			 conn,
			 "select keyCol, booleanCol from " + BOOLEAN_TABLE + " where keyCol > " + base + "\n" +
			 "order by keyCol\n"
			 );
		ResultSet			rs = ps.executeQuery();

		assertColumnEquals( rs, 2, new Object[] { TRUE, FALSE, null } );

		close( rs );
		close( ps );			 

		return base + 100;
	}
	
	private	void	verifyCasts
		( ResultSet rs, int firstColumn, int lastColumn, Boolean expectedValue )
		throws Exception
	{
		for ( int columnNumber = firstColumn; columnNumber <= lastColumn; columnNumber++ )
		{
			assertColumnEquals( "Column number " + columnNumber, rs, columnNumber, expectedValue );
		}
	}
	
	// verify that we can select an expression which evaluates to a boolean value
	private	void	verifyBooleanInSelect( Connection conn )
		throws Exception
	{
		PreparedStatement	ps = prepare
			(
			 conn,
			 "select (booleanCol > booleanCol) from " + BOOLEAN_TABLE
			 );
		ResultSet			rs = ps.executeQuery();

		while( rs.next() )
		{
			assertFalse( rs.getBoolean( 1 ) );
		}

		close( rs );
		close( ps );			 
	}

	// verify that we can use a boolean expression in a values clause
	private	void	verifyBooleanInValuesClause( Connection conn )
		throws Exception
	{
		assertScalar
			(
			 conn,
			 "values " + NEGATE_BOOLEAN_FUNC + "( true )",
			 FALSE
			 );
		assertScalar
			(
			 conn,
			 "values " + NEGATE_BOOLEAN_FUNC + "( (current_date > '1994-02-23') )",
			 FALSE
			 );
	}

}
