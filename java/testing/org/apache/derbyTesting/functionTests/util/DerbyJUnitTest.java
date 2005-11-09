/*

Derby - Class org.apache.derbyTesting.functionTests.util

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
 * This class factors out utility methods (including assertion machinery)
 * for re-use by Derby JUnit tests.
 * </p>
 *
 * @author Rick
 */

package org.apache.derbyTesting.functionTests.util;

import java.io.*;
import java.sql.*;

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

	/////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	/////////////////////////////////////////////////////////////

	private	static	boolean		_debug;					// if true, we print chatty diagnostics
	
	private	static	PrintStream	_outputStream = System.out;	// where to print debug output

	/////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTOR
	//
	/////////////////////////////////////////////////////////////
	
	public	DerbyJUnitTest() {}

	/////////////////////////////////////////////////////////////
	//
	//	PUBLIC BEHAVIOR
	//
	/////////////////////////////////////////////////////////////

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
		if ( _debug )
		{
			_outputStream.println( text );
			_outputStream.flush();
		}
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

	/**
	 * <p>
	 * Return a meaningful exit status so that calling scripts can take
	 * evasive action.
	 * </p>
	 */
	public	void	exit( int exitStatus )
	{
		Runtime.getRuntime().exit( exitStatus );
	}

	/////////////////////////////////////////////////////////////
	//
	//	EXTRA ASSERTIONS
	//
	/////////////////////////////////////////////////////////////

	/**
	 * <p>
	 * Compare two objects, allowing nulls to be equal.
	 * </p>
	 */
	public	void	compareObjects( String message, Object left, Object right )
		throws Exception
	{
		if ( left == null )
		{
			assertNull( message, right );
		}
		else
		{
			assertNotNull( right );

			if ( left instanceof byte[] ) { compareBytes( message, left, right ); }
			else if ( left instanceof java.util.Date ) { compareDates( message, left, right ); }
			else { assertTrue( message, left.equals( right ) ); }
		}
	}

	/**
	 * <p>
	 * Compare two byte arrays, allowing nulls to be equal.
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
	 * Compare two Dates, allowing nulls to be equal.
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

