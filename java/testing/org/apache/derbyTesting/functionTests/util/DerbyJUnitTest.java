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

import junit.framework.*;

public	class	DerbyJUnitTest	extends	TestCase
{
	/////////////////////////////////////////////////////////////
	//
	//	CONSTANTS
	//
	/////////////////////////////////////////////////////////////

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
	
	public	DerbyJUnitTest() {}

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

