/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.WISCInsert

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.*;

/**
 * This class is a VTI for loading data into the Wisconsin benchmark schema.
 * See The Benchmark Handbook, Second Edition (edited by Jim Gray).
 */
public class WISCInsert {

	private static final char[] chars = {
		'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
		'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'
	};

	int numrows;
	int prime;
	int generator; 
	int rowsReturned = 0;

	int unique1;
	int unique2;
	int two;
	int four;
	int ten;
	int twenty;
	int onePercent;
	int tenPercent;
	int twentyPercent;
	int fiftyPercent;
	int unique3;
	int evenOnePercent;
	int oddOnePercent;
	String stringu1;
	String stringu2;
	String string4;

	int seed;
	static final String[] cyclicStrings = {
		"AAAAxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
		"HHHHxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
		"OOOOxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
		"VVVVxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
		};

	boolean closed = false;

	public WISCInsert()
	{
	}

	public int doWISCInsert(int numrows, String tableName, Connection conn) throws SQLException {
		this.numrows = numrows;

		/* Choose prime and generator values for the desired table size */
		if (numrows <= 1000) {
			generator = 279;
			prime = 1009;
		} else if (numrows <= 10000) {
			generator = 2969;
			prime = 10007;
		} else if (numrows <= 100000) {
			generator = 21395;
			prime = 100003;
		} else if (numrows <= 1000000) {
			generator = 2107;
			prime = 1000003;
		} else if (numrows <= 10000000) {
			generator = 211;
			prime = 10000019;
		} else if (numrows <= 100000000) {
			generator = 21;
			prime = 100000007;
		} else {
			throw new SQLException("Too many rows - maximum is 100000000, " +
									numrows + " requested.");
		}

		seed = generator;

		String insertString = "insert into " + tableName + " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement ps = conn.prepareStatement(insertString);

		// loop the insert statement
		for (int i = 0; i < numrows; i++)
		{
			next();
			ps.setInt(1, unique1);
			ps.setInt(2, unique2);
			ps.setInt(3, two);
			ps.setInt(4, four);
			ps.setInt(5, ten);
			ps.setInt(6, twenty);
			ps.setInt(7, onePercent);
			ps.setInt(8, tenPercent);
			ps.setInt(9, twentyPercent);
			ps.setInt(10, fiftyPercent);
			ps.setInt(11, unique3);
			ps.setInt(12, evenOnePercent);
			ps.setInt(13, oddOnePercent);
			ps.setString(14, stringu1);
			ps.setString(15, stringu2);
			ps.setString(16, string4);
			ps.executeUpdate();
			// commit every once in a while?
		}
		return numrows;
	}


	public boolean next() throws SQLException {
		if (rowsReturned >= numrows)
			return false;

		seed = rand(seed, numrows);

		unique1 = seed - 1;
		unique2 = rowsReturned;
		two = unique1 % 2;
		four = unique1 % 4;
		ten = unique1 % 10;
		twenty = unique1 % 20;
		onePercent = unique1 % 100;
		tenPercent = unique1 % 10;
		twentyPercent = unique1 % 5;
		fiftyPercent = unique1 % 2;
		unique3 = unique1;
		evenOnePercent = onePercent * 2;
		oddOnePercent = evenOnePercent + 1;
		stringu1 = uniqueString(unique1);
		stringu2 = uniqueString(unique2);
		string4 = cyclicStrings[rowsReturned % cyclicStrings.length];

		rowsReturned++;

		return true;
	}


	private int rand(int seed, int limit) {
		do {
			seed = (generator * seed) % prime;
		} while (seed > limit);

		return seed;
	}

 
	private String uniqueString(int unique) {
		int i;
		int rem;
		char[] retval = new char[52];

		// First set result string to
		// "AAAAAAA                                             "
		for (i = 0; i < 7; i++) {
			retval[i] = 'A';
		}
		for (i = 7; i < retval.length; i++) {
			retval[i] = 'x';
		}

		// Convert unique value from right to left into an alphabetic string
		i = 6;
		while (unique > 0) {
			rem = unique % 26;
			retval[i] = chars[rem];
			unique /= 26;
			i--;
		}

		return new String(retval);
	}


	public String getShortTestDescription()
	{
		StringBuffer st = new StringBuffer( "insert values into wisconsin benchmark schema.");
		st.append("See The Benchmark Handbook, Second Edition (edited by Jim Gray).");
		return st.toString();
	}


	public String getLongTestDescription()
	{
		StringBuffer st = new StringBuffer(getShortTestDescription() +"\n Called from performance.wisc.WiscLoad. This is not actually a test itself. Based on a scale value by which to multiply the number of rows, the values are generated. This class is based on the vti org.apache.derbyTesting.functionTests.tests.lang.Wisc, however, this will work with any database, not just Cloudscape.");
		return st.toString();

	}


	public boolean isCloudscapeSpecificTest()
	{
	    return false;
	}

}
