/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.WiscVTI

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.vti.VTITemplate;

import java.sql.SQLException;
import java.sql.ResultSetMetaData;
import java.sql.DriverManager;
import java.sql.Connection;

/**
 * This class is a VTI for loading data into the Wisconsin benchmark schema.
 * See The Benchmark Handbook, Second Edition (edited by Jim Gray).
 */
public class WiscVTI extends VTITemplate {

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

	static final WiscMetaData metaData = new WiscMetaData();

	public WiscVTI(int numrows) throws SQLException {
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
	}

	public ResultSetMetaData getMetaData() {
		return metaData;
	}

	public boolean next() throws SQLException {
		if (closed) {
			throw new SQLException("next() call on a closed result set");
		}

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

	public int getInt(int columnIndex) throws SQLException {
		if (closed) {
			throw new SQLException("getInt() call on a closed result set");
		}

		switch (columnIndex) {
		  case 1:
			return unique1;

		  case 2:
			return unique2;

		  case 3:
			return two;

		  case 4:
			return four;

		  case 5:
			return ten;

		  case 6:
			return twenty;

		  case 7:
			return onePercent;

		  case 8:
			return tenPercent;

		  case 9:
			return twentyPercent;

		  case 10:
			return fiftyPercent;

		  case 11:
			return unique3;

		  case 12:
			return evenOnePercent;

		  case 13:
			return oddOnePercent;

		  default:
			throw new SQLException("getInt() invalid for column " + columnIndex);
		}
	}

	public String getString(int columnIndex) throws SQLException {
		if (closed) {
			throw new SQLException("getString() call on a closed result set");
		}

		switch (columnIndex) {
		  case 14:
			return stringu1;

		  case 15:
			return stringu2;

		  case 16:
			return string4;

		  default:
			throw new SQLException("getString() invalid for column " +
																columnIndex);
		}
	}

	public void close() {
		closed = true;
	}

	private int rand(int seed, int limit) {
		do {
			seed = (generator * seed) % prime;
		} while (seed > limit);

		return seed;
	}

	private static final char[] chars = {
		'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
		'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'
	};
 
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

	public static void WISCInsertWOConnection(int numrows, String tableName) throws SQLException {
	
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		WISCInsert wi = new WISCInsert();
		wi.doWISCInsert(numrows, tableName, conn);
	}

}
