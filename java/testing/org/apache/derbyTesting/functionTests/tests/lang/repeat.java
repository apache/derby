/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.repeat

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;

import org.apache.derby.tools.ij;

/**
	Test the statement cache -- reusing statements with the
	matching SQL text. The only way to verify this output
	is to look at the log file for StatementCache debug flag
	messages about accesses.
	<p>
	The one concrete test here is that the statements
	are actually dumped when the connection is closed,
	and attempts to execute them will fail.

	@author ames
 */

public class repeat {


	public static void main(String[] args) {
		System.out.println("Test repeat starting");
		boolean passed = false;
		try {
			Connection conn;

			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			conn = ij.startJBMS();

			Statement s = conn.createStatement();
			s.execute("create table t (i int)");

			s.execute("insert into t values(180)");

			// should find statement in cache:
			s.execute("insert into t values(180)");

			// should find statement in cache:
			PreparedStatement ps1 = conn.prepareStatement("insert into t values(180)");

			for (int i=1; i<=2; i++) {
				int rows = ps1.executeUpdate();

				if (rows != 1)
					System.out.println("FAIL -- insert wrong number of rows");
			}

			conn.close();

			try {
				int rows = ps1.executeUpdate();
			} catch (Throwable e) {
				passed = true;
			}
			if (!passed)
				System.out.println("FAIL -- able to insert after disconnect");

		} catch (Throwable e) {
			e.printStackTrace();
		}

		if (passed)
			System.out.println("PASS");
		System.out.println("Test repeat finished");
	}
}

