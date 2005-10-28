/*

   Derby - Class org.apache.derby.impl.tools.ij.AsyncStatement

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.tools.ij;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
//import java.io.PrintStream;

class AsyncStatement extends Thread {
	Connection conn;
	String stmt;
	ijResult result;

	AsyncStatement(Connection theConn, String theStmt) {
		conn = theConn;
		stmt = theStmt;
	}

	public void run() {
		Statement aStatement = null;
		try {
			aStatement = conn.createStatement();
			aStatement.execute(stmt);
			result = new ijStatementResult(aStatement,true);
			// caller must release its resources
		} catch (SQLException e) {
			result = new ijExceptionResult(e);
			if (aStatement!=null) 
				try {
					aStatement.close();
				} catch (SQLException e2) {
					// not a lot we can do here...
				}
		}
		aStatement = null;
	}

	public ijResult getResult() { return result; }
}
