/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.tools.ij
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.tools.ij;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
//import java.io.PrintStream;

class AsyncStatement extends Thread {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
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
