/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.savepointJdbc30_XA.java

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.Connection;
import java.sql.Savepoint;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Properties;

import javax.sql.XADataSource;

import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.TestUtil;
/**
 * Additional savepoint tests not available with J2ME for connections
 * obtained from XADataSource (DERBY-898/DERBY-899)
 */


public class savepointJdbc30_XA  extends savepointJdbc30_JSR169{



	public static void main(String[] args) {
		Connection con = null, con2 = null;
	    // Check savepoints for local transactions for
		// connections fromXADataSource	    
	    
	    try {
	    // Get a connection just to set up the environment with the properties
	    ij.getPropertyArg(args);	
	    ij.startJBMS().close();
		// Test connections obtained via XADataSource DERBY-898/899
		Properties dsprops = new Properties();
		dsprops.setProperty("databaseName","wombat");
		XADataSource ds  = TestUtil.getXADataSource(dsprops);
		con = ds.getXAConnection().getConnection();
		con2 = ds.getXAConnection().getConnection();
		
		runTests("connections from XADataSource (local tranasaction)", 
				 con, con2);
		con.close();
		con2.close();
	    }
		catch (SQLException e) {
		    dumpSQLExceptions(e);
				}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:");
			e.printStackTrace(System.out);
		}

	}
}
