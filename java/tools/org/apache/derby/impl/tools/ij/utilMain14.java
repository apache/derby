/*

   Derby - Class org.apache.derby.impl.tools.ij.utilMain14

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

package org.apache.derby.impl.tools.ij;
                
import org.apache.derby.iapi.reference.JDBC20Translation;
import org.apache.derby.iapi.reference.JDBC30Translation;

import java.util.Hashtable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.derby.iapi.tools.i18n.LocalizedOutput;
/**
	This class is utilities specific to the two ij Main's.
	This factoring enables sharing the functionality for
	single and dual connection ij runs.

	@author jerry
 */
public class utilMain14 extends utilMain
{
    private static final String JDBC_NOTSUPPORTED = "JDBC 3 method called - not yet supported";
	/**
	 * Set up the test to run with 'numConnections' connections/users.
	 *
	 * @param numConnections	The number of connections/users to test.
	 *
	 * @return Nothing.
	 */
	public utilMain14(int numConnections, LocalizedOutput out)
		throws ijFatalException
	{
		super(numConnections, out, (Hashtable)null);
	}

	/**
	 * Set up the test to run with 'numConnections' connections/users.
	 *
	 * @param numConnections	The number of connections/users to test.
	 * @param ignoreErrors		A list of errors to ignore.  If null,
	 *							all errors are printed out and nothing
	 *							is fatal.  If non-null, if an error is
	 *							hit and it is in this list, it is silently	
	 *							ignore.  Otherwise, an ijFatalException is
	 *							thrown.  ignoreErrors is used for stress
	 *							tests.
	 *
	 * @return Nothing.
	 */
	public utilMain14(int numConnections, LocalizedOutput out, Hashtable ignoreErrors)
		throws ijFatalException
	{
		super(numConnections, out, ignoreErrors);
	}

	/**
	 * Return the right utilMain to use.  (JDBC 1.1 or 2.0 or 3.0)
	 *
	 */
	public utilMain getUtilMain()
	{
		return this;
	}

	/**
	 * Connections by default create ResultSet objects with holdability true. This method can be used
	 * to change the holdability of the connection by passing one of ResultSet.HOLD_CURSORS_OVER_COMMIT
	 * or ResultSet.CLOSE_CURSORS_AT_COMMIT
	 *
	 * @param conn			The connection.
	 * @param holdType	The new holdability for the Connection object.
	 *
	 * @return	The connection object with holdability set to passed value.
	 */
	public Connection setHoldability(Connection conn, int holdType)
		throws SQLException
	{
		conn.setHoldability(holdType);
		return conn;
	}

	/**
		JDBC 3.0
	 * Retrieves the current holdability of ResultSet objects created using this
	 * Connection object.
	 *
	 *
	 * @return  The holdability, one of ResultSet.HOLD_CURSORS_OVER_COMMIT
	 * or ResultSet.CLOSE_CURSORS_AT_COMMIT
	 *
	 */
	public int getHoldability(Connection conn)
		throws SQLException
	{
		return conn.getHoldability();
	}

	/**
	 * Create the right kind of statement (scrolling or not)
	 * off of the specified connection.
	 *
	 * @param conn			The connection.
	 * @param scrollType	The scroll type of the cursor.
	 *
	 * @return	The statement.
	 */
	public Statement createStatement(Connection conn, int scrollType, int holdType)
		throws SQLException
	{
    	Statement stmt;
        try {
        	stmt = conn.createStatement(scrollType, JDBC20Translation.CONCUR_READ_ONLY, holdType);
        }catch(SQLException se) {
			//since jcc doesn't yet support JDBC3.0 we have to go back to JDBC2.0 
			if (isJCC && se.getMessage().equals(JDBC_NOTSUPPORTED))
	        	stmt = conn.createStatement(scrollType, JDBC20Translation.CONCUR_READ_ONLY);
			else 
				throw se;
		}
        catch(AbstractMethodError ame) {
	        //because weblogic 4.5 doesn't yet implement jdbc 2.0 interfaces, need
	        //to go back to jdbc 1.x functionality
			//The jcc obfuscated jar gets this error
			if (isJCC)
	        	stmt = conn.createStatement(scrollType, JDBC20Translation.CONCUR_READ_ONLY);
			else
	        	stmt = conn.createStatement();
		}
		return stmt;
	}

}
