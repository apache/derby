/*

   Derby - Class org.apache.derbyTesting.functionTests.store.LogChecksumRecovery

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

package org.apache.derbyTesting.functionTests.tests.store;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.zip.CRC32;
import org.apache.derby.tools.ij;

/*
 * Purpose of this class is to test the database recovery of 
 * the inserts executed with simulated log corruption in LogChecksumSetup.java
 * and perform some updates after a successfully boot. 
 * This test should be run after the store/LogChecksumSetup.java.
 *
 * @author <a href="mailto:suresh.thalamati@gmail.com">Suresh Thalamati</a>
 * @version 1.0
 * @see LogChecksumSetup
 */

public class LogChecksumRecovery extends LogChecksumSetup {

	LogChecksumRecovery()
	{
		super();
	}
	
	private void runTest(Connection conn) throws SQLException
	{
		logMessage("Begin LogCheckumRecovery Test");
		verifyData(conn, 10);
		updateAndCorrupt(conn, 10);
		logMessage("End LogCheckumRecovery Test");
	}
	
	public static void main(String[] argv) 
        throws Throwable
    {
		
        LogChecksumRecovery lctest = new LogChecksumRecovery();
   		ij.getPropertyArg(argv); 
        Connection conn = ij.startJBMS();
        conn.setAutoCommit(false);

        try {
            lctest.runTest(conn);
        }
        catch (SQLException sqle) {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(
                System.out, sqle);
			sqle.printStackTrace(System.out);
		}
    }
}









