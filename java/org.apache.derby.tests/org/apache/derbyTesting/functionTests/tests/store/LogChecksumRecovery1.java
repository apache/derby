/*

   Derby - Class org.apache.derbyTesting.functionTests.store.LogChecksumRecovery1

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

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
 * Purpose of this class is to test the database recovery of the  
 * updates statements executed in LogChecksumRecovery.java.
 * This test should be run after the store/LogChecksumRecovery.java.
 *
 * @version 1.0
 * @see LogChecksumSetup
 * @see LogChecksumRecovery
 */

public class LogChecksumRecovery1 extends LogChecksumSetup {

	LogChecksumRecovery1() {
		super();
	}
	
	private void runTest(Connection conn) throws SQLException
	{
		logMessage("Begin LogCheckumRecovery1 Test");
		verifyData(conn, 10);
		logMessage("End LogCheckumRecovery1 Test");
	}
	
	public static void main(String[] argv) 
        throws Throwable
    {
		
        LogChecksumRecovery1 lctest = new LogChecksumRecovery1();
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









