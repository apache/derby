/*

   Derby - Class org.apache.derbyTesting.functionTests.store.MaxLogNumber

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
import java.security.AccessController;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.derby.tools.ij;
import org.apache.derby.shared.common.sanity.SanityManager;

/*
 * This class  tests recovery logic with large log file id's and  the error
 * handling logic when Max possible log file limit is reached. MaxLogNumber.java
 * test does the setup, so it should be run before this test. 
 * In Non debug mode, this tests just acts as a plain log recovery test.
 *
 * @version 1.0
 * @see MaxLogNumber
 */

public class MaxLogNumberRecovery extends MaxLogNumber {

	MaxLogNumberRecovery() {
		super();
	}
	
	private void runTest(Connection conn) throws SQLException {
		logMessage("Begin MaxLogNumberRecovery Test");
		verifyData(conn, 100);
		boolean hitMaxLogLimitError = false;
		try{
			insert(conn, 110, COMMIT, 11);
			update(conn, 110, ROLLBACK, 5);
			update(conn, 110, NOACTION, 5);
			verifyData(conn, 210);
			if (SanityManager.DEBUG)
			{
				// do lot of inserts in debug mode , 
				// so that actuall reach the max log file number 
				// limit
				insert(conn, 11000, COMMIT, 5);
			}
		} catch(SQLException se) {
			
			SQLException ose = se;
			while (se != null) {
      			if ("XSLAK".equals(se.getSQLState())) {
					hitMaxLogLimitError = true;
					break;
				}
				se = se.getNextException();
			}
			if(!hitMaxLogLimitError)
				throw ose;
		}

		if (SanityManager.DEBUG)
		{
			// In the debug build mode , this test should hit the max log limit while
			// doing above DML. 
			if(!hitMaxLogLimitError)
				logMessage("Expected: ERROR XSLAK:" +
						   "Database has exceeded largest log file" +
						   "number 8,589,934,591.");
        }

		logMessage("End MaxLogNumberRecovery Test");
	}


    /**
     * Set system property
     *
     * @param name name of the property
     * @param value value of the property
     */
    private static void setSystemProperty(final String name, 
                        final String value)
    {
    
    AccessController.doPrivileged
        (new java.security.PrivilegedAction<Void>(){
            
            public Void run(){
            System.setProperty( name, value);
            return null;
            
            }
            
        }
         );
    
    }
	public static void main(String[] argv) throws Throwable {
		
        MaxLogNumberRecovery test = new MaxLogNumberRecovery();
   		ij.getPropertyArg(argv); 
        //DERBY -4856 will cause XSLAK create diagnostic information set the
        //extenedDiagSeverityLevel higher so no thread dump or diagnostic info
        setSystemProperty("derby.stream.error.extendedDiagSeverityLevel","50000");
        Connection conn = ij.startJBMS();
        conn.setAutoCommit(false);

        try {
            test.runTest(conn);
        }
        catch (SQLException sqle) {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(
                System.out, sqle);
			sqle.printStackTrace(System.out);
		}
    }
}
