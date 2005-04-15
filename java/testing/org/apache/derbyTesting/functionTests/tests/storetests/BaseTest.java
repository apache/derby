/*

   Derby - Class org.apache.derbyTesting.functionTests.harness.procedure

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

package org.apache.derbyTesting.functionTests.tests.storetests;

import com.ibm.db2j.functionTests.TestUtil;

import org.apache.derby.tools.ij;

import java.sql.Connection;
import java.sql.SQLException;


public abstract class BaseTest
{
    abstract void testList(Connection conn) throws SQLException;

    void runTests(String[] argv)
        throws Throwable
    {
   		ij.getPropertyArg(argv); 
        Connection conn = ij.startJBMS();
        System.out.println("conn from ij.startJBMS() = " + conn);
        conn.setAutoCommit(false);

        try
        {
            testList(conn);
        }
        catch (SQLException sqle)
        {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(
                System.out, sqle);
			sqle.printStackTrace(System.out);
		}
    }

    public BaseTest()
    {
    }

    protected void beginTest(
    Connection  conn,
    String      str)
        throws SQLException
    {
        log("Beginning test: " + str);
        conn.commit();
    }

    protected void endTest(
    Connection  conn,
    String      str)
        throws SQLException
    {
        conn.commit();
        log("Ending test: " + str);
    }
    protected void log(String   str)
    {
        System.out.println(str);
    }
}
