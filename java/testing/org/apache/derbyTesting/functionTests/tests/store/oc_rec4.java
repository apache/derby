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

package org.apache.derbyTesting.functionTests.tests.store;

import org.apache.derby.iapi.db.OnlineCompress;

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.derby.tools.ij;


public class oc_rec4 extends OnlineCompressTest
{

    public oc_rec4()
    {
    }

    /**
     * setup for restart recovery test.
     * <p>
     * Do setup to test restart recovery of online compress.  Real work
     * is done in next test oc_rec4 which will run restart recovery on
     * the work done in this test.
     *
     **/
    private void test1(
    Connection  conn,
    String      test_name,
    String      table_name)
        throws SQLException
    {
        beginTest(conn, test_name);

        // oc_rec3 left the table  with no rows, but compress command
        // did not commit.
        String table_name_2 =  table_name + "_2";
        if (!checkConsistency(conn, "APP", table_name_2))
        {
            logError("conistency check failed.");
        }

        // make sure we can add data to the existing table after redo
        // recovery.
        createAndLoadTable(conn, false, table_name_2, 6000, 0);
        if (!checkConsistency(conn, "APP", table_name_2))
        {
            logError("conistency check failed.");
        }
        endTest(conn, test_name);
    }

    public void testList(Connection conn)
        throws SQLException
    {
        test1(conn, "test1", "TEST1");
    }

    public static void main(String[] argv) 
        throws Throwable
    {
        oc_rec4 test = new oc_rec4();

   		ij.getPropertyArg(argv); 
        Connection conn = ij.startJBMS();
        conn.setAutoCommit(false);

        try
        {
            test.testList(conn);
        }
        catch (SQLException sqle)
        {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(
                System.out, sqle);
			sqle.printStackTrace(System.out);
		}
    }
}
