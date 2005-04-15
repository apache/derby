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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.derby.tools.ij;


public class OnlineCompressTest extends BaseTest
{

    OnlineCompressTest()
    {
    }


    private void createAndLoadTable(
    Connection  conn,
    String      tblname)
        throws SQLException
    {
        Statement s = conn.createStatement();

        s.execute(
            "create table " + tblname + 
                "(keycol int, indcol1 int, indcol2 int, indcol3 int, data1 varchar(2000), data2 varchar(2000))");

        PreparedStatement insert_stmt = 
            conn.prepareStatement(
                "insert into " + tblname + " values(?, ?, ?, ?, ?, ?)");

        char[]  data1_data = new char[500];
        char[]  data2_data = new char[500];

        for (int i = 0; i < data1_data.length; i++)
        {
            data1_data[i] = 'a';
            data2_data[i] = 'b';
        }

        String  data1_str = new String(data1_data);
        String  data2_str = new String(data2_data);

        for (int i = 0; i < 10000; i++)
        {
            insert_stmt.setInt(1, i);               // keycol
            insert_stmt.setInt(2, i * 10);          // indcol1
            insert_stmt.setInt(3, i * 100);         // indcol2
            insert_stmt.setInt(4, -i);              // indcol3
            insert_stmt.setString(5, data1_str);   // data1_data
            insert_stmt.setString(6, data2_str);   // data2_data
        }

        conn.commit();
    }

    private void test1(Connection conn) 
        throws SQLException 
    {
        beginTest(conn, "test1");

        createAndLoadTable(conn, "test1");

        OnlineCompress.compressTable("APP", "TEST1", true, true, true);

        endTest(conn, "test1");
    }

    public void testList(Connection conn)
        throws SQLException
    {
        test1(conn);
    }

    public static void main(String[] argv) 
        throws Throwable
    {
        OnlineCompressTest test = new OnlineCompressTest();

   		ij.getPropertyArg(argv); 
        Connection conn = ij.startJBMS();
        System.out.println("conn 2 from ij.startJBMS() = " + conn);
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
