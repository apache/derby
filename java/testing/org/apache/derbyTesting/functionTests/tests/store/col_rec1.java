/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.store.col_rec1

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

import org.apache.derby.shared.common.sanity.SanityManager;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.derby.tools.ij;

/**
 * The purpose of this test and col_rec2 test is to create a territory based 
 * database and create some objects with collation sensitive character types. 
 * Then, make the database crash so that during the recovery, store engine has 
 * to do collation related operations. Those collation related operations are 
 * going to require that we use correct Collator object. DERBY-3302 
 * demonstrated a npe during this operation because Derby was relying on
 * database context to get the correct Collator object. But database context
 * is not available at this point in the recovery. With the fix for DERBY-3302, 
 * the Collator object will now be obtained from collation sensitive datatypes 
 * itself rather than looking at database context which is not available at 
 * this point in recovery. 
 * 
 * This particular class will do the steps of create a territory based database
 * and create some objects with collation sensitive character types. Then, make 
 * the database crash. col_rec2.java will do the part of rebooting the crashed
 * db which will require store to go through recovery.
 */

public class col_rec1 extends BaseTest
{

    public col_rec1()
    {
    }

    /**
     * setup for restart recovery test which will require the use of correct
     * Collator object during recovery of territory based database that will 
     * be created and crashed in this test and later will be recovered in
     * col_rec2.
     **/
    private void test1(
    Connection  conn,
    String      test_name,
    String      table_name)
        throws SQLException
    {
        beginTest(conn, test_name);
        Statement s = conn.createStatement();
        s.execute(
                "create table t(x varchar(100) primary key)");
        conn.commit();
        conn.setAutoCommit(false);
        s.execute("insert into t values 'xxxx'");
		Connection connSecond = DriverManager.getConnection
		("jdbc:derby:collationDB");
        connSecond.setAutoCommit(false);
        Statement sSecond = connSecond.createStatement();
        sSecond.execute("insert into t values 'abab'");
        endTest(conn, test_name);
    }

    public void testList(Connection conn)
        throws SQLException
    {
        test1(conn, "test1", "T");
    }

    public static void main(String[] argv) 
        throws Throwable
    {
    	col_rec1 test = new col_rec1();

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
