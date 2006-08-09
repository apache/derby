/*

   Derby - Class org.apache.derbyTesting.functionTests.harness.procedure

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

package org.apache.derbyTesting.functionTests.tests.storetests;


import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derbyTesting.functionTests.tests.store.BaseTest;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.derby.tools.ij;


/**

The purpose of this test is to reproduce JIRA DERBY-715:

Sometimes a deadlock would be incorrectly reported as a deadlock.  The
bug seemed to always reproduce at least once if the following test
was run (at least one of the iterations in the loop would get an
incorrect timeout vs. a deadlock).

**/

public class st_derby715 extends BaseTest
{
    static boolean verbose = false;

    public st_derby715()
    {
    }


    /**
     * Create the base table that the 2 threads will use.
     **/
    private static void setup()
        throws Exception
    {
        Connection conn = ij.startJBMS();
        Statement  stmt = conn.createStatement();

        // drop table, ignore table does not exist error.

        try
        {
            stmt.executeUpdate("drop table a");
        }
        catch (Exception e)
        {
            // ignore drop table errors.
        }

        try
        {
            stmt.executeUpdate("drop table b");
        }
        catch (Exception e)
        {
            // ignore drop table errors.
        }

        stmt.executeUpdate("create table a (a integer)");
        stmt.executeUpdate("create table b (b integer)");
        stmt.close();
        conn.commit();
        conn.close();
    }

    public static class t1 implements Runnable
    {
        String[] argv;

        public t1(String[] argv)
        {
            argv = argv;
        }
        public void run()
        {
            try
            {
                ij.getPropertyArg(argv); 
                Connection conn = ij.startJBMS();
                conn.setAutoCommit(false);
                conn.setTransactionIsolation(
                        Connection.TRANSACTION_SERIALIZABLE);

                Statement stmt = conn.createStatement();
                if (verbose)
                    System.out.println("Thread 1 before selecting from b");

                // get row locks on all rows in b
                ResultSet rs = stmt.executeQuery("select * from b");

                if (verbose)
                    System.out.println("Thread 1 before selecting next from b");

                while (rs.next())
                {
                    if (verbose)
                        System.out.println("Thread t1 got " + rs.getString(1));
                }
                if (verbose)
                    System.out.println("Thread 1 after all next.");

                // give thread 2 a chance to catch up.
                Thread.sleep(500);

                if (verbose)
                    System.out.println("Thread 1 before inserting into a...");

                // now wait on lock inserting row into table a - either 
                // thread 1 or thread 2 should get a deadlock, NOT a timeout.
                stmt.executeUpdate("insert into a values(1)");

                if (verbose)
                    System.out.println("Thread 1 after inserting into a...");

                conn.rollback();
            }
            catch (SQLException sqle)
            {
                if (sqle.getSQLState().equals("40001"))
                {
                    // only expected exception is a deadlock, we should
                    // get at least one deadlock, so print it to output.
                    // Don't know which thread will get the deadlock, so
                    // don't label it.
                    System.out.println("Got a Deadlock.");
                }
                else
                {
                    org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(
                        System.out, sqle);
                    sqle.printStackTrace(System.out);
                }
                if (verbose)
                    System.out.println("Thread 1 got exception:\n");
            }
            catch (Exception ex)
            {
                System.out.println("got unexpected exception: " + ex);
            }
        }
    }

    public static class t2 implements Runnable
    {
        String[] argv;
        public t2 (String[] argv)
        {
            argv = argv;
        }
        public void run()
        {
            try
            {
                ij.getPropertyArg(argv); 
                Connection conn = ij.startJBMS();
                conn.setAutoCommit(false);
                conn.setTransactionIsolation(
                        Connection.TRANSACTION_SERIALIZABLE);

                Statement stmt = conn.createStatement();

                if (verbose)
                    System.out.println("Thread 2 before selecting from a");

                ResultSet rs = stmt.executeQuery("select * from a");

                if (verbose)
                    System.out.println("Thread 2 before selecting next from a");

                while (rs.next())
                {
                    if (verbose)
                        System.out.println("Thread t2 got " + rs.getString(1));
                }

                if (verbose)
                    System.out.println("Thread 2 after all next.");


                Thread.sleep(500);
                
                if (verbose)
                    System.out.println("Thread 2 before inserting into b");

                stmt.executeUpdate("insert into b values(2)");

                if (verbose)
                    System.out.println("Thread 2 after inserting into b");

                conn.rollback();
            }
            catch (SQLException sqle)
            {
                if (verbose)
                    System.out.println("Thread 1 got exception:\n");

                if (sqle.getSQLState().equals("40001"))
                {
                    // only expected exception is a deadlock, we should
                    // get at least one deadlock, so print it to output.
                    // Don't know which thread will get the deadlock, so
                    // don't label it.
                    System.out.println("Got a Deadlock.");
                }
                else
                {
                    org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(
                        System.out, sqle);
                    sqle.printStackTrace(System.out);
                }
            }
            catch (Exception ex)
            {
                System.out.println("got unexpected exception: " + ex);
            }
        }
    }
    

    public void testList(Connection conn)
        throws SQLException
    {
    }

    public static void main(String[] argv) 
        throws Throwable
    {
        ij.getPropertyArg(argv); 

        st_derby715 setup_ddl = new st_derby715();
        setup_ddl.setup();
        setup_ddl = null;

        {
            for (int i = 0; i < 5; i++)
            {
                Thread test1 = new Thread(new t1(argv));
                Thread test2 = new Thread(new t2(argv));
                test1.start();
                test2.start();
                test1.join();
                test2.join();
            }
        }
        /*
        catch (SQLException sqle)
        {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(
                System.out, sqle);
			sqle.printStackTrace(System.out);
		}
        */
    }
}
