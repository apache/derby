/*

Derby - Class org.apache.derbyTesting.functionTests.tests.memory.ConnectionHandling

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

package org.apache.derbyTesting.functionTests.tests.memory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

/**
 * Test opening connections until a failure due to out of memory
 * and then continue with 500 connection requests to see if the
 * system reacts well of falls over.
 *
 */
public class ConnectionHandling {

 public static void main(String[] args) throws Exception {

        System.out.println("Test ConnectionHandling starting");


        new org.apache.derby.jdbc.EmbeddedDriver();
        
        Connection conn = DriverManager.getConnection("jdbc:derby:wombat;create=true");
        conn.close();
        conn = null;
        
        ArrayList list = new ArrayList();
        list.ensureCapacity(30000);
        
        Properties p = new Properties();
        
        while (true) {
            Connection c;
            try {

                c = DriverManager.getConnection("jdbc:derby:wombat", p);
            } catch (SQLException e) {
                if ("08004".equals(e.getSQLState()))
                    System.out.println("FIRST OOME: " + e.getSQLState() + " "
                            + e.getMessage());
                else {
                    System.out.println("UNKNOWN ERROR " + e.getSQLState() + " "
                            + e.getMessage());
                    e.printStackTrace(System.out);
                }
                break;
            } catch (Throwable t) {
                System.out.println("UNKNOWN ERROR " + t);
                t.printStackTrace(System.out);
                break;
            }
            list.add(c);
            if ((list.size() % 1000) == 0) {
                System.out.print(".");
            }
        }
        
        System.out.println("");
        
        System.out.println(list.size() + " successful connections");
        
        list.ensureCapacity(list.size() + 500);
        
        // try to make 500 more connection requests.
        int fail_sqloome = 0;
        int fail_sql = 0;
        int fail_bad = 0;
        int ok = 0;
        for (int i = 0; i < 500; i++)
        {
            // Sleep for 10 secs as we know the implementation
            // of the low meory watermark resets after 5 seconds.
            if (i == 300)
                Thread.sleep(10000L);
            try {
                  Connection c = DriverManager.getConnection("jdbc:derby:wombat", p);
                  list.add(c);
                  ok++;
            } catch (SQLException e) {
                if ("08004".equals(e.getSQLState()))
                    fail_sqloome++;
                else {
                    fail_sql++;
                    System.out.println("UNKNOWN ERROR " + e.getSQLState() + " " + e.getMessage());
                    e.printStackTrace(System.out);
                }
            } catch (Throwable t) {
                fail_bad++;
                System.out.println("UNKNOWN ERROR " + t);
                t.printStackTrace(System.out);
            }
        }
        
        System.out.println("OK                  : " + ok);
        System.out.println("Failed 08004        : " + fail_sqloome);
        System.out.println("Failed SQLException : " + fail_sql);
        System.out.println("Failed Throwable    : " + fail_bad);
        
        System.out.println("closing connections : " + list.size());
        int alreadyClosed = 0;
        for (int i = 0; i < list.size(); i++)
        {
            Connection c = (Connection) list.get(i);
            list.set(i, null);
            if (c.isClosed())
                alreadyClosed++;
            else 
                c.close();
        }
        System.out.println("already closed      : " + alreadyClosed);        
  }
}