/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.TestConnection
 
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

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import java.sql.Connection;
import java.sql.DriverManager;

 /**
  * This class is used to create a connection. This class is then used by the
  * other classes in the jdbc4 suite to create connection to a database
  */
public class TestConnection {
    TestConnection() {
    }
    
    /**
     * This method creates a connection using the Embedded JDBC driver
     */
    Connection createEmbeddedConnection() {
        Connection conn=null;
        try {
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
            conn = DriverManager.getConnection("jdbc:derby:jdbc4_embedded_test_database;create=true");
        } catch(Exception e) {
            System.out.println("Failed in creating a Connection:	"+e);
            e.printStackTrace();
        }
        return conn;
    }
    
    /**
     * This method creates a connection using the derby Client Driver
     */
    
    Connection createClientConnection() {
        Connection conn=null;
        try {
            Class.forName("org.apache.derby.jdbc.ClientDriver");
            conn = DriverManager.getConnection("jdbc:derby://localhost:1527/jdbc4_client_test_database" +
                                               ";create=true");
        } catch(Exception e) {
            System.out.println("Failed in creating a Connection:	"+e);
            e.printStackTrace();
        }
        return conn;
    }
    
    /**
     * closes the connection whose handle is given to the method
     */
    
    void closeConnection(Connection conn) {
        try {
            conn.close();
        } catch(Exception e) {
            System.out.println("Failed to close a connection" + e);
            e.printStackTrace();
        }
    }
    void startTest() {
        Connection conn = createEmbeddedConnection();
        closeConnection(conn);
        Connection conn1 = createClientConnection();
        closeConnection(conn1);
    }
}
