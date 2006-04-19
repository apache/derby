/*

Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.TestEnc

Copyright 2006 The Apache Software Foundation or its licensors, as applicable.

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
package org.apache.derbyTesting.functionTests.tests.derbynet;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.derby.tools.ij;

/**
 * This test is part of the encodingTests suite and has regression testcases that
 * have caused problems because of usage of non-portable methods, constructors like
 * String(byte[]) etc. These problems were noticed on Z/OS but can be reproduced
 * when server and client are running with different native encoding
 */
public class TestEnc {
    
    private PrintWriter out;
    
    public static void main(String args[]) throws Exception {
        new TestEnc().go(args);
    }
    
    public void go(String[] args) throws Exception {
        
        // Load the JDBC Driver class
        // use the ij utility to read the property file and
        // make the initial connection.
        ij.getPropertyArg(args);
        Connection conn = ij.startJBMS();
        
        conn.setAutoCommit(true);
        Statement stmt = conn.createStatement();
        
        // Error messages on z/os were garbled because
        // of different native encoding on server/client
        // Related jira issues are 
        // DERBY-583,DERBY-900,DERBY-901,DERBY-902.
        try {
            stmt.execute("select bla");
        } catch (SQLException e) {
            if (e.getSQLState().equals("42X01")) {
                System.out.println("Message "+e.getMessage());
            }
            else
                handleSQLException("DERBY-583",e,false);
        }
        finally {
            if (stmt != null)
                stmt.close();
        }
    }
    
    public void handleSQLException(String method,
            SQLException e,
            boolean expected) throws Exception {
        do {
            out.print("\t" + method + " \tException " +
                    "SQLSTATE:" + e.getSQLState());
            if (expected)
                out.println("  (EXPECTED)");
            else
                e.printStackTrace(out);
            e = e.getNextException();
        } while (e != null);
        
    }
}
