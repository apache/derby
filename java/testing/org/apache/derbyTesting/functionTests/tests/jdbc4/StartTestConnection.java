/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.TestConnectionMethods
 
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

import java.sql.*;

import org.apache.derby.tools.ij;


public class StartTestConnection {
    public static void main(String args[]) {
        try {
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
		
			Connection	conn_main = ij.startJBMS();

            TestConnection tc = new TestConnection();
            tc.startTest( conn_main );
        } catch(Exception e) {
            System.out.println("Exception occurred in code StartTestConnection " + e);
            e.printStackTrace();
        }
    }
}

