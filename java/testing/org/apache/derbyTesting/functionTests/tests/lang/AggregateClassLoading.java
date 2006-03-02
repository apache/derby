/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.AggregateClassLoading

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.derby.tools.ij;

/**
 * Test for ensuring the aggregate implementation classes are loaded
 * correctly, even when the context class loader loads Derby engine
 * classes as well. This is a typical situation we have seen with
 * J2EE servers where Derby may be in the application WAR and provided
 * as a system service by the container.
 * <BR>
 * Jira issue DERBY-997
 * <BR>
 * Assumes embedded and only needs to be run in embedded, since
 * all class loading happens on the engine side.
 *
 */
public class AggregateClassLoading {
	
    public static void main(String[] args) throws Exception {

		System.out.println("Test AggregateClassLoading starting");

		// use the ij utility to read the property file and
		// make the initial connection.
		ij.getPropertyArg(args);
		Connection conn = ij.startJBMS();
		
		// Find the location of the code for the Derby connection.
		// The rest of the engine will be at the same location!
		URL derbyURL = conn.getClass().getProtectionDomain().getCodeSource().getLocation();
		
		// Create a new loader that loads from the same location as the engine.
		// Create it without a parent, otherwise the parent
		// will be the class loader of this class which is most likely
		// the same as the engine. Since the class loader delegates to
		// its parent first the bug would not show, as all the derby
		// engine classes would be from a single loader.
		URLClassLoader cl = new URLClassLoader(new URL[] {derbyURL}, null);				
		Thread.currentThread().setContextClassLoader(cl);
		
		Statement s = conn.createStatement();
		
		s.execute("create table t (i int)");
		s.execute("insert into t values 1,2,3,4,5,6,null,4,5,456,2,4,6,7,2144,44,2,-2,4");
		System.out.println(s.getUpdateCount() + " rows inserted");
		
		// Test some aggregates, their generated class will attempt
		// to load the internal aggregate through the context loader
		// first, and then any remaining loader.
		testAggregate(s, "select MAX(i) from t");
		testAggregate(s, "select MIN(i) from t");
		testAggregate(s, "select AVG(i) from t");
		testAggregate(s, "select COUNT(i) from t");
		testAggregate(s, "select COUNT(*) from t");
		
        s.execute("drop table t");
	    s.close();
		conn.close();
		
		Thread.currentThread().setContextClassLoader(null);
    }
    
    /**
     * Just run and display the aggregates result.
     */
    private static void testAggregate(Statement s, String query) {
		try {
			ResultSet rs = s.executeQuery(query);
			rs.next();
			System.out.println("query = " + rs.getInt(1));
			rs.close();
		} catch (SQLException e) {
			System.out.println("FAIL " + e.getSQLState() + " " + e.getMessage());
			// TODO Auto-generated catch block
			e.printStackTrace(System.out);
		}
   }
}
