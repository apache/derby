/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.DMDBugsTest

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
package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

public class DMDBugsTest extends BaseJDBCTestCase {

	public DMDBugsTest(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}
	
	public void testDerby3000() throws SQLException, IOException {
		ResultSet rs;
		// Derby-3000 make sure we process only valid TableType values and
		// process them correctly.
		DatabaseMetaData dmd = getConnection().getMetaData();
	
		Statement s = createStatement();
		s.executeUpdate("CREATE TABLE APP.TAB (i int)");
		s.executeUpdate("CREATE VIEW  APP.V  as SELECT * FROM TAB");
		s.executeUpdate("CREATE SYNONYM TSYN FOR APP.TAB");
	
		String[] withInvalidTableTypes = {"SYNONYM","TABLE","VIEW",
		"GLOBAL TEMPORARY"};
		// just ignore invalid types
		rs = dmd.getTables( "%", "%", "%", withInvalidTableTypes);			
		JDBC.assertFullResultSet(rs,
			new String[][] {{"","APP","TSYN","SYNONYM","",null,null,null,null,null},
			{"","APP","TAB","TABLE","",null,null,null,null,null},
			{"","APP","V","VIEW","",null,null,null,null,null}});


		rs = dmd.getTables("%", "%", "%", new String[] {"GLOBAL TEMPORARY"});
		JDBC.assertEmpty(rs);
		
		rs = dmd.getTables("%", "%", "%", new String[] {"VIEW"});
		JDBC.assertUnorderedResultSet(rs, new String[][] 
		            {{"","APP","V","VIEW","",null,null,null,null,null}});

		
		rs = dmd.getTables("%", "%", "%", new String[] {"TABLE"});
		JDBC.assertUnorderedResultSet(rs,new String[][]
		          {{"","APP","TAB","TABLE","",null,null,null,null,null}} );
		
		rs = dmd.getTables("%", "%", "%", new String[] {"SYNONYM"});
		JDBC.assertUnorderedResultSet(rs, new String[][]
	                  {{"","APP","TSYN","SYNONYM","",null,null,null,null,null}});

		rs = dmd.getTables( "%", "%", "%", new String[] {"SYSTEM TABLE"});
		assertEquals(23, JDBC.assertDrainResults(rs));
		s.executeUpdate("DROP VIEW APP.V");
		s.executeUpdate("DROP TABLE APP.TAB");
		s.executeUpdate("DROP SYNONYM APP.TSYN");
	}
	   
		
	/* Default suite for running this test.
	 */
	public static Test suite() {
        BaseTestSuite suite = new BaseTestSuite("DMDBugsTest");
		suite.addTest(
				TestConfiguration.defaultSuite(DMDBugsTest.class));
		return suite;
	        	
	}
}
