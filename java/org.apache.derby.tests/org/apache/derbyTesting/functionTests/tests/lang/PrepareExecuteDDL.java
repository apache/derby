/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.PrepareExecuteDDL

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test the dependency system for active statements when 
 * a DDL is executed in a separate connection after the
 * prepare but before the execute.
 *
 */
public class PrepareExecuteDDL extends BaseJDBCTestCase {
	
    /**
     * Connection to execute the DDL on. Needs
     * to be different to the single connection
     * provided by the super-class. This connection
     * is used to execute DDL while the other connection
     * has open objcts dependent on the objct changed by the DDL.
     */
	private Connection connDDL;
	
	/**
	 * List of statements that are prepared and then executed.
	 * The testPrepareExecute method prepares each statement
	 * in this list, executes one DDL, executes each prepared
	 * statement and then checks the result.
	 * <BR>
	 * The result checking is driven off the initial text
	 * of the statement.
	 */
	private static final String[] STMTS =
	{
		"SELECT * FROM PED001",
		"SELECT A, B FROM PED001",
		"GRANT SELECT ON PED001 TO U_PED_001",
		"GRANT SELECT(A,B) ON PED001 TO U_PED_001",
		"REVOKE SELECT(A,B) ON PED001 FROM U_PED_001",
		"REVOKE SELECT ON PED001 FROM U_PED_001",
	};
	
	/**
	 * All the DDL commands that will be executed, one per
	 * fixture, as the mutation between the prepare and execute.
	 */
	private static final String[] DDL =
	{
		"ALTER TABLE PED001 ADD COLUMN D BIGINT",
		"ALTER TABLE PED001 ADD CONSTRAINT PED001_PK PRIMARY KEY (A)",
		"ALTER TABLE PED001 LOCKSIZE ROW",
		"ALTER TABLE PED001 LOCKSIZE TABLE",
		"DROP TABLE PED001",
	};
	
	/**
	 * Create a suite of tests, one per statement in DDL.
     * This test is for testing the embedded dependency system
     * though possibly it could be used for testing in client
     * as well.
	 */
    public static Test suite() {
        BaseTestSuite suite = new BaseTestSuite("PrepareExecuteDDL");
        for (int i = 0; i < DDL.length; i++)
        	suite.addTest(new PrepareExecuteDDL("testPrepareExcute", DDL[i]));
        return TestConfiguration.sqlAuthorizationDecorator(suite);
    }
	private final String ddl;
	
	private PrepareExecuteDDL(String name, String ddl)
	{
		super(name);
		this.ddl = ddl;
	}
	
	private boolean tableDropped()
	{
		return ddl.startsWith("DROP TABLE ");
	}
	
	public void testPrepareExcute() throws SQLException
	{
        Connection conn = getConnection();
        
		PreparedStatement[] psa= new PreparedStatement[STMTS.length];
		for (int i = 0; i < STMTS.length; i++)
		{
			String sql = STMTS[i];
			psa[i] = conn.prepareStatement(sql);
		}
		
		connDDL.createStatement().execute(ddl);
		
		for (int i = 0; i < STMTS.length; i++)
		{
			String sql = STMTS[i];
			if (sql.startsWith("SELECT "))
				checkSelect(psa[i], sql);
			else if (sql.startsWith("GRANT ")
					|| sql.startsWith("REVOKE "))
				checkGrantRevoke(psa[i], sql);
			else
				fail("unknown SQL" + sql);
            
            psa[i].close();
		}
	}
	
	private void checkSelect(PreparedStatement ps, String sql)
	throws SQLException
	{
		assertEquals(true, sql.startsWith("SELECT "));
		
		boolean result;
		try {
			result = ps.execute();
		} catch (SQLException e) {
			
			//TODO: Use DMD to see if table exists or not.
			assertSQLState("42X05", e);
			assertTrue(tableDropped());
			
			return;
		}
		assertTrue(result);
		
		ResultSet rs = ps.getResultSet();
		
		DatabaseMetaData dmd = connDDL.getMetaData();
		JDBC.assertMetaDataMatch(dmd, rs.getMetaData());
		
		boolean isSelectStar = sql.startsWith("SELECT * ");
		
		if (isSelectStar)
			;
		
		JDBC.assertDrainResults(rs);
	}
	
	
	private void checkGrantRevoke(PreparedStatement ps, String sql)
	throws SQLException
	{
		assertEquals(true, sql.startsWith("GRANT ")
				|| sql.startsWith("REVOKE "));
		
		try {
			assertFalse(ps.execute());
		} catch (SQLException e) {
			
			assertSQLState("42X05", e);
			assertTrue(tableDropped());
			
			return;
		}
	}	
	/**
	 * Set the fixture up with a clean, standard table PED001.
	 */
	protected void setUp() throws SQLException
	{
		
		connDDL = openDefaultConnection();
		Statement s = connDDL.createStatement();
		
		s.execute(
		"CREATE TABLE PED001 (A INT NOT NULL, B DECIMAL(6,4), C VARCHAR(20))");
		
		s.close();
	}
	
	/**
	 * Tear-down the fixture by removing the table (if it still
	 * exists).
	 */
	protected void tearDown() throws Exception
	{
		Statement s = connDDL.createStatement();
		try {
			s.execute("DROP TABLE PED001");
		} catch (SQLException e) {
			assertSQLState("42Y55", e);
		}
		s.close();
		JDBC.cleanup(connDDL);
		connDDL = null;
        super.tearDown();
		
	}
}
