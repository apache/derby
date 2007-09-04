/*

Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.ParameterMetaDataJdbc30Test

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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Properties;
import java.sql.CallableStatement;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Types;
import java.math.BigDecimal;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestResult;
import junit.framework.TestSuite;

/**
 * Test the ParameterMetaData class in JDBC 30.
 * This test converts the old jdbcapi/parameterMetaDataJdbc30.java 
 * test to JUnit. 
 */

public class ParameterMetaDataJdbc30Test extends BaseJDBCTestCase {

	/**
         * Create a test with the given name.
         *
         * @param name name of the test.
         */

        public ParameterMetaDataJdbc30Test(String name) {
                super(name);
        }
        
	/**
         * Create suite containing client and embedded tests and to run
	 * all tests in this class
         */
	public static Test suite() {
        	TestSuite suite = new TestSuite("ParameterMetaDataJdbc30Test");
		 if (JDBC.vmSupportsJSR169())
			return new TestSuite("Empty ParameterMetaDataJDBC30. JSR169 does not support ParameterMetaData");
		else {	
        		suite.addTest(baseSuite("ParameterMetaDataJdbc30Test:embedded"));
        		suite.addTest(
                		TestConfiguration.clientServerDecorator(
                        		baseSuite("ParameterMetaDataJdbc30Test:client")));
		}
        	return suite;
    	}

	private static Test baseSuite(String name)
    	{
        	TestSuite suite = new TestSuite(name);

        	suite.addTestSuite(ParameterMetaDataJdbc30Test.class);

        	return new CleanDatabaseTestSetup(suite) {
			/**
             		 * Creates the tables and the stored procedures used in the test
             		 * cases.
			 *
             		 * @exception SQLException if a database error occurs
             		 */
            		protected void decorateSQL(Statement stmt) throws SQLException {
            		
				Connection conn = getConnection();

				/**
		                 * Creates the table used in the test cases.
               			 *
                 		 */
                		stmt.execute("create table t ( "+
                          		/* 1 */ "c char(5), "+
                          		/* 2 */ "iNoNull int not null, "+
                          		/* 3 */ "i int, "+
                          		/* 4 */ "de decimal, "+
                          		/* 5 */ "d date)");
                		stmt.executeUpdate("create function RDB(P1 INT) " +
                                		   "RETURNS DECIMAL(10,2) " +
                                  		   "language java external name " +
                                  		   "'org.apache.derbyTesting.functionTests." +
                                  		   "tests.lang.outparams30.returnsBigDecimal' " +
                                   		   "parameter style java");
            		}
        	};
  	}

       /**
     	* Testing a callable statement by calling a SQL procedure with 
	* IN parameters, OUT parameters and IN_OUT parameters.
        *
	* @exception SQLException if error occurs
     	*/
	public void testCallableStatement () throws SQLException {

        	Statement stmt = createStatement();

        	stmt.executeUpdate("create procedure dummyint( " + 
				   "in a integer, in b integer, " + 
				   "out c integer, inout d integer) " + 
				   "language java external name "+ 
				   "'org.apache.derbyTesting.functionTests." +
				   "tests.jdbcapi.ParameterMetaDataJdbc30Test.dummyint' " +
				   "parameter style java");
       		CallableStatement cs = prepareCall("CALL dummyint(?,?,?,?)");
		// parameters 1 and 2 are input only
      		cs.setInt(1,1);
      		cs.setInt(2,1);
      		//parameter 3 is output only
      		cs.registerOutParameter(3,Types.INTEGER);
      		// parameter 4 is input and output
      		Object x = new Integer(1);
      		cs.setObject(4,x, Types.INTEGER);
      		cs.registerOutParameter(4,Types.INTEGER);

      		//verify the meta data for the parameters
      		ParameterMetaData paramMetaData = cs.getParameterMetaData();
		assertEquals("Unexpected parameter count", 4, paramMetaData.getParameterCount());

		//expected values to be stored in a 2dim. array	
		String [][] parameterMetaDataArray0 = {
                //isNullable, isSigned, getPrecision, getScale, getParameterType, getParameterTypeName, getParameterClassName, getParameterMode
		{"PARAMETER_NULLABLE", "true", "10", "0", "4", "INTEGER", "java.lang.Integer", "PARAMETER_MODE_IN"},
		{"PARAMETER_NULLABLE", "true", "10", "0", "4", "INTEGER", "java.lang.Integer", "PARAMETER_MODE_IN"},
		{"PARAMETER_NULLABLE", "true", "10", "0", "4", "INTEGER", "java.lang.Integer", "PARAMETER_MODE_OUT"},
		{"PARAMETER_NULLABLE", "true", "10", "0", "4", "INTEGER", "java.lang.Integer", "PARAMETER_MODE_IN_OUT"}};
			
		testParameterMetaData(paramMetaData, parameterMetaDataArray0);

      		cs.execute();

		/*
		/* bug 4450 - parameter meta data info for the return parameter was giving
      		/* null pointer exception. In the past, we didn't need to keep the return
      		/* parameter info for callable statement execution and hence we never
      		/* generated the meta data for it. To fix the problem, at the parsing time,
      		/* I set a flag if the call statement is of ? = form. If so, the first
      		/* parameter is a return parameter and save it's meta data rather than
      		/* discarding it.
	 	 */	
	
      		cs = prepareCall("? = call RDB(?)");
      		paramMetaData = cs.getParameterMetaData();
		assertEquals("Unexpected parameter count", 2, paramMetaData.getParameterCount());

		//expected values to be stored in a 2dim. array
                String parameterMetaDataArray1 [][] = {
                //isNullable, isSigned, getPrecision, getScale, getParameterType, getParameterTypeName, getParameterClassName, getParameterMode
                {"PARAMETER_NULLABLE", "true", "31", "0", "3", "DECIMAL", "java.math.BigDecimal", "PARAMETER_MODE_OUT"},
                {"PARAMETER_NULLABLE", "true", "10", "0", "4", "INTEGER", "java.lang.Integer", "PARAMETER_MODE_IN"}};

                testParameterMetaData(paramMetaData, parameterMetaDataArray1);

		stmt.close();
		cs.close();
	}
	/**
         * Testing a prepared statement.
     	 *
	 * @exception SQLException if database access errors or other errors occur
         */
	public void testPreparedStatement () throws SQLException {
		//next testing a prepared statement
      		PreparedStatement ps = prepareStatement("insert into t values(?, ?, ?, ?, ?)");
      		ps.setNull(1, java.sql.Types.CHAR);
      		ps.setInt(2, 1);
      		ps.setNull(3, java.sql.Types.INTEGER);
      		ps.setBigDecimal(4,new BigDecimal("1"));
      		ps.setNull(5, java.sql.Types.DATE);

      		ParameterMetaData paramMetaData = ps.getParameterMetaData();
		assertEquals("Unexpected parameter count", 5, paramMetaData.getParameterCount());

		//expected values to be stored in a 2dim. array
                String [][] parameterMetaDataArray0 = {
                //isNullable, isSigned, getPrecision, getScale, getParameterType, getParameterTypeName, getParameterClassName, getParameterMode
                {"PARAMETER_NULLABLE", "false", "5", "0", "1", "CHAR", "java.lang.String", "PARAMETER_MODE_IN"},
                {"PARAMETER_NULLABLE", "true", "10", "0", "4", "INTEGER", "java.lang.Integer", "PARAMETER_MODE_IN"},
                {"PARAMETER_NULLABLE", "true", "10", "0", "4", "INTEGER", "java.lang.Integer", "PARAMETER_MODE_IN"},
                {"PARAMETER_NULLABLE", "true", "5", "0", "3", "DECIMAL", "java.math.BigDecimal", "PARAMETER_MODE_IN"},
                {"PARAMETER_NULLABLE", "false", "10", "0", "91", "DATE", "java.sql.Date", "PARAMETER_MODE_IN"}};

                testParameterMetaData(paramMetaData, parameterMetaDataArray0);
	
		/*
      		 *  JCC seems to report these parameters as MODE_UNKNOWN, where as Derby uses MODE_IN
      		 *  JCC behaviour with network server matches its behaviour with DB2
      		 *  getPrecision() returns 0 for CHAR/DATE/BIT types for Derby. JCC shows maxlen
		 */
      		ps.execute();

		/*
		 * bug 4533 - associated parameters should not be included in the parameter meta data list
      		 * Following statement systab will generate 4 associated parameters for the 2
      		 * user parameters. This results in total 6 parameters for the prepared statement
      		 * internally. But we should only show 2 user visible parameters through
      		 * getParameterMetaData().
		 */
      		ps = prepareStatement("select * from sys.systables where " +
             			      " CAST(tablename AS VARCHAR(128)) like ? and CAST(tableID AS CHAR(36)) like ?");
      		ps.setString (1, "SYS%");
      		ps.setString (2, "8000001%");
      		paramMetaData = ps.getParameterMetaData();
		assertEquals("Unexpected parameter count", 2, paramMetaData.getParameterCount());

		//expected values to be stored in a 2dim. array
                String parameterMetaDataArray1 [][] = {
                //isNullable, isSigned, getPrecision, getScale, getParameterType, getParameterTypeName, getParameterClassName, getParameterMode
                {"PARAMETER_NULLABLE", "false", "128", "0", "12", "VARCHAR", "java.lang.String", "PARAMETER_MODE_IN"},
                {"PARAMETER_NULLABLE", "false", "36", "0", "1", "CHAR", "java.lang.String", "PARAMETER_MODE_IN"}};

                testParameterMetaData(paramMetaData, parameterMetaDataArray1);

      		ps.execute();

		ps.close();
	}

	/** 
	 * DERBY-44 added support for SELECT ... WHERE column LIKE ? ESCAPE ?
         * This test case tests
         *   a) that such a statement compiles, and
         *   b) that we get the correct error message if the escape
         *      sequence is an empty string (at one point this would
         *      lead to a StringIndexOutOfBoundsException)`
	 *
	 * @exception SQLException if error occurs
	 */
	public void testLikeEscaleStatement () throws SQLException {

      		//variation 1, testing DERBY-44 
      		PreparedStatement ps = prepareStatement("select * from sys.systables " +
							"where CAST(tablename AS VARCHAR(128)) like ? escape CAST(? AS VARCHAR(128))");
      		ps.setString (1, "SYS%");
      		ps.setString (2, "");
      		ParameterMetaData paramMetaData = ps.getParameterMetaData();
		assertEquals("Unexpected parameter count", 2, paramMetaData.getParameterCount());

		//expected values to be stored in a 2dim. array
                String parameterMetaDataArray0 [][] = {
                //isNullable, isSigned, getPrecision, getScale, getParameterType, getParameterTypeName, getParameterClassName, getParameterMode
                {"PARAMETER_NULLABLE", "false", "128", "0", "12", "VARCHAR", "java.lang.String", "PARAMETER_MODE_IN"},
                {"PARAMETER_NULLABLE", "false", "128", "0", "12", "VARCHAR", "java.lang.String", "PARAMETER_MODE_IN"}};

                testParameterMetaData(paramMetaData, parameterMetaDataArray0);

      		try {
          		ResultSet rs = ps.executeQuery();
          		rs.next();
			fail("DERBY-44 failed (didn't get SQLSTATE 22019)");
          		rs.close();
      		} catch (SQLException e) {
			assertSQLState("22019", e);
      		}
		ps.close();
	}

	 /** 
          * test execute statements that no parameters would be returned if 
          * prepareStatement("execute statement systab using values('SYS%','8000001%')");
	  *
	  * @exception SQLException if error occurs
          */

	public void testExecuteStatementUsing () throws SQLException {
	
		/*
		 * the test no longer tests 4552, but kept as an interesting test scenario
                 * bug 4552 - no parameters would be returned for execute statement using
                 * System.out.println("Bug 4552 - no parameters would be returned for execute statement using");
                 * orig: ps = con.prepareStatement("execute statement systab using values('SYS%','8000001%')");
		 */
      		PreparedStatement ps = prepareStatement("select * from sys.systables " + 
							"where CAST(tablename AS VARCHAR(128)) like 'SYS%' and " + 
							"CAST(tableID AS VARCHAR(128)) like '8000001%'");

      		ParameterMetaData paramMetaData = ps.getParameterMetaData();
		assertEquals("Unexpected parameter count", 0, paramMetaData.getParameterCount());

		//expected values to be stored in a 2dim. array
                String parameterMetaDataArray0 [][] = null;

                testParameterMetaData(paramMetaData, parameterMetaDataArray0);

      		ps.execute();

		ps.close();
	}
	/** 
         * testing SELECT statements for BOOLEANs (1 for true, 0 for false) 
	 *
	 * @exception SQLException if error occurs
	 */ 
	public void testSelectStatementUsingBoolean () throws SQLException {

		/*
		 * Bug 4654 - Null Pointer exception while executuing a select with a
         	 * where clause parameter of type 'TRUE' or 'FALSE' constants. The existing prior to
         	 * exposing parameter metadata didn't need to fill in metadata information for where
         	 * clause parameter in the example above.
         	 * This no longer makes sense, for we cannot take BOOLEANs anymore.
         	 * replace with a simple where 1 = ?. Which would take either 1 for true, or 0 for false
		 */
      		PreparedStatement ps = prepareStatement("select * from t "+  
							"where 1=? for update");

      		ParameterMetaData paramMetaData = ps.getParameterMetaData();
		assertEquals("Unexpected parameter count", 1, paramMetaData.getParameterCount());

		//expected values to be stored in a 2dim. array
                String parameterMetaDataArray0 [][] = {
                //isNullable, isSigned, getPrecision, getScale, getParameterType, getParameterTypeName, getParameterClassName, getParameterMode
                {"PARAMETER_NULLABLE", "true", "10", "0", "4", "INTEGER", "java.lang.Integer", "PARAMETER_MODE_IN"}};

                testParameterMetaData(paramMetaData, parameterMetaDataArray0);

      		dumpParameterMetaDataNegative(paramMetaData);
      		ps.setInt(1,1);
      		ps.execute();

		ps.close();
	}
	/** 
         * test: no parameter for the statement and then do getParameterMetaData() 
	 *
	 * @exception SQLException if error occurs
         */
	public void testSelectStatementUsingNoParameter () throws SQLException {

      		PreparedStatement ps = prepareStatement("select * from t");
      		ParameterMetaData paramMetaData = ps.getParameterMetaData();
		assertEquals("Unexpected parameter count", 0, paramMetaData.getParameterCount());
		
		//expected values to be stored in a 2dim. array
                String parameterMetaDataArray0 [][] = null; 

                testParameterMetaData(paramMetaData, parameterMetaDataArray0);

      		ps.execute();

		ps.close();
	}
	/**
         * test: the scale returned should be the one set by registerOutParameter 
	 *
 	 * @exception SQLException
         */
	public void testCallableStatementReturnedScale () throws SQLException {

		/*
                 *  DERBY-2810 - getParameterType behavior is different in Embedded and 
                 *  Network Client when set by registerOutParameter 
                 *  temporarily disabling Network Client.
                 */
		if (!usingDerbyNetClient()) {
			Statement stmt = createStatement();
      			stmt.executeUpdate("create procedure dummy_numeric_Proc(out a NUMERIC(30,15), out b NUMERIC(30,15)) language java parameter style java external name 'org.apache.derbyTesting.functionTests.tests.jdbcapi.ParameterMetaDataJdbc30Test.dummy_numeric_Proc'");
      			CallableStatement cs = prepareCall("CALL dummy_numeric_Proc(?,?)");
      			cs.registerOutParameter(1, Types.NUMERIC);
      			cs.registerOutParameter(2, Types.NUMERIC,15);
      			cs.execute();
			assertEquals("Unexpected parameter count", 2, cs.getParameterMetaData().getParameterCount());

			//expected values to be stored in a 2dim. array
                	String parameterMetaDataArray0 [][] = {
                	//isNullable, isSigned, getPrecision, getScale, getParameterType, getParameterTypeName, getParameterClassName, getParameterMode
                	{"PARAMETER_NULLABLE", "true", "30", "15", "2", "NUMERIC", "java.math.BigDecimal", "PARAMETER_MODE_OUT"},
                	{"PARAMETER_NULLABLE", "true", "30", "15", "2", "NUMERIC", "java.math.BigDecimal", "PARAMETER_MODE_OUT"}};

                	testParameterMetaData(cs.getParameterMetaData(), parameterMetaDataArray0);
		
			cs.close();
		}
	}
	/**
         * test behaviour of meta data and out params after re-compile 
	 *
	 * @exception SQLException if error occurs
         */
	public void testMetatdataAfterProcRecompile () throws SQLException {

		Statement stmt = createStatement();
      		CallableStatement cs = prepareCall("CALL dummyint(?,?,?,?)");
          	cs.registerOutParameter(3,Types.INTEGER);
      		cs.registerOutParameter(4,Types.INTEGER);
      		cs.setInt(1,1);
      		cs.setInt(2,1);
          	cs.setInt(4,4);
	
		//expected values to be stored in a 2dim. array
		String parameterMetaDataArray0 [][] = {
                //isNullable, isSigned, getPrecision, getScale, getParameterType, getParameterTypeName, getParameterClassName, getParameterMode
                {"PARAMETER_NULLABLE", "true", "10", "0", "4", "INTEGER", "java.lang.Integer", "PARAMETER_MODE_IN"},
                {"PARAMETER_NULLABLE", "true", "10", "0", "4", "INTEGER", "java.lang.Integer", "PARAMETER_MODE_IN"},
                {"PARAMETER_NULLABLE", "true", "10", "0", "4", "INTEGER", "java.lang.Integer", "PARAMETER_MODE_OUT"},
                {"PARAMETER_NULLABLE", "true", "10", "0", "4", "INTEGER", "java.lang.Integer", "PARAMETER_MODE_IN_OUT"}};

                testParameterMetaData(cs.getParameterMetaData(), parameterMetaDataArray0);

          	cs.execute();
		assertEquals("Unexpected DUMMYINT alias returned", 11111, cs.getInt(4));

		/* 
		 *  DERBY-2786 - Behaviour of inout parameters in Embedded and Network client is
         	 *  different if parameters are set but the CallableStatment is not executed.
		 *  temporarily disabling Network Client. 
         	 */
		if (!usingDerbyNetClient()) {
      			stmt.executeUpdate("drop procedure dummyint");
      			stmt.executeUpdate("create procedure dummyint(in a integer, in b integer, out c integer, inout d integer) language java external name 'org.apache.derbyTesting.functionTests.tests.jdbcapi.ParameterMetaDataJdbc30Test.dummyint2' parameter style java");
      			cs.execute();

			String parameterMetaDataArray1 [][] = {
                	//isNullable, isSigned, getPrecision, getScale, getParameterType, getParameterTypeName, getParameterClassName, getParameterMode
                	{"PARAMETER_NULLABLE", "true", "10", "0", "4", "INTEGER", "java.lang.Integer", "PARAMETER_MODE_IN"},
                	{"PARAMETER_NULLABLE", "true", "10", "0", "4", "INTEGER", "java.lang.Integer", "PARAMETER_MODE_IN"},
                	{"PARAMETER_NULLABLE", "true", "10", "0", "4", "INTEGER", "java.lang.Integer", "PARAMETER_MODE_OUT"},
                	{"PARAMETER_NULLABLE", "true", "10", "0", "4", "INTEGER", "java.lang.Integer", "PARAMETER_MODE_IN_OUT"}};

                	testParameterMetaData(cs.getParameterMetaData(), parameterMetaDataArray1);

      			cs.setInt(4, 6);
      			// following is incorrect sequence, should execute first, then get
      			// but leaving it in as an additional negative test. see beetle 5886
			assertEquals("Unexpected DUMMYINT alias returned", 6, cs.getInt(4));

      			cs.execute();
			assertEquals("Unexpected DUMMYINT alias returned", 22222, cs.getInt(4));
		}
      		cs.close();
	}
	/**
         * test ParameterMetaData for Java procedures with INTEGER parameters 
	 * 
	 * @exception SQLException if error occurs
         */
	public void testParameterMetadataWithINTParameters () throws SQLException {

		Statement stmt = createStatement();
       		stmt.execute("CREATE PROCEDURE PMDI(IN pmdI_1 INTEGER, IN pmdI_2 INTEGER, INOUT pmdI_3 INTEGER, OUT pmdI_4 INTEGER) language java parameter style java external name 'org.apache.derbyTesting.functionTests.tests.jdbcapi.ParameterMetaDataJdbc30Test.dummyint'");
      		CallableStatement cs = prepareCall("CALL PMDI(?, ?, ?, ?)");
	
		// parameters 1 and 2 are input only
               	cs.setInt(1,1);
        	cs.setInt(2,1);
		// parameter 3 is input and output
                Object x = new Integer(1);
                cs.setObject(3,x, Types.INTEGER);
                cs.registerOutParameter(3,Types.INTEGER);
               	//parameter 4 is output only
               	cs.registerOutParameter(4,Types.INTEGER);

               	//verify the meta data for the parameters
               	ParameterMetaData paramMetaData = cs.getParameterMetaData();
               	assertEquals("Unexpected parameter count", 4, paramMetaData.getParameterCount());

		//expected values to be stored in a 2dim. array
                String parameterMetaDataArray0 [][] = {
                //isNullable, isSigned, getPrecision, getScale, getParameterType, getParameterTypeName, getParameterClassName, getParameterMode
                {"PARAMETER_NULLABLE", "true", "10", "0", "4", "INTEGER", "java.lang.Integer", "PARAMETER_MODE_IN"},
                {"PARAMETER_NULLABLE", "true", "10", "0", "4", "INTEGER", "java.lang.Integer", "PARAMETER_MODE_IN"},
                {"PARAMETER_NULLABLE", "true", "10", "0", "4", "INTEGER", "java.lang.Integer", "PARAMETER_MODE_IN_OUT"},
                {"PARAMETER_NULLABLE", "true", "10", "0", "4", "INTEGER", "java.lang.Integer", "PARAMETER_MODE_OUT"}};

                testParameterMetaData(cs.getParameterMetaData(), parameterMetaDataArray0);
			
       		cs.close();
       		stmt.execute("DROP PROCEDURE PMDI");
            stmt.close();
	}
	/**
         * test ParameterMetaData for Java procedures with CHAR parameters
	 *
	 * @exception SQLException if error occurs
         */
	 public void testParameterMetadataWithCHARParameters () throws SQLException {

		Statement stmt = createStatement();
       		stmt.execute("CREATE PROCEDURE PMDC(IN pmdI_1 CHAR(10), IN pmdI_2 VARCHAR(25), INOUT pmdI_3 CHAR(19), OUT pmdI_4 VARCHAR(32)) language java parameter style java external name 'org.apache.derbyTesting.functionTests.tests.jdbcapi.ParameterMetaDataJdbc30Test.dummyString'");
      		CallableStatement cs = prepareCall("CALL PMDC(?, ?, ?, ?)");
		// parameters 1 and 2 are input only	
		cs.setString(1, "TEST0");
		cs.setString(2, "TEST1");
		// parameter 3 is input and output
                Object x = new String("TEST");
                cs.setObject(3,x, Types.CHAR);
                cs.registerOutParameter(3,Types.CHAR);
                //parameter 4 is output only
                cs.registerOutParameter(4,Types.CHAR);
 		//verify the meta data for the parameters
               	ParameterMetaData paramMetaData = cs.getParameterMetaData();
		assertEquals("Unexpected parameter count", 4, paramMetaData.getParameterCount());
	
		//expected values to be stored in a 2dim. array
                String parameterMetaDataArray0 [][] = {
                //isNullable, isSigned, getPrecision, getScale, getParameterType, getParameterTypeName, getParameterClassName, getParameterMode
                {"PARAMETER_NULLABLE", "false", "10", "0", "1", "CHAR", "java.lang.String", "PARAMETER_MODE_IN"},
                {"PARAMETER_NULLABLE", "false", "25", "0", "12", "VARCHAR", "java.lang.String", "PARAMETER_MODE_IN"},
                {"PARAMETER_NULLABLE", "false", "19", "0", "1", "CHAR", "java.lang.String", "PARAMETER_MODE_IN_OUT"},
                {"PARAMETER_NULLABLE", "false", "32", "0", "12", "VARCHAR", "java.lang.String", "PARAMETER_MODE_OUT"}};

		testParameterMetaData(cs.getParameterMetaData(), parameterMetaDataArray0);

       		cs.close();
       		stmt.execute("DROP PROCEDURE PMDC");
            stmt.close();
	}
	/**
         *  test ParameterMetaData for Java procedures with DECIMAL parameters
	 *
	 * @exception SQLException if error occurs
         */
	public void testParameterMetadataWithDECIMALParameters () throws SQLException {

		Statement stmt = createStatement();
       		stmt.execute("CREATE PROCEDURE PMDD(IN pmdI_1 DECIMAL(5,3), IN pmdI_2 DECIMAL(4,2), INOUT pmdI_3 DECIMAL(9,0), OUT pmdI_4 DECIMAL(10,2)) language java parameter style java external name 'org.apache.derbyTesting.functionTests.tests.jdbcapi.ParameterMetaDataJdbc30Test.dummyDecimal'");
      		CallableStatement cs = prepareCall("CALL PMDD(?, ?, ?, ?)");

		// parameters 1 and 2 are input only
                cs.setBigDecimal(1,new BigDecimal("1"));;
                cs.setBigDecimal(2,new BigDecimal("1"));;
                // parameter 3 is input and output
                Object x = new BigDecimal(1.1);
                cs.setObject(3,x, Types.DECIMAL);
                cs.registerOutParameter(3,Types.DECIMAL);
                //parameter 4 is output only
                cs.registerOutParameter(4,Types.DECIMAL);
		//verify the meta data for the parameters
              	ParameterMetaData paramMetaData = cs.getParameterMetaData();
		assertEquals("Unexpected parameter count", 4, paramMetaData.getParameterCount());

		//expected values to be stored in a 2dim. array
                String parameterMetaDataArray0 [][] = {
                //isNullable, isSigned, getPrecision, getScale, getParameterType, getParameterTypeName, getParameterClassName, getParameterMode
                {"PARAMETER_NULLABLE", "true", "5", "3", "3", "DECIMAL", "java.math.BigDecimal", "PARAMETER_MODE_IN"},
                {"PARAMETER_NULLABLE", "true", "4", "2", "3", "DECIMAL", "java.math.BigDecimal", "PARAMETER_MODE_IN"},
                {"PARAMETER_NULLABLE", "true", "9", "0", "3", "DECIMAL", "java.math.BigDecimal", "PARAMETER_MODE_IN_OUT"},
                {"PARAMETER_NULLABLE", "true", "10", "2", "3", "DECIMAL", "java.math.BigDecimal", "PARAMETER_MODE_OUT"}};

                testParameterMetaData(cs.getParameterMetaData(), parameterMetaDataArray0);

       		cs.close();
	}
	/**
         * test ParameterMetaData for Java procedures with some literal parameters
	 *
	 * @exception SQLException if error occurs
         */
	public void testParameterMetadataWithLITERALParameters () throws SQLException {

		Statement stmt = createStatement();
      		CallableStatement cs = prepareCall("CALL PMDD(32.4, ?, ?, ?)");
		// parameters 2 is input only
                cs.setBigDecimal(1,new BigDecimal("1"));;
                // parameter 3 is input and output
                Object x = new BigDecimal(1.1);
                cs.setObject(2,x, Types.DECIMAL);
                cs.registerOutParameter(2,Types.DECIMAL);
                //parameter 4 is output only
                cs.registerOutParameter(3,Types.DECIMAL);

		//verify the meta data for the parameters
                ParameterMetaData paramMetaData = cs.getParameterMetaData();
                assertEquals("Unexpected parameter count", 3, paramMetaData.getParameterCount());

		//expected values to be stored in a 2dim. array
                String parameterMetaDataArray0 [][] = {
                //isNullable, isSigned, getPrecision, getScale, getParameterType, getParameterTypeName, getParameterClassName, getParameterMode
                {"PARAMETER_NULLABLE", "true", "4", "2", "3", "DECIMAL", "java.math.BigDecimal", "PARAMETER_MODE_IN"},
                {"PARAMETER_NULLABLE", "true", "9", "0", "3", "DECIMAL", "java.math.BigDecimal", "PARAMETER_MODE_IN_OUT"},
                {"PARAMETER_NULLABLE", "true", "10", "2", "3", "DECIMAL", "java.math.BigDecimal", "PARAMETER_MODE_OUT"}};

                testParameterMetaData(cs.getParameterMetaData(), parameterMetaDataArray0);

          	cs.close();

      		cs = prepareCall("CALL PMDD(32.4, 47.9, ?, ?)");
		// parameter 3 is input and output
                Object y = new BigDecimal(1.1);
                cs.setObject(1,y, Types.DECIMAL);
                cs.registerOutParameter(1,Types.DECIMAL);
                //parameter 4 is output only
                cs.registerOutParameter(2,Types.DECIMAL);
		paramMetaData = cs.getParameterMetaData();
                assertEquals("Unexpected parameter count", 2, paramMetaData.getParameterCount());

		//expected values to be stored in a 2dim. array
                String parameterMetaDataArray1 [][] = {
                //isNullable, isSigned, getPrecision, getScale, getParameterType, getParameterTypeName, getParameterClassName, getParameterMode
                {"PARAMETER_NULLABLE", "true", "9", "0", "3", "DECIMAL", "java.math.BigDecimal", "PARAMETER_MODE_IN_OUT"},
                {"PARAMETER_NULLABLE", "true", "10", "2", "3", "DECIMAL", "java.math.BigDecimal", "PARAMETER_MODE_OUT"}};

                testParameterMetaData(cs.getParameterMetaData(), parameterMetaDataArray1);

          	cs.close();

      		cs = prepareCall("CALL PMDD(?, 38.2, ?, ?)");
		// parameters 1 is input only
                cs.setBigDecimal(1,new BigDecimal("1"));;
                // parameter 3 is input and output
                Object z = new BigDecimal(1.1);
                cs.setObject(2,z, Types.DECIMAL);
                cs.registerOutParameter(2,Types.DECIMAL);
                //parameter 4 is output only
                cs.registerOutParameter(3,Types.DECIMAL);

		//verify the meta data for the parameters
                paramMetaData = cs.getParameterMetaData();
		assertEquals("Unexpected parameter count", 3, paramMetaData.getParameterCount());

		//expected values to be stored in a 2dim. array
                String parameterMetaDataArray2 [][] = {
                //isNullable, isSigned, getPrecision, getScale, getParameterType, getParameterTypeName, getParameterClassName, getParameterMode
                {"PARAMETER_NULLABLE", "true", "5", "3", "3", "DECIMAL", "java.math.BigDecimal", "PARAMETER_MODE_IN"},
                {"PARAMETER_NULLABLE", "true", "9", "0", "3", "DECIMAL", "java.math.BigDecimal", "PARAMETER_MODE_IN_OUT"},
                {"PARAMETER_NULLABLE", "true", "10", "2", "3", "DECIMAL", "java.math.BigDecimal", "PARAMETER_MODE_OUT"}};

                testParameterMetaData(cs.getParameterMetaData(), parameterMetaDataArray2);

          	cs.close();
          	stmt.execute("DROP PROCEDURE PMDD");
            stmt.close();
	}
	/**
         * print the parameter isNullable value in human readable form
	 *
	 * @param nullabilityValue 
         */
	// @return the nullability status of the given parameter
	static String parameterIsNullableInStringForm(int nullabilityValue){
		if (nullabilityValue ==  ParameterMetaData.parameterNoNulls)
				  return("PARAMETER_NO_NULLS");
		else if (nullabilityValue ==  ParameterMetaData.parameterNullable)
				  return("PARAMETER_NULLABLE");
		else if (nullabilityValue ==  ParameterMetaData.parameterNullableUnknown)
				  return("PARAMETER_NULLABLE_UNKNOWN");
		else
				  return("ERROR: donot recognize this parameter isNullable() value");
  	}

	/**
         * print the parameter mode in human readable form
         *
         * @param mode identifies parameter's mode (IN, OUT, or IN_OUT)
	 * @return     the parameter mode in readable form
         *             
         */
	static String parameterModeInStringForm(int mode){
		if (mode ==  ParameterMetaData.parameterModeIn)
				  return("PARAMETER_MODE_IN");
		else if (mode ==  ParameterMetaData.parameterModeInOut )
				  return("PARAMETER_MODE_IN_OUT");
		else if (mode ==  ParameterMetaData.parameterModeOut)
				  return("PARAMETER_MODE_OUT");
		else if (mode ==  ParameterMetaData.parameterModeUnknown)
				  return("PARAMETER_MODE_UNKNOWN");
		else
				  return("ERROR: donot recognize this parameter mode");
  	}
	/**
         * tests parameterMetaData and reports error if the ParameterMetaData results
	 * does not match the expected results.
         *
         * @param paramMetaData ParameterMetadata object
	 * @param paramMetaDataArray 2 dimensional array containing expected test results. 
	 * @exception SQLException if any error occurs
         */
	static void testParameterMetaData(ParameterMetaData paramMetaData, String [][] paramMetaDataArray) throws SQLException {
		int numParam = paramMetaData.getParameterCount();
		
		for (int i=0, j=0; i<numParam; i++) {	
                       	assertEquals("Unexpected parameter isNullable", paramMetaDataArray[i][j++], parameterIsNullableInStringForm(paramMetaData.isNullable(i+1)));
                       	assertEquals("Unexpected parameter isSigned", Boolean.valueOf(paramMetaDataArray[i][j++]).booleanValue(), paramMetaData.isSigned(i+1));
                       	assertEquals("Unexpected parameter getPrecision", Integer.parseInt(paramMetaDataArray[i][j++]) ,  paramMetaData.getPrecision(i+1));
                       	assertEquals("Unexpected parameter getScale", Integer.parseInt(paramMetaDataArray[i][j++]) , paramMetaData.getScale(i+1));
                       	assertEquals("Unexpected parameter getParameterType", Integer.parseInt(paramMetaDataArray[i][j++]) , paramMetaData.getParameterType(i+1));
                       	assertEquals("Unexpected parameter getParameterTypeName", paramMetaDataArray[i][j++] , paramMetaData.getParameterTypeName(i+1));
                       	assertEquals("Unexpected parameter getParameterClassName", paramMetaDataArray[i][j++] , paramMetaData.getParameterClassName(i+1));
                       	assertEquals("Unexpected parameter getParameterMode", paramMetaDataArray[i][j++] , parameterModeInStringForm(paramMetaData.getParameterMode(i+1)));
			
			j=0;
		}
	}
 	/**
         * ParameterMetaData Negative Test 
         *
         * @param paramMetaData ParameterMetadata object
         * @exception SQLException if any error occurs
         */	
	static void dumpParameterMetaDataNegative(ParameterMetaData paramMetaData) throws SQLException {

                int numParam = paramMetaData.getParameterCount();

		/*
                *  DERBY-3039 - ParameterMetaData.isNullable() returns differenet SQLState in Embedded  
                *  and Network Client 
                */
		String expectedSQLState = (usingEmbedded())?"XCL13":"XCL14";
	
		try {
       			paramMetaData.isNullable(-1);
       			fail("parameterMetaData.isNullable(-1) should have failed");
   		} catch (SQLException se)
       		{
       			assertSQLState(expectedSQLState, se);
       		}
		try {
                        paramMetaData.isNullable(0);
                        fail("parameterMetaData.isNullable(0) should have failed");
                } catch (SQLException se)
                {
                        assertSQLState(expectedSQLState, se);
                }
		try {
                        paramMetaData.isNullable(numParam+1);
                        fail("parameterMetaData.isNullable("+(numParam+1)+") should have failed");
                } catch (SQLException se)
                {
                        assertSQLState(expectedSQLState, se);
                }

	}
	/**
         * A simple method to test callable statement. This is the Java method 
	 * for procedure dummyint.
         *
         * @param in_param 
	 * @param in_param2  
	 * @param in_param3 
	 * @param in_param4
         * @exception SQLException
         */
        public static void dummyint (int in_param, int in_param2, int[] in_param3, int[] in_param4)
                                                                   throws SQLException {

                in_param4[0] = 11111;
        }

	/**
         *  This is the Java method for procedure dummyint.
         *
         * @param in_param  
         * @param in_param2 
         * @param in_param3 
         * @param in_param4 
         * @exception SQLException
         */
        public static void dummyint2 (int in_param, int in_param2, int[] in_param3, int[] in_param4)
                                                                   throws SQLException {
                in_param4[0] = 22222;
        }

	/**
	 * A really simple method to test callable statement. This is the Java method
         * for procedure dummy_numeric_Proc.
         *
         * @param max_param  
         * @param min_param  
         * @exception SQLException
         */
        public static void dummy_numeric_Proc (BigDecimal[] max_param,BigDecimal[] min_param)
                                                                 throws SQLException {
        }
	
	/**
         * Java method for procedure PMDC which tests ParameterMetaData for Java procedures 
         * with CHAR parameters.  
	 * 
         * @param in_param 
         * @param in_param2
         * @param in_param3 
         * @param in_param4 
         */
        public static void dummyString (String in_param, String in_param2, String[] in_param3, String[] in_param4) {
        }
	
	/**
         * Java method for procedure PMDD which tests ParameterMetaData for Java procedures
	 * with DECIMAL parameters. 
         *
         * @param in_param  
         * @param in_param2 
         * @param in_param3 
         * @param in_param4 
         */
        public static void dummyDecimal(BigDecimal in_param, BigDecimal in_param2, BigDecimal[] in_param3, BigDecimal[] in_param4) {
	 }
}
