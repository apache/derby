/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.parameterMetaDataJdbc30

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Types;

import java.math.BigDecimal;

import org.apache.derby.tools.ij;

/**
 * Test the new class ParameterMetaData in jdbc 30.
 * Testing both callable and prepared statements meta data
 *
 * @author mamta
 */


public class parameterMetaDataJdbc30 {
	private static boolean isDerbyNet;
	public static void main(String[] args) {
		Connection con = null;
		Statement  s;
		CallableStatement  cs;
		PreparedStatement  ps;
		ParameterMetaData paramMetaData;

		System.out.println("Test parameterMetaDataJdbc30 starting");

		try
		{
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			con = ij.startJBMS();
			con.setAutoCommit(true); // make sure it is true
			String framework = System.getProperty("framework");
			if (framework != null && framework.toUpperCase().equals("DERBYNET"))
			isDerbyNet = true;

			s = con.createStatement();

			/* Create the table and do any other set-up */
			setUpTest(s);

      s.executeUpdate("create function RDB(P1 INT) RETURNS DECIMAL(10,2) language java external name 'org.apache.derbyTesting.functionTests.tests.lang.outparams.returnsBigDecimal' parameter style java");

      //first testing a callable statement
      s.executeUpdate("create procedure dummyint(in a integer, in b integer, out c integer, inout d integer) language java external name 'org.apache.derbyTesting.functionTests.tests.jdbcapi.parameterMetaDataJdbc30.dummyint' parameter style java");
      cs = con.prepareCall("CALL dummyint(?,?,?,?)");

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
      paramMetaData = cs.getParameterMetaData();
      System.out.println("parameters count for callable statement is " + paramMetaData.getParameterCount());
      // TODO: Some of the OUT params are getting reported as IN_OUT for embedded.
      // Network server reports it correctly.
      dumpParameterMetaData(paramMetaData);
      cs.execute();

      //bug 4450 - parameter meta data info for the return parameter was giving
      //null pointer exception. In the past, we didn't need to keep the return
      //parameter info for callable statement execution and hence we never
      //generated the meta data for it. To fix the problem, at the parsing time,
      //I set a flag if the call statement is of ? = form. If so, the first
      //parameter is a return parameter and save it's meta data rather than
      //discarding it.
      System.out.println("Bug 4450 - generate metadata for return parameter");
      cs = con.prepareCall("? = call RDB(?)");
      paramMetaData = cs.getParameterMetaData();
      System.out.println("param count is: "+paramMetaData.getParameterCount());
      dumpParameterMetaData(paramMetaData);

      //next testing a prepared statement
      ps = con.prepareStatement("insert into t values(?, ?, ?, ?, ?)");
      ps.setNull(1, java.sql.Types.CHAR);
      ps.setInt(2, 1);
      ps.setNull(3, java.sql.Types.INTEGER);
      ps.setBigDecimal(4,new BigDecimal("1"));
      ps.setNull(5, java.sql.Types.DATE);

      paramMetaData = ps.getParameterMetaData();
      System.out.println("parameters count for prepared statement is " + paramMetaData.getParameterCount());
      // JCC seems to report these parameters as MODE_UNKNOWN, where as Derby uses MODE_IN
      // JCC behaviour with network server matches its behaviour with DB2
      // getPrecision() returns 0 for CHAR/DATE/BIT types for Derby. JCC shows maxlen
      dumpParameterMetaData(paramMetaData);
      ps.execute();

      //bug 4533 - associated parameters should not be included in the parameter meta data list
      //Following statement systab will generate 4 associated parameters for the 2
      //user parameters. This results in total 6 parameters for the prepared statement
      //internally. But we should only show 2 user visible parameters through
      //getParameterMetaData().
      System.out.println("Bug 4533 - hide associated parameters");
      ps = con.prepareStatement("select * from sys.systables where " +
             " tablename like ? and tableID like ?");
      ps.setString (1, "SYS%");
      ps.setString (2, "8000001%");
      paramMetaData = ps.getParameterMetaData();
      System.out.println("parameters count for prepared statement is " + paramMetaData.getParameterCount());
      dumpParameterMetaData(paramMetaData);
      ps.execute();

      // variation, and also test out empty string in the escape (jira 44). 
      System.out.println("variation 1, testing jira 44");
      ps = con.prepareStatement("select * from sys.systables where tablename like ? escape ?");
      ps.setString (1, "SYS%");
      ps.setString (2, "");
      paramMetaData = ps.getParameterMetaData();
      System.out.println("parameters count for prepared statement is " + paramMetaData.getParameterCount());
      dumpParameterMetaData(paramMetaData);
      ps.execute();

      // the test no longer tests 4552, but kept as an interesting test scenario 
      // bug 4552 - no parameters would be returned for execute statement using
      // System.out.println("Bug 4552 - no parameters would be returned for execute statement using");
      // orig: ps = con.prepareStatement("execute statement systab using values('SYS%','8000001%')");
      ps = con.prepareStatement("select * from sys.systables where tablename like 'SYS%' and tableID like '8000001%'");

      paramMetaData = ps.getParameterMetaData();
      System.out.println("parameters count for prepared statement is " + paramMetaData.getParameterCount());
      dumpParameterMetaData(paramMetaData);
      ps.execute();

      //Bug 4654 - Null Pointer exception while executuing a select with a
      //where clause parameter of type 'TRUE' or 'FALSE' constants. The existing prior to
      //exposing parameter metadata didn't need to fill in metadata information for where
      //clause parameter in the example above.
      // This no longer makes sense, for we cannot take BOOLEANs anymore.
      // replace with a simple where 1 = ?. Which would take either 1 for true, or 0 for false 
      System.out.println("Bug 4654 - fill in where clause parameter type info");
      ps = con.prepareStatement("select * from t where 1=? for update");

      paramMetaData = ps.getParameterMetaData();
      System.out.println("parameters count for prepared statement is " + paramMetaData.getParameterCount());
      dumpParameterMetaData(paramMetaData);
      dumpParameterMetaDataNegative(paramMetaData);
      //ps.setBoolean(1,true);
      ps.setInt(1,1);
      ps.execute();

      System.out.println("test: no parameter for the statement and then do getParameterMetaData()");
      ps = con.prepareStatement("select * from t");
      paramMetaData = ps.getParameterMetaData();
      System.out.println("parameters count for prepared statement is " + paramMetaData.getParameterCount());
      dumpParameterMetaData(paramMetaData);
      ps.execute();

      cs.close();

      System.out.println("test: the scale returned should be the one set by registerOutParameter");
      s.executeUpdate("create procedure dummy_numeric_Proc(out a NUMERIC(30,15), out b NUMERIC(30,15)) language java parameter style java external name 'org.apache.derbyTesting.functionTests.tests.jdbcapi.parameterMetaDataJdbc30.dummy_numeric_Proc'");
      cs = con.prepareCall("CALL dummy_numeric_Proc(?,?)");
      cs.registerOutParameter(1, Types.NUMERIC);
      cs.registerOutParameter(2, Types.NUMERIC,15);
      cs.execute();
      dumpParameterMetaData(cs.getParameterMetaData());

      cs.close();

	   System.out.println("Behaviour of meta data and out params after re-compile");

      cs = con.prepareCall("CALL dummyint(?,?,?,?)");
	  cs.registerOutParameter(3,Types.INTEGER);
      cs.registerOutParameter(4,Types.INTEGER);
      cs.setInt(1,1);
      cs.setInt(2,1);
	  cs.setInt(4,4);
	  dumpParameterMetaData(cs.getParameterMetaData());
	  cs.execute();
      System.out.println("DUMMYINT alias returned " + cs.getInt(4));

      s.executeUpdate("drop procedure dummyint");
      s.executeUpdate("create procedure dummyint(in a integer, in b integer, out c integer, inout d integer) language java external name 'org.apache.derbyTesting.functionTests.tests.jdbcapi.parameterMetaDataJdbc30.dummyint2' parameter style java");
      cs.execute();
      dumpParameterMetaData(cs.getParameterMetaData());
      cs.setInt(4, 6);
      // following is incorrect sequence, should execute first, then get 
      // but leaving it in as an additional negative test. see beetle 5886 
      System.out.println("DUMMYINT alias returned " + cs.getInt(4));
      cs.execute();
      System.out.println("DUMMYINT alias returned " + cs.getInt(4));

      cs.close();

      // temp disable for network server
      if (!isDerbyNet) {
      // Java procedure support
	  System.out.println("ParameterMetaData for Java procedures with INTEGER parameters");
	  s.execute("CREATE PROCEDURE PMDI(IN pmdI_1 INTEGER, IN pmdI_2 INTEGER, INOUT pmdI_3 INTEGER, OUT pmdI_4 INTEGER) language java parameter style java external name 'org.apache.derbyTesting.functionTests.tests.jdbcapi.parameterMetaDataJdbc30.dummyint'");
      cs = con.prepareCall("CALL PMDI(?, ?, ?, ?)");
	  dumpParameterMetaData(cs.getParameterMetaData());
	  cs.close();
	  s.execute("DROP PROCEDURE PMDI");

	  System.out.println("ParameterMetaData for Java procedures with CHAR parameters");
	  s.execute("CREATE PROCEDURE PMDC(IN pmdI_1 CHAR(10), IN pmdI_2 VARCHAR(25), INOUT pmdI_3 CHAR(19), OUT pmdI_4 VARCHAR(32)) language java parameter style java external name 'org.apache.derbyTesting.functionTests.tests.jdbcapi.parameterMetaDataJdbc30.dummyString'");
      cs = con.prepareCall("CALL PMDC(?, ?, ?, ?)");
	  dumpParameterMetaData(cs.getParameterMetaData());
	  cs.close();
	  s.execute("DROP PROCEDURE PMDC");

	  System.out.println("ParameterMetaData for Java procedures with DECIMAL parameters");
	  s.execute("CREATE PROCEDURE PMDD(IN pmdI_1 DECIMAL(5,3), IN pmdI_2 DECIMAL(4,2), INOUT pmdI_3 DECIMAL(9,0), OUT pmdI_4 DECIMAL(10,2)) language java parameter style java external name 'org.apache.derbyTesting.functionTests.tests.jdbcapi.parameterMetaDataJdbc30.dummyDecimal'");
      cs = con.prepareCall("CALL PMDD(?, ?, ?, ?)");
	  dumpParameterMetaData(cs.getParameterMetaData());
	  cs.close();

	  System.out.println("ParameterMetaData for Java procedures with some literal parameters");
      cs = con.prepareCall("CALL PMDD(32.4, ?, ?, ?)");
	  dumpParameterMetaData(cs.getParameterMetaData());
	  cs.close();
      cs = con.prepareCall("CALL PMDD(32.4, 47.9, ?, ?)");
	  dumpParameterMetaData(cs.getParameterMetaData());
	  cs.close();
      cs = con.prepareCall("CALL PMDD(?, 38.2, ?, ?)");
	  dumpParameterMetaData(cs.getParameterMetaData());
	  cs.close();
	  s.execute("DROP PROCEDURE PMDD");
      }
     }
	 catch (SQLException e) {
	 dumpSQLExceptions(e);
	 }
	 catch (Throwable e) {
		System.out.println("FAIL -- unexpected exception:");
		e.printStackTrace(System.out);
	 }
	 System.out.println("Test parameterMetaDataJdbc30 finished");
	}

	static void dumpParameterMetaData(ParameterMetaData paramMetaData) throws SQLException {
		int numParam = paramMetaData.getParameterCount();
		for (int i=1; i<=numParam; i++) {
			try {
			System.out.println("Parameter number : " + i);
			System.out.println("parameter isNullable " + parameterIsNullableInStringForm(paramMetaData.isNullable(i)));
			System.out.println("parameter isSigned " + paramMetaData.isSigned(i));
			System.out.println("parameter getPrecision " + paramMetaData.getPrecision(i));
			System.out.println("parameter getScale " + paramMetaData.getScale(i));
			System.out.println("parameter getParameterType " + paramMetaData.getParameterType(i));
			System.out.println("parameter getParameterTypeName " + paramMetaData.getParameterTypeName(i));
			System.out.println("parameter getParameterClassName " + paramMetaData.getParameterClassName(i));
			System.out.println("parameter getParameterMode " + parameterModeInStringForm(paramMetaData.getParameterMode(i)));
			} catch (Throwable t) {
				System.out.println(t.toString());
				t.printStackTrace(System.out);
			}
		}
	}

	//negative test
	static void dumpParameterMetaDataNegative(ParameterMetaData paramMetaData) throws SQLException {
		int numParam = paramMetaData.getParameterCount();
		try {
			System.out.println("parameter isNullable " + paramMetaData.isNullable(-1));
		} catch (SQLException e) {
			dumpExpectedSQLExceptions(e);
		}
		try {
			System.out.println("parameter isNullable " + paramMetaData.isNullable(0));
		} catch (SQLException e) {
			dumpExpectedSQLExceptions(e);
		}
		try {
			System.out.println("parameter isNullable " + paramMetaData.isNullable(numParam+1));
		} catch (SQLException e) {
			dumpExpectedSQLExceptions(e);
		}
	}

	static private void dumpExpectedSQLExceptions (SQLException se) {
		System.out.println("PASS -- expected exception");
		while (se != null)
		{
			System.out.println("SQLSTATE("+se.getSQLState()+"): "+ "SQL Exception: " + se.getMessage());
			se = se.getNextException();
        }
    }

	//print the parameter mode in human readable form
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

	//print the parameter isNullable value in human readable form
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

	//Set up the test by creating the table used by the rest of the test.
	static void setUpTest(Statement s)
					throws SQLException {
		/* Create a table */
		s.execute("create table t ( "+
				  /* 1 */ "c char(5), "+
				  /* 2 */ "iNoNull int not null, "+
				  /* 3 */ "i int, "+
				  /* 4 */ "de decimal, "+
				  /* 5 */ "d date)");

	}

	//A really simple method to test callable statement
	public static void dummyint (int in_param, int in_param2, int[] in_param3, int[] in_param4)
    								   throws SQLException {

		in_param4[0] = 11111;
	}
	public static void dummyint2 (int in_param, int in_param2, int[] in_param3, int[] in_param4)
    								   throws SQLException {
		in_param4[0] = 22222;
	}
	
	public static void dummy_numeric_Proc (BigDecimal[] max_param,BigDecimal[] min_param)
        							 throws SQLException {
//	System.out.println("dummy_numeric_Proc -- all output parameters"); taking println out because it won't display in master under drda
	}

	public static void dummyString (String in_param, String in_param2, String[] in_param3, String[] in_param4) {
	}
	public static void dummyDecimal(BigDecimal in_param, BigDecimal in_param2, BigDecimal[] in_param3, BigDecimal[] in_param4) {
	}
	
	static private void dumpSQLExceptions (SQLException se) {
		System.out.println("FAIL -- unexpected exception");
		while (se != null) {
			System.out.print("SQLSTATE("+se.getSQLState()+"):");
			se.printStackTrace(System.out);
			se = se.getNextException();
		}
	}
}
