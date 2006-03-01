/*

   Derby - Class org.apache.derbyTesting.functionTests.harness.procedure

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derbyTesting.functionTests.util.TestUtil;
import java.sql.*;


import org.apache.derby.tools.ij;
import org.apache.derby.iapi.reference.JDBC30Translation;
import org.apache.derby.iapi.reference.SQLState;

import java.io.PrintStream;
import java.math.BigInteger;
import java.math.BigDecimal;

import java.lang.reflect.*;

import org.apache.derbyTesting.functionTests.tests.jdbcapi.parameterMetaDataJdbc30;

public class procedure
{

  private static Class[] CONN_PARAM = { Integer.TYPE };
  private static Object[] CONN_ARG = { new Integer(JDBC30Translation.CLOSE_CURSORS_AT_COMMIT)};

	static private boolean isDerbyNet = false;

	public static void main (String[] argv) throws Throwable
	{
   		ij.getPropertyArg(argv); 
        Connection conn = ij.startJBMS();
		cleanUp(conn);
		isDerbyNet = TestUtil.isNetFramework();

		// DB2 !!
		// com.ibm.db2.jcc.DB2DataSource ds = new com.ibm.db2.jcc.DB2DataSource();

		// ds.setDatabaseName("testdb");

		// ds.setServerName("localhost");
		//ds.setPortNumber(1527);
		// ds.setDriverType(4);

		 // Connection conn = ds.getConnection("db2admin", "password");

		//Class.forName("COM.ibm.db2.jdbc.app.DB2Driver").newInstance();
		//Connection conn = DriverManager.getConnection("jdbc:db2:testdb", "USER", "XXXXX");


        runTests( conn);
    }

    public static void runTests( Connection conn) throws Throwable
    {
		try {
			testNegative(conn);
			testDelayedClassChecking(conn);
			testDuplicates(conn);
			ambigiousMethods(conn);
			zeroArgProcedures(conn);
			sqlProcedures(conn);
			dynamicResultSets(conn, ij.startJBMS());

			testParameterTypes(conn);
			testOutparams(conn);

			testSQLControl(conn);
			testLiterals(conn);
            
            multipleRSTests(conn);
                        jira_491_492(conn);
            testImplicitClose(conn);
			cleanUp(conn);
		} catch (SQLException sqle) {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, sqle);
			sqle.printStackTrace(System.out);
		}
		
	}

	public static void testNegative(Connection conn) throws SQLException {

		System.out.println("testNegative");

		Statement s = conn.createStatement();

		// no '.' in path/method
		statementExceptionExpected(s, "create procedure asdf() language java external name 'asdfasdf' parameter style java");

		// trailing '.'
		statementExceptionExpected(s, "create procedure asdf() language java external name 'asdfasdf.' parameter style java");

		// procedure name too long
		statementExceptionExpected(s, "create procedure a23456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789() language java external name 'asdf.asdf' parameter style java");

		// -- missing parens on procedure name
		statementExceptionExpected(s, "create procedure asdf language java external name java.lang.Thread.currentThread parameter style java");

		// -- incorrect language, (almost) straight from DB2 docs 

		statementExceptionExpected(s, "CREATE PROCEDURE ASSEMBLY_PARTS (IN ASSEMBLY_NUM INTEGER, OUT NUM_PARTS INTEGER, OUT COST DOUBLE) EXTERNAL NAME 'parts!assembly' DYNAMIC RESULT SETS 1 LANGUAGE C PARAMETER STYLE GENERAL");

		// invalid schema 
		statementExceptionExpected(s, "create procedure sys.proc1() language java external name 'java.lang.System.gc' parameter style java");

		// repeated elements
		statementExceptionExpected(s, "create procedure noclass() language java external name 'asdf.asdf' parameter style java language java");
		statementExceptionExpected(s, "create procedure noclass() parameter style java language java external name 'asdf.asdf' parameter style java");
		statementExceptionExpected(s, "create procedure noclass() external name 'asdf.xxxx' language java external name 'asdf.asdf' parameter style java");
		statementExceptionExpected(s, "create procedure noclass() parameter style java language java external name 'asdf.asdf' parameter style derby_rs_collection");

		// missing elements
		statementExceptionExpected(s, "create procedure missing01()");
		statementExceptionExpected(s, "create procedure missing02() language java");
		statementExceptionExpected(s, "create procedure missing03() language java parameter style java");
		statementExceptionExpected(s, "create procedure missing04() language java external name 'foo.bar'");
		statementExceptionExpected(s, "create procedure missing05() parameter style java");
		statementExceptionExpected(s, "create procedure missing06() parameter style java external name 'foo.bar'");
		statementExceptionExpected(s, "create procedure missing07() external name 'goo.bar'");
		statementExceptionExpected(s, "create procedure missing08() dynamic result sets 1");
		//statementExceptionExpected(s, "create procedure missing09() specific name fred");
		
		// RETURNS NULL ON NULL INPUT not allowed in procedures.
		statementExceptionExpected(s, "create procedure nullinput2() returns null on null input language java parameter style java external name 'foo.bar'");
		


		// no BLOB/CLOB/ long parameters
		statementExceptionExpected(s, "create procedure NO_BLOB(IN P1 BLOB(3k)) language java parameter style java external name 'no.blob'");
		statementExceptionExpected(s, "create procedure NO_CLOB(IN P1 CLOB(3k)) language java parameter style java external name 'no.clob'");
		statementExceptionExpected(s, "create procedure NO_LVC(IN P1 LONG VARCHAR) language java parameter style java external name 'no.lvc'");

		// duplicate names
		statementExceptionExpected(s, "create procedure DUP_P1(IN FRED INT, OUT RON CHAR(10), IN FRED INT) language java parameter style java external name 'no.dup1'");
		statementExceptionExpected(s, "create procedure D2.DUP_P2(IN \"FreD\" INT, OUT RON CHAR(10), IN \"FreD\" INT) language java parameter style java external name 'no.dup2'");
		statementExceptionExpected(s, "create procedure D3.DUP_P3(IN \"FRED\" INT, OUT RON CHAR(10), IN fred INT) language java parameter style java external name 'no.dup3'");
		s.execute("create procedure DUP_POK(IN \"FreD\" INT, OUT RON CHAR(10), IN fred INT) language java parameter style java external name 'no.dupok'");
		s.execute("drop procedure DUP_POK");

		// procedure not found with explicit schema name
		statementExceptionExpected(s, "CALL APP.NSP(?, ?)");

		// bug 5760 - this caused a null pointer exception at one time.
		statementExceptionExpected(s, "call syscs_util.syscs_set_database_property(\"foo\", \"bar\")");
		
		// Derby-258 specific signatures with types not matching JDBC spec.
		System.out.println("signature mismatched types");
		s.execute("CREATE PROCEDURE SIGNATURE_BUG_DERBY_258_A(IN A INT) LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME 'java.lang.System.load(java.lang.String)'");
		statementExceptionExpected(s, "CALL APP.SIGNATURE_BUG_DERBY_258_A(4)");
		s.execute("DROP PROCEDURE SIGNATURE_BUG_DERBY_258_A");

		// signature with wrong number of arguments, too many
		System.out.println("signature too many parameters");
		s.execute("CREATE FUNCTION SIGNATURE_BUG_DERBY_258_B(A INT) RETURNS VARCHAR(128) LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME 'java.lang.Integer.toString(int, int)'");
		statementExceptionExpected(s, "VALUES APP.SIGNATURE_BUG_DERBY_258_B(4)");
		s.execute("DROP FUNCTION SIGNATURE_BUG_DERBY_258_B");

		// and too few
		System.out.println("signature too few parameters");
		s.execute("CREATE PROCEDURE SIGNATURE_BUG_DERBY_258_C(IN A INT) LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME 'java.lang.System.gc()'");
		statementExceptionExpected(s, "CALL APP.SIGNATURE_BUG_DERBY_258_C(4)");
		s.execute("DROP PROCEDURE SIGNATURE_BUG_DERBY_258_C");

		// only a leading paren
		System.out.println("signature invalid format");
		s.execute("CREATE PROCEDURE SIGNATURE_BUG_DERBY_258_F(IN A INT) LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME 'java.lang.System.gc('");
		statementExceptionExpected(s, "CALL APP.SIGNATURE_BUG_DERBY_258_F(4)");
		s.execute("DROP PROCEDURE SIGNATURE_BUG_DERBY_258_F");

		// signature of (,,)
		System.out.println("signature invalid format");
		s.execute("CREATE PROCEDURE SIGNATURE_BUG_DERBY_258_G(IN A INT) LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME 'java.lang.System.gc(,,)'");
		statementExceptionExpected(s, "CALL APP.SIGNATURE_BUG_DERBY_258_G(4)");
		s.execute("DROP PROCEDURE SIGNATURE_BUG_DERBY_258_G");

		// signature of (, ,)
		System.out.println("signature invalid format");
		s.execute("CREATE PROCEDURE SIGNATURE_BUG_DERBY_258_H(IN A INT) LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME 'java.lang.System.gc(, ,)'");
		statementExceptionExpected(s, "CALL APP.SIGNATURE_BUG_DERBY_258_H(4)");
		s.execute("DROP PROCEDURE SIGNATURE_BUG_DERBY_258_H");

		// signature of (int,)
		System.out.println("signature invalid format");
		s.execute("CREATE PROCEDURE SIGNATURE_BUG_DERBY_258_I(IN A INT) LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME 'java.lang.System.gc(int ,)'");
		statementExceptionExpected(s, "CALL APP.SIGNATURE_BUG_DERBY_258_I(4)");
		s.execute("DROP PROCEDURE SIGNATURE_BUG_DERBY_258_I");
		
		s.close();
		
	}
	
   
	public static void testBug5280(Connection conn) throws SQLException
	{
		String csString = "CALL SQLCONTROL3_0 (?, ?, ?, ?, ?, ?, ?)";
		// Bug 5280 If we don't register the outparams
		// we don't get an error with network server.
		//for (int p = 1; p <= 7; p++) {
		//	cs.registerOutParameter(p,Types.VARCHAR);
		//}
		callExceptionExpected(conn, csString);
	}

	public static void testDelayedClassChecking(Connection conn) throws SQLException {

		System.out.println("testDelayedClassChecking");


		Statement s = conn.createStatement();
		// -- procedures do not check if the class or method exists at create time.
		s.execute("create procedure noclass() language java external name 'asdf.asdf' parameter style java");
		s.execute("create procedure nomethod() language java external name 'java.lang.Integer.asdf' parameter style java");
		s.execute("create procedure notstatic() language java external name 'java.lang.Integer.equals' parameter style java");
		s.execute("create procedure notvoid() language java external name 'java.lang.Runtime.getRuntime' parameter style java");

		//  - but they are checked at runtime
		callExceptionExpected(conn, "call noclass()");
		callExceptionExpected(conn, "call nomethod()");
		callExceptionExpected(conn, "call notstatic()");
		callExceptionExpected(conn, "call notvoid()");

		// CHECK SYSALIAS
		s.execute("drop procedure noclass");
		s.execute("drop procedure nomethod");
		s.execute("drop procedure notstatic");
		s.execute("drop procedure notvoid");

		s.close();

	}

	public static void testDuplicates(Connection conn) throws SQLException {
		System.out.println("testDuplicates");


		Statement s = conn.createStatement();

		s.execute("create schema S1");
		s.execute("create schema S2");

		s.execute("create procedure PROCDUP() language java external name 'okAPP.ok0' parameter style java");
		s.execute("create procedure s1.PROCDUP() language java external name 'oks1.ok0' parameter style java");
		s.execute("create procedure s2.PROCDUP() language java external name 'oks2.ok0' parameter style java");

		statementExceptionExpected(s, "create procedure PROCDUP() language java external name 'failAPP.fail0' parameter style java");
		statementExceptionExpected(s, "create procedure s1.PROCDUP() language java external name 'fails1.fail0' parameter style java");
		statementExceptionExpected(s, "create procedure s2.PROCDUP() language java external name 'fails2.fail0' parameter style java");

		showMatchingProcedures(conn, "PROCDUP");

		statementExceptionExpected(s, "create procedure S1.NOTYET() SPECIFIC fred language java external name 'failAPP.fail0' parameter style java");
		
		s.execute("drop procedure s1.PROCDUP");
		s.execute("drop procedure s2.PROCDUP");

		s.execute("drop schema S1 RESTRICT");
		s.execute("drop schema S2 RESTRICT");
		s.close();


	}

	public static void ambigiousMethods(Connection conn) throws SQLException {
		System.out.println("ambigiousMethods");

		Statement s = conn.createStatement();

		// ambigious resolution - with result sets
		s.execute("create procedure ambigious01(p1 INTEGER, p2 CHAR(20)) dynamic result sets 1 language java parameter style java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.ambigious1'");
		callExceptionExpected(conn, "call AMBIGIOUS01(?, ?)");
		s.execute("drop procedure AMBIGIOUS01");

		// ambigious in defined parameters
		s.execute("create procedure ambigious02(p1 INTEGER, p2 INTEGER) dynamic result sets 1 language java parameter style java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.ambigious2'");
		callExceptionExpected(conn, "call AMBIGIOUS02(?, ?)");
		s.execute("drop procedure AMBIGIOUS02");

        // verify we can find it with a Java signature
        s.execute("create procedure ambigious03(p1 INTEGER, p2 INTEGER) dynamic result sets 1 language java parameter style java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.ambigious2(int,java.lang.Integer)'");
        executeProcedure(s, "{call ambigious03(1, NULL)}");
        s.execute("drop procedure AMBIGIOUS03");
        s.execute("create procedure ambigious04(p1 INTEGER, p2 INTEGER) dynamic result sets 1 language java parameter style java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.ambigious2(java.lang.Integer,int)'");
        executeProcedure(s, "{call ambigious04(NULL, 1)}");
        s.execute("drop procedure AMBIGIOUS04");
        s.close();
	}

	public static void zeroArgProcedures(Connection conn) throws SQLException {
		System.out.println("zeroArgProcedures");

		Statement s = conn.createStatement();
		s.execute("create procedure za() language java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.zeroArg' parameter style java");

		executeProcedure(s, "call za()");
		PreparedStatement ps = conn.prepareStatement("call za()");
		executeProcedure(ps);
		ps.close();

		ps = conn.prepareStatement("{call za()}");
		executeProcedure(ps);
		ps.close();


		try {
			ps = conn.prepareStatement("call za(?)");
			System.out.println("FAIL - prepareStatement call za(?)");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED SQL Exception: " + sqle.getMessage());
		}

		CallableStatement cs = conn.prepareCall("call za()");
		executeProcedure(cs);
		cs.close();

		cs = conn.prepareCall("{call za()}");
		executeProcedure(cs);
		cs.close();

		showMatchingProcedures(conn, "ZA");
		s.execute("drop procedure za");
		showMatchingProcedures(conn, "ZA");

		s.close();

	}

	private static void sqlProcedures(Connection conn) throws SQLException {

		System.out.println("sqlProcedures()");

		Statement s = conn.createStatement();

		s.execute("create table t1(i int not null primary key, b char(15))");
		s.execute("create procedure ir(p1 int) MODIFIES SQL DATA dynamic result sets 0 language java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.insertRow' parameter style java");
		s.execute("create procedure ir2(p1 int, p2 char(10)) language java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.insertRow' MODIFIES SQL DATA parameter style java");

		showMatchingProcedures(conn, "IR%");

		callExceptionExpected(conn, "CALL IR()");

		CallableStatement ir1 = conn.prepareCall("CALL IR(?)");

		ir1.setInt(1, 1);
		executeProcedure(ir1);

		ir1.setInt(1,2);
		executeProcedure(ir1);
		try {
			ir1.execute();
			System.out.println("FAIL - duplicate key insertion through ir");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED SQL Exception: " + sqle.getMessage());
		}

		ir1.setString(1, "3");
		executeProcedure(ir1);

		ir1.close();

		ir1 = conn.prepareCall("CALL APP.IR(?)");
		ir1.setInt(1, 7);
		executeProcedure(ir1);

		CallableStatement ir2 = conn.prepareCall("CALL IR2(?, ?)");

		ir2.setInt(1, 4);
		ir2.setInt(2, 4);
		executeProcedure(ir2);

		ir2.setInt(1, 5);
		ir2.setString(2, "ir2");
		executeProcedure(ir2);


		ir2.setInt(1, 6);
		ir2.setString(2, "'012345678990'");
		executeProcedure(ir2);

		ir1.close();
		ir2.close();

		if (!conn.getAutoCommit())
			conn.commit();



		ResultSet rs = s.executeQuery("select * from t1");
		org.apache.derby.tools.JDBCDisplayUtil.DisplayResults(System.out, rs, conn);

		if (!conn.getAutoCommit())
			conn.commit();

		callExceptionExpected(conn, "CALL IR2(2, 'no way')");
		callExceptionExpected(conn, "CALL IR2(?, 'no way')");
		callExceptionExpected(conn, "CALL IR2(2, ?)");

		s.execute("drop procedure IR");
		s.execute("drop procedure IR2");

		s.close();
	}
    // This test case provides tests for bugs DERBY-491 and DERBY-492. These
    // two bug reports describe different symptoms, but the underlying bug
    // is identical: the network server's implementation of LMTBLKPRC was
    // incorrectly manipulating DDMWriter's bytes buffer. Depending on the
    // details, the symptom of this bug was generally a hang, because the
    // server mistakenly truncated the unsent data in its network buffer and
    // hence sent only a partial transmission, causing the client to hang,
    // waiting for data that would never arrive. A more detailed analysis
    // of some other possible symptoms that could arise from these tests is
    // available in the bug notes for bug 491 in JIRA at:
    // http://issues.apache.org/jira/browse/DERBY-491
    //
    private static void jira_491_492(Connection conn)
        throws SQLException
    {
        Statement st = conn.createStatement();
        PreparedStatement pSt = null;

        // JIRA-491: Result set has a row that is approx 32K long.
        // When originally filed, this bug script caused  a protocol
        // exception and connection deallocation, but that was because the
        // bug script provoked both JIRA-614 *and* JIRA-491. If you have
        // the fix for JIRA-614, but JIRA-491 has regressed, you will hang.

        try {
            st.execute("drop table testtable1");
        } catch (SQLException se) {}

        // Create an array of chars to be used as the input parameter.
        // Note that the array should roughly 32K or larger.
        char [] cData = new char[32500];
        for (int i = 0; i < cData.length; i++)
            cData[i] = Character.forDigit(i%10, 10);

        try {
            st.execute("create table jira491 (int1 integer, varchar32k varchar(32500))");
            pSt=conn.prepareStatement("insert into jira491 values (?,?)");
            for (int i = 1; i <= 5; i++) {
                pSt.setInt(1, i);
                pSt.setString(2, new String(cData));
                pSt.execute();
            }
        } catch (SQLException se) {
            System.out.println("JIRA-491: FAILURE in data generation:");
            se.printStackTrace(System.out);
        }

        try {
            st.execute("drop procedure TEST_PROC_JIRA_491");
        } catch (SQLException se) {} // Ignore "proc does not exist" errors
	
        try {
            st.execute("create procedure TEST_PROC_JIRA_491(in i int) " +
						"language java parameter style java external name " +
						"'org.apache.derbyTesting.functionTests.util.ProcedureTest.BIG_COL_491' result sets 2");
        } catch (SQLException se) {
            System.out.println("JIRA-491: FAILURE in procedure creation:");
            se.printStackTrace(System.out);
        }

        // Call setupStatementReuse which will make the server to reuse an existing statement. 
        setupStatementReuse(conn);
        CallableStatement cSt = conn.prepareCall("call TEST_PROC_JIRA_491(?)");
        cSt.setInt(1, 3);
        try {
            cSt.execute();
            do {
                ResultSet rs = cSt.getResultSet();
                while (rs.next()) {
                    String s = rs.getString(2);
                }
            } while (cSt.getMoreResults());
            System.out.println("JIRA-491 Successful.");
        }
        catch (Exception e)
        {
            System.out.println("JIRA-491 FAILURE: Caught Exception:");
            e.printStackTrace(System.out);
        }
	
        // JIRA-492: Result set has hundreds of columns.
        // This test case, when originally filed, exposed several problems:
        // - first, this test case causes the server to respond with a very
        // long response message which gets handled using DRDA Layer B DSS
        // segmentation. This long message was corrupted due to bug DERBY-125.
        // - then, the test case causes the server to perform LMTBLKPRC
        // message truncation in a situation in which there are multiple
        // chained messages in the DDMWriter buffer. Due to bug DERBY-491/2,
        // the message truncation logic truncated not only the last DSS block,
        // but also the multi-segment long message which was still sitting
        // unsent in the buffer.This then caused a HANG in the client, which
        // waited forever for the never-to-be-sent truncated data.

        try {
            st.execute("drop table jira492");
        } catch (SQLException se) {}

        try {
            st.execute("create table jira492 (id integer, nsi smallint, " +
                "ni integer, nbi DECIMAL(19,0), nd decimal(7,2), nr real, " +
                "ndo double)");
            st.execute("insert into jira492 values (" +
						"1, 2, 3, 4.5, 6.7, 8.9, 10.11)");
        } catch (SQLException se) {
            System.out.println("JIRA-492: FAILURE in data setup:");
            se.printStackTrace(System.out);
        }
	
        try {
            st.execute("drop procedure TEST_PROC_JIRA_492");
        } catch (SQLException se) {}
	
        try {
            st.execute("create procedure TEST_PROC_JIRA_492() " +
                    "language java parameter style java external name " +
                    "'org.apache.derbyTesting.functionTests.util.ProcedureTest.LOTS_O_COLS_492' result sets 1");
        } catch (SQLException se) {
            System.out.println("JIRA-492: FAILURE in procedure creation:");
            se.printStackTrace(System.out);
        }
	
        cSt = conn.prepareCall("call TEST_PROC_JIRA_492()");
        cSt.execute();
        System.out.println("JIRA-492 successful -- no hang!");
    }

	private static void executeProcedure(Statement s, String sql) throws SQLException {
		boolean firstResultIsAResultSet = s.execute(sql);

		procedureResults(s, firstResultIsAResultSet);
	}


	private static void executeProcedure(PreparedStatement ps) throws SQLException {
		boolean firstResultIsAResultSet = ps.execute();

		procedureResults(ps, firstResultIsAResultSet);
	}


	private static void procedureResults(Statement ps, boolean firstResultIsAResultSet) throws SQLException {

		org.apache.derby.tools.JDBCDisplayUtil.ShowWarnings(System.out, ps);

		boolean sawOneResult = false;
		boolean isFirst = true;
		do {

			boolean gotResult = false;

			ResultSet rs = ps.getResultSet();
			int updateCount = ps.getUpdateCount();
			if (rs == null) {

				if (isFirst && firstResultIsAResultSet) {
					System.out.println("FAIL - execute() indicated first result was a result set but getResultSet() returned null");
				}

				if (updateCount != -1) {
					gotResult = true;
					sawOneResult = true;
					System.out.println("UPDATE COUNT " + updateCount);
				}
			}
			else {

				if (updateCount != -1)
					System.out.println("FAIL - HAVE RESULT SET AND UPDATE COUNT OF " + updateCount);
				org.apache.derby.tools.JDBCDisplayUtil.DisplayResults(System.out, rs, ps.getConnection());
				gotResult = true;
				sawOneResult = true;
			}

			// if we did not get a result and this is not the first result then
			// there is a bug since the getMoreResults() returned true.
			//
			// This may also be an error on the first pass but maybe it's
			// ok to have no results at all?
			if (!gotResult && !isFirst) {
				System.out.println("FAIL - getMoreResults indicated more results but none was found");
			}

			isFirst = false;

		} while (ps.getMoreResults());
		SQLWarning warnings = ps.getWarnings();
		if (warnings != null)
			System.out.println("SQLWarning :" + warnings.getMessage());

		if (!sawOneResult)
			System.out.println("No ResultSet or update count returned");
	}

	/**
		1. basic testing
		2. correct auto commit logic
		3. correct holdability (JDBC 3)
	*/
	private static void dynamicResultSets(Connection conn, Connection conn2) throws SQLException {

		System.out.println("dynamicResultSets - parameter style JAVA");

		Statement s = conn.createStatement();

		statementExceptionExpected(s, "create procedure DRS(p1 int) parameter style JAVA READS SQL DATA dynamic result sets -1 language java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.selectRows'");

		s.execute("create procedure DRS(p1 int) parameter style JAVA READS SQL DATA dynamic result sets 1 language java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.selectRows'");

		showMatchingProcedures(conn, "DRS");

		callExceptionExpected(conn, "CALL DRS()");
		callExceptionExpected(conn, "CALL DRS(?,?)");

		CallableStatement drs1 = conn.prepareCall("CALL DRS(?)");

		drs1.setInt(1, 3);
		executeProcedure(drs1);
		drs1.close();

		s.execute("create procedure DRS2(p1 int, p2 int) parameter style JAVA READS SQL DATA dynamic result sets 2 language java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.selectRows'");
		showMatchingProcedures(conn, "DRS2");

		drs1 = conn.prepareCall("CALL DRS2(?, ?)");
		drs1.setInt(1, 2);
		drs1.setInt(2, 6);
		executeProcedure(drs1);
			
		// execute it returning one closed result set
		drs1.setInt(1, 2);
		drs1.setInt(2, 99); // will close the second result set
		executeProcedure(drs1);

		// execute it returning no result sets
		if (! isDerbyNet)
		{
			//RESOLVE there appears to be a JCC Bug when returning no 
			// resultSets.
			drs1.setInt(1, 2);
			drs1.setInt(2, 199); // return no results at all
			executeProcedure(drs1);
		}
		// execute it returning two result sets but with the order swapped in the parameters
		// doesnot affect display order.
		drs1.setInt(1, 2);
		drs1.setInt(2, 299); // swap results
		executeProcedure(drs1);
		
		if (!isDerbyNet)
		{
		// execute it returning two result sets, and check to see the result set is closed after getMoreResults.
		drs1.setInt(1, 2);
		drs1.setInt(2, 2);
		drs1.execute();
		ResultSet lastResultSet = null;
		int pass = 1;
		do {

			if (lastResultSet != null) {
				try {
					lastResultSet.next();
					System.out.println("FAILED - result set should be closed");
				} catch (SQLException sqle) {
					System.out.println("EXPECTED : " + sqle.getMessage());
				}
			}

			lastResultSet = drs1.getResultSet();
			System.out.println("pass " + (pass++) + " got result set " + (lastResultSet != null));

		} while (drs1.getMoreResults() || lastResultSet != null);

		checkCommitWithMultipleResultSets(drs1, conn2, "autocommit");
		checkCommitWithMultipleResultSets(drs1, conn2, "noautocommit");
		checkCommitWithMultipleResultSets(drs1, conn2, "statement");
		}




		drs1.close();

		// use escape syntax
		drs1 = conn.prepareCall("{call DRS2(?, ?)}");
		drs1.setInt(1, 2);
		drs1.setInt(2, 6);
		executeProcedure(drs1);
		drs1.close();


		// check that a procedure with dynamic result sets can not resolve to a method with no ResultSet argument.
		s.execute("create procedure irdrs(p1 int) dynamic result sets 1 language java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.missingDynamicParameter' parameter style JAVA");
		callExceptionExpected(conn, "CALL IRDRS(?)");
		s.execute("drop procedure irdrs");

		// check that a procedure with dynamic result sets can not resolve to a method with an argument that is a ResultSet impl,
		s.execute("create procedure rsi(p1 int) dynamic result sets 1 language java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.badDynamicParameter' parameter style JAVA");
		callExceptionExpected(conn, "CALL rsi(?)");
		s.execute("drop procedure rsi");

		// simple check for a no-arg method that has dynamic result sets but does not return any
		System.out.println("no dynamic result sets");
		s.execute("create procedure zadrs() dynamic result sets 4 language java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.zeroArgDynamicResult' parameter style  JAVA");
		CallableStatement zadrs = conn.prepareCall("CALL ZADRS()");
		executeProcedure(zadrs);
		zadrs.close();
		s.execute("drop procedure ZADRS");

		// return too many result sets
		System.out.println("Testing too many result sets");
		s.execute("create procedure way.toomany(p1 int, p2 int) READS SQL DATA dynamic result sets 1 language java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.selectRows' parameter style  JAVA");
		CallableStatement toomany = conn.prepareCall("CALL way.toomany(?, ?)");
		toomany.setInt(1, 2);
		toomany.setInt(2, 6);
		System.out.println("... too many result sets");
		executeProcedure(toomany);

		System.out.println("... one additional closed result set");
		toomany.setInt(1, 2);
		toomany.setInt(2, 99); // will close the second result set.
		executeProcedure(toomany);

		toomany.close();
		s.execute("drop procedure way.toomany");

		testResultSetsWithLobs(conn);

		s.close();
		conn2.close();
	}


	private static void checkCommitWithMultipleResultSets(CallableStatement drs1, Connection conn2, String action) throws SQLException
	{
		Connection conn = drs1.getConnection();
    //Use reflection to set the holdability to false so that the test can run in jdk14 and lower jdks as well
    try {
				Method sh = conn.getClass().getMethod("setHoldability", CONN_PARAM);
				sh.invoke(conn, CONN_ARG);
    } catch (Exception e) {System.out.println("shouldn't get that error " + e.getMessage());}//for jdks prior to jdk14

		// check to see that the commit of the transaction happens at the correct time.
		// switch isolation levels to keep the locks around.
		int oldIsolation = conn.getTransactionIsolation();
		boolean oldAutoCommit = conn.getAutoCommit();
         
		if (action.equals("noautocommit"))
			conn.setAutoCommit(false);
		else
			conn.setAutoCommit(true);

		conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
		System.out.println("auto commit is " + conn.getAutoCommit());
		PreparedStatement psLocks = conn2.prepareStatement("select count(*) from SYSCS_DIAG.LOCK_TABLE AS LT");

		showLocks(psLocks, "lock count before execution ");

		drs1.execute();

		showLocks(psLocks, "lock count after execution ");

		ResultSet rs = drs1.getResultSet();
		rs.next();
		showLocks(psLocks, "lock count after next on first rs ");

		boolean expectClosed = false;

		// execute another statement to ensure that the result sets close.
		if (action.equals("statement")) {
			System.out.println("executing statement to force auto commit on open CALL statement");

			conn.createStatement().executeQuery("values 1").next();
			expectClosed = true;
			showLocks(psLocks, "lock count after statement execution ");

			try {
				rs.next();
				System.out.println("FAIL - result set open in auto commit mode after another statement execution");
			} catch (SQLException sqle) {
				System.out.println("Expected - " + sqle.getMessage());
			}
		}



		boolean anyMore = drs1.getMoreResults();
		System.out.println("Is there a second result ? " + anyMore);
		showLocks(psLocks, "lock count after first getMoreResults() ");

		if (anyMore) {
		
			rs = drs1.getResultSet();
			try {
				rs.next();
				if (expectClosed)
					System.out.println("FAIL - result set open in auto commit mode after another statement execution");
			} catch (SQLException sqle) {
				if (expectClosed)
					System.out.println("Expected - " + sqle.getMessage());
				else
					throw sqle;
			}
			showLocks(psLocks, "lock count after next on second rs ");

			// should commit here since all results are closed
			boolean more = drs1.getMoreResults();
			System.out.println("more results (should be false) " + more);
			showLocks(psLocks, "lock count after second getMoreResults() ");

			conn.setTransactionIsolation(oldIsolation);
			conn.setAutoCommit(oldAutoCommit);
		}

		psLocks.close();
	}

	private static void showLocks(PreparedStatement psLocks, String where) throws SQLException {
		ResultSet locks = psLocks.executeQuery();
		locks.next();
		System.out.println(where + locks.getInt(1));
		locks.close();
	}

	private static void testParameterTypes(Connection conn) throws SQLException  {
		System.out.println("parameterTypes");
		Statement s = conn.createStatement();

		s.execute("create table PT1(A INTEGER not null primary key, B CHAR(10), C VARCHAR(20))"); 
		s.execute("create procedure PT1(IN a int, IN b char(10), c varchar(20)) parameter style java dynamic result sets 1 language java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.parameter1' MODIFIES SQL DATA");
		showMatchingProcedures(conn, "PT1");

		CallableStatement pt1 = conn.prepareCall("CALL PT1(?, ?, ?)");

		pt1.setInt(1, 20);
		pt1.setString(2, "abc");
		pt1.setString(3, "efgh");
		executeProcedure(pt1);


		pt1.setInt(1, 30);
		pt1.setString(2, "abc   ");
		pt1.setString(3, "efgh  ");
		executeProcedure(pt1);

		pt1.setInt(1, 40);
		pt1.setString(2, "abc                                                                           ");
		pt1.setString(3, "efgh                                                                             ");
		executeProcedure(pt1);

		pt1.setInt(1, 50);
		pt1.setString(2, "0123456789X");
		pt1.setString(3, "efgh  ");
		executeProcedure(pt1);
		pt1.close();

		s.execute("DROP procedure PT1");

		s.execute("create procedure PT2(IN a int, IN b DECIMAL(4), c DECIMAL(7,3)) parameter style java dynamic result sets 1 language java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.parameter2' MODIFIES SQL DATA");
		showMatchingProcedures(conn, "PT2");

		CallableStatement pt2 = conn.prepareCall("CALL PT2(?, ?, ?)");

		pt2.setInt(1, 60);
		pt2.setString(2, "34");
		pt2.setString(3, "54.1");
		executeProcedure(pt2);

		pt2.setInt(1, 70);
		pt2.setBigDecimal(2, new BigDecimal("831"));
		pt2.setBigDecimal(3, new BigDecimal("45.7"));
		executeProcedure(pt2);
		
		pt2.setInt(1, -1);
		pt2.setBigDecimal(2, new BigDecimal("10243"));
		pt2.setBigDecimal(3, null);
		try {
			executeProcedure(pt2);
			System.out.println("FAIL - too many digits in decimal value accepted");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED SQL Exception: " + sqle.getMessage());
		}
		pt2.setInt(1, 80);
		pt2.setBigDecimal(2, new BigDecimal("993"));
		pt2.setBigDecimal(3, new BigDecimal("1234.5678"));
		executeProcedure(pt2);
		pt2.close();

		s.execute("DROP procedure PT2");
/*		
		s.execute("create procedure PTBOOL2(IN p_in BOOLEAN, INOUT p_inout BOOLEAN, OUT p_out BOOLEAN) parameter style java dynamic result sets 0 language java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.pBOOLEAN' NO SQL");
		showMatchingProcedures(conn, "PTBOOL%");

		{

		CallableStatement ptb = conn.prepareCall("CALL PTBOOL2(?, ?, ?)");
		ptb.registerOutParameter(2, Types.BIT); 
		ptb.registerOutParameter(3, Types.BIT);

		if (!isDerbyNet){ // bug 5437
		ptb.setObject(1, null);
		ptb.setObject(2, Boolean.FALSE);
		try {
			ptb.execute();
			System.out.println("FAIL NULL PASSED to  primitive");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED SQL Exception: " + sqle.getMessage());
		}
		}

		ptb.setBoolean(1, true);
		ptb.setBoolean(2, false);
		ptb.execute();
		System.out.println("p_inout " + ptb.getObject(2) + " p_out " + ptb.getObject(3));
		ptb.setBoolean(2, false);
		ptb.execute();
		System.out.println("p_inout " + ptb.getBoolean(2) + " null?" + ptb.wasNull() + " p_out " + ptb.getBoolean(3) + " null?" + ptb.wasNull());
		ptb.close();
		}

		s.execute("DROP procedure PTBOOL2");

		s.execute("create procedure PTTINYINT2(IN p_in TINYINT, INOUT p_inout TINYINT, OUT p_out TINYINT) parameter style java dynamic result sets 0 language java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.pTINYINT' NO SQL");
		showMatchingProcedures(conn, "PTTINYINT%");


		CallableStatement ptti = conn.prepareCall("CALL PTTINYINT2(?, ?, ?)");
		ptti.registerOutParameter(2, Types.TINYINT); 
		ptti.registerOutParameter(3, Types.TINYINT);

		ptti.setNull(1, Types.TINYINT);
		ptti.setByte(2, (byte) 7);
		try {
			ptti.execute();
			System.out.println("FAIL NULL PASSED to  primitive");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED SQL Exception: " + sqle.getMessage());
		}

		ptti.setByte(1, (byte) 4);
		ptti.setNull(2, Types.TINYINT);
		try {
			ptti.execute();
			System.out.println("FAIL NULL PASSED to  primitive");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED SQL Exception: " + sqle.getMessage());
		}

		ptti.setByte(1, (byte) 6);
		ptti.setByte(2, (byte) 3);
		ptti.execute();
		System.out.println("p_inout " + ptti.getObject(2) + " p_out " + ptti.getObject(3));
		ptti.setByte(2, (byte) 3);
		ptti.execute();
		System.out.println("p_inout " + ptti.getByte(2) + " null?" + ptti.wasNull() + " p_out " + ptti.getByte(3) + " null?" + ptti.wasNull());
		ptti.close();


		s.execute("DROP procedure PTTINYINT2");

	*/	
		s.execute("create procedure PTSMALLINT2(IN p_in SMALLINT, INOUT p_inout SMALLINT, OUT p_out SMALLINT) parameter style java dynamic result sets 0 language java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.pSMALLINT' NO SQL");
		showMatchingProcedures(conn, "PTSMALLINT%");


		CallableStatement ptsi = conn.prepareCall("CALL PTSMALLINT2(?, ?, ?)");
		ptsi.registerOutParameter(2, Types.SMALLINT); 
		ptsi.registerOutParameter(3, Types.SMALLINT);

		ptsi.setNull(1, Types.SMALLINT);
		ptsi.setShort(2, (short) 7);
		try {
			ptsi.execute();
			System.out.println("FAIL NULL PASSED to  primitive");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED SQL Exception: (" + sqle.getSQLState() + ") " + sqle.getMessage());
		}

		ptsi.setShort(1, (short) 4);
		ptsi.setNull(2, Types.SMALLINT);
		try {
			ptsi.execute();
			System.out.println("FAIL NULL PASSED to  primitive");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED SQL Exception: (" + sqle.getSQLState() + ") " + sqle.getMessage());
		}

		ptsi.setShort(1, (short) 6);
		ptsi.setShort(2, (short) 3);
		ptsi.execute();
		System.out.println("p_inout " + ptsi.getObject(2) + " p_out " + ptsi.getObject(3));
		ptsi.setShort(2, (short) 3);
		ptsi.execute();
		System.out.println("p_inout " + ptsi.getByte(2) + " null?" + ptsi.wasNull() + " p_out " + ptsi.getByte(3) + " null?" + ptsi.wasNull());

		// with setObject . Beetle 5439
		ptsi.setObject(1, new Integer(6));
		ptsi.setObject(2, new Integer(3));
		
		ptsi.execute();
		System.out.println("p_inout " + ptsi.getByte(2) + " null?" + ptsi.wasNull() + " p_out " + ptsi.getByte(3) + " null?" + ptsi.wasNull());
		ptsi.close();
		
		s.execute("DROP procedure PTSMALLINT2");		
		s.execute("DROP TABLE PT1");

		s.close();

	}

	private static void testOutparams(Connection conn) throws SQLException  {


		System.out.println("outparams");

		Statement s = conn.createStatement();

		s.execute("create procedure OP1(OUT a int, IN b int) parameter style java language java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.outparams1'");
		showMatchingProcedures(conn, "OP1");

		
		// check execute via a Statement fails for use of OUT parameter
		if (! isDerbyNet) { // bug 5263
		try {
			executeProcedure(s, "CALL OP1(?, ?)");
			System.out.println("FAIL execute succeeded on OUT param with Statement");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED SQL Exception: " + sqle.getMessage());
		}
		}

		if (! isDerbyNet) { // bug 5276
		// check execute via a PreparedStatement fails for use of OUT parameter
		try {
			PreparedStatement ps = conn.prepareStatement("CALL OP1(?, ?)");
			System.out.println("FAIL prepare succeeded on OUT param with PreparedStatement");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED SQL Exception: " + sqle.getMessage());
		}
		}

		CallableStatement op = conn.prepareCall("CALL OP1(?, ?)");

		op.registerOutParameter(1, Types.INTEGER);
		op.setInt(2, 7);

		executeProcedure(op);

		System.out.println("OP1 " + op.getInt(1) + " null ? " + op.wasNull());

		op.close();

		s.execute("create procedure OP2(INOUT a int, IN b int) parameter style java language java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.inoutparams2'");
		showMatchingProcedures(conn, "OP2");

		// check execute via a Statement fails for use of INOUT parameter
		if (!isDerbyNet) { // bug 5263
		try {
			executeProcedure(s, "CALL OP2(?, ?)");
			System.out.println("FAIL execute succeeded on INOUT param with Statement");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED SQL Exception: " + sqle.getMessage());
		}
		}

		if (! isDerbyNet) { // bug 5276

		// check execute via a PreparedStatement fails for use of INOUT parameter
		try {
			PreparedStatement ps = conn.prepareStatement("CALL OP2(?, ?)");
			System.out.println("FAIL prepare succeeded on INOUT param with PreparedStatement");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED SQL Exception: " + sqle.getMessage());
		}
		}

		op = conn.prepareCall("CALL OP2(?, ?)");

		op.registerOutParameter(1, Types.INTEGER);
		op.setInt(1, 3);
		op.setInt(2, 7);

		executeProcedure(op);
		System.out.println("OP2 " + op.getInt(1) + " null ? " + op.wasNull());
		op.close();

		// INOUT & OUT procedures with variable length
		s.execute("create procedure OP3(INOUT a CHAR(10), IN b int) parameter style java language java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.inoutparams3'");
		showMatchingProcedures(conn, "OP3");

		op = conn.prepareCall("CALL OP3(?, ?)");

		op.registerOutParameter(1, Types.CHAR);
		op.setString(1, "dan");
		op.setInt(2, 8);

		executeProcedure(op);
		System.out.println("OP3 >" + op.getString(1) + "< null ? " + op.wasNull());
		op.close();

		// INOUT & OUT DECIMAL procedures with variable length
		s.execute("create procedure OP4(OUT a DECIMAL(4,2), IN b VARCHAR(255)) parameter style java language java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.inoutparams4'");
		showMatchingProcedures(conn, "OP4");

		op = conn.prepareCall("CALL OP4(?, ?)");

		op.registerOutParameter(1, Types.DECIMAL);
		op.setString(2, null);
		executeProcedure(op);
		System.out.println("OP4 null >" + op.getBigDecimal(1) + "< null ? " + op.wasNull());

		op.setString(2, "14");
		executeProcedure(op);
		System.out.println("OP4 14 >" + op.getBigDecimal(1) + "< null ? " + op.wasNull());

		op.setString(2, "11.3");
		executeProcedure(op);
		System.out.println("OP4 11.3 >" + op.getBigDecimal(1) + "< null ? " + op.wasNull());

		op.setString(2, "39.345");
		executeProcedure(op);
		System.out.println("OP4 39.345 >" + op.getBigDecimal(1) + "< null ? " + op.wasNull());

		op.setString(2, "83");
		try {
			executeProcedure(op);
			System.out.println("FAIL - execution ok on out of range out parameter");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED SQL Exception: " + sqle.getMessage());
		}

		if (!isDerbyNet) {
		// Bug 5316 - JCC clears registration with  clearParameters()
		op.clearParameters();
		try {
			// b not set
			executeProcedure(op);
			System.out.println("FAIL - b not set");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED SQL Exception: " + sqle.getMessage());
		}

		// try to set an OUT param
		try {
			op.setBigDecimal(1, new BigDecimal("22.32"));
			System.out.println("FAIL - set OUT param to value");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED SQL Exception: " + sqle.getMessage());
		}

		try {
			op.setBigDecimal(1, null);
			System.out.println("FAIL - set OUT param to null value");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED SQL Exception: " + sqle.getMessage());
		}
		try {
			op.setNull(1, Types.DECIMAL);
			System.out.println("FAIL - set OUT param to null");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED SQL Exception: " + sqle.getMessage());
		}
		}

		// can we get an IN param?
		op.setString(2, "49.345");
		executeProcedure(op);
		System.out.println("OP4 49.345 >" + op.getBigDecimal(1) + "< null ? " + op.wasNull());
		try {
			System.out.println("FAIL OP4 GET 49.345 >" + op.getString(2) + "< null ? " + op.wasNull());
		} catch (SQLException sqle) {
			System.out.println("EXPECTED SQL Exception: " + sqle.getMessage());
		}
		op.close();

		// check to see that a registration is required first for the out parameter.
		op = conn.prepareCall("CALL OP4(?, ?)");
		op.setString(2, "14");
		try {
			executeProcedure(op);
			System.out.println("FAIL - execute succeeded without registration of out parameter");
		} catch (SQLException sqle) {
			expectedException(sqle);
		}
		op.close();

		s.execute("create procedure OP4INOUT(INOUT a DECIMAL(4,2), IN b VARCHAR(255)) parameter style java language java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.inoutparams4'");
		showMatchingProcedures(conn, "OP4INOUT");

		// bug 5264 - first execution fails with parameter not set.

		op = conn.prepareCall("CALL OP4INOUT(?, ?)");
		op.registerOutParameter(1, Types.DECIMAL);

		op.setString(2, null);


		op.setBigDecimal(1, null);
		executeProcedure(op);
		System.out.println("OP4INOUT null >" + op.getBigDecimal(1) + "< null ? " + op.wasNull());


		op.setBigDecimal(1, new BigDecimal("99"));
		executeProcedure(op);
		System.out.println("OP4INOUT null(2) >" + op.getBigDecimal(1) + "< null ? " + op.wasNull());

		op.setString(2, "23.5");
		op.setBigDecimal(1, new BigDecimal("14"));
		executeProcedure(op);
		System.out.println("OP4INOUT 14+23.5 >" + op.getBigDecimal(1) + "< null ? " + op.wasNull());
		
		op.setString(2, "23.505");
		op.setBigDecimal(1, new BigDecimal("9"));
		executeProcedure(op);
		System.out.println("OP4INOUT 9+23.505 >" + op.getBigDecimal(1) + "< null ? " + op.wasNull());

		if (! isDerbyNet) { // with the network server it retains its old value of 9 
		// repeat execution. INOUT parameter now has the value 32.50
		executeProcedure(op);
		System.out.println("OP4INOUT 32.50+23.505 >" + op.getBigDecimal(1) + "< null ? " + op.wasNull());

		} // end bug 5264

		op.setString(2, "67.99");
		op.setBigDecimal(1, new BigDecimal("32.01"));
		try {
			executeProcedure(op);
			System.out.println("FAIL OP4INOUT 32.01+67.99 >" + op.getBigDecimal(1) + "< null ? " + op.wasNull());
		} catch (SQLException sqle) {
			System.out.println("EXPECTED SQL Exception: " + sqle.getMessage());
		}

		op.setString(2, "1");
		op.setBigDecimal(1, new BigDecimal("102.33"));
		try {
			executeProcedure(op);
			System.out.println("FAIL OP4INOUT 1+102.33 >" + op.getBigDecimal(1) + "< null ? " + op.wasNull());
		} catch (SQLException sqle) {
			System.out.println("EXPECTED SQL Exception: " + sqle.getMessage());
		}

		if (! isDerbyNet) {
		// now some checks to requirements for parameter setting.
		op.clearParameters();
		try {
			// a,b not set
			executeProcedure(op);
			System.out.println("FAIL - a,b not set");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED SQL Exception: " + sqle.getMessage());
		}

		op.setString(2, "2");
		try {
			// a not set
			executeProcedure(op);
			System.out.println("FAIL - a  not set");
		} catch (SQLException sqle) {
			System.out.println("EXPECTED SQL Exception: " + sqle.getMessage());
		}

		op.clearParameters();
		op.setBigDecimal(1, new BigDecimal("33"));
		try {
			// b not set
			executeProcedure(op);
			System.out.println("FAIL - b  not set");
		} catch (SQLException sqle) {
			expectedException(sqle);
		}

		} // end bug 5264


		op.close();

		op = conn.prepareCall("CALL OP4INOUT(?, ?)");
		op.setString(2, "14");
		try {
			executeProcedure(op);
			System.out.println("FAIL - execute succeeded without registration of INOUT parameter");
		} catch (SQLException sqle) {
			expectedException(sqle);
		}
		op.close();

		s.execute("DROP PROCEDURE OP1");
		s.execute("DROP PROCEDURE OP2");
		s.execute("DROP PROCEDURE OP3");
		s.execute("DROP PROCEDURE OP4");
		s.execute("DROP PROCEDURE OP4INOUT");
		s.close();

	}

	private static final String[] LITERALS = 
	{"12" /* INTEGER */, "23.43e1" /* DOUBLE */, "176.3" /* DECIMAL */, "'12.34'" /* VARCHAR */};
	private static final String[] LIT_PROC_TYPES = 
	{"SMALLINT", "INTEGER", "BIGINT", "REAL", "DOUBLE", "DECIMAL", "CHAR", "VARCHAR"};
	private static void testLiterals(Connection conn) throws SQLException  {


		System.out.println("literals");

		Statement s = conn.createStatement();

		s.execute("CREATE PROCEDURE LITT.TY_SMALLINT(IN P1 SMALLINT, OUT P2 VARCHAR(256)) NO SQL external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.literalTest' parameter style java language java");
		s.execute("CREATE PROCEDURE LITT.TY_INTEGER(IN P1 INTEGER, OUT P2 VARCHAR(256)) NO SQL external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.literalTest' parameter style java language java");
		s.execute("CREATE PROCEDURE LITT.TY_BIGINT(IN P1 BIGINT, OUT P2 VARCHAR(256)) NO SQL external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.literalTest' parameter style java language java");
		s.execute("CREATE PROCEDURE LITT.TY_REAL(IN P1 REAL, OUT P2 VARCHAR(256)) NO SQL external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.literalTest' parameter style java language java");
		s.execute("CREATE PROCEDURE LITT.TY_DOUBLE(IN P1 DOUBLE, OUT P2 VARCHAR(256)) NO SQL external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.literalTest' parameter style java language java");
		s.execute("CREATE PROCEDURE LITT.TY_DECIMAL(IN P1 DECIMAL(5,2), OUT P2 VARCHAR(256)) NO SQL external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.literalTest' parameter style java language java");
		s.execute("CREATE PROCEDURE LITT.TY_CHAR(IN P1 CHAR(10), OUT P2 VARCHAR(256)) NO SQL external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.literalTest' parameter style java language java");
		s.execute("CREATE PROCEDURE LITT.TY_VARCHAR(IN P1 VARCHAR(10), OUT P2 VARCHAR(256)) NO SQL external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.literalTest' parameter style java language java");

		showMatchingProcedures(conn, "TY_%");

		for (int t = 0; t < LIT_PROC_TYPES.length; t++) {

			String type = LIT_PROC_TYPES[t];

			String sql = "CALL LITT.TY_" + type + " (null, ?)";
			System.out.print(sql);

			try {
				CallableStatement cs = conn.prepareCall(sql);
				cs.registerOutParameter(1, Types.VARCHAR);
				cs.execute();
				String val = cs.getString(1);
				cs.close();
				System.out.println("=" + (val == null ? "<NULL>" : val));
			} catch (SQLException sqle) {
				System.out.println(" (" + sqle.getSQLState() + ") " + sqle.getMessage());
				// more code should be added to check on assignments
				// for now, commenting out the print of the stack, to prevent 
				// failures due to differences between jvms.
				// sqle.printStackTrace(System.out);
			}
		}

		for (int l = 0; l < LITERALS.length; l++) {
			String literal = LITERALS[l];
			for (int t = 0; t < LIT_PROC_TYPES.length; t++) {

				String type = LIT_PROC_TYPES[t];

				String sql = "CALL LITT.TY_" + type + " (" + literal + ", ?)";
				System.out.print(sql);

				try {
					CallableStatement cs = conn.prepareCall(sql);
					cs.registerOutParameter(1, Types.VARCHAR);
					cs.execute();
					String val = cs.getString(1);
					cs.close();
					System.out.println("=" + (val == null ? "<NULL>" : val));
				} catch (SQLException sqle) {
					System.out.println(" (" + sqle.getSQLState() + ") " + sqle.getMessage());
					// code should be added to show the expected errors, now commenting 
					// out the stack print to prevent false failures with different jvms
					//sqle.printStackTrace(System.out);
				}
			}
		}
	}



	private static void expectedException(SQLException sqle) {
		String sqlState = sqle.getSQLState();
		if (sqlState == null) {
			sqlState = "<NULL>";
		}
		System.out.println("EXPECTED SQL Exception: (" + sqlState + ") " + sqle.getMessage());
	}
	
	private static void testSQLControl(Connection conn) throws SQLException  {


		System.out.println("SQL Control");


		Statement s = conn.createStatement();

		s.execute("CREATE SCHEMA SQLC");
		s.execute("CREATE TABLE SQLC.SQLCONTROL_DML(I INT)");
		s.execute("INSERT INTO SQLC.SQLCONTROL_DML VALUES 4");

		String[] control = {"", "NO SQL", "CONTAINS SQL", "READS SQL DATA", "MODIFIES SQL DATA"};

		for (int i = 0; i < control.length; i++) {

			StringBuffer cp = new StringBuffer(256);
			cp.append("CREATE PROCEDURE SQLC.SQLCONTROL1_");
			cp.append(i);
			cp.append(" (OUT E1 VARCHAR(128), OUT E2 VARCHAR(128), OUT E3 VARCHAR(128), OUT E4 VARCHAR(128), OUT E5 VARCHAR(128), OUT E6 VARCHAR(128), OUT E7 VARCHAR(128)) ");
			cp.append(control[i]);
			cp.append(" PARAMETER STYLE JAVA LANGUAGE JAVA EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.ProcedureTest.sqlControl'");

			String cpsql = cp.toString();
			System.out.println(cpsql);

			s.execute(cpsql);
			
			cp.setLength(0);
			cp.append("CREATE PROCEDURE SQLC.SQLCONTROL2_");
			cp.append(i);
			cp.append(" (OUT E1 VARCHAR(128), OUT E2 VARCHAR(128), OUT E3 VARCHAR(128), OUT E4 VARCHAR(128), OUT E5 VARCHAR(128), OUT E6 VARCHAR(128), OUT E7 VARCHAR(128)) ");
			cp.append(control[i]);
			cp.append(" PARAMETER STYLE JAVA LANGUAGE JAVA EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.ProcedureTest.sqlControl2'");

			cpsql = cp.toString();
			System.out.println(cpsql);

			s.execute(cpsql);

			cp.setLength(0);
			cp.append("CREATE PROCEDURE SQLC.SQLCONTROL3_");
			cp.append(i);
			cp.append(" (OUT E1 VARCHAR(128), OUT E2 VARCHAR(128), OUT E3 VARCHAR(128), OUT E4 VARCHAR(128), OUT E5 VARCHAR(128), OUT E6 VARCHAR(128), OUT E7 VARCHAR(128)) ");
			cp.append(control[i]);
			cp.append(" PARAMETER STYLE JAVA LANGUAGE JAVA EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.ProcedureTest.sqlControl3'");

			cpsql = cp.toString();
			System.out.println(cpsql);

			s.execute(cpsql);

			cp.setLength(0);
			cp.append("CREATE PROCEDURE SQLC.SQLCONTROL4_");
			cp.append(i);
			cp.append(" (IN SQLC INTEGER, OUT E1 VARCHAR(128), OUT E2 VARCHAR(128), OUT E3 VARCHAR(128), OUT E4 VARCHAR(128), OUT E5 VARCHAR(128), OUT E6 VARCHAR(128), OUT E7 VARCHAR(128), OUT E8 VARCHAR(128)) ");
			cp.append(control[i]);
			cp.append(" PARAMETER STYLE JAVA LANGUAGE JAVA EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.ProcedureTest.sqlControl4'");

			cpsql = cp.toString();
			System.out.println(cpsql);

			s.execute(cpsql);
		}
		showMatchingProcedures(conn, "SQLCONTROL1_%");
		showMatchingProcedures(conn, "SQLCONTROL2_%");
		showMatchingProcedures(conn, "SQLCONTROL3_%");
		showMatchingProcedures(conn, "SQLCONTROL4_%");

		if (!conn.getAutoCommit())
			conn.commit();

		for (int i = 0; i < control.length; i++) {
			String type = control[i];
			if (type.length() == 0)
				type = "DEFAULT (MODIFIES SQL DATA)";

			System.out.println("** SQL ** " + type);
			for (int k = 1; k <=3; k++) {
				CallableStatement cs = conn.prepareCall("CALL SQLC.SQLCONTROL" + k + "_" + i + " (?, ?, ?, ?, ?, ?, ?)");
				for (int rop = 1; rop <=7 ; rop++) {
					cs.registerOutParameter(rop, Types.VARCHAR);
				}
				cs.execute();
				for (int p = 1; p <= 7; p++) {
					System.out.println("    " + cs.getString(p));
				}
				cs.close();
			}
			
		}

		// test procedures that call others, e.g. to ensure that within a READS SQL DATA procedure, a MODIFIES SQL DATA cannot be called.
		// table was dropped by previous executions.
		s.execute("CREATE TABLE SQLC.SQLCONTROL_DML(I INT)");
		s.execute("INSERT INTO SQLC.SQLCONTROL_DML VALUES 4");
		for (int i = 0; i < control.length; i++) {
			String type = control[i];
			if (type.length() == 0)
				type = "DEFAULT (MODIFIES SQL DATA)";

			System.out.println("CALL ** " + type);
			for (int t = 0; t < control.length; t++) {

				String ttype = control[t];
				if (ttype.length() == 0)
					ttype = "DEFAULT (MODIFIES SQL DATA)";
				System.out.println("    CALLLING " + ttype);
				CallableStatement cs = conn.prepareCall("CALL SQLC.SQLCONTROL4_" + i + " (?, ?, ?, ?, ?, ?, ?, ?, ?)");
				cs.setInt(1, t);
				for (int rop = 2; rop <=9 ; rop++) {
					cs.registerOutParameter(rop, Types.VARCHAR);
				}

				cs.execute();
				for (int p = 2; p <= 9; p++) {
					String so = cs.getString(p);
					if (so == null)
						continue;
					System.out.println("         " + so);
				}
				cs.close();
			}
		}
		// Make sure we throw proper error with network server 
		// if params are not registered
		testBug5280(conn);

		s.execute("DROP TABLE SQLC.SQLCONTROL_DML");

		for (int i = 0; i < control.length; i++) {
			s.execute("DROP PROCEDURE SQLCONTROL1_" + i);
			s.execute("DROP PROCEDURE SQLCONTROL2_" + i);
			s.execute("DROP PROCEDURE SQLCONTROL4_" + i);
		}
		s.execute("DROP TABLE SQLC.SQLCONTROL_DDL");
		s.execute("SET SCHEMA APP");
		s.execute("DROP SCHEMA SQLC RESTRICT");

		s.close();
	}

	private static void showMatchingProcedures(Connection conn, String procedureName) throws SQLException {
		// Until cs defaults to hold cursor we need to turn autocommit off 
		// while we do this because one metadata call will close the other's
		// cursor
		boolean saveAutoCommit = conn.getAutoCommit();
		conn.setAutoCommit(false);
		System.out.println("DEFINED PROCEDURES FOR " + procedureName);
		PreparedStatement ps = conn.prepareStatement("select schemaname, alias, CAST (((javaclassname || '.' ) || CAST (aliasinfo AS VARCHAR(1000))) AS VARCHAR(2000)) AS SIGNATURE " + 
								" from sys.sysaliases A, sys.sysschemas S where alias like ? and A.schemaid = S.schemaid ORDER BY 1,2,3");

		ps.setString(1, procedureName);

		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			System.out.println("  " + rs.getString(1) + "." + rs.getString(2) + " AS " + rs.getString(3));
		}
		rs.close();

		System.out.println("DATABASE METATDATA PROCEDURES FOR " + procedureName);
		DatabaseMetaData dmd = conn.getMetaData();

		rs = dmd.getProcedures(null, null, procedureName);
		// with jcc 2.1 for now this will fail on the second round, 
		// because the resultset gets closed when we do getProcedureColumns. 
		// thus, catch that gracefully...
		try {
		while (rs.next()) {
			String schema = rs.getString(2);
			String name = rs.getString(3);
			System.out.println("  " + schema + "." + name + " AS " + rs.getString(7) + " type " + TYPE(rs.getShort(8)));
			// get the column information.
			ResultSet rsc = dmd.getProcedureColumns(null, schema, name, null);
			while (rsc.next()) {
				System.out.println("    " + PARAMTYPE(rsc.getShort(5)) + " " + rsc.getString(4) +  " " + rsc.getString(7));
			}
			rsc.close();
		}
		rs.close();
		// restore previous autocommit mode
		conn.setAutoCommit(saveAutoCommit);
		} catch (SQLException sqle) {
			System.out.println("FAILure: ");
			sqle.printStackTrace();
		}

		System.out.println("------------");
	}

	static String TYPE(short type) {
		switch (type) {
		case DatabaseMetaData.procedureResultUnknown:
			return "procedureResultUnknown";
		case DatabaseMetaData.procedureNoResult:
			return "procedureNoResult";
		case DatabaseMetaData.procedureReturnsResult:
			return "procedureReturnsResult";
		default:
			return "??????";
		}

	}
	static String PARAMTYPE(short type) {
		switch (type) {
		case DatabaseMetaData.procedureColumnUnknown: return "procedureColumnUnknown";
		case DatabaseMetaData.procedureColumnIn: return "procedureColumnIn";
		case DatabaseMetaData.procedureColumnInOut: return "procedureColumnInOut";
		case DatabaseMetaData.procedureColumnOut: return "procedureColumnOut";
		case DatabaseMetaData.procedureColumnReturn : return "procedureColumnReturn";
		case DatabaseMetaData.procedureColumnResult : return "procedureColumnResult";
		default: return "???";
		}
	}

	private static void statementExceptionExpected(Statement s, String sql) {
		System.out.println(sql);
		try {
			s.execute(sql);
			System.out.println("FAIL - SQL expected to throw exception");
		} catch (SQLException sqle) {
			expectedException(sqle);
		}
	}
	private static void callExceptionExpected(Connection conn, String callSQL) throws SQLException {
		System.out.println(callSQL);
		try {
			CallableStatement cs = conn.prepareCall(callSQL);
			executeProcedure(cs);
			cs.close();
			System.out.println("FAIL - SQL expected to throw exception ");
		} catch (SQLException sqle) {
			expectedException(sqle);
		}
	}

	/* ****
	 * Beetle 5292 (for Network Server): Check for the return
	 * of LOB columns in a result set.
	 */

	private static void testResultSetsWithLobs(Connection conn) {

		Statement s = null;

		// Create objects.
		try {
			s = conn.createStatement();
			
			// Clob.
			s.execute("create table lobCheckOne (c clob(30))");
			s.execute("insert into lobCheckOne values (cast " +
					  "('yayorsomething' as clob(30)))");
			s.execute("insert into lobCheckOne values (cast " +
						  "('yayorsomething2' as clob(30)))");
			s.execute("create procedure clobproc () parameter style java " +
				"language java external name " +
					  "'org.apache.derbyTesting.functionTests.util.ProcedureTest.clobselect' " +
					  "dynamic result sets 3 reads sql data");
			// Blob.
			s.execute("create table lobCheckTwo (b blob(30))");
			s.execute("insert into lobCheckTwo values (cast " + "(" + 
					  TestUtil.stringToHexLiteral("101010001101") +
					  " as blob(30)))");
			s.execute("insert into lobCheckTwo values (cast " +
					  "(" +
					  TestUtil.stringToHexLiteral("101010001101") +
					  " as blob(30)))");
			s.execute("create procedure blobproc () parameter style java " +
				"language java external name " +
				"'org.apache.derbyTesting.functionTests.util.ProcedureTest.blobselect' " +
				"dynamic result sets 1 reads sql data");

		} catch (SQLException e) {
			System.out.println("FAIL: Couldn't create required objects:");
			e.printStackTrace();
		}

		// Run 5292 Tests.
		try {

			// Clobs.

			System.out.println("Stored Procedure w/ CLOB in result set.");
			CallableStatement cs = conn.prepareCall("CALL clobproc()");
			executeProcedure(cs);
			cs.close();
			
			// Blobs.

			System.out.println("Stored Procedure w/ BLOB in result set.");
			cs = conn.prepareCall("CALL blobproc()");
			executeProcedure(cs);
			cs.close();

		} catch (Exception e) {
			System.out.println("FAIL: Encountered exception:");
			e.printStackTrace();
		}

		try {
		// Clean up.
			s.execute("drop table lobCheckOne");
			s.execute("drop table lobCheckTwo");
			s.execute("drop procedure clobproc");
			s.execute("drop procedure blobproc");
			s.close();
		} catch (Exception e) {
			System.out.println("FAIL: Cleanup for lob result sets test:");
			e.printStackTrace();
		}

		return;

	}

    /**
     * Sets up and runs two tests with multiple ResultSets
     * 
     * @param conn The Connection
     * @throws SQLException
     */
    private static void multipleRSTests(Connection conn) throws SQLException {
        //DerbyNet is known to fail this test
        if (TestUtil.isJCCFramework()) return;
        
        setHoldability(conn, JDBC30Translation.HOLD_CURSORS_OVER_COMMIT);
        int iso = conn.getTransactionIsolation();
        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        //Installing Procedure
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select tablename from sys.systables " +
                "where tablename = 'AUTOCOMMITTABLE'");
        if (rs.next()) {
            rs.close();
            stmt.executeUpdate("delete from autoCommitTable");
        } else {
            rs.close();
            stmt.executeUpdate("create table autoCommitTable (num int)");
        }
        
        ResultSet mdrs = conn.getMetaData().getProcedures(
                null, null, "MULTIRESULT");
        if (mdrs != null || !mdrs.next()) {
            stmt.executeUpdate("create procedure multiResult(p1 int, " +
                    "p2 int) parameter style JAVA READS SQL DATA dynamic " +
                    "result sets 2 language java external name " +
                    "'org.apache.derbyTesting.functionTests." +
                    "util.ProcedureTest.multiResult'");
        }
        mdrs.close();
        multipleRSAutoCommit(conn);
        multipleRSNoCommit(conn);
        stmt.executeUpdate("drop procedure multiResult");
        stmt.executeUpdate("drop table autoCommitTable");
        stmt.close();
        conn.setTransactionIsolation(iso);
    }
    
    /**
     * Test to see that an auto commit occurs for multiple ResultSets if all 
     * ResultSets but one are closed and the final ResultSet has completed.
     * 
     * @param conn The Connection
     * @throws SQLException
     */
    private static void multipleRSAutoCommit(Connection conn) throws SQLException {
        System.out.print("MultipleRSAutoCommit: ");
        CallableStatement cs = conn.prepareCall("call multiResult(?, ?)");
        cs.setInt(1, 1);
        cs.setInt(2, 2);
        cs.execute();
        ResultSet rs = null;
        do {
            if (rs != null)
                rs.close();
            rs = cs.getResultSet();
            while (rs.next());
            
            if (rs.next()) {
                System.out.println("FAIL. Final call to ResultSet should return false.");
            }
        } while (getMoreResults(cs));
        
        if (!checkLocks()) {
            return;
        }
        
        System.out.println("PASS. ");
        
        if (rs != null)
            rs.close();
        cs.close();
    }
    
    /**
     * Used to insure that there is no auto-commit in the event that there is
     * more then one ResultSet open.
     * 
     * @param conn The Connection
     * @throws SQLException
     */
    private static void multipleRSNoCommit(Connection conn) throws SQLException {
        System.out.print("MultipleRSNoCommit: ");
        CallableStatement cs = conn.prepareCall("call multiResult(?, ?)");
        cs.setInt(1, 1);
        cs.setInt(2, 2);
        cs.execute();
        ResultSet rs = null;
        do {
            rs = cs.getResultSet();
            while (rs.next());
            
            if (rs.next()) {
                System.out.println("FAIL. Final call to ResultSet should return false.");
            }
        } while (getMoreResults(cs));
        
        if (checkLocks()) {
            System.out.println("FAIL. Connection incorrectly auto-committed.");
        }
        
        System.out.println("PASS. ");
        
        if (rs != null)
            rs.close();
        cs.close();
    }

    // DERBY-821: Test that the result set is not implicitly closed on
    // the server when EXCSQLSTT is used to open the result set.
    private static void testImplicitClose(Connection conn) throws SQLException {
		System.out.print("testImplicitClose(): ");
		final String proc =
			"org.apache.derbyTesting.functionTests.util.ProcedureTest." +
			"selectRows";
		boolean savedAutoCommit = conn.getAutoCommit();
		conn.setAutoCommit(false);
		Statement stmt = conn.createStatement();
		stmt.executeUpdate("create table derby821 (id int)");
		stmt.executeUpdate("insert into derby821 (id) values (1), (2)");
		stmt.execute("create procedure jira821 (name varchar(50)) " +
					 "parameter style java language java external name " +
					 "'" + proc + "' dynamic result sets 1 reads sql data");
		
        // Call setupStatementReuse which will make the server to reuse an existing statement.
		setupStatementReuse(conn);
		CallableStatement cs = conn.prepareCall("call jira821 (?)");
		cs.setString(1, "derby821");
		cs.execute();
		ResultSet rs = cs.getResultSet();
		rs.next();
		boolean passed = false;
		try {
			// We expect the result set to be open, so dropping the
			// table should fail.
			stmt.executeUpdate("drop table derby821");
		} catch (SQLException sqle) {
			if (sqle.getSQLState().equals("X0X95")) {
				System.out.println("PASSED");
				passed = true;
			} else {
				System.out.println("FAILED");
				throw sqle;
			}
		}
		if (!passed) {
			// Table was successfully dropped, hence the result set
			// must have been implicitly closed.
			System.out.println("FAILED (no exception thrown)");
		}
		conn.rollback();
		conn.setAutoCommit(savedAutoCommit);
	}

    /**
     * This method is used to set up an environment which can be used to test 
     * DERBY-1002. It creates statements and closes them to provoke the client
     * driver to re-use sections which in turn will make the network server to
     * re-use statements and result sets. It does not test anything by itself.
     * It just sets up an environment where the statements used in this test 
     * will be re-used in later tests. It is called from methods 
     * 'jira_491_492' and 'testImplicitClose'. When the re-use was not happening 
     * correctly, 'jira_491_492' and 'testImplicitClose' were giving following 
     * errors:
     * 
     * 1. In the test for jira491, client expects a QRYDTA for the CNTQRY request. 
     * Instead, it recieves a QRYNOPRM reply because server closes the query 
     * wrongly.
     * 2. In testImplicitClose, the query is not supposed to be closed in case
     * of EXCSQLSTT commands. If re-use happens wrongly, server closes the query 
     * for EXCSQLSTT commands too.
     *   
     * @param conn Connection
     */
    private static void setupStatementReuse(Connection conn)
    							throws SQLException{
    	
    	Statement stmt = conn.createStatement();
		try {
			stmt.execute("drop table test_table_jira_1002");
		} catch (SQLException se) { }

		try {
			stmt.execute("drop procedure test_proc_jira_1002");
		} catch (SQLException se) { }

		stmt.execute("create table test_table_jira_1002(id int)");
		stmt.execute("insert into test_table_jira_1002 values(1) , (2)");

		//create a procedure which returns a result set
		stmt.execute("create procedure test_proc_jira_1002(name varchar(50)) " +
					"language java parameter style java external name " +
					"'org.apache.derbyTesting.functionTests.util.ProcedureTest.selectRows'" +
					"dynamic result sets 1");
    	
    	
		// Create a select statement to make currentDrdaRs.qryclsimp=CodePoint.QRYCLSIMP_YES
    	Statement st_opnqry = conn.createStatement();
		ResultSet rs_opnqry = st_opnqry.executeQuery("SELECT * FROM TEST_TABLE_JIRA_1002");
		rs_opnqry.next();
		// Close st_opnqry so that cSt1 will reuse same DRDAStatement
		st_opnqry.close();

		// Use up the next statement's result set to make currentDrdaRs.hasdata=false
		CallableStatement cSt1 = conn.prepareCall("call test_proc_jira_1002(?)");
		cSt1.setString(1, "test_table_jira_1002");
		cSt1.execute();
		ResultSet rs1 = cSt1.getResultSet();
		rs1.next();
		// Close cSt1 so that a statement created after a call to this method 
		// will cause the server to use same DRDAStatement.
		cSt1.close();
    	
    }    
    
    /**
     * Checks to see if there is a lock on a table by attempting to modify the
     * same table. If the first connection was serializable then it will 
     * continue to hold a lock and the second Connection will time out.
     * 
     * @return false if the a lock could not be established, true if a lock
     * can be established.
     * @throws SQLException
     */
    private static boolean checkLocks() throws SQLException {
        Connection conn = null;
        try {
            conn = ij.startJBMS();
        } catch (Exception e) {
            System.out.println("FAIL. Unable to establish connection in checkLocks");
            return false;
        }
        Statement stmt = conn.createStatement();
        try {
            stmt.executeUpdate("update AutoCommitTable " 
                    + "set num = 3 where num = 2");
            stmt.executeUpdate("update AutoCommitTable " 
                    + "set num = 2 where num = 3");
        } catch (SQLException e) {
            if (e.getSQLState().equals(SQLState.LOCK_TIMEOUT)) {
                return false;
            } else {
                throw e;
            }
        }
        stmt.close();
        conn.close();
        return true;
    }
    
    /**
     * Sets the holdability of a Connection using reflection so it is
     * JDBC2.0 compatible.
     * 
     * @param conn The Connection
     * @param hold The new holdability.
     * @throws SQLException
     */
    public static void setHoldability(Connection conn, int hold) throws SQLException {
        try {
            Object[] holdArray = {new Integer(hold)};
            Method sh = conn.getClass().getMethod("setHoldability", CONN_PARAM);
            sh.invoke(conn, holdArray);
        } catch (Exception e) {System.out.println("shouldn't get that error " + e.getMessage());}//for jdks prior to jdk14
    }
    
    /**
     * Uses reflection to call CallableStatement.getMoreResults(KEEP_CURRENT_RESULT)
     * for JDBC2.0 compatibilty
     * @param cs The Callable statement
     * @return boolean value indicating if there are more results 
     * @throws SQLException
     */
    public static boolean getMoreResults(CallableStatement cs) throws SQLException {
        try {
            Object[] holdArray = {new Integer(JDBC30Translation.KEEP_CURRENT_RESULT)};
            Method sh = cs.getClass().getMethod("getMoreResults", CONN_PARAM);
            Boolean temp = (Boolean)sh.invoke(cs, holdArray);
            return temp.booleanValue();
        } catch (Exception e) {return cs.getMoreResults();}//for jdks prior to jdk14 
    }

    /** 
     * clean up any objects not cleaned up by previous efforts
     */
    private static void cleanUp(Connection conn) throws SQLException {
        String[] testObjects = {
            "table t1", "procedure procdup", "schema s1 restrict",
            "schema s2 restrict", "procedure drs", "procedure drs2",
            "procedure litt.ty_smallint", "procedure litt.ty_integer", "procedure litt.ty_bigint",
            "procedure litt.ty_real", "procedure litt.ty_double", "procedure litt.ty_decimal",
            "procedure litt.ty_char", "procedure litt.ty_varchar",
            "table SQLC.SQLCONTROL_DDL", "table SQLCONTROL_DDL",
            "table SQLC.SQLCONTROL_DML",
             }; 
        Statement stmt = conn.createStatement();
        TestUtil.cleanUpTest(stmt, testObjects);
    } 

}
