/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.TestResultSetMethods
 
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

import org.apache.derby.impl.jdbc.Util;

import java.io.Reader;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.derby.shared.common.reference.SQLState;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.SQLStateConstants;

/**
 * This class is used to test the implementations of the JDBC 4.0 methods
 * in the ResultSet interface
 */
public class TestResultSetMethods {
    
    Connection conn=null;
    PreparedStatement ps=null;
    ResultSet rs=null;
    
    /**
     * Checks that a <code>boolean</code> value is
     * <code>true</code>. Throws an exception if it is false.
     *
     * @param expr boolean expected to be true
     * @param msg message when assertion fails
     * @exception RuntimeException if <code>expr</code> is false
     */
    private static void assert_(boolean expr, String msg) {
        if (!expr) {
            throw new RuntimeException("Assertion failed: " + msg);
        }
    }
    
    /**
     * Tests that <code>ResultSet.getHoldability()</code> has the
     * correct behaviour.
     */
    void t_getHoldability() {
        Boolean savedAutoCommit = null;
        try {
            savedAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);

            // test default holdability
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("values(1)");
            assert_(rs.getHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT,
                    "default holdability is HOLD_CURSORS_OVER_COMMIT");
            rs.close();
            try {
                rs.getHoldability();
                assert_(false, "getHoldability() should fail when closed");
            } catch (SQLException sqle) {
                String sqlState = sqle.getSQLState();
                // client driver throws exception with SQL state null
                // when result set is closed
                if (sqlState != null &&
                    !sqlState.equals("XCL16")) {
                    throw sqle;
                }
            }

            // test explicitly set holdability
            final int[] holdabilities = {
                ResultSet.HOLD_CURSORS_OVER_COMMIT,
                ResultSet.CLOSE_CURSORS_AT_COMMIT,
            };
            for (int h : holdabilities) {
                Statement s =
                    conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                         ResultSet.CONCUR_READ_ONLY, h);
                rs = s.executeQuery("values(1)");
                assert_(rs.getHoldability() == h,
                        "holdability " + holdabilityString(h));
                rs.close();
                s.close();
            }

            // test holdability of result set returned from a stored
            // procedure (DERBY-1101)
            stmt.execute("create procedure getresultsetwithhold(in hold int) " +
                         "parameter style java language java external name " +
                         "'org.apache.derbyTesting.functionTests.tests." +
                         "jdbc4.TestResultSetMethods." +
                         "getResultSetWithHoldability' " +
                         "dynamic result sets 1 reads sql data");
            for (int statementHoldability : holdabilities) {
                for (int procHoldability : holdabilities) {
                    CallableStatement cs =
                        conn.prepareCall("call getresultsetwithhold(?)",
                                         ResultSet.TYPE_FORWARD_ONLY,
                                         ResultSet.CONCUR_READ_ONLY,
                                         statementHoldability);
                    cs.setInt(1, procHoldability);
                    cs.execute();
                    rs = cs.getResultSet();
                    int holdability = rs.getHoldability();
                    assert_(holdability == procHoldability,
                            "holdability of ResultSet from stored proc: " +
                            holdabilityString(holdability));
                    conn.commit();
                    boolean holdable;
                    try {
                        rs.next();
                        holdable = true;
                    } catch (SQLException sqle) {
                        String sqlstate = sqle.getSQLState();
                        // SQL state for closed result set is XCL16,
                        // but it is null in the client driver
                        if (sqlstate == null || sqlstate.equals("XCL16")) {
                            holdable = false;
                        } else {
                            throw sqle;
                        }
                    }
                    if (holdable) {
                        assert_(holdability ==
                                ResultSet.HOLD_CURSORS_OVER_COMMIT,
                                "non-holdable result set not closed on commit");
                    } else {
                        assert_(holdability ==
                                ResultSet.CLOSE_CURSORS_AT_COMMIT,
                                "holdable result set closed on commit");
                    }
                    rs.close();
                    cs.close();
                }
            }
            stmt.execute("drop procedure getresultsetwithhold");
            stmt.close();
            conn.commit();
        } catch(Exception e) {
            System.out.println("Unexpected exception caught " + e);
            e.printStackTrace(System.out);
        } finally {
            if (savedAutoCommit != null) {
                try {
                    conn.setAutoCommit(savedAutoCommit);
                } catch (SQLException sqle) {
                    sqle.printStackTrace(System.out);
                }
            }
        }
    }

    /**
     * Convert holdability from an integer to a readable string.
     *
     * @param holdability an <code>int</code> value representing a holdability
     * @return a <code>String</code> value representing the same holdability
     */
    private static String holdabilityString(int holdability) {
        switch (holdability) {
        case ResultSet.HOLD_CURSORS_OVER_COMMIT:
            return "HOLD_CURSORS_OVER_COMMIT";
        case ResultSet.CLOSE_CURSORS_AT_COMMIT:
            return "CLOSE_CURSORS_AT_COMMIT";
        default:
            return "UNKNOWN HOLDABILITY";
        }
    }
    
    /**
     * Tests that <code>ResultSet.isClosed()</code> returns the
     * correct value in different situations.
     */
    void t_isClosed(){
        try {
            Statement stmt = conn.createStatement();

            // simple open/read/close test
            ResultSet rs = stmt.executeQuery("values(1)");
            assert_(!rs.isClosed(), "rs should be open");
            while (rs.next());
            assert_(!rs.isClosed(), "rs should be open");
            rs.close();
            assert_(rs.isClosed(), "rs should be closed");

            // execute and re-execute statement
            rs = stmt.executeQuery("values(1)");
            assert_(!rs.isClosed(), "rs should be open");
            ResultSet rs2 = stmt.executeQuery("values(1)");
            assert_(rs.isClosed(), "rs should be closed");
            assert_(!rs2.isClosed(), "rs2 should be open");

            // re-execute another statement on the same connection
            Statement stmt2 = conn.createStatement();
            rs = stmt2.executeQuery("values(1)");
            assert_(!rs2.isClosed(), "rs2 should be open");
            assert_(!rs.isClosed(), "rs should be open");

            // retrieve multiple result sets
            stmt.execute("create procedure retrieve_result_sets() " +
                         "parameter style java language java external name " +
                         "'org.apache.derbyTesting.functionTests.tests." +
                         "jdbc4.TestResultSetMethods.threeResultSets' " +
                         "dynamic result sets 3 reads sql data");
            stmt.execute("call retrieve_result_sets()");
            ResultSet[] rss = new ResultSet[3];
            int count = 0;
            do {
                rss[count] = stmt.getResultSet();
                assert_(!rss[count].isClosed(),
                        "rss[" + count + "] should be open");
                if (count > 0) {
                    assert_(rss[count-1].isClosed(),
                            "rss[" + (count-1) + "] should be closed");
                }
                ++count;
            } while (stmt.getMoreResults());
            assert_(count == 3, "expected three result sets");
            stmt.execute("drop procedure retrieve_result_sets");

            // close statement
            rs = stmt2.executeQuery("values(1)");
            stmt2.close();
            assert_(rs.isClosed(), "rs should be closed");

            // close connection
            Connection conn2 = ij.startJBMS();
            stmt2 = conn2.createStatement();
            rs = stmt2.executeQuery("values(1)");
            conn2.close();
            assert_(rs.isClosed(), "rs should be closed");

            stmt.close();
            stmt2.close();
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
        }
    }
    
    /**
     * Test that an exception is thrown when methods are called
     * on a closed result set (DERBY-1060).
     */
    private void testExceptionWhenClosed() {
        try {
            // create a result set and close it
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("values(1)");
            rs.close();

            // maps method name to parameter list
            HashMap<String, Class[]> params = new HashMap<String, Class[]>();
            // maps method name to argument list
            HashMap<String, Object[]> args = new HashMap<String, Object[]>();

            // methods with no parameters
            String[] zeroArgMethods = {
                "getWarnings", "clearWarnings", "getStatement",
                "getMetaData", "getConcurrency", "getHoldability",
                "getRow", "getType", "rowDeleted", "rowInserted",
                "rowUpdated", "getFetchDirection", "getFetchSize",
            };
            for (String name : zeroArgMethods) {
                params.put(name, null);
                args.put(name, null);
            }

            // methods with a single int parameter
            for (String name : new String[] { "setFetchDirection",
                                              "setFetchSize" }) {
                params.put(name, new Class[] { Integer.TYPE });
                args.put(name, new Integer[] { 0 });
            }

            // invoke the methods
            for (String name : params.keySet()) {
                try {
                    Method method =
                        rs.getClass().getMethod(name, params.get(name));
                    try {
                        method.invoke(rs, args.get(name));
                    } catch (InvocationTargetException ite) {
                        Throwable cause = ite.getCause();
                        if (cause instanceof SQLException) {
                            SQLException sqle = (SQLException) cause;
                            String state = sqle.getSQLState();
                            // Should get SQL state XCL16 when the
                            // result set is closed, but client driver
                            // sends null.
                            if (state == null ||
                                state.equals("XCL16")) {
                                continue;
                            }
                        }
                        throw cause;
                    }
                    System.out.println("no exception thrown for " + name +
                                       "() when ResultSet is closed");
                } catch (Throwable t) {
                    System.out.println("Unexpected exception when " +
                                       "invoking " + name + "():");
                    t.printStackTrace(System.out);
                }
            }
            stmt.close();
        } catch (SQLException e) {
            System.out.println("Unexpected exception caught:");
            e.printStackTrace(System.out);
        }
    }
    
    /**
     * Tests the wrapper methods isWrapperFor and unwrap. There are two cases
     * to be tested
     * Case 1: isWrapperFor returns true and we call unwrap
     * Case 2: isWrapperFor returns false and we call unwrap
     *
     * @param rs The ResultSet object on which the wrapper 
     *           methods are tested
     */
    void t_wrapper(ResultSet rs) {
        Class<ResultSet> wrap_class = ResultSet.class;
        
        //The if succeeds and we call the unwrap method on the conn object        
        try {
            if(rs.isWrapperFor(wrap_class)) {
                ResultSet rs1 = 
                        (ResultSet)rs.unwrap(wrap_class);
            }
            else {
                System.out.println("isWrapperFor wrongly returns false");
            }
        }
        catch(SQLException sqle) {
            sqle.printStackTrace();
        }
        
        //Being Test for Case2
        //test for the case when isWrapper returns false
        //using some class that will return false when 
        //passed to isWrapperFor
        Class<PreparedStatement> wrap_class1 = PreparedStatement.class;
        
        try {
            //returning false is the correct behaviour in this case
            //Generate a message if it returns true
            if(rs.isWrapperFor(wrap_class1)) {
                System.out.println("isWrapperFor wrongly returns true");
            }
            else {
                PreparedStatement ps1 = (PreparedStatement)
                                           rs.unwrap(wrap_class1);
                System.out.println("unwrap does not throw the expected " +
                                   "exception");
            }
        }
        catch (SQLException sqle) {
            //Calling unwrap in this case throws an 
            //SQLException ensure that this SQLException 
            //has the correct SQLState
            if(!SQLStateConstants.UNABLE_TO_UNWRAP.equals(sqle.getSQLState())) {
                sqle.printStackTrace();
            }
        }
    }
    void startTestResultSetMethods(Connection conn_in,PreparedStatement ps_in,ResultSet rs_in) {
        conn = conn_in;
        ps = ps_in;
        rs = rs_in;
        
        t_getHoldability();
        t_isClosed();
        
        testExceptionWhenClosed();
    }
    
    /**
     * Method that is invoked by <code>t_isClosed()</code> (as a
     * stored procedure) to retrieve three result sets.
     *
     * @param rs1 first result set
     * @param rs2 second result set
     * @param rs3 third result set
     * @exception SQLException if a database error occurs
     */
    public static void threeResultSets(ResultSet[] rs1,
                                       ResultSet[] rs2,
                                       ResultSet[] rs3)
        throws SQLException
    {
        Connection c = DriverManager.getConnection("jdbc:default:connection");
        Statement stmt1 = c.createStatement();
        rs1[0] = stmt1.executeQuery("values(1)");
        Statement stmt2 = c.createStatement();
        rs2[0] = stmt2.executeQuery("values(1)");
        Statement stmt3 = c.createStatement();
        rs3[0] = stmt3.executeQuery("values(1)");
        c.close();
    }

    /**
     * Method invoked by <code>t_getHoldability()</code> (as a stored
     * procedure) to retrieve a result set with a given holdability.
     *
     * @param holdability requested holdability
     * @param rs result set returned from stored procedure
     * @exception SQLException if a database error occurs
     */
    public static void getResultSetWithHoldability(int holdability,
                                                   ResultSet[] rs)
        throws SQLException
    {
        Connection c = DriverManager.getConnection("jdbc:default:connection");
        Statement s = c.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                        ResultSet.CONCUR_READ_ONLY,
                                        holdability);
        rs[0] = s.executeQuery("values (1), (2), (3)");
        c.close();
    }
    
    /**
     * <p>
     * Return true if we're running under the embedded client.
     * </p>
     * @return a boolean value signifying whether we are running under the 
     *         embedded framework
     */
    private static boolean usingEmbeddedClient()
    {
            return "embedded".equals(System.getProperty("framework" ) );
    }
    
    public static void main(String args[]) {
		try {
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
		
			Connection	conn_main = ij.startJBMS();

			PreparedStatement ps_main=null;
			ResultSet rs_main=null;
        
			ps_main = conn_main.prepareStatement("select count(*) from sys.systables");
			rs_main = ps_main.executeQuery();
        
			TestResultSetMethods trsm = new TestResultSetMethods();
                        
			trsm.startTestResultSetMethods(conn_main,ps_main,rs_main);
                        trsm.t_wrapper(rs_main);
		} catch(Exception e) {
			System.out.println(""+e);
			e.printStackTrace();
		}
        
        
    }
}
