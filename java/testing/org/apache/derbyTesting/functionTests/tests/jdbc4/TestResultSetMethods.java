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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.derby.shared.common.reference.SQLState;

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
    
    void t_getRowId1() {
        try {
            rs.getRowId(0);
            System.out.println("unImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
        }
    }
    void t_getRowId2(){
        try {
            rs.getRowId(null);
            System.out.println("unImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
        }
    }
    
    void t_updateRowId1() {
        try {
            rs.updateRowId(0,null);
            System.out.println("unImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
        }
    }
    
    void t_updateRowId2(){
        try {
            rs.updateRowId(null,null);
            System.out.println("unImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
        }
    }
    
    /**
     * Tests that <code>ResultSet.getHoldability()</code> has the
     * correct behaviour.
     */
    void t_getHoldability() {
        try {
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
            stmt.close();
            // test explicitly set holdability
            for (int h : new int[] { ResultSet.HOLD_CURSORS_OVER_COMMIT,
                                     ResultSet.CLOSE_CURSORS_AT_COMMIT }) {
                stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                            ResultSet.CONCUR_READ_ONLY, h);
                rs = stmt.executeQuery("values(1)");
                assert_(rs.getHoldability() == h, "holdability " + h);
                rs.close();
                stmt.close();
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
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

            stmt.close();
            stmt2.close();
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
        }
    }
    
    void t_updateNString1() {
        try {
            rs.updateNString(0,null);
            System.out.println("unImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
        }
    }
    
    void t_updateNString2() {
        try {
            rs.updateNString(null,null);
            System.out.println("unImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
        }
    }
    
    void t_updateNClob1() {
        try {
            rs.updateNClob(0,null);
            System.out.println("unImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
        }
    }
    
    void t_updateNClob2() {
        try {
            rs.updateNClob(null,null);
            System.out.println("unImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
        }
    }
    
    void t_getNClob1() {
        try {
            rs.getNClob(0);
            System.out.println("unImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
        }
    }
    
    void t_getNClob2() {
        try {
            rs.getNClob(null);
            System.out.println("unImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
        }
    }
    
    void t_getSQLXML1() {
        try {
            rs.getSQLXML(0);
            System.out.println("unImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
        }
    }
    
    void t_getSQLXML2() {
        try {
            rs.getSQLXML(null);
            System.out.println("unImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
        }
    }
    
    void t_updateSQLXML1(){
    }
    
    void t_updateSQLXML2() {
    }
    
    void startTestResultSetMethods(Connection conn_in,PreparedStatement ps_in,ResultSet rs_in) {
        conn = conn_in;
        ps = ps_in;
        rs = rs_in;
        
        t_getRowId1();
        t_getRowId2();
        
        t_updateRowId1();
        t_updateRowId2();
        
        t_getHoldability();
        t_isClosed();
        
        t_updateNString1();
        t_updateNString2();
        
        t_updateNClob1();
        t_updateNClob2();
        
        t_getNClob1();
        t_getNClob2();
        
        t_getSQLXML1();
        t_getSQLXML2();
        
        t_updateSQLXML1();
        t_updateSQLXML2();
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
    
    public static void main(String args[]) {
        TestConnection tc=null;
        Connection conn_main=null;
        PreparedStatement ps_main=null;
        ResultSet rs_main=null;
        
        try {
            tc = new TestConnection();
            conn_main = tc.createEmbeddedConnection();
        } catch(Exception e) {
            e.printStackTrace();
        }
        
        try {
            ps_main = conn_main.prepareStatement("select count(*) from sys.systables");
            rs_main = ps_main.executeQuery();
        } catch(SQLException e) {
            System.out.println(""+e);
            e.printStackTrace();
        }
        
        TestResultSetMethods trsm = new TestResultSetMethods();
        trsm.startTestResultSetMethods(conn_main,ps_main,rs_main);
        
        /****************************************************************************************
         * This tests the client server part of derby
         *****************************************************************************************/
        
        conn_main=null;
        ps_main=null;
        
        try {
            conn_main = tc.createClientConnection();
        } catch(Exception e) {
            e.printStackTrace();
        }
        
        try {
            ps_main = conn_main.prepareStatement("select count(*) from sys.systables");
            rs_main = ps_main.executeQuery();
        } catch(SQLException e) {
            System.out.println(""+e);
            e.printStackTrace();
        }
        
        trsm.startTestResultSetMethods(conn_main,ps_main,rs_main);
        
    }
}
