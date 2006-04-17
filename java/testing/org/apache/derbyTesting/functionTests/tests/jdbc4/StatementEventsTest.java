/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.StatementEventsTest
 
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

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import java.sql.*;
import javax.sql.*;
import junit.framework.*;

import org.apache.derbyTesting.functionTests.util.BaseJDBCTestCase;

/*
    This class is used to test the JDBC4 statement event 
    support 
*/
public class StatementEventsTest extends BaseJDBCTestCase 
        implements StatementEventListener {
    
    PooledConnection pooledConnection;
    Connection conn;
    PreparedStatement ps_close;
    PreparedStatement ps_error;
    boolean statementCloseEventOccurred=false;
    boolean statementErrorEventOccurred=false;
    
    
    /**
     * Create a test with the given name.
     *
     * @param name name of the test.
     */
    public StatementEventsTest(String name) {
        super(name);
    }
    
    /**
     *
     * get a connection object from which the PreparedStatement objects
     * that will be used to raise the events will be created
     *
     */
    public void setUp() throws SQLException {
        ConnectionPoolDataSource cpds = getConnectionPoolDataSource();
        pooledConnection = cpds.getPooledConnection();
        //register this class as a event listener for the
        //statement events
        pooledConnection.addStatementEventListener(this);
        
        //Get a connection from the PooledConnection object
        conn = pooledConnection.getConnection();
    }
    
    /**
     *
     * Close the PooledConnection object and the connection and the 
     * statements obtained from it.
     * 
     */
    
    public void tearDown() throws SQLException {
        if(ps_close != null && !ps_close.isClosed()) {
            ps_close.close();
        }
        if(ps_error != null && !ps_error.isClosed()) {
            ps_error.close();
        }
        if(conn != null && !conn.isClosed()) {
            conn.rollback();
            conn.close();
        }
        if(pooledConnection != null)
            pooledConnection.close();
    }
    
    /*
        The method closes a created Prepared Statement
        to raise a closed event  
    */
    void raiseCloseEvent() {
        try {
            ps_close = conn.prepareStatement("create table temp(n int)");

            //call the close method on this prepared statement object
            //this should result in a statement event being generated 
            //control is transferred to the sattementCLosed function
            ps_close.close();
        } catch(SQLException e) {
            e.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /*
        This method closes a connection and then tries to close the prepared 
        statement associated with the connection causing an error
        event
    */
    void raiseErrorEvent() {
        try {
            ps_error = conn.prepareStatement("create table temp(n int)");
            
            //close the connection associated with this prepared statement
            conn.close();
            //Now execute the prepared statement this should cause an error
            ps_error.execute();
            
        } catch(SQLException e) {
            /*  
                Throw an exception only if the exception does not have a
                state of 08003 which is the state of the SqlException
                got when the connection associated with the PreparedStatement
                is closed before doing a execute on the PreparedStatement 
            */
            if(!(e.getSQLState().compareTo("08003") == 0)) {
                e.printStackTrace();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /*
        implementations of methods in interface 
        javax.sql.StatementEventListener
    */
    
    public void statementClosed(StatementEvent event) {
        statementCloseEventOccurred = true;
        if(ps_close==null || !event.getStatement().equals(ps_close)) {
            System.out.println("The statement event has the wrong reference +  of PreparedStatement");
        }
    }

    public void statementErrorOccurred(StatementEvent event) {
        statementErrorEventOccurred = true;
        if(ps_error==null || !event.getStatement().equals(ps_error)) {
            System.out.println("The statement event has the wrong reference +  of PreparedStatement");
        }
    }
    
    /*
        Check to see if the events were properly raised during execution
     */
    public void testIfEventOccurred() {
        raiseCloseEvent();
        raiseErrorEvent();
        if(statementCloseEventOccurred != true) {
            System.out.println("The Close Event did not occur");
        }
        if(statementErrorEventOccurred != true) {
            System.out.println("The Error Event did not occur");
        }
    }
    
    /**
     * Return suite with all tests of the class.
     */
    public static Test suite() {
        return (new TestSuite(StatementEventsTest.class,
                              "StatementEventsTest suite"));
    }
}
