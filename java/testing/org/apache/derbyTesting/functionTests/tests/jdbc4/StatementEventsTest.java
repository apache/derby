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
    //used to test StatementEvents raised from PooledConnection
    PooledConnection pooledConnection;
    //used to test StatementEvents raised from XAConnection
    XAConnection xaconnection;
    
    Connection conn;
    PreparedStatement ps_close;
    PreparedStatement ps_error;
    boolean statementCloseEventOccurred=false;
    boolean statementErrorEventOccurred=false;
    
    //In the case of the client driver when the connection is closed then 
    //the prepared statements associated with the connection are also closed
    //this would raise closed events for corresponding prepared statements
    
    //using a flag to identify the occurrence of the error event in the
    //network client which can also cause the close event to be raised
    boolean client_ErrorEvent=false;
    
    
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
        XADataSource xadatasource = getXADataSource();
        ConnectionPoolDataSource cpds = getConnectionPoolDataSource();
        
        pooledConnection = cpds.getPooledConnection();
        xaconnection = xadatasource.getXAConnection();
        //register this class as a event listener for the
        //statement events
        //registering as a listener for the 
        //PooledConnection object
        pooledConnection.addStatementEventListener(this);
        //registering as a listener for the 
        //XAConnection
        xaconnection.addStatementEventListener(this);
    }
    
    /*
        The method closes a created Prepared Statement
        to raise a closed event  
    */
    void raiseCloseEvent() {
        try {
            ps_close = conn.prepareStatement("values 1");
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
            //mark the falg to indicate that we are raising a error event 
            //on the client framework
            if(usingDerbyNetClient())            
                client_ErrorEvent = true;
            
            ps_error = conn.prepareStatement("values 1");
            
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
        //If the event was caused by ps_close and not
        //by ps_error. In this case client_ErrorEvent
        //will be false.
        //In this case check if the StatementEvent
        //has a proper reference to ps_close
        //which is the actual prepared statement
        //that caused the error event.
        if(!client_ErrorEvent && (ps_close==null || 
            !event.getStatement().equals(ps_close))) {
            System.out.println("The statement event has the wrong reference " +
                "of PreparedStatement");
        }
        
        //If it is caused by the error occurred event on the 
        //Network client side. upon doing a Connection.close()
        //the Prepared Statements associated with the 
        //Connection are automatically marked closed
        //check if the StatementEvent has a proper reference to ps_error
        if(client_ErrorEvent && (ps_error==null || 
            !event.getStatement().equals(ps_error))) {
            System.out.println("The statement event has the wrong reference" +
                " of PreparedStatement");
        }
    }

    public void statementErrorOccurred(StatementEvent event) {
        statementErrorEventOccurred = true;
        if(ps_error==null || !event.getStatement().equals(ps_error)) {
            System.out.println("The statement event has the wrong reference +  of PreparedStatement");
        }
    }
    
    /**
     *
     * Check to see if the events were properly raised during execution.
     * raise the close and the error event for the PooledConnection and
     * check if they occur properly.
     * 
     * @throws java.sql.SQLException 
     *
     */
    public void testIfEventOccurredInPooledConnection() throws SQLException {
        //Get a connection from the PooledConnection object
        conn = pooledConnection.getConnection();
        raiseCloseEvent();
        raiseErrorEvent();
        
        //reset the flags to enable it to be used for 
        //both the cases of XAConnection and PooledConnection
        if(statementCloseEventOccurred != true) {
            System.out.println("The Close Event did not occur");
        }
        else {
            statementCloseEventOccurred = false;
        }
            
        if(statementErrorEventOccurred != true) {
            System.out.println("The Error Event did not occur");
        }
        else {
            statementErrorEventOccurred = false;
        }
        
        //close the used prepared statements and connections
        //for the PooledConnection StatementEventListener tests
        //so that if tests on PooledConnection is the first instance of the 
        //tests that are run then we can run the same for
        //XAConnection.
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
    
    /**
     * Check to see if the events were properly raised during execution.
     * Raise the close and the error event for the XAConnection and check if 
     * if they occur properly.
     *
     * @throws java.sql.SQLExeption
     */
    public void testIfEventOccurredInXAConnection() throws SQLException {
        //Get a connection from the XAConnection object
        conn = xaconnection.getConnection();
        raiseCloseEvent();
        raiseErrorEvent();
        
        //reset the flags to enable it to be used for 
        //both the cases of XAConnection and PooledConnection
        if(statementCloseEventOccurred != true) {
            System.out.println("The Close Event did not occur");
        }
        else {
            statementCloseEventOccurred = false;
        }
        
        if(statementErrorEventOccurred != true) {
            System.out.println("The Error Event did not occur");
        }
        else {
            statementErrorEventOccurred = false;
        }
        
        //close the used prepared statements and connections
        //for the XAConnection StatementEventListener tests
        //so that if tests on XAConnection is the first instance of the 
        //tests that are run then we can run the same for
        //PooledConnection.
        if(ps_close != null) {
            ps_close.close();
        }
        if(ps_error != null) {
            ps_error.close();
        }
        if(conn != null && !conn.isClosed()) {
            conn.rollback();
            conn.close();
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
