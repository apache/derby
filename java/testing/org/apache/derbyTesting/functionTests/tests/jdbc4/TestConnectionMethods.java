/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.TestConnectionMethods
 
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 
 */

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derby.tools.ij;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derbyTesting.functionTests.util.SQLStateConstants;
import org.apache.derbyTesting.functionTests.util.TestUtil;

/**
 * This class is used to test the implementations of the JDBC 4.0 methods
 * in the Connection interface
 */

public class TestConnectionMethods {
    Connection conn = null;
    
    /**
     * Constructor for an object that is used for running test of the
     * new connection methods defined by JDBC 4.
     */
    public TestConnectionMethods(Connection connIn) {
        conn = connIn;
    }
    
    /**
     * Test the createClob method implementation in the Connection interface 
     * in the Network Client
     */
    void t_createClob_Client() {
        int c;
        Clob clob;
        try {
            Statement s = conn.createStatement();
            s.execute("create table clobtable2(n int,clobcol CLOB)");
            PreparedStatement ps = conn.prepareStatement("insert into clobtable2" +
                    " values(?,?)");
            ps.setInt(1,1000);
            clob = conn.createClob();
            File file = new File("extin/short.txt");
            FileInputStream is = new FileInputStream(file);
            OutputStream os = clob.setAsciiStream(1);
            c = is.read();
            while(c>0) {
                os.write(c);
                c = is.read();
            }
            ps.setClob(2, clob);
            ps.executeUpdate();
        } catch(SQLException e) {
            e.printStackTrace();
        } catch(FileNotFoundException fnfe){
            fnfe.printStackTrace();
        } catch(IOException ioe) {
            ioe.printStackTrace();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * Test the createBlob method implementation in the Connection interface for
     * in the Network Client
     */
    void t_createBlob_Client() {
        int c;
        Blob blob;
        try {
            Statement s = conn.createStatement();
            s.execute("create table blobtable2(n int,blobcol BLOB)");
            PreparedStatement ps = conn.prepareStatement("insert into blobtable2" +
                    " values(?,?)");
            ps.setInt(1,1000);
            blob = conn.createBlob();
            File file = new File("extin/short.txt");
            FileInputStream is = new FileInputStream(file);
            OutputStream os = blob.setBinaryStream(1);
            c = is.read();
            while(c>0) {
                os.write(c);
                c = is.read();
            }
            ps.setBlob(2, blob);
            ps.executeUpdate();
        } catch(SQLException e) {
            e.printStackTrace();
        } catch(FileNotFoundException fnfe){
            fnfe.printStackTrace();
        } catch(IOException ioe) {
            ioe.printStackTrace();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * Test the Connection.isValid method
     */
    void t_isValid() {
        /*
         * Test illegal parameter values
         */
        try {
            conn.isValid(-1);  // Negative timeout
            System.out.println("FAIL: isValid(-1): " + 
                               "Invalid argument execption not thrown");
        } catch (SQLException e) {
            if(!StandardException.getSQLStateFromIdentifier(
                SQLState.INVALID_API_PARAMETER).equals(e.getSQLState())) {
                System.out.println("FAIL: isValid(-1): Unexpected SQLException" +
                                   e);
            }
        }

        /*
         * Test with no timeout
         */
        try {
            if (!conn.isValid(0)) {
                System.out.println("FAIL: isValid(0): returned false");
            }
        } catch(Exception e) {
            System.out.println("FAIL: isValid(0): Unexpected exception: " + e);
        }

        /*
         * Test with a valid timeout
         */
        try {
            if (!conn.isValid(1)) {
                System.out.println("FAIL: isValid(1): returned false");
            }
        } catch(Exception e) {
            System.out.println("FAIL: isValid(1): Unexpected exception: " + e);
        }

        /*
         * Test on a closed connection
         */
        try {
            conn.close();
        } catch (SQLException e) {
            System.out.println("FAIL: close failed: Unexpected exception: " + e);
        }

        try {
            if (conn.isValid(0)) {
                System.out.println("FAIL: isValid(0) on closed connection: " +
                                   "returned true");
            }
        } catch(Exception e) {
            System.out.println("FAIL: isValid(0) on closed connection: " + 
                               "Unexpected exception: " + e);
        }

        /* Open a new connection and test it */
        try {
            conn = ij.startJBMS();
        } catch (Exception e) {
            System.out.println("FAIL: failed to open new connection: " +
                               "Unexpected exception: " + e);
        }

        try {
            if (!conn.isValid(0)) {
                System.out.println("FAIL: isValid(0) on open connection: " + 
                                   "returned false");
            }
        } catch(Exception e) {
            System.out.println("FAIL: isValid(0) on open connection: " + 
                               "Unexpected exception: " + e);
        }

        /*
         * Test on stopped database
         */
        shutdownDatabase();

        /* Test if that connection is not valid */
        try {
            if (conn.isValid(0)) {
                System.out.println("FAIL: isValid(0) on stopped database: " + 
                                   "returned true");
            }
        } catch(Exception e) {
            System.out.println("FAIL: isValid(0) on a stopped database: " + 
                               "Unexpected exception: " + e);
        } 

        /* Start the database by getting a new connection to it */
        try {
            conn = ij.startJBMS();
        } catch (Exception e) {
            System.out.println("FAIL: failed to re-start database: " +
                               "Unexpected exception: " + e);
        }

        /* Check that a new connection to the newly started database is valid */
        try {
            if (!conn.isValid(0)) {
                System.out.println("FAIL: isValid(0) on new connection: " + 
                                   "returned false");
            }
        } catch(Exception e) {
            System.out.println("FAIL: isValid(0) on new connection: " + 
                               "Unexpected exception: " + e);
        }

        /*
         * Test on stopped Network Server client
         */
        if ( !usingEmbeddedClient() ) {
            stopNetworkServer();

            /* Test that the connection is not valid */
            try {
                if (conn.isValid(0)) {
                    System.out.println("FAIL: isValid(0) on stopped database: " +
                                      "returned true");
                }
            } catch(Exception e) {
                System.out.println("FAIL: isValid(0) on a stopped database: " + 
                                   "Unexpected exception: " + e);
            } 

            /*
             * Start the network server and get a new connection and check that
             * the new connection is valid.
             */
            startNetworkServer();

            try {
                // Get a new connection to the database
                conn = ij.startJBMS();
            } catch (Exception e) {
                System.out.println("FAIL: failed to re-start database: " +
                                   "Unexpected exception: " + e);
                e.printStackTrace();
            }

            /* Check that a new connection to the newly started Derby is valid */
            try {
                if (!conn.isValid(0)) {
                    System.out.println("FAIL: isValid(0) on new connection: " + 
                                       "returned false");
                }
            } catch(Exception e) {
                System.out.println("FAIL: isValid(0) on new connection: " + 
                                  "Unexpected exception: " + e);
            }
        }
    }

    public void startTestConnectionMethods_Client() {
        t_createClob_Client();
        t_createBlob_Client();
        t_isValid();
    }
    
    public void startTestConnectionMethods_Embedded() {
        t_isValid();
    }

    /**
     * Shut down the test database
     */
    private void shutdownDatabase() {
        try {
            // Get the name for the database from the test's property file
            String databaseName = System.getProperty("ij.dataSource.databaseName");
            if (databaseName != null) {
                TestUtil.getConnection(databaseName, "shutdown=true");
            }
            else {
                System.out.println("FAIL: shutdownDatabase: " +
                           "property ij.dataSource.databaseName not defined");
            }
	 } catch (Exception e) {
            // Ignore any exeptions from shutdown
	 }
    }


    /**
     * Stop the network server
     */
    private void stopNetworkServer() {
        try {
            NetworkServerControl networkServer = new NetworkServerControl();
            networkServer.shutdown();
        } catch(Exception e) {
            System.out.println("INFO: Network server shutdown returned: " + e);
        }
    }


    /**
     * Start the network server
     */
    private void startNetworkServer() {
        String hostName = null;
        int serverPort;

        // Determines which host and port to run the network server on
        // This is based how it is done in the test testSecMec.java
        String serverName = TestUtil.getHostName();
        if (serverName.equals("localhost")) {
            serverPort = 1527;
        }
        else {
            serverPort = 20000;
        }

        try {
            NetworkServerControl networkServer = 
                     new NetworkServerControl(InetAddress.getByName(serverName), 
                                              serverPort);
            networkServer.start(null);

            // Wait for the network server to start
            boolean started = false;
            int retries = 10;         // Max retries = max seconds to wait
            while (!started && retries > 0) {
                try {
                    // Sleep 1 second and then ping the network server
		      Thread.sleep(1000);
                    networkServer.ping();

                    // If ping does not throw an exception the server has started
                    started = true;
                } catch(Exception e) {
                    System.out.println("INFO: ping returned: " + e);
                    retries--;
	         }
	     }

            // Check if we got a reply on ping
            if (!started) {
                System.out.println("FAIL: Failed to start network server");
            }
        } catch (Exception e) {
            System.out.println("FAIL: startNetworkServer got exception: " + e);
        }
    }


	/**
	 * <p>
	 * Return true if we're running under the embedded client.
	 * </p>
	 */
	private	static	boolean	usingEmbeddedClient()
	{
		return "embedded".equals( System.getProperty( "framework" ) );
	}

	
    public static void main(String args[]) {
		try {
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
		
			Connection	conn_main = ij.startJBMS();

			if ( usingEmbeddedClient() )
			{
				TestConnectionMethods tcm = new TestConnectionMethods( conn_main );
				tcm.startTestConnectionMethods_Embedded();
			}
			else // DerbyNetClient
			{
				TestConnectionMethods tcm1 = new TestConnectionMethods( conn_main );
				tcm1.startTestConnectionMethods_Client();
			}

			conn_main.close();

		} catch(Exception e) {
			System.out.println(""+e);
			e.printStackTrace();
		}
    }
}
