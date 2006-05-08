/*

Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.OutBufferedStream

Copyright 1999, 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.derbynet;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.net.InetAddress;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.Blob;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.sql.SQLException;

import org.apache.derby.drda.NetworkServerControl;
import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.TestUtil;

/**
 *
 * This program tests streaming lob with derby.drda.streamOutBufferSize configuration.
 *
 * When derby.drda.streamOutBufferSize was configured, 
 * a buffer of configured size was placed at network server just before sending stream to client.
 *
 * The configuration was written in OutBufferedStream_app.properties.
 *
 */
public class OutBufferedStream {
    private static NetworkServerControl networkServer = null;
    
    public static void main(String[] args){
	
	try{
	    
	    ij.getPropertyArg(args);
	    
	    
	    SwitchablePrintStream testExecutionOutput = new SwitchablePrintStream(System.out);
	    SwitchablePrintStream testExecutionErrOutput = new SwitchablePrintStream(System.out);
	    
	    OutputStream originalOut = System.out;
	    OutputStream originalErr = System.err;

	    System.setOut(testExecutionOutput);
	    System.setErr(testExecutionErrOutput);
	    
	    startServer();
	    
	    System.out.println("start test.");

	    createTestTable();
	    testReadOfLob();
	    
	    System.out.println("test done.");
	    
	    OutputStream shutdownLogFileStream = null;
	    OutputStream shutdownErrLogFileStream = null;

	    try{
		shutdownLogFileStream = getShutdownLogFileStream();
		shutdownErrLogFileStream = getShutdownErrLogFileStream();
		
		testExecutionOutput.switchOutput(shutdownLogFileStream);
		testExecutionErrOutput.switchOutput(shutdownErrLogFileStream);

		shutdownServer();
		
		testExecutionOutput.switchOutput(originalOut);
		testExecutionErrOutput.switchOutput(originalErr);
		
	    }finally{
		
		if(shutdownLogFileStream != null){
		    shutdownLogFileStream.flush();
		    shutdownLogFileStream.close();
		}

		if(shutdownErrLogFileStream != null){
		    shutdownErrLogFileStream.flush();
		    shutdownErrLogFileStream.close();
		}
	    }   
	    
	}catch(Throwable t){
	    t.printStackTrace();
	    
	}
    }
    
    
    private static void createTestTable() 
	throws SQLException,
	       IllegalAccessException,
	       ClassNotFoundException,
	       InstantiationException
    {

	Connection conn = getConnection();
	
	Statement createTableSt = conn.createStatement();
	createTableSt.execute("create table TEST_TABLE( TEST_COL blob( 65536 ))");
	createTableSt.close();
	
	conn.commit();
	conn.close();

    }
    
    
    private static void testReadOfLob() 
	throws SQLException,
	       IOException,
	       IllegalAccessException,
	       ClassNotFoundException,
	       InstantiationException
    {
	
	Connection conn = 
	    getConnection();
	
	conn.setAutoCommit(false);
	
	PreparedStatement insertLobSt = 
	    conn.prepareStatement("insert into TEST_TABLE( TEST_COL ) values(?)");
	insertLobSt.setBinaryStream(1, 
				    createOriginalDataInputStream( 65536 ), 
				    65536 );
	insertLobSt.executeUpdate();
	insertLobSt.close();

	conn.commit();
	
	PreparedStatement st = conn.prepareStatement("select TEST_COL from TEST_TABLE");
	ResultSet rs = st.executeQuery();
	
	rs.next();
	
	InputStream is = rs.getBinaryStream(1);

	int c;
	while( ( c = is.read() ) > -1 ){
	    
	    System.out.print(c);
	    System.out.print(",");
	    
	    if( ( (c + 1) % 256 ) == 0 )
		System.out.println();

	}
	
	is.close();
	
	rs.close();
	st.close();
	
	conn.commit();
	conn.close();

	System.out.println();
	
    }
    
    
    private static ByteArrayInputStream createOriginalDataInputStream(int length){

	byte[] originalValue = new byte[ length ];

	for(int i = 0; i < originalValue.length; i ++){
	    originalValue[i] = (byte) (i % 256);
	}
	
	return new ByteArrayInputStream(originalValue);

    }
    
    
    protected static boolean isServerStarted(NetworkServerControl server, 
					     int ntries){
	for (int i = 1; i <= ntries; i ++){
	    try {
		Thread.sleep(500);
		server.ping();
		return true;
	    } catch (Exception e) {
		if (i == ntries)
		    return false;
	    }
	}
	return false;
    }

    
    private static void startServer() 
	throws Exception{
	
	try{
	    TestUtil.loadDriver();
	    
	}catch(Exception e){
	    e.printStackTrace();
	}
	
	
	networkServer = 
	    new NetworkServerControl(InetAddress.getByName("localhost"),
				     1527);
	networkServer.start( null );
	    
	if(! isServerStarted( networkServer, 
			      60 ) )
	    System.exit(-1);
	
    }
    
    
    private static void shutdownServer()
	throws Exception{
	
	networkServer.shutdown();
	
    }
    
    
    private static Connection getConnection()
	throws SQLException {
	
	return DriverManager.getConnection(TestUtil.getJdbcUrlPrefix("localhost",
								     1527) + 
					   "wombat;create=true",
					   "testuser",
					   "testpassword");
	
    }
    
    
    private static OutputStream getShutdownLogFileStream() 
	throws FileNotFoundException {
	
	return new FileOutputStream("outBufferedStream." + 
				    System.getProperty("framework","") + "." + 
				    "shutdown.std.log");
    }
    
    
    private static OutputStream getShutdownErrLogFileStream()
	throws FileNotFoundException{
	
	return new FileOutputStream("outBufferedStream." + 
				    System.getProperty("framework","") + "." + 
				    "shutdown.err.log");
    }
    
    
}
