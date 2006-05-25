/*

Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.SuicideOfStreaming

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
import java.net.InetAddress;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.Blob;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.derby.drda.NetworkServerControl;
import org.apache.derby.tools.ij;
import org.apache.derby.client.am.SqlCode;
import org.apache.derbyTesting.functionTests.util.TestUtil;
import org.apache.derbyTesting.functionTests.util.BaseJDBCTestCase;

/**
 *
 * This test needs to be build with sanity=true and executed with system property of derby.debug.suicideOfLayerBStreaming=true.
 * In thosee situation, exception will be happen when streaming from server to client as negative test.
 * 
 * See SuicideOfStreaming_app.properties also.
 *
 */
public class SuicideOfStreaming extends BaseJDBCTestCase {
    
    private static NetworkServerControl networkServer = null;
    
    public static void main(String[] args){
	
	try{
	    
	    setSystemProperty("derby.debug.suicideOfLayerBStreaming","true");
	    
	    startServer();
	    
	    createTestTable();
	    testInterruptedReadOfLob();
	    
	    shutdownServer();
	    
	    fail("Streaming was not encountered exception. Suicide of streaming seems to be failed.");

	}catch(Throwable t){
	    examineThrowable(t);
	    
	}
    }
    
    
    private static void createTestTable() 
	throws SQLException,
	       IllegalAccessException,
	       ClassNotFoundException,
	       InstantiationException
    {

	Connection conn = connectServer();
	
	Statement createTableSt = conn.createStatement();
	createTableSt.execute("create table TEST_TABLE( TEST_COL blob( 65536 ))");
	createTableSt.close();
	
	conn.commit();
	conn.close();

    }
    
    
    private static void testInterruptedReadOfLob() 
	throws SQLException,
	       IOException,
	       IllegalAccessException,
	       ClassNotFoundException,
	       InstantiationException
    {
	
	Connection conn = 
	    connectServer();
	
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
    
    
    private static Connection connectServer()
	throws SQLException {
	
	return DriverManager.getConnection(TestUtil.getJdbcUrlPrefix("localhost",
								     1527) + 
					   "wombat;create=true",
					   "testuser",
					   "testpassword");
	
    }

    
    
    private static void examineThrowable( Throwable t ) {
	
	if(t instanceof SQLException){
	    examineSQLException( ( SQLException ) t );
	    
	}else{
	    t.printStackTrace();
	    fail( t.getMessage() );
	    
	}

    }
    

    private static void examineSQLException( SQLException sqlex ) {
	
	if( ( usingDerbyNetClient() && 
	      examineExpectedInDerbyNetClient(sqlex) ) || 
	    ( usingDerbyNet() && 
	      examineExpectedInDerbyNet(sqlex) ) ){
	    
	    return;

	}
	    
	fail(sqlex.getMessage() + "," +
	     "SqlState: " + sqlex.getSQLState() + "," + 
	     "SqlCode: " + sqlex.getErrorCode() );
	
    }


    private static boolean examineExpectedInDerbyNetClient( SQLException sqlex ) {
	return 
	    sqlex.getSQLState().equals("58009") && 
	    sqlex.getErrorCode() == SqlCode.disconnectError.getCode();
    }


    private static boolean examineExpectedInDerbyNet( SQLException sqlex ) {
	return sqlex.getErrorCode() == SqlCode.disconnectError.getCode();
    }
    
    
    //JUnit test support.
    //
    public SuicideOfStreaming(){
	super("testSuicideOfStreaming");
    }
    
    
    public void testSuicideOfStreaming(){
	main(new String[0]);
    }
    
}

