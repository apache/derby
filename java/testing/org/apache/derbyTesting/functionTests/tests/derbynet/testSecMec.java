/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.testSecMec

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

package org.apache.derbyTesting.functionTests.tests.derbynet;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.DriverManager;
import javax.sql.DataSource;

import org.apache.derby.tools.JDBCDisplayUtil;
import org.apache.derby.tools.ij;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derbyTesting.functionTests.util.TestUtil;
import java.io.*;
import java.net.InetAddress;
import java.util.Hashtable;
import java.util.Properties;

import java.lang.reflect.*;

/**
 * This class tests the security mechanisms supported by Network Server
 * Network server supports SECMEC_EUSRIDPWD, SECMEC_USRIDPWD, SECMEC_USRIDONL
 * Note  - currently the SECMEC_EUSRIDPWD does not work with all versions of ibm142
 * because of the following reason
 * The DiffieHelman algorithm that is used here uses a prime of 32bytes and this is not 
 * supported by Sun JCE , but is supported in ibm141 and some latest versions of ibm142
 *
 */
public class testSecMec extends dataSourcePermissions_net

{

    // Need this to keep track of database has been created or not
    // to avoid case of DERBY-300
    private static boolean dbNotCreated = true;

    // values for derby.drda.securityMechanism property
    private static String[] derby_drda_securityMechanism = { null, //not set
            "USER_ONLY_SECURITY", "CLEAR_TEXT_PASSWORD_SECURITY",
            "ENCRYPTED_USER_AND_PASSWORD_SECURITY", "INVALID_VALUE", "" };

	private static int NETWORKSERVER_PORT;

	private static NetworkServerControl networkServer = null;
    
    private testSecMec(SwitchablePrintStream consoleLogStream,
		       PrintStream originalStream,
		       FileOutputStream shutdownLogStream,
		       SwitchablePrintStream consoleErrLogStream, 
		       PrintStream originalErrStream,
		       FileOutputStream shutdownErrLogStream){
	
	super(consoleLogStream,
	      originalStream,
	      shutdownLogStream,
	      consoleErrLogStream,
	      originalErrStream,
	      shutdownErrLogStream);
	
    }

	public static void main(String[] args) throws Exception {

		// Load harness properties.
		ij.getPropertyArg(args);
		
		String hostName = TestUtil.getHostName();
		if (hostName.equals("localhost"))
			NETWORKSERVER_PORT = 20000;
		else
			NETWORKSERVER_PORT = 1527;

		// "runTest()" is going to try to connect to the database through
		// the server at port NETWORKSERVER_PORT.  Thus, we have to
		// start the server on that port before calling runTest.

		try {
			TestUtil.loadDriver();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		PrintStream originalStream = System.out;
		FileOutputStream shutdownLogStream = 
		    new FileOutputStream("testSecMec." + 
					 System.getProperty("framework","") + "." + 
					 "shutdown.std.log");
		SwitchablePrintStream consoleLogStream = 
		    new SwitchablePrintStream( originalStream );
		
		PrintStream originalErrStream = System.err;
		FileOutputStream shutdownErrLogStream = 
		    new FileOutputStream("testSecMec." + 
					 System.getProperty("framework","") + "." + 
					 "shutdown.err.log");
		SwitchablePrintStream consoleErrLogStream = 
		    new SwitchablePrintStream( originalErrStream );

		System.setOut( consoleLogStream );
		System.setErr( consoleErrLogStream );

           
        // Start server with a specific value for derby.drda.securityMechanism
        // and run tests. Note connections will be successful or not depending on
        // derby.drda.securityMechanism property specified on the server (DERBY-928)
        // @see
        // org.apache.derby.iapi.reference.Property#DRDA_PROP_SECURITYMECHANISM
		for ( int i = 0; i < derby_drda_securityMechanism.length; i++)
		{
		    if (derby_drda_securityMechanism[i]!=null)
		        System.setProperty("derby.drda.securityMechanism",derby_drda_securityMechanism[i]);
		    
		    System.out.println("----------------------------------------------");
		    System.out.println("Testing with derby.drda.securityMechanism="+
		            System.getProperty("derby.drda.securityMechanism"));
		    // Start the NetworkServer on another thread, unless it's a remote host
		    if (hostName.equals("localhost"))
		    {
		        try
		        {
		            networkServer = new NetworkServerControl(InetAddress.getByName(hostName),NETWORKSERVER_PORT);
		            networkServer.start(null);
		        }catch(Exception e)
		        {
		            if ( derby_drda_securityMechanism[i].equals("INVALID_VALUE")||
		                    derby_drda_securityMechanism[i].equals("")) 
		            {
		                System.out.println("EXPECTED EXCEPTION "+ e.getMessage());
		                continue;
		            }
		        }
		        
		        // Wait for the NetworkServer to start.
		        if (!isServerStarted(networkServer, 60))
		            System.exit(-1);
		    }
		    
		    // Now, go ahead and run the test.
		    try {
		        testSecMec tester = 
		            new testSecMec(consoleLogStream,
		                    originalStream,
		                    shutdownLogStream,
		                    consoleErrLogStream,
		                    originalErrStream,
		                    shutdownErrLogStream);
                // Now run the test, note connections will be successful or 
                // throw an exception depending on derby.drda.securityMechanism 
                // property specified on the server
		        tester.runTest();
		        
		    } catch (Exception e) {
		        // if we catch an exception of some sort, we need to make sure to
		        // close our streams before returning; otherwise, we can get
		        // hangs in the harness.  SO, catching all exceptions here keeps
		        // us from exiting before closing the necessary streams.
		        System.out.println("FAIL - Exiting due to unexpected error: " +
		                e.getMessage());
		        e.printStackTrace();
		    }
		    
		    // Shutdown the server.
		    if (hostName.equals("localhost"))
		    {
		        consoleLogStream.switchOutput( shutdownLogStream );
		        consoleErrLogStream.switchOutput( shutdownErrLogStream );
		        
		        networkServer.shutdown();
		        consoleLogStream.flush();
		        // how do we do this with the new api?
		        //networkServer.join();
		        Thread.sleep(5000);
		        
		        consoleLogStream.switchOutput( originalStream );
		        consoleErrLogStream.switchOutput( originalErrStream );
		        
		    }
		}
		System.out.println("Completed testSecMec");

		originalStream.close();
		shutdownLogStream.close();

		originalErrStream.close();
		shutdownErrLogStream.close();

	}


	  // Indicates userid/encrypted password security mechanism.
	  static final short SECMEC_EUSRIDPWD = 0x09;

	  // Indicates userid only security mechanism.
	  static final short SECMEC_USRIDONL = 0x04;

	  // Indicates userid/encrypted password security mechanism.
	  static final short SECMEC_USRENCPWD = 0x07;

	  // Indicates userid/new password security mechanism.
	  static final short SECMEC_USRIDNWPWD = 0x05;

	  // Indicates userid/password security mechanism.
	  static final short SECMEC_USRIDPWD = 0x03;


	/**
	 *  Test cases for security mechanism
	 *  ---------------------------------------------------------------
	 *  T1 - default , no user			PASS (for derbyclient)
	 *  T2 - user only					PASS (for derbyclient)
	 *	T3 - user,password			PASS (only for derbynet)
	 *  T4 - user,password, security mechanism not set  FAIL
	 *  T5 - user,password, security mechanism set to SECMEC_USRIDPWD    PASS
	 *  T6 - user, security mechanism set to SECMEC_USRIDONL   PASS
	 *  T7 - user,password, security mechanism set to SECMEC_USRENCPWD   FAIL
	 *  Test with datasource as well as DriverManager
	 *  T8 - user,password security mechanism set to SECMEC_USRIDONL   PASS
	 *  Test with datasource as well as DriverManager
     * Note, that with DERBY928, the pass/fail for the connections 
     * will depend on the security mechanism specified at the server by property
     * derby.drda.securityMechanism.  Please check out the following html file 
     * http://issues.apache.org/jira/secure/attachment/12322971/Derby928_Table_SecurityMechanisms..htm
     * for a combination of url/security mechanisms and the expected results 
	 */

	protected void runTest()
	{
		// Test cases with get connection via drivermanager and using
		// different security mechanisms.
		// Network server supports SECMEC_USRIDPWD, SECMEC_USRIDONL,SECMEC_EUSRIDPWD
		System.out.println("Checking security mechanism authentication with DriverManager");
        
        // DERBY-300; Creation of SQLWarning on a getConnection causes hang on 
        // 131 vms when server and client are in same vm.
        // To avoid hitting this case with 1.3.1 vms, dont try to send create=true
        // if database is already created as otherwise it will lead to a SQLWarning
        if ( dbNotCreated )
        {
            getConnectionUsingDriverManager(getJDBCUrl("wombat;create=true","user=neelima;password=lee;securityMechanism="+SECMEC_USRIDPWD),"T4:");
            dbNotCreated = false;
        }
        else
            getConnectionUsingDriverManager(getJDBCUrl("wombat","user=neelima;password=lee;securityMechanism="+SECMEC_USRIDPWD),"T4:");
		getConnectionUsingDriverManager(getJDBCUrl("wombat",null),"T1:");
		getConnectionUsingDriverManager(getJDBCUrl("wombat","user=max"),"T2:");
		getConnectionUsingDriverManager(getJDBCUrl("wombat","user=neelima;password=lee"),"T3:");

        // Please note: EUSRIDPWD security mechanism in DRDA uses Diffie-Helman for generation of shared keys.
        // The spec specifies the prime to use for DH which is 32 bytes and this needs to be used as is.
        // Sun JCE does not support a prime of 32 bytes for Diffie Helman and some 
        // older versions of IBM JCE ( 1.4.2) also do not support it.
        // Hence the following call to get connection might not be successful when 
        // client is running in JVM  where the JCE does not support the DH (32 byte prime)
		getConnectionUsingDriverManager(getJDBCUrl("wombat","user=neelima;password=lee;securityMechanism="+SECMEC_EUSRIDPWD),"T5:");
		getConnectionUsingDriverManager(getJDBCUrl("wombat","user=neelima;securityMechanism="+SECMEC_USRIDONL),"T6:");
                
                // disable as ibm142 and sun jce doesnt support DH prime of 32 bytes
		//getConnectionUsingDriverManager(getJDBCUrl("wombat","user=neelima;password=lee;securityMechanism="+SECMEC_USRENCPWD),"T7:");
		getConnectionUsingDriverManager(getJDBCUrl("wombat","user=neelima;password=lee;securityMechanism="+SECMEC_USRIDONL),"T8:");

		getConnectionUsingDataSource();

	}

        /*
         * Get connection from datasource and also set security mechanism
         */

	public void getConnectionUsingDataSource()
	{
		// bug in jcc, throws error with null password
		//testSecurityMechanism("sarah",null,new Short(SECMEC_USRIDONL),"SECMEC_USRIDONL:");
		testSecurityMechanism("john","sarah",new Short(SECMEC_USRIDPWD),"SECMEC_USRIDPWD:");
                
        // Possible bug in JCC, hence disable this test for JCC framework only
        // the security mechanism when set on JCC datasource does not seem to 
        // have an effect. JCC driver is sending a secmec of 3( USRIDPWD) to 
        // the server even though the security mechanism on datasource is set to 
        // EUSRIDPWD (9)
        if (!TestUtil.isJCCFramework())
        {
            // Please note: EUSRIDPWD security mechanism in DRDA uses Diffie-Helman for generation of shared keys.
            // The spec specifies the prime to use for DH which is 32 bytes and this needs to be used as is.
            // Sun JCE does not support a prime of 32 bytes for Diffie Helman and some 
            // older versions of IBM JCE ( 1.4.2)  also do not support it.
            // Hence the following call to get connection might not be successful when 
            // client is running in JVM  where the JCE does not support the DH (32 byte prime)
            testSecurityMechanism("john","sarah",new Short(SECMEC_EUSRIDPWD),"SECMEC_EUSRIDPWD:");
        }
	}

	public void testSecurityMechanism(String user, String password,Short secmec,String msg)
	{
		Connection conn;
		String securityMechanismProperty = "SecurityMechanism";
		Class[] argType = { Short.TYPE };
		String methodName = TestUtil.getSetterName(securityMechanismProperty);
		Object[] args = new Short[1];
		args[0] = secmec;

		try {
			DataSource ds = getDS("wombat", user,password);
			Method sh = ds.getClass().getMethod(methodName, argType);
			sh.invoke(ds, args);
			conn = ds.getConnection();
			conn.close();
			System.out.println(msg +" OK");
		}
		catch (SQLException sqle)
		{
            // Exceptions expected in certain cases depending on JCE used for 
            // running the test. hence printing message instead of stack traces
            // here.
            System.out.println(msg +"EXCEPTION testSecurityMechanism()  " + sqle.getMessage());
            dumpSQLException(sqle.getNextException());
		}
        catch (Exception e)
        {
            System.out.println("UNEXPECTED EXCEPTION!!!" +msg);
            e.printStackTrace();
        }
        
	}

	public void getConnectionUsingDriverManager(String dbUrl, String msg)
	{

		try
		{
			DriverManager.getConnection(dbUrl);
			System.out.println(msg +" "+dbUrl );
		}
		catch(SQLException sqle)
		{
            // Ideally - we would print stack trace of nested SQLException for
            // any unexpected exception.
            // But in this testcase, one test can give an exception in one JCE
            // implementation and in some JCE's the test can pass. 
            // Hence printing the messages instead of stack traces.
			System.out.println(msg +" "+dbUrl +" - EXCEPTION "+ sqle.getMessage());
            dumpSQLException(sqle.getNextException());
		}
	}


    /**
     * Dump SQLState and message for the complete nested chain of SQLException 
     * @param sqle SQLException whose complete chain of exceptions is traversed and sqlstate and 
     * message is printed out
     */
    public static void dumpSQLException(SQLException sqle)
    {
        while ( sqle != null)
        {
            System.out.println("SQLSTATE("+sqle.getSQLState()+"): " + sqle.getMessage());
            sqle = sqle.getNextException();
        }
    }

}
