/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.testSecMec

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

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
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

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
 * and SECMEC_USRSSBPWD.
 * 
 * -----------------------------------------------------------------
 * Security Mechanism | secmec codepoint value | User friendly name
 * -----------------------------------------------------------------
 * USRIDONL           | 0x04                   | USER_ONLY_SECURITY
 * USRIDPWD           | 0x03                   | CLEAR_TEXT_PASSWORD_SECURITY
 * EUSRIDPWD          | 0x09                   | ENCRYPTED_USER_AND_PASSWORD_SECURITY
 * USRSSBPWD          | 0x08                   | STRONG_PASSWORD_SUBSTITUTE_SECURITY
 * -----------------------------------------------------------------
 * 
 * Key points: 
 * 1)Server and client support encrypted userid/password (EUSRIDPWD) via the
 * use of Diffie Helman key-agreement protocol - however current Open Group DRDA
 * specifications imposes small prime and base generator values (256 bits) that
 * prevents other JCE's to be used as java cryptography providers - typical
 * minimum security requirements is usually of 1024 bits (512-bit absolute
 * minimum) when using DH key-agreement protocol to generate a session key.
 * 
 * (Reference: DDM manual, page 281 and 282. Section: Generating the shared
 * private key. DRDA's diffie helman agreed public values for prime are 256
 * bits. The spec gives the public values for the prime, generator and the size
 * of exponent required for DH . These values must be used as is to generate a
 * shared private key.)
 * 
 * Encryption is done using JCE. Hence JCE support of the necessary algorithm is
 * required for a particular security mechanism to work. Thus even though the
 * server and client have code to support EUSRIDPWD, this security mechanism
 * will not work in all JVMs.
 * 
 * JVMs where support for DH(32byte prime) is not available and thus EUSRIDPWD
 * wont work are Sun JVM (versions 1.3.1,1.4.1,1.4.2,1.5) and IBM JVM (versions
 * 1.3.1 and some old versions of 1.4.2 (in 2004) )
 * 
 * JVMs where support for DH(32bytes prime) is available and thus EUSRIDPWD will
 * work are IBM JVM [versions 1.4.1, later versions of 1.4.2 (from 2005), 1.5]
 * 
 * #2) JCC 2.6 client does some automatic upgrade of security mechanism in one
 * case. Logic is  as follows:
 * If client sends USRIDPWD to server and server rejects this
 * and says it accepts only EUSRIDPWD, in that case JCC 2.6 will upgrade the 
 * security mechanism to EUSRIDPWD and retry the request with EUSRIDPWD.
 * This switching will also override the security mechanism specified by user.
 * Thus if JCC client is running with Sun JVM 1.4.2 and even though Sun JCE
 * does not have support for algorithms needed for  EUSRIDPWD, the JCC client
 * will still try to switch to  EUSRIDPWD and throw an exception with 
 * ClassNotFoundException for the IBM JCE.
 *
 * - Default security mechanism is USRIDPWD(0x03)
 * - If securityMechanism is not explicitly specified on connection request 
 *  and if no user specified, an exception is thrown - Null userid not supported
 * - If securityMechanism is not explicitly specified on connection request, 
 * and if no password is specified, an exception is thrown - null password not supported
 * If securityMechanism is explicitly specified to be USRIDONL,  then a password 
 * is not required. But in other cases (EUSRIDPWD, USRIDPWD, USRSSBPWD)  if
 * password is null, an exception with the message 'a null password not valid'
 * will be thrown.
 * - On datasource, setting a security mechanism works. It also allows a security 
 * mechanism of USRIDONL to be set on datasource unlike jcc 2.4.
 * 
 * #3)JCC 2.4 client behavior 
 *  Default security mechanism used is USRIDPWD (0x03)
 *  If securityMechanism is not explicitly specified on connection request,
 *  and if no user is specified, an exception is thrown. - Null userid not supported.
 *  If securityMechanism is not explicitly specified on connection request, 
 *  and if no password is specified, an exception is thrown - null password not supported
 *  If security mechanism is specified, jcc client will not override the security mechanism.
 *  If securityMechanism is explicitly specified to be USRIDONL,  then a password
 *  is not required. But in other cases (EUSRIDPWD,USRIDPWD)  if password is null
 *  - an exception with the message 'a null password not valid' will be thrown.
 * On datasource, setting a security mechanism does not work (bug). It defaults
 * to USRIDPWD.  Setting a value of USRIDONL or EUSRIDPWD does not seem to have
 * an effect.
 * 
 * #4) Note, if  server restricts the client connections based on security mechanism 
 * by setting derby.drda.securityMechanism, in that case the clients will see an 
 * error similar to this
 * "Connection authorization failure occurred. Reason: security mechanism not supported"
 *
 * #5) USRSSBPWD - Strong password substitute is only supported starting from
 *     Apache Derby 10.2.
 *	 NOTE: USRSSBPWD only works with the derby network client driver for now.
 *   ---- 
 */
public class testSecMec extends dataSourcePermissions_net
{
    // Need this to keep track of database has been created or not
    // to avoid case of DERBY-300
    private static boolean dbNotCreated = true;

    // values for derby.drda.securityMechanism property
    private static String[] derby_drda_securityMechanism = { null, //not set
            "USER_ONLY_SECURITY", "CLEAR_TEXT_PASSWORD_SECURITY",
            "ENCRYPTED_USER_AND_PASSWORD_SECURITY",
            "STRONG_PASSWORD_SUBSTITUTE_SECURITY", "INVALID_VALUE", "" };

    // possible interesting combinations with respect to security mechanism
    // upgrade logic for user attribute
    private static String[] USER_ATTRIBUTE = {"calvin",null};
 
    // possible interesting combinations with respect to security mechanism
    // upgrade logic for password attribute
    private static String[] PWD_ATTRIBUTE = {"hobbes",null};

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
		        if (!isServerStarted(networkServer, 60)) {
                    System.out.println("FAIL: Server failed to respond to ping - ending test");
                    break;
                }
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

            // Now we want to test 
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

	// Indicates strong password substitute security mechanism.
	static final short SECMEC_USRSSBPWD = 0x08;
      
	// client and server recognize these secmec values
	private static short[] SECMEC_ATTRIBUTE = {
                SECMEC_USRIDONL,
	            SECMEC_USRIDPWD,
	            SECMEC_EUSRIDPWD,
                SECMEC_USRSSBPWD
	};

	/**
	 *  Test cases for security mechanism
	 *  ---------------------------------------------------------------
	 *  T1 - default , no user			PASS (for derbyclient)
	 *  T2 - user only				PASS (for derbyclient)
	 *  T3 - user,password				PASS (only for derbynet)
	 *  T4 - user,password, security mechanism not set  FAIL
	 *  T5 - user,password, security mechanism set to SECMEC_EUSRIDPWD  PASS/FAIL
     *       (Fails with Sun JVM as EUSRIDPWD secmec cannot be used)
	 *  T6 - user, security mechanism set to SECMEC_USRIDONL   PASS
	 *  T7 - user,password, security mechanism set to SECMEC_USRENCPWD  FAIL
	 *  Test with datasource as well as DriverManager
	 *  T8 - user,password security mechanism set to SECMEC_USRIDONL   PASS
	 *  T9 - user,password security mechanism set to SECMEC_USRSSBPWD  PASS
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
		// Network server supports SECMEC_USRIDPWD, SECMEC_USRIDONL,
        // SECMEC_EUSRIDPWD and USRSSBPWD (derby network client only)
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
        // Test strong password substitute DRDA security mechanism (only works with DerbyClient driver right now)
		getConnectionUsingDriverManager(getJDBCUrl("wombat","user=neelima;password=lee;securityMechanism="+SECMEC_USRSSBPWD),"T9:");

		getConnectionUsingDataSource();

        // regression test for DERBY-1080
        testDerby1080();

        // test for DERBY-962
        testAllCombinationsOfUserPasswordSecMecInput();

        // test USRSSBPWD (DERBY-528) with Derby BUILTIN authentication scheme
        // both with none and USRSSBPWD specified DRDA SecMec upon
        // starting the network server.
        String serverSecurityMechanism =
            System.getProperty("derby.drda.securityMechanism");

        if ((serverSecurityMechanism == null) ||
            (serverSecurityMechanism.equals(
                        "STRONG_PASSWORD_SUBSTITUTE_SECURITY")))
        {
            testUSRSSBPWD_with_BUILTIN();
        }
	}

    /**
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
            // JCC does not support USRSSBPWD security mechanism
		    testSecurityMechanism("john","sarah",new Short(SECMEC_USRSSBPWD),"SECMEC_USRSSBPWD:");
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
	 * Test different interesting combinations of user,password, security mechanism
	 * for testing security mechanism upgrade logic. This test has been added 
     * as part of DERBY-962. Two things have been fixed in DERBY-962, affects
     * only client behavior.
     *
	 * 1)Upgrade logic should not override security mechanism if it has been 
     * explicitly set in connection request (either via DriverManager or 
     * using DataSource)
     *
     * 2)Upgrade security mechanism to a more secure one , ie preferably 
     * to encrypted userid and password if the JVM in which the client is 
     * running has support for it.
     * 
     * Details are:  
     * If security mechanism is not specified as part of the connection request,
     * then client will do an automatic switching (upgrade) of
     * security mechanism to use. The logic is as follows :
     * if password is available, and if the JVM in which the client is running 
     * supports EUSRIDPWD mechanism, in that case the security mechanism is 
     * upgraded to EUSRIDPWD.
     * if password is available, and if the JVM in which the client is running 
     * does not support EUSRIDPWD mechanism, in that case the client will then
     * default to USRIDPWD.
     * Also see DERBY-962 http://issues.apache.org/jira/browse/DERBY-962
     * <BR>
     * To understand which JVMs support EUSRIDPWD or not, please see class level 
     * comments (#1)
     * <BR>
     * The expected output from this test will depend on the following
     * -- the client behavior (JCC 2.4, JCC2.6 or derby client).For the derby client,
     * the table below represents what security mechanism the client will send 
     * to server. 
     * -- See class level comments (#2,#3) to understand the JCC2.6 and JCC2.4 
     * behavior
     * -- Note: in case of derby client, if no user  is specified, user defaults to APP.
     * -- Will depend on if the server has been started with derby.drda.securityMechanism
     * and to the value it is set to.  See main method to check if server is using the 
     * derby.drda.securityMechanism to restrict client connections based on
     * security mechanism. 
     * 
     TABLE with all different combinations of userid, password,
     security mechanism of derby client that is covered by this testcase if test
     is run against IBM15 and JDK15. 
          
     IBM15 supports eusridpwd, whereas SunJDK15 doesnt support EUSRIDPWD
     
     Security Mechanisms supported by derby server and client
     ====================================================================
     |SecMec     |codepoint value|   User friendly name                  |
     ====================================================================
     |USRIDONL   |   0x04        |   USER_ONLY_SECURITY                  |
     |USRIDPWD   |   0x03        |   CLEAR_TEXT_PASSWORD_SECURITY        |
     |EUSRIDPWD  |   0x09        |   ENCRYPTED_USER_AND_PASSWORD_SECURITY|
     |USRSSBPWD  |   0x08        |   STRONG_PASSWORD_SUBSTITUTE_SECURITY |
     =====================================================================
	 Explanation of columns in table. 
	 
	 a) Connection request specifies a user or not.
	 Note: if no user is specified, client defaults to APP
	 b) Connection request specifies a password or not
	 c) Connection request specifies securityMechanism or not. the valid
	 values are 4(USRIDONL), 3(USRIDPWD), 9(EUSRIDPWD) and 8(USRSSBPWD).
	 d) support eusridpwd means whether this jvm supports encrypted userid/
	 password security mechanism or not.  A value of Y means it supports
	 and N means no.
	 The next three columns specify what the client sends to the server
	 e) Does client send user information 
	 f) Does client send password information
	 g) What security mechanism value (secmec value) is sent to server.
	 
	 SecMec refers to securityMechanism.
	 Y means yes, N means No,  - or blank means not specified.
	 Err stands for error.
	 Err(1) stands for null password not supported
	 Err(2) stands for case when the JCE does not support encrypted userid and
	 password security mechanism. 
	 ----------------------------------------------------------------
	 | url connection      | support   | Client sends to Server      |
	 |User |Pwd    |secmec |eusridpwd  |User   Pwd    SecMec         |
	 |#a   |#b     |#c     |#d         |#e     #f      #g            |
	 |---------------------------------------------------------------|
	 =================================================================
     |SecMec not specified on connection request                    
	 =================================================================
	 |Y    |Y     |-       |Y         |Y        Y       9            |
	 |----------------------------------------------------------------
	 |     |Y     |-       |Y         |Y        Y       9            |
	 -----------------------------------------------------------------
	 |Y    |      |-       |Y         |Y        N       4            |
	 -----------------------------------------------------------------
	 |     |      |-       |Y         |Y        N       4            |
	 =================================================================
     |Y    |Y     |-       |N         |Y        Y       3            |
     |----------------------------------------------------------------
     |     |Y     |-       |N         |Y        Y       3            |
     -----------------------------------------------------------------
     |Y    |      |-       |N         |Y        N       4            |
     -----------------------------------------------------------------
     |     |      |-       |N         |Y        N       4            |
     =================================================================
	 SecMec specified to 3 (clear text userid and password)
	 =================================================================
     |Y    |Y     |3       |Y         |Y        Y       3            |
     |----------------------------------------------------------------
     |     |Y     |3       |Y         |Y        Y       3            |
     -----------------------------------------------------------------
     |Y    |      |3       |Y         |-        -       Err1         |
     -----------------------------------------------------------------
     |     |      |3       |Y         |-        -       Err1         |
     =================================================================
     |Y    |Y     |3       |N         |Y        Y       3            |
     |----------------------------------------------------------------
     |     |Y     |3       |N         |Y        Y       3            |
     -----------------------------------------------------------------
     |Y    |      |3       |N         |-        -       Err1         |
     -----------------------------------------------------------------
     |     |      |3       |N         |-        -       Err1         |
     =================================================================
	 SecMec specified to 9 (encrypted userid/password)
     =================================================================
     |Y    |Y     |9       |Y         |Y        Y       9            |
     |----------------------------------------------------------------
     |     |Y     |9       |Y         |Y        Y       9            |
     -----------------------------------------------------------------
     |Y    |      |9       |Y         | -       -       Err1         |
     -----------------------------------------------------------------
     |     |      |9       |Y         | -       -       Err1         |
     =================================================================
     |Y    |Y     |9       |N         | -       -       Err2         |
     |----------------------------------------------------------------
     |     |Y     |9       |N         | -       -       Err2         |
     -----------------------------------------------------------------
     |Y    |      |9       |N         | -       -       Err1         |
     -----------------------------------------------------------------
     |     |      |9       |N         | -       -       Err1         |
     =================================================================
	 SecMec specified to 4 (userid only security)
     =================================================================
     |Y    |Y     |4       |Y         |Y        N       4            |
     |----------------------------------------------------------------
     |     |Y     |4       |Y         |Y        N       4            |
     -----------------------------------------------------------------
     |Y    |      |4       |Y         |Y        N       4            |
     -----------------------------------------------------------------
     |     |      |4       |Y         |Y        N       4            |
     =================================================================
     |Y    |Y     |4       |N         |Y        N       4            |
     |----------------------------------------------------------------
     |     |Y     |4       |N         |Y        N       4            |
     -----------------------------------------------------------------
     |Y    |      |4       |N         |Y        N       4            |
     -----------------------------------------------------------------
     |     |      |4       |N         |Y        N       4            |
     =================================================================
	 SecMec specified to 8 (strong password substitute)
     =================================================================
     |Y    |Y     |8       |Y         |Y        Y       8            |
     |----------------------------------------------------------------
     |     |Y     |8       |Y         |Y        Y       8            |
     -----------------------------------------------------------------
     |Y    |      |8       |Y         | -       -       Err1         |
     -----------------------------------------------------------------
     |     |      |8       |Y         | -       -       Err1         |
     =================================================================
     |Y    |Y     |8       |N         | -       Y       8            |
     |----------------------------------------------------------------
     |     |Y     |8       |N         | -       Y       8            |
     -----------------------------------------------------------------
     |Y    |      |8       |N         | -       -       Err1         |
     -----------------------------------------------------------------
     |     |      |8       |N         | -       -       Err1         |
     ================================================================= 
	 */
    public void testAllCombinationsOfUserPasswordSecMecInput() {
        // Try following combinations:
        // user { null, user attribute given}
        // password {null, pwd specified}
        // securityMechanism attribute specified and not specified.
        // try with different security mechanism values - {encrypted
        // useridpassword, userid only, clear text userid &password)
        String urlAttributes = null;

        System.out.println("******testAllCombinationsOfUserPasswordsSecMecInput***");
        for (int k = 0; k < USER_ATTRIBUTE.length; k++) {
            for (int j = 0; j < PWD_ATTRIBUTE.length; j++) {
                urlAttributes = "";
                if (USER_ATTRIBUTE[k] != null)
                    urlAttributes += "user=" + USER_ATTRIBUTE[k] +";";
                if (PWD_ATTRIBUTE[j] != null)
                    urlAttributes += "password=" + PWD_ATTRIBUTE[j] +";";
               
                // removing the last semicolon that we added here, because 
                // on call to getJDBCUrl in dataSourcePermissions_net, another
                // semicolon will get added for jcc and jcc will not like it.
                if (urlAttributes.length() >= 1)
                    urlAttributes = urlAttributes.substring(0,urlAttributes.length()-1);

                // case - do not specify securityMechanism explicitly in the url
                // get connection via driver manager and datasource.
                getConnectionUsingDriverManager(getJDBCUrl("wombat",
                        urlAttributes), "Test:");
                getDataSourceConnection(USER_ATTRIBUTE[k],PWD_ATTRIBUTE[j],
                        "TEST_DS("+urlAttributes+")");
                
                for (int i = 0; i < SECMEC_ATTRIBUTE.length; i++) {
                    // case - specify securityMechanism attribute in url
                    // get connection using DriverManager
                    getConnectionUsingDriverManager(getJDBCUrl("wombat",  
                            urlAttributes + ";securityMechanism="
                                    + SECMEC_ATTRIBUTE[i]), "#");
                    // case - specify security mechanism on datasource
                    testSecurityMechanism(USER_ATTRIBUTE[k],PWD_ATTRIBUTE[j],
                            new Short(SECMEC_ATTRIBUTE[i]),"TEST_DS ("+urlAttributes+
                            ",securityMechanism="+SECMEC_ATTRIBUTE[i]+")");
                }
            }
        }
    }

    /**
     * Helper method to get connection from datasource and to print
     * the exceptions if any when getting a connection. This method 
     * is used in testAllCombinationsOfUserPasswordSecMecInput.
     * For explanation of exceptions that might arise in this method,
     * please check testAllCombinationsOfUserPasswordSecMecInput
     * javadoc comment.
     * get connection from datasource
     * @param user username
     * @param password password
     * @param msg message to print for testcase
     */
    public void getDataSourceConnection(String user, String password,String msg)
    {
        Connection conn;
        try {
            // get connection via datasource without setting securityMechanism
            DataSource ds = getDS("wombat", user, password);
            conn = ds.getConnection();
            conn.close();
            System.out.println(msg + " OK");
        }
        catch (SQLException sqle)
        {
            // Exceptions expected in certain case hence printing message
            // instead of stack traces here. 
            // - For cases when userid is null or password is null and
            // by default JCC does not allow a null password or null userid.
            // - For case when JVM does not support EUSRIDPWD and JCC 2.6 tries
            // to do autoswitching of security mechanism.
            // - For case if server doesnt accept connection with this security
            // mechanism
            // - For case when client driver does support USRSSBPWD security
            // mechanism
            System.out.println(msg + "EXCEPTION getDataSourceConnection()  " + sqle.getMessage());
            dumpSQLException(sqle.getNextException());
        }
        catch (Exception e)
        {
            System.out.println("UNEXPECTED EXCEPTION!!!" + msg);
            e.printStackTrace();
        }
    }

    /**
     * Dump SQLState and message for the complete nested chain of SQLException 
     * @param sqle SQLException whose complete chain of exceptions is
     * traversed and sqlstate and message is printed out
     */
    public static void dumpSQLException(SQLException sqle)
    {
        while ( sqle != null)
        {
            System.out.println("SQLSTATE("+sqle.getSQLState()+"): " + sqle.getMessage());
            sqle = sqle.getNextException();
        }
    }

    /**
     * Test a deferred connection reset. When connection pooling is done
     * and connection is reset, the client sends EXCSAT,ACCSEC and followed
     * by SECCHK and ACCRDB. Test if the security mechanism related information
     * is correctly reset or not. This method was added to help simulate regression 
     * test for DERBY-1080. It is called from testDerby1080   
     * @param user username 
     * @param password password for connection
     * @param secmec security mechanism for datasource
     * @throws Exception
     */
    public void testSecMecWithConnPooling(String user, String password,
            Short secmec) throws Exception
    {
        System.out.println("withConnectionPooling");
        Connection conn;
        String securityMechanismProperty = "SecurityMechanism";
        Class[] argType = { Short.TYPE };
        String methodName = TestUtil.getSetterName(securityMechanismProperty);
        Object[] args = new Short[1];
        args[0] = secmec;
        
        ConnectionPoolDataSource cpds = getCPDS("wombat", user,password);
        
        // call setSecurityMechanism with secmec.
        Method sh = cpds.getClass().getMethod(methodName, argType);
        sh.invoke(cpds, args);
        
        // simulate case when connection will be re-used by getting 
        // a connection, closing it and then the next call to
        // getConnection will re-use the previous connection.  
        PooledConnection pc = cpds.getPooledConnection();
        conn = pc.getConnection();
        conn.close();
        conn = pc.getConnection();
        test(conn);
        conn.close();
        System.out.println("OK");
    }

    /**
     * Test a connection by executing a sample query
     * @param   conn    database connection
     * @throws Exception if there is any error
     */
    public void test(Connection conn)
        throws Exception
    {

      Statement stmt = null;
      ResultSet rs = null;
      try
      {
        // To test our connection, we will try to do a select from the system catalog tables
        stmt = conn.createStatement();
        rs = stmt.executeQuery("select count(*) from sys.systables");
        while(rs.next())
            System.out.println(" query ok ");

      }
      catch(SQLException sqle)
      {
          System.out.println("SQLException when querying on the database connection; "+ sqle);
          throw sqle;
      }
      finally
      {
          if(rs != null)
            rs.close();
          if(stmt != null)
            stmt.close();
      }
    }
    
    /**
     * This is a regression test for DERBY-1080 - where some variables required
     * only for the EUSRIDPWD security mechanism case were not getting reset on
     * connection re-use and resulting in protocol error. This also applies to
     * USRSSBPWD security mechanism. 
     * 
     * Read class level comments (#1) to understand what is specified by drda
     * spec for EUSRIDPWD.  
     * <br>
     * Encryption is done using JCE. Hence JCE support of the necessary
     * algorithm is required for EUSRIDPWD security mechanism to work. Thus
     * even though the server and client have code to support EUSRIDPWD, this
     * security mechanism will not work in all JVMs. 
     * 
     * JVMs where support for DH(32byte prime) is not available and thus EUSRIDPWD 
     * wont work are Sun JVM (versions 1.3.1,1.4.1,1.4.2,1.5) and 
     * IBM JVM (versions 1.3.1 and some old versions of 1.4.2 (in 2004) )
     * 
     * Expected behavior for this test:
     * If no regression has occurred, this test should work OK, given the 
     * expected exception in following cases:
     * 1) When EUSRIDPWD is not supported in JVM the test is running, a CNFE
     * with initializing EncryptionManager will happen. This will happen for 
     * Sun JVM (versions 1.3.1,1.4.1,1.4.2,1.5) and 
     * IBM JVM (versions 1.3.1 and some old versions of 1.4.2 (in 2004) )
     * For JCC clients, error message is   
     * "java.lang.ClassNotFoundException is caught when initializing
     * EncryptionManager 'IBMJCE'"
     * For derby client, the error message is 
     * "Security exception encountered, see next exception for details."
     * 2)If server does not accept EUSRIDPWD security mechanism from clients,then
     * error message will be "Connection authorization failure
     * occurred. Reason: security mechanism not supported"
     * Note: #2 can happen if server is started with derby.drda.securityMechanism
     * and thus restricts what security mechanisms the client can connect with.
     * This will happen for the test run when derby.drda.securityMechanism is set and 
     * to some valid value other than ENCRYPTED_USER_AND_PASSWORD_SECURITY.
     * <br>
     * See RunTest where this method is called to test for regression for DERBY-1080.
     * Also see main method to check if server is using the derby.drda.securityMechanism to 
     * restrict client connections based on security mechanism.
     */
    public void testDerby1080()
    {
        try
        {
            System.out.println("Test DERBY-1080");
            // simulate connection re-set using connection pooling on a pooled datasource
            // set security mechanism to use encrypted userid and password.
            testSecMecWithConnPooling("peter","neelima",new Short(SECMEC_EUSRIDPWD));
        }
        catch (SQLException sqle)
        {
            // Exceptions expected in certain case hence printing message instead of stack traces
            // here. 
            // - For cases where the jvm does not support EUSRIDPWD.
            // - For case if server doesnt accept connection with this security mechanism
            // Please see javadoc comments for this test method for more details of expected
            // exceptions.
            System.out.println("DERBY-1080  EXCEPTION ()  " + sqle.getMessage());
            dumpSQLException(sqle.getNextException());
        }
        catch (Exception e)
        {
            System.out.println("UNEXPECTED EXCEPTION!!!" );
            e.printStackTrace();
        }

    }

    /**
     * Test SECMEC_USRSSBPWD with derby BUILTIN authentication turned ON.
     *
     * We want to test a combination of USRSSBPWD with BUILTIN as password
     * substitute is only supported with NONE or BUILTIN Derby authentication
     * scheme right now (DERBY-528).
     * 
     * @throws Exception if there an unexpected error
     */
    public void testUSRSSBPWD_with_BUILTIN()
    {
        // Turn on Derby BUILTIN authentication and attempt connecting
        // with USRSSBPWD security mechanism.
        System.out.println(
            "Test USRSSBPWD_with_BUILTIN - derby.drda.securityMechanism=" +
            System.getProperty("derby.drda.securityMechanism"));

        try
        {
            System.out.println("Turning ON Derby BUILTIN authentication");
            Connection conn =
                getConnectionWithSecMec("neelima", "lee",
                                        new Short(SECMEC_USRSSBPWD));
            if (conn == null)
                return; // Exception would have been raised

            // Turn on BUILTIN authentication
            CallableStatement cs =
                conn.prepareCall(
                        "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?, ?)");
            
            cs.setString(1, "derby.user.neelima");
            cs.setString(2, "lee");
            cs.execute();
            
            cs.setString(1, "derby.connection.requireAuthentication");
            cs.setString(2, "true");
            cs.execute();

            cs.close();
            cs = null;

            conn.close();

            // Shutdown 'wombat' database for BUILTIN
            // authentication to take effect the next time it is
            // booted - derby.connection.requireAuthentication is a
            // static property.
		    getConnectionUsingDriverManager(getJDBCUrl(
                "wombat","user=neelima;password=lee;shutdown=true;securityMechanism=" +
                SECMEC_USRSSBPWD),"USRSSBPWD (T0):");

            // Now test some connection(s) with SECMEC_USRSSBPWD
            // via DriverManager and Datasource
		    getConnectionUsingDriverManager(getJDBCUrl(
                "wombat","user=neelima;password=lee;securityMechanism=" +
                SECMEC_USRSSBPWD),"USRSSBPWD + BUILTIN (T1):");
		    testSecurityMechanism("neelima","lee",new Short(SECMEC_USRSSBPWD),
                                  "TEST_DS - USRSSBPWD + BUILTIN (T2):");
            // Attempting to connect with some invalid user
		    getConnectionUsingDriverManager(getJDBCUrl(
                "wombat","user=invalid;password=user;securityMechanism=" +
                SECMEC_USRSSBPWD),"USRSSBPWD + BUILTIN (T3):");
		    testSecurityMechanism("invalid","user",new Short(SECMEC_USRSSBPWD),
                                  "TEST_DS - USRSSBPWD + BUILTIN (T4):");

            System.out.println("Turning OFF Derby BUILTIN authentication");
            conn = getConnectionWithSecMec("neelima", "lee",
                                           new Short(SECMEC_USRSSBPWD));

            if (conn == null)
                return; // Exception would have been raised

            // Turn off BUILTIN authentication
            cs = conn.prepareCall(
                "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?, ?)");
            
            cs.setString(1, "derby.connection.requireAuthentication");
            cs.setString(2, "false");
            cs.execute();

            cs.close();
            cs = null;
            conn.close();

            // Shutdown 'wombat' database for BUILTIN authentication
            // to take effect the next time it is booted
		    getConnectionUsingDriverManager(getJDBCUrl(
                "wombat","user=neelima;password=lee;shutdown=true;securityMechanism=" +
                SECMEC_USRSSBPWD),"USRSSBPWD + BUILTIN (T5):");
        } 
        catch (Exception e)
        {
            System.out.println(
                "FAIL: testUSRSSBPWD_with_BUILTIN(). Unexpected Exception " +
                e.getMessage());
            e.printStackTrace();
        }
    }
    
	public Connection getConnectionWithSecMec(String user,
                                              String password,
                                              Short secMec)
	{
		Connection conn = null;
		String securityMechanismProperty = "SecurityMechanism";
		Class[] argType = { Short.TYPE };
		String methodName = TestUtil.getSetterName(securityMechanismProperty);
		Object[] args = new Short[1];
		args[0] = secMec;

		try {
			DataSource ds = getDS("wombat", user, password);
			Method sh = ds.getClass().getMethod(methodName, argType);
			sh.invoke(ds, args);
			conn = ds.getConnection();
		}
		catch (SQLException sqle)
		{
            // Exceptions expected in certain cases depending on JCE used for 
            // running the test. hence printing message instead of stack traces
            // here.
            System.out.println("EXCEPTION getConnectionWithSecMec()  " + sqle.getMessage());
            dumpSQLException(sqle.getNextException());
		}
        catch (Exception e)
        {
            System.out.println(
                    "UNEXPECTED EXCEPTION!!! getConnectionWithSecMec() - " +
                    secMec);
            e.printStackTrace();
        }

        return conn;
	}
}
