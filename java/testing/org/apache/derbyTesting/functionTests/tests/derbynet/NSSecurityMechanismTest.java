/*

   Derby - Class 
      org.apache.derbyTesting.functionTests.tests.derbynet.NSSecurityMechanismTest

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

import java.lang.reflect.*;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Iterator;

import java.security.AccessController;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.CallableStatement;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.DriverManager;
import javax.sql.DataSource;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

import org.apache.derby.drda.NetworkServerControl;
import org.apache.derby.jdbc.ClientConnectionPoolDataSource40;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.J2EEDataSource;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.NetworkServerTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * This class tests the security mechanisms supported by Network Server
 * Network server supports SECMEC_EUSRIDPWD, SECMEC_USRIDPWD, SECMEC_USRIDONL
 * and SECMEC_USRSSBPWD.
 * 
 * -----------------------------------------------------------------
 * Security Mechanism | secmec           | User friendly name   
 *                    |  codepoint value |
 * -----------------------------------------------------------------
 * USRIDONL           | 0x04             | USER_ONLY_SECURITY
 * USRIDPWD           | 0x03             | CLEAR_TEXT_PASSWORD_SECURITY
 * EUSRIDPWD          | 0x09             | ENCRYPTED_USER_AND_PASSWORD_SECURITY
 * USRSSBPWD          | 0x08             | STRONG_PASSWORD_SUBSTITUTE_SECURITY
 * -----------------------------------------------------------------
 * 
 * Key points: 
 * #1) Server and client support encrypted userid/password (EUSRIDPWD) via the
 * use of Diffie Helman key-agreement protocol - but current Open Group DRDA
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
 * Encryption is done using JCE. Hence JCE support of the necessary algorithm
 * is required for a particular security mechanism to work. Thus even though 
 * the server and client have code to support EUSRIDPWD, this security 
 * mechanism will not work in all JVMs.
 * 
 * JVMs where support for DH(32byte prime) is not available and thus EUSRIDPWD
 * won't work are Sun JVM (versions 1.3.1,1.4.1,1.4.2,1.5) and IBM JVM (
 * versions 1.3.1 and some old versions of 1.4.2 (in 2004))
 * 
 * JVMs where support for DH(32bytes prime) is available and thus EUSRIDPWD 
 * will work are IBM JVM [versions 1.4.1, later versions of 1.4.2 (from 2005),
 * 1.5]
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
 *   and if no user specified, an exception is thrown - Null userid not 
 *   supported
 * - If securityMechanism is not explicitly specified on connection request, 
 *   and if no password is specified, an exception is thrown - null password
 *   not supported
 *   If securityMechanism is explicitly specified to be USRIDONL,  then a
 *   password is not required. But in other cases (EUSRIDPWD, USRIDPWD, 
 *   USRSSBPWD) if password is null, an exception with the message 'a null
 *   password not valid' will be thrown.
 * - On datasource, setting a security mechanism works. It also allows a 
 *   security mechanism of USRIDONL to be set on datasource unlike jcc 2.4.
 * 
 * #3)JCC 2.4 client behavior 
 * Default security mechanism used is USRIDPWD (0x03)
 * If securityMechanism is not explicitly specified on connection request, and
 * if no user is specified, an exception is thrown - Null userid not supported.
 * If securityMechanism is not explicitly specified on connection request, and
 * if no password is specified, an exception is thrown - null password not
 * supported.
 * If security mechanism is specified, jcc client will not override the 
 * security mechanism.
 * If securityMechanism is explicitly specified to be USRIDONL, then a password
 * is not required. But in other cases (EUSRIDPWD,USRIDPWD) if password is null
 * an exception with the message 'a null password not valid' will be thrown.
 * On datasource, setting a security mechanism does not work (bug). It defaults
 * to USRIDPWD.  Setting a value of USRIDONL or EUSRIDPWD does not seem to have
 * an effect.
 * 
 * #4) Note, if  server restricts the client connections based on security 
 * mechanism by setting derby.drda.securityMechanism, in that case the clients 
 * will see an error similar to this:
 * "Connection authorization failure occurred. Reason: security mechanism not
 *  supported"
 *
 * #5) USRSSBPWD - Strong password substitute is only supported starting from
 *     Apache Derby 10.2.
 *	 NOTE: USRSSBPWD only works with the derby network client driver for now.
 *   ---- 
 */
public class NSSecurityMechanismTest extends BaseJDBCTestCase
{
    // values for derby.drda.securityMechanism property
    private static String[] derby_drda_securityMechanisms = {
        null, // not set 
        "USER_ONLY_SECURITY", "CLEAR_TEXT_PASSWORD_SECURITY",
        // this will give a DRDA_InvalidValue with jvms that do not support it
        "ENCRYPTED_USER_AND_PASSWORD_SECURITY",
        "STRONG_PASSWORD_SUBSTITUTE_SECURITY",
        //these two are always invalid values, again, will give DRDA_InvalidValue
        "INVALID_VALUE", ""};
    
    private static String derby_drda_securityMechanism;

    // possible interesting combinations with respect to security mechanism
    // upgrade logic for user attribute
    private static String[] USER_ATTRIBUTE = {"calvin",null};

    // possible interesting combinations with respect to security mechanism
    // upgrade logic for password attribute
    private static String[] PWD_ATTRIBUTE = {"hobbes",null};

    public NSSecurityMechanismTest(String name)
    {
        super(name);
    }
    
    public static Test suite() 
    {
        TestSuite suite = new TestSuite("NSSecurityMechanismTest");

        TestSuite clientSuite =
            new TestSuite("NSSecurityMechanismTest - client");
        clientSuite.addTest(new NSSecurityMechanismTest(
            "testNetworkServerSecurityMechanism"));
        suite.addTest(TestConfiguration.clientServerDecorator(clientSuite));

        // Test case for embedded mode. Enable builtin authentication.
        suite.addTest(
            DatabasePropertyTestSetup.builtinAuthentication(
                new NSSecurityMechanismTest("testSecurityMechanismOnEmbedded"),
                new String[] { "calvin" }, "pw"));

        return suite;
    }
    
    protected void tearDown() throws Exception {
        removeSystemProperty("derby.drda.securityMechanism");

        super.tearDown();
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
     *  DriverManager:
     *  T1 - default , no user      PASS (for derbyclient)
     *  T2 - user only              PASS (for derbyclient)
     *  T3 - user,password          PASS (only for derbynet)
     *  T4 - user,password, security mechanism not set  FAIL
     *  T5 - user,password, security mechanism set
     *       to SECMEC_EUSRIDPWD  PASS/FAIL
     *       (Fails with Sun JVM as EUSRIDPWD secmec cannot be used)
     *  T6 - user, security mechanism set to SECMEC_USRIDONL   PASS
     *  T7 - user,password, security mechanism set to SECMEC_USRENCPWD  FAIL
     *  Test with datasource as well as DriverManager
     *  T8 - user,password security mechanism set to SECMEC_USRIDONL   PASS
     *  T9 - user,password security mechanism set to SECMEC_USRSSBPWD  PASS
     *  Test with datasource as well as DriverManager
     * Note, that with DERBY-928, the pass/fail for the connections will depend 
     * on the security mechanism specified at the server by property 
     * derby.drda.securityMechanism.  Please check out the following
     * html file http://issues.apache.org/jira/secure/attachment/12322971/Derby928_Table_SecurityMechanisms..htm
     * for a combination of url/security mechanisms and the expected results 
     */
    public void testNetworkServerSecurityMechanism() throws SQLException, Exception
    {
        String[][] allDriverManagerExpectedValues = {
            {"null",
                "OK","OK","OK","OK","OK","OK","?","OK","OK"},
            {"USER_ONLY_SECURITY",
                "OK","OK","08004","08004","08004","OK","?","OK","08004"},
            {"CLEAR_TEXT_PASSWORD_SECURITY",
                "08004","08004","OK","OK","08004","08004",
                "?","08004","08004"},
            // this should give a DRDA_InvalidValue with jvms that do not
            // support it. For instance, it will fail with jdk14 jvms.
            {"ENCRYPTED_USER_AND_PASSWORD_SECURITY",
                "08004","08004","08004","08004","OK","08004",
                "?","08004","08004"},
            {"STRONG_PASSWORD_SUBSTITUTE_SECURITY",
                    "08004","08004","08004","08004","08004","08004",
                    "?","08004","OK"},
            };
        
        String[][] allDataSourceExpectedValues = {
            {null,"OK","OK","OK","OK"},
            {"USER_ONLY_SECURITY","OK","08004","08004","08004"},
            {"CLEAR_TEXT_PASSWORD_SECURITY","08004","OK","08004","08004"},
            {"ENCRYPTED_USER_AND_PASSWORD_SECURITY",
                "08004","08004","OK","08004"},
            {"STRONG_PASSWORD_SUBSTITUTE_SECURITY",
                "08004","08004","08004","OK"}};
        
        String[] DERBY1080ExpectedValues = {"OK","08004","08004","OK","08004"};
        
        String[][] allUserPwdSecMecExpectedValues = {
            // if secmec is null, all expected to pass.
            {null},
            {"USER_ONLY_SECURITY",
                "08004","OK","08004","08004","08004", 
                "OK","OK","08001.C.8","08001.C.8","08001.C.8",
                "08004","OK","08004","08004","08004",
                "OK","OK","08001.C.8","08001.C.8","08001.C.8"
            },
            {"CLEAR_TEXT_PASSWORD_SECURITY",
                "OK","08004","OK","08004","08004", 
                "08004","08004","08001.C.8","08001.C.8","08001.C.8",
                "OK","08004","OK","08004","08004",
                "08004","08004","08001.C.8","08001.C.8","08001.C.8"
            },
            {"ENCRYPTED_USER_AND_PASSWORD_SECURITY",
                "08004","08004","08004","OK","08004", 
                "08004","08004","08001.C.8","08001.C.8","08001.C.8",
                "08004","08004","08004","OK","08004",
                "08004","08004","08001.C.8","08001.C.8","08001.C.8"
            },
            {"STRONG_PASSWORD_SUBSTITUTE_SECURITY",
                "08004","08004","08004","08004","OK", 
                "08004","08004","08001.C.8","08001.C.8","08001.C.8",
                "08004","08004","08004","08004","OK",
                "08004","08004","08001.C.8","08001.C.8","08001.C.8"
            }
        };
        
        String[] testDERBY528ExpectedValues = {
            null,"08006","OK","08004","08006"
        };
        
        // just to see if this will work
        getConnection().getAutoCommit();
        
        for ( int i = 0; i < derby_drda_securityMechanisms.length; i++)
        {   
            derby_drda_securityMechanism = derby_drda_securityMechanisms[i];

            // Using 'null' will give a nullpointer exception...
            // as it's the first in the array, it should use default setting
            if (derby_drda_securityMechanism != null)
            {
                // with "" or "INVALID_VALUE", or with other mechanisms with
                // certain jvms, some settings are not supported. Flag the loop
                // to not try connections
                if (setSecurityMechanism(derby_drda_securityMechanism))
                    continue;
            }
            // Test cases with get connection via drivermanager and via
            // datasource, using different security mechanisms.
            // Network server supports SECMEC_USRIDPWD, SECMEC_USRIDONL,
            // SECMEC_EUSRIDPWD and USRSSBPWD (derby network client only)

            assertConnectionsUsingDriverManager(
                allDriverManagerExpectedValues[i]);
            
            assertConnectionUsingDataSource(allDataSourceExpectedValues[i]);

            // regression test for DERBY-1080
            assertDerby1080Fixed(DERBY1080ExpectedValues[i]);

            // test for DERBY-962
            // test all combinations of user/password with security mechanism
            assertAllCombinationsOfUserPasswordSecMecInputOK(
                allUserPwdSecMecExpectedValues[i]);

            // test USRSSBPWD (DERBY-528) with Derby BUILTIN authentication 
            // scheme both with none and USRSSBPWD specified DRDA SecMec upon
            // starting the network server.
            if ((derby_drda_securityMechanism == null) ||
                (derby_drda_securityMechanism.equals(
                "STRONG_PASSWORD_SUBSTITUTE_SECURITY")))
            {
                assertUSRSSBPWD_with_BUILTIN(testDERBY528ExpectedValues);
            }
            else
            {
                // shutdown the database - this will prevent slow startup 
                // when bouncing the server with next security mechanism.
                // for ENCRYPTED_USER_AND_PASSWORD_SECURITY:
                short secmeccode=SECMEC_EUSRIDPWD;
                if (derby_drda_securityMechanism.equals("USER_ONLY_SECURITY"))
                    secmeccode=SECMEC_USRIDONL;
                else if (derby_drda_securityMechanism.equals(
                    "CLEAR_TEXT_PASSWORD_SECURITY"))
                    secmeccode=SECMEC_USRIDPWD;
                assertConnectionUsingDriverManager(getJDBCUrl(
                    "user=APP;password=APP;shutdown=true;securityMechanism=" +
                    secmeccode)," BUILTIN (T5):",
                    "08006");
            }
        }
    }

    /**
     * Test that securityMechanism=8 is ignored by the embedded driver
     * (DERBY-3025).
     */
    public void testSecurityMechanismOnEmbedded() throws SQLException {
        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(
            ds, "connectionAttributes", "securityMechanism=8");

        // DERBY-3025: NullPointerException or AssertFailure was thrown here
        Connection c = ds.getConnection("calvin", "calvinpw");

        c.close();
    }

    // returns a boolean true if the security mechanism is not supported
    // so the loop in which this is called can be continued without
    // causing unnecessary/impossible tests to be run
    private boolean setSecurityMechanism(String derby_security_mechanism) 
    throws Exception {
        try {
            // getting a networkservercontrol to shutdown the currently running
            // server, before setting the next security mechanism
            final TestConfiguration config = TestConfiguration.getCurrent();
            NetworkServerControl server = new NetworkServerControl(
                InetAddress.getByName(config.getHostName()),
                config.getPort(),
                config.getUserName(),
                config.getUserPassword());

            // shut down the server
            server.shutdown();
        } catch (Exception e) {
            if (!(e.getMessage().substring(0,17).equals("DRDA_InvalidValue")))
            {
                fail("unexpected error");
            }
        }

        setSystemProperty("derby.drda.securityMechanism",
                derby_drda_securityMechanism);
        try {
            
            // if the security mechanism isn't supported or invalid, getting a
            // networkservercontrol will fail.
            NetworkServerControl server2 = new NetworkServerControl(
                InetAddress.getByName(
                    TestConfiguration.getCurrent().getHostName()),
                    TestConfiguration.getCurrent().getPort());

            // For debugging, to make output come to console uncomment:
            //server2.start(new PrintWriter(System.out, true));
            // and comment out:
            server2.start(null);
            NetworkServerTestSetup.waitForServerStart(server2);
            
            if (derby_drda_securityMechanism.equals("") ||
                derby_drda_securityMechanism.equals("INVALID_VALUE"))
            {
                fail(
                    "expected server not to start with invalid or empty " +
                     "security mechanism, but passed");
            }
        } catch (Exception e) {
            // "" or "INVALID_VALUE" should always give DRDA_Invalid_Value, 
            // hence the 'fail' above.
            // However, other mechanisms may not be supported with certain
            // jvms, and then also will get the same exception. This is true
            // for ENCRYPTED_USER_AND_PASSWORD_SECURITY.
            // If we're not getting DRDA_Invalid_Value here, something's gone
            // wrong.
            if (derby_drda_securityMechanism.equals("") ||
                derby_drda_securityMechanism.equals("INVALID_VALUE") ||
                derby_drda_securityMechanism.equals(
                    "ENCRYPTED_USER_AND_PASSWORD_SECURITY"))
            {
                // should give invalid value 
                assertEquals("DRDA_InvalidValue",e.getMessage().substring(0,17));
                return true;
            }
            fail ("got unexpected exception setting the mechanism " + 
                derby_security_mechanism + "; message: " + e.getMessage());
        }
        return false;
    }

    private void assertConnectionsUsingDriverManager(String[] expectedValues)
    {
        assertConnectionUsingDriverManager(
            getJDBCUrl(null),"T1:", expectedValues[1]);
        assertConnectionUsingDriverManager(
            getJDBCUrl("user=max"),"T2:", expectedValues[2]);
        assertConnectionUsingDriverManager(
            getJDBCUrl("user=neelima;password=lee"),"T3:", expectedValues[3]);
        assertConnectionUsingDriverManager(
            getJDBCUrl("user=neelima;password=lee;securityMechanism=" +
            SECMEC_USRIDPWD),"T4:", expectedValues[4]);
        assertConnectionUsingDriverManager(
            getJDBCUrl("user=neelima;password=lee;securityMechanism=" +
            SECMEC_EUSRIDPWD),"T5:", expectedValues[5]);
        assertConnectionUsingDriverManager(
            getJDBCUrl("user=neelima;securityMechanism=" +
            SECMEC_USRIDONL),"T6:", expectedValues[6]);

        // disable as ibm142 and sun jce doesnt support DH prime of 32 bytes
        //assertConnectionUsingDriverManager(
        //    getJDBCUrl("user=neelima;password=lee;securityMechanism=" +
        //    SECMEC_USRENCPWD),"T7:", expectedValues[7]);
        assertConnectionUsingDriverManager(
            getJDBCUrl("user=neelima;password=lee;securityMechanism=" +
            SECMEC_USRIDONL),"T8:", expectedValues[8]);
        // Test strong password substitute DRDA security mechanism 
        // (only works with DerbyClient driver right now)
        assertConnectionUsingDriverManager(
            getJDBCUrl("user=neelima;password=lee;securityMechanism=" +
            SECMEC_USRSSBPWD),"T9:", expectedValues[9]);
    }
    
    
    /**
     * Get connection from datasource and also set security mechanism
     */
    private void assertConnectionUsingDataSource(String[] expectedValues)
    {
        // Note: bug in jcc, throws error with null password
        if (usingDerbyNetClient())
        {
            assertSecurityMechanismOK("sarah",null, new Short(
                SECMEC_USRIDONL),"SECMEC_USRIDONL:", expectedValues[1]);
        }
        assertSecurityMechanismOK("john","sarah", new Short(
            SECMEC_USRIDPWD),"SECMEC_USRIDPWD:", expectedValues[2]);

        // Possible bug in JCC, hence disable this test for JCC framework only
        // the security mechanism when set on JCC datasource does not seem to 
        // have an effect. JCC driver is sending a secmec of 3( USRIDPWD) to 
        // the server even though the security mechanism on datasource is set to 
        // EUSRIDPWD (9)
        if (usingDerbyNetClient())
        {
            // Please note: EUSRIDPWD security mechanism in DRDA uses 
            // Diffie-Helman for generation of shared keys. The spec specifies
            // the prime to use for DH which is 32 bytes and this needs to be
            // used as is.
            // Sun JCE does not support a prime of 32 bytes for Diffie Helman
            // nor do some older versions of IBM JCE (1.4.2).
            // Hence the following call to get connection might not be 
            // successful when client is running in a JVM where the JCE does
            // not support the DH (32 byte prime).
            // The test methods are implemented to work either way.
            assertSecurityMechanismOK("john","sarah",new Short(
                SECMEC_EUSRIDPWD),"SECMEC_EUSRIDPWD:", expectedValues[3]);
            // JCC does not support USRSSBPWD security mechanism
            assertSecurityMechanismOK("john","sarah",new Short(
                SECMEC_USRSSBPWD),"SECMEC_USRSSBPWD:", expectedValues[4]);
        }
    }

    private void assertSecurityMechanismOK(String user, String password,
        Short secmec, String msg, String expectedValue)
    {
        Connection conn;

        DataSource ds = getDS(user,password);
        try {
            JDBCDataSource.setBeanProperty(ds,
                    "SecurityMechanism", secmec);
            conn = ds.getConnection(user, password);
            conn.close();
            // EUSRIDPWD is supported with some jvm( version)s, not with others
            if (!(secmec.equals(new Short(SECMEC_EUSRIDPWD))))
            {
                if (!expectedValue.equals("OK"))
                {
                    fail("should have encountered an Exception");
                }
            }
        }
        catch (SQLException sqle)
        {
            if (sqle.getSQLState().equals("08001"))
            {
                // with null user id there's a difference between errors coming
                // from driver manager vs. datasource, because the datasource
                // getconnection call had to be specify user/password or the 
                // junit framework pads it with 'APP'.
                if (user == null)
                    assertSQLState08001("08001.C.7", sqle);
                else
                    assertSQLState08001(expectedValue, sqle);
            }
            // Exceptions expected in certain cases depending on JCE used for 
            // running the test. So, instead of '08004' (connection refused),
            // or "OK", we may see 'not supported' (XJ112).
            else if (secmec.equals(new Short(SECMEC_EUSRIDPWD))) 
            {
                if (!(sqle.getSQLState().equals("XJ112")))
                    assertSQLState(expectedValue, sqle);
            }
            else
                assertSQLState(expectedValue, sqle);
            // for debugging, uncomment:
            //dumpSQLException(sqle.getNextException());
        }
        catch (Exception e)
        {   
            fail(" should not have seen an exception");
        }
    }

    private void assertConnectionUsingDriverManager(
        String dbUrl, String msg, String expectedValue)
    {
        try
        {
            TestConfiguration.getCurrent();
            DriverManager.getConnection(dbUrl).close();
            // Please note: EUSRIDPWD security mechanism in DRDA uses 
            // Diffie-Helman for generation of shared keys. 
            // The spec specifies the prime to use for DH which is 32 bytes and
            // this needs to be used as is. Sun JCE does not support a prime of
            // 32 bytes for Diffie Helman and some older versions of IBM JCE 
            // ( 1.4.2) also do not support it. Hence the following call to get 
            // connection might not be successful when client is running in JVM
            // where the JCE does not support the DH (32 byte prime)
            
            if (derby_drda_securityMechanism != null && 
                !(derby_drda_securityMechanism.equals(
                "ENCRYPTED_USER_AND_PASSWORD_SECURITY") &&
                ((msg.indexOf("T5")>0) || (dbUrl.indexOf("9")>0))))
            {
                if (!expectedValue.equals("OK"))
                    fail("should have encountered an Exception");
            }
        }
        catch(SQLException sqle)
        {
            // if we're trying T5, and we've got EUSRIDPWD, 08004 is also
            // possible, or XJ112 instead of OK.
            if (derby_drda_securityMechanism != null  &&
                derby_drda_securityMechanism.equals(
                "ENCRYPTED_USER_AND_PASSWORD_SECURITY") &&
                ((msg.indexOf("T5")>0) ))
            {
                if (!(sqle.getSQLState().equals("XJ112")))
                    assertSQLState(expectedValue, sqle);
            }
            else if (sqle.getSQLState().equals("08001"))
                assertSQLState08001(expectedValue, sqle);
            else if (dbUrl.indexOf("9")>0)
            {
                if (!(sqle.getSQLState().equals("XJ112")))
                    assertSQLState(expectedValue, sqle);
            }
            else 
            {
                assertSQLState(expectedValue, sqle);
            }
            //for debugging sqles, uncomment: 
            // dumpSQLException(sqle.getNextException());
        }
    }

    /**
     * Test different interesting combinations of user,password, security 
     * mechanism for testing security mechanism upgrade logic. This test
     * has been added as part of DERBY-962. Two things have been fixed in
     * DERBY-962, affects only client behavior.
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
     * If security mechanism is not specified as part of the connection 
     * request, then client will do an automatic switching (upgrade) of 
     * security mechanism to use. The logic is as follows :
     * if password is available, and if the JVM in which the client is running 
     * supports EUSRIDPWD mechanism, in that case also, USRIDPWD security 
     * mechanism is used. 
     * if password is available, and if the JVM in which the client is running 
     * does not support EUSRIDPWD mechanism, in that case the client will then
     * default to USRIDPWD.
     * Also see DERBY-962 http://issues.apache.org/jira/browse/DERBY-962
     * <BR>
     * To understand which JVMs support EUSRIDPWD or not, please see class
     * level comments (#1)
     * <BR>
     * The expected output from this test will depend on the following
     * -- the client behavior (JCC 2.4, JCC2.6 or derby client).For the derby
     * client, the table below represents what security mechanism the client
     * will send to server. 
     * -- See class level comments (#2,#3) to understand the JCC2.6 and JCC2.4 
     * behavior
     * -- Note: in case of derby client, if no user  is specified, user 
     * defaults to APP.
     * -- Will depend on if the server has been started with property 
     * derby.drda.securityMechanism and to the value it is set to.  See method
     * (fixture) testNetworkServerSecurityMechanism() to check if server is 
     * using the derby.drda.securityMechanism to restrict client connections
     * based on security mechanism. 
     * 
     TABLE with all different combinations of userid, password, security
     mechanism of derby client that is covered by this testcase if test
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
	 d) support eusridpwd means whether this client jvm supports encrypted 
     userid/password security mechanism or not.  A value of Y means it 
     supports and N means no.
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
	 |Y    |Y     |-       |Y         |Y        Y       3            |
	 |----------------------------------------------------------------
	 |     |Y     |-       |Y         |Y        Y       3            |
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
    private void assertAllCombinationsOfUserPasswordSecMecInputOK(
        String[] expectedValues) {
        // Try following combinations:
        // user { null, user attribute given}
        // password {null, pwd specified}
        // securityMechanism attribute specified and not specified.
        // try with different security mechanism values - 
        // {encrypted useridpassword, userid only, clear text userid &password}
        // try with drivermanager, try with datasource
        
        String urlAttributes = null;

        for (int k = 0; k < USER_ATTRIBUTE.length; k++) {
            for (int j = 0; j < PWD_ATTRIBUTE.length; j++) {
                urlAttributes = "";
                if (USER_ATTRIBUTE[k] != null)
                    urlAttributes += "user=" + USER_ATTRIBUTE[k] +";";
                if (PWD_ATTRIBUTE[j] != null)
                    urlAttributes += "password=" + PWD_ATTRIBUTE[j] +";";
                
                // removing the last semicolon that we added here, getJDBCUrl
                // will add another semicolon for jcc, which would be too many.
                if (urlAttributes.length() >= 1)
                    urlAttributes = urlAttributes.substring(
                        0,urlAttributes.length()-1);

                // case - do not specify securityMechanism explicitly in the 
                // url get connection via driver manager and datasource.
                assertConnectionUsingDriverManager(
                    getJDBCUrl(urlAttributes), "Test:", 
                    getExpectedValueFromAll(expectedValues, k, j, 4));
                getDataSourceConnection(USER_ATTRIBUTE[k],PWD_ATTRIBUTE[j],
                    getExpectedValueFromAll(expectedValues, k, j, 4));

                for (int i = 0; i < SECMEC_ATTRIBUTE.length; i++) {
                    // case - specify securityMechanism attribute in url
                    // get connection using DriverManager
                    assertConnectionUsingDriverManager(
                        getJDBCUrl(urlAttributes + ";securityMechanism=" + 
                            SECMEC_ATTRIBUTE[i]), "#", 
                            getExpectedValueFromAll(expectedValues, k, j, i));
                    // case - specify security mechanism on datasource
                    assertSecurityMechanismOK(
                        USER_ATTRIBUTE[k],PWD_ATTRIBUTE[j], new Short 
                            (SECMEC_ATTRIBUTE[i]), "TEST_DS (" + urlAttributes
                            + ",securityMechanism="+SECMEC_ATTRIBUTE[i]+")", 
                            getExpectedValueFromAll(expectedValues, k, j, i));
                }
            }
        }
    }
    
    private String getExpectedValueFromAll(String[] expectedValues,
        int USER_ATTR, int PWD_ATTR, int SECMEC_ATTR)
    {
        String expectedValue;
        // There are 2 values each for USER_ATTR and PWD_ATTR.
        if (derby_drda_securityMechanism == null)
            return "OK";
        // elses
        // value 0 is just the name of the sec mechanism
        // sec mec '4' means no security mechanism specified
        // datasource and drivermanager calls should have same result
        // values 1-6 are for user 1, pwd 1
        if (USER_ATTR == 0 && PWD_ATTR == 0)
        {
            if (SECMEC_ATTR == 4)
                expectedValue = expectedValues[1];
            else
                expectedValue = expectedValues[2+SECMEC_ATTR];
        }
        else if (USER_ATTR == 0 && PWD_ATTR == 1)
        {
            if (SECMEC_ATTR == 4)
                expectedValue = expectedValues[6];
            else
                expectedValue = expectedValues[7+SECMEC_ATTR];
        }
        else if (USER_ATTR == 1 && PWD_ATTR == 0)
        {
            if (SECMEC_ATTR == 4)
                expectedValue = expectedValues[11];
            else
                expectedValue = expectedValues[12+SECMEC_ATTR];
        }
        else
        {
            if (SECMEC_ATTR == 4)
                expectedValue = expectedValues[16];
            else
                expectedValue = expectedValues[17+SECMEC_ATTR];
        }
        return expectedValue;
    }

    /**
     * Helper method to get connection from datasource and to print
     * the exceptions if any when getting a connection. This method 
     * is used in assertAllCombinationsOfUserPasswordSecMecInputOK.
     * For explanation of exceptions that might arise in this method,
     * please check assertAllCombinationsOfUserPasswordSecMecInputOK
     * javadoc comment.
     * get connection from datasource
     * @param user username
     * @param password password
     * @param expectedValue expected sql state
     */
    private void getDataSourceConnection(
        String user, String password, String expectedValue)
    {
        Connection conn;
        DataSource ds = getDS(user, password);
        try {
            // get connection via datasource without setting securityMechanism
            // cannot use ds.getConnection, because junit framework will
            // substitute 'null' with 'APP'.
            conn = ds.getConnection(user, password);
            conn.close();
        }
        catch (SQLException sqle)
        {
            // Exceptions expected in certain case hence printing message
            // instead of stack traces here. 
            // - For cases when userid is null or password is null and by
            //   default JCC does not allow a null password or null userid.
            // - For case when JVM does not support EUSRIDPWD and JCC 2.6 
            //   tries to do autoswitching of security mechanism.
            // - For case if server doesnt accept connection with this 
            //   security mechanism
            // - For case when client driver does support USRSSBPWD security
            //   mechanism
            if ((user == null) && (sqle.getSQLState().equals("08001")))
                assertSQLState08001("08001.C.7", sqle);
            else
                assertSQLState(expectedValue, sqle);
            // for debugging, uncomment:
            //dumpSQLException(sqle.getNextException());
        }
        catch (Exception e)
        {
            fail ("should not have gotten an exception");
        }
    }

    /**
     * Dump SQLState and message for the complete nested chain of SQLException 
     * @param sqle SQLException whose complete chain of exceptions is
     * traversed and sqlstate and message is printed out
     */
    private static void dumpSQLException(SQLException sqle)
    {
        while ( sqle != null)
        {
            println("SQLSTATE("+sqle.getSQLState()+"): " + 
                sqle.getMessage());
            sqle = sqle.getNextException();
        }
    }

    /**
     * Test a deferred connection reset. When connection pooling is done
     * and connection is reset, the client sends EXCSAT,ACCSEC and followed
     * by SECCHK and ACCRDB. Test if the security mechanism related information
     * is correctly reset or not. This method was added to help simulate 
     * regression test for DERBY-1080. It is called from testDerby1080.   
     * @param user username 
     * @param password password for connection
     * @param secmec security mechanism for datasource
     * @throws Exception
     */
    private void assertSecMecWithConnPoolingOK(
        String user, String password, Short secmec) throws Exception
    {     
        ConnectionPoolDataSource cpds = getCPDS(user,password);
        
        // call setSecurityMechanism with secmec.
        JDBCDataSource.setBeanProperty(cpds,
                "SecurityMechanism", secmec);
               
        // simulate case when connection will be re-used by getting 
        // a connection, closing it and then the next call to
        // getConnection will re-use the previous connection.  
        PooledConnection pc = cpds.getPooledConnection();
        Connection conn = pc.getConnection();
        conn.close();
        conn = pc.getConnection();
        assertConnectionOK(conn);
        pc.close();
        conn.close();
    }

    /**
     * Test a connection by executing a sample query
     * @param   conn    database connection
     * @throws Exception if there is any error
     */
    private void assertConnectionOK(Connection conn)
    throws SQLException
    {
        Statement stmt = conn.createStatement();
        ResultSet rs = null;
        // To test our connection, we will try to do a select from the 
        // system catalog tables
        rs = stmt.executeQuery("select count(*) from sys.systables");
        int updatecount=0;
        while(rs.next())
        {
            rs.getInt(1); // assume ok if no exception, ignore result
            updatecount++;
        }
        assertEquals(1,updatecount);
        
        if(rs != null)
            rs.close();
        if(stmt != null)
            stmt.close();
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
     * wont work are Sun JVM (versions 1.3.1, 1.4.1, 1.4.2, 1.5) and 
     * IBM JVM (versions 1.3.1 and some old versions of 1.4.2 (in 2004) )
     * 
     * Expected behavior for this test:
     * If no regression has occurred, this test should work OK, given the 
     * expected exception in following cases:
     * 1) When EUSRIDPWD is not supported in JVM the test is running, a CNFE
     * with initializing EncryptionManager will happen. This will happen for 
     * Sun JVM (versions 1.3.1, 1.4.1, 1.4.2, 1.5) and 
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
     * This will happen for the test run when derby.drda.securityMechanism is
     * set, to a valid value other than ENCRYPTED_USER_AND_PASSWORD_SECURITY.
     * <br>
     * See testNetworkServerSecurityMechanism where this method is called to 
     * test for regression for DERBY-1080, and to check if server is using the
     * derby.drda.securityMechanism to restrict client connections based on
     * the security mechanism.
     */
    private void assertDerby1080Fixed(String expectedValue)
            throws Exception {
        try
        {
            // simulate connection re-set using connection pooling on a pooled
            // datasource set security mechanism to use encrypted userid and
            // password.
            assertSecMecWithConnPoolingOK(
                "peter","neelima",new Short(SECMEC_EUSRIDPWD));
            if (!expectedValue.equals("OK"))
                fail("expected SQLException if DERBY-1080 did not regress");
        }
        catch (SQLException sqle)
        {
            // Exceptions expected in certain case hence accepting different
            // SQLStates.
            // - For cases where the jvm does not support EUSRIDPWD.
            // - For case if server doesnt accept connection with this security
            // mechanism
            // Please see javadoc comments for this test method for more 
            // details of expected exceptions.
            if(!(sqle.getSQLState().equals("XJ112")))
                assertSQLState(expectedValue, sqle);
            // for debugging, uncomment:
            // dumpSQLException(sqle.getNextException());
        }
    }

    /**
     * Test SECMEC_USRSSBPWD with derby BUILTIN authentication turned ON.
     *
     * We want to test a combination of USRSSBPWD with BUILTIN as password
     * substitute is only supported with NONE or BUILTIN Derby authentication
     * scheme right now (DERBY-528). Also, it doesn't work if passwords are
     * hashed with the configurable hash authentication scheme (DERBY-4483)
     * before they are stored in the database, so we'll need to disable that.
     * 
     * @throws Exception if there an unexpected error
     */
    private void assertUSRSSBPWD_with_BUILTIN(String[] expectedValues)
            throws Exception {
        // Turn on Derby BUILTIN authentication and attempt connecting with
        // USRSSBPWD security mechanism.
        println("Turning ON Derby BUILTIN authentication");
        Connection conn = getDataSourceConnectionWithSecMec(
            "neelima", "lee", new Short(SECMEC_USRSSBPWD));

        // Turn on BUILTIN authentication
        CallableStatement cs = conn.prepareCall(
            "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?, ?)");

        // First, disable the configurable hash authentication scheme so that
        // passwords are stored using the old hash algorithm.
        cs.setString(1, "derby.authentication.builtin.algorithm");
        cs.setString(2, null);
        cs.execute();

        cs.setString(1, "derby.user.neelima");
        cs.setString(2, "lee");
        cs.execute();

        cs.setString(1, "derby.user.APP");
        cs.setString(2, "APP");
        cs.execute();

        cs.setString(1, "derby.database.fullAccessUsers");
        cs.setString(2, "neelima,APP");
        cs.execute();

        cs.setString(1, "derby.connection.requireAuthentication");
        cs.setString(2, "true");
        cs.execute();

        cs.close();
        cs = null;

        conn.close();

        // Shutdown database for BUILTIN
        // authentication to take effect the next time it is
        // booted - derby.connection.requireAuthentication is a
        // static property.
        assertConnectionUsingDriverManager(getJDBCUrl(
            "user=APP;password=APP;shutdown=true;securityMechanism=" +
            SECMEC_USRSSBPWD),"USRSSBPWD (T0):", expectedValues[1]);

        // Now test some connection(s) with SECMEC_USRSSBPWD
        // via DriverManager and Datasource
        assertConnectionUsingDriverManager(getJDBCUrl(
            "user=neelima;password=lee;securityMechanism=" +
            SECMEC_USRSSBPWD),"USRSSBPWD + BUILTIN (T1):", expectedValues[2]);
        assertSecurityMechanismOK(
            "neelima","lee",new Short(SECMEC_USRSSBPWD),
            "TEST_DS - USRSSBPWD + BUILTIN (T2):", expectedValues[2]);
        // Attempting to connect with some invalid user
        assertConnectionUsingDriverManager(getJDBCUrl(
            "user=invalid;password=user;securityMechanism=" +
            SECMEC_USRSSBPWD),"USRSSBPWD + BUILTIN (T3):",expectedValues[3]);
        assertSecurityMechanismOK(
            "invalid","user",new Short(SECMEC_USRSSBPWD),
            "TEST_DS - USRSSBPWD + BUILTIN (T4):", expectedValues[3]);

        // Prepare to turn OFF Derby BUILTIN authentication
        conn = getDataSourceConnectionWithSecMec("neelima", "lee",
            new Short(SECMEC_USRSSBPWD));

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
        assertConnectionUsingDriverManager(getJDBCUrl(
            "user=APP;password=APP;shutdown=true;securityMechanism=" +
            SECMEC_USRSSBPWD),"USRSSBPWD + BUILTIN (T5):", expectedValues[4]);
    }
    
    private Connection getDataSourceConnectionWithSecMec(
        String user, String password, Short secMec)
            throws Exception {

        DataSource ds = getDS(user, password);
        JDBCDataSource.setBeanProperty(ds,
                "SecurityMechanism", secMec);
        return ds.getConnection();
    }

    private String getJDBCUrl(String attrs) {
        String dbName = 
            TestConfiguration.getCurrent().getDefaultDatabaseName();
        // s is protocol, subprotocol, + dbName
        String s = TestConfiguration.getCurrent().getJDBCUrl(dbName);

        if (attrs != null)
            if (usingDerbyNetClient())
                s = s + ";" + attrs;
            else
                s = s + ":" + attrs + ";";
        return s;
    }

    private javax.sql.DataSource getDS(String user, String password)
    {
        return getDS(user,password,null);
    }

    private javax.sql.DataSource getDS(
        String user, String password, HashMap attrs)
    {
        if (attrs == null)
            attrs = new HashMap();
        if (user != null)
            attrs.put("user", user);
        if (password != null)
            attrs.put("password", password);
        attrs = addRequiredAttributes(attrs);

        DataSource ds = JDBCDataSource.getDataSource();
        for (Iterator i = attrs.keySet().iterator(); i.hasNext(); )
        {
            String property = (String) i.next();
            Object value = attrs.get(property);
            JDBCDataSource.setBeanProperty(ds, property, value);
        }
        return ds;
    }

    private HashMap addRequiredAttributes(HashMap attrs)
    {
        String hostName = TestConfiguration.getCurrent().getHostName();
        int port = TestConfiguration.getCurrent().getPort();
        if (usingDB2Client())
        {
            //attrs.put("retrieveMessagesFromServerOnGetMessage","true");
            attrs.put("driverType","4");
            /**
             * As per the fix of derby-410 servername should
             * default to localhost, but for jcc it's still needed  
             */
            attrs.put("serverName",hostName);
        }
        /** 
         * For a remote host of course it's also needed 
         */
        if (!hostName.equals("localhost"))
        {
            attrs.put("serverName", hostName);
            attrs.put("portNumber", new Integer(port));
        }
        else
        {
            attrs.put("portNumber", new Integer(port));
        }
        return attrs;
    }

    private javax.sql.ConnectionPoolDataSource getCPDS(
        String user, String password)
    {
        HashMap attrs = new HashMap();
        if (user != null)
            attrs.put("user", user);
        if (password != null)
            attrs.put("password", password);

        attrs = addRequiredAttributes(attrs);
        ConnectionPoolDataSource cpds = 
            J2EEDataSource.getConnectionPoolDataSource();
        for (Iterator i = attrs.keySet().iterator(); i.hasNext(); )
        {
            String property = (String) i.next();
            Object value = attrs.get(property);
            JDBCDataSource.setBeanProperty(cpds, property, value);
        }
        return cpds;
    }
    
    private void assertSQLState08001(String expectedValue, SQLException sqle)
    {
        if (expectedValue.equals("08001.C.7"))
            assertEquals("User id can not be null.", sqle.getMessage());
        if (expectedValue.equals("08001.C.8"))
            assertEquals("Password can not be null.", sqle.getMessage());
    }
}
