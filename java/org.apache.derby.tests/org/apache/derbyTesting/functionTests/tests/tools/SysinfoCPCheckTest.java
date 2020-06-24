/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.tools.SysinfoCPCheckTest

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

package org.apache.derbyTesting.functionTests.tests.tools;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Locale;
import junit.framework.Test;
import org.apache.derby.shared.common.info.JVMInfo;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.Derby;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.LocaleTestSetup;

public class SysinfoCPCheckTest extends BaseJDBCTestCase {

    public SysinfoCPCheckTest(String name) { 
        super(name); 
    }

    private static boolean isClient = true;
    private static boolean isServer = true;
    
    public static Test suite() {

        if (!Derby.hasTools())
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
            return new BaseTestSuite("empty: no tools support");
        
        // check to see if we have derbynet.jar or derbyclient.jar
        // before starting the security manager...
        if (!Derby.hasServer())
            isServer=false;
        if (!Derby.hasClient())
            isClient=false;

//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        Test suite = new BaseTestSuite(
            SysinfoCPCheckTest.class, "Sysinfo ClassPath Checker");

//IC see: https://issues.apache.org/jira/browse/DERBY-5211
        return new LocaleTestSetup(suite, Locale.ENGLISH);
    }

    /**
     * Test sysinfo.testClassPathChecker()
     *
     * Tests sysinfo classpathtester
     * This test compares expected output strings; expected language is en_US
     * 
     */
    /**
     *  Test Classpath Checker output for 3 supported variations
     */
    public void testClassPathChecker() throws IOException {
//IC see: https://issues.apache.org/jira/browse/DERBY-5879
        String Success = "Success: All Derby related classes found in class path.";
        // for testing the -cp with valid class
        String thisclass = "org.apache.derbyTesting.functionTests.tests.tools." +
        "SysinfoCPCheckTest.class";
        // initialize the array of arguments and expected return strings
        // The purpose of the values in the inner level is:
        // {0: argument to be passed with -cp,
        //  1: line number in the output to compare with,
        //  2: string to compare the above line with
        //  3: optional string to search for in addition to the above line
        //  4: a string/number to unravel the output
        final String[][] tstargs = {
                // empty string; should check all; what to check? Just check top
                // to ensure it recognizes it needs to check all.
                {null, "0", "Testing for presence of all Derby-related " +
                    "libraries; typically, only some are needed.", null},
                // incorrect syntax, or 'args' - should return usage
                {
//IC see: https://issues.apache.org/jira/browse/DERBY-4806
//IC see: https://issues.apache.org/jira/browse/DERBY-4597
                        "a",
                        "0",
//IC see: https://issues.apache.org/jira/browse/DERBY-5879
                        "Usage: java org.apache.derby.tools.sysinfo -cp ["
                                + " [ embedded ][ server ][ client] [ tools ]"
                                + " [ anyClass.class ] ]", null },
                {"embedded", "6", Success, "derby.jar"}, 
                {"server", "10", Success, "derbynet.jar"},
                {"tools", "6", Success, "derbytools.jar"},
                {"client", "6", Success, "derbyclient.jar"},
                {thisclass, "6", Success, "SysinfoCPCheckTest"},
                // neg tst, hope this doesn't exist
                {"nonexist.class", "6", "    (nonexist not found.)", null}
        };

        

        PrintStream out = System.out;

        int tst=0;
        for (tst=0; tst<tstargs.length ; tst++)
        {
            ByteArrayOutputStream rawBytes = getOutputStream();
//IC see: https://issues.apache.org/jira/browse/DERBY-2903

            // First obtain the output for the sysinfo command
//IC see: https://issues.apache.org/jira/browse/DERBY-3771
            PrintStream testOut = new PrintStream(rawBytes,
                    false);
            setSystemOut(testOut);
         
            if (!checkClientOrServer(tstargs[tst][0]))
                continue;

            // First command has only 1 arg, prevent NPE with if/else block 
            if (tstargs[tst][0] == null)
                org.apache.derby.tools.sysinfo.main(new String[] {"-cp"} );
            else
                org.apache.derby.tools.sysinfo.main(
                    new String[] {"-cp", tstargs[tst][0]} );

            setSystemOut(out);

            rawBytes.flush();
            rawBytes.close();

            byte[] testRawBytes = rawBytes.toByteArray();

            //System.out.println("cp command: -cp " + tstargs[tst][0]);

            String s = null;

            try {
                BufferedReader sysinfoOutput = new BufferedReader(
                    new InputStreamReader(
                        new ByteArrayInputStream(testRawBytes)));
//IC see: https://issues.apache.org/jira/browse/DERBY-3771

                // evaluate the output
                // compare the sentence picked

                // first one is a bit different - is classpath dependent, so
                // we're not going to look through all lines.
                if (tstargs[tst][0]==null)
                {
                    s=sysinfoOutput.readLine();
                    assertEquals(tstargs[tst][2], s);
                    while (s != null)
                    {
                        s=sysinfoOutput.readLine();
                    }
                    continue;
                }

                if (!checkClientOrServer(tstargs[tst][0]))
                    continue;

                // get the appropriate line for the full line comparison
                int linenumber = Integer.parseInt(tstargs[tst][1]);

                boolean found = false;

                for (int i=0; i<linenumber; i++)
                {
                    s = sysinfoOutput.readLine();
                    if (tstargs[tst][3] != null)
                    {
                        // do the search for the optional string comparison
                        if (s.indexOf(tstargs[tst][3])>0)
                            found = true;
                    }
                }
                if (tstargs[tst][3] != null && !found)
                    fail ("did not find the string searched for: " + 
                         tstargs[tst][3] + " for command -cp: " + tstargs[tst][0]);
//IC see: https://issues.apache.org/jira/browse/DERBY-2903

                // read the line to be compared
                s = sysinfoOutput.readLine();

                if (s == null)
                    fail("encountered unexpected null strings");
                else
                {
                    assertEquals(tstargs[tst][2], s);
                }

                // read one more line - should be the next command's sequence number
                s = sysinfoOutput.readLine();

//IC see: https://issues.apache.org/jira/browse/DERBY-2903
                sysinfoOutput.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public boolean checkClientOrServer(String kind)
    {
        if (kind == null)
            return true;
        // if there is no derbynet.jar, the syntax should still
        // work, but the comparisons will fail. So never mind.
        // JSR169 / J2ME does not support client or server
//IC see: https://issues.apache.org/jira/browse/DERBY-2903
        if ((kind.equals("server") || kind.equals("client")) 
                && JDBC.vmSupportsJSR169())
            return false;

        if (kind.equals("server")) 
            return isServer;
        // same for derbyclient.jar
        if (kind.equals("client"))
            return isClient;
        return true;
    }

//IC see: https://issues.apache.org/jira/browse/DERBY-2903
    ByteArrayOutputStream getOutputStream() {
        return new ByteArrayOutputStream(20 * 1024);
    }
    
    public void testjavaVersion(){
//IC see: https://issues.apache.org/jira/browse/DERBY-6857
        assertEquals(JVMInfo.J2SE_18, JVMInfo.JDK_ID);
    }
     
    // Still testing this here although we don't actually put
    // out this line with sysinfo anymore.
    public void testderbyVMLevel() {
        switch (JVMInfo.JDK_ID) {
            case JVMInfo.J2SE_18:
                assertEquals("Java SE 8 - JDBC 4.2", JVMInfo.derbyVMLevel());
                break;
            default:
                assertEquals("?-?", JVMInfo.derbyVMLevel());
                break;
        }
    }
     
     public void testisSunJVM(){
    	 if(JVMInfo.isSunJVM()==true){
    		 assertEquals(true,JVMInfo.isSunJVM());	
    	 }
    	 else{
    		 assertEquals(false,JVMInfo.isSunJVM());		 
    	 }
     }
     
     public void testisIBMJVM(){
    	 if(JVMInfo.isIBMJVM()==true){
    		 assertEquals(true,JVMInfo.isIBMJVM());	
    	 }
    	 else{
    		 assertEquals(false,JVMInfo.isIBMJVM());		 
    	 }
    }
     
    public void testhasJNDI(){
    	if(JVMInfo.hasJNDI()==true){
    		assertEquals(true,JVMInfo.hasJNDI());		
    	}
    	else{
    		assertEquals(false,JVMInfo.hasJNDI());
    	}
    	
    }
}
