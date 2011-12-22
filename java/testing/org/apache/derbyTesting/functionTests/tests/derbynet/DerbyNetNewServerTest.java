/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.DerbyNetNewServerTest

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

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derby.drda.NetworkServerControl;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.NetworkServerTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test NetworkServerControl.start(PrintWriter) writes to the print Writer
 *
 * test:
 *<ul>
 *<li> start( printWriter)
 *<li> start( (PrintWriter) null)
 *</ul>
 */
public class DerbyNetNewServerTest extends BaseJDBCTestCase {

    public DerbyNetNewServerTest(String name) {
        super(name);    
    }    

    public static Test suite() {
        // Test does not run on J2ME
        if (JDBC.vmSupportsJDBC3()) {
            return new TestSuite(DerbyNetNewServerTest.class);
        } else {
            return new TestSuite("DerbyNetNewServerTest.empty");
        }
    }

    protected void setUp() throws Exception {
        // The test cases in this test start a new network server. Wait until
        // the network server in the previous test case has shut down
        // completely and released the network port before attempting to start
        // a new server.
        NetworkServerTestSetup.waitForAvailablePort();
    }

    public void testStartWithPrintWriter()
            throws UnknownHostException,
            Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(10240);
        // DERBY-1466, Test that messages are flushed to the
        // writer irrespective of whether the user's writer is
        // set to autoflush true.
        PrintWriter writer = new PrintWriter(bos);

        NetworkServerControl server = new NetworkServerControl(InetAddress
                .getByName(TestConfiguration.getCurrent().getHostName()),
                TestConfiguration.getCurrent().getPort());

        assertEquals("No log initially", 0, bos.size());
        server.start(writer);
        
        NetworkServerTestSetup.waitForServerStart(server);
        int sizeAfterPing = bos.size();
        assertTrue("Create log with start message", 0 < sizeAfterPing);        
        
        server.shutdown();
        int sizeAfterShutDown = bos.size();
        bos.close();
        bos = null;
        writer.close();
        assertTrue("Num of log item should add", 
                sizeAfterShutDown > sizeAfterPing);
    }
    
    public void testStartWithoutPrintWriter()
            throws UnknownHostException,
            Exception {
        NetworkServerControl server = new NetworkServerControl(InetAddress
                .getByName(TestConfiguration.getCurrent().getHostName()),
                TestConfiguration.getCurrent().getPort());

        server.start(null);
        NetworkServerTestSetup.waitForServerStart(server);
        server.shutdown();

        //to show this is a right workflow.
        assertTrue(true);
    }

}
