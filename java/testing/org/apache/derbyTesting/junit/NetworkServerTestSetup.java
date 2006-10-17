/*
 *
 * Derby - Class org.apache.derbyTesting.junit.NetworkServerTestSetup
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.junit;

import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.security.AccessController;
import java.security.PrivilegedAction;
import junit.extensions.TestSetup;
import junit.framework.Test;
import org.apache.derby.drda.NetworkServerControl;

/**
 * Test decorator that starts the network server on startup
 * and stops it on teardown.
 * 
 * It does not start it if the test is configured to run in
 * embedded mode.
 *
 * Currently it will start the network server in the same VM
 * and it does not support starting it from a remote 
 * machine.
 */
final public class NetworkServerTestSetup extends TestSetup {

    private FileOutputStream serverOutput;
    
    /**
     * Decorator this test with the NetworkServerTestSetup
     */
    public NetworkServerTestSetup(Test test) {
        super(test);
    }

    /**
     * Start the network server.
     */
    protected void setUp() throws Exception {
        
        TestConfiguration config = TestConfiguration.getCurrent();
        
            BaseTestCase.println("Starting network server:");

            
            serverOutput = (FileOutputStream)
            AccessController.doPrivileged(new PrivilegedAction() {
                public Object run() {
                    File logs = new File("logs");
                    logs.mkdir();
                    File console = new File(logs, "serverConsoleOutput.log");
                    FileOutputStream fos = null;
                    try {
                        fos = new FileOutputStream(console.getPath(), true);
                    } catch (FileNotFoundException ex) {
                        ex.printStackTrace();
                    }
                    return fos;
                }
            });

            networkServerController = new NetworkServerControl
                (InetAddress.getByName(config.getHostName()), config.getPort());
            
            networkServerController.start(new PrintWriter(serverOutput));
            
            final long startTime = System.currentTimeMillis();
            while (true) {
                Thread.sleep(SLEEP_TIME);
                try {
                    networkServerController.ping();
                    break;
                } catch (Exception e) {
                    if (System.currentTimeMillis() - startTime > WAIT_TIME) {
                        e.printStackTrace();
                        fail("Timed out waiting for network server to start");
                    }
                }
            }
    }

    /**
     * Stop the network server.
     */
    protected void tearDown() throws Exception {
        if (networkServerController != null) {
            networkServerController.shutdown();
            serverOutput.close();
        }
    }
    
    /* Network Server Control */
    private NetworkServerControl networkServerController;
    
    /** Wait maximum 1 minute for server to start */
    private static final int WAIT_TIME = 60000;
    
    /** Sleep for 50 ms before pinging the network server (again) */
    private static final int SLEEP_TIME = 50;
}
