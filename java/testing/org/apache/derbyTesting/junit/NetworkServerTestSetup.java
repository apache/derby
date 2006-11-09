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
import java.net.UnknownHostException;
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
 *
 * Currently it will start the network server in the same VM
 * and it does not support starting it from a remote 
 * machine.
 */
final public class NetworkServerTestSetup extends TestSetup {

    private FileOutputStream serverOutput;
    private final boolean asCommand;
    
    /**
     * Decorator this test with the NetworkServerTestSetup
     */
    public NetworkServerTestSetup(Test test, boolean asCommand) {
        super(test);
        this.asCommand = asCommand;
    }

    /**
     * Start the network server.
     */
    protected void setUp() throws Exception {
        BaseTestCase.println("Starting network server:");
        
        networkServerController = getNetworkServerControl();
        
        if (asCommand)
            startWithCommand();
        else
            startWithAPI();
        
        waitForServerStart(networkServerController);
    }

    private void startWithAPI() throws Exception
    {
            
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
            
            networkServerController.start(new PrintWriter(serverOutput));
   
    }
    
    private void startWithCommand() throws Exception
    {
        final TestConfiguration config = TestConfiguration.getCurrent();
        
        // start the server through the command line
        // arguments using a new thread to do so.
        new Thread(
        new Runnable() {
            public void run() {
                org.apache.derby.drda.NetworkServerControl.main(
                        new String[] {
                                "start",
                                "-h",
                                config.getHostName(),
                                "-p",
                                Integer.toString(config.getPort())
                        });                
            }
            
        }, "NetworkServerTestSetup command").start();
    }

    /**
     * Stop the network server if it still
     * appears to be running.
     */
    protected void tearDown() throws Exception {
        if (networkServerController != null) {
            boolean running = false;
            try {
                networkServerController.ping();
                running = true;
            } catch (Exception e) {
            }
      
            if (running)
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
    
    
    /*
     * Utility methods related to controlling network server.
     */
    
    /**
     * Return a new NetworkServerControl for the current configuration.
     */
    public static NetworkServerControl getNetworkServerControl()
        throws Exception
    {
        TestConfiguration config = TestConfiguration.getCurrent();
        return new NetworkServerControl
        (InetAddress.getByName(config.getHostName()), config.getPort());
    }
    
    /**
     * Ping the server until it has started. Asserts a failure
     * if the server has not started within sixty seconds.
     */
    public static void waitForServerStart(NetworkServerControl networkServerController)
        throws InterruptedException {
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
}
