/*

   Derby - Class org.apache.derby.impl.drda.ClientThread

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

package org.apache.derby.impl.drda;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;

final class ClientThread extends Thread {
//IC see: https://issues.apache.org/jira/browse/DERBY-467

//IC see: https://issues.apache.org/jira/browse/DERBY-5896
    NetworkServerControlImpl parent;
    ServerSocket serverSocket;
    private int timeSlice;
    
//IC see: https://issues.apache.org/jira/browse/DERBY-467
    ClientThread (NetworkServerControlImpl nsi, ServerSocket ss) {
        // Use a more meaningful name for this thread.
        super(NetworkServerControlImpl.getUniqueThreadName(
                "NetworkServerThread"));

        parent=nsi;
        serverSocket=ss;
        timeSlice=nsi.getTimeSlice();
    }
    
    public void run() 
    {
        Socket clientSocket = null;
        
        for (;;) { // Nearly infinite loop. The loop is terminated if
                   // 1) We are shut down or 2) SSL won't work. In all
                   // other cases we just continue and try another
                   // accept on the socket.

            try { // Check for all other exceptions....

                try { // Check for underlying InterruptedException,
                      // SSLException and IOException

                    try { // Check for PrivilegedActionException
                        clientSocket =
                                    acceptClientWithRetry();
                        // Server may have been shut down.  If so, close this
                        // client socket and break out of the loop.
                        // DERBY-3869
                        if (parent.getShutdown()) {
                            if (clientSocket != null)
                                clientSocket.close();
                            return;
                        }
                            
                        clientSocket.setKeepAlive(parent.getKeepAlive());
                        
                        // Set time out: Stops DDMReader.fill() from
                        // waiting indefinitely when timeSlice is set.
//IC see: https://issues.apache.org/jira/browse/DERBY-2748
                        if (timeSlice > 0)
                            clientSocket.setSoTimeout(timeSlice);
                        
                        //create a new Session for this socket
//IC see: https://issues.apache.org/jira/browse/DERBY-1817
                        parent.addSession(clientSocket);
                        
                    } catch (PrivilegedActionException e) {
                        // Just throw the underlying exception
                        throw e.getException();
                    } // end inner try/catch block
                    
                } catch (InterruptedException ie) {
//IC see: https://issues.apache.org/jira/browse/DERBY-4326
                    if (parent.getShutdown()) {
                        // This is a shutdown and we'll just exit the
                        // thread. NOTE: This is according to the logic
                        // before this rewrite. I am not convinced that it
                        // is allways the case, but will not alter the
                        // behaviour since it is not within the scope of
                        // this change (DERBY-2108).
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
                        clientSocket.close();
                        return;
                    }
                    parent.consoleExceptionPrintTrace(ie);
                    if (clientSocket != null)
                        clientSocket.close();

                } catch (javax.net.ssl.SSLException ssle) {
                    // SSLException is a subclass of
                    // IOException. Print stack trace and...
                    
                    parent.consoleExceptionPrintTrace(ssle);
                    
                    // ... we need to do a controlled shutdown of the
                    // server, since SSL for some reason will not
                    // work.
                    // DERBY-3537: circumvent any shutdown security checks
                    parent.directShutdownInternal();
                    
                    return; // Exit the thread
                    
                } catch (IOException ioe) {
//IC see: https://issues.apache.org/jira/browse/DERBY-4326
                    if (clientSocket != null)
                        clientSocket.close();
                    // IOException causes this thread to stop.  No
                    // console error message if this was caused by a
                    // shutdown
                    synchronized (parent.getShutdownSync()) {
                        if (parent.getShutdown()) {
                            return; // Exit the thread
                        } 
                    }
//IC see: https://issues.apache.org/jira/browse/DERBY-3704
                    parent.consoleExceptionPrintTrace(ioe);
                }
            } catch (Exception e) {
                // Catch and log all other exceptions
                
                parent.consoleExceptionPrintTrace(e);
//IC see: https://issues.apache.org/jira/browse/DERBY-3704
                try {
                    if (clientSocket != null)
                        clientSocket.close();
                } catch (IOException closeioe)
                {
                    parent.consoleExceptionPrintTrace(closeioe);
                }
            } // end outer try/catch block
            
        } // end for(;;)
        
    }// end run()

    /**
     * Perform a server socket accept. Allow three attempts with a one second
     * wait between each
     * 
     * @return client socket or null if accept failed.
     * 
     */
    private Socket acceptClientWithRetry() {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        return AccessController.doPrivileged(
                new PrivilegedAction<Socket>() {
                    public Socket run() {
                        for (int trycount = 1; trycount <= 3; trycount++) {
                            try {
                                // DERBY-5347 Need to exit if
                                // accept fails with IOException
                                // Cannot just aimlessly loop
                                // writing errors
                                return serverSocket.accept();
//IC see: https://issues.apache.org/jira/browse/DERBY-6112
                            } catch (Exception acceptE) {
                                // If not a normal shutdown,
                                // log and shutdown the server
                                if (!parent.getShutdown()) {
                                    parent
                                            .consoleExceptionPrintTrace(acceptE);
                                    if (trycount == 3) {
                                        // give up after three tries
                                        parent.directShutdownInternal();
                                    } else {
                                        // otherwise wait 1 second and retry
                                        try {
                                            Thread.sleep(1000);
                                        } catch (InterruptedException ie) {
                                            parent
                                            .consoleExceptionPrintTrace(ie);
                                        }
                                    }
                                }
                            }
                        }
                        return null; // no socket to return after three tries
                    }
                }

                );
    }
}







