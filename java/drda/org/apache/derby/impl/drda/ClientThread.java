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

import java.io.*;
import java.net.*;
import java.security.*;

final class ClientThread extends Thread {

	NetworkServerControlImpl parent;
	ServerSocket serverSocket;
	private int timeSlice;
	private int connNum;

		ClientThread (NetworkServerControlImpl nsi, ServerSocket ss) {

			// Create a more meaningful name for this thread (but preserve its
			// thread id from the default name).
			NetworkServerControlImpl.setUniqueThreadName(this, "NetworkServerThread");

			parent=nsi;
			serverSocket=ss;
			timeSlice=nsi.getTimeSlice();
		}
			
		public void run() 
		{

			Socket clientSocket = null;


			for (;;)
			{
				try {
	          		try{
	             			clientSocket = (Socket) AccessController.doPrivileged(
						new PrivilegedExceptionAction() {
							public Object run() throws IOException
							{
								return serverSocket.accept();
							}
						}
					 );
							clientSocket.setKeepAlive(parent.getKeepAlive());
					//set time out					
					//this looks highly suspect.  Why does timeSlice setSoTimeout?		
					if (timeSlice != 0)
						clientSocket.setSoTimeout(timeSlice);
				} catch (PrivilegedActionException e) {
					Exception e1 = e.getException();
	            			if (e1 instanceof IOException){
						synchronized(parent.getShutdownSync()) {
							if (!parent.getShutdown())
	                					parent.consolePropertyMessage("DRDA_UnableToAccept.S");
							}
					} else throw e1;
	                		break;
				} // end priv try/catch block
				
				connNum = parent.getNewConnNum();
                                if (parent.getLogConnections())
                                    parent.consolePropertyMessage("DRDA_ConnNumber.I", 
							Integer.toString(connNum));

				//create a new Session for this session
				parent.addSession(connNum, clientSocket);

				}catch (Exception e) {
					if (e instanceof InterruptedException)
						return;
					parent.consoleExceptionPrintTrace(e);
				} // end outer try/catch block
			} // end for(;;)

		}// end run()
}







