/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.drda
   (C) Copyright IBM Corp. 2002, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.drda;

import java.io.*;
import java.net.*;
import java.security.*;

class ClientThread extends Thread {
	/**
		IBM Copyright &copy notice.
	*/

 private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2002_2004;

	DB2jServerImpl parent;
	ServerSocket serverSocket;
	private int timeSlice;
	private int connNum;
	private String traceDir;
	private boolean traceAll;

		protected ClientThread (DB2jServerImpl dsi, ServerSocket ss) {

			// Create a more meaningful name for this thread (but preserve its
			// thread id from the default name).
			DB2jServerImpl.setUniqueThreadName(this, "NetworkServerThread");

			parent=dsi;
			serverSocket=ss;
			timeSlice=dsi.getTimeSlice();
			traceDir=parent.getTraceDirectory();
			traceAll=parent.getTraceAll();
		}
			
		public void run() 
		{

			Socket clientSocket = null;
			Session clientSession = null;


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
	                	parent.consolePropertyMessage("DRDA_ConnNumber.I", 
							Integer.toString(connNum));

				//create a new Session for this session
				clientSession = new Session(connNum, clientSocket, 
					traceDir, traceAll);

				//add to Session list
				parent.addToSessionTable(new Integer(connNum), clientSession);

				//create a new thread for this connection if we need one
				//and if we are allowed
				if (parent.getFreeThreads() == 0 && 
					(parent.getMaxThreads() == 0  || 
					parent.getThreadList().size() < parent.getMaxThreads()))
				{
					DRDAConnThread thread = new DRDAConnThread(clientSession, 
						parent, timeSlice, parent.getLogConnections());
					parent.getThreadList().addElement(thread);
					thread.start();
				}
				else //wait for a free thread
					parent.runQueueAdd(clientSession);
				}catch (Exception e) {
					if (e instanceof InterruptedException)
						return;
					parent.consoleExceptionPrintTrace(e);
				} // end outer try/catch block
			} // end for(;;)

		}// end run()
}







