/*

   Derby - Class org.apache.derby.iapi.jdbc.DRDAServerStarter

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

package org.apache.derby.iapi.jdbc;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.iapi.reference.Property;
import java.io.PrintWriter;
import java.lang.Runnable;
import java.lang.Thread;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

public class DRDAServerStarter implements ModuleControl, Runnable
{

    private Object server;
    private Method serverStartMethod;
	private Method serverShutdownMethod;
    private boolean loadSysIBM;
    private Thread serverThread;
    private static final String serverClassName = "org.apache.derby.impl.drda.DB2jServerImpl";
    private Class serverClass;
	
	private InetAddress listenAddress =null;
	private int portNumber = -1;
	private PrintWriter consoleWriter = null;

    /**
     * Try to start the DRDA server. Log an error in error log and continue if it cannot be started.
     */
//     public static void start()
//     {


	public void setStartInfo(InetAddress listenAddress, int portNumber, PrintWriter
							 consoleWriter)
	{
		this.listenAddress = listenAddress;
		this.portNumber = portNumber;
		this.consoleWriter = consoleWriter;
	}



    public void boot(boolean create,
                     java.util.Properties properties)
    {
        if( server != null)
        {
            if (SanityManager.DEBUG)
                SanityManager.THROWASSERT( "Network server starter module booted twice.");
            return;
        }
        // Load the server class indirectly so that Cloudscape does not require the network code
        try
        {
            serverClass = Class.forName( serverClassName);
        }
        catch( ClassNotFoundException cnfe)
        {
            Monitor.logTextMessage( MessageId.CONN_NETWORK_SERVER_CLASS_FIND, serverClassName);
            return;
        }
        catch( java.lang.Error e)
        {
            Monitor.logTextMessage( MessageId.CONN_NETWORK_SERVER_CLASS_LOAD,
                                    serverClassName,
                                    e.getMessage());
            return;
        }
        try
        {
            Constructor  serverConstructor;
            try
            {
                serverConstructor = (Constructor) AccessController.doPrivileged(
			      new PrivilegedExceptionAction() {
						  public Object run() throws NoSuchMethodException, SecurityException
						  {
							  if (listenAddress == null)
								  return serverClass.getDeclaredConstructor(null);
							  else
								  return
									  serverClass.getDeclaredConstructor(new
										  Class[] {java.net.InetAddress.class,
												   Integer.TYPE});}
					  }
				  );
				serverStartMethod = (Method) AccessController.doPrivileged(
				   new PrivilegedExceptionAction() {
						   public Object run() throws NoSuchMethodException, SecurityException
						   { return serverClass.getMethod( "blockingStart", new Class[] { java.io.PrintWriter.class});}
					   }
				   );
				
				serverShutdownMethod = (Method) AccessController.doPrivileged(
				   new PrivilegedExceptionAction() {
						   public Object run() throws NoSuchMethodException, SecurityException
						   { return serverClass.getMethod( "directShutdown", null);}
					   }
				   );
            }
            catch( PrivilegedActionException e)
            {
                Exception e1 = e.getException();
                Monitor.logTextMessage(
									   MessageId.CONN_NETWORK_SERVER_START_EXCEPTION, e1.getMessage());
				e.printStackTrace(Monitor.getStream().getPrintWriter());
                return;

            }
			if (listenAddress == null)
				server = serverConstructor.newInstance( null);
			else
				server = serverConstructor.newInstance(new Object[]
					{listenAddress, new Integer(portNumber)});

            serverThread = Monitor.getMonitor().getDaemonThread( this, "NetworkServerStarter", false);
            serverThread.start();
        }
        catch( Exception e)
        {
			Monitor.logTextMessage( MessageId.CONN_NETWORK_SERVER_START_EXCEPTION, e.getMessage());
			server = null;
			e.printStackTrace(Monitor.getStream().getPrintWriter());
        }
    } // end of boot

    public void run()
    {
        try
        {
            serverStartMethod.invoke( server,
                                      new Object[] {consoleWriter });
        }
        catch( InvocationTargetException ite)
        {
            Monitor.logTextMessage(
								   MessageId.CONN_NETWORK_SERVER_START_EXCEPTION, ite.getTargetException().getMessage());
			ite.printStackTrace(Monitor.getStream().getPrintWriter());

            server = null;
        }
        catch( Exception e)
        {
            Monitor.logTextMessage( MessageId.CONN_NETWORK_SERVER_START_EXCEPTION, e.getMessage());
            server = null;
			e.printStackTrace(Monitor.getStream().getPrintWriter());
        }
    }
    
    public void stop()
    {
		try {
			if( serverThread != null && serverThread.isAlive())
			{
				serverShutdownMethod.invoke( server,
											 null);
				serverThread.interrupt();
				serverThread = null;
			}
		   
		}
		catch( InvocationTargetException ite)
        {
			Monitor.logTextMessage(
								   MessageId.CONN_NETWORK_SERVER_SHUTDOWN_EXCEPTION, ite.getTargetException().getMessage());
			ite.printStackTrace(Monitor.getStream().getPrintWriter());
			
        }
        catch( Exception e)
        {
            Monitor.logTextMessage( MessageId.CONN_NETWORK_SERVER_SHUTDOWN_EXCEPTION, e.getMessage());
			e.printStackTrace(Monitor.getStream().getPrintWriter());
		}
			
		serverThread = null;
		server = null;
		serverClass = null;
		serverStartMethod = null;
		serverShutdownMethod = null;
		listenAddress = null;
		portNumber = -1;
		consoleWriter = null;
		
    } // end of stop
}
