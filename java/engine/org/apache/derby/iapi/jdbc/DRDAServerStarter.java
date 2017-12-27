/*

   Derby - Class org.apache.derby.iapi.jdbc.DRDAServerStarter

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.iapi.jdbc;

import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.shared.common.reference.MessageId;
import org.apache.derby.iapi.reference.Property;
import java.io.PrintWriter;
import java.lang.Runnable;
import java.lang.Thread;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * Class that starts the network server in its own daemon thread.
 * Works in two situations.
 * <BR>
 * As a module in the engine's Monitor, booted if the
 * property derby.drda.startNetworkServer is set to true.
 * In this case the boot and shutdown is through the
 * standard ModuleControl methods.
 * <BR>
 * Direct calls from the NetworkServerControlImpl start methods.
 * This is to centralize the creation of the daemon thread in
 * this class in the engine code, since the Monitor provides
 * the thread. This means that NetworkServerControlImpl calls
 * this class to create a thread which in turn calls back
 * to NetworkServerControlImpl.runServer to start the server.
 *
 * @see ModuleControl#boot
 * @see ModuleControl#stop
 */
public final class DRDAServerStarter implements ModuleControl, Runnable
{
    /**
     * The instance of the NetworkServerControlImpl
     * being used to run the server.
     */
    private Object server;
    
    /**
     * Reflect reference to the method to run the server.
     * NetworkServerControlImpl.blockingStart
     */
    private Method runServerMethod;
    
    /**
     * Reflect reference to the method to directly
     * shutdown the server.
     * NetworkServerControlImpl.directShutdown
     */
    private Method serverShutdownMethod;

    private Thread serverThread;
    private static final String serverClassName = "org.apache.derby.impl.drda.NetworkServerControlImpl";
    private Class<?> serverClass;
	
	private InetAddress listenAddress =null;
	private int portNumber = -1;
	private String userArg = null;
	private String passwordArg = null;
	private PrintWriter consoleWriter = null;

    /**
     * Try to start the DRDA server. Log an error in error log and continue if it cannot be started.
     */
//     public static void start()
//     {


	/**
	 * Sets configuration information for the network server to be started.
	 * @param listenAddress InetAddress to listen on
	 * @param portNumber    portNumber to listen on
	 * @param userName      the user name for actions requiring authorization
	 * @param password      the password for actions requiring authorization
	 */
	public void setStartInfo(InetAddress listenAddress, int portNumber,
                             String userName, String password,
                             PrintWriter consoleWriter)
	{
		this.userArg = userName;
		this.passwordArg = password;
        setStartInfo(listenAddress, portNumber, consoleWriter);
    }

	public void setStartInfo(InetAddress listenAddress, int portNumber, PrintWriter
							 consoleWriter)
	{
		this.listenAddress = listenAddress;
		this.portNumber = portNumber;

        // wrap the user-set consoleWriter with autoflush to true.
        // this will ensure that messages to console will be 
        // written out to the consoleWriter on a println.
        // DERBY-1466
        if (consoleWriter != null)
            this.consoleWriter = new PrintWriter(consoleWriter,true);
        else
            this.consoleWriter = consoleWriter;
	}

    /**
     * Find the methods to start and shutdown the server.
     * Perfomed through reflection so that the engine
     * code is not dependent on the network server code.
     * @param serverClass
     * @throws NoSuchMethodException 
     * @throws SecurityException 
     */
    private void findStartStopMethods(final Class<?> serverClass)
        throws SecurityException, NoSuchMethodException
    {
        // Methods are public so no need for privilege blocks.
        runServerMethod = serverClass.getMethod(
                "blockingStart", new Class[] { java.io.PrintWriter.class});
               
        serverShutdownMethod = serverClass.getMethod(
                "directShutdown", null);
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
        // Load the server class indirectly so that Derby does not require the network code
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
                serverConstructor = AccessController.doPrivileged(
			      new PrivilegedExceptionAction<Constructor>() {
						  public Constructor run() throws NoSuchMethodException, SecurityException
						  {
							  if (listenAddress == null)
								  return serverClass.getConstructor(
                                      new Class[]{String.class, String.class});
							  else
								  return
									  serverClass.getConstructor(new
										  Class[] {java.net.InetAddress.class,
												   Integer.TYPE,
                                                   String.class,
                                                   String.class});
                          }
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
            
            findStartStopMethods(serverClass);
            
			if (listenAddress == null) {
				server = serverConstructor.newInstance(
                    new Object[]{userArg, passwordArg});
            } else {
				server = serverConstructor.newInstance(new Object[]
					{listenAddress, portNumber,
                     userArg, passwordArg});
            }

            serverThread = getMonitor().getDaemonThread( this, "NetworkServerStarter", false);
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
            runServerMethod.invoke( server,
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
				AccessController.doPrivileged(
							      new PrivilegedAction<Object>() {
								  public Object run() {
								      serverThread.interrupt();
								      return null;
								  }
							      });				
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
		listenAddress = null;
		portNumber = -1;
		consoleWriter = null;
		
    } // end of stop
    
    /**
     * Privileged Monitor lookup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ModuleFactory  getMonitor()
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<ModuleFactory>()
             {
                 public ModuleFactory run()
                 {
                     return Monitor.getMonitor();
                 }
             }
             );
    }
}
