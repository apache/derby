/*

   Derby - Class org.apache.derby.impl.drda.DB2jServerImpl

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.drda;

import java.io.IOException;
import java.io.InputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Enumeration;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Locale;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.services.info.ProductGenusNames;
import org.apache.derby.iapi.services.info.ProductVersionHolder;
import org.apache.derby.iapi.services.info.JVMInfo;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.impl.jdbc.EmbedSQLException;
import org.apache.derby.iapi.jdbc.DRDAServerStarter;
import org.apache.derby.iapi.tools.i18n.LocalizedResource;
import org.apache.derby.iapi.tools.i18n.LocalizedOutput;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.util.StringUtil;
import org.apache.derby.drda.NetworkServerControl;

/** 
	
	DB2jServerImpl does all the work for DB2jServer
	@see NetworkServerControl for description

*/
public class DB2jServerImpl {
	private final static int NO_USAGE_MSGS= 12;
	private final static String [] COMMANDS = 
	{"start","shutdown","trace","tracedirectory","ping", 
	 "logconnections", "sysinfo", "runtimeinfo",  "maxthreads", "timeslice"};
	// number of required arguments for each command
	private final static int [] COMMAND_ARGS =
	{0, 0, 1, 1, 0, 1, 0, 0, 1, 1};
	private final static int COMMAND_START = 0;
	private final static int COMMAND_SHUTDOWN = 1;
	private final static int COMMAND_TRACE = 2;
	private final static int COMMAND_TRACEDIRECTORY = 3;
	private final static int COMMAND_TESTCONNECTION = 4;
	private final static int COMMAND_LOGCONNECTIONS = 5;
	private final static int COMMAND_SYSINFO = 6;
	private final static int COMMAND_RUNTIME_INFO = 7;
	private final static int COMMAND_MAXTHREADS = 8;
	private final static int COMMAND_TIMESLICE = 9;
	private final static int COMMAND_PROPERTIES = 10;
	private final static int COMMAND_UNKNOWN = -1;
	private final static String [] DASHARGS =
	{"p","d","u","ld","ea","ep", "b", "h", "s"};
	private final static int DASHARG_PORT = 0;
	private final static int DASHARG_DATABASE = 1;
	private final static int DASHARG_USER = 2;
	private final static int DASHARG_LOADSYSIBM = 3;
	private final static int DASHARG_ENCALG = 4;
	private final static int DASHARG_ENCPRV = 5;
	private final static int DASHARG_BOOTPASSWORD = 6;
	private final static int DASHARG_HOST = 7;
	private final static int DASHARG_SESSION = 8;

	// command protocol version - you need to increase this number each time
	// the command protocol changes 
	private final static int PROTOCOL_VERSION = 1;
	private final static String COMMAND_HEADER = "CMD:";
	private final static String REPLY_HEADER = "RPY:";
	private final static int REPLY_HEADER_LENGTH = REPLY_HEADER.length();
	private final static int OK = 0;
	private final static int WARNING = 1;
	private final static int ERROR = 2;
	private final static int SQLERROR = 3;
	private final static int SQLWARNING = 4;

	private final static String
		DB2J_PROP_STREAM_ERROR_FIELD="derby.stream.error.field";

	private final static String
		DB2J_PROP_STREAM_ERROR_METHOD="derby.stream.error.method";
	private final static String
		DB2J_PROP_STREAM_ERROR_FILE="derby.stream.error.file";

	private final static String DRDA_PROP_MESSAGES = "org.apache.derby.loc.drda.messages";
	private final static String DRDA_PROP_DEBUG = "derby.drda.debug";
	private final static String CLOUDSCAPE_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";

	protected final static String UNEXPECTED_ERR = "Unexpected exception";

	private final static int MIN_MAXTHREADS = -1;
	private final static int MIN_TIMESLICE = -1;
	private final static int USE_DEFAULT = -1;
	private final static int DEFAULT_MAXTHREADS = 0; //for now create whenever needed
	private final static int DEFAULT_TIMESLICE = 0;	//for now never yield

   private final static String DEFAULT_HOST = "localhost";
	private final static String DRDA_MSG_PREFIX = "DRDA_";
	private final static String DEFAULT_LOCALE= "en";
	private final static String DEFAULT_LOCALE_COUNTRY="US";

	// Check up to 10 seconds to see if shutdown occurred
	private final static int SHUTDOWN_CHECK_ATTEMPTS = 20;
	private final static int SHUTDOWN_CHECK_INTERVAL= 500;

	// maximum reply size
	private final static int MAXREPLY = 32767;

	// Application Server Attributes.
	protected static String att_srvclsnm;
	protected final static String ATT_SRVNAM = "NetworkServerControl";

	protected static String att_extnam;
	protected static String att_srvrlslv; 
	protected static String prdId;
	private static String buildNumber;
	// we will use single or mixed, not double byte to reduce traffic on the
	// wire, this is in keeping with JCC
	// Note we specify UTF8 for the single byte encoding even though it can
	// be multi-byte.
	protected final static int CCSIDSBC = 1208; //use UTF8
	protected final static int CCSIDMBC = 1208; //use UTF8
	protected final static String DEFAULT_ENCODING = "UTF8"; // use UTF8 for writing
	protected final static int DEFAULT_CCSID = 1208;
	protected final static byte SPACE_CHAR = 32;
														
	protected final static int DEFAULT_SECURITY_MECHANISM =  CodePoint.SECMEC_USRIDPWD;

	// Application Server manager levels - this needs to be in sync
	// with CodePoint.MGR_CODEPOINTS
	protected final static int [] MGR_LEVELS = { 7, // AGENT
												 4,	// CCSID Manager
												 0, // CNMAPPC not implemented
												 0, // CMNSYNCPT not implemented
												 5, // CMNTCPIP
												 0, // DICTIONARY
												 7, // RDB
												 0, // RSYNCMGR
												 7, // SECMGR
												 7, // SQLAM
												 0, // SUPERVISOR
												 0, // SYNCPTMGR
												 0  // XAMGR
												};
											
	
	protected PrintWriter logWriter;                        // console
	protected PrintWriter cloudscapeLogWriter;              // derby.log
	protected Driver cloudscapeDriver;

	// error types
	private final static int ERRTYPE_SEVERE = 1;
	private final static int ERRTYPE_USER = 2;
	private final static int ERRTYPE_INFO = 3;
	private final static int ERRTYPE_UNKNOWN = -1;

	// command argument information
	private Vector commandArgs = new Vector();
	private String databaseArg;
	private String userArg;
	private String passwordArg;
	private String bootPasswordArg;
	private String encAlgArg;
	private String encPrvArg;
	private String hostArg = DEFAULT_HOST;	
	private InetAddress hostAddress;
	private int sessionArg;

	// Used to debug memory in SanityManager.DEBUG mode
	private memCheck mc;

	// reply buffer
	private byte [] replyBuffer;	
	private int replyBufferCount;	//length of reply
	private int replyBufferPos;		//current position in reply

	//
	// server configuration
	//
	// static values - set at start can't be changed once server has started
	private int	portNumber = NetworkServerControl.DEFAULT_PORTNUMBER;	// port server listens to

	// configurable values
	private String traceDirectory;		// directory to place trace files in
	private Object traceDirectorySync = new Object();// object to use for syncing
	private boolean traceAll;			// trace all sessions
	private Object traceAllSync = new Object();	// object to use for syncing reading
										// and changing trace all
	private Object serverStartSync = new Object();	// for syncing start of server.
	private boolean logConnections;		// log connect and disconnects
	private Object logConnectionsSync = new Object(); // object to use for syncing 
										// logConnections value
	private int minThreads;				// default minimum number of connection threads
	private int maxThreads;				// default maximum number of connection threads
	private Object threadsSync = new Object(); // object to use for syncing reading
										// and changing default min and max threads
	private int timeSlice;				// default time slice of a session to a thread
	private Object timeSliceSync = new Object();// object to use for syncing reading
										// and changing timeSlice

	private boolean keepAlive = true;   // keepAlive value for client socket 
	private int minPoolSize;			//minimum pool size for pooled connections
	private int maxPoolSize;			//maximum pool size for pooled connections
	private Object poolSync = new Object();	// object to use for syning reading

	protected boolean debugOutput = false;
	private boolean cleanupOnStart = false;	// Should we clean up when starting the server?
	private boolean restartFlag = false;

	private String errorLogLocation = null;

	//
	// variables for a client command session
	//
	private Socket clientSocket = null;
	private InputStream clientIs = null;
	private OutputStream clientOs = null;
	private ByteArrayOutputStream byteArrayOs = new ByteArrayOutputStream();
	private DataOutputStream commandOs = new DataOutputStream(byteArrayOs);
	
	private Object shutdownSync = new Object();
	private boolean shutdown;
	private int connNum;		// number of connections since server started
	private ServerSocket serverSocket;
	private DB2jServerImpl serverInstance;
	private LocalizedResource langUtil;
	public String clientLocale;
	ArrayList  localAddresses; // list of local addresses for checking admin
	                              // commands. 

	// open sessions
	private Hashtable sessionTable = new Hashtable();

	// current session
	private Session currentSession;
	// DRDAConnThreads
	private Vector threadList = new Vector();

	// queue of sessions waiting for a free thread - the queue is managed
	// in a simple first come, first serve manner - no priorities
	private Vector runQueue = new Vector();

	// number of DRDAConnThreads waiting for something to do
	private int freeThreads;

	// known application requesters
	private Hashtable appRequesterTable = new Hashtable();

	// accessed by inner classes for privileged action
	private String propertyFileName;
	private Runnable acceptClients;
	

	

	// constructor
	public DB2jServerImpl() throws Exception
	{
		getPropertyInfo();
		init();
    }


	/**
	 * Internal constructor for NetworkServerControl API. 
	 * @ param address - InetAddress to listen on, May not be null. 
	 * Throws NPE if null
	 * @ portNumber - portNumber to listen on, -1 use propert or default.
	 * @ throw Exception on error
	 * @see NetworkServerControl
	 */
	public DB2jServerImpl(InetAddress address, int portNumber) throws Exception
	{
		getPropertyInfo();
		this.hostAddress = address;
		this.portNumber = (portNumber <= 0) ?
			this.portNumber: portNumber;
		this.hostArg = address.getHostAddress();
		init();
	}

    private void init() throws Exception
    {

        // adjust the application in accordance with derby.ui.locale and derby.ui.codeset
		langUtil = new LocalizedResource(null,null,DRDA_PROP_MESSAGES);

		serverInstance = this;
		
		//set Server attributes to be used in EXCSAT
		ProductVersionHolder myPVH = getNetProductVersionHolder();
		att_extnam = ATT_SRVNAM + " " + java.lang.Thread.currentThread().getName();
		
		att_srvclsnm = myPVH.getProductName();

		String majorStr = String.valueOf(myPVH.getMajorVersion());
		String minorStr = String.valueOf(myPVH.getMinorVersion());
		// Maintenance version. Server protocol version.
		// Only changed if client needs to recognize a new server version.
		String drdaMaintStr = String.valueOf(myPVH.getDrdaMaintVersion());

		// PRDID format as JCC expects it: CSSMMmx
		// MM = major version
		// mm = minor version
		// x = drda MaintenanceVersion

		prdId = "CSS";
		if (majorStr.length() == 1)
			prdId += "0";
		prdId += majorStr;

		if (minorStr.length() == 1)
			prdId += "0";

		prdId += minorStr;
		
		prdId += drdaMaintStr;
		att_srvrlslv = prdId + "/" + myPVH.getVersionBuildString(false);
		
		if (SanityManager.DEBUG)
		{
			if (majorStr.length() > 2  || 
				minorStr.length() > 2 || 
				drdaMaintStr.length() > 1)
				SanityManager.THROWASSERT("version values out of expected range  for PRDID");
		}

		buildNumber = myPVH.getBuildNumber();
	}

    private PrintWriter makePrintWriter( OutputStream out)
    {
		if (out != null)
			return new PrintWriter(out, true /* flush the buffer at the end of each line */);
		else
			return null;
    }
	/********************************************************************************
	 * Implementation of NetworkServerControl API
	 * The server commands throw exceptions for errors, so that users can handle
	 * them themselves in addition to having the errors written to the console
	 * and possibly DB2j.log.  To turn off logging the errors to the console,
	 * set the output writer to null.
	 ********************************************************************************/


	/**
	 * Set the output stream for console messages
	 * If this is set to null, no messages will be written to the console
	 *
	 * @param outStream	output stream for console messages
	 */
	public void setLogWriter(PrintWriter outWriter)
	{
		logWriter = outWriter;
    }


	
	/**
	 * Write an error message to console output stream
	 * and throw an exception for this error
	 *
	 * @param msg	error message
	 * @exception Exception
	 */
	public void consoleError(String msg)
		throws Exception
	{
		consoleMessage(msg);
		throw new Exception(msg);
	}

	/**
	 * Write an exception to console output stream,
	 * but only if debugOutput is true.
	 *
	 * @param e	exception 
	 */
	public void consoleExceptionPrint(Exception e)
	{
		if (debugOutput == true)
			consoleExceptionPrintTrace(e);

		return;
	}

	/**
	 * Write an exception (with trace) to console
	 * output stream.
	 *
	 * @param e	exception 
	 */
	public void consoleExceptionPrintTrace(Throwable e)
	{
		consoleMessage(e.getMessage());
		if (logWriter != null)
		{
			synchronized (logWriter) {
				e.printStackTrace(logWriter);
			}
		}
		else
		{
			e.printStackTrace();
		}
		
		if (cloudscapeLogWriter != null)
		{
			synchronized(cloudscapeLogWriter) {
				e.printStackTrace(cloudscapeLogWriter);
			}
		}
	}




	/**
	 * Write a message to console output stream
	 *
	 * @param msg	message
	 */
	public void consoleMessage(String msg)
	{
		// print to console if we have one
		if (logWriter != null)
		{
			synchronized(logWriter) {
				logWriter.println(msg);
			}
		}
		// always print to derby.log
		if (cloudscapeLogWriter != null)
			synchronized(cloudscapeLogWriter)
			{
				Monitor.logMessage(msg);
			}
	}



	/**
	 * Start a network server.  Launches a separate thread with 
	 * DRDAServerStarter.  Want to use Monitor.startModule,
	 * so it can all get shutdown when cloudscape shuts down, but 
	 * can't get it working right now.
	 *
	 * @param consoleWriter   PrintWriter to which server console will be 
	 *                        output. Null will disable console output.
	 *
	 *		   
	 * @exception Exception	throws an exception if an error occurs
	 */
	public void start(PrintWriter consoleWriter)
		throws Exception
	{
		DRDAServerStarter starter = new DRDAServerStarter();
		starter.setStartInfo(hostAddress,portNumber,consoleWriter);
		startDB2j();
		starter.boot(false,null);
	}


	/**
	 * Start a network server
	 *
	 * @param consoleWriter   PrintWriter to which server console will be 
	 *                        output. Null will disable console output.
	 *
	 *		   
	 * @exception Exception	throws an exception if an error occurs
	 */
	public void blockingStart(PrintWriter consoleWriter)
		throws Exception
	{
		startDB2j();
		setLogWriter(consoleWriter);
		cloudscapeLogWriter = Monitor.getStream().getPrintWriter();
		if (SanityManager.DEBUG && debugOutput)
		{
			memCheck.showmem();
			mc = new memCheck(200000);
			mc.start();
		}
        // Open a server socket listener      
	    try{
	    	serverSocket = (ServerSocket) AccessController.doPrivileged(
								new PrivilegedExceptionAction() {
										public Object run() throws IOException,UnknownHostException
										{
											if (hostAddress == null)
												hostAddress = InetAddress.getByName(hostArg);
											// Make a list of valid
											// InetAddresses for NetworkServerControl
											// admin commands.
											buildLocalAddressList(hostAddress);
											return new ServerSocket(portNumber
																	,0,
																	hostAddress);
										}
									}
								);
		} catch (PrivilegedActionException e) {
			Exception e1 = e.getException();
	    	if (e1 instanceof IOException)
            	consolePropertyMessage("DRDA_ListenPort.S", 
									   new String [] {
										   Integer.toString(portNumber), 
										   hostArg}); 
			if (e1 instanceof UnknownHostException) {
				consolePropertyMessage("DRDA_UnknownHost.S", hostArg);
			}
			else
				throw e1;
		} catch (Exception e) {
		// If we find other (unexpected) errors, we ultimately exit--so make
		// sure we print the error message before doing so (Beetle 5033).
			throwUnexpectedException(e);
		}

		consolePropertyMessage("DRDA_Ready.I", Integer.toString(portNumber));

		// We accept clients on a separate thread so we don't run into a problem
		// blocking on the accept when trying to process a shutdown
		acceptClients = (Runnable)new ClientThread(this, serverSocket);
		Thread clientThread =  (Thread) AccessController.doPrivileged(
								new PrivilegedExceptionAction() {
									public Object run() throws Exception
									{
										return new Thread(acceptClients);
									}
								}
							);
		clientThread.start();
			
		// wait until we are told to shutdown or someone sends an InterruptedException
        synchronized(shutdownSync) {
            try {
				shutdownSync.wait();
            }
            catch (InterruptedException e)
            {
                shutdown = true;
            }
        }

		// Need to interrupt the memcheck thread if it is sleeping.
		if (mc != null)
			mc.interrupt();

		//interrupt client thread
		clientThread.interrupt();

 		// Close out the sessions
 		synchronized(sessionTable) {
 			for (Enumeration e = sessionTable.elements(); e.hasMoreElements(); )
 			{	
 				Session session = (Session) e.nextElement();
 				session.close();
 			}
 		}

		synchronized (threadList)
		{
 			//interupt any connection threads still active
 			for (int i = 0; i < threadList.size(); i++)
 			{
 				((DRDAConnThread)threadList.get(i)).close();
 				((DRDAConnThread)threadList.get(i)).interrupt();
 			}
 			threadList.clear();
		}
	   
 

	
	    // close the listener socket
	    try{
	       serverSocket.close();
	    }catch(IOException e){
			consolePropertyMessage("DRDA_ListenerClose.S");
	    }


		// Wake up those waiting on sessions, so
		// they can close down
		synchronized (runQueue) {
			runQueue.notifyAll();
		}						

		/*
		// Shutdown Cloudscape
		try {
			if (cloudscapeDriver != null)
				cloudscapeDriver.connect("jdbc:derby:;shutdown=true", 
										 (Properties) null);
		} catch (SQLException sqle) {
			// If we can't shutdown cloudscape. Perhaps authentication is
			// set to true or some other reason. We will just print a
			// message to the console and proceed.
			if (((EmbedSQLException)sqle).getMessageId() !=
			  SQLState.CLOUDSCAPE_SYSTEM_SHUTDOWN)
				consolePropertyMessage("DRDA_ShutdownWarning.I",
									   sqle.getMessage());
		}
		*/


		consolePropertyMessage("DRDA_ShutdownSuccess.I");
		

    }
	
	/** 
	 * Load Cloudscape and save driver for future use.
	 * We can't call Driver Manager when the client connects, 
	 * because they might be holding the DriverManager lock.
	 *
	 * 
	 */

	


	protected void startDB2j() throws Exception
	{

		// we start the cloudscape server here.
		boolean restartCheck = this.restartFlag;
		synchronized (serverStartSync) {

			if (restartCheck == this.restartFlag) {
			// then we can go ahead and restart the server (odds
			// that some else has just done so are very slim (but not
			// impossible--however, even if it does happen, things
			// should still work correctly, just not as efficiently...))

				try {
	
					if (cleanupOnStart) {
					// we're restarting the server (probably after a shutdown
					// exception), so we need to clean up first.

						// Close and remove sessions on runQueue.
						for (int i = 0; i < runQueue.size(); i++)
							((Session)runQueue.get(i)).close();
						runQueue.clear();

						// Close and remove DRDAConnThreads on threadList.
						for (int i = 0; i < threadList.size(); i++)
							((DRDAConnThread)threadList.get(i)).close();
						threadList.clear();
						freeThreads = 0;

						// Unload driver, then restart the server.
						cloudscapeDriver = null;	// so it gets collected.
						System.gc();
					}

					// start the server.
					Class.forName(CLOUDSCAPE_DRIVER).newInstance();
					cloudscapeDriver = DriverManager.getDriver(Attribute.PROTOCOL);

				}
				catch (Exception e) {
					consolePropertyMessage("DRDA_LoadException.S", e.getMessage());
				}
				cleanupOnStart = true;
				this.restartFlag = !this.restartFlag;
			}
			// else, multiple threads hit this synchronize block at the same
			// time, but one of them already executed it--so all others just
			// return and do nothing (no need to restart the server multiple
			// times in a row).
		}
	}

	/**
	 * Shutdown a network server
	 *
	 * @param host		machine network server is running on, if null, localhost is used
	 * @param portNumber	port number server is to use, if <= 0, default port number
	 *			is used
	 *
	 * @exception Exception	throws an exception if an error occurs
	 */
	public void shutdown()
		throws Exception
	{
		setUpSocket();
		writeCommandHeader(COMMAND_SHUTDOWN);
		send();
		readResult();
		// Wait up to 10 seconds for things to really shut down
		// need a quiet ping so temporarily disable the logwriter
		PrintWriter savWriter = logWriter;
		setLogWriter(null);
		int ntry;
		for (ntry = 0; ntry < SHUTDOWN_CHECK_ATTEMPTS; ntry++)
		{
			Thread.sleep(SHUTDOWN_CHECK_INTERVAL);
			try {
				ping();
			} catch (Exception e) 
			{
				// as soon as we can't ping return
				if (ntry == SHUTDOWN_CHECK_ATTEMPTS)
					consolePropertyMessage("DRDA_ShutdownError.S", new String [] {
						Integer.toString(portNumber), 
						hostArg}); 
				break;
			}
		}
		logWriter= savWriter;
		return;
	}

	/*
	 Shutdown the server directly (If you have the original object)
	 No Network communication needed.
	*/
	public void directShutdown() 	{
		shutdown = true;
		synchronized(shutdownSync) {						
			// wake up the server thread
			shutdownSync.notifyAll();
		}
		
	}


	/**
	 */
	public boolean isServerStarted() throws Exception
	{
		try {
			ping();
		}
		catch (Exception e) {
			return false;
		}
		return true;
	}

	public void ping() throws Exception
	{
		// database no longer used, but don't change the protocol 
		// in case we add
		// authorization  later.
		String database = null; // no longer used but don't change the protocol
		String user = null;
		String password = null;

			setUpSocket();
			writeCommandHeader(COMMAND_TESTCONNECTION);
			writeLDString(database);
			writeLDString(user);
			writeLDString(password);
			send();
			readResult();

	}


	/**
	 * Turn tracing on or off for all sessions
	 *
	 * @param on			true to turn tracing on, false to turn tracing off
	 * @param host		machine network server is running on, if null, localhost is used
	 * @param portNumber	port number server is to use, if <= 0, default port number
	 *			is used
	 *
	 * @exception Exception	throws an exception if an error occurs
	 */
	public void trace(boolean on)
		throws Exception
	{
		trace(0, on);
	}

	/**
	 * Turn tracing on or off for one session or all sessions
	 *
	 * @param connNum	the connNum of the session, 0 if all sessions
	 * @param on			true to turn tracing on, false to turn tracing off
	 *
	 * @exception Exception	throws an exception if an error occurs
	 */
	public void trace(int connNum, boolean on)
		throws Exception
	{
		setUpSocket();
		writeCommandHeader(COMMAND_TRACE);
		commandOs.writeInt(connNum);
		writeByte(on ? 1 : 0);
		send();
		readResult();
		consoleTraceMessage(connNum, on);
	}

	/**
	 * Print trace change message to console
	 *
	 * @param on			true to print tracing on, false to print tracing off
	 *
	 * @exception Exception	throws an exception if an error occurs
	 */
	private void consoleTraceMessage(int connNum, boolean on)
		throws Exception
	{
		if (connNum == 0)
			consolePropertyMessage("DRDA_TraceChangeAll.I", on ? "DRDA_ON.I" : "DRDA_OFF.I");
		else
		{
			String[] args = new String[2];
			args[0] = on ? "DRDA_ON.I" : "DRDA_OFF.I";
			args[1] = new Integer(connNum).toString();
			consolePropertyMessage("DRDA_TraceChangeOne.I", args);
		}
	}

	/**
	 * Turn logging connections on or off. When logging is turned on a message is
	 * written to DB2j.log each time a connection connects or disconnects.
	 *
	 * @param on			true to turn on, false to turn  off
	 * @param host		machine network server is running on, if null, localhost is used
	 * @param portNumber	port number server is to use, if <= 0, default port number
	 *			is used
	 *
	 * @exception Exception	throws an exception if an error occurs
	 */
	public void logConnections(boolean on)
		throws Exception
	{
		setUpSocket();
		writeCommandHeader(COMMAND_LOGCONNECTIONS);
		writeByte(on ? 1 : 0);
		send();
		readResult();
	}

	/**
	 *@see NetworkServerControl#setTraceDirectory
	 */
	public void sendSetTraceDirectory(String traceDirectory)
		throws Exception
	{
		setUpSocket();
		writeCommandHeader(COMMAND_TRACEDIRECTORY);
		writeLDString(traceDirectory);
		send();
		readResult();
	}

	/**
	 *@see NetworkServerControl#getSysinfo
	 */
	public String sysinfo()
		throws Exception
	{
		setUpSocket();
		writeCommandHeader(COMMAND_SYSINFO);
		send();
		return readStringReply("DRDA_SysInfoError.S");
	}

	/**
	 *@see NetworkServerControl#runtimeinfo
	 */
	public String runtimeInfo()
	throws Exception 
	{
		setUpSocket();
		writeCommandHeader(COMMAND_RUNTIME_INFO);
		send();
		return readStringReply("DRDA_RuntimeInfoError.S");
	}

	/**
	 * Display usage information
	 *
	 */
	public void usage()
	{
		try {
		for (int i = 1; i <= NO_USAGE_MSGS; i++)
			consolePropertyMessage("DRDA_Usage"+i+".I");
		} catch (Exception e) {}	// ignore exceptions - there shouldn't be any
	}

	/**
	 * Set connection pool parameters for a database
	 *
	 * @param database	database parameters applied to
	 * @param min		minimum number of connections, if 0, pooled connections not used
	 *						if -1, use default						
	 * @param max		maximum number of connections, if 0, pooled connections 
	 *						created when no free connection available, if -1, 
	 *						use default
	 * @param host		machine network server is running on, if null, localhost is used
	 * @param portNumber	port number server is to use, if <= 0, default port number
	 *			is used
	 *
	 * @exception Exception	throws an exception if an error occurs
	 */
	protected void setConnectionPoolParameters(String database, int min, int max,
		String host, int portNumber)
		throws Exception
	{
		consolePropertyMessage("DRDA_NotImplemented.S", "conpool");
	}

	/**
	 * Set default connection pool parameters 
	 *
	 * @param min		minimum number of connections, if 0, pooled connections not used
	 *						if -1, use default
	 * @param max		maximum number of connections, if 0, pooled connections 
	 *						created when no free connection available
	 *						if -1, use default
	 * @param host		machine network server is running on, if null, localhost is used
	 * @param portNumber	port number server is to use, if <= 0, default port number
	 *			is used
	 *
	 * @exception Exception	throws an exception if an error occurs
	 */
	protected void setConnectionPoolParameters(int min, int max, String host, 
			int portNumber) throws Exception
	{
		consolePropertyMessage("DRDA_NotImplemented.S", "conpool");
	}

	/**
	 * Connect to  network server and set connection maxthread parameter
	 *
	 * @param max		maximum number of connections, if 0, connections 
	 *						created when no free connection available
	 *						if -1, use default
	 *
	 * @exception Exception	throws an exception if an error occurs
	 */
	public void netSetMaxThreads(int max) throws Exception
	{
		setUpSocket();
		writeCommandHeader(COMMAND_MAXTHREADS);
		commandOs.writeInt(max);
		send();
		readResult();
		int newval = readInt();
		consolePropertyMessage("DRDA_MaxThreadsChange.I", 
 					new Integer(newval).toString());
	}

	/**
	 * Set network server connection timeslice parameter
	 *
	 * @param timeslice	amount of time given to each session before yielding to 
	 *						another session, if 0, never yield. if -1, use default.
	 *
	 * @exception Exception	throws an exception if an error occurs
	 */
	public void netSetTimeSlice(int timeslice)
		throws Exception
	{
		setUpSocket();
		writeCommandHeader(COMMAND_TIMESLICE);
		commandOs.writeInt(timeslice);
		send();
		readResult();
		int newval = readInt();
		consolePropertyMessage("DRDA_TimeSliceChange.I", 
									   new Integer(newval).toString());
	}

	/**
	 * Get current properties
	 *
	 * @return Properties object containing properties
	 * @exception Exception	throws an exception if an error occurs
	 */
	public Properties getCurrentProperties() 
		throws Exception
	{
		setUpSocket();
		writeCommandHeader(COMMAND_PROPERTIES);
		send();
		byte [] val = readBytesReply("DRDA_PropertyError.S");
		Properties p = new Properties();
		try {
			ByteArrayInputStream bs = new ByteArrayInputStream(val);
			p.load(bs);
		} catch (IOException io) {
			consolePropertyMessage("DRDA_IOException.S", 
						io.getMessage());
		}
		return p;
	}

	/**
	 * Set a thread name to be something that is both meaningful and unique (primarily
	 * for debugging purposes).
	 *
	 * @param thrd An instance of a Thread object that still has its default
	 *  thread name (as generated by the jvm Thread constructor).  This should
	 *  always be of the form "Thread-N", where N is a unique thread id
	 *  generated by the jvm.  Ex. "Thread-0", "Thread-1", etc.
	 *
	 * @return The received thread's name has been set to a new string of the form
	 *  [newName + "_n"], where 'n' is a unique thread id originally generated
	 *  by the jvm Thread constructor.  If the default name of the thread has
	 *  been changed before getting here, then nothing is done.
	 *
	 **/
	public static void setUniqueThreadName(Thread thrd, String newName) {

		// First, pull off the unique thread id already found in thrd's default name;
		// we do so by searching for the '-' character, and then counting everything
		// after it as a N.
		if (thrd.getName().indexOf("Thread-") == -1) {
		// default name has been changed; don't do anything.
			return;
		}
		else {
			String oldName = thrd.getName();
			thrd.setName(newName + "_" +
			  oldName.substring(oldName.indexOf("-")+1, oldName.length()));
		} // end else.

		return;

	}

	/*******************************************************************************/
	/*        Protected methods                                                    */
	/*******************************************************************************/
	/**
	 * Remove session from session table
	 *
	 * @param sessionid 	id of session to be removed
	 */
	protected void removeFromSessionTable(int sessionid)
	{
		sessionTable.remove(new Integer(sessionid));
	}

	/**
	 * 	processCommands reads and processes DB2jServerImpl commands sent
	 * 	to the network server over the socket.  The protocol used is
	 * 		4 bytes 	- String CMD:
	 * 		2 bytes		- Protocol version
	 *		1 byte		- length of locale (0 for default)
	 *			n bytes - locale
	 *		1 byte		- length of codeset (0 for default)
	 *			n bytes - codeset
	 * 		1 byte		- command
	 * 		n bytes		- parameters for the command
	 * 	The server returns
	 *		4 bytes		- String RPY:
	 *	for most commands
	 *		1 byte		- command result, 0 - OK, 1 - warning, 2 - error
	 *	 if warning or error
	 *		1 bytes		- length of message key
	 *		n bytes		- message key
	 *		1 byte		- number of parameters to message
	 *		{2 bytes		- length of parameter
	 *		n bytes		- parameter} for each parameter
	 *  for sysinfo
	 *		1 byte		- command result, 0 - OK, 1 - warning, 2 - error
	 *   if OK 
	 *		2 bytes		- length of sysinfo
	 *		n bytes		- sysinfo
	 *		
	 * 		
	 * 	Note, the 3rd byte of the command must not be 'D0' to distinquish it 
	 *	from DSS structures.
	 * 	The protocol for the parameters for each command follows:
	 *
	 * 	Command: trace <connection id> {on | off}
	 * 	Protocol:
	 * 		4 bytes		- connection id - connection id of 0 means all sessions
	 * 		1 byte		- 0 off, 1 on
	 * 
	 * 	Command: logConnections {on | off}
	 * 	Protocol:
	 * 		1 byte		- 0 off, 1 on
	 * 
	 * 	Command: shutdown
	 * 	No parameters
	 * 
	 * 	Command: sysinfo
	 * 	No parameters
	 * 
	 * 	Command: dbstart
	 * 	Protocol:
	 * 		2 bytes		- length of database name
	 * 		n bytes		- database name
	 * 		2 bytes		- length of boot password
	 * 		n bytes		- boot password 
	 * 		2 bytes		- length of encryption algorithm
	 * 		n bytes		- encryption algorithm
	 * 		2 bytes		- length of encryption provider
	 * 		n bytes		- encryption provider
	 * 		2 bytes		- length of user name
	 * 		n bytes		- user name
	 * 		2 bytes		- length of password
	 * 		n bytes		- password
	 *
	 * 	Command: dbshutdown
	 * 	Protocol:
	 * 		2 bytes		- length of database name
	 * 		n bytes		- database name
	 * 		2 bytes		- length of user name
	 * 		n bytes		- user name
	 * 		2 bytes		- length of password
	 * 		n bytes		- password
	 *
	 * 	Command: connpool
	 * 	Protocol:
	 * 		2 bytes		- length of database name, if 0, default for all databases
	 *						is set
	 * 		n bytes		- database name
	 *		2 bytes		- minimum number of connections, if 0, connection pool not used
	 *						if value is -1 use default
	 *		2 bytes		- maximum number of connections, if 0, connections are created
	 *						as needed, if value is -1 use default
	 *
	 * 	Command: maxthreads
	 * 	Protocol:
	 *		2 bytes		- maximum number of threads
	 *
	 * 	Command: timeslice 
	 * 	Protocol:
	 *		4 bytes		- timeslice value
	 *
	 * 	Command: tracedirectory
	 * 	Protocol:
	 * 		2 bytes		- length of directory name
	 * 		n bytes		- directory name
	 *
	 *	Command: test connection
	 * 	Protocol:
	 * 		2 bytes		- length of database name if 0, just the connection
	 *						to the network server is tested and user name and 
	 *						password aren't sent
	 * 		n bytes		- database name
	 * 		2 bytes		- length of user name (optional)
	 * 		n bytes		- user name
	 * 		2 bytes		- length of password  (optional)
	 * 		n bytes		- password
	 *
	 *	The calling routine is synchronized so that multiple threads don't clobber each
	 * 	other. This means that configuration commands will be serialized.
	 * 	This shouldn't be a problem since they should be fairly rare.
	 * 		
	 * @param reader	input reader for command
	 * @param writer output writer for command
	 * @param session	session information
	 *
	 * @exception Throwable	throws an exception if an error occurs
	 */
	protected synchronized void processCommands(DDMReader reader, DDMWriter writer, 
		Session session) throws Throwable
	{
		try {
			String protocolStr = reader.readCmdString(4);
			String locale = DEFAULT_LOCALE;
			String codeset = null;
			// get the version
			int version = reader.readNetworkShort();
			if (version <= 0 || version > PROTOCOL_VERSION)
				throw new Throwable(langUtil.getTextMessage("DRDA_UnknownProtocol.S",  new Integer(version).toString()));
			int localeLen = reader.readByte();
			if (localeLen > 0)
			{
				currentSession = session;
				locale = reader.readCmdString(localeLen);
				session.langUtil = new LocalizedResource(codeset,locale,DRDA_PROP_MESSAGES);
			}
			String notLocalMessage = null;
			// for now codesetLen is always 0
			int codesetLen = reader.readByte();
			int command = reader.readByte();
			if (command !=  COMMAND_TESTCONNECTION)
			{
				try {
					checkAddressIsLocal(session.clientSocket.getInetAddress());
				}catch (Exception e)
				{
					notLocalMessage = e.getMessage();
				}
			}
			if (notLocalMessage != null)
			{
				sendMessage(writer, ERROR,notLocalMessage);
				session.langUtil = null;
				currentSession = null;
				return;
			}

			switch(command)
			{
				case COMMAND_SHUTDOWN:
					sendOK(writer);
					directShutdown();
					break;
				case COMMAND_TRACE:
					sessionArg = reader.readNetworkInt();
					boolean on = (reader.readByte() == 1);
					if (setTrace(on))
					{
						sendOK(writer);
					}
					else
					{
						sendMessage(writer, ERROR,  
							localizeMessage("DRDA_SessionNotFound.U", 
							(session.langUtil == null) ? langUtil : session.langUtil,
							new String [] {new Integer(sessionArg).toString()}));
					}
					break;
				case COMMAND_TRACEDIRECTORY:
					setTraceDirectory(reader.readCmdString());
					sendOK(writer);
					consolePropertyMessage("DRDA_TraceDirectoryChange.I", traceDirectory);
					break;
				case COMMAND_TESTCONNECTION:
					databaseArg = reader.readCmdString();
					userArg = reader.readCmdString();
					passwordArg = reader.readCmdString();
					if (databaseArg != null)
						connectToDatabase(writer, databaseArg, userArg, passwordArg);
					else
						sendOK(writer);
					break;
				case COMMAND_LOGCONNECTIONS:
					boolean log = (reader.readByte() == 1);
					setLogConnections(log);
					sendOK(writer);
					consolePropertyMessage("DRDA_LogConnectionsChange.I",
						(log ? "DRDA_ON.I" : "DRDA_OFF.I"));
					break;
				case COMMAND_SYSINFO:
					sendSysInfo(writer);
					break;
				case COMMAND_PROPERTIES:
					sendPropInfo(writer);
					break;
				case COMMAND_RUNTIME_INFO:
					sendRuntimeInfo(writer);
					break;
				case COMMAND_MAXTHREADS:
					int max = reader.readNetworkInt();
					try {
						setMaxThreads(max);
					}catch (Exception e) {
						sendMessage(writer, ERROR, e.getMessage());
						return;
					}
					int newval = getMaxThreads();
					sendOKInt(writer, newval);
					consolePropertyMessage("DRDA_MaxThreadsChange.I", 
						new Integer(newval).toString());
					break;
				case COMMAND_TIMESLICE:
					int timeslice = reader.readNetworkInt();
					try {
						setTimeSlice(timeslice);
					}catch (Exception e) {
						sendMessage(writer, ERROR, e.getMessage());
						return;
					}
					newval = getTimeSlice();
					sendOKInt(writer, newval);
					consolePropertyMessage("DRDA_TimeSliceChange.I", 
						new Integer(newval).toString());
					break;
			}
		} catch (DRDAProtocolException e) {
			//we need to handle this since we aren't in DRDA land here
			consoleExceptionPrintTrace(e);

		} catch (Exception e) {
			consoleExceptionPrintTrace(e);
		}
		finally {
			session.langUtil = null;
			currentSession = null;
		}
	}
	/**
	 * Get the next session for the thread to work on
	 * Called from DRDAConnThread after session completes or timeslice
	 * exceeded.   
	 *
	 * If there is a waiting session, pick it up and put currentSession 
	 * at the back of the queue if there is one.
	 * @param currentSession	session thread is currently working on
	 *
	 * @return  next session to work on, could be same as current session
	 */
	protected Session getNextSession(Session currentSession)
	{
		Session retval = null;
		if (shutdown == true)
			return retval;
		synchronized (runQueue)
		{
			try {
				// nobody waiting - go on with current session
				if (runQueue.size() == 0)
				{
					// no current session - wait for some work
					if (currentSession == null)
					{
						while (runQueue.size() == 0)
						{
							// This thread has nothing to do now so 
							// we will add it to freeThreads
							freeThreads++;
							runQueue.wait();
							if (shutdown == true)
								return null;
							freeThreads--;
						}
					}
					else
						return currentSession;
				}
				retval = (Session) runQueue.elementAt(0);
				runQueue.removeElementAt(0);
				if (currentSession != null)
					runQueueAdd(currentSession);
			} catch (InterruptedException e) {
			// If for whatever reason (ex. database shutdown) a waiting thread is
			// interrupted while in this method, that thread is going to be
			// closed down, so we need to decrement the number of threads
			// that will be available for use.
				freeThreads--;
			}
		}
		return retval;
	}
	/**
	 * Get the stored application requester or store if we haven't seen it yet
	 *
	 * @param appRequester Application Requester to look for
	 *
	 * @return  stored application requester
	 */
	protected AppRequester getAppRequester(AppRequester appRequester)
	{
		AppRequester s = null;

		if (SanityManager.DEBUG) {
			if (appRequester == null)
				SanityManager.THROWASSERT("null appRequester in getAppRequester");
		}

		if (!appRequesterTable.isEmpty())
			s = (AppRequester)appRequesterTable.get(appRequester.prdid);

		if (s == null)
		{
			appRequesterTable.put(appRequester.prdid, appRequester);
			return appRequester;
		}
		else
		{
			//compare just in case there are some differences
			//if they are different use the one we just read in
			if (s.equals(appRequester))
				return s;
			else
				return appRequester;
		}
	}
	/**
	 * Get the server manager level for a given manager
	 *
	 * @param manger codepoint for manager
	 * @return manager level
	 */
	protected int getManagerLevel(int manager)
	{
		int mindex = CodePoint.getManagerIndex(manager);
		if (SanityManager.DEBUG) {
			if (mindex == CodePoint.UNKNOWN_MANAGER)
			SanityManager.THROWASSERT("manager out of bounds");
		}
		return MGR_LEVELS[mindex];
	}
	/**
	 * Check whether a CCSID code page is supported
	 *
	 * @param CCSID to check
	 * @return true if supported; false otherwise
	 */
	protected boolean supportsCCSID(int ccsid)
	{
		try {
			CharacterEncodings.getJavaEncoding(ccsid);
			}
		catch (Exception e) {
			return false;
		}
		return true;
	}
	/**
	 * Put property message on console
	 *
	 * @param msgProp		message property key
	 *
	 * @exception throws an Exception if an error occurs
	 */
	protected void consolePropertyMessage(String msgProp)
		throws Exception
	{
		consolePropertyMessageWork(msgProp, null);
	}
	/**
	 * Put property message on console
	 *
	 * @param msgProp		message property key
	 * @param arg			argument for message
	 *
	 * @exception throws an Exception if an error occurs
	 */
	protected void consolePropertyMessage(String msgProp, String arg)
		throws Exception
	{
		consolePropertyMessageWork(msgProp, new String [] {arg});
	}
	/**
	 * Put property message on console
	 *
	 * @param msgProp		message property key
	 * @param args			argument array for message
	 *
	 * @exception throws an Exception if an error occurs
	 */
	protected void consolePropertyMessage(String msgProp, String [] args)
		throws Exception
	{
		consolePropertyMessageWork(msgProp, args);
	}
	/**
	 * Is this the command protocol
	 * 
	 * @param  val
	 */
	protected static boolean isCmd(String val)
	{
		if (val.equals(COMMAND_HEADER))
			return true;
		else
			return false;
	}

	/*******************************************************************************/
	/*        Private methods                                                      */
	/*******************************************************************************/
	/**
	 * Write Command reply
	 *
	 * @param writer	writer to use 
	 *
	 * @exception	throws Exception if a problem occurs sending OK
	 */
	private void writeCommandReplyHeader(DDMWriter writer) throws Exception
	{
		writer.setCMDProtocol();
		writer.writeString(REPLY_HEADER);
	}
	 
	/**
	 * Send OK from server to client after processing a command
	 *
	 * @param writer	writer to use for sending OK
	 *
	 * @exception	throws Exception if a problem occurs sending OK
	 */
	private void sendOK(DDMWriter writer) throws Exception
	{
		writeCommandReplyHeader(writer);
		writer.writeByte(OK);
		writer.flush();
	}
	/**
	 * Send OK and int value
	 *
	 * @param writer writer to use for sending
	 * @param val 	int val to send
	 * 
	 * @exception throws Exception if a problem occurs
	 */
	private void sendOKInt(DDMWriter writer, int val) throws Exception
	{
		writeCommandReplyHeader(writer);
		writer.writeByte(OK);
		writer.writeNetworkInt(val);
		writer.flush();
	}
	/**
	 * Send Error or Warning from server to client after processing a command
	 *
	 * @param writer	writer to use for sending message
	 * @param messageType	1 for Warning, 2 for Error 3 for SQLError
	 * @param message 	message 
	 *
	 * @exception	throws Exception if a problem occurs sending message
	 */
	private void sendMessage(DDMWriter writer, int messageType, String message) 
		throws Exception
	{
		writeCommandReplyHeader(writer);
		writer.writeByte(messageType);
		writer.writeLDString(message);
		writer.flush();
	}
	/**
	 * Send SQL Exception from server to client after processing a command
	 *
	 * @param writer	writer to use for sending message
	 * @param se		Cloudscape exception
	 * @param type		type of exception, SQLERROR or SQLWARNING
	 *
	 * @exception	throws Exception if a problem occurs sending message
	 */
	private void sendSQLMessage(DDMWriter writer, SQLException se, int type)
		throws Exception
	{
		StringBuffer locMsg = new StringBuffer();
		//localize message if necessary
		while (se != null)
		{
			if (currentSession != null && currentSession.langUtil != null)
			{
				locMsg.append(se.getSQLState()+":"+ 
					MessageService.getLocalizedMessage(
					currentSession.langUtil.getLocale(), ((EmbedSQLException)se).getMessageId(), 
					((EmbedSQLException)se).getArguments()));
			}
			else
				locMsg.append(se.getSQLState()+":"+se.getMessage());
			se = se.getNextException();
			if (se != null)
				locMsg.append("\n");
		}
		sendMessage(writer, type, locMsg.toString());
	}
	/**
	 * Send SysInfo information from server to client
	 *
	 * @param writer 	writer to use for sending sysinfo
	 *
	 * @exception throws Exception if a problem occurs sending value
	 */
	private void sendSysInfo(DDMWriter writer) throws Exception
	{
		StringBuffer sysinfo = new StringBuffer();
		sysinfo.append(getNetSysInfo());
		sysinfo.append(getCLSSysInfo());
		try {
			writeCommandReplyHeader(writer);
			writer.writeByte(0);	//O.K.
			writer.writeLDString(sysinfo.toString());
		} catch (DRDAProtocolException e) {
			consolePropertyMessage("DRDA_SysInfoWriteError.S", e.getMessage());
		}
		writer.flush();
	}
	
	/**
	 * Send RuntimeInfo information from server to client
	 *
	 * @param writer 	writer to use for sending sysinfo
	 *
	 * @exception throws Exception if a problem occurs sending value
	 */
	private void sendRuntimeInfo(DDMWriter writer) throws Exception
	{
		try {
			writeCommandReplyHeader(writer);
			writer.writeByte(0);	//O.K.
			writer.writeLDString(getRuntimeInfo());
				} catch (DRDAProtocolException e) {
			consolePropertyMessage("DRDA_SysInfoWriteError.S", e.getMessage());
		}
		writer.flush();
	}

	

	/**
	 * Send property information from server to client
	 *
	 * @param writer 	writer to use for sending sysinfo
	 *
	 * @exception throws Exception if a problem occurs sending value
	 */
	private void sendPropInfo(DDMWriter writer) throws Exception
	{
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			Properties p = getPropertyValues();
			p.store(out, "NetworkServerControl properties");
			try {
				writeCommandReplyHeader(writer);
				writer.writeByte(0);		//O.K.
				writer.writeLDBytes(out.toByteArray());
			} catch (DRDAProtocolException e) {
				consolePropertyMessage("DRDA_PropInfoWriteError.S", e.getMessage());
			}
			writer.flush();
		} 
		catch (Exception e) {
			consoleExceptionPrintTrace(e);
		}
	}

	/**
	 * Get Net Server information
	 *
	 * @return system information for the Network Server
	 */
	private String getNetSysInfo() 
	{
		StringBuffer sysinfo = new StringBuffer();
		LocalizedResource localLangUtil = langUtil;
		if (currentSession != null && currentSession.langUtil != null)
		localLangUtil = currentSession.langUtil;
		sysinfo.append(localLangUtil.getTextMessage("DRDA_SysInfoBanner.I")+ "\n");
		sysinfo.append(localLangUtil.getTextMessage("DRDA_SysInfoVersion.I")+ " " + att_srvrlslv);
		sysinfo.append("  ");
		sysinfo.append(localLangUtil.getTextMessage("DRDA_SysInfoBuild.I")+ " " + buildNumber);
		sysinfo.append("  ");
		sysinfo.append(localLangUtil.getTextMessage("DRDA_SysInfoDrdaPRDID.I")+ " " + prdId);
		if (SanityManager.DEBUG)
		{
			sysinfo.append("  ** SANE BUILD **");
		}
		sysinfo.append("\n");
		// add property information
		Properties p = getPropertyValues();
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		PrintStream ps =  new PrintStream(bos);
		p.list(ps);
		sysinfo.append(bos.toString());
		return sysinfo.toString();
	}

	/**
	 * @see NetworkServerControl#getRuntimeInfo
	 */
	private String getRuntimeInfo() 
	{
		return buildRuntimeInfo(langUtil);
	}

	/**
	 * Get Cloudscape information
	 *
	 * @return system information for Cloudscape
	 *
	 * @exception throws IOException if a problem occurs encoding string
	 */
	private String getCLSSysInfo() throws IOException
	{
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		LocalizedResource localLangUtil = langUtil;
		if (currentSession != null && currentSession.langUtil != null)
		localLangUtil = currentSession.langUtil;
		LocalizedOutput aw = localLangUtil.getNewOutput(bos);
		org.apache.derby.impl.tools.sysinfo.Main.getMainInfo(aw, false);
		return bos.toString();
	}

	/**
	 * Execute the command given on the command line
	 *
	 * @param args	array of arguments indicating command to be executed
	 *
	 * @exception Exception	throws an exception if an error occurs
	 * see class comments for more information
	 */
	public static void execute(String args[]) 
	{
		DB2jServerImpl server = null;
		try {
			server = new DB2jServerImpl();
			server.executeWork(args);
		} catch (Exception e){
			//if there was an error, exit(1)
			if ((e.getMessage() == null) ||
				!e.getMessage().equals(DB2jServerImpl.UNEXPECTED_ERR))
			{
				if (server != null)
					server.consoleExceptionPrint(e);
				else
					e.printStackTrace();  // default output stream is System.out
			}
			// else, we've already printed a trace, so just exit.
			System.exit(1);
		}
		System.exit(0);
	}


	/**
	 * Execute the command given on the command line
	 *
	 * @param args	array of arguments indicating command to be executed
	 *
	 * @exception Exception	throws an exception if an error occurs
	 * see class comments for more information
	 */
	protected void executeWork(String args[]) throws Exception
	{
		// For convenience just use DB2jServerImpls log writer for user messages
		logWriter = makePrintWriter(System.out);
		
		int command = 0; 
		if (args.length > 0)
			command = findCommand(args);
		else
		{
			consolePropertyMessage("DRDA_NoArgs.U");
		}

		// if we didn't have a valid command just return - error already generated
		if (command == COMMAND_UNKNOWN)
			return;

		// check that we have the right number of required arguments
		if (commandArgs.size() != COMMAND_ARGS[command])
			consolePropertyMessage("DRDA_InvalidNoArgs.U", COMMANDS[command]);
		int min;
		int max;


		switch (command)
		{
			case COMMAND_START:
				blockingStart(makePrintWriter(System.out));
				break;
			case COMMAND_SHUTDOWN:
				shutdown();
				consolePropertyMessage("DRDA_ShutdownSuccess.I");
				break;
			case COMMAND_TRACE:
				{
					boolean on = isOn((String)commandArgs.elementAt(0));
					trace(sessionArg, on);
					consoleTraceMessage(sessionArg, on);
					break;
				}
			case COMMAND_TRACEDIRECTORY:
				setTraceDirectory((String) commandArgs.elementAt(0));
				consolePropertyMessage("DRDA_TraceDirectoryChange.I", traceDirectory);
				break;
			case COMMAND_TESTCONNECTION:
				ping();
				consolePropertyMessage("DRDA_ConnectionTested.I", new String [] 
					{hostArg, (new Integer(portNumber)).toString()});
				break;
			case COMMAND_LOGCONNECTIONS:
				{
					boolean on = isOn((String)commandArgs.elementAt(0));
					logConnections(on);
					consolePropertyMessage("DRDA_LogConnectionsChange.I", on ? "DRDA_ON.I" : "DRDA_OFF.I");
					break;
				}
			case COMMAND_SYSINFO:
				{
					String info = sysinfo();
					consoleMessage(info);
					break;
				}
			case COMMAND_MAXTHREADS:
				max = 0;
				try{
					max = Integer.parseInt((String)commandArgs.elementAt(0));
				}catch(NumberFormatException e){
					consolePropertyMessage("DRDA_InvalidValue.U", new String [] 
						{(String)commandArgs.elementAt(0), "maxthreads"});
				}
				if (max < MIN_MAXTHREADS)
					consolePropertyMessage("DRDA_InvalidValue.U", new String [] 
						{new Integer(max).toString(), "maxthreads"});
				netSetMaxThreads(max);

				break;
			case COMMAND_RUNTIME_INFO:
				String reply = runtimeInfo();
				consoleMessage(reply);
				break;
			case COMMAND_TIMESLICE:
				int timeslice = 0;
				String timeSliceArg = (String)commandArgs.elementAt(0);
            	try{
                	timeslice = Integer.parseInt(timeSliceArg);
            	}catch(NumberFormatException e){
					consolePropertyMessage("DRDA_InvalidValue.U", new String [] 
						{(String)commandArgs.elementAt(0), "timeslice"});
            	}
				if (timeslice < MIN_TIMESLICE)
					consolePropertyMessage("DRDA_InvalidValue.U", new String [] 
						{new Integer(timeslice).toString(), "timeslice"});
				netSetTimeSlice(timeslice);
				
				break;
			default:
				//shouldn't get here
				if (SanityManager.DEBUG)
					SanityManager.THROWASSERT("Invalid command in switch:"+ command);
		}
	}

  
	/**
	 * Add session to the run queue
	 *
	 * @param clientSession	session needing work
	 */
	protected void runQueueAdd(Session clientSession)
	{
		synchronized(runQueue)
		{
			runQueue.addElement(clientSession);
			runQueue.notify();
		}
	}
	/**
	 * Go through the arguments and find the command and save the dash arguments
	 *	and arguments to the command.  Only one command is allowed in the argument
	 *	list.
	 *
	 * @param args	arguments to search
	 *
	 * @return  command
	 */
	private int findCommand(String [] args) throws Exception
	{
		try {
			// process the dashArgs and pull out the command args 
			int i = 0;
			int newpos = 0;
			while (i < args.length)
			{
				if (args[i].startsWith("-"))
				{
					newpos = processDashArg(i, args);
					if (newpos == i)
						commandArgs.addElement(args[i++]);
					else
						i = newpos;
				}
				else
					commandArgs.addElement(args[i++]);
			}
					
			// look up command
			if (commandArgs.size() > 0)
			{
				for (i = 0; i < COMMANDS.length; i++)
				{
					if (StringUtil.SQLEqualsIgnoreCase(COMMANDS[i], 
													   (String)commandArgs.firstElement()))
					{
						commandArgs.removeElementAt(0);
						return i;
					}
				}
			}
			// didn't find command
			consolePropertyMessage("DRDA_UnknownCommand.U", 
				(String) commandArgs.firstElement());
		} catch (Exception e) {
			if (e.getMessage().equals(DB2jServerImpl.UNEXPECTED_ERR))
				throw e;
			//Ignore expected exceptions, they will have been
									//handled by the consolePropertyMessage routine
		}
		return COMMAND_UNKNOWN;
	}
	/**
	 * Get the dash argument. Optional arguments are formated as -x value.
	 *
	 * @param pos	starting point
	 * @param args	arguments to search
	 *
	 * @return  command
	 *
	 * @exception Exception	thrown if an error occurs
	 */
	private int processDashArg(int pos, String[] args)
		throws Exception
	{
		//check for a negative number
		char c = args[pos].charAt(1);
		if (c >= '0' && c <= '9')
			return pos;
		int dashArg = -1;
		for (int i = 0; i < DASHARGS.length; i++)
		{
			if (DASHARGS[i].equals(args[pos].substring(1)))
			{
				dashArg = i;
				pos++;
				break;
			}
		}
		if (dashArg == -1)
			consolePropertyMessage("DRDA_UnknownArgument.U", args[pos]);
		switch (dashArg)
		{
			case DASHARG_PORT:
				if (pos < args.length)
				{
            		try{
                		portNumber = Integer.parseInt(args[pos]);
            		}catch(NumberFormatException e){
						consolePropertyMessage("DRDA_InvalidValue.U", 
							new String [] {args[pos], "DRDA_PortNumber.I"});
            		}
				}
				else
					consolePropertyMessage("DRDA_MissingValue.U", "DRDA_PortNumber.I");
				break;
			case DASHARG_HOST:
				if (pos < args.length)
				{
					hostArg = args[pos];
				}
				else
					consolePropertyMessage("DRDA_MissingValue.U", "DRDA_Host.I");
				break;
			case DASHARG_DATABASE:
				if (pos < args.length)
					databaseArg = args[pos];
				else
					consolePropertyMessage("DRDA_MissingValue.U", 
						"DRDA_DatabaseDirectory.I");
				break;
			case DASHARG_USER:
				if (pos < args.length)
				{
					userArg = args[pos++];
					if (pos < args.length)
						passwordArg = args[pos];
					else
						consolePropertyMessage("DRDA_MissingValue.U", 
							"DRDA_Password.I");
				}
				else
					consolePropertyMessage("DRDA_MissingValue.U", "DRDA_User.I");
				break;
			case DASHARG_ENCALG:
				if (pos < args.length)
					encAlgArg = args[pos];
				else
					consolePropertyMessage("DRDA_MissingValue.U", 
						"DRDA_EncryptionAlgorithm.I");
				break;
			case DASHARG_ENCPRV:
				if (pos < args.length)
					encPrvArg = args[pos];
				else
					consolePropertyMessage("DRDA_MissingValue.U", 
						"DRDA_EncryptionProvider.I");
				break;
			case DASHARG_LOADSYSIBM:
				break;
			case DASHARG_SESSION:
				if (pos < args.length)
            		try{
                		sessionArg = Integer.parseInt(args[pos]);
            		}catch(NumberFormatException e){
						consolePropertyMessage("DRDA_InvalidValue.U", 
							new String [] {args[pos], "DRDA_Session.I"});
            		}
				else
					consolePropertyMessage("DRDA_MissingValue.U", "DRDA_Session.I");
				break;
			default:
				//shouldn't get here
		}
		return pos+1;
	}

	/**
	 * Is string "on" or "off"
	 *
	 * @param string	string to check
	 *
	 * @return  true if string is "on", false if string is "off"
	 *
	 * @exception Exception	thrown if string is not one of "on" or "off"
	 */
	private boolean isOn(String arg)
		throws Exception
	{
		if (StringUtil.SQLEqualsIgnoreCase(arg, "on"))
			return true;
		else if (!StringUtil.SQLEqualsIgnoreCase(arg, "off"))
			consolePropertyMessage("DRDA_OnOffValue.U", arg);
		return false;
	}

	/**
	 * Set up client socket to send a command to the network server
	 *
   	 * @exception Exception	thrown if exception encountered
	 */
	private void setUpSocket() throws Exception
	{
		
		try {
            clientSocket = (Socket) AccessController.doPrivileged(
								new PrivilegedExceptionAction() {
										
									public Object run() throws UnknownHostException,IOException
									{
										if (hostAddress == null)
											hostAddress = InetAddress.getByName(hostArg);

										// JDK131 can't connect with a client
										// socket with 0.0.0.0 (all addresses) so we will
										// getLocalHost() which will suffice.
										InetAddress connectAddress;
										if (JVMInfo.JDK_ID < 4 &&
											hostAddress.getHostAddress().equals("0.0.0.0"))
											connectAddress = InetAddress.getLocalHost();
										else
											connectAddress = hostAddress;

										return new Socket(connectAddress, portNumber);
									}
								}
							);
		} catch (PrivilegedActionException pae) {
			Exception e1 = pae.getException();
        	if (e1 instanceof UnknownHostException) {
					consolePropertyMessage("DRDA_UnknownHost.S", hostArg);
			}
        	else if (e1 instanceof IOException) {
					consolePropertyMessage("DRDA_NoIO.S",
						new String [] {hostArg, (new Integer(portNumber)).toString()});
			}
		} catch (Exception e) {
		// If we find other (unexpected) errors, we ultimately exit--so make
		// sure we print the error message before doing so (Beetle 5033).
			throwUnexpectedException(e);
		}

        try
        {
	       clientIs = clientSocket.getInputStream();
	       clientOs = clientSocket.getOutputStream();
		} catch (IOException e) {
			consolePropertyMessage("DRDA_NoInputStream.I");
			throw e;
        }
	}

	
	private void checkAddressIsLocal(InetAddress inetAddr) throws UnknownHostException,Exception
	{
		for(int i = 0; i < localAddresses.size(); i++)
		{
			if (inetAddr.equals((InetAddress)localAddresses.get(i)))
			{
				return;
			}
		}
		consolePropertyMessage("DRDA_NeedLocalHost.S", new String[] {inetAddr.getHostName(),serverSocket.getInetAddress().getHostName()});

	}


	/**
	 * Build local address list to allow admin commands.
	 *
	 * @param bindAddr Address on which server was started
	 * 
	 * Note: Some systems may not support localhost.
	 * In that case a console message will print for the localhost entries,
	 * but the server will continue to start.
	 **/
	private void buildLocalAddressList(InetAddress bindAddr) 
	{
			localAddresses = new ArrayList(3);
			localAddresses.add(bindAddr);
			try {
				localAddresses.add(InetAddress.getLocalHost());
				localAddresses.add(InetAddress.getByName("localhost"));
			}catch(UnknownHostException uhe)
			{
				try {
					consolePropertyMessage("DRDA_UnknownHostWarning.I",uhe.getMessage());
				} catch (Exception e)
				{ // just a warning shouldn't actually throw an exception
				}
			}			
	}
	
	/**
	 * Routines for writing commands for DB2jServerImpl being used as a client
	 * to a server
	 */

	/**
	 * Write command header consisting of command header string and protocol
	 * version and command
	 *
	 * @param command	command to be written
	 *
	 * @exception Exception	throws an exception if an error occurs
	 */
	private void writeCommandHeader(int command) throws Exception
	{
		try {
			writeString(COMMAND_HEADER);
			commandOs.writeByte((byte)((PROTOCOL_VERSION & 0xf0) >> 8 ));
			commandOs.writeByte((byte)(PROTOCOL_VERSION & 0x0f));

			if (clientLocale != null && clientLocale != DEFAULT_LOCALE)
			{
				commandOs.writeByte(clientLocale.length());
				commandOs.writeBytes(clientLocale);
			}
			else
				commandOs.writeByte((byte) 0);
			commandOs.writeByte((byte) 0);
			commandOs.writeByte((byte) command);
		}
		catch (IOException e)
		{
			clientSocketError(e);
		}
	}
	/**
	 * Write length delimited string string
	 *
	 * @param msg	string to be written
	 *
	 * @exception Exception	throws an exception if an error occurs
	 */
	private void writeLDString(String msg) throws Exception
	{
		try {
			if (msg == null)
			{
				commandOs.writeShort(0);
			}
			else
			{
				commandOs.writeShort(msg.length());
				writeString(msg);
			}
		}
		catch (IOException e)
		{
			clientSocketError(e);
		}
	}

	/** Write string
	 *
	 * @param msg String to write
	 */

	protected void writeString(String msg) throws Exception
	{
		byte[] msgBytes = msg.getBytes(DEFAULT_ENCODING);
		commandOs.write(msgBytes,0,msgBytes.length);
	}

	/**
	 * Write short
	 *
	 * @param value	value to be written
	 *
	 * @exception Exception	throws an exception if an error occurs
	 */
	private void writeShort(int value) throws Exception
	{
		try {
			commandOs.writeByte((byte)((value & 0xf0) >> 8 ));
			commandOs.writeByte((byte)(value & 0x0f));
		}
		catch (IOException e)
		{
			clientSocketError(e);
		}
	}
	/**
	 * Write byte
	 *
	 * @param value	value to be written
	 *
	 * @exception Exception	throws an exception if an error occurs
	 */
	private void writeByte(int value) throws Exception
	{
		try {
			commandOs.writeByte((byte)(value & 0x0f));
		}
		catch (IOException e)
		{
			clientSocketError(e);
		}
	}
	/**
	 * Send client message to server
	 *
	 *
	 * @exception Exception	throws an exception if an error occurs
	 */
	private void send() throws Exception
	{
		try {
			byteArrayOs.writeTo(clientOs);
			commandOs.flush();
			byteArrayOs.reset();	//discard anything currently in the byte array
		}
		catch (IOException e)
		{
			clientSocketError(e);
		}
	}
	/**
	 * Stream error writing to client socket
 	 */
	private void clientSocketError(IOException e) throws IOException
	{
		try {
			consolePropertyMessage("DRDA_ClientSocketError.S", e.getMessage());
		} catch (Exception ce) {} // catch the exception consolePropertyMessage will
								 // throw since we also want to print a stack trace
		consoleExceptionPrintTrace(e);
			throw e;
	}
	/**
	 * Read result from sending client message to server
	 *
	 * @exception Exception	throws an exception if an error occurs
	 */
	private void readResult() throws Exception
	{
		fillReplyBuffer();
		readCommandReplyHeader();
		if (replyBufferPos >= replyBufferCount)
			consolePropertyMessage("DRDA_InvalidReplyTooShort.S");
		int messageType = replyBuffer[replyBufferPos++] & 0xFF;
		if (messageType == OK)		// O.K.
			return;
		// get error and display and throw exception
		String message =  readLDString();
		if (messageType == SQLERROR)
			wrapSQLError(message);
		else if (messageType == SQLWARNING)
			wrapSQLWarning(message);
		else
			consolePropertyMessage(message);
	}

	

	/**
	 * Ensure the reply buffer is at large enought to hold all the data;
	 * don't just rely on OS level defaults
	 *
	 *
	 * @param minimumBytesNeeded	size of buffer required	
	 * @exception Exception throws an exception if a problem reading the reply
	 */
	private void ensureDataInBuffer(int minimumBytesNeeded) throws Exception
	{
		// make sure the buffer is large enough
		while ((replyBufferCount - replyBufferPos) < minimumBytesNeeded)
		{
			try {
				int bytesRead = clientIs.read(replyBuffer, replyBufferCount, replyBuffer.length - replyBufferCount);
				replyBufferCount += bytesRead;
		
			} catch (IOException e)
			{
				clientSocketError(e);
			}
		}
	}


	/**
	 * Fill the reply buffer with the reply allocates a reply buffer if one doesn't
	 * exist
	 *
	 *
	 * @exception Exception throws an exception if a problem reading the reply
	 */
	private void fillReplyBuffer() throws Exception
	{
		if (replyBuffer == null)
			replyBuffer = new byte[MAXREPLY];
		try {
			replyBufferCount = clientIs.read(replyBuffer);
		}
		catch (IOException e)
		{
			clientSocketError(e);
		}
		if (replyBufferCount == -1)
			consolePropertyMessage("DRDA_InvalidReplyTooShort.S");
		replyBufferPos = 0;
	}
	/**
	 * Read the command reply header from the server
	 *
	 * @exception Exception throws an exception if an error occurs
	 */
	private void readCommandReplyHeader() throws Exception
	{
		ensureDataInBuffer(REPLY_HEADER_LENGTH);
		if (replyBufferCount < REPLY_HEADER_LENGTH)
		{
			consolePropertyMessage("DRDA_InvalidReplyHeader1.S", Integer.toString(replyBufferCount));
		}
		String header =  new String(replyBuffer, 0, REPLY_HEADER_LENGTH, DEFAULT_ENCODING);
		if (!header.equals(REPLY_HEADER))
		{
			consolePropertyMessage("DRDA_InvalidReplyHeader2.S", header);
		}
		replyBufferPos += REPLY_HEADER_LENGTH;
	}
	/**
	 * Read short from buffer
	 * @exception Exception	throws an exception if an error occurs
	 */
	private int readShort() throws Exception
	{
		ensureDataInBuffer(2);
		if (replyBufferPos + 2 > replyBufferCount)
			consolePropertyMessage("DRDA_InvalidReplyTooShort.S");
	 	return ((replyBuffer[replyBufferPos++] & 0xff) << 8) + 
			    (replyBuffer[replyBufferPos++] & 0xff);
	}
	/**
	 * Read int from buffer
	 * @exception Exception	throws an exception if an error occurs
	 */
	private int readInt() throws Exception
	{
		ensureDataInBuffer(4);
		if (replyBufferPos + 4 > replyBufferCount)
			consolePropertyMessage("DRDA_InvalidReplyTooShort.S");
	 	return ((replyBuffer[replyBufferPos++] & 0xff) << 24) + 
	 	 	((replyBuffer[replyBufferPos++] & 0xff) << 16) + 
	 		((replyBuffer[replyBufferPos++] & 0xff) << 8) + 
			    (replyBuffer[replyBufferPos++] & 0xff);
	}
	/**
	 * Read String reply
	 *
	 * @param msgKey	error message key
	 * @return string value or null 
	 * @exception Exception throws an error if problems reading reply
	 */
	private String readStringReply(String msgKey) throws Exception
	{
		fillReplyBuffer();
		readCommandReplyHeader();
		if (replyBuffer[replyBufferPos++] == 0)		// O.K.
			return readLDString();
		else
			consolePropertyMessage(msgKey);
		return null;
			
	}



	
	/**
	 * Read length delimited string from a buffer
	 *
	 * @return string value from buffer
	 * @exception Exception throws an error if problems reading reply
	 */
	private String readLDString() throws Exception
	{
		int strlen = readShort();
		ensureDataInBuffer(strlen);
		if (replyBufferPos + strlen > replyBufferCount)
			consolePropertyMessage("DRDA_InvalidReplyTooShort.S");
		String retval= new String(replyBuffer, replyBufferPos, strlen, DEFAULT_ENCODING);
		replyBufferPos += strlen;
		return retval;
	}
	/**
	 * Read Bytes reply
	 *
	 * @param msgKey	error message key
	 * @return string value or null 
	 * @exception Exception throws an error if problems reading reply
	 */
	private byte [] readBytesReply(String msgKey) throws Exception
	{
		fillReplyBuffer();
		readCommandReplyHeader();
		if (replyBuffer[replyBufferPos++] == 0)		// O.K.
			return readLDBytes();
		else
			consolePropertyMessage(msgKey);
		return null;
			
	}
	/**
	 * Read length delimited bytes from a buffer
	 *
	 * @return byte array from buffer
	 * @exception Exception throws an error if problems reading reply
	 */
	private byte[] readLDBytes() throws Exception
	{
		int len = readShort();
		ensureDataInBuffer(len);
		if (replyBufferPos + len > replyBufferCount)
			consolePropertyMessage("DRDA_InvalidReplyTooShort.S");
		byte [] retval =  new byte[len];
		for (int i = 0; i < len; i++)
			retval[i] = replyBuffer[replyBufferPos++];
		return retval;
	}

	/**
	 * Get property info
	 *
	 * @return system properties
	 */
	private void  getPropertyInfo() throws Exception
	{
		//set values according to properties
		
		String directory = PropertyUtil.getSystemProperty(Property.SYSTEM_HOME_PROPERTY);
		String propval = PropertyUtil.getSystemProperty(
			Property.DRDA_PROP_LOGCONNECTIONS);
		if (propval != null && StringUtil.SQLEqualsIgnoreCase(propval,"true"))  
			setLogConnections(true);
		propval = PropertyUtil.getSystemProperty(Property.DRDA_PROP_TRACEALL);
		if (propval != null && StringUtil.SQLEqualsIgnoreCase(propval, 
															  "true"))  
			setTraceAll(true);

		setTraceDirectory(PropertyUtil.getSystemProperty( 
			Property.DRDA_PROP_TRACEDIRECTORY));

		propval = PropertyUtil.getSystemProperty( 
			Property.DRDA_PROP_MINTHREADS);
		if (propval != null)
			setMinThreads(getIntPropVal(Property.DRDA_PROP_MINTHREADS, propval));

		propval = PropertyUtil.getSystemProperty( 
			Property.DRDA_PROP_MAXTHREADS);
		if (propval != null)
			setMaxThreads(getIntPropVal(Property.DRDA_PROP_MAXTHREADS, propval));


		propval = PropertyUtil.getSystemProperty( 
			Property.DRDA_PROP_TIMESLICE);
		if (propval != null)
			setTimeSlice(getIntPropVal(Property.DRDA_PROP_TIMESLICE, propval));

		propval = PropertyUtil.getSystemProperty( 
			Property.DRDA_PROP_PORTNUMBER);
		if (propval != null)
			portNumber = getIntPropVal(Property.DRDA_PROP_PORTNUMBER, propval);

		propval = PropertyUtil.getSystemProperty( 
			Property.DRDA_PROP_KEEPALIVE);
		if (propval != null && 
			StringUtil.SQLEqualsIgnoreCase(propval,"false"))
			keepAlive = false;
		
		propval = PropertyUtil.getSystemProperty( 
			Property.DRDA_PROP_HOSTNAME);
		if (propval != null)
			hostArg = propval;

		propval = PropertyUtil.getSystemProperty(
						 DB2jServerImpl.DRDA_PROP_DEBUG);
		if (propval != null  && StringUtil.SQLEqualsIgnoreCase(propval, "true"))
			debugOutput = true;

		//RESOLVE: Need to clean this up. There should be just a
		// server API call to get the log location
		// Determine errror log location
		propval = PropertyUtil.getSystemProperty(
								   DB2jServerImpl.DB2J_PROP_STREAM_ERROR_FIELD);
		if (propval == null)
			propval = PropertyUtil.getSystemProperty( 
									   DB2jServerImpl.DB2J_PROP_STREAM_ERROR_METHOD);
		if (propval == null)
		{
			propval = PropertyUtil.getSystemProperty( 
									   DB2jServerImpl.DB2J_PROP_STREAM_ERROR_FILE);
			if (propval == null)
				propval = "derby.log";
		}
		File errorFile = new File(propval);
		if (errorFile.isAbsolute())
			errorLogLocation = errorFile.getPath();
		else
			errorLogLocation = (new File
				(directory,propval)).getPath();
		
	}

	/**
	 * Get integer property values
	 *
	 * @param propName 	property name
	 * @param propVal	string property value
	 * @return integer value
	 *
	 * @exception Exception if not a valid integer
	 */
	private int getIntPropVal(String propName, String propVal)
		throws Exception
	{
		int val = 0;
		try {
			 val = (new Integer(propVal)).intValue();
		} catch (Exception e)
		{
			consolePropertyMessage("DRDA_InvalidPropVal.S", new String [] 
				{propName, propVal});
		}
		return val;
	}

	/**
	 * Handle console error message
	 * 	- display on console and if it is a user error, display usage
	 *  - if user error or severe error, throw exception with message key and message
	 *
	 * @param messageKey	message key
	 * @param args			arguments to message
	 *
	 * @exception throws an Exception if an error occurs
	 */
	private void consolePropertyMessageWork(String messageKey, String [] args)
		throws Exception
	{
		String locMsg = null;

		int type = getMessageType(messageKey);

		if (type == ERRTYPE_UNKNOWN)
			locMsg = messageKey;
		else
			locMsg = localizeMessage(messageKey, langUtil, args);

		//display on the console
		consoleMessage(locMsg);

		//if it is a user error display usage
		if (type == ERRTYPE_USER)
			usage();

		//we may want to use a different locale for throwing the exception
		//since this can be sent to a browser with a different locale
		if (currentSession != null && 
				currentSession.langUtil != null &&
				type != ERRTYPE_UNKNOWN)
			locMsg = localizeMessage(messageKey, currentSession.langUtil, args);

		// throw an exception for severe and user errors
		if (type == ERRTYPE_SEVERE || type == ERRTYPE_USER)
		{
			if (messageKey.equals("DRDA_SQLException.S"))
				throwSQLException(args[0]);
			else if (messageKey.equals("DRDA_SQLWarning.I"))
				throwSQLWarning(args[0]);
			else 
				throw new Exception(messageKey+":"+locMsg);
		}

		// throw an exception with just the message if the error type is
		// unknown
		if (type == ERRTYPE_UNKNOWN)
			throw new Exception(locMsg);

		return;

	}
	/**
	 * Throw a SQL Exception which was sent over by a server
	 * Format of the msg is SQLSTATE:localized message\nSQLSTATE:next localized message
	 *
	 * @param msg		msg containing SQL Exception
	 *
	 * @exception throws a SQLException 
	 */
	private void throwSQLException(String msg) throws SQLException
	{
		SQLException se = null;
		SQLException ne;
		SQLException ce = null;
		StringBuffer strbuf = new StringBuffer();
		StringTokenizer tokenizer = new StringTokenizer(msg, "\n");
		String sqlstate = null;
		String str;
		while (tokenizer.hasMoreTokens())
		{
			str = tokenizer.nextToken();
			//start of the next message
			if (str.charAt(5) == ':')
			{
				if (strbuf.length() > 0)
				{
					if (se == null)
					{
						se = new SQLException(strbuf.toString(), sqlstate);
						ce = se;
					}
					else
					{
						ne = new SQLException(strbuf.toString(), sqlstate);
						ce.setNextException(ne);
						ce = ne;
					}
					strbuf = new StringBuffer();
				}
				strbuf.append(str.substring(6));
				sqlstate = str.substring(0,5);
			}
			else
				strbuf.append(str);
		}
		if (strbuf.length() > 0)
		{
			if (se == null)
			{
				se = new SQLException(strbuf.toString(), sqlstate);
				ce = se;
			}
			else
			{
				ne = new SQLException(strbuf.toString(), sqlstate);
				ce.setNextException(ne);
				ce = ne;
			}
		}
		throw se;
	}
	/**
	 * Throw a SQL Warning which was sent over by a server
	 * Format of the msg is SQLSTATE:localized message\nSQLSTATE:next localized message
	 *
	 * @param msg		msg containing SQL Warning
	 *
	 * @exception throws a SQLWarning
	 */
	private void throwSQLWarning(String msg) throws SQLWarning
	{
		SQLWarning se = null;
		SQLWarning ne;
		SQLWarning ce = null;
		StringBuffer strbuf = new StringBuffer();
		StringTokenizer tokenizer = new StringTokenizer(msg, "\n");
		String sqlstate = null;
		String str;
		while (tokenizer.hasMoreTokens())
		{
			str = tokenizer.nextToken();
			//start of the next message
			if (str.charAt(5) == ':')
			{
				if (strbuf.length() > 0)
				{
					if (se == null)
					{
						se = new SQLWarning(strbuf.toString(), sqlstate);
						ce = se;
					}
					else
					{
						ne = new SQLWarning(strbuf.toString(), sqlstate);
						ce.setNextException(ne);
						ce = ne;
					}
					strbuf = new StringBuffer();
				}
				strbuf.append(str.substring(6));
				sqlstate = str.substring(0,5);
			}
			else
				strbuf.append(str);
		}
		if (strbuf.length() > 0)
		{
			if (se == null)
			{
				se = new SQLWarning(strbuf.toString(), sqlstate);
				ce = se;
			}
			else
			{
				ne = new SQLWarning(strbuf.toString(), sqlstate);
				ce.setNextException(ne);
				ce = ne;
			}
		}
		throw se;
	}

	/**
	 * Print a trace for the (unexpected) exception received, then
	 * throw a generic exception indicating that 1) an unexpected
	 * exception was thrown, and 2) we've already printed the trace
	 * (so don't do it again).
	 * 
	 * @param e An unexpected exception.
	 * @exception throws an Exception with message UNEXPECTED_ERR.
	 */
	private void throwUnexpectedException(Exception e)
	 throws Exception {

		consoleExceptionPrintTrace(e);
		throw new Exception(UNEXPECTED_ERR);

	}

	/**
	 * Localize a message given a particular AppUI 
	 *
	 * @param msgProp	message key
	 * @param localAppUI	AppUI to use to localize message
	 * @param args		arguments to message
	 *
	 */
	private String localizeMessage(String msgProp, LocalizedResource localLangUtil, String [] args)
	{
		String locMsg = null;
		//check if the argument is a property
		if (args != null)
		{
			String [] argMsg = new String[args.length];
			for (int i = 0; i < args.length; i++)
			{
				if (isMsgProperty(args[i]))
					argMsg[i] = localLangUtil.getTextMessage(args[i]);
				else
					argMsg[i] = args[i];
			}
			switch (args.length)
			{
				case 1:
				 	locMsg = localLangUtil.getTextMessage(msgProp, argMsg[0]);
					break;
				case 2:
					locMsg = localLangUtil.getTextMessage(msgProp, argMsg[0], argMsg[1]);
					break;
				case 3:
					locMsg = localLangUtil.getTextMessage(msgProp, argMsg[0], argMsg[1], argMsg[2]);
					break;
				case 4:
					locMsg = localLangUtil.getTextMessage(msgProp, argMsg[0], argMsg[1], argMsg[2], argMsg[3]);
					break;
				default:
					//shouldn't get here
			}
		}
		else
		 	locMsg = localLangUtil.getTextMessage(msgProp);
		return locMsg;
	}
	/**
	 * Determine type of message
	 *
	 * @param msg		message 
	 *
	 * @return message type
	 */
	private int getMessageType(String msg)
	{
		//all property messages should start with DRDA_
		if (!msg.startsWith(DRDA_MSG_PREFIX))
			return ERRTYPE_UNKNOWN;
		int startpos = msg.indexOf('.')+1;
		if (startpos >= msg.length())
			return ERRTYPE_UNKNOWN;
		if (msg.length() > (startpos + 1))
			return ERRTYPE_UNKNOWN;
		char type = msg.charAt(startpos);
		if (type == 'S')
			return ERRTYPE_SEVERE;
		if (type == 'U')
			return ERRTYPE_USER;
		if (type == 'I')
			return ERRTYPE_INFO;
		return ERRTYPE_UNKNOWN;
	}
	/**
	 * Determine whether string is a property key or not
	 * 	property keys start with DRDA_MSG_PREFIX
	 *
	 * @param msg		message 
	 *
	 * @return true if it is a property key; false otherwise
	 */
	private boolean isMsgProperty(String msg)
	{
		if (msg.startsWith(DRDA_MSG_PREFIX))
			return true;
		else
			return false;
	}
	/**
	 * Get the current value of logging connections
	 *
	 * @return true if logging connections is on; false otherwise
	 */
	public boolean getLogConnections()
	{
		synchronized(logConnectionsSync) {
			return logConnections;
		}
	}
	/**
	 * Set the current value of logging connections
	 *
	 * @param value	true to turn logging connections on; false to turn it off
	 */
	private void setLogConnections(boolean value)
	{
		synchronized(logConnectionsSync) {
			logConnections = value;
		}
		// update the value in all the threads
		synchronized(threadList) {
			for (Enumeration e = threadList.elements(); e.hasMoreElements(); )
			{
				DRDAConnThread thread = (DRDAConnThread)e.nextElement();
				thread.setLogConnections(value);
			}
		}
	}

	/**
	 * Set the trace on/off for all sessions, or one session, depending on
	 * whether we got -s argument.
	 *
	 * @param on	true to turn trace on; false to turn it off
	 * @return true if set false if an error occurred
	 */
	private boolean setTrace(boolean on)
	{
		if (sessionArg == 0)
		{
			setTraceAll(on);
			synchronized(sessionTable) {
				for (Enumeration e = sessionTable.elements(); e.hasMoreElements(); )
				{	
					Session session = (Session) e.nextElement();
					if (on)
						session.setTraceOn(traceDirectory);
					else
						session.setTraceOff();
				}
			}
		}
		else
		{
			Session session = (Session) sessionTable.get(new Integer(sessionArg));
			if (session != null)
			{	
				if (on)
					session.setTraceOn(traceDirectory);
				else
					session.setTraceOff();
			}
			else
				return false;
		}
		return true;
	}


	/**
	 * Get the current value of the time slice
	 *
	 * @return time slice value
	 */
	protected int getTimeSlice()
	{
			return timeSlice;
	}
	/**
	 * Set the current value of  time slice
	 *
	 * @param value time slice value
	 * @exception Exception if value is < 0
	 */
	private void setTimeSlice(int value)
		throws Exception
	{
		if (value < MIN_TIMESLICE)
			consolePropertyMessage("DRDA_InvalidValue.U", new String [] 
				{new Integer(value).toString(), "timeslice"});
		if (value == USE_DEFAULT)
			value = DEFAULT_TIMESLICE;
		synchronized(timeSliceSync) {
			timeSlice = value;
		}
	}
	
	/** 
	 * Get the current value of keepAlive to configure how long the server
	 * should keep the socket alive for a disconnected client
	 */
	protected boolean getKeepAlive()
	{
		return keepAlive;
	}

	/**
	 * Get the current value of minimum number of threads to create at start
	 *
	 * @return value of minimum number of threads
	 */
	private int getMinThreads()
	{
		synchronized(threadsSync) {
			return minThreads;
		}
	}
	/**
	 * Set the current value of minimum number of threads to create at start
	 *
	 * @param value	 value of minimum number of threads
	 */
	private void setMinThreads(int value)
	{
		synchronized(threadsSync) {
			minThreads = value;
		}
	}
	/**
	 * Get the current value of maximum number of threads to create 
	 *
	 * @return value of maximum number of threads
	 */
	protected int getMaxThreads()
	{
		synchronized(threadsSync) {
			return maxThreads;
		}
	}
	/**
	 * Set the current value of maximum number of threads to create 
	 *
	 * @param value	value of maximum number of threads
	 * @exception Exception if value is less than 0
	 */
	private void setMaxThreads(int value) throws Exception
	{
		if (value < MIN_MAXTHREADS)
			consolePropertyMessage("DRDA_InvalidValue.U", new String [] 
				{new Integer(value).toString(), "maxthreads"});
		if (value == USE_DEFAULT)
			value = DEFAULT_MAXTHREADS;
		synchronized(threadsSync) {
			maxThreads = value;
		}
	}
	/**
	 * Get the current value of minimum number of pooled connections to create at start
	 *
	 * @return value of minimum number of pooled connections
	 */
	private int getMinPoolSize()
	{
		synchronized(threadsSync) {
			return minPoolSize;
		}
	}

	/**
	 * Set the current value of minimum number of pooled connections to create at start
	 *
	 * @param value	 value of minimum number of pooled connections
	 */
	private void setMinPoolSize(int value)
	{
		synchronized(poolSync) {
			minPoolSize = value;
		}
	}
	
	/**
	 * Get the current value of maximum number of pooled connections to create 
	 *
	 * @return value of maximum number of pooled connections
	 */
	private int getMaxPoolSize()
	{
		synchronized(poolSync) {
			return maxPoolSize;
		}
	}
	/**
	 * Set the current value of maximum number of pooled connections to create 
	 *
	 * @param value	value of maximum number of pooled connections
	 */
	private void setMaxPoolSize(int value)
	{
		synchronized(poolSync) {
			maxPoolSize = value;
		}
	}
	/**
	 * Get the current value of whether to trace all the sessions
	 *
	 * @return true if tracing is on for all sessions; false otherwise
	 */
	protected boolean getTraceAll()
	{
		synchronized(traceAllSync) {
			return traceAll;
		}
	}
	/**
	 * Set the current value of whether to trace all the sessions
	 *
	 * @param value	true if tracing is on for all sessions; false otherwise
	 */
	private void setTraceAll(boolean value)
	{
		synchronized(traceAllSync) {
			traceAll = value;
		}
	}
	/**
	 * Get the current value of trace directory
	 *
	 * @return trace directory
	 */
	protected String getTraceDirectory()
	{
		synchronized(traceDirectorySync) {
			return traceDirectory;
		}
	}
	/**
	 * Set the current value of trace directory
	 *
	 * @param value	trace directory
	 */
	private void setTraceDirectory(String value)
	{
		synchronized(traceDirectorySync) {
			traceDirectory = value;
		}
	}

	/**
	 * Connect to a database to test whether a connection can be made
	 *
	 * @param writer	connection to send message to
	 * @param database 	database directory to connect to
	 * @param user		user to use
	 * @param password	password to use
	 */
	private void connectToDatabase(DDMWriter writer, String database, String user, 
		String password) throws Exception
	{
		Properties p = new Properties();
		if (user != null)
			p.put("user", user);
		if (password != null)
			p.put("password", password);
	 	try {
     		Class.forName(CLOUDSCAPE_DRIVER);
		}
		catch (Exception e) {
			sendMessage(writer, ERROR, e.getMessage());
			return;
	  	}
	 	try {
			//Note, we add database to the url so that we can allow additional
			//url attributes
			Connection conn = DriverManager.getConnection(Attribute.PROTOCOL+database, p);
			// send warnings
			SQLWarning warn = conn.getWarnings();
			if (warn != null)
				sendSQLMessage(writer, warn, SQLWARNING);
			else
				sendOK(writer);
			conn.close();
			return;
	  	} catch (SQLException se) {
			sendSQLMessage(writer, se, SQLERROR);
	  	}
	}
	/**
	 * Boot database 
	 *
	 * @param writer	connection to send message to
	 * @param database 	database directory to connect to
	 * @param bootPassword	boot password
	 * @param encPrv	encryption provider
	 * @param encAlg	encryption algorithm
	 * @param user		user to use
	 * @param password	password to use
	 */
	private void startDatabase(DDMWriter writer, String database,
		String bootPassword, String encPrv, String encAlg, String user, 
			String password) throws Exception
	{
		Properties p = new Properties();
		if (bootPassword != null)
			p.put(Attribute.BOOT_PASSWORD, bootPassword);
		if (encPrv != null)
			p.put(Attribute.CRYPTO_PROVIDER, encPrv);
		if (encAlg != null)
			p.put(Attribute.CRYPTO_ALGORITHM, encAlg);
		if (user != null)
			p.put(Attribute.USERNAME_ATTR, user);
		if (password != null)
			p.put(Attribute.PASSWORD_ATTR, password);
	 	try {
     		Class.forName(CLOUDSCAPE_DRIVER);
		}
		catch (Exception e) {
			sendMessage(writer, ERROR, e.getMessage());
			return;
	  	}
	 	try {
			//Note, we add database to the url so that we can allow additional
			//url attributes
			Connection conn = DriverManager.getConnection(Attribute.PROTOCOL+database, p);
			SQLWarning warn = conn.getWarnings();
			if (warn != null)
				sendSQLMessage(writer, warn, SQLWARNING);
			else
				sendOK(writer);
			conn.close();
	  	} catch (SQLException se) {
			sendSQLMessage(writer, se, SQLERROR);
	  	} catch (Exception e) {
			sendMessage(writer, ERROR, e.getMessage());
		}
	}
	/**
	 * Shutdown a database 
	 *
	 * @param writer	connection to send message to
	 * @param database 	database directory to shutdown to
	 * @param user		user to use
	 * @param password	password to use
	 */
	private void shutdownDatabase(DDMWriter writer, String database, String user, 
		String password) throws Exception
	{

		StringBuffer url = new StringBuffer(Attribute.PROTOCOL + database);
		if (user != null)
			url.append(";user="+user);
		if (password != null)
			url.append(";password="+password);
		url.append(";shutdown=true");
	 	try {
     		Class.forName(CLOUDSCAPE_DRIVER);
		}
		catch (Exception e) {
			sendMessage(writer, ERROR, e.getMessage());
			return;
	  	}
	 	try {
			Connection conn = DriverManager.getConnection(url.toString());
			SQLWarning warn = conn.getWarnings();
			if (warn != null)
				sendSQLMessage(writer, warn, SQLWARNING);
			else
				sendOK(writer);
			conn.close();
	  	} catch (SQLException se) {
			//ignore shutdown error
			if (!(((EmbedSQLException)se).getMessageId() == SQLState.SHUTDOWN_DATABASE))
			{
				sendSQLMessage(writer, se, SQLERROR);
				return;
			}
			sendOK(writer);
	  	}
	}
	/**
	 * Wrap SQL Error - display to console and raise exception
	 *
	 * @param messageKey	Cloudscape SQL Exception message id
	 *
	 * @exception Exception raises exception for message
	 */
	private void wrapSQLError(String messageKey)
		throws Exception
	{
		consolePropertyMessage("DRDA_SQLException.S", messageKey);
	}

	/**
	 * Wrap SQL Warning - display to console and raise exception
	 *
	 * @param messageKey	Cloudscape SQL Exception message id
	 *
	 * @exception Exception raises exception for message
	 */
	private void wrapSQLWarning(String messageKey)
		throws Exception
	{
		consolePropertyMessage("DRDA_SQLWarning.I", messageKey);
	}
	private Properties getPropertyValues()
	{
		Properties retval = new Properties();
		retval.put(Property.DRDA_PROP_PORTNUMBER, new Integer(portNumber).toString());
		retval.put(Property.DRDA_PROP_HOSTNAME, hostArg);
		retval.put(Property.DRDA_PROP_KEEPALIVE, new Boolean(keepAlive).toString());

		String tracedir = getTraceDirectory();
		if (tracedir != null)
			retval.put(Property.DRDA_PROP_TRACEDIRECTORY, tracedir);
		retval.put(Property.DRDA_PROP_TRACEALL, new Boolean(getTraceAll()).toString());
		retval.put(Property.DRDA_PROP_MINTHREADS, new Integer(getMinThreads()).toString());
		retval.put(Property.DRDA_PROP_MAXTHREADS, new Integer(getMaxThreads()).toString());
		retval.put(Property.DRDA_PROP_TIMESLICE, new Integer(getTimeSlice()).toString());

		retval.put(Property.DRDA_PROP_TIMESLICE, new  Integer(getTimeSlice()).toString());
		retval.put(Property.DRDA_PROP_LOGCONNECTIONS, new Boolean(getLogConnections()).toString());
		String startDRDA = PropertyUtil.getSystemProperty(Property.START_DRDA);
		retval.put(Property.START_DRDA, (startDRDA == null)? "false" : startDRDA);

		//get the trace value for each session if tracing for all is not set
		if (!getTraceAll())
		{
			synchronized(sessionTable) {
				for (Enumeration e = sessionTable.elements(); e.hasMoreElements(); )
				{	
					Session session = (Session) e.nextElement();
					if (session.isTraceOn())
						retval.put(Property.DRDA_PROP_TRACE+"."+session.getConnNum(), "true");
				}
			}
		}
		return retval;
	}

	public String getErrorLogLocation ()
	{
		return errorLogLocation;
	}


	/**
	 * Add To Session Table - for use by ClientThread, add a new Session to the sessionTable.
	 *
	 * @param num	Connection number to register
	 * @param s	Session to add to the sessionTable
	 */
	protected void addToSessionTable(Integer i, Session s)
	{
		sessionTable.put(i, s);
	}

	/**
	 * Get New Conn Num - for use by ClientThread, generate a new connection number for the attempted Session.
	 *
	 * @return	a new connection number
	 */
	protected int getNewConnNum()
	{
		return ++connNum;
	}


	/**
	 * Get Free Threads - for use by ClientThread, get the number of 
	 * free threads in order to determine if
	 * a new thread can be run.
	 *
	 * @return	the number of free threads
	 */
	protected int getFreeThreads()
	{
		synchronized(runQueue)
		{
			return freeThreads;
		}
	}

	/**
	 * Get Thread List - for use by ClientThread, get the thread list 
	 * Vector so that a newly spawned thread
	 * can be run and added to the ThreadList from the ClientThread 
	 *
	 * @return	the threadList Vector
	 */
	protected Vector getThreadList()
	{
		return threadList;
	}
	
	protected Object getShutdownSync() { return shutdownSync; } 
	protected boolean getShutdown() { return shutdown; } 


	public String buildRuntimeInfo(LocalizedResource locallangUtil)
	{
		
		String s = locallangUtil.getTextMessage("DRDA_RuntimeInfoBanner.I")+ "\n";
		int sessionCount = 0;
		s += locallangUtil.getTextMessage("DRDA_RuntimeInfoSessionBanner.I") + "\n";
		for (int i = 0; i < threadList.size(); i++)
		{
			String sessionInfo  = ((DRDAConnThread)
								   threadList.get(i)).buildRuntimeInfo("",locallangUtil) ;
			if (!sessionInfo.equals(""))
			{
				sessionCount ++;
				s += sessionInfo + "\n";
			}
		}
		int waitingSessions = 0;
		for (int i = 0; i < runQueue.size(); i++)
		{
				s += ((Session)runQueue.get(i)).buildRuntimeInfo("", locallangUtil);
				waitingSessions ++;
		}
		s+= "-------------------------------------------------------------\n";
		s += locallangUtil.getTextMessage("DRDA_RuntimeInfoNumThreads.I") +
			threadList.size() + "\n";
		s += locallangUtil.getTextMessage("DRDA_RuntimeInfoNumActiveSessions.I") +
			sessionCount  +"\n";
		s +=locallangUtil.getTextMessage("DRDA_RuntimeInfoNumWaitingSessions.I") +
			+ waitingSessions + "\n\n";

		Runtime rt = Runtime.getRuntime();
		rt.gc();
		long totalmem = rt.totalMemory();
		long freemem = rt.freeMemory();
		s += locallangUtil.getTextMessage("DRDA_RuntimeInfoTotalMemory.I") +
			+ totalmem + "\t";
		s += locallangUtil.getTextMessage("DRDA_RuntimeInfoFreeMemory.I") +
			+ freemem + "\n\n";
		
		return s;
	}


	protected void setClientLocale(String locale)
	{
		clientLocale = locale;
	}

	/**
	 * Retrieve product version information
	 * We need to make sure that this method gets the stream and passes it to 
	 * ProductVersionHolder, because it lives in the Network Server jar
	 * and won't be readily available to ProductVersionHolder when running
	 * under security manager.
	 */
	private ProductVersionHolder getNetProductVersionHolder() throws Exception
	{
		ProductVersionHolder myPVH= null;
		try {
			myPVH = (ProductVersionHolder) AccessController.doPrivileged(
								new PrivilegedExceptionAction() {
										
									public Object run() throws UnknownHostException,IOException
									{
										InputStream versionStream = getClass().getResourceAsStream(ProductGenusNames.NET_INFO);

										return ProductVersionHolder.getProductVersionHolderFromMyEnv(versionStream);
									}
									});
		
}
		catch(PrivilegedActionException e) {
			Exception e1 = e.getException();
			consolePropertyMessage("DRDA_ProductVersionReadError.S", e1.getMessage());			
		}
		return myPVH;
	}

}






