/*

   Derby - Class org.apache.derby.impl.drda.NetworkServerControlImpl

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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.security.AccessControlException;
import java.security.AccessController;
import java.security.Permission;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.jdbc.AuthenticationService;
import org.apache.derby.iapi.jdbc.DRDAServerStarter;
import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.iapi.reference.DRDAConstants;
import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.iapi.reference.Module;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.services.info.ProductGenusNames;
import org.apache.derby.iapi.services.info.ProductVersionHolder;
import org.apache.derby.iapi.services.info.Version;
import org.apache.derby.iapi.services.jmx.ManagementService;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.tools.i18n.LocalizedOutput;
import org.apache.derby.iapi.tools.i18n.LocalizedResource;
import org.apache.derby.iapi.util.StringUtil;
import org.apache.derby.impl.jdbc.EmbedSQLException;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.mbeans.VersionMBean;
import org.apache.derby.mbeans.drda.NetworkServerMBean;
import org.apache.derby.security.SystemPermission;

/** 
    
    NetworkServerControlImpl does all the work for NetworkServerControl
    @see NetworkServerControl for description

*/
public final class NetworkServerControlImpl {
    private final static int NO_USAGE_MSGS= 12;
    private final static String [] COMMANDS = 
    {"start","shutdown","trace","tracedirectory","ping", 
     "logconnections", "sysinfo", "runtimeinfo",  "maxthreads", "timeslice", ""};
    // number of required arguments for each command
    private final static int [] COMMAND_ARGS =
    {0, 0, 1, 1, 0, 1, 0, 0, 1, 1, 0};
    public final static int COMMAND_START = 0;
    public final static int COMMAND_SHUTDOWN = 1;
    public final static int COMMAND_TRACE = 2;
    public final static int COMMAND_TRACEDIRECTORY = 3;
    public final static int COMMAND_TESTCONNECTION = 4;
    public final static int COMMAND_LOGCONNECTIONS = 5;
    public final static int COMMAND_SYSINFO = 6;
    public final static int COMMAND_RUNTIME_INFO = 7;
    public final static int COMMAND_MAXTHREADS = 8;
    public final static int COMMAND_TIMESLICE = 9;
    public final static int COMMAND_PROPERTIES = 10;
    public final static int COMMAND_UNKNOWN = -1;
    public final static String [] DASHARGS =
    {"p", "d", "user", "password", "ld", "ea", "ep", "b", "h", "s",
         "noSecurityManager", "ssl"};
    public final static int DASHARG_PORT = 0;
    public final static int DASHARG_DATABASE = 1;
    public final static int DASHARG_USER = 2;
    public final static int DASHARG_PASSWORD = 3;
    public final static int DASHARG_LOADSYSIBM = 4;
    public final static int DASHARG_ENCALG = 5;
    public final static int DASHARG_ENCPRV = 6;
    public final static int DASHARG_BOOTPASSWORD = 7;
    public final static int DASHARG_HOST = 8;
    public final static int DASHARG_SESSION = 9;
    public final static int DASHARG_UNSECURE = 10;
    private final static int DASHARG_SSL = 11;

    //All the commands except shutdown with username and password are at 
    //protocol level 1. 
    private final static int DEFAULT_PROTOCOL_VERSION = 1;
    // DERBY-2109: shutdown command now transmits optional user credentials
    //For shutdown with username/password, we have added a new protocol level
    private final static int SHUTDOWN_WITH_CREDENTIAL_PROTOCOL_VERSION = 2;
    //The highest protocol level is 2. The reason for it to be at 2 is 
    //the shutdown command with username/password
    private final static int MAX_ALLOWED_PROTOCOL_VERSION = 2;

    private final static String COMMAND_HEADER = "CMD:";
    private final static String REPLY_HEADER = "RPY:";
    private final static int REPLY_HEADER_LENGTH = REPLY_HEADER.length();
    private final static int OK = 0;
    private final static int WARNING = 1;
    private final static int ERROR = 2;
    private final static int SQLERROR = 3;
    private final static int SQLWARNING = 4;

    private final static String DRDA_PROP_MESSAGES = "org.apache.derby.loc.drda.messages";
    private final static String DRDA_PROP_DEBUG = "derby.drda.debug";
    private final static String CLOUDSCAPE_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";

    public final static String UNEXPECTED_ERR = "Unexpected exception";

    private final static int MIN_MAXTHREADS = -1;
    private final static int MIN_TIMESLICE = -1;
    private final static int USE_DEFAULT = -1;
    private final static int DEFAULT_MAXTHREADS = 0; //for now create whenever needed
    private final static int DEFAULT_TIMESLICE = 0; //for now never yield

    private final static String DEFAULT_HOST = "localhost";
    private final static String DRDA_MSG_PREFIX = "DRDA_";
    private final static String DEFAULT_LOCALE= "en";
    private final static String DEFAULT_LOCALE_COUNTRY="US";

    // Check up to 10 seconds to see if shutdown occurred
    private final static int SHUTDOWN_CHECK_ATTEMPTS = 100;
    private final static int SHUTDOWN_CHECK_INTERVAL= 100;

    // maximum reply size
    private final static int MAXREPLY = 32767;

    // Application Server Attributes.
    protected static String att_srvclsnm;
    protected final static String ATT_SRVNAM = "NetworkServerControl";

    protected static String att_extnam;
    protected static String att_srvrlslv; 
    protected static String prdId;
    protected static byte[] prdIdBytes_;
    
    private static String buildNumber;
    private static String versionString;
    // we will use single or mixed, not double byte to reduce traffic on the
    // wire, this is in keeping with JCC
    // Note we specify UTF8 for the single byte encoding even though it can
    // be multi-byte.
    protected final static int CCSIDSBC = 1208; //use UTF8
    protected final static int CCSIDMBC = 1208; //use UTF8
    protected final static String DEFAULT_ENCODING = "UTF8"; // use UTF8 for writing
    final static Charset DEFAULT_CHARSET = Charset.forName(DEFAULT_ENCODING);
    protected final static int DEFAULT_CCSID = 1208;
    protected final static byte SPACE_CHAR = 32;
                                                        

    // Application Server manager levels - this needs to be in sync
    // with CodePoint.MGR_CODEPOINTS
    protected final static int [] MGR_LEVELS = { 7, // AGENT
                                                 4, // CCSID Manager
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
                                                 1208, // UNICODE Manager
                                                 7  // XAMGR
                                                };
                                            
    
    protected PrintWriter logWriter;                        // console
    protected PrintWriter cloudscapeLogWriter;              // derby.log
    private static Driver cloudscapeDriver;

    // error types
    private final static int ERRTYPE_SEVERE = 1;
    private final static int ERRTYPE_USER = 2;
    private final static int ERRTYPE_INFO = 3;
    private final static int ERRTYPE_UNKNOWN = -1;

    // command argument information
    private Vector<String> commandArgs = new Vector<String>();
    private String databaseArg;
    // DERBY-2109: Note that derby JDBC clients have a default user name
    // "APP" (= Property.DEFAULT_USER_NAME) assigned if they don't provide
    // credentials.  We could do the same for NetworkServerControl clients
    // here, but this class is robust enough to allow for null as default.
    private String userArg = null;
    private String passwordArg = null;
    private String bootPasswordArg;
    private String encAlgArg;
    private String encPrvArg;
    private String hostArg = DEFAULT_HOST;
    private InetAddress hostAddress;
    private int sessionArg;
    private boolean unsecureArg;

    // Used to debug memory in SanityManager.DEBUG mode
    private memCheck mc;

    // reply buffer
    private byte [] replyBuffer;    
    private int replyBufferCount;   //length of reply
    private int replyBufferPos;     //current position in reply

    //
    // server configuration
    //
    // static values - set at start can't be changed once server has started
    private int portNumber = NetworkServerControl.DEFAULT_PORTNUMBER;   // port server listens to

    // configurable values
    private String traceDirectory;      // directory to place trace files in
    private Object traceDirectorySync = new Object();// object to use for syncing
    private boolean traceAll;           // trace all sessions
    private Object traceAllSync = new Object(); // object to use for syncing reading
                                        // and changing trace all
    private Object serverStartSync = new Object();  // for syncing start of server.
    private boolean logConnections;     // log connects
    private Object logConnectionsSync = new Object(); // object to use for syncing 
                                        // logConnections value
    private int minThreads;             // default minimum number of connection threads
    private int maxThreads;             // default maximum number of connection threads
    private Object threadsSync = new Object(); // object to use for syncing reading
                                        // and changing default min and max threads
    private int timeSlice;              // default time slice of a session to a thread
    private Object timeSliceSync = new Object();// object to use for syncing reading
                                        // and changing timeSlice

    private boolean keepAlive = true;   // keepAlive value for client socket 
    private int minPoolSize;            //minimum pool size for pooled connections
    private int maxPoolSize;            //maximum pool size for pooled connections
    private Object poolSync = new Object(); // object to use for syning reading

    protected boolean debugOutput = false;
    private boolean cleanupOnStart = false; // Should we clean up when starting the server?
    private boolean restartFlag = false;

    protected final static int INVALID_OR_NOTSET_SECURITYMECHANISM = -1; 
    // variable to store value set to derby.drda.securityMechanism
    // default value is -1 which indicates that this property isnt set or
    // the value is invalid
    private int allowOnlySecurityMechanism = INVALID_OR_NOTSET_SECURITYMECHANISM;
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
    private int connNum;        // number of connections since server started
    private ServerSocket serverSocket;
    private NetworkServerControlImpl serverInstance;
    private LocalizedResource langUtil;
    public String clientLocale;
    /** List of local addresses for checking admin commands. */
    ArrayList<InetAddress> localAddresses;

    // open sessions
    private Hashtable<Integer, Session> sessionTable =
            new Hashtable<Integer, Session>();

    // current session
    private Session currentSession;
    // DRDAConnThreads
    private Vector<DRDAConnThread> threadList = new Vector<DRDAConnThread>();

    // queue of sessions waiting for a free thread - the queue is managed
    // in a simple first come, first serve manner - no priorities
    private Vector<Session> runQueue = new Vector<Session>();

    // number of DRDAConnThreads waiting for something to do
    private int freeThreads;

    // known application requesters
    private Hashtable<String, AppRequester> appRequesterTable =
            new Hashtable<String, AppRequester>();

    // accessed by inner classes for privileged action
    private String propertyFileName;
    private NetworkServerControlImpl thisControl = this;

    // if the server is started from the command line, it should shutdown the
    // databases it has booted.
    private boolean shutdownDatabasesOnShutdown = false;

    // SSL related stuff
    private static final int SSL_OFF = 0;
    private static final int SSL_BASIC = 1;
    private static final int SSL_PEER_AUTHENTICATION = 2;

    private int sslMode = SSL_OFF;

    /**
     * Can EUSRIDPWD security mechanism be used with 
     * the current JVM
     */
    private static boolean SUPPORTS_EUSRIDPWD = false;

    /*
     * DRDA Specification for the EUSRIDPWD security mechanism
     * requires DH algorithm support with a 32-byte prime to be
     * used. Not all JCE implementations have support for this.
     * Hence here we need to find out if EUSRIDPWD can be used
     * with the current JVM.
     */ 
    static
    {
        try
        {
            // The DecryptionManager class will instantiate objects of the required 
            // security algorithms that are needed for EUSRIDPWD
            // An exception will be thrown if support is not available
            // in the JCE implementation in the JVM in which the server
            // is started.
            new DecryptionManager();
            SUPPORTS_EUSRIDPWD = true;
        }catch(Exception e)
        {
            // if an exception is thrown, ignore exception.
            // set SUPPORTS_EUSRIDPWD to false indicating that the server 
            // does not have support for EUSRIDPWD security mechanism
            SUPPORTS_EUSRIDPWD = false;
        }
    }

    /**
     * Get the host where we listen for connections.
     */
    public  String  getHost() { return hostArg; }

    /**
     * Get the port where we listen for connections.
     * @return the port number
     */
    public int getPort() {
        return portNumber;
    }

    /**
     * Return true if the customer forcibly overrode our decision to install a
     * default SecurityManager.
     */
    public  boolean runningUnsecure() { return unsecureArg; }
    
    // constructor
    public NetworkServerControlImpl() throws Exception
    {
        init();
        getPropertyInfo();
    }

    /**
     * Internal constructor for NetworkServerControl API. 
     * @param address InetAddress to listen on, throws NPE if null
     * @param portNumber portNumber to listen on, -1 use property or default
     * @throws Exception on error
     * @see NetworkServerControl
     */
    public NetworkServerControlImpl(InetAddress address, int portNumber) throws Exception
    {
        this();
        this.hostAddress = address;
        this.portNumber = (portNumber <= 0) ?
            this.portNumber: portNumber;
        this.hostArg = address.getHostAddress();
    }

    /**
     * Internal constructor for NetworkServerControl API. 
     * @param userName the user name for actions requiring authorization
     * @param password the password for actions requiring authorization
     * @throws Exception on error
     * @see NetworkServerControl
     */
    public NetworkServerControlImpl(String userName, String password)
        throws Exception
    {
        this();
        this.userArg = userName;
        this.passwordArg = password;
    }

    /**
     * Internal constructor for NetworkServerControl API. 
     * @param address InetAddress to listen on, throws NPE if null
     * @param portNumber portNumber to listen on, -1 use property or default
     * @param userName the user name for actions requiring authorization
     * @param password the password for actions requiring authorization
     * @throws Exception on error
     * @see NetworkServerControl
     */
    public NetworkServerControlImpl(InetAddress address, int portNumber,
                                    String userName, String password)
        throws Exception
    {
        this(address, portNumber);
        this.userArg = userName;
        this.passwordArg = password;
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
        versionString = myPVH.getVersionBuildString(true);
        
        String majorStr = String.valueOf(myPVH.getMajorVersion());
        String minorStr = String.valueOf(myPVH.getMinorVersion());
        // Maintenance version. Server protocol version.
        // Only changed if client needs to recognize a new server version.
        String drdaMaintStr = String.valueOf(myPVH.getDrdaMaintVersion());

        // PRDID format as JCC expects it: CSSMMmx
        // MM = major version
        // mm = minor version
        // x = drda MaintenanceVersion

        prdId = DRDAConstants.DERBY_DRDA_SERVER_ID;
        if (majorStr.length() == 1)
            prdId += "0";
        prdId += majorStr;

        if (minorStr.length() == 1)
            prdId += "0";

        prdId += minorStr;
        
        prdId += drdaMaintStr;
        att_srvrlslv = prdId + "/" + myPVH.getVersionBuildString(true);
                // Precompute this to save some cycles
                prdIdBytes_ = prdId.getBytes(DEFAULT_ENCODING);
 
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

    protected static Driver getDriver()
    {
        return cloudscapeDriver;
    }
    

    /********************************************************************************
     * Implementation of NetworkServerControl API
     * The server commands throw exceptions for errors, so that users can handle
     * them themselves in addition to having the errors written to the console
     * and possibly derby.log.  To turn off logging the errors to the console,
     * set the output writer to null.
     ********************************************************************************/


    /**
     * Set the output stream for console messages
     * If this is set to null, no messages will be written to the console
     *
     * @param outWriter output stream for console messages
     */
    public void setLogWriter(PrintWriter outWriter)
    {
        // wrap the user-set outWriter with, autoflush to true.
        // this will ensure that messages to console will be 
        // written out to the outWriter on a println.
        // DERBY-1466
        if ( outWriter != null )
            logWriter = new PrintWriter(outWriter,true);
        else
            logWriter = outWriter;
    }


    
    /**
     * Write an error message to console output stream
     * and throw an exception for this error
     *
     * @param msg   error message
     * @exception Exception
     */
    public void consoleError(String msg)
        throws Exception
    {
        consoleMessage(msg, true);
        throw new Exception(msg);
    }

    /**
     * Write an exception to console output stream,
     * but only if debugOutput is true.
     *
     * @param e exception
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
     * @param e exception
     */
    public void consoleExceptionPrintTrace(Throwable e)
    {
        consoleMessage(e.getMessage(), true);
        PrintWriter lw = logWriter;
        if (lw != null)
        {
            synchronized (lw) {
                e.printStackTrace(lw);
            }
        }
        // DERBY-5610 - If there is no log writer, only print
        // exception to System.out if derby.drda.debug=true
        else if (debugOutput)
        {
            e.printStackTrace();
        }
        
        lw = cloudscapeLogWriter;
        if (lw != null)
        {
            synchronized(lw) {
                e.printStackTrace(lw);
            }
        }
    }




    /**
     * Write a message to console output stream
     *
     * @param msg   message
     * @param printTimeStamp Whether to prepend a timestamp to the message or not
     */
    public void consoleMessage(String msg, boolean printTimeStamp)
    {
        // print to console if we have one
        PrintWriter lw = logWriter;
        if (lw != null)
        {
            synchronized(lw) {
                if (printTimeStamp) {
                    lw.println(new Date() + " : " + msg);
                } else {
                    lw.println(msg);                    
                }
            }
        }
        // always print to derby.log
        lw = cloudscapeLogWriter;
        if (lw != null)
            synchronized(lw)
            {
                if (printTimeStamp) {
                    Monitor.logMessage(new Date() + " : " + msg);
                } else {
                    Monitor.logMessage(msg);
                }
            }
    }



    /**
     * Start a network server.  Launches a separate thread with 
     * DRDAServerStarter.  Want to use Monitor.startModule,
     * so it can all get shutdown when Derby shuts down, but 
     * can't get it working right now.
     *
     * @param consoleWriter   PrintWriter to which server console will be 
     *                        output. Null will disable console output.
     *
     *
     * @exception Exception throws an exception if an error occurs
     */
    public void start(PrintWriter consoleWriter)
        throws Exception
    {
        DRDAServerStarter starter = new DRDAServerStarter();
        starter.setStartInfo(hostAddress,portNumber,consoleWriter);
        this.setLogWriter(consoleWriter);
        startNetworkServer();
        starter.boot(false,null);
    }

    /**
     * Create the right kind of server socket
     */
    
    private ServerSocket createServerSocket()
        throws IOException
    {
        if (hostAddress == null)
            hostAddress = InetAddress.getByName(hostArg);
        // Make a list of valid
        // InetAddresses for NetworkServerControl
        // admin commands.
        buildLocalAddressList(hostAddress);
                                            
        // Create the right kind of socket
        switch (getSSLMode()) {
        case SSL_OFF:
        default:
            ServerSocketFactory sf =
                ServerSocketFactory.getDefault();
            return sf.createServerSocket(portNumber
                                         ,0,
                                         hostAddress);
        case SSL_BASIC:
            SSLServerSocketFactory ssf =
                (SSLServerSocketFactory)SSLServerSocketFactory.getDefault();
            return (SSLServerSocket)ssf.createServerSocket(portNumber,
                                                           0,
                                                           hostAddress);
        case SSL_PEER_AUTHENTICATION:
            SSLServerSocketFactory ssf2 =
                (SSLServerSocketFactory)SSLServerSocketFactory.getDefault();
            SSLServerSocket sss2= 
                (SSLServerSocket)ssf2.createServerSocket(portNumber,
                                                         0,
                                                         hostAddress);
            sss2.setNeedClientAuth(true);
            return sss2;
        }
    }
    

    /**
     * Start a network server
     *
     * @param consoleWriter   PrintWriter to which server console will be 
     *                        output. Null will disable console output.
     *
     *
     * @exception Exception throws an exception if an error occurs
     */
    public void blockingStart(PrintWriter consoleWriter)
        throws Exception
    {
        startNetworkServer();
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
            serverSocket = 
                AccessController.doPrivileged(
                    new PrivilegedExceptionAction<ServerSocket>() {
                        public ServerSocket run() throws IOException
                        {
                            return createServerSocket();
                        }
                    });
        } catch (PrivilegedActionException e) {
            Exception e1 = e.getException();

            // Test for UnknownHostException first since it's a
            // subbclass of IOException (and consolePropertyMessage
            // throws an exception when the severity is S (or U).
            if (e1 instanceof UnknownHostException) {
                consolePropertyMessage("DRDA_UnknownHost.S", hostArg);
            } else if (e1 instanceof IOException) {
                consolePropertyMessage("DRDA_ListenPort.S", 
                                       new String [] {
                                           Integer.toString(portNumber), 
                                           hostArg,
                                           // Since SocketException
                                           // is used for a phletora
                                           // of situations, we need
                                           // to communicate the
                                           // underlying exception
                                           // string to the user.
                                           e1.toString()}); 
            } else {
                throw e1;
            }
        } catch (Exception e) {
        // If we find other (unexpected) errors, we ultimately exit--so make
        // sure we print the error message before doing so (Beetle 5033).
            throwUnexpectedException(e);
        }
        
        switch (getSSLMode()) {
        default:
        case SSL_OFF:
            consolePropertyMessage("DRDA_Ready.I", new String [] 
                {Integer.toString(portNumber), att_srvclsnm, versionString});
            break;
        case SSL_BASIC:
            consolePropertyMessage("DRDA_SSLReady.I", new String [] 
                {Integer.toString(portNumber), att_srvclsnm, versionString});
            break;
        case SSL_PEER_AUTHENTICATION:
            consolePropertyMessage("DRDA_SSLClientAuthReady.I", new String [] 
                {Integer.toString(portNumber), att_srvclsnm, versionString});
            break;
        }

        // First, register any MBeans. We do this before we start accepting
        // connections from the clients to ease testing of JMX (DERBY-3689).
        // This way we know that once we can connect to the network server,
        // the MBeans will be available.
        ManagementService mgmtService = ((ManagementService)
                Monitor.getSystemModule(Module.JMX));

        final Object versionMBean = mgmtService.registerMBean(
                           new Version(
                                   getNetProductVersionHolder(),
                                   SystemPermission.SERVER),
                           VersionMBean.class,
                           "type=Version,jar=derbynet.jar");
        final Object networkServerMBean = mgmtService.registerMBean(
                            new NetworkServerMBeanImpl(this),
                            NetworkServerMBean.class,
                            "type=NetworkServer");

        // We accept clients on a separate thread so we don't run into a problem
        // blocking on the accept when trying to process a shutdown
        final ClientThread clientThread = AccessController.doPrivileged(
                new PrivilegedExceptionAction<ClientThread>() {
                    public ClientThread run() throws Exception {
                        return new ClientThread(thisControl, serverSocket);
                    }
                });
        clientThread.start();

        try {
            // wait until we are told to shutdown or someone sends an InterruptedException
            synchronized(shutdownSync) {
                try {
                    while (!shutdown) {
                        shutdownSync.wait();
                    }
                }
                catch (InterruptedException e)
                {
                    shutdown = true;
                }
            }
            
            try {
                AccessController.doPrivileged(
                        new PrivilegedAction<Void>() {
                            public Void run() {
                            // Need to interrupt the memcheck thread if it is sleeping.
                                if (mc != null)
                                    mc.interrupt();

                                //interrupt client thread
                                clientThread.interrupt();

                                return null;
                           }
                        });
            } catch (Exception exception) {
                consolePrintAndIgnore("DRDA_UnexpectedException.S", exception, true);
            }
            
            // Close out the sessions
            synchronized(sessionTable) {
                for (Session session : sessionTable.values())
                {
                    try {
                        session.close();
                    } catch (Exception exception) {
                        consolePrintAndIgnore("DRDA_UnexpectedException.S", exception, true);
                    }
                }
            }

            synchronized (threadList)
            {
                //interupt any connection threads still active
                for (final DRDAConnThread threadi : threadList)
                {
                    try {
                        threadi.close();
                        AccessController.doPrivileged(
                                new PrivilegedAction<Void>() {
                                    public Void run() {
                                        threadi.interrupt();
                                        return null;
                                    }
                                });
                    } catch (Exception exception) {
                        consolePrintAndIgnore("DRDA_UnexpectedException.S", exception, true);
                    }
                }
                threadList.clear();
            }

            // close the listener socket
            try{
               serverSocket.close();
            }catch(IOException e){
                consolePropertyMessage("DRDA_ListenerClose.S", true);
            } catch (Exception exception) {
                consolePrintAndIgnore("DRDA_UnexpectedException.S", exception, true);
            }

            // Wake up those waiting on sessions, so
            // they can close down
            try{
                synchronized (runQueue) {
                    runQueue.notifyAll();
                }
            } catch (Exception exception) {
                consolePrintAndIgnore("DRDA_UnexpectedException.S", exception, true);
            }
            
            // And now unregister any MBeans.
            try {
                mgmtService.unregisterMBean(versionMBean);
                mgmtService.unregisterMBean(networkServerMBean);
            } catch (Exception exception) {
                consolePrintAndIgnore("DRDA_UnexpectedException.S", exception, true);
            }

            if (shutdownDatabasesOnShutdown) {

                // Shutdown Derby
                try {
                    // tell driver to shutdown the engine
                    if (cloudscapeDriver != null) {
                        // DERBY-2109: pass user credentials for driver shutdown
                        final Properties p = new Properties();
                        if (userArg != null) {
                            p.setProperty("user", userArg);
                        }
                        if (passwordArg != null) {
                            p.setProperty("password", passwordArg);
                        }
                        // DERBY-6224: DriverManager.deregisterDriver() requires
                        // an extra permission in JDBC 4.2 and later. Invoke
                        // system shutdown with deregister=false to avoid the
                        // need for the extre permission in the default server
                        // policy. Since the JVM is about to terminate, we don't
                        // care whether the JDBC driver is deregistered.
                        cloudscapeDriver.connect(
                            "jdbc:derby:;shutdown=true;deregister=false", p);
                    }
                } catch (SQLException sqle) {
                    // If we can't shutdown Derby, perhaps, authentication has
                    // failed or System Privileges weren't granted. We will just
                    // print a message to the console and proceed.
                    String expectedState =
                        StandardException.getSQLStateFromIdentifier(
                                SQLState.CLOUDSCAPE_SYSTEM_SHUTDOWN);
                    if (!expectedState.equals(sqle.getSQLState())) {
                        consolePropertyMessage("DRDA_ShutdownWarning.I",
                                               sqle.getMessage());
                    }
                } catch (Exception exception) {
                    consolePrintAndIgnore("DRDA_UnexpectedException.S", exception, true);
                }
            }

            consolePropertyMessage("DRDA_ShutdownSuccess.I", new String [] 
                                    {att_srvclsnm, versionString});
            
        } catch (Exception ex) {
            try {
                //If the console printing is not available,  then we have
                //a simple stack trace print below to atleast print some
                //exception info
                consolePrintAndIgnore("DRDA_UnexpectedException.S", ex, true);
            } catch (Exception e) {}
            ex.printStackTrace();
        }
    }
    
    //Print the passed exception on the console and ignore it after that
    private void consolePrintAndIgnore(String msgProp, 
            Exception e, boolean printTimeStamp) {
        // catch the exception consolePropertyMessage will throw since we
        // just want to print information about it and move on.
        try {
            consolePropertyMessage(msgProp, true);
        } catch (Exception ce) {} 
        consoleExceptionPrintTrace(e);
    }
    
    /** 
     * Load Derby and save driver for future use.
     * We can't call Driver Manager when the client connects, 
     * because they might be holding the DriverManager lock.
     *
     * 
     */

    


    protected void startNetworkServer() throws Exception
    {

        // we start the Derby server here.
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
                        synchronized (runQueue) {
                            for (Session s : runQueue) {
                                s.close();
                                removeFromSessionTable(s.getConnNum());
                            }
                            runQueue.clear();
                        }

                        // DERBY-1326: There could be active threads that
                        // contain old/invalid sessions. These sessions won't
                        // be cleaned up until there is some activity on
                        // them. We could optimize this by going through
                        // sessionTable and closing the sessions' socket
                        // streams.

                        // Unload driver, then restart the server.
                        cloudscapeDriver = null;    // so it gets collected.
                        System.gc();
                    }

                    // start the server.
                    cloudscapeDriver = (Driver) Class.forName(CLOUDSCAPE_DRIVER).newInstance();

                }
                catch (Exception e) {
                    this.consoleExceptionPrintTrace(e);
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
     * @exception Exception throws an exception if an error occurs
     */
    public void shutdown()
        throws Exception
    {
        // Wait up to 10 seconds for things to really shut down
        // need a quiet ping so temporarily disable the logwriter
        PrintWriter savWriter;
        int ntry;
        try {
            setUpSocket();
            try {
                writeCommandHeader(COMMAND_SHUTDOWN, SHUTDOWN_WITH_CREDENTIAL_PROTOCOL_VERSION);
                // DERBY-2109: transmit user credentials for System Privileges check
                writeLDString(userArg);
                writeLDString(passwordArg);
                send();
                readResult();
            } catch (Exception e) {
                //The shutdown command with protocol level 2 failed. If 
                //the username or password were supplied then we can't 
                //try the shutdown with protocol level 1 because protocol
                //leve 1 does not support username/password. Because of
                //that, we should simply throw the caught exception to the
                //client
                if(userArg != null || passwordArg != null)
                    throw e;
                //If no username and password is specified then we can try
                //shutdown with the old protocol level of 1 which is the 
                //default protocol level. But this can be tried only if the
                //exception for attempt of shutdown with protocol level 2
                //was DRDA_InvalidReplyHead. This can happen if we are 
                //dealing with an older Network server product which do not
                //recognize shutdown at protocol level 2.
                if (e.getMessage().indexOf("DRDA_InvalidReplyHead") != -1)
                {
                    try {
                        closeSocket();
                        setUpSocket();
                        writeCommandHeader(COMMAND_SHUTDOWN);
                        send();
                        readResult();
                    } catch (Exception e1) {
                        e1.initCause(e);
                        throw e1;
                    }
                }
                else
                    throw e;
            }
            savWriter = logWriter;
            // DERBY-1571: If logWriter is null, stack traces are printed to
            // System.err. Set logWriter to a silent stream to suppress stack
            // traces too.
            FilterOutputStream silentStream = new FilterOutputStream(null) {
                public void write(int b) {
                }

                public void flush() {
                }

                public void close() {
                }
            };
            setLogWriter(new PrintWriter(silentStream));
            for (ntry = 0; ntry < SHUTDOWN_CHECK_ATTEMPTS; ntry++) {
                Thread.sleep(SHUTDOWN_CHECK_INTERVAL);
                try {
                    pingWithNoOpen();
                } catch (Exception e) {
                    // as soon as we can't ping return
                    break;
                }
            }
        } finally {
            closeSocket();
        }
        
        if (ntry == SHUTDOWN_CHECK_ATTEMPTS)
            consolePropertyMessage("DRDA_ShutdownError.S", new String [] {
                Integer.toString(portNumber), 
                hostArg}); 
        
        logWriter= savWriter;
        return;
    }

    /**
     * Authenticates the user and checks for shutdown System Privileges.
     * No Network communication needed.
     *
     * To perform this check the following policy grant is required
     * <ul>
     * <li> to run the encapsulated test:
     *      permission javax.security.auth.AuthPermission "doAsPrivileged";
     * </ul>
     * or a SQLException will be raised detailing the cause.
     * <p>
     * In addition, for the test to succeed
     * <ul>
     * <li> the given user needs to be covered by a grant:
     *      principal org.apache.derby.authentication.SystemPrincipal "..." {}
     * <li> that lists a shutdown permission:
     *      permission org.apache.derby.security.SystemPermission "shutdown";
     * </ul>
     * or it will fail with a SQLException detailing the cause.
     *
     * @param user The user to be checked for shutdown privileges
     * @throws SQLException if the privileges check fails
     */
    /**
     * @throws SQLException if authentication or privileges check fails
     */
    public void checkShutdownPrivileges() throws SQLException {    
        // get the system's authentication service
        final AuthenticationService auth
            = ((AuthenticationService)
               Monitor.findService(AuthenticationService.MODULE,
                                   "authentication"));

        // authenticate user
        if (auth != null) {
            final Properties finfo = new Properties();
            if (userArg != null) {
                finfo.setProperty("user", userArg);
            }
            if (passwordArg != null) {
                finfo.setProperty("password", passwordArg);
            }
            if (!auth.authenticate((String)null, finfo)) {
                // not a valid user
                throw Util.generateCsSQLException(
                SQLState.NET_CONNECT_AUTH_FAILED,
                MessageService.getTextMessage(MessageId.AUTH_INVALID));
            }
        }

        // approve action if not running under a security manager
        if (System.getSecurityManager() == null) {
            return;
        }

        // the check
        try {
            final Permission sp  = new SystemPermission(
                  SystemPermission.SERVER, SystemPermission.SHUTDOWN);
            // For porting the network server to J2ME/CDC, consider calling
            // abstract method InternalDriver.checkShutdownPrivileges(user)
            // instead of static SecurityUtil.checkUserHasPermission().
            // SecurityUtil.checkUserHasPermission(userArg, sp);
        } catch (AccessControlException ace) {
            throw Util.generateCsSQLException(
                SQLState.AUTH_SHUTDOWN_MISSING_PERMISSION,
                userArg, (Object)ace); // overloaded method
        }
    }

    /*
     Shutdown the server directly (If you have the original object)
     No Network communication needed.
    */
    public void directShutdown() throws SQLException {
        // DERBY-2109: the public shutdown method now checks privileges
        checkShutdownPrivileges();
        directShutdownInternal();
    }
    
    /*
     Shutdown the server directly (If you have the original object)
     No Network communication needed.
    */
    void directShutdownInternal() {
        // DERBY-2109: the direct, unchecked shutdown is made private
        synchronized(shutdownSync) {                        
            shutdown = true;
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

    /**
     * Ping opening an new socket and close it.
     * @throws Exception
     */
    public void ping() throws Exception
    {
        try {
            setUpSocket();
            pingWithNoOpen();
        } finally {
            closeSocket();
        }
    }
    
    /**
     * Ping the server using the client socket that is already open.
     */
    private void pingWithNoOpen() throws Exception
    {
    
        // database no longer used, but don't change the protocol 
        // in case we add
        // authorization  later.
        String database = null; // no longer used but don't change the protocol
        String user = null;
        String password = null;

            try {
            writeCommandHeader(COMMAND_TESTCONNECTION);
            writeLDString(database);
            writeLDString(user);
            writeLDString(password);
            send();
            readResult();
            } catch (IOException ioe) {
                consolePropertyMessage("DRDA_NoIO.S",
                        new String [] {hostArg, 
                        (new Integer(portNumber)).toString(), 
                        ioe.getMessage()}); 
    }
    }


    /**
     * Turn tracing on or off for all sessions
     *
     * @param on            true to turn tracing on, false to turn tracing off
     *
     * @exception Exception throws an exception if an error occurs
     */
    public void trace(boolean on)
        throws Exception
    {
        trace(0, on);
    }

    /**
     * Turn tracing on or off for one session or all sessions
     *
     * @param connNum   the connNum of the session, 0 if all sessions
     * @param on            true to turn tracing on, false to turn tracing off
     *
     * @exception Exception throws an exception if an error occurs
     */
    public void trace(int connNum, boolean on)
        throws Exception
    {
        try {
            setUpSocket();
            writeCommandHeader(COMMAND_TRACE);
            commandOs.writeInt(connNum);
            writeByte(on ? 1 : 0);
            send();
            readResult();
            consoleTraceMessage(connNum, on);
        } finally {
            closeSocket();
        }
        
    }

    /**
     * Print trace change message to console
     *
     * @param on            true to print tracing on, false to print tracing off
     *
     * @exception Exception throws an exception if an error occurs
     */
    private void consoleTraceMessage(int connNum, boolean on)
        throws Exception
    {
        String  messageID;
        String[]    args = null;
        
        if (connNum == 0)
        {
            if ( on ) { messageID = "DRDA_TraceChangeAllOn.I"; }
            else { messageID = "DRDA_TraceChangeAllOff.I"; }
        }
        else
        {
            if ( on ) { messageID = "DRDA_TraceChangeOneOn.I"; }
            else { messageID = "DRDA_TraceChangeOneOff.I"; }

            args = new String[] { Integer.toString(connNum) };
            
        }
        
        consolePropertyMessage( messageID, args );
    }

    /**
     * Turn logging connections on or off. When logging is turned on a message is
     * written to derby.log each time a connection is made.
     *
     * @param on            true to turn on, false to turn  off
     *
     * @exception Exception throws an exception if an error occurs
     */
    public void logConnections(boolean on)
        throws Exception
    {
        try {
            setUpSocket();
            writeCommandHeader(COMMAND_LOGCONNECTIONS);
            writeByte(on ? 1 : 0);
            send();
            readResult();
        } finally {
            closeSocket();
        }
    }

    /**
     *@see NetworkServerControl#setTraceDirectory
     */
    public void sendSetTraceDirectory(String traceDirectory)
        throws Exception
    {
        try {
            setUpSocket();
            writeCommandHeader(COMMAND_TRACEDIRECTORY);
            writeLDString(traceDirectory);
            send();
            readResult();
        } finally {
            closeSocket();
        }
        
    }

    /**
     *@see NetworkServerControl#getSysinfo
     */
    public String sysinfo()
        throws Exception
    {
        try {
            setUpSocket();
            writeCommandHeader(COMMAND_SYSINFO);
            send();
            return readStringReply("DRDA_SysInfoError.S");
        } finally {
            closeSocket();
        }
    }

    /**
     *@see NetworkServerControl#getRuntimeInfo
     */
    public String runtimeInfo()
    throws Exception 
    {
        try {
            setUpSocket();
            writeCommandHeader(COMMAND_RUNTIME_INFO);
            send();
            return readStringReply("DRDA_RuntimeInfoError.S");
        } finally {
            closeSocket();
        }
    }

    /**
     * Display usage information
     *
     */
    public void usage()
    {
        try {
        for (int i = 1; i <= NO_USAGE_MSGS; i++)
            consolePropertyMessage("DRDA_Usage"+i+".I", false);
        } catch (Exception e) {}    // ignore exceptions - there shouldn't be any
    }

    /**
     * Connect to  network server and set connection maxthread parameter
     *
     * @param max       maximum number of connections, if 0, connections
     *                      created when no free connection available
     *                      if -1, use default
     *
     * @exception Exception throws an exception if an error occurs
     */
    public void netSetMaxThreads(int max) throws Exception
    {
        try {
            setUpSocket();
            writeCommandHeader(COMMAND_MAXTHREADS);
            commandOs.writeInt(max);
            send();
            readResult();
            int newval = readInt();
            consolePropertyMessage("DRDA_MaxThreadsChange.I",
                    String.valueOf(newval));
        } finally {
            closeSocket();
        }
        
    }

    /**
     * Set network server connection timeslice parameter
     *
     * @param timeslice amount of time given to each session before yielding to
     *                      another session, if 0, never yield. if -1, use default.
     *
     * @exception Exception throws an exception if an error occurs
     */
    public void netSetTimeSlice(int timeslice)
        throws Exception
    {
        try {
            setUpSocket();
            writeCommandHeader(COMMAND_TIMESLICE);
            commandOs.writeInt(timeslice);
            send();
            readResult();
            int newval = readInt();
            consolePropertyMessage("DRDA_TimeSliceChange.I",
                    String.valueOf(newval));
        } finally {
            closeSocket();
        }  
    }

    /**
     * Get current properties
     *
     * @return Properties object containing properties
     * @exception Exception throws an exception if an error occurs
     */
    public Properties getCurrentProperties() 
        throws Exception
    {
        try {
            setUpSocket();
            writeCommandHeader(COMMAND_PROPERTIES);
            send();
            byte[] val = readBytesReply("DRDA_PropertyError.S");
            
            Properties p = new Properties();
            try {
                ByteArrayInputStream bs = new ByteArrayInputStream(val);
                p.load(bs);
            } catch (IOException io) {
                consolePropertyMessage("DRDA_IOException.S", io.getMessage());
            }
            return p;
        } finally {
            closeSocket();
        }
    }

    /**
     * Holds the sequence number to be assigned to the next thread that
     * calls {@link #getUniqueThreadName(String)}.
     */
    private static final AtomicInteger nextThreadNumber = new AtomicInteger(1);

    /**
     * Get a thread name that is both meaningful and unique (primarily for
     * debugging purposes).
     *
     * @param base the first part of the thread name (the meaningful part)
     * @return a unique thread name that starts with {@code base} and is
     * followed by an underscore and a unique sequence number
     */
    static String getUniqueThreadName(String base) {
        return base + "_" + nextThreadNumber.getAndIncrement();
    }

    /*******************************************************************************/
    /*        Protected methods                                                    */
    /*******************************************************************************/
    /**
     * Remove session from session table
     *
     * @param sessionid     id of session to be removed
     */
    protected void removeFromSessionTable(int sessionid)
    {
        sessionTable.remove(sessionid);
    }

    /**
     *  processCommands reads and processes NetworkServerControlImpl commands sent
     *  to the network server over the socket.  The protocol used is
     *      4 bytes     - String CMD:
     *      2 bytes     - Protocol version
     *      1 byte      - length of locale (0 for default)
     *          n bytes - locale
     *      1 byte      - length of codeset (0 for default)
     *          n bytes - codeset
     *      1 byte      - command
     *      n bytes     - parameters for the command
     *  The server returns
     *      4 bytes     - String RPY:
     *  for most commands
     *      1 byte      - command result, 0 - OK, 1 - warning, 2 - error
     *   if warning or error
     *      1 bytes     - length of message key
     *      n bytes     - message key
     *      1 byte      - number of parameters to message
     *      {2 bytes        - length of parameter
     *      n bytes     - parameter} for each parameter
     *  for sysinfo
     *      1 byte      - command result, 0 - OK, 1 - warning, 2 - error
     *   if OK 
     *      2 bytes     - length of sysinfo
     *      n bytes     - sysinfo
     *
     *
     *  Note, the 3rd byte of the command must not be 'D0' to distinquish it
     *  from DSS structures.
     *  The protocol for the parameters for each command follows:
     *
     *  Command: trace <connection id> {on | off}
     *  Protocol:
     *      4 bytes     - connection id - connection id of 0 means all sessions
     *      1 byte      - 0 off, 1 on
     * 
     *  Command: logConnections {on | off}
     *  Protocol:
     *      1 byte      - 0 off, 1 on
     * 
     *  Command: shutdown
     *      // DERBY-2109: transmit user credentials for System Privileges check
     *      2 bytes     - length of user name
     *      n bytes     - user name
     *      2 bytes     - length of password
     *      n bytes     - password
     * 
     *  Command: sysinfo
     *  No parameters
     * 
     *  Command: dbstart
     *  Protocol:
     *      2 bytes     - length of database name
     *      n bytes     - database name
     *      2 bytes     - length of boot password
     *      n bytes     - boot password
     *      2 bytes     - length of encryption algorithm
     *      n bytes     - encryption algorithm
     *      2 bytes     - length of encryption provider
     *      n bytes     - encryption provider
     *      2 bytes     - length of user name
     *      n bytes     - user name
     *      2 bytes     - length of password
     *      n bytes     - password
     *
     *  Command: dbshutdown
     *  Protocol:
     *      2 bytes     - length of database name
     *      n bytes     - database name
     *      2 bytes     - length of user name
     *      n bytes     - user name
     *      2 bytes     - length of password
     *      n bytes     - password
     *
     *  Command: connpool
     *  Protocol:
     *      2 bytes     - length of database name, if 0, default for all databases
     *                      is set
     *      n bytes     - database name
     *      2 bytes     - minimum number of connections, if 0, connection pool not used
     *                      if value is -1 use default
     *      2 bytes     - maximum number of connections, if 0, connections are created
     *                      as needed, if value is -1 use default
     *
     *  Command: maxthreads
     *  Protocol:
     *      2 bytes     - maximum number of threads
     *
     *  Command: timeslice
     *  Protocol:
     *      4 bytes     - timeslice value
     *
     *  Command: tracedirectory
     *  Protocol:
     *      2 bytes     - length of directory name
     *      n bytes     - directory name
     *
     *  Command: test connection
     *  Protocol:
     *      2 bytes     - length of database name if 0, just the connection
     *                      to the network server is tested and user name and
     *                      password aren't sent
     *      n bytes     - database name
     *      2 bytes     - length of user name (optional)
     *      n bytes     - user name
     *      2 bytes     - length of password  (optional)
     *      n bytes     - password
     *
     *  The calling routine is synchronized so that multiple threads don't clobber each
     *  other. This means that configuration commands will be serialized.
     *  This shouldn't be a problem since they should be fairly rare.
     *
     * @param reader    input reader for command
     * @param writer output writer for command
     * @param session   session information
     *
     * @exception Throwable throws an exception if an error occurs
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
            if (version <= 0 || version > MAX_ALLOWED_PROTOCOL_VERSION) {
                throw new Throwable(langUtil.getTextMessage(
                        "DRDA_UnknownProtocol.S", version));
            }
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
                    if (version == SHUTDOWN_WITH_CREDENTIAL_PROTOCOL_VERSION) {
                        //Protocol version of client is not at default protocol
                        //of 1 because this version of shutdown command has
                        //username and password supplied with it. When the
                        //protocol version of client is 
                        //SHUTDOWN_WITH_CREDENTIAL_PROTOCOL_VERSION, then we 
                        //know to expect username and password
                        // DERBY-2109: receive user credentials for shutdown
                        // System Privileges check
                        userArg = reader.readCmdString();
                        passwordArg = reader.readCmdString();
                    }
                    try {
                        checkShutdownPrivileges();
                        sendOK(writer);
                        directShutdownInternal();
                    } catch (SQLException sqle) {
                        sendSQLMessage(writer, sqle, SQLERROR);
                        // also print a message to the console
                        consolePropertyMessage("DRDA_ShutdownWarning.I",
                                               sqle.getMessage());
                    }
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
                        if (sessionArg != 0)
                            sendMessage(writer, ERROR,  
                                localizeMessage("DRDA_SessionNotFound.U", 
                                        (session.langUtil == null) ? langUtil : session.langUtil,
                                        new String [] {Integer.toString(sessionArg)}));
                        else
                            sendMessage(writer, ERROR,  
                                        localizeMessage("DRDA_ErrorStartingTracing.S",null));          
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
                    logConnectionsChange( log );
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
                        String.valueOf(newval));
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
                        String.valueOf(newval));
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

    /** Record a change to the connection logging mode */
    private void    logConnectionsChange( boolean on )
        throws Exception
    {
        String[]    args = null;
        String      messageID;

        if ( on ) { messageID = "DRDA_LogConnectionsChangeOn.I"; }
        else { messageID = "DRDA_LogConnectionsChangeOff.I"; }

        consolePropertyMessage( messageID, args );
    }
    
    /**
     * Get the next session for the thread to work on
     * Called from DRDAConnThread after session completes or timeslice
     * exceeded.   
     *
     * If there is a waiting session, pick it up and put currentSession 
     * at the back of the queue if there is one.
     * @param currentSession    session thread is currently working on
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
                retval = (Session) runQueue.get(0);
                runQueue.remove(0);
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
     * @param manager codepoint for manager
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
     * @param ccsid CCSID to check
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
     * @param msgProp       message property key
     * @param printTimeStamp whether to prepend a timestamp to the message
     *
     * @throws Exception if an error occurs
     */
    protected void consolePropertyMessage(String msgProp, boolean printTimeStamp)
        throws Exception
    {
        consolePropertyMessageWork(msgProp, null, printTimeStamp);
    }
    /**
     * Put property message on console
     *
     * @param msgProp       message property key
     * @param arg           argument for message
     *
     * @throws Exception if an error occurs
     */
    protected void consolePropertyMessage(String msgProp, String arg)
        throws Exception
    {
        consolePropertyMessageWork(msgProp, new String [] {arg}, true);
    }
    /**
     * Put property message on console
     *
     * @param msgProp       message property key
     * @param args          argument array for message
     *
     * @throws Exception if an error occurs
     */
    protected void consolePropertyMessage(String msgProp, String [] args)
        throws Exception
    {
        consolePropertyMessageWork(msgProp, args, true);
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
     * @param writer    writer to use 
     *
     * @throws Exception if a problem occurs sending OK
     */
    private void writeCommandReplyHeader(DDMWriter writer) throws Exception
    {
        writer.setCMDProtocol();
        writer.writeString(REPLY_HEADER);
    }
     
    /**
     * Send OK from server to client after processing a command
     *
     * @param writer    writer to use for sending OK
     *
     * @throws Exception if a problem occurs sending OK
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
     * @param val   int val to send
     * 
     * @throws Exception if a problem occurs
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
     * @param writer    writer to use for sending message
     * @param messageType   1 for Warning, 2 for Error 3 for SQLError
     * @param message   message
     *
     * @throws Exception if a problem occurs sending message
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
     * @param writer    writer to use for sending message
     * @param se        Derby exception
     * @param type      type of exception, SQLERROR or SQLWARNING
     *
     * @throws Exception if a problem occurs sending message
     */
    private void sendSQLMessage(DDMWriter writer, SQLException se, int type)
        throws Exception
    {
        StringBuffer locMsg = new StringBuffer();
        //localize message if necessary
        while (se != null)
        {
            if (currentSession != null && currentSession.langUtil != null &&
                se instanceof EmbedSQLException)
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
     * @param writer    writer to use for sending sysinfo
     *
     * @throws Exception if a problem occurs sending value
     */
    private void sendSysInfo(DDMWriter writer) throws Exception
    {
        StringBuffer sysinfo = new StringBuffer();
        sysinfo.append(getNetSysInfo());
        sysinfo.append(getCLSSysInfo());
        try {
            writeCommandReplyHeader(writer);
            writer.writeByte(0);    //O.K.
            writer.writeLDString(sysinfo.toString());
        } catch (DRDAProtocolException e) {
            consolePropertyMessage("DRDA_SysInfoWriteError.S", e.getMessage());
        }
        writer.flush();
    }
    
    /**
     * Send RuntimeInfo information from server to client
     *
     * @param writer    writer to use for sending sysinfo
     *
     * @throws Exception if a problem occurs sending value
     */
    private void sendRuntimeInfo(DDMWriter writer) throws Exception
    {
        try {
            writeCommandReplyHeader(writer);
            writer.writeByte(0);    //O.K.
            writer.writeLDString(getRuntimeInfo());
                } catch (DRDAProtocolException e) {
            consolePropertyMessage("DRDA_SysInfoWriteError.S", e.getMessage());
        }
        writer.flush();
    }

    

    /**
     * Send property information from server to client
     *
     * @param writer    writer to use for sending sysinfo
     *
     * @throws Exception if a problem occurs sending value
     */
    private void sendPropInfo(DDMWriter writer) throws Exception
    {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Properties p = getPropertyValues();
            p.store(out, "NetworkServerControl properties");
            try {
                writeCommandReplyHeader(writer);
                writer.writeByte(0);        //O.K.
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
     * Get Derby information
     *
     * @return system information for Derby
     *
     * @throws IOException if a problem occurs encoding string
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
     * Parse the command-line arguments. As a side-effect, fills in various instance
     * fields. This method was carved out of executeWork() so that
     * NetworkServerControl can figure out whether to install a security manager
     * before the server actually comes up. This is part of the work for DERBY-2196.
     *
     * @param args  array of arguments indicating command to be executed
     *
     * @return the command to be executed
     */
    public int parseArgs(String args[]) throws Exception
    {
        // For convenience just use NetworkServerControlImpls log writer for user messages
        logWriter = makePrintWriter(System.out);

        int command = findCommand(args);
        if (command == COMMAND_UNKNOWN)
        {
            consolePropertyMessage("DRDA_NoCommand.U", true);
        }

        return command;
    }

    /**
     * Execute the command given on the command line
     *
     * @param command   The command to execute. The command itself was determined by an earlier call to parseArgs().
     *
     * @exception Exception throws an exception if an error occurs
     * see class comments for more information
     */
    public void executeWork(int command) throws Exception
    {
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
                // the server was started from the command line, shutdown the
                // databases when the server is shutdown
                shutdownDatabasesOnShutdown = true;
                blockingStart(makePrintWriter(System.out));
                break;
            case COMMAND_SHUTDOWN:
                shutdown();
                consolePropertyMessage("DRDA_ShutdownSuccess.I", new String [] 
                                {att_srvclsnm, versionString});
                break;
            case COMMAND_TRACE:
                {
                    boolean on = isOn((String)commandArgs.get(0));
                    trace(sessionArg, on);
                    consoleTraceMessage(sessionArg, on);
                    break;
                }
            case COMMAND_TRACEDIRECTORY:
                String directory = (String) commandArgs.get(0);
                sendSetTraceDirectory(directory);
                consolePropertyMessage("DRDA_TraceDirectoryChange.I", directory);
                break;
            case COMMAND_TESTCONNECTION:
                ping();
                consolePropertyMessage("DRDA_ConnectionTested.I", new String [] 
                    {hostArg, Integer.toString(portNumber)});
                break;
            case COMMAND_LOGCONNECTIONS:
                {
                    boolean on = isOn((String)commandArgs.get(0));
                    logConnections(on);
                    logConnectionsChange( on );
                    break;
                }
            case COMMAND_SYSINFO:
                {
                    String info = sysinfo();
                    consoleMessage(info, false);
                    break;
                }
            case COMMAND_MAXTHREADS:
                max = 0;
                try{
                    max = Integer.parseInt((String)commandArgs.get(0));
                }catch(NumberFormatException e){
                    consolePropertyMessage("DRDA_InvalidValue.U", new String [] 
                        {(String)commandArgs.get(0), "maxthreads"});
                }
                if (max < MIN_MAXTHREADS)
                    consolePropertyMessage("DRDA_InvalidValue.U", new String [] 
                        {Integer.toString(max), "maxthreads"});
                netSetMaxThreads(max);

                break;
            case COMMAND_RUNTIME_INFO:
                String reply = runtimeInfo();
                consoleMessage(reply, false);
                break;
            case COMMAND_TIMESLICE:
                int timeslice = 0;
                String timeSliceArg = (String)commandArgs.get(0);
                try{
                    timeslice = Integer.parseInt(timeSliceArg);
                }catch(NumberFormatException e){
                    consolePropertyMessage("DRDA_InvalidValue.U", new String [] 
                        {(String)commandArgs.get(0), "timeslice"});
                }
                if (timeslice < MIN_TIMESLICE)
                    consolePropertyMessage("DRDA_InvalidValue.U", new String [] 
                        {Integer.toString(timeslice), "timeslice"});
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
     * @param clientSession session needing work
     */
    private void runQueueAdd(Session clientSession)
    {
        synchronized(runQueue)
        {
            runQueue.add(clientSession);
            runQueue.notify();
        }
    }
    /**
     * Go through the arguments and find the command and save the dash arguments
     *  and arguments to the command.  Only one command is allowed in the argument
     *  list.
     *
     * @param args  arguments to search
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
                        commandArgs.add(args[i++]);
                    else
                        i = newpos;
                }
                else
                    commandArgs.add(args[i++]);
            }
                    
            // look up command
            if (commandArgs.size() > 0)
            {
                for (i = 0; i < COMMANDS.length; i++)
                {
                    if (StringUtil.SQLEqualsIgnoreCase(COMMANDS[i], 
                                                       (String)commandArgs.firstElement()))
                    {
                        commandArgs.remove(0);
                        return i;
                    }
                }

                // didn't find command
                consolePropertyMessage("DRDA_UnknownCommand.U", 
                    (String) commandArgs.firstElement());
            }
        } catch (Exception e) {
            if (e.getMessage().equals(NetworkServerControlImpl.UNEXPECTED_ERR))
                throw e;
            //Ignore expected exceptions, they will have been
                                    //handled by the consolePropertyMessage routine
        }
        return COMMAND_UNKNOWN;
    }
    /**
     * Get the dash argument. Optional arguments are formated as -x value.
     *
     * @param pos   starting point
     * @param args  arguments to search
     *
     * @return  command
     *
     * @exception Exception thrown if an error occurs
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
                if ( dashArg != DASHARG_UNSECURE ) { pos++ ; }
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
                    userArg = args[pos];
                else
                    consolePropertyMessage("DRDA_MissingValue.U", "DRDA_User.I");
                break;
            case DASHARG_PASSWORD:
                if (pos < args.length)
                    passwordArg = args[pos];
                else
                    consolePropertyMessage("DRDA_MissingValue.U", "DRDA_Password.I");
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
            case DASHARG_UNSECURE:
                unsecureArg = true;
                break;

            case DASHARG_SSL:
                if (pos < args.length) {
                    setSSLMode(getSSLModeValue(args[pos]));
                } else {
                    consolePropertyMessage("DRDA_MissingValue.U", "DRDA_SslMode.I");
                }
                break;

            default:
                //shouldn't get here
        }
        return pos+1;
    }

    /**
     * Is string "on" or "off"
     *
     * @param arg   string to check
     *
     * @return  true if string is "on", false if string is "off"
     *
     * @exception Exception thrown if string is not one of "on" or "off"
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
     * Close the resources associated with the opened socket.
     * @throws IOException
     */
    private void closeSocket() throws IOException
    {
        try {
            if (clientIs != null)
                clientIs.close();
            if (clientOs != null)
                clientOs.close();
            if (clientSocket != null)
                clientSocket.close();
        } finally {
            clientIs = null;
            clientOs = null;
            clientSocket = null;
        }
    }

    /**
     * Set up client socket to send a command to the network server
     *
     * @exception Exception thrown if exception encountered
     */
    private void setUpSocket() throws Exception
    {
        
        try {
            clientSocket = AccessController.doPrivileged(
                                new PrivilegedExceptionAction<Socket>() {
                                        
                                    public Socket run()
                                        throws UnknownHostException,
                                               IOException, 
                                               java.security.NoSuchAlgorithmException,
                                               java.security.KeyManagementException,
                                               java.security.NoSuchProviderException,
                                               java.security.KeyStoreException,
                                               java.security.UnrecoverableKeyException,
                                               java.security.cert.CertificateException
                                    {
                                        if (hostAddress == null)
                                            hostAddress = InetAddress.getByName(hostArg);
                                        
                                        switch(getSSLMode()) {
                                        case SSL_BASIC:
                                            SSLSocket s1 = (SSLSocket)NaiveTrustManager.getSocketFactory().
                                                createSocket(hostAddress, portNumber);
                                            // Need to handshake now to get proper error reporting.
                                            s1.startHandshake();
                                            return s1;

                                        case SSL_PEER_AUTHENTICATION:
                                            SSLSocket s2 = (SSLSocket)SSLSocketFactory.getDefault().
                                                createSocket(hostAddress, portNumber);
                                            // Need to handshake now to get proper error reporting.
                                            s2.startHandshake();
                                            return s2;

                                        case SSL_OFF:
                                        default:
                                            return SocketFactory.getDefault().
                                                createSocket(hostAddress, portNumber);
                                        }
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
                        new String [] {
                            hostArg,
                            Integer.toString(portNumber),
                            e1.getMessage()
                        });
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
            consolePropertyMessage("DRDA_NoInputStream.I", true);
            throw e;
        }
    }

    
    private void checkAddressIsLocal(InetAddress inetAddr) throws UnknownHostException,Exception
    {
        if (localAddresses.contains(inetAddr)) {
            return;
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
        localAddresses = new ArrayList<InetAddress>(3);
        localAddresses.add(bindAddr);
        
        try { localAddresses.add(InetAddress.getLocalHost()); }
        catch(UnknownHostException uhe) { unknownHostException( uhe ); }
        
        try { localAddresses.add(InetAddress.getByName("localhost")); }
        catch(UnknownHostException uhe) { unknownHostException( uhe ); }
    }
    private void unknownHostException( Throwable t )
    {
        try {
            consolePropertyMessage( "DRDA_UnknownHostWarning.I", t.getMessage() );
        } catch (Exception e)
        { 
            // just a warning shouldn't actually throw an exception
        }
    }
    
    /**
     * Routines for writing commands for NetworkServerControlImpl being used as a client
     * to a server
     */

    /**
     * Write command header consisting of command header string and default
     * protocol version and command. At this point, all the commands except
     * shutdown with username/passwrod use default protocol version.
     *
     * @param command   command to be written
     *
     * @exception Exception throws an exception if an error occurs
     */
    private void writeCommandHeader(int command) throws Exception
    {
        writeCommandHeader(command, DEFAULT_PROTOCOL_VERSION);
    }
    
    /**
     * Write command header consisting of command header string and passed
     * protocol version and command. At this point, all the commands except
     * shutdown with username/passwrod use default protocol version.
     *
     * @param command   command to be written
     * @param protocol_version_for_command protocol version to be used
     *   for the given command
     *
     * @exception Exception throws an exception if an error occurs
     */
    private void writeCommandHeader(int command, int protocol_version_for_command) throws Exception
    {
        try {
            writeString(COMMAND_HEADER);
            commandOs.writeByte((byte)((protocol_version_for_command & 0xf0) >> 8 ));
            commandOs.writeByte((byte)(protocol_version_for_command & 0x0f));

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
     * @param msg   string to be written
     *
     * @exception Exception throws an exception if an error occurs
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
     * @param value value to be written
     *
     * @exception Exception throws an exception if an error occurs
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
     * @param value value to be written
     *
     * @exception Exception throws an exception if an error occurs
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
     * @exception Exception throws an exception if an error occurs
     */
    private void send() throws Exception
    {
        try {
            byteArrayOs.writeTo(clientOs);
            commandOs.flush();
            byteArrayOs.reset();    //discard anything currently in the byte array
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
     * @exception Exception throws an exception if an error occurs
     */
    private void readResult() throws Exception
    {
        fillReplyBuffer();
        readCommandReplyHeader();
        if (replyBufferPos >= replyBufferCount)
            consolePropertyMessage("DRDA_InvalidReplyTooShort.S", true);
        int messageType = replyBuffer[replyBufferPos++] & 0xFF;
        if (messageType == OK)      // O.K.
            return;
        // get error and display and throw exception
        String message =  readLDString();
        if (messageType == SQLERROR)
            wrapSQLError(message);
        else if (messageType == SQLWARNING)
            wrapSQLWarning(message);
        else
            consolePropertyMessage(message, true);
    }

    

    /**
     * Ensure the reply buffer is at large enought to hold all the data;
     * don't just rely on OS level defaults
     *
     *
     * @param minimumBytesNeeded    size of buffer required
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
            consolePropertyMessage("DRDA_InvalidReplyTooShort.S", true);
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
     * @exception Exception throws an exception if an error occurs
     */
    private int readShort() throws Exception
    {
        ensureDataInBuffer(2);
        if (replyBufferPos + 2 > replyBufferCount)
            consolePropertyMessage("DRDA_InvalidReplyTooShort.S", true);
        return ((replyBuffer[replyBufferPos++] & 0xff) << 8) +
                (replyBuffer[replyBufferPos++] & 0xff);
    }
    /**
     * Read int from buffer
     * @exception Exception throws an exception if an error occurs
     */
    private int readInt() throws Exception
    {
        ensureDataInBuffer(4);
        if (replyBufferPos + 4 > replyBufferCount)
            consolePropertyMessage("DRDA_InvalidReplyTooShort.S", true);
        return ((replyBuffer[replyBufferPos++] & 0xff) << 24) +
            ((replyBuffer[replyBufferPos++] & 0xff) << 16) +
            ((replyBuffer[replyBufferPos++] & 0xff) << 8) +
                (replyBuffer[replyBufferPos++] & 0xff);
    }
    /**
     * Read String reply
     *
     * @param msgKey    error message key
     * @return string value or null 
     * @exception Exception throws an error if problems reading reply
     */
    private String readStringReply(String msgKey) throws Exception
    {
        fillReplyBuffer();
        readCommandReplyHeader();
        if (replyBuffer[replyBufferPos++] == 0)     // O.K.
            return readLDString();
        else
            consolePropertyMessage(msgKey, true);
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
            consolePropertyMessage("DRDA_InvalidReplyTooShort.S", true);
        String retval= new String(replyBuffer, replyBufferPos, strlen, DEFAULT_ENCODING);
        replyBufferPos += strlen;
        return retval;
    }
    /**
     * Read Bytes reply
     *
     * @param msgKey    error message key
     * @return string value or null 
     * @exception Exception throws an error if problems reading reply
     */
    private byte [] readBytesReply(String msgKey) throws Exception
    {
        fillReplyBuffer();
        readCommandReplyHeader();
        if (replyBuffer[replyBufferPos++] == 0)     // O.K.
            return readLDBytes();
        else
            consolePropertyMessage(msgKey, true);
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
            consolePropertyMessage("DRDA_InvalidReplyTooShort.S", true);
        byte [] retval =  new byte[len];
        for (int i = 0; i < len; i++)
            retval[i] = replyBuffer[replyBufferPos++];
        return retval;
    }

    /**
     * Initialize fields from system properties
     *
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

        //If the derby.system.home property has been set, it is the default. 
        //Otherwise, the default is the current directory. 
        //If derby.system.home is not set, directory will be null and trace files will get
        //created in current directory.
        propval = PropertyUtil.getSystemProperty(Property.DRDA_PROP_TRACEDIRECTORY,directory);
        if(propval != null){
            if(propval.equals(""))
                propval = directory;
            setTraceDirectory(propval);
        }

        //DERBY-375 If a system property is specified without any value, getProperty returns 
        //an empty string. Use default values in such cases.
        propval = PropertyUtil.getSystemProperty( 
            Property.DRDA_PROP_MINTHREADS);
        if (propval != null){
            if(propval.equals(""))
                propval = "0";
            setMinThreads(getIntPropVal(Property.DRDA_PROP_MINTHREADS, propval));
        }

        propval = PropertyUtil.getSystemProperty( 
            Property.DRDA_PROP_MAXTHREADS);
        if (propval != null){
            if(propval.equals(""))
                propval = "0";
            setMaxThreads(getIntPropVal(Property.DRDA_PROP_MAXTHREADS, propval));
        }


        propval = PropertyUtil.getSystemProperty( 
            Property.DRDA_PROP_TIMESLICE);
        if (propval != null){
            if(propval.equals(""))
                propval = "0";
            setTimeSlice(getIntPropVal(Property.DRDA_PROP_TIMESLICE, propval));
        }

        propval = PropertyUtil.getSystemProperty( 
            Property.DRDA_PROP_PORTNUMBER);
        if (propval != null){
            if(propval.equals(""))
                propval = String.valueOf(NetworkServerControl.DEFAULT_PORTNUMBER);
            portNumber = getIntPropVal(Property.DRDA_PROP_PORTNUMBER, propval);
        }

        propval = PropertyUtil.getSystemProperty(
            Property.DRDA_PROP_SSL_MODE);
        setSSLMode(getSSLModeValue(propval));
                                                 
        propval = PropertyUtil.getSystemProperty( 
            Property.DRDA_PROP_KEEPALIVE);
        if (propval != null && 
            StringUtil.SQLEqualsIgnoreCase(propval,"false"))
            keepAlive = false;
        
        propval = PropertyUtil.getSystemProperty( 
            Property.DRDA_PROP_HOSTNAME);
        if (propval != null){
            if(propval.equals(""))
                hostArg = DEFAULT_HOST; 
            else
                hostArg = propval;
        }
        propval = PropertyUtil.getSystemProperty(
                         NetworkServerControlImpl.DRDA_PROP_DEBUG);
        if (propval != null  && StringUtil.SQLEqualsIgnoreCase(propval, "true"))
            debugOutput = true;

        propval = PropertyUtil.getSystemProperty( 
                Property.DRDA_PROP_SECURITYMECHANISM);
        if (propval != null){
            setSecurityMechanism(propval);
        }

    }

    /**
     * Retrieve the SECMEC integer value from the
     * user friendly security mechanism name
     * @param s  security mechanism name
     * @return integer value , return the SECMEC value for 
     * the security mechanism as defined by DRDA spec
     * or INVALID_OR_NOTSET_SECURITYMECHANISM if 's'
     * passed is invalid  or not supported security 
     * mechanism
     */
    private int getSecMecValue(String s)
    {
        int secmec = INVALID_OR_NOTSET_SECURITYMECHANISM;

        if( StringUtil.SQLEqualsIgnoreCase(s,"USER_ONLY_SECURITY"))
                secmec = CodePoint.SECMEC_USRIDONL;
        else if( StringUtil.SQLEqualsIgnoreCase(s,"CLEAR_TEXT_PASSWORD_SECURITY"))
                secmec = CodePoint.SECMEC_USRIDPWD;
        else if( StringUtil.SQLEqualsIgnoreCase(s,"ENCRYPTED_USER_AND_PASSWORD_SECURITY"))
                secmec = CodePoint.SECMEC_EUSRIDPWD;
        else if( StringUtil.SQLEqualsIgnoreCase(s,"STRONG_PASSWORD_SUBSTITUTE_SECURITY"))
                secmec = CodePoint.SECMEC_USRSSBPWD;
        
        return secmec;
    }

    /**
     * Retrieve the string name for the integer
     * secmec value
     * @param secmecVal   secmec value
     * @return String - return the string name corresponding 
     * to the secmec value if recognized else returns null
     */
    private String getStringValueForSecMec(int secmecVal)
    {
        switch(secmecVal)
        {
            case CodePoint.SECMEC_USRIDONL:
                return "USER_ONLY_SECURITY";
            
            case CodePoint.SECMEC_USRIDPWD:
                return "CLEAR_TEXT_PASSWORD_SECURITY";
            
            case CodePoint.SECMEC_EUSRIDPWD:
                return "ENCRYPTED_USER_AND_PASSWORD_SECURITY";
            
            case CodePoint.SECMEC_USRSSBPWD:
                return "STRONG_PASSWORD_SUBSTITUTE_SECURITY";
        }
        return null;
    }
   
    /**
     * This method returns whether EUSRIDPWD security mechanism
     * is supported or not. See class static block for more
     * info.
     * @return true if EUSRIDPWD is supported, false otherwise
     */ 
    boolean supportsEUSRIDPWD()
    {
        return SUPPORTS_EUSRIDPWD;
    }
    
    /**
     * Get the SSL-mode from a string.
     * @param s the SSL-mode string ("off"/"false", "on"/"true" or
     * "authenticate"/"auth"
     * @return SSL_OFF, SSL_BASIC or SSL_PEER_AUTHENTICATION. Will default to
     * SSL_OFF if the input does not match one of the four listed
     * above.
     **/

    private int getSSLModeValue(String s)
        throws Exception
    {
        if (s != null){
            if (StringUtil.SQLEqualsIgnoreCase(s,"off")) {
                return SSL_OFF;
            } else if (StringUtil.SQLEqualsIgnoreCase(s,"basic")) {
                return SSL_BASIC;
            } else if (StringUtil.SQLEqualsIgnoreCase(s,"peerAuthentication")) {
                return SSL_PEER_AUTHENTICATION;
            } else {
                // Unknown value
                consolePropertyMessage("DRDA_InvalidValue.U", 
                                       new String [] {s, Property.DRDA_PROP_SSL_MODE});
                
                return SSL_OFF;
            }
        } else {
            // Default
            return SSL_OFF;
        }
    }

    /**
     * Get the string value of the SSL-mode. This is the inverse of
     * getSSLModeValue.
     * @param i The SSL-mode value (SSL_OFF, SSL_BASIC or
     * SSL_PEER_AUTHENTICATION)
     * @return The string representation ("off","on" or
     * "autneticate"). Will default to SSL_OFF for other values than
     * those listed above.
     */
    
    private String getSSLModeString(int i)
    {
        switch(i) {
        case SSL_OFF:
            return "off";
        case SSL_BASIC:
            return "basic";
        case SSL_PEER_AUTHENTICATION:
            return "peerAuthentication";
        default: 
            // Assumes no SSL encryption for faulty values Anyway,
            // this should not happen thince the input values are
            // strings...
            return "off";
        }
    }

    
    /**
     * Get integer property values
     *
     * @param propName  property name
     * @param propVal   string property value
     * @return integer value
     *
     * @exception Exception if not a valid integer
     */
    private int getIntPropVal(String propName, String propVal)
        throws Exception
    {
        int val = 0;
        try {
            val = Integer.parseInt(propVal);
        } catch (Exception e)
        {
            consolePropertyMessage("DRDA_InvalidPropVal.S", new String [] 
                {propName, propVal});
        }
        return val;
    }

    /**
     * Handle console error message
     *  - display on console and if it is a user error, display usage
     *  - if user error or severe error, throw exception with message key and message
     *
     * @param messageKey    message key
     * @param args          arguments to message
     * @param printTimeStamp whether to prepend a timestamp to the message
     *
     * @throws Exception if an error occurs
     */
    private void consolePropertyMessageWork(String messageKey, String [] args, boolean printTimeStamp)
        throws Exception
    {
        String locMsg = null;

        int type = getMessageType(messageKey);

        if (type == ERRTYPE_UNKNOWN)
            locMsg = messageKey;
        else
            locMsg = localizeMessage(messageKey, langUtil, args);

        //display on the console
        consoleMessage(locMsg, printTimeStamp);

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
     * @param msg       msg containing SQL Exception
     *
     * @throws SQLException
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
     * @param msg       msg containing SQL Warning
     *
     * @throws SQLWarning
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
     * @throws Exception with message UNEXPECTED_ERR.
     */
    private void throwUnexpectedException(Exception e)
     throws Exception {

        consoleExceptionPrintTrace(e);
        throw new Exception(UNEXPECTED_ERR);

    }

    /**
     * Convenience routine so that NetworkServerControl can localize messages.
     *
     * @param msgProp   message key
     * @param args      arguments to message
     *
     */
    public String localizeMessage( String msgProp, String[] args )
    {
        return localizeMessage( msgProp, langUtil, args );
    }

    /**
     * Localize a message given a particular AppUI 
     *
     * @param msgProp   message key
     * @param localLangUtil LocalizedResource to use to localize message
     * @param args      arguments to message
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
     * @param msg       message
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
     *  property keys start with DRDA_MSG_PREFIX
     *
     * @param msg       message
     *
     * @return true if it is a property key; false otherwise
     */
    private boolean isMsgProperty(String msg)
    {
        if (msg != null && msg.startsWith(DRDA_MSG_PREFIX))
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
     * @param value true to turn logging connections on; false to turn it off
     */
    private void setLogConnections(boolean value)
    {
        synchronized(logConnectionsSync) {
            logConnections = value;
        }
        // update the value in all the threads
        synchronized(threadList) {
            for (DRDAConnThread thread : threadList)
            {
                thread.setLogConnections(value);
            }
        }
    }

    /**
     * Set the security mechanism for derby.drda.securityMechanism
     * If this property is set, server will only allow connections
     * from client with this security mechanism.
     * This method will map the user friendly string representing 
     * the security mechanism to the corresponding drda secmec value
     * @param s security mechanism string value
     * @throws Exception if  value to set is invalid
     * @see Property#DRDA_PROP_SECURITYMECHANISM 
     */
    private void setSecurityMechanism(String s)
        throws Exception
    {
       allowOnlySecurityMechanism = getSecMecValue(s);
       
       // if server vm cannot support EUSRIDPWD, then do not allow 
       // derby.drda.securityMechanism to be set to EUSRIDPWD security
       // mechanism
       if ((allowOnlySecurityMechanism == INVALID_OR_NOTSET_SECURITYMECHANISM) ||
              (allowOnlySecurityMechanism == CodePoint.SECMEC_EUSRIDPWD &&
              !SUPPORTS_EUSRIDPWD))
           consolePropertyMessage("DRDA_InvalidValue.U", new String [] 
                       {s, Property.DRDA_PROP_SECURITYMECHANISM});
    }
    
    /**
     * get the security mechanism (secmec value) that the server
     * will accept connections from.
     * @return the securitymechanism value. It is value that 
     * the derby.drda.securityMechanism was set to, if it is not set, then
     * it is equal to INVALID_OR_NOTSET_SECURITYMECHANISM
     * @see Property#DRDA_PROP_SECURITYMECHANISM 
     */
    protected int getSecurityMechanism()
    {
        return allowOnlySecurityMechanism;
    }
    /**
     * Set the trace on/off for all sessions, or one session, depending on
     * whether we got -s argument.
     *
     * @param on    true to turn trace on; false to turn it off
     * @return true if set false if an error occurred
     */
    private boolean setTrace(boolean on)
    {
        boolean setTraceSuccessful = true;
        if (sessionArg == 0)
        {
            synchronized(sessionTable) {
                for (Session session : sessionTable.values())
                {
                    if (on)
                        try {
                            session.setTraceOn(traceDirectory,true);
                        } catch (Exception te ) {
                            consoleExceptionPrintTrace(te);
                            setTraceSuccessful = false;
                            session.setTraceOff();
                        }
                    else
                        session.setTraceOff();
                }
                if (setTraceSuccessful)
                    setTraceAll(on);
            }
        }
        else
        {
            Session session = sessionTable.get(sessionArg);
            if (session != null)
            {
                if (on)
                    try {                         
                        session.setTraceOn(traceDirectory,true);
                    }catch (Exception te) {
                        consoleExceptionPrintTrace(te);
                        setTraceSuccessful = false;
                        session.setTraceOff();
                    }
                else
                    session.setTraceOff();
            }
            else
                return false;
        }
        return setTraceSuccessful;
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
                {Integer.toString(value), "timeslice"});
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
     * @param value  value of minimum number of threads
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
    private int getMaxThreads()
    {
        synchronized(threadsSync) {
            return maxThreads;
        }
    }
    /**
     * Set the current value of maximum number of threads to create 
     *
     * @param value value of maximum number of threads
     * @exception Exception if value is less than 0
     */
    private void setMaxThreads(int value) throws Exception
    {
        if (value < MIN_MAXTHREADS)
            consolePropertyMessage("DRDA_InvalidValue.U", new String [] 
                {Integer.toString(value), "maxthreads"});
        if (value == USE_DEFAULT)
            value = DEFAULT_MAXTHREADS;
        synchronized(threadsSync) {
            maxThreads = value;
        }
    }

    protected void setSSLMode(int mode)
    {
        sslMode = mode;
    }

    protected int getSSLMode() 
    {
        return sslMode;
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
     * @param value true if tracing is on for all sessions; false otherwise
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
     * @param value trace directory
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
     * @param writer    connection to send message to
     * @param database  database directory to connect to
     * @param user      user to use
     * @param password  password to use
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
            Connection conn = getDriver().connect(Attribute.PROTOCOL+database, p);
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
     * Wrap SQL Error - display to console and raise exception
     *
     * @param messageKey    Derby SQL Exception message id
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
     * @param messageKey    Derby SQL Exception message id
     *
     * @exception Exception raises exception for message
     */
    private void wrapSQLWarning(String messageKey)
        throws Exception
    {
        consolePropertyMessage("DRDA_SQLWarning.I", messageKey);
    }
    
    /**
     * <p>
     * Constructs an object containing network server related properties
     * and their values. Some properties are only included if set. Some 
     * other properties are included with a default value if not set.</p>
     * <p>
     * This method is accessing the local JVM in which the network server
     * instance is actually running (i.e. no networking).</p>
     * <p>
     * This method is package private to allow access from relevant MBean 
     * implementations in the same package.</p>
     * 
     * @return a collection of network server properties and their current 
     *         values
     */
    Properties getPropertyValues()
    {
        Properties retval = new Properties();
        retval.put(Property.DRDA_PROP_PORTNUMBER, Integer.toString(portNumber));
        retval.put(Property.DRDA_PROP_HOSTNAME, hostArg);
        retval.put(Property.DRDA_PROP_KEEPALIVE, Boolean.toString(keepAlive));

        String tracedir = getTraceDirectory();
        if (tracedir != null)
            retval.put(Property.DRDA_PROP_TRACEDIRECTORY, tracedir);
        retval.put(Property.DRDA_PROP_TRACEALL, Boolean.toString(getTraceAll()));
        retval.put(Property.DRDA_PROP_MINTHREADS, Integer.toString(getMinThreads()));
        retval.put(Property.DRDA_PROP_MAXTHREADS, Integer.toString(getMaxThreads()));
        retval.put(Property.DRDA_PROP_TIMESLICE, Integer.toString(getTimeSlice()));

        retval.put(Property.DRDA_PROP_TIMESLICE, Integer.toString(getTimeSlice()));
        retval.put(Property.DRDA_PROP_LOGCONNECTIONS, Boolean.toString(getLogConnections()));
        String startDRDA = PropertyUtil.getSystemProperty(Property.START_DRDA);
        //DERBY-375 If a system property is specified without any value, getProperty returns 
        //an empty string. Use default values in such cases.
        if(startDRDA!=null && startDRDA.equals(""))
            startDRDA = "false";

        retval.put(Property.START_DRDA, (startDRDA == null)? "false" : startDRDA);

        // DERBY-2108 SSL
        retval.put(Property.DRDA_PROP_SSL_MODE, getSSLModeString(getSSLMode()));
        
        // if Property.DRDA_PROP_SECURITYMECHANISM has been set on server
        // then put it in retval else the default behavior is as though 
        // it is not set
        if ( getSecurityMechanism() != INVALID_OR_NOTSET_SECURITYMECHANISM )
            retval.put( Property.DRDA_PROP_SECURITYMECHANISM, getStringValueForSecMec(getSecurityMechanism()));
        
        //get the trace value for each session if tracing for all is not set
        if (!getTraceAll())
        {
            synchronized(sessionTable) {
                for (Session session : sessionTable.values())
                {
                    if (session.isTraceOn())
                        retval.put(Property.DRDA_PROP_TRACE+"."+session.getConnNum(), "true");
                }
            }
        }
        return retval;
    }


    /**
     * Add a session - for use by <code>ClientThread</code>. Put the session
     * into the session table and the run queue. Start a new
     * <code>DRDAConnThread</code> if there are more sessions waiting than
     * there are free threads, and the maximum number of threads is not
     * exceeded.
     *
     * <p><code>addSession()</code> should only be called from one thread at a
     * time.
     *
     * @param clientSocket the socket to read from and write to
     */
    void addSession(Socket clientSocket) throws Exception {

        int connectionNumber = ++connNum;

        if (getLogConnections()) {
            consolePropertyMessage("DRDA_ConnNumber.I",
                                   Integer.toString(connectionNumber));
        }

        // Note that we always re-fetch the tracing configuration because it
        // may have changed (there are administrative commands which allow
        // dynamic tracing reconfiguration).
        Session session = new Session(this,connectionNumber, clientSocket,
                                      getTraceDirectory(), getTraceAll());

        sessionTable.put(connectionNumber, session);

        // Check whether there are enough free threads to service all the
        // threads in the run queue in addition to the newly added session.
        boolean enoughThreads;
        synchronized (runQueue) {
            enoughThreads = (runQueue.size() < freeThreads);
        }
        // No need to hold the synchronization on runQueue any longer than
        // this. Since no other threads can make runQueue grow, and no other
        // threads will reduce the number of free threads without removing
        // sessions from runQueue, (runQueue.size() < freeThreads) cannot go
        // from true to false until addSession() returns.

        DRDAConnThread thread = null;

        // try to start a new thread if we don't have enough free threads
        if (!enoughThreads) {
            // Synchronize on threadsSync to ensure that the value of
            // maxThreads doesn't change until the new thread is added to
            // threadList.
            synchronized (threadsSync) {
                // only start a new thread if we have no maximum number of
                // threads or the maximum number of threads is not exceeded
                if ((maxThreads == 0) || (threadList.size() < maxThreads)) {
                    thread = new DRDAConnThread(session, this, getTimeSlice(),
                                                getLogConnections());
                    threadList.add(thread);
                    thread.start();
                }
            }
        }

        // add the session to the run queue if we didn't start a new thread
        if (thread == null) {
            runQueueAdd(session);
        }
    }

    /**
     * Remove a thread from the thread list. Should be called when a
     * <code>DRDAConnThread</code> has been closed.
     *
     * @param thread the closed thread
     */
    void removeThread(DRDAConnThread thread) {
        threadList.remove(thread);
    }
    
    protected Object getShutdownSync() { return shutdownSync; } 
    protected boolean getShutdown() { return shutdown; } 


    public String buildRuntimeInfo(LocalizedResource locallangUtil)
    {
        
        String s = locallangUtil.getTextMessage("DRDA_RuntimeInfoBanner.I")+ "\n";
        int sessionCount = 0;
        s += locallangUtil.getTextMessage("DRDA_RuntimeInfoSessionBanner.I") + "\n";
        for (DRDAConnThread thread : threadList)
        {
            String sessionInfo = thread.buildRuntimeInfo("", locallangUtil);
            if (!sessionInfo.equals(""))
            {
                sessionCount ++;
                s += sessionInfo + "\n";
            }
        }
        int waitingSessions = 0;
        for (Session session : runQueue)
        {
                s += session.buildRuntimeInfo("", locallangUtil);
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
    
    long getBytesRead() {
        long count=0;
        for (DRDAConnThread thread : threadList) {
            count += thread.getBytesRead();
        }
        return count;
    }
    
     long getBytesWritten() {
        long count=0;
        for (DRDAConnThread thread : threadList) {
            count += thread.getBytesWritten();
        }
        return count;
    }
     
    int getActiveSessions() {
        int count=0;
        for (DRDAConnThread thread : threadList) {
           if (thread.hasSession()) {
               count++;
           }
        }
        return count;
    }

    int getRunQueueSize() {
        return runQueue.size();
    }
    
    int getThreadListSize() {
        return threadList.size();
    }
    
    int getConnectionNumber() {
        return connNum;
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
            myPVH = AccessController.doPrivileged(
                new PrivilegedExceptionAction<ProductVersionHolder>() {
                    public ProductVersionHolder run()
                            throws UnknownHostException, IOException {
                        InputStream versionStream =
                            getClass().getResourceAsStream(
                                ProductGenusNames.NET_INFO);
                        return ProductVersionHolder.
                                getProductVersionHolderFromMyEnv(versionStream);
                    }
                });
        } catch (PrivilegedActionException e) {
            Exception e1 = e.getException();
            consolePropertyMessage("DRDA_ProductVersionReadError.S", e1.getMessage());
        }
        return myPVH;
    }
}
