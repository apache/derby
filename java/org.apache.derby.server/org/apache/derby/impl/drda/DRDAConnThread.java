/*
    Derby - Class org.apache.derby.impl.drda.DRDAConnThread

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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DataTruncation;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import org.apache.derby.catalog.SystemProcedures;
import org.apache.derby.shared.common.error.ExceptionSeverity;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.jdbc.AuthenticationService;
import org.apache.derby.iapi.jdbc.EngineLOB;
import org.apache.derby.iapi.jdbc.EngineStatement;
import org.apache.derby.iapi.jdbc.EnginePreparedStatement;
import org.apache.derby.shared.common.reference.Attribute;
import org.apache.derby.shared.common.reference.DRDAConstants;
import org.apache.derby.shared.common.reference.Property;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.info.JVMInfo;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.shared.common.error.MessageUtils;
import org.apache.derby.shared.common.stream.HeaderPrintWriter;
import org.apache.derby.iapi.tools.i18n.LocalizedResource;
import org.apache.derby.iapi.util.StringUtil;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.iapi.jdbc.InternalDriver;

/**
 * This class translates DRDA protocol from an application requester to JDBC
 * for Derby and then translates the results from Derby to DRDA
 * for return to the application requester.
 */
class DRDAConnThread extends Thread {

    private static final String leftBrace = "{";
    private static final String rightBrace = "}";
    private static final byte NULL_VALUE = (byte)0xff;
    private static final String SYNTAX_ERR = "42X01";

    // Manager Level 3 constant.
    private static final int MGRLVL_3 = 0x03;

    // Manager Level 4 constant.
    private static final int MGRLVL_4 = 0x04;

    // Manager Level 5 constant.
    private static final int MGRLVL_5 = 0x05;

    // Manager level 6 constant.
    private static final int MGRLVL_6 = 0x06;

    // Manager Level 7 constant.
    private static final int MGRLVL_7 = 0x07;


    // Commit or rollback UOWDSP values
    private static final int COMMIT = 1;
    private static final int ROLLBACK = 2;


    private int correlationID;
    private InputStream sockis;
    private OutputStream sockos;
    private DDMReader reader;
    private DDMWriter writer;
    private DRDAXAProtocol xaProto;

    private static int [] ACCRDB_REQUIRED = {CodePoint.RDBACCCL, 
                                             CodePoint.CRRTKN,
                                             CodePoint.PRDID,
                                             CodePoint.TYPDEFNAM,
                                             CodePoint.TYPDEFOVR};

    private static int MAX_REQUIRED_LEN = 5;

    private int currentRequiredLength = 0;
    private int [] required = new int[MAX_REQUIRED_LEN];


    private NetworkServerControlImpl server;            // server who created me
    private Session session;    // information about the session
    /** Time slice for this thread. */
    private volatile long timeSlice;
    /** Whether or not to log connections. */
    private volatile boolean logConnections;

    private boolean sendWarningsOnCNTQRY = false;   // Send Warnings for SELECT if true
    /** End this thread. */
    private volatile boolean close;
    private static HeaderPrintWriter logStream;
    private AppRequester appRequester;  // pointer to the application requester
                                        // for the session being serviced
    private Database database;  // pointer to the current database
    private int sqlamLevel;     // SQLAM Level - determines protocol

    // DRDA diagnostic level, DIAGLVL0 by default 
    private byte diagnosticLevel = (byte)0xF0; 

    // manager processing
    private List<Integer> unknownManagers;
    private List<Integer> knownManagers;

    // database accessed failed
    private SQLException databaseAccessException;

    // these fields are needed to feed back to jcc about a statement/procedure's PKGNAMCSN
    /** The value returned by the previous call to
     * <code>parsePKGNAMCSN()</code>. */
    private Pkgnamcsn prevPkgnamcsn = null;
    /** Current RDB Package Name. */
    private DRDAString rdbnam = null;
    /** Current RDB Collection Identifier. */
    private DRDAString rdbcolid = null;
    /** Current RDB Package Identifier. */
    private DRDAString pkgid = null;
    /** Current RDB Package Consistency Token. */
    private DRDAString pkgcnstkn = null;
    /** Current RDB Package Section Number. */
    private int pkgsn;

    private final static String TIMEOUT_STATEMENT = "SET STATEMENT_TIMEOUT ";

    private int pendingStatementTimeout; // < 0 means no pending timeout to set

    // this flag is for an execute statement/procedure which actually returns a result set;
    // do not commit the statement, otherwise result set is closed

    // for decryption
    private static DecryptionManager decryptionManager;

    // public key generated by Deffie-Hellman algorithm, to be passed to the encrypter,
    // as well as used to initialize the cipher
    private byte[] myPublicKey;

    // generated target seed to be used to generate the password substitute
    // as part of SECMEC_USRSSBPWD security mechanism
    private byte[] myTargetSeed;

    // product id as bytes
    private static  byte[]  prdIdBytes;

    // Some byte[] constants that are frequently written into messages. It is more efficient to 
    // use these constants than to convert from a String each time 
    // (This replaces the qryscraft_ and notQryscraft_ static exception objects.)
    private static final byte[] eod00000 = { '0', '0', '0', '0', '0' };
    private static final byte[] eod02000 = { '0', '2', '0', '0', '0' };
    private static final byte[] nullSQLState = { ' ', ' ', ' ', ' ', ' ' };
    private static final byte[] errD5_D6 = { 0, 0, 0, 0, 0, 0, 0, 0 }; // 8x0 
    private static final byte[] warn0_warnA = { ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ' };  // 11x ' '

    private final static String AUTHENTICATION_PROVIDER_BUILTIN_CLASS =
    "org.apache.derby.impl.jdbc.authentication.BasicAuthenticationServiceImpl";

    private final static String AUTHENTICATION_PROVIDER_NONE_CLASS =
    "org.apache.derby.impl.jdbc.authentication.NoneAuthenticationServiceImpl";

    // Work around a classloader bug involving interrupt handling during
    // class loading. If the first request to load the
    // DRDAProtocolExceptionInfo class occurs during shutdown, the
    // loading of the class may be aborted when the Network Server calls
    // Thread.interrupt() on the DRDAConnThread. By including a static
    // reference to the DRDAProtocolExceptionInfo class here, we ensure
    // that it is loaded as soon as the DRDAConnThread class is loaded,
    // and therefore we know we won't be trying to load the class during
    // shutdown. See DERBY-1338 for more background, including pointers
    // to the apparent classloader bug in the JVM.
    private static final DRDAProtocolExceptionInfo dummy =
        new DRDAProtocolExceptionInfo(0,0,0,false);
    /**
     * Tells if the reset / connect request is a deferred request.
     * This information is used to work around a bug (DERBY-3596) in a
     * compatible manner, which also avoids any changes in the client driver.
     * <p>
     * The bug manifests itself when a connection pool data source is used and
     * logical connections are obtained from the physical connection associated
     * with the data source. Each new logical connection causes a new physical
     * connection on the server, including a new transaction. These connections
     * and transactions are not closed / cleaned up.
     */
    private boolean deferredReset = false;

    // constructor
    /**
     * Create a new Thread for processing session requests
     *
     * @param session Session requesting processing
     * @param server  Server starting thread
     * @param timeSlice timeSlice for thread
     * @param logConnections
     **/

    DRDAConnThread(Session session, NetworkServerControlImpl server, 
                          long timeSlice,
                          boolean logConnections) {
        // Use a more meaningful name for this thread.
        super(NetworkServerControlImpl.getUniqueThreadName("DRDAConnThread"));

        this.session = session;
        this.server = server;
        this.timeSlice = timeSlice;
        this.logConnections = logConnections;
        this.pendingStatementTimeout = -1;
        initialize();
    }

    /**
     * Main routine for thread, loops until the thread is closed
     * Gets a session, does work for the session
     */
    @Override
    public void run() {
        if (SanityManager.DEBUG) {
            trace("Starting new connection thread");
        }

        Session prevSession;
        while(!closed())
        {

            // get a new session
            prevSession = session;
            session = server.getNextSession(session);
            if (session == null) {
                close();
            }

            if (closed()) {
                break;
            }

            if (session != prevSession)
            {
                initializeForSession();
            }
            try {
                long timeStart = System.currentTimeMillis();

                switch (session.state)
                {
                    case Session.INIT:
                        sessionInitialState();
                        if (session == null) {
                            break;
                        }
                        // else fallthrough
                    case Session.ATTEXC:
                    case Session.SECACC:
                    case Session.CHKSEC:
                        long currentTimeSlice;

                        do {
                            try {
                                processCommands();
                            } catch (DRDASocketTimeoutException ste) {
                                // Just ignore the exception. This was
                                // a timeout on the read call in
                                // DDMReader.fill(), which will happen
                                // only when timeSlice is set.
                            }
                            currentTimeSlice = getTimeSlice();
                        } while ((currentTimeSlice <= 0)  || 
                            (System.currentTimeMillis() - timeStart < currentTimeSlice));

                        break;
                    default:
                        // this is an error
                        agentError("Session in invalid state:" + session.state);
                }
            } catch (Exception e) {
                if (e instanceof DRDAProtocolException && 
                        ((DRDAProtocolException)e).isDisconnectException())
                {
                    // client went away - this is O.K. here
                    closeSession();
                }
                else
                {
                    handleException(e);
                }
            } catch (Error error) {
                // Do as little as possible, but try to cut loose the client
                // to avoid that it hangs in a socket read-call.
                // TODO: Could make use of Throwable.addSuppressed here when
                //       compiled as Java 7 (or newer).
                try {
                    closeSession();
                } catch (Throwable t) {
                    // One last attempt...
                    try {
                        session.clientSocket.close();
                    } catch (IOException ioe) {
                        // Ignore, we're in deeper trouble already.
                    } 
                } finally {
                    // Rethrow the original error, ignore errors that happened
                    // when trying to close the socket to the client.
                    throw error;
                }
            }
        }
        if (SanityManager.DEBUG) {
            trace("Ending connection thread");
        }
        server.removeThread(this);

    }
    /**
     * Get input stream
     *
     * @return input stream
     */
    protected InputStream getInputStream()
    {
        return sockis;
    }

    /**
     * Get output stream
     *
     * @return output stream
     */
    protected OutputStream getOutputStream()
    {
        return sockos;
    }

    /** Get product id as bytes */
    private static  byte[]  getProductIDBytes()
    {
        if ( prdIdBytes == null ) { prdIdBytes = NetworkServerControlImpl.prdIdBytes(); }
        return prdIdBytes;
    }

    /**
     *  get DDMReader
     * @return DDMReader for this thread
     */
    protected DDMReader getReader()
    {
        return reader;
    }
    
    /** 
     * get  DDMWriter 
     * @return DDMWriter for this thread
     */
    protected DDMWriter getWriter()
    {
        return writer;
    }

    /**
     * Get correlation id
     *
     * @return correlation id
     */
    protected int getCorrelationID ()
    {
        return correlationID;
    }

    /**
     * Get session we are working on
     *
     * @return session
     */
    protected Session getSession()
    {
        return session;
    }

    /**
     * Get Database we are working on
     *
     * @return database
     */
    protected Database getDatabase()
    {
        return database;
    }
    /**
     * Get server
     *
     * @return server
     */
    protected NetworkServerControlImpl getServer()
    {
        return server;
    }
    /**
     * Get correlation token
     *
     * @return crrtkn
     */
    protected byte[] getCrrtkn()
    {
        return (database == null) ? null : database.crrtkn;
    }
    /**
     * Get database name
     *
     * @return database name
     */
    protected String getDbName()
    {
        return (database == null) ? null : database.getDatabaseName();
    }
    /**
     * Close DRDA  connection thread
     */
    protected void close()
    {
        close = true;
    }

    /**
     * Set logging of connections
     * 
     * @param value value to set for logging connections
     */
    protected void setLogConnections(boolean value)
    {
        logConnections = value;
    }
    /**
     * Set time slice value
     *
     * @param value new value for time slice
     */
    protected void setTimeSlice(long value)
    {
        timeSlice = value;
    }
    /**
     * Indicate a communications failure
     * 
     * @param arg1 - info about the communications failure
     * @param arg2 - info about the communications failure
     * @param arg3 - info about the communications failure
     * @param arg4 - info about the communications failure
     *
     * @exception DRDAProtocolException  disconnect exception always thrown
     */
    protected void markCommunicationsFailure(String arg1, String arg2, String arg3,
        String arg4) throws DRDAProtocolException
    {
        markCommunicationsFailure(null,arg1,arg2,arg3, arg4);

    }
        
        
        /**
         * Indicate a communications failure. Log to derby.log
         * 
         * @param e  - Source exception that was thrown
         * @param arg1 - info about the communications failure
         * @param arg2 - info about the communications failure
         * @param arg3 - info about the communications failure
         * @param arg4 - info about the communications failure
         *
         * @exception DRDAProtocolException  disconnect exception always thrown
         */
        protected void markCommunicationsFailure(Exception e, String arg1, String arg2, String arg3,
                String arg4) throws DRDAProtocolException
        {
            if (e != null) {
                println2Log(getDbName(), session.drdaID, e.getMessage());
                server.consoleExceptionPrintTrace(e);
            }
        
            Object[] oa = {arg1,arg2,arg3,arg4};
            throw DRDAProtocolException.newDisconnectException(this,oa);
        }

    /**
     * Syntax error
     *
     * @param errcd     Error code
     * @param cpArg  code point value
     * @exception DRDAProtocolException
     */

    protected  void throwSyntaxrm(int errcd, int cpArg)
        throws DRDAProtocolException
    {
        throw new
            DRDAProtocolException(DRDAProtocolException.DRDA_Proto_SYNTAXRM,
                                  this,
                                  cpArg,
                                  errcd);
    }
    /**
     * Agent error - something very bad happened
     *
     * @param msg   Message describing error
     *
     * @exception DRDAProtocolException  newAgentError always thrown
     */
    protected void agentError(String msg) throws DRDAProtocolException
    {
        throw DRDAProtocolException.newAgentError(this, CodePoint.SVRCOD_PRMDMG, 
            getDbName(), msg);
    }
    /**
     * Missing code point
     *
     * @param codePoint  code point value
     * @exception DRDAProtocolException
     */
    protected void missingCodePoint(int codePoint) throws DRDAProtocolException
    {
        throwSyntaxrm(CodePoint.SYNERRCD_REQ_OBJ_NOT_FOUND, codePoint);
    }
    /**
     * Print a line to the DB2j log
     *
     * @param dbname  database name
     * @param drdaID    DRDA identifier
     * @param msg   message
     */
    protected static void println2Log(String dbname, String drdaID, String msg)
    {
        if (logStream == null) {
            logStream = Monitor.getStream();
        }

        if (dbname != null)
        {
            int endOfName = dbname.indexOf(';');
            if (endOfName != -1) {
                dbname = dbname.substring(0, endOfName);
            }
        }
        logStream.printlnWithHeader("(DATABASE = " + dbname + "), (DRDAID = " + drdaID + "), " + msg);
    }
    /**
     * Write RDBNAM
     *
     * @param rdbnam    database name
     * @exception DRDAProtocolException
     */
    protected void writeRDBNAM(String rdbnam)
        throws DRDAProtocolException
    {
        CcsidManager currentManager = writer.getCurrentCcsidManager();

        int len = Math.max(
                CodePoint.RDBNAM_LEN,
                currentManager.getByteLength(rdbnam));

        /* Write the string padded */
        writer.writeScalarPaddedString(CodePoint.RDBNAM, rdbnam, len);
    }
    /***************************************************************************
     *                   Private methods
     ***************************************************************************/

    /**
     * Initialize class
     */
    private void initialize()
    {
        // set input and output sockets
        // this needs to be done before creating reader
        sockis = session.sessionInput;
        sockos = session.sessionOutput;

        reader = new DDMReader(this, session.dssTrace);
        writer = new DDMWriter(this, session.dssTrace);
        
        /* At this stage we can initialize the strings as we have
         * the CcsidManager for the DDMWriter. */
        rdbnam = new DRDAString(writer);
        rdbcolid = new DRDAString(writer);
        pkgid = new DRDAString(writer);
        pkgcnstkn = new DRDAString(writer);
    }

    /**
     * Initialize for a new session
     */
    private void initializeForSession()
    {
        // set input and output sockets
        sockis = session.sessionInput;
        sockos = session.sessionOutput;

        // intialize reader and writer
        reader.initialize(this, session.dssTrace);
        writer.reset(session.dssTrace);

        // initialize local pointers to session info
        database = session.database;
        appRequester = session.appRequester;

        // set sqlamLevel
        if (session.state == Session.ATTEXC) {
            sqlamLevel = appRequester.getManagerLevel(CodePoint.SQLAM);
        }

        /* All sessions MUST start as EBCDIC */
        reader.setEbcdicCcsid();
        writer.setEbcdicCcsid();
    }
    /**      
     * In initial state for a session, 
     * determine whether this is a command
     * session or a DRDA protocol session.  A command session is for changing
     * the configuration of the Net server, e.g., turning tracing on
     * If it is a command session, process the command and close the session.
     * If it is a DRDA session, exchange server attributes and change session
     * state.
     */
    private void sessionInitialState()
        throws Exception
    {
        // process NetworkServerControl commands - if it is not either valid protocol  let the 
        // DRDA error handling handle it
        if (reader.isCmd())
        {
            try {
                server.processCommands(reader, writer, session);
                // reset reader and writer
                reader.initialize(this, null);
                writer.reset(null);
                closeSession();
            } catch (InterruptedException ie) {
                throw ie;
            } catch (Throwable t) {
                server.consoleExceptionPrintTrace(t);
            }

        }
        else
        {
            // exchange attributes with application requester
            exchangeServerAttributes();
        }
    }

    /**
     * Cleans up and closes a result set if an exception is thrown
     * when collecting QRYDTA in response to OPNQRY or CNTQRY.
     *
     * @param stmt the DRDA statement to clean up
     * @param sqle the exception that was thrown
     * @param writerMark start index for the first DSS to clear from
     * the output buffer
     * @exception DRDAProtocolException if a DRDA protocol error is
     * detected
     */
    private void cleanUpAndCloseResultSet(DRDAStatement stmt,
                                          SQLException sqle,
                                          int writerMark)
        throws DRDAProtocolException
    {
        if (stmt != null) {
            writer.clearDSSesBackToMark(writerMark);
            if (!stmt.rsIsClosed()) {
                try {
                    stmt.rsClose();
                } catch (SQLException ec) {
                    if (SanityManager.DEBUG) {
                        trace("Warning: Error closing result set");
                    }
                }
                writeABNUOWRM();
                writeSQLCARD(sqle, 0, 0);
            }
        } else {
            writeSQLCARDs(sqle, 0);
        }
        errorInChain(sqle);
    }

    /**
     * Process DRDA commands we can receive once server attributes have been
     * exchanged.
     *
     * @exception DRDAProtocolException
     */
    private void processCommands() throws DRDAProtocolException
    {
        DRDAStatement stmt = null;
        boolean PRPSQLSTTfailed = false;
        boolean checkSecurityCodepoint = session.requiresSecurityCodepoint();
        do
        {
            correlationID = reader.readDssHeader();
            int codePoint = reader.readLengthAndCodePoint( false );
            int writerMark = writer.markDSSClearPoint();
            
            if (checkSecurityCodepoint) {
                verifyInOrderACCSEC_SECCHK(codePoint,session.getRequiredSecurityCodepoint());
            }

            switch(codePoint)
            {
                case CodePoint.CNTQRY:
                    try{
                        stmt = parseCNTQRY();
                        if (stmt != null)
                        {
                            writeQRYDTA(stmt);
                            if (stmt.rsIsClosed())
                            {
                                writeENDQRYRM(CodePoint.SVRCOD_WARNING);
                                writeNullSQLCARDobject();
                            }
                            // Send any warnings if JCC can handle them
                            checkWarning(null, null, stmt.getResultSet(), 0, false, sendWarningsOnCNTQRY);
                            writePBSD();
                        }
                    }
                    catch(SQLException e)
                    {
                        // if we got a SQLException we need to clean up and
                        // close the result set Beetle 4758
                        cleanUpAndCloseResultSet(stmt, e, writerMark);
                    }
                    break;
                case CodePoint.EXCSQLIMM:
                    try {
                        long updateCount = parseEXCSQLIMM();
                        // RESOLVE: checking updateCount is not sufficient
                        // since it will be 0 for creates, we need to know when
                        // any logged changes are made to the database
                        // Not getting this right for JCC is probably O.K., this
                        // will probably be a problem for ODBC and XA
                        // The problem is that JDBC doesn't provide this information
                        // so we would have to expand the JDBC API or call a
                        // builtin method to check(expensive)
                        // For now we will assume that every execute immediate
                        // does an update (that is the most conservative thing)
                        if (database.RDBUPDRM_sent == false)
                        {
                            writeRDBUPDRM();
                        }

                        // we need to set update count in SQLCARD
                        checkWarning(null, database.getDefaultStatement().getStatement(),
                            null, updateCount, true, true);
                        writePBSD();
                    } catch (SQLException e)
                    {
                        writer.clearDSSesBackToMark(writerMark);
                        writeSQLCARDs(e, 0);
                        errorInChain(e);
                    }
                    break;

                case CodePoint.EXCSQLSET:
                    try {
                        if (parseEXCSQLSET()) {
                            // all went well.
                            writeSQLCARDs(null, 0);
                        }
                    }
                    catch (SQLWarning w)
                    {
                        writeSQLCARD(w, 0, 0);
                    }
                    catch (SQLException e)
                    {
                        writer.clearDSSesBackToMark(writerMark);
                        writeSQLCARDs(e, 0);
                        errorInChain(e);
                    }
                    break;
                    
                case CodePoint.PRPSQLSTT:
                    int sqldaType;
                    PRPSQLSTTfailed = false;
                    try {
                        database.getConnection().clearWarnings();
                        sqldaType = parsePRPSQLSTT();
                        database.getCurrentStatement().sqldaType = sqldaType;
                        if (sqldaType > 0) {    // do write SQLDARD
                            writeSQLDARD(database.getCurrentStatement(),
                                         (sqldaType ==  CodePoint.TYPSQLDA_LIGHT_OUTPUT),
                                         database.getConnection().getWarnings());
                        } else {
                            checkWarning(database.getConnection(), null, null, 0, true, true);
                        }

                    } catch (SQLException e)
                    {
                        writer.clearDSSesBackToMark(writerMark);
                        writeSQLCARDs(e, 0, true);
                        PRPSQLSTTfailed = true;
                        errorInChain(e);
                    }
                    break;
                case CodePoint.OPNQRY:
                    try {
                        // activate the following to run the isValid(timeout) test in jdbc4.ConnectionTest
                        /*try { 
                            Thread.sleep(2000);
                        } catch (InterruptedException ie) {
                            System.out.println("interrupted exception: " + ie.getMessage());
                            ie.printStackTrace();
                        }*/
                        if (PRPSQLSTTfailed) {
                            // read the command objects
                            // for ps with parameter
                            // Skip objects/parameters
                            skipRemainder(true);

                            // If we failed to prepare, then we fail
                            // to open, which  means OPNQFLRM.
                            writeOPNQFLRM(null);
                            break;
                        }
                        Pkgnamcsn pkgnamcsn = parseOPNQRY();
                        if (pkgnamcsn != null)
                        {
                            stmt = database.getDRDAStatement(pkgnamcsn);
                            PreparedStatement ps = stmt.getPreparedStatement();
                            ps.clearWarnings();
                            if (pendingStatementTimeout >= 0) {
                                ps.setQueryTimeout(pendingStatementTimeout);
                                pendingStatementTimeout = -1;
                            }
                            stmt.execute();
                            writeOPNQRYRM(false, stmt);
                            checkWarning(null, ps, null, 0, false, true);

                            long sentVersion = stmt.versionCounter;
                            long currentVersion =
                                    ((EnginePreparedStatement)stmt.ps).
                                    getVersionCounter();

                            if (stmt.sqldaType ==
                                    CodePoint.TYPSQLDA_LIGHT_OUTPUT &&
                                    currentVersion != sentVersion) {
                                // DERBY-5459. The prepared statement has a
                                // result set and has changed on the server
                                // since we last informed the client about its
                                // shape, so re-send metadata.
                                //
                                // NOTE: This is an extension of the standard
                                // DRDA protocol since we send the SQLDARD
                                // even if it isn't requested in this case.
                                // This is OK because there is already code on the
                                // client to handle an unrequested SQLDARD at
                                // this point in the protocol.
                                writeSQLDARD(stmt, true, null);
                            }
                            writeQRYDSC(stmt, false);

                            stmt.rsSuspend();

                            if (stmt.getQryprctyp() == CodePoint.LMTBLKPRC &&
                                    stmt.getQryrowset() != 0) {
                                // The DRDA spec allows us to send
                                // QRYDTA here if there are no LOB
                                // columns.
                                DRDAResultSet drdars =
                                    stmt.getCurrentDrdaResultSet();
                                try {
                                    if (drdars != null &&
                                        !drdars.hasLobColumns()) {
                                        writeQRYDTA(stmt);
                                    }
                                } catch (SQLException sqle) {
                                    cleanUpAndCloseResultSet(stmt, sqle,
                                                             writerMark);
                                }
                            }
                        }
                        writePBSD();
                    }
                    catch (SQLException e)
                    {
                        writer.clearDSSesBackToMark(writerMark);
                        // The fix for DERBY-1196 removed code 
                        // here to close the prepared statement 
                        // if OPNQRY failed.
                            writeOPNQFLRM(e);
                    }
                    break;
                case CodePoint.RDBCMM:
                    try
                    {
                        if (SanityManager.DEBUG) {
                            trace("Received commit");
                        }
                        if (!database.getConnection().getAutoCommit())
                        {
                            database.getConnection().clearWarnings();
                            database.commit();
                            writeENDUOWRM(COMMIT);
                            checkWarning(database.getConnection(), null, null, 0, true, true);
                        }
                        // we only want to write one of these per transaction
                        // so set to false in preparation for next command
                        database.RDBUPDRM_sent = false;
                    }
                    catch (SQLException e)
                    {
                        writer.clearDSSesBackToMark(writerMark);
                        // Even in case of error, we have to write the ENDUOWRM.
                        writeENDUOWRM(COMMIT);
                        writeSQLCARDs(e, 0);
                        errorInChain(e);
                    }
                    break;
                case CodePoint.RDBRLLBCK:
                    try
                    {
                        if (SanityManager.DEBUG) {
                            trace("Received rollback");
                        }
                        database.getConnection().clearWarnings();
                        database.rollback();
                        writeENDUOWRM(ROLLBACK);
                        checkWarning(database.getConnection(), null, null, 0, true, true);
                        // we only want to write one of these per transaction
                        // so set to false in preparation for next command
                        database.RDBUPDRM_sent = false;
                    }
                    catch (SQLException e)
                    {
                        writer.clearDSSesBackToMark(writerMark);
                        // Even in case of error, we have to write the ENDUOWRM.
                        writeENDUOWRM(ROLLBACK);
                        writeSQLCARDs(e, 0);
                        errorInChain(e);
                    }
                    break;
                case CodePoint.CLSQRY:
                    try{
                        stmt = parseCLSQRY();
                        stmt.rsClose();
                        writeSQLCARDs(null, 0);
                    }
                    catch (SQLException e)
                    {
                        writer.clearDSSesBackToMark(writerMark);
                        writeSQLCARDs(e, 0);
                        errorInChain(e);
                    }
                    break;
                case CodePoint.EXCSAT:
                    parseEXCSAT();
                    writeEXCSATRD();
                    break;
                case CodePoint.ACCSEC:
                    int securityCheckCode = parseACCSEC();
                    writeACCSECRD(securityCheckCode); 
                    
                    /* ACCSECRD is the last reply that is mandatorily in EBCDIC */
                    if (appRequester.supportsUtf8Ccsid()) {
                        switchToUtf8();
                    } else {
                        /* This thread might serve several requests.
                         * Revert if not supported by current client. */
                        switchToEbcdic();
                    }
                    checkSecurityCodepoint = true;
                    break;
                case CodePoint.SECCHK:
                    if (parseDRDAConnection()) {
                        // security all checked and connection ok
                        checkSecurityCodepoint = false;
                    }
                    break;
                /* since we don't support sqlj, we won't get bind commands from jcc, we
                 * might get it from ccc; just skip them.
                 */
                case CodePoint.BGNBND:
                    reader.skipBytes();
                    writeSQLCARDs(null, 0);
                    break;
                case CodePoint.BNDSQLSTT:
                    reader.skipBytes();
                    parseSQLSTTDss();
                    writeSQLCARDs(null, 0);
                    break;
                case CodePoint.SQLSTTVRB:
                    // optional
                    reader.skipBytes();
                    break;
                case CodePoint.ENDBND:
                    reader.skipBytes();
                    writeSQLCARDs(null, 0);
                    break;
                case CodePoint.DSCSQLSTT:
                    if (PRPSQLSTTfailed) {
                        reader.skipBytes();
                        writeSQLCARDs(null, 0);
                        break;
                    }
                    try {
                        boolean rtnOutput = parseDSCSQLSTT();
                        writeSQLDARD(database.getCurrentStatement(), rtnOutput,
                                     null);
                        
                    } catch (SQLException e)
                    {
                        writer.clearDSSesBackToMark(writerMark);
                        server.consoleExceptionPrint(e);
                        try {
                            writeSQLDARD(database.getCurrentStatement(), true, e);
                        } catch (SQLException e2) { // should not get here since doing nothing with ps
                            agentError("Why am I getting another SQLException?");
                        }
                        errorInChain(e);
                    }
                    break;
                case CodePoint.EXCSQLSTT:
                    if (PRPSQLSTTfailed) {
                        // Skip parameters too if they are chained Beetle 4867
                        skipRemainder(true);
                        writeSQLCARDs(null, 0);
                        break;
                    }
                    try {
                        parseEXCSQLSTT();

                        DRDAStatement curStmt = database.getCurrentStatement();
                        if (curStmt != null) {
                            curStmt.rsSuspend();
                        }
                        writePBSD();
                    } catch (SQLException e)
                    {
                        skipRemainder(true);
                        writer.clearDSSesBackToMark(writerMark);
                        if (SanityManager.DEBUG) 
                        {
                            server.consoleExceptionPrint(e);
                        }
                        writeSQLCARDs(e, 0);
                        errorInChain(e);
                    }
                    break;
                case CodePoint.SYNCCTL:
                    if (xaProto == null) {
                        xaProto = new DRDAXAProtocol(this);
                    }
                    xaProto.parseSYNCCTL();
                    try {
                        writePBSD();
                    } catch (SQLException se) {
                        server.consoleExceptionPrint(se);
                        errorInChain(se);
                    }
                    break;
                default:
                    codePointNotSupported(codePoint);
            }

            if (SanityManager.DEBUG) {
                String cpStr = new CodePointNameTable().lookup(codePoint);
                try {
                    PiggyBackedSessionData pbsd =
                            database.getPiggyBackedSessionData(false);
                    // DERBY-3596
                    // Don't perform this assert if a deferred reset is
                    // happening or has recently taken place, because the
                    // connection state has been changed under the feet of the
                    // piggy-backing mechanism.
                    if (!this.deferredReset && pbsd != null) {
                        // Session data has already been piggy-backed. Refresh
                        // the data from the connection, to make sure it has
                        // not changed behind our back.
                        pbsd.refresh();
                        SanityManager.ASSERT(!pbsd.isModified(),
                              "Unexpected PBSD modification: " + pbsd +
                              " after codePoint " + cpStr);
                    }
                    // Not having a pbsd here is ok. No data has been
                    // piggy-backed and the client has no cached values.
                    // If needed it will send an explicit request to get
                    // session data
                } catch (SQLException sqle) {
                    server.consoleExceptionPrint(sqle);
                    SanityManager.THROWASSERT("Unexpected exception after " +
                            "codePoint "+cpStr, sqle);
                }
            }

            // Set the correct chaining bits for whatever
            // reply DSS(es) we just wrote.  If we've reached
            // the end of the chain, this method will send
            // the DSS(es) across.
            finalizeChain();

        }
        while (reader.isChainedWithSameID() || reader.isChainedWithDiffID());
    }

    /**
     * If there's a severe error in the DDM chain, and if the header indicates
     * "terminate chain on error", we stop processing further commands in the chain
     * nor do we send any reply for them.  In accordance to this, a SQLERRRM message 
     * indicating the severe error must have been sent! (otherwise application requestor,
     * such as JCC, would not terminate the receiving of chain replies.)
     *
     * Each DRDA command is processed independently. DRDA defines no interdependencies
     * across chained commands. A command is processed the same when received within
     * a set of chained commands or received separately.  The chaining was originally
     * defined as a way to save network costs.
     *
     * @param e     the SQLException raised
     * @exception   DRDAProtocolException
     */
    private void errorInChain(SQLException e) throws DRDAProtocolException
    {
        if (reader.terminateChainOnErr() && (getExceptionSeverity(e) > CodePoint.SVRCOD_ERROR))
        {
            if (SanityManager.DEBUG) {
                trace("terminating the chain on error...");
            }
            skipRemainder(false);
        }
    }

    /**
     * Exchange server attributes with application requester
     *
     * @exception DRDAProtocolException
     */
    private void exchangeServerAttributes()
        throws  DRDAProtocolException
    {
        int codePoint;
        correlationID = reader.readDssHeader();
        if (SanityManager.DEBUG) {
          if (correlationID == 0)
          {
            SanityManager.THROWASSERT(
                          "Unexpected value for correlationId = " + correlationID);
          }
        }

        codePoint = reader.readLengthAndCodePoint( false );

        // The first code point in the exchange of attributes must be EXCSAT
        if (codePoint != CodePoint.EXCSAT)
        {
            //Throw PRCCNVRM 
            throw
                new DRDAProtocolException(DRDAProtocolException.DRDA_Proto_PRCCNVRM,
                                          this, codePoint,
                                          CodePoint.PRCCNVCD_EXCSAT_FIRST_AFTER_CONN);
        }

        parseEXCSAT();
        writeEXCSATRD();
        finalizeChain();
        session.setState(Session.ATTEXC);
    }
    

    private boolean parseDRDAConnection() throws DRDAProtocolException
    {
        int codePoint;
        boolean sessionOK = true;


        int securityCheckCode = parseSECCHK();
        if (SanityManager.DEBUG) {
            trace("*** SECCHKRM securityCheckCode is: " + securityCheckCode);
        }
        writeSECCHKRM(securityCheckCode);
        //at this point if the security check failed, we're done, the session failed
        if (securityCheckCode != 0)
        {
            return false;
        }

        correlationID = reader.readDssHeader();
        codePoint = reader.readLengthAndCodePoint( false );
        verifyRequiredObject(codePoint,CodePoint.ACCRDB);
        int svrcod = parseACCRDB();

        //If network server gets a null connection form InternalDriver, reply with
        //RDBAFLRM and SQLCARD with null SQLException 
        if(database.getConnection() == null && databaseAccessException == null){
            writeRDBfailure(CodePoint.RDBAFLRM);
            return false;
        }
        
        //if earlier we couldn't access the database
        if (databaseAccessException != null)
        {

            //if the Database was not found we will try DS
            int failureType = getRdbAccessErrorCodePoint();
            if (failureType == CodePoint.RDBNFNRM 
                || failureType == CodePoint.RDBATHRM)
            {
                writeRDBfailure(failureType);
            }
            else
            {
                writeRDBfailure(CodePoint.RDBAFLRM);
            }
            return false;
        }
        else if (database.accessCount > 1 ) // already in conversation with database
        {
            writeRDBfailure(CodePoint.RDBACCRM);
            return false;
        }
        else
        {
            writeACCRDBRM(svrcod);
        }

        // compare this application requester with previously stored
        // application requesters and if we have already seen this one
        // use stored application requester 
        session.appRequester = server.getAppRequester(appRequester);
        return sessionOK;
    }

    /**
     * Switch the DDMWriter and DDMReader to UTF8 IF supported
     */
    private void switchToUtf8() {
        writer.setUtf8Ccsid();
        reader.setUtf8Ccsid();
    }
    
    /**
     * Switch the DDMWriter and DDMReader to EBCDIC
     */
    private void switchToEbcdic() {
        writer.setEbcdicCcsid();
        reader.setEbcdicCcsid();
    }
    
    /**
     * Write RDB Failure
     *
     * Instance Variables
     *  SVRCOD - Severity Code - required
     *  RDBNAM - Relational Database name - required
     *  SRVDGN - Server Diagnostics - optional (not sent for now)
     *
     * @param   codePoint   codepoint of failure
     */
    private void writeRDBfailure(int codePoint) throws DRDAProtocolException
    {
        writer.createDssReply();
        writer.startDdm(codePoint);
        writer.writeScalar2Bytes(CodePoint.SVRCOD, CodePoint.SVRCOD_ERROR);
        writeRDBNAM(database.getDatabaseName());
        writer.endDdmAndDss();
        
        switch(codePoint){
            case CodePoint.RDBAFLRM:
                //RDBAFLRM requires TYPDEFNAM and TYPDEFOVR
                writer.createDssObject();
                writer.writeScalarString(CodePoint.TYPDEFNAM,
                                         CodePoint.TYPDEFNAM_QTDSQLASC);
                writeTYPDEFOVR();
                writer.endDss();
            case CodePoint.RDBNFNRM:
            case CodePoint.RDBATHRM:
                writeSQLCARD(databaseAccessException, 0, 0);
            case CodePoint.RDBACCRM:
                //Ignore anything that was chained to the ACCRDB.
                skipRemainder(false);

                // Finalize chain state for whatever we wrote in
                // response to ACCRDB.
                finalizeChain();
                break;
        }
        
    }

    /* Check the database access exception and return the appropriate
       error codepoint.
       RDBNFNRM - Database not found
       RDBATHRM - Not Authorized
       RDBAFLRM - Access failure
       @return RDB Access codepoint 
               
    */

    private int getRdbAccessErrorCodePoint()
    {
        String sqlState = databaseAccessException.getSQLState();
        // These tests are ok since DATABASE_NOT_FOUND and 
        // AUTH_INVALID_USER_NAME are not ambigious error codes (on the first
        // five characters) in SQLState. If they were, we would have to
        // perform a similar check as done in method isAuthenticationException
        if (sqlState.regionMatches(0,SQLState.DATABASE_NOT_FOUND,0,5)) {
            // RDB not found codepoint
            return CodePoint.RDBNFNRM;
        } else {
            if (isAuthenticationException(databaseAccessException) ||
                sqlState.regionMatches(0,SQLState.AUTH_INVALID_USER_NAME,0,5)) {
                // Not Authorized To RDB reply message codepoint
                return CodePoint.RDBATHRM;
            } else {
                // RDB Access Failed Reply Message codepoint
                return CodePoint.RDBAFLRM;
            }
        }
    }

    /**
     * There are multiple reasons for not getting a connection, and
     * all these should throw SQLExceptions with SQL state 08004
     * according to the SQL standard. Since only one of these SQL
     * states indicate that an authentication error has occurred, it
     * is not enough to check that the SQL state is 08004 and conclude
     * that authentication caused the exception to be thrown.
     *
     * This method tries to get a StandardException from the SQLException
     * and use getMessageId on that object to check for authentication
     * error instead of the SQL state we get from
     * SQLExceptions#getSQLState. getMessageId returns the entire id
     * as defined in SQLState (e.g. 08004.C.1), while getSQLState only
     * return the 5 first characters (i.e. 08004 instead of 08004.C.1)
     *
     * If the SQLException isn't linked to a StandardException, the
     * assumption that SQL State 08004 is caused by an authentication
     * failure is followed even though this is not correct. This was
     * the pre DERBY-3060 way of solving the issue.
     *
     * @param sqlException The exception that is checked to see if
     * this is really caused by an authentication failure
     * @return true if sqlException is (or has to be assumed to be)
     * caused by an authentication failure, false otherwise.
     * @see SQLState
     */
    private boolean isAuthenticationException (SQLException sqlException) {
        boolean authFail = false;

        // get exception which carries Derby messageID and args
        StandardException se = StandardException.getArgumentFerry(sqlException);

        if (se != null) {
            // DERBY-3060: if this is a Derby exception, we can
            // check the messageId to find out what caused the
            // exception.

            String msgId = se.getMessageId();

            // Of the 08004.C.x messages, only
            // SQLState.NET_CONNECT_AUTH_FAILED is an authentication
            // exception
            if (msgId.equals(SQLState.NET_CONNECT_AUTH_FAILED)) {
                authFail = true;
            }
        } else {
            String sqlState = sqlException.getSQLState();
            if (sqlState.regionMatches(0,SQLState.LOGIN_FAILED,0,5)) {
                // Unchanged by DERBY-3060: This is not an
                // Derby exception, so we cannot check the
                // messageId. As before DERBY-3060, we assume that all
                // 08004 error codes are due to an authentication
                // failure, even though this ambigious
                authFail = true;
            }
        }
        return authFail;
    }

    /**
     * Verify userId and password
     *
     * Username and password is verified by making a connection to the
     * database
     *
     * @return security check code, 0 is O.K.
     * @exception DRDAProtocolException
     */
    private int verifyUserIdPassword() throws DRDAProtocolException
    {
        databaseAccessException = null;
        return getConnFromDatabaseName();
    }

    /**
     * Get connection from a database name
     *
     * Username and password is verified by making a connection to the
     * database
     *
     * @return security check code, 0 is O.K.
     * @exception DRDAProtocolException
     */
    private int getConnFromDatabaseName() throws DRDAProtocolException
    {
        Properties p = new Properties();
        databaseAccessException = null;
        //if we haven't got the correlation token yet, use session number for drdaID
        if (session.drdaID == null) {
            session.drdaID = leftBrace + session.connNum + rightBrace;
        }
        p.put(Attribute.DRDAID_ATTR, session.drdaID);

        // We pass extra property information for the authentication provider
        // to successfully re-compute the substitute (hashed) password and
        // compare it with what we've got from the requester (source).
        //
        // If a password attribute appears as part of the connection URL
        // attributes, we then don't use the substitute hashed password
        // to authenticate with the engine _as_ the one (if any) as part
        // of the connection URL attributes, will be used to authenticate
        // against Derby's BUILT-IN authentication provider - As a reminder,
        // Derby allows password to be mentioned as part of the connection
        // URL attributes, as this extra capability could be useful to pass
        // passwords to external authentication providers for Derby; hence
        // a password defined as part of the connection URL attributes cannot
        // be substituted (single-hashed) as it is not recoverable.
        if ((database.securityMechanism == CodePoint.SECMEC_USRSSBPWD) &&
            (database.getDatabaseName().indexOf(Attribute.PASSWORD_ATTR) == -1))
        {
            p.put(Attribute.DRDA_SECMEC,
                  String.valueOf(database.securityMechanism));
            p.put(Attribute.DRDA_SECTKN_IN,
                  DecryptionManager.toHexString(database.secTokenIn, 0,
                                                database.secTokenIn.length));
            p.put(Attribute.DRDA_SECTKN_OUT,
                  DecryptionManager.toHexString(database.secTokenOut, 0,
                                                database.secTokenOut.length));
        }
            
        try {
            database.makeConnection(p);
        } catch (SQLException se) {
            databaseAccessException = se;
            for (; se != null; se = se.getNextException())
            {
                if (SanityManager.DEBUG) {
                    trace(se.getMessage());
                }
                println2Log(database.getDatabaseName(), session.drdaID, se.getMessage());
            }

            if (isAuthenticationException(databaseAccessException)) {
                // need to set the security check code based on the
                // reason the connection was denied, Derby doesn't say
                // whether the userid or password caused the problem,
                // so we will just return userid invalid
                return CodePoint.SECCHKCD_USERIDINVALID;
            } else {
                return 0;
            }
        }
        catch (Exception e)
        {
            // If Derby has shut down for some reason,
            // we will send  an agent error and then try to 
            // get the driver loaded again.  We have to get
            // rid of the client first in case they are holding
            // the DriverManager lock.
            println2Log(database.getDatabaseName(), session.drdaID, 
                        "Driver not loaded"
                        + e.getMessage());
                try {
                    agentError("Driver not loaded");
                }
                catch (DRDAProtocolException dpe)
                {
                    // Retry starting the server before rethrowing 
                    // the protocol exception.  Then hopfully all
                    // will be well when they try again.
                    try {
                        server.startNetworkServer();
                    } catch (Exception re) {
                        println2Log(database.getDatabaseName(), session.drdaID, "Failed attempt to reload driver " +re.getMessage()  );
                    }
                    throw dpe;
                }
        }
        
    
        // Everything worked so log connection to the database.
        if (getLogConnections()) {
            println2Log(database.getDatabaseName(), session.drdaID,
                "Apache Derby Network Server connected to database " +
                        database.getDatabaseName());
        }
        return 0;
    }


    /**
     * Parses EXCSAT (Exchange Server Attributes)
     * Instance variables
     *  EXTNAM(External Name)   - optional
     *  MGRLVLLS(Manager Levels) - optional
     *  SPVNAM(Supervisor Name) - optional
     *  SRVCLSNM(Server Class Name) - optional
     *  SRVNAM(Server Name) - optional, ignorable
     *  SRVRLSLV(Server Product Release Level) - optional, ignorable
     *
     * @exception DRDAProtocolException
     */
    private void parseEXCSAT() throws DRDAProtocolException
    {
        int codePoint;
        String strVal;

        // There are three kinds of EXCSAT's we might get.
        // 1) Initial Exchange attributes.
        //    For this we need to initialize the apprequester.
        //    Session state is set to ATTEXC and then the AR must 
        //    follow up with ACCSEC and SECCHK to get the connection.
        //  2) Send of EXCSAT as ping or mangager level adjustment. 
        //     (see parseEXCSAT2())
        //     For this we just ignore the EXCSAT objects that
        //     are already set.
        //  3) Send of EXCSAT for connection reset. (see parseEXCSAT2())
        //     This is treated just like ping and will be followed up 
        //     by an ACCSEC request if in fact it is a connection reset.

        // If we have already exchanged attributes once just 
        // process any new manager levels and return (case 2 and 3 above)
        this.deferredReset = false; // Always reset, only set to true below.
        if (appRequester != null)
        {
            // DERBY-3596
            // Don't mess with XA requests, as the logic for these are handled
            // by the server side (embedded) objects. Note that XA requests
            // results in a different database object implementation, and it
            // does not have the bug we are working around.
            if (!appRequester.isXARequester()) {
                this.deferredReset = true; // Non-XA deferred reset detected.
            }
            parseEXCSAT2();
            return;
        }

        // set up a new Application Requester to store information about the
        // application requester for this session

        appRequester = new AppRequester();

        reader.markCollection();

        codePoint = reader.getCodePoint();
        while (codePoint != -1)
        {
            switch (codePoint)
            {
                // optional
                case CodePoint.EXTNAM:
                    appRequester.extnam = reader.readString();
                    if (SanityManager.DEBUG) {
                        trace("extName = " + appRequester.extnam);
                    }
                    if (appRequester.extnam.length() > CodePoint.MAX_NAME) {
                        tooBig(CodePoint.EXTNAM);
                    }
                    break;
                // optional
                case CodePoint.MGRLVLLS:
                    parseMGRLVLLS(1);
                    break;
                // optional 
                case CodePoint.SPVNAM:
                    appRequester.spvnam = reader.readString();
                    // This is specified as a null parameter so length should
                    // be zero
                    if (appRequester.spvnam != null) {
                        badObjectLength(CodePoint.SPVNAM);
                    }
                    break;
                // optional
                case CodePoint.SRVNAM:
                    appRequester.srvnam = reader.readString();
                    if (SanityManager.DEBUG) {
                        trace("serverName = " +  appRequester.srvnam);
                    }
                    if (appRequester.srvnam.length() > CodePoint.MAX_NAME) {
                        tooBig(CodePoint.SRVNAM);
                    }
                    break;
                // optional
                case CodePoint.SRVRLSLV:
                    appRequester.srvrlslv = reader.readString();
                    if (SanityManager.DEBUG) {
                        trace("serverlslv = " + appRequester.srvrlslv);
                    }
                    if (appRequester.srvrlslv.length() > CodePoint.MAX_NAME) {
                        tooBig(CodePoint.SRVRLSLV);
                    }
                    break;
                // optional
                case CodePoint.SRVCLSNM:
                    appRequester.srvclsnm = reader.readString();
                    if (SanityManager.DEBUG) {
                        trace("serverClassName = " + appRequester.srvclsnm);
                    }
                    if (appRequester.srvclsnm.length() > CodePoint.MAX_NAME) {
                        tooBig(CodePoint.SRVCLSNM);
                    }
                    break;
                default:
                    invalidCodePoint(codePoint);
            }
            codePoint = reader.getCodePoint();
        }
    }

    /**
     * Parses EXCSAT2 (Exchange Server Attributes)
     * Instance variables
     *  EXTNAM(External Name)   - optional
     *  MGRLVLLS(Manager Levels) - optional
     *  SPVNAM(Supervisor Name) - optional
     *  SRVCLSNM(Server Class Name) - optional
     *  SRVNAM(Server Name) - optional, ignorable
     *  SRVRLSLV(Server Product Release Level) - optional, ignorable
     *
     * @exception DRDAProtocolException
     * 
     * This parses a second occurrence of an EXCSAT command
     * The target must ignore the values for extnam, srvclsnm, srvnam and srvrlslv.
     * I am also going to ignore spvnam since it should be null anyway.
     * Only new managers can be added.
     */
    private void parseEXCSAT2() throws DRDAProtocolException
    {
        int codePoint;
        reader.markCollection();

        codePoint = reader.getCodePoint();
        while (codePoint != -1)
        {
            switch (codePoint)
            {
                // optional
                case CodePoint.EXTNAM:
                case CodePoint.SRVNAM:
                case CodePoint.SRVRLSLV:
                case CodePoint.SRVCLSNM:
                case CodePoint.SPVNAM:
                    reader.skipBytes();
                    break;
                // optional
                case CodePoint.MGRLVLLS:
                    parseMGRLVLLS(2);
                    break;
                default:
                    invalidCodePoint(codePoint);
            }
            codePoint = reader.getCodePoint();
        }
    }

    /**
     *  Parse manager levels
     *  Instance variables
     *      MGRLVL - repeatable, required
     *        CODEPOINT
     *          CCSIDMGR - CCSID Manager
     *          CMNAPPC - LU 6.2 Conversational Communications Manager
     *          CMNSYNCPT - SNA LU 6.2 SyncPoint Conversational Communications Manager
     *          CMNTCPIP - TCP/IP Communication Manager
     *          DICTIONARY - Dictionary
     *          RDB - Relational Database
     *          RSYNCMGR - Resynchronization Manager
     *          SECMGR - Security Manager
     *          SQLAM - SQL Application Manager
     *          SUPERVISOR - Supervisor
     *          SYNCPTMGR - Sync Point Manager
     *        VALUE
     *
     *  On the second appearance of this codepoint, it can only add managers
     *
     * @param time  1 for first time this is seen, 2 for subsequent ones
     * @exception DRDAProtocolException
     * 
     */
    private void parseMGRLVLLS(int time) throws DRDAProtocolException
    {
        int manager, managerLevel;
        int currentLevel;
        // set up vectors to keep track of manager information
        unknownManagers = new ArrayList<Integer>();
        knownManagers = new ArrayList<Integer>();
        ArrayList<Integer> errorManagers = new ArrayList<Integer>();
        ArrayList<Integer> errorManagersLevel = new ArrayList<Integer>();
        if (SanityManager.DEBUG) {
            trace("Manager Levels");
        }

        while (reader.moreDdmData())
        {
            manager = reader.readNetworkShort();
            managerLevel = reader.readNetworkShort();
            if (CodePoint.isKnownManager(manager))
            {
                knownManagers.add(manager);
                //if the manager level hasn't been set, set it
                currentLevel = appRequester.getManagerLevel(manager);
                if (currentLevel == AppRequester.MGR_LEVEL_UNKNOWN)
                {
                    appRequester.setManagerLevel(manager, managerLevel);
                }
                else
                {
                    //if the level is still the same we'll ignore it
                    if (currentLevel != managerLevel)
                    {
                        //keep a list of conflicting managers
                        errorManagers.add(manager);
                        errorManagersLevel.add(managerLevel);
                    }
                }

            }
            else
            {
                unknownManagers.add(manager);
            }

            if (SanityManager.DEBUG) {
               trace("Manager = " + java.lang.Integer.toHexString(manager) + 
                      " ManagerLevel " + managerLevel);
            }
        }
        sqlamLevel = appRequester.getManagerLevel(CodePoint.SQLAM);
        // did we have any errors
        if (errorManagers.size() > 0)
        {
            Object [] oa = new Object[errorManagers.size()*2];
            int j = 0;
            for (int i = 0; i < errorManagers.size(); i++)
            {
                oa[j++] = errorManagers.get(i);
                oa[j++] = errorManagersLevel.get(i);
            }
            throw new DRDAProtocolException(DRDAProtocolException.DRDA_Proto_MGRLVLRM,
                                          this, 0,
                                          0, oa);
        }
    }
    /**
     * Write reply to EXCSAT command
     * Instance Variables
     *  EXTNAM - External Name (optional)
     *  MGRLVLLS - Manager Level List (optional)
     *  SRVCLSNM - Server Class Name (optional) - used by JCC
     *  SRVNAM - Server Name (optional)
     *  SRVRLSLV - Server Product Release Level (optional)
     *
     * @exception DRDAProtocolException
     */
    private void writeEXCSATRD() throws DRDAProtocolException
    {
        writer.createDssReply();
        writer.startDdm(CodePoint.EXCSATRD);
        writer.writeScalarString(CodePoint.EXTNAM,
                                 NetworkServerControlImpl.att_extnam());
        //only reply with manager levels if we got sent some
        if (knownManagers != null && knownManagers.size() > 0) {
            writeMGRLEVELS();
        }
        writer.writeScalarString(CodePoint.SRVCLSNM,
                                 NetworkServerControlImpl.att_srvclsnm());
        writer.writeScalarString(CodePoint.SRVNAM,
                                 NetworkServerControlImpl.ATT_SRVNAM);
        writer.writeScalarString(CodePoint.SRVRLSLV,
                                 NetworkServerControlImpl.att_srvrlslv());
        writer.endDdmAndDss();
    }
    /**
     * Write manager levels
     * The target server must not provide information for any target
     * managers unless the source explicitly requests it.
     * For each manager class, if the target server's support level
     * is greater than or equal to the source server's level, then the source
     * server's level is returned for that class if the target server can operate
     * at the source's level; otherwise a level 0 is returned.  If the target
     * server's support level is less than the source server's level, the
     * target server's level is returned for that class.  If the target server
     * does not recognize the code point of a manager class or does not support
     * that class, it returns a level of 0.  The target server then waits
     * for the next command or for the source server to terminate communications.
     * When the source server receives EXCSATRD, it must compare each of the entries
     * in the mgrlvlls parameter it received to the corresponding entries in the mgrlvlls
     * parameter it sent.  If any level mismatches, the source server must decide
     * whether it can use or adjust to the lower level of target support for that manager
     * class.  There are no architectural criteria for making this decision.
     * The source server can terminate communications or continue at the target
     * servers level of support.  It can also attempt to use whatever
     * commands its user requests while receiving error reply messages for real
     * functional mismatches.
     * The manager levels the source server specifies or the target server
     * returns must be compatible with the manager-level dependencies of the specified
     * manangers.  Incompatible manager levels cannot be specified.
     *  Instance variables
     *      MGRLVL - repeatable, required
     *        CODEPOINT
     *          CCSIDMGR - CCSID Manager
     *          CMNAPPC - LU 6.2 Conversational Communications Manager
     *          CMNSYNCPT - SNA LU 6.2 SyncPoint Conversational Communications Manager
     *          CMNTCPIP - TCP/IP Communication Manager
     *          DICTIONARY - Dictionary
     *          RDB - Relational Database
     *          RSYNCMGR - Resynchronization Manager
     *          SECMGR - Security Manager
     *          SQLAM - SQL Application Manager
     *          SUPERVISOR - Supervisor
     *          SYNCPTMGR - Sync Point Manager
     *          XAMGR - XA manager
     *        VALUE
     */
    private void writeMGRLEVELS() throws DRDAProtocolException
    {
        writer.startDdm(CodePoint.MGRLVLLS);
        for (int manager : knownManagers)
        {
            int appLevel = appRequester.getManagerLevel(manager);
            int serverLevel = server.getManagerLevel(manager);
            if (serverLevel >= appLevel)
            {
                //Note appLevel has already been set to 0 if we can't support
                //the original app Level
                writer.writeCodePoint4Bytes(manager, appLevel);
            }
            else
            {
                writer.writeCodePoint4Bytes(manager, serverLevel);
                // reset application manager level to server level
                appRequester.setManagerLevel(manager, serverLevel);
            }
        }
        // write 0 for all unknown managers
        for (int manager : unknownManagers)
        {
            writer.writeCodePoint4Bytes(manager, 0);
        }
        writer.endDdm();
    }
    /**
     *  Parse Access Security
     *
     *  If the target server supports the SECMEC requested by the application requester
     *  then a single value is returned and it is identical to the SECMEC value
     *  in the ACCSEC command. If the target server does not support the SECMEC
     *  requested, then one or more values are returned and the application requester
     *  must choose one of these values for the security mechanism.
     *  We currently support
     *      - user id and password (default for JCC)
     *      - encrypted user id and password
     *      - strong password substitute (USRSSBPWD w/
     *                                    Derby network client only)
     *
     *  Instance variables
     *    SECMGRNM  - security manager name - optional
     *    SECMEC    - security mechanism - required
     *    RDBNAM    - relational database name - optional
     *    SECTKN    - security token - optional, (required if sec mech. needs it)
     *
     *  @return security check code - 0 if everything O.K.
     */
    private int parseACCSEC() throws  DRDAProtocolException
    {
        int securityCheckCode = 0;
        int securityMechanism = 0;
        byte [] secTokenIn = null;

        reader.markCollection();
        int codePoint = reader.getCodePoint();
        while (codePoint != -1)
        {
            switch(codePoint)
            {
                //optional
                case CodePoint.SECMGRNM:
                    // this is defined to be 0 length
                    if (reader.getDdmLength() != 0) {
                        badObjectLength(CodePoint.SECMGRNM);
                    }
                    break;
                //required
                case CodePoint.SECMEC:
                    checkLength(CodePoint.SECMEC, 2);
                    securityMechanism = reader.readNetworkShort();
                    if (SanityManager.DEBUG) {
                        trace("parseACCSEC - Security mechanism = " + securityMechanism);
                    }
                    
                    // if Property.DRDA_PROP_SECURITYMECHANISM has been set, then
                    // network server only accepts connections which use that
                    // security mechanism. No other types of connections 
                    // are accepted.
                    // Make check to see if this property has been set.
                    // if set, and if the client requested security mechanism 
                    // is not the same, then return a security check code 
                    // that the server does not support/allow this security 
                    // mechanism
                    if ( (server.getSecurityMechanism() != 
                        NetworkServerControlImpl.INVALID_OR_NOTSET_SECURITYMECHANISM)
                            && securityMechanism != server.getSecurityMechanism())
                    {
                        securityCheckCode = CodePoint.SECCHKCD_NOTSUPPORTED;
                        if (SanityManager.DEBUG) {
                            trace("parseACCSEC - SECCHKCD_NOTSUPPORTED [1] - " +
                                  securityMechanism + " <> " +
                                  server.getSecurityMechanism() + "\n");
                        }
                    }
                    else
                    {
                        // for plain text userid,password USRIDPWD, and USRIDONL
                        // no need of decryptionManager
                        if (securityMechanism != CodePoint.SECMEC_USRIDPWD &&
                                securityMechanism != CodePoint.SECMEC_USRIDONL)
                        {
                            // These are the only other mechanisms we understand
                            if (((securityMechanism != CodePoint.SECMEC_EUSRIDPWD) ||
                                 (securityMechanism == CodePoint.SECMEC_EUSRIDPWD && 
                                   !server.supportsEUSRIDPWD())
                                 ) &&
                                (securityMechanism !=
                                        CodePoint.SECMEC_USRSSBPWD))
                                //securityCheckCode = CodePoint.SECCHKCD_NOTSUPPORTED;
                    {
                        securityCheckCode = CodePoint.SECCHKCD_NOTSUPPORTED;
                        if (SanityManager.DEBUG) {
                            trace("parseACCSEC - SECCHKCD_NOTSUPPORTED [2]\n");
                        }
                    }
                            else
                            {
                                // We delay the initialization and required
                                // processing for SECMEC_USRSSBPWD as we need
                                // to ensure the database is booted so that
                                // we can verify that the current auth scheme
                                // is set to BUILT-IN or NONE. For this we need
                                // to have the RDBNAM codepoint available.
                                //
                                // See validateSecMecUSRSSBPWD() call below
                                if (securityMechanism ==
                                        CodePoint.SECMEC_USRSSBPWD) {
                                    break;
                                }

                                // SECMEC_EUSRIDPWD initialization
                                try {
                                    if (decryptionManager == null) {
                                        decryptionManager = new DecryptionManager();
                                    }
                                    myPublicKey = decryptionManager.obtainPublicKey();
                                } catch (SQLException e) {
                                    println2Log(null, session.drdaID, e.getMessage());
                                    // Local security service non-retryable error.
                                    securityCheckCode = CodePoint.SECCHKCD_0A;
                                }
                            }
                        }
                    }
                    break;
                //optional (currently required for Derby - needed for
                //          DERBY-528 as well)
                case CodePoint.RDBNAM:
                    String dbname = parseRDBNAM();
                    Database d = session.getDatabase(dbname);
                    if (d == null)
                    {
                        initializeDatabase(dbname);
                    }
                    else
                    {
                        // reset database for connection re-use 
                        // DERBY-3596
                        // If we are reusing resources for a new physical
                        // connection, reset the database object. If the client
                        // is in the process of creating a new logical
                        // connection only, don't reset the database object.
                        if (!deferredReset) {
                            d.reset();
                        }
                        database = d;
                    }
                    break;
                //optional - depending on security Mechanism 
                case CodePoint.SECTKN:
                    secTokenIn = reader.readBytes();
                    break;
                default:
                    invalidCodePoint(codePoint);
            }
            codePoint = reader.getCodePoint();
        }

        // check for required CodePoint's
        if (securityMechanism == 0) {
            missingCodePoint(CodePoint.SECMEC);
        }


        if (database == null) {
            initializeDatabase(null);
        }
        database.securityMechanism = securityMechanism;
        database.secTokenIn = secTokenIn;

        // If security mechanism is SECMEC_USRSSBPWD, then ensure it can be
        // used for the database or system based on the client's connection
        // URL and its identity.
        if (securityCheckCode == 0  &&
            (database.securityMechanism == CodePoint.SECMEC_USRSSBPWD))
        {
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT((securityCheckCode == 0),
                        "SECMEC_USRSSBPWD: securityCheckCode should not " +
                        "already be set, found it initialized with " +
                        "a value of '" + securityCheckCode + "'.");
            }
            securityCheckCode = validateSecMecUSRSSBPWD();
        }

        // need security token
        if (securityCheckCode == 0  && 
            (database.securityMechanism == CodePoint.SECMEC_EUSRIDPWD ||
            database.securityMechanism == CodePoint.SECMEC_USRSSBPWD) &&
            database.secTokenIn == null)
        {
            securityCheckCode = CodePoint.SECCHKCD_SECTKNMISSING_OR_INVALID;
        }

        // shouldn't have security token
        if (securityCheckCode == 0 &&
            (database.securityMechanism == CodePoint.SECMEC_USRIDPWD ||
            database.securityMechanism == CodePoint.SECMEC_USRIDONL)  &&
            database.secTokenIn != null)
        {
            securityCheckCode = CodePoint.SECCHKCD_SECTKNMISSING_OR_INVALID;
        }

        if (SanityManager.DEBUG) {
            trace("** ACCSECRD securityCheckCode is: " + securityCheckCode);
        }
        
        // If the security check was successful set the session state to
        // security accesseed.  Otherwise go back to attributes exchanged so we
        // require another ACCSEC
        session.setState(
            (securityCheckCode == 0) ? Session.SECACC : Session.ATTEXC);

        return securityCheckCode;
    }

    /**
     * Parse OPNQRY
     * Instance Variables
     *  RDBNAM - relational database name - optional
     *  PKGNAMCSN - RDB Package Name, Consistency Token and Section Number - required
     *  QRYBLKSZ - Query Block Size - required
     *  QRYBLKCTL - Query Block Protocol Control - optional 
     *  MAXBLKEXT - Maximum Number of Extra Blocks - optional - default value 0
     *  OUTOVROPT - Output Override Option
     *  QRYROWSET - Query Rowset Size - optional - level 7
     *  MONITOR - Monitor events - optional.
     *
     * @return RDB Package Name, Consistency Token, and Section Number
     * @exception DRDAProtocolException
     */
    private Pkgnamcsn parseOPNQRY() throws DRDAProtocolException, SQLException
    {
        Pkgnamcsn pkgnamcsn = null;
        boolean gotQryblksz = false;
        int blksize = 0;
        int qryblkctl = CodePoint.QRYBLKCTL_DEFAULT;
        int maxblkext = CodePoint.MAXBLKEXT_DEFAULT;
        int qryrowset = CodePoint.QRYROWSET_DEFAULT;
        int qryclsimp = DRDAResultSet.QRYCLSIMP_DEFAULT;
        int outovropt = CodePoint.OUTOVRFRS;
        reader.markCollection();
        int codePoint = reader.getCodePoint();
        while (codePoint != -1)
        {
            switch(codePoint)
            {
                //optional
                case CodePoint.RDBNAM:
                    setDatabase(CodePoint.OPNQRY);
                    break;
                //required
                case CodePoint.PKGNAMCSN:
                    pkgnamcsn = parsePKGNAMCSN();
                    break;
                //required
                case CodePoint.QRYBLKSZ:
                    blksize = parseQRYBLKSZ();
                    gotQryblksz = true;
                    break;
                //optional
                case CodePoint.QRYBLKCTL:
                    qryblkctl = reader.readNetworkShort();
                    //The only type of query block control we can specify here
                    //is forced fixed row
                    if (qryblkctl != CodePoint.FRCFIXROW) {
                        invalidCodePoint(qryblkctl);
                    }
                    if (SanityManager.DEBUG) {
                        trace("!!qryblkctl = "+Integer.toHexString(qryblkctl));
                    }
                    gotQryblksz = true;
                    break;
                //optional
                case CodePoint.MAXBLKEXT:
                    maxblkext = reader.readSignedNetworkShort();
                    if (SanityManager.DEBUG) {
                        trace("maxblkext = "+maxblkext);
                    }
                    break;
                // optional
                case CodePoint.OUTOVROPT:
                    outovropt = parseOUTOVROPT();
                    break;
                //optional
                case CodePoint.QRYROWSET:
                    //Note minimum for OPNQRY is 0
                    qryrowset = parseQRYROWSET(0);
                    break;
                case CodePoint.QRYCLSIMP:
                    // Implicitly close non-scrollable cursor
                    qryclsimp = parseQRYCLSIMP();
                    break;
                case CodePoint.QRYCLSRLS:
                    // Ignore release of read locks.  Nothing we can do here
                    parseQRYCLSRLS();
                    break;
                // optional
                case CodePoint.MONITOR:
                    parseMONITOR();
                    break;
                default:
                    invalidCodePoint(codePoint);
            }
            codePoint = reader.getCodePoint();
        }
        // check for required variables
        if (pkgnamcsn == null) {
            missingCodePoint(CodePoint.PKGNAMCSN);
        }
        if (!gotQryblksz) {
            missingCodePoint(CodePoint.QRYBLKSZ);
        }

        // get the statement we are opening
        DRDAStatement stmt = database.getDRDAStatement(pkgnamcsn);
        if (stmt == null)
        {
            //XXX should really throw a SQL Exception here
            invalidValue(CodePoint.PKGNAMCSN);
        }

        // check that this statement is not already open
        // commenting this check out for now
        // it turns out that JCC doesn't send a close if executeQuery is
        // done again without closing the previous result set
        // this check can't be done since the second executeQuery should work
        //if (stmt.state != DRDAStatement.NOT_OPENED)
        //{
        //  writeQRYPOPRM();
        //  pkgnamcsn = null;
        //}
        //else
        //{
        stmt.setOPNQRYOptions(blksize,qryblkctl,maxblkext,outovropt,
                              qryrowset, qryclsimp);
        //}
        
        // read the command objects
        // for ps with parameter
        if (reader.isChainedWithSameID())
        {
            if (SanityManager.DEBUG) {
                trace("&&&&&& parsing SQLDTA");
            }
            parseOPNQRYobjects(stmt);
        }
        return pkgnamcsn;
    }
    /**
     * Parse OPNQRY objects
     * Objects
     *  TYPDEFNAM - Data type definition name - optional
     *  TYPDEFOVR - Type defintion overrides - optional
     *  SQLDTA- SQL Program Variable Data - optional
     *
     * If TYPDEFNAM and TYPDEFOVR are supplied, they apply to the objects
     * sent with the statement.  Once the statement is over, the default values
     * sent in the ACCRDB are once again in effect.  If no values are supplied,
     * the values sent in the ACCRDB are used.
     * Objects may follow in one DSS or in several DSS chained together.
     * 
     * @throws DRDAProtocolException
     * @throws SQLException
     */
    private void parseOPNQRYobjects(DRDAStatement stmt) 
        throws DRDAProtocolException, SQLException
    {
        int codePoint;
        do
        {
            correlationID = reader.readDssHeader();
            while (reader.moreDssData())
            {
                codePoint = reader.readLengthAndCodePoint( false );
                switch(codePoint)
                {
                    // optional
                    case CodePoint.TYPDEFNAM:
                        setStmtOrDbByteOrder(false, stmt, parseTYPDEFNAM());
                        break;
                    // optional
                    case CodePoint.TYPDEFOVR:
                        parseTYPDEFOVR(stmt);
                        break;
                    // optional 
                    case CodePoint.SQLDTA:
                        parseSQLDTA(stmt);
                        break;
                    // optional
                    case CodePoint.EXTDTA:
                        readAndSetAllExtParams(stmt, false);
                        break;
                    default:
                        invalidCodePoint(codePoint);
                }
            }
        } while (reader.isChainedWithSameID());

    }
    /**
     * Parse OUTOVROPT - this indicates whether output description can be
     * overridden on just the first CNTQRY or on any CNTQRY
     *
     * @return output override option
     * @exception DRDAProtocolException
     */
    private int parseOUTOVROPT() throws DRDAProtocolException
    {
        checkLength(CodePoint.OUTOVROPT, 1);
        int outovropt = reader.readUnsignedByte();
        if (SanityManager.DEBUG) {
            trace("output override option: "+outovropt);
        }
        if (outovropt != CodePoint.OUTOVRFRS &&
                outovropt != CodePoint.OUTOVRANY) {
            invalidValue(CodePoint.OUTOVROPT);
        }
        return outovropt;
    }

    /**
     * Parse QRYBLSZ - this gives the maximum size of the query blocks that
     * can be returned to the requester
     *
     * @return query block size
     * @exception DRDAProtocolException
     */
    private int parseQRYBLKSZ() throws DRDAProtocolException
    {
        checkLength(CodePoint.QRYBLKSZ, 4);
        int blksize = reader.readNetworkInt();
        if (SanityManager.DEBUG) {
            trace("qryblksz = "+blksize);
        }
        if (blksize < CodePoint.QRYBLKSZ_MIN ||
                blksize > CodePoint.QRYBLKSZ_MAX) {
            invalidValue(CodePoint.QRYBLKSZ);
        }
        return blksize;
    }
    /**
     * Parse QRYROWSET - this is the number of rows to return
     *
     * @param minVal - minimum value
     * @return query row set size
     * @exception DRDAProtocolException
     */
    private int parseQRYROWSET(int minVal) throws DRDAProtocolException
    {
        checkLength(CodePoint.QRYROWSET, 4);
        int qryrowset = reader.readNetworkInt();
        if (SanityManager.DEBUG) {
            trace("qryrowset = " + qryrowset);
        }
        if (qryrowset < minVal || qryrowset > CodePoint.QRYROWSET_MAX) {
            invalidValue(CodePoint.QRYROWSET);
        }
        return qryrowset;
    }

    /** Parse a QRYCLSIMP - Implicitly close non-scrollable cursor 
     * after end of data.
     * @return  true to close on end of data 
     */
    private int  parseQRYCLSIMP() throws DRDAProtocolException
    {
       
        checkLength(CodePoint.QRYCLSIMP, 1);
        int qryclsimp = reader.readUnsignedByte();
        if (SanityManager.DEBUG) {
            trace ("qryclsimp = " + qryclsimp);
        }
        if (qryclsimp != CodePoint.QRYCLSIMP_SERVER_CHOICE &&
            qryclsimp != CodePoint.QRYCLSIMP_YES &&
            qryclsimp != CodePoint.QRYCLSIMP_NO )
        {
            invalidValue(CodePoint.QRYCLSIMP);
        }
        return qryclsimp;
    }


    private int parseQRYCLSRLS() throws DRDAProtocolException
    {
        reader.skipBytes();
        return 0;
    }

    /**
     * Write a QRYPOPRM - Query Previously opened
     * Instance Variables
     *  SVRCOD - Severity Code - required - 8 ERROR
     *  RDBNAM - Relational Database Name - required
     *  PKGNAMCSN - RDB Package Name, Consistency Token, and Section Number - required
     * 
     * @exception DRDAProtocolException
     */
    private void writeQRYPOPRM() throws DRDAProtocolException
    {
        writer.createDssReply();
        writer.startDdm(CodePoint.QRYPOPRM);
        writer.writeScalar2Bytes(CodePoint.SVRCOD, CodePoint.SVRCOD_ERROR);
        writeRDBNAM(database.getDatabaseName());
        writePKGNAMCSN();
        writer.endDdmAndDss();
    }
    /**
     * Write a QRYNOPRM - Query Not Opened
     * Instance Variables
     *  SVRCOD - Severity Code - required -  4 Warning 8 ERROR
     *  RDBNAM - Relational Database Name - required
     *  PKGNAMCSN - RDB Package Name, Consistency Token, and Section Number - required
     * 
     * @param svrCod    Severity Code
     * @exception DRDAProtocolException
     */
    private void writeQRYNOPRM(int svrCod) throws DRDAProtocolException
    {
        writer.createDssReply();
        writer.startDdm(CodePoint.QRYNOPRM);
        writer.writeScalar2Bytes(CodePoint.SVRCOD, svrCod);
        writeRDBNAM(database.getDatabaseName());
        writePKGNAMCSN();
        writer.endDdmAndDss();
    }
    /**
     * Write a OPNQFLRM - Open Query Failure
     * Instance Variables
     *  SVRCOD - Severity Code - required - 8 ERROR
     *  RDBNAM - Relational Database Name - required
     *
     * @param   e   Exception describing failure
     *
     * @exception DRDAProtocolException
     */
    private void writeOPNQFLRM(SQLException e) throws DRDAProtocolException
    {
        writer.createDssReply();
        writer.startDdm(CodePoint.OPNQFLRM);
        writer.writeScalar2Bytes(CodePoint.SVRCOD, CodePoint.SVRCOD_ERROR);
        writeRDBNAM(database.getDatabaseName());
        writer.endDdm();
        writer.startDdm(CodePoint.SQLCARD);
        writeSQLCAGRP(e, 0, 0);
        writer.endDdmAndDss();
    }
    /**
     * Write PKGNAMCSN
     * Instance Variables
     *   NAMESYMDR - database name - not validated
     *   RDBCOLID - RDB Collection Identifier
     *   PKGID - RDB Package Identifier
     *   PKGCNSTKN - RDB Package Consistency Token
     *   PKGSN - RDB Package Section Number
     *
     * There are two possible formats, fixed and extended which includes length
     * information for the strings
     *
     * @throws DRDAProtocolException
     */
    private void writePKGNAMCSN(byte[] pkgcnstkn) throws DRDAProtocolException
    {
        writer.startDdm(CodePoint.PKGNAMCSN);
        if (rdbnam.length() <= CodePoint.RDBNAM_LEN &&
            rdbcolid.length() <= CodePoint.RDBCOLID_LEN &&
            pkgid.length() <= CodePoint.PKGID_LEN)
        {   // if none of RDBNAM, RDBCOLID and PKGID have a length of
            // more than 18, use fixed format
            writer.writeScalarPaddedString(rdbnam, CodePoint.RDBNAM_LEN);
            writer.writeScalarPaddedString(rdbcolid, CodePoint.RDBCOLID_LEN);
            writer.writeScalarPaddedString(pkgid, CodePoint.PKGID_LEN);
            writer.writeScalarPaddedBytes(pkgcnstkn,
                                          CodePoint.PKGCNSTKN_LEN, (byte) 0);
            writer.writeShort(pkgsn);
        }
        else    // extended format
        {
            int len = Math.max(CodePoint.RDBNAM_LEN, rdbnam.length());
            writer.writeShort(len);
            writer.writeScalarPaddedString(rdbnam, len);
            len = Math.max(CodePoint.RDBCOLID_LEN, rdbcolid.length());
            writer.writeShort(len);
            writer.writeScalarPaddedString(rdbcolid, len);
            len = Math.max(CodePoint.PKGID_LEN, pkgid.length());
            writer.writeShort(len);
            writer.writeScalarPaddedString(pkgid, len);
            writer.writeScalarPaddedBytes(pkgcnstkn,
                                          CodePoint.PKGCNSTKN_LEN, (byte) 0);
            writer.writeShort(pkgsn);
        }
        writer.endDdm();
    }

    private void writePKGNAMCSN() throws DRDAProtocolException
    {
        writePKGNAMCSN(pkgcnstkn.getBytes());
    }

    /**
     * Parse CNTQRY - Continue Query
     * Instance Variables
     *   RDBNAM - Relational Database Name - optional
     *   PKGNAMCSN - RDB Package Name, Consistency Token, and Section Number - required
     *   QRYBLKSZ - Query Block Size - required
     *   QRYRELSCR - Query Relative Scrolling Action - optional
     *   QRYSCRORN - Query Scroll Orientation - optional - level 7
     *   QRYROWNBR - Query Row Number - optional
     *   QRYROWSNS - Query Row Sensitivity - optional - level 7
     *   QRYBLKRST - Query Block Reset - optional - level 7
     *   QRYRTNDTA - Query Returns Data - optional - level 7
     *   QRYROWSET - Query Rowset Size - optional - level 7
     *   QRYRFRTBL - Query Refresh Answer Set Table - optional
     *   NBRROW - Number of Fetch or Insert Rows - optional
     *   MAXBLKEXT - Maximum number of extra blocks - optional
     *   RTNEXTDTA - Return of EXTDTA Option - optional
     *   MONITOR - Monitor events - optional.
     *
     * @return DRDAStatement we are continuing
     * @throws DRDAProtocolException
     * @throws SQLException
     */
    private DRDAStatement parseCNTQRY() throws DRDAProtocolException, SQLException
    {
        byte val;
        Pkgnamcsn pkgnamcsn = null;
        boolean gotQryblksz = false;
        boolean qryrelscr = true;
        long qryrownbr = 1;
        boolean qryrfrtbl = false;
        int nbrrow = 1;
        int blksize = 0;
        int maxblkext = -1;
        long qryinsid;
        boolean gotQryinsid = false;
        int qryscrorn = CodePoint.QRYSCRREL;
        boolean qryrowsns = false;
        boolean gotQryrowsns = false;
        boolean qryblkrst = false;
        boolean qryrtndta = true;
        int qryrowset = CodePoint.QRYROWSET_DEFAULT;
        int rtnextdta = CodePoint.RTNEXTROW;
        reader.markCollection();
        int codePoint = reader.getCodePoint();
        while (codePoint != -1)
        {
            switch(codePoint)
            {
                //optional
                case CodePoint.RDBNAM:
                    setDatabase(CodePoint.CNTQRY);
                    break;
                //required
                case CodePoint.PKGNAMCSN:
                    pkgnamcsn = parsePKGNAMCSN();
                    break;
                //required
                case CodePoint.QRYBLKSZ:
                    blksize = parseQRYBLKSZ();
                    gotQryblksz = true;
                    break;
                //optional
                case CodePoint.QRYRELSCR:
                    qryrelscr = readBoolean(CodePoint.QRYRELSCR);
                    if (SanityManager.DEBUG) {
                        trace("qryrelscr = "+qryrelscr);
                    }
                    break;
                //optional
                case CodePoint.QRYSCRORN:
                    checkLength(CodePoint.QRYSCRORN, 1);
                    qryscrorn = reader.readUnsignedByte();
                    if (SanityManager.DEBUG) {
                        trace("qryscrorn = "+qryscrorn);
                    }
                    switch (qryscrorn)
                    {
                        case CodePoint.QRYSCRREL:
                        case CodePoint.QRYSCRABS:
                        case CodePoint.QRYSCRAFT:
                        case CodePoint.QRYSCRBEF:
                            break;
                        default:
                            invalidValue(CodePoint.QRYSCRORN);
                    }
                    break;
                //optional
                case CodePoint.QRYROWNBR:
                    checkLength(CodePoint.QRYROWNBR, 8);
                    qryrownbr = reader.readNetworkLong();
                    if (SanityManager.DEBUG) {
                        trace("qryrownbr = "+qryrownbr);
                    }
                    break;
                //optional
                case CodePoint.QRYROWSNS:
                    checkLength(CodePoint.QRYROWSNS, 1);
                    qryrowsns = readBoolean(CodePoint.QRYROWSNS);
                    if (SanityManager.DEBUG) {
                        trace("qryrowsns = "+qryrowsns);
                    }
                    gotQryrowsns = true;
                    break;
                //optional
                case CodePoint.QRYBLKRST:
                    checkLength(CodePoint.QRYBLKRST, 1);
                    qryblkrst = readBoolean(CodePoint.QRYBLKRST);
                    if (SanityManager.DEBUG) {
                        trace("qryblkrst = "+qryblkrst);
                    }
                    break;
                //optional
                case CodePoint.QRYRTNDTA:
                    qryrtndta = readBoolean(CodePoint.QRYRTNDTA);
                    if (SanityManager.DEBUG) {
                        trace("qryrtndta = "+qryrtndta);
                    }
                    break;
                //optional
                case CodePoint.QRYROWSET:
                    //Note minimum for CNTQRY is 1
                    qryrowset = parseQRYROWSET(1);
                    if (SanityManager.DEBUG) {
                        trace("qryrowset = "+qryrowset);
                    }
                    break;
                //optional
                case CodePoint.QRYRFRTBL:
                    qryrfrtbl = readBoolean(CodePoint.QRYRFRTBL);
                    if (SanityManager.DEBUG) {
                        trace("qryrfrtbl = "+qryrfrtbl);
                    }
                    break;
                //optional
                case CodePoint.NBRROW:
                    checkLength(CodePoint.NBRROW, 4);
                    nbrrow = reader.readNetworkInt();
                    if (SanityManager.DEBUG) {
                        trace("nbrrow = "+nbrrow);
                    }
                    break;
                //optional
                case CodePoint.MAXBLKEXT:
                    checkLength(CodePoint.MAXBLKEXT, 2);
                    maxblkext = reader.readSignedNetworkShort();
                    if (SanityManager.DEBUG) {
                        trace("maxblkext = "+maxblkext);
                    }
                    break;
                //optional
                case CodePoint.RTNEXTDTA:
                    checkLength(CodePoint.RTNEXTDTA, 1);
                    rtnextdta = reader.readUnsignedByte();
                    if (rtnextdta != CodePoint.RTNEXTROW && 
                            rtnextdta != CodePoint.RTNEXTALL) {
                        invalidValue(CodePoint.RTNEXTDTA);
                    }
                    if (SanityManager.DEBUG) {
                        trace("rtnextdta = "+rtnextdta);
                    }
                    break;
                // required for SQLAM >= 7
                case CodePoint.QRYINSID:
                    checkLength(CodePoint.QRYINSID, 8);
                    qryinsid = reader.readNetworkLong();
                    gotQryinsid = true;
                    if (SanityManager.DEBUG) {
                        trace("qryinsid = "+qryinsid);
                    }
                    break;
                // optional
                case CodePoint.MONITOR:
                    parseMONITOR();
                    break;
                default:
                    invalidCodePoint(codePoint);
            }
            codePoint = reader.getCodePoint();
        }
        // check for required variables
        if (pkgnamcsn == null) {
            missingCodePoint(CodePoint.PKGNAMCSN);
        }
        if (!gotQryblksz) {
            missingCodePoint(CodePoint.QRYBLKSZ);
        }
        if (sqlamLevel >= MGRLVL_7 && !gotQryinsid) {
            missingCodePoint(CodePoint.QRYINSID);
        }

        // get the statement we are continuing
        DRDAStatement stmt = database.getDRDAStatement(pkgnamcsn);
        if (stmt == null)
        {
            //XXX should really throw a SQL Exception here
            invalidValue(CodePoint.CNTQRY);
        }

        if (stmt.rsIsClosed())
        {
            writeQRYNOPRM(CodePoint.SVRCOD_ERROR);
            skipRemainder(true);
            return null;
        }
        stmt.setQueryOptions(blksize,qryrelscr,qryrownbr,qryrfrtbl,nbrrow,maxblkext,
                         qryscrorn,qryrowsns,qryblkrst,qryrtndta,qryrowset,
                         rtnextdta);

        if (reader.isChainedWithSameID()) {
            parseCNTQRYobjects(stmt);
        }

        return stmt;
    }
    /**
     * Skip remainder of current DSS and all chained DSS'es
     *
     * @param onlySkipSameIds True if we _only_ want to skip DSS'es
     *   that are chained with the SAME id as the current DSS.
     *   False means skip ALL chained DSSes, whether they're
     *   chained with same or different ids.
     * @exception DRDAProtocolException
     */
    private void skipRemainder(boolean onlySkipSameIds) throws DRDAProtocolException
    {
        reader.skipDss();
        while (reader.isChainedWithSameID() ||
            (!onlySkipSameIds && reader.isChainedWithDiffID()))
        {
            reader.readDssHeader();
            reader.skipDss();
        }
    }
    /**
     * Parse CNTQRY objects
     * Instance Variables
     *   OUTOVR - Output Override Descriptor - optional
     *
     * @param stmt DRDA statement we are working on
     * @exception DRDAProtocolException
     */
    private void parseCNTQRYobjects(DRDAStatement stmt) throws DRDAProtocolException, SQLException
    {
        int codePoint;
        do
        {
            correlationID = reader.readDssHeader();
            while (reader.moreDssData())
            {
                codePoint = reader.readLengthAndCodePoint( false );
                switch(codePoint)
                {
                    // optional
                    case CodePoint.OUTOVR:
                        parseOUTOVR(stmt);
                        break;
                    default:
                        invalidCodePoint(codePoint);
                }
            }
        } while (reader.isChainedWithSameID());

    }
    /**
     * Parse OUTOVR - Output Override Descriptor
     * This specifies the output format for data to be returned as output to a SQL
     * statement or as output from a query.
     *
     * @param stmt  DRDA statement this applies to
     * @exception DRDAProtocolException
     */
    private void parseOUTOVR(DRDAStatement stmt) throws DRDAProtocolException, SQLException
    {
        boolean first = true;
        int numVars;
        int dtaGrpLen;
        int tripType;
        int tripId;
        int precision;
        int start = 0;
        while (true)
        {
            dtaGrpLen = reader.readUnsignedByte();
            tripType = reader.readUnsignedByte();
            tripId = reader.readUnsignedByte();
            // check if we have reached the end of the data
            if (tripType == FdocaConstants.RLO_TRIPLET_TYPE)
            {
                //read last part of footer
                reader.skipBytes();
                break;
            }
            numVars = (dtaGrpLen - 3) / 3;
            if (SanityManager.DEBUG) {
                trace("num of vars is: "+numVars);
            }
            int[] outovr_drdaType;
            if (first)
            {
                outovr_drdaType = new int[numVars];
                first = false;
            }
            else
            {
                int[] oldoutovr_drdaType = stmt.getOutovr_drdaType();
                int oldlen = oldoutovr_drdaType.length;
                // create new array and copy over already read stuff
                outovr_drdaType = new int[oldlen + numVars];
                System.arraycopy(oldoutovr_drdaType, 0,
                                 outovr_drdaType,0,
                                 oldlen);
                start = oldlen;
            }
            for (int i = start; i < numVars + start; i++)
            {
                int drdaType = reader.readUnsignedByte();
                if (!database.supportsLocator()) { 
                    // ignore requests for locator when it is not supported
                    if ((drdaType >= DRDAConstants.DRDA_TYPE_LOBLOC)
                        && (drdaType <= DRDAConstants.DRDA_TYPE_NCLOBLOC)) {
                        if (SanityManager.DEBUG) {
                            trace("ignoring drdaType: " + drdaType);
                        }
                        reader.readNetworkShort(); // Skip rest
                        continue;
                    }
                }
                outovr_drdaType[i] = drdaType;
                if (SanityManager.DEBUG) {
                    trace("drdaType is: "+ outovr_drdaType[i]);
                }
                precision = reader.readNetworkShort();
                if (SanityManager.DEBUG) {
                    trace("drdaLength is: "+precision);
                }
                outovr_drdaType[i] |= (precision << 8);
            }
            stmt.setOutovr_drdaType(outovr_drdaType);
        }
    }

    /**
     * Piggy-back any modified session attributes on the current message. Writes
     * a PBSD conataining one or both of PBSD_ISO and PBSD_SCHEMA. PBSD_ISO is
     * followed by the jdbc isolation level as an unsigned byte. PBSD_SCHEMA is
     * followed by the name of the current schema as an UTF-8 String.
     * @throws java.sql.SQLException
     * @throws org.apache.derby.impl.drda.DRDAProtocolException
     */
    private void writePBSD() throws SQLException, DRDAProtocolException
    {
        if (!appRequester.supportsSessionDataCaching()) {
            return;
        }
        PiggyBackedSessionData pbsd = database.getPiggyBackedSessionData(true);
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(pbsd != null, "pbsd is not expected to be null");
        }
        // DERBY-3596
        // Reset the flag. In sane builds it is used to avoid an assert, but
        // we want to reset it as soon as possible to avoid masking real bugs.
        // We have to do this because we are changing the connection state
        // at an unexpected time (deferred reset, see parseSECCHK). This was
        // done to avoid having to change the client code.
        this.deferredReset = false;
        pbsd.refresh();
        if (pbsd.isModified()) {
            writer.createDssReply();
            writer.startDdm(CodePoint.PBSD);

            if (pbsd.isIsoModified()) {
                writer.writeScalar1Byte(CodePoint.PBSD_ISO, pbsd.getIso());
            }

            if (pbsd.isSchemaModified()) {
                writer.startDdm(CodePoint.PBSD_SCHEMA);
                writer.writeString(pbsd.getSchema());
                writer.endDdm();
            }
            writer.endDdmAndDss();
        }
        pbsd.setUnmodified();
        if (SanityManager.DEBUG) {
            PiggyBackedSessionData pbsdNew =
                database.getPiggyBackedSessionData(true);
            SanityManager.ASSERT(pbsdNew == pbsd,
                                 "pbsdNew and pbsd are expected to reference " +
                                 "the same object");
            pbsd.refresh();
            SanityManager.ASSERT
                (!pbsd.isModified(),
                 "pbsd=("+pbsd+") is not expected to be modified");
        }
    }

    /**
     * Write OPNQRYRM - Open Query Complete
     * Instance Variables
     *   SVRCOD - Severity Code - required
     *   QRYPRCTYP - Query Protocol Type - required
     *   SQLCSRHLD - Hold Cursor Position - optional
     *   QRYATTSCR - Query Attribute for Scrollability - optional - level 7
     *   QRYATTSNS - Query Attribute for Sensitivity - optional - level 7
     *   QRYATTUPD - Query Attribute for Updatability -optional - level 7
     *   QRYINSID - Query Instance Identifier - required - level 7
     *   SRVDGN - Server Diagnostic Information - optional
     *
     * @param isDssObject - return as a DSS object (part of a reply) 
     * @param stmt - DRDA statement we are processing
     *
     * @exception DRDAProtocolException
     */
    private void writeOPNQRYRM(boolean isDssObject, DRDAStatement stmt) 
        throws DRDAProtocolException, SQLException
    {
        if (SanityManager.DEBUG) {
            trace("WriteOPNQRYRM");
        }

        if (isDssObject) {
            writer.createDssObject();
        } else {
            writer.createDssReply();
        }
        writer.startDdm(CodePoint.OPNQRYRM);
        writer.writeScalar2Bytes(CodePoint.SVRCOD,CodePoint.SVRCOD_INFO);

        // There is currently a problem specifying LMTBLKPRC for LOBs with JCC
        // JCC will throw an ArrayOutOfBounds exception.  Once this is fixed, we
        // don't need to pass the two arguments for getQryprctyp.
        int prcType = stmt.getQryprctyp();
        if (SanityManager.DEBUG) {
            trace("sending QRYPRCTYP: " + prcType);
        }
        writer.writeScalar2Bytes(CodePoint.QRYPRCTYP, prcType);

        //pass the SQLCSRHLD codepoint only if statement producing the ResultSet has 
        //hold cursors over commit set. In case of stored procedures which use server-side
        //JDBC, the holdability of the ResultSet will be the holdability of the statement 
        //in the stored procedure, not the holdability of the calling statement.
        if (stmt.getCurrentDrdaResultSet().withHoldCursor == ResultSet.HOLD_CURSORS_OVER_COMMIT)
        {
            writer.writeScalar1Byte(CodePoint.SQLCSRHLD, CodePoint.TRUE);
        }
        if (sqlamLevel >= MGRLVL_7)
        {
            writer.writeScalarHeader(CodePoint.QRYINSID, 8);
            //This is implementer defined.  DB2 uses this for the nesting level
            //of the query.  A query from an application would be nesting level 0,
            //from a stored procedure, nesting level 1, from a recursive call of
            //a stored procedure, nesting level 2, etc.
            writer.writeInt(0);     
            //This is a unique sequence number per session
            writer.writeInt(session.qryinsid++);
            //Write the scroll attributes if they are set
            if (stmt.isScrollable())
            {
                writer.writeScalar1Byte(CodePoint.QRYATTSCR, CodePoint.TRUE);
                if ((stmt.getConcurType() == ResultSet.CONCUR_UPDATABLE) &&
                        (stmt.getResultSet().getType() == 
                         ResultSet.TYPE_SCROLL_INSENSITIVE)) {
                    writer.writeScalar1Byte(CodePoint.QRYATTSNS, 
                                            CodePoint.QRYSNSSTC);
                } else {
                    writer.writeScalar1Byte(CodePoint.QRYATTSNS, 
                                            CodePoint.QRYINS);
                }
            }
            if (stmt.getConcurType() == ResultSet.CONCUR_UPDATABLE) {
                if (stmt.getResultSet() != null) { 
                    // Resultset concurrency can be less than statement
                    // concurreny if the underlying language resultset
                    // is not updatable.
                    if (stmt.getResultSet().getConcurrency() == 
                        ResultSet.CONCUR_UPDATABLE) {
                        writer.writeScalar1Byte(CodePoint.QRYATTUPD, 
                                                CodePoint.QRYUPD);
                    } else {
                        writer.writeScalar1Byte(CodePoint.QRYATTUPD, 
                                                CodePoint.QRYRDO);
                    }
                } else {
                    writer.writeScalar1Byte(CodePoint.QRYATTUPD, 
                                            CodePoint.QRYUPD);
                }
            } else {
                writer.writeScalar1Byte(CodePoint.QRYATTUPD, CodePoint.QRYRDO);
            }
        }
        writer.endDdmAndDss ();
    }
    /**
     * Write ENDQRYRM - query process has terminated in such a manner that the
     *  query or result set is now closed.  It cannot be resumed with the CNTQRY
     *  command or closed with the CLSQRY command
     * @param svrCod  Severity code - WARNING or ERROR
     * @exception DRDAProtocolException
     */
    private void writeENDQRYRM(int svrCod) throws DRDAProtocolException
    {
        writer.createDssReply();
        writer.startDdm(CodePoint.ENDQRYRM);
        writer.writeScalar2Bytes(CodePoint.SVRCOD,svrCod);
        writer.endDdmAndDss();
    }
/**
     * Write ABNUOWRM - query process has terminated in an error condition
     * such as deadlock or lock timeout.
     * Severity code is always error
     *   * @exception DRDAProtocolException
     */
    private void writeABNUOWRM() throws DRDAProtocolException
    {
        writer.createDssReply();
        writer.startDdm(CodePoint.ABNUOWRM);
        writer.writeScalar2Bytes(CodePoint.SVRCOD,CodePoint.SVRCOD_ERROR);
        writeRDBNAM(database.getDatabaseName());
        writer.endDdmAndDss();
    }
    /**
     * Parse database name
     *
     * @return database name
     *
     * @exception DRDAProtocolException
     */
    private String parseRDBNAM() throws DRDAProtocolException
    {
        String name;
        byte [] rdbName = reader.readBytes();
        if (rdbName.length == 0)
        {
            // throw RDBNFNRM
            rdbNotFound(null);
        }
        //SQLAM level 7 allows db name up to 255, level 6 fixed len 18
        // but Derby 10.11 allows 1024
        if (rdbName.length < CodePoint.RDBNAM_LEN ||
                rdbName.length > CodePoint.RDBNAM_MAX_NAME) {
            badObjectLength(CodePoint.RDBNAM);
        }
        name = reader.convertBytes(rdbName);
        // trim trailing blanks from the database name
        name = name.trim();
        if (SanityManager.DEBUG) {
            trace("RdbName " + name);
        }
        return name;
    }

    /**
     * Write ACCSECRD
     * If the security mechanism is known, we just send it back along with
     * the security token if encryption is going to be used.
     * If the security mechanism is not known, we send a list of the ones
     * we know.
     * Instance Variables
     *  SECMEC - security mechanism - required
     *  SECTKN - security token - optional (required if security mechanism
     *                      uses encryption)
     *  SECCHKCD - security check code - error occurred in processing ACCSEC
     *
     * @param securityCheckCode
     * 
     * @exception DRDAProtocolException
     */
    private void writeACCSECRD(int securityCheckCode)
        throws DRDAProtocolException
    {
        writer.createDssReply();
        writer.startDdm(CodePoint.ACCSECRD);

        if (securityCheckCode != CodePoint.SECCHKCD_NOTSUPPORTED)
        {
            writer.writeScalar2Bytes(CodePoint.SECMEC, database.securityMechanism);
        }
        else
        { 
            // if server doesnt recognize or allow the client requested security mechanism,
            // then need to return the list of security mechanisms supported/allowed by the server
            
            // check if server is set to accept connections from client at a certain 
            // security mechanism, if so send only the security mechanism  that the 
            // server will accept, to the client
            if ( server.getSecurityMechanism() != NetworkServerControlImpl.INVALID_OR_NOTSET_SECURITYMECHANISM )
            {
                writer.writeScalar2Bytes(CodePoint.SECMEC, server.getSecurityMechanism());
            }
            else
            {
                // note: per the DDM manual , ACCSECRD response is of 
                // form SECMEC (value{value..})  
                // Need to fix the below to send a list of supported security 
                // mechanisms for value of one SECMEC codepoint (JIRA 926)
                // these are the ones we know about
                writer.writeScalar2Bytes(CodePoint.SECMEC, CodePoint.SECMEC_USRIDPWD);
                // include EUSRIDPWD in the list of supported secmec only if 
                // server can truely support it in the jvm that is running in
                if ( server.supportsEUSRIDPWD()) {
                    writer.writeScalar2Bytes(CodePoint.SECMEC, CodePoint.SECMEC_EUSRIDPWD);
                }
                writer.writeScalar2Bytes(CodePoint.SECMEC, CodePoint.SECMEC_USRIDONL);
                writer.writeScalar2Bytes(CodePoint.SECMEC, CodePoint.SECMEC_USRSSBPWD);
            }
        }

        if (securityCheckCode != 0)
        {
            writer.writeScalar1Byte(CodePoint.SECCHKCD, securityCheckCode);
        }
        else
        {
            // we need to send back the key if encryption is being used
            if (database.securityMechanism == CodePoint.SECMEC_EUSRIDPWD)
            {
                writer.writeScalarBytes(CodePoint.SECTKN, myPublicKey);
            }
            else if (database.securityMechanism == CodePoint.SECMEC_USRSSBPWD)
            {
                writer.writeScalarBytes(CodePoint.SECTKN, myTargetSeed);
            }
        }
        writer.endDdmAndDss ();

        if (securityCheckCode != 0) {
        // then we have an error and so can ignore the rest of the
        // DSS request chain.
            skipRemainder(false);
        }

        finalizeChain();
    }

    /**
     * Parse security check
     * Instance Variables
     *  SECMGRNM - security manager name - optional, ignorable
     *  SECMEC  - security mechanism - required
     *  SECTKN  - security token - optional, (required if encryption used)
     *  PASSWORD - password - optional, (required if security mechanism uses it)
     *  NEWPASSWORD - new password - optional, (required if sec mech. uses it)
     *  USRID   - user id - optional, (required if sec mec. uses it)
     *  RDBNAM  - database name - optional (required if databases can have own sec.)
     *
     * 
     * @return security check code
     * @exception DRDAProtocolException
     */
    private int parseSECCHK() throws DRDAProtocolException
    {
        int codePoint, securityCheckCode = 0;
        int securityMechanism = 0;
        databaseAccessException = null;
        reader.markCollection();
        codePoint = reader.getCodePoint();
        if (this.deferredReset) {
            // Skip the SECCHK, but assure a minimal degree of correctness.
            while (codePoint != -1) {
                switch (codePoint) {
                    // Note the fall-through.
                    // Minimal level of checking to detect protocol errors.
                    // NOTE: SECMGR level 8 code points are not handled.
                    case CodePoint.SECMGRNM:
                    case CodePoint.SECMEC:
                    case CodePoint.SECTKN:
                    case CodePoint.PASSWORD:
                    case CodePoint.NEWPASSWORD:
                    case CodePoint.USRID:
                    case CodePoint.RDBNAM:
                        reader.skipBytes();
                        break;
                    default:
                        invalidCodePoint(codePoint);
                }
                codePoint = reader.getCodePoint();
            }
        } else {
        while (codePoint != -1)
        {
            switch (codePoint)
            {
                //optional, ignorable
                case CodePoint.SECMGRNM:
                    reader.skipBytes();
                    break;
                //required
                case CodePoint.SECMEC:
                    checkLength(CodePoint.SECMEC, 2);
                    securityMechanism = reader.readNetworkShort();
                    if (SanityManager.DEBUG) {
                        trace("parseSECCHK - Security mechanism = " + securityMechanism);
                    }
                    //RESOLVE - spec is not clear on what should happen
                    //in this case
                    if (securityMechanism != database.securityMechanism) {
                        invalidValue(CodePoint.SECMEC);
                    }
                    break;
                //optional - depending on security Mechanism 
                case CodePoint.SECTKN:
                    if ((database.securityMechanism !=
                                        CodePoint.SECMEC_EUSRIDPWD) &&
                        (database.securityMechanism !=
                                        CodePoint.SECMEC_USRSSBPWD))
                    {
                        securityCheckCode = CodePoint.SECCHKCD_SECTKNMISSING_OR_INVALID;
                        reader.skipBytes();
                    }
                    else if (database.securityMechanism ==
                                                CodePoint.SECMEC_EUSRIDPWD)
                    {
                        if (database.decryptedUserId == null)
                        {
                            try {
                                database.decryptedUserId =
                                    reader.readEncryptedString(
                                                decryptionManager,
                                                database.securityMechanism,
                                                myPublicKey,
                                                database.secTokenIn);
                            } catch (SQLException se) {
                                println2Log(database.getDatabaseName(), session.drdaID,
                                            se.getMessage());
                                if (securityCheckCode == 0) {
                                    //userid invalid
                                    securityCheckCode = CodePoint.SECCHKCD_13;
                                }
                            }
                            database.userId = database.decryptedUserId;
                            if (SanityManager.DEBUG) {
                                trace("**decrypted userid is: "+database.userId);
                            }
                        }
                        else if (database.decryptedPassword == null)
                        {
                            try {
                                database.decryptedPassword =
                                    reader.readEncryptedString(
                                            decryptionManager,
                                            database.securityMechanism,
                                            myPublicKey,
                                            database.secTokenIn);
                            } catch (SQLException se) {
                                println2Log(database.getDatabaseName(), session.drdaID,
                                            se.getMessage());
                                if (securityCheckCode == 0) {
                                    //password invalid
                                    securityCheckCode = CodePoint.SECCHKCD_0F;
                                }
                            }
                            database.password = database.decryptedPassword;
                            if (SanityManager.DEBUG) {
                                trace("**decrypted password is: " +
                                      database.password);
                            }
                        }
                    }
                    else if (database.securityMechanism ==
                                                CodePoint.SECMEC_USRSSBPWD)
                    {
                        if (database.passwordSubstitute == null)
                        {
                            database.passwordSubstitute = reader.readBytes();
                            if (SanityManager.DEBUG) {
                                trace("** Substitute Password is:" +
                                      DecryptionManager.toHexString(
                                        database.passwordSubstitute, 0,
                                        database.passwordSubstitute.length));
                            }
                            database.password =
                                DecryptionManager.toHexString(
                                    database.passwordSubstitute, 0,
                                    database.passwordSubstitute.length);
                        }
                    }
                    else
                    {
                        tooMany(CodePoint.SECTKN);
                    }
                    break;
                //optional - depending on security Mechanism
                case CodePoint.PASSWORD:
                    database.password = reader.readString();
                    if (SanityManager.DEBUG) {
                        trace("PASSWORD " + database.password);
                    }
                    break;
                //optional - depending on security Mechanism
                //we are not supporting this method so we'll skip bytes
                case CodePoint.NEWPASSWORD:
                    reader.skipBytes();
                    break;
                //optional - depending on security Mechanism
                case CodePoint.USRID:
                    database.userId = reader.readString();
                    if (SanityManager.DEBUG) {
                        trace("USERID " + database.userId);
                    }
                    break;
                //optional - depending on security Mechanism
                case CodePoint.RDBNAM:
                    String dbname = parseRDBNAM();
                    if (database != null) 
                    {
                        if (database.getDatabaseName() == null) {
                            // we didn't get the RDBNAM on ACCSEC. Set it here
                            database.setDatabaseName(dbname);
                            session.addDatabase(database);
                            session.database = database;
                        }
                        else if (!database.getDatabaseName().equals(dbname)) {
                            rdbnamMismatch(CodePoint.SECCHK);
                        }
                    }
                    else
                    {
                        // we should already have added the database in ACCSEC
                        // added code here in case we make the SECMEC session rather
                        // than database wide
                        initializeDatabase(dbname);
                    }
                    break;
                default:
                    invalidCodePoint(codePoint);

            }
            codePoint = reader.getCodePoint();
        }
        // check for SECMEC which is required
        if (securityMechanism == 0) {
            missingCodePoint(CodePoint.SECMEC);
        }

        // Check that we have a database name.
        if (getDbName() == null) {
            missingCodePoint(CodePoint.RDBNAM);
        }

        //check if we have a userid and password when we need it
        if (securityCheckCode == 0 && 
           (database.securityMechanism == CodePoint.SECMEC_USRIDPWD||
            database.securityMechanism == CodePoint.SECMEC_USRIDONL ))
        {
            if (database.userId == null)
            {
                securityCheckCode = CodePoint.SECCHKCD_USERIDMISSING;
            }
            else if (database.securityMechanism == CodePoint.SECMEC_USRIDPWD)
            {
                if (database.password == null) {
                    securityCheckCode = CodePoint.SECCHKCD_PASSWORDMISSING;
                }
            }
            //Note, we'll ignore encryptedUserId and encryptedPassword if they
            //are also set
        }

        if (securityCheckCode == 0 && 
                database.securityMechanism == CodePoint.SECMEC_USRSSBPWD)
        {
            if (database.userId == null) {
                securityCheckCode = CodePoint.SECCHKCD_USERIDMISSING;
            } else if (database.passwordSubstitute == null) {
                securityCheckCode = CodePoint.SECCHKCD_PASSWORDMISSING;
            }
        }

        if (securityCheckCode == 0 && 
                database.securityMechanism == CodePoint.SECMEC_EUSRIDPWD)
        {
            if (database.decryptedUserId == null) {
                securityCheckCode = CodePoint.SECCHKCD_USERIDMISSING;
            } else if (database.decryptedPassword == null) {
                securityCheckCode = CodePoint.SECCHKCD_PASSWORDMISSING;
            }
        }
        // RESOLVE - when we do security we need to decrypt encrypted userid & password
        // before proceeding
        } // End "if (deferredReset) ... else ..." block

        // verify userid and password, if we haven't had any errors thus far.
        if ((securityCheckCode == 0) && (databaseAccessException == null))
        {
            // DERBY-3596: Reset server side (embedded) physical connection for
            //     use with a new logical connection on the client.
            if (this.deferredReset) {
                // Reset the existing connection here.
                try {
                    database.getConnection().resetFromPool();
                    database.getConnection().setHoldability(
                            ResultSet.HOLD_CURSORS_OVER_COMMIT);
                    // Reset isolation level to default, as the client is in
                    // the process of creating a new logical connection.
                    database.getConnection().setTransactionIsolation(
                            Connection.TRANSACTION_READ_COMMITTED);
                } catch (SQLException sqle) {
                    handleException(sqle);
                }
            } else {
                securityCheckCode = verifyUserIdPassword();
            }
        }

        // Security all checked 
        if (securityCheckCode == 0) {
            session.setState(Session.CHKSEC);
        }
        
        return securityCheckCode;

    }

    /**
     * Write security check reply
     * Instance variables
     *  SVRCOD - serverity code - required
     *  SECCHKCD    - security check code  - required
     *  SECTKN - security token - optional, ignorable
     *  SVCERRNO    - security service error number
     *  SRVDGN  - Server Diagnostic Information
     *
     * @exception DRDAProtocolException
     */
    private void writeSECCHKRM(int securityCheckCode) throws DRDAProtocolException
    {
        writer.createDssReply();
        writer.startDdm(CodePoint.SECCHKRM);
        writer.writeScalar2Bytes(CodePoint.SVRCOD, svrcodFromSecchkcd(securityCheckCode));
        writer.writeScalar1Byte(CodePoint.SECCHKCD, securityCheckCode);
        writer.endDdmAndDss ();

        if (securityCheckCode != 0) {
        // then we have an error and are going to end up ignoring the rest
        // of the DSS request chain.
            skipRemainder(false);
        }

        finalizeChain();

    }
    /**
     * Calculate SVRCOD value from SECCHKCD
     *
     * @param securityCheckCode
     * @return SVRCOD value
     */
    private int svrcodFromSecchkcd(int securityCheckCode)
    {
        if (securityCheckCode == 0 || securityCheckCode == 2 ||
            securityCheckCode == 5 || securityCheckCode == 8) {
            return CodePoint.SVRCOD_INFO;
        } else {
            return CodePoint.SVRCOD_ERROR;
        }
    }
    /**
     * Parse access RDB
     * Instance variables
     *  RDBACCCL - RDB Access Manager Class - required must be SQLAM
     *  CRRTKN - Correlation Token - required
     *  RDBNAM - Relational database name -required
     *  PRDID - Product specific identifier - required
     *  TYPDEFNAM   - Data Type Definition Name -required
     *  TYPDEFOVR   - Type definition overrides -required
     *  RDBALWUPD -  RDB Allow Updates optional
     *  PRDDTA - Product Specific Data - optional - ignorable
     *  STTDECDEL - Statement Decimal Delimiter - optional
     *  STTSTRDEL - Statement String Delimiter - optional
     *  TRGDFTRT - Target Default Value Return - optional
     *
     * @return severity code
     *
     * @exception DRDAProtocolException
     */
    private int parseACCRDB() throws  DRDAProtocolException
    {
        int codePoint;
        int svrcod = 0;
        copyToRequired(ACCRDB_REQUIRED);
        reader.markCollection();
        codePoint = reader.getCodePoint();
        while (codePoint != -1)
        {
            switch (codePoint)
            {
                //required
                case CodePoint.RDBACCCL:
                    checkLength(CodePoint.RDBACCCL, 2);
                    int sqlam = reader.readNetworkShort();
                    if (SanityManager.DEBUG) {
                        trace("RDBACCCL = " + sqlam);
                    }
                    // required to be SQLAM 

                    if (sqlam != CodePoint.SQLAM) {
                        invalidValue(CodePoint.RDBACCCL);
                    }
                    removeFromRequired(CodePoint.RDBACCCL);
                    break;
                //required
                case CodePoint.CRRTKN:
                    database.crrtkn = reader.readBytes();
                    if (SanityManager.DEBUG) {
                        trace("crrtkn " + convertToHexString(database.crrtkn));
                    }
                    removeFromRequired(CodePoint.CRRTKN);
                    int l = database.crrtkn.length;
                    if (l > CodePoint.MAX_NAME) {
                        tooBig(CodePoint.CRRTKN);
                    }
                    // the format of the CRRTKN is defined in the DRDA reference
                    // x.yz where x is 1 to 8 bytes (variable)
                    //      y is 1 to 8 bytes (variable)
                    //      x is 6 bytes fixed
                    //      size is variable between 9 and 23
                    if (l < 9 || l > 23) {
                        invalidValue(CodePoint.CRRTKN);
                    }
                    byte[] part1 = new byte[l - 6];
                    System.arraycopy(database.crrtkn, 0,
                                     part1, 0, part1.length);
                    long time = SignedBinary.getLong(database.crrtkn, 
                            l-8, SignedBinary.BIG_ENDIAN); // as "long" as unique
                    session.drdaID = reader.convertBytes(part1) + 
                                    time + leftBrace + session.connNum + rightBrace;
                    if (SanityManager.DEBUG) {
                        trace("******************************************drdaID is: " + session.drdaID);
                    }
                    database.setDrdaID(session.drdaID);
    
                    break;
                //required
                case CodePoint.RDBNAM:
                    String dbname = parseRDBNAM();
                    if (database != null)
                    { 
                        if (!database.getDatabaseName().equals(dbname)) {
                            rdbnamMismatch(CodePoint.ACCRDB);
                        }
                    }
                    else
                    {
                        //first time we have seen a database name
                        Database d = session.getDatabase(dbname);
                        if (d == null)
                        {
                            initializeDatabase(dbname);
                        }
                        else
                        {
                            database = d;
                            database.accessCount++;
                        }
                    }
                    removeFromRequired(CodePoint.RDBNAM);
                    break;
                //required
                case CodePoint.PRDID:
                    appRequester.setClientVersion(reader.readString());
                    if (SanityManager.DEBUG) {
                        trace("prdId " + appRequester.prdid);
                    }
                    if (appRequester.prdid.length() > CodePoint.PRDID_MAX) {
                        tooBig(CodePoint.PRDID);
                    }
                    if (appRequester.getClientType() != AppRequester.DNC_CLIENT) {
                        invalidClient(appRequester.prdid);
                    }
                    // All versions of DNC,the only client supported, handle
                    // warnings on CNTQRY
                    sendWarningsOnCNTQRY = true;
                    // The client can not request DIAGLVL because when run with
                    // an older server it will cause an exception. Older version
                    // of the server do not recognize requests for DIAGLVL.
                    if ((appRequester.getClientType() == AppRequester.DNC_CLIENT) &&
                            appRequester.greaterThanOrEqualTo(10, 2, 0)) {
                        diagnosticLevel = CodePoint.DIAGLVL1;
                    }

                    removeFromRequired(CodePoint.PRDID);
                    break;
                //required
                case CodePoint.TYPDEFNAM:
                    setStmtOrDbByteOrder(true, null, parseTYPDEFNAM());
                    removeFromRequired(CodePoint.TYPDEFNAM);
                    break;
                //required
                case CodePoint.TYPDEFOVR:
                    parseTYPDEFOVR(null);
                    removeFromRequired(CodePoint.TYPDEFOVR);
                    break;
                //optional 
                case CodePoint.RDBALWUPD:
                    checkLength(CodePoint.RDBALWUPD, 1);
                    database.rdbAllowUpdates = readBoolean(CodePoint.RDBALWUPD);
                    if (SanityManager.DEBUG) {
                        trace("rdbAllowUpdates = "+database.rdbAllowUpdates);
                    }
                    break;
                //optional, ignorable
                case CodePoint.PRDDTA:
                    // check that it fits in maximum but otherwise ignore for now
                    if (reader.getDdmLength() > CodePoint.MAX_NAME) {
                        tooBig(CodePoint.PRDDTA);
                    }
                    reader.skipBytes();
                    break;
                case CodePoint.TRGDFTRT:
                    if (reader.readByte() == (byte) 0xF1) {
                        database.sendTRGDFTRT = true;
                    }
                    break;
                //optional - not used in JCC so skip for now
                case CodePoint.STTDECDEL:
                case CodePoint.STTSTRDEL:
                    codePointNotSupported(codePoint);
                    break;
                default:
                    invalidCodePoint(codePoint);
            }
            codePoint = reader.getCodePoint();
        }
        checkRequired(CodePoint.ACCRDB);
        // check that we can support the double-byte and mixed-byte CCSIDS
        // set svrcod to warning if they are not supported
        if ((database.ccsidDBC != 0 && !server.supportsCCSID(database.ccsidDBC)) ||
                (database.ccsidMBC != 0 && !server.supportsCCSID(database.ccsidMBC))) 
        {
            svrcod = CodePoint.SVRCOD_WARNING;
        }
        return svrcod;
    }
    /**
     * Parse TYPDEFNAM
     *
     * @return typdefnam
     * @exception DRDAProtocolException
     */
    private String parseTYPDEFNAM() throws DRDAProtocolException
    {
        String typDefNam = reader.readString();
        if (SanityManager.DEBUG) {
            trace("typeDefName " + typDefNam);
        }
        if (typDefNam.length() > CodePoint.MAX_NAME) {
            tooBig(CodePoint.TYPDEFNAM);
        }
        checkValidTypDefNam(typDefNam);
        // check if the typedef is one we support
        if (!typDefNam.equals(CodePoint.TYPDEFNAM_QTDSQLASC)  &&
            !typDefNam.equals(CodePoint.TYPDEFNAM_QTDSQLJVM) &&
            !typDefNam.equals(CodePoint.TYPDEFNAM_QTDSQLX86))
        {
            valueNotSupported(CodePoint.TYPDEFNAM);
        }
        return typDefNam;
    }

    /**
     * Set a statement or the database' byte order, depending on the arguments
     *
     * @param setDatabase   if true, set database' byte order, otherwise set statement's
     * @param stmt          DRDAStatement, used when setDatabase is false
     * @param typDefNam     TYPDEFNAM value
     */
    private void setStmtOrDbByteOrder(boolean setDatabase, DRDAStatement stmt, String typDefNam)
    {
        int byteOrder = (typDefNam.equals(CodePoint.TYPDEFNAM_QTDSQLX86) ?
                            SignedBinary.LITTLE_ENDIAN : SignedBinary.BIG_ENDIAN);
        if (setDatabase)
        {
            database.typDefNam = typDefNam;
            database.byteOrder = byteOrder;
        }
        else
        {
            stmt.typDefNam = typDefNam;
            stmt.byteOrder = byteOrder;
        }
    }

    /**
     * Write Access to RDB Completed
     * Instance Variables
     *  SVRCOD - severity code - 0 info, 4 warning -required
     *  PRDID - product specific identifier -required
     *  TYPDEFNAM - type definition name -required
     *  TYPDEFOVR - type definition overrides - required
     *  RDBINTTKN - token which can be used to interrupt DDM commands - optional
     *  CRRTKN  - correlation token - only returned if we didn't get one from requester
     *  SRVDGN - server diagnostic information - optional
     *  PKGDFTCST - package default character subtype - optional
     *  USRID - User ID at the target system - optional
     *  SRVLST - Server List
     * 
     * @exception DRDAProtocolException
     */
    private void writeACCRDBRM(int svrcod) throws DRDAProtocolException
    {
        writer.createDssReply();
        writer.startDdm(CodePoint.ACCRDBRM);
        writer.writeScalar2Bytes(CodePoint.SVRCOD, svrcod);
        writer.writeScalarString(CodePoint.PRDID,
//IC see: https://issues.apache.org/jira/browse/DERBY-6206
                                 NetworkServerControlImpl.prdId());
        //TYPDEFNAM -required - JCC doesn't support QTDSQLJVM so for now we
        // just use ASCII, though we should eventually be able to use QTDSQLJVM
        // at level 7
        writer.writeScalarString(CodePoint.TYPDEFNAM,
                                 CodePoint.TYPDEFNAM_QTDSQLASC);
        writeTYPDEFOVR();
        writer.endDdmAndDss ();

         // Write the initial piggy-backed data, currently the isolation level
         // and the schema name. Only write it if the client supports session
         // data caching.
         // Sending the session data on connection initialization was introduced
         // in Derby 10.7.
//IC see: https://issues.apache.org/jira/browse/DERBY-5847
//IC see: https://issues.apache.org/jira/browse/DERBY-436
         if ((appRequester.getClientType() == AppRequester.DNC_CLIENT) &&
                 appRequester.greaterThanOrEqualTo(10, 7, 0)) {
             try {
                 writePBSD();
             } catch (SQLException se) {
                 server.consoleExceptionPrint(se);
                 errorInChain(se);
             }
         }
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        finalizeChain();
    }
    
    private void writeTYPDEFOVR() throws DRDAProtocolException
    {
        //TYPDEFOVR - required - only single byte and mixed byte are specified
        writer.startDdm(CodePoint.TYPDEFOVR);
//IC see: https://issues.apache.org/jira/browse/DERBY-5847
//IC see: https://issues.apache.org/jira/browse/DERBY-436
        writer.writeScalar2Bytes(CodePoint.CCSIDSBC,
                                 NetworkServerControlImpl.CCSIDSBC);
        writer.writeScalar2Bytes(CodePoint.CCSIDMBC,
                                 NetworkServerControlImpl.CCSIDMBC);
        // PKGDFTCST - Send character subtype and userid if requested
        if (database.sendTRGDFTRT)
        {
            // default to multibyte character
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
            writer.startDdm(CodePoint.PKGDFTCST);
            writer.writeShort(CodePoint.CSTMBCS);
            writer.endDdm();
            // userid
            writer.startDdm(CodePoint.USRID);
            writer.writeString(database.userId);
            writer.endDdm();
        }
        writer.endDdm();

    }
    
    /**
     * Parse Type Defintion Overrides
     *  TYPDEF Overrides specifies the Coded Character SET Identifiers (CCSIDs)
     *  that are in a named TYPDEF.
     * Instance Variables
     *  CCSIDSBC - CCSID for Single-Byte - optional
     *  CCSIDDBC - CCSID for Double-Byte - optional
     *  CCSIDMBC - CCSID for Mixed-byte characters -optional
     *
     * @param st    Statement this TYPDEFOVR applies to
     *
     * @exception DRDAProtocolException
     */
    private void parseTYPDEFOVR(DRDAStatement st) throws  DRDAProtocolException
    {
        int codePoint;
        int ccsidSBC = 0;
        int ccsidDBC = 0;
        int ccsidMBC = 0;
        String ccsidSBCEncoding = null;
        String ccsidDBCEncoding = null;
        String ccsidMBCEncoding = null;

        reader.markCollection();

        codePoint = reader.getCodePoint();
        // at least one of the following instance variable is required
        // if the TYPDEFOVR is specified in a command object
        if (codePoint == -1 && st != null) {
            missingCodePoint(CodePoint.CCSIDSBC);
        }

        while (codePoint != -1)
        {
            switch (codePoint)
            {
                case CodePoint.CCSIDSBC:
                    checkLength(CodePoint.CCSIDSBC, 2);
                    ccsidSBC = reader.readNetworkShort();
                    try {
                        ccsidSBCEncoding = 
                            CharacterEncodings.getJavaEncoding(ccsidSBC);
                    } catch (Exception e) {
                        valueNotSupported(CodePoint.CCSIDSBC);
                    }
                    if (SanityManager.DEBUG) {
                        trace("ccsidsbc = " + ccsidSBC + " encoding = " + ccsidSBCEncoding);
                    }
                    break;
                case CodePoint.CCSIDDBC:
                    checkLength(CodePoint.CCSIDDBC, 2);
                    ccsidDBC = reader.readNetworkShort();
                    try {
                        ccsidDBCEncoding = 
                            CharacterEncodings.getJavaEncoding(ccsidDBC);
                    } catch (Exception e) {
                        // we write a warning later for this so no error
                        // unless for a statement
                        ccsidDBCEncoding = null;
                        if (st != null) {
                            valueNotSupported(CodePoint.CCSIDSBC);
                        }
                    }
                    if (SanityManager.DEBUG) {
                        trace("ccsiddbc = " + ccsidDBC + " encoding = " + ccsidDBCEncoding);
                    }
                    break;
                case CodePoint.CCSIDMBC:
                    checkLength(CodePoint.CCSIDMBC, 2);
                    ccsidMBC = reader.readNetworkShort();
                    try {
                        ccsidMBCEncoding = 
                            CharacterEncodings.getJavaEncoding(ccsidMBC);
                    } catch (Exception e) {
                        // we write a warning later for this so no error
                        ccsidMBCEncoding = null;
                        if (st != null) {
                            valueNotSupported(CodePoint.CCSIDMBC);
                        }
                    }
                    if (SanityManager.DEBUG) {
                        trace("ccsidmbc = " + ccsidMBC + " encoding = " + ccsidMBCEncoding);
                    }
                    break;
                default:
                    invalidCodePoint(codePoint);

            }
            codePoint = reader.getCodePoint();
        }
        if (st == null)
        {
            if (ccsidSBC != 0)
            {
                database.ccsidSBC = ccsidSBC;
                database.ccsidSBCEncoding = ccsidSBCEncoding;
            }
            if (ccsidDBC != 0)
            {
                database.ccsidDBC = ccsidDBC;
                database.ccsidDBCEncoding = ccsidDBCEncoding;
            }
            if (ccsidMBC != 0)
            {
                database.ccsidMBC = ccsidMBC;
                database.ccsidMBCEncoding = ccsidMBCEncoding;
            }
        }
        else
        {
            if (ccsidSBC != 0)
            {
                st.ccsidSBC = ccsidSBC;
                st.ccsidSBCEncoding = ccsidSBCEncoding;
            }
            if (ccsidDBC != 0)
            {
                st.ccsidDBC = ccsidDBC;
                st.ccsidDBCEncoding = ccsidDBCEncoding;
            }
            if (ccsidMBC != 0)
            {
                st.ccsidMBC = ccsidMBC;
                st.ccsidMBCEncoding = ccsidMBCEncoding;
            }
        }
    }
    /**
     * Parse PRPSQLSTT - Prepare SQL Statement
     * Instance Variables
     *   RDBNAM - Relational Database Name - optional
     *   PKGNAMCSN - RDB Package Name, Consistency Token, and Section Number - required
     *   RTNSQLDA - Return SQL Descriptor Area - optional
     *   MONITOR - Monitor events - optional.
     *   
     * @return return 0 - don't return sqlda, 1 - return input sqlda, 
     *      2 - return output sqlda
     * @throws DRDAProtocolException
     * @throws SQLException
     */
    private int parsePRPSQLSTT() throws DRDAProtocolException,SQLException
    {
        int codePoint;
        boolean rtnsqlda = false;
        boolean rtnOutput = true;   // Return output SQLDA is default
        Pkgnamcsn pkgnamcsn = null;
//IC see: https://issues.apache.org/jira/browse/DERBY-212

        Database databaseToSet = null;

        reader.markCollection();

        codePoint = reader.getCodePoint();
        while (codePoint != -1)
        {
            switch (codePoint)
            {
                // optional
                case CodePoint.RDBNAM:
                    setDatabase(CodePoint.PRPSQLSTT);
                    databaseToSet = database;
                    break;
                // required
                case CodePoint.PKGNAMCSN:
                    pkgnamcsn = parsePKGNAMCSN(); 
                    break;
                //optional
                case CodePoint.RTNSQLDA:
                // Return SQLDA with description of statement
                    rtnsqlda = readBoolean(CodePoint.RTNSQLDA);
                    break;
                //optional
                case CodePoint.TYPSQLDA:
                    rtnOutput = parseTYPSQLDA();
                    break;
                //optional
                case CodePoint.MONITOR:
                    parseMONITOR();
                    break;
                default:
                    invalidCodePoint(codePoint);

            }
            codePoint = reader.getCodePoint();
        }

        DRDAStatement stmt = database.newDRDAStatement(pkgnamcsn);
        String sqlStmt = parsePRPSQLSTTobjects(stmt);
        if (databaseToSet != null) {
            stmt.setDatabase(database);
        }
        stmt.explicitPrepare(sqlStmt);
        // set the statement as the current statement
        database.setCurrentStatement(stmt);

        if (!rtnsqlda) {
            return 0;
        } else if (rtnOutput) {
            return 2;
        } else {
            return 1;
        }
    }
    /**
     * Parse PRPSQLSTT objects
     * Objects
     *  TYPDEFNAM - Data type definition name - optional
     *  TYPDEFOVR - Type defintion overrides - optional
     *  SQLSTT - SQL Statement required
     *  SQLATTR - Cursor attributes on prepare - optional - level 7
     *
     * If TYPDEFNAM and TYPDEFOVR are supplied, they apply to the objects
     * sent with the statement.  Once the statement is over, the default values
     * sent in the ACCRDB are once again in effect.  If no values are supplied,
     * the values sent in the ACCRDB are used.
     * Objects may follow in one DSS or in several DSS chained together.
     * 
     * @return SQL statement
     * @throws DRDAProtocolException
     * @throws SQLException
     */
    private String parsePRPSQLSTTobjects(DRDAStatement stmt) 
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        throws DRDAProtocolException, SQLException
    {
        String sqlStmt = null;
        int codePoint;
        do
        {
            correlationID = reader.readDssHeader();
            while (reader.moreDssData())
            {
                codePoint = reader.readLengthAndCodePoint( false );
                switch(codePoint)
                {
                    // required
                    case CodePoint.SQLSTT:
                        sqlStmt = parseEncodedString();
                        if (SanityManager.DEBUG) {
                            trace("sqlStmt = " + sqlStmt);
                        }
                        break;
                    // optional
                    case CodePoint.TYPDEFNAM:
                        setStmtOrDbByteOrder(false, stmt, parseTYPDEFNAM());
                        break;
                    // optional
                    case CodePoint.TYPDEFOVR:
                        parseTYPDEFOVR(stmt);
                        break;
                    // optional
                    case CodePoint.SQLATTR:
                        parseSQLATTR(stmt);
                        break;
                    default:
                        invalidCodePoint(codePoint);
                }
            }
        } while (reader.isChainedWithSameID());
        if (sqlStmt == null) {
            missingCodePoint(CodePoint.SQLSTT);
        }

        return sqlStmt;
    }

    /**
     * Parse TYPSQLDA - Type of the SQL Descriptor Area
     *
     * @return true if for output; false otherwise
     * @exception DRDAProtocolException
     */
    private boolean parseTYPSQLDA() throws DRDAProtocolException
    {
        checkLength(CodePoint.TYPSQLDA, 1);
        byte sqldaType = reader.readByte();
        if (SanityManager.DEBUG) {
            trace("typSQLDa " + sqldaType);
        }
        if (sqldaType == CodePoint.TYPSQLDA_STD_OUTPUT ||
                sqldaType == CodePoint.TYPSQLDA_LIGHT_OUTPUT ||
                sqldaType == CodePoint.TYPSQLDA_X_OUTPUT)
        {
            return true;
        }
        else if (sqldaType == CodePoint.TYPSQLDA_STD_INPUT ||
                     sqldaType == CodePoint.TYPSQLDA_LIGHT_INPUT ||
                     sqldaType == CodePoint.TYPSQLDA_X_INPUT)
        {
                return false;
        }
        else
        {
            invalidValue(CodePoint.TYPSQLDA);
        }

        // shouldn't get here but have to shut up compiler
        return false;
    }
    /**
     * Parse SQLATTR - Cursor attributes on prepare
     *   This is an encoded string. Can have combination of following, eg INSENSITIVE SCROLL WITH HOLD
     * Possible strings are
     *  SENSITIVE DYNAMIC SCROLL [FOR UPDATE]
     *  SENSITIVE STATIC SCROLL [FOR UPDATE]
     *  INSENSITIVE SCROLL
     *  FOR UPDATE
     *  WITH HOLD
     *
     * @param stmt DRDAStatement
     * @exception DRDAProtocolException
     */
    protected void parseSQLATTR(DRDAStatement stmt) throws DRDAProtocolException
    {
        String attrs = parseEncodedString();
        if (SanityManager.DEBUG) {
            trace("sqlattr = '" + attrs+"'");
        }
        //let Derby handle any errors in the types it doesn't support
        //just set the attributes

        boolean validAttribute = false;
        if (attrs.indexOf("INSENSITIVE SCROLL") != -1 || attrs.indexOf("SCROLL INSENSITIVE") != -1) //CLI
        {
            stmt.scrollType = ResultSet.TYPE_SCROLL_INSENSITIVE;
            stmt.concurType = ResultSet.CONCUR_READ_ONLY;
            validAttribute = true;
        }
        if ((attrs.indexOf("SENSITIVE DYNAMIC SCROLL") != -1) || (attrs.indexOf("SENSITIVE STATIC SCROLL") != -1))
        {
            stmt.scrollType = ResultSet.TYPE_SCROLL_SENSITIVE;
            validAttribute = true;
        }

        if ((attrs.indexOf("FOR UPDATE") != -1))
        {
            validAttribute = true;
            stmt.concurType = ResultSet.CONCUR_UPDATABLE;
        }

        if (attrs.indexOf("WITH HOLD") != -1)
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-3484
            stmt.withHoldCursor = ResultSet.HOLD_CURSORS_OVER_COMMIT;
            validAttribute = true;
        }

        if (!validAttribute)
        {
            invalidValue(CodePoint.SQLATTR);
        }
    }

    /**
     * Parse DSCSQLSTT - Describe SQL Statement previously prepared
     * Instance Variables
     *  TYPSQLDA - sqlda type expected (output or input)
     *  RDBNAM - relational database name - optional
     *  PKGNAMCSN - RDB Package Name, Consistency Token and Section Number - required
     *  MONITOR - Monitor events - optional.
     *
     * @return expect "output sqlda" or not
     * @throws DRDAProtocolException
     * @throws SQLException
     */
    private boolean parseDSCSQLSTT() throws DRDAProtocolException,SQLException
    {
        int codePoint;
        boolean rtnOutput = true;   // default
        Pkgnamcsn pkgnamcsn = null;
        reader.markCollection();

        codePoint = reader.getCodePoint();
        while (codePoint != -1)
        {
            switch (codePoint)
            {
                // optional
                case CodePoint.TYPSQLDA:
                    rtnOutput = parseTYPSQLDA();
                    break;
                // optional
                case CodePoint.RDBNAM:
                    setDatabase(CodePoint.DSCSQLSTT);
                    break;
                // required
                case CodePoint.PKGNAMCSN:
                    pkgnamcsn = parsePKGNAMCSN();
                    DRDAStatement stmt = database.getDRDAStatement(pkgnamcsn);     
                    if (stmt == null)
                    {
                        invalidValue(CodePoint.PKGNAMCSN);
                    }
                    break;
                //optional
                case CodePoint.MONITOR:
                    parseMONITOR();
                    break;
                default:
                    invalidCodePoint(codePoint);
            }
            codePoint = reader.getCodePoint();
        }
        if (pkgnamcsn == null) {
            missingCodePoint(CodePoint.PKGNAMCSN);
        }
        return rtnOutput;
    }

    /**
     * Parse EXCSQLSTT - Execute non-cursor SQL Statement previously prepared
     * Instance Variables
     *  RDBNAM - relational database name - optional
     *  PKGNAMCSN - RDB Package Name, Consistency Token and Section Number - required
     *  OUTEXP - Output expected
     *  NBRROW - Number of rows to be inserted if it's an insert
     *  PRCNAM - procedure name if specified by host variable, not needed for Derby
     *  QRYBLKSZ - query block size
     *  MAXRSLCNT - max resultset count
     *  MAXBLKEXT - Max number of extra blocks
     *  RSLSETFLG - resultset flag
     *  RDBCMTOK - RDB Commit Allowed - optional
     *  OUTOVROPT - output override option
     *  QRYROWSET - Query Rowset Size - Level 7
     *  MONITOR - Monitor events - optional.
     *
     * @throws DRDAProtocolException
     * @throws SQLException
     */
    private void parseEXCSQLSTT() throws DRDAProtocolException,SQLException
    {
        int codePoint;
        String strVal;
        reader.markCollection();

        codePoint = reader.getCodePoint();
        boolean outputExpected = false;
//IC see: https://issues.apache.org/jira/browse/DERBY-212
        Pkgnamcsn pkgnamcsn = null;
        int numRows = 1;    // default value
        int blkSize =  0;
        int maxrslcnt = 0;  // default value
        int maxblkext = CodePoint.MAXBLKEXT_DEFAULT;
        int qryrowset = CodePoint.QRYROWSET_DEFAULT;
        int outovropt = CodePoint.OUTOVRFRS;
        byte [] rslsetflg = null;
        String procName = null;

        while (codePoint != -1)
        {
            switch (codePoint)
            {
                // optional
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
                case CodePoint.RDBNAM:
                    setDatabase(CodePoint.EXCSQLSTT);
                    break;
                // required
                case CodePoint.PKGNAMCSN:
                    pkgnamcsn = parsePKGNAMCSN();
                    break;
                // optional
                case CodePoint.OUTEXP:
                    outputExpected = readBoolean(CodePoint.OUTEXP);
                    if (SanityManager.DEBUG) {
                        trace("outexp = "+ outputExpected);
                    }
                    break;
                // optional
                case CodePoint.NBRROW:
                    checkLength(CodePoint.NBRROW, 4);
                    numRows = reader.readNetworkInt();
                    if (SanityManager.DEBUG) {
                        trace("# of rows: "+numRows);
                    }
                    break;
                // optional
                case CodePoint.PRCNAM:
                    procName = reader.readString();
                    if (SanityManager.DEBUG) {
                        trace("Procedure Name = " + procName);
                    }
                    break;
                // optional
                case CodePoint.QRYBLKSZ:
                    blkSize = parseQRYBLKSZ();
                    break;
                // optional
                case CodePoint.MAXRSLCNT:
                    // this is the maximum result set count
                    // values are 0 - requester is not capabable of receiving result
                    // sets as reply data in the response to EXCSQLSTT
                    // -1 - requester is able to receive all result sets
                    checkLength(CodePoint.MAXRSLCNT, 2);
                    maxrslcnt = reader.readNetworkShort();
                    if (SanityManager.DEBUG) {
                        trace("max rs count: "+maxrslcnt);
                    }
                    break;
                // optional
                case CodePoint.MAXBLKEXT:
                    // number of extra qury blocks of answer set data per result set
                    // 0 - no extra query blocks
                    // -1 - can receive entire result set
                    checkLength(CodePoint.MAXBLKEXT, 2);
                    maxblkext = reader.readNetworkShort();
                    if (SanityManager.DEBUG) {
                        trace("max extra blocks: "+maxblkext);
                    }
                    break;
                // optional
                case CodePoint.RSLSETFLG:
                    //Result set flags
                    rslsetflg = reader.readBytes();
                    if (SanityManager.DEBUG) {
                        for (byte b : rslsetflg) {
                            trace("rslsetflg: " + b);
                        }
                    }
                    break;
                // optional
                case CodePoint.RDBCMTOK:
                    parseRDBCMTOK();
                    break;
                // optional
                case CodePoint.OUTOVROPT:
                    outovropt = parseOUTOVROPT();
                    break;
                // optional
                case CodePoint.QRYROWSET:
                    //Note minimum for OPNQRY is 0, we'll assume it is the same
                    //for EXCSQLSTT though the standard doesn't say
                    qryrowset = parseQRYROWSET(0);
                    break;
                //optional
                case CodePoint.MONITOR:
                    parseMONITOR();
                    break;
                default:
                    invalidCodePoint(codePoint);
            }
            codePoint = reader.getCodePoint();
        }

        if (pkgnamcsn == null) {
            missingCodePoint(CodePoint.PKGNAMCSN);
        }

        DRDAStatement stmt;
        boolean needPrepareCall = false;

        stmt  = database.getDRDAStatement(pkgnamcsn);
        boolean isProcedure = (procName !=null || 
                               (stmt != null && 
                                stmt.wasExplicitlyPrepared() &&
                                stmt.isCall));

        if (isProcedure)        // stored procedure call
        {
            if ( stmt == null  || !(stmt.wasExplicitlyPrepared()))
            {
                stmt  = database.newDRDAStatement(pkgnamcsn);
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
                stmt.setQryprctyp(CodePoint.QRYBLKCTL_DEFAULT);
                needPrepareCall = true;
            }
                
            stmt.procName = procName;
            stmt.outputExpected = outputExpected;
        }
        else
        {
            // we can't find the statement
            if (stmt == null)
            {
                invalidValue(CodePoint.PKGNAMCSN);
            }
            stmt.setQryprctyp(CodePoint.QRYBLKCTL_DEFAULT);
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        stmt.nbrrow = numRows;
        stmt.qryrowset = qryrowset;
        stmt.blksize = blkSize;
        stmt.maxblkext = maxblkext;
        stmt.maxrslcnt = maxrslcnt;
        stmt.outovropt = outovropt;
        stmt.rslsetflg = rslsetflg;
//IC see: https://issues.apache.org/jira/browse/DERBY-506
        if (pendingStatementTimeout >= 0) {
            stmt.getPreparedStatement().setQueryTimeout(pendingStatementTimeout);
            pendingStatementTimeout = -1;
        }
 
    
        // set the statement as the current statement
        database.setCurrentStatement(stmt);
        
        boolean hasResultSet;
//IC see: https://issues.apache.org/jira/browse/DERBY-1559
        if (reader.isChainedWithSameID()) 
        {
            hasResultSet = parseEXCSQLSTTobjects(stmt);
        } else 
        {
            if (isProcedure  && (needPrepareCall))
            {
                // if we had parameters the callable statement would
                // be prepared with parseEXCQLSTTobjects, otherwise we
                // have to do it here
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
                String prepareString = "call " + stmt.procName +"()";
                if (SanityManager.DEBUG) {
                    trace ("$$$prepareCall is: "+prepareString);
                }
                database.getConnection().clearWarnings();
                CallableStatement cs = (CallableStatement) stmt.prepare(prepareString);
            }
            stmt.ps.clearWarnings();
            hasResultSet = stmt.execute();
        }
        
        
        ResultSet rs = null;
        if (hasResultSet)
        {
            rs = stmt.getResultSet();
        }
        // temp until ps.execute() return value fixed
        hasResultSet = (rs != null);
        int numResults = 0;
        if (hasResultSet)
        {
            numResults = stmt.getNumResultSets();
            writeRSLSETRM(stmt);
        }

        // First of all, we send if there really are output params. Otherwise
        // CLI (.Net driver) fails. DRDA spec (page 151,152) says send SQLDTARD
        // if server has output param data to send.
        boolean sendSQLDTARD = stmt.hasOutputParams() && outputExpected;
        if (isProcedure)
        {
            if (sendSQLDTARD) {
                writer.createDssObject();
                writer.startDdm(CodePoint.SQLDTARD);
                writer.startDdm(CodePoint.FDODSC);
                writeQRYDSC(stmt, true);
                writer.endDdm();
                writer.startDdm(CodePoint.FDODTA);
                writeFDODTA(stmt);
//IC see: https://issues.apache.org/jira/browse/DERBY-4066
                writer.endDdm();
                writer.endDdmAndDss();

                if (stmt.getExtDtaObjects() != null)
                {
                    // writeScalarStream() ends the dss
                    writeEXTDTA(stmt);
                }
            }
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
            else if (hasResultSet) {
            // DRDA spec says that we MUST return either an
            // SQLDTARD or an SQLCARD--the former when we have
            // output parameters, the latter when we don't.
            // If we have a result set, then we have to write
            // the SQLCARD _now_, since it is expected before
            // we send the result set info below; if we don't
            // have a result set and we don't send SQLDTARD,
            // then we can wait until we reach the call to
            // checkWarning() below, which will write an
            // SQLCARD for us.
                writeNullSQLCARDobject();
            }
        }

        //We need to marke that params are finished so that we know we 
        // are ready to send resultset info.
        stmt.finishParams();
            
//IC see: https://issues.apache.org/jira/browse/DERBY-6000
        EnginePreparedStatement ps = stmt.getPreparedStatement();
        int rsNum = 0;
        do {
        if (hasResultSet)
        {
            stmt.setCurrentDrdaResultSet(rsNum);
            //indicate that we are going to return data
            stmt.setQryrtndta(true);
            if (!isProcedure) {
                checkWarning(null, ps, null, -1, true, true);
            }
            if (rsNum == 0) {
                writeSQLRSLRD(stmt);
            }
            writeOPNQRYRM(true, stmt);
            writeSQLCINRD(stmt);
            writeQRYDSC(stmt, false);
            stmt.rsSuspend();

            /* Currently, if LMTBLKPRC is used, a pre-condition is that no lob columns.
             * But in the future, when we do support LOB in LMTBLKPRC, the drda spec still
             * does not allow LOB to be sent with OPNQRYRM.  So this "if" here will have
             * to add "no lob columns".
             */
            if (stmt.getQryprctyp() == CodePoint.LMTBLKPRC) {
                writeQRYDTA(stmt);
            }
        }
        else  if (! sendSQLDTARD)
        {
            long updateCount = ps.getLargeUpdateCount();
//IC see: https://issues.apache.org/jira/browse/DERBY-6000

            // The protocol wants us to send RDBUPDRM here, but we don't do
            // that because it used to cause protocol errors. DERBY-5847 has
            // some discussion about this issue.

            checkWarning(database.getConnection(), stmt.ps, null, updateCount, true, true);
        }

        } while(hasResultSet && (++rsNum < numResults));
    }


    /**
     * Parse RDBCMTOK - tells the database whether to allow commits or rollbacks
     * to be executed as part of the command
     * Since we don't have a SQL commit or rollback command, we will just ignore
     * this for now
     *
     * @exception DRDAProtocolException
     */
    private void parseRDBCMTOK() throws DRDAProtocolException
    {
        boolean rdbcmtok = readBoolean(CodePoint.RDBCMTOK);
        if (SanityManager.DEBUG) {
            trace("rdbcmtok = " + rdbcmtok);
        }
    }

    /**
     * Parse EXCSQLSTT command objects
     * Command Objects
     *  TYPDEFNAM - Data Type Definition Name - optional
     *  TYPDEFOVR - TYPDEF Overrides -optional
     *  SQLDTA - optional, variable data, specified if prpared statement has input parameters
     *  EXTDTA - optional, externalized FD:OCA data
     *  OUTOVR - output override descriptor, not allowed for stored procedure calls
     *
     * If TYPDEFNAM and TYPDEFOVR are supplied, they apply to the objects
     * sent with the statement.  Once the statement is over, the default values
     * sent in the ACCRDB are once again in effect.  If no values are supplied,
     * the values sent in the ACCRDB are used.
     * Objects may follow in one DSS or in several DSS chained together.
     * 
     * @param stmt  the DRDAStatement to execute
     * @throws DRDAProtocolException
     * @throws SQLException
     */
    private boolean parseEXCSQLSTTobjects(DRDAStatement stmt) throws DRDAProtocolException, SQLException
    {
        int codePoint;
        boolean gotSQLDTA = false, gotEXTDTA = false;
        boolean result = false;
        do
        {
            correlationID = reader.readDssHeader();
            while (reader.moreDssData())
            {
                codePoint = reader.readLengthAndCodePoint( true );
                switch(codePoint)
                {
                    // optional
                    case CodePoint.TYPDEFNAM:
                        setStmtOrDbByteOrder(false, stmt, parseTYPDEFNAM());
//IC see: https://issues.apache.org/jira/browse/DERBY-1559
                        stmt.setTypDefValues();
                        break;
                    // optional
                    case CodePoint.TYPDEFOVR:
                        parseTYPDEFOVR(stmt);
                        stmt.setTypDefValues();
                        break;
                    // required
                    case CodePoint.SQLDTA:
                        parseSQLDTA(stmt);
                        gotSQLDTA = true;
                        break;
                    // optional
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
                    case CodePoint.EXTDTA:
//IC see: https://issues.apache.org/jira/browse/DERBY-1559
                        readAndSetAllExtParams(stmt, true);
                        stmt.ps.clearWarnings();
                        result = stmt.execute();
                        gotEXTDTA = true;
                        break;
                    // optional
                    case CodePoint.OUTOVR:
                        parseOUTOVR(stmt);
                        break;
                    default:
                        invalidCodePoint(codePoint);
                }
            }
        } while (reader.isChainedWithSameID());

        // SQLDTA is required
        if (!gotSQLDTA) {
            missingCodePoint(CodePoint.SQLDTA);
        }
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1559
        if (! gotEXTDTA) {
            stmt.ps.clearWarnings();
            result = stmt.execute();
        }
        
        return result;
    }

    /**
     * Write SQLCINRD - result set column information
     *
     * @throws DRDAProtocolException
     * @throws SQLException
     */
    private void writeSQLCINRD(DRDAStatement stmt) throws DRDAProtocolException,SQLException
    {
        ResultSet rs = stmt.getResultSet();

        writer.createDssObject();
        writer.startDdm(CodePoint.SQLCINRD);
        if (sqlamLevel >= MGRLVL_7) {
//IC see: https://issues.apache.org/jira/browse/DERBY-1984
            writeSQLDHROW(rs.getHoldability());
        }

        ResultSetMetaData rsmeta = rs.getMetaData();
        int ncols = rsmeta.getColumnCount();
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        writer.writeShort(ncols);   // num of columns
        if (sqlamLevel >= MGRLVL_7)
        {
            for (int i = 0; i < ncols; i++) {
                writeSQLDAGRP (rsmeta, null, i, true);
            }
        }
        else
        {
            for (int i = 0; i < ncols; i++)
            {
                writeVCMorVCS(rsmeta.getColumnName(i+1));
                writeVCMorVCS(rsmeta.getColumnLabel(i+1));
                writeVCMorVCS(null);
            }
        }
        writer.endDdmAndDss();
    }

    /**
     * Write SQLRSLRD - result set reply data
     *
     * @throws DRDAProtocolException
     * @throws SQLException
     */
    private void writeSQLRSLRD(DRDAStatement stmt) throws DRDAProtocolException,SQLException
    {
        int numResults = stmt.getNumResultSets();

        writer.createDssObject();
        writer.startDdm(CodePoint.SQLRSLRD);
        writer.writeShort(numResults); // num of result sets

        for (int i = 0; i < numResults; i ++)
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
                writer.writeInt(i); // rsLocator
                writeVCMorVCS(stmt.getResultSetCursorName(i));
                writer.writeInt(1); // num of rows XXX resolve, it doesn't matter for now

            }
        writer.endDdmAndDss();
    }

    /**
     * Write RSLSETRM
     * Instance variables
     *  SVRCOD - Severity code - Information only - required
     *  PKGSNLST - list of PKGNAMCSN -required
     *  SRVDGN - Server Diagnostic Information -optional
     *
     * @throws DRDAProtocolException
     * @throws SQLException
     */
    private void writeRSLSETRM(DRDAStatement stmt) throws DRDAProtocolException,SQLException
    {
        int numResults = stmt.getNumResultSets();
        writer.createDssReply();
        writer.startDdm(CodePoint.RSLSETRM);
        writer.writeScalar2Bytes(CodePoint.SVRCOD, 0);
        writer.startDdm(CodePoint.PKGSNLST);
        
        for (int i = 0; i < numResults; i++) {
//IC see: https://issues.apache.org/jira/browse/DERBY-212
            writePKGNAMCSN(stmt.getResultSetPkgcnstkn(i).getBytes());
        }
        writer.endDdm();
        writer.endDdmAndDss();
    }

    
    /**
     * Parse SQLDTA - SQL program variable data 
     * and handle exception.
     * @see #parseSQLDTA_work
     */

    private void parseSQLDTA(DRDAStatement stmt) throws DRDAProtocolException,SQLException
    {
        try {
            parseSQLDTA_work(stmt);
        } 
        catch (SQLException se)
        {
            skipRemainder(true);
            throw se;
        }
    }
    
    /**
     * Parse SQLDTA - SQL program variable data
     * Instance Variables
     *  FDODSC - FD:OCA data descriptor - required
     *  FDODTA - FD:OCA data - optional
     *    
     * @throws DRDAProtocolException
     * @throws SQLException
     */
    private void parseSQLDTA_work(DRDAStatement stmt) throws DRDAProtocolException,SQLException
    {
        String strVal;
//IC see: https://issues.apache.org/jira/browse/DERBY-6000
        EnginePreparedStatement ps = stmt.getPreparedStatement();
        int codePoint;
        ParameterMetaData pmeta = null;
//IC see: https://issues.apache.org/jira/browse/DERBY-2121

        // Clear params without releasing storage
        stmt.clearDrdaParams();
//IC see: https://issues.apache.org/jira/browse/DERBY-815

        int numVars = 0;
        boolean rtnParam = false;

        reader.markCollection();        
        codePoint = reader.getCodePoint();
        while (codePoint != -1)
        {
                switch (codePoint)
                {
                    // required
                    case CodePoint.FDODSC:
                        while (reader.getDdmLength() > 6) //we get parameter info til last 6 byte
                    {
                        int dtaGrpLen = reader.readUnsignedByte();
                        int numVarsInGrp = (dtaGrpLen - 3) / 3;
                        if (SanityManager.DEBUG) {
                            trace("num of vars in this group is: "+numVarsInGrp);
                        }
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
                        reader.readByte();      // tripletType
                        reader.readByte();      // id
                        for (int j = 0; j < numVarsInGrp; j++)
                        {
//IC see: https://issues.apache.org/jira/browse/DERBY-815
                            final byte t = reader.readByte();
                            if (SanityManager.DEBUG)  {
                                trace("drdaType is: "+ "0x" +
                                      Integer.toHexString(t));
                            }
                            int drdaLength = reader.readNetworkShort();
                            if (SanityManager.DEBUG) {
                                trace("drdaLength is: "+drdaLength);
                            }
                            stmt.addDrdaParam(t, drdaLength);
                        }
                    }
                    numVars = stmt.getDrdaParamCount();
                    if (SanityManager.DEBUG) {
                        trace("numVars = " + numVars);
                    }
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
                    if (ps == null)     // it is a CallableStatement under construction
                    {
                        StringBuilder marks = new StringBuilder();  // construct parameter marks
                        marks.append("(?");
                        for (int i = 1; i < numVars; i++) {
                            marks.append(", ?");
                        }
                        String prepareString = "call " + stmt.procName + marks.toString() + ")";
                        if (SanityManager.DEBUG) {
                            trace ("$$ prepareCall is: "+prepareString);
                        }
                        CallableStatement cs = null;
                        try {
                            cs = (CallableStatement)
                                stmt.prepare(prepareString);            
                            stmt.registerAllOutParams();
                        } catch (SQLException se) {
                            if (! stmt.outputExpected || 
                                (!se.getSQLState().equals(
                                    SQLState.LANG_NO_METHOD_FOUND))) {
                                throw se;
                            }
                            if (SanityManager.DEBUG) {
                                trace("****** second try with return parameter...");
                            }
                            // Save first SQLException most likely suspect
                            if (numVars == 1) {
                                prepareString = "? = call " + stmt.procName +"()";
                            } else {
                                prepareString = "? = call " + stmt.procName +"("+marks.substring(3) + ")";
                            }
                            if (SanityManager.DEBUG) {
                                trace ("$$ prepareCall is: "+prepareString);
                            }
                            try {
                                cs = (CallableStatement) stmt.prepare(prepareString);
                            } catch (SQLException se2)
                            {
                                // The first exception is the most likely suspect
                                throw se;
                            }
//IC see: https://issues.apache.org/jira/browse/DERBY-815
                            rtnParam = true;
                        }
//IC see: https://issues.apache.org/jira/browse/DERBY-6000
                        ps = (EnginePreparedStatement) cs;
                        stmt.ps = ps;
                    }

                    pmeta = stmt.getParameterMetaData();

                    reader.readBytes(6);    // descriptor footer
                    break;
                // optional
                case CodePoint.FDODTA:
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
                    reader.readByte();  // row indicator
                    for (int i = 0; i < numVars; i++)
                    {
                    
                        if ((stmt.getParamDRDAType(i+1) & 0x1) == 0x1)  // nullable
                        {
                            int nullData = reader.readUnsignedByte();
                            if ((nullData & 0xFF) == FdocaConstants.NULL_DATA)
                            {
                                if (SanityManager.DEBUG) {
                                    trace("******param null");
                                }
                                if (pmeta.getParameterMode(i + 1)
                                    != ParameterMetaData.parameterModeOut) {
                                        ps.setNull(i+1, pmeta.getParameterType(i+1));
                                }
                                if (stmt.isOutputParam(i+1)) {
                                    stmt.registerOutParam(i+1);
                                }
                                continue;
                            }
                        }

                        // not null, read and set it
//IC see: https://issues.apache.org/jira/browse/DERBY-815
                        readAndSetParams(i, stmt, pmeta);
                    }
                    break;
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
                case CodePoint.EXTDTA:
//IC see: https://issues.apache.org/jira/browse/DERBY-1559
//IC see: https://issues.apache.org/jira/browse/DERBY-1559
                    readAndSetAllExtParams(stmt, false);
                    break;
                default:
                    invalidCodePoint(codePoint);

            }
                codePoint = reader.getCodePoint();
        }


    }

    private int getByteOrder()
    {
        DRDAStatement stmt = database.getCurrentStatement();
        return ((stmt != null && stmt.typDefNam != null) ? stmt.byteOrder : database.byteOrder);
    }

    /** A cached {@code Calendar} instance using the GMT time zone. */
    private Calendar gmtCalendar;

    /**
     * Get a {@code Calendar} instance with time zone set to GMT. The instance
     * is cached for reuse by this thread. This calendar can be used to
     * consistently read and write date and time values using the same
     * calendar. Since the local default calendar may not be able to represent
     * all times (for instance because the time would fall into a non-existing
     * hour of the day when switching to daylight saving time, see DERBY-4582),
     * we use the GMT time zone which doesn't observe daylight saving time.
     *
     * @return a calendar in the GMT time zone
     */
    private Calendar getGMTCalendar() {
        if (gmtCalendar == null) {
            TimeZone gmt = TimeZone.getTimeZone("GMT");
            gmtCalendar = Calendar.getInstance(gmt);
        }
        return gmtCalendar;
    }

    /**
     * Read different types of input parameters and set them in
     * PreparedStatement
     * @param i         index of the parameter
     * @param stmt      drda statement
     * @param pmeta     parameter meta data
     *
     * @throws DRDAProtocolException
     * @throws SQLException
     */
    private void readAndSetParams(int i,
//IC see: https://issues.apache.org/jira/browse/DERBY-815
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
                                  DRDAStatement stmt,
//IC see: https://issues.apache.org/jira/browse/DERBY-2121
                                  ParameterMetaData pmeta)
                throws DRDAProtocolException, SQLException
    {
        PreparedStatement ps = stmt.getPreparedStatement();

        // mask out null indicator
        final int drdaType = ((stmt.getParamDRDAType(i+1) | 0x01) & 0xff);
        final int paramLenNumBytes = stmt.getParamLen(i+1);

        if (ps instanceof CallableStatement)
        {
            if (stmt.isOutputParam(i+1))
            {
                CallableStatement cs = (CallableStatement) ps;
                cs.registerOutParameter(i+1, stmt.getOutputParamType(i+1));
            }
        }

        switch (drdaType)
        {
            case DRDAConstants.DRDA_TYPE_NBOOLEAN:
            {
                boolean paramVal = (reader.readByte() == 1);
                if (SanityManager.DEBUG) {
                    trace("boolean parameter value is: " + paramVal);
                }
                ps.setBoolean(i+1, paramVal);
                break;
            }
//IC see: https://issues.apache.org/jira/browse/DERBY-499
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
            case DRDAConstants.DRDA_TYPE_NSMALL:
            {
                short paramVal = (short) reader.readShort(getByteOrder());
                if (SanityManager.DEBUG) {
                    trace("short parameter value is: "+paramVal);
                }
//IC see: https://issues.apache.org/jira/browse/DERBY-4889
                ps.setShort(i+1, paramVal);
                break;
            }
//IC see: https://issues.apache.org/jira/browse/DERBY-499
            case  DRDAConstants.DRDA_TYPE_NINTEGER:
            {
                int paramVal = reader.readInt(getByteOrder());
                if (SanityManager.DEBUG) {
                    trace("integer parameter value is: "+paramVal);
                }
                ps.setInt(i+1, paramVal);
                break;
            }
//IC see: https://issues.apache.org/jira/browse/DERBY-499
            case DRDAConstants.DRDA_TYPE_NINTEGER8:
            {
                long paramVal = reader.readLong(getByteOrder());
                if (SanityManager.DEBUG) {
                    trace("parameter value is: "+paramVal);
                }
                ps.setLong(i+1, paramVal);
                break;
            }
//IC see: https://issues.apache.org/jira/browse/DERBY-499
            case DRDAConstants.DRDA_TYPE_NFLOAT4:
            {
                float paramVal = reader.readFloat(getByteOrder());
                if (SanityManager.DEBUG) {
                    trace("parameter value is: "+paramVal);
                }
                ps.setFloat(i+1, paramVal);
                break;
            }
//IC see: https://issues.apache.org/jira/browse/DERBY-499
            case DRDAConstants.DRDA_TYPE_NFLOAT8:
            {
                double paramVal = reader.readDouble(getByteOrder());
                if (SanityManager.DEBUG) {
                    trace("nfloat8 parameter value is: "+paramVal);
                }
                ps.setDouble(i+1, paramVal);
                break;
            }
//IC see: https://issues.apache.org/jira/browse/DERBY-499
            case DRDAConstants.DRDA_TYPE_NDECIMAL:
            {
                int precision = (paramLenNumBytes >> 8) & 0xff;
                int scale = paramLenNumBytes & 0xff;
                BigDecimal paramVal = reader.readBigDecimal(precision, scale);
                if (SanityManager.DEBUG) {
                    trace("ndecimal parameter value is: "+paramVal);
                }
                ps.setBigDecimal(i+1, paramVal);
                break;
            }
//IC see: https://issues.apache.org/jira/browse/DERBY-499
            case DRDAConstants.DRDA_TYPE_NDATE:
            {
                String paramVal = reader.readStringData(10).trim();  //parameter may be char value
                if (SanityManager.DEBUG) {
                    trace("ndate parameter value is: \""+paramVal+"\"");
                }
//IC see: https://issues.apache.org/jira/browse/DERBY-157
                try {
                    Calendar cal = getGMTCalendar();
                    ps.setDate(i+1, parseDate(paramVal, cal), cal);
                } catch (java.lang.IllegalArgumentException e) {
                    // Just use SQLSTATE as message since, if user wants to
                    // retrieve it, the message will be looked up by the
                    // sqlcamessage() proc, which will get the localized
                    // message based on SQLSTATE, and will ignore the
                    // the message we use here...
                    throw new SQLException(SQLState.LANG_DATE_SYNTAX_EXCEPTION,
                        SQLState.LANG_DATE_SYNTAX_EXCEPTION.substring(0,5));
                }
                break;
            }
//IC see: https://issues.apache.org/jira/browse/DERBY-499
            case DRDAConstants.DRDA_TYPE_NTIME:
            {
                String paramVal = reader.readStringData(8).trim();  //parameter may be char value
                if (SanityManager.DEBUG) {
                    trace("ntime parameter value is: "+paramVal);
                }
                try {
                    Calendar cal = getGMTCalendar();
                    ps.setTime(i+1, parseTime(paramVal, cal), cal);
                } catch (java.lang.IllegalArgumentException e) {
                    throw new SQLException(SQLState.LANG_DATE_SYNTAX_EXCEPTION,
                        SQLState.LANG_DATE_SYNTAX_EXCEPTION.substring(0,5));
                }
                break;
            }
//IC see: https://issues.apache.org/jira/browse/DERBY-499
            case DRDAConstants.DRDA_TYPE_NTIMESTAMP:
            {
                // JCC represents ts in a slightly different format than Java standard, so
                // we do the conversion to Java standard here.
                int timestampLength = appRequester.getTimestampLength();
                
                String paramVal = reader.readStringData( timestampLength ).trim();  //parameter may be char value
                if (SanityManager.DEBUG) {
                    trace("ntimestamp parameter value is: "+paramVal);
                }
                try {
                    Calendar cal = getGMTCalendar();
                    ps.setTimestamp(i+1, parseTimestamp(paramVal, cal), cal);
                } catch (java.lang.IllegalArgumentException e1) {
                // thrown by parseTimestamp(...) for bad syntax...
                    throw new SQLException(SQLState.LANG_DATE_SYNTAX_EXCEPTION,
                        SQLState.LANG_DATE_SYNTAX_EXCEPTION.substring(0,5));
                }
                break;
            }
//IC see: https://issues.apache.org/jira/browse/DERBY-499
            case DRDAConstants.DRDA_TYPE_NCHAR:
            case DRDAConstants.DRDA_TYPE_NVARCHAR:
            case DRDAConstants.DRDA_TYPE_NLONG:
            case DRDAConstants.DRDA_TYPE_NVARMIX:
            case DRDAConstants.DRDA_TYPE_NLONGMIX:
            {
                String paramVal = reader.readLDStringData(stmt.ccsidMBCEncoding);
                if (SanityManager.DEBUG) {
                    trace("char/varchar parameter value is: "+paramVal);
                }
                ps.setString(i+1, paramVal);
                break;
            }
//IC see: https://issues.apache.org/jira/browse/DERBY-499
            case  DRDAConstants.DRDA_TYPE_NFIXBYTE:
            {
                byte[] paramVal = reader.readBytes();
                if (SanityManager.DEBUG) {
                    trace("fix bytes parameter value is: "+ convertToHexString(paramVal));
                }
                ps.setBytes(i+1, paramVal);
                break;
            }
            case DRDAConstants.DRDA_TYPE_NVARBYTE:
            case DRDAConstants.DRDA_TYPE_NLONGVARBYTE:
            {
                int length = reader.readNetworkShort(); //protocol control data always follows big endian
                if (SanityManager.DEBUG) {
                    trace("===== binary param length is: " + length);
                }
                byte[] paramVal = reader.readBytes(length);
                ps.setBytes(i+1, paramVal);
                break;
            }
            case DRDAConstants.DRDA_TYPE_NUDT:
            {
                Object paramVal = readUDT();
                ps.setObject(i+1, paramVal);
                break;
            }
//IC see: https://issues.apache.org/jira/browse/DERBY-499
            case DRDAConstants.DRDA_TYPE_NLOBBYTES:
            case DRDAConstants.DRDA_TYPE_NLOBCMIXED:
            case DRDAConstants.DRDA_TYPE_NLOBCSBCS:
            case DRDAConstants.DRDA_TYPE_NLOBCDBCS:
             {
                 long length = readLobLength(paramLenNumBytes);
                 if (length != 0) //can be -1 for CLI if "data at exec" mode, see clifp/exec test
                 {
//IC see: https://issues.apache.org/jira/browse/DERBY-815
                    stmt.addExtPosition(i);
                 }
                 else   /* empty */
                 {
                     if (drdaType == DRDAConstants.DRDA_TYPE_NLOBBYTES) {
                         ps.setBytes(i + 1, new byte[0]);
                     } else {
                         ps.setString(i + 1, "");
                     }
                 }
                 break;
             }
//IC see: https://issues.apache.org/jira/browse/DERBY-2506
                        case DRDAConstants.DRDA_TYPE_NLOBLOC:
                        {
                            //read the locator value
                            int paramVal = reader.readInt(getByteOrder());
                            
                            if (SanityManager.DEBUG) {
                                trace("locator value is: "+paramVal);
                            }
                            
                            //Map the locator value to the Blob object in the
                            //Hash map.
                            java.sql.Blob blobFromLocator = (java.sql.Blob)
                            database.getConnection().getLOBMapping(paramVal);
                            
                            //set the PreparedStatement parameter to the mapped
                            //Blob object.
                            ps.setBlob(i+1, blobFromLocator);
                            break;
                        }
                        case DRDAConstants.DRDA_TYPE_NCLOBLOC:
                        {
                            //read the locator value.
                            int paramVal = reader.readInt(getByteOrder());
                            
                            if (SanityManager.DEBUG) {
                                trace("locator value is: "+paramVal);
                            }
                            
                            //Map the locator value to the Clob object in the
                            //Hash Map.
                            java.sql.Clob clobFromLocator = (java.sql.Clob)
                            database.getConnection().getLOBMapping(paramVal);
                            
                            //set the PreparedStatement parameter to the mapped
                            //Clob object.
                            ps.setClob(i+1, clobFromLocator);
                            break;
                        }
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
            default:
                {
                String paramVal = reader.readLDStringData(stmt.ccsidMBCEncoding);
                if (SanityManager.DEBUG) {
                    trace("default type parameter value is: "+paramVal);
                }
                ps.setObject(i+1, paramVal);
            }
        }
    }

    /** Read a UDT from the stream */
    private Object readUDT() throws DRDAProtocolException
    {
        int length = reader.readNetworkShort(); //protocol control data always follows big endian
        if (SanityManager.DEBUG) { trace("===== udt param length is: " + length); }
        byte[] bytes = reader.readBytes(length);
        
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream( bytes );
            ObjectInputStream ois = new ObjectInputStream( bais );

            return ois.readObject();
        }
        catch (Exception e)
        {
            markCommunicationsFailure
                ( e,"DRDAConnThread.readUDT()", "", e.getMessage(), "*" );
            return null;
        }
    }


    private long readLobLength(int extLenIndicator) 
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        throws DRDAProtocolException
    {
        switch (extLenIndicator)
        {
            case 0x8002:
                return (long) reader.readNetworkShort();
            case 0x8004:
                return (long) reader.readNetworkInt();
            case 0x8006:
                return (long) reader.readNetworkSixByteLong();
            case 0x8008:
                return (long) reader.readNetworkLong();
            default:
                throwSyntaxrm(CodePoint.SYNERRCD_INCORRECT_EXTENDED_LEN, extLenIndicator);
                return 0L;
        }
        


    }

    /**
     * Parse a date string as it is received from the client.
     * See org.apache.derby.client.am.DateTime#dateToDateBytes
     *
     * @param dateString the date string to parse
     * @param cal the calendar in which the date is parsed
     * @return a Date object representing the date in the specified calendar
     * @throws IllegalArgumentException if the date is not correctly formatted
     */
    private java.sql.Date parseDate(String dateString, Calendar cal) {
        // Get each component out of YYYY-MM-DD
        String[] components = dateString.split("-");
        if (components.length != 3) {
            throw new IllegalArgumentException();
        }

        cal.clear();

        // Set date components
        cal.set(Calendar.YEAR, Integer.parseInt(components[0]));
        cal.set(Calendar.MONTH, Integer.parseInt(components[1]) - 1);
        cal.set(Calendar.DAY_OF_MONTH, Integer.parseInt(components[2]));

        // Normalize time components as specified by java.sql.Date
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        return new java.sql.Date(cal.getTimeInMillis());
    }

    /**
     * Parse a time string as it is received from the client.
     * See org.apache.derby.client.am.DateTime#timeToTimeBytes
     *
     * @param timeString the time string to parse
     * @param cal the calendar in which the time is parsed
     * @return a Date object representing the time in the specified calendar
     * @throws IllegalArgumentException if the time is not correctly formatted
     */
    private Time parseTime(String timeString, Calendar cal) {
        // Get each component out of HH:MM:SS
        String[] components = timeString.split(":");
        if (components.length != 3) {
            throw new IllegalArgumentException();
        }

        cal.clear();

        // Normalize date components as specified by java.sql.Time
        cal.set(Calendar.YEAR, 1970);
        cal.set(Calendar.MONTH, Calendar.JANUARY);
        cal.set(Calendar.DAY_OF_MONTH, 1);

        // Set time components
        cal.set(Calendar.HOUR_OF_DAY, Integer.parseInt(components[0]));
        cal.set(Calendar.MINUTE, Integer.parseInt(components[1]));
        cal.set(Calendar.SECOND, Integer.parseInt(components[2]));

        // No millisecond resolution for Time
        cal.set(Calendar.MILLISECOND, 0);

        return new Time(cal.getTimeInMillis());
    }

    /**
     * Parse a timestamp string as it is received from the client.
     * See org.apache.derby.client.am.DateTime#timestampToTimestampBytes
     *
     * @param timeString the time string to parse
     * @param cal the calendar in which the timestamp is parsed
     * @return a Date object representing the timestamp in the specified
     * calendar
     * @throws IllegalArgumentException if the timestamp is not correctly
     * formatted
     */
    private Timestamp parseTimestamp(String timeString, Calendar cal) {
        // Get each component out of YYYY-MM-DD-HH.MM.SS.fffffffff
        String[] components = timeString.split("[-.]");
        if (components.length != 7) {
            throw new IllegalArgumentException();
        }

        cal.clear();
        cal.set(Calendar.YEAR, Integer.parseInt(components[0]));
        cal.set(Calendar.MONTH, Integer.parseInt(components[1]) - 1);
        cal.set(Calendar.DAY_OF_MONTH, Integer.parseInt(components[2]));
        cal.set(Calendar.HOUR_OF_DAY, Integer.parseInt(components[3]));
        cal.set(Calendar.MINUTE, Integer.parseInt(components[4]));
        cal.set(Calendar.SECOND, Integer.parseInt(components[5]));

        int nanos = 0;

        final int radix = 10;
        String nanoString = components[6];

        // Get up to nine digits from the nano second component
        for (int i = 0; i < 9; i++) {
            // Scale up the intermediate result
            nanos *= radix;

            // Add the next digit, if there is one. Continue the loop even if
            // there are no more digits, since we still need to scale up the
            // intermediate result as if the fraction part were padded with
            // zeros.
            if (i < nanoString.length()) {
                int digit = Character.digit(nanoString.charAt(i), radix);
                if (digit == -1) {
                    // not a digit
                    throw new IllegalArgumentException();
                }
                nanos += digit;
            }
        }

        Timestamp ts = new Timestamp(cal.getTimeInMillis());
        ts.setNanos(nanos);
        return ts;
    }

    private void readAndSetAllExtParams(final DRDAStatement stmt, final boolean streamLOB) 
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        throws SQLException, DRDAProtocolException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-815
        final int numExt = stmt.getExtPositionCount();
        for (int i = 0; i < numExt; i++)
                    {
                        int paramPos = stmt.getExtPosition(i);
                        // Only the last EXTDTA is streamed.  This is because all of 
                        // the parameters have to be set before execution and are 
                        // consecutive in the network server stream, so only the last
                        // one can be streamed.
                        final boolean doStreamLOB = (streamLOB && i == numExt -1);
                        readAndSetExtParam(paramPos,
                                           stmt,
                                           stmt.getParamDRDAType(paramPos+1),
                                           stmt.getParamLen(paramPos+1),
                                           doStreamLOB);
                        // Each extdta in it's own dss
                        if (i < numExt -1)
                        {
                            correlationID = reader.readDssHeader();
                            int codePoint = reader.readLengthAndCodePoint( true );
                        }
                    }

    }
    

    /**
     * Read different types of input parameters and set them in PreparedStatement
     * @param i zero-based index of the parameter
     * @param stmt          associated ps
     * @param drdaType  drda type of the parameter
     *
     * @throws DRDAProtocolException
     * @throws SQLException
     */
    private void readAndSetExtParam( int i, DRDAStatement stmt,
                                     int drdaType, int extLen, boolean streamLOB)
                throws DRDAProtocolException, SQLException
        {
            // Note the switch from zero-based to one-based index below.
            drdaType = (drdaType & 0x000000ff); // need unsigned value
            boolean checkNullability = false;
            if (sqlamLevel >= MGRLVL_7 && FdocaConstants.isNullable(drdaType)) {
                checkNullability = true;
            }

//IC see: https://issues.apache.org/jira/browse/DERBY-2017
            final EXTDTAReaderInputStream stream =
                reader.getEXTDTAReaderInputStream(checkNullability);

            // Determine encoding first, mostly for debug/tracing purposes
            String encoding = "na";
            switch (drdaType) {
                case DRDAConstants.DRDA_TYPE_LOBCSBCS:
                case DRDAConstants.DRDA_TYPE_NLOBCSBCS:
                    encoding = stmt.ccsidSBCEncoding;
                    break;
                case DRDAConstants.DRDA_TYPE_LOBCDBCS:
                case DRDAConstants.DRDA_TYPE_NLOBCDBCS:
                    encoding = stmt.ccsidDBCEncoding;
                    break;
                case DRDAConstants.DRDA_TYPE_LOBCMIXED:
                case DRDAConstants.DRDA_TYPE_NLOBCMIXED:
                    encoding = stmt.ccsidMBCEncoding;
                    break;
            }

            traceEXTDTARead(drdaType, i+1, stream, streamLOB, encoding);

//IC see: https://issues.apache.org/jira/browse/DERBY-5896
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
            try {
                switch (drdaType)
                {
//IC see: https://issues.apache.org/jira/browse/DERBY-499
                    case  DRDAConstants.DRDA_TYPE_LOBBYTES:
                    case  DRDAConstants.DRDA_TYPE_NLOBBYTES:
                        setAsBinaryStream(stmt, i+1, stream, streamLOB);
                        break;
                    case DRDAConstants.DRDA_TYPE_LOBCSBCS:
                    case DRDAConstants.DRDA_TYPE_NLOBCSBCS:
                    case DRDAConstants.DRDA_TYPE_LOBCDBCS:
                    case DRDAConstants.DRDA_TYPE_NLOBCDBCS:
                    case DRDAConstants.DRDA_TYPE_LOBCMIXED:
                    case DRDAConstants.DRDA_TYPE_NLOBCMIXED:
                        setAsCharacterStream(stmt, i+1, stream, streamLOB,
                                encoding);
                        break;
                    default:
                        invalidValue(drdaType);
                }
                 
            }
            catch (java.io.UnsupportedEncodingException e) {
                throw new SQLException (e.getMessage());
                
            } catch( IOException e ){
                throw new SQLException ( e.getMessage() );
                
            }
        }

    /**
     * Parse EXCSQLIMM - Execute Immediate Statement
     * Instance Variables
     *  RDBNAM - relational database name - optional
     *  PKGNAMCSN - RDB Package Name, Consistency Token and Section Number - required
     *  RDBCMTOK - RDB Commit Allowed - optional
     *  MONITOR - Monitor Events - optional
     *
     * Command Objects
     *  TYPDEFNAM - Data Type Definition Name - optional
     *  TYPDEFOVR - TYPDEF Overrides -optional
     *  SQLSTT - SQL Statement -required
     *
     * @return update count
     * @throws DRDAProtocolException
     * @throws SQLException
     */
    private long parseEXCSQLIMM() throws DRDAProtocolException,SQLException
    {
        int codePoint;
        reader.markCollection();
//IC see: https://issues.apache.org/jira/browse/DERBY-212
        Pkgnamcsn pkgnamcsn = null;
        codePoint = reader.getCodePoint();
        while (codePoint != -1)
        {
            switch (codePoint)
            {
                // optional
                case CodePoint.RDBNAM:
                    setDatabase(CodePoint.EXCSQLIMM);
                    break;
                // required
                case CodePoint.PKGNAMCSN:
                    pkgnamcsn = parsePKGNAMCSN();
                    break;
                case CodePoint.RDBCMTOK:
                    parseRDBCMTOK();
                    break;
                //optional
                case CodePoint.MONITOR:
                    parseMONITOR();
                    break;
                default:
                    invalidCodePoint(codePoint);

            }
            codePoint = reader.getCodePoint();
        }
        DRDAStatement drdaStmt =  database.getDefaultStatement(pkgnamcsn);
        // initialize statement for reuse
        drdaStmt.initialize();
        String sqlStmt = parseEXECSQLIMMobjects();
//IC see: https://issues.apache.org/jira/browse/DERBY-6000
        EngineStatement statement = drdaStmt.getStatement();
        statement.clearWarnings();
        if (pendingStatementTimeout >= 0) {
            statement.setQueryTimeout(pendingStatementTimeout);
            pendingStatementTimeout = -1;
        }
        long updCount = statement.executeLargeUpdate(sqlStmt);
        return updCount;
    }

    /**
     * Parse EXCSQLSET - Execute Set SQL Environment
     * Instance Variables
     *  RDBNAM - relational database name - optional
     *  PKGNAMCT - RDB Package Name, Consistency Token  - optional
     *  MONITOR - Monitor Events - optional
     *
     * Command Objects
     *  TYPDEFNAM - Data Type Definition Name - required
     *  TYPDEFOVR - TYPDEF Overrides - required
     *  SQLSTT - SQL Statement - required (at least one; may be more)
     *
     * @throws DRDAProtocolException
     * @throws SQLException
     */
    private boolean parseEXCSQLSET() throws DRDAProtocolException,SQLException
    {

        int codePoint;
        reader.markCollection();


        codePoint = reader.getCodePoint();
        while (codePoint != -1)
        {
            switch (codePoint)
            {
                // optional
                case CodePoint.RDBNAM:
                    setDatabase(CodePoint.EXCSQLSET);
                    break;
                // optional
                case CodePoint.PKGNAMCT:
                    // we are going to ignore this for EXCSQLSET
                    // since we are just going to reuse an existing statement
                    String pkgnamct = parsePKGNAMCT();
                    break;
                // optional
                case CodePoint.MONITOR:
                    parseMONITOR();
                    break;
                // required
                case CodePoint.PKGNAMCSN:
                    // we are going to ignore this for EXCSQLSET.
                    // since we are just going to reuse an existing statement.
                    // NOTE: This codepoint is not in the DDM spec for 'EXCSQLSET',
                    // but since it DOES get sent by jcc1.2, we have to have
                    // a case for it...
//IC see: https://issues.apache.org/jira/browse/DERBY-212
                    Pkgnamcsn pkgnamcsn = parsePKGNAMCSN();
                    break;
                default:
                    invalidCodePoint(codePoint);

            }
            codePoint = reader.getCodePoint();
        }

        parseEXCSQLSETobjects();
        return true;
    }

    /**
     * Parse EXCSQLIMM objects
     * Objects
     *  TYPDEFNAM - Data type definition name - optional
     *  TYPDEFOVR - Type defintion overrides
     *  SQLSTT - SQL Statement required
     *
     * If TYPDEFNAM and TYPDEFOVR are supplied, they apply to the objects
     * sent with the statement.  Once the statement is over, the default values
     * sent in the ACCRDB are once again in effect.  If no values are supplied,
     * the values sent in the ACCRDB are used.
     * Objects may follow in one DSS or in several DSS chained together.
     * 
     * @return SQL Statement
     * @throws DRDAProtocolException
     * @throws SQLException
     */
    private String parseEXECSQLIMMobjects() throws DRDAProtocolException, SQLException
    {
        String sqlStmt = null;
        int codePoint;
        DRDAStatement stmt = database.getDefaultStatement();
        do
        {
            correlationID = reader.readDssHeader();
            while (reader.moreDssData())
            {
                codePoint = reader.readLengthAndCodePoint( false );
                switch(codePoint)
                {
                    // optional
                    case CodePoint.TYPDEFNAM:
                        setStmtOrDbByteOrder(false, stmt, parseTYPDEFNAM());
                        break;
                    // optional
                    case CodePoint.TYPDEFOVR:
                        parseTYPDEFOVR(stmt);
                        break;
                    // required
                    case CodePoint.SQLSTT:
                        sqlStmt = parseEncodedString();
                        if (SanityManager.DEBUG) {
                            trace("sqlStmt = " + sqlStmt);
                        }
                        break;
                    default:
                        invalidCodePoint(codePoint);
                }
            }
        } while (reader.isChainedWithSameID());

        // SQLSTT is required
        if (sqlStmt == null) {
            missingCodePoint(CodePoint.SQLSTT);
        }
        return sqlStmt;
    }

    /**
     * Parse EXCSQLSET objects
     * Objects
     *  TYPDEFNAM - Data type definition name - optional
     *  TYPDEFOVR - Type defintion overrides - optional
     *  SQLSTT - SQL Statement - required (a list of at least one)
     *
     * Objects may follow in one DSS or in several DSS chained together.
     * 
     * @throws DRDAProtocolException
     * @throws SQLException
     */
    private void parseEXCSQLSETobjects()
        throws DRDAProtocolException, SQLException
    {

        boolean gotSqlStt = false;
        boolean hadUnrecognizedStmt = false;

        DRDAStatement drdaStmt = database.getDefaultStatement();
        drdaStmt.initialize();

        do
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
            correlationID = reader.readDssHeader();
            while (reader.moreDssData())
            {

                int codePoint = reader.readLengthAndCodePoint(false);

                switch(codePoint)
                {
                    // optional
                    case CodePoint.TYPDEFNAM:
                        setStmtOrDbByteOrder(false, drdaStmt, parseTYPDEFNAM());
                        break;
                    // optional
                    case CodePoint.TYPDEFOVR:
                        parseTYPDEFOVR(drdaStmt);
                        break;
                    // required
                    case CodePoint.SQLSTT:
                        String sqlStmt = parseEncodedString();
                        if (sqlStmt != null) {
                        // then we have at least one SQL Statement.
                            gotSqlStt = true;
                        }

//IC see: https://issues.apache.org/jira/browse/DERBY-506
                        if (sqlStmt.startsWith(TIMEOUT_STATEMENT)) {
                            String timeoutString = sqlStmt.substring(TIMEOUT_STATEMENT.length());
//IC see: https://issues.apache.org/jira/browse/DERBY-5053
                            pendingStatementTimeout = Integer.parseInt(timeoutString);
                            break;
                        }

//IC see: https://issues.apache.org/jira/browse/DERBY-5896
                        if (canIgnoreStmt(sqlStmt)) {
                        // We _know_ Derby doesn't recognize this
                        // statement; don't bother trying to execute it.
                        // NOTE: at time of writing, this only applies
                        // to "SET CLIENT" commands, and it was decided
                        // that throwing a Warning for these commands
                        // would confuse people, so even though the DDM
                        // spec says to do so, we choose not to (but
                        // only for SET CLIENT cases).  If this changes
                        // at some point in the future, simply remove
                        // the follwing line; we will then throw a
                        // warning.
//                          hadUnrecognizedStmt = true;
                            break;
                        }

                        if (SanityManager.DEBUG) {
                            trace("sqlStmt = " + sqlStmt);
                        }

                        // initialize statement for reuse
                        drdaStmt.initialize();
                        drdaStmt.getStatement().clearWarnings();
                        try {
                            drdaStmt.getStatement().executeUpdate(sqlStmt);
                        } catch (SQLException e) {

                            // if this is a syntax error, then we take it
                            // to mean that the given SET statement is not
                            // recognized; take note (so we can throw a
                            // warning later), but don't interfere otherwise.
                            if (e.getSQLState().equals(SYNTAX_ERR)) {
                                hadUnrecognizedStmt = true;
                            } else {
                                // something else; assume it's serious.
                                throw e;
                            }
                        }
                        break;
                    default:
                        invalidCodePoint(codePoint);
                }
            }

        } while (reader.isChainedWithSameID());

        // SQLSTT is required.
        if (!gotSqlStt) {
            missingCodePoint(CodePoint.SQLSTT);
        }

        // Now that we've processed all SET statements (assuming no
        // severe exceptions), check for warnings and, if we had any,
        // note this in the SQLCARD reply object (but DON'T cause the
        // EXCSQLSET statement to fail).
        if (hadUnrecognizedStmt) {
            SQLWarning warn = new SQLWarning("One or more SET statements " +
                "not recognized.", "01000");
            throw warn;
        } // end if.
    }

    private boolean canIgnoreStmt(String stmt)
    {
        return (stmt.indexOf("SET CLIENT") != -1);
    }

    /**
     * Write RDBUPDRM
     * Instance variables
     *  SVRCOD - Severity code - Information only - required
     *  RDBNAM - Relational database name -required
     *  SRVDGN - Server Diagnostic Information -optional
     *
     * @exception DRDAProtocolException
     */
    private void writeRDBUPDRM() throws DRDAProtocolException
    {
        database.RDBUPDRM_sent = true;
        writer.createDssReply();
        writer.startDdm(CodePoint.RDBUPDRM);
        writer.writeScalar2Bytes(CodePoint.SVRCOD, CodePoint.SVRCOD_INFO);
        writeRDBNAM(database.getDatabaseName());
        writer.endDdmAndDss();
    }


    private String parsePKGNAMCT() throws DRDAProtocolException
    {
        reader.skipBytes();
        return null;
    }

    /**
     * Parse PKGNAMCSN - RDB Package Name, Consistency Token, and Section Number
     * Instance Variables
     *   NAMESYMDR - database name - not validated
     *   RDBCOLID - RDB Collection Identifier
     *   PKGID - RDB Package Identifier
     *   PKGCNSTKN - RDB Package Consistency Token
     *   PKGSN - RDB Package Section Number
     *
     * @return <code>Pkgnamcsn</code> value
     * @throws DRDAProtocolException
     */
    private Pkgnamcsn parsePKGNAMCSN() throws DRDAProtocolException
    {
        if (reader.getDdmLength() == CodePoint.PKGNAMCSN_LEN)
        {
            // This is a scalar object with the following fields
//IC see: https://issues.apache.org/jira/browse/DERBY-212
            reader.readString(rdbnam, CodePoint.RDBNAM_LEN, true);
            if (SanityManager.DEBUG) {
                trace("rdbnam = " + rdbnam);
            }
                        
            // A check that the rdbnam field corresponds to a database
            // specified in a ACCRDB term.
            // The check is not performed if the client is DNC_CLIENT
            // with version before 10.3.0 because these clients
            // are broken and send incorrect database name
            // if multiple connections to different databases
            // are created
                        
            // This check was added because of DERBY-1434  
            // check the client version first
//IC see: https://issues.apache.org/jira/browse/DERBY-5565
            if (appRequester.greaterThanOrEqualTo(10,3,0) ) {
                // check the database name
                if (!rdbnam.toString().equals(database.getDatabaseName())) {
                    rdbnamMismatch(CodePoint.PKGNAMCSN);
                }
            }

//IC see: https://issues.apache.org/jira/browse/DERBY-5896
            reader.readString(rdbcolid, CodePoint.RDBCOLID_LEN, true);
            if (SanityManager.DEBUG)  {
                trace("rdbcolid = " + rdbcolid);
            }

            reader.readString(pkgid, CodePoint.PKGID_LEN, true);
            if (SanityManager.DEBUG) {
                trace("pkgid = " + pkgid);
            }

            // we need to use the same UCS2 encoding, as this can be
            // bounced back to jcc (or keep the byte array)
            reader.readString(pkgcnstkn, CodePoint.PKGCNSTKN_LEN, false);
            if (SanityManager.DEBUG) {
                trace("pkgcnstkn = " + pkgcnstkn);
            }

            pkgsn = reader.readNetworkShort();
            if (SanityManager.DEBUG) {
                trace("pkgsn = " + pkgsn);
            }
        }
        else    // extended format
        {
            int length = reader.readNetworkShort();
            if (length < CodePoint.RDBNAM_LEN || length > CodePoint.RDBNAM_MAX_NAME) {
                badObjectLength(CodePoint.RDBNAM);
            }
            reader.readString(rdbnam, length, true);
            if (SanityManager.DEBUG) {
                trace("rdbnam = " + rdbnam);
            }

            // A check that the rdbnam field corresponds to a database
            // specified in a ACCRDB term.
            // The check is not performed if the client is DNC_CLIENT
            // with version before 10.3.0 because these clients
            // are broken and send incorrect database name
            // if multiple connections to different databases
            // are created
                        
            // This check was added because of DERBY-1434
                        
            // check the client version first
            if ( appRequester.getClientType() != AppRequester.DNC_CLIENT
                 || appRequester.greaterThanOrEqualTo(10,3,0) ) {
                // check the database name
                if (!rdbnam.toString().equals(database.getDatabaseName())) {
                    rdbnamMismatch(CodePoint.PKGNAMCSN);
                }
            }

            //RDBCOLID can be variable length in this format
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
            length = reader.readNetworkShort();
            reader.readString(rdbcolid, length, true);
            if (SanityManager.DEBUG) {
                trace("rdbcolid = " + rdbcolid);
            }

            length = reader.readNetworkShort();
            if (length != CodePoint.PKGID_LEN) {
                badObjectLength(CodePoint.PKGID);
            }
            reader.readString(pkgid, CodePoint.PKGID_LEN, true);
            if (SanityManager.DEBUG) {
                trace("pkgid = " + pkgid);
            }

            reader.readString(pkgcnstkn, CodePoint.PKGCNSTKN_LEN, false);
            if (SanityManager.DEBUG) {
                trace("pkgcnstkn = " + pkgcnstkn);
            }

            pkgsn = reader.readNetworkShort();
            if (SanityManager.DEBUG) {
                trace("pkgsn = " + pkgsn);
            }
        }

        // In most cases, the pkgnamcsn object is equal to the
        // previously returned object. To avoid allocation of a new
        // object in these cases, we first check to see if the old
        // object can be reused.
        if ((prevPkgnamcsn == null) ||
            rdbnam.wasModified() ||
            rdbcolid.wasModified() ||
            pkgid.wasModified() ||
            pkgcnstkn.wasModified() ||
            (prevPkgnamcsn.getPkgsn() != pkgsn))
        {
            // The byte array returned by pkgcnstkn.getBytes() might
            // be modified by DDMReader.readString() later, so we have
            // to create a copy of the array.
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
            byte[] token = new byte[pkgcnstkn.length()];
            System.arraycopy(pkgcnstkn.getBytes(), 0, token, 0, token.length);

            prevPkgnamcsn =
                new Pkgnamcsn(rdbnam.toString(), rdbcolid.toString(),
                              pkgid.toString(), pkgsn,
                              new ConsistencyToken(token));
        }

        return prevPkgnamcsn;
    }

    /**
     * Parse SQLSTT Dss
     * @exception DRDAProtocolException
     */
    private String parseSQLSTTDss() throws DRDAProtocolException
    {
        correlationID = reader.readDssHeader();
        int codePoint = reader.readLengthAndCodePoint( false );
        String strVal = parseEncodedString();
        if (SanityManager.DEBUG) {
            trace("SQL Statement = " + strVal);
        }
        return strVal;
    }

    /**
     * Parse an encoded data string from the Application Requester
     *
     * @return string value
     * @exception DRDAProtocolException
     */
    private String parseEncodedString() throws DRDAProtocolException
    {
        return (sqlamLevel < 7) ? parseVCMorVCS() : parseNOCMorNOCS();
    }

    /**
     * Parse variable character mixed byte or variable character single byte
     * Format
     *  I2 - VCM Length
     *  N bytes - VCM value
     *  I2 - VCS Length
     *  N bytes - VCS value 
     * Only 1 of VCM length or VCS length can be non-zero
     *
     * @return string value
     */
    private String parseVCMorVCS() throws DRDAProtocolException
    {
        String strVal = null;
        int vcm_length = reader.readNetworkShort();
        if (vcm_length > 0) {
            strVal = parseCcsidMBC(vcm_length);
        }
        int vcs_length = reader.readNetworkShort();
        if (vcs_length > 0)
        {
            if (strVal != null) {
                agentError ("Both VCM and VCS have lengths > 0");
            }
            strVal = parseCcsidSBC(vcs_length);
        }
        return strVal;
    }
    /**
     * Parse nullable character mixed byte or nullable character single byte
     * Format
     *  1 byte - null indicator
     *  I4 - mixed character length
     *  N bytes - mixed character string
     *  1 byte - null indicator
     *  I4 - single character length
     *  N bytes - single character length string
     *
     * @return string value
     * @exception DRDAProtocolException
     */
    private String parseNOCMorNOCS() throws DRDAProtocolException
    {
        byte nocm_nullByte = reader.readByte();
        String strVal = null;
        int length;
        if (nocm_nullByte != NULL_VALUE)
        {
            length = reader.readNetworkInt();
            strVal = parseCcsidMBC(length);
        }
        byte nocs_nullByte = reader.readByte();
        if (nocs_nullByte != NULL_VALUE)
        {
            if (strVal != null) {
                agentError("Both CM and CS are non null");
            }
            length = reader.readNetworkInt();
            strVal = parseCcsidSBC(length);
        }
        return strVal;
    }
    /**
     * Parse mixed character string
     * 
     * @return string value
     * @exception DRDAProtocolException
     */
    private String parseCcsidMBC(int length) throws DRDAProtocolException
    {
        String strVal = null;
        DRDAStatement  currentStatement;

        currentStatement = database.getCurrentStatement();
        if (currentStatement == null)
        {
            currentStatement = database.getDefaultStatement();
            currentStatement.initialize();
        }
        String ccsidMBCEncoding = currentStatement.ccsidMBCEncoding;

        if (length == 0) {
            // Can't return null here as that will indicate that the cp is 
            // missing, when it in fact was present, but contained an empty string
//IC see: https://issues.apache.org/jira/browse/DERBY-5806
            return ""; 
        }
        byte [] byteStr = reader.readBytes(length);
        if (ccsidMBCEncoding != null)
        {
            try {
                strVal = new String(byteStr, 0, length, ccsidMBCEncoding);
            } catch (UnsupportedEncodingException e) {
                agentError("Unsupported encoding " + ccsidMBCEncoding +
                    "in parseCcsidMBC");
            }
        }
        else
        {
            agentError("Attempt to decode mixed byte string without CCSID being set");
        }
        return strVal;
    }
    /**
     * Parse single byte character string
     * 
     * @return string value
     * @exception DRDAProtocolException
     */
    private String parseCcsidSBC(int length) throws DRDAProtocolException
    {
        String strVal = null;
//IC see: https://issues.apache.org/jira/browse/DERBY-706
        DRDAStatement  currentStatement;
        
        currentStatement = database.getCurrentStatement();
        if (currentStatement == null)
        {
            currentStatement = database.getDefaultStatement();
            currentStatement.initialize();
        }
        String ccsidSBCEncoding = currentStatement.ccsidSBCEncoding;
        System.out.println("ccsidSBCEncoding - " + ccsidSBCEncoding);
        
        if (length == 0) {
            return null;
        }
        byte [] byteStr = reader.readBytes(length);
        if (ccsidSBCEncoding != null)
        {
            try {
                strVal = new String(byteStr, 0, length, ccsidSBCEncoding);
            } catch (UnsupportedEncodingException e) {
                agentError("Unsupported encoding " + ccsidSBCEncoding +
                    "in parseCcsidSBC");
            }
        }
        else
        {
            agentError("Attempt to decode single byte string without CCSID being set");
        }
        return strVal;
    }
    /**
     * Parse CLSQRY
     * Instance Variables
     *  RDBNAM - relational database name - optional
     *  PKGNAMCSN - RDB Package Name, Consistency Token and Section Number - required
     *  QRYINSID - Query Instance Identifier - required - level 7
     *  MONITOR - Monitor events - optional.
     *
     * @return DRDAstatement being closed
     * @throws DRDAProtocolException
     * @throws SQLException
     */
    private DRDAStatement parseCLSQRY() throws DRDAProtocolException, SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-212
//IC see: https://issues.apache.org/jira/browse/DERBY-212
        Pkgnamcsn pkgnamcsn = null;
        reader.markCollection();
        long qryinsid = 0;
        boolean gotQryinsid = false;

        int codePoint = reader.getCodePoint();
        while (codePoint != -1)
        {
            switch (codePoint)
            {
                // optional
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
                case CodePoint.RDBNAM:
                    setDatabase(CodePoint.CLSQRY);
                    break;
                    // required
                case CodePoint.PKGNAMCSN:
                    pkgnamcsn = parsePKGNAMCSN();
                    break;
                case CodePoint.QRYINSID:
                    qryinsid = reader.readNetworkLong();
                    gotQryinsid = true;
                    break;
                // optional
                case CodePoint.MONITOR:
                    parseMONITOR();
                    break;
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
                default:
                    invalidCodePoint(codePoint);
            }
            codePoint = reader.getCodePoint();
        }
        // check for required variables
        if (pkgnamcsn == null) {
            missingCodePoint(CodePoint.PKGNAMCSN);
        }
        if (sqlamLevel >= MGRLVL_7 && !gotQryinsid) {
            missingCodePoint(CodePoint.QRYINSID);
        }

        DRDAStatement stmt = database.getDRDAStatement(pkgnamcsn);
        if (stmt == null)
        {
            //XXX should really throw a SQL Exception here
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
            invalidValue(CodePoint.PKGNAMCSN);
        }

        if (stmt.wasExplicitlyClosed())
        {
            // JCC still sends a CLSQRY even though we have
            // implicitly closed the resultSet.
            // Then complains if we send the writeQRYNOPRM
            // So for now don't send it
            // Also metadata calls seem to get bound to the same
            // PGKNAMCSN, so even for explicit closes we have
            // to ignore.
            //writeQRYNOPRM(CodePoint.SVRCOD_ERROR);
        }

        stmt.CLSQRY();
       
        return stmt;
    }

    /**
     * Parse MONITOR
     * DRDA spec says this is optional.  Since we
     * don't currently support it, we just ignore.
     */
    private void parseMONITOR() 
        throws DRDAProtocolException
    {
        // Just ignore it.
        reader.skipBytes();
    }

    private void writeSQLCARDs(SQLException e, long updateCount)
                                    throws DRDAProtocolException
    {
        writeSQLCARDs(e, updateCount, false);
    }

    private void writeSQLCARDs(SQLException e, long updateCount, boolean sendSQLERRRM)
                                    throws DRDAProtocolException
    {

        if (e == null)
        {
            writeSQLCARD(e, updateCount, 0);
            return;
        }

        // instead of writing a chain of sql error or warning, we send the first one, this is
        // jcc/db2 limitation, see beetle 4629

        // If it is a real SQL Error write a SQLERRRM first
        int severity = getExceptionSeverity(e);
        if (severity > CodePoint.SVRCOD_ERROR)
        {
            // For a session ending error > CodePoint.SRVCOD_ERROR you cannot
            // send a SQLERRRM. A CMDCHKRM is required.  In XA if there is a
            // lock timeout it ends the whole session. I am not sure this 
            // is the correct behaviour but if it occurs we have to send 
            // a CMDCHKRM instead of SQLERRM
            writeCMDCHKRM(severity);
        }
        else if (sendSQLERRRM)
        {
            writeSQLERRRM(severity);
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-2601
//IC see: https://issues.apache.org/jira/browse/DERBY-2601
        writeSQLCARD(e, updateCount, 0);
    }

    /**
     * <p>
     * Get the SQLCODE to send for an exception or a warning.
     * </p>
     *
     * <p>
     * The client expects a negative SQLCODE for exceptions and a positive
     * SQLCODE for warnings. SQLCODE 0 means there is no error or warning
     * condition. SQLCODE is also used to encode the severity of the condition
     * (as returned by {@code SQLException.getErrorCode()}).
     * </p>
     *
     * <p>
     * For warnings, the SQLCODE is 10000, which is identical to
     * {@link ExceptionSeverity#WARNING_SEVERITY}.
     * </p>
     *
     * <p>
     * For exceptions, the SQLCODE is set to {@code -severity-1}, which allows
     * all non-negative severity values to be encoded. (Derby only uses
     * non-negative severity values in the first place.)
     * </p>
     *
     * @param e the exception or warning to get the SQLCODE for
     * @return the value to send as SQLCODE
     */
    private int getSqlCode(SQLException e)
    {
        if (e == null) {
            return 0;
        }

        // All SQLWarnings should have warning severity. However,
        // DataTruncation conditions for write operations (with SQL state
        // 22001) are thrown as exceptions, even though DataTruncation
        // technically is a sub-class of SQLWarning.
        if (e instanceof SQLWarning &&
                !SQLState.LANG_STRING_TRUNCATION.equals(e.getSQLState())) {
            return ExceptionSeverity.WARNING_SEVERITY;
        }

        // The exception represents an error condition, so encode the severity
        // as a negative value in the SQLCODE. Negative severity values are
        // changed to 0 (NO_APPLICABLE_SEVERITY).
        int severity =
                Math.max(ExceptionSeverity.NO_APPLICABLE_SEVERITY,
                         e.getErrorCode());
        return -severity - 1;
    }

    private void writeSQLCARD(SQLException e,
//IC see: https://issues.apache.org/jira/browse/DERBY-6000
        long updateCount, long rowCount ) throws DRDAProtocolException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        writer.createDssObject();
        writer.startDdm(CodePoint.SQLCARD);
        writeSQLCAGRP(e, updateCount, rowCount);
        writer.endDdmAndDss();

        // If we have a shutdown exception, restart the server.
        if (e != null) {
            String sqlState = e.getSQLState();
            if (sqlState.regionMatches(0,
              SQLState.CLOUDSCAPE_SYSTEM_SHUTDOWN, 0, 5)) {
            // then we're here because of a shutdown exception;
            // "clean up" by restarting the server.
                try {
                    server.startNetworkServer();
                } catch (Exception restart)
                // any error messages should have already been printed,
                // so we ignore this exception here.
                {}
            }
        }

    }

    /**
     * Write a null SQLCARD as an object
     *
     * @exception DRDAProtocolException
     */
    private void writeNullSQLCARDobject()
        throws DRDAProtocolException
    {
        writer.createDssObject();
        writer.startDdm(CodePoint.SQLCARD);
//IC see: https://issues.apache.org/jira/browse/DERBY-825
        writeSQLCAGRP(nullSQLState, 0, 0, 0);
        writer.endDdmAndDss();
    }
    /**
     * Write SQLERRRM
     *
     * Instance Variables
     *  SVRCOD - Severity Code - required
     *
     * @param   severity    severity of error
     *
     * @exception DRDAProtocolException
     */
    private void writeSQLERRRM(int severity) throws DRDAProtocolException
    {
        writer.createDssReply();
        writer.startDdm(CodePoint.SQLERRRM);
        writer.writeScalar2Bytes(CodePoint.SVRCOD, severity);
        writer.endDdmAndDss ();

    }

    /**
     * Write CMDCHKRM
     *
     * Instance Variables
     *  SVRCOD - Severity Code - required
     *
     * @param   severity    severity of error
     *
     * @exception DRDAProtocolException
     */
    private void writeCMDCHKRM(int severity) throws DRDAProtocolException
    {
        writer.createDssReply();
        writer.startDdm(CodePoint.CMDCHKRM);
        writer.writeScalar2Bytes(CodePoint.SVRCOD, severity);
        writer.endDdmAndDss ();

    }

    /**
     * Translate from Derby exception severity to SVRCOD
     *
     * @param e SQLException
     */
    private int getExceptionSeverity (SQLException e)
    {
        int severity= CodePoint.SVRCOD_INFO;

        if (e == null) {
            return severity;
        }

        int ec = e.getErrorCode();
        switch (ec)
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
            case ExceptionSeverity.STATEMENT_SEVERITY:
            case ExceptionSeverity.TRANSACTION_SEVERITY:
                severity = CodePoint.SVRCOD_ERROR;
                break;
            case ExceptionSeverity.WARNING_SEVERITY:
                severity = CodePoint.SVRCOD_WARNING;
                break;
            case ExceptionSeverity.SESSION_SEVERITY:
            case ExceptionSeverity.DATABASE_SEVERITY:
            case ExceptionSeverity.SYSTEM_SEVERITY:
                severity = CodePoint.SVRCOD_SESDMG;
                break;
            default:
                String sqlState = e.getSQLState();
                if (sqlState != null && sqlState.startsWith("01")) {
                    // warning
                    severity = CodePoint.SVRCOD_WARNING;
                } else {
                    severity = CodePoint.SVRCOD_ERROR;
                }
        }

        return severity;

    }
    /**
     * Write SQLCAGRP
     *
     * SQLCAGRP : FDOCA EARLY GROUP
     * SQL Communcations Area Group Description
     *
     * FORMAT FOR SQLAM &lt;= 6
     *   SQLCODE; DRDA TYPE I4; ENVLID 0x02; Length Override 4
     *   SQLSTATE; DRDA TYPE FCS; ENVLID 0x30; Length Override 5
     *   SQLERRPROC; DRDA TYPE FCS; ENVLID 0x30; Length Override 8
     *   SQLCAXGRP; DRDA TYPE N-GDA; ENVLID 0x52; Length Override 0
     *
     * FORMAT FOR SQLAM &gt;= 7
     *   SQLCODE; DRDA TYPE I4; ENVLID 0x02; Length Override 4
     *   SQLSTATE; DRDA TYPE FCS; ENVLID 0x30; Length Override 5
     *   SQLERRPROC; DRDA TYPE FCS; ENVLID 0x30; Length Override 8
     *   SQLCAXGRP; DRDA TYPE N-GDA; ENVLID 0x52; Length Override 0
     *   SQLDIAGGRP; DRDA TYPE N-GDA; ENVLID 0x56; Length Override 0
     *
     * @param e     SQLException encountered
     * 
     * @exception DRDAProtocolException
     */
    private void writeSQLCAGRP(SQLException e, long updateCount, long rowCount)
        throws DRDAProtocolException
    {
        int sqlcode = getSqlCode(e);

//IC see: https://issues.apache.org/jira/browse/DERBY-825
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
        if (e == null) {
            // Forwarding to the optimized version when there is no
            // exception object
            writeSQLCAGRP(nullSQLState, sqlcode, updateCount, rowCount);
            return;
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        if (rowCount < 0 && updateCount < 0)
        {
            writer.writeByte(CodePoint.NULLDATA);
            return;
        }
            
//IC see: https://issues.apache.org/jira/browse/DERBY-6206
        if (SanityManager.DEBUG && server.debugOutput() && sqlcode < 0) {
            trace("handle SQLException here");
            trace("reason is: "+e.getMessage());
            trace("SQLState is: "+e.getSQLState());
            trace("vendorCode is: "+e.getErrorCode());
            trace("nextException is: "+e.getNextException());
            server.consoleExceptionPrint(e);
            trace("wrapping SQLException into SQLCARD...");
        }
        
        //null indicator
        writer.writeByte(0);
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
//IC see: https://issues.apache.org/jira/browse/DERBY-5896

        // SQLCODE
        writer.writeInt(sqlcode);

        // SQLSTATE
        writer.writeString(e.getSQLState());
//IC see: https://issues.apache.org/jira/browse/DERBY-825

        // SQLERRPROC
        // Write the byte[] constant rather than the string, for efficiency
        writer.writeBytes( getProductIDBytes() );
//IC see: https://issues.apache.org/jira/browse/DERBY-6206

        // SQLCAXGRP
        writeSQLCAXGRP(updateCount, rowCount, buildSqlerrmc(e), e.getNextException());
    }

    /**
     * Same as writeSQLCAGRP, but optimized for the case
         * when there is no real exception, i.e. the exception is null, or "End
         * of data"
     *
     * SQLCAGRP : FDOCA EARLY GROUP
     * SQL Communcations Area Group Description
     *
     * FORMAT FOR SQLAM &lt;= 6
     *   SQLCODE; DRDA TYPE I4; ENVLID 0x02; Length Override 4
     *   SQLSTATE; DRDA TYPE FCS; ENVLID 0x30; Length Override 5
     *   SQLERRPROC; DRDA TYPE FCS; ENVLID 0x30; Length Override 8
     *   SQLCAXGRP; DRDA TYPE N-GDA; ENVLID 0x52; Length Override 0
     *
     * FORMAT FOR SQLAM &gt;= 7
     *   SQLCODE; DRDA TYPE I4; ENVLID 0x02; Length Override 4
     *   SQLSTATE; DRDA TYPE FCS; ENVLID 0x30; Length Override 5
     *   SQLERRPROC; DRDA TYPE FCS; ENVLID 0x30; Length Override 8
     *   SQLCAXGRP; DRDA TYPE N-GDA; ENVLID 0x52; Length Override 0
     *   SQLDIAGGRP; DRDA TYPE N-GDA; ENVLID 0x56; Length Override 0
     *
     * @param sqlState     SQLState (already converted to UTF8)
     * @param sqlcode    sqlcode
         * @param updateCount
         * @param rowCount
     * 
     * @exception DRDAProtocolException
     */

    private void writeSQLCAGRP(byte[] sqlState, int sqlcode, 
//IC see: https://issues.apache.org/jira/browse/DERBY-6000
                               long updateCount, long rowCount) throws DRDAProtocolException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
        if (rowCount < 0 && updateCount < 0) {
            writer.writeByte(CodePoint.NULLDATA);
            return;
        }
        
        //null indicator
        writer.writeByte(0);
        
        // SQLCODE
        writer.writeInt(sqlcode);

        // SQLSTATE
        writer.writeBytes(sqlState);

        // SQLERRPROC
        writer.writeBytes( getProductIDBytes() );
//IC see: https://issues.apache.org/jira/browse/DERBY-6206

        // SQLCAXGRP (Uses null as sqlerrmc since there is no error)
        writeSQLCAXGRP(updateCount, rowCount, null, null);
    }
    
    
    // Delimiters for SQLERRMC values.
    // The token delimiter value will be used to parse the MessageId from the 
    // SQLERRMC in MessageService.getLocalizedMessage and the MessageId will be
    // used to retrive the localized message. If this delimiter value is changed
    // please make sure to make appropriate changes in
    // MessageService.getLocalizedMessage that gets called from 
    // SystemProcedures.SQLCAMESSAGE
    /**
     * <code>SQLERRMC_TOKEN_DELIMITER</code> separates message argument tokens 
     */
    private static String SQLERRMC_TOKEN_DELIMITER = new String(new char[] {(char)20});

    /**
     * <code>SQLERRMC_PREFORMATTED_MESSAGE_DELIMITER</code>, When full message text is 
     * sent for severe errors. This value separates the messages. 
     */
    private static String SQLERRMC_PREFORMATTED_MESSAGE_DELIMITER = "::";
    
    /**
     * Create error message or message argements to return to client.  
     * The SQLERRMC will normally be passed back  to the server in a call 
     * to the SYSIBM.SQLCAMESSAGE but for severe exceptions the stored procedure 
     * call cannot be made. So for Severe messages we will just send the message text.
     * 
     * This method will also truncate the value according the client capacity.
     * CCC can only handle 70 characters.
     * 
     * Server sends the sqlerrmc using UTF8 encoding to the client.
     * To get the message, client sends back information to the server
     * calling SYSIBM.SQLCAMESSAGE (see Sqlca.getMessage).  Several parameters 
     * are sent to this procedure including the locale, the sqlerrmc that the 
     * client received from the server. 
     * On server side, the procedure SQLCAMESSAGE in SystemProcedures then calls
     * the MessageService.getLocalizedMessage to retrieve the localized error message. 
     * In MessageService.getLocalizedMessage the sqlerrmc that is passed in, 
     * is parsed to retrieve the message id. The value it uses to parse the MessageId
     * is char value of 20, otherwise it uses the entire sqlerrmc as the message id. 
     * This messageId is then used to retrieve the localized message if present, to 
     * the client.
     * 
     * @param se  SQLException to build SQLERRMC
     *  
     * @return  String which is either the message arguments to be passed to 
     *          SYSIBM.SQLCAMESSAGE or just message text for severe errors.  
     */
    private String buildSqlerrmc (SQLException se) 
    {
        boolean severe = (se.getErrorCode() >=  ExceptionSeverity.SESSION_SEVERITY);    
        String sqlerrmc;
//IC see: https://issues.apache.org/jira/browse/DERBY-5896

        // get exception which carries Derby messageID and args, per DERBY-1178
        StandardException ferry = StandardException.getArgumentFerry(se);
//IC see: https://issues.apache.org/jira/browse/DERBY-6488

        if (se instanceof DataTruncation) {
            // Encode DataTruncation in a special way.
            sqlerrmc = buildDataTruncationSqlerrmc((DataTruncation) se);
        } else if (ferry != null && !severe) {
            // All other non-severe Derby exceptions are encoded here.
            sqlerrmc = buildTokenizedSqlerrmc(se);
        } else {
            // If this is not a Derby exception or is a severe excecption where
            // we have no hope of succussfully calling the SYSIBM.SQLCAMESSAGE send
            // preformatted message using the server locale
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
            sqlerrmc = buildPreformattedSqlerrmc(se);
        }

        // Truncate the sqlerrmc to a length that the client can support.
        int maxlen = (sqlerrmc == null) ? -1 : Math.min(sqlerrmc.length(),
                    appRequester.supportedMessageParamLength());
        if ((maxlen >= 0) && (sqlerrmc.length() > maxlen)) {
            // have to truncate so the client can handle it.
            sqlerrmc = sqlerrmc.substring(0, maxlen);
        }

        return sqlerrmc;
    }

    /**
     * Build preformatted SQLException text 
     * for severe exceptions or SQLExceptions that are not Derby exceptions.
     * Just send the message text localized to the server locale.
     * 
     * @param se  SQLException for which to build SQLERRMC
     * @return preformated message text 
     *          with messages separted by SQLERRMC_PREFORMATED_MESSAGE_DELIMITER
     * 
     */
    private String  buildPreformattedSqlerrmc(SQLException se) {
        if (se == null) {
            return "";
        }
        
         // String buffer to build up message
        StringBuilder sb = new StringBuilder();
        sb.append(se.getLocalizedMessage());
        while ((se = se.getNextException()) != null) {
            sb.append(SQLERRMC_PREFORMATTED_MESSAGE_DELIMITER);
            sb.append("SQLSTATE: ");
            sb.append(se.getSQLState());
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        return sb.toString();
    }

    /**
     * Build Tokenized SQLERRMC to just send the tokenized arguments to the client.
     * for a Derby SQLException or an SQLException thrown by user code.
     * Message argument tokens are separated by SQLERRMC_TOKEN_DELIMITER 
     * Multiple messages are separated by SystemProcedures.SQLERRMC_MESSAGE_DELIMITER
     * 
     *                 ...
     * @param se   SQLException to print
     * 
     */
    private String buildTokenizedSqlerrmc(SQLException se) {
        
        String sqlerrmc = "";
        do {
            StandardException ferry = StandardException.getArgumentFerry(se);
            if (ferry != null)
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-6802
//IC see: https://issues.apache.org/jira/browse/DERBY-6823
                sqlerrmc += MessageUtils.encodeMessageAndArgumentsAsSqlerrmc(
                                ferry.getMessageId(),
                                ferry.getArguments());
                se = se.getNextException();
            }
            else
            {   
                // this could happen for instance if an SQLException was thrown
                // from a stored procedure.
                sqlerrmc += MessageUtils.encodeExceptionAsSqlerrmc( se );
                se = se.getNextException();
            }
            if (se != null)
            {
                sqlerrmc += MessageUtils.SQLERRMC_MESSAGE_DELIMITER + se.getSQLState() + ":";
            }
        } while (se != null);
        return sqlerrmc;
    }

    /**
     * Build the SQLERRMC for a {@code java.sql.DataTruncation} warning.
     * Serialize all the fields of the {@code DataTruncation} instance in the
     * order in which they appear in the parameter list of the constructor.
     *
     * @param dt the {@code DataTruncation} instance to serialize
     * @return the SQLERRMC string with all fields of the warning
     */
    private String buildDataTruncationSqlerrmc(DataTruncation dt) {
        return dt.getIndex() + SQLERRMC_TOKEN_DELIMITER +
               dt.getParameter() + SQLERRMC_TOKEN_DELIMITER +
               dt.getRead() + SQLERRMC_TOKEN_DELIMITER +
               dt.getDataSize() + SQLERRMC_TOKEN_DELIMITER +
               dt.getTransferSize();
    }
    
    /**
     * Write SQLCAXGRP
     *
     * SQLCAXGRP : EARLY FDOCA GROUP
     * SQL Communications Area Exceptions Group Description
     *
     * FORMAT FOR SQLAM &lt;= 6
     *   SQLRDBNME; DRDA TYPE FCS; ENVLID 0x30; Length Override 18
     *   SQLERRD1; DRDA TYPE I4; ENVLID 0x02; Length Override 4
     *   SQLERRD2; DRDA TYPE I4; ENVLID 0x02; Length Override 4
     *   SQLERRD3; DRDA TYPE I4; ENVLID 0x02; Length Override 4
     *   SQLERRD4; DRDA TYPE I4; ENVLID 0x02; Length Override 4
     *   SQLERRD5; DRDA TYPE I4; ENVLID 0x02; Length Override 4
     *   SQLERRD6; DRDA TYPE I4; ENVLID 0x02; Length Override 4
     *   SQLWARN0; DRDA TYPE FCS; ENVLID 0x30; Length Override 1
     *   SQLWARN1; DRDA TYPE FCS; ENVLID 0x30; Length Override 1
     *   SQLWARN2; DRDA TYPE FCS; ENVLID 0x30; Length Override 1
     *   SQLWARN3; DRDA TYPE FCS; ENVLID 0x30; Length Override 1
     *   SQLWARN4; DRDA TYPE FCS; ENVLID 0x30; Length Override 1
     *   SQLWARN5; DRDA TYPE FCS; ENVLID 0x30; Length Override 1
     *   SQLWARN6; DRDA TYPE FCS; ENVLID 0x30; Length Override 1
     *   SQLWARN7; DRDA TYPE FCS; ENVLID 0x30; Length Override 1
     *   SQLWARN8; DRDA TYPE FCS; ENVLID 0x30; Length Override 1
     *   SQLWARN9; DRDA TYPE FCS; ENVLID 0x30; Length Override 1
     *   SQLWARNA; DRDA TYPE FCS; ENVLID 0x30; Length Override 1
     *   SQLERRMSG_m; DRDA TYPE VCM; ENVLID 0x3E; Length Override 70
     *   SQLERRMSG_s; DRDA TYPE VCS; ENVLID 0x32; Length Override 70
     *
     * FORMAT FOR SQLAM &gt;= 7
     *   SQLERRD1; DRDA TYPE I4; ENVLID 0x02; Length Override 4
     *   SQLERRD2; DRDA TYPE I4; ENVLID 0x02; Length Override 4
     *   SQLERRD3; DRDA TYPE I4; ENVLID 0x02; Length Override 4
     *   SQLERRD4; DRDA TYPE I4; ENVLID 0x02; Length Override 4
     *   SQLERRD5; DRDA TYPE I4; ENVLID 0x02; Length Override 4
     *   SQLERRD6; DRDA TYPE I4; ENVLID 0x02; Length Override 4
     *   SQLWARN0; DRDA TYPE FCS; ENVLID 0x30; Length Override 1
     *   SQLWARN1; DRDA TYPE FCS; ENVLID 0x30; Length Override 1
     *   SQLWARN2; DRDA TYPE FCS; ENVLID 0x30; Length Override 1
     *   SQLWARN3; DRDA TYPE FCS; ENVLID 0x30; Length Override 1
     *   SQLWARN4; DRDA TYPE FCS; ENVLID 0x30; Length Override 1
     *   SQLWARN5; DRDA TYPE FCS; ENVLID 0x30; Length Override 1
     *   SQLWARN6; DRDA TYPE FCS; ENVLID 0x30; Length Override 1
     *   SQLWARN7; DRDA TYPE FCS; ENVLID 0x30; Length Override 1
     *   SQLWARN8; DRDA TYPE FCS; ENVLID 0x30; Length Override 1
     *   SQLWARN9; DRDA TYPE FCS; ENVLID 0x30; Length Override 1
     *   SQLWARNA; DRDA TYPE FCS; ENVLID 0x30; Length Override 1
     *   SQLRDBNAME; DRDA TYPE VCS; ENVLID 0x32; Length Override 1024
     *   SQLERRMSG_m; DRDA TYPE VCM; ENVLID 0x3E; Length Override 70
     *   SQLERRMSG_s; DRDA TYPE VCS; ENVLID 0x32; Length Override 70
     * @param nextException SQLException encountered
     * @param sqlerrmc sqlcode
     * 
     * @exception DRDAProtocolException
     */
    private void writeSQLCAXGRP(long updateCount,  long rowCount, String sqlerrmc,
                SQLException nextException) throws DRDAProtocolException
    {
        writer.writeByte(0);        // SQLCAXGRP INDICATOR
        if (sqlamLevel < 7)
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-728
//IC see: https://issues.apache.org/jira/browse/DERBY-728
//IC see: https://issues.apache.org/jira/browse/DERBY-728
            writeRDBNAM(database.getDatabaseName());
            writeSQLCAERRWARN(updateCount, rowCount);
        }
        else
        {
            // SQL ERRD1 - D6, WARN0-WARNA (35 bytes)
            writeSQLCAERRWARN(updateCount, rowCount);
            writer.writeShort(0);  //CCC on Win does not take RDBNAME
        }
        writeVCMorVCS(sqlerrmc);
        if (sqlamLevel >= 7) {
            writeSQLDIAGGRP(nextException);
        }
    }

    /**
     * Write the ERR and WARN part of the SQLCA
     *
     * @param updateCount
     * @param rowCount 
     */
    private void writeSQLCAERRWARN(long updateCount, long rowCount) 
    {
        // SQL ERRD1 = Sqlca.HIGH_ORDER_ROW_COUNT
//IC see: https://issues.apache.org/jira/browse/DERBY-6000
        writer.writeInt((int)((rowCount>>>32)));
        // SQL ERRD2 = Sqlca.LOW_ORDER_ROW_COUNT
        writer.writeInt((int)(rowCount & 0x0000000ffffffffL));
        // SQL ERRD3 = Sqlca.LOW_ORDER_UPDATE_COUNT
        writer.writeInt( (int)(updateCount & 0x0000000ffffffffL) );
        // SQL ERRD4 = Sqlca.HIGH_ORDER_UPDATE_COUNT
        writer.writeInt( (int)(updateCount>>>32) );
        // SQL ERRD5 - D6 (8 bytes)
        writer.writeBytes(errD5_D6); // byte[] constant
        // WARN0-WARNA (11 bytes)
        writer.writeBytes(warn0_warnA); // byte[] constant
    }

    /**
     * Write SQLDIAGGRP: SQL Diagnostics Group Description - Identity 0xD1
     * Nullable Group
     * SQLDIAGSTT; DRDA TYPE N-GDA; ENVLID 0xD3; Length Override 0
     * SQLDIAGCN;  DRFA TYPE N-RLO; ENVLID 0xF6; Length Override 0
     * SQLDIAGCI;  DRDA TYPE N-RLO; ENVLID 0xF5; Length Override 0
     */
    private void writeSQLDIAGGRP(SQLException nextException) 
        throws DRDAProtocolException
    {
        // for now we only want to send ROW_DELETED and ROW_UPDATED warnings
        // as extended diagnostics
        // move to first ROW_DELETED or ROW_UPDATED exception. These have been
        // added to the end of the warning chain.
        while (!(nextException == null ||
                 SQLState.ROW_UPDATED.equals(nextException.getSQLState()) ||
                 SQLState.ROW_DELETED.equals(nextException.getSQLState()))) {
            nextException = nextException.getNextException();
        }

        if ((nextException == null) || 
                (diagnosticLevel == CodePoint.DIAGLVL0)) {
            writer.writeByte(CodePoint.NULLDATA);
            return;
        }
        writer.writeByte(0); // SQLDIAGGRP indicator

        writeSQLDIAGSTT();
        writeSQLDIAGCI(nextException);
        writeSQLDIAGCN();
    }

    /*
     * writeSQLDIAGSTT: Write NULLDATA for now
     */
    private void writeSQLDIAGSTT()
        throws DRDAProtocolException
    {
        writer.writeByte(CodePoint.NULLDATA);
    }

    /**
     * writeSQLDIAGCI: SQL Diagnostics Condition Information Array - Identity 0xF5
     * SQLNUMROW; ROW LID 0x68; ELEMENT TAKEN 0(all); REP FACTOR 1
     * SQLDCIROW; ROW LID 0xE5; ELEMENT TAKEN 0(all); REP FACTOR 0(all)
     */
    private void writeSQLDIAGCI(SQLException nextException)
        throws DRDAProtocolException
    {
        SQLException se = nextException;
        long rowNum = 1;

        /* Write the number of next exceptions to expect */
        writeSQLNUMROW(se);

        while (se != null)
        {
            String sqlState = se.getSQLState();

            // SQLCode > 0 -> Warning
            // SQLCode = 0 -> Info
            // SQLCode < 0 -> Error
            int severity = getExceptionSeverity(se);
            int sqlCode = -1;
            if (severity == CodePoint.SVRCOD_WARNING) {
                sqlCode = 1;
            } else if (severity == CodePoint.SVRCOD_INFO) {
                sqlCode = 0;
            }

            String sqlerrmc = "";
            if (diagnosticLevel == CodePoint.DIAGLVL1) {
                sqlerrmc = se.getLocalizedMessage();
            }

            // arguments are variable part of a message
            // only send arguments for diagnostic level 0
            if (diagnosticLevel == CodePoint.DIAGLVL0) {
                // we are only able to get arguments of Derby exceptions
//IC see: https://issues.apache.org/jira/browse/DERBY-6488
                StandardException ferry =
                        StandardException.getArgumentFerry(se);
                if (ferry != null) {
                    Object[] args = ferry.getArguments();
                    for (int i = 0; args != null &&  i < args.length; i++) {
                        sqlerrmc += args[i].toString() + SQLERRMC_TOKEN_DELIMITER;
                    }
                }
            }

            writeSQLDCROW(rowNum++, sqlCode, sqlState, getDbName(), sqlerrmc);

            se = se.getNextException();
        }
    }

    /**
     * writeSQLNUMROW: Writes SQLNUMROW : FDOCA EARLY ROW
     * SQL Number of Elements Row Description
     * FORMAT FOR SQLAM LEVELS
     * SQLNUMGRP; GROUP LID 0x58; ELEMENT TAKEN 0(all); REP FACTOR 1
     */
    private void writeSQLNUMROW(SQLException nextException)
         throws DRDAProtocolException
    {
        writeSQLNUMGRP(nextException);
    }

    /**
     * writeSQLNUMGRP: Writes SQLNUMGRP : FDOCA EARLY GROUP
     * SQL Number of Elements Group Description
     * FORMAT FOR ALL SQLAM LEVELS
     * SQLNUM; DRDA TYPE I2; ENVLID 0x04; Length Override 2
     */
    private void writeSQLNUMGRP(SQLException nextException)
         throws DRDAProtocolException
    {
        int i=0;
        SQLException se;

        /* Count the number of chained exceptions to be sent */
        for (se = nextException; se != null; se = se.getNextException()) {
            i++;
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-1313
        writer.writeShort(i);
    }

    /**
     * writeSQLDCROW: SQL Diagnostics Condition Row - Identity 0xE5
     * SQLDCGRP; GROUP LID 0xD5; ELEMENT TAKEN 0(all); REP FACTOR 1
     */
    private void writeSQLDCROW(long rowNum, int sqlCode, String sqlState, String dbname,
         String sqlerrmc) throws DRDAProtocolException
    {
        writeSQLDCGRP(rowNum, sqlCode, sqlState, dbname, sqlerrmc);
    }

    /**
     * writeSQLDCGRP: SQL Diagnostics Condition Group Description
     * 
     * SQLDCCODE; DRDA TYPE I4; ENVLID 0x02; Length Override 4
     * SQLDCSTATE; DRDA TYPE FCS; ENVLID Ox30; Lengeh Override 5
     * SQLDCREASON; DRDA TYPE I4; ENVLID 0x02; Length Override 4
     * SQLDCLINEN; DRDA TYPE I4; ENVLID 0x02; Length Override 4
     * SQLDCROWN; DRDA TYPE FD; ENVLID 0x0E; Lengeh Override 31
     * SQLDCER01; DRDA TYPE I4; ENVLID 0x02; Length Override 4
     * SQLDCER02; DRDA TYPE I4; ENVLID 0x02; Length Override 4
     * SQLDCER03; DRDA TYPE I4; ENVLID 0x02; Length Override 4
     * SQLDCER04; DRDA TYPE I4; ENVLID 0x02; Length Override 4
     * SQLDCPART; DRDA TYPE I4; ENVLID 0x02; Length Override 4
     * SQLDCPPOP; DRDA TYPE I4; ENVLID 0x02; Length Override 4
     * SQLDCMSGID; DRDA TYPE FCS; ENVLID 0x30; Length Override 10
     * SQLDCMDE; DRDA TYPE FCS; ENVLID 0x30; Length Override 8
     * SQLDCPMOD; DRDA TYPE FCS; ENVLID 0x30; Length Override 5
     * SQLDCRDB; DRDA TYPE VCS; ENVLID 0x32; Length Override 255
     * SQLDCTOKS; DRDA TYPE N-RLO; ENVLID 0xF7; Length Override 0
     * SQLDCMSG_m; DRDA TYPE NVMC; ENVLID 0x3F; Length Override 32672
     * SQLDCMSG_S; DRDA TYPE NVCS; ENVLID 0x33; Length Override 32672
     * SQLDCCOLN_m; DRDA TYPE NVCM ; ENVLID 0x3F; Length Override 255
     * SQLDCCOLN_s; DRDA TYPE NVCS; ENVLID 0x33; Length Override 255
     * SQLDCCURN_m; DRDA TYPE NVCM; ENVLID 0x3F; Length Override 255
     * SQLDCCURN_s; DRDA TYPE NVCS; ENVLID 0x33; Length Override 255
     * SQLDCPNAM_m; DRDA TYPE NVCM; ENVLID 0x3F; Length Override 255
     * SQLDCPNAM_s; DRDA TYPE NVCS; ENVLID 0x33; Length Override 255
     * SQLDCXGRP; DRDA TYPE N-GDA; ENVLID 0xD3; Length Override 1
     */
    private void writeSQLDCGRP(long rowNum, int sqlCode, String sqlState, String dbname,
         String sqlerrmc) throws DRDAProtocolException
    {
        // SQLDCCODE
        writer.writeInt(sqlCode);

        // SQLDCSTATE
        writer.writeString(sqlState);


//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        writer.writeInt(0);                     // REASON_CODE
        writer.writeInt(0);                     // LINE_NUMBER
        writer.writeLong(rowNum);               // ROW_NUMBER

        byte[] byteArray = new byte[1];
        writer.writeScalarPaddedBytes(byteArray, 47, (byte) 0);

        writer.writeShort(0);                   // CCC on Win does not take RDBNAME
        writer.writeByte(CodePoint.NULLDATA);   // MESSAGE_TOKENS
        writer.writeLDString(sqlerrmc);         // MESSAGE_TEXT

        writeVCMorVCS(null);                    // COLUMN_NAME
        writeVCMorVCS(null);                    // PARAMETER_NAME
        writeVCMorVCS(null);                    // EXTENDED_NAME
        writer.writeByte(CodePoint.NULLDATA);   // SQLDCXGRP
    }

    /*
     * writeSQLDIAGCN: Write NULLDATA for now
     */
    private void writeSQLDIAGCN()
        throws DRDAProtocolException
    {
        writer.writeByte(CodePoint.NULLDATA);
    }

    /** 
     * Write SQLDARD
     *
     * SQLDARD : FDOCA EARLY ARRAY
     * SQL Descriptor Area Row Description with SQL Communications Area
     *
     * FORMAT FOR SQLAM &lt;= 6
     *   SQLCARD; ROW LID 0x64; ELEMENT TAKEN 0(all); REP FACTOR 1
     *   SQLNUMROW; ROW LID 0x68; ELEMENT TAKEN 0(all); REP FACTOR 1
     *   SQLDAROW; ROW LID 0x60; ELEMENT TAKEN 0(all); REP FACTOR 0(all)
     *
     * FORMAT FOR SQLAM &gt;= 7
     *   SQLCARD; ROW LID 0x64; ELEMENT TAKEN 0(all); REP FACTOR 1
     *   SQLDHROW; ROW LID 0xE0; ELEMENT TAKEN 0(all); REP FACTOR 1
     *   SQLNUMROW; ROW LID 0x68; ELEMENT TAKEN 0(all); REP FACTOR 1
     *
     * @param stmt  prepared statement
     *
     * @throws DRDAProtocolException
     * @throws SQLException
     */
    private void writeSQLDARD(DRDAStatement stmt, boolean rtnOutput, SQLException e) throws DRDAProtocolException, SQLException
    {
        PreparedStatement ps = stmt.getPreparedStatement();
        ResultSetMetaData rsmeta = ps.getMetaData();
//IC see: https://issues.apache.org/jira/browse/DERBY-2121
        ParameterMetaData pmeta = stmt.getParameterMetaData();
        int numElems = 0;
        if (e == null || e instanceof SQLWarning)
        {
            if (rtnOutput && (rsmeta != null)) {
                numElems = rsmeta.getColumnCount();
            } else if ((! rtnOutput) && (pmeta != null)) {
                numElems = pmeta.getParameterCount();
            }
        }

        writer.createDssObject();

        // all went well we will just write a null SQLCA
        writer.startDdm(CodePoint.SQLDARD);
        writeSQLCAGRP(e, 0, 0);

        if (sqlamLevel >= MGRLVL_7) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3311
            writeSQLDHROW(ps.getResultSetHoldability());
        }

        //SQLNUMROW
        if (SanityManager.DEBUG) {
            trace("num Elements = " + numElems);
        }
        writer.writeShort(numElems);

        for (int i=0; i < numElems; i++) {
            writeSQLDAGRP (rsmeta, pmeta, i, rtnOutput);
        }
        writer.endDdmAndDss();

    }
    /**
     * Write QRYDSC - Query Answer Set Description
     *
     * @param stmt DRDAStatement we are working on
     * @param FDODSConly    simply the FDODSC, without the wrap
     *
     * Instance Variables
     *   SQLDTAGRP - required
     * 
     * Only 84 columns can be sent in a single QRYDSC.  If there are more columns
     * they must be sent in subsequent QRYDSC.
     * If the QRYDSC will not fit into the current block, as many columns as can
     * fit are sent and then the remaining are sent in the following blocks.
     * 
     * @throws DRDAProtocolException
     * @throws SQLException
     */
    private void writeQRYDSC(DRDAStatement stmt, boolean FDODSConly)
        throws DRDAProtocolException, SQLException
    {

        ResultSet rs = null;
        ResultSetMetaData rsmeta = null;
//IC see: https://issues.apache.org/jira/browse/DERBY-2121
        ParameterMetaData pmeta = null;
        if (!stmt.needsToSendParamData) {
            rs = stmt.getResultSet();
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        if (rs == null) {
            // this is a CallableStatement, use parameter meta data
            pmeta = stmt.getParameterMetaData();
        } else {
            rsmeta = rs.getMetaData();
        }

        int  numCols = (rsmeta != null ? rsmeta.getColumnCount() : pmeta.getParameterCount());
        int numGroups = 1;
        int colStart = 1;
        int colEnd = numCols;
        int blksize = stmt.getBlksize() > 0 ? stmt.getBlksize() : CodePoint.QRYBLKSZ_MAX;

        // check for remaining space in current query block
        // Need to mod with blksize so remaining doesn't go negative. 4868
        int remaining = blksize - (writer.getDSSLength()  % blksize) - (3 + 
                FdocaConstants.SQLCADTA_SQLDTARD_RLO_SIZE);


        // calcuate how may columns can be sent in the current query block
        int firstcols = remaining/FdocaConstants.SQLDTAGRP_COL_DSC_SIZE;

        // check if it doesn't all fit into the first block and 
        //  under FdocaConstants.MAX_VARS_IN_NGDA
        if (firstcols < numCols || numCols > FdocaConstants.MAX_VARS_IN_NGDA)
        {
            // we are limited to FdocaConstants.MAX_VARS_IN_NGDA
            if (firstcols > FdocaConstants.MAX_VARS_IN_NGDA)
            {
                if (SanityManager.DEBUG) {
                    SanityManager.ASSERT(numCols > FdocaConstants.MAX_VARS_IN_NGDA,
                        "Number of columns " + numCols + 
                        " is less than MAX_VARS_IN_NGDA");
                }
                numGroups = numCols/FdocaConstants.MAX_VARS_IN_NGDA;
                // some left over
                if (FdocaConstants.MAX_VARS_IN_NGDA * numGroups < numCols) {
                    numGroups++;
                }
                colEnd = FdocaConstants.MAX_VARS_IN_NGDA;
            }
            else
            {
                colEnd = firstcols;
                numGroups += (numCols-firstcols)/FdocaConstants.MAX_VARS_IN_NGDA;
                if (FdocaConstants.MAX_VARS_IN_NGDA * numGroups < numCols) {
                    numGroups++;
                }
            }
        }

        if (! FDODSConly)
        {
            writer.createDssObject();
            writer.startDdm(CodePoint.QRYDSC);
        }

        for (int i = 0; i < numGroups; i++)
        {
            writeSQLDTAGRP(stmt, rsmeta, pmeta, colStart, colEnd, 
                            (i == 0 ? true : false));
            colStart = colEnd + 1;
            // 4868 - Limit range to MAX_VARS_IN_NGDA (used to have extra col)
            colEnd = Math.min(
                    colEnd + FdocaConstants.MAX_VARS_IN_NGDA,
                    numCols);
        }
        writer.writeBytes(FdocaConstants.SQLCADTA_SQLDTARD_RLO);
        if (!FDODSConly) {
            writer.endDdmAndDss();
        }
    }
    /**
     * Write SQLDTAGRP
     * SQLDAGRP : Late FDOCA GROUP
     * SQL Data Value Group Descriptor
     *  LENGTH - length of the SQLDTAGRP
     *  TRIPLET_TYPE - NGDA for first, CPT for following
     *  ID - SQLDTAGRP_LID for first, NULL_LID for following
     *  For each column
     *    DRDA TYPE 
     *    LENGTH OVERRIDE
     *      For numeric/decimal types
     *        PRECISON
     *        SCALE
     *      otherwise
     *        LENGTH or DISPLAY_WIDTH
     *
     * @param stmt      drda statement
     * @param rsmeta    resultset meta data
     * @param pmeta     parameter meta data for CallableStatement
     * @param colStart  starting column for group to send
     * @param colEnd    end column to send
     * @param first     is this the first group
     *
     * @throws DRDAProtocolException
     * @throws SQLException
     */
    private void writeSQLDTAGRP(DRDAStatement stmt, ResultSetMetaData rsmeta, 
//IC see: https://issues.apache.org/jira/browse/DERBY-2121
                                ParameterMetaData pmeta,
                                int colStart, int colEnd, boolean first)
        throws DRDAProtocolException, SQLException
    {

        int length =  (FdocaConstants.SQLDTAGRP_COL_DSC_SIZE * 
                    ((colEnd+1) - colStart)) + 3;
        writer.writeByte(length);
        if (first)
        {

            writer.writeByte(FdocaConstants.NGDA_TRIPLET_TYPE);
            writer.writeByte(FdocaConstants.SQLDTAGRP_LID);
        }
        else
        {
            //continued
            writer.writeByte(FdocaConstants.CPT_TRIPLET_TYPE);
            writer.writeByte(FdocaConstants.NULL_LID);

        }

                           

        boolean hasRs = (rsmeta != null);   //  if don't have result, then we look at parameter meta

        for (int i = colStart; i <= colEnd; i++)
        {
            boolean nullable = hasRs ?
//IC see: https://issues.apache.org/jira/browse/DERBY-5847
//IC see: https://issues.apache.org/jira/browse/DERBY-436
                (rsmeta.isNullable(i) == ResultSetMetaData.columnNullable) :
                (pmeta.isNullable(i) == ParameterMetaData.parameterNullable);
            int colType = (hasRs ? rsmeta.getColumnType(i) : pmeta.getParameterType(i));
            int[] outlen = {-1};
            int drdaType = FdocaConstants.mapJdbcTypeToDrdaType( colType, nullable, appRequester, outlen );
                

            boolean isDecimal = ((drdaType | 1) == DRDAConstants.DRDA_TYPE_NDECIMAL);
            int precision = 0, scale = 0;
            if (hasRs)
            {
                precision = rsmeta.getPrecision(i);
                scale = rsmeta.getScale(i);
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
                stmt.setRsDRDAType(i,drdaType);
                stmt.setRsPrecision(i, precision);
                stmt.setRsScale(i,scale);
            }

            else if (isDecimal)
            {
                if (stmt.isOutputParam(i))
                {
//IC see: https://issues.apache.org/jira/browse/DERBY-2381
                    precision = pmeta.getPrecision(i);
                    scale = pmeta.getScale(i);
                    ((CallableStatement) stmt.ps).registerOutParameter(i,Types.DECIMAL,scale);
                  
                }

            }

            if (SanityManager.DEBUG) {
                trace("jdbcType=" + colType + "  \tdrdaType=" + Integer.toHexString(drdaType));
            }

            // Length or precision and scale for decimal values.
            writer.writeByte(drdaType);
            if (isDecimal)
            {
                writer.writeByte(precision);
                writer.writeByte(scale);
            }
            else if (outlen[0] != -1)
            {
                writer.writeShort(outlen[0]);
            }
            else if (hasRs)
            {
                writer.writeShort(rsmeta.getColumnDisplaySize(i));
            }
            else
            {
                writer.writeShort(stmt.getParamLen(i));
            }
        }
    }




    /**
     * Holdability passed in as it can represent the holdability of
     * the statement or a specific result set.
     * @param holdability HOLD_CURSORS_OVER_COMMIT or CLOSE_CURSORS_AT_COMMIT
     * @throws DRDAProtocolException
     * @throws SQLException
     */
    private void writeSQLDHROW(int holdability) throws DRDAProtocolException,SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        if (JVMInfo.JDK_ID < 2) //write null indicator for SQLDHROW because there is no holdability support prior to jdk1.3
        {
            writer.writeByte(CodePoint.NULLDATA);
            return;
        }

        writer.writeByte(0);        // SQLDHROW INDICATOR

        //SQLDHOLD
//IC see: https://issues.apache.org/jira/browse/DERBY-3311
        writer.writeShort(holdability);
        
        //SQLDRETURN
        writer.writeShort(0);
        //SQLDSCROLL
        writer.writeShort(0);
        //SQLDSENSITIVE
        writer.writeShort(0);
        //SQLDFCODE
        writer.writeShort(0);
        //SQLDKEYTYPE
        writer.writeShort(0);
        //SQLRDBNAME
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        writer.writeShort(0);   //CCC on Windows somehow does not take any dbname
        //SQLDSCHEMA
        writeVCMorVCS(null);

    }

    /**
     * Write QRYDTA - Query Answer Set Data
     *  Contains some or all of the answer set data resulting from a query
     *  If the client is not using rowset processing, this routine attempts
     *  to pack as much data into the QRYDTA as it can. This may result in
     *  splitting the last row across the block, in which case when the
     *  client calls CNTQRY we will return the remainder of the row.
     *
     *  Splitting a QRYDTA block is expensive, for several reasons:
     *  - extra logic must be run, on both client and server side
     *  - more network round-trips are involved
     *  - the QRYDTA block which contains the continuation of the split
     *    row is generally wasteful, since it contains the remainder of
     *    the split row but no additional rows.
     *  Since splitting is expensive, the server makes some attempt to
     *  avoid it. Currently, the server's algorithm for this is to
     *  compute the length of the current row, and to stop trying to pack
     *  more rows into this buffer if another row of that length would
     *  not fit. However, since rows can vary substantially in length,
     *  this algorithm is often ineffective at preventing splits. For
     *  example, if a short row near the end of the buffer is then
     *  followed by a long row, that long row will be split. It is possible
     *  to improve this algorithm substantially:
     *  - instead of just using the length of the previous row as a guide
     *    for whether to attempt packing another row in, use some sort of
     *    overall average row size computed over multiple rows (e.g., all
     *    the rows we've placed into this QRYDTA block, or all the rows
     *    we've process for this result set)
     *  - when we discover that the next row will not fit, rather than
     *    splitting the row across QRYDTA blocks, if it is relatively
     *    small, we could just hold the entire row in a buffer to place
     *    it entirely into the next QRYDTA block, or reset the result
     *    set cursor back one row to "unread" this row.
     *  - when splitting a row across QRYDTA blocks, we tend to copy
     *    data around multiple times. Careful coding could remove some
     *    of these copies.
     *  However, it is important not to over-complicate this code: it is
     *  better to be correct than to be efficient, and there have been
     *  several bugs in the split logic already.
     *
     * Instance Variables
     *   Byte string
     *
     * @param stmt  DRDA statement we are processing
     * @throws DRDAProtocolException
     * @throws SQLException
     */
    private void writeQRYDTA (DRDAStatement stmt) 
        throws DRDAProtocolException, SQLException
    {
        boolean getMoreData = true;
        boolean sentExtData = false;
        int startLength = 0;
        writer.createDssObject();

        if (SanityManager.DEBUG) {
            trace("Write QRYDTA");
        }
        writer.startDdm(CodePoint.QRYDTA);
        // Check to see if there was leftover data from splitting
        // the previous QRYDTA for this result set. If there was, and
        // if we have now sent all of it, send any EXTDTA for that row
        // and increment the rowCount which we failed to increment in
        // writeFDODTA when we realized the row needed to be split.
        if (processLeftoverQRYDTA(stmt))
        {
            if (stmt.getSplitQRYDTA() == null)
            {
                stmt.rowCount += 1;
                if (stmt.getExtDtaObjects() != null) {
                    writeEXTDTA(stmt);
                }
            }
            return;
        }

        while(getMoreData)
        {
            sentExtData = false;
            getMoreData = writeFDODTA(stmt);

            if (stmt.getExtDtaObjects() != null &&
                    stmt.getSplitQRYDTA() == null)
            {
                writer.endDdmAndDss();
                writeEXTDTA(stmt);
                getMoreData=false;
                sentExtData = true;
            }

            // if we don't have enough room for a row of the 
            // last row's size, don't try to cram it in.
            // It would get split up but it is not very efficient.
            if (getMoreData == true)
            {
                int endLength = writer.getDSSLength();
                int rowsize = endLength - startLength;
                if ((stmt.getBlksize() - endLength ) < rowsize) {
                    getMoreData = false;
                }

                startLength = endLength;
            }

        }
        // If we sent extDta we will rely on
        // writeScalarStream to end the dss with the proper chaining.
        // otherwise end it here.
        if (!sentExtData) {
            writer.endDdmAndDss();
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-821
        if (!stmt.hasdata()) {
            final boolean qryclsOnLmtblkprc =
                appRequester.supportsQryclsimpForLmtblkprc();
            if (stmt.isRSCloseImplicit(qryclsOnLmtblkprc)) {
                stmt.rsClose();
            }
        }
    }

    /**
     * This routine places some data into the current QRYDTA block using
     * FDODTA (Formatted Data Object DaTA rules).
     *
     * There are 3 basic types of processing flow for this routine:
     * - In normal non-rowset, non-scrollable cursor flow, this routine
     *   places a single row into the QRYDTA block and returns TRUE,
     *   indicating that the caller can call us back to place another
     *   row into the result set if he wishes. (The caller may need to
     *   send Externalized Data, which would be a reason for him NOT to
     *   place any more rows into the QRYDTA).
     * - In ROWSET processing, this routine places an entire ROWSET of
     *   rows into the QRYDTA block and returns FALSE, indicating that
     *   the QRYDTA block is full and should now be sent.
     * - In callable statement processing, this routine places the
     *   results from the output parameters of the called procedure into
     *   the QRYDTA block. This code path is really dramatically
     *   different from the other two paths and shares only a very small
     *   amount of common code in this routine.
     *
     * In all cases, it is possible that the data we wish to return may
     * not fit into the QRYDTA block, in which case we call splitQRYDTA
     * to split the data and remember the remainder data in the result set.
     * Splitting the data is relatively rare in the normal cursor case,
     * because our caller (writeQRYDTA) uses a coarse estimation
     * technique to avoid calling us if he thinks a split is likely.
     *
     * The overall structure of this routine is implemented as two
     * loops:
     * - the outer "do ... while ... " loop processes a ROWSET, one row
     *   at a time. For non-ROWSET cursors, and for callable statements,
     *   this loop executes only once.
     * - the inner "for ... i &lt; numCols ..." loop processes each column
     *   in the current row, or each output parmeter in the procedure.
     *
     * Most column data is written directly inline in the QRYDTA block.
     * Some data, however, is written as Externalized Data. This is
     * commonly used for Large Objects. In that case, an Externalized
     * Data Pointer is written into the QRYDTA block, and the actual
     * data flows in separate EXTDTA blocks which are returned
     * after this QRYDTA block.
     */
    private boolean writeFDODTA (DRDAStatement stmt) 
        throws DRDAProtocolException, SQLException
    {
        boolean hasdata;
        int blksize = stmt.getBlksize() > 0 ? stmt.getBlksize() : CodePoint.QRYBLKSZ_MAX;
        long rowCount = 0;
        ResultSet rs =null;
        boolean moreData = (stmt.getQryprctyp()
                            == CodePoint.LMTBLKPRC);
        int  numCols;

        if (!stmt.needsToSendParamData)
        {
            rs = stmt.getResultSet();
        }

        if (rs != null)
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
            numCols = stmt.getNumRsCols();
            if (stmt.isScrollable()) {
                hasdata = positionCursor(stmt, rs);
            } else {
                hasdata = rs.next();
            }
        }
        else    // it's for a CallableStatement
        {
            hasdata = stmt.hasOutputParams();
//IC see: https://issues.apache.org/jira/browse/DERBY-815
            numCols = stmt.getDrdaParamCount();
        }


        do {
            if (!hasdata)
            {
                doneData(stmt, rs);
                moreData = false;
                return moreData;
            }
            
            // Send ResultSet warnings if there are any
            SQLWarning sqlw = (rs != null)? rs.getWarnings(): null;
//IC see: https://issues.apache.org/jira/browse/DERBY-1481
            if (rs != null) {
                rs.clearWarnings();
            }

            // for updatable, insensitive result sets we signal the
            // row updated condition to the client via a warning to be 
            // popped by client onto its rowUpdated state, i.e. this 
            // warning should not reach API level.
            if (rs != null && rs.rowUpdated()) {
//IC see: https://issues.apache.org/jira/browse/DERBY-1374
                SQLWarning w = new SQLWarning("", SQLState.ROW_UPDATED,
//IC see: https://issues.apache.org/jira/browse/DERBY-1313
                        ExceptionSeverity.WARNING_SEVERITY);
                if (sqlw != null) {
                    sqlw.setNextWarning(w);
                } else {
                    sqlw = w;
                }
            }
            // Delete holes are manifest as a row consisting of a non-null
            // SQLCARD and a null data group. The SQLCARD has a warning
            // SQLSTATE of 02502
            if (rs != null && rs.rowDeleted()) {
//IC see: https://issues.apache.org/jira/browse/DERBY-1374
                SQLWarning w = new SQLWarning("", SQLState.ROW_DELETED,
                        ExceptionSeverity.WARNING_SEVERITY);
                if (sqlw != null) {
                    sqlw.setNextWarning(w);
                } else {
                    sqlw = w;
                }
            }

            // Save the position where we start writing the warnings in case
            // we need to add more warnings later.
            final int sqlcagrpStart = writer.getBufferPosition();

            if (sqlw == null) {
//IC see: https://issues.apache.org/jira/browse/DERBY-825
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
                writeSQLCAGRP(nullSQLState, 0, -1, -1);
            } else {
                writeSQLCAGRP(sqlw, 1, -1);
            }

            // Save the position right after the warnings so we know where to
            // insert more warnings later.
            final int sqlcagrpEnd = writer.getBufferPosition();

            // if we were asked not to return data, mark QRYDTA null; do not
            // return yet, need to make rowCount right
            // if the row has been deleted return QRYDTA null (delete hole)
            boolean noRetrieveRS = (rs != null && 
                    (!stmt.getQryrtndta() || rs.rowDeleted()));
            if (noRetrieveRS) {
                writer.writeByte(0xFF);  //QRYDTA null indicator: IS NULL
            } else {
                writer.writeByte(0);  //QRYDTA null indicator: not null
            }

            for (int i = 1; i <= numCols; i++)
            {
                if (noRetrieveRS) {
                    break;
                }

                int drdaType;
                int ndrdaType;
                int precision;
                int scale;

                boolean valNull;
                if (rs != null)
                {
                    drdaType =   stmt.getRsDRDAType(i) & 0xff;
                    precision = stmt.getRsPrecision(i);
                    scale = stmt.getRsScale(i);
                    ndrdaType = drdaType | 1;

                    if (SanityManager.DEBUG) {
                        trace("!!drdaType = " + java.lang.Integer.toHexString(drdaType) + 
//IC see: https://issues.apache.org/jira/browse/DERBY-2941
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
                                 " precision=" + precision +" scale = " + scale);
                    }
                    switch (ndrdaType)
                    {
//IC see: https://issues.apache.org/jira/browse/DERBY-499
                        case DRDAConstants.DRDA_TYPE_NLOBBYTES:
                        case  DRDAConstants.DRDA_TYPE_NLOBCMIXED:
                            EXTDTAInputStream extdtaStream=  
                                EXTDTAInputStream.getEXTDTAStream(rs, i, drdaType);
                            writeFdocaVal(i, extdtaStream, drdaType, precision,
                                    scale, extdtaStream.isNull(), stmt, false);
                            break;
                        case DRDAConstants.DRDA_TYPE_NINTEGER:
                            int ival = rs.getInt(i);
                            valNull = rs.wasNull();
                            if (SanityManager.DEBUG) {
                                trace("====== writing int: "+ ival + " is null: " + valNull);
                            }
                            writeNullability(drdaType,valNull);
                            if (!valNull) {
                                writer.writeInt(ival);
                            }
                            break;
//IC see: https://issues.apache.org/jira/browse/DERBY-499
                        case DRDAConstants.DRDA_TYPE_NSMALL:
                            short sval = rs.getShort(i);
                            valNull = rs.wasNull();
                            if (SanityManager.DEBUG) {
                                trace("====== writing small: "+ sval + " is null: " + valNull);
                            }
                            writeNullability(drdaType,valNull);
                            if (!valNull) {
                                writer.writeShort(sval);
                            }
                            break;
//IC see: https://issues.apache.org/jira/browse/DERBY-499
                        case DRDAConstants.DRDA_TYPE_NINTEGER8:
                            long lval = rs.getLong(i);
                            valNull = rs.wasNull();
                            if (SanityManager.DEBUG) {
                                trace("====== writing long: "+ lval + " is null: " + valNull);
                            }
                            writeNullability(drdaType,valNull);
                            if (!valNull) {
                                writer.writeLong(lval);
                            }
                            break;
//IC see: https://issues.apache.org/jira/browse/DERBY-499
                        case DRDAConstants.DRDA_TYPE_NFLOAT4:
                            float fval = rs.getFloat(i);
                            valNull = rs.wasNull();
                            if (SanityManager.DEBUG) {
                                trace("====== writing float: "+ fval + " is null: " + valNull);
                            }
                            writeNullability(drdaType,valNull);
                            if (!valNull) {
                                writer.writeFloat(fval);
                            }
                            break;
//IC see: https://issues.apache.org/jira/browse/DERBY-499
                        case DRDAConstants.DRDA_TYPE_NFLOAT8:
                            double dval = rs.getDouble(i);
                            valNull = rs.wasNull();
                            if (SanityManager.DEBUG) {
                                trace("====== writing double: "+ dval + " is null: " + valNull);
                            }
                            writeNullability(drdaType,valNull);
                            if (!valNull) {
                                writer.writeDouble(dval);
                            }
                            break;
//IC see: https://issues.apache.org/jira/browse/DERBY-499
                        case DRDAConstants.DRDA_TYPE_NCHAR:
                        case DRDAConstants.DRDA_TYPE_NVARCHAR:
                        case DRDAConstants.DRDA_TYPE_NVARMIX:
                        case DRDAConstants.DRDA_TYPE_NLONG:
                        case DRDAConstants.DRDA_TYPE_NLONGMIX:
                            String valStr = rs.getString(i);
                            if (SanityManager.DEBUG) {
                                trace("====== writing char/varchar/mix :"+ valStr + ":");
                            }
                            writeFdocaVal(i, valStr, drdaType,
                                          precision, scale, rs.wasNull(),
                                          stmt, false);
                            break;
                        default:
                            Object val =
                                getObjectForWriteFdoca(rs, i, drdaType);
                            writeFdocaVal(i, val, drdaType,
                                          precision, scale, rs.wasNull(),
                                          stmt, false);
                    }
                }
                else
                {
                                    
                    drdaType =   stmt.getParamDRDAType(i) & 0xff;
                    precision = stmt.getParamPrecision(i);
                    scale = stmt.getParamScale(i);
                    
                    if (stmt.isOutputParam(i)) {
                        int[] outlen = new int[1];
                        drdaType = FdocaConstants.mapJdbcTypeToDrdaType( stmt.getOutputParamType(i), true, appRequester, outlen );
//IC see: https://issues.apache.org/jira/browse/DERBY-2381
                        precision = stmt.getOutputParamPrecision(i);
                        scale = stmt.getOutputParamScale(i);
                                                
                        if (SanityManager.DEBUG) {
                            trace("***getting Object "+i);
                        }
                        Object val = getObjectForWriteFdoca(
                                (CallableStatement) stmt.ps, i, drdaType);
                        valNull = (val == null);
                        writeFdocaVal(i, val, drdaType, precision, scale,
                                      valNull, stmt, true);
                    }
                    else {
                        writeFdocaVal(i, null, drdaType, precision, scale,
                                      true, stmt, true);
                    }
                }
            }

            DataTruncation truncated = stmt.getTruncationWarnings();
            if (truncated != null) {
                // Some of the data was truncated, so we need to add a
                // truncation warning. Save a copy of the row data, then move
                // back to the SQLCAGRP section and overwrite it with the new
                // warnings, and finally re-insert the row data after the new
                // SQLCAGRP section.
                byte[] data = writer.getBufferContents(sqlcagrpEnd);
                writer.setBufferPosition(sqlcagrpStart);
                if (sqlw != null) {
                    truncated.setNextWarning(sqlw);
                }
                writeSQLCAGRP(truncated, 1, -1);
                writer.writeBytes(data);
                stmt.clearTruncationWarnings();
            }

            // does all this fit in one QRYDTA
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
            if (writer.getDSSLength() > blksize)
            {
                splitQRYDTA(stmt, blksize);
                return false;
            }

            if (rs == null) {
                return moreData;
            }

            //get the next row
            rowCount++;
            if (rowCount < stmt.getQryrowset())
            {
                hasdata = rs.next();
            }
            /*(1) scrollable we return at most a row set; OR (2) no retrieve data
             */
            else if (stmt.isScrollable() || noRetrieveRS)
            {
                moreData=false;
            }

        } while (hasdata && rowCount < stmt.getQryrowset());

        // add rowCount to statement row count
        // for non scrollable cursors
        if (!stmt.isScrollable()) {
            stmt.rowCount += rowCount;
        }

        if (!hasdata)
        {
            doneData(stmt, rs);
            moreData=false;
        }

        if (!stmt.isScrollable()) {
            stmt.setHasdata(hasdata);
        }

        return moreData;
    }

    /**
     * <p>
     * Get a column value of the specified type from a {@code ResultSet}, in
     * a form suitable for being writted by {@link #writeFdocaVal}. For most
     * types, this means just calling {@code ResultSet.getObject(int)}.
     * </p>
     *
     * <p>
     * The only exception currently is the data types representing dates and
     * times, as they need to be fetched using the same
     * {@code java.util.Calendar} as {@link #writeFdocaVal} uses when writing
     * them (DERBY-4582).
     * </p>
     *
     * <p>
     * <b>Note:</b> Changes made in this method should also be made in the
     * corresponding method for {@code CallableStatement}:
     * {@link #getObjectForWriteFdoca(java.sql.CallableStatement, int, int)}.
     * </p>
     *
     * @param rs the result set to fetch the object from
     * @param index the column index
     * @param drdaType the DRDA type of the object to fetch
     * @return an object with the value of the column
     * @throws if a database error occurs while fetching the column value
     * @see #getObjectForWriteFdoca(java.sql.CallableStatement, int, int)
     */
    private Object getObjectForWriteFdoca(ResultSet rs, int index, int drdaType)
            throws SQLException {
        // convert to corresponding nullable type to reduce number of cases
        int ndrdaType = drdaType | 1;
        switch (ndrdaType) {
            case DRDAConstants.DRDA_TYPE_NDATE:
                return rs.getDate(index, getGMTCalendar());
            case DRDAConstants.DRDA_TYPE_NTIME:
                return rs.getTime(index, getGMTCalendar());
            case DRDAConstants.DRDA_TYPE_NTIMESTAMP:
                return rs.getTimestamp(index, getGMTCalendar());
            default:
                return rs.getObject(index);
        }
    }

    /**
     * <p>
     * Get the value of an output parameter of the specified type from a
     * {@code CallableStatement}, in a form suitable for being writted by
     * {@link #writeFdocaVal}. For most types, this means just calling
     * {@code CallableStatement.getObject(int)}.
     * </p>
     *
     * <p>
     * This method should behave like the corresponding method for
     * {@code ResultSet}, and changes made to one of these methods, must be
     * reflected in the other method. See
     * {@link #getObjectForWriteFdoca(java.sql.ResultSet, int, int)}
     * for details.
     * </p>
     *
     * @param cs the callable statement to fetch the object from
     * @param index the parameter index
     * @param drdaType the DRDA type of the object to fetch
     * @return an object with the value of the output parameter
     * @throws if a database error occurs while fetching the parameter value
     * @see #getObjectForWriteFdoca(java.sql.ResultSet, int, int)
     */
    private Object getObjectForWriteFdoca(CallableStatement cs,
                                          int index, int drdaType)
            throws SQLException {
        // convert to corresponding nullable type to reduce number of cases
        int ndrdaType = drdaType | 1;
        switch (ndrdaType) {
            case DRDAConstants.DRDA_TYPE_NDATE:
                return cs.getDate(index, getGMTCalendar());
            case DRDAConstants.DRDA_TYPE_NTIME:
                return cs.getTime(index, getGMTCalendar());
            case DRDAConstants.DRDA_TYPE_NTIMESTAMP:
                return cs.getTimestamp(index, getGMTCalendar());
//IC see: https://issues.apache.org/jira/browse/DERBY-4066
            case DRDAConstants.DRDA_TYPE_NLOBBYTES:
            case  DRDAConstants.DRDA_TYPE_NLOBCMIXED:
                return EXTDTAInputStream.getEXTDTAStream(cs, index, drdaType);
            default:
                return cs.getObject(index);
        }
    }

    /**
     * Split QRYDTA into blksize chunks
     *
     * This routine is called if the QRYDTA data will not fit. It writes
     * as much data as it can, then stores the remainder in the result
     * set. At some later point, when the client returns with a CNTQRY,
     * we will call processLeftoverQRYDTA to handle that data.
     *
     * The interaction between DRDAConnThread and DDMWriter is rather
     * complicated here. This routine gets called because DRDAConnThread
     * realizes that it has constructed a QRYDTA message which is too
     * large. At that point, we need to reclaim the "extra" data and
     * hold on to it. To aid us in that processing, DDMWriter provides
     * the routines getDSSLength, copyDSSDataToEnd, and truncateDSS.
     * For some additional detail on this complex sub-protocol, the
     * interested reader should study bug DERBY-491 and 492 at:
     * http://issues.apache.org/jira/browse/DERBY-491 and
     * http://issues.apache.org/jira/browse/DERBY-492
     *
     * @param stmt DRDA statment
     * @param blksize size of query block
     * 
     * @throws SQLException
     * @throws DRDAProtocolException
     */
    private void splitQRYDTA(DRDAStatement stmt, int blksize) throws SQLException, 
            DRDAProtocolException
    {
        // make copy of extra data
        byte [] temp = writer.copyDSSDataToEnd(blksize);
        // truncate to end of blocksize
        writer.truncateDSS(blksize);
        if (temp.length == 0) {
            agentError("LMTBLKPRC violation: splitQRYDTA was " +
                "called to split a QRYDTA block, but the " +
                "entire row fit successfully into the " +
                "current block. Server rowsize computation " +
                "was probably incorrect (perhaps an off-by-" +
                "one bug?). QRYDTA blocksize: " + blksize);
        }
        stmt.setSplitQRYDTA(temp);
    }
    /**
     * Process remainder data resulting from a split.
     *
     * This routine is called at the start of building each QRYDTA block.
     * Normally, it observes that there is no remainder data from the
     * previous QRYDTA block, and returns FALSE, indicating that there
     * was nothing to do.
     *
     * However, if it discovers that the previous QRYDTA block was split,
     * then it retrieves the remainder data from the result set, writes
     * as much of it as will fit into the QRYDTA block (hopefully all of
     * it will fit, but the row may be very long), and returns TRUE,
     * indicating that this QRYDTA block has been filled with remainder
     * data and should now be sent immediately.
     */
    private boolean processLeftoverQRYDTA(DRDAStatement stmt)
        throws SQLException,DRDAProtocolException
    {
        byte []leftovers = stmt.getSplitQRYDTA();
        if (leftovers == null) {
            return false;
        }
        int blksize = stmt.getBlksize() > 0 ? stmt.getBlksize() : CodePoint.QRYBLKSZ_MAX;
        blksize = blksize - 10; //DSS header + QRYDTA and length
        if (leftovers.length < blksize)
        {
            writer.writeBytes(leftovers, 0, leftovers.length);
            stmt.setSplitQRYDTA(null);
        }
        else
        {
            writer.writeBytes(leftovers, 0, blksize);
            byte []newLeftovers = new byte[leftovers.length-blksize];
            System.arraycopy(
                leftovers, blksize, newLeftovers, 0, newLeftovers.length);
            stmt.setSplitQRYDTA(newLeftovers);
        }
        // finish off query block and send
        writer.endDdmAndDss();
        return true;
    }


    /**
     * Done data
     * Send SQLCARD for the end of the data
     * 
     * @param stmt DRDA statement
     * @param rs Result set
     * @throws DRDAProtocolException
     * @throws SQLException
     */
    private void doneData(DRDAStatement stmt, ResultSet rs) 
            throws DRDAProtocolException, SQLException
    {
        if (SanityManager.DEBUG) {
            trace("*****NO MORE DATA!!");
        }
        int blksize = stmt.getBlksize() > 0 ? stmt.getBlksize() : CodePoint.QRYBLKSZ_MAX;
        if (rs != null)
        {
            if (stmt.isScrollable())
            {
                                //keep isAfterLast and isBeforeFirst to be able 
                                //to reposition after counting rows
                                boolean isAfterLast = rs.isAfterLast();
                                boolean isBeforeFirst = rs.isBeforeFirst();
                                
                // for scrollable cursors - calculate the row count
                // since we may not have gone through each row
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
                rs.last();
                stmt.rowCount  = rs.getRow();

                                // reposition after last or before first
//IC see: https://issues.apache.org/jira/browse/DERBY-517
                                if (isAfterLast) {
                                    rs.afterLast();
                                }
                                if (isBeforeFirst) {
                                    rs.beforeFirst();
                                } 
            }
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
            else  // non-scrollable cursor
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-821
                final boolean qryclsOnLmtblkprc =
                    appRequester.supportsQryclsimpForLmtblkprc();
                if (stmt.isRSCloseImplicit(qryclsOnLmtblkprc)) {
                    stmt.rsClose();
                    stmt.rsSuspend();
                }
             
            }
        }

        // For scrollable cursor's QRYSCRAFT, when we reach here, DRDA spec says sqlstate
        // is 00000, sqlcode is not mentioned.  But DB2 CLI code expects sqlcode to be 0.
        // We return sqlcode 0 in this case, as the DB2 server does.
        boolean isQRYSCRAFT = (stmt.getQryscrorn() == CodePoint.QRYSCRAFT);

        // Using sqlstate 00000 or 02000 for end of data.
//IC see: https://issues.apache.org/jira/browse/DERBY-825
                writeSQLCAGRP((isQRYSCRAFT ? eod00000 : eod02000),
                              (isQRYSCRAFT ? 0 : 100), 0, stmt.rowCount);
                
        writer.writeByte(CodePoint.NULLDATA);
        // does all this fit in one QRYDTA
//IC see: https://issues.apache.org/jira/browse/DERBY-1454
//IC see: https://issues.apache.org/jira/browse/DERBY-1454
        if (writer.getDSSLength() > blksize)
        {
            splitQRYDTA(stmt, blksize);
        }
    }
    /**
     * Position cursor for insensitive scrollable cursors
     *
     * @param stmt  DRDA statement
     * @param rs    Result set
     */
    private boolean positionCursor(DRDAStatement stmt, ResultSet rs) 
        throws SQLException, DRDAProtocolException
    {
        boolean retval = false;
        switch (stmt.getQryscrorn())
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-1302
//IC see: https://issues.apache.org/jira/browse/DERBY-326
            case CodePoint.QRYSCRREL:
                                int rows = (int)stmt.getQryrownbr();
//IC see: https://issues.apache.org/jira/browse/DERBY-517
                                if ((rs.isAfterLast() && rows > 0) || (rs.isBeforeFirst() && rows < 0)) {
                                    retval = false;
                                } else {
                                    retval = rs.relative(rows);
                                }
                                break;
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
            case CodePoint.QRYSCRABS:
                // JCC uses an absolute value of 0 which is not allowed in JDBC
                // We translate it into beforeFirst which seems to work.
                if (stmt.getQryrownbr() == 0)
                {
                    rs.beforeFirst();
                    retval = false;
                }
                else
                {
                    retval = rs.absolute((int)stmt.getQryrownbr());
                }
                break;
            case CodePoint.QRYSCRAFT:
                rs.afterLast();
                retval = false;
                break;
            case CodePoint.QRYSCRBEF:
                rs.beforeFirst();
                retval = false;
                break;
            default:      
                agentError("Invalid value for cursor orientation "+ stmt.getQryscrorn());
        }
        return retval;
    }
    /**
     * Write SQLDAGRP
     * SQLDAGRP : EARLY FDOCA GROUP
     * SQL Data Area Group Description
     *
     * FORMAT FOR SQLAM &lt;= 6
     *   SQLPRECISION; DRDA TYPE I2; ENVLID 0x04; Length Override 2
     *   SQLSCALE; DRDA TYPE I2; ENVLID 0x04; Length Override 2
     *   SQLLENGTH; DRDA TYPE I4; ENVLID 0x02; Length Override 4
     *   SQLTYPE; DRDA TYPE I2; ENVLID 0x04; Length Override 2
     *   SQLCCSID; DRDA TYPE FB; ENVLID 0x26; Length Override 2
     *   SQLNAME_m; DRDA TYPE VCM; ENVLID 0x3E; Length Override 30
     *   SQLNAME_s; DRDA TYPE VCS; ENVLID 0x32; Length Override 30
     *   SQLLABEL_m; DRDA TYPE VCM; ENVLID 0x3E; Length Override 30
     *   SQLLABEL_s; DRDA TYPE VCS; ENVLID 0x32; Length Override 30
     *   SQLCOMMENTS_m; DRDA TYPE VCM; ENVLID 0x3E; Length Override 254
     *   SQLCOMMENTS_m; DRDA TYPE VCS; ENVLID 0x32; Length Override 254
     *
     * FORMAT FOR SQLAM == 6
     *   SQLPRECISION; DRDA TYPE I2; ENVLID 0x04; Length Override 2
     *   SQLSCALE; DRDA TYPE I2; ENVLID 0x04; Length Override 2
     *   SQLLENGTH; DRDA TYPE I8; ENVLID 0x16; Length Override 8
     *   SQLTYPE; DRDA TYPE I2; ENVLID 0x04; Length Override 2
     *   SQLCCSID; DRDA TYPE FB; ENVLID 0x26; Length Override 2
     *   SQLNAME_m; DRDA TYPE VCM; ENVLID 0x3E; Length Override 30
     *   SQLNAME_s; DRDA TYPE VCS; ENVLID 0x32; Length Override 30
     *   SQLLABEL_m; DRDA TYPE VCM; ENVLID 0x3E; Length Override 30
     *   SQLLABEL_s; DRDA TYPE VCS; ENVLID 0x32; Length Override 30
     *   SQLCOMMENTS_m; DRDA TYPE VCM; ENVLID 0x3E; Length Override 254
     *   SQLCOMMENTS_m; DRDA TYPE VCS; ENVLID 0x32; Length Override 254
     *   SQLUDTGRP; DRDA TYPE N-GDA; ENVLID 0x51; Length Override 0
     *
     * FORMAT FOR SQLAM &gt;= 7
     *   SQLPRECISION; DRDA TYPE I2; ENVLID 0x04; Length Override 2
     *   SQLSCALE; DRDA TYPE I2; ENVLID 0x04; Length Override 2
     *   SQLLENGTH; DRDA TYPE I8; ENVLID 0x16; Length Override 8
     *   SQLTYPE; DRDA TYPE I2; ENVLID 0x04; Length Override 2
     *   SQLCCSID; DRDA TYPE FB; ENVLID 0x26; Length Override 2
     *   SQLDOPTGRP; DRDA TYPE N-GDA; ENVLID 0xD2; Length Override 0
     *
     * @param rsmeta    resultset meta data
     * @param pmeta     parameter meta data
     * @param elemNum   column number we are returning (in case of result set), or,
     *                  parameter number (in case of parameter)
     * @param rtnOutput whether this is for a result set
     *
     * @throws DRDAProtocolException
     * @throws SQLException
     */
    private void writeSQLDAGRP(ResultSetMetaData rsmeta,
//IC see: https://issues.apache.org/jira/browse/DERBY-2121
                               ParameterMetaData pmeta,
                               int elemNum, boolean rtnOutput)
        throws DRDAProtocolException, SQLException
    {
        //jdbc uses offset of 1

        int jdbcElemNum = elemNum +1;
        // length to be retreived as output parameter
        int[]  outlen = {-1};  

        int elemType = rtnOutput ? rsmeta.getColumnType(jdbcElemNum) : pmeta.getParameterType(jdbcElemNum);

        int precision = Math.min(
                FdocaConstants.NUMERIC_MAX_PRECISION,
                rtnOutput ? rsmeta.getPrecision(jdbcElemNum) :
                            pmeta.getPrecision(jdbcElemNum));

        // 2-byte precision
        writer.writeShort(precision);
        // 2-byte scale
        int scale = (rtnOutput ? rsmeta.getScale(jdbcElemNum) : pmeta.getScale(jdbcElemNum));
        writer.writeShort(scale);

        boolean nullable = rtnOutput ?
//IC see: https://issues.apache.org/jira/browse/DERBY-5847
//IC see: https://issues.apache.org/jira/browse/DERBY-436
         (rsmeta.isNullable(jdbcElemNum) == ResultSetMetaData.columnNullable) :
         (pmeta.isNullable(jdbcElemNum) == ParameterMetaData.parameterNullable);
        
        int sqlType = SQLTypes.mapJdbcTypeToDB2SqlType(elemType,
                                                       nullable, appRequester,
                                                       outlen);

        if (outlen[0] == -1) //some types not set
        {
            switch (elemType)
            {
                case Types.DECIMAL:
                case Types.NUMERIC:
                    scale = rtnOutput ? rsmeta.getScale(jdbcElemNum) : pmeta.getScale(jdbcElemNum);
                    outlen[0] = ((precision <<8) | scale);
                    if (SanityManager.DEBUG) {
                        trace("\n\nprecision =" +precision +
                          " scale =" + scale);
                    }
                    break;
                default:
                    outlen[0] = Math.min(FdocaConstants.LONGVARCHAR_MAX_LEN,
                                        (rtnOutput ? rsmeta.getColumnDisplaySize(jdbcElemNum) :
                                                pmeta.getPrecision(jdbcElemNum)));
            }
        }

        switch (elemType)
        {
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:            //for CLI describe to be correct
            case Types.CLOB:
                outlen[0] = (rtnOutput ? rsmeta.getPrecision(jdbcElemNum) :
                                            pmeta.getPrecision(jdbcElemNum));
        }

        if (SanityManager.DEBUG) {
            trace("SQLDAGRP len =" + java.lang.Integer.toHexString(outlen[0]) + "for type:" + elemType);
        }

       // 8 or 4 byte sqllength
        if (sqlamLevel >= MGRLVL_6) {
            writer.writeLong(outlen[0]);
        } else {
            writer.writeInt(outlen[0]);
        }


        String typeName = rtnOutput ? rsmeta.getColumnTypeName(jdbcElemNum) :
                                        pmeta.getParameterTypeName(jdbcElemNum);
        if (SanityManager.DEBUG) {
            trace("jdbcType =" + typeName + "  sqlType =" + sqlType + "len =" +outlen[0]);
        }

        writer.writeShort(sqlType);

        // CCSID
        // CCSID should be 0 for Binary Types.
        
        if (elemType == java.sql.Types.CHAR ||
            elemType == java.sql.Types.VARCHAR
            || elemType == java.sql.Types.LONGVARCHAR
            || elemType == java.sql.Types.CLOB) {
            writer.writeScalar2Bytes(1208);
        } else {
            writer.writeScalar2Bytes(0);
        }

        if (sqlamLevel < MGRLVL_7) 
        {

            //SQLName
            writeVCMorVCS(rtnOutput ? rsmeta.getColumnName(jdbcElemNum) : null);
            //SQLLabel
            writeVCMorVCS(null);
            //SQLComments
            writeVCMorVCS(null);

            if (sqlamLevel == MGRLVL_6) {
                writeSQLUDTGRP(rsmeta, pmeta, jdbcElemNum, rtnOutput);
            }
        }
        else
        {
            writeSQLDOPTGRP(rsmeta, pmeta, jdbcElemNum, rtnOutput);
        }

    }

    /**
     * Write variable character mixed byte or single byte
     * The preference is to write mixed byte if it is defined for the server,
     * since that is our default and we don't allow it to be changed, we always
     * write mixed byte.
     * 
     * @param s string to write
     * @exception DRDAProtocolException
     */
    private void writeVCMorVCS(String s)
        throws DRDAProtocolException
    {
        //Write only VCM and 0 length for VCS

        if (s == null)
        {
            writer.writeShort(0);
            writer.writeShort(0);
            return;
        }

        // VCM
        writer.writeLDString(s);
        // VCS
        writer.writeShort(0);
    }

  
    /**
     * Write SQLUDTGRP (SQL Descriptor User-Defined Type Group Descriptor)
     * 
     * This is the format from the DRDA spec, Volume 1, section 5.6.4.10.
     * However, this format is not rich enough to carry the information needed
     * by JDBC. This format does not have a subtype code for JAVA_OBJECT and
     * this format does not convey the Java class name needed
     * by ResultSetMetaData.getColumnClassName().
     *
     *   SQLUDXTYPE; DRDA TYPE I4; ENVLID 0x02; Length Override 4
     *                        Constants which map to java.sql.Types constants DISTINCT, STRUCT, and REF.
     *                        But DRDA does not define a constant which maps to java.sql.Types.JAVA_OBJECT.
     *   SQLUDTRDB; DRDA TYPE VCS; ENVLID 0x32; Length Override 255
     *                       Database name.
     *   SQLUDTSCHEMA_m; DRDA TYPE VCM; ENVLID 0x3E; Length Override 255
     *   SQLUDTSCHEMA_s; DRDA TYPE VCS; ENVLID 0x32; Length Override 255
     *                         Schema name. One of the above.
     *   SQLUDTNAME_m; DRDA TYPE VCM; ENVLID 0x3E; Length Override 255
     *   SQLUDTNAME_s; DRDA TYPE VCS; ENVLID 0x32; Length Override 255
     *                         Unqualified UDT name. One of the above.
     *
     * Instead, we use the following format and only for communication between
     * Derby servers and Derby clients which are both at version 10.6 or higher.
     * For all other client/server combinations, we send null.
     *
     *   SQLUDTNAME_m; DRDA TYPE VCM; ENVLID 0x3E; Length Override 255
     *   SQLUDTNAME_s; DRDA TYPE VCS; ENVLID 0x32; Length Override 255
     *                         Fully qualified UDT name. One of the above.
     *   SQLUDTCLASSNAME_m; DRDA TYPE VCM; ENVLID 0x3E; Length Override FdocaConstants.LONGVARCHAR_MAX_LEN
     *   SQLUDTCLASSNAME_s; DRDA TYPE VCS; ENVLID 0x32; Length Override FdocaConstants.LONGVARCHAR_MAX_LEN
     *                         Name of the Java class bound to the UDT. One of the above.
     *
     * @param rsmeta    resultset meta data
     * @param pmeta     parameter meta data
     * @param jdbcElemNum   column number we are returning (in case of result set), or,
     *                  parameter number (in case of parameter)
     * @param rtnOutput whether this is for a result set
     *
     * @throws DRDAProtocolException
     * @throws SQLException
     */
    private void writeSQLUDTGRP(ResultSetMetaData rsmeta,
//IC see: https://issues.apache.org/jira/browse/DERBY-2121
                                ParameterMetaData pmeta,
                                int jdbcElemNum, boolean rtnOutput)
        throws DRDAProtocolException,SQLException
    {
        int jdbcType = rtnOutput ?
            rsmeta.getColumnType( jdbcElemNum) : pmeta.getParameterType( jdbcElemNum );

        if ( !(jdbcType == Types.JAVA_OBJECT) || !appRequester.supportsUDTs() )
        {
            writer.writeByte(CodePoint.NULLDATA);
            return;
        }
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        String typeName = rtnOutput ?
            rsmeta.getColumnTypeName( jdbcElemNum ) : pmeta.getParameterTypeName( jdbcElemNum );
        String className = rtnOutput ?
            rsmeta.getColumnClassName( jdbcElemNum ) : pmeta.getParameterClassName( jdbcElemNum );
        
        writeVCMorVCS( typeName );
        writeVCMorVCS( className );
    }

    private void writeSQLDOPTGRP(ResultSetMetaData rsmeta,
                                 ParameterMetaData pmeta,
                                 int jdbcElemNum, boolean rtnOutput)
        throws DRDAProtocolException,SQLException
    {

        writer.writeByte(0);
        //SQLUNAMED
        writer.writeShort(0);
        //SQLName
        writeVCMorVCS(rtnOutput ? rsmeta.getColumnName(jdbcElemNum) : null);
        //SQLLabel
        writeVCMorVCS(null);
        //SQLComments
        writeVCMorVCS(null);
        //SQLDUDTGRP 
        writeSQLUDTGRP(rsmeta, pmeta, jdbcElemNum, rtnOutput);
        //SQLDXGRP
        writeSQLDXGRP(rsmeta, pmeta, jdbcElemNum, rtnOutput);
    }


    private void writeSQLDXGRP(ResultSetMetaData rsmeta,
//IC see: https://issues.apache.org/jira/browse/DERBY-2121
                               ParameterMetaData pmeta,
                               int jdbcElemNum, boolean rtnOutput)
        throws DRDAProtocolException,SQLException
    {
        // Null indicator indicates we have data
        writer.writeByte(0);
        //   SQLXKEYMEM; DRDA TYPE I2; ENVLID 0x04; Length Override 2
        // Hard to get primary key info. Send 0 for now
        writer.writeShort(0);
        //   SQLXUPDATEABLE; DRDA TYPE I2; ENVLID 0x04; Length Override 2
        writer.writeShort(rtnOutput ? rsmeta.isWritable(jdbcElemNum) : false);

        //   SQLXGENERATED; DRDA TYPE I2; ENVLID 0x04; Length Override 2
        if (rtnOutput && rsmeta.isAutoIncrement(jdbcElemNum)) {
            writer.writeShort(2);
        } else {
            writer.writeShort(0);
        }

        //   SQLXPARMMODE; DRDA TYPE I2; ENVLID 0x04; Length Override 2
        if (pmeta != null && !rtnOutput)
        {
            int mode = pmeta.getParameterMode(jdbcElemNum);
//IC see: https://issues.apache.org/jira/browse/DERBY-5847
//IC see: https://issues.apache.org/jira/browse/DERBY-436
            if (mode ==  ParameterMetaData.parameterModeUnknown)
            {
                // For old style callable statements. We assume in/out if it
                // is an output parameter.
                int type =  DRDAStatement.getOutputParameterTypeFromClassName(
                                                                              pmeta.getParameterClassName(jdbcElemNum));
                if (type != DRDAStatement.NOT_OUTPUT_PARAM) {
                    mode = ParameterMetaData.parameterModeInOut;
                }
            }
            writer.writeShort(mode);
        }
        else
        {
            writer.writeShort(0);
        }
    
        //   SQLXRDBNAM; DRDA TYPE VCS; ENVLID 0x32; Length Override 1024
        // JCC uses this as the catalog name so we will send null.
        writer.writeShort(0);

        //   SQLXCORNAME_m; DRDA TYPE VCM; ENVLID 0x3E; Length Override 255
        //   SQLXCORNAME_s; DRDA TYPE VCS; ENVLID 0x32; Length Override 255
        writeVCMorVCS(null);

        //   SQLXBASENAME_m; DRDA TYPE VCM; ENVLID 0x3E; Length Override 255
        //   SQLXBASENAME_s; DRDA TYPE VCS; ENVLID 0x32; Length Override 255
        writeVCMorVCS(rtnOutput ? rsmeta.getTableName(jdbcElemNum) : null);

        //   SQLXSCHEMA_m; DRDA TYPE VCM; ENVLID 0x3E; Length Override 255
        //   SQLXSCHEMA_s; DRDA TYPE VCS; ENVLID 0x32; Length Override 255
        writeVCMorVCS(rtnOutput ? rsmeta.getSchemaName(jdbcElemNum): null);


        //   SQLXNAME_m; DRDA TYPE VCM; ENVLID 0x3E; Length Override 255
        //   SQLXNAME_s; DRDA TYPE VCS; ENVLID 0x32; Length Override 255
        writeVCMorVCS(rtnOutput ? rsmeta.getColumnName(jdbcElemNum): null);
        
    }

  /**
   * Write Fdoca Value to client 
   * @param index     Index of column being returned
   * @param val       Value to write to client
   * @param drdaType  FD:OCA DRDA Type from FdocaConstants
   * @param precision Precision
   * @param stmt       Statement being processed
   * @param isParam   True when writing a value for a procedure parameter
   *
   * @exception DRDAProtocolException  
   * 
   * @exception SQLException
   *
   * @see FdocaConstants
   */

    protected void writeFdocaVal(int index, Object val, int drdaType,
                                 int precision, int scale, boolean valNull,
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
                                 DRDAStatement stmt, boolean isParam)
            throws DRDAProtocolException, SQLException
    {
        writeNullability(drdaType,valNull);

        if (! valNull)
        {
            int ndrdaType = drdaType | 1;
            long valLength = 0;
            switch (ndrdaType)
            {
                case DRDAConstants.DRDA_TYPE_NBOOLEAN:
                    writer.writeBoolean( ((Boolean) val).booleanValue() );
                    break;
                case DRDAConstants.DRDA_TYPE_NSMALL:
                    // DB2 does not have a BOOLEAN java.sql.bit type,
                    // so we need to send it as a small
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
                    if (val instanceof Boolean)
                    {
                        writer.writeShort(((Boolean) val).booleanValue());
                    }
                    else
                    {
                        writer.writeShort(((Number) val).shortValue());
                    }
                    break;
//IC see: https://issues.apache.org/jira/browse/DERBY-499
                case  DRDAConstants.DRDA_TYPE_NINTEGER:
                    writer.writeInt(((Integer) val).intValue());
                    break;
                case DRDAConstants.DRDA_TYPE_NINTEGER8:
                    writer.writeLong(((Long) val).longValue());
                    break;
                case DRDAConstants.DRDA_TYPE_NFLOAT4:
                    writer.writeFloat(((Float) val).floatValue());
                    break;
                case DRDAConstants.DRDA_TYPE_NFLOAT8:
                    writer.writeDouble(((Double) val).doubleValue());
                    break;
                case DRDAConstants.DRDA_TYPE_NDECIMAL:
                    if (precision == 0) {
                        precision = FdocaConstants.NUMERIC_DEFAULT_PRECISION;
                    }
                    BigDecimal bd = (java.math.BigDecimal) val;
                    writer.writeBigDecimal(bd,precision,scale);
                    break;
                case DRDAConstants.DRDA_TYPE_NDATE:
                    writer.writeString(formatDate((java.sql.Date) val));
                    break;
                case DRDAConstants.DRDA_TYPE_NTIME:
                    writer.writeString(formatTime((Time) val));
                    break;
                case DRDAConstants.DRDA_TYPE_NTIMESTAMP:
                    writer.writeString(formatTimestamp((Timestamp) val));
                    break;
//IC see: https://issues.apache.org/jira/browse/DERBY-499
                case DRDAConstants.DRDA_TYPE_NCHAR:
                    writer.writeString(((String) val).toString());
                    break;
                case DRDAConstants.DRDA_TYPE_NVARCHAR:
                case DRDAConstants.DRDA_TYPE_NVARMIX:
                case DRDAConstants.DRDA_TYPE_NLONG:
                case DRDAConstants.DRDA_TYPE_NLONGMIX:
                    //WriteLDString and generate warning if truncated
                    // which will be picked up by checkWarning()
                    writer.writeLDString(val.toString(), index, stmt, isParam);
                    break;
                case DRDAConstants.DRDA_TYPE_NLOBBYTES:
                case DRDAConstants.DRDA_TYPE_NLOBCMIXED:

                    // do not send EXTDTA for lob of length 0, beetle 5967
//IC see: https://issues.apache.org/jira/browse/DERBY-326
                    if( ! ((EXTDTAInputStream) val).isEmptyStream() ){
                        stmt.addExtDtaObject(val, index);
                    
                    //indicate externalized and size is unknown.
                    writer.writeExtendedLength(0x8000);
                    
                    }else{
                    writer.writeExtendedLength(0);
                    
                    }
                    
                    break;
                    
                case  DRDAConstants.DRDA_TYPE_NFIXBYTE:
                    writer.writeBytes((byte[]) val);
                    break;
//IC see: https://issues.apache.org/jira/browse/DERBY-499
                case DRDAConstants.DRDA_TYPE_NVARBYTE:
                case DRDAConstants.DRDA_TYPE_NLONGVARBYTE:
                        writer.writeLDBytes((byte[]) val, index);
                    break;
                case DRDAConstants.DRDA_TYPE_NLOBLOC:
                case DRDAConstants.DRDA_TYPE_NCLOBLOC:
//IC see: https://issues.apache.org/jira/browse/DERBY-3576
                    writer.writeInt(((EngineLOB)val).getLocator());
                    break;
                case DRDAConstants.DRDA_TYPE_NUDT:
                    writer.writeUDT( val, index );
                    break;
                default:
                    if (SanityManager.DEBUG) {
                        trace("ndrdaType is: "+ndrdaType);
                    }
                    writer.writeLDString(val.toString(), index, stmt, isParam);
            }
        }
    }

    /**
     * write nullability if this is a nullable drdatype and FDOCA null
     * value if appropriate
     * @param drdaType      FDOCA type
     * @param valNull       true if this is a null value. False otherwise
     * 
     **/
    private void writeNullability(int drdaType, boolean valNull)
    {
        if(FdocaConstants.isNullable(drdaType))
        {
            if (valNull)
            {
                writer.writeByte(FdocaConstants.NULL_DATA);
            }
            else
            {
                writer.writeByte(FdocaConstants.INDICATOR_NULLABLE);
            }
        }
        
    }

    /**
     * Convert a {@code java.sql.Date} to a string with the format expected
     * by the client.
     * See org.apache.derby.client.am.DateTime#dateBytesToDate
     *
     * @param date the date to format
     * @return a string on the format YYYY-MM-DD representing the date
     */
    private String formatDate(java.sql.Date date) {
        Calendar cal = getGMTCalendar();
        cal.clear();
        cal.setTime(date);

        char[] buf = "YYYY-MM-DD".toCharArray();
        padInt(buf, 0, 4, cal.get(Calendar.YEAR));
        padInt(buf, 5, 2, cal.get(Calendar.MONTH) + 1);
        padInt(buf, 8, 2, cal.get(Calendar.DAY_OF_MONTH));

        return new String(buf);
    }

    /**
     * Convert a {@code java.sql.Time} to a string with the format expected
     * by the client.
     * See org.apache.derby.client.am.DateTime#timeBytesToTime
     *
     * @param time the time to format
     * @return a string on the format HH:MM:SS representing the time
     */
    private String formatTime(Time time) {
        Calendar cal = getGMTCalendar();
        cal.clear();
        cal.setTime(time);

        char[] buf = "HH:MM:SS".toCharArray();
        padInt(buf, 0, 2, cal.get(Calendar.HOUR_OF_DAY));
        padInt(buf, 3, 2, cal.get(Calendar.MINUTE));
        padInt(buf, 6, 2, cal.get(Calendar.SECOND));

        return new String(buf);
    }

    /**
     * Convert a {@code java.sql.Timestamp} to a string with the format
     * expected by the client.
     * See org.apache.derby.client.am.DateTime#timestampBytesToTimestamp
     *
     * @param ts the timestamp to format
     * @return a string on the format YYYY-MM-DD-HH.MM.SS.ffffff[fff]
     */
    private String formatTimestamp(Timestamp ts) {
        Calendar cal = getGMTCalendar();
        cal.clear();
        cal.setTime(ts);

        char[] buf = new char[appRequester.getTimestampLength()];
        padInt(buf, 0, 4, cal.get(Calendar.YEAR));
        buf[4] = '-';
        padInt(buf, 5, 2, cal.get(Calendar.MONTH) + 1);
        buf[7] = '-';
        padInt(buf, 8, 2, cal.get(Calendar.DAY_OF_MONTH));
        buf[10] = '-';
        padInt(buf, 11, 2, cal.get(Calendar.HOUR_OF_DAY));
        buf[13] = '.';
        padInt(buf, 14, 2, cal.get(Calendar.MINUTE));
        buf[16] = '.';
        padInt(buf, 17, 2, cal.get(Calendar.SECOND));
        buf[19] = '.';

        int nanos = ts.getNanos();
        if (appRequester.supportsTimestampNanoseconds()) {
            padInt(buf, 20, 9, nanos);
        } else {
            padInt(buf, 20, 6, nanos / 1000);
        }

        return new String(buf);
    }

    /**
     * Insert an integer into a char array and pad it with leading zeros if
     * its string representation is shorter than {@code length} characters.
     *
     * @param buf the char array
     * @param offset where in the array to start inserting the value
     * @param length the desired length of the inserted string
     * @param value the integer value to insert
     */
    private void padInt(char[] buf, int offset, int length, int value) {
        final int radix = 10;
        for (int i = offset + length - 1; i >= offset; i--) {
            buf[i] = Character.forDigit(value % radix, radix);
            value /= radix;
        }
    }

    /**
     * Methods to keep track of required codepoints
     */
    /**
     * Copy a list of required code points to template for checking
     *
     * @param req list of required codepoints
     */
    private void copyToRequired(int [] req)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        currentRequiredLength = req.length;
        if (currentRequiredLength > required.length) {
            required = new int[currentRequiredLength];
        }
        System.arraycopy(req, 0, required, 0, req.length);
    }
    /**
     * Remove codepoint from required list
     *
     * @param codePoint - code point to be removed
     */
    private void removeFromRequired(int codePoint)
    {
        for (int i = 0; i < currentRequiredLength; i++) {
            if (required[i] == codePoint) {
                required[i] = 0;
            }
        }
    }
    /**
     * Check whether we have seen all the required code points
     *
     * @param codePoint code point for which list of code points is required
     */
    private void checkRequired(int codePoint) throws DRDAProtocolException
    {
        int firstMissing = 0;
        for (int i = 0; i < currentRequiredLength; i++)
        {
            if (required[i] != 0)
            {
                firstMissing = required[i];
                break;
            }
        }
        if (firstMissing != 0) {
            missingCodePoint(firstMissing);
        }
    }
    /**
     * Error routines
     */
    /**
     * Seen too many of this code point
     *
     * @param codePoint  code point which has been duplicated
     *
     * @exception DRDAProtocolException
     */
    private void tooMany(int codePoint) throws DRDAProtocolException
    {
        throwSyntaxrm(CodePoint.SYNERRCD_TOO_MANY, codePoint);
    }
    /**
     * Object too big
     *
     * @param codePoint  code point with too big object
     * @exception DRDAProtocolException
     */
    private void tooBig(int codePoint) throws DRDAProtocolException
    {
        throwSyntaxrm(CodePoint.SYNERRCD_TOO_BIG, codePoint);
    }
     
    
    /**
     * Invalid non-derby client tried to connect.
     * thrown a required Value not found error and log a message to derby.log
     * 
     * @param prdid product id that does not match DNC 
     * @throws DRDAProtocolException
     */
    private void invalidClient(String prdid) throws DRDAProtocolException {
//IC see: https://issues.apache.org/jira/browse/DERBY-5565
        Monitor.logMessage(new Date()
                + " : "
                + server.localizeMessage("DRDA_InvalidClient.S",
                        new String[] { prdid }));
        requiredValueNotFound(CodePoint.PRDID);

    }
    
    /*** Required value not found.
     * 
     * @param codePoint code point with invalid value
     * 
     */
    private void requiredValueNotFound(int codePoint) throws DRDAProtocolException {
        throwSyntaxrm(CodePoint.SYNERRCD_REQ_VAL_NOT_FOUND, codePoint);
    }
    
    
    /**
     * Object length not allowed
     *
     * @param codePoint  code point with bad object length
     * @exception DRDAProtocolException
     */
    private void badObjectLength(int codePoint) throws DRDAProtocolException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        throwSyntaxrm(CodePoint.SYNERRCD_OBJ_LEN_NOT_ALLOWED, codePoint);
    }
    /**
     * RDB not found
     *
     * @param rdbnam  name of database
     * @exception DRDAProtocolException
     */
    private void rdbNotFound(String rdbnam) throws DRDAProtocolException
    {
        Object[] oa = {rdbnam};
        throw new
            DRDAProtocolException(DRDAProtocolException.DRDA_Proto_RDBNFNRM,
                                  this,0,
                                  DRDAProtocolException.NO_ASSOC_ERRCD, oa);
    }
    /**
     * Invalid value for this code point
     *
     * @param codePoint  code point value
     * @exception DRDAProtocolException
     */
    private void invalidValue(int codePoint) throws DRDAProtocolException
    {
        throwSyntaxrm(CodePoint.SYNERRCD_REQ_VAL_NOT_FOUND, codePoint);
    }
    /**
     * Invalid codepoint for this command
     *
     * @param codePoint code point value
     *
     * @exception DRDAProtocolException
     */
    protected void invalidCodePoint(int codePoint) throws DRDAProtocolException
    {
        throwSyntaxrm(CodePoint.SYNERRCD_INVALID_CP_FOR_CMD, codePoint);
    }
    /**
     * Don't support this code point
     *
     * @param codePoint  code point value
     * @exception DRDAProtocolException
     */
    protected void codePointNotSupported(int codePoint) throws DRDAProtocolException
    {
        throw new
            DRDAProtocolException(DRDAProtocolException.DRDA_Proto_CMDNSPRM,
                                  this,codePoint,
                                  DRDAProtocolException.NO_ASSOC_ERRCD);
    }
    /**
     * Don't support this value
     *
     * @param codePoint  code point value
     * @exception DRDAProtocolException
     */
    private void valueNotSupported(int codePoint) throws DRDAProtocolException
    {
        throw new
            DRDAProtocolException(DRDAProtocolException.DRDA_Proto_VALNSPRM,
                                  this,codePoint,
                                  DRDAProtocolException.NO_ASSOC_ERRCD);
    }
    /**
     * Verify that the code point is the required code point
     *
     * @param codePoint code point we have
     * @param reqCodePoint code point required at this time
     *
     * @exception DRDAProtocolException
     */
    private void verifyRequiredObject(int codePoint, int reqCodePoint)
        throws DRDAProtocolException
    {
        if (codePoint != reqCodePoint )
        {
            throwSyntaxrm(CodePoint.SYNERRCD_REQ_OBJ_NOT_FOUND,codePoint);
        }
    }
    /**
     * Verify that the code point is in the right order
     *
     * @param codePoint code point we have
     * @param reqCodePoint code point required at this time
     *
     * @exception DRDAProtocolException
     */
    private void verifyInOrderACCSEC_SECCHK(int codePoint, int reqCodePoint)
        throws DRDAProtocolException
    {
        if (codePoint != reqCodePoint )
        {
            throw
                new DRDAProtocolException(DRDAProtocolException.DRDA_Proto_PRCCNVRM,
                                          this, codePoint,
                                          CodePoint.PRCCNVCD_ACCSEC_SECCHK_WRONG_STATE);
        }
    }

    /**
     * Database name given under code point doesn't match previous database names
     *
     * @param codePoint codepoint where the mismatch occurred
     *
     * @exception DRDAProtocolException
     */
    private void rdbnamMismatch(int codePoint)
        throws DRDAProtocolException
    {
        throw new DRDAProtocolException(DRDAProtocolException.DRDA_Proto_PRCCNVRM,
                                          this, codePoint,
                                          CodePoint.PRCCNVCD_RDBNAM_MISMATCH);
    }
    /**
     * Close the current session
     */
    private void closeSession()
    {
        if (session == null) {
            return;
        }

        /* DERBY-2220: Rollback the current XA transaction if it is
           still associated with the connection. */
        if (xaProto != null) {
            xaProto.rollbackCurrentTransaction();
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        server.removeFromSessionTable(session.connNum);
        try {
            session.close();
        } catch (SQLException se)
        {
            // If something went wrong closing down the session.
            // Print an error to the console and close this 
            //thread. (6013)
            sendUnexpectedException(se);
            close();
        }
        finally {
            session = null;
            database = null;
            appRequester=null;
            sockis = null;
            sockos=null;
            databaseAccessException=null;
        }
    }

    /**
     * Handle Exceptions - write error protocol if appropriate and close session
     *  or thread as appropriate
     */
    private void handleException(Exception e)
    {
        try {
            if (e instanceof DRDAProtocolException) {
                // protocol error - write error message
                sendProtocolException((DRDAProtocolException) e);
            } else {
                // something unexpected happened
                sendUnexpectedException(e);
                server.consoleExceptionPrintTrace(e);
            }
        } finally {
            // always close the session and stop the thread after handling
            // these exceptions
            closeSession();
            close();
        }
    }
    
    /**
     * Notice the client about a protocol error.
     *
     * @param de <code>DRDAProtocolException</code> to be sent
     */
    private void sendProtocolException(DRDAProtocolException de) {
        String dbname = getDbName();
        try {
            println2Log(dbname, session.drdaID, de.getMessage());
            server.consoleExceptionPrintTrace(de);
            reader.clearBuffer();
            de.write(writer);
            finalizeChain();
        } catch (DRDAProtocolException ioe) {
            // There may be an IO exception in the write.
            println2Log(dbname, session.drdaID, de.getMessage());
            server.consoleExceptionPrintTrace(ioe);
        }
    }

    /**
     * Send unpexpected error to the client
     * @param e Exception to be sent
     */
    private void sendUnexpectedException(Exception e)
    {

        DRDAProtocolException unExpDe;
        try {
            String dbname = getDbName();
            println2Log(dbname,session.drdaID, e.getMessage());
            server.consoleExceptionPrintTrace(e);
            unExpDe = DRDAProtocolException.newAgentError(this,
                                                          CodePoint.SVRCOD_PRMDMG,  
                                                          dbname, e.getMessage());
        
            reader.clearBuffer();
            unExpDe.write(writer);
            finalizeChain();
        }
        catch (DRDAProtocolException nde) 
        {
            // we can't tell the client, but we tried.
        }
        
    }


    /**
     * Test if DRDA connection thread is closed
     *
     * @return true if close; false otherwise
     */
    private boolean closed()
    {
        return close;
    }
    /**
     * Get whether connections are logged
     *
     * @return true if connections are being logged; false otherwise
     */
    private boolean getLogConnections()
    {
        return logConnections;
    }
    /**
     * Get time slice value for length of time to work on a session
     *
     * @return time slice
     */
    private long getTimeSlice()
    {
        return timeSlice;
    }
    /**
     * Send string to console
     *
     * @param value - value to print on console
     */
    protected  void trace(String value)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6206
        if (SanityManager.DEBUG && server.debugOutput() == true) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3706
            server.consoleMessage(value, true);
        }
    }


    /**
     * Sends a trace string to the console when reading an EXTDTA value (if
     * tracing is enabled).
     *
     * @param drdaType the DRDA type of the EXTDTA value
     * @param index the one-based parameter index
     * @param stream the stream being read
     * @param streamLOB whether or not the value is being streamed as the last
     *      parameter value in the DRDA protocol flow
     * @param encoding the encoding of the data, if any
     */
    private void traceEXTDTARead(int drdaType, int index,
//IC see: https://issues.apache.org/jira/browse/DERBY-2017
                                 EXTDTAReaderInputStream stream,
                                 boolean streamLOB, String encoding) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6206
        if (SanityManager.DEBUG && server.debugOutput() == true) {
            StringBuilder sb = new StringBuilder("Reading/setting EXTDTA: ");
            // Data: t<type>/i<ob_index>/<streamLOB>/<encoding>/
            //       <statusByteExpected>/b<byteLength>
            sb.append("t").append(drdaType).append("/i").append(index).
                    append("/").append(streamLOB).
                    append("/").append(encoding).append("/").
                    append(stream.readStatusByte). append("/b");
            if (stream == null) {
                sb.append("NULL");
            } else if (stream.isLayerBStream()) {
                sb.append("UNKNOWN_LENGTH");
            } else {
                sb.append(
                        ((StandardEXTDTAReaderInputStream)stream).getLength());
            }
            trace(sb.toString());
        }
    }

    /***
     * Show runtime memory
     *
     ***/
    public static void showmem() {
        Runtime rt = Runtime.getRuntime();
        Date d = new Date();
//IC see: https://issues.apache.org/jira/browse/DERBY-5896
        rt.gc();
        System.out.println("total memory: "
                           + rt.totalMemory()
                           + " free: "
                           + rt.freeMemory()
                           + " " + d.toString());

    }

    /**
     * convert byte array to a Hex string
     * 
     * @param buf buffer to  convert
     * @return hex string representation of byte array
     */
    private String convertToHexString(byte [] buf)
    {
        return "0x" + StringUtil.toHexString(buf, 0, buf.length);
    }
    /**
     * check that the given typdefnam is acceptable
     * 
     * @param typdefnam 
     *
     * @exception DRDAProtocolException
     */
    private void checkValidTypDefNam(String typdefnam)
        throws DRDAProtocolException
    {
        if (!(typdefnam.equals("QTDSQL370")
                || typdefnam.equals("QTDSQL400")
                || typdefnam.equals("QTDSQLX86")
                || typdefnam.equals("QTDSQLASC")
                || typdefnam.equals("QTDSQLVAX")
                || typdefnam.equals("QTDSQLJVM"))) {
            invalidValue(CodePoint.TYPDEFNAM);
        }
    }
    /**
     * Check that the length is equal to the required length for this codepoint
     *
     * @param codepoint codepoint we are checking
     * @param reqlen    required length
     * 
     * @exception DRDAProtocolException
     */
    private void checkLength(int codepoint, int reqlen)
        throws DRDAProtocolException
    {
        long len = reader.getDdmLength();
        if (len < reqlen) {
            badObjectLength(codepoint);
        } else if (len > reqlen) {
            tooBig(codepoint);
        }
    }
    /**
     * Read and check a boolean value
     * 
     * @param codepoint codePoint to be used in error reporting
     * @return true or false depending on boolean value read
     *
     * @exception DRDAProtocolException
     */
    private boolean readBoolean(int codepoint) throws DRDAProtocolException
    {
        checkLength(codepoint, 1);
        byte val = reader.readByte();
        if (val == CodePoint.TRUE) {
            return true;
        } else if (val == CodePoint.FALSE) {
            return false;
        } else {
            invalidValue(codepoint);
            return false;   // to shut the compiler up
        }
    }
    /**
     * Create a new database and intialize the 
     * DRDAConnThread database.
     * 
     * @param dbname database name to initialize. If 
     * dbnam is non null, add database to the current session
     *
     */
    private void initializeDatabase(String dbname)
    {
        Database db;
        if (appRequester.isXARequester())
        {
            db = new XADatabase(dbname);
        }
        else
        {
            db = new Database(dbname);
        }
        if (dbname != null) {
            session.addDatabase(db);
            session.database = db;
        }
        database = db;
    }
    /**
     * Set the current database
     * 
     * @param codePoint     codepoint we are processing
     *
     * @exception DRDAProtocolException
     */
    private void setDatabase(int codePoint) throws DRDAProtocolException
    {
        String dbname = parseRDBNAM();
        // using same database so we are done
        if (database != null && database.getDatabaseName().equals(dbname)) {
            return;
        }
        Database d = session.getDatabase(dbname);
        if (d == null) {
            rdbnamMismatch(codePoint);
        } else {
            database = d;
        }
        session.database = d;
    }
    /**
     * Write ENDUOWRM
     * Instance Variables
     *  SVCOD - severity code - WARNING - required
     *  UOWDSP - Unit of Work Disposition - required
     *  RDBNAM - Relational Database name - optional
     *  SRVDGN - Server Diagnostics information - optional
     *
     * @param opType - operation type 1 - commit, 2 -rollback
     */
    private void writeENDUOWRM(int opType)
    {
        writer.createDssReply();
        writer.startDdm(CodePoint.ENDUOWRM);
        writer.writeScalar2Bytes(CodePoint.SVRCOD, CodePoint.SVRCOD_WARNING);
        writer.writeScalar1Byte(CodePoint.UOWDSP, opType);
        writer.endDdmAndDss();
    }

  void writeEXTDTA (DRDAStatement stmt) throws SQLException, DRDAProtocolException
  {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
      ArrayList<Object> extdtaValues = stmt.getExtDtaObjects();
    // build the EXTDTA data, if necessary
    if (extdtaValues == null) {
        return;
    }
    boolean chainFlag, chainedWithSameCorrelator;
    boolean writeNullByte = false;

    for (int i = 0; i < extdtaValues.size(); i++) {
        // is this the last EXTDTA to be built?
        if (i != extdtaValues.size() - 1) { // no
            chainFlag = true;
            chainedWithSameCorrelator = true;
        }
        else { // yes
            chainFlag = false; //last blob DSS stream itself is NOT chained with the NEXT DSS
            chainedWithSameCorrelator = false;
        }

        
        if ((sqlamLevel >= MGRLVL_7) && stmt.isExtDtaValueNullable(i)) {
            writeNullByte = true;
        }
        
        Object o  = extdtaValues.get(i);
        if (o instanceof EXTDTAInputStream) {
            EXTDTAInputStream stream = (EXTDTAInputStream) o;
                        
//IC see: https://issues.apache.org/jira/browse/DERBY-326
            try{
//IC see: https://issues.apache.org/jira/browse/DERBY-2941
                        stream.initInputStream();
            writer.writeScalarStream (chainedWithSameCorrelator,
                                      CodePoint.EXTDTA,
                                      stream,
                                      writeNullByte);
            
            }finally{
                // close the stream when done
                closeStream(stream);
        }
            
        }
    }
    // reset extdtaValues after sending
    stmt.clearExtDtaObjects();

  }


    /**
     * Check SQLWarning and write SQLCARD as needed.
     * 
     * @param conn      connection to check
     * @param stmt      statement to check
     * @param rs        result set to check
     * @param updateCount   update count to include in SQLCARD
     * @param alwaysSend    whether always send SQLCARD regardless of
     *                      the existance of warnings
     * @param sendWarn  whether to send any warnings or not.
     *
     * @exception DRDAProtocolException
     */
    private void checkWarning(Connection conn, Statement stmt, ResultSet rs,
//IC see: https://issues.apache.org/jira/browse/DERBY-6000
                          long updateCount, boolean alwaysSend, boolean sendWarn)
        throws DRDAProtocolException, SQLException
    {
        // instead of writing a chain of sql warning, we send the first one, this is
        // jcc/db2 limitation, see beetle 4629
        SQLWarning reportWarning = null;
        try
        {
            if (stmt != null)
            {
                SQLWarning warning = stmt.getWarnings();
                if (warning != null)
                {
                    stmt.clearWarnings();
                    reportWarning = warning;
                }
            }
            if (rs != null)
            {
                SQLWarning warning = rs.getWarnings();
                if (warning != null)
                {
                    rs.clearWarnings();
                    if (reportWarning == null) {
                        reportWarning = warning;
                    }
                }
            }
            if (conn != null)
            {
                SQLWarning warning = conn.getWarnings();
                if (warning != null)
                {
                    conn.clearWarnings();
                    if (reportWarning == null) {
                        reportWarning = warning;
                    }
                }
            }
            
        }
        catch (SQLException se)
        {
            if (SanityManager.DEBUG) {
                trace("got SQLException while trying to get warnings.");
            }
        }


        if ((alwaysSend || reportWarning != null) && sendWarn) {
            writeSQLCARDs(reportWarning, updateCount);
        }
    }

    boolean hasSession() {
//IC see: https://issues.apache.org/jira/browse/DERBY-3435
        return session != null;
    }
    
    long getBytesRead() {
        return reader.totalByteCount;
    }
    
    long getBytesWritten() {
        return writer.totalByteCount;
    }

    protected String buildRuntimeInfo(String indent, LocalizedResource localLangUtil )
    {
        // DERBY-6714: session can be null if the session gets closed just
        // as we try to read its runtime info.
        Session s = session;
        if (s == null) {
            return "";
        } else {
            return s.buildRuntimeInfo("", localLangUtil) + "\n";
        }
    }


    /**
     * Finalize the current DSS chain and send it if
     * needed.
     */
    private void finalizeChain() throws DRDAProtocolException {
        writer.finalizeChain(reader.getCurrChainState(), getOutputStream());
    }

    /**
     *  Validate SECMEC_USRSSBPWD (Strong Password Substitute) can be used as
     *  DRDA security mechanism.
     *
     *  Here we check that the target server can support SECMEC_USRSSBPWD
     *  security mechanism based on the environment, application
     *  requester's identity (PRDID) and connection URL.
     *
     *  IMPORTANT NOTE:
     *  --------------
     *  SECMEC_USRSSBPWD is ONLY supported by the target server if:
     *      - current authentication provider is Derby BUILTIN or
     *        NONE. (database / system level) (Phase I)
     *      - database-level password must have been encrypted with the
     *        SHA-1 based authentication scheme
     *      - Application requester is 'DNC' (Derby Network Client)
     *        (Phase I)
     *
     *  @return security check code - 0 if everything O.K.
     */
    private int validateSecMecUSRSSBPWD() throws  DRDAProtocolException
    {
        AuthenticationService authenticationService = null;
        org.apache.derby.iapi.db.Database databaseObj = null;
        String srvrlslv = appRequester.srvrlslv;

        // Check if application requester is the Derby Network Client (DNC)
        //
        // We use a trick here - as the product ID is not yet available
        // since ACCRDB message is only coming later, we check the server
        // release level field sent as part of the initial EXCSAT message;
        // indeed, the product ID (PRDID) is prefixed to in the field.
        // Derby always sets it as part of the EXCSAT message so if it is
        // not available, we stop here and inform the requester that
        // SECMEC_USRSSBPWD cannot be supported for this connection.
        if ((srvrlslv == null) || (srvrlslv.length() == 0) ||
            (srvrlslv.length() < CodePoint.PRDID_MAX) ||
            (srvrlslv.indexOf(DRDAConstants.DERBY_DRDA_CLIENT_ID)
                    == -1)) {
            return CodePoint.SECCHKCD_NOTSUPPORTED; // Not Supported
        }


        // Client product version is extracted from the srvrlslv field.
        // srvrlslv has the format <PRDID>/<ALTERNATE VERSION FORMAT>
        // typically, a known Derby client has a four part version number
        // with a pattern such as DNC10020/10.2.0.3 alpha. If the alternate
        // version format is not specified, clientProductVersion_ will just
        // be set to the srvrlslvl. Final fallback will be the product id.
        //
        // SECMEC_USRSSBPWD is only supported by the Derby engine and network
        // server code starting at version major '10' and minor '02'. Hence,
        // as this is the same for the derby client driver, we need to ensure
        // our DNC client is at version and release level of 10.2 at least.
        // We set the client version in the application requester and check
        // if it is at the level we require at a minimum.
        appRequester.setClientVersion(
                srvrlslv.substring(0, (int) CodePoint.PRDID_MAX));

        if (appRequester.supportsSecMecUSRSSBPWD() == false) {
            return CodePoint.SECCHKCD_NOTSUPPORTED; // Not Supported
        }

        String dbName = database.getShortDbName();
        // Check if the database is available (booted)
        // 
        // First we need to have the database name available and it should
        // have been set as part of the ACCSEC request (in the case of a Derby
        // 'DNC' client)
        if ((dbName == null) || (dbName.length() == 0))
        {
            // No database specified in the connection URL attributes
            //
            // In this case, we get the authentication service handle from the
            // local driver, as the requester may simply be trying to shutdown
            // the engine.
            authenticationService = ((InternalDriver)
              NetworkServerControlImpl.getDriver()).getAuthenticationService();
        }
        else
        {
            // We get the authentication service from the database as this
            // last one might have specified its own auth provider (at the
            // database level).
            // 
            // if monitor is never setup by any ModuleControl, getMonitor
            // returns null and no Derby database has been booted. 
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
            if (getMonitor() != null) {
                databaseObj = (org.apache.derby.iapi.db.Database)
                    findService(Property.DATABASE_MODULE, dbName);
            }

            if (databaseObj == null)
            {
                // If database is not found, try connecting to it. 
                database.makeDummyConnection();

                // now try to find it again
                databaseObj = (org.apache.derby.iapi.db.Database)
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
                    findService(Property.DATABASE_MODULE, dbName);
            }

            // If database still could not be found, it means the database
            // does not exist - we just return security mechanism not
            // supported down below as we could not verify we can handle
            // it.
//IC see: https://issues.apache.org/jira/browse/DERBY-3184
            try {
                if (databaseObj != null) {
                    authenticationService =
                        databaseObj.getAuthenticationService();
                }
            } catch (StandardException se) {
                println2Log(null, session.drdaID, se.getMessage());
                // Local security service non-retryable error.
                return CodePoint.SECCHKCD_0A;
            }

        }

        // Now we check if the authentication provider is NONE or BUILTIN
        if (authenticationService != null)
        {
            String authClassName = authenticationService.getClass().getName();
                
            if (!authClassName.equals(AUTHENTICATION_PROVIDER_BUILTIN_CLASS) &&
                !authClassName.equals(AUTHENTICATION_PROVIDER_NONE_CLASS)) {
                return CodePoint.SECCHKCD_NOTSUPPORTED; // Not Supported
            }
        }

        // SECMEC_USRSSBPWD target initialization
        try {
//IC see: https://issues.apache.org/jira/browse/DERBY-5055
            myTargetSeed = DecryptionManager.generateSeed();
            database.secTokenOut = myTargetSeed;
        } catch (SQLException se) {
            println2Log(null, session.drdaID, se.getMessage());
            // Local security service non-retryable error.
           return CodePoint.SECCHKCD_0A;
        }
                                
        return 0; // SECMEC_USRSSBPWD is supported
    }

    /**
     * Close a stream.
     *
     * @param stream the stream to close (possibly {@code null})
     * @throws SQLException wrapped around an {@code IOException} if closing
     * the stream failed
     */
    private static void closeStream(InputStream stream) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-5396
        try {
            if (stream != null) {
                stream.close();
            }
        } catch (IOException e) {
            throw Util.javaException(e);
        }
    }
    
    
    private static InputStream
        convertAsByteArrayInputStream( EXTDTAReaderInputStream stream )
        throws IOException {

        // Suppress the exception that may be thrown when reading the status
        // byte here, we want the embedded statement to fail while executing.
        stream.setSuppressException(true);
//IC see: https://issues.apache.org/jira/browse/DERBY-2017

        final int byteArrayLength = 
            stream instanceof StandardEXTDTAReaderInputStream ?
            (int) ( ( StandardEXTDTAReaderInputStream ) stream ).getLength() : 
            1 + stream.available(); // +1 to avoid infinite loop

        // TODO: We will run into OOMEs for large values here.
        //       Could avoid this by saving value temporarily to disk, for
        //       instance by using the existing LOB code.
        PublicBufferOutputStream pbos = 
            new PublicBufferOutputStream( byteArrayLength );

        byte[] buffer = new byte[Math.min(byteArrayLength, 32*1024)];
        
        int c;
        
        while( ( c = stream.read( buffer,
                                  0,
                                  buffer.length ) ) > -1 ) {
            pbos.write( buffer, 0, c );
        }

        // Check if the client driver encountered any errors when reading the
        // source on the client side.
//IC see: https://issues.apache.org/jira/browse/DERBY-2017
        if (stream.isStatusSet() &&
                stream.getStatus() != DRDAConstants.STREAM_OK) {
            // Create a stream that will just fail when accessed.
            return new FailingEXTDTAInputStream(stream.getStatus());
        } else {
            return new ByteArrayInputStream( pbos.getBuffer(),
                                             0,
                                             pbos.getCount() );
        }

    }
    
    
    private static class PublicBufferOutputStream extends ByteArrayOutputStream{
        
        PublicBufferOutputStream(int size){
            super(size);
        }
        
        public byte[] getBuffer(){
            return buf;
        }
        
        public int getCount(){
            return count;
        }
        
    }
    
    /**
     * Sets the specified character EXTDTA parameter of the embedded statement.
     *
     * @param stmt the DRDA statement to use
     * @param i the one-based index of the parameter
     * @param extdtaStream the EXTDTA stream to read data from
     * @param streamLOB whether or not the stream content is streamed as the
     *      last value in the DRDA protocol flow
     * @param encoding the encoding of the EXTDTA stream
     * @throws IOException if reading from the stream fails
     * @throws SQLException if setting the stream fails
     */
    private static void setAsCharacterStream(
//IC see: https://issues.apache.org/jira/browse/DERBY-2017
                                         DRDAStatement stmt,
                                         int i,
                                         EXTDTAReaderInputStream extdtaStream,
                                         boolean streamLOB,
                                         String encoding)
           throws IOException, SQLException {
        PreparedStatement ps = stmt.getPreparedStatement();
        
        // DERBY-3085. Save the stream so it can be drained later
        // if not  used.
        if (streamLOB) {
            stmt.setStreamedParameter(extdtaStream);
        }
        
        final InputStream is = 
            streamLOB ?
            (InputStream) extdtaStream :
            convertAsByteArrayInputStream( extdtaStream );
        
        final InputStreamReader streamReader = 
            new InputStreamReader( is,
                                   encoding ) ;
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1984
        ps.setCharacterStream(i, streamReader);
    }

    /**
     * Sets the specified binary EXTDTA parameter of the embedded statement.
     *
     * @param stmt the DRDA statement to use
     * @param index the one-based index of the parameter
     * @param stream the EXTDTA stream to read data from
     * @param streamLOB whether or not the stream content is streamed as the
     *      last value in the DRDA protocol flow
     * @throws IOException if reading from the stream fails
     * @throws SQLException  if setting the stream fails
     */
    private static void setAsBinaryStream(DRDAStatement stmt,
                                          int index,
                                          EXTDTAReaderInputStream stream,
                                          boolean streamLOB)
            throws IOException, SQLException {
        int type = stmt.getParameterMetaData().getParameterType(index);
        boolean useSetBinaryStream = (type == Types.BLOB);
        PreparedStatement ps = stmt.getPreparedStatement();

        if (streamLOB && useSetBinaryStream) {
            // Save the streamed parameter so we can drain it if it does not
            // get used by embedded when the statement is executed. DERBY-3085
            stmt.setStreamedParameter(stream);
            if (stream == null) {
                ps.setBytes(index, null);
            } else if (!stream.isLayerBStream()) {
                int length = (int)((StandardEXTDTAReaderInputStream)
                                                            stream).getLength();
                ps.setBinaryStream(index, stream, length);

            } else {
//IC see: https://issues.apache.org/jira/browse/DERBY-1984
                ps.setBinaryStream(index, stream);
            }
        } else {
            if (stream == null) {
                ps.setBytes(index, null);
            } else {
                InputStream bais = convertAsByteArrayInputStream(stream);
                ps.setBinaryStream(index, bais, bais.available());
            }
        }
    }
    
    /**
     * Privileged Monitor lookup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ModuleFactory  getMonitor()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
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

    /**
     * Privileged service lookup. Must be private so that user code
     * can't call this entry point.
     */
    private static  Object findService( final String factoryInterface, final String serviceName )
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<Object>()
             {
                 public Object run()
                 {
                     return Monitor.findService( factoryInterface, serviceName );
                 }
             }
             );
    }
    
}
