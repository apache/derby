/*
   Derby - Class org.apache.derby.impl.drda.DRDAConnThread

   Copyright 2001, 2004 The Apache Software Foundation or its licensors, as applicable.

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

/**
 * This class translates DRDA protocol from an application requester to JDBC
 * for Cloudscape and then translates the results from Cloudscape to DRDA
 * for return to the application requester.
 * @author ge, marsden, peachey
 */
package org.apache.derby.impl.drda;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.*;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.DataTruncation;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.sql.Blob;
import java.sql.Clob;

import java.util.Properties;
import java.util.Enumeration;
import java.util.Vector;
import java.util.ArrayList;
import java.util.Date;
import java.math.BigDecimal;

import org.apache.derby.iapi.tools.i18n.LocalizedResource;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.iapi.reference.DB2Limit;
import org.apache.derby.iapi.error.ExceptionSeverity;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.impl.jdbc.EmbedSQLException;
import org.apache.derby.impl.jdbc.EmbedSQLWarning;
import org.apache.derby.impl.jdbc.EmbedStatement;
import org.apache.derby.impl.jdbc.EmbedPreparedStatement;
import org.apache.derby.impl.jdbc.EmbedParameterSetMetaData;
import org.apache.derby.impl.jdbc.EmbedConnection;

import org.apache.derby.iapi.reference.JDBC30Translation;

import org.apache.derby.iapi.services.info.JVMInfo;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.services.monitor.Monitor;

public class DRDAConnThread extends Thread {

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


	protected CcsidManager ccsidManager = new EbcdicCcsidManager();
	private int correlationID;
	private InputStream sockis;
	private OutputStream sockos;
	private DDMReader reader;
	private DDMWriter writer;
	private static int [] ACCRDB_REQUIRED = {CodePoint.RDBACCCL, 
											 CodePoint.CRRTKN,
											 CodePoint.PRDID,
											 CodePoint.TYPDEFNAM,
											 CodePoint.TYPDEFOVR};
	private static int MAX_REQUIRED_LEN = 5;

	private int currentRequiredLength = 0;
	private int [] required = new int[MAX_REQUIRED_LEN];


	private DB2jServerImpl server;			// server who created me
	private Session	session;	// information about the session
	private long timeSlice;				// time slice for this thread
	private Object timeSliceSync = new Object(); // sync object for updating time slice 
	private boolean logConnections;		// log connections to databases

	private boolean	sendWarningsOnCNTQRY = false;	// Send Warnings for SELECT if true
	private Object logConnectionsSync = new Object(); // sync object for log connect
	private boolean close;				// end this thread
	private Object closeSync = new Object();	// sync object for parent to close us down
	private static HeaderPrintWriter logStream;
	private AppRequester appRequester;	// pointer to the application requester
										// for the session being serviced
	private Database database; 	// pointer to the current database
	private int sqlamLevel;		// SQLAM Level - determines protocol

	// manager processing
	private Vector unknownManagers;
	private Vector knownManagers;
	private Vector errorManagers;
	private Vector errorManagersLevel;

	// database accessed failed
	private SQLException databaseAccessException;

	// these fields are needed to feed back to jcc about a statement/procedure's PKGNAMCSN
	private String rdbnam;
	private String rdbcolid;
	private String pkgid;
	private String pkgcnstknStr;
	private int secnumber;

	// this flag is for an execute statement/procedure which actually returns a result set;
	// do not commit the statement, otherwise result set is closed

	// for decryption
	private static DecryptionManager decryptionManager;
	// public key generated by Deffie-Hellman algorithm, to be passed to the encrypter,
	// as well as used to initialize the cipher
	private byte[] myPublicKey;

	// constructor
	/**
	 * Create a new Thread for processing session requests
	 *
	 * @param session Session requesting processing
	 * @param server  Server starting thread
	 * @param timeSlice timeSlice for thread
	 * @param logConnections
	 **/

	public DRDAConnThread(Session session, DB2jServerImpl server, 
						  long timeSlice,
						  boolean logConnections) {
	
   	super();

		// Create a more meaningful name for this thread (but preserve its
		// thread id from the default name).
		DB2jServerImpl.setUniqueThreadName(this, "DRDAConnThread");

		this.session = session;
		this.server = server;
		this.timeSlice = timeSlice;
		this.logConnections = logConnections;
		initialize();
    }

	/**
	 * Main routine for thread, loops until the thread is closed
	 * Gets a session, does work for the session
	 */
    public void run() {
		if (SanityManager.DEBUG)
			trace("Starting new connection thread");

		Session prevSession;
		while(!closed())
		{

			// get a new session
			prevSession = session;
			session = server.getNextSession(session);
			if (session == null)
				close();

			if (closed())
				break;
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
						if (session == null)
							break;
					case Session.ATTEXC:
						long currentTimeSlice;

						do {
							processCommands();
							currentTimeSlice = getTimeSlice();
						} while ((currentTimeSlice == 0)  || 
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
			}
		}
		if (SanityManager.DEBUG)
			trace("Ending connection thread");
		server.getThreadList().removeElement(this);

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
	protected DB2jServerImpl getServer()
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
		if (database != null)
			return database.crrtkn;
		return null;
	}
	/**
	 * Get database name
	 *
	 * @return database name
	 */
	protected String getDbName()
	{
		if (database != null)
			return database.dbName;
		return null;
	}
	/**
	 * Close DRDA  connection thread
	 */
	protected void close()
	{
		synchronized (closeSync)
		{
			close = true;
		}
	}

	/**
	 * Set logging of connections
	 * 
	 * @param value value to set for logging connections
	 */
	protected void setLogConnections(boolean value)
	{
		synchronized(logConnectionsSync) {
			logConnections = value;
		}
	}
	/**
	 * Set time slice value
	 *
	 * @param value new value for time slice
	 */
	protected void setTimeSlice(long value)
	{
		synchronized(timeSliceSync) {
			timeSlice = value;
		}
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
		Object[] oa = {arg1,arg2,arg3,arg4};
		throw DRDAProtocolException.newDisconnectException(this,oa);

	}
	/**
	 * Syntax error
	 *
	 * @param errcd		Error code
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
	 * @param msg	Message describing error
	 *
	 * @exception DRDAProtocolException  newAgentError always thrown
	 */
	protected void agentError(String msg) throws DRDAProtocolException
	{

		String dbname = null;
		if (database != null)
			dbname = database.dbName;
		throw DRDAProtocolException.newAgentError(this, CodePoint.SVRCOD_PRMDMG, 
			dbname, msg);
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
	 * @param drdaID	DRDA identifier
	 * @param msg	message
	 */
	protected static void println2Log(String dbname, String drdaID, String msg)
	{
		if (logStream == null)
			logStream = Monitor.getStream();

		if (dbname != null)
		{
			int endOfName = dbname.indexOf(';');
			if (endOfName != -1)
				dbname = dbname.substring(0, endOfName);
		}
		logStream.printlnWithHeader("(DATABASE = " + dbname + "), (DRDAID = " + drdaID + "), " + msg);
	}
	/**
	 * Write RDBNAM
	 *
	 * @param rdbnam 	database name
	 * @exception DRDAProtocolException
	 */
	protected void writeRDBNAM(String rdbnam)
		throws DRDAProtocolException
	{
		int len = rdbnam.length();
		if (len < CodePoint.RDBNAM_LEN)
			len = CodePoint.RDBNAM_LEN;
		writer.writeScalarHeader(CodePoint.RDBNAM, len);
		try {
			writer.writeScalarPaddedBytes(rdbnam.getBytes(server.DEFAULT_ENCODING),
				len, server.SPACE_CHAR);
		}
		catch (UnsupportedEncodingException e)
		{
			agentError("Unsupported coding exception for server encoding "
				+ server.DEFAULT_ENCODING);
		}
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
		writer = new DDMWriter(ccsidManager, this, session.dssTrace);
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
		if (session.state == Session.ATTEXC)
			sqlamLevel = appRequester.getManagerLevel(CodePoint.SQLAM);

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
			} catch (Throwable t) {
				if (t instanceof InterruptedException)
					throw (InterruptedException)t;
				else
				{
					server.consoleExceptionPrintTrace(t);
				}
			}

		}
		else
		{
			// exchange attributes with application requester
			if (exchangeServerAttributes())
				session.state = Session.ATTEXC;
			else
				closeSession();
		}
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
		int updateCount = 0;
		boolean PRPSQLSTTfailed = false;
		do
		{
			correlationID = reader.readDssHeader();
			int codePoint = reader.readLengthAndCodePoint();
			int writerMark = writer.markDSSClearPoint();
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
						}
					}
					catch(SQLException e)
					{
						if (stmt != null)
 						{
							// if we got a SQLException we need to clean up and
 							// close the statement Beetle 4758
							writer.clearDSSesBackToMark(writerMark);
 							if (! stmt.rsIsClosed())
 							{
 								try {
 									stmt.rsClose();
 								}
 								catch (SQLException ec)
 								{
 									if (SanityManager.DEBUG)
 										trace("Warning: Error closing statement");
								}
								writeABNUOWRM();
								writeSQLCARD(e,CodePoint.SVRCOD_ERROR,0,0);
							}
						}
						else 
						{
							writeSQLCARDs(e, 0);
						}
						errorInChain(e);
					}
					break;
				case CodePoint.EXCSQLIMM:
					try {
						updateCount = parseEXCSQLIMM();
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
					} catch (SQLException e)
					{
						writer.clearDSSesBackToMark(writerMark);
						writeSQLCARDs(e, 0);
						errorInChain(e);
					}
					break;

				case CodePoint.EXCSQLSET:
					try {
						if (parseEXCSQLSET())
						// all went well.
							writeSQLCARDs(null,0);
					}
					catch (SQLWarning w)
					{
						writeSQLCARD(w, CodePoint.SVRCOD_WARNING, 0, 0);
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
						if (sqldaType > 0)		// do write SQLDARD
							writeSQLDARD(database.getCurrentStatement(),
										 (sqldaType ==  CodePoint.TYPSQLDA_LIGHT_OUTPUT),
										 database.getConnection().getWarnings());
						else
							checkWarning(database.getConnection(), null, null, 0, true, true);

					} catch (SQLException e)
					{
						writer.clearDSSesBackToMark(writerMark);
						writeSQLCARDs(e, 0, true);
						PRPSQLSTTfailed = true;
						errorInChain(e);
					}
					break;
				case CodePoint.OPNQRY:
					PreparedStatement ps = null;
					try {
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
						String pkgnamcsn = parseOPNQRY();
						if (pkgnamcsn != null)
						{
							stmt = database.getDRDAStatement(pkgnamcsn);
							ps = stmt.getPreparedStatement();
							ps.clearWarnings();
							stmt.execute();
							writeOPNQRYRM(false, stmt);
							checkWarning(null, ps, null, 0, false, true);

							writeQRYDSC(stmt, false);
							// We could send QRYDTA here if there's no LOB data
							// in the result set, and if we are using LMTBLKPRC, as
							// allowed by drda spec, as an option.

							stmt.rsSuspend();
						}
					}
					catch (SQLException e)
					{
						writer.clearDSSesBackToMark(writerMark);
						try {
							// Try to cleanup if we hit an error.
							if (ps != null)
								ps.close();
							writeOPNQFLRM(e);
						}
						catch (SQLException pse) {}
						errorInChain(e);
					}
					break;
				case CodePoint.RDBCMM:
					try
					{
						if (SanityManager.DEBUG)
							trace("Received commit");
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
						if (SanityManager.DEBUG)
							trace("Received rollback");
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
					parseEXCSAT2();
					writeEXCSATRD();
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
						} catch (SQLException e2) {	// should not get here since doing nothing with ps
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
						if (curStmt != null)
							curStmt.rsSuspend();
					} catch (SQLException e)
					{
						writer.clearDSSesBackToMark(writerMark);
						if (SanityManager.DEBUG) 
						{
							server.consoleExceptionPrint(e);
						}
						writeSQLCARDs(e, 0);
						errorInChain(e);
					}
					break;
				default:
					codePointNotSupported(codePoint);
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
 	 * @param		the SQLException raised
	 * @exception	DRDAProtocolException
	 */
	private void errorInChain(SQLException e) throws DRDAProtocolException
	{
		if (reader.terminateChainOnErr() && (getExceptionSeverity(e) > CodePoint.SVRCOD_ERROR))
		{
			if (SanityManager.DEBUG)  trace("terminating the chain on error...");
			skipRemainder(false);
		}
	}

	/**
	 * Exchange server attributes with application requester
	 *
 	 * @return true if the session was started successfully; false otherwise
	 * @exception DRDAProtocolException, SQLException
	 */
	private boolean exchangeServerAttributes()
		throws  DRDAProtocolException, SQLException
	{
		int codePoint;
		boolean sessionOK = true;
		correlationID = reader.readDssHeader();
		if (SanityManager.DEBUG) {
		  if (correlationID == 0)
		  {
		    SanityManager.THROWASSERT(
					      "Unexpected value for correlationId = " + correlationID);
		  }
		}

		codePoint = reader.readLengthAndCodePoint();

		// The first code point in the exchange of attributes must be EXCSAT
		if (codePoint != CodePoint.EXCSAT)
		{
			//Throw PRCCNVRM 
			throw
			    new DRDAProtocolException(DRDAProtocolException.DRDA_Proto_PRCCNVRM,
										  this, codePoint,
										  CodePoint.PRCCNVCD_EXCSAT_FIRST_AFTER_CONN);
		}

		// set up a new Application Requester to store information about the
		// application requester for this session
		appRequester = new AppRequester();
		parseEXCSAT();
		writeEXCSATRD();
		finalizeChain();

		//we may have to do the access security more than once if we don't
		//provide the requested security mechanism or we run into errors
		//if we don't know the requested security mechanism
		//we'll send our known security mechanisms and the requester will pick
		//if he picks one that requires a security token then another ACCSEC 
		//will flow
		int securityCheckCode = 0;
		boolean notdone = true;
		while (notdone)
		{
			correlationID = reader.readDssHeader();
			codePoint = reader.readLengthAndCodePoint();
			verifyInOrderACCSEC_SECCHK(codePoint,CodePoint.ACCSEC);
			securityCheckCode = parseACCSEC();
			// need security token
			if (securityCheckCode == 0  && 
				database.securityMechanism == CodePoint.SECMEC_EUSRIDPWD &&
				database.publicKeyIn == null)
					securityCheckCode = CodePoint.SECCHKCD_SECTKNMISSING;

			// shouldn't have security token
			if (securityCheckCode == 0 &&
				database.securityMechanism == CodePoint.SECMEC_USRIDPWD &&
				database.publicKeyIn != null)
					securityCheckCode = CodePoint.SECCHKCD_SECTKNMISSING;
			if (SanityManager.DEBUG)
				trace("** ACCSECRD securityCheckCode is: "+securityCheckCode);
			writeACCSECRD(securityCheckCode);
			// everything is O.K., we're done
			if (securityCheckCode == 0) 
			{
				notdone = false;
			}
		}
		correlationID = reader.readDssHeader();
		codePoint = reader.readLengthAndCodePoint();
		verifyInOrderACCSEC_SECCHK(codePoint,CodePoint.SECCHK);
		securityCheckCode = parseSECCHK();
		if (SanityManager.DEBUG)
			trace("*** SECCHKRM securityCheckCode is: "+securityCheckCode);
		writeSECCHKRM(securityCheckCode);
		//at this point if the security check failed, we're done, the session failed
		if (securityCheckCode != 0)
		{
			return false;
		}

		correlationID = reader.readDssHeader();
		codePoint = reader.readLengthAndCodePoint();
		verifyRequiredObject(codePoint,CodePoint.ACCRDB);
		int svrcod = parseACCRDB();

		//if earlier we couldn't access the database
		if (databaseAccessException != null)
		{

			//if the Database was not found we will try DS
			int failureType = getRdbAccessErrorCodePoint();
			if (failureType == CodePoint.RDBNFNRM 
				|| failureType == CodePoint.RDBATHRM)
			{
				writeRDBfailure(failureType);
				writeSQLCARD(databaseAccessException,
					CodePoint.SVRCOD_ERROR,0,0);
			}
			else
			{
				writeRDBfailure(CodePoint.RDBAFLRM);

				// RDBAFLRM requires TYPDEFNAM and TYPDEFOVR
				writer.createDssObject();
				writer.writeScalarString(CodePoint.TYPDEFNAM,
										 CodePoint.TYPDEFNAM_QTDSQLASC);
				writeTYPDEFOVR();
				writer.endDss();

				// Finally, per DDM spec, "an SQLCARD always follows
				// the RDBAFLRM".
				writeSQLCARD(databaseAccessException,
							 CodePoint.SVRCOD_ERROR,0,0);
			}

			// Ignore anything that was chained to the ACCRDB.
			skipRemainder(false);

			// Finalize chain state for whatever we wrote in
			// response to ACCRDB.
			finalizeChain();
			return false;
		}
		else if (database.accessCount > 1 )	// already in conversation with database
		{
			writeRDBfailure(CodePoint.RDBACCRM);

			// Ignore anything that was chained to the ACCRDB.
			skipRemainder(false);

			// Finalize chain state for RDBACCRM
			finalizeChain();
			return false;
		}
		else // everything is fine 
			writeACCRDBRM(svrcod);

		// compare this application requester with previously stored
		// application requesters and if we have already seen this one
		// use stored application requester 
		session.appRequester = server.getAppRequester(appRequester);
		return sessionOK;
	}
	/**
	 * Write RDB Failure
	 *
	 * Instance Variables
	 * 	SVRCOD - Severity Code - required
	 *	RDBNAM - Relational Database name - required
	 *  SRVDGN - Server Diagnostics - optional (not sent for now)
 	 *
	 * @param	codePoint	codepoint of failure
	 */
	private void writeRDBfailure(int codePoint) throws DRDAProtocolException
	{
		writer.createDssReply();
		writer.startDdm(codePoint);
		writer.writeScalar2Bytes(CodePoint.SVRCOD, CodePoint.SVRCOD_ERROR);
		writeRDBNAM(database.dbName);
    	writer.endDdmAndDss();

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
		if (sqlState.regionMatches(0,SQLState.DATABASE_NOT_FOUND,0,5) |
			sqlState.regionMatches(0,SQLState.NO_SUCH_DATABASE,0,5))
			return CodePoint.RDBNFNRM;
		else
			if (sqlState.regionMatches(0,SQLState.LOGIN_FAILED,0,5) ||
				sqlState.regionMatches(0,SQLState.AUTH_INVALID_USER_NAME,0,5))
				return CodePoint.RDBATHRM;
		else
				return CodePoint.RDBAFLRM;
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
		int retSecChkCode = 0;


		String realName = database.dbName; //first strip off properties
		int endOfName = realName.indexOf(';');
		if (endOfName != -1)
			realName = realName.substring(0, endOfName);
		retSecChkCode = getConnFromDatabaseName();

		return retSecChkCode;
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
		p.put(Attribute.USERNAME_ATTR, database.userId);
		p.put(Attribute.PASSWORD_ATTR, database.password);
		//if we haven't got the correlation token yet, use session number for drdaID
		if (session.drdaID == null)
			session.drdaID = leftBrace + session.connNum + rightBrace;
		p.put(Attribute.DRDAID_ATTR, session.drdaID);
	 	try {
			Connection conn =
				server.cloudscapeDriver.connect(Attribute.PROTOCOL  + database.dbName, p);
	  		conn.setAutoCommit(false);
			database.setConnection(conn);
	  	} catch (SQLException se) {
			String sqlState = se.getSQLState();
			// need to set the security check code based on the reason the connection     
			// was denied, Cloudscape doesn't say whether the userid or password caused
			// the problem, so we will just return userid invalid
			databaseAccessException = se;
			for (; se != null; se = se.getNextException())
			{
				if (SanityManager.DEBUG)
					trace(se.getMessage());
	 			println2Log(database.dbName, session.drdaID, se.getMessage());
			}

			if (sqlState.regionMatches(0,SQLState.LOGIN_FAILED,0,5))
				return CodePoint.SECCHKCD_USERIDINVALID;

			return 0;
			
		}
		catch (Exception e)
		{
			// If cloudscape has shut down for some reason,
			// we will send  an agent error and then try to 
			// get the driver loaded again.  We have to get
			// rid of the client first in case they are holding
			// the DriverManager lock.
			println2Log(database.dbName, session.drdaID, 
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
						server.startDB2j();
					} catch (Exception re) {
						println2Log(database.dbName, session.drdaID, "Failed attempt to reload driver " +re.getMessage()  );
					}
					throw dpe;
				}
		}
		
	
		// Everything worked so log connection to the database.
		if (getLogConnections())
	 		println2Log(database.dbName, session.drdaID,
				"Cloudscape Network Server connected to database " +
						database.dbName);
		return 0;
	}


	/**
	 * Parses EXCSAT (Exchange Server Attributes)
	 * Instance variables
	 *	EXTNAM(External Name)	- optional
	 *  MGRLVLLS(Manager Levels) - optional
	 *	SPVNAM(Supervisor Name) - optional
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


		reader.markCollection();

		codePoint = reader.getCodePoint();
		while (codePoint != -1)
		{
			switch (codePoint)
			{
				// optional
				case CodePoint.EXTNAM:
					appRequester.extnam = reader.readString();
					if (SanityManager.DEBUG)
						trace("extName = " + appRequester.extnam);
					if (appRequester.extnam.length() > CodePoint.MAX_NAME)
						tooBig(CodePoint.EXTNAM);
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
					if (appRequester.spvnam != null)
						badObjectLength(CodePoint.SPVNAM);
					break;
				// optional
				case CodePoint.SRVNAM:
					appRequester.srvnam = reader.readString();
					if (SanityManager.DEBUG)
						trace("serverName = " +  appRequester.srvnam);
					if (appRequester.srvnam.length() > CodePoint.MAX_NAME)
						tooBig(CodePoint.SRVNAM);
					break;
				// optional
				case CodePoint.SRVRLSLV:
					appRequester.srvrlslv = reader.readString();
					if (SanityManager.DEBUG)
						trace("serverlslv = " + appRequester.srvrlslv);
					if (appRequester.srvrlslv.length() > CodePoint.MAX_NAME)
						tooBig(CodePoint.SRVRLSLV);
					break;
				// optional
				case CodePoint.SRVCLSNM:
					appRequester.srvclsnm = reader.readString();
					if (SanityManager.DEBUG)
						trace("serverClassName = " + appRequester.srvclsnm);
					if (appRequester.srvclsnm.length() > CodePoint.MAX_NAME)
						tooBig(CodePoint.SRVCLSNM);
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
	 *	EXTNAM(External Name)	- optional
	 *  MGRLVLLS(Manager Levels) - optional
	 *	SPVNAM(Supervisor Name) - optional
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
	 *	Parse manager levels
	 *  Instance variables
	 *		MGRLVL - repeatable, required
	 *		  CODEPOINT
	 *			CCSIDMGR - CCSID Manager
	 *			CMNAPPC - LU 6.2 Conversational Communications Manager 
	 *			CMNSYNCPT - SNA LU 6.2 SyncPoint Conversational Communications Manager
	 *			CMNTCPIP - TCP/IP Communication Manager 
	 *			DICTIONARY - Dictionary
	 *			RDB - Relational Database 
	 *			RSYNCMGR - Resynchronization Manager 
	 *			SECMGR - Security Manager
	 *			SQLAM - SQL Application Manager
	 *			SUPERVISOR - Supervisor
	 *			SYNCPTMGR - Sync Point Manager 
	 *		  VALUE
	 *
	 *	On the second appearance of this codepoint, it can only add managers
	 *
	 * @param time	1 for first time this is seen, 2 for subsequent ones
	 * @exception DRDAProtocolException
	 * 
	 */
	private void parseMGRLVLLS(int time) throws DRDAProtocolException
	{
		int manager, managerLevel;
		int currentLevel;
		// set up vectors to keep track of manager information
		unknownManagers = new Vector();
		knownManagers = new Vector();
		errorManagers = new Vector();
		errorManagersLevel = new Vector();
		if (SanityManager.DEBUG)
			trace("Manager Levels");

		while (reader.moreDdmData())
		{
			manager = reader.readNetworkShort();
			managerLevel = reader.readNetworkShort();
			if (CodePoint.isKnownManager(manager))
			{
				knownManagers.addElement(new Integer(manager));
				//if the manager level hasn't been set, set it
				currentLevel = appRequester.getManagerLevel(manager);
				if (currentLevel == appRequester.MGR_LEVEL_UNKNOWN)
			    	appRequester.setManagerLevel(manager, managerLevel);
				else
				{
					//if the level is still the same we'll ignore it
					if (currentLevel != managerLevel)
					{
						//keep a list of conflicting managers
						errorManagers.addElement(new Integer(manager));
						errorManagersLevel.addElement(new Integer (managerLevel));
					}
				}

			}
			else
				unknownManagers.addElement(new Integer(manager));
			if (SanityManager.DEBUG)
				trace("Manager = " + java.lang.Integer.toHexString(manager) + 
					  " ManagerLevel " + managerLevel);
		}
		sqlamLevel = appRequester.getManagerLevel(CodePoint.SQLAM);
		// did we have any errors
		if (errorManagers.size() > 0)
		{
			Object [] oa = new Object[errorManagers.size()*2];
			int j = 0;
			for (int i = 0; i < errorManagers.size(); i++)
			{
				oa[j++] = errorManagers.elementAt(i);
				oa[j++] = errorManagersLevel.elementAt(i);
			}
			throw new DRDAProtocolException(DRDAProtocolException.DRDA_Proto_MGRLVLRM,
										  this, 0,
										  0, oa);
		}
	}
	/**
	 * Write reply to EXCSAT command
	 * Instance Variables
	 *	EXTNAM - External Name (optional)
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
		writer.writeScalarString(CodePoint.EXTNAM, server.att_extnam);
		//only reply with manager levels if we got sent some
		if (knownManagers != null && knownManagers.size() > 0)
			writeMGRLEVELS();
		writer.writeScalarString(CodePoint.SRVCLSNM, server.att_srvclsnm);
		writer.writeScalarString(CodePoint.SRVNAM, server.ATT_SRVNAM);
		writer.writeScalarString(CodePoint.SRVRLSLV, server.att_srvrlslv);
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
	 *		MGRLVL - repeatable, required
	 *		  CODEPOINT
	 *			CCSIDMGR - CCSID Manager
	 *			CMNAPPC - LU 6.2 Conversational Communications Manager 
	 *			CMNSYNCPT - SNA LU 6.2 SyncPoint Conversational Communications Manager
	 *			CMNTCPIP - TCP/IP Communication Manager   
	 *			DICTIONARY - Dictionary 
	 *			RDB - Relational Database 
	 *			RSYNCMGR - Resynchronization Manager   
	 *			SECMGR - Security Manager
	 *			SQLAM - SQL Application Manager 
	 *			SUPERVISOR - Supervisor 
	 *			SYNCPTMGR - Sync Point Manager 
	 *			XAMGR - XA manager 
	 *		  VALUE
	 */
	private void writeMGRLEVELS() throws DRDAProtocolException
	{
		int manager;
		int appLevel;
		int serverLevel;
		writer.startDdm(CodePoint.MGRLVLLS);
		for (int i = 0; i < knownManagers.size(); i++)
		{
			manager = ((Integer)knownManagers.elementAt(i)).intValue();
			appLevel = appRequester.getManagerLevel(manager);
			serverLevel = server.getManagerLevel(manager);
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
		for (int i = 0; i < unknownManagers.size(); i++)
		{
			manager = ((Integer)unknownManagers.elementAt(i)).intValue();
			writer.writeCodePoint4Bytes(manager, 0);
		}
		writer.endDdm();
	}
	/**
	 *  Parse Access Security
	 *
	 *	If the target server supports the SECMEC requested by the application requester
	 *	then a single value is returned and it is identical to the SECMEC value
	 *	in the ACCSEC command.  If the target server does not support the SECMEC
	 *	requested, then one or more values are returned and the application requester
	 *  must choose one of these values for the security mechanism.
	 *  We currently support
	 *		- user id and password (default for JCC)
	 *		- encrypted user id and password
	 *
     *  Instance variables
	 *    SECMGRNM  - security manager name - optional
	 *	  SECMEC 	- security mechanism - required
	 *	  RDBNAM	- relational database name - optional
	 * 	  SECTKN	- security token - optional, (required if sec mech. needs it)
	 *
	 *  @return security check code - 0 if everything O.K.
	 */
	private int parseACCSEC() throws  DRDAProtocolException
	{
		int securityCheckCode = 0;
		int securityMechanism = 0;
		byte [] publicKeyIn = null;

		reader.markCollection();
		int codePoint = reader.getCodePoint();
		while (codePoint != -1)
		{
			switch(codePoint)
			{
				//optional
				case CodePoint.SECMGRNM:
					// this is defined to be 0 length
					if (reader.getDdmLength() != 0)
						badObjectLength(CodePoint.SECMGRNM);
					break;
				//required
				case CodePoint.SECMEC:
					checkLength(CodePoint.SECMEC, 2);
					securityMechanism = reader.readNetworkShort();
					if (SanityManager.DEBUG)
						trace("Security mechanism = " + securityMechanism);
					if (securityMechanism != server.DEFAULT_SECURITY_MECHANISM)
					{
						//this is the only other one we understand
						if (securityMechanism != CodePoint.SECMEC_EUSRIDPWD)
							securityCheckCode = CodePoint.SECCHKCD_NOTSUPPORTED;
						else
						{
							try {
								if (decryptionManager == null)
									decryptionManager = new DecryptionManager();
								myPublicKey = decryptionManager.obtainPublicKey();
							} catch (SQLException e) {
								println2Log(null, session.drdaID, e.getMessage());
								// Local security service non-retryable error.
								securityCheckCode = CodePoint.SECCHKCD_0A;
							}
						}
					}
					break;
				//optional (currently required for Cloudscape - may need to revisit)
				case CodePoint.RDBNAM:
					String dbname = parseRDBNAM();
					Database d = session.getDatabase(dbname);
					if (d == null)
						addDatabase(dbname);
					else
						database = d;
					break;
				//optional - depending on security Mechanism 
				case CodePoint.SECTKN:
					publicKeyIn = reader.readBytes();
					break;
				default:
					invalidCodePoint(codePoint);
			}
			codePoint = reader.getCodePoint();
		}
		// check for required CodePoint's
		if (securityMechanism == 0)
			missingCodePoint(CodePoint.SECMEC);


		// RESOLVE - when we look further into security we might want to
		// handle this part of the protocol at the session level without
		// requiring a database for when authentication is used but there
		// is no database level security
		if (database == null)
			missingCodePoint(CodePoint.RDBNAM);

		database.securityMechanism = securityMechanism;
		database.publicKeyIn = publicKeyIn;

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
	 * @return package name consistency token
	 * @exception DRDAProtocolException
	 */
	private String parseOPNQRY() throws DRDAProtocolException, SQLException
	{
		String pkgnamcsn = null;
		boolean gotQryblksz = false;
		int blksize = 0;
		int qryblkctl = CodePoint.QRYBLKCTL_DEFAULT;
		int maxblkext = CodePoint.MAXBLKEXT_DEFAULT;
		int qryrowset = CodePoint.QRYROWSET_DEFAULT;
		int qryclsimp = CodePoint.QRYCLSIMP_DEFAULT;
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
					if (qryblkctl != CodePoint.FRCFIXROW)
						invalidCodePoint(qryblkctl);
					if (SanityManager.DEBUG)
						trace("!!qryblkctl = "+Integer.toHexString(qryblkctl));
					gotQryblksz = true;
					break;
				//optional
			  	case CodePoint.MAXBLKEXT:
					maxblkext = reader.readSignedNetworkShort();
					if (SanityManager.DEBUG)
						trace("maxblkext = "+maxblkext);
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
				case CodePoint.QRYOPTVAL:
					// optimize for n rows. Not supported by cloudscape(ignore)
					parseQRYOPTVAL();
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
		if (pkgnamcsn == null)
			missingCodePoint(CodePoint.PKGNAMCSN);
		if (!gotQryblksz)
			missingCodePoint(CodePoint.QRYBLKSZ);

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
		//	writeQRYPOPRM();
		//	pkgnamcsn = null;
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
			if (SanityManager.DEBUG)
				trace("&&&&&& parsing SQLDTA");
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
	 * @exception DRDAProtocolException, SQLException
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
				codePoint = reader.readLengthAndCodePoint();
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
						readAndSetAllExtParams(stmt);
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
		if (SanityManager.DEBUG)
			trace("output override option: "+outovropt);
		if (outovropt != CodePoint.OUTOVRFRS && outovropt != CodePoint.OUTOVRANY)
			invalidValue(CodePoint.OUTOVROPT);
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
		if (SanityManager.DEBUG)
			trace("qryblksz = "+blksize);
		if (blksize < CodePoint.QRYBLKSZ_MIN || blksize > CodePoint.QRYBLKSZ_MAX)
			invalidValue(CodePoint.QRYBLKSZ);
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
		if (SanityManager.DEBUG)
			trace("qryrowset = " + qryrowset);
		if (qryrowset < minVal || qryrowset > CodePoint.QRYROWSET_MAX)
			invalidValue(CodePoint.QRYROWSET);
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
		if (SanityManager.DEBUG)
			trace ("qryclsimp = " + qryclsimp);
		if (qryclsimp != CodePoint.QRYCLSIMP_SERVER_CHOICE &&
			qryclsimp != CodePoint.QRYCLSIMP_YES &&
			qryclsimp != CodePoint.QRYCLSIMP_NO )
			invalidValue(CodePoint.QRYCLSIMP);
		return qryclsimp;
	}


	private int parseQRYCLSRLS() throws DRDAProtocolException
	{
		reader.skipBytes();
		return 0;
	}

	private int parseQRYOPTVAL() throws DRDAProtocolException
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
		writeRDBNAM(database.dbName);
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
	 * @param svrCod	Severity Code
	 * @exception DRDAProtocolException
	 */
	private void writeQRYNOPRM(int svrCod) throws DRDAProtocolException
	{
		writer.createDssReply();
		writer.startDdm(CodePoint.QRYNOPRM);
		writer.writeScalar2Bytes(CodePoint.SVRCOD, svrCod);
		writeRDBNAM(database.dbName);
		writePKGNAMCSN();
		writer.endDdmAndDss();
	}
	/**
	 * Write a OPNQFLRM - Open Query Failure
	 * Instance Variables
	 *  SVRCOD - Severity Code - required - 8 ERROR
	 *  RDBNAM - Relational Database Name - required
	 *
	 * @param	e	Exception describing failure
	 *
	 * @exception DRDAProtocolException
	 */
	private void writeOPNQFLRM(SQLException e) throws DRDAProtocolException
	{
		writer.createDssReply();
		writer.startDdm(CodePoint.OPNQFLRM);
		writer.writeScalar2Bytes(CodePoint.SVRCOD, CodePoint.SVRCOD_ERROR);
		writeRDBNAM(database.dbName);
		writer.endDdm();
		writer.startDdm(CodePoint.SQLCARD);
		writeSQLCAGRP(e, getSqlCode(getExceptionSeverity(e)), 0, 0);
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
	 * @exception throws DRDAProtocolException
	 */
	private void writePKGNAMCSN(String pkgcnstknStr) throws DRDAProtocolException
	{
		writer.startDdm(CodePoint.PKGNAMCSN);
		if (rdbcolid.length() == CodePoint.RDBCOLID_LEN && rdbnam.length() <= CodePoint.RDBNAM_LEN)
		{	//fixed format
			writer.writeScalarPaddedString(rdbnam, CodePoint.RDBNAM_LEN);
			writer.writeScalarPaddedString(rdbcolid, CodePoint.RDBCOLID_LEN);
			writer.writeScalarPaddedString(pkgid, CodePoint.PKGID_LEN);
			writer.writeScalarPaddedString(pkgcnstknStr, CodePoint.PKGCNSTKN_LEN);
			writer.writeShort(secnumber);
		}
		else	// extended format
		{
			writer.writeShort(rdbnam.length());
			writer.writeScalarPaddedString(rdbnam, rdbnam.length());
			writer.writeShort(rdbcolid.length());
			writer.writeScalarPaddedString(rdbcolid, rdbcolid.length());
			writer.writeShort(pkgid.length());
			writer.writeScalarPaddedString(pkgid, pkgid.length());
			writer.writeScalarPaddedString(pkgcnstknStr, CodePoint.PKGCNSTKN_LEN);
			writer.writeShort(secnumber);
		}
		writer.endDdm();
	}

	private void writePKGNAMCSN() throws DRDAProtocolException
	{
		writePKGNAMCSN(pkgcnstknStr);
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
	 * @exception DRDAProtocolException, SQLException
	 */
	private DRDAStatement parseCNTQRY() throws DRDAProtocolException, SQLException
	{
		byte val;
		String pkgnamcsn = null;
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
					if (SanityManager.DEBUG)
						trace("qryrelscr = "+qryrelscr);
					break;
				//optional
				case CodePoint.QRYSCRORN:
					checkLength(CodePoint.QRYSCRORN, 1);
					qryscrorn = reader.readUnsignedByte();
					if (SanityManager.DEBUG)
						trace("qryscrorn = "+qryscrorn);
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
					if (SanityManager.DEBUG)
						trace("qryrownbr = "+qryrownbr);
					break;
				//optional
				case CodePoint.QRYROWSNS:
					checkLength(CodePoint.QRYROWSNS, 1);
					qryrowsns = readBoolean(CodePoint.QRYROWSNS);
					if (SanityManager.DEBUG)
						trace("qryrowsns = "+qryrowsns);
					gotQryrowsns = true;
					break;
				//optional
				case CodePoint.QRYBLKRST:
					checkLength(CodePoint.QRYBLKRST, 1);
					qryblkrst = readBoolean(CodePoint.QRYBLKRST);
					if (SanityManager.DEBUG)
						trace("qryblkrst = "+qryblkrst);
					break;
				//optional
				case CodePoint.QRYRTNDTA:
					qryrtndta = readBoolean(CodePoint.QRYRTNDTA);
					if (SanityManager.DEBUG)
						trace("qryrtndta = "+qryrtndta);
					break;
				//optional
				case CodePoint.QRYROWSET:
					//Note minimum for CNTQRY is 1
					qryrowset = parseQRYROWSET(1);
					if (SanityManager.DEBUG)
						trace("qryrowset = "+qryrowset);
					break;
				//optional
				case CodePoint.QRYRFRTBL:
					qryrfrtbl = readBoolean(CodePoint.QRYRFRTBL);
					if (SanityManager.DEBUG)
						trace("qryrfrtbl = "+qryrfrtbl);
					break;
				//optional
				case CodePoint.NBRROW:
					checkLength(CodePoint.NBRROW, 4);
					nbrrow = reader.readNetworkInt();
					if (SanityManager.DEBUG)
						trace("nbrrow = "+nbrrow);
					break;
				//optional
				case CodePoint.MAXBLKEXT:
					checkLength(CodePoint.MAXBLKEXT, 2);
					maxblkext = reader.readSignedNetworkShort();
					if (SanityManager.DEBUG)
						trace("maxblkext = "+maxblkext);
					break;
				//optional
				case CodePoint.RTNEXTDTA:
					checkLength(CodePoint.RTNEXTDTA, 1);
					rtnextdta = reader.readUnsignedByte();
					if (rtnextdta != CodePoint.RTNEXTROW && 
							rtnextdta != CodePoint.RTNEXTALL)
						invalidValue(CodePoint.RTNEXTDTA);
					if (SanityManager.DEBUG)
						trace("rtnextdta = "+rtnextdta);
					break;
				// required for SQLAM >= 7
				case CodePoint.QRYINSID:
					checkLength(CodePoint.QRYINSID, 8);
					qryinsid = reader.readNetworkLong();
					gotQryinsid = true;
					if (SanityManager.DEBUG)
						trace("qryinsid = "+qryinsid);
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
		if (pkgnamcsn == null)
			missingCodePoint(CodePoint.PKGNAMCSN);
		if (!gotQryblksz)
			missingCodePoint(CodePoint.QRYBLKSZ);
		if (sqlamLevel >= MGRLVL_7 && !gotQryinsid)
			missingCodePoint(CodePoint.QRYINSID);

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

		if (reader.isChainedWithSameID())
			parseCNTQRYobjects(stmt);
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
				codePoint = reader.readLengthAndCodePoint();
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
	 * @param stmt	DRDA statement this applies to
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
			if (SanityManager.DEBUG)
				trace("num of vars is: "+numVars);
			int[] outovr_drdaType = null;
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
				outovr_drdaType[i] = reader.readUnsignedByte();
				if (SanityManager.DEBUG)
					trace("drdaType is: "+ outovr_drdaType[i]);
				precision = reader.readNetworkShort();
				if (SanityManager.DEBUG)
					trace("drdaLength is: "+precision);
				outovr_drdaType[i] |= (precision << 8);
			}
			stmt.setOutovr_drdaType(outovr_drdaType);
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
		if (SanityManager.DEBUG)
			trace("WriteOPNQRYRM");

		if (isDssObject)
			writer.createDssObject();
		else
			writer.createDssReply();
		writer.startDdm(CodePoint.OPNQRYRM);
		writer.writeScalar2Bytes(CodePoint.SVRCOD,CodePoint.SVRCOD_INFO);

		// There is currently a problem specifying LMTBLKPRC for LOBs with JCC
		// JCC will throw an ArrayOutOfBounds exception.  Once this is fixed, we
		// don't need to pass the two arguments for getQryprctyp.
		int prcType = stmt.getQryprctyp();
		if (SanityManager.DEBUG)
			trace("sending QRYPRCTYP: " + prcType);
		writer.writeScalar2Bytes(CodePoint.QRYPRCTYP, prcType);

		//pass the SQLCSRHLD codepoint only if statement has hold cursors over commit set
		if (stmt.withHoldCursor == JDBC30Translation.HOLD_CURSORS_OVER_COMMIT)
			writer.writeScalar1Byte(CodePoint.SQLCSRHLD, CodePoint.TRUE);
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
			if (stmt.getScrollType() != 0)
			{
				writer.writeScalar1Byte(CodePoint.QRYATTSCR, CodePoint.TRUE);
				//Cloudscape only supports insensitive scroll cursors
				writer.writeScalar1Byte(CodePoint.QRYATTSNS, CodePoint.QRYINS);
				//Cloudscape only supports read only scrollable cursors
				writer.writeScalar1Byte(CodePoint.QRYATTUPD, CodePoint.QRYRDO);
			}
			else
			{
				if (stmt.getConcurType() == ResultSet.CONCUR_UPDATABLE)
					writer.writeScalar1Byte(CodePoint.QRYATTUPD, CodePoint.QRYUPD);
				else
					writer.writeScalar1Byte(CodePoint.QRYATTUPD, CodePoint.QRYRDO);
			}

		}
		writer.endDdmAndDss ();
	}
	/**
	 * Write ENDQRYRM - query process has terminated in such a manner that the
	 *	query or result set is now closed.  It cannot be resumed with the CNTQRY
	 *  command or closed with the CLSQRY command
	 * @param srvCod  Severity code - WARNING or ERROR
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
	 * 	 * @exception DRDAProtocolException
	 */
	private void writeABNUOWRM() throws DRDAProtocolException
	{
		writer.createDssReply();
		writer.startDdm(CodePoint.ABNUOWRM);
		writer.writeScalar2Bytes(CodePoint.SVRCOD,CodePoint.SVRCOD_ERROR);
		writeRDBNAM(database.dbName);
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
		if (rdbName.length < CodePoint.RDBNAM_LEN || rdbName.length > CodePoint.MAX_NAME)
			badObjectLength(CodePoint.RDBNAM);
		name = reader.convertBytes(rdbName);
		// trim trailing blanks from the database name
		name = name.trim();
		if (SanityManager.DEBUG)
			trace("RdbName " + name);
		return name;
	}
	/**
	 * Write ACCSECRD
	 * If the security mechanism is known, we just send it back along with
	 * the security token if encryption is going to be used.
	 * If the security mechanism is not known, we send a list of the ones
	 * we know.
	 * Instance Variables
	 * 	SECMEC - security mechanism - required
	 *	SECTKN - security token	- optional (required if security mechanism 
	 *						uses encryption)
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
			writer.writeScalar2Bytes(CodePoint.SECMEC, database.securityMechanism);
		else
		{ 
			// these are the ones we know about
			writer.writeScalar2Bytes(CodePoint.SECMEC, CodePoint.SECMEC_USRIDPWD);
			writer.writeScalar2Bytes(CodePoint.SECMEC, CodePoint.SECMEC_EUSRIDPWD);
		}
		if (securityCheckCode != 0)
		{
			writer.writeScalar1Byte(CodePoint.SECCHKCD, securityCheckCode);
		}
		else
		{
			// we need to send back the key if encryption is being used
			if (database.securityMechanism == CodePoint.SECMEC_EUSRIDPWD)
				writer.writeScalarBytes(CodePoint.SECTKN, myPublicKey);
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
	 *	SECMGRNM - security manager name - optional, ignorable
	 *  SECMEC	- security mechanism - required
	 *	SECTKN	- security token - optional, (required if encryption used)
	 *	PASSWORD - password - optional, (required if security mechanism uses it)
	 *	NEWPASSWORD - new password - optional, (required if sec mech. uses it)
	 *	USRID	- user id - optional, (required if sec mec. uses it)
	 *	RDBNAM	- database name - optional (required if databases can have own sec.)
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
					if (SanityManager.DEBUG) 
						trace("Security mechanism = " + securityMechanism);
					//RESOLVE - spec is not clear on what should happen
					//in this case
					if (securityMechanism != database.securityMechanism)
						invalidValue(CodePoint.SECMEC);
					break;
				//optional - depending on security Mechanism 
				case CodePoint.SECTKN:
					if (database.securityMechanism != CodePoint.SECMEC_EUSRIDPWD)
					{
						securityCheckCode = CodePoint.SECCHKCD_SECTKNMISSING;
						reader.skipBytes();
					}
					else if (database.decryptedUserId == null) {
						try {
							database.decryptedUserId = reader.readEncryptedString(decryptionManager,
												database.securityMechanism, myPublicKey, database.publicKeyIn);
						} catch (SQLException se)
						{
							println2Log(database.dbName, session.drdaID, se.getMessage());
							if (securityCheckCode == 0)
								securityCheckCode = CodePoint.SECCHKCD_13;	//userid invalid
						}
						database.userId = database.decryptedUserId;
						if (SanityManager.DEBUG) trace("**decrypted userid is: "+database.userId);
					}
					else if (database.decryptedPassword == null) {
						try {
							database.decryptedPassword = reader.readEncryptedString(decryptionManager,
												database.securityMechanism, myPublicKey, database.publicKeyIn);
						} catch (SQLException se)
						{	
							println2Log(database.dbName, session.drdaID, se.getMessage());
							if (securityCheckCode == 0)
								securityCheckCode = CodePoint.SECCHKCD_0F;	//password invalid
						}
						database.password = database.decryptedPassword;
						if (SanityManager.DEBUG) trace("**decrypted password is: "+database.password);
					}
					else
					{
						tooMany(CodePoint.SECTKN);
					}
					break;
				//optional - depending on security Mechanism
				case CodePoint.PASSWORD:
					database.password = reader.readString();
					if (SanityManager.DEBUG) trace("PASSWORD " + database.password);
					break;
				//optional - depending on security Mechanism
				//we are not supporting this method so we'll skip bytes
				case CodePoint.NEWPASSWORD:
					reader.skipBytes();
					break;
				//optional - depending on security Mechanism
				case CodePoint.USRID:
					database.userId = reader.readString();
					if (SanityManager.DEBUG) trace("USERID " + database.userId);
					break;
				//optional - depending on security Mechanism
				case CodePoint.RDBNAM:
					String dbname = parseRDBNAM();
					if (database != null) 
					{
						if (!database.dbName.equals(dbname))
							rdbnamMismatch(CodePoint.SECCHK);
					}
					else
					{
						// we should already have added the database in ACCSEC
						// added code here in case we make the SECMEC session rather
						// than database wide
						addDatabase(dbname);
					}
					break;
				default:
					invalidCodePoint(codePoint);

			}
			codePoint = reader.getCodePoint();
		}
		// check for SECMEC which is required
		if (securityMechanism == 0)
			missingCodePoint(CodePoint.SECMEC);

		//check if we have a userid and password when we need it
		if (securityCheckCode == 0 && 
				database.securityMechanism == CodePoint.SECMEC_USRIDPWD)
		{
			if (database.userId == null)
				securityCheckCode = CodePoint.SECCHKCD_USERIDMISSING;
			else if (database.password == null)
				securityCheckCode = CodePoint.SECCHKCD_PASSWORDMISSING;
			//Note, we'll ignore encryptedUserId and encryptedPassword if they
			//are also set
		}
		if (securityCheckCode == 0 && 
				database.securityMechanism == CodePoint.SECMEC_EUSRIDPWD)
		{
			if (database.decryptedUserId == null)
				securityCheckCode = CodePoint.SECCHKCD_USERIDMISSING;
			else if (database.decryptedPassword == null)
				securityCheckCode = CodePoint.SECCHKCD_PASSWORDMISSING;

		}
		// RESOLVE - when we do security we need to decrypt encrypted userid & password
		// before proceeding

		// verify userid and password, if we haven't had any errors thus far.
		if ((securityCheckCode == 0) && (databaseAccessException == null))
		{
			securityCheckCode = verifyUserIdPassword();
		}
		return securityCheckCode;

	}
	/**
	 * Write security check reply
	 * Instance variables
	 * 	SVRCOD - serverity code - required
	 *	SECCHKCD	- security check code  - required
	 *	SECTKN - security token - optional, ignorable
	 *	SVCERRNO	- security service error number
	 *	SRVDGN	- Server Diagnostic Information
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
			securityCheckCode == 5 || securityCheckCode == 8)
			return CodePoint.SVRCOD_INFO;
		else
			return CodePoint.SVRCOD_ERROR;
	}
	/**
	 * Parse access RDB
	 * Instance variables
	 *	RDBACCCL - RDB Access Manager Class - required must be SQLAM
	 *  CRRTKN - Correlation Token - required
	 *  RDBNAM - Relational database name -required
	 *  PRDID - Product specific identifier - required
	 *  TYPDEFNAM	- Data Type Definition Name -required
	 *  TYPDEFOVR	- Type definition overrides -required
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
					if (SanityManager.DEBUG) 
						trace("RDBACCCL = " + sqlam);
					// required to be SQLAM 

					if (sqlam != CodePoint.SQLAM)
						invalidValue(CodePoint.RDBACCCL);
					removeFromRequired(CodePoint.RDBACCCL);
					break;
				//required
				case CodePoint.CRRTKN:
					database.crrtkn = reader.readBytes();
					if (SanityManager.DEBUG) 
						trace("crrtkn " + convertToHexString(database.crrtkn));
					removeFromRequired(CodePoint.CRRTKN);
					int l = database.crrtkn.length;
					if (l > CodePoint.MAX_NAME)
						tooBig(CodePoint.CRRTKN);
					// the format of the CRRTKN is defined in the DRDA reference
					// x.yz where x is 1 to 8 bytes (variable)
					// 		y is 1 to 8 bytes (variable)
					//		x is 6 bytes fixed
					//		size is variable between 9 and 23
					if (l < 9 || l > 23)
						invalidValue(CodePoint.CRRTKN);
					byte[] part1 = new byte[l - 6];
					for (int i = 0; i < part1.length; i++)
						part1[i] = database.crrtkn[i];
					long time = SignedBinary.getLong(database.crrtkn, 
							l-8, SignedBinary.BIG_ENDIAN); // as "long" as unique
					session.drdaID = reader.convertBytes(part1) + 
									time + leftBrace + session.connNum + rightBrace;
					if (SanityManager.DEBUG) 
						trace("******************************************drdaID is: " + session.drdaID);
					EmbedConnection conn = (EmbedConnection)(database.getConnection());
					if (conn != null)
						conn.setDrdaID(session.drdaID);
					break;
				//required
				case CodePoint.RDBNAM:
					String dbname = parseRDBNAM();
					if (database != null)
					{ 
						if (!database.dbName.equals(dbname))
							rdbnamMismatch(CodePoint.ACCRDB);
					}
					else
					{
						//first time we have seen a database name
						Database d = session.getDatabase(dbname);
						if (d == null)
							addDatabase(dbname);
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
					if (SanityManager.DEBUG) 
						trace("prdId " + appRequester.prdid);
					if (appRequester.prdid.length() > CodePoint.PRDID_MAX)
						tooBig(CodePoint.PRDID);

					/* If JCC version is 1.5 or later, send SQLWarning on CNTQRY */
					if ((appRequester.getClientType() == appRequester.JCC_CLIENT) &&
						(appRequester.greaterThanOrEqualTo(1, 5, 0)))
					{
						sendWarningsOnCNTQRY = true;
					}
					else sendWarningsOnCNTQRY = false;

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
					if (SanityManager.DEBUG) 
						trace("rdbAllowUpdates = "+database.rdbAllowUpdates);
					break;
				//optional, ignorable
				case CodePoint.PRDDTA:
					// check that it fits in maximum but otherwise ignore for now
					if (reader.getDdmLength() > CodePoint.MAX_NAME)
						tooBig(CodePoint.PRDDTA);
					reader.skipBytes();
					break;
				case CodePoint.TRGDFTRT:
					byte b = reader.readByte();
					if (b == 0xF1)
						database.sendTRGDFTRT = true;
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
			svrcod = CodePoint.SVRCOD_WARNING;
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
		if (SanityManager.DEBUG) trace("typeDefName " + typDefNam);
		if (typDefNam.length() > CodePoint.MAX_NAME)
			tooBig(CodePoint.TYPDEFNAM);
		checkValidTypDefNam(typDefNam);
		// check if the typedef is one we support
		if (!typDefNam.equals(CodePoint.TYPDEFNAM_QTDSQLASC)  &&
			!typDefNam.equals(CodePoint.TYPDEFNAM_QTDSQLJVM) &&
			!typDefNam.equals(CodePoint.TYPDEFNAM_QTDSQLX86))
			valueNotSupported(CodePoint.TYPDEFNAM);
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
	 *  CRRTKN	- correlation token - only returned if we didn't get one from requester
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
		writer.writeScalarString(CodePoint.PRDID, server.prdId);
		//TYPDEFNAM -required - JCC doesn't support QTDSQLJVM so for now we
		// just use ASCII, though we should eventually be able to use QTDSQLJVM
		// at level 7
		writer.writeScalarString(CodePoint.TYPDEFNAM,
								 CodePoint.TYPDEFNAM_QTDSQLASC);
		writeTYPDEFOVR();
		writer.endDdmAndDss ();
		finalizeChain();
	}
	
	private void writeTYPDEFOVR() throws DRDAProtocolException
	{
		//TYPDEFOVR - required - only single byte and mixed byte are specified
		writer.startDdm(CodePoint.TYPDEFOVR);
		writer.writeScalar2Bytes(CodePoint.CCSIDSBC, server.CCSIDSBC);
		writer.writeScalar2Bytes(CodePoint.CCSIDMBC, server.CCSIDMBC);
		// PKGDFTCST - Send character subtype and userid if requested
		if (database.sendTRGDFTRT)
		{
			// default to multibyte character
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
	 * 	TYPDEF Overrides specifies the Coded Character SET Identifiers (CCSIDs)
	 *  that are in a named TYPDEF.
	 * Instance Variables
	 *  CCSIDSBC - CCSID for Single-Byte - optional
	 *  CCSIDDBC - CCSID for Double-Byte - optional
	 *  CCSIDMBC - CCSID for Mixed-byte characters -optional
	 *
 	 * @param st	Statement this TYPDEFOVR applies to
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
		if (codePoint == -1 && st != null)
			missingCodePoint(CodePoint.CCSIDSBC);

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
					if (SanityManager.DEBUG) 
						trace("ccsidsbc = " + ccsidSBC + " encoding = " + ccsidSBCEncoding);
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
						if (st != null)
							valueNotSupported(CodePoint.CCSIDSBC);
					}
					if (SanityManager.DEBUG) 
						trace("ccsiddbc = " + ccsidDBC + " encoding = " + ccsidDBCEncoding);
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
						if (st != null)
							valueNotSupported(CodePoint.CCSIDMBC);
					}
					if (SanityManager.DEBUG) 
						trace("ccsidmbc = " + ccsidMBC + " encoding = " + ccsidMBCEncoding);
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
	 *   PKGNAMCAN - RDB Package Name, Consistency Token, and Section Number - required
	 *   RTNSQLDA - Return SQL Descriptor Area - optional
	 *   MONITOR - Monitor events - optional.
	 *   
	 * @return return 0 - don't return sqlda, 1 - return input sqlda, 
	 * 		2 - return output sqlda
	 * @exception DRDAProtocolException, SQLException
	 */
	private int parsePRPSQLSTT() throws DRDAProtocolException,SQLException
	{
		int codePoint;
		boolean rtnsqlda = false;
		boolean rtnOutput = true; 	// Return output SQLDA is default
		String typdefnam;
		String pkgnamcsn = null;

		DRDAStatement stmt = null;  
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

		stmt = database.newDRDAStatement(pkgnamcsn);
		String sqlStmt = parsePRPSQLSTTobjects(stmt);
		if (databaseToSet != null)
			stmt.setDatabase(database);
		stmt.explicitPrepare(sqlStmt);
		// set the statement as the current statement
		database.setCurrentStatement(stmt);

		if (!rtnsqlda)
			return 0;
		else if (rtnOutput)   
			return 2;
		else
			return 1;
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
	 * @exception DRDAProtocolException, SQLException
	 */
	private String parsePRPSQLSTTobjects(DRDAStatement stmt) 
		throws DRDAProtocolException, SQLException
	{
		String sqlStmt = null;
		int codePoint;
		do
		{
			correlationID = reader.readDssHeader();
			while (reader.moreDssData())
			{
				codePoint = reader.readLengthAndCodePoint();
				switch(codePoint)
				{
					// required
					case CodePoint.SQLSTT:
						sqlStmt = parseEncodedString();
						if (SanityManager.DEBUG) 
							trace("sqlStmt = " + sqlStmt);
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
		if (sqlStmt == null)
			missingCodePoint(CodePoint.SQLSTT);

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
		if (SanityManager.DEBUG) 
			trace("typSQLDa " + sqldaType);
		if (sqldaType == CodePoint.TYPSQLDA_STD_OUTPUT ||
				sqldaType == CodePoint.TYPSQLDA_LIGHT_OUTPUT ||
				sqldaType == CodePoint.TYPSQLDA_X_OUTPUT)
			return true;
		else if (sqldaType == CodePoint.TYPSQLDA_STD_INPUT ||
					 sqldaType == CodePoint.TYPSQLDA_LIGHT_INPUT ||
					 sqldaType == CodePoint.TYPSQLDA_X_INPUT)
				return false;
		else
			invalidValue(CodePoint.TYPSQLDA);

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
		if (SanityManager.DEBUG)
			trace("sqlattr = '" + attrs+"'");
		//let Cloudscape handle any errors in the types it doesn't support
		//just set the attributes

		boolean insensitive = false;
		boolean validAttribute = false;
		if (attrs.indexOf("INSENSITIVE SCROLL") != -1 || attrs.indexOf("SCROLL INSENSITIVE") != -1) //CLI
		{
			stmt.scrollType = ResultSet.TYPE_SCROLL_INSENSITIVE;
			stmt.concurType = ResultSet.CONCUR_READ_ONLY;
			insensitive = true;
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
			if (!insensitive)
			stmt.concurType = ResultSet.CONCUR_UPDATABLE;
		}

		if (attrs.indexOf("WITH HOLD") != -1)
		{
			stmt.withHoldCursor = JDBC30Translation.HOLD_CURSORS_OVER_COMMIT;
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
	 *	TYPSQLDA - sqlda type expected (output or input)
	 *  RDBNAM - relational database name - optional
	 *  PKGNAMCSN - RDB Package Name, Consistency Token and Section Number - required
	 *  MONITOR - Monitor events - optional.
	 *
	 * @return expect "output sqlda" or not
	 * @exception DRDAProtocolException, SQLException
	 */
	private boolean parseDSCSQLSTT() throws DRDAProtocolException,SQLException
	{
		int codePoint;
		boolean rtnOutput = true;	// default
		String pkgnamcsn = null;
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
		if (pkgnamcsn == null)
			missingCodePoint(CodePoint.PKGNAMCSN);
		return rtnOutput;
	}

	/**
	 * Parse EXCSQLSTT - Execute non-cursor SQL Statement previously prepared
	 * Instance Variables
	 *  RDBNAM - relational database name - optional
	 *  PKGNAMCSN - RDB Package Name, Consistency Token and Section Number - required
	 *  OUTEXP - Output expected
	 *  NBRROW - Number of rows to be inserted if it's an insert
	 *  PRCNAM - procedure name if specified by host variable, not needed for Cloudscape
	 *  QRYBLKSZ - query block size
	 *  MAXRSLCNT - max resultset count
	 *  MAXBLKEXT - Max number of extra blocks
	 *  RSLSETFLG - resultset flag
	 *  RDBCMTOK - RDB Commit Allowed - optional
	 *  OUTOVROPT - output override option
	 *  QRYROWSET - Query Rowset Size - Level 7
	 *  MONITOR - Monitor events - optional.
	 *
	 * @exception DRDAProtocolException, SQLException
	 */
	private void parseEXCSQLSTT() throws DRDAProtocolException,SQLException
	{
		int codePoint;
		String strVal;
		reader.markCollection();

		codePoint = reader.getCodePoint();
		boolean outputExpected = false;
		String pkgnamcsn = null;
		int numRows = 1;	// default value
		int blkSize =  0;
 		int maxrslcnt = 0; 	// default value
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
					if (SanityManager.DEBUG) 
						trace("outexp = "+ outputExpected);
					break;
				// optional
				case CodePoint.NBRROW:
					checkLength(CodePoint.NBRROW, 4);
					numRows = reader.readNetworkInt();
					if (SanityManager.DEBUG) 
						trace("# of rows: "+numRows);
					break;
				// optional
				case CodePoint.PRCNAM:
					procName = reader.readString();
					if (SanityManager.DEBUG) 
						trace("Procedure Name = " + procName);
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
					if (SanityManager.DEBUG) 
						trace("max rs count: "+maxrslcnt);
					break;
				// optional
				case CodePoint.MAXBLKEXT:
					// number of extra qury blocks of answer set data per result set
					// 0 - no extra query blocks
					// -1 - can receive entire result set
					checkLength(CodePoint.MAXBLKEXT, 2);
					maxblkext = reader.readNetworkShort();
					if (SanityManager.DEBUG) 
						trace("max extra blocks: "+maxblkext);
					break;
				// optional
				case CodePoint.RSLSETFLG:
					//Result set flags
					rslsetflg = reader.readBytes();
					for (int i=0;i<rslsetflg.length;i++)
						if (SanityManager.DEBUG) 
							trace("rslsetflg: "+rslsetflg[i]);
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

		if (pkgnamcsn == null)
			missingCodePoint(CodePoint.PKGNAMCSN);

		DRDAStatement stmt;
		boolean needPrepareCall = false;

		stmt  = database.getDRDAStatement(pkgnamcsn);
		boolean isProcedure = (procName !=null || 
							   (stmt != null && 
								stmt.wasExplicitlyPrepared() &&
								stmt.isCall));

		if (isProcedure)		// stored procedure call
		{
			if ( stmt == null  || !(stmt.wasExplicitlyPrepared()))
			{ 				
				stmt  = database.newDRDAStatement(pkgnamcsn);
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

 		stmt.nbrrow = numRows;
 		stmt.qryrowset = qryrowset;
 		stmt.blksize = blkSize;
 		stmt.maxblkext = maxblkext;
 		stmt.maxrslcnt = maxrslcnt;
 		stmt.outovropt = outovropt;
 		stmt.rslsetflg = rslsetflg;
 
	
		// set the statement as the current statement
		database.setCurrentStatement(stmt);
		
		
		if (reader.isChainedWithSameID())
			parseEXCSQLSTTobjects(stmt);
		else if (isProcedure  && (needPrepareCall))
		{
			// if we had parameters the callable statement would
			// be prepared with parseEXCQLSTTobjects, otherwise we
			// have to do it here
			String prepareString = "call " + stmt.procName +"()";
			if (SanityManager.DEBUG) 
				trace ("$$$prepareCall is: "+prepareString);
			database.getConnection().clearWarnings();
			CallableStatement cs = (CallableStatement) stmt.prepare(prepareString);
		}

		stmt.ps.clearWarnings();

		boolean hasResultSet = stmt.execute();
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
				writer.endDdm();
				writer.endDdmAndDss();
			}
			else if (hasResultSet)
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
		
		//We need to marke that params are finished so that we know we 
		// are ready to send resultset info.
		stmt.finishParams();
			
		PreparedStatement ps = stmt.getPreparedStatement();
		int rsNum = 0;
		do {
		if (hasResultSet)
		{
			stmt.setCurrentDrdaResultSet(rsNum);
			//indicate that we are going to return data
			stmt.setQryrtndta(true);
			if (! isProcedure)
				checkWarning(null, ps, null, -1, true, true);
			if (rsNum == 0)
				writeSQLRSLRD(stmt);
			writeOPNQRYRM(true, stmt);
			writeSQLCINRD(stmt);
			writeQRYDSC(stmt, false);
			stmt.rsSuspend();

			/* Currently, if LMTBLKPRC is used, a pre-condition is that no lob columns.
			 * But in the future, when we do support LOB in LMTBLKPRC, the drda spec still
			 * does not allow LOB to be sent with OPNQRYRM.  So this "if" here will have
			 * to add "no lob columns".
			 */
			if (stmt.getQryprctyp() == CodePoint.LMTBLKPRC)
				writeQRYDTA(stmt);
		}
		else  if (! sendSQLDTARD)
		{
			int updateCount = ps.getUpdateCount();
			if (false && (database.RDBUPDRM_sent == false) &&
				! isProcedure)
			{
				writeRDBUPDRM();
			}

			checkWarning(database.getConnection(), stmt.ps, null, updateCount, true, true);
		}

		} while(hasResultSet && (++rsNum < numResults));
		
		return;			// we are done
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
		if (SanityManager.DEBUG) 
			trace("rdbcmtok = " + rdbcmtok);
	}

	/**
	 * Parse EXCSQLSTT command objects
	 * Command Objects
	 *  TYPDEFNAM - Data Type Definition Name - optional
	 *  TYPDEFOVR - TYPDEF Overrides -optional
	 *	SQLDTA - optional, variable data, specified if prpared statement has input parameters
	 *	EXTDTA - optional, externalized FD:OCA data
	 *	OUTOVR - output override descriptor, not allowed for stored procedure calls
	 *
	 * If TYPDEFNAM and TYPDEFOVR are supplied, they apply to the objects
	 * sent with the statement.  Once the statement is over, the default values
	 * sent in the ACCRDB are once again in effect.  If no values are supplied,
	 * the values sent in the ACCRDB are used.
	 * Objects may follow in one DSS or in several DSS chained together.
	 * 
	 * @param the DRDAStatement to execute
	 * @exception DRDAProtocolException, SQLException
	 */
	private void parseEXCSQLSTTobjects(DRDAStatement stmt) throws DRDAProtocolException, SQLException
	{
		int codePoint;
		boolean gotSQLDTA = false, typeDefChanged = false;
		do
		{
			correlationID = reader.readDssHeader();
			while (reader.moreDssData())
			{
				codePoint = reader.readLengthAndCodePoint();
				switch(codePoint)
				{
					// optional
					case CodePoint.TYPDEFNAM:
						setStmtOrDbByteOrder(false, stmt, parseTYPDEFNAM());
						typeDefChanged = true;
						break;
					// optional
					case CodePoint.TYPDEFOVR:
						parseTYPDEFOVR(stmt);
						typeDefChanged = true;
						break;
					// required
					case CodePoint.SQLDTA:
						parseSQLDTA(stmt);
						gotSQLDTA = true;
						break;
					// optional
					case CodePoint.EXTDTA:	
						readAndSetAllExtParams(stmt);
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
		if (! gotSQLDTA)
			missingCodePoint(CodePoint.SQLDTA);
		if (typeDefChanged)
			stmt.setTypDefValues();
	}

	/**
	 * Write SQLCINRD - result set column information
	 *
	 * @exception DRDAProtocolException, SQLException
	 */
	private void writeSQLCINRD(DRDAStatement stmt) throws DRDAProtocolException,SQLException
	{
		ResultSet rs = null;
		PreparedStatement ps = stmt.getPreparedStatement();
		
		if (!stmt.needsToSendParamData)
			rs = stmt.getResultSet();

		writer.createDssObject();
		writer.startDdm(CodePoint.SQLCINRD);
		if (sqlamLevel >= MGRLVL_7)
			writeSQLDHROW (stmt);

		ResultSetMetaData rsmeta = rs.getMetaData();
		int ncols = rsmeta.getColumnCount();
		writer.writeShort(ncols);	// num of columns
		if (sqlamLevel >= MGRLVL_7)
		{
			for (int i = 0; i < ncols; i++)
				writeSQLDAGRP (rsmeta, null, i, true);
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
	 * @exception DRDAProtocolException, SQLException
	 */
	private void writeSQLRSLRD(DRDAStatement stmt) throws DRDAProtocolException,SQLException
	{

		EmbedPreparedStatement ps = (EmbedPreparedStatement) stmt.getPreparedStatement();
		int numResults = stmt.getNumResultSets();

		writer.createDssObject();
		writer.startDdm(CodePoint.SQLRSLRD);
		writer.writeShort(numResults); // num of result sets

		for (int i = 0; i < numResults; i ++)
			{
				writer.writeInt(i);	// rsLocator
				writeVCMorVCS(stmt.getResultSetCursorName(i));
				writer.writeInt(1);	// num of rows XXX resolve, it doesn't matter for now

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
	 * @exception DRDAProtocolException, SQLException
	 */
	private void writeRSLSETRM(DRDAStatement stmt) throws DRDAProtocolException,SQLException
	{
		int numResults = stmt.getNumResultSets();
		writer.createDssReply();
		writer.startDdm(CodePoint.RSLSETRM);
		writer.writeScalar2Bytes(CodePoint.SVRCOD, 0);
		writer.startDdm(CodePoint.PKGSNLST);
		
		for (int i = 0; i < numResults; i++)
			writePKGNAMCSN(stmt.getResultSetPkgcnstknStr(i));
		writer.endDdm();
		writer.endDdmAndDss();
	}

	
	/**
	 * Parse SQLDTA - SQL program variable data 
	 * and handle exception.
	 * @see parseSQLDTA_work
	 */

	private void parseSQLDTA(DRDAStatement stmt) throws DRDAProtocolException,SQLException
	{
		try {
			parseSQLDTA_work(stmt);
		} 
		catch (SQLException se)
		{
			skipRemainder(false);
			throw se;
		}
	}
	
	/**
	 * Parse SQLDTA - SQL program variable data
	 * Instance Variables
	 *  FDODSC - FD:OCA data descriptor - required
	 *  FDODTA - FD:OCA data - optional
	 *    
	 * @exception DRDAProtocolException, SQLException
	 */
	private void parseSQLDTA_work(DRDAStatement stmt) throws DRDAProtocolException,SQLException
	{
		String strVal;
		PreparedStatement ps = stmt.getPreparedStatement();
		int codePoint;
		EmbedParameterSetMetaData pmeta = null;
		Vector paramDrdaTypes = new Vector();
		Vector paramLens = new Vector();
		ArrayList paramExtPositions = null;
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
						if (SanityManager.DEBUG) 
							trace("num of vars in this group is: "+numVarsInGrp);
						reader.readByte();		// tripletType
						reader.readByte();		// id
						for (int j = 0; j < numVarsInGrp; j++)
						{
							paramDrdaTypes.addElement(new Byte(reader.readByte()));
							if (SanityManager.DEBUG) 
								trace("drdaType is: "+ "0x" +
       								  Integer.toHexString(((Byte ) paramDrdaTypes.lastElement()).byteValue()));
							int drdaLength = reader.readNetworkShort();
							if (SanityManager.DEBUG) 
								trace("drdaLength is: "+drdaLength);
							paramLens.addElement(new Integer(drdaLength));
						}
					}
					numVars = paramDrdaTypes.size();
					if (SanityManager.DEBUG)
						trace("numVars = " + numVars);
					if (ps == null)		// it is a CallableStatement under construction
					{
						String marks = "(?";	// construct parameter marks
						for (int i = 1; i < numVars; i++)
							marks += ", ?";
						String prepareString = "call " + stmt.procName + marks + ")";
						if (SanityManager.DEBUG) 
							trace ("$$ prepareCall is: "+prepareString);
						CallableStatement cs = null;
						try {
							cs = (CallableStatement)
								stmt.prepare(prepareString);			
							stmt.registerAllOutParams();
						} catch (SQLException se) {
							if (! stmt.outputExpected || 
								(!se.getSQLState().equals(SQLState.LANG_NO_METHOD_FOUND)))
								throw se;
							if (SanityManager.DEBUG) 
								trace("****** second try with return parameter...");
							// Save first SQLException most likely suspect
							if (numVars == 1)
								prepareString = "? = call " + stmt.procName +"()";
							else
								prepareString = "? = call " + stmt.procName +"("+marks.substring(3) + ")";
							if (SanityManager.DEBUG)
								trace ("$$ prepareCall is: "+prepareString);
							try {
								cs = (CallableStatement) stmt.prepare(prepareString);
							} catch (SQLException se2)
							{
								// The first exception is the most likely suspect
								throw se;
							}
							rtnParam = true;
						}
						ps = cs;
					}

					pmeta = ((EmbedPreparedStatement) ps).getEmbedParameterSetMetaData();

					reader.readBytes(6);	// descriptor footer
					break;
				// optional
				case CodePoint.FDODTA:
					reader.readByte();	// row indicator
					for (int i = 0; i < numVars; i++)
					{
					
						if ((((Byte)paramDrdaTypes.elementAt(i)).byteValue() & 0x1) == 0x1)	// nullable
						{
							int nullData = reader.readUnsignedByte();
							if ((nullData & 0xFF) == FdocaConstants.NULL_DATA)
							{
								if (SanityManager.DEBUG)
									trace("******param null");
								if (pmeta.getParameterMode(i + 1) 
									!= JDBC30Translation.PARAMETER_MODE_OUT )
										ps.setNull(i+1, pmeta.getParameterType(i+1));
								if (stmt.isOutputParam(i+1))
									stmt.registerOutParam(i+1);
								continue;
							}
						}

						// not null, read and set it
						paramExtPositions = readAndSetParams(i, stmt,
															 ((Byte)paramDrdaTypes.elementAt(i)).byteValue(),
															 pmeta,
															 paramExtPositions,
															 ((Integer)(paramLens.elementAt(i))).intValue());
					}
					stmt.cliParamExtPositions = paramExtPositions;
					stmt.cliParamDrdaTypes = paramDrdaTypes;
					stmt.cliParamLens = paramLens;	
					break;
				case CodePoint.EXTDTA:
					readAndSetAllExtParams(stmt);
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

	/**
	 * Read different types of input parameters and set them in PreparedStatement
	 * @param i			index of the parameter
	 * @param stmt       drda statement
	 * @param drdaType	drda type of the parameter
	 * @param pmeta		parameter meta data
	 * @param paramExtPositions  ArrayList of parameters with extdta
	 * @param paramLenNumBytes Number of bytes for encoding LOB Length
	 *
	 * @return updated paramExtPositions
	 * @exception DRDAProtocolException, SQLException
	 */
	private ArrayList readAndSetParams(int i, DRDAStatement stmt, int
									   drdaType, EmbedParameterSetMetaData pmeta,
									   ArrayList paramExtPositions,
									   int paramLenNumBytes)
				throws DRDAProtocolException, SQLException
	{
		PreparedStatement ps = stmt.getPreparedStatement();
		// mask out null indicator
		drdaType = ((drdaType | 0x01) & 0x000000ff);

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
			case FdocaConstants.DRDA_TYPE_NSMALL:
			{
				short paramVal = (short) reader.readShort(getByteOrder());
				if (SanityManager.DEBUG)
					trace("short parameter value is: "+paramVal);
 				// DB2 does not have a BOOLEAN java.sql.bit type, it's sent as small
				if (pmeta.getParameterType(i+1) == JDBC30Translation.BOOLEAN)
					ps.setBoolean(i+1, (paramVal == 1));
				else
					ps.setShort(i+1, paramVal);
				break;
			}
			case  FdocaConstants.DRDA_TYPE_NINTEGER:
			{
				int paramVal = reader.readInt(getByteOrder());
				if (SanityManager.DEBUG)
					trace("integer parameter value is: "+paramVal);
				ps.setInt(i+1, paramVal);
				break;
			}
			case FdocaConstants.DRDA_TYPE_NINTEGER8:
			{
				long paramVal = reader.readLong(getByteOrder());
				if (SanityManager.DEBUG)
					trace("parameter value is: "+paramVal);
				ps.setLong(i+1, paramVal);
				break;
			}
			case FdocaConstants.DRDA_TYPE_NFLOAT4:
			{
				float paramVal = reader.readFloat(getByteOrder());
				if (SanityManager.DEBUG) 
					trace("parameter value is: "+paramVal);
				ps.setFloat(i+1, paramVal);
				break;
			}
			case FdocaConstants.DRDA_TYPE_NFLOAT8:
			{
				double paramVal = reader.readDouble(getByteOrder());
				if (SanityManager.DEBUG) 
					trace("nfloat8 parameter value is: "+paramVal);
				ps.setDouble(i+1, paramVal);
				break;
			}
			case FdocaConstants.DRDA_TYPE_NDECIMAL:
			{
				int precision = (paramLenNumBytes >> 8) & 0xff;
				int scale = paramLenNumBytes & 0xff;
				BigDecimal paramVal = reader.readBigDecimal(precision, scale);
				if (SanityManager.DEBUG)
					trace("ndecimal parameter value is: "+paramVal);
				ps.setBigDecimal(i+1, paramVal);
				break;
			}
			case FdocaConstants.DRDA_TYPE_NDATE:
			{
				String paramVal = reader.readStringData(10).trim();  //parameter may be char value
				if (SanityManager.DEBUG) 
					trace("ndate parameter value is: \""+paramVal+"\"");
				ps.setDate(i+1, java.sql.Date.valueOf(paramVal));
				break;
			}
			case FdocaConstants.DRDA_TYPE_NTIME:
			{
				String paramVal = reader.readStringData(8).trim();  //parameter may be char value
				if (SanityManager.DEBUG) 
					trace("ntime parameter value is: "+paramVal);
				ps.setTime(i+1, java.sql.Time.valueOf(paramVal));
				break;
			}
			case FdocaConstants.DRDA_TYPE_NTIMESTAMP:
			{
				// DB2 represents ts with 26 chars, and a slightly different format than Java standard
				// we do the conversion and pad 3 digits for nano seconds.
				String paramVal = reader.readStringData(26).trim();  //parameter may be char value
				if (SanityManager.DEBUG)
					trace("ntimestamp parameter value is: "+paramVal);
				String tsString = paramVal.substring(0,10)+" "+paramVal.substring(11,19).replace('.', ':')+paramVal.substring(19)+"000";
				if (SanityManager.DEBUG)
					trace("tsString is: "+tsString);
				ps.setTimestamp(i+1, java.sql.Timestamp.valueOf(tsString));
				break;
			}
			case FdocaConstants.DRDA_TYPE_NCHAR:
			case FdocaConstants.DRDA_TYPE_NVARCHAR:
			case FdocaConstants.DRDA_TYPE_NLONG:
			case FdocaConstants.DRDA_TYPE_NVARMIX:
			case FdocaConstants.DRDA_TYPE_NLONGMIX:
			{
				String paramVal = reader.readLDStringData(stmt.ccsidMBCEncoding);
				if (SanityManager.DEBUG)
					trace("char/varchar parameter value is: "+paramVal);
				ps.setString(i+1, paramVal);
				break;
			}
			case  FdocaConstants.DRDA_TYPE_NFIXBYTE:
			{
				byte[] paramVal = reader.readBytes();
				if (SanityManager.DEBUG) 
					trace("fix bytes parameter value is: "+new String(paramVal));
				ps.setBytes(i+1, paramVal);
				break;
			}
			case FdocaConstants.DRDA_TYPE_NVARBYTE:
			case FdocaConstants.DRDA_TYPE_NLONGVARBYTE:
			{
				int length = reader.readNetworkShort();	//protocol control data always follows big endian
				if (SanityManager.DEBUG)
					trace("===== binary param length is: " + length);
				byte[] paramVal = reader.readBytes(length);
				ps.setBytes(i+1, paramVal);
				break;
			}
			case FdocaConstants.DRDA_TYPE_NLOBBYTES:
			case FdocaConstants.DRDA_TYPE_NLOBCMIXED:
			case FdocaConstants.DRDA_TYPE_NLOBCSBCS:
			case FdocaConstants.DRDA_TYPE_NLOBCDBCS:
			 {
				 long length = readLobLength(paramLenNumBytes);
				 if (length != 0) //can be -1 for CLI if "data at exec" mode, see clifp/exec test
				 {
					if (paramExtPositions == null)
						 paramExtPositions = new ArrayList();
				 	paramExtPositions.add(new Integer(i));
				 }
				 else   /* empty */
				 {
					if (drdaType == FdocaConstants.DRDA_TYPE_NLOBBYTES)
						ps.setBytes(i+1, new byte[0]);
					else
						ps.setString(i+1, "");
				 }
				 break;
			 }
			default:
				{
				String paramVal = reader.readLDStringData(stmt.ccsidMBCEncoding);
				if (SanityManager.DEBUG) 
					trace("default type parameter value is: "+paramVal);
				ps.setObject(i+1, paramVal);
			}
		}
		return paramExtPositions;
	}

	private long readLobLength(int extLenIndicator) 
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
	

	private void readAndSetAllExtParams(DRDAStatement stmt) 
		throws SQLException, DRDAProtocolException
	{
		int numExt = stmt.cliParamExtPositions.size();
		for (int i = 0; i < stmt.cliParamExtPositions.size(); i++)
					{
						int paramPos = ((Integer) (stmt.cliParamExtPositions).get(i)).intValue();
						readAndSetExtParam(paramPos,
										   stmt,
										   ((Byte)stmt.cliParamDrdaTypes.elementAt(paramPos)).intValue(),((Integer)(stmt.cliParamLens.elementAt(paramPos))).intValue());
						// Each extdta in it's own dss
						if (i < numExt -1)
						{
							correlationID = reader.readDssHeader();
							int codePoint = reader.readLengthAndCodePoint();
						}
					}

	}
	

	/**
	 * Read different types of input parameters and set them in PreparedStatement
	 * @param i			index of the parameter
	 * @param ps		associated ps
	 * @param drdaType	drda type of the parameter
	 *
	 * @exception DRDAProtocolException, SQLException
	 */
	private void readAndSetExtParam( int i, DRDAStatement stmt,
									  int drdaType, int extLen)
				throws DRDAProtocolException, SQLException
		{
			PreparedStatement ps = stmt.getPreparedStatement();
			drdaType = (drdaType & 0x000000ff); // need unsigned value
			boolean checkNullability = false;
			if (sqlamLevel >= MGRLVL_7 &&
				FdocaConstants.isNullable(drdaType))
				checkNullability = true;
	
			try {	
				byte[] paramBytes = reader.getExtData(checkNullability);
				String paramString = null;
				switch (drdaType)
				{
					case  FdocaConstants.DRDA_TYPE_LOBBYTES:
					case  FdocaConstants.DRDA_TYPE_NLOBBYTES:
						if (SanityManager.DEBUG)
							trace("parameter value is: "+paramBytes);
						ps.setBytes(i+1, paramBytes);
						break;
					case FdocaConstants.DRDA_TYPE_LOBCSBCS:
					case FdocaConstants.DRDA_TYPE_NLOBCSBCS:
						paramString = new String(paramBytes, stmt.ccsidSBCEncoding);
						if (SanityManager.DEBUG)
							trace("parameter value is: "+ paramString);
						ps.setString(i+1,paramString);
						break;
					case FdocaConstants.DRDA_TYPE_LOBCDBCS:
					case FdocaConstants.DRDA_TYPE_NLOBCDBCS:
						paramString = new String(paramBytes, stmt.ccsidDBCEncoding );
						if (SanityManager.DEBUG)
							trace("parameter value is: "+ paramString);
						ps.setString(i+1,paramString);
						break;
					case FdocaConstants.DRDA_TYPE_LOBCMIXED:
					case FdocaConstants.DRDA_TYPE_NLOBCMIXED:
						paramString = new String(paramBytes, stmt.ccsidMBCEncoding);
						if (SanityManager.DEBUG)
							trace("parameter value is: "+ paramString);
						ps.setString(i+1,paramString);
						break;
					default:
						invalidValue(drdaType);
				}
			     
			}
			catch (java.io.UnsupportedEncodingException e) {
				throw new SQLException (e.getMessage());
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
	 * @exception DRDAProtocolException, SQLException
	 */
	private int parseEXCSQLIMM() throws DRDAProtocolException,SQLException
	{
		int codePoint;
		reader.markCollection();
		String pkgnamcsn = null;
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
		drdaStmt.getStatement().clearWarnings();
		int updCount = drdaStmt.getStatement().executeUpdate(sqlStmt);
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
	 * @exception DRDAProtocolException, SQLException
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
					String pkgnamcsn = parsePKGNAMCSN();
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
	 * @exception DRDAProtocolException, SQLException
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
				codePoint = reader.readLengthAndCodePoint();
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
						if (SanityManager.DEBUG) 
							trace("sqlStmt = " + sqlStmt);
						break;
					default:
						invalidCodePoint(codePoint);
				}
			}
		} while (reader.isChainedWithSameID());

		// SQLSTT is required
		if (sqlStmt == null)
			missingCodePoint(CodePoint.SQLSTT);
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
	 * @return Count of updated rows.
	 * @exception DRDAProtocolException, SQLException
	 */
	private void parseEXCSQLSETobjects()
		throws DRDAProtocolException, SQLException
	{

		boolean gotSqlStt = false;
		boolean hadUnrecognizedStmt = false;

		String sqlStmt = null;
		int codePoint;
		DRDAStatement drdaStmt = database.getDefaultStatement();
		drdaStmt.initialize();

		do
		{
			correlationID = reader.readDssHeader();
			while (reader.moreDssData())
			{

				codePoint = reader.readLengthAndCodePoint();

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
						sqlStmt = parseEncodedString();
						if (sqlStmt != null)
						// then we have at least one SQL Statement.
							gotSqlStt = true;

						if (canIgnoreStmt(sqlStmt)) {
						// We _know_ Cloudscape doesn't recognize this
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
//							hadUnrecognizedStmt = true;
							break;
						}

						if (SanityManager.DEBUG) 
							trace("sqlStmt = " + sqlStmt);

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
							if (e.getSQLState().equals(SYNTAX_ERR))
								hadUnrecognizedStmt = true;
							else
							// something else; assume it's serious.
								throw e;
						}
						break;
					default:
						invalidCodePoint(codePoint);
				}
			}

		} while (reader.isChainedWithSameID());

		// SQLSTT is required.
		if (!gotSqlStt)
			missingCodePoint(CodePoint.SQLSTT);

		// Now that we've processed all SET statements (assuming no
		// severe exceptions), check for warnings and, if we had any,
		// note this in the SQLCARD reply object (but DON'T cause the
		// EXCSQLSET statement to fail).
		if (hadUnrecognizedStmt) {
			SQLWarning warn = new SQLWarning("One or more SET statements " +
				"not recognized.", "01000");
			throw warn;
		} // end if.

		return;
	}

	private boolean canIgnoreStmt(String stmt)
	{
		if (stmt.indexOf("SET CLIENT") != -1)
			return true;
		return false;
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
		writeRDBNAM(database.dbName);
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
	 * @exception throws DRDAProtocolException
	 */
	private String parsePKGNAMCSN() throws DRDAProtocolException
	{
		rdbnam = null;
		rdbcolid = null;
		pkgid = null;
		secnumber = 0;

		if (reader.getDdmLength() == CodePoint.PKGNAMCSN_LEN)
		{
			// This is a scalar object with the following fields
			rdbnam = reader.readString(CodePoint.RDBNAM_LEN);
			if (SanityManager.DEBUG) 
				trace("rdbnam = " + rdbnam);

			rdbcolid = reader.readString(CodePoint.RDBCOLID_LEN);
			if (SanityManager.DEBUG) 
				trace("rdbcolid = " + rdbcolid);

			pkgid = reader.readString(CodePoint.PKGID_LEN);
			if (SanityManager.DEBUG)
				trace("pkgid = " + pkgid);

			// we need to use the same UCS2 encoding, as this can be
			// bounced back to jcc (or keep the byte array)
			pkgcnstknStr = reader.readString(CodePoint.PKGCNSTKN_LEN);
			if (SanityManager.DEBUG) 
				trace("pkgcnstkn = " + pkgcnstknStr);

			secnumber = reader.readNetworkShort();
			if (SanityManager.DEBUG)
				trace("secnumber = " + secnumber);
		}
		else	// extended format
		{
			int length = reader.readNetworkShort();
			if (length < CodePoint.RDBNAM_LEN || length > CodePoint.MAX_NAME)
				badObjectLength(CodePoint.RDBNAM);
			rdbnam = reader.readString(length);
			if (SanityManager.DEBUG)
				trace("rdbnam = " + rdbnam);

			//RDBCOLID can be variable length in this format
			length = reader.readNetworkShort();
			rdbcolid = reader.readString(length);
			if (SanityManager.DEBUG) 
				trace("rdbcolid = " + rdbcolid);

			length = reader.readNetworkShort();
			if (length != CodePoint.PKGID_LEN)
				badObjectLength(CodePoint.PKGID);
			pkgid = reader.readString(CodePoint.PKGID_LEN);
			if (SanityManager.DEBUG) 
				trace("pkgid = " + pkgid);

			pkgcnstknStr = reader.readString(CodePoint.PKGCNSTKN_LEN);
			if (SanityManager.DEBUG) 
				trace("pkgcnstkn = " + pkgcnstknStr);

			secnumber = reader.readNetworkShort();
			if (SanityManager.DEBUG) 
				trace("secnumber = " + secnumber);
		}
		// Note: This string  is parsed by DRDAStatement.buildDB2CursorName()
		return rdbnam + " " + rdbcolid + " " + pkgid + " " + secnumber + " " + pkgcnstknStr;
	}

	/**
	 * Parse SQLSTT Dss
	 * @exception DRDAProtocolException
	 */
	private String parseSQLSTTDss() throws DRDAProtocolException
	{
		correlationID = reader.readDssHeader();
		int codePoint = reader.readLengthAndCodePoint();
		String strVal = parseEncodedString();
		if (SanityManager.DEBUG) 
			trace("SQL Statement = " + strVal);
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
		if (sqlamLevel < 7)
			return parseVCMorVCS();
		else
			return parseNOCMorNOCS();
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
		if (vcm_length > 0)
			strVal = parseCcsidMBC(vcm_length);
		int vcs_length = reader.readNetworkShort();
		if (vcs_length > 0)
		{
			if (strVal != null)
				agentError ("Both VCM and VCS have lengths > 0");
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
			if (strVal != null)
				agentError("Both CM and CS are non null");
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

		if (length == 0)
			return null;
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
			agentError("Attempt to decode mixed byte string without CCSID being set");
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
		String ccsidSBCEncoding = database.getCurrentStatement().ccsidSBCEncoding;
		if (length == 0)
			return null;
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
			agentError("Attempt to decode single byte string without CCSID being set");
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
	 * @exception DRDAProtocolException, SQLException
	 */
	private DRDAStatement parseCLSQRY() throws DRDAProtocolException, SQLException
	{
		String pkgnamcsn = null;
		reader.markCollection();
		long qryinsid = 0;
		boolean gotQryinsid = false;

		int codePoint = reader.getCodePoint();
		while (codePoint != -1)
		{
			switch (codePoint)
			{
				// optional
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
				default:
					invalidCodePoint(codePoint);
			}
			codePoint = reader.getCodePoint();
		}
		// check for required variables
		if (pkgnamcsn == null)
			missingCodePoint(CodePoint.PKGNAMCSN);
		if (sqlamLevel >= MGRLVL_7 && !gotQryinsid)
			missingCodePoint(CodePoint.QRYINSID);

		DRDAStatement stmt = database.getDRDAStatement(pkgnamcsn);
		if (stmt == null)
		{
			//XXX should really throw a SQL Exception here
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
			pkgnamcsn = null;
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
		return;

	}

	private void writeSQLCARDs(SQLException e, int updateCount)
									throws DRDAProtocolException
	{
		writeSQLCARDs(e, updateCount, false);
	}

	private void writeSQLCARDs(SQLException e, int updateCount, boolean sendSQLERRRM)
									throws DRDAProtocolException
	{

		int severity = CodePoint.SVRCOD_INFO;
		if (e == null)
		{
			writeSQLCARD(e,severity, updateCount, 0);
			return;
		}

		// instead of writing a chain of sql error or warning, we send the first one, this is
		// jcc/db2 limitation, see beetle 4629

		// If it is a real SQL Error write a SQLERRRM first
		severity = getExceptionSeverity(e);
		if (sendSQLERRRM || (severity > CodePoint.SVRCOD_ERROR))
		{
			writeSQLERRRM(severity);
		}
		writeSQLCARD(e,severity, updateCount, 0);
	}

	private int getSqlCode(int severity)
	{
		if (severity == CodePoint.SVRCOD_WARNING)		// warning
			return 100;		//CLI likes it
		else if (severity == CodePoint.SVRCOD_INFO)             
			return 0;
		else
			return -1;
	}

	private void writeSQLCARD(SQLException e,int severity, 
		int updateCount, long rowCount ) throws DRDAProtocolException
	{
		writer.createDssObject();
		writer.startDdm(CodePoint.SQLCARD);
		writeSQLCAGRP(e, getSqlCode(severity), updateCount, rowCount);
		writer.endDdmAndDss();

		// If we have a shutdown exception, restart the server.
		if (e != null) {
			String sqlState = e.getSQLState();
			if (sqlState.regionMatches(0,
			  SQLState.CLOUDSCAPE_SYSTEM_SHUTDOWN, 0, 5)) {
			// then we're here because of a shutdown exception;
			// "clean up" by restarting the server.
				try {
					server.startDB2j();
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
		writeSQLCAGRP(null, 0, 0, 0);
		writer.endDdmAndDss();
	}
	/**
	 * Write SQLERRRM
	 *
	 * Instance Variables
	 * 	SVRCOD - Severity Code - required
 	 *
	 * @param	severity	severity of error
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
	 * Translate from Cloudscape exception severity to SVRCOD
	 *
	 * @param e SQLException
	 */
	private int getExceptionSeverity (SQLException e)
	{
		int severity= CodePoint.SVRCOD_INFO;

		if (e == null)
			return severity;

		int ec = e.getErrorCode();
		switch (ec)
		{
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
				if (sqlState != null && sqlState.startsWith("01"))		// warning
					severity = CodePoint.SVRCOD_WARNING;
				else
					severity = CodePoint.SVRCOD_ERROR;
		}

		return severity;

	}
	/**
	 * Write SQLCAGRP
	 *
	 * SQLCAGRP : FDOCA EARLY GROUP
	 * SQL Communcations Area Group Description
	 *
	 * FORMAT FOR SQLAM <= 6
	 *   SQLCODE; DRDA TYPE I4; ENVLID 0x02; Length Override 4
	 *   SQLSTATE; DRDA TYPE FCS; ENVLID 0x30; Length Override 5
	 *   SQLERRPROC; DRDA TYPE FCS; ENVLID 0x30; Length Override 8
	 *   SQLCAXGRP; DRDA TYPE N-GDA; ENVLID 0x52; Length Override 0
	 *
	 * FORMAT FOR SQLAM >= 7
	 *   SQLCODE; DRDA TYPE I4; ENVLID 0x02; Length Override 4
	 *   SQLSTATE; DRDA TYPE FCS; ENVLID 0x30; Length Override 5
	 *   SQLERRPROC; DRDA TYPE FCS; ENVLID 0x30; Length Override 8
	 *   SQLCAXGRP; DRDA TYPE N-GDA; ENVLID 0x52; Length Override 0
	 *   SQLDIAGGRP; DRDA TYPE N-GDA; ENVLID 0x56; Length Override 0
	 *
	 * @param e 	SQLException encountered
	 * @param sqlcode	sqlcode
	 * 
	 * @exception DRDAProtocolException
	 */
	private void writeSQLCAGRP(SQLException e, int sqlcode, int updateCount,
			long rowCount) throws DRDAProtocolException
	{
		String sqlerrmc;
		String sqlState;
		String severeExceptionInfo = null;
		SQLException nextException = null;

		if (rowCount < 0 && updateCount < 0)
		{
			writer.writeByte(CodePoint.NULLDATA);
			return;
		}
		if (e == null)
		{
			sqlerrmc = null;
			sqlState = "     "; // set to 5 blanks when no error
		}
		else {
			if (sqlcode < 0)
			{
				if (SanityManager.DEBUG && server.debugOutput) 
				{
					trace("handle SQLException here");
					trace("reason is: "+e.getMessage());
					trace("SQLState is: "+e.getSQLState());
					trace("vendorCode is: "+e.getErrorCode());
					trace("nextException is: "+e.getNextException());
					server.consoleExceptionPrint(e);
					trace("wrapping SQLException into SQLCARD...");
				}
			}
			sqlState = e.getSQLState();
			if (e instanceof EmbedSQLException)
			{
				EmbedSQLException ce = (EmbedSQLException) e;
				boolean severeException = (ce.getErrorCode() >=  ExceptionSeverity.SESSION_SEVERITY);
				/* we need messageId to retrieve the localized message, just using
				 * sqlstate may not be easy, because it is the messageId that we
				 * used as key in the property file, for many sqlstates, they are
				 * just the first 5 characters of the corresponding messageId.
				 * We append messageId as the last element of sqlerrmc.  We can't
				 * send messageId in the place of sqlstate because jcc expects only
				 * 5 chars for sqlstate.
				 */
				sqlerrmc = "";
				byte[] sep = {20};	// use it as separator of sqlerrmc tokens
				byte[] errSep = {20, 20, 20};  // mark between exceptions
				String separator = new String(sep);
				String errSeparator = new String(errSep); 
				String dbname = null;
				if (database != null)
					dbname = database.dbName;
				
				do {
					String messageId = ce.getMessageId();
					
					// arguments are variable part of a message
					Object[] args = ce.getArguments();
					for (int i = 0; args != null &&  i < args.length; i++)
						sqlerrmc += args[i].toString() + separator;
					
					// Severe exceptions need to be logged in the error log
					// also log location and non-localized message will be
					// returned to client as appended arguments
					if (severeException)	
					{
						if (severeExceptionInfo == null)
							severeExceptionInfo = "";
						severeExceptionInfo += ce.getMessage() + separator;
						println2Log(dbname, session.drdaID, ce.getMessage());
					}
					sqlerrmc += messageId; 	//append MessageId
				
					e = ce.getNextException();
					if (e != null) {
						if (e instanceof EmbedSQLException) {
							ce = (EmbedSQLException)e;
							sqlerrmc += errSeparator + ce.getSQLState() + ":";
						}
						else {
							sqlerrmc += errSeparator + e.getSQLState() + ":";
							ce = null;
						}
					}
					else
						ce = null;
				} while (ce != null);
						
				if (severeExceptionInfo != null)
				{
					severeExceptionInfo += "(" + "server log:" +
						server.getErrorLogLocation() + ")" ;
					sqlerrmc += separator + severeExceptionInfo;
				}
			}
			else
				sqlerrmc = e.getMessage();
		}

		// Truncate the sqlerrmc to a length that the client can support.
		int maxlen = (sqlerrmc == null) ? -1 : Math.min(sqlerrmc.length(),
				appRequester.supportedMessageParamLength());
		if ((maxlen >= 0) && (sqlerrmc.length() > maxlen))
		// have to truncate so the client can handle it.
			sqlerrmc = sqlerrmc.substring(0, maxlen);

		//null indicator
		writer.writeByte(0);

		// SQLCODE
		writer.writeInt(sqlcode);

		// SQLSTATE
		writer.writeString(sqlState);

		// SQLERRPROC
		writer.writeString(server.prdId);

		// SQLCAXGRP
		writeSQLCAXGRP(updateCount, rowCount, sqlerrmc, nextException);
	}
	/**
	 * Write SQLCAXGRP
	 *
	 * SQLCAXGRP : EARLY FDOCA GROUP
	 * SQL Communications Area Exceptions Group Description
	 *
	 * FORMAT FOR SQLAM <= 6
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
	 * FORMAT FOR SQLAM >= 7
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
	 *   SQLRDBNAME; DRDA TYPE VCS; ENVLID 0x32; Length Override 255
	 *   SQLERRMSG_m; DRDA TYPE VCM; ENVLID 0x3E; Length Override 70
	 *   SQLERRMSG_s; DRDA TYPE VCS; ENVLID 0x32; Length Override 70
	 * @param e 	SQLException encountered
	 * @param sqlcode	sqlcode
	 * 
	 * @exception DRDAProtocolException
	 */
	private void writeSQLCAXGRP(int updateCount,  long rowCount, String sqlerrmc,
				SQLException nextException) throws DRDAProtocolException
	{
		writer.writeByte(0);		// SQLCAXGRP INDICATOR
		if (sqlamLevel < 7)
		{
			writeRDBNAM(database.dbName);
			writeSQLCAERRWARN(updateCount, rowCount);
		}
		else
		{
			// SQL ERRD1 - D6, WARN0-WARNA (35 bytes)
			writeSQLCAERRWARN(updateCount, rowCount);
			writer.writeShort(0);  //CCC on Win does not take RDBNAME
		}
		writeVCMorVCS(sqlerrmc);
		if (sqlamLevel >=7)
			writeSQLDIAGGRP(nextException);
	}

	/**
	 * Write the ERR and WARN part of the SQLCA
	 *
	 * @param updateCount
	 * @param rowCount 
	 */
	private void writeSQLCAERRWARN(int updateCount, long rowCount) 
	{
		// SQL ERRD1 - ERRD2 - row Count
		writer.writeInt((int)((rowCount>>>32))); 
		writer.writeInt((int)(rowCount & 0x0000000ffffffffL));
		// SQL ERRD3 - updateCount
		writer.writeInt(updateCount);
		// SQL ERRD4 - D6 (12 bytes)
		byte[] byteArray = new byte[1];
		byteArray[0] = 0;
		writer.writeScalarPaddedBytes(byteArray, 12, (byte) 0);
		// WARN0-WARNA (11 bytes)
		byteArray[0] = ' ';  //CLI tests need this to be blank
		writer.writeScalarPaddedBytes(byteArray, 11, (byte) ' ');
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
		writer.writeByte(CodePoint.NULLDATA);
		return;

		/**
		 * TODO: Enable the following code when JCC can support SQLDIAGGRP
		 * for all SQLCARD accesses. Commented out for now.
		 */
		/*
		if (nextException == null)
		{
			writer.writeByte(CodePoint.NULLDATA);
			return;
		}

		writeSQLDIAGSTT();
		writeSQLDIAGCI(nextException);
		writeSQLDIAGCN();
		*/
	}

	/*
	 * writeSQLDIAGSTT: Write NULLDATA for now
	 */
	private void writeSQLDIAGSTT()
		throws DRDAProtocolException
	{
		writer.writeByte(CodePoint.NULLDATA);
		return;
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
			int sqlCode = getSqlCode(getExceptionSeverity(se));
			String sqlerrmc = "";
			byte[] sep = {20};	// use it as separator of sqlerrmc tokens
			String separator = new String(sep);
				
			// arguments are variable part of a message
			Object[] args = ((EmbedSQLException)se).getArguments();
			for (int i = 0; args != null &&  i < args.length; i++)
				sqlerrmc += args[i].toString() + separator;

			String dbname = null;
			if (database != null)
				dbname = database.dbName;

			writeSQLDCROW(rowNum++, sqlCode, sqlState, dbname, sqlerrmc);

			se = se.getNextException();
		}
			
		writer.writeByte(CodePoint.NULLDATA);
		return;
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
		for (se = nextException; se != null; se = se.getNextException()) i++;
		writer.writeInt(i);
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


		writer.writeInt(0);						// REASON_CODE
		writer.writeInt(0);						// LINE_NUMBER
		writer.writeLong(rowNum);				// ROW_NUMBER

		byte[] byteArray = new byte[1];
		writer.writeScalarPaddedBytes(byteArray, 47, (byte) 0);

		writer.writeShort(0);					// CCC on Win does not take RDBNAME
		writer.writeByte(CodePoint.NULLDATA);	// MESSAGE_TOKENS
		writer.writeLDString(sqlerrmc);			// MESSAGE_TEXT

		writeVCMorVCS(null);					// COLUMN_NAME
		writeVCMorVCS(null);					// PARAMETER_NAME
		writeVCMorVCS(null);					// EXTENDED_NAME
		writer.writeByte(CodePoint.NULLDATA);	// SQLDCXGRP
	}

	/*
	 * writeSQLDIAGCN: Write NULLDATA for now
	 */
	private void writeSQLDIAGCN()
		throws DRDAProtocolException
	{
		writer.writeByte(CodePoint.NULLDATA);
		return;
	}

	/** 
	 * Write SQLDARD
	 *
	 * SQLDARD : FDOCA EARLY ARRAY
	 * SQL Descriptor Area Row Description with SQL Communications Area
	 *
	 * FORMAT FOR SQLAM <= 6
	 *   SQLCARD; ROW LID 0x64; ELEMENT TAKEN 0(all); REP FACTOR 1
	 *   SQLNUMROW; ROW LID 0x68; ELEMENT TAKEN 0(all); REP FACTOR 1
	 *   SQLDAROW; ROW LID 0x60; ELEMENT TAKEN 0(all); REP FACTOR 0(all)
	 *
	 * FORMAT FOR SQLAM >= 7
	 *   SQLCARD; ROW LID 0x64; ELEMENT TAKEN 0(all); REP FACTOR 1
	 *   SQLDHROW; ROW LID 0xE0; ELEMENT TAKEN 0(all); REP FACTOR 1
	 *   SQLNUMROW; ROW LID 0x68; ELEMENT TAKEN 0(all); REP FACTOR 1
	 *
	 * @param ps prepared statement
	 *
	 * @exception DRDAProtocolException, SQLException
	 */
	private void writeSQLDARD(DRDAStatement stmt, boolean rtnOutput, SQLException e) throws DRDAProtocolException, SQLException
	{
		PreparedStatement ps = stmt.getPreparedStatement();
		ResultSetMetaData rsmeta = ps.getMetaData();
		EmbedParameterSetMetaData pmeta = ((EmbedPreparedStatement) ps).getEmbedParameterSetMetaData();
		int numElems = 0;
		if (e == null || e instanceof SQLWarning)
		{
			if (rtnOutput && (rsmeta != null))
				numElems = rsmeta.getColumnCount();
			else if ((! rtnOutput) && (pmeta != null))
				numElems = pmeta.getParameterCount();
		}

		writer.createDssObject();

		// all went well we will just write a null SQLCA
		writer.startDdm(CodePoint.SQLDARD);
		writeSQLCAGRP(e, getSqlCode(getExceptionSeverity(e)), 0, 0);

		if (sqlamLevel >= MGRLVL_7)
			writeSQLDHROW (stmt);

		//SQLNUMROW
		if (SanityManager.DEBUG) 
			trace("num Elements = " + numElems);
		writer.writeShort(numElems);

		for (int i=0; i < numElems; i++)
			writeSQLDAGRP (rsmeta, pmeta, i, rtnOutput);
		writer.endDdmAndDss();

	}
	/**
	 * Write QRYDSC - Query Answer Set Description
	 *
	 * @param stmt			associated statement
	 * @param FDODSConly	simply the FDODSC, without the wrap
	 *
	 * Instance Variables
	 *   SQLDTAGRP - required
	 * 
	 * Only 84 columns can be sent in a single QRYDSC.  If there are more columns
	 * they must be sent in subsequent QRYDSC.
	 * If the QRYDSC will not fit into the current block, as many columns as can
	 * fit are sent and then the remaining are sent in the following blocks.
	 * @param stmt DRDAStatement we are working on
	 * @exception DRDAProtocolException, SQLException
	 */
	private void writeQRYDSC(DRDAStatement stmt, boolean FDODSConly)
		throws DRDAProtocolException, SQLException
	{

		ResultSet rs = null;
		ResultSetMetaData rsmeta = null;
		EmbedParameterSetMetaData pmeta = null;
		if (!stmt.needsToSendParamData)
			rs = stmt.getResultSet();
		if (rs == null)		// this is a CallableStatement, use parameter meta data
			pmeta = ((EmbedPreparedStatement) stmt.ps).getEmbedParameterSetMetaData();
		else
			rsmeta = rs.getMetaData();

	    int  numCols = (rsmeta != null ? rsmeta.getColumnCount() : pmeta.getParameterCount());
		int numGroups = 1;
		int colStart = 1;
		int colEnd = numCols;
		int blksize = stmt.getBlksize() > 0 ? stmt.getBlksize() : CodePoint.QRYBLKSZ_MAX;

		// check for remaining space in current query block
		// Need to mod with blksize so remaining doesn't go negative. 4868
		int remaining = blksize - (writer.getOffset()  % blksize) - (3 + 
				FdocaConstants.SQLCADTA_SQLDTARD_RLO_SIZE);


		// calcuate how may columns can be sent in the current query block
		int firstcols = remaining/FdocaConstants.SQLDTAGRP_COL_DSC_SIZE;

		// check if it doesn't all fit into the first block and 
		//	under FdocaConstants.MAX_VARS_IN_NGDA
		if (firstcols < numCols || numCols > FdocaConstants.MAX_VARS_IN_NGDA)
		{
			// we are limited to FdocaConstants.MAX_VARS_IN_NGDA
			if (firstcols > FdocaConstants.MAX_VARS_IN_NGDA)
			{
				if (SanityManager.DEBUG)
					SanityManager.ASSERT(numCols > FdocaConstants.MAX_VARS_IN_NGDA,
						"Number of columns " + numCols + 
						" is less than MAX_VARS_IN_NGDA");
				numGroups = numCols/FdocaConstants.MAX_VARS_IN_NGDA;
				// some left over
				if (FdocaConstants.MAX_VARS_IN_NGDA * numGroups < numCols)
					numGroups++;
				colEnd = FdocaConstants.MAX_VARS_IN_NGDA;
			}
			else
			{
				colEnd = firstcols;
				numGroups += (numCols-firstcols)/FdocaConstants.MAX_VARS_IN_NGDA;
				if (FdocaConstants.MAX_VARS_IN_NGDA * numGroups < numCols)
					numGroups++;
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
			colEnd = colEnd + FdocaConstants.MAX_VARS_IN_NGDA;
			if (colEnd > numCols)
				colEnd = numCols;
		}
		writer.writeBytes(FdocaConstants.SQLCADTA_SQLDTARD_RLO);
		if (! FDODSConly)
			writer.endDdmAndDss();
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
	 *	  LENGTH OVERRIDE
	 *	    For numeric/decimal types
	 *		  PRECISON
	 *		  SCALE
	 *	    otherwise
	 *		  LENGTH or DISPLAY_WIDTH
	 *
	 * @param stmt		drda statement
	 * @param rsmeta	resultset meta data
	 * @param pmeta		parameter meta data for CallableStatement
	 * @param colStart	starting column for group to send
	 * @param colEnd	end column to send
	 * @param first		is this the first group
	 *
	 * @exception DRDAProtocolException, SQLException
	 */
	private void writeSQLDTAGRP(DRDAStatement stmt, ResultSetMetaData rsmeta, 
								EmbedParameterSetMetaData pmeta,
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

						   

		boolean hasRs = (rsmeta != null);	//  if don't have result, then we look at parameter meta

		for (int i = colStart; i <= colEnd; i++)
		{
			boolean nullable = (hasRs ? (rsmeta.isNullable(i) == rsmeta.columnNullable) :
												 (pmeta.isNullable(i) == JDBC30Translation.PARAMETER_NULLABLE));
			int colType = (hasRs ? rsmeta.getColumnType(i) : pmeta.getParameterType(i));
			int[] outlen = {-1};
			int drdaType = (hasRs ?FdocaConstants.mapJdbcTypeToDrdaType(colType,nullable,outlen): 
								stmt.getParamDRDAType(i));

			boolean isDecimal = ((drdaType | 1) == FdocaConstants.DRDA_TYPE_NDECIMAL);
			int precision = 0, scale = 0;
			if (hasRs)
			{
				precision = rsmeta.getPrecision(i);
				scale = rsmeta.getScale(i);
				stmt.setRsDRDAType(i,drdaType);			
				stmt.setRsPrecision(i, precision);
				stmt.setRsScale(i,scale);
			}

			else if (isDecimal)
			{
				if (stmt.isOutputParam(i))
					((CallableStatement) stmt.ps).registerOutParameter(i,Types.DECIMAL);
				precision = pmeta.getPrecision(i);
				scale = pmeta.getScale(i);

			}

			if (SanityManager.DEBUG)
				trace("jdbcType=" + colType + "  \tdrdaType=" + Integer.toHexString(drdaType));

			// Length or precision and scale for decimal values.
			writer.writeByte(drdaType);
			if (isDecimal)
			{
				writer.writeByte(precision);
				writer.writeByte(scale);
			}
			else if (outlen[0] != -1)
				writer.writeShort(outlen[0]);
			else if (hasRs)
				writer.writeShort(rsmeta.getColumnDisplaySize(i));
			else
				writer.writeShort(stmt.getParamLen(i));
		}
	}



	//pass PreparedStatement here so we can send correct holdability on the wire for jdk1.3 and higher
	//For jdk1.3, we provide hold cursor support through reflection.
	private void writeSQLDHROW (DRDAStatement stmt) throws DRDAProtocolException,SQLException
	{
		ResultSet rs = null;
		EmbedStatement rsstmt;
		if (!stmt.needsToSendParamData)
			rs = stmt.getResultSet();
		
		if (rs != null)
			rsstmt = (EmbedStatement) rs.getStatement();
		else
			rsstmt = (EmbedStatement) stmt.getPreparedStatement();

		if (JVMInfo.JDK_ID < 2) //write null indicator for SQLDHROW because there is no holdability support prior to jdk1.3
		{
			writer.writeByte(CodePoint.NULLDATA);
			return;
		}

		writer.writeByte(0);		// SQLDHROW INDICATOR

		//SQLDHOLD
		writer.writeShort(rsstmt.getResultSetHoldability());
		
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
		writer.writeShort(0);	//CCC on Windows somehow does not take any dbname
		//SQLDSCHEMA
		writeVCMorVCS(null);

	}

	/**
	 * Write QRYDTA - Query Answer Set Data
	 *  Contains some or all of the answer set data resulting from a query
	 *  returns true if there is more data, false if we reached the end
	 * Instance Variables
	 *   Byte string
	 *
	 * @param stmt	DRDA statement we are processing
	 * @exception DRDAProtocolException, SQLException
	 */
	private void writeQRYDTA (DRDAStatement stmt) 
		throws DRDAProtocolException, SQLException
	{
		boolean getMoreData = true;
		boolean sentExtData = false;
		int startOffset = writer.getOffset();
		writer.createDssObject();

		if (SanityManager.DEBUG) 
			trace("Write QRYDTA");
		writer.startDdm(CodePoint.QRYDTA);
		
		while(getMoreData)
		{			
			sentExtData = false;
			getMoreData = writeFDODTA(stmt);

			if (stmt.getExtDtaObjects() != null)
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
				int endOffset = writer.getOffset();
				int rowsize = endOffset- startOffset;
				if ((stmt.getBlksize() - endOffset ) < rowsize)
					getMoreData = false;

			}

		}
		// If we sent extDta we will rely on
		// writeScalarStream to end the dss with the proper chaining.
		// otherwise end it here.
		if (! sentExtData)
			writer.endDdmAndDss();

		if( (!stmt.hasdata()) &&
			stmt.getQryclsimp() != CodePoint.QRYCLSIMP_NO &&
			stmt.getQryprctyp() != CodePoint.LMTBLKPRC)
			stmt.rsClose();
		
	}

	private boolean writeFDODTA (DRDAStatement stmt) 
		throws DRDAProtocolException, SQLException
	{
		boolean hasdata = false;
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
			numCols = stmt.getNumRsCols();					
			if (stmt.getScrollType() != 0)
				hasdata = positionCursor(stmt, rs);
			else
				hasdata = rs.next();

		}
		else	// it's for a CallableStatement
		{
			hasdata = stmt.hasOutputParams();
			numCols = stmt.getNumParams();
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
			if (sqlw == null)
				writeSQLCAGRP(null, 0, -1, -1);
			else
				writeSQLCAGRP(sqlw, sqlw.getErrorCode(), 1, -1);

			// if we were asked not to return data, mark QRYDTA null; do not
			// return yet, need to make rowCount right
			boolean noRetrieveRS = (rs != null && !stmt.getQryrtndta());
			if (noRetrieveRS)
				writer.writeByte(0xFF);  //QRYDTA null indicator: IS NULL
			else
				writer.writeByte(0);  //QRYDTA null indicator: not null

			for (int i = 1; i <= numCols; i++)
			{
				if (noRetrieveRS)
					break;

				int drdaType;
				int ndrdaType;
				int precision;
				int scale;

				Object val = null;
				boolean valNull;
				if (rs != null)
				{
					drdaType =   stmt.getRsDRDAType(i) & 0xff;
					precision = stmt.getRsPrecision(i);
					scale = stmt.getRsScale(i);
					ndrdaType = drdaType | 1;

					if (SanityManager.DEBUG)
						trace("!!drdaType = " + java.lang.Integer.toHexString(drdaType) + 
					 			 "precision = " + precision +" scale = " + scale);
					switch (ndrdaType)
					{
						case FdocaConstants.DRDA_TYPE_NLOBBYTES:
							writeFdocaVal(i,rs.getBlob(i),drdaType,
										  precision,scale,rs.wasNull(),stmt);
							break;
						case  FdocaConstants.DRDA_TYPE_NLOBCMIXED:
							writeFdocaVal(i,rs.getClob(i),drdaType,
										  precision,scale,rs.wasNull(),stmt);
							break;
						case FdocaConstants.DRDA_TYPE_NINTEGER:
							int ival = rs.getInt(i);
							valNull = rs.wasNull();
							if (SanityManager.DEBUG)
								trace("====== writing int: "+ ival + " is null: " + valNull);
							writeNullability(drdaType,valNull);
							if (! valNull)
								writer.writeInt(ival);
							break;
						case FdocaConstants.DRDA_TYPE_NSMALL:
							short sval = rs.getShort(i);
							valNull = rs.wasNull();
							if (SanityManager.DEBUG)
								trace("====== writing small: "+ sval + " is null: " + valNull);
							writeNullability(drdaType,valNull);
							if (! valNull)
								writer.writeShort(sval);
							break;
						case FdocaConstants.DRDA_TYPE_NINTEGER8:
							long lval = rs.getLong(i);
							valNull = rs.wasNull();
							if (SanityManager.DEBUG)
								trace("====== writing long: "+ lval + " is null: " + valNull);
							writeNullability(drdaType,valNull);
							if (! valNull)
								writer.writeLong(lval);
							break;
						case FdocaConstants.DRDA_TYPE_NFLOAT4:
							float fval = rs.getFloat(i);
							valNull = rs.wasNull();
							if (SanityManager.DEBUG)
								trace("====== writing float: "+ fval + " is null: " + valNull);
							writeNullability(drdaType,valNull);
							if (! valNull)
								writer.writeFloat(fval);
							break;
						case FdocaConstants.DRDA_TYPE_NFLOAT8:
							double dval = rs.getDouble(i);
							valNull = rs.wasNull();
							if (SanityManager.DEBUG)
								trace("====== writing double: "+ dval + " is null: " + valNull);
							writeNullability(drdaType,valNull);
							if (! valNull)
								writer.writeDouble(dval);
							break;
						case FdocaConstants.DRDA_TYPE_NCHAR:
						case FdocaConstants.DRDA_TYPE_NVARCHAR:
						case FdocaConstants.DRDA_TYPE_NVARMIX:
						case FdocaConstants.DRDA_TYPE_NLONG:
						case FdocaConstants.DRDA_TYPE_NLONGMIX:
							String valStr = rs.getString(i);
							if (SanityManager.DEBUG)
								trace("====== writing char/varchar/mix :"+ valStr + ":");
							writeFdocaVal(i, valStr, drdaType,
										  precision,scale,rs.wasNull(),stmt);
							break;
						default:
							writeFdocaVal(i, rs.getObject(i),drdaType,
										  precision,scale,rs.wasNull(),stmt);
					}
				}
				else
				{
					drdaType =   stmt.getParamDRDAType(i) & 0xff;
					precision = stmt.getParamPrecision(i);
					scale = stmt.getParamScale(i);
					ndrdaType = drdaType | 1;
					
					if (stmt.isOutputParam(i)) {
						if (SanityManager.DEBUG)
							trace("***getting Object "+i);
						val = ((CallableStatement) stmt.ps).getObject(i);
						valNull = (val == null);
						writeFdocaVal(i,val,drdaType,precision, scale, valNull,stmt);
					}
					else
						writeFdocaVal(i,null,drdaType,precision,scale,true,stmt);

				}
			}
			// does all this fit in one QRYDTA
			if (writer.getOffset() >= blksize)
			{
				splitQRYDTA(stmt, blksize);
				moreData = false;
			}

			if (rs == null)
				return moreData;

			//get the next row
			rowCount++;
			if (rowCount < stmt.getQryrowset())
			{
				hasdata = rs.next();
			}
			/*(1) scrollable we return at most a row set; OR (2) no retrieve data
			 */
			else if (stmt.getScrollType() != 0 || noRetrieveRS)
				moreData=false;

		} while (hasdata && rowCount < stmt.getQryrowset());

		// add rowCount to statement row count
		// for non scrollable cursors
		if (stmt.getScrollType() == 0)
			stmt.rowCount += rowCount;

		if (!hasdata)
		{
			doneData(stmt, rs);
			moreData=false;
		}

		if (stmt.getScrollType() == 0)
			stmt.setHasdata(hasdata);
		return moreData;
	}
	/**
	 * Split QRYDTA into blksize chunks
	 *
	 * @param stmt DRDA statment
	 * @param blksize size of query block
	 * 
	 * @exception SQLException, DRDAProtocolException
	 */
	private void splitQRYDTA(DRDAStatement stmt, int blksize) throws SQLException, 
			DRDAProtocolException
	{
		// make copy of extra data
		byte [] temp = writer.copyDataToEnd(blksize);
		// truncate to end of blocksize
		writer.setOffset(blksize);
		int remain = temp.length;
		int start = 0;
		int dataLen = blksize - 10; //DSS header + QRYDTA and length
		while (remain > 0)
		{
			// finish off query block and send
			writer.endDdmAndDss();
			finalizeChain();
			// read CNTQRY - not sure why JCC sends this
			correlationID = reader.readDssHeader();
			int codePoint = reader.readLengthAndCodePoint();
			DRDAStatement contstmt = parseCNTQRY();
			if (stmt != contstmt)
				agentError("continued query stmt not the same");
			// start a new query block for the next row
			writer.createDssObject();
			writer.startDdm(CodePoint.QRYDTA);
			// write out remaining data
			if (remain > blksize)
			{
				writer.writeBytes(temp, start, dataLen);
				remain -= dataLen; //DSS header + QRYDTA and length
				start += dataLen;
			}
			else
			{
				writer.writeBytes(temp, start, remain);
				remain = 0;
			}
		}
	}
	/**
	 * Done data
	 * Send SQLCARD for the end of the data
	 * 
	 * @param stmt DRDA statement
	 * @param rs Result set
	 * @exception DRDAProtocolException,SQLException
	 */
	private void doneData(DRDAStatement stmt, ResultSet rs) 
			throws DRDAProtocolException, SQLException
	{
		if (SanityManager.DEBUG) 
			trace("*****NO MORE DATA!!");
		int blksize = stmt.getBlksize() > 0 ? stmt.getBlksize() : CodePoint.QRYBLKSZ_MAX;
		if (rs != null)
		{
			if (stmt.getScrollType() != 0)
			{
				// for scrollable cursors - calculate the row count
				// since we may not have gone through each row
				rs.last();
				stmt.rowCount  = rs.getRow();
				//reposition after last
				rs.afterLast();
			}
			else  // non-scrollable cursor
			{
				// for QRYCLSIMP_YES or SERVER_CHOICE close the rs
				int qryclsimp = stmt.getQryclsimp();
				if (qryclsimp != CodePoint.QRYCLSIMP_NO &&
					//For LMTBLKPRC, we reach the end early, but cursor should
					//not be closed prematurely.
					stmt.getQryprctyp()
										!= CodePoint.LMTBLKPRC)
				{
					stmt.rsClose();
					stmt.rsSuspend();
				}
			 
			}
		}

		// For scrollable cursor's QRYSCRAFT, when we reach here, DRDA spec says sqlstate
		// is 00000, sqlcode is not mentioned.  But DB2 CLI code expects sqlcode to be 0.
		// We return sqlcode 0 in this case, as the DB2 server does.
		boolean isQRYSCRAFT = (stmt.getQryscrorn() == CodePoint.QRYSCRAFT);

		// sqlstate 02000 for end of data.
		// RESOLVE: Need statics for sqlcodes.
		writeSQLCAGRP(new SQLException("End of Data", (isQRYSCRAFT ? "00000" : "02000")),
							(isQRYSCRAFT ? 0 : 100), 0, stmt.rowCount);

		writer.writeByte(CodePoint.NULLDATA);
		// does all this fit in one QRYDTA
		if (writer.getOffset() >= blksize)
		{
			splitQRYDTA(stmt, blksize);
		}
	}
	/**
	 * Position cursor for insensitive scrollable cursors
	 *
	 * @param stmt	DRDA statement 
	 * @param rs	Result set
	 */
	private boolean positionCursor(DRDAStatement stmt, ResultSet rs) 
		throws SQLException, DRDAProtocolException
	{
		boolean retval = false;
		switch (stmt.getQryscrorn())
		{
			case CodePoint.QRYSCRREL:
				//we aren't on a row - go to first row
				//JCC seems to use relative 1 to get to the first row
				//JDBC doesn't allow you to use relative unless you are on
				//a valid row so we cheat here.
				if (rs.isBeforeFirst() || rs.isAfterLast())
					retval = rs.first();
				else
					retval = rs.relative((int)stmt.getQryrownbr());
				break;
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
	 * FORMAT FOR SQLAM <= 6
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
	 * FORMAT FOR SQLAM >= 7
	 *   SQLPRECISION; DRDA TYPE I2; ENVLID 0x04; Length Override 2
	 *   SQLSCALE; DRDA TYPE I2; ENVLID 0x04; Length Override 2
	 *   SQLLENGTH; DRDA TYPE I8; ENVLID 0x16; Length Override 8
	 *   SQLTYPE; DRDA TYPE I2; ENVLID 0x04; Length Override 2
	 *   SQLCCSID; DRDA TYPE FB; ENVLID 0x26; Length Override 2
	 *   SQLDOPTGRP; DRDA TYPE N-GDA; ENVLID 0xD2; Length Override 0
	 *
	 * @param rsmeta	resultset meta data
	 * @param pmeta		parameter meta data
	 * @param elemNum	column number we are returning (in case of result set), or,
	 *					parameter number (in case of parameter)
	 * @param rtnOutput	whether this is for a result set	
	 *
	 * @exception DRDAProtocolException, SQLException
	 */
	private void writeSQLDAGRP(ResultSetMetaData rsmeta, EmbedParameterSetMetaData pmeta, int elemNum, boolean rtnOutput)
		throws DRDAProtocolException, SQLException
	{
		//jdbc uses offset of 1

		int jdbcElemNum = elemNum +1;
		// length to be retreived as output parameter
		int[]  outlen = {-1};  

		int elemType = rtnOutput ? rsmeta.getColumnType(jdbcElemNum) : pmeta.getParameterType(jdbcElemNum);

		int precision = rtnOutput ? rsmeta.getPrecision(jdbcElemNum) : pmeta.getPrecision(jdbcElemNum);
		if (precision > FdocaConstants.NUMERIC_MAX_PRECISION)
			precision = FdocaConstants.NUMERIC_MAX_PRECISION;

		// 2-byte precision
		writer.writeShort(precision);
		// 2-byte scale
		int scale = (rtnOutput ? rsmeta.getScale(jdbcElemNum) : pmeta.getScale(jdbcElemNum));
		writer.writeShort(scale);

		boolean nullable = rtnOutput ? (rsmeta.isNullable(jdbcElemNum) ==
										ResultSetMetaData.columnNullable) : 
			(pmeta.isNullable(jdbcElemNum) == JDBC30Translation.PARAMETER_NULLABLE);
		
		int sqlType = SQLTypes.mapJdbcTypeToDB2SqlType(elemType,
													   nullable,
													   outlen);
		
		if (outlen[0] == -1) //some types not set
		{
			switch (elemType)
			{
				case Types.DECIMAL:
				case Types.NUMERIC:
					scale = rtnOutput ? rsmeta.getScale(jdbcElemNum) : pmeta.getScale(jdbcElemNum);
					outlen[0] = ((precision <<8) | (scale <<0));
					if (SanityManager.DEBUG) 
						trace("\n\nprecision =" +precision +
						  " scale =" + scale);
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
			case Types.BLOB:			//for CLI describe to be correct
			case Types.CLOB:
				outlen[0] = (rtnOutput ? rsmeta.getPrecision(jdbcElemNum) :
											pmeta.getPrecision(jdbcElemNum));
		}

		if (SanityManager.DEBUG) 
			trace("SQLDAGRP len =" + java.lang.Integer.toHexString(outlen[0]) + "for type:" + elemType);

	   // 8 or 4 byte sqllength
		if (sqlamLevel >= MGRLVL_6)
			writer.writeLong(outlen[0]);
		else
			writer.writeInt(outlen[0]);


		String typeName = rtnOutput ? rsmeta.getColumnTypeName(jdbcElemNum) :
										pmeta.getParameterTypeName(jdbcElemNum);
		if (SanityManager.DEBUG) 
			trace("jdbcType =" + typeName + "  sqlType =" + sqlType + "len =" +outlen[0]);

		writer.writeShort(sqlType);

		// CCSID
		// CCSID should be 0 for Binary Types.
		
		if (elemType == java.sql.Types.CHAR ||
			elemType == java.sql.Types.VARCHAR
			|| elemType == java.sql.Types.LONGVARCHAR
			|| elemType == java.sql.Types.CLOB)
			writer.writeScalar2Bytes(1208);
		else
			writer.writeScalar2Bytes(0);

		if (sqlamLevel < MGRLVL_7) 
		{

			//SQLName
			writeVCMorVCS(rtnOutput ? rsmeta.getColumnName(jdbcElemNum) : null);
			//SQLLabel
			writeVCMorVCS(null);
			//SQLComments
			writeVCMorVCS(null);

			if (sqlamLevel == MGRLVL_6)
				writeSQLUDTGRP(rsmeta, pmeta, jdbcElemNum, rtnOutput);
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
	 * @param s	string to write
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

  
	private void writeSQLUDTGRP(ResultSetMetaData rsmeta, EmbedParameterSetMetaData pmeta, int jdbcElemNum, boolean rtnOutput)
		throws DRDAProtocolException,SQLException
	{
		writer.writeByte(CodePoint.NULLDATA);

	}

	private void writeSQLDOPTGRP(ResultSetMetaData rsmeta, EmbedParameterSetMetaData pmeta, int jdbcElemNum, boolean rtnOutput)
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


	private void writeSQLDXGRP(ResultSetMetaData rsmeta, EmbedParameterSetMetaData pmeta, int jdbcElemNum, boolean rtnOutput)
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
		if (rtnOutput && rsmeta.isAutoIncrement(jdbcElemNum)) 
			writer.writeShort(2);
		else
			writer.writeShort(0);

		//   SQLXPARMMODE; DRDA TYPE I2; ENVLID 0x04; Length Override 2
		if (pmeta != null && !rtnOutput)
		{
			int mode = pmeta.getParameterMode(jdbcElemNum);
			if (mode ==  JDBC30Translation.PARAMETER_MODE_UNKNOWN)
			{
				// For old style callable statements. We assume in/out if it
				// is an output parameter.
				int type =  DRDAStatement.getOutputParameterTypeFromClassName(
																			  pmeta.getParameterClassName(jdbcElemNum));
				if (type != DRDAStatement.NOT_OUTPUT_PARAM)
					mode = JDBC30Translation.PARAMETER_MODE_IN_OUT;
			}
			writer.writeShort(mode);
		}
		else
		{
			writer.writeShort(0);
		}
	
		//   SQLXRDBNAM; DRDA TYPE VCS; ENVLID 0x32; Length Override 255
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
   *
   * @exception DRDAProtocolException  
   * @exception SQLException
   *
   * @see FdocaConstants
   */

	protected void writeFdocaVal(int index, Object val, int drdaType,
								 int precision, int scale, boolean valNull,
								 
								 DRDAStatement stmt) throws DRDAProtocolException, SQLException
	{
		writeNullability(drdaType,valNull);

		if (! valNull)
		{
			int ndrdaType = drdaType | 1;
			long valLength = 0;
			switch (ndrdaType)
			{
			case FdocaConstants.DRDA_TYPE_NSMALL:
 					// DB2 does not have a BOOLEAN java.sql.bit type,
					// so we need to send it as a small
 					if (val instanceof Boolean)
 					{
 						writer.writeShort(((Boolean) val).booleanValue());
 					}
 					else if (val instanceof Short)
 						writer.writeShort(((Short) val).shortValue());
 					else if (val instanceof Byte)
 						writer.writeShort(((Byte) val).byteValue());
					else
 						writer.writeShort(((Integer) val).shortValue());
					break;
				case  FdocaConstants.DRDA_TYPE_NINTEGER:
					writer.writeInt(((Integer) val).intValue());
					break;
				case FdocaConstants.DRDA_TYPE_NINTEGER8:
					writer.writeLong(((Long) val).longValue());
					break;
				case FdocaConstants.DRDA_TYPE_NFLOAT4:
					writer.writeFloat(((Float) val).floatValue());
					break;
				case FdocaConstants.DRDA_TYPE_NFLOAT8:
					writer.writeDouble(((Double) val).doubleValue());
					break;
				case FdocaConstants.DRDA_TYPE_NDECIMAL:
					if (precision == 0)
						precision = FdocaConstants.NUMERIC_DEFAULT_PRECISION;
					BigDecimal bd = (java.math.BigDecimal) val;
					writer.writeBigDecimal(bd,precision,scale);
					break;
				case FdocaConstants.DRDA_TYPE_NDATE:
					writer.writeString(((java.sql.Date) val).toString());
					break;
				case FdocaConstants.DRDA_TYPE_NTIME:
					writer.writeString(((java.sql.Time) val).toString());
					break;
				case FdocaConstants.DRDA_TYPE_NTIMESTAMP:
					// we need to send it in a slightly different format, and pad it
					// up to or truncate it into 26 chars
					String ts1 = ((java.sql.Timestamp) val).toString();
					String ts2 = ts1.replace(' ','-').replace(':','.');
					int tsLen = ts2.length();
					if (tsLen < 26)
					{
						for (int i = 0; i < 26-tsLen; i++)
							ts2 += "0";
					}
					else if (tsLen > 26)
						ts2 = ts2.substring(0,26);
					writer.writeString(ts2);
					break;
				case FdocaConstants.DRDA_TYPE_NCHAR:
					writer.writeString(((String) val).toString());
					break;
				case FdocaConstants.DRDA_TYPE_NVARCHAR:
				case FdocaConstants.DRDA_TYPE_NVARMIX:
				case FdocaConstants.DRDA_TYPE_NLONG:
				case FdocaConstants.DRDA_TYPE_NLONGMIX:
					//WriteLDString and generate warning if truncated
					// which will be picked up by checkWarning()
					writer.writeLDString(val.toString(), index);
					break;
				case FdocaConstants.DRDA_TYPE_NLOBBYTES:
					// do not send EXTDTA for lob of length 0, beetle 5967
					valLength = ((Blob) val).length();
					if (valLength > 0)
						stmt.addExtDtaObject(val, index);
					writer.writeExtendedLength (valLength);
					break;
				case FdocaConstants.DRDA_TYPE_NLOBCMIXED:
					valLength = ((Clob) val).length();
					// do not send EXTDTA for lob of length 0, beetle 5967
					if (valLength > 0) 
						stmt.addExtDtaObject(val,index);
					writer.writeExtendedLength(valLength);
					break;
				case  FdocaConstants.DRDA_TYPE_NFIXBYTE:
					writer.writeBytes((byte[]) val);
					break;
				case FdocaConstants.DRDA_TYPE_NVARBYTE:
				case FdocaConstants.DRDA_TYPE_NLONGVARBYTE:
						writer.writeLDBytes((byte[]) val, index);
					break;
				default:
					if (SanityManager.DEBUG) 
						trace("ndrdaType is: "+ndrdaType);
					writer.writeLDString(val.toString(), index);
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
				writer.writeByte(FdocaConstants.NULL_DATA);
			else
			{
				writer.writeByte(FdocaConstants.INDICATOR_NULLABLE);
			}
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
		currentRequiredLength = req.length;
		if (currentRequiredLength > required.length)
			required = new int[currentRequiredLength];
		for (int i = 0; i < req.length; i++)
			required[i] = req[i];
	}
	/**
	 * Remove codepoint from required list
 	 *
	 * @param codePoint - code point to be removed
	 */
	private void removeFromRequired(int codePoint)
	{
		for (int i = 0; i < currentRequiredLength; i++)
			if (required[i] == codePoint)
				required[i] = 0;

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
		if (firstMissing != 0)
			missingCodePoint(firstMissing);
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
	 * Object length not allowed
	 *
	 * @param codePoint  code point with bad object length
	 * @exception DRDAProtocolException
	 */
	private void badObjectLength(int codePoint) throws DRDAProtocolException
	{
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
	private void invalidCodePoint(int codePoint) throws DRDAProtocolException
	{
		throwSyntaxrm(CodePoint.SYNERRCD_INVALID_CP_FOR_CMD, codePoint);
	}
	/**
	 * Don't support this code point
	 *
	 * @param codePoint  code point value
	 * @exception DRDAProtocolException
	 */
	private void codePointNotSupported(int codePoint) throws DRDAProtocolException
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
	 * @param codepoint codepoint where the mismatch occurred
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
		if (session == null)
			return;
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
	 *	or thread as appropriate
	 */
	private void handleException(Exception e)
	{
		String dbname = null;
		if (database != null)
			dbname = database.dbName;
		
		// protocol error - write error message and close session
		if (e instanceof DRDAProtocolException)
		{
			try
			{
				DRDAProtocolException de = (DRDAProtocolException) e;
				println2Log(dbname,session.drdaID, 
							e.getMessage());
				server.consoleExceptionPrintTrace(e);
				reader.clearBuffer();
				de.write(writer);
				finalizeChain();
				closeSession();
				close();
			}
			catch (DRDAProtocolException ioe)
			{
				// There may be an IO exception in the write.
				println2Log(dbname,session.drdaID, 
							e.getMessage());
				server.consoleExceptionPrintTrace(ioe);
				closeSession();
				close();
			}
		}
		else
		{
			// something unexpected happened so let's kill this thread
			sendUnexpectedException(e);

			server.consoleExceptionPrintTrace(e);
			closeSession();
			close();
		}
	}
	

	/**
	 * Send unpexpected error to the client
	 * @param e Exception to be sent
	 */
	private void sendUnexpectedException(Exception e)
	{

		DRDAProtocolException unExpDe;
		String dbname = null;
		try {
			if (database != null)
				dbname = database.dbName;
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
		synchronized (closeSync)
		{
			return close;
		}
	}
	/**
	 * Get whether connections are logged
	 *
	 * @return true if connections are being logged; false otherwise
	 */
	private boolean getLogConnections()
	{
		synchronized(logConnectionsSync) {
			return logConnections;
		}
	}
	/**
	 * Get time slice value for length of time to work on a session
	 *
	 * @return time slice
	 */
	private long getTimeSlice()
	{
		synchronized(timeSliceSync) {
			return timeSlice;
		}
	}
	/**
	 * Send string to console
	 *
	 * @param value - value to print on console
	 */
	protected  void trace(String value)
	{
		if (SanityManager.DEBUG && server.debugOutput == true)
			server.consoleMessage(value);
	}

	/***
	 * Show runtime memory
	 *
	 ***/
	public static void showmem() {
		Runtime rt = null;
		Date d = null;
		rt = Runtime.getRuntime();
		rt.gc();
		d = new Date();
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
		StringBuffer str = new StringBuffer();
		str.append("0x");
		String val;
		int byteVal;
		for (int i = 0; i < buf.length; i++)
		{
			byteVal = buf[i] & 0xff;
			val = Integer.toHexString(byteVal);
			if (val.length() < 2)
				str.append("0");
			str.append(val);
		}
		return str.toString();
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
		if (typdefnam.equals("QTDSQL370"))
			return;
		if (typdefnam.equals("QTDSQL400"))
			return;
		if (typdefnam.equals("QTDSQLX86"))
			return;
		if (typdefnam.equals("QTDSQLASC"))
			return;
		if (typdefnam.equals("QTDSQLVAX"))
			return;
		if (typdefnam.equals("QTDSQLJVM"))
			return;
		invalidValue(CodePoint.TYPDEFNAM);
	}
	/**
	 * Check that the length is equal to the required length for this codepoint
	 *
	 * @param codepoint	codepoint we are checking
	 * @param reqlen	required length
	 * 
 	 * @exception DRDAProtocolException
	 */
	private void checkLength(int codepoint, int reqlen)
		throws DRDAProtocolException
	{
		long len = reader.getDdmLength();
		if (len < reqlen)
			badObjectLength(codepoint);
		else if (len > reqlen)
			tooBig(codepoint);
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
		if (val == CodePoint.TRUE)
			return true;
		else if (val == CodePoint.FALSE)
			return false;
		else
			invalidValue(codepoint);
		return false;	//to shut the compiler up
	}
	/**
	 * Add a database to the current session
	 *
	 */
	private void addDatabase(String dbname)
	{
		Database db = new Database(dbname);
		session.addDatabase(db);
		session.database = db;
		database = db;
	}
	/**
	 * Set the current database
	 * 
	 * @param codePoint 	codepoint we are processing
	 *
	 * @exception DRDAProtocolException
	 */
	private void setDatabase(int codePoint) throws DRDAProtocolException
	{
		String rdbnam = parseRDBNAM();
		// using same database so we are done
		if (database != null && database.dbName.equals(rdbnam))
			return;
		Database d = session.getDatabase(rdbnam);
		if (d == null)
			rdbnamMismatch(codePoint);
		else
			database = d;
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

	  ArrayList extdtaValues = stmt.getExtDtaObjects();
    // build the EXTDTA data, if necessary
    if (extdtaValues == null) 
		return;
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

		
		if (sqlamLevel >= MGRLVL_7)
			if (stmt.isExtDtaValueNullable(i))
				writeNullByte = true;
		
		Object o  = extdtaValues.get(i);
        if (o instanceof Blob) {
			Blob b = (Blob) o;
			long blobLength = b.length();
			writer.writeScalarStream (chainedWithSameCorrelator,
									  CodePoint.EXTDTA,
									  (int) Math.min(blobLength,
													 Integer.MAX_VALUE),
									  b.getBinaryStream (),
									  writeNullByte);
			
		}
		else if (o instanceof  Clob) {
			Clob c = (Clob) o;
			long[] outlen = {-1};
			ByteArrayInputStream  unicodeStream =
				convertClobToUnicodeStream(c, outlen);
			writer.writeScalarStream (chainedWithSameCorrelator,
									  CodePoint.EXTDTA,
									  (int) Math.min(outlen[0],
													 Integer.MAX_VALUE),		 
									  unicodeStream,
									  writeNullByte);
		}
		else if (o instanceof  byte[]) {
			byte[] b = (byte []) o;
			writer.writeScalarStream (chainedWithSameCorrelator,
									  CodePoint.EXTDTA,
									  (int) b.length,
									  new ByteArrayInputStream(b),
									  writeNullByte);
		}
	}
	// reset extdtaValues after sending
	stmt.clearExtDtaObjects();

  }



	private  java.io.ByteArrayInputStream  
		convertClobToUnicodeStream (
								Clob c,
								long outlen[]) throws SQLException
	{
		java.io.Reader characterStream = c.getCharacterStream();
		// Extract all the characters and write into a StringWriter.
		java.io.StringWriter sw = new java.io.StringWriter ();
		try {
			int read = characterStream.read();
			while (read != -1) {
				sw.write(read);
				read = characterStream.read();
			}
    }
		catch (java.io.IOException e) {
			throw new SQLException (e.getMessage());
		}

		// Extract the String from the StringWriter and extract the UTF-8 bytes.
		String string = sw.toString();

		byte[] utf8Bytes = null;
		try {
			utf8Bytes = string.getBytes("UTF-8");
		}
		catch (java.io.UnsupportedEncodingException e) {
			throw new SQLException (e.getMessage());
    }

		// Create a new ByteArrayInputStream based on the bytes.

		outlen[0]= utf8Bytes.length;
		return new java.io.ByteArrayInputStream (utf8Bytes);
		}

	/**
	 * Check SQLWarning and write SQLCARD as needed.
	 * 
	 * @param conn 		connection to check
	 * @param stmt 		statement to check
	 * @param rs 		result set to check
	 * @param updateCount 	update count to include in SQLCARD
	 * @param alwaysSend 	whether always send SQLCARD regardless of
	 *						the existance of warnings
	 * @param sendWarn 	whether to send any warnings or not. 
	 *
	 * @exception DRDAProtocolException
	 */
	private void checkWarning(Connection conn, Statement stmt, ResultSet rs,
						  int updateCount, boolean alwaysSend, boolean sendWarn)
		throws DRDAProtocolException, SQLException
	{
		// instead of writing a chain of sql warning, we send the first one, this is
		// jcc/db2 limitation, see beetle 4629
		SQLWarning warning = null;
		SQLWarning reportWarning = null;
		try
		{
			if (stmt != null)
			{
				warning = stmt.getWarnings();
				if (warning != null)
				{
					stmt.clearWarnings();
					reportWarning = warning;
				}
			}
			if (rs != null)
			{
				warning = rs.getWarnings();
				if (warning != null)
				{
					rs.clearWarnings();
					if (reportWarning == null)
						reportWarning = warning;
				}
			}
			if (conn != null)
			{
				warning = conn.getWarnings();
				if (warning != null)
				{
					conn.clearWarnings();
					if (reportWarning == null)
						reportWarning = warning;
				}
			}
			
		}
		catch (SQLException se)
		{
			if (SanityManager.DEBUG) 
				trace("got SQLException while trying to get warnings.");
		}


		if ((alwaysSend || reportWarning != null) && sendWarn)
			writeSQLCARDs(reportWarning, updateCount);
	}


	protected String buildRuntimeInfo(String indent, LocalizedResource localLangUtil )
	{
		String s ="";
		if (session == null)
			return s;
		else
			s += session.buildRuntimeInfo("", localLangUtil);
		s += "\n";
		return s;
	}

	/**
	 * Finalize the current DSS chain and send it if
	 * needed.
	 */
	private void finalizeChain() throws DRDAProtocolException {

		writer.finalizeChain(reader.getCurrChainState(), getOutputStream());
		return;

	}

}


