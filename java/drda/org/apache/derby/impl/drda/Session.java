/*

   Derby - Class org.apache.derby.impl.drda.Session

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

package org.apache.derby.impl.drda;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Enumeration;
import java.util.Hashtable;
import org.apache.derby.iapi.tools.i18n.LocalizedResource;
import java.sql.SQLException;

/**
	Session stores information about the current session
	It is used so that a DRDAConnThread can work on any session.
*/
class Session
{

	// session states
	protected static final int INIT = 1;	// before exchange of server attributes
	protected static final int ATTEXC = 2;	// after exchange of server attributes
	protected static final int CLOSED = 3;	// session has ended

	// session types
	protected static final int DRDA_SESSION = 1;
	protected static final int CMD_SESSION = 2;
	
	// trace name prefix and suffix
	private static final String TRACENAME_PREFIX = "Server";
	private static final String TRACENAME_SUFFIX = ".trace";

	// session information
	protected Socket clientSocket;		// session socket
	protected int connNum;				// connection number
	protected InputStream sessionInput;	// session input stream
	protected OutputStream sessionOutput;	// session output stream
	protected String traceFileName;		// trace file name for session
	protected boolean traceOn;			// whether trace is currently on for the session
	protected int state;				// the current state of the session
	protected int sessionType;			// type of session - DRDA or NetworkServerControl command
	protected String drdaID;			// DRDA ID of the session
	protected DssTrace dssTrace;		// trace object associated with the session
	protected AppRequester appRequester;	// Application requester for this session
	protected Database database;		// current database
	protected int qryinsid;				// unique identifier for each query
	protected LocalizedResource langUtil;		// localization information for command session
										// client

	private	Hashtable	dbtable;		// Table of databases accessed in this session

	// constructor
	/**
	 * Session constructor
	 * 
	 * @param connNum		connection number
	 * @param clientSocket	communications socket for this session
	 * @param traceDirectory	location for trace files
	 * @param traceOn		whether to start tracing this connection
	 *
	 * @exception throws IOException
	 */
	protected Session (int connNum, Socket clientSocket, String traceDirectory,
			boolean traceOn) throws IOException
	{
		this.connNum = connNum;
		this.clientSocket = clientSocket;
		this.traceOn = traceOn;
		if (traceOn)
			dssTrace = new DssTrace(); 
		dbtable = new Hashtable();
		initialize(traceDirectory);
	}

	/**
	 * Close session - close connection sockets and set state to closed
	 * 
	 */
	protected void close() throws SQLException
	{
		
		try {
			sessionInput.close();
			sessionOutput.close();
			clientSocket.close();
			if (dbtable != null)
				for (Enumeration e = dbtable.elements() ; e.hasMoreElements() ;) 
				{
					((Database) e.nextElement()).close();
				}
			
		}catch (IOException e) {} // ignore IOException when we are shutting down
		finally {
			state = CLOSED;
			dbtable = null;
			database = null;
		}
	}

	/**
	 * initialize a server trace for the DRDA protocol
	 * 
	 * @param traceDirectory - directory for trace file
	 */
	protected void initTrace(String traceDirectory)
	{
		if (traceDirectory != null)
			traceFileName = traceDirectory + "/" + TRACENAME_PREFIX+
				connNum+ TRACENAME_SUFFIX;
		else
			traceFileName = TRACENAME_PREFIX +connNum+ TRACENAME_SUFFIX;
		traceOn = true;
		if (dssTrace == null)
			dssTrace = new DssTrace();
		dssTrace.startComBufferTrace (traceFileName);
	}

	/**
	 * Set tracing on
	 * 
	 * @param traceDirectory 	directory for trace files
	 */
	protected void setTraceOn(String traceDirectory)
	{
		if (traceOn)
			return;
		initTrace(traceDirectory);
	}
	/**
	 * Get whether tracing is on 
	 *
	 * @return true if tracing is on false otherwise
	 */
	protected boolean isTraceOn()
	{
		if (traceOn)
			return true;
		else
			return false;
	}

	/**
	 * Get connection number
	 *
	 * @return connection number
	 */
	protected int getConnNum()
	{
		return connNum;
	}

	/**
	 * Set tracing off
	 * 
	 */
	protected void setTraceOff()
	{
		if (! traceOn)
			return;
		traceOn = false;
		if (traceFileName != null)
			dssTrace.stopComBufferTrace();
	}
	/**
	 * Add database to session table
	 */
	protected void addDatabase(Database d)
	{
		dbtable.put(d.dbName, d);
	}

	/**
	 * Get database
	 */
	protected Database getDatabase(String dbName)
	{
		return (Database)dbtable.get(dbName);
	}


	/**
	 * Get session into initial state
	 *
	 * @param traceDirectory	- directory for trace files
	 */
	private void initialize(String traceDirectory)
		throws IOException
	{
		sessionInput = clientSocket.getInputStream();
		sessionOutput = clientSocket.getOutputStream();
		if (traceOn)
			initTrace(traceDirectory);
		state = INIT;
	}

	protected  String buildRuntimeInfo(String indent, LocalizedResource localLangUtil)
	{
		String s = "";
		s += indent +  localLangUtil.getTextMessage("DRDA_RuntimeInfoSessionNumber.I")
			+ connNum + "\n";
		if (database == null)
			return s;
		s += database.buildRuntimeInfo(indent,localLangUtil);
		s += "\n";
		return s;
	}

	private String getStateString(int s)
	{
		switch (s)
		{
			case INIT: 
				return "INIT";
			case ATTEXC:
				return "ATTEXC";
			case CLOSED:
				return "CLOSED";
			default:
				return "UNKNOWN_STATE";

		}
	}

	private String getTypeString(int t)
	{
		switch (t)
		{
			case DRDA_SESSION:
				return "DRDA_SESSION";
			case CMD_SESSION:
				return "CMD_SESSION";
			default:
				return "UNKNOWN_TYPE";
		}
					
	}
}
