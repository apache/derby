/*

   Derby - Class org.apache.derby.impl.drda.Database

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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Enumeration;
import org.apache.derby.iapi.tools.i18n.LocalizedResource;

import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.iapi.services.sanity.SanityManager;
/**
	Database stores information about the current database
	It is used so that a session may have more than one database
*/
class Database
{

	protected String dbName;			// database name 
	protected int securityMechanism;	// Security mechanism
	protected String userId;			// User Id
	protected String password;			// password
	protected String decryptedUserId;	// Decrypted User id
	protected String decryptedPassword;	// Decrypted password
	protected boolean rdbAllowUpdates = true; // Database allows updates -default is true		
	protected int	accessCount;		// Number of times we have tried to
										// set up access to this database (only 1 
										// allowed)
	protected byte[] publicKeyIn;		// Security token from app requester
	protected byte[] publicKeyOut;		// Security token sent to app requester
	protected byte[] crrtkn;			// Correlation token
	protected String typDefNam;			// Type definition name
	protected int byteOrder;			//deduced from typDefNam, save String comparisons
	protected int ccsidSBC;				// Single byte CCSID
	protected int ccsidDBC;				// Double byte CCSID
	protected int ccsidMBC;				// Mixed byte CCSID
	protected String ccsidSBCEncoding;	// Encoding for single byte code page
	protected String ccsidDBCEncoding;	// Encoding for double byte code page
	protected String ccsidMBCEncoding;	// Encoding for mixed byte code page
	protected boolean RDBUPDRM_sent = false;	//We have sent that an update
											// occurred in this transaction
	protected boolean sendTRGDFTRT = false; // Send package target default value

	private Connection conn;			// Connection to the database
	private DRDAStatement defaultStatement; // default statement used 
													   // for execute imm
	private DRDAStatement currentStatement; // current statement we are working on
	private Hashtable stmtTable;		// Hash table for storing statements


	// constructor
	/**
	 * Database constructor
	 * 
	 * @param dbName	database name
	 */
	protected Database (String dbName)
	{
		this.dbName = dbName;
		this.stmtTable = new Hashtable();
		this.defaultStatement = new DRDAStatement(this);
	}
	/**
	 * Set connection and create the SQL statement for the default statement
	 *
	 * @param conn Connection
	 * @exception SQLException
	 */
	protected void setConnection(Connection conn)
		throws SQLException
	{
		this.conn = conn;
		defaultStatement.setStatement(conn);
	}
	/**
	 * Get the connection
	 *
	 * @return connection
	 */
	protected Connection getConnection()
	{
		return conn;
	}
	/**
	 * Get current DRDA statement 
	 *
	 * @return DRDAStatement
	 * @exception SQLException
	 */
	protected DRDAStatement getCurrentStatement() 
	{
		return currentStatement;
	}
	/**
	 * Get default statement for use in EXCIMM
	 *
	 * @return DRDAStatement
	 */
	protected DRDAStatement getDefaultStatement() 
	{
		currentStatement = defaultStatement;
		return defaultStatement;
	}

	/**
	 * Get default statement for use in EXCIMM with specified pkgnamcsn
	 * The pkgnamcsn has the encoded isolation level
	 *
	 * @param pkgnamcsn package/ section # for statement
	 * @return DRDAStatement
	 */
	protected DRDAStatement getDefaultStatement(String pkgnamcsn) 
	{
		currentStatement = defaultStatement;
		currentStatement.setPkgnamcsn(pkgnamcsn);
		return currentStatement;
	}

	/**
	 * Get prepared statement based on pkgnamcsn
	 *
	 * @param pkgnamcsn - key to access statement
	 * @return prepared statement
	 */
	protected PreparedStatement getPreparedStatement(String pkgnamcsn) 
		throws SQLException
	{
		currentStatement = getDRDAStatement(pkgnamcsn);
		if (currentStatement == null)
			return null;
		return currentStatement.getPreparedStatement();
	}
	
	/**
	 * Get a new DRDA statement and store it in the stmtTable if stortStmt is true
	 * If possible recycle an existing statement
	 * If we are asking for one with the same name it means it
	 * was already closed.
	 * @param pkgnamcsn  Package name and section
	 * @return DRDAStatement  
	 */
	protected DRDAStatement newDRDAStatement(String pkgnamcsn)
	throws SQLException
	{
		DRDAStatement stmt = getDRDAStatement(pkgnamcsn);
		if (stmt != null)
			stmt.close();
		else
		{
			stmt = new DRDAStatement(this);
			stmt.setPkgnamcsn(pkgnamcsn);
			storeStatement(stmt);
		}
		return stmt;
	}

	/**
	 * Get DRDA statement based on pkgnamcsn
	 *
	 * @param pkgnamcsn - key to access statement
	 * @return DRDAStatement
	 */
	protected DRDAStatement getDRDAStatement(String pkgnamcsn) 
		throws SQLException
	{
		// Need to get the short version because resultSets have different
		// corelation ids.
		String key = getStmtKey(pkgnamcsn);
		DRDAStatement newStmt = null;

		// If our current statement doesn't match,retrieve the statement
		// and make it current if not null.
		if (currentStatement == null || 
			!key.equals(getStmtKey(currentStatement.getPkgnamcsn())));
			{
				newStmt  = (DRDAStatement) stmtTable.get(key);				
			}
			
			if (newStmt != null)	 // don't blow away currentStatement if we can't find this one
				currentStatement = newStmt;
			else
				return null;

		// Set the correct result set.
		currentStatement.setCurrentDrdaResultSet(pkgnamcsn);
		return currentStatement;
	}

	/**
	 * Get result set
	 *
	 * @param pkgnamcsn - key to access prepared statement
	 * @return result set
	 */
	protected ResultSet getResultSet(String pkgnamcsn) throws SQLException
	{
		return getDRDAStatement(pkgnamcsn).getResultSet();
	}
	/**
 	 * Set result set
	 *
	 * @param value
	 */
	protected void setResultSet(ResultSet value) throws SQLException
	{
		currentStatement.setResultSet(value);
	}
	/**
	 * Store DRDA prepared statement
	 * @param  stmt	DRDA prepared statement
	 */
	protected void storeStatement(DRDAStatement stmt) throws SQLException
	{
		stmtTable.put(getStmtKey(stmt.getPkgnamcsn()), stmt);
	}

	protected void removeStatement(DRDAStatement stmt) throws SQLException
	{
		stmtTable.remove(stmt.getPkgnamcsn());
		stmt.close();
	}
	
	/**
	 * Make statement the current statement
	 * @param stmt
	 *
	 */

	protected void setCurrentStatement(DRDAStatement stmt)
	{
		currentStatement = stmt;
	}

   
	protected void commit() throws SQLException
	{
		
		if (conn != null)
			conn.commit();
	}

	protected void rollback() throws SQLException
	{
		
		if (conn != null)
			conn.rollback();
	}
	/**
	  * Close the connection and clean up the statement table
	  * @throws SQLException on conn.close() error to be handled in DRDAConnThread.
	  */
	protected void close() throws SQLException
	{

		try {
			if (stmtTable != null)
			{
				for (Enumeration e = stmtTable.elements() ; e.hasMoreElements() ;) 
				{
					((DRDAStatement) e.nextElement()).close();
				}
			
			}
			if (defaultStatement != null)			
				defaultStatement.close();
			if ((conn != null) && !conn.isClosed())
			{
				conn.rollback();
				conn.close();
			}
		}
		finally {
			conn = null;
			currentStatement = null;
			defaultStatement = null;
			stmtTable=null;
		}
	}

	/**
	 *  Set the internal isolation level to use for preparing statements.
	 *  Subsequent prepares will use this isoalation level
	 * @param internal isolation level 
	 *
	 * @throws SQLException
	 * @see EmbedConnection#setPrepareIsolation
	 * 
	 */
	protected void setPrepareIsolation(int level) throws SQLException
	{
		((EmbedConnection) conn).setPrepareIsolation(level);
	}

	protected int getPrepareIsolation() throws SQLException
	{
		return ((EmbedConnection) conn).getPrepareIsolation();
	}

	protected String buildRuntimeInfo(String indent, LocalizedResource localLangUtil)
	{	
	  
		String s = indent + 
		localLangUtil.getTextMessage("DRDA_RuntimeInfoDatabase.I") +
			dbName + "\n" +  
		localLangUtil.getTextMessage("DRDA_RuntimeInfoUser.I")  +
			userId +  "\n" +
		localLangUtil.getTextMessage("DRDA_RuntimeInfoNumStatements.I") +
			stmtTable.size() + "\n";
		s += localLangUtil.getTextMessage("DRDA_RuntimeInfoPreparedStatementHeader.I");
		for (Enumeration e = stmtTable.elements() ; e.hasMoreElements() ;) 
				{
					s += ((DRDAStatement) e.nextElement()).toDebugString(indent
																		 +"\t") +"\n";
				}
		return s;
	}


	private String getStmtKey(String pkgnamcsn)
	{
		if (pkgnamcsn == null)
			return null;
		return  pkgnamcsn.substring(0,pkgnamcsn.length() - CodePoint.PKGCNSTKN_LEN);
	}
}














