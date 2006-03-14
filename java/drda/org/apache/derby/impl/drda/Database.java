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
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.derby.iapi.jdbc.EngineConnection;
import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.iapi.tools.i18n.LocalizedResource;
import org.apache.derby.iapi.services.sanity.SanityManager;
/**
	Database stores information about the current database
	It is used so that a session may have more than one database
*/
class Database
{

	protected String dbName;			// database name 
	protected String shortDbName;       // database name without attributes
	String attrString="";               // attribute string
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

    /**
     * Connection to the database in the embedded engine.
     */
	private EngineConnection conn;
	DRDAStatement defaultStatement;    // default statement used 
													   // for execute imm
	private DRDAStatement currentStatement; // current statement we are working on
	private Hashtable stmtTable;		// Hash table for storing statements

	boolean forXA = false;

	// constructor
	/**
	 * Database constructor
	 * 
	 * @param dbName	database name
	 */
	Database (String dbName)
	{
		if (dbName != null)
		{
			int attrOffset = dbName.indexOf(';');
			if (attrOffset != -1)
			{
				this.attrString = dbName.substring(attrOffset,dbName.length());
				this.shortDbName = dbName.substring(0,attrOffset);
			}
			else
				this.shortDbName = dbName;
		}

		this.dbName = dbName;
		this.stmtTable = new Hashtable();
		initializeDefaultStatement();
	}


	private void initializeDefaultStatement()
	{
		this.defaultStatement = new DRDAStatement(this);
	}

	/**
	 * Set connection and create the SQL statement for the default statement
	 *
	 * @param conn Connection
	 * @exception SQLException
	 */
	final void setConnection(EngineConnection conn)
		throws SQLException
	{
		this.conn = conn;
		if(conn != null)
			defaultStatement.setStatement(conn);
	}
	/**
	 * Get the connection
	 *
	 * @return connection
	 */
	final EngineConnection getConnection()
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
	protected DRDAStatement getDefaultStatement(Pkgnamcsn pkgnamcsn) 
	{
		currentStatement = defaultStatement;
		currentStatement.setPkgnamcsn(pkgnamcsn);
		return currentStatement;
	}

	/**
	 * Get a new DRDA statement and store it in the stmtTable if stortStmt is 
	 * true. If possible recycle an existing statement. When the server gets a
	 * new statement with a previously used pkgnamcsn, it means that 
	 * client-side statement associated with this pkgnamcsn has been closed. In 
	 * this case, server can re-use the DRDAStatement by doing the following:  
	 * 1) Retrieve the old DRDAStatement associated with this pkgnamcsn and
	 * close it.
	 * 2) Reset the DRDAStatement state for re-use.
	 * 
	 * @param pkgnamcsn  Package name and section
	 * @return DRDAStatement  
	 */
	protected DRDAStatement newDRDAStatement(Pkgnamcsn pkgnamcsn)
	throws SQLException
	{
		DRDAStatement stmt = getDRDAStatement(pkgnamcsn);
		if (stmt != null) {
			stmt.close();
			stmt.reset();
		}
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
	protected DRDAStatement getDRDAStatement(Pkgnamcsn pkgnamcsn) {
		DRDAStatement newStmt =
			(DRDAStatement) stmtTable.get(pkgnamcsn.getStatementKey());
		if (newStmt != null) {
			currentStatement = newStmt;
			currentStatement.setCurrentDrdaResultSet(pkgnamcsn);
		}
		return newStmt;
	}

	/**
	 * Make a new connection using the database name and set 
	 * the connection in the database
	 * @param p Properties for connection attributes to pass to connect
	 */
	void makeConnection(Properties p) throws SQLException
	{
		p.put(Attribute.USERNAME_ATTR, userId);
                
                // take care of case of SECMEC_USRIDONL
                if(password != null) 
		    p.put(Attribute.PASSWORD_ATTR, password);
                
        // Contract between network server and embedded engine
        // is that any connection returned implements EngineConnection.
        EngineConnection conn = (EngineConnection)
            NetworkServerControlImpl.getDriver().connect(Attribute.PROTOCOL
							 + shortDbName + attrString, p);
		if(conn != null){
			conn.setAutoCommit(false);
		}
		setConnection(conn);
	}

	// Create string to pass to DataSource.setConnectionAttributes
	String appendAttrString(Properties p)
	{
		if (p == null)
			return null;
		
		Enumeration pKeys = p.propertyNames();
		while (pKeys.hasMoreElements()) 
		{
			String key = (String) pKeys.nextElement();
			attrString +=";" + key  +"=" + p.getProperty(key);
		}

		return attrString;
	}

	/**
	 * Store DRDA prepared statement
	 * @param  stmt	DRDA prepared statement
	 */
	protected void storeStatement(DRDAStatement stmt) throws SQLException
	{
		stmtTable.put(stmt.getPkgnamcsn().getStatementKey(), stmt);
	}

	protected void removeStatement(DRDAStatement stmt) throws SQLException
	{
		stmtTable.remove(stmt.getPkgnamcsn().getStatementKey());
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
				if (! forXA)
				{
					conn.rollback();
				}
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

	final void setDrdaID(String drdaID)
	{
		if (conn != null)
			conn.setDrdaID(drdaID);
	}

	/**
	 *  Set the internal isolation level to use for preparing statements.
	 *  Subsequent prepares will use this isoalation level
	 * @param level internal isolation level 
	 *
	 * @throws SQLException
	 * @see EngineConnection#setPrepareIsolation
	 * 
	 */
	final void setPrepareIsolation(int level) throws SQLException
	{
		conn.setPrepareIsolation(level);
	}

	final int getPrepareIsolation() throws SQLException
	{
		return conn.getPrepareIsolation();
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

    /**
     * This method resets the state of this Database object so that it can
     * be re-used.
     * Note: currently this method resets the variables related to security
     * mechanisms that have been investigated as needing a reset.  
     * TODO: Investigate what all variables in this class need to be 
     * reset when this database object is re-used on a connection pooling or
     * transaction pooling. see DRDAConnThread.parseACCSEC (CodePoint.RDBNAM)
     * where database object is re-used on a connection reset.
     */
    public void reset()
    {
        // Reset variables for connection re-use. Currently only takes care
        // of reset the variables that affect EUSRIDPWD security mechanism.  (DERBY-1080)
        decryptedUserId = null;
        decryptedPassword = null;
        publicKeyIn = null;
        publicKeyOut = null;
        userId = null;
        password = null;
        securityMechanism = 0;
    }
        
}














