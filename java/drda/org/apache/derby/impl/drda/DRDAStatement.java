/*

   Derby - Class org.apache.derby.impl.drda.DRDAStatement

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

import java.lang.reflect.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.Enumeration;

import org.apache.derby.iapi.reference.JDBC30Translation;
import org.apache.derby.iapi.services.info.JVMInfo;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.jdbc.EmbedResultSet;
import org.apache.derby.impl.jdbc.EmbedPreparedStatement;
import org.apache.derby.impl.jdbc.EmbedCallableStatement;
import org.apache.derby.impl.jdbc.EmbedParameterSetMetaData;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.impl.jdbc.EmbedSQLException;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.util.StringUtil;

import java.math.BigInteger;
import java.io.UnsupportedEncodingException;
import java.util.Hashtable;

/**
	DRDAStatement stores information about the statement being executed
*/
class DRDAStatement
{



	protected String typDefNam;		//TYPDEFNAM for this statement
	protected int byteOrder;		//deduced from typDefNam, save String comparisons
	protected int ccsidSBC;			//CCSID for single byte characters
	protected int ccsidDBC;			//CCSID for double byte characters
	protected int ccsidMBC;			//CCSID for mixed byte characters
	protected String ccsidSBCEncoding;	//Java encoding for CCSIDSBC
	protected String ccsidDBCEncoding;	//Java encoding for CCSIDDBC
	protected String ccsidMBCEncoding;	//Java encoding for CCSIDMBC

	protected Database database;		// Database this statement is created for
	private   String pkgnamcsn;         // Package name/section # and  consistency token
	protected String pkgcnstknStr;       // Consistency token for the first result set
 	protected String pkgid;              // package id
 	protected String sectionNumber;      // section number
	protected int withHoldCursor = -1;	 // hold cursor after commit attribute.
	protected int isolationLevel;         //JCC isolation level for Statement
	protected String cursorName;
	protected int scrollType;			// Sensitive or Insensitive scroll attribute
	protected int concurType;			// Concurency type
	protected long rowCount;			// Number of rows we have processed
	protected byte [] rslsetflg;		// Result Set Flags
	protected int maxrslcnt;			// Maximum Result set count
	protected PreparedStatement ps;     // Prepared statement
	protected boolean isCall;
	protected String procName;			// callable statement's method name
	private   int[] outputTypes;		// jdbc type for output parameter or NOT_OUTPUT_PARAM
	                                    // if not an output parameter.
	protected static int NOT_OUTPUT_PARAM = -100000;
	protected boolean outputExpected;	// expect output from a callable statement
	private Statement stmt;				// SQL statement


	private DRDAResultSet currentDrdaRs;  // Current ResultSet
	private Hashtable resultSetTable;     // Hashtable with resultsets            
	private ArrayList resultSetKeyList;  // ordered list of hash keys
	private int numResultSets = 0;  

	// State for parameter data
	protected  Vector cliParamDrdaTypes = new Vector();
	protected Vector cliParamLens = new Vector();
	protected ArrayList cliParamExtPositions = null;

	// Query options  sent on EXCSQLSTT
	// These the default for ResultSets created for this statement.
	// These can be overriden by OPNQRY or CNTQRY,
	protected int nbrrow;			// number of fetch or insert rows
	protected int qryrowset;			// Query row set
	protected int blksize;				// Query block size
	protected int maxblkext;			// Maximum number of extra blocks
	protected int outovropt;			// Output Override option
	protected int qryclsimp;            // Implicit Query Close Setting
	protected boolean qryrfrtbl;		// Query refresh answer set table
	private int qryprctyp = CodePoint.QRYBLKCTL_DEFAULT;   // Protocol type
	
	

	boolean needsToSendParamData = false;
	boolean explicitlyPrepared = false;    //Prepared with PRPSQLSTT (reusable) 

	// constructor
	/**
	 * DRDAStatement constructor
	 *
	 * @param database
	 * 
	 */
	protected DRDAStatement (Database database) 
	{
		this.database = database;
		setTypDefValues();
		this.currentDrdaRs = new DRDAResultSet();
	}

	/**
	 * set TypDef values
	 *
	 */
	protected void setTypDefValues()
	{
		// initialize statement values to current database values
		this.typDefNam = database.typDefNam;
		this.byteOrder = database.byteOrder;
		this.ccsidSBC = database.ccsidSBC;
		this.ccsidDBC = database.ccsidDBC;
		this.ccsidMBC = database.ccsidMBC;
		this.ccsidSBCEncoding = database.ccsidSBCEncoding;
		this.ccsidDBCEncoding = database.ccsidDBCEncoding;
		this.ccsidMBCEncoding = database.ccsidMBCEncoding;
	}
	/**
	 * Set database
	 *
	 * @param database
	 */
	protected void setDatabase(Database database)
	{
		this.database = database;
		setTypDefValues();
	}
	/**
	 * Set statement
	 *
	 * @param conn	Connection
	 * @exception SQLException
	 */
	protected void setStatement(Connection conn)
		throws SQLException
	{
		stmt = conn.createStatement();
		//beetle 3849 -  see  prepareStatement for details
		if (cursorName != null)
			stmt.setCursorName(cursorName);
	}
	/**
	 * Get the statement
	 *
	 * @return statement
	 * @exception SQLException
	 */
	protected Statement getStatement() 
		throws SQLException
	{
		return stmt;
	}

	/**Set resultSet defaults to match 
	 * the statement defaults sent on EXCSQLSTT
	 * This might be overridden on OPNQRY or CNTQRY
	 **/

	protected void setRsDefaultOptions(DRDAResultSet drs)
	{
		drs.nbrrow = nbrrow;
 		drs.qryrowset = qryrowset;
 		drs.blksize = blksize;
 		drs.maxblkext = maxblkext;
 		drs.outovropt = outovropt;
 		drs.rslsetflg = rslsetflg;
		drs.scrollType = scrollType;
		drs.concurType = concurType;
		drs.setQryprctyp(qryprctyp);
		drs.qryrowset = qryrowset;
	}

	/**
	 * Set result set options to default for statement
	 */
	protected void setRsDefaultOptions()
	{
		setRsDefaultOptions(currentDrdaRs);
	}

	/**
	 * Get the extData Objects
	 *
	 *  @return ArrayList with extdta
	 */
	protected ArrayList getExtDtaObjects()
	{
		return currentDrdaRs.getExtDtaObjects();
	}

	/**
	 * Set the extData Objects
	 *
	 *  @return ArrayList with extdta
	 */
	protected void  setExtDtaObjects(ArrayList a)
	{
		currentDrdaRs.setExtDtaObjects(a);
	}
	
   	/**
	 * Add extDtaObject
	 * @param o - object to  add
	 * @param jdbcIndex - jdbc index for parameter
	 */
	protected void  addExtDtaObject (Object o, int jdbcIndex )
	{
		currentDrdaRs.addExtDtaObject(o,jdbcIndex);
	}

	
	/**
	 * Clear externalized lob objects in current result set
	 */
	protected void  clearExtDtaObjects ()
	{
		currentDrdaRs.clearExtDtaObjects();
	}


	/*
	 * Is lob object nullable
	 * @param index - offset starting with 0
	 * @return true if object is nullable
	 */
	protected boolean isExtDtaValueNullable(int index)
	{
		return currentDrdaRs.isExtDtaValueNullable(index);
	}
	

	/**
	 * Set query options sent on OPNQRY
	 */
	protected void setOPNQRYOptions(int blksize, int qryblkctl,
								  int maxblkext, int outovropt,int qryrowset,int qryclsimpl)
	{
		currentDrdaRs.blksize = blksize;
		currentDrdaRs.setQryprctyp(qryblkctl);
		currentDrdaRs.maxblkext = maxblkext;
		currentDrdaRs.outovropt = outovropt;
		currentDrdaRs.qryrowset = qryrowset;
		currentDrdaRs.qryclsimp = qryclsimp;
	}

	/*
	 * Set query options sent on CNTQRY
	 */
	protected void setQueryOptions(int blksize, boolean qryrelscr, 
									long qryrownbr,
									boolean qryfrtbl,int nbrrow,int maxblkext,
									int qryscrorn, boolean qryrowsns,
									boolean qryblkrst,
									boolean qryrtndta,int qryrowset,
									int rtnextdta)
	{
		currentDrdaRs.blksize = blksize;
		currentDrdaRs.qryrelscr = qryrelscr;
		currentDrdaRs.qryrownbr = qryrownbr;
		currentDrdaRs.qryrfrtbl = qryrfrtbl;
		currentDrdaRs.nbrrow = nbrrow;
		currentDrdaRs.maxblkext = maxblkext;
		currentDrdaRs.qryscrorn = qryscrorn;
		currentDrdaRs.qryrowsns = qryrowsns;
		currentDrdaRs.qryblkrst = qryblkrst;
		currentDrdaRs.qryrtndta = qryrtndta;
		currentDrdaRs.qryrowset = qryrowset;
		currentDrdaRs.rtnextdta = rtnextdta;
	}



	protected void setQryprctyp(int qryprctyp)
	{
		this.qryprctyp = qryprctyp;
		currentDrdaRs.setQryprctyp(qryprctyp);
	}

	protected int  getQryprctyp()
		throws SQLException
	{
		return currentDrdaRs.getQryprctyp();
	}

	protected void setQryrownbr(long qryrownbr)
	{
		currentDrdaRs.qryrownbr = qryrownbr;
	}

	protected long  getQryrownbr()
	{
		return currentDrdaRs.qryrownbr;
	}


	protected int  getQryrowset()
	{
		return currentDrdaRs.qryrowset;
	}

	
	protected int getBlksize()
	{
		return currentDrdaRs.blksize;
	}

	protected void setQryrtndta(boolean qryrtndta)
	{
		currentDrdaRs.qryrtndta = qryrtndta;
	}

	protected boolean  getQryrtndta()
	{
		return currentDrdaRs.qryrtndta;
	}


	protected void setQryscrorn(int qryscrorn)
	{
		currentDrdaRs.qryscrorn = qryscrorn;
	}

	protected int  getQryscrorn()
	{
		return currentDrdaRs.qryscrorn;
	}

	protected void getQryclsimp(int value)
	{
		currentDrdaRs.qryclsimp = value;
	}

	protected int  getQryclsimp()
	{
		return currentDrdaRs.qryclsimp;
	}

	protected void setScrollType(int scrollType)
	{
		currentDrdaRs.scrollType = scrollType;
	}

	protected int  getScrollType()
	{
		return currentDrdaRs.scrollType;
	}


	protected void setConcurType(int scrollType)
	{
		currentDrdaRs.concurType = scrollType;
	}

	protected int  getConcurType()
	{
		return currentDrdaRs.concurType;
	}

	protected void 	setOutovr_drdaType(int[] outovr_drdaType) 
	{
	   currentDrdaRs.outovr_drdaType = outovr_drdaType;
	}


	protected int[] 	getOutovr_drdaType() 
	{
		return currentDrdaRs.outovr_drdaType;
	}
	
	protected boolean hasdata()
	{
		return currentDrdaRs.hasdata;
	}
	
	protected void  setHasdata(boolean hasdata)
	{
		currentDrdaRs.hasdata = hasdata;
	}

	/**
	 * Initialize for reuse
	 */
	protected void initialize() 
	{
		setTypDefValues();
	}


	protected PreparedStatement explicitPrepare(String sqlStmt) throws SQLException
	{
		explicitlyPrepared = true;
		return prepare(sqlStmt);
	}

	protected boolean wasExplicitlyPrepared()
	{
		return explicitlyPrepared;
	}

	/**
	 * Create a prepared statement
	 *
	 * @param sqlStmt - SQL statement
	 *
	 * @exception SQLException
	 */
	protected PreparedStatement prepare(String sqlStmt)   throws SQLException
	{
		// save current prepare iso level
		int saveIsolationLevel = -1;
		boolean isolationSet = false;
		if (pkgnamcsn !=null)
		{
			saveIsolationLevel = database.getPrepareIsolation();
			database.setPrepareIsolation(isolationLevel);
			isolationSet = true;
		}
		
		if (isCallableSQL(sqlStmt))
		{
			isCall = true;
			ps = database.getConnection().prepareCall(sqlStmt);
			setupCallableStatementParams((CallableStatement)ps);
			if (isolationSet)
				database.setPrepareIsolation(saveIsolationLevel);
			return ps;
		}
		parsePkgidToFindHoldability();
		if (withHoldCursor == JDBC30Translation.CLOSE_CURSORS_AT_COMMIT) {
			if (JVMInfo.JDK_ID == 2) {//need to use reflection for holdability for jdk 1.3
				//prepareStatement takes 4 parameters
				Class[] PREP_STMT_PARAM = { String.class, Integer.TYPE, Integer.TYPE, Integer.TYPE };
				Object[] PREP_STMT_ARG = { sqlStmt, new Integer(scrollType),
				new Integer(concurType), new Integer(JDBC30Translation.CLOSE_CURSORS_AT_COMMIT)};
				try {
					//create a prepared statement with close cursor at commit using reflection.
					Method sh = database.getConnection().getClass().getMethod("prepareStatement", PREP_STMT_PARAM);
					ps = (PreparedStatement) (sh.invoke(database.getConnection(), PREP_STMT_ARG));
				} catch (InvocationTargetException itex) {
					Throwable e = itex.getTargetException();
					//prepareStatement can only throw SQLExcepton
					if (e instanceof SQLException)
					{
						throw (SQLException) e;
					}
					else
						throw Util.javaException(e);
				}
				catch (Exception e) {
					// invoke can throw IllegalAccessException or 
					// IllegalArgumentException, but these should not 
					// occur from this code. Just in case we will throw it
					throw Util.javaException(e);
				}
			} else if (JVMInfo.JDK_ID >= 4) 
				ps = ((EmbedConnection)(database.getConnection())).prepareStatement(sqlStmt, scrollType, concurType, withHoldCursor);
			else //no holdability change support for jdk 12 and less
				ps = database.getConnection().prepareStatement(sqlStmt);
		} else if (scrollType != 0)
			ps = database.getConnection().prepareStatement(sqlStmt, scrollType, concurType);
		else
			ps = database.getConnection().prepareStatement(sqlStmt);
      
		// beetle 3849  -  Need to change the cursor name to what
		// JCC thinks it will be, since there is no way in the 
		// protocol to communicate the actual cursor name.  JCC keeps 
		// a mapping from the client cursor names to the DB2 style cursor names
		if (cursorName != null)//cursorName not null means we are dealing with dynamic pacakges
			ps.setCursorName(cursorName);
		if (isolationSet)
			database.setPrepareIsolation(saveIsolationLevel);
				return ps;
	}

	/**
	 * Get prepared statement
	 *
	 * @return prepared statement
	 */
	protected PreparedStatement getPreparedStatement() 
	{
		return ps;
	}


	/**
	 * Executes the prepared statement and populates the resultSetTable.
	 * Access to the various resultSets is then possible by using
	 * setCurrentDrdaResultSet(String pkgnamcsn)  to set the current
	 * resultSet and then calling getResultSet() or the other access 
	 * methods to get resultset data.
	 *
	 * @ return true if the execution has resultSets
	 */
	protected boolean execute() throws SQLException
	{
		boolean hasResultSet = ps.execute();

		// java.sql.Statement says any result sets that are opened
		// when the statement is re-executed must be closed; this
		// is handled by the call to "ps.execute()" above--but we
		// also have to reset our 'numResultSets' counter, since
		// all previously opened result sets are now invalid.
		numResultSets = 0;

		ResultSet rs = null;
		boolean isCallable = (ps instanceof java.sql.CallableStatement);
		if (isCallable)
			needsToSendParamData = true;

		do {
			rs = ps.getResultSet();
			if (rs !=null)
			{
				addResultSet(rs);
				hasResultSet = true;
			}
			// For normal selects we are done, but procedures might
			// have more resultSets
		}while (isCallable && ((EmbedPreparedStatement) ps).getMoreResults(JDBC30Translation.KEEP_CURRENT_RESULT));

		return hasResultSet;

	}
	
	/**
	 * clear out type data for parameters.
	 * Unfortunately we currently overload the resultSet type info
	 * rsDRDATypes et al with parameter info.
	 * RESOLVE: Need to separate this
	 */
   protected void finishParams()
	{
		needsToSendParamData = false;
	}

	/**
	 * Set the pkgid sec num for this statement and the 
	 * consistency token that will be used for the first resultSet.
	 * For dyamic packages The package name is encoded as follows
	 * SYS(S/L)(H/N)xyy 
	 * where 'S' represents Small package and 'L' large 
	 *                      (ignored by cloudscape) 
	 * Where 'H' represents WITH HOLD, and 'N' represents NO WITH HOLD. 
	 *                      (May be overridden by SQLATTR for WITH
	 *                       HOLD")
	 *
	 * Where 'www' is the package iteration (ignored by cloudcape)
	 * Where 'x' is the isolation level: 0=NC, 1=UR, 2=CS, 3=RS, 4=RR 
	 * Where 'yy' is the package iteration 00 through FF 
	 * Where 'zz' is unique for each platform
	 * Happilly, these values correspond precisely to the internal cloudscape
	 * isolation levels  in ExecutionContext.java
	 * x   Isolation Level                                           
	 * --  ---------------------
	 * 0   NC  (java.sql.Connection.TRANSACTION_NONE)
	 * 1   UR  (java.sql.Connection.TRANACTION_READ_UNCOMMITTED)
	 * 2   CS  (java.sql.Connection.TRANSACTION_READ_COMMITTED)
	 * 3   RS  (java.sql.Connection.TRANSACTION_REPEATABLE_READ)
	 * 4   RR  (java.sql.Connection.TRANSACTION_SERIALIZABLE)
	 * 
	 * static packages have preset isolation levels 
	 * (see getStaticPackageIsolation)
	 * @param pkgnamcsn  package id section number and token from the client
	 */
	protected void setPkgnamcsn(String pkgnamcsn)
	{
		this.pkgnamcsn =  pkgnamcsn;
		// Store the consistency string for the first ResultSet.
		// this will be used to calculate consistency strings for the 
		// other result sets.
		StringTokenizer st = new StringTokenizer(pkgnamcsn);
		st.nextToken();   // rdbnam (disregard)
		st.nextToken();   // rdbcolid (disregard)
		pkgid = st.nextToken();   // pkgid

		if (isDynamicPkgid(pkgid))
		{
			isolationLevel = Integer.parseInt(pkgid.substring(5,6));
			
			
			/*
			 *   generate DB2-style cursorname
			 *   example value : SQL_CURSN200C1
			 *   where 
			 *      SQL_CUR is db2 cursor name prefix;
			 *      S - Small package , L -Large package
			 *      N - normal cursor, H - hold cursor 
			 *      200 - package id as sent by jcc 
			 *      C - tack-on code for cursors
			 *      1 - section number sent by jcc		 
			 */
			
			

			// cursor name
			// trim the SYS off the pkgid so it wont' be in the cursor name
			String shortPkgid = pkgid.substring(pkgid.length() -5 , pkgid.length());
			sectionNumber = st.nextToken() ;
			this.cursorName = "SQL_CUR" +  shortPkgid + "C" + sectionNumber ;
		}
		else // static package
		{
			isolationLevel = getStaticPackageIsolation(pkgid);
		}

		this.pkgcnstknStr = st.nextToken();

	}


	/**
	 * get the isolation level for a static package.
	 * @param pkgid - Package identifier string (e.g. SYSSTAT)
	 * @return isolation
	 */
	private int getStaticPackageIsolation(String pkgid)
	{
		// SYSSTAT is used for metadata. and is the only static package used
		// for JCC. Other static packages will need to be supported for 
		// CCC. Maybe a static hash table would then be in order.
		if (pkgid.equals("SYSSTAT"))
			return ExecutionContext.READ_UNCOMMITTED_ISOLATION_LEVEL;
		else
			return ExecutionContext.UNSPECIFIED_ISOLATION_LEVEL;
	}

	/**
	 * Get pkgnamcsn
	 *
	 * @return pkgnamcsn
	 */
	protected String getPkgnamcsn() 
	{
		return pkgnamcsn;

	}
	/**
	 * Get result set
	 *
	 * @return result set
	 */
	protected ResultSet getResultSet() 
	{
		return currentDrdaRs.getResultSet();
	}

	
	/** 
	 * Just get the resultset. Don't set it to current
	 * Assumes resultSet rsnum exists.
	 *
	 * @param rsnum - resultSetNumber starting with 0
	 * @return  The result set in the order it was retrieved
	 *         
	 *          with getMoreResults()
	 **/
	private  ResultSet getResultSet(int rsNum)  
	{
		if (rsNum == 0)
			return currentDrdaRs.getResultSet();
		else
		{
			String key = (String) resultSetKeyList.get(rsNum);
			return ((DRDAResultSet) (resultSetTable.get( key))).getResultSet();
		}
	}

	/**
 	 * Set result set
	 *
	 * @param value
	 */
	protected void setResultSet(ResultSet value) throws SQLException
	{
		if (currentDrdaRs.getResultSet() == null)
			numResultSets = 1;
		currentDrdaRs.setResultSet(value);
		setRsDefaultOptions(currentDrdaRs);
	}

	/**
 	 * Set currentDrdaResultSet 
	 *
	 * @param rsNum   The result set number starting with 0
	 *                 
	 */
	protected void setCurrentDrdaResultSet(int rsNum)
	{
		String consistToken = getResultSetPkgcnstknStr(rsNum);
		if (currentDrdaRs.pkgcnstknStr == consistToken)
			return;
		currentDrdaRs = getDrdaResultSet(consistToken);

	}

	/**
 	 * Set currentDrdaResultSet 
	 *
	 * @String pkgnamcsn  The pkgid section number and unique resultset
	 *                    consistency token
	 *                 
	 */
	protected void setCurrentDrdaResultSet(String pkgnamcsn)
	{
		String consistToken = extractPkgcnstknStr(pkgnamcsn);
		DRDAResultSet newDrdaRs = getDrdaResultSet(consistToken);
		if (newDrdaRs != null)
			currentDrdaRs = newDrdaRs;
	}


	/*
	 * get DRDAResultSet by consistency token
	 *
	 */
	private DRDAResultSet getDrdaResultSet(String consistToken)
	{
		if ( resultSetTable   == null || 
			 (currentDrdaRs != null &&
			  currentDrdaRs.pkgcnstknStr == consistToken ))
		{
			return currentDrdaRs;
		}
		else
		{
			return (DRDAResultSet) (resultSetTable.get(consistToken));
		}
	}
	

	/*
	 * get DRDAResultSet by result set number
	 *
	 */
	private DRDAResultSet getDrdaResultSet(int rsNum)
	{
		String consistToken = getResultSetPkgcnstknStr(rsNum);
		return getDrdaResultSet(consistToken);
	}


	/*
	 *  get consistency token from pkgnamcsn
	 */
	private String extractPkgcnstknStr(String pkgnamcsn)
	{
		StringTokenizer st = new StringTokenizer(pkgnamcsn);
		st.nextToken();   // rdbnam (disregard)
		st.nextToken();   // rdbcolid (disregard)
		pkgid = st.nextToken();           // pkgid
		sectionNumber = st.nextToken() ;  // secno
		return st.nextToken();
	}

	/** Add a new resultSet to this statement.
	 * Set as the current result set if  there is not an 
	 * existing current resultset.
	 * @param value - ResultSet to add
	 * @return    Consistency token  for this resultSet
	 *            For a single resultSet that is the same as the statement's 
	 *            For multiple resultSets just the consistency token is changed 
	 */
	protected String  addResultSet(ResultSet value) throws SQLException
	{

		DRDAResultSet newDrdaRs = null;

		int rsNum = numResultSets;
		String newRsPkgcnstknStr = calculateResultSetPkgcnstknStr(rsNum);

		if (rsNum == 0)
			newDrdaRs = currentDrdaRs;

		else
		{
			newDrdaRs = new DRDAResultSet();

			// Multiple resultSets we neeed to setup the hash table
			if (resultSetTable == null)
			{
				// If hashtable doesn't exist, create it and store resultSet 0
				// before we store our new resultSet.
				// For just a single resultSet we don't ever create the Hashtable.
				resultSetTable = new Hashtable();
				resultSetTable.put(pkgcnstknStr,currentDrdaRs);
				resultSetKeyList = new ArrayList();
				resultSetKeyList.add(0,pkgcnstknStr);
			}

			resultSetTable.put(newRsPkgcnstknStr,newDrdaRs);
			resultSetKeyList.add(rsNum, newRsPkgcnstknStr);
		}

		newDrdaRs.setResultSet(value);
		newDrdaRs.setPkgcnstknStr(newRsPkgcnstknStr);
		setRsDefaultOptions(newDrdaRs);
		newDrdaRs.suspend();
		numResultSets++;
		return newRsPkgcnstknStr;
	}

	/**
	 *
	 * @return 	number of result sets
	 */
	protected int getNumResultSets()
	{
		return numResultSets;
	}
	
	
	/**
	 * @param rsNum result set starting with 0
	 * @return  consistency token (key) for the result set	 
	 */
	protected String getResultSetPkgcnstknStr(int rsNum)
	{
		if (rsNum == 0)
			return pkgcnstknStr;
		else 
			return (String) resultSetKeyList.get(rsNum);			   
	}


	/** 
	 * Set ResultSet DRDA DataTypes
	 * @param drddaTypes for columns.
	 **/
	protected void setRsDRDATypes(int [] value)
	{
		currentDrdaRs.setRsDRDATypes(value);
	}

	/**
	 *@return ResultSet DRDA DataTypes
	 **/

	protected int[] getRsDRDATypes()
	{
		return currentDrdaRs.getRsDRDATypes();

	}


	/** 
	 * Set ResultSet DRDA DataTypes Lengths
	 * @param drddaTypes for columns.
	 **/
	protected void setRsLens(int [] value)
	{
		currentDrdaRs.rsLens = value;

	}

	/**
	 *@return ResultSet DRDA DataTypes Lengths
	 **/

	protected int[] getRsLens()
	{
		return currentDrdaRs.rsLens;
	}

	/**
	 *  Close the current resultSet
	 */
	protected void rsClose() throws SQLException
	{
		if (currentDrdaRs.getResultSet() == null) 
			return;

		currentDrdaRs.close();
		needsToSendParamData = false;		
		numResultSets--;
	}

	/**
	 * Explicitly close the result set by CLSQRY
	 * needed to check for double close.
	 */
	protected void CLSQRY()
	{
		currentDrdaRs.CLSQRY();
	}

	/* 
	 * @return whether CLSQRY has been called on the
	 *         current result set.
	 */
	protected boolean wasExplicitlyClosed()
	{
		return currentDrdaRs.wasExplicitlyClosed();
	}

	/** Clean up statements and resultSet
	 * 
	 */
	protected void close()  throws SQLException
	{
		
		if (ps != null)
			ps.close();
		if (stmt != null)
			stmt.close();
		rsClose();
		resultSetTable = null;
		resultSetKeyList = null;
		numResultSets = 0;
		ps = null;
		stmt = null;
		scrollType = 0;
		concurType = 0;
		withHoldCursor = -1;
		rowCount = 0;
		rslsetflg = null;
		maxrslcnt = 0;
		procName = null;
		outputTypes = null;
		outputExpected = false;
		isCall = false;
		explicitlyPrepared = false;
		cliParamDrdaTypes = null;
		cliParamLens = null;
		cliParamExtPositions = null;

	}	

	/**
	 * is Statement closed
	 * @return whether the statement is closed
	 */
	protected boolean rsIsClosed()
	{
		return currentDrdaRs.isClosed();
	}
	
	/**
	 * Set state to SUSPENDED (result set is opened)
	 */
	protected void rsSuspend()
	{
		currentDrdaRs.suspend();
	}


	/**
	 * set resultset/out parameter precision
	 *
	 * @param index - starting with 1
	 * @param precision
	 */
	protected void setRsPrecision(int index, int precision)
	{
		currentDrdaRs.setRsPrecision(index,precision);
	}

	/**
	 * get resultset /out paramter precision
	 * @param index -starting with 1
	 * @return precision of column
	 */
	protected int getRsPrecision(int index)
	{
		return currentDrdaRs.getRsPrecision(index);
	}

	/**
	 * set resultset/out parameter scale
	 *
	 * @param index - starting with 1
	 * @param scale
	 */
	protected void setRsScale(int index, int scale)
	{
		currentDrdaRs.setRsScale(index, scale);
	}

	/**
	 * get resultset /out paramter scale
	 * @param index -starting with 1
	 * @return scale of column
	 */
	protected int  getRsScale(int index)
	{
		return currentDrdaRs.getRsScale(index);
	}
	

	/**
	 * set result  DRDAType
	 *
	 * @param index - starting with 1
	 * @param type
	 */
	protected  void setRsDRDAType(int index, int type)
	{
		currentDrdaRs.setRsDRDAType(index,type);
		
	}

	
	/**
	 * get parameter DRDAType
	 *
	 * @param index - starting with 1
	 * @return  DRDA Type of column
	 */
	protected int getParamDRDAType(int index)
	{
		
		return ((Byte)cliParamDrdaTypes.get(index -1)).intValue();
	}


	/**
	 * set param  DRDAType
	 *
	 * @param index - starting with 1
	 * @param type
	 */
	protected  void setParamDRDAType(int index, byte type)
	{
		cliParamDrdaTypes.addElement(new Byte(type));
		
	}
	/**
	 * returns drda length of parameter as sent by client.
	 * @param index
	 * @return data length

	 */

	protected int getParamLen(int index)
	{
		return ((Integer) cliParamLens.elementAt(index -1)).intValue();
	}
	/**
	 *  get parameter precision or DB2 max (31)
	 *
	 *  @param index parameter index starting with 1
	 *
	 *  @return  precision
	 */
	protected int getParamPrecision(int index) throws SQLException
	{
		if (ps != null && ps instanceof CallableStatement)
		{
			EmbedParameterSetMetaData pmeta = 	((EmbedCallableStatement)
											 ps).getEmbedParameterSetMetaData();
			return Math.min(pmeta.getPrecision(index),
							FdocaConstants.NUMERIC_MAX_PRECISION);

		}
		else 
			return -1;
	}
	
	/**
	 *  get parameter scale or DB2 max (31)
	 *
	 *  @param index parameter index starting with 1
	 *
	 *  @return  scale
	 */
	protected int getParamScale(int index) throws SQLException
	{
		if (ps != null && ps instanceof CallableStatement)
		{
			EmbedParameterSetMetaData pmeta = 	((EmbedCallableStatement)
											 ps).getEmbedParameterSetMetaData();
			return Math.min(pmeta.getScale(index),FdocaConstants.NUMERIC_MAX_PRECISION);
		}
		else 
			return -1;
	}

	/**
	 * save parameter len sent by client
	 * @param index parameter index starting with 1
	 * @param value  length of data value
	 *
	 */
	protected void  setParamLen(int index, int value)
	{
		cliParamLens.add(index -1, new Integer(value));
	}

	/**
	 * get the number of parameters for this statement
	 * 
	 * @return number of parameters
	 */
	protected int getNumParams()
	{
		if (cliParamDrdaTypes != null)
			return cliParamDrdaTypes.size();
		else
			return 0;
	}
	   
	/** 
	 * get the number of result set columns for the current resultSet
	 * 
	 * @return number of columns
	 */

	protected int getNumRsCols()
	{
		int[] rsDrdaTypes = currentDrdaRs.getRsDRDATypes();
		if (rsDrdaTypes != null)
			return rsDrdaTypes.length;
		else 
			return 0;
	}

	/**
	 * get  resultset/out parameter DRDAType
	 *
	 * @param index - starting with 1
	 * @return  DRDA Type of column
	 */
	protected int getRsDRDAType(int index)
	{
		return currentDrdaRs.getRsDRDAType(index);
	}

	/**
	 * get resultset/out parameter DRDALen
	 * @param index starting with 1
	 * 
	 * @return length of drda data
	 */
	 
	protected int getRsLen(int index)
	{
		return currentDrdaRs.getRsLen(index);
	}

	/**
	 * set resultset column data length
	 * @param index starting with 1
	 * @value length
	 */
	protected void  setRsLen(int index, int value)
	{
		currentDrdaRs.setRsLen(index,value);
	}

	/**
	 * return whether this is a procedure
	 * 
	 * @return true if procName is not null 
	 * RESOLVE: (should we check for isCall or is this good enough)
	 */ 
	public  boolean isProcedure()
	{
		return (procName != null);
	}


	/**
	 * @param rsNum  - result set # starting with 0 
	 */
	public String getResultSetCursorName(int rsNum) throws SQLException
	{
		ResultSet rs = getResultSet(rsNum);
		return rs.getCursorName();			

	}


	protected String toDebugString(String indent)
	{
		ResultSet rs = currentDrdaRs.getResultSet();
		
		String s ="";
		if (ps == null) 
			s += indent + ps;
		else
		{
			s += indent + pkgid + sectionNumber ;
			s += "\t" + ((EmbedPreparedStatement) ps).getSQLText();
		}
		return s;
	}

	/**  For a single result set, just echo the consistency token that the client sent us.
	 * For subsequent resultSets, just subtract the resultset number from
	 * the consistency token and that will differentiate the result sets.
	 * This seems to be what DB2 does
	 * @param rsNum  - result set # starting with 0
	 * 
	 * @return  Consistency token for result set
	 */

	protected String calculateResultSetPkgcnstknStr(int rsNum)
	{	
		String consistToken = pkgcnstknStr;

		if (rsNum == 0 || pkgcnstknStr == null)
			return consistToken;
		else
		{
			try {
				BigInteger  consistTokenBi = 
					new BigInteger(consistToken.getBytes(DB2jServerImpl.DEFAULT_ENCODING));
				BigInteger rsNumBi = BigInteger.valueOf(rsNum);
				consistTokenBi = consistTokenBi.subtract(rsNumBi);
				consistToken = new String(consistTokenBi.toByteArray(),DB2jServerImpl.DEFAULT_ENCODING);
			}
			catch (UnsupportedEncodingException e)
			{// Default encoding always supported
			}
		}
		return consistToken;
	}

	protected boolean isCallableStatement()
	{
		return isCall;
	}

	private boolean isCallableSQL(String sql)
	{
		java.util.StringTokenizer tokenizer = new java.util.StringTokenizer
			(sql, "\t\n\r\f=? (");
		 String firstToken = tokenizer.nextToken();
		 if (StringUtil.SQLEqualsIgnoreCase(firstToken, 
											"call")) // captures CALL...and ?=CALL...
			 return true;
		 return false;
				 
	}

	private void setupCallableStatementParams(CallableStatement cs) throws SQLException
	{
		EmbedParameterSetMetaData pmeta = 	((EmbedCallableStatement) cs).getEmbedParameterSetMetaData();
		int numElems = pmeta.getParameterCount();

		for ( int i = 0; i < numElems; i ++)
		{
			boolean outputFlag = false;
			
			int parameterMode = pmeta.getParameterMode(i + 1);
			int parameterType = pmeta.getParameterType(i + 1);

			switch (parameterMode) {
				case JDBC30Translation.PARAMETER_MODE_IN:
					break;
				case JDBC30Translation.PARAMETER_MODE_OUT:
				case JDBC30Translation.PARAMETER_MODE_IN_OUT:
					outputFlag = true;
					break;
				case JDBC30Translation.PARAMETER_MODE_UNKNOWN:
					// It's only unknown if array
					String objectType = pmeta.getParameterClassName(i+1);
					parameterType =
						getOutputParameterTypeFromClassName(objectType);
					if (parameterType  != NOT_OUTPUT_PARAM)
						outputFlag = true;
			}

			if (outputFlag)
			{
				if (outputTypes == null) //not initialized yet, since previously none output
				{
					outputTypes = new int[numElems];
					for (int j = 0; j < numElems; j++)
						outputTypes[j] = NOT_OUTPUT_PARAM;  //default init value
				}
				// save the output type so we can register when we parse
				// the SQLDTA
				outputTypes[i] = parameterType;
			}
			
		}
	}



	/** 
		Given an object class  name get the paramameter type if the 
		parameter mode is unknown.
		
		Arrays except for byte arrrays are assumed to be output parameters
		TINYINT output parameters are going to be broken because there
		is no way to differentiate them from binary input parameters.
		@param objectName Class name of object being evaluated.
		indicating if this an output parameter
		@return type from java.sql.Types
	**/
	
	protected static int getOutputParameterTypeFromClassName(String
																	objectName)
	{
		
		if (objectName.endsWith("[]"))
		{
					// For byte[] we are going to assume it is input.
			// For TINYINT output params you gotta use 
			//  object Integer[] or use a procedure				   
					if (objectName.equals("byte[]"))
					{
						return NOT_OUTPUT_PARAM;
							
							//isOutParam[offset] = false;
							//return java.sql.Types.VARBINARY;
					}
					
					// Known arrays are output parameters
					// otherwise we pass it's a JAVA_OBJECT
					if (objectName.equals("java.lang.Byte[]"))
						return java.sql.Types.TINYINT;
					
					if (objectName.equals("byte[][]"))
						return java.sql.Types.VARBINARY;
					if (objectName.equals("java.lang.String[]"))
						return java.sql.Types.VARCHAR; 
					if (objectName.equals("int[]") || 
						objectName.equals("java.lang.Integer[]"))
						return java.sql.Types.INTEGER;
					else if (objectName.equals("long[]")
							 || objectName.equals("java.lang.Long[]"))
						return java.sql.Types.BIGINT;
					else if (objectName.equals("java.math.BigDecimal[]"))
						return java.sql.Types.NUMERIC;
					else if (objectName.equals("boolean[]")  || 
							 objectName.equals("java.lang.Boolean[]"))
						return java.sql.Types.BIT;
					else if (objectName.equals("short[]"))
						return java.sql.Types.SMALLINT;
					else if (objectName.equals("float[]") ||
							 objectName.equals("java.lang.Float[]"))
						return java.sql.Types.REAL;
					else if (objectName.equals("double[]") ||
							 objectName.equals("java.lang.Double[]"))
						return java.sql.Types.DOUBLE;
					else if (objectName.equals("java.sql.Date[]"))
						return java.sql.Types.DATE;
					else if (objectName.equals("java.sql.Time[]"))
						return java.sql.Types.TIME;
					else if (objectName.equals("java.sql.Timestamp[]"))
						return java.sql.Types.TIMESTAMP;
		}
		// Not one of the ones we know. This must be a JAVA_OBJECT
		return NOT_OUTPUT_PARAM;
		//isOutParam[offset] = false;				
		//return java.sql.Types.JAVA_OBJECT;

	}
	
	
	public void registerAllOutParams() throws SQLException
	{
		if (isCall && (outputTypes != null))
			for (int i = 1; i <= outputTypes.length; i ++)
				registerOutParam(i);
		
	}
	
	public void registerOutParam(int paramNum) throws SQLException
	{
		CallableStatement cs;
		if (isOutputParam(paramNum))
		{
			cs = (CallableStatement) ps;
			cs.registerOutParameter(paramNum, getOutputParamType(paramNum));
		}
	}

	protected boolean hasOutputParams()
	{
		return (outputTypes != null);
	}

	/**
	 * is  parameter an ouput parameter
	 * @param paramNum parameter number starting with 1.
	 * return true if this is an output parameter.
	 */
	boolean isOutputParam(int paramNum)
	{
		if (outputTypes != null)
			return (outputTypes[paramNum - 1] != NOT_OUTPUT_PARAM);
		return false;
		
	}
	/** 
	 * get type for output parameter. 
	 *
	 * @param paramNum - parameter number starting with 1
	 * @return jdbcType or NOT_OUTPUT_PARAM if this is not an output parameter
	 */
	int getOutputParamType(int paramNum)
	{
		if (outputTypes != null)
			return (outputTypes[ paramNum - 1 ]);
		return NOT_OUTPUT_PARAM;
	}

	private boolean isDynamicPkgid(String pkgid)
	{
		char size = pkgid.charAt(3);
		
		//  separate attribute used for holdability in 5.1.60
		// this is just for checking that it is a dynamic package
		char holdability = pkgid.charAt(4); 			                                    
		return (pkgid.substring(0,3).equals("SYS") && (size == 'S' ||
													   size == 'L')
				&& (holdability == 'H' || holdability == 'N'));
		
	}

   
	private  void parsePkgidToFindHoldability()
	{
		if (withHoldCursor != -1)
			return;
		//First, check if holdability was passed as a SQL attribute "WITH HOLD" for this prepare. If yes, then withHoldCursor
		//should not get overwritten by holdability from package name and that is why the check for -1
		if (isDynamicPkgid(pkgid))
		{
			if(pkgid.charAt(4) == 'N')
				withHoldCursor = JDBC30Translation.CLOSE_CURSORS_AT_COMMIT;
			else  
				withHoldCursor = JDBC30Translation.HOLD_CURSORS_OVER_COMMIT;
		}
		else 
		{
			withHoldCursor = JDBC30Translation.HOLD_CURSORS_OVER_COMMIT;
		
		}
	}
}










