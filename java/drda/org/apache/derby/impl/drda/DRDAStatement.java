/*

   Derby - Class org.apache.derby.impl.drda.DRDAStatement

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

import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.StringTokenizer;
import java.util.Vector;
import java.lang.reflect.Array;

import org.apache.derby.iapi.jdbc.BrokeredConnection;
import org.apache.derby.iapi.jdbc.BrokeredPreparedStatement;
import org.apache.derby.iapi.jdbc.EngineConnection;
import org.apache.derby.iapi.jdbc.EnginePreparedStatement;
import org.apache.derby.iapi.reference.JDBC30Translation;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.util.StringUtil;
import org.apache.derby.impl.jdbc.Util;

/**
	DRDAStatement stores information about the statement being executed
*/
class DRDAStatement
{

	//NOTE!
	//
	// Since DRDAStatements are reused, ALL variables (except those noted in 
	// the comments for reset method) should be set to their default values 
	// in reset().
	

	protected String typDefNam;		//TYPDEFNAM for this statement
	protected int byteOrder;		//deduced from typDefNam, save String comparisons
	protected int ccsidSBC;			//CCSID for single byte characters
	protected int ccsidDBC;			//CCSID for double byte characters
	protected int ccsidMBC;			//CCSID for mixed byte characters
	protected String ccsidSBCEncoding;	//Java encoding for CCSIDSBC
	protected String ccsidDBCEncoding;	//Java encoding for CCSIDDBC
	protected String ccsidMBCEncoding;	//Java encoding for CCSIDMBC

	protected Database database;		// Database this statement is created for
	private   Pkgnamcsn pkgnamcsn;		// Package name/section # and  consistency token
	protected ConsistencyToken pkgcnstkn;       // Consistency token for the first result set
 	protected String pkgid;              // package id
 	protected int pkgsn;		// section number
	int withHoldCursor = -1;	 // hold cursor after commit attribute.
	protected int isolationLevel;         //JCC isolation level for Statement
	protected String cursorName;
	protected int scrollType = ResultSet.TYPE_FORWARD_ONLY;			// Sensitive or Insensitive scroll attribute
	protected int concurType = ResultSet.CONCUR_READ_ONLY;;			// Concurency type
	protected long rowCount;			// Number of rows we have processed
	protected byte [] rslsetflg;		// Result Set Flags
	protected int maxrslcnt;			// Maximum Result set count
	protected PreparedStatement ps;     // Prepared statement
	protected ParameterMetaData stmtPmeta; // param metadata
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

	/** This class is used to keep track of the statement's parameters
	 * as they are received from the client. It uses arrays to track
	 * the DRDA type, the length in bytes and the externalness of each
	 * parameter. Arrays of int/byte are used rather than ArrayLists
	 * of Integer/Byte in order to re-use the same storage each time
	 * the statement is executed. */
	private static class DrdaParamState {
		private int typeLstEnd_ = 0;
		private byte[] typeLst_ = new byte[10];
		private int[]  lenLst_ = new int[10];
		private int extLstEnd_ = 0;
		private int[]  extLst_ = new int[10];

		private static Object growArray(Object array) {
			final int oldLen = Array.getLength(array);
			Object tmp =
				Array.newInstance(array.getClass().getComponentType(),
								  Math.max(oldLen,1)*2);
			System.arraycopy(array, 0, tmp, 0, oldLen);
			return tmp;
		}

		/**
		 * <code>clear</code> resets the arrays so that new parameters
		 * will be added at the beginning. No initialization or
		 * releasing of storage takes place unless the trim argument
		 * is true.
		 *
		 * @param trim - if true; release excess storage
		 */
		protected void clear(boolean trim) {
			typeLstEnd_ = 0;
			extLstEnd_ = 0;
			if (trim && typeLst_.length > 10) {
				typeLst_ = new byte[10];
				lenLst_ = new int[10];
				extLst_ = new int[10];
			}
		}

		/**
		 * <code>addDrdaParam</code> adds a new parameter with its
		 * DRDA type and byte length. The arrays are automatically
		 * grown if needed.
		 *
		 * @param t a <code>byte</code> value, the DRDA type of the
		 * parameter being added
		 * @param s an <code>int</code> value, the length in bytes of
		 * the parameter being added
		 */
		protected void addDrdaParam(byte t, int s) {
			if (typeLstEnd_ >= typeLst_.length) {
				typeLst_ = (byte[])growArray(typeLst_);
				lenLst_ = (int[])growArray(lenLst_);
			}
			typeLst_[typeLstEnd_] = t;
			lenLst_[typeLstEnd_] = s;
			++typeLstEnd_;
		}

		/**
		 * <code>getDrdaParamCount</code> return the number of
		 * parameters added so far (since last clear).
		 *
		 * @return an <code>int</code> value, the number of parameters
		 */
		protected int  getDrdaParamCount() { return typeLstEnd_; }

		/**
		 * <code>getDrdaType</code> returns a byte that represents the
		 * DRDA type of the ith parameter.
		 *
		 * @param i an <code>int</code> value, a parameter position
		 * (zero-based)
		 * @return a <code>byte</code> value, the DRDA type
		 */
		protected byte getDrdaType(int i) { return typeLst_[i]; }

		/**
		 * <code>getDrdaLen</code> returns the length in bytes of the
		 * ith parameter.
		 *
		 * @param i an <code>int</code> value, a parameter position
		 * (zero-based)
		 * @return an <code>int</code> value
		 */
		protected int getDrdaLen(int i) { return lenLst_[i]; }

		/**
		 * <code>addExtPos</code> marks parameter i as external. The
		 * array is grown as needed.
		 *
		 * @param p an <code>int</code> value, a parameter position
		 * (zero-based)
		 */
		protected void addExtPos(int p) {
			if (extLstEnd_ >= extLst_.length) {
				extLst_ = (int[])growArray(extLst_);
			}
			extLst_[extLstEnd_] = p;
			++extLstEnd_;
		}

		/**
		 * <code>getExtPosCount</code> returns the number of
		 * parameters marked as external so far (since last clear).
		 *
		 * @return an <code>int</code> value, the number of external
		 * parameters.
		 */
		protected int getExtPosCount() { return extLstEnd_; }

		/**
		 * <code>getExtPos</code> returns the actual parameter position
		 * of the ith external parameter.
		 *
		 * @param i an <code>int</code> value, index into the list of
		 * external parameters, zero-based
		 * @return an <code>int</code> value, the parameter position
		 * of the ith external parameter (zero-based)
		 */
		protected int getExtPos(int i) { return extLst_[i]; }
	}
	private DrdaParamState drdaParamState_ = new DrdaParamState();

	// Query options  sent on EXCSQLSTT
	// These the default for ResultSets created for this statement.
	// These can be overriden by OPNQRY or CNTQRY,
	protected int nbrrow;			// number of fetch or insert rows
	protected int qryrowset;			// Query row set
	protected int blksize;				// Query block size
	protected int maxblkext;			// Maximum number of extra blocks
	protected int outovropt;			// Output Override option
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
	DRDAStatement (Database database) 
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
	 */
	protected void  setExtDtaObjects(ArrayList a)
	{
		currentDrdaRs.setExtDtaObjects(a);
	}

	public void setSplitQRYDTA(byte []data)
	{
		currentDrdaRs.setSplitQRYDTA(data);
	}
	public byte[]getSplitQRYDTA()
	{
		return currentDrdaRs.getSplitQRYDTA();
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

	/**
	 *
	 *  get resultSetHoldability.
	 * 
	 * @return the resultSet holdability for the prepared statement
	 *
	 */
	protected int getResultSetHoldability() throws SQLException
	{
		return getResultSetHoldability(getResultSet());
	}
	
	/**
	 *
	 *  get resultSetHoldability.
	 * 
	 * @param rs ResultSet 
	 * @return the resultSet holdability for the prepared statement
	 *
	 */
	int getResultSetHoldability(ResultSet rs) throws SQLException
	{
		Statement rsstmt;

		if (rs  != null)
			rsstmt = rs.getStatement();
		else
			rsstmt = getPreparedStatement();
        
        return rsstmt.getResultSetHoldability();
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
	 * Set query options sent on OPNQRY and pass options down to the
	 * current <code>DRDAResultSet</code> object.
	 *
	 * @param blksize QRYBLKSZ (Query Block Size)
	 * @param qryblkctl QRYPRCTYP (Query Protocol Type)
	 * @param maxblkext MAXBLKEXT (Maximum Number of Extra Blocks)
	 * @param outovropt OUTOVROPT (Output Override Option)
	 * @param qryrowset QRYROWSET (Query Rowset Size)
	 * @param qryclsimpl QRYCLSIMP (Query Close Implicit)
	 * @see DRDAResultSet#setOPNQRYOptions(int, int, int, int, int, int)
	 */
	protected void setOPNQRYOptions(int blksize, int qryblkctl,
								  int maxblkext, int outovropt,int qryrowset,int qryclsimpl)
	{
		this.blksize = blksize;
		this.qryprctyp = qryblkctl;
		this.maxblkext = maxblkext;
		this.outovropt = outovropt;
		this.qryrowset = qryrowset;
		currentDrdaRs.setOPNQRYOptions( blksize, qryblkctl, maxblkext, 
				outovropt, qryrowset, qryclsimpl);
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

	protected void setScrollType(int scrollType)
	{
		currentDrdaRs.scrollType = scrollType;
	}

	protected int  getScrollType()
	{
		return currentDrdaRs.scrollType;
	}

	/** 
	 * is this a scrollable cursor?
	 * return true if this is not a forward only cursor
	 */
	protected boolean isScrollable()
	{
		return (getScrollType() != ResultSet.TYPE_FORWARD_ONLY);
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
	 * This method is used to initialize the default statement of the database
	 * for re-use. It is different from reset() method since default statements
	 * get initiliazed differently. e.g: stmt variable used in default statement
	 * is created only once in Database.makeConnection. 
	 * TODO: Need to see what exactly it means to initialize the default 
	 * statement. (DERBY-1002)
	 * 
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
		if (pkgnamcsn !=null && 
			isolationLevel != Connection.TRANSACTION_NONE)
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
		ps = prepareStatementJDBC3(sqlStmt, scrollType, concurType, 
									   withHoldCursor);
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
	protected PreparedStatement getPreparedStatement() throws SQLException
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
	 * @return true if the execution has resultSets
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
				//For callable statement, get holdability of statement generating the result set
				if(isCallable)
					addResultSet(rs,getResultSetHoldability(rs));
				else
					addResultSet(rs,withHoldCursor);
				hasResultSet = true;
			}
			// For normal selects we are done, but procedures might
			// have more resultSets
		}while (isCallable && getMoreResults(JDBC30Translation.KEEP_CURRENT_RESULT));

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
	protected void setPkgnamcsn(Pkgnamcsn pkgnamcsn)
	{
		this.pkgnamcsn =  pkgnamcsn;
		// Store the consistency string for the first ResultSet.
		// this will be used to calculate consistency strings for the 
		// other result sets.
		pkgid = pkgnamcsn.getPkgid();

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
			pkgsn = pkgnamcsn.getPkgsn();
			this.cursorName = "SQL_CUR" +  shortPkgid + "C" + pkgsn ;
		}
		else // static package
		{
			isolationLevel = getStaticPackageIsolation(pkgid);
		}

		this.pkgcnstkn = pkgnamcsn.getPkgcnstkn();

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
	protected Pkgnamcsn getPkgnamcsn() 
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
	 * @param rsNum resultSetNumber starting with 0
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
			ConsistencyToken key = (ConsistencyToken) resultSetKeyList.get(rsNum);
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
	 * Gets the current DRDA ResultSet
	 * 
	 * @return DRDAResultSet
	 */
	protected DRDAResultSet getCurrentDrdaResultSet()
	{
		return currentDrdaRs ;
	}

	/**
 	 * Set currentDrdaResultSet 
	 *
	 * @param rsNum   The result set number starting with 0
	 *                 
	 */
	protected void setCurrentDrdaResultSet(int rsNum)
	{
		ConsistencyToken consistToken = getResultSetPkgcnstkn(rsNum);
		if (currentDrdaRs.pkgcnstkn == consistToken)
			return;
		currentDrdaRs = getDrdaResultSet(consistToken);

	}

	/**
 	 * Set currentDrdaResultSet 
	 *
	 * @param pkgnamcsn  The pkgid section number and unique resultset
	 *                    consistency token
	 *                 
	 */
	protected void setCurrentDrdaResultSet(Pkgnamcsn pkgnamcsn)
	{
		pkgid = pkgnamcsn.getPkgid();
		pkgsn = pkgnamcsn.getPkgsn();
		ConsistencyToken consistToken = pkgnamcsn.getPkgcnstkn();
		DRDAResultSet newDrdaRs = getDrdaResultSet(consistToken);
		if (newDrdaRs != null)
			currentDrdaRs = newDrdaRs;
	}


	/*
	 * get DRDAResultSet by consistency token
	 *
	 */
	private DRDAResultSet getDrdaResultSet(ConsistencyToken consistToken)
	{
		if ( resultSetTable   == null || 
			 (currentDrdaRs != null &&
			  currentDrdaRs.pkgcnstkn == consistToken ))
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
		ConsistencyToken consistToken = getResultSetPkgcnstkn(rsNum);
		return getDrdaResultSet(consistToken);
	}

	/** Add a new resultSet to this statement.
	 * Set as the current result set if  there is not an 
	 * existing current resultset.
	 * @param value - ResultSet to add
	 * @param holdValue - Holdability of the ResultSet 
	 * @return    Consistency token  for this resultSet
	 *            For a single resultSet that is the same as the statement's 
	 *            For multiple resultSets just the consistency token is changed 
	 */
	protected ConsistencyToken addResultSet(ResultSet value, int holdValue) throws SQLException
	{

		DRDAResultSet newDrdaRs = null;

		int rsNum = numResultSets;
		ConsistencyToken newRsPkgcnstkn = calculateResultSetPkgcnstkn(rsNum);

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
				resultSetTable.put(pkgcnstkn, currentDrdaRs);
				resultSetKeyList = new ArrayList();
				resultSetKeyList.add(0, pkgcnstkn);
			}

			resultSetTable.put(newRsPkgcnstkn, newDrdaRs);
			resultSetKeyList.add(rsNum, newRsPkgcnstkn);
		}

		newDrdaRs.setResultSet(value);
		newDrdaRs.setPkgcnstkn(newRsPkgcnstkn);
		newDrdaRs.withHoldCursor = holdValue;
		setRsDefaultOptions(newDrdaRs);
		newDrdaRs.suspend();
		numResultSets++;
		return newRsPkgcnstkn;
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
	protected ConsistencyToken getResultSetPkgcnstkn(int rsNum)
	{
		if (rsNum == 0)
			return pkgcnstkn;
		else 
			return (ConsistencyToken) resultSetKeyList.get(rsNum);			   
	}


	/** 
	 * Set ResultSet DRDA DataTypes
	 * @param value drdaTypes for columns.
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
	 * @param value drdaTypes for columns.
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

	/**
	 * This method closes the JDBC objects and frees up all references held by
	 * this object.
	 * 
	 * @throws SQLException
	 */
	protected void close()  throws SQLException
	{
		if (ps != null)
			ps.close();
		if (stmt != null)
			stmt.close();
		currentDrdaRs.close();
		resultSetTable = null;
		resultSetKeyList = null;
		ps = null;
		stmtPmeta = null;
		stmt = null;
		rslsetflg = null;
		procName = null;
		outputTypes = null;
		// Clear parameters and release excess storage
		drdaParamState_.clear(true);
	}
	
	/**
	 * This method resets the state of this DRDAStatement object so that it can
	 * be re-used. This method should reset all variables of this class except 
	 * the following:
     * 1. database - This variable gets initialized in the constructor and by
     * call to setDatabase.
     * 2. members which get initialized in setPkgnamcsn (pkgnamcsn, pkgcnstkn, 
     * pkgid, pkgsn, isolationLevel, cursorName). pkgnamcsn is the key used to 
     * find if the DRDAStatement can be re-used. Hence its value will not change 
     * when the object is re-used.
	 * 
	 */
	protected void reset() 
	{
		setTypDefValues();
		
		withHoldCursor = -1;
		scrollType = ResultSet.TYPE_FORWARD_ONLY;	
		concurType = ResultSet.CONCUR_READ_ONLY;;
		rowCount = 0;
		rslsetflg = null;
		maxrslcnt = 0;
		ps = null;
		stmtPmeta = null;
		isCall = false;
		procName = null;
		outputTypes = null;
		outputExpected = false;
		stmt = null;
		
		currentDrdaRs.reset();
		resultSetTable = null;
		resultSetKeyList = null;
		numResultSets = 0;
		
		// Clear parameters without releasing storage
		drdaParamState_.clear(false);
		
		nbrrow = 0;
		qryrowset = 0;	
		blksize = 0;		
		maxblkext = 0;	
		outovropt = 0;	
		qryrfrtbl = false;
		qryprctyp = CodePoint.QRYBLKCTL_DEFAULT;

		needsToSendParamData = false;
		explicitlyPrepared = false;
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

	/** Clears the parameter state (type, length and ext information)
	 * stored in this statement, but does not release any
	 * storage. This reduces the cost of re-executing the statement
	 * since no new storage needs to be allocated. */
	protected void clearDrdaParams() {
		drdaParamState_.clear(false);
	}

	/** Get the number of external parameters in this
	 * statement. External means parameters that are transmitted in a
	 * separate DSS in the DRDA protocol.
	 * @return the number of external parameters
	 */
	protected int getExtPositionCount() {
		return drdaParamState_.getExtPosCount();
	}

	/** Get the parameter position of the i'th external parameter
	 * @param i - zero-based index into list of external parameters
	 * @return the parameter position of the i'th external parameter
	 */
	protected int getExtPosition(int i) {
		return drdaParamState_.getExtPos(i);
	}

	/** Mark the pos'th parameter as external
	 * @param pos - zero-based index into list of external parameters
	 */
	protected void addExtPosition(int pos) {
		drdaParamState_.addExtPos(pos);
	}

	/** Get the number of parameters, internal and external, that has
	 * been added to this statement.
	 * @return the number of parameters
	 */
	protected int getDrdaParamCount() {
		return drdaParamState_.getDrdaParamCount();
	}

	/** Add another parameter to this statement.
	 * @param t - type of the parameter
	 * @param l - length in bytes of the parameter
	 */
	protected void addDrdaParam(byte t, int l) {
		drdaParamState_.addDrdaParam(t, l);
	}

	/**
	 * get parameter DRDAType
	 *
	 * @param index - starting with 1
	 * @return  DRDA Type of column
	 */
 	protected int getParamDRDAType(int index) {
		return drdaParamState_.getDrdaType(index-1);
 	}

	/**
	 * returns drda length of parameter as sent by client.
	 * @param index - starting with 1
	 * @return data length

	 */
	protected int getParamLen(int index)
	{
		return drdaParamState_.getDrdaLen(index-1);
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
			ParameterMetaData pmeta = getParameterMetaData();

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
			ParameterMetaData pmeta = getParameterMetaData();
			return Math.min(pmeta.getScale(index),FdocaConstants.NUMERIC_MAX_PRECISION);
		}
		else 
			return -1;
	}

	/** 
	 * get the number of result set columns for the current resultSet
	 * 
	 * @return number of columns
	 */

	protected int getNumRsCols()
	{
		int[] rsDrdaTypes = getRsDRDATypes();
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
	 * @param value length
	 */
	protected void  setRsLen(int index, int value)
	{
		currentDrdaRs.setRsLen(index,value);
	}

	/**
	 * @param rsNum  - result set # starting with 0 
	 */
	public String getResultSetCursorName(int rsNum) throws SQLException
	{
		DRDAResultSet drdaRs = getDrdaResultSet(rsNum);
		return drdaRs.getResultSetCursorName();			

	}


	protected String toDebugString(String indent)
	{
		ResultSet rs = currentDrdaRs.getResultSet();
		
		String s ="";
		if (ps == null) 
			s += indent + ps;
		else
		{
			s += indent + pkgid + pkgsn ;
			s += "\t" + getSQLText();
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

	protected ConsistencyToken calculateResultSetPkgcnstkn(int rsNum)
	{	
		ConsistencyToken consistToken = pkgcnstkn;

		if (rsNum == 0 || pkgcnstkn == null)
			return consistToken;
		else
		{
			BigInteger consistTokenBi =
				new BigInteger(consistToken.getBytes());
			BigInteger rsNumBi = BigInteger.valueOf(rsNum);
			consistTokenBi = consistTokenBi.subtract(rsNumBi);
			consistToken = new ConsistencyToken(consistTokenBi.toByteArray());
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
		ParameterMetaData pmeta = getParameterMetaData();
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


	/**
	 *  prepare a statement using EngineConnection.prepareStatement
     *  so that server can run on jdk131 and still pass holdability.  
	 *  @param sqlStmt - SQL statement text
	 *  @param scrollType - scroll type
	 *  @param concurType - concurrency type
	 *  @param withHoldCursor - holdability
	 * 
	 *  @throws SQLException
	 *  @return Prepared Statement
	 *  @see java.sql.Connection#prepareStatement
	 */
	private PreparedStatement prepareStatementJDBC3(String sqlStmt, int
													scrollType, int concurType,
													int withHoldCursor) throws SQLException
	{
        EngineConnection conn = database.getConnection();
        if (withHoldCursor == -1) {
            // Holdability not explictly set, let the
            // connection provide the default.
            return conn.prepareStatement(sqlStmt,
                    scrollType, concurType);
        }
        
        // Holdability explictly set. 
        return conn.prepareStatement(sqlStmt,
                scrollType, concurType, withHoldCursor);
	}

	
	/** 
	 * Retrieve the ParameterMetaData for the prepared statement. 
	 * @return ParameterMetaData for the prepared statement. 
	 * Note: there is no separate BrokeredParameterSetMetaData.
	 */
	protected ParameterMetaData getParameterMetaData() throws SQLException
	{
		if (stmtPmeta != null)
			return stmtPmeta;

		stmtPmeta = ps.getParameterMetaData();
        
        return stmtPmeta;
	}
	
	/**
	 * get more results using reflection.
	 * @param current - flag to pass to Statement.getMoreResults(current)
	 * @return true if there are more results.
	 * @throws SQLException
	 * @see java.sql.Statement#getMoreResults
	 *
	 */
	private boolean getMoreResults(int current) throws SQLException
	{       
        return getPreparedStatement().getMoreResults(current);
	}

	/**
	 * Use reflection to retrieve SQL Text for EmbedPreparedStatement  
	 * or BrokeredPreparedStatement.
	 * @return SQL text
	 */
	private String getSQLText() 
	{
	   String retVal = null;
		Class[] emptyPARAM = {};
		Object[] args = null;
		try {
			Method sh = getPreparedStatement().getClass().getMethod("getSQLText",emptyPARAM);
			retVal = (String) sh.invoke(getPreparedStatement(),args);
		}
		catch (Exception e)
		{
			//  do nothing we will just return a null string
		}
		return retVal;

	}

	/** helper method to handle exceptions generated by methods invoked 
	 * through  reflection.
	 * @param e - exception thrown
	 * @throws SQLException - actual exception that occurred
	 */
	private void handleReflectionException(Exception e) throws SQLException
	{
		if  (e instanceof InvocationTargetException) 
		{
			Throwable t = ((InvocationTargetException) e).getTargetException();
			
			if (t  instanceof SQLException)
			{
				throw (SQLException) t;
			}
			else
			{
				t.printStackTrace();
				throw Util.javaException(t);
			}
		}
		else
			// invoke can throw IllegalAccessException or 
			// IllegalArgumentException, but these should not 
			// occur from this code. Just in case we will throw it
			throw Util.javaException(e);
		
	}
	
	/**
	 * Method to decide whether the ResultSet should be closed
	 * implicitly based on the QRYCLSIMP value sent from the
	 * client. Only forward-only result sets should be implicitly
	 * closed. Some clients do not expect result sets to be closed
	 * implicitly if the protocol is LMTBLKPRC.
	 *
	 * @param lmtblkprcOK <code>true</code> if the client expects
	 * QRYCLSIMP to be respected for the LMTBLKPRC protocol
	 * @return implicit close boolean
	 * @exception SQLException
	 */
	boolean isRSCloseImplicit(boolean lmtblkprcOK) throws SQLException {
		return
			(currentDrdaRs.qryclsimp == CodePoint.QRYCLSIMP_YES) &&
			!isScrollable() &&
			(lmtblkprcOK ||
			 (currentDrdaRs.getQryprctyp() != CodePoint.LMTBLKPRC));
	}
}










