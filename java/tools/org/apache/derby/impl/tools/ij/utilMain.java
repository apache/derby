/*

   Derby - Class org.apache.derby.impl.tools.ij.utilMain

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.tools.ij;
                
import org.apache.derby.iapi.reference.JDBC20Translation;
import org.apache.derby.iapi.reference.JDBC30Translation;

import org.apache.derby.tools.JDBCDisplayUtil;
import org.apache.derby.iapi.tools.i18n.*;

import org.apache.derby.iapi.services.info.ProductVersionHolder;
import org.apache.derby.iapi.services.info.ProductGenusNames;

import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.error.StandardException;

import java.util.Stack;
import java.util.Hashtable;
import java.util.Properties;

import java.io.InputStream;
import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.StringReader;
import java.sql.DriverManager;
import java.sql.Driver;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.PreparedStatement;

import java.lang.reflect.*;

/**
	This class is utilities specific to the two ij Main's.
	This factoring enables sharing the functionality for
	single and dual connection ij runs.

	@author jerry
 */
public class utilMain implements java.security.PrivilegedAction {

  private static Class[] CONN_PARAM = { Integer.TYPE };
  private static Object[] CONN_ARG = { new Integer(JDBC30Translation.CLOSE_CURSORS_AT_COMMIT)};

	StatementFinder[] commandGrabber;
	UCode_CharStream charStream;
	ijTokenManager ijTokMgr;
	ij ijParser;
	ConnectionEnv[] connEnv;
	int currCE;
	private int		numConnections;
	private boolean fileInput;
	private boolean initialFileInput;
	private boolean mtUse;
	private boolean firstRun = true;
	private LocalizedOutput out = null;
	private Properties connAttributeDefaults;
	private Hashtable ignoreErrors;

	protected boolean isJCC;	//The driver being used is JCC

	/*
		In the goodness of time, this could be an ij property
	 */
	public static final int BUFFEREDFILESIZE = 2048;

	/*
	 * command can be redirected, so we stack up command
	 * grabbers as needed.
	 */
	Stack oldGrabbers = new Stack();

	LocalizedResource langUtil = LocalizedResource.getInstance();
	/**
	 * Set up the test to run with 'numConnections' connections/users.
	 *
	 * @param numConnections	The number of connections/users to test.
	 */
	public utilMain(int numConnections, LocalizedOutput out)
		throws ijFatalException
	{
		this(numConnections, out, (Hashtable)null);
	}

	/**
	 * Set up the test to run with 'numConnections' connections/users.
	 *
	 * @param numConnections	The number of connections/users to test.
	 * @param ignoreErrors		A list of errors to ignore.  If null,
	 *							all errors are printed out and nothing
	 *							is fatal.  If non-null, if an error is
	 *							hit and it is in this list, it is silently	
	 *							ignore.  Otherwise, an ijFatalException is
	 *							thrown.  ignoreErrors is used for stress
	 *							tests.
	 */
	public utilMain(int numConnections, LocalizedOutput out, Hashtable ignoreErrors)
		throws ijFatalException
	{
		String framework_property = util.getSystemProperty("framework");
		
		if (framework_property != null)
		{
			if (framework_property.equals("DB2jNet") 
					|| framework_property.equals("DB2jcc"))
				isJCC = true;
		}
		/* init the parser; give it no input to start with.
		 * (1 parser for entire test.)
		 */
		charStream = new UCode_CharStream(
						new StringReader(" "), 1, 1);
		ijTokMgr = new ijTokenManager(charStream);
		ijParser = new ij(ijTokMgr, getUtilMain());
		this.out = out;
		this.ignoreErrors = ignoreErrors;

		this.numConnections = numConnections;
		/* 1 StatementFinder and ConnectionEnv per connection/user. */
		commandGrabber = new StatementFinder[numConnections];
		connEnv = new ConnectionEnv[numConnections];

		for (int ictr = 0; ictr < numConnections; ictr++)
		{
		    commandGrabber[ictr] = new StatementFinder(langUtil.getNewInput(System.in));
			connEnv[ictr] = new ConnectionEnv(ictr, (numConnections > 1), (numConnections == 1));
			try {
				connEnv[ictr].init(out);
			} catch (SQLException s) {
				JDBCDisplayUtil.ShowException(out, s); // will continue past connect failure
			} catch (ClassNotFoundException c) {
				JDBCDisplayUtil.ShowException(out, c); // will continue past driver failure
			} catch (InstantiationException i) {
				JDBCDisplayUtil.ShowException(out, i); // will continue past driver failure
			} catch (IllegalAccessException ia) {
				JDBCDisplayUtil.ShowException(out, ia); // will continue past driver failure
			}
		}

		/* Start with connection/user 0 */
		currCE = 0;
		fileInput = false;
		initialFileInput = false;
		firstRun = true;
	}


	/**
	 * run ij over the specified input, sending output to the
	 * specified output. Any prior input and output will be lost.
	 *
	 * @param in source for input to ij
	 * @param out sink for output from ij
	 * @param connAttributeDefaults  connection attributes from -ca ij arg
	 */
	public void go(LocalizedInput[] in, LocalizedOutput out,
				   Properties connAttributeDefaults) throws ijFatalException
	{
		boolean done = false;

		String command = null;

		this.out = out;
		this.connAttributeDefaults = connAttributeDefaults;
		
		ijParser.setConnection(connEnv[currCE], (numConnections > 1));
		fileInput = initialFileInput = (!in[currCE].isStandardInput());

		for (int ictr = 0; ictr < commandGrabber.length; ictr++) {
			commandGrabber[ictr].ReInit(in[ictr]);
		}

		if (firstRun) {

			// figure out which version this is
			InputStream versionStream = (InputStream) java.security.AccessController.doPrivileged(this);

			// figure out which version this is
			ProductVersionHolder ijVersion = 
				ProductVersionHolder.getProductVersionHolderFromMyEnv(versionStream);

			String version;
			if (ijVersion != null)
			{
				version = "" + ijVersion.getMajorVersion() + "." +
					ijVersion.getMinorVersion();
			}
			else
			{
				version = "?";
			}

   			out.println(langUtil.getTextMessage("IJ_IjVers30C199", version));
			for (int i=connEnv.length-1;i>=0;i--) { // print out any initial warnings...
				Connection c = connEnv[i].getConnection();
				if (c!=null) {
					JDBCDisplayUtil.ShowWarnings(out,c);
				}
			}
			firstRun = false;

      		//check if the property is set to not show select count and set the static variable
      		//accordingly. 
    		boolean showNoCountForSelect = Boolean.getBoolean("ij.showNoCountForSelect");
      		JDBCDisplayUtil.showSelectCount = !showNoCountForSelect;

      		//check if the property is set to not show initial connections and accordingly set the
      		//static variable.
    		boolean showNoConnectionsAtStart = Boolean.getBoolean("ij.showNoConnectionsAtStart");
      		if (!(showNoConnectionsAtStart)) {
         		try {
           			ijResult result = ijParser.showConnectionsMethod(true);
 					displayResult(out,result,connEnv[currCE].getConnection());
         		} catch (SQLException ex) {
           			handleSQLException(out,ex);
         		}
      		}
    	}

		while (!ijParser.exit && !done) {
			try{
				ijParser.setConnection(connEnv[currCE], (numConnections > 1));
			} catch(Throwable t){
				//do nothing
				}

			connEnv[currCE].doPrompt(true, out);
   			try {
				command = null;

				out.flush();
				command = commandGrabber[currCE].nextStatement();

				// if there is no next statement,
				// pop back to the top saved grabber.
				while (command == null && ! oldGrabbers.empty()) {
					// close the old input file if not System.in
					if (fileInput) commandGrabber[currCE].close();
					commandGrabber[currCE] = (StatementFinder)oldGrabbers.pop();
					if (oldGrabbers.empty())
						fileInput = initialFileInput;
					command = commandGrabber[currCE].nextStatement();
				}

				// if there are no grabbers left,
				// we are done.
				if (command == null && oldGrabbers.empty()) {
					done = true;
				}
				else {
					boolean	elapsedTimeOn = ijParser.getElapsedTimeState();
					long	beginTime = 0;
					long	endTime;

					if (fileInput) {
						out.println(command+";");
						out.flush();
					}

					charStream.ReInit(new StringReader(command), 1, 1);
					ijTokMgr.ReInit(charStream);
					ijParser.ReInit(ijTokMgr);

					if (elapsedTimeOn) {
						beginTime = System.currentTimeMillis();
					}

					ijResult result = ijParser.ijStatement();
					displayResult(out,result,connEnv[currCE].getConnection());

					// if something went wrong, an SQLException or ijException was thrown.
					// we can keep going to the next statement on those (see catches below).
					// ijParseException means we try the SQL parser.

					/* Print the elapsed time if appropriate */
					if (elapsedTimeOn) {
						endTime = System.currentTimeMillis();
						out.println(langUtil.getTextMessage("IJ_ElapTime0Mil", 
						langUtil.getNumberAsString(endTime - beginTime)));
					}

					// would like when it completes a statement
					// to see if there is stuff after the ;
					// and before the <EOL> that we will IGNORE
					// (with a warning to that effect)
				}

    			} catch (ParseException e) {
					if (command != null) doCatch(command);
				} catch (TokenMgrError e) {
					if (command != null) doCatch(command);
    			} catch (SQLException e) {
					// SQL exception occurred in ij's actions; print and continue
					// unless it is considered fatal.
					handleSQLException(out,e);
    			} catch (ijException e) {
					// exception occurred in ij's actions; print and continue
    			  	out.println(langUtil.getTextMessage("IJ_IjErro0",e.getMessage()));
					doTrace(e);
    			} catch (Throwable e) {
    			  	out.println(langUtil.getTextMessage("IJ_JavaErro0",e.toString()));
					doTrace(e);
				}

			/* Go to the next connection/user, if there is one */
			currCE = ++currCE % connEnv.length;
		}

		// we need to close all sessions when done; otherwise we have
		// a problem when a single VM runs successive IJ threads
		try {
			for (int i = 0; i < connEnv.length; i++) {
				connEnv[i].removeAllSessions();
			}
		} catch (SQLException se ) {
			handleSQLException(out,se);
		}
		// similarly must close input files
		for (int i = 0; i < numConnections; i++) {
			try {
				if (!in[i].isStandardInput() )
					in[i].close();	
			} catch (Exception e ) {
    			  	out.println(langUtil.getTextMessage("IJ_CannotCloseInFile",
					e.toString()));
			}
		}

		/*
			If an exit was requested, then we will be shutting down.
		 */
		if (ijParser.exit || (initialFileInput && !mtUse)) {
			Driver d = null;
			try {
			    d = DriverManager.getDriver("jdbc:derby:");
			} catch (Exception e) {
				d = null;
			}
			if (d!=null) { // do we have a driver running? shutdown on exit.
				try {
					DriverManager.getConnection("jdbc:derby:;shutdown=true");
				} catch (SQLException e) {
					// ignore the errors, they are expected.
				}
			}
		}
  	}

	private void displayResult(LocalizedOutput out, ijResult result, Connection conn) throws SQLException {
		// display the result, if appropriate.
		if (result!=null) {
			if (result.isConnection()) {
				if (result.hasWarnings()) {
					JDBCDisplayUtil.ShowWarnings(out,result.getSQLWarnings());
					result.clearSQLWarnings();
				}
			} else if (result.isStatement()) {
				Statement s = result.getStatement();
				try {
				    JDBCDisplayUtil.DisplayResults(out,s,connEnv[currCE].getConnection());
				} catch (SQLException se) {
				    result.closeStatement();
					throw se;
				}
				result.closeStatement();
			} else if (result.isNextRowOfResultSet()) {
				ResultSet r = result.getNextRowOfResultSet();
				JDBCDisplayUtil.DisplayCurrentRow(out,r,connEnv[currCE].getConnection());
			} else if (result.isVector()) {
				util.DisplayVector(out,result.getVector());
				if (result.hasWarnings()) {
					JDBCDisplayUtil.ShowWarnings(out,result.getSQLWarnings());
					result.clearSQLWarnings();
				}
			} else if (result.isMulti()) {
			    try {
				    util.DisplayMulti(out,(PreparedStatement)result.getStatement(),result.getResultSet(),connEnv[currCE].getConnection());
				} catch (SQLException se) {
				    result.closeStatement();
					throw se;
				}
				result.closeStatement(); // done with the statement now
				if (result.hasWarnings()) {
					JDBCDisplayUtil.ShowWarnings(out,result.getSQLWarnings());
					result.clearSQLWarnings();
				}
			} else if (result.isException()) {
				JDBCDisplayUtil.ShowException(out,result.getException());
			}
		}
	}

	/**
	 * catch processing on failed commands. This really ought to
	 * be in ij somehow, but it was easier to catch in Main.
	 */
	private void doCatch(String command) {
		// this retries the failed statement
		// as a JSQL statement; it uses the
		// ijParser since that maintains our
		// connection and state.

	    try {
			boolean	elapsedTimeOn = ijParser.getElapsedTimeState();
			long	beginTime = 0;
			long	endTime;

			if (elapsedTimeOn) {
				beginTime = System.currentTimeMillis();
			}

			ijResult result = ijParser.executeImmediate(command);
			displayResult(out,result,connEnv[currCE].getConnection());

			/* Print the elapsed time if appropriate */
			if (elapsedTimeOn) {
				endTime = System.currentTimeMillis();
				out.println(langUtil.getTextMessage("IJ_ElapTime0Mil_4", 
				langUtil.getNumberAsString(endTime - beginTime)));
			}

	    } catch (SQLException e) {
			// SQL exception occurred in ij's actions; print and continue
			// unless it is considered fatal.
			handleSQLException(out,e);
	    } catch (ijException i) {
	  		out.println(langUtil.getTextMessage("IJ_IjErro0_5", i.getMessage()));
			doTrace(i);
		} catch (ijTokenException ie) {
	  		out.println(langUtil.getTextMessage("IJ_IjErro0_6", ie.getMessage()));
			doTrace(ie);
	    } catch (Throwable t) {
	  		out.println(langUtil.getTextMessage("IJ_JavaErro0_7", t.toString()));
			doTrace(t);
	    }
	}

	/**
	 * This routine displays SQL exceptions and decides whether they
	 * are fatal or not, based on the ignoreErrors field. If they
	 * are fatal, an ijFatalException is thrown.
	 * Lifted from ij/util.java:ShowSQLException
	 */
	public void handleSQLException(LocalizedOutput out, SQLException e) 
		throws ijFatalException
	{
		String errorCode;
		String sqlState = null;
		SQLException fatalException = null;

		if (Boolean.getBoolean("ij.showErrorCode")) {
			errorCode = langUtil.getTextMessage("IJ_Erro0", 
			langUtil.getNumberAsString(e.getErrorCode()));
		}
		else {
			errorCode = "";
		}

		for (; e!=null; e=e.getNextException())
		{
			/*
			** If we are to throw errors, then throw the exceptions
			** that aren't in the ignoreErrors list.  If
			** the ignoreErrors list is null we don't throw
			** any errors.
			*/
		 	if (ignoreErrors != null) 
			{
				sqlState = e.getSQLState();
				if ((sqlState != null) &&
					(ignoreErrors.get(sqlState) != null))
				{
					continue;
				}
				else
				{
					fatalException = e;
				}
			}

			String st1 = JDBCDisplayUtil.mapNull(e.getSQLState(),langUtil.getTextMessage("IJ_NoSqls"));
			String st2 = JDBCDisplayUtil.mapNull(e.getMessage(),langUtil.getTextMessage("IJ_NoMess"));
			out.println(langUtil.getTextMessage("IJ_Erro012",  st1, st2, errorCode));
			JDBCDisplayUtil.doTrace(out, e);
		}
		if (fatalException != null)
		{
			throw new ijFatalException(fatalException);
		}
	}

	/**
	 * stack trace dumper
	 */
	private void doTrace(Throwable t) {
		if (util.getSystemProperty("ij.exceptionTrace") != null) {
			t.printStackTrace(out);
		}
		out.flush();
	}

	void newInput(String fileName) {
		FileInputStream newFile = null;
		try {
			newFile = new FileInputStream(fileName);
      	} catch (FileNotFoundException e) {
        	throw ijException.fileNotFound();
		}
		if (newFile == null) return;

		// if the file was opened, move to use it for input.
		oldGrabbers.push(commandGrabber[currCE]);
	    commandGrabber[currCE] = 
                new StatementFinder(langUtil.getNewInput(new BufferedInputStream(newFile, BUFFEREDFILESIZE)));
		fileInput = true;
	}

	void newResourceInput(String resourceName) {
		InputStream is = util.getResourceAsStream(resourceName);
		if (is==null) throw ijException.resourceNotFound();
		oldGrabbers.push(commandGrabber[currCE]);
	    commandGrabber[currCE] = 
                new StatementFinder(langUtil.getNewInput(new BufferedInputStream(is, BUFFEREDFILESIZE)));
		fileInput = true;
	}

	/**
	 * REMIND: eventually this might be part of StatementFinder,
	 * used at each carriage return to show that it is still "live"
	 * when it is reading multi-line input.
	 */
	static void doPrompt(boolean newStatement, LocalizedOutput out, String tag) 
	 {
		if (newStatement) {
	  		out.print("ij"+(tag==null?"":tag)+"> ");
		}
		else {
			out.print("> ");
		}
		out.flush();
	}

	void setMtUse(boolean b) {
		mtUse = b;
	}

	// JDBC 2.0 support

	/**
	 * Return the right utilMain to use.  (JDBC 1.1 or 2.0)
	 *
	 */
	public utilMain getUtilMain()
	{
		return this;
	}

	/**
	 * Connections by default create ResultSet objects with holdability true. This method can be used
	 * to change the holdability of the connection by passing one of ResultSet.HOLD_CURSORS_OVER_COMMIT
	 * or ResultSet.CLOSE_CURSORS_AT_COMMIT. We implement this using reflection in jdk13 and lower
	 *
	 * @param conn			The connection.
	 * @param holdType	The new holdability for the Connection object.
	 *
	 * @return	The connection object with holdability set to passed value.
	 */
	public Connection setHoldability(Connection conn, int holdType)
		throws SQLException
	{
    //Prior to db2 compatibility work, the default holdability for connections was close cursors over commit and all the tests
    //were written based on that assumption
    //Later, as part of db2 compatibility, we changed the default holdability for connection to hold cursors over commit.
    //But in order for the existing tests to work fine, the tests needed a way to set the holdability to close cursors for connections
    //Since there is no direct jdbc api in jdk13 and lower to do that, we are using reflection to set the holdability to close cursors
    try { //for jdks prior to jdk14, need to use reflection to set holdability to false. 
    	Method sh = conn.getClass().getMethod("setHoldability", CONN_PARAM);
    	sh.invoke(conn, CONN_ARG);
    } catch( Exception e) {
    	throw PublicAPI.wrapStandardException( StandardException.plainWrapException( e));
    }
    return conn;
	}

	/**
	 * Retrieves the current holdability of ResultSet objects created using this
	 * Connection object. We implement this using reflection in jdk13 and lower
	 *
	 * @return  The holdability, one of ResultSet.HOLD_CURSORS_OVER_COMMIT
	 * or ResultSet.CLOSE_CURSORS_AT_COMMIT
	 *
	 */
	public int getHoldability(Connection conn)
		throws SQLException
	{
    //this method is used to make sure we are not trying to create a statement with holdability different than the connection holdability
    //This is because jdk13 and lower does not have support for that.
    //The holdability of connection and statement can differ if connection holdability is set to close cursor on commit using reflection
    //and statement is getting created with holdability true
    //Another instance of holdability of connection and statement not being same is when connection holdability is hold cursor
    //over commit and statement is being created with holdability false
    int defaultHoldability = JDBC30Translation.HOLD_CURSORS_OVER_COMMIT;
    try {
    	Method sh = conn.getClass().getMethod("getHoldability", null);
    	defaultHoldability = ((Integer)sh.invoke(conn, null)).intValue();
    } catch( Exception e) {
    	throw PublicAPI.wrapStandardException( StandardException.plainWrapException( e));
    }
    return defaultHoldability;
	}

	/**
	 * Create the right kind of statement (scrolling or not)
	 * off of the specified connection.
	 *
	 * @param conn			The connection.
	 * @param scrollType	The scroll type of the cursor.
	 *
	 * @return	The statement.
	 */
	public Statement createStatement(Connection conn, int scrollType, int holdType)
		throws SQLException
	{
    	//following if is used to make sure we are not trying to create a statement with holdability different that the connection
    	//holdability. This is because jdk13 and lower does not have support for that.
    	//The holdability of connection and statement can differ if connection holdability is set to close cursor on commit using reflection
    	//and statement is getting created with holdability true
    	//Another instance of holdability of connection and statement not being same is when connection holdability is hold cursor
    	//over commit and statement is being created with holdability false
    	if (holdType != getHoldability(conn))
    	{
        	throw ijException.holdCursorsNotSupported();
    	}
      
    	Statement stmt;
        try {
        	stmt = conn.createStatement(scrollType, JDBC20Translation.CONCUR_READ_ONLY);
        } catch(AbstractMethodError ame) {
        //because weblogic 4.5 doesn't yet implement jdbc 2.0 interfaces, need to go back
        //to jdbc 1.x functionality
        	stmt = conn.createStatement();
        }
		return stmt;
	}

	/**
	 * Position on the specified row of the specified ResultSet.
	 *
	 * @param rs	The specified ResultSet.
	 * @param row	The row # to move to.
	 *				(Negative means from the end of the result set.)
	 *
	 * @return	NULL.
	 *
	 * @exception	SQLException thrown on error.
	 *				(absolute() not supported pre-JDBC2.0)
	 */
	public ijResult absolute(ResultSet rs, int row)
		throws SQLException
	{
        boolean forwardOnly;
    	try {
		// absolute is only allowed on scroll cursors
		    forwardOnly = (rs.getStatement().getResultSetType() == JDBC20Translation.TYPE_FORWARD_ONLY);
        } catch (AbstractMethodError ame) {
        //because weblogic 4.5 doesn't yet implement jdbc 2.0 interfaces, need to go back
        //to jdbc 1.x functionality
            forwardOnly = true;
        }
        if (forwardOnly)
		{
			throw ijException.forwardOnlyCursor("ABSOLUTE");
		}

		// 0 is an invalid value for row
		if (row == 0)
		{
			throw ijException.zeroInvalidForAbsolute();
		}

		return new ijRowResult(rs, rs.absolute(row));
	}

	/**
	 * Move the cursor position by the specified amount.
	 *
	 * @param rs	The specified ResultSet.
	 * @param row	The # of rows to move.
	 *				(Negative means toward the beginning of the result set.)
	 *
	 * @return	NULL.
	 *
	 * @exception	SQLException thrown on error.
	 *				(relative() not supported pre-JDBC2.0)
	 */
	public ijResult relative(ResultSet rs, int row)
		throws SQLException
	{
    	boolean forwardOnly;
        try {
        	forwardOnly = (rs.getStatement().getResultSetType() == JDBC20Translation.TYPE_FORWARD_ONLY);
        } catch (AbstractMethodError ame) {
        //because weblogic 4.5 doesn't yet implement jdbc 2.0 interfaces, need to go back
        //to jdbc 1.x functionality
            forwardOnly = true;
        }
		// relative is only allowed on scroll cursors
		if (forwardOnly)
		{
			throw ijException.forwardOnlyCursor("RELATIVE");
		}

		return new ijRowResult(rs, rs.relative(row));
	}

	/**
	 * Position before the first row of the specified ResultSet
	 * and return NULL to the user.
	 *
	 * @param rs	The specified ResultSet.
	 *
	 * @return	NULL.
	 *
	 * @exception	SQLException thrown on error.
	 *				(beforeFirst() not supported pre-JDBC2.0)
	 */
	public ijResult beforeFirst(ResultSet rs)
		throws SQLException
	{
    	boolean forwardOnly;
        try {
        	forwardOnly = (rs.getStatement().getResultSetType() == JDBC20Translation.TYPE_FORWARD_ONLY);
        } catch (AbstractMethodError ame) {
        //because weblogic 4.5 doesn't yet implement jdbc 2.0 interfaces, need to go back
        //to jdbc 1.x functionality
            forwardOnly = true;
        }
		// before first is only allowed on scroll cursors
		if (forwardOnly)
		{
			throw ijException.forwardOnlyCursor("BEFORE FIRST");
		}

		rs.beforeFirst();
		return new ijRowResult(rs, false);
	}

	/**
	 * Position on the first row of the specified ResultSet
	 * and return that row to the user.
	 *
	 * @param rs	The specified ResultSet.
	 *
	 * @return	The first row of the ResultSet.
	 *
	 * @exception	SQLException thrown on error.
	 *				(first() not supported pre-JDBC2.0)
	 */
	public ijResult first(ResultSet rs)
		throws SQLException
	{
    	boolean forwardOnly;
        try {
        	forwardOnly = (rs.getStatement().getResultSetType() == JDBC20Translation.TYPE_FORWARD_ONLY);
        } catch (AbstractMethodError ame) {
        //because weblogic 4.5 doesn't yet implement jdbc 2.0 interfaces, need to go back
        //to jdbc 1.x functionality
            forwardOnly = true;
        }
		// first is only allowed on scroll cursors
		if (forwardOnly)
		{
			throw ijException.forwardOnlyCursor("FIRST");
		}

		return new ijRowResult(rs, rs.first());
	}

	/**
	 * Position after the last row of the specified ResultSet
	 * and return NULL to the user.
	 *
	 * @param rs	The specified ResultSet.
	 *
	 * @return	NULL.
	 *
	 * @exception	SQLException thrown on error.
	 *				(afterLast() not supported pre-JDBC2.0)
	 */
	public ijResult afterLast(ResultSet rs)
		throws SQLException
	{
    	boolean forwardOnly;
        try {
        	forwardOnly = (rs.getStatement().getResultSetType() == JDBC20Translation.TYPE_FORWARD_ONLY);
        } catch (AbstractMethodError ame) {
        //because weblogic 4.5 doesn't yet implement jdbc 2.0 interfaces, need to go back
        //to jdbc 1.x functionality
            forwardOnly = true;
        }
		// after last is only allowed on scroll cursors
		if (forwardOnly)
		{
			throw ijException.forwardOnlyCursor("AFTER LAST");
		}

		rs.afterLast();
		return new ijRowResult(rs, false);
	}

	/**
	 * Position on the last row of the specified ResultSet
	 * and return that row to the user.
	 *
	 * @param rs	The specified ResultSet.
	 *
	 * @return	The last row of the ResultSet.
	 *
	 * @exception	SQLException thrown on error.
	 *				(last() not supported pre-JDBC2.0)
	 */
	public ijResult last(ResultSet rs)
		throws SQLException
	{
    	boolean forwardOnly;
        try {
        	forwardOnly = (rs.getStatement().getResultSetType() == JDBC20Translation.TYPE_FORWARD_ONLY);
        } catch (AbstractMethodError ame) {
        //because weblogic 4.5 doesn't yet implement jdbc 2.0 interfaces, need to go back
        //to jdbc 1.x functionality
            forwardOnly = true;
        }
		// last is only allowed on scroll cursors
		if (forwardOnly)
		{
			throw ijException.forwardOnlyCursor("LAST");
		}

		return new ijRowResult(rs, rs.last());
	}

	/**
	 * Position on the previous row of the specified ResultSet
	 * and return that row to the user.
	 *
	 * @param rs	The specified ResultSet.
	 *
	 * @return	The previous row of the ResultSet.
	 *
	 * @exception	SQLException thrown on error.
	 *				(previous() not supported pre-JDBC2.0)
	 */
	public ijResult previous(ResultSet rs)
		throws SQLException
	{
    	boolean forwardOnly;
        try {
        	forwardOnly = (rs.getStatement().getResultSetType() == JDBC20Translation.TYPE_FORWARD_ONLY);
        } catch (AbstractMethodError ame) {
        //because weblogic 4.5 doesn't yet implement jdbc 2.0 interfaces, need to go back
        //to jdbc 1.x functionality
            forwardOnly = true;
        }
		// first is only allowed on scroll cursors
		if (forwardOnly)
		{
			throw ijException.forwardOnlyCursor("PREVIOUS");
		}

		return new ijRowResult(rs, rs.previous());
	}

	/**
	 * Get the current row number
	 *
	 * @param rs	The specified ResultSet.
	 *
	 * @return	The current row number
	 *
	 * @exception	SQLException thrown on error.
	 *				(getRow() not supported pre-JDBC2.0)
	 */
	public int getCurrentRowNumber(ResultSet rs)
		throws SQLException
	{
		boolean forwardOnly;
		try 
		{
			forwardOnly = (rs.getStatement().getResultSetType() == JDBC20Translation.TYPE_FORWARD_ONLY);
		} catch (AbstractMethodError ame) 
		{
			//because weblogic 4.5 doesn't yet implement jdbc 2.0 interfaces, need to go back
			//to jdbc 1.x functionality
			forwardOnly = true;
        }

		// getCurrentRow is only allowed on scroll cursors
		if (forwardOnly)
		{
			throw ijException.forwardOnlyCursor("GETCURRENTROWNUMBER");
		}

		return rs.getRow();
	}

	public Properties getConnAttributeDefaults ()
	{
		return connAttributeDefaults;
	}

	public final Object run() {
		return  getClass().getResourceAsStream(ProductGenusNames.TOOLS_INFO);
	}
}
