/*

   Derby - Class org.apache.derby.iapi.jdbc.BrokeredStatement

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.jdbc;

import org.apache.derby.iapi.reference.JDBC30Translation;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.error.PublicAPI;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import java.lang.reflect.*;

/**
	A Statement implementation that forwards all of its requests to an underlying Statement.
 */
public class BrokeredStatement implements Statement
{

	/**
		My control. Use the controlCheck() method to obtain the control
		when calling a check method. This will result in the correct exception
		being thrown if the statement is already closed.
	*/
	protected final BrokeredStatementControl control;

	protected final int jdbcLevel;
	protected final int resultSetType;
	protected final int resultSetConcurrency;
	protected final int resultSetHoldability;

	/**
		My state
	*/
	private String cursorName;
	private Boolean escapeProcessing;

    protected BrokeredStatement(BrokeredStatementControl control, int jdbcLevel) throws SQLException
    {
		this.control = control;
		this.jdbcLevel = jdbcLevel;

		// save the state of the Statement while we are pretty much guaranteed the
		// underlying statement is open.
		resultSetType = getResultSetType();
		resultSetConcurrency = getResultSetConcurrency();

		resultSetHoldability = getResultSetHoldability();
    }


    public final void addBatch(String sql)
              throws SQLException
    {
		getStatement().addBatch( sql);
	}

    public final void clearBatch()
        throws SQLException
    {
           getStatement().clearBatch();
	}

    public final int[] executeBatch()
        throws SQLException
    {
		return getStatement().executeBatch();
	}


    public final void cancel()
        throws SQLException
    {
        getStatement().cancel();
    }

    public final boolean execute(String sql) throws SQLException
	{
		controlCheck().checkHoldCursors(resultSetHoldability);
		return getStatement().execute(sql);
    } 

    public final ResultSet executeQuery(String sql) throws SQLException
	{
		controlCheck().checkHoldCursors(resultSetHoldability);
		return wrapResultSet(getStatement().executeQuery(sql));
    }

    public final int executeUpdate(String sql) throws SQLException
	{
		return getStatement().executeUpdate(sql);
    }

        
    /**
     * In many cases, it is desirable to immediately release a
     * Statements's database and JDBC resources instead of waiting for
     * this to happen when it is automatically closed; the close
     * method provides this immediate release.
     *
     * <P><B>Note:</B> A Statement is automatically closed when it is
     * garbage collected. When a Statement is closed its current
     * ResultSet, if one exists, is also closed.
	 * @exception SQLException thrown on failure.
     */
	public final void close() throws SQLException
    {
		getStatement().close();
    }

    public final Connection getConnection()
        throws SQLException
    {
		return getStatement().getConnection();
	}

    public final int getFetchDirection()
        throws SQLException
    {
		return getStatement().getFetchDirection();
	}

    public final int getFetchSize()
        throws SQLException
    {
		return getStatement().getFetchSize();
	}

    public final int getMaxFieldSize()
        throws SQLException
    {
		return getStatement().getMaxFieldSize();
	}

    public final int getMaxRows()
        throws SQLException
    {
		return getStatement().getMaxRows();
	}

    public final int getResultSetConcurrency()
        throws SQLException
    {
		return getStatement().getResultSetConcurrency();
	}

    /**
     * The maxFieldSize limit (in bytes) is set to limit the size of
     * data that can be returned for any column value; it only applies
     * to BINARY, VARBINARY, LONGVARBINARY, CHAR, VARCHAR, and
     * LONGVARCHAR fields.  If the limit is exceeded, the excess data
     * is silently discarded.
     *
     * @param max the new max column size limit; zero means unlimited
	 * @exception SQLException thrown on failure.
     */
	public final void setMaxFieldSize(int max) throws SQLException
    {
        getStatement().setMaxFieldSize(max);
	}

    /**
     * The maxRows limit is set to limit the number of rows that any
     * ResultSet can contain.  If the limit is exceeded, the excess
     * rows are silently dropped.
     *
     * @param max the new max rows limit; zero means unlimited
	 * @exception SQLException thrown on failure.
     */
	public final void setMaxRows(int max) throws SQLException	
	{
        getStatement().setMaxRows( max);
    }

    /**
     * If escape scanning is on (the default) the driver will do
     * escape substitution before sending the SQL to the database.
     *
     * @param enable true to enable; false to disable
	 * @exception SQLException thrown on failure.
     */
	public final void setEscapeProcessing(boolean enable) throws SQLException
    {
        getStatement().setEscapeProcessing( enable);
		escapeProcessing = enable ? Boolean.TRUE : Boolean.FALSE;
	}

    /**
     * The first warning reported by calls on this Statement is
     * returned.  A Statment's execute methods clear its SQLWarning
     * chain. Subsequent Statement warnings will be chained to this
     * SQLWarning.
     *
     * <p>The warning chain is automatically cleared each time
     * a statement is (re)executed.
     *
     * <P><B>Note:</B> If you are processing a ResultSet then any
     * warnings associated with ResultSet reads will be chained on the
     * ResultSet object.
     *
     * @return the first SQLWarning or null
	 * @exception SQLException thrown on failure.
     */
	public final SQLWarning getWarnings() throws SQLException
    {
		return getStatement().getWarnings();
	}

    /**
     * After this call getWarnings returns null until a new warning is
     * reported for this Statement.
	 * @exception SQLException thrown on failure.
     */
	public final void clearWarnings() throws SQLException
    {
		getStatement().clearWarnings();
    }

    /**
     * setCursorName defines the SQL cursor name that will be used by
     * subsequent Statement execute methods. This name can then be
     * used in SQL positioned update/delete statements to identify the
     * current row in the ResultSet generated by this getStatement().  If
     * the database doesn't support positioned update/delete, this
     * method is a noop.
     *
     * <P><B>Note:</B> By definition, positioned update/delete
     * execution must be done by a different Statement than the one
     * which generated the ResultSet being used for positioning. Also,
     * cursor names must be unique within a Connection.
     *
     * @param name the new cursor name.
     */
	public final void setCursorName(String name) throws SQLException
    {
		getStatement().setCursorName( name);
		cursorName = name;
	}
    
    
    /**
     *  getResultSet returns the current result as a ResultSet.  It
     *  should only be called once per result.
     *
     * @return the current result as a ResultSet; null if the result
     * is an update count or there are no more results or the statement
	 * was closed.
     * @see #execute
     */
	public final ResultSet getResultSet() throws SQLException
    {
        return wrapResultSet(getStatement().getResultSet());
    }
    
    /**
     *  getUpdateCount returns the current result as an update count;
     *  if the result is a ResultSet or there are no more results -1
     *  is returned.  It should only be called once per result.
     *
     * <P>The only way to tell for sure that the result is an update
     *  count is to first test to see if it is a ResultSet. If it is
     *  not a ResultSet it is either an update count or there are no
     *  more results.
     *
     * @return the current result as an update count; -1 if it is a
     * ResultSet or there are no more results
     * @see #execute
     */
	public final int getUpdateCount()	throws SQLException
    {
        return getStatement().getUpdateCount();
    }

    /**
     * getMoreResults moves to a Statement's next result.  It returns true if
     * this result is a ResultSet.  getMoreResults also implicitly
     * closes any current ResultSet obtained with getResultSet.
     *
     * There are no more results when (!getMoreResults() &&
     * (getUpdateCount() == -1)
     *
     * @return true if the next result is a ResultSet; false if it is
     * an update count or there are no more results
     * @see #execute
	 * @exception SQLException thrown on failure.
     */
	public final boolean getMoreResults() throws SQLException
    {
        return getStatement().getMoreResults();
    }

    /**
     * JDBC 2.0
     *
     * Determine the result set type.
     *
     * @exception SQLException Feature not implemented for now.
     */
    public final int getResultSetType()
		throws SQLException 
	{
        return getStatement().getResultSetType();
    }

    /**
     * JDBC 2.0
     *
     * Give a hint as to the direction in which the rows in a result set
     * will be processed. The hint applies only to result sets created
     * using this Statement object.  The default value is 
     * ResultSet.FETCH_FORWARD.
     *
     * @param direction the initial direction for processing rows
     * @exception SQLException if a database-access error occurs or direction
     * is not one of ResultSet.FETCH_FORWARD, ResultSet.FETCH_REVERSE, or
     * ResultSet.FETCH_UNKNOWN
     */
    public final void setFetchDirection(int direction) throws SQLException
    {
        getStatement().setFetchDirection( direction);
    }

    /**
     * JDBC 2.0
     *
     * Give the JDBC driver a hint as to the number of rows that should
     * be fetched from the database when more rows are needed.  The number 
     * of rows specified only affects result sets created using this 
     * getStatement(). If the value specified is zero, then the hint is ignored.
     * The default value is zero.
     *
     * @param rows the number of rows to fetch
     * @exception SQLException if a database-access error occurs, or the
     * condition 0 <= rows <= this.getMaxRows() is not satisfied.
     */
    public final void setFetchSize(int rows) throws SQLException
    {
        getStatement().setFetchSize( rows);
    }

    public final int getQueryTimeout()
        throws SQLException
    {
        return getStatement().getQueryTimeout();
    }

    public final void setQueryTimeout(int seconds)
        throws SQLException
    {
        getStatement().setQueryTimeout( seconds);
    }


	/*
	** JDBC 3.0 methods
	*/
	public final boolean execute(String sql,
                           int autoGeneratedKeys)
        throws SQLException
    {

        return  getStatement().execute( sql, autoGeneratedKeys);
    }


    public final boolean execute(String sql,
                           int[] columnIndexes)
        throws SQLException
    {
        return getStatement().execute( sql, columnIndexes);
    }

    public final boolean execute(String sql,
                           String[] columnNames)
        throws SQLException
    {
        return getStatement().execute( sql, columnNames);
    }

    public final int executeUpdate(String sql,
                           int autoGeneratedKeys)
        throws SQLException
    {
                int retVal =  getStatement().executeUpdate( sql, autoGeneratedKeys);
                return retVal;
    }

    public final int executeUpdate(String sql,
                           int[] columnIndexes)
        throws SQLException
    {
             return  getStatement().executeUpdate( sql, columnIndexes);
    }

    public final int executeUpdate(String sql,
                           String[] columnNames)
        throws SQLException
    {

        return getStatement().executeUpdate( sql, columnNames);
    }



    /**
     * JDBC 3.0
     *
     * Moves to this Statement obect's next result, deals with any current ResultSet
     * object(s) according to the instructions specified by the given flag, and
     * returns true if the next result is a ResultSet object
     *
     * @param current - one of the following Statement constants indicating what
     * should happen to current ResultSet objects obtained using the method
     * getResultSetCLOSE_CURRENT_RESULT, KEEP_CURRENT_RESULT, or CLOSE_ALL_RESULTS
     * @return true if the next result is a ResultSet; false if it is
     * an update count or there are no more results
     * @see #execute
     * @exception SQLException thrown on failure.
     */
	public final boolean getMoreResults(int current) throws SQLException
    {
        return getStatement().getMoreResults( current);
	}

    /**
     * JDBC 3.0
     *
     * Retrieves any auto-generated keys created as a result of executing this
     * Statement object. If this Statement object did not generate any keys, an empty
     * ResultSet object is returned. If this Statement is a non-insert statement,
     * an exception will be thrown.
     *
     * @return a ResultSet object containing the auto-generated key(s) generated by
     * the execution of this Statement object
     * @exception SQLException if a database access error occurs
     */
	public final ResultSet getGeneratedKeys() throws SQLException
    {
        return wrapResultSet(getStatement().getGeneratedKeys());
    }

	public final int getResultSetHoldability()
        throws SQLException
	{
		int defaultHoldability = JDBC30Translation.HOLD_CURSORS_OVER_COMMIT;
		if (org.apache.derby.iapi.services.info.JVMInfo.JDK_ID > 2) { //No need to use reflection for jdks 1.4 and higher
			defaultHoldability = getStatement().getResultSetHoldability();
		} else {
			try {
				Method sh = getStatement().getClass().getMethod("getResultSetHoldability", null);
				defaultHoldability = ((Integer)sh.invoke(getStatement(), null)).intValue();
			} catch( Exception e)
			{
				throw PublicAPI.wrapStandardException( StandardException.plainWrapException( e));
			}
		}
		return defaultHoldability;
	}

	/*
	** Control methods
	*/

	public Statement createDuplicateStatement(Connection conn, Statement oldStatement) throws SQLException {

		Statement newStatement;
		
		if (jdbcLevel == 2)
			newStatement = conn.createStatement(resultSetType, resultSetConcurrency);
		else
			newStatement = conn.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);

		setStatementState(oldStatement, newStatement);

		return newStatement;
	}

	protected void setStatementState(Statement oldStatement, Statement newStatement) throws SQLException {
		if (cursorName != null)
			newStatement.setCursorName(cursorName);
		if (escapeProcessing != null)
			newStatement.setEscapeProcessing(escapeProcessing.booleanValue());

		newStatement.setFetchDirection(oldStatement.getFetchDirection());
		newStatement.setFetchSize(oldStatement.getFetchSize());
		newStatement.setMaxFieldSize(oldStatement.getMaxFieldSize());
		newStatement.setMaxRows(oldStatement.getMaxRows());
		newStatement.setQueryTimeout(oldStatement.getQueryTimeout());
	}

	protected Statement getStatement() throws SQLException {
		return control.getRealStatement();
	}
	protected final ResultSet wrapResultSet(ResultSet rs) {
		return control.wrapResultSet(rs);
	}

	/**
		Get the BrokeredStatementControl in order to perform a check.
		Obtained indirectly to ensure that the correct exception is
		thrown if the Statement has been closed.
	*/
	protected final BrokeredStatementControl controlCheck() throws SQLException
	{
		// simplest method that will throw an exception if the Statement is closed
		getStatement().getConnection();
		return control;
	}
}
