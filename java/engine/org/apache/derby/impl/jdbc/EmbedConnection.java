/*

   Derby - Class org.apache.derby.impl.jdbc.EmbedConnection

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

package org.apache.derby.impl.jdbc;

import org.apache.derby.jdbc.Driver169;

import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.iapi.reference.JDBC20Translation;
import org.apache.derby.iapi.reference.JDBC30Translation;
import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.jdbc.AuthenticationService;

import org.apache.derby.iapi.db.Database;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;

/* can't import due to name overlap:
import java.sql.Connection;
import java.sql.ResultSet;
*/
import java.sql.PreparedStatement;
import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import java.util.Properties;

/**
 * Local implementation of Connection for a JDBC driver in 
 * the same process as the database.
 * <p> 
 * There is always a single root (parent) connection.  The
 * initial JDBC connection is the root connection. A
 * call to <I>getCurrentConnection()</I> or with the URL 
 * <I>jdbc:default:connection</I> yields a nested connection that shares
 * the same root connection as the parent.  A nested connection
 * is implemented using this class.  The nested connection copies the 
 * state of the parent connection and shares some of the same 
 * objects (e.g. ContextManager) that are shared across all
 * nesting levels.  The proxy also maintains its own
 * state that is distinct from its parent connection (e.g.
 * autocommit or warnings).
 * <p>
 * <B>SYNCHRONIZATION</B>: Just about all JDBC actions are
 * synchronized across all connections stemming from the
 * same root connection.  The synchronization is upon
 * the a synchronized object return by the rootConnection.
 * 
 *	@author djd
 *
 * @see TransactionResourceImpl
 *
 */
public abstract class EmbedConnection implements java.sql.Connection
{

	private static final StandardException exceptionClose = StandardException.closeException();

	//////////////////////////////////////////////////////////
	// OBJECTS SHARED ACROSS CONNECTION NESTING
	//////////////////////////////////////////////////////////
	protected DatabaseMetaData dbMetadata;

	protected final TransactionResourceImpl tr; // always access tr thru getTR()


	//////////////////////////////////////////////////////////
	// STATE (copied to new nested connections, but nesting
	// specific)
	//////////////////////////////////////////////////////////
	private boolean	active;
	protected boolean	autoCommit = true;
	boolean	needCommit;
	//following is a new feature in JDBC3.0 where you can specify the holdability
	//of a resultset at the end of the transaction. This gets set by the
	//new method setHoldability(int) in JDBC3.0
	protected int	connectionHoldAbility = JDBC30Translation.HOLD_CURSORS_OVER_COMMIT;


	//////////////////////////////////////////////////////////
	// NESTING SPECIFIC OBJECTS
	//////////////////////////////////////////////////////////
	/*
	** The root connection is the base connection upon
	** which all actions are synchronized.  By default,
	** we are the root connection unless we are created
	** by copying the state from another connection.
	*/
	protected final EmbedConnection rootConnection;
	private SQLWarning 		topWarning;
	/**	
		Factory for JDBC objects to be created.
	*/
	public Driver169 factory;

	/**
		The Connection object the application is using when accessing the
		database through this connection. In most cases this will be equal
		to this. When Connection pooling is being used, then it will
		be set to the Connection object handed to the application.
		It is used for the getConnection() methods of various JDBC objects.
	*/
	private java.sql.Connection applicationConnection;

	/**
		An increasing counter to assign to a ResultSet on its creation.
		Used for ordering ResultSets returned from a procedure, always
		returned in order of their creation. Is maintained at the root connection.
	*/
	private int resultSetId;


	//////////////////////////////////////////////////////////
	// CONSTRUCTORS
	//////////////////////////////////////////////////////////

	// create a new Local Connection, using a new context manager
	//
	public EmbedConnection(Driver169 driver, String url, Properties info)
		 throws SQLException
	{
		// Create a root connection.
		applicationConnection = rootConnection = this;
		factory = driver;


		tr = new TransactionResourceImpl(driver, url, info);

		active = true;

		// register this thread and its context manager with
		// the global context service
		setupContextStack();

		try {

			// stick my context into the context manager
			EmbedConnectionContext context = pushConnectionContext(tr.getContextManager());

			// if we are shutting down don't attempt to boot or create the database
			boolean shutdown = Boolean.valueOf(info.getProperty(Attribute.SHUTDOWN_ATTR)).booleanValue();

			// see if database is already booted
			Database database = (Database) Monitor.findService(Property.DATABASE_MODULE, tr.getDBName());

			// See if user wants to create a new database.
			boolean	createBoot = createBoot(info);	
			if (database != null)
			{
				// database already booted by someone else
				tr.setDatabase(database);
			}
			else if (!shutdown)
			{
				// Return false iff the monitor cannot handle a service of the type
				// indicated by the proptocol within the name.  If that's the case
				// then we are the wrong driver.
				if (!bootDatabase(info))
				{
					tr.clearContextInError();
					setInactive();
					return;
				}
			}


			if (createBoot && !shutdown)
			{
				// if we are shutting down don't attempt to boot or create the database

				if (tr.getDatabase() != null) {
					addWarning(EmbedSQLWarning.newEmbedSQLWarning(SQLState.DATABASE_EXISTS, getDBName()));
				} else {

					// check for user's credential and authenticate the user
					// with system level authentication service.
					// FIXME: We should also check for CREATE DATABASE operation
					//		  authorization for the user if authorization was
					//		  set at the system level.
					//		  Right now, the authorization service does not
					//		  restrict/account for Create database op.
					checkUserCredentials(null, info);
					
					// Process with database creation
					database = createDatabase(tr.getDBName(), info);
					tr.setDatabase(database);
				}
			}


			if (tr.getDatabase() == null) {
				String dbname = tr.getDBName();
				// do not clear the TransactionResource context. It will be restored
                // as part of the finally clause below.
				this.setInactive();
				throw newSQLException(SQLState.DATABASE_NOT_FOUND, dbname);
			}


			// Check User's credentials and if it is a valid user of
			// the database
			//
			checkUserCredentials(tr.getDBName(), info);

			// Make a real connection into the database, setup lcc, tc and all
			// the rest.
			tr.startTransaction();

			// now we have the database connection, we can shut down
			if (shutdown) {
				throw tr.shutdownDatabaseException();
			}

		} catch (Throwable t) {
			throw handleException(t);
		} finally {
			restoreContextStack();
			info = null;
		}
	}


	/**
	  Examine the attributes set provided and determine if this is a create
	  boot. A boot is a create boot iff.

	  <OL>
	  <LI>create=true - This means create a standard database.
	  <LI> createFrom = Path - creates database from backup if it does not exist.
	  <LI> restoreFrom = Path - database is restored completley from backup.
           if a database exists in the same place it is replaced by the version
		   in the backup otherwise a new one is created using the backup copy.
      <LI> rollForwardRecoveryFrom = Path  - rollforward is performed 
      using the version backup and any active and archived log files.
	  </OL>

	  @param a the attribute set.

	  @exception SQLException Ooops.
	  */
	private boolean createBoot(Properties p) throws SQLException
	{
		int createCount = 0;

		if (Boolean.valueOf(p.getProperty(Attribute.CREATE_ATTR)).booleanValue())
			createCount++;

		int restoreCount=0;
		//check if the user has specified any /create/restore/recover from backup attributes.
		if (p.getProperty(Attribute.CREATE_FROM) != null)
			restoreCount++;
		if (p.getProperty(Attribute.RESTORE_FROM) != null)
			restoreCount++;
		if (p.getProperty(Attribute.ROLL_FORWARD_RECOVERY_FROM)!=null)
			restoreCount++;
		if(restoreCount > 1)
			throw newSQLException(SQLState.CONFLICTING_RESTORE_ATTRIBUTES);
	
		//add the restore count to create count to make sure 
		//user has not specified and restore together by mistake.
		createCount = createCount + restoreCount ;

		//
		if (createCount > 1) throw newSQLException(SQLState.CONFLICTING_CREATE_ATTRIBUTES);
		
		//retuns true only for the  create flag not for restore flags
		return (createCount - restoreCount) == 1;
	}

	/**
	 * Create a new connection based off of the 
	 * connection passed in.  Initializes state
	 * based on input connection, and copies 
	 * appropriate object pointers. This is only used
	   for nested connections.
	 *
	 * @param inputConnection the input connection
	 */
	public EmbedConnection(EmbedConnection inputConnection) 
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(inputConnection.active, 
			"trying to create a proxy for an inactive conneciton");
		}

		// Proxy connections are always autocommit false
		// thus needCommit is irrelavent.
		autoCommit = false;


		/*
		** Nesting specific state we are copying from 
		** the inputConnection
		*/

		/*
		** Objects we are sharing across nestings
		*/
		// set it to null to allow it to be final.
		tr = null;			// a proxy connection has no direct
								// pointer to the tr.  Every call has to go
								// thru the rootConnection's tr.
		active = true;
		this.rootConnection = inputConnection.rootConnection;
		this.applicationConnection = this;
		this.factory = inputConnection.factory;

		//if no holdability specified for the resultset, use the holability
		//defined for the connection
		this.connectionHoldAbility = inputConnection.connectionHoldAbility;

		//RESOLVE: although it looks like the right
		// thing to share the metadata object, if
		// we do we'll get the wrong behavior on
		// getCurrentConnection().getMetaData().isReadOnly()
		// so don't try to be smart and uncomment the
		// following.  Ultimately, the metadata should
		// be shared by all connections anyway.
		//dbMetadata = inputConnection.dbMetadata;
	}

	//
	// Check passed-in user's credentials.
	//
	private void checkUserCredentials(String dbname,
									  Properties userInfo)
	  throws SQLException
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isClosed(), "connection is closed");

		// If a database name was passed-in then check user's credential
		// in that database using the database's authentication service,
		// otherwise check if it is a valid user in the JBMS system.
		//
		// NOTE: We always expect an authentication service per database
		// and one at the system level.
		//
		AuthenticationService authenticationService = null;

		// Retrieve appropriate authentication service handle
		if (dbname == null)
			authenticationService = getLocalDriver().getAuthenticationService();
		else
			authenticationService = getTR().getDatabase().getAuthenticationService();

		// check that we do have a authentication service
		// it is _always_ expected.
		if (authenticationService == null)
		{
			String failedString = MessageService.getTextMessage(
				(dbname == null) ? MessageId.AUTH_NO_SERVICE_FOR_SYSTEM : MessageId.AUTH_NO_SERVICE_FOR_DB);

			throw newSQLException(SQLState.LOGIN_FAILED, failedString);
		}
		
		// Let's authenticate now
			
		if (!authenticationService.authenticate(
											   dbname,
											   userInfo
											   )) {

			throw newSQLException(SQLState.LOGIN_FAILED, MessageService.getTextMessage(MessageId.AUTH_INVALID));

		}
	}

    /**
     * Gets the EngineType of the connected database.
     *
     * @return 0 if there is no database, the engine type otherwise. @see org.apache.derby.iapi.reference.EngineType
     */
    public int getEngineType()
    {
        Database db = getDatabase();

        if( null == db)
            return 0;
        return db.getEngineType();
    }
    
	/*
	** Methods from java.sql.Connection
	*/

    /**
	 * SQL statements without parameters are normally
     * executed using Statement objects. If the same SQL statement 
     * is executed many times, it is more efficient to use a 
     * PreparedStatement
     *
     * JDBC 2.0
     *
     * Result sets created using the returned Statement will have
     * forward-only type, and read-only concurrency, by default.
     *
     * @return a new Statement object 
     * @exception SQLException if a database-access error occurs.
     */
	public final Statement createStatement() throws SQLException 
	{
		return createStatement(JDBC20Translation.TYPE_FORWARD_ONLY,
							   JDBC20Translation.CONCUR_READ_ONLY,
							   connectionHoldAbility);
	}

    /**
     * JDBC 2.0
     *
     * Same as createStatement() above, but allows the default result set
     * type and result set concurrency type to be overridden.
     *
     * @param resultSetType a result set type, see ResultSet.TYPE_XXX
     * @param resultSetConcurrency a concurrency type, see ResultSet.CONCUR_XXX
     * @return a new Statement object 
      * @exception SQLException if a database-access error occurs.
    */
    public final Statement createStatement(int resultSetType,
    								 int resultSetConcurrency) 
						throws SQLException
	{
		return createStatement(resultSetType, resultSetConcurrency,
			    connectionHoldAbility);
	}

    /**
     * JDBC 3.0
     *
     * Same as createStatement() above, but allows the default result set
     * type, result set concurrency type and result set holdability type to
     * be overridden.
     *
     * @param resultSetType a result set type, see ResultSet.TYPE_XXX
     * @param resultSetConcurrency a concurrency type, see ResultSet.CONCUR_XXX
     * @param resultSetHoldability a holdability type,
     *  ResultSet.HOLD_CURSORS_OVER_COMMIT or ResultSet.CLOSE_CURSORS_AT_COMMIT
     * @return a new Statement object
     * @exception SQLException if a database-access error occurs.
     */
    public final Statement createStatement(int resultSetType,
    								 int resultSetConcurrency,
    								 int resultSetHoldability)
						throws SQLException
	{
		if (isClosed())
			throw Util.noCurrentConnection();

		return factory.newEmbedStatement(this, false,
			setResultSetType(resultSetType), setResultSetConcurrency(resultSetConcurrency),
			resultSetHoldability);
	}

    /**
     * A SQL statement with or without IN parameters can be
     * pre-compiled and stored in a PreparedStatement object. This
     * object can then be used to efficiently execute this statement
     * multiple times.
     *
     * <P><B>Note:</B> This method is optimized for handling
     * parametric SQL statements that benefit from precompilation. If
     * the driver supports precompilation, prepareStatement will send
     * the statement to the database for precompilation. Some drivers
     * may not support precompilation. In this case, the statement may
     * not be sent to the database until the PreparedStatement is
     * executed.  This has no direct affect on users; however, it does
     * affect which method throws certain SQLExceptions.
     *
     * JDBC 2.0
     *
     * Result sets created using the returned PreparedStatement will have
     * forward-only type, and read-only concurrency, by default.
     *
     * @param sql a SQL statement that may contain one or more '?' IN
     * parameter placeholders
     * @return a new PreparedStatement object containing the
     * pre-compiled statement 
     * @exception SQLException if a database-access error occurs.
     */
    public final PreparedStatement prepareStatement(String sql)
	    throws SQLException
	{
		return prepareStatement(sql,JDBC20Translation.TYPE_FORWARD_ONLY,
			JDBC20Translation.CONCUR_READ_ONLY,
			connectionHoldAbility,
			JDBC30Translation.NO_GENERATED_KEYS,
			null,
			null);
	}


    /**
     * JDBC 2.0
     *
     * Same as prepareStatement() above, but allows the default result set
     * type and result set concurrency type to be overridden.
     *
     * @param resultSetType a result set type, see ResultSet.TYPE_XXX
     * @param resultSetConcurrency a concurrency type, see ResultSet.CONCUR_XXX
     * @return a new PreparedStatement object containing the
     * pre-compiled SQL statement
     * @exception SQLException if a database-access error occurs.
     */
    public final PreparedStatement prepareStatement(String sql, int resultSetType,
					int resultSetConcurrency)
	    throws SQLException
	{
		return prepareStatement(sql,
			resultSetType,
			resultSetConcurrency,
			connectionHoldAbility,
			JDBC30Translation.NO_GENERATED_KEYS,
			null,
			null);
	}

    /**
     * JDBC 3.0
     *
     * Same as prepareStatement() above, but allows the default result set
     * type, result set concurrency type and result set holdability
     * to be overridden.
     *
     * @param resultSetType a result set type, see ResultSet.TYPE_XXX
     * @param resultSetConcurrency a concurrency type, see ResultSet.CONCUR_XXX
     * @param resultSetHoldability - one of the following ResultSet constants:
     *  ResultSet.HOLD_CURSORS_OVER_COMMIT or ResultSet.CLOSE_CURSORS_AT_COMMIT
     * @return a new PreparedStatement object containing the
     *  pre-compiled SQL statement
     * @exception SQLException if a database-access error occurs.
     */
    public final PreparedStatement prepareStatement(String sql, int resultSetType,
					int resultSetConcurrency, int resultSetHoldability)
	    throws SQLException
	{
		return prepareStatement(sql,
			resultSetType,
			resultSetConcurrency,
			resultSetHoldability,
			JDBC30Translation.NO_GENERATED_KEYS,
			null,
			null);
	}


	/**
	 * Creates a default PreparedStatement object capable of returning
	 * the auto-generated keys designated by the given array. This array contains
	 * the indexes of the columns in the target table that contain the auto-generated
	 * keys that should be made available. This array is ignored if the SQL statement
	 * is not an INSERT statement

		JDBC 3.0
	 *
	 *
	 * @param sql  An SQL statement that may contain one or more ? IN parameter placeholders
	 * @param columnIndexes  An array of column indexes indicating the columns
	 *  that should be returned from the inserted row or rows
	 *
	 * @return  A new PreparedStatement object, containing the pre-compiled
	 *  SQL statement, that will have the capability of returning auto-generated keys
	 *  designated by the given array of column indexes
	 *
	 * @exception SQLException  Feature not implemented for now.
	 */
	public final PreparedStatement prepareStatement(
			String sql,
			int[] columnIndexes)
    throws SQLException
	{
 		throw Util.notImplemented("prepareStatement(String, int[])");
	}

	/**
	 * Creates a default PreparedStatement object capable of returning
	 * the auto-generated keys designated by the given array. This array contains
	 * the names of the columns in the target table that contain the auto-generated
	 * keys that should be returned. This array is ignored if the SQL statement
	 * is not an INSERT statement
	 *
		JDBC 3.0
	 *
	 * @param sql  An SQL statement that may contain one or more ? IN parameter placeholders
	 * @param columnNames  An array of column names indicating the columns
	 *  that should be returned from the inserted row or rows
	 *
	 * @return  A new PreparedStatement object, containing the pre-compiled
	 *  SQL statement, that will have the capability of returning auto-generated keys
	 *  designated by the given array of column names
	 *
	 * @exception SQLException  Feature not implemented for now.
	 */
	public final PreparedStatement prepareStatement(
			String sql,
			String[] columnNames)
    throws SQLException
	{
 		throw Util.notImplemented("prepareStatement(String, String[])");
	}

	/**
	 * Creates a default PreparedStatement object that has the capability to
	 * retieve auto-generated keys. The given constant tells the driver
	 * whether it should make auto-generated keys available for retrieval.
	 * This parameter is ignored if the SQL statement is not an INSERT statement.
	 * JDBC 3.0
	 *
	 * @param sql  A SQL statement that may contain one or more ? IN parameter placeholders
	 * @param autoGeneratedKeys  A flag indicating whether auto-generated keys
	 *  should be returned
	 *
	 * @return  A new PreparedStatement object, containing the pre-compiled
	 *  SQL statement, that will have the capability of returning auto-generated keys
	 *
	 * @exception SQLException  Feature not implemented for now.
	 */
	public final PreparedStatement prepareStatement(
			String sql,
			int autoGeneratedKeys)
    throws SQLException
	{
		return prepareStatement(sql,
			JDBC20Translation.TYPE_FORWARD_ONLY,
			JDBC20Translation.CONCUR_READ_ONLY,
			connectionHoldAbility,
			autoGeneratedKeys,
			null,
			null);
	}
    
	private PreparedStatement prepareStatement(String sql, int resultSetType,
					int resultSetConcurrency, int resultSetHoldability,
					int autoGeneratedKeys, int[] columnIndexes, String[] columnNames)
       throws SQLException
	 {
		synchronized (getConnectionSynchronization()) {
                        setupContextStack();
			try {
			    return factory.newEmbedPreparedStatement(this, sql, false,
											   setResultSetType(resultSetType),
											   setResultSetConcurrency(resultSetConcurrency),
											   resultSetHoldability,
											   autoGeneratedKeys,
											   columnIndexes,
											   columnNames);
			} finally {
			    restoreContextStack();
			}
		}
     }

    /**
     * A SQL stored procedure call statement is handled by creating a
     * CallableStatement for it. The CallableStatement provides
     * methods for setting up its IN and OUT parameters, and
     * methods for executing it.
     *
     * <P><B>Note:</B> This method is optimized for handling stored
     * procedure call statements. Some drivers may send the call
     * statement to the database when the prepareCall is done; others
     * may wait until the CallableStatement is executed. This has no
     * direct affect on users; however, it does affect which method
     * throws certain SQLExceptions.
     *
     * JDBC 2.0
     *
     * Result sets created using the returned CallableStatement will have
     * forward-only type, and read-only concurrency, by default.
     *
     * @param sql a SQL statement that may contain one or more '?'
     * parameter placeholders. Typically this  statement is a JDBC
     * function call escape string.
     * @return a new CallableStatement object containing the
     * pre-compiled SQL statement 
     * @exception SQLException if a database-access error occurs.
     */
	public final CallableStatement prepareCall(String sql) 
		throws SQLException 
	{
		return prepareCall(sql, JDBC20Translation.TYPE_FORWARD_ONLY,
						   JDBC20Translation.CONCUR_READ_ONLY,
						   connectionHoldAbility);
	}

    /**
     * JDBC 2.0
     *
     * Same as prepareCall() above, but allows the default result set
     * type and result set concurrency type to be overridden.
     *
     * @param resultSetType a result set type, see ResultSet.TYPE_XXX
     * @param resultSetConcurrency a concurrency type, see ResultSet.CONCUR_XXX
     * @return a new CallableStatement object containing the
     * pre-compiled SQL statement 
     * @exception SQLException if a database-access error occurs.
     */
    public final CallableStatement prepareCall(String sql, int resultSetType,
				 int resultSetConcurrency)
		throws SQLException 
	{
		return prepareCall(sql, resultSetType, resultSetConcurrency,
						   connectionHoldAbility);
	}

    /**
     * JDBC 3.0
     *
     * Same as prepareCall() above, but allows the default result set
     * type, result set concurrency type and result set holdability
     * to be overridden.
     *
     * @param resultSetType a result set type, see ResultSet.TYPE_XXX
     * @param resultSetConcurrency a concurrency type, see ResultSet.CONCUR_XXX
     * @param resultSetHoldability - one of the following ResultSet constants:
     *  ResultSet.HOLD_CURSORS_OVER_COMMIT or ResultSet.CLOSE_CURSORS_AT_COMMIT
     * @return a new CallableStatement object containing the
     * pre-compiled SQL statement 
     * @exception SQLException if a database-access error occurs.
     */
    public final CallableStatement prepareCall(String sql, int resultSetType, 
				 int resultSetConcurrency, int resultSetHoldability)
		throws SQLException 
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isClosed(), "connection is closed");

		synchronized (getConnectionSynchronization())
		{
            setupContextStack();
			try 
			{
			    return factory.newEmbedCallableStatement(this, sql,
											   setResultSetType(resultSetType),
											   setResultSetConcurrency(resultSetConcurrency),
											   resultSetHoldability);
			} 
			finally 
			{
			    restoreContextStack();
			}
		}
	}

    /**
     * A driver may convert the JDBC sql grammar into its system's
     * native SQL grammar prior to sending it; nativeSQL returns the
     * native form of the statement that the driver would have sent.
     *
     * @param sql a SQL statement that may contain one or more '?'
     * parameter placeholders
     * @return the native form of this statement
     */
    public String nativeSQL(String sql) {
		// we don't massage the strings at all, so this is easy:
		return sql;
	}

    /**
     * If a connection is in auto-commit mode, then all its SQL
     * statements will be executed and committed as individual
     * transactions.  Otherwise, its SQL statements are grouped into
     * transactions that are terminated by either commit() or
     * rollback().  By default, new connections are in auto-commit
     * mode.
     *
     * The commit occurs when the statement completes or the next
     * execute occurs, whichever comes first. In the case of
     * statements returning a ResultSet, the statement completes when
     * the last row of the ResultSet has been retrieved or the
     * ResultSet has been closed. In advanced cases, a single
     * statement may return multiple results as well as output
     * parameter values. Here the commit occurs when all results and
     * output param values have been retrieved.
     *
     * @param autoCommit true enables auto-commit; false disables
     * auto-commit.  
     * @exception SQLException if a database-access error occurs.
     */
	public void setAutoCommit(boolean autoCommit) throws SQLException {

		// Is this a nested connection
		if (rootConnection != this) {
			if (autoCommit)
				throw newSQLException(SQLState.NO_AUTO_COMMIT_ON);
		}

		if (this.autoCommit != autoCommit)
			commit();

		this.autoCommit = autoCommit;
	}

    /**
     * Get the current auto-commit state.
     *
     * @return Current state of auto-commit mode.
     * @see #setAutoCommit 
     */
    public boolean getAutoCommit() {
		return autoCommit;
	}

    /**
     * Commit makes all changes made since the previous
     * commit/rollback permanent and releases any database locks
     * currently held by the Connection. This method should only be
     * used when auto commit has been disabled.
     *
     * @exception SQLException if a database-access error occurs.
     * @see #setAutoCommit 
     */
    public void commit() throws SQLException {
		synchronized (getConnectionSynchronization())
		{
			/*
			** Note that the context stack is
			** needed even for rollback & commit
			*/
            setupContextStack();

			try
			{
		    	getTR().commit();
			}
            catch (Throwable t)
			{
				throw handleException(t);
			}
			finally 
			{
				restoreContextStack();
			}

			needCommit = false;
		}
	}

    /**
     * Rollback drops all changes made since the previous
     * commit/rollback and releases any database locks currently held
     * by the Connection. This method should only be used when auto
     * commit has been disabled.
     *
     * @exception SQLException if a database-access error occurs.
     * @see #setAutoCommit 
     */
    public void rollback() throws SQLException {

		synchronized (getConnectionSynchronization())
		{
			/*
			** Note that the context stack is
			** needed even for rollback & commit
			*/
            setupContextStack();
			try
			{
		    	getTR().rollback();
			} catch (Throwable t) {
				throw handleException(t);
			}
			finally 
			{
				restoreContextStack();
			}
			needCommit = false;
		} 
	}

    /**
     * In some cases, it is desirable to immediately release a
     * Connection's database and JDBC resources instead of waiting for
     * them to be automatically released; the close method provides this
     * immediate release. 
     *
     * <P><B>Note:</B> A Connection is automatically closed when it is
     * garbage collected. Certain fatal errors also result in a closed
     * Connection.
     *
     * @exception SQLException if a database-access error occurs.
     */
    public void close() throws SQLException {
		// JDK 1.4 javadoc indicates close on a closed connection is a no-op
		if (isClosed())
		   	return;


		if (rootConnection == this)
		{
			/* Throw error to match DB2/JDBC if a tran is pending in non-autocommit mode */
			if (!autoCommit && !transactionIsIdle()) {
				throw newSQLException(SQLState.LANG_INVALID_TRANSACTION_STATE);
			}

			close(exceptionClose);
		}
		else
			setInactive(); // nested connection
	}

	// This inner close takes the exception and calls 
	// the context manager to make the connection close.
	// The exception must be a session severity exception.
	//
	// NOTE: This method is not part of JDBC specs.
	//
    private void close(Exception e) throws SQLException {
		
		synchronized(getConnectionSynchronization())
		{
			if (rootConnection == this)
			{
				/*
				 * If it isn't active, it's already been closed.
				 */
				if (active) {
					setupContextStack();
					try {
						tr.rollback();

						// Let go of lcc reference so it can be GC'ed after
						// cleanupOnError, the tr will stay around until the
						// rootConnection itself is GC'ed, which is dependent
						// on how long the client program wants to hold on to
						// the Connection object.
						tr.clearLcc(); 
						tr.cleanupOnError(e);

					} catch (Throwable t) {
						throw handleException(t);
					} finally {
						restoreContextStack();
					}
				}
			}

			if (!isClosed())
				setInactive();
		}
	}

    /**
     * Tests to see if a Connection is closed.
     *
     * @return true if the connection is closed; false if it's still open
     */
    public final boolean isClosed() {
		if (active) {

			// I am attached, check the database state
			if (getTR().isActive()) {
				return false;
			}

			setInactive();

		}
		return true;
	}

    /**
     * A Connection's database is able to provide information
     * describing its tables, its supported SQL grammar, its stored
     * procedures, the capabilities of this connection, etc. This
     * information is made available through a DatabaseMetaData
     * object.
     *
     * @return a DatabaseMetaData object for this Connection 
     * @exception SQLException if a database-access error occurs.
     */
    public DatabaseMetaData getMetaData() throws SQLException {
		if (isClosed())
			throw Util.noCurrentConnection();

		if (dbMetadata == null) {

 			// There is a case where dbname can be null.
			// Replication client of this method does not have a
			// JDBC connection; therefore dbname is null and this
			// is expected.
			//
			dbMetadata = factory.newEmbedDatabaseMetaData(this, getTR().getUrl());
		}
		return dbMetadata;
	}

	/**
		JDBC 3.0
	 * Retrieves the current holdability of ResultSet objects created using this
	 * Connection object.
	 *
	 *
	 * @return  The holdability, one of ResultSet.HOLD_CURSORS_OVER_COMMIT
	 * or ResultSet.CLOSE_CURSORS_AT_COMMIT
	 *
	 */
	public final int getHoldability()
	{
		return connectionHoldAbility;
	}

	/**
		JDBC 3.0
	 * Changes the holdability of ResultSet objects created using this
	 * Connection object to the given holdability.
	 *
	 *
	 * @param holdability  A ResultSet holdability constant, one of ResultSet.HOLD_CURSORS_OVER_COMMIT
	 * or ResultSet.CLOSE_CURSORS_AT_COMMIT
	 *
	 */
	public final void setHoldability(int holdability)
	{
		connectionHoldAbility = holdability;
	}

    /**
     * You can put a connection in read-only mode as a hint to enable 
     * database optimizations.
     *
     * <P><B>Note:</B> setReadOnly cannot be called while in the
     * middle of a transaction.
     *
     * @param readOnly true enables read-only mode; false disables
     * read-only mode.  
     * @exception SQLException if a database-access error occurs.
     */
    public final void setReadOnly(boolean readOnly) throws SQLException
	{
		synchronized(getConnectionSynchronization())
		{
                        setupContextStack();
			try {
				getLanguageConnection().setReadOnly(readOnly);
			} catch (StandardException e) {
				throw handleException(e);
			} finally {
				restoreContextStack();
			}
		}
	}

    /**
     * Tests to see if the connection is in read-only mode.
     *
     * @return true if connection is read-only
     * @exception SQLException if a database-access error occurs.
     */
    public final boolean isReadOnly() throws SQLException
	{
		return getLanguageConnection().isReadOnly();
	}

    /**
     * A sub-space of this Connection's database may be selected by setting a
     * catalog name. If the driver does not support catalogs it will
     * silently ignore this request.
     *
     * @exception SQLException if a database-access error occurs.
     */
    public void setCatalog(String catalog) throws SQLException {
		// silently ignoring this request like the javadoc said.
		return;
	}

    /**
     * Return the Connection's current catalog name.
     *
     * @return the current catalog name or null
     * @exception SQLException if a database-access error occurs.
     */
	public String getCatalog() throws SQLException {
		// we do not have support for Catalog, just return null as
		// the JDBC specs mentions then.
		return null;
	}

    /**
     * You can call this method to try to change the transaction
     * isolation level using one of the TRANSACTION_* values.
     *
     * <P><B>Note:</B> setTransactionIsolation causes the current
     * transaction to commit
     *
     * @param level one of the TRANSACTION_* isolation values with the
     * exception of TRANSACTION_NONE; some databases may not support
     * other values
     * @exception SQLException if a database-access error occurs.
     * @see DatabaseMetaData#supportsTransactionIsolationLevel 
     */
    public void setTransactionIsolation(int level) throws SQLException {

		if (level == getTransactionIsolation())
			return;

		// Convert the isolation level to the internal one
		int iLevel;
		switch (level)
		{
		case java.sql.Connection.TRANSACTION_READ_UNCOMMITTED:
			iLevel = ExecutionContext.READ_UNCOMMITTED_ISOLATION_LEVEL;
			break;

		case java.sql.Connection.TRANSACTION_READ_COMMITTED:
			iLevel = ExecutionContext.READ_COMMITTED_ISOLATION_LEVEL;
			break;

		case java.sql.Connection.TRANSACTION_REPEATABLE_READ:
            iLevel = ExecutionContext.REPEATABLE_READ_ISOLATION_LEVEL;
            break;

		case java.sql.Connection.TRANSACTION_SERIALIZABLE:
			iLevel = ExecutionContext.SERIALIZABLE_ISOLATION_LEVEL;
			break;
		default:
			throw newSQLException(SQLState.UNIMPLEMENTED_ISOLATION_LEVEL, new Integer(level));
		}

		synchronized(getConnectionSynchronization())
		{
            setupContextStack();
			try {
				getLanguageConnection().setIsolationLevel(iLevel);
			} catch (StandardException e) {
				throw handleException(e);
			} finally {
				restoreContextStack();
			}
		}
	}


    /**
     * Get this Connection's current transaction isolation mode.
     *
     * @return the current TRANSACTION_* mode value
     * @exception SQLException if a database-access error occurs.
     */
    public final int getTransactionIsolation() throws SQLException {

		return ExecutionContext.CS_TO_JDBC_ISOLATION_LEVEL_MAP[getLanguageConnection().getCurrentIsolationLevel()];
	}

    /**
     * The first warning reported by calls on this Connection is
     * returned.  
     *
     * <P><B>Note:</B> Subsequent warnings will be chained to this
     * SQLWarning.
     *
     * @return the first SQLWarning or null 
     *
	 * Synchronization note: Warnings are synchronized 
	 * on nesting level
     */
	public final synchronized SQLWarning getWarnings() {
   		return topWarning;
	}

    /**
     * After this call, getWarnings returns null until a new warning is
     * reported for this Connection.  
     *
	 * Synchronization node: Warnings are synchonized 
	 * on nesting level
     */
    public final synchronized void clearWarnings() {
		topWarning = null;
	}


	/////////////////////////////////////////////////////////////////////////
	//
	//	Implementation specific methods	
	//
	/////////////////////////////////////////////////////////////////////////

	/**
		Add a warning to the current list of warnings, to follow
		this note from Connection.getWarnings.
		Note: Subsequent warnings will be chained to this SQLWarning. 

		@see java.sql.Connection#getWarnings
	*/
	 public final synchronized void addWarning(SQLWarning newWarning) {
		if (topWarning == null) {
			topWarning = newWarning;
			return;
		}

		topWarning.setNextWarning(newWarning);
	}

	/**
	 * Return the dbname for this connection.
	 *
	 * @return String	The dbname for this connection.
	 */
	public String getDBName()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isClosed(), "connection is closed");

		return getTR().getDBName();
	}

	public final LanguageConnectionContext getLanguageConnection() {

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isClosed(), "connection is closed");

		return getTR().getLcc();
	}

	//EmbedConnection30 overrides this method so it can release the savepoints array if
	//the exception severity is transaction level
	public SQLException handleException(Throwable thrownException)
			throws SQLException
	{
		/*
		** By default, rollback the connection on if autocommit
	 	** is on.
		*/
		return getTR().handleException(thrownException, 
								  autoCommit,
								  true // Rollback xact on auto commit
								  );
	}

	/**
		Handle any type of Exception.
		<UL>
		<LI> Inform the contexts of the error
		<LI> Throw an Util based upon the thrown exception.
		</UL>

		REMIND: now that we know all the exceptions from our driver
		are Utils, would it make sense to shut down the system
		for unknown SQLExceptions? At present, we do not.

		Because this is the last stop for exceptions,
		it will catch anything that occurs in it and try
		to cleanup before re-throwing them.
	
		@param thrownException the exception
		@param rollbackOnAutoCommit rollback the xact on if autocommit is
				on, otherwise rollback stmt but leave xact open (and
				continue to hold on to locks).  Most of the time, this
				will be true, excepting operations on result sets, like
				getInt().
	*/
	final SQLException handleException(Throwable thrownException, 
									   boolean rollbackOnAutoCommit) 
			throws SQLException 
	{
		return getTR().handleException(thrownException, autoCommit,
								  rollbackOnAutoCommit); 

	}

	/*
	   This is called from the EmbedConnectionContext to
	   close on errors.  We assume all handling of the connectin
	   is dealt with via the context stack, and our only role
	   is to mark ourself as closed.
	 */

	/**
		Close the connection when processing errors, or when
	 	closing a nested connection.
		<p>
		This only marks it as closed and frees up its resources;
		any closing of the underlying connection or commit work
		is assumed to be done elsewhere.

		Called from EmbedConnectionContext's cleanup routine,	
		and by proxy.close().
	 */

	public final void setInactive() {

		if (active == false)
			return;
		// active = false
		// tr = null !-> active = false

		synchronized (getConnectionSynchronization()) {
			active = false;
			// tr = null; cleanupOnerror sets inactive but still needs tr to
			// restore context later
			dbMetadata = null;
		}
	}

	/**
		@exception Throwable	standard error policy
	 */
	protected void finalize() throws Throwable 
	{
		if (rootConnection == this)
		{
			super.finalize();
			if (!isClosed())
	    		close(exceptionClose);
		}
	}

	/**
	 * if auto commit is on, remember that we need to commit
	 * the current statement.
	 */
    protected void needCommit() {
		if (!needCommit) needCommit = true;
	}

	/**
	 * if a commit is needed, perform it.
     *
     * Must have connection synchonization and context set up already.
     *
	 * @exception SQLException if commit returns error
	 */
	protected void commitIfNeeded() throws SQLException 
    {
		if (autoCommit && needCommit) 
        {
            try
            {
                getTR().commit();
            } 
            catch (Throwable t)
            {
                throw handleException(t);
            }
            needCommit = false;
		}
	}

	/**
	 * If in autocommit, then commit.
     * 
     * Used to force a commit after a result set closes in autocommit mode.
     * The needCommit mechanism does not work correctly as there are times
     * with cursors (like a commit, followed by a next, followed by a close)
     * where the system does not think it needs a commit but we need to 
     * force the commit on close.  It seemed safer to just force a commit
     * on close rather than count on keeping the needCommit flag correct for
     * all cursor cases.
     *
     * Must have connection synchonization and context set up already.
     *
	 * @exception SQLException if commit returns error
	 */
	protected void commitIfAutoCommit() throws SQLException 
    {
		if (autoCommit) 
        {
            try
            {
                getTR().commit();
            } 
            catch (Throwable t)
            {
                throw handleException(t);
            }
            needCommit = false;
		}
	}


	final protected Object getConnectionSynchronization()
  {
		return rootConnection;
  }

	/**
		Install the context manager for this thread.  Check connection status here.
	 	@exception SQLException if fails
	 */
	protected final void  setupContextStack() throws SQLException {

		/*
			Track this entry, then throw an exception
			rather than doing the quiet return.  Need the
			track before the throw because the backtrack
			is in a finally block.
		 */

		if (isClosed())
			throw Util.noCurrentConnection();

		getTR().setupContextStack();

	}

	protected final void restoreContextStack() throws SQLException {

		if (SanityManager.DEBUG)
		Util.ASSERT(this, (active) || getTR().getCsf() !=null, "No context service to do restore");

		TransactionResourceImpl tr = getTR();

		//REMIND: someone is leaving an incorrect manager on when they
		// are exiting the system in the nested case.
		if (SanityManager.DEBUG)
		{
			if ((tr.getCsf() != null) && (tr.getCsf().getCurrentContextManager() !=
				tr.getContextManager()))
			{
				Util.THROWASSERT(this, 
					"Current Context Manager not the one was expected: " +
					 tr.getCsf().getCurrentContextManager() + " " + 
					 tr.getContextManager());
			}
		}

		tr.restoreContextStack();
	}

	/*
	** Create database methods.
	*/

	/**
		Create a new database.
		@param dbname the database name
		@param info the properties

		@return	Database The newly created database or null.

	 	@exception SQLException if fails to create database
	*/

	private Database createDatabase(String dbname, Properties info)
		throws SQLException {

		info = filterProperties(info);

		try {
			if (Monitor.createPersistentService(Property.DATABASE_MODULE, dbname, info) == null) 
			{
				// service already exists, create a warning
				addWarning(EmbedSQLWarning.newEmbedSQLWarning(SQLState.DATABASE_EXISTS, dbname));
			}
		} catch (StandardException mse) {

			SQLException se = newSQLException(SQLState.CREATE_DATABASE_FAILED, dbname);
			se.setNextException(handleException(mse));
			throw se;
		}

		// clear these values as some modules hang onto
		// the properties set corresponding to service.properties
		// and they shouldn't be interested in these JDBC attributes.
		info.clear();

		return (Database) Monitor.findService(Property.DATABASE_MODULE, dbname);
	}


	/**
		Return false iff the monitor cannot handle a service
		of the type indicated by the protocol within the name.
		If that's the case then we are the wrong driver.

		Throw exception if anything else is wrong.
	 */

	private boolean bootDatabase(Properties info) throws Throwable
	{
		String dbname = tr.getDBName();

		// boot database now
		try {

			info = filterProperties(info);
			
			// try to start the service if it doesn't already exist
			if (!Monitor.startPersistentService(dbname, info)) {
				// a false indicates the monitor cannot handle a service
				// of the type indicated by the protocol within the name.
				// If that's the case then we are the wrong driver
				// so just return null.
				return false;
			}

			// clear these values as some modules hang onto
			// the properties set corresponding to service.properties
			// and they shouldn't be interested in these JDBC attributes.
			info.clear();

			Database database = (Database) Monitor.findService(Property.DATABASE_MODULE, dbname);
			tr.setDatabase(database);

		} catch (StandardException mse) {
			SQLException se = newSQLException(SQLState.BOOT_DATABASE_FAILED, dbname);

			Throwable ne = mse.getNestedException();
			SQLException nse;

			/*
			  If there is a next exception, assume
			  that the first one is just a redundant "see the
			  next exception" message.
			  if it is a BEI, treat it as a database exception.
			  If there isn't a BEI, treat it as a java exception.

			  In general we probably want to walk the chain
			  and return all of them, but empirically, this
			  is all we need to do for now.
			  */
			if (ne instanceof StandardException)
				nse = Util.generateCsSQLException((StandardException)ne);
			else if (ne != null)
				nse = Util.javaException(ne);
			else
				nse = Util.generateCsSQLException(mse);

			se.setNextException(nse);
			throw se;
		}

		// If database exists, getDatabase() will return the database object.
		// If any error occured while booting an existing database, an
		// exception would have been thrown already.
		return true;

	}

	/*
	 * Class interface methods used by database metadata to ensure
	 * good relations with autocommit.
	 */

    PreparedStatement prepareMetaDataStatement(String sql)
	    throws SQLException {
		synchronized (getConnectionSynchronization()) {
                        setupContextStack();
			PreparedStatement s = null;
			try {
			    s = factory.newEmbedPreparedStatement(this, sql, true,
											  JDBC20Translation.TYPE_FORWARD_ONLY,
											  JDBC20Translation.CONCUR_READ_ONLY,
											  connectionHoldAbility,
											  JDBC30Translation.NO_GENERATED_KEYS,
											  null,
											  null);
			} finally {
			    restoreContextStack();
			}
			return s;
		}
	}

	public final Driver169 getLocalDriver()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isClosed(), "connection is closed");

		return getTR().getDriver();
	}

	/**
		Return the context manager for this connection.
	*/
	public final ContextManager getContextManager() {

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isClosed(), "connection is closed");

		return getTR().getContextManager();
	}

	private Properties filterProperties(Properties inputSet) {
		Properties limited = new Properties();

		// filter out any derby.* properties, only
		// JDBC attributes can be set this way
		for (java.util.Enumeration e = inputSet.propertyNames(); e.hasMoreElements(); ) {

			String key = (String) e.nextElement();

			// we don't allow properties to be set this way
			if (key.startsWith("derby."))
				continue;
			limited.put(key, inputSet.getProperty(key));
		}
		return limited;
	}

	/*
	** methods to be overridden by subimplementations wishing to insert
	** their classes into the mix.
	*/

	protected Database getDatabase() 
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isClosed(), "connection is closed");

		return getTR().getDatabase();
	}

	final protected TransactionResourceImpl getTR()
	{
		return rootConnection.tr;
	}

	private EmbedConnectionContext pushConnectionContext(ContextManager cm) {
		return new EmbedConnectionContext(cm, this);
	}

	public final void setApplicationConnection(java.sql.Connection applicationConnection) {
		this.applicationConnection = applicationConnection;
	}

	public final java.sql.Connection getApplicationConnection() {
		return applicationConnection;
	}

	public void setDrdaID(String drdaID) {
		getLanguageConnection().setDrdaID(drdaID);
	}

	/**
	 * returns false if there is an underlying transaction and that transaction
	 * has done work.  True if there is no underlying transaction or that
	 * underlying transaction is idle
	 */
	public final boolean transactionIsIdle()
	{
		return getTR().isIdle();
	}
	private int setResultSetType(int resultSetType) {

		/* Add warning if scroll sensitive cursor
		 * and downgrade to scroll insensitive cursor.
		 */
		if (resultSetType == JDBC20Translation.TYPE_SCROLL_SENSITIVE)
		{
			addWarning(EmbedSQLWarning.newEmbedSQLWarning(SQLState.NO_SCROLL_SENSITIVE_CURSORS));
			resultSetType = JDBC20Translation.TYPE_SCROLL_INSENSITIVE;
		}
		return resultSetType;
	}

	

	/** 
	 * Set the transaction isolation level that will be used for the 
	 * next prepare.  Used by network server to implement DB2 style 
	 * isolation levels.
	 * @param level Isolation level to change to.  level is the DB2 level
	 *               specified in the package names which happen to correspond
	 *               to our internal levels. If 
	 *               level == ExecutionContext.UNSPECIFIED_ISOLATION,
	 *               the statement won't be prepared with an isolation level.
	 * 
	 * 
	 */
	public void setPrepareIsolation(int level) throws SQLException
	{
		if (level == getPrepareIsolation())
			return;

		switch (level)
		{
			case ExecutionContext.READ_UNCOMMITTED_ISOLATION_LEVEL:
			case ExecutionContext.REPEATABLE_READ_ISOLATION_LEVEL:
			case ExecutionContext.READ_COMMITTED_ISOLATION_LEVEL:
			case ExecutionContext.SERIALIZABLE_ISOLATION_LEVEL:
			case ExecutionContext.UNSPECIFIED_ISOLATION_LEVEL:
				break;
			default:
				throw Util.generateCsSQLException(
															   SQLState.UNIMPLEMENTED_ISOLATION_LEVEL, new Integer(level));
		}
		
		synchronized(getConnectionSynchronization())
		{
			getLanguageConnection().setPrepareIsolationLevel(level);
		}
	}

	/**
	 * Return prepare isolation 
	 */
	public int getPrepareIsolation()
	{
		return getLanguageConnection().getPrepareIsolationLevel();
	}

	private int setResultSetConcurrency(int resultSetConcurrency) {

		/* Add warning if updatable concurrency
		 * and downgrade to read only.
		 */
		if (resultSetConcurrency == JDBC20Translation.CONCUR_UPDATABLE)
		{
			addWarning(EmbedSQLWarning.newEmbedSQLWarning(SQLState.NO_UPDATABLE_CONCURRENCY));
			resultSetConcurrency = JDBC20Translation.CONCUR_READ_ONLY;
		}
		return resultSetConcurrency;
	}

	/**
		Return a unique order number for a result set.
		A unique value is only needed if the result set is
		being created within procedure and thus must be using
		a nested connection.
	*/
	final int getResultSetOrderId() {

		if (this == rootConnection) {
			return 0;
		} else {
			return rootConnection.resultSetId++;
		}
	}

	protected SQLException newSQLException(String messageId) {
		return Util.generateCsSQLException(messageId);
	}
	protected SQLException newSQLException(String messageId, Object arg1) {
		return Util.generateCsSQLException(messageId, arg1);
	}
	protected SQLException newSQLException(String messageId, Object arg1, Object arg2) {
		return Util.generateCsSQLException(messageId, arg1, arg2);
	}

	/////////////////////////////////////////////////////////////////////////
	//
	//	OBJECT OVERLOADS
	//
	/////////////////////////////////////////////////////////////////////////

    public	String	toString() { return "EmbedConnection"; }
}
