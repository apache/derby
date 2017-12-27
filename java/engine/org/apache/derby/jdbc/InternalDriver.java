
/*

   Derby - Class org.apache.derby.jdbc.InternalDriver

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

package org.apache.derby.jdbc;

import java.security.AccessController;
import java.security.AccessControlException;
import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedActionException;
import java.security.Permission;
import java.security.PrivilegedAction;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.jdbc.AuthenticationService;
import org.apache.derby.iapi.jdbc.BrokeredConnection;
import org.apache.derby.iapi.jdbc.BrokeredConnectionControl;
import org.apache.derby.iapi.jdbc.ConnectionContext;
import org.apache.derby.iapi.jdbc.ResourceAdapter;
import org.apache.derby.shared.common.reference.Attribute;
import org.apache.derby.shared.common.reference.MessageId;
import org.apache.derby.iapi.reference.Module;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.security.SecurityUtil;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.services.io.FormatableProperties;
import org.apache.derby.iapi.services.jmx.ManagementService;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.util.InterruptStatus;
import org.apache.derby.impl.jdbc.*;
import org.apache.derby.mbeans.JDBCMBean;
import org.apache.derby.security.SystemPermission;

/**
 * Factory class and API for JDBC objects.
 */
public class InternalDriver implements ModuleControl, Driver {
    
	private static final Object syncMe = new Object();
	private static InternalDriver activeDriver;
    
    private Object mbean;

	protected boolean active;
	private ContextService contextServiceFactory;
	private AuthenticationService	authenticationService;

    /**
     * Tells whether or not {@code AutoloadedDriver} should deregister itself
     * on shutdown. This flag is true unless the deregister attribute has been
     * set to false by the user (DERBY-2905).
     */
    private static boolean deregister = true;

    /**
     * <p>
     * An executor service used for executing connection attempts when a
     * login timeout has been specified.
     * </p>
     *
     * <p>
     * DERBY-6107: Core pool size and keep alive timeout should be zero so
     * that no threads are cached. By creating a fresh thread each time a
     * task is submitted, we make sure that the task will run in a thread
     * with the same context class loader as the thread that submitted the
     * task. This is important for example when connecting to a database
     * using the classpath subsubprotocol, and the database lives in the
     * context class loader. If threads are cached, a task may execute in
     * a thread that has a different context class loader.
     * </p>
     */
    private static final ExecutorService _executorPool =
            new ThreadPoolExecutor(0, Integer.MAX_VALUE, 0L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), new DaemonThreadFactory());

	public static final InternalDriver activeDriver()
	{
		return activeDriver;
	}

	public InternalDriver() {
		contextServiceFactory = getContextService();
	}

	/*
	**	Methods from ModuleControl
	*/

	public void boot(boolean create, Properties properties) throws StandardException {

		synchronized (InternalDriver.syncMe)
		{
			InternalDriver.activeDriver = this;
		}

		active = true;
        
        mbean = ((ManagementService)
           getSystemModule(Module.JMX)).registerMBean(
                   new JDBC(this),
                   JDBCMBean.class,
                   "type=JDBC");

        // Register with the driver manager
        AutoloadedDriver.registerDriverModule(this);
	}

	public void stop() {

		synchronized (InternalDriver.syncMe)
		{
			InternalDriver.activeDriver = null;
		}
        
        ((ManagementService)
                getSystemModule(Module.JMX)).unregisterMBean(
                        mbean);

		active = false;

		contextServiceFactory = null;

        AutoloadedDriver.unregisterDriverModule();
	}

	/*
	** Methods from java.sql.Driver
	*/
	public boolean acceptsURL(String url) throws SQLException
    {
		return active && embeddedDriverAcceptsURL( url );
	}

	/*
	** This method can be called by AutoloadedDriver so that we
	** don't accidentally boot Derby while answering the question "Can
	** you handle this URL?"
	*/
	public static	boolean embeddedDriverAcceptsURL(String url) throws SQLException
    {
        if ( url == null )
        {
            throw Util.generateCsSQLException( SQLState.MALFORMED_URL, "null" );
        }
        
		return
		//	need to reject network driver's URL's
		!url.startsWith(Attribute.JCC_PROTOCOL) && !url.startsWith(Attribute.DNC_PROTOCOL) &&
		(url.startsWith(Attribute.PROTOCOL) || url.equals(Attribute.SQLJ_NESTED));
				
	}

	public Connection connect( String url, Properties info, int loginTimeoutSeconds )
		 throws SQLException 
	{
		if (!acceptsURL(url)) { return null; }
		
        /**
         * If we are below the low memory watermark for obtaining
         * a connection, then don't even try. Just throw an exception.
         */
		if (EmbedConnection.memoryState.isLowMemory())
		{
			throw EmbedConnection.NO_MEM;
		}
        			
		/*
		** A url "jdbc:default:connection" means get the current
		** connection.  From within a method called from JSQL, the
		** "current" connection is the one that is running the
		** JSQL statement containing the method call.
		*/
		boolean current = url.equals(Attribute.SQLJ_NESTED);
		
		/* If jdbc:default:connection, see if user already has a
		 * connection. All connection attributes are ignored.
		 */
		if (current) {

			ConnectionContext connContext = getConnectionContext();

			if (connContext != null) {
						
				return connContext.getNestedConnection(false);
				
			}
			// there is no Derby connection, so
			// return null, as we are not the driver to handle this
			return null;
		}

		// convert the ;name=value attributes in the URL into
		// properties.
		FormatableProperties finfo = null;
        
		try {
            
            finfo = getAttributes(url, info);
            info = null; // ensure we don't use this reference directly again.

			/*
			** A property "shutdown=true" means shut the system or database down
			*/
			boolean shutdown = Boolean.valueOf(finfo.getProperty(Attribute.SHUTDOWN_ATTR)).booleanValue();
			
			if (shutdown) {				
				// If we are shutting down the system don't attempt to create
				// a connection; but we validate users credentials if we have to.
				// In case of datbase shutdown, we ask the database authentication
				// service to authenticate the user. If it is a system shutdown,
				// then we ask the Driver to do the authentication.
				//
				if (InternalDriver.getDatabaseName(url, finfo).length() == 0) {
					//
					// We need to authenticate the user if authentication is
					// ON. Note that this is a system shutdown.
					// check that we do have a authentication service
					// it is _always_ expected.
					if (this.getAuthenticationService() == null)
						throw Util.generateCsSQLException(
                        SQLState.LOGIN_FAILED, 
						MessageService.getTextMessage(MessageId.AUTH_NO_SERVICE_FOR_SYSTEM));
					
						
					if (!this.getAuthenticationService().authenticate((String) null, finfo)) {

						// not a valid user
						throw Util.generateCsSQLException(
                                    SQLState.NET_CONNECT_AUTH_FAILED,
                                    MessageService.
                                    getTextMessage(MessageId.AUTH_INVALID));
					}

                    // DERBY-2905, allow users to provide deregister attribute to 
                    // leave AutoloadedDriver registered in DriverManager, default
                    // value is true
                    if (finfo.getProperty(Attribute.DEREGISTER_ATTR) != null) {
                        boolean deregister = Boolean.valueOf(
                                finfo.getProperty(Attribute.DEREGISTER_ATTR))
                                .booleanValue();
                        InternalDriver.setDeregister(deregister);
                    }

					// check for shutdown privileges
					// DERBY-3495: uncomment to enable system privileges checks
					//final String user = IdUtil.getUserNameFromURLProps(finfo);
					//checkShutdownPrivileges(user);

					getMonitor().shutdown();

					throw Util.generateCsSQLException(
                                         SQLState.CLOUDSCAPE_SYSTEM_SHUTDOWN);
				}
			}

            EmbedConnection conn;
			
            if ( loginTimeoutSeconds <= 0 ) { conn = getNewEmbedConnection( url, finfo ); }
            else { conn = timeLogin( url, finfo, loginTimeoutSeconds ); }
            
			// if this is not the correct driver a EmbedConnection
			// object is returned in the closed state.
			if (conn.isClosed()) {
				return null;
			}

			return conn;
		}
		catch (OutOfMemoryError noMemory)
		{
			EmbedConnection.memoryState.setLowMemory();
			throw EmbedConnection.NO_MEM;
		}
		finally {
			// break any link with the user's Properties set.
            if (finfo != null)
			    finfo.clearDefaults();
		}
	}

    /**
     * Enforce the login timeout.
     */
    private EmbedConnection timeLogin(
            String url, Properties info, int loginTimeoutSeconds)
        throws SQLException
    {
        try {
            LoginCallable callable = new LoginCallable(this, url, info);
            Future<EmbedConnection> task = _executorPool.submit(callable);
            long now = System.currentTimeMillis();
            long giveUp = now + loginTimeoutSeconds * 1000L;

            while (now < giveUp) {
                try {
                    return task.get(giveUp - now, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ie) {
                    InterruptStatus.setInterrupted();
                    now = System.currentTimeMillis();
                    continue;
                } catch (ExecutionException ee) {
                    throw processException(ee);
                } catch (TimeoutException te) {
                    throw Util.generateCsSQLException(SQLState.LOGIN_TIMEOUT);
                }
            }

            // Timed out due to interrupts, throw.
            throw Util.generateCsSQLException(SQLState.LOGIN_TIMEOUT);
        } finally {
            InterruptStatus.restoreIntrFlagIfSeen();
        }
    }

    /** Process exceptions raised while running a timed login. */
    private SQLException processException(Throwable t) {
        Throwable cause = t.getCause();
        if (cause instanceof SQLException) {
            return (SQLException) cause;
        } else {
            return Util.javaException(t);
        }
    }

    /**
     * Thread factory to produce daemon threads which don't block VM shutdown.
     */
    private static final class DaemonThreadFactory implements ThreadFactory {
        public Thread newThread(Runnable r) {
            Thread result = new Thread(r);
            result.setDaemon(true);
            return result;
        }
    }

    /**
     * This code is called in a thread which puts time limits on it.
     */
    public static final class LoginCallable implements
            Callable<EmbedConnection> {

        private InternalDriver _driver;
        private String _url;
        private Properties _info;

        public LoginCallable(InternalDriver driver, String url, Properties info) {
            _driver = driver;
            _url = url;
            _info = info;
        }

        public EmbedConnection call() throws SQLException {
            // Erase the state variables after we use them.
            // Might be paranoid but there could be security-sensitive info
            // in here.
            String url = _url;
            Properties info = _info;
            InternalDriver driver = _driver;
            _url = null;
            _info = null;
            _driver = null;

            return driver.getNewEmbedConnection(url, info);
        }
    }

    /**
     * Checks for System Privileges.
     *
     * @param user The user to be checked for having the permission
     * @param perm The permission to be checked
     * @throws AccessControlException if permissions are missing
     */
    public void checkSystemPrivileges(String user, Permission perm) {
        SecurityUtil.checkUserHasPermission(user, perm);
    }

    /**
     * Checks for shutdown System Privileges.
     *
     * To perform this check the following policy grant is required
     * <ul>
     * <li> to run the encapsulated test:
     *      permission javax.security.auth.AuthPermission "doAsPrivileged";
     * </ul>
     * or a SQLException will be raised detailing the cause.
     * <p>
     * In addition, for the test to succeed
     * <ul>
     * <li> the given user needs to be covered by a grant:
     *      principal org.apache.derby.authentication.SystemPrincipal "..." {}
     * <li> that lists a shutdown permission:
     *      permission org.apache.derby.security.SystemPermission "shutdown";
     * </ul>
     * or it will fail with a SQLException detailing the cause.
     *
     * @param user The user to be checked for shutdown privileges
     * @throws SQLException if the privileges check fails
     */
    private void checkShutdownPrivileges(String user) throws SQLException {
        // approve action if not running under a security manager
        if (System.getSecurityManager() == null) {
            return;
        }

        // the check
        try {
            final Permission sp = new SystemPermission(
                SystemPermission.ENGINE, SystemPermission.SHUTDOWN);
            checkSystemPrivileges(user, sp);
        } catch (AccessControlException ace) {
            throw Util.generateCsSQLException(
				SQLState.AUTH_SHUTDOWN_MISSING_PERMISSION,
				user, (Object)ace); // overloaded method
        } catch (Exception e) {
            throw Util.generateCsSQLException(
				SQLState.AUTH_SHUTDOWN_MISSING_PERMISSION,
				user, (Object)e); // overloaded method
        }
    }

	public int getMajorVersion() {
		return getMonitor().getEngineVersion().getMajorVersion();
	}
	
	public int getMinorVersion() {
		return getMonitor().getEngineVersion().getMinorVersion();
	}

	public boolean jdbcCompliant() {
		return true;
	}

	/*
	** URL manipulation
	*/

	/**
		Convert all the attributes in the url into properties and
		combine them with the set provided. 
		<BR>
		If the caller passed in a set of attributes (info != null)
		then we set that up as the default of the returned property
		set as the user's set. This means we can easily break the link
		with the user's set, ensuring that we don't hang onto the users object.
		It also means that we don't add our attributes into the user's
		own property object.

		@exception SQLException thrown if URL form bad
	*/
	protected FormatableProperties getAttributes(String url, Properties info) 
		throws SQLException {

		// We use FormatableProperties here to take advantage
		// of the clearDefaults, method.
		FormatableProperties finfo = new FormatableProperties(info);
		info = null; // ensure we don't use this reference directly again.


		StringTokenizer st = new StringTokenizer(url, ";");
		st.nextToken(); // skip the first part of the url

		while (st.hasMoreTokens()) {

			String v = st.nextToken();

			int eqPos = v.indexOf('=');
			if (eqPos == -1)
				throw Util.generateCsSQLException(
                                            SQLState.MALFORMED_URL, url);

			//if (eqPos != v.lastIndexOf('='))
			//	throw Util.malformedURL(url);

			finfo.put((v.substring(0, eqPos)).trim(),
					 (v.substring(eqPos + 1)).trim()
					);
		}

		// now validate any attributes we can
		//
		// Boolean attributes -
		//  dataEncryption,create,createSource,convertToSource,shutdown,upgrade,current


		checkBoolean(finfo, Attribute.DATA_ENCRYPTION);
		checkBoolean(finfo, Attribute.CREATE_ATTR);
		checkBoolean(finfo, Attribute.SHUTDOWN_ATTR);
        checkBoolean(finfo, Attribute.DEREGISTER_ATTR);
		checkBoolean(finfo, Attribute.UPGRADE_ATTR);

		return finfo;
	}

	private static void checkBoolean(Properties set, String attribute) throws SQLException
    {
        final String[] booleanChoices = {"true", "false"};
        checkEnumeration( set, attribute, booleanChoices);
	}


	private static void checkEnumeration(Properties set, String attribute, String[] choices) throws SQLException
    {
		String value = set.getProperty(attribute);
		if (value == null)
			return;

        for( int i = 0; i < choices.length; i++)
        {
            if( value.toUpperCase(java.util.Locale.ENGLISH).equals( choices[i].toUpperCase(java.util.Locale.ENGLISH)))
                return;
        }

        // The attribute value is invalid. Construct a string giving the choices for
        // display in the error message.
        String choicesStr = "{";
        for( int i = 0; i < choices.length; i++)
        {
            if( i > 0)
                choicesStr += "|";
            choicesStr += choices[i];
        }
        
		throw Util.generateCsSQLException(
                SQLState.INVALID_ATTRIBUTE, attribute, value, choicesStr + "}");
	}


	/**
		Get the database name from the url.
		Copes with three forms

		jdbc:derby:dbname
		jdbc:derby:dbname;...
		jdbc:derby:;subname=dbname

		@param url The url being used for the connection
		@param info The properties set being used for the connection, must include
		the properties derived from the attributes in the url

		@return a String containing the database name or an empty string ("") if
		no database name is present in the URL.
	*/
	public static String getDatabaseName(String url, Properties info) {

		if (url.equals(Attribute.SQLJ_NESTED))
		{
			return "";
		}	
		
		// skip the jdbc:derby:
		int attributeStart = url.indexOf(';');
		String dbname;
		if (attributeStart == -1)
			dbname = url.substring(Attribute.PROTOCOL.length());
		else
			dbname = url.substring(Attribute.PROTOCOL.length(), attributeStart);

		// For security reasons we rely on here an non-null string being
		// taken as the database name, before the databaseName connection
		// attribute. Specifically, even if dbname is blank we still we
		// to use it rather than the connection attribute, even though
		// it will end up, after the trim, as a zero-length string.
		// See EmbeddedDataSource.update()

		if (dbname.length() == 0) {
		    if (info != null)
				dbname = info.getProperty(Attribute.DBNAME_ATTR, dbname);
		}
		// Beetle 4653 - trim database name to remove blanks that might make a difference on finding the database
		// on unix platforms
		dbname = dbname.trim();

		return dbname;
	}

	public final ContextService getContextServiceFactory() {
		return contextServiceFactory;
	}

	// returns the authenticationService handle
	public AuthenticationService getAuthenticationService() {
		//
		// If authenticationService handle not cached in yet, then
		// ask the monitor to find it for us and set it here in its
		// attribute.
		//
		if (this.authenticationService == null) {
			this.authenticationService = (AuthenticationService)
				findService(AuthenticationService.MODULE,
									"authentication"
								   );
		}

		return this.authenticationService;
	}

	/*
		Methods to be overloaded in sub-implementations such as
		a tracing driver.
	 */
    EmbedConnection getNewEmbedConnection( final String url, final Properties info)
        throws SQLException
    {
        final   InternalDriver  myself = this;

        try {
            return AccessController.doPrivileged
                (
                 new PrivilegedExceptionAction<EmbedConnection>()
                 {
                     public EmbedConnection run()
                         throws SQLException
                     {
                         return new EmbedConnection(myself, url, info);
                     }
                 }
                 );
        } catch (PrivilegedActionException pae)
        {
            Throwable   cause = pae.getCause();
            if ( (cause != null) && (cause instanceof SQLException) )
            {
                throw (SQLException) cause;
            }
            else
            {
                throw Util.javaException( pae );
            }
        }
    }

	private ConnectionContext getConnectionContext() {

		/*
		** The current connection is the one in the current
		** connection context, so get the context.
		*/
		ContextManager	cm = getCurrentContextManager();

		ConnectionContext localCC = null;

		/*
			cm is null the very first time, and whenever
			we aren't actually nested.
		 */
		if (cm != null) {
			localCC = (ConnectionContext)
				(cm.getContext(ConnectionContext.CONTEXT_ID));
		}

		return localCC;
	}

	private ContextManager getCurrentContextManager() {
		return getContextServiceFactory().getCurrentContextManager();
	}


	/**
		Return true if this driver is active. Package private method.
	*/
	public boolean isActive() {
		return active;
	}

	/**
 	 * Get a new nested connection.
	 *
	 * @param conn	The EmbedConnection.
	 *
	 * @return A nested connection object.
	 *
	 */
    public Connection getNewNestedConnection(EmbedConnection conn) {
        return new EmbedConnection(conn);
    }

	/*
	** methods to be overridden by subimplementations wishing to insert
	** their classes into the mix.
	*/

    public Statement newEmbedStatement(
				EmbedConnection conn,
				boolean forMetaData,
				int resultSetType,
				int resultSetConcurrency,
                int resultSetHoldability)
    {
        return new EmbedStatement(conn, forMetaData, resultSetType,
                resultSetConcurrency, resultSetHoldability);
    }

	/**
	 	@exception SQLException if fails to create statement
	 */
    public PreparedStatement newEmbedPreparedStatement(
				EmbedConnection conn,
				String stmt, 
				boolean forMetaData, 
				int resultSetType,
				int resultSetConcurrency,
				int resultSetHoldability,
				int autoGeneratedKeys,
				int[] columnIndexes,
				String[] columnNames)
        throws SQLException
    {
        return new EmbedPreparedStatement(conn,
                stmt, forMetaData, resultSetType, resultSetConcurrency,
                resultSetHoldability, autoGeneratedKeys, columnIndexes,
                columnNames);
    }

	/**
	 	@exception SQLException if fails to create statement
	 */
    public CallableStatement newEmbedCallableStatement(
				EmbedConnection conn,
				String stmt, 
				int resultSetType,
				int resultSetConcurrency,
				int resultSetHoldability)
        throws SQLException
    {
        return new EmbedCallableStatement(conn, stmt, resultSetType,
                resultSetConcurrency, resultSetHoldability);
    }

	/**
	 * Return a new java.sql.DatabaseMetaData instance for this implementation.
	 	@exception SQLException on failure to create.
	 */
    public DatabaseMetaData newEmbedDatabaseMetaData(
            EmbedConnection conn, String dbname) throws SQLException {
        return new EmbedDatabaseMetaData(conn, dbname);
    }

	/**
	 * Return a new java.sql.ResultSet instance for this implementation.
	 * @param conn Owning connection
	 * @param results Top level of language result set tree
	 * @param forMetaData Is this for meta-data
	 * @param statement The statement that is creating the SQL ResultSet
	 * @param isAtomic 
	 * @return a new java.sql.ResultSet
	 * @throws SQLException
	 */
    public EmbedResultSet newEmbedResultSet(EmbedConnection conn,
            ResultSet results, boolean forMetaData, EmbedStatement statement,
            boolean isAtomic) throws SQLException {
        return new EmbedResultSet(conn, results, forMetaData, statement,
                isAtomic);
    }
        
    /**
     * Returns a new java.sql.ResultSetMetaData for this implementation
     *
     * @param columnInfo a ResultColumnDescriptor that stores information
     *        about the columns in a ResultSet
     */
    public EmbedResultSetMetaData newEmbedResultSetMetaData(
            ResultColumnDescriptor[] columnInfo) {
        return new EmbedResultSetMetaData(columnInfo);
    }

    /**
     * Return a new BrokeredConnection for this implementation.
     */
    BrokeredConnection newBrokeredConnection(
            BrokeredConnectionControl control) throws SQLException {
        return new BrokeredConnection(control);
    }

    /**
     * Create and return an EmbedPooledConnection from the received instance of
     * EmbeddedDataSource.
     */
    protected PooledConnection getNewPooledConnection(
            BasicEmbeddedDataSource40 eds, String user, String password,
            boolean requestPassword) throws SQLException {
        return new EmbedPooledConnection(eds, user, password, requestPassword);
    }

    /**
     * Create and return an EmbedXAConnection from the received instance of
     * BasicEmbeddedDataSource40.
     */
    protected XAConnection getNewXAConnection(
            BasicEmbeddedDataSource40 eds, ResourceAdapter ra,
            String user, String password, boolean requestPassword)
            throws SQLException {
        return new EmbedXAConnection(eds, ra, user, password, requestPassword);
    }

    private static final String[] BOOLEAN_CHOICES = {"false", "true"};

    /**
     * <p>The getPropertyInfo method is intended to allow a generic GUI tool to
     * discover what properties it should prompt a human for in order to get
     * enough information to connect to a database.  Note that depending on
     * the values the human has supplied so far, additional values may become
     * necessary, so it may be necessary to iterate though several calls
     * to getPropertyInfo.
     *
     * @param url The URL of the database to connect to.
     * @param info A proposed list of tag/value pairs that will be sent on
     *          connect open.
     * @return An array of DriverPropertyInfo objects describing possible
     *          properties.  This array may be an empty array if no properties
     *          are required.
     * @exception SQLException if a database-access error occurs.
     */
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
            throws SQLException {

        // RESOLVE other properties should be added into this method in the future ...

        if (info != null) {
            if (Boolean.valueOf(info.getProperty(Attribute.SHUTDOWN_ATTR)).booleanValue()) {

                // no other options possible when shutdown is set to be true
                return new DriverPropertyInfo[0];
            }
        }

        // at this point we have databaseName,

        String dbname = InternalDriver.getDatabaseName(url, info);

        // convert the ;name=value attributes in the URL into
        // properties.
        FormatableProperties finfo = getAttributes(url, info);
        info = null; // ensure we don't use this reference directly again.
        boolean encryptDB = Boolean.valueOf(finfo.getProperty(Attribute.DATA_ENCRYPTION)).booleanValue();
        String encryptpassword = finfo.getProperty(Attribute.BOOT_PASSWORD);

        if (dbname.length() == 0 || (encryptDB && encryptpassword == null)) {

            // with no database name we can have shutdown or a database name

            // In future, if any new attribute info needs to be included in this
            // method, it just has to be added to either string or boolean or secret array
            // depending on whether it accepts string or boolean or secret(ie passwords) value.

            String[][] connStringAttributes = {
                {Attribute.DBNAME_ATTR, MessageId.CONN_DATABASE_IDENTITY},
                {Attribute.CRYPTO_PROVIDER, MessageId.CONN_CRYPTO_PROVIDER},
                {Attribute.CRYPTO_ALGORITHM, MessageId.CONN_CRYPTO_ALGORITHM},
                {Attribute.CRYPTO_KEY_LENGTH, MessageId.CONN_CRYPTO_KEY_LENGTH},
                {Attribute.CRYPTO_EXTERNAL_KEY, MessageId.CONN_CRYPTO_EXTERNAL_KEY},
                {Attribute.TERRITORY, MessageId.CONN_LOCALE},
                {Attribute.COLLATION, MessageId.CONN_COLLATION},
                {Attribute.USERNAME_ATTR, MessageId.CONN_USERNAME_ATTR},
                {Attribute.LOG_DEVICE, MessageId.CONN_LOG_DEVICE},
                {Attribute.ROLL_FORWARD_RECOVERY_FROM, MessageId.CONN_ROLL_FORWARD_RECOVERY_FROM},
                {Attribute.CREATE_FROM, MessageId.CONN_CREATE_FROM},
                {Attribute.RESTORE_FROM, MessageId.CONN_RESTORE_FROM},
            };

            String[][] connBooleanAttributes = {
                {Attribute.SHUTDOWN_ATTR, MessageId.CONN_SHUT_DOWN_CLOUDSCAPE},
                {Attribute.DEREGISTER_ATTR, MessageId.CONN_DEREGISTER_AUTOLOADEDDRIVER},
                {Attribute.CREATE_ATTR, MessageId.CONN_CREATE_DATABASE},
                {Attribute.DATA_ENCRYPTION, MessageId.CONN_DATA_ENCRYPTION},
                {Attribute.UPGRADE_ATTR, MessageId.CONN_UPGRADE_DATABASE},
                };

            String[][] connStringSecretAttributes = {
                {Attribute.BOOT_PASSWORD, MessageId.CONN_BOOT_PASSWORD},
                {Attribute.PASSWORD_ATTR, MessageId.CONN_PASSWORD_ATTR},
                };


            DriverPropertyInfo[] optionsNoDB = new  DriverPropertyInfo[connStringAttributes.length+
                                                                      connBooleanAttributes.length+
                                                                      connStringSecretAttributes.length];

            int attrIndex = 0;
            for( int i = 0; i < connStringAttributes.length; i++, attrIndex++ )
            {
                optionsNoDB[attrIndex] = new DriverPropertyInfo(connStringAttributes[i][0],
                                      finfo.getProperty(connStringAttributes[i][0]));
                optionsNoDB[attrIndex].description = MessageService.getTextMessage(connStringAttributes[i][1]);
            }

            optionsNoDB[0].choices = getMonitor().getServiceList(Property.DATABASE_MODULE);
            // since database name is not stored in FormatableProperties, we
            // assign here explicitly
            optionsNoDB[0].value = dbname;

            for( int i = 0; i < connStringSecretAttributes.length; i++, attrIndex++ )
            {
                optionsNoDB[attrIndex] = new DriverPropertyInfo(connStringSecretAttributes[i][0],
                                      (finfo.getProperty(connStringSecretAttributes[i][0]) == null? "" : "****"));
                optionsNoDB[attrIndex].description = MessageService.getTextMessage(connStringSecretAttributes[i][1]);
            }

            for( int i = 0; i < connBooleanAttributes.length; i++, attrIndex++ )
            {
                optionsNoDB[attrIndex] = new DriverPropertyInfo(connBooleanAttributes[i][0],
                    Boolean.valueOf(finfo == null? "" : finfo.getProperty(connBooleanAttributes[i][0])).toString());
                optionsNoDB[attrIndex].description = MessageService.getTextMessage(connBooleanAttributes[i][1]);
                optionsNoDB[attrIndex].choices = BOOLEAN_CHOICES;
            }

            return optionsNoDB;
        }

        return new DriverPropertyInfo[0];
    }

    public Connection connect(String url, Properties info) throws SQLException {
        return connect(url, info, DriverManager.getLoginTimeout());
    }

    ////////////////////////////////////////////////////////////////////
    //
    // INTRODUCED BY JDBC 4.1 IN JAVA 7
    //
    ////////////////////////////////////////////////////////////////////

    public Logger getParentLogger()
            throws SQLFeatureNotSupportedException {
        throw (SQLFeatureNotSupportedException)
                Util.notImplemented("getParentLogger()");
    }

    /**
     * Indicate to {@code AutoloadedDriver} whether it should deregister
     * itself on shutdown.
     *
     * @param deregister whether or not {@code AutoloadedDriver} should
     * deregister itself
     */
    static void setDeregister(boolean deregister) {
        InternalDriver.deregister = deregister;
    }

    /**
     * Check whether {@code AutoloadedDriver} should deregister itself on
     * shutdown.
     *
     * @return the deregister value
     */
    static boolean getDeregister() {
        return InternalDriver.deregister;
    }

    
    /**
     * Privileged lookup of the ContextService. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ContextService    getContextService()
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<ContextService>()
             {
                 public ContextService run()
                 {
                     return ContextService.getFactory();
                 }
             }
             );
    }

    
    /**
     * Privileged Monitor lookup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ModuleFactory  getMonitor()
    {
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
     * Privileged module lookup. Must be private so that user code
     * can't call this entry point.
     */
    private static  Object getSystemModule( final String factoryInterface )
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<Object>()
             {
                 public Object run()
                 {
                     return Monitor.getSystemModule( factoryInterface );
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
