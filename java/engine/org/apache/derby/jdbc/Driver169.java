/*

   Derby - Class org.apache.derby.jdbc.Driver169

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

package org.apache.derby.jdbc;

import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.services.info.ProductVersionHolder;
import org.apache.derby.iapi.services.io.FormatableProperties;

import org.apache.derby.iapi.jdbc.ConnectionContext;

import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.i18n.MessageService;

import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.jdbc.AuthenticationService;

import org.apache.derby.impl.jdbc.*;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import java.util.Properties;
import java.util.StringTokenizer;


/**
	A Local JDBC Driver. Note that this is an implementation of a 
	protocol that is not defined by Cloudscape, i.e. we are viewing
	java.sql as a protocol. But this module should not be accessed
	through findModule(), the correct way to get obtain JDBC drivers
	is through java.sql.DriverManager.

	@author djd
*/

public abstract class Driver169 implements ModuleControl {

	protected boolean active;
	private ContextService contextServiceFactory;
	private AuthenticationService	authenticationService;

	public Driver169() {
		contextServiceFactory = ContextService.getFactory();
	}

	/*
	**	Methods from ModuleControl
	*/

	public void boot(boolean create, Properties properties) throws StandardException {

		active = true;
	}

	public void stop() {

		active = false;

		contextServiceFactory = null;
	}

	/*
	** Methods from java.sql.Driver
	*/
	public boolean acceptsURL(String url) {
		return active && (url.startsWith(Attribute.PROTOCOL) || url.equals(Attribute.SQLJ_NESTED));
	}

	public Connection connect(String url, Properties info)
		 throws SQLException 
	{
		if (!acceptsURL(url)) { return null; }

			
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
			// there is no cloudscape connection, so
			// return null, as we are not the driver to handle this
			return null;
		}

		// convert the ;name=value attributes in the URL into
		// properties.
		FormatableProperties finfo = getAttributes(url, info);
		info = null; // ensure we don't use this reference directly again.
		try {

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
				if (Driver169.getDatabaseName(url, finfo).length() == 0) {
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
                                  SQLState.LOGIN_FAILED, MessageService.getTextMessage(MessageId.AUTH_INVALID));
					}

					Monitor.getMonitor().shutdown();
					throw Util.generateCsSQLException(
                                         SQLState.CLOUDSCAPE_SYSTEM_SHUTDOWN);
				}
			}
			
			EmbedConnection conn = getNewEmbedConnection(url, finfo);

			// if this is not the correct driver a EmbedConnection
			// object is returned in the closed state.
			if (conn.isClosed()) {
				return null;
			}

			return conn;
		}
		finally {
			// break any link with the user's Properties set.
			finfo.clearDefaults();
		}
	}

	public int getMajorVersion() {
		return Monitor.getMonitor().getEngineVersion().getMajorVersion();
	}
	
	public int getMinorVersion() {
		return Monitor.getMonitor().getEngineVersion().getMinorVersion();
	}

	public boolean jdbcCompliant() {
		return false;
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
				Monitor.findService(AuthenticationService.MODULE,
									"authentication"
								   );
		}

		// We should have a Authentication Service (always)
		//
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(this.authenticationService != null, 
				"Unexpected - There is no valid authentication service!");
		}
		return this.authenticationService;
	}

	/*
		Methods to be overloaded in sub-implementations such as
		a tracing driver.
	 */
	protected abstract EmbedConnection getNewEmbedConnection(String url, Properties info) 
		 throws SQLException ;
//	{
		// make a new local connection with a new transaction resource
//		return new EmbedConnection(this, url, info);
//	}

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
	public abstract Connection getNewNestedConnection(EmbedConnection conn);

	/*
	** methods to be overridden by subimplementations wishing to insert
	** their classes into the mix.
	*/

	public java.sql.Statement newEmbedStatement(
				EmbedConnection conn,
				boolean forMetaData,
				int resultSetType,
				int resultSetConcurrency,
				int resultSetHoldability)
	{
		return new EmbedStatement(conn, forMetaData, resultSetType, resultSetConcurrency,
		resultSetHoldability);
	}
	/**
	 	@exception SQLException if fails to create statement
	 */
	public abstract java.sql.PreparedStatement newEmbedPreparedStatement(
				EmbedConnection conn,
				String stmt, 
				boolean forMetaData, 
				int resultSetType,
				int resultSetConcurrency,
				int resultSetHoldability,
				int autoGeneratedKeys,
				int[] columnIndexes,
				String[] columnNames)
		throws SQLException;
//	{
//		return new EmbedPreparedStatement(conn,stmt,forMetaData, resultSetType,
//		resultSetConcurrency, resultSetHoldability, autoGeneratedKeys, columnIndexes,
//		columnNames);
//	}
	/**
	 	@exception SQLException if fails to create statement
	 */
	public abstract java.sql.CallableStatement newEmbedCallableStatement(
				EmbedConnection conn,
				String stmt, 
				int resultSetType,
				int resultSetConcurrency,
				int resultSetHoldability)
		throws SQLException;
//	{
//		return new EmbedCallableStatement(conn,stmt, resultSetType,
//		resultSetConcurrency, resultSetHoldability);
//	}
	/**
	 	@exception StandardException if fails to create metadata
	 	@exception SQLException on SPS property error
	 */
	public DatabaseMetaData newEmbedDatabaseMetaData(EmbedConnection conn,
		String dbname) throws SQLException {
		return new EmbedDatabaseMetaData(conn,dbname);
	}

	public abstract EmbedResultSet 
		newEmbedResultSet(EmbedConnection conn, ResultSet results, boolean forMetaData, EmbedStatement statement, boolean isAtomic) ;
//	{
//		return new EmbedResultSet(conn, results, forMetaData, statement, isAtomic, false);
//	}



}



