/*

   Derby - Class org.apache.derby.jdbc.EmbeddedSimpleDataSource

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

import org.apache.derby.iapi.jdbc.JDBCBoot;
import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.iapi.reference.MessageId;

import java.sql.Connection;
import java.sql.SQLException;

import java.io.PrintWriter;
import java.util.Properties;

/* -- New jdbc 20 extension types --- */
import javax.sql.DataSource;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.impl.jdbc.Util;

/**
 * 
 * 
 * EmbeddedSimpleDataSource is Derby's DataSource implementation
 * for J2ME/CDC/Foundation. It is also supports J2SE platforms.
 * 
 * 
 * Supports the same properties as EmbeddedDataSource, see that class for details.
 * <P>
	EmbeddedSimpleDataSource automatically supports the correct JDBC specification version
	for the Java Virtual Machine's environment.
	<UL>
	<LI> JDBC Optional Package for CDC/Foundation Profile(JSR-169) - J2ME - CDC/Foundation
	<LI> JDBC 3.0 - Java 2 - JDK 1.4, J2SE 5.0
	</UL> 
 * @see EmbeddedDataSource
 *
 */
public final class EmbeddedSimpleDataSource implements DataSource {

	private String password;

	private String user;

	/**
	 * The database name.
	 * 
	 * @serial
	 */
	private String databaseName;

	/**
	 * The data source name.
	 * 
	 * @serial
	 */
	private String dataSourceName;

	/**
	 * Description of the database.
	 * 
	 * @serial
	 */
	private String description;

	/**
	 * Set to "create" if the database should be created.
	 * 
	 * @serial
	 */
	private String createDatabase;

	/**
	 * Set to "shutdown" if the database should be shutdown.
	 * 
	 * @serial
	 */
	private String shutdownDatabase;

	/**
	 * Derby specific connection attributes.
	 * 
	 * @serial
	 */
	private String connectionAttributes;

	/** instance variables that will not be serialized */
	transient private PrintWriter printer;

	transient private int loginTimeout;

	// Unlike a DataSource, LocalDriver is shared by all
	// Derby databases in the same jvm.
	transient private InternalDriver driver;

	transient private String jdbcurl;

	/**
	 * No-arg constructor.
	 */
	public EmbeddedSimpleDataSource() {
	}

	/*
	 * DataSource methods
	 */

	/**
	 * Gets the maximum time in seconds that this data source can wait while
	 * attempting to connect to a database. A value of zero means that the
	 * timeout is the default system timeout if there is one; otherwise it means
	 * that there is no timeout. When a data source object is created, the login
	 * timeout is initially zero.
	 * 
	 * @return the data source login time limit
	 * @exception SQLException
	 *                if a database access error occurs.
	 */
	public int getLoginTimeout() throws SQLException {
		return loginTimeout;
	}

	/**
	 * Sets the maximum time in seconds that this data source will wait while
	 * attempting to connect to a database. A value of zero specifies that the
	 * timeout is the default system timeout if there is one; otherwise it
	 * specifies that there is no timeout. When a data source object is created,
	 * the login timeout is initially zero.
	 * <P>
	 * Derby ignores this property.
	 * 
	 * @param seconds
	 *            the data source login time limit
	 * @exception SQLException
	 *                if a database access error occurs.
	 */
	public void setLoginTimeout(int seconds) throws SQLException {
		loginTimeout = seconds;
	}

	/**
	 * Get the log writer for this data source.
	 * 
	 * <p>
	 * The log writer is a character output stream to which all logging and
	 * tracing messages for this data source object instance will be printed.
	 * This includes messages printed by the methods of this object, messages
	 * printed by methods of other objects manufactured by this object, and so
	 * on. Messages printed to a data source specific log writer are not printed
	 * to the log writer associated with the java.sql.Drivermanager class. When
	 * a data source object is created the log writer is initially null, in
	 * other words, logging is disabled.
	 * 
	 * @return the log writer for this data source, null if disabled
	 * @exception SQLException
	 *                if a database-access error occurs.
	 */
	public PrintWriter getLogWriter() throws SQLException {
		return printer;
	}

	/**
	 * Set the log writer for this data source.
	 * 
	 * <p>
	 * The log writer is a character output stream to which all logging and
	 * tracing messages for this data source object instance will be printed.
	 * This includes messages printed by the methods of this object, messages
	 * printed by methods of other objects manufactured by this object, and so
	 * on. Messages printed to a data source specific log writer are not printed
	 * to the log writer associated with the java.sql.Drivermanager class. When
	 * a data source object is created the log writer is initially null, in
	 * other words, logging is disabled.
	 * 
	 * @param out
	 *            the new log writer; to disable, set to null
	 * @exception SQLException
	 *                if a database-access error occurs.
	 */
	public void setLogWriter(PrintWriter out) throws SQLException {
		printer = out;
	}

	/*
	 * Properties to be seen by Bean - access thru reflection.
	 */
	/**
	 * Set the database name. Setting this property is mandatory. If a database
	 * named wombat at g:/db needs to be accessed, database name should be set
	 * to "g:/db/wombat". The database will be booted if it is not already
	 * running in the system.
	 * 
	 * @param databaseName
	 *            the name of the database
	 */
	public final synchronized void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
		update();
	}

	public String getDatabaseName() {
		return databaseName;
	}

	/**
	 * Set the data source name. The property is not mandatory. It is used for
	 * informational purposes only.
	 * 
	 * @param dsn
	 *            the name of the data source
	 */
	public final void setDataSourceName(String dsn) {
		dataSourceName = dsn;
	}

	/** @return data source name */
	public final String getDataSourceName() {
		return dataSourceName;
	}

	/**
	 * Set the data source descripton. This property is not mandatory. It is
	 * used for informational purposes only.
	 * 
	 * @param desc
	 *            the description of the data source
	 */
	public final void setDescription(String desc) {
		description = desc;
	}

	/** @return description */
	public final String getDescription() {
		return description;
	}

	/**
	 * Set the <code>user</code> property for the data source. This is user
	 * name for any data source getConnection() call that takes no arguments.
	 */
	public final void setUser(String user) {
		this.user = user;
	}

	/** @return user */
	public final String getUser() {
		return user;
	}

	/**
	 * Set the <code>password</code> property for the data source. This is
	 * user's password for any data source getConnection() call that takes no
	 * arguments.
	 */
	public final void setPassword(String password) {
		this.password = password;
	}

	/** @return password */
	public final String getPassword() {
		return password;
	}

	/**
	 * Set this property to create a new database. If this property is not set,
	 * the database (identified by databaseName) is assumed to be already
	 * existing.
	 * 
	 * @param create
	 *            if set to the string "create", this data source will try to
	 *            create a new database of databaseName, or boot the database if
	 *            one by that name already exists.
	 */
	public final void setCreateDatabase(String create) {
		if (create != null
				&& create.toLowerCase(java.util.Locale.ENGLISH)
						.equals("create"))
			createDatabase = create;
		else
			createDatabase = null;
	}

	/** @return "create" if create is set, or null if not */
	public final String getCreateDatabase() {
		return createDatabase;
	}

	/**
	 * Set this property if one wishes to shutdown the database identified by
	 * databaseName.
	 * 
	 * @param shutdown
	 *            if set to the string "shutdown", this data source will
	 *            shutdown the database if it is running.
	 */
	public final void setShutdownDatabase(String shutdown) {
		if (shutdown != null && shutdown.equalsIgnoreCase("shutdown"))
			shutdownDatabase = shutdown;
		else
			shutdownDatabase = null;
	}

	/** @return "shutdown" if shutdown is set, or null if not */
	public final String getShutdownDatabase() {
		return shutdownDatabase;
	}

	/**
	 * Set this property to pass in more Derby specific connection URL
	 * attributes.
	<BR>
   Any attributes that can be set using a property of this DataSource implementation
   (e.g user, password) should not be set in connectionAttributes. Conflicting
   settings in connectionAttributes and properties of the DataSource will lead to
   unexpected behaviour. 
   	 * 
	 * @param prop
	 *            set to the list of Derby connection attributes separated
	 *            by semi-colons. E.g., to specify an encryption bootPassword of
	 *            "x8hhk2adf", and set upgrade to true, do the following:
	 * 
	 * <PRE>
	 * 
	 * ds.setConnectionAttributes("bootPassword=x8hhk2adf;upgrade=true");
	 * 
	 * </PRE>
	 * 
	 * See Derby's documentation for complete list.
	 */
	public final void setConnectionAttributes(String prop) {
		connectionAttributes = prop;
		update();
	}

	/** @return Derby specific connection URL attributes */
	public final String getConnectionAttributes() {
		return connectionAttributes;
	}

	/*
	 * DataSource methods
	 */

	/**
	 * Attempt to establish a database connection.
	 * 
	 * @return a Connection to the database
	 * @exception SQLException
	 *                if a database-access error occurs.
	 */
	public final Connection getConnection() throws SQLException {
		return this.getConnection(getUser(), getPassword());
	}

	/**
	 * Attempt to establish a database connection with the given username and
	 * password. If the attributeAsPassword property is set to true then the
	 * password argument is taken to be a list of connection attributes with the
	 * same format as the connectionAttributes property.
	 * 
	 * 
	 * @param username
	 *            the database user on whose behalf the Connection is being made
	 * @param password
	 *            the user's password
	 * @return a Connection to the database
	 * @exception SQLException
	 *                if a database-access error occurs.
	 */
	public final Connection getConnection(String username, String password)
			throws SQLException {

		Properties info = new Properties();
		if (username != null)
			info.put(Attribute.USERNAME_ATTR, username);

		if (password != null)
			info.put(Attribute.PASSWORD_ATTR, password);

		if (createDatabase != null)
			info.put(Attribute.CREATE_ATTR, "true");
		if (shutdownDatabase != null)
			info.put(Attribute.SHUTDOWN_ATTR, "true");

		Connection conn = findDriver().connect(jdbcurl, info);

		// JDBC driver's getConnection method returns null if
		// the driver does not handle the request's URL.
		if (conn == null)
			throw Util.generateCsSQLException(SQLState.PROPERTY_INVALID_VALUE,
					Attribute.DBNAME_ATTR, getDatabaseName());

		return conn;
	}

	private InternalDriver findDriver() throws SQLException {
		String url = jdbcurl;

		if (driver == null || !driver.acceptsURL(url)) {
			synchronized (this) {
				// The driver has either never been booted, or it has been
				// shutdown by a 'jdbc:derby:;shutdown=true'
				if (driver == null || !driver.acceptsURL(url)) {
					
					
					new JDBCBoot().boot(Attribute.PROTOCOL, System.err);
					
					// If we know the driver, we loaded it. Otherwise only
					// work if DriverManager has already loaded it.

					driver = InternalDriver.activeDriver();
					
					if (driver == null)
						throw new SQLException(MessageService.getTextMessage(MessageId.CORE_JDBC_DRIVER_UNREGISTERED));
				}
			}
		}
		return driver;
		// else driver != null and driver can accept url
	}

	private void update() {
		StringBuffer sb = new StringBuffer(64);

		sb.append(Attribute.PROTOCOL);

		// Set the database name from the databaseName property
		String dbName = getDatabaseName();

		if (dbName != null) {
			dbName = dbName.trim();
		}

		if (dbName == null || dbName.length() == 0) {
			// need to put something in so that we do not allow the
			// database name to be set from the request or from the
			// connection attributes.

			// this space will selected as the database name (and trimmed to an
			// empty string)
			// See the getDatabaseName() code in InternalDriver. Since this is a
			// non-null
			// value, it will be selected over any databaseName connection
			// attribute.
			dbName = " ";
		}

		sb.append(dbName);

		String connAttrs = getConnectionAttributes();
		if (connAttrs != null) {
			connAttrs = connAttrs.trim();
			if (connAttrs.length() != 0) {
				sb.append(';');
				sb.append(connectionAttributes);
			}
		}

		jdbcurl = sb.toString();
	}
}

