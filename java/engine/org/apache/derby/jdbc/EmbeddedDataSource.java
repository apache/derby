/*

   Derby - Class org.apache.derby.jdbc.EmbeddedDataSource

   Copyright 2001, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import java.io.PrintWriter;
import java.util.Properties;

/* -- New jdbc 20 extension types --- */
import javax.sql.DataSource;


import org.apache.derby.iapi.reference.Attribute;

/** 
	

	EmbeddedDataSource is Cloudscape's DataSource implementation.
	

	<P>A DataSource  is a factory for Connection objects. An object that
	implements the DataSource interface will typically be registered with a
	JNDI service provider.
	<P>
	EmbeddedDataSource automatically supports the correct JDBC specification version
	for the Java Virtual Machine's environment.
	<UL>
	<LI> JDBC 3.0 - Java 2 - JDK 1.4
	<LI> JDBC 2.0 - Java 2 - JDK 1.2,1.3
	</UL>

	<P>The following is a list of properties that can be set on a Cloudscape
	DataSource object:
	<P><B>Standard DataSource properties</B> (from JDBC 3.0 specification).

	<UL><LI><B><code>databaseName</code></B> (String): <I>Mandatory</I>
	<BR>This property must be set and it
	identifies which database to access.  If a database named wombat located at
	g:/db/wombat is to be accessed, then one should call
	<code>setDatabaseName("g:/db/wombat")</code> on the data source object.</LI>

	<LI><B><code>dataSourceName</code></B> (String): <I>Optional</I>
	<BR> Name for DataSource.  Not used by the data source object.  Used for
	informational purpose only.</LI>

	<LI><B><code>description</code></B> (String): <I>Optional</I>
	<BR>Description of the data source.  Not
	used by the data source object.  Used for informational purpose only.</LI> 

	<LI><B><code>password</code></B> (String): <I>Optional</I>
	<BR>Database password for the no argument <code>DataSource.getConnection()</code>,
	<code>ConnectionPoolDataSource.getPooledConnection()</code>
	and <code>XADataSource.getXAConnection()</code> methods.

	<LI><B><code>user</code></B> (String): <I>Optional</I>
	<BR>Database user for the no argument <code>DataSource.getConnection()</code>,
	<code>ConnectionPoolDataSource.getPooledConnection()</code>
	and <code>XADataSource.getXAConnection()</code> methods.
	</UL>

	<BR><B>Cloudscape specific DataSource properties.</B>

  <UL>

  <LI><B><code>attributesAsPassword</code></B> (Boolean): <I>Optional</I>
	<BR>If true, treat the password value in a
	<code>DataSource.getConnection(String user, String password)</code>,
	<code>ConnectionPoolDataSource.getPooledConnection(String user, String password)</code>
	or <code>XADataSource.getXAConnection(String user, String password)</code> as a set
	of connection attributes. The format of the attributes is the same as the format
	of the attributes in the property connectionAttributes. If false the password value
	is treated normally as the password for the given user.
	Setting this property to true allows a connection request from an application to
	provide more authentication information that just a password, for example the request
	can include the user's password and an encrypted database's boot password.</LI>

  <LI><B><code>connectionAttributes</code></B> (String): <I>Optional</I>
  <BR>Defines a set of Cloudscape connection attributes for use in all connection requests.
  The format of the String matches the format of the connection attributes in a Cloudscape JDBC URL.
  That is a list of attributes in the form <code><I>attribute</I>=<I>value</I></code>, each separated by semi-colon (';').
  E.g. <code>setConnectionAttributes("bootPassword=erd3234dggd3kazkj3000");</code>.
  <BR>The database name must be set by the DataSource property <code>databaseName</code> and not by setting the <code>databaseName</code>
  connection attribute in the <code>connectionAttributes</code> property.
  <BR>Please see Cloudscape's documentation for a complete list of connection attributes. </LI>

  <LI><B><code>createDatabase</code></B> (String): <I>Optional</I>
	<BR>If set to the string "create", this will
	cause a new database of <code>databaseName</code> if that database does not already
	exist.  The database is created when a connection object is obtained from
	the data source. </LI> 

	<LI><B><code>shutdownDatabase</code></B> (String): <I>Optional</I>
	<BR>If set to the string "shutdown",
	this will cause the database to shutdown when a java.sql.Connection object
	is obtained from the data source.  E.g., If the data source is an
	XADataSource, a getXAConnection().getConnection() is necessary to cause the
	database to shutdown.

	</UL>

	<P><B>Examples.</B>

	<P>This is an example of setting a property directly using Cloudscape's
	EmbeddedDataSource object.  This code is typically written by a system integrator :
	<PRE> 
	*
	* import org.apache.derby.jdbc.*;
	*
	* // dbname is the database name
	* // if create is true, create the database if necessary
	* javax.sql.DataSource makeDataSource (String dbname, boolean create)
	*	throws Throwable 
	* { 
	*	EmbeddedDataSource ds = new EmbeddedDataSource(); 
	*	ds.setDatabaseName(dbname);
	*
	*	if (create)
	*		ds.setCreateDatabase("create");
    *   
	*	return ds;
	* }
	</PRE>

	<P>Example of setting properties thru reflection.  This code is typically
	generated by tools or written by a system integrator: <PRE>
	*	
	* javax.sql.DataSource makeDataSource(String dbname) 
	*	throws Throwable 
	* {
	*	Class[] parameter = new Class[1];
	*	parameter[0] = dbname.getClass();
	*	DataSource ds =  new EmbeddedDataSource();
	*	Class cl = ds.getClass();
	*
	*	Method setName = cl.getMethod("setDatabaseName", parameter);
	*	Object[] arg = new Object[1];
	*	arg[0] = dbname;
	*	setName.invoke(ds, arg);
	*
	*	return ds;
	* }
	</PRE>

	<P>Example on how to register a data source object with a JNDI naming
	service.
	<PRE>
	* DataSource ds = makeDataSource("mydb");
	* Context ctx = new InitialContext();
	* ctx.bind("jdbc/MyDB", ds);
	</PRE>

	<P>Example on how to retrieve a data source object from a JNDI naming
	service. 
	<PRE>
	* Context ctx = new InitialContext();
	* DataSource ds = (DataSource)ctx.lookup("jdbc/MyDB");
	</PRE>

*/
public class EmbeddedDataSource extends ReferenceableDataSource implements
				javax.sql.DataSource
{

	private static final long serialVersionUID = -4945135214995641181L;

	/** instance variables that will be serialized */

	/**
	 * The database name.
	 * @serial
	 */
	private String databaseName;

	/**
	 * The data source name.
	 * @serial
	 */
	private String dataSourceName;

	/**
	 * Description of the database.
	 * @serial
	 */
	private String description;

	/**
	 * Set to "create" if the database should be created.
	 * @serial
	 */
	private String createDatabase;

	/**
	 * Set to "shutdown" if the database should be shutdown.
	 * @serial
	 */
	private String shutdownDatabase;

	/**
	 * Cloudscape specific connection attributes.
	 * @serial
	 */
	private String connectionAttributes;

	/**
		Set password to be a set of connection attributes.
	*/
	private boolean attributesAsPassword;

	/** instance variables that will not be serialized */
	transient private PrintWriter printer;
	transient private int loginTimeout;

	// Unlike a DataSource, LocalDriver is shared by all
	// Cloudscape databases in the same jvm.
	transient protected Driver169 driver;

	transient private String jdbcurl;

	/**
		No-arg constructor.
	 */
	public EmbeddedDataSource() {
		// needed by Object Factory

		// don't put anything in here or in any of the set method because this
		// object may be materialized in a remote machine and then sent thru
		// the net to the machine where it will be used.
	}


  //Most of our customers would be using jndi to get the data
  //sources. Since we don't have a jndi to test this, we are
  //adding this method to fake it. This is getting used in
  //xaJNDI test so we can compare the 2 data sources.
	public boolean equals(Object p0) {
    if (p0 instanceof EmbeddedDataSource) {
      EmbeddedDataSource ds = (EmbeddedDataSource)p0;

      boolean match = true;
      
			if (databaseName != null) {
        if  (!(databaseName.equals(ds.databaseName)))
					match = false;
			} else if (ds.databaseName != null)
        match = false;

			if (dataSourceName != null) {
        if  (!(dataSourceName.equals(ds.dataSourceName)))
					match = false;
			} else if (ds.dataSourceName != null)
        match = false;

			if (description != null) {
        if  (!(description.equals(ds.description)))
					match = false;
			} else if (ds.description != null)
        match = false;

			if (createDatabase != null) {
        if  (!(createDatabase.equals(ds.createDatabase)))
					match = false;
			} else if (ds.createDatabase != null)
        match = false;

			if (shutdownDatabase != null) {
        if  (!(shutdownDatabase.equals(ds.shutdownDatabase)))
					match = false;
			} else if (ds.shutdownDatabase != null)
        match = false;

			if (connectionAttributes != null) {
        if  (!(connectionAttributes.equals(ds.connectionAttributes)))
					match = false;
			} else if (ds.connectionAttributes != null)
        match = false;

      if (loginTimeout != ds.loginTimeout)
        match = false;

      return match;

    }

    return false;
	}

	/*
	 * Properties to be seen by Bean - access thru reflection.
	 */

	/**
		Set this property to create a new database.  If this
		property is not set, the database (identified by databaseName) is
		assumed to be already existing.

		@param create if set to the string "create", this data source will try
		to create a new database of databaseName, or boot the database if one
		by that name already exists.
	 */
	public final void setCreateDatabase(String create) {
		if (create != null && create.toLowerCase(java.util.Locale.ENGLISH).equals("create"))
			createDatabase = create;
		else
			createDatabase = null;
	}
	/** @return "create" if create is set, or null if not */
	public final String getCreateDatabase() {
		return createDatabase;
	}


	/**
 		Set this property if one wishes to shutdown the database identified by
		databaseName. 

		@param shutdown if set to the string "shutdown", this data source will 
		shutdown the database if it is running.
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
 		Set this property to pass in more Cloudscape specific
		connection URL attributes.

		@param prop set to the list of Cloudscape connection
		attributes separated by semi-colons.   E.g., to specify an encryption
		bootPassword of "x8hhk2adf", and set upgrade to true, do the following: 
		<PRE>
			ds.setConnectionAttributes("bootPassword=x8hhk2adf;upgrade=true");
		</PRE>
		See Cloudscape's documentation for complete list.
	 */
	public final void setConnectionAttributes(String prop) {
		 connectionAttributes = prop;
		 update();
	}
	/** @return Cloudscape specific connection URL attributes */
	public final String getConnectionAttributes() {
		return connectionAttributes;
	}


	/**
		Set attributeAsPassword property to enable passing connection request attributes in the password argument of getConnection.
		If the property is set to true then the password argument of the DataSource.getConnection(String user, String password)
		method call is taken to be a list of connection attributes with the same format as the connectionAttributes property.

		@param attributesAsPassword true to encode password argument as a set of connection attributes in a connection request.
	*/
	public final void setAttributesAsPassword(boolean attributesAsPassword) {
		this.attributesAsPassword = attributesAsPassword;
		 update();
	}

	/**
		Return the value of the attributesAsPassword property.
	*/
	public final boolean getAttributesAsPassword() {
		return attributesAsPassword;
	}

	/*
	 * DataSource methods 
	 */


	/**
	 * Attempt to establish a database connection.
	 *
	 * @return  a Connection to the database
	 * @exception SQLException if a database-access error occurs.
	 */
	public final Connection getConnection() throws SQLException
	{
		return this.getConnection(getUser(), getPassword(), false);
	}

	/**
	 * Attempt to establish a database connection with the given username and password.
	   If the attributeAsPassword property is set to true then the password argument is taken to be a list of
	   connection attributes with the same format as the connectionAttributes property.

	 *
	 * @param user the database user on whose behalf the Connection is 
	 *  being made
	 * @param password the user's password
	 * @return  a Connection to the database
	 * @exception SQLException if a database-access error occurs.
	 */
	public final Connection getConnection(String username, String password) 
		 throws SQLException
	{
		return this.getConnection(username, password, true);
	}

	/**
		@param	requestPassword true if the password came from the getConnection() call.
	*/
	final Connection getConnection(String username, String password, boolean requestPassword)
		throws SQLException {

		Properties info = new Properties();
		if (username != null)
			info.put(Attribute.USERNAME_ATTR, username);

		if (!requestPassword || !attributesAsPassword)
		{
			if (password != null)
				info.put(Attribute.PASSWORD_ATTR, password);
		}

		if (createDatabase != null)
			info.put(Attribute.CREATE_ATTR, "true");
		if (shutdownDatabase != null)
			info.put(Attribute.SHUTDOWN_ATTR, "true");

		String url = jdbcurl;

		if (attributesAsPassword && requestPassword && password != null) {

			StringBuffer sb = new StringBuffer(url.length() + password.length() + 1);

			sb.append(url);
			sb.append(';');
			sb.append(password); // these are now request attributes on the URL

			url = sb.toString();

		}

		return findDriver().connect(url, info);
	}
   
	Driver169 findDriver() throws SQLException
	{
		String url = jdbcurl;

		if (driver == null || !driver.acceptsURL(url))
		{
			synchronized(this)
			{
				// The driver has either never been booted, or it has been
				// shutdown by a 'jdbc:derby:;shutdown=true'
				if (driver == null || !driver.acceptsURL(url))
				{

					new org.apache.derby.jdbc.EmbeddedDriver();

					// If we know the driver, we loaded it.   Otherwise only
					// work if DriverManager has already loaded it.

					driver = (Driver169) DriverManager.getDriver(url);
					// DriverManager will throw an exception if it cannot find the driver
				}
			}
		}
		return driver;
		// else driver != null and driver can accept url
	}

	void update()
	{
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

			// this space will selected as the database name (and trimmed to an empty string)
			// See the getDatabaseName() code in Driver169. Since this is a non-null
			// value, it will be selected over any databaseName connection attribute.
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
