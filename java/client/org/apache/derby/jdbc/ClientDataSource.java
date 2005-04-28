/*

   Derby - Class org.apache.derby.client.ClientDataSource

   Copyright (c) 2001, 2005 The Apache Software Foundation or its licensors, where applicable.

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
import org.apache.derby.client.ClientBaseDataSource;

/**
 * ClientDataSource is a simple data source implementation that can be used
 * for establishing connections in a non-pooling, non-distributed environment.
 * The class ClientDataSource can be used in a connection pooling environment,
 * and the class ClientXADataSource can be used in a distributed, and pooling environment.
 * <p>
 * The example below registers a DNC data source object with a JNDI naming service.
 * <pre>
 * org.apache.derby.client.ClientDataSource dataSource = new org.apache.derby.client.ClientDataSource ();
 * dataSource.setServerName ("my_derby_database_server");
 * dataSource.setDatabaseName ("my_derby_database_name");
 * javax.naming.Context context = new javax.naming.InitialContext();
 * context.bind ("jdbc/my_datasource_name", dataSource);
 * </pre>
 * The first line of code in the example creates a data source object.
 * The next two lines initialize the data source's properties.
 * Then a Java object that references the initial JNDI naming
 * context is created by calling the InitialContext() constructor, which is provided by
 * JNDI. System properties (not shown) are used to tell JNDI the service provider to use.
 * The JNDI name space is hierarchical, similar to the directory structure of many file
 * systems. The data source object is bound to a logical JNDI name by calling Context.bind().
 * In this case the JNDI name identifies a subcontext, "jdbc", of the root
 * naming context and a logical name, "my_datasource_name", within the jdbc subcontext. This
 * is all of the code required to deploy a data source object within JNDI.
 * This example is provided mainly for illustrative purposes. We expect
 * that developers or system administrators will normally use a GUI tool to deploy a data
 * source object.
 * <p>
 * Once a data source has been registered with JNDI, it can then be used by a JDBC application,
 * as is shown in the following example.
 * <pre>
 * javax.naming.Context context = new javax.naming.InitialContext ();
 * javax.sql.DataSource dataSource = (javax.sql.DataSource) context.lookup ("jdbc/my_datasource_name");
 * java.sql.Connection connection = dataSource.getConnection ("user", "password");
 * </pre>
 * The first line in the example creates a Java object that references the initial JNDI naming
 * context. Next, the initial naming context is used to do a lookup operation using the
 * logical name of the data source. The Context.lookup() method returns a reference to
 * a Java Object, which is narrowed to a javax.sql.DataSource object. In the last line,
 * the DataSource.getConnection() method is called to produce a database connection.
 * <p>
 * This simple data source subclass of ClientBaseDataSource maintains it's own private <code>password</code> property.
 * <p>
 * The specified password, along with the user,
 * is validated by DERBY.  This property
 * can be overwritten by specifing the password parameter on the
 * DataSource.getConnection() method call.
 * <p>
 * This password property is not declared transient, and therefore may be serialized
 * to a file in clear-text, or stored to a JNDI server in clear-text when the data source is saved.
 * Care must taken by the user to prevent security breaches.
 * <p>
 *
 */
public class ClientDataSource extends ClientBaseDataSource implements javax.sql.DataSource,
                                                                      java.io.Serializable,
                                                                      javax.naming.Referenceable
{
  private final static long serialVersionUID = 1894299584216955553L;
  public static final String className__ = "org.apache.derby.jdbc.ClientDataSource";

  // If a newer version of a serialized object has to be compatible with an older version, it is important that the newer version abides
  // by the rules for compatible and incompatible changes.
  //
  // A compatible change is one that can be made to a new version of the class, which still keeps the stream compatible with older
  // versions of the class. Examples of compatible changes are:
  //
  // Addition of new fields or classes does not affect serialization, as any new data in the stream is simply ignored by older
  // versions. When the instance of an older version of the class is deserialized, the newly added field will be set to its default
  // value.
  // You can field change access modifiers like private, public, protected or package as they are not reflected to the serial
  // stream.
  // You can change a transient or static field to a non-transient or non-static field, as it is similar to adding a field.
  // You can change the access modifiers for constructors and methods of the class. For instance a previously private method
  // can now be made public, an instance method can be changed to static, etc. The only exception is that you cannot change
  // the default signatures for readObject() and writeObject() if you are implementing custom serialization. The serialization
  // process looks at only instance data, and not the methods of a class.
  //
  // Changes which would render the stream incompatible are:
  //
  // Once a class implements the Serializable interface, you cannot later make it implement the Externalizable interface, since
  // this will result in the creation of an incompatible stream.
  // Deleting fields can cause a problem. Now, when the object is serialized, an earlier version of the class would set the old
  // field to its default value since nothing was available within the stream. Consequently, this default data may lead the newly
  // created object to assume an invalid state.
  // Changing a non-static into static or non-transient into transient is not permitted as it is equivalent to deleting fields.
  // You also cannot change the field types within a class, as this would cause a failure when attempting to read in the original
  // field into the new field.
  // You cannot alter the position of the class in the class hierarchy. Since the fully-qualified class name is written as part of
  // the bytestream, this change will result in the creation of an incompatible stream.
  // You cannot change the name of the class or the package it belongs to, as that information is written to the stream during
  // serialization.

  private String password = null;
  synchronized public void setPassword (String password) { this.password = password; }


  /**
   * Creates a simple DERBY data source with default property values
   * for a non-pooling, non-distributed environment.
   * No particular DatabaseName or other properties are associated with the data source.
   * <p>
   * Every Java Bean should provide a constructor with no arguments
   * since many beanboxes attempt to instantiate a bean by invoking
   * its no-argument constructor.
   *
   */
  public ClientDataSource () { super(); }


  // ---------------------------interface methods-------------------------------

  /**
   * Attempt to establish a database connection in a non-pooling, non-distributed environment.
   *
   * @return a Connection to the database
   * @throws java.sql.SQLException if a database-access error occurs.
   **/
  public java.sql.Connection getConnection () throws java.sql.SQLException
  { return getConnection (this.user, this.password); }

  /**
   * Attempt to establish a database connection in a non-pooling, non-distributed environment.
   *
   * @param user the database user on whose behalf the Connection is being made
   * @param password the user's password
   * @return a Connection to the database
   * @throws java.sql.SQLException if a database-access error occurs.
   **/
  public java.sql.Connection getConnection (String user, String password) throws java.sql.SQLException
  {
    // Jdbc 2 connections will write driver trace info on a
    // datasource-wide basis using the jdbc 2 data source log writer.
    // This log writer may be narrowed to the connection-level
    // This log writer will be passed to the agent constructor.

    org.apache.derby.client.am.LogWriter dncLogWriter = super.computeDncLogWriterForNewConnection ("_sds");
	updateDataSourceValues(tokenizeAttributes(connectionAttributes,null));
    return
        new org.apache.derby.client.net.NetConnection (
          (org.apache.derby.client.net.NetLogWriter) dncLogWriter,
          user,
          password,
          this,
          -1,
          false);
  }

	/*
	 * Properties to be seen by Bean - access thru reflection.
	 */

	// -- Stardard JDBC DataSource Properties
	
	public synchronized void setDatabaseName (String databaseName) { this.databaseName = databaseName; }
	public String getDatabaseName () { return this.databaseName; }
	
	
	public synchronized void setDataSourceName (String dataSourceName) { this.dataSourceName = dataSourceName; }
	public String getDataSourceName () { return this.dataSourceName; }
	
  public synchronized void setDescription (String description) { this.description = description; }
  public String getDescription () { return this.description; }


	public synchronized void setPortNumber (int portNumber) { this.portNumber = portNumber; }
	public int getPortNumber () { return this.portNumber; }
	
	public synchronized void setServerName (String serverName) { this.serverName = serverName; }
	public String getServerName () { return this.serverName; }


	public synchronized void setUser (String user) { this.user = user; }
	public String getUser () { return this.user; }
	
	synchronized public void setRetrieveMessageText (boolean retrieveMessageText) { this.retrieveMessageText = retrieveMessageText; }
	public boolean getRetrieveMessageText () { return this.retrieveMessageText; }

	// ---------------------------- securityMechanism -----------------------------------
	/**
	 *The source security mechanism to use when connecting to this data source.
  * <p>
  * Security mechanism options are:
  * <ul>
  * <li> USER_ONLY_SECURITY
  * <li> CLEAR_TEXT_PASSWORD_SECURITY
  * <li> ENCRYPTED_PASSWORD_SECURITY
  * <li> ENCRYPTED_USER_AND_PASSWORD_SECURITY - both password and user are encrypted
  * </ul>
  * The default security mechanism is USER_ONLY SECURITY
  * <p>
  * If the application specifies a security
  * mechanism then it will be the only one attempted.
  * If the specified security mechanism is not supported by the conversation
  * then an exception will be thrown and there will be no additional retries.
  * <p>
  * This property is currently only available for the  DNC driver.
  * <p>
  * Both user and password need to be set for all security mechanism except USER_ONLY_SECURITY 
  */
	// We use the NET layer constants to avoid a mapping for the NET driver.
	public final static short USER_ONLY_SECURITY = (short) org.apache.derby.client.net.NetConfiguration.SECMEC_USRIDONL;
	public final static short CLEAR_TEXT_PASSWORD_SECURITY = (short) org.apache.derby.client.net.NetConfiguration.SECMEC_USRIDPWD;
	public final static short ENCRYPTED_PASSWORD_SECURITY = (short) org.apache.derby.client.net.NetConfiguration.SECMEC_USRENCPWD;
	public final static short ENCRYPTED_USER_AND_PASSWORD_SECURITY = (short) org.apache.derby.client.net.NetConfiguration.SECMEC_EUSRIDPWD;
	
	synchronized public void setSecurityMechanism (short securityMechanism) { this.securityMechanism = securityMechanism; }
	public short getSecurityMechanism () { 
		return getUpgradedSecurityMechanism(this.securityMechanism,
											this.password);
	}

	protected String connectionAttributes = "";

	/**
 		Set this property to pass in more Derby specific
		connection URL attributes.

		@param prop set to the list of Cloudscape connection
		attributes separated by semi-colons.   E.g., to specify an encryption
		bootPassword of "x8hhk2adf", and set upgrade to true, do the following: 
		<PRE>
			ds.setConnectionAttributes("bootPassword=x8hhk2adf;upgrade=true");
		</PRE>
		See Derby documentation for complete list.
	 */
	public final void setConnectionAttributes(String prop) {
		 connectionAttributes = prop;
	}

	/** @return Derby specific connection URL attributes */
	public final String getConnectionAttributes() {
		return connectionAttributes;
	}
	

	public final static int TRACE_NONE = 0x0;
	public final static int TRACE_CONNECTION_CALLS = 0x1;
	public final static int TRACE_STATEMENT_CALLS= 0x2;
	public final static int TRACE_RESULT_SET_CALLS = 0x4;
	public final static int TRACE_DRIVER_CONFIGURATION = 0x10;
	public final static int TRACE_CONNECTS = 0x20;
	public final static int TRACE_PROTOCOL_FLOWS = 0x40;
	public final static int TRACE_RESULT_SET_META_DATA = 0x80;
	public final static int TRACE_PARAMETER_META_DATA = 0x100;
	public final static int TRACE_DIAGNOSTICS = 0x200;
	public final static int TRACE_XA_CALLS = 0x800;
  public final static int TRACE_ALL = 0xFFFFFFFF;
	synchronized public void setTraceLevel (int traceLevel) { this.traceLevel = traceLevel; }
	public int getTraceLevel () { return this.traceLevel; }
	

	public synchronized void setTraceFile (String traceFile) { this.traceFile = traceFile; }
	public String getTraceFile () { return this.traceFile; }


	public synchronized void setTraceDirectory (String traceDirectory) { this.traceDirectory = traceDirectory; }
	public String getTraceDirectory () { return this.traceDirectory; }

	synchronized public void setTraceFileAppend (boolean traceFileAppend) { this.traceFileAppend = traceFileAppend; }
	public boolean getTraceFileAppend () { return this.traceFileAppend; }




	// --- private helper methods

	
	/**
	 * The dataSource keeps individual fields for the values that are 
	 * relevant to the client. These need to be updated when 
	 * set connection attributes is called.
	 */
	private void updateDataSourceValues(java.util.Properties prop)
	{
		if ( prop.containsKey(propertyKey_user))
			setUser(getUser(prop));
		if ( prop.containsKey(propertyKey_securityMechanism))
			 setSecurityMechanism(getSecurityMechanism(prop));
		if ( prop.containsKey(propertyKey_traceFile))
			 setTraceFile(getTraceFile(prop));
		if ( prop.containsKey(propertyKey_traceDirectory))
			 setTraceDirectory(getTraceDirectory(prop));
		if ( prop.containsKey(propertyKey_traceFileAppend))
			setTraceFileAppend(getTraceFileAppend(prop));
		if ( prop.containsKey(propertyKey_securityMechanism))
			setSecurityMechanism(getSecurityMechanism(prop));
		if ( prop.containsKey(propertyKey_retrieveMessageText))
			setRetrieveMessageText(getRetrieveMessageText(prop));
	}

}

