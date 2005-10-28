/*

   Derby - Class org.apache.derby.jdbc.ReferenceableDataSource

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
package org.apache.derby.jdbc;

import java.sql.SQLException;
import java.lang.reflect.*;


import java.io.Serializable;
import java.io.PrintWriter;
import java.util.Properties;

/* -- JNDI -- */
import javax.naming.NamingException;
import javax.naming.Referenceable;
import javax.naming.Reference;
import javax.naming.StringRefAddr;
import javax.naming.spi.ObjectFactory;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.RefAddr;
import java.util.Hashtable;
import java.util.Enumeration;

/** 

	Cloudscape DataSource implementation base class.
	ReferenceableDataSource provides support for JDBC standard DataSource attributes and acts
	as the ObjectFactory to generate Cloudscape DataSource implementations.
	<P>
	The standard attributes provided by this class are:
	<UL>
	<LI>databaseName
	<LI>dataSourceName
	<LI>description
	<LI>password
	<LI>user
	</UL>
	<BR>
	See the specific Cloudscape DataSource implementation for details on their meaning.
	<BR>
	See the JDBC 3.0 specification for more details.


*/
public class ReferenceableDataSource implements
				javax.naming.Referenceable,
				java.io.Serializable,
				ObjectFactory
{


	private static final long serialVersionUID = 1872877359127597176L;


	private static final Class[] STRING_ARG = { "".getClass() };
	private static final Class[] INT_ARG = { Integer.TYPE };
	private static final Class[] BOOLEAN_ARG = { Boolean.TYPE };

	private String description;
	private String dataSourceName;
	private String databaseName;
	private String password;
	private String user;
	private int loginTimeout;


	/** instance variables that will not be serialized */
	transient private PrintWriter printer;

	/**
		No-arg constructor.
	 */
	public ReferenceableDataSource() {
		update();
	}


	/*
	 * Properties to be seen by Bean - access thru reflection.
	 */

	/** 
		Set the database name.  Setting this property is mandatory.  If a
		database named wombat at g:/db needs to be accessed, database name
		should be set to "g:/db/wombat".  The database will be booted if it
		is not already running in the system.

		@param databaseName the name of the database 
	*/
	public final synchronized void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
		update();
	}
	public String getDatabaseName() {
		return databaseName;
	}

	/** 
		Set the data source name.  The property is not mandatory.  It is used
		for informational purposes only.

		@param dsn the name of the data source
	*/
	public final void setDataSourceName(String dsn) {
		dataSourceName = dsn;
	}
	/** @return data source name */
	public final String getDataSourceName() {
		return dataSourceName;
	}

	/**
		Set the data source descripton. This property is not mandatory.
		It is used for informational purposes only.

		@param desc the description of the data source
	 */
	public final void setDescription(String desc) {
		description = desc;
	}
	/** @return description */
	public final String getDescription() {
		return description;
	}

	/**
		Set the <code>user</code> property for the data source.
		This is user name for any data source getConnection() call
		that takes no arguments.
	*/
	public final void setUser(String user) {
		this.user = user;
	}
	/** @return user */
	public final String getUser() {
		return user;
	}

	/**
		Set the <code>password</code> property for the data source.
		This is user's password for any data source getConnection() call
		that takes no arguments.
	*/
	public final void setPassword(String password) {
		this.password = password;
	}
	/** @return password */
	public final String getPassword() {
		return password;
	}

	/*
	 * DataSource methods 
	 */


	/**
	 * Gets the maximum time in seconds that this data source can wait
	 * while attempting to connect to a database.  A value of zero
	 * means that the timeout is the default system timeout 
	 * if there is one; otherwise it means that there is no timeout.
	 * When a data source object is created, the login timeout is
	 * initially zero.
	 *
	 * @return the data source login time limit
	 * @exception SQLException if a database access error occurs.
	 */
	public int getLoginTimeout() throws SQLException
	{
		return loginTimeout;
	}

	/**
	 * Sets the maximum time in seconds that this data source will wait
	 * while attempting to connect to a database.  A value of zero
	 * specifies that the timeout is the default system timeout 
	 * if there is one; otherwise it specifies that there is no timeout.
	 * When a data source object is created, the login timeout is
	 * initially zero.
	 <P>
		Cloudscape ignores this property.
	 * @param seconds the data source login time limit
	 * @exception SQLException if a database access error occurs.
	 */
	public void setLoginTimeout(int seconds) throws SQLException
	{
		loginTimeout = seconds;
	}


	/** 
	 * Get the log writer for this data source.  
	 *
	 * <p>The log writer is a character output stream to which all logging
	 * and tracing messages for this data source object instance will be
	 * printed.  This includes messages printed by the methods of this
	 * object, messages printed by methods of other objects manufactured
	 * by this object, and so on.  Messages printed to a data source
	 * specific log writer are not printed to the log writer associated
	 * with the java.sql.Drivermanager class.  When a data source object is
	 * created the log writer is initially null, in other words, logging
	 * is disabled.
	 *
	 * @return the log writer for this data source, null if disabled
	 * @exception SQLException if a database-access error occurs.  
	 */
	public PrintWriter getLogWriter() throws SQLException
	{
		return printer;
	}

	/**
	 * Set the log writer for this data source.
	 *
	 * <p>The log writer is a character output stream to which all logging
	 * and tracing messages for this data source object instance will be
	 * printed.  This includes messages printed by the methods of this
	 * object, messages printed by methods of other objects manufactured
	 * by this object, and so on.  Messages printed to a data source
	 * specific log writer are not printed to the log writer associated
	 * with the java.sql.Drivermanager class. When a data source object is
	 * created the log writer is initially null, in other words, logging
	 * is disabled.
	 *
	 * @param out the new log writer; to disable, set to null
	 * @exception SQLException if a database-access error occurs.  
	 */
	public void setLogWriter(PrintWriter out) throws SQLException
	{
		printer = out;
	}

	/*
	** Reference methods etc.
	*/

	/*
	 * Object Factory method
	 */

	/**
		Re-Create Cloudscape datasource given a reference.

		@param obj The possibly null object containing location or reference
		information that can be used in creating an object. 
		@param name The name of this object relative to nameCtx, or null if no
		name is specified. 
		@param nameCtx The context relative to which the name parameter is
		specified, or null if name is relative to the default initial context. 
		@param environment The possibly null environment that is used in
		creating the object. 

		@return One of the Cloudscape datasource object created; null if an
		object cannot be created. 

		@exception Exception  if this object factory encountered an exception
		while attempting to create an object, and no other object factories are
		to be tried. 
	 */
	public Object getObjectInstance(Object obj,
									Name name,
									Context nameCtx,
									Hashtable environment)
		 throws Exception
	{
		Reference ref = (Reference)obj;
		String classname = ref.getClassName();

		Object ds = Class.forName(classname).newInstance();

		for (Enumeration e = ref.getAll(); e.hasMoreElements(); ) {
			
			RefAddr attribute = (RefAddr) e.nextElement();

			String propertyName = attribute.getType();

			String value = (String) attribute.getContent();

			String methodName = "set" + propertyName.substring(0,1).toUpperCase(java.util.Locale.ENGLISH) + propertyName.substring(1);

			Method m;
			
			Object argValue;
			try {
				m = ds.getClass().getMethod(methodName, STRING_ARG);
				argValue = value;
			} catch (NoSuchMethodException nsme) {
				try {
					m = ds.getClass().getMethod(methodName, INT_ARG);
					argValue = Integer.valueOf(value);
				} catch (NoSuchMethodException nsme2) {
					m = ds.getClass().getMethod(methodName, BOOLEAN_ARG);
					argValue = Boolean.valueOf(value);
				}
			}
			m.invoke(ds, new Object[] { argValue });
		}

		return ds;
	}

	/**
		Referenceable method.

		@exception NamingException cannot find named object
	 */
	public final Reference getReference() throws NamingException 
	{
		// These fields will be set by the JNDI server when it decides to
		// materialize a data source.
		Reference ref = new Reference(this.getClass().getName(),
									  "org.apache.derby.jdbc.ReferenceableDataSource",
									  null);


		// Look for all the getXXX methods in the class that take no arguments.
		Method[] methods = this.getClass().getMethods();

		for (int i = 0; i < methods.length; i++) {

			Method m = methods[i];

			// only look for simple getter methods.
			if (m.getParameterTypes().length != 0)
				continue;

			// only non-static methods
			if (Modifier.isStatic(m.getModifiers()))
				continue;

			// Only getXXX methods
			String methodName = m.getName();
			if ((methodName.length() < 5) || !methodName.startsWith("get"))
				continue;



			Class returnType = m.getReturnType();

			if (Integer.TYPE.equals(returnType) || STRING_ARG[0].equals(returnType) || Boolean.TYPE.equals(returnType)) {

				// setSomeProperty
				// 01234

				String propertyName = methodName.substring(3,4).toLowerCase(java.util.Locale.ENGLISH).concat(methodName.substring(4));

				try {
					Object ov = m.invoke(this, null);

					//Need to check for nullability for all the properties, otherwise
					//rather than null, "null" string gets stored in jndi.
					if (ov != null) {
						ref.add(new StringRefAddr(propertyName, ov.toString()));
					}
				} catch (IllegalAccessException iae) {
				} catch (InvocationTargetException ite) {
				}


			}
		}

		return ref;
	}


	void update() {
	}

	/**
		Return a connection for the Cloudscape family of data source implementations.
	*/
	java.sql.Connection getConnection(String username, String password, boolean requestPassword) throws SQLException {
		return null;
	}

}
