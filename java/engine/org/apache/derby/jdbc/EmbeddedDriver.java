/*

   Derby - Class org.apache.derby.jdbc.EmbeddedDriver

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

import java.sql.DriverManager;
import java.sql.Driver;
import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;

import java.io.PrintStream;
import java.util.Properties;

import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.jdbc.JDBCBoot;


/**
	The embedded JDBC driver (Type 4) for Cloudscape.
	<P>
	The driver automatically supports the correct JDBC specification version
	for the Java Virtual Machine's environment.
	<UL>
	<LI> JDBC 3.0 - Java 2 - JDK 1.4
	<LI> JDBC 2.0 - Java 2 - JDK 1.2,1.3
	</UL>

	<P>
	Loading this JDBC driver boots the database engine
	within the same Java virtual machine.
	<P>
	The correct code to load a Cloudscape engine using this driver is
	(with approriate try/catch blocks):
	 <PRE>
	 Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();

	 // or

     new org.apache.derby.jdbc.EmbeddedDriver();

    
	</PRE>
	 When loaded in this way, the class boots the actual JDBC driver indirectly.
	The JDBC specification recommends the Class.ForName method without the .newInstance()
	method call, but adding the newInstance() guarantees
	that Cloudscape will be booted on any Java Virtual Machine.

	<P>
	Any initial error messages are placed in the PrintStream
	supplied by the DriverManager. If the PrintStream is null error messages are
	sent to System.err. Once the Cloudscape engine has set up an error
	logging facility (by default to derby.log) all subsequent messages are sent to it.
	<P>
	By convention, the class used in the Class.forName() method to
	boot a JDBC driver implements java.sql.Driver.

	This class is not the actual JDBC driver that gets registered with
	the Driver Manager. It proxies requests to the registered Cloudscape JDBC driver.

	@see java.sql.DriverManager
	@see java.sql.DriverManager#getLogStream
	@see java.sql.Driver
	@see java.sql.SQLException
*/

public class EmbeddedDriver implements Driver {

	static {

		EmbeddedDriver.boot();
	}

	// Boot from the constructor as well to ensure that
	// Class.forName(...).newInstance() reboots Cloudscape
	// after a shutdown inside the same JVM.
	public EmbeddedDriver() {
		EmbeddedDriver.boot();
	}

	/*
	** Methods from java.sql.Driver.
	*/
	/**
		Accept anything that starts with <CODE>jdbc:derby:</CODE>.
		@exception SQLException if a database-access error occurs.
    @see java.sql.Driver
	*/
	public boolean acceptsURL(String url) throws SQLException {
		return getRegisteredDriver().acceptsURL(url);
	}

	/**
		Connect to the URL if possible
		@exception SQLException illegal url or problem with connectiong
    @see java.sql.Driver
  */
	public Connection connect(String url, Properties info)
		throws SQLException
	{
		return getRegisteredDriver().connect(url, info);
	}

  /**
   * Returns an array of DriverPropertyInfo objects describing possible properties.
    @exception SQLException if a database-access error occurs.
    @see java.sql.Driver
   */
	public  DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
		throws SQLException
	{
		return getRegisteredDriver().getPropertyInfo(url, info);
	}

    /**
     * Returns the driver's major version number. 
     @see java.sql.Driver
     */
	public int getMajorVersion() {
		try {
			return (getRegisteredDriver().getMajorVersion());
		}
		catch (SQLException se) {
			return 0;
		}
	}
    /**
     * Returns the driver's minor version number.
     @see java.sql.Driver
     */
	public int getMinorVersion() {
		try {
			return (getRegisteredDriver().getMinorVersion());
		}
		catch (SQLException se) {
			return 0;
		}
	}

  /**
   * Report whether the Driver is a genuine JDBC COMPLIANT (tm) driver.
     @see java.sql.Driver
   */
	public boolean jdbcCompliant() {
		try {
			return (getRegisteredDriver().jdbcCompliant());
		}
		catch (SQLException se) {
			return false;
		}
	}

	private static void boot() {
		PrintStream ps = DriverManager.getLogStream();

		if (ps == null)
			ps = System.err;

		new JDBCBoot().boot(Attribute.PROTOCOL, ps);
	}

	/*
	** Retrieve the actual Registered Driver,
	** probe the DriverManager in order to get it.
	*/
	private Driver getRegisteredDriver() throws SQLException {

		try {
		  return DriverManager.getDriver(Attribute.PROTOCOL);
		}
		catch (SQLException se) {
			// Driver not registered 
			throw new SQLException(MessageService.getTextMessage(MessageId.CORE_JDBC_DRIVER_UNREGISTERED));
		}
	}
}
