/*

   Derby - Class org.apache.derby.jdbc.EmbeddedDriver

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

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import org.apache.derby.iapi.jdbc.JDBCBoot;
import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.impl.jdbc.Util;


/**
	The embedded JDBC driver (Type 4) for Derby.
	<P>
	The driver automatically supports the correct JDBC specification version
	for the Java Virtual Machine's environment.
	<UL>
	<LI> JDBC 4.0 - Java SE 6
	<LI> JDBC 3.0 - Java 2 - JDK 1.4, J2SE 5.0
	</UL>

	<P>
	Loading this JDBC driver boots the database engine
	within the same Java virtual machine.
	<P>
	The correct code to load the Derby engine using this driver is
	(with approriate try/catch blocks):
	 <PRE>
	 Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();

	 // or

     new org.apache.derby.jdbc.EmbeddedDriver();

    
	</PRE>
	When loaded in this way, the class boots the actual JDBC driver indirectly.
	The JDBC specification recommends the Class.ForName method without the .newInstance()
	method call, but adding the newInstance() guarantees
	that Derby will be booted on any Java Virtual Machine.

	<P>
	Note that you do not need to manually load the driver this way if you are
	running on Jave SE 6 or later. In that environment, the driver will be
	automatically loaded for you when your application requests a connection to
	a Derby database.
	<P>
	Any initial error messages are placed in the PrintStream
	supplied by the DriverManager. If the PrintStream is null error messages are
	sent to System.err. Once the Derby engine has set up an error
	logging facility (by default to derby.log) all subsequent messages are sent to it.
	<P>
	By convention, the class used in the Class.forName() method to
	boot a JDBC driver implements java.sql.Driver.

	This class is not the actual JDBC driver that gets registered with
	the Driver Manager. It proxies requests to the registered Derby JDBC driver.

	@see java.sql.DriverManager
	@see java.sql.DriverManager#getLogStream
	@see java.sql.Driver
	@see java.sql.SQLException
*/

public class EmbeddedDriver  implements Driver {

	static {
		EmbeddedDriver.boot();
	}

	// Boot from the constructor as well to ensure that
	// Class.forName(...).newInstance() reboots Derby 
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
		return getDriverModule().acceptsURL(url);
	}

	/**
		Connect to the URL if possible
		@exception SQLException illegal url or problem with connectiong
    @see java.sql.Driver
  */
	public Connection connect(String url, Properties info)
		throws SQLException
	{
		return getDriverModule().connect(url, info);
	}

  /**
   * Returns an array of DriverPropertyInfo objects describing possible properties.
    @exception SQLException if a database-access error occurs.
    @see java.sql.Driver
   */
	public  DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
		throws SQLException
	{
		return getDriverModule().getPropertyInfo(url, info);
	}

    /**
     * Returns the driver's major version number. 
     @see java.sql.Driver
     */
	public int getMajorVersion() {
		try {
			return (getDriverModule().getMajorVersion());
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
			return (getDriverModule().getMinorVersion());
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
			return (getDriverModule().jdbcCompliant());
		}
		catch (SQLException se) {
			return false;
		}
	}

  /**
   * Lookup the booted driver module appropriate to our JDBC level.
   */
	private	Driver	getDriverModule()
		throws SQLException
	{
		return AutoloadedDriver.getDriverModule();
	}


   /*
	** Find the appropriate driver for our JDBC level and boot it.
	*  This is package protected so that AutoloadedDriver can call it.
	*/
	static void boot() {
        PrintWriter pw = DriverManager.getLogWriter();

        if (pw == null) {
            pw = new PrintWriter(System.err, true);
        }

        try {
            new JDBCBoot().boot(Attribute.PROTOCOL, pw);
        }
        catch (Throwable t)
        {
            t.printStackTrace( pw );
            if ( t instanceof RuntimeException ) { throw (RuntimeException) t; }
        }
	}

    ////////////////////////////////////////////////////////////////////
    //
    // INTRODUCED BY JDBC 4.1 IN JAVA 7
    //
    ////////////////////////////////////////////////////////////////////

    public  Logger getParentLogger() throws SQLFeatureNotSupportedException
    {
        throw (SQLFeatureNotSupportedException) Util.notImplemented( "getParentLogger()" );
    }
	
}
