/*

   Derby - Class org.apache.derby.jdbc.AutoloadedDriver

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
   This is the dummy driver which is autoloaded under JDBC4 and registered with
   the DriverManager. Loading this class will NOT automatically boot the Derby engine.
   Instead, the engine boots lazily when you ask for a
   Connection. Alternatively, you can force the engine to boot as follows:

   	 <PRE>
	 Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();

	 // or

     new org.apache.derby.jdbc.EmbeddedDriver();

    
	</PRE>
*/
public class AutoloadedDriver implements Driver
{
	// This flag is set if the engine is forcibly brought down.
	private	static	boolean	_engineForcedDown = false;
	
	//
	// This is the driver that's specific to the JDBC level we're running at.
	// It's the module which boots the whole Derby engine.
	//
	private	static	Driver	_driverModule;
	
	static
	{
		try {
			DriverManager.registerDriver( new AutoloadedDriver() );
		}
		catch (SQLException se)
		{
			String	message = MessageService.getTextMessage
				(MessageId.JDBC_DRIVER_REGISTER_ERROR, se.getMessage() );

			throw new IllegalStateException( message );
		}
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

		//
		// We don't want to accidentally boot the engine just because
		// the application is looking for a connection from some other
		// driver.
		//
		return ( isBooted() && InternalDriver.embeddedDriverAcceptsURL(url) );
	}

   
	/**
		Connect to the URL if possible
		@exception SQLException illegal url or problem with connectiong
    @see java.sql.Driver
  */
	public Connection connect(String url, Properties info)
		throws SQLException
	{
		//
		// This pretty piece of logic compensates for the following behavior
		// of the DriverManager: When asked to get a Connection, the
		// DriverManager cycles through all of its autoloaded drivers, looking
		// for one which will return a Connection. Without this pretty logic,
		// the embedded driver module will be booted by any request for
		// a connection which cannot be satisfied by drivers ahead of us
		// in the list.
		if (!InternalDriver.embeddedDriverAcceptsURL(url)) { return null; }

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

	///////////////////////////////////////////////////////////////////////
	//
	// Support for booting and shutting down the engine.
	//
	///////////////////////////////////////////////////////////////////////

	/*
	** Retrieve the driver which is specific to our JDBC level.
	** We defer real work to this specific driver.
	*/
	public static	Driver getDriverModule() throws SQLException {

		if ( _engineForcedDown )
		{
			// Driver not registered 
			throw new SQLException
				(MessageService.getTextMessage(MessageId.CORE_JDBC_DRIVER_UNREGISTERED));
		}

		if ( !isBooted() ) { EmbeddedDriver.boot(); }

		return _driverModule;
	}
	
	/*
	** Record which driver module actually booted.
	*/
	protected	static	void	registerDriverModule( Driver driver )
	{
		_driverModule = driver;
		_engineForcedDown = false;
	}
	
	/*
	** Unregister the driver. This happens when the engine is
	** forcibly shut down.
	*/
	protected	static	void	unregisterDriverModule()
	{
		_driverModule = null;
		_engineForcedDown = true;
	}
	

	/*
	** Return true if the engine has been booted.
	*/
	private	static	boolean	isBooted()
	{
		return ( _driverModule != null );
	}
	
}

