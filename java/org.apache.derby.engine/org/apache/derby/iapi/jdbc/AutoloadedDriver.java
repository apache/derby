/*

   Derby - Class org.apache.derby.iapi.jdbc.AutoloadedDriver

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

package org.apache.derby.iapi.jdbc;

import java.sql.DriverManager;
import java.sql.Driver;
import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import java.security.AccessControlException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.derby.shared.common.reference.MessageId;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.i18n.MessageService;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.impl.jdbc.Util;


/**
   This is the dummy driver which is registered with the DriverManager and
   which is autoloaded by JDBC4. Loading this class will NOT automatically
   boot the Derby engine, but it will register this class as a valid
   Driver with the DriverManager.
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
	

    // This is the driver that memorizes the autoloadeddriver (DERBY-2905)
    private static AutoloadedDriver _autoloadedDriver;

	//
	// This is the driver that's specific to the JDBC level we're running at.
	// It's the module which boots the whole Derby engine.
	//
	private	static	Driver	_driverModule;
	
	static
	{
            registerMe( new AutoloadedDriver() );
	}

	protected static void   registerMe( AutoloadedDriver me )
	{
		try {
            _autoloadedDriver = me;
            DriverManager.registerDriver( _autoloadedDriver );
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
//IC see: https://issues.apache.org/jira/browse/DERBY-2788
		return !_engineForcedDown && InternalDriver.embeddedDriverAcceptsURL(url);
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

    ////////////////////////////////////////////////////////////////////
    //
    // INTRODUCED BY JDBC 4.1 IN JAVA 7
    //
    ////////////////////////////////////////////////////////////////////

    public  Logger getParentLogger() throws SQLFeatureNotSupportedException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        throw (SQLFeatureNotSupportedException) Util.notImplemented( "getParentLogger()" );
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

		if ( _engineForcedDown && (_autoloadedDriver == null))
		{
			// Driver not registered
            throw Util.generateCsSQLException( SQLState.CORE_JDBC_DRIVER_UNREGISTERED );
		}

		if ( !isBooted() ) { JDBCBoot.boot(); }
//IC see: https://issues.apache.org/jira/browse/DERBY-6945

		return _driverModule;
	}
	
	/**
	** Record which driver module actually booted.
	*  @param driver the driver register to DriverManager is not AutoloadedDriver
	**/
	public static	void	registerDriverModule( Driver driver )
	{
		_driverModule = driver;
		_engineForcedDown = false;
		
        try {
            if (_autoloadedDriver == null) {
                //Support JDBC 4 or higher (DERBY-2905)
//IC see: https://issues.apache.org/jira/browse/DERBY-5029
//IC see: https://issues.apache.org/jira/browse/DERBY-2095
                _autoloadedDriver = makeAutoloadedDriver();
                DriverManager.registerDriver(_autoloadedDriver);
            }
        } catch (SQLException e) {
            if (SanityManager.DEBUG)
                SanityManager.THROWASSERT(e);
        }
	}
	
	/**
	** Unregister the driver and the AutoloadedDriver if exists. 
	*  This happens when the engine is forcibly shut down.
	*  
	**/
	public static	void	unregisterDriverModule()
	{
		_engineForcedDown = true;
        try {
            // deregister is false if user set deregister=false attribute (DERBY-2905)
//IC see: https://issues.apache.org/jira/browse/DERBY-4845
            if (InternalDriver.getDeregister() && _autoloadedDriver != null) {
                deregisterDriver(_autoloadedDriver);
                _autoloadedDriver = null;
            }

            // DERBY-5085, need to restore the default value
            InternalDriver.setDeregister(true);

            _driverModule = null;
        } catch (SQLException e) {
            if (SanityManager.DEBUG)
                SanityManager.THROWASSERT(e);
        }
	}

    private static void deregisterDriver(final AutoloadedDriver driver)
            throws SQLException {
        // DERBY-6224: DriverManager.deregisterDriver() requires a special
        // permission in JDBC 4.2 and later. Call it in a privileged block
        // so that the permission doesn't have to be granted to code that
        // invokes engine shutdown.
        try {
            AccessController.doPrivileged(
                    new PrivilegedExceptionAction<Void>() {
                public Void run() throws SQLException {
                    // Requires SQLPermission("deregisterDriver")
                    DriverManager.deregisterDriver(driver);
                    return null;
                }
            });
        } catch (PrivilegedActionException pae) {
            throw (SQLException) pae.getCause();
        } catch (AccessControlException ace) {
            // Since no permission was needed for deregisterDriver() before
            // Java 8, applications may be surprised to find that engine
            // shutdown fails because of it. For backward compatibility,
            // don't fail shutdown if the permission is missing. Instead,
            // log a message saying the driver could not be deregistered.
            Monitor.logTextMessage(MessageId.CONN_DEREGISTER_NOT_PERMITTED);
            Monitor.logThrowable(ace);
        }
    }

	/*
	** Return true if the engine has been booted.
	*/
	private	static	boolean	isBooted()
	{
		return ( _driverModule != null );
	}
	
    /**
     * Load the most capable driver available.
     * But if the vm level doesn't support it, then we fall
     * back on a lower-level driver.
     * @return AutoloadedDriver 
     */
    private static AutoloadedDriver makeAutoloadedDriver() 
    {
        // Currently, there's only one driver, and it supports all the JDBC
        // levels that Derby supports. Return an instance of it.
        return new AutoloadedDriver(); 
    } 
}

