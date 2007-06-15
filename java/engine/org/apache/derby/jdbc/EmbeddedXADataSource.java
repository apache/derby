/*

   Derby - Class org.apache.derby.jdbc.EmbeddedXADataSource

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

import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.iapi.services.i18n.MessageService;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.jdbc.ResourceAdapter;
import org.apache.derby.iapi.db.Database;

import org.apache.derby.iapi.reference.Property;

import org.apache.derby.iapi.error.ExceptionSeverity;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Driver;


/** -- jdbc 2.0. extension -- */
import javax.sql.XADataSource;
import javax.sql.XAConnection;


/**

	EmbeddedXADataSource is Derby's XADataSource implementation for JDBC3.0.
	

	<P>An XADataSource is a factory for XAConnection objects.  It represents a
	RM in a DTP environment.  An object that implements the XADataSource
	interface is typically registered with a JNDI service provider.   	
	<P>
	EmbeddedXADataSource automatically supports the correct JDBC specification version
	for the Java Virtual Machine's environment.
	<UL>
	<LI> JDBC 3.0 - Java 2 - JDK 1.4, J2SE 5.0
	</UL>

	<P>EmbeddedXADataSource object only works on a local database.  There is no
	client/server support.  An EmbeddedXADataSource object must live in the same jvm as
	the database. 

	<P>EmbeddedXADataSource is serializable and referenceable.

	<P>See EmbeddedDataSource for DataSource properties.

 */
public class EmbeddedXADataSource extends EmbeddedDataSource implements
				javax.sql.XADataSource
{

	private static final long serialVersionUID = -5715798975598379738L;

	// link to the database
	transient private ResourceAdapter ra;
  
	/**
	  no-arg constructor
	*/
	public EmbeddedXADataSource() 
	{
		super();
	}


	/*
	 * XADataSource methods 
	 */


	/**
	 * Attempt to establish a database connection.
	 *
	 * @return  a Connection to the database
	 * @exception SQLException if a database-access error occurs.
	 */
	public final XAConnection getXAConnection() throws SQLException
	{
		if (ra == null || !ra.isActive())
			setupResourceAdapter(null, null, false);

		return createXAConnection (ra, getUser(), getPassword(), false);
	}

	/**
	 * Attempt to establish a database connection with the given user
	 * name and password.
	 *
	 * @param user the database user on whose behalf the Connection is being made
	 * @param password the user's password
	 * @return  a Connection to the database
	 * @exception SQLException if a database-access error occurs.
	 */
	public final XAConnection getXAConnection(String user, String password)
		 throws SQLException 
	{
		if (ra == null || !ra.isActive())
			setupResourceAdapter(user, password, true);
                return createXAConnection (ra, user, password, true);
	}
	
	/*
	 * private method
	 */

	void update() {
		ra = null;
		super.update();
	}

	private void setupResourceAdapter(String user, String password, boolean requestPassword) throws SQLException
	{
		synchronized(this)
		{
			if (ra == null || !ra.isActive())
			{
				// If it is inactive, it is useless.
				ra = null;

				String dbName = getDatabaseName();
				if (dbName != null) {

					// see if database already booted, if it is, then don't make a
					// connection. 
					Database database = null;

					// if monitor is never setup by any ModuleControl, getMonitor
					// returns null and no Derby database has been booted. 
					if (Monitor.getMonitor() != null)
						database = (Database)
							Monitor.findService(Property.DATABASE_MODULE, dbName);

					if (database == null)
					{
						// If database is not found, try connecting to it.  This
						// boots and/or creates the database.  If database cannot
						// be found, this throws SQLException.
						if (requestPassword)
							getConnection(user, password).close();
						else
							getConnection().close();

						// now try to find it again
						database = (Database)
							Monitor.findService(Property.DATABASE_MODULE, dbName); 
					}

					if (database != null)
						ra = (ResourceAdapter) database.getResourceAdapter();
				}

				if (ra == null)
					throw new SQLException(MessageService.getTextMessage(MessageId.CORE_DATABASE_NOT_AVAILABLE),
										   "08006",
										   ExceptionSeverity.DATABASE_SEVERITY);


				// If database is already up, we need to set up driver
				// seperately. 
				findDriver();

				if (driver == null)
					throw new SQLException(MessageService.getTextMessage(MessageId.CORE_DRIVER_NOT_AVAILABLE),
										   "08006",
										   ExceptionSeverity.DATABASE_SEVERITY);

			}
		}
	}
        
    /**
     * Intantiate and return an EmbedXAConnection from this instance
     * of EmbeddedXADataSource.
     *
     * @param user 
     * @param password 
     * @return XAConnection
     */
    protected XAConnection createXAConnection (ResourceAdapter ra, 
        String user, String password, boolean requestPassword)
        throws SQLException
    {
        /* This object (EmbeddedXADataSource) is a JDBC 2 and JDBC 3
         * implementation of XADatSource.  However, it's possible that we
         * are running with a newer driver (esp. JDBC 4) in which case we
         * should return a PooledConnection that implements the newer JDBC
         * interfaces--even if "this" object does not itself satisfy those
         * interfaces.  As an example, if we have a JDK 6 application then
         * even though this specific object doesn't implement JDBC 4 (it
         * only implements JDBC 2 and 3), we should still return an
         * XAConnection object that *does* implement JDBC 4 because that's
         * what a JDK 6 app expects.
         *
         * By calling "findDriver()" here we will get the appropriate
         * driver for the JDK in use (ex. if using JDK 6 then findDriver()
         * will return the JDBC 4 driver).  If we then ask the driver to
         * give us an XA connection, we will get a connection that
         * corresponds to whatever driver/JDBC implementation is being
         * used--which is what we want.  So for a JDK 6 application we
         * will correctly return a JDBC 4 XAConnection. DERBY-2488.
         *
         * This type of scenario can occur if an application that was
         * previously running with an older JVM (ex. JDK 1.4/1.5) starts
         * running with a newer JVM (ex. JDK 6), in which case the app
         * is probably still using the "old" data source (ex. is still
         * instantiating EmbeddedXADataSource) instead of the newer one
         * (EmbeddedXADataSource40).
         */
        return ((Driver30) findDriver()).getNewXAConnection(
            this, ra, user, password, requestPassword);
    }

}
