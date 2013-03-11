/*

   Derby - Class org.apache.derby.jdbc.Driver20

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

import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.iapi.reference.Property;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.jdbc.BrokeredConnection;
import org.apache.derby.iapi.jdbc.BrokeredConnectionControl;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.io.FormatableProperties;
import org.apache.derby.iapi.security.SecurityUtil;

import org.apache.derby.impl.jdbc.*;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;

import java.security.Permission;
import java.security.AccessControlException;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import org.apache.derby.iapi.util.InterruptStatus;

/**
	This class extends the local JDBC driver in order to determine at JBMS
	boot-up if the JVM that runs us does support JDBC 2.0. If it is the case
	then we will load the appropriate class(es) that have JDBC 2.0 new public
	methods and sql types.
*/

public abstract class Driver20 extends InternalDriver implements Driver {

    private static  ThreadPoolExecutor _executorPool;
    static
    {
        _executorPool = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                                      60L, TimeUnit.SECONDS,
                                      new SynchronousQueue<Runnable>());   
        _executorPool.setThreadFactory( new DaemonThreadFactory() );
    } 

	private static final String[] BOOLEAN_CHOICES = {"false", "true"};

	private Class  antiGCDriverManager;

	/*
	**	Methods from ModuleControl
	*/

	public void boot(boolean create, Properties properties) throws StandardException {

		super.boot(create, properties);

		// Register with the driver manager
		AutoloadedDriver.registerDriverModule( this );

		// hold onto the driver manager to avoid its being garbage collected.
		// make sure the class is loaded by using .class
		antiGCDriverManager = java.sql.DriverManager.class;
	}

	public void stop() {

		super.stop();

		AutoloadedDriver.unregisterDriverModule();
	}
  
	public org.apache.derby.impl.jdbc.EmbedResultSet 
	newEmbedResultSet(EmbedConnection conn, ResultSet results, boolean forMetaData, org.apache.derby.impl.jdbc.EmbedStatement statement, boolean isAtomic)
		throws SQLException
	{
		return new EmbedResultSet20(conn, results, forMetaData, statement,
								 isAtomic); 
	}

    public abstract BrokeredConnection newBrokeredConnection(
            BrokeredConnectionControl control) throws SQLException;

    /**
     * <p>The getPropertyInfo method is intended to allow a generic GUI tool to 
     * discover what properties it should prompt a human for in order to get 
     * enough information to connect to a database.  Note that depending on
     * the values the human has supplied so far, additional values may become
     * necessary, so it may be necessary to iterate though several calls
     * to getPropertyInfo.
     *
     * @param url The URL of the database to connect to.
     * @param info A proposed list of tag/value pairs that will be sent on
     *          connect open.
     * @return An array of DriverPropertyInfo objects describing possible
     *          properties.  This array may be an empty array if no properties
     *          are required.
     * @exception SQLException if a database-access error occurs.
     */
	public  DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {

		// RESOLVE other properties should be added into this method in the future ... 

        if (info != null) {
			if (Boolean.valueOf(info.getProperty(Attribute.SHUTDOWN_ATTR)).booleanValue()) {
	
				// no other options possible when shutdown is set to be true
				return new DriverPropertyInfo[0];
			}
		}

		// at this point we have databaseName, 

		String dbname = InternalDriver.getDatabaseName(url, info);

		// convert the ;name=value attributes in the URL into
		// properties.
		FormatableProperties finfo = getAttributes(url, info);
		info = null; // ensure we don't use this reference directly again.
		boolean encryptDB = Boolean.valueOf(finfo.getProperty(Attribute.DATA_ENCRYPTION)).booleanValue();		
		String encryptpassword = finfo.getProperty(Attribute.BOOT_PASSWORD);

		if (dbname.length() == 0 || (encryptDB && encryptpassword == null)) {

			// with no database name we can have shutdown or a database name

			// In future, if any new attribute info needs to be included in this
			// method, it just has to be added to either string or boolean or secret array
			// depending on whether it accepts string or boolean or secret(ie passwords) value. 

			String[][] connStringAttributes = {
				{Attribute.DBNAME_ATTR, MessageId.CONN_DATABASE_IDENTITY},
				{Attribute.CRYPTO_PROVIDER, MessageId.CONN_CRYPTO_PROVIDER},
				{Attribute.CRYPTO_ALGORITHM, MessageId.CONN_CRYPTO_ALGORITHM},
				{Attribute.CRYPTO_KEY_LENGTH, MessageId.CONN_CRYPTO_KEY_LENGTH},
				{Attribute.CRYPTO_EXTERNAL_KEY, MessageId.CONN_CRYPTO_EXTERNAL_KEY},
				{Attribute.TERRITORY, MessageId.CONN_LOCALE},
				{Attribute.COLLATION, MessageId.CONN_COLLATION},
				{Attribute.USERNAME_ATTR, MessageId.CONN_USERNAME_ATTR},
				{Attribute.LOG_DEVICE, MessageId.CONN_LOG_DEVICE},
				{Attribute.ROLL_FORWARD_RECOVERY_FROM, MessageId.CONN_ROLL_FORWARD_RECOVERY_FROM},
				{Attribute.CREATE_FROM, MessageId.CONN_CREATE_FROM},
				{Attribute.RESTORE_FROM, MessageId.CONN_RESTORE_FROM},
			};

			String[][] connBooleanAttributes = {
				{Attribute.SHUTDOWN_ATTR, MessageId.CONN_SHUT_DOWN_CLOUDSCAPE},
                {Attribute.DEREGISTER_ATTR, MessageId.CONN_DEREGISTER_AUTOLOADEDDRIVER},
				{Attribute.CREATE_ATTR, MessageId.CONN_CREATE_DATABASE},
				{Attribute.DATA_ENCRYPTION, MessageId.CONN_DATA_ENCRYPTION},
				{Attribute.UPGRADE_ATTR, MessageId.CONN_UPGRADE_DATABASE},
				};

			String[][] connStringSecretAttributes = {
				{Attribute.BOOT_PASSWORD, MessageId.CONN_BOOT_PASSWORD},
				{Attribute.PASSWORD_ATTR, MessageId.CONN_PASSWORD_ATTR},
				};

			
			DriverPropertyInfo[] optionsNoDB = new 	DriverPropertyInfo[connStringAttributes.length+
																	  connBooleanAttributes.length+
			                                                          connStringSecretAttributes.length];
			
			int attrIndex = 0;
			for( int i = 0; i < connStringAttributes.length; i++, attrIndex++ )
			{
				optionsNoDB[attrIndex] = new DriverPropertyInfo(connStringAttributes[i][0], 
									  finfo.getProperty(connStringAttributes[i][0]));
				optionsNoDB[attrIndex].description = MessageService.getTextMessage(connStringAttributes[i][1]);
			}

			optionsNoDB[0].choices = Monitor.getMonitor().getServiceList(Property.DATABASE_MODULE);
			// since database name is not stored in FormatableProperties, we
			// assign here explicitly
			optionsNoDB[0].value = dbname;

			for( int i = 0; i < connStringSecretAttributes.length; i++, attrIndex++ )
			{
				optionsNoDB[attrIndex] = new DriverPropertyInfo(connStringSecretAttributes[i][0], 
									  (finfo.getProperty(connStringSecretAttributes[i][0]) == null? "" : "****"));
				optionsNoDB[attrIndex].description = MessageService.getTextMessage(connStringSecretAttributes[i][1]);
			}

			for( int i = 0; i < connBooleanAttributes.length; i++, attrIndex++ )
			{
				optionsNoDB[attrIndex] = new DriverPropertyInfo(connBooleanAttributes[i][0], 
           		    Boolean.valueOf(finfo == null? "" : finfo.getProperty(connBooleanAttributes[i][0])).toString());
				optionsNoDB[attrIndex].description = MessageService.getTextMessage(connBooleanAttributes[i][1]);
				optionsNoDB[attrIndex].choices = BOOLEAN_CHOICES;				
			}

			return optionsNoDB;
		}

		return new DriverPropertyInfo[0];
	}

    /**
     * Checks for System Privileges.
     *
     * @param user The user to be checked for having the permission
     * @param perm The permission to be checked
     * @throws AccessControlException if permissions are missing
     * @throws Exception if the privileges check fails for some other reason
     */
    public void checkSystemPrivileges(String user,
                                      Permission perm)
        throws Exception {
        SecurityUtil.checkUserHasPermission(user, perm);
    }

	public Connection connect( String url, Properties info )
		 throws SQLException 
	{
        return connect( url, info, DriverManager.getLoginTimeout() );
    }
    
    private static final String driver20 = "driver20"; 
    /**
     * Use java.util.concurrent package to enforce login timeouts.
     */
    protected EmbedConnection  timeLogin( String url, Properties info, int loginTimeoutSeconds )
        throws SQLException
    {
        try {
            LoginCallable callable = new LoginCallable( this, url, info );
            Future<EmbedConnection>  task = _executorPool.submit( callable );
            long startTime = System.currentTimeMillis();
            long interruptedTime = startTime;
            
            while ((startTime - interruptedTime) / 1000.0 < loginTimeoutSeconds) {
                try {
                    return task.get( loginTimeoutSeconds, TimeUnit.SECONDS );
                }
                catch (InterruptedException ie) {
                    interruptedTime = System.currentTimeMillis();
                    InterruptStatus.setInterrupted();
                    continue;
                }
                catch (ExecutionException ee) { throw processException( ee ); }
                catch (TimeoutException te) { throw Util.generateCsSQLException( SQLState.LOGIN_TIMEOUT ); }
            }
            
            // Timed out due to interrupts, throw.
            throw Util.generateCsSQLException( SQLState.LOGIN_TIMEOUT );
        } finally {
            InterruptStatus.restoreIntrFlagIfSeen();
        }
    }
    /** Process exceptions raised while running a timed login */
    private SQLException    processException( Throwable t )
    {
        Throwable   cause = t.getCause();
        if ( !(cause instanceof SQLException) ) { return Util.javaException( t ); }
        else { return (SQLException) cause; }
    }

    /** Thread factory to produce daemon threads which don't block VM shutdown */
    private static  final   class   DaemonThreadFactory implements ThreadFactory
    {
        public  Thread newThread( Runnable r )
        {
            Thread  result = new Thread( r );
            result.setDaemon( true );
            return result;
        }
    }

    /**
     * This code is called in a thread which puts time limits on it.
     */
    public  static  final   class   LoginCallable implements  Callable<EmbedConnection>
    {
        private Driver20        _driver;
        private String      _url;
        private Properties  _info;

        public  LoginCallable( Driver20 driver, String url, Properties info )
        {
            _driver = driver;
            _url = url;
            _info = info;
        }

        public  EmbedConnection call()  throws SQLException
        {
            // erase the state variables after we use them.
            // might be paranoid but there could be security-sensitive info
            // in here.
            String  url = _url;
            Properties  info = _info;
            Driver20    driver = _driver;
            _url = null;
            _info = null;
            _driver = null;
            
            return driver.getNewEmbedConnection( url, info );
        }
    }

}
