/*

   Derby - Class org.apache.derby.impl.db.BasicDatabase

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.db;

import org.apache.derby.shared.common.error.PublicAPI;

import org.apache.derby.shared.common.reference.Property;
import org.apache.derby.shared.common.reference.EngineType;
import org.apache.derby.iapi.util.DoubleProperties;
import org.apache.derby.iapi.util.IdUtil;

import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.loader.JarReader;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.db.Database;
import org.apache.derby.iapi.db.DatabaseContext;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.store.raw.data.DataFactory;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.util.StringUtil;
import org.apache.derby.iapi.sql.conn.LanguageConnectionFactory;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.FileInfoDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.sql.LanguageFactory;
import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.store.access.FileResource;
import org.apache.derby.iapi.services.property.PropertyFactory;
import org.apache.derby.iapi.services.property.PropertySetCallback;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.jdbc.AuthenticationService;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.impl.sql.execute.JarUtil;
import org.apache.derby.iapi.services.io.FileUtil;
import org.apache.derby.io.StorageFile;
import org.apache.derby.io.StorageFactory;
import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.store.replication.slave.SlaveFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Dictionary;
import java.util.Locale;
import java.text.DateFormat;

/**
 * The Database interface provides control over the physical database
 * (that is, the stored data and the files the data are stored in),
 * connections to the database, operations on the database such as
 * backup and recovery, and all other things that are associated
 * with the database itself.
 * <p>
 * The Database interface does not provide control over things that are part of
 * the Domain, such as users.
 * <p>
 * I'm not sure what this will hold in a real system, for now
 * it simply provides connection-creation for us.  Perhaps when it boots,
 * it creates the datadictionary object for the database, which all users
 * will then interact with?
 *
 */

public class BasicDatabase implements ModuleControl, ModuleSupportable, PropertySetCallback, Database, JarReader
{
	protected boolean	active;
	private AuthenticationService authenticationService;
	protected AccessFactory af;
	protected PropertyFactory pf;
	protected ClassFactory cfDB; // classFactory but only set when per-database
    /**
     * DataDictionary for this database.
     */
    private DataDictionary dd;
    
	protected LanguageConnectionFactory lcf;
	protected LanguageFactory lf;
	// hold resourceAdapter in an Object instead of a ResourceAdapter
	// so that XA class use can be isolated to XA modules.
	protected Object resourceAdapter;
	private Locale databaseLocale;
	private DateFormat dateFormat;
	private DateFormat timeFormat;
	private DateFormat timestampFormat;
	private UUID		myUUID;

	protected boolean lastToBoot; // is this class last to boot

	/*
	 * ModuleControl interface
	 */

	public boolean canSupport(Properties startParams) {
        boolean supported =
            Monitor.isDesiredCreateType(startParams, getEngineType());
//IC see: https://issues.apache.org/jira/browse/DERBY-3184

        if (supported) {
            String repliMode =
                startParams.getProperty(SlaveFactory.REPLICATION_MODE);
            if (repliMode != null &&
                !repliMode.equals(SlaveFactory.SLAVE_PRE_MODE)) {
                supported = false;
            }
        }

        return supported;
	}

	public void boot(boolean create, Properties startParams)
		throws StandardException
	{

//IC see: https://issues.apache.org/jira/browse/DERBY-6648
		ModuleFactory monitor = getMonitor();
		if (create)
		{
			if (startParams.getProperty(Property.CREATE_WITH_NO_LOG) == null)
				startParams.put(Property.CREATE_WITH_NO_LOG, "true");

			String localeID = 
                startParams.getProperty(
                    org.apache.derby.shared.common.reference.Attribute.TERRITORY);
//IC see: https://issues.apache.org/jira/browse/DERBY-6945

			if (localeID == null) {
				localeID = Locale.getDefault().toString();
			}
			databaseLocale = monitor.setLocale(startParams, localeID);

		} else {
			databaseLocale = monitor.getLocale(this);
		}
		setLocale(databaseLocale);      
//IC see: https://issues.apache.org/jira/browse/DERBY-3147

		// boot the validation needed to do property validation, now property
		// validation is separated from AccessFactory, therefore from store
		bootValidation(create, startParams);
		
		// boot the type factory before store to ensure any dynamically
		// registered types (DECIMAL) are there before logical undo recovery 
        // might need them.
		DataValueFactory dvf = (DataValueFactory) 
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
            bootServiceModule(
                create, 
                this,
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
				org.apache.derby.shared.common.reference.ClassName.DataValueFactory, 
                startParams);

		bootStore(create, startParams);

		// create a database ID if one doesn't already exist
		myUUID = makeDatabaseID(create, startParams);


        // Add the database properties read from disk (not stored
        // in service.properties) into the set seen by booting modules.
//IC see: https://issues.apache.org/jira/browse/DERBY-2341
		Properties allParams =
            new DoubleProperties(getAllDatabaseProperties(), startParams);

		if (pf != null)
			pf.addPropertySetNotification(this);

			// Boot the ClassFactory, will be per-database or per-system.
			// reget the tc in case someone inadverdently destroyed it 
		bootClassFactory(create, allParams);
        
        dd = (DataDictionary)
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
            bootServiceModule(create, this,
                    DataDictionary.MODULE, allParams);
//IC see: https://issues.apache.org/jira/browse/DERBY-2341

		lcf = (LanguageConnectionFactory) 
            bootServiceModule(
                create, this, LanguageConnectionFactory.MODULE, allParams);

		lf = (LanguageFactory) 
            bootServiceModule(
                create, this, LanguageFactory.MODULE, allParams);

		bootResourceAdapter(create, allParams);


		// may also want to set up a check that we are a singleton,
		// or that there isn't already a database object in the system
		// for the same database?


		//
		// We boot the authentication service. There should at least be one
		// per database (even if authentication is turned off) .
		//
		authenticationService = bootAuthenticationService(create, allParams);
		if (SanityManager.DEBUG) {
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
			SanityManager.ASSERT(
                authenticationService != null,
                "Failed to set the Authentication service for the database");
		}

		// Lastly, let store knows that database creation is done and turn
		// on logging
		if (create && lastToBoot &&
			(startParams.getProperty(Property.CREATE_WITH_NO_LOG) != null))
		{
			createFinished();
		}

		active = true;

        // Create an index statistics update daemon.
//IC see: https://issues.apache.org/jira/browse/DERBY-4936
        if (dd.doCreateIndexStatsRefresher()) {
            dd.createIndexStatsRefresher(this, allParams.getProperty(
                        Property.PROPERTY_RUNTIME_PREFIX + "serviceDirectory"));
        }
    }

	public void stop() {
        // The data dictionary is not available if this database has the
        // role as an active replication slave database.
//IC see: https://issues.apache.org/jira/browse/DERBY-5390
        if (dd != null) {
            try {
                // on orderly shutdown, try not to leak unused numbers from
                // the sequence generators.
                dd.clearSequenceCaches();
            } catch (StandardException se) {
                se.printStackTrace(Monitor.getStream().getPrintWriter());
            }
        }
		active = false;
	}

	/*
	** Methods related to  ModuleControl
	*/

	/*
	 * Database interface
 	 */

	/**
     * Return the engine type that this Database implementation
     * supports.
     * This implementation supports the standard database.
	  */
	public int getEngineType() {
//IC see: https://issues.apache.org/jira/browse/DERBY-2164
        return EngineType.STANDALONE_DB;
    }

	public boolean isReadOnly()
	{
		//
		//Notice if no full users?
		//RESOLVE: (Make access factory check?)
		return af.isReadOnly();
	}

	public LanguageConnectionContext setupConnection(ContextManager cm, String user, String drdaID, String dbname)
		throws StandardException {

		TransactionController tc = getConnectionTransaction(cm);

		cm.setLocaleFinder(this);
		pushDbContext(cm);

		// push a database shutdown context
		// we also need to push a language connection context.
		LanguageConnectionContext lctx = lcf.newLanguageConnectionContext(cm, tc, lf, this, user, drdaID, dbname);

		// push the context that defines our class factory
		pushClassFactoryContext(cm, lcf.getClassFactory());

		// we also need to push an execution context.
		ExecutionFactory ef = lcf.getExecutionFactory();

		ef.newExecutionContext(cm);
		//
		//Initialize our language connection context. Note: This is
		//a bit of a hack. Unfortunately, we can't initialize this
		//when we push it. We first must push a few more contexts. 
		lctx.initialize();		
//IC see: https://issues.apache.org/jira/browse/DERBY-3147

		// Need to commit this to release locks gotten in initialize.  
		// Commit it but make sure transaction not have any updates. 
		lctx.internalCommitNoSync(
			TransactionController.RELEASE_LOCKS |
			TransactionController.READONLY_TRANSACTION_INITIALIZATION);

		return lctx;

	}
    
    /**
     * Return the DataDictionary for this database, set up at boot time.
     */
    public final DataDictionary getDataDictionary()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2138
        return dd;
    }

	public void pushDbContext(ContextManager cm)
	{
		/* We cache the locale in the DatabaseContext
		 * so that the Datatypes can get to it easily.
		 */
		DatabaseContext dc = new DatabaseContextImpl(cm, this);
	}

	public AuthenticationService getAuthenticationService()
		throws StandardException{
//IC see: https://issues.apache.org/jira/browse/DERBY-3184

		// Expected to find one - Sanity check being done at
		// DB boot-up.

		// We should have a Authentication Service
		//
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(this.authenticationService != null, 
				"Unexpected - There is no valid authentication service for the database!");
		}
		return this.authenticationService;
	}

    /**
     * Start the replication master role for this database
     * @param dbmaster The master database that is being replicated.
     * @param host The hostname for the slave
     * @param port The port the slave is listening on
     * @param replicationMode The type of replication contract.
     * Currently only asynchronous replication is supported, but
     * 1-safe/2-safe/very-safe modes may be added later.
     * @exception SQLException Thrown on error
     */
    public void startReplicationMaster(String dbmaster, String host, int port,
//IC see: https://issues.apache.org/jira/browse/DERBY-2977
                                       String replicationMode)
        throws SQLException {
        try {
//IC see: https://issues.apache.org/jira/browse/DERBY-3189
            af.startReplicationMaster(dbmaster, host, port, replicationMode);
        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        }
    }
    
    /**
     * Stop the replication master role for this database.
     * 
     * @exception SQLException Thrown on error
     */
    public void stopReplicationMaster()  throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-3189
        try {
            af.stopReplicationMaster();
        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        }
    }
    
    /**
     * Only a SlaveDatabase can be in replication slave mode. Always 
     * throws an exception
     * 
     * @exception SQLException Always thrown because BasicDatabase cannot 
     * be in replication slave mode
     */
    public void stopReplicationSlave() throws SQLException {
        StandardException se = StandardException.
            newException(SQLState.REPLICATION_NOT_IN_SLAVE_MODE);
        throw PublicAPI.wrapStandardException(se);
    }
    
    public boolean isInSlaveMode() {
        return false;
    }
    
    /**
     * @see org.apache.derby.iapi.db.Database#failover(String)
     */
    public void failover(String dbname) throws StandardException {
//IC see: https://issues.apache.org/jira/browse/DERBY-3428
        af.failover(dbname);
    }

	public void freeze() throws SQLException
	{
		try {
			af.freeze();
		} catch (StandardException se) {
			throw PublicAPI.wrapStandardException(se);
		}
	}

	public void unfreeze() throws SQLException
	{
		try {
			af.unfreeze();
		} catch (StandardException se) {
			throw PublicAPI.wrapStandardException(se);
		}
	}


    public void backup(String backupDir, boolean wait) 
//IC see: https://issues.apache.org/jira/browse/DERBY-239
//IC see: https://issues.apache.org/jira/browse/DERBY-523
        throws SQLException
    {
		try {
			af.backup(backupDir, wait);
            if ( luceneLoaded() )
            {
                backupLucene( backupDir );
            }
		} catch (StandardException se) {
			throw PublicAPI.wrapStandardException(se);
		}
	}


    public void backupAndEnableLogArchiveMode(String  backupDir, 
                                              boolean deleteOnlineArchivedLogFiles,
                                              boolean wait)
        throws SQLException
	{
		try {
			af.backupAndEnableLogArchiveMode(backupDir, 
                                             deleteOnlineArchivedLogFiles,
                                             wait); 
//IC see: https://issues.apache.org/jira/browse/DERBY-590
//IC see: https://issues.apache.org/jira/browse/DERBY-590
            if ( luceneLoaded() )
            {
                backupLucene( backupDir );
            }
		} catch (StandardException se) {
			throw PublicAPI.wrapStandardException(se);
		}
	}

	
	public void disableLogArchiveMode(boolean deleteOnlineArchivedLogFiles)
		throws SQLException
	{
		try{
			af.disableLogArchiveMode(deleteOnlineArchivedLogFiles);
		}catch (StandardException se) {
			throw PublicAPI.wrapStandardException(se);
		}
	}


	public void	checkpoint() throws SQLException
	{
		try {
			af.checkpoint();
		} catch (StandardException se) {
			throw PublicAPI.wrapStandardException(se);
		}
	}

	/* Methods from org.apache.derby.database.Database */
	public Locale getLocale() {
		return databaseLocale;
	}


	/**
		Return the UUID of this database.
        @deprecated
	*/
	public final UUID getId() {
		return myUUID;
	}

	/* LocaleFinder methods */

	/** @exception StandardException	Thrown on error */
	public Locale getCurrentLocale() throws StandardException {
		if (databaseLocale != null)
			return databaseLocale;
		throw noLocale();
	}

	/** @exception StandardException	Thrown on error */
	public DateFormat getDateFormat() throws StandardException {
		if (databaseLocale != null) {
			if (dateFormat == null) {
				dateFormat = DateFormat.getDateInstance(DateFormat.LONG,
																databaseLocale);
			}

			return dateFormat;
		}

		throw noLocale();
	}

	/** @exception StandardException	Thrown on error */
	public DateFormat getTimeFormat() throws StandardException {
		if (databaseLocale != null) {
			if (timeFormat == null) {
				timeFormat = DateFormat.getTimeInstance(DateFormat.LONG,
																databaseLocale);
			}

			return timeFormat;
		}

		throw noLocale();
	}

	/** @exception StandardException	Thrown on error */
	public DateFormat getTimestampFormat() throws StandardException {
		if (databaseLocale != null) {
			if (timestampFormat == null) {
				timestampFormat = DateFormat.getDateTimeInstance(
															DateFormat.LONG,
															DateFormat.LONG,
															databaseLocale);
			}

			return timestampFormat;
		}

		throw noLocale();
	}

	private static StandardException noLocale() {
		return StandardException.newException(SQLState.NO_LOCALE);
	}

	public void setLocale(Locale locale) {
		databaseLocale = locale;

		dateFormat = null;
		timeFormat = null;
		timestampFormat = null;
	}

	/**
		Is the database active (open).
	*/
	public boolean isActive() {
		return active;
	}

	/*
	 * class interface
	 */
	public BasicDatabase() {
		lastToBoot = true;
	}


	protected	UUID	makeDatabaseID(boolean create, Properties startParams)
		throws StandardException
	{
		
		TransactionController tc = af.getTransaction(
				getContextService().getCurrentContextManager());
//IC see: https://issues.apache.org/jira/browse/DERBY-6648

		String  upgradeID = null;
		UUID	databaseID;

		if ((databaseID = (UUID) tc.getProperty(DataDictionary.DATABASE_ID)) == null) {

			// no property defined in the Transaction set
			// this could be an upgrade, see if it's stored in the service set

			UUIDFactory	uuidFactory  = getMonitor().getUUIDFactory();
//IC see: https://issues.apache.org/jira/browse/DERBY-6648

			
			upgradeID = startParams.getProperty(DataDictionary.DATABASE_ID);
			if (upgradeID == null )
			{
				// just create one
				databaseID = uuidFactory.createUUID();
			} else {
				databaseID = uuidFactory.recreateUUID(upgradeID);
			}

			tc.setProperty(DataDictionary.DATABASE_ID, databaseID, true);
		}

		// Remove the database identifier from the service.properties
		// file only if we upgraded it to be stored in the transactional
		// property set.
		if (upgradeID != null)
			startParams.remove(DataDictionary.DATABASE_ID);

		tc.commit();
		tc.destroy();

		return databaseID;
	}

	/*
	** Return an Object instead of a ResourceAdapter
	** so that XA classes are only used where needed;
	** caller must cast to ResourceAdapter.
	*/
	public Object getResourceAdapter()
	{
		return resourceAdapter;
	}

	/*
	** Methods of PropertySetCallback
	*/
	public void init(boolean dbOnly, Dictionary p) {
		// not called yet ...
	}

	/**
	  @see PropertySetCallback#validate
	  @exception StandardException Thrown on error.
	*/
	public boolean validate(String key,
						 Serializable value,
						 Dictionary p)
		throws StandardException
	{
		//
		//Disallow setting static creation time only configuration properties
	    if (key.equals(EngineType.PROPERTY))
			throw StandardException.newException(SQLState.PROPERTY_UNSUPPORTED_CHANGE, key, value);
	
		// only interested in the classpath
		if (!key.equals(Property.DATABASE_CLASSPATH)) return false;

		String newClasspath = (String) value;
		String[][] dbcp = null; //The parsed dbclasspath

		if (newClasspath != null) {
			// parse it when it is set to ensure only valid values
			// are written to the actual conglomerate.
//IC see: https://issues.apache.org/jira/browse/DERBY-3147
			dbcp = IdUtil.parseDbClassPath(newClasspath);
		}

		//
		//Verify that all jar files on the database classpath are in the data dictionary.
		if (dbcp != null)
		{
			for (int ix=0;ix<dbcp.length;ix++)
			{
				SchemaDescriptor sd = dd.getSchemaDescriptor(dbcp[ix][IdUtil.DBCP_SCHEMA_NAME], null, false);

                FileInfoDescriptor fid = null;	
				if (sd != null) 
					fid = dd.getFileInfoDescriptor(sd,dbcp[ix][IdUtil.DBCP_SQL_JAR_NAME]);			

				if (fid == null){
					throw StandardException.newException(SQLState.LANG_DB_CLASS_PATH_HAS_MISSING_JAR						, IdUtil.mkQualifiedName(dbcp[ix]));
				}
			}
		}

		return true;
	}
	/**
	  @see PropertySetCallback#apply
	  @exception StandardException Thrown on error.
	*/
	public Serviceable apply(String key, Serializable value, Dictionary p)
		throws StandardException
	{
		// only interested in the classpath
		if (!key.equals(Property.DATABASE_CLASSPATH)) return null;

		// only do the change dynamically if we are already
		// a per-database classapath.
		if (cfDB != null) {

			//
			// Invalidate stored plans.
//IC see: https://issues.apache.org/jira/browse/DERBY-2138
            getDataDictionary().invalidateAllSPSPlans();
		
			String newClasspath = (String) value;
			if (newClasspath == null) newClasspath = "";
			cfDB.notifyModifyClasspath(newClasspath);
		}
		return null;
	}
	/**
	  @see PropertySetCallback#map
	*/
	public Serializable map(String key,Serializable value,Dictionary p)
	{
		return null;
	}

	/*
	 * methods specific to this class 
	 */
	protected void createFinished() throws StandardException
	{
		// find the access factory and tell it that database creation has
		// finished
		af.createFinished();
	}

	protected String getClasspath(Properties startParams) {
		String cp = PropertyUtil.getPropertyFromSet(startParams, Property.DATABASE_CLASSPATH);
		if (cp == null)
			cp = PropertyUtil.getSystemProperty(Property.DATABASE_CLASSPATH, "");
		return cp;
	}


	protected void bootClassFactory(boolean create,
								  Properties startParams) 
		 throws StandardException
	{ 
			String classpath = getClasspath(startParams);

			// parse the class path and allow 2 part names.
			IdUtil.parseDbClassPath(classpath);
//IC see: https://issues.apache.org/jira/browse/DERBY-3147

			startParams.put(Property.BOOT_DB_CLASSPATH, classpath);
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
			cfDB = (ClassFactory) bootServiceModule(create, this,
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
					org.apache.derby.shared.common.reference.Module.ClassFactory, startParams);
	}


	/*
	** Methods to allow sub-classes to offer alternate implementations.
	*/

	protected TransactionController getConnectionTransaction(ContextManager cm)
		throws StandardException {

		// start a local transaction
		return af.getTransaction(cm);
	}

	protected AuthenticationService bootAuthenticationService(boolean create, Properties props) throws StandardException {
		return (AuthenticationService)
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
				bootServiceModule(create, this, AuthenticationService.MODULE, props);
	}

	protected void bootValidation(boolean create, Properties startParams)
		throws StandardException {
		pf = (PropertyFactory) bootServiceModule(create, this,
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
			org.apache.derby.shared.common.reference.Module.PropertyFactory, startParams);
	}

	protected void bootStore(boolean create, Properties startParams)
		throws StandardException {
		af = (AccessFactory) bootServiceModule(create, this, AccessFactory.MODULE, startParams);
	}

    /**
     * Get the set of database properties from the set stored
     * on disk outside of service.properties.
     */
	protected Properties getAllDatabaseProperties()
		throws StandardException {

		TransactionController tc = af.getTransaction(
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
                    getContextService().getCurrentContextManager());
		Properties dbProps = tc.getProperties();
		tc.commit();
		tc.destroy();

		return dbProps;
	}

	protected void bootResourceAdapter(boolean create, Properties allParams) {

		// Boot resource adapter - only if we are running Java 2 or
		// beyondwith JDBC20 extension, JTA and JNDI classes in the classpath
		//
		// assume if it doesn't boot it was because the required
		// classes were missing, and continue without it.
		// Done this way to work around Chai's need to preload
		// classes.
		// Assume both of these classes are in the class path.
		// Assume we may need a ResourceAdapter since we don't know how
		// this database is going to be used.
		try
		{
			resourceAdapter = 
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
				bootServiceModule(create, this,
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
										 org.apache.derby.shared.common.reference.Module.ResourceAdapter,
										 allParams);
		}
		catch (StandardException mse)
		{
			// OK, resourceAdapter is an optional module
		}
	}

	protected void pushClassFactoryContext(ContextManager cm, ClassFactory cf) {
		new StoreClassFactoryContext(cm, cf, af, this);
	}

	/*
	** Methods of JarReader
	*/
	public StorageFile getJarFile(String schemaName, String sqlName)
		throws StandardException {

		SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, null, true);
		FileInfoDescriptor fid = dd.getFileInfoDescriptor(sd,sqlName);
		if (fid == null)
			throw StandardException.newException(SQLState.LANG_FILE_DOES_NOT_EXIST, sqlName,schemaName);

		long generationId = fid.getGenerationId();

//IC see: https://issues.apache.org/jira/browse/DERBY-6648
        ContextManager cm = getContextService().getCurrentContextManager();
		FileResource fr = af.getTransaction(cm).getFileHandler();

//IC see: https://issues.apache.org/jira/browse/DERBY-5357
        String externalName = JarUtil.mkExternalName(
            fid.getUUID(), schemaName, sqlName, fr.getSeparatorChar());

		return fr.getAsFile(externalName, generationId);
	}

    ////////////////////////////////////////////////////////////////////////
    //
    // SUPPORT FOR BACKING UP LUCENE DIRECTORY
    //
    ////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Return true if the Lucene plugin is loaded.
     * </p>
     */
    private boolean luceneLoaded()
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        throws StandardException
    {
        try {
            return AccessController.doPrivileged
                (
                 new PrivilegedExceptionAction<Boolean>()
                 {
                     public Boolean run()
                         throws StandardException
                     {
                         return getLuceneDir().exists();
                     }
                 }
                 ).booleanValue();
        }
        catch (PrivilegedActionException pae) { throw StandardException.plainWrapException( pae ); }
    }

    /** Get the location of the Lucene indexes */
    private StorageFile getLuceneDir()
        throws StandardException
    {
        StorageFactory  dir = getStorageFactory();
		
		return dir.newStorageFile( Database.LUCENE_DIR );
    }

    /**
     * <p>
     * Get the database StorageFactory.
     * </p>
     */
    private StorageFactory  getStorageFactory()
        throws StandardException
    {
        DataFactory dataFactory = (DataFactory) findServiceModule( this, DataFactory.MODULE );
//IC see: https://issues.apache.org/jira/browse/DERBY-6648

        return dataFactory.getStorageFactory();
    }

    /**
     * <p>
     * Backup Lucene indexes to the backup directory. This assumes
     * that the rest of the database has been backup up and sanity
     * checks have been run.
     * </p>
     */
    private void    backupLucene( String backupDir )
        throws StandardException
    {
        try {
            File            backupRoot = new File( backupDir );
            StorageFactory  storageFactory = getStorageFactory();
            String      canonicalDbName = storageFactory.getCanonicalName();
            String      dbname = StringUtil.shortDBName( canonicalDbName, storageFactory.getSeparator() );
            File        backupDB = new File( backupRoot, dbname );
        
            final   File            targetDir = new File( backupDB, Database.LUCENE_DIR );
            final   StorageFile sourceDir = getLuceneDir();

            AccessController.doPrivileged
                (
                 new PrivilegedExceptionAction<Object>()
                 {
                     public Boolean run()
                         throws StandardException
                     {
                         if ( !FileUtil.copyDirectory( getStorageFactory(), sourceDir, targetDir, null, null, true ) )
                         {
                             throw StandardException.newException
                                 (
                                  SQLState.UNABLE_TO_COPY_FILE_FROM_BACKUP,
                                  sourceDir.getPath(),
                                  targetDir.getAbsolutePath()
                                  );
                         }
                         
                         return null;
                     }
                 }
                 );
        }
        catch (IOException ioe) { throw StandardException.plainWrapException( ioe ); }
        catch (PrivilegedActionException pae) { throw StandardException.plainWrapException( pae ); }
    }

    /**
     * Privileged lookup of the ContextService. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ContextService    getContextService()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
        return AccessController.doPrivileged
            (
             new PrivilegedAction<ContextService>()
             {
                 public ContextService run()
                 {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
                     return ContextService.getFactory();
                 }
             }
             );
    }

    /**
     * Privileged Monitor lookup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ModuleFactory  getMonitor()
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<ModuleFactory>()
             {
                 public ModuleFactory run()
                 {
                     return Monitor.getMonitor();
                 }
             }
             );
    }

    
    /**
     * Privileged startup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  Object bootServiceModule
        (
         final boolean create, final Object serviceModule,
         final String factoryInterface, final Properties properties
         )
        throws StandardException
    {
        try {
            return AccessController.doPrivileged
                (
                 new PrivilegedExceptionAction<Object>()
                 {
                     public Object run()
                         throws StandardException
                     {
                         return Monitor.bootServiceModule( create, serviceModule, factoryInterface, properties );
                     }
                 }
                 );
        } catch (PrivilegedActionException pae)
        {
            throw StandardException.plainWrapException( pae );
        }
    }

    /**
     * Privileged startup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  Object findServiceModule( final Object serviceModule, final String factoryInterface)
        throws StandardException
    {
        try {
            return AccessController.doPrivileged
                (
                 new PrivilegedExceptionAction<Object>()
                 {
                     public Object run()
                         throws StandardException
                     {
                         return Monitor.findServiceModule( serviceModule, factoryInterface );
                     }
                 }
                 );
        } catch (PrivilegedActionException pae)
        {
            throw StandardException.plainWrapException( pae );
        }
    }

}
