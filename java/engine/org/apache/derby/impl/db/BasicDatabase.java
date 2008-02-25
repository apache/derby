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

import org.apache.derby.iapi.error.PublicAPI;

import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.EngineType;
import org.apache.derby.iapi.util.DoubleProperties;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.iapi.services.info.JVMInfo;

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
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.property.PersistentSet;
import org.apache.derby.iapi.db.Database;
import org.apache.derby.iapi.db.DatabaseContext;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.sql.compile.OptimizerFactory;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;

import org.apache.derby.iapi.sql.conn.LanguageConnectionFactory;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.FileInfoDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.SPSDescriptor;

import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.LanguageFactory;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.store.access.FileResource;
import org.apache.derby.iapi.services.property.PropertyFactory;
import org.apache.derby.iapi.services.property.PropertySetCallback;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.jdbc.AuthenticationService;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.impl.sql.execute.JarUtil;
import org.apache.derby.io.StorageFile;
import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.services.replication.slave.SlaveFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.File;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Locale;
import java.lang.reflect.Method;
import java.text.Collator;
import java.text.RuleBasedCollator;
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
	private RuleBasedCollator ruleBasedCollator;
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

		ModuleFactory monitor = Monitor.getMonitor();
		if (create)
		{
			if (startParams.getProperty(Property.CREATE_WITH_NO_LOG) == null)
				startParams.put(Property.CREATE_WITH_NO_LOG, "true");

			String localeID = 
                startParams.getProperty(
                    org.apache.derby.iapi.reference.Attribute.TERRITORY);

			if (localeID == null) {
				localeID = Locale.getDefault().toString();
			}
			databaseLocale = monitor.setLocale(startParams, localeID);

		} else {
			databaseLocale = monitor.getLocale(this);
		}
		setLocale(databaseLocale);      

		// boot the validation needed to do property validation, now property
		// validation is separated from AccessFactory, therefore from store
		bootValidation(create, startParams);
		
		// boot the type factory before store to ensure any dynamically
		// registered types (DECIMAL) are there before logical undo recovery 
        // might need them.
		DataValueFactory dvf = (DataValueFactory) 
            Monitor.bootServiceModule(
                create, 
                this,
				org.apache.derby.iapi.reference.ClassName.DataValueFactory, 
                startParams);

		//After booting the DVF, set the Locale information into it. This 
		//Locale will be either the Locale obtained from the territory 
		//attribute supplied by the user on the JDBC url at database create 
		//time or if user didn't provide the territory attribute at database
		//create time, then it will be set to the default JVM locale. If user 
		//has requested territory based collation then a Collator object will
		//be constructed from this Locale object. 
		dvf.setLocale(databaseLocale);

		bootStore(create, startParams);

		// create a database ID if one doesn't already exist
		myUUID = makeDatabaseID(create, startParams);


        // Add the database properties read from disk (not stored
        // in service.properties) into the set seen by booting modules.
		Properties allParams =
            new DoubleProperties(getAllDatabaseProperties(), startParams);

		if (pf != null)
			pf.addPropertySetNotification(this);

			// Boot the ClassFactory, will be per-database or per-system.
			// reget the tc in case someone inadverdently destroyed it 
		bootClassFactory(create, allParams);
        
        dd = (DataDictionary)
            Monitor.bootServiceModule(create, this,
                    DataDictionary.MODULE, allParams);

		lcf = (LanguageConnectionFactory) 
            Monitor.bootServiceModule(
                create, this, LanguageConnectionFactory.MODULE, allParams);

		lf = (LanguageFactory) 
            Monitor.bootServiceModule(
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

	}

	public void stop() {
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
                                       String replicationMode)
        throws SQLException {
        try {
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
     * @see org.apache.derby.database.Database#failover(String dbname).
     */
    public void failover(String dbname) throws SQLException {
        try {
            af.failover(dbname);
        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        }
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
        throws SQLException
    {
		try {
			af.backup(backupDir, wait);
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
	public RuleBasedCollator getCollator() throws StandardException {
		RuleBasedCollator retval = ruleBasedCollator;

		if (retval == null) {
			if (databaseLocale != null) {
				retval = ruleBasedCollator =
					(RuleBasedCollator) Collator.getInstance(databaseLocale);
			} else {
				throw noLocale();
			}
		}

		return retval;
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
				ContextService.getFactory().getCurrentContextManager());

		String  upgradeID = null;
		UUID	databaseID;

		if ((databaseID = (UUID) tc.getProperty(DataDictionary.DATABASE_ID)) == null) {

			// no property defined in the Transaction set
			// this could be an upgrade, see if it's stored in the service set

			UUIDFactory	uuidFactory  = Monitor.getMonitor().getUUIDFactory();

			
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

			startParams.put(Property.BOOT_DB_CLASSPATH, classpath);
			cfDB = (ClassFactory) Monitor.bootServiceModule(create, this,
					org.apache.derby.iapi.reference.Module.ClassFactory, startParams);
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
				Monitor.bootServiceModule(create, this, AuthenticationService.MODULE, props);
	}

	protected void bootValidation(boolean create, Properties startParams)
		throws StandardException {
		pf = (PropertyFactory) Monitor.bootServiceModule(create, this,
			org.apache.derby.iapi.reference.Module.PropertyFactory, startParams);
	}

	protected void bootStore(boolean create, Properties startParams)
		throws StandardException {
		af = (AccessFactory) Monitor.bootServiceModule(create, this, AccessFactory.MODULE, startParams);
	}

    /**
     * Get the set of database properties from the set stored
     * on disk outside of service.properties.
     */
	protected Properties getAllDatabaseProperties()
		throws StandardException {

		TransactionController tc = af.getTransaction(
                    ContextService.getFactory().getCurrentContextManager());
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
				Monitor.bootServiceModule(create, this,
										 org.apache.derby.iapi.reference.Module.ResourceAdapter,
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

        ContextManager cm = ContextService.getFactory().getCurrentContextManager();
		FileResource fr = af.getTransaction(cm).getFileHandler();

		String externalName = JarUtil.mkExternalName(schemaName, sqlName, fr.getSeparatorChar());

		return fr.getAsFile(externalName, generationId);
	}

}
