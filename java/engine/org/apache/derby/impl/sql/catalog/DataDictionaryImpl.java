/*

   Derby - Class org.apache.derby.impl.sql.catalog.DataDictionaryImpl

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

package org.apache.derby.impl.sql.catalog;

import org.apache.derby.iapi.reference.JDBC30Translation;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.DB2Limit;

import org.apache.derby.iapi.sql.compile.CompilerContext;

import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;

import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptorList;
import org.apache.derby.iapi.sql.dictionary.FileInfoDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptorList;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptorList;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.DefaultDescriptor;
import org.apache.derby.iapi.sql.dictionary.DependencyDescriptor;
import org.apache.derby.iapi.sql.dictionary.ForeignKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.GenericDescriptorList;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.sql.dictionary.KeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ReferencedKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.RowList;
import org.apache.derby.iapi.sql.dictionary.SPSDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.CheckConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.SubCheckConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.SubConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.SubKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.TabInfo;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;
import org.apache.derby.iapi.sql.dictionary.ViewDescriptor;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;

import org.apache.derby.iapi.sql.depend.DependencyManager;

import org.apache.derby.impl.sql.depend.BasicDependencyManager;

import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.ScanQualifier;

import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.NumberDataValue;

import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.LanguageConnectionFactory;

import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.types.Orderable;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.store.access.RowUtil;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.Qualifier;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;

import org.apache.derby.iapi.error.StandardException;

// RESOLVE - paulat - remove this import when track 3677 is fixed
import org.apache.derby.iapi.services.sanity.AssertFailure;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.TupleFilter;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.cache.CacheFactory;
import org.apache.derby.iapi.services.cache.CacheManager;
import org.apache.derby.iapi.services.cache.Cacheable;
import org.apache.derby.iapi.services.cache.CacheableFactory;

import org.apache.derby.iapi.services.locks.LockFactory;
import org.apache.derby.iapi.services.locks.C_LockFactory;

import org.apache.derby.iapi.services.property.PropertyUtil;

import org.apache.derby.impl.services.locks.Timeout;

import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.catalog.AliasInfo;
import org.apache.derby.catalog.DefaultInfo;
import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.types.RoutineAliasInfo;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.locks.ShExLockable;
import org.apache.derby.iapi.services.locks.ShExQual;
import org.apache.derby.iapi.util.StringUtil;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

import java.util.List;
import java.util.Iterator;

import java.util.Enumeration;
import java.io.InputStream;
import java.io.IOException;

import java.sql.Types;

/**
 * This abstract class contains the common code for the "regular" and
 * limited data dictionaries.  The limited configuration puts an upper
 * limit on the number of tables a user is allowed to create.
 *
 * This class provides the entire implementation of DataDictionary,
 * and ModuleControl, except for the stop()
 * method, which is to be provided by a non-abstract super-class.
 * The reason for putting the stop() method in the super-class is to
 * prevent someone from inadvertently changing this to a non-abstract
 * class.  This class is shipped with both the limited and non-limited
 * configurations, and we don't want anyone to be able to cheat by
 * booting this class instead of booting the super-class.
 */

public	class	DataDictionaryImpl
	implements DataDictionary, CacheableFactory, ModuleControl, ModuleSupportable,java.security.PrivilegedAction
{

    private static final String		CFG_SYSTABLES_ID = "SystablesIdentifier";
	private static final String		CFG_SYSTABLES_INDEX1_ID = "SystablesIndex1Identifier";
	private static final String		CFG_SYSTABLES_INDEX2_ID = "SystablesIndex2Identifier";
	private static final String		CFG_SYSCOLUMNS_ID = "SyscolumnsIdentifier";
	private static final String		CFG_SYSCOLUMNS_INDEX1_ID = "SyscolumnsIndex1Identifier";
	private static final String		CFG_SYSCOLUMNS_INDEX2_ID = "SyscolumnsIndex2Identifier";
	private static final String		CFG_SYSCONGLOMERATES_ID = "SysconglomeratesIdentifier";
	private static final String		CFG_SYSCONGLOMERATES_INDEX1_ID = "SysconglomeratesIndex1Identifier";
	private static final String		CFG_SYSCONGLOMERATES_INDEX2_ID = "SysconglomeratesIndex2Identifier";
	private static final String		CFG_SYSCONGLOMERATES_INDEX3_ID = "SysconglomeratesIndex3Identifier";
	private static final String		CFG_SYSSCHEMAS_ID = "SysschemasIdentifier";
	private static final String		CFG_SYSSCHEMAS_INDEX1_ID = "SysschemasIndex1Identifier";
	private static final String		CFG_SYSSCHEMAS_INDEX2_ID = "SysschemasIndex2Identifier";

	private	static final int		SYSCONGLOMERATES_CORE_NUM = 0;
	private	static final int		SYSTABLES_CORE_NUM = 1;
	private static final int		SYSCOLUMNS_CORE_NUM = 2;
	private	static final int		SYSSCHEMAS_CORE_NUM = 3;
	private static final int        NUM_CORE = 4;

	// the structure that holds all the core table info
	private TabInfo[] coreInfo;

	/*
	** SchemaDescriptors for system and app schemas.  Both
	** are canonical.  We cache them for fast lookup.
	*/
	protected SchemaDescriptor systemSchemaDesc;
	protected SchemaDescriptor sysIBMSchemaDesc;
	protected SchemaDescriptor declaredGlobalTemporaryTablesSchemaDesc;
	protected SchemaDescriptor systemDiagSchemaDesc;
	protected SchemaDescriptor systemUtilSchemaDesc;

    private String systemSchemaName;
    private String systemDiagSchemaName;
    private String systemUtilSchemaName;
    private String sysIBMSchemaName;
    private String declaredGlobalTemporaryTablesSchemaName;
    boolean builtinSchemasAreFromLCC;

    protected boolean convertIdToLower;
    // Convert identifiers to lower case (as in Foundation) or not.
    
	private	static final int		NUM_NONCORE = 12;

	// This array of non-core table names *MUST* be in the same order
	// as the non-core table numbers, above.
	private static final String[] nonCoreNames = {
									"SYSCONSTRAINTS",
									"SYSKEYS",
									"SYSDEPENDS",
									"SYSALIASES",
									"SYSVIEWS",
									"SYSCHECKS",
									"SYSFOREIGNKEYS",
									"SYSSTATEMENTS",
									"SYSFILES",
									"SYSTRIGGERS",
									"SYSSTATISTICS",
									"SYSDUMMY1"
									};

    /**
     * List of all "system" schemas
     * <p>
     * This list should contain all schema's used by the system and are
     * created when the database is created.  Users should not be able to
     * create or drop these schema's and should not be able to create or
     * drop objects in these schema's.  This list is used by code that
     * needs to check if a particular schema is a "system" schema.
     **/
    private static final String[] systemSchemaNames = {
        SchemaDescriptor.IBM_SYSTEM_CAT_SCHEMA_NAME,
        SchemaDescriptor.IBM_SYSTEM_FUN_SCHEMA_NAME,
        SchemaDescriptor.IBM_SYSTEM_PROC_SCHEMA_NAME,
        SchemaDescriptor.IBM_SYSTEM_STAT_SCHEMA_NAME,
        SchemaDescriptor.IBM_SYSTEM_NULLID_SCHEMA_NAME,
        SchemaDescriptor.STD_SYSTEM_DIAG_SCHEMA_NAME,
        SchemaDescriptor.STD_SYSTEM_UTIL_SCHEMA_NAME,
        SchemaDescriptor.IBM_SYSTEM_SCHEMA_NAME,
        SchemaDescriptor.STD_SQLJ_SCHEMA_NAME,
        SchemaDescriptor.STD_SYSTEM_SCHEMA_NAME
    };
        

	/** Dictionary version of the on-disk database */
	private DD_Version  dictionaryVersion;
	/** Dictionary version of the currently running engine */
	private DD_Version  softwareVersion;

	/*
	** This property and value are written into the database properties
	** when the database is created, and are used to determine whether
	** the system catalogs need to be upgraded.
	*/

	// the structure that holds all the noncore info
	private TabInfo[] noncoreInfo;

	// no other system tables have id's in the configuration.

	public	DataDescriptorGenerator		dataDescriptorGenerator;
	protected DataValueFactory			dvf;
	protected AccessFactory               af;
	//DataDictionaryContext				ddc;

	private	ExecutionFactory	exFactory;
	protected UUIDFactory		uuidFactory;

	Properties			startupParameters;
	int					engineType;

	/* Information about whether or not we are at boot time */
	protected	boolean				booting;
	private TransactionController	bootingTC;
	protected   DependencyManager dmgr;

	/* Cache of table descriptors */
	CacheManager	OIDTdCache;
	CacheManager	nameTdCache;
	private CacheManager	spsNameCache;
	private Hashtable		spsIdHash;
	// private Hashtable       spsTextHash;
	int				tdCacheSize;
	int				stmtCacheSize;	

	/*
	** Lockable object for synchronizing transition from caching to non-caching
	*/
	ShExLockable	cacheCoordinator;
	public	LockFactory		lockFactory;

	volatile int	cacheMode = DataDictionary.COMPILE_ONLY_MODE;

	/* Number of DDL users */
	volatile int	ddlUsers; 
	/* Number of readers that start in DDL_MODE */
	volatile int	readersInDDLMode;


	/**
		True if the database is read only and requires
		some form of upgrade, that makes the stored prepared
		statements invalid.
		With this case the engine is running at a different
		version to the underlying stored database. This
		can happen in 5.1 if the database is read only
		and a different point release (later than 5.1.25?)
		to the one that created it, has been booted. (Beetle 5170).

		<P>
		In 5.2 and newer this will be the case if the engine
		booting the database is newer than the engine
		that created it.

	*/
	public boolean readOnlyUpgrade;

	//systemSQLNameNumber is the number used as the last digit during the previous call to getSystemSQLName.
	//If it is 9 for a given calendarForLastSystemSQLName, we will restart the counter to 0
	//and increment the calendarForLastSystemSQLName by 10ms.
	private int systemSQLNameNumber;
	private GregorianCalendar calendarForLastSystemSQLName = new GregorianCalendar();
	private long timeForLastSystemSQLName;

	/*
	** Constructor
	*/

	public DataDictionaryImpl() {
		
	}


	/**
	  Currently, all this routine does is check to see if the Replication
	  property has been turned on for this database. If so, then this is not
	  the NodeFactory that's wanted--so we return false. The NodeFactory that
	  is wanted is our child class "RepNodeFactory".

	  @return	true		if this database does not want Replication
	            false		otherwise
	  */

	public boolean canSupport(Properties startParams)
	{
		return	Monitor.isDesiredType( startParams, org.apache.derby.iapi.reference.EngineType.NONE );
	}

	/**
	 * Start-up method for this instance of the data dictionary.
	 *
	 * @param startParams	The start-up parameters
	 *
	 * @return	Nothing
	 *
	 *	@exception StandardException	Thrown if the module fails to start
	 */
	public void boot(boolean create, Properties startParams) 
			throws StandardException
	{
		softwareVersion = new DD_Version(this, DataDictionary.DD_VERSION_CS_10_0);

		/* There is a bootstrapping problem here. We would like to use
         * a language connection context to find the name of the system and default
         * schemas. However a language connection context is not available when a
         * database is being created, as it is when this method is called. So,
         * this method must look at the params properties to discover the identifier
         * casing and convert the standard names as necessary, essentially duplicating
         * logic found in GenericLanguageConnectionContext.
         */
        convertIdToLower = false;

		startupParameters = startParams;

		uuidFactory = Monitor.getMonitor().getUUIDFactory();

		engineType = Monitor.getEngineType( startParams );

		// REMIND: actually, we're supposed to get the DataValueFactory
		// out of the connection context...this is a bit of a shortcut.
		// We get the DataValueFactory early in order to help bootstrap the system catalogs.
		LanguageConnectionFactory langConnFactory = (LanguageConnectionFactory) Monitor.bootServiceModule(
			create, this, LanguageConnectionFactory.MODULE, startParams);

		dvf = langConnFactory.getDataValueFactory();
		exFactory = (ExecutionFactory) Monitor.bootServiceModule(
														create, this,
														ExecutionFactory.MODULE,
														startParams);

		// initailze the arrays of core and noncore tables
		initializeCatalogInfo();

		// indicate that we are in the process of booting
		booting = true;

		// set only if child class hasn't overriden this already
		if ( dataDescriptorGenerator == null )
		{ dataDescriptorGenerator = new DataDescriptorGenerator( this ); }

		if (!create) {


			// SYSTABLES

			coreInfo[SYSTABLES_CORE_NUM].setHeapConglomerate(
					getBootParameter(startParams, CFG_SYSTABLES_ID, true));

			coreInfo[SYSTABLES_CORE_NUM].setIndexConglomerate(SYSTABLESRowFactory.SYSTABLES_INDEX1_ID,
					getBootParameter(startParams, CFG_SYSTABLES_INDEX1_ID, true));


			coreInfo[SYSTABLES_CORE_NUM].setIndexConglomerate(
					SYSTABLESRowFactory.SYSTABLES_INDEX2_ID,
					getBootParameter(startParams, CFG_SYSTABLES_INDEX2_ID, true));

			// SYSCOLUMNS

			coreInfo[SYSCOLUMNS_CORE_NUM].setHeapConglomerate(
					getBootParameter(startParams, CFG_SYSCOLUMNS_ID, true));


			coreInfo[SYSCOLUMNS_CORE_NUM].setIndexConglomerate(
					SYSCOLUMNSRowFactory.SYSCOLUMNS_INDEX1_ID,
					getBootParameter(startParams, CFG_SYSCOLUMNS_INDEX1_ID, true));
			// 2nd syscolumns index added in Xena, hence may not be there
			coreInfo[SYSCOLUMNS_CORE_NUM].setIndexConglomerate(
					SYSCOLUMNSRowFactory.SYSCOLUMNS_INDEX2_ID,
					getBootParameter(startParams, CFG_SYSCOLUMNS_INDEX2_ID, false));

			// SYSCONGLOMERATES

			coreInfo[SYSCONGLOMERATES_CORE_NUM].setHeapConglomerate(
					getBootParameter(startParams, CFG_SYSCONGLOMERATES_ID, true));


			coreInfo[SYSCONGLOMERATES_CORE_NUM].setIndexConglomerate(
					SYSCONGLOMERATESRowFactory.SYSCONGLOMERATES_INDEX1_ID,
					getBootParameter(startParams, CFG_SYSCONGLOMERATES_INDEX1_ID, true));


			coreInfo[SYSCONGLOMERATES_CORE_NUM].setIndexConglomerate(
					SYSCONGLOMERATESRowFactory.SYSCONGLOMERATES_INDEX2_ID,
					getBootParameter(startParams, CFG_SYSCONGLOMERATES_INDEX2_ID, true));

			coreInfo[SYSCONGLOMERATES_CORE_NUM].setIndexConglomerate(
					SYSCONGLOMERATESRowFactory.SYSCONGLOMERATES_INDEX3_ID,
					getBootParameter(startParams, CFG_SYSCONGLOMERATES_INDEX3_ID, true));


			// SYSSCHEMAS
			coreInfo[SYSSCHEMAS_CORE_NUM].setHeapConglomerate(
					getBootParameter(startParams, CFG_SYSSCHEMAS_ID, true));


			coreInfo[SYSSCHEMAS_CORE_NUM].setIndexConglomerate(
					SYSSCHEMASRowFactory.SYSSCHEMAS_INDEX1_ID,
					getBootParameter(startParams, CFG_SYSSCHEMAS_INDEX1_ID, true));

			coreInfo[SYSSCHEMAS_CORE_NUM].setIndexConglomerate(
					SYSSCHEMASRowFactory.SYSSCHEMAS_INDEX2_ID,
					getBootParameter(startParams, CFG_SYSSCHEMAS_INDEX2_ID, true));

		}



		String value = startParams.getProperty(Property.LANG_TD_CACHE_SIZE);
		tdCacheSize = PropertyUtil.intPropertyValue(Property.LANG_TD_CACHE_SIZE, value,
									   0, Integer.MAX_VALUE, Property.LANG_TD_CACHE_SIZE_DEFAULT);

		
		value = startParams.getProperty(Property.LANG_SPS_CACHE_SIZE);
		stmtCacheSize = PropertyUtil.intPropertyValue(Property.LANG_SPS_CACHE_SIZE, value,
									   0, Integer.MAX_VALUE, Property.LANG_SPS_CACHE_SIZE_DEFAULT);


		/*
		 * data dictionary contexts are only associated with connections.
		 * we have to look for the basic data dictionary, as there is
		 * no connection, and thus no context stack yet.
		 */

		/*
		 * Get the table descriptor cache.
		 */
		CacheFactory cf =
				(CacheFactory) Monitor.startSystemModule(org.apache.derby.iapi.reference.Module.CacheFactory);
		OIDTdCache =
			cf.newCacheManager(this,
				"TableDescriptorOIDCache",
				tdCacheSize,
				tdCacheSize);
		nameTdCache =
			cf.newCacheManager(this,
				"TableDescriptorNameCache",
				tdCacheSize,
				tdCacheSize);

		if (stmtCacheSize > 0)
		{
			spsNameCache =
				cf.newCacheManager(this,
					"SPSNameDescriptorCache",
					stmtCacheSize,
					stmtCacheSize);
			spsIdHash = new Hashtable(stmtCacheSize);
			// spsTextHash = new Hashtable(stmtCacheSize);
		}


		/* Get the object to coordinate cache transitions */
		cacheCoordinator = new ShExLockable();

		/* Get AccessFactory in order to transaction stuff */
		af = (AccessFactory)  Monitor.findServiceModule(this, AccessFactory.MODULE);

		/* Get the lock factory */
		lockFactory = af.getLockFactory();

		/*
		 * now we need to setup a context stack for the database creation work.
		 * We assume the System boot process has created a context
		 * manager already, but not that contexts we need are there.
		 */
		ContextService csf = ContextService.getFactory();

		ContextManager cm = csf.getCurrentContextManager();
		if (SanityManager.DEBUG)
			SanityManager.ASSERT((cm != null), "Failed to get current ContextManager");

		/* push a datadictionary context onto this stack */
		pushDataDictionaryContext(cm, false);

		// RESOLVE other non-StandardException errors.
		bootingTC = null;
		try
		{
			// Get a transaction controller. This has the side effect of
			// creating a transaction context if there isn't one already.
			bootingTC = af.getTransaction(cm);

			/*
				We need an execution context so that we can generate rows
				REMIND: maybe only for create case?
			 */
			exFactory.newExecutionContext(cm);

			DataDescriptorGenerator ddg = getDataDescriptorGenerator();
	
			if (create) {

				// create any required tables.
				createDictionaryTables(startParams, bootingTC, ddg);
				//create procedures for network server metadata
				create_SYSIBM_procedures(bootingTC);
				//create metadata sps statement required for network server
				createSystemSps(bootingTC);
                // create the SYSCS_UTIL system procedures)
                create_SYSCS_procedures(bootingTC);
				// log the current dictionary version
				dictionaryVersion = softwareVersion;

				/* Set properties for current and create time 
				 * DataDictionary versions.
				 */
				bootingTC.setProperty(
                    DataDictionary.CORE_DATA_DICTIONARY_VERSION,
                    dictionaryVersion, true);

				bootingTC.setProperty(
                    DataDictionary.CREATE_DATA_DICTIONARY_VERSION,
                    dictionaryVersion, true);

			} else {
				// Get the ids for non-core tables
				loadDictionaryTables(bootingTC, ddg, startParams);
			}
					
			/* Commit & destroy the create database */
			bootingTC.commit();
			cm.getContext(ExecutionContext.CONTEXT_ID).popMe(); // done with ctx
		} finally {

			if (bootingTC != null) {
				bootingTC.destroy();  // gets rid of the transaction context
				bootingTC = null;
			}
			cm.popContext(); // the data dictionary context; check that it is?
		}
	
		setDependencyManager();
		booting = false;
	}

	/** 
	 * sets the dependencymanager associated with this dd. subclasses can
	 * override this to install their own funky dependency manager.
	 */
	protected void setDependencyManager()
	{
		dmgr = new BasicDependencyManager();
	}

	/**
	 * returns the dependencymanager associated with this datadictionary.
	 * @see DataDictionary#getDependencyManager
	 */
	public DependencyManager getDependencyManager()
	{
		return dmgr;
	}

	/**
	 * Stop this module.  In this case, nothing needs to be done.
	 *
	 * @return	Nothing
	 */

	public void stop()
	{
	}

	/*
	** CacheableFactory interface
	*/
	public Cacheable newCacheable(CacheManager cm) {

		if (cm == OIDTdCache)
			return new OIDTDCacheable(this);
		else if (cm == nameTdCache)
			return new NameTDCacheable(this);
		else {
			return new SPSNameCacheable(this);
		}
	}

	/*
	** Methods related to ModuleControl
	*/

	/**
	 * @see org.apache.derby.iapi.sql.dictionary.DataDictionary#startReading
	 *
	 * @exception StandardException		Thrown on error
	 */
	public int startReading(LanguageConnectionContext lcc)
		throws StandardException
	{
		int     bindCount = lcc.incrementBindCount();
		int     localCacheMode;

        boolean needRetry = false;

        do
        {
            if (needRetry)
            {
                // could not get lock while holding the synchronized(this),
                // so now wait until we can get the lock.  Once we get the
                // lock it is automatically released, hopefully when we 
                // go the the synchronized(this) block we will be able to
                // get the lock, while holding the synchronized(this)
                // monitor now.

                try
                {
                    lockFactory.zeroDurationlockObject(
                        lcc.getTransactionExecute().getLockObject(),
                        cacheCoordinator,
                        ShExQual.SH,
                        C_LockFactory.WAIT_FOREVER);
                }
                catch (StandardException e)
                {
                    // DEADLOCK, timeout will not happen with WAIT_FOREVER

                    lcc.decrementBindCount();
                    throw e;
                }
                needRetry = false;
            }

            // "this" is used to synchronize between startReading,doneReading, 
            //  and startWriting.

            synchronized(this)
            {
                localCacheMode = getCacheMode();

                /*
                ** Keep track of how deeply nested this bind() operation is.
                ** It's possible for nested binding to happen if the user
                ** prepares SQL statements from within a static initializer
                ** of a class, and calls a method on that class (or uses a
                ** field in the class).
                **
                ** If nested binding is happening, we only want to lock the
                ** DataDictionary on the outermost nesting level.
                */
                if (bindCount == 1)
                {
                    if (localCacheMode == DataDictionary.COMPILE_ONLY_MODE)
                    {
                        if (SanityManager.DEBUG)
                        {
                            SanityManager.ASSERT(ddlUsers == 0,
                                "Cache mode is COMPILE_ONLY and there are DDL users.");
                        }

                        /*
                        ** If we deadlock while waiting for a lock,
                        ** then be sure to restore things as they
                        ** were.
                        */
                        boolean lockGranted = false;

                        try
                        {
                            // When the C_LockFactory.NO_WAIT is used this 
                            // routine will not throw timeout or deadlock 
                            // exceptions.  The boolean returned will indicate 
                            // if the lock was granted or not.  If it would 
                            // have had to wait, it just returns immediately 
                            // and returns false.
                            // 
                            // See if we can get this lock granted without 
                            // waiting (while holding the dataDictionary 
                            // synchronization).

                            lockGranted = 
                                lockFactory.lockObject(
                                    lcc.getTransactionExecute().getLockObject(),
                                    lcc.getTransactionExecute().getLockObject(),
                                    cacheCoordinator,
                                    ShExQual.SH,
                                    C_LockFactory.NO_WAIT);
                        } 
                        catch (StandardException e)
                        {
                            // neither TIMEOUT or DEADLOCK can happen with 
                            // NO_WAIT flag.  This must be some other exception.

                            lcc.decrementBindCount();
                            throw e;
                        }

                        if (!lockGranted)
                            needRetry = true;
                    }
                    else
                    {
                        readersInDDLMode++;
                    }
                }
            } // end of sync block
            
        } while (needRetry);

		return localCacheMode;
	}

	/* @see org.apache.derby.iapi.sql.dictionary.DataDictionary#doneReading */
	public void doneReading(int mode, LanguageConnectionContext lcc) 
		throws StandardException
	{
		int bindCount = lcc.decrementBindCount();

		/* This is an arbitrary choice of object to synchronize these methods */
		synchronized(this)
		{
			/*
			** Keep track of how deeply nested this bind() operation is.
			** It's possible for nested binding to happen if the user
			** prepares SQL statements from within a static initializer
			** of a class, and calls a method on that class (or uses a
			** field in the class).
			**
			** If nested binding is happening, we only want to unlock the
			** DataDictionary on the outermost nesting level.
			*/
			if (bindCount == 0)
			{
				if (mode == DataDictionary.COMPILE_ONLY_MODE)
				{
					/*
					** Release the share lock that was acquired by the reader when
					** it called startReading().
					** Beetle 4418, during bind, we may even execute something (eg., in a vti
					** constructor) and if a severe error occured, the transaction is rolled
					** back and lock released already, so don't try to unlock if statement context
					** is cleared.
					*/
					if ((lcc.getStatementContext() != null) && lcc.getStatementContext().inUse())
					{
						int unlockCount = lockFactory.unlock(lcc.getTransactionExecute().getLockObject(),
										lcc.getTransactionExecute().getLockObject(),
										cacheCoordinator,
										ShExQual.SH);
						if (SanityManager.DEBUG)
						{
							if (unlockCount != 1)
							{
								SanityManager.THROWASSERT("unlockCount not "+
									"1 as expected, it is "+unlockCount);
							}
						}
					}
				}
				else 
				{
					readersInDDLMode--;

					/*
					** We can only switch back to cached (COMPILE_ONLY)
					** mode if there aren't any readers that started in
					** DDL_MODE.  Otherwise we could get a reader
					** in DDL_MODE that reads a cached object that
					** was brought in by a reader in COMPILE_ONLY_MODE.
					** If 2nd reader finished and releases it lock
					** on the cache there is nothing to pevent another
					** writer from coming along an deleting the cached
					** object.
					*/
					if (ddlUsers == 0 && readersInDDLMode == 0)
					{
						clearCaches();
						setCacheMode(DataDictionary.COMPILE_ONLY_MODE);
					}

					if (SanityManager.DEBUG)
					{
						SanityManager.ASSERT(readersInDDLMode >= 0,
							"readersInDDLMode is invalid -- should never be < 0");
					}
				}
			}
		}
	}

	/*
	 * @see org.apache.derby.iapi.sql.dictionary.DataDictionary#startWriting
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void startWriting(LanguageConnectionContext lcc) 
		throws StandardException
	{

		boolean blocked = true;
	
		/*
		** Don't allow DDL if we're binding a SQL statement.
		*/
		if (lcc.getBindCount() != 0)
		{
			throw StandardException.newException(SQLState.LANG_DDL_IN_BIND);
		}
	
		/*
		** Check whether we've already done a DDL statement in this
		** transaction.  If so, we don't want to re-enter DDL mode, or
		** bump the DDL user count.
		*/
		if ( ! lcc.dataDictionaryInWriteMode())
		{
			for (int i = 0; blocked; i++)
			{
				/*
				** If we already tried 5 times and failed, do
				** an unbounded wait for the lock w/o
				** synchronization.  Immediately unlock and
				** sleep a random amount of time and start
				** the whole process over again.
				*/
				if (i > 4 && 
					getCacheMode() == DataDictionary.COMPILE_ONLY_MODE)
				{
                    // Wait until the settable timeout value for the lock,
                    // and once granted, immediately release the lock.  If
                    // this wait time's out then a TIMEOUT error is sent
                    // up the stack.

                    lockFactory.zeroDurationlockObject(
                        lcc.getTransactionExecute().getLockObject(),
                        cacheCoordinator,
                        ShExQual.EX,
                        C_LockFactory.TIMED_WAIT);


					i = 1;
				}

				if (i > 0)
				{
					try 
                    { 
                        Thread.sleep(
                            (long)((java.lang.Math.random() * 1131) % 20)); 
                    }
					catch (InterruptedException ie)
					{
						throw StandardException.interrupt(ie);
					}
				} 

				synchronized(this)
				{
					if (getCacheMode() == DataDictionary.COMPILE_ONLY_MODE)
					{
                        // When the C_LockFactory.NO_WAIT is used this routine 
                        // will not throw timeout or deadlock exceptions.  The 
                        // boolean returned will indicate if the lock was 
                        // granted or not.  If it would have had to wait, it 
                        // just returns immediately and returns false.
                        // 

                        // See if we can get this lock granted without waiting
                        // (while holding the dataDictionary synchronization).

                        boolean lockGranted = 
                            lockFactory.zeroDurationlockObject(
                                lcc.getTransactionExecute().getLockObject(),
                                cacheCoordinator,
                                ShExQual.EX,
                                C_LockFactory.NO_WAIT);

                        if (!lockGranted)
                            continue;

						/* Switch the caching mode to DDL */
						setCacheMode(DataDictionary.DDL_MODE);
	
						/* Clear out all the caches */
						clearCaches();
					}
		
					/* Keep track of the number of DDL users */
					ddlUsers++;
				} // end synchronized	
	
				/*
				** Tell the connection the DD is in DDL mode, so it can take
				** it out of DDL mode when the transaction finishes.
				*/
				lcc.setDataDictionaryWriteMode();
				blocked = false;
			}

		}
		else if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(getCacheMode() == DataDictionary.DDL_MODE,
				"lcc.getDictionaryInWriteMode() but DataDictionary is COMPILE_MODE");
		}
	}


	/* @see org.apache.derby.iapi.sql.dictionary.DataDictionary#transactionFinished */
	public void transactionFinished() throws StandardException
	{
		/* This is an arbitrary choice of object to synchronize these methods */
		synchronized(this)
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(ddlUsers > 0,
					"Number of DDL Users is <= 0 when finishing a transaction");

				SanityManager.ASSERT(getCacheMode() == DataDictionary.DDL_MODE,
					"transactionFinished called when not in DDL_MODE");
			}

			ddlUsers--;

			/*
			** We can only switch back to cached (COMPILE_ONLY)
			** mode if there aren't any readers that started in
			** DDL_MODE.  Otherwise we could get a reader
			** in DDL_MODE that reads a cached object that
			** was brought in by a reader in COMPILE_ONLY_MODE.
			** If 2nd reader finished and releases it lock
			** on the cache there is nothing to pevent another
			** writer from coming along an deleting the cached
			** object.
			*/
			if (ddlUsers == 0 && readersInDDLMode == 0)
			{
				clearCaches();
				setCacheMode(DataDictionary.COMPILE_ONLY_MODE);
			}

		}
	}

	/*
	** SYNCHRONIZATION: no synchronization
	** necessary since integer reads/writes
	** are atomic
	*/
	public int getCacheMode()
	{
		return cacheMode;
	}

	/*
	** SYNCHRONIZATION: no synchronization
	** necessary since integer reads/writes
	** are atomic
	*/
	private void setCacheMode(int newMode)
	{
		cacheMode = newMode;
	}

	/**
	 * Get a DataDescriptorGenerator, through which we can create
	 * objects to be stored in the DataDictionary.
	 *
	 * @return	A DataDescriptorGenerator
	 */

	public DataDescriptorGenerator	getDataDescriptorGenerator()
	{
		return dataDescriptorGenerator;
	}

	/**
	 * Get a DataValueFactory, through which we can create
	 * data value objects.
	 *
	 * @return	A DataValueFactory
	 */
	public DataValueFactory	getDataValueFactory()
	{
		return dvf;
	}

	/**
	 * Get ExecutionFactory associated with this database.
	 *
	 * @return	The ExecutionFactory
	 */
	public ExecutionFactory	getExecutionFactory()
	{
		return exFactory;
	}

	/**
	 * @see DataDictionary#pushDataDictionaryContext
	 */
	public DataDictionaryContext pushDataDictionaryContext(ContextManager contextManager,
														   boolean nested)
	{
		DataDictionaryContextImpl dataDictionaryContextImpl =
			new DataDictionaryContextImpl(contextManager, this, nested);

		return dataDictionaryContextImpl;
	}


    /* We defer getting the builtin schemas (system and default) past boot time so that
     * the language connection context will be available.
     */
    private void getBuiltinSchemaNames() throws StandardException
    {
        if( builtinSchemasAreFromLCC)
            return;
        
        LanguageConnectionContext lcc = getLCC();
        if( null == lcc)
        {
            systemSchemaName        = SchemaDescriptor.STD_SYSTEM_SCHEMA_NAME;
            sysIBMSchemaName        = SchemaDescriptor.IBM_SYSTEM_SCHEMA_NAME;

            systemDiagSchemaName    = 
                SchemaDescriptor.STD_SYSTEM_DIAG_SCHEMA_NAME;
            systemUtilSchemaName    = 
                SchemaDescriptor.STD_SYSTEM_UTIL_SCHEMA_NAME;
            declaredGlobalTemporaryTablesSchemaName = 
                SchemaDescriptor.STD_DECLARED_GLOBAL_TEMPORARY_TABLES_SCHEMA_NAME;
        }
        else
        {
            systemSchemaName        = lcc.getSystemSchemaName();
            sysIBMSchemaName        = lcc.getSysIBMSchemaName();
            systemDiagSchemaName    = lcc.getSystemDiagSchemaName();
            systemUtilSchemaName    = lcc.getSystemUtilSchemaName();
            declaredGlobalTemporaryTablesSchemaName = 
                lcc.getDeclaredGlobalTemporaryTablesSchemaName();

            builtinSchemasAreFromLCC = true;
        }
    }

    private void getBuiltinSchemas() throws StandardException
    {
        if( builtinSchemasAreFromLCC
            && null != systemSchemaDesc
            && null != sysIBMSchemaDesc
            && null != systemDiagSchemaDesc
            && null != systemUtilSchemaDesc
            && null != declaredGlobalTemporaryTablesSchemaDesc)
            return;

        getBuiltinSchemaNames();

		systemSchemaDesc = 
            newSystemSchemaDesc(
                systemSchemaName, SchemaDescriptor.SYSTEM_SCHEMA_UUID);
		sysIBMSchemaDesc = 
            newSystemSchemaDesc(
                sysIBMSchemaName, SchemaDescriptor.SYSIBM_SCHEMA_UUID);
		systemDiagSchemaDesc  = 
            newSystemSchemaDesc(
                systemDiagSchemaName, SchemaDescriptor.SYSCS_DIAG_SCHEMA_UUID);
		systemUtilSchemaDesc  = 
            newSystemSchemaDesc(
                systemUtilSchemaName, SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID);

		declaredGlobalTemporaryTablesSchemaDesc = 
            newDeclaredGlobalTemporaryTablesSchemaDesc(
                declaredGlobalTemporaryTablesSchemaName);
    }

	/**
	 * Get the descriptor for the system schema. Schema descriptors include 
     * authorization ids and schema ids.
     *
	 * SQL92 allows a schema to specify a default character set - we will
	 * not support this.
	 *
	 * @return	The descriptor for the schema.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public SchemaDescriptor	getSystemSchemaDescriptor()
						throws StandardException
    {
        getBuiltinSchemas();
        return systemSchemaDesc;
    }


	/**
	 * Get the descriptor for the SYSCS_UTIL system schema. 
     * Schema descriptors include authorization ids and schema ids.
     *
	 * SQL92 allows a schema to specify a default character set - we will
	 * not support this.
	 *
	 * @return	The descriptor for the schema.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public SchemaDescriptor	getSystemUtilSchemaDescriptor()
						throws StandardException
    {
        getBuiltinSchemas();
        return(systemUtilSchemaDesc);
    }

	/**
	 * Get the descriptor for the SYSCS_DIAG system schema. 
     * Schema descriptors include authorization ids and schema ids.
     *
	 * SQL92 allows a schema to specify a default character set - we will
	 * not support this.
	 *
	 * @return	The descriptor for the schema.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public SchemaDescriptor	getSystemDiagSchemaDescriptor()
						throws StandardException
    {
        getBuiltinSchemas();
        return(systemDiagSchemaDesc);
    }

	/**
	 * Get the descriptor for the SYSIBM schema. Schema descriptors include 
     * authorization ids and schema ids.
     *
	 * SQL92 allows a schema to specify a default character set - we will
	 * not support this.
	 *
	 * @return	The descriptor for the schema.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public SchemaDescriptor	getSysIBMSchemaDescriptor()
						throws StandardException
    {
        getBuiltinSchemas();
        return sysIBMSchemaDesc;
    }

	/**
	 * Get the descriptor for the declared global temporary table schema which 
     * is always named "SESSION".
	 *
	 * @return	The descriptor for the schema.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public SchemaDescriptor	getDeclaredGlobalTemporaryTablesSchemaDescriptor()
        throws StandardException
    {
        getBuiltinSchemas();
        return declaredGlobalTemporaryTablesSchemaDesc;
    }  
    
    /**
     * Determine whether a string is the name of the system schema.
     *
     * @param name
     * @return	true or false
	 *
	 * @exception StandardException		Thrown on failure
     */
    public boolean isSystemSchemaName( String name)
        throws StandardException
    {
        getBuiltinSchemaNames();

        boolean ret_val = false;

        for (int i = systemSchemaNames.length - 1; i >= 0;)
        {
            if ((ret_val = systemSchemaNames[i--].equals(name)))
                break;
        }
            
        return(ret_val);
    }

	/**
	 * Get the descriptor for the named schema.
	 * Schema descriptors include authorization ids and schema ids.
	 * SQL92 allows a schema to specify a default character set - we will
	 * not support this.  Will check default schema for a match
	 * before scanning a system table.
	 * 
	 * @param schemaName	The name of the schema we're interested in. Must not be null.
	 * @param tc			TransactionController
	 *
	 * @param raiseError    whether an exception should be thrown if the schema does not exist.
	 *
	 * @return	The descriptor for the schema. Can be null (not found) if raiseError is false.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public SchemaDescriptor	getSchemaDescriptor(String schemaName,
												TransactionController tc,
												boolean raiseError)
		throws StandardException
	{
		/*
		** Check for APP and SYS schemas before going any
		** further.
		*/
	
		if ( tc == null )
		{
		    tc = getTransactionCompile();
        }

		if (getSystemSchemaDescriptor().getSchemaName().equals(schemaName))
		{
			return getSystemSchemaDescriptor();
		}
		else if (getSysIBMSchemaDescriptor().getSchemaName().equals(schemaName))
		{
			// oh you are really asking SYSIBM, if this db is soft upgraded 
            // from pre 52, I may have 2 versions for you, one on disk 
            // (user SYSIBM), one imaginary (builtin). The
			// one on disk (real one, if it exists), should always be used.
			if (dictionaryVersion.checkVersion(
                        DataDictionary.DD_VERSION_CS_5_2, null))
            {
				return getSysIBMSchemaDescriptor();
            }
		}

		/*
		** Manual lookup
		*/
		SchemaDescriptor sd = locateSchemaRow(schemaName, tc);

		//if no schema found and schema name is SESSION, then create an 
        //in-memory schema descriptor
		if (sd == null && 
            getDeclaredGlobalTemporaryTablesSchemaDescriptor().getSchemaName().equals(schemaName))
        {
			return getDeclaredGlobalTemporaryTablesSchemaDescriptor();
        }

		if (sd == null && raiseError)
		{
			throw StandardException.newException(
                    SQLState.LANG_SCHEMA_DOES_NOT_EXIST, schemaName);
		}
		else
        {
			return sd;
        }
	}
		
	/**
	 * Get the target schema by searching for a matching row
	 * in SYSSCHEMAS by schemaId.  Read only scan.
	 * 
	 * @param schemaId		The id of the schema we're interested in.
	 *						If non-null, overrides schemaName
	 * 
	 * @param tc			TransactionController.  If null, one
	 *						is gotten off of the language connection context.
	 *
	 * @return	The row for the schema
	 *
	 * @exception StandardException		Thrown on error
	 */
	private SchemaDescriptor locateSchemaRow(UUID schemaId,
								TransactionController tc)
		throws StandardException
	{
		DataValueDescriptor		UUIDStringOrderable;
		TabInfo					ti = coreInfo[SYSSCHEMAS_CORE_NUM];

		/* Use UUIDStringOrderable in both start and stop positions for scan */
		UUIDStringOrderable = dvf.getCharDataValue(schemaId.toString());

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = exFactory.getIndexableRow(1);
		keyRow.setColumn(1, UUIDStringOrderable);

		return (SchemaDescriptor)
					getDescriptorViaIndex(
						SYSSCHEMASRowFactory.SYSSCHEMAS_INDEX2_ID,
						keyRow,
						(ScanQualifier [][]) null,
						ti,
						(TupleDescriptor) null,
						(List) null,
						false);
	}
		
	/**
	 * Get the target schema by searching for a matching row
	 * in SYSSCHEMAS by schema name.  Read only scan.
	 * 
	 * @param schemaName	The name of the schema we're interested in.
	 *						If schemaId is null, used to qual.
	 *
	 * @param tc			TransactionController.  If null, one
	 *						is gotten off of the language connection context.
	 *
	 * @return	The row for the schema
	 *
	 * @exception StandardException		Thrown on error
	 */
	private SchemaDescriptor locateSchemaRow(String schemaName, 
								TransactionController tc)
		throws StandardException
	{
		DataValueDescriptor		  schemaNameOrderable;
		TabInfo					  ti = coreInfo[SYSSCHEMAS_CORE_NUM];

		/* Use aliasNameOrderable in both start 
		 * and stop position for scan. 
		 */
		schemaNameOrderable = dvf.getVarcharDataValue(schemaName);

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = exFactory.getIndexableRow(1);
		keyRow.setColumn(1, schemaNameOrderable);

		return (SchemaDescriptor)
					getDescriptorViaIndex(
						SYSSCHEMASRowFactory.SYSSCHEMAS_INDEX1_ID,
						keyRow,
						(ScanQualifier [][]) null,
						ti,
						(TupleDescriptor) null,
						(List) null,
						false);
	}

	/**
	 * Get the descriptor for the named schema. If the schemaId
	 * parameter is NULL, it gets the descriptor for the current (default)
	 * schema. Schema descriptors include authorization ids and schema ids.
	 * SQL92 allows a schema to specify a default character set - we will
	 * not support this.  Will check default schema for a match
	 * before scanning a system table.
	 * 
	 * @param schemaId	The id of the schema we're interested in.
	 *			If the name is NULL, get the descriptor for the
	 *			current schema.
	 * @param tc			TransactionController
	 *
	 *
	 * @return	The descriptor for the schema.  <I> Warning: <\I> may
	 * 			return NULL if schemaName is non-NULL and doesn't exist
	 *			in SYSSCHEMAS
	 *
	 * @exception StandardException		Thrown on error
	 */
	public SchemaDescriptor	getSchemaDescriptor(UUID schemaId,
									TransactionController tc)
		throws StandardException
	{
		SchemaDescriptor		sd = null;
		
		if ( tc == null )
		{
		    tc = getTransactionCompile();
        }

		/*
		** Check for APP and SYS schemas before going any
		** further.
		*/
		if (schemaId != null)
		{
			if (getSystemSchemaDescriptor().getUUID().equals(schemaId))
			{
				return getSystemSchemaDescriptor();
			}
			else if (getSysIBMSchemaDescriptor().getUUID().equals(schemaId))
			{
				return getSysIBMSchemaDescriptor();
			}
		}

		/*
		** If we aren't booting, lets see if we already
		** have the descriptor.  If we are in the middle
		** of booting we cannot get the LanguageConnectionContext.
		*/
		if (!booting)
		{

			LanguageConnectionContext	lcc = getLCC();

			if (lcc != null)
			{
				sd = lcc.getDefaultSchema();

				if ((sd != null) &&
						((schemaId == null) ||
							schemaId.equals(sd.getUUID())))
				{
					return sd;
				}
			}
		}

		return locateSchemaRow(schemaId, tc);
	}

	/** 
	 * @see DataDictionary#addDescriptor
	 */
	public void addDescriptor(TupleDescriptor td, TupleDescriptor parent,
							  int catalogNumber, boolean duplicatesAllowed,
							  TransactionController tc)
		throws StandardException
	{
		addDescriptorNow(td, parent, catalogNumber, duplicatesAllowed, tc, true);
	}

	private void addDescriptorNow(TupleDescriptor td, TupleDescriptor parent,
							  int catalogNumber, boolean duplicatesAllowed,
							  TransactionController tc, boolean wait)
		throws StandardException
	{
		TabInfo ti =  (catalogNumber < NUM_CORE) ? coreInfo[catalogNumber] :
			getNonCoreTI(catalogNumber);

		ExecRow row = ti.getCatalogRowFactory().makeRow(td, parent);

		int insertRetCode = ti.insertRow(row, tc, wait);

		if (!duplicatesAllowed)
		{
			if (insertRetCode != TabInfo.ROWNOTDUPLICATE)
				throw duplicateDescriptorException(td, parent);
		}	
	}

	private StandardException 
		duplicateDescriptorException(TupleDescriptor tuple,
									 TupleDescriptor parent)
	{
		if (parent != null)
			return
				StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT,
											   tuple.getDescriptorType(),
											   tuple.getDescriptorName(),
											   parent.getDescriptorType(),
											   parent.getDescriptorName());

		else return
				 StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS,
												tuple.getDescriptorType(),
												tuple.getDescriptorName());
	}

	/** array version of addDescriptor.
	 * @see DataDictionary#addDescriptor
	 */
	public void addDescriptorArray(TupleDescriptor[] td, 
								   TupleDescriptor  parent,
								   int catalogNumber,
								   boolean allowDuplicates,
								   TransactionController tc)
		throws StandardException
	{
		TabInfo ti =  (catalogNumber < NUM_CORE) ? coreInfo[catalogNumber] :
			getNonCoreTI(catalogNumber);
		CatalogRowFactory crf = ti.getCatalogRowFactory();
		RowList rl = new RowList();

		for (int index = 0; index < td.length; index++)
		{
			ExecRow row = crf.makeRow(td[index], parent);
			rl.add(row);
		}

		int insertRetCode = ti.insertRowList( rl, tc );
		if (!allowDuplicates && insertRetCode != TabInfo.ROWNOTDUPLICATE)
		{
			throw duplicateDescriptorException(td[insertRetCode], parent);
		}
	}

	/**
	 * Drop the descriptor for a schema, given the schema's name
	 *
	 * @param schemaName	The name of the schema to drop
	 * @param tc			TransactionController for the transaction
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	dropSchemaDescriptor(String schemaName,
							TransactionController tc)
		throws StandardException
	{
		ExecIndexRow			keyRow1 = null;
		DataValueDescriptor		schemaNameOrderable;
		TabInfo					ti = coreInfo[SYSSCHEMAS_CORE_NUM];

		if (SanityManager.DEBUG)
		{
			SchemaDescriptor sd = getSchemaDescriptor(schemaName, getTransactionCompile(), true);
			if (!isSchemaEmpty(sd))
			{
				SanityManager.THROWASSERT("Attempt to drop schema "+schemaName+" that is not empty");
			}
		}

		/* Use schemaNameOrderable in both start 
		 * and stop position for index 1 scan. 
		 */
		schemaNameOrderable = dvf.getVarcharDataValue(schemaName);

		/* Set up the start/stop position for the scan */
		keyRow1 = exFactory.getIndexableRow(1);
		keyRow1.setColumn(1, schemaNameOrderable);

		ti.deleteRow( tc, keyRow1, SYSSCHEMASRowFactory.SYSSCHEMAS_INDEX1_ID );
	}

	/**
	 * Get the descriptor for the named table within the given schema.
	 * If the schema parameter is NULL, it looks for the table in the
	 * current (default) schema. Table descriptors include object ids,
	 * object types (table, view, etc.)
	 *
	 * @param tableName	The name of the table to get the descriptor for
	 * @param schema	The descriptor for the schema the table lives in.
	 *			If null, use the system schema.
	 *
	 * @return	The descriptor for the table, null if table does not
	 *		exist.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public TableDescriptor		getTableDescriptor(String tableName,
					SchemaDescriptor schema)
			throws StandardException
	{
		TableDescriptor		retval = null;

		/*
		** If we didn't get a schema descriptor, we had better
		** have a system table.
		*/
		if (SanityManager.DEBUG)
		{
			if ((schema == null) && !tableName.startsWith("SYS"))
			{
				SanityManager.THROWASSERT("null schema for non system table "+tableName);
			}
		}

		SchemaDescriptor sd = (schema == null) ?
				getSystemSchemaDescriptor()
				: schema;

		UUID schemaUUID = sd.getUUID();
		TableKey tableKey = 	new TableKey(schemaUUID, tableName);

		/* Only use the cache if we're in compile-only mode */
		if (getCacheMode() == DataDictionary.COMPILE_ONLY_MODE)
		{
			NameTDCacheable cacheEntry = (NameTDCacheable) nameTdCache.find(tableKey);
			if (cacheEntry != null)
			{
				retval = cacheEntry.getTableDescriptor();
				// bind in previous command might have set refernced cols
				retval.setReferencedColumnMap(null);
				nameTdCache.release(cacheEntry);
			}
			return retval;
		}

		return getTableDescriptorIndex1Scan(tableName, schemaUUID.toString());

	}

	/**
	 * Scan systables_index1 (tablename, schemaid) for a match.
	 *
	 * @return TableDescriptor	The matching descriptor, if any.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	private TableDescriptor getTableDescriptorIndex1Scan(
										String tableName, 
										String schemaUUID)
				throws StandardException
	{
		DataValueDescriptor		  schemaIDOrderable;
		DataValueDescriptor		  tableNameOrderable;
		TableDescriptor			  td;
		TabInfo					  ti = coreInfo[SYSTABLES_CORE_NUM];

		/* Use tableNameOrderable and schemaIdOrderable in both start 
		 * and stop position for scan. 
		 */
		tableNameOrderable = dvf.getVarcharDataValue(tableName);
		schemaIDOrderable = dvf.getCharDataValue(schemaUUID);

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = exFactory.getIndexableRow(2);
		keyRow.setColumn(1, tableNameOrderable);
		keyRow.setColumn(2, schemaIDOrderable);

		td = (TableDescriptor)
					getDescriptorViaIndex(
						SYSTABLESRowFactory.SYSTABLES_INDEX1_ID,
						keyRow,
						(ScanQualifier [][]) null,
						ti,
						(TupleDescriptor) null,
						(List) null,
						false);

		return finishTableDescriptor(td);
	}

	/**
	 * This method can get called from the DataDictionary cache.
	 *
	 * @param tableKey	The TableKey of the table
	 *
	 * @return	The descriptor for the table, null if the table does
	 *		not exist.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	TableDescriptor getUncachedTableDescriptor(TableKey tableKey)
				throws StandardException
	{
		return getTableDescriptorIndex1Scan(tableKey.getTableName(), 
											tableKey.getSchemaId().toString());
	}

	/**
	 * Get the descriptor for the table with the given UUID.
	 *
	 * NOTE: I'm assuming that the object store will define an UUID for
	 * persistent objects. I'm also assuming that UUIDs are unique across
	 * schemas, and that the object store will be able to do efficient
	 * lookups across schemas (i.e. that no schema descriptor parameter
	 * is needed).
	 *
	 * @param tableID	The UUID of the table to get the descriptor for
	 *
	 * @return	The descriptor for the table, null if the table does
	 *		not exist.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public TableDescriptor		getTableDescriptor(UUID tableID)
			throws StandardException
	{
		OIDTDCacheable		cacheEntry;
		TableDescriptor	retval = null;

		/* Only use the cache if we're in compile-only mode */
		if (getCacheMode() == DataDictionary.COMPILE_ONLY_MODE)
		{
			cacheEntry = (OIDTDCacheable) OIDTdCache.find(tableID);
			if (cacheEntry != null)
			{
				retval = cacheEntry.getTableDescriptor();
				// bind in previous command might have set refernced cols
				retval.setReferencedColumnMap(null);
				OIDTdCache.release(cacheEntry);
			}

			return retval;

		}

		return getTableDescriptorIndex2Scan(tableID.toString());
	}

	/**
	 * This method can get called from the DataDictionary cache.
	 *
	 * @param tableID	The UUID of the table to get the descriptor for
	 *
	 * @return	The descriptor for the table, null if the table does
	 *		not exist.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	protected TableDescriptor	getUncachedTableDescriptor(UUID tableID)
				throws StandardException
	{
		return getTableDescriptorIndex2Scan(tableID.toString());
	}

	/**
	 * Scan systables_index2 (tableid) for a match.
	 *
	 * @return TableDescriptor	The matching descriptor, if any.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	private TableDescriptor getTableDescriptorIndex2Scan(
										String tableUUID)
				throws StandardException
	{
		DataValueDescriptor		  tableIDOrderable;
		TableDescriptor			  td;
		TabInfo					  ti = coreInfo[SYSTABLES_CORE_NUM];

		/* Use tableNameOrderable and schemaIdOrderable in both start 
		 * and stop position for scan. 
		 */
		tableIDOrderable = dvf.getCharDataValue(tableUUID);

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = exFactory.getIndexableRow(1);
		keyRow.setColumn(1, tableIDOrderable);

		td = (TableDescriptor)
					getDescriptorViaIndex(
						SYSTABLESRowFactory.SYSTABLES_INDEX2_ID,
						keyRow,
						(ScanQualifier [][]) null,
						ti,
						(TupleDescriptor) null,
						(List) null,
						false);

		return finishTableDescriptor(td);
	}

	/**
	 * Finish filling in the TableDescriptor.
	 * (Build the various lists that hang off the TD.)
	 *
	 * @param td	The TableDescriptor.
	 *
	 * @return The completed TableDescriptor.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	private TableDescriptor finishTableDescriptor(TableDescriptor td)
		throws StandardException
	{

		if (td != null)
		{
			synchronized(td)
			{
				getColumnDescriptorsScan(td);
				getConglomerateDescriptorsScan(td);
			}
		}

		return td;
	}

	/**
	 * Indicate whether there is anything in the 
	 * particular schema.  Checks for tables in the
	 * the schema, on the assumption that there cannot
	 * be any other objects in a schema w/o a table.
	 *
	 * @param schema descriptor
	 *
	 * @return true/false
	 *
	 * @exception StandardException on error
	 */
	public boolean isSchemaEmpty(SchemaDescriptor sd) 
		throws StandardException
	{
		DataValueDescriptor     schemaIdOrderable;
		TransactionController	tc = getTransactionCompile();

		schemaIdOrderable = getValueAsDVD(sd.getUUID());

		if (isSchemaReferenced(tc, coreInfo[SYSTABLES_CORE_NUM],
					SYSTABLESRowFactory.SYSTABLES_INDEX1_ID,
					SYSTABLESRowFactory.SYSTABLES_INDEX1_SCHEMAID,
					schemaIdOrderable))
		{
			return false;
		}
	
		if (isSchemaReferenced(tc, getNonCoreTI(SYSCONSTRAINTS_CATALOG_NUM),
					SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_INDEX2_ID,
					2,
					schemaIdOrderable))
		{
			return false;
		}

		if (isSchemaReferenced(tc, getNonCoreTI(SYSSTATEMENTS_CATALOG_NUM),
					SYSSTATEMENTSRowFactory.SYSSTATEMENTS_INDEX2_ID,
					2,
					schemaIdOrderable))
		{
			return false;
		}

		if (isSchemaReferenced(tc, getNonCoreTI(SYSTRIGGERS_CATALOG_NUM),
					SYSTRIGGERSRowFactory.SYSTRIGGERS_INDEX2_ID,
					2,
					schemaIdOrderable))
		{
			return false;
		}

		return true;
	}

	/**
 	 * Is the schema id referenced by the system table in question?
	 * Currently assumes that the schema id is in an index.
	 * NOTE: could be generalized a bit, and possibly used
	 * elsewhere...
	 *
	 * @param tc	transaction controller
	 * @param ti	table info for the system table
	 * @param indexId	index id
	 * @param indexCol	1 based index column
	 * @param schemaIdOrderable	the schemaid in a char orderable
 	 *
	 * @return true if there is a reference to this schema
	 *
	 * @exception StandardException on error
	 */
	protected boolean isSchemaReferenced(TransactionController	tc, 
						TabInfo					ti, 
						int						indexId, 
						int						indexCol, 
						DataValueDescriptor		schemaIdOrderable )
		throws StandardException
	{
		ConglomerateController	heapCC = null;
		ExecIndexRow	  		indexRow1;
		ExecIndexRow			indexTemplateRow;
		ExecRow 				outRow;
		ScanController			scanController = null;
		boolean					foundRow;
		FormatableBitSet					colToCheck = new FormatableBitSet(indexCol);
		CatalogRowFactory		rf = ti.getCatalogRowFactory();	

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(indexId >= 0, "code needs to be enhanced"+
				" to support a table scan to find the index id");
		}

		colToCheck.set(indexCol - 1);

		ScanQualifier[][] qualifier = exFactory.getScanQualifier(1);
		qualifier[0][0].setQualifier
				(indexCol - 1,
				 schemaIdOrderable,
				 Orderable.ORDER_OP_EQUALS,
				 false,
				 false,
				 false);

		outRow = rf.makeEmptyRow();

		try
		{
			heapCC = 
	            tc.openConglomerate(
	                ti.getHeapConglomerate(), false, 0, 
                    TransactionController.MODE_RECORD,
                    TransactionController.ISOLATION_REPEATABLE_READ);
	
			scanController = tc.openScan(
					ti.getIndexConglomerate(indexId),	// conglomerate to open
					false, 								// don't hold open across commit
					0,                                  // for read
	                TransactionController.MODE_RECORD,	// row locking
                    TransactionController.ISOLATION_REPEATABLE_READ,
					colToCheck, 						// don't get any rows
					null,   							// start position - first row
					ScanController.GE,      			// startSearchOperation
					qualifier, 							// scanQualifier,
					null,   							// stop position - through last row
					ScanController.GT);     			// stopSearchOperation
	
			foundRow = (scanController.next());
		}
		finally
		{
			if (scanController != null)	
			{
				scanController.close();
			}
			if (heapCC != null)
			{
				heapCC.close();
			}
		}
		
		return foundRow;
	}

	/**
	 * Drop the table descriptor.
	 *
	 * @param descriptor	The table descriptor to drop
	 * @param schema		A descriptor for the schema the table
	 *						is a part of.  If this parameter is
	 *						NULL, then the table is part of the
	 *						current (default) schema
	 * @param tc			TransactionController for the transaction
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	dropTableDescriptor(TableDescriptor td, SchemaDescriptor schema,
									TransactionController tc)
		throws StandardException
	{
		ConglomerateController	heapCC;
		ExecIndexRow			keyRow1 = null;
		DataValueDescriptor		schemaIDOrderable;
		DataValueDescriptor		tableNameOrderable;
		TabInfo					ti = coreInfo[SYSTABLES_CORE_NUM];

		/* Use tableIdOrderable and schemaIdOrderable in both start 
		 * and stop position for index 1 scan. 
		 */
		tableNameOrderable = dvf.getVarcharDataValue(td.getName());
		schemaIDOrderable = getValueAsDVD(schema.getUUID());

		/* Set up the start/stop position for the scan */
		keyRow1 = (ExecIndexRow) exFactory.getIndexableRow(2);
		keyRow1.setColumn(1, tableNameOrderable);
		keyRow1.setColumn(2, schemaIDOrderable);

		ti.deleteRow( tc, keyRow1, SYSTABLESRowFactory.SYSTABLES_INDEX1_ID );
	}

	/**
	 * Update the lockGranularity for the specified table.
	 *
	 * @param td				The TableDescriptor for the table
	 * @param schema			The SchemaDescriptor for the table
	 * @param lockGranularity	The new lockGranularity
	 * @param tc				The TransactionController to use.
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void updateLockGranularity(TableDescriptor td, SchemaDescriptor schema,
									  char lockGranularity, TransactionController tc)
		throws StandardException
	{
		ConglomerateController	heapCC;
		ExecIndexRow			keyRow1 = null;
		ExecRow    				row;
		DataValueDescriptor		schemaIDOrderable;
		DataValueDescriptor		tableNameOrderable;
		TabInfo					ti = coreInfo[SYSTABLES_CORE_NUM];
		SYSTABLESRowFactory  rf = (SYSTABLESRowFactory) ti.getCatalogRowFactory();

		/* Use tableIdOrderable and schemaIdOrderable in both start 
		 * and stop position for index 1 scan. 
		 */
		tableNameOrderable = dvf.getVarcharDataValue(td.getName());
		schemaIDOrderable = getValueAsDVD(schema.getUUID());

		/* Set up the start/stop position for the scan */
		keyRow1 = (ExecIndexRow) exFactory.getIndexableRow(2);
		keyRow1.setColumn(1, tableNameOrderable);
		keyRow1.setColumn(2, schemaIDOrderable);

		// build the row to be stuffed into SYSTABLES. 
		row = rf.makeRow(td, schema);
		// update row in catalog (no indexes)
		boolean[] bArray = new boolean[2];
		for (int index = 0; index < 2; index++)
		{
			bArray[index] = false;
		}
		ti.updateRow(keyRow1, row, 
					 SYSTABLESRowFactory.SYSTABLES_INDEX1_ID,
					 bArray,
					 (int[])null,
					 tc);
	}

	/**
	 * Drop all table descriptors for a schema.
	 *
	 * @param schema	A descriptor for the schema to drop the tables
	 *			from.
	 *
	 * @return  Nothing.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	/*
	public void dropAllTableDescriptors(SchemaDescriptor schema)
						throws StandardException
	{
		if (SanityManager.DEBUG) SanityManager.NOTREACHED();
	}
	*/

	/**
	 * Get a ColumnDescriptor given its Default ID.
	 *
	 * @param uuid	The UUID of the default
	 *
	 * @return The ColumnDescriptor for the column.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ColumnDescriptor getColumnDescriptorByDefaultId(UUID uuid)
				throws StandardException
	{
		DataValueDescriptor	UUIDStringOrderable;
		TabInfo				ti = coreInfo[SYSCOLUMNS_CORE_NUM];

		/* Use UUIDStringOrderable in both start and stop positions for scan */
		UUIDStringOrderable = dvf.getCharDataValue(uuid.toString());

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = exFactory.getIndexableRow(1);
		keyRow.setColumn(1, UUIDStringOrderable);

		return (ColumnDescriptor)
					getDescriptorViaIndex(
						SYSCOLUMNSRowFactory.SYSCOLUMNS_INDEX2_ID,
						keyRow,
						(ScanQualifier [][]) null,
						ti,
						(DefaultDescriptor) null,
						(List) null,
						false);
	}


	/** 
	 * Populate the ColumnDescriptorList for the specified TableDescriptor.
	 *
	 * MT synchronization: it is assumed that the caller has synchronized
	 * on the CDL in the given TD.
	 *
	 * @param td				The TableDescriptor.
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	private void getColumnDescriptorsScan(TableDescriptor td)
			throws StandardException
	{
		getColumnDescriptorsScan(
						td.getUUID(),
						td.getColumnDescriptorList(),
						td);
	}

	/** 
	 * Populate the ColumnDescriptorList for the specified TableDescriptor.
	 *
	 * MT synchronization: it is assumed that the caller has synchronized
	 * on the CDL in the given TD.
	 *
	 * @param uuid				The referencing UUID
	 * @param cdlist			The column descriptor list
	 * @param td				The parent tuple descriptor
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	 private void getColumnDescriptorsScan(
											UUID 					uuid, 
											ColumnDescriptorList 	cdl,
											TupleDescriptor 		td)
			throws StandardException
	{
		ColumnDescriptor		cd;
		ColumnDescriptorList    cdlCopy         = new ColumnDescriptorList();
		DataValueDescriptor		refIDOrderable  = null;
		TabInfo                 ti              = coreInfo[SYSCOLUMNS_CORE_NUM];

		/* Use refIDOrderable in both start and stop position for scan. */
		refIDOrderable = getValueAsDVD(uuid);

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = exFactory.getIndexableRow(1);
		keyRow.setColumn(1, refIDOrderable);

		getDescriptorViaIndex(
						SYSCOLUMNSRowFactory.SYSCOLUMNS_INDEX1_ID,
						keyRow,
						(ScanQualifier [][]) null,
						ti,
						td,
						(ColumnDescriptorList) cdl,
						false);

		/* The TableDescriptor's column descriptor list must be ordered by
		 * columnNumber.  (It is probably not ordered correctly at this point due
		 * to the index on syscolumns being on (tableId, columnName).)  The
		 * cheapest way to reorder the list appears to be to copy it (above), and then
		 * walk the copy and put the elements back into the original in the
		 * expected locations.
		 */
		int cdlSize = cdl.size();
		for (int index = 0; index < cdlSize; index++)
		{
			cdlCopy.add( cdl.get(index));
		}
		for (int index = 0; index < cdlSize; index++)
		{
			cd = (ColumnDescriptor) cdlCopy.elementAt(index);
			cdl.set(cd.getPosition() - 1, cd);
		}
	}

	/**
	 * Given a column name and a table ID, drops the column descriptor
	 * from the table.
	 *
	 * @param tableID	The UUID of the table to drop the column from
	 * @param columnName	The name of the column to drop
	 * @param tc		TransactionController for the transaction
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	dropColumnDescriptor(UUID tableID,
				String columnName, TransactionController tc)
		throws StandardException
	{
		DataValueDescriptor	columnNameOrderable;
		DataValueDescriptor	tableIdOrderable;

		/* Use tableIDOrderable and columnNameOrderable in both start 
		 * and stop position for scan. 
		 */
		tableIdOrderable = getValueAsDVD(tableID);
		columnNameOrderable = dvf.getVarcharDataValue(columnName);

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = exFactory.getIndexableRow(2);
		keyRow.setColumn(1, tableIdOrderable);
		keyRow.setColumn(2, columnNameOrderable);

		dropColumnDescriptorCore( tc, keyRow);
	}

	/**
	 * Drops all column descriptors from the given table.  Useful for
	 * DROP TABLE.
	 *
	 * @param tableID	The UUID of the table from which to drop
	 *			all the column descriptors
	 * @param tc		TransactionController for the transaction
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	dropAllColumnDescriptors(UUID tableID, TransactionController tc)
		throws StandardException
	{
		DataValueDescriptor		tableIdOrderable;

		/* Use tableIDOrderable in both start and stop position for scan. */
		tableIdOrderable = getValueAsDVD(tableID);

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = exFactory.getIndexableRow(1);
		keyRow.setColumn(1, tableIdOrderable);

		dropColumnDescriptorCore(tc, keyRow);
	}

	/**
	 * Delete the appropriate rows from syscolumns when
	 * dropping 1 or more columns.
	 * 
	 * @param tc			The TransactionController
	 * @param keyRow		Start/stop position.
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	private void dropColumnDescriptorCore(
					TransactionController tc,
					ExecIndexRow keyRow)
			throws StandardException
	{
		TabInfo				   ti = coreInfo[SYSCOLUMNS_CORE_NUM];

		ti.deleteRow( tc, keyRow, SYSCOLUMNSRowFactory.SYSCOLUMNS_INDEX1_ID );
	}

	/**
	 * Update the column descriptor in question.  Updates
	 * every row in the base conglomerate.  
	 *
	 * @param cd					The ColumnDescriptor
	 * @param formerUUID			The UUID for this column in SYSCOLUMNS,
	 *								may differ from what is in cd if this
	 *								is the column that is being set.
	 * @param formerName			The name for this column in SYSCOLUMNS
	 *								may differ from what is in cd if this
	 *								is the column that is being set.
	 * @param colsToSet 			Array of ints of columns to be modified,
	 *								1 based.  May be null (all cols).
	 * @param tc					The TransactionController to use
	 * @param wait		If true, then the caller wants to wait for locks. False will be
	 * when we using a nested user xaction - we want to timeout right away if the parent
	 * holds the lock.  (bug 4821)
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	private void updateColumnDescriptor(ColumnDescriptor cd,
										UUID		formerUUID,
										String		formerName,
										int[]		colsToSet,
										TransactionController tc,
										boolean wait)
		throws StandardException
	{
		ExecIndexRow				keyRow1 = null;
		ExecRow    					row;
		DataValueDescriptor			refIDOrderable;
		DataValueDescriptor			columnNameOrderable;
		TabInfo						ti = coreInfo[SYSCOLUMNS_CORE_NUM];
		SYSCOLUMNSRowFactory  rf = (SYSCOLUMNSRowFactory) ti.getCatalogRowFactory();

		/* Use objectID/columnName in both start 
		 * and stop position for index 1 scan. 
		 */
		refIDOrderable = getValueAsDVD(formerUUID);
		columnNameOrderable = dvf.getVarcharDataValue(formerName);

		/* Set up the start/stop position for the scan */
		keyRow1 = (ExecIndexRow) exFactory.getIndexableRow(2);
		keyRow1.setColumn(1, refIDOrderable);
		keyRow1.setColumn(2, columnNameOrderable);

		// build the row to be stuffed into SYSCOLUMNS. 
		row = rf.makeRow(cd, null);

		/*
		** Figure out if the index in syscolumns needs 
		** to be updated. 
		*/
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(rf.getNumIndexes() == 2, 
					"There are more indexes on syscolumns than expected, the code herein needs to change");
		}

		boolean[] bArray = new boolean[rf.getNumIndexes()];

		/*
		** Do we need to update indexes?
		*/
		if (colsToSet == null)
		{
			bArray[0] = true;
			bArray[1] = true;
		}
		else
		{
			/*
			** Check the specific columns for indexed
			** columns.
			*/
			for (int i = 0; i < colsToSet.length; i++)
			{
				if ((colsToSet[i] == rf.SYSCOLUMNS_COLUMNNAME) ||
					(colsToSet[i] == rf.SYSCOLUMNS_REFERENCEID))
				{
					bArray[0] = true;
					break;
				}
				else if (colsToSet[i] == rf.SYSCOLUMNS_COLUMNDEFAULTID)
				{
					bArray[1] = true;
					break;
				}
			}
		}

		ti.updateRow(keyRow1, row, 
					 SYSCOLUMNSRowFactory.SYSCOLUMNS_INDEX1_ID,
					 bArray,
					 colsToSet,
					 tc,
					 wait);
	}

	/**
	 * Gets the viewDescriptor for the view with the given UUID.
	 *
	 * @param uuid	The UUID for the view
	 *
	 * @return  A descriptor for the view
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ViewDescriptor	getViewDescriptor(UUID uuid)
		throws StandardException
	{
		return getViewDescriptor(getTableDescriptor(uuid));
	}

	/**
	 * Gets the viewDescriptor for the view given the TableDescriptor.
	 *
	 * @param td	The TableDescriptor for the view.
	 *
	 * @return	A descriptor for the view
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ViewDescriptor	getViewDescriptor(TableDescriptor td)
		throws StandardException
	{
		TableDescriptor	tdi = (TableDescriptor) td;

		/* See if the view info is cached */
		if (tdi.getViewDescriptor() != null)
		{
			return tdi.getViewDescriptor();
		}

		synchronized(tdi)
		{
			/* See if we were waiting on someone who just filled it in */
			if (tdi.getViewDescriptor() != null)
			{
				return tdi.getViewDescriptor();
			}

			tdi.setViewDescriptor((ViewDescriptor) getViewDescriptorScan(tdi));
		}
		return tdi.getViewDescriptor();
	}

	/**
	 * Get the information for the view from sys.sysviews.
	 *
	 * @param tdi					The TableDescriptor for the view.
	 *
	 * @return ViewDescriptor	The ViewDescriptor for the view.
	 *
	 * @exception StandardException		Thrown on error
	 */
	private ViewDescriptor getViewDescriptorScan(TableDescriptor tdi)
		throws StandardException
	{
		ViewDescriptor		  vd;
		DataValueDescriptor	  viewIdOrderable;
		TabInfo				  ti = getNonCoreTI(SYSVIEWS_CATALOG_NUM);
		UUID				  viewID = tdi.getUUID();

		/* Use viewIdOrderable in both start 
		 * and stop position for scan. 
		 */
		viewIdOrderable = dvf.getCharDataValue(viewID.toString());

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = exFactory.getIndexableRow(1);
		keyRow.setColumn(1, viewIdOrderable);

		vd = (ViewDescriptor)
					getDescriptorViaIndex(
						SYSVIEWSRowFactory.SYSVIEWS_INDEX1_ID,
						keyRow,
						(ScanQualifier [][]) null,
						ti,
						(TupleDescriptor) null,
						(List) null,
						false);

		if (vd != null)
		{
			vd.setViewName(tdi.getName());
		}
		return vd;
	}

	/**
	 * Drops the view descriptor from the data dictionary.
	 *
	 * @param vd		A descriptor for the view to be dropped
	 * @param tc		TransactionController to use
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	dropViewDescriptor(ViewDescriptor vd,
								   TransactionController tc)
		throws StandardException
	{
		DataValueDescriptor		viewIdOrderable;
		TabInfo					ti = getNonCoreTI(SYSVIEWS_CATALOG_NUM);

		/* Use aliasNameOrderable in both start 
		 * and stop position for scan. 
		 */
		viewIdOrderable = getValueAsDVD(vd.getUUID());

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = (ExecIndexRow) exFactory.getIndexableRow(1);
		keyRow.setColumn(1, viewIdOrderable);

		ti.deleteRow( tc, keyRow, SYSVIEWSRowFactory.SYSVIEWS_INDEX1_ID );

	}

	/**
	 * Scan sysfiles_index2 (id) for a match.
	 * @return TableDescriptor	The matching descriptor, or null.
	 * @exception StandardException		Thrown on failure
	 */
	private FileInfoDescriptor
	getFileInfoDescriptorIndex2Scan(UUID id)
				throws StandardException
	{
		DataValueDescriptor		  idOrderable;
		TabInfo					  ti = getNonCoreTI(SYSFILES_CATALOG_NUM);
		idOrderable = dvf.getCharDataValue(id.toString());

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = exFactory.getIndexableRow(1);
		keyRow.setColumn(1, idOrderable);

		return (FileInfoDescriptor)
					getDescriptorViaIndex(
						SYSFILESRowFactory.SYSFILES_INDEX2_ID,
						keyRow,
						(ScanQualifier [][]) null,
						ti,
						(TupleDescriptor) null,
						(List) null,
						false);
	}

	/**
	 * @see DataDictionary#getFileInfoDescriptor
	 * @exception StandardException		Thrown on failure
	 */
	public FileInfoDescriptor getFileInfoDescriptor(UUID id)
				throws StandardException
	{
		return getFileInfoDescriptorIndex2Scan(id);
	}


	/**
	 * Scan sysfiles_index1 (schemaid,name) for a match.
	 * @return The matching descriptor or null.
	 * @exception StandardException		Thrown on failure
	 */
	private FileInfoDescriptor getFileInfoDescriptorIndex1Scan(
										UUID schemaId,
										String name)
				throws StandardException
	{
		DataValueDescriptor		  schemaIDOrderable;
		DataValueDescriptor		  nameOrderable;
		TabInfo					  ti = getNonCoreTI(SYSFILES_CATALOG_NUM);

		nameOrderable = dvf.getVarcharDataValue(name);
		schemaIDOrderable = dvf.getCharDataValue(schemaId.toString());

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = exFactory.getIndexableRow(2);
		keyRow.setColumn(1, nameOrderable);
		keyRow.setColumn(2, schemaIDOrderable);
		FileInfoDescriptor r = (FileInfoDescriptor)
					getDescriptorViaIndex(
						SYSFILESRowFactory.SYSFILES_INDEX1_ID,
						keyRow,
						(ScanQualifier [][]) null,
						ti,
						(TupleDescriptor) null,
						(List) null,
						false);
        return r;
	}

	/**
	 * @see DataDictionary#getFileInfoDescriptor
	 * @exception StandardException		Thrown on failure
	 */
	public FileInfoDescriptor getFileInfoDescriptor(SchemaDescriptor sd, String name)
				throws StandardException
	{
		return getFileInfoDescriptorIndex1Scan(sd.getUUID(),name);
	}

	/**
	 * @see DataDictionary#dropFileInfoDescriptor
	 * @exception StandardException		Thrown on error
	 */
	public void	dropFileInfoDescriptor(FileInfoDescriptor fid)
						throws StandardException
	{
		ConglomerateController	heapCC;
		ExecIndexRow			keyRow1 = null;
		DataValueDescriptor		idOrderable;
		TabInfo					ti = getNonCoreTI(SYSFILES_CATALOG_NUM);
		TransactionController   tc = getTransactionExecute();
		
		/* Use tableIdOrderable and schemaIdOrderable in both start 
		 * and stop position for index 1 scan. 
		 */
		idOrderable = getValueAsDVD(fid.getUUID());

		/* Set up the start/stop position for the scan */
		keyRow1 = (ExecIndexRow) exFactory.getIndexableRow(1);
		keyRow1.setColumn(1, idOrderable);
		ti.deleteRow( tc, keyRow1, SYSFILESRowFactory.SYSFILES_INDEX2_ID );
	}

	/**
	 * Get a SPSDescriptor given its UUID.
	 *
	 * @param uuid	The UUID
	 *
	 * @return The SPSDescriptor for the constraint.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public SPSDescriptor getSPSDescriptor(UUID uuid)
				throws StandardException
	{
		SPSDescriptor 		sps;

		/* Make sure that non-core info is initialized */
		getNonCoreTI(SYSSTATEMENTS_CATALOG_NUM);

		/* Only use the cache if we're in compile-only mode */
		if ((spsNameCache != null) && 
			(getCacheMode() == DataDictionary.COMPILE_ONLY_MODE))
		{
			sps = (SPSDescriptor)spsIdHash.get(uuid);
			if (sps != null)
			{
				//System.out.println("found in hash table ");
				// System.out.println("stmt text " + sps.getText());

				return sps;
			}
	
			sps = getSPSDescriptorIndex2Scan(uuid.toString());
			TableKey stmtKey = new TableKey(sps.getSchemaDescriptor().getUUID(), sps.getName());
			try
			{
				SPSNameCacheable 	cacheEntry = (SPSNameCacheable)spsNameCache.create(stmtKey, sps);
				spsNameCache.release(cacheEntry);
			} catch (StandardException se)
			{
				/*
				** If the error is that the item is already
				** in the cache, then that is ok.
				*/
				if (SQLState.OBJECT_EXISTS_IN_CACHE.equals(se.getMessageId()))
				{
					return sps;
				}
				else
				{
					throw se;
				}
			}
			
		}
		else
		{
			sps = getSPSDescriptorIndex2Scan(uuid.toString());
		}

		return sps;
	}

	/**
		Add an entry to the hashtables for lookup from the cache.
	 */
	void spsCacheEntryAdded(SPSDescriptor spsd)
	{
		spsIdHash.put(spsd.getUUID(), spsd);
		// spsTextHash.put(spsd.getText(), spsd);
	}

	void spsCacheEntryRemoved(SPSDescriptor spsd) {
		spsIdHash.remove(spsd.getUUID());
		// spsTextHash.remove(spsd.getText());
	}

	//public SPSDescriptor getSPSBySQLText(String text) {

	//	return (SPSDescriptor) spsTextHash.get(text);
	//}

	/**
	 * This method can get called from the DataDictionary cache.
	 *
	 * @param stmtKey	The TableKey of the sps
	 *
	 * @return	The descriptor for the sps, null if the sps does
	 *		not exist.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public SPSDescriptor getUncachedSPSDescriptor(TableKey stmtKey)
				throws StandardException
	{
		return getSPSDescriptorIndex1Scan(stmtKey.getTableName(), 
											stmtKey.getSchemaId().toString());
	}

	/**
	 * This method can get called from the DataDictionary cache.
	 *
	 * @param stmtId	The UUID of the stmt to get the descriptor for
	 *
	 * @return	The descriptor for the stmt, null if the table does
	 *		not exist.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	protected SPSDescriptor	getUncachedSPSDescriptor(UUID stmtId)
				throws StandardException
	{
		return getSPSDescriptorIndex2Scan(stmtId.toString());
	}

	/**
	 * Scan sysstatements_index2 (stmtid) for a match.
	 * Note that we do not do a lookup of parameter info.
	 *
	 * @return SPSDescriptor	The matching descriptor, if any.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	private SPSDescriptor getSPSDescriptorIndex2Scan(
										String stmtUUID)
				throws StandardException
	{
		DataValueDescriptor		  stmtIDOrderable;
		TabInfo					  ti = getNonCoreTI(SYSSTATEMENTS_CATALOG_NUM);

		/* Use stmtIdOrderable in both start 
		 * and stop position for scan. 
		 */
		stmtIDOrderable = dvf.getCharDataValue(stmtUUID);

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = exFactory.getIndexableRow(1);
		keyRow.setColumn(1, stmtIDOrderable);

		SPSDescriptor spsd = (SPSDescriptor)
					getDescriptorViaIndex(
						SYSSTATEMENTSRowFactory.SYSSTATEMENTS_INDEX1_ID,
						keyRow,
						(ScanQualifier [][]) null,
						ti,
						(TupleDescriptor) null,
						(List) null,
						false);

		return spsd;
	}

	/**
	 * Get a SPSDescriptor given its name.
	 * Currently no cacheing.  With caching
	 * we need to be very careful about invalidation.
	 * No caching means invalidations block on
	 * existing SPSD instances (since they were read in
	 *
	 * @param stmtName	the statement name
	 * @param sd	The SchemaDescriptor
	 *
	 * @return The SPSDescriptor for the constraint.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public SPSDescriptor getSPSDescriptor(String stmtName, SchemaDescriptor sd)
		throws StandardException
	{
		SPSDescriptor		sps = null;
		TableKey			stmtKey;
		UUID				schemaUUID;

		/*
		** If we didn't get a schema descriptor, we had better
		** have a system table.
		*/
		if (SanityManager.DEBUG)
		{
			if (sd == null)
			{
				SanityManager.THROWASSERT("null schema for statement "+stmtName);	
			}
		}

		schemaUUID = sd.getUUID();
		stmtKey = new TableKey(schemaUUID, stmtName);

		/* Only use the cache if we're in compile-only mode */
		if ((spsNameCache != null) && 
			(getCacheMode() == DataDictionary.COMPILE_ONLY_MODE))
		{
			SPSNameCacheable cacheEntry = (SPSNameCacheable) spsNameCache.find(stmtKey);
			if (cacheEntry != null)
			{
				sps = cacheEntry.getSPSDescriptor();
				spsNameCache.release(cacheEntry);
			}
			//System.out.println("found in cache " + stmtName);
			//System.out.println("stmt text " + sps.getText());
			return sps;
		}

		return getSPSDescriptorIndex1Scan(stmtName, schemaUUID.toString());
	}

	/**
	 * Scan sysschemas_index1 (stmtname, schemaid) for a match.
	 *
	 * @return SPSDescriptor	The matching descriptor, if any.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	private SPSDescriptor getSPSDescriptorIndex1Scan(
										String stmtName, 
										String schemaUUID)
				throws StandardException
	{
		DataValueDescriptor		  schemaIDOrderable;
		DataValueDescriptor		  stmtNameOrderable;
		TabInfo					  ti = getNonCoreTI(SYSSTATEMENTS_CATALOG_NUM);

		/* Use stmtNameOrderable and schemaIdOrderable in both start 
		 * and stop position for scan. 
		 */
		stmtNameOrderable = dvf.getVarcharDataValue(stmtName);
		schemaIDOrderable = dvf.getCharDataValue(schemaUUID);

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = exFactory.getIndexableRow(2);
		keyRow.setColumn(1, stmtNameOrderable);
		keyRow.setColumn(2, schemaIDOrderable);

		SPSDescriptor spsd = (SPSDescriptor)
					getDescriptorViaIndex(
						SYSSTATEMENTSRowFactory.SYSSTATEMENTS_INDEX2_ID,
						keyRow,
						(ScanQualifier [][]) null,
						ti,
						(TupleDescriptor) null,
						(List) null,
						false);
	
		/*
		** Set up the parameter defaults.  We are only
		** doing this when we look up by name because
		** this is the only time we cache, and it can
		** be foolish to look up the parameter defaults
		** for someone that doesn't need them.
		*/
		if (spsd != null)
		{
			Vector v = new Vector();
			spsd.setParams(getSPSParams(spsd, v));
			Object[] defaults = new Object[v.size()];
			v.copyInto(defaults);
			spsd.setParameterDefaults(defaults);
		}

		return spsd;
	}

	/**
	 * Adds the given SPSDescriptor to the data dictionary,
	 * associated with the given table and constraint type.
	 *
	 * @param descriptor	The descriptor to add
	 * @param tc			The transaction controller
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	addSPSDescriptor
	(
		SPSDescriptor 			descriptor,
		TransactionController	tc,
		boolean wait
	) throws StandardException
	{
		ExecRow        			row;
		TabInfo					ti = getNonCoreTI(SYSSTATEMENTS_CATALOG_NUM);
		SYSSTATEMENTSRowFactory	rf = (SYSSTATEMENTSRowFactory) ti.getCatalogRowFactory();
		int						insertRetCode;

		/*
		** We must make sure the descriptor is locked
		** while we are writing it out.  Otherwise,
		** the descriptor could be invalidated while
		** we are writing.
		*/
		synchronized(descriptor)
		{
			// build the row to be stuffed into SYSSTATEMENTS. this will stuff an
			// UUID into the descriptor
			boolean	compileMe = descriptor.initiallyCompilable();
			row = rf.makeSYSSTATEMENTSrow(compileMe, descriptor);
	
			// insert row into catalog and all its indices
			insertRetCode = ti.insertRow(row, tc, wait);
		}

		// Throw an exception duplicate table descriptor
		if (insertRetCode != TabInfo.ROWNOTDUPLICATE)
		{
			throw StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT, 
												 descriptor.getDescriptorType(),
												 descriptor.getDescriptorName(),
												 descriptor.getSchemaDescriptor().getDescriptorType(),
												 descriptor.getSchemaDescriptor().getSchemaName());
		}

		addSPSParams(descriptor, tc, wait);
	}

	/**
	 * Add a column in SYS.SYSCOLUMNS for each parameter in the
	 * parameter list.
	 */
	private void addSPSParams(SPSDescriptor spsd, TransactionController tc, boolean wait)
			throws StandardException
	{
		UUID 					uuid = spsd.getUUID();
		DataTypeDescriptor[] params = spsd.getParams();
		Object[] parameterDefaults = spsd.getParameterDefaults();

		if (params == null)
			return;

		/* Create the columns */
		int pdlSize = params.length;
		for (int index = 0; index < pdlSize; index++)
		{
			int parameterId = index + 1;

			//RESOLVEAUTOINCREMENT
			ColumnDescriptor cd = 
                new ColumnDescriptor(
                    "PARAM" + parameterId,
                    parameterId,	// position
					params[index],
                    ((parameterDefaults == null) || // default
                        (index >= parameterDefaults.length)) ? 
                        (DataValueDescriptor)null :
                        (DataValueDescriptor)parameterDefaults[index],
                    (DefaultInfo) null,
                    uuid,
                    (UUID) null, 0, 0, false);
										
			addDescriptorNow(cd, null, SYSCOLUMNS_CATALOG_NUM, 
						  false, // no chance of duplicates here
						  tc, wait);
		}
	}

	/**
	 * Get all the parameter descriptors for an SPS.
	 * Look up the params in SYSCOLUMNS and turn them
	 * into parameter descriptors.  
	 *
	 * @param spsd	sps descriptor
	 * @param defaults	vector for storing column defaults
	 *
	 * @return array of data type descriptors
	 *
	 * @exception StandardException		Thrown on error
	 */
	public DataTypeDescriptor[] getSPSParams(SPSDescriptor spsd, Vector defaults)
		throws StandardException
	{
		ColumnDescriptorList cdl = new ColumnDescriptorList();
		getColumnDescriptorsScan(spsd.getUUID(), cdl, spsd);

		int cdlSize = cdl.size();
		DataTypeDescriptor[] params = new DataTypeDescriptor[cdlSize];	
		for (int index = 0; index < cdlSize; index++)
		{
			ColumnDescriptor cd = (ColumnDescriptor) cdl.elementAt(index);
			params[index] = cd.getType();
			if (defaults != null)
			{
				defaults.addElement(cd.getDefaultValue());
			}
		}

		return params;	
	}

	/**
	 * Updates SYS.SYSSTATEMENTS with the info from the
	 * SPSD. 
	 *
	 * @param descriptor	The descriptor to add
	 * @param tc			The transaction controller
	 * @param updCols		Int array of columns that are to be updated 
	 *						(1 based).  Not null.
	 * @param updateParamDescriptors If true, will update the
	 *						parameter descriptors in SYS.SYSCOLUMNS.
	 * @param wait		If true, then the caller wants to wait for locks. False will be
	 * @param firstCompilation  true, if Statement is getting compiled for first
	 *                          time and SPS was created with NOCOMPILE option.
	 *
	 * when we using a nested user xaction - we want to timeout right away if the parent
	 * holds the lock.  (bug 4821)
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	updateSPS(
			SPSDescriptor			spsd,
			TransactionController	tc,
			boolean                 recompile,
			boolean					updateParamDescriptors,
			boolean					wait,
			boolean                 firstCompilation)
						throws StandardException
	{
		ExecIndexRow				keyRow1 = null;
		ExecRow    					row;
		DataValueDescriptor			idOrderable;
		DataValueDescriptor			columnNameOrderable;
		TabInfo						ti = getNonCoreTI(SYSSTATEMENTS_CATALOG_NUM);
		SYSSTATEMENTSRowFactory  rf = (SYSSTATEMENTSRowFactory) ti.getCatalogRowFactory();
		int[] updCols;
		if (recompile)
		{
			if(firstCompilation)
			{
				updCols = new int[] {SYSSTATEMENTSRowFactory.SYSSTATEMENTS_VALID,
									 SYSSTATEMENTSRowFactory.SYSSTATEMENTS_LASTCOMPILED,
									 SYSSTATEMENTSRowFactory.SYSSTATEMENTS_USINGTEXT,
									 SYSSTATEMENTSRowFactory.SYSSTATEMENTS_CONSTANTSTATE,
									 SYSSTATEMENTSRowFactory.SYSSTATEMENTS_INITIALLY_COMPILABLE};
			}else
			{

				updCols = new int[] {SYSSTATEMENTSRowFactory.SYSSTATEMENTS_VALID,
										 SYSSTATEMENTSRowFactory.SYSSTATEMENTS_LASTCOMPILED,
										 SYSSTATEMENTSRowFactory.SYSSTATEMENTS_USINGTEXT,
										 SYSSTATEMENTSRowFactory.SYSSTATEMENTS_CONSTANTSTATE };
			}
		}
		else 
		{
			updCols = new int[]	{SYSSTATEMENTSRowFactory.SYSSTATEMENTS_VALID} ;
		}
			
		idOrderable = getValueAsDVD(spsd.getUUID());

		/* Set up the start/stop position for the scan */
		keyRow1 = (ExecIndexRow) exFactory.getIndexableRow(1);
		keyRow1.setColumn(1, idOrderable);

		row = rf.makeSYSSTATEMENTSrow(false, 	// don't compile
									  spsd);

		/*
		** Not updating any indexes
		*/
		boolean[] bArray = new boolean[2];

		/*
		** Partial update
		*/
		ti.updateRow(keyRow1, row, 
					 SYSSTATEMENTSRowFactory.SYSSTATEMENTS_INDEX1_ID,
					 bArray,
					 updCols,
					 tc,
					 wait);


		/*
		** If we don't need to update the parameter
		** descriptors, we are done.
		*/
		if (!updateParamDescriptors)
		{
			return;
		}

		/*
		** Set the defaults and datatypes for the parameters, if
		** there are parameters.
		*/
		DataTypeDescriptor[] params = spsd.getParams();
		if (params == null)
		{
			return;
		}

		if(firstCompilation)
		{
			/*beetle:5119, reason for doing add here instead of update
			 *is with NOCOMPILE option of create statement/boot time SPS,
			 *SPS statement is not compiled to find out the parameter info.
			 *Because of the parameter info was not inserted at SPSDescriptor 
			 *creation time. As this is the first time we are compiling paramter
			 *infor should be inserted instead of the update.
			 */
			addSPSParams(spsd, tc, wait);
		}
		else
		{
			Object[] parameterDefaults = spsd.getParameterDefaults();

			/* 
			** Update each column with the new defaults and with
			** the new datatypes.  It is possible that someone has
			** done a drop/create on the underlying table and 
			** changed the type of a column, which has changed
			** the type of a parameter to our statement.
			*/
			int[] columnsToSet = new int[2];
			columnsToSet[0] = SYSCOLUMNSRowFactory.SYSCOLUMNS_COLUMNDATATYPE;
			columnsToSet[1] = SYSCOLUMNSRowFactory.SYSCOLUMNS_COLUMNDEFAULT;

			UUID uuid = spsd.getUUID();

			for (int index = 0; index < params.length; index++)
			{
				int parameterId = index + 1;

			//RESOLVEAUTOINCREMENT
				ColumnDescriptor cd = new ColumnDescriptor("PARAM" + parameterId,
										  parameterId,	// position
										  params[index],
										  ((parameterDefaults == null) || // default
										   (index >= parameterDefaults.length)) ? 
										  (DataValueDescriptor)null :
										  (DataValueDescriptor)parameterDefaults[index],
										  (DefaultInfo) null,
										  uuid,
										  (UUID) null,
										  0, 0, false);
										
				updateColumnDescriptor(cd,
									   cd.getReferencingUUID(), 
									   cd.getColumnName(),
									   columnsToSet, 
									   tc,
									   wait);
			}
		}
	}

	/**
	 * @see DataDictionary#invalidateAllSPSPlans
	 * @exception StandardException		Thrown on error
	 */
	public void invalidateAllSPSPlans() throws StandardException
	{
		LanguageConnectionContext lcc = (LanguageConnectionContext) 
			ContextService.getContext(LanguageConnectionContext.CONTEXT_ID);
		startWriting(lcc);

		for (java.util.Iterator li = getAllSPSDescriptors().iterator(); li.hasNext(); )
		{ 
			SPSDescriptor spsd = (SPSDescriptor) li.next();
			spsd.makeInvalid(DependencyManager.USER_RECOMPILE_REQUEST, lcc);
		} 
	}


	/**
	 * Mark all SPS plans in the data dictionary invalid. This does
	 * not invalidate cached plans. This function is for use by
	 * the boot-up code.
	 * @exception StandardException		Thrown on error
	 */
	void clearSPSPlans() throws StandardException
	{
		TabInfo ti = getNonCoreTI(SYSSTATEMENTS_CATALOG_NUM);
		faultInTabInfo(ti);

		TransactionController tc = getTransactionExecute();

		FormatableBitSet columnToReadSet = new FormatableBitSet(SYSSTATEMENTSRowFactory.SYSSTATEMENTS_COLUMN_COUNT);
		FormatableBitSet columnToUpdateSet = new FormatableBitSet(SYSSTATEMENTSRowFactory.SYSSTATEMENTS_COLUMN_COUNT);
		columnToUpdateSet.set(SYSSTATEMENTSRowFactory.SYSSTATEMENTS_VALID -1);
		columnToUpdateSet.set(SYSSTATEMENTSRowFactory.SYSSTATEMENTS_CONSTANTSTATE -1);

		DataValueDescriptor[] replaceRow = 
            new DataValueDescriptor[SYSSTATEMENTSRowFactory.SYSSTATEMENTS_COLUMN_COUNT];

		/* Set up a couple of row templates for fetching CHARS */

		replaceRow[SYSSTATEMENTSRowFactory.SYSSTATEMENTS_VALID - 1] = 
            dvf.getDataValue(false);
		replaceRow[SYSSTATEMENTSRowFactory.SYSSTATEMENTS_CONSTANTSTATE - 1] = 
            dvf.getDataValue((Object) null);

		/* Scan the entire heap */
		ScanController sc = 
            tc.openScan(
                ti.getHeapConglomerate(),
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_REPEATABLE_READ,
                columnToReadSet,
                (DataValueDescriptor[]) null,
                ScanController.NA,
                (Qualifier[][]) null,
                (DataValueDescriptor[]) null,
                ScanController.NA);

		while (sc.fetchNext((DataValueDescriptor[]) null))
		{
			/* Replace the column in the table */
			sc.replace(replaceRow, columnToUpdateSet);
		}

		sc.close();
	}

	/**
	 * Drops the given SPSDescriptor.
	 *
	 * @param descriptor	The descriptor to drop
	 * @param tc	The TransactionController.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	dropSPSDescriptor(SPSDescriptor descriptor,
			TransactionController tc)
						throws StandardException
	{
		dropSPSDescriptor(descriptor.getUUID(), tc);
	}

	/**
	 * Drops the given SPSDescriptor. 
	 *
	 * @param uuid	the statement uuid
	 * @param tc	The TransactionController.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	dropSPSDescriptor
	(
		UUID 					uuid,
		TransactionController	tc
	) throws StandardException
	{
		DataValueDescriptor		stmtIdOrderable;
		TabInfo					ti = getNonCoreTI(SYSSTATEMENTS_CATALOG_NUM);

		stmtIdOrderable = getValueAsDVD(uuid);

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = (ExecIndexRow) exFactory.getIndexableRow(1);
		keyRow.setColumn(1, stmtIdOrderable);

		ti.deleteRow( tc, keyRow, SYSSTATEMENTSRowFactory.SYSSTATEMENTS_INDEX1_ID );

		/* drop all columns in SYSCOLUMNS */	
		dropAllColumnDescriptors(uuid, tc);
	}

	/**
	 * Get every statement in this database.
	 * Return the SPSDescriptors in an list.
	 *
	 * @return the list of descriptors
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public List getAllSPSDescriptors()
		throws StandardException
	{
		TabInfo					ti = getNonCoreTI(SYSSTATEMENTS_CATALOG_NUM);

		List list = newSList();

		getDescriptorViaHeap(
						(ScanQualifier[][]) null,
						ti,
						(TupleDescriptor) null,
						list);

		return list;

	}

	/**
	 * Get every constraint in this database.
	 * Note that this list of ConstraintDescriptors is
	 * not going to be the same objects that are typically
	 * cached off of the table descriptors, so this will
	 * most likely instantiate some duplicate objects.
	 *
	 * @return the list of descriptors
	 *
	 * @exception StandardException		Thrown on failure
	 */
	private ConstraintDescriptorList getAllConstraintDescriptors()
		throws StandardException
	{
		TabInfo					ti = getNonCoreTI(SYSCONSTRAINTS_CATALOG_NUM);

		ConstraintDescriptorList list = new ConstraintDescriptorList();

		getConstraintDescriptorViaHeap(
						(ScanQualifier[][]) null,
						ti,
						(TupleDescriptor) null,
						list);
		return list;
	}

	/**
	 * Get every trigger in this database.
	 * Note that this list of TriggerDescriptors is
	 * not going to be the same objects that are typically
	 * cached off of the table descriptors, so this will
	 * most likely instantiate some duplicate objects.
	 *
	 * @return the list of descriptors
	 *
	 * @exception StandardException		Thrown on failure
	 */
	private GenericDescriptorList getAllTriggerDescriptors()
		throws StandardException
	{
		TabInfo					ti = getNonCoreTI(SYSTRIGGERS_CATALOG_NUM);

		GenericDescriptorList list = new GenericDescriptorList();

		getDescriptorViaHeap(
						(ScanQualifier[][]) null,
						ti,
						(TupleDescriptor) null,
						list);
		return list;
	}

	/**
	 * Get a TriggerDescriptor given its UUID.
	 *
	 * @param uuid	The UUID
	 *
	 * @return The TriggerDescriptor for the constraint.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public TriggerDescriptor getTriggerDescriptor(UUID uuid)
				throws StandardException
	{
		TabInfo					  ti = getNonCoreTI(SYSTRIGGERS_CATALOG_NUM);
		DataValueDescriptor triggerIdOrderable = dvf.getCharDataValue(uuid.toString());

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = exFactory.getIndexableRow(1);
		keyRow.setColumn(1, triggerIdOrderable);

		return (TriggerDescriptor)
					getDescriptorViaIndex(
						SYSTRIGGERSRowFactory.SYSTRIGGERS_INDEX1_ID,
						keyRow,
						(ScanQualifier [][]) null,
						ti,
						(TupleDescriptor) null,
						(List) null,
						false);
	}

	/** 
	 * Get the stored prepared statement descriptor given 
	 * a sps name.
	 *
	 * @param name	The sps name.
	 * @param sd	The schema descriptor.
	 *
	 * @return The TriggerDescriptor for the constraint.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public TriggerDescriptor getTriggerDescriptor(String name, SchemaDescriptor sd)
				throws StandardException
	{
		DataValueDescriptor		  schemaIDOrderable;
		DataValueDescriptor		  triggerNameOrderable;
		TabInfo					  ti = getNonCoreTI(SYSTRIGGERS_CATALOG_NUM);

		/* Use triggerNameOrderable and schemaIdOrderable in both start 
		 * and stop position for scan. 
		 */
		triggerNameOrderable = dvf.getVarcharDataValue(name);
		schemaIDOrderable = dvf.getCharDataValue(sd.getUUID().toString());

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = exFactory.getIndexableRow(2);
		keyRow.setColumn(1, triggerNameOrderable);
		keyRow.setColumn(2, schemaIDOrderable);

		return (TriggerDescriptor)
					getDescriptorViaIndex(
						SYSTRIGGERSRowFactory.SYSTRIGGERS_INDEX2_ID,
						keyRow,
						(ScanQualifier [][]) null,
						ti,
						(TupleDescriptor) null,
						(List) null,
						false);
	}

	/**
	 * Load up the trigger descriptor list for this table
	 * descriptor and return it.  If the descriptor list
	 * is already loaded up, it is retuned without further
	 * ado.
	 *
	 * @param table			The table descriptor.
	 *
	 * @return The ConstraintDescriptorList for the table
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public GenericDescriptorList getTriggerDescriptors(TableDescriptor td)
		throws StandardException
	{
		GenericDescriptorList	gdl;

		if (td == null)
		{
			return getAllTriggerDescriptors();
		}

		/* Build the TableDescriptor's TDL if it is currently empty */
		gdl = td.getTriggerDescriptorList();

		/*
		** Synchronize the building of the TDL.  The TDL itself is created
		** empty when the TD is created, so there is no need to synchronize
		** the getting of the TDL.
		*/
		synchronized(gdl)
		{
			if (!gdl.getScanned())
			{
				getTriggerDescriptorsScan(td, false);
			}
		}

		return gdl;
	}

	/** 
	 * Populate the GenericDescriptorList for the specified TableDescriptor.
	 *
	 * MT synchronization: it is assumed that the caller has synchronized
	 * on the CDL in the given TD.
	 *
	 * @param td				The TableDescriptor.
	 * @param forUpdate			Whether or not to open scan for update
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	 private void getTriggerDescriptorsScan(TableDescriptor td, boolean forUpdate)
			throws StandardException
	{
		GenericDescriptorList  	gdl = (td).getTriggerDescriptorList();
		DataValueDescriptor		tableIDOrderable = null;
		TabInfo					ti = getNonCoreTI(SYSTRIGGERS_CATALOG_NUM);

		/* Use tableIDOrderable in both start and stop positions for scan */
		tableIDOrderable = getValueAsDVD(td.getUUID());

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = (ExecIndexRow) exFactory.getIndexableRow(1);

		keyRow.setColumn(1, tableIDOrderable);

		getDescriptorViaIndex(
					SYSTRIGGERSRowFactory.SYSTRIGGERS_INDEX3_ID,
					keyRow,
					(ScanQualifier [][]) null,
					ti,
					(TupleDescriptor) null,
					gdl,
					forUpdate);
		gdl.setScanned(true);
	}

	/**
	 * Drops the given TriggerDescriptor.  WARNING: does
	 * not drop its SPSes!!!
	 *
	 * @param descriptor	The descriptor to drop
	 * @param tc	The TransactionController.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	dropTriggerDescriptor
	(
		TriggerDescriptor 		descriptor,
		TransactionController 	tc
	) throws StandardException
	{
		DataValueDescriptor		idOrderable;
		TabInfo					ti = getNonCoreTI(SYSTRIGGERS_CATALOG_NUM);

		idOrderable = getValueAsDVD(descriptor.getUUID());

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = (ExecIndexRow) exFactory.getIndexableRow(1);
		keyRow.setColumn(1, idOrderable);

		ti.deleteRow(tc, keyRow, SYSTRIGGERSRowFactory.SYSTRIGGERS_INDEX1_ID);
	}

	/**
	 * Update the trigger descriptor in question.  Updates
	 * every row in the base conglomerate that matches the uuid.
	 *
	 * @param triggerd				The Trigger descriptor
	 * @param formerUUID			The UUID for this column in SYSTRIGGERS,
	 *								may differ from what is in triggerd if this
	 *								is the column that is being set.
	 * @param colsToSet 			Array of ints of columns to be modified,
	 *								1 based.  May be null (all cols).
	 * @param tc					The TransactionController to use
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void updateTriggerDescriptor
	(
		TriggerDescriptor 		triggerd,
		UUID					formerUUID,
		int[]					colsToSet,
		TransactionController	tc
	) throws StandardException
	{
		ExecIndexRow				keyRow1 = null;
		ExecRow    					row;
		DataValueDescriptor			IDOrderable;
		DataValueDescriptor			columnNameOrderable;
		TabInfo						ti = getNonCoreTI(SYSTRIGGERS_CATALOG_NUM);
		SYSTRIGGERSRowFactory  		rf = (SYSTRIGGERSRowFactory) ti.getCatalogRowFactory();

		/* Use objectID in both start 
		 * and stop position for index 1 scan. 
		 */
		IDOrderable = getValueAsDVD(formerUUID);

		/* Set up the start/stop position for the scan */
		keyRow1 = (ExecIndexRow) exFactory.getIndexableRow(1);
		keyRow1.setColumn(1, IDOrderable);

		// build the row to be stuffed into SYSTRIGGERS. 
		row = rf.makeRow(triggerd, null);

		/*
		** Figure out if the index in systriggers needs 
		** to be updated. 
		*/
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(rf.getNumIndexes() == 3, 
					"There are more indexes on systriggers than expected, the code herein needs to change");
		}

		boolean[] bArray = new boolean[3];

		/*
		** Do we need to update indexes?
		*/
		if (colsToSet == null)
		{
			bArray[0] = true;
			bArray[1] = true;
			bArray[2] = true;
		}
		else
		{
			/*
			** Check the specific columns for indexed
			** columns.
			*/
			for (int i = 0; i < colsToSet.length; i++)
			{
				switch (colsToSet[i])
				{
					case SYSTRIGGERSRowFactory.SYSTRIGGERS_TRIGGERID:
						bArray[0] = true;
						break;

					case SYSTRIGGERSRowFactory.SYSTRIGGERS_TRIGGERNAME:
					case SYSTRIGGERSRowFactory.SYSTRIGGERS_SCHEMAID:
						bArray[1] = true;
						break;
					
					case SYSTRIGGERSRowFactory.SYSTRIGGERS_TABLEID:
						bArray[2] = true;
						break;
				}
			}
		}

		ti.updateRow(keyRow1, row, 
					 SYSTRIGGERSRowFactory.SYSTRIGGERS_INDEX1_ID,
					 bArray,
					 colsToSet,
					 tc);
	}


	/**
	 * Get a ConstraintDescriptor given its UUID.  Please
	 * use getConstraintDescriptorById() is you have the
	 * constraints table descriptor, it is much faster.
	 *
	 * @param uuid	The UUID
	 *
	 *
	 * @return The ConstraintDescriptor for the constraint.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstraintDescriptor getConstraintDescriptor(UUID uuid)
				throws StandardException
	{
		DataValueDescriptor		UUIDStringOrderable;
		TabInfo					ti = getNonCoreTI(SYSCONSTRAINTS_CATALOG_NUM);

		/* Use UUIDStringOrderable in both start and stop positions for scan */
		UUIDStringOrderable = dvf.getCharDataValue(uuid.toString());

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = exFactory.getIndexableRow(1);
		keyRow.setColumn(1, UUIDStringOrderable);

		return getConstraintDescriptorViaIndex(
					SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_INDEX1_ID,
					keyRow,
					ti,
					(TableDescriptor) null,
					(ConstraintDescriptorList) null,
					false);
	}

	/**
	 * Get a ConstraintDescriptor given its name and schema ID.
	 * Please use getConstraintDescriptorByName() if you have the
	 * constraint's table descriptor, it is much faster.
	 *
	 * @param constraintName	Constraint name.
	 * @param schemaID			The schema UUID
	 *
	 * @return The ConstraintDescriptor for the constraint.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstraintDescriptor getConstraintDescriptor
	(
		String	constraintName,
		UUID	schemaID
    )
		throws StandardException
	{
		DataValueDescriptor		UUIDStringOrderable;
		DataValueDescriptor		constraintNameOrderable;
		TabInfo					ti = getNonCoreTI(SYSCONSTRAINTS_CATALOG_NUM);

		/* Construct keys for both start and stop positions for scan */
		constraintNameOrderable = dvf.getVarcharDataValue(constraintName);
		UUIDStringOrderable = dvf.getCharDataValue(schemaID.toString());

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = exFactory.getIndexableRow(2);
		keyRow.setColumn(1, constraintNameOrderable);
		keyRow.setColumn(2, UUIDStringOrderable);

		return getConstraintDescriptorViaIndex(
					SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_INDEX2_ID,
					keyRow,
					ti,
					(TableDescriptor) null,
					(ConstraintDescriptorList) null,
					false);
	}

	/** get all the statistiscs descriptors for a given table.
	 * @param  td	Table Descriptor for which I need statistics
	 */
	public List getStatisticsDescriptors(TableDescriptor td)
		throws StandardException
	{
		TabInfo ti = getNonCoreTI(SYSSTATISTICS_CATALOG_NUM);
		List statDescriptorList = newSList();
		DataValueDescriptor UUIDStringOrderable;

		/* set up the start/stop position for the scan */
		UUIDStringOrderable = dvf.getCharDataValue(td.getUUID().toString());
		ExecIndexRow keyRow = exFactory.getIndexableRow(1);
		keyRow.setColumn(1, UUIDStringOrderable);
		
		getDescriptorViaIndex(SYSSTATISTICSRowFactory.SYSSTATISTICS_INDEX1_ID,
							  keyRow,
							  (ScanQualifier [][])null,
							  ti, 
							  (TupleDescriptor)null,
							  statDescriptorList, false);

		return statDescriptorList;
	}
	
	/**
	 * Load up the constraint descriptor list for this table
	 * descriptor and return it.  If the descriptor list
	 * is already loaded up, it is retuned without further
	 * ado.  If no table descriptor is passed in, then all
	 * constraint descriptors are retrieved.  Note that in 
	 * this case, the constraint descriptor objects may be
	 * duplicates of constraint descriptors that are hung
	 * off of the table descriptor cache.
	 *
	 * @param table			The table descriptor.  If null,
	 *						all constraint descriptors are returned.
	 *
	 *
	 * @return The ConstraintDescriptorList for the table
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstraintDescriptorList getConstraintDescriptors(TableDescriptor td)
		throws StandardException
	{
		ConstraintDescriptorList	cdl;

		if (td == null)
		{
			return getAllConstraintDescriptors();
		}

		/* RESOLVE - need to look at multi-user aspects of hanging constraint
		 * descriptor list off of table descriptor when we restore the cache.
		 */

		/* Build the TableDescriptor's CDL if it is currently empty */
		cdl = td.getConstraintDescriptorList();

		/*
		** Synchronize the building of the CDL.  The CDL itself is created
		** empty when the TD is created, so there is no need to synchronize
		** the getting of the CDL.
		*/
		synchronized(cdl)
		{
			if (! cdl.getScanned())
			{
				getConstraintDescriptorsScan(td, false);
			}
		}

		return cdl;
	}

	/**
	 * Convert a constraint descriptor list into a list
	 * of active constraints, that is, constraints which
	 * must be enforced. For the Core product, these
	 * are just the constraints on the original list.
	 * However, during REFRESH we may have deferred some
	 * constraints until statement end. This method returns
	 * the corresponding list of constraints which AREN'T
	 * deferred.
	 *
	 * @param cdl	The constraint descriptor list to wrap with
	 *				an Active constraint descriptor list.
	 *
	 * @return The corresponding Active ConstraintDescriptorList
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstraintDescriptorList getActiveConstraintDescriptors(ConstraintDescriptorList cdl)
		throws StandardException
	{ return cdl; }

	/**
	 * Reports whether an individual constraint must be
	 * enforced. For the Core product, this routine always
	 * returns true.
	 *
	 * However, during REFRESH we may have deferred some
	 * constraints until statement end. This method returns
	 * false if the constraint deferred
	 *
	 * @param constraint	the constraint to check
	 *
	 *
	 * @return The corresponding Active ConstraintDescriptorList
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public boolean activeConstraint( ConstraintDescriptor constraint )
		throws StandardException
	{ return true; }

	/** 
	 * Get the constraint descriptor given a table and the UUID String
	 * of the backing index.
	 *
	 * @param table			The table descriptor.
	 * @param uuid			the UUID for the backing index.
	 *
	 * @return The ConstraintDescriptor for the constraint.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstraintDescriptor getConstraintDescriptor(TableDescriptor td, 
														UUID uuid)
				throws StandardException
	{
		return getConstraintDescriptors(td).getConstraintDescriptor(uuid);
	}


	/**
	 * Get the constraint descriptor given a table and the UUID String
	 * of the constraint
	 *
	 * @param table			The table descriptor.
	 * @param uuid			The UUID for the constraint
	 *
	 * @return The ConstraintDescriptor for the constraint.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstraintDescriptor getConstraintDescriptorById
	(
		TableDescriptor	td,
		UUID			uuid
    )
		throws StandardException
	{
		return getConstraintDescriptors(td).getConstraintDescriptorById(uuid);
	}

	/** 
	 * Get the constraint descriptor given a TableDescriptor and the constraint name.
	 *
	 * @param table				The table descriptor.
	 * @param sd				The schema descriptor for the constraint
	 * @param constraintName	The constraint name.
	 * @param forUpdate			Whether or not access is for update
	 *
	 * @return The ConstraintDescriptor for the constraint.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstraintDescriptor getConstraintDescriptorByName(TableDescriptor td, 
															  SchemaDescriptor sd,
															  String constraintName,
															  boolean forUpdate)
				throws StandardException
	{
		/* If forUpdate, then we need to actually read from the table. */
		if (forUpdate)
		{
			td.emptyConstraintDescriptorList();
			getConstraintDescriptorsScan(td, true);
		}
		return getConstraintDescriptors(td).getConstraintDescriptorByName(sd, constraintName);
	}

	/** 
	 * Populate the ConstraintDescriptorList for the specified TableDescriptor.
	 *
	 * MT synchronization: it is assumed that the caller has synchronized
	 * on the CDL in the given TD.
	 *
	 * @param td				The TableDescriptor.
	 * @param forUpdate			Whether or not to open scan for update
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	 private void getConstraintDescriptorsScan(TableDescriptor td, boolean forUpdate)
			throws StandardException
	{
		ConstraintDescriptorList  cdl = td.getConstraintDescriptorList();
		DataValueDescriptor		  tableIDOrderable = null;
		TabInfo					  ti = getNonCoreTI(SYSCONSTRAINTS_CATALOG_NUM);

		/* Use tableIDOrderable in both start and stop positions for scan */
		tableIDOrderable = getValueAsDVD(td.getUUID());

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = (ExecIndexRow) exFactory.getIndexableRow(1);

		keyRow.setColumn(1, tableIDOrderable);

		getConstraintDescriptorViaIndex(
					SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_INDEX3_ID,
					keyRow,
					ti,
					td,
					cdl,
					forUpdate);
		cdl.setScanned(true);
	}

	/**
	 * Return a (single or list of) ConstraintDescriptor(s) from
	 * SYSCONSTRAINTS where the access is from the index to the heap.
	 *
	 * @param indexId	The id of the index (0 to # of indexes on table) to use
	 * @param keyRow	The supplied ExecIndexRow for search
	 * @param ti		The TabInfo to use
	 * @param td		The TableDescriptor, if supplied.
	 * @param dList		The list to build, if supplied.  If null, then caller expects
	 *					a single descriptor
	 * @param forUpdate			Whether or not to open scan for update
	 *
	 * @return	The last matching descriptor
	 *
	 * @exception StandardException		Thrown on error
	 */
	protected ConstraintDescriptor getConstraintDescriptorViaIndex(
						int indexId,
						ExecIndexRow keyRow,
						TabInfo ti,
						TableDescriptor td,
						ConstraintDescriptorList dList,
						boolean forUpdate)
			throws StandardException
	{
		SYSCONSTRAINTSRowFactory rf = (SYSCONSTRAINTSRowFactory) ti.getCatalogRowFactory();
		ConglomerateController	heapCC;
		ConstraintDescriptor	cd = null;
		ExecIndexRow	  		indexRow1;
		ExecIndexRow			indexTemplateRow;
		ExecRow 				outRow;
		RowLocation				baseRowLocation;
		ScanController			scanController;
		TransactionController	tc;

		// Get the current transaction controller
		tc = getTransactionCompile();

		outRow = rf.makeEmptyRow();

		heapCC = 
            tc.openConglomerate(
                ti.getHeapConglomerate(), false, 0, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_REPEATABLE_READ);


		/* Scan the index and go to the data pages for qualifying rows to
		 * build the column descriptor.
		 */
		scanController = tc.openScan(
				ti.getIndexConglomerate(indexId),  // conglomerate to open
				false, // don't hold open across commit
				(forUpdate) ? TransactionController.OPENMODE_FORUPDATE : 0, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_REPEATABLE_READ,
				(FormatableBitSet) null,         // all fields as objects
				keyRow.getRowArray(),   // start position - exact key match.
				ScanController.GE,      // startSearchOperation
				null,                   //scanQualifier,
				keyRow.getRowArray(),   // stop position - exact key match.
				ScanController.GT);     // stopSearchOperation

		while (scanController.next())
		{
			SubConstraintDescriptor subCD = null;

			// create an index row template
			indexRow1 = getIndexRowFromHeapRow(
									ti.getIndexRowGenerator(indexId),
									heapCC.newRowLocationTemplate(),
									outRow);

			scanController.fetch(indexRow1.getRowArray());

			baseRowLocation = (RowLocation)	indexRow1.getColumn(
												indexRow1.nColumns());

			boolean base_row_exists = 
                heapCC.fetch(
                    baseRowLocation, outRow.getRowArray(), (FormatableBitSet) null);

            if (SanityManager.DEBUG)
            {
                // it can not be possible for heap row to disappear while 
                // holding scan cursor on index at ISOLATION_REPEATABLE_READ.
                SanityManager.ASSERT(base_row_exists, "base row doesn't exist");
            }

			switch (rf.getConstraintType(outRow))
			{
				case DataDictionary.PRIMARYKEY_CONSTRAINT: 
				case DataDictionary.FOREIGNKEY_CONSTRAINT: 
				case DataDictionary.UNIQUE_CONSTRAINT: 
					subCD = getSubKeyConstraint(
								rf.getConstraintId(outRow), rf.getConstraintType(outRow));
					break;

				case DataDictionary.CHECK_CONSTRAINT: 
					subCD = getSubCheckConstraint(
								rf.getConstraintId(outRow));
					break;

				default:
					if (SanityManager.DEBUG)
					{
						SanityManager.THROWASSERT("unexpected value "+
								"from rf.getConstraintType(outRow)" +
								rf.getConstraintType(outRow));
					}
			}

			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(subCD != null,
									 "subCD is expected to be non-null");
			}

			/* Cache the TD in the SCD so that
			 * the row factory doesn't need to go
			 * out to disk to get it.
			 */
			subCD.setTableDescriptor(td);

			cd = (ConstraintDescriptor) rf.buildDescriptor(
												outRow,
												subCD,
												this);

			/* If dList is null, then caller only wants a single descriptor - we're done
			 * else just add the current descriptor to the list.
			 */
			if (dList == null)
			{
				break;
			}
			else
			{
				dList.add(cd);
			}
		}
        scanController.close();
		heapCC.close();
		return cd;
	}

	/**
	 * Return a (single or list of) catalog row descriptor(s) from
	 * SYSCONSTRAINTS through a heap scan 
	 *
	 * @param scanQualifiers			qualifiers
	 * @param ti						The TabInfo to use
	 * @param parentTupleDescriptor		The parentDescriptor, if applicable.
	 * @param dList						The list to build, if supplied.  
	 *									If null, then caller expects a single descriptor
	 *
	 * @return	The last matching descriptor
	 *
	 * @exception StandardException		Thrown on error
	 */
	protected TupleDescriptor getConstraintDescriptorViaHeap(
						ScanQualifier [][] scanQualifiers,
						TabInfo ti,
						TupleDescriptor parentTupleDescriptor,
						List list)
			throws StandardException
	{
		SYSCONSTRAINTSRowFactory rf = (SYSCONSTRAINTSRowFactory) ti.getCatalogRowFactory();
		ConglomerateController	heapCC;
		ExecRow 				outRow;
		ExecRow 				templateRow;
		ScanController			scanController;
		TransactionController	tc;
		ConstraintDescriptor	cd = null;

		// Get the current transaction controller
		tc = getTransactionCompile();

		outRow = rf.makeEmptyRow();

		/*
		** Table scan
		*/
		scanController = tc.openScan(
				ti.getHeapConglomerate(),	  // conglomerate to open
				false, 						  // don't hold open across commit
				0, 							  // for read
				TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_REPEATABLE_READ,
				(FormatableBitSet) null,               // all fields as objects
				(DataValueDescriptor[]) null, // start position - first row
				0,      				      // startSearchOperation - none
				scanQualifiers, 		      // scanQualifier,
				(DataValueDescriptor[]) null, // stop position -through last row
				0);     				      // stopSearchOperation - none

		try
		{
			while (scanController.fetchNext(outRow.getRowArray()))
			{
				SubConstraintDescriptor subCD = null;
	
				switch (rf.getConstraintType(outRow))
				{
					case DataDictionary.PRIMARYKEY_CONSTRAINT: 
					case DataDictionary.FOREIGNKEY_CONSTRAINT: 
					case DataDictionary.UNIQUE_CONSTRAINT: 
						subCD = getSubKeyConstraint(
									rf.getConstraintId(outRow), rf.getConstraintType(outRow));
						break;
	
					case DataDictionary.CHECK_CONSTRAINT: 
						subCD = getSubCheckConstraint(
									rf.getConstraintId(outRow));
						break;
	
					default:
						if (SanityManager.DEBUG)
						{
							SanityManager.THROWASSERT("unexpected value from "+
									" rf.getConstraintType(outRow) "
									+ rf.getConstraintType(outRow));
						}
				}
	
				if (SanityManager.DEBUG)
				{
					SanityManager.ASSERT(subCD != null,
										 "subCD is expected to be non-null");
				}
				cd = (ConstraintDescriptor) rf.buildDescriptor(
													outRow,
													subCD,
													this);
	
				/* If dList is null, then caller only wants a single descriptor - we're done
				 * else just add the current descriptor to the list.
				 */
				if (list == null)
				{
					break;
				}
				else
				{
					list.add(cd);
				}
			}
		}
		finally
		{
			scanController.close();
		}
		return cd;
	}

	/**
	 * Return a table descriptor corresponding to the TABLEID
	 * field in SYSCONSTRAINTS where CONSTRAINTID matches
	 * the constraintId passsed in.
	 *
	 * @param constraintId	The id of the constraint
	 *
	 * @return	the corresponding table descriptor
	 *
	 * @exception StandardException		Thrown on error
	 */
	public TableDescriptor getConstraintTableDescriptor(UUID constraintId)
			throws StandardException
	{
		List slist = getConstraints(constraintId,
											SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_INDEX1_ID,
											SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_TABLEID);

		if (slist.size() == 0)
		{
			return null;
		}
	
		// get the table descriptor	
		return getTableDescriptor((UUID)slist.get(0));
	}

	/**
	 * Return a list of foreign keys constraints referencing
	 * this constraint.  Returns both enabled and disabled
	 * foreign keys.  
	 *
	 * @param constraintId	The id of the referenced constraint
	 *
	 * @return	list of constraints, empty of there are none
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ConstraintDescriptorList getForeignKeys(UUID constraintId)
			throws StandardException
	{
		TabInfo ti = getNonCoreTI(SYSFOREIGNKEYS_CATALOG_NUM);
		List fkList = newSList();

		// Use constraintIDOrderable in both start and stop positions for scan
		DataValueDescriptor constraintIDOrderable = getValueAsDVD(constraintId);

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = (ExecIndexRow) exFactory.getIndexableRow(1);
		keyRow.setColumn(1, constraintIDOrderable);

		getDescriptorViaIndex(
						SYSFOREIGNKEYSRowFactory.SYSFOREIGNKEYS_INDEX2_ID,
						keyRow,
						(ScanQualifier [][]) null,
						ti,
						(TupleDescriptor) null,
						fkList,
						false);

		SubKeyConstraintDescriptor cd;
		TableDescriptor td;
		ConstraintDescriptorList cdl = new ConstraintDescriptorList();
		ConstraintDescriptorList tmpCdl;

		for (Iterator iterator = fkList.iterator(); iterator.hasNext(); )
		{
			cd = (SubKeyConstraintDescriptor) iterator.next();
			td = getConstraintTableDescriptor(cd.getUUID());
			cdl.add(getConstraintDescriptors(td).getConstraintDescriptorById(cd.getUUID()));
		}

		return cdl;
	}

	/**
	 * Return an List which of the relevant column matching
	 * the indexed criteria.  If nothing matches, returns an
	 * empty List (never returns null).
	 *
	 * @param constraintId	The id of the constraint
	 * @param indexId		The index id in SYS.SYSCONSTRAINTS
	 * @param columnNum		The column to retrieve
	 *
	 * @return a list of UUIDs in an List.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public List getConstraints(UUID uuid, int indexId, int columnNum)
			throws StandardException
	{
		ExecIndexRow	  		indexRow1;
		ExecIndexRow			indexTemplateRow;
		ExecRow 				outRow;
		RowLocation				baseRowLocation;
		ConglomerateController	heapCC = null;
		ScanController			scanController = null;
		TransactionController	tc;
		TabInfo 				ti = getNonCoreTI(SYSCONSTRAINTS_CATALOG_NUM);
		SYSCONSTRAINTSRowFactory rf = (SYSCONSTRAINTSRowFactory) ti.getCatalogRowFactory();
		TableDescriptor			td = null;
		List					slist = newSList();

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(indexId == SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_INDEX1_ID ||
								 indexId == SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_INDEX3_ID, 
									"bad index id, must be one of the indexes on a uuid");
			SanityManager.ASSERT(columnNum > 0 && 
								 columnNum <= SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_COLUMN_COUNT,
									"invalid column number for column to be retrieved");
		}

		try
		{
			/* Use tableIDOrderable in both start and stop positions for scan */
			DataValueDescriptor orderable = getValueAsDVD(uuid);
	
			/* Set up the start/stop position for the scan */
			ExecIndexRow keyRow = (ExecIndexRow) exFactory.getIndexableRow(1);
			keyRow.setColumn(1, orderable);

			// Get the current transaction controller
			tc = getTransactionCompile();
	
			outRow = rf.makeEmptyRow();
	
			heapCC = 
                tc.openConglomerate(
                    ti.getHeapConglomerate(), false, 0, 
                    TransactionController.MODE_RECORD,
                    TransactionController.ISOLATION_REPEATABLE_READ);

			// create an index row template
			indexRow1 = getIndexRowFromHeapRow(
								ti.getIndexRowGenerator(indexId), 
								heapCC.newRowLocationTemplate(),
								outRow);
	
			// just interested in one column
			DataValueDescriptor[] rowTemplate    = 
              new DataValueDescriptor[SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_COLUMN_COUNT];
			FormatableBitSet  columnToGetSet = 
              new FormatableBitSet(SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_COLUMN_COUNT);
			columnToGetSet.set(columnNum - 1);

			rowTemplate[columnNum - 1] = 
                dvf.getNullChar((StringDataValue) null);
	
			// Scan the index and go to the data pages for qualifying rows 
			scanController = tc.openScan(
					ti.getIndexConglomerate(indexId),// conglomerate to open
					false, 							// don't hold open across commit
					0, 								// for read
	                TransactionController.MODE_RECORD,
	                TransactionController.ISOLATION_REPEATABLE_READ,// RESOLVE: should be level 2
					(FormatableBitSet) null,                 // all fields as objects
					keyRow.getRowArray(),			// start position - exact key match.
					ScanController.GE, 				// startSearchOperation
					null, 							// scanQualifier (none)
					keyRow.getRowArray(),			// stop position - exact key match.
					ScanController.GT);				// stopSearchOperation

			while (scanController.fetchNext(indexRow1.getRowArray()))
			{	
				baseRowLocation = (RowLocation)	
                    indexRow1.getColumn(indexRow1.nColumns());
	
				// get the row and grab the uuid
				boolean base_row_exists = 
                    heapCC.fetch(
                        baseRowLocation, rowTemplate, columnToGetSet);

                if (SanityManager.DEBUG)
                {
                    // it can not be possible for heap row to disappear while 
                    // holding scan cursor on index at ISOLATION_REPEATABLE_READ.
                    SanityManager.ASSERT(base_row_exists, "base row not found");
                }

				slist.add(uuidFactory.recreateUUID(
                    (String)((DataValueDescriptor)rowTemplate[columnNum - 1]).getObject()));
			}
		}
		finally
		{
			if (heapCC != null)
			{
				heapCC.close();
			}
			if (scanController != null)
			{
				scanController.close();
			}
		}
		return slist;
	}

	/**
	 * Adds the given ConstraintDescriptor to the data dictionary,
	 * associated with the given table and constraint type.
	 *
	 * @param descriptor	The descriptor to add
	 * @param tc			The transaction controller
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	addConstraintDescriptor(
			ConstraintDescriptor descriptor,
			TransactionController tc)
		throws StandardException
	{
		ExecRow        			row = null;
		int						type = descriptor.getConstraintType();
		TabInfo					  ti = getNonCoreTI(SYSCONSTRAINTS_CATALOG_NUM);
		SYSCONSTRAINTSRowFactory  rf = (SYSCONSTRAINTSRowFactory) ti.getCatalogRowFactory();
		int						insertRetCode;

		if (SanityManager.DEBUG)
		{
			if (!(type == DataDictionary.PRIMARYKEY_CONSTRAINT ||
								 type == DataDictionary.FOREIGNKEY_CONSTRAINT ||
								 type == DataDictionary.UNIQUE_CONSTRAINT ||
								 type == DataDictionary.CHECK_CONSTRAINT))
			{
				SanityManager.THROWASSERT("constraint type (" + type +
					") is unexpected value");
			}
		}

		addDescriptor(descriptor, descriptor.getSchemaDescriptor(),
					  SYSCONSTRAINTS_CATALOG_NUM, false,
					  tc);

		switch (type)
		{
			case DataDictionary.PRIMARYKEY_CONSTRAINT:
			case DataDictionary.FOREIGNKEY_CONSTRAINT:
			case DataDictionary.UNIQUE_CONSTRAINT:
				if (SanityManager.DEBUG)
				{
					if (!(descriptor instanceof KeyConstraintDescriptor))
					{
						SanityManager.THROWASSERT(
							"descriptor expected to be instanceof KeyConstraintDescriptor, " +
							"not, " + descriptor.getClass().getName());
					}
				}

				addSubKeyConstraint((KeyConstraintDescriptor) descriptor, tc);
				break;

			case DataDictionary.CHECK_CONSTRAINT:
				if (SanityManager.DEBUG)
				{
					if (!(descriptor instanceof CheckConstraintDescriptor))
					{
						SanityManager.THROWASSERT("descriptor expected "+
							"to be instanceof CheckConstraintDescriptorImpl, " +
							"not, " + descriptor.getClass().getName());
					}
				}

				addDescriptor(descriptor, null, SYSCHECKS_CATALOG_NUM, true, tc);
				break;
		}
	}

	/**
	 * Update the constraint descriptor in question.  Updates
	 * every row in the base conglomerate.  
	 *
	 * @param cd					The Constraintescriptor
	 * @param formerUUID			The UUID for this column in SYSCONSTRAINTS,
	 *								may differ from what is in cd if this
	 *								is the column that is being set.
	 * @param colsToSet 			Array of ints of columns to be modified,
	 *								1 based.  May be null (all cols).
	 * @param tc					The TransactionController to use
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void updateConstraintDescriptor(ConstraintDescriptor cd,
										UUID		formerUUID,
										int[]		colsToSet,
										TransactionController tc)
		throws StandardException
	{
		ExecIndexRow				keyRow1 = null;
		ExecRow    					row;
		DataValueDescriptor			IDOrderable;
		DataValueDescriptor			columnNameOrderable;
		TabInfo						ti = getNonCoreTI(SYSCONSTRAINTS_CATALOG_NUM);
		SYSCONSTRAINTSRowFactory  	rf = (SYSCONSTRAINTSRowFactory) ti.getCatalogRowFactory();

		/* Use objectID/columnName in both start 
		 * and stop position for index 1 scan. 
		 */
		IDOrderable = getValueAsDVD(formerUUID);

		/* Set up the start/stop position for the scan */
		keyRow1 = (ExecIndexRow) exFactory.getIndexableRow(1);
		keyRow1.setColumn(1, IDOrderable);

		// build the row to be stuffed into SYSCONSTRAINTS. 
		row = rf.makeRow(cd, null);

		/*
		** Figure out if the index in sysconstraints needs 
		** to be updated. 
		*/
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(rf.getNumIndexes() == 3, 
					"There are more indexes on sysconstraints than expected, the code herein needs to change");
		}

		boolean[] bArray = new boolean[3];

		/*
		** Do we need to update indexes?
		*/
		if (colsToSet == null)
		{
			bArray[0] = true;
			bArray[1] = true;
			bArray[2] = true;
		}
		else
		{
			/*
			** Check the specific columns for indexed
			** columns.
			*/
			for (int i = 0; i < colsToSet.length; i++)
			{
				switch (colsToSet[i])
				{
					case SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_CONSTRAINTID:
						bArray[0] = true;
						break;

					case SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_CONSTRAINTNAME:
					case SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_SCHEMAID:
						bArray[1] = true;
						break;
					
					case SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_TABLEID:
						bArray[2] = true;
						break;
				}
			}
		}

		ti.updateRow(keyRow1, row, 
					 SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_INDEX1_ID,
					 bArray,
					 colsToSet,
					 tc);
	}

	/**
	 * Drops the given ConstraintDescriptor that is associated
	 * with the given table and constraint type from the data dictionary.
	 *
	 * @param table	The table from which to drop the
	 *			constraint descriptor
	 * @param descriptor	The descriptor to drop
	 * @param tc			The TransactionController
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	dropConstraintDescriptor(TableDescriptor table,
			ConstraintDescriptor descriptor,
			TransactionController tc)
		throws StandardException
	{
		ExecIndexRow			keyRow = null;
		DataValueDescriptor		schemaIDOrderable;
		DataValueDescriptor		constraintNameOrderable;
		TabInfo					ti = getNonCoreTI(SYSCONSTRAINTS_CATALOG_NUM);

		switch (descriptor.getConstraintType())
		{
			case DataDictionary.PRIMARYKEY_CONSTRAINT: 
			case DataDictionary.FOREIGNKEY_CONSTRAINT: 
			case DataDictionary.UNIQUE_CONSTRAINT: 
				dropSubKeyConstraint(
							descriptor,
							tc);
				break;

			case DataDictionary.CHECK_CONSTRAINT: 
				dropSubCheckConstraint(
							descriptor.getUUID(),
							tc);
				break;
		}

		/* Use constraintNameOrderable and schemaIdOrderable in both start 
		 * and stop position for index 2 scan. 
		 */
		constraintNameOrderable = dvf.getVarcharDataValue(descriptor.getConstraintName());
		schemaIDOrderable = getValueAsDVD(descriptor.getSchemaDescriptor().getUUID());

		/* Set up the start/stop position for the scan */
		keyRow = (ExecIndexRow) exFactory.getIndexableRow(2);
		keyRow.setColumn(1, constraintNameOrderable);
		keyRow.setColumn(2, schemaIDOrderable);

		ti.deleteRow( tc, keyRow, SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_INDEX2_ID );
	}

	/**
	 * Drops all ConstraintDescriptors from the data dictionary
	 * that are associated with the given table,
	 *
	 * @param table	The table from which to drop all
	 *			constraint descriptors
	 * @param tc	The TransactionController
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	dropAllConstraintDescriptors(TableDescriptor table, 
											 TransactionController tc)
		throws StandardException
	{
		ConstraintDescriptorList cdl = getConstraintDescriptors(table);

		// Walk the table's CDL and drop each ConstraintDescriptor.
		for (Iterator iterator = cdl.iterator(); iterator.hasNext(); )
		{
			ConstraintDescriptor cd = (ConstraintDescriptor) iterator.next();
			dropConstraintDescriptor(table, cd, tc);
		}

		/*
		** Null out the table's constraint descriptor list.  NOTE: This is
		** not really necessary at the time of this writing (11/3/97), because
		** we do not cache data dictionary objects while DDL is going on,
		** but in the future it might be necessary.
		*/
		table.setConstraintDescriptorList(null);
	}

	/**
	 * Get a SubKeyConstraintDescriptor from syskeys or sysforeignkeys for
	 * the specified constraint id.  For primary foreign and and unique
	 * key constraints.
	 *
	 * @param constraintId	The UUID for the constraint.
	 * @param type	The type of the constraint 
	 *		(e.g. DataDictionary.FOREIGNKEY_CONSTRAINT)
	 *
	 * @return SubKeyConstraintDescriptor	The Sub descriptor for the constraint.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public SubKeyConstraintDescriptor getSubKeyConstraint(UUID constraintId, int type)
		throws StandardException
	{
		DataValueDescriptor		constraintIDOrderable = null;
		TabInfo					ti;
		int						indexNum;
		int						baseNum;

		if (type == DataDictionary.FOREIGNKEY_CONSTRAINT)
		{
			baseNum = SYSFOREIGNKEYS_CATALOG_NUM;
			indexNum = SYSFOREIGNKEYSRowFactory.SYSFOREIGNKEYS_INDEX1_ID;
		}
		else
		{
			baseNum = SYSKEYS_CATALOG_NUM;
			indexNum = SYSKEYSRowFactory.SYSKEYS_INDEX1_ID;
		}
		ti = getNonCoreTI(baseNum);

		/* Use constraintIDOrderable in both start and stop positions for scan */
		constraintIDOrderable = getValueAsDVD(constraintId);

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = (ExecIndexRow) exFactory.getIndexableRow(1);
		keyRow.setColumn(1, constraintIDOrderable);

		return (SubKeyConstraintDescriptor)
					getDescriptorViaIndex(
						indexNum,
						keyRow,
						(ScanQualifier [][]) null,
						ti,
						(TupleDescriptor) null,
						(List) null,
						false);
	}

	/**
	 * Add the matching row to syskeys when adding a unique or primary key constraint
	 *
	 * @param descriptor	The KeyConstraintDescriptor for the constraint.
	 * @param tc			The TransactionController
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	private void addSubKeyConstraint(KeyConstraintDescriptor descriptor,
									 TransactionController tc)
		throws StandardException
	{
		ExecRow	row;
		TabInfo	ti;

		/*
		** Foreign keys get a row in SYSFOREIGNKEYS, and
		** all others get a row in SYSKEYS.
		*/
		if (descriptor.getConstraintType() 
				== DataDictionary.FOREIGNKEY_CONSTRAINT)
		{
			ForeignKeyConstraintDescriptor fkDescriptor =
					(ForeignKeyConstraintDescriptor)descriptor;

			if (SanityManager.DEBUG)
			{
				if (!(descriptor instanceof ForeignKeyConstraintDescriptor))
				{
					SanityManager.THROWASSERT("descriptor not an fk descriptor, is "+
						descriptor.getClass().getName());
				}
			}
			
			ti = getNonCoreTI(SYSFOREIGNKEYS_CATALOG_NUM);
			SYSFOREIGNKEYSRowFactory fkkeysRF = (SYSFOREIGNKEYSRowFactory)ti.getCatalogRowFactory();

			row = fkkeysRF.makeRow(fkDescriptor, null);

			/*
			** Now we need to bump the reference count of the
			** contraint that this FK references
			*/
			ReferencedKeyConstraintDescriptor refDescriptor =
							fkDescriptor.getReferencedConstraint();

			refDescriptor.incrementReferenceCount();

			int[] colsToSet = new int[1];
			colsToSet[0] = SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_REFERENCECOUNT;

			updateConstraintDescriptor(refDescriptor, 
											refDescriptor.getUUID(), 
											colsToSet,
											tc);
		}
		else
		{
			ti = getNonCoreTI(SYSKEYS_CATALOG_NUM);
			SYSKEYSRowFactory keysRF = (SYSKEYSRowFactory) ti.getCatalogRowFactory();

			// build the row to be stuffed into SYSKEYS
			row = keysRF.makeRow(descriptor, null);
		}

		// insert row into catalog and all its indices
		ti.insertRow(row, tc, true);
	}

	/**
	 * Drop the matching row from syskeys when dropping a primary key
	 * or unique constraint.
	 *
	 * @param constraint	the constraint
	 * @param tc			The TransactionController
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	private void dropSubKeyConstraint(ConstraintDescriptor constraint, TransactionController tc)
		throws StandardException
	{
		ExecIndexRow			keyRow1 = null;
		DataValueDescriptor		constraintIdOrderable;
		TabInfo					ti;
		int						baseNum;
		int						indexNum;

		if (constraint.getConstraintType() 
				== DataDictionary.FOREIGNKEY_CONSTRAINT)
		{
			baseNum = SYSFOREIGNKEYS_CATALOG_NUM;
			indexNum = SYSFOREIGNKEYSRowFactory.SYSFOREIGNKEYS_INDEX1_ID;

			/*
			** If we have a foreign key, we need to decrement the 
			** reference count of the contraint that this FK references.
			** We need to do this *before* we drop the foreign key
			** because of the way FK.getReferencedConstraint() works.	
			*/
			if (constraint.getConstraintType() 
					== DataDictionary.FOREIGNKEY_CONSTRAINT)
			{
				ReferencedKeyConstraintDescriptor refDescriptor =
						(ReferencedKeyConstraintDescriptor)
								getConstraintDescriptor(
									((ForeignKeyConstraintDescriptor)constraint).
											getReferencedConstraintId());

				if (refDescriptor != null)
				{
					refDescriptor.decrementReferenceCount();
	
					int[] colsToSet = new int[1];
					colsToSet[0] = SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_REFERENCECOUNT;
		
					updateConstraintDescriptor(refDescriptor, 
												refDescriptor.getUUID(), 
												colsToSet,
												tc);
				}
			}
		}
		else
		{
			baseNum = SYSKEYS_CATALOG_NUM;
			indexNum = SYSKEYSRowFactory.SYSKEYS_INDEX1_ID;
		}

		ti = getNonCoreTI(baseNum);

		/* Use constraintIdOrderable in both start 
		 * and stop position for index 1 scan. 
		 */
		constraintIdOrderable = getValueAsDVD(constraint.getUUID());

		/* Set up the start/stop position for the scan */
		keyRow1 = (ExecIndexRow) exFactory.getIndexableRow(1);
		keyRow1.setColumn(1, constraintIdOrderable);

		ti.deleteRow( tc, keyRow1, indexNum);
	}

	/**
	 * Get a SubCheckConstraintDescriptor from syschecks for
	 * the specified constraint id.  (Useful for check constraints.)
	 *
	 * @param constraintId	The UUID for the constraint.
	 *
	 * @return SubCheckConstraintDescriptor	The Sub descriptor for the constraint.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	private SubCheckConstraintDescriptor getSubCheckConstraint(UUID constraintId)
		throws StandardException
	{
		DataValueDescriptor			constraintIDOrderable = null;
		TabInfo						ti = getNonCoreTI(SYSCHECKS_CATALOG_NUM);
		SYSCHECKSRowFactory			rf = (SYSCHECKSRowFactory) ti.getCatalogRowFactory();

		/* Use constraintIDOrderable in both start and stop positions for scan */
		constraintIDOrderable = getValueAsDVD(constraintId);

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = (ExecIndexRow) exFactory.getIndexableRow(1);
		keyRow.setColumn(1, constraintIDOrderable);

		return (SubCheckConstraintDescriptor)
					getDescriptorViaIndex(
						SYSCHECKSRowFactory.SYSCHECKS_INDEX1_ID,
						keyRow,
						(ScanQualifier [][]) null,
						ti,
						(TupleDescriptor) null,
						(List) null,
						false);
	}

	/**
	 * Drop the matching row from syschecks when dropping a check constraint.
	 *
	 * @param constraintId	The constraint id.
	 * @param tc			The TransactionController
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	private void dropSubCheckConstraint(UUID constraintId, TransactionController tc)
		throws StandardException
	{
		ExecIndexRow			checkRow1 = null;
		DataValueDescriptor		constraintIdOrderable;
		TabInfo					ti = getNonCoreTI(SYSCHECKS_CATALOG_NUM);

		/* Use constraintIdOrderable in both start 
		 * and stop position for index 1 scan. 
		 */
		constraintIdOrderable = getValueAsDVD(constraintId);

		/* Set up the start/stop position for the scan */
		checkRow1 = (ExecIndexRow) exFactory.getIndexableRow(1);
		checkRow1.setColumn(1, constraintIdOrderable);

		ti.deleteRow( tc, checkRow1, SYSCHECKSRowFactory.SYSCHECKS_INDEX1_ID );
	}

	/**
	 * Get all of the ConglomerateDescriptors in the database and
	 * hash them by conglomerate number.
	 * This is useful as a performance optimization for the locking VTIs.
	 * NOTE:  This method will scan SYS.SYSCONGLOMERATES at READ UNCOMMITTED.
	 *
	 * @param tc		TransactionController for the transaction
	 *
	 * @return	A Hashtable with all of the ConglomerateDescriptors
	 *		in the database hashed by conglomerate number.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public Hashtable hashAllConglomerateDescriptorsByNumber(TransactionController tc)
		throws StandardException
	{
		Hashtable ht = new Hashtable();
		ConglomerateDescriptor	  cd = null;
		ScanController			  scanController;
		ExecRow 				  outRow;
		// ExecIndexRow			  keyRow = null;
		TabInfo					  ti = coreInfo[SYSCONGLOMERATES_CORE_NUM];
		SYSCONGLOMERATESRowFactory  rf = (SYSCONGLOMERATESRowFactory) ti.getCatalogRowFactory();

		outRow = rf.makeEmptyRow();
		scanController = tc.openScan(
				ti.getHeapConglomerate(),  // conglomerate to open
				false, // don't hold open across commit
				0, // for read
                TransactionController.MODE_RECORD,  // scans whole table.
                TransactionController.ISOLATION_READ_UNCOMMITTED,
				(FormatableBitSet) null, // all fields as objects
				(DataValueDescriptor[]) null, //keyRow.getRowArray(),   // start position - first row
				ScanController.GE,      // startSearchOperation
				(ScanQualifier [][]) null,
				(DataValueDescriptor[]) null, //keyRow.getRowArray(),   // stop position - through last row
				ScanController.GT);     // stopSearchOperation

        // it is important for read uncommitted scans to use fetchNext() rather
        // than fetch, so that the fetch happens while latch is held, otherwise
        // the next() might position the scan on a row, but the subsequent
        // fetch() may find the row deleted or purged from the table.
		while (scanController.fetchNext(outRow.getRowArray()))
		{
			cd = (ConglomerateDescriptor) rf.buildDescriptor(
												outRow,
												(TupleDescriptor) null,
												this );
			Long hashKey = new Long(cd.getConglomerateNumber());
			ht.put(hashKey, cd);
		}

        scanController.close();

		return ht;
	}

	/**
	 * Get all of the TableDescriptors in the database and hash them by TableId
	 * This is useful as a performance optimization for the locking VTIs.
	 * NOTE:  This method will scan SYS.SYSTABLES at READ UNCOMMITTED.
	 *
	 * @param tc		TransactionController for the transaction
	 *
	 * @return	A Hashtable with all of the Table descriptors in the database
	 *			hashed by TableId
	 *
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public Hashtable hashAllTableDescriptorsByTableId(TransactionController tc)
		throws StandardException
	{
		Hashtable ht = new Hashtable();
		ScanController			  scanController;
		ExecRow 				  outRow;
		TabInfo					ti = coreInfo[SYSTABLES_CORE_NUM];
		SYSTABLESRowFactory
					rf = (SYSTABLESRowFactory) ti.getCatalogRowFactory();

		outRow = rf.makeEmptyRow();

		scanController = tc.openScan(
				ti.getHeapConglomerate(),       // sys.systable
				false,                          // don't hold open across commit
				0,                              // for read
                TransactionController.MODE_RECORD,// scans whole table.
                TransactionController.ISOLATION_READ_UNCOMMITTED,
				(FormatableBitSet) null,                 // all fields as objects
				(DataValueDescriptor[])null,    // start position - first row
				ScanController.GE,              // startSearchOperation
				(ScanQualifier[][])null,        //scanQualifier,
				(DataValueDescriptor[])null,    //stop position-through last row
				ScanController.GT);             // stopSearchOperation

        // it is important for read uncommitted scans to use fetchNext() rather
        // than fetch, so that the fetch happens while latch is held, otherwise
        // the next() might position the scan on a row, but the subsequent
        // fetch() may find the row deleted or purged from the table.
		while(scanController.fetchNext(outRow.getRowArray()))
		{
			TableDescriptor td = (TableDescriptor)
				rf.buildDescriptor(outRow, (TupleDescriptor)null,
								   this);
			ht.put(td.getUUID(), td);
		}
		scanController.close();
		return ht;
	}

	/**
	 * Get a ConglomerateDescriptor given its UUID.  If it is an index
	 * conglomerate shared by at least another duplicate index, this returns
	 * one of the ConglomerateDescriptors for those indexes. 
	 *
	 * @param uuid	The UUID
	 *
	 *
	 * @return A ConglomerateDescriptor for the conglomerate.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConglomerateDescriptor getConglomerateDescriptor(UUID uuid)
				throws StandardException
	{
		ConglomerateDescriptor[] cds = getConglomerateDescriptors(uuid);
		if (cds.length == 0)
			return null;
		return cds[0];
	}

	/**
	 * Get an array of ConglomerateDescriptors given the UUID.  If it is a
	 * heap conglomerate or an index conglomerate not shared by a duplicate
	 * index, the size of the return array is 1.
	 *
	 * @param uuid	The UUID
	 *
	 *
	 * @return An array of ConglomerateDescriptors for the conglomerate.
	 *				returns size 0 array if no such conglomerate.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConglomerateDescriptor[] getConglomerateDescriptors(UUID uuid)
				throws StandardException
	{
		DataValueDescriptor		UUIDStringOrderable;
		SYSCONGLOMERATESRowFactory rf;
		TabInfo					ti = coreInfo[SYSCONGLOMERATES_CORE_NUM];

		/* Use UUIDStringOrderable in both start and stop positions for scan */
		UUIDStringOrderable = dvf.getCharDataValue(uuid.toString());

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = exFactory.getIndexableRow(1);
		keyRow.setColumn(1, UUIDStringOrderable);

		List cdl = newSList();

		getDescriptorViaIndex(
						SYSCONGLOMERATESRowFactory.SYSCONGLOMERATES_INDEX1_ID,
						keyRow,
						(ScanQualifier [][]) null,
						ti,
						(TupleDescriptor) null,
						cdl,
						false);

		ConglomerateDescriptor[] cda = new ConglomerateDescriptor[cdl.size()];
		cdl.toArray(cda);
		return cda;

	}

	/**
	 * Get a ConglomerateDescriptor given its conglomerate number.  If it is an
	 * index conglomerate shared by at least another duplicate index, this
	 * returns one of the ConglomerateDescriptors for those indexes. 
	 *
	 * @param conglomerateNumber	The conglomerate number.
	 *
	 *
	 * @return A ConglomerateDescriptor for the conglomerate.  Returns NULL if
	 *				no such conglomerate.
	 *
	 * @exception StandardException		Thrown on failure
	 */
  	public ConglomerateDescriptor getConglomerateDescriptor(
  									long conglomerateNumber)
  									throws StandardException
  	{
		ConglomerateDescriptor[] cds = getConglomerateDescriptors(conglomerateNumber);
		if (cds.length == 0)
			return null;
		return cds[0];
	}

	/**
	 * Get an array of conglomerate descriptors for the given conglomerate
	 * number.  If it is a heap conglomerate or an index conglomerate not
	 * shared by a duplicate index, the size of the return array is 1.
	 *
	 * @param conglomerateNumber	The number for the conglomerate
	 *				we're interested in
	 *
	 * @return	An array of ConglomerateDescriptors that share the requested
	 *		conglomerate. Returns size 0 array if no such conglomerate.
	 *
	 * @exception StandardException		Thrown on failure
	 */
  	public ConglomerateDescriptor[] getConglomerateDescriptors(
  									long conglomerateNumber)
  									throws StandardException
  	{
  		ScanController			  scanController;
  		TransactionController	  tc;
  		ExecRow 				  outRow;
  		DataValueDescriptor		  conglomNumberOrderable = null;
  		TabInfo					  ti = coreInfo[SYSCONGLOMERATES_CORE_NUM];
  		SYSCONGLOMERATESRowFactory  rf = (SYSCONGLOMERATESRowFactory) ti.getCatalogRowFactory();

  		conglomNumberOrderable = 
  				dvf.getDataValue(conglomerateNumber);

		ScanQualifier[][] scanQualifier = exFactory.getScanQualifier(1);
  		scanQualifier[0][0].setQualifier(
  				rf.SYSCONGLOMERATES_CONGLOMERATENUMBER - 1,	/* column number */
  				conglomNumberOrderable,
  				Orderable.ORDER_OP_EQUALS,
 				false,
  				false,
  				false);

		ConglomerateDescriptorList cdl = new ConglomerateDescriptorList();
		getDescriptorViaHeap(scanQualifier,
								 ti,
								 null,
								 cdl);

		int size = cdl.size();
		ConglomerateDescriptor[] cda = new ConglomerateDescriptor[size];
		for (int index = 0; index < size; index++)
			cda[index] = (ConglomerateDescriptor) cdl.get(index);

		return cda;
	}
	

	/** 
	 * Populate the ConglomerateDescriptorList for the
	 * specified TableDescriptor by scanning sysconglomerates.
	 *
	 * MT synchronization: it is assumed that the caller has synchronized
	 * on the CDL in the given TD.
	 *
	 * @param td			The TableDescriptor.
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	private void getConglomerateDescriptorsScan(TableDescriptor td)
			throws StandardException
	{
		ConglomerateDescriptorList cdl = td.getConglomerateDescriptorList();

		ExecIndexRow			keyRow3 = null;
		DataValueDescriptor		tableIDOrderable;
		TabInfo					ti = coreInfo[SYSCONGLOMERATES_CORE_NUM];

		/* Use tableIDOrderable in both start and stop positions for scan */
		tableIDOrderable = getValueAsDVD(td.getUUID());

		/* Set up the start/stop position for the scan */
		keyRow3 = (ExecIndexRow) exFactory.getIndexableRow(1);
		keyRow3.setColumn(1, tableIDOrderable);

		getDescriptorViaIndex(
			SYSCONGLOMERATESRowFactory.SYSCONGLOMERATES_INDEX3_ID,
				keyRow3,
				(ScanQualifier [][]) null,
				ti,
				(TupleDescriptor) null,
				cdl,
				false);
	}

	/**
	 * Gets a conglomerate descriptor for the named index in the given schema,
	 * getting an exclusive row lock on the matching row in 
	 * sys.sysconglomerates (for DDL concurrency) if requested.
	 *
	 * @param indexName	The name of the index we're looking for
	 * @param sd		The schema descriptor
	 * @param forUpdate	Whether or not to get an exclusive row 
	 *					lock on the row in sys.sysconglomerates.
	 *
	 * @return	A ConglomerateDescriptor describing the requested
	 *		conglomerate. Returns NULL if no such conglomerate.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConglomerateDescriptor	getConglomerateDescriptor(
						String indexName,
						SchemaDescriptor sd,
						boolean forUpdate)
						throws StandardException
	{
		ExecIndexRow			  keyRow2 = null;
		DataValueDescriptor		  nameOrderable;
		DataValueDescriptor		  schemaIDOrderable = null;
		TabInfo					  ti = coreInfo[SYSCONGLOMERATES_CORE_NUM];

		nameOrderable = dvf.getVarcharDataValue(indexName);
		schemaIDOrderable = getValueAsDVD(sd.getUUID());

		/* Set up the start/stop position for the scan */
		keyRow2 = exFactory.getIndexableRow(2);
		keyRow2.setColumn(1, nameOrderable);
		keyRow2.setColumn(2, schemaIDOrderable);

		return (ConglomerateDescriptor)
					getDescriptorViaIndex(
						SYSCONGLOMERATESRowFactory.SYSCONGLOMERATES_INDEX2_ID,
						keyRow2,
						(ScanQualifier [][]) null,
						ti,
						(TupleDescriptor) null,
						(List) null,
						forUpdate);
	}
									
	/**
	 * Drops a conglomerate descriptor
	 *
	 * @param conglomerate	The ConglomerateDescriptor for the conglomerate
	 * @param tc		TransactionController for the transaction
	 *
	 * @return	Nothing.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void dropConglomerateDescriptor(
						ConglomerateDescriptor conglomerate,
						TransactionController tc)
						throws StandardException
	{
		ExecIndexRow			keyRow2 = null;
		DataValueDescriptor		nameOrderable;
		DataValueDescriptor		schemaIDOrderable = null;
		TabInfo					ti = coreInfo[SYSCONGLOMERATES_CORE_NUM];

		nameOrderable = dvf.getVarcharDataValue(conglomerate.getConglomerateName());
		schemaIDOrderable = getValueAsDVD(conglomerate.getSchemaID());

		/* Set up the start/stop position for the scan */
		keyRow2 = (ExecIndexRow) exFactory.getIndexableRow(2);
		keyRow2.setColumn(1, nameOrderable);
		keyRow2.setColumn(2, schemaIDOrderable);

		ti.deleteRow( tc, keyRow2, SYSCONGLOMERATESRowFactory.SYSCONGLOMERATES_INDEX2_ID );

	}
 
	/**
	 * Drops all conglomerates associated with a table.
	 *
	 * @param td		The TableDescriptor of the table 
	 * @param tc		TransactionController for the transaction
	 *
	 * @return	Nothing.
	 *
	 * @exception StandardException		Thrown on failure
	 */

	public void dropAllConglomerateDescriptors(
						TableDescriptor td,
						TransactionController tc)
					throws StandardException
	{		
		ExecIndexRow			keyRow3 = null;
		DataValueDescriptor		tableIDOrderable;
		TabInfo					ti = coreInfo[SYSCONGLOMERATES_CORE_NUM];

		/* Use tableIDOrderable in both start 
		 * and stop position for index 3 scan. 
		 */
		tableIDOrderable = getValueAsDVD(td.getUUID());

		/* Set up the start/stop position for the scan */
		keyRow3 = (ExecIndexRow) exFactory.getIndexableRow(1);
		keyRow3.setColumn(1, tableIDOrderable);


		ti.deleteRow( tc, keyRow3, SYSCONGLOMERATESRowFactory.SYSCONGLOMERATES_INDEX3_ID );
	}

	/**
	 * Update the conglomerateNumber for a ConglomerateDescriptor.
	 * This is useful, in 1.3, when doing a bulkInsert into an 
	 * empty table where we insert into a new conglomerate.
	 * (This will go away in 1.4.)
	 *
	 * @param cd					The ConglomerateDescriptor
	 * @param conglomerateNumber	The new conglomerate number
	 * @param tc					The TransactionController to use
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void updateConglomerateDescriptor(ConglomerateDescriptor cd,
											 long conglomerateNumber,
											 TransactionController tc)
		throws StandardException
	{
		ConglomerateDescriptor[] cds = new ConglomerateDescriptor[1];
		cds[0] = cd;
		updateConglomerateDescriptor(cds, conglomerateNumber, tc);
	}

	/**
	 * Update the conglomerateNumber for an array of ConglomerateDescriptors.
	 * In case of more than one ConglomerateDescriptor, they are for duplicate
	 * indexes sharing one conglomerate.
	 * This is useful, in 1.3, when doing a bulkInsert into an 
	 * empty table where we insert into a new conglomerate.
	 * (This will go away in 1.4.)
	 *
	 * @param cds					The array of ConglomerateDescriptors
	 * @param conglomerateNumber	The new conglomerate number
	 * @param tc					The TransactionController to use
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void updateConglomerateDescriptor(ConglomerateDescriptor[] cds,
											 long conglomerateNumber,
											 TransactionController tc)
		throws StandardException
	{
		ExecIndexRow				keyRow1 = null;
		ExecRow[]    				rows = new ExecRow[cds.length];
		DataValueDescriptor			conglomIDOrderable;
		TabInfo						ti = coreInfo[SYSCONGLOMERATES_CORE_NUM];
		SYSCONGLOMERATESRowFactory  rf = (SYSCONGLOMERATESRowFactory) ti.getCatalogRowFactory();

		/* Use conglomIDOrderable in both start 
		 * and stop position for index 1 scan. 
		 */
		conglomIDOrderable = getValueAsDVD(cds[0].getUUID());

		/* Set up the start/stop position for the scan */
		keyRow1 = (ExecIndexRow) exFactory.getIndexableRow(1);
		keyRow1.setColumn(1, conglomIDOrderable);

		for (int i = 0; i < cds.length; i++)
		{
			cds[i].setConglomerateNumber(conglomerateNumber);
			// build the row to be stuffed into SYSCONGLOMERATES. 
			rows[i] = rf.makeRow(cds[i], null);
		}

		// update row in catalog (no indexes)
		boolean[] bArray = new boolean[3];
		for (int index = 0; index < 3; index++)
		{
			bArray[index] = false;
		}
		ti.updateRow(keyRow1, rows,
					 SYSCONGLOMERATESRowFactory.SYSCONGLOMERATES_INDEX1_ID,
					 bArray,
					 (int[])null,
					 tc);
	}

	
	/**
	 * Gets a list of the dependency descriptors for the given dependent's id.
	 *
	 * @param dependentID		The ID of the dependent we're interested in
	 *
	 * @return	List			Returns a list of DependencyDescriptors. 
	 *							Returns an empty List if no stored dependencies for the
	 *							dependent's ID.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public List getDependentsDescriptorList(String dependentID)
		throws StandardException
	{
		List					ddlList = newSList();
		DataValueDescriptor		dependentIDOrderable;
		TabInfo					ti = getNonCoreTI(SYSDEPENDS_CATALOG_NUM);

		/* Use dependentIDOrderable in both start and stop positions for scan */
		dependentIDOrderable = dvf.getCharDataValue(dependentID);

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = exFactory.getIndexableRow(1);
		keyRow.setColumn(1, dependentIDOrderable);

		getDescriptorViaIndex(
			SYSDEPENDSRowFactory.SYSDEPENDS_INDEX1_ID,
			keyRow,
			(ScanQualifier [][]) null,
			ti,
			(TupleDescriptor) null,
			ddlList,
			false);
				
		return ddlList;
	}

	/**
	 * Gets a list of the dependency descriptors for the given provider's id.
	 *
	 * @param dependentID		The ID of the provider we're interested in
	 *
	 * @return	List			Returns a list of DependencyDescriptors. 
	 *							Returns an empty List if no stored dependencies for the
	 *							provider's ID.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public List getProvidersDescriptorList(String providerID)
		throws StandardException
	{
		List					ddlList = newSList();
		DataValueDescriptor		providerIDOrderable;
		TabInfo					ti = getNonCoreTI(SYSDEPENDS_CATALOG_NUM);

		/* Use providerIDOrderable in both start and stop positions for scan */
		providerIDOrderable = dvf.getCharDataValue(providerID);

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = exFactory.getIndexableRow(1);
		keyRow.setColumn(1, providerIDOrderable);

		getDescriptorViaIndex(
			SYSDEPENDSRowFactory.SYSDEPENDS_INDEX2_ID,
			keyRow,
			(ScanQualifier [][]) null,
			ti,
			(TupleDescriptor) null,
			ddlList,
			false);



		return ddlList;
	}

	/**
	 * Build and return an List with DependencyDescriptors for
	 * all of the stored dependencies.  
	 * This is useful for consistency checking.
	 *
	 * @return List		List of all DependencyDescriptors.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public List getAllDependencyDescriptorsList()
				throws StandardException
	{
		ScanController			 	scanController;
		TransactionController	  	tc;
		ExecRow					  	outRow;
		ExecRow					 	templateRow;
		List						ddl = newSList();
		TabInfo						ti = getNonCoreTI(SYSDEPENDS_CATALOG_NUM);
		SYSDEPENDSRowFactory		rf = (SYSDEPENDSRowFactory) ti.getCatalogRowFactory();


		// Get the current transaction controller
		tc = getTransactionCompile();

		outRow = rf.makeEmptyRow();

		scanController = tc.openScan(
			ti.getHeapConglomerate(),  // conglomerate to open
			false, // don't hold open across commit
			0, // for read
            TransactionController.MODE_TABLE,   // scans entire table.
            TransactionController.ISOLATION_REPEATABLE_READ,
			(FormatableBitSet) null,                     // all fields as objects
			null,   // start position - first row
            ScanController.GE,      // startSearchOperation
			null,
			null,   // stop position - through last row
            ScanController.GT);     // stopSearchOperation

		while (scanController.fetchNext(outRow.getRowArray()))
        {
			DependencyDescriptor		dependencyDescriptor;

			dependencyDescriptor = (DependencyDescriptor)
				     rf.buildDescriptor(outRow,
												(TupleDescriptor) null,
												this);

			ddl.add(dependencyDescriptor);
        }

        scanController.close();

		return ddl;
	}

	/** 
	 * Drop a single dependency from the data dictionary.
	 * 
	 * @param dd	The DependencyDescriptor.
	 * @param tc	TransactionController for the transaction
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void dropStoredDependency(DependencyDescriptor dd,
									TransactionController tc )
				throws StandardException
	{
		ExecIndexRow			keyRow1 = null;
		UUID					dependentID = dd.getUUID();
		UUID					providerID = dd.getProviderID();
		DataValueDescriptor		dependentIDOrderable = getValueAsDVD(dependentID);
		TabInfo					ti = getNonCoreTI(SYSDEPENDS_CATALOG_NUM);

		/* Use dependentIDOrderable in both start 
		 * and stop position for index 1 scan. 
		 */
		keyRow1 = (ExecIndexRow) exFactory.getIndexableRow(1);
		keyRow1.setColumn(1, dependentIDOrderable);

		// only drop the rows which have this providerID
		TupleFilter				filter = new DropDependencyFilter( providerID );

		ti.deleteRows( tc,
					   keyRow1,				// start row
					   ScanController.GE,
					   null,                //qualifier
					   filter,				// filter on base row
					   keyRow1,				// stop row
					   ScanController.GT,
					   SYSDEPENDSRowFactory.SYSDEPENDS_INDEX1_ID );

	}


	/** 
	 * Remove all of the stored dependencies for a given dependent's ID 
	 * from the data dictionary.
	 * 
	 * @param dependentsUUID	Dependent's uuid
	 * @param tc				TransactionController for the transaction
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void dropDependentsStoredDependencies(UUID dependentsUUID,
									   TransactionController tc) 
				throws StandardException	
	{
		ExecIndexRow			keyRow1 = null;
		DataValueDescriptor		dependentIDOrderable;
		TabInfo					ti = getNonCoreTI(SYSDEPENDS_CATALOG_NUM);

		/* Use dependentIDOrderable in both start 
		 * and stop position for index 1 scan. 
		 */
		dependentIDOrderable = getValueAsDVD(dependentsUUID);

		/* Set up the start/stop position for the scan */
		keyRow1 = (ExecIndexRow) exFactory.getIndexableRow(1);
		keyRow1.setColumn(1, dependentIDOrderable);

		ti.deleteRow( tc, keyRow1, SYSDEPENDSRowFactory.SYSDEPENDS_INDEX1_ID );

	}

	/**
	 * Get the UUID Factory.  (No need to make the UUIDFactory a module.)
	 *
	 * @return UUIDFactory	The UUID Factory for this DataDictionary.
	 */
	public UUIDFactory getUUIDFactory()
	{
		return uuidFactory;
	}

	/**
	 * Get a AliasDescriptor given its UUID.
	 *
	 * @param uuid	The UUID
	 *
	 *
	 * @return The AliasDescriptor for the alias.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public AliasDescriptor getAliasDescriptor(UUID uuid)
				throws StandardException
	{
		DataValueDescriptor		UUIDStringOrderable;
		SYSALIASESRowFactory	rf;
		TabInfo					ti = getNonCoreTI(SYSALIASES_CATALOG_NUM);

		rf = (SYSALIASESRowFactory) ti.getCatalogRowFactory();

		/* Use UUIDStringOrderable in both start and stop positions for scan */
		UUIDStringOrderable = dvf.getCharDataValue(uuid.toString());

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = exFactory.getIndexableRow(1);
		keyRow.setColumn(1, UUIDStringOrderable);

		return (AliasDescriptor)
					getDescriptorViaIndex(
						SYSALIASESRowFactory.SYSALIASES_INDEX2_ID,
						keyRow,
						(ScanQualifier [][]) null,
						ti,
						(TupleDescriptor) null,
						(List) null,
						false);
	}

	/**
	 * Get a AliasDescriptor by alias name and name space.
	 * NOTE: caller responsible for handling no match.
	 *
	   @param schemaId		schema identifier
	 * @param aliasName		The alias name.
	 * @param nameSpace		The alias type.
	 *
	 * @return AliasDescriptor	AliasDescriptor for the alias name and name space
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public AliasDescriptor getAliasDescriptor(String schemaId, String aliasName, char nameSpace)
			throws StandardException
	{
		DataValueDescriptor		  aliasNameOrderable;
		DataValueDescriptor		  nameSpaceOrderable;
		TabInfo					  ti = getNonCoreTI(SYSALIASES_CATALOG_NUM);
		SYSALIASESRowFactory	  rf = (SYSALIASESRowFactory) ti.getCatalogRowFactory();

		/* Use aliasNameOrderable and aliasTypeOrderable in both start 
		 * and stop position for scan. 
		 */
		aliasNameOrderable = dvf.getVarcharDataValue(aliasName);
		char[] charArray = new char[1];
		charArray[0] = nameSpace;
		nameSpaceOrderable = dvf.getCharDataValue(new String(charArray));

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = exFactory.getIndexableRow(3);
		keyRow.setColumn(1, dvf.getCharDataValue(schemaId));
		keyRow.setColumn(2, aliasNameOrderable);
		keyRow.setColumn(3, nameSpaceOrderable);

		return (AliasDescriptor)
					getDescriptorViaIndex(
						SYSALIASESRowFactory.SYSALIASES_INDEX1_ID,
						keyRow,
						(ScanQualifier [][]) null,
						ti,
						(TupleDescriptor) null,
						(List) null,
						false);
	}

	/**
		Get the list of routines matching the schema and routine name.
		While we only support a single alias for a given name,namespace just
		return a list of zero or one item.
	 */
	public java.util.List getRoutineList(String schemaID, String routineName, char nameSpace)
		throws StandardException {

		java.util.List list = new  java.util.ArrayList();

		AliasDescriptor ad = getAliasDescriptor(schemaID, routineName, nameSpace);
		if (ad != null) {
			list.add(ad);
		}
		return list;
	}

	/** 
	 * Drop a AliasDescriptor from the DataDictionary
	 *
	 * @param ad	The AliasDescriptor to drop
	 * @param tc	The TransactionController
	 *
	 * @return	Nothing.
	 *
	 * @exception StandardException		Thrown on failure
	 */

	public void dropAliasDescriptor(AliasDescriptor ad, 
									TransactionController tc)
			throws StandardException
	{	
		ExecIndexRow			keyRow1 = null;
		DataValueDescriptor		aliasNameOrderable;
		DataValueDescriptor		nameSpaceOrderable;
		TabInfo					ti = getNonCoreTI(SYSALIASES_CATALOG_NUM);

		/* Use aliasNameOrderable and nameSpaceOrderable in both start 
		 * and stop position for index 1 scan. 
		 */
		aliasNameOrderable = dvf.getVarcharDataValue(ad.getDescriptorName());
		char[] charArray = new char[1];
		charArray[0] = ad.getNameSpace();
		nameSpaceOrderable = dvf.getCharDataValue(new String(charArray));

		/* Set up the start/stop position for the scan */
		keyRow1 = (ExecIndexRow) exFactory.getIndexableRow(3);
		keyRow1.setColumn(1, dvf.getCharDataValue(ad.getSchemaUUID().toString()));
		keyRow1.setColumn(2, aliasNameOrderable);
		keyRow1.setColumn(3, nameSpaceOrderable);

		ti.deleteRow( tc, keyRow1, SYSALIASESRowFactory.SYSALIASES_INDEX1_ID );

	}

	//
	// class implementation
	//

	/**
	 *	Initialize system catalogs. This is where we perform upgrade. It is our
	 *	pious hope that we won't ever have to upgrade the core catalogs, other than
	 *	to add fields inside Formatable columns in these catalogs.
	 *
	 *	If we do have to upgrade the core catalogs, then we may need to move the
	 *	loadCatalog calls into the upgrade machinery. It's do-able, just not pretty.
	 *
	 *
	 *	@param	tc		TransactionController
	 *	@param	ddg		DataDescriptorGenerator
	 *
	 * 	@exception StandardException		Thrown on error
	 */
	protected void loadDictionaryTables(TransactionController tc,
										DataDescriptorGenerator ddg,
										Properties startParams)
		throws StandardException
	{        
		// load the core catalogs first
		loadCatalogs(ddg, coreInfo);

		dictionaryVersion = (DD_Version)tc.getProperty(
											DataDictionary.CORE_DATA_DICTIONARY_VERSION);

		softwareVersion.upgradeIfNeeded(dictionaryVersion, tc, startParams);
	}

	/**
	 * Initialize indices for an array of catalogs
	 *
	 *	@param	ddg		DataDescriptorGenerator
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void loadCatalogs(DataDescriptorGenerator ddg, TabInfo[] catalogArray)
		throws StandardException
	{
		int			ictr;
		int			numIndexes;
		int			indexCtr;
		TabInfo		catalog;
		int			catalogCount = catalogArray.length;

		/* Initialize the various variables associated with index scans of these catalogs */
		for (ictr = 0; ictr < catalogCount; ictr++)
		{
			// NOTE: This only works for core catalogs, which are initialized
			// up front.
			catalog = catalogArray[ictr];

			numIndexes = catalog.getNumberOfIndexes();

			if (numIndexes > 0)
			{
				for (indexCtr = 0; indexCtr < numIndexes; indexCtr++)
				{
					initSystemIndexVariables(ddg, catalog, indexCtr);	
				}
			}
		}

	}

	/*
	** Methods related to create
	*/

	/**
		Create all the required dictionary tables. Any classes that extend this class
		and need to create new tables shoudl override this method, and then
		call this method as the first action in the new method, e.g.
		<PRE>
		protected Configuration createDictionaryTables(Configuration cfg, TransactionController tc,
				DataDescriptorGenerator ddg)
				throws StandardException
		{
			super.createDictionaryTables(params, tc, ddg);

			...
		}
		</PRE>

		@exception StandardException Standard Cloudscape error policy
	*/
	protected void createDictionaryTables(Properties params, TransactionController tc,
			DataDescriptorGenerator ddg)
					throws StandardException
	{
        /*
		** Create a new schema descriptor -- with no args
		** creates the system schema descriptor in which 
		** all tables reside (SYS)
		*/
		systemSchemaDesc =
          newSystemSchemaDesc(
              convertIdToLower ? 
			      StringUtil.SQLToLowerCase(SchemaDescriptor.STD_SYSTEM_SCHEMA_NAME) : 
                  SchemaDescriptor.STD_SYSTEM_SCHEMA_NAME, 
                  SchemaDescriptor.SYSTEM_SCHEMA_UUID);

		/* Create the core tables and generate the UUIDs for their
		 * heaps (before creating the indexes).
		 * RESOLVE - This loop will eventually drive all of the
		 * work for creating the core tables.
		 */
		for (int coreCtr = 0; coreCtr < NUM_CORE; coreCtr++)
		{
			TabInfo	ti = coreInfo[coreCtr];

			Properties	heapProperties = ti.getCreateHeapProperties();

			ti.setHeapConglomerate(
				createConglomerate(
							ti.getTableName(),
							tc,
							ti.getCatalogRowFactory().makeEmptyRow(),
							heapProperties
							)
				);

			// bootstrap indexes on core tables before bootstraping the tables themselves
			if (coreInfo[coreCtr].getNumberOfIndexes() > 0)
			{
				bootStrapSystemIndexes(systemSchemaDesc, tc, ddg, ti);
			}
		}

		// bootstrap the core tables into the data dictionary
		for ( int ictr = 0; ictr < NUM_CORE; ictr++ )
		{
			/* RESOLVE - need to do something with COLUMNTYPE in following table creating code */
			TabInfo			ti = coreInfo[ictr];

			addSystemTableToDictionary(ti, systemSchemaDesc, tc, ddg);
		}

		// Add the bootstrap information to the configuration
		params.put(CFG_SYSTABLES_ID, 
				   Long.toString(
						coreInfo[SYSTABLES_CORE_NUM].getHeapConglomerate()));
		params.put(CFG_SYSTABLES_INDEX1_ID, 
				   Long.toString(
						coreInfo[SYSTABLES_CORE_NUM].getIndexConglomerate(
							((SYSTABLESRowFactory) coreInfo[SYSTABLES_CORE_NUM].
								getCatalogRowFactory()).SYSTABLES_INDEX1_ID)));
		params.put(CFG_SYSTABLES_INDEX2_ID, 
				   Long.toString(
						coreInfo[SYSTABLES_CORE_NUM].getIndexConglomerate(
							((SYSTABLESRowFactory) coreInfo[SYSTABLES_CORE_NUM].
								getCatalogRowFactory()).SYSTABLES_INDEX2_ID)));

		params.put(CFG_SYSCOLUMNS_ID, 
				   Long.toString(
						coreInfo[SYSCOLUMNS_CORE_NUM].getHeapConglomerate()));
		params.put(CFG_SYSCOLUMNS_INDEX1_ID, 
					Long.toString(
						coreInfo[SYSCOLUMNS_CORE_NUM].getIndexConglomerate(
							((SYSCOLUMNSRowFactory) coreInfo[SYSCOLUMNS_CORE_NUM].
								getCatalogRowFactory()).SYSCOLUMNS_INDEX1_ID)));
		params.put(CFG_SYSCOLUMNS_INDEX2_ID, 
					Long.toString(
						coreInfo[SYSCOLUMNS_CORE_NUM].getIndexConglomerate(
							((SYSCOLUMNSRowFactory) coreInfo[SYSCOLUMNS_CORE_NUM].
								getCatalogRowFactory()).SYSCOLUMNS_INDEX2_ID)));

		params.put(CFG_SYSCONGLOMERATES_ID, 
				   Long.toString(
						coreInfo[SYSCONGLOMERATES_CORE_NUM].getHeapConglomerate()));
		params.put(CFG_SYSCONGLOMERATES_INDEX1_ID, 
					Long.toString(
						coreInfo[SYSCONGLOMERATES_CORE_NUM].getIndexConglomerate(
							((SYSCONGLOMERATESRowFactory) coreInfo[SYSCONGLOMERATES_CORE_NUM].
								getCatalogRowFactory()).SYSCONGLOMERATES_INDEX1_ID)));
		params.put(CFG_SYSCONGLOMERATES_INDEX2_ID, 
					Long.toString(
						coreInfo[SYSCONGLOMERATES_CORE_NUM].getIndexConglomerate(
							((SYSCONGLOMERATESRowFactory) coreInfo[SYSCONGLOMERATES_CORE_NUM].
								getCatalogRowFactory()).SYSCONGLOMERATES_INDEX2_ID)));
		params.put(CFG_SYSCONGLOMERATES_INDEX3_ID, 
					Long.toString(
						coreInfo[SYSCONGLOMERATES_CORE_NUM].getIndexConglomerate(
							((SYSCONGLOMERATESRowFactory) coreInfo[SYSCONGLOMERATES_CORE_NUM].
								getCatalogRowFactory()).SYSCONGLOMERATES_INDEX3_ID)));

		params.put(CFG_SYSSCHEMAS_ID, 
				   Long.toString(
						coreInfo[SYSSCHEMAS_CORE_NUM].getHeapConglomerate()));
		params.put(CFG_SYSSCHEMAS_INDEX1_ID, 
					Long.toString(
						coreInfo[SYSSCHEMAS_CORE_NUM].getIndexConglomerate(
							((SYSSCHEMASRowFactory) coreInfo[SYSSCHEMAS_CORE_NUM].
								getCatalogRowFactory()).SYSSCHEMAS_INDEX1_ID)));
		params.put(CFG_SYSSCHEMAS_INDEX2_ID, 
					Long.toString(
						coreInfo[SYSSCHEMAS_CORE_NUM].getIndexConglomerate(
							((SYSSCHEMASRowFactory) coreInfo[SYSSCHEMAS_CORE_NUM].
								getCatalogRowFactory()).SYSSCHEMAS_INDEX2_ID)));

		//Add the SYSIBM Schema
		sysIBMSchemaDesc = 
            addSystemSchema(
                SchemaDescriptor.IBM_SYSTEM_SCHEMA_NAME,
                SchemaDescriptor.SYSIBM_SCHEMA_UUID, tc);

		/* Create the non-core tables and generate the UUIDs for their
		 * heaps (before creating the indexes).
		 * RESOLVE - This loop will eventually drive all of the
		 * work for creating the non-core tables.
		 */
		for (int noncoreCtr = 0; noncoreCtr < NUM_NONCORE; noncoreCtr++)
		{
			int catalogNumber = noncoreCtr + NUM_CORE;
			boolean isDummy = (catalogNumber == SYSDUMMY1_CATALOG_NUM);

			TabInfo ti = getNonCoreTIByNumber(catalogNumber);

			makeCatalog(ti, isDummy ? sysIBMSchemaDesc : systemSchemaDesc, tc );

			if (isDummy)
				populateSYSDUMMY1(tc);

			// Clear the table entry for this non-core table,
			// to allow it to be garbage-collected. The idea
			// is that a running database might never need to
			// reference a non-core table after it was created.
			clearNoncoreTable(noncoreCtr);
		}

		//Add ths System Schema
		addDescriptor(
                systemSchemaDesc, null, SYSSCHEMAS_CATALOG_NUM, false, tc);


        // Add the following system Schema's to be compatible with DB2, 
        // currently Cloudscape does not use them, but by creating them as
        // system schema's it will insure applications can't create them,
        // drop them, or create objects in them.  This set includes:
        //     SYSCAT
        //     SYSFUN
        //     SYSPROC
        //     SYSSTAT
        //     NULLID

		//Add the SYSCAT Schema 
        addSystemSchema(
            SchemaDescriptor.IBM_SYSTEM_CAT_SCHEMA_NAME,
            SchemaDescriptor.SYSCAT_SCHEMA_UUID, tc);

		//Add the SYSFUN Schema
        addSystemSchema(
            SchemaDescriptor.IBM_SYSTEM_FUN_SCHEMA_NAME,
            SchemaDescriptor.SYSFUN_SCHEMA_UUID, tc);

		//Add the SYSPROC Schema
        addSystemSchema(
            SchemaDescriptor.IBM_SYSTEM_PROC_SCHEMA_NAME,
            SchemaDescriptor.SYSPROC_SCHEMA_UUID, tc);

		//Add the SYSSTAT Schema
        addSystemSchema(
            SchemaDescriptor.IBM_SYSTEM_STAT_SCHEMA_NAME,
            SchemaDescriptor.SYSSTAT_SCHEMA_UUID, tc);

		//Add the NULLID Schema
        addSystemSchema(
            SchemaDescriptor.IBM_SYSTEM_NULLID_SCHEMA_NAME,
            SchemaDescriptor.NULLID_SCHEMA_UUID, tc);

		//Add the SQLJ Schema
        addSystemSchema(
            SchemaDescriptor.STD_SQLJ_SCHEMA_NAME,
            SchemaDescriptor.SQLJ_SCHEMA_UUID, tc);

		//Add the SYSCS_DIAG Schema
        addSystemSchema(
            SchemaDescriptor.STD_SYSTEM_DIAG_SCHEMA_NAME,
            SchemaDescriptor.SYSCS_DIAG_SCHEMA_UUID, tc);

		//Add the SYSCS_UTIL Schema
        addSystemSchema(
            SchemaDescriptor.STD_SYSTEM_UTIL_SCHEMA_NAME,
            SchemaDescriptor.SYSCS_UTIL_SCHEMA_UUID, tc);

  		//Add the APP schema
  		SchemaDescriptor appSchemaDesc = new SchemaDescriptor(this,
                                        SchemaDescriptor.STD_DEFAULT_SCHEMA_NAME,
                                        SchemaDescriptor.DEFAULT_USER_NAME,
                                        uuidFactory.recreateUUID( SchemaDescriptor.DEFAULT_SCHEMA_UUID),
                                        false);
  
  		addDescriptor(appSchemaDesc, null, SYSSCHEMAS_CATALOG_NUM, false, tc);
	}

    /**
     * Add a system schema to the database.
     * <p>
     *
     * @param schema_name   name of the schema to add.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    private SchemaDescriptor addSystemSchema(
    String                  schema_name,
    String                  schema_uuid,
    TransactionController   tc)
		throws StandardException
    {
		// create the descriptor
		SchemaDescriptor schema_desc = 
            new SchemaDescriptor(
                this, 
                convertIdToLower ? schema_name.toLowerCase() : schema_name, 
                SchemaDescriptor.SA_USER_NAME,
                uuidFactory.recreateUUID(schema_uuid),
                true);

        // add it to the catalog.
		addDescriptor(schema_desc, null, SYSSCHEMAS_CATALOG_NUM, false, tc);

        return(schema_desc);
    }

	/** called by the upgrade code (dd_xena etc) to add a new system catalog.
	 * 
	 * @param 	tc 				TransactionController to use.
	 * @param 	catalogNumber	catalogNumber
	 */
	protected void upgradeMakeCatalog(TransactionController tc, int catalogNumber)
		throws StandardException
	{
		TabInfo ti;
		if (catalogNumber >= NUM_CORE)
			ti = getNonCoreTIByNumber(catalogNumber);
		else
			ti = coreInfo[catalogNumber];
		
		makeCatalog(ti, (catalogNumber == SYSDUMMY1_CATALOG_NUM) ? getSysIBMSchemaDescriptor() :
								getSystemSchemaDescriptor(), tc);
	}

	/**
	 *	The dirty work of creating a catalog.
	 *
	 *	@param	ti			TabInfo describing catalog to create.
	 *	@param	sd			Schema to create catalogs in.
	 *	@param	tc			Transaction context.
	 *
	 *	@exception StandardException Standard Cloudscape error policy
	 */
	public	void	makeCatalog( TabInfo					ti,
								 SchemaDescriptor			sd,
								 TransactionController 		tc )
					throws StandardException
	{
		DataDescriptorGenerator ddg = getDataDescriptorGenerator();

		Properties	heapProperties = ti.getCreateHeapProperties();
		ti.setHeapConglomerate(
			createConglomerate(
				ti.getTableName(),
				tc,
				ti.getCatalogRowFactory().makeEmptyRow(),
				heapProperties
				)
			);

		// bootstrap indexes on core tables before bootstrapping the tables themselves
		if (ti.getNumberOfIndexes() > 0)
		{
			bootStrapSystemIndexes(sd, tc, ddg, ti);
		}

		addSystemTableToDictionary(ti, sd, tc, ddg);
	}							 
	/**
	  *	Upgrade an existing catalog by  setting the nullability
	  *
	  *	@param	columnNumber			The column to change
	  *	@param	nullability				true if nullable 
	  *	@param	tc						Transaction controller
	  *
	  *	@exception StandardException Standard Cloudscape error policy
	  */
	public void upgrade_setNullability(CatalogRowFactory rowFactory,
									   int columnNumber,
									   boolean nullability,
									   TransactionController tc)
		throws StandardException
	{
		SystemColumn		theColumn;
		SystemColumn[]		columns = rowFactory.buildColumnList();
		SchemaDescriptor	sd = getSystemSchemaDescriptor();
		String columnName;

		TableDescriptor td = getTableDescriptor(rowFactory.getCatalogName(), sd);

		theColumn = columns[columnNumber - 1];	// from 1 to 0 based
		ColumnDescriptor cd = makeColumnDescriptor(theColumn, td );
		columnName = cd.getColumnName();
		cd.getType().setNullability(nullability);
		int[] columnNameColArray = new int[1];
		columnNameColArray[0] = SYSCOLUMNSRowFactory.SYSCOLUMNS_COLUMNDATATYPE ;
		updateColumnDescriptor(cd,
								td.getUUID(),
								columnName,
								columnNameColArray, 
								tc,
								true);

	}

  
	/**
	  *	Upgrade an existing catalog by adding columns.
	  *
	  *	@param	rowFactory				Associated with this catalog.
	  *	@param	newColumnIDs			Array of 1-based column ids.
	  *	@param	tc						Transaction controller
	  *
	  *	@exception StandardException Standard Cloudscape error policy
	  */
	public	void	upgrade_addColumns( CatalogRowFactory		rowFactory,
										int[]					newColumnIDs,
										TransactionController	tc )
					throws StandardException
	{
		int					columnID;
		SystemColumn		currentColumn;
		ColumnDescriptor	cd;

		SystemColumn[]		columns = rowFactory.buildColumnList();
		ExecRow				templateRow = rowFactory.makeEmptyRow();
		int					columnCount = newColumnIDs.length;
		SchemaDescriptor	sd = getSystemSchemaDescriptor();
		TableDescriptor		td;
		long				conglomID;

		// Special case when adding a column to systables or syscolumns, 
		// since we can't go to systables/syscolumns to get the 
		// table/column descriptor until after we add and populate the new column.
		if (rowFactory instanceof SYSTABLESRowFactory)
		{
			td = dataDescriptorGenerator.newTableDescriptor(
						"SYSTABLES",
						sd,
						TableDescriptor.BASE_TABLE_TYPE,
						TableDescriptor.ROW_LOCK_GRANULARITY);
			td.setUUID(getUUIDForCoreTable("SYSTABLES", sd.getUUID().toString(), tc));
			conglomID = coreInfo[SYSTABLES_CORE_NUM].getHeapConglomerate();
		}
		else if (rowFactory instanceof SYSCOLUMNSRowFactory)
		{
			td = dataDescriptorGenerator.newTableDescriptor(
						"SYSCOLUMNS",
						sd,
						TableDescriptor.BASE_TABLE_TYPE,
						TableDescriptor.ROW_LOCK_GRANULARITY);
			td.setUUID(getUUIDForCoreTable("SYSCOLUMNS", sd.getUUID().toString(), tc));
			conglomID = coreInfo[SYSCOLUMNS_CORE_NUM].getHeapConglomerate();
		}
		else
		{
			td = getTableDescriptor( rowFactory.getCatalogName(), sd );
			conglomID = td.getHeapConglomerateId();
		}

		widenConglomerate( templateRow, newColumnIDs, conglomID, tc );


		ColumnDescriptor[] cdArray = new ColumnDescriptor[columnCount];
		for ( int ix = 0; ix < columnCount; ix++ )
		{
			columnID = newColumnIDs[ix];
			currentColumn = columns[ columnID - 1 ];	// from 1 to 0 based

			cdArray[ix] = makeColumnDescriptor( currentColumn, td );
		}
		addDescriptorArray(cdArray, td, SYSCOLUMNS_CATALOG_NUM, false, tc);

	}

	/**
	  *	Add invisible columns to an existing system catalog
	  *
	  *	@param	rowFactory				Associated with this catalog.
	  *	@param	newColumnIDs			Array of 1-based column ids.
	  *	@param	tc						Transaction controller
	  *
	  *	@exception StandardException Standard Cloudscape error policy
	  */
	public	void	upgrade_addInvisibleColumns
	(
		CatalogRowFactory		rowFactory,
		int[]					newColumnIDs,
		TransactionController	tc
    )
		throws StandardException
	{
		ExecRow				templateRow = rowFactory.makeEmptyRow();
		SchemaDescriptor	sd = getSystemSchemaDescriptor( );
		long				conglomID = getTableDescriptor( rowFactory.getCatalogName(), sd ).getHeapConglomerateId();

		widenConglomerate( templateRow, newColumnIDs, conglomID, tc );
	}


	/**
	  *	Adds columns to the conglomerate underlying a system table.
	  *
	  *	@param	templateRow				Ultimate shape of base row of table
	  *	@param	newColumnIDs			Array of 1-based column ids
	  *	@param	conglomID				heap id
	  *	@param	tc						Transaction controller
	  *
	  *	@exception StandardException Standard Cloudscape error policy
	  */
	private	void	widenConglomerate
	(
		ExecRow					templateRow,
		int[]					newColumnIDs,
		long					conglomID,
		TransactionController	tc
    )
		throws StandardException
	{
		int					columnCount = newColumnIDs.length;

		for ( int ix = 0; ix < columnCount; ix++ )
		{
			int columnID = newColumnIDs[ix];
			int storablePosition = columnID - 1;			// from 1 to 0 based

			tc.addColumnToConglomerate( conglomID,
										storablePosition,
										templateRow.getColumn( columnID) );
		}

	}


	/**
	  *	Code to add an index to a catalog during upgrade.
	  *
	  *	@param	tc						transaction controller
	  *	@param	ti						information on the catalog that's having a new index added
	  *	@param	indexNumber				0-based index number
	  *	@param	heapConglomerateNumber	what it is
	  *
	  * @return The conglomerate number of the new index.
	  *
	  * @exception StandardException		Thrown on failure
	  */
	public	long	upgrade_makeOneIndex
	(
		TransactionController	tc,
		TabInfo					ti,
		int						indexNumber,
		long					heapConglomerateNumber
    )
		throws StandardException
	{
		SchemaDescriptor		sd = getSystemSchemaDescriptor( );
		DataDescriptorGenerator ddg = getDataDescriptorGenerator();
		long					indexConglomerateNumber;

		ConglomerateDescriptor	conglomerateDescriptor = bootstrapOneIndex
			( sd, tc, ddg, ti, indexNumber, heapConglomerateNumber );

		indexConglomerateNumber = conglomerateDescriptor.getConglomerateNumber();

		addDescriptor(conglomerateDescriptor, sd, 
					  SYSCONGLOMERATES_CATALOG_NUM, false, tc);
					  
		return indexConglomerateNumber;
	}	  


	/**
	 * Get the UUID for the specified system table.  Prior
	 * to Plato, system tables did not have canonical UUIDs, so
	 * we need to scan systables to get the UUID when we
	 * are updating the core tables.
	 *
	 * @param tableName		Name of the table
	 * @param schemaUUID	UUID of schema
	 * @param tc			TransactionController to user
	 *
	 * @return UUID	The UUID of the core table.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	private UUID getUUIDForCoreTable(String tableName, 
									 String schemaUUID,
									 TransactionController tc)
				throws StandardException
	{
		ConglomerateController	heapCC;
		ExecIndexRow	  		indexRow1;
		ExecRow					row;
		DataValueDescriptor	    schemaIDOrderable;
		DataValueDescriptor		tableNameOrderable;
		ScanController			scanController;
		TabInfo					ti = coreInfo[SYSTABLES_CORE_NUM];
		CatalogRowFactory		rf = ti.getCatalogRowFactory();

		// We only want the 1st column from the heap
		row = exFactory.getValueRow(1);

		/* Use tableNameOrderable and schemaIdOrderable in both start 
		 * and stop position for scan. 
		 */
		tableNameOrderable = dvf.getVarcharDataValue(tableName);
		schemaIDOrderable = dvf.getCharDataValue(schemaUUID);

		/* Set up the start/stop position for the scan */
		ExecIndexRow keyRow = exFactory.getIndexableRow(2);
		keyRow.setColumn(1, tableNameOrderable);
		keyRow.setColumn(2, schemaIDOrderable);

		heapCC = tc.openConglomerate(
                ti.getHeapConglomerate(), false, 0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_REPEATABLE_READ);

		ExecRow indexTemplateRow = rf.buildEmptyIndexRow( SYSTABLESRowFactory.SYSTABLES_INDEX1_ID, heapCC.newRowLocationTemplate() );

		/* Scan the index and go to the data pages for qualifying rows to
		 * build the column descriptor.
		 */
		scanController = tc.openScan(
				ti.getIndexConglomerate(SYSTABLESRowFactory.SYSTABLES_INDEX1_ID),  // conglomerate to open
				false, // don't hold open across commit
				0, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_REPEATABLE_READ,
				(FormatableBitSet) null,         // all fields as objects
				keyRow.getRowArray(),   // start position - first row
				ScanController.GE,      // startSearchOperation
				(ScanQualifier[][]) null, //scanQualifier,
				keyRow.getRowArray(),   // stop position - through last row
				ScanController.GT);     // stopSearchOperation

        /* OK to fetch into the template row, 
         * since we won't be doing a next.
         */
		if (scanController.fetchNext(indexTemplateRow.getRowArray()))
		{
			RowLocation	baseRowLocation;


			baseRowLocation = (RowLocation)	indexTemplateRow.getColumn(
												indexTemplateRow.nColumns());
	
			/* 1st column is TABLEID (UUID - char(36)) */
			row.setColumn(SYSTABLESRowFactory.SYSTABLES_TABLEID, dvf.getCharDataValue((String) null));
			FormatableBitSet bi = new FormatableBitSet(1);
			bi.set(0);
			boolean base_row_exists = 
                heapCC.fetch(
                    baseRowLocation, row.getRowArray(), (FormatableBitSet) null);

            if (SanityManager.DEBUG)
            {
                // it can not be possible for heap row to disappear while 
                // holding scan cursor on index at ISOLATION_REPEATABLE_READ.
                SanityManager.ASSERT(base_row_exists, "base row not found");
            }
		}

        scanController.close();
		heapCC.close();

		return uuidFactory.recreateUUID(row.getColumn(1).toString());
	}
	 
 
	/**
	 * Initialize noncore columns to fixed values
	 *
	 * @param tc					The TransactionController for the transaction to do the
	 *								upgrade in.
	 * @param isCoreTable			true if it is a core table
	 * @param tableNum				the noncore table number
	 * @param columnsToUpdateSet	a bit set of columns to update.  ZERO BASED	
	 * @param replaceRow			an object array of Orderables for the new values
	 *
	 * @exception StandardException		Thrown on error
	 */
	void upgrade_initSystemTableCols(
    TransactionController 	tc, 
    boolean					isCoreTable,
    int 					tableNum, 
    FormatableBitSet 				columnsToUpdateSet, 
    DataValueDescriptor[] 	replaceRow
	)
		throws StandardException
	{
		
		TabInfo ti = (isCoreTable) ?  coreInfo[tableNum] :
										getNonCoreTIByNumber(tableNum);

		if (!isCoreTable)
			faultInTabInfo(ti);

		/* Scan the entire heap */
		ScanController sc = 
            tc.openScan(
                ti.getHeapConglomerate(),
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_REPEATABLE_READ,
                RowUtil.EMPTY_ROW_BITSET,
                (DataValueDescriptor[]) null,
                ScanController.NA,
                (Qualifier[][]) null,
                (DataValueDescriptor[]) null,
                ScanController.NA);

		while (sc.next())
		{
			/* Replace the column in the table */
			sc.replace(replaceRow, columnsToUpdateSet);
		}

		sc.close();
	}



	/*
 	*******************************************************************************
	*
	*	See RepBasicDataDictionary for sample code on how to create a system
	*	table.
	*
	*	What follows here is special code for the core catalogs. These are catalogs
	*	which have to exist before any other system tables are created.
	*
	*	Creating a core catalog consists of two steps: 1) creating all the infrastructure
	*	needed to make generic systemTableCreation work, 2) actually populating the
	*	Data Dictionary and core conglomerates with tuples.
	*
 	*******************************************************************************
	*/


	/**
	 *	Infrastructure work for indexes on catalogs.
	 *
	   @exception StandardException Standard Cloudscape error policy

	 */
	private void bootStrapSystemIndexes(
								  SchemaDescriptor sd, 
								  TransactionController tc,
								  DataDescriptorGenerator ddg,
								  TabInfo ti)
						throws StandardException
	{
		ConglomerateDescriptor[] cgd = new ConglomerateDescriptor[ti.getNumberOfIndexes()];

		/* Ordering problem with sysconglomerates.  We need to create 
		 * all of the conglomerates first before adding rows to 
		 * sysconglomerates.  (All of the conglomerates for sysconglomerates
		 * must be there before we can add to them.)
		 *
		 */
		for (int indexCtr = 0; indexCtr < ti.getNumberOfIndexes(); indexCtr++)
		{
			cgd[indexCtr] = bootstrapOneIndex( sd, tc, ddg, ti, indexCtr, ti.getHeapConglomerate() );
		}

		for (int indexCtr = 0; indexCtr < ti.getNumberOfIndexes(); indexCtr++)
		{
			addDescriptor(cgd[indexCtr], sd, 
						  SYSCONGLOMERATES_CATALOG_NUM, false, tc);
		}
	}

	/**
	 * @see DataDictionary#computeAutoincRowLocations
	 */
	public RowLocation[] computeAutoincRowLocations(TransactionController tc,
													TableDescriptor td)
				throws StandardException
	{
		int size;
		if (!(td.tableHasAutoincrement()))
			return null;

		size = td.getNumberOfColumns();
		RowLocation[] rla = new RowLocation[size];

		for (int i = 0; i < size; i++)
		{
			ColumnDescriptor cd = td.getColumnDescriptor(i + 1);
			if (cd.isAutoincrement())
				rla[i] = computeRowLocation(tc, td, cd.getColumnName());
		}
		return rla;
	}
    

	/**
	 * @see DataDictionary#getSetAutoincrementValue
	 */
	public NumberDataValue getSetAutoincrementValue(
											RowLocation rl, 
											TransactionController tc,
											boolean doUpdate,
											NumberDataValue newValue,
											boolean wait)
	       throws StandardException
	{

		FormatableBitSet columnToUpdate = new 
  			FormatableBitSet(SYSCOLUMNSRowFactory.SYSCOLUMNS_COLUMN_COUNT);
  		int columnNum = SYSCOLUMNSRowFactory.SYSCOLUMNS_AUTOINCREMENTVALUE;
		TabInfo ti = coreInfo[SYSCOLUMNS_CORE_NUM];
  		ConglomerateController heapCC = null;
		SYSCOLUMNSRowFactory	rf = (SYSCOLUMNSRowFactory) ti.getCatalogRowFactory();
		ExecRow row = rf.makeEmptyRow();

  		FormatableBitSet  columnToRead = new
  			FormatableBitSet(SYSCOLUMNSRowFactory.SYSCOLUMNS_COLUMN_COUNT);
		
		// FormatableBitSet is 0 based.
  		columnToRead.set(columnNum - 1); // current value.
		columnToRead.set(columnNum);     // start value.
		columnToRead.set(columnNum + 1); // increment value.

        try
        {
			/* if wait is true then we need to do a wait while trying to
			   open/fetch from the conglomerate. note we use wait both to
			   open as well as fetch from the conglomerate.
			*/
            heapCC = 
                tc.openConglomerate(
                    ti.getHeapConglomerate(), 
                    false,
                    (TransactionController.OPENMODE_FORUPDATE |
                     ((wait) ? 0 : TransactionController.OPENMODE_LOCK_NOWAIT)),
                    TransactionController.MODE_RECORD,
                    TransactionController.ISOLATION_REPEATABLE_READ);

            boolean baseRowExists = 
                heapCC.fetch(rl, row.getRowArray(), columnToRead, wait);

            columnToUpdate.set(columnNum - 1); // current value.

            // while the Row interface is 1 based.
            NumberDataValue currentAI = (NumberDataValue)row.getColumn(columnNum);
            long currentAIValue = currentAI.getLong();
            NumberDataValue increment = (NumberDataValue)row.getColumn(columnNum + 2);
            
            if (doUpdate)
            {
                // we increment and store the new value in SYSCOLUMNS
                currentAI = currentAI.plus(currentAI, increment, currentAI);
                row.setColumn(columnNum, currentAI);
                heapCC.replace(rl, row.getRowArray(), columnToUpdate);
            }
                
            // but we return the "currentAIValue"-- i.e the value before
            // incrementing it. 
            if (newValue != null)
            {
                // user has passed in an object; set the current value in there and
                // return it.
                newValue.setValue(currentAIValue);
                return newValue;
            }
            
            else
            {
                // reuse the object read from row.
                currentAI.setValue(currentAIValue);
                return currentAI;
            }
        }
        finally
        {
            if (heapCC != null)
                heapCC.close();
        }
	}

	private	ConglomerateDescriptor	bootstrapOneIndex
	(
		SchemaDescriptor		sd, 
		TransactionController	tc,
		DataDescriptorGenerator	ddg,
		TabInfo					ti,
		int						indexNumber,
		long					heapConglomerateNumber
    )
		throws StandardException
	{
		boolean						isUnique;
		ConglomerateController		cc;
		ExecRow						baseRow;
		ExecIndexRow				indexableRow;
		int							numColumns;
		long						conglomId;
		RowLocation					rl;
		CatalogRowFactory			rf = ti.getCatalogRowFactory();
		IndexRowGenerator			irg;
		ConglomerateDescriptor	conglomerateDescriptor;

		initSystemIndexVariables(ddg, ti, indexNumber);

		irg = ti.getIndexRowGenerator(indexNumber);

		numColumns = ti.getIndexColumnCount(indexNumber);

		/* Is the index unique */
		isUnique = ti.isIndexUnique(indexNumber);

		// create an index row template
		indexableRow = irg.getIndexRowTemplate();

		baseRow = rf.makeEmptyRow();

		// Get a RowLocation template
		cc = tc.openConglomerate(
			heapConglomerateNumber, false, 0,
            TransactionController.MODE_RECORD,
			TransactionController.ISOLATION_REPEATABLE_READ);

		rl = cc.newRowLocationTemplate();
		cc.close();

		// Get an index row based on the base row
		irg.getIndexRow(baseRow, rl, indexableRow, (FormatableBitSet) null);

		// Describe the properties of the index to the store using Properties
		// RESOLVE: The following properties assume a BTREE index.
		Properties	indexProperties = ti.getCreateIndexProperties(indexNumber);

		// Tell it the conglomerate id of the base table
		indexProperties.put(
			"baseConglomerateId",
			Long.toString( heapConglomerateNumber ) );

		// All indexes are unique because they contain the RowLocation.
		// The number of uniqueness columns must include the RowLocation
		// if the user did not specify a unique index.
		indexProperties.put("nUniqueColumns",
							Integer.toString(
								isUnique ? numColumns : numColumns + 1));

		// By convention, the row location column is the last column
		indexProperties.put("rowLocationColumn",
							Integer.toString(numColumns));

		// For now, all columns are key fields, including the RowLocation
		indexProperties.put("nKeyFields",
							Integer.toString(numColumns + 1));

		/* Create and add the conglomerate (index) */
		conglomId = tc.createConglomerate(
			"BTREE", // we're requesting an index conglomerate
			indexableRow.getRowArray(),
			null, //default sort order
			indexProperties, // default properties
			TransactionController.IS_DEFAULT); // not temporary

		conglomerateDescriptor = 
			ddg.newConglomerateDescriptor(conglomId,
										  rf.getIndexName(indexNumber),
										  true,
										  irg,
										  false,
										  rf.getCanonicalIndexUUID(indexNumber),
										  rf.getCanonicalTableUUID(),
										  sd.getUUID());
		ti.setIndexConglomerate( conglomerateDescriptor );

		return conglomerateDescriptor;
	}

	public void initSystemIndexVariables(DataDescriptorGenerator ddg,
										   TabInfo ti,
										   int indexNumber)
		throws StandardException
	{
		int					numCols = ti.getIndexColumnCount(indexNumber);
		int[]				baseColumnPositions = new int[numCols];
		CatalogRowFactory	rf = ti.getCatalogRowFactory();

		for (int colCtr = 0; colCtr < numCols; colCtr++)
		{
			baseColumnPositions[colCtr] = 
				ti.getBaseColumnPosition(indexNumber, colCtr);
		}

		boolean[] isAscending = new boolean[baseColumnPositions.length];
		for (int i = 0; i < baseColumnPositions.length; i++)
			isAscending[i] = true;

		// For now, assume that all index columns are ordered columns
		ti.setIndexRowGenerator(indexNumber, 
								new IndexRowGenerator(
											"BTREE", ti.isIndexUnique(indexNumber),
											baseColumnPositions,
											isAscending,
											baseColumnPositions.length));
	}

	/**
	 *	Populate SYSDUMMY1 table with a single row.
	 *
	 * @exception StandardException Standard Cloudscape error policy
	 */
	protected void populateSYSDUMMY1(
							TransactionController tc)
		throws StandardException
	{
		TabInfo						ti = getNonCoreTI(SYSDUMMY1_CATALOG_NUM);
		SYSDUMMY1RowFactory			rf = (SYSDUMMY1RowFactory) ti.getCatalogRowFactory();
		ExecRow row = ti.getCatalogRowFactory().makeRow(null, null);

		int insertRetCode = ti.insertRow(row, tc, true);
	}

	/**
	 * Clear all of the DataDictionary caches.
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException Standard Cloudscape error policy
	 */
	public void clearCaches() throws StandardException
	{
		nameTdCache.cleanAll();
		nameTdCache.ageOut();
		OIDTdCache.cleanAll();
		OIDTdCache.ageOut();
		if (spsNameCache != null)
		{
			//System.out.println("CLEARING SPS CACHE");
			spsNameCache.cleanAll();
			spsNameCache.ageOut();
			spsIdHash.clear();
			// spsTextHash.clear();
		}
	}

	/**
		Add the required entries to the data dictionary for a System table.
	*/

	private void addSystemTableToDictionary(TabInfo ti,
		                          SchemaDescriptor sd, 
								  TransactionController tc, 
								  DataDescriptorGenerator ddg)
						throws StandardException
	{
		CatalogRowFactory	crf = ti.getCatalogRowFactory();

		String				name = ti.getTableName();
		long				conglomId = ti.getHeapConglomerate();
		SystemColumn[]		columnList = crf.buildColumnList();
		UUID				heapUUID = crf.getCanonicalHeapUUID();
		String				heapName = crf.getCanonicalHeapName();
		TableDescriptor		td;
		UUID				toid;
		ColumnDescriptor	cd;
		int					columnCount;
		SystemColumn		column;

		// add table to the data dictionary

		columnCount = columnList.length;
		td = ddg.newTableDescriptor(name, sd, TableDescriptor.SYSTEM_TABLE_TYPE, 
								    TableDescriptor.ROW_LOCK_GRANULARITY);
		td.setUUID(crf.getCanonicalTableUUID());
		addDescriptor(td, sd, SYSTABLES_CATALOG_NUM,
					  false, tc);
		toid = td.getUUID();
	
		/* Add the conglomerate for the heap */
		ConglomerateDescriptor cgd = ddg.newConglomerateDescriptor(conglomId,
																	heapName,
																	false,
																	null,
																	false,
																	heapUUID,
																    toid,
																	sd.getUUID());

		addDescriptor(cgd, sd, SYSCONGLOMERATES_CATALOG_NUM, false, tc);

		/* Create the columns */
		ColumnDescriptor[] cdlArray = new ColumnDescriptor[columnCount];

		for (int columnNumber = 0; columnNumber < columnCount; columnNumber++)
		{
			column = columnList[columnNumber];

			if (SanityManager.DEBUG)
			{
				if (column == null)
				{
					SanityManager.THROWASSERT("column "+columnNumber+" for table "+ti.getTableName()+" is null");
				}
			}
			cdlArray[columnNumber] = makeColumnDescriptor( column, td );
		}
		addDescriptorArray(cdlArray, td, SYSCOLUMNS_CATALOG_NUM, false, tc);
		
		// now add the columns to the cdl of the table.
		ColumnDescriptorList cdl = td.getColumnDescriptorList();
		for (int i = 0; i < columnCount; i++)
			cdl.add(cdlArray[i]);
	}

	/**
	  *	Converts a SystemColumn to a ColumnDescriptor.
	  *
	  *	@param	column	a SystemColumn
	  *	@param	td		descriptor for table that column lives in
	  *
	  *	@return	a ColumnDes*criptor
	  *
	  *	@exception StandardException Standard Cloudscape error policy
	  */
	private	ColumnDescriptor	makeColumnDescriptor( SystemColumn		column,
													  TableDescriptor	td )
						throws StandardException
	{
		DataTypeDescriptor	typeDesc;
		TypeId	typeId;

		if (column.builtInType())
	    {
			typeId = TypeId.getBuiltInTypeId(column.getDataType());
		}
		else
		{

			typeId = TypeId.getUserDefinedTypeId(column.getDataType(), false);
		}

		typeDesc = new DataTypeDescriptor(
							   typeId,
							   column.getPrecision(),
							   column.getScale(),
							   column.getNullability(),
							   column.getMaxLength()
							   );

		//RESOLVEAUTOINCREMENT
		return new ColumnDescriptor
			(column.getName(), column.getID(), typeDesc, null, null, td,
			 (UUID) null, // No defaults yet for system columns
			 0, 0, false
			 );
	}


	/**
	 *	Create a conglomerate for a system table
	 *
	 *	@param name 		Name of new catalog.
	 *	@param tc			Transaction context.
	 *	@param rowTemplate	Template for rows for the new table
	 *  @param properties	Properties for createConglomerate
	 *
	 *	@return	Conglomerate id.

		@exception StandardException Standard Cloudscape error policy.
	 */
	private long createConglomerate(String name, TransactionController tc,
									ExecRow rowTemplate,
									Properties properties)
						throws StandardException
	{
		long				conglomId;

		conglomId = tc.createConglomerate(
			"heap", // we're requesting a heap conglomerate
			rowTemplate.getRowArray(), // row template
			null, // default sort order
			properties, // default properties
			TransactionController.IS_DEFAULT); // not temporary

		return conglomId;
	}

	/**
	  *	Converts a UUID to an DataValueDescriptor.
	  *
	  *	@return	the UUID converted to an DataValueDescriptor
	 *
	 * @exception StandardException		Thrown on error
	 */
	public DataValueDescriptor getValueAsDVD( UUID uuid ) throws StandardException
	{
		String	uuidString = uuid.toString();
		return 	dvf.getCharDataValue(uuidString);
	}

	/**
	  *	Initialize catalog information. This method is overridden by children.
	  * @exception StandardException		Thrown on error
	  */
	public	void	initializeCatalogInfo()
		throws StandardException
	{
		initializeCoreInfo();
		initializeNoncoreInfo();
	}

	/**
	 * Initialized the core info array.
	 */
	private void initializeCoreInfo()
		throws StandardException
	{
		TabInfo[] lcoreInfo = coreInfo = new TabInfo[NUM_CORE];

		UUIDFactory luuidFactory = uuidFactory;

		lcoreInfo[SYSTABLES_CORE_NUM] = 
			new TabInfoImpl(new SYSTABLESRowFactory(luuidFactory, exFactory, dvf, convertIdToLower));
		lcoreInfo[SYSCOLUMNS_CORE_NUM] = 
			new TabInfoImpl(new SYSCOLUMNSRowFactory(luuidFactory, exFactory, dvf, convertIdToLower));
		lcoreInfo[SYSCONGLOMERATES_CORE_NUM] = 
			new TabInfoImpl(new SYSCONGLOMERATESRowFactory(luuidFactory, exFactory, dvf, convertIdToLower));
		lcoreInfo[SYSSCHEMAS_CORE_NUM] = 
			new TabInfoImpl(new SYSSCHEMASRowFactory(luuidFactory, exFactory, dvf, convertIdToLower));
	}

	/**
	 * Initialized the noncore info array.
	 */
	private void initializeNoncoreInfo()
		throws StandardException
	{
		noncoreInfo = new TabInfo[NUM_NONCORE];
	}

 	/**
 	  *	Get the tabinfo of a system catalog. Paw through the tabinfo arrays looking for the tabinfo
 	  *	corresponding to this table name.
 	  *
	  * RESOLVE: This does not bother to fault in the TabInfo. It assumes it already
	  *          has been faulted in. This seems odd.
 	  *
	  *	@param	tableName	name of table to get the TabInfo for.
 	  *
	  *	@return	tabinfo corresponding to tablename
 	  *
 	  * @exception StandardException		Thrown on error
	  */
 	public	TabInfo	getTabInfo( String tableName ) throws StandardException
 	{
 		TabInfo		tabinfo;
 
 		for ( int ictr = 0; ictr < coreInfo.length; ictr++ )
 		{
 			tabinfo = coreInfo[ ictr ];
 			if ( tabinfo.getTableName().equals( tableName ) ) { return tabinfo; }
 		}
 
 		// Non-core tables are not pre-loaded. Translate the name
 		// to a number and look it up that way.
 		for (int ictr = 0; ictr < nonCoreNames.length; ictr++)
		{
 			if (nonCoreNames[ictr].equals(tableName))
			{
				return getNonCoreTI(ictr);
			}
		}

 		return	null;
	}

	/**
	 * Get the TransactionController to use, when not
	 * passed in as a parameter.  (This hides logic about
	 * whether or not we're at boot time in a single
	 * place.  NOTE:  There's no LCC at boot time.)
	 * NOTE: All <get> methods in the DD should call this method.
	 *
	 * @return TransactionController	The TC to use.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public TransactionController getTransactionCompile()
		throws StandardException
	{
		if (bootingTC != null)
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(booting, "booting is expected to be true");
			}
			return bootingTC;
		}
		else
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(! booting, "booting is expected to be false");
			}
			{
			LanguageConnectionContext lcc = getLCC();
			return lcc.getTransactionCompile();
			}
		}
	}


	/**
	 * Get the TransactionController to use, when not
	 * passed in as a parameter.  (This hides logic about
	 * whether or not we're at boot time in a single
	 * place.  NOTE:  There's no LCC at boot time.)
	 * NOTE: All <get> methods in the DD should call this method.
	 *
	 * @return TransactionController	The TC to use.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public TransactionController getTransactionExecute()
		throws StandardException
	{
		if (bootingTC != null)
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(booting, "booting is expected to be true");
			}
			return bootingTC;
		}
		else
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(! booting, "booting is expected to be false");
			}
			{
			LanguageConnectionContext lcc = getLCC();
			return lcc.getTransactionExecute();
			}
		}
	}

	/**
	 * Return a (single or list of) catalog row descriptor(s) from a
	 * system table where the access is from the index to the heap.
	 *
	 * @param indexId	The id of the index (0 to # of indexes on table) to use
	 * @param keyRow	The supplied ExecIndexRow for search
	 * @param ti		The TabInfo to use
	 * @param parentTupleDescriptor		The parentDescriptor, if applicable.
	 * @param dList		The list to build, if supplied.  If null, then caller expects
	 *					a single descriptor
	 * @param forUpdate	Whether or not to open the index for update.
	 *
	 * @return	The last matching descriptor
	 *
	 * @exception StandardException		Thrown on error
	 */
	private final TupleDescriptor getDescriptorViaIndex(
						int indexId,
						ExecIndexRow keyRow,
						ScanQualifier [][] scanQualifiers,
						TabInfo ti,
						TupleDescriptor parentTupleDescriptor,
						List list,
						boolean forUpdate)
			throws StandardException
	{
		CatalogRowFactory		rf = ti.getCatalogRowFactory();
		ConglomerateController	heapCC;
		ExecIndexRow	  		indexRow1;
		ExecIndexRow			indexTemplateRow;
		ExecRow 				outRow;
		RowLocation				baseRowLocation;
		ScanController			scanController;
		TransactionController	tc;
		TupleDescriptor			td = null;

		// Get the current transaction controller
		tc = getTransactionCompile();

		outRow = rf.makeEmptyRow();

		heapCC = tc.openConglomerate(
                ti.getHeapConglomerate(), false, 0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_REPEATABLE_READ);

		/* Scan the index and go to the data pages for qualifying rows to
		 * build the column descriptor.
		 */
		scanController = tc.openScan(
				ti.getIndexConglomerate(indexId),  // conglomerate to open
				false, // don't hold open across commit
				(forUpdate) ? TransactionController.OPENMODE_FORUPDATE : 0,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_REPEATABLE_READ,
				(FormatableBitSet) null,         // all fields as objects
				keyRow.getRowArray(),   // start position - first row
				ScanController.GE,      // startSearchOperation
				scanQualifiers,         //scanQualifier,
				keyRow.getRowArray(),   // stop position - through last row
				ScanController.GT);     // stopSearchOperation

		while (scanController.next())
		{
 			// create an index row template
			indexRow1 = getIndexRowFromHeapRow(
									ti.getIndexRowGenerator(indexId),
									heapCC.newRowLocationTemplate(),
									outRow);

			scanController.fetch(indexRow1.getRowArray());

			baseRowLocation = (RowLocation)	indexRow1.getColumn(
												indexRow1.nColumns());

            // RESOLVE paulat - remove the try catch block when track 3677 is fixed
            // just leave the contents of the try block
            // adding to get more info on track 3677

            boolean base_row_exists = false;
            try
            {
			    base_row_exists =
                    heapCC.fetch(
                        baseRowLocation, outRow.getRowArray(), (FormatableBitSet) null);
            }
            catch (RuntimeException re)
            {
                if (SanityManager.DEBUG)
                {
                    if (re instanceof AssertFailure)
                    {
					    StringBuffer strbuf = new StringBuffer("Error retrieving base row in table "+ti.getTableName());
					    strbuf.append(": An ASSERT was thrown when trying to locate a row matching index row "+indexRow1+" from index "+ti.getIndexName(indexId)+", conglom number "+ti.getIndexConglomerate(indexId));
                        debugGenerateInfo(strbuf,tc,heapCC,ti,indexId);
                    }
                }
                throw re;
            }
            catch (StandardException se)
            {
                if (SanityManager.DEBUG)
                {
                    // only look for a specific error i.e. that of record on page
                    // no longer exists
                    // do not want to catch lock timeout errors here
                    if (se.getSQLState().equals("XSRS9"))
                    {
					    StringBuffer strbuf = new StringBuffer("Error retrieving base row in table "+ti.getTableName());
					    strbuf.append(": A StandardException was thrown when trying to locate a row matching index row "+indexRow1+" from index "+ti.getIndexName(indexId)+", conglom number "+ti.getIndexConglomerate(indexId));
                        debugGenerateInfo(strbuf,tc,heapCC,ti,indexId);
                    }
                }
                throw se;
            }

            if (SanityManager.DEBUG)
            {
                // it can not be possible for heap row to disappear while
                // holding scan cursor on index at ISOLATION_REPEATABLE_READ.
				if (! base_row_exists)
				{
					StringBuffer strbuf = new StringBuffer("Error retrieving base row in table "+ti.getTableName());
					strbuf.append(": could not locate a row matching index row "+indexRow1+" from index "+ti.getIndexName(indexId)+", conglom number "+ti.getIndexConglomerate(indexId));
                    debugGenerateInfo(strbuf,tc,heapCC,ti,indexId);
                    // RESOLVE: for now, we are going to kill the VM
                    // to help debug this problem.
                    System.exit(1);

                    // RESOLVE: not currently reached
                    //SanityManager.THROWASSERT(strbuf.toString());
				}
            }

			td = rf.buildDescriptor(outRow, parentTupleDescriptor, this);

			/* If list is null, then caller only wants a single descriptor - we're done
			 * else just add the current descriptor to the list.
			 */
			if (list == null)
			{
				break;
			}
			else
			{
				list.add(td);
			}
		}
        scanController.close();
		heapCC.close();
		return td;
	}


    private void debugGenerateInfo(StringBuffer strbuf,
        TransactionController tc, ConglomerateController heapCC, TabInfo ti,
        int indexId)
    {
		if (SanityManager.DEBUG) {
        try
        {
            strbuf.append("\nadditional information: ");

            // print the lock table
            // will get a NullPointerException if lcc doesn't yet exist e.g. at boot time
            LanguageConnectionContext lcc = (LanguageConnectionContext)
                ContextService.getContext(LanguageConnectionContext.CONTEXT_ID);
            if (lcc != null)
            {
                long currentTime = System.currentTimeMillis();
//EXCLUDE-START-lockdiag-
                Enumeration lockTable = lockFactory.makeVirtualLockTable();
                String lockTableString = Timeout.buildString(lockTable,currentTime);
                strbuf.append("lock table at time of failure\n\n");
                strbuf.append(lockTableString);
//EXCLUDE-END-lockdiag-
            }

            // consistency checking etc.
            ConglomerateController btreeCC =
                tc.openConglomerate(
                    ti.getIndexConglomerate(indexId),
                    false,
                    0, TransactionController.MODE_RECORD,
                    TransactionController.ISOLATION_REPEATABLE_READ);

            btreeCC.debugConglomerate();
            heapCC.debugConglomerate();
            heapCC.checkConsistency();
            strbuf.append("\nheapCC.checkConsistency() = true");
            ConglomerateController indexCC = tc.openConglomerate(
                ti.getIndexConglomerate(indexId),
                false,
                0,
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_REPEATABLE_READ);
            indexCC.checkConsistency();
            strbuf.append("\nindexCC.checkConsistency() = true");

            System.err.println("ASSERT FAILURE: "+strbuf.toString());
            System.out.println("ASSERT FAILURE: "+strbuf.toString());
            SanityManager.DEBUG_PRINT("ASSERT FAILURE", strbuf.toString());
        }
        catch (StandardException se)
        {
            strbuf.append("\ngot the following error when doing extra consistency checks:\n"+se.toString());
        }
		}
    }



	/**
	 * Return a (single or list of) catalog row descriptor(s) from a
	 * system table where the access a heap scan
	 *
	 * @param scanQualifiers			qualifiers
	 * @param ti						The TabInfo to use
	 * @param parentTupleDescriptor		The parentDescriptor, if applicable.
	 * @param dList						The list to build, if supplied.  
	 *									If null, then caller expects a single descriptor
	 *
	 * @return	The last matching descriptor
	 *
	 * @exception StandardException		Thrown on error
	 */
	protected TupleDescriptor getDescriptorViaHeap(
						ScanQualifier [][] scanQualifiers,
						TabInfo ti,
						TupleDescriptor parentTupleDescriptor,
						List list)
			throws StandardException
	{
		CatalogRowFactory		rf = ti.getCatalogRowFactory();
		ConglomerateController	heapCC;
		ExecRow 				outRow;
		ScanController			scanController;
		TransactionController	tc;
		TupleDescriptor			td = null;

		// Get the current transaction controller
		tc = getTransactionCompile();

		outRow = rf.makeEmptyRow();

		/*
		** Table scan
		*/
		scanController = tc.openScan(
				ti.getHeapConglomerate(),	// conglomerate to open
				false, 						// don't hold open across commit
				0, 							// for read
				TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_REPEATABLE_READ,
				(FormatableBitSet) null,         // all fields as objects
				(DataValueDescriptor[]) null,		// start position - first row
				0,      				// startSearchOperation - none
				scanQualifiers, 		// scanQualifier,
				(DataValueDescriptor[]) null,		// stop position - through last row
				0);     				// stopSearchOperation - none

		while (scanController.fetchNext(outRow.getRowArray()))
		{
			td = rf.buildDescriptor(outRow, parentTupleDescriptor, this);

			/* If dList is null, then caller only wants a single descriptor - we're done
			 * else just add the current descriptor to the list.
			 */
			if (list == null)
			{
				break;
			}
			else
			{
				list.add(td);
			}
		}
		scanController.close();
		return td;
	}

	/**
	 * Get a TabInfo for a non-core table.
	 * (We fault in information about non-core tables as needed.)
	 *
	 * @param nonCoreNum	The index into noncoreTable[].
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	private TabInfo getNonCoreTI(int catalogNumber)
		throws StandardException
	{
		TabInfo	ti = getNonCoreTIByNumber(catalogNumber);

		faultInTabInfo( ti );

		return ti;
	}

	/** returns the tabinfo for a non core system catalog. Input is a
	 * catalogNumber (defined in DataDictionary). 
	 */
	protected TabInfo getNonCoreTIByNumber(int catalogNumber)
						throws StandardException
	{
		int nonCoreNum = catalogNumber - NUM_CORE;

		// Look up the TabInfo in the array. This does not have to be
		// synchronized, because getting a reference is atomic.

		TabInfo retval = noncoreInfo[nonCoreNum];

		if (retval == null)
		{
			// If we did not find the TabInfo, get the right one and
			// load it into the array. There is a small chance that
			// two threads will do this at the same time. The code will
			// work properly in that case, since storing a reference
			// is atomic (although we could get extra object instantiation
			// if two threads come through here at the same time.
			UUIDFactory luuidFactory = uuidFactory;

			switch (catalogNumber)
			{
			  case SYSCONSTRAINTS_CATALOG_NUM:
				retval = new TabInfoImpl(new SYSCONSTRAINTSRowFactory(
												luuidFactory, exFactory, dvf, convertIdToLower));
				break;

			  case SYSKEYS_CATALOG_NUM:
				retval = new TabInfoImpl(new SYSKEYSRowFactory(
												luuidFactory, exFactory, dvf, convertIdToLower));
				break;

			  case SYSDEPENDS_CATALOG_NUM:
				retval = new TabInfoImpl(new SYSDEPENDSRowFactory(
												luuidFactory, exFactory, dvf, convertIdToLower));
				break;

			  case SYSVIEWS_CATALOG_NUM:
				retval = new TabInfoImpl(new SYSVIEWSRowFactory(
												luuidFactory, exFactory, dvf, convertIdToLower));
				break;

			  case SYSCHECKS_CATALOG_NUM:
				retval = new TabInfoImpl(new SYSCHECKSRowFactory(
												luuidFactory, exFactory, dvf, convertIdToLower));
				break;

			  case SYSFOREIGNKEYS_CATALOG_NUM:
				retval = new TabInfoImpl(new SYSFOREIGNKEYSRowFactory(
												luuidFactory, exFactory, dvf, convertIdToLower));
				break;

			  case SYSSTATEMENTS_CATALOG_NUM:
				retval = new TabInfoImpl(new SYSSTATEMENTSRowFactory(
												luuidFactory, exFactory, dvf, convertIdToLower));
				break;

			  case SYSFILES_CATALOG_NUM:
				retval = new TabInfoImpl(new SYSFILESRowFactory(
												luuidFactory, exFactory, dvf, convertIdToLower));
				break;

			  case SYSALIASES_CATALOG_NUM:
				retval = new TabInfoImpl(new SYSALIASESRowFactory(
												luuidFactory, exFactory, dvf, convertIdToLower));
				break;

			  case SYSTRIGGERS_CATALOG_NUM:
				retval = new TabInfoImpl(new SYSTRIGGERSRowFactory(
												luuidFactory, exFactory, dvf, convertIdToLower));
				break;

			  case SYSSTATISTICS_CATALOG_NUM:
				retval = new TabInfoImpl(new SYSSTATISTICSRowFactory(
												 luuidFactory, exFactory, dvf, convertIdToLower));					 
				break;

			  case SYSDUMMY1_CATALOG_NUM:
				retval = new TabInfoImpl(new SYSDUMMY1RowFactory(
												 luuidFactory, exFactory, dvf, convertIdToLower));					 
				break;
			}

			initSystemIndexVariables(retval);

			noncoreInfo[nonCoreNum] = retval;
		}

		return retval;
	}

	protected void initSystemIndexVariables(TabInfo ti)
						throws StandardException
	{
		int numIndexes = ti.getNumberOfIndexes();

		if (numIndexes > 0)
		{
			DataDescriptorGenerator ddg = getDataDescriptorGenerator();

			for (int indexCtr = 0; indexCtr < numIndexes; indexCtr++)
			{
				initSystemIndexVariables(ddg, ti, indexCtr);	
			}
		}
	}

	// Expected to be called only during boot time, so no synchronization.
	private void clearNoncoreTable(int nonCoreNum)
	{
		noncoreInfo[nonCoreNum] = null;
	}

	/**
	 * Get core catalog info.
	 *
	 * @param coreNum	The index into coreInfo[].
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public TabInfo getCoreCatalog(int coreNum)
		throws StandardException
	{
		return coreInfo[coreNum];
	}

	/**
	  *	Finishes building a TabInfo if it hasn't already been faulted in.
	  *	NOP if TabInfo has already been faulted in.
	  *
	  *	@param	ti	TabInfo to fault in.
	  *
	  * @exception StandardException		Thrown on error
	  */
	public	void	faultInTabInfo( TabInfo ti )
		throws StandardException
	{
		int		numIndexes;

		/* Most of the time, the noncoreInfo will be complete.
		 * It's okay to do an unsynchronized check and return
		 * if it is complete, since it never becomes "un-complete".
		 * If we change the code, for some reason, to allow
		 * it to become "un-complete" after being complete,
		 * then we will have to do a synchronized check here
		 * as well.
		 */
		if (ti.isComplete())
		{
			return;
		}

		/* The completing of the noncoreInfo entry must be synchronized. 
		 * NOTE: We are assuming that we will not access a different
		 * noncoreInfo in the course of completing of this noncoreInfo,
		 * otherwise a deadlock could occur.
		 */
		synchronized(ti)
		{
			/* Now that we can run, the 1st thing that we must do
			 * is to verify that we still need to complete the
			 * object.  (We may have been blocked on another user
			 * doing the same.)
			 */
			if (ti.isComplete())
			{
				return;
			}

			TableDescriptor td = getTableDescriptor(ti.getTableName(),
													getSystemSchemaDescriptor());

			// It's possible that the system table is not there right
			// now. This can happen, for example, if we're in the
			// process of upgrading a source or target to Xena, in 
			// which case SYSSYNCINSTANTS is dropped and re-created.
			// Just return in this case, so we don't get a null pointer
			// exception.
			if (td == null)
			{
				return;
			}

			ConglomerateDescriptor cd = null;
			ConglomerateDescriptor[] cds = td.getConglomerateDescriptors();

			/* Init the heap conglomerate here */
			for (int index = 0; index < cds.length; index++)
			{
				cd = cds[index];

				if (! cd.isIndex())
				{
					ti.setHeapConglomerate(cd.getConglomerateNumber());
					break;
				}
			}
			
			if (SanityManager.DEBUG)
			{
				if (cd == null)
				{
					SanityManager.THROWASSERT("No heap conglomerate found for " 
						+ ti.getTableName());
				}
			}

			/* Initialize the index conglomerates */
			numIndexes = ti.getCatalogRowFactory().getNumIndexes();
			if (numIndexes == 0)
			{
				return;
			}

			/* For each index, we get its id from the CDL */
			ConglomerateDescriptor icd = null;
			int	indexCount = 0;

			for (int index = 0; index < cds.length; index++)
			{
				icd = cds[index];

				if (icd.isIndex())
				{
					ti.setIndexConglomerate(icd);
					indexCount++;
				}
				continue;
			}

			if (SanityManager.DEBUG)
			{
				if (indexCount != ti.getCatalogRowFactory().getNumIndexes())
				{
					SanityManager.THROWASSERT("Number of indexes found (" + indexCount + 
						") does not match the number expected (" +
						ti.getCatalogRowFactory().getNumIndexes() + ")");
				}
			}
		}
	}


	/**
	 * Get an index row based on a row from the heap.
	 *
	 * @param irg		IndexRowGenerator to use
	 * @param rl		RowLocation for heap
	 * @param heapRow	Row from the heap
	 *
	 * @return ExecIndexRow	Index row.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public static	ExecIndexRow getIndexRowFromHeapRow(IndexRowGenerator irg,
														RowLocation rl,
														ExecRow heapRow)
		throws StandardException
	{
		ExecIndexRow		indexRow;

		indexRow = irg.getIndexRowTemplate();
		// Get an index row based on the base row
		irg.getIndexRow(heapRow, rl, indexRow, (FormatableBitSet) null);

		return indexRow;
	}

	public	int	getEngineType()
	{
		return	engineType;
	}


	/**
	 * Get the heap conglomerate number for SYS.SYSCOLUMNS.
	 * (Useful for adding new index to the table.)
	 *
	 * @return The heap conglomerate number for SYS.SYSCOLUMNS.
	 */
	public long getSYSCOLUMNSHeapConglomerateNumber()
	{
		return coreInfo[SYSCOLUMNS_CORE_NUM].getHeapConglomerate();
	}

	void addSYSCOLUMNSIndex2Property(TransactionController tc, long index2ConglomerateNumber)
	{
		startupParameters.put(CFG_SYSCOLUMNS_INDEX2_ID, 
					Long.toString(index2ConglomerateNumber));
	}

	/**
	*/
	private long getBootParameter(Properties startParams, String key, boolean required)
		throws StandardException {

		String value = startParams.getProperty(key);
		if (value == null)
		{
			if (! required)
			{
				return -1;
			}
			throw StandardException.newException(SQLState.PROPERTY_MISSING, key);
		}

		try {
			return Long.parseLong(value);
		} catch (NumberFormatException nfe) {
			throw StandardException.newException(SQLState.PROPERTY_INVALID_VALUE, key, value);
		}
	}

	/**
	  *	Returns a unique system generated name of the form SQLyymmddhhmmssxxn
	  *	  yy - year, mm - month, dd - day of month, hh - hour, mm - minute, ss - second,
	  *	  xx - the first 2 digits of millisec because we don't have enough space to keep the exact millisec value,
	  *	  n - number between 0-9
	  *
	  *	  The number at the end is to handle more than one system generated name request came at the same time.
	  *	  In that case, the timestamp will remain the same, we will just increment n at the end of the name.
	  *
	  *	  Following is how we get around the problem of more than 10 system generated name requestes at the same time:
	  *	  When the database boots up, we start a counter with value -1 for the last digit in the generated name.
	  *	  We also keep the time in millisec to keep track of when the last system name was generated. At the
	  *	  boot time, it will be default to 0L. In addition, we have a calendar object for the time in millisec
	  *	  That calendar object is used to fetch yy, mm, dd, etc for the string SQLyymmddhhmmssxxn
	  *
	  *	  When the first request for the system generated name comes, time of last system generated name will be less than
	  *	  the current time. We initialize the counter to 0, set the time of last system generated name to the
	  *	  current time truncated off to lower 10ms time. The first name request is the only time we know for sure the
	  *	  time of last system generated name will be less than the current time. After this first request, the next request
	  *	  could be at any time. We go through the following algorithm for every generated name request.
	  *
	  *	  First check if the current time(truncated off to lower 10ms) is greater than the timestamp for last system generated name
	  *
	  *	    If yes, then we change the timestamp for system generated name to the current timestamp and reset the counter to 0
	  *	    and generate the name using the current timestamp and 0 as the number at the end of the generated name.
	  *
	  *	    If no, then it means this request for generated name has come at the same time as last one.
	  *	    Or it may come at a time less than the last generated name request. This could be because of seasonal time change
	  *	    or somebody manually changing the time on the computer. In any case,
	  *	    if the counter is less than 10(meaning this is not yet our 11th request for generated name at a given time),
	  *	    we use that in the generated name. But if the counter has reached 10(which means, this is the 11th name request
	  *	    at the same time), then we increment the system generated name timestamp by 10ms and reset the counter to 0
	  *	    (notice, at this point, the timestamp for system generated names is not in sync with the real current time, but we
	  *	    need to have this mechanism to get around the problem of more than 10 generated name requests at a same physical time).
	  *
	  *	@return	system generated unique name
	  */
	public String getSystemSQLName()
	{
		//note that we are using Date class to change the Calendar object time rather than Calendar.setTimeInMills.
		//This is because of java bug 4243802. setTimeInMillis and getTimeInMillis were protected prior to jdk14
		//But since we still support jdk13, we can't rely on these methods being public starting jdk14.
		//Because of that, we are using Date to do all the time manipulations on the Calendar object 
		StringBuffer generatedSystemSQLName = new StringBuffer("SQL");
			synchronized (this) {
				//get the current timestamp
				long timeNow = (System.currentTimeMillis()/10L)*10L;

				//if the current timestamp is greater than last constraint name generation time, then we reset the counter and
				//record the new timestamp
				if (timeNow > timeForLastSystemSQLName) {
					systemSQLNameNumber = 0;
					calendarForLastSystemSQLName.setTime(new Date(timeNow));
					timeForLastSystemSQLName = timeNow;
				} else { 
					//the request has come at the same time as the last generated name request
					//or it has come at a time less than the time the last generated name request. This can happen
					//because of seasonal time change or manual update of computer time.
          
					//get the number that was last used for the last digit of generated name and increment it by 1.
					systemSQLNameNumber++;
					if (systemSQLNameNumber == 10) { //we have already generated 10 names at the last system generated timestamp value
						//so reset the counter
						systemSQLNameNumber = 0;
						timeForLastSystemSQLName = timeForLastSystemSQLName + 10L;
						//increment the timestamp for system generated names by 10ms
            calendarForLastSystemSQLName.setTime(new Date(timeForLastSystemSQLName));
					}
				}

				generatedSystemSQLName.append(twoDigits(calendarForLastSystemSQLName.get(Calendar.YEAR)));
				//have to add 1 to the month value returned because the method give 0-January, 1-February and so on and so forth
				generatedSystemSQLName.append(twoDigits(calendarForLastSystemSQLName.get(Calendar.MONTH)+1));
				generatedSystemSQLName.append(twoDigits(calendarForLastSystemSQLName.get(Calendar.DAY_OF_MONTH)));
				generatedSystemSQLName.append(twoDigits(calendarForLastSystemSQLName.get(Calendar.HOUR)));
				generatedSystemSQLName.append(twoDigits(calendarForLastSystemSQLName.get(Calendar.MINUTE)));
				generatedSystemSQLName.append(twoDigits(calendarForLastSystemSQLName.get(Calendar.SECOND)));
				//because we don't have enough space to store the entire millisec value, just store the higher 2 digits.
				generatedSystemSQLName.append(twoDigits((int) (calendarForLastSystemSQLName.get(Calendar.MILLISECOND)/10)));
				generatedSystemSQLName.append(systemSQLNameNumber);
			}
		return generatedSystemSQLName.toString();
	}

	private static String twoDigits(int val) {
		String retval;

		if (val < 10) {
			retval = "0" + val;
		} else {
			int retvalLength = Integer.toString(val).length();
			retval = Integer.toString(val).substring(retvalLength-2);
		}

		return retval;
	}

	/**
	 * sets a new value in SYSCOLUMNS for a particular
	 * autoincrement column.
	 * 
	 * @param tc		 Transaction Controller to use.
	 * @param td		 Table Descriptor
	 * @param columnName Name of the column.
	 * @param aiValue	 Value to write to SYSCOLUMNS.
	 * @param incrementNeeded whether to increment the value passed in by the
	 * user (aiValue) or not before writing it to SYSCOLUMNS.
	 */
	public void setAutoincrementValue(TransactionController tc,
									  UUID tableUUID,
									  String columnName,
									  long aiValue, boolean incrementNeeded)
			throws StandardException						  
	{
		TabInfo ti = coreInfo[SYSCOLUMNS_CORE_NUM];
		ExecIndexRow keyRow = null;

		keyRow = (ExecIndexRow)exFactory.getIndexableRow(2);
		keyRow.setColumn(1, dvf.getCharDataValue(tableUUID.toString()));
  		keyRow.setColumn(2, dvf.getCharDataValue(columnName));
		
		SYSCOLUMNSRowFactory	rf = (SYSCOLUMNSRowFactory) ti.getCatalogRowFactory();
		ExecRow row = rf.makeEmptyRow();

		boolean[] bArray = new boolean[2];
		for (int index = 0; index < 2; index++)
		{
			bArray[index] = false;
		}
		
		int[] colsToUpdate = new int[1];
		
		colsToUpdate[0] = SYSCOLUMNSRowFactory.SYSCOLUMNS_AUTOINCREMENTVALUE;

		if (incrementNeeded)
		{
			ExecRow readRow = ti.getRow(tc, keyRow, 
									SYSCOLUMNSRowFactory.SYSCOLUMNS_INDEX1_ID);
			NumberDataValue increment = 
				(NumberDataValue)readRow.getColumn(SYSCOLUMNSRowFactory.SYSCOLUMNS_AUTOINCREMENTINC);
			aiValue += increment.getLong();
		}
		row.setColumn(SYSCOLUMNSRowFactory.SYSCOLUMNS_AUTOINCREMENTVALUE,
					  dvf.getDataValue(aiValue));

		ti.updateRow(keyRow, row,
					 SYSCOLUMNSRowFactory.SYSCOLUMNS_INDEX1_ID,
					 bArray, 
					 colsToUpdate,
					 tc);
		return;
	}

	/**
	 * Computes the RowLocation in SYSCOLUMNS for a particular 
	 * autoincrement column.
	 * 
	 * @param tc			Transaction Controller to use.
	 * @param td			Table Descriptor.
	 * @param columnName	Name of column which has autoincrement column.
	 * 
	 * @exception StandardException thrown on failure.
	 */ 
	private RowLocation computeRowLocation(TransactionController tc,
										  TableDescriptor td,
										  String columnName)
		throws StandardException								  
	{
		TabInfo ti = coreInfo[SYSCOLUMNS_CORE_NUM];
		ExecIndexRow keyRow = null;
		ExecRow row;
		UUID tableUUID = td.getUUID();

		keyRow = (ExecIndexRow)exFactory.getIndexableRow(2);
		keyRow.setColumn(1, dvf.getCharDataValue(tableUUID.toString()));
  		keyRow.setColumn(2, dvf.getCharDataValue(columnName));
		return ti.getRowLocation(tc, keyRow, 
								 SYSCOLUMNSRowFactory.SYSCOLUMNS_INDEX1_ID);
	}

    public RowLocation getRowLocationTemplate(LanguageConnectionContext lcc,
                                              TableDescriptor td)
          throws StandardException
    {
    	RowLocation 			rl; 
		ConglomerateController 	heapCC = null;

		TransactionController tc = 
			lcc.getTransactionCompile();

		long tableId = td.getHeapConglomerateId();
		heapCC = 
            tc.openConglomerate(
                tableId, false, 0, tc.MODE_RECORD, tc.ISOLATION_READ_COMMITTED);
		try
		{
			rl = heapCC.newRowLocationTemplate();
		}
		finally
		{
			heapCC.close();
		}

		return rl;
    }
	
	/**
	 * 
	 * Add a table descriptor to the "other" cache. The other cache is
	 * determined by the type of the object c.
	 *
	 * @param td	TableDescriptor to add to the other cache.
	 * @param c		Cacheable Object which lets us figure out the other cache.
	 *
	 * @exception	StandardException
	 */
	public void addTableDescriptorToOtherCache(TableDescriptor td,
											   Cacheable c) 
		throws StandardException
	{
		// get the other cache. if the entry we are setting in the cache is of
		// type oidtdcacheable then use the nametdcache 
		CacheManager otherCache = 
			(c instanceof OIDTDCacheable) ? nameTdCache : OIDTdCache;
		Object key;
		TDCacheable otherCacheEntry = null;

		if (otherCache == nameTdCache) 
			key = new TableKey(td.getSchemaDescriptor().getUUID(), td.getName());
		else
			key = td.getUUID();

		try
		{
			// insert the entry into the the other cache.
			otherCacheEntry = (TDCacheable)otherCache.create(key, td);
		}
		catch (StandardException se)
		{
			// if the object already exists in cache then somebody beat us to it
			// otherwise throw the error.
			if (!(se.getMessageId().equals(SQLState.OBJECT_EXISTS_IN_CACHE)))
				throw se;
		}
		finally 
		{
			if (otherCacheEntry != null)
				otherCache.release(otherCacheEntry);
		}
	}
	

	/** @see DataDictionary#dropStatisticsDescriptors */
	public void dropStatisticsDescriptors(UUID tableUUID, UUID referenceUUID,
									 TransactionController tc)
		throws StandardException
	{
		TabInfo					ti = getNonCoreTI(SYSSTATISTICS_CATALOG_NUM);
		DataValueDescriptor first, second;
		first = dvf.getCharDataValue(tableUUID.toString());

		ExecIndexRow keyRow;
		if (referenceUUID != null)
		{
			keyRow = exFactory.getIndexableRow(2);
			second = dvf.getCharDataValue(referenceUUID.toString());
			keyRow.setColumn(2, second);
		}
		else
		{
			keyRow = exFactory.getIndexableRow(1);
		}

		keyRow.setColumn(1, first);

		ti.deleteRow(tc, keyRow,
					 SYSSTATISTICSRowFactory.SYSSTATISTICS_INDEX1_ID); 
		
	}

	private static LanguageConnectionContext getLCC() {
		return (LanguageConnectionContext) 
					ContextService.getContextOrNull(LanguageConnectionContext.CONTEXT_ID);
	}

    private SchemaDescriptor newSystemSchemaDesc(
    String  name, 
    String  uuid)
    {
        return new SchemaDescriptor(
                this,
                name,
                SchemaDescriptor.SA_USER_NAME,
                uuidFactory.recreateUUID(uuid),
                true);
    }

    private SchemaDescriptor newDeclaredGlobalTemporaryTablesSchemaDesc( String name)
    {
        return new SchemaDescriptor(this,
                                        name,
                                        SchemaDescriptor.SA_USER_NAME,
                                        (UUID) null,
                                        false);
    }
	/**
		Check to see if a database has been upgraded to the required
		level in order to use a langauge feature.

		@param majorVersion Data Dictionary major version
		@param feature Non-null to throw an error, null to return the state of the version match.

		@return True if the database has been upgraded to the required level, false otherwise.
	*/
	public boolean checkVersion(int requiredMajorVersion, String feature) throws StandardException {

		if (requiredMajorVersion == DataDictionary.DD_VERSION_CURRENT) {
			requiredMajorVersion = softwareVersion.majorVersionNumber;
		}

		return dictionaryVersion.checkVersion(requiredMajorVersion, feature);
	}
		
	/**
	** Create system built-in metadata stored prepared statements.
	*/
	void createSystemSps(TransactionController tc)
		throws StandardException
	{
		// DatabaseMetadata stored plans
		createSPSSet(tc, false, getSystemSchemaDescriptor().getUUID());

		// network server stored plans
		createSPSSet(tc, true, getSysIBMSchemaDescriptor().getUUID());
	}

	/**
		Create a set of stored prepared statements from a properties file.
		Key is the statement name, value is the SQL statement.
	*/
	private void createSPSSet(TransactionController tc, boolean net, UUID schemaID)
		throws StandardException
	{
		Properties p = getQueryDescriptions(net);
		Enumeration enum = p.keys();
		//statement will get compiled on first execution
		//Note: Don't change this to FALSE LCC is not available for compiling
		boolean nocompile = true;
		
		while (enum.hasMoreElements())
		{
			String spsName = (String)enum.nextElement();
			String spsText =  p.getProperty(spsName);
			SPSDescriptor spsd = new SPSDescriptor(this, spsName,
												   getUUIDFactory().createUUID(),
												   schemaID,
												   schemaID,
												   SPSDescriptor.SPS_TYPE_REGULAR,
												   !nocompile,		// it is valid, unless nocompile
												   spsText, //sps text
												   (String)null,	// using text
												   (Object[])null, //	paramDefaults,
												   !nocompile );
			
			addSPSDescriptor(spsd, tc, true);
		}
	}


    /**
     * Generic create procedure routine.
     * <p>
     * Takes the input procedure and inserts it into the appropriate
     * catalog.  
     *
     * Assumes all arguments are "IN" type.
     *
	 * @return The identifier to be used to open the conglomerate later.
     *
     * @param routine_name  name of the routine in java and the SQL 
     *                      procedure name.
     *
     * @param arg_names     String array of procedure argument names in order.
     *
     * @param arg_types     Internal SQL types of the arguments
     *
     * @param routine_sql_control     
     *                      One of the RoutineAliasInfo constants:
     *                          MODIFIES_SQL_DATA
     *                          READS_SQL_DATA
     *                          CONTAINS_SQL
     *                          NO_SQL
     *
     * @param return_type   null for procedure.  For functions the return type
     *                      of the function.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    private final void createSystemProcedureOrFunction(
    String                  routine_name,
    UUID                    schema_uuid,
    String[]                arg_names,
    TypeDescriptor[]        arg_types,
	int						num_out_param,
	int						num_result_sets,
    short                   routine_sql_control,
    TypeDescriptor          return_type,
    TransactionController   tc)
        throws StandardException
    {
        int num_args = 0;
        if (arg_names != null)
            num_args = arg_names.length;

        if (SanityManager.DEBUG)
        {
            if (num_args != 0)
            {
                SanityManager.ASSERT(arg_names != null);
                SanityManager.ASSERT(arg_types != null);
                SanityManager.ASSERT(arg_names.length == arg_types.length);
            }
        }

        // Actual procedures are in this class:
		String procClass = "org.apache.derby.catalog.SystemProcedures";

        // all args are only "in" arguments
        int[] arg_modes = null;
        if (num_args != 0)
        {
            arg_modes = new int[num_args];
			int num_in_param = num_args - num_out_param;
            for (int i = 0; i < num_in_param; i++)
                arg_modes[i] = JDBC30Translation.PARAMETER_MODE_IN;
            for (int i = 0; i < num_out_param; i++)
                arg_modes[num_in_param + i] = JDBC30Translation.PARAMETER_MODE_OUT;
        }

        RoutineAliasInfo routine_alias_info = 
            new RoutineAliasInfo(
                routine_name,                       // name of routine
                num_args,                           // number of params
                arg_names,                          // names of params
                arg_types,                          // types of params
                arg_modes,                          // all "IN" params
                num_result_sets,                    // number of result sets
                RoutineAliasInfo.PS_JAVA,           // link to java routine
                routine_sql_control,                // one of:
                                                    //  MODIFIES_SQL_DATA
                                                    //  READS_SQL_DATA
                                                    //  CONTAINS_SQL
                                                    //  NO_SQL
                true,                               // true - calledOnNullInput
                return_type);

        AliasDescriptor ads = 
            new AliasDescriptor(
                this,
                getUUIDFactory().createUUID(),
                routine_name,
                schema_uuid,
                procClass,	
                (return_type == null) ? 
                    AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR :
                    AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR,
                (return_type == null) ? 
                    AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR :
                    AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR,
                false,
                routine_alias_info, null);

        addDescriptor(
            ads, null, DataDictionary.SYSALIASES_CATALOG_NUM, false, tc);
    }

    /**
     * Create system procedures
     * <p>
     * Used to add the system procedures to the database when
     * it is created.  System procedures are currently added to
     * either SYSCS_UTIL or SQLJ schemas.
     * <p>
     *
     * @param tc     transaction controller to use.  Counts on caller to
     *               commit.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    private final void create_SYSCS_procedures(
    TransactionController   tc)
        throws StandardException
    {
        /*
		** SYSCS_UTIL routines.
		*/

        // used to put procedure into the SYSCS_UTIL schema
		UUID sysUtilUUID = getSystemUtilSchemaDescriptor().getUUID();


        // void SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(
        //     varchar(128), varchar(DB2Limit.DB2_VARCHAR_MAXWIDTH))
        {

            // procedure argument names
            String[] arg_names = {
                "KEY", 
                "VALUE"};

            // procedure argument types
            TypeDescriptor[] arg_types = {
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.VARCHAR, 128),
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.VARCHAR, DB2Limit.DB2_VARCHAR_MAXWIDTH)
            };

            createSystemProcedureOrFunction(
                "SYSCS_SET_DATABASE_PROPERTY",
                sysUtilUUID,
                arg_names,
                arg_types,
				0,
				0,
                RoutineAliasInfo.MODIFIES_SQL_DATA,
                (TypeDescriptor) null,
                tc);
        }


        // void SYSCS_UTIL.SYSCS_COMPRESS_TABLE(varchar(128), varchar(128), SMALLINT)
        {
            // procedure argument names
            String[] arg_names = {"SCHEMANAME", "TABLENAME", "SEQUENTIAL"};

            // procedure argument types
            TypeDescriptor[] arg_types = {
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.VARCHAR, 128),
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.VARCHAR, 128),
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.SMALLINT)

            };

            createSystemProcedureOrFunction(
                "SYSCS_COMPRESS_TABLE",
                sysUtilUUID,
                arg_names,
                arg_types,
				0,
				0,
                RoutineAliasInfo.MODIFIES_SQL_DATA,
                (TypeDescriptor) null,
                tc);
        }

        // void SYSCS_UTIL.SYSCS_CHECKPOINT_DATABASE()
        {
            createSystemProcedureOrFunction(
                "SYSCS_CHECKPOINT_DATABASE",
                sysUtilUUID,
                null,
                null,
				0,
				0,
                RoutineAliasInfo.CONTAINS_SQL,
                (TypeDescriptor) null,
                tc);
        }

        // void SYSCS_UTIL.SYSCS_FREEZE_DATABASE()
        {
            createSystemProcedureOrFunction(
                "SYSCS_FREEZE_DATABASE",
                sysUtilUUID,
                null,
                null,
				0,
				0,
                RoutineAliasInfo.CONTAINS_SQL,
                (TypeDescriptor) null,
                tc);
        }

        // void SYSCS_UTIL.SYSCS_UNFREEZE_DATABASE()
        {
            createSystemProcedureOrFunction(
                "SYSCS_UNFREEZE_DATABASE",
                sysUtilUUID,
                null,
                null,
				0,
				0,
                RoutineAliasInfo.CONTAINS_SQL,
                (TypeDescriptor) null,
                tc);
        }

        // void SYSCS_UTIL.SYSCS_BACKUP_DATABASE(varchar DB2Limit.DB2_VARCHAR_MAXWIDTH)
        {
            // procedure argument names
            String[] arg_names = {"BACKUPDIR"};

            // procedure argument types
            TypeDescriptor[] arg_types = {
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.VARCHAR, DB2Limit.DB2_VARCHAR_MAXWIDTH)
            };

            createSystemProcedureOrFunction(
                "SYSCS_BACKUP_DATABASE",
                sysUtilUUID,
                arg_names,
                arg_types,
				0,
				0,
                RoutineAliasInfo.MODIFIES_SQL_DATA,
                (TypeDescriptor) null,
                tc);
        }

        // void SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE(
        //     varchar DB2Limit.DB2_VARCHAR_MAXWIDTH, smallint)
        {
            // procedure argument names
            String[] arg_names = {"BACKUPDIR", "DELETE_ARCHIVED_LOG_FILES"};

            // procedure argument types
            TypeDescriptor[] arg_types = {
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.VARCHAR, DB2Limit.DB2_VARCHAR_MAXWIDTH),
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.SMALLINT)
            };

            createSystemProcedureOrFunction(
                "SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE",
                sysUtilUUID,
                arg_names,
                arg_types,
				0,
				0,
                RoutineAliasInfo.MODIFIES_SQL_DATA,
                (TypeDescriptor) null,
                tc);
        }

        // void SYSCS_UTIL.SYSCS_DISABLE_LOG_ARCHIVE_MODE(smallint)
        {
            // procedure argument names
            String[] arg_names = {"DELETE_ARCHIVED_LOG_FILES"};

            // procedure argument types
            TypeDescriptor[] arg_types = {
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.SMALLINT)
            };

            createSystemProcedureOrFunction(
                "SYSCS_DISABLE_LOG_ARCHIVE_MODE",
                sysUtilUUID,
                arg_names,
                arg_types,
				0,
				0,
                RoutineAliasInfo.MODIFIES_SQL_DATA,
                (TypeDescriptor) null,
                tc);
        }

        // void SYSCS_UTIL.SYSCS_SET_RUNTIMESTTISTICS(smallint)
        {
            // procedure argument names
            String[] arg_names = {"ENABLE"};

            // procedure argument types
            TypeDescriptor[] arg_types = {
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.SMALLINT)
            };

            createSystemProcedureOrFunction(
                "SYSCS_SET_RUNTIMESTATISTICS",
                sysUtilUUID,
                arg_names,
                arg_types,
				0,
				0,
                RoutineAliasInfo.CONTAINS_SQL,
                (TypeDescriptor) null,
                tc);
        }

        // void SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(smallint)
        {
            // procedure argument names
            String[] arg_names = {"ENABLE"};

            // procedure argument types
            TypeDescriptor[] arg_types = {
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.SMALLINT)
            };

            createSystemProcedureOrFunction(
                "SYSCS_SET_STATISTICS_TIMING",
                sysUtilUUID,
                arg_names,
                arg_types,
				0,
				0,
                RoutineAliasInfo.CONTAINS_SQL,
                (TypeDescriptor) null,
                tc);
        }


        // SYSCS_UTIL functions
        //
        // TODO (mikem) - 
        // the following need to be functions when that is supported.
        // until then calling them will not work.

        // VARCHAR(DB2Limit.DB2_VARCHAR_MAXWIDTH) 
        // SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY(varchar(128))

        {
            // procedure argument names
            String[] arg_names = {"KEY"};

            // procedure argument types
            TypeDescriptor[] arg_types = {
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.VARCHAR, 128)
            };

            createSystemProcedureOrFunction(
                "SYSCS_GET_DATABASE_PROPERTY",
                sysUtilUUID,
                arg_names,
                arg_types,
				0,
				0,
                RoutineAliasInfo.READS_SQL_DATA,
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.VARCHAR, DB2Limit.DB2_VARCHAR_MAXWIDTH),
                tc);
        }

        // SMALLINT SYSCS_UTIL.SYSCS_CHECK_TABLE(varchar(128))
        {
            // procedure argument names
            String[] arg_names = {"SCHEMANAME", "TABLENAME"};

            // procedure argument types
            TypeDescriptor[] arg_types = {
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.VARCHAR, 128),
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.VARCHAR, 128)
            };

            createSystemProcedureOrFunction(
                "SYSCS_CHECK_TABLE",
                sysUtilUUID,
                arg_names,
                arg_types,
				0,
				0,
                RoutineAliasInfo.READS_SQL_DATA,
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.INTEGER),
                tc);
        }

        // CLOB SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()
        {

            createSystemProcedureOrFunction(
                "SYSCS_GET_RUNTIMESTATISTICS",
                sysUtilUUID,
                null,
                null,
				0,
				0,
                RoutineAliasInfo.CONTAINS_SQL,
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.VARCHAR, DB2Limit.DB2_VARCHAR_MAXWIDTH),

                /*
                TODO - mikem, wants to be a CLOB, but don't know how to do 
                that yet.  Testing it with varchar for now.
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.CLOB, DB2Limit.DB2_LOB_MAXWIDTH),
                */
                tc);
        }

        /*
		** SQLJ routine.
		*/

		UUID sqlJUUID = 
            getSchemaDescriptor(
                SchemaDescriptor.STD_SQLJ_SCHEMA_NAME, tc, true).getUUID();

		// SQLJ.INSTALL_JAR(URL VARCHAR(??), JAR VARCHAR(128), DEPLOY INT)
		{
            String[] arg_names = {"URL", "JAR", "DEPLOY"};

            TypeDescriptor[] arg_types = {
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.VARCHAR, 256),
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.INTEGER)
            };

            createSystemProcedureOrFunction(
                "INSTALL_JAR",
                sqlJUUID,
                arg_names,
                arg_types,
				0,
				0,
                RoutineAliasInfo.MODIFIES_SQL_DATA,
                (TypeDescriptor) null,
                tc);
        }

		// SQLJ.REPLACE_JAR(URL VARCHAR(??), JAR VARCHAR(128))
		{
            String[] arg_names = {"URL", "JAR"};

            TypeDescriptor[] arg_types = {
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.VARCHAR, 256),
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.VARCHAR, 128)
            };

            createSystemProcedureOrFunction(
                "REPLACE_JAR",
                sqlJUUID,
                arg_names,
                arg_types,
				0,
				0,
                RoutineAliasInfo.MODIFIES_SQL_DATA,
                (TypeDescriptor) null,
                tc);
        }
	
		// SQLJ.REMOVE_JAR(JAR VARCHAR(128), UNDEPLOY INT)
		{
            String[] arg_names = {"JAR", "UNDEPLOY"};

            TypeDescriptor[] arg_types = {
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.VARCHAR, 128),
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.INTEGER)
            };

            createSystemProcedureOrFunction(
                "REMOVE_JAR",
                sqlJUUID,
                arg_names,
                arg_types,
				0,
				0,
                RoutineAliasInfo.MODIFIES_SQL_DATA,
                (TypeDescriptor) null,
                tc);
        }	

		/*  SYSCS_EXPORT_TABLE (IN SCHEMANAME  VARCHAR(128), 
         * IN TABLENAME    VARCHAR(128),  IN FILENAME VARCHAR(32672) , 
         * IN COLUMNDELIMITER CHAR(1),  IN CHARACTERDELIMITER CHAR(1) ,  
		 * IN CODESET VARCHAR(128))
		 */

		
        {
            // procedure argument names
            String[] arg_names = {"schemaName", "tableName" , 
								  "fileName"," columnDelimiter", 
								  "characterDelimiter", "codeset"};

            // procedure argument types
            TypeDescriptor[] arg_types = {
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.VARCHAR, 128), 
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.VARCHAR, 32672),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.CHAR, 1),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.CHAR, 1),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.VARCHAR, 128)
            };

            createSystemProcedureOrFunction(
                "SYSCS_EXPORT_TABLE",
                sysUtilUUID,
                arg_names,
                arg_types,
				0,
				0,
				RoutineAliasInfo.READS_SQL_DATA,
                (TypeDescriptor) null,
                tc);
        }


		/* SYSCS_EXPORT_QUERY (IN SELECTSTATEMENT  VARCHAR(32672), 
		 * IN FILENAME VARCHAR(32672) , 
         * IN COLUMNDELIMITER CHAR(1),  IN CHARACTERDELIMITER CHAR(1) ,  
		 * IN CODESET VARCHAR(128))
		 */
        {
            // procedure argument names
            String[] arg_names = {"selectStatement", "fileName",
								  " columnDelimiter", "characterDelimiter", 
								  "codeset"};

            // procedure argument types
            TypeDescriptor[] arg_types = {
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.VARCHAR, 32672), 
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.VARCHAR, 32672),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.CHAR, 1),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.CHAR, 1),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.VARCHAR, 128)
            };

            createSystemProcedureOrFunction(
   			   "SYSCS_EXPORT_QUERY",
                sysUtilUUID,
                arg_names,
                arg_types,
				0,
				0,
				RoutineAliasInfo.READS_SQL_DATA,
                (TypeDescriptor) null,
                tc);
        }

		
		/*  SYSCS_IMPORT_TABLE(IN SCHEMANAME VARCHAR(128), 
		 *  IN TABLENAME VARCHAR(128),  IN FILENAME VARCHAR(32762), 
		 *  IN COLUMNDELIMITER CHAR(1), IN CHARACTERDELIMITER  CHAR(1),  
		 *  IN CODESET VARCHAR(128) , IN  REPLACE SMALLINT)
		 */
        {
            // procedure argument names
            String[] arg_names = {"schemaName", "tableName", "fileName",
								  " columnDelimiter", "characterDelimiter", 
								  "codeset", "replace"};

            // procedure argument types
			
            // procedure argument types
            TypeDescriptor[] arg_types = {
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.VARCHAR, 128), 
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.VARCHAR, 32672),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.CHAR, 1),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.CHAR, 1),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.VARCHAR,	128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.SMALLINT),
            };


            createSystemProcedureOrFunction(
   			   "SYSCS_IMPORT_TABLE",
                sysUtilUUID,
                arg_names,
                arg_types,
				0,
				0,
				RoutineAliasInfo.MODIFIES_SQL_DATA,
                (TypeDescriptor) null,
                tc);
        }

				
		/*  SYSCS_IMPORT_DATA(IN SCHEMANAME VARCHAR(128), 
		 *  IN TABLENAME VARCHAR(128), IN INSERTCOLUMNLIST VARCHAR(32762), 
		 *  IN COLUMNINDEXES VARCHAR(32762), IN IN FILENAME VARCHAR(32762), 
		 *  IN COLUMNDELIMITER CHAR(1), IN CHARACTERDELIMITER  CHAR(1),  
		 *  IN CODESET VARCHAR(128) , IN  REPLACE SMALLINT)
		 */
        {
            // procedure argument names
            String[] arg_names = {"schemaName", "tableName", "insertColumnList","columnIndexes",
								  "fileName", " columnDelimiter", "characterDelimiter", 
								  "codeset", "replace"};

            // procedure argument types
			
            // procedure argument types
            TypeDescriptor[] arg_types = {
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.VARCHAR, 128), 
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.VARCHAR, 32672),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.VARCHAR, 32672),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.VARCHAR, 32672),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.CHAR, 1),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.CHAR, 1),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.VARCHAR,	128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.SMALLINT),
            };


            createSystemProcedureOrFunction(
   			   "SYSCS_IMPORT_DATA",
                sysUtilUUID,
                arg_names,
                arg_types,
				0,
				0,
				RoutineAliasInfo.MODIFIES_SQL_DATA,
                (TypeDescriptor) null,
                tc);
        }

						
		/*  
		 * SYSCS_BULK_INSERT (IN SCHEMANAME  VARCHAR(128), IN TABLENAME    VARCHAR(128),  
		 *                    IN VTINAME VARCHAR(32762), IN VTIARG VARCHAR(32762))
		 */
        {
            // procedure argument names
            String[] arg_names = {"schemaName", "tableName", "vtiName","vtiArg"};

			
            // procedure argument types
            TypeDescriptor[] arg_types = {
                DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.VARCHAR, 128), 
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.VARCHAR, 32672),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.VARCHAR, 32672),
            };


            createSystemProcedureOrFunction(
   			   "SYSCS_BULK_INSERT",
                sysUtilUUID,
                arg_names,
                arg_types,
				0,
				0,
				RoutineAliasInfo.MODIFIES_SQL_DATA,
                (TypeDescriptor) null,
                tc);
        }


    }

    /**
     * Create system procedures in SYSIBM
     * <p>
     * Used to add the system procedures to the database when
     * it is created.  Full upgrade from version 5.1 or earlier also
	 * calls this method.
     * <p>
     *
     * @param tc     transaction controller to use.  Counts on caller to
     *               commit.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    protected final void create_SYSIBM_procedures(
    TransactionController   tc)
        throws StandardException
    {
        /*
		** SYSIBM routines.
		*/

        // used to put procedure into the SYSIBM schema
		UUID sysIBMUUID = getSysIBMSchemaDescriptor().getUUID();


        // SYSIBM.SQLCAMESSAGE(
        {

            // procedure argument names
            String[] arg_names = {
				"SQLCODE",
				"SQLERRML",
				"SQLERRMC",
				"SQLERRP",
				"SQLERRD0",
				"SQLERRD1",
				"SQLERRD2",
				"SQLERRD3",
				"SQLERRD4",
				"SQLERRD5",
				"SQLWARN",
				"SQLSTATE",
				"FILE",
				"LOCALE",
				"MESSAGE",
				"RETURNCODE"};

            // procedure argument types
            TypeDescriptor[] arg_types = {
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.SMALLINT),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(
						Types.VARCHAR, DB2Limit.DB2_JCC_MAX_EXCEPTION_PARAM_LENGTH),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, 8),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, 11),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, 5),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 50),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, 5),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 2400),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)
            };

            createSystemProcedureOrFunction(
                "SQLCAMESSAGE",
                sysIBMUUID,
                arg_names,
                arg_types,
				2,
				0,
                RoutineAliasInfo.READS_SQL_DATA,
                (TypeDescriptor) null,
                tc);
        }

        // SYSIBM.SQLPROCEDURES(VARCHAR(128), VARCHAR(128), VARCHAR(128), VARCHAR(4000))
        {

            // procedure argument names
            String[] arg_names = {
				"CATALOGNAME",
				"SCHEMANAME",
				"PROCNAME",
				"OPTIONS"};

            // procedure argument types
            TypeDescriptor[] arg_types = {
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 4000)};

            createSystemProcedureOrFunction(
                "SQLPROCEDURES",
                sysIBMUUID,
                arg_names,
                arg_types,
				0,
				1,
                RoutineAliasInfo.READS_SQL_DATA,
                (TypeDescriptor) null,
                tc);
        }

        // SYSIBM.SQLTABLEPRIVILEGES(VARCHAR(128), VARCHAR(128), VARCHAR(128), VARCHAR(4000))
        {

            // procedure argument names
            String[] arg_names = {
				"CATALOGNAME",
				"SCHEMANAME",
				"TABLENAME",
				"OPTIONS"};

            // procedure argument types
            TypeDescriptor[] arg_types = {
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 4000)};

            createSystemProcedureOrFunction(
                "SQLTABLEPRIVILEGES",
                sysIBMUUID,
                arg_names,
                arg_types,
				0,
				1,
                RoutineAliasInfo.READS_SQL_DATA,
                (TypeDescriptor) null,
                tc);
        }

        // SYSIBM.SQLPRIMARYKEYS(VARCHAR(128), VARCHAR(128), VARCHAR(128), VARCHAR(4000))
        {

            // procedure argument names
            String[] arg_names = {
				"CATALOGNAME",
				"SCHEMANAME",
				"TABLENAME",
				"OPTIONS"};

            // procedure argument types
            TypeDescriptor[] arg_types = {
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 4000)};

            createSystemProcedureOrFunction(
                "SQLPRIMARYKEYS",
                sysIBMUUID,
                arg_names,
                arg_types,
				0,
				1,
                RoutineAliasInfo.READS_SQL_DATA,
                (TypeDescriptor) null,
                tc);
        }

        // SYSIBM.SQLTABLES(VARCHAR(128), VARCHAR(128), VARCHAR(128), VARCHAR(4000), VARCHAR(4000))
        {

            // procedure argument names
            String[] arg_names = {
				"CATALOGNAME",
				"SCHEMANAME",
				"TABLENAME",
				"TABLETYPE",
				"OPTIONS"};

            // procedure argument types
            TypeDescriptor[] arg_types = {
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 4000),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 4000)};

            createSystemProcedureOrFunction(
                "SQLTABLES",
                sysIBMUUID,
                arg_names,
                arg_types,
				0,
				1,
                RoutineAliasInfo.READS_SQL_DATA,
                (TypeDescriptor) null,
                tc);
        }


        // SYSIBM.SQLPROCEDURECOLS(VARCHAR(128), VARCHAR(128), VARCHAR(128), VARCHAR(128), VARCHAR(4000))
        {

            // procedure argument names
            String[] arg_names = {
				"CATALOGNAME",
				"SCHEMANAME",
				"PROCNAME",
				"PARAMNAME",
				"OPTIONS"};

            // procedure argument types
            TypeDescriptor[] arg_types = {
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 4000)};

            createSystemProcedureOrFunction(
                "SQLPROCEDURECOLS",
                sysIBMUUID,
                arg_names,
                arg_types,
				0,
				1,
                RoutineAliasInfo.READS_SQL_DATA,
                (TypeDescriptor) null,
                tc);
        }

        // SYSIBM.SQLCOLUMNS(VARCHAR(128), VARCHAR(128), VARCHAR(128), VARCHAR(128), VARCHAR(4000))
        {

            // procedure argument names
            String[] arg_names = {
				"CATALOGNAME",
				"SCHEMANAME",
				"TABLENAME",
				"COLUMNNAME",
				"OPTIONS"};

            // procedure argument types
            TypeDescriptor[] arg_types = {
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 4000)};

            createSystemProcedureOrFunction(
                "SQLCOLUMNS",
                sysIBMUUID,
                arg_names,
                arg_types,
				0,
				1,
                RoutineAliasInfo.READS_SQL_DATA,
                (TypeDescriptor) null,
                tc);
        }

        // SYSIBM.SQLCOLPRIVILEGES(VARCHAR(128), VARCHAR(128), VARCHAR(128), VARCHAR(128), VARCHAR(4000))
        {

            // procedure argument names
            String[] arg_names = {
				"CATALOGNAME",
				"SCHEMANAME",
				"TABLENAME",
				"COLUMNNAME",
				"OPTIONS"};

            // procedure argument types
            TypeDescriptor[] arg_types = {
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 4000)};

            createSystemProcedureOrFunction(
                "SQLCOLPRIVILEGES",
                sysIBMUUID,
                arg_names,
                arg_types,
				0,
				1,
                RoutineAliasInfo.READS_SQL_DATA,
                (TypeDescriptor) null,
                tc);
        }

        // SYSIBM.SQLUDTS(VARCHAR(128), VARCHAR(128), VARCHAR(128), VARCHAR(128), VARCHAR(4000))
        {

            // procedure argument names
            String[] arg_names = {
				"CATALOGNAME",
				"SCHEMAPATTERN",
				"TYPENAMEPATTERN",
				"UDTTYPES",
				"OPTIONS"};

            // procedure argument types
            TypeDescriptor[] arg_types = {
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 4000)};

            createSystemProcedureOrFunction(
                "SQLUDTS",
                sysIBMUUID,
                arg_names,
                arg_types,
				0,
				1,
                RoutineAliasInfo.READS_SQL_DATA,
                (TypeDescriptor) null,
                tc);
        }

        // SYSIBM.SQLFOREIGNKEYS(VARCHAR(128), VARCHAR(128), VARCHAR(128), VARCHAR(128),
		//						 VARCHAR(128), VARCHAR(128), VARCHAR(4000))
        {

            // procedure argument names
            String[] arg_names = {
				"PKCATALOGNAME",
				"PKSCHEMANAME",
				"PKTABLENAME",
				"FKCATALOGNAME",
				"FKSCHEMANAME",
				"FKTABLENAME",
				"OPTIONS"};

            // procedure argument types
            TypeDescriptor[] arg_types = {
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 4000)};

            createSystemProcedureOrFunction(
                "SQLFOREIGNKEYS",
                sysIBMUUID,
                arg_names,
                arg_types,
				0,
				1,
                RoutineAliasInfo.READS_SQL_DATA,
                (TypeDescriptor) null,
                tc);
        }

        // SYSIBM.SQLSPECIALCOLUMNS(SMALLINT, VARCHAR(128), VARCHAR(128), VARCHAR(128),
		//						 	SMALLINT, SMALLINT, VARCHAR(4000))
        {

            // procedure argument names
            String[] arg_names = {
				"COLTYPE",
				"CATALOGNAME",
				"SCHEMANAME",
				"TABLENAME",
				"SCOPE",
				"NULLABLE",
				"OPTIONS"};

            // procedure argument types
            TypeDescriptor[] arg_types = {
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.SMALLINT),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.SMALLINT),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.SMALLINT),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 4000)};

            createSystemProcedureOrFunction(
                "SQLSPECIALCOLUMNS",
                sysIBMUUID,
                arg_names,
                arg_types,
				0,
				1,
                RoutineAliasInfo.READS_SQL_DATA,
                (TypeDescriptor) null,
                tc);
        }

        // SYSIBM.SQLGETTYPEINFO(SMALLINT, VARCHAR(4000))
        {

            // procedure argument names
            String[] arg_names = {
				"DATATYPE",
				"OPTIONS"};

            // procedure argument types
            TypeDescriptor[] arg_types = {
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.SMALLINT),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 4000)};

            createSystemProcedureOrFunction(
                "SQLGETTYPEINFO",
                sysIBMUUID,
                arg_names,
                arg_types,
				0,
				1,
                RoutineAliasInfo.READS_SQL_DATA,
                (TypeDescriptor) null,
                tc);
        }

        // SYSIBM.SQLSTATISTICS(VARCHAR(128), VARCHAR(128), VARCHAR(128),
		//						 	SMALLINT, SMALLINT, VARCHAR(4000))
        {

            // procedure argument names
            String[] arg_names = {
				"CATALOGNAME",
				"SCHEMANAME",
				"TABLENAME",
				"UNIQUE",
				"RESERVED",
				"OPTIONS"};

            // procedure argument types
            TypeDescriptor[] arg_types = {
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 128),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.SMALLINT),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.SMALLINT),
				DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 4000)};

            createSystemProcedureOrFunction(
                "SQLSTATISTICS",
                sysIBMUUID,
                arg_names,
                arg_types,
				0,
				1,
                RoutineAliasInfo.READS_SQL_DATA,
                (TypeDescriptor) null,
                tc);
        }

        // void SYSIBM.METADATA()
        {
            createSystemProcedureOrFunction(
                "METADATA",
                sysIBMUUID,
                null,
                null,
				0,
				1,
                RoutineAliasInfo.READS_SQL_DATA,
                (TypeDescriptor) null,
                tc);
        }
	}


	/*
	** Priv block code to load net work server meta data queries.
	*/

	private String spsSet;
	private final synchronized Properties getQueryDescriptions(boolean net) {
		spsSet = net ? "metadata_net.properties" : "/org/apache/derby/impl/jdbc/metadata.properties";
		return (Properties) java.security.AccessController.doPrivileged(this);
	}

	public final Object run() {
		// SECURITY PERMISSION - IP3
		Properties p = new Properties();
		try {

			// SECURITY PERMISSION - IP3
			InputStream is = getClass().getResourceAsStream(spsSet);
			p.load(is);
			is.close();
		} catch (IOException ioe) {}
		return p;
	}


	private static List newSList() {
		return java.util.Collections.synchronizedList(new java.util.LinkedList());
	}
}









