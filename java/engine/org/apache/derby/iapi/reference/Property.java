/*

   Derby - Class org.apache.derby.iapi.reference.Property

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.reference;

/**
	List of all properties understood by the system. It also has some other static fields.


	<P>
	This class exists for two reasons
	<Ol>
	<LI> To act as the internal documentation for the properties. 
	<LI> To remove the need to declare a java static field for the property
	name in the protocol/implementation class. This reduces the footprint as
	the string is final and thus can be included simply as a String constant pool entry.
	</OL>
	<P>
	This class should not be shipped with the product.

	<P>
	This class has no methods, all it contains are String's which by
	are public, static and final since they are declared in an interface.
*/

public interface Property { 

	/**
		Name of the file that contains system wide properties.
		Has to be located in ${derby.system.home} if set, otherwise ${user.dir}
	*/
	public static final String PROPERTIES_FILE = "derby.properties";


	/**
		By convention properties that must not be stored any persistent form of
		service properties start with this prefix.
	*/
	public static final String PROPERTY_RUNTIME_PREFIX = "derby.__rt.";

	/*
	** derby.service.* and related properties
	*/


	/*
	** derby.stream.* and related properties
	*/
	
	/**
		derby.stream.error.logSeverityLevel=integerValue
		<BR>
		Indicates the minimum level of severity for errors that are reported to the error stream.
		Default to 0 in a "sane" server, and SESSION_SEVERITY in the insane (and product) server.

		@see org.apache.derby.iapi.error.ExceptionSeverity#SESSION_SEVERITY
	*/
	String LOG_SEVERITY_LEVEL = "derby.stream.error.logSeverityLevel";

	/** 
	derby.infolog.append={true,false}
	<BR>
	* If the info stream goes to a file and the file already exist, it can
	* either delete the existing file or append to it.  User can specifiy
	* whether info log file should append or not by setting
	* derby.infolog.append={true/false}
	*
	* The default behavior is that the exiting file will be deleted when a new
	* info stream is started.  
	*/
	String LOG_FILE_APPEND = "derby.infolog.append";

	/*
	** derby.service.* and related properties
	*/
	/**
		derby.system.home
		<BR>
		Property name for the home directory. Any relative path in the
		system should be accessed though this property
	*/
	String SYSTEM_HOME_PROPERTY = "derby.system.home";

	/**
		derby.system.bootAll
		<BR>
		Automatically boot any services at start up time. When set to true
		this services will  be booted at startup, otherwise services
		will be booted on demand.
	*/
	String BOOT_ALL = "derby.system.bootAll";

	/**
		derby.database.noAutoBoot
		<BR>
		Don't automatically boot this service at start up time. When set to true
		this service will only be booted on demand, otherwise the service
		will be booted at startup time if possible.
	*/
	String NO_AUTO_BOOT = "derby.database.noAutoBoot";
    
	/**
		derby.__deleteOnCreate
		<BR>
		Before creating this service delete any remenants (e.g. the directory)
		of a previous service at the same location.

		<P>
		<B>INTERNAL USE ONLY</B> 
	*/
	String DELETE_ON_CREATE = "derby.__deleteOnCreate";

	/**
        derby.database.forceDatabaseLock
		<BR>
        Cloudscape attempts to prevent two instances of Cloudscape from booting
        the same database with the use of a file called db.lck inside the 
        database directory.

        On some platforms, Cloudscape can successfully prevent a second 
        instance of Cloudscape from booting the database, and thus prevents 
        corruption. If this is the case, you will see an SQLException like the
        following:

        ERROR XJ040: Failed to start database 'toursDB', see the next exception
        for details.
        ERROR XSDB6: Another instance of Cloudscape may have already booted the
        database C:\databases\toursDB.

        The error is also written to the information log.

        On other platforms, Cloudscape issues a warning message if an instance
        of Cloudscape attempts to boot a database that may already have a
        running instance of Cloudscape attached to it.
        However, it does not prevent the second instance from booting, and thus
        potentially corrupting, the database.

        If a warning message has been issued, corruption may already have 
        occurred.


        NOTE: When you are using Cloudview, error messages appear in the 
        console or operating system window from which Cloudview was started.

        The warning message looks like this:

        WARNING: Cloudscape (instance 80000000-00d2-3265-de92-000a0a0a0200) is
        attempting to boot the database /export/home/sky/wombat even though
        Cloudscape (instance 80000000-00d2-3265-8abf-000a0a0a0200) may still be
        active. Only one instance of Cloudscape
        should boot a database at a time. Severe and non-recoverable corruption
        can result and may have already occurred.

        The warning is also written to the information log.

        This warning is primarily a Technical Support aid to determine the 
        cause of corruption. However, if you see this warning, your best 
        choice is to close the connection and exit the JVM. This minimizes the
        risk of a corruption. Close all instances of Cloudscape, then restart
        one instance of Cloudscape and shut down the database properly so that
        the db.lck file can be removed. The warning message continues to appear
        until a proper shutdown of the Cloudscape system can delete the db.lck
        file.

        If the "derby.database.forceDatabaseLock" property is set to true
        then this default behavior is altered on systems where cloudscape cannot
        prevent this dual booting.  If the to true, then if the platform does
        not provide the ability for cloudscape to guarantee no double boot, and
        if cloudscape finds a db.lck file when it boots, it will throw an 
        exception (TODO - mikem - add what exception), leave the db.lck file
        in place and not boot the system.  At this point the system will not 
        boot until the db.lck file is removed by hand.  Note that this 
        situation can arise even when 2 VM's are not accessing the same
        cloudscape system.  Also note that if the db.lck file is removed by 
        hand while a VM is still accessing a derby.database, then there 
        is no way for cloudscape to prevent a second VM from starting up and 
        possibly corrupting the database.  In this situation no warning 
        message will be logged to the error log.

        To disable the default behavior of the db.lck file set property as 
        follows:

        derby.database.forceDatabaseLock=true

	*/
	String FORCE_DATABASE_LOCK = "derby.database.forceDatabaseLock";


	/*
	** derby.locks.* and related properties
	*/

	String LOCKS_INTRO = "derby.locks.";

	/**
		derby.locks.escalationThreshold
		<BR>
		The number of row locks on a table after which we escalate to
		table locking.  Also used by the optimizer to decide when to
		start with table locking.  The String value must be convertible
		to an int.
	 */
	String LOCKS_ESCALATION_THRESHOLD = "derby.locks.escalationThreshold";

	/**
		The default value for LOCKS_ESCALATION_THRESHOLD
	 */
	int DEFAULT_LOCKS_ESCALATION_THRESHOLD = 5000;

	/**
		The minimum value for LOCKS_ESCALATION_THRESHOLD
	 */
	int MIN_LOCKS_ESCALATION_THRESHOLD = 100;

	/**
		Configuration parameter for deadlock timeouts, set in seconds.
	*/
	public static final String DEADLOCK_TIMEOUT = "derby.locks.deadlockTimeout";

	/**
		Default value for deadlock timesouts (20 seconds)
	*/
	public static final int DEADLOCK_TIMEOUT_DEFAULT = 20;

	/**
		Default value for wait timeouts (60 seconds)
	*/
	public static final int WAIT_TIMEOUT_DEFAULT = 60;

	/**
		Turn on lock monitor to help debug deadlocks.  Default value is OFF.
		With this property turned on, all deadlocks will cause a tracing to be
		output to the db2j.LOG file.
		<BR>
		This property takes effect dynamically.
	 */
	public static final String DEADLOCK_MONITOR = "derby.locks.monitor";

	/**
		Turn on deadlock trace to help debug deadlocks.
        
        Effect 1: This property only takes effect if DEADLOCK_MONITOR is turned
        ON for deadlock trace.  With this property turned on, each lock object
        involved in a deadlock will output its stack trace to db2j.LOG.
        
        Effect 2: When a timeout occurs, a lockTable dump will also be output
        to db2j.LOG.  This acts independent of DEADLOCK_MONITOR.
		<BR>
		This property takes effect dynamically.
	 */
	public static final String DEADLOCK_TRACE = "derby.locks.deadlockTrace";

	/**
		Configuration parameter for lock wait timeouts, set in seconds.
	*/
	public static final String LOCKWAIT_TIMEOUT = "derby.locks.waitTimeout";

	/*
	** db2j.database.*
	*/
	
	/**
		derby.database.classpath
		<BR>
		Consists of a series of two part jar names.
	*/
	String DATABASE_CLASSPATH = "derby.database.classpath";

	/**
		internal use only, passes the database classpathinto the class manager
	*/
	String BOOT_DB_CLASSPATH = PROPERTY_RUNTIME_PREFIX + "database.classpath";



	/**
		derby.database.propertiesOnly
	*/
	String DATABASE_PROPERTIES_ONLY = "derby.database.propertiesOnly";

	/*
	** derby.storage.*
	*/

    /**
     * Creation of an access factory should be done with no logging.
	 * This is a run-time property that should not make it to disk
	 * in the service.properties file.
     **/
	public static final String CREATE_WITH_NO_LOG =
		PROPERTY_RUNTIME_PREFIX + "storage.createWithNoLog";

    /**
     * The page size to create a table or index with.  Must be a multiple
     * of 2k, usual choices are: 2k, 4k, 8k, 16k, 32k, 64k.  The default
     * if property is not set is 4k.
     **/
    public static final String PAGE_SIZE_PARAMETER = "derby.storage.pageSize";

    /**
     * The default page size to use for tables that contain a long column.
     **/
    public static final String PAGE_SIZE_DEFAULT_LONG = "32768";

    /**
     * The bump threshold for pages sizes for create tables
     * If the approximate column sizes of a table is greater than this
     * threshold, the page size for the tbl is bumped to PAGE_SIZE_DEFAULT_LONG
     * provided the page size is not already specified as a property
     **/
    public static final int TBL_PAGE_SIZE_BUMP_THRESHOLD = 4096;

    /**
     * The bump threshold for pages size for index.
     * If the approximate key columns of an index is greater than this
     * threshold, the page size for the index is bumped to PAGE_SIZE_DEFAULT_LONG
     * provided the page size is not already specified as a property
     **/
    public static final int IDX_PAGE_SIZE_BUMP_THRESHOLD = 1024;

    /**
     * In cloudscape products which support Row Level Locking (rll), use this 
     * property to disable rll.  Application's which use rll will use more 
     * system resources, so if an application knows that it does not need rll 
     * then it can use this system property to force all locking in the system 
     * to lock at the table level.
     * 
     * This property can be set to the boolean values "true" or "false".  
     * Setting the property to true is the same as not setting the property at 
     * all, and will result in rll being enabled.  Setting the property to 
     * false disables rll.
     *
     **/
	public static final String ROW_LOCKING = "derby.storage.rowLocking";

	/**
		derby.storage.propertiesId
		<BR>
		Stores the id of the conglomerate that holds the per-database
		properties. Is stored in the service.properties file.

		<P>
		<B>INTERNAL USE ONLY</B> 
	*/
	String PROPERTIES_CONGLOM_ID = "derby.storage.propertiesId";

	/**
		derby.storage.tempDirectory
		<BR>
		Sets the temp directory for a database.
		<P>
	*/
	String STORAGE_TEMP_DIRECTORY = "derby.storage.tempDirectory";

    /**
     * derby.storage.dataNotSyncedAtCheckPoint
     * <p>
     * When set, the store system will not force a sync() call on the
     * containers during a checkpoint.
     * <p>
     * An internal debug system only flag.  The recovery system will not
     * work properly if this flag is enabled, it is provided to do performance
     * debugging to see whether the system is I/O bound based on checkpoint
     * synchronous I/O.
     * <p>
     *
     **/
	public static final String STORAGE_DATA_NOT_SYNCED_AT_CHECKPOINT = 
        "derby.storage.dataNotSyncedAtCheckPoint";

    /**
     * derby.storage.dataNotSyncedAtAllocation
     * <p>
     * When set, the store system will not force a sync() call on the
     * containers when pages are allocated.
     * <p>
     * An internal debug system only flag.  The recovery system will not
     * work properly if this flag is enabled, it is provided to do performance
     * debugging to see whether the system is I/O bound based on page allocation
     * synchronous I/O.
     * <p>
     *
     **/
	public static final String STORAGE_DATA_NOT_SYNCED_AT_ALLOCATION = 
        "derby.storage.dataNotSyncedAtAllocation";

    /**
     * derby.storage.logNotSynced
     * <p>
     * When set, the store system will not force a sync() call on the log at 
     * commit.
     * <p>
     * An internal debug system only flag.  The recovery system will not
     * work properly if this flag is enabled, it is provided to do performance
     * debugging to see whether the system is I/O bound based on log file
     * synchronous I/O.
     * <p>
     *
     **/
	public static final String STORAGE_LOG_NOT_SYNCED = 
        "derby.storage.logNotSynced";

	/**
     * derby.storage.fileSyncTransactionLog
     * <p>
     * When set, the store system will use sync() call on the log at 
     * commit instead of doing  a write sync on all writes to  the log;
	 * even if the write sync mode (rws) is supported in the JVM. 
     * <p>
     *
     **/
	public static final String FILESYNC_TRANSACTION_LOG = 
        "derby.storage.fileSyncTransactionLog";


	/**
	 *	derby.storage.logArchiveMode
	 *<BR>
	 *used to identify whether the log is being archived for the database or not.
	 *  It Is stored in the service.properties file.
	 * 
     * This property can be set to the boolean values "true" or "false".  
     * Setting the property to true means log is being archived, which could be 
	 * used for roll-forward recovery. Setting the property to 
     * false disables log archive mode.
	 *<P>
	 *<B>INTERNAL USE ONLY</B> 
	 */
	String LOG_ARCHIVE_MODE = "derby.storage.logArchiveMode";


	/**
	 *	derby.storage.logDeviceWhenBackedUp
	 *<BR>
	 *  This property indicates the logDevice location(path) when the backup was 
	 *  taken, used to restore the log to the same location while restoring from
	 *  backup.
	 *<P>
	 *<B>INTERNAL USE ONLY</B> 
	 */
	String LOG_DEVICE_AT_BACKUP = "derby.storage.logDeviceWhenBackedUp";

    /**
     *  derby.subSubProtocol.xxx
     *<p>
     *
     * A new subsubprotocol can be defined by specifying the class that handles storage for the
     * subsubprotocol by implementing the
     * {@link org.apache.derby.io.StorageFactory StorageFactory} or
     * {@link org.apache.derby.io.WritableStorageFactory WritableStorageFactory} interface. This
     * is done using a property named db2j.subsubprotocol.<i>xxx</i> where <i>xxx</i> is the subsubprotocol name.
     * Subsubprotocol names are case sensitive and must be at least 3 characters in length.
     *<p>
     *
     * For instance:
     *<br>
     * derby.subSubProtocol.mem=com.mycompany.MemStore
     *<br>
     * defines the "mem" subsubprotocol with class com.mycompany.MemStore as its StorageFactory implementation.
     * A database implemented using this subsubprotocol can be opened with the URL "jdbc:derby:mem:myDatabase".
     *<p>
     *
     * Subsubprotocols "directory", "classpath", "jar", "http", and "https" are built in and may not be overridden.
     */
    String SUB_SUB_PROTOCOL_PREFIX = "derby.subSubProtocol.";

	/*
	** derby.language.*
	*/

	/**
	 * The size of the table descriptor cache used by the
	 * data dictionary.  Database.  Static.
	 * <p>
	 * Undocumented.
	 */
	String	LANG_TD_CACHE_SIZE = "derby.language.tableDescriptorCacheSize";
	int		LANG_TD_CACHE_SIZE_DEFAULT = 64;

	/**
	 * The size of the stored prepared statment descriptor cache 
	 * used by the data dictionary.  Database.  Static.
	 * <p>
	 * Externally visible.
	 */
	String	LANG_SPS_CACHE_SIZE = "derby.language.spsCacheSize";
	int		LANG_SPS_CACHE_SIZE_DEFAULT =32;

	/**
	  derby.language.stalePlanCheckInterval

	  <P>
	  This property tells the number of times a prepared statement should
	  be executed before checking whether its plan is stale.  Database.
	  Dynamic.
	  <P>
	  Externally visible.
	 */
	String LANGUAGE_STALE_PLAN_CHECK_INTERVAL =
								"derby.language.stalePlanCheckInterval";

	
	/** Default value for above */
	int DEFAULT_LANGUAGE_STALE_PLAN_CHECK_INTERVAL = 100;

	/** Minimum value for above */
	int MIN_LANGUAGE_STALE_PLAN_CHECK_INTERVAL = 5;


	/*
		Statement plan cache size
		By default, 100 statements are cached
	 */
	String STATEMENT_CACHE_SIZE = "derby.language.statementCacheSize";
	int STATEMENT_CACHE_SIZE_DEFAULT = 100;

  /* some static fields */
	public static final String DEFAULT_USER_NAME = "APP";
	public static final String DATABASE_MODULE = "org.apache.derby.database.Database";

	public static final String
	DEFAULT_CONNECTION_MODE_PROPERTY = "derby.database.defaultConnectionMode";

	public static final String NO_ACCESS = "noAccess";
	public static final String READ_ONLY_ACCESS = "readOnlyAccess";
	public static final String FULL_ACCESS = "fullAccess";
	public static final String DEFAULT_ACCESS = FULL_ACCESS;

	public static final String
	READ_ONLY_ACCESS_USERS_PROPERTY = "derby.database.readOnlyAccessUsers";

	public static final String
	FULL_ACCESS_USERS_PROPERTY = "derby.database.fullAccessUsers";

	/*
	** Authentication
	*/

	// This is the property that turn on/off authentication
	public static final String REQUIRE_AUTHENTICATION_PARAMETER =
								"derby.connection.requireAuthentication";

	public static final String AUTHENTICATION_PROVIDER_PARAMETER =
								"derby.authentication.provider";

	// This is the user property used by Cloudscape and LDAP schemes
	public static final String USER_PROPERTY_PREFIX = "derby.user.";

	// These are the different built-in providers Cloudscape supports

	public static final String AUTHENTICATION_PROVIDER_BUILTIN =
								"BUILTIN";

	public static final String AUTHENTICATION_PROVIDER_LDAP =
								"LDAP";

	public static final String AUTHENTICATION_SERVER_PARAMETER =
								"derby.authentication.server";

	/*
	** Log
	*/

	/**
		Property name for specifying log switch interval
		@see #checkpoint
	 */
	public static final String LOG_SWITCH_INTERVAL = "derby.storage.logSwitchInterval";

	/**
		Property name for specifying checkpoint interval
		@see #checkpoint
	 */
	public static final String CHECKPOINT_INTERVAL = "derby.storage.checkpointInterval";

	/**
		Property name for specifying log archival location
		@see #logArchived
	 */
	public static final String LOG_ARCHIVAL_DIRECTORY = "derby.storage.logArchive";

	/**
		Property name for specifying log Buffer Size
	 */
	public static final String LOG_BUFFER_SIZE = "derby.storage.logBufferSize";
	
	
	/*
	** Upgrade
	*/
	    
	/**
		db2j.inRestore
		<BR>
		This Property is used to indicate that we are in restore mode if
		if the system is doing a restore from backup.
		Used internally to set flags to indicate that service is not booted.
		<P>
		<B>INTERNAL USE ONLY</B> 
	*/
	String IN_RESTORE_FROM_BACKUP = PROPERTY_RUNTIME_PREFIX  + "inRestore";
	
		    
	/**
		db2j.deleteRootOnError
		<BR>
		If we a new root is created while doing restore from backup,
		it should be deleted if a error occur before we could complete restore 
		successfully.
		<P>
		<B>INTERNAL USE ONLY</B> 
	*/
	String DELETE_ROOT_ON_ERROR  = PROPERTY_RUNTIME_PREFIX  + "deleteRootOnError";
	
	public static final String HTTP_DB_FILE_OFFSET = "db2j.http.file.offset";
	public static final String HTTP_DB_FILE_LENGTH = "db2j.http.file.length";
	public static final String HTTP_DB_FILE_NAME =   "db2j.http.file.name";

    /**
     * derby.drda.startNetworkServer
     *<BR>
     * If true then we will attempt to start a DRDA network server when Cloudscape boots,
     * turning the current JVM into a server.
     *<BR>
     * Default: false
     */
    public static final String START_DRDA = "derby.drda.startNetworkServer";

    /**
     * derby.drda.logConnections
     *<BR>
     * Indicates whether to log connections and disconnections.
     *<BR>
     * Default: false
     */
	public final static String DRDA_PROP_LOGCONNECTIONS = "derby.drda.logConnections";
    /**
     * derby.drda.traceAll
     *<BR>
     * Turns tracing on for all sessions.
     *<BR>
     * Default: false
     */
	public final static String DRDA_PROP_TRACEALL = "derby.drda.traceAll";
	public final static String DRDA_PROP_TRACE = "derby.drda.trace";

    /**
     * derby.drda.traceDirectory
     *<BR>
     * The directory used for network server tracing files.
     *<BR>
     * Default: if the derby.system.home property has been set,
     * it is the default. Otherwise, the default is the current directory.
     */
	public final static String DRDA_PROP_TRACEDIRECTORY = "derby.drda.traceDirectory";

	public final static String DRDA_PROP_MINTHREADS = "derby.drda.minThreads";
	public final static String DRDA_PROP_MAXTHREADS = "derby.drda.maxThreads";
	public final static String DRDA_PROP_TIMESLICE = "derby.drda.timeSlice";

    /**
     * derby.drda.portNumber
     *<BR>
     * The port number used by the network server.
     */
	public final static String DRDA_PROP_PORTNUMBER = "derby.drda.portNumber";
	public final static String DRDA_PROP_HOSTNAME = "derby.drda.host";

	/**
	 * derby.drda.keepAlive
	 *
	 *<BR>
	 * client socket setKeepAlive value
	 */
	public final static String DRDA_PROP_KEEPALIVE = "derby.drda.keepAlive";
	
	/*
	** Internal properties, mainly used by Monitor.
	*/
	public static final String SERVICE_PROTOCOL = "derby.serviceProtocol";
	public static final String SERVICE_LOCALE = "derby.serviceLocale";

}
