/*

   Derby - Class org.apache.derby.shared.common.reference.Property

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

package org.apache.derby.shared.common.reference;

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
	 * Name of the file that contains system wide properties. Has to be located
	 * in ${derby.system.home} if set, otherwise ${user.dir}
	 */
	public static final String PROPERTIES_FILE = "derby.properties";


	/**
		By convention properties that must not be stored any persistent form of
		service properties start with this prefix. These properties are not documented.
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

		@see org.apache.derby.shared.common.error.ExceptionSeverity#SESSION_SEVERITY
	*/
	String LOG_SEVERITY_LEVEL = "derby.stream.error.logSeverityLevel";
    
    /**
      * derby.stream.error.ExtendedDiagSeverityLevel=integerValue
      * <BR>
      * Indicates the minimum level of severity for errors that are reported thread dump information
      * and diagnosis information depends on jvm vender.
      * Default to SESSION_SEVERITY(40000).
      *
     */
    String EXT_DIAG_SEVERITY_LEVEL = "derby.stream.error.extendedDiagSeverityLevel";
    /**
     * derby.stream.error.logBootTrace
     * <BR>
     * defaults to false. If set to true logs a stack trace to 
     * the error stream on successful boot or shutdown.
     * This can be useful when trying to debug dual boot 
     * scenarios especially with multiple class loaders.
     * 
     */
	
	String LOG_BOOT_TRACE = "derby.stream.error.logBootTrace";
    
        /**
		derby.stream.error.style=<b>The error stream error style.</b>
        <b>rollingFile<b> is the only file currently supported.
		Takes precendence over derby.stream.error.file.
		Takes precendence over derby.stream.error.method.
		Takes precendence over derby.stream.error.field
	*/	
	String ERRORLOG_STYLE_PROPERTY = "derby.stream.error.style";

        /**
		derby.stream.error.file=<b>absolute or relative error log filename</b>
		Takes precendence over derby.stream.error.method.
		Takes precendence over derby.stream.error.field
	*/
	
	String ERRORLOG_FILE_PROPERTY = "derby.stream.error.file";

        /**
		derby.stream.error.method=
			<className>.<methodName> returning an OutputStream or Writer object
		Takes precendence over derby.stream.error.field
	*/
	
	String ERRORLOG_METHOD_PROPERTY = "derby.stream.error.method";

        /**
		derby.stream.error.field=
			className.fieldName returning an OutputStream or Writer object
	*/
	
	String ERRORLOG_FIELD_PROPERTY = "derby.stream.error.field";

        /**
		derby.stream.error.rollingfile.pattern=<b>the pattern</b>
        A pattern consists of a string that includes the following special
        components that will be replaced at runtime:
        <UL>
        <LI>    "/"    the local pathname separator 
        <LI>     "%t"   the system temporary directory
        <LI>     "%h"   the value of the "user.home" system property
        <LI>     "%d"   the value of the "derby.system.home" system property
        <LI>     "%g"   the generation number to distinguish rotated logs
        <LI>     "%u"   a unique number to resolve conflicts
        <LI>     "%%"   translates to a single percent sign "%"
        </UL>
        If no "%g" field has been specified and the file count is greater
        than one, then the generation number will be added to the end of
        the generated filename, after a dot.
        <P> 
        Thus for example a pattern of "%t/java%g.log" with a count of 2
        would typically cause files to be written on Solaris to 
        /var/tmp/java0.log and /var/tmp/java1.log whereas on Windows 95 they
        would be typically written to C:\TEMP\java0.log and C:\TEMP\java1.log
        <P> 
        Generation numbers follow the sequence 0, 1, 2, etc.
        <P>
        Normally the "%u" unique field is set to 0.  However, if the <tt>FileHandler</tt>
        tries to open the filename and finds the file is currently in use by
        another process it will increment the unique number field and try
        again.  This will be repeated until <tt>FileHandler</tt> finds a file name that
        is  not currently in use. If there is a conflict and no "%u" field has
        been specified, it will be added at the end of the filename after a dot.
        (This will be after any automatically added generation number.)
        <P>
        Thus if three processes were all trying to output to fred%u.%g.txt then 
        they  might end up using fred0.0.txt, fred1.0.txt, fred2.0.txt as
        the first file in their rotating sequences.
        <P>
        Note that the use of unique ids to avoid conflicts is only guaranteed
        to work reliably when using a local disk file system.
        <P>
        The default pattern is "%d/derby-%g.log"
	*/	
	String ERRORLOG_ROLLINGFILE_PATTERN_PROPERTY = "derby.stream.error.rollingFile.pattern";

        /**
		derby.stream.error.rollingfile.limit=<b>the rolling file size limit</b>
        <P>
        The default limit is 1024000
	*/
	
	String ERRORLOG_ROLLINGFILE_LIMIT_PROPERTY = "derby.stream.error.rollingFile.limit";

        /**
		derby.stream.error.rollingfile.count=<b>the rolling file count</b>
        <P>
        The default count is 10
	*/
	
	String ERRORLOG_ROLLINGFILE_COUNT_PROPERTY = "derby.stream.error.rollingFile.count";

    
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
        Derby attempts to prevent two instances of Derby from booting
        the same database with the use of a file called db.lck inside the 
        database directory.

        On some platforms, Derby can successfully prevent a second 
        instance of Derby from booting the database, and thus prevents 
        corruption. If this is the case, you will see an SQLException like the
        following:

        ERROR XJ040: Failed to start database 'toursDB', see the next exception
        for details.
        ERROR XSDB6: Another instance of Derby may have already booted the
        database C:\databases\toursDB.

        The error is also written to the information log.

        On other platforms, Derby issues a warning message if an instance
        of Derby attempts to boot a database that may already have a
        running instance of Derby attached to it.
        However, it does not prevent the second instance from booting, and thus
        potentially corrupting, the database.

        If a warning message has been issued, corruption may already have 
        occurred.


        The warning message looks like this:

        WARNING: Derby (instance 80000000-00d2-3265-de92-000a0a0a0200) is
        attempting to boot the database /export/home/sky/wombat even though
        Derby (instance 80000000-00d2-3265-8abf-000a0a0a0200) may still be
        active. Only one instance of Derby 
        should boot a database at a time. Severe and non-recoverable corruption
        can result and may have already occurred.

        The warning is also written to the information log.

        This warning is primarily a Technical Support aid to determine the 
        cause of corruption. However, if you see this warning, your best 
        choice is to close the connection and exit the JVM. This minimizes the
        risk of a corruption. Close all instances of Derby, then restart
        one instance of Derby and shut down the database properly so that
        the db.lck file can be removed. The warning message continues to appear
        until a proper shutdown of the Derby system can delete the db.lck
        file.

        If the "derby.database.forceDatabaseLock" property is set to true
        then this default behavior is altered on systems where Derby cannot
        prevent this dual booting.  If the to true, then if the platform does
        not provide the ability for Derby to guarantee no double boot, and
        if Derby finds a db.lck file when it boots, it will throw an 
        exception (TODO - mikem - add what exception), leave the db.lck file
        in place and not boot the system.  At this point the system will not 
        boot until the db.lck file is removed by hand.  Note that this 
        situation can arise even when 2 VM's are not accessing the same
        Derby system.  Also note that if the db.lck file is removed by 
        hand while a VM is still accessing a derby.database, then there 
        is no way for Derby to prevent a second VM from starting up and 
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

        Undocumented.
	*/
	String BOOT_DB_CLASSPATH = PROPERTY_RUNTIME_PREFIX + "database.classpath";



	/**
		derby.database.propertiesOnly
	*/
	String DATABASE_PROPERTIES_ONLY = "derby.database.propertiesOnly";

    /**
     * This property is private to Derby.
     * This property is forcibly set by the Network Server to override
     * any values which the user may have set. This property is only used to
     * parameterize the Basic security policy used by the Network Server.
     * This property is the location of the derby jars.
     **/
    public static final String DERBY_INSTALL_URL = "derby.install.url";

    /**
     * This property is private to Derby.
     * This property is forcibly set by the Network Server to override
     * any values which the user may have set. This property is only used to
     * parameterize the Basic security policy used by the Network Server.
     * This property is the hostname which the server uses.
     **/
    public static final String DERBY_SECURITY_HOST = "derby.security.host";

    /**
     * This property is private to Derby.
     * This property is forcibly set by the Network Server to override
     * any values which the user may have set. This property is only used to
     * parameterize the Basic security policy used by the Network Server.
     * This property is the port number which the server listens to.
     */
    public static final String DERBY_SECURITY_PORT = "derby.security.port";

	/*
	** derby.storage.*
	*/

    /**
     * <p>
     * Creation of an access factory should be done with no logging.
	 * This is a run-time property that should not make it to disk
	 * in the service.properties file.
     * </p>
     *
     * <p>
     * Undocumented.
     * </p>
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
     * Derby supports Row Level Locking (rll),  but you can use this 
     * property to disable rll.  Applications which use rll will use more 
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
     * derby.system.durability
     * <p>
     * Currently the only valid supported case insensitive value is 'test' 
     * Note, if this property is set to any other value other than 'test', this 
     * property setting is ignored
     * 
     * In the future, this property can be used to set different modes - for 
     * example a form of relaxed durability where database can recover to a 
     * consistent state, or to enable some kind of in-memory mode.
     * <BR>
     * When set to 'test', the store system will not force sync calls in the 
     * following cases  
     * - for the log file at each commit
     * - for the log file before data page is forced to disk
     * - for page allocation when file is grown
     * - for data writes during checkpoint
     * 
     * That means
     * - a commit no longer guarantees that the transaction's modification
     *   will survive a system crash or JVM termination
     * - the database may not recover successfully upon restart
     * - a near full disk at runtime may cause unexpected errors
     * - database can be in an inconsistent state
     * <p>
     * This setting is provided for performance reasons and should ideally
     * only be used when the system can withstand the above consequences.
     * <BR> 
     * One sample use would be to use this mode (derby.system.durability=test)
     * when using Derby as a test database, where high performance is required
     * and the data is not very important
     * <BR>
     * Valid supported values are test
     * <BR>
     * Example
     * derby.system.durability=test
     * One can set this as a command line option to the JVM when starting the
     * application or in the derby.properties file. It is a system level 
     * property.
     * <BR>
     * This property is static; if you change it while Derby is running, 
     * the change does not take effect until you reboot.  
     */
	public static final String DURABILITY_PROPERTY = 
        "derby.system.durability";
 	
    /**
     * This is a value supported for derby.system.durability
     * When derby.system.durability=test, the storage system does not
     * force syncs and the system may not recover. It is also possible that
     * the database might be in an inconsistent state
     * @see #DURABILITY_PROPERTY
     */
    public static final String DURABILITY_TESTMODE_NO_SYNC = "test";
    
	/**
     * derby.storage.fileSyncTransactionLog
     * <p>
     * When set, the store system will use sync() call on the log at 
     * commit instead of doing  a write sync on all writes to  the log;
	 * even if the write sync mode (rws) is supported in the JVM. 
     * </p>
     *
	 * <p>
	 * Undocumented.
     * </p>
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
     * derby.module.modulename
     * <P>
     * Defines a new module. Modulename is a name used when loading the definition
     * of a module, it provides the linkage to other properties used to define the
     * module, derby.env.jdk.modulename and derby.env.classes.modulename.
     * 
     * The value is a Java class name that implements functionality required by
     * the other parts of a Derby system or database. The class can optionally implement
     * these classes to control its use and startup.
     * <UL>
     * <LI> org.apache.derby.iapi.services.monitor.ModuleControl
     * <LI> org.apache.derby.iapi.services.monitor.ModuleSupportable
     * </UL>
     */
    String MODULE_PREFIX = "derby.module.";

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
    
    
    /**
     * Declare a minimum JDK level the class for a module or sub sub protocol supports.
     * Set to an integer value from the JVMInfo class to represent a JDK.
     * If the JDK is running at a lower level than the class requires
     * then the class will not be loaded and will not be used.
     * 
     * If there are multiple modules classes implementing the same functionality
     * and supported by the JVM, then the one with the highest JDK
     * requirements will be selected. This functionality is not present for
     * sub sub protocol classes yet.
     * 
     * See org.apache.derby.shared.common.info.JVMInfo.JDK_ID
     */
    String MODULE_ENV_JDK_PREFIX = "derby.env.jdk.";

    /**
     * Declare a set of classes that the class for a module or sub sub protocol requires.
     * Value is a comma separated list of classes. If the classes listed are not
     * loadable by the virtual machine then the module class will not be loaded and will not be used.
    */
    String MODULE_ENV_CLASSES_PREFIX = "derby.env.classes.";

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
     * The size of the permissions cache used by the data dictionary.
     * Database.  Static.
	 * <p>
	 * Undocumented.
	 */
	String	LANG_PERMISSIONS_CACHE_SIZE = "derby.language.permissionsCacheSize";
	int		LANG_PERMISSIONS_CACHE_SIZE_DEFAULT = 64;
	/**
     * <p>
	 * The size of the stored prepared statment descriptor cache 
	 * used by the data dictionary.  Database.  Static.
	 * </p>
     *
     * <p>
	 * Externally visible but undocumented.
	 * </p>
	 */
	String	LANG_SPS_CACHE_SIZE = "derby.language.spsCacheSize";
	int		LANG_SPS_CACHE_SIZE_DEFAULT =32;

	/**
	 * The size of the sequence generator cache 
	 * used by the data dictionary.  Database.  Static.
	 * <p>
	 * Externally visible and documented.
	 */
	String	LANG_SEQGEN_CACHE_SIZE = "derby.language.sequenceGeneratorCacheSize";
	int		LANG_SEQGEN_CACHE_SIZE_DEFAULT = 1000;

	/**
	 * Name of the implementation of SequencePreallocator which is used
     * to tune how many values Derby pre-allocates for identity columns
     * and sequences. Database.  Static.
	 * <p>
	 * Externally visible.
	 */
	String	LANG_SEQUENCE_PREALLOCATOR = "derby.language.sequence.preallocator";
    
	/**
	  derby.language.stalePlanCheckInterval

	  <P>
	  This property tells the number of times a prepared statement should
	  be executed before checking whether its plan is stale.  Database.
	  Dynamic.
	  <P>
	  Externally visible but undocumented.
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

    /**
     * Tells if automatic index statistics update is enabled (default is true).
     */
    String STORAGE_AUTO_INDEX_STATS = "derby.storage.indexStats.auto";

    /**
     * Tells if activities related to automatic index statistics update should
     * be written to the Derby system log file (derby.log).
     */
    String STORAGE_AUTO_INDEX_STATS_LOGGING = "derby.storage.indexStats.log";

    /**
     * Tells if more detailed activities related to automatic index statistics
     * update should be traced. Accepted values are: *off*, stdout, log, both
     */
    String STORAGE_AUTO_INDEX_STATS_TRACING = "derby.storage.indexStats.trace";

    /**
     * <p>
     * Specifies the lower threshold for the number of rows in a table before
     * creating statistics for the associated indexes.
     * </p>
     *
     * <p>
     * <em>NOTE:</em> This is a debug property which will be removed or renamed.
     * </p>
     *
     * <p>
     * Undocumented.
     * </p>
     */
    String STORAGE_AUTO_INDEX_STATS_DEBUG_CREATE_THRESHOLD =
            "derby.storage.indexStats.debug.createThreshold";
    int STORAGE_AUTO_INDEX_STATS_DEBUG_CREATE_THRESHOLD_DEFAULT = 100;

    /**
     * <p>
     * Specifies the lower threshold for the absolute difference between the
     * row estimate for the table and the row estimate for the index before
     * creating statistics for the associated indexes.
     * </p>
     *
     * <p>
     * <em>NOTE:</em> This is a debug property which will be removed or renamed.
     * </p>
     *
     * <p>
     * Undocumented.
     * </p>
     */
    String STORAGE_AUTO_INDEX_STATS_DEBUG_ABSDIFF_THRESHOLD =
            "derby.storage.indexStats.debug.absdiffThreshold";
    int STORAGE_AUTO_INDEX_STATS_DEBUG_ABSDIFF_THRESHOLD_DEFAULT = 1000;

    /**
     * <p>
     * Specifies the lower threshold for the logarithmical (natural logarithm e)
     * difference between the row estimate for the table and the row estimate
     * for the index before creating statistics for the associated indexes.
     * </p>
     *
     * <p>
     * <em>NOTE:</em> This is a debug property which will be removed or renamed.
     * </p>
     *
     * <p>
     * Undocumented.
     * </p>
     */
    String STORAGE_AUTO_INDEX_STATS_DEBUG_LNDIFF_THRESHOLD =
            "derby.storage.indexStats.debug.lndiffThreshold";
    double STORAGE_AUTO_INDEX_STATS_DEBUG_LNDIFF_THRESHOLD_DEFAULT = 1.0;

    /**
     * <p>
     * Specifies the size of the work unit queue in the index statistics update
     * daemon.
     * </p>
     *
     * <p>
     * <em>NOTE:</em> This is a debug property which will be removed or renamed.
     * </p>
     *
     * <p>
     * Undocumented.
     * </p>
     */
    String STORAGE_AUTO_INDEX_STATS_DEBUG_QUEUE_SIZE =
            "derby.storage.indexStats.debug.queueSize";
    int STORAGE_AUTO_INDEX_STATS_DEBUG_QUEUE_SIZE_DEFAULT = 20;

    /**
     * <p>
     * Specifies whether to revert to 10.8 behavior and keep disposable stats.
     * </p>
     *
     * <p>
     * <em>NOTE:</em> This is a debug property which will be removed or renamed.
     * </p>
     *
     * <p>
     * Undocumented.
     * </p>
     */
    String STORAGE_AUTO_INDEX_STATS_DEBUG_KEEP_DISPOSABLE_STATS =
            "derby.storage.indexStats.debug.keepDisposableStats";

	/*
	** Transactions
	*/

    /** The property name used to get the default value for XA transaction
      * timeout in seconds. Zero means no timout.
      */
    String PROP_XA_TRANSACTION_TIMEOUT = "derby.jdbc.xaTransactionTimeout";

    /** The default value for XA transaction timeout if the corresponding
      * property is not found in system properties. Zero means no timeout.
      */
    int DEFAULT_XA_TRANSACTION_TIMEOUT = 0;


  /* some static fields */
	public static final String DEFAULT_USER_NAME = "APP";
	public static final String DATABASE_MODULE = "org.apache.derby.database.Database";

	/*
		Property to enable Grant & Revoke SQL authorization. Introduced in Derby 10.2
		release. New databases and existing databases (in Derby 10.2) still use legacy
		authorization by default and by setting this property to true could request for
		SQL standard authorization model.
	 */
	public static final String
	SQL_AUTHORIZATION_PROPERTY = "derby.database.sqlAuthorization";

    /**
     * Default connection level authorization, set to
     * one of NO_ACCESS, READ_ONLY_ACCESS or FULL_ACCESS.
     * Defaults to FULL_ACCESS if not set.
     */
	public static final String
	DEFAULT_CONNECTION_MODE_PROPERTY = "derby.database.defaultConnectionMode";

	public static final String NO_ACCESS = "NOACCESS";
	public static final String READ_ONLY_ACCESS = "READONLYACCESS";
	public static final String FULL_ACCESS = "FULLACCESS";

    /**
     * List of users with read-only connection level authorization.
     */
	public static final String
	READ_ONLY_ACCESS_USERS_PROPERTY = "derby.database.readOnlyAccessUsers";

    /**
     * List of users with full access connection level authorization.
     */
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

	// This is the user property used by Derby and LDAP schemes
    //
    // Should not be documented any longer.
	public static final String USER_PROPERTY_PREFIX = "derby.user.";

	// These are the different built-in providers Derby supports

	public static final String AUTHENTICATION_PROVIDER_NATIVE =
								"NATIVE:";

	public static final String AUTHENTICATION_PROVIDER_BUILTIN =
								"BUILTIN";

	public static final String AUTHENTICATION_PROVIDER_LDAP =
								"LDAP";

	public static final String AUTHENTICATION_SERVER_PARAMETER =
								"derby.authentication.server";

    // this suffix on the NATIVE authentication provider means that
    // database operations should be authenticated locally
	public static final String AUTHENTICATION_PROVIDER_LOCAL_SUFFIX =
								":LOCAL";

    // when local native authentication is enabled, we store this value for derby.authentication.provider
    public static final String AUTHENTICATION_PROVIDER_NATIVE_LOCAL =
        AUTHENTICATION_PROVIDER_NATIVE + AUTHENTICATION_PROVIDER_LOCAL_SUFFIX;
    
    // lifetime (in milliseconds) of a NATIVE password. if <= 0, then the password never expires
    public static final String AUTHENTICATION_NATIVE_PASSWORD_LIFETIME =
        "derby.authentication.native.passwordLifetimeMillis";
    
    // default lifetime (in milliseconds) of a NATIVE password. 31 days.
    public static final long MILLISECONDS_IN_DAY = 1000L * 60L * 60L * 24L;
    public static final long AUTHENTICATION_NATIVE_PASSWORD_LIFETIME_DEFAULT = MILLISECONDS_IN_DAY * 31L;
    
    // threshhold for raising a warning that a password is about to expire.
    // raise a warning if the remaining password lifetime is less than this proportion of the max lifetime.
    public static final String  AUTHENTICATION_PASSWORD_EXPIRATION_THRESHOLD =
        "derby.authentication.native.passwordLifetimeThreshold";
    public static final double  AUTHENTICATION_PASSWORD_EXPIRATION_THRESHOLD_DEFAULT = 0.125;
    

    /**
     * <p>
     * Property that specifies the name of the hash algorithm to use with
     * the configurable hash authentication scheme.
     * </p>
     */
    public static final String AUTHENTICATION_BUILTIN_ALGORITHM =
            "derby.authentication.builtin.algorithm";

    /**
     * Default value for derby.authentication.builtin.algorithm when creating
     * a new database.
     */
    public static final String AUTHENTICATION_BUILTIN_ALGORITHM_DEFAULT =
            "SHA-256";

    /**
     * Alternative default value for derby.authentication.builtin.algorithm if
     * {@link #AUTHENTICATION_BUILTIN_ALGORITHM_DEFAULT} is not available at
     * database creation time.
     */
    public static final String AUTHENTICATION_BUILTIN_ALGORITHM_FALLBACK =
            "SHA-1";

    /**
     * <p>
     * Property that specifies the number of bytes with random salt to use
     * when hashing credentials using the configurable hash authentication
     * scheme.
     * </p>
     */
    public static final String AUTHENTICATION_BUILTIN_SALT_LENGTH =
            "derby.authentication.builtin.saltLength";

    /**
     * The default value for derby.authentication.builtin.saltLength.
     */
    public static final int AUTHENTICATION_BUILTIN_SALT_LENGTH_DEFAULT = 16;

    /**
     * <p>
     * Property that specifies the number of times to apply the hash
     * function in the configurable hash authentication scheme.
     * </p>
     */
    public static final String AUTHENTICATION_BUILTIN_ITERATIONS =
            "derby.authentication.builtin.iterations";

    /**
     * Default value for derby.authentication.builtin.iterations.
     */
    public static final int AUTHENTICATION_BUILTIN_ITERATIONS_DEFAULT = 1000;

	/*
	** Log
	*/

	/**
		Property name for specifying log switch interval

        Undocumented.
	 */
	public static final String LOG_SWITCH_INTERVAL = "derby.storage.logSwitchInterval";

	/**
		Property name for specifying checkpoint interval

        Undocumented.
	 */
	public static final String CHECKPOINT_INTERVAL = "derby.storage.checkpointInterval";

	/**
		Property name for specifying log Buffer Size

        Undocumented.
	 */
	public static final String LOG_BUFFER_SIZE = "derby.storage.logBufferSize";
	
	
	/*
	** Replication
	*/

	/** Property name for specifying the size of the replication log buffers */
	public static final String REPLICATION_LOG_BUFFER_SIZE= "derby.replication.logBufferSize";

	/** Property name for specifying the minimum log shipping interval*/
	public static final String REPLICATION_MIN_SHIPPING_INTERVAL = "derby.replication.minLogShippingInterval";

	/** Property name for specifying the maximum log shipping interval*/
	public static final String REPLICATION_MAX_SHIPPING_INTERVAL = "derby.replication.maxLogShippingInterval";

	/** Property name for specifying whether or not replication messages are
	 * written to the log*/
	public static final String REPLICATION_VERBOSE = "derby.replication.verbose";

	/*
	** Upgrade
	*/
	
	/**
     * <p>
	 * Allow database upgrade during alpha/beta time. Only intended
	 * to be used to allow Derby developers to test their upgrade code.
	 * Only supported as a system/application (derby.properties) property.
     * </p>
     *
     * <p>
     * Undocumented.
     * </p>
	 */
	String ALPHA_BETA_ALLOW_UPGRADE = "derby.database.allowPreReleaseUpgrade";
	    
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
     * <p>
     * derby.drda.startNetworkServer
     * </p>
     *
     *<p>
     * If true then we will attempt to start a DRDA network server when Derby 
     * boots, turning the current JVM into a server.
     *</p>
     *
     * <p>
     * Default: false
     * </p>
     */
    public static final String START_DRDA = "derby.drda.startNetworkServer";

    /**
     * <p>
     * derby.drda.logConnections
     *</p>
     *
     * <p>
     * Indicates whether to log connections and disconnections.
     *</p>
     *
     * <p>
     * Default: false
     * </p>
     */
	public final static String DRDA_PROP_LOGCONNECTIONS = "derby.drda.logConnections";

    /**
     * <p>
     * derby.drda.traceAll
     *</p>
     *
     * <p>
     * Turns tracing on for all sessions.
     *</p>
     * Default: false
     *
     * <p>
     * Default: false
     * </p>
     */
	public final static String DRDA_PROP_TRACEALL = "derby.drda.traceAll";

    /**
     * <p>
     * derby.drda.trace
     *</p>
     */
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
     * derby.drda.sslMode
     * <BR>
     * This property may be set to one of the following three values
     * off: No Wire encryption
     * basic:  Encryption, but no SSL client authentication
     * peerAuthentication: Encryption and with SSL client
     * authentication 
     */

    public final static String DRDA_PROP_SSL_MODE = "derby.drda.sslMode";

    /**
     * derby.drda.securityMechanism
     *<BR>
     * This property can be set to one of the following values
     * USER_ONLY_SECURITY
     * CLEAR_TEXT_PASSWORD_SECURITY
     * ENCRYPTED_USER_AND_PASSWORD_SECURITY
     * STRONG_PASSWORD_SUBSTITUTE_SECURITY
     * <BR>
     * if derby.drda.securityMechanism is set to a valid mechanism, then
     * the Network Server accepts only connections which use that
     * security mechanism. No other types of connections are accepted.
     * <BR>
     * if the derby.drda.securityMechanism is not set at all, then the
     * Network Server accepts any connection which uses a valid
     * security mechanism.
     * <BR> 
     * E.g derby.drda.securityMechanism=USER_ONLY_SECURITY
     * This property is static. Server must be restarted for the property to take effect.
     * Default value for this property is as though it is not set - in which case
     * the server will allow clients with supported security mechanisms to connect
     */
    public final static String DRDA_PROP_SECURITYMECHANISM = "derby.drda.securityMechanism";

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
	

    /**
     * derby.drda.streamOutBufferSize
     * size of buffer used when stream out for client.
     *
     */
    public final static String DRDA_PROP_STREAMOUTBUFFERSIZE = "derby.drda.streamOutBufferSize";

	/*
	** Internal properties, mainly used by Monitor.
	*/
	public static final String SERVICE_PROTOCOL = "derby.serviceProtocol";
	public static final String SERVICE_LOCALE = "derby.serviceLocale";

    // Undocumented
	public static final String COLLATION = "derby.database.collation";
	// These are the six possible values for collation type if the collation
	// derivation is not NONE. If collation derivation is NONE, then collation
	// type should be ignored. The TERRITORY_BASED collation uses the default
	// collator strength while the four with a colon uses a specific strength.
	public static final String UCS_BASIC_COLLATION =
								"UCS_BASIC";
	public static final String TERRITORY_BASED_COLLATION =
								"TERRITORY_BASED";
	public static final String TERRITORY_BASED_PRIMARY_COLLATION =
								"TERRITORY_BASED:PRIMARY";
	public static final String TERRITORY_BASED_SECONDARY_COLLATION =
								"TERRITORY_BASED:SECONDARY";
	public static final String TERRITORY_BASED_TERTIARY_COLLATION =
								"TERRITORY_BASED:TERTIARY";
	public static final String TERRITORY_BASED_IDENTICAL_COLLATION =
								"TERRITORY_BASED:IDENTICAL";
	// Define a static string for collation derivation NONE
	public static final String COLLATION_NONE =
		"NONE";

    /**
     * db2j.storage.dataNotSyncedAtCheckPoint
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
        "db2j.storage.dataNotSyncedAtCheckPoint";

    /**
     * db2j.storage.dataNotSyncedAtAllocation
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
        "db2j.storage.dataNotSyncedAtAllocation";

    /**
     * db2j.storage.logNotSynced
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
        "db2j.storage.logNotSynced";


    /**
     * <p>
     * derby.storage.useDefaultFilePermissions = {false,true}
     * </p>
     * <p>
     * When set to true, the store system will not limit file permissions of
     * files created by Derby to owner, but rely on the current OS default.  On
     * Unix, this is determined by {@code umask(1)}. Only relevant for JVM &gt;=
     * 6.
     * </p>
     * <p>
     * The default value is {@code true} on embedded, but {@code false} on the
     * Network server if started from command line, otherwise it is true for
     * the server, too (i.e. started via API).
     * </p>
     */
    String STORAGE_USE_DEFAULT_FILE_PERMISSIONS =
        "derby.storage.useDefaultFilePermissions";

    /**
     * Internal. True if the network server was started from the command line
     * (not from API).  Used to determine whether to narrow file permissions
     * iff {@code derby.storage.useDefaultFilePermissions} isn't specified.
     * <B>INTERNAL USE ONLY</B>
     */
    String SERVER_STARTED_FROM_CMD_LINE =
            "derby.__serverStartedFromCmdLine";
}
