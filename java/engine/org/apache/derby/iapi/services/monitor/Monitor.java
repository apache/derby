/*

   Derby - Class org.apache.derby.iapi.services.monitor.Monitor

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

package org.apache.derby.iapi.services.monitor;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.services.info.ProductGenusNames;
import org.apache.derby.iapi.services.info.ProductVersionHolder;
import org.apache.derby.iapi.reference.EngineType;
import org.apache.derby.iapi.reference.Attribute;

import org.apache.derby.iapi.services.loader.InstanceGetter;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.Property;

import java.util.Properties;
import java.util.Locale;
import java.io.PrintStream;

/**
	<P><B>Services</B><BR>

	A service is a collection of modules that combine to provide
	the full functionality defined by the service. A service is defined
	by three pieces of information:
	<OL>
	<LI>A fully qualified java class name that identifies the functionality or API
	that the service must provide. Typically this class represents a java interface.
	This class name is termed the <EM>factory interface</EM>.
	<LI>The <EM>identifier</EM> of the service. Services are identified by a String, this may
	be hard-coded, come from a UUID or any other source.
	<LI>An optional java.util.Properties set.
	</OL>
	<BR>
	The running functionality of the service is provided by a module
	that implements the factory interface. The identifier of the this module
	is not (need not be) the same as the identifier of the service. The identifier
	of the service is held by the monitor in its service tables.
	<BR>
	Each module in a service is keyed by at least one factory interface, identifier}
	pair. This pair is guaranteed to be unique within the service.
	<BR>
	The lifetime of a module in a service is no longer than the lifetime of the service.
	Thus shutting down a service shuts down all the modules within a service.
	<B>Optionally - </B> an individual module within a service may be shutdown, this will
	in turn shutdown any modules it started if those module are not in use by other
	modules within the service. This would be handled by the monitor, not the module itself.
	<BR>
	A service may be persistent, it goes through a boot in create mode, and subsequently boot
	in non-create mode, or a non-peristent service, it always boots in non-create mode.
	Persistent services can store their re-start parameters in their properties set, the monitor
	provides the persistent storage of the properties set.
	Non-persistent services do not have a properties set.

	<P><B>Booting Services</B><BR>
	Services can be booted a number of ways
	<UL>
	<LI>A non-persistent service can be booted by having a property in the application properties
	or the system (JVM) set.
	<PRE>
	derby.service.<EM>service name</EM>=<EM>class name</EM>
	e.g.
	# Added to the properties automatically by the class org.apache.derby.jdbc.EmbeddedDriver
	derby.service.jdbc=java.sql.Driver
	</PRE>
	<LI>A persistent service can be booted by having a property in the application properties
	or the system (JVM) set.
	<PRE>
	derby.service.<EM>service name</EM>=<EM>persistent storage type</EM>
	e.g.
	derby.service.mydatabase=serviceDirectory
	</PRE>
	serviceDirectory is a type understood by the monitor which means that there is a directory
	named mydatabase within the system directory and within it is a properties file service.properties. This properties
	set is the set for the service and must contain a property
	<PRE>
	derby.protocol=<EM>class name</EM>
	</PRE>
	This is then the factory interface for the service. Other storage types could be added in
	the future.
	<LI>
	The monitor at start time looks for all persistent services that it can find and starts them.
	E.g. all directories in the system directory that have a file service.properties are started
	as services.
	<LI>Services are started on demand, e.g. a findService attempts to boot a service if it
	cannot be found.
	</UL>
	<B>Any or all of these three latter methods can be implemented. A first release may
	just implement the look for all services and boot them.</B>
	.
	<P><B>System Service</B><BR>
	A special service exists, the System Service. This service has no factory interface,
	no identifier and no Properties set. It allows modules to be started that are required
	by another service (or the monitor itself) but are not fundamentally part of the service.
	Modules within this service are unidentified.
	Typically these modules are system wide types of functionality like streams, uuid creation etc.
	<BR>
	The lifetime of a system module is the lifetime of the monitor.
	<B>Optionally - </B> this could be changed to reference count on individual modules, requires
	some minor api changes.

	<P><B>Modules</B><BR>

	A module is found or booted using four pieces of information:
	<OL>
	<LI>The service the module lives in or will live in.
	<LI>A fully qualified java class name that identifies the functionality or API
	that the module must provide. Typically this class represents a java interface.
	This class name is termed the <EM>factory interface</EM>.
	<LI>The <EM>identifier</EM> of the module. Modules are identified by a String, this may
	be null, be hard-coded, come from a UUID or any other source. If the identifier
	is null then the module is described as <EM>unidentified</EM>.
	<LI>Boot time only - A java.util.Properties set. This Properties set is service wide
	and typically contains parameters used to determine module implementation or runtime
	behaviour.
	</OL>
	<BR>
	The service is identified by explicitly identifiying the System Service or
	by providing a reference to a module that already exists with the required service.
	<BR>
	The factory interface is provided by a String constant of the form class.MODULE
	from the required interface.
	<BR>
	The module identifier is provided in a fashion determined by the code, in most
	cases a unidentified module will suffice.
	<BR>
	The Properties set is also determined in a fashion determined by the code at
	create or add service time.

  <P><B>Module Implementations</B><BR>

	When creating an instance of a module, an implementation is found through lists of
	potential implementations.
	<BR>
	A list of potential implementations is obtained from a Properties set. Any property
	within this set that is of the form
	<PRE>
	derby.module.<EM>tag</EM>=<EM>java class name</EM>
	</PRE>
	is seen by the monitor as a possible implementation. <EM>tag</EM> has no meaning within
	the monitor, it is only there to provide uniqueness within the properties file. Typically
	the tag is to provide some description for human readers of the properties file, e.g.
	derby.module.lockManager for an implementation of a lock manager.
	<BR>
	The monitor looks through four properties sets for lists of potential implementations in this
	order. 
	<OL>
	<LI>The properties set of the service (i.e. that passed into Monitor.createPersistentService()
	or Monitor.startService()).
	<LI>The System (JVM) properties set (i.e. java.lang.System.getProperties()).
	<LI>The application properties set (i.e. obtained from the cloudscape.properties file).
	<LI>The default implementation properties set (i.e. obtained from the
	/org/apache/derby/modules.properties resource).
	</OL>
	Any one of the properties can be missing or not have any implementations listed within it.
	<BR>
	Every request to create an instance of a module searches the four implementation
	lists in the order above. Which list the current running code or the passed in service
	module came from is not relevant.
	<BR>
	Within each list of potential implementations the search is conducted as follows:
	<OL>
	<LI>Attempt to load the class, if the class cannot be loaded skip to the next potential
	implementation.
	<LI>See if the factory interface is assignable from the class (isAssignableFrom() method
	of java.lang.Class), if not skip to the next potential implementation.
	<LI>See if an instance of the class can be created without any exceptions (newInstance() method
	of java.lang.Class), if not skip to the next potential implementation.
	<LI>[boot time only] See if the canSupport() method of ModuleControl returns true when called with the
	Properties set of the service, if not skip to the next potential implementation.
	</OL>
	If all these checks pass then the instance is a valid implementation and its boot() method
	of ModuleControl is called to activate it. Note that the search order within
	the list obtained from a Properties set is not guaranteed.

	 <P><B>Module Searching</B><BR>

	When searching for a module the search space is always restricted to a single service.
	This service is usually the system service or the service of the module making the
	search request. It would be very rare (wrong?) to search for a module in a service that
	was not the current service and not the system service.
	<BR>
	Within the list of modules in the service the search is conducted as follows:
	<OL>
	<LI>See if the instance of the module an instance of the factory interface (isInstance() method
	of java.lang.Class), if not skip to the next module.
	<LI>See if the identifier of the module matches the required identifier, if not skip to the next module.
	<LI>See if the canSupport() method of ModuleControl returns true when called with the
	Properties set of the service, if not skip to the next module.
	</OL>
	Note that no search order of the modules is guaranteed.
	<BR>
	Also note that a module may be found by a different factory interface to the one
	it was created under. Thus a class may implement multiple factory interfaces, its boot
	method has no knowledge of which factory interface it was requested by.

  <P><B>Service Properties</B><BR>

	Within the service's Properties a module may search for its parameters. It identifies
	its parameters using a unqiue parameter name and its identifier.
	<BR>
	Unique parameter names are made unique through the 'dot' convention of Properties
	files. A module protocol picks some unique key portion to start, e.g. RawStore for the RawStoreFactory
	and then extends that for specific parameters, e.g. RawStore.PageSize. Thus
	parameters that are typically understood by all implementations of that protocol would
	start with that key portion. Parameters for specific implementations add another key portion
	onto the protocol key portion, e.g. RawStore.FileSystem for an file system implementation
	of the raw store, with a specific parameter being RawStore.FileSystem.SectorSize.

	<BR>These are general guidelines, UUID's could be used as the properties keys but
	would make the parameters hard to read.
	<BR>
	When a module is unidentified it should look for a parameter using just
	the property key for that parameter, e.g. getProperty("RawStore.PageSize").
	<BR>
	When a module has an identifier is should look for a property using the
	key with a dot and the identifier appended, e.g. getProperty("RawStore.PageSize" + "." + identifier).
	<BR>
	In addition to searching for parameters in the service properties set, the system and
	application set may be searched using the getProperty() method of ModuleFactory.
	<BR><B>Should any order be defined for this, should it be automatic?</B>
*/
public class Monitor {

	public static final String SERVICE_TYPE_DIRECTORY = "serviceDirectory";

	public static final Object syncMe = new Object();

	/**
	  Global debug flag to turn on tracing of reads calls newInstanceFromIdentifier()
	  */
	public final static String
		NEW_INSTANCE_FROM_ID_TRACE_DEBUG_FLAG = SanityManager.DEBUG ? "MonitorNewInstanceFromId" : null;
	
	public static final String DEBUG_TRUE = SanityManager.DEBUG ? "derby.debug.true" : null;
	public static final String DEBUG_FALSE = SanityManager.DEBUG ? "derby.debug.false" : null;


	private static ModuleFactory monitor;
	private static boolean active;

	public Monitor() {
	}

	/**
		Start a Monitor based software system.

		This method will execute the following steps.

  <OL>
  <LI> Create an instance of a module (monitor) of the required implementation.
  <LI> Start the monitor which will in turn start any requested services
  <LI> Execute the run() method of startCode (if startCode was not null).
  <LI> Return.
  </OL>
  <P> If MonitorBoot.start() is called more then once then subsequent calls
  have no effect.

		@param properties The application properties
		@param logging Where to place initial error output. This location will be used
			until an InfoStreams module is successfully started.
	*/

	public static void startMonitor(Properties bootProperties, PrintStream logging) {

		new org.apache.derby.impl.services.monitor.FileMonitor(bootProperties, logging);			
	}
	/**
		Initialise this class, must only be called by an implementation
		of the monitor (ModuleFactory).
	*/
	public static boolean setMonitor(ModuleFactory theMonitor) {

		synchronized (syncMe) {
			if (active)
				return false;

			monitor = theMonitor;
			active = true;
			return true;
		}
	}

	public static void clearMonitor() {
		// the monitor reference needs to remain valid
		// as there are some accesses to getMonitor()
		// after the system has been shutdown.
		synchronized (syncMe) {
			active = false;
		}
	}

	/**
		Get the monitor.
	*/
	public static ModuleFactory getMonitor() {
		return monitor;
	}
	public static ModuleFactory getMonitorLite() {
		synchronized (syncMe) {
			if (active && monitor != null)
				return monitor;
		}

		// initialize a monitor just to get system properties
		// with the right secuirty checks and the correct sematics
		// for lookup of derby.system.home.
		// This instance will be discarded once it is used.				;

		return new org.apache.derby.impl.services.monitor.FileMonitor();
	}

	public static HeaderPrintWriter getStream() {
		return monitor.getSystemStreams().stream();
	}

	/**
		Return the name of the service that the passed in module lives in.
	*/
	public static String getServiceName(Object serviceModule) {
		return monitor.getServiceName(serviceModule);
	}


	/**
		Start or find a module in the system service. This call allows modules
		to explictly start services they require.
		If no module matching the criteria is found (see this class's prologue for details)
		then an instance will be created (see prologue) and booted as follows.
		<PRE>
		((ModuleControl) instance).boot(false, (String) null, (Properties) null);
		</PRE>

		@return a reference to a module.

		@exception StandardException An attempt to start the module failed.

		@see ModuleControl#boot
	*/
	public static Object startSystemModule(String factoryInterface)
		throws StandardException {

		Object module = monitor.startModule(false, (Object) null, factoryInterface, (String) null, (Properties) null);
		
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(module != null, "module is null - " + factoryInterface);
		}

		return module;
	}

	/**
		Find a module in the system service.

		@return a reference to a module or null if one cannot be found.
	*/
	public static Object findSystemModule(String factoryInterface) throws StandardException
	{
		Object module = getMonitor().findModule((Object) null,
									  factoryInterface, (String) null);
		if (module == null)
			throw Monitor.missingImplementation(factoryInterface);

		return module;
	}

	public static Object getSystemModule(String factoryInterface)
	{
		Object module = getMonitor().findModule((Object) null,
									  factoryInterface, (String) null);
		return module;
	}

	/**
		Boot or find a unidentified module within a service. This call allows modules
		to start or create any modules they explicitly require to exist within
		their service. If no module matching the criteria is found (see this class's prologue for details)
		then an instance will be created (see prologue) and booted as follows.
		<PRE>
		((ModuleControl) instance).boot(create, (String) null, properties);
		</PRE>
		<BR>
		The service is defined by the service that the module serviceModule lives in,
		typically this call is made from the boot method of a module and thus
		'this' is passed in for serviceModule.

		@return a reference to a module.

		@exception StandardException An attempt to start the module failed.

	*/
	public static Object bootServiceModule(boolean create, Object serviceModule,
		String factoryInterface, Properties properties)
		throws StandardException {

		Object module = monitor.startModule(create, serviceModule, factoryInterface,
						(String) null, properties);

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(module != null, "module is null - " + factoryInterface);
		}

		return module;
	}

	/**
		Boot or find a identified module within a service. This call allows modules
		to start or create any modules they explicitly require to exist within
		their service. If no module matching the criteria is found (see this class's prologue for details)
		then an instance will be created (see prologue) and booted as follows.
		<PRE>
		((ModuleControl) instance).boot(create, identifer, properties);
		</PRE>
		<BR>
		The service is defined by the service that the module serviceModule lives in,
		typically this call is made from the boot method of a module and thus
		'this' is passed in for serviceModule.

		@return a reference to a module.

		@exception StandardException An attempt to start the module failed.

	*/
	public static Object bootServiceModule(boolean create, Object serviceModule,
		String factoryInterface, String identifier, Properties properties)
		throws StandardException {

		Object module = monitor.startModule(create, serviceModule, factoryInterface, identifier, properties);
		
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(module != null, "module is null - " + factoryInterface);
		}

		return module;
	}

	/**
		Find an unidentified module within a service.
		<BR>
		The service is defined by the service that the module serviceModule lives in.

		@return a reference to a module or null if one cannot be found.

	*/
	public static Object findServiceModule(Object serviceModule, String factoryInterface)
		throws StandardException {
		Object module = getMonitor().findModule(serviceModule, factoryInterface, (String) null);
		if (module == null)
			throw Monitor.missingImplementation(factoryInterface);
		return module;
	}
	public static Object getServiceModule(Object serviceModule, String factoryInterface)
	{
		Object module = getMonitor().findModule(serviceModule, factoryInterface, (String) null);
		return module;
	}

	/**
		Find an identified module within a service.
		<BR>
		The service is defined by the service that the module serviceModule lives in.

		@return a reference to a module or null if one cannot be found.

	*/
	//public static Object findServiceModule(Object serviceModule, String factoryInterface, String identifier) {
	//	return monitor.findModule(serviceModule, factoryInterface, identifier);
	//}


	/**
		Find a service.

		@return a refrence to a module represeting the service or null if the service does not exist.

	*/
	public static Object findService(String factoryInterface, String serviceName) {
		return monitor.findService(factoryInterface, serviceName);
	}

	/**
		Start a persistent service. The name of the service can include a
		service type, in the form 'type:serviceName'.
		<BR>
		Note that the return type only indicates
		if the service can be handled by the monitor. It does not indicate
		the service was started successfully. The cases are
		<OL>
		<LI> Service type not handled - false returned.
		<LI> Service type handled, service does not exist, true returned.
		<LI> Service type handled, service exists and booted OK, true returned.
		<LI> Service type handled, service exists and failed to boot, exception thrown.
		</OL>

		If true is returned then findService should be used to see if the service
		exists or not.

		@return true if the service type is handled by the monitor, false if it isn't

		@exception StandardException An attempt to start the service failed.
	*/

	public static boolean startPersistentService(String serviceName, 
												 Properties properties) 
		throws StandardException {

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(serviceName != null, "serviceName is null");
		}

		return monitor.startPersistentService(serviceName, properties);
	}

	/**
		Start a non-persistent service. 
		<P><B>Context</B><BR>
		A context manager will be created and installed at the start of this method and destroyed
		just before this method returns.

		@return The module from the service if it was started successfully. 

		@exception StandardException An exception was thrown trying to start the service.
	*/
	public static Object startNonPersistentService(String factoryInterface, String serviceName, Properties properties)
		throws StandardException {

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(factoryInterface != null, "serviceName is null");
			SanityManager.ASSERT(serviceName != null, "serviceName is null");
		}

		return monitor.startNonPersistentService(factoryInterface, serviceName, properties);
	}

	/**
		Create a named service that implements the java interface (or class) fully qualified by factoryInterface.
		The Properties object specifies create time parameters to be used by the modules within the
		service. Any module created by this service may add or remove parameters within the
		properties object in their ModuleControl.boot() method. The properties set will be saved
		by the Monitor for later use when the monitor is started.
		<P><B>Context</B><BR>
		A context manager will be created and installed at the start of this method and destroyed
		just before this method returns.

		@return The module from the service if it was created successfully, null if a service already existed. 

		@exception StandardException An exception was thrown trying to create the service.
	*/
	public static Object createPersistentService(String factoryInterface, String serviceName, Properties properties) 
		throws StandardException {

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(factoryInterface != null, "serviceName is null");
			SanityManager.ASSERT(serviceName != null, "serviceName is null");
		}
		
		return monitor.createPersistentService(factoryInterface, serviceName, properties);
	}
    public static void removePersistentService(String name)
        throws StandardException
    {
		monitor.removePersistentService(name);
    }

	/**
		Obtain the class object for a class that supports the given identifier.

		If no class has been registered for the identifier then a StandardException
		is thrown with no attached java.lang exception (nextException).
		If a problem loading or accessing the class is obtained then a StandardException
		is thrown with the real java.lang exception attached.

		@return a valid class object

		@exception StandardException See text above.
	*/
	public static InstanceGetter classFromIdentifier(int identifier) 
		throws StandardException {
		return monitor.classFromIdentifier(identifier);
	}

	/**
		Obtain an new instance of a class that supports the given identifier.

		If no class has been registered for the identifier then a StandardException
		is thrown with no attached java.lang exception (getNestedException).
		If a problem loading or accessing the class or creating the object is obtained
		then a StandardException is thrown with the real java.lang exception attached.

		@return a reference to a newly created object

		@exception StandardException See text above.
	*/
	public static Object newInstanceFromIdentifier(int identifier) 
		throws StandardException {
		return monitor.newInstanceFromIdentifier(identifier);
	}


	/* 
	** Static methods for startup type exceptions.
	*/
		/**
		return a StandardException to indicate that a module failed to
		start because it could not obtain the version of a required product.

		@param productGenusName The genus name of the product.
		@return The exception.
	*/
	public static StandardException missingProductVersion(String productGenusName)
	{
		return StandardException.newException(SQLState.MISSING_PRODUCT_VERSION, productGenusName);
	}

	/**
		return a StandardException to indicate a missing
		implementation.

		@param implementation the module name of the missing implementation.

		@return The exception.
	*/
	public static StandardException missingImplementation(String implementation) 
	{
		return StandardException.newException(SQLState.SERVICE_MISSING_IMPLEMENTATION, implementation);
	}

	/**
		return a StandardException to indicate that an exception caused
		starting the module to fail.

		@param t the exception which caused starting the module to fail.

		@return The exception.
	*/
	public static StandardException exceptionStartingModule(Throwable t)
	{
		return StandardException.newException(SQLState.SERVICE_STARTUP_EXCEPTION, t);
	}

	public static void logMessage(String messageText) {
		getStream().println(messageText);
	}

	public static void logTextMessage(String messageID) {
		getStream().println(MessageService.getTextMessage(messageID));
	}
	public static void logTextMessage(String messageID, Object a1) {
		getStream().println(MessageService.getTextMessage(messageID, a1));
	}
	public static void logTextMessage(String messageID, Object a1, Object a2) {
		getStream().println(MessageService.getTextMessage(messageID, a1, a2));
	}
	public static void logTextMessage(String messageID, Object a1, Object a2, Object a3) {
		getStream().println(MessageService.getTextMessage(messageID, a1, a2, a3));
	}
	public static void logTextMessage(String messageID, Object a1, Object a2, Object a3, Object a4) {
		getStream().println(MessageService.getTextMessage(messageID, a1, a2, a3, a4));
	}

	/**
	 *  Translate a localeDescription of the form ll[_CC[_variant]] to
	 *  a Locale object.
	 */
	public static Locale getLocaleFromString(String localeDescription)
								throws StandardException {
		return monitor.getLocaleFromString(localeDescription);
	}


	/**
		Single point for checking if an upgrade is allowed.
	 */
	public static boolean isFullUpgrade(Properties startParams, String oldVersionInfo) throws StandardException {

		boolean fullUpgrade = Boolean.valueOf(startParams.getProperty(org.apache.derby.iapi.reference.Attribute.UPGRADE_ATTR)).booleanValue();

		if (true || !fullUpgrade) {

			ProductVersionHolder engineVersion = Monitor.getMonitor().getEngineVersion();

			if (engineVersion.isBeta() || engineVersion.isAlpha()) {
				// soft upgrade not supported for beta.
				throw StandardException.newException(SQLState.NO_UPGRADE, oldVersionInfo, engineVersion.getSimpleVersionString());
			}

			// Gandalf release does not support any soft or hard upgrade,
			// remove this exception when upgrade support is added, and
			// add back in the following code which has been commented out
			// as it is currently unreachable.
			throw StandardException.newException(
				SQLState.LANG_CANT_UPGRADE_DATABASE, oldVersionInfo, engineVersion);
		}


		return fullUpgrade;
	}

	/**
	  *
	  *	@param	startParams			startup parameters
	  *	@param	desiredProperty		property we're interested in
	  *
	  *	@return	true		type is as desired.
	  *			false		otherwise
	  *
	  */
	public static boolean isDesiredType(Properties startParams, int desiredProperty )
	{
		boolean	retval = false;
		int		engineType = EngineType.NONE;

		if ( startParams != null )
		{
			engineType = Monitor.getEngineType( startParams );
		}

		return (engineType & desiredProperty) != 0;
	}
	public static boolean isDesiredType(int engineType, int desiredProperty) {
		return (engineType & desiredProperty) != 0;
	}
	
	/**
	  *	@param	startParams		startup parameters
	  *
	  *	@return	type of engine
	  *
	  */

	static	public	int	getEngineType(Properties startParams)
	{
		if ( startParams != null )
		{
			String etp = startParams.getProperty(EngineType.PROPERTY);

			int engineType = etp == null ? EngineType.STANDALONE_DB : Integer.parseInt(etp.trim());

			return engineType;
		}

		return EngineType.STANDALONE_DB;
	}

	/**
	  Return true if the properties set provided contains
	  database creation attributes for a database
	  of the correct type
	  */
	public static boolean isDesiredCreateType(Properties p, int type)
	{
		boolean plainCreate = Boolean.valueOf(p.getProperty(Attribute.CREATE_ATTR)).booleanValue();

		if (plainCreate) {
			return (type & EngineType.NONE) != 0;
		}

		// database must already exist
		return isDesiredType(p, type);
	}
}
