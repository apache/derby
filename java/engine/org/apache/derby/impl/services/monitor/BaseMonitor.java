/*

   Derby - Class org.apache.derby.impl.services.monitor.BaseMonitor

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

package org.apache.derby.impl.services.monitor;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;

import org.apache.derby.iapi.services.monitor.PersistentService;

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.RegisteredFormatIds;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.Context;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.context.ShutdownException;

import org.apache.derby.iapi.services.stream.InfoStreams;
import org.apache.derby.iapi.services.stream.PrintWriterGetHeader;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.iapi.services.property.PropertyUtil;

import org.apache.derby.iapi.services.io.AccessibleByteArrayOutputStream;
import org.apache.derby.iapi.services.loader.ClassInfo;
import org.apache.derby.iapi.services.loader.InstanceGetter;
import org.apache.derby.iapi.services.io.FormatableInstanceGetter;
import org.apache.derby.iapi.error.ExceptionSeverity;

import  org.apache.derby.io.StorageFactory;

import org.apache.derby.iapi.services.context.ErrorStringBuilder;

import org.apache.derby.iapi.services.info.JVMInfo;
import org.apache.derby.iapi.services.i18n.BundleFinder;
import org.apache.derby.iapi.services.i18n.MessageService;

import org.apache.derby.impl.services.monitor.PersistentServiceImpl;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.BufferedInputStream;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.ByteArrayInputStream;
import java.io.PrintStream;

import java.util.Hashtable;
import java.util.HashMap;
import java.util.Properties;
import java.util.Enumeration;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.NoSuchElementException;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.InvocationTargetException;

import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedActionException;

import java.net.URL;

/**
	Implementation of the monitor that uses the class loader
	that the its was loaded in for all class loading.

*/

public abstract class BaseMonitor
	implements ModuleFactory, BundleFinder {

	/* Fields */

	/**
		Hashtable of objects that implement PersistentService keyed by their getType() method.
	*/
	protected Hashtable serviceProviders;

	// Vector of class objects of implementations, found in the System, application
	// and default (modules.properties) properties

	protected Vector[]     implementationSets;

	private Vector	  services;					// Vector of TopServices

	protected Properties bootProperties;		// specifc properties provided by the boot method, override everything else
	protected Properties applicationProperties;

	boolean inShutdown;

	// Here are the list of modules that we always boot
	private InfoStreams systemStreams;
	private ContextService contextService;
	private UUIDFactory uuidFactory;

	protected boolean reportOn;
	private PrintStream logging;

	protected ThreadGroup daemonGroup;

	// anti GC stuff
	AntiGC dontGC;

	// class registry
/* one byte  format identifiers never used
	private InstanceGetter[]	rc1;
*/
	private InstanceGetter[]	rc2;
//	private InstanceGetter[]	rc4;

	/* Constructor  */
	public  BaseMonitor() {
		super();

		services = new Vector(0, 1);
		services.addElement(new TopService(this));	// first element is always the free-floating service
	}

	/* Methods of ModuleFactory includes BootStrap and Runnable */

	public InfoStreams getSystemStreams() {
		return systemStreams;
	}

	public void shutdown() {

		// allow only one caller to shut the monitor down
		synchronized (this) {
			if (inShutdown)
				return;
			inShutdown = true;
		}

		if (SanityManager.DEBUG && reportOn) {
			report("Shutdown request");
		}

		// Shutdown all threads by iterrupting them
		contextService.notifyAllActiveThreads((Context) null);

		for (;;) {

			TopService ts;
			int position;
			synchronized (this) {
				position = services.size()  - 1;
				if (position == 0)
					break;

				ts = (TopService) services.elementAt(position);
			}

			// push a new context manager
			ContextManager cm = contextService.newContextManager();
			try {
				// pop the default shutdown context, we are shutting down
				cm.popContext();

				contextService.setCurrentContextManager(cm);


				shutdown(ts.getService());

			} finally {
				contextService.resetCurrentContextManager(cm);
			}

		}
		((TopService) services.elementAt(0)).shutdown();

		synchronized (dontGC) {

			dontGC.goAway = true;
			dontGC.notifyAll();
		}

		contextService.stop();
		Monitor.clearMonitor();
	}

	/**
		Shut down a service that was started by this Monitor. Will
		cause the stop() method to be called on each loaded module.
	*/
	public void shutdown(Object serviceModule) {
		if (serviceModule == null)
			return;

		TopService ts = findTopService(serviceModule);
		if (ts == null)
			return;

		// shutdown() returns false if the service is already being shutdown
		boolean removeService = true;
		try {
			removeService = ts.shutdown();
		} finally {
			synchronized (this) {
				if (removeService) {
					boolean found = services.removeElement(ts);
					if (SanityManager.DEBUG) {
						SanityManager.ASSERT(found, "service was not found " + serviceModule);
					}
				}
			}
		}
	}

	protected final void runWithState(Properties properties, PrintStream log) {

		bootProperties = properties;
		logging = log;

		// false indicates the full monitor is required, not the lite.
		if (!initialize(false))
			return;

		// if monitor is already set then the system is already
		// booted or in the process of booting or shutting down.
		if (!Monitor.setMonitor(this))
			return;

		Object msgService = MessageService.setFinder(this);

		// start a backgorund thread which keeps a reference to this
		// this monitor, and an instance of the Monitor class to ensure
		// that the monitor instance and the class is not garbage collected
		// See Sun's bug 4057924 in Java Developer Section 97/08/06

		Object[] keepItems = new Object[3];
		keepItems[0] = this;
		keepItems[1] = new Monitor();
		keepItems[2] = msgService;
		dontGC = new AntiGC(keepItems);

		Thread dontGCthread = getDaemonThread(dontGC, "antiGC", true);
		dontGCthread.start();

		if (SanityManager.DEBUG) {
			reportOn = Boolean.valueOf(PropertyUtil.getSystemProperty("derby.monitor.verbose")).booleanValue();
		}

		// Set up the application properties
		applicationProperties = readApplicationProperties();

		// The security manager may not let us get the System properties
		// object itself, although it may let us look at the properties in it.
		Properties systemProperties = null;

		if (SanityManager.DEBUG) {
			// In a production system having this call would
			// mean would we have to document it for security
			// permission reasons. Since we don't require it and
			// its a big security hole to allow external code to
			// overwrite our own implementations we just support
			// it for debugging. This means VM executions such as
			// java -Dderby.module.javaCompiler=com.ibm.db2j.impl.BasicServices.JavaCompiler.JavaLang.JLJava ...
			// would only work with a sane codeline.
			try {
				systemProperties = System.getProperties();
			} catch (SecurityException se) {
			}
		}

		Vector bootImplementations = getImplementations(bootProperties, false);

		Vector systemImplementations = null;
		Vector applicationImplementations = null;

		// TEMP - making this sanity only breaks the unit test code
		// I will fix soon, djd.
		if (true || SanityManager.DEBUG) {
			// Don't allow external code to override our implementations.
			systemImplementations = getImplementations(systemProperties, false);
			applicationImplementations = getImplementations(applicationProperties, false);
		}

		Vector defaultImplementations = getDefaultImplementations();

		int implementationCount = 0;
		if (bootImplementations != null)
			implementationCount++;

		// TEMP - making this sanity only breaks the unit test code
		if (true || SanityManager.DEBUG) {
			// Don't allow external code to override our implementations.
			if (systemImplementations != null)
				implementationCount++;
			if (applicationImplementations != null)
				implementationCount++;
		}

		if (defaultImplementations != null)
			implementationCount++;
		implementationSets = new Vector[implementationCount];

		implementationCount = 0;
		if (bootImplementations != null)
			implementationSets[implementationCount++] = bootImplementations;
		
		if (true || SanityManager.DEBUG) {
			// Don't allow external code to override our implementations.
			if (systemImplementations != null)
				implementationSets[implementationCount++] = systemImplementations;
			if (applicationImplementations != null)
				implementationSets[implementationCount++] = applicationImplementations;
		}

		if (defaultImplementations != null)
			implementationSets[implementationCount++] = defaultImplementations;


		if (SanityManager.DEBUG) {
			// Look for the derby.debug.* properties.
			if (applicationProperties != null) {
				addDebugFlags(applicationProperties.getProperty(Monitor.DEBUG_FALSE), false);
				addDebugFlags(applicationProperties.getProperty(Monitor.DEBUG_TRUE), true);
			}

			addDebugFlags(System.getProperty(Monitor.DEBUG_FALSE), false);
			addDebugFlags(System.getProperty(Monitor.DEBUG_TRUE), true);
		}

		try {
			systemStreams = (InfoStreams) Monitor.startSystemModule("org.apache.derby.iapi.services.stream.InfoStreams");

			if (SanityManager.DEBUG) {
				SanityManager.SET_DEBUG_STREAM(systemStreams.stream().getPrintWriter());
			}

			contextService = new ContextService();

			uuidFactory = (UUIDFactory) Monitor.startSystemModule("org.apache.derby.iapi.services.uuid.UUIDFactory");

		} catch (StandardException se) {

			// if we can't create an error log or a context then there's no point going on
			reportException(se);
			// dump any messages we have been saving ...
			dumpTempWriter(true);

			return;
		}

		// switch cover to the real error stream and
		// dump any messages we have been saving ...
		dumpTempWriter(false);

		if (SanityManager.DEBUG && reportOn) {
			dumpProperties("-- Boot Properties --", bootProperties);
			dumpProperties("-- System Properties --", systemProperties);
			dumpProperties("-- Application Properties --", applicationProperties);
		}

		// bootup all the service providers
		bootServiceProviders();

		// See if automatic booting of persistent services is required
		boolean bootAll = Boolean.valueOf(PropertyUtil.getSystemProperty(Property.BOOT_ALL)).booleanValue();


		startServices(bootProperties, bootAll);
		startServices(systemProperties, bootAll);
		startServices(applicationProperties, bootAll);

		if (bootAll) // only if automatic booting is required
			bootPersistentServices( );
	}

	public Object findService(String factoryInterface, String serviceName) {

		if (serviceName == null)
			return null;

		ProtocolKey key;

		try {
			key = ProtocolKey.create(factoryInterface, serviceName);
		} catch (StandardException se) {
			return null;
		}

		TopService myts = null;
		synchronized (this) {
			for (int i = 1; i < services.size(); i++) {
				TopService ts = (TopService) services.elementAt(i);
				if (ts.isPotentialService(key)) {
					myts = ts;
					break;
				}
			}
		}

		// the isActiveService() call may sleep
		// so don't hold the 'this' synchronization
		if (myts != null) {
			if (myts.isActiveService(key))
				return myts.getService();
		}

		return null;
	}

	public Locale getLocale(Object serviceModule) {

		TopService ts = findTopService(serviceModule);

		if (ts == null)
			return null;

		return ts.serviceLocale;

	}

	public Locale getLocaleFromString(String localeDescription)
											throws StandardException {
		return staticGetLocaleFromString(localeDescription);
	}

	/**
		Return the name of the service that the passed in module lives in.
	*/
	public String getServiceName(Object serviceModule) {

		TopService ts = findTopService(serviceModule);

		if (ts == null)
			return null;

		return ts.getServiceType().getUserServiceName(ts.getKey().getIdentifier());
	}

	/**
		Set the locale for the service *outside* of boot time.

		@exception StandardException Standard Cloudscape error.
	*/
	public Locale setLocale(Object serviceModule, String userDefinedLocale)
		throws StandardException {

		TopService ts = findTopService(serviceModule);

		if (ts == null)
			return null;

		PersistentService provider = ts.getServiceType();
		if (provider == null)
			return null;

		String serviceName = ts.getKey().getIdentifier();

		Properties properties = provider.getServiceProperties(serviceName, (Properties) null);

		properties = new UpdateServiceProperties(provider, serviceName, properties, true);

		return setLocale(properties, userDefinedLocale);

	}

	/**
		Set the locale for the service at boot time. The passed in
		properties must be the one passed to the boot method.

		@exception StandardException Standard Cloudscape error.
	*/
	public Locale setLocale(Properties serviceProperties, String userDefinedLocale)
		throws StandardException {

		Locale locale = staticGetLocaleFromString(userDefinedLocale);

		// this will write the property through to the service.properties file.
		serviceProperties.put(Property.SERVICE_LOCALE, locale.toString());

		return locale;
	}

	/**
		Return the PersistentService object for a service.
		Will return null if the service does not exist.
	*/
	public PersistentService getServiceType(Object serviceModule) {
		TopService ts = findTopService(serviceModule);

		if (ts == null)
			return null;

		return ts.getServiceType();
	}


	/**
		Start a module.

		@exception StandardException se An attempt to start the module failed.

		@see ModuleFactory#startModule
	*/
	public Object startModule(boolean create, Object serviceModule, String factoryInterface,
		String identifier, Properties properties) throws StandardException {


		ProtocolKey key = ProtocolKey.create(factoryInterface, identifier);

		TopService ts = findTopService(serviceModule);

		Object instance = ts.bootModule(create, serviceModule, key, properties);

		if (instance == null)
			throw Monitor.missingImplementation(factoryInterface);

		return instance;
	}

	private synchronized TopService findTopService(Object serviceModule) {

		if (serviceModule == null)
			return (TopService) services.elementAt(0);

		for (int i = 1; i < services.size(); i++) {
			TopService ts = (TopService) services.elementAt(i);
			if (ts.inService(serviceModule))
				return ts;
		}

		return null;
	}

	public Object findModule(Object serviceModule, String factoryInterface, String identifier)
	{

		ProtocolKey key;

		try {
			key = ProtocolKey.create(factoryInterface, identifier);
		} catch (StandardException se) {
			return null;
		}

		TopService ts = findTopService(serviceModule);
		if (ts == null)
			return null;

		return ts.findModule(key, true, null);
	}


	/**
		Obtain a class that supports the given identifier.

		@param identifier	identifer to associate with class
		@param length		number of bytes to use from identifier

		@return a reference InstanceGetter

		@exception StandardException See Monitor.classFromIdentifier

		@see ModuleFactory#classFromIdentifier
	*/
	public InstanceGetter classFromIdentifier(int fmtId)
		throws StandardException {

		String className;
		int off;
		InstanceGetter[] iga;
		InstanceGetter ig;

		try {

			off = fmtId - StoredFormatIds.MIN_TWO_BYTE_FORMAT_ID;
			iga = rc2;
			if (iga == null) {
				iga = rc2 = new InstanceGetter[RegisteredFormatIds.TwoByte.length];
			}

			ig = iga[off];
			if (ig != null) {
				return ig;
			}
			className = RegisteredFormatIds.TwoByte[off];

		} catch (ArrayIndexOutOfBoundsException aioobe) {
			className = null;
			iga = null;
			off = 0;
		}

		if (className != null) {

			Throwable t;
			try {
				Class clazz = Class.forName(className);

				// See if it is a FormatableInstanceGetter
				if (FormatableInstanceGetter.class.isAssignableFrom(clazz)) {
					FormatableInstanceGetter tfig = (FormatableInstanceGetter) clazz.newInstance();
					tfig.setFormatId(fmtId);
					return iga[off] = tfig;
				}

				return iga[off] = new ClassInfo(clazz);

			} catch (ClassNotFoundException cnfe) {
				t = cnfe;
			} catch (IllegalAccessException iae) {
				t = iae;
			} catch (InstantiationException ie) {
				t = ie;
			} catch (LinkageError le) {
				t = le;
			}
			throw StandardException.newException(SQLState.REGISTERED_CLASS_LINAKGE_ERROR,
				t, FormatIdUtil.formatIdToString(fmtId), className);
		}

		throw StandardException.newException(SQLState.REGISTERED_CLASS_NONE, FormatIdUtil.formatIdToString(fmtId));
	}


	/**
		Obtain an new instance of a class that supports the given identifier.

		@return a reference to a newly created object or null if a matching class
			    cannot be found.
	*/
	public Object newInstanceFromIdentifier(int identifier)
		throws StandardException {

		InstanceGetter ci = classFromIdentifier(identifier);

		Throwable t;
		try {
			Object result = ci.getNewInstance();
/*
				if (SanityManager.DEBUG) {
					if(SanityManager.DEBUG_ON(Monitor.NEW_INSTANCE_FROM_ID_TRACE_DEBUG_FLAG))
					{
						String traceResult = "null";

						if (result != null) traceResult = "not null";

						SanityManager.DEBUG(Monitor.NEW_INSTANCE_FROM_ID_TRACE_DEBUG_FLAG,
											"newInstanceFromIdentifier("+identifier+") "+
											" ClassName: "+
											result.getClass().getName() +
											" returned "+
											traceResult);
					}
				}
*/
			return result;
		}
		catch (InstantiationException ie) {
			t = ie;
		}
 		catch (IllegalAccessException iae) {
			t = iae;
		}
		catch (InvocationTargetException ite) {
			t = ite;
		}
		catch (LinkageError le) {
			t = le;
		}
		throw StandardException.newException(SQLState.REGISTERED_CLASS_INSTANCE_ERROR,
			t, new Integer(identifier), "XX" /*ci.getClassName()*/);
	}

	private Boolean exceptionTrace;

	/**
		load a module instance.

		Look through the implementations for a module that implements the
		required factory interface and can handle the properties given.

		The module's start or create method is not called.
	*/

	protected Object loadInstance(Class factoryInterface, Properties properties) {

		Object instance = null;

		Vector localImplementations = getImplementations(properties, false);
		if (localImplementations != null) {
			instance = loadInstance(localImplementations, factoryInterface, properties);
		}

		for (int i = 0; i < implementationSets.length; i++) {
			instance = loadInstance(implementationSets[i], factoryInterface, properties);
			if (instance != null)
				break;
		}

		return instance;
	}


	private Object loadInstance(Vector implementations, Class factoryInterface, Properties properties) {

		for (int index = 0; true; index++) {

			// find an implementation
			index = findImplementation(implementations, index, factoryInterface);
			if (index < 0)
				return null;

			// try to create an instance
			Object instance = newInstance((Class) implementations.elementAt(index));

			if (BaseMonitor.canSupport(instance, properties))
				return instance;
		}
	}


	/**
		Find a class that implements the required index, return the index
		into the implementations vecotr of that class. Returns -1 if no class
		could be found.
	*/
	private static int findImplementation(Vector implementations, int startIndex, Class factoryInterface) {

		for (int i = startIndex; i < implementations.size(); i++) {

			//try {
				Class factoryClass = (Class) implementations.elementAt(i);
				if (!factoryInterface.isAssignableFrom(factoryClass)) {
					continue;
				}

				return i;
			//}
			//catch (ClassNotFoundException e) {
			//	report("Class not found " + (String) implementations.elementAt(i));
			//	continue;
			//}
		}

		return -1;
	}

	/**
	*/
	private Object newInstance(String className) {

		try {

			Class factoryClass = Class.forName(className);
			return factoryClass.newInstance();
		}
		catch (ClassNotFoundException e) {
			report(className + " " + e.toString());
		}
		catch (InstantiationException e) {
			report(className + " " + e.toString());
		}
 		catch (IllegalAccessException e) {
			report(className + " " + e.toString());
		}
		catch (LinkageError le) {
			report(className + " " + le.toString());
			reportException(le);
		}

		return null;
	}
	/**
	*/
	private Object newInstance(Class classObject) {

		try {
			return classObject.newInstance();
		}
		catch (InstantiationException e) {
			report(classObject.getName() + " " + e.toString());
		}
 		catch (IllegalAccessException e) {
			report(classObject.getName() + " " + e.toString());
		}
		catch (LinkageError le) {
			report(classObject.getName() + " " + le.toString());
			reportException(le);
		}

		return null;
	}

	public Properties getApplicationProperties() {
		return applicationProperties;
	}

	/**
		Return an array of the service identifiers that are running and
		implement the passed in protocol (java interface class name).

		@return The list of service names, if no services exist that
		implement the protocol an array with zero elements is returned.

		@see ModuleFactory#getServiceList
	*/
	public String[] getServiceList(String protocol) {

		TopService ts;

		synchronized (this) {
			int count = 0;

			// count the number of services that implement the required protocol
			for (int i = 1; i < services.size(); i++) {
				ts = (TopService) services.elementAt(i);
				if (ts.isActiveService()) {
					if (ts.getKey().getFactoryInterface().getName().equals(protocol))
						count++;
				}
			}

			// and then fill in the newly allocated string array
			String[] list = new String[count];
			if (count != 0) {
				int j = 0;
				for (int i = 1; i < services.size(); i++) {
					ts = (TopService) services.elementAt(i);
					if (ts.isActiveService()) {
						if (ts.getKey().getFactoryInterface().getName().equals(protocol)) {
							list[j++] = ts.getServiceType().getUserServiceName(ts.getKey().getIdentifier());
							if (j == count)
								break;
						}
					}
				}
			}
			return list;
		}
	}

	/*
	** non-public methods.
	*/


	void dumpProperties(String title, Properties props) {
		if (SanityManager.DEBUG) {
			// this method is only called if reportOn is true, so no need to check it here
			report(title);
			if (props != null) {
				for (Enumeration e = props.propertyNames(); e.hasMoreElements(); ) {
					String key = (String) e.nextElement();
					// Get property as object in case of non-string properties
					report(key + "=" + props.getProperty(key));
				}
			}
			report("-- end --");
		}

	}


	/**
		Should only be called if reportOn is true
		apart from report/Exception().
	*/
	protected void report(String message)	{

		PrintWriter tpw = getTempWriter();

		if (tpw != null)
			tpw.println(message);

		if (systemStreams != null)
			systemStreams.stream().printlnWithHeader(message);
	}

	protected void reportException(Throwable t) {


		PrintWriterGetHeader pwgh = null;
		if (systemStreams != null)
			pwgh = systemStreams.stream().getHeader();

		ErrorStringBuilder esb = new ErrorStringBuilder(pwgh);

		esb.appendln(t.getMessage());
		esb.stackTrace(t);

		report(esb.get().toString());
	}

	private void addDebugFlags(String flags, boolean set) {
		if (SanityManager.DEBUG) {
			if (flags == null)
				return;

			StringTokenizer st = new StringTokenizer(flags, ",");
			for (; st.hasMoreTokens(); ) {
				String flag = st.nextToken();

				if (set)
					SanityManager.DEBUG_SET(flag);
				else
					SanityManager.DEBUG_CLEAR(flag);
			}
		}
	}

	/**
		Look for any services in the a properties set and the application
		property set and then start them.

		A service is defined by derby.service.name=protocol
	*/
	private static final String SERVICE = "derby.service.";

	public void startServices(Properties properties, boolean bootAll) {

		if (properties == null)
			return;

		for (Enumeration e = properties.propertyNames(); e.hasMoreElements(); ) {

			String key = (String) e.nextElement();
			if (key.startsWith(SERVICE)) {
				String name = key.substring(SERVICE.length());

				String protocolOrType = properties.getProperty(key);

				try {
					if (protocolOrType.equals(Monitor.SERVICE_TYPE_DIRECTORY)) {
						if (bootAll)	// only if automatic booting is required
							startPersistentService(name, properties, true);
					} else {
						bootService((PersistentService) null,
							protocolOrType, name, (Properties)null, false);
					}

				} catch (StandardException se) {
					// error already in error log, just continue booting
					// for persistent services, but non-persistent ones
					// will not have put the error in the log
					if (!protocolOrType.equals(Monitor.SERVICE_TYPE_DIRECTORY))
						reportException(se);
				}
			}
		}
	}

	/**
		Start a peristent service.

		@see ModuleFactory#startPersistentService
		@see Monitor#startPersistentService
	*/
	public boolean startPersistentService(String name, Properties properties)
		throws StandardException {

		return startPersistentService(name, properties, false);

	}

	protected boolean startPersistentService(String name, 
				 Properties properties, boolean bootTime)
		throws StandardException {

		return findProviderAndStartService(name, properties, bootTime);
	}

	/**
		Create a persistent service.

		@return The module from the service if it was created successfully, null if a service already existed.

		@exception StandardException An exception was thrown trying to create the service.

		@see Monitor#createPersistentService
	*/

	public Object createPersistentService(String factoryInterface, String name, Properties properties)
		throws StandardException {


		PersistentService provider = findProviderForCreate(properties, name);
		if (provider == null) {
			throw StandardException.newException(SQLState.PROTOCOL_UNKNOWN, name);
		}

		return bootService(provider, factoryInterface, name, properties, true);
	}
    /* Removes a PersistentService
       @param name : Service name to be removed.
       
       Note : Currently needed by dropPublisher. But this can be used to
              remove any PersistentService.
	*/
    public void removePersistentService(String name)
         throws StandardException 
    {
        PersistentService provider=null;
		provider = findProviderForCreate(null, name);
        String serviceName = provider.getCanonicalServiceName(name);
        boolean removed = provider.removeServiceRoot(serviceName);
        if (removed == false)
			throw StandardException.newException(SQLState.SERVICE_DIRECTORY_REMOVE_ERROR,serviceName);
    }
	/**
		Start a non-persistent service.

		@see Monitor#startNonPersistentService
		@see ModuleFactory#startNonPersistentService
	*/
	public Object startNonPersistentService(String factoryInterface, String serviceName, Properties properties)
		throws StandardException {

		return bootService((PersistentService) null, factoryInterface, serviceName, properties, false);
	}


	/**
		Create an implementation set.
		Look through the properties object for all properties that
		start with derby.module and add the value into the vector.

		If no implementations are listed in the properties object
		then null is returned.
	*/
	protected Vector getImplementations(Properties moduleList, boolean actualModuleList) {

		if (moduleList == null)
			return null;

		Vector implementations = actualModuleList ? new Vector(moduleList.size()) : new Vector(0,1);

		// Get my current JDK environment
		int theJDKId = JVMInfo.JDK_ID;

		int[] envModuleCount = new int[theJDKId + 1];

nextModule:
		for (Enumeration e = moduleList.propertyNames(); e.hasMoreElements(); ) {

			String key = (String) e.nextElement();
			if (key.startsWith("derby.module.")) {
				int keylength = "derby.module.".length();
				String tag = key.substring(keylength);

				// Check to see if it has any environment requirements

				// derby.env.jdk.<tag> - Any JDK requirements.
				String envKey = "derby.env.jdk.".concat(tag);
				String envJDK = moduleList.getProperty(envKey);
				int envJDKId = 0;
				
				if (envJDK != null) {
					envJDKId = Integer.parseInt(envJDK.trim());
					if (envJDKId > theJDKId) {
						continue nextModule;
					}
				}

				// derby.env.classes.<tag> - Any class requirements
				envKey = "derby.env.classes.".concat(tag);
				String envClasses = moduleList.getProperty(envKey);
				if (envClasses != null) {

					StringTokenizer st = new StringTokenizer(envClasses, ",");
					for (; st.hasMoreTokens(); ) {
						try {
							Class.forName(st.nextToken().trim());
						} catch (ClassNotFoundException cnfe) {
							continue nextModule;
						} catch (LinkageError le) {
							continue nextModule;
						}
					}
				}



				// we load the class and run its registerFormatC
				// if we can't load the class or create an instance then
				// we don't use this calls as a valid module implementation
				String className = moduleList.getProperty(key);

				if (SanityManager.DEBUG && reportOn) {
					report("Accessing module " + className + " to run initializers at boot time");
				}

				try {
					Class possibleModule = Class.forName(className);

					// Look for the monitors special modules, PersistentService ones.
					if (getPersistentServiceImplementation(possibleModule))
                        continue;

					// If this is a specific JDK version (environment) module
					// then it must be ordered in the implementation list by envJDKId.
					// Those with a higher number are at the front, e.g.
					//
					//	JDK 1.4 modules (envJDKId == 4)
					//  JDK 1.2/1.3 modules (envJDKId == 2)
					//  JDK 1.1 modules (envJDKId == 1)
					//  generic modules (envJDKId == 0 (not set in modules.properties)
					//
					//  Note modules with envJDKId > theJDKId do not get here

					if (envJDKId != 0) {

						// total how many modules with a higher envJDKId are ahead of us
						int offset = 0;
						for (int eji = theJDKId; eji > envJDKId; eji--) {
							offset += envModuleCount[eji];
						}

						implementations.insertElementAt(possibleModule, offset);
						envModuleCount[envJDKId]++;

					}
					else {
						// just add to the end of the vector
						implementations.addElement(possibleModule);
					}

					// Since ModuleControl and ModuleSupportable are not called directly
					// check that if the have the methods then the class implements the
					// interface.
					if (SanityManager.DEBUG) {
						// ModuleSupportable
						Class[] csParams = { new java.util.Properties().getClass()};
						try {
							possibleModule.getMethod("canSupport", csParams);
							if (!ModuleSupportable.class.isAssignableFrom(possibleModule)) {
								SanityManager.THROWASSERT("Module does not implement ModuleSupportable but has canSupport() - " + className);
							}
						} catch (NoSuchMethodException nsme){/* ok*/}

						// ModuleControl
						boolean eitherMethod = false;

						Class[] bootParams = {Boolean.TYPE, new java.util.Properties().getClass()};
						try {
							possibleModule.getMethod("boot", bootParams);
							eitherMethod = true;
						} catch (NoSuchMethodException nsme){/*ok*/}

						Class[] stopParams = {};
						try {
							possibleModule.getMethod("stop", stopParams);
							eitherMethod = true;
						} catch (NoSuchMethodException nsme){/*ok*/}

						if (eitherMethod) {
							if (!ModuleControl.class.isAssignableFrom(possibleModule)) {
								SanityManager.THROWASSERT("Module does not implement ModuleControl but has its methods - " + className);
							}
						}


						
					}

				}
				catch (ClassNotFoundException cnfe) {
					report("Class " + className + " " + cnfe.toString() + ", module ignored.");
				}
				catch (LinkageError le) {
					report("Class " + className + " " + le.toString() + ", module ignored.");
				}
			}
            else if( key.startsWith( Property.SUB_SUB_PROTOCOL_PREFIX)) {
                String subSubProtocol = key.substring( Property.SUB_SUB_PROTOCOL_PREFIX.length());
                String className = moduleList.getProperty(key);

				if (SanityManager.DEBUG && reportOn) {
					report("Accessing module " + className + " to run initializers at boot time");
				}
                try {
                    Class possibleImplementation = Class.forName(className);
					// Look for the monitors special classes, PersistentService and StorageFactory ones.
                    if( getPersistentServiceImplementation( possibleImplementation))
                        continue;
                    if( StorageFactory.class.isAssignableFrom( possibleImplementation)) {
                        if( newInstance( possibleImplementation) == null)
                            report("Class " + className + " cannot create instance, StorageFactory ignored.");
                        else
                            storageFactories.put( subSubProtocol, className);
                        continue;
                    }
                }
				catch (ClassNotFoundException cnfe) {
					report("Class " + className + " " + cnfe.toString() + ", module ignored.");
				}
				catch (LinkageError le) {
					report("Class " + className + " " + le.toString() + ", module ignored.");
				}
            }
        }

		if (implementations.isEmpty())
			return null;
		implementations.trimToSize();

		return implementations;
	}

    private boolean getPersistentServiceImplementation( Class possibleModule)
    {
        if( ! PersistentService.class.isAssignableFrom(possibleModule))
            return false;

        PersistentService ps = (PersistentService) newInstance(possibleModule);
        if (ps == null) {
            report("Class " + possibleModule.getName() + " cannot create instance, module ignored.");
        } else {
            if (serviceProviders == null)
                serviceProviders = new Hashtable(3, (float) 1.0);
            serviceProviders.put(ps.getType(), ps);
        }
        return true;
    } // end of getPersistentServiceImplementation
        
	protected Vector getDefaultImplementations() {

		Properties moduleList = new Properties();
        boolean firstList = true;
        ClassLoader cl = getClass().getClassLoader();

        try {
            for( Enumeration e = cl.getResources( "org/apache/derby/modules.properties");
                 e.hasMoreElements() ;) {
                URL modulesPropertiesURL = (URL) e.nextElement();
                InputStream is = null;
                try {
                    is = loadModuleDefinitions( modulesPropertiesURL);
                    if( firstList) {
                        moduleList.load( is);
                        firstList = false;
                    }
                    else {
                        // Check for duplicates
                        Properties otherList = new Properties();
                        otherList.load( is);
                        for( Enumeration newKeys = otherList.keys(); newKeys.hasMoreElements() ;)
                        {
                            String key = (String) newKeys.nextElement();
                            if( moduleList.contains( key))
                                // RESOLVE how do we localize messages before we have finished initialization?
                                report( "Ignored duplicate property " + key + " in " + modulesPropertiesURL.toString());
                            else
                                moduleList.setProperty( key, otherList.getProperty( key));
                        }
                    }
                } catch (IOException ioe) {
                    if (SanityManager.DEBUG)
                        report("Can't load implementation list " + modulesPropertiesURL.toString() + ": " + ioe.toString());
                } finally {
                    try {
                        if( is != null)
                            is.close();
                    } catch (IOException ioe2) {
                    }
                }
            }
        } catch (IOException ioe) {
            if (SanityManager.DEBUG)
                report("Can't load implementation list: " + ioe.toString());
        }
        if( firstList) {
			if (SanityManager.DEBUG)
				report("Default implementation list not found");
			return null;
		}

		return getImplementations(moduleList, true);
	} // end of getDefaultImplementations

	protected InputStream loadModuleDefinitions( URL propertyFileURL) throws IOException {
		// SECURITY PERMISSION - IP1
		return propertyFileURL.openStream();
	}

	/*
	** Class methods
	*/

	/**
		Return a property set that has the runtime properties removed.
	*/
	protected static Properties removeRuntimeProperties(Properties properties) {

		Properties subset = new Properties();

		for (Enumeration e = properties.keys(); e.hasMoreElements(); ) {

			String key = (String) e.nextElement();
			if (key.startsWith(Property.PROPERTY_RUNTIME_PREFIX))
				continue;

			subset.put(key, properties.get(key));
		}

		return subset;
	}


	/**	
		Get InputStream for application properties file Returns nul if it does not exist.
	*/
	protected abstract InputStream applicationPropertiesStream()
	  throws IOException;


	/**
	*/
	protected Properties readApplicationProperties() {

		InputStream is = null;

		try {
			// SECURITY PERMISSION - OP3
			is = applicationPropertiesStream();
			if (is == null)
				return null;

			Properties properties = new Properties();

			// Trim off excess whitespace from properties file, if any,
			// and then load the properties into 'properties'.
			org.apache.derby.iapi.util.PropertyUtil.loadWithTrimmedValues(
				new BufferedInputStream(is), properties);

			return properties;

		} catch (SecurityException se) {
			return null;
		} catch (IOException ioe) {
			report(ioe.toString() + " (" + Property.PROPERTIES_FILE + ")");
			reportException(ioe);
			return null;

		}finally {


			try {
				if (is != null) {
					is.close();
					is = null;
				}

			} catch (IOException e) {
			}
		}
	}


	/*
	** Methods related to service providers.
	**
	** A service provider implements PersistentService and
	** abstracts out:
	**
	**    Finding all serivces that should be started at boot time.
	**    Finding the service.properties file for a service
	**    Creating a service's root.
	**
	** A monitor can have any number of service providers installed,
	** any module that implements PersistentService is treated specially
	** and stored only in the serviceProviders hashtable, indexed by
	** its getType() method.
	**
	** Once all the implementations have loaded the service providers
	** are booted. If they fail to boot then they aare discarded.
	** E.g. a marimba service provider may detect that its not in
	** a channel so it refuses to boot.
	*/

	/**
		Boot all the service providers, ie. any module that implemented
		PersistentService. Upon entry to this call is the hashtable has
		PersistentService objects that have been created but not booted.
	*/
	protected void bootServiceProviders() {

		if (serviceProviders == null) {
			return;
		}

		for (Enumeration e = serviceProviders.keys(); e.hasMoreElements(); ) {

			String serviceType = (String) e.nextElement();
			Object provider = serviceProviders.get(serviceType);

			// see if this provider can live in this environment
			if (!BaseMonitor.canSupport(provider, (Properties) null)) {
				serviceProviders.remove(serviceType);
				continue;
			}
		}
	}
	/**
		Boot all persistent services that can be located at run time.

		<BR>
		This method enumerates through all the service providers that
		are active and calls bootPersistentServices(PersistentService)
		to boot all the services that that provider knows about.
	*/
	protected void bootPersistentServices() {
		for (Enumeration e = new ProviderEnumeration( applicationProperties); ; ) {

			PersistentService provider = (PersistentService) e.nextElement();
			bootProviderServices(provider);
		}

	}

	/**
		Boot all persistent services that can be located by a single service provider

		<BR>
		This method enumerates through all the service providers that
		are active and calls bootPersistentServices(PersistentService)
		to boot all the services that that provider knows about.
	*/
	protected void bootProviderServices(PersistentService provider) {

		if (SanityManager.DEBUG && reportOn) {
			report("Booting persistent services for provider: " + provider.getType());
		}

		for (Enumeration e = provider.getBootTimeServices(); (e != null) && e.hasMoreElements(); ) {

			String serviceName = (String) e.nextElement();

			Properties serviceProperties;
			try {
				serviceProperties = provider.getServiceProperties(serviceName, null);
			} catch (StandardException mse) {
				report("Failed to load service properties, name: " + serviceName + ", type = " + provider.getType());
				reportException(mse);
				continue;
			}

			// see if this service does not want to be auto-booted.
			if (Boolean.valueOf(serviceProperties.getProperty(Property.NO_AUTO_BOOT)).booleanValue())
				continue;


			try {
				startProviderService(provider, serviceName, serviceProperties);
			} catch (StandardException mse) {
				report("Service failed to boot, name: " + serviceName + ", type = " + provider.getType());
				reportException(mse);
				continue;
			}
		}
	}
	/**
		Find a provider and start  a service.
	*/
	protected boolean findProviderAndStartService(String name, 
						  Properties properties, boolean bootTime)
		throws StandardException {

		PersistentService actualProvider = null;

		Properties serviceProperties = null;
		String serviceName = null;

		// see if the name already includes a service type
		int colon = name.indexOf(':');
		if (colon != -1) {
			actualProvider = findProviderFromName(properties, name, colon);

			// if null is returned here then its a sub-sub protocol/provider
			// that we don't understand. Attempt to load it as an untyped name.
			// If we have a protool
			// that we do understand and we can't open the service we will
			// throw an exception
			if (actualProvider != null) {

				serviceName = actualProvider.getCanonicalServiceName(name);
				if (serviceName == null)
					return true;  // we understand the type, but the service does not exist

				serviceProperties =
					actualProvider.getServiceProperties(serviceName, properties);

				if (serviceProperties == null)
					return true; // we understand the type, but the service does not exist

				// see if this service does not want to be auto-booted.
				if (bootTime && Boolean.valueOf(serviceProperties.getProperty(Property.NO_AUTO_BOOT)).booleanValue())
					return true;

				startProviderService(actualProvider, serviceName, serviceProperties);
				return true; // we understand the type
			}
		}

		StandardException savedMse = null;

		for (Enumeration e = new ProviderEnumeration( properties); e.hasMoreElements(); ) {

			PersistentService provider = (PersistentService) e.nextElement();

			String sn = provider.getCanonicalServiceName(name);
			if (sn == null)
				continue;

			Properties p = null;
			try {
				p = provider.getServiceProperties(sn, properties);
				// service does not exist.
				if (p == null)
					continue;

			} catch (StandardException mse) {
				savedMse = mse;
			}


			// yes we can attempt to boot this service
			if (actualProvider == null) {
				actualProvider = provider;
				serviceName = sn;
				serviceProperties = p;
				continue;
			}

			// we have an ambigious service name
			throw StandardException.newException(SQLState.AMBIGIOUS_PROTOCOL, name);
		}

		// no such service, if this was a name with no type, ie just name instead of type:name.
		// the monitor claims to always understand these.
		if (actualProvider == null)
			return colon == -1;

		if (savedMse != null)
			throw savedMse;

		// see if this service does not want to be auto-booted.
		if (bootTime && Boolean.valueOf(serviceProperties.getProperty(Property.NO_AUTO_BOOT)).booleanValue())
			return true;

		startProviderService(actualProvider, serviceName, serviceProperties);
		return true;
	}

	protected PersistentService findProvider() throws StandardException
	{
		// This is a hack. This is called when we want to re-write 
		// services.properties, and need the provider for the database
		// directory.
		return findProviderForCreate(null, "");
	}

	protected PersistentService findProviderForCreate(Properties startParams, String name) throws StandardException {
		// RESOLVE - hard code creating databases in directories for now.
		return (PersistentService) findProviderFromName( startParams, name, name.indexOf(':'));
	}

	/**
		Find the service provider from a name that includes a service type,
		ie. is of the form 'type:name'. If type is less than 3 chanacters
		then it is assumed to be of type directory, i.e. a windows driver letter.
	*/
	private PersistentService findProviderFromName(Properties startParams, String name, int colon) throws StandardException
    {
		// empty type, treat as a unknown protocol
		if (colon == 0)
			return null;

		String serviceType;
		if (colon < 2) {
			// assume it's a windows path (a:/foo etc.) and set the type to be DIRECTORY
			serviceType = PersistentService.DIRECTORY;
		} else {
			serviceType = name.substring(0, colon);
		}
		return getServiceProvider( startParams, serviceType);
	}

    public PersistentService getServiceProvider( Properties startParams, String subSubProtocol) throws StandardException
    {
        if( subSubProtocol == null)
            return null;
        if( serviceProviders != null)
        {
            PersistentService ps = (PersistentService) serviceProviders.get( subSubProtocol);
            if( ps != null)
                return ps;
        }
        return getPersistentService( startParams, subSubProtocol);
    } // end of getServiceProvider

    private PersistentService getPersistentService( Properties properties, String subSubProtocol)
        throws StandardException
    {
        String className = getStorageFactoryClassName( properties, subSubProtocol);
        return getPersistentService( className, subSubProtocol);
    }

    private PersistentService getPersistentService( final String className, String subSubProtocol) throws StandardException
    {
        if( className == null)
            return null;
        Class storageFactoryClass = null;
        try
        {
            storageFactoryClass = Class.forName( className);
       }
        catch (Throwable e)
        {
            throw StandardException.newException( SQLState.INSTANTIATE_STORAGE_FACTORY_ERROR,
                                                  e,
                                                  subSubProtocol, className);
        }
        return new PersistentServiceImpl( subSubProtocol, storageFactoryClass);
    } // end of getPersistentService

    private String getStorageFactoryClassName( Properties properties, String subSubProtocol)
    {
        String propertyName = Property.SUB_SUB_PROTOCOL_PREFIX + subSubProtocol;
        String className = null;
        if( properties != null)
            className = properties.getProperty( propertyName);
        if( className == null)
            className = PropertyUtil.getSystemProperty( propertyName);
        if( className != null)
            return className;
        return (String) storageFactories.get( subSubProtocol);
    } // end of getStorageFactoryClassName

    private static final HashMap storageFactories = new HashMap();
    static {
		String dirStorageFactoryClass;
		if( JVMInfo.JDK_ID >= 4)
            dirStorageFactoryClass = "org.apache.derby.impl.io.DirStorageFactory4";
        else
            dirStorageFactoryClass = "org.apache.derby.impl.io.DirStorageFactory";


        storageFactories.put( PersistentService.DIRECTORY, dirStorageFactoryClass);
        storageFactories.put( PersistentService.CLASSPATH,
                                "org.apache.derby.impl.io.CPStorageFactory");
        storageFactories.put( PersistentService.JAR,
                                "org.apache.derby.impl.io.JarStorageFactory");
        storageFactories.put( PersistentService.HTTP,
                                "org.apache.derby.impl.io.URLStorageFactory");
        storageFactories.put( PersistentService.HTTPS,
                                "org.apache.derby.impl.io.URLStorageFactory");
    }

	/**
		Boot a service under the control of the provider
	*/
	protected void startProviderService(PersistentService provider, String serviceName, Properties serviceProperties)
		throws StandardException {

		String protocol = serviceProperties.getProperty(Property.SERVICE_PROTOCOL);

		if (protocol == null) {
			throw StandardException.newException(SQLState.PROPERTY_MISSING, Property.SERVICE_PROTOCOL);
		}

		bootService(provider, protocol, serviceName, serviceProperties, false);
	}

	/**
		Boot (start or create) a service (persistent or non-persistent).
	*/
	protected Object bootService(PersistentService provider,
		String factoryInterface, String serviceName, Properties properties,
		boolean create) throws StandardException {

		//reget the canonical service name in case if it was recreated
		//after we got service name.(like in case of restoring from backup).
		if(provider != null)
			serviceName = provider.getCanonicalServiceName(serviceName);
		ProtocolKey serviceKey = ProtocolKey.create(factoryInterface, serviceName);
		if (SanityManager.DEBUG && reportOn) {
			report("Booting service " + serviceKey + " create = " + create);
		}

		ContextManager previousCM = contextService.getCurrentContextManager();
		ContextManager cm = previousCM;
		Object instance;
		TopService ts = null;
		Context sb = null;


		try {


			synchronized (this) {

				if (inShutdown) {
					throw StandardException.newException(SQLState.CLOUDSCAPE_SYSTEM_SHUTDOWN);
				}

				for (int i = 1; i < services.size(); i++) {
					TopService ts2 = (TopService) services.elementAt(i);
					if (ts2.isPotentialService(serviceKey)) {
						// if the service already exists then  just return null
						return null;
					}
				}


				Locale serviceLocale = null;
				if (create) {

					
					// always wrap the property set in an outer set.
					// this ensures that any random attributes from
					// a JDBC URL are not written into the service.properties
					// file (e.g. like user and password :-)
					properties = new Properties(properties);

					serviceLocale = setLocale(properties);

					properties.put(Property.SERVICE_PROTOCOL, factoryInterface);

					serviceName = provider.createServiceRoot(serviceName,
							Boolean.valueOf(properties.getProperty(Property.DELETE_ON_CREATE)).booleanValue());

					serviceKey = ProtocolKey.create(factoryInterface, serviceName);
				} else if (properties != null) {
					String serverLocaleDescription = properties.getProperty(Property.SERVICE_LOCALE);
					if ( serverLocaleDescription != null)
						serviceLocale = staticGetLocaleFromString(serverLocaleDescription);
				}

				ts = new TopService(this, serviceKey, provider, serviceLocale);
				services.addElement(ts);
			}

			if (SanityManager.DEBUG) {
				if (provider != null)
				{
					SanityManager.ASSERT(provider.getCanonicalServiceName(serviceName).equals(serviceName),
						"mismatched canonical names " + provider.getCanonicalServiceName(serviceName)
						+ " != " + serviceName);
					SanityManager.ASSERT(serviceName.equals(serviceKey.getIdentifier()),
						"mismatched names " + serviceName + " != " + serviceKey.getIdentifier());
				}
			}


			if (properties != null) {

				// these properties must not be stored in the persistent properties,
				// otherwise moving databases from one directory to another
				// will not work. Thus they all have a fixed prefix

				// the root of the data
				properties.put(PersistentService.ROOT, serviceName);

				// the type of the service
				properties.put(PersistentService.TYPE, provider.getType());
			}

			if (SanityManager.DEBUG && reportOn) {
				dumpProperties("Service Properties: " + serviceKey.toString(), properties);
			}

			// push a new context manager
			if (previousCM == null) {
				cm = contextService.newContextManager();

				contextService.setCurrentContextManager(cm);
			}
			sb = new ServiceBootContext(cm);

			UpdateServiceProperties usProperties;
			Properties serviceProperties;


			//while doing restore from backup, we don't want service properties to be
			//updated until all the files are copied from backup.
			boolean inRestore = (properties !=null ?
								 properties.getProperty(Property.IN_RESTORE_FROM_BACKUP) != null:false);
			
			if ((provider != null) && (properties != null)) {
				// we need to track to see if the properties have
				// been updated or not. If the database is not created yet, we don't create the
				// services.properties file yet. We let the following if (create) statement do
				//that at the end of the database creation. After that, the changes in
				// services.properties file will be tracked by UpdateServiceProperties.
				usProperties = new UpdateServiceProperties(provider,
														   serviceName,
														   properties, 
														   !(create || inRestore));
				serviceProperties = usProperties;
			} else {
				usProperties = null;
				serviceProperties = properties;
			}

			instance = ts.bootModule(create, null, serviceKey, serviceProperties);

			if (create || inRestore) {
				// remove all the in-memory properties
				provider.saveServiceProperties(serviceName, usProperties.getStorageFactory(),
						BaseMonitor.removeRuntimeProperties(properties), false);
				usProperties.setServiceBooted();
			}

		} catch (Throwable t) {

			// ensure that the severity will shutdown the service
			if ((t instanceof StandardException) && (((StandardException) t).getSeverity() == ExceptionSeverity.DATABASE_SEVERITY))
				;
			else
				t = Monitor.exceptionStartingModule(t);

			if (cm != previousCM) {
				cm.cleanupOnError(t);
			}

			if (ts != null) {
				ts.shutdown();
				synchronized (this) {
					services.removeElement(ts);
				}

				// Service root will only have been created if
				// ts is non-null.
				boolean deleteOnError = (properties !=null ?
										 properties.getProperty(Property.DELETE_ROOT_ON_ERROR) !=null:false);
				if (create || deleteOnError)
					provider.removeServiceRoot(serviceName);
			}


			Throwable nested = ((StandardException) t).getNestedException();

			// never hide ThreadDeath
			if (nested instanceof ThreadDeath)
				throw (ThreadDeath) t;

			if (nested instanceof StandardException)
				throw (StandardException) t;

			throw (StandardException) t;

		} finally {
			if ((previousCM == cm) && (sb != null))
				sb.popMe();

			if (previousCM == null)
				contextService.resetCurrentContextManager(cm);
		}

		// from this point onwards the service is open for business
		ts.setTopModule(instance);

		//
		// The following yield allows our background threads to
		// execute their run methods. This is needed due to
		// bug 4081540 on Solaris. When the bug is fixed we can
		// remove this yield.
		Thread.yield();

		return instance;
	}

	/*
	** Methods of com.ibm.db2j.system.System
	*/

	/**
	Return the UUID factory for this system.  Returns null
	if there isn't one.
	@see com.ibm.db2j.system.System
	*/
	public UUIDFactory getUUIDFactory()	{

		return uuidFactory;
	}

	/*
	** Methods to deal with storing error messages until an InfoStreams is available.
	*/

	private PrintWriter tmpWriter;
	private AccessibleByteArrayOutputStream tmpArray;
	private boolean dumpedTempWriter;

	private PrintWriter getTempWriter() {
		if (tmpWriter == null && !dumpedTempWriter) {
			tmpArray = new AccessibleByteArrayOutputStream();
			tmpWriter = new PrintWriter(tmpArray);
		}
		return tmpWriter;
	}

	private void dumpTempWriter(boolean bothPlaces) {

		if (tmpWriter == null)
			return;

		tmpWriter.flush();

		BufferedReader lnr = new BufferedReader(
			new InputStreamReader(
				new ByteArrayInputStream(tmpArray.getInternalByteArray())));
		try {
			String s;
			while ((s = lnr.readLine()) != null) {
				if (systemStreams != null)
					systemStreams.stream().printlnWithHeader(s);

				if ((systemStreams == null) || bothPlaces)
					logging.println(s);
			}
		} catch (IOException ioe) {
		}

		if ((systemStreams == null) || bothPlaces)
			logging.flush();

		tmpWriter = null;
		tmpArray = null;
		dumpedTempWriter = true;
		logging = null;
	}

	/**
		If the module implements ModuleSupportable then call its
		canSupport() method to see if it can or should run in
		this setup. If it doesn't then it can always run.
	*/
	static boolean canSupport(Object instance, Properties properties) {
		if (instance instanceof ModuleSupportable) {
			// see if the instance can support the properties
			if (!((ModuleSupportable) instance).canSupport(properties))
				return false;
		}
		return true;
	}


	/**
		Boot a module. If the module implements ModuleControl
		then its boot() method is called. Otherwise all the
		boot code is assumed to take place in its constructor.
	*/
	static void boot(Object module, boolean create, Properties properties)
		throws StandardException {

		if (module instanceof ModuleControl)
			((ModuleControl) module).boot(create, properties);
	}

	/*
	** Locale handling
	*/
	private static Locale staticGetLocaleFromString(String localeDescription)
		throws StandardException {

		// Check String is of expected format
		// even though country should not be optional
		// some jvm's support this, so go with the flow.
		// xx[_YY[_variant]]

		int len = localeDescription.length();

		boolean isOk = (len == 2) || (len == 5) || (len > 6);

		// must have underscores at position 2
		if (isOk && (len != 2))
			isOk = localeDescription.charAt(2) == '_';

		// must have underscores at position 2
		if (isOk && (len > 5))
			isOk = localeDescription.charAt(5) == '_';

		if (!isOk)
			throw StandardException.newException(SQLState.INVALID_LOCALE_DESCRIPTION, localeDescription);

		String language = localeDescription.substring(0, 2);
		String country = len == 2 ? "" : localeDescription.substring(3, 5);

		if (len < 6) {
			return new Locale(language, country);
		}

		String variant = (len > 6) ? localeDescription.substring(6, len) : null;

		return new Locale(language, country, variant);
	}

	private static Locale setLocale(Properties properties)
		throws StandardException {

		String userDefinedLocale = properties.getProperty(Attribute.TERRITORY);
		Locale locale;
		if (userDefinedLocale == null)
			locale = Locale.getDefault();
		else {
			// validate the passed in string
			locale = staticGetLocaleFromString(userDefinedLocale);
		}

		properties.put(Property.SERVICE_LOCALE, locale.toString());
		return locale;
	}

	/*
	** BundleFinder
	*/

	//private Hashtable localeBundles;

	/**
		Get the locale from the ContextManager and then find the bundle
		based upon that locale.
	*/
	public ResourceBundle getBundle(String messageId) {
		ContextManager cm;
		try {
			cm = ContextService.getFactory().getCurrentContextManager();
		} catch (ShutdownException se) {
			cm = null;
		}

		if (cm != null) {
			return MessageService.getBundleForLocale(cm.getMessageLocale(), messageId);
		}
		return null;
	}

	public Thread getDaemonThread(Runnable task, String name, boolean setMinPriority) {
		Thread t =  new Thread(daemonGroup, task, "derby.".concat(name));
		t.setDaemon(true);
		if (setMinPriority) {
			t.setPriority(Thread.MIN_PRIORITY);
		}
		return t;

	}

	public void setThreadPriority(int priority) {

		Thread t = Thread.currentThread();

		if (t.getThreadGroup() == daemonGroup) {
			t.setPriority(priority);
		}
	}

	/**
		Initialize the monitor wrt the current environemnt.
		Returns false if the monitor cannot be initialized, true otherwise.
	*/
	public abstract boolean initialize(boolean lite);

    class ProviderEnumeration implements Enumeration
    {
        private Enumeration serviceProvidersKeys = (serviceProviders == null) ? null : serviceProviders.keys();
        private Properties startParams;
        private Enumeration paramEnumeration;
        private boolean enumeratedDirectoryProvider;
        private PersistentService storageFactoryPersistentService;

        ProviderEnumeration( Properties startParams)
        {
            this.startParams = startParams;
            if( startParams != null)
                paramEnumeration = startParams.keys();
        }

        public Object nextElement() throws NoSuchElementException
        {
            if( serviceProvidersKeys != null && serviceProvidersKeys.hasMoreElements())
                return serviceProviders.get( serviceProvidersKeys.nextElement());
            getNextStorageFactory();
            Object ret = storageFactoryPersistentService;
            storageFactoryPersistentService = null;
            return ret;
        }

        private void getNextStorageFactory()
        {
            if( storageFactoryPersistentService != null)
                return;
            if( paramEnumeration != null)
            {
                while( paramEnumeration.hasMoreElements())
                {
                    String prop = (String) paramEnumeration.nextElement();
                    if( prop.startsWith( Property.SUB_SUB_PROTOCOL_PREFIX))
                    {
                        try
                        {
                            String storageFactoryClassName = (String) startParams.get( prop);
                            if( storageFactoryClassName != null)
                            {
                                storageFactoryPersistentService =
                                  getPersistentService( (String) startParams.get( prop),
                                                        prop.substring( Property.SUB_SUB_PROTOCOL_PREFIX.length()));
                                if( storageFactoryPersistentService != null)
                                    return;
                            }
                        }
                        catch( StandardException se){};
                    }
                }
            }
            if( ! enumeratedDirectoryProvider)
            {
                try
                {
                    storageFactoryPersistentService
                      = getPersistentService( getStorageFactoryClassName( null, PersistentService.DIRECTORY),
                                              PersistentService.DIRECTORY);
                }
                catch( StandardException se){ storageFactoryPersistentService = null; }
                enumeratedDirectoryProvider = true;
            }
        } // end of getNextStorageFactory

        public boolean hasMoreElements()
        {
            if( serviceProvidersKeys != null && serviceProvidersKeys.hasMoreElements())
                return true;
            getNextStorageFactory();
            return storageFactoryPersistentService != null;
        }
    } // end of class ProviderEnumeration
} // end of class BaseMonitor

class AntiGC implements Runnable {

	boolean goAway;
	private Object keep1;

	AntiGC(Object a) {
		keep1 = a;
	}

	public void run() {

		goAway = false;

		while (true) {
			synchronized (this) {
				if (goAway)
					return;
				try {
					wait();
				} catch (InterruptedException ie) {
				}
			}
		}
	}
} // end of class AntiGC
