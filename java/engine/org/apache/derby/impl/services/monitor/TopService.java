/*

   Derby - Class org.apache.derby.impl.services.monitor.TopService

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

import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.monitor.PersistentService;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.EngineType;

import java.util.Hashtable;
import java.util.Vector;
import java.util.Properties;
import java.util.Locale;

/**
	A description of an instance of a module.
*/


class TopService {

	/*
	** Fields.
	*/

	/**
		The idenity of this service, note that it may not be active yet.
	*/
	protected ProtocolKey key;

	/**
		The top module instance
	*/
	protected ModuleInstance topModule;

	/**
		List of protocols.
	*/
	protected Hashtable		protocolTable;

	/**
	*/
	protected Vector		moduleInstances;

	/**
	*/
	protected BaseMonitor	monitor;

	protected boolean inShutdown;

	/**
		The type of service this was created by. If null then this is a non-persistent service.
	*/
	protected PersistentService serviceType;

	Locale serviceLocale;

	/*
	** Constructor
	*/


	TopService(BaseMonitor monitor) {
		super();
		this.monitor = monitor;
		protocolTable = new Hashtable();
		moduleInstances = new Vector(0, 5);
	}

	TopService(BaseMonitor monitor, ProtocolKey key, PersistentService serviceType, Locale serviceLocale)
	{
		this(monitor);

		this.key = key;
		this.serviceType = serviceType;
		this.serviceLocale = serviceLocale;
	}

	protected void setTopModule(Object instance) {
		synchronized (this) {
			for (int i = 0; i < moduleInstances.size(); i++) {
				ModuleInstance module = (ModuleInstance) moduleInstances.elementAt(i);
				if (module.getInstance() == instance) {
					topModule = module;
					notifyAll();
					break;
				}
			}

			// now add an additional entry into the hashtable
			// that maps the server name as seen by the user
			// onto the top module. This allows modules to find their
			// top most service moduel using the monitor.getServiceName() call,
			// e.g. Monitor.findModule(ref, inferface, Monitor.getServiceName(ref));
			if (getServiceType() != null) {
				ProtocolKey userKey = new ProtocolKey(key.getFactoryInterface(),
					monitor.getServiceName(instance));
				addToProtocol(userKey, topModule);
			}

		}
	}

	protected Object getService() {

		return topModule.getInstance();
	}

	protected boolean isPotentialService(ProtocolKey otherKey) {


		String otherCanonicalName;

		if (serviceType == null)
			otherCanonicalName = otherKey.getIdentifier();
		else {
			otherCanonicalName = serviceType.getCanonicalServiceName(otherKey.getIdentifier());

			// if the service name cannot be converted into a canonical name then it is not a service.
			if (otherCanonicalName == null)
				return false;
		}

		if (topModule != null)
			return topModule.isTypeAndName(serviceType, key.getFactoryInterface(), otherCanonicalName);


		if (!otherKey.getFactoryInterface().isAssignableFrom(key.getFactoryInterface()))
			return false;

		return serviceType.isSameService(key.getIdentifier(), otherCanonicalName);
	}

	boolean isActiveService() {
		synchronized (this) {
			return (topModule != null);
		}
	}

	boolean isActiveService(ProtocolKey otherKey) {

		synchronized (this) {
			if (inShutdown)
				return false;

			if (!isPotentialService(otherKey))
				return false;

			if (topModule != null) {
				if (SanityManager.DEBUG) {
					SanityManager.ASSERT(topModule.isTypeAndName(serviceType,
						key.getFactoryInterface(), key.getIdentifier()));
				}

				return true;
			}

			// now wait for topModule to be set
			while (!inShutdown && (topModule == null)) {
				try {
					wait();
				} catch (InterruptedException ioe) {
					return false;
				}
			}

			if (inShutdown)
				return false;

			return true;
		}
	}

	/**
		Find an module in the protocol table that supports the required protocol
		name combination and can handle the properties.

		Returns the instance of the module or null if one does not exist in
		the protocol table.
	*/
	protected synchronized Object findModule(ProtocolKey key, boolean findOnly, Properties properties) {

		ModuleInstance module = (ModuleInstance) protocolTable.get(key);

		if (module == null)
			return null;

		Object instance = module.getInstance();

		if (findOnly || BaseMonitor.canSupport(instance, properties))
			return instance;

		return null;
	}

	/**
		Boot a module, performs three steps.

		<OL>
		<LI> Look for an existing module in the protocol table
		<LI> Look for a module in the implementation table that handles this protocol
		<LI> Create an instance that handles this protocol.
		</OL>
	*/
	Object bootModule(boolean create, Object service, ProtocolKey key, Properties properties) 
		throws StandardException {

		synchronized (this) {
			if (inShutdown)
				throw StandardException.newException(SQLState.SHUTDOWN_DATABASE, getKey().getIdentifier());
		}

		//  see if this system already has a module that will work.
		Object instance = findModule(key, false, properties);
		if (instance != null)
			return instance;
		
		if (monitor.reportOn) {
			monitor.report("Booting Module   " + key.toString() + " create = " + create);
		}

		// see if a running implementation will handle this protocol
		synchronized (this) {

			for (int i = 0; i < moduleInstances.size(); i++) {
				ModuleInstance module = (ModuleInstance) moduleInstances.elementAt(i);

				if (!module.isTypeAndName((PersistentService) null, key.getFactoryInterface(), key.getIdentifier()))
					continue;

				instance = module.getInstance();
				if (!BaseMonitor.canSupport(instance, properties))
					continue;

				// add it to the protocol table, if this returns false then we can't use
				// this module, continue looking.
				if (!addToProtocol(key, module))
					continue;

				if (monitor.reportOn) {
					monitor.report("Started Module   " + key.toString());
					monitor.report("  Implementation " + instance.getClass().getName());
				}

				return instance;
			}
		}

		// try and load an instance that will support this protocol
		instance = monitor.loadInstance(key.getFactoryInterface(), properties);
		if (instance == null)
		{
			throw Monitor.missingImplementation(key.getFactoryInterface().getName());
		}
		ModuleInstance module = new ModuleInstance(instance, key.getIdentifier(), service,
				topModule == null ? (Object) null : topModule.getInstance());

		moduleInstances.addElement(module);

		try {
			BaseMonitor.boot(instance, create, properties);
		} catch (StandardException se) {
			moduleInstances.removeElement(module);
			throw se;
		}

		synchronized (this) {


			// add it to the protocol table, if this returns false then we can't use
			// this module, shut it down.
			if (addToProtocol(key, module)) {

				if (monitor.reportOn) {
					monitor.report("Started Module   " + key.toString());
					monitor.report("  Implementation " + module.getInstance().getClass().getName());
				}

				return module.getInstance();
			}

			
		}
	
		TopService.stop(instance);
		moduleInstances.removeElement(module);

		// if we reached here it's because someone else beat us adding the module, so use theirs.
		return findModule(key, true, properties);
	}

	/**	
		If the service is already beign shutdown we return false.
	*/
	boolean shutdown() {

		synchronized (this) {
			if (inShutdown)
				return false;

			inShutdown = true;
			notifyAll();
		}

		for (;;) {

			ModuleInstance module;

			synchronized (this) {

				if (moduleInstances.isEmpty())
					return true;

				module = (ModuleInstance) moduleInstances.elementAt(0);

			}
			
			Object instance = module.getInstance();
			TopService.stop(instance);
			
			synchronized (this) {
				moduleInstances.removeElementAt(0);
			}
		}
	}

	/**
		Add a running module into the protocol hash table. Return true
		if the module was added successfully, false if it couldn't
		be added. In the latter case the module should be shutdown
		if its reference count is 0.
	*/

	private boolean addToProtocol(ProtocolKey key, ModuleInstance module) {

		String identifier = module.getIdentifier();

		synchronized (this) {

			Object value = protocolTable.get(key);
			if (value == null) {

				protocolTable.put(key, module);
				return true;
			}

			if (value == module)
				return true;

			return false;
		}
	}

	protected boolean inService(Object instance) {

		for (int i = 0; i < moduleInstances.size(); i++) {

			ModuleInstance mi = (ModuleInstance) moduleInstances.elementAt(i);
			if (mi.getInstance() == instance)
				return true;
		}
		return false;
	}

	public ProtocolKey getKey() {
		return key;
	}

	PersistentService getServiceType() {
		return serviceType;
	}

	private static void stop(Object instance) {
		if (instance instanceof ModuleControl) {
			((ModuleControl) instance).stop();
		}
	}
}
