/*

   Derby - Class org.apache.derby.impl.services.monitor.ModuleInstance

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

import org.apache.derby.iapi.services.monitor.PersistentService;

import java.util.Properties;

/**
	A description of an instance of a module.
*/


class ModuleInstance {

	/*
	** Fields.
	*/

	/**
		The module instance
	*/
	protected Object instance;

	/**
		name of module, can be null
	*/
	protected String		identifier;

	/**
		the top-level service this module lives in, can be null or the service itself
	*/
	protected Object	topLevelService;

	/**
		the actual service to which I belong, could be null.
	*/
	protected Object	service;

	/*
	** Constructor
	*/

	protected ModuleInstance(Object instance, String identifier,
			Object service, Object topLevelService)
	{
		super();
		this.instance = instance;
		this.identifier = identifier;
		this.topLevelService = topLevelService;
		this.service = service;

	}

	protected ModuleInstance(Object instance) {

		this(instance, null, null, null);
	}

	protected boolean isTypeAndName(PersistentService serviceType, 
		Class factoryInterface, String otherCanonicalName)
	{
		// see if the correct interface is implemented
		if (!factoryInterface.isInstance(instance))
			return false;

		if ((serviceType != null) && (otherCanonicalName != null))
			return serviceType.isSameService(identifier, otherCanonicalName);


		// see if the identifiers match
		if (otherCanonicalName != null) {
			if (identifier == null)
				return false;
			if (!otherCanonicalName.equals(identifier))
				return false;
		} else if (identifier != null) {
			return false;
		}

		return true;
	}

	protected String getIdentifier() {
		return identifier;
	}

	protected Object getTopLevelService() {
		return topLevelService;
	}

	protected Object getInstance() {
		return instance;
	}
}
