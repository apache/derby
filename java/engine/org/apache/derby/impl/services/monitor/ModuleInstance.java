/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.monitor
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.services.monitor;

import org.apache.derby.iapi.services.monitor.PersistentService;

import java.util.Properties;

/**
	A description of an instance of a module.
*/


class ModuleInstance {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;

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
