/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.monitor
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.services.monitor;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.monitor.Monitor;


/**
	A class that represents a key for a module search.
*/


class ProtocolKey {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;

	/*
	** Fields.
	*/

	/**
		The class of the factory
	*/
	protected Class factoryInterface;

	/**
		name of module, can be null
	*/
	protected String		identifier;

	/*
	** Constructor
	*/

	protected ProtocolKey(Class factoryInterface, String identifier)
	{
		super();
		this.factoryInterface = factoryInterface;
		this.identifier = identifier;
	}

	static ProtocolKey create(String className, String identifier) throws StandardException {

		Throwable t;
		try {
			return new ProtocolKey(Class.forName(className), identifier);

		} catch (ClassNotFoundException cnfe) {
			t = cnfe;
		} catch (IllegalArgumentException iae) {
			t = iae;
		}

		throw Monitor.exceptionStartingModule(t);	
	}

	/*
	** Methods required to use this key
	*/

	protected Class getFactoryInterface() {
		return factoryInterface;
	}

	protected String getIdentifier() {
		return identifier;
	}

	/*
	**
	*/

	public int hashCode() {
		return factoryInterface.hashCode() +
			(identifier == null ? 0  : identifier.hashCode());
	}

	public boolean equals(Object other) {
		if (other instanceof ProtocolKey) {
			ProtocolKey otherKey = (ProtocolKey) other;

			if (factoryInterface != otherKey.factoryInterface)
				return false;

			if (identifier == null) {
				if (otherKey.identifier != null)
					return false;
			} else {

				if (otherKey.identifier == null)
					return false;

				if (!identifier.equals(otherKey.identifier))
					return false;
			}

			return true;
		}
		return false;
	}

	public String toString() {

		return factoryInterface.getName() + " (" + identifier + ")";
	}
}
