/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.depend
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.depend;

import org.apache.derby.iapi.sql.depend.Dependency;
import org.apache.derby.iapi.sql.depend.Dependent;
import org.apache.derby.iapi.sql.depend.Provider;

import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.error.StandardException;

/**
	A dependency represents a reliance of the dependent on
	the provider for some information the dependent contains
	or uses.  In Language, the usual case is a prepared statement
	using information about a schema object in its executable form.
	It needs to be notified if the schema object changes, so that
	it can recompile against the new information.
 */
class BasicDependency implements Dependency { 

	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;

	//
	// Dependency interface
	//

	/**
		return the provider's key for this dependency.
		@return the provider' key for this dependency
	 */
	public UUID getProviderKey() {
		return provider.getObjectID();
	}

	/**
		return the provider for this dependency.
		@return the provider for this dependency
	 */
	public Provider getProvider() {
		return provider;
	}

	/**
		return the dependent for this dependency.
		@return the dependent for this dependency
	 */
	public Dependent getDependent() {
		return dependent;
	}

	//
	// class interface
	//
	BasicDependency(Dependent d, Provider p) {
		dependent = d;
		provider = p;
	}

	//
	// class implementation
	//
	protected Provider	provider;
	protected Dependent	dependent;
}
