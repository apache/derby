/*

   Derby - Class org.apache.derby.impl.sql.depend.BasicDependency

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
