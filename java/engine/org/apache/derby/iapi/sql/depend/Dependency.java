/*

   Derby - Class org.apache.derby.iapi.sql.depend.Dependency

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

package org.apache.derby.iapi.sql.depend;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.catalog.UUID;

/**
	A dependency represents a reliance of the dependent on
	the provider for some information the dependent contains
	or uses.  In Language, the usual case is a prepared statement
	using information about a schema object in its executable form.
	It needs to be notified if the schema object changes, so that
	it can recompile against the new information.
 */
public interface Dependency {

	/**
		return the provider's key for this dependency.
		@return the provider's key for this dependency
	 */
	UUID getProviderKey();

	/**
		return the provider for this dependency.
		@return the provider for this dependency
	 */
	Provider getProvider();

	/**
		return the dependent for this dependency.
		@return the dependent for this dependency
	 */
	Dependent getDependent();

}
