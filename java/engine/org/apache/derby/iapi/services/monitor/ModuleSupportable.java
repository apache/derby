/*

   Derby - Class org.apache.derby.iapi.services.monitor.ModuleSupportable

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import java.util.Properties;

/**
	Allows a module to check its environment
	before it is selected as an implementation.
*/

public interface ModuleSupportable {

	/**
		See if this implementation can support any attributes that are listed in properties.
		This call may be made on a newly created instance before the
		boot() method has been called, or after the boot method has
		been called for a running module.
		<P>
		The module can check for attributes in the properties to
		see if it can fulfill the required behaviour. E.g. the raw
		store may define an attribute called RawStore.Recoverable.
		If a temporary raw store is required the property RawStore.recoverable=false
		would be added to the properties before calling bootServiceModule. If a
		raw store cannot support this attribute its canSupport method would
		return null. Also see the Monitor class's prologue to see how the
		identifier is used in looking up properties.
		<BR><B>Actually a better way maybe to have properties of the form
		RawStore.Attributes.mandatory=recoverable,smallfootprint and
		RawStore.Attributes.requested=oltp,fast
		</B>

		@return true if this instance can be used, false otherwise.
	*/
	public boolean canSupport(Properties properties);

}
