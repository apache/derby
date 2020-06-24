/*

   Derby - Class org.apache.derby.iapi.services.monitor.ModuleControl

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.iapi.services.monitor;

import java.util.Properties;
import org.apache.derby.shared.common.error.StandardException;

/**
	ModuleControl is <B>optionally</B> implemented by a module's factory class.
*/

public interface ModuleControl {

	/**
		Boot this module with the given properties. Creates a module instance
		that can be found using the findModule() methods of Monitor.
		The module can only be found using one of these findModule() methods
		once this method has returned.
		<P>
		An implementation's boot method can throw StandardException. If it
		is thrown the module is not registered by the monitor and therefore cannot
		be found through a findModule(). In this case the module's stop() method
		is not called, thus throwing this exception must free up any
		resources.
		<P>
		When create is true the contents of the properties object
		will be written to the service.properties of the persistent
		service. Thus any code that requires an entry in service.properties
		must <B>explicitly</B> place the value in this properties set
		using the put method.
		<BR>
		Typically the properties object contains one or more default
		properties sets, which are not written out to service.properties.
		These default sets are how callers modify the create process. In a
		JDBC connection database create the first set of defaults is a properties
		object that contains the attributes that were set on the jdbc:derby: URL.
		This attributes properties set has the second default properties set as
		its default. This set (which could be null) contains the properties
		that the user set on their DriverManager.getConnection() call, and are thus
//IC see: https://issues.apache.org/jira/browse/DERBY-2400
		not owned by Derby code, and thus must not be modified by Derby 
		code.
		<P>
		When create is false the properties object contains all the properties
		set in the service.properties file plus a <B>limited</B> number of
		attributes from the JDBC URL attributes or connection properties set.
		This avoids properties set by the user compromising the boot process.
		An example of a property passed in from the JDBC world is the bootPassword
		for encrypted databases.

		<P>
		Code should not hold onto the passed in properties reference after boot time
		as its contents may change underneath it. At least after the complete boot
		is completed, the links to all the default sets will be removed.

        @param create True if the database should be created
        @param properties Configuration properties
		@exception StandardException Module cannot be started.

		@see Monitor
		@see ModuleFactory
		
	*/

	public void boot(boolean create, Properties properties)
		throws StandardException;

	/**
		Stop the module.

		The module may be found via a findModule() method until some time after
		this method returns. Therefore the factory must be prepared to reject requests
		to it once it has been stopped. In addition other modules may cache a reference
		to the module and make requests of it after it has been stopped, these requests
		should be rejected as well.

		@see Monitor
		@see ModuleFactory
	*/

	public void stop();


}
