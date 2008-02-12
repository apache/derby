/*
   Derby Classname org.apache.derby.iapi.services.jmx.ManagementService
  
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

package org.apache.derby.iapi.services.jmx;


/**
* This interface represents a Management Service. An implementation of this 
* service is started by the Derby monitor if the system property derby.system.jmx has
* been set. The following services are provided:
* 
*	<li> Create and start an instance of MBean server to register MBeans.
*       <li> Create managed beans (MBeans) to instrument derby resources for
*            management and monitoring.
* 
* The following code can be used to locate an instance of this service
* if running.
*
* ManagementService ms = (ManagementService)Monitor.getSystemModule
*		("org.apache.derby.iapi.services.mbeans.ManagementService");
*
*/
public interface ManagementService {
	
    /* Class name of this interface */
    public static final String MODULE = 
            "org.apache.derby.iapi.services.jmx.ManagementService";
}
