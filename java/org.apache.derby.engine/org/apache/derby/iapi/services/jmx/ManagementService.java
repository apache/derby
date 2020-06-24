/*

   Derby - Class org.apache.derby.iapi.services.jmx.ManagementService

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.iapi.services.jmx;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.mbeans.ManagementMBean;

/**
* <p>
* This interface represents a Management Service. An implementation of this 
* service is started by the Derby monitor if the system property derby.system.jmx has
* been set. The following services are provided:
* </p>
* 
* <ul>
*	<li> Create and start an instance of MBean server to register MBeans.</li>
*   <li> Create managed beans (MBeans) to instrument derby resources for
*            management and monitoring.</li>
* </ul>
* 
* <p>
* The following code can be used to locate an instance of this service
* if running.
* </p>
*
* <p>
* ManagementService ms = (ManagementService)
*        Monitor.getSystemModule(Module.JMX);
* </p>
*
*/
public interface ManagementService extends ManagementMBean {
   
    /**
     * The domain for all of derby's mbeans: org.apache.derby
     */
    public static final String DERBY_JMX_DOMAIN = "org.apache.derby";
    
    /**
     * Registers an MBean with the MBean server.
     * The mbean will be unregistered automatically when Derby shuts down.
     * 
     *
     * @param <T> The kind of bean
     * @param bean The MBean to wrap with a StandardMBean and register
     * @param beanInterface The management interface for the MBean.
     * @param keyProperties The String representation of the MBean's key properties,
     * they will be added into the ObjectName with Derby's domain. Key
     * type should be first with a short name for the bean, typically the
     * class name without the package.
     *
     * @return An identifier that can later be used to unregister the mbean.
     * @throws StandardException on error
     */
    public <T> Object registerMBean(T bean,
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
            Class<T> beanInterface,
            String keyProperties)
            throws StandardException;
    
    /**
     * Unregister a mbean previously registered with registerMBean.
     * 
     * @param mbeanIdentifier An identifier returned by registerMBean.
     */
    public void unregisterMBean(Object mbeanIdentifier);

    /**
     * Quote an MBean key property value, so that it is safe to pass to
     * {@link #registerMBean} even if it potentially contains special
     * characters.
     *
     * @param value the value to quote
     * @return the quoted value
     * @see javax.management.ObjectName#quote(String)
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-6733
    String quotePropertyValue(String value);
}
