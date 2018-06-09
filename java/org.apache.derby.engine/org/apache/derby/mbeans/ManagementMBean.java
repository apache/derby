/*

   Derby - Class org.apache.derby.mbeans.ManagementMBean

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

package org.apache.derby.mbeans;

/**
 * JMX MBean inteface to control visibility of Derby's MBeans.
 * When Derby boots it attempts to register its MBeans.
 * It may fail due to lack of valid permissions.
 * If Derby does not register its MBeans then an
 * application may register the Management implementation
 * of ManagementMBean itself and use it to start Derby's
 * JMX management.
 * <P>
 * Key properties for registered MBean when registered by Derby:
 * <UL>
 * <LI> <code>type=Management</code>
 * <LI> <code>system=</code><em>runtime system identifier</em> (see overview)
 * </UL>
 * 
 * @see Management
 * @see ManagementMBean#getSystemIdentifier()
 */
public interface ManagementMBean {
    
    /**
     * Is Derby's JMX management active. If active then Derby
     * has registered MBeans relevant to its current state.
     * @return true Derby has registered beans, false Derby has not
     * registered any beans.
     */
    public boolean isManagementActive();
    
    /**
     * Get the system identifier that this MBean is managing.
     * The system identifier is a runtime value to disambiguate
     * multiple Derby systems in the same virtual machine but
     * different class loaders.
     * 
     * @return Runtime identifier for the system, null if Derby is not running.
     */
    public String getSystemIdentifier();
    
    /**
     * Inform Derby to start its JMX management by registering
     * MBeans relevant to its current state. If Derby is not
     * booted then no action is taken.
     * <P>
     * Require <code>SystemPermission("jmx", "control")</code> if a security
     * manager is installed.
     * 
     * @see org.apache.derby.shared.common.security.SystemPermission
     */
    public void startManagement();
    
    /**
     * Inform Derby to stop its JMX management by unregistering
     * its MBeans. If Derby is not booted then no action is taken.
     * <P>
     * Require <code>SystemPermission("jmx", "control")</code> if a security
     * manager is installed.
     * 
     * @see org.apache.derby.shared.common.security.SystemPermission
     */
    public void stopManagement();
}
