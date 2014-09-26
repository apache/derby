/*

   Derby - Class org.apache.derby.mbeans.Management

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

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.apache.derby.iapi.reference.Module;
import org.apache.derby.iapi.services.monitor.Monitor;

/** 
 * Management MBean to allow applications to dynamically
 * control visibility of Derby's MBeans.
 * If Derby does not register its ManagementMBean then an
 * application may register this implementation
 * of ManagementMBean itself and use it to start Derby's
 * JMX management.
 * <P>
 * If Derby is not booted then invoking startManagement will
 * do nothing.
*/
public class Management implements ManagementMBean {
    
    private ManagementMBean getManagementService() {
        return (ManagementMBean)
             getSystemModule(Module.JMX);
    }
    
    /**
     * Start Derby's MBeans.
     * @see ManagementMBean#startManagement()
     */
    public void startManagement() {
        
        ManagementMBean mgmtService = getManagementService();
        if (mgmtService != null)
            mgmtService.startManagement();   
    }
    
    /**
     * Stop Derby's MBeans.
     * @see ManagementMBean#stopManagement()
     */
    public void stopManagement() {
        ManagementMBean mgmtService = getManagementService();
        if (mgmtService != null)
            mgmtService.stopManagement();
    }

    /**
     * Return state of Derby's JMX management.
     * @see ManagementMBean#isManagementActive()
     */
    public boolean isManagementActive() {
        ManagementMBean mgmtService = getManagementService();
        if (mgmtService == null)
             return false;
        return mgmtService.isManagementActive();
    }

    /**
     * Return the system identifier that this MBean is managing.
     * @see ManagementMBean#getSystemIdentifier()
     */
    public String getSystemIdentifier() {
        ManagementMBean mgmtService = getManagementService();
        if (mgmtService == null)
             return null;
        return mgmtService.getSystemIdentifier();
    }
    
    /**
     * Privileged module lookup. Must be private so that user code
     * can't call this entry point.
     */
    private static  Object getSystemModule( final String factoryInterface )
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<Object>()
             {
                 public Object run()
                 {
                     return Monitor.getSystemModule( factoryInterface );
                 }
             }
             );
    }

}
