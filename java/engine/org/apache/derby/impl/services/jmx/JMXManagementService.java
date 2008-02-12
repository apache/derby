/*
 Derby Classname org.apache.derby.impl.services.jmx.JMXManagementService
 
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

package org.apache.derby.impl.services.jmx;

import java.lang.management.ManagementFactory;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.services.jmx.ManagementService;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.mbeans.Version;

/** 
 * This class implements the ManagementService interface and provides a simple
 * management and monitoring service.
 *
 * @see org.apache.derby.iapi.services.jmx.ManagementService
 */
public class JMXManagementService implements ManagementService, ModuleControl {

    /**
     * Platfrom MBean server, from ManagementFactory.getPlatformMBeanServer().
     */
    private MBeanServer mbeanServer;
    
    /**
     * The set of mbeans registered by this service.
     */
    private Set<ObjectName> registeredMbeans;

    public JMXManagementService() {

    }

    /**
     * Start the management service if derby.system.jmx is true.
     * <P>
     * Starting the service means:
     * <UL>
     * <LI> getting the platform MBeanServer which may require starting it
     * <LI> registering a Version mbean representing the system
     * </UL>
     */
    public void boot(boolean create, Properties properties)
            throws StandardException {

        if (PropertyUtil.getSystemBoolean(Property.JMX))
            initialize();
    }

    public synchronized void stop() {
        if (mbeanServer == null)
            return;
        
        for (ObjectName mbeanName : registeredMbeans)
            try {
                unregisterMBean(mbeanName);
            } catch (StandardException e) {
                // TODO: what to do here?
            }
    }

    /**
     * Initialize the management service by obtaining the platform
     * MBeanServer and registering system beans. Separate from
     * boot() to allow future changes where the jmx management
     * can be enabled on the fly.
     * 
     * @throws StandardException
     */
    private synchronized void initialize() throws StandardException {
        
        registeredMbeans = new HashSet<ObjectName>();
        try {
            mbeanServer = AccessController
                    .doPrivileged(new PrivilegedAction<MBeanServer>() {
                        public MBeanServer run() {
                            return ManagementFactory.getPlatformMBeanServer();
                        }
                    });

            registerMBean(new Version(Monitor.getMonitor().getEngineVersion()),
                    "org.apache.derby:type=Version,jar=derby.jar");
            
        } catch (SecurityException se) {
            // TODO: just ignoring inability to create the mbean server.
            // or should an error or warning be raised?
        }
    }

    /**
     * Registers an MBean with the MBean server. The object name instance 
     * represented by the given String will be created by this method.
     * 
     * @param bean The MBean to register
     * @param name The String representation of the MBean's object name.
     * 
     */
    private synchronized void registerMBean(final Object bean, final String name)
            throws StandardException {

        if (mbeanServer == null)
            return;

        try {
            final ObjectName beanName = new ObjectName(name);
            try {

                AccessController
                        .doPrivileged(new PrivilegedExceptionAction<Object>() {

                            public Object run() throws JMException {
                                mbeanServer.registerMBean(bean, beanName);
                                return null;
                            }

                        });
                
                registeredMbeans.add(beanName);

            } catch (PrivilegedActionException pae) {
                throw (JMException) pae.getException();
            }
        } catch (JMException jme) {
            throw StandardException.plainWrapException(jme);
        }
    }
    
    /**
     * Unregisters an mbean that was registered  by this service.
     * @param mbeanName Bean to unregister.
     * @throws StandardException
     */
    private synchronized void unregisterMBean(final ObjectName mbeanName)
        throws StandardException
    {
        if (!registeredMbeans.remove(mbeanName))
            return;

        if (!mbeanServer.isRegistered(mbeanName))
            return;

        try {

            AccessController
                    .doPrivileged(new PrivilegedExceptionAction<Object>() {

                        public Object run() throws JMException {
                            mbeanServer.unregisterMBean(mbeanName);
                            return null;
                        }

                    });

        } catch (PrivilegedActionException pae) {
            throw StandardException.plainWrapException(pae.getException());
        }
    }
}
