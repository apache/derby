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

import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.services.info.Version;
import org.apache.derby.iapi.services.jmx.ManagementService;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.mbeans.VersionMBean;

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
            unregisterMBean(mbeanName);
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
                    VersionMBean.class,
                    "type=Version,jar=derby.jar");
            
        } catch (SecurityException se) {
            // TODO: just ignoring inability to create the mbean server.
            // or should an error or warning be raised?
        }
    }

    /**
     * Registers an MBean with the MBean server as a StandardMBean.
     * Use of the StandardMBean allows the implementation details
     * of Derby's mbeans to be hidden from JMX, thus only exposing
     * the MBean's interface in org.apache.derby.mbeans.
     * 
     * The object name instance 
     * represented by the given String will be created by this method.
     * 
     * @param bean The MBean to wrap with a StandardMBean and register
     * @param beanInterface The management interface for the MBean.
     * @param nameAttributes The String representation of the MBean's attrributes,
     * they will be added into the ObjectName with Derby's domain
     * 
     */
    public synchronized Object registerMBean(final Object bean,
            final Class beanInterface,
            final String nameAttributes)
            throws StandardException {

        if (mbeanServer == null)
            return null;

        try {
            final ObjectName beanName = new ObjectName(
                    DERBY_JMX_DOMAIN + ":" + nameAttributes);
            final StandardMBean standardMBean =
                new StandardMBean(bean, beanInterface);
            try {

                AccessController
                        .doPrivileged(new PrivilegedExceptionAction<Object>() {

                            public Object run() throws JMException {
                                mbeanServer.registerMBean(standardMBean, beanName);
                                return null;
                            }

                        });
                
                registeredMbeans.add(beanName);
                return beanName;

            } catch (PrivilegedActionException pae) {
                throw (JMException) pae.getException();
            }
        } catch (JMException jme) {
            throw StandardException.plainWrapException(jme);
        }
    }
    
    /**
     * Unregister an mbean using an object previous returned
     * from registerMBean.
     */
    public void unregisterMBean(Object mbeanIdentifier)
    {
        if (mbeanIdentifier == null)
            return;
        unregisterMBean((ObjectName) mbeanIdentifier);
    }
    
    /**
     * Unregisters an mbean that was registered  by this service.
     * @param mbeanName Bean to unregister.
     */
    private synchronized void unregisterMBean(final ObjectName mbeanName)
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
            // TODO - this is called on shutdown where
            // we don't really care about errors.
            // JMException jme = (JMException) pae.getException();
            //if (!(jme instanceof InstanceNotFoundException))
                // throw StandardException.plainWrapException(jme);
        }
    }
}
