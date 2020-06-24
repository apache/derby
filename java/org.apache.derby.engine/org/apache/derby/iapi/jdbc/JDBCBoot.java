/*

   Derby - Class org.apache.derby.iapi.jdbc.JDBCBoot

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

package org.apache.derby.iapi.jdbc;

import java.io.PrintWriter;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.DriverManager;
import java.util.Properties;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.Attribute;
import org.apache.derby.shared.common.reference.MessageId;
import org.apache.derby.shared.common.reference.Property;
import org.apache.derby.iapi.jdbc.InternalDriver;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.property.PropertyUtil;

/**
	A class to boot a Derby system that includes a JDBC driver.
	Should be used indirectly through JDBCDriver or JDBCServletBoot
	or any other useful booting mechanism that comes along.
*/
public class JDBCBoot {

	private Properties bootProperties;

    private static final String NETWORK_SERVER_AUTOSTART_CLASS_NAME = "org.apache.derby.iapi.jdbc.DRDAServerStarter";

	public JDBCBoot() {
		bootProperties = new Properties();
	}

	void addProperty(String name, String value) {
		bootProperties.put(name, value);
	}

   /*
	** Find the appropriate driver for our JDBC level and boot it.
	*  This is package protected so that AutoloadedDriver can call it.
	*/
	public static void boot() {
        PrintWriter pw = DriverManager.getLogWriter();
//IC see: https://issues.apache.org/jira/browse/DERBY-6945

        if (pw == null) {
            pw = new PrintWriter(System.err, true);
        }

        try {
            new JDBCBoot().boot(Attribute.PROTOCOL, pw);
        }
        catch (Throwable t)
        {
            t.printStackTrace( pw );
            if ( t instanceof RuntimeException ) { throw (RuntimeException) t; }
        }
	}

	/**
		Boot a system requesting a JDBC driver but only if there is
		no current JDBC driver that is handling the required protocol.

        @param protocol The database protocol
        @param logging The diagnostic log writer
	*/
	public void boot(String protocol, final PrintWriter logging) {

        //
        // Synchronization added as part of DERBY-6945. Further improvement
        // on DERBY-4480. Moving the boot method out of EmbeddedDriver
        // somehow created a race condition during simultaneous getConnection() calls.
        //
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        synchronized(NETWORK_SERVER_AUTOSTART_CLASS_NAME)
        {
            if (InternalDriver.activeDriver() == null)
            {

                // request that the InternalDriver (JDBC) service and the
                // authentication service be started.
                //
                addProperty("derby.service.jdbc", InternalDriver.class.getName());
                addProperty("derby.service.authentication", AuthenticationService.MODULE);

//IC see: https://issues.apache.org/jira/browse/DERBY-6648
                boot( bootProperties, logging);
            }
        }
	}
    
    /**
     * Privileged startup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  void    boot( final Properties props, final PrintWriter logging )
    {
        AccessController.doPrivileged
            (
             new PrivilegedAction<Object>()
             {
                 public Object run()
                 {
                     Monitor.startMonitor(props, logging);

                     /* The network server starter module is started differently from other modules because
                      * 1. its start is conditional, depending on a system property, and PropertyUtil.getSystemProperty
                      *    does not work until the Monitor has started,
                      * 2. we do not want the server to try to field requests before Derby has booted, and
                      * 3. if the module fails to start we want to log a message to the error log and continue as
                      *    an embedded database.
                      */
                     if( Boolean.valueOf(PropertyUtil.getSystemProperty(Property.START_DRDA)).booleanValue())
                     {
                         try
                         {
                             Monitor.startSystemModule( NETWORK_SERVER_AUTOSTART_CLASS_NAME);
                         }
                         catch( StandardException se)
                         {
                             Monitor.logTextMessage( MessageId.CONN_NETWORK_SERVER_START_EXCEPTION,
                                                     se.getMessage());
                         }
                     }
                     
                     return null;
                 }
             }
             );
    }
}
