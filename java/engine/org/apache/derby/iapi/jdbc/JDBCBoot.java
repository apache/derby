/*

   Derby - Class org.apache.derby.iapi.jdbc.JDBCBoot

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

package org.apache.derby.iapi.jdbc;

import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.iapi.jdbc.AuthenticationService;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.services.monitor.Monitor;

import java.sql.DriverManager;
import java.sql.SQLException;

import java.util.Properties;
import java.io.PrintStream;

/**
	A class to boot a cloudscape system that includes a JDBC driver.
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

	/**
		Boot a system requesting a JDBC driver but only if there is
		no current JDBC driver that is handling the required protocol.

	*/
	public void boot(String protocol, PrintStream logging) {

		try {

			// throws a SQLException if there is no driver
			DriverManager.getDriver(protocol);

		} catch (SQLException sqle) {

			// request that the java.sql.Driver (JDBC) service and the
			// authentication service be started.
			//
			addProperty("derby.service.jdbc", "org.apache.derby.jdbc.Driver169");
			addProperty("derby.service.authentication", AuthenticationService.MODULE);

			Monitor.startMonitor(bootProperties, logging);

            /* The network server starter module is started differently from other modules because
             * 1. its start is conditional, depending on a system property, and PropertyUtil.getSystemProperty
             *    does not work until the Monitor has started,
             * 2. we do not want the server to try to field requests before Cloudscape has booted, and
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
		}
	}
}
