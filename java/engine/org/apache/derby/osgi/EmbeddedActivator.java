/*

   Derby - Class org.apache.derby.osgi.EmbeddedActivator

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.osgi;

import java.sql.DriverManager;
import java.sql.SQLException;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

public class EmbeddedActivator implements BundleActivator {

	public void start(BundleContext context) {
		new org.apache.derby.jdbc.EmbeddedDriver();
	}

	public void stop(BundleContext context) {
		try {
			DriverManager.getConnection("jdbc:derby:;shutdown=true");
		} catch (SQLException sqle) {
		}
	}
}

