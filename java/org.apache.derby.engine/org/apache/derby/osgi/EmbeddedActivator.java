/*

   Derby - Class org.apache.derby.osgi.EmbeddedActivator

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

package org.apache.derby.osgi;

import java.sql.DriverManager;
import java.sql.SQLException;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

public final class EmbeddedActivator implements BundleActivator {

	public void start(BundleContext context) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
		new org.apache.derby.iapi.jdbc.AutoloadedDriver();
	}

	public void stop(BundleContext context) {
		try {
			DriverManager.getConnection("jdbc:derby:;shutdown=true");
		} catch (SQLException sqle) {
		}
	}
}

