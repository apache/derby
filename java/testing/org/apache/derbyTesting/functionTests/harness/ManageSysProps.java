/* 

   Derby - Class org.apache.derbyTesting.functionTests.harness.ManageSysProps

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.harness;

import java.util.Enumeration;
import java.util.Properties;

/*
 **
 **	Keeps a copy of the system properties saved at a critical early
 **	point during the running of the test harness.  Uses this copy
 **	to create new copies which can then be mussed up and thrown
 **	away, as needed.
 */

public class ManageSysProps
{

	private static Properties savedSysProps = null;

	public static void saveSysProps() {
		Properties sp = System.getProperties();
		savedSysProps = new Properties();
		String key = null;
		for (Enumeration e = sp.propertyNames(); e.hasMoreElements();) {
			key = (String)e.nextElement();
			savedSysProps.put(key, sp.getProperty(key));
		}
	}

	// reset the system properties to prevent confusion
	// when running with java threads
	public static void resetSysProps() {
		String key = null;
		Properties nup = new Properties();
		for (Enumeration e = savedSysProps.propertyNames(); e.hasMoreElements();) {
			key = (String)e.nextElement();
			nup.put(key, savedSysProps.getProperty(key));
		}
		System.setProperties(nup);
	}
}
