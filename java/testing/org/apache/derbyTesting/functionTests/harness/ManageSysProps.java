/* 

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.harness
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
