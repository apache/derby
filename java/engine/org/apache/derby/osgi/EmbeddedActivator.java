/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.osgi
   (C) Copyright IBM Corp. 2003, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.osgi;

import java.sql.DriverManager;
import java.sql.SQLException;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

public class EmbeddedActivator implements BundleActivator {

	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2003_2004;

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

