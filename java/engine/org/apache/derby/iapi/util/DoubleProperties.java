/*

   Licensed Materials - Property of IBM
   Cloudscape - Package com.ihost.cs
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.util;

import java.util.Properties;
import java.util.Enumeration;

/**
	A properties object that links two independent
	properties together. The read property set is always
	searched first, with the write property set being
	second. But any put() calls are always made directly to
	the write object.

    Only the put(), keys() and getProperty() methods are supported
	by this class.
*/

public final class DoubleProperties extends Properties {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;

	private final Properties read;
	private final Properties write;

	public DoubleProperties(Properties read, Properties write) {
		this.read = read;
		this.write = write;
	}

	public Object put(Object key, Object value) {
		return write.put(key, value);
	}

	public String getProperty(String key) {

		return read.getProperty(key, write.getProperty(key));
	}

	public String getProperty(String key, String defaultValue) {
		return read.getProperty(key, write.getProperty(key, defaultValue));

	}

	public Enumeration propertyNames() {

		Properties p = new Properties();

		if (write != null) {

			for (Enumeration e = write.propertyNames(); e.hasMoreElements(); ) {
				String key = (String) e.nextElement();
				p.put(key, write.getProperty(key));
			}
		}

		if (read != null) {
			for (Enumeration e = read.propertyNames(); e.hasMoreElements(); ) {
				String key = (String) e.nextElement();
				p.put(key, read.getProperty(key));
			}
		}
		return p.keys();
	}
}
