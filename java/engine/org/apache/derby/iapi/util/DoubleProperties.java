/*

   Derby - Class com.ihost.cs.DoubleProperties

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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
