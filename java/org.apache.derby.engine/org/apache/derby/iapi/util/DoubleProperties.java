/*

   Derby - Class org.apache.derby.iapi.util.DoubleProperties

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

package org.apache.derby.iapi.util;

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Properties;

/**
	A properties object that links two independent
	properties together. The read property set is always
	searched first, with the write property set being
	second. But any put() calls are always made directly to
	the write object.

//IC see: https://issues.apache.org/jira/browse/DERBY-5830
//IC see: https://issues.apache.org/jira/browse/DERBY-4269
    Only the put(), propertyNames() and getProperty() methods are supported
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

	public Enumeration<Object> propertyNames() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        HashSet<Object> names = new HashSet<Object>();
//IC see: https://issues.apache.org/jira/browse/DERBY-5830
//IC see: https://issues.apache.org/jira/browse/DERBY-4269
        addAllNames(write, names);
        addAllNames(read, names);
        return Collections.enumeration(names);
	}

    /**
     * Add all property names in the Properties object {@code src} to the
     * HashSet {@code dest}.
     */
    private static void addAllNames(Properties src, HashSet<Object> dest) {
        if (src != null) {
            for (Enumeration e = src.propertyNames(); e.hasMoreElements(); ) {
                dest.add(e.nextElement());
            }
        }
    }
}
