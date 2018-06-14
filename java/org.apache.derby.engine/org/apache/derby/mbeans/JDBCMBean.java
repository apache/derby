/*

   Derby - Class org.apache.derby.mbeans.JDBCMBean

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

package org.apache.derby.mbeans;

import java.sql.SQLException;

/** 
 * Management and information for the embedded JDBC driver.
 * <P>
 * Key properties for registered MBean:
 * <UL>
 * <LI> <code>type=JDBC</code>
 * <LI> <code>system=</code><em>runtime system identifier</em> (see overview)
 * </UL>
*/
public interface JDBCMBean {
    
    /**
     * Get the JDBC driver's implementation level
     *
     * @return the driver level
     */
    public String getDriverLevel();

    /**
     * Return the JDBC driver's major version.
     * @return major version
     * @see java.sql.Driver#getMajorVersion()
     */
    public int getMajorVersion();
    
    /**
     * Return the JDBC driver's minor version.
     * @return minor version
     * @see java.sql.Driver#getMinorVersion()
     */
    public int getMinorVersion();
    
    /**
     * Is the JDBC driver compliant.
     * @return compliance state
     * @see java.sql.Driver#jdbcCompliant()
     */
    public boolean isCompliantDriver();
    
    /**
     * Does the driver accept the passed in JDBC URL
     * @param url JDBC URL to check.
     * @return True if it supports it, false otherwise.
     * @throws SQLException on error
     * @see java.sql.Driver#acceptsURL(String)
     */
    public boolean acceptsURL(String url) throws SQLException;

}
