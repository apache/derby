/*

   Derby - Class org.apache.derby.jdbc.BasicClientDataSource40

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

package org.apache.derby.jdbc;

import org.apache.derby.client.BasicClientDataSource;

/**
 * This data source is suitable for client/server use of Derby,
 * running on Java 8 Compact Profile 2 or higher.
 * <p/>
 * BasicClientDataSource40 is similar to ClientDataSource except it
 * can not be used with JNDI, i.e. it does not implement
 * {@code javax.naming.Referenceable}.
 * <p/>
 *  * The standard attributes provided are, cf. e.g. table
 * 9.1 in the JDBC 4.2 specification.
 * <ul>
 *   <li>databaseName</li>
 *   <li>dataSourceName</li>
 *   <li>description</li>
 *   <li>password</li>
 *   <li>user</li>
 * </ul>
 * These standard attributes are not supported:
 * <ul>
 *   <li>networkProtocol</li>
 *   <li>roleName</li>
 * </ul>
 * The Derby client driver also supports these attributes:
 * <ul>
 *   <li>loginTimeout</li> @see javax.sql.CommonDataSource set/get
 *   <li>logWriter</li> @see javax.sql.CommonDataSource set/get
 *   <li>createDatabase</li>
 *   <li>connectionAttributes</li>
 *   <li>shutdownDatabase</li>
 *   <li>attributesAsPassword</li>
 *   <li>retrieveMessageText</li>
 *   <li>securityMechanism</li>
 *   <li>traceDirectory</li>
 *   <li>traceFile</li>
 *   <li>traceFileAppend</li>
 *   <li>traceLevel<li>
 * </ul>
 */
public class BasicClientDataSource40 extends BasicClientDataSource
{
    private final static long serialVersionUID = 1894299584216955554L;

    /**
     * Creates a simple DERBY data source with default property values
     * for a non-pooling, non-distributed environment.  No particular
     * DatabaseName or other properties are associated with the data
     * source.
     * <p/>
     * Every Java Bean should provide a constructor with no arguments
     * since many beanboxes attempt to instantiate a bean by invoking
     * its no-argument constructor.
     */
    public BasicClientDataSource40() {
        super();
    }
}

