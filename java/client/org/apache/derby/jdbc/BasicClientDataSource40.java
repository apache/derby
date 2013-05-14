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

import javax.sql.DataSource;

/**
 * This datasource is suitable for client/server use of Derby,
 * running on Java 8 Compact Profile 2 or higher.
 * <p/>
 * BasicClientDataSource40 is similar to ClientDataSource except it
 * can not be used with JNDI, i.e. it does not implement
 * {@code javax.naming.Referenceable}.
 */
public class BasicClientDataSource40
    extends ClientBaseDataSourceRoot implements DataSource {

    private final static long serialVersionUID = 1894299584216955554L;
    public final static String className__ =
            "org.apache.derby.jdbc.BasicClientDataSource40";

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
