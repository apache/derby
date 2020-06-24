/*

   Derby - Class org.apache.derby.client.ClientConnectionPoolDataSourceInterface

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

package org.apache.derby.client;

import javax.sql.ConnectionPoolDataSource;

/**
 * Specifies Derby extensions to the {@code java.sqlx.ConnectionPoolDataSource}.
 */
public interface ClientConnectionPoolDataSourceInterface
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    extends ClientDataSourceInterface, ConnectionPoolDataSource {
    /**
     * Returns the maximum number of JDBC prepared statements a connection is
     * allowed to cache.
     *
     * @return Maximum number of statements to cache, or {@code 0} if
     *      caching is disabled (default).
     */
    public int getMaxStatements();

    /**
     * Specifies the maximum size of the statement cache.
     *
     * @param maxStatements maximum number of cached statements
     *
     * @throws IllegalArgumentException if {@code maxStatements} is
     *      negative
     */
    public void setMaxStatements(int maxStatements);

}
