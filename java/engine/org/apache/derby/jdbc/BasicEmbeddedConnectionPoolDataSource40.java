/*

   Derby - Class org.apache.derby.jdbc.BasicEmbeddedConnectionPoolDataSource40

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

import java.sql.SQLException;
import javax.sql.PooledConnection;

/**
 * This data source is suitable for an application using embedded Derby,
 * running on Java 8 Compact Profile 2 or higher.
 * <p/>
 * BasicEmbeddedConnectionPoolDataSource40 is similar to
 * EmbeddedConnectionPoolDataSource40 except it does not support JNDI naming,
 * i.e. it does not implement {@code javax.naming.Referenceable}.
 *
 * @see EmbeddedConnectionPoolDataSource40
 */
public class BasicEmbeddedConnectionPoolDataSource40
    extends BasicEmbeddedDataSource40
    implements javax.sql.ConnectionPoolDataSource,
        EmbeddedConnectionPoolDataSourceInterface {

    private static final long serialVersionUID = 7852784308039674161L;

    /**
     *  No-argument constructor.
     */
    public BasicEmbeddedConnectionPoolDataSource40() {
        super();
    }

    /*
     * Implementation of ConnectionPoolDataSource interface methods
     */

    @Override
    public final PooledConnection getPooledConnection() throws SQLException {
        return createPooledConnection (getUser(), getPassword(), false);
    }

    @Override
    public final PooledConnection getPooledConnection(
            String username,
            String password) throws SQLException {

        return createPooledConnection (username, password, true);
    }

    /**
     * Minion helper method. Create and return a pooled connection
     *
     * @param user the user name used to authenticate the connection
     * @param password the user's password
     * @param requestPassword {@code false} if original call is from a
     *        no-argument constructor, otherwise {@code true}
     *
     * @return a connection to the database
     * @throws SQLException if a database-access error occurs
     */
    private PooledConnection createPooledConnection (
            String user,
            String password,
            boolean requestPassword) throws SQLException {

        findDriver();
        return new EmbedPooledConnection(this, user, password, requestPassword);
    }
}
