/*

   Derby - Class org.apache.derby.jdbc.BasicClientConnectionPoolDataSource40

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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.SQLException;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;
import org.apache.derby.shared.common.i18n.MessageUtil;
import org.apache.derby.shared.common.reference.MessageId;

/**
 * This data source is suitable for client/server use of Derby,
 * running on Java 8 Compact Profile 2 or higher.
 * <p/>
 * BasicClientConnectionPoolDataSource40 is similar to
 * ClientConnectionPoolDataSource except that it does not support JNDI,
 * i.e. it does not implement {@code javax.naming.Referenceable}.
 *
 * @see ClientConnectionPoolDataSource40
 */
public class BasicClientConnectionPoolDataSource40
        extends BasicClientDataSource40
        implements ConnectionPoolDataSource,
                   ClientConnectionPoolDataSourceInterface {

    private static final long serialVersionUID = -539234282156481378L;

    /** Message utility used to obtain localized messages. */
    private static final MessageUtil msgUtil =
            new MessageUtil("org.apache.derby.loc.clientmessages");

    public static final String className__ =
            "org.apache.derby.jdbc.BasicClientConnectionPoolDataSource40";

    /**
     * Specifies the maximum number of statements that can be cached per
     * connection by the JDBC driver.
     * <p>
     * A value of {@code 0} disables statement caching, negative values
     * are not allowed. The default is that caching is disabled.
     *
     * @serial
     */
    private int maxStatements = 0;

    public BasicClientConnectionPoolDataSource40() {
        super();
    }

    // ---------------------------interface methods----------------------------

    /**
     * @see javax.sql.ConnectionPoolDataSource#getPooledConnection()
     */
    public PooledConnection getPooledConnection() throws SQLException {
        return getPooledConnectionMinion();
    }

    /**
     * @see javax.sql.ConnectionPoolDataSource#getPooledConnection(
     *      java.lang.String, java.lang.String)
     */
    public PooledConnection getPooledConnection(String user, String password)
            throws SQLException {

        return getPooledConnectionMinion(user, password);
    }


    /**
     * Specifies the maximum size of the statement cache.
     *
     * @param maxStatements maximum number of cached statements
     *
     * @throws IllegalArgumentException if {@code maxStatements} is
     *      negative
     */
    public void setMaxStatements(int maxStatements) {
        // Disallow negative values.
        if (maxStatements < 0) {
            throw new IllegalArgumentException(msgUtil.getTextMessage(
                    MessageId.CONN_NEGATIVE_MAXSTATEMENTS, maxStatements));
        }

        this.maxStatements = maxStatements;
    }

    /**
     * Returns the maximum number of JDBC prepared statements a connection is
     * allowed to cache.
     *
     * @return Maximum number of statements to cache, or {@code 0} if
     *      caching is disabled (default).
     */
    public int getMaxStatements() {
        return this.maxStatements;
    }

    /**
     * Internally used method.
     *
     * @see ClientBaseDataSourceRoot#maxStatementsToPool
     */
    public int maxStatementsToPool() {
        return this.maxStatements;
    }

    /**
     * Make sure the state of the de-serialized object is valid.
     */
    private final void validateState() {
        // Make sure maxStatements is zero or higher.
        if (maxStatements < 0) {
            throw new IllegalArgumentException(msgUtil.getTextMessage(
                    MessageId.CONN_NEGATIVE_MAXSTATEMENTS, maxStatements));
        }
    }

    /**
     * Read an object from the ObjectInputStream.
     * <p>
     * This implementation differs from the default one by initiating state
     * validation of the object created.
     *
     * @param inputStream data stream to read objects from
     * @throws ClassNotFoundException if instantiating a class fails
     * @throws IOException if reading from the stream fails
     */
    private void readObject(ObjectInputStream inputStream)
            throws ClassNotFoundException, IOException {
        // Always perform the default de-serialization first
        inputStream.defaultReadObject();

        // Ensure that object state has not been corrupted or tampered with.
        validateState();
    }
}
