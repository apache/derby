/*

   Derby - Class org.apache.derby.jdbc.ClientConnectionPoolDataSource

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
import javax.sql.PooledConnection;
import org.apache.derby.shared.common.i18n.MessageUtil;
import org.apache.derby.shared.common.reference.MessageId;
import org.apache.derby.client.ClientConnectionPoolDataSourceInterface;

/**
 * This datasource is suitable for a client/server use of Derby,
 * running on full Java SE 6 and higher, corresponding to JDBC 4.0 and higher.
 * <p/>
 * ClientConnectionPoolDataSource is a factory for PooledConnection objects.
 * An object that implements this interface
 * will typically be registered with a naming service that is based on the
 * Java Naming and Directory Interface (JNDI).
 */
public class ClientConnectionPoolDataSource extends ClientDataSource 
    implements ClientConnectionPoolDataSourceInterface
{
    private static final long serialVersionUID = -539234282156481377L;
    /** Message utility used to obtain localized messages. */
    private static final MessageUtil msgUtil =
            new MessageUtil(MessageUtil.CLIENT_MESSAGE_RESOURCE_NAME);
    public static final String className__ = "org.apache.derby.jdbc.ClientConnectionPoolDataSource";

    /**
     * Specifies the maximum number of statements that can be cached per
     * connection by the JDBC driver.
     * <p>
     * A value of <code>0</code> disables statement caching, negative values
     * are not allowed. The default is that caching is disabled.
     *
     * @serial
     */
    private int maxStatements = 0;

    public ClientConnectionPoolDataSource() {
        super();
    }

    // ---------------------------interface methods-------------------------------

    // Attempt to establish a physical database connection that can be used as a pooled connection.
    public PooledConnection getPooledConnection() throws SQLException {
        return getPooledConnectionMinion();
    }

    // Standard method that establishes the initial physical connection using CPDS properties.
    public PooledConnection getPooledConnection(String user, String password) throws SQLException {
        return getPooledConnectionMinion(user, password);
    }

    /**
     * Specifies the maximum size of the statement cache.
     *
     * @param maxStatements maximum number of cached statements
     *
     * @throws IllegalArgumentException if <code>maxStatements</code> is
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
     * @return Maximum number of statements to cache, or <code>0</code> if
     *      caching is disabled (default).
     */
    public int getMaxStatements() {
        return this.maxStatements;
    }

    /**
     * Internally used method.
     *
     * @see BasicClientDataSource40#maxStatementsToPool
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
