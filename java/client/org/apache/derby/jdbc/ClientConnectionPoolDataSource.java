/*

   Derby - Class org.apache.derby.jdbc.ClientConnectionPoolDataSource

   Copyright (c) 2001, 2005 The Apache Software Foundation or its licensors, where applicable.

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

package org.apache.derby.jdbc;

import org.apache.derby.client.ClientPooledConnection;

/**
 * ClientConnectionPoolDataSource is a factory for PooledConnection objects. An object that implements this interface
 * will typically be registered with a naming service that is based on the Java Naming and Directory Interface (JNDI).
 */
public class ClientConnectionPoolDataSource extends ClientDataSource implements javax.sql.ConnectionPoolDataSource,
        java.io.Serializable,
        javax.naming.Referenceable {
    static final long serialVersionUID = -539234282156481377L;
    public static final String className__ = "org.apache.derby.jdbc.ClientConnectionPoolDataSource";

    private String password = null;

    synchronized public void setPassword(String password) {
        this.password = password;
    }

    public final static String propertyKey_password = "password";

    // deprecated.  do not use.  this member remains only for serial compatibility.
    // this member should never be used.  pre-empted by super.traceFileSuffixIndex_.
    // private int traceFileSuffixIndex = 0;

    public ClientConnectionPoolDataSource() {
        super();
    }

    // ---------------------------interface methods-------------------------------

    // Attempt to establish a physical database connection that can be used as a pooled connection.
    public javax.sql.PooledConnection getPooledConnection() throws java.sql.SQLException {
        org.apache.derby.client.am.LogWriter dncLogWriter = super.computeDncLogWriterForNewConnection("_cpds");
        if (dncLogWriter != null) {
            dncLogWriter.traceEntry(this, "getPooledConnection");
        }
        javax.sql.PooledConnection pooledConnection = getPooledConnectionX(dncLogWriter, this, this.user, this.password);
        if (dncLogWriter != null) {
            dncLogWriter.traceExit(this, "getPooledConnection", pooledConnection);
        }
        return pooledConnection;
    }

    // Standard method that establishes the initial physical connection using CPDS properties.
    public javax.sql.PooledConnection getPooledConnection(String user,
                                                          String password) throws java.sql.SQLException {
        org.apache.derby.client.am.LogWriter dncLogWriter = super.computeDncLogWriterForNewConnection("_cpds");
        if (dncLogWriter != null) {
            dncLogWriter.traceEntry(this, "getPooledConnection", user, "<escaped>");
        }
        javax.sql.PooledConnection pooledConnection = getPooledConnectionX(dncLogWriter, this, user, password);
        if (dncLogWriter != null) {
            dncLogWriter.traceExit(this, "getPooledConnection", pooledConnection);
        }
        return pooledConnection;
    }

    //  method that establishes the initial physical connection using DS properties instead of CPDS properties.
    public javax.sql.PooledConnection getPooledConnection(ClientDataSource ds,
                                                          String user,
                                                          String password) throws java.sql.SQLException {
        org.apache.derby.client.am.LogWriter dncLogWriter = ds.computeDncLogWriterForNewConnection("_cpds");
        if (dncLogWriter != null) {
            dncLogWriter.traceEntry(this, "getPooledConnection", ds, user, "<escaped>");
        }
        javax.sql.PooledConnection pooledConnection = getPooledConnectionX(dncLogWriter, ds, user, password);
        if (dncLogWriter != null) {
            dncLogWriter.traceExit(this, "getPooledConnection", pooledConnection);
        }
        return pooledConnection;
    }

    //  method that establishes the initial physical connection
    // using DS properties instead of CPDS properties.
    private javax.sql.PooledConnection getPooledConnectionX(org.apache.derby.client.am.LogWriter dncLogWriter,
                                                            ClientDataSource ds,
                                                            String user,
                                                            String password) throws java.sql.SQLException {
        return new ClientPooledConnection(ds, dncLogWriter, user, password);
    }
}
