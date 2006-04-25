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

import java.sql.SQLException;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.PooledConnection;
import org.apache.derby.client.am.LogWriter;
import org.apache.derby.client.am.SqlException;

/**
 * ClientConnectionPoolDataSource is a factory for PooledConnection objects. An object that implements this interface
 * will typically be registered with a naming service that is based on the Java Naming and Directory Interface (JNDI).
 */
public class ClientConnectionPoolDataSource extends ClientBaseDataSource 
                                           implements ConnectionPoolDataSource {
    private static final long serialVersionUID = -539234282156481377L;
    public static final String className__ = "org.apache.derby.jdbc.ClientConnectionPoolDataSource";

    public ClientConnectionPoolDataSource() {
        super();
    }

    // ---------------------------interface methods-------------------------------

    // Attempt to establish a physical database connection that can be used as a pooled connection.
    public PooledConnection getPooledConnection() throws SQLException {
        try
        {
            LogWriter dncLogWriter = super.computeDncLogWriterForNewConnection("_cpds");
            if (dncLogWriter != null) {
                dncLogWriter.traceEntry(this, "getPooledConnection");
            }
            PooledConnection pooledConnection = getPooledConnectionX(dncLogWriter, this, getUser(), getPassword());
            if (dncLogWriter != null) {
                dncLogWriter.traceExit(this, "getPooledConnection", pooledConnection);
            }
            return pooledConnection;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // Standard method that establishes the initial physical connection using CPDS properties.
    public PooledConnection getPooledConnection(String user, String password) throws SQLException {
        try
        {
            LogWriter dncLogWriter = super.computeDncLogWriterForNewConnection("_cpds");
            if (dncLogWriter != null) {
                dncLogWriter.traceEntry(this, "getPooledConnection", user, "<escaped>");
            }
            PooledConnection pooledConnection = getPooledConnectionX(dncLogWriter, this, user, password);
            if (dncLogWriter != null) {
                dncLogWriter.traceExit(this, "getPooledConnection", pooledConnection);
            }
            return pooledConnection;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    //  method that establishes the initial physical connection
    // using DS properties instead of CPDS properties.
    private PooledConnection getPooledConnectionX(LogWriter dncLogWriter, 
                        ClientBaseDataSource ds, String user, 
                        String password) throws SQLException {
            return ClientDriver.getFactory().newClientPooledConnection(ds,
                    dncLogWriter, user, password);
    }   
}
