/*

   Derby - Class org.apache.derby.jdbc.ClientConnectionPoolDataSource40

   Copyright (c) 2006 The Apache Software Foundation or its licensors, where applicable.

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

import java.sql.BaseQuery;
import java.sql.QueryObjectGenerator;
import java.sql.SQLException;
import javax.sql.ConnectionPoolDataSource;
import org.apache.derby.client.am.SQLExceptionFactory;

/**
 * ClientConnectionPoolDataSource40 is a factory for PooledConnection objects.
 * An object that implements this interface
 * will typically be registered with a naming service that is based on the
 * Java Naming and Directory Interface (JNDI). Use this factory
 * if your application runs under JDBC4.0.
 * Use
 * ClientConnectionPoolDataSource, instead, if your application runs under
 * JDBC3.0 or JDBC2.0, that is, on the following Java Virtual Machines:
 * <p/>
 * <UL>
 * <LI> JDBC 3.0 - Java 2 - JDK 1.4, J2SE 5.0
 * <LI> JDBC 2.0 - Java 2 - JDK 1.2,1.3
 * </UL>
 */
public class ClientConnectionPoolDataSource40
        extends ClientConnectionPoolDataSource {
    /**
     * Retrieves the QueryObjectGenerator for the given JDBC driver.  If the
     * JDBC driver does not provide its own QueryObjectGenerator, NULL is
     * returned.
     *
     * @return The QueryObjectGenerator for this JDBC Driver or NULL if the 
     * driver does not provide its own implementation
     * @exception SQLException if a database access error occurs
     */
    public QueryObjectGenerator getQueryObjectGenerator() throws SQLException {
        return null;
    }    
}
