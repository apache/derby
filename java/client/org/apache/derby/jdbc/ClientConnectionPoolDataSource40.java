/*

   Derby - Class org.apache.derby.jdbc.ClientConnectionPoolDataSource40

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

import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;
import org.apache.derby.client.am.ClientMessageId;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.shared.common.reference.SQLState;

/**
 * This datasource is suitable for a client/server use of Derby,
 * running on the following platforms:
 * <p>
 * <ul>
 *   <li>Java SE 7 (JDBC 4.1) and
 *   <li>full Java SE 8 (JDBC 4.2).
 * </ul>
 * <p>
 * Use BasicClientConnectionPoolDataSource40 if your application
 * runs on Java 8 Compact Profile 2.
 * <p>
 * Use ClientConnectionPoolDataSource if your application
 * runs on the following platforms:
 * <p>
 * <ul>
 *   <li> JDBC 4.0 - Java SE 6
 *   <li> JDBC 3.0 - J2SE 5.0
 * </ul>
 * <p>
 * ClientConnectionPoolDataSource40 is a factory for PooledConnection objects.
 * An object that implements this interface
 * will typically be registered with a naming service that is based on the
 * Java Naming and Directory Interface (JNDI).
 */
public class ClientConnectionPoolDataSource40
    extends ClientConnectionPoolDataSource
    implements javax.sql.ConnectionPoolDataSource /* compile-time
                                                   * check for 4.1
                                                   * extension */
{
   private static final long serialVersionUID = 6313966728809326579L;

    ////////////////////////////////////////////////////////////////////
    //
    // INTRODUCED BY JDBC 4.1 IN JAVA 7
    //
    ////////////////////////////////////////////////////////////////////

    public  Logger getParentLogger()
        throws SQLFeatureNotSupportedException
    {
        throw (SQLFeatureNotSupportedException)
            (
             new SqlException( null, new ClientMessageId(SQLState.NOT_IMPLEMENTED), "getParentLogger" )
             ).getSQLException();
    }
    
}
