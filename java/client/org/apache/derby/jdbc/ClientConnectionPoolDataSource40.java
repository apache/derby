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
 * ClientConnectionPoolDataSource40 is a factory for PooledConnection objects.
 * An object that implements this interface
 * will typically be registered with a naming service that is based on the
 * Java Naming and Directory Interface (JNDI). Use this factory
 * if your application runs at JDBC level 4.1 (or higher). Use
 * ClientConnectionPoolDataSource, instead, if your application runs under
 * JDBC 4.0 or JDBC 3.0, that is, on the following Java Virtual Machines:
 * <p/>
 * <UL>
 * <LI> JDBC 4.0 - Java SE 6
 * <LI> JDBC 3.0 - J2SE 5.0
 * </UL>
 */
public class ClientConnectionPoolDataSource40
        extends ClientConnectionPoolDataSource {
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
