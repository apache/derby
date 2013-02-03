/*

   Derby - Class org.apache.derby.jdbc.ClientXADataSource40

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
 * Use BasicClientXADataSource40 if your application runs on Java 8
 * Compact Profile 2.
 * <p>
 * Use ClientXADataSource if your application runs on the following
 * platforms:
 * <p>
 * <ul>
 *   <li> JDBC 4.0 - Java SE 6
 *   <li> JDBC 3.0 - J2SE 5.0
 * </ul>
 * <p>
 * An XADataSource is a factory for XAConnection objects.  It represents a
 * RM in a DTP environment.  An object that implements the XADataSource
 * interface is typically registered with a JNDI service provider.
 * <P>
 * <P>ClientXADataSource40 is serializable and referenceable.</p>
 *
 * <P>See ClientDataSource40 for DataSource properties.</p>
 */
public class ClientXADataSource40 extends ClientXADataSource
    implements javax.sql.XADataSource /* compile-time check for 4.1 extension */
{
   private static final long serialVersionUID = -3463444509507830926L;

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
