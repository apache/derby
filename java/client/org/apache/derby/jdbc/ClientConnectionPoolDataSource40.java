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
 * ConnectionPoolDataSource for jdbc4.0
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
