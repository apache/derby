/*

   Derby - Class org.apache.derby.client.am.Utils42

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

package org.apache.derby.client.am;

import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLType;

import org.apache.derby.shared.common.reference.SQLState;

/**
 * <p>
 * Utility methods for JDBC 4.2.
 * </p>
 */
public final class Utils42
{
    /**
     * <p>
     * Get the int type id from java.sql.Types which corresponds to the SQLType.
     * </p>
     *
     * @param agent The agent
     * @param sqlType The SQLType to map
     *
     * @return the corresponding type id
     * @throws SQLException on error
     */
    public  static  int getTypeAsInt( Agent agent, SQLType sqlType )
        throws SQLException
    {
        // must correspond to something in java.sql.Types
        if ( sqlType instanceof JDBCType )
        {
            int     jdbcType = ((JDBCType) sqlType).getVendorTypeNumber();

            try {
                agent.checkForSupportedDataType( jdbcType );
            } catch (SqlException se) { throw se.getSQLException(); }
        
            return jdbcType;
        }

        throw new SqlException
            (
             agent.logWriter_,
             new ClientMessageId(SQLState.DATA_TYPE_NOT_SUPPORTED),
             sqlType
             ).getSQLException();
    }

}
