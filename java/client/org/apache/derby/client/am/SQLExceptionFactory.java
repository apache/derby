/*

   Derby - Class org.apache.derby.client.am.SQLExceptionFactory

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

import java.sql.SQLException;
import org.apache.derby.shared.common.reference.SQLState;

/**
 * class to create SQLException
 */

public class SQLExceptionFactory {     
     
    public static SQLException notImplemented (String feature) {
        SqlException sqlException = new SqlException (null, 
                new ClientMessageId (SQLState.NOT_IMPLEMENTED), feature);
        return sqlException.getSQLException();
    }
    
    /**
     * creates SQLException initialized with all the params received from the 
     * caller. This method will be overwritten to support jdbc version specific 
     * exception class.
     */
    public SQLException getSQLException (String message, String sqlState, 
            int errCode) {
        return new SQLException (message, sqlState, errCode);           
    }    
}
 
