/*
 
   Derby - Class org.apache.derby.impl.jdbc.Util42
 
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 
 */

package org.apache.derby.impl.jdbc;

import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Types;

import org.apache.derby.iapi.reference.SQLState;

/**
 * <p>
 * Utility methods for JDBC 4.2
 * </p>
 */
public class Util42
{    
    /**
     * <p>
     * Get the int type id from java.sql.Types which corresponds to the SQLType.
     * </p>
     */
    public  static  int getTypeAsInt( ConnectionChild connChild, SQLType sqlType )
        throws SQLException
    {
        // must correspond to something in java.sql.Types
        if ( sqlType instanceof JDBCType )
        {
            int     jdbcType = ((JDBCType) sqlType).getVendorTypeNumber();
            
            connChild.checkForSupportedDataType( jdbcType );
        
            return jdbcType;
        }

        throw connChild.newSQLException
            ( SQLState.DATA_TYPE_NOT_SUPPORTED, sqlType );
    }
    
}    

