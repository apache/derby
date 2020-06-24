/*
 
   Derby - Class org.apache.derby.iapi.jdbc.BrokeredConnection42
 
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

package org.apache.derby.iapi.jdbc;

import java.sql.SQLException;

public class BrokeredConnection42 extends BrokeredConnection
{
    /**
     * Creates a new instance of BrokeredConnection40
     *
     * @param control The control variable
     *
     * @throws SQLException on error
     */
    public BrokeredConnection42(BrokeredConnectionControl control)
            throws SQLException {
        super(control);
    }
    
    public final BrokeredPreparedStatement newBrokeredStatement
        ( BrokeredStatementControl statementControl, String sql, Object generatedKeys )
        throws SQLException
    {
        try {
            return new BrokeredPreparedStatement42( statementControl, sql, generatedKeys );
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }
    
    public BrokeredCallableStatement newBrokeredStatement(BrokeredStatementControl statementControl, String sql) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-6000
        try {
            return new BrokeredCallableStatement42(statementControl, sql);
        } catch (SQLException sqle) {
            notifyException(sqle);
            throw sqle;
        }
    }
}
