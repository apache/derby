/*

   Derby - Class org.apache.derby.iapi.jdbc.BrokeredStatement40

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

public class BrokeredStatement40 extends BrokeredStatement {
    
    /**
     * calls the superclass constructor to pass the parameters
     *
     * @param control   BrokeredStatementControl
     * @throws java.sql.SQLException
     *
     */
    
    BrokeredStatement40(BrokeredStatementControl control) 
                                                    throws SQLException {
        super(control);
    }

    /** 
     * Forwards to the real Statement.
     * @return true if the underlying Statement is poolable, false otherwise.
     * @throws SQLException if the forwarding call fails.
     */
    public boolean isPoolable() throws SQLException {
        return getStatement().isPoolable();
    }

    /** 
     * Forwards to the real Statement.
     * @param poolable the new value for the poolable hint.
     * @throws SQLException if the forwarding call fails.
     */
    public void setPoolable(boolean poolable) throws SQLException {
        getStatement().setPoolable(poolable);
    }
}
