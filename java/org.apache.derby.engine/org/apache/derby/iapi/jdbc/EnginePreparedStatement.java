/*
 
 Derby - Class org.apache.derby.iapi.jdbc.EnginePreparedStatement
 
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
import java.sql.PreparedStatement;

/**
 * Additional methods the embedded engine exposes on its 
 * PreparedStatement object implementations. An internal api only, mainly 
 * for the network server. Allows consistent interaction between embedded 
 * PreparedStatement and Brokered PreparedStatements.
 * (DERBY-1015)
 */
public interface EnginePreparedStatement extends PreparedStatement, EngineStatement {
    
    /**
     * Get the version of the prepared statement. If this has not been changed,
     * the caller may assume that a recompilation has not taken place, i.e.
     * meta-data are (also) unchanged.
     * @return version counter
     * @throws SQLException on error
     */
    public long getVersionCounter() throws SQLException;
    
    ////////////////////////////////////////////////////////////////////
    //
    // INTRODUCED BY JDBC 4.2 IN JAVA 8
    //
    ////////////////////////////////////////////////////////////////////

    public long executeLargeUpdate() throws SQLException;
    
}
