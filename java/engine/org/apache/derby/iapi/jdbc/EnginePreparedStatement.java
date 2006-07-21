/*
 
 Derby - Class org.apache.derby.iapi.jdbc.EnginePreparedStatement
 
 Copyright 2006 The Apache Software Foundation or its licensors, as applicable.
 
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
public interface EnginePreparedStatement extends PreparedStatement {
    
    /**
     * Imitate the getParameterMetaData() that is in JDBC 3.0
     * Once,JDK1.3 stops being supported, instead of returning EngineParameterMetaData
     * the JDBC 3.0 class - ParameterMetaData can be used.
     *
     * Retrieves the number, types and properties of this PreparedStatement
     * object's parameters.
     *
     * @return a EngineParameterMetaData object that contains information about the
     * number, types and properties of this PreparedStatement object's parameters.
     * @exception SQLException if a database access error occurs
     */
    public EngineParameterMetaData getEmbedParameterSetMetaData()
        throws SQLException;
    
}
