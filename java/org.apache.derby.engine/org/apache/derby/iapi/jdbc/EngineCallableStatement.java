/*

   Derby - Class org.apache.derby.iapi.jdbc.EngineCallableStatement

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

import java.sql.CallableStatement;
import java.sql.SQLException;

/**
 * Additional methods the engine exposes on its CallableStatement object
 * implementations, whose signatures are not compatible with older platforms.
 */
public interface EngineCallableStatement
        extends EngineStatement, CallableStatement {
    // JDBC 4.1 methods that use generics and won't compile on CDC.
    <T> T getObject(int parameterIndex, Class<T> type) throws SQLException;
    <T> T getObject(String parameterName, Class<T> type) throws SQLException;
}
