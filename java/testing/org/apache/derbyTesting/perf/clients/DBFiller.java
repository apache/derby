/*

Derby - Class org.apache.derbyTesting.perf.clients.DBFiller

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

package org.apache.derbyTesting.perf.clients;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Interface for classes that populate a database for a certain test.
 */
public interface DBFiller {
    /**
     * Populate the database with the data needed by a test.
     *
     * @param c the connection to use
     * @throws SQLException if a database error occurs
     */
    void fill(Connection c) throws SQLException;
}
