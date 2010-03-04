/*

Derby - Class org.apache.derbyTesting.perf.clients.Client

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

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Interface that must be implemented by performance clients. The
 * implementations of this interface normally perform a single operation, and
 * the selected implementation of {@code LoadGenerator} repeatedly invokes this
 * operation in order to get the desired distribution.
 */
public interface Client {
    /**
     * Initialize this client (typically prepare the statements needed in the
     * {@code doWork()} method).
     *
     * @param c a connection which can be used by this client
     * @throws SQLException if a database error occurs
     */
    void init(Connection c) throws SQLException;

    /**
     * Perform the work for a single iteration of the test (typically a single
     * transaction).
     *
     * @throws SQLException if a database error occurs
     */
    void doWork() throws SQLException;
    
    /**
     * Print a report from the test run.
     *
     * @param out stream to print the report to
     */
    void printReport(PrintStream out);
}
