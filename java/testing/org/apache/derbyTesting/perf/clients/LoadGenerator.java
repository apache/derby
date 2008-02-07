/*

Derby - Class org.apache.derbyTesting.perf.clients.LoadGenerator

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

/**
 * Interface implemented by load generators. A load generator generates the
 * test load on the DBMS by invoking the clients' {@code doWork()} methods.
 * Different load generators may generate load with different characteristics.
 * The actual database operations performed are decided by the clients passed
 * in to the load generator's {@code init()} method.
 */
public interface LoadGenerator {
    /**
     * Initialize the load generator.
     * @param clients tells the load generator which clients it should use
     * to generate load with
     */
    void init(Client[] clients);

    /**
     * Start the warmup phase. This means that the load generator is started,
     * but the results are not collected.
     */
    void startWarmup();

    /**
     * Move from the warmup phase to the steady-state phase. Start collecting
     * results.
     */
    void startSteadyState();

    /**
     * Stop the load generator.
     */
    void stop();

    /**
     * Print a report from the test run.
     *
     * @param out stream to print the report to
     */
    void printReport(PrintStream out);
}
