/*
 *
 * Derby - Class org.apache.derbyTesting.junit.Connector
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.junit;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Factory for getting connections within the tests that is designed
 * for the simple working case for most tests. Most tests just
 * need to connect or shutdown the database, this hides through
 * BaseJDBCTestCase and TestConfiguration the details of how
 * those operations are performed.
 * <P>
 * Tests that need finer control over the connection handling
 * should use the JDBC classes directly, such as DriverManager
 * or DataSource.
 * <P>
 * This is split out into an interface and sub-classes to
 * ensure that no ClassNotFoundExceptions are thrown when
 * running in an JSR 169 environment and DriverManager is
 * not available.
 */
interface Connector {
    
    /**
     * Link this connector to the given configuration.
     * Should be called once upon setup.
     */
    abstract void setConfiguration(TestConfiguration config);
    
    /**
     * Open a connection with the database, user and password
     * defined by the configuration passed to setConfiguration.
     * If the database does not exist then it should be created.
     */
    abstract Connection openConnection() throws SQLException;
   
    /**
     * Open a connection with the database, user and password
     * defined by the configuration passed to setConfiguration.
     * If the database does not exist then it should be created.
     */
    abstract Connection openConnection(String databaseName) throws SQLException;
   
    /**
     * Open a connection to the database
     * defined by the configuration passed to setConfiguration.
     * If the database does not exist then it should be created.
     */
     abstract Connection openConnection(String user, String password)
         throws SQLException;

    /**
     * Open a connection to the database
     * defined by the configuration passed to setConfiguration.
     * If the database does not exist then it should be created.
     */
     abstract Connection openConnection(String databaseName, String user, String password)
         throws SQLException;

    /**
     * Shutdown the running default database using user and password
     * defined by the configuration passed to setConfiguration.
     * Return nothing, exception is expected to be thrown with SQLState 08006
     */
    abstract void shutDatabase() throws SQLException;
   
    /**
     * Shutdown the running derby engine (not the network server).
     * This method can only be called when the
     * engine is running embedded in this JVM.
     * Return nothing, exception is expected to be thrown with SQLState XJ015
     */
    abstract void shutEngine() throws SQLException;
}
