/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.Changes10_4

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
package org.apache.derbyTesting.functionTests.tests.upgradeTests;

import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.SupportFilesSetup;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Upgrade test cases for 10.4.
 * If the old version is 10.4 or later then these tests
 * will not be run.
 * <BR>
    10.4 Upgrade issues

 */
public class Changes10_4 extends UpgradeChange {

    public Changes10_4(String name) {
        super(name);
    }
    
    /**
     * Return the suite of tests to test the changes made in 10.4.
     * @param phase an integer that indicates the current phase in
     *              the upgrade test.
     * @return the test suite created.
     */   
    public static Test suite(int phase) {
        TestSuite suite = new TestSuite("Upgrade test for 10.4");
        
        suite.addTestSuite(Changes10_4.class);
        return new SupportFilesSetup((Test) suite);
    }
    
    /**
     * Just a place holder until we add actual tests.
     * @throws SQLException 
     *
     */
    public void testRemoveMeAfterRealTestIsAdded() throws SQLException
    {
    }
}
