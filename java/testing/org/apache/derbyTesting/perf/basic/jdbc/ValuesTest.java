/*
 *
 * Derby - Class ValuesTest
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
package org.apache.derbyTesting.perf.basic.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBCPerfTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Add performance tests that use VALUES statement 
 */
public class ValuesTest extends JDBCPerfTestCase{
    
    private PreparedStatement ps;
    
    /**
     * @return Tests that should be run as part of this class.
     */
    public static Test suite()
    {
        int iterations = 100000;
        int repeats = 4;
        
        BaseTestSuite suite = new BaseTestSuite("ValuesTest");
        
        // To add embed tests.
        suite.addTest(new ValuesTest("fetchByColumnName",iterations,repeats));
        suite.addTest(new ValuesTest("fetchByColumnNumber",iterations,repeats));
        
        // To add client tests.
        BaseTestSuite client = new BaseTestSuite("Client_ValuesTest");
        client.addTest(new ValuesTest("fetchByColumnName",iterations,repeats));
        client.addTest(new ValuesTest("fetchByColumnNumber",iterations,repeats));
        suite.addTest(TestConfiguration.clientServerDecorator(client));
        
        return suite;   
    }
    
    
    public ValuesTest(String name,int iterations,int repeats) {
        super(name,iterations,repeats);
    }
    
    public ValuesTest(String name)
    {
        super(name);
    }
    
    /**
     * Setup for the tests that query the simple VALUES statement.
     * Open a connection to the database, and prepare the query here
     */
    public void setUp() throws SQLException
    {
        println("Setup()::ValuesTest");
        ps = openDefaultConnection().prepareStatement(
                "SELECT * FROM TABLE(VALUES (1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5)) AS"+
                " T(\"cOlumN1\", COLUMN2, \"column3\")",
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.CLOSE_CURSORS_AT_COMMIT);
        
    }
    
    /**
     * Query is a simple VALUES statement.
     * <P>
     * "SELECT * FROM TABLE(VALUES (1,1,1), (2,2,2), (3,3,3), (4,4,4),
     * (5,5,5)) AS T(\"cOlumN1\", COLUMN2, \"column3\")",
     * ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, 
     * ResultSet.CLOSE_CURSORS_AT_COMMIT);
     * <P>
     * This test fetches data using column name
     */
    public void fetchByColumnName()
    throws SQLException
    {
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            rs.getInt("column1");
            rs.getInt("COLUMN2");
            rs.getInt("column3");
        }
        rs.close();    
    }
    
    /**
     * Query is a simple VALUES statement.
     * <P>
     * "SELECT * FROM TABLE(VALUES (1,1,1), (2,2,2), (3,3,3), (4,4,4), 
     * (5,5,5)) AS T(\"cOlumN1\", COLUMN2, \"column3\")",
     * ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, 
     * ResultSet.CLOSE_CURSORS_AT_COMMIT);
     * <P>
     * This test fetches data using column number
     */
    public void fetchByColumnNumber()
    throws SQLException
    {
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            rs.getInt(1);
            rs.getInt(2);
            rs.getInt(3);
        }
        rs.close();    
    }
    
    /**
     * cleanup resources
     */
    public void tearDown() throws Exception
    {
        println("ValuesTest::tearDown");
        ps.close();
        ps = null;
        super.tearDown();
    }
}
