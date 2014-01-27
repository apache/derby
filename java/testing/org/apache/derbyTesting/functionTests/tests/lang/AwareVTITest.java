/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.AwareVTITest

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;

/**
 * <p>
 * Test AwareVTIs. See DERBY-6117.
 * </p>
 */
public class AwareVTITest  extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Create a new instance.
     */

    public AwareVTITest(String name)
    {
        super(name);
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////


    /**
     * Construct top level suite in this JUnit test
     */
    public static Test suite()
    {
        TestSuite suite = (TestSuite) TestConfiguration.embeddedSuite(AwareVTITest.class);
        Test        result = new CleanDatabaseTestSetup( suite );

        return result;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Basic test of AwareVTIs.
     * </p>
     */
    public void test_01_basic() throws Exception
    {
        Connection conn = getConnection();

        vetBasic( conn, "s1", "f1" );
        vetBasic( conn, "s2", "f2" );
    }
    private void    vetBasic( Connection conn, String schema, String function ) throws Exception
    {
        goodStatement( conn, "create schema " + schema );
        goodStatement
            (
             conn,
             "create function " + schema + "." + function + "() returns table\n" +
             "(\n" +
             "    schema_name varchar( 20 ),\n" +
             "    vti_name varchar( 20 ),\n" +
             "    statement_text varchar( 100 )\n" +
             ")\n" +
             "language java parameter style derby_jdbc_result_set no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.AwareVTITest.dummyAwareVTI'\n"
             );

        String  query = "select * from table( " + schema + "." + function + "() ) s";
        assertResults
            (
             conn,
             query,
             new String[][]
             {
                 { schema.toUpperCase(), function.toUpperCase(), query },
             },
             false
             );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ROUTINES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  static  DummyAwareVTI   dummyAwareVTI()
    {
        return new DummyAwareVTI();
    }

}

