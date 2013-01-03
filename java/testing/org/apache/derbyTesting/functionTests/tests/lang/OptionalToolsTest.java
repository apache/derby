/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.OptionalToolsTest

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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Blob;
import java.sql.Clob;
import java.util.HashMap;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.Decorator;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.JDBC;

/**
 * <p>
 * Test optional tools. See DERBY-6022.
 * </p>
 */
public class OptionalToolsTest  extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    protected static final    String NO_SUCH_TABLE_FUNCTION = "42ZB4";

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

    public OptionalToolsTest(String name)
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
        TestSuite       suite = new TestSuite( "OptionalToolsTest" );

        suite.addTest( TestConfiguration.defaultSuite(OptionalToolsTest.class) );

        return suite;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test the optional package of routines which wrap the DatabaseMetaData methods.
     * </p>
     */
    public void test_01_dbmdWrapper() throws Exception
    {
        Connection conn = getConnection();
        String  getTypeInfo = "select type_name, minimum_scale, maximum_scale from table( getTypeInfo() ) s";

        // the routines don't exist unless you register them
        expectCompilationError( NO_SUCH_TABLE_FUNCTION, getTypeInfo );

        // now register the database metadata wrappers
        goodStatement( conn, "call syscs_util.syscs_register_tool( 'dbmd', true )" );

        // now the routine exists
        assertResults
            (
             conn,
             getTypeInfo,
             new String[][]
             {
                 { "BIGINT", "0", "0" },
                 { "LONG VARCHAR FOR BIT DATA", null, null },
                 { "VARCHAR () FOR BIT DATA", null, null },
                 { "CHAR () FOR BIT DATA", null, null },
                 { "LONG VARCHAR", null, null },
                 { "CHAR", null, null },
                 { "NUMERIC", "0", "31" },
                 { "DECIMAL", "0", "31" },
                 { "INTEGER", "0", "0" },
                 { "SMALLINT", "0", "0" },
                 { "FLOAT", null, null },
                 { "REAL", null, null },
                 { "DOUBLE", null, null },
                 { "VARCHAR", null, null },
                 { "BOOLEAN", null, null },
                 { "DATE", "0", "0" },
                 { "TIME", "0", "0" },
                 { "TIMESTAMP", "0", "9" },
                 { "OBJECT", null, null },
                 { "BLOB", null, null }, 
                 { "CLOB", null, null },
                 { "XML", null, null },
             },
             false
             );

        // now unregister the database metadata wrappers
        goodStatement( conn, "call syscs_util.syscs_register_tool( 'dbmd', false )" );

        // the routines don't exist anymore
        expectCompilationError( NO_SUCH_TABLE_FUNCTION, getTypeInfo );
    }
    
}

