/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.GeneratedColumnsTest

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

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.DriverManager;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;

import org.apache.derby.catalog.types.RoutineAliasInfo;

/**
 * <p>
 * Test generated columns. See DERBY-481.
 * </p>
 */
public class GeneratedColumnsTest extends BaseJDBCTestCase
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

    public GeneratedColumnsTest(String name)
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
        TestSuite suite = (TestSuite) TestConfiguration.embeddedSuite(GeneratedColumnsTest.class);

        return new CleanDatabaseTestSetup( suite );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // SUCCESSFUL RESOLUTIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test that the stored system procedures and functions are non-deterministic. If you want
     * a particular procedure/function to be deterministic, add some logic here.
     *
     * Also test that, by default, user-defined routines are created as NOT DETERMINISTIC.
     * </p>
     */
    public  void    test_001_determinism_of_stored_system_routines()
        throws Exception
    {
        Connection  conn = getConnection();

        //
        // Create a user-defined function and procedure and verify
        // that they too are NOT DETERMINISTIC.
        // 
        PreparedStatement functionCreate = conn.prepareStatement
            (
             "create function f1()\n" +
             "returns int\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'foo.bar.wibble'\n"
             );
        functionCreate.execute();
        functionCreate.close();

        PreparedStatement procedureCreate = conn.prepareStatement
            (
             "create procedure p1()\n" +
             "language java\n" +
             "parameter style java\n" +
             "modifies sql data\n" +
             "external name 'foo.bar.wibble'\n"
             );
        procedureCreate.execute();
        procedureCreate.close();

        //
        // OK, now verify that all routines in the catalogs are NOT DETERMINISTIC 
        //
        PreparedStatement   ps = conn.prepareStatement
            (
             "select s.schemaname, a.alias, a.aliastype, a.systemalias, a.aliasinfo\n" +
             "from sys.sysschemas s, sys.sysaliases a\n" +
             "where s.schemaid = a.schemaid\n" +
             "order by s.schemaname, a.alias\n"
             );
        ResultSet               rs = ps.executeQuery();

        while ( rs.next() )
        {
            String    aliasName = rs.getString( 2 );
            boolean isSystemAlias = rs.getBoolean( 4 );

            RoutineAliasInfo    rai = (RoutineAliasInfo) rs.getObject( 5 );

            assertFalse( aliasName, rai.isDeterministic() );
        }

        rs.close();
        ps.close();
    }


    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

}