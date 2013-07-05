/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.XMLOptimizerTraceTest
   
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

import java.io.File;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.Decorator;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.JDBC;

/**
 * <p>
 * Test xml-based optimizer tracing, introduced by DERBY-6211.
 * </p>
 */
public class XMLOptimizerTraceTest  extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String  TRACE_FILE_NAME = "xott.xml";

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

    public XMLOptimizerTraceTest(String name)
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
        TestSuite       suite = new TestSuite( "XMLOptimizerTraceTest" );

        suite.addTest( TestConfiguration.defaultSuite( XMLOptimizerTraceTest.class ) );

        return new SupportFilesSetup( TestConfiguration.singleUseDatabaseDecorator( suite ) );
    }

    protected void    setUp()
        throws Exception
    {
        super.setUp();

        Connection conn = getConnection();

        if ( !routineExists( conn, "INTEGERLIST" ) )
        {
            goodStatement
                (
                 conn,
                 "create function integerList()\n" +
                 "returns table( a int, b int, c int, d int )\n" +
                 "language java parameter style derby_jdbc_result_set no sql\n" +
                 "external name 'org.apache.derbyTesting.functionTests.tests.lang.RestrictedVTITest.integerList'\n"
                 );
        }
        
        if ( !routineExists( conn, "GETRESULTSETMETADATA" ) )
        {
            goodStatement
                (
                 conn,
                 "create function getResultSetMetaData( query varchar( 32672 ) )\n" +
                 "returns table\n" +
                 "(\n" +
                 "        getCatalogName varchar( 32672 ),\n" +
                 "        getColumnClassName varchar( 32672 ),\n" +
                 "        getColumnDisplaySize int,\n" +
                 "        getColumnLabel varchar( 32672 ),\n" +
                 "        getColumnName varchar( 32672 ),\n" +
                 "        getColumnType int,\n" +
                 "        getColumnTypeName varchar( 32672 ),\n" +
                 "        getPrecision int,\n" +
                 "        getScale int,\n" +
                 "        getSchemaName varchar( 32672 ),\n" +
                 "        getTableName varchar( 32672 ),\n" +
                 "        isAutoIncrement boolean,\n" +
                 "        isCaseSensitive boolean,\n" +
                 "        isCurrency boolean,\n" +
                 "        isDefinitelyWritable boolean,\n" +
                 "        isNullable int,\n" +
                 "        isReadOnly boolean,\n" +
                 "        isSearchable boolean,\n" +
                 "        isSigned boolean,\n" +
                 "        isWritable boolean\n" +
                 ")\n" +
                 "language java parameter style derby_jdbc_result_set reads sql data\n" +
                 "external name 'org.apache.derbyTesting.functionTests.tests.lang.RSMDWrapper.getResultSetMetaData'\n"
                 );
        }
        
        if ( !tableExists( conn, "T" ) )
        {
            goodStatement
                (
                 conn,
                 "create table t( a int, b varchar( 100 ) )"
                 );
            goodStatement
                (
                 conn,
                 "create index t_a on t( a )"
                 );
        }
        
        if ( !tableExists( conn, "S" ) )
        {
            goodStatement
                (
                 conn,
                 "create table s( a int, b varchar( 100 ) )"
                 );
            goodStatement
                (
                 conn,
                 "create index s_a on s( a )"
                 );
        }
        
        if ( !tableExists( conn, "R" ) )
        {
            goodStatement
                (
                 conn,
                 "create table r( a int, b varchar( 100 ) )"
                 );
            goodStatement
                (
                 conn,
                 "create index r_a on r( a )"
                 );
        }
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test the planCost table function.
     * </p>
     */
    public void test_01_planCost() throws Exception
    {
        Connection conn = getConnection();
        File    traceFile = SupportFilesSetup.getReadWrite( TRACE_FILE_NAME );

        // turn on xml-based optimizer tracing and run some queries
        goodStatement
            (
             conn,
             "call syscs_util.syscs_register_tool( 'optimizerTracing', true, 'xml' )"
             );

        // 2-table query
        goodStatement
            (
             conn,
             "select s.a from t, s where t.a = s.a"
             );
        // 3-table query
        goodStatement
            (
             conn,
             "select s.a from t, s, r where t.a = s.a and s.a = r.a"
             );
        // query involving a table function
        goodStatement
            (
             conn,
             "select s.a from s, table( integerList() ) i where s.a = i.a"
             );

        // turn off optimizer tracing and dump the xml trace to a file
        goodStatement
            (
             conn,
             "call syscs_util.syscs_register_tool( 'optimizerTracing', false, '" + traceFile.getPath() + "' )"
             );

        // install the planCost table function and view
        goodStatement
            (
             conn,
             "call syscs_util.syscs_register_tool( 'optimizerTracingViews', true, '" + traceFile.getPath() + "' )"
             );

        // verify the full signature of the planCost table function
        assertResults
            (
             conn,
             "select getColumnName, getColumnTypeName, getPrecision from table( getResultSetMetaData( 'select * from planCost where 1=2' ) ) g",
             new String[][]
             {
                 { "TEXT", "VARCHAR", "32672" },
                 { "STMTID", "INTEGER", "10" },
                 { "QBID", "INTEGER", "10" },
                 { "COMPLETE", "BOOLEAN", "1" },
                 { "SUMMARY", "VARCHAR", "32672" },
                 { "TYPE", "VARCHAR", "50" },
                 { "ESTIMATEDCOST", "DOUBLE", "15" },
                 { "ESTIMATEDROWCOUNT", "BIGINT", "19" },         
             },
             false
             );

        // verify some contents of the xml output which we hope will remain stable
        // across test platforms
        assertResults
            (
             conn,
             "select distinct stmtID, summary from planCost where complete order by stmtID, summary",
             new String[][]
             {
                 { "1", "( \"APP\".\"S_A\" # \"APP\".\"T_A\" )" },
                 { "1", "( \"APP\".\"T_A\" # \"APP\".\"S_A\" )" },
                 { "2", "( ( \"APP\".\"R_A\" # \"APP\".\"S_A\" ) * \"APP\".\"T_A\" )" },
                 { "2", "( ( \"APP\".\"R_A\" # \"APP\".\"T_A\" ) * \"APP\".\"S_A\" )" },
                 { "2", "( ( \"APP\".\"S_A\" # \"APP\".\"R_A\" ) * \"APP\".\"T_A\" )" },
                 { "2", "( ( \"APP\".\"S_A\" # \"APP\".\"T_A\" ) * \"APP\".\"R_A\" )" },
                 { "2", "( ( \"APP\".\"T_A\" # \"APP\".\"R_A\" ) * \"APP\".\"S_A\" )" },
                 { "2", "( ( \"APP\".\"T_A\" # \"APP\".\"S_A\" ) * \"APP\".\"R_A\" )" },
                 { "3", "( \"APP\".\"INTEGERLIST\"() # \"APP\".\"S_A\" )" },
                 { "3", "( \"APP\".\"S_A\" # \"APP\".\"INTEGERLIST\"() )" },
             },
             false
             );

        // uninstall the planCost table function and view
        goodStatement
            (
             conn,
             "call syscs_util.syscs_register_tool( 'optimizerTracingViews', false )"
             );
    }

   ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Return true if the SQL routine exists */
    private boolean routineExists( Connection conn, String functionName ) throws Exception
    {
        PreparedStatement ps = chattyPrepare( conn, "select count (*) from sys.sysaliases where alias = ?" );
        ps.setString( 1, functionName );

        ResultSet rs = ps.executeQuery();
        rs.next();

        boolean retval = rs.getInt( 1 ) > 0 ? true : false;

        rs.close();
        ps.close();

        return retval;
    }

    /** Return true if the table exists */
    private boolean tableExists( Connection conn, String tableName ) throws Exception
    {
        PreparedStatement ps = chattyPrepare( conn, "select count (*) from sys.systables where tablename = ?" );
        ps.setString( 1, tableName );

        ResultSet rs = ps.executeQuery();
        rs.next();

        boolean retval = rs.getInt( 1 ) > 0 ? true : false;

        rs.close();
        ps.close();

        return retval;
    }

}
