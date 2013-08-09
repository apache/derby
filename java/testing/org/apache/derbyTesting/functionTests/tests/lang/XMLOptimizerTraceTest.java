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
import java.net.URL;
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
    private static  final   String  SAVED_TRACE_NAME = "xmlOptimizer.trace";

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
        String[]    testFiles = new String[] { "functionTests/tests/lang/" + SAVED_TRACE_NAME };

        TestSuite       suite = new TestSuite( "XMLOptimizerTraceTest" );

        suite.addTest( TestConfiguration.defaultSuite( XMLOptimizerTraceTest.class ) );
 
        return new SupportFilesSetup( TestConfiguration.singleUseDatabaseDecorator( suite ), testFiles );
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
        
        if ( !tableExists( conn, "T1" ) )
        {
            goodStatement
                (
                 conn,
                 "create table t1( c1 int, c2 int, c3 int )"
                 );
        }
        
        if ( !tableExists( conn, "T2" ) )
        {
            goodStatement
                (
                 conn,
                 "create table t2( c1 int, c2 int, c3 int )"
                 );
        }
        
        if ( !tableExists( conn, "T3" ) )
        {
            goodStatement
                (
                 conn,
                 "create table t3( c1 int, c2 int, c3 int )"
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

        // use planCost to examine an outer join
        vetOuterJoin( conn );
    }

    /**
     * <p>
     * Some general tests for XmlVTI.
     * </p>
     */
    public void test_02_xmlVTI() throws Exception
    {
        Connection conn = getConnection();
        File    traceFile = SupportFilesSetup.getReadOnly( SAVED_TRACE_NAME );
        URL     traceURL = SupportFilesSetup.getReadOnlyURL( SAVED_TRACE_NAME );
        String[][]  resultsParentAndChild = new String[][]
            {
                { "1", "R_A", "HASH", "20.1395", "6" },
                { "1", "R_A", "NESTEDLOOP", "20.039500000000004", "6" },
                { "1",  "S_A", "HASH", "20.1395", "6" },
                { "1", "S_A", "NESTEDLOOP", "20.039500000000004", "6" },
                { "1", "T_A", "HASH", "20.1395", "6" },
                { "1", "T_A", "NESTEDLOOP", "20.039500000000004", "6" },
            };
        String[][]  resultsChildOnly = new String[][]
            {
                { "R_A", "HASH", "20.1395", "6" },
                { "R_A", "NESTEDLOOP", "20.039500000000004", "6" },
                { "S_A", "HASH", "20.1395", "6" },
                { "S_A", "NESTEDLOOP", "20.039500000000004", "6" },
                { "T_A", "HASH", "20.1395", "6" },
                { "T_A", "NESTEDLOOP", "20.039500000000004", "6" },
            };

        // create the type and factory function needed by the XmlVTI
        goodStatement
            (
             conn,
             "create type ArrayList external name 'java.util.ArrayList' language java"
             );
        goodStatement
            (
             conn,
             "create function asList( cell varchar( 32672 ) ... ) returns ArrayList\n" +
             "language java parameter style derby no sql\n" +
             "external name 'org.apache.derby.vti.XmlVTI.asList'\n"
             );

        // create an XmlVTI which reads from a file and incorporates parent tags
        goodStatement
            (
             conn,
             "create function decorationWithParentInfo\n" +
             "(\n" +
             "    fileName varchar( 32672 ),\n" +
             "    rowTag varchar( 32672 ),\n" +
             "    parentTags ArrayList,\n" +
             "    childTags ArrayList\n" +
             ")\n" +
             "returns table\n" +
             "(\n" +
             "        qbID int,\n" +
             "        conglomerateName varchar( 36 ),\n" +
             "        joinStrategy varchar( 20 ),\n" +
             "        estimatedCost double,\n" +
             "        estimatedRowCount int\n" +
             ")\n" +
             "language java parameter style derby_jdbc_result_set no sql\n" +
             "external name 'org.apache.derby.vti.XmlVTI.xmlVTI'\n"
             );
        goodStatement
            (
             conn,
             "create view decorationWithParentInfo as\n" +
             "select * from table\n" +
             "(\n" +
             "    decorationWithParentInfo\n" +
             "    (\n" +
             "        '" + traceFile.getPath() + "',\n" +
             "        'decoration',\n" +
             "        asList( 'qbID' ),\n" +
             "        asList( 'decConglomerateName', 'decJoinStrategy', 'ceEstimatedCost', 'ceEstimatedRowCount' )\n" +
             "    )\n" +
             ") v\n"
             );

        // create an XmlVTI which reads from a file and only used child tags
        goodStatement
            (
             conn,
             "create function decorationChildOnly\n" +
             "(\n" +
             "    fileName varchar( 32672 ),\n" +
             "    rowTag varchar( 32672 ),\n" +
             "    childTags varchar( 32672 )...\n" +
             ")\n" +
             "returns table\n" +
             "(\n" +
             "        conglomerateName varchar( 36 ),\n" +
             "        joinStrategy varchar( 20 ),\n" +
             "        estimatedCost double,\n" +
             "        estimatedRowCount int\n" +
             ")\n" +
             "language java parameter style derby_jdbc_result_set no sql\n" +
             "external name 'org.apache.derby.vti.XmlVTI.xmlVTI'\n"
             );
        goodStatement
            (
             conn,
             "create view decorationChildOnly as\n" +
             "select * from table\n" +
             "(\n" +
             "    decorationChildOnly\n" +
             "    (\n" +
             "        '" + traceFile.getPath() + "',\n" +
             "        'decoration',\n" +
             "        'decConglomerateName', 'decJoinStrategy', 'ceEstimatedCost', 'ceEstimatedRowCount'\n" +
             "    )\n" +
             ") v\n"
             );

        // create an XmlVTI which reads from an url file and uses parent tags
        goodStatement
            (
             conn,
             "create function decorationURLParentInfo\n" +
             "(\n" +
             "    urlString varchar( 32672 ),\n" +
             "    rowTag varchar( 32672 ),\n" +
             "    parentTags ArrayList,\n" +
             "    childTags ArrayList\n" +
             ")\n" +
             "returns table\n" +
             "(\n" +
             "        qbID int,\n" +
             "        conglomerateName varchar( 36 ),\n" +
             "        joinStrategy varchar( 20 ),\n" +
             "        estimatedCost double,\n" +
             "        estimatedRowCount int\n" +
             ")\n" +
             "language java parameter style derby_jdbc_result_set no sql\n" +
             "external name 'org.apache.derby.vti.XmlVTI.xmlVTIFromURL'\n"
             );
        goodStatement
            (
             conn,
             "create view decorationURLParentInfo as\n" +
             "select * from table\n" +
             "(\n" +
             "    decorationURLParentInfo\n" +
             "    (\n" +
             "        '" + traceURL.toString() + "',\n" +
             "        'decoration',\n" +
             "        asList( 'qbID' ),\n" +
             "        asList( 'decConglomerateName', 'decJoinStrategy', 'ceEstimatedCost', 'ceEstimatedRowCount' )\n" +
             "    )\n" +
             ") v\n"
             );

        // create an XmlVTI which reads from an url file and uses only child tags
        goodStatement
            (
             conn,
             "create function decorationURLChildOnly\n" +
             "(\n" +
             "    urlString varchar( 32672 ),\n" +
             "    rowTag varchar( 32672 ),\n" +
             "    childTags varchar( 32672 )...\n" +
             ")\n" +
             "returns table\n" +
             "(\n" +
             "        conglomerateName varchar( 36 ),\n" +
             "        joinStrategy varchar( 20 ),\n" +
             "        estimatedCost double,\n" +
             "        estimatedRowCount int\n" +
             ")\n" +
             "language java parameter style derby_jdbc_result_set no sql\n" +
             "external name 'org.apache.derby.vti.XmlVTI.xmlVTIFromURL'\n"
             );
        goodStatement
            (
             conn,
             "create view decorationURLChildOnly as\n" +
             "select * from table\n" +
             "(\n" +
             "    decorationURLChildOnly\n" +
             "    (\n" +
             "        '" + traceURL.toString() + "',\n" +
             "        'decoration',\n" +
             "        'decConglomerateName', 'decJoinStrategy', 'ceEstimatedCost', 'ceEstimatedRowCount'\n" +
             "    )\n" +
             ") v\n"
             );

        // verify that the XmlVTIs work
        assertResults
            (
             conn,
             "select distinct qbID, conglomerateName, joinStrategy, estimatedCost, estimatedRowCount\n" +
             "from decorationWithParentInfo\n" +
             "where conglomerateName like '%_A' and estimatedCost is not null\n" +
             "order by qbID, conglomerateName, joinStrategy, estimatedCost, estimatedRowCount\n",
             resultsParentAndChild,
             false
             );

        assertResults
            (
             conn,
             "select distinct conglomerateName, joinStrategy, estimatedCost, estimatedRowCount\n" +
             "from decorationChildOnly\n" +
             "where conglomerateName like '%_A' and estimatedCost is not null\n" +
             "order by conglomerateName, joinStrategy, estimatedCost, estimatedRowCount\n",
             resultsChildOnly,
             false
             );
        
        assertResults
            (
             conn,
             "select distinct qbID, conglomerateName, joinStrategy, estimatedCost, estimatedRowCount\n" +
             "from decorationURLParentInfo\n" +
             "where conglomerateName like '%_A' and estimatedCost is not null\n" +
             "order by qbID, conglomerateName, joinStrategy, estimatedCost, estimatedRowCount\n",
             resultsParentAndChild,
             false
             );

        assertResults
            (
             conn,
             "select distinct conglomerateName, joinStrategy, estimatedCost, estimatedRowCount\n" +
             "from decorationURLChildOnly\n" +
             "where conglomerateName like '%_A' and estimatedCost is not null\n" +
             "order by conglomerateName, joinStrategy, estimatedCost, estimatedRowCount\n",
             resultsChildOnly,
             false
             );
        
        // clean up after ourselves
        goodStatement( conn, "drop view decorationURLChildOnly" );
        goodStatement( conn, "drop function decorationURLChildOnly" );
        goodStatement( conn, "drop view decorationURLParentInfo" );
        goodStatement( conn, "drop function decorationURLParentInfo" );
        goodStatement( conn, "drop view decorationChildOnly" );
        goodStatement( conn, "drop function decorationChildOnly" );
        goodStatement( conn, "drop view decorationWithParentInfo" );
        goodStatement( conn, "drop function decorationWithParentInfo" );
        goodStatement( conn, "drop function asList" );
        goodStatement( conn, "drop type ArrayList restrict" );
    }


    /**
     * <p>
     * Test xml optimizer tracing of outer joins.
     * </p>
     */
    private void vetOuterJoin( Connection conn ) throws Exception
    {
        File    traceFile = SupportFilesSetup.getReadWrite( TRACE_FILE_NAME );

        // turn on xml-based optimizer tracing
        goodStatement
            (
             conn,
             "call syscs_util.syscs_register_tool( 'optimizerTracing', true, 'xml' )"
             );

        // run an outer join
        goodStatement
            (
             conn,
             "select * from t3, (t1 left outer join t2 on t1.c1 = t2.c1) where t3.c1 = t1.c1"
             );

        // turn off optimizer tracing
        goodStatement
            (
             conn,
             "call syscs_util.syscs_register_tool( 'optimizerTracing', false, '" + traceFile.getPath() + "' )"
             );

        // load the trace viewer
        goodStatement
            (
             conn,
             "call syscs_util.syscs_register_tool( 'optimizerTracingViews', true, '" + traceFile.getPath() + "' )"
             );

        // verify the plan shapes which were considered
        PreparedStatement   ps = chattyPrepare
            (
             conn,
             "select distinct summary from planCost\n" +
             "where complete and qbID = 1\n" +
             "order by summary\n"
             );
        ResultSet   rs = ps.executeQuery();
        rs.next();
        String  summary1 = rs.getString( 1 ).trim();
        rs.next();
        String  summary2 = rs.getString( 1 ).trim();
        assertTrue( summary1.startsWith( "( \"APP\"." ) );
        assertTrue( summary1.endsWith( " * ProjectRestrictNode )" ) );
        assertTrue( summary2.startsWith( "( ProjectRestrictNode # \"APP\"." ) );
        rs.close();
        ps.close();

        // unload the trace viewer
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
