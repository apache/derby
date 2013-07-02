/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.NewOptimizerOverridesTest

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

import java.io.StringWriter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.Decorator;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.JDBC;

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.iapi.sql.Activation;

/**
 * <p>
 * Test the complete plan overrides added by DERBY-6267.
 * </p>
 */
public class NewOptimizerOverridesTest  extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String  WRONG_ROW_SOURCE_COUNT = "42ZCC";
    private static  final   String  NOT_LEFT_DEEP = "42ZCD";
    private static  final   String  MISSING_INDEX = "42X65";
    private static  final   String  MISSING_FUNCTION = "42X94";
    private static  final   String  MISSING_SCHEMA = "42Y07";
    private static  final   String  UNSUPPORTED_PLAN_SHAPE = "42Y69";

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

    public NewOptimizerOverridesTest(String name)
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
        TestSuite       suite = new TestSuite( "NewOptimizerOverridesTest" );

        suite.addTest( TestConfiguration.embeddedSuite( NewOptimizerOverridesTest.class ) );

        // use a policy file which allows the xml-based plan reader to access fields in the ResultSet graph
        return new SecurityManagerSetup
            (
             suite,
             "org/apache/derbyTesting/functionTests/tests/lang/resultSetReader.policy"
             );
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
        
        if ( !tableExists( conn, "V" ) )
        {
            goodStatement
                (
                 conn,
                 "create view v as select tablename from sys.systables"
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
     * Basic syntax.
     * </p>
     */
    public void test_01_basicSyntax() throws Exception
    {
        Connection conn = getConnection();

        // these statements, without optimizer overrides, should run fine
        goodStatement
            ( conn,
              "select tablename from v, sys.syscolumns\n" +
              "where tablename = columnname\n"
              );
        goodStatement
            ( conn,
              "select columnname from sys.syscolumns, table( integerList() ) i\n" +
              "where columnnumber = -i.a\n"
              );
        goodStatement
            ( conn,
              "select tablename\n" +
              "from sys.systables t, sys.syscolumns c, sys.sysaliases a\n" +
              "where tablename = columnname and columnname = alias\n"
              );

        // properly stated plan
        goodStatement
            ( conn,
              "select columnname from sys.syscolumns, table( integerList() ) i\n" +
              "where columnnumber = -i.a\n" +
              "--derbyplan ( app.integerList() # sys.syscolumns_heap )\n"
              );

        // wrong number of row sources in the plan
        expectCompilationError
            ( WRONG_ROW_SOURCE_COUNT,
              "select tablename from v, sys.syscolumns\n" +
              "where tablename = columnname\n" +
              "--derbyplan A\n"
              );
        expectCompilationError
            ( WRONG_ROW_SOURCE_COUNT,
              "select tablename from v, sys.syscolumns\n" +
              "where tablename = columnname\n" +
              "--derbyplan A.B\n"
              );
        expectCompilationError
            ( WRONG_ROW_SOURCE_COUNT,
              "select tablename from v, sys.syscolumns\n" +
              "where tablename = columnname\n" +
              "--derbyplan ( ( A.B # C.D ) * E )\n"
              );

        // unknown conglomerates
        expectCompilationError
            ( MISSING_INDEX,
              "select tablename from v, sys.syscolumns\n" +
              "where tablename = columnname\n" +
              "--derbyplan ( A * C )\n"
              );

        // unknown function
        expectCompilationError
            ( MISSING_FUNCTION,
              "select tablename from v, sys.syscolumns\n" +
              "where tablename = columnname\n" +
              "--derbyplan ( A() * C )\n"
              );

        // unknown schema
        expectCompilationError
            ( MISSING_SCHEMA,
              "select tablename from v, sys.syscolumns\n" +
              "where tablename = columnname\n" +
              "--derbyplan ( A.B * C )\n"
              );
        expectCompilationError
            ( MISSING_SCHEMA,
              "select tablename from v, sys.syscolumns\n" +
              "where tablename = columnname\n" +
              "--derbyplan ( A.B # C.D )\n"
              );

        // plan is not left deep
        expectCompilationError
            ( NOT_LEFT_DEEP,
              "select tablename\n" +
              "from sys.systables t, sys.syscolumns c, sys.sysaliases a\n" +
              "where tablename = columnname and columnname = alias\n" +
              "--derbyplan ( A.B # ( C.D * E ) )\n"
              );

        // syntax errors
        expectCompilationError
            ( SYNTAX_ERROR,
              "select tablename\n" +
              "from sys.systables t, sys.syscolumns c, sys.sysaliases a\n" +
              "where tablename = columnname and columnname = alias\n" +
              "--derbyplan blah blah blah ( ( A.B # C.D ) * E )\n"
              );

        // bad join operator
        expectCompilationError
            ( LEXICAL_ERROR,
              "select tablename\n" +
              "from sys.systables t, sys.syscolumns c, sys.sysaliases a\n" +
              "where tablename = columnname and columnname = alias\n" +
              "--derbyplan ( ( A.B # C.D ) $ E )\n"
              );
    }
    
    /**
     * <p>
     * Verify that plan shapes can be overridden for simple selects.
     * </p>
     */
    public void test_02_simpleSelects() throws Exception
    {
        Connection conn = getConnection();
        String      select;

        //
        // 2 RowSource plan.
        //
        select =
            "select columnname from sys.syscolumns, table( integerList() ) i\n" +
            "where columnnumber = -i.a\n";

        // by itself without an optimizer override. the table function is in the outer slot.
        assertPlanShape
            (
             conn, select,
             "( org.apache.derbyTesting.functionTests.tests.lang.RestrictedVTITest # SYSCOLUMNS )"
             );

        // with an override which places the table function on the inner slot
        assertPlanShape
            (
             conn, select + "\n--derbyplan ( sys.syscolumns_heap * app.integerList() )\n",
             "( SYSCOLUMNS * org.apache.derbyTesting.functionTests.tests.lang.RestrictedVTITest )"
             );

        // hashjoin strategy not allowed for this query
        expectCompilationError
            ( UNSUPPORTED_PLAN_SHAPE,
              select + "\n--derbyplan ( sys.syscolumns_heap # app.integerList() )"
              );
        
        //
        // 4 RowSource plan.
        //
        select =
            "select tablename from sys.systables t, sys.syscolumns c, sys.sysaliases a, sys.syssequences s\n" +
            "where t.tablename = c.columnname and c.columnname = a.alias and a.alias = s.sequencename\n";

        // with an override the join order is syssequences, syscolumns, sysaliases, systables
        assertPlanShape
            (
             conn, select + "--derbyplan ( ((SYS.SYSSEQUENCES_INDEX2 # SYS.SYSCOLUMNS_HEAP) # SYS.SYSALIASES_INDEX1) # SYS.SYSTABLES_INDEX1 )\n",
             "( ( ( SYSSEQUENCES_INDEX2 # SYSCOLUMNS ) # SYSALIASES_INDEX1 ) # SYSTABLES_INDEX1 )"
             );

        // missing a RowSource for SYSALIASES
        expectCompilationError
            ( UNSUPPORTED_PLAN_SHAPE,
              select + "\n--derbyplan ( ((SYS.SYSSEQUENCES_INDEX2 # SYS.SYSCOLUMNS_HEAP) # SYS.SYSCOLUMNS_HEAP) # SYS.SYSTABLES_INDEX1 )"
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

    /** Assert that a query produces the expected plan shape */
    static  void    assertPlanShape( Connection conn, String query, String expectedPlanShape )
        throws Exception
    {
        ResultSet   rs = conn.prepareStatement( query ).executeQuery();
        String      actualPlanShape = summarize( getLastQueryPlan( conn, rs ) );
        rs.close();

        println( "Expected plan shape = " + expectedPlanShape );
        println( "Actual plan shape = " + actualPlanShape );

        assertEquals( expectedPlanShape, actualPlanShape );
    }

    /** Get an xml-based picture of the plan chosen for the last query. The query is identified by its JDBC ResultSet */
    static  Document    getLastQueryPlan( Connection conn, ResultSet rs ) throws Exception
    {
        ContextManager      contextManager = ((EmbedConnection) conn).getContextManager();
        LanguageConnectionContext   lcc = (LanguageConnectionContext) contextManager.getContext( "LanguageConnectionContext" );
        org.apache.derby.iapi.sql.ResultSet derbyRS = lcc.getLastActivation().getResultSet();

        Document    doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
        Element     root = doc.createElement( "planTrace" );
        doc.appendChild( root );

        derbyRS.toXML( root, "top" );

        return doc;
    }

    public static  String    summarize( Document doc ) throws Exception
    {
        StringBuilder   buffer = new StringBuilder();
        Element     root = getFirstElement( doc.getDocumentElement(), "top" );;

        summarize( buffer, root );

        return buffer.toString();
    }

    private static  void    summarize( StringBuilder buffer, Element element ) throws Exception
    {
        String  type = element.getAttribute( "type" );

        if ( "HashJoinResultSet".equals( type ) ) { summarizeJoin( buffer, element, "#" ); }
        else if ( "NestedLoopJoinResultSet".equals( type ) ) { summarizeJoin( buffer, element, "*" ); }
        else if ( "ProjectRestrictResultSet".equals( type ) ) { summarize( buffer, getFirstElement( element, "source" ) ); }
        else
        {
            String  indexName = element.getAttribute( "indexName" );
            String  tableName = element.getAttribute( "tableName" );
            String  javaClassName = element.getAttribute( "javaClassName" );

            if ( indexName.length() != 0 ) { buffer.append( indexName ); }
            else if ( tableName.length() != 0 ) { buffer.append( tableName ); }
            else if ( javaClassName.length() != 0 ) { buffer.append( javaClassName ); }
            else { buffer.append( type ); }
        }
    }

    private static  void    summarizeJoin( StringBuilder buffer, Element element, String joinSymbol )
        throws Exception
    {
        buffer.append( "( " );
        summarize( buffer, getFirstElement( element, "leftResultSet" ) );
        buffer.append( " " + joinSymbol + " " );
        summarize( buffer, getFirstElement( element, "rightResultSet" ) );
        buffer.append( " )" );
    }

    /** Get first element by the give tag name */
    private static  Element    getFirstElement( Element parent, String tag ) throws Exception
    {
        NodeList    list = parent.getChildNodes();

        for ( int i = 0; i < list.getLength(); i++ )
        {
            Node    child = list.item( i );
            if ( tag.equals( child.getNodeName() ) ) { return (Element) child; }
        }

        return null;
    }

    /** Print a document to a string. Not actually used. Useful for debugging this test. */
    static  String  printDocument( Document doc ) throws Exception
    {
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();
        DOMSource source = new DOMSource( doc );
        StringWriter    sw = new StringWriter();
        StreamResult result = new StreamResult( sw );
        
        // pretty-print
        transformer.setOutputProperty( OutputKeys.OMIT_XML_DECLARATION, "no" );
        transformer.setOutputProperty( OutputKeys.METHOD, "xml" );
        transformer.setOutputProperty( OutputKeys.INDENT, "yes" );
        transformer.setOutputProperty( OutputKeys.ENCODING, "UTF-8" );
        transformer.setOutputProperty( "{http://xml.apache.org/xslt}indent-amount", "4" );
        
        transformer.transform( source, result );

        return sw.toString();
    }

}
