/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.lang.XMLXXETest.java
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
package org.apache.derbyTesting.functionTests.tests.lang;

import java.io.File;
import java.io.PrintWriter;
import java.net.URL;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.LocaleTestSetup;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.XML;

/**
 * XMLXXETest this test suite runs with NO SECURITY MANAGER. It is designed
 * to explore the so-called XXE family of vulnerabilities. For more
 * information, try:
 *
 * http://h3xstream.github.io/find-sec-bugs/bugs.htm#XXE_DOCUMENT
 * https://www.owasp.org/index.php/XML_External_Entity_%28XXE%29_Processing
 * http://www.ws-attacks.org/index.php/XML_Entity_Expansion
 * http://www.ws-attacks.org/index.php/XML_External_Entity_DOS
 * http://www.ws-attacks.org/index.php/XML_Entity_Reference_Attack
 */
public final class XMLXXETest extends BaseJDBCTestCase {
    
    public XMLXXETest(String name)
    {
        super(name);
    }

    /**
     * Only return a suite IF the testing classpath has the
     * required XML classes.  Otherwise just return an empty suite.
     */
    public static Test suite()
    {
        BaseTestSuite suite =
            new BaseTestSuite("XML XXE Vulnerability tests\n");

        if (!XML.classpathMeetsXMLReqs())
            return suite;

//IC see: https://issues.apache.org/jira/browse/DERBY-6810
//IC see: https://issues.apache.org/jira/browse/DERBY-6820
	String[] testFiles = new String[] {
	    "functionTests/tests/lang/xmlOptimizerXXE1Payload.trace",
	    "functionTests/tests/lang/xmlOptimizerXXE1.trace",
	    "functionTests/tests/lang/xmlOptimizerXXE2.trace"
	};

        suite.addTest( new SupportFilesSetup( 
			TestConfiguration.defaultSuite(XMLXXETest.class),
			testFiles ) );

        // Need to run in US locale because the test checks error messages
        // which may be different in different locales (DERBY-6869).
        return new LocaleTestSetup(
                SecurityManagerSetup.noSecurityManager(suite),
                Locale.US);
    }
 
    /**
     * Test for Derby-6807. We create a file with some (presumably sensitive)
     * data in it, and check whether an XML query can be tricked into reading
     * the data from that file. If it can, a security leak has occurred.
     */

    public void testDerby6807FileAccess ()
			throws Exception
    {
        File password = null;
        String path;

        password = new File("test6807.txt");
        PrintWriter writer = new PrintWriter("test6807.txt", "UTF-8");
        writer.print("HelloWorld");
        writer.close();
        path = password.getAbsolutePath();
        
        Statement s = createStatement();
        
        s.execute("CREATE TABLE xml_data(xml_col XML)");

//IC see: https://issues.apache.org/jira/browse/DERBY-6810
        String stmt = "INSERT INTO xml_data(xml_col) VALUES(XMLPARSE(DOCUMENT" 
                + "'<!DOCTYPE foo [<!ENTITY xxe SYSTEM \"file:"
                + path +"\" >]><yolo>&xxe;</yolo>'"
                + "PRESERVE WHITESPACE))";

	// System.out.println( stmt );

	s.execute( stmt );

        JDBC.assertSingleValueResultSet(
                s.executeQuery(
	            "SELECT XMLSERIALIZE(xml_col AS CLOB) FROM xml_data"),
	            "<yolo/>");

        password.delete();
    }

    public void testDerby6807BillionLaughs() throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6810
        Statement st = createStatement();
        st.executeUpdate("create table xml_billion_laughs( xml_col xml )");

String xmlBillionLaughs = "insert into xml_billion_laughs( xml_col ) values(" +
                         " xmlparse(document '" +
"<!DOCTYPE lolz [" +
" <!ENTITY lol \"lol\">" +
" <!ELEMENT lolz (#PCDATA)>" +
" <!ENTITY lol1 \"&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;\">" +
" <!ENTITY lol2 \"&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;\">" +
" <!ENTITY lol3 \"&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;\">" +
" <!ENTITY lol4 \"&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;\">" +
" <!ENTITY lol5 \"&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;\">" +
" <!ENTITY lol6 \"&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;\">" +
" <!ENTITY lol7 \"&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;\">" +
" <!ENTITY lol8 \"&lol7;&lol7;&lol7;&lol7;&lol7;&lol7;&lol7;&lol7;&lol7;&lol7;\">" +
" <!ENTITY lol9 \"&lol8;&lol8;&lol8;&lol8;&lol8;&lol8;&lol8;&lol8;&lol8;&lol8;\">" +
"]>" +
"<lolz>&lol9;</lolz>' PRESERVE WHITESPACE))";

	assertStatementError( "2200M", st, xmlBillionLaughs );

	// Since we can't even parse the document, we never get to the point
	// where we might try to serialize it back out.
        //    "select xmlserialize(xml_col as clob) from xml_billion_laughs");
    }

    public void testDerby6807FileAccessVTI()
//IC see: https://issues.apache.org/jira/browse/DERBY-6810
//IC see: https://issues.apache.org/jira/browse/DERBY-6820
		throws Exception
    {
	String VULNERABLE_XML = "xmlOptimizerXXE1.trace";
	URL     traceURL = SupportFilesSetup.getReadOnlyURL( VULNERABLE_XML );
	//URL payloadURL = SupportFilesSetup.getReadOnlyURL(
	//			"xmlOptimizerXXE1Payload.trace" );

        Statement s = createStatement();
	s.execute(
             "create function decorationURLChildOnly\n" +
             "(\n" +
             "    urlString varchar( 32672 ),\n" +
             "    rowTag varchar( 32672 ),\n" +
             "    childTags varchar( 32672 )...\n" +
             ")\n" +
             "returns table\n" +
             "(\n" +
             "        conglomerateName varchar( 36 ),\n" +
             "        joinStrategy varchar( 200 ),\n" +
             "        estimatedCost double,\n" +
             "        estimatedRowCount varchar( 200 )\n" +
             ")\n" +
             "language java parameter style derby_jdbc_result_set no sql\n" +
             "external name 'org.apache.derby.vti.XmlVTI.xmlVTIFromURL'\n"
             );
	s.execute(
             "create view decorationURLChildOnly as\n" +
             "select * from table\n" +
             "(\n" +
             "    decorationURLChildOnly\n" +
             "    (\n" +
             "        '" + traceURL.toString() + "',\n" +
             "        'decoration',\n" +
             "        'decConglomerateName', 'decJoinStrategy',\n" +
	     "        'ceEstimatedCost', 'ceEstimatedRowCount'\n" +
             "    )\n" +
             ") v\n"
             );
	ResultSet rs = s.executeQuery(
             "select distinct conglomerateName, joinStrategy," +
	     "                estimatedCost, estimatedRowCount\n" +
             "from decorationURLChildOnly\n" +
             "where conglomerateName like '%_A' and " +
	     "      estimatedCost is not null\n" +
             "order by conglomerateName, joinStrategy, " +
	     "         estimatedCost, estimatedRowCount\n"
             );
	assertTrue( rs.next() );

	assertEquals( "null", rs.getString( 4 ).trim() );

	assertFalse( rs.next() );
    }

    public void testDerby6807BillionLaughsVTI()
		throws Exception
    {
	String VULNERABLE_XML = "xmlOptimizerXXE2.trace";
	URL     traceURL = SupportFilesSetup.getReadOnlyURL( VULNERABLE_XML );

        Statement s = createStatement();
	s.execute(
             "create function lolzURL\n" +
             "(\n" +
             "    urlString varchar( 32672 ),\n" +
             "    rowTag varchar( 32672 ),\n" +
             "    childTags varchar( 32672 )...\n" +
             ")\n" +
             "returns table\n" +
             "(\n" +
             "        lolz varchar( 32000 )\n" +
             ")\n" +
             "language java parameter style derby_jdbc_result_set no sql\n" +
             "external name 'org.apache.derby.vti.XmlVTI.xmlVTIFromURL'\n"
             );
	s.execute(
             "create view lolzURL as\n" +
             "select * from table\n" +
             "(\n" +
             "    lolzURL\n" +
             "    (\n" +
             "        '" + traceURL.toString() + "',\n" +
             "        'lolz'\n" +
             "    )\n" +
             ") v\n"
             );
	try {
	        ResultSet rs = s.executeQuery( "select lolz from lolzURL" );
		assertTrue( rs.next() );

		// This next line will need to change once DERBY-6807 is fixed:
		fail( "Expected SAXParseException" );
	} catch ( Throwable e ) {
        if (!e.getMessage().contains("entity expansions")) {
            fail("Expected SAXParseException", e);
        }
	}
    }
}
