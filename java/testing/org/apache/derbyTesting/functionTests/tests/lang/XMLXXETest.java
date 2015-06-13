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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Types;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseJDBCTestSetup;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
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

        suite.addTest(TestConfiguration.defaultSuite(XMLXXETest.class));

        return SecurityManagerSetup.noSecurityManager(suite);
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

        s.execute("INSERT INTO xml_data(xml_col) VALUES(XMLPARSE(DOCUMENT" 
                + "'<!DOCTYPE foo [<!ENTITY xxe SYSTEM \"file:"
	        + File.separator    
                + path +"\" >]><yolo>&xxe;</yolo>'"
                + "PRESERVE WHITESPACE))");

	// XXX: The next result is wrong. The expected behavior is that the
	// query should be rejected as a security violation. See DERBY-6807
	// for more details; when that issue is resolved, this test will need
	// to be changed.

        JDBC.assertSingleValueResultSet(
                s.executeQuery(
	            "SELECT XMLSERIALIZE(xml_col AS CLOB) FROM xml_data"),
	            "<yolo>HelloWorld</yolo>");

        password.delete();
    }

    public void testDerby6807BillionLaughs() throws SQLException
    {
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

}
