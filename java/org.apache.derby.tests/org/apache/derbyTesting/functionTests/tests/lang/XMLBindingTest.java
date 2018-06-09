/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.XMLBindingTest

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseJDBCTestSetup;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.XML;

/**
 * This test checks to make sure that the XML data type and
 * the corresponding XML operations all work as expected
 * from the JDBC side of things.  In particular, this test
 * verifies that 1) it is NOT possible to bind to/from an XML
 * datatype (because such an operation requires JDBC 4.0 and
 * is not yet supported by Derby), and 2) the correct behavior
 * occurs when null values (both Java and SQL) are bound
 * into the bindable parameters for the XML operators.
 *
 * This test also checks that insertion from XML files
 * via a character stream works, which is important since
 * XML files can be arbitrarily long and thus stream-based
 * processing is a must.
 */
public class XMLBindingTest extends BaseJDBCTestCase {

    /**
     * Public constructor required for running test as standalone JUnit.
     */
    public XMLBindingTest(String name)
    {
        super(name);
    }

    /**
     * Return a suite that runs a set of XML binding tests.  Only return
     * such a suite IF the testing classpath has the required XML classes.
     * Otherwise just return an empty suite.
     */
    public static Test suite()
    {
        BaseTestSuite suite = new BaseTestSuite("XML Binding Suite");
        if (XML.classpathMeetsXMLReqs())
        {
            /* "false" in the next line means that we will *not* clean the
             * database before the embedded and client suites.  This ensures
             * that we do not remove the objects created by XBindTestSetup.
             */
            Test test =
                TestConfiguration.defaultSuite(XMLBindingTest.class, false);

            test = new XBindTestSetup(test);

            /* XML parser needs to read "personal.dtd" for schema-based
             * insertion, so copy it to user directory.
             */
            test = new SupportFilesSetup(test,
                new String [] {
                    "functionTests/tests/lang/xmlTestFiles/personal.dtd"
                });

            // JEP 185 (http://openjdk.java.net/jeps/185) in Java SE 8 added
            // restrictions on access to external resources. This system
            // property loosens the restriction so that the XML parser is
            // allowed to read the DTD.
            test = SystemPropertyTestSetup.singleProperty(
                    test, "javax.xml.accessExternalDTD", "file");

            suite.addTest(test);
        }

        return suite;
    }

    /**
     * Performs a series of binding checks to make sure binding
     * to or from an XML value never works.
     */
    public void testInvalidXMLBindings() throws Exception
    {
        // Binding to an XML column.
        assertCompileError("42Z70", "insert into xTable.t1(x) values (?)");

        // Binding to an XML value in the XMLSERIALIZE operator.
        assertCompileError("42Z70", 
                "select XMLSERIALIZE(? AS CLOB) FROM XTABLE.T1");

        // Binding to an XML value in the XMLEXISTS operator.
        assertCompileError("42Z70", "select i from xTable.t1 where " +
                "XMLEXISTS('//*' PASSING BY REF ?)");

        // Binding to an XML value in the XMLQUERY operator.
        assertCompileError("42Z70", "select i from xTable.t1 where " +
                "XMLQUERY('//*' PASSING BY REF ? EMPTY ON EMPTY) is " +
                "not null");

        /* Make sure that attempts to bind _from_ XML will fail.
         * We should fail at compile time with an error saying
         * that XML values are not allowed in top-level result
         * sets (and thus cannot be bound).
         */
        assertCompileError("42Z71", "select x from xTable.t1");
    }

    /**
     * Test serialization of the XML values inserted as part
     * XBindTestSetup processing.  For the documents that are
     * are larger than 32K, this tests that they can be correctly
     * read from disk as a stream (instead of just as as string).
     */
    public void testXMLSerializeBinding() throws Exception
    {
        // Array of expected character counts for every row inserted
        // into xTable.t1 as part of XBindTestSetup setup.  A "0"
        // means empty string; a "-1" means we inserted a null.
        int [] expectedCharCounts =
            new int [] { 40228, 38712, 1948, 1942, 1967, 1709, 22, -1, -1 };

        int rowCount = 0;
        ResultSet rs = createStatement().executeQuery(
                "select i, XMLSERIALIZE(X AS CLOB) FROM xTable.t1");

        while (rs.next())
        {
            int charCount;
            java.io.Reader xResult = rs.getCharacterStream(2);

            // Count the number of characters we read back.
            if (!rs.wasNull())
            {
                int ch = xResult.read();
                for (charCount = 0; ch != -1; ch = xResult.read())
                {
                    /* Xalan serialization produces platform-specific line-
                     * endings (DERBY-2106), which can throw off the character
                     * count on Windows.  So if we see the Windows '\r' char
                     * we do not count it.
                     */
                    if ((char)ch != '\r')
                        charCount++;
                }
                xResult.close();
            }
            else
                charCount = -1;

            assertEquals("Unexpected serialized character count:",
                expectedCharCounts[rowCount], charCount);

            rowCount++;
        }

        assertEquals("Unexpected row count when serializing:",
            expectedCharCounts.length, rowCount);

        /* Test binding to the XMLSERIALIZE operand.  Since
         * the operand is an XML value, and since we don't
         * allow binding to an XML value (which is tested in
         * testInvalidXMLBindings()), there's nothing more to
         * to do here.
         */
    }

    /**
     * Run some simple XPath queries against the documents
     * inserted as part of XBindTestSetup to verify correct
     * functionality in insertion and XMLEXISTS.  Also test
     * binding of values into the first XMLEXISTS operator
     * (should fail).
     */
    public void testXMLExistsBinding() throws Exception
    {
        /* Test binding to the XMLEXISTS operands.  Binding
         * of the second (XML) operand is not allowed and was
         * checked in "testInvalidXMLBindings()" above.  Here we
         * check binding of the first operand, which should fail
         * because SQL/XML spec says the first operand must
         * be a string literal.
         */
        assertCompileError("42Z75", 
                "select i from xTable.t1 where " +
                "XMLEXISTS (? PASSING BY REF x)");

        // Run some sample queries.
        existsQuery("//abb", 1);
        existsQuery("//d50", 1);
        existsQuery("//person/email", 4);
        existsQuery("/personnel", 5);
        existsQuery("//person/@id", 4);

        /* This next one is important because it verifies
         * that implicit/default values which are defined
         * in a DTD _are_ actually processed, even though
         * we don't perform validation.  Thus this next
         * query _should_ return a match.
         */
        existsQuery("//person/@noteTwo", 1);
    }

    /**
     * Test binding of values into the first XMLQUERY operand
     * (should fail).
     */
    public void testXMLQueryBinding() throws Exception
    {
        /* Binding of the second (XML) operand is not allowed
         * and is checked as part of "testInvalidXMLBindings()".
         * Here we check binding of the first operand, which
         * should fail because SQL/XML spec says the first
         * operand must be a string literal.
         */
        assertCompileError("42Z75", 
                "select i from xTable.t1 where " +
                "XMLQUERY (? PASSING BY REF x EMPTY ON EMPTY) " +
                "is not null");
    }

    /**
     * Helper method.  Selects all rows (from xTable.t1) against which
     * evaluation of the received XPath expression returns a non-empty
     * sequence.  Evaluates the query using the XMLEXISTS operator and
     * then verifies that the number of rows matches the expected row
     * row count.
     *
     * @param xPath The XPath expression to evaluate.
     * @param expectedRows Number of rows for which we expect XMLEXISTS
     *  to return "true".
     */
    private void existsQuery(String xPath, int expectedRows)
        throws Exception
    {
        ResultSet rs = createStatement().executeQuery(
            "select i from xTable.t1 where " +
            "xmlexists('" + xPath + "' passing by ref x)");

        JDBC.assertDrainResults(rs, expectedRows);
    }

    /**
     * Helper class.  Creates a test table and populates it with data.
     * That data is then used throughout the various test methods that
     * are run in XMLBindingTest.
     */
    private static class XBindTestSetup extends BaseJDBCTestSetup
    {
        public XBindTestSetup(Test test) {
            super(test);
        }

        /**
         * Create the XML table and insert all of the test documents.
         * Some of the documents are small, others are larger than
         * 32K (which will test stream processing of XML data).  This
         * method is called as part of the XBindTestSetup because the
         * data is used throughout the test methods in XMLBindingTest.
         * That said, though, this method is itself a test, as well--
         * namley, it tests that XMLPARSE binding succeeds in all
         * of the cases where it is expected to succeed.
         */
        public void setUp() throws Exception
        {
            String tName = "xTable.t1";
            Connection c = getConnection();
            c.createStatement().execute("create table " + tName +
                 "(i int generated always as identity, x xml)");

            // Test parsing of > 32K XML documents.
            XML.insertFile(c, tName, "x", "wide40k.xml", 1);
            XML.insertFile(c, tName, "x", "deep40k.xml", 1);

            /* Test parsing of docs that use schemas.  Since DTDs
             * are stored in "{user.dir}/extin" we have to modify
             * the XML documents that use DTDs so that they can find
             * the DTD files.
             */

            XML.insertFile(c, tName, "x", "xsdDoc.xml", 1);
            XML.insertDocWithDTD(c, tName, "x",
                "dtdDoc.xml", "personal.dtd", 1);

            // XMLPARSE is not supposed to validate, so the following
            // inserts should SUCCEED, even though the documents
            // don't adhere to their schemas.

            XML.insertFile(c, tName, "x", "xsdDoc_invalid.xml", 1);
            XML.insertDocWithDTD(c, tName, "x",
                "dtdDoc_invalid.xml", "personal.dtd", 1);

            // Test simple binding to the XMLPARSE operand.

            PreparedStatement pSt = getConnection().prepareStatement(
                "insert into xTable.t1(x) values " +
                "(XMLPARSE (DOCUMENT CAST (? as CLOB) PRESERVE WHITESPACE))");

            // This should work.  Note we check binding via
            // a character stream method in XML.insertFile().

            pSt.setString(1, "<simple> doc </simple>");
            pSt.execute();

            // Null should work, too.  Make sure the inserts execute without
            // error here.  We'll verify the results as part of the testing
            // for XMLSERIALIZE.

            // Java null.
            pSt.setString(1, null);
            pSt.execute();

            // SQL null.
            pSt.setNull(1, Types.CLOB);
            pSt.execute();
            pSt.close();
            c = null;
        }

        /**
         * Just have to drop the table we created in setUp().
         */
        public void tearDown() throws Exception
        {
            getConnection().createStatement().execute("drop table xTable.t1");
            super.tearDown();
        }
    }
}
