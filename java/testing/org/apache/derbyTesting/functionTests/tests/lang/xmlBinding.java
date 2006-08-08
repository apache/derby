/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.xmlBinding

   Copyright 2005 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.functionTests.tests.lang;

import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;

/**
 * This class checks to make sure that the XML data type and
 * the corresponding XML operations all work as expected
 * from the JDBC side of things.  In particular, this test
 * verifies that 1) it is NOT possible to bind to/from an XML
 * datatype (because the JDBC specification doesn't indicate
 * how that should be done), and 2) the correct behavior
 * occurs when null values (both Java and SQL) are bound
 * into the bindable parameters for the XML operators.
 * This file also checks that insertion from XML files
 * via a character stream works, which is important since
 * XML files can be arbitrarily long and thus stream-based
 * processing is a must.
 */
public class xmlBinding
{
    /**
     * Create an instance of this class and do the test.
     */
    public static void main(String [] args)
    {
        new xmlBinding().go(args);
    }

    /**
     * Create a JDBC connection using the arguments passed
     * in from the harness, and then run the binding
     * tests.
     * @param args Arguments from the harness.
     */
    public void go(String [] args)
    {
        try {

            // use the ij utility to read the property file and
            // make the initial connection.
            ij.getPropertyArg(args);
            Connection conn = ij.startJBMS();

            // Create our test table.
            Statement st = conn.createStatement();
            st.execute("create table xTable.t1 " +
                "(i int generated always as identity, x xml)");

            // Do the tests.
            doBindTests(conn);
            doXMLParseTests(conn);
            doXMLSerializeTests(conn);
            doXMLExistsTests(conn);

            // Clean up.
            st.close();
            conn.close();

            System.out.println("[ Done. ]\n");

        } catch (Exception e) {

            System.out.println("Unexpected error: ");
            e.printStackTrace(System.out);

        }
    }

    /**
     * Performs a series of binding checks to make sure
     * binding to an XML value never works.
     * @param conn A connection to the test database.
     */
    private void doBindTests(Connection conn)
    {
        // Make sure that attempts to bind _to_ XML will fail.
        System.out.println("\n[ Beginning XML binding tests. ]\n");

        // Binding to an XML column.
        PreparedStatement pSt = null;
        try {

            // If we're running in embedded mode or else with
            // the Derby Client, then the next line will fail
            // because there is NO deferred prepare.  If, however,
            // we're running with JCC, the default is to defer
            // the prepare until execution, so the next line will
            // be fine, but the following four checks will fail.
            // This difference in behavior okay--it requires two
            // different masters, but ultimately it's a good way
            // to check behavior in both cases.
            pSt = conn.prepareStatement(
                "insert into xTable.t1(x) values (?)");

            System.out.print("XML column -- bind String to XML: ");
            bindAndExecute(pSt, 1, Types.VARCHAR, "shouldn't work", "X0X14", false);

            System.out.print("XML column -- bind Java null to XML: ");
               bindAndExecute(pSt, 1, Types.VARCHAR, null, "X0X14", false);

            System.out.print("XML column -- bind SQL NULL to XML: ");
            bindAndExecute(pSt, 1, Types.VARCHAR, null, "X0X14", true);

            System.out.print("XML column -- bind integer to XML: ");
            bindAndExecute(pSt, 1, Types.INTEGER, new Integer(8), "X0X14", false);

        } catch (SQLException se) {
            // Must be running with embedded or Derby Network Client.
            System.out.print("XML column -- insertion via parameter:  ");
            checkException(se, "X0X14");
        }

        // Binding to an XML value in the XMLSERIALIZE operator.
        // Should get compile-time error saying that 
        // parameters aren't allowed for XML data values.
        System.out.print("Trying to bind to XML in XMLSERIALIZE: ");
        try {
            pSt = conn.prepareStatement(
                "select XMLSERIALIZE(? AS CLOB) FROM XTABLE.T1");
            bindAndExecute(pSt, 1, Types.VARCHAR, null, "X0X14", true);
        } catch (SQLException se) {
            checkException(se, "X0X14");
        }

        // Binding to an XML value in the XMLEXISTS operator.
        // Should get compile-time error saying that 
        // parameters aren't allowed for XML data values.
        System.out.print("Trying to bind to XML in XMLEXISTS: ");
        try {
            pSt = conn.prepareStatement(
                "select i from xTable.t1 where " +
                "XMLEXISTS('//*' PASSING BY REF ?)");
            bindAndExecute(pSt, 1, Types.VARCHAR, null, "X0X14", true);
        } catch (SQLException se) {
            checkException(se, "X0X14");
        }

        // Make sure that attempts to bind _from_ XML will fail.
        // We should fail at compile time, even before
        // we get a chance to execute the query.
        System.out.print("XML value in result set: ");
        try {
            pSt = conn.prepareStatement("select x from xTable.t1");
            pSt.execute();
        } catch (SQLException se) {
            checkException(se, "X0X15");
        }

        System.out.println("\n[ End XML binding tests. ]\n");
    }

    /**
     * Test insertion of documents larger than 32K (this
     * will test stream processing of XML data), and
     * test binding of null values in the XMLPARSE
     * operator.
     * @param conn A connection to the test database.
     */
    private void doXMLParseTests(Connection conn)
    {
        System.out.println("\n[ Beginning XMLPARSE tests. ]\n");

        System.out.println("Test insertions from file: ");
        try { 

            // Test parsing of > 32K XML documents.
            insertFiles(conn, "xTable.t1", "xmlTestFiles/wide40k.xml", 1);
            insertFiles(conn, "xTable.t1", "xmlTestFiles/deep40k.xml", 1);

            // Test parsing of docs that use schemas.  Since server
            // and client tests run in a subdirectory, we have to modify
            // the XML documents that use DTDs so that they can find
            // the DTD files.

            insertDocWithDTD(conn, "xTable.t1", "xmlTestFiles/dtdDoc.xml",
                "personal.dtd", 1);
            insertFiles(conn, "xTable.t1", "xmlTestFiles/xsdDoc.xml", 1);

            // XMLPARSE is not supposed to validate, so the following
            // inserts should SUCCEED, even though the documents
            // don't adhere to their schemas.
            insertDocWithDTD(conn, "xTable.t1",
                "xmlTestFiles/dtdDoc_invalid.xml", "personal.dtd", 1);
            insertFiles(conn, "xTable.t1",
                "xmlTestFiles/xsdDoc_invalid.xml", 1);

            System.out.println("--> Insertions all PASS.");

        } catch (SQLException se) {
            System.out.println("FAIL: Unexpected exception: ");
            while (se != null) {
                se.printStackTrace(System.out);
                se = se.getNextException();
            }
        } catch (Exception e) {
            System.out.println("FAIL: Unexpected exception: ");
            e.printStackTrace(System.out);
        }

        // Test binding nulls to the XMLPARSE operand.

        try {

            PreparedStatement pSt = conn.prepareStatement(
                "insert into xTable.t1(x) values " +
                "(XMLPARSE (DOCUMENT ? PRESERVE WHITESPACE))");

            // This should work.  Note we check binding to
            // a character stream method in "insertFiles".
            System.out.print("Binding string in XMLPARSE: ");
            bindAndExecute(pSt, 1, Types.CHAR, "<simple> doc </simple>",
                null, false);

            // Null should work, too.
            System.out.print("Binding Java null string in XMLPARSE: ");
            bindAndExecute(pSt, 1, Types.CHAR, null, null, false);
            System.out.print("Binding SQL NULL string in XMLPARSE: ");
            bindAndExecute(pSt, 1, Types.CLOB, null, null, true);

        } catch (Exception e) {
            System.out.println("Unexpected exception: ");
            e.printStackTrace(System.out);
        }

        System.out.println("\n[ End XMLPARSE tests. ]\n");
    }

    /**
     * Test serialization of the XML values inserted by
     * the doXMLParseTests() method above.  For the documents
     * that are larger than 32K, this tests that they can
     * be correctly read from disk as a stream (instead of
     * just as as string).
     * @param conn A connection to the test database.
     */
    private void doXMLSerializeTests(Connection conn)
    {
        System.out.println("\n[ Beginning XMLSERIALIZE tests. ]\n");

        try {

            PreparedStatement pSt = conn.prepareStatement(
                "select i, XMLSERIALIZE(X AS CLOB) FROM xTable.t1");
            ResultSet rs = pSt.executeQuery();

            String xResult = null;
            int rowCount = 0;
            while (rs.next()) {
                xResult = rs.getString(2);
                if (!rs.wasNull()) {
                    System.out.println(rs.getInt(1) + ", " +
                        "[ roughly " + (xResult.length() / 1000) + "k ]");
                }
                else
                    System.out.println(rs.getInt(1) + ", NULL");
                rowCount++;
            }

        } catch (Exception e) {
            System.out.println("Unexpected exception: ");
            e.printStackTrace(System.out);
        }

        // Test binding to the XMLSERIALIZE operand.  Since
        // the operand is an XML value, and since we don't
        // allow binding to an XML value (see "doBindTests()"
        // above), there's nothing more to do here.

        System.out.println("\n[ End XMLSERIALIZE tests. ]\n");
    }

    /**
     * Run some simple XPath queries against the documents
     * inserted in doXMLParseTests() above, and then test
     * binding of null values in the XMLEXISTS operator.
     * @param conn A connection to the test database.
     */
    private void doXMLExistsTests(Connection conn)
    {
        System.out.println("\n[ Begin XMLEXISTS tests. ]\n");

        // Run some sample queries.
        try {

            existsQuery(conn, "xTable.t1", "//abb");
            existsQuery(conn, "xTable.t1", "//d50");
            existsQuery(conn, "xTable.t1", "//person/email");
            existsQuery(conn, "xTable.t1", "/personnel");
            existsQuery(conn, "xTable.t1", "//person/@id");

            // This next one is important because it verifies
            // that implicit/default values which are defined
            // in a DTD _are_ actually processed, even though
            // we don't perform validation.  Thus this next
            // query _should_ return a match.
            int rowCount = existsQuery(conn, "xTable.t1", "//person/@noteTwo");
            if (rowCount == 0) {
                System.out.println("FAILED: Query on DTD default didn't " +
                    "return any matches.");
            }

        } catch (Exception e) {
            System.out.println("Unexpected exception: ");
            e.printStackTrace(System.out);
        }

        // Test binding to the XMLEXISTS operands.  Binding
        // of the second (XML) operand is not allowed and was
        // checked in "doBindTests()" above.  Here we check
        // binding of the first operand, which should fail
        // because SQL/XML spec says the first operand must
        // be a string literal.
        try {

            System.out.print("Parameter as first operand in XMLEXISTS: ");

            // If we're running in embedded mode or else with
            // the Derby Client, then the next line will fail
            // because there is NO deferred prepare.  If, however,
            // we're running with JCC, the default is to defer
            // the prepare until execution, so the next line will
            // be fine, but the subsequent "execute" should fail.
            PreparedStatement pSt = conn.prepareStatement(
                "select i from xTable.t1 where " +
                "XMLEXISTS (? PASSING BY REF x)");
            pSt.setString(1, "//*");
            pSt.execute();

        } catch (SQLException se) {
            checkException(se, "X0X19");
        }

        System.out.println("\n[ End XMLEXISTS tests. ]\n");
    }

    /**
     * Helper method.  Inserts the contents of a file into
     * the received table using "setCharacterStream".
     * @param conn A connection to the test database.
     * @param tableName Name of the target table
     * @param fName Name of the file whose content we
     *  want to insert.
     * @param numRows Number of times we should insert
     *  the received file's content.
     */
    private void insertFiles(Connection conn, 
        String tableName, String fName, int numRows)
        throws Exception
    {
        // First we have to figure out many chars long the
        // file is.
        InputStream iS = this.getClass().getResourceAsStream(fName);
        InputStreamReader reader = new InputStreamReader(iS);
        char [] cA = new char[1024];
        int charCount = 0;
        for (int len = reader.read(cA, 0, cA.length); len != -1;
            charCount += len, len = reader.read(cA, 0, cA.length));

        reader.close();

        // Now that we know the number of characters, we can
        // insert using a stream.

        PreparedStatement pSt = conn.prepareStatement(
            "insert into xTable.t1(x) values (" +
            "xmlparse(document ? preserve whitespace))");

        for (int i = 0; i < numRows; i++) {

            iS = this.getClass().getResourceAsStream(fName);
            reader = new InputStreamReader(iS);
            pSt.setCharacterStream(1, reader, charCount);
            pSt.execute();
            reader.close();
            System.out.println("Inserted roughly " +
                (charCount / 1000) + "k of data.");

        }
    }

    /**
     * Helper method.  Inserts an XML document into the
     * received table using setString.  This method
     * parallels "insertFiles" above, except that it
     * should be used for documents that require a DTD
     * in order to be complete.  In that case, the
     * location of the DTD has to modified _in_ the
     * document so that it can be found regardless of
     * whether we're running in embedded mode or in
     * server/client mode.
     * @param conn A connection to the test database.
     * @param tableName Name of the target table
     * @param fName Name of the file whose content we
     *  want to insert.
     * @param dtdName Name of the DTD file that the
     *  received file uses.
     * @param numRows Number of times we should insert
     *  the received file's content.
     */
    private void insertDocWithDTD(Connection conn, 
        String tableName, String fName, String dtdName,
        int numRows) throws Exception
    {
        boolean needsUpdate = true;
        String currPath = System.getProperty("user.dir");
        String fileSep = System.getProperty("file.separator");

        String dtdPath = currPath;
        boolean foundDTD = false;
        while (!foundDTD) {

            try {

                FileReader fR = new FileReader(dtdPath +
                    fileSep + dtdName);

                // If we get here, then we found the DTD in
                // the current path, so we're done.
                foundDTD = true;
                dtdPath = "file:///" + dtdPath + fileSep + dtdName;
                break;

            } catch (java.io.IOException ie) {

                // Couldn't find the DTD in the current path.
                // The harness uses a lot of subdirectories when
                // running tests (for client, or server, or
                // suites, or nested suites...etc.), so we
                // back up one directory and try again.

                int pos = dtdPath.lastIndexOf(fileSep);
                if (pos == -1) {
                // we're at the top of the path and haven't
                // found the DTD yet.  This shouldn't happen.
                    throw new Exception("Couldn't find DTD '" +
                        dtdName + "' for insertion of file '" +
                        fName + "'.");
                }
                dtdPath = dtdPath.substring(0, pos);

            }
        }

        // Read the file into memory so we can update it.
        InputStream iS = this.getClass().getResourceAsStream(fName);
        InputStreamReader reader = new InputStreamReader(iS);
        char [] cA = new char[1024];
        StringBuffer sBuf = new StringBuffer();
        int charCount = 0;
        for (int len = reader.read(cA, 0, cA.length); len != -1;
            charCount += len, len = reader.read(cA, 0, cA.length))
        {
            sBuf.append(cA, 0, len);
        }

        reader.close();

        // Now replace the DTD location, if needed.
        String docAsString = sBuf.toString();
        int pos = docAsString.indexOf(dtdName);
        if (pos != -1)
            sBuf.replace(pos, pos + dtdName.length(), dtdPath);

        // Now (finally) do the insert using the in-memory
        // document with the correct DTD location.
        docAsString = sBuf.toString();
        PreparedStatement pSt = conn.prepareStatement(
            "insert into xTable.t1(x) values (" +
            "xmlparse(document ? preserve whitespace))");

        charCount = docAsString.length();
        for (int i = 0; i < numRows; i++) {

            pSt.setString(1, docAsString);
            pSt.execute();
            System.out.println("Inserted roughly " +
                (charCount / 1000) + "k of data.");

        }
    }

    /**
     * Helper method.  Selects all rows from the received
     * table name that have at least one node matching
     * the received XPath expression.  Does this query
     * using the XMLEXISTS operator.
     * @param conn A connection to the test database.
     * @param tableName Table to query.
     * @param xPath The XPath expression to evaluate.
     * @return The number of rows that match the
     *  XPath expression.
     */
    private int existsQuery(Connection conn,
        String tableName, String xPath) throws Exception
    {
        PreparedStatement pSt = conn.prepareStatement(
            "select i from " + tableName + " where " +
            "xmlexists('" + xPath + "' passing by ref x)");

        System.out.println("Running XMLEXISTS with: " + xPath);
        ResultSet rs = pSt.executeQuery();
        String xResult = null;
        int rowCount = 0;
        while (rs.next()) {
            rowCount++;
        }

        System.out.println("--> Matching rows: " + rowCount);
        return rowCount;
    }

    /**
     * Helper method.  Attempts to bind a parameter to a
     * given value using the given type, and then prints
     * the result of that attempt (PASS/FAIL).
     * @param pSt The prepared statement holding the parameter
     *  that we want to bind.
     * @param paramNum Which parameter in pSt we want to bind.
     * @param paramType The type of the value to be bound.
     * @param bindValue The value to be used for binding.
     * @param sqlState The expected SQLState for the binding
     *  error, if one is expected.  Null if the bind is expected
     *  to succeed.
     * @param bindSqlNull True if we should bind using a SQL
     *  NULL (i.e. "setNull()").
     */
    private void bindAndExecute(PreparedStatement pSt, int paramNum,
        int paramType, Object bindValue, String sqlState,
        boolean bindSqlNull)
    {
        SQLException actualException = null;
        try {

            // First try to bind.
            if (bindSqlNull) {
                pSt.setNull(paramNum, paramType);
            }
            else {
                switch (paramType)
                {
                    case Types.CHAR:
                    case Types.VARCHAR:

                        pSt.setString(paramNum, (String)bindValue);
                        break;

                    case Types.INTEGER:

                        pSt.setInt(paramNum, ((Integer)bindValue).intValue());
                        break;

                    default:

                        System.out.println("ERROR: Unexpected bind type (" +
                            paramType + ") in call to doBind.");
                        break;
                }
            }

            // Now try to execute.
            pSt.execute();

        } catch (SQLException e) {
            actualException = e;
        }

        checkException(actualException, sqlState);
    }

    /**
     * Helper method.  Checks to see if the received SQLException
     * has a SQLState that matches the target/expected SQLState.
     * Prints out a message saying the result of this check, and
     * in the case where the actual error is NOT the expected
     * error, prints a full stack trace to System.out.
     * @param se The SQLException to be checked.
     * @param targetState The expected SQLState; null if no
     *  error was expected.
     */
    private void checkException(SQLException se,
        String targetState)
    {
        if (targetState == null) {
            if (se == null) {
                System.out.println("PASS -- Completed without exception, " +
                    "as expected.");
            }
            else {
                System.out.println("FAIL -- Was expected to succeed, but " +
                    "failed with error " + se.getSQLState() + ".");
                se.printStackTrace(System.out);
            }
            return;
        }

        if (se == null) {
            System.out.println("FAIL -- Completed without exception when " +
                "error " + targetState + " was expected.");
            return;
        }

        if (!targetState.equals(se.getSQLState())) {
            System.out.println("FAIL: Caught error " + se.getSQLState() +
                " when was expecting error " + targetState + ".");
            se.printStackTrace(System.out);
            return;
        }

        System.out.println("PASS -- caught expected error " +
            targetState + ".");
    }
}
