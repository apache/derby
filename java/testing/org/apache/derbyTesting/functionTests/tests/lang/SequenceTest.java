/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.SequenceTest

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
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test sequences.
 */
public class SequenceTest extends BaseJDBCTestCase {

    private static final String TEST_DBO = "TEST_DBO";
    private static final String ALPHA = "ALPHA";
    private static final String BETA = "BETA";
    private static final String[] LEGAL_USERS = {TEST_DBO, ALPHA, BETA};

    public SequenceTest(String name) {
        super(name);
        // TODO Auto-generated constructor stub
    }

    // SETUP

    /**
     * Construct top level suite in this JUnit test
     */
    public static Test suite() {
        TestSuite suite = new TestSuite(SequenceTest.class, "Sequence Test");
        // Need atleast JSR169 to run these tests
        if (!JDBC.vmSupportsJSR169() && !JDBC.vmSupportsJDBC3()) {
            return suite;
        }

        Test cleanTest = new CleanDatabaseTestSetup(suite);
        Test authenticatedTest = DatabasePropertyTestSetup.builtinAuthentication
                (cleanTest, LEGAL_USERS, "sequence");
        Test authorizedTest = TestConfiguration.sqlAuthorizationDecorator(authenticatedTest);

        return authorizedTest;
    }

    public void testCreateSequence() throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("CREATE SEQUENCE mySeq");
    }

    public void testDropSequence() throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("CREATE SEQUENCE mySeq1");
        s.executeUpdate("DROP SEQUENCE mySeq1");
    }

    public void testDuplicateCreationFailure() {
        try {
            Statement s = createStatement();
            s.executeUpdate("CREATE SEQUENCE mySeq1");
            s.executeUpdate("CREATE SEQUENCE mySeq1");
        } catch (SQLException sqle) {
            assertSQLState("X0Y68", sqle);
        }
    }

    public void testImplicitSchemaCreation() throws SQLException {
        Connection adminCon = openUserConnection(TEST_DBO);

        Connection alphaCon = openUserConnection(ALPHA);
        Statement stmt = alphaCon.createStatement();

        // should implicitly create schema ALPHA
        stmt.executeUpdate("CREATE SEQUENCE alpha_seq");
        stmt.executeUpdate("DROP SEQUENCE alpha_seq");
        stmt.close();
        alphaCon.close();
        adminCon.close();
    }

    public void testCreateWithSchemaSpecified() throws SQLException {

        // create DB
        Connection alphaCon = openUserConnection(ALPHA);
        Statement stmt = alphaCon.createStatement();

        // should implicitly create schema ALPHA
        stmt.executeUpdate("CREATE SEQUENCE alpha.alpha_seq");
        stmt.executeUpdate("DROP SEQUENCE alpha.alpha_seq");
        stmt.close();
        alphaCon.close();
    }

    public void testCreateWithSchemaSpecifiedCreateTrue() throws SQLException {
        Connection alphaCon = openUserConnection(ALPHA);
        Statement stmt = alphaCon.createStatement();

        // should implicitly create schema ALPHA
        stmt.executeUpdate("CREATE SEQUENCE alpha.alpha_seq");
        stmt.executeUpdate("DROP SEQUENCE alpha.alpha_seq");
        stmt.close();
        alphaCon.close();
    }

    public void testCreateWithSchemaDropWithNoSchema() throws SQLException {
        Connection alphaCon = openUserConnection(ALPHA);
        Statement stmt = alphaCon.createStatement();

        // should implicitly create schema ALPHA
        stmt.executeUpdate("CREATE SEQUENCE alpha.alpha_seq");
        stmt.executeUpdate("DROP SEQUENCE alpha_seq");
        stmt.close();
        alphaCon.close();
    }

    /**
     * Test trying to drop a sequence in a schema that doesn't belong to one
     */
    public void testDropOtherSchemaSequence() throws SQLException {
        Connection adminCon = openUserConnection(TEST_DBO);

        Connection alphaCon = openUserConnection(ALPHA);
        Statement stmtAlpha = alphaCon.createStatement();
        stmtAlpha.executeUpdate("CREATE SEQUENCE alpha_seq");

        Connection betaCon = openUserConnection(BETA);
        Statement stmtBeta = betaCon.createStatement();

        // should implicitly create schema ALPHA
        assertStatementError("42507", stmtBeta, "DROP SEQUENCE alpha.alpha_seq");

        stmtAlpha.close();
        stmtBeta.close();
        alphaCon.close();
        betaCon.close();
        adminCon.close();
    }

    /**
     * Test trying to create a sequence in a schema that doesn't belong to one
     */
    public void testCreateOtherSchemaSequence() throws SQLException {
        // create DB
        Connection adminCon = openUserConnection(TEST_DBO);

        Connection alphaCon = openUserConnection(ALPHA);
        Statement stmtAlpha = alphaCon.createStatement();

        Connection betaCon = openUserConnection(BETA);
        Statement stmtBeta = betaCon.createStatement();

        // should implicitly create schema ALPHA
        assertStatementError("42507", stmtBeta, "CREATE SEQUENCE alpha.alpha_seq3");

        stmtAlpha.close();
        stmtBeta.close();
        alphaCon.close();
        betaCon.close();
        adminCon.close();
    }

}
