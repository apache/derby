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
import org.apache.derby.iapi.reference.SQLState;

/**
 * Test sequences.
 */
public class SequenceTest extends GeneratedColumnsHelper {

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

    public void test_01_CreateSequence() throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("CREATE SEQUENCE mySeq");
    }

    public void test_02_DropSequence() throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("CREATE SEQUENCE mySeq1");
        s.executeUpdate("DROP SEQUENCE mySeq1");
    }

    public void test_03_DuplicateCreationFailure() throws SQLException {
        Statement s = null;
        try {
            s = createStatement();
            s.executeUpdate("CREATE SEQUENCE mySeq1");
            s.executeUpdate("CREATE SEQUENCE mySeq1");
        } catch (SQLException sqle) {
            assertSQLState("X0Y68", sqle);
        }finally{
            s.executeUpdate("DROP SEQUENCE mySeq1"); // Drop the one created.
        }
    }

    public void test_04_ImplicitSchemaCreation() throws SQLException {
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

    public void test_05CreateWithSchemaSpecified() throws SQLException {

        // create DB
        Connection alphaCon = openUserConnection(ALPHA);
        Statement stmt = alphaCon.createStatement();

        // should implicitly create schema ALPHA
        stmt.executeUpdate("CREATE SEQUENCE alpha.alpha_seq");
        stmt.executeUpdate("DROP SEQUENCE alpha.alpha_seq");
        stmt.close();
        alphaCon.close();
    }

    public void test_06_CreateWithSchemaSpecifiedCreateTrue() throws SQLException {
        Connection alphaCon = openUserConnection(ALPHA);
        Statement stmt = alphaCon.createStatement();

        // should implicitly create schema ALPHA
        stmt.executeUpdate("CREATE SEQUENCE alpha.alpha_seq");
        stmt.executeUpdate("DROP SEQUENCE alpha.alpha_seq");
        stmt.close();
        alphaCon.close();
    }

    public void test_07_CreateWithSchemaDropWithNoSchema() throws SQLException {
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
    public void test_08_DropOtherSchemaSequence() throws SQLException {
        Connection adminCon = openUserConnection(TEST_DBO);

        Connection alphaCon = openUserConnection(ALPHA);
        Statement stmtAlpha = alphaCon.createStatement();
        stmtAlpha.executeUpdate("CREATE SEQUENCE alpha_seq");

        Connection betaCon = openUserConnection(BETA);
        Statement stmtBeta = betaCon.createStatement();

        // should implicitly create schema ALPHA
        assertStatementError("42507", stmtBeta, "DROP SEQUENCE alpha.alpha_seq");

        // Cleanup:
        stmtAlpha.executeUpdate("DROP SEQUENCE alpha_seq");
        
        stmtAlpha.close();
        stmtBeta.close();
        alphaCon.close();
        betaCon.close();
        adminCon.close();
    }

    /**
     * Test trying to create a sequence in a schema that doesn't belong to one
     */
    public void test_09_CreateOtherSchemaSequence() throws SQLException {
        // create DB
        Connection adminCon = openUserConnection(TEST_DBO);

        Connection alphaCon = openUserConnection(ALPHA);
        Statement stmtAlpha = alphaCon.createStatement();
        stmtAlpha.executeUpdate("CREATE SEQUENCE alpha_seq");

        Connection betaCon = openUserConnection(BETA);
        Statement stmtBeta = betaCon.createStatement();

        // should implicitly create schema ALPHA
        assertStatementError("42507", stmtBeta, "CREATE SEQUENCE alpha.alpha_seq3");

        // Cleanup:
        stmtAlpha.executeUpdate("DROP SEQUENCE alpha_seq");
        
        stmtAlpha.close();
        stmtBeta.close();
        alphaCon.close();
        betaCon.close();
        adminCon.close();
    }

    public void testCreateSequenceWithArguments() throws Exception {
        Connection alphaCon = openUserConnection(ALPHA);

        goodStatement(alphaCon,
                "CREATE SEQUENCE small1 AS SMALLINT START WITH 0 INCREMENT BY 1");

        goodStatement(alphaCon,
                "CREATE SEQUENCE small2 AS SMALLINT START WITH " + Short.MIN_VALUE
                        + " MAXVALUE " + Short.MAX_VALUE);

        goodStatement(alphaCon,
                "CREATE SEQUENCE small3 AS SMALLINT START WITH 1200"
                        + " INCREMENT BY -5 MAXVALUE 32000 NO MINVALUE CYCLE");

        // maxvalue out of range
        expectCompilationError(alphaCon,
                SQLState.LANG_SEQ_ARG_OUT_OF_DATATYPE_RANGE,
                "CREATE SEQUENCE small3 AS SMALLINT START WITH " + Short.MIN_VALUE
                        + " MAXVALUE " + Integer.MAX_VALUE);

         // start with out of range
        expectCompilationError(alphaCon,
                SQLState.LANG_SEQ_INVALID_START,
                "CREATE SEQUENCE small4 AS SMALLINT START WITH " + Integer.MIN_VALUE);

        // minvalue larger than maxvalue negative
        expectCompilationError(alphaCon,
                SQLState.LANG_SEQ_MIN_EXCEEDS_MAX,
                "CREATE SEQUENCE small5 AS SMALLINT MAXVALUE -20000 MINVALUE -1");

        goodStatement(alphaCon,
                "CREATE SEQUENCE int1 AS INTEGER START WITH " + Integer.MIN_VALUE + " INCREMENT BY -10 CYCLE");

        goodStatement(alphaCon,
                "CREATE SEQUENCE int2 AS INTEGER INCREMENT BY 5"
                        + " MAXVALUE " + Integer.MAX_VALUE);

        goodStatement(alphaCon,
                "CREATE SEQUENCE int3 AS INTEGER START WITH 1200 INCREMENT BY 5 "
                        + "NO MAXVALUE MINVALUE -320000 CYCLE");

        // minvalue out of range
        expectCompilationError(alphaCon,
                SQLState.LANG_SEQ_ARG_OUT_OF_DATATYPE_RANGE,
                "CREATE SEQUENCE int4 AS INTEGER START WITH " + Integer.MIN_VALUE
                        + " MAXVALUE " + Short.MAX_VALUE
                        + " MINVALUE " + Long.MIN_VALUE);

        // increment out of range
        expectCompilationError(alphaCon,
                SQLState.LANG_SEQ_INCREMENT_OUT_OF_RANGE,
                "CREATE SEQUENCE int5 AS INTEGER INCREMENT BY " + Long.MAX_VALUE);

        // increment 0
        expectCompilationError(alphaCon,
                SQLState.LANG_SEQ_INCREMENT_ZERO,
                "CREATE SEQUENCE int5 AS INTEGER INCREMENT BY 0");

       // increment too big
        expectCompilationError(alphaCon,
                SQLState.LANG_SEQ_INCREMENT_OUT_OF_RANGE,
                "CREATE SEQUENCE int6 AS INTEGER INCREMENT BY " + Long.MAX_VALUE);

        goodStatement(alphaCon,
                "CREATE SEQUENCE long1 AS BIGINT START WITH " + Long.MIN_VALUE + " INCREMENT BY -100 NO CYCLE");

        goodStatement(alphaCon,
                "CREATE SEQUENCE long2 AS BIGINT INCREMENT BY 25"
                        + " MAXVALUE " + Integer.MAX_VALUE);

        goodStatement(alphaCon,
                "CREATE SEQUENCE long3 AS BIGINT START WITH 0 INCREMENT BY 5 "
                        + "NO MAXVALUE MINVALUE " + Long.MIN_VALUE + " CYCLE");

        // invalid minvalue
        expectCompilationError(alphaCon,
                SQLState.LANG_SEQ_ARG_OUT_OF_DATATYPE_RANGE,
                "CREATE SEQUENCE long4 AS BIGINT START WITH " + Integer.MAX_VALUE
                        + " MINVALUE " + Long.MAX_VALUE);

        // minvalue larger than maxvalue
        expectCompilationError(alphaCon,
                SQLState.LANG_SEQ_MIN_EXCEEDS_MAX,
                "CREATE SEQUENCE long5 AS BIGINT START WITH 0 MAXVALUE 100000 MINVALUE 100001");

        // should fail for non-int TYPES
        expectCompilationError(alphaCon,
                SQLState.LANG_SYNTAX_ERROR,
                "CREATE SEQUENCE char1 AS CHAR INCREMENT BY 1");

    }

    /**
     * initial test for next value
     * @throws SQLException on error
     */
    public void test_10_NextValue() throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("CREATE SEQUENCE mySeq1");
        s.execute("SELECT NEXT VALUE FOR mySeq1 from sys.systables");
    }


}
