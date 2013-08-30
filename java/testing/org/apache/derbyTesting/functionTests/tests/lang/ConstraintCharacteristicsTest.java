/*

 Derby - Class org.apache.derbyTesting.functionTests.tests.lang.ConstraintCharacteristicsTest

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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.sql.GenericPreparedStatement;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;

public class ConstraintCharacteristicsTest extends BaseJDBCTestCase
{
    public ConstraintCharacteristicsTest(String name)      {
        super(name);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite("ConstraintCharacteristicsTest");
        suite.addTest(new ConstraintCharacteristicsTest("testSyntaxAndBinding"));

        // Need to set a property to allow non default characteristics until
        // feature completed: remove then
        Properties systemProperties = new Properties();
        systemProperties.setProperty("derby.constraintsTesting", "true");

        TestSuite s = new TestSuite("WithLenientChecking");
        s.addTest(new ConstraintCharacteristicsTest(
                "testCreateConstraintDictionaryEncodings"));
        s.addTest(new ConstraintCharacteristicsTest(
                "testAlterConstraintDictionaryEncodings"));
        s.addTest(new ConstraintCharacteristicsTest(
                "testAlterConstraintInvalidation"));
        
        suite.addTest(
            new SystemPropertyTestSetup(
                s,
                systemProperties));

        return suite;
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        createStatement().
                executeUpdate("create table referenced(i int primary key)");
        setAutoCommit(false);
    }

    @Override
    protected void tearDown() throws Exception {
        rollback();
        setAutoCommit(true);
        getConnection().createStatement().
                executeUpdate("drop table referenced");
        super.tearDown();
    }

    public void testSyntaxAndBinding() throws SQLException {
        Statement s = getConnection().createStatement();

        /*
         * T A B L E    L E V E L    C O N S T R A I N T S
         */

        assertTableLevelDefaultBehaviorAccepted(s);
        assertTableLevelFailTillFeatureImplemented(s);

        /*
         * A L T E R    C O N S T R A I N T    C H A R A C T E R I S T I C S
         */
        s.executeUpdate("create table t(i int, constraint app.c primary key(i))");

        // default, so allow (nil action for now)
        s.executeUpdate("alter table t alter constraint c enforced");

        // not default behavior, so expect error until feature implemented
        assertStatementError("0A000", s, "alter table t alter constraint c not enforced");

        for (String ch : illegalAlterCharacteristics) {
            // Anything beyond enforcement is illegal in ALTER context
            assertStatementError("42X01", s, "alter table t alter constraint c " + ch);
        }

        // Unknown constraint name
        assertStatementError("42X86", s, "alter table t alter constraint cuckoo not enforced");

        /*
         * S E T   C O N S T R A I N T
         */

        assertStatementError("0A000", s, "set constraints c deferred");
        assertStatementError("0A000", s, "set constraints all deferred");

        // Unknown constraint name
        assertStatementError( "42X94", s, "set constraints cuckoo deferred");
        rollback();

        /*
         * C O L U M N    L E V E L    C O N S T R A I N T S
         */

        assertColumnLevelDefaultBehaviorAccepted(s);
        assertColumnLevelFailTillFeatureImplemented(s);

        // Characteristics are not currently allowed for NOT NULL,
        // since Derby does not represent NOT NULL as a constraint,
        // but rather as an aspect of the column's data type. It is
        // possible to alter the column nullable and vice versa,
        // though.
        assertStatementError("42XAN", s,
                    "create table t(i int " +
                    "not null deferrable initially immediate)");
    }

    /**
     * Check that constraint characteristics are correctly encoded
     * into the STATE column in SYS.SYSCONSTRAINTS.
     * Cf. specification attached to DERBY-532.
     *
     * FIXME: Note that this test runs with property derby.constraintsTesting
     * to bypass NOT IMPLEMENTED checks.  Remove this property usage when
     * DERBY-532 is done.
     * @throws SQLException
     */
    public void testCreateConstraintDictionaryEncodings() throws SQLException {
        Statement s = getConnection().createStatement();

        for (String[] ch : defaultCharacteristics) {
            assertDictState(s, ch[0], ch[1]);
        }

        for (String[] ch : nonDefaultCharacteristics) {
            assertDictState(s, ch[0], ch[1]);
        }

        for (String ch : illegalCharacteristics) {
            assertCreateInconsistentCharacteristics(s, ch);
        }

        rollback();
    }

    /**
     * Check that constraint characteristics are correctly encoded
     * into the STATE column in SYS.SYSCONSTRAINTS.
     * Cf. specification attached to DERBY-532.
     *
     * FIXME: Note that this test runs with property derby.constraintsTesting
     * to bypass NOT IMPLEMENTED checks.  Remove this property usage when
     * DERBY-532 is done.
     * @throws SQLException
     */
    public void testAlterConstraintDictionaryEncodings() throws SQLException {
        Statement s = getConnection().createStatement();

        for (String[] ch : defaultCharacteristics) {
            s.executeUpdate(
                    "create table t(i int, constraint c primary key(i) " +
                    ch[0] + ")");

            assertAlterDictState(s, "enforced");
            assertAlterDictState(s, "not enforced");
            rollback();
        }

        for (String[] ch : nonDefaultCharacteristics) {
            s.executeUpdate(
                    "create table t(i int, constraint c primary key(i) " +
                    ch[0] + ")");

            assertAlterDictState(s, "enforced");
            assertAlterDictState(s, "not enforced");
            rollback();
        }

        for (String ch : illegalAlterCharacteristics) {
            assertAlterInconsistentCharacteristics(s, ch);
        }
    }


    /**
     * Check that altering constraint characteristics invalidates prepared
     * statements.
     * @throws SQLException
     */
    public void testAlterConstraintInvalidation() throws SQLException {
        Connection c = getConnection();
        Statement s = c.createStatement();
        
        s.executeUpdate("create table t(i int, constraint c primary key(i))");
        PreparedStatement ps = c.prepareStatement("insert into t values 3");
        ps.execute();

        s.executeUpdate("alter table t alter constraint c not enforced ");
        
        ContextManager contextManager = 
                ((EmbedConnection)c).getContextManager();
        LanguageConnectionContext lcc = 
                (LanguageConnectionContext)contextManager.getContext(
                "LanguageConnectionContext");
        GenericPreparedStatement derbyPs = 
                (GenericPreparedStatement)lcc.getLastActivation().
                getPreparedStatement();

        assertFalse(derbyPs.isValid());
        
        rollback();
    }
        
    private final static String[] tableConstraintTypes = {
            " foreign key (i) references referenced(i)",
            " primary key(i)",
            " unique(i)",
            " check(i<3)"
        };

    private final static String[] columnConstraintTypes = {
            " references referenced(i)",
            " primary key",
            " unique",
            " check(i<3)"
        };

    // Each of the three characteristics can have 3 values corresponding to
    // {default, on, off}. This translates into 3 x 3 x 3 = 27 syntax
    // permutations, classified below with their corresponding dictionary state.
    //
    private final static String[][] defaultCharacteristics = {
        {" not deferrable initially immediate enforced", "E"},
        {" not deferrable initially immediate", "E"},
        {" not deferrable enforced", "E"},
        {" not deferrable", "E"},
        {" initially immediate enforced", "E"},
        {" initially immediate", "E"},
        {" enforced", "E"},
        {"", "E"}};

    private final static String[][] nonDefaultCharacteristics = {
        {" deferrable", "i"},
        {" deferrable initially immediate", "i"},
        {" deferrable enforced", "i"},
        {" deferrable initially immediate enforced", "i"},
        {" deferrable initially deferred", "e"},
        {" deferrable initially deferred enforced", "e"},
        {" initially deferred enforced", "e"},
        {" initially deferred", "e"},
        {" deferrable not enforced", "j"},
        {" deferrable initially immediate not enforced", "j"},
        {" deferrable initially deferred not enforced", "d"},
        {" initially deferred not enforced", "d"},
        {" not enforced", "D"},
        {" initially immediate not enforced", "D"},
        {" not deferrable not enforced", "D"},
        {" not deferrable initially immediate not enforced", "D"}
    };

    private final static String[] illegalCharacteristics = {
        " not deferrable initially deferred",
        " not deferrable initially deferred enforced",
        " not deferrable initially deferred not enforced"
    };

    private final static String[] illegalAlterCharacteristics;

    static {
        List<String> chars = new ArrayList<String>();
        chars.addAll(Arrays.asList(defaultCharacteristics[0]));
        chars.addAll(Arrays.asList(nonDefaultCharacteristics[0]));
        chars.addAll(Arrays.asList(illegalCharacteristics));
        chars.remove(" not enforced");
        chars.remove(" enforced");
        chars.remove("");
        illegalAlterCharacteristics = chars.toArray(new String[0]);
    }

    private final static Map<String, String[]> inverseState =
            new HashMap<String, String[]>();

    static {
        inverseState.put("E", new String[]{"E", "D"});
        inverseState.put("D", new String[]{"E", "D"});
        inverseState.put("i", new String[]{"i", "j"});
        inverseState.put("j", new String[]{"i", "j"});
        inverseState.put("i", new String[]{"i", "j"});
        inverseState.put("e", new String[]{"e", "d"});
        inverseState.put("d", new String[]{"e", "d"});
    }

    /**
     * Assert that we fail with feature not implemented
     * until feature is implemented (for characteristics that are not Derby
     * default).
     *
     * @param s statement

     * @throws SQLException
     */
    private void assertTableLevelFailTillFeatureImplemented(
            Statement s) throws SQLException {

        for (String ct : tableConstraintTypes) {
            for (String[] ch : nonDefaultCharacteristics) {
                assertStatementError("0A000",
                        s,
                        "create table t(i int, constraint c " +
                        ct + ch[0] + ")");
            }
        }
    }

    /**
     * Assert that we fail with feature not implemented
     * until feature is implemented (for characteristics that are not Derby
     * default).
     *
     * @param s statement

     * @throws SQLException
     */
    private void assertColumnLevelFailTillFeatureImplemented(
            Statement s) throws SQLException {

        for (String ct : columnConstraintTypes) {
            for (String[] ch : nonDefaultCharacteristics) {
                assertStatementError("0A000",
                        s,
                        "create table t(i int " +
                        ct + ch[0] + ")");
            }
        }
    }

    /**
     * Assert that we accept characteristics that merely specify the default
     * behavior anyway.
     *
     * @param s statement
     *
     * @throws SQLException
     */
    private void assertTableLevelDefaultBehaviorAccepted (
            Statement s) throws SQLException {

        for (String ct : tableConstraintTypes) {
            for (String[] ch : defaultCharacteristics) {
                assertUpdateCount(
                        s, 0,
                        "create table t(i int, constraint c " + ct + ch[0] + ")");
                rollback();
            }
        }
    }

    /**
     * Assert that we accept characteristics that merely specify the default
     * behavior anyway.
     *
     * @param s statement
     *
     * @throws SQLException
     */
    private void assertColumnLevelDefaultBehaviorAccepted (
            Statement s) throws SQLException {

        for (String ct : columnConstraintTypes) {
            for (String ch[] : defaultCharacteristics) {
                assertUpdateCount(
                        s, 0,
                        "create table t(i int " + ct + ch[0] + ")");
                rollback();
            }
        }
    }


    /**
     * Check that the dictionary state resulting from {@code characteristics}
     * equals {@code}.
     *
     * @param characteristics  A table level constraint characteristics string
     * @param code             Character encoding for characteristics
     *
     * @throws SQLException
     */
    private void assertFailTillFeatureImplemened(
            Statement s,
            String characteristics,
            char code) throws SQLException {

        for (String ct: tableConstraintTypes) {
            s.executeUpdate("alter table alter constraint c " + ct + " " +
                    characteristics + ")");

            JDBC.assertFullResultSet(
                s.executeQuery("select state from sys.sysconstraints " +
                               "    where constraintname = 'C'"),
                new String[][]{{String.valueOf(code)}});

            rollback();
        }
    }

    /**
     * Check that the dictionary state resulting from {@code characteristics}
     * equals {@code}.
     *
     * @param characteristics  A table level constraint characteristics string
     * @param code             Character encoding for characteristics
     *
     * @throws SQLException
     */
    private void assertDictState(
            Statement s,
            String characteristics,
            String code) throws SQLException {

        for (String ct: tableConstraintTypes) {
            s.executeUpdate("create table t(i int, constraint c " + ct + " " +
                    characteristics + ")");

            JDBC.assertFullResultSet(
                s.executeQuery("select state from sys.sysconstraints " +
                               "    where constraintname = 'C'"),
                new String[][]{{code}});

            rollback();
        }
    }

    /**
     * Check that the altered dictionary state resulting from new
     * {@code characteristics} equals {@code}.
     *
     * @param s                The statement object to use
     * @param enforcement  String containing ENFORCED or NOT ENFORCED
     *
     * @throws SQLException
     */
    private void assertAlterDictState(
            Statement s,
            String enforcement) throws SQLException {

        String oldState = getOldState(s);
        String newState = computeNewState(oldState, enforcement);

        s.executeUpdate("alter table t alter constraint c " +
                    enforcement);

        JDBC.assertFullResultSet(
                s.executeQuery("select state from sys.sysconstraints " +
                "    where constraintname = 'C'"),
                new String[][]{{newState}});
    }

    private String getOldState(Statement s) throws SQLException {
        ResultSet rs = s.executeQuery(
                "select state from sys.sysconstraints " +
                "    where constraintname = 'C'");
        try {
            rs.next();
            return rs.getString(1);
        } finally {
            rs.close();
        }
    }


    private String computeNewState(String oldState, String enforcement) {
        return inverseState.get(oldState)[enforcement.equals("enforced") ? 0 : 1];
    }

    private void assertCreateInconsistentCharacteristics(
            Statement s,
            String characteristics) throws SQLException {

        for (String ct: tableConstraintTypes) {
            try {
                s.executeUpdate("create table t(i int, constraint c " + ct + " " +
                        characteristics + ")");
                fail("wrong characteristics unexpectedly passed muster");
                rollback();
            } catch (SQLException e) {
                assertSQLState("42X97", e);
            }
        }
    }

    private void assertAlterInconsistentCharacteristics(
            Statement s,
            String characteristics) throws SQLException {

        try {
            s.executeUpdate("alter table t alter constraint c " +
                    characteristics);
            fail("wrong characteristics unexpectedly passed muster");
            rollback();
        } catch (SQLException e) {
            assertSQLState("42X01", e);
        }
    }
}

