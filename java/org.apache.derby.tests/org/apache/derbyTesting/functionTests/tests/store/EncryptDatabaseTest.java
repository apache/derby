/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.store.EncryptDatabaseTest

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

package org.apache.derbyTesting.functionTests.tests.store;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import javax.sql.DataSource;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.Decorator;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;


/**
 * Database encryption testing, mainly that handling of bootPassword works ok
 * across encryption algorithms. Converted from {@code encryptDatabase.sql} in
 * old harness, which was used in suites for the different algoritms,
 * e.g. {@code encryptionECB}.  DERBY-2687.
 */

public class EncryptDatabaseTest  extends BaseJDBCTestCase
{
    // SQL states 
    private static final String ENCRYPTION_NOCHANGE_ALGORITHM = "XBCXD";
    private static final String ENCRYPTION_NOCHANGE_PROVIDER = "XBCXE";
    private static final String ILLEGAL_BP_LENGTH = "XBCX2";
    private static final String NULL_BOOT_PASSWORD = "XBCX5";
    private static final String WRONG_BOOT_PASSWORD = "XBCXA";
    private static final String WRONG_PASSWORD_CHANGE_FORMAT = "XBCX7";

    
    public EncryptDatabaseTest(String name) {
        super(name);
    }

    
    /**
     * Construct top level suite in this JUnit test
     *
     * @return A suite containing embedded suites
     */
    public static Test suite() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("EncryptDatabase");
        suite.addTest(wrapTest());
        suite.addTest(wrapTest("DESede/CBC/NoPadding")); // from encryption
        suite.addTest(wrapTest("DESede/CFB/NoPadding")); // from encryptionCFB
        suite.addTest(wrapTest("DES/OFB/NoPadding"));    // from encryptionOFB
        suite.addTest(wrapTest("DES/ECB/NoPadding"));    // from encryptionECB
        suite.addTest(wrapTest("DES/CBC/NoPadding"));    // from encryptionDES
        suite.addTest(wrapTest("Blowfish/CBC/NoPadding")); // from e..Blowfish
        suite.addTest(wrapTest("AES/CBC/NoPadding"));    // from encryptionAES
        suite.addTest(wrapTest("AES/OFB/NoPadding"));
        return suite;
    }

    
    private static Test wrapTest() {
        return Decorator.encryptedDatabaseBpw(
                          TestConfiguration.embeddedSuite(
                              EncryptDatabaseTest.class),
                          "Thursday"); // only initial bootPassword, though..
    }


    private static Test wrapTest(String encryptionMethod) {
        return Decorator.encryptedDatabaseBpw(
                          TestConfiguration.embeddedSuite(
                              EncryptDatabaseTest.class),
                          encryptionMethod,
                          "Thursday"); // only initial bootPassword, though..
    }


    public void testEncryption() throws SQLException {

        // for bug 3668 - you couldn't change the password without exiting
        // out of db create session

        Statement s = createStatement();
        s.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
                        "'bootPassword', 'Thursday, Wednesday')");

        TestConfiguration.getCurrent().shutdownEngine();

        // -- test for bug 3668
        // -- try the old password, should fail
        assertFailedBoot("Thursday");

        assertSuccessfulBoot("Wednesday");
        s = createStatement();

        // -- switch back to old password
        s.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
                        "'bootPassword', 'Wednesday, Thursday')");

        // create table t1 ( a char(20));
        s.executeUpdate("create table t1 ( a char(20))");

        // -- make sure we cannot access the secret key

        JDBC.assertSingleValueResultSet(
            s.executeQuery("values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY(" +
                           "'bootPassword')"),
            null);

        JDBC.assertSingleValueResultSet(
            s.executeQuery("values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY(" +
                           "'encryptedBootPassword')"),
            null);

        s.executeUpdate("insert into t1 values ('hello world')");

        // -- change the secret key

        // -- these should fail

        assertFailedStatement(
            s,
            "call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'bootPassword', null)",
            NULL_BOOT_PASSWORD);

        assertFailedStatement(
            s,
            "call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'bootPassword', 'wrongkey, ')",
            ILLEGAL_BP_LENGTH);

        assertFailedStatement(
            s,
            "call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'bootPassword', 'Thursday')",
            WRONG_PASSWORD_CHANGE_FORMAT);

        assertFailedStatement(
            s,
            "call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'bootPassword', 'Thursday , ')",
            ILLEGAL_BP_LENGTH);

        assertFailedStatement(
            s,
            "call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'bootPassword', 'Thursday , short')",
            ILLEGAL_BP_LENGTH);

        assertFailedStatement(
            s,
            "call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'bootPassword', 'Thursdya , derbypwd')",
            WRONG_BOOT_PASSWORD);

        assertFailedStatement(
            s,
            "call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'bootPassword', 'Thursdayx , derbypwd')",
            WRONG_BOOT_PASSWORD);

        assertFailedStatement(
            s,
            "call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'bootPassword', 'xThursday , derbypwd')",
            WRONG_BOOT_PASSWORD);

        assertFailedStatement(
            s,
            "call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'bootPassword', 'thursday , derbypwd')",
            WRONG_BOOT_PASSWORD);

        s.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
                        "'bootPassword', ' Thursday , Saturday')");

        assertFailedStatement(
            s,
            "call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'bootPassword', 'Thursday , derbypwd')",
            WRONG_BOOT_PASSWORD);


        // -- change it again

        s.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
                        "'bootPassword', 'Saturday,derbypwd')");

        // -- make sure we cannot access the secret key
        JDBC.assertSingleValueResultSet(
            s.executeQuery("values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY(" +
                           "'bootPassword')"),
            null);

        JDBC.assertSingleValueResultSet(
            s.executeQuery("values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY(" +
                           "'encryptedBootPassword')"),
            null);


        TestConfiguration.getCurrent().shutdownEngine();

        assertFailedBoot(null);
        assertFailedBoot("Thursday");
        assertFailedBoot("Saturday");
        assertSuccessfulBoot("derbypwd");

        s = createStatement();

        JDBC.assertSingleValueResultSet(
            s.executeQuery("values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY(" +
                           "'bootPassword')"),
            null);

        JDBC.assertSingleValueResultSet(
            s.executeQuery("values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY(" +
                           "'encryptedBootPassword')"),
            null);

        // -- change it again, make sure it trims white spaces

        s.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
                        "'bootPassword', '   derbypwd   ,  bbderbypwdx  ')");
        s.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
                        "'bootPassword', 'bbderbypwdx, derbypwdxx ')");


        JDBC.assertSingleValueResultSet(
            s.executeQuery("values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY(" +
                           "'bootPassword')"),
            null);

        JDBC.assertSingleValueResultSet(
            s.executeQuery("values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY(" +
                           "'encryptedBootPassword')"),
            null);

        TestConfiguration.getCurrent().shutdownEngine();

        assertFailedBoot("derbypwd");
        assertSuccessfulBoot("derbypwdxx");

        s = createStatement();

        JDBC.assertSingleValueResultSet(
            s.executeQuery("select * from t1"),
            "hello world");

        // test that you cannot change the encryption provider or algorithm
        // after database creation

        assertFailedStatement(
            s,
            "call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'encryptionAlgorithm', 'DES/blabla/NoPadding')",
            ENCRYPTION_NOCHANGE_ALGORITHM);

        assertFailedStatement(
            s,
            "call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'encryptionProvider', 'com.pom.aplomb')",
            ENCRYPTION_NOCHANGE_PROVIDER);
    }


    private void assertFailedBoot(String bootPassword) throws SQLException {
        DataSource ds = JDBCDataSource.getDataSource();

        JDBCDataSource.setBeanProperty(
                ds, "connectionAttributes",
                (bootPassword != null ? "bootPassword=" + bootPassword
                 : "")); // "": lest we inherit bootPassword from current config


        try {
            ds.getConnection();
            fail("boot worked: unexpected");
        } catch (SQLException e) {

            String [] accepted = new String[]{
                "XBM06",  // normal: wrong bootpassword
                "XJ040"}; // Java error during boot: DERBY-2687
                          // Remove when DERBY-5622 is fixed.
            boolean found = Arrays.asList(accepted).contains(e.getSQLState()); 

            if (!found) {
                throw e;
            }
        }
    }


    private static void assertSuccessfulBoot(String bootPassword)
            throws SQLException {

        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(
            ds, "connectionAttributes", "bootPassword=" + bootPassword);
        ds.getConnection().close();
    }


    private static void assertFailedStatement(Statement s,
                                              String sql,
                                              String state) {
        assertStatementError(state, s, sql);
    }
}
