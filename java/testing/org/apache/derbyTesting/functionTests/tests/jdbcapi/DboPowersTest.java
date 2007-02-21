/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.DboPowersTest

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.SQLException;
import javax.sql.DataSource;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * This JUnit tests enforcement of dbo (=database owner) powers, cf.
* DERBY-2264.
 *
 * The tests are run in the cross product (cardinality 10) of contexts:
 *
 *    {client/server, embedded} x 
 *    {no authentication, authentication and authentication/sqlAuthorization} x
 *    {data base owner, other user }
 *
 * One could consider removing the client/server suite to speed up
 * this test as it does not add much value given the nature of the changes.
 *
*/
public class DboPowersTest extends BaseJDBCTestCase
{
    /* test execution security context: one of three below */
    final private int authLevel; 
    final private static int NOAUTHENTICATION=0;
    final private static int AUTHENTICATION=1;
    final private static int SQLAUTHORIZATION=2;
    
    /**
     * Create a new instance of DboPowersTest
    *
     * @param name Fixture name
     * @param authLevel authentication level with which test is run
     */
    public DboPowersTest(String name, int authLevel) 
    { 
        super(name); 
        this.authLevel = authLevel;
    }

    /**
    * Construct top level suite in this JUnit test
     *
     * @return A suite containing embedded and client suites
     */
    public static Test suite()
    {
        TestSuite suite = new TestSuite("DboPowersTest");
        suite.addTest(dboSuite("embedded"));
        suite.addTest(TestConfiguration.clientServerDecorator(
                          dboSuite("client")));
        return suite;
   }
        
    /**
     *
     * Construct default suite of tests
     *
     * @param framework Derby framework
     * @return A suite containing the test cases incarnated for the three
     * security levels no authentication, authentication, and
     * authentication plus sqlAuthorization, 
     * The latter two has an instance for dbo, and one for ordinary user,
    * in all five incarnations of tests.
     */
    private static Test dboSuite(String framework) 
    {
        final String[][] users = {
            /* authLevel == AUTHENTICATION: dbo is APP/APP for db 'wombat',
             * so use that as first user.  Otherwise,
             * builtinAuthentication decorator's db shutdown fails to
             * work after DERBY-2264(!).
             */
            {"APP", "U1"}, 
           /* authLevel == SQLAUTHORIZATION: sqlAuthorizationDecorator
             * decorator presumes TEST_DBO as dbo, so add it to set of
             * valid users. Uses a fresh db 'dbsqlauth', not 'wombat'.
             */
            {"TEST_DBO", "U1"}};
        
        final String pwSuffix = "pwSuffix";

        Test tests[] = new Test[3]; // one per authLevel

        tests[NOAUTHENTICATION] = collectFixtures(NOAUTHENTICATION);

        /** First decorate with users, then with authentication. Do this
         * twice, once for authentication only, and once for
         * authentication and sqlAuthorization (see extra decorator
         * added below).
         */
        for (int autLev = AUTHENTICATION; 
             autLev <= SQLAUTHORIZATION ; autLev++) {

            // add decorator for different users authenticated
            TestSuite userSuite =  new TestSuite(
                "userSuite:"+ (autLev == AUTHENTICATION ? "authentication"
                              : "sqlAuthorization"));

            for (int userNo = 0; userNo < users.length; userNo++) {
                userSuite.addTest
                    (TestConfiguration.changeUserDecorator
                     (collectFixtures(autLev),
                      users[autLev-1][userNo], 
                      users[autLev-1][userNo].concat(pwSuffix)));
            }
        
            tests[autLev] = DatabasePropertyTestSetup.
               builtinAuthentication(userSuite, users[autLev-1], pwSuffix);
        }

        TestSuite suite = new TestSuite("dboPowers:"+framework);

        /* run tests with no authentication enabled */
        suite.addTest(tests[NOAUTHENTICATION]);

        /* run test for all users with only authentication enabled */
        suite.addTest(tests[AUTHENTICATION]);

       /* run test for all users with authentication and
         * sqlAuthorization enabled
         */
        suite.addTest(
            TestConfiguration.
            sqlAuthorizationDecorator(tests[SQLAUTHORIZATION]));
                         
        return suite;
    }

    /**
    * Picks up individual test fixtures explicitly, since we need to
     * provide the context.
     */
    private static TestSuite collectFixtures(int authLevel)
    {
        TestSuite suite = new TestSuite("dboPowersTests");
        suite.addTest(new DboPowersTest("testShutDown", authLevel));
        return suite;
    }

    /**
    * Test database shutdown power enforcement
     */
    public void testShutDown() throws SQLException
    {
        // make sure db is booted
        getConnection().close();

        String user = getTestConfiguration().getUserName();
        String password = getTestConfiguration().getUserPassword();

        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(
            ds, "connectionAttributes", "shutdown=true");
        JDBCDataSource.setBeanProperty(ds, "user", user);
        JDBCDataSource.setBeanProperty(ds, "password", password);
        try {
            ds.getConnection();
            fail("shutdown failed: no exception");
        } catch (SQLException e) {
            if ("08006".equals(e.getSQLState())) {
                // reboot if shutdown succeeded
                JDBCDataSource.setBeanProperty(ds, "connectionAttributes", "");
               ds.getConnection().close();
            }

            vetShutdownException(user, e);
        }
    }

    /**
     * Decide if the result of trying to shut down the database is
     * compliant with the semantics introduced by DERBY-2264.
     */
   private void vetShutdownException (String user, SQLException e)
    {
        switch (authLevel) {
        case NOAUTHENTICATION:
            assertSQLState("database shutdown, no authentication", 
                           "08006", e);
            break;
        case AUTHENTICATION:
            if ("APP".equals(user)) {
                assertSQLState("database shutdown, authentication, db owner", 
                               "08006", e);
           } else {
                assertSQLState("database shutdown restriction, authentication," +
                               " not db owner", "2850H", e);
            }
            break;
        case SQLAUTHORIZATION:
            if ("TEST_DBO".equals(user)) {
                assertSQLState("database shutdown, SQL authorization, db owner",
                               "08006", e);
            } else {
                assertSQLState("database shutdown restriction, " + 
                              "SQL authorization, not db owner",
                               "2850H", e);
            }
            break;
        default:
            fail("test error");
            break;
        }
    }
}

