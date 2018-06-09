/*
 * Class org.apache.derbyTesting.functionTests.tests.lang.Derby5005Test
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;

public class Derby5005Test extends BaseJDBCTestCase {

    public Derby5005Test(String name) {
        super(name);
    }

    /**
     * Construct top level suite in this JUnit test
     *
     * @return A suite containing embedded and client suites.
     */
    public static Test suite()
    {
        BaseTestSuite suite = new BaseTestSuite("Derby5005Test");

        suite.addTest(makeSuite());
        // suite.addTest(
        //      TestConfiguration.clientServerDecorator(makeSuite()));

        return suite;
    }

    /**
     * Construct suite of tests
     *
     * @return A suite containing the test cases.
     */
    private static Test makeSuite()
    {
        return new CleanDatabaseTestSetup(
            new BaseTestSuite(Derby5005Test.class)) {
                protected void decorateSQL(Statement s)
                        throws SQLException {
                    getConnection().setAutoCommit(false);

                    s.execute("create table app.a (a integer)");
                    s.execute("create view app.v as select * from app.a");
                    s.execute("insert into app.a (a) values(1)");

                    getConnection().commit();
                }
            };
    }

    public void testInsertSelectOrderBy5005() throws SQLException {

        Statement s = createStatement();

        JDBC.assertFullResultSet(
            s.executeQuery("select app.a.a from app.a where app.a.a <> 2 " +
                           "order by app.a.a asc"),
            new String[][]{{"1"}});

        JDBC.assertFullResultSet(
            s.executeQuery("select app.v.a from app.v where app.v.a <> 2 " +
                           "order by v.a asc"),
            new String[][]{{"1"}});

        // Next query fails in DERBY-5005:
        JDBC.assertFullResultSet(
            s.executeQuery("select v.a from app.v where v.a <> 2 " +
                           "order by app.v.a asc"),
            new String[][]{{"1"}});
    }
}
