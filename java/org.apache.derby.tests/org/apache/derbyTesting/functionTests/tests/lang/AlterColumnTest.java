/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.AlterColumnTest

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

import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.JDBC;

/**
 * Test cases for altering columns.
 */
public class AlterColumnTest extends BaseJDBCTestCase {

    public AlterColumnTest(String name) {
        super(name);
    }

    public static Test suite() {
        return TestConfiguration.defaultSuite(AlterColumnTest.class);
    }

    /**
     * Test ALTER COLUMN default
     */
    public void testAlterDefault() throws SQLException {
        Statement s = createStatement();
        s.execute("create table t(i int default 0)");
        PreparedStatement pd = prepareStatement("delete from t");
        PreparedStatement pi = prepareStatement("insert into t values default");
        PreparedStatement ps = prepareStatement("select * from t order by i");

        pi.executeUpdate();
        JDBC.assertFullResultSet(
            ps.executeQuery(), new String[][]{ {"0"} });

        /*
         * Try different syntaxes allowed
         */
        s.execute("alter table t alter COLUMN i DEFAULT 1");
        tryAndExpect(pd, pi, ps, "1");

        s.execute("alter table t alter COLUMN i WITH DEFAULT 2");
        tryAndExpect(pd, pi, ps, "2");

        // Standard SQL syntax added in DERBY-4013
        s.execute("alter table t alter COLUMN i SET DEFAULT 3");
        tryAndExpect(pd, pi, ps, "3");

        s.execute("alter table t alter i DEFAULT 4");
        tryAndExpect(pd, pi, ps, "4");

        s.execute("alter table t alter i WITH DEFAULT 5");
        tryAndExpect(pd, pi, ps, "5");

        // Standard SQL syntax added in DERBY-4013
        s.execute("alter table t alter i SET DEFAULT 6");
        tryAndExpect(pd, pi, ps, "6");

        s.execute("alter table t alter i SET DEFAULT null");
        tryAndExpect(pd, pi, ps, null);

        s.execute("alter table t alter i SET DEFAULT 1");
        tryAndExpect(pd, pi, ps, "1");

        // Standard SQL syntax added in DERBY-4013
        s.execute("alter table t alter i DROP DEFAULT");
        tryAndExpect(pd, pi, ps, null);

        s.close();
        pd.close();
        pi.close();
        ps.close();
    }

    /**
     * Auxiliary method: Execute the delete statement d to clean table, then
     * the insert statement i to exercise the default mechanism, then check via
     * the select statement s, that the value inserted is e.
     *
     * @param d delete statement
     * @param i insert statement
     * @param s select statement
     * @param e expected value as a string
     */
    private static void tryAndExpect(
        PreparedStatement d,
        PreparedStatement i,
        PreparedStatement s,
        String e) throws SQLException {

        d.executeUpdate();
        i.executeUpdate();
        JDBC.assertSingleValueResultSet(s.executeQuery(), e);
    }
}
