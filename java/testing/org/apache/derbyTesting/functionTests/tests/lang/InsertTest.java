/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.InsertTest

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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * This class contains test cases for the INSERT statement.
 */
public class InsertTest extends BaseJDBCTestCase {

    public InsertTest(String name) {
        super(name);
    }

    public static Test suite() {
        return TestConfiguration.defaultSuite(InsertTest.class);
    }

    /**
     * Regression test case for DERBY-4348 where an INSERT INTO .. SELECT FROM
     * statement would result in a LONG VARCHAR column becoming populated with
     * the wrong values.
     */
    public void testInsertIntoSelectFromWithLongVarchar() throws SQLException {
        // Generate the data that we want table T2 to hold when the test
        // completes.
        String[][] data = new String[100][2];
        for (int i = 0; i < data.length; i++) {
            // first column should have integers 0,1,...,99
            data[i][0] = Integer.toString(i);
            // second column should always be -1
            data[i][1] = "-1";
        }

        // Turn off auto-commit so that the tables used in the test are
        // automatically cleaned up in tearDown().
        setAutoCommit(false);

        Statement s = createStatement();
        s.execute("create table t1(a long varchar)");

        // Fill table T1 with the values we want to see in T2's first column.
        PreparedStatement insT1 = prepareStatement("insert into t1 values ?");
        for (int i = 0; i < data.length; i++) {
            insT1.setString(1, data[i][0]);
            insT1.executeUpdate();
        }

        // Create table T2 and insert the contents of T1. Column B must have
        // a default value and a NOT NULL constraint in order to expose
        // DERBY-4348. The presence of NOT NULL makes the INSERT statement use
        // a NormalizeResultSet, and the bug was caused by a bug in the
        // normalization.
        s.execute("create table t2(a long varchar, b int default -1 not null)");
        s.execute("insert into t2(a) select * from t1");

        // Verify that T1 contains the expected values. Use an ORDER BY to
        // guarantee the same ordering as in data[][].
        JDBC.assertFullResultSet(s.executeQuery(
                    "select * from t2 order by int(cast (a as varchar(10)))"),
                data);
    }
}
