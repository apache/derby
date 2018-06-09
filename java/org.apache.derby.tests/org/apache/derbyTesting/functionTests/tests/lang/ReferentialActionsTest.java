/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.ReferentialActionsTest

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;

/**
 * This class tests SQL referential actions.
 */
public class ReferentialActionsTest extends BaseJDBCTestCase {

    public ReferentialActionsTest(String name) {
        super(name);
    }

    public static Test suite() {
        BaseTestSuite suite = new BaseTestSuite("ReferentialActionsTest");

        // DERBY-2353: Need to set derby.language.logQueryPlan to expose the
        // bug (got a NullPointerException when writing the plan to derby.log)
        suite.addTest(DatabasePropertyTestSetup.singleProperty(
                new ReferentialActionsTest("onDeleteCascadeWithLogQueryPlan"),
                "derby.language.logQueryPlan", "true", true));

        return suite;
    }

    /**
     * Test that cascading delete works when derby.language.logQueryPlan is
     * set to true - DERBY-2353.
     */
    public void onDeleteCascadeWithLogQueryPlan() throws SQLException {
        setAutoCommit(false);
        Statement s = createStatement();
        s.execute("create table a (a1 int primary key)");
        s.execute("insert into a values 1");
        s.execute("create table b (b1 int references a on delete cascade)");
        s.execute("insert into b values 1");
        // The next line used to cause a NullPointerException
        s.execute("delete from a");
    }
}
