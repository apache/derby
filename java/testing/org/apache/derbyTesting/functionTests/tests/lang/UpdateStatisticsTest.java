/*
 * Class org.apache.derbyTesting.functionTests.tests.lang.UpdateStatisticsTest
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
import java.sql.ResultSet;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests for updating the statistics of one index or all the indexes on a
 * table DERBY-269.
 */
public class UpdateStatisticsTest extends BaseJDBCTestCase {

    public UpdateStatisticsTest(String name) {
        super(name);
    }

    public static Test suite() {
        return TestConfiguration.defaultSuite(UpdateStatisticsTest.class);
    }

    /**
     * Test for update statistics
     */
    public void testUpdateStatistics() throws SQLException {
        Statement s = createStatement();
        //following should fail because table APP.T1 does not exist
        assertStatementError("42Y55", s, 
            "CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','T1',null)");
        s.execute("CREATE TABLE t1 (c11 int, c12 varchar(128))");
        //following will pass now because we have created APP.T1
        s.execute("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','T1',null)");
        //following should fail because index I1 does not exist on table APP.T1
        assertStatementError("42X65", s, 
            "CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','T1','I1')");
        s.execute("CREATE INDEX i1 on t1(c12)");
        //following will pass now because we have created index I1 on APP.T1
        s.execute("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','T1','I1')");

        //The following set of subtest will ensure that when an index is
        //created on a table when there is no data in the table, then Derby
        //will not generate a row for it in sysstatistics table. If the index
        //is created after the table has data on it, there will be a row for
        //it in sysstatistics table. In order to generate statistics for the
        //first index, users can run the stored procedure 
        //SYSCS_UPDATE_STATISTICS
        //So far the table t1 is empty and we have already created index I1 on 
        //it. Since three was no data in the table when index I1 was created,
        //there will be no row in sysstatistics table
        ResultSet rs = s.executeQuery("SELECT * FROM SYS.SYSSTATISTICS");
        JDBC.assertEmpty(rs);
        //Now insert some data into t1 and then create a new index on the 
        //table. This will cause sysstatistics table to have one row for this
        //new index. Old index will still not have a row for it in
        //sysstatistics table
        s.executeUpdate("INSERT INTO T1 VALUES(1,'a'),(2,'b'),(3,'c'),(4,'d')");
        s.execute("CREATE INDEX i2 on t1(c11)");
        rs = s.executeQuery("SELECT * FROM SYS.SYSSTATISTICS");
        JDBC.assertDrainResults(rs, 1);
        //Now update the statistics for the old index I1 using the new stored
        //procedure. Doing this should add a row for it in sysstatistics table
        s.execute("CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('APP','T1','I1')");
        rs = s.executeQuery("SELECT * FROM SYS.SYSSTATISTICS");
        JDBC.assertDrainResults(rs, 2);

        //calls to system procedure for update statisitcs is internally
        //converted into ALTER TABLE ... sql but that generated sql format
        //is not available to end user to issue directly. Write a test case
        //for that sql syntax
        assertStatementError("42X01", s, 
            "ALTER TABLE APP.T1 ALL UPDATE STATISTICS");
        assertStatementError("42X01", s, 
            "ALTER TABLE APP.T1 UPDATE STATISTICS I1");

        //Try update statistics on global temporary table
		s.executeUpdate("declare global temporary table SESSION.t1(c11 int, c12 int) on commit delete rows not logged");
		s.executeUpdate("insert into session.t1 values(11, 1)");
        //following should fail because update statistics can't be issued on
		//global temporary tables
        assertStatementError("42995", s, 
            "CALL SYSCS_UTIL.SYSCS_UPDATE_STATISTICS('SESSION','T1',null)");
    }
}
