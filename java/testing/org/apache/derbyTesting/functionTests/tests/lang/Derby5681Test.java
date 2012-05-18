/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.Derby5681Test
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

import java.sql.Connection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.IndexStatsUtil;

/**
 * DERBY-5681(When a foreign key constraint on a table is dropped,
 *  the associated statistics row for the conglomerate is not removed.)
 * @throws Exception
 */
public class Derby5681Test extends BaseJDBCTestCase {

    /**
     * Basic constructor.
     */
    public Derby5681Test(String name) {
        super(name);
    }

    /**
     * Returns the implemented tests.
     * 
     * @return An instance of <code>Test</code> with the implemented tests to
     *         run.
     */
    public static Test suite() {
        return TestConfiguration.defaultSuite(Derby5681Test.class);
    }

    /**
     * The test can't really demonstrate the fix for DERBY-5681 until 
     *  DERBY-5702(Creating a foreign key constraint does not automatically
     *  create a statistics row if foreign key constraint will share a backing
     *  index created for a primary key) is fixed. The fix for DERBY-5681
     *  is going to take care of the hanging statistics row left for a
     *  constraint(which shares a backing index with another constraint) that 
     *  is getting dropped. But because of DERBY-5702, the statistics row
     *  never gets created for a constraint that shares a backing index. In
     *  10.5 and higher, it is possible to create the missing statistics row
     *  using update statistics procedure but that procedure is not available
     *  in 10.4 and earlier releases.
     * @throws SQLException
     */
    public void testBug4356() throws SQLException {
        // Helper object to obtain information about index statistics.
        IndexStatsUtil stats = new IndexStatsUtil(openDefaultConnection());
        Statement s = createStatement();
    	
        //Test - primary key constraint
        s.executeUpdate("CREATE TABLE TEST_TAB_1 (c11 int not null,"+
                "c12 int not null, c13 int)");
        stats.assertNoStatsTable("TEST_TAB_1");
        //Insert data into table with no constraint and there will be no stat
        // for that table at this point
        s.executeUpdate("INSERT INTO TEST_TAB_1 VALUES(1,1,1),(2,2,2)");
        stats.assertNoStatsTable("TEST_TAB_1");
        //Add primary key constraint to the table and now we should find a 
        // statistics row for it
        s.executeUpdate("ALTER TABLE TEST_TAB_1 "+
                "ADD CONSTRAINT TEST_TAB_1_PK_1 "+
        		"PRIMARY KEY (c11)");
        stats.assertTableStats("TEST_TAB_1",1);
        //Dropping primary key constraint will drop the corresponding
        // statistics
        s.executeUpdate("ALTER TABLE TEST_TAB_1 "+
                "DROP CONSTRAINT TEST_TAB_1_PK_1");
        stats.assertNoStatsTable("TEST_TAB_1");
        //Add the primary key constraint back since it will be used by the next
        // test to create foreign key constraint
        s.executeUpdate("ALTER TABLE TEST_TAB_1 "+
                "ADD CONSTRAINT TEST_TAB_1_PK_1 "+
        		"PRIMARY KEY (c11)");
        //The statistics for primary key constraint has been added
        stats.assertTableStats("TEST_TAB_1",1);

        //Test - unique key constraint
        s.executeUpdate("ALTER TABLE TEST_TAB_1 "+
                "ADD CONSTRAINT TEST_TAB_1_UNQ_1 "+
        		"UNIQUE (c12)");
        stats.assertTableStats("TEST_TAB_1",2);
        s.executeUpdate("ALTER TABLE TEST_TAB_1 "+
                "DROP CONSTRAINT TEST_TAB_1_UNQ_1");
        stats.assertTableStats("TEST_TAB_1",1);
        s.executeUpdate("ALTER TABLE TEST_TAB_1 "+
                "DROP CONSTRAINT TEST_TAB_1_PK_1");
        stats.assertNoStatsTable("TEST_TAB_1");
        s.executeUpdate("ALTER TABLE TEST_TAB_1 "+
                "ADD CONSTRAINT TEST_TAB_1_PK_1 "+
        		"PRIMARY KEY (c11)");
        stats.assertTableStats("TEST_TAB_1",1);

        //Test - unique key constraint on nullable column & non-nullable column
        s.executeUpdate("ALTER TABLE TEST_TAB_1 "+
                "ADD CONSTRAINT TEST_TAB_1_UNQ_2 "+
        		"UNIQUE (c12, c13)");
        stats.assertTableStats("TEST_TAB_1",3);
        s.executeUpdate("ALTER TABLE TEST_TAB_1 "+
                "DROP CONSTRAINT TEST_TAB_1_UNQ_2");
        stats.assertTableStats("TEST_TAB_1",1);
        s.executeUpdate("ALTER TABLE TEST_TAB_1 "+
                "DROP CONSTRAINT TEST_TAB_1_PK_1");
        stats.assertNoStatsTable("TEST_TAB_1");
        s.executeUpdate("ALTER TABLE TEST_TAB_1 "+
                "ADD CONSTRAINT TEST_TAB_1_PK_1 "+
        		"PRIMARY KEY (c11)");
        stats.assertTableStats("TEST_TAB_1",1);
        
        //Test - foreign key but no primary key constraint
        s.executeUpdate("CREATE TABLE TEST_TAB_3 (c31 int not null)");
        s.executeUpdate("INSERT INTO TEST_TAB_3 VALUES(1),(2)");
        s.executeUpdate("ALTER TABLE TEST_TAB_3 "+
                "ADD CONSTRAINT TEST_TAB_3_FK_1 "+
        		"FOREIGN KEY(c31) REFERENCES TEST_TAB_1(c11)");
        stats.assertTableStats("TEST_TAB_3",1);
        s.executeUpdate("ALTER TABLE TEST_TAB_3 "+
                "DROP CONSTRAINT TEST_TAB_3_FK_1");
        stats.assertNoStatsTable("TEST_TAB_3");

        //Test - foreign key and primary key constraint
        s.executeUpdate("CREATE TABLE TEST_TAB_2 (c21 int not null)");
        s.executeUpdate("INSERT INTO TEST_TAB_2 VALUES(1),(2)");
        s.executeUpdate("ALTER TABLE TEST_TAB_2 "+
                "ADD CONSTRAINT TEST_TAB_2_PK_1 "+
        		"PRIMARY KEY (c21)");
        stats.assertTableStats("TEST_TAB_2",1);
        //DERBY-5702 Add a foreign key constraint and now we should find 2 rows
        // of statistics for TEST_TAB_2 - 1 for primary key and other for
        // foreign key constraint
        s.executeUpdate("ALTER TABLE TEST_TAB_2 "+
                "ADD CONSTRAINT TEST_TAB_2_FK_1 "+
        		"FOREIGN KEY(c21) REFERENCES TEST_TAB_1(c11)");
        //DERBY-5702 Like primary key earlier, adding foreign key constraint
        // didn't automatically add a statistics row for it. And there is
        // no update statistics procedure available in 10.4 and earlier
        // releases and there is no way to create the statistics row for
        // constraints which share the backing index with anothe constraint.
        //After DERBY-5702 is fixed, we should see 2 rows below and not just
        // one statistics row
        stats.assertTableStats("TEST_TAB_2",1);
        //Number of statistics row for TEST_TAB_1 will remain unchanged since
        // it has only primary key defined on it
        stats.assertTableStats("TEST_TAB_1",1);
        s.executeUpdate("ALTER TABLE TEST_TAB_2 "+
                "DROP CONSTRAINT TEST_TAB_2_FK_1");
        //Dropping the foreign key constraint should remove one of the 
        // statistics row for TEST_TAB_2. 
        stats.assertTableStats("TEST_TAB_2",1);
        s.execute("drop table TEST_TAB_2");
        s.execute("drop table TEST_TAB_1");
        stats.release();
    }
}
