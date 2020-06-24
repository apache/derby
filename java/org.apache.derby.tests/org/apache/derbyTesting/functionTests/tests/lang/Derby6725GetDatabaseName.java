/*
 * Class org.apache.derbyTesting.functionTests.tests.lang.Derby6725GetDatabaseName
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

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import junit.framework.Test;

import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

public class Derby6725GetDatabaseName extends BaseJDBCTestCase {

    public Derby6725GetDatabaseName(String name) {
        super(name);
    }

    /**
     * Construct top level suite in this JUnit test
     *
     * @return A suite containing embedded and client suites.
     */
    public static Test suite()
    {
        Test test;
        test = TestConfiguration.defaultSuite(Derby6725GetDatabaseName.class);
        test = TestConfiguration.additionalDatabaseDecorator(test, "FIRSTDB1");
        test = TestConfiguration.additionalDatabaseDecorator(test, "SECONDDB2");
        return test;
    }

    //Make sure we get correct db name for different databases
    public void testDatabaseName() throws SQLException {
    	checkDbName(getConnection(), "wombat");
    	checkDbName(openConnection("FIRSTDB1"), "singleUse/oneuse0");
    	checkDbName(openConnection("SECONDDB2"), "singleUse/oneuse1");
    }
    
    private void checkDbName(Connection conn, String expectedDbName) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-6725
        File    systemDir = new File( "system" );
        File    dbDir = new File( systemDir, expectedDbName );
        String  databasePath = PrivilegedFileOpsForTests.getAbsolutePath( dbDir );

        PreparedStatement ps = conn.prepareStatement("values syscs_util.SYSCS_GET_DATABASE_NAME()");
        ResultSet rs = ps.executeQuery();
        rs.next();
        assertEquals( databasePath, rs.getString( 1 ) );
        rs.close();
        ps.close();
    }
}
