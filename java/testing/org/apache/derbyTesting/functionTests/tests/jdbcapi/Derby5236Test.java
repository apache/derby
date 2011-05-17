/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.Derby5236Test

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test case for DERBY-5236.
 */
public class Derby5236Test extends BaseJDBCTestCase {
    public Derby5236Test(String name) {
        super(name);
    }

    public static Test suite() {
        return TestConfiguration.defaultSuite(Derby5236Test.class);
    }

    /**
     * Verify that string values aren't truncated when their UTF-8 encoded
     * representation exceeds 32KB.
     */
    public void testLongColumn() throws SQLException {
        PreparedStatement ps = prepareStatement(
                "values cast(? as varchar(20000))");

        char[] chars = new char[20000];
        Arrays.fill(chars, '\u4e10');
        String str = new String(chars);

        ps.setString(1, str);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), str);
    }
}
