/**
 * Derby - Class org.apache.derbyTesting.functionTests.tests.lang.FloatTypesTest
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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test floating point data types.
 */
public class FloatTypesTest extends BaseJDBCTestCase {

    public FloatTypesTest(String name) {
        super(name);
    }

    public static Test suite() {
        return TestConfiguration.defaultSuite(FloatTypesTest.class);
    }

    /**
     * Test that normalization of negative zero to positive zero works for
     * floats. In some JVMs this used to give wrong results after runtime
     * optimization. See DERBY-2447 and
     * <a href="http://bugs.sun.com/view_bug.do?bug_id=6833879">CR6833879</a>
     * in Sun's bug database.
     */
    public void testNegativeZeroFloatJvmBug() throws SQLException {
        PreparedStatement ps = prepareStatement("values -cast(? as real)");
        ps.setFloat(1, 0.0f);
        // Execute the statement many times so that the JVM is likely to
        // produce native, optimized code.
        for (int i = 0; i < 7000; i++) {
            JDBC.assertSingleValueResultSet(ps.executeQuery(), "0.0");
        }
    }

    /**
     * Test that normalization of negative zero to positive zero works for
     * doubles. In some JVMs this used to give wrong results after runtime
     * optimization. See DERBY-2447 and
     * <a href="http://bugs.sun.com/view_bug.do?bug_id=6833879">CR6833879</a>
     * in Sun's bug database.
     */
    public void testNegativeZeroDoubleJvmBug() throws SQLException {
        PreparedStatement ps = prepareStatement("values -cast(? as double)");
        ps.setDouble(1, 0.0d);
        // Execute the statement many times so that the JVM is likely to
        // produce native, optimized code.
        for (int i = 0; i < 7000; i++) {
            JDBC.assertSingleValueResultSet(ps.executeQuery(), "0.0");
        }
    }
}
