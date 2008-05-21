/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.CompressTableTest

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
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test cases for compressing tables.
 */
public class CompressTableTest extends BaseJDBCTestCase {

    public CompressTableTest(String name) {
        super(name);
    }

    public static Test suite() {
        // compress table is an embedded feature, no need to run network tests
        return TestConfiguration.embeddedSuite(CompressTableTest.class);
    }

    /**
     * Test that SYSCS_COMPRESS_TABLE and SYSCS_INPLACE_COMPRESS_TABLE work
     * when the table name contains a double quote. It used to raise a syntax
     * error. Fixed as part of DERBY-1062.
     */
    public void testCompressTableWithDoubleQuoteInName() throws SQLException {
        Statement s = createStatement();
        s.execute("create table app.\"abc\"\"def\" (x int)");
        s.execute("call syscs_util.syscs_compress_table('APP','abc\"def',1)");
        s.execute("call syscs_util.syscs_inplace_compress_table('APP'," +
                  "'abc\"def', 1, 1, 1)");
        s.execute("drop table app.\"abc\"\"def\"");
    }
}
