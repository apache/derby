/*
  Class org.apache.derbyTesting.functionTests.tests.engine.Derby6396Test

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

package org.apache.derbyTesting.functionTests.tests.engine;

import java.io.File;
import java.sql.SQLException;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Regression test case for DERBY-6396. Verify that booting the database
 * does not fail with a NullPointerException if the user lacks read access
 * on the temporary directory.
 */
public class Derby6396Test extends BaseJDBCTestCase {
    private File tmpDir;

    public Derby6396Test(String name) {
        super(name);
    }

    public static Test suite() {
        // Use a separate database for this test to reduce the risk of
        // interfering with other tests when changing file permissions.
        return TestConfiguration.singleUseDatabaseDecorator(
                TestConfiguration.embeddedSuite(Derby6396Test.class));
    }

    @Override
    protected void tearDown() throws Exception {
        if (tmpDir != null) {
            // Reset the permission of the temporary directory so that we
            // don't run into problems when dropping the database.
            PrivilegedFileOpsForTests.setReadable(tmpDir, true, true);
            tmpDir = null;
        }
        super.tearDown();
    }

    public void testTempNotReadable() throws SQLException {
        final TestConfiguration config = TestConfiguration.getCurrent();

        // First make sure the database exists and is not booted.
        getConnection().close();
        config.shutdownDatabase();

        // Now make sure the database has a tmp directory that cannot be read.
        tmpDir = new File(
            config.getDatabasePath(config.getDefaultDatabaseName()), "tmp");
        assertTrue(PrivilegedFileOpsForTests.mkdir(tmpDir));
        PrivilegedFileOpsForTests.setReadable(tmpDir, false, true);

        // Booting the database used to fail with a NullPointerException.
        // Should succeed now.
        getConnection().close();
    }
}
