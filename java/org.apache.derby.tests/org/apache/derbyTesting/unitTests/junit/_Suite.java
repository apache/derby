/*

   Derby - Class org.apache.derbyTesting.unitTests.junit._Suite

       Licensed to the Apache Software Foundation (ASF) under one
       or more contributor license agreements.  See the NOTICE file
       distributed with this work for additional information
       regarding copyright ownership.  The ASF licenses this file
       to you under the Apache License, Version 2.0 (the
       "License"); you may not use this file except in compliance
       with the License.  You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

       Unless required by applicable law or agreed to in writing,
       software distributed under the License is distributed on an
       "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
       KIND, either express or implied.  See the License for the
       specific language governing permissions and limitations
       under the License
*/
package org.apache.derbyTesting.unitTests.junit;

import java.sql.SQLException;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;

/**
 * Suite to run all JUnit tests in this package:
 * org.apache.derbyTesting.unitTests.junit
 *
 */
public class _Suite extends BaseTestCase {

    /**
     * Use suite method instead.
     */
    private _Suite(String name) {
        super(name);
    }

    public static Test suite() throws SQLException {

        BaseTestSuite suite = new BaseTestSuite("JUnit unit tests");
//IC see: https://issues.apache.org/jira/browse/DERBY-6590

        suite.addTest(ArrayInputStreamTest.suite());
        suite.addTest(FormatableBitSetTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3531
        suite.addTest(SystemPrivilegesPermissionTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2760
        suite.addTest(UTF8UtilTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3742
        suite.addTestSuite(CompressedNumberTest.class);
//IC see: https://issues.apache.org/jira/browse/DERBY-3618
        suite.addTest(AssertFailureTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3770
        suite.addTest(InputStreamUtilTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3936
        suite.addTest(CharacterStreamDescriptorTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-646
//IC see: https://issues.apache.org/jira/browse/DERBY-4084
        suite.addTest(BlockedByteArrayTest.suite());
        suite.addTest(PathUtilTest.suite());
        suite.addTest(VirtualFileTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-4122
        suite.addTest(ReaderToUTF8StreamTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3941
        suite.addTest(DataInputUtilTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-5475
        suite.addTest(DerbyVersionTest.suite());
        suite.addTest(MissingPermissionsTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-6617

        return suite;
    }
}
