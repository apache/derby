/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.store._Suite

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
package org.apache.derbyTesting.functionTests.tests.store;

import junit.framework.Test;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;

/**
 * Suite to run all JUnit tests in this package:
 * org.apache.derbyTesting.functionTests.tests.lang
 * <P>
 * All tests are run "as-is", just as if they were run
 * individually. Thus this test is just a collection
 * of all the JUNit tests in this package (excluding itself).
 * While the old test harness is in use, some use of decorators
 * may be required.
 *
 */
public class _Suite extends BaseTestCase  {

    /**
     * Use suite method instead.
     */
    private _Suite(String name) {
        super(name);
    }

    public static Test suite() {

        BaseTestSuite suite = new BaseTestSuite("store");
//IC see: https://issues.apache.org/jira/browse/DERBY-6590

        suite.addTest(BootAllTest.suite());
        suite.addTest(ClassLoaderBootTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3663
        suite.addTest(StreamingColumnTest.suite());
        suite.addTest(Derby3625Test.suite());
        suite.addTest(Derby4577Test.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-4741
        suite.addTest(InterruptResilienceTest.suite());
        suite.addTest(Derby4676Test.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-4179
//IC see: https://issues.apache.org/jira/browse/DERBY-4646
//IC see: https://issues.apache.org/jira/browse/DERBY-4647
        suite.addTest(BootLockTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-5305
        suite.addTest(UpdateLocksTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3735
        suite.addTest(PositionedStoreStreamTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3837
        suite.addTest(OSReadOnlyTest.suite());
        suite.addTest(BackupRestoreTest.suite());
        suite.addTest(OfflineBackupTest.suite());
        suite.addTest(LiveLockTest.suite());
        suite.addTest(ClobReclamationTest.suite());
        suite.addTest(IndexSplitDeadlockTest.suite());
        suite.addTest(HoldCursorJDBC30Test.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-4038
        suite.addTest(AccessTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-4939
//IC see: https://issues.apache.org/jira/browse/DERBY-5131
        suite.addTest(AutomaticIndexStatisticsTest.suite());
        suite.addTest(Derby5582AutomaticIndexStatisticsTest.suite());
        suite.addTest(AutomaticIndexStatisticsMultiTest.suite());
        suite.addTest(BTreeMaxScanTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-5127
        suite.addTest(MadhareTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-5156
        suite.addTest(LongColumnTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-5282
        suite.addTest(RowLockBasicTest.suite());
        suite.addTest(RecoveryTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-5382
        suite.addTest(OCRecoveryTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-5301
        suite.addTest(TableLockBasicTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-5283
        suite.addTest(ServicePropertiesFileTest.suite());
        suite.addTest(Derby5234Test.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3790
        suite.addTest(KeepDisposableStatsPropertyTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-5630
        suite.addTest(LockTableVtiTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-5977
        suite.addTest(StoreScriptsTest.suite());
        suite.addTest(Derby4923Test.suite());
        suite.addTest(SpaceTableTest.suite());
        
        /* Tests that only run in sane builds */
//IC see: https://issues.apache.org/jira/browse/DERBY-3842
        if (SanityManager.DEBUG) {
            suite.addTest(HoldCursorExternalSortJDBC30Test.suite());
        }

        // Encryption only supported for Derby in J2SE/J2EE environments.
        // J2ME (JSR169) does not support encryption.
//IC see: https://issues.apache.org/jira/browse/DERBY-1001
        if (JDBC.vmSupportsJDBC3()) {
            // Add tests of basic functionality on encrypted databases.
//IC see: https://issues.apache.org/jira/browse/DERBY-2644
            suite.addTest(EncryptionKeyAESTest.suite());
            suite.addTest(EncryptionKeyBlowfishTest.suite());
            suite.addTest(EncryptionKeyDESTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3711
            suite.addTest(EncryptionAESTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2687
//IC see: https://issues.apache.org/jira/browse/DERBY-5622
            suite.addTest(EncryptDatabaseTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-5934
            suite.addTest(CryptoCrashRecoveryTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-5792
            suite.addTest(DecryptDatabaseTest.suite());
        }

        return suite;
    }
}
