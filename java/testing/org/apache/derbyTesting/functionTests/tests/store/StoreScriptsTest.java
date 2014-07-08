/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.store.StoreScriptsTest

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

package org.apache.derbyTesting.functionTests.tests.store;

import java.sql.Statement;
import java.util.Properties;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.ScriptTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;

public class StoreScriptsTest extends ScriptTestCase {
    
    /**
     * Store SQL scripts (.sql files) that only run in embedded.
     * Most tests that are testing SQL functionality can just 
     * run in emebdded.
     */
    private static final String[] EMBEDDED_TESTS = {
        /* in comments reasons why scripts from the storemore suite can/cannot be run like this*/
        "cisco",        
        "connectDisconnect", 
        "databaseProperties", // causes failures in AccessTest in EncryptionSuite
        "longRow",
        //"logDevice", // cannot run like this; test tries to set up 
        // a separate logDevice by using ij.DataSource.connectionAttribute
        // =logDevice=exitnout/weirdlog
        "Rllmodule1", // note, original had the following properties:
        // derby.module.access.rll=org.apache.derby.impl.store.RllRAMAccessManager.
        // derby.storage.rowLocking=true 
        // behavior is the same with them set, and without...
        // "Rllmodule2",// cannot run as is, because needs special properties
        // derby.module.access.rll=org.apache.derby.impl.store.RllRAMAccessManager.
        // derby.storage.rowLocking=false
        "Rllmodule3", 
        "Rllmodule4", // note, original had special properties
        // derby.module.access.rll=org.apache.derby.impl.store.RllRAMAccessManager.
        // derby.storage.rowLocking=true 
        // behavior is the same with them set, and without...
        // might be able to get that set for a separate suite...
        "RowLockIso",
        "rlliso1multi",
        "rlliso2multi",
        "rlliso3multi", // note, original test had property 'derby.storage.rowLocking=true'
        // "TransactionTable", // note, original test had property 'derby.storage.rowLocking=true'
        //      but this cannot be run like this, because weme gives a different result than
        //      ibm16 - there could be a timing issue, or it could be due to 
        //      diffs between using datasource or drivermanager.
        //      also note, this would set maximumdisplaywidth, and the setting would
        //      remain for subsequent tests.
        "testsqldecimal", // note, original test had properties set:
        // derby.language.StatementCache=true
        // derby.storage.sortBufferMax=78
        // derby.debug.true=testSort
        "heapscan", // note, original had properties set:
        /*derby.storage.userLockTrace=true
        derby.locks.deadlockTimeout=1
        derby.locks.waitTimeout=5
        #derby.infolog.append=true
        derby.language.logStatementText=true
        #derby.debug.true=userLockStackTrace,DeadlockTrace
        #derby.debug.true=DeadlockTrace
        #derby.debug.true=enableRowLocking,DeadlockTrace
        derby.debug.true=verbose_heap_post_commit */
        "removeStubs",
        //"rollForwardBackup", has a _sed.properties file; connects to 
        // multiple databases - needs to be fully converted.
        // "rollForwardRecovery", original uses useextdirs=true, which
        // includes separate processing in the old harness.
        // also needs adjustment of run resource call 
        "readlocks", 
        //"backupRestore", // uses another database than wombat
        "bug3498", 
        //"onlineBackupTest2", // runs into lock time out 
        //"onlineBackupTest4" // runs into security exception
        // Following scripts were part of the 'storetests' suite
        "st_derby1189",
        // "st_1",// this one can just be removed - was the
        // first initial test for the SYSCS_UTIL schema. All functionality
        // is now better tested elsewhere.
        "st_b5772",
        //"derby94" // this one needs special property 
        // derby.locks.escalationThreshold=102
        };

    public StoreScriptsTest(String script) {
        super(script, true);
    }
    
    private static Test getSuite(String[] list)
    {
        BaseTestSuite suite = new BaseTestSuite("SQL scripts");
        for (int i = 0; i < list.length; i++)
            suite.addTest(
                    new CleanDatabaseTestSetup(
                    new StoreScriptsTest(list[i])));

        return getIJConfig(suite);
    }
    
    public static Test suite() {        
        Properties props = new Properties();

        props.setProperty("derby.infolog.append", "true");  
        props.setProperty("ij.protocol", "jdbc:derby:");
        props.setProperty("ij.database", "wombat;create=true");

        Test test = new SystemPropertyTestSetup(
                getSuite(EMBEDDED_TESTS), props);

        // Lock timeout settings that were set for the old harness store tests
        test = DatabasePropertyTestSetup.setLockTimeouts(test, 1, 4);
        
        BaseTestSuite suite = new BaseTestSuite("StoreScripts");
        suite.addTest(test);

        return getIJConfig(suite); 
    }   
    
    protected void tearDown() throws Exception {
        rollback();
        Statement s = createStatement();
        // Clear the database properties set by this test so that they
        // don't affect other tests.
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY" +
                "('derby.storage.pageSize', NULL)");
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY" +
                "('derby.storage.pageReservedSpace', NULL)");
        s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY" +
                "('derby.database.propertiesOnly', false)");
        // databaseProperties.sql sets this as a system property as well.
        removeSystemProperty("derby.storage.pageSize");

        super.tearDown();
    }


}
