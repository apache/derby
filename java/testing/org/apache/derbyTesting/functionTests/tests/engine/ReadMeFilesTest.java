/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.engine.ReadMeFilesTest


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

package org.apache.derbyTesting.functionTests.tests.engine;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.Decorator;
import org.apache.derbyTesting.junit.TestConfiguration;


/**
 * Tests related to the 3 Derby readme files. These readmes warn users against 
 *   editing/deleting any of the files in the database directories. The 
 *   location of the readme files are  
 *   1)at the db level directory, 
 *   2)in seg0 directory and 
 *   3)in the log directocy.
 * All the three readme files are named README_DO_NOT_TOUCH_FILES.txt
 */
public class ReadMeFilesTest extends BaseJDBCTestCase {
    /**
    The readme file cautioning users against touching the files in
    the database directory 
    */
    private static final String DB_README_FILE_NAME = "README_DO_NOT_TOUCH_FILES.txt";
    static String logDir = BaseTestCase.getSystemProperty("derby.system.home")+File.separator+"abcs";

    public ReadMeFilesTest(String name) {
        super(name);
    }

    public static Test suite() {
    	TestSuite suite = new TestSuite("ReadMeFilesTest");

        //DERBY-5232 (Put a stern README file in log and seg0 directories 
        // to warn users of corrpution they will cause if they touch files 
        // there)
        //Test the existence of readme files for a default embedded config
        // which means that "log" directory is under the database directory
        // along with "seg0" directory
        suite.addTest(TestConfiguration.singleUseDatabaseDecorator(
            TestConfiguration.embeddedSuite(ReadMeFilesTest.class)));

        //DERBY-5995 (Add a test case to check the 3 readme files get created 
        // even when log directory has been changed with jdbc url attribute 
        // logDevice )
        //Test the existence of readme files for a database configuration
        // where "log" directory may not be under the database directory.
        // It's location is determined by jdbc url attribute logDevice.
        logDir = BaseTestCase.getSystemProperty("derby.system.home")+
            File.separator+"abcs";
        suite.addTest(
            Decorator.logDeviceAttributeDatabase(
                TestConfiguration.embeddedSuite(ReadMeFilesTest.class),
                logDir));
        return suite;
    }

    public void testReadMeFilesExist() throws IOException, SQLException {
        getConnection();
        TestConfiguration currentConfig = TestConfiguration.getCurrent();
        String dbPath = currentConfig.getDatabasePath(currentConfig.getDefaultDatabaseName());
        lookForReadmeFile(dbPath);
        lookForReadmeFile(dbPath+File.separator+"seg0");

        String logDevice = currentConfig.getConnectionAttributes().getProperty("logDevice");
        if (logDevice != null) {
            lookForReadmeFile(logDir+File.separator+"log");
        } else {
            lookForReadmeFile(dbPath+File.separator+"log");
        }
    }

    private void lookForReadmeFile(String path) throws IOException {
        File readmeFile = new File(path,
            DB_README_FILE_NAME);
        assertTrue(readmeFile + "doesn't exist", 
            PrivilegedFileOpsForTests.exists(readmeFile));
    }
}
