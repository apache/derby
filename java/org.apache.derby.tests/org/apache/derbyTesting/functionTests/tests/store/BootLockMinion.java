/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.store.BootLockMinion

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

import java.sql.Connection;
import java.sql.Statement;
import java.io.File;
import javax.sql.DataSource;
import org.apache.derbyTesting.junit.JDBCDataSource;

/**
 * Create and boot the supplied db argument. This auxiliary program is used by
 * {@code BootLockTest.java} to boot a db in a different jvm and subsequently
 * attempt a boot to from the original VM to detect dual boot attempt.
 * <p/>
 * Started as:
 * {@code java org.apache.derbyTesting.functionTests.tests.store.BootLockMinion <dbname> <port>}
 */

public class BootLockMinion {
    private static int WAIT_FOR_DESTROY_MAX_MILLIS = BootLockTest.MINION_WAIT_MAX_MILLIS;
    
    public static void main(String[] args) {
        String dbName = args[0];
        Connection con;
        Statement stmt;
        System.setProperty("derby.stream.error.file",
                           "BootLockMinion.log");
        try
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-6651
            DataSource ds = JDBCDataSource.getDataSource(dbName);
            JDBCDataSource.setBeanProperty(ds, "createDatabase", "create");

            con = ds.getConnection();
            stmt = con.createStatement();

            stmt.execute("create table t1(i int)");
            // Once we are finished creating the database and making the
            // connection, create the file minionComplete that BootLockTest
            //can check in order to proceed with its work.
//IC see: https://issues.apache.org/jira/browse/DERBY-4985
            File checkFile = new File(BootLockTest.minionCompleteFileName);
            checkFile.createNewFile();
            //infinite loop until we get killed by BootLockTest.java
            int wait = WAIT_FOR_DESTROY_MAX_MILLIS;
//IC see: https://issues.apache.org/jira/browse/DERBY-4987
            while(wait > 0)
            {
                Thread.sleep(10000);
                wait -= 10000;
            }
            System.err.println("BootLockMinion exceeded maximum wait for destroy");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
