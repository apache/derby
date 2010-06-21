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
import java.net.Socket;
import java.lang.Integer;

import org.apache.derby.jdbc.EmbeddedSimpleDataSource;
/**
 * Create and boot the supplied db argument. This auxiliary program is used by
 * {@code BootLockTest.java} to boot a db in a different jvm and subsequently
 * attempt a boot to from the original VM to detect dual boot attempt.
 * <p/>
 * Started as:
 * {@code java org.apache.derbyTesting.functionTests.tests.store.BootLockMinion <dbname> <port>}
 */

public class BootLockMinion {
    public static void main(String[] args) {
        String dbName = args[0];
        Connection con;
        Statement stmt;
        System.setProperty("derby.stream.error.file",
                           "BootLockMinion.log");
        try
        {
            EmbeddedSimpleDataSource ds = new EmbeddedSimpleDataSource();
            ds.setDatabaseName(dbName);
            ds.setCreateDatabase("create");

            con = ds.getConnection();
            stmt = con.createStatement();

            stmt.execute("create table t1(i int)");
            //infinite loop until we get killed by BootLockTest.java
            for(;;)
            {
                Thread.sleep(30000);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
