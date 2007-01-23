/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.DRDAProtocolTest
 
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

package org.apache.derbyTesting.functionTests.tests.derbynet;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import javax.sql.DataSource;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseJDBCTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.SupportFilesSetup;

/** The test of the properties of the DRDA network protocol implementation.
  */
public class DRDAProtocolTest extends BaseJDBCTestCase {
    
    /** Test whether the multiple connections to different databases
      * on a same derby instance are working without exceptions. */
    public void testMultipleConnections() throws Exception {
        Connection conn1 = openConnection("FIRSTDB1");
        conn1.setAutoCommit(false);

        Statement st = conn1.createStatement();
        st.execute("create table FIRSTDB_T1 (i int, j int, k int)");
        st.execute("insert into FIRSTDB_T1 values (1, 3, 5)");
        PreparedStatement pSt1 =
                conn1.prepareStatement("select * from FIRSTDB_T1");
        
        ResultSet rs1 = pSt1.executeQuery();
        rs1.next();
        rs1.close();
        
        Connection conn2 = openConnection("SECONDDB2");
        conn2.setAutoCommit(false);
        Statement st2 = conn2.createStatement();
        st2.execute("create table SECONDDB_T1 (i int, j int, k int)");
        st2.execute("insert into SECONDDB_T1 values (2, 4, 6)");
        PreparedStatement pSt2 =
                conn2.prepareStatement("select * from SECONDDB_T1");
        
        rs1 = pSt2.executeQuery();
        rs1.next();
        rs1.close();
        
        conn1.rollback();
        conn1.close();
        conn2.rollback();
        conn2.close();
    }
    
    /* ------------------- end helper methods  -------------------------- */
    public DRDAProtocolTest(String name) {
        super(name);
    }
    
    public static Test suite() {
        Test test;
        test = TestConfiguration.clientServerSuite(DRDAProtocolTest.class);
        test = TestConfiguration.singleUseDatabaseDecorator(test, "FIRSTDB1", false);
        test = TestConfiguration.singleUseDatabaseDecorator(test, "SECONDDB2", false);
        return test;
    }
    
}
