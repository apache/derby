/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.Derby6587Test

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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;

/**
 * Test case for DERBY-6131: select from view with "upper" and "in" list 
 * throws a ClassCastException null value functionality.
 */
public class Derby6587Test extends BaseJDBCTestCase {

    public Derby6587Test(String name) {
        super(name);
    }

    public static Test suite(){
        BaseTestSuite suite = new BaseTestSuite("Derby6587Test");
        suite.addTest(baseSuite("Derby6587:embedded"));
        return suite;
    }
    public static Test baseSuite(String name) {
        BaseTestSuite suite = new BaseTestSuite(Derby6587Test.class, name);
        Test test = new SupportFilesSetup(suite, 
            new String[] {
                "functionTests/testData/Derby6587/Derby6587_TABLE1_T.csv",
                "functionTests/testData/Derby6587/Derby6587_TABLE2_T.csv"});
        return new CleanDatabaseTestSetup(test) {
            protected void decorateSQL(Statement stmt) throws SQLException {
                stmt.executeUpdate(
                        "CREATE TABLE TABLE1_T " +
                        "(UUID char (16) for bit data NOT NULL," +
                        "NR integer NOT NULL," +
                        "TEXT varchar(200) NOT NULL," +
                        "CONSTRAINT IDX_1 PRIMARY KEY (UUID, NR))");
                stmt.executeUpdate(
                        "CREATE TABLE TABLE2_T " +
                        "(UUID char (16) for bit data NOT NULL," +
                        "ID1 char(5) NOT NULL," +
                        "ID2 integer NOT NULL," +
                        "NR integer NOT NULL," +
                        "CONSTRAINT IDX_2 PRIMARY KEY (ID1, UUID))");
                stmt.executeUpdate(
                        "ALTER TABLE TABLE2_T " +
                        "ADD CONSTRAINT FK_1 FOREIGN KEY (UUID, NR) " +
                        "REFERENCES TABLE1_T (UUID, NR) " +
                        "ON DELETE NO ACTION ON UPDATE NO ACTION");
            }
        };
    }

    public void setUp() throws SQLException{
        getConnection().setAutoCommit(false);
    }

    /**
     * Test the original user report of this issue:
     * <p>
     * the issue can be reproduced
     * 1. create table 1
     * 2. create table 2
     * 3. run bulk import on both tables
     * <p>
     **/
    public void testBulkImport()
        throws SQLException
    {
        getConnection();
        try {
            doImportFromFile( "extin/Derby6587_TABLE1_T.csv", "TABLE1_T" );
            doImportFromFile( "extin/Derby6587_TABLE2_T.csv", "TABLE2_T" );
        } catch (SQLException sqle) {
            // in the failing case, we hit a XIE0R, Import failed; INSERT on
            // TABLE2_T caused a violation of foreign key constraint FK_1...
            fail("caught SQLException: " + 
                sqle.getSQLState() + "; " + sqle.getMessage());
        }
    }
    
    @Override
    protected void tearDown() throws Exception {
        BaseJDBCTestCase.dropTable(getConnection(), "TABLE2_T");
        BaseJDBCTestCase.dropTable(getConnection(), "TABLE1_T");
        super.tearDown();
    }
    
    // method which calls the import table, pre-filling some of the values
    private void doImportFromFile(String fileName, String toTable) 
            throws SQLException
   {
       String impsql = "call SYSCS_UTIL.SYSCS_IMPORT_TABLE (? , ? , ? , ?, ? , ?, ?)";
       PreparedStatement ps = prepareStatement(impsql);
       ps.setString(1, "APP");
       ps.setString(2, toTable);
       ps.setString(3, fileName);
       ps.setString(4 , null);
       ps.setString(5 , null);
       ps.setString(6 , null);
       ps.setInt(7, 0); // assuming replace, otherwise this needs to be non-0
       ps.execute();
       ps.close();
   }
}
