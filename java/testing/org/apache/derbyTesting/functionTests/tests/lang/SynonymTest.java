/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.SynonymTest
 *  
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;

/**
 * Synonym testing using junit
 */
public class SynonymTest extends BaseJDBCTestCase {

    /**
     * Basic constructor.
     */
    public SynonymTest(String name) {
        super(name);
    }

    /**
     * Create a suite of tests.
     */
    public static Test suite() {
        TestSuite suite = new TestSuite(SynonymTest.class, "SynonymTest");
        return new CleanDatabaseTestSetup(suite);
    }

    /**
     * The test makes sure that we correctly throw dependency exception when
     * user requests to drop a synonym which has dependent objects on it. Once
     * the dependency is removed, we should be able to drop the synonym.
     * @throws SQLException
     */
    public void testViewDependency() throws SQLException {
        Statement stmt = createStatement();  
        stmt.executeUpdate("create synonym mySyn for sys.systables");
        stmt.executeUpdate("create view v1 as select * from mySyn");
        stmt.executeUpdate("create view v2 as select * from v1");
        // Drop synonym should fail since it is used in two views.
        assertStatementError("X0Y23", stmt, "drop synonym mySyn");
        stmt.executeUpdate("drop view v2");
        // fails still because of view v1's dependency
        assertStatementError("X0Y23", stmt, "drop synonym mySyn");
        stmt.executeUpdate("drop view v1");
        stmt.executeUpdate("drop synonym mySyn");
        stmt.close();
    }

    /**
     * DERBY-5244 DatabaseMetaData.getColumns(null, null, tableName, null) 
     * 	does not return the columns meta for a SYNONYM. This is because for
     *  synonyms, we do not add any rows in SYSCOLUMNS. But the metadata query 
     *  for DatabaseMetaData.getColumns() looks at SYSCOLUMNS to get the 
     *  resultset. Views and Tables do not have problems because we do keep
     *  their columns information in SYSCOLUMNS.
     * 
     * Test DatabaseMetaData.getColumns call on synonyms
     *
     * This test confirms the behavior noticed in DERBY-5244.
     */
    public void testMetaDataCallOnSynonymsDERBY5244()
        throws SQLException
    {
            Statement st = createStatement();
            st.executeUpdate("create table t1Derby5422 "+
            		"( c11 int not null, c12 char(2) )");
            //Create a synonym on table t1Derby5422
            st.executeUpdate("create synonym s1Derby5422 for t1Derby5422");
            st.executeUpdate("create view v1Derby5422 as select * from t1Derby5422");
            
            //Verify that the synonym has been created successfully by
            // doing a select from it
            ResultSet rs = st.executeQuery("select * from S1DERBY5422");
            JDBC.assertEmpty(rs);
            DatabaseMetaData dbmd = getConnection().getMetaData();
            //Derby can find metadata info for the base table
            rs = dbmd.getColumns(null, null, "T1DERBY5422", null);
            JDBC.assertDrainResultsHasData(rs);
            //Derby can find metadata info for the view
            rs = dbmd.getColumns(null, null, "V1DERBY5422", null);
            JDBC.assertDrainResultsHasData(rs);
            //But Derby does not locate the metadata info for synonym
            rs = dbmd.getColumns(null, null, "S1DERBY5422", null);
            JDBC.assertEmpty(rs);
    }

    /**
     * Test that synonyms are dereferenced properly for a searched DELETE.
     *
     * This test verifies that DERBY-4110 is fixed.
     */
    public void testSynonymsInSearchedDeleteDERBY4110()
        throws SQLException
    {
        Statement st = createStatement();
        st.executeUpdate("create schema test1");
        st.executeUpdate("create schema test2");
        st.executeUpdate("create table test1.t1 ( id bigint not null )");
        st.executeUpdate("insert into test1.t1 values (1),(2)");
        st.executeUpdate("create synonym test2.t1 for test1.t1");
        st.executeUpdate("create unique index idx4110 on test1.t1 (id)");
        st.executeUpdate("set schema test2");
        st.executeUpdate("delete from t1 where id = 2"); // DERBY-4110 here
        st.executeUpdate("drop synonym test2.t1");
        st.executeUpdate("drop table test1.t1");
        st.executeUpdate("drop schema test2 restrict");
        st.executeUpdate("drop schema test1 restrict");
    }

    /**
     * Verify the fix for DERBY-5168. SynonymAliasInfo.toString() used to
     * return a value with incorrect syntax if the synonym referred to a
     * table that had a double quote character either in its name or in the
     * schema name.
     */
    public void testSynonymsForTablesWithDoubleQuotes() throws SQLException {
        setAutoCommit(false);
        Statement s = createStatement();
        s.execute("create schema \"\"\"\"");
        s.execute("create table \"\"\"\".\"\"\"\" (x int)");
        s.execute("create synonym derby_5168_synonym for \"\"\"\".\"\"\"\"");

        // We can exercise SynonymAliasInfo.toString() by reading the ALIASINFO
        // column in SYS.SYSALIASES. This assert used to fail before the fix.
        JDBC.assertSingleValueResultSet(
            s.executeQuery(
                "select aliasinfo from sys.sysaliases " +
                "where alias = 'DERBY_5168_SYNONYM'"),
            "\"\"\"\".\"\"\"\"");
    }
}
