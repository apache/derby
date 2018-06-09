/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.CreateTableFromQueryTest

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
package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Assert;
import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test for creating tables using a query expression.
 */
public final class CreateTableFromQueryTest extends BaseJDBCTestCase {

    /**
     * Public constructor required for running test as standalone JUnit.
     */
    public CreateTableFromQueryTest(String name)
    {
        super(name);
    }

    /*
     * Factored out for reuse in other TestCases which add
     * the same test method in their suite() method.
     *
     * Currently done for a few testcases reused in replication testing:
     * o.a.dT.ft.tests.replicationTests.StandardTests.
     */
    public static void decorate(Statement stmt)
         throws SQLException
    {
        // create base tables t1 and t2
        stmt.executeUpdate(
                "create table t1(i int not null, s smallint, f float, dp "
                + "double precision, v varchar(10) not null)");
        
        stmt.executeUpdate("create table t2 (a int, s varchar(5))");
    }
    
    /**
     * Create a suite of tests.
    */
    public static Test suite()
    {
        Test test = TestConfiguration.embeddedSuite(CreateTableFromQueryTest.class);
        return new CleanDatabaseTestSetup(test) {

            protected void decorateSQL(Statement stmt) throws SQLException
            {
                decorate(stmt);
            }
        };
    }

    /**
     * Test basic table creation.
     * @throws Exception
     */
    public void testBasicTableCreation() throws Exception
    {
        positiveTest("create table t3 as select * from t1 with no data",
            new String [] {"I", "S", "F", "DP", "V"},
            new String [] {"NO", "YES", "YES", "YES", "NO"},
            new String [] {"INTEGER", "SMALLINT", "DOUBLE", "DOUBLE", "VARCHAR"});
    }
    
    /**
     * Test creating table with a list of column names.
     * @throws Exception
     */
    public void testCreateTableWithColumnList() throws Exception
    {
        positiveTest(
            "create table t3 (a,b,c,d,e) as select * from t1 with no data",
            new String [] {"A", "B", "C", "D", "E"},
            new String [] {"NO", "YES", "YES", "YES", "NO"},
            new String [] {"INTEGER", "SMALLINT", "DOUBLE", "DOUBLE", "VARCHAR"});
    }

    /**
     * Test creating a table with a subset of the base table's columns.
     * @throws Exception
     */
    public void testCreateTableWithSubsetOfColumns() throws Exception
    {
        positiveTest("create table t3 as select v,f from t1 with no data",
            new String [] {"V", "F"},
            new String [] {"NO", "YES"},
            new String [] {"VARCHAR", "DOUBLE"});
    }

    /**
     * Test creating a table with a subset of the base table's columns
     * and a column list.
     * @throws Exception
     */
    public void testCreateTableWithSubsetOfColumnsAndColumnList() throws Exception
    {
        positiveTest(
            "create table t3 (a,b,c) as select v,dp,i from t1 with no data",
            new String [] {"A", "B", "C"},
            new String [] {"NO", "YES", "NO"},
            new String [] {"VARCHAR", "DOUBLE", "INTEGER"});
    }

    /**
     * Test creating a table with multiple base tables.
     * @throws Exception
     */
    public void testCreateTableWithMultipleBaseTables() throws Exception
    {
        positiveTest("create table t3 (one, two) as select x.s, y.v from "
                     + "t1 y, t2 x where x.a = y.i with no data",
            new String [] {"ONE", "TWO"},
            new String [] {"YES", "NO"},
            new String [] {"VARCHAR", "VARCHAR"});
    }

    /**
     * Test creating a table with a column list and system generated
     * column names in the query.
     * @throws Exception
     */
    public void testCreateTableWithDerivedColumnName() throws Exception
    {
        positiveTest(
            "create table t3 (x,y) as select 2*i,2.0*f from t1 with no data",
            new String [] {"X", "Y"},
            new String [] {"NO", "YES"},
            new String [] {"INTEGER", "DOUBLE"});
    }

    /**
     * Test creating a table from a values statement.
     * @throws Exception
     */
    public void testCreateTableFromValues() throws Exception
    {
        positiveTest(
            "create table t3 (x,y) as values (1, 'name') with no data",
            new String [] {"X", "Y"},
            new String [] {"NO", "NO"},
            new String [] {"INTEGER", "CHAR"});
    }
    
    public void testCreateTableWithGroupByInQuery() throws Exception 
    {
        positiveTest(
            "create table t3 (x, y) as " +
            " (select v, sum(i) from t1 where i > 0 " +
            " group by i, v having i <= " +
            " ANY (select a from t2)) with no data",
            new String[] {"X", "Y"},
            new String[] {"NO", "YES"},
            new String[] {"VARCHAR", "INTEGER"});
    }

    public void testDerby6956() throws Exception
    {
        Statement stmt = createStatement();

        stmt.executeUpdate(
            "CREATE TABLE DERBYTEST6956 " +
                "(STRINGCOLUMN varchar(255), "+
                " INTEGERCOLUMN integer, "+
                " SHORTCOLUMN varchar(255), "+
                " LONGCOLUMN bigint, "+
                " DOUBLECOLUMN double, "+
                " FLOATCOLUMN double, "+
                " DECIMALCOLUMN decimal(31, 6), "+
                " BOOLEANCOLUMN smallint, "+
                " DATECOLUMN timestamp, "+
                " DATETIMECOLUMN timestamp, "+
                " ID integer, "+
                " LASTMODTIME timestamp, "+
                " PRIMARY KEY (ID))");
        stmt.executeUpdate(
            "CREATE TABLE DERBYTEST_TEMP6956 "+
                "AS SELECT * FROM DERBYTEST6956 WITH NO DATA");

        stmt.executeUpdate(
            "CREATE TABLE DERBYTEST6956_A " +
                "(STRINGCOLUMN varchar(255), "+
                " INTEGERCOLUMN integer, "+
                " SHORTCOLUMN varchar(255), "+
                " LONGCOLUMN bigint, "+
                " DOUBLECOLUMN double, "+
                " FLOATCOLUMN double, "+
                " DECIMALCOLUMN decimal(29, 6), "+
                " BOOLEANCOLUMN smallint, "+
                " DATECOLUMN timestamp, "+
                " DATETIMECOLUMN timestamp, "+
                " ID integer, "+
                " LASTMODTIME timestamp, "+
                " PRIMARY KEY (ID))");
        stmt.executeUpdate(
            "CREATE TABLE DERBYTEST_TEMP6956_A "+
                "AS SELECT * FROM DERBYTEST6956_A WITH NO DATA");

        stmt.executeUpdate(
            "CREATE TABLE DERBYTEST6956_B " +
                "(STRINGCOLUMN varchar(255), "+
                " INTEGERCOLUMN integer, "+
                " SHORTCOLUMN varchar(255), "+
                " LONGCOLUMN bigint, "+
                " DOUBLECOLUMN double, "+
                " FLOATCOLUMN double, "+
                " DECIMALCOLUMN decimal(31,31), "+
                " BOOLEANCOLUMN smallint, "+
                " DATECOLUMN timestamp, "+
                " DATETIMECOLUMN timestamp, "+
                " ID integer, "+
                " LASTMODTIME timestamp, "+
                " PRIMARY KEY (ID))");
        stmt.executeUpdate(
            "CREATE TABLE DERBYTEST_TEMP6956_B "+
                "AS SELECT * FROM DERBYTEST6956_B WITH NO DATA");

        stmt.executeUpdate(
            "CREATE TABLE DERBYTEST6956_C " +
                "(STRINGCOLUMN varchar(255), "+
                " INTEGERCOLUMN integer, "+
                " SHORTCOLUMN varchar(255), "+
                " LONGCOLUMN bigint, "+
                " DOUBLECOLUMN double, "+
                " FLOATCOLUMN double, "+
                " DECIMALCOLUMN decimal(31,0), "+
                " BOOLEANCOLUMN smallint, "+
                " DATECOLUMN timestamp, "+
                " DATETIMECOLUMN timestamp, "+
                " ID integer, "+
                " LASTMODTIME timestamp, "+
                " PRIMARY KEY (ID))");
        stmt.executeUpdate(
            "CREATE TABLE DERBYTEST_TEMP6956_C "+
                "AS SELECT * FROM DERBYTEST6956_C WITH NO DATA");

    }

    /**
     * Test error when base table does not exist.
     * @throws Exception
     */
    public void testBaseTableDoesNotExist() throws Exception
    {
        assertStatementError("42X05", createStatement(),
            "create table t3 as select * from t4 with no data");
    }

    /**
     * Test error when parameters are supplied in the query expression.
     * @throws Exception
     */
    public void testParametersNotAllowed() throws Exception
    {
        assertStatementError("42X99", createStatement(),
            "create table t3 as select * from t1 where i = ? with no data");
    }

    /**
     * Test error when duplicate column names are specified in the column list.
     * @throws Exception
     */
    public void testDuplicateColumnName() throws Exception
    {
        assertStatementError("42X12", createStatement(),
            "create table t3 (c1,c2,c1) "
                + "as select i, s, f from t1 with no data");
    }

    /**
     * Test error when the number of columns in the column list does
     * not match the number of columns in the query expression.
     * @throws Exception
     */
    public void testColumnCountMismatch() throws Exception
    {
        assertStatementError("42X70", createStatement(),
            "create table t3 (c1,c2,c3) as select i,s from t1 with no data");
    }

    /**
     * Test error when the query expression contains system generated
     * column names and no column list was provided.
     * @throws Exception
     */
    public void testSystemGeneratedColumnName() throws Exception
    {
        assertStatementError("42909", createStatement(),
            "create table t3 as select i, 2*i from t1 with no data");
    }

    /**
     * Test error when the column type can not be determined.
     * @throws Exception
     */
    public void testNullValues() throws Exception
    {
        assertStatementError("42X07", createStatement(),
            "create table t3 (x) as values null with no data");
    }

    /**
     * Test error for unimplemented WITH DATA clause.
     * @throws Exception
     */
    public void testUnimplementedWithDataClause() throws Exception
    {
        assertStatementError("0A000", createStatement(),
            "create table t3 as select * from t1 with data");
    }
    
    /**
     * Test error for creating table where the data type is invalid.
     */
    public void testInvalidDataType() throws Exception
    {
        Statement stmt = createStatement();

        // USER (Java Object)
        assertStatementError("42X71", stmt,
            "create table t as select aliasinfo from sys.sysaliases with no data");
        
        // DECIMAL(44,0)
        assertStatementError("42X71", stmt,
        	"create table t(x) as values 12345678901234567890123456789012345678901234 with no data");
    }
   
    private void positiveTest(String sql, String [] columnNames,
            String [] nullability, String [] types) throws Exception
    {
        Statement stmt = createStatement();

        // create table
        stmt.executeUpdate(sql);

        // check column's name, nullability, and type
        DatabaseMetaData dmd = getConnection().getMetaData();
        ResultSet rs = dmd.getColumns(null, null, "T3", null);
        int col = 0;
        while (rs.next()) {
            Assert.assertEquals("Column names do not match:",
                    columnNames[col], rs.getString("COLUMN_NAME"));
            Assert.assertEquals("Nullability incorrect:",
                    nullability[col], rs.getString("IS_NULLABLE"));
            Assert.assertEquals("Column type incorrect:",
                    types[col], rs.getString("TYPE_NAME"));
            col++;
        }
        rs.close();
        Assert.assertEquals("Unexpected column count:",
                columnNames.length, col);
        stmt.executeUpdate("drop table t3");
    }
    
    /**
     * Set the fixture up with base tables t1 and t2.
     */
    protected void setUp() throws SQLException
    {
        setAutoCommit(false);
    }
}
