/*

 Derby - Class org.apache.derbyTesting.perf.basic.jdbc.BaseLoad100TestSetup

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
package org.apache.derbyTesting.perf.basic.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import junit.framework.Test;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;

/**
 * TestSetup decorator to load the schema that is used for some of the
 * performance tests.
 * Schema includes:
 * Table: is  9 column table (100bytes)  - 5 columns of type int, 4 columns of char(20)")
 * data distribution - { column 1 has unique values,column 2 is set to 2,
 * column 3 has 1 % of rows with same value, column 4 is set to 2,
 * column 5 has unique values, column 6 is set to a constant value,
 *  column 7 has 1% of rows with known pattern,
 * column 8 has constant value,column 9 has values having the same suffix.
 * One of the constructors allows the data type of the last four columns to be
 * changed to CHAR(20) FOR BIT DATA.
 *
 * Connection props :  autocommit - false, default isolation level- Read Committed.
 *
 * If any indexes have to be created or any other schema additions need to be made, then
 * the decorateSQL() method needs to be overriden.
 */
public class BaseLoad100TestSetup extends CleanDatabaseTestSetup {

    protected int rowsToLoad=10000;
    protected String tableName = "BASELOAD100";
    private boolean binaryData;

    /**
     *
     * @param test
     */
    public BaseLoad100TestSetup(Test test) {
        super(test);
    }

    /**
     * @param test name of test
     * @param rowsToLoad number of rows to insert
     */
    public BaseLoad100TestSetup(Test test, int rowsToLoad)
    {
        super(test);
        this.rowsToLoad=rowsToLoad;
    }

    /**
     * @param test name of the test
     * @param tableName name of the table to insert the rows into
     */
    public BaseLoad100TestSetup(Test test, String tableName)
    {
        super(test);
        this.tableName = tableName;
    }

    /**
     * @param test name of test
     * @param rowsToLoad number of rows to insert
     * @param tableName name of the table to insert the rows into
     */
    public BaseLoad100TestSetup(Test test,int rowsToLoad, String tableName)
    {
        this(test, rowsToLoad, tableName, false);
    }

    /**
     * @param test name of test
     * @param rowsToLoad number of rows to insert
     * @param tableName name of the table to insert the rows into
     * @param binaryData whether or not c6, ..., c9 should contain binary data
     */
    public BaseLoad100TestSetup(
            Test test, int rowsToLoad, String tableName, boolean binaryData) {
        super(test);
        this.tableName = tableName;
        this.rowsToLoad = rowsToLoad;
        this.binaryData = binaryData;
    }

    /**
     * Clean the default database using the default connection
     * and calls the decorateSQL to allow sub-classes to
     * initialize their schema requirments.
     */
    protected void setUp() throws Exception {
        super.setUp();

        Connection conn = getConnection();
        conn.setAutoCommit(false);
        PreparedStatement insert = conn.prepareStatement(
                "INSERT INTO "+tableName+" VALUES ( ?,?,?,?,?,?,?,?,? )");
        loadData(insert);
        insert.close();
        conn.close();
    }


    /**
     * Override the decorateSQL and create the necessary schema.
     * @see org.apache.derbyTesting.junit.CleanDatabaseTestSetup#decorateSQL(java.sql.Statement)
     */
    protected void decorateSQL(Statement s)
        throws SQLException
    {
        StringBuffer ddl = new StringBuffer();
        ddl.append("CREATE TABLE ").append(tableName);
        ddl.append("(i1 INT, i2 INT, i3 INT, i4 INT, i5 INT");
        for (int i = 6; i <= 9; i++) {
            ddl.append(", c").append(i).append(" CHAR(20)");
            if (binaryData) {
                ddl.append(" FOR BIT DATA");
            }
        }
        ddl.append(')');
        s.execute(ddl.toString());
    }

    /**
     * Load the data into the table.
     * @param insert  prepared statement to use for inserting data.
     * @throws Exception
     */
    private void loadData(PreparedStatement insert) throws Exception {

        for (int i = 0; i < rowsToLoad; i++) {
            insert.setInt(1, i);
            insert.setInt(2, 2);

            // 1% of rows with a known pattern for where etc.
            if ((i % 100) == 57)
                insert.setInt(3, 436);
            else
                insert.setInt(3, 2);

            insert.setInt(4, 2);
            insert.setInt(5, i);
            insert.setObject(6, convertData("01234567890123456789"));

            // 1% of rows with a known pattern for like etc.
            if ((i % 100) == 34)
                insert.setObject(7, convertData("012345javaone6789"));
            else
                insert.setObject(7, convertData("01234567890123456789"));

            insert.setObject(8, convertData("01234567890123456789"));

            insert.setObject(9, convertData((i + 1000) + "0123456789012"));
            insert.executeUpdate();
        }
        insert.getConnection().commit();
    }

    /**
     * Convert a string to a data type appropriate for the columns c6 to c9,
     * that is, either a {@code String} value or a {@code byte[]} value.
     *
     * @param string the string to generate the value from
     * @return either {@code string}, or a {@code byte[]} value representing
     * {@code string} if {@code binaryData} is {@code true}
     * @throws IOException if the string cannot be converted to a byte array
     */
    private Object convertData(String string) throws IOException {
        if (binaryData) {
            return string.getBytes("US-ASCII");
        } else {
            return string;
        }
    }
}
