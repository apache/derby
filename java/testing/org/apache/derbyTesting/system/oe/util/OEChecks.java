/*
 *
 * Derby - Class org.apache.derbyTesting.system.oe.util.OEChecks
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.system.oe.util;

import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Connection;

import org.apache.derbyTesting.system.oe.client.Load;

/**
 * Do some OEChecks on the Order Entry database.
 */
public class OEChecks  {
    
    /**
     * How to report an error.
     */
    private HandleCheckError errorHandler;

    /**
     * Warehouse scaling factor
     */
    private short scale = 1;

    private Connection conn = null;

    public void initialize(HandleCheckError errorHandler,
            Connection conn, short scale)
    throws Exception
    {
        this.errorHandler = errorHandler;
        this.conn = conn;
        conn.setAutoCommit(false);
        this.scale = scale;
    }
    
    /**
     * Return suite of tests that OEChecks the row counts for all the tables in
     * the Order Entry bechmark.
     */
    public void checkAllRowCounts() throws Exception {
        testWarehouseRows();
        testStockRows();
        testItemRows();
        testCustomerRows();
        testDistrictRows();
        testOrdersRows();
        testNewOrdersRows();
        testOrderLineRows();
        testHistoryRows();
    }
    
    /**
     * Test cardinality of WAREHOUSE table
     * 
     * @throws Exception
     */
    public void testWarehouseRows() throws Exception {
        checkCountStar("WAREHOUSE", scale);
    }

    /**
     * Test cardinality of STOCK table
     * 
     * @throws Exception
     */
    public void testStockRows() throws Exception {
        checkCountStar("STOCK", Load.STOCK_COUNT_W * scale);
    }

    /**
     * Test cardinality of ORDERS table
     * 
     * @throws Exception
     */
    public void testOrdersRows() throws Exception {
        checkCountStar("ORDERS", Load.ORDERS_COUNT_W * scale);
    }

    /**
     * Test cardinality of DISTRICT table
     * 
     * @throws Exception
     */
    public void testDistrictRows() throws Exception {
        checkCountStar("DISTRICT", Load.DISTRICT_COUNT_W * scale);
    }

    /**
     * Test cardinality of CUSTOMER table
     * 
     * @throws Exception
     */
    public void testCustomerRows() throws Exception {
        checkCountStar("CUSTOMER", Load.CUSTOMER_COUNT_W * scale);
    }

    /**
     * Test cardinality of ITEM table
     * 
     * @throws Exception
     */
    public void testItemRows() throws Exception {
        checkCountStar("ITEM", Load.ITEM_COUNT);
    }

    /**
     * Test cardinality of NEWORDERS table
     * 
     * @throws Exception
     */
    public void testNewOrdersRows() throws Exception {
        checkCountStar("NEWORDERS", Load.NEWORDERS_COUNT_W * scale);
    }

    /**
     * Test cardinality of HISTORY table
     * 
     * @throws Exception
     */
    public void testHistoryRows() throws Exception {
        checkCountStar("HISTORY", Load.HISTORY_COUNT_W * scale);
    }

    /**
     * Test cardinality of ORDERLINE table
     * 
     * @throws Exception
     */
    public void testOrderLineRows() throws Exception {
        checkWithinOnePercent("ORDERLINE", Load.ORDERLINE_COUNT_WV * scale);
    }

    /**
     * Check if number of rows in table is as expected
     * 
     * @param table -
     *            table on which to execute the query
     * @param expected -
     *            expected number of rows
     * @throws Exception
     */
    private void checkCountStar(String table, int expected) throws Exception {
        if( expected != rowsInTable(table))
            errorHandler.handleCheckError("ERROR:Number of rows loaded for " + table +
                    " not correct, expected="+expected +" rows found="+ 
                    rowsInTable(table));

    }

    /**
     * Return the number of rows in the table. A simple select count(*) from
     * tableName
     * 
     * @param tableName -
     *            name of the table
     * @throws SQLException
     */
    private int rowsInTable(String tableName) throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName);
        rs.next();
        int count = rs.getInt(1);
        conn.commit();
        rs.close();
        stmt.close();

        return count;
    }

    /**
     * Check if number of rows in table is within one percent of expected value
     * 
     * @param tableName -
     *            table on which to execute the query
     * @param expected -
     *            expected number of rows
     * @throws Exception
     */
    private void checkWithinOnePercent(String tableName, int expected)
            throws Exception {

        double count = rowsInTable(tableName);
        double low = ((double) expected) * 0.99;
        double high = ((double) expected) * 1.01;
        if ( (count < low) || (count >high))
            errorHandler.handleCheckError("ERROR! Initial rows" + count + " in " + 
                tableName + " is out of range.[" + low + "-" + high + "]");
        
    }
    
}
