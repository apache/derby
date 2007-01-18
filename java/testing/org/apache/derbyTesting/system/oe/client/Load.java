/*
 *
 * Derby - Class org.apache.derbyTesting.system.oe.client.Load
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
package org.apache.derbyTesting.system.oe.client;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Interface for a client to populate the database. Various implementations can
 * be provided, e.g. via SQL, via VTI, via import etc
 * An implementation for Load must be able to do the following
 * <OL>
 * <LI> Use the setupLoad to perform any necessary initialization for the load
 * phase
 * <LI> Load data into the tables 
 * <LI> Provide information about cardinality of rows in each table.
 * </OL>
 * <P>
 * DECIMAL values are represented as String objects to allow Order Entry to be
 * run on J2ME/CDC/Foundation which does not support BigDecimal.
 */
public interface Load {

    /**
     * Cardinality constants 
     * These numbers are factored by W, the number of Warehouses,
     * to illustrate the database scaling.
     * 
     * see section 1.2.1 of TPC-C specification
     */
    public static final short DISTRICT_COUNT_W = 10;

    public static final int CUSTOMER_COUNT_W = 3000 * DISTRICT_COUNT_W;

    public static final int ITEM_COUNT = 100000;

    public static final int NEWORDERS_COUNT_W = (CUSTOMER_COUNT_W * 9) / 30;

    // 1 per customer initially
    public static final int HISTORY_COUNT_W = CUSTOMER_COUNT_W; 

    public static final int STOCK_COUNT_W = ITEM_COUNT;

    public static final int ORDERS_COUNT_W = CUSTOMER_COUNT_W;

    //  5 to 15 , per order. so average 10
    public static final int ORDERLINE_COUNT_WV = ORDERS_COUNT_W * 10; 

    public static final int NEWORDERS_BREAKPOINT = 
        (ORDERS_COUNT_W - NEWORDERS_COUNT_W)/ DISTRICT_COUNT_W;

    /**
     * Return the warehouse scale factor of the database. 
     * @return
     */
    public short getScale();

    /**
     * Perform the necessary setup before database population.
     * @param conn - database connection 
     * @param scale - scale of the database.  The WAREHOUSE table is 
     * used as the base unit of scaling. 
     * @throws Exception
     */
    public void setupLoad(Connection conn, short scale) throws Exception;

    /**
     * Follow the initial database population requirements in Section 4.3.3 
     * and populate all the required tables.
     * @throws SQLException
     */
    public void populateAllTables() throws SQLException;

    /**
     * Return the number of rows in the table. 
     * A simple select count(*) from tableName 
     * @param tableName - name of the table
     * @throws SQLException
     */
    public int rowsInTable(String tableName) throws SQLException;

    /**
     * Populate the ITEM table 
     * See population requirements in section 4.3.3.1
     * <BR>
     * @param itemStart insert item information starting from this Item id (ITEM.I_ID) 
     * @param itemEnd  last Item id (ITEM.I_ID) for inserting information for
     * @throws SQLException
     */
    public void itemTable(int itemStart, int itemEnd) throws SQLException;

    /**
     * Populate the WAREHOUSE table for a given warehouse.
     * See population requirements in section 4.3.3.1
     * @param w WAREHOUSE ID (W_ID)
     * @throws SQLException
     */
    public void warehouseTable(short w) throws SQLException;

    /**
     * Populate the STOCK table for a given warehouse.
     * See population requirements in section 4.3.3.1
     * <BR>
     * @param itemStart insert stocks of items from this Item id (ITEM.I_ID) 
     * @param itemEnd  last Item id (ITEM.I_ID) to insert stocks of times for.
     * @param w WAREHOUSE id (W_ID) for which the stock is populated.
     * @throws SQLException
     */
    public void stockTable(int itemStart, int itemEnd, short w)
    throws SQLException;

    /**
     * Populate the DISTRICT table for a given warehouse.
     * See population requirements in section 4.3.3.1
     * <BR>
     * @param w - WAREHOUSE id (W_ID)
     * @param d - DISTRICT id (D_ID)
     * @throws SQLException
     */
    public void districtTable(short w, short d) throws SQLException;

    /**
     * Populate the CUSTOMER table for a given district for a specific warehouse.
     * See population requirements in section 4.3.3.1
     * <BR>
     * @param w - WAREHOUSE id (W_ID)
     * @param d - DISTRICT id (D_ID)
     * @throws SQLException
     */
    public void customerTable(short w, short d) throws SQLException;

    /**
     * Populate the ORDER table 
     * See population requirements in section 4.3.3.1
     * @param w - WAREHOUSE id (W_ID)
     * @param d - DISTRICT id (D_ID)
     * @throws SQLException
     */
    public void orderTable(short w, short d) throws SQLException;

}
