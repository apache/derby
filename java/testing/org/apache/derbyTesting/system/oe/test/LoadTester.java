/*
 *
 * Derby - Class org.apache.derbyTesting.system.oe.test.LoadTester
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
package org.apache.derbyTesting.system.oe.test;

import java.sql.ResultSet;
import java.sql.Statement;

import junit.framework.Assert;
import org.apache.derbyTesting.system.oe.client.Load;
/**
 * Test an implementation of Load.
 */
class LoadTester  {

    private final Load load;

    LoadTester(Load load) {
        this.load = load;
    }

    /**
     * test the population of database
     * @throws Exception
     */
    void test() throws Exception
    {
        load.populateAllTables();
    }

    public void testWarehouseRows() throws Exception
    {
        checkCountStar("WAREHOUSE",load.getScale());
    }

    public void testStockRows() throws Exception
    {
        checkCountStar("STOCK",Load.STOCK_COUNT_W*load.getScale());
    }

    public void testOrdersRows() throws Exception
    {
        checkCountStar("ORDERS",Load.ORDERS_COUNT_W*load.getScale());
    }
    public void testDistrictRows() throws Exception
    {
        checkCountStar("DISTRICT",Load.DISTRICT_COUNT_W*load.getScale());
    }
    public void testCustomerRows() throws Exception
    {
        checkCountStar("CUSTOMER",Load.CUSTOMER_COUNT_W*load.getScale());
    }
    public void testItemRows() throws Exception
    {
        checkCountStar("ITEM",Load.ITEM_COUNT);
    }
    public void testNewOrdersRows() throws Exception
    {
        checkCountStar("NEWORDERS",Load.NEWORDERS_COUNT_W*load.getScale());
    }
    public void testHistoryRows() throws Exception
    {
        checkCountStar("HISTORY",Load.HISTORY_COUNT_W*load.getScale());
    }

    public void testOrderLineRows() throws Exception
    {
        checkWithinOnePercent("ORDERLINE",Load.ORDERLINE_COUNT_WV*load.getScale());
    }

    void checkCountStar(String table, int expected) throws Exception
    {
        Assert.assertEquals("Number of rows loaded for "+ table +" not correct",expected, load.rowsInTable(table));
    }    

    void checkWithinOnePercent(String tableName, int expected)
    throws Exception {

        double count = load.rowsInTable(tableName);

        double low =  ((double) expected) * 0.99;
        double high =  ((double) expected) * 1.01;

        Assert.assertEquals("Initial rows"+count+" in "+tableName+" is out of range.["+low+"-"+high+"]", false, ((count < low) || (count > high)));

    }

}
