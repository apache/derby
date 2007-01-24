/*
 *
 * Derby - Class org.apache.derbyTesting.system.oe.test.OperationsTester
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

import org.apache.derbyTesting.system.oe.client.Display;
import org.apache.derbyTesting.system.oe.client.Operations;
import org.apache.derbyTesting.system.oe.model.Customer;
import org.apache.derbyTesting.system.oe.model.District;
import org.apache.derbyTesting.system.oe.model.Order;
import org.apache.derbyTesting.system.oe.model.OrderLine;
import org.apache.derbyTesting.system.oe.model.Warehouse;
import org.apache.derbyTesting.system.oe.util.OERandom;

/**
 * Test an implementation of Operations.
 * Currently just tests the setup but as more
 * code is added the implemetations of the transactions
 * will be added.
 */
class OperationsTester implements Display {

    private final Operations ops;
    private final OERandom rand;
    private final short w = 1;
    
    OperationsTester(Operations ops) {
        this.ops = ops;
        this.rand = new OERandom(-1, -1, -1, 3458567);
    }
    
    void test() throws Exception
    {
        testStockLevel();
        testOrderStatus();
        testPayment();
        testNewOrder();
        testScheduleDelivery();
        testDelivery();
    }
    
    private void testStockLevel() throws Exception
    {
        ops.setupStockLevel();
        
        // Check a null display is handled
        ops.stockLevel(null, null,
                w, rand.district(), rand.threshold());
        
        for (int i = 0; i < 20; i++)
            ops.stockLevel(this, null,
                    w, rand.district(), rand.threshold());
    }
    
    /**
     * Execute a number of order-status transactions
     * by name and identifier. Also check the implementation
     * accepts a null display.
     * @throws Exception
     */
    private void testOrderStatus() throws Exception
    {
        ops.setupOrderStatus();
        
        // By identifier
        ops.orderStatus(null, null,
                w, rand.district(), rand.NURand1023());
        for (int i = 0; i < 50; i++) {
            ops.orderStatus(this, null,
                    w, rand.district(), rand.NURand1023());
        }
        
        // By name 
        ops.orderStatus(null, null,
                w, rand.district(), rand.randomCLast());
        for (int i = 0; i < 50; i++)
        {
            ops.orderStatus(this, null,
                    w, rand.district(), rand.randomCLast());
            
        }
        //
    }
    private void testPayment() throws Exception
    {
        ops.setupPayment();
    }
    private void testNewOrder() throws Exception
    {
        ops.setupNewOrder();
    }
    private void testScheduleDelivery() throws Exception
    {
        ops.setupScheduleDelivery();
    }
    private void testDelivery() throws Exception
    {
        ops.setupDelivery();
    }

    public void displayStockLevel(Object displayData, short w, short d, int threshold, int level) throws Exception {
        // TODO: Check expected data is set.  
    }

    public void displayOrderStatus(Object displayData, boolean byName, Customer customer, Order order, OrderLine[] lineItems) throws Exception {
        // TODO: Check expected data is set.   
        
    }

    public void displayPayment(Object displayData, String amount, boolean byName, Warehouse warehouse, District district, Customer customer) throws Exception {
        // TODO Auto-generated method stub
        
    }

    public void displayNewOrder(Object displayData, Warehouse warehouse, District district, Customer customer, Order order) throws Exception {
        // TODO Auto-generated method stub
        
    }

    public void displayScheduleDelivery(Object displayData, short w, short carrier) throws Exception {
        // TODO Auto-generated method stub
        
    }
}
